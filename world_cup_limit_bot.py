"""
Simple Polymarket World Cup Match Limit Order Bot
=================================================

Places BUY limit orders only. It can target specific Polymarket market slugs,
event slugs, or search Gamma for World Cup match markets.

Recommended .env values:
    DRY_RUN=true
    WORLD_CUP_MARKET_SLUGS=market-slug-1,market-slug-2
    # or WORLD_CUP_EVENT_SLUGS=event-slug-1,event-slug-2
    # or WORLD_CUP_SEARCH_QUERY=world cup
    WORLD_CUP_OUTCOME=YES
    WORLD_CUP_ORDER_PRICE=0.10
    WORLD_CUP_ORDER_SIZE=5
    WORLD_CUP_MAX_MARKETS=5

Live trading additionally needs the same Polymarket credentials used by
eth_limit_v2.py:
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
"""

import json
import logging
import math
import os
import sys
import time
from typing import Any

import requests
from dotenv import load_dotenv
from py_clob_client_v2 import ApiCreds, ClobClient, OpenOrderParams, OrderArgs, OrderType, Side
from py_clob_client_v2.constants import POLYGON

load_dotenv()

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
ORDER_PRICE = float(os.getenv("WORLD_CUP_ORDER_PRICE", os.getenv("ORDER_PRICE", "0.10")))
ORDER_SIZE = float(os.getenv("WORLD_CUP_ORDER_SIZE", os.getenv("ORDER_SIZE", "5")))
OUTCOME = os.getenv("WORLD_CUP_OUTCOME", "YES").strip()
MARKET_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_MARKET_SLUGS", "").split(",") if s.strip()]
EVENT_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_EVENT_SLUGS", "").split(",") if s.strip()]
SEARCH_QUERY = os.getenv("WORLD_CUP_SEARCH_QUERY", "world cup").strip()
MAX_MARKETS = int(os.getenv("WORLD_CUP_MAX_MARKETS", "5"))
SKIP_EXISTING = os.getenv("WORLD_CUP_SKIP_EXISTING", "true").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger(__name__)


def _csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def build_client() -> ClobClient | None:
    """Build the Polymarket CLOB client, or return None for dry-runs without credentials."""
    private_key = os.getenv("POLY_PRIVATE_KEY")
    api_key = os.getenv("POLY_API_KEY")
    api_secret = os.getenv("POLY_API_SECRET")
    api_passphrase = os.getenv("POLY_API_PASSPHRASE")
    funder = os.getenv("POLY_FUNDER_ADDRESS", "")

    if not all([private_key, api_key, api_secret, api_passphrase]):
        if DRY_RUN:
            log.info("[DRY RUN] Missing Polymarket credentials; running without CLOB client.")
            return None
        raise SystemExit(
            "Missing POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, or POLY_API_PASSPHRASE."
        )

    return ClobClient(
        host=CLOB_API,
        chain_id=POLYGON,
        key=private_key,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        ),
        signature_type=1,
        funder=funder or None,
    )


def gamma_get(path: str, **params: Any) -> Any:
    try:
        response = requests.get(f"{GAMMA_API}{path}", params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        log.error("Gamma API request failed for %s with %s: %s", path, params, exc)
        return []


def fetch_market_by_slug(slug: str) -> dict[str, Any] | None:
    data = gamma_get("/markets", slug=slug)
    markets = data if isinstance(data, list) else data.get("markets", [])
    if markets:
        return markets[0]
    events = gamma_get("/events", slug=slug)
    if isinstance(events, list) and events:
        nested = events[0].get("markets") or []
        return nested[0] if nested else None
    return None


def fetch_event_markets(slug: str) -> list[dict[str, Any]]:
    events = gamma_get("/events", slug=slug)
    if not isinstance(events, list) or not events:
        return []
    return [m for event in events for m in (event.get("markets") or [])]


def search_world_cup_markets() -> list[dict[str, Any]]:
    """Search Gamma and keep active CLOB-enabled markets that look like World Cup matches."""
    data = gamma_get("/markets", search=SEARCH_QUERY, active="true", closed="false", limit=MAX_MARKETS * 4)
    markets = data if isinstance(data, list) else data.get("markets", [])
    filtered: list[dict[str, Any]] = []
    for market in markets:
        text = " ".join(str(market.get(k, "")) for k in ("question", "slug", "description")).lower()
        if "world cup" not in text:
            continue
        if not market.get("clobTokenIds") and not market.get("clob_token_ids"):
            continue
        filtered.append(market)
        if len(filtered) >= MAX_MARKETS:
            break
    return filtered


def parse_json_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw]
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
            if isinstance(value, list):
                return [str(x).strip() for x in value]
        except json.JSONDecodeError:
            return _csv(raw)
    return []


def token_for_outcome(market: dict[str, Any], desired_outcome: str) -> tuple[str, str]:
    outcomes = parse_json_list(market.get("outcomes"))
    token_ids = parse_json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
    if len(outcomes) != len(token_ids) or not token_ids:
        raise ValueError(f"Cannot match outcomes to token IDs for {market.get('slug')}")

    wanted = desired_outcome.strip().lower()
    for outcome, token_id in zip(outcomes, token_ids, strict=True):
        if outcome.lower() == wanted:
            return outcome, token_id

    available = ", ".join(outcomes)
    raise ValueError(
        f"Outcome '{desired_outcome}' not found for {market.get('slug')}; available outcomes: {available}"
    )


def tick_size(client: ClobClient | None, token_id: str, market: dict[str, Any]) -> float:
    for key in ("minimumTickSize", "minimum_tick_size", "minTickSize", "tickSize"):
        raw = market.get(key)
        if raw is not None:
            value = float(raw)
            if value > 0:
                return value

    if client is not None and not DRY_RUN:
        book = client.get_order_book(token_id)
        raw_tick = book.get("tick_size") if isinstance(book, dict) else getattr(book, "tick_size", None)
        if raw_tick is not None and float(raw_tick) > 0:
            return float(raw_tick)

    return 0.001 if DRY_RUN else 0.01


def snap_price(price: float, tick: float) -> float:
    return round(math.ceil(round(price / tick, 10)) * tick, 10)


def existing_buy_order(client: ClobClient | None, condition_id: str, token_id: str) -> str | None:
    if client is None or DRY_RUN or not SKIP_EXISTING:
        return None
    orders = client.get_open_orders(OpenOrderParams(market=condition_id)) or []
    for order in orders:
        if order.get("asset_id") == token_id and order.get("side", "").upper() == "BUY":
            return order.get("id") or order.get("orderID")
    return None


def place_limit_buy(client: ClobClient | None, market: dict[str, Any]) -> str | None:
    slug = str(market.get("slug", ""))
    question = str(market.get("question", slug))
    condition_id = market.get("conditionId") or market.get("condition_id") or ""
    outcome, token_id = token_for_outcome(market, OUTCOME)

    existing_id = existing_buy_order(client, condition_id, token_id)
    if existing_id:
        log.info("Skipping %s | %s; existing BUY order %s", slug, outcome, existing_id)
        return existing_id

    price = snap_price(ORDER_PRICE, tick_size(client, token_id, market))
    cost = round(price * ORDER_SIZE, 4)
    label = f"{question} | {outcome}"

    if DRY_RUN:
        dry_id = f"dry-world-cup-{int(time.time())}"
        log.info("[DRY RUN] LIMIT BUY %s | price=$%.4f | size=%s | cost=$%.4f", label, price, ORDER_SIZE, cost)
        return dry_id

    if client is None:
        raise RuntimeError("CLOB client is required when DRY_RUN=false")

    response = client.create_and_post_order(
        order_args=OrderArgs(token_id=token_id, price=price, size=ORDER_SIZE, side=Side.BUY),
        order_type=OrderType.GTC,
    )
    if isinstance(response, dict) and not response.get("success", True):
        log.error("Order rejected for %s: %s", slug, response.get("errorMsg", "unknown"))
        return None
    order_id = response.get("orderID") or response.get("id") or str(response)
    log.info("LIMIT BUY placed %s | price=$%.4f | size=%s | order_id=%s", label, price, ORDER_SIZE, order_id)
    return order_id


def collect_markets() -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    seen: set[str] = set()

    for slug in MARKET_SLUGS:
        market = fetch_market_by_slug(slug)
        if market:
            key = market.get("conditionId") or market.get("slug") or slug
            if key not in seen:
                markets.append(market)
                seen.add(key)
        else:
            log.warning("Market not found for slug: %s", slug)

    for event_slug in EVENT_SLUGS:
        for market in fetch_event_markets(event_slug):
            key = market.get("conditionId") or market.get("slug") or ""
            if key and key not in seen:
                markets.append(market)
                seen.add(key)

    if not markets:
        for market in search_world_cup_markets():
            key = market.get("conditionId") or market.get("slug") or ""
            if key and key not in seen:
                markets.append(market)
                seen.add(key)

    return markets[:MAX_MARKETS]


def main() -> None:
    log.info("Simple Polymarket World Cup Limit Bot")
    log.info("Mode=%s | outcome=%s | price=$%.4f | size=%s | max_markets=%s", "DRY_RUN" if DRY_RUN else "LIVE", OUTCOME, ORDER_PRICE, ORDER_SIZE, MAX_MARKETS)

    client = build_client()
    markets = collect_markets()
    if not markets:
        log.warning("No World Cup markets found. Set WORLD_CUP_MARKET_SLUGS or WORLD_CUP_EVENT_SLUGS for precise targeting.")
        return

    placed = 0
    for market in markets:
        try:
            if place_limit_buy(client, market):
                placed += 1
        except Exception as exc:
            log.error("Skipping %s: %s", market.get("slug", "unknown"), exc)

    log.info("Done. %s/%s market(s) had an order placed or resumed.", placed, len(markets))


if __name__ == "__main__":
    main()
