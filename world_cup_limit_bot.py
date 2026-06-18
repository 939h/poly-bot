"""
Polymarket World Cup Exact Score Spread Bot
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...

    WORLD_CUP_MARKET_SLUGS=slug1,slug2        # optional; must be exact-score markets
    WORLD_CUP_MATCH_SLUGS=world-cup/fifwc-cze-rsa-2026-06-18  # Gamma /sports slug, bare event slug, path, or URL
    WORLD_CUP_EVENT_SLUGS=event1,event2       # optional alias for match/event slugs
    WORLD_CUP_SEARCH_QUERY=world cup exact score
    WORLD_CUP_EXACT_SCORE_OUTCOMES=           # optional CSV, e.g. "1-0,2-1"; empty = all outcomes
    WORLD_CUP_ORDER_SIZE=5
    WORLD_CUP_SPREAD_RATIO_MIN=1.8
    WORLD_CUP_TAKE_PROFIT_MULTIPLIER=2
    WORLD_CUP_MAX_MARKETS=5
    WORLD_CUP_MAX_OUTCOMES_PER_MARKET=40
    WORLD_CUP_MIN_SCORE_OUTCOMES=3             # exact-score markets have outcomes like 0-0, 0-1, 1-0, 3-3
    WORLD_CUP_POLL_SECS=30
    WORLD_CUP_DAY_TZ_OFFSET=0                 # UTC day; use 8 for MYT calendar day
"""

import json
import logging
import math
import os
import sys
import re
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from typing import Any

import requests
from dotenv import load_dotenv
from py_clob_client_v2 import ApiCreds, ClobClient, OpenOrderParams, OrderArgs, OrderType, Side
from py_clob_client_v2.constants import POLYGON

load_dotenv()

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MARKET_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_MARKET_SLUGS", "").split(",") if s.strip()]
MATCH_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_MATCH_SLUGS", "").split(",") if s.strip()]
EVENT_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_EVENT_SLUGS", "").split(",") if s.strip()]
SEARCH_QUERY = os.getenv("WORLD_CUP_SEARCH_QUERY", "world cup exact score").strip()
OUTCOME_FILTERS = [s.strip().lower() for s in os.getenv("WORLD_CUP_EXACT_SCORE_OUTCOMES", "").split(",") if s.strip()]
ORDER_SIZE = float(os.getenv("WORLD_CUP_ORDER_SIZE", os.getenv("ORDER_SIZE", "5")))
SPREAD_RATIO_MIN = float(os.getenv("WORLD_CUP_SPREAD_RATIO_MIN", "1.8"))
TAKE_PROFIT_MULTIPLIER = float(os.getenv("WORLD_CUP_TAKE_PROFIT_MULTIPLIER", "2"))
MAX_MARKETS = int(os.getenv("WORLD_CUP_MAX_MARKETS", "5"))
MAX_OUTCOMES_PER_MARKET = int(os.getenv("WORLD_CUP_MAX_OUTCOMES_PER_MARKET", "40"))
POLL_SECS = int(os.getenv("WORLD_CUP_POLL_SECS", "30"))
DAY_TZ_OFFSET = int(os.getenv("WORLD_CUP_DAY_TZ_OFFSET", "0"))
SKIP_EXISTING = os.getenv("WORLD_CUP_SKIP_EXISTING", "true").lower() == "true"
RUN_ONCE = os.getenv("WORLD_CUP_RUN_ONCE", "false").lower() == "true"
MIN_SCORE_OUTCOMES = int(os.getenv("WORLD_CUP_MIN_SCORE_OUTCOMES", "3"))
FIFWC_EVENT_RE = re.compile(r"fifwc-[a-z0-9]+-[a-z0-9]+-(\d{4})-(\d{2})-(\d{2})", re.IGNORECASE)
SCORE_OUTCOME_RE = re.compile(r"\b\d+\s*[-–]\s*\d+\b")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger(__name__)


def build_client() -> ClobClient | None:
    private_key = os.getenv("POLY_PRIVATE_KEY")
    api_key = os.getenv("POLY_API_KEY")
    api_secret = os.getenv("POLY_API_SECRET")
    api_passphrase = os.getenv("POLY_API_PASSPHRASE")
    funder = os.getenv("POLY_FUNDER_ADDRESS", "")

    if not all([private_key, api_key, api_secret, api_passphrase]):
        if DRY_RUN:
            log.info("[DRY RUN] Missing Polymarket credentials; running without CLOB client.")
            return None
        raise SystemExit("Missing POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, or POLY_API_PASSPHRASE.")

    return ClobClient(
        host=CLOB_API,
        chain_id=POLYGON,
        key=private_key,
        creds=ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase),
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


def sports_slug_from_value(value: str) -> str:
    """Return the Gamma sports slug, e.g. world-cup/fifwc-cze-rsa-2026-06-18."""
    cleaned = value.strip().strip("/")
    parsed = urlparse(cleaned)
    path = parsed.path.strip("/") if parsed.scheme or parsed.netloc else cleaned.split("?", 1)[0].strip("/")
    if path.startswith("sports/"):
        path = path[len("sports/"):]
    return path


def slug_from_value(value: str) -> str:
    """Accept a bare slug, sports path, or Polymarket URL and return the final event slug.

    Examples accepted:
      - fifwc-cze-rsa-2026-06-18
      - world-cup/fifwc-cze-rsa-2026-06-18
      - https://polymarket.com/sports/world-cup/fifwc-cze-rsa-2026-06-18
    """
    cleaned = value.strip().rstrip("/")
    parsed = urlparse(cleaned)
    path = parsed.path if parsed.scheme or parsed.netloc else cleaned.split("?", 1)[0]
    return path.rstrip("/").split("/")[-1]


def event_date_from_slug(slug: str) -> datetime | None:
    match = FIFWC_EVENT_RE.search(slug_from_value(slug))
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return datetime(year, month, day, tzinfo=UTC)


def local_day_window_utc() -> tuple[datetime, datetime]:
    now = datetime.now(UTC) + timedelta(hours=DAY_TZ_OFFSET)
    day_start_local = datetime(now.year, now.month, now.day, tzinfo=UTC)
    start_utc = day_start_local - timedelta(hours=DAY_TZ_OFFSET)
    end_utc = start_utc + timedelta(days=1)
    return start_utc, end_utc


def parse_dt(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def is_today_market(market: dict[str, Any]) -> bool:
    start, end = local_day_window_utc()
    market_dt = parse_dt(market.get("endDateIso") or market.get("end_date_iso") or market.get("endDate"))
    if market_dt is None:
        market_dt = event_date_from_slug(str(market.get("_event_slug") or market.get("slug") or ""))
    if market_dt is None:
        return False
    return start <= market_dt < end


def is_world_cup_market(market: dict[str, Any]) -> bool:
    event_slug = str(market.get("_event_slug") or market.get("slug") or "").lower()
    text = " ".join(str(market.get(k, "")) for k in ("question", "slug", "description", "groupItemTitle")).lower()
    return event_slug.startswith("fifwc-") or "world cup" in text or "fifa world cup" in text


def has_exact_score_outcomes(market: dict[str, Any]) -> bool:
    outcomes = parse_json_list(market.get("outcomes"))
    if any(outcome.lower() == "any other score" for outcome in outcomes):
        return True
    score_like = sum(1 for outcome in outcomes if SCORE_OUTCOME_RE.search(outcome))
    return score_like >= MIN_SCORE_OUTCOMES


def is_exact_score_world_cup_market(market: dict[str, Any]) -> bool:
    text = " ".join(str(market.get(k, "")) for k in ("question", "slug", "description", "groupItemTitle")).lower()
    text_match = any(marker in text for marker in ("exact score", "correct score", "any other score", "actual score is not"))
    return is_world_cup_market(market) and (text_match or has_exact_score_outcomes(market))


def as_list_payload(data: Any, key: str) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        nested = data.get(key)
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        if key == "events" and data.get("slug"):
            return [data]
        if key == "markets" and (data.get("conditionId") or data.get("condition_id") or data.get("slug")):
            return [data]
    return []


def fetch_market_by_slug(slug: str) -> dict[str, Any] | None:
    data = gamma_get("/markets", slug=slug)
    markets = as_list_payload(data, "markets")
    if markets:
        return markets[0]
    events = gamma_get("/events", slug=slug)
    event_list = as_list_payload(events, "events")
    if event_list:
        nested = event_list[0].get("markets") or []
        return nested[0] if nested else None
    return None


def looks_like_market(data: dict[str, Any]) -> bool:
    return bool(
        data.get("clobTokenIds")
        or data.get("clob_token_ids")
        or (data.get("outcomes") and (data.get("conditionId") or data.get("condition_id") or data.get("slug")))
    )


def extract_nested_markets(data: Any) -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            markets.extend(extract_nested_markets(item))
    elif isinstance(data, dict):
        if looks_like_market(data):
            markets.append(data)
        for key in ("markets", "children", "items", "events"):
            nested = data.get(key)
            if nested is not None:
                markets.extend(extract_nested_markets(nested))
    return markets


def fetch_sports_markets(slug: str) -> list[dict[str, Any]]:
    sports_slug = sports_slug_from_value(slug)
    event_slug = slug_from_value(slug)
    payload = gamma_get("/sports", slug=sports_slug)
    markets = extract_nested_markets(payload)
    for market in markets:
        market["_event_slug"] = market.get("_event_slug") or event_slug
    if not markets:
        log.warning("No sports markets found for sports slug: %s", sports_slug)
    else:
        log.info("Fetched %s nested sports market(s) for %s", len(markets), sports_slug)
    return markets


def fetch_event_markets(slug: str) -> list[dict[str, Any]]:
    sports_markets = fetch_sports_markets(slug)
    if sports_markets:
        return sports_markets

    event_slug = slug_from_value(slug)
    events = gamma_get("/events", slug=event_slug)
    event_list = as_list_payload(events, "events")
    if not event_list:
        log.warning("No event payload found for match/event slug: %s", event_slug)
        return []
    markets: list[dict[str, Any]] = []
    for event in event_list:
        parent_slug = event.get("slug") or event_slug
        for market in event.get("markets") or []:
            market["_event_slug"] = parent_slug
            markets.append(market)
    if not markets:
        log.warning("No nested markets found for match/event slug: %s", event_slug)
    return markets


def search_match_events() -> list[dict[str, Any]]:
    data = gamma_get("/events", search=SEARCH_QUERY, active="true", closed="false", limit=MAX_MARKETS * 10)
    events = as_list_payload(data, "events")
    today_events: list[dict[str, Any]] = []
    for event in events:
        slug = str(event.get("slug") or "")
        if not slug.lower().startswith("fifwc-"):
            continue
        event_date = event_date_from_slug(slug)
        start, end = local_day_window_utc()
        if event_date is None or not (start <= event_date < end):
            continue
        today_events.append(event)
    return today_events


def search_exact_score_markets() -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    for event in search_match_events():
        slug = str(event.get("slug") or "")
        for market in event.get("markets") or []:
            market["_event_slug"] = slug
            if is_exact_score_world_cup_market(market) and is_today_market(market):
                markets.append(market)

    if markets:
        return markets

    data = gamma_get("/markets", search=SEARCH_QUERY, active="true", closed="false", limit=MAX_MARKETS * 10)
    found = as_list_payload(data, "markets")
    return [m for m in found if is_exact_score_world_cup_market(m) and is_today_market(m)]


def collect_markets() -> list[dict[str, Any]]:
    markets: list[dict[str, Any]] = []
    seen: set[str] = set()

    candidates: list[dict[str, Any]] = []
    for slug in MARKET_SLUGS:
        market = fetch_market_by_slug(slug_from_value(slug))
        if market:
            candidates.append(market)
        else:
            log.warning("Market not found for slug: %s", slug)

    for event_slug in [*MATCH_SLUGS, *EVENT_SLUGS]:
        candidates.extend(fetch_event_markets(event_slug))

    if not candidates:
        candidates.extend(search_exact_score_markets())

    if candidates:
        log.info("Scanning %s candidate market(s) from configured/discovered World Cup match sources", len(candidates))

    for market in candidates:
        key = market.get("conditionId") or market.get("condition_id") or market.get("slug") or ""
        if not key or key in seen:
            continue
        if not is_exact_score_world_cup_market(market):
            log.info("Skipping non-exact-score/non-World-Cup market: %s", market.get("slug"))
            continue
        if not is_today_market(market):
            log.info("Skipping non-current-day market: %s", market.get("slug"))
            continue
        if not market.get("clobTokenIds") and not market.get("clob_token_ids"):
            log.info("Skipping market without CLOB tokens: %s", market.get("slug"))
            continue
        markets.append(market)
        seen.add(key)
        if len(markets) >= MAX_MARKETS:
            break

    if not markets:
        log.warning("No current-day World Cup exact-score CLOB markets found after filtering")
    else:
        log.info("Found %s current-day World Cup exact-score market(s)", len(markets))
    return markets


def parse_json_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw]
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
            if isinstance(value, list):
                return [str(x).strip() for x in value]
        except json.JSONDecodeError:
            return [part.strip() for part in raw.split(",") if part.strip()]
    return []


def market_outcomes(market: dict[str, Any]) -> list[tuple[str, str]]:
    outcomes = parse_json_list(market.get("outcomes"))
    token_ids = parse_json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
    if len(outcomes) != len(token_ids) or not token_ids:
        raise ValueError(f"Cannot match outcomes to token IDs for {market.get('slug')}")

    pairs = [(outcome, token_id) for outcome, token_id in zip(outcomes, token_ids, strict=True)]
    if OUTCOME_FILTERS:
        pairs = [(outcome, token_id) for outcome, token_id in pairs if outcome.lower() in OUTCOME_FILTERS]
    return pairs[:MAX_OUTCOMES_PER_MARKET]


def get_levels(book: Any, side_name: str) -> list[tuple[float, float]]:
    raw_levels = book.get(side_name) if isinstance(book, dict) else getattr(book, side_name, None)
    levels: list[tuple[float, float]] = []
    for level in raw_levels or []:
        price = level.get("price") if isinstance(level, dict) else getattr(level, "price", None)
        size = level.get("size") if isinstance(level, dict) else getattr(level, "size", None)
        if price is not None:
            levels.append((float(price), float(size or 0)))
    return levels


def best_bid_ask(client: ClobClient | None, token_id: str) -> tuple[float | None, float | None]:
    if client is None or DRY_RUN:
        dry_bid = float(os.getenv("WORLD_CUP_DRY_BEST_BID", "0.022"))
        dry_ask = float(os.getenv("WORLD_CUP_DRY_BEST_ASK", "0.044"))
        return dry_bid, dry_ask

    book = client.get_order_book(token_id)
    bids = get_levels(book, "bids")
    asks = get_levels(book, "asks")
    best_bid = max((price for price, _ in bids), default=None)
    best_ask = min((price for price, _ in asks), default=None)
    return best_bid, best_ask


def spread_ratio(client: ClobClient | None, token_id: str) -> tuple[float | None, float | None, float | None]:
    bid, ask = best_bid_ask(client, token_id)
    if bid is None or ask is None or bid <= 0:
        return bid, ask, None
    return bid, ask, ask / bid


def tick_size(client: ClobClient | None, token_id: str, market: dict[str, Any]) -> float:
    for key in ("minimumTickSize", "minimum_tick_size", "minTickSize", "tickSize"):
        raw = market.get(key)
        if raw is not None and float(raw) > 0:
            return float(raw)

    if client is not None and not DRY_RUN:
        book = client.get_order_book(token_id)
        raw_tick = book.get("tick_size") if isinstance(book, dict) else getattr(book, "tick_size", None)
        if raw_tick is not None and float(raw_tick) > 0:
            return float(raw_tick)

    return 0.001 if DRY_RUN else 0.01


def snap_price(price: float, tick: float) -> float:
    return round(math.ceil(round(price / tick, 10)) * tick, 10)


def open_buy_order_id(client: ClobClient | None, condition_id: str, token_id: str) -> str | None:
    if client is None or DRY_RUN or not SKIP_EXISTING:
        return None
    orders = client.get_open_orders(OpenOrderParams(market=condition_id)) or []
    for order in orders:
        if order.get("asset_id") == token_id and order.get("side", "").upper() == "BUY":
            return order.get("id") or order.get("orderID")
    return None


def order_is_open(client: ClobClient | None, condition_id: str, order_id: str) -> bool:
    if client is None or DRY_RUN:
        return True
    orders = client.get_open_orders(OpenOrderParams(market=condition_id)) or []
    return any((order.get("id") or order.get("orderID")) == order_id for order in orders)


def get_filled_shares(client: ClobClient | None, order_id: str) -> float:
    if client is None or DRY_RUN:
        return ORDER_SIZE
    details = client.get_order(order_id)
    if isinstance(details, dict):
        for field in ("size_matched", "sizeMatched", "sizeFilled", "size_filled", "filledSize"):
            if details.get(field) is not None:
                return float(details[field])
    return 0.0


def cancel_order(client: ClobClient | None, order_id: str, label: str) -> bool:
    if client is None or DRY_RUN:
        log.info("[DRY RUN] CANCEL %s | order_id=%s", label, order_id)
        return True
    try:
        client.cancel(order_id)
        log.info("Cancelled %s | order_id=%s", label, order_id)
        return True
    except Exception as exc:
        log.warning("Cancel failed for %s | order_id=%s | %s", label, order_id, exc)
        return False


def place_order(client: ClobClient | None, token_id: str, label: str, price: float, size: float, side: Side) -> str | None:
    side_name = "BUY" if side == Side.BUY else "SELL"
    if DRY_RUN:
        dry_id = f"dry-{side_name.lower()}-{int(time.time() * 1000)}"
        log.info("[DRY RUN] LIMIT %s %s | price=$%.4f | size=%s | order_id=%s", side_name, label, price, size, dry_id)
        return dry_id
    if client is None:
        raise RuntimeError("CLOB client is required when DRY_RUN=false")

    response = client.create_and_post_order(
        order_args=OrderArgs(token_id=token_id, price=price, size=size, side=side),
        order_type=OrderType.GTC,
    )
    if isinstance(response, dict) and not response.get("success", True):
        log.error("%s rejected for %s: %s", side_name, label, response.get("errorMsg", "unknown"))
        return None
    order_id = response.get("orderID") or response.get("id") or str(response)
    log.info("LIMIT %s placed %s | price=$%.4f | size=%s | order_id=%s", side_name, label, price, size, order_id)
    return order_id


def place_take_profit(client: ClobClient | None, market: dict[str, Any], token_id: str, label: str, entry_price: float, shares: float) -> str | None:
    sell_price = snap_price(entry_price * TAKE_PROFIT_MULTIPLIER, tick_size(client, token_id, market))
    return place_order(client, token_id, f"TP {label}", sell_price, shares, Side.SELL)


def scan_and_place(client: ClobClient | None, pending: dict[str, dict[str, Any]], sold: set[str]) -> None:
    for market in collect_markets():
        condition_id = market.get("conditionId") or market.get("condition_id") or ""
        question = str(market.get("question") or market.get("slug"))
        for outcome, token_id in market_outcomes(market):
            key = f"{condition_id}:{token_id}"
            label = f"{question} | {outcome}"
            if key in sold or any(order["key"] == key for order in pending.values()):
                continue

            existing_id = open_buy_order_id(client, condition_id, token_id)
            if existing_id:
                log.info("Resuming existing BUY %s | order_id=%s", label, existing_id)
                bid, ask, ratio = spread_ratio(client, token_id)
                if bid is not None:
                    pending[existing_id] = {"key": key, "condition_id": condition_id, "token_id": token_id, "market": market, "label": label, "entry_price": bid}
                continue

            bid, ask, ratio = spread_ratio(client, token_id)
            if ratio is None:
                log.info("No usable book for %s | bid=%s ask=%s", label, bid, ask)
                continue
            if ratio <= SPREAD_RATIO_MIN:
                log.info("Spread too tight for %s | bid=%.4f ask=%.4f ratio=%.2fx", label, bid, ask, ratio)
                continue

            buy_price = snap_price(bid, tick_size(client, token_id, market))
            order_id = place_order(client, token_id, label, buy_price, ORDER_SIZE, Side.BUY)
            if order_id:
                pending[order_id] = {"key": key, "condition_id": condition_id, "token_id": token_id, "market": market, "label": label, "entry_price": buy_price}
                log.info("BUY pending while ratio stays > %.2fx | bid=%.4f ask=%.4f ratio=%.2fx", SPREAD_RATIO_MIN, bid, ask, ratio)


def monitor_pending(client: ClobClient | None, pending: dict[str, dict[str, Any]], sold: set[str]) -> None:
    for order_id, order in list(pending.items()):
        label = order["label"]
        token_id = order["token_id"]
        condition_id = order["condition_id"]

        if not order_is_open(client, condition_id, order_id):
            shares = get_filled_shares(client, order_id)
            pending.pop(order_id, None)
            if shares > 0:
                log.info("BUY filled %s | shares=%s | entry=$%.4f", label, shares, order["entry_price"])
                place_take_profit(client, order["market"], token_id, label, order["entry_price"], shares)
                sold.add(order["key"])
            else:
                log.info("BUY gone with 0 fill %s | order_id=%s", label, order_id)
            continue

        bid, ask, ratio = spread_ratio(client, token_id)
        if ratio is None:
            log.info("Keeping %s; no usable updated book | bid=%s ask=%s", label, bid, ask)
            continue
        if ratio < SPREAD_RATIO_MIN:
            log.info("Spread contracted below %.2fx for %s | bid=%.4f ask=%.4f ratio=%.2fx", SPREAD_RATIO_MIN, label, bid, ask, ratio)
            if cancel_order(client, order_id, label):
                pending.pop(order_id, None)


def main() -> None:
    log.info("Polymarket World Cup Exact Score Spread Bot")
    log.info(
        "Mode=%s | current-day exact-score only | spread>%.2fx | buy at best bid | TP=%.2fx | size=%s",
        "DRY_RUN" if DRY_RUN else "LIVE",
        SPREAD_RATIO_MIN,
        TAKE_PROFIT_MULTIPLIER,
        ORDER_SIZE,
    )

    client = build_client()
    pending: dict[str, dict[str, Any]] = {}
    sold: set[str] = set()

    while True:
        scan_and_place(client, pending, sold)
        monitor_pending(client, pending, sold)
        if RUN_ONCE:
            break
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
