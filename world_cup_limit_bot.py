"""
Polymarket World Cup Exact Score Spread Bot
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...

    WORLD_CUP_MARKET_SLUGS=slug1,slug2        # optional; must be exact-score markets
    # Match slug rule: fifwc-{home FIFA code}-{away FIFA code}-{YYYY-MM-DD}, lower-case
    # Example from CZE vs RSA on 2026-06-18: world-cup/fifwc-cze-rsa-2026-06-18
    WORLD_CUP_MATCH_SLUGS=world-cup/fifwc-cze-rsa-2026-06-18  # sports slug, /event URL, bare event slug, path, or URL
    WORLD_CUP_EVENT_SLUGS=event1,event2       # optional alias for match/event slugs
    WORLD_CUP_EXACT_SCORE_OUTCOMES=           # optional CSV, e.g. "1-0,2-1"; empty = all outcomes
    WORLD_CUP_ORDER_SIZE=5
    WORLD_CUP_SPREAD_RATIO_MIN=1.8
    WORLD_CUP_TAKE_PROFIT_MULTIPLIER=2
    WORLD_CUP_POSITION_MONITOR=true          # detect existing World Cup positions and place TP sell tranches
    WORLD_CUP_POSITION_WALLET=0x...          # optional; defaults to POLY_FUNDER_ADDRESS
    WORLD_CUP_POSITION_TP_MULTIPLIERS=20,60,150
    WORLD_CUP_POSITION_TP_CAP=0.90
    WORLD_CUP_POSITION_MIN_NEW_SHARES=10  # minimum newly bought shares before placing another TP tranche set
    WORLD_CUP_POSITION_MIN_TRANCHE_SHARES=5  # Polymarket CLOB minimum order size for each SELL tranche
    WORLD_CUP_MAX_OUTCOMES_PER_MARKET=40
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
DATA_API = "https://data-api.polymarket.com"
USER_AGENT = "poly-bot/world-cup-limit-bot (+https://github.com/cemini23/world-cup-bot configured-markets)"

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MARKET_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_MARKET_SLUGS", "").split(",") if s.strip()]
MATCH_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_MATCH_SLUGS", "").split(",") if s.strip()]
EVENT_SLUGS = [s.strip() for s in os.getenv("WORLD_CUP_EVENT_SLUGS", "").split(",") if s.strip()]
OUTCOME_FILTERS = [s.strip().lower() for s in os.getenv("WORLD_CUP_EXACT_SCORE_OUTCOMES", "").split(",") if s.strip()]
ORDER_SIZE = float(os.getenv("WORLD_CUP_ORDER_SIZE", os.getenv("ORDER_SIZE", "5")))
SPREAD_RATIO_MIN = float(os.getenv("WORLD_CUP_SPREAD_RATIO_MIN", "1.5"))
TAKE_PROFIT_MULTIPLIER = float(os.getenv("WORLD_CUP_TAKE_PROFIT_MULTIPLIER", "2"))
POSITION_MONITOR = os.getenv("WORLD_CUP_POSITION_MONITOR", "true").lower() == "true"
POSITION_WALLET = os.getenv("WORLD_CUP_POSITION_WALLET", os.getenv("POLY_FUNDER_ADDRESS", "")).strip()
POSITION_TP_MULTIPLIERS = [
    float(s.strip())
    for s in os.getenv("WORLD_CUP_POSITION_TP_MULTIPLIERS", "20,60,150").split(",")
    if s.strip()
]
POSITION_TP_CAP = float(os.getenv("WORLD_CUP_POSITION_TP_CAP", "0.90"))
POSITION_MIN_NEW_SHARES = float(os.getenv("WORLD_CUP_POSITION_MIN_NEW_SHARES", "10"))
POSITION_MIN_TRANCHE_SHARES = float(os.getenv("WORLD_CUP_POSITION_MIN_TRANCHE_SHARES", "5"))
MAX_OUTCOMES_PER_MARKET = int(os.getenv("WORLD_CUP_MAX_OUTCOMES_PER_MARKET", "40"))
POLL_SECS = int(os.getenv("WORLD_CUP_POLL_SECS", "60"))
DAY_TZ_OFFSET = int(os.getenv("WORLD_CUP_DAY_TZ_OFFSET", "0"))
SKIP_EXISTING = os.getenv("WORLD_CUP_SKIP_EXISTING", "true").lower() == "true"
RUN_ONCE = os.getenv("WORLD_CUP_RUN_ONCE", "false").lower() == "true"
FIFWC_EVENT_RE = re.compile(r"fifwc-[a-z0-9]+-[a-z0-9]+-(\d{4})-(\d{2})-(\d{2})", re.IGNORECASE)
# Match score lines like "0-0", "0 - 0", or "10–9" without treating the
# month/day portion of dates like "2026-06-18" as a score.
SCORE_OUTCOME_RE = re.compile(r"(?<![\d.][-–])(?<!\d{4}[-–])\b\d{1,2}\s*[-–]\s*\d{1,2}\b(?![-–]\d)(?!\.\d)")
# Match exact/correct score variants with optional punctuation (handles "exact-score", "Exact Score", "exact.score", etc.)
EXACT_KW_RE = re.compile(r"exact.?score|correct.?score", re.I)

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
        response = requests.get(
            f"{GAMMA_API}{path}",
            params=params,
            timeout=15,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        log.error("Gamma API request failed for %s with %s: %s", path, params, exc)
        return []

def match_slug_from_fixture(home_code: str, away_code: str, match_date: str) -> str:
    """Build a Polymarket World Cup match slug from FIFA codes and date.

    World Cup sports pages use lower-case FIFA team codes in match order:
    fifwc-{home}-{away}-{YYYY-MM-DD}.
    """
    return f"fifwc-{home_code.strip().lower()}-{away_code.strip().lower()}-{match_date.strip()}"

def sports_slug_from_fixture(home_code: str, away_code: str, match_date: str) -> str:
    """Build the Gamma /sports slug for a World Cup fixture."""
    return f"world-cup/{match_slug_from_fixture(home_code, away_code, match_date)}"

def sports_slug_from_value(value: str) -> str:
    """Return the Gamma sports slug, e.g. world-cup/fifwc-cze-rsa-2026-06-18."""
    cleaned = value.strip().strip("/")
    parsed = urlparse(cleaned)
    path = parsed.path.strip("/") if parsed.scheme or parsed.netloc else cleaned.split("?", 1)[0].strip("/")
    if path.startswith("sports/"):
        path = path[len("sports/"):]
    if path.startswith("event/"):
        path = path[len("event/"):]
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

def market_datetime(market: dict[str, Any]) -> datetime | None:
    # Sports markets often use startDate/startDateIso for market creation or
    # listing dates, while gameStartTime/endDate carry the actual fixture time.
    for key in (
        "gameStartTime",
        "game_start_time",
        "endDateIso",
        "end_date_iso",
        "endDate",
        "startDateIso",
        "start_date_iso",
        "startDate",
    ):
        market_dt = parse_dt(market.get(key))
        if market_dt is not None:
            return market_dt
    return event_date_from_slug(str(market.get("_event_slug") or market.get("slug") or ""))

def is_upcoming_market(market: dict[str, Any]) -> bool:
    # Slug dates only encode the match date, not kickoff time. Compare against the
    # configured local-day start so today's remaining matches are still eligible.
    start, _ = local_day_window_utc()
    market_dt = market_datetime(market)
    if market_dt is None:
        return False
    return market_dt >= start

def is_world_cup_market(market: dict[str, Any]) -> bool:
    event_slug = str(market.get("_event_slug") or market.get("slug") or "").lower()
    text = " ".join(
        str(market.get(k, ""))
        for k in ("question", "slug", "description", "groupItemTitle", "_event_title")
    ).lower()
    return event_slug.startswith("fifwc-") or "world cup" in text or "fifa world cup" in text

def token_ids_from_market(market: dict[str, Any]) -> list[str]:
    token_ids = parse_json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))
    if token_ids:
        return token_ids
    tokens = market.get("tokens") or market.get("clobTokens") or []
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except json.JSONDecodeError:
            tokens = []
    if isinstance(tokens, list):
        ids: list[str] = []
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = token.get("token_id") or token.get("tokenId") or token.get("asset_id") or token.get("assetId") or token.get("id")
            if token_id:
                ids.append(str(token_id).strip())
        return ids
    return []

def outcomes_from_market(market: dict[str, Any]) -> list[str]:
    outcomes = parse_json_list(market.get("outcomes"))
    if outcomes:
        return outcomes
    tokens = market.get("tokens") or market.get("clobTokens") or []
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except json.JSONDecodeError:
            tokens = []
    if isinstance(tokens, list):
        parsed: list[str] = []
        for token in tokens:
            if isinstance(token, dict):
                outcome = token.get("outcome") or token.get("name")
                if outcome:
                    parsed.append(str(outcome).strip())
        return parsed
    return []

def has_exact_score_outcomes(market: dict[str, Any]) -> bool:
    outcomes = outcomes_from_market(market)
    # Accept "Any Other Score" variants
    if any("any other" in outcome.lower() for outcome in outcomes):
        return True
    # Count score-like outcomes (e.g. "1-0")
    score_like = sum(1 for outcome in outcomes if SCORE_OUTCOME_RE.search(outcome))
    if score_like >= 3:
        return True
    # Handle binary markets where each score is a separate YES/NO market and the question contains the score
    question = str(market.get("question") or "")
    if {o.lower() for o in outcomes} >= {"yes", "no"} and SCORE_OUTCOME_RE.search(question):
        return True
    return False

def is_exact_score_world_cup_market(market: dict[str, Any]) -> bool:
    sports_market_type = str(market.get("sportsMarketType") or market.get("sports_market_type") or "").lower()
    if sports_market_type and "exact_score" not in sports_market_type and "correct_score" not in sports_market_type:
        return False
    text = " ".join(
        str(market.get(k, ""))
        for k in ("question", "slug", "description", "groupItemTitle", "_event_title")
    ).lower()
    # normalize common punctuation to improve substring/regex matches
    normalized_text = text.replace("-", " ").replace("–", " ").replace("_", " ")
    # match "exact score" / "correct score" allowing hyphens/dots/spaces
    text_match = bool(EXACT_KW_RE.search(text)) or "any other score" in text or "actual score is not" in text
    score_in_market_text = SCORE_OUTCOME_RE.search(text) is not None
    return is_world_cup_market(market) and (text_match or score_in_market_text or has_exact_score_outcomes(market))

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
    market_slug = slug_from_value(slug)

    # Docs-supported market slug forms:
    #   GET /markets?slug={slug}
    #   GET /markets/slug/{slug}
    for payload in (
        gamma_get("/markets", slug=market_slug),
        gamma_get(f"/markets/slug/{market_slug}"),
    ):
        markets = as_list_payload(payload, "markets")
        if markets:
            return markets[0]

    # If the supplied slug is actually an event slug, pull the first nested market.
    for payload in (
        gamma_get("/events", slug=market_slug),
        gamma_get(f"/events/slug/{market_slug}"),
    ):
        event_list = as_list_payload(payload, "events")
        if event_list:
            nested = event_list[0].get("markets") or []
            return nested[0] if nested else None
    return None

def looks_like_market(data: dict[str, Any]) -> bool:
    return bool(
        token_ids_from_market(data)
        or (outcomes_from_market(data) and (data.get("conditionId") or data.get("condition_id") or data.get("slug")))
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
        exact_sports_markets = [market for market in sports_markets if is_exact_score_world_cup_market(market)]
        if exact_sports_markets:
            return sports_markets

    event_slug = slug_from_value(slug)
    event_payloads = (
        gamma_get("/events", slug=event_slug),
        gamma_get(f"/events/slug/{event_slug}"),
    )
    event_list: list[dict[str, Any]] = []
    for payload in event_payloads:
        event_list = as_list_payload(payload, "events")
        if event_list:
            break
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

    if candidates:
        log.info("Scanning %s candidate market(s) from configured World Cup market sources", len(candidates))

    for market in candidates:
        key = market.get("conditionId") or market.get("condition_id") or market.get("slug") or ""
        if not key or key in seen:
            continue
        if not is_exact_score_world_cup_market(market):
            log.info("Skipping non-exact-score/non-World-Cup market: %s | question=%s | outcomes=%s", market.get("slug"), market.get("question"), market.get("outcomes"))
            continue
        if not is_upcoming_market(market):
            log.info("Skipping past/non-upcoming market: %s", market.get("slug"))
            continue
        if not token_ids_from_market(market):
            log.info("Skipping market without CLOB tokens: %s | token_fields=%s", market.get("slug"), list(market.keys()))
            continue
        markets.append(market)
        seen.add(key)

    if not markets:
        log.debug("No configured World Cup exact-score CLOB markets found after filtering")
    else:
        log.info("Found %s configured World Cup exact-score market(s)", len(markets))
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
    outcomes = outcomes_from_market(market)
    token_ids = token_ids_from_market(market)
    if len(outcomes) != len(token_ids) or not token_ids:
        raise ValueError(f"Cannot match outcomes to token IDs for {market.get('slug')}")

    pairs = [(outcome, token_id) for outcome, token_id in zip(outcomes, token_ids, strict=True)]
    if OUTCOME_FILTERS:
        pairs = [(outcome, token_id) for outcome, token_id in pairs if outcome.lower() in OUTCOME_FILTERS]
    elif {outcome.lower() for outcome in outcomes} >= {"yes", "no"}:
        # Sports exact-score tabs commonly expose each score as a binary market.
        # In that shape, the exact-score bet is the YES token for that score.
        pairs = [(outcome, token_id) for outcome, token_id in pairs if outcome.lower() == "yes"]
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

    try:
        response = client.create_and_post_order(
            order_args=OrderArgs(token_id=token_id, price=price, size=size, side=side),
            order_type=OrderType.GTC,
        )
    except Exception as exc:
        log.error("%s failed for %s | price=$%.4f | size=%s | %s", side_name, label, price, size, exc)
        return None
    if isinstance(response, dict) and not response.get("success", True):
        log.error("%s rejected for %s: %s", side_name, label, response.get("errorMsg", "unknown"))
        return None
    order_id = response.get("orderID") or response.get("id") or str(response)
    log.info("LIMIT %s placed %s | price=$%.4f | size=%s | order_id=%s", side_name, label, price, size, order_id)
    return order_id

def place_take_profit(client: ClobClient | None, market: dict[str, Any], token_id: str, label: str, entry_price: float, shares: float) -> str | None:
    sell_price = snap_price(entry_price * TAKE_PROFIT_MULTIPLIER, tick_size(client, token_id, market))
    return place_order(client, token_id, f"TP {label}", sell_price, shares, Side.SELL)

def data_get(path: str, **params: Any) -> Any:
    try:
        response = requests.get(
            f"{DATA_API}{path}",
            params=params,
            timeout=15,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        log.error("Data API request failed for %s with %s: %s", path, params, exc)
        return []

def as_float(raw: Any, default: float = 0.0) -> float:
    try:
        if raw is None:
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default

def account_positions(wallet: str) -> list[dict[str, Any]]:
    if not wallet:
        return []
    payload = data_get("/positions", user=wallet, limit=500)
    return payload if isinstance(payload, list) else []

def position_token_id(position: dict[str, Any]) -> str:
    for key in ("asset", "assetId", "asset_id", "tokenId", "token_id"):
        if position.get(key):
            return str(position[key]).strip()
    return ""

def position_size(position: dict[str, Any]) -> float:
    for key in ("size", "shares", "balance", "quantity"):
        size = as_float(position.get(key))
        if size > 0:
            return size
    return 0.0

def position_entry_price(position: dict[str, Any]) -> float:
    for key in ("avgPrice", "avg_price", "averagePrice", "average_price", "price"):
        price = as_float(position.get(key))
        if price > 0:
            return price
    return 0.0

def position_market_slug(position: dict[str, Any]) -> str:
    for key in ("marketSlug", "market_slug", "slug"):
        if position.get(key):
            return str(position[key]).strip()
    return ""

def is_world_cup_position(position: dict[str, Any]) -> bool:
    text = " ".join(
        str(position.get(key, ""))
        for key in (
            "title",
            "eventTitle",
            "eventSlug",
            "market",
            "marketSlug",
            "slug",
            "conditionId",
            "outcome",
        )
    ).lower()
    return "world cup" in text or "fifa world cup" in text or "fifwc-" in text

def order_size(order: dict[str, Any]) -> float:
    for key in ("size", "original_size", "originalSize", "remaining_size", "remainingSize"):
        size = as_float(order.get(key))
        if size > 0:
            return size
    return 0.0

def open_sell_order_size(client: ClobClient | None, condition_id: str, token_id: str) -> float:
    if client is None or DRY_RUN:
        return 0.0
    try:
        orders = client.get_open_orders(OpenOrderParams(market=condition_id)) or []
    except Exception as exc:
        log.warning("Open SELL check failed for %s: %s", token_id, exc)
        return 0.0
    return sum(
        order_size(order)
        for order in orders
        if str(order.get("asset_id") or order.get("assetId") or "") == token_id
        and str(order.get("side", "")).upper() == "SELL"
    )

def tranche_sizes(shares: float, tranches: int, min_size: float = 0.0) -> list[float]:
    if tranches <= 1:
        return [shares] if shares >= min_size else []

    rounded_shares = math.floor(shares)
    if rounded_shares <= 0:
        return []

    tranche_count = tranches
    if min_size > 0:
        tranche_count = min(tranches, math.floor(rounded_shares / min_size))
        if tranche_count <= 0:
            return []

    first = math.floor(rounded_shares / tranche_count)
    sizes = [first for _ in range(tranche_count - 1)]
    sizes.append(rounded_shares - sum(sizes))
    return [float(size) for size in sizes if size >= min_size]

def place_position_sell_tranches(
    client: ClobClient | None,
    market: dict[str, Any],
    token_id: str,
    label: str,
    shares: float,
    entry_price: float,
) -> float:
    multipliers = POSITION_TP_MULTIPLIERS[:3] or [20.0, 60.0, 150.0]
    if len(multipliers) < 3:
        multipliers.extend([multipliers[-1]] * (3 - len(multipliers)))
    sizes = tranche_sizes(shares, 3, POSITION_MIN_TRANCHE_SHARES)
    if not sizes:
        log.info(
            "World Cup position %s has %s share(s), below minimum SELL tranche size %.4g; skipping TP placement.",
            label,
            shares,
            POSITION_MIN_TRANCHE_SHARES,
        )
        return 0.0

    tick = tick_size(client, token_id, market)
    _, best_ask, _ = spread_ratio(client, token_id)
    placed_shares = 0.0
    log.info(
        "Placing World Cup position sell tranches for %s | shares=%s | entry=$%.4f | multipliers=%s | cap=$%.2f | best_ask=%s",
        label,
        shares,
        entry_price,
        "/".join(f"{m:g}x" for m in multipliers[: len(sizes)]),
        POSITION_TP_CAP,
        f"${best_ask:.4f}" if best_ask is not None else "n/a",
    )
    for idx, (size, multiplier) in enumerate(zip(sizes, multipliers, strict=False), 1):
        target_price = min(entry_price * multiplier, POSITION_TP_CAP)
        if best_ask is not None and best_ask > target_price:
            log.info(
                "Raising %s TP%s from $%.4f to best ask $%.4f",
                label,
                idx,
                target_price,
                best_ask,
            )
            target_price = best_ask
        price = snap_price(target_price, tick)
        order_id = place_order(client, token_id, f"{label}-TP{idx}-{multiplier:g}x", price, size, Side.SELL)
        if order_id:
            placed_shares += size
    return placed_shares

def monitor_world_cup_positions(client: ClobClient | None, protected: dict[str, float]) -> None:
    if not POSITION_MONITOR:
        return
    if DRY_RUN and not POSITION_WALLET:
        log.info("[DRY RUN] WORLD_CUP_POSITION_WALLET/POLY_FUNDER_ADDRESS not set; skipping account position monitor.")
        return
    if not POSITION_WALLET:
        log.warning("WORLD_CUP_POSITION_MONITOR=true but WORLD_CUP_POSITION_WALLET/POLY_FUNDER_ADDRESS is not set.")
        return

    for position in account_positions(POSITION_WALLET):
        if not is_world_cup_position(position):
            continue
        token_id = position_token_id(position)
        shares = position_size(position)
        entry_price = position_entry_price(position)
        if not token_id or shares <= 0 or entry_price <= 0:
            log.info("Skipping World Cup position with incomplete data: %s", position)
            continue
        market_slug = position_market_slug(position)
        market = fetch_market_by_slug(market_slug) if market_slug else None
        condition_id = str(position.get("conditionId") or position.get("condition_id") or "")
        if not market:
            if not condition_id:
                log.info("Skipping World Cup position without resolvable market slug/condition: %s", market_slug or token_id)
                continue
            market = {"conditionId": condition_id, "question": position.get("title") or position.get("market") or market_slug}
        condition_id = str(market.get("conditionId") or market.get("condition_id") or condition_id)
        key = f"{condition_id}:{token_id}:position-tp"
        covered_shares = max(open_sell_order_size(client, condition_id, token_id), protected.get(key, 0.0))
        newly_bought_shares = shares - covered_shares
        if newly_bought_shares < POSITION_MIN_NEW_SHARES:
            log.info(
                "World Cup position %s has %.4g uncovered new share(s), below %.4g minimum; skipping TP placement.",
                market_slug or token_id,
                max(newly_bought_shares, 0.0),
                POSITION_MIN_NEW_SHARES,
            )
            protected[key] = min(shares, covered_shares)
            continue
        label = str(position.get("title") or position.get("market") or market.get("question") or market_slug)
        placed_shares = place_position_sell_tranches(client, market, token_id, label, newly_bought_shares, entry_price)
        if placed_shares > 0:
            protected[key] = covered_shares + placed_shares

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
        "Mode=%s | configured market sources only | spread>%.2fx | buy at best bid | TP=%.2fx | size=%s",
        "DRY_RUN" if DRY_RUN else "LIVE",
        SPREAD_RATIO_MIN,
        TAKE_PROFIT_MULTIPLIER,
        ORDER_SIZE,
    )

    client = build_client()
    pending: dict[str, dict[str, Any]] = {}
    sold: set[str] = set()
    protected_positions: dict[str, float] = {}

    while True:
        monitor_world_cup_positions(client, protected_positions)
        scan_and_place(client, pending, sold)
        monitor_pending(client, pending, sold)
        if RUN_ONCE:
            break
        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
