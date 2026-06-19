"""World Cup exact-score market scanner for Polymarket Gamma.

This module is intentionally separate from ``world_cup_limit_bot.py`` so the
bot can opt into scanner-based market discovery without mixing the scanner
implementation into the trading logic.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

GAMMA_API = "https://gamma-api.polymarket.com"
USER_AGENT = "poly-bot/world-cup-market-scanner (+https://github.com/cemini23/world-cup-bot)"

SCANNER_ENABLED = os.getenv("WORLD_CUP_SCANNER_ENABLED", "true").lower() == "true"
SCANNER_TAG_ID = os.getenv("WORLD_CUP_SCANNER_TAG_ID", "102232").strip()
SCANNER_MATCHES = int(os.getenv("WORLD_CUP_SCANNER_MATCHES", "1"))
SCANNER_SCORE_MAX = int(os.getenv("WORLD_CUP_SCANNER_SCORE_MAX", "5"))
SCANNER_DAY_TZ_OFFSET = int(os.getenv("WORLD_CUP_DAY_TZ_OFFSET", "8"))
SCANNER_TIMEOUT = float(os.getenv("WORLD_CUP_SCANNER_TIMEOUT", "15"))
SCANNER_SCORES_RAW = (
    os.getenv("WORLD_CUP_SCANNER_SCORES", "").strip()
    or os.getenv("WORLD_CUP_TARGET_SCORES", "").strip()
    or os.getenv("WORLD_CUP_EXACT_SCORE_OUTCOMES", "").strip()
)
SCANNER_SCORES = [
    score.strip().replace("–", "-").replace(" ", "")
    for score in SCANNER_SCORES_RAW.split(",")
    if score.strip()
]

FIFWC_EVENT_RE = re.compile(r"fifwc-[a-z0-9]+-[a-z0-9]+-(\d{4})-(\d{2})-(\d{2})", re.IGNORECASE)
FIFWC_MATCH_SLUG_RE = re.compile(r"^fifwc-[a-z0-9]+-[a-z0-9]+-\d{4}-\d{2}-\d{2}$", re.IGNORECASE)

log = logging.getLogger(__name__)


def scanner_enabled() -> bool:
    return SCANNER_ENABLED


def gamma_get(path: str, **params: Any) -> Any:
    try:
        response = requests.get(
            f"{GAMMA_API}{path}",
            params=params,
            timeout=SCANNER_TIMEOUT,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        log.warning("World Cup scanner Gamma request failed for %s with %s: %s", path, params, exc)
        return []


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


def parse_dt(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def event_date_from_slug(slug: str) -> datetime | None:
    match = FIFWC_EVENT_RE.search(slug)
    if not match:
        return None
    year, month, day = (int(part) for part in match.groups())
    return datetime(year, month, day, tzinfo=UTC)


def local_day_window_utc() -> tuple[datetime, datetime]:
    now = datetime.now(UTC) + timedelta(hours=SCANNER_DAY_TZ_OFFSET)
    day_start_local = datetime(now.year, now.month, now.day, tzinfo=UTC)
    start_utc = day_start_local - timedelta(hours=SCANNER_DAY_TZ_OFFSET)
    return start_utc, start_utc + timedelta(days=1)


def event_datetime(event: dict[str, Any]) -> datetime | None:
    for key in ("gameStartTime", "endDateIso", "endDate", "startDateIso", "startDate"):
        event_dt = parse_dt(event.get(key))
        if event_dt is not None:
            return event_dt
    return event_date_from_slug(str(event.get("slug") or ""))


def local_time_label(dt: datetime | None) -> str:
    if dt is None:
        return "kickoff=n/a"
    sign = "+" if SCANNER_DAY_TZ_OFFSET >= 0 else "-"
    local_dt = dt.astimezone(UTC) + timedelta(hours=SCANNER_DAY_TZ_OFFSET)
    return f"{local_dt:%Y-%m-%d %H:%M} UTC{sign}{abs(SCANNER_DAY_TZ_OFFSET)}"


def event_label(event: dict[str, Any]) -> str:
    return str(event.get("title") or event.get("name") or event.get("slug") or "unknown match")


def fetch_tag_events(tag_id: str = SCANNER_TAG_ID, page_size: int = 100) -> list[dict[str, Any]]:
    if not tag_id:
        return []
    events: list[dict[str, Any]] = []
    offset = 0
    while True:
        payload = gamma_get("/events", tag_id=tag_id, closed="false", limit=page_size, offset=offset)
        page = as_list_payload(payload, "events")
        if not page:
            break
        events.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return events


def upcoming_world_cup_events(limit: int = SCANNER_MATCHES) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    upcoming: list[tuple[datetime, dict[str, Any]]] = []
    for event in fetch_tag_events():
        slug = str(event.get("slug") or "")
        title = str(event.get("title") or "")
        title_lower = title.lower()
        is_match_slug = FIFWC_MATCH_SLUG_RE.fullmatch(slug) is not None
        if not is_match_slug and "world cup" not in title_lower:
            continue
        if "player prop" in title_lower or "player-prop" in slug.lower():
            continue
        event_dt = event_datetime(event)
        if event_dt is None or event_dt < now:
            continue
        upcoming.append((event_dt, event))
    upcoming.sort(key=lambda item: item[0])
    return [event for _, event in upcoming[:limit]]


def scanner_scores() -> list[str]:
    if SCANNER_SCORES:
        return SCANNER_SCORES
    log.warning(
        "No target scores configured; set WORLD_CUP_TARGET_SCORES=1-0,2-1 to scan/place only those scores."
    )
    return []


def exact_score_market_slug(event_slug: str, score: str) -> str:
    normalized_score = score.replace("–", "-").replace(" ", "")
    return f"{event_slug}-exact-score-{normalized_score}"


def candidate_market_slugs(event_slug: str) -> list[str]:
    return [exact_score_market_slug(event_slug, score) for score in scanner_scores()]


def fetch_market_by_slug(slug: str) -> dict[str, Any] | None:
    for payload in (gamma_get("/markets", slug=slug), gamma_get(f"/markets/slug/{slug}")):
        markets = as_list_payload(payload, "markets")
        if markets:
            return markets[0]
    return None


def market_key(market: dict[str, Any]) -> str:
    return str(market.get("conditionId") or market.get("condition_id") or market.get("slug") or id(market))


def scan_world_cup_exact_score_markets(limit: int = SCANNER_MATCHES) -> list[dict[str, Any]]:
    if not scanner_enabled():
        return []

    markets: list[dict[str, Any]] = []
    seen: set[str] = set()
    events = upcoming_world_cup_events(limit)
    event_summaries: list[str] = []
    for event in events:
        event_slug = str(event.get("slug") or "")
        if not event_slug:
            continue
        label = event_label(event)
        kickoff = event_datetime(event)
        event_summaries.append(f"{label} ({local_time_label(kickoff)})")
        log.info("World Cup scanner upcoming match: %s | %s", label, local_time_label(kickoff))
        for market_slug in candidate_market_slugs(event_slug):
            market = fetch_market_by_slug(market_slug)
            if not market:
                continue
            key = market_key(market)
            if key in seen:
                continue
            seen.add(key)
            market["_event_slug"] = event_slug
            market["_event_title"] = event.get("title") or ""
            if event.get("endDate") and not market.get("endDate"):
                market["endDate"] = event.get("endDate")
            markets.append(market)
    log.info(
        "World Cup scanner found %s exact-score market(s) across %s upcoming match(es): %s",
        len(markets),
        len(events),
        "; ".join(event_summaries) if event_summaries else "none",
    )
    return markets


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    print(json.dumps(scan_world_cup_exact_score_markets(), indent=2))
