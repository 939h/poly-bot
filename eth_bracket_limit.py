"""
Polymarket ETH Daily Bracket Limit Order Bot
=============================================
Strategy:
  On startup and on each new UTC day, place GTC limit BUY orders on the two
  price brackets *outside* the current ETH 100-unit range:

    upper bracket = ceil(eth / 100) * 100    e.g. $2333 → 2400
    lower bracket = floor(eth / 100) * 100 - 100          → 2200

  Upper bracket ("ETH above 2400?") → BUY YES at ORDER_PRICE (cheap ~4%)
  Lower bracket ("ETH above 2200?") → BUY NO  at ORDER_PRICE (cheap ~1%)

  Orders are placed for TODAY and TOMORROW (tomorrow may not be live yet —
  silently skipped if market not found).

  After a BUY fills, three tiered GTC limit SELL orders are placed:
    TP1  50% of shares  at max(buy_price * 2, best_ask)   ← 2x or better
    TP2  25% of shares  at buy_price * 10
    TP3  remaining 25%  at buy_price * 50

  BUY orders are cancelled after CANCEL_AFTER_HOURS if unfilled.
  SELL orders are placed-and-forget.

Requirements:
    pip install py-clob-client python-dotenv requests colorama

.env keys:
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
    DRY_RUN=true
    ORDER_PRICE=0.004
    ORDER_SIZE=300
"""

import math
import os
import sys
import json
import time
import logging
import threading
import requests
from datetime import datetime, timezone, timedelta, date

from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        OrderArgs,
        OpenOrderParams,
        OrderType,
        ApiCreds,
    )
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client python-dotenv requests colorama")
    sys.exit(1)

try:
    import colorama
    from colorama import Fore, Style
    colorama.init()
    _COLORS = True
except ImportError:
    _COLORS = False

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
ORDER_PRICE        = float(os.getenv("ORDER_PRICE", "0.01"))    # $ per share — min tick is $0.01
ORDER_SIZE         = int(os.getenv("ORDER_SIZE", "300"))        # shares per side
CANCEL_AFTER_HOURS = 4       # cancel unfilled BUY orders after this many hours
POLL_SECS          = 60      # fill-check interval
LOOP_SLEEP         = 30      # main-loop tick (seconds)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"

# ── Logging ───────────────────────────────────────────────────────────────────

class _ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if not _COLORS:
            return msg
        if "[DRY RUN]" in msg:
            return Fore.CYAN + msg + Style.RESET_ALL
        if any(t in msg for t in ("FILLED", "SELL TP", "placed")):
            return Fore.GREEN + msg + Style.RESET_ALL
        if any(t in msg for t in ("CANCEL", "ERROR", "error", "not found")):
            return Fore.RED + msg + Style.RESET_ALL
        if "WARNING" in msg or "warn" in msg.lower():
            return Fore.YELLOW + msg + Style.RESET_ALL
        return msg

_fmt = "%(asctime)s [%(levelname)s] %(message)s"
_handler = logging.StreamHandler(
    open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
)
_handler.setFormatter(_ColorFormatter(_fmt))
_file_handler = logging.FileHandler("eth_bracket_limit.log", encoding="utf-8")
_file_handler.setFormatter(logging.Formatter(_fmt))
logging.basicConfig(level=logging.INFO, handlers=[_handler, _file_handler], force=True)
log = logging.getLogger(__name__)

# ── Binance ───────────────────────────────────────────────────────────────────

def fetch_eth_spot() -> float:
    """Return current ETH/USDT spot price from Binance public REST API.
    Override with ETH_PRICE env var for testing."""
    override = os.getenv("ETH_PRICE")
    if override:
        price = float(override)
        log.info(f"ETH price (override): ${price:,.2f}")
        return price
    url = "https://api.binance.com/api/v3/ticker/price"
    r = requests.get(url, params={"symbol": "ETHUSDT"}, timeout=10)
    r.raise_for_status()
    price = float(r.json()["price"])
    log.info(f"Binance ETH spot: ${price:,.2f}")
    return price

# ── Bracket ───────────────────────────────────────────────────────────────────

def get_brackets(eth_price: float) -> tuple[int, int]:
    """
    Return (upper_bracket, lower_bracket) based on ETH spot price.

    Example: eth_price = 2333
      upper = ceil(2333 / 100) * 100       = 2400
      lower = floor(2333 / 100) * 100 - 100 = 2200
    """
    upper = int(math.ceil(eth_price / 100) * 100)
    lower = int(math.floor(eth_price / 100) * 100) - 100
    log.info(f"Brackets: upper={upper}, lower={lower} (ETH ${eth_price:,.2f})")
    return upper, lower

# ── Slug / Market ─────────────────────────────────────────────────────────────

def build_slug(bracket: int, target_date: date) -> str:
    """
    Build Polymarket slug for an 'Ethereum above X on [date]' market.
    Format: ethereum-above-{bracket}-on-{month}-{day}
    Example: ethereum-above-2400-on-april-16
    """
    date_str = target_date.strftime("%B-%-d").lower()
    return f"ethereum-above-{bracket}-on-{date_str}"


def fetch_market(slug: str) -> dict | None:
    """Fetch market data from Gamma API by slug; fallback to /events."""
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
        r.raise_for_status()
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if markets:
            return markets[0]
        r2 = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
        r2.raise_for_status()
        events = r2.json()
        if isinstance(events, list) and events:
            nested = events[0].get("markets", [])
            if nested:
                return nested[0]
        return None
    except Exception as e:
        log.error(f"Gamma API error ({slug}): {e}")
        return None


def extract_tokens(market: dict) -> tuple[str | None, str | None]:
    """Return (yes_token_id, no_token_id) from a market dict."""
    raw = market.get("clobTokenIds") or market.get("clob_token_ids", [])
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None, None
    if not raw or len(raw) < 2:
        return None, None
    return str(raw[0]).strip(), str(raw[1]).strip()

# ── Polymarket Client ─────────────────────────────────────────────────────────

def build_client() -> ClobClient | None:
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        if DRY_RUN:
            log.info("[DRY RUN] No .env credentials — running without CLOB client.")
            return None
        log.error("Missing .env credentials (POLY_PRIVATE_KEY / POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE).")
        sys.exit(1)
    client = ClobClient(
        host=CLOB_API,
        chain_id=POLYGON,
        key=pk,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_sec,
            api_passphrase=api_pass,
        ),
        signature_type=1,
        funder=funder or None,
    )
    log.info("Connected to Polymarket CLOB.")
    return client

# ── Order Helpers ─────────────────────────────────────────────────────────────

def place_limit_order(
    client: ClobClient,
    token_id: str,
    label: str,
    price: float,
    size: int,
    side,
) -> str | None:
    """
    Place a GTC limit order. Returns order_id on success, None on failure.
    side: BUY or SELL constant from py_clob_client.
    """
    side_str = "BUY" if side == BUY else "SELL"
    cost = round(price * size, 4)

    if DRY_RUN:
        log.info(
            f"[DRY RUN] LIMIT {side_str} {label}"
            f" | price=${price} | size={size} | cost=${cost:.4f} USDC"
        )
        return f"dry-{label.lower().replace(' ', '-')}-{int(time.time())}"

    try:
        order = client.create_order(
            OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )
        )
        resp = client.post_order(order, OrderType.GTC)
        order_id = resp.get("orderID") or resp.get("id") or str(resp)
        log.info(
            f"  Limit {side_str} placed: {label}"
            f" | price=${price} | size={size} | order_id={order_id}"
        )
        return order_id
    except Exception as e:
        log.error(f"  Limit {side_str} failed ({label}): {e}")
        return None


def get_best_ask(client: ClobClient, token_id: str) -> float | None:
    """Fetch the best (lowest) ask price from the orderbook."""
    try:
        book = client.get_order_book(token_id)
        asks = None
        if isinstance(book, dict):
            asks = book.get("asks") or book.get("ask")
        else:
            asks = getattr(book, "asks", None)
        if not asks:
            return None
        prices = []
        for entry in asks:
            if isinstance(entry, dict):
                p = entry.get("price") or entry.get("p")
            else:
                p = getattr(entry, "price", None)
            if p is not None:
                prices.append(float(p))
        return min(prices) if prices else None
    except Exception as e:
        log.warning(f"  get_best_ask failed for {token_id[:12]}: {e}")
        return None


def get_filled_shares(client: ClobClient, order_id: str) -> int:
    """Get number of shares filled on a GTC order via get_order()."""
    try:
        details = client.get_order(order_id)
        if isinstance(details, dict):
            raw = (
                details.get("sizeFilled")
                or details.get("size_filled")
                or details.get("filledSize")
                or 0
            )
        else:
            raw = getattr(details, "size_filled", 0) or getattr(details, "sizeFilled", 0)
        return int(float(raw))
    except Exception as e:
        log.warning(f"  get_filled_shares failed for {order_id}: {e}")
        return ORDER_SIZE  # assume full fill as fallback


def place_sell_tranches(
    client: ClobClient,
    token_id: str,
    label: str,
    filled_shares: int,
    buy_price: float,
) -> None:
    """
    Place 3 tiered GTC limit SELL orders after a BUY fills.

    TP1  50% shares  at max(buy_price * 2, best_ask)
    TP2  25% shares  at buy_price * 10
    TP3  remaining   at buy_price * 50
    """
    tp1_shares = math.floor(filled_shares * 0.50)
    tp2_shares = math.floor(filled_shares * 0.25)
    tp3_shares = filled_shares - tp1_shares - tp2_shares

    # TP1: check best ask; sell at whichever is higher
    tp1_base = round(buy_price * 2, 4)
    if not DRY_RUN:
        best_ask = get_best_ask(client, token_id)
        if best_ask is not None and best_ask > tp1_base:
            log.info(
                f"  TP1 price adjusted: best_ask={best_ask:.4f}"
                f" > 2x={tp1_base:.4f} → using best_ask"
            )
            tp1_base = round(best_ask, 4)
    else:
        log.info(f"  [DRY RUN] TP1 would check orderbook; using 2x=${tp1_base}")

    log.info(
        f"  Placing sell tranches for {label}"
        f" | filled={filled_shares} shares | buy_price=${buy_price}"
    )
    place_limit_order(client, token_id, f"{label}-TP1", tp1_base,  tp1_shares, SELL)
    place_limit_order(client, token_id, f"{label}-TP2", round(buy_price * 10, 4), tp2_shares, SELL)
    place_limit_order(client, token_id, f"{label}-TP3", round(buy_price * 50, 4), tp3_shares, SELL)


def get_open_order_ids(client: ClobClient, condition_id: str) -> set[str]:
    """Return set of open order IDs for this market condition."""
    if DRY_RUN:
        return set()
    try:
        open_orders = client.get_orders(OpenOrderParams(market=condition_id))
        if isinstance(open_orders, list):
            return {o.get("id") or o.get("orderID", "") for o in open_orders}
        return set()
    except Exception as e:
        log.warning(f"  Could not fetch open orders: {e}")
        return set()


def cancel_order(client: ClobClient, order_id: str, label: str) -> None:
    """Cancel a single order by ID."""
    if DRY_RUN:
        log.info(f"  [DRY RUN] CANCEL {label} order_id={order_id}")
        return
    try:
        client.cancel(order_id)
        log.info(f"  CANCELLED {label} order_id={order_id}")
    except Exception as e:
        log.error(f"  Cancel failed ({label} / {order_id}): {e}")

# ── Monitor + Sell ────────────────────────────────────────────────────────────

def monitor_and_sell(client: ClobClient, orders: list[dict]) -> None:
    """
    Poll open orders. On fill → place sell tranches. Cancel unfilled at deadline.

    Each entry in orders:
        {label, order_id, condition_id, token_id, buy_price}
    """
    if not orders:
        return

    if DRY_RUN:
        log.info(f"[DRY RUN] Skipping monitor loop ({len(orders)} orders).")
        # Simulate sell tranches in dry run so output is visible
        for o in orders:
            place_sell_tranches(client, o["token_id"], o["label"], ORDER_SIZE, o["buy_price"])
        return

    deadline = time.time() + CANCEL_AFTER_HOURS * 3600
    pending  = {o["order_id"]: o for o in orders if o.get("order_id")}

    if not pending:
        log.warning("No valid order IDs to monitor.")
        return

    log.info(
        f"Monitoring {len(pending)} order(s) — "
        f"cancel deadline in {CANCEL_AFTER_HOURS}h"
    )

    while pending:
        time.sleep(POLL_SECS)
        now = time.time()

        # Group by condition_id to minimise API calls
        by_condition: dict[str, list[str]] = {}
        for oid, o in pending.items():
            by_condition.setdefault(o["condition_id"], []).append(oid)

        for cid, oids in by_condition.items():
            open_ids = get_open_order_ids(client, cid)
            for oid in oids:
                if oid not in open_ids and oid in pending:
                    o = pending.pop(oid)
                    filled = get_filled_shares(client, oid)
                    if filled > 0:
                        log.info(f"  FILLED ✓ {o['label']} | {filled} shares | order_id={oid}")
                        place_sell_tranches(client, o["token_id"], o["label"], filled, o["buy_price"])
                    else:
                        log.info(f"  Order {o['label']} gone (0 fill) — skipping sell.")

        if not pending:
            log.info("All orders filled or gone.")
            return

        if now >= deadline:
            log.warning(
                f"Timeout — cancelling {len(pending)} unfilled order(s): "
                + ", ".join(pending[oid]["label"] for oid in pending)
            )
            for oid, o in list(pending.items()):
                cancel_order(client, oid, o["label"])
            return

        remaining_min = (deadline - now) / 60
        log.info(
            f"Waiting on {[pending[oid]['label'] for oid in pending]} | "
            f"{remaining_min:.0f}m until cancel"
        )

# ── Per-date placement ────────────────────────────────────────────────────────

def place_for_date(
    client: ClobClient,
    target_date: date,
    upper: int,
    lower: int,
    skip_slugs: set[str] | None = None,
) -> list[dict]:
    """
    For a given date, place BUY limit orders on the upper (YES) and lower (NO)
    brackets. Returns a list of order dicts for the monitor loop.
    Silently skips any bracket whose market is not yet live.

    skip_slugs: if provided, slugs already placed are skipped and newly placed
                slugs are added to the set (used for tomorrow retry logic).
    """
    date_label = target_date.strftime("%b %-d")
    log.info(f"--- {date_label} | upper={upper} YES | lower={lower} NO ---")
    placed = []

    brackets = [
        (upper, "YES", 0),  # (bracket, side_label, token_index)
        (lower, "NO",  1),
    ]

    for bracket, side_label, tok_idx in brackets:
        slug = build_slug(bracket, target_date)

        if skip_slugs is not None and slug in skip_slugs:
            log.info(f"  Already placed: {slug} — skipping")
            continue

        market = fetch_market(slug)
        if market is None:
            log.warning(f"  Market not found: {slug} — skipping")
            continue

        condition_id = market.get("conditionId") or market.get("condition_id", "")
        yes_tok, no_tok = extract_tokens(market)
        if not yes_tok or not no_tok:
            log.error(f"  Could not extract tokens for {slug}")
            continue

        token_id = yes_tok if tok_idx == 0 else no_tok
        label = f"{side_label} {slug}"

        order_id = place_limit_order(
            client, token_id, label, ORDER_PRICE, ORDER_SIZE, BUY
        )

        if order_id:
            placed.append({
                "label":        label,
                "order_id":     order_id,
                "condition_id": condition_id,
                "token_id":     token_id,
                "buy_price":    ORDER_PRICE,
            })
            if skip_slugs is not None:
                skip_slugs.add(slug)

    return placed

# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("ETH Daily Bracket Limit Order Bot")
    log.info(f"Mode  : {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info(f"Order : {ORDER_SIZE} shares @ ${ORDER_PRICE} each side")
    log.info(f"TP1   : 50% @ 2x = ${ORDER_PRICE * 2:.4f}  (or best ask if higher)")
    log.info(f"TP2   : 25% @ 10x = ${ORDER_PRICE * 10:.4f}")
    log.info(f"TP3   : 25% @ 50x = ${ORDER_PRICE * 50:.4f}")
    log.info("=" * 60)

    client = build_client()
    placed_dates:     set[date] = set()  # today dates fully placed
    tmrw_placed:      set[str]  = set()  # tomorrow slugs already placed (for retry)

    while True:
        today    = datetime.now(timezone.utc).date()
        tomorrow = today + timedelta(days=1)

        if today not in placed_dates:
            try:
                eth   = fetch_eth_spot()
                upper, lower = get_brackets(eth)

                all_orders: list[dict] = []
                all_orders += place_for_date(client, today, upper, lower)
                placed_dates.add(today)
                tmrw_placed.clear()  # new day — reset tomorrow tracking

                tmrw_orders = place_for_date(client, tomorrow, upper, lower,
                                             skip_slugs=tmrw_placed)
                all_orders += tmrw_orders

                threading.Thread(
                    target=monitor_and_sell,
                    args=(client, all_orders),
                    daemon=True,
                ).start()

            except Exception as e:
                log.error(f"Placement cycle failed: {e}")

        else:
            # Retry tomorrow brackets that weren't live yet when the day started
            if len(tmrw_placed) < 2:
                try:
                    eth = fetch_eth_spot()
                    upper, lower = get_brackets(eth)
                    retry_orders = place_for_date(client, tomorrow, upper, lower,
                                                  skip_slugs=tmrw_placed)
                    if retry_orders:
                        log.info(f"Tomorrow retry: placed {len(retry_orders)} new order(s).")
                        threading.Thread(
                            target=monitor_and_sell,
                            args=(client, retry_orders),
                            daemon=True,
                        ).start()
                except Exception as e:
                    log.error(f"Tomorrow retry failed: {e}")

        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    main()
