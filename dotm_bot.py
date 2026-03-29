"""
Polymarket DOTM (Deep Out-of-the-Money) Daily Limit Order Bot
==============================================================
Target:  "Bitcoin Above $X on [Date]" daily markets
Strategy: Place GTC limit orders for BOTH YES and NO at $0.002
          on the bracket 2000 below the current Binance BTC spot price.

Bracket logic:
  binance_price = 66054
  target_base   = 66054 - 2000  → 64054
  bracket       = (64054 // 1000) * 1000  → 64000

Order size: 100 shares each (YES + NO)
Order price: $0.002 per share
Total max cost: $0.40 USDC (200 shares × $0.002)

After placement the bot polls every POLL_SECS to detect fills
and cancels any unfilled orders after CANCEL_AFTER_HOURS.

Requirements:
    pip install py-clob-client python-dotenv requests colorama
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        LimitOrderArgs,
        OpenOrderParams,
        OrderType,
        ApiCreds,
    )
    from py_clob_client.order_builder.constants import BUY
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
ORDER_PRICE        = 0.002          # $0.002 per share
ORDER_SIZE         = 100            # shares per side
PRICE_STEP         = 2000           # drop 2000 below spot then floor to nearest 1000
FLOOR_STEP         = 1000           # rounding step for bracket
POLL_SECS          = 60             # how often to check fill status
CANCEL_AFTER_HOURS = 4              # cancel unfilled orders after this many hours

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
        if "FILLED" in msg or "placed" in msg.lower():
            return Fore.GREEN + msg + Style.RESET_ALL
        if "CANCEL" in msg or "ERROR" in msg or "error" in msg.lower():
            return Fore.RED + msg + Style.RESET_ALL
        if "WARNING" in msg:
            return Fore.YELLOW + msg + Style.RESET_ALL
        return msg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
for h in logging.getLogger().handlers:
    if isinstance(h, logging.StreamHandler):
        h.setFormatter(_ColorFormatter("%(asctime)s [%(levelname)s] %(message)s"))

log = logging.getLogger(__name__)

# ── Binance ───────────────────────────────────────────────────────────────────

def fetch_btc_spot() -> float:
    """Return current BTC/USDT spot price from Binance public API."""
    url = "https://api.binance.com/api/v3/ticker/price"
    try:
        r = requests.get(url, params={"symbol": "BTCUSDT"}, timeout=10)
        r.raise_for_status()
        price = float(r.json()["price"])
        log.info(f"Binance BTC spot: ${price:,.2f}")
        return price
    except Exception as e:
        log.error(f"Binance fetch failed: {e}")
        raise

# ── Bracket ───────────────────────────────────────────────────────────────────

def get_target_bracket(binance_price: float) -> int:
    """
    Return the Polymarket price bracket to target.

    Example:
        binance_price = 66054
        target_base   = 66054 - 2000  → 64054
        bracket       = (64054 // 1000) * 1000  → 64000
    """
    target_base = binance_price - PRICE_STEP
    bracket = int((target_base // FLOOR_STEP) * FLOOR_STEP)
    log.info(
        f"Bracket: ${binance_price:,.2f} → base={binance_price - PRICE_STEP:,.2f}"
        f" → bracket=${bracket:,}"
    )
    return bracket

# ── Slug / Market ─────────────────────────────────────────────────────────────

def build_slug(bracket: int) -> str:
    """
    Build the Polymarket slug for today's 'Bitcoin Above X' market.

    Format: bitcoin-above-{bracket}-on-{month}-{day}
    Example: bitcoin-above-64000-on-march-30
    """
    now = datetime.now(timezone.utc)
    # %-d removes zero-padding on Linux (produces "30" not "030")
    date_str = now.strftime("%B-%-d").lower()
    slug = f"bitcoin-above-{bracket}-on-{date_str}"
    log.info(f"Target slug: {slug}")
    return slug

def fetch_market(slug: str) -> dict | None:
    """Fetch market data from Gamma API by slug."""
    try:
        r = requests.get(
            f"{GAMMA_API}/markets",
            params={"slug": slug},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if markets:
            return markets[0]
        # Fallback: try /events endpoint
        r2 = requests.get(
            f"{GAMMA_API}/events",
            params={"slug": slug},
            timeout=10,
        )
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

def build_client() -> ClobClient:
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
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

# ── Order Placement ───────────────────────────────────────────────────────────

def place_limit_order(
    client: ClobClient,
    token_id: str,
    label: str,
) -> str | None:
    """
    Place a GTC limit order at ORDER_PRICE for ORDER_SIZE shares.
    Returns the order ID on success, None on failure.
    """
    if DRY_RUN:
        log.info(
            f"  [DRY RUN] LIMIT BUY {label}"
            f" | price=${ORDER_PRICE} | size={ORDER_SIZE} shares"
            f" | cost=${ORDER_PRICE * ORDER_SIZE:.2f} USDC"
        )
        return f"dry-run-{label.lower()}-{int(time.time())}"

    try:
        order = client.create_order(
            LimitOrderArgs(
                token_id=token_id,
                price=ORDER_PRICE,
                size=ORDER_SIZE,
                side=BUY,
            )
        )
        resp = client.post_order(order, OrderType.GTC)
        order_id = resp.get("orderID") or resp.get("id") or str(resp)
        log.info(
            f"  Limit order placed: {label}"
            f" | price=${ORDER_PRICE} | size={ORDER_SIZE}"
            f" | order_id={order_id}"
        )
        return order_id
    except Exception as e:
        log.error(f"  Limit order failed ({label}): {e}")
        return None

# ── Fill Monitor ──────────────────────────────────────────────────────────────

def get_open_order_ids(client: ClobClient, condition_id: str) -> set[str]:
    """Return set of open order IDs for this market."""
    if DRY_RUN:
        return set()
    try:
        open_orders = client.get_orders(OpenOrderParams(market=condition_id))
        if isinstance(open_orders, list):
            return {o.get("id") or o.get("orderID", "") for o in open_orders}
        return set()
    except Exception as e:
        log.warning(f"Could not fetch open orders: {e}")
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

def monitor_orders(
    client: ClobClient,
    condition_id: str,
    yes_order_id: str | None,
    no_order_id: str | None,
) -> None:
    """
    Poll until both orders are filled or CANCEL_AFTER_HOURS elapses.
    Cancels any remaining open orders at timeout.
    """
    if DRY_RUN:
        log.info("[DRY RUN] Skipping monitor loop (no real orders placed).")
        return

    deadline = time.time() + CANCEL_AFTER_HOURS * 3600
    pending = {}
    if yes_order_id:
        pending["YES"] = yes_order_id
    if no_order_id:
        pending["NO"] = no_order_id

    if not pending:
        log.warning("No valid order IDs to monitor.")
        return

    log.info(
        f"Monitoring {len(pending)} order(s) — will cancel after {CANCEL_AFTER_HOURS}h"
        f" (deadline {datetime.fromtimestamp(deadline, tz=timezone.utc).strftime('%H:%M:%S UTC')})"
    )

    while pending:
        time.sleep(POLL_SECS)
        now = time.time()

        open_ids = get_open_order_ids(client, condition_id)

        filled_labels = []
        for label, oid in list(pending.items()):
            if oid not in open_ids:
                log.info(f"  FILLED ✓ {label} order_id={oid}")
                filled_labels.append(label)

        for label in filled_labels:
            del pending[label]

        if not pending:
            log.info("All orders filled.")
            return

        if now >= deadline:
            log.warning(
                f"Timeout reached — cancelling {len(pending)} unfilled order(s): "
                + ", ".join(pending.keys())
            )
            for label, oid in pending.items():
                cancel_order(client, oid, label)
            return

        remaining_min = (deadline - now) / 60
        log.info(
            f"Waiting on {list(pending.keys())} | "
            f"{remaining_min:.0f}m until cancel"
        )

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("DOTM Daily Limit Order Bot")
    log.info(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info(f"Order: {ORDER_SIZE} shares @ ${ORDER_PRICE} each side")
    log.info("=" * 60)

    # 1. Fetch BTC spot from Binance
    btc_price = fetch_btc_spot()

    # 2. Calculate target bracket
    bracket = get_target_bracket(btc_price)

    # 3. Build slug and find market on Polymarket
    slug = build_slug(bracket)
    market = fetch_market(slug)

    if market is None:
        log.error(f"Market not found for slug: {slug}")
        log.error(
            "Check: (1) Is today's market live on Polymarket? "
            "(2) Is the slug format correct? "
            "Try https://gamma-api.polymarket.com/markets?slug=<slug>"
        )
        sys.exit(1)

    condition_id = market.get("conditionId") or market.get("condition_id", "")
    yes_token_id, no_token_id = extract_tokens(market)

    if not yes_token_id or not no_token_id:
        log.error(f"Could not extract token IDs from market: {market}")
        sys.exit(1)

    log.info(f"Market found: condition_id={condition_id}")
    log.info(f"  YES token: {yes_token_id}")
    log.info(f"  NO  token: {no_token_id}")

    # 4. Build Polymarket client
    client = build_client()

    # 5. Place limit orders
    log.info("Placing limit orders...")
    yes_order_id = place_limit_order(client, yes_token_id, "YES")
    no_order_id  = place_limit_order(client, no_token_id,  "NO")

    if not yes_order_id and not no_order_id:
        log.error("Both orders failed. Exiting.")
        sys.exit(1)

    log.info(f"  YES order_id: {yes_order_id}")
    log.info(f"  NO  order_id: {no_order_id}")

    # 6. Monitor until filled or timeout
    monitor_orders(client, condition_id, yes_order_id, no_order_id)

    log.info("Done.")


if __name__ == "__main__":
    main()
