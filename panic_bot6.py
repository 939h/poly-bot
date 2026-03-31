"""
Polymarket 15-Min Crypto Up/Down — Panic Bot
============================================
Strategy:
  Phase 1 — Settle (first SETTLE_SECS of each window):
    Collect prices quietly. Build sigma baseline. No trading.
  Phase 2 — Armed (after settle):
    Lock reference_price = mean of settle prices.
    Buy when ALL 3 conditions true:
      1. CAP      — price < ENTRY_PRICE_CAP  (lottery zone)
      2. VELOCITY — price dropped >= DROP_FROM_REF from reference
      3. SIGMA    — price < sigma floor  (statistical confirmation)
    YES cheap → buy YES  |  NO cheap (YES pumped) → buy NO
  Exit: TP1 (50% shares) → TP2 (remaining) | trailing stop after TP1 | force stop loss

Infrastructure from fresh_bot10:
  - 4-key ApiCreds auth  (POLY_PRIVATE_KEY / POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE)
  - get_midpoint()       replaces REST /price polling
  - get_server_time()    replaces local UTC clock
  - build_slug()         deterministic slug generation
  - fetch_market_by_slug() with /events fallback
  - get_tokens()         with .strip() cleanup
  - market_buy/sell()    with BalanceAllowanceParams refresh after buy
  - token_cache          avoids re-fetching same market every poll

Requirements:
    pip install py-clob-client requests numpy python-dotenv

.env keys:
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
    DRY_RUN=true
    ORDER_AMOUNT_USDC=5
"""

import os
import sys
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import numpy as np
from datetime import datetime, timezone
from collections import deque
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        MarketOrderArgs, OrderType, ApiCreds,
        BalanceAllowanceParams, AssetType,
    )
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client requests numpy python-dotenv")
    sys.exit(1)

load_dotenv()

# ── Logging (UTF-8 so Windows never crashes on special chars) -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
        ),
        logging.FileHandler("panic_bot.log", encoding="utf-8"),
    ],
    force=True,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# =============================================================================
#  USER SETTINGS — edit everything in this block freely
# =============================================================================

# --- Assets to watch ---------------------------------------------------------
ASSETS           = ["btc", "eth", "sol", "xrp"]   # any combo of btc/eth/sol/xrp

# --- Trading mode & order size -----------------------------------------------
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"
                                                   # override via .env: DRY_RUN=false
ORDER_AMOUNT     = float(os.getenv("ORDER_AMOUNT_USDC", "5"))
                                                   # USDC per trade (override via .env)
TAKER_FEE        = 0.0156 # max taker fee on 15m crypto markets (1.56% at 50% prob)
                          # actual fee is lower near 0% or 100% — using max is conservative
                          # shares received = (ORDER_AMOUNT / entry_price) * (1 - TAKER_FEE)

# --- Settle phase (no trading during this period) ----------------------------
SETTLE_SECS      = 10    # first 120s of window = collect prices, no trading
                          # reference price = mean of prices collected here

# --- Trigger conditions (ALL 3 must be true to buy) --------------------------
ENTRY_PRICE_CAP  = 0.15   # condition 1: price must be below this (lottery zone)
DROP_FROM_REF    = 0.25   # condition 2: price must drop >= 25% from reference price
SD_LOOKBACK      = 10     # condition 3: sigma — number of samples for baseline
SD_THRESH        = 1.5    #              sigma floor multiplier (looser = more signals)

# --- Exit strategy -----------------------------------------------------------
TP1_MULT         = 2.0    # take profit 1 — sell 50% of shares at entry x this
TP2_MULT         = 4.0    # take profit 2 — sell remaining 50% of shares at entry x this
TRAILING_STOP    = 0.20   # sell remaining shares if price drops 20% from peak after TP1
FORCE_STOP_LOSS  = 0.50   # cut loss ALL shares immediately if price drops 50% below entry
                          # fires regardless of peak — protects against falling knife

# --- Force stop cooldown (wait period AFTER cut loss triggers) ---------------
# Window divided into 3 x 5-min periods — cooldown shrinks as window ages
# When force stop fires, bot waits this long before actually selling.
# If price recovers above stop during cooldown → cancel (it was a wick).
HOLD_EARLY_SECS  = 60     # 0-5 min   (early market)  — wait 60s before selling
HOLD_MID_SECS    = 40     # 5-10 min  (middle market) — wait 40s before selling
HOLD_LATE_SECS   = 20     # 10-15 min (late market)   — wait 20s before selling

# --- Timing ------------------------------------------------------------------
POLL_SECS        = 1      # seconds between each price scan
STOP_TRADE_SECS  = 780    # stop opening NEW trades after this many seconds into window
                          # 780 = 13 minutes  (window is 900s = 15 min)
                          # open positions continue to be monitored and sold normally

# =============================================================================
#  INTERNAL CONSTANTS — do not change these
# =============================================================================
GAMMA_API        = "https://gamma-api.polymarket.com"
CLOB_API         = "https://clob.polymarket.com"
INTERVAL         = "15m"
WINDOW_SECS      = 900

# ── State ---------------------------------------------------------------------
price_history   = {}   # "btc_yes" -> deque of floats  (sigma baseline)
open_positions  = {}   # "btc_yes" -> position dict
token_cache     = {}   # window_start -> {"btc": yes_tok, ...}  (YES token only)
live_prices     = {}   # "btc_yes" -> latest midpoint float
traded_this_window = set() # assets already bought this window e.g. {"btc", "eth"}
armed_logged       = False  # True after [ARMED] message shown once per window

stats = {
    "scans":    0,
    "triggers": 0,
    "buys":     0,
    "wins":     0,
    "losses":   0,
    "pnl":      0.0,
}

STATE_FILE = "bot_state.json"

def save_state():
    """Write current bot state to bot_state.json for pnl.py to read."""
    positions_out = {}
    for k, p in open_positions.items():
        entry     = p["entry_price"]
        curr      = live_prices.get(k, entry)
        tp1       = p["tp1_price"]
        tp2       = p["tp2_price"]
        tp1_done  = p.get("tp1_done", False)
        net_shares = p.get("net_shares", ORDER_AMOUNT / entry)

        # Progress: toward TP1 until TP1 is done, then toward TP2
        if not tp1_done:
            target = tp1
            pct = round((curr - entry) / (tp1 - entry) * 100, 1) if tp1 != entry else 0
        else:
            target = tp2
            pct = round((curr - entry) / (tp2 - entry) * 100, 1) if tp2 != entry else 0

        # After TP1 fired, only tp2_shares remain — use correct share count
        shares_held = p.get("tp2_shares", net_shares) if tp1_done else net_shares
        pnl = round((curr - entry) * shares_held, 4)
        positions_out[k] = {
            "entry":    round(entry, 4),
            "current":  round(curr, 4),
            "peak":     round(p["peak_price"], 4),
            "tp1":      round(tp1, 4),
            "tp2":      round(tp2, 4),
            "tp1_done": tp1_done,
            "target":   round(target, 4),
            "pnl":      pnl,
            "pct":      max(0, min(100, pct)),
        }
    state = {
        "updated":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dry_run":   DRY_RUN,
        "stats":     dict(stats),
        "positions": positions_out,
        "prices":    dict(live_prices),
        "settings": {
            "assets":    ASSETS,
            "sigma":     SD_THRESH,
            "cap":       ENTRY_PRICE_CAP,
            "drop_ref":  DROP_FROM_REF,
            "settle":    SETTLE_SECS,
            "tp1":       TP1_MULT,
            "tp2":       TP2_MULT,
            "trail":     TRAILING_STOP,
            "order":     ORDER_AMOUNT,
            "poll":      POLL_SECS,
            "lookback":  SD_LOOKBACK,
        },
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.debug("save_state error: %s", e)

# ── CLOB helpers (from fresh_bot10) ------------------------------------------

def build_client():
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        log.error("Missing .env keys. Need: POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE")
        sys.exit(1)
    client = ClobClient(
        host=CLOB_API,
        chain_id=POLYGON,
        key=pk,
        creds=ApiCreds(api_key=api_key, api_secret=api_sec, api_passphrase=api_pass),
        signature_type=1,
        funder=funder or None,
    )
    log.info("[AUTH] Connected to Polymarket CLOB")
    return client


def get_server_time():
    try:
        r = requests.get(f"{CLOB_API}/time", timeout=5)
        return int(r.json())
    except Exception:
        return int(datetime.now(timezone.utc).timestamp())


def get_current_window_start(server_ts):
    return (server_ts // WINDOW_SECS) * WINDOW_SECS


def build_slug(asset, window_ts):
    return f"{asset}-updown-{INTERVAL}-{window_ts}"


def fetch_market_by_slug(slug):
    """Fetch from Gamma /markets; falls back to /events if not found."""
    try:
        r    = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if markets:
            return markets[0]
        # fallback
        r2     = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
        events = r2.json()
        if isinstance(events, list) and events:
            nested = events[0].get("markets", [])
            if nested:
                return nested[0]
        return None
    except Exception as e:
        log.error("Gamma API error (%s): %s", slug, e)
        return None


def get_tokens(market):
    raw = market.get("clobTokenIds") or market.get("clob_token_ids", [])
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None, None
    if not raw or len(raw) < 2:
        return None, None
    return raw[0].strip(), raw[1].strip()


def get_midpoint(client, token_id):
    try:
        return float(client.get_midpoint(token_id)["mid"])
    except Exception:
        return 0.0


def market_buy(client, token_id, label):
    amount = round(ORDER_AMOUNT, 4)
    if DRY_RUN:
        log.info("[DRY-RUN] MARKET BUY %s $%.2f USDC", label, amount)
        return True
    try:
        order = client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=amount, side=BUY)
        )
        resp = client.post_order(order, OrderType.FOK)
        log.info("[BUY] Executed %s | %s", label, resp)
        # Refresh conditional token balance so sell works immediately
        try:
            client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
        except Exception:
            pass
        return True
    except Exception as e:
        log.error("[BUY] Failed %s: %s", label, e)
        return False


def market_sell(client, token_id, shares, price, label):
    amount = round(shares * price, 4)
    if DRY_RUN:
        log.info("[DRY-RUN] MARKET SELL %s $%.4f USDC", label, amount)
        return True
    try:
        order = client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=amount, side=SELL)
        )
        resp = client.post_order(order, OrderType.FOK)
        log.info("[SELL] Executed %s | %s", label, resp)
        return True
    except Exception as e:
        log.error("[SELL] Failed %s: %s", label, e)
        return False

# ── Trigger logic -------------------------------------------------------------

def update_history(key, price):
    """Feed price into sigma baseline. Always called every poll."""
    if key not in price_history:
        price_history[key] = deque(maxlen=SD_LOOKBACK)
    price_history[key].append(price)


def check_trigger(key, current_price, secs_into):
    """
    Settle phase  (secs_into < SETTLE_SECS):
      Trading locked out — no signals, no appending, just return False.
      price_history still builds during this time via _update_prices_and_history.

    Armed phase  (secs_into >= SETTLE_SECS):
      All signals are purely rolling — both velocity and sigma use the last
      SD_LOOKBACK prices from price_history, updated every poll.
      No settle buffer involved in any signal calculation.

      Fire when ALL 3 true:
        1. CAP      price < ENTRY_PRICE_CAP
        2. VELOCITY price dropped >= DROP_FROM_REF from rolling mean
        3. SIGMA    price < rolling sigma floor
    """
    # Dead market guard
    if current_price <= 0.01:
        log.debug("[SKIP] %s  price=%.4f  ignored (<=0.01)", key, current_price)
        return False

    # No new trades after STOP_TRADE_SECS
    if secs_into >= STOP_TRADE_SECS:
        log.debug("[SKIP] %s  secs_into=%d  past trade cutoff", key, secs_into)
        return False

    # ── Settle lockout — no signals, trading blocked ──────────────────────────
    if secs_into < SETTLE_SECS:
        if key.endswith("_yes"):
            asset   = key.rsplit("_", 1)[0]
            yes_key = f"{asset}_yes"
            pts     = len(price_history.get(yes_key) or [])
            log.info("[SETTLE] %s  price=%.4f  locked (%ds left)  history=%d pts",
                     key, current_price, SETTLE_SECS - secs_into, pts)
        return False

    # ── Armed — compute all signals from rolling price_history ────────────────
    asset   = key.rsplit("_", 1)[0]
    side    = key.rsplit("_", 1)[1]
    yes_key = f"{asset}_yes"
    hist    = price_history.get(yes_key)
    pts     = len(hist) if hist else 0

    # Need SD_LOOKBACK pts for any signal — keep warming until ready
    if pts < SD_LOOKBACK:
        if key.endswith("_yes"):
            log.info("[SCAN] %s  price=%.4f  warming up (%d/%d pts)",
                     key, current_price, pts, SD_LOOKBACK)
        return False

    # Compute rolling stats once — shared by velocity ref and sigma floor
    yes_mean = float(np.mean(hist))
    yes_std  = float(np.std(hist))

    # Derive side-specific mean (NO = 1 - YES)
    mean_p = yes_mean if side == "yes" else round(1.0 - yes_mean, 6)

    # Velocity reference = rolling mean of last SD_LOOKBACK prices
    ref = mean_p

    # ── Condition 1: lottery zone ─────────────────────────────────────────────
    if current_price > ENTRY_PRICE_CAP:
        return False

    # ── Condition 2: velocity — price dropped enough from rolling mean ─────────
    drop_pct = (ref - current_price) / ref if ref > 0 else 0
    if drop_pct < DROP_FROM_REF:
        log.info("[SCAN] %s  price=%.4f  ref=%.4f  drop=%.1f%%  need=%.0f%%  vel=NO",
                 key, current_price, ref, drop_pct * 100, DROP_FROM_REF * 100)
        return False

    # ── Condition 3: sigma floor ──────────────────────────────────────────────
    if side == "yes":
        std_p = yes_std
    else:
        no_series = np.array([1.0 - p for p in hist])
        std_p     = float(np.std(no_series))

    if std_p == 0:
        return False

    # Clamp floor: never below 1% of mean (prevents negative floors)
    floor = max(mean_p - (SD_THRESH * std_p), mean_p * 0.01)

    if current_price >= floor:
        log.info("[SCAN] %s  price=%.4f  ref=%.4f  drop=%.1f%%  sigma=NO(floor=%.4f)",
                 key, current_price, ref, drop_pct * 100, floor)
        return False

    # ── All 3 conditions met — PANIC ─────────────────────────────────────────
    log.info("[PANIC] %s  price=%.4f  ref=%.4f  drop=%.1f%%  floor=%.4f  mean=%.4f  std=%.4f",
             key, current_price, ref, drop_pct * 100, floor, mean_p, std_p)
    return True

# ── Position management -------------------------------------------------------

def open_position(key, token_id, entry_price):
    # Shares received after taker fee deducted
    gross_shares = ORDER_AMOUNT / entry_price
    net_shares   = round(gross_shares * (1 - TAKER_FEE), 6)
    tp1_shares = round(net_shares * 0.50, 6)   # 50% sold at TP1
    tp2_shares = round(net_shares - tp1_shares, 6)  # remaining 50% sold at TP2
    open_positions[key] = {
        "token_id":    token_id,
        "entry_price": entry_price,
        "peak_price":  entry_price,
        "tp1_price":   round(entry_price * TP1_MULT, 4),
        "tp2_price":   round(entry_price * TP2_MULT, 4),
        "net_shares":  net_shares,
        "tp1_shares":  tp1_shares,
        "tp2_shares":  tp2_shares,
        "tp1_done":              False,  # True after TP1 has been executed
        "force_stop_triggered":  None,   # timestamp when force stop cooldown started
        "force_stop_cooldown":   None,   # cooldown duration in seconds
    }
    stats["buys"] += 1
    log.info(
        "[OPEN] %s  entry=%.4f  shares=%.4f(after %.1f%%fee)  "
        "TP1=%.4f(%.4fsh)  TP2=%.4f(%.4fsh)  force-stop=%.4f  trail=%.0f%%",
        key, entry_price, net_shares, TAKER_FEE * 100,
        entry_price * TP1_MULT, tp1_shares,
        entry_price * TP2_MULT, tp2_shares,
        entry_price * (1 - FORCE_STOP_LOSS),
        TRAILING_STOP * 100
    )


def manage_positions(client):
    """
    Exit logic (checked in order):
      Exit 0 — Force stop loss : sell ALL shares immediately if price drops FORCE_STOP_LOSS% from entry
      Exit 1 — TP1             : sell 50% of shares at TP1_MULT x entry
      Exit 2 — TP2             : sell remaining 50% at TP2_MULT x entry
      Exit 3 — Trailing stop   : sell remaining shares if peak retraces TRAILING_STOP% (after TP1)
    """
    to_close = []

    for key, pos in open_positions.items():
        # Reuse live_prices already fetched by scan_markets this poll
        # Fall back to get_midpoint only if price not yet available
        current_price = live_prices.get(key)
        if current_price is None or current_price <= 0:
            current_price = get_midpoint(client, pos["token_id"])
        if current_price <= 0:
            continue

        # Update trailing peak
        if current_price > pos["peak_price"]:
            pos["peak_price"] = current_price

        entry      = pos["entry_price"]
        remaining  = pos["tp2_shares"] if pos["tp1_done"] else pos["net_shares"]
        pnl_remain = (current_price - entry) * remaining

        # ── Exit 0: Force stop loss with period-based cooldown ───────────────
        # When price hits stop level, start a cooldown timer instead of selling instantly.
        # If price recovers above stop during cooldown → cancel (it was a wick).
        # If price stays below stop for the full cooldown → sell confirmed.
        # Cooldown duration depends on where we are in the 15-min window.
        force_stop_price = round(entry * (1 - FORCE_STOP_LOSS), 4)
        if current_price <= force_stop_price:
            now = time.time()

            if pos.get("force_stop_triggered") is None:
                # First time hitting stop — start the cooldown, don't sell yet
                server_ts_now = get_server_time()
                w_start       = get_current_window_start(server_ts_now)
                secs_in       = server_ts_now - w_start
                if secs_in < 300:
                    cooldown = HOLD_EARLY_SECS
                    period   = "early"
                elif secs_in < 600:
                    cooldown = HOLD_MID_SECS
                    period   = "mid"
                else:
                    cooldown = HOLD_LATE_SECS
                    period   = "late"
                pos["force_stop_triggered"] = now
                pos["force_stop_cooldown"]  = cooldown
                log.info("[STOP-WAIT] %s  price=%.4f  stop=%.4f  waiting %ds(%s) for confirmation",
                         key, current_price, force_stop_price, cooldown, period)
                continue

            # Cooldown is running — check if enough time has passed
            cooldown  = pos.get("force_stop_cooldown", HOLD_LATE_SECS)
            secs_held = now - pos.get("force_stop_triggered", now)

            if secs_held < cooldown:
                log.info("[STOP-WAIT] %s  price=%.4f  stop=%.4f  confirming %ds/%ds",
                         key, current_price, force_stop_price,
                         int(secs_held), cooldown)
                continue

            # Cooldown expired — sell confirmed
            log.info("[FORCE-STOP] %s  price=%.4f  entry=%.4f  stop=%.4f  PnL=$%.4f  (confirmed after %ds)",
                     key, current_price, entry, force_stop_price, pnl_remain, int(secs_held))
            if market_sell(client, pos["token_id"], remaining, current_price, key.upper()):
                if pnl_remain > 0:
                    stats["wins"]  += 1
                else:
                    stats["losses"] += 1
                stats["pnl"] += pnl_remain
                to_close.append(key)
            continue

        # Price recovered above stop — reset cooldown timer
        if pos.get("force_stop_triggered") is not None:
            log.info("[STOP-CANCEL] %s  price=%.4f recovered above stop=%.4f  cooldown cancelled",
                     key, current_price, force_stop_price)
            pos["force_stop_triggered"] = None
            pos["force_stop_cooldown"]  = None

        # ── Exit 1: TP1 — sell 50% of shares ─────────────────────────────────
        if not pos["tp1_done"] and current_price >= pos["tp1_price"]:
            tp1_sh  = pos["tp1_shares"]
            tp1_pnl = (current_price - entry) * tp1_sh
            log.info("[TP1] %s  price=%.4f  sold=%.4fsh  PnL=$%.4f  holding %.4fsh for TP2",
                     key, current_price, tp1_sh, tp1_pnl, pos["tp2_shares"])
            if market_sell(client, pos["token_id"], tp1_sh, current_price, f"{key.upper()}-TP1"):
                pos["tp1_done"]   = True
                pos["peak_price"] = current_price  # reset peak from TP1 for trailing
                stats["wins"]    += 1
                stats["pnl"]     += tp1_pnl
            continue

        # ── Exit 2: TP2 — sell remaining shares ───────────────────────────────
        if pos["tp1_done"] and current_price >= pos["tp2_price"]:
            tp2_sh  = pos["tp2_shares"]
            tp2_pnl = (current_price - entry) * tp2_sh
            log.info("[TP2] %s  price=%.4f  sold=%.4fsh  PnL=$%.4f  position closed",
                     key, current_price, tp2_sh, tp2_pnl)
            if market_sell(client, pos["token_id"], tp2_sh, current_price, f"{key.upper()}-TP2"):
                stats["wins"] += 1
                stats["pnl"]  += tp2_pnl
                to_close.append(key)
            continue

        # ── Exit 3: Trailing stop — only after TP1, on remaining shares ───────
        if pos["tp1_done"] and pos["peak_price"] > entry:
            stop_price = pos["peak_price"] * (1 - TRAILING_STOP)
            if current_price <= stop_price:
                tp2_sh  = pos["tp2_shares"]
                tp2_pnl = (current_price - entry) * tp2_sh
                log.info("[TRAIL-STOP] %s  price=%.4f  peak=%.4f  sold=%.4fsh  PnL=$%.4f",
                         key, current_price, pos["peak_price"], tp2_sh, tp2_pnl)
                if market_sell(client, pos["token_id"], tp2_sh, current_price, f"{key.upper()}-TRAIL"):
                    if tp2_pnl > 0:
                        stats["wins"] += 1
                    else:
                        stats["losses"] += 1
                    stats["pnl"] += tp2_pnl
                    to_close.append(key)

    for key in to_close:
        del open_positions[key]

# ── Market scan ---------------------------------------------------------------

def _fetch_asset(client, asset, window_start):
    """
    Fetch YES midpoint only — one network call per asset.
    Both YES and NO tokens are cached (needed for order execution).
    NO price is derived: no_price = 1.0 - yes_price.
    Only YES price history is tracked — NO is monitored via YES inversion.
    Returns (asset, yes_price, yes_token, no_token) or None on failure.
    """
    # Cache both tokens once per window (YES to fetch price, NO to buy when YES pumps)
    if asset not in token_cache.get(window_start, {}):
        slug = build_slug(asset, window_start)
        mkt  = fetch_market_by_slug(slug)
        if not mkt:
            log.debug("No market found: %s", slug)
            return None
        yes_tok, no_tok = get_tokens(mkt)
        if not yes_tok or not no_tok:
            log.debug("Bad tokens for %s", slug)
            return None
        if window_start not in token_cache:
            token_cache[window_start] = {}
        token_cache[window_start][asset] = (yes_tok, no_tok)
        yes_mid = get_midpoint(client, yes_tok)
        log.info("[MARKET] Loaded %s  yes=%.4f  no=%.4f", slug, yes_mid, round(1.0 - yes_mid, 4))

    tokens = token_cache.get(window_start, {}).get(asset)
    if not tokens:
        return None
    yes_token, no_token = tokens

    # ONE network call — NO price derived from YES
    yes_price = get_midpoint(client, yes_token)
    if yes_price <= 0:
        log.debug("Midpoint unavailable: %s_yes", asset)
        return None

    return (asset, yes_price, yes_token, no_token)


def _update_prices_and_history(result):
    """
    Always called every poll for every asset — even after a trade.
    Keeps live_prices and sigma history current regardless of trade state.
    """
    if result is None:
        return
    asset, yes_price, yes_token, no_token = result
    no_price = round(1.0 - yes_price, 4)
    live_prices[f"{asset}_yes"] = yes_price
    live_prices[f"{asset}_no"]  = no_price
    update_history(f"{asset}_yes", yes_price)

    # settle_buffer no longer used for signals — settle phase is trading lockout only


def _evaluate_asset(result, secs_into):
    """
    Evaluate trigger for one asset using YES price only.
    Called only when asset has not yet traded this window.
    Price/history updates are handled separately by _update_prices_and_history.

    YES dump  (yes_price cheap + dropped from ref)  → buy YES token
    YES pump  (no_price  cheap + yes rose from ref) → buy NO  token
    """
    if result is None:
        return None

    asset, yes_price, yes_token, no_token = result
    no_price = round(1.0 - yes_price, 4)

    # YES dump → buy YES
    if f"{asset}_yes" not in open_positions:
        if check_trigger(f"{asset}_yes", yes_price, secs_into):
            return (f"{asset}_yes", yes_token, yes_price)

    # YES pump → NO is cheap → buy NO
    if f"{asset}_no" not in open_positions:
        if check_trigger(f"{asset}_no", no_price, secs_into):
            return (f"{asset}_no", no_token, no_price)

    return None


def scan_markets(client, window_start, secs_into, executor):
    """
    Parallel fetch: all 4 assets fire simultaneously via ThreadPoolExecutor.
    Each asset = 1 network call (YES only, NO = 1.0 - YES).

    Trigger logic (per side, independent):
      Phase 1 settle  (0 to SETTLE_SECS):   collect prices, no trading
      Phase 2 armed   (SETTLE_SECS onward):  cap + velocity + sigma
    """
    stats["scans"] += 1

    if window_start not in token_cache:
        token_cache[window_start] = {}

    # ── Fetch all assets in parallel ──────────────────────────────────────────
    results = {}
    futures = {executor.submit(_fetch_asset, client, asset, window_start): asset
               for asset in ASSETS}
    for future in as_completed(futures):
        asset = futures[future]
        try:
            results[asset] = future.result()
        except Exception as e:
            log.error("[FETCH] %s error: %s", asset, e)
            results[asset] = None

    # ── Update prices + history for ALL assets (always, even after a trade) ────
    for asset in ASSETS:
        _update_prices_and_history(results.get(asset))

    # ── Evaluate trigger only for assets not yet traded this window ───────────
    for asset in ASSETS:
        if asset in traded_this_window:
            log.debug("[SKIP] %s already traded this window", asset.upper())
            continue

        buy_signal = _evaluate_asset(results.get(asset), secs_into)
        if buy_signal:
            key, check_token, check_price = buy_signal
            stats["triggers"] += 1
            label = f"{asset.upper()}-{key.split('_')[1].upper()}"
            if market_buy(client, check_token, label):
                open_position(key, check_token, check_price)
                traded_this_window.add(asset)

# ── Status print --------------------------------------------------------------

def print_status(secs_left=None):
    wr = (f"{round(stats['wins'] / (stats['wins'] + stats['losses']) * 100)}% win"
          if (stats["wins"] + stats["losses"]) > 0 else "no closed trades")
    win_str = f"  window={secs_left}s left" if secs_left is not None else ""
    log.info(
        "[STATUS] scans=%d  triggers=%d  buys=%d  wins=%d  losses=%d  PnL=$%.4f  (%s)  open=%s%s",
        stats["scans"], stats["triggers"], stats["buys"],
        stats["wins"], stats["losses"], stats["pnl"], wr,
        list(open_positions.keys()) or "none", win_str,
    )

# ── Main loop -----------------------------------------------------------------

def main():
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    log.info("=" * 55)
    log.info("  Panic Bot  [%s]", mode)
    log.info("  Assets : %s", ", ".join(a.upper() for a in ASSETS))
    log.info("  settle=%ds  cap=$%.2f  drop=%.0f%%  sigma=%.1f(n=%d)",
             SETTLE_SECS, ENTRY_PRICE_CAP, DROP_FROM_REF * 100, SD_THRESH, SD_LOOKBACK)
    log.info("  TP1=%.1fx  TP2=%.1fx  trail=%.0f%%  order=$%.0f  poll=%ds",
             TP1_MULT, TP2_MULT, TRAILING_STOP * 100, ORDER_AMOUNT, POLL_SECS)
    log.info("=" * 55)

    global armed_logged
    client       = build_client()
    last_status  = time.time()
    last_window  = None
    executor     = ThreadPoolExecutor(max_workers=len(ASSETS))

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = WINDOW_SECS - secs_into

            armed_logged = False  # reset per window
            # New window — reset everything (each 15m window is independent)
            if last_window is not None and window_start != last_window:
                token_cache.clear()
                price_history.clear()
                live_prices.clear()
                traded_this_window.clear()
                log.info("[WINDOW] New window  ts=%d  secs_left=%d  settle=%ds  armed at %ds",
                         window_start, secs_left, SETTLE_SECS, SETTLE_SECS)
            last_window = window_start

            if secs_into >= SETTLE_SECS and not armed_logged:
                log.info("[ARMED] Window armed — scanning for panic triggers")
                armed_logged = True
            scan_markets(client, window_start, secs_into, executor)
            manage_positions(client)
            save_state()

        except KeyboardInterrupt:
            log.info("Shutting down...")
            executor.shutdown(wait=False)
            print_status(secs_left)
            break
        except Exception as e:
            log.error("Unexpected error: %s", e, exc_info=True)

        if time.time() - last_status >= 60:
            print_status()
            last_status = time.time()

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
