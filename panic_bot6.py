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
  Exit: TP1 (50% shares) → TP2 (remaining) | trailing stop after TP1 | force stop loss | dead market guard :line 373

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
from datetime import datetime, timezone, timedelta
import csv
import os
import sys
import json
import time
import logging
import threading
import math
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
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

# ── export csv -----------------
def log_price_to_csv(asset, price):
    # Ensure the directory exists
    if not os.path.exists("data"):
        os.makedirs("data")
        
    file_path = "data/live_prices.csv"
    file_exists = os.path.isfile(file_path)
    
    with open(file_path, mode='a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "asset", "YESprice"])
        
        # Use UTC+8 for the CSV timestamp
        tz_8 = timezone(timedelta(hours=8))
        ts = datetime.now(tz_8).strftime("%Y-%m-%d %H:%M:%S")
        writer.writerow([ts, asset, price])

# =============================================================================
#  USER SETTINGS — edit everything in this block freely
# =============================================================================

# --- Assets to watch ---------------------------------------------------------
ASSETS           = ["eth", "sol", "xrp"]   # any combo of btc/eth/sol/xrp

# --- Trading mode & order size -----------------------------------------------
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"
                                                   # override via .env: DRY_RUN=false
ORDER_AMOUNT     = float(os.getenv("ORDER_AMOUNT_USDC", "5"))
                                                   # USDC per trade (override via .env)
# Do not hardcode exchange fee into share accounting.
# Use actual fill amounts from CLOB responses whenever available.

# --- Settle phase (no trading during this period) ----------------------------
SETTLE_SECS      = 20    # first 120s of window = collect prices, no trading
                          # reference price = mean of prices collected here

# --- Trigger conditions (ALL 3 must be true to buy) --------------------------
ENTRY_PRICE_CAP  = 0.06   # condition 1: price must be below this (lottery zone)
DROP_FROM_REF    = 0.35   # condition 2: price must drop >= 30% from reference price
SD_LOOKBACK      = 15     # condition 3: sigma — number of samples for baseline
SD_THRESH        = 1.3    #              sigma floor multiplier (looser = more signals)

# --- Exit strategy -----------------------------------------------------------
TP1_MULT         = 1.7    # take profit 1 — sell 50% of shares at entry x this
TP2_MULT         = 3    # take profit 2 — sell remaining 50% of shares at entry x this
TRAILING_STOP    = 0.30   # sell remaining shares if price drops 20% from peak after TP1
FORCE_STOP_LOSS  = 0.40   # cut loss ALL shares immediately if price drops 50% below entry
                          # fires regardless of peak — protects against falling knife

# --- Force stop cooldown (wait period AFTER cut loss triggers) ---------------
# Window divided into 3 x 5-min periods — cooldown shrinks as window ages
# When force stop fires, bot waits this long before actually selling.
# If price recovers above stop during cooldown → cancel (it was a wick).
HOLD_EARLY_SECS  = 40     # 0-5 min   (early market)  — wait 60s before selling
HOLD_MID_SECS    = 30     # 5-10 min  (middle market) — wait 40s before selling
HOLD_LATE_SECS   = 15     # 10-15 min (late market)   — wait 20s before selling

# --- Timing ------------------------------------------------------------------
POLL_SECS        = 1      # seconds between each price scan
STOP_TRADE_SECS  = 810    # stop opening NEW trades after this many seconds into window
                          # 780 = 13 minutes  (window is 900s = 15 min)
                          # open positions continue to be monitored and sold normally

# --- Optional trading windows (entry only; exits always allowed) -------------
# Leave TRADING_WINDOWS_ENABLED=False to trade anytime.
# Example: trade only 8pm-10pm local time -> TRADING_WINDOWS_ENABLED=True; TRADING_WINDOWS=[(20, 22)]
TRADING_WINDOWS_ENABLED = False
TRADING_TZ_OFFSET_HRS   = 8      # local timezone offset from UTC
TRADING_WINDOWS         = []     # list of (start_hour, end_hour), end is exclusive

EXIT_RETRY_COOLDOWN_SECS = 1  # avoid hammering the API if exits fail
TP1_MIN_FILL_RATIO = 0.95     # require near-complete TP1 fill before switching to TP2 mode
CLOSE_FILL_RATIO = 0.98       # require near-complete fill to mark position closed
UNSOLD_TOLERANCE_RATIO = 0.02 # treat <=2% leftover as dust and close the trade
TP_SELL_MAX_ATTEMPTS = 10     # once TP triggers, retry sell immediately up to N times
TP_SELL_RETRY_DELAY_SECS = 0.25
MIN_SELL_SHARES = 1           # venue/share handling: only send whole-share sell sizes

# =============================================================================
#  INTERNAL CONSTANTS — do not change these
# =============================================================================
GAMMA_API        = "https://gamma-api.polymarket.com"
CLOB_API         = "https://clob.polymarket.com"
INTERVAL         = "15m"
WINDOW_SECS      = 900
HTTP_PORT        = int(os.getenv('PORT', 8080))  # Railway injects PORT automatically

# ── State ---------------------------------------------------------------------
price_history   = {}   # "btc_yes" -> deque of floats  (sigma baseline)
open_positions  = {}   # "btc_yes" -> position dict
token_cache     = {}   # window_start -> {"btc": yes_tok, ...}  (YES token only)
live_prices     = {}   # "btc_yes" -> latest midpoint float
traded_this_window = set() # assets already bought this window e.g. {"btc", "eth"}
armed_logged       = False  # True after [ARMED] message shown once per window
pnl_history        = []     # [{ts, label, pnl}] snapshot every 5min for chart
asset_history      = {}     # {asset: {trades, wins, losses, pnl}} closed trade records
last_pnl_snapshot  = 0      # unix timestamp of last pnl_history append
pnl_history = []

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
        data = requests.get(f"{CLOB_API}/time", timeout=5).json()
        if isinstance(data, dict):
            ts = data.get("time") or data.get("timestamp") or data.get("serverTime")
            if ts is not None:
                return int(ts)
        return int(data)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp())



def get_current_window_start(server_ts):
    return (server_ts // WINDOW_SECS) * WINDOW_SECS


def can_open_new_trades(server_ts):
    """Optional time gate for NEW entries only."""
    if not TRADING_WINDOWS_ENABLED:
        return True
    if not TRADING_WINDOWS:
        return True

    local_hour = (datetime.fromtimestamp(server_ts, tz=timezone.utc)
                  + timedelta(hours=TRADING_TZ_OFFSET_HRS)).hour
    for start_raw, end_raw in TRADING_WINDOWS:
        start = int(start_raw) % 24
        end = int(end_raw) % 24
        if start == end:
            return True  # treat as full-day window
        if start < end and start <= local_hour < end:
            return True
        if start > end and (local_hour >= start or local_hour < end):
            return True
    return False


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


def market_buy(client, token_id, label, price_hint=None):
    amount = round(ORDER_AMOUNT, 4)  # amount = USDC to spend when side=BUY
    if DRY_RUN:
        log.info("[DRY-RUN] MARKET BUY %s $%.2f USDC", label, amount)
        entry_est = float(price_hint or 0) or get_midpoint(client, token_id)
        est_shares = int(max(math.floor(amount / entry_est), 0)) if entry_est > 0 else 0
        return {
            "ok": True,
            "resp": {"dry_run": True},
            "filled_shares": est_shares,
            "filled_price": float(entry_est) if entry_est > 0 else float(price_hint or 0),
        }
    try:
        order = client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=amount, side=BUY, order_type=OrderType.FOK) 
        )
        resp = client.post_order(order, OrderType.FOK)
        log.info("[BUY] Executed %s | %s", label, resp)
        # BUY responses expose received shares in takingAmount.
        raw_taking = float(resp.get("takingAmount") or 0)  # shares received
        raw_making = float(resp.get("makingAmount") or 0)  # quote spent
        filled_shares = int(max(math.floor(raw_taking), 0))
        filled_price = (raw_making / raw_taking) if raw_taking > 0 and raw_making > 0 else 0.0
        if filled_shares <= 0:
            log.warning("[BUY] %s filled shares unavailable in response; falling back to estimate", label)
            entry_est = float(price_hint or 0) or get_midpoint(client, token_id)
            filled_shares = int(max(math.floor(amount / entry_est), 0)) if entry_est > 0 else 0
            filled_price = float(entry_est) if entry_est > 0 else 0.0
        # Refresh conditional token balance so sell works immediately
        try:
            client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
        except Exception:
            pass
        return {"ok": True, "resp": resp, "filled_shares": filled_shares, "filled_price": filled_price}
    except Exception as e:
        log.error("[BUY] Failed %s: %s", label, e)
        return {"ok": False, "resp": None, "filled_shares": 0.0, "filled_price": 0.0}


def market_sell(client, token_id, shares, price, label):
    # amount = number of shares when side=SELL (not USDC)
    sell_shares = int(max(math.floor(shares), 0))
    if sell_shares < MIN_SELL_SHARES:
        return {"ok": False, "resp": None, "filled_shares": 0.0, "filled_quote": 0.0}

    if DRY_RUN:
        est_revenue = round(sell_shares * price, 4)
        log.info("[DRY-RUN] MARKET SELL %s %d sh (est $%.4f)", label, sell_shares, est_revenue)
        return {"ok": True, "resp": {"dry_run": True}, "filled_shares": sell_shares, "filled_quote": est_revenue}

    # best-effort allowance refresh
    try:
        client.update_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
    except Exception:
        pass

    attempt_shares = sell_shares
    for attempt in range(2):
        try:
            order = client.create_market_order(
                MarketOrderArgs(token_id=token_id, amount=attempt_shares, side=SELL, order_type=OrderType.FAK)
            )
            resp = client.post_order(order, OrderType.FAK)
            log.info("[SELL] Executed %s | %s", label, resp)
            try:
                client.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                )
            except Exception:
                pass
            filled_shares = int(max(math.floor(float(resp.get("makingAmount") or attempt_shares or 0)), 0))
            filled_quote = float(resp.get("takingAmount") or 0)  # quote received
            return {"ok": True, "resp": resp, "filled_shares": filled_shares, "filled_quote": filled_quote}
        except Exception as e:
            err = str(e)
            if attempt == 0 and "not enough balance / allowance" in err:
                bal_m = re.search(r"balance:\s*(\d+)", err)
                amt_m = re.search(r"order amount:\s*(\d+)", err)
                if bal_m and amt_m:
                    bal_raw = int(bal_m.group(1))
                    amt_raw = int(amt_m.group(1))
                    if 0 < bal_raw < amt_raw:
                        # balance/order amount are in the same unit in the error payload.
                        # Scale by ratio to avoid hardcoding token decimals.
                        ratio = bal_raw / amt_raw
                        retry_shares = int(max(math.floor((attempt_shares * ratio) * 0.999), 0))
                        if retry_shares >= MIN_SELL_SHARES:
                            log.warning(
                                "[SELL] %s size reduced %d -> %d to match available balance",
                                label, attempt_shares, retry_shares
                            )
                            attempt_shares = retry_shares
                            continue
            log.error("[SELL] Failed %s: %s", label, e)
            return {"ok": False, "resp": None, "filled_shares": 0.0, "filled_quote": 0.0}
    return {"ok": False, "resp": None, "filled_shares": 0.0, "filled_quote": 0.0}


def market_sell_with_retries(client, token_id, shares, price, label, max_attempts=TP_SELL_MAX_ATTEMPTS):
    """Retry a TP sell immediately so we don't miss fast moves after trigger."""
    last = {"ok": False, "resp": None, "filled_shares": 0.0}
    for attempt in range(1, max_attempts + 1):
        last = market_sell(client, token_id, shares, price, label)
        if last["ok"]:
            if attempt > 1:
                log.info("[SELL-RETRY] %s success on attempt %d/%d", label, attempt, max_attempts)
            return last
        if attempt < max_attempts:
            time.sleep(TP_SELL_RETRY_DELAY_SECS)
    log.error("[SELL-RETRY] %s failed after %d attempts", label, max_attempts)
    return last


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
    if current_price <= 0.025:
        log.debug("[SKIP] %s  price=%.4f  ignored (<=0.025)", key, current_price)
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
            log.debug("[SETTLE] %s  price=%.4f  locked (%ds left)  history=%d pts",
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

def open_position(key, token_id, entry_price, filled_shares=None):
    # Use executed fill size (from buy response) to keep inventory in sync.
    if filled_shares is not None and filled_shares > 0:
        net_shares = int(max(math.floor(float(filled_shares)), 0))
    else:
        gross_shares = ORDER_AMOUNT / entry_price
        net_shares   = int(max(math.floor(gross_shares), 0))
    tp1_shares = int(max(math.floor(net_shares * 0.50), 0))   # 50% sold at TP1
    tp2_shares = int(max(net_shares - tp1_shares, 0))  # remaining 50% sold at TP2
    open_positions[key] = {
        "token_id":    token_id,
        "entry_price": entry_price,
        "initial_shares": net_shares,
        "peak_price":  entry_price,
        "tp1_price":   round(entry_price * TP1_MULT, 4),
        "tp2_price":   round(entry_price * TP2_MULT, 4),
        "net_shares":  net_shares,
        "tp1_shares":  tp1_shares,
        "tp2_shares":  tp2_shares,
        "tp1_done":              False,  # True after TP1 has been executed
        "cost":        round(ORDER_AMOUNT, 4), # what we paid
        "force_stop_triggered":  None,   # timestamp when force stop cooldown started
        "force_stop_cooldown":   None,   # cooldown duration in seconds
        "tp1_revenue":           0.0,    # actual revenue received at TP1 sell
        "realized_revenue":      0.0,    # cumulative realized revenue across all successful sells
        "last_exit_attempt_ts":  0.0,    # avoids retry-spam when venue rejects exits
    }
    stats["buys"] += 1
    log.info(
        "[OPEN] %s  entry=%.4f  shares=%.4f  "
        "TP1=%.4f(%.4fsh)  TP2=%.4f(%.4fsh)  force-stop=%.4f  trail=%.0f%%",
        key, entry_price, net_shares,
        entry_price * TP1_MULT, tp1_shares,
        entry_price * TP2_MULT, tp2_shares,
        entry_price * (1 - FORCE_STOP_LOSS),
        TRAILING_STOP * 100
    )


def _record_closed_trade(key, pnl):
    """Record a closed trade into asset_history for the per-asset breakdown."""
    asset = key.split("_")[0]
    if asset not in asset_history:
        asset_history[asset] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
    r = asset_history[asset]
    r["trades"] += 1
    if pnl > 0:
        r["wins"] += 1
    else:
        r["losses"] += 1
    r["pnl"] = round(r["pnl"] + pnl, 4)


def _calc_pnl(pos, final_revenue):
    """pnl = (realized_revenue + final_revenue) - cost"""
    return round((pos.get("realized_revenue", 0.0) + final_revenue) - pos["cost"], 4)


def _within_unsold_tolerance(pos, remaining_shares):
    base = max(float(pos.get("initial_shares", 0.0)), 0.0)
    if base <= 0:
        return remaining_shares <= 0
    return remaining_shares <= (base * UNSOLD_TOLERANCE_RATIO)


def manage_positions(client):
    """
    Exit logic:
      Exit 0 — Force stop (cooldown): sell ALL remaining shares
      Exit 1 — TP1 : sell 50% shares, store tp1_revenue
      Exit 2 — TP2 : sell remaining 50%, calc pnl = (tp1_revenue + tp2_revenue) - cost
      Exit 3 — Trailing stop (after TP1): sell remaining, calc pnl

    PnL = (tp1_revenue + final_revenue) - cost
    """
    to_close = []

    for key, pos in open_positions.items():
        now = time.time()
        if now - pos.get("last_exit_attempt_ts", 0.0) < EXIT_RETRY_COOLDOWN_SECS:
            continue

        current_price = live_prices.get(key)
        if current_price is None or current_price <= 0:
            current_price = get_midpoint(client, pos["token_id"])
        if current_price <= 0:
            continue

        if current_price > pos["peak_price"]:
            pos["peak_price"] = current_price

        entry     = pos["entry_price"]
        remaining = pos["tp2_shares"] if pos["tp1_done"] else pos["net_shares"]

        # ── Exit 0: Force stop with cooldown ─────────────────────────────────
        force_stop_price = round(entry * (1 - FORCE_STOP_LOSS), 4)
        if current_price <= force_stop_price:
            now = time.time()
            if pos.get("force_stop_triggered") is None:
                server_ts_now = get_server_time()
                secs_in = server_ts_now - get_current_window_start(server_ts_now)
                if secs_in < 300:
                    cooldown, period = HOLD_EARLY_SECS, "early"
                elif secs_in < 600:
                    cooldown, period = HOLD_MID_SECS, "mid"
                else:
                    cooldown, period = HOLD_LATE_SECS, "late"
                pos["force_stop_triggered"] = now
                pos["force_stop_cooldown"]  = cooldown
                log.info("[STOP-WAIT] %s  price=%.4f  stop=%.4f  waiting %ds(%s)",
                         key, current_price, force_stop_price, cooldown, period)
                continue

            cooldown  = pos.get("force_stop_cooldown", HOLD_LATE_SECS)
            secs_held = now - pos.get("force_stop_triggered", now)
            if secs_held < cooldown:
                log.info("[STOP-WAIT] %s  price=%.4f  confirming %ds/%ds",
                         key, current_price, int(secs_held), cooldown)
                continue

            # Confirmed — sell all remaining
            revenue = round(remaining * current_price, 4)
            pnl     = _calc_pnl(pos, revenue)
            log.info("[FORCE-STOP] %s  price=%.4f  attempting %.4fsh  (after %ds)",
                     key, current_price, remaining, int(secs_held))
            sell = market_sell(client, pos["token_id"], remaining, current_price, key.upper())
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                sold_sh = min(int(remaining), int(max(math.floor(float(sell.get("filled_shares") or remaining)), 0)))
                sold_revenue = float(sell.get("filled_quote") or round(sold_sh * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + sold_revenue, 4)
                leftover = int(max(pos["net_shares"] - sold_sh, 0))
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                log.info("[FORCE-STOP] %s finalized  sold=%dsh  leftover=%dsh ignored  pnl=$%.4f",
                         key, sold_sh, leftover, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                to_close.append(key)
            continue

        # Price recovered — reset cooldown
        if pos.get("force_stop_triggered") is not None:
            log.info("[STOP-CANCEL] %s  price=%.4f  recovered above stop=%.4f",
                     key, current_price, force_stop_price)
            pos["force_stop_triggered"] = None
            pos["force_stop_cooldown"]  = None

        # ── Exit 1: TP1 — sell 50%, store revenue ────────────────────────────
        if not pos["tp1_done"] and current_price >= pos["tp1_price"]:
            tp1_sh      = pos["tp1_shares"]
            if tp1_sh <= 0:
                continue
            log.info("[TP1] %s  price=%.4f  trigger hit  attempting %.4fsh",
                     key, current_price, tp1_sh)
            sell = market_sell_with_retries(
                client, pos["token_id"], tp1_sh, current_price, f"{key.upper()}-TP1"
            )
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                sold_sh = min(int(tp1_sh), int(max(math.floor(float(sell.get("filled_shares") or tp1_sh)), 0)))
                tp1_revenue = float(sell.get("filled_quote") or round(sold_sh * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + tp1_revenue, 4)
                remaining_after_sell = int(max(pos["net_shares"] - sold_sh, 0))
                if sold_sh >= tp1_sh * TP1_MIN_FILL_RATIO:
                    pos["tp1_done"]    = True
                    pos["tp1_revenue"] = round(pos.get("tp1_revenue", 0.0) + tp1_revenue, 4)
                    pos["net_shares"]  = remaining_after_sell
                    pos["tp1_shares"]  = 0
                    pos["tp2_shares"]  = remaining_after_sell
                    pos["peak_price"]  = current_price
                    log.info("[TP1] %s executed  sold=%.4fsh  revenue=$%.4f  holding %.4fsh",
                             key, sold_sh, tp1_revenue, remaining_after_sell)
                else:
                    pos["net_shares"] = remaining_after_sell
                    pos["tp1_shares"] = int(max(tp1_sh - sold_sh, 0))
                    pos["tp2_shares"] = int(max(pos["net_shares"] - pos["tp1_shares"], 0))
                    log.warning("[TP1] %s partial fill  sold=%.4fsh  tp1_remaining=%.4fsh",
                                key, sold_sh, pos["tp1_shares"])
            continue

        # ── Exit 2: TP2 — sell remaining, final pnl ──────────────────────────
        if pos["tp1_done"] and current_price >= pos["tp2_price"]:
            tp2_sh      = pos["tp2_shares"]
            if tp2_sh <= 0:
                to_close.append(key)
                continue
            log.info("[TP2] %s  price=%.4f  trigger hit  attempting %.4fsh",
                     key, current_price, tp2_sh)
            sell = market_sell_with_retries(
                client, pos["token_id"], tp2_sh, current_price, f"{key.upper()}-TP2"
            )
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                sold_sh = min(int(tp2_sh), int(max(math.floor(float(sell.get("filled_shares") or tp2_sh)), 0)))
                tp2_revenue = float(sell.get("filled_quote") or round(sold_sh * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + tp2_revenue, 4)
                leftover = int(max(pos["tp2_shares"] - sold_sh, 0))
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                log.info("[TP2] %s finalized  sold=%dsh  leftover=%dsh ignored  pnl=$%.4f",
                         key, sold_sh, leftover, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                to_close.append(key)
            continue

        # ── Exit 3: Trailing stop — after TP1, remaining shares ──────────────
        if pos["tp1_done"] and pos["peak_price"] > entry:
            stop_price = pos["peak_price"] * (1 - TRAILING_STOP)
            if current_price <= stop_price:
                tp2_sh      = pos["tp2_shares"]
                if tp2_sh <= 0:
                    to_close.append(key)
                    continue
                log.info("[TRAIL-STOP] %s  price=%.4f  peak=%.4f  attempting %.4fsh",
                         key, current_price, pos["peak_price"], tp2_sh)
                sell = market_sell_with_retries(
                    client, pos["token_id"], tp2_sh, current_price, f"{key.upper()}-TRAIL"
                )
                pos["last_exit_attempt_ts"] = time.time()
                if sell["ok"]:
                    sold_sh = min(int(tp2_sh), int(max(math.floor(float(sell.get("filled_shares") or tp2_sh)), 0)))
                    tp2_revenue = float(sell.get("filled_quote") or round(sold_sh * current_price, 4))
                    pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + tp2_revenue, 4)
                    leftover = int(max(pos["tp2_shares"] - sold_sh, 0))
                    pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                    log.info("[TRAIL-STOP] %s finalized  sold=%dsh  leftover=%dsh ignored  pnl=$%.4f",
                             key, sold_sh, leftover, pnl)
                    stats["wins" if pnl > 0 else "losses"] += 1
                    stats["pnl"] += pnl
                    _record_closed_trade(key, pnl)
                    to_close.append(key)

    for key in to_close:
        del open_positions[key]

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
            log.info("No market found: %s", slug)
            return None
        yes_tok, no_tok = get_tokens(mkt)
        if not yes_tok or not no_tok:
            log.info("Bad tokens for %s", slug)
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
   # --- ADD THIS LINE HERE ---
    log_price_to_csv(asset, yes_price)
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


def scan_markets(client, window_start, secs_into, server_ts, executor):
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

    if not can_open_new_trades(server_ts):
        return

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
            buy = market_buy(client, check_token, label, price_hint=check_price)
            if buy["ok"]:
                entry_px = float(buy.get("filled_price") or check_price)
                open_position(key, check_token, entry_px, filled_shares=buy.get("filled_shares"))
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


# ── HTTP server (PnL dashboard for Railway) -----------------------------------

def _build_state_snapshot():
    """Build a clean JSON-serialisable snapshot of live in-memory state."""
    positions_out = {}
    for k, p in open_positions.items():
        entry    = p["entry_price"]
        curr     = live_prices.get(k, entry)
        tp1      = p["tp1_price"]
        tp2      = p["tp2_price"]
        tp1_done = p.get("tp1_done", False)
        net_sh   = p.get("net_shares", ORDER_AMOUNT / entry)
        shares_held = p.get("tp2_shares", net_sh) if tp1_done else net_sh
        if not tp1_done:
            target = tp1
            pct = round((curr - entry) / (tp1 - entry) * 100, 1) if tp1 != entry else 0
        else:
            target = tp2
            pct = round((curr - entry) / (tp2 - entry) * 100, 1) if tp2 != entry else 0
        pnl = round((curr - entry) * shares_held, 4)
        positions_out[k] = {
            "entry": round(entry, 4), "current": round(curr, 4),
            "peak": round(p["peak_price"], 4),
            "tp1": round(tp1, 4), "tp2": round(tp2, 4),
            "tp1_done": tp1_done, "target": round(target, 4),
            "pnl": pnl, "pct": max(0, min(100, pct)),
        }
    now_ts  = int(time.time())
    slot_ts = (now_ts // 900) * 900
    secs_in = now_ts - slot_ts
    return {
        "updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dry_run":    DRY_RUN,
        "stats":      dict(stats),
        "positions":  positions_out,
        "prices":     dict(live_prices),
        "window": {
            "secs_into": secs_in,
            "secs_left": 900 - secs_in,
            "period": "early" if secs_in < 300 else ("mid" if secs_in < 600 else "late"),
        },
        "pnl_history":   list(pnl_history),
        "asset_history":  dict(asset_history),
        "settings": {
            "assets": ASSETS, "sigma": SD_THRESH, "cap": ENTRY_PRICE_CAP,
            "drop_ref": DROP_FROM_REF, "settle": SETTLE_SECS,
            "tp1": TP1_MULT, "tp2": TP2_MULT, "trail": TRAILING_STOP,
            "order": ORDER_AMOUNT, "poll": POLL_SECS, "lookback": SD_LOOKBACK,
        },
    }


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Panic Bot</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0f1117;color:#e8edf5;font-size:14px;padding:20px}
h2{font-size:15px;font-weight:600;margin:0 0 14px;color:#e8edf5}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:16px}
.card{background:#161b27;border:1px solid #2a3347;border-radius:10px;padding:14px}
.card .lbl{font-size:11px;color:#5a6a85;text-transform:uppercase;letter-spacing:.06em;margin-bottom:5px}
.card .val{font-size:22px;font-weight:600}
.green{color:#4ade9f}.red{color:#f87171}.amber{color:#fbbf24}.blue{color:#60a5fa}.dim{color:#5a6a85}
.section{background:#161b27;border:1px solid #2a3347;border-radius:10px;padding:16px;margin-bottom:16px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:#5a6a85;font-weight:500;padding:0 0 8px;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
td{padding:7px 0;border-top:1px solid #2a3347;font-family:monospace;font-size:13px}
td:first-child{font-family:system-ui;font-weight:500;color:#e8edf5}
.bar-bg{height:6px;background:#2a3347;border-radius:3px;overflow:hidden;margin-top:4px}
.bar-fill{height:100%;border-radius:3px;transition:width .5s}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}
.badge.dry{background:#2a1e08;color:#fbbf24;border:1px solid #5c3d08}
.badge.live{background:#2a0d0d;color:#f87171;border:1px solid #5c1d1d}
.badge.early{background:#0d2a1e;color:#4ade9f}.badge.mid{background:#0d1e2a;color:#60a5fa}.badge.late{background:#2a0d0d;color:#f87171}
.pos-card{background:#1e2533;border:1px solid #2a3347;border-radius:8px;padding:12px;margin-bottom:8px}
.pos-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.pos-meta{display:flex;gap:16px;font-size:12px;color:#5a6a85;font-family:monospace;margin-top:6px;flex-wrap:wrap}
.chart-wrap{position:relative;height:180px;margin-top:4px}
canvas{display:block;width:100%!important;height:180px!important}
.asset-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}
.asset-card{background:#1e2533;border:1px solid #2a3347;border-radius:8px;padding:12px}
.asset-card .name{font-size:13px;font-weight:600;margin-bottom:8px}
.asset-row{display:flex;justify-content:space-between;font-size:12px;padding:3px 0;border-bottom:1px solid #252d3d}
.asset-row:last-child{border-bottom:none}
.asset-row .k{color:#5a6a85}
footer{text-align:center;color:#2a3347;font-size:11px;margin-top:20px;padding-bottom:10px}
</style>
</head>
<body>
<div id="root"><p style="color:#5a6a85;padding:40px;text-align:center">Loading...</p></div>
<canvas id="pnlChart" style="display:none"></canvas>
<script>
// ── Globals ──────────────────────────────────────────────────────────────────
let chartInst = null;

function fmt(v,d=4){return v!=null?'$'+parseFloat(v).toFixed(d):'—'}
function fmtPnl(v){
  const n=parseFloat(v)||0;
  const s=n>=0?'+':'';
  const col=n>0?'green':n<0?'red':'dim';
  return `<span class="${col}">${s}$${Math.abs(n).toFixed(4)}</span>`;
}
function pnlColor(v){return v>0?'green':v<0?'red':'dim'}

// ── PnL chart ─────────────────────────────────────────────────────────────────
function drawChart(history){
  const canvas = document.getElementById('pnlChart');
  if(!history||history.length<2){
    if(canvas._chartSection){
      canvas._chartSection.innerHTML='<p class="dim" style="padding:12px 0;font-size:12px">Not enough data yet — chart updates every 5 minutes</p>';
    }
    return;
  }
  const wrap = canvas._chartSection;
  if(!wrap) return;

  const W=wrap.offsetWidth||600, H=180;
  canvas.width=W; canvas.height=H;
  canvas.style.display='block';

  const ctx=canvas.getContext('2d');
  ctx.clearRect(0,0,W,H);

  const vals=history.map(p=>p.pnl);
  const labels=history.map(p=>p.ts);
  const minV=Math.min(...vals,0), maxV=Math.max(...vals,0);
  const range=maxV-minV||1;
  const padT=20,padB=40,padL=60,padR=16;
  const chartW=W-padL-padR, chartH=H-padT-padB;

  const xOf=i=>padL+i*(chartW/(history.length-1));
  const yOf=v=>padT+chartH-(((v-minV)/range)*chartH);

  // Grid lines
  ctx.strokeStyle='#2a3347'; ctx.lineWidth=1;
  [0,0.25,0.5,0.75,1].forEach(t=>{
    const y=padT+chartH*t;
    ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(W-padR,y); ctx.stroke();
    const lbl=((minV+(maxV-minV)*(1-t))||0).toFixed(3);
    ctx.fillStyle='#5a6a85'; ctx.font='10px system-ui'; ctx.textAlign='right';
    ctx.fillText((parseFloat(lbl)>=0?'+':'')+lbl,padL-6,y+4);
  });

  // Zero line
  if(minV<0&&maxV>0){
    const yz=yOf(0);
    ctx.strokeStyle='#3a4560'; ctx.lineWidth=1;
    ctx.setLineDash([4,4]);
    ctx.beginPath(); ctx.moveTo(padL,yz); ctx.lineTo(W-padR,yz); ctx.stroke();
    ctx.setLineDash([]);
  }

  // Fill area
  const grad=ctx.createLinearGradient(0,padT,0,padT+chartH);
  const latestPnl=vals[vals.length-1]||0;
  if(latestPnl>=0){
    grad.addColorStop(0,'rgba(29,184,122,0.35)');
    grad.addColorStop(1,'rgba(29,184,122,0.03)');
  } else {
    grad.addColorStop(0,'rgba(226,75,74,0.03)');
    grad.addColorStop(1,'rgba(226,75,74,0.35)');
  }
  ctx.beginPath();
  ctx.moveTo(xOf(0),yOf(vals[0]));
  vals.forEach((v,i)=>{if(i>0)ctx.lineTo(xOf(i),yOf(v));});
  ctx.lineTo(xOf(vals.length-1),padT+chartH);
  ctx.lineTo(xOf(0),padT+chartH);
  ctx.closePath();
  ctx.fillStyle=grad; ctx.fill();

  // Line
  ctx.beginPath();
  ctx.strokeStyle=latestPnl>=0?'#1db87a':'#e24b4a';
  ctx.lineWidth=2;
  vals.forEach((v,i)=>{i===0?ctx.moveTo(xOf(i),yOf(v)):ctx.lineTo(xOf(i),yOf(v));});
  ctx.stroke();

  // Dots at data points
  vals.forEach((v,i)=>{
    ctx.beginPath();
    ctx.arc(xOf(i),yOf(v),3,0,Math.PI*2);
    ctx.fillStyle=latestPnl>=0?'#1db87a':'#e24b4a';
    ctx.fill();
  });

  // X-axis labels — show max 8 evenly spaced, never overlap
  ctx.fillStyle='#5a6a85'; ctx.font='10px system-ui'; ctx.textAlign='center';
  const maxLabels=Math.min(8,history.length);
  const step=Math.max(1,Math.floor(history.length/maxLabels));
  for(let i=0;i<history.length;i+=step){
    ctx.fillText(labels[i],xOf(i),H-padB+16);
  }
  // Always show last label
  const last=history.length-1;
  if(last%step!==0) ctx.fillText(labels[last],xOf(last),H-padB+16);
}

// ── Asset history breakdown ───────────────────────────────────────────────────
function renderAssetHistory(assetHist, assets){
  if(!assetHist||Object.keys(assetHist).length===0){
    return '<p class="dim" style="padding:8px 0;font-size:12px">No closed trades yet</p>';
  }
  return '<div class="asset-grid">'+assets.map(a=>{
    const h=assetHist[a];
    if(!h) return `<div class="asset-card"><div class="name">${a.toUpperCase()}</div><div class="dim" style="font-size:12px">No trades</div></div>`;
    const total=h.trades||0;
    const wr=total>0?Math.round((h.wins||0)/total*100)+'%':'—';
    const p=h.pnl||0;
    return `<div class="asset-card">
      <div class="name">${a.toUpperCase()}</div>
      <div class="asset-row"><span class="k">Closed trades</span><span>${total}</span></div>
      <div class="asset-row"><span class="k">Wins</span><span class="green">${h.wins||0}</span></div>
      <div class="asset-row"><span class="k">Losses</span><span class="${(h.losses||0)>0?'red':'dim'}">${h.losses||0}</span></div>
      <div class="asset-row"><span class="k">Win rate</span><span class="${total>0?'green':'dim'}">${wr}</span></div>
      <div class="asset-row"><span class="k">Net PnL</span><span>${fmtPnl(p)}</span></div>
    </div>`;
  }).join('')+'</div>';
}

// ── Main render ───────────────────────────────────────────────────────────────
function render(s){
  const st=s.stats||{}, pos=s.positions||{}, pr=s.prices||{};
  const cfg=s.settings||{}, w=s.window||{};
  const pnlHist=s.pnl_history||[];
  const assetHist=s.asset_history||{};
  const assets=cfg.assets||['btc','eth','sol','xrp'];
  const cap=cfg.cap||0.08;

  const mode=s.dry_run?'<span class="badge dry">DRY RUN</span>':'<span class="badge live">LIVE</span>';
  const period=w.period||'early';
  const mm=v=>String(Math.floor((v||0)/60)).padStart(2,'0');
  const ss=v=>String((v||0)%60).padStart(2,'0');
  const wStr=`${mm(w.secs_into)}:${ss(w.secs_into)} in &nbsp;|&nbsp; ${mm(w.secs_left)}:${ss(w.secs_left)} left`;

  const total=(st.wins||0)+(st.losses||0);
  const wr=total>0?Math.round(st.wins/total*100)+'%':'—';
  const pnl=st.pnl||0;

  const priceRows=assets.map(a=>{
    const yp=pr[a+'_yes'], np=pr[a+'_no'];
    const yc=yp!=null&&yp<=cap?'red':'';
    const nc=np!=null&&np<=cap?'red':'';
    const holding=[(a+'_yes' in pos)?'<span class="green">YES</span>':'',(a+'_no' in pos)?'<span class="green">NO</span>':''].filter(Boolean).join(' ');
    return `<tr><td>${a.toUpperCase()}</td><td class="${yc}">${fmt(yp)}</td><td class="${nc}">${fmt(np)}</td><td>${holding||'<span class="dim">—</span>'}</td></tr>`;
  }).join('');

  const posCards=Object.entries(pos).map(([k,p])=>{
    const [asset,side]=k.split('_');
    const col=p.current>=p.entry?'green':'red';
    const phase=p.tp1_done?'<span class="blue">TP1 done — riding to TP2</span>':'Watching TP1';
    const tgt=p.tp1_done?'TP2':'TP1';
    const pnlV=p.pnl||0;
    return `<div class="pos-card">
      <div class="pos-hdr"><strong>${asset.toUpperCase()}-${side.toUpperCase()}</strong><span style="font-size:12px">${phase}</span></div>
      <div style="display:flex;gap:16px;font-size:13px;flex-wrap:wrap">
        <span><span class="dim">entry</span> <strong>${fmt(p.entry)}</strong></span>
        <span><span class="dim">current</span> <strong class="${col}">${fmt(p.current)}</strong></span>
        <span><span class="dim">peak</span> <strong>${fmt(p.peak)}</strong></span>
        <span><span class="dim">TP1</span> <strong>${fmt(p.tp1)}</strong></span>
        <span><span class="dim">TP2</span> <strong>${fmt(p.tp2)}</strong></span>
      </div>
      <div class="bar-bg"><div class="bar-fill" style="width:${p.pct||0}%;background:${p.current>=p.entry?'#1db87a':'#e24b4a'}"></div></div>
      <div class="pos-meta">
        <span>${(p.pct||0).toFixed(0)}% to ${tgt}</span>
        <span>Unrealised PnL: ${fmtPnl(pnlV)}</span>
      </div>
    </div>`;
  }).join('')||'<p class="dim" style="padding:8px 0">No open positions</p>';

  document.getElementById('root').innerHTML=`
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:8px">
      <div><strong style="font-size:18px">Panic<span class="green">Bot</span></strong> &nbsp; ${mode}</div>
      <div style="font-size:12px;color:#5a6a85">${s.updated||''} &nbsp; <span class="badge ${period}">${period.toUpperCase()}</span> &nbsp; ${wStr}</div>
    </div>

    <div class="grid">
      <div class="card"><div class="lbl">Scans</div><div class="val">${st.scans||0}</div></div>
      <div class="card"><div class="lbl">Triggers</div><div class="val amber">${st.triggers||0}</div></div>
      <div class="card"><div class="lbl">Buys</div><div class="val">${st.buys||0}</div></div>
      <div class="card"><div class="lbl">Wins</div><div class="val green">${st.wins||0}</div></div>
      <div class="card"><div class="lbl">Losses</div><div class="val ${(st.losses||0)>0?'red':'dim'}">${st.losses||0}</div></div>
      <div class="card"><div class="lbl">Win rate</div><div class="val ${total>0?'green':'dim'}">${wr}</div></div>
      <div class="card"><div class="lbl">Open</div><div class="val blue">${Object.keys(pos).length}</div></div>
      <div class="card"><div class="lbl">Net PnL</div><div class="val ${pnlColor(pnl)}">${pnl>=0?'+':''}$${pnl.toFixed(4)}</div></div>
    </div>

    <div class="section">
      <h2>Net PnL Over Time <span style="font-size:11px;color:#5a6a85;font-weight:400">(recorded every 5 min)</span></h2>
      <div class="chart-wrap" id="chartWrap">
        <canvas id="pnlChart"></canvas>
      </div>
    </div>

    <div class="section">
      <h2>Live Prices</h2>
      <table><thead><tr><th>Asset</th><th>YES</th><th>NO</th><th>Holding</th></tr></thead>
      <tbody>${priceRows}</tbody></table>
    </div>

    <div class="section"><h2>Open Positions (${Object.keys(pos).length})</h2>${posCards}</div>

    <div class="section">
      <h2>Closed Trades — Per Asset</h2>
      ${renderAssetHistory(assetHist, assets)}
    </div>

    <div class="section">
      <h2>Settings</h2>
      <table><tbody>
        <tr><td>Settle</td><td>${cfg.settle||30}s</td><td>Price cap</td><td>$${(cfg.cap||0.08).toFixed(2)}</td></tr>
        <tr><td>Drop from ref</td><td>${((cfg.drop_ref||0.30)*100).toFixed(0)}%</td><td>Sigma</td><td>${cfg.sigma||2.2} (n=${cfg.lookback||10})</td></tr>
        <tr><td>TP1</td><td>${cfg.tp1||2}x</td><td>TP2</td><td>${cfg.tp2||10}x</td></tr>
        <tr><td>Trailing stop</td><td>${((cfg.trail||0.30)*100).toFixed(0)}%</td><td>Order size</td><td>$${cfg.order||5}</td></tr>
      </tbody></table>
    </div>

    <footer>Auto-refreshes every 5s &nbsp;&mdash;&nbsp; panic_bot6 &nbsp;&mdash;&nbsp; PnL chart records every 5 min</footer>`;

  // Draw chart after DOM is updated
  const wrap = document.getElementById('chartWrap');
  const cnv  = document.getElementById('pnlChart');
  if(cnv) cnv._chartSection = wrap;
  drawChart(pnlHist);
}

async function poll(){
  try{
    const r=await fetch('/state');
    const d=await r.json();
    render(d);
  } catch(e){ console.error('fetch error',e); }
}
poll();
setInterval(poll,5000);
</script>
</body>
</html>"""


class _PnLHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/state":
            data = json.dumps(_build_state_snapshot(), indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
          # --- ADD THIS NEW SECTION HERE ---
        elif self.path == "/download":
            file_path = "data/live_prices.csv"
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    data = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv')
                self.send_header('Content-Disposition', 'attachment; filename="live_prices.csv"')
                self.send_header('Content-Length', len(data))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_error(404, "CSV file not found yet. Please wait for the first price poll.")
        # ---------------------------------
        elif self.path in ("/", "/pnl"):
            data = _DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass  # silence HTTP access logs from cluttering bot output


def start_http_server():
    server = HTTPServer(("0.0.0.0", HTTP_PORT), _PnLHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[HTTP] PnL dashboard running on port %d", HTTP_PORT)


# ── Main loop -----------------------------------------------------------------

def main():
    global last_pnl_snapshot
    global pnl_history
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
    start_http_server()
    client       = build_client()
    last_status  = time.time()
    last_window  = None
    executor     = ThreadPoolExecutor(max_workers=len(ASSETS))

    secs_left = 0  # init so KeyboardInterrupt handler always has a value
    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = WINDOW_SECS - secs_into

            # New window — reset everything (each 15m window is independent)
            if last_window is not None and window_start != last_window:
                if open_positions:
                    log.info("[WINDOW] Closing %d stale positions from prior window", len(open_positions))
                    open_positions.clear()
                armed_logged = False
                token_cache.clear()
                price_history.clear()
                live_prices.clear()
                traded_this_window.clear()
                log.info("[WINDOW] New window  ts=%d  secs_left=%d  settle=%ds  armed at %ds",
                         window_start, secs_left, SETTLE_SECS, SETTLE_SECS)


            if secs_into >= SETTLE_SECS and not armed_logged:
                log.debug("[ARMED] Window armed — scanning for panic triggers")
                armed_logged = True
            scan_markets(client, window_start, secs_into, server_ts, executor)
            manage_positions(client)
            save_state()

        except KeyboardInterrupt:
            log.info("Shutting down...")
            executor.shutdown(wait=False)
            print_status(secs_left)
            break
        except Exception as e:
            log.error("Unexpected error: %s", e, exc_info=True)
            executor.shutdown(wait=False)
            executor = ThreadPoolExecutor(max_workers=len(ASSETS))

        if time.time() - last_status >= 300:
            print_status()
            last_status = time.time()

        # Record PnL snapshot every 5 minutes for chart
        def your_loop_function():
          global last_pnl_snapshot
        
        now_t = time.time()
        if now_t - last_pnl_snapshot >= 1800:
            last_pnl_snapshot = now_t
            pnl_history.append({
                "ts":    datetime.now().strftime("%H"),
                "pnl":   round(stats["pnl"], 4),
            })
            if len(pnl_history) > 288:  # keep max 24h of 5-min snapshots
                pnl_history.pop(0)

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
