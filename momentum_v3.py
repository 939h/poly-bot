"""
Polymarket 15-Min Up/Down Bot — momentum bot v3
============================================
Strategy:
  Buy YES or NO when price hits BUY_PRICE_MIN–BUY_PRICE_MAX between
  ENTRY_AFTER and STOP_BUY_AT seconds into window.

  Gap guard (pre-buy, inverted vs panic_rsi):
    Checks abs(binance_live_close - binance_candle_open) >= threshold.
    ALLOW buy if gap is large enough (confirms real momentum).
    WAIT up to GAP_WAIT_SECS for gap to widen. If still too small → blacklist.

  Sell all at SELL_PRICE (99c).
  Cut loss at CUT_LOSS_PCT of buy price → immediately flip to opposite side
    if opposite is between FLIP_MIN and FLIP_MAX (once per window).
  Flip trades: sell at 99c only, hold to resolution if window closes.

Requirements:
    pip install py-clob-client-v2 python-dotenv requests colorama websocket-client

.env keys:
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
    DRY_RUN=true
    SIMULATE_NORMAL_BUY_ONLY=false
    BUY_AMOUNT=2
"""

import os
import sys
import json
import math
import time
import logging
import threading
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from collections import deque

import requests
import numpy as np
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    import colorama
    from colorama import Fore, Style
    colorama.init()
    COLORS = True
except ImportError:
    COLORS = False

try:
    from py_clob_client_v2 import (
        ClobClient,
        MarketOrderArgs,
        OrderType,
        ApiCreds,
        BalanceAllowanceParams,
        AssetType,
        Side,
    )
    from py_clob_client_v2.constants import POLYGON
    BUY  = Side.BUY
    SELL = Side.SELL
except ImportError:
    print("Run: pip install py-clob-client-v2 python-dotenv requests colorama websocket-client")
    sys.exit(1)

load_dotenv()

from binance_ws import candle_open, live_close, start_rsi_feed

# ── Logging ───────────────────────────────────────────────────────────────────

class _ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if not COLORS:
            return msg
        if any(t in msg for t in ("[BUY]", "[OPEN]", "[SELL]", "[WIN]", "[FORCE-SELL]")):
            return Fore.GREEN + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[CUT-LOSS]", "[LOSS]", "[REBOUND-DEAD]", "[REBOUND-CAP]", "[-DISCARD]")):
            return Fore.RED + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[FLIP]", "[REBOUND-FLIP]",  "[FORCE-STOP]")):
            return Fore.CYAN + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[GAP-ALLOW]",)):
            return Fore.GREEN + msg + Style.RESET_ALL
        if any(t in msg for t in ("[GAP-WAIT]", "[GAP-BLOCK]")):
            return Fore.MAGENTA + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[WINDOW]", "[MARKET]", "[STATUS]")):
            return Fore.CYAN + msg + Style.RESET_ALL
        if "[DRY-RUN]" in msg:
            return Fore.MAGENTA + msg + Style.RESET_ALL
        if record.levelno >= logging.ERROR:
            return Fore.RED + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[-WAIT]",)):
            return Fore.YELLOW + msg + Style.RESET_ALL
        if record.levelno >= logging.WARNING:
            return Fore.YELLOW + msg + Style.RESET_ALL
        return msg

_fmt = "%(asctime)s [%(levelname)s] %(message)s"
_console = logging.StreamHandler(
    open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
)
_console.setFormatter(_ColorFormatter(_fmt))
_file = logging.FileHandler("fresh_bot23.log", encoding="utf-8")
_file.setFormatter(logging.Formatter(_fmt))
logging.basicConfig(level=logging.INFO, handlers=[_console, _file], force=True)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# =============================================================================
#  USER SETTINGS
# =============================================================================

ASSETS         = ["btc", "eth", "sol", "xrp"]

DRY_RUN        = os.getenv("DRY_RUN", "true").lower() != "false"
SIMULATE_NORMAL_BUY_ONLY = os.getenv("SIMULATE_NORMAL_BUY_ONLY", "false").lower() == "true"
SIMULATE_REBOUND_MODE_ENABLED = os.getenv("SIMULATE_REBOUND_MODE_ENABLED", "false").lower() == "true"
BUY_AMOUNT     = float(os.getenv("BUY_AMOUNT", "3"))   # USDC per trade
REBOUND_BUY_AMOUNT = float(os.getenv("REBOUND_BUY_AMOUNT", str(BUY_AMOUNT)))  # USDC for rebound trades; defaults to BUY_AMOUNT if not set

# ── Buy trigger ───────────────────────────────────────────────────────────────
BUY_PRICE_MIN  = 1.00   # buy if price >= this
BUY_PRICE_MAX  = 1.00   # buy if price <= this
ENTRY_AFTER    = 30    # seconds into window before buying allowed (5 min)
STOP_BUY_AT    = 840    # seconds into window after which no new buys (13.5 min)
TREND_GUARD_PRICE = 0.65
TREND_GUARD_MIN_CONFIRMATIONS = 2


# ── Gap guard (inverted — large gap ALLOWS buy) ───────────────────────────────
# abs(binance_live_close - binance_candle_open) >= threshold → allow buy
# threshold = candle_open × GAP_SWING[asset] × GAP_MAGNITUDE[stage]
GAP_SWING = {
    "btc": 0.001,    # 0.1% of BTC open
    "eth": 0.001,   # 0.15% of ETH open
    "sol": 0.001,    # 0.1% of SOL open
    "xrp": 0.001,    # 0.2% of XRP open
}
GAP_MAGNITUDE = {
    "early": 1.0,   # 0–5 min
    "mid":   0.6,   # 5–10 min
    "late":  0.5,   # 10–15 min
}
GAP_WAIT_SECS = {
    "early": 300,
    "mid": 60,
    "late": 30,
}   # wait this long for gap to widen before blacklisting

# ── Exit ──────────────────────────────────────────────────────────────────────
SELL_MULTIPLIER = float(os.getenv("SELL_MULTIPLIER", "1.4"))
SELL_CAP        = float(os.getenv("SELL_CAP", "0.94"))
CUT_LOSS_PCT   = float(os.getenv("CUT_LOSS_PCT", "0.6"))   # if 0.65, u loss 35%
HOLD_EARLY_SECS = 30    # force-stop cooldown 0–5 min
HOLD_MID_SECS   = 5    # force-stop cooldown 5–10 min
HOLD_LATE_SECS  = 2    # force-stop cooldown 10–15 min
FORCE_SELL_GAP_MULT = float(os.getenv("FORCE_SELL_GAP_MULT", "4"))

# ── Flip ──────────────────────────────────────────────────────────────────────
FLIP_MIN       = 0.20   # flip only if opposite >= this
FLIP_MAX       = 0.75   # flip only if opposite <= this
REBOUND_CUTLOSS_MULT = float(os.getenv("REBOUND_CUTLOSS_MULT", "1.1"))
REBOUND_CUTLOSS_DEAD_ZONE = float(os.getenv("REBOUND_CUTLOSS_DEAD_ZONE", "0.04"))
REBOUND_CUTLOSS_CAP = float(os.getenv("REBOUND_CUTLOSS_CAP", "0.80"))
REBOUND_STOP_BUY_AT = int(os.getenv("REBOUND_STOP_BUY_AT", str(STOP_BUY_AT)))
REBOUND_SELL_MULTIPLIER = float(os.getenv("REBOUND_SELL_MULTIPLIER", "1.7"))
REBOUND_MAX_TARGET_PRICE = float(os.getenv("REBOUND_MAX_TARGET_PRICE", "0.95"))

# ── Spread guard ─────────────────────────────────────────────────────────────
MAX_BOOK_SPREAD        = 0.03
SPREAD_MAX_RETRIES     = 20
FORCE_STOP_SPREAD_RETRIES = 20
COOLDOWN_SEC           = int(os.getenv("COOLDOWN_SEC", "30"))

# ── 3v1 opposite-direction mode ──────────────────────────────────────────────
OPPO_MODE_ENABLED      = os.getenv("OPPO_MODE_ENABLED", "true").lower() == "true"
OPPO_WINDOW_START_SEC  = int(os.getenv("OPPO_WINDOW_START_SEC", "60"))
OPPO_PRICE_HIGH        = float(os.getenv("OPPO_PRICE_HIGH", "0.50"))
OPPO_MAX_PRICE         = float(os.getenv("OPPO_MAX_PRICE", "0.30"))
OPPO_MIN_PRICE         = float(os.getenv("OPPO_MIN_PRICE", "0.03"))
OPPO_GAP_MAG           = float(os.getenv("OPPO_GAP_MAG", "0.5"))
OPPO_SELL_MULTIPLIER   = float(os.getenv("OPPO_SELL_MULTIPLIER", "5.0"))
OPPO_SELL_CAP          = float(os.getenv("OPPO_SELL_CAP", "0.75"))
OPPO_CUT_LOSS_PCT      = float(os.getenv("OPPO_CUT_LOSS_PCT", "0.20"))
OPPO_REBOUND_MULT      = float(os.getenv("OPPO_REBOUND_MULT", "1.3"))
OPPO_DEAD_ZONE         = float(os.getenv("OPPO_DEAD_ZONE", "0.03"))
OPPO_FIRST_SELL_FRACTION = 0.50
OPPO_FIRST_SELL_MULTIPLIER = 2.0
OPPO_FINAL_SELL_MULTIPLIER = 5.0

# ── Timing ────────────────────────────────────────────────────────────────────
POLL_SECS              = 1.0
WINDOW_SECS            = 900
HOLD_POSITION_LIMIT_SECS = int(os.getenv("HOLD_POSITION_LIMIT_SECS", "400"))

# ── Trading windows (optional) ────────────────────────────────────────────────
TRADING_WINDOWS_ENABLED = False
TRADING_TZ_OFFSET_HRS   = 8
TRADING_WINDOWS         = [(12, 30, 16, 0), (18, 0, 20, 0), (23, 0, 23, 59), (0, 0, 4, 0)]

# ── Misc ──────────────────────────────────────────────────────────────────────
EXIT_RETRY_COOLDOWN_SECS = 1
SELL_MAX_ATTEMPTS        = 5
SELL_RETRY_DELAY_SECS    = 0.5
MIN_SELL_SHARES          = 0.001
CRYPTO_TAKER_FEE_RATE    = float(os.getenv("CRYPTO_TAKER_FEE_RATE", "0.072"))
gap_mag_vol = 1.0



def validate_settings():
    errors = []
    if REBOUND_BUY_AMOUNT <= 0:
        errors.append("REBOUND_BUY_AMOUNT must be > 0")
    if FORCE_SELL_GAP_MULT <= 0:
        errors.append("FORCE_SELL_GAP_MULT must be > 0")
    if REBOUND_CUTLOSS_MULT <= 1.0:
        errors.append("REBOUND_CUTLOSS_MULT must be > 1.0")
    if REBOUND_CUTLOSS_DEAD_ZONE < 0:
        errors.append("REBOUND_CUTLOSS_DEAD_ZONE must be >= 0")
    if REBOUND_CUTLOSS_CAP <= REBOUND_CUTLOSS_DEAD_ZONE:
        errors.append("REBOUND_CUTLOSS_CAP must be greater than REBOUND_CUTLOSS_DEAD_ZONE")
    if REBOUND_CUTLOSS_CAP >= 1.0:
        errors.append("REBOUND_CUTLOSS_CAP must be < 1.0")
    if REBOUND_STOP_BUY_AT < 0:
        errors.append("REBOUND_STOP_BUY_AT must be >= 0")
    if REBOUND_SELL_MULTIPLIER <= 0:
        errors.append("REBOUND_SELL_MULTIPLIER must be > 0")
    if not 0 < REBOUND_MAX_TARGET_PRICE < 1:
        errors.append("REBOUND_MAX_TARGET_PRICE must be between 0 and 1")
    if errors:
        for err in errors:
            log.error("[CONFIG] %s", err)
        sys.exit(1)

# =============================================================================
#  INTERNAL CONSTANTS
# =============================================================================
GAMMA_API  = "https://gamma-api.polymarket.com"
CLOB_API   = "https://clob.polymarket.com"
INTERVAL   = "15m"
HTTP_PORT  = int(os.getenv("PORT", 8080))

# ── State ─────────────────────────────────────────────────────────────────────
open_positions     = {}   # key ("eth_yes") -> position dict
token_cache        = {}   # window_start -> {asset: (yes_tok, no_tok)}
live_prices        = {}   # "eth_yes" / "eth_no" -> float
traded_this_window  = set()
flipped_this_window = set()   # assets that have already used their flip this window
gap_wait            = {}   # asset -> {triggered_at, key, token, price}
peak_gap            = {}   # asset -> float, highest gap seen this window
armed_logged       = False
last_entry_ts       = {}   # asset -> unix seconds of last buy
oppo_bought_windows = set()  # window_start set when oppo mode already bought
skip_buy_until_window = None  # window_start ts (exclusive): skip buys until this window start
skip_log_window = None        # throttle skip log to once per window
normal_blacklisted_assets = set()  # assets blacklisted for normal buys this window
trend_guarded_assets = set()       # assets blocked by trend guard this window
oppo_rebound_tracker = {}          # key asset_side -> trough price
oppo_last_trigger = {}             # key asset_side -> latest oppo trigger/status for dashboard
oppo_log_suppressed_until = 0.0    # unix ts; temporarily suppress OPPO log repopulation after manual reset

def record_oppo_trigger(opp_key, opp_asset, side, opp_price, status, detail=""):
    if time.time() < oppo_log_suppressed_until:
        return
    oppo_last_trigger[opp_key] = {
        "asset": opp_asset,
        "side": side,
        "price": opp_price,
        "status": status,
        "detail": detail,
        "updated": datetime.now().strftime("%H:%M:%S"),
    }
rebound_cutloss_tracker = {}       # key asset_side -> rebound tracking state after cut-loss

pnl_history        = []
asset_history      = {}
trade_log          = []
oppo_trigger_log   = []
last_pnl_snapshot  = 0

_skip_first_window = False
_startup_window_ts = None

stats = {
    "scans":    0,
    "triggers": 0,
    "buys":     0,
    "wins":     0,
    "losses":   0,
    "pnl":      0.0,
}

STATE_FILE = "bot_state.json"

# ── Persistence ───────────────────────────────────────────────────────────────

def load_state():
    global stats, pnl_history, asset_history, trade_log, last_pnl_snapshot
    if not os.path.exists(STATE_FILE):
        log.info("[STATE] No saved state — starting fresh")
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        saved_stats = saved.get("stats", {})
        for k in stats:
            if k in saved_stats:
                stats[k] = saved_stats[k]
        pnl_history   = saved.get("pnl_history", [])
        asset_history = saved.get("asset_history", {})
        trade_log     = saved.get("trade_log", [])
        if pnl_history:
            last_pnl_snapshot = time.time()
        log.info(
            "[STATE] Restored — buys=%d  wins=%d  losses=%d  pnl=$%.4f  "
            "trades=%d  pnl_pts=%d",
            stats["buys"], stats["wins"], stats["losses"], stats["pnl"],
            len(trade_log), len(pnl_history),
        )
    except Exception as e:
        log.warning("[STATE] Load failed: %s — starting fresh", e)


def reset_state():
    global stats, pnl_history, asset_history, trade_log, oppo_trigger_log, last_pnl_snapshot
    stats = {"scans": 0, "triggers": 0, "buys": 0, "wins": 0, "losses": 0, "pnl": 0.0}
    pnl_history   = []
    asset_history = {}
    trade_log     = []
    oppo_trigger_log = []
    last_pnl_snapshot = 0
    log.info("[STATE] Reset by user")
    save_state()

def reset_oppo_log():
    global oppo_log_suppressed_until
    oppo_last_trigger.clear()
    oppo_trigger_log.clear()
    # Prevent immediate re-population from the very next scan cycle.
    oppo_log_suppressed_until = time.time() + max(2.0, POLL_SECS * 3)
    log.info("[STATE] OPPO trigger log reset by user")
    save_state()


def get_position_live_price(key, fallback):
    price = live_prices.get(key)
    if price is not None and price > 0:
        return price
    if key.endswith("_oppo"):
        base_key = key[:-5]
        base_price = live_prices.get(base_key)
        if base_price is not None and base_price > 0:
            return base_price
    return fallback

def save_state():
    positions_out = {}
    for k, p in open_positions.items():
        entry    = p["entry_price"]
        curr     = get_position_live_price(k, entry)
        target   = p["sell_price"]
        cut      = p["cut_loss_price"]
        pnl_unreal = round(p.get("realized_revenue", 0.0) + (curr * p["net_shares"]) - p.get("cost", 0.0), 4)
        pct      = round((curr - entry) / (target - entry) * 100, 1) if target != entry else 0
        positions_out[k] = {
            "entry":       round(entry, 4),
            "current":     round(curr, 4),
            "target":      round(target, 4),
            "cut_loss":    round(cut, 4),
            "is_flip":     p.get("is_flip", False),
            "is_rebound":  p.get("is_rebound", False),
            "is_oppo":     p.get("is_oppo", k.endswith("_oppo")),
            "rebound_tranches": p.get("rebound_tranches", []),
            "cut_loss_pct": round(p.get("cut_loss_pct", OPPO_CUT_LOSS_PCT if k.endswith("_oppo") else CUT_LOSS_PCT), 4),
            "pnl":         pnl_unreal,
            "pct":         max(0, min(100, pct)),
            "opened_at":   p.get("opened_at", "—"),
            "rebound_buy_amount": REBOUND_BUY_AMOUNT,
        }
    gap_out = {}
    gap_threshold_out = {}
    for a in ASSETS:
        c_open = candle_open.get(a, 0.0)
        c_live = live_close.get(a)
        if c_open > 0:
            swing = GAP_SWING.get(a, 0.001)
            gap_threshold_out[a] = {
                "early": round(c_open * swing * GAP_MAGNITUDE["early"], 4),
                "mid":   round(c_open * swing * GAP_MAGNITUDE["mid"], 4),
                "late":  round(c_open * swing * GAP_MAGNITUDE["late"], 4),
            }
        else:
            gap_threshold_out[a] = None
        if c_open > 0 and c_live is not None:
            gap_out[a] = round(abs(c_live - c_open), 4)
        else:
            gap_out[a] = None
    state = {
        "updated":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dry_run":       DRY_RUN,
        "stats":         dict(stats),
        "positions":     positions_out,
        "prices":        dict(live_prices),
        "gap":           gap_out,
        "gap_threshold": gap_threshold_out,
        "pnl_history":   list(pnl_history),
        "asset_history": dict(asset_history),
        "trade_log":     list(trade_log),
        "oppo_trigger_log": list(oppo_trigger_log),
        "settings": {
            "assets":     ASSETS,
            "buy_min":    BUY_PRICE_MIN,
            "buy_max":    BUY_PRICE_MAX,
            "sell_multiplier": SELL_MULTIPLIER,
            "sell_cap":   SELL_CAP,
            "cut_loss":   CUT_LOSS_PCT,
            "oppo_cut_loss": OPPO_CUT_LOSS_PCT,
            "flip_min":   FLIP_MIN,
            "flip_max":   FLIP_MAX,
            "force_sell_gap_mult": FORCE_SELL_GAP_MULT,
            "rebound_cutloss_mult": REBOUND_CUTLOSS_MULT,
            "rebound_cutloss_dead_zone": REBOUND_CUTLOSS_DEAD_ZONE,
            "rebound_cutloss_cap": REBOUND_CUTLOSS_CAP,
            "rebound_stop_buy_at": REBOUND_STOP_BUY_AT,
            "rebound_sell_multiplier": REBOUND_SELL_MULTIPLIER,
            "rebound_max_target_price": REBOUND_MAX_TARGET_PRICE,
            "simulate_rebound_mode_enabled": SIMULATE_REBOUND_MODE_ENABLED,
            "order":      BUY_AMOUNT,
            "poll":       POLL_SECS,
            "entry_after": ENTRY_AFTER,
            "stop_buy":   STOP_BUY_AT,
        },
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log.debug("save_state error: %s", e)

# ── Record helpers ────────────────────────────────────────────────────────────

def _record_closed_trade(key, pnl):
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


def _record_trade_log(key, pos, exit_type, close_price, pnl):
    parts = key.split("_", 1)
    record = {
        "time":     pos.get("opened_at", "—"),
        "asset":    parts[0].upper(),
        "side":     parts[1].upper() if len(parts) > 1 else "—",
        "entry":    round(pos["entry_price"], 2),
        "target":   round(pos["sell_price"], 2),
        "exit":     exit_type,
        "exit_px":  round(close_price, 2),
        "is_flip":  pos.get("is_flip", False),
        "is_rebound": pos.get("is_rebound", False),
        "pnl":      round(pnl, 4),
    }
    trade_log.insert(0, record)
    if len(trade_log) > 200:
        trade_log.pop()


def _record_oppo_trigger(asset, side, price, status, reason):
    oppo_trigger_log.insert(0, {
        "time": datetime.now().strftime("%H:%M:%S"),
        "asset": asset.upper(),
        "side": side.upper(),
        "price": round(float(price), 4) if price is not None else None,
        "status": status,
        "reason": reason,
    })
    
# ── CLOB helpers ──────────────────────────────────────────────────────────────

def build_client():
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        log.error("Missing .env keys.")
        sys.exit(1)
    client = ClobClient(
        host=CLOB_API, chain_id=POLYGON, key=pk,
        creds=ApiCreds(api_key=api_key, api_secret=api_sec, api_passphrase=api_pass),
        signature_type=1, funder=funder or None,
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
    if not TRADING_WINDOWS_ENABLED:
        return True
    local_dt = datetime.fromtimestamp(server_ts, tz=timezone.utc) + timedelta(hours=TRADING_TZ_OFFSET_HRS)
    now_mins = local_dt.hour * 60 + local_dt.minute
    for window in TRADING_WINDOWS:
        if len(window) == 2:
            sh, eh = window; sm, em = 0, 0
        else:
            sh, sm, eh, em = window
        start = (int(sh) % 24) * 60 + int(sm)
        end   = (int(eh) % 24) * 60 + int(em)
        if start == end:
            return True
        if start < end and start <= now_mins < end:
            return True
        if start > end and (now_mins >= start or now_mins < end):
            return True
    return False


def build_slug(asset, window_ts):
    return f"{asset}-updown-{INTERVAL}-{window_ts}"


def fetch_market_by_slug(slug):
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=10)
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if markets:
            return markets[0]
        r2 = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=10)
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
    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = [o.strip() for o in outcomes.split(",") if o.strip()]
    if not raw or len(raw) < 2:
        return None, None
    if outcomes and len(outcomes) == len(raw):
        yes_idx = next((i for i, o in enumerate(outcomes) if str(o).lower() in ("yes", "up")), None)
        no_idx  = next((i for i, o in enumerate(outcomes) if str(o).lower() in ("no", "down")), None)
        if yes_idx is not None and no_idx is not None:
            return raw[yes_idx].strip(), raw[no_idx].strip()
    return raw[0].strip(), raw[1].strip()


def get_midpoint(client, token_id):
    try:
        return float(client.get_midpoint(token_id)["mid"])
    except Exception:
        return 0.0


def get_spread_value(client, token_id):
    try:
        resp = client.get_spread(token_id)
        if isinstance(resp, dict):
            val = resp.get("spread") or resp.get("spread_amount") or resp.get("value")
        else:
            val = getattr(resp, "spread", None)
        if val is None:
            return None
        return float(val)
    except Exception:
        return None


def market_buy(client, token_id, label, price_hint=None, amount=None, simulate=False):
    amount = round(amount if amount is not None else BUY_AMOUNT, 4)
    def _estimate_buy_shares(entry_price):
        if entry_price <= 0:
            return 0.0
        gross_shares = amount / entry_price
        # Polymarket buy fees are collected in shares: fee = C * r * p * (1 - p).
        fee_shares = gross_shares * CRYPTO_TAKER_FEE_RATE * (1 - entry_price)
        return round(max(gross_shares - fee_shares, 0.0), 3)
    if DRY_RUN or simulate:
        entry_est = float(price_hint or 0) or get_midpoint(client, token_id)
        est_shares = _estimate_buy_shares(entry_est)
        mode = "DRY-RUN" if DRY_RUN else "SIMULATED-BUY"
        log.info("[%s] MARKET BUY %s $%.2f USDC → est %.3f shares @ %.4f",
                 mode, label, amount, est_shares, entry_est)
        return {
            "ok": True, "resp": {"dry_run": DRY_RUN, "simulated": simulate},
            "filled_shares": est_shares,
            "filled_price": float(entry_est),
        }
    try:
        resp = client.create_and_post_market_order(
            order_args=MarketOrderArgs(token_id=token_id, amount=amount, side=BUY),
            order_type=OrderType.FOK,
        )
        log.info("[BUY] Executed %s | %s", label, resp)
        raw_taking = float(resp.get("takingAmount") or 0)
        raw_making = float(resp.get("makingAmount") or 0)
        filled_shares = round(raw_taking, 3)
        filled_price  = (raw_making / raw_taking) if raw_taking > 0 and raw_making > 0 else 0.0
        if raw_taking == 0 and raw_making == 0:
            log.warning("[BUY] %s FOK zero fill — order not executed", label)
            return {"ok": False, "resp": resp, "filled_shares": 0.0, "filled_price": 0.0}
        if filled_shares <= 0:
            entry_est = float(price_hint or 0) or get_midpoint(client, token_id)
            filled_shares = _estimate_buy_shares(entry_est)
            filled_price  = float(entry_est)
        elif filled_price <= 0:
            filled_price = float(price_hint or 0) or get_midpoint(client, token_id)
        try:
            client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
        except Exception as e:
            log.warning("[BUY] balance refresh failed for %s: %s", label, e)
        return {"ok": True, "resp": resp, "filled_shares": filled_shares, "filled_price": filled_price}
    except Exception as e:
        log.error("[BUY] Failed %s: %s", label, e)
        return {"ok": False, "resp": None, "filled_shares": 0, "filled_price": 0.0}


def market_sell(client, token_id, shares, price, label, simulate=False):
    sell_shares = round(shares, 3)
    if sell_shares < MIN_SELL_SHARES:
        return {"ok": False, "resp": None, "filled_shares": 0.0, "filled_quote": 0.0}
    if DRY_RUN or simulate:
        est = round(sell_shares * price, 4)
        mode = "DRY-RUN" if DRY_RUN else "SIM-SELL"
        log.info("[%s] MARKET SELL %s %.3f sh @ %.4f (est $%.4f)", mode, label, sell_shares, price, est)
        return {"ok": True, "resp": {"dry_run": DRY_RUN, "simulated": simulate}, "filled_shares": sell_shares, "filled_quote": est}
    try:
        client.update_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        )
    except Exception as e:
        log.warning("[SELL] pre-sell balance refresh failed %s: %s", label, e)
    attempt_shares = sell_shares
    for attempt in range(2):
        try:
            resp = client.create_and_post_market_order(
                order_args=MarketOrderArgs(token_id=token_id, amount=attempt_shares, side=SELL),
                order_type=OrderType.FAK,
            )
            log.info("[SELL] Executed %s | %s", label, resp)
            try:
                client.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
                )
            except Exception:
                pass
            raw_making  = float(resp.get("makingAmount") or 0)
            filled_sh   = round(raw_making, 3)
            filled_quote = float(resp.get("takingAmount") or 0)
            return {"ok": True, "resp": resp, "filled_shares": filled_sh, "filled_quote": filled_quote}
        except Exception as e:
            err = str(e)
            if attempt == 0 and "not enough balance / allowance" in err:
                bal_m = re.search(r"balance:\s*(\d+)", err)
                amt_m = re.search(r"order amount:\s*(\d+)", err)
                if bal_m and amt_m:
                    bal_raw = int(bal_m.group(1))
                    amt_raw = int(amt_m.group(1))
                    if 0 < bal_raw < amt_raw:
                        ratio = bal_raw / amt_raw
                        retry = round(attempt_shares * ratio * 0.999, 3)
                        if retry >= MIN_SELL_SHARES:
                            log.warning("[SELL] %s size %.3f → %.3f to match balance", label, attempt_shares, retry)
                            attempt_shares = retry
                            continue
            log.error("[SELL] Failed %s: %s", label, e)
            return {"ok": False, "resp": None, "filled_shares": 0, "filled_quote": 0.0}
    return {"ok": False, "resp": None, "filled_shares": 0, "filled_quote": 0.0}


def market_sell_with_retries(client, token_id, shares, price, label, simulate=False):
    last = {"ok": False, "resp": None, "filled_shares": 0}
    for attempt in range(1, SELL_MAX_ATTEMPTS + 1):
        last = market_sell(client, token_id, shares, price, label, simulate=simulate)
        if last["ok"]:
            if attempt > 1:
                log.info("[SELL-RETRY] %s ok on attempt %d/%d", label, attempt, SELL_MAX_ATTEMPTS)
            return last
        if attempt < SELL_MAX_ATTEMPTS:
            time.sleep(SELL_RETRY_DELAY_SECS)
    log.error("[SELL-RETRY] %s failed after %d attempts", label, SELL_MAX_ATTEMPTS)
    return last

# ── Gap guard (inverted — large gap ALLOWS buy) ───────────────────────────────

def check_gap_guard(asset, secs_into):
    """
    Returns True  → gap large enough → ALLOW buy.
    Returns False → gap too small → wait/block.

    Safe fallback: data not ready → allow (True).
    """
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)

    if c_open <= 0.0 or c_live is None:
        log.debug("[GAP-GUARD] %s data not ready — allowing by default", asset.upper())
        return True   # allow when data unavailable

    stage = get_stage(secs_into)

    swing     = GAP_SWING.get(asset, 0.001)
    magnitude = GAP_MAGNITUDE[stage]
    threshold = c_open * swing * magnitude
    actual    = abs(c_live - c_open)

    log.debug(
        "[GAP-GUARD] %s  open=%.4f  live=%.4f  actual=%.4f  threshold=%.4f"
        "  (swing=%.4f × mag=%.1f  stage=%s)",
        asset.upper(), c_open, c_live, actual, threshold, swing, magnitude, stage,
    )

    if actual >= threshold:
        log.info("[GAP-ALLOW] %s  gap %.4f >= threshold %.4f → buy allowed",
                 asset.upper(), actual, threshold)
        return True   # gap large enough — allow

    return False   # gap too small — wait/block



def get_gap_threshold(asset, secs_into, multiplier=1.0):
    c_open = candle_open.get(asset, 0.0)
    if c_open <= 0.0:
        return None
    stage = get_stage(secs_into)
    return c_open * GAP_SWING.get(asset, 0.001) * GAP_MAGNITUDE[stage] * multiplier


def get_stage(secs_into):
    if secs_into < 300:
        return "early"
    if secs_into < 600:
        return "mid"
    return "late"


def get_binance_gap(asset):
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)
    if c_open <= 0.0 or c_live is None:
        return None
    return abs(c_live - c_open)


def force_sell_gap_triggered(asset, secs_into):
    threshold = get_gap_threshold(asset, secs_into, FORCE_SELL_GAP_MULT)
    actual = get_binance_gap(asset)
    if threshold is None or actual is None:
        return False, actual, threshold
    return actual >= threshold, actual, threshold

# ── Position management ───────────────────────────────────────────────────────

def open_position(key, token_id, entry_price, filled_shares=None, window_start=None,
                  is_flip=False, is_rebound=False, buy_amount=None, is_simulated=False):
    amount = buy_amount if buy_amount is not None else BUY_AMOUNT
    if filled_shares is not None and filled_shares > 0:
        net_shares = round(float(filled_shares), 3)
    else:
        gross_shares = amount / entry_price if entry_price > 0 else 0.0
        fee_shares = gross_shares * CRYPTO_TAKER_FEE_RATE * (1 - entry_price)
        net_shares = round(max(gross_shares - fee_shares, 0.0), 3)
    is_oppo = key.endswith("_oppo")
    sell_mult = OPPO_SELL_MULTIPLIER if is_oppo else (REBOUND_SELL_MULTIPLIER if is_rebound else SELL_MULTIPLIER)
    sell_cap = OPPO_SELL_CAP if is_oppo else SELL_CAP
    if is_rebound:
        rebound_5x_target = min(round(entry_price * REBOUND_SELL_MULTIPLIER, 4), REBOUND_MAX_TARGET_PRICE)
        sell_price = rebound_5x_target
    elif is_oppo:
        sell_price = min(round(entry_price * OPPO_FIRST_SELL_MULTIPLIER, 4), OPPO_SELL_CAP)
    else:
        sell_price = min(round(entry_price * sell_mult, 4), sell_cap)
    cut_loss_pct = OPPO_CUT_LOSS_PCT if is_oppo else CUT_LOSS_PCT
    cut_loss_price = round(entry_price * cut_loss_pct, 4)

    rebound_tranches = []
    if is_rebound:
        rebound_tranches = [
            {
                "name": "REBOUND",
                "target": min(round(entry_price * REBOUND_SELL_MULTIPLIER, 4), REBOUND_MAX_TARGET_PRICE),
                "shares": net_shares,
                "sold": False,
            },
        ]
    oppo_tranches = []
    if is_oppo:
        oppo_first_shares = round(net_shares * OPPO_FIRST_SELL_FRACTION, 3)
        oppo_final_shares = round(max(net_shares - oppo_first_shares, 0.0), 3)
        oppo_tranches = [
            {
                "name": "2X",
                "target": min(round(entry_price * OPPO_FIRST_SELL_MULTIPLIER, 4), OPPO_SELL_CAP),
                "shares": oppo_first_shares,
                "sold": False,
            },
            {
                "name": "5X",
                "target": min(round(entry_price * OPPO_FINAL_SELL_MULTIPLIER, 4), OPPO_SELL_CAP),
                "shares": oppo_final_shares,
                "sold": False,
            },
        ]

    open_positions[key] = {
        "token_id":             token_id,
        "entry_price":          entry_price,
        "sell_price":           sell_price,
        "cut_loss_price":       cut_loss_price,
        "cut_loss_pct":         cut_loss_pct,
        "is_oppo":              is_oppo,
        "net_shares":           net_shares,
        "cost":                 round(amount, 4),
        "realized_revenue":     0.0,
        "is_flip":              is_flip,
        "is_rebound":           is_rebound,
        "rebound_tranches":     rebound_tranches,
        "oppo_tranches":        oppo_tranches,
        "force_stop_triggered": None,
        "force_stop_cooldown":  None,
        "force_stop_spread_retries": 0,
        "last_exit_attempt_ts": 0.0,
        "opened_at":            datetime.now().strftime("%H:%M"),
        "opened_ts":            time.time(),
        "window_start":         window_start,
        "is_simulated":         is_simulated,
    }
    base_asset = key.split("_")[0]
    last_entry_ts[base_asset] = time.time()
    stats["buys"] += 1
    tag = "REBOUND FLIP " if is_rebound else ("FLIP " if is_flip else "")
    log.info(
        "[OPEN] %s%s  entry=%.4f  shares=%.3f  sell=%.4f  cut-loss=%.4f (%.0f%%)",
        tag, key, entry_price, net_shares, sell_price, cut_loss_price, cut_loss_pct * 100,
    )
    if is_rebound:
        log.info(
            "[REBOUND-TARGET] %s  100%% @ %.4f",
            key, rebound_tranches[0]["target"],
        )



def update_rebound_sell_price(pos):
    unsold_targets = [
        float(t["target"]) for t in pos.get("rebound_tranches", [])
        if not t.get("sold") and float(t.get("shares", 0.0)) >= MIN_SELL_SHARES
    ]
    if unsold_targets:
        pos["sell_price"] = min(unsold_targets)


def manage_rebound_target_sells(client, key, pos, current_price):
    for tranche in pos.get("rebound_tranches", []):
        if tranche.get("sold"):
            continue
        target = float(tranche.get("target", 0.0))
        tranche_shares = round(float(tranche.get("shares", 0.0)), 3)
        available_shares = round(float(pos.get("net_shares", 0.0)), 3)
        sell_shares = min(tranche_shares, available_shares)

        if sell_shares < MIN_SELL_SHARES:
            tranche["sold"] = True
            continue
        if current_price < target:
            continue

        log.info(
            "[REBOUND-SELL-%s] %s price=%.4f >= target=%.4f selling %.3f/%.3f shares",
            tranche.get("name", "TRANCHE"), key, current_price, target, sell_shares, available_shares,
        )
        pos["closing"] = True
        sell = market_sell_with_retries(
            client, pos["token_id"], sell_shares, current_price, key.upper(),
            simulate=pos.get("is_simulated", False),
        )
        pos["last_exit_attempt_ts"] = time.time()
        if not sell["ok"]:
            pos["closing"] = False
            log.warning("[REBOUND-SELL-%s] %s sell failed — will retry on next loop", tranche.get("name", "TRANCHE"), key)
            return False

        filled_shares = round(float(sell.get("filled_shares") or sell_shares), 3)
        revenue = float(sell.get("filled_quote") or round(filled_shares * current_price, 4))
        pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
        pos["net_shares"] = round(max(available_shares - filled_shares, 0.0), 3)
        tranche["sold"] = True
        log.info(
            "[REBOUND-SELL-%s] %s partial finalized revenue=$%.4f remaining=%.3f",
            tranche.get("name", "TRANCHE"), key, revenue, pos["net_shares"],
        )

    pos["closing"] = False
    update_rebound_sell_price(pos)
    all_sold = all(t.get("sold") for t in pos.get("rebound_tranches", []))
    if all_sold or float(pos.get("net_shares", 0.0)) < MIN_SELL_SHARES:
        pnl = round(pos.get("realized_revenue", 0.0) - pos["cost"], 4)
        log.info("[REBOUND-SELL] %s finalized  pnl=$%.4f", key, pnl)
        stats["wins" if pnl > 0 else "losses"] += 1
        stats["pnl"] += pnl
        _record_closed_trade(key, pnl)
        _record_trade_log(key, pos, "REBOUND-SELL", current_price, pnl)
        return True

    return False


def update_oppo_sell_price(pos):
    unsold_targets = [
        float(t["target"]) for t in pos.get("oppo_tranches", [])
        if not t.get("sold") and float(t.get("shares", 0.0)) >= MIN_SELL_SHARES
    ]
    if unsold_targets:
        pos["sell_price"] = min(unsold_targets)


def manage_oppo_target_sells(client, key, pos, current_price):
    for tranche in pos.get("oppo_tranches", []):
        if tranche.get("sold"):
            continue
        target = float(tranche.get("target", 0.0))
        tranche_shares = round(float(tranche.get("shares", 0.0)), 3)
        available_shares = round(float(pos.get("net_shares", 0.0)), 3)
        sell_shares = min(tranche_shares, available_shares)

        if sell_shares < MIN_SELL_SHARES:
            tranche["sold"] = True
            continue
        if current_price < target:
            continue

        log.info(
            "[OPPO-SELL-%s] %s price=%.4f >= target=%.4f selling %.3f/%.3f shares",
            tranche.get("name", "TRANCHE"), key, current_price, target, sell_shares, available_shares,
        )
        pos["closing"] = True
        sell = market_sell_with_retries(
            client, pos["token_id"], sell_shares, current_price, key.upper(),
            simulate=pos.get("is_simulated", False),
        )
        pos["last_exit_attempt_ts"] = time.time()
        if not sell["ok"]:
            pos["closing"] = False
            log.warning("[OPPO-SELL-%s] %s sell failed — will retry on next loop", tranche.get("name", "TRANCHE"), key)
            return False

        filled_shares = round(float(sell.get("filled_shares") or sell_shares), 3)
        revenue = float(sell.get("filled_quote") or round(filled_shares * current_price, 4))
        pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
        pos["net_shares"] = round(max(available_shares - filled_shares, 0.0), 3)
        tranche["sold"] = True
        log.info(
            "[OPPO-SELL-%s] %s partial finalized revenue=$%.4f remaining=%.3f",
            tranche.get("name", "TRANCHE"), key, revenue, pos["net_shares"],
        )
        parts = key.split("_")
        if len(parts) >= 2:
            _record_oppo_trigger(parts[0], parts[1], current_price, "SELL", f"{tranche.get('name', 'TRANCHE').lower()}-filled")

    pos["closing"] = False
    update_oppo_sell_price(pos)
    all_sold = all(t.get("sold") for t in pos.get("oppo_tranches", []))
    if all_sold or float(pos.get("net_shares", 0.0)) < MIN_SELL_SHARES:
        pnl = round(pos.get("realized_revenue", 0.0) - pos["cost"], 4)
        log.info("[OPPO-SELL] %s finalized  pnl=$%.4f", key, pnl)
        stats["wins" if pnl > 0 else "losses"] += 1
        stats["pnl"] += pnl
        _record_closed_trade(key, pnl)
        _record_trade_log(key, pos, "OPPO-SELL", current_price, pnl)
        parts = key.split("_")
        if len(parts) >= 2:
            _record_oppo_trigger(parts[0], parts[1], current_price, "SOLD", f"final pnl={pnl:+.4f}")
        return True

    return False


def manage_positions(client, server_ts=None):
    to_close = []

    for key, pos in list(open_positions.items()):
      # SKIP if this position is already in the process of being sold
        if pos.get("closing"):
            continue
        now = time.time()
        if now - pos.get("last_exit_attempt_ts", 0.0) < EXIT_RETRY_COOLDOWN_SECS:
            continue

        current_price = get_position_live_price(key, None)
        if current_price is None or current_price <= 0:
            current_price = get_midpoint(client, pos["token_id"])
        if current_price <= 0:
            continue

        entry     = pos["entry_price"]
        shares    = pos["net_shares"]
        cut_loss  = pos["cut_loss_price"]
        is_flip   = pos.get("is_flip", False)
        hold_secs = now - float(pos.get("opened_ts", now))
        unrealized_revenue = round(pos.get("realized_revenue", 0.0) + (current_price * shares), 4)
        unrealized_pnl = round(unrealized_revenue - pos["cost"], 4)
        server_ts_now = server_ts if server_ts is not None else get_server_time()
        secs_into_now = server_ts_now - get_current_window_start(server_ts_now)

        # ── Force sell profitable positions when Binance gap overextends ─────
        gap_hit, actual_gap, force_threshold = force_sell_gap_triggered(key.split("_")[0], secs_into_now)
        if not pos.get("is_rebound") and unrealized_pnl > 0 and gap_hit:
            log.info(
                "[FORCE-SELL] %s  pnl=$%.4f  price=%.4f  gap=%.4f >= force-threshold=%.4f  selling %.3f shares",
                key, unrealized_pnl, current_price, actual_gap, force_threshold, shares,
            )
            pos["closing"] = True
            sell = market_sell_with_retries(
                client, pos["token_id"], shares, current_price, key.upper(),
                simulate=pos.get("is_simulated", False),
            )
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                revenue = float(sell.get("filled_quote") or round(shares * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                log.info("[FORCE-SELL] %s finalized  pnl=$%.4f", key, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                _record_trade_log(key, pos, "FORCE-SELL", current_price, pnl)
                to_close.append(key)
            else:
                pos["closing"] = False
                log.warning("[FORCE-SELL] %s sell failed — will retry on next loop", key)
            continue

        # ── Max holding time limit ───────────────────────────────────────────
        if HOLD_POSITION_LIMIT_SECS > 0 and hold_secs >= HOLD_POSITION_LIMIT_SECS:
            log.info("[TIME-LIMIT-SELL] %s  held=%ds/%ds  price=%.4f  selling %.3f shares",
                     key, int(hold_secs), HOLD_POSITION_LIMIT_SECS, current_price, shares)
            pos["closing"] = True
            sell = market_sell_with_retries(
                client, pos["token_id"], shares, current_price, key.upper(),
                simulate=pos.get("is_simulated", False),
            )
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                revenue = float(sell.get("filled_quote") or round(shares * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                log.info("[TIME-LIMIT-SELL] %s finalized  pnl=$%.4f", key, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                _record_trade_log(key, pos, "TIME-LIMIT-SELL", current_price, pnl)
                to_close.append(key)
            else:
                pos["closing"] = False
                log.warning("[TIME-LIMIT-SELL] %s sell failed — will retry on next loop", key)
            continue

        # ── Force stop (cut-loss) with cooldown ───────────────────────────────
        if current_price <= cut_loss:
            if pos.get("force_stop_triggered") is None:
                # Spread check on first trigger
                fsr = pos.get("force_stop_spread_retries", 0)
                spread = get_spread_value(client, pos["token_id"])
                if spread is not None and spread > MAX_BOOK_SPREAD:
                    fsr += 1
                    pos["force_stop_spread_retries"] = fsr
                    if fsr < FORCE_STOP_SPREAD_RETRIES:
                        log.info("[STOP-SPREAD-SKIP] %s  spread=%.4f  retry %d/%d",
                                 key, spread, fsr, FORCE_STOP_SPREAD_RETRIES)
                        continue
                    log.info("[STOP-SPREAD-FORCE] %s  forcing stop after %d retries", key, fsr)
                secs_in = secs_into_now
                if secs_in < 300:
                    cooldown, period = HOLD_EARLY_SECS, "early"
                elif secs_in < 600:
                    cooldown, period = HOLD_MID_SECS, "mid"
                else:
                    cooldown, period = HOLD_LATE_SECS, "late"
                pos["force_stop_triggered"] = now
                pos["force_stop_cooldown"]  = cooldown
                log.info("[STOP-WAIT] %s  price=%.4f  cut=%.4f  waiting %ds (%s)",
                         key, current_price, cut_loss, cooldown, period)
                continue

            cooldown  = pos.get("force_stop_cooldown", HOLD_LATE_SECS)
            secs_held = now - pos.get("force_stop_triggered", now)
            if secs_held < cooldown:
                log.info("[STOP-WAIT] %s  price=%.4f  %ds/%ds", key, current_price, int(secs_held), cooldown)
                continue

            # Confirmed cut-loss — sell all
            log.info("[CUT-LOSS] %s  price=%.4f  selling %.3f shares", key, current_price, shares)
            pos["closing"] = True
            sell = market_sell_with_retries(
                client, pos["token_id"], shares, current_price, key.upper(),
                simulate=pos.get("is_simulated", False),
            )
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                revenue = float(sell.get("filled_quote") or round(shares * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                log.info("[CUT-LOSS] %s finalized  pnl=$%.4f", key, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                _record_trade_log(key, pos, "CUT-LOSS", current_price, pnl)
                if pos.get("is_oppo") or key.endswith("_oppo"):
                    parts = key.split("_")
                    if len(parts) >= 2:
                        _record_oppo_trigger(parts[0], parts[1], current_price, "CUT-LOSS", f"pnl={pnl:+.4f}")
                to_close.append(key)

                # ── Rebound cut-loss flip: trace opposite side trough, then buy opposite side ──
                asset = key.split("_")[0]
                side = key.split("_")[1]
                flip_side = "no" if side == "yes" else "yes"
                flip_key = f"{asset}_{flip_side}"
                flip_token = get_token_for_key(asset, flip_side, pos.get("window_start"))
                if asset not in flipped_this_window and not pos.get("is_oppo") and flip_token:
                    flip_price = live_prices.get(flip_key)
                    if flip_price is None or flip_price <= 0:
                        flip_price = get_midpoint(client, flip_token)
                    if flip_price and flip_price > 0:
                        rebound_cutloss_tracker[flip_key] = {
                            "token": flip_token,
                            "trough": flip_price,
                            "window_start": pos.get("window_start"),
                            "armed_at": time.time(),
                        }
                        log.info(
                            "[REBOUND-FLIP] %s armed from %s cut-loss  trough=%.4f  need %.2fx rebound below cap %.4f; discard <= %.4f",
                            flip_key, key, flip_price, REBOUND_CUTLOSS_MULT, REBOUND_CUTLOSS_CAP, REBOUND_CUTLOSS_DEAD_ZONE,
                        )
                    else:
                        log.info("[REBOUND-FLIP] %s skipped — no valid price to arm", flip_key)
                else:
                    log.info("[REBOUND-FLIP] %s skipped — already flipped this window, oppo position, or missing token", asset.upper())
            else:
                pos["closing"] = False
                log.warning("[CUT-LOSS] %s sell failed — will retry on next loop", key)
            continue

        # Price recovered — reset cooldown
        if pos.get("force_stop_triggered") is not None:
            log.info("[STOP-CANCEL] %s  price=%.4f recovered above cut=%.4f",
                     key, current_price, cut_loss)
            pos["force_stop_triggered"] = None
            pos["force_stop_cooldown"]  = None
            pos["force_stop_spread_retries"] = 0

        # ── Rebound positions sell in two tranches ───────────────────────────
        if pos.get("is_rebound"):
            if manage_rebound_target_sells(client, key, pos, current_price):
                to_close.append(key)
            continue

        # ── Oppo positions sell in two tranches ─────────────────────────────
        if pos.get("is_oppo"):
            if manage_oppo_target_sells(client, key, pos, current_price):
                to_close.append(key)
            continue

        # ── Sell at target ────────────────────────────────────────────────────
        if current_price >= pos["sell_price"]:
            tag = "FLIP-SELL" if is_flip else "SELL"
            log.info("[%s] %s  price=%.4f  selling %.3f shares", tag, key, current_price, shares)

            pos["closing"] = True
          
            sell = market_sell_with_retries(
                client, pos["token_id"], shares, current_price, key.upper(),
                simulate=pos.get("is_simulated", False),
            )
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                revenue = float(sell.get("filled_quote") or round(shares * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                exit_type = "FLIP-SELL" if is_flip else "SELL"
                log.info("[%s] %s finalized  pnl=$%.4f", exit_type, key, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                _record_trade_log(key, pos, exit_type, current_price, pnl)
                to_close.append(key)
            else:
                pos["closing"] = False
                log.warning("[%s] %s sell failed — will retry on next loop", tag, key)

    for key in to_close:
        del open_positions[key]

# ── Market fetch + scan ───────────────────────────────────────────────────────

def get_token_for_key(asset, side, window_start=None):
    """Return token id for asset side ('yes'/'no') from cache."""
    side = (side or "").lower()
    if side not in ("yes", "no"):
        return None

    # Prefer explicit window first
    if window_start is not None:
        tokens = token_cache.get(window_start, {}).get(asset)
        if tokens:
            return tokens[0] if side == "yes" else tokens[1]

    # Fallback: newest cached window containing this asset
    for ws in sorted(token_cache.keys(), reverse=True):
        tokens = token_cache.get(ws, {}).get(asset)
        if tokens:
            return tokens[0] if side == "yes" else tokens[1]

    return None


def _fetch_asset(client, asset, window_start):
    if asset not in token_cache.get(window_start, {}):
        slug = build_slug(asset, window_start)
        mkt  = fetch_market_by_slug(slug)
        if not mkt:
            log.debug("No market: %s", slug)
            return None
        yes_tok, no_tok = get_tokens(mkt)
        if not yes_tok or not no_tok:
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
    yes_price = get_midpoint(client, yes_token)
    if yes_price <= 0:
        return None
    return (asset, yes_price, yes_token, no_token)


def _update_prices(result):
    if result is None:
        return
    asset, yes_price, yes_token, no_token = result
    no_price = round(1.0 - yes_price, 4)
    live_prices[f"{asset}_yes"] = yes_price
    live_prices[f"{asset}_no"]  = no_price
    # Update peak gap for volatility check
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)
    if c_open > 0 and c_live is not None:
        current_gap = abs(c_live - c_open)
        if current_gap > peak_gap.get(asset, 0.0):
            peak_gap[asset] = current_gap

def _trend_guard_ok(trigger_asset, trigger_side, results):
    confirmations = []
    for asset in ASSETS:
        if asset == trigger_asset:
            continue
        result = results.get(asset)
        if result is None:
            continue
        _, yes_price, _, _ = result
        side_price = yes_price if trigger_side == "yes" else round(1.0 - yes_price, 4)
        if side_price > TREND_GUARD_PRICE:
            confirmations.append(f"{asset}_{trigger_side}={side_price:.4f}")

    if len(confirmations) >= TREND_GUARD_MIN_CONFIRMATIONS:
        log.info("[TREND-GUARD] %s_%s confirmed by %s",
                 trigger_asset.upper(), trigger_side.upper(), ", ".join(confirmations))
        return True

    log.debug("[TREND-GUARD-BLOCK] %s_%s confirmed=%d/%d  need side > %.2f  matches=%s",
             trigger_asset.upper(), trigger_side.upper(),
             len(confirmations), TREND_GUARD_MIN_CONFIRMATIONS,
             TREND_GUARD_PRICE, ", ".join(confirmations) if confirmations else "none")
    return False
  
def _volatility_check(asset, secs_into):
    """
    Returns True → gap dropped ≥80% from peak → blacklist, skip buy.
    Returns False → gap healthy, allow buy.
    If no peak data yet → allow (safe fallback).
    """
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)
    if c_open <= 0 or c_live is None:
        return False   # no data → allow
    current_gap = abs(c_live - c_open)
    peak        = peak_gap.get(asset, 0.0)
    if peak <= 0:
        return False   # no peak recorded yet → allow
    if current_gap <= peak * 0.20:
        log.info(
            "[VOL-BLOCK] %s  current_gap=%.4f  peak_gap=%.4f  drop=%.1f%%"
            " — gap collapsed, blacklisting",
            asset.upper(), current_gap, peak,
            (1 - current_gap / peak) * 100,
        )
        return True   # blocked
    return False

def _extreme_gap_skip_triggered(secs_into):
    """
    Returns True if any asset has gap > (threshold * 15).
    Uses a simple gap magnitude volume (gap_mag_vol = 1.0) and does not
    use the staged GAP_MAGNITUDE values.
    """
    
    for asset in ASSETS:
        c_open = candle_open.get(asset, 0.0)
        c_live = live_close.get(asset)
        if c_open <= 0 or c_live is None:
            continue
        swing = GAP_SWING.get(asset, 0.001)
        threshold = c_open * swing * gap_mag_vol
        actual_gap = abs(c_live - c_open)
        if actual_gap > threshold * 15:
            log.warning("[EXTREME-GAP] %s gap=%.4f > threshold*8=%.4f",
                        asset.upper(), actual_gap, threshold * 15)
            return True
    return False


def advance_rebound_cutloss_tracker(client, window_start, secs_into=None):
    if secs_into is None:
        secs_into = max(0, int(time.time()) - int(window_start))
    for key in list(rebound_cutloss_tracker.keys()):
        tracker = rebound_cutloss_tracker[key]
        asset = key.split("_")[0]

        if tracker.get("window_start") != window_start:
            del rebound_cutloss_tracker[key]
            continue
        if key in open_positions:
            continue
        if asset in flipped_this_window:
            del rebound_cutloss_tracker[key]
            continue

        token = tracker["token"]
        current_price = get_position_live_price(key, None)
        if current_price is None or current_price <= 0:
            current_price = get_midpoint(client, token)
        if current_price <= 0:
            continue

        if secs_into > REBOUND_STOP_BUY_AT:
            log.info(
                "[REBOUND-STOP-BUY] %s secs_into=%d > rebound_stop_buy_at=%d — discarding",
                key, secs_into, REBOUND_STOP_BUY_AT,
            )
            del rebound_cutloss_tracker[key]
            continue

        trough = float(tracker.get("trough", current_price))

        if current_price <= REBOUND_CUTLOSS_DEAD_ZONE:
            log.info(
                "[REBOUND-DEAD] %s price=%.4f <= dead-zone %.4f — discarding rebound buy",
                key, current_price, REBOUND_CUTLOSS_DEAD_ZONE,
            )
            del rebound_cutloss_tracker[key]
            continue

        if current_price < trough:
            tracker["trough"] = current_price
            log.info("[REBOUND-FLIP] %s new trough=%.4f", key, current_price)
            continue

        rebound_ratio = current_price / trough if trough > 0 else 0.0
        if rebound_ratio < REBOUND_CUTLOSS_MULT:
            log.info(
                "[REBOUND-FLIP] %s waiting %.3fx/%.2fx from trough %.4f",
                key, rebound_ratio, REBOUND_CUTLOSS_MULT, trough,
            )
            continue

        if current_price >= REBOUND_CUTLOSS_CAP:
            log.info(
                "[REBOUND-CAP] %s rebound %.3fx reached at %.4f >= cap %.4f — discarding rebound buy",
                key, rebound_ratio, current_price, REBOUND_CUTLOSS_CAP,
            )
            del rebound_cutloss_tracker[key]
            continue

        spread = get_spread_value(client, token)
        if spread is not None and spread > MAX_BOOK_SPREAD:
            log.info(
                "[REBOUND-SPREAD-SKIP] %s spread=%.4f > %.4f",
                key, spread, MAX_BOOK_SPREAD,
            )
            continue

        label = f"{asset.upper()}-{key.split('_')[1].upper()}-REBOUND-FLIP"
        log.info(
            "[REBOUND-FLIP] %s buying after rebound %.3fx from trough %.4f @ %.4f",
            key, rebound_ratio, trough, current_price,
        )
        buy = market_buy(
            client, token, label,
            price_hint=current_price,
            amount=REBOUND_BUY_AMOUNT,
            simulate=SIMULATE_REBOUND_MODE_ENABLED,
        )
        if buy["ok"]:
            entry_px = float(buy.get("filled_price") or current_price)
            open_position(
                key, token, entry_px,
                filled_shares=buy.get("filled_shares"),
                window_start=window_start,
                is_flip=True,
                is_rebound=True,
                buy_amount=REBOUND_BUY_AMOUNT,
                is_simulated=bool((buy.get("resp") or {}).get("simulated")),
            )
            flipped_this_window.add(asset)
            traded_this_window.add(asset)
            live_prices[key] = current_price
            del rebound_cutloss_tracker[key]



def scan_markets(client, window_start, secs_into, server_ts, executor):
    global _skip_first_window, _startup_window_ts, skip_buy_until_window, skip_log_window

    stats["scans"] += 1

    if window_start not in token_cache:
        token_cache[window_start] = {}

    # Parallel fetch
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

    for asset in ASSETS:
        _update_prices(results.get(asset))

    if not can_open_new_trades(server_ts):
        return

    advance_rebound_cutloss_tracker(client, window_start, secs_into)

    if _extreme_gap_skip_triggered(secs_into):
        skip_buy_until_window = window_start + (WINDOW_SECS * 4)  # this + next 3 windows
        skip_log_window = None
        log.warning("[BUY-SKIP-WINDOWS] Extreme gap detected — skipping buys until window %d",
                    skip_buy_until_window)
        return

    if skip_buy_until_window is not None and window_start < skip_buy_until_window:
        windows_left = max(0, int((skip_buy_until_window - window_start) // WINDOW_SECS))
        if skip_log_window != window_start:
            log.info("[BUY-SKIP] buy-disabled for %d window(s) due to prior extreme gap", windows_left)
            skip_log_window = window_start
        return


    # ── Startup window skip ───────────────────────────────────────────────────
    if _skip_first_window:
        if _startup_window_ts is None:
            _startup_window_ts = window_start
            log.info("[SKIP-WINDOW] Mid-window start (secs_into=%d) — skipping window %d",
                     secs_into, window_start)
        if window_start == _startup_window_ts:
            return
        else:
            _skip_first_window = False
            log.info("[SKIP-WINDOW] Clean window started — gap guard active")

    # ── Advance gap_wait — check if gap has widened ───────────────────────────
    for asset in list(gap_wait.keys()):
        if asset in traded_this_window:
            del gap_wait[asset]
            continue

        gw      = gap_wait[asset]
        elapsed = time.time() - gw["triggered_at"]

        if check_gap_guard(asset, secs_into):
            # Gap is now large enough — volatility check first
            if _volatility_check(asset, secs_into):
                normal_blacklisted_assets.add(asset)
                del gap_wait[asset]
                continue
            # Proceed to buy
            log.info("[GAP-CLEARED] %s  gap widened after %.1fs — proceeding to buy",
                     asset.upper(), elapsed)
            key   = gw["key"]
            token = gw["token"]
            price = live_prices.get(key, gw["price"])
            if not (BUY_PRICE_MIN <= price <= BUY_PRICE_MAX):
                log.info(
                    "[GAP-CLEAR-SKIP] %s price=%.4f outside buy zone %.4f–%.4f after wait — requiring new trigger",
                    key, price, BUY_PRICE_MIN, BUY_PRICE_MAX,
                )
                del gap_wait[asset]
                continue
            side  = key.split("_")[1]
            label = f"{asset.upper()}-{key.split('_')[1].upper()}"

            # Spread check before buy
            spread = get_spread_value(client, token)
            if spread is not None and spread > MAX_BOOK_SPREAD:
                gw["spread_retries"] = gw.get("spread_retries", 0) + 1
                if gw["spread_retries"] < SPREAD_MAX_RETRIES:
                    log.info("[SPREAD-WAIT] %s  spread=%.4f  retry %d/%d",
                             asset.upper(), spread, gw["spread_retries"], SPREAD_MAX_RETRIES)
                    continue
                log.info("[SPREAD-SKIP] %s  spread still wide — blacklisting (normal-buy only)", asset.upper())
                normal_blacklisted_assets.add(asset)
                del gap_wait[asset]
                continue

            buy = market_buy(
                client, token, label, price_hint=price,
                simulate=SIMULATE_NORMAL_BUY_ONLY,
            )
            del gap_wait[asset]
            if buy["ok"]:
                entry_px = float(buy.get("filled_price") or price)
                open_position(key, token, entry_px,
                              filled_shares=buy.get("filled_shares"),
                              window_start=window_start,
                              is_simulated=bool((buy.get("resp") or {}).get("simulated")))
                traded_this_window.add(asset)
            continue

        wait_secs = GAP_WAIT_SECS[get_stage(secs_into)]
        if elapsed >= wait_secs:
            log.info("[GAP-DEFER] %s  gap still too small after %.1fs — not blacklisted, will re-check on next trigger",
                     asset.upper(), elapsed)
            del gap_wait[asset]
        # else: still waiting, no log spam

    # ── Evaluate fresh buy triggers ───────────────────────────────────────────
    if secs_into < ENTRY_AFTER or secs_into > STOP_BUY_AT:
        return

    if OPPO_MODE_ENABLED and secs_into >= OPPO_WINDOW_START_SEC and window_start not in oppo_bought_windows:
        side_values = {"yes": {}, "no": {}}
        for asset in ASSETS:
            result = results.get(asset)
            if result is None:
                continue
            _, yes_price, yes_token, no_token = result
            side_values["yes"][asset] = (yes_price, yes_token)
            side_values["no"][asset] = (round(1.0 - yes_price, 4), no_token)

        for side in ("yes", "no"):
            low_assets = []
            for asset, (price, token) in side_values[side].items():
                if OPPO_MIN_PRICE <= price <= OPPO_MAX_PRICE:
                    low_assets.append((asset, price, token))
            if not low_assets:
                continue

            for opp_asset, opp_price, opp_token in low_assets:
                opp_key = f"{opp_asset}_{side}"

                if opp_price <= OPPO_DEAD_ZONE:
                    oppo_rebound_tracker.pop(opp_key, None)
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "DEAD-ZONE", f"<= {OPPO_DEAD_ZONE:.4f}")
                    log.info("[OPPO-DISCARD] %s price=%.4f <= dead-zone %.4f",
                             opp_key, opp_price, OPPO_DEAD_ZONE)
                    _record_oppo_trigger(opp_asset, side, opp_price, "SKIPPED", "dead-zone")
                    continue

                trough = oppo_rebound_tracker.get(opp_key)
                if trough is None:
                    oppo_rebound_tracker[opp_key] = opp_price
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "WAIT", f"start trough {opp_price:.4f}")
                    log.info("[OPPO-WAIT] %s start trough=%.4f wait %.2fx rebound",
                             opp_key, opp_price, OPPO_REBOUND_MULT)
                    _record_oppo_trigger(opp_asset, side, opp_price, "TRACKING", "trough-start")
                    continue
                if opp_price < trough:
                    oppo_rebound_tracker[opp_key] = opp_price
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "WAIT", f"new trough {opp_price:.4f}")
                    log.info("[OPPO-WAIT] %s new trough=%.4f", opp_key, opp_price)
                    _record_oppo_trigger(opp_asset, side, opp_price, "TRACKING", "trough-lower")
                    continue
                rebound_ratio = opp_price / trough if trough > 0 else 0.0
                if rebound_ratio < OPPO_REBOUND_MULT:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "WAIT", f"rebound {rebound_ratio:.3f}x/{OPPO_REBOUND_MULT:.2f}x")
                    log.info("[OPPO-WAIT] %s waiting %.3fx/%.2fx",
                             opp_key, rebound_ratio, OPPO_REBOUND_MULT)
                    _record_oppo_trigger(opp_asset, side, opp_price, "TRACKING", f"rebound {rebound_ratio:.2f}x")
                    continue
                if f"{opp_asset}_{side}_oppo" in open_positions:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "SKIP", "already open")
                    continue
                if server_ts - last_entry_ts.get(opp_asset, 0) < COOLDOWN_SEC:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "COOLDOWN", f"{COOLDOWN_SEC}s")
                    log.info("[OPPO-COOLDOWN] %s_%s cooling down (%ds)",
                             opp_asset.upper(), side.upper(), COOLDOWN_SEC)
                    continue

                spread = get_spread_value(client, opp_token)
                if spread is not None and spread > MAX_BOOK_SPREAD:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "SPREAD", f"{spread:.4f}>{MAX_BOOK_SPREAD:.4f}")
                    log.info("[OPPO-DISCARD] %s_%s spread=%.4f > %.4f",
                             opp_asset.upper(), side.upper(), spread, MAX_BOOK_SPREAD)
                    _record_oppo_trigger(opp_asset, side, opp_price, "SKIPPED", "spread-too-wide")
                    continue

                c_open = candle_open.get(opp_asset, 0.0)
                c_live = live_close.get(opp_asset)
                if c_open > 0 and c_live is not None:
                    actual_gap = abs(c_live - c_open)
                    oppo_gap_threshold = c_open * GAP_SWING.get(opp_asset, 0.001) * OPPO_GAP_MAG
                    if actual_gap >= oppo_gap_threshold:
                        record_oppo_trigger(opp_key, opp_asset, side, opp_price, "GAP-BLOCK", f"{actual_gap:.4f}>={oppo_gap_threshold:.4f}")
                        log.info("[OPPO-DISCARD] %s_%s actual_gap=%.4f >= oppo_threshold=%.4f (need <)",
                                 opp_asset.upper(), side.upper(), actual_gap, oppo_gap_threshold)
                        _record_oppo_trigger(opp_asset, side, opp_price, "SKIPPED", "gap-too-large")
                        continue

                label = f"{opp_asset.upper()}-{side.upper()}-OPPO"
                buy = market_buy(client, opp_token, label, price_hint=opp_price)
                if buy["ok"]:
                    entry_px = float(buy.get("filled_price") or opp_price)
                    open_position(f"{opp_asset}_{side}_oppo", opp_token, entry_px,
                                  filled_shares=buy.get("filled_shares"),
                                  window_start=window_start,
                                  is_simulated=bool((buy.get("resp") or {}).get("simulated")))
                    oppo_bought_windows.add(window_start)
                    oppo_rebound_tracker.pop(opp_key, None)
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "BOUGHT", "success")
                    _record_oppo_trigger(opp_asset, side, opp_price, "BOUGHT", "entry-filled")
                    log.info("[OPPO-BUY] %s_%s triggered oppo setup", opp_asset.upper(), side.upper())
                else:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "BUY-FAIL", "order rejected")
                break

    for asset in ASSETS:
        if asset in traded_this_window:
            continue
        if asset in normal_blacklisted_assets:
            continue
        if asset in gap_wait:
            continue

        result = results.get(asset)
        if result is None:
            continue

        _, yes_price, yes_token, no_token = result
        no_price = round(1.0 - yes_price, 4)

        # Determine which side triggered
        triggered_key   = None
        triggered_token = None
        triggered_price = None

        if BUY_PRICE_MIN <= yes_price <= BUY_PRICE_MAX and f"{asset}_yes" not in open_positions:
            triggered_key   = f"{asset}_yes"
            triggered_token = yes_token
            triggered_price = yes_price
        elif BUY_PRICE_MIN <= no_price <= BUY_PRICE_MAX and f"{asset}_no" not in open_positions:
            triggered_key   = f"{asset}_no"
            triggered_token = no_token
            triggered_price = no_price

        if triggered_key is None:
            continue
        triggered_side = triggered_key.split("_")[1]
        if not _trend_guard_ok(asset, triggered_side, results):
            trend_guarded_assets.add(asset)
            continue

        stats["triggers"] += 1
        log.info("[TRIGGER] %s  price=%.4f  checking volatility + gap guard", triggered_key, triggered_price)

        # ── Volatility check — gap collapsed from peak → blacklist ────────────
        if _volatility_check(asset, secs_into):
            normal_blacklisted_assets.add(asset)
            continue
          

        if check_gap_guard(asset, secs_into):
            # Gap large enough — check spread then buy immediately
            spread = get_spread_value(client, triggered_token)
            if spread is not None and spread > MAX_BOOK_SPREAD:
                log.info("[SPREAD-SKIP] %s  spread=%.4f > max=%.4f — skipping",
                         triggered_key, spread, MAX_BOOK_SPREAD)
                continue
            label = f"{asset.upper()}-{triggered_key.split('_')[1].upper()}"
            buy = market_buy(
                client, triggered_token, label, price_hint=triggered_price,
                simulate=SIMULATE_NORMAL_BUY_ONLY,
            )
            if buy["ok"]:
                entry_px = float(buy.get("filled_price") or triggered_price)
                open_position(triggered_key, triggered_token, entry_px,
                              filled_shares=buy.get("filled_shares"),
                              window_start=window_start,
                              is_simulated=bool((buy.get("resp") or {}).get("simulated")))
                traded_this_window.add(asset)
        else:
            # Gap too small — wait up to GAP_WAIT_SECS for it to widen
            gap_wait[asset] = {
                "triggered_at":  time.time(),
                "key":           triggered_key,
                "token":         triggered_token,
                "price":         triggered_price,
                "spread_retries": 0,
            }
            wait_secs = GAP_WAIT_SECS[get_stage(secs_into)]
            log.info("[GAP-WAIT] %s  gap too small — waiting %.0fs for momentum  price=%.4f",
                     asset.upper(), wait_secs, triggered_price)

# ── Status ────────────────────────────────────────────────────────────────────

def print_status(secs_left=None):
    total = stats["wins"] + stats["losses"]
    wr = f"{round(stats['wins']/total*100)}% win" if total > 0 else "no closed trades"
    win_str = f"  window={secs_left}s left" if secs_left is not None else ""
    log.info(
        "[STATUS] scans=%d  triggers=%d  buys=%d  wins=%d  losses=%d  PnL=$%.4f  (%s)  open=%s%s",
        stats["scans"], stats["triggers"], stats["buys"],
        stats["wins"], stats["losses"], stats["pnl"], wr,
        list(open_positions.keys()) or "none", win_str,
    )

# ── Dashboard ─────────────────────────────────────────────────────────────────

def _build_state_snapshot():
    positions_out = {}
    for k, p in open_positions.items():
        entry   = p["entry_price"]
        curr    = get_position_live_price(k, entry)
        target  = p["sell_price"]
        cut     = p["cut_loss_price"]
        shares  = p["net_shares"]
        pnl_unreal = round(p.get("realized_revenue", 0.0) + (curr * shares) - p.get("cost", 0.0), 4)
        pct = round((curr - entry) / (target - entry) * 100, 1) if target != entry else 0
        positions_out[k] = {
            "entry":     round(entry, 4),
            "current":   round(curr, 4),
            "target":    round(target, 4),
            "cut_loss":  round(cut, 4),
            "is_flip":   p.get("is_flip", False),
            "is_rebound": p.get("is_rebound", False),
            "rebound_tranches": p.get("rebound_tranches", []),
            "pnl":       pnl_unreal,
            "pct":       max(0, min(100, pct)),
            "opened_at": p.get("opened_at", "—"),
        }
    now_ts  = int(time.time())
    slot_ts = (now_ts // 900) * 900
    secs_in = now_ts - slot_ts
    gap_out = {}
    gap_threshold_out = {}
    for a in ASSETS:
        c_open = candle_open.get(a, 0.0)
        c_live = live_close.get(a)
        if c_open > 0:
            swing = GAP_SWING.get(a, 0.001)
            gap_threshold_out[a] = {
                "early": round(c_open * swing * GAP_MAGNITUDE["early"], 4),
                "mid":   round(c_open * swing * GAP_MAGNITUDE["mid"], 4),
                "late":  round(c_open * swing * GAP_MAGNITUDE["late"], 4),
            }
        else:
            gap_threshold_out[a] = None
        gap_out[a] = round(abs(c_live - c_open), 4) if c_open > 0 and c_live is not None else None
    return {
        "updated":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dry_run":       DRY_RUN,
        "stats":         dict(stats),
        "positions":     positions_out,
        "prices":        dict(live_prices),
        "gap":           gap_out,
        "gap_threshold": gap_threshold_out,
        "window": {
            "secs_into": secs_in,
            "secs_left": 900 - secs_in,
            "period":    "early" if secs_in < 300 else ("mid" if secs_in < 600 else "late"),
        },
        "normal_blacklisted_assets": sorted(list(normal_blacklisted_assets)),
        "trend_guarded_assets": sorted(list(trend_guarded_assets)),
        "rebound_cutloss_tracker": {
            k: {
                "trough": round(float(v.get("trough", 0.0)), 4),
                "window_start": v.get("window_start"),
            } for k, v in rebound_cutloss_tracker.items()
        },
        "oppo_last_trigger": {
            k: {
                "asset": v.get("asset"),
                "side": v.get("side"),
                "price": round(float(v.get("price", 0.0)), 4) if v.get("price") is not None else None,
                "status": v.get("status", ""),
                "detail": v.get("detail", ""),
                "updated": v.get("updated", ""),
            } for k, v in oppo_last_trigger.items()
        },
        "asset_status": {
            a: {
                "blacklisted": a in normal_blacklisted_assets,
                "trend_guarded": a in trend_guarded_assets,
            } for a in ASSETS
        },
        "pnl_history":   list(pnl_history),
        "asset_history": dict(asset_history),
        "trade_log":     list(trade_log),
        "oppo_trigger_log": list(oppo_trigger_log),
        "settings": {
            "assets":     ASSETS,
            "buy_min":    BUY_PRICE_MIN,
            "buy_max":    BUY_PRICE_MAX,
            "sell_multiplier": SELL_MULTIPLIER,
            "sell_cap":   SELL_CAP,
            "cut_loss":   CUT_LOSS_PCT,
            "oppo_cut_loss": OPPO_CUT_LOSS_PCT,
            "flip_min":   FLIP_MIN,
            "flip_max":   FLIP_MAX,
            "force_sell_gap_mult": FORCE_SELL_GAP_MULT,
            "rebound_cutloss_mult": REBOUND_CUTLOSS_MULT,
            "rebound_cutloss_dead_zone": REBOUND_CUTLOSS_DEAD_ZONE,
            "rebound_cutloss_cap": REBOUND_CUTLOSS_CAP,
            "rebound_stop_buy_at": REBOUND_STOP_BUY_AT,
            "rebound_sell_multiplier": REBOUND_SELL_MULTIPLIER,
            "rebound_max_target_price": REBOUND_MAX_TARGET_PRICE,
            "simulate_rebound_mode_enabled": SIMULATE_REBOUND_MODE_ENABLED,
            "order":      BUY_AMOUNT,
            "poll":       POLL_SECS,
            "entry_after": ENTRY_AFTER,
            "stop_buy":   STOP_BUY_AT,
        },
    }


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<ttitleMomentumBot</title>
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
.chart-wrap{height:180px;margin-top:4px}
.oppo-log-wrap{max-height:320px;overflow-y:auto;overflow-x:hidden;border:1px solid #252d3d;border-radius:6px;padding:0 8px 0 0}
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
<script>
let oppoResetConfirmOpen=false;
let oppoLogScrollTop=0;
function fmt(v,d=4){return v!=null?'$'+parseFloat(v).toFixed(d):'—'}
function fmtPct(v){return v!=null?(parseFloat(v)*100).toFixed(0)+'%':'—'}
function fmtPnl(v){
  const n=parseFloat(v)||0;
  return `<span class="${n>0?'green':n<0?'red':'dim'}">${n>=0?'+':''}$${Math.abs(n).toFixed(4)}</span>`;
}
function pnlColor(v){return v>0?'green':v<0?'red':'dim'}

function drawChart(history,wrap){
  if(!history||history.length<2){
    wrap.innerHTML='<p class="dim" style="padding:12px 0;font-size:12px">Not enough data yet</p>';
    return;
  }
  let canvas=wrap.querySelector('canvas');
  if(!canvas){canvas=document.createElement('canvas');wrap.appendChild(canvas);}
  const W=wrap.offsetWidth||600,H=180;
  canvas.width=W;canvas.height=H;
  const ctx=canvas.getContext('2d');
  ctx.clearRect(0,0,W,H);
  const vals=history.map(p=>p.pnl),labels=history.map(p=>p.ts);
  const minV=Math.min(...vals,0),maxV=Math.max(...vals,0),range=maxV-minV||1;
  const padT=20,padB=40,padL=60,padR=16;
  const cW=W-padL-padR,cH=H-padT-padB;
  const xOf=i=>padL+i*(cW/(history.length-1));
  const yOf=v=>padT+cH-(((v-minV)/range)*cH);
  ctx.strokeStyle='#2a3347';ctx.lineWidth=1;
  [0,.25,.5,.75,1].forEach(t=>{
    const y=padT+cH*t;
    ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(W-padR,y);ctx.stroke();
    const lbl=((minV+(maxV-minV)*(1-t))||0).toFixed(3);
    ctx.fillStyle='#5a6a85';ctx.font='10px system-ui';ctx.textAlign='right';
    ctx.fillText((parseFloat(lbl)>=0?'+':'')+lbl,padL-6,y+4);
  });
  if(minV<0&&maxV>0){
    const yz=yOf(0);ctx.strokeStyle='#3a4560';ctx.lineWidth=1;
    ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(padL,yz);ctx.lineTo(W-padR,yz);ctx.stroke();ctx.setLineDash([]);
  }
  const latestPnl=vals[vals.length-1]||0;
  const grad=ctx.createLinearGradient(0,padT,0,padT+cH);
  if(latestPnl>=0){grad.addColorStop(0,'rgba(29,184,122,.35)');grad.addColorStop(1,'rgba(29,184,122,.03)');}
  else{grad.addColorStop(0,'rgba(226,75,74,.03)');grad.addColorStop(1,'rgba(226,75,74,.35)');}
  ctx.beginPath();ctx.moveTo(xOf(0),yOf(vals[0]));
  vals.forEach((v,i)=>{if(i>0)ctx.lineTo(xOf(i),yOf(v));});
  ctx.lineTo(xOf(vals.length-1),padT+cH);ctx.lineTo(xOf(0),padT+cH);ctx.closePath();
  ctx.fillStyle=grad;ctx.fill();
  ctx.beginPath();ctx.strokeStyle=latestPnl>=0?'#1db87a':'#e24b4a';ctx.lineWidth=2;
  vals.forEach((v,i)=>{i===0?ctx.moveTo(xOf(i),yOf(v)):ctx.lineTo(xOf(i),yOf(v));});ctx.stroke();
  vals.forEach((v,i)=>{ctx.beginPath();ctx.arc(xOf(i),yOf(v),3,0,Math.PI*2);ctx.fillStyle=latestPnl>=0?'#1db87a':'#e24b4a';ctx.fill();});
  ctx.fillStyle='#5a6a85';ctx.font='10px system-ui';ctx.textAlign='center';
  const step=Math.max(1,Math.floor(history.length/8));
  for(let i=0;i<history.length;i+=step)ctx.fillText(labels[i],xOf(i),H-padB+16);
  const last=history.length-1;if(last%step!==0)ctx.fillText(labels[last],xOf(last),H-padB+16);
}

let _tlExpanded=false;
const TL_COLLAPSE=5;
function tlToggle(){
  _tlExpanded=!_tlExpanded;
  document.querySelectorAll('.tl-row').forEach((r,i)=>{if(i>=TL_COLLAPSE)r.style.display=_tlExpanded?'':'none';});
  const btn=document.getElementById('tlToggle');
  if(btn){const h=Array.from(document.querySelectorAll('.tl-row')).filter(r=>r.style.display==='none').length;
    btn.textContent=_tlExpanded?'▲ Show less':'▼ Show '+h+' more';}
}

function renderTradeLog(log){
  if(!log||!log.length)return'<p class="dim" style="padding:8px 0;font-size:12px">No closed trades yet</p>';
  const exitBadge=e=>{
    const col={SELL:'#0d2a1e','FLIP-SELL':'#0d1a2a','CUT-LOSS':'#2a0d0d'}[e]||'#2a0d0d';
    const tc={'SELL':'#4ade9f','FLIP-SELL':'#60a5fa','CUT-LOSS':'#f87171'}[e]||'#f87171';
    const bc={'SELL':'#1a5c3a','FLIP-SELL':'#1a3a5c','CUT-LOSS':'#5c1d1d'}[e]||'#5c1d1d';
    return `<span class="badge" style="background:${col};color:${tc};border:1px solid ${bc}">${e}</span>`;
  };
  const rows=log.map((t,i)=>{
    const p=t.pnl||0,ps=(p>=0?'+':'')+p.toFixed(4);
    const flipTag=t.is_flip?'<span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c;font-size:10px;margin-left:4px">FLIP</span>':'';
    return `<tr class="tl-row" style="${i>=TL_COLLAPSE&&!_tlExpanded?'display:none':''}">
      <td>${t.time||'—'}</td>
      <td><strong>${t.asset}-${t.side}</strong>${flipTag}</td>
      <td>${fmt(t.entry, 2)}</td>
      <td>${fmt(t.target, 2)}</td>
      <td>${exitBadge(t.exit, 2)}</td>
      <td>${fmt(t.exit_px, 2)}</td>
      <td class="${p>0?'green':p<0?'red':'dim'}" style="font-weight:600">$${ps}</td>
    </tr>`;
  }).join('');
  const extra=log.length-TL_COLLAPSE;
  const btn=extra>0?`<button id="tlToggle" onclick="tlToggle()" style="margin-top:10px;background:#1e2533;border:1px solid #2a3347;color:#60a5fa;border-radius:6px;padding:5px 14px;font-size:12px;cursor:pointer">${_tlExpanded?'▲ Show less':'▼ Show '+extra+' more'}</button>`:'';
  return `<div style="overflow-x:auto"><table>
    <thead><tr><th>Time</th><th>Asset</th><th>Entry</th><th>Target</th><th>Exit</th><th>Exit $</th><th>PnL</th></tr></thead>
    <tbody>${rows}</tbody></table></div>${btn}`;
}

function renderAssetHistory(assetHist,assets){
  if(!assetHist||!Object.keys(assetHist).length)return'<p class="dim" style="padding:8px 0;font-size:12px">No closed trades yet</p>';
  return '<div class="asset-grid">'+assets.map(a=>{
    const h=assetHist[a];
    if(!h)return`<div class="asset-card"><div class="name">${a.toUpperCase()}</div><div class="dim" style="font-size:12px">No trades</div></div>`;
    const total=h.trades||0,wr=total>0?Math.round((h.wins||0)/total*100)+'%':'—',p=h.pnl||0;
    return`<div class="asset-card">
      <div class="name">${a.toUpperCase()}</div>
      <div class="asset-row"><span class="k">Closed</span><span>${total}</span></div>
      <div class="asset-row"><span class="k">Wins</span><span class="green">${h.wins||0}</span></div>
      <div class="asset-row"><span class="k">Losses</span><span class="${(h.losses||0)>0?'red':'dim'}">${h.losses||0}</span></div>
      <div class="asset-row"><span class="k">Win rate</span><span class="${total>0?'green':'dim'}">${wr}</span></div>
      <div class="asset-row"><span class="k">Net PnL</span><span>${fmtPnl(p)}</span></div>
    </div>`;
  }).join('')+'</div>';
}

function render(s){
  const prevOppoLogWrap=document.getElementById('oppoLogWrap');
  if(prevOppoLogWrap) oppoLogScrollTop=prevOppoLogWrap.scrollTop;
  const st=s.stats||{},pos=s.positions||{},pr=s.prices||{};
  const cfg=s.settings||{},w=s.window||{},gap=s.gap||{},gapThreshold=s.gap_threshold||{};
  const assetStatus=s.asset_status||{};
  const oppoLastTrigger=s.oppo_last_trigger||{};
  const normalBlacklisted=new Set(s.normal_blacklisted_assets||[]);
  const trendGuarded=new Set(s.trend_guarded_assets||[]);
  const pnlHist=s.pnl_history||[],assetHist=s.asset_history||{},tLog=s.trade_log||[];
  const oppoLog=(s.oppo_trigger_log||[]).filter(o=>['BOUGHT','SELL','SOLD','CUT-LOSS'].includes(o.status));
  const assets=cfg.assets||['btc','eth','sol','xrp'];
  const mode=s.dry_run?'<span class="badge dry">DRY RUN</span>':'<span class="badge live">LIVE</span>';
  const period=w.period||'early';
  const mm=v=>String(Math.floor((v||0)/60)).padStart(2,'0');
  const ss=v=>String((v||0)%60).padStart(2,'0');
  const wStr=`${mm(w.secs_into)}:${ss(w.secs_into)} in &nbsp;|&nbsp; ${mm(w.secs_left)}:${ss(w.secs_left)} left`;
  const total=(st.wins||0)+(st.losses||0);
  const wr=total>0?Math.round(st.wins/total*100)+'%':'—';
  const pnl=st.pnl||0;

  // Buy zone indicator per asset
  const priceRows=assets.map(a=>{
    const yp=pr[a+'_yes'],np=pr[a+'_no'];
    const inZone=p=>p!=null&&p>=cfg.buy_min&&p<=cfg.buy_max;
    const yc=inZone(yp)?'green':'';
    const nc=inZone(np)?'green':'';
    const holding=[(a+'_yes' in pos)?'<span class="green">YES</span>':'',(a+'_no' in pos)?'<span class="green">NO</span>':'',(a+'_yes_oppo' in pos)?'<span class="amber">YES OPPO</span>':'',(a+'_no_oppo' in pos)?'<span class="amber">NO OPPO</span>':''].filter(Boolean).join(' ');
    const stAsset=assetStatus[a]||{};
    const isBlacklisted=stAsset.blacklisted===true || normalBlacklisted.has(a);
    const isTrendGuarded=stAsset.trend_guarded===true || trendGuarded.has(a);
    const flags=[isBlacklisted?'<span class="red">BLACKLISTED</span>':'',isTrendGuarded?'<span style="color:#f59e0b">TREND GUARDED</span>':''].filter(Boolean).join(' ');
    const holdingCell=[holding,flags].filter(Boolean).join(' <span class="dim">|</span> ');
    const gv=gap[a],gt=gapThreshold[a]&&w.period?gapThreshold[a][w.period]:null;
    const oppYes=oppoLastTrigger[a+'_yes'];
    const oppNo=oppoLastTrigger[a+'_no'];
    const oppoParts=[oppYes,oppNo].filter(Boolean).map(o=>`${(o.side||'').toUpperCase()}: ${o.status||'—'}`).join(' <span class="dim">|</span> ');
    const oppoCell=oppoParts||'<span class="dim">—</span>';
    const gStr=gv!=null?gv.toFixed(4):'—';
    const tStr=gt!=null?gt.toFixed(4):'—';
    return`<tr><td>${a.toUpperCase()}</td><td class="${yc}" style="padding-right:3px">${fmt(yp,2)}</td><td class="${nc}" style="padding-left:3px;padding-right:18px">${fmt(np,2)}</td><td style="font-family:monospace;padding-left:18px">${gStr} / ${tStr}</td><td>${holdingCell||'<span class="dim">—</span>'}</td><td>${oppoCell}</td></tr>`;
  }).join('');

  const posCards=Object.entries(pos).map(([k,p])=>{
    const [asset,side]=k.split('_');
    const col=p.current>=p.entry?'green':'red';
    const badges=[p.is_flip?'<span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c;margin-left:6px">FLIP</span>':'',p.is_rebound?'<span class="badge" style="background:#082a1b;color:#34d399;border:1px solid #065f46;margin-left:6px">REBOUND</span>':'',p.is_oppo?'<span class="badge" style="background:#2a1e08;color:#fbbf24;border:1px solid #5c3d08;margin-left:6px">OPPO</span>':''].join('');
    const pnlV=p.pnl||0;
    return`<div class="pos-card">
      <div class="pos-hdr"><strong>${asset.toUpperCase()}-${side.toUpperCase()}</strong>${badges}<span style="font-size:12px;color:#5a6a85">opened ${p.opened_at||'—'}</span></div>
      <div style="display:flex;gap:16px;font-size:13px;flex-wrap:wrap">
        <span><span class="dim">entry</span> <strong>${fmt(p.entry)}</strong></span>
        <span><span class="dim">current</span> <strong class="${col}">${fmt(p.current)}</strong></span>
        <span><span class="dim">sell @</span> <strong class="green">${fmt(p.target,2)}</strong></span>
        <span><span class="dim">cut @</span> <strong class="red">${fmt(p.cut_loss)}</strong> <span class="dim">(${((p.cut_loss_pct||cfg.cut_loss||0)*100).toFixed(0)}%)</span></span>
      </div>
      <div class="bar-bg"><div class="bar-fill" style="width:${p.pct||0}%;background:${p.current>=p.entry?'#1db87a':'#e24b4a'}"></div></div>
      <div class="pos-meta">
        <span>${(p.pct||0).toFixed(0)}% to target</span>
        <span>Unrealised: ${fmtPnl(pnlV)}</span>
      </div>
    </div>`;
  }).join('')||'<p class="dim" style="padding:8px 0">No open positions</p>';

  const oppoRows=oppoLog.map(o=>{
    const statusCls=o.status==='BOUGHT'?'green':'amber';
    const priceTxt=o.price!=null?fmt(o.price,2):'—';
    return `<tr>
      <td>${o.time||'—'}</td>
      <td><strong>${o.asset||'—'}-${o.side||'—'}</strong></td>
      <td>${priceTxt}</td>
      <td class="${statusCls}" style="font-weight:600">${o.status||'—'}</td>
      <td>${o.reason||'—'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="5" class="dim">No OPPO triggers yet</td></tr>';

  document.getElementById('root').innerHTML=`
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:8px">
      <div><strong style="font-size:18px">Momentum<span class="green">Bot_v3</span></strong> &nbsp; ${mode}</div>
      <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
        <div style="font-size:12px;color:#5a6a85">${s.updated||''} &nbsp; <span class="badge ${period}">${period.toUpperCase()}</span> &nbsp; ${wStr}</div>
        <div style="display:flex;align-items:center;gap:6px">
          <button onclick="startReset()" style="padding:4px 12px;background:transparent;border:1px solid #5c1d1d;color:#f87171;font-size:11px;border-radius:4px;cursor:pointer;font-family:monospace">Reset Stats</button>
          <span id="resetConfirm" style="display:none;align-items:center;gap:6px">
            <span style="font-size:11px;color:#f87171">Clear all history?</span>
            <button onclick="doReset()" style="padding:3px 10px;background:rgba(248,113,113,.15);border:1px solid #f87171;color:#f87171;font-size:11px;border-radius:4px;cursor:pointer">Confirm</button>
            <button onclick="cancelReset()" style="padding:3px 10px;background:#161b27;border:1px solid #2a3347;color:#5a6a85;font-size:11px;border-radius:4px;cursor:pointer">Cancel</button>
          </span>
          <span id="resetOk" style="display:none;font-size:11px;color:#4ade9f">&#10003; Cleared</span>
          <button onclick="startOppoReset()" style="padding:4px 12px;background:transparent;border:1px solid #5c3d08;color:#fbbf24;font-size:11px;border-radius:4px;cursor:pointer;font-family:monospace">Reset OPPO Log</button>
          <span id="oppoResetConfirm" style="display:none;align-items:center;gap:6px">
            <span style="font-size:11px;color:#fbbf24">Clear OPPO trigger log only?</span>
            <button onclick="doOppoReset()" style="padding:3px 10px;background:rgba(251,191,36,.15);border:1px solid #fbbf24;color:#fbbf24;font-size:11px;border-radius:4px;cursor:pointer">Confirm</button>
            <button onclick="cancelOppoReset()" style="padding:3px 10px;background:#161b27;border:1px solid #2a3347;color:#5a6a85;font-size:11px;border-radius:4px;cursor:pointer">Cancel</button>
          </span>
          <span id="oppoResetOk" style="display:none;font-size:11px;color:#fbbf24">&#10003; OPPO cleared</span>
        </div>
      </div>
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
      <h2>Net PnL Over Time <span style="font-size:11px;color:#5a6a85;font-weight:400">(every 30 min)</span></h2>
      <div class="chart-wrap" id="chartWrap"></div>
    </div>

    <div class="section">
      <h2>Live Prices <span style="font-size:11px;color:#5a6a85;font-weight:400">buy zone ${(cfg.buy_min||0.82)*100|0}–${(cfg.buy_max||0.86)*100|0}¢</span></h2>
      <table><thead><tr><th>Asset</th><th>YES</th><th>NO</th><th>Binance Gap / Threshold</th><th>Holding</th><th>OPPO Trigger</th></tr></thead>
      <tbody>${priceRows}</tbody></table>
    </div>

    <div class="section"><h2>Open Positions (${Object.keys(pos).length})</h2>${posCards}</div>

    <div class="section">
      <h2>OPPO Trigger Log <span style="font-size:11px;color:#5a6a85;font-weight:400">(shows OPPO buy/sell/cutloss events)</span></h2>
      <div class="oppo-log-wrap" id="oppoLogWrap"><table><thead><tr><th>Time</th><th>Asset</th><th>Price</th><th>Status</th><th>Reason</th></tr></thead>
      <tbody>${oppoRows}</tbody></table></div>
    </div>

    <div class="section">
      <h2>Trade Log <span style="font-size:11px;color:#5a6a85;font-weight:400">(${tLog.length} closed)</span></h2>
      ${renderTradeLog(tLog)}
    </div>

    <div class="section">
      <h2>Per-Asset Summary</h2>
      ${renderAssetHistory(assetHist,assets)}
    </div>

    <div class="section">
      <h2>Settings</h2>
      <table><tbody>
        <tr><td>Cut loss</td><td>Normal ${((cfg.cut_loss||0.6)*100).toFixed(0)}% / OPPO ${((cfg.oppo_cut_loss||0.7)*100).toFixed(0)}% of entry</td><td>Order size</td><td>$${cfg.order||2}</td></tr>
        <tr><td>Flip range</td><td>${(cfg.flip_min||0.5)*100|0}–${(cfg.flip_max||0.75)*100|0}¢</td><td>Poll</td><td>${cfg.poll||2}s</td></tr>
        <tr><td>Entry window</td><td>${(cfg.entry_after||600)/60|0}–${(cfg.stop_buy||780)/60|0} min</td><td></td><td></td></tr>
        <tr><td>Buy zone</td><td>${(cfg.buy_min||0)*100|0}–${(cfg.buy_max||0)*100|0}¢</td><td>Sell target</td><td>${cfg.sell_multiplier ? ('x'+Number(cfg.sell_multiplier).toFixed(2)+' (cap '+((cfg.sell_cap||0.99)*100|0)+'¢)') : (((cfg.sell||0.99)*100|0)+'¢')}</td></tr>
      </tbody></table>
    </div>

    <footer>Auto-refreshes every 2s &nbsp;&mdash;&nbsp; MomentumBot</footer>`;

  const wrap=document.getElementById('chartWrap');
  if(wrap)drawChart(pnlHist,wrap);
  const oppoLogWrap=document.getElementById('oppoLogWrap');
  if(oppoLogWrap){
    oppoLogWrap.scrollTop=Math.min(oppoLogScrollTop, Math.max(0, oppoLogWrap.scrollHeight-oppoLogWrap.clientHeight));
  }
}

function startReset(){document.getElementById('resetConfirm').style.display='inline-flex';}
function cancelReset(){document.getElementById('resetConfirm').style.display='none';}
async function doReset(){
  document.getElementById('resetConfirm').style.display='none';
  try{
    await fetch('/reset',{method:'POST'});
    const ok=document.getElementById('resetOk');ok.style.display='inline';
    setTimeout(()=>{ok.style.display='none'},3000);
  }catch(e){console.error('reset failed',e)}
}
function startOppoReset(){
  oppoResetConfirmOpen=true;
  const el=document.getElementById('oppoResetConfirm');
  if(el)el.style.display='inline-flex';
}
function cancelOppoReset(){
  oppoResetConfirmOpen=false;
  const el=document.getElementById('oppoResetConfirm');
  if(el)el.style.display='none';
}
async function doOppoReset(){
  oppoResetConfirmOpen=false;
  document.getElementById('oppoResetConfirm').style.display='none';
  try{
    await fetch('/reset-oppo',{method:'POST'});
    const ok=document.getElementById('oppoResetOk');ok.style.display='inline';
    setTimeout(()=>{ok.style.display='none'},3000);
  }catch(e){console.error('oppo reset failed',e)}
}
async function poll(){
  try{const r=await fetch('/state');const d=await r.json();render(d);}
  catch(e){console.error('fetch error',e);}
  const el=document.getElementById('oppoResetConfirm');
  if(el)el.style.display=oppoResetConfirmOpen?'inline-flex':'none';
}
poll();setInterval(poll,2000);
</script></body></html>"""


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/state":
            data = json.dumps(_build_state_snapshot(), indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
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

    def do_POST(self):
        if self.path == "/reset":
            reset_state()
            resp = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(resp))
            self.end_headers()
            self.wfile.write(resp)
            log.info("[HTTP] Dashboard reset by user")
        elif self.path == "/reset-oppo":
            reset_oppo_log()
            resp = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(resp))
            self.end_headers()
            self.wfile.write(resp)
            log.info("[HTTP] Dashboard OPPO log reset by user")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


def start_http_server():
    server = HTTPServer(("0.0.0.0", HTTP_PORT), _Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("[HTTP] Dashboard on port %d", HTTP_PORT)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global last_pnl_snapshot, pnl_history, armed_logged, skip_buy_until_window

    validate_settings()
    load_state()
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    log.info("=" * 55)
    log.info("  Momentum v3  [%s]", mode)
    log.info("  Assets : %s", ", ".join(a.upper() for a in ASSETS))
    log.info("  Buy zone: %.0f–%.0f¢  entry %d–%ds  sell=x%.2f (cap %.0f¢)  cut-loss=%.0f%%  oppo-cut-loss=%.0f%%",
             BUY_PRICE_MIN*100, BUY_PRICE_MAX*100,
             ENTRY_AFTER, STOP_BUY_AT,
             SELL_MULTIPLIER, SELL_CAP*100, CUT_LOSS_PCT*100, OPPO_CUT_LOSS_PCT*100)
    log.info("  Flip: %.0f–%.0f¢  order=$%.0f  poll=%.1fs",
             FLIP_MIN*100, FLIP_MAX*100, BUY_AMOUNT, POLL_SECS)
    log.info("  Force sell: pnl>0 and Binance gap >= %.2fx staged threshold", FORCE_SELL_GAP_MULT)
    log.info(
        "  Rebound cutloss: buy same side after %.2fx rebound, cap < %.0f¢, discard <= %.0f¢, "
        "stop-buy=%ds, single-sell 100%% at x%.2f (target capped by %.0f¢)",
        REBOUND_CUTLOSS_MULT, REBOUND_CUTLOSS_CAP * 100, REBOUND_CUTLOSS_DEAD_ZONE * 100,
        REBOUND_STOP_BUY_AT, REBOUND_SELL_MULTIPLIER, REBOUND_MAX_TARGET_PRICE * 100,
    )
    log.info("  Gap guard: swing=%s  magnitude=%s  wait=%s",
             {k: f"{v*100:.2f}%" for k, v in GAP_SWING.items()}, GAP_MAGNITUDE, GAP_WAIT_SECS)
    log.info("=" * 55)

    start_http_server()
    client = build_client()
    start_rsi_feed()   # Binance WebSocket — populates candle_open + live_close

    last_status = time.time()
    last_window = None
    executor    = ThreadPoolExecutor(max_workers=len(ASSETS))
    was_idle    = None
    secs_left   = 0

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = WINDOW_SECS - secs_into

            # ── New window reset ──────────────────────────────────────────────
            if last_window is not None and window_start != last_window:
                if open_positions:
                    log.info("[WINDOW] Closing %d stale positions from prior window",
                             len(open_positions))
                    open_positions.clear()
                armed_logged = False
                token_cache.clear()
                live_prices.clear()
                traded_this_window.clear()
                flipped_this_window.clear()
                gap_wait.clear()
                peak_gap.clear()
                oppo_bought_windows.clear()
                normal_blacklisted_assets.clear()
                trend_guarded_assets.clear()
                oppo_rebound_tracker.clear()
                rebound_cutloss_tracker.clear()
                log.info("[WINDOW] New window  ts=%d  secs_left=%d  entry at %ds",
                         window_start, secs_left, ENTRY_AFTER)
            last_window = window_start

            if secs_into >= ENTRY_AFTER and not armed_logged:
                log.info("[ARMED] Window armed — buy zone active")
                armed_logged = True

            idle_now = not can_open_new_trades(server_ts)
            if idle_now:
                if was_idle is not True:
                    log.info("[IDLE] Outside trading window — no new entries")
                was_idle = True
            else:
                if was_idle is not False:
                    log.info("[IDLE] Trading window open — resumed")
                was_idle = False

                # If extreme-gap skip is active, sleep whole windows when flat.
                if (
                    skip_buy_until_window is not None
                    and window_start < skip_buy_until_window
                    and not open_positions
                ):
                    sleep_for = max(1, secs_left + 1)
                    windows_left = int((skip_buy_until_window - window_start) // WINDOW_SECS)
                    log.info("[SLEEP-SKIP] Flat + buy-skip active, sleeping %ds (windows_left=%d)",
                             sleep_for, windows_left)
                    time.sleep(sleep_for)
                    continue

                scan_markets(client, window_start, secs_into, server_ts, executor)

            manage_positions(client, server_ts)
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

        if time.time() - last_status >= 3600:
            print_status()
            last_status = time.time()

        # PnL snapshot every 30 min for chart
        now_t = time.time()
        if now_t - last_pnl_snapshot >= 1800:
            last_pnl_snapshot = now_t
            pnl_history.append({
                "ts":  datetime.now().strftime("%H:%M"),
                "pnl": round(stats["pnl"], 4),
            })
            if len(pnl_history) > 288:
                pnl_history.pop(0)

        if idle_now and not open_positions:
            time.sleep(30)
        else:
            time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
