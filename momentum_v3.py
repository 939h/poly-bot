"""
Polymarket 15-Min Up/Down Bot — momentum bot v3
============================================
Strategy:
  Buy YES or NO when price hits BUY_PRICE_MIN–BUY_PRICE_MAX between
  ENTRY_AFTER and STOP_BUY_AT seconds into window.

  Gap guard (pre-buy, inverted vs panic_rsi):
    Checks abs(kraken_live_close - kraken_candle_open) >= threshold.
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
    OPPO_COUNTER_ENABLED=false
    OPPO_COUNTER_MIN_PRICE=0.05
    OPPO_COUNTER_MAX_PRICE=0.08
    OPPO_COUNTER_SELL_MULTIPLIER=1.4
    OPPO_COUNTER_SELL_CAP=0.94
    OPPO_COUNTER_CUT_LOSS_PCT=0.60
    OPPO_GOLDEN_GAP_MAG=3.0
    OPPO_OPTIMIZER_ENABLED=true
    OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES=8
    OPPO_TRADE_OPTIMIZER_ENABLED=true
    OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES=5
    OPPO_OPTIMIZER_MIN_OBSERVATION_SECS=60
    OPPO_OPTIMIZER_MIN_PRICE_UPDATES=5
    OPPO_OPTIMIZER_MAX_MULTIPLE_CAP=10
    OPPO_OPTIMIZER_NO_PUMP_MULTIPLE=1.1
    OPPO_OPTIMIZER_HISTORY_HOURS=24
    OPPO_OPTIMIZER_HISTORY_REFRESH_SECS=60
    OPPO_OPTIMIZER_SCORE_EQUIVALENCE=0.10
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

from kraken_ws import (
    candle_open,
    live_close,
    start_kraken_metrics_feed,
    get_cvd_snapshot,
    get_ema_snapshot,
    get_candle_history,
    get_volume_snapshot,
    get_rvol_reversal_snapshot,
    get_golden_optimizer_snapshot,
)

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
FLEXI_RVOL_BUY_AMOUNT = float(os.getenv("FLEXI_RVOL_BUY_AMOUNT", "1"))  # USDC for low-RVOL OPPO flex orders
FLEXI_RVOL_ENABLED = os.getenv("FLEXI_RVOL_ENABLED", "true").lower() == "true"

# ── Buy trigger ───────────────────────────────────────────────────────────────
BUY_PRICE_MIN  = 1.00   # buy if price >= this
BUY_PRICE_MAX  = 1.00   # buy if price <= this
ENTRY_AFTER    = 25    # seconds into window before buying allowed (5 min)
STOP_BUY_AT    = 810    # seconds into window after which no new buys (13.5 min)
TREND_GUARD_PRICE = 0.65
TREND_GUARD_MIN_CONFIRMATIONS = 2
# Backward-compatible status constant for deployments or integrations that still
# read the old setting. It is intentionally always False; EMA never gates buys.
EMA_CONFIRM_ENABLED = False
EMA_FAST_PERIOD = int(os.getenv("EMA_FAST_PERIOD", "8"))
EMA_SLOW_PERIOD = int(os.getenv("EMA_SLOW_PERIOD", "25"))


# ── Gap guard (inverted — large gap ALLOWS buy) ───────────────────────────────
# abs(kraken_live_close - kraken_candle_open) >= threshold → allow buy
# threshold = candle_open × GAP_SWING[asset] × GAP_MAGNITUDE[stage]
GAP_SWING = {
    "btc": 0.001,    # 0.1% of BTC open
    "eth": 0.001,   # 0.15% of ETH open
    "sol": 0.001,    # 0.1% of SOL open
    "xrp": 0.001,    # 0.2% of XRP open
}
GAP_MAGNITUDE = {
    "early": 1.0,   # 0–5 min
    "mid":   1.0,   # 5–10 min
    "late":  1.0,   # 10–15 min
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
HOLD_MID_SECS   = 10    # force-stop cooldown 5–10 min
HOLD_LATE_SECS  = 5    # force-stop cooldown 10–15 min
FORCE_SELL_GAP_MULT = float(os.getenv("FORCE_SELL_GAP_MULT", "50"))
BREAKEVEN_GAP_MULT = float(os.getenv("BREAKEVEN_GAP_MULT", "20.0")) #original is 1.5 - set 20.0 is "disable" it purposely
BREAKEVEN_POLL_CONFIRMATIONS = int(os.getenv("BREAKEVEN_POLL_CONFIRMATIONS", "5"))

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
OPPO_MAX_PRICE         = float(os.getenv("OPPO_MAX_PRICE", "0.15"))
OPPO_MIN_PRICE         = float(os.getenv("OPPO_MIN_PRICE", "0.03"))
OPPO_REBOUND_MAX_PRICE = float(os.getenv("OPPO_REBOUND_MAX_PRICE", "0.25"))
OPPO_GAP_MAG           = float(os.getenv("OPPO_GAP_MAG", "2.0"))
OPPO_GOLDEN_GAP_MAG    = float(os.getenv("OPPO_GOLDEN_GAP_MAG", "3.0"))
OPPO_SELL_MULTIPLIER   = float(os.getenv("OPPO_SELL_MULTIPLIER", "5.0"))
OPPO_SELL_CAP          = float(os.getenv("OPPO_SELL_CAP", "0.80"))
OPPO_CUT_LOSS_PCT      = float(os.getenv("OPPO_CUT_LOSS_PCT", "0.40")) #set 0.20 means lose 80% of fund
OPPO_REBOUND_MULT      = float(os.getenv("OPPO_REBOUND_MULT", "2.0"))
OPPO_FALLING_KNIFE_MIN_MOVE = float(os.getenv("OPPO_FALLING_KNIFE_MIN_MOVE", "0.25"))
OPPO_DEAD_ZONE         = float(os.getenv("OPPO_DEAD_ZONE", "0.04"))
OPPO_FIRST_SELL_FRACTION = 0.50
OPPO_FIRST_SELL_MULTIPLIER = 2.0
OPPO_FINAL_SELL_MULTIPLIER = 10.0
OPPO_TP2_TRAIL_PCT = float(os.getenv("OPPO_TP2_TRAIL_PCT", "0.40"))
OPPO_COUNTER_ENABLED = os.getenv("OPPO_COUNTER_ENABLED", "false").lower() == "false"
OPPO_COUNTER_MIN_PRICE = float(os.getenv("OPPO_COUNTER_MIN_PRICE", "0.05"))
OPPO_COUNTER_MAX_PRICE = float(os.getenv("OPPO_COUNTER_MAX_PRICE", "0.08"))
OPPO_COUNTER_BUY_AMOUNT = float(os.getenv("OPPO_COUNTER_BUY_AMOUNT", "1"))
OPPO_COUNTER_SELL_MULTIPLIER = float(os.getenv("OPPO_COUNTER_SELL_MULTIPLIER", "7"))
OPPO_COUNTER_SELL_CAP = float(os.getenv("OPPO_COUNTER_SELL_CAP", "0.5"))
OPPO_COUNTER_CUT_LOSS_PCT = float(os.getenv("OPPO_COUNTER_CUT_LOSS_PCT", "0.5"))
CVD_OPPO_ENABLED = os.getenv("CVD_OPPO_ENABLED", "true").lower() == "true"
CVD_OPPO_SLOPE_POLLS = max(1, int(os.getenv("CVD_OPPO_SLOPE_POLLS", "5")))
VOLUME_AVG_PERIOD = max(1, int(os.getenv("VOLUME_AVG_PERIOD", "20")))
RVOL_MIN_PER_MIN = float(os.getenv("RVOL_MIN_PER_MIN", str(1 / 15)))
RVOL_MIN = RVOL_MIN_PER_MIN * 15
OPPO_RVOL_GUARD_ENABLED = os.getenv("OPPO_RVOL_GUARD_ENABLED", "true").lower() == "true"
OPPO_GOLDEN_RVOL_ENABLED = os.getenv("OPPO_GOLDEN_RVOL_ENABLED", "true").lower() == "true"
OPPO_GOLDEN_RVOL_LOOKBACK = max(1, int(os.getenv("OPPO_GOLDEN_RVOL_LOOKBACK", "3")))
OPPO_GOLDEN_RVOL_MIN_HIGH = max(1, int(os.getenv("OPPO_GOLDEN_RVOL_MIN_HIGH", "2")))
OPPO_GOLDEN_RVOL_THRESHOLD = float(os.getenv("OPPO_GOLDEN_RVOL_THRESHOLD", "1.0"))
OPPO_GOLDEN_MIN_PROBABILITY = float(os.getenv("OPPO_GOLDEN_MIN_PROBABILITY", "0.5"))
OPPO_GOLDEN_MIN_SAMPLES = max(0, int(os.getenv("OPPO_GOLDEN_MIN_SAMPLES", "5")))
OPPO_OPTIMIZER_ENABLED = os.getenv("OPPO_OPTIMIZER_ENABLED", "true").lower() == "true"
OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES = max(1, int(os.getenv("OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES", "8")))
OPPO_TRADE_OPTIMIZER_ENABLED = os.getenv("OPPO_TRADE_OPTIMIZER_ENABLED", "true").lower() == "true"
OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES = max(1, int(os.getenv("OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES", "5")))
OPPO_OPTIMIZER_MIN_OBSERVATION_SECS = max(0, int(os.getenv("OPPO_OPTIMIZER_MIN_OBSERVATION_SECS", "60")))
OPPO_OPTIMIZER_MIN_PRICE_UPDATES = max(1, int(os.getenv("OPPO_OPTIMIZER_MIN_PRICE_UPDATES", "5")))
OPPO_OPTIMIZER_MAX_MULTIPLE_CAP = max(1.0, float(os.getenv("OPPO_OPTIMIZER_MAX_MULTIPLE_CAP", "10")))
OPPO_OPTIMIZER_NO_PUMP_MULTIPLE = max(1.0, float(os.getenv("OPPO_OPTIMIZER_NO_PUMP_MULTIPLE", "1.1")))
OPPO_OPTIMIZER_HISTORY_HOURS = max(1, int(os.getenv("OPPO_OPTIMIZER_HISTORY_HOURS", "24")))
OPPO_OPTIMIZER_HISTORY_REFRESH_SECS = max(10, int(os.getenv("OPPO_OPTIMIZER_HISTORY_REFRESH_SECS", "60")))
OPPO_OPTIMIZER_SCORE_EQUIVALENCE = max(0.0, float(os.getenv("OPPO_OPTIMIZER_SCORE_EQUIVALENCE", "0.10")))

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

# ── Pump tracker ─────────────────────────────────────────────────────────────
PUMP_TRACK_START_PRICE = float(os.getenv("PUMP_TRACK_START_PRICE", "0.15"))
PUMP_TRACK_DEAD_ZONE_PRICE = float(os.getenv("PUMP_TRACK_DEAD_ZONE_PRICE", "0.04"))
PUMP_TRACK_MILESTONES = (1.9, 4, 5)
PUMP_TRACK_SUCCESS_MIN_MULTIPLE = float(os.getenv("PUMP_TRACK_SUCCESS_MIN_MULTIPLE", "2.0"))
PUMP_TRACK_WINDOW_SECS = WINDOW_SECS



def validate_settings():
    errors = []
    if REBOUND_BUY_AMOUNT <= 0:
        errors.append("REBOUND_BUY_AMOUNT must be > 0")
    if FLEXI_RVOL_BUY_AMOUNT <= 0:
        errors.append("FLEXI_RVOL_BUY_AMOUNT must be > 0")
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
    if OPPO_REBOUND_MAX_PRICE <= OPPO_MAX_PRICE:
        errors.append("OPPO_REBOUND_MAX_PRICE must be greater than OPPO_MAX_PRICE")
    if OPPO_REBOUND_MAX_PRICE >= 1.0:
        errors.append("OPPO_REBOUND_MAX_PRICE must be < 1.0")
    if OPPO_COUNTER_BUY_AMOUNT <= 0:
        errors.append("OPPO_COUNTER_BUY_AMOUNT must be > 0")
    if not 0 <= OPPO_COUNTER_MIN_PRICE <= OPPO_COUNTER_MAX_PRICE < 1:
        errors.append("OPPO_COUNTER_MIN_PRICE/MAX_PRICE must satisfy 0 <= min <= max < 1")
    if OPPO_COUNTER_SELL_MULTIPLIER <= 0:
        errors.append("OPPO_COUNTER_SELL_MULTIPLIER must be > 0")
    if not 0 < OPPO_COUNTER_SELL_CAP < 1:
        errors.append("OPPO_COUNTER_SELL_CAP must be between 0 and 1")
    if not 0 < OPPO_COUNTER_CUT_LOSS_PCT < 1:
        errors.append("OPPO_COUNTER_CUT_LOSS_PCT must be between 0 and 1")
    if OPPO_FALLING_KNIFE_MIN_MOVE <= 0:
        errors.append("OPPO_FALLING_KNIFE_MIN_MOVE must be > 0")
    if OPPO_GAP_MAG <= 0:
        errors.append("OPPO_GAP_MAG must be > 0")
    if OPPO_GOLDEN_GAP_MAG <= 0:
        errors.append("OPPO_GOLDEN_GAP_MAG must be > 0")
    if PUMP_TRACK_SUCCESS_MIN_MULTIPLE <= 0:
        errors.append("PUMP_TRACK_SUCCESS_MIN_MULTIPLE must be > 0")
    if OPPO_OPTIMIZER_MAX_MULTIPLE_CAP < OPPO_OPTIMIZER_NO_PUMP_MULTIPLE:
        errors.append("OPPO_OPTIMIZER_MAX_MULTIPLE_CAP must be >= OPPO_OPTIMIZER_NO_PUMP_MULTIPLE")
    if VOLUME_AVG_PERIOD <= 0:
        errors.append("VOLUME_AVG_PERIOD must be > 0")
    if RVOL_MIN_PER_MIN <= 0:
        errors.append("RVOL_MIN_PER_MIN must be > 0")
    if OPPO_GOLDEN_RVOL_MIN_HIGH > OPPO_GOLDEN_RVOL_LOOKBACK:
        errors.append("OPPO_GOLDEN_RVOL_MIN_HIGH must be <= OPPO_GOLDEN_RVOL_LOOKBACK")
    if OPPO_GOLDEN_RVOL_THRESHOLD <= 0:
        errors.append("OPPO_GOLDEN_RVOL_THRESHOLD must be > 0")
    if not 0 <= OPPO_GOLDEN_MIN_PROBABILITY <= 1:
        errors.append("OPPO_GOLDEN_MIN_PROBABILITY must be between 0 and 1")
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
normal_blacklisted_assets = set()  # assets blacklisted for normal buys this window
oppo_dead_zone_blacklisted_assets = set()  # assets whose OPPO price hit dead-zone this window
oppo_rvol_blacklisted_assets = set()  # assets blocked by OPPO RVOL guard this window
oppo_knife_blacklisted_assets = set()  # assets blocked by OPPO falling-knife guard this window
trend_guarded_assets = set()       # assets blocked by trend guard this window
oppo_rebound_tracker = {}          # key asset_side -> trough price
oppo_counter_tracker = {}          # key asset_side -> counter buy tracking armed after OPPO TP2
oppo_cvd_polls = {}                # key asset_side -> consecutive cvd-confirmed polls
oppo_last_trigger = {}             # key asset_side -> latest oppo trigger/status for dashboard
oppo_log_suppressed_until = 0.0    # unix ts; temporarily suppress OPPO log repopulation after manual reset

def _counter_key(asset, side):
    return f"{asset}_{side}_counter"


def _base_price_key(key):
    if key.endswith("_counter"):
        return key[:-8]
    if key.endswith("_oppo"):
        return key[:-5]
    return key


def _arm_oppo_counter(asset, oppo_side, price, window_start, reason):
    if not OPPO_COUNTER_ENABLED:
        return
    if asset in oppo_knife_blacklisted_assets:
        return
    counter_side = "no" if oppo_side == "yes" else "yes"
    counter_key = f"{asset}_{counter_side}"
    if _counter_key(asset, counter_side) in open_positions:
        return
    if counter_key in oppo_counter_tracker:
        return
    token = get_token_for_key(asset, counter_side, window_start)
    oppo_counter_tracker[counter_key] = {
        "asset": asset,
        "side": counter_side,
        "token": token,
        "window_start": window_start,
        "armed_at": time.time(),
        "source_price": price,
        "reason": reason,
    }
    log.info(
        "[OPPO-COUNTER-ARM] %s armed after %s at %.4f — buy %s at %.0f–%.0f¢ between %d–%ds",
        counter_key, reason, price, counter_key, OPPO_COUNTER_MIN_PRICE * 100,
        OPPO_COUNTER_MAX_PRICE * 100, ENTRY_AFTER, STOP_BUY_AT,
    )
    _record_oppo_trigger(asset, counter_side, price, "COUNTER-ARM", reason)


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
oppo_dashboard_once_per_window = set()  # (asset, side, status) entries shown only once per market window
pump_tracker       = {}  # key asset_side -> trough/current/multiple tracking for prices starting below 20c
pump_log           = []  # historical pump milestone events
pump_finished_tracker_keys = set()  # (window_start, asset_side) pairs already finalized this window
optimizer_recommendation_history = []  # config changes retained for CSV export
optimizer_history_last_refresh = 0.0
last_pnl_snapshot  = 0
ema_history = {a: deque(maxlen=120) for a in ASSETS}
cvd_history = {a: deque(maxlen=120) for a in ASSETS}

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

def _migrate_gap_ratio_names(value):
    """Rename persisted legacy gap-magnitude fields without changing their values."""
    if isinstance(value, dict):
        renames = {
            "kraken_gap_magnitude": "kraken_gap_ratio",
            "entry_gap_magnitude": "entry_kraken_gap_ratio",
            "max_gap_magnitude": "max_kraken_gap_ratio",
        }
        for old_name, new_name in renames.items():
            if old_name in value and new_name not in value:
                value[new_name] = value.pop(old_name)
        for nested in value.values():
            _migrate_gap_ratio_names(nested)
    elif isinstance(value, list):
        for nested in value:
            _migrate_gap_ratio_names(nested)
    return value

def load_state():
    global stats, pnl_history, asset_history, trade_log, pump_tracker, pump_log, pump_finished_tracker_keys, optimizer_recommendation_history, last_pnl_snapshot
    if not os.path.exists(STATE_FILE):
        log.info("[STATE] No saved state — starting fresh")
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = _migrate_gap_ratio_names(json.load(f))
        saved_stats = saved.get("stats", {})
        for k in stats:
            if k in saved_stats:
                stats[k] = saved_stats[k]
        pnl_history   = saved.get("pnl_history", [])
        asset_history = saved.get("asset_history", {})
        trade_log     = saved.get("trade_log", [])
        pump_tracker  = {
            k: v for k, v in saved.get("pump_tracker", {}).items()
            if float(v.get("current", v.get("base_price", 0.0)) or 0.0) >= PUMP_TRACK_DEAD_ZONE_PRICE
        }
        pump_log      = saved.get("pump_log", [])
        optimizer_recommendation_history = saved.get("optimizer_recommendation_history", [])
        _prune_optimizer_recommendation_history()
        pump_finished_tracker_keys = _finished_pump_keys_from_log(pump_log)
        if pnl_history:
            last_pnl_snapshot = time.time()
        log.info(
            "[STATE] Restored — buys=%d  wins=%d  losses=%d  pnl=$%.4f  "
            "trades=%d  pumps=%d  optimizer_changes=%d  pnl_pts=%d",
            stats["buys"], stats["wins"], stats["losses"], stats["pnl"],
            len(trade_log), len(pump_log), len(optimizer_recommendation_history), len(pnl_history),
        )
    except Exception as e:
        log.warning("[STATE] Load failed: %s — starting fresh", e)


def reset_state():
    global stats, pnl_history, asset_history, trade_log, oppo_trigger_log, pump_tracker, pump_log, pump_finished_tracker_keys, optimizer_recommendation_history, last_pnl_snapshot, ema_history, cvd_history
    stats = {"scans": 0, "triggers": 0, "buys": 0, "wins": 0, "losses": 0, "pnl": 0.0}
    pnl_history   = []
    asset_history = {}
    trade_log     = []
    oppo_trigger_log = []
    oppo_dashboard_once_per_window.clear()
    pump_tracker = {}
    pump_log = []
    optimizer_recommendation_history = []
    pump_finished_tracker_keys = set()
    normal_blacklisted_assets.clear()
    oppo_dead_zone_blacklisted_assets.clear()
    oppo_rvol_blacklisted_assets.clear()
    oppo_knife_blacklisted_assets.clear()
    trend_guarded_assets.clear()
    ema_history = {a: deque(maxlen=120) for a in ASSETS}
    cvd_history = {a: deque(maxlen=240) for a in ASSETS}
    last_pnl_snapshot = 0
    log.info("[STATE] Reset by user")
    save_state()

def reset_oppo_log():
    global oppo_log_suppressed_until
    oppo_last_trigger.clear()
    oppo_trigger_log.clear()
    oppo_dashboard_once_per_window.clear()
    # Prevent immediate re-population from the very next scan cycle.
    oppo_log_suppressed_until = time.time() + max(2.0, POLL_SECS * 3)
    log.info("[STATE] OPPO trigger log reset by user")
    save_state()


def get_position_live_price(key, fallback):
    price = live_prices.get(key)
    if price is not None and price > 0:
        return price
    base_key = _base_price_key(key)
    if base_key != key:
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
            "is_counter":  p.get("is_counter", k.endswith("_counter")),
            "is_oppo":     p.get("is_oppo", k.endswith("_oppo")),
            "rebound_tranches": p.get("rebound_tranches", []),
            "cut_loss_pct": round(p.get("cut_loss_pct", OPPO_CUT_LOSS_PCT if k.endswith("_oppo") else CUT_LOSS_PCT), 4),
            "pnl":         pnl_unreal,
            "pct":         max(0, min(100, pct)),
            "opened_at":   p.get("opened_at", "—"),
            "rebound_buy_amount": REBOUND_BUY_AMOUNT,
            "breakeven_armed": bool(p.get("breakeven_armed", False)),
            "breakeven_gap_polls": int(p.get("breakeven_gap_polls", 0)),
        }
    gap_out = {}
    gap_threshold_out = {}
    cvd_out = {}
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
        "cvd": cvd_out,
        "pnl_history":   list(pnl_history),
        "asset_history": dict(asset_history),
        "trade_log":     list(trade_log),
        "oppo_trigger_log": list(oppo_trigger_log),
        "pump_tracker": {
            k: {
                "asset": v.get("asset", k.split("_")[0]).upper(),
                "side": v.get("side", k.split("_")[1] if "_" in k else "").upper(),
                "window_start": v.get("window_start"),
                "started_at": v.get("started_at", ""),
                "base_price": round(float(v.get("base_price", 0.0)), 4),
                "trough": round(float(v.get("trough", 0.0)), 4),
                "current": round(float(v.get("current", 0.0)), 4),
                "multiple": round(float(v.get("multiple", 0.0)), 3),
                "max_price": round(float(v.get("max_price", 0.0)), 4),
                "max_multiple": round(float(v.get("max_multiple", 0.0)), 3),
                "kraken_gap": round(float(v["kraken_gap"]), 4) if v.get("kraken_gap") is not None else None,
                "kraken_gap_ratio": round(float(v["kraken_gap_ratio"]), 4) if v.get("kraken_gap_ratio") is not None else None,
                "cvd_slope": round(float(v["cvd_slope"]), 6) if v.get("cvd_slope") is not None else None,
                "rvol": round(float(v["rvol"]), 3) if v.get("rvol") is not None else None,
                "entry_at": v.get("entry_at", v.get("started_at", "")),
                "entry_ts": v.get("entry_ts"),
                "price_updates": int(v.get("price_updates", 0)),
                "entry_kraken_gap_ratio": round(float(v["entry_kraken_gap_ratio"]), 4) if v.get("entry_kraken_gap_ratio") is not None else None,
                "entry_cvd_slope": round(float(v["entry_cvd_slope"]), 6) if v.get("entry_cvd_slope") is not None else None,
                "entry_rvol": round(float(v["entry_rvol"]), 3) if v.get("entry_rvol") is not None else None,
                "status": v.get("status", "TRACKING"),
                "highest_milestone": int(v.get("highest_milestone", 1)),
            } for k, v in pump_tracker.items()
        },
        "pump_log":       list(pump_log),
        "optimizer_recommendation_history": list(optimizer_recommendation_history),
        "settings": {
            "assets":     ASSETS,
            "buy_min":    BUY_PRICE_MIN,
            "buy_max":    BUY_PRICE_MAX,
            "sell_multiplier": SELL_MULTIPLIER,
            "sell_cap":   SELL_CAP,
            "cut_loss":   CUT_LOSS_PCT,
            "oppo_cut_loss": OPPO_CUT_LOSS_PCT,
            "oppo_min_price": OPPO_MIN_PRICE,
            "oppo_max_price": OPPO_MAX_PRICE,
            "oppo_rebound_mult": OPPO_REBOUND_MULT,
            "oppo_falling_knife_min_move": OPPO_FALLING_KNIFE_MIN_MOVE,
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
            "oppo_rebound_max_price": OPPO_REBOUND_MAX_PRICE,
            "oppo_gap_mag": OPPO_GAP_MAG,
            "oppo_golden_gap_mag": OPPO_GOLDEN_GAP_MAG,
            "oppo_counter_enabled": OPPO_COUNTER_ENABLED,
            "oppo_counter_min_price": OPPO_COUNTER_MIN_PRICE,
            "oppo_counter_max_price": OPPO_COUNTER_MAX_PRICE,
            "oppo_counter_buy_amount": OPPO_COUNTER_BUY_AMOUNT,
            "oppo_counter_sell_multiplier": OPPO_COUNTER_SELL_MULTIPLIER,
            "oppo_counter_sell_cap": OPPO_COUNTER_SELL_CAP,
            "oppo_counter_cut_loss_pct": OPPO_COUNTER_CUT_LOSS_PCT,
            "flexi_rvol_buy_amount": FLEXI_RVOL_BUY_AMOUNT,
            "flexi_rvol_enabled": FLEXI_RVOL_ENABLED,
            "order":      BUY_AMOUNT,
            "poll":       POLL_SECS,
            "breakeven_polls": BREAKEVEN_POLL_CONFIRMATIONS,
            "entry_after": ENTRY_AFTER,
            "stop_buy":   STOP_BUY_AT,
            "volume_avg_period": VOLUME_AVG_PERIOD,
            "rvol_min": RVOL_MIN,
            "rvol_min_per_min": RVOL_MIN_PER_MIN,
            "oppo_rvol_guard_enabled": OPPO_RVOL_GUARD_ENABLED,
            "oppo_golden_rvol_enabled": OPPO_GOLDEN_RVOL_ENABLED,
            "oppo_golden_rvol_rule": f"{OPPO_GOLDEN_RVOL_MIN_HIGH}/{OPPO_GOLDEN_RVOL_LOOKBACK} > {OPPO_GOLDEN_RVOL_THRESHOLD:.2f}",
            "oppo_golden_rvol_threshold": OPPO_GOLDEN_RVOL_THRESHOLD,
            "oppo_golden_min_probability": OPPO_GOLDEN_MIN_PROBABILITY,
            "oppo_golden_min_samples": OPPO_GOLDEN_MIN_SAMPLES,
            "oppo_optimizer_enabled": OPPO_OPTIMIZER_ENABLED,
            "oppo_optimizer_min_validation_samples": OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES,
            "oppo_trade_optimizer_enabled": OPPO_TRADE_OPTIMIZER_ENABLED,
            "oppo_trade_optimizer_min_validation_trades": OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES,
            "oppo_optimizer_min_observation_secs": OPPO_OPTIMIZER_MIN_OBSERVATION_SECS,
            "oppo_optimizer_min_price_updates": OPPO_OPTIMIZER_MIN_PRICE_UPDATES,
            "oppo_optimizer_max_multiple_cap": OPPO_OPTIMIZER_MAX_MULTIPLE_CAP,
            "oppo_optimizer_no_pump_multiple": OPPO_OPTIMIZER_NO_PUMP_MULTIPLE,
            "oppo_optimizer_history_hours": OPPO_OPTIMIZER_HISTORY_HOURS,
            "oppo_optimizer_score_equivalence": OPPO_OPTIMIZER_SCORE_EQUIVALENCE,
            "pump_track_start_price": PUMP_TRACK_START_PRICE,
            "pump_track_window_secs": PUMP_TRACK_WINDOW_SECS,
            "pump_track_dead_zone_price": PUMP_TRACK_DEAD_ZONE_PRICE,
            "pump_track_success_min_multiple": PUMP_TRACK_SUCCESS_MIN_MULTIPLE,
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
    entry_rvol = pos.get("entry_rvol")
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
        "is_counter": pos.get("is_counter", False),
        "is_oppo": pos.get("is_oppo", key.endswith("_oppo")),
        "is_golden_oppo": bool(pos.get("is_golden_oppo", False)),
        "pnl":      round(pnl, 4),
        "entry_rvol": round(float(entry_rvol), 3) if entry_rvol is not None else None,
        "entry_kraken_gap_ratio": pos.get("entry_kraken_gap_ratio"),
        "entry_rebound_ratio": pos.get("entry_rebound_ratio"),
        "entry_cvd_slope": pos.get("entry_cvd_slope"),
    }
    trade_log.insert(0, record)
    if len(trade_log) > 200:
        trade_log.pop()



def _build_oppo_trade_optimizer_snapshot():
    """Recommend robust standard OPPO filters from completed pump traces."""
    completed = [event for event in reversed(pump_log) if event.get("status") in ("SUCCESS", "FAILED")]

    def quality_reason(event):
        if event.get("entry_rvol") is None or event.get("entry_kraken_gap_ratio") is None:
            return "missing-entry-metrics"
        if float(event.get("max_multiple", 0.0) or 0.0) <= 0:
            return "invalid-multiple"
        # Legacy rows without quality counters remain usable. Full-window traces and
        # genuine fast failures remain negative evidence; under-observed rows do not.
        if event.get("observation_secs") is not None and float(event["observation_secs"]) < OPPO_OPTIMIZER_MIN_OBSERVATION_SECS:
            return "short-observation"
        if event.get("price_updates") is not None and int(event["price_updates"]) < OPPO_OPTIMIZER_MIN_PRICE_UPDATES:
            return "few-price-updates"
        return None

    quality_exclusions = {}
    samples = []
    for event in completed:
        reason = quality_reason(event)
        if reason:
            quality_exclusions[reason] = quality_exclusions.get(reason, 0) + 1
        else:
            samples.append(event)
    result = {
        "mode": "shadow-recommend-only",
        "enabled": OPPO_TRADE_OPTIMIZER_ENABLED,
        "ready": False,
        "samples": len(samples),
        "trades": len(samples),
        "quality_excluded": len(completed) - len(samples),
        "quality_exclusions": quality_exclusions,
        "note": "Pump traces run until dead-zone exit or window end; robust score penalizes no-pumps, outliers, sparse samples, and train/validation instability",
    }
    if not OPPO_TRADE_OPTIMIZER_ENABLED:
        result["readiness_reason"] = "optimizer disabled"
        return result
    if len(samples) < OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES * 2:
        result["readiness_reason"] = "collecting more quality pump traces"
        return result
    validation_start = max(1, int(len(samples) * 0.70))

    def summarize(multiples):
        capped = [min(value, OPPO_OPTIMIZER_MAX_MULTIPLE_CAP) for value in multiples]
        ordered = sorted(capped)
        trim = int(len(ordered) * 0.10) if len(ordered) >= 10 else 0
        trimmed = ordered[trim:len(ordered) - trim] if trim else ordered
        rate = lambda threshold: sum(value >= threshold for value in multiples) / len(multiples) if multiples else None
        no_pumps = sum(value < OPPO_OPTIMIZER_NO_PUMP_MULTIPLE for value in multiples)
        weak_pumps = sum(value < PUMP_TRACK_SUCCESS_MIN_MULTIPLE for value in multiples)
        return {
            "samples": len(multiples), "trades": len(multiples),
            "wins": sum(value >= PUMP_TRACK_SUCCESS_MIN_MULTIPLE for value in multiples),
            "good_pumps": sum(value >= PUMP_TRACK_SUCCESS_MIN_MULTIPLE for value in multiples),
            "weak_pumps": weak_pumps, "weak_pump_rate": weak_pumps / len(multiples) if multiples else None,
            "rate": rate(PUMP_TRACK_SUCCESS_MIN_MULTIPLE), "win_rate": rate(PUMP_TRACK_SUCCESS_MIN_MULTIPLE),
            "rate_1_5x": rate(1.5), "rate_2x": rate(2.0), "rate_3x": rate(3.0),
            "rate_4x": rate(4.0), "rate_5x": rate(5.0),
            "no_pumps": no_pumps, "no_pump_rate": no_pumps / len(multiples) if multiples else None,
            "average_max_multiple": round(sum(multiples) / len(multiples), 4) if multiples else None,
            "capped_average_max_multiple": round(sum(capped) / len(capped), 4) if capped else None,
            "trimmed_average_max_multiple": round(sum(trimmed) / len(trimmed), 4) if trimmed else None,
            "median_max_multiple": round(float(np.median(multiples)), 4) if multiples else None,
            "highest_max_multiple": round(max(multiples), 4) if multiples else None,
        }

    dataset_train = summarize([float(sample["max_multiple"]) for sample in samples[:validation_start]])
    dataset_validation = summarize([float(sample["max_multiple"]) for sample in samples[validation_start:]])
    outcome_diverse = bool(dataset_validation["weak_pumps"])
    good_pump_entry_ratios = [
        float(sample["entry_kraken_gap_ratio"])
        for sample in samples
        if float(sample["max_multiple"]) >= PUMP_TRACK_SUCCESS_MIN_MULTIPLE
    ]
    good_pump_ratio_average = round(sum(good_pump_entry_ratios) / len(good_pump_entry_ratios), 4) if good_pump_entry_ratios else None
    good_pump_ratio_median = round(float(np.median(good_pump_entry_ratios)), 4) if good_pump_entry_ratios else None
    result.update({
        "dataset_train": dataset_train,
        "dataset_validation": dataset_validation,
        "outcome_diverse": outcome_diverse,
        "outcome_warning": None if outcome_diverse else "validation data has no weak/failed pumps below 2x",
        "good_pump_entry_ratio_samples": len(good_pump_entry_ratios),
        "good_pump_entry_ratio_average": good_pump_ratio_average,
        "good_pump_entry_ratio_median": good_pump_ratio_median,
        "recommended_ratio_cap": good_pump_ratio_median,
        "readiness_reason": None,
    })

    def evaluate(config):
        sections = {"train": [], "validation": []}
        for index, sample in enumerate(samples):
            if float(sample["entry_rvol"]) < config["min_rvol"] or float(sample["entry_kraken_gap_ratio"]) > config["max_kraken_gap_ratio"]:
                continue
            sections["validation" if index >= validation_start else "train"].append(float(sample["max_multiple"]))
        metrics = {name: summarize(values) for name, values in sections.items()}
        train, val = metrics["train"], metrics["validation"]
        if val["samples"] < OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES:
            metrics["score"] = None
            return metrics
        confidence = min(1.0, val["samples"] / (OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES * 2))
        stability_penalty = abs((train["median_max_multiple"] or 0) - (val["median_max_multiple"] or 0)) * 0.5
        stability_penalty += abs((train["rate_2x"] or 0) - (val["rate_2x"] or 0))
        metrics["confidence"] = round(confidence, 4)
        metrics["stability_penalty"] = round(stability_penalty, 4)
        metrics["score"] = round(
            0.35 * val["median_max_multiple"]
            + 0.25 * val["trimmed_average_max_multiple"]
            + 0.20 * val["rate_2x"]
            + 0.10 * val["rate_4x"]
            + 0.10 * confidence
            - val["no_pump_rate"]
            - stability_penalty,
            6,
        )
        return metrics

    candidates = []
    ratio_candidates = {0.5, 1.0, 2.0, 3.0, 5.0, 10.0}
    if good_pump_ratio_median is not None:
        ratio_candidates.add(good_pump_ratio_median)
    for min_rvol in (0.0, 0.5, 1.0, 1.5, 2.0):
        for max_gap in sorted(ratio_candidates):
            config = {"min_rvol": min_rvol, "max_kraken_gap_ratio": max_gap}
            metrics = evaluate(config)
            candidates.append({"config": config, **metrics})
    eligible = [candidate for candidate in candidates if candidate["score"] is not None]
    eligible.sort(key=lambda item: (item["score"], item["validation"]["median_max_multiple"] or 0, item["validation"]["samples"]), reverse=True)

    recommendation = None
    ratio_capped_eligible = [
        item for item in eligible
        if good_pump_ratio_median is not None
        and item["config"]["max_kraken_gap_ratio"] <= good_pump_ratio_median
    ]
    if ratio_capped_eligible:
        best_score = ratio_capped_eligible[0]["score"]
        equivalent = [item for item in ratio_capped_eligible if item["score"] >= best_score - OPPO_OPTIMIZER_SCORE_EQUIVALENCE]
        max_coverage = max(item["validation"]["samples"] for item in equivalent)
        coverage_floor = max(OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES, math.ceil(max_coverage * 0.80))
        conservative = [item for item in equivalent if item["validation"]["samples"] >= coverage_floor]
        recommendation = min(
            conservative,
            key=lambda item: (item["config"]["max_kraken_gap_ratio"], -item["config"]["min_rvol"], -item["score"]),
        )

    current_config = {"min_rvol": 0.0, "max_kraken_gap_ratio": OPPO_GAP_MAG}
    result.update({
        "ready": recommendation is not None,
        "readiness_reason": None if recommendation is not None else "collecting enough samples below the good-pump median Kraken gap ratio",
        "candidate_count": len(eligible),
        "ratio_capped_candidate_count": len(ratio_capped_eligible),
        "evaluated_candidate_count": len(candidates),
        "recommendation": recommendation,
        "candidates": candidates,
        "current": {"config": current_config, **evaluate(current_config)},
        "score_equivalence": OPPO_OPTIMIZER_SCORE_EQUIVALENCE,
    })
    return result


def _prune_optimizer_recommendation_history(now=None):
    """Keep only the configured rolling recommendation-history window."""
    global optimizer_recommendation_history
    cutoff = float(now if now is not None else time.time()) - (OPPO_OPTIMIZER_HISTORY_HOURS * 3600)
    optimizer_recommendation_history = [
        row for row in optimizer_recommendation_history
        if float(row.get("timestamp_unix", 0) or 0) >= cutoff
    ]


def _optimizer_history_row(optimizer, asset, snapshot, now):
    recommendation = snapshot.get("recommendation") or {}
    config = recommendation.get("config") or {}
    train = recommendation.get("train") or {}
    validation = recommendation.get("validation") or {}
    current = snapshot.get("current") or {}
    current_train = current.get("train") or {}
    current_validation = current.get("validation") or {}
    return {
        "timestamp": datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="seconds"),
        "timestamp_unix": float(now),
        "optimizer": optimizer,
        "asset": asset,
        "config": dict(config),
        "train_samples": train.get("samples", train.get("trades")),
        "train_wins": train.get("wins"),
        "train_rate": train.get("rate", train.get("win_rate")),
        "train_pnl": train.get("pnl"),
        "validation_samples": validation.get("samples", validation.get("trades")),
        "validation_wins": validation.get("wins"),
        "validation_rate": validation.get("rate", validation.get("win_rate")),
        "validation_pnl": validation.get("pnl"),
        "validation_average_max_multiple": validation.get("average_max_multiple"),
        "validation_median_max_multiple": validation.get("median_max_multiple"),
        "validation_highest_max_multiple": validation.get("highest_max_multiple"),
        "current_train_samples": current_train.get("samples", current_train.get("trades")),
        "current_train_wins": current_train.get("wins"),
        "current_train_rate": current_train.get("rate", current_train.get("win_rate")),
        "current_train_pnl": current_train.get("pnl"),
        "current_validation_samples": current_validation.get("samples", current_validation.get("trades")),
        "current_validation_wins": current_validation.get("wins"),
        "current_validation_rate": current_validation.get("rate", current_validation.get("win_rate")),
        "current_validation_pnl": current_validation.get("pnl"),
        "current_validation_average_max_multiple": current_validation.get("average_max_multiple"),
        "current_validation_median_max_multiple": current_validation.get("median_max_multiple"),
        "current_validation_highest_max_multiple": current_validation.get("highest_max_multiple"),
        "good_pump_entry_ratio_samples": snapshot.get("good_pump_entry_ratio_samples"),
        "good_pump_entry_ratio_average": snapshot.get("good_pump_entry_ratio_average"),
        "good_pump_entry_ratio_median": snapshot.get("good_pump_entry_ratio_median"),
        "candidate_count": snapshot.get("candidate_count"),
        "score": recommendation.get("score"),
    }


def _record_optimizer_recommendations(golden_optimizer, oppo_trade_optimizer, now=None):
    """Record a row only when a ready optimizer's recommended config changes."""
    now = float(now if now is not None else time.time())
    _prune_optimizer_recommendation_history(now)
    snapshots = [("golden", asset.upper(), snapshot) for asset, snapshot in golden_optimizer.items()]
    snapshots.append(("standard", "ALL", oppo_trade_optimizer))
    for optimizer, asset, snapshot in snapshots:
        recommendation = snapshot.get("recommendation") if snapshot and snapshot.get("ready") else None
        if not recommendation or not recommendation.get("config"):
            continue
        latest = next((
            row for row in reversed(optimizer_recommendation_history)
            if row.get("optimizer") == optimizer and row.get("asset") == asset
        ), None)
        if latest and latest.get("config") == recommendation.get("config"):
            continue
        optimizer_recommendation_history.append(_optimizer_history_row(optimizer, asset, snapshot, now))


def _refresh_optimizer_recommendation_history(force=False, now=None):
    """Refresh recommendation history independently of whether the dashboard is open."""
    global optimizer_history_last_refresh
    now = float(now if now is not None else time.time())
    if not force and now - optimizer_history_last_refresh < OPPO_OPTIMIZER_HISTORY_REFRESH_SECS:
        return
    optimizer_history_last_refresh = now
    golden = {}
    for asset in ASSETS:
        golden[asset] = get_golden_optimizer_snapshot(
            asset, VOLUME_AVG_PERIOD,
            current={
                "lookback": OPPO_GOLDEN_RVOL_LOOKBACK,
                "min_high": OPPO_GOLDEN_RVOL_MIN_HIGH,
                "threshold": OPPO_GOLDEN_RVOL_THRESHOLD,
                "gap_magnitude": OPPO_GOLDEN_GAP_MAG,
            },
            min_validation_samples=OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES,
        ) if OPPO_OPTIMIZER_ENABLED else {"mode": "off", "ready": False}
    _record_optimizer_recommendations(golden, _build_oppo_trade_optimizer_snapshot(), now)


def _get_pump_kraken_snapshot(asset):
    """Return the latest Kraken gap plus Kraken CVD slope and RVOL for pump tracking."""
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)
    kraken_gap = round(abs(c_live - c_open), 4) if c_open > 0 and c_live is not None else None
    gap_unit = c_open * GAP_SWING.get(asset, 0.001) if c_open > 0 else 0.0
    kraken_gap_ratio = round(kraken_gap / gap_unit, 4) if kraken_gap is not None and gap_unit > 0 else None
    try:
        _, _, cvd_slope = get_cvd_snapshot(asset)
    except Exception:
        cvd_slope = None
    vol = get_volume_snapshot(asset, VOLUME_AVG_PERIOD, RVOL_MIN)
    rvol = vol.get("rvol") if vol else None
    return {
        "kraken_gap": kraken_gap,
        "kraken_gap_ratio": kraken_gap_ratio,
        "cvd_slope": round(float(cvd_slope), 6) if cvd_slope is not None else None,
        "rvol": round(float(rvol), 3) if rvol is not None else None,
    }


def _update_pump_kraken_snapshot(tracker):
    tracker.update(_get_pump_kraken_snapshot(tracker.get("asset")))


def _pump_finished_id(window_start, key):
    return (str(window_start), key.lower())


def _finished_pump_keys_from_log(log_entries):
    finished = set()
    for event in log_entries or []:
        if event.get("status") not in ("SUCCESS", "FAILED"):
            continue
        window_start = event.get("window_start")
        asset = str(event.get("asset", "")).lower()
        side = str(event.get("side", "")).lower()
        if window_start is not None and asset and side:
            finished.add(_pump_finished_id(window_start, f"{asset}_{side}"))
    return finished


def _pump_tracker_already_finished(window_start, key):
    return _pump_finished_id(window_start, key) in pump_finished_tracker_keys



def _pump_result_from_tracker(tracker):
    """Return SUCCESS only when the final pump is at least the configured minimum and near its max."""
    current_multiple = float(tracker.get("multiple", 0.0))
    max_multiple = float(tracker.get("max_multiple", 0.0))
    if max_multiple <= 0:
        return "FAILED"
    if current_multiple < PUMP_TRACK_SUCCESS_MIN_MULTIPLE:
        return "FAILED"
    return "SUCCESS" if current_multiple >= max_multiple * 0.85 else "FAILED"


def _record_pump_event(key, tracker, milestone, status=None):
    _update_pump_kraken_snapshot(tracker)
    event = {
        "time": datetime.now().strftime("%H:%M"),
        "window_start": tracker.get("window_start"),
        "asset": tracker.get("asset", key.split("_")[0]).upper(),
        "side": tracker.get("side", key.split("_")[1] if "_" in key else "").upper(),
        "base_price": round(float(tracker.get("base_price", 0.0)), 4),
        "trough": round(float(tracker.get("trough", 0.0)), 4),
        "current": round(float(tracker.get("current", 0.0)), 4),
        "multiple": round(float(tracker.get("multiple", 0.0)), 3),
        "max_price": round(float(tracker.get("max_price", 0.0)), 4),
        "max_multiple": round(float(tracker.get("max_multiple", 0.0)), 3),
        "kraken_gap": tracker.get("kraken_gap"),
        "kraken_gap_ratio": tracker.get("kraken_gap_ratio"),
        "cvd_slope": tracker.get("cvd_slope"),
        "rvol": tracker.get("rvol"),
        "entry_at": tracker.get("entry_at", tracker.get("started_at")),
        "entry_ts": tracker.get("entry_ts"),
        "observation_secs": round(max(0.0, time.time() - float(tracker.get("entry_ts", time.time()))), 2),
        "price_updates": int(tracker.get("price_updates", 0)),
        "entry_kraken_gap_ratio": tracker.get("entry_kraken_gap_ratio", tracker.get("kraken_gap_ratio")),
        "entry_cvd_slope": tracker.get("entry_cvd_slope", tracker.get("cvd_slope")),
        "entry_rvol": tracker.get("entry_rvol", tracker.get("rvol")),
        "status": status or tracker.get("status", "TRACKING"),
        "finish_reason": tracker.get("finish_reason"),
        "milestone": f"{milestone}x" if isinstance(milestone, int) else str(milestone),
    }
    pump_log.insert(0, event)
    if len(pump_log) > 500:
        pump_log.pop()


def _finish_pump_tracker(key, tracker, reason="END"):
    """Record the final pump tracker result and remove it from active tracking."""
    window_start = tracker.get("window_start")
    if _pump_tracker_already_finished(window_start, key):
        pump_tracker.pop(key, None)
        return
    tracker["status"] = _pump_result_from_tracker(tracker)
    tracker["finish_reason"] = reason
    pump_finished_tracker_keys.add(_pump_finished_id(window_start, key))
    _record_pump_event(key, tracker, reason, tracker["status"])
    log.info(
        "[PUMP-%s] %s now=%.3fx max=%.3fx current=%.4f max_px=%.4f",
        tracker["status"], key, float(tracker.get("multiple", 0.0)),
        float(tracker.get("max_multiple", 0.0)), float(tracker.get("current", 0.0)),
        float(tracker.get("max_price", 0.0)),
    )
    pump_tracker.pop(key, None)


def _finish_window_pump_trackers(window_start):
    """Finalize pump traces at the market-window boundary using the latest price."""
    for key, tracker in list(pump_tracker.items()):
        if tracker.get("window_start") != window_start:
            continue
        _refresh_pump_tracker_price(key, tracker)
        _finish_pump_tracker(key, tracker, "FULL-WINDOW")


def _refresh_pump_tracker_price(key, tracker):
    price = live_prices.get(key)
    if price is not None and price > 0:
        tracker["price_updates"] = int(tracker.get("price_updates", 0)) + 1
        tracker["current"] = price
        if price < float(tracker.get("trough", price)):
            # A lower trough becomes the new possible entry. Re-snapshot its early-window
            # conditions so any later peak multiple is matched to the conditions at its base.
            entry_snapshot = _get_pump_kraken_snapshot(tracker.get("asset"))
            tracker["trough"] = price
            tracker["base_price"] = price
            tracker["max_price"] = price
            tracker["max_multiple"] = 1.0
            tracker["highest_milestone"] = 1
            tracker["entry_at"] = datetime.now().strftime("%H:%M:%S")
            tracker["entry_ts"] = time.time()
            tracker["price_updates"] = 1
            tracker["entry_kraken_gap_ratio"] = entry_snapshot.get("kraken_gap_ratio")
            tracker["entry_cvd_slope"] = entry_snapshot.get("cvd_slope")
            tracker["entry_rvol"] = entry_snapshot.get("rvol")
        trough = float(tracker.get("trough", 0.0))
        multiple = price / trough if trough > 0 else 0.0
        tracker["multiple"] = multiple
        if price > float(tracker.get("max_price", 0.0)):
            tracker["max_price"] = price
        if multiple > float(tracker.get("max_multiple", 0.0)):
            tracker["max_multiple"] = multiple
    _update_pump_kraken_snapshot(tracker)


def update_pump_trackers(window_start, secs_into):
    """Track YES/NO pump opportunities for the complete 900-second market window."""
    for asset in ASSETS:
        for side in ("yes", "no"):
            key = f"{asset}_{side}"
            price = live_prices.get(key)
            if price is None or price <= 0:
                continue

            tracker = pump_tracker.get(key)
            if tracker and tracker.get("window_start") != window_start:
                # Do not refresh stale trackers with a new window's live price.
                _finish_pump_tracker(key, tracker, "END")
                tracker = None
            # The dead zone is outside the pump-tracking universe. Finalize an
            # active tracker immediately and do not start or restart it this window.
            if price < PUMP_TRACK_DEAD_ZONE_PRICE:
                if tracker:
                    tracker["price_updates"] = int(tracker.get("price_updates", 0)) + 1
                    tracker["current"] = price
                    trough = float(tracker.get("trough", 0.0))
                    tracker["multiple"] = price / trough if trough > 0 else 0.0
                    _update_pump_kraken_snapshot(tracker)
                    _finish_pump_tracker(key, tracker, "DEAD-ZONE")
                continue

            if tracker is None:
                if _pump_tracker_already_finished(window_start, key):
                    continue
                if PUMP_TRACK_DEAD_ZONE_PRICE <= price < PUMP_TRACK_START_PRICE:
                    entry_snapshot = _get_pump_kraken_snapshot(asset)
                    pump_tracker[key] = {
                        "asset": asset,
                        "side": side,
                        "window_start": window_start,
                        "started_at": datetime.now().strftime("%H:%M"),
                        "base_price": price,
                        "trough": price,
                        "current": price,
                        "multiple": 1.0,
                        "max_price": price,
                        "max_multiple": 1.0,
                        "status": "TRACKING",
                        "highest_milestone": 1,
                        "entry_at": datetime.now().strftime("%H:%M:%S"),
                        "entry_ts": time.time(),
                        "price_updates": 1,
                        "entry_kraken_gap_ratio": entry_snapshot.get("kraken_gap_ratio"),
                        "entry_cvd_slope": entry_snapshot.get("cvd_slope"),
                        "entry_rvol": entry_snapshot.get("rvol"),
                        **entry_snapshot,
                    }
                continue

            _refresh_pump_tracker_price(key, tracker)

            multiple = float(tracker.get("multiple", 0.0))
            trough = float(tracker.get("trough", 0.0))
            max_whole_multiple = int(math.floor(multiple))
            first_milestone = min(PUMP_TRACK_MILESTONES)
            highest = int(tracker.get("highest_milestone", 1))
            if max_whole_multiple >= first_milestone and max_whole_multiple > highest:
                for milestone in range(max(first_milestone, highest + 1), max_whole_multiple + 1):
                    _record_pump_event(key, tracker, milestone)
                    log.debug(
                        "[PUMP-%dX] %s base=%.4f current=%.4f multiple=%.3fx",
                        milestone, key, trough, price, multiple,
                    )
                tracker["highest_milestone"] = max_whole_multiple

def _record_oppo_trigger(asset, side, price, status, reason):
    # GOLDEN setup and gap-block conditions can remain true for many scan polls.
    # Keep the dashboard useful by showing only their first event per market window.
    once_key = (asset.lower(), side.lower(), status)
    if status in {"GOLDEN", "GOLDEN-GAP-BLOCK"}:
        if once_key in oppo_dashboard_once_per_window:
            return
        oppo_dashboard_once_per_window.add(once_key)

    oppo_trigger_log.insert(0, {
        "time": datetime.now().strftime("%H:%M:%S"),
        "asset": asset.upper(),
        "side": side.upper(),
        "price": round(float(price), 4) if price is not None else None,
        "status": status,
        "reason": reason,
    })


def _clear_oppo_tracking_for_asset(asset):
    """Remove all per-window OPPO trackers for an asset."""
    prefixes = (f"{asset}_yes", f"{asset}_no")
    for key in list(oppo_rebound_tracker.keys()):
        if key.startswith(prefixes):
            del oppo_rebound_tracker[key]
    for key in list(oppo_counter_tracker.keys()):
        tracker_asset = oppo_counter_tracker[key].get("asset") if isinstance(oppo_counter_tracker[key], dict) else None
        if tracker_asset == asset or key.startswith(prefixes):
            del oppo_counter_tracker[key]
    for key in list(oppo_cvd_polls.keys()):
        if key.startswith(prefixes):
            del oppo_cvd_polls[key]


def blacklist_oppo_dead_zone_asset(asset, side, price):
    """Blacklist an asset for the current window after an OPPO dead-zone touch."""
    first_dead_zone_hit = asset not in oppo_dead_zone_blacklisted_assets
    normal_blacklisted_assets.add(asset)
    oppo_dead_zone_blacklisted_assets.add(asset)
    _clear_oppo_tracking_for_asset(asset)
    if not first_dead_zone_hit:
        return

    opp_key = f"{asset}_{side}"
    detail = f"<= {OPPO_DEAD_ZONE:.4f}; asset blacklisted this window"
    record_oppo_trigger(opp_key, asset, side, price, "DEAD-ZONE", detail)
    _record_oppo_trigger(asset, side, price, "SKIPPED", "dead-zone-blacklisted")
    log.info(
        "[OPPO-DEAD-ZONE] %s price=%.4f <= %.4f — blacklisted asset for current window and cleared OPPO tracking",
        opp_key, price, OPPO_DEAD_ZONE,
    )


def _oppo_golden_rvol_setup(asset, side):
    """Return an armed fourth-window reversal setup for the requested OPPO side."""
    snapshot = get_rvol_reversal_snapshot(
        asset, VOLUME_AVG_PERIOD, OPPO_GOLDEN_RVOL_LOOKBACK,
        OPPO_GOLDEN_RVOL_MIN_HIGH, OPPO_GOLDEN_RVOL_THRESHOLD,
    )
    probability = snapshot.get("probability")
    probability_ok = (
        snapshot.get("samples", 0) >= OPPO_GOLDEN_MIN_SAMPLES
        and (probability is None or probability >= OPPO_GOLDEN_MIN_PROBABILITY)
    )
    snapshot["qualified"] = bool(
        OPPO_GOLDEN_RVOL_ENABLED and snapshot.get("armed")
        and snapshot.get("side") == side and probability_ok
    )
    return snapshot


def _oppo_rvol_guard_ok(asset, side, price, secs_into):
    """Final OPPO buy gate: select normal or flexi size from minute-scaled Kraken RVOL."""
    minute = get_rvol_minute(secs_into)
    rvol_min = get_rvol_min(secs_into=secs_into)
    vol = get_volume_snapshot(asset, VOLUME_AVG_PERIOD, rvol_min)
    if not OPPO_RVOL_GUARD_ENABLED:
        return True, vol, BUY_AMOUNT

    rvol = vol.get("rvol")
    avg = vol.get("average")
    current = vol.get("current")
    opp_key = f"{asset}_{side}"
    if rvol is None:
        detail = f"not-ready; needs {VOLUME_AVG_PERIOD} candles"
        record_oppo_trigger(opp_key, asset, side, price, "RVOL-WAIT", detail)
        _record_oppo_trigger(asset, side, price, "SKIPPED", "rvol-not-ready")
        log.info(
            "[OPPO-RVOL-WAIT] %s minute=%d rvol=not-ready threshold>%.3fx",
            opp_key, minute, rvol_min,
        )
        return False, vol, None

    rvol = float(rvol)
    rvol_confirmed = rvol > rvol_min
    if rvol_confirmed:
        log.info(
            "[OPPO-RVOL-PASS] %s_%s minute=%d rvol=%.3fx > %.3fx  volume=%.2f avg=%.2f order=$%.2f",
            asset.upper(), side.upper(), minute, rvol, rvol_min, float(current or 0.0), float(avg or 0.0), BUY_AMOUNT,
        )
        return True, vol, BUY_AMOUNT

    if not FLEXI_RVOL_ENABLED:
        detail = f"{rvol:.3f}x <= {rvol_min:.3f}x (minute {minute}); flexi disabled"
        record_oppo_trigger(opp_key, asset, side, price, "RVOL-BLOCK", detail)
        _record_oppo_trigger(asset, side, price, "RVOL-BLOCK", detail)
        log.info(
            "[OPPO-RVOL-BLOCK] %s minute=%d rvol=%.3fx threshold>%.3fx — flexi disabled",
            opp_key, minute, rvol, rvol_min,
        )
        return False, vol, None

    detail = f"{rvol:.3f}x <= {rvol_min:.3f}x; using ${FLEXI_RVOL_BUY_AMOUNT:g} order"
    record_oppo_trigger(opp_key, asset, side, price, "RVOL-FLEXI", detail)
    _record_oppo_trigger(asset, side, price, "RVOL-FLEXI", detail)
    log.info(
        "[OPPO-RVOL-FLEXI] %s minute=%d rvol=%.3fx <= %.3fx  volume=%.2f avg=%.2f order=$%.2f",
        opp_key, minute, rvol, rvol_min, float(current or 0.0), float(avg or 0.0), FLEXI_RVOL_BUY_AMOUNT,
    )
    return True, vol, FLEXI_RVOL_BUY_AMOUNT


def _oppo_pump_range(opp_key, window_start):
    tracker = pump_tracker.get(opp_key)
    trough = float(tracker.get("trough", 0.0) or 0.0) if isinstance(tracker, dict) else 0.0
    peak = float(tracker.get("max_price", 0.0) or 0.0) if isinstance(tracker, dict) else 0.0
    asset, side = opp_key.split("_", 1)
    for event in pump_log:
        if event.get("window_start") != window_start:
            continue
        if str(event.get("asset", "")).lower() != asset or str(event.get("side", "")).lower() != side:
            continue
        event_trough = float(event.get("trough", 0.0) or 0.0)
        event_peak = max(float(event.get("max_price", 0.0) or 0.0), float(event.get("current", 0.0) or 0.0))
        trough = event_trough if trough <= 0 else min(trough, event_trough)
        peak = max(peak, event_peak)
        break
    return trough, peak


def _oppo_falling_knife_blocked(opp_key, window_start, price):
    trough, peak = _oppo_pump_range(opp_key, window_start)
    if trough <= 0 or peak <= 0 or price <= 0:
        return False, trough, peak, 0.0, 0.0
    pump_move = peak - trough
    drop = peak - price
    blocked = pump_move >= OPPO_FALLING_KNIFE_MIN_MOVE and drop >= OPPO_FALLING_KNIFE_MIN_MOVE
    min_ok_price = peak - OPPO_FALLING_KNIFE_MIN_MOVE
    return blocked, trough, peak, drop, min_ok_price
    
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


def get_rvol_minute(secs_into=None):
    secs = max(0, int(secs_into if secs_into is not None else 0))
    return max(1, min(15, (secs // 60) + 1))


def get_rvol_min(secs_into=None):
    return RVOL_MIN_PER_MIN * get_rvol_minute(secs_into)


def get_kraken_gap(asset):
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)
    if c_open <= 0.0 or c_live is None:
        return None
    return abs(c_live - c_open)


def force_sell_gap_triggered(asset, secs_into):
    threshold = get_gap_threshold(asset, secs_into, FORCE_SELL_GAP_MULT)
    actual = get_kraken_gap(asset)
    if threshold is None or actual is None:
        return False, actual, threshold
    return actual >= threshold, actual, threshold

# ── Position management ───────────────────────────────────────────────────────

def open_position(key, token_id, entry_price, filled_shares=None, window_start=None,
                  is_flip=False, is_rebound=False, buy_amount=None, is_simulated=False,
                  entry_rvol=None, entry_kraken_gap_ratio=None, entry_rebound_ratio=None,
                  entry_cvd_slope=None, is_golden_oppo=False):
    amount = buy_amount if buy_amount is not None else BUY_AMOUNT
    if filled_shares is not None and filled_shares > 0:
        net_shares = round(float(filled_shares), 3)
    else:
        gross_shares = amount / entry_price if entry_price > 0 else 0.0
        fee_shares = gross_shares * CRYPTO_TAKER_FEE_RATE * (1 - entry_price)
        net_shares = round(max(gross_shares - fee_shares, 0.0), 3)
    is_oppo = key.endswith("_oppo")
    is_counter = key.endswith("_counter")
    sell_mult = OPPO_SELL_MULTIPLIER if is_oppo else (OPPO_COUNTER_SELL_MULTIPLIER if is_counter else (REBOUND_SELL_MULTIPLIER if is_rebound else SELL_MULTIPLIER))
    sell_cap = OPPO_SELL_CAP if is_oppo else (OPPO_COUNTER_SELL_CAP if is_counter else SELL_CAP)
    if is_rebound:
        rebound_5x_target = min(round(entry_price * REBOUND_SELL_MULTIPLIER, 4), REBOUND_MAX_TARGET_PRICE)
        sell_price = rebound_5x_target
    elif is_oppo:
        sell_price = min(round(entry_price * OPPO_FIRST_SELL_MULTIPLIER, 4), OPPO_SELL_CAP)
    else:
        sell_price = min(round(entry_price * sell_mult, 4), sell_cap)
    cut_loss_pct = OPPO_CUT_LOSS_PCT if is_oppo else (OPPO_COUNTER_CUT_LOSS_PCT if is_counter else CUT_LOSS_PCT)
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
        "is_counter":           is_counter,
        "rebound_tranches":     rebound_tranches,
        "oppo_tranches":        oppo_tranches,
        "oppo_tp2_peak":        0.0,
        "force_stop_triggered": None,
        "force_stop_cooldown":  None,
        "force_stop_spread_retries": 0,
        "last_exit_attempt_ts": 0.0,
        "breakeven_armed":      False,
        "breakeven_gap_polls":  0,
        "opened_at":            datetime.now().strftime("%H:%M"),
        "opened_ts":            time.time(),
        "window_start":         window_start,
        "is_simulated":         is_simulated,
        "entry_rvol":           round(float(entry_rvol), 3) if entry_rvol is not None else None,
        "entry_kraken_gap_ratio":   round(float(entry_kraken_gap_ratio), 3) if entry_kraken_gap_ratio is not None else None,
        "entry_rebound_ratio":   round(float(entry_rebound_ratio), 3) if entry_rebound_ratio is not None else None,
        "entry_cvd_slope":       round(float(entry_cvd_slope), 6) if entry_cvd_slope is not None else None,
        "is_golden_oppo":        bool(is_golden_oppo),
    }
    base_asset = key.split("_")[0]
    last_entry_ts[base_asset] = time.time()
    stats["buys"] += 1
    tag = "COUNTER " if is_counter else ("REBOUND FLIP " if is_rebound else ("FLIP " if is_flip else ""))
    log.info(
        "[OPEN] %s%s  entry=%.4f  shares=%.3f  sell=%.4f  cut-loss=%.4f (%.0f%%)",
        tag, key, entry_price, net_shares, sell_price, cut_loss_price, cut_loss_pct * 100,
    )
    if is_rebound:
        log.info(
            "[REBOUND-TARGET] %s  100%% @ %.4f",
            key, rebound_tranches[0]["target"],
        )
    if is_counter:
        log.info(
            "[OPPO-COUNTER-TARGET] %s  sell @ %.4f (x%.2f cap %.4f)  cut-loss=%.4f",
            key, sell_price, OPPO_COUNTER_SELL_MULTIPLIER, OPPO_COUNTER_SELL_CAP, cut_loss_price,
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


def _oppo_tp1_sold(pos):
    return any(t.get("name") == "2X" and t.get("sold") for t in pos.get("oppo_tranches", []))


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
        if tranche.get("name") == "5X" and not pos.get("oppo_counter_armed"):
            parts = key.split("_")
            if len(parts) >= 2:
                _arm_oppo_counter(parts[0], parts[1], current_price, pos.get("window_start"), "oppo-tp2-target")
                pos["oppo_counter_armed"] = True
        log.info(
            "[OPPO-SELL-%s] %s partial finalized revenue=$%.4f remaining=%.3f",
            tranche.get("name", "TRANCHE"), key, revenue, pos["net_shares"],
        )
        parts = key.split("_")
        if len(parts) >= 2:
            _record_oppo_trigger(parts[0], parts[1], current_price, "SELL", f"{tranche.get('name', 'TRANCHE').lower()}-filled")

    pos["closing"] = False

    first_tranche_sold = any(t.get("name") == "2X" and t.get("sold") for t in pos.get("oppo_tranches", []))
    final_tranche = next((t for t in pos.get("oppo_tranches", []) if t.get("name") == "5X"), None)
    if first_tranche_sold and final_tranche and not final_tranche.get("sold"):
        prev_peak = float(pos.get("oppo_tp2_peak", 0.0) or 0.0)
        if current_price > prev_peak:
            pos["oppo_tp2_peak"] = current_price
        peak = float(pos.get("oppo_tp2_peak", 0.0) or 0.0)
        trail_stop = peak * (1.0 - OPPO_TP2_TRAIL_PCT) if peak > 0 else 0.0
        if peak > 0 and current_price <= trail_stop:
            available_shares = round(float(pos.get("net_shares", 0.0)), 3)
            final_shares = round(float(final_tranche.get("shares", 0.0)), 3)
            sell_shares = min(available_shares, final_shares)
            if sell_shares >= MIN_SELL_SHARES:
                log.info("[OPPO-TP2-TRAIL] %s peak=%.4f stop=%.4f price=%.4f selling %.3f shares", key, peak, trail_stop, current_price, sell_shares)
                pos["closing"] = True
                sell = market_sell_with_retries(
                    client, pos["token_id"], sell_shares, current_price, key.upper(),
                    simulate=pos.get("is_simulated", False),
                )
                pos["last_exit_attempt_ts"] = time.time()
                if not sell["ok"]:
                    pos["closing"] = False
                    log.warning("[OPPO-TP2-TRAIL] %s sell failed — will retry on next loop", key)
                    return False
                filled_shares = round(float(sell.get("filled_shares") or sell_shares), 3)
                revenue = float(sell.get("filled_quote") or round(filled_shares * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
                pos["net_shares"] = round(max(available_shares - filled_shares, 0.0), 3)
                final_tranche["sold"] = True
                parts = key.split("_")
                if len(parts) >= 2:
                    if not pos.get("oppo_counter_armed"):
                        _arm_oppo_counter(parts[0], parts[1], current_price, pos.get("window_start"), "oppo-tp2-trail")
                        pos["oppo_counter_armed"] = True
                    _record_oppo_trigger(parts[0], parts[1], current_price, "SELL", f"tp2-trail peak={peak:.4f}")
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

        gap_hit, actual_gap, force_threshold = force_sell_gap_triggered(key.split("_")[0], secs_into_now)
        # ── OPPO-only breakeven arm/exit before TP1; after TP1, hold for TP2/trail.
        if pos.get("is_oppo"):
            if _oppo_tp1_sold(pos):
                if pos.get("breakeven_armed") or pos.get("breakeven_gap_polls", 0):
                    log.info("[BREAKEVEN-DISABLED] %s TP1 already sold — skipping breakeven for remaining OPPO shares", key)
                pos["breakeven_armed"] = False
                pos["breakeven_gap_polls"] = 0
            else:
                base_threshold = get_gap_threshold(key.split("_")[0], secs_into_now)
                if base_threshold is not None and base_threshold > 0 and actual_gap is not None:
                    breakeven_gap_hit = actual_gap >= (base_threshold * BREAKEVEN_GAP_MULT)
                    if breakeven_gap_hit:
                        pos["breakeven_gap_polls"] = int(pos.get("breakeven_gap_polls", 0)) + 1
                    else:
                        pos["breakeven_gap_polls"] = 0

                    if (not pos.get("breakeven_armed")) and pos.get("breakeven_gap_polls", 0) >= BREAKEVEN_POLL_CONFIRMATIONS:
                        pos["breakeven_armed"] = True
                        pos["sell_price"] = max(pos.get("sell_price", entry), entry)
                        for t in pos.get("oppo_tranches", []):
                            if not t.get("sold"):
                                t["target"] = max(float(t.get("target", entry)), entry)
                        log.info("[BREAKEVEN-ARM] %s gap=%.4f >= %.2fx threshold(%.4f) for %d polls — arm entry exit at %.4f", key, actual_gap, BREAKEVEN_GAP_MULT, base_threshold, BREAKEVEN_POLL_CONFIRMATIONS, entry)

                    if pos.get("breakeven_armed") and current_price >= entry:
                        log.info("[BREAKEVEN-SELL] %s price=%.4f >= entry=%.4f  selling %.3f shares", key, current_price, entry, shares)
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
                            log.info("[BREAKEVEN-SELL] %s finalized  pnl=$%.4f", key, pnl)
                            stats["wins" if pnl > 0 else "losses"] += 1
                            stats["pnl"] += pnl
                            _record_closed_trade(key, pnl)
                            _record_trade_log(key, pos, "BREAKEVEN-SELL", current_price, pnl)
                            to_close.append(key)
                        else:
                            pos["closing"] = False
                            log.warning("[BREAKEVEN-SELL] %s sell failed — will retry on next loop", key)
                        continue

        # ── Force sell profitable positions when Kraken gap overextends ─────
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
                if pos.get("is_oppo") or key.endswith("_oppo") or pos.get("is_counter") or key.endswith("_counter"):
                    parts = key.split("_")
                    if len(parts) >= 2:
                        status = "COUNTER-CUT-LOSS" if (pos.get("is_counter") or key.endswith("_counter")) else "CUT-LOSS"
                        _record_oppo_trigger(parts[0], parts[1], current_price, status, f"pnl={pnl:+.4f}")
                to_close.append(key)

                # ── Rebound cut-loss flip: trace opposite side trough, then buy opposite side ──
                asset = key.split("_")[0]
                side = key.split("_")[1]
                flip_side = "no" if side == "yes" else "yes"
                flip_key = f"{asset}_{flip_side}"
                flip_token = get_token_for_key(asset, flip_side, pos.get("window_start"))
                if asset not in flipped_this_window and not pos.get("is_oppo") and not pos.get("is_counter") and flip_token:
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
            tag = "COUNTER-SELL" if pos.get("is_counter") else ("FLIP-SELL" if is_flip else "SELL")
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
                exit_type = "COUNTER-SELL" if pos.get("is_counter") else ("FLIP-SELL" if is_flip else "SELL")
                log.info("[%s] %s finalized  pnl=$%.4f", exit_type, key, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                _record_trade_log(key, pos, exit_type, current_price, pnl)
                if pos.get("is_counter") or key.endswith("_counter"):
                    parts = key.split("_")
                    if len(parts) >= 2:
                        _record_oppo_trigger(parts[0], parts[1], current_price, "COUNTER-SELL", f"pnl={pnl:+.4f}")
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



def advance_oppo_counter_tracker(client, window_start, secs_into):
    if not OPPO_COUNTER_ENABLED:
        oppo_counter_tracker.clear()
        return

    for key in list(oppo_counter_tracker.keys()):
        tracker = oppo_counter_tracker[key]
        asset = tracker.get("asset") or key.split("_")[0]
        side = tracker.get("side") or key.split("_")[1]
        counter_position_key = _counter_key(asset, side)

        if tracker.get("window_start") != window_start:
            log.info("[OPPO-COUNTER-DISCARD] %s stale window", key)
            del oppo_counter_tracker[key]
            continue
        if secs_into < ENTRY_AFTER:
            log.debug("[OPPO-COUNTER-WAIT] %s secs_into=%d < entry_after=%d", key, secs_into, ENTRY_AFTER)
            continue
        if secs_into > STOP_BUY_AT:
            log.info(
                "[OPPO-COUNTER-DISCARD] %s secs_into=%d > stop_buy_at=%d",
                key, secs_into, STOP_BUY_AT,
            )
            del oppo_counter_tracker[key]
            continue
        if counter_position_key in open_positions:
            log.info("[OPPO-COUNTER-SKIP] %s already open", counter_position_key)
            del oppo_counter_tracker[key]
            continue
        if asset in traded_this_window:
            log.info("[OPPO-COUNTER-SKIP] %s asset already traded this window", key)
            del oppo_counter_tracker[key]
            continue
        if asset in oppo_rvol_blacklisted_assets:
            log.info("[OPPO-COUNTER-SKIP] %s asset RVOL-blacklisted this window", key)
            del oppo_counter_tracker[key]
            continue
        if asset in oppo_knife_blacklisted_assets:
            log.info("[OPPO-COUNTER-SKIP] %s asset knife-blocked this window", key)
            del oppo_counter_tracker[key]
            continue

        token = tracker.get("token") or get_token_for_key(asset, side, window_start)
        if not token:
            log.debug("[OPPO-COUNTER-WAIT] %s token not loaded yet", key)
            continue
        tracker["token"] = token

        price = live_prices.get(key)
        if price is None or price <= 0:
            price = get_midpoint(client, token)
        if price is None or price <= 0:
            continue

        if price < OPPO_COUNTER_MIN_PRICE:
            log.info(
                "[OPPO-COUNTER-WAIT] %s price=%.4f below %.4f",
                key, price, OPPO_COUNTER_MIN_PRICE,
            )
            continue
        if price > OPPO_COUNTER_MAX_PRICE:
            log.info(
                "[OPPO-COUNTER-WAIT] %s price=%.4f above %.4f",
                key, price, OPPO_COUNTER_MAX_PRICE,
            )
            continue

        spread = get_spread_value(client, token)
        if spread is not None and spread > MAX_BOOK_SPREAD:
            log.info(
                "[OPPO-COUNTER-SPREAD] %s spread=%.4f > %.4f",
                key, spread, MAX_BOOK_SPREAD,
            )
            continue

        label = f"{asset.upper()}-{side.upper()}-OPPO-COUNTER"
        log.info(
            "[OPPO-COUNTER-BUY] %s buying @ %.4f in %.4f–%.4f range",
            counter_position_key, price, OPPO_COUNTER_MIN_PRICE, OPPO_COUNTER_MAX_PRICE,
        )
        buy = market_buy(
            client, token, label,
            price_hint=price,
            amount=OPPO_COUNTER_BUY_AMOUNT,
        )
        if buy["ok"]:
            entry_px = float(buy.get("filled_price") or price)
            open_position(
                counter_position_key, token, entry_px,
                filled_shares=buy.get("filled_shares"),
                window_start=window_start,
                buy_amount=OPPO_COUNTER_BUY_AMOUNT,
                is_simulated=bool((buy.get("resp") or {}).get("simulated")),
            )
            traded_this_window.add(asset)
            del oppo_counter_tracker[key]
            _record_oppo_trigger(asset, side, price, "COUNTER-BOUGHT", "entry-filled")
        else:
            _record_oppo_trigger(asset, side, price, "COUNTER-FAIL", "order rejected")



def scan_markets(client, window_start, secs_into, server_ts, executor):
    global _skip_first_window, _startup_window_ts

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
    update_pump_trackers(window_start, secs_into)

    if not can_open_new_trades(server_ts):
        return

    advance_rebound_cutloss_tracker(client, window_start, secs_into)

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

    advance_oppo_counter_tracker(client, window_start, secs_into)

    if OPPO_MODE_ENABLED and secs_into >= OPPO_WINDOW_START_SEC:
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
                opp_key = f"{asset}_{side}"
                is_initial_oppo_zone = price <= OPPO_DEAD_ZONE or OPPO_MIN_PRICE <= price <= OPPO_MAX_PRICE
                is_tracked_rebound_zone = opp_key in oppo_rebound_tracker
                if is_initial_oppo_zone or is_tracked_rebound_zone:
                    low_assets.append((asset, price, token))
            if not low_assets:
                continue

            for opp_asset, opp_price, opp_token in low_assets:
                opp_key = f"{opp_asset}_{side}"
                golden_setup = _oppo_golden_rvol_setup(opp_asset, side)
                golden_opportunity = golden_setup.get("qualified", False)

                if opp_asset in traded_this_window:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "SKIP", "asset already traded this window")
                    continue

                if (
                    opp_asset in oppo_dead_zone_blacklisted_assets
                    or (not golden_opportunity and opp_asset in oppo_rvol_blacklisted_assets)
                    or (not golden_opportunity and opp_asset in oppo_knife_blacklisted_assets)
                ):
                    _clear_oppo_tracking_for_asset(opp_asset)
                    continue

                if opp_price <= OPPO_DEAD_ZONE:
                    blacklist_oppo_dead_zone_asset(opp_asset, side, opp_price)
                    continue

                trough = oppo_rebound_tracker.get(opp_key)
                if trough is not None and opp_price > OPPO_REBOUND_MAX_PRICE:
                    record_oppo_trigger(
                        opp_key, opp_asset, side, opp_price, "REBOUND-CAP",
                        f"price {opp_price:.4f} > rebound max {OPPO_REBOUND_MAX_PRICE:.4f}",
                    )
                    _record_oppo_trigger(opp_asset, side, opp_price, "SKIPPED", "oppo-rebound-price-too-high")
                    continue
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
                    log.debug("[OPPO-WAIT] %s waiting %.3fx/%.2fx (price=%.4f trough=%.4f need>=%.4f)",
                             opp_key, rebound_ratio, OPPO_REBOUND_MULT, opp_price, trough, trough * OPPO_REBOUND_MULT)
                    _record_oppo_trigger(opp_asset, side, opp_price, "TRACKING", f"rebound {rebound_ratio:.2f}x")
                    continue

                falling_knife, pump_trough, pump_peak, drop, min_ok_price = _oppo_falling_knife_blocked(opp_key, window_start, opp_price)
                if falling_knife and not golden_opportunity:
                    pump_move = pump_peak - pump_trough
                    detail = (
                        f"pump +{pump_move:.4f} then drop -{drop:.4f} from peak {pump_peak:.4f}; "
                        "asset blacklisted this window"
                    )
                    normal_blacklisted_assets.add(opp_asset)
                    oppo_knife_blacklisted_assets.add(opp_asset)
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "KNIFE-BLOCK", detail)
                    log.info(
                        "[OPPO-KNIFE-BLOCK] %s price=%.4f trough=%.4f peak=%.4f pump=+%.4f drop=-%.4f >= %.4f — asset blacklisted this window",
                        opp_key, opp_price, pump_trough, pump_peak, pump_move, drop, OPPO_FALLING_KNIFE_MIN_MOVE,
                    )
                    _record_oppo_trigger(opp_asset, side, opp_price, "KNIFE-BLOCK", detail)
                    _clear_oppo_tracking_for_asset(opp_asset)
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
                actual_gap = abs(c_live - c_open) if c_open > 0 and c_live is not None else None
                if actual_gap is not None:
                    gap_magnitude = OPPO_GOLDEN_GAP_MAG if golden_opportunity else OPPO_GAP_MAG
                    oppo_gap_threshold = c_open * GAP_SWING.get(opp_asset, 0.001) * gap_magnitude
                    if actual_gap >= oppo_gap_threshold:
                        status = "GOLDEN-GAP-BLOCK" if golden_opportunity else "GAP-BLOCK"
                        detail = f"{actual_gap:.4f}>={oppo_gap_threshold:.4f} mag={gap_magnitude:.2f}x"
                        record_oppo_trigger(opp_key, opp_asset, side, opp_price, status, detail)
                        log.info(
                            "[OPPO-%s] %s_%s actual_gap=%.4f >= threshold=%.4f magnitude=%.2fx",
                            status, opp_asset.upper(), side.upper(), actual_gap, oppo_gap_threshold, gap_magnitude,
                        )
                        _record_oppo_trigger(opp_asset, side, opp_price, status, detail)
                        continue

                if CVD_OPPO_ENABLED and not golden_opportunity:
                    _, cvd_window, cvd_slope = get_cvd_snapshot(opp_asset)
                    cvd_key = opp_key
                    slope_ok = (cvd_slope > 0) if side == "yes" else (cvd_slope < 0)
                    if slope_ok:
                        oppo_cvd_polls[cvd_key] = int(oppo_cvd_polls.get(cvd_key, 0)) + 1
                    else:
                        oppo_cvd_polls[cvd_key] = 0
                    if oppo_cvd_polls.get(cvd_key, 0) < CVD_OPPO_SLOPE_POLLS:
                        record_oppo_trigger(opp_key, opp_asset, side, opp_price, "CVD-WAIT", f"polls {oppo_cvd_polls.get(cvd_key,0)}/{CVD_OPPO_SLOPE_POLLS} slope={cvd_slope:.6f} win={cvd_window:.2f}")
                        _record_oppo_trigger(opp_asset, side, opp_price, "SKIPPED", "cvd-not-confirmed")
                        continue
                    log.info("[OPPO-CVD-PASS] %s_%s polls=%d/%d slope=%.6f win=%.2f", opp_asset.upper(), side.upper(), oppo_cvd_polls.get(cvd_key,0), CVD_OPPO_SLOPE_POLLS, cvd_slope, cvd_window)

                if golden_opportunity:
                    rvol_snapshot = get_volume_snapshot(opp_asset, VOLUME_AVG_PERIOD, get_rvol_min(secs_into))
                    oppo_buy_amount = BUY_AMOUNT
                    probability = golden_setup.get("probability")
                    probability_text = f"{probability:.1%}" if probability is not None else "n/a"
                    prior_rvols = ",".join(f"{value:.2f}" for value in golden_setup.get("rvols", []) if value is not None)
                    log.info(
                        "[OPPO-GOLDEN] %s_%s fourth-window reversal armed: high-rvol=%d/%d prior=[%s] historical=%s (%d/%d) — golden gap passed; bypassing knife/CVD/current-RVOL guards",
                        opp_asset.upper(), side.upper(), golden_setup.get("high_rvol_count", 0),
                        OPPO_GOLDEN_RVOL_LOOKBACK, prior_rvols, probability_text,
                        golden_setup.get("wins", 0), golden_setup.get("samples", 0),
                    )
                    golden_detail = (
                        f"{golden_setup.get('high_rvol_count', 0)}/{OPPO_GOLDEN_RVOL_LOOKBACK} high RVOL; "
                        f"historical reversal={probability_text} ({golden_setup.get('wins', 0)}/{golden_setup.get('samples', 0)})"
                    )
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "GOLDEN", golden_detail)
                    _record_oppo_trigger(opp_asset, side, opp_price, "GOLDEN", golden_detail)
                else:
                    rvol_ok, rvol_snapshot, oppo_buy_amount = _oppo_rvol_guard_ok(opp_asset, side, opp_price, secs_into)
                    if not rvol_ok:
                        continue
                entry_rvol = rvol_snapshot.get("rvol") if rvol_snapshot else None

                base_gap = c_open * GAP_SWING.get(opp_asset, 0.001) if c_open > 0 else None
                entry_kraken_gap_ratio = actual_gap / base_gap if base_gap and base_gap > 0 else None
                _, _, entry_cvd_slope = get_cvd_snapshot(opp_asset)
                label = f"{opp_asset.upper()}-{side.upper()}-OPPO"
                buy = market_buy(client, opp_token, label, price_hint=opp_price, amount=oppo_buy_amount)
                if buy["ok"]:
                    entry_px = float(buy.get("filled_price") or opp_price)
                    open_position(f"{opp_asset}_{side}_oppo", opp_token, entry_px,
                                  filled_shares=buy.get("filled_shares"),
                                  window_start=window_start,
                                  is_simulated=bool((buy.get("resp") or {}).get("simulated")),
                                  buy_amount=oppo_buy_amount,
                                  entry_rvol=entry_rvol,
                                  entry_kraken_gap_ratio=entry_kraken_gap_ratio,
                                  entry_rebound_ratio=rebound_ratio,
                                  entry_cvd_slope=entry_cvd_slope,
                                  is_golden_oppo=golden_opportunity)
                    traded_this_window.add(opp_asset)
                    oppo_rebound_tracker.pop(opp_key, None)
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "BOUGHT", "success")
                    _record_oppo_trigger(opp_asset, side, opp_price, "BOUGHT", "entry-filled")
                    log.info("[OPPO-BUY] %s_%s triggered oppo setup — continuing to scan other assets", opp_asset.upper(), side.upper())
                    continue
                else:
                    record_oppo_trigger(opp_key, opp_asset, side, opp_price, "BUY-FAIL", "order rejected")
                    continue

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
            "is_counter": p.get("is_counter", k.endswith("_counter")),
            "rebound_tranches": p.get("rebound_tranches", []),
            "pnl":       pnl_unreal,
            "pct":       max(0, min(100, pct)),
            "opened_at": p.get("opened_at", "—"),
            "breakeven_armed": bool(p.get("breakeven_armed", False)),
            "breakeven_gap_polls": int(p.get("breakeven_gap_polls", 0)),
        }
    now_ts  = int(time.time())
    slot_ts = (now_ts // 900) * 900
    secs_in = now_ts - slot_ts
    gap_out = {}
    gap_threshold_out = {}
    cvd_out = {}
    volume_out = {}
    golden_rvol_out = {}
    golden_optimizer_out = {}
    ema_now = {}
    candle_out = {}
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
        cvd_session, cvd_window, cvd_slope = get_cvd_snapshot(a)
        cvd_out[a] = {"session": round(cvd_session, 3), "window": round(cvd_window, 3), "slope": round(cvd_slope, 6)}
        rvol_min = get_rvol_min(secs_into=secs_in)
        vol = get_volume_snapshot(a, VOLUME_AVG_PERIOD, rvol_min)
        rvol_value = vol.get("rvol")
        rvol_confirmed = rvol_value is not None and float(rvol_value) > rvol_min
        volume_out[a] = {
            "rvol_minute": get_rvol_minute(secs_in),
            "current": round(float(vol["current"]), 2) if vol.get("current") is not None else None,
            "average": round(float(vol["average"]), 2) if vol.get("average") is not None else None,
            "rvol": round(float(vol["rvol"]), 3) if vol.get("rvol") is not None else None,
            "above_average": bool(vol.get("above_average", False)),
            "confirmed": bool(rvol_confirmed),
            "period": int(vol.get("period", VOLUME_AVG_PERIOD)),
            "rvol_min": float(vol.get("rvol_min", rvol_min)),
            "ready": bool(vol.get("ready", False)),
        }
        golden = get_rvol_reversal_snapshot(
            a, VOLUME_AVG_PERIOD, OPPO_GOLDEN_RVOL_LOOKBACK,
            OPPO_GOLDEN_RVOL_MIN_HIGH, OPPO_GOLDEN_RVOL_THRESHOLD,
        )
        probability = golden.get("probability")
        samples = int(golden.get("samples", 0))
        probability_ok = samples >= OPPO_GOLDEN_MIN_SAMPLES and (
            probability is None or probability >= OPPO_GOLDEN_MIN_PROBABILITY
        )
        prior_rvols = list(golden.get("rvols", []))
        golden_gap_limit = c_open * GAP_SWING.get(a, 0.001) * OPPO_GOLDEN_GAP_MAG if c_open > 0 else None
        golden_actual_gap = abs(c_live - c_open) if c_open > 0 and c_live is not None else None
        golden_gap_passed = bool(
            golden_gap_limit is None or golden_actual_gap is None or golden_actual_gap < golden_gap_limit
        )
        setup_qualified = bool(OPPO_GOLDEN_RVOL_ENABLED and golden.get("armed") and probability_ok)
        golden_rvol_out[a] = {
            "enabled": OPPO_GOLDEN_RVOL_ENABLED,
            "armed": bool(golden.get("armed", False)),
            "setup_qualified": setup_qualified,
            "qualified": bool(setup_qualified and golden_gap_passed),
            "gap_passed": golden_gap_passed,
            "gap_actual": round(float(golden_actual_gap), 4) if golden_actual_gap is not None else None,
            "gap_limit": round(float(golden_gap_limit), 4) if golden_gap_limit is not None else None,
            "gap_magnitude": OPPO_GOLDEN_GAP_MAG,
            "side": golden.get("side"),
            "high_rvol_count": int(golden.get("high_rvol_count", 0)),
            "required": OPPO_GOLDEN_RVOL_MIN_HIGH,
            "lookback": OPPO_GOLDEN_RVOL_LOOKBACK,
            "threshold": OPPO_GOLDEN_RVOL_THRESHOLD,
            "probability": round(float(probability), 4) if probability is not None else None,
            "samples": samples,
            "wins": int(golden.get("wins", 0)),
            "candles": [
                {
                    "label": f"i-{offset}",
                    "rvol": round(float(value), 3) if value is not None else None,
                    "passed": bool(value is not None and value > OPPO_GOLDEN_RVOL_THRESHOLD),
                }
                for offset, value in enumerate(reversed(prior_rvols), start=1)
            ],
        }
        if OPPO_OPTIMIZER_ENABLED:
            golden_optimizer_out[a] = get_golden_optimizer_snapshot(
                a, VOLUME_AVG_PERIOD,
                current={
                    "lookback": OPPO_GOLDEN_RVOL_LOOKBACK,
                    "min_high": OPPO_GOLDEN_RVOL_MIN_HIGH,
                    "threshold": OPPO_GOLDEN_RVOL_THRESHOLD,
                    "gap_magnitude": OPPO_GOLDEN_GAP_MAG,
                },
                min_validation_samples=OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES,
            )
        else:
            golden_optimizer_out[a] = {"mode": "off", "ready": False}
        ema_fast, ema_slow = get_ema_snapshot(a)
        ema_now[a] = {
            "ema_fast": round(float(ema_fast), 4) if ema_fast is not None else None,
            "ema_slow": round(float(ema_slow), 4) if ema_slow is not None else None,
        }
        candle_out[a] = get_candle_history(a, limit=18)
    oppo_trade_optimizer_out = _build_oppo_trade_optimizer_snapshot()
    _record_optimizer_recommendations(golden_optimizer_out, oppo_trade_optimizer_out)
    return {
        "updated":       datetime.now().strftime("%Y-%m-%d %H:%M"),
        "dry_run":       DRY_RUN,
        "stats":         dict(stats),
        "positions":     positions_out,
        "prices":        dict(live_prices),
        "gap":           gap_out,
        "gap_threshold": gap_threshold_out,
        "cvd":           cvd_out,
        "volume":        volume_out,
        "golden_rvol":   golden_rvol_out,
        "golden_optimizer": golden_optimizer_out,
        "oppo_trade_optimizer": oppo_trade_optimizer_out,
        "cvd_history":   {a: list(cvd_history.get(a, [])) for a in ASSETS},
        "ema_now":       ema_now,
        "ema_history":   {a: list(ema_history.get(a, [])) for a in ASSETS},
        "kraken_candles": candle_out,
        "window": {
            "secs_into": secs_in,
            "secs_left": 900 - secs_in,
            "period":    "early" if secs_in < 300 else ("mid" if secs_in < 600 else "late"),
        },
        "normal_blacklisted_assets": sorted(list(normal_blacklisted_assets)),
        "oppo_dead_zone_blacklisted_assets": sorted(list(oppo_dead_zone_blacklisted_assets)),
        "oppo_rvol_blacklisted_assets": sorted(list(oppo_rvol_blacklisted_assets)),
        "oppo_knife_blacklisted_assets": sorted(list(oppo_knife_blacklisted_assets)),
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
                "oppo_rvol_blacklisted": a in oppo_rvol_blacklisted_assets,
                "oppo_knife_blacklisted": a in oppo_knife_blacklisted_assets,
                "trend_guarded": a in trend_guarded_assets,
            } for a in ASSETS
        },
        "pnl_history":   list(pnl_history),
        "asset_history": dict(asset_history),
        "trade_log":     list(trade_log),
        "oppo_trigger_log": list(oppo_trigger_log),
        "pump_tracker": {
            k: {
                "asset": v.get("asset", k.split("_")[0]).upper(),
                "side": v.get("side", k.split("_")[1] if "_" in k else "").upper(),
                "window_start": v.get("window_start"),
                "started_at": v.get("started_at", ""),
                "base_price": round(float(v.get("base_price", 0.0)), 4),
                "trough": round(float(v.get("trough", 0.0)), 4),
                "current": round(float(v.get("current", 0.0)), 4),
                "multiple": round(float(v.get("multiple", 0.0)), 3),
                "max_price": round(float(v.get("max_price", 0.0)), 4),
                "max_multiple": round(float(v.get("max_multiple", 0.0)), 3),
                "kraken_gap": round(float(v["kraken_gap"]), 4) if v.get("kraken_gap") is not None else None,
                "kraken_gap_ratio": round(float(v["kraken_gap_ratio"]), 4) if v.get("kraken_gap_ratio") is not None else None,
                "cvd_slope": round(float(v["cvd_slope"]), 6) if v.get("cvd_slope") is not None else None,
                "rvol": round(float(v["rvol"]), 3) if v.get("rvol") is not None else None,
                "entry_at": v.get("entry_at", v.get("started_at", "")),
                "entry_ts": v.get("entry_ts"),
                "price_updates": int(v.get("price_updates", 0)),
                "entry_kraken_gap_ratio": round(float(v["entry_kraken_gap_ratio"]), 4) if v.get("entry_kraken_gap_ratio") is not None else None,
                "entry_cvd_slope": round(float(v["entry_cvd_slope"]), 6) if v.get("entry_cvd_slope") is not None else None,
                "entry_rvol": round(float(v["entry_rvol"]), 3) if v.get("entry_rvol") is not None else None,
                "status": v.get("status", "TRACKING"),
                "highest_milestone": int(v.get("highest_milestone", 1)),
            } for k, v in pump_tracker.items()
        },
        "pump_log":       list(pump_log),
        "optimizer_recommendation_history": list(optimizer_recommendation_history),
        "settings": {
            "assets":     ASSETS,
            "buy_min":    BUY_PRICE_MIN,
            "buy_max":    BUY_PRICE_MAX,
            "sell_multiplier": SELL_MULTIPLIER,
            "sell_cap":   SELL_CAP,
            "cut_loss":   CUT_LOSS_PCT,
            "oppo_cut_loss": OPPO_CUT_LOSS_PCT,
            "oppo_min_price": OPPO_MIN_PRICE,
            "oppo_max_price": OPPO_MAX_PRICE,
            "oppo_rebound_mult": OPPO_REBOUND_MULT,
            "oppo_falling_knife_min_move": OPPO_FALLING_KNIFE_MIN_MOVE,
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
            "oppo_rebound_max_price": OPPO_REBOUND_MAX_PRICE,
            "oppo_gap_mag": OPPO_GAP_MAG,
            "oppo_golden_gap_mag": OPPO_GOLDEN_GAP_MAG,
            "oppo_counter_enabled": OPPO_COUNTER_ENABLED,
            "oppo_counter_min_price": OPPO_COUNTER_MIN_PRICE,
            "oppo_counter_max_price": OPPO_COUNTER_MAX_PRICE,
            "oppo_counter_buy_amount": OPPO_COUNTER_BUY_AMOUNT,
            "oppo_counter_sell_multiplier": OPPO_COUNTER_SELL_MULTIPLIER,
            "oppo_counter_sell_cap": OPPO_COUNTER_SELL_CAP,
            "oppo_counter_cut_loss_pct": OPPO_COUNTER_CUT_LOSS_PCT,
            "flexi_rvol_buy_amount": FLEXI_RVOL_BUY_AMOUNT,
            "flexi_rvol_enabled": FLEXI_RVOL_ENABLED,
            "order":      BUY_AMOUNT,
            "poll":       POLL_SECS,
            "breakeven_polls": BREAKEVEN_POLL_CONFIRMATIONS,
            "entry_after": ENTRY_AFTER,
            "stop_buy":   STOP_BUY_AT,
            "volume_avg_period": VOLUME_AVG_PERIOD,
            "rvol_min": RVOL_MIN,
            "rvol_min_per_min": RVOL_MIN_PER_MIN,
            "oppo_rvol_guard_enabled": OPPO_RVOL_GUARD_ENABLED,
            "oppo_golden_rvol_enabled": OPPO_GOLDEN_RVOL_ENABLED,
            "oppo_golden_rvol_rule": f"{OPPO_GOLDEN_RVOL_MIN_HIGH}/{OPPO_GOLDEN_RVOL_LOOKBACK} > {OPPO_GOLDEN_RVOL_THRESHOLD:.2f}",
            "oppo_golden_rvol_threshold": OPPO_GOLDEN_RVOL_THRESHOLD,
            "oppo_golden_min_probability": OPPO_GOLDEN_MIN_PROBABILITY,
            "oppo_golden_min_samples": OPPO_GOLDEN_MIN_SAMPLES,
            "oppo_optimizer_enabled": OPPO_OPTIMIZER_ENABLED,
            "oppo_optimizer_min_validation_samples": OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES,
            "oppo_trade_optimizer_enabled": OPPO_TRADE_OPTIMIZER_ENABLED,
            "oppo_trade_optimizer_min_validation_trades": OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES,
            "oppo_optimizer_min_observation_secs": OPPO_OPTIMIZER_MIN_OBSERVATION_SECS,
            "oppo_optimizer_min_price_updates": OPPO_OPTIMIZER_MIN_PRICE_UPDATES,
            "oppo_optimizer_max_multiple_cap": OPPO_OPTIMIZER_MAX_MULTIPLE_CAP,
            "oppo_optimizer_no_pump_multiple": OPPO_OPTIMIZER_NO_PUMP_MULTIPLE,
            "oppo_optimizer_history_hours": OPPO_OPTIMIZER_HISTORY_HOURS,
            "oppo_optimizer_score_equivalence": OPPO_OPTIMIZER_SCORE_EQUIVALENCE,
            "pump_track_start_price": PUMP_TRACK_START_PRICE,
            "pump_track_window_secs": PUMP_TRACK_WINDOW_SECS,
            "pump_track_dead_zone_price": PUMP_TRACK_DEAD_ZONE_PRICE,
            "pump_track_success_min_multiple": PUMP_TRACK_SUCCESS_MIN_MULTIPLE,
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
.pump-table th,.pump-table td{padding-left:2px;padding-right:2px}
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
.golden-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:10px}
.golden-card{background:#1e2533;border:1px solid #2a3347;border-radius:9px;padding:12px}
.golden-card.qualified{border-color:#fbbf24;box-shadow:0 0 0 1px rgba(251,191,36,.18)}
.golden-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}
.golden-candles{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}
.golden-candle{background:#161b27;border:1px solid #30394b;border-radius:7px;padding:7px;text-align:center;font-family:monospace;font-size:11px}
.golden-candle.pass{background:#0d2a1e;border-color:#166534;color:#4ade9f}
.golden-candle.fail{color:#7b879d}
.golden-candle .slot{display:block;font-size:10px;text-transform:uppercase;margin-bottom:2px;color:inherit}
.golden-meta{display:flex;justify-content:space-between;gap:8px;margin-top:9px;font-size:11px;color:#7b879d;font-family:monospace}
footer{text-align:center;color:#2a3347;font-size:11px;margin-top:20px;padding-bottom:10px}
</style>
</head>
<body>
<div id="root"><p style="color:#5a6a85;padding:40px;text-align:center">Loading...</p></div>
<script>
let oppoResetConfirmOpen=false;
let oppoLogScrollTop=0;
const pumpScrollIds=['pumpActiveWrap','pumpLogWrap'];
const pumpScrollLeft={pumpActiveWrap:0,pumpLogWrap:0};
const PUMP_SCROLL_HOLD_MS=2500;
let pumpScrollHoldUntil=0;
let pendingPumpState=null;
function markPumpScrollActive(){pumpScrollHoldUntil=Date.now()+PUMP_SCROLL_HOLD_MS;}
function pumpScrollIsActive(){
  return Date.now()<pumpScrollHoldUntil && pumpScrollIds.some(id=>document.getElementById(id));
}
function capturePumpScroll(){
  pumpScrollIds.forEach(id=>{const el=document.getElementById(id); if(el)pumpScrollLeft[id]=el.scrollLeft;});
}
function restorePumpScroll(){
  pumpScrollIds.forEach(id=>{
    const el=document.getElementById(id);
    if(!el)return;
    const maxLeft=Math.max(0,el.scrollWidth-el.clientWidth);
    el.scrollLeft=Math.min(pumpScrollLeft[id]||0,maxLeft);
    if(!el.dataset.pumpScrollBound){
      el.addEventListener('scroll',()=>{pumpScrollLeft[id]=el.scrollLeft;markPumpScrollActive();},{passive:true});
      ['wheel','pointerdown','mousedown','touchstart'].forEach(evt=>el.addEventListener(evt,markPumpScrollActive,{passive:true}));
      el.dataset.pumpScrollBound='1';
    }
  });
}
function fmt(v,d=4){return v!=null?'$'+parseFloat(v).toFixed(d):'—'}
function fmtCents(v,d=0){return v!=null?(parseFloat(v)*100).toFixed(d)+'c':'—'}
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

function drawEmaChart(candles, wrap){
  if(!candles||candles.length<2){
    wrap.innerHTML='<p class="dim" style="padding:12px 0;font-size:12px">Not enough candle data yet</p>';
    return;
  }
  let canvas=wrap.querySelector('canvas');
  if(!canvas){canvas=document.createElement('canvas');wrap.appendChild(canvas);}
  const W=wrap.offsetWidth||600,H=180;
  canvas.width=W;canvas.height=H;
  const ctx=canvas.getContext('2d');
  ctx.clearRect(0,0,W,H);
  const padT=12,padB=20,padL=48,padR=12;
  const cW=W-padL-padR,cH=H-padT-padB;
  const vals=[];candles.forEach(c=>vals.push(c.high,c.low));
  const minV=Math.min(...vals),maxV=Math.max(...vals),range=maxV-minV||1;
  const xStep=cW/candles.length;
  const yOf=v=>padT+cH-((v-minV)/range)*cH;
  ctx.strokeStyle='#2a3347';ctx.lineWidth=1;
  [0,.5,1].forEach(t=>{const y=padT+cH*t;ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(W-padR,y);ctx.stroke();});
  candles.forEach((c,i)=>{
    const x=padL+i*xStep+xStep*0.5;
    const yH=yOf(c.high), yL=yOf(c.low), yO=yOf(c.open), yC=yOf(c.close);
    const up=c.close>=c.open;
    ctx.strokeStyle=up?'#1db87a':'#e24b4a';
    ctx.beginPath();ctx.moveTo(x,yH);ctx.lineTo(x,yL);ctx.stroke();
    const bw=Math.max(3,xStep*0.6), by=Math.min(yO,yC), bh=Math.max(1,Math.abs(yC-yO));
    ctx.fillStyle=up?'#1db87a':'#e24b4a';ctx.fillRect(x-bw/2,by,bw,bh);
  });
  const k=2/(8+1),k2=2/(25+1); let e8=null,e25=null; const s8=[],s25=[];
  candles.forEach(c=>{e8=e8==null?c.close:(c.close-e8)*k+e8; e25=e25==null?c.close:(c.close-e25)*k2+e25; s8.push(e8); s25.push(e25);});
  const drawLine=(series,color)=>{ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=2;series.forEach((v,i)=>{const x=padL+i*xStep+xStep*0.5,y=yOf(v);i?ctx.lineTo(x,y):ctx.moveTo(x,y);});ctx.stroke();};
  drawLine(s8,'#fbbf24'); drawLine(s25,'#ff4fd8');
}

function drawCvdChart(historyMap, asset, wrap){
  const points=(historyMap&&historyMap[asset]||[]).filter(p=>p&&p.window!=null);
  if(points.length<2){
    wrap.innerHTML='<p class="dim" style="padding:12px 0;font-size:12px">Not enough CVD history yet</p>';
    return;
  }
  let canvas=wrap.querySelector('canvas');
  if(!canvas){canvas=document.createElement('canvas');wrap.appendChild(canvas);}
  const W=wrap.offsetWidth||600,H=180;
  canvas.width=W;canvas.height=H;
  const ctx=canvas.getContext('2d');
  ctx.clearRect(0,0,W,H);
  const padT=18,padB=34,padL=58,padR=140;
  const cW=W-padL-padR,cH=H-padT-padB;
  const vals=points.map(p=>Number(p.window)||0);
  const minV=Math.min(...vals,0),maxV=Math.max(...vals,0),range=maxV-minV||1;
  const xOf=i=>padL+(points.length<=1?0:i*(cW/(points.length-1)));
  const yOf=v=>padT+cH-(((v-minV)/range)*cH);
  ctx.strokeStyle='#2a3347';ctx.lineWidth=1;
  [0,.25,.5,.75,1].forEach(t=>{
    const y=padT+cH*t;ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(W-padR,y);ctx.stroke();
    const lbl=(minV+(maxV-minV)*(1-t)).toFixed(1);
    ctx.fillStyle='#5a6a85';ctx.font='10px system-ui';ctx.textAlign='right';ctx.fillText(lbl,padL-6,y+4);
  });
  if(minV<0&&maxV>0){
    const yz=yOf(0);ctx.strokeStyle='#3a4560';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(padL,yz);ctx.lineTo(W-padR,yz);ctx.stroke();ctx.setLineDash([]);
  }
  ctx.beginPath();ctx.strokeStyle='#60a5fa';ctx.lineWidth=2;
  vals.forEach((v,i)=>{const x=xOf(i),y=yOf(v);i?ctx.lineTo(x,y):ctx.moveTo(x,y);});
  ctx.stroke();
  vals.forEach((v,i)=>{ctx.beginPath();ctx.arc(xOf(i),yOf(v),2.5,0,Math.PI*2);ctx.fillStyle='#60a5fa';ctx.fill();});
  const last=points[points.length-1];
  ctx.fillStyle='#60a5fa';ctx.font='12px system-ui';ctx.textAlign='left';
  ctx.fillText(`${asset.toUpperCase()} window ${Number(last.window||0).toFixed(1)}`, W-padR+8, padT+14);
  ctx.fillStyle='#fbbf24';
  ctx.fillText(`slope ${Number(last.slope||0).toFixed(4)}`, W-padR+8, padT+32);
  ctx.fillStyle='#5a6a85';
  ctx.fillText(`session ${Number(last.session||0).toFixed(1)}`, W-padR+8, padT+50);
  const step=Math.max(1,Math.floor(points.length/6));
  ctx.fillStyle='#5a6a85';ctx.font='10px system-ui';ctx.textAlign='center';
  for(let i=0;i<points.length;i+=step)ctx.fillText(points[i].ts||'',xOf(i),H-12);
  if((points.length-1)%step!==0)ctx.fillText(points[points.length-1].ts||'',xOf(points.length-1),H-12);
}


let _tlExpanded=false;
const TL_COLLAPSE=5;
let _pumpActiveExpanded=false;
let _pumpLogExpanded=false;
const PUMP_ACTIVE_COLLAPSE=8;
const PUMP_LOG_COLLAPSE=40;
function tlToggle(){
  _tlExpanded=!_tlExpanded;
  document.querySelectorAll('.tl-row').forEach((r,i)=>{if(i>=TL_COLLAPSE)r.style.display=_tlExpanded?'':'none';});
  const btn=document.getElementById('tlToggle');
  if(btn){const h=Array.from(document.querySelectorAll('.tl-row')).filter(r=>r.style.display==='none').length;
    btn.textContent=_tlExpanded?'▲ Show less':'▼ Show '+h+' more';}
}
function pumpToggle(kind){
  const isActive=kind==='active';
  const cls=isActive?'.pump-active-row':'.pump-log-row';
  const limit=isActive?PUMP_ACTIVE_COLLAPSE:PUMP_LOG_COLLAPSE;
  if(isActive)_pumpActiveExpanded=!_pumpActiveExpanded; else _pumpLogExpanded=!_pumpLogExpanded;
  const expanded=isActive?_pumpActiveExpanded:_pumpLogExpanded;
  document.querySelectorAll(cls).forEach((r,i)=>{if(i>=limit)r.style.display=expanded?'':'none';});
  const btn=document.getElementById(isActive?'pumpActiveToggle':'pumpLogToggle');
  if(btn){const h=Array.from(document.querySelectorAll(cls)).filter(r=>r.style.display==='none').length;
    btn.textContent=expanded?'▲ Show less':'▼ Show '+h+' more';}
}
function pumpAssetLabel(asset,side){
  const colors={BTC:['#f59e0b','#2a1f08','#5c3d08'],ETH:['#60a5fa','#0d1a2a','#1a3a5c'],SOL:['#a78bfa','#21133f','#4c1d95'],XRP:['#94a3b8','#172033','#334155']};
  const a=String(asset||'—').toUpperCase(),s=String(side||'—').toUpperCase();
  const c=colors[a]||['#e8edf5','#1e2533','#2a3347'];
  const sideStyle=s==='YES'?'background:#0d2a1e;color:#4ade80;border-color:#166534':(s==='NO'?'background:#2a0d0d;color:#f87171;border-color:#7f1d1d':'background:#1e2533;color:#8a9ab5;border-color:#2a3347');
  return `<span class="badge" style="background:${c[1]};color:${c[0]};border:1px solid ${c[2]};font-size:10px">${a}</span><span class="badge" style="${sideStyle};font-size:10px;margin-left:4px">${s}</span>`;
}
function showMoreButton(id,onclick,total,limit,expanded){
  const extra=total-limit;
  return extra>0?`<button id="${id}" onclick="${onclick}" style="margin-top:10px;background:#1e2533;border:1px solid #2a3347;color:#60a5fa;border-radius:6px;padding:5px 14px;font-size:12px;cursor:pointer">${expanded?'▲ Show less':'▼ Show '+extra+' more'}</button>`:'';
}

function renderTradeLog(log){
  if(!log||!log.length)return'<p class="dim" style="padding:8px 0;font-size:12px">No closed trades yet</p>';
  const exitBadge=e=>{
    const col={SELL:'#0d2a1e','FLIP-SELL':'#0d1a2a','COUNTER-SELL':'#1e1b4b','CUT-LOSS':'#2a0d0d','BREAKEVEN-SELL':'#0f172a'}[e]||'#2a0d0d';
    const tc={'SELL':'#4ade9f','FLIP-SELL':'#60a5fa','COUNTER-SELL':'#a5b4fc','CUT-LOSS':'#f87171','BREAKEVEN-SELL':'#93c5fd'}[e]||'#f87171';
    const bc={'SELL':'#1a5c3a','FLIP-SELL':'#1a3a5c','COUNTER-SELL':'#4338ca','CUT-LOSS':'#5c1d1d','BREAKEVEN-SELL':'#334155'}[e]||'#5c1d1d';
    return `<span class="badge" style="background:${col};color:${tc};border:1px solid ${bc}">${e}</span>`;
  };
  const rows=log.map((t,i)=>{
    const p=t.pnl||0,ps=(p>=0?'+':'')+p.toFixed(4);
    const flipTag=t.is_flip?'<span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c;font-size:10px;margin-left:4px">FLIP</span>':'';
    const counterTag=t.is_counter?'<span class="badge" style="background:#1e1b4b;color:#a5b4fc;border:1px solid #4338ca;font-size:10px;margin-left:4px">COUNTER</span>':'';
    const rvol=t.entry_rvol!=null?Number(t.entry_rvol):null;
    const rvolTxt=rvol!=null?rvol.toFixed(2)+'x':'—';
    const rvolCls=rvol!=null?'blue':'dim';
    return `<tr class="tl-row" style="${i>=TL_COLLAPSE&&!_tlExpanded?'display:none':''}">
      <td>${t.time||'—'}</td>
      <td><strong>${t.asset}-${t.side}</strong>${flipTag}${counterTag}</td>
      <td>${fmt(t.entry, 2)}</td>
      <td>${fmt(t.target, 2)}</td>
      <td>${exitBadge(t.exit, 2)}</td>
      <td>${fmt(t.exit_px, 2)}</td>
      <td class="${p>0?'green':p<0?'red':'dim'}" style="font-weight:600">$${ps}</td>
      <td class="${rvolCls}" style="font-weight:600">${rvolTxt}</td>
    </tr>`;
  }).join('');
  const extra=log.length-TL_COLLAPSE;
  const btn=extra>0?`<button id="tlToggle" onclick="tlToggle()" style="margin-top:10px;background:#1e2533;border:1px solid #2a3347;color:#60a5fa;border-radius:6px;padding:5px 14px;font-size:12px;cursor:pointer">${_tlExpanded?'▲ Show less':'▼ Show '+extra+' more'}</button>`:'';
  return `<div style="overflow-x:auto"><table>
    <thead><tr><th>Time</th><th>Asset</th><th>Entry</th><th>Target</th><th>Exit</th><th>Exit $</th><th>PnL</th><th>Buy RVOL</th></tr></thead>
    <tbody>${rows}</tbody></table></div>${btn}`;
}

function renderPumpTracker(trackers,log,startPrice,deadZonePrice){
  const active=Object.values(trackers||{}).sort((a,b)=>(b.max_multiple||0)-(a.max_multiple||0));
  const activeRows=active.map((p,i)=>{
    const mult=Number(p.multiple||0), maxMult=Number(p.max_multiple||0);
    const cls=maxMult>=5?'red':maxMult>=4?'amber':maxMult>=3?'blue':'dim';
    const gap=p.kraken_gap!=null?Number(p.kraken_gap).toFixed(4):'—';
    const slope=p.cvd_slope!=null?Number(p.cvd_slope).toFixed(4):'—';
    const slopeCls=p.cvd_slope>0?'green':p.cvd_slope<0?'red':'dim';
    const rvol=p.rvol!=null?Number(p.rvol).toFixed(2)+'x':'—';
    const rvolCls=p.rvol!=null?'blue':'dim';
    const status=p.status||'TRACKING';
    const statusCls=status==='SUCCESS'?'green':status==='FAILED'?'red':'dim';
    return `<tr class="pump-active-row" style="${i>=PUMP_ACTIVE_COLLAPSE&&!_pumpActiveExpanded?'display:none':''}">
      <td><strong>${pumpAssetLabel(p.asset,p.side)}</strong></td>
      <td>${p.started_at||'—'}</td>
      <td>${fmtCents(p.base_price,1)}</td>
      <td>${fmtCents(p.trough,1)}</td>
      <td>${fmtCents(p.current,1)}</td>
      <td class="${cls}" style="font-weight:600">${mult.toFixed(2)}x</td>
      <td class="${cls}" style="font-weight:600">${maxMult.toFixed(2)}x</td>
      <td style="font-family:monospace">${gap}</td>
      <td class="${slopeCls}" style="font-family:monospace">${slope}</td>
      <td class="${rvolCls}" style="font-family:monospace">${rvol}</td>
      <td class="${statusCls}" style="font-weight:600">${status}</td>
      <td>${p.highest_milestone>=3?p.highest_milestone+'x':'—'}</td>
    </tr>`;
  }).join('') || `<tr><td colspan="12" class="dim">No active ${Math.round((deadZonePrice||0.05)*100)}–${Math.round((startPrice||0.2)*100)}¢ pump trackers yet</td></tr>`;
  const windowTimeColors=['#e8edf5','#fbbf24'];
  const windowColorIndex=new Map();
  let nextWindowColor=0;
  const eventRows=(log||[]).map((e,i)=>{
    const m=Number(e.multiple||0),cls=m>=5?'red':m>=4?'amber':'blue';
    const gap=e.kraken_gap!=null?Number(e.kraken_gap).toFixed(4):'—';
    const slope=e.cvd_slope!=null?Number(e.cvd_slope).toFixed(4):'—';
    const slopeCls=e.cvd_slope>0?'green':e.cvd_slope<0?'red':'dim';
    const rvol=e.rvol!=null?Number(e.rvol).toFixed(2)+'x':'—';
    const rvolCls=e.rvol!=null?'blue':'dim';
    const status=e.status||'TRACKING';
    const statusCls=status==='SUCCESS'?'green':status==='FAILED'?'red':'dim';
    const windowKey=e.window_start!=null?String(e.window_start):`unknown-${e.time||i}`;
    if(!windowColorIndex.has(windowKey))windowColorIndex.set(windowKey,nextWindowColor++%windowTimeColors.length);
    const timeColor=windowTimeColors[windowColorIndex.get(windowKey)];
    return `<tr class="pump-log-row" style="${i>=PUMP_LOG_COLLAPSE&&!_pumpLogExpanded?'display:none':''}">
      <td style="color:${timeColor};font-weight:700">${e.time||'—'}</td>
      <td><strong>${pumpAssetLabel(e.asset,e.side)}</strong></td>
      <td><span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c">${e.milestone||'—'}</span></td>
      <td>${fmtCents(e.base_price,1)}</td>
      <td>${fmtCents(e.current,1)}</td>
      <td class="${cls}" style="font-weight:600">${m.toFixed(2)}x</td>
      <td style="font-family:monospace">${gap}</td>
      <td class="${slopeCls}" style="font-family:monospace">${slope}</td>
      <td class="${rvolCls}" style="font-family:monospace">${rvol}</td>
      <td class="${statusCls}" style="font-weight:600">${status}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="10" class="dim">No 3x+ pump milestones yet</td></tr>';
  const activeBtn=showMoreButton('pumpActiveToggle',"pumpToggle('active')",active.length,PUMP_ACTIVE_COLLAPSE,_pumpActiveExpanded);
  const logBtn=showMoreButton('pumpLogToggle',"pumpToggle('log')",(log||[]).length,PUMP_LOG_COLLAPSE,_pumpLogExpanded);
  return `<div id="pumpActiveWrap" style="overflow-x:auto"><table class="pump-table">
    <thead><tr><th>Active</th><th>Started</th><th>Base</th><th>Trough</th><th>Current</th><th>Now</th><th>Max</th><th>Kraken Gap</th><th>CVD Slope</th><th>RVOL</th><th>Status</th><th>Hit</th></tr></thead>
    <tbody>${activeRows}</tbody></table></div>${activeBtn}
    <div style="height:10px"></div>
    <div id="pumpLogWrap" style="overflow-x:auto"><table class="pump-table">
    <thead><tr><th>Time</th><th>Asset</th><th>Milestone</th><th>Base</th><th>Price</th><th>Multiple</th><th>Kraken Gap</th><th>CVD Slope</th><th>RVOL</th><th>Status</th></tr></thead>
    <tbody>${eventRows}</tbody></table></div>${logBtn}`;
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
  capturePumpScroll();
  const st=s.stats||{},pos=s.positions||{},pr=s.prices||{};
  const cfg=s.settings||{},w=s.window||{},gap=s.gap||{},gapThreshold=s.gap_threshold||{},cvd=s.cvd||{},volumes=s.volume||{},cvdHistory=s.cvd_history||{};
  const assetStatus=s.asset_status||{};
  const oppoLastTrigger=s.oppo_last_trigger||{};
  const normalBlacklisted=new Set(s.normal_blacklisted_assets||[]);
  const knifeBlacklisted=new Set(s.oppo_knife_blacklisted_assets||[]);
  const trendGuarded=new Set(s.trend_guarded_assets||[]);
  const pnlHist=s.pnl_history||[],assetHist=s.asset_history||{},tLog=s.trade_log||[],pumpTrackers=s.pump_tracker||{},pumpLog=s.pump_log||[];
  const emaNow=s.ema_now||{},emaHistory=s.ema_history||{},krakenCandles=s.kraken_candles||{},goldenRvol=s.golden_rvol||{},goldenOptimizer=s.golden_optimizer||{},oppoTradeOptimizer=s.oppo_trade_optimizer||{};
  const oppoLog=(s.oppo_trigger_log||[]).filter(o=>['GOLDEN','GOLDEN-GAP-BLOCK','BOUGHT','SELL','SOLD','CUT-LOSS','RVOL-BLOCK','KNIFE-BLOCK','COUNTER-ARM','COUNTER-BOUGHT','COUNTER-SELL','COUNTER-CUT-LOSS'].includes(o.status));
  const assets=cfg.assets||['btc','eth','sol','xrp'];
  const mode=s.dry_run?'<span class="badge dry">DRY RUN</span>':'<span class="badge live">LIVE</span>';
  const period=w.period||'early';
  const mm=v=>String(Math.floor((v||0)/60)).padStart(2,'0');
  const ss=v=>String((v||0)%60).padStart(2,'0');
  const wStr=`${mm(w.secs_into)}:${ss(w.secs_into)} in &nbsp;|&nbsp; ${mm(w.secs_left)}:${ss(w.secs_left)} left`;
  const total=(st.wins||0)+(st.losses||0);
  const wr=total>0?Math.round(st.wins/total*100)+'%':'—';
  const pnl=st.pnl||0;

  const goldenCards=assets.map(a=>{
    const g=goldenRvol[a]||{},candles=g.candles||[];
    const gapBlocked=g.setup_qualified && g.gap_passed===false;
    const state=!g.enabled?'OFF':(gapBlocked?'GAP BLOCK':(g.qualified?'GOLDEN':(g.armed?'ARMED':'WATCHING')));
    const stateCls=gapBlocked?'red':(g.qualified?'amber':(g.armed?'blue':'dim'));
    const probability=g.probability!=null?`${(Number(g.probability)*100).toFixed(1)}% (${g.wins||0}/${g.samples||0})`:'collecting samples';
    const candleCells=[1,2,3].map(slot=>{
      const c=candles.find(x=>x.label===`i-${slot}`)||{};
      const cls=c.passed?'pass':'fail';
      const value=c.rvol!=null?`${Number(c.rvol).toFixed(2)}x`:'—';
      return `<div class="golden-candle ${cls}"><span class="slot">i-${slot}</span><strong>${value}</strong><div>${c.passed?'PASS':'WAIT'}</div></div>`;
    }).join('');
    return `<div class="golden-card ${g.qualified?'qualified':''}">
      <div class="golden-head"><strong>${a.toUpperCase()}</strong><span class="${stateCls}" style="font-weight:700">${state}${g.side?` · ${(g.side||'').toUpperCase()}`:''}</span></div>
      <div class="golden-candles">${candleCells}</div>
      <div class="golden-meta"><span>${g.high_rvol_count||0}/${g.lookback||3} high · need ${g.required||2}</span><span>history ${probability}</span></div>
      <div class="golden-meta"><span>golden gap x${Number(g.gap_magnitude||3).toFixed(2)}</span><span class="${g.gap_passed===false?'red':'green'}">${g.gap_actual!=null?Number(g.gap_actual).toFixed(4):'—'} / ${g.gap_limit!=null?Number(g.gap_limit).toFixed(4):'—'}</span></div>
    </div>`;
  }).join('');

  const optimizerRows=assets.map(a=>{
    const o=goldenOptimizer[a]||{},r=o.recommendation||{},rc=r.config||{},rv=r.validation||{},cur=o.current||{},cv=cur.validation||{};
    const rec=o.ready?`L${rc.lookback} · ${rc.min_high} high · RVOL>${Number(rc.threshold).toFixed(2)} · gap x${Number(rc.gap_magnitude).toFixed(1)}`:'collecting data';
    const recRate=rv.rate!=null?`${(Number(rv.rate)*100).toFixed(1)}% (${rv.wins||0}/${rv.samples||0})`:'—';
    const curRate=cv.rate!=null?`${(Number(cv.rate)*100).toFixed(1)}% (${cv.wins||0}/${cv.samples||0})`:'—';
    return `<tr><td>${a.toUpperCase()}</td><td>${o.mode||'shadow-recommend-only'}</td><td>${rec}</td><td class="${rv.rate!=null&&cv.rate!=null&&rv.rate>cv.rate?'green':'dim'}">${recRate}</td><td>${curRate}</td><td>${o.candidate_count||0}</td></tr>`;
  }).join('');

  const oto=oppoTradeOptimizer||{},otr=oto.recommendation||{},otc=otr.config||{},otv=otr.validation||{},otcur=oto.current||{},otcv=otcur.validation||{};
  const oppoOptimizerRecommendation=oto.ready
    ? `RVOL ≥ ${Number(otc.min_rvol).toFixed(1)} · Kraken gap ratio &lt; x${Number(otc.max_kraken_gap_ratio).toFixed(1)}`
    : `not ready · ${oto.readiness_reason||'collecting pump traces'}`;
  const oppoOptimizerValidation=otv.samples!=null?`median ${Number(otv.median_max_multiple||0).toFixed(2)}x · trimmed ${Number(otv.trimmed_average_max_multiple||0).toFixed(2)}x · 2x+ ${((otv.rate_2x||0)*100).toFixed(0)}% · no-pump ${((otv.no_pump_rate||0)*100).toFixed(0)}% · ${otv.samples} samples`:'—';
  const oppoOptimizerCurrent=otcv.samples!=null?`median ${Number(otcv.median_max_multiple||0).toFixed(2)}x · trimmed ${Number(otcv.trimmed_average_max_multiple||0).toFixed(2)}x · 2x+ ${((otcv.rate_2x||0)*100).toFixed(0)}% · no-pump ${((otcv.no_pump_rate||0)*100).toFixed(0)}% · ${otcv.samples} samples`:'—';
  const optimizerDataset=oto.dataset_validation||{};
  const optimizerExclusions=Object.entries(oto.quality_exclusions||{}).map(([k,v])=>`${k} ${v}`).join(', ')||'none';
  const optimizerOutcomeWarning=oto.outcome_warning?` · warning: ${oto.outcome_warning}`:'';
  const ratioStats=oto.good_pump_entry_ratio_median!=null?` · good-pump entry Kraken gap ratio median x${Number(oto.good_pump_entry_ratio_median).toFixed(2)} / average x${Number(oto.good_pump_entry_ratio_average||0).toFixed(2)}`:'';
  const optimizerQuality=`kept ${oto.samples||0} · excluded ${oto.quality_excluded||0} (${optimizerExclusions}) · validation weak &lt;2x ${optimizerDataset.weak_pumps||0}/${optimizerDataset.samples||0}${ratioStats}${optimizerOutcomeWarning}`;

  // Buy zone indicator per asset
  const priceRows=assets.map(a=>{
    const yp=pr[a+'_yes'],np=pr[a+'_no'];
    const inZone=p=>p!=null&&p>=cfg.buy_min&&p<=cfg.buy_max;
    const yc=inZone(yp)?'green':'';
    const nc=inZone(np)?'green':'';
    const holding=[(a+'_yes' in pos)?'<span class="green">YES</span>':'',(a+'_no' in pos)?'<span class="green">NO</span>':'',(a+'_yes_oppo' in pos)?'<span class="amber">YES OPPO</span>':'',(a+'_no_oppo' in pos)?'<span class="amber">NO OPPO</span>':''].filter(Boolean).join(' ');
    const stAsset=assetStatus[a]||{};
    const isBlacklisted=stAsset.blacklisted===true || normalBlacklisted.has(a);
    const isKnifeBlocked=stAsset.oppo_knife_blacklisted===true || knifeBlacklisted.has(a);
    const isTrendGuarded=stAsset.trend_guarded===true || trendGuarded.has(a);
    const flags=[isKnifeBlocked?'<span class="red">KNIFE BLOCKED</span>':(isBlacklisted?'<span class="red">BLACKLISTED</span>':''),isTrendGuarded?'<span style="color:#f59e0b">TREND GUARDED</span>':''].filter(Boolean).join(' ');
    const holdingCell=[holding,flags].filter(Boolean).join(' <span class="dim">|</span> ');
    const gv=gap[a],gt=gapThreshold[a]&&w.period?gapThreshold[a][w.period]:null;
    const cv=cvd[a]||{};
    const vol=volumes[a]||{};
    const oppYes=oppoLastTrigger[a+'_yes'];
    const oppNo=oppoLastTrigger[a+'_no'];
    const oppoParts=[oppYes,oppNo].filter(Boolean).map(o=>`${(o.side||'').toUpperCase()}: ${o.status||'—'}`).join(' <span class="dim">|</span> ');
    const oppoCell=oppoParts||'<span class="dim">—</span>';
    const gStr=gv!=null?gv.toFixed(4):'—';
    const tStr=gt!=null?gt.toFixed(4):'—';
    const cvdStr=(cv.window!=null?cv.window.toFixed(1):'—')+' / '+(cv.slope!=null?cv.slope.toFixed(4):'—');
    const rv=vol.rvol!=null?Number(vol.rvol):null;
    const rvMin=vol.rvol_min!=null?Number(vol.rvol_min):null;
    const rvCls=vol.confirmed?'green':(vol.above_average?'amber':'dim');
    const volStr=rv!=null?`${rv.toFixed(2)}x ${vol.confirmed?'✓':(vol.above_average?'↑':'')} ${rvMin!=null?`/ >${rvMin.toFixed(2)}x`:''}`:'—';
    return`<tr><td>${a.toUpperCase()}</td><td class="${yc}" style="padding-right:3px">${fmt(yp,2)}</td><td class="${nc}" style="padding-left:3px;padding-right:18px">${fmt(np,2)}</td><td style="font-family:monospace;padding-left:18px">${gStr} / ${tStr}</td><td style="font-family:monospace">${cvdStr}</td><td class="${rvCls}" style="font-family:monospace">${volStr}</td><td>${holdingCell||'<span class="dim">—</span>'}</td><td>${oppoCell}</td></tr>`;
  }).join('');

  const posCards=Object.entries(pos).map(([k,p])=>{
    const [asset,side]=k.split('_');
    const col=p.current>=p.entry?'green':'red';
    const badges=[p.is_flip?'<span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c;margin-left:6px">FLIP</span>':'',p.is_rebound?'<span class="badge" style="background:#082a1b;color:#34d399;border:1px solid #065f46;margin-left:6px">REBOUND</span>':'',p.is_counter?'<span class="badge" style="background:#1e1b4b;color:#a5b4fc;border:1px solid #4338ca;margin-left:6px">COUNTER</span>':'',p.is_oppo?'<span class="badge" style="background:#2a1e08;color:#fbbf24;border:1px solid #5c3d08;margin-left:6px">OPPO</span>':''].join('');
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
        ${p.is_oppo?`<span>BE armed: <strong class="${p.breakeven_armed?'amber':'dim'}">${p.breakeven_armed?'YES':'NO'}</strong></span><span>BE polls: <strong>${p.breakeven_gap_polls||0}</strong>/${cfg.breakeven_polls||5}</span>`:''}
      </div>
    </div>`;
  }).join('')||'<p class="dim" style="padding:8px 0">No open positions</p>';

  const oppoRows=oppoLog.map(o=>{
    const redStatuses=new Set(['CUT-LOSS','RVOL-BLOCK','GOLDEN-GAP-BLOCK','KNIFE-BLOCK','COUNTER-CUT-LOSS']);
    const greenStatuses=new Set(['BOUGHT','SOLD','SELL','COUNTER-BOUGHT','COUNTER-SELL']);
    const statusCls=greenStatuses.has(o.status)?'green':(redStatuses.has(o.status)?'red':'amber');
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
      <h2>EMA Trend (Kraken 15m) <span style="font-size:11px;color:#5a6a85;font-weight:400">EMA8 <span style="color:#fbbf24">yellow</span> / EMA25 <span style="color:#ff4fd8">pink</span></span></h2>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">${assets.map(a=>{
        const e=emaNow[a]||{},f=e.ema_fast,s=e.ema_slow;
        let t='—',c='dim';
        if(f!=null && s!=null){ t=(f>=s)?'UP':'DOWN'; c=(f>=s)?'green':'red'; }
        return `<button id="emaBtn_${a}" onclick="window.__emaAsset='${a}'" style="padding:4px 10px;background:#1e2533;border:1px solid #2a3347;color:#e8edf5;border-radius:6px;font-size:11px;cursor:pointer">${a.toUpperCase()} <span class="${c}">${t}</span></button>`;
      }).join('')}</div>
      <div class="chart-wrap" id="emaChartWrap"></div>
    </div>

    <div class="section">
      <h2>CVD Window Chart <span style="font-size:11px;color:#5a6a85;font-weight:400">select one asset; legend shows latest window / slope / session</span></h2>
      <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">${assets.map(a=>{
        const c=cvd[a]||{},active=(window.__cvdAsset||assets[0])===a;
        const slope=c.slope!=null?Number(c.slope).toFixed(4):'—';
        return `<button id="cvdBtn_${a}" onclick="window.__cvdAsset='${a}'" style="padding:4px 10px;background:${active?'#0d1e2a':'#1e2533'};border:1px solid ${active?'#60a5fa':'#2a3347'};color:#e8edf5;border-radius:6px;font-size:11px;cursor:pointer">${a.toUpperCase()} <span class="${(c.slope||0)>0?'green':(c.slope||0)<0?'red':'dim'}">${slope}</span></button>`;
      }).join('')}</div>
      <div class="chart-wrap" id="cvdChartWrap"></div>
    </div>

    <div class="section">
      <h2>Golden OPPO Progress <span style="font-size:11px;color:#5a6a85;font-weight:400">previous completed 15m candles; PASS when RVOL &gt; ${Number(cfg.oppo_golden_rvol_threshold||1).toFixed(2)}x</span></h2>
      <div class="golden-grid">${goldenCards}</div>
    </div>

    <div class="section">
      <h2 style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">Golden OPPO Optimizer <span style="font-size:11px;color:#5a6a85;font-weight:400">shadow + recommend-only; held-out Kraken validation proxy; never changes live settings</span><a href="/optimizer-history.csv" style="padding:4px 10px;background:#1e2533;border:1px solid #2a3347;color:#fbbf24;border-radius:6px;font-size:11px;text-decoration:none;font-family:monospace">Export 24h History CSV</a></h2>
      <table><thead><tr><th>Asset</th><th>Mode</th><th>Recommended golden settings</th><th>Validation reversal</th><th>Current validation</th><th>Candidates</th></tr></thead><tbody>${optimizerRows}</tbody></table>
    </div>

    <div class="section">
      <h2 style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">Standard OPPO Optimizer <span style="font-size:11px;color:#5a6a85;font-weight:400">robust score keeps valid 1x outcomes and limits outlier influence; never changes live settings</span><a href="/optimizer-configs.csv" style="padding:4px 10px;background:#1e2533;border:1px solid #2a3347;color:#60a5fa;border-radius:6px;font-size:11px;text-decoration:none;font-family:monospace">Export All Configs CSV</a></h2>
      <table><thead><tr><th>Mode</th><th>Recommended entry filters</th><th>Validation result</th><th>Current result</th><th>Pump samples</th><th>Candidates</th></tr></thead>
      <tbody><tr><td>${oto.mode||'shadow-recommend-only'}</td><td>${oppoOptimizerRecommendation}</td><td class="${(otr.score||0)>(otcur.score||0)?'green':'dim'}">${oppoOptimizerValidation}</td><td>${oppoOptimizerCurrent}</td><td>${optimizerQuality}</td><td>${oto.candidate_count||0}</td></tr></tbody></table>
    </div>

    <div class="section">
      <h2>Live Prices <span style="font-size:11px;color:#5a6a85;font-weight:400">buy zone ${(cfg.buy_min||0.82)*100|0}–${(cfg.buy_max||0.86)*100|0}¢ / RVOL avg ${cfg.volume_avg_period||20} candles, pass >${Number(cfg.rvol_min_per_min||0.0666).toFixed(4)}x × minute (1–15)</span></h2>
      <table><thead><tr><th>Asset</th><th>YES</th><th>NO</th><th>Kraken Gap / Threshold</th><th>CVD (win/slope)</th><th>RVOL</th><th>Holding</th><th>OPPO Trigger</th></tr></thead>
      <tbody>${priceRows}</tbody></table>
    </div>

    <div class="section"><h2>Open Positions (${Object.keys(pos).length})</h2>${posCards}</div>

    <div class="section">
      <h2>OPPO Trigger Log <span style="font-size:11px;color:#5a6a85;font-weight:400">(shows golden/buy/sell/cutloss/knife-block events)</span></h2>
      <div class="oppo-log-wrap" id="oppoLogWrap"><table><thead><tr><th>Time</th><th>Asset</th><th>Price</th><th>Status</th><th>Reason</th></tr></thead>
      <tbody>${oppoRows}</tbody></table></div>
    </div>

    <div class="section">
      <h2 style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">Trade Log <span style="font-size:11px;color:#5a6a85;font-weight:400">(${tLog.length} closed)</span><a href="/trade-log.csv" style="padding:4px 10px;background:#1e2533;border:1px solid #2a3347;color:#60a5fa;border-radius:6px;font-size:11px;text-decoration:none;font-family:monospace">Export CSV</a></h2>
      ${renderTradeLog(tLog)}
    </div>

    <div class="section">
      <h2 style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">Pump Tracker <span style="font-size:11px;color:#5a6a85;font-weight:400">tracks YES/NO prices from ${Math.round((cfg.pump_track_dead_zone_price||0.05)*100)}–${Math.round((cfg.pump_track_start_price||0.2)*100)}¢ until stop-buy ${cfg.stop_buy||840}s, milestones at 3x/4x/5x+</span><a href="/pump-log.csv" style="padding:4px 10px;background:#1e2533;border:1px solid #2a3347;color:#60a5fa;border-radius:6px;font-size:11px;text-decoration:none;font-family:monospace">Export CSV</a></h2>
      ${renderPumpTracker(pumpTrackers,pumpLog,cfg.pump_track_start_price||0.2,cfg.pump_track_dead_zone_price||0.05)}
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
        <tr><td>OPPO counter</td><td>${cfg.oppo_counter_enabled?'ON':'OFF'} buy ${(cfg.oppo_counter_min_price||0.05)*100|0}–${(cfg.oppo_counter_max_price||0.08)*100|0}¢ / sell x${Number(cfg.oppo_counter_sell_multiplier||1.4).toFixed(2)} cap ${((cfg.oppo_counter_sell_cap||0.94)*100|0)}¢ / cut ${((cfg.oppo_counter_cut_loss_pct||0.6)*100).toFixed(0)}%</td><td>Counter order</td><td>$${cfg.oppo_counter_buy_amount||cfg.order||2}</td></tr>
        <tr><td>Buy zone</td><td>${(cfg.buy_min||0)*100|0}–${(cfg.buy_max||0)*100|0}¢</td><td>Sell target</td><td>${cfg.sell_multiplier ? ('x'+Number(cfg.sell_multiplier).toFixed(2)+' (cap '+((cfg.sell_cap||0.99)*100|0)+'¢)') : (((cfg.sell||0.99)*100|0)+'¢')}</td></tr>
        <tr><td>OPPO rebound cap</td><td>Initial OPPO zone ${((cfg.oppo_min_price||0.03)*100).toFixed(0)}–${((cfg.oppo_max_price||0.15)*100).toFixed(0)}¢; tracked rebound can buy up to ${((cfg.oppo_rebound_max_price||0.25)*100).toFixed(0)}¢</td><td>Rebound</td><td>Requires x${Number(cfg.oppo_rebound_mult||2).toFixed(2)} from tracked trough</td></tr>
        <tr><td>OPPO RVOL guard</td><td>${cfg.oppo_rvol_guard_enabled?'ON':'OFF'} — current quote volume / avg ${cfg.volume_avg_period||20} candles</td><td>Pass / flexi</td><td>Minute threshold = ${Number(cfg.rvol_min_per_min||0.0666).toFixed(4)}x × current minute (1–15); below threshold → $${cfg.flexi_rvol_buy_amount||1} flexi order</td></tr>
        <tr><td>OPPO gap guards</td><td>Normal Kraken gap ratio x${Number(cfg.oppo_gap_mag||1).toFixed(2)}</td><td>Golden Kraken gap ratio</td><td>x${Number(cfg.oppo_golden_gap_mag||3).toFixed(2)} — golden blocks when current Kraken gap reaches this threshold</td></tr>
        <tr><td>OPPO knife guard</td><td>Blocks the whole asset for the current window after a pump+dump knife signal</td><td>Pass</td><td>Requires pump +$${Number(cfg.oppo_falling_knife_min_move||0.3).toFixed(2)} then peak drop -$${Number(cfg.oppo_falling_knife_min_move||0.3).toFixed(2)}</td></tr>
      </tbody></table>
    </div>

    <footer>Auto-refreshes every 2s &nbsp;&mdash;&nbsp; MomentumBot</footer>`;

  const wrap=document.getElementById('chartWrap');
  if(wrap)drawChart(pnlHist,wrap);
  const selected=(window.__emaAsset && assets.includes(window.__emaAsset))?window.__emaAsset:assets[0];
  window.__emaAsset=selected;
  const selectedCvd=(window.__cvdAsset && assets.includes(window.__cvdAsset))?window.__cvdAsset:assets[0];
  window.__cvdAsset=selectedCvd;
  const cvdWrap=document.getElementById('cvdChartWrap');
  if(cvdWrap)drawCvdChart(cvdHistory, selectedCvd, cvdWrap);
  const emaWrap=document.getElementById('emaChartWrap');
  if(emaWrap)drawEmaChart(krakenCandles[selected]||[], emaWrap);
  const oppoLogWrap=document.getElementById('oppoLogWrap');
  if(oppoLogWrap){
    oppoLogWrap.scrollTop=Math.min(oppoLogScrollTop, Math.max(0, oppoLogWrap.scrollHeight-oppoLogWrap.clientHeight));
  }
  restorePumpScroll();
  requestAnimationFrame(restorePumpScroll);
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
  try{
    const r=await fetch('/state');const d=await r.json();
    if(pumpScrollIsActive()){
      pendingPumpState=d;
    }else{
      render(pendingPumpState||d);
      pendingPumpState=null;
    }
  }catch(e){console.error('fetch error',e);}
  const el=document.getElementById('oppoResetConfirm');
  if(el)el.style.display=oppoResetConfirmOpen?'inline-flex':'none';
}
poll();setInterval(poll,2000);
</script></body></html>"""




def _trade_log_csv_bytes():
    import io
    import csv
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["time", "asset", "side", "entry", "target", "exit", "exit_px", "is_flip", "is_rebound", "is_counter", "pnl", "entry_rvol"])
    for t in trade_log:
        w.writerow([
            t.get("time", ""), t.get("asset", ""), t.get("side", ""),
            t.get("entry", ""), t.get("target", ""), t.get("exit", ""),
            t.get("exit_px", ""), t.get("is_flip", False), t.get("is_rebound", False),
            t.get("is_counter", False), t.get("pnl", ""), t.get("entry_rvol", ""),
        ])
    return buf.getvalue().encode("utf-8")


def _optimizer_history_csv_bytes():
    import io
    import csv
    columns = [
        "timestamp", "optimizer", "asset", "lookback", "min_high", "threshold",
        "gap_magnitude", "min_rvol", "max_kraken_gap_ratio", "min_rebound_ratio",
        "good_pump_entry_ratio_samples", "good_pump_entry_ratio_average", "good_pump_entry_ratio_median",
        "train_samples", "train_wins", "train_rate", "train_pnl",
        "validation_samples", "validation_wins", "validation_rate", "validation_pnl",
        "validation_average_max_multiple", "validation_median_max_multiple", "validation_highest_max_multiple",
        "current_train_samples", "current_train_wins", "current_train_rate", "current_train_pnl",
        "current_validation_samples", "current_validation_wins", "current_validation_rate",
        "current_validation_pnl", "current_validation_average_max_multiple", "current_validation_median_max_multiple",
        "current_validation_highest_max_multiple", "candidate_count", "score",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    for row in optimizer_recommendation_history:
        config = row.get("config") or {}
        writer.writerow({column: config.get(column, row.get(column, "")) for column in columns})
    return buf.getvalue().encode("utf-8")


def _optimizer_configs_csv_bytes():
    """Export every currently evaluated Standard OPPO optimizer configuration."""
    import io
    import csv
    snapshot = _build_oppo_trade_optimizer_snapshot()
    columns = [
        "rank", "recommended", "min_rvol", "max_kraken_gap_ratio", "good_pump_entry_ratio_median", "good_pump_entry_ratio_average", "good_pump_entry_ratio_samples", "score", "confidence", "stability_penalty",
        "train_samples", "train_median", "train_trimmed_average", "train_no_pump_rate", "train_weak_pumps", "train_weak_pump_rate", "train_rate_2x", "train_rate_4x",
        "validation_samples", "validation_median", "validation_trimmed_average", "validation_capped_average", "validation_weak_pumps", "validation_weak_pump_rate",
        "validation_highest", "validation_no_pump_rate", "validation_rate_1_5x", "validation_rate_2x",
        "validation_rate_3x", "validation_rate_4x", "validation_rate_5x",
    ]
    candidates = list(snapshot.get("candidates", []))
    candidates.sort(key=lambda item: (item.get("score") is not None, item.get("score") or float("-inf")), reverse=True)
    recommended = (snapshot.get("recommendation") or {}).get("config")
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns)
    writer.writeheader()
    for rank, candidate in enumerate(candidates, start=1):
        config, train, validation = candidate.get("config", {}), candidate.get("train", {}), candidate.get("validation", {})
        writer.writerow({
            "rank": rank, "recommended": config == recommended, "min_rvol": config.get("min_rvol"),
            "max_kraken_gap_ratio": config.get("max_kraken_gap_ratio"),
            "good_pump_entry_ratio_median": snapshot.get("good_pump_entry_ratio_median"),
            "good_pump_entry_ratio_average": snapshot.get("good_pump_entry_ratio_average"),
            "good_pump_entry_ratio_samples": snapshot.get("good_pump_entry_ratio_samples"),
            "score": candidate.get("score"),
            "confidence": candidate.get("confidence"), "stability_penalty": candidate.get("stability_penalty"),
            "train_samples": train.get("samples"), "train_median": train.get("median_max_multiple"),
            "train_trimmed_average": train.get("trimmed_average_max_multiple"), "train_no_pump_rate": train.get("no_pump_rate"),
            "train_weak_pumps": train.get("weak_pumps"), "train_weak_pump_rate": train.get("weak_pump_rate"),
            "train_rate_2x": train.get("rate_2x"), "train_rate_4x": train.get("rate_4x"),
            "validation_samples": validation.get("samples"), "validation_median": validation.get("median_max_multiple"),
            "validation_trimmed_average": validation.get("trimmed_average_max_multiple"),
            "validation_capped_average": validation.get("capped_average_max_multiple"),
            "validation_weak_pumps": validation.get("weak_pumps"), "validation_weak_pump_rate": validation.get("weak_pump_rate"),
            "validation_highest": validation.get("highest_max_multiple"), "validation_no_pump_rate": validation.get("no_pump_rate"),
            "validation_rate_1_5x": validation.get("rate_1_5x"), "validation_rate_2x": validation.get("rate_2x"),
            "validation_rate_3x": validation.get("rate_3x"), "validation_rate_4x": validation.get("rate_4x"),
            "validation_rate_5x": validation.get("rate_5x"),
        })
    return buf.getvalue().encode("utf-8")


def _pump_log_csv_bytes():
    import io
    import csv
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["time", "window_start", "asset", "side", "base_price", "trough", "current", "multiple", "max_price", "max_multiple", "kraken_gap", "kraken_gap_ratio", "cvd_slope", "rvol", "entry_at", "observation_secs", "price_updates", "entry_kraken_gap_ratio", "entry_cvd_slope", "entry_rvol", "status", "finish_reason", "milestone"])
    for e in pump_log:
        w.writerow([
            e.get("time", ""), e.get("window_start", ""), e.get("asset", ""),
            e.get("side", ""), e.get("base_price", ""), e.get("trough", ""),
            e.get("current", ""), e.get("multiple", ""), e.get("max_price", ""),
            e.get("max_multiple", ""), e.get("kraken_gap", ""), e.get("kraken_gap_ratio", ""), e.get("cvd_slope", ""),
            e.get("rvol", ""), e.get("entry_at", ""), e.get("observation_secs", ""), e.get("price_updates", ""), e.get("entry_kraken_gap_ratio", ""), e.get("entry_cvd_slope", ""), e.get("entry_rvol", ""), e.get("status", ""), e.get("finish_reason", ""), e.get("milestone", ""),
        ])
    return buf.getvalue().encode("utf-8")

class _Handler(BaseHTTPRequestHandler):
    def _safe_write(self, data, context):
        try:
            self.wfile.write(data)
            return True
        except (BrokenPipeError, ConnectionResetError):
            log.debug("[HTTP] Client disconnected while writing %s", context)
            return False

    def do_GET(self):
        if self.path == "/state":
            data = json.dumps(_build_state_snapshot(), indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self._safe_write(data, "/state")
        elif self.path in ("/", "/pnl"):
            data = _DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self._safe_write(data, "/")
        elif self.path == "/trade-log.csv":
            data = _trade_log_csv_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=trade_log.csv")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self._safe_write(data, "/trade-log.csv")
        elif self.path == "/optimizer-configs.csv":
            data = _optimizer_configs_csv_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=oppo_optimizer_all_configs.csv")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self._safe_write(data, "/optimizer-configs.csv")
        elif self.path == "/optimizer-history.csv":
            data = _optimizer_history_csv_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=optimizer_recommendations_24h.csv")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self._safe_write(data, "/optimizer-history.csv")
        elif self.path == "/pump-log.csv":
            data = _pump_log_csv_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=pump_log.csv")
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self._safe_write(data, "/pump-log.csv")
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
            self._safe_write(resp, "/reset")
            log.info("[HTTP] Dashboard reset by user")
        elif self.path == "/reset-oppo":
            reset_oppo_log()
            resp = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(resp))
            self.end_headers()
            self._safe_write(resp, "/reset-oppo")
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
    global last_pnl_snapshot, pnl_history, armed_logged

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
    log.info("  Force sell: pnl>0 and Kraken gap >= %.2fx staged threshold", FORCE_SELL_GAP_MULT)
    log.info("  EMA dashboard visualization: fast=%d slow=%d (not used for entry gating)", EMA_FAST_PERIOD, EMA_SLOW_PERIOD)
    log.info("  OPPO CVD gate (Kraken): enabled=%s  slope_polls=%d (YES slope>0, NO slope<0)", CVD_OPPO_ENABLED, CVD_OPPO_SLOPE_POLLS)
    log.info("  OPPO falling-knife guard: blacklist asset after pump +$%.2f and peak drop -$%.2f", OPPO_FALLING_KNIFE_MIN_MOVE, OPPO_FALLING_KNIFE_MIN_MOVE)
    log.info("  OPPO rebound: initial zone %.0f–%.0f¢  rebound max %.0f¢  rebound x%.2f", OPPO_MIN_PRICE * 100, OPPO_MAX_PRICE * 100, OPPO_REBOUND_MAX_PRICE * 100, OPPO_REBOUND_MULT)
    log.info(
        "  OPPO RVOL guard (Kraken): enabled=%s  flexi=%s  avg_period=%d  pass >%.4fx × minute (1–15), else order=$%.2f",
        OPPO_RVOL_GUARD_ENABLED, FLEXI_RVOL_ENABLED, VOLUME_AVG_PERIOD, RVOL_MIN_PER_MIN, FLEXI_RVOL_BUY_AMOUNT,
    )
    log.info(
        "  OPPO golden fourth-window: enabled=%s  prior-high=%d/%d > %.2fx  historical reversal >= %.0f%% over >=%d samples",
        OPPO_GOLDEN_RVOL_ENABLED, OPPO_GOLDEN_RVOL_MIN_HIGH, OPPO_GOLDEN_RVOL_LOOKBACK,
        OPPO_GOLDEN_RVOL_THRESHOLD, OPPO_GOLDEN_MIN_PROBABILITY * 100, OPPO_GOLDEN_MIN_SAMPLES,
    )
    log.info("  OPPO gap guards: normal magnitude=%.2fx  golden magnitude=%.2fx", OPPO_GAP_MAG, OPPO_GOLDEN_GAP_MAG)
    log.info("  OPPO optimizer: enabled=%s mode=shadow-recommend-only min_validation_samples=%d", OPPO_OPTIMIZER_ENABLED, OPPO_OPTIMIZER_MIN_VALIDATION_SAMPLES)
    log.info("  Standard OPPO pump optimizer: enabled=%s mode=shadow-recommend-only min_validation_samples=%d score_equivalence=%.2f pump_trace=%ds", OPPO_TRADE_OPTIMIZER_ENABLED, OPPO_TRADE_OPTIMIZER_MIN_VALIDATION_TRADES, OPPO_OPTIMIZER_SCORE_EQUIVALENCE, PUMP_TRACK_WINDOW_SECS)
    log.info("  OPPO counter: enabled=%s  buy %.0f–%.0f¢  sell=x%.2f cap %.0f¢  cut-loss=%.0f%%  order=$%.0f  entry %d–%ds",
             OPPO_COUNTER_ENABLED, OPPO_COUNTER_MIN_PRICE * 100, OPPO_COUNTER_MAX_PRICE * 100,
             OPPO_COUNTER_SELL_MULTIPLIER, OPPO_COUNTER_SELL_CAP * 100, OPPO_COUNTER_CUT_LOSS_PCT * 100,
             OPPO_COUNTER_BUY_AMOUNT, ENTRY_AFTER, STOP_BUY_AT)
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
    start_kraken_metrics_feed()   # Kraken WebSocket — populates candle_open + live_close + EMA + CVD + RVOL

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
                # Capture the final observable price before clearing prior-window
                # state so pump traces cover the complete 900-second market window.
                _finish_window_pump_trackers(last_window)
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
                normal_blacklisted_assets.clear()
                oppo_dead_zone_blacklisted_assets.clear()
                oppo_rvol_blacklisted_assets.clear()
                oppo_knife_blacklisted_assets.clear()
                trend_guarded_assets.clear()
                oppo_rebound_tracker.clear()
                oppo_counter_tracker.clear()
                oppo_cvd_polls.clear()
                oppo_dashboard_once_per_window.clear()
                rebound_cutloss_tracker.clear()
                pump_tracker.clear()
                pump_finished_tracker_keys.clear()
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

                scan_markets(client, window_start, secs_into, server_ts, executor)

            manage_positions(client, server_ts)
            for a in ASSETS:
                ema_fast, ema_slow = get_ema_snapshot(a)
                if ema_fast is not None and ema_slow is not None:
                    ema_history[a].append({
                        "ts": datetime.now().strftime("%H:%M:%S"),
                        "ema_fast": round(float(ema_fast), 4),
                        "ema_slow": round(float(ema_slow), 4),
                    })
                cvd_session, cvd_window, cvd_slope = get_cvd_snapshot(a)
                cvd_history[a].append({
                    "ts": datetime.now().strftime("%H:%M:%S"),
                    "session": round(float(cvd_session), 3),
                    "window": round(float(cvd_window), 3),
                    "slope": round(float(cvd_slope), 6),
                })
            _refresh_optimizer_recommendation_history()
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
