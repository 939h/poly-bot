"""
Polymarket 15-Min Up/Down Bot — fresh_bot23
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
        if any(t in msg for t in ("[BUY]", "[OPEN]", "[SELL]", "[WIN]", "[FORCE-GAP-SELL]", "[REBOUND-BUY]")):
            return Fore.GREEN + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[CUT-LOSS]", "[LOSS]", "[FORCE-STOP]")):
            return Fore.RED + Style.BRIGHT + msg + Style.RESET_ALL
        if any(t in msg for t in ("[FLIP]", "[REBOUND]")):
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

ASSETS         = ["btc", "eth", "sol"]

DRY_RUN        = os.getenv("DRY_RUN", "true").lower() != "false"
BUY_AMOUNT     = float(os.getenv("BUY_AMOUNT", "2"))   # USDC per trade

# ── Buy trigger ───────────────────────────────────────────────────────────────
BUY_PRICE_MIN  = 0.75   # buy if price >= this
BUY_PRICE_MAX  = 0.85   # buy if price <= this
ENTRY_AFTER    = 540    # seconds into window before buying allowed (10 min)
STOP_BUY_AT    = 780    # seconds into window after which no new buys (13 min)

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
    "early": 5.0,   # 0–5 min
    "mid":   0.6,   # 5–10 min
    "late":  0.6,   # 10–15 min
}
GAP_WAIT_SECS = 20   # wait this long for gap to widen before blacklisting

# ── Exit ──────────────────────────────────────────────────────────────────────
SELL_PRICE     = 0.96   # sell all at this price
FORCE_SELL_GAP_THRESHOLD = float(os.getenv("FORCE_SELL_GAP_THRESHOLD", "2"))
CUT_LOSS_PCT   = 0.60   # cut loss if price drops to this fraction of buy price
HOLD_EARLY_SECS = 60    # force-stop cooldown 0–5 min
HOLD_MID_SECS   = 30    # force-stop cooldown 5–10 min
HOLD_LATE_SECS  = 15    # force-stop cooldown 10–15 min

# ── Flip ──────────────────────────────────────────────────────────────────────
FLIP_MIN       = 0.50   # flip only if opposite >= this
FLIP_MAX       = 0.75   # flip only if opposite <= this

# ── Rebound cut-loss flip ─────────────────────────────────────────────────────
REBOUND_CUTLOSS_MULTIPLIER = float(os.getenv("REBOUND_CUTLOSS_MULTIPLIER", "1.5"))
REBOUND_BUY_CAP_PRICE      = float(os.getenv("REBOUND_BUY_CAP_PRICE", "0.40"))
REBOUND_DISCARD_PRICE      = float(os.getenv("REBOUND_DISCARD_PRICE", "0.05"))

# ── Spread guard ─────────────────────────────────────────────────────────────
MAX_BOOK_SPREAD        = 0.02
SPREAD_MAX_RETRIES     = 10
FORCE_STOP_SPREAD_RETRIES = 10

# ── Timing ────────────────────────────────────────────────────────────────────
POLL_SECS              = 1.0
WINDOW_SECS            = 900

# ── Trading windows (optional) ────────────────────────────────────────────────
TRADING_WINDOWS_ENABLED = False
TRADING_TZ_OFFSET_HRS   = 8
TRADING_WINDOWS         = [(8, 30, 9, 30), (16, 17), (21, 22)]

# ── Misc ──────────────────────────────────────────────────────────────────────
EXIT_RETRY_COOLDOWN_SECS = 1
SELL_MAX_ATTEMPTS        = 5
SELL_RETRY_DELAY_SECS    = 0.5
MIN_SELL_SHARES          = 1
FEE_BUFFER               = 1.00

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
traded_this_window = set()
gap_wait           = {}   # asset -> {triggered_at, key, token, price}
rebound_watch      = {}   # asset -> {key, token, low, window_start, spread_retries}
armed_logged       = False

pnl_history        = []
asset_history      = {}
trade_log          = []
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
    global stats, pnl_history, asset_history, trade_log, last_pnl_snapshot, rebound_watch
    stats = {"scans": 0, "triggers": 0, "buys": 0, "wins": 0, "losses": 0, "pnl": 0.0}
    pnl_history   = []
    asset_history = {}
    trade_log     = []
    rebound_watch = {}
    last_pnl_snapshot = 0
    log.info("[STATE] Reset by user")
    save_state()


def save_state():
    positions_out = {}
    for k, p in open_positions.items():
        entry    = p["entry_price"]
        curr     = live_prices.get(k, entry)
        target   = p["sell_price"]
        cut      = p["cut_loss_price"]
        pnl_unreal = round((curr - entry) * p["net_shares"], 4)
        pct      = round((curr - entry) / (target - entry) * 100, 1) if target != entry else 0
        positions_out[k] = {
            "entry":       round(entry, 4),
            "current":     round(curr, 4),
            "target":      round(target, 4),
            "cut_loss":    round(cut, 4),
            "is_flip":     p.get("is_flip", False),
            "pnl":         pnl_unreal,
            "pct":         max(0, min(100, pct)),
            "opened_at":   p.get("opened_at", "—"),
        }
    gap_out = {}
    for a in ASSETS:
        c_open = candle_open.get(a, 0.0)
        c_live = live_close.get(a)
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
        "rebound_watch": dict(rebound_watch),
        "pnl_history":   list(pnl_history),
        "asset_history": dict(asset_history),
        "trade_log":     list(trade_log),
        "settings": {
            "assets":     ASSETS,
            "buy_min":    BUY_PRICE_MIN,
            "buy_max":    BUY_PRICE_MAX,
            "sell":       SELL_PRICE,
            "force_sell_gap_threshold": FORCE_SELL_GAP_THRESHOLD,
            "cut_loss":   CUT_LOSS_PCT,
            "flip_min":   FLIP_MIN,
            "flip_max":   FLIP_MAX,
            "rebound_multiplier": REBOUND_CUTLOSS_MULTIPLIER,
            "rebound_buy_cap": REBOUND_BUY_CAP_PRICE,
            "rebound_discard": REBOUND_DISCARD_PRICE,
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
        "entry":    round(pos["entry_price"], 4),
        "target":   round(pos["sell_price"], 4),
        "exit":     exit_type,
        "exit_px":  round(close_price, 4),
        "is_flip":  pos.get("is_flip", False),
        "pnl":      round(pnl, 4),
    }
    trade_log.insert(0, record)
    if len(trade_log) > 200:
        trade_log.pop()

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


def market_buy(client, token_id, label, price_hint=None):
    amount = round(BUY_AMOUNT, 4)
    if DRY_RUN:
        entry_est = float(price_hint or 0) or get_midpoint(client, token_id)
        est_shares = round(max((amount / entry_est) * FEE_BUFFER, 0.0), 4) if entry_est > 0 else 0
        log.info("[DRY-RUN] MARKET BUY %s $%.2f USDC → est %d shares @ %.4f",
                 label, amount, est_shares, entry_est)
        return {
            "ok": True, "resp": {"dry_run": True},
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
        filled_shares = round(max(raw_taking, 0.0), 4)
        filled_price  = (raw_making / raw_taking) if raw_taking > 0 and raw_making > 0 else 0.0
        if raw_taking == 0 and raw_making == 0:
            log.warning("[BUY] %s FOK zero fill — order not executed", label)
            return {"ok": False, "resp": resp, "filled_shares": 0, "filled_price": 0.0}
        if filled_shares <= 0:
            entry_est = float(price_hint or 0) or get_midpoint(client, token_id)
            filled_shares = round(max((amount / entry_est) * FEE_BUFFER, 0.0), 4) if entry_est > 0 else 0
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


def market_sell(client, token_id, shares, price, label):
    sell_shares = round(max(shares, 0.0), 4)
    if sell_shares < MIN_SELL_SHARES:
        return {"ok": False, "resp": None, "filled_shares": 0, "filled_quote": 0.0}
    if DRY_RUN:
        est = round(sell_shares * price, 4)
        log.info("[DRY-RUN] MARKET SELL %s %d sh @ %.4f (est $%.4f)", label, sell_shares, price, est)
        return {"ok": True, "resp": {"dry_run": True}, "filled_shares": sell_shares, "filled_quote": est}
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
            filled_sh   = round(max(raw_making, 0.0), 4)
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
                        retry = round(max((attempt_shares * ratio) * 0.999, 0.0), 4)
                        if retry >= MIN_SELL_SHARES:
                            log.warning("[SELL] %s size %d → %d to match balance", label, attempt_shares, retry)
                            attempt_shares = retry
                            continue
            log.error("[SELL] Failed %s: %s", label, e)
            return {"ok": False, "resp": None, "filled_shares": 0, "filled_quote": 0.0}
    return {"ok": False, "resp": None, "filled_shares": 0, "filled_quote": 0.0}


def market_sell_with_retries(client, token_id, shares, price, label):
    last = {"ok": False, "resp": None, "filled_shares": 0}
    for attempt in range(1, SELL_MAX_ATTEMPTS + 1):
        last = market_sell(client, token_id, shares, price, label)
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

    if secs_into < 300:
        stage = "early"
    elif secs_into < 600:
        stage = "mid"
    else:
        stage = "late"

    swing     = GAP_SWING.get(asset, 0.001)
    magnitude = GAP_MAGNITUDE[stage]
    threshold = c_open * swing * magnitude
    actual    = abs(c_live - c_open)

    log.info(
        "[GAP-GUARD] %s  open=%.4f  live=%.4f  actual=%.4f  threshold=%.4f"
        "  (swing=%.4f × mag=%.1f  stage=%s)",
        asset.upper(), c_open, c_live, actual, threshold, swing, magnitude, stage,
    )

    if actual >= threshold:
        log.info("[GAP-ALLOW] %s  gap %.4f >= threshold %.4f → buy allowed",
                 asset.upper(), actual, threshold)
        return True   # gap large enough — allow

    return False   # gap too small — wait/block


def get_binance_gap(asset):
    """Return the absolute Binance live-vs-open gap for an asset, or None if unavailable."""
    c_open = candle_open.get(asset, 0.0)
    c_live = live_close.get(asset)
    if c_open <= 0.0 or c_live is None:
        return None
    return abs(c_live - c_open)

# ── Position management ───────────────────────────────────────────────────────

def start_rebound_watch(key, pos, current_price):
    asset = key.split("_")[0]
    if current_price < REBOUND_DISCARD_PRICE:
        log.info(
            "[REBOUND-DISCARD] %s  price=%.4f below discard=%.4f — no rebound buy",
            key, current_price, REBOUND_DISCARD_PRICE,
        )
        traded_this_window.add(asset)
        return

    rebound_watch[asset] = {
        "key":            key,
        "token":          pos["token_id"],
        "low":            round(current_price, 4),
        "window_start":   pos.get("window_start"),
        "spread_retries": 0,
    }
    traded_this_window.add(asset)
    log.info(
        "[REBOUND-WATCH] %s  low=%.4f  trigger=>%.4f  buy_cap<%.4f  discard<%.4f",
        key, current_price, current_price * REBOUND_CUTLOSS_MULTIPLIER,
        REBOUND_BUY_CAP_PRICE, REBOUND_DISCARD_PRICE,
    )


def process_rebound_watches(client, window_start):
    for asset in list(rebound_watch.keys()):
        rw = rebound_watch[asset]
        if rw.get("window_start") != window_start:
            log.info("[REBOUND-DISCARD] %s  expired with prior window", rw["key"])
            del rebound_watch[asset]
            continue

        key = rw["key"]
        if key in open_positions:
            del rebound_watch[asset]
            continue

        token = rw["token"]
        current_price = live_prices.get(key)
        if current_price is None or current_price <= 0:
            current_price = get_midpoint(client, token)
        if current_price <= 0:
            continue

        if current_price < REBOUND_DISCARD_PRICE:
            log.info(
                "[REBOUND-DISCARD] %s  price=%.4f below discard=%.4f — no rebound buy",
                key, current_price, REBOUND_DISCARD_PRICE,
            )
            del rebound_watch[asset]
            continue

        low = rw.get("low", current_price)
        if current_price < low:
            rw["low"] = round(current_price, 4)
            low = current_price
            log.info("[REBOUND-LOW] %s  new low=%.4f", key, low)
            continue

        trigger_price = low * REBOUND_CUTLOSS_MULTIPLIER
        if current_price > trigger_price:
            if current_price >= REBOUND_BUY_CAP_PRICE:
                log.info(
                    "[REBOUND-DISCARD] %s  price=%.4f reached buy cap=%.4f — no rebound buy",
                    key, current_price, REBOUND_BUY_CAP_PRICE,
                )
                del rebound_watch[asset]
                continue

            spread = get_spread_value(client, token)
            if spread is not None and spread > MAX_BOOK_SPREAD:
                rw["spread_retries"] = rw.get("spread_retries", 0) + 1
                if rw["spread_retries"] < SPREAD_MAX_RETRIES:
                    log.info(
                        "[REBOUND-SPREAD-WAIT] %s  spread=%.4f  retry %d/%d",
                        key, spread, rw["spread_retries"], SPREAD_MAX_RETRIES,
                    )
                    continue
                log.info("[REBOUND-DISCARD] %s  spread still wide — no rebound buy", key)
                del rebound_watch[asset]
                continue

            log.info(
                "[REBOUND-BUY] %s  price=%.4f rebounded from low=%.4f by %.2fx",
                key, current_price, low, current_price / low if low > 0 else 0,
            )
            buy = market_buy(client, token, f"{asset.upper()}-{key.split('_')[1].upper()}-REBOUND",
                             price_hint=current_price)
            del rebound_watch[asset]
            if buy["ok"]:
                entry_px = float(buy.get("filled_price") or current_price)
                open_position(key, token, entry_px,
                              filled_shares=buy.get("filled_shares"),
                              window_start=window_start,
                              is_flip=True)
                live_prices[key] = current_price


def open_position(key, token_id, entry_price, filled_shares=None, window_start=None, is_flip=False):
    if filled_shares is not None and filled_shares > 0:
        net_shares = round(max(float(filled_shares), 0.0), 4)
    else:
        net_shares = round(max((BUY_AMOUNT / entry_price) * FEE_BUFFER, 0.0), 4)

    cut_loss_price = round(entry_price * CUT_LOSS_PCT, 4)

    open_positions[key] = {
        "token_id":             token_id,
        "entry_price":          entry_price,
        "sell_price":           SELL_PRICE,
        "cut_loss_price":       cut_loss_price,
        "net_shares":           net_shares,
        "cost":                 round(BUY_AMOUNT, 4),
        "realized_revenue":     0.0,
        "is_flip":              is_flip,
        "force_stop_triggered": None,
        "force_stop_cooldown":  None,
        "force_stop_spread_retries": 0,
        "last_exit_attempt_ts": 0.0,
        "opened_at":            datetime.now().strftime("%H:%M"),
        "window_start":         window_start,
    }
    stats["buys"] += 1
    tag = "FLIP " if is_flip else ""
    log.info(
        "[OPEN] %s%s  entry=%.4f  shares=%d  sell=%.4f  cut-loss=%.4f",
        tag, key, entry_price, net_shares, SELL_PRICE, cut_loss_price,
    )


def manage_positions(client, server_ts=None):
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

        entry     = pos["entry_price"]
        shares    = pos["net_shares"]
        cut_loss  = pos["cut_loss_price"]
        is_flip   = pos.get("is_flip", False)
        asset     = key.split("_")[0]
        unrealized_pnl = round((current_price - entry) * shares, 4)
        binance_gap = get_binance_gap(asset)

        # ── Force sell on profit + large Binance gap ──────────────────────────
        if (
            unrealized_pnl > 0
            and binance_gap is not None
            and binance_gap > FORCE_SELL_GAP_THRESHOLD
        ):
            log.info(
                "[FORCE-GAP-SELL] %s  pnl=$%.4f  binance_gap=%.4f > threshold=%.4f  selling %d shares",
                key, unrealized_pnl, binance_gap, FORCE_SELL_GAP_THRESHOLD, shares,
            )
            sell = market_sell_with_retries(client, pos["token_id"], shares, current_price, key.upper())
            pos["last_exit_attempt_ts"] = time.time()
            if sell["ok"]:
                revenue = float(sell.get("filled_quote") or round(shares * current_price, 4))
                pos["realized_revenue"] = round(pos.get("realized_revenue", 0.0) + revenue, 4)
                pnl = round(pos["realized_revenue"] - pos["cost"], 4)
                log.info("[FORCE-GAP-SELL] %s finalized  pnl=$%.4f", key, pnl)
                stats["wins" if pnl > 0 else "losses"] += 1
                stats["pnl"] += pnl
                _record_closed_trade(key, pnl)
                _record_trade_log(key, pos, "FORCE-GAP-SELL", current_price, pnl)
                to_close.append(key)
            continue

        # ── Force stop (cut-loss) with cooldown ───────────────────────────────
        if current_price <= cut_loss and not is_flip:
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
                server_ts_now = server_ts if server_ts is not None else get_server_time()
                secs_in = server_ts_now - get_current_window_start(server_ts_now)
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
            log.info("[CUT-LOSS] %s  price=%.4f  selling %d shares", key, current_price, shares)
            sell = market_sell(client, pos["token_id"], shares, current_price, key.upper())
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
                to_close.append(key)

                # ── Rebound cut-loss flip — watch same side for a bounce ───────
                start_rebound_watch(key, pos, current_price)
            continue

        # Price recovered — reset cooldown
        if pos.get("force_stop_triggered") is not None:
            log.info("[STOP-CANCEL] %s  price=%.4f recovered above cut=%.4f",
                     key, current_price, cut_loss)
            pos["force_stop_triggered"] = None
            pos["force_stop_cooldown"]  = None
            pos["force_stop_spread_retries"] = 0

        # ── Sell at target ────────────────────────────────────────────────────
        if current_price >= SELL_PRICE:
            tag = "FLIP-SELL" if is_flip else "SELL"
            log.info("[%s] %s  price=%.4f  selling %d shares", tag, key, current_price, shares)
            sell = market_sell_with_retries(client, pos["token_id"], shares, current_price, key.upper())
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

    for key in to_close:
        del open_positions[key]

# ── Market fetch + scan ───────────────────────────────────────────────────────

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

    if not can_open_new_trades(server_ts):
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

    # ── Advance rebound cut-loss watches ──────────────────────────────────────
    process_rebound_watches(client, window_start)

    # ── Advance gap_wait — check if gap has widened ───────────────────────────
    for asset in list(gap_wait.keys()):
        if asset in traded_this_window:
            del gap_wait[asset]
            continue

        gw      = gap_wait[asset]
        elapsed = time.time() - gw["triggered_at"]

        if check_gap_guard(asset, secs_into):
            # Gap is now large enough — proceed to buy
            log.info("[GAP-CLEARED] %s  gap widened after %.1fs — proceeding to buy",
                     asset.upper(), elapsed)
            key   = gw["key"]
            token = gw["token"]
            price = live_prices.get(key, gw["price"])
            label = f"{asset.upper()}-{key.split('_')[1].upper()}"

            # Spread check before buy
            spread = get_spread_value(client, token)
            if spread is not None and spread > MAX_BOOK_SPREAD:
                gw["spread_retries"] = gw.get("spread_retries", 0) + 1
                if gw["spread_retries"] < SPREAD_MAX_RETRIES:
                    log.info("[SPREAD-WAIT] %s  spread=%.4f  retry %d/%d",
                             asset.upper(), spread, gw["spread_retries"], SPREAD_MAX_RETRIES)
                    continue
                log.info("[SPREAD-SKIP] %s  spread still wide — blacklisting", asset.upper())
                traded_this_window.add(asset)
                del gap_wait[asset]
                continue

            buy = market_buy(client, token, label, price_hint=price)
            del gap_wait[asset]
            if buy["ok"]:
                entry_px = float(buy.get("filled_price") or price)
                open_position(key, token, entry_px,
                              filled_shares=buy.get("filled_shares"),
                              window_start=window_start)
                traded_this_window.add(asset)
            continue

        if elapsed >= GAP_WAIT_SECS:
            log.info("[GAP-BLOCK] %s  gap still too small after %.1fs — blacklisted",
                     asset.upper(), elapsed)
            traded_this_window.add(asset)
            del gap_wait[asset]
        # else: still waiting, no log spam

    # ── Evaluate fresh buy triggers ───────────────────────────────────────────
    if secs_into < ENTRY_AFTER or secs_into > STOP_BUY_AT:
        return

    for asset in ASSETS:
        if asset in traded_this_window:
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

        stats["triggers"] += 1
        log.info("[TRIGGER] %s  price=%.4f  checking gap guard", triggered_key, triggered_price)

        if check_gap_guard(asset, secs_into):
            # Gap large enough — check spread then buy immediately
            spread = get_spread_value(client, triggered_token)
            if spread is not None and spread > MAX_BOOK_SPREAD:
                log.info("[SPREAD-SKIP] %s  spread=%.4f > max=%.4f — skipping",
                         triggered_key, spread, MAX_BOOK_SPREAD)
                continue
            label = f"{asset.upper()}-{triggered_key.split('_')[1].upper()}"
            buy = market_buy(client, triggered_token, label, price_hint=triggered_price)
            if buy["ok"]:
                entry_px = float(buy.get("filled_price") or triggered_price)
                open_position(triggered_key, triggered_token, entry_px,
                              filled_shares=buy.get("filled_shares"),
                              window_start=window_start)
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
            log.info("[GAP-WAIT] %s  gap too small — waiting %.0fs for momentum  price=%.4f",
                     asset.upper(), GAP_WAIT_SECS, triggered_price)

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
        curr    = live_prices.get(k, entry)
        target  = p["sell_price"]
        cut     = p["cut_loss_price"]
        shares  = p["net_shares"]
        pnl_unreal = round((curr - entry) * shares, 4)
        pct = round((curr - entry) / (target - entry) * 100, 1) if target != entry else 0
        positions_out[k] = {
            "entry":     round(entry, 4),
            "current":   round(curr, 4),
            "target":    round(target, 4),
            "cut_loss":  round(cut, 4),
            "is_flip":   p.get("is_flip", False),
            "pnl":       pnl_unreal,
            "pct":       max(0, min(100, pct)),
            "opened_at": p.get("opened_at", "—"),
        }
    now_ts  = int(time.time())
    slot_ts = (now_ts // 900) * 900
    secs_in = now_ts - slot_ts
    gap_out = {}
    for a in ASSETS:
        c_open = candle_open.get(a, 0.0)
        c_live = live_close.get(a)
        gap_out[a] = round(abs(c_live - c_open), 4) if c_open > 0 and c_live is not None else None
    return {
        "updated":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dry_run":       DRY_RUN,
        "stats":         dict(stats),
        "positions":     positions_out,
        "prices":        dict(live_prices),
        "gap":           gap_out,
        "window": {
            "secs_into": secs_in,
            "secs_left": 900 - secs_in,
            "period":    "early" if secs_in < 300 else ("mid" if secs_in < 600 else "late"),
        },
        "pnl_history":   list(pnl_history),
        "asset_history": dict(asset_history),
        "trade_log":     list(trade_log),
        "rebound_watch": dict(rebound_watch),
        "settings": {
            "assets":     ASSETS,
            "buy_min":    BUY_PRICE_MIN,
            "buy_max":    BUY_PRICE_MAX,
            "sell":       SELL_PRICE,
            "force_sell_gap_threshold": FORCE_SELL_GAP_THRESHOLD,
            "cut_loss":   CUT_LOSS_PCT,
            "flip_min":   FLIP_MIN,
            "flip_max":   FLIP_MAX,
            "rebound_multiplier": REBOUND_CUTLOSS_MULTIPLIER,
            "rebound_buy_cap": REBOUND_BUY_CAP_PRICE,
            "rebound_discard": REBOUND_DISCARD_PRICE,
            "order":      BUY_AMOUNT,
            "poll":       POLL_SECS,
            "entry_after": ENTRY_AFTER,
            "stop_buy":   STOP_BUY_AT,
        },
    }


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fresh Bot 23</title>
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
    const col={SELL:'#0d2a1e','FLIP-SELL':'#0d1a2a','FORCE-GAP-SELL':'#24150a','CUT-LOSS':'#2a0d0d'}[e]||'#2a0d0d';
    const tc={'SELL':'#4ade9f','FLIP-SELL':'#60a5fa','FORCE-GAP-SELL':'#f59e0b','CUT-LOSS':'#f87171'}[e]||'#f87171';
    const bc={'SELL':'#1a5c3a','FLIP-SELL':'#1a3a5c','FORCE-GAP-SELL':'#7c4a03','CUT-LOSS':'#5c1d1d'}[e]||'#5c1d1d';
    return `<span class="badge" style="background:${col};color:${tc};border:1px solid ${bc}">${e}</span>`;
  };
  const rows=log.map((t,i)=>{
    const p=t.pnl||0,ps=(p>=0?'+':'')+p.toFixed(4);
    const flipTag=t.is_flip?'<span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c;font-size:10px;margin-left:4px">FLIP</span>':'';
    return `<tr class="tl-row" style="${i>=TL_COLLAPSE&&!_tlExpanded?'display:none':''}">
      <td>${t.time||'—'}</td>
      <td><strong>${t.asset}-${t.side}</strong>${flipTag}</td>
      <td>${fmt(t.entry)}</td>
      <td>${fmt(t.target,2)}</td>
      <td>${exitBadge(t.exit)}</td>
      <td>${fmt(t.exit_px)}</td>
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
  const st=s.stats||{},pos=s.positions||{},pr=s.prices||{};
  const cfg=s.settings||{},w=s.window||{},gap=s.gap||{};
  const pnlHist=s.pnl_history||[],assetHist=s.asset_history||{},tLog=s.trade_log||[];
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
    const holding=[(a+'_yes' in pos)?'<span class="green">YES</span>':'',(a+'_no' in pos)?'<span class="green">NO</span>':''].filter(Boolean).join(' ');
    const gv=gap[a];const gStr=gv!=null?gv.toFixed(4):'—';
    return`<tr><td>${a.toUpperCase()}</td><td class="${yc}">${fmt(yp,2)}</td><td class="${nc}">${fmt(np,2)}</td><td style="font-family:monospace">${gStr}</td><td>${holding||'<span class="dim">—</span>'}</td></tr>`;
  }).join('');

  const posCards=Object.entries(pos).map(([k,p])=>{
    const [asset,side]=k.split('_');
    const col=p.current>=p.entry?'green':'red';
    const flipBadge=p.is_flip?'<span class="badge" style="background:#0d1e2a;color:#60a5fa;border:1px solid #1a3a5c;margin-left:6px">FLIP</span>':'';
    const pnlV=p.pnl||0;
    return`<div class="pos-card">
      <div class="pos-hdr"><strong>${asset.toUpperCase()}-${side.toUpperCase()}</strong>${flipBadge}<span style="font-size:12px;color:#5a6a85">opened ${p.opened_at||'—'}</span></div>
      <div style="display:flex;gap:16px;font-size:13px;flex-wrap:wrap">
        <span><span class="dim">entry</span> <strong>${fmt(p.entry)}</strong></span>
        <span><span class="dim">current</span> <strong class="${col}">${fmt(p.current)}</strong></span>
        <span><span class="dim">sell @</span> <strong class="green">${fmt(p.target,2)}</strong></span>
        <span><span class="dim">cut @</span> <strong class="red">${fmt(p.cut_loss)}</strong></span>
      </div>
      <div class="bar-bg"><div class="bar-fill" style="width:${p.pct||0}%;background:${p.current>=p.entry?'#1db87a':'#e24b4a'}"></div></div>
      <div class="pos-meta">
        <span>${(p.pct||0).toFixed(0)}% to target</span>
        <span>Unrealised: ${fmtPnl(pnlV)}</span>
      </div>
    </div>`;
  }).join('')||'<p class="dim" style="padding:8px 0">No open positions</p>';

  document.getElementById('root').innerHTML=`
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;flex-wrap:wrap;gap:8px">
      <div><strong style="font-size:18px">Fresh<span class="green">Bot23</span></strong> &nbsp; ${mode}</div>
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
      <table><thead><tr><th>Asset</th><th>YES</th><th>NO</th><th>Binance Gap</th><th>Holding</th></tr></thead>
      <tbody>${priceRows}</tbody></table>
    </div>

    <div class="section"><h2>Open Positions (${Object.keys(pos).length})</h2>${posCards}</div>

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
        <tr><td>Buy zone</td><td>${(cfg.buy_min||0)*100|0}–${(cfg.buy_max||0)*100|0}¢</td><td>Sell at</td><td>${(cfg.sell||0.99)*100|0}¢</td></tr>
        <tr><td>Cut loss</td><td>${((cfg.cut_loss||0.6)*100).toFixed(0)}% of entry</td><td>Force sell gap</td><td>${cfg.force_sell_gap_threshold ?? 2}</td></tr>
        <tr><td>Flip range</td><td>${(cfg.flip_min||0.5)*100|0}–${(cfg.flip_max||0.75)*100|0}¢</td><td>Order size</td><td>$${cfg.order||2}</td></tr>
        <tr><td>Rebound flip</td><td>&gt;${cfg.rebound_multiplier ?? 1.5}x from low</td><td>Buy cap</td><td>&lt;${fmt(cfg.rebound_buy_cap ?? 0.4)}</td></tr>
        <tr><td>Discard below</td><td>${fmt(cfg.rebound_discard ?? 0.05)}</td><td></td><td></td></tr>
        <tr><td>Poll</td><td>${cfg.poll||2}s</td><td></td><td></td></tr>
        <tr><td>Entry window</td><td>${(cfg.entry_after||600)/60|0}–${(cfg.stop_buy||780)/60|0} min</td><td></td><td></td></tr>
      </tbody></table>
    </div>

    <footer>Auto-refreshes every 5s &nbsp;&mdash;&nbsp; FreshBot23</footer>`;

  const wrap=document.getElementById('chartWrap');
  if(wrap)drawChart(pnlHist,wrap);
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
async function poll(){
  try{const r=await fetch('/state');const d=await r.json();render(d);}
  catch(e){console.error('fetch error',e);}
}
poll();setInterval(poll,5000);
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

    load_state()
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    log.info("=" * 55)
    log.info("  FreshBot23  [%s]", mode)
    log.info("  Assets : %s", ", ".join(a.upper() for a in ASSETS))
    log.info("  Buy zone: %.0f–%.0f¢  entry %d–%ds  sell=%.0f¢  cut-loss=%.0f%%",
             BUY_PRICE_MIN*100, BUY_PRICE_MAX*100,
             ENTRY_AFTER, STOP_BUY_AT,
             SELL_PRICE*100, CUT_LOSS_PCT*100)
    log.info("  Flip: %.0f–%.0f¢  order=$%.0f  poll=%.1fs",
             FLIP_MIN*100, FLIP_MAX*100, BUY_AMOUNT, POLL_SECS)
    log.info("  Force gap sell: pnl > $0 and Binance gap > %.4f", FORCE_SELL_GAP_THRESHOLD)
    log.info("  Rebound flip: buy same side after >%.2fx rebound below %.4f; discard below %.4f",
             REBOUND_CUTLOSS_MULTIPLIER, REBOUND_BUY_CAP_PRICE, REBOUND_DISCARD_PRICE)
    log.info("  Gap guard: swing=%s  magnitude=%s  wait=%ds",
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
                gap_wait.clear()
                rebound_watch.clear()
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
