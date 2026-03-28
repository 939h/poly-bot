"""
Polymarket 15-Min Up/Down Bot — Fresh v7
Markets: BTC, ETH, SOL, XRP
With Google Sheets PnL + Railway deployment

Strategy:
  1. Buy YES or NO if price hits 80-85c between min 10-14
  2. Watch opposite side — buy 20 insurance shares when opposite <= 1.5c
  3. Sell insurance at 20c
  4. Sell main shares at 95c, cut loss at 50% of buy price
  5. Orderbook recorded silently to log file

Requirements:
    pip install py-clob-client python-dotenv requests colorama gspread google-auth
"""

import os
import sys
import csv
import json
import time
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType, ApiCreds
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client python-dotenv requests gspread")
    exit(1)

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_OK = True
except ImportError:
    GSHEETS_OK = False
    print("Warning: gspread not installed — Google Sheets disabled")

load_dotenv()

# ── Color Logging ─────────────────────────────────────────────────────────────

try:
    import colorama
    from colorama import Fore, Style
    colorama.init()
    COLORS = True
except ImportError:
    COLORS = False

class ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if not COLORS:
            return msg
        if "BUY" in msg:
            return Fore.GREEN + msg + Style.RESET_ALL
        elif "SELL" in msg and "95" in msg:
            return Fore.YELLOW + msg + Style.RESET_ALL
        elif "WIN" in msg:
            return Fore.GREEN + Style.BRIGHT + msg + Style.RESET_ALL
        elif "LOSS" in msg:
            return Fore.RED + Style.BRIGHT + msg + Style.RESET_ALL
        elif "TRIGGER" in msg:
            return Fore.CYAN + msg + Style.RESET_ALL
        elif "ERROR" in msg:
            return Fore.RED + msg + Style.RESET_ALL
        return msg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("fresh_bot7.log"),
    ],
    force=True,
)
for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
        handler.setFormatter(ColorFormatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ── Silent Orderbook Logger (file only, no CMD) ───────────────────────────────

ob_logger = logging.getLogger("orderbook")
ob_logger.setLevel(logging.INFO)
ob_logger.propagate = False  # never show in CMD
_ob_handler = logging.FileHandler("fresh_bot7.log")
_ob_handler.setFormatter(logging.Formatter("%(asctime)s [ORDERBOOK] %(message)s"))
ob_logger.addHandler(_ob_handler)

def record_orderbook(asset, yes_price, no_price):
    """Record YES/NO prices silently to log file only."""
    ob_logger.info(f"{asset.upper()} | YES={yes_price:.4f} NO={no_price:.4f}")

# ── Settings ──────────────────────────────────────────────────────────────────

DRY_RUN         = True
ASSETS          = ["btc", "eth", "sol", "xrp"]
BUY_SHARES      = 10
BUY_PRICE_MIN   = 0.80    # Buy if price >= 80c
BUY_PRICE_MAX   = 0.85    # Buy if price <= 85c
SELL_PRICE      = 0.99    # Sell main shares at 95c
INS_SHARES      = 20      # Insurance shares (opposite side)
INS_MAX_PRICE   = 0.015   # Buy insurance only if price <= 1.5c
ENTRY_AFTER     = 600     # Start buying after 10 minutes (600s)
STOP_BUY_AT     = 840     # Stop buying after 14 minutes (840s)
WINDOW_SECS     = 900     # 15-minute window
POLL_SECS       = 1

GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
PNL_FILE    = "fresh_bot7_pnl.csv"
SHEET_ID    = os.getenv("GOOGLE_SHEET_ID", "1X9EVZBcMUvRuloX2fi71SajlbpmVglSzAhfdsN6AGk4")
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "")
GEMINI_URL  = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

# ── Gemini AI Decision ────────────────────────────────────────────────────────

# Price history: (asset, window) -> list of (timestamp, yes_price, no_price)
price_history = {}

def record_price_history(asset, window, yes_price, no_price):
    key = (asset, window)
    if key not in price_history:
        price_history[key] = []
    price_history[key].append((int(time.time()), yes_price, no_price))
    # Keep last 60 entries (60 seconds)
    if len(price_history[key]) > 60:
        price_history[key].pop(0)


def gemini_decide(asset, side, current_price, secs_into, secs_left):
    """Ask Gemini AI whether to BUY or SKIP using Polymarket + Binance data."""
    # This is now defined after Binance indicators section below
    pass


# ── Binance Indicators ────────────────────────────────────────────────────────

BINANCE_SYMBOLS = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
}

def fetch_binance_candles(asset, interval="1m", limit=20):
    """Fetch last N candles from Binance public API. No key needed."""
    symbol = BINANCE_SYMBOLS.get(asset.lower())
    if not symbol:
        return []
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=5,
        )
        # Each candle: [open_time, open, high, low, close, volume, ...]
        return r.json()
    except Exception as e:
        log.error(f"Binance API error ({asset}): {e}")
        return []

def calc_rsi(closes, period=14):
    """Calculate RSI from list of close prices."""
    if len(closes) < period + 1:
        return 50.0  # neutral if not enough data
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)

def calc_macd(closes, fast=12, slow=26, signal=9):
    """Calculate MACD line and signal line."""
    def ema(data, period):
        k = 2 / (period + 1)
        e = data[0]
        for p in data[1:]:
            e = p * k + e * (1 - k)
        return e
    if len(closes) < slow:
        return 0.0, 0.0, "neutral"
    macd_line   = ema(closes, fast) - ema(closes, slow)
    signal_line = ema(closes[-signal:], signal)
    hist        = macd_line - signal_line
    trend = "bullish" if hist > 0 else "bearish"
    return round(macd_line, 4), round(signal_line, 4), trend

def get_binance_indicators(asset):
    """Fetch Binance 1min candles and compute RSI, MACD, volume, trend."""
    candles = fetch_binance_candles(asset, interval="1m", limit=30)
    if not candles:
        return None
    closes  = [float(c[4]) for c in candles]
    volumes = [float(c[5]) for c in candles]
    highs   = [float(c[2]) for c in candles]
    lows    = [float(c[3]) for c in candles]

    rsi            = calc_rsi(closes)
    macd, sig, trend = calc_macd(closes)
    avg_vol        = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
    last_vol       = volumes[-1]
    vol_signal     = "HIGH" if last_vol > avg_vol * 1.2 else "LOW" if last_vol < avg_vol * 0.8 else "NORMAL"

    # Last 3 candles direction
    candle_dirs = []
    for c in candles[-3:]:
        o, cl = float(c[1]), float(c[4])
        candle_dirs.append("green" if cl >= o else "red")

    # Price change over last 5 and 15 candles
    change_5  = round((closes[-1] - closes[-5]) / closes[-5] * 100, 2) if len(closes) >= 5 else 0
    change_15 = round((closes[-1] - closes[-15]) / closes[-15] * 100, 2) if len(closes) >= 15 else 0

    return {
        "price":       round(closes[-1], 4),
        "rsi":         rsi,
        "macd_trend":  trend,
        "volume":      vol_signal,
        "candles":     ", ".join(candle_dirs),
        "change_5m":   change_5,
        "change_15m":  change_15,
    }


def gemini_decide(asset, side, current_price, secs_into, secs_left):
    """Ask Gemini AI whether to BUY or SKIP using Polymarket + Binance data."""
    if not GEMINI_KEY:
        return "BUY", "Gemini not configured — using default"

    # Polymarket price history
    key     = (asset, int(time.time() // WINDOW_SECS) * WINDOW_SECS)
    history = price_history.get(key, [])
    price_30s_ago = current_price
    price_60s_ago = current_price
    now_ts = int(time.time())
    for ts, yp, np in reversed(history):
        p = yp if side == "YES" else np
        if now_ts - ts >= 30 and price_30s_ago == current_price:
            price_30s_ago = p
        if now_ts - ts >= 60 and price_60s_ago == current_price:
            price_60s_ago = p
            break
    momentum = "rising" if current_price > price_30s_ago else "falling" if current_price < price_30s_ago else "flat"

    # Binance indicators
    ind = get_binance_indicators(asset)
    if ind:
        binance_section = f"""
Binance 1-min chart indicators:
- Spot price: ${ind['price']:,.4f}
- RSI (14): {ind['rsi']} {'⚠ overbought' if ind['rsi'] > 70 else '⚠ oversold' if ind['rsi'] < 30 else '✓ neutral'}
- MACD trend: {ind['macd_trend']}
- Volume: {ind['volume']}
- Last 3 candles: {ind['candles']}
- Price change last 5min: {ind['change_5m']:+.2f}%
- Price change last 15min: {ind['change_15m']:+.2f}%"""
    else:
        binance_section = "Binance data: unavailable"

    prompt = f"""You are a trading assistant for Polymarket 15-minute crypto up/down markets.
A prediction market is asking: will {asset.upper()} go UP or DOWN in this 15-minute window?
Someone wants to buy {side} shares at {current_price:.0%} probability. Decide BUY or SKIP.

Polymarket data:
- Asset: {asset.upper()} {side}
- Current odds: {current_price:.2%}
- Odds 30s ago: {price_30s_ago:.2%}
- Odds 60s ago: {price_60s_ago:.2%}
- Polymarket momentum: {momentum}
- Time into 15min window: {secs_into//60}min {secs_into%60}s
- Time remaining: {secs_left//60}min {secs_left%60}s
{binance_section}

Decision rules:
- BUY if Binance momentum confirms Polymarket direction
- BUY if RSI is not overbought (< 70 for YES, > 30 for NO)
- BUY if volume is NORMAL or HIGH (confirms real move)
- BUY if MACD trend matches the side being bought
- SKIP if RSI > 75 for YES or RSI < 25 for NO (extreme, likely reversal)
- SKIP if last 3 candles all opposite direction to the side
- SKIP if volume is LOW (fake move, no conviction)
- SKIP if less than 60 seconds left in window
- SKIP if Polymarket odds are falling back from 80c+

Reply ONLY in this exact JSON, nothing else:
{{"action": "BUY", "reason": "brief reason"}}
or
{{"action": "SKIP", "reason": "brief reason"}}"""

    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={GEMINI_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=8,
        )
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        text = text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        action = data.get("action", "BUY").upper()
        reason = data.get("reason", "")
        return action, reason
    except Exception as e:
        log.error(f"Gemini error: {e} — defaulting to BUY")
        return "BUY", "Gemini error — default BUY"

# ── Google Sheets ─────────────────────────────────────────────────────────────

def connect_sheet():
    """Connect to Google Sheet using service account JSON from env var."""
    if not GSHEETS_OK:
        return None
    try:
        creds_json = os.getenv("GOOGLE_CREDS_JSON", "")
        if not creds_json:
            log.warning("GOOGLE_CREDS_JSON not set — Sheets disabled")
            return None
        creds_dict = json.loads(creds_json)
        scopes     = ["https://www.googleapis.com/auth/spreadsheets"]
        creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc         = gspread.authorize(creds)
        sheet      = gc.open_by_key(SHEET_ID).sheet1
        log.info("Google Sheets connected!")
        return sheet
    except Exception as e:
        log.error(f"Google Sheets connect error: {e}")
        return None


def sheet_full_sync(sheet, trades):
    """Wipe sheet and rewrite all trades from scratch — always accurate."""
    if sheet is None:
        return
    try:
        headers = [
            "datetime", "asset", "window", "side",
            "buy_shares", "buy_price", "buy_cost",
            "ins_shares", "ins_price", "ins_cost",
            "sell_shares", "sell_price", "sell_revenue",
            "ins_result", "ins_revenue",
            "status", "pnl_usdc", "running_total"
        ]
        rows    = [headers]
        running = 0.0
        for t in trades:
            if t["status"] != "OPEN":
                running = round(running + t["pnl_usdc"], 4)
            rows.append([
                t["datetime"], t["asset"], t["window"], t["side"],
                t["buy_shares"], t["buy_price"], t["buy_cost"],
                t["ins_shares"], t["ins_price"], t["ins_cost"],
                t["sell_shares"], t["sell_price"], t["sell_revenue"],
                t["ins_result"], t["ins_revenue"],
                t["status"], t["pnl_usdc"],
                running if t["status"] != "OPEN" else "OPEN"
            ])
        sheet.clear()
        sheet.update("A1", rows)
        log.info(f"  Sheets | Synced {len(trades)} trades to Google Sheets ✓")
    except Exception as e:
        log.error(f"Sheet sync error: {e}")

# ── PnL Tracker ───────────────────────────────────────────────────────────────

class PnLTracker:
    def __init__(self, path=PNL_FILE, sheet=None):
        self.path  = path
        self.sheet = sheet
        self.trades = []
        self._init_file()

    def _init_file(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "datetime", "asset", "window", "side",
                    "buy_shares", "buy_price", "buy_cost",
                    "ins_shares", "ins_price", "ins_cost",
                    "sell_shares", "sell_price", "sell_revenue",
                    "ins_result", "ins_revenue",
                    "status", "pnl_usdc", "running_total"
                ])

    def load_from_csv(self):
        if not os.path.exists(self.path):
            return []
        self.trades = []
        pending = []
        with open(self.path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("window", "").strip():
                    continue
                trade = {
                    "datetime":     row["datetime"],
                    "asset":        row.get("asset", "btc"),
                    "window":       int(row["window"]),
                    "side":         row["side"],
                    "buy_shares":   float(row["buy_shares"]),
                    "buy_price":    float(row["buy_price"]),
                    "buy_cost":     float(row["buy_cost"]),
                    "ins_shares":   float(row.get("ins_shares") or 0),
                    "ins_price":    float(row.get("ins_price") or 0),
                    "ins_cost":     float(row.get("ins_cost") or 0),
                    "sell_shares":  float(row.get("sell_shares") or 0),
                    "sell_price":   float(row.get("sell_price") or 0),
                    "sell_revenue": float(row.get("sell_revenue") or 0),
                    "ins_result":   row.get("ins_result", "PENDING"),
                    "ins_revenue":  float(row.get("ins_revenue") or 0),
                    "status":       row.get("status", "OPEN"),
                    "pnl_usdc":     float(row.get("pnl_usdc") or 0),
                }
                self.trades.append(trade)
                if trade["status"] == "OPEN":
                    pending.append({
                        "trade_idx": len(self.trades) - 1,
                        "asset":     trade["asset"],
                        "window":    trade["window"],
                        "side":      trade["side"],
                    })
        return pending

    def record_buy(self, asset, window, side, shares, price, ins_shares=0, ins_price=0):
        cost     = round(shares * price, 4)
        ins_cost = round(ins_shares * ins_price, 4)
        trade = {
            "datetime":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "asset":        asset,
            "window":       window,
            "side":         side,
            "buy_shares":   shares,
            "buy_price":    price,
            "buy_cost":     cost,
            "ins_shares":   ins_shares,
            "ins_price":    ins_price,
            "ins_cost":     ins_cost,
            "sell_shares":  0.0,
            "sell_price":   0.0,
            "sell_revenue": 0.0,
            "ins_result":   "PENDING" if ins_shares > 0 else "N/A",
            "ins_revenue":  0.0,
            "status":       "OPEN",
            "pnl_usdc":     0.0,
        }
        self.trades.append(trade)
        log.info(f"  PnL | [{asset.upper()}] BUY {shares} {side} @ {price:.2%} = ${cost:.4f} USDC")
        if ins_shares > 0:
            opp = "NO" if side == "YES" else "YES"
            log.info(f"  PnL | [{asset.upper()}] INSURANCE {ins_shares} {opp} @ {ins_price:.2%} = ${ins_cost:.4f} USDC")
        self._rewrite()
        return len(self.trades) - 1

    def record_insurance(self, trade_idx, shares, price):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        ins_cost       = round(shares * price, 4)
        t["ins_shares"] = shares
        t["ins_price"]  = price
        t["ins_cost"]   = ins_cost
        t["ins_result"] = "PENDING"
        opp = "NO" if t["side"] == "YES" else "YES"
        log.info(f"  PnL | [{t['asset'].upper()}] INSURANCE {shares} {opp} @ {price:.2%} = ${ins_cost:.4f} USDC")
        self._rewrite()

    def record_sell(self, trade_idx, price, reason="95c"):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] != "OPEN":
            return
        revenue    = round(t["buy_shares"] * price, 4)
        total_cost = round(t["buy_cost"] + t["ins_cost"], 4)
        pnl        = round(revenue - total_cost, 4)
        t["sell_shares"]  = t["buy_shares"]
        t["sell_price"]   = price
        t["sell_revenue"] = revenue
        t["pnl_usdc"]     = pnl
        t["status"]       = reason  # "95c", "CUT-LOSS"
        log.info(f"  PnL | [{t['asset'].upper()}] {reason} {t['buy_shares']} {t['side']} @ {price:.2%} | pnl={'+' if pnl>=0 else ''}{pnl:.4f} USDC")
        self._rewrite()

    def record_resolved(self, trade_idx, won):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] != "OPEN":
            return
        # Main position
        main_price   = 1.0 if won else 0.0
        main_rev     = round(t["buy_shares"] * main_price, 4)
        # Insurance pays out if opposite side won
        ins_won      = not won if t["ins_shares"] > 0 else False
        ins_rev      = round(t["ins_shares"] * (1.0 if ins_won else 0.0), 4)
        total_rev    = round(main_rev + ins_rev, 4)
        total_cost   = round(t["buy_cost"] + t["ins_cost"], 4)
        pnl          = round(total_rev - total_cost, 4)
        t["sell_shares"]  = t["buy_shares"]
        t["sell_price"]   = main_price
        t["sell_revenue"] = main_rev
        t["ins_result"]   = "WIN" if ins_won else "LOSS"
        t["ins_revenue"]  = ins_rev
        t["pnl_usdc"]     = pnl
        t["status"]       = "WIN" if won else "LOSS"
        settled = [x for x in self.trades if x["status"] != "OPEN"]
        total   = round(sum(x["pnl_usdc"] for x in settled), 4)
        log.info(
            f"  PnL | [{t['asset'].upper()}] {'WIN' if won else 'LOSS'} | "
            f"main={'+' if main_rev-t['buy_cost']>=0 else ''}{main_rev-t['buy_cost']:.4f} | "
            f"ins={'+' if ins_rev-t['ins_cost']>=0 else ''}{ins_rev-t['ins_cost']:.4f} | "
            f"total={'+' if pnl>=0 else ''}{pnl:.4f} USDC"
        )
        self._rewrite()

    def print_summary(self):
        if not self.trades:
            log.info("PnL | No trades yet.")
            return
        settled = [t for t in self.trades if t["status"] != "OPEN"]
        open_   = [t for t in self.trades if t["status"] == "OPEN"]
        total   = round(sum(t["pnl_usdc"] for t in settled), 4)
        spent   = round(sum(t["buy_cost"] for t in self.trades), 4)
        log.info("=" * 55)
        log.info("  PnL SUMMARY")
        for asset in ASSETS:
            ac = [t for t in settled if t["asset"] == asset]
            ap = round(sum(t["pnl_usdc"] for t in ac), 4)
            log.info(f"  {asset.upper()}: {len(ac)} trades | pnl={'+' if ap>=0 else ''}{ap:.4f}")
        log.info(f"  Total trades : {len(self.trades)} ({len(open_)} open)")
        log.info(f"  Total spent  : ${spent:.4f} USDC")
        log.info(f"  Net PnL      : {'+' if total>=0 else ''}{total:.4f} USDC")
        log.info(f"  Log saved to : {self.path}")
        log.info("=" * 55)

    def _rewrite(self):
        running = 0.0
        with open(self.path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "datetime", "asset", "window", "side",
                "buy_shares", "buy_price", "buy_cost",
                "ins_shares", "ins_price", "ins_cost",
                "sell_shares", "sell_price", "sell_revenue",
                "ins_result", "ins_revenue",
                "status", "pnl_usdc", "running_total"
            ])
            for t in self.trades:
                if t["status"] != "OPEN":
                    running = round(running + t["pnl_usdc"], 4)
                writer.writerow([
                    t["datetime"], t["asset"], t["window"], t["side"],
                    t["buy_shares"], t["buy_price"], t["buy_cost"],
                    t["ins_shares"], t["ins_price"], t["ins_cost"],
                    t["sell_shares"], t["sell_price"], t["sell_revenue"],
                    t["ins_result"], t["ins_revenue"],
                    t["status"], t["pnl_usdc"],
                    running if t["status"] != "OPEN" else "OPEN"
                ])
        # Sync all trades to Google Sheets
        if self.sheet:
            sheet_full_sync(self.sheet, self.trades)

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_server_time():
    try:
        r = requests.get(f"{CLOB_API}/time", timeout=5)
        return int(r.json())
    except Exception:
        return int(datetime.now(timezone.utc).timestamp())

def get_current_window_start(server_ts):
    return (server_ts // WINDOW_SECS) * WINDOW_SECS

def build_slug(asset, window_ts):
    return f"{asset}-updown-15m-{window_ts}"

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
        log.error(f"Gamma API error ({slug}): {e}")
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

def build_client():
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        log.error("Missing .env keys.")
        exit(1)
    client = ClobClient(
        host=CLOB_API, chain_id=POLYGON, key=pk,
        creds=ApiCreds(
            api_key=api_key,
            api_secret=api_sec,
            api_passphrase=api_pass,
        ),
        signature_type=0, funder=funder or None,
    )
    log.info("Connected to Polymarket CLOB.")
    return client

def market_buy(client, token_id, shares, price, label):
    amount = round(shares * price, 4)
    if DRY_RUN:
        log.info(f"  [DRY RUN] MARKET BUY {shares} {label} @ {price:.2%} = ${amount:.4f} USDC")
        return price
    try:
        order = client.create_market_order(MarketOrderArgs(token_id=token_id, amount=amount, side=BUY))
        resp  = client.post_order(order, OrderType.FOK)
        log.info(f"  MARKET BUY executed: {label} | {resp}")
        return price
    except Exception as e:
        log.error(f"  MARKET BUY failed ({label}): {e}")
        return None

def market_sell(client, token_id, shares, price, label):
    amount = round(shares * price, 4)
    if DRY_RUN:
        log.info(f"  [DRY RUN] MARKET SELL {shares} {label} @ {price:.2%} = ${amount:.4f} USDC")
        return price
    try:
        order = client.create_market_order(MarketOrderArgs(token_id=token_id, amount=amount, side=SELL))
        resp  = client.post_order(order, OrderType.FOK)
        log.info(f"  MARKET SELL executed: {label} | {resp}")
        return price
    except Exception as e:
        log.error(f"  MARKET SELL failed ({label}): {e}")
        return None

def resolve_pending_on_startup(pnl):
    unresolved = pnl.load_from_csv()
    if not unresolved:
        return {}
    grouped = {}
    for item in unresolved:
        key = (item["asset"], item["window"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(item)
    live_pending = {}
    server_ts = get_server_time()
    for (asset, w), items in grouped.items():
        if server_ts < w + WINDOW_SECS + 30:
            live_pending[(asset, w)] = items
            continue
        try:
            mkt = fetch_market_by_slug(build_slug(asset, w))
            if mkt:
                result = mkt.get("result") or mkt.get("winner") or mkt.get("resolutionResult")
                if result:
                    for item in items:
                        won = (result.strip().upper() == item["side"].upper())
                        pnl.record_resolved(item["trade_idx"], won)
                else:
                    live_pending[(asset, w)] = items
            else:
                live_pending[(asset, w)] = items
        except Exception:
            live_pending[(asset, w)] = items
    return live_pending

# ── Main Loop ─────────────────────────────────────────────────────────────────

def run():
    client = build_client()
    sheet  = connect_sheet()
    pnl    = PnLTracker(sheet=sheet)

    log.info("=" * 55)
    log.info(f"  {'DRY RUN MODE' if DRY_RUN else 'LIVE MODE'}")
    log.info("=" * 55)

    pending          = resolve_pending_on_startup(pnl)
    traded           = set(pending.keys())
    token_cache      = {}
    active_positions = {}
    for key, items in pending.items():
        active_positions[key] = [
            {"trade_idx": i["trade_idx"], "side": i["side"],
             "ins_bought": pnl.trades[i["trade_idx"]]["ins_shares"] > 0}
            for i in items
        ]

    log.info(f"Bot started — {', '.join(a.upper() for a in ASSETS)}")
    log.info(f"Buy {BUY_PRICE_MIN:.0%}-{BUY_PRICE_MAX:.0%} between {ENTRY_AFTER//60}-{STOP_BUY_AT//60}min | Sell @ {SELL_PRICE:.0%} | Insurance {INS_SHARES} shares @ <={INS_MAX_PRICE:.1%}")

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = (window_start + WINDOW_SECS) - server_ts

            for asset in ASSETS:
                key = (asset, window_start)

                # ── Pre-cache tokens ──────────────────────────────────────────
                if key not in token_cache:
                    mkt = fetch_market_by_slug(build_slug(asset, window_start))
                    if mkt:
                        yt, nt = get_tokens(mkt)
                        if yt and nt:
                            token_cache[key] = (yt, nt)

                tokens = token_cache.get(key)
                if not tokens:
                    continue

                yes_token, no_token = tokens

                # ── Record orderbook silently to log file ─────────────────────
                yes_price_ob = get_midpoint(client, yes_token)
                no_price_ob  = get_midpoint(client, no_token)
                if yes_price_ob > 0 and no_price_ob > 0:
                    record_orderbook(asset, yes_price_ob, no_price_ob)
                    record_price_history(asset, window_start, yes_price_ob, no_price_ob)

                # ── Monitor active positions for sell trigger ─────────────────
                for pos_key, positions in list(active_positions.items()):
                    pos_asset, pos_window = pos_key
                    pos_tokens = token_cache.get(pos_key)
                    if not pos_tokens:
                        mkt2 = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                        if mkt2:
                            yt2, nt2 = get_tokens(mkt2)
                            if yt2 and nt2:
                                token_cache[pos_key] = (yt2, nt2)
                                pos_tokens = (yt2, nt2)
                    if not pos_tokens:
                        continue
                    pyt, pnt = pos_tokens

                    for pos in list(positions):
                        idx      = pos["trade_idx"]
                        side     = pos["side"]
                        token_id = pyt if side == "YES" else pnt
                        opp_id   = pnt if side == "YES" else pyt
                        price    = get_midpoint(client, token_id)
                        if price <= 0:
                            continue

                        # Watch opposite side for insurance trigger
                        if not pos["ins_bought"]:
                            opp_price = get_midpoint(client, opp_id)
                            if opp_price <= INS_MAX_PRICE:
                                opp_label = f"{pos_asset.upper()}-{'NO' if side == 'YES' else 'YES'}-INS"
                                log.info(f"  [{pos_asset.upper()}] INSURANCE TRIGGER: opposite @ {opp_price:.2%} <= {INS_MAX_PRICE:.1%}")
                                ins_r = market_buy(client, opp_id, INS_SHARES, opp_price, opp_label)
                                if ins_r is not None:
                                    pnl.record_insurance(idx, INS_SHARES, opp_price)
                                    pos["ins_bought"] = True

                        # Cut loss at 50% of buy price
                        buy_price      = pnl.trades[idx]["buy_price"]
                        cut_loss_price = round(buy_price * 0.50, 4)
                        if price <= cut_loss_price:
                            log.info(f"  [{pos_asset.upper()} {side}] CUT LOSS @ {price:.2%} (bought @ {buy_price:.2%} cut @ {cut_loss_price:.2%})")
                            sp = market_sell(client, token_id, BUY_SHARES, price, f"{pos_asset.upper()}-{side}")
                            if sp is not None:
                                pnl.record_sell(idx, sp, "CUT-LOSS")
                                positions.remove(pos)
                            continue

                        # Sell main at 95c
                        if price >= SELL_PRICE:
                            log.info(f"  [{pos_asset.upper()} {side}] TRIGGER SELL @ {price:.2%}!")
                            sp = market_sell(client, token_id, BUY_SHARES, price, f"{pos_asset.upper()}-{side}")
                            if sp is not None:
                                pnl.record_sell(idx, sp, "95c")
                                positions.remove(pos)

                    # Resolve closed windows
                    if server_ts > pos_window + WINDOW_SECS + 30:
                        try:
                            mkt = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                            if mkt:
                                result = mkt.get("result") or mkt.get("winner") or mkt.get("resolutionResult")
                                if result:
                                    for pos in list(positions):
                                        won = (result.strip().upper() == pos["side"].upper())
                                        pnl.record_resolved(pos["trade_idx"], won)
                                    del active_positions[pos_key]
                                    if pos_key in pending:
                                        del pending[pos_key]
                        except Exception as e:
                            log.error(f"  Resolution error: {e}")

                # ── Skip already-traded ───────────────────────────────────────
                if key in traded:
                    continue

                # ── Only buy between 10-14 minutes ────────────────────────────
                if secs_into < ENTRY_AFTER:
                    continue
                if secs_into > STOP_BUY_AT:
                    continue

                # ── Check buy trigger (reuse cached prices) ───────────────────
                yes_price = yes_price_ob
                no_price  = no_price_ob

                if BUY_PRICE_MIN <= yes_price <= BUY_PRICE_MAX:
                    log.info(f"  [{asset.upper()}] TRIGGER: YES @ {yes_price:.2%} — asking Gemini...")
                    action, reason = gemini_decide(asset, "YES", yes_price, secs_into, secs_left)
                    log.info(f"  [{asset.upper()}] Gemini: {action} — {reason}")
                    if action == "BUY":
                        fill = market_buy(client, yes_token, BUY_SHARES, yes_price, f"{asset.upper()}-YES")
                        if fill is not None:
                            idx = pnl.record_buy(asset, window_start, "YES", BUY_SHARES, fill)
                            pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": "YES"})
                            active_positions.setdefault(key, []).append({"trade_idx": idx, "side": "YES", "ins_bought": False})
                            traded.add(key)
                    else:
                        log.info(f"  [{asset.upper()}] SKIPPED by Gemini — {reason}")
                        traded.add(key)  # skip this window

                elif BUY_PRICE_MIN <= no_price <= BUY_PRICE_MAX:
                    log.info(f"  [{asset.upper()}] TRIGGER: NO @ {no_price:.2%} — asking Gemini...")
                    action, reason = gemini_decide(asset, "NO", no_price, secs_into, secs_left)
                    log.info(f"  [{asset.upper()}] Gemini: {action} — {reason}")
                    if action == "BUY":
                        fill = market_buy(client, no_token, BUY_SHARES, no_price, f"{asset.upper()}-NO")
                        if fill is not None:
                            idx = pnl.record_buy(asset, window_start, "NO", BUY_SHARES, fill)
                            pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": "NO"})
                            active_positions.setdefault(key, []).append({"trade_idx": idx, "side": "NO", "ins_bought": False})
                            traded.add(key)
                    else:
                        log.info(f"  [{asset.upper()}] SKIPPED by Gemini — {reason}")
                        traded.add(key)  # skip this window

        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            pnl.print_summary()
            break
        except Exception as e:
            log.error(f"Unexpected error: {e}")

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    run()