"""
Polymarket 15-Min Up/Down Bot — Fresh v25
Markets: BTC, ETH, SOL

Strategy:
  1. BUY — enter YES or NO when price hits 80-85c between min 10-14 of window
  2. SELL — exit at 98c (target)
  3. CUT LOSS — sell if price drops to 50% of buy price
  4. FLIP — after cut loss, immediately buy opposite side IF:
             opposite side is between 60c and 75c (sweet spot)
             below 60c = too risky/chaotic, skip
             above 75c = too expensive, skip
             only flip once per window (no chain flipping)
  5. ORDERBOOK — record every second price to Google Sheets Sheet2 (min 10-15 only)
  6. PNL — synced live to Google Sheets Sheet1

PnL status values:
  OPEN          = active normal trade
  OPEN-FLIP     = active flip trade
  SOLD-99c      = sold at 99c target
  SOLD-99c-FLIP = flip trade sold at 99c target
  CUT-LOSS      = cut loss on normal trade (flip may follow)
  CUT-LOSS-FLIP = cut loss on flip trade (no more flips)
  WIN           = held to resolution, won
  WIN-FLIP      = flip trade won at resolution
  LOSS          = held to resolution, lost
  LOSS-FLIP     = flip trade lost at resolution

Requirements:
    pip install py-clob-client python-dotenv requests colorama gspread google-auth
"""

import os
import sys
import csv
import json
import math
from collections import deque
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

MYT = timezone(timedelta(hours=8))  # Malaysia Time UTC+8

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType, ApiCreds, BalanceAllowanceParams, AssetType
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client python-dotenv requests gspread")
    exit(1)

try:
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderConfig
    from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
    from poly_web3 import RELAYER_URL, PolyWeb3Service
    POLY_WEB3_OK = True
except ImportError:
    POLY_WEB3_OK = False
    print("Warning: poly-web3 not installed — auto-redeem disabled")

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
        logging.FileHandler("fresh_bot25.log"),
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
_ob_handler = logging.FileHandler("fresh_bot25.log")
_ob_handler.setFormatter(logging.Formatter("%(asctime)s [ORDERBOOK] %(message)s"))
ob_logger.addHandler(_ob_handler)

def record_orderbook(asset, yes_price, no_price):
    """Record YES/NO prices silently to log file only."""
    ob_logger.info(f"{asset.upper()} | YES={yes_price:.4f} NO={no_price:.4f}")

# ── Settings ──────────────────────────────────────────────────────────────────

DRY_RUN         = False
ASSETS          = ["btc", "eth", "sol", "xrp"]
BUY_PRICE_MIN   = 0.78    # S1 monitor: direction detected when price >= 78c
BUY_PRICE_MAX   = 0.84    # S1 monitor: direction detected when price <= 84c
FEE_BUFFER      = 0.98    # 2% buffer covers taker fee (~0.88% at 82-84c) + rounding
TRADING_HOURS   = False   # Set True to restrict trading to 12:30–20:00 MYT
WINDOW_SECS     = 900     # 15-minute window
POLL_SECS       = 2

# ── Volatility Cycle Guard (per-asset state machine) ─────────────────────────
# Baseline captured at min 5.  A cycle completes when:
#   Pump leg:  price rises ≥40c from baseline  → state "pumped", track peak
#   Dump leg:  from peak, price drops ≥40c     → state "volatile"
#   (vice versa: dump ≥40c from baseline, then pump ≥40c from trough)
VOL_SWING = 0.35  # minimum move (in cents) for each leg

# ── Strategy 2 — Volatility Rebound Buy ──────────────────────────────────────
S2_ENTRY_AFTER = 360   # S2 entry opens at min 6 (360s)
S2_STOP_BUY_AT = 720   # S2 entry closes at min 12 (720s)
S2_BUY_MIN  = 0.02   # Buy cheap side if price >= 2c
S2_BUY_MAX  = 0.10   # Buy cheap side if price <= 10c
S2_AMOUNT   = 2.0    # USDC stake for S2 trades
S2_TP1      = 0.15   # First take-profit: sell half at 15c
S2_TP2      = 0.40   # Second take-profit: sell rest at 40c

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
PNL_FILE  = "fresh_bot25_pnl.csv"

SHEET_ID  = os.getenv("GOOGLE_SHEET_ID", "1X9EVZBcMUvRuloX2fi71SajlbpmVglSzAhfdsN6AGk4")

# ── Google Sheets ─────────────────────────────────────────────────────────────

def connect_sheet():
    """Connect to Google Sheet — returns dict {asset: worksheet} for each asset tab."""
    global _ob_sheet
    if not GSHEETS_OK:
        return None
    try:
        creds_json = os.getenv("GOOGLE_CREDS_JSON", "")
        if not creds_json:
            log.warning("GOOGLE_CREDS_JSON not set — Sheets disabled")
            return None
        creds_dict  = json.loads(creds_json)
        scopes      = ["https://www.googleapis.com/auth/spreadsheets"]
        creds       = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc          = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(SHEET_ID)
        _ob_sheet   = get_or_create_orderbook_sheet(gc)
        asset_sheets = {}
        for asset in ASSETS:
            tab_name = asset.upper()
            try:
                ws = spreadsheet.worksheet(tab_name)
            except Exception:
                ws = spreadsheet.add_worksheet(title=tab_name, rows=5000, cols=15)
                log.info(f"  Sheets | Created tab: {tab_name}")
            asset_sheets[asset] = ws
        # Clean up orphaned tabs (e.g. old Sheet1) — delete anything not an asset tab or Orderbook
        asset_tab_names = {a.upper() for a in ASSETS} | {"Orderbook"}
        for ws in spreadsheet.worksheets():
            if ws.title not in asset_tab_names:
                try:
                    spreadsheet.del_worksheet(ws)
                    log.info(f"  Sheets | Deleted orphaned tab: '{ws.title}'")
                except Exception as e:
                    log.warning(f"  Sheets | Could not delete '{ws.title}': {e}")
        log.info(f"Google Sheets connected! Tabs: {', '.join(a.upper() for a in ASSETS)}")
        return asset_sheets
    except Exception as e:
        log.error(f"Google Sheets connect error: {e}")
        return None


def sheet_full_sync(asset_sheets, all_trades):
    """Wipe each asset tab and rewrite its trades — always accurate."""
    if not asset_sheets:
        return
    headers = [
        "datetime", "asset", "window", "side",
        "buy_shares", "buy_price", "buy_cost",
        "sell_shares", "sell_price", "sell_revenue",
        "status", "pnl_usdc", "running_total"
    ]
    open_statuses = ("OPEN", "OPEN-S2")
    for asset, ws in asset_sheets.items():
        try:
            trades  = [t for t in all_trades if t["asset"] == asset]
            rows    = [headers]
            running = 0.0
            for t in trades:
                if t["status"] not in open_statuses:
                    running = round(running + t["pnl_usdc"], 4)
                rows.append([
                    t["datetime"], t["asset"], t["window"], t["side"],
                    t["buy_shares"], t["buy_price"], t["buy_cost"],
                    t["sell_shares"], t["sell_price"], t["sell_revenue"],
                    t["status"], t["pnl_usdc"],
                    running if t["status"] not in open_statuses else "OPEN"
                ])
            ws.clear()
            ws.update(rows, "A1")
            # log.info(f"  Sheets | {asset.upper()} tab synced ({len(trades)} trades) ✓")
        except Exception as e:
            log.error(f"Sheet sync error [{asset.upper()}]: {e}")


def get_or_create_orderbook_sheet(gc_client):
    """Get or create the Orderbook tab — clears old data on startup."""
    try:
        spreadsheet = gc_client.open_by_key(SHEET_ID)
        try:
            ob_sheet = spreadsheet.worksheet("Orderbook")
        except Exception:
            ob_sheet = spreadsheet.add_worksheet(title="Orderbook", rows=10000, cols=6)
        log.info("  Sheets | Orderbook and ready ✓")
        return ob_sheet
    except Exception as e:
        log.error(f"Orderbook sheet error: {e}")
        return None


# Buffer orderbook rows — flush to sheet in batches to avoid rate limits
_ob_buffer = []
_ob_sheet  = None

def ob_record(asset, yes_price, no_price, event="", pnl=0.0):
    """Add a row to the orderbook buffer."""
    global _ob_buffer
    now = datetime.now(MYT).strftime("%m/%d %H:%M:%S")  # shorter: 03/21 19:25:01
    _ob_buffer.append([
        now,
        asset.upper(),
        round(yes_price, 4),
        round(no_price, 4),
        event,
        round(pnl, 4) if pnl else ""
    ])

def ob_flush():
    """Flush buffered orderbook rows to Google Sheet."""
    global _ob_buffer, _ob_sheet
    if not _ob_buffer or _ob_sheet is None:
        _ob_buffer = []
        return
    try:
        _ob_sheet.append_rows(_ob_buffer)
        _ob_buffer = []
    except Exception as e:
        log.error(f"Orderbook flush error: {e}")
        _ob_buffer = []

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
                    "sell_shares", "sell_price", "sell_revenue",
                    "status", "pnl_usdc", "running_total",
                    "actual_shares", "condition_id"
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
                    "sell_shares":  float(row.get("sell_shares") or 0),
                    "sell_price":   float(row.get("sell_price") or 0),
                    "sell_revenue": float(row.get("sell_revenue") or 0),
                    "status":       row.get("status", "OPEN"),
                    "pnl_usdc":     float(row.get("pnl_usdc") or 0),
                    "actual_shares": float(row["actual_shares"]) if row.get("actual_shares") else None,
                    "condition_id":  row.get("condition_id") or None,
                }
                self.trades.append(trade)
                if trade["status"] in ("OPEN",):
                    pending.append({
                        "trade_idx": len(self.trades) - 1,
                        "asset":     trade["asset"],
                        "window":    trade["window"],
                        "side":      trade["side"],
                    })
        return pending

    def record_buy(self, asset, window, side, shares, price, trade_type="OPEN", actual_shares=None, condition_id=None):
        cost  = round(shares, 4)   # shares = amount (USDC spent)
        trade = {
            "datetime":     datetime.now(MYT).strftime("%Y-%m-%d %H:%M:%S MYT"),
            "asset":        asset,
            "window":       window,
            "side":         side,
            "buy_shares":   shares,
            "buy_price":    price,
            "buy_cost":     cost,
            "sell_shares":  0.0,
            "sell_price":   0.0,
            "sell_revenue": 0.0,
            "status":       trade_type,
            "pnl_usdc":     0.0,
            "actual_shares": actual_shares,
            "condition_id":  condition_id,
        }
        self.trades.append(trade)
        label = "BUY"
        log.info(f"  PnL | [{asset.upper()}] {label} {shares} {side} @ {price:.2%} = ${cost:.4f} USDC")
        self._rewrite()
        return len(self.trades) - 1

    def record_sell(self, trade_idx, price, reason="SOLD-99c", actual_shares=None):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] not in ("OPEN",):
            return
        shares  = actual_shares if actual_shares else t["buy_shares"]
        revenue = round(shares * price, 4)
        pnl     = round(revenue - t["buy_cost"], 4)
        t["sell_shares"]  = shares
        t["sell_price"]   = price
        t["sell_revenue"] = revenue
        t["pnl_usdc"]     = pnl
        t["status"] = reason
        log.info(f"  PnL | [{t['asset'].upper()}] {t['status']} {t['buy_shares']} {t['side']} @ {price:.2%} | pnl={'+' if pnl>=0 else ''}{pnl:.4f} USDC")
        self._rewrite()

    def record_resolved(self, trade_idx, won, actual_shares=None):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] not in ("OPEN",):
            return
        shares  = actual_shares if actual_shares else t["buy_shares"]
        price   = 1.0 if won else 0.0
        revenue = round(shares * price, 4)
        pnl     = round(revenue - t["buy_cost"], 4)
        t["sell_shares"]  = shares
        t["sell_price"]   = price
        t["sell_revenue"] = revenue
        t["pnl_usdc"]     = pnl
        t["status"] = "WIN" if won else "LOSS"
        settled = [x for x in self.trades if x["status"] not in ("OPEN",)]
        total   = round(sum(x["pnl_usdc"] for x in settled), 4)
        log.info(
            f"  PnL | [{t['asset'].upper()}] {t['status']} | "
            f"pnl={'+' if pnl>=0 else ''}{pnl:.4f} USDC | total={'+' if total>=0 else ''}{total:.4f}"
        )
        self._rewrite()

    def print_summary(self):
        if not self.trades:
            log.info("PnL | No trades yet.")
            return
        open_statuses = ("OPEN",)
        settled = [t for t in self.trades if t["status"] not in open_statuses]
        open_   = [t for t in self.trades if t["status"] in open_statuses]
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
                "sell_shares", "sell_price", "sell_revenue",
                "status", "pnl_usdc", "running_total",
                "actual_shares", "condition_id"
            ])
            for t in self.trades:
                if t["status"] not in ("OPEN",):
                    running = round(running + t["pnl_usdc"], 4)
                writer.writerow([
                    t["datetime"], t["asset"], t["window"], t["side"],
                    t["buy_shares"], t["buy_price"], t["buy_cost"],
                    t["sell_shares"], t["sell_price"], t["sell_revenue"],
                    t["status"], t["pnl_usdc"],
                    running if t["status"] not in ("OPEN",) else "OPEN",
                    t.get("actual_shares", ""), t.get("condition_id", "")
                ])
        # Sync all trades to Google Sheets (per-asset tabs)
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
        signature_type=1, funder=funder or None,
    )
    log.info("Connected to Polymarket CLOB.")
    return client

def market_buy(client, token_id, shares, price, label):
    amount = round(shares, 4)  # BUY side: amount is USDC directly
    if DRY_RUN:
        log.info(f"  [DRY RUN] MARKET BUY {label} @ {price:.2%} = ${amount:.4f} USDC")
        return price, round(amount / price, 4)
    try:
        order = client.create_market_order(MarketOrderArgs(token_id=token_id, amount=amount, side=BUY))
        resp  = client.post_order(order, OrderType.FOK)
        log.info(f"  MARKET BUY executed: {label} | {resp}")
        # Calculate actual shares using fee buffer (avoids CLOB balance cache issues)
        # actual = floor((USDC / price) × FEE_BUFFER, 1dp)
        actual_shares = math.floor((amount / price) * FEE_BUFFER * 10) / 10
        log.info(f"  Shares calculated: ${amount:.4f} / {price:.2%} × {FEE_BUFFER} = {actual_shares} shares")
        # Refresh CLOB conditional token cache so sell works immediately
        try:
            client.update_balance_allowance(BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL, token_id=token_id
            ))
        except Exception:
            pass
        return price, actual_shares
    except Exception as e:
        log.error(f"  MARKET BUY failed ({label}): {e}")
        return None, None

def market_sell(client, token_id, shares, price, label):
    # SELL side: amount = shares to sell directly (NOT USDC)
    sell_amount = math.floor(shares * 10) / 10  # floor to 1dp for CLOB cache safety
    if sell_amount <= 0:
        sell_amount = shares
    if DRY_RUN:
        log.info(f"  [DRY RUN] MARKET SELL {sell_amount} {label} @ {price:.2%}")
        return price
    # Refresh conditional token balance before selling
    try:
        client.update_balance_allowance(BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL, token_id=token_id
        ))
    except Exception:
        pass
    try:
        order = client.create_market_order(MarketOrderArgs(token_id=token_id, amount=sell_amount, side=SELL))
        resp  = client.post_order(order, OrderType.FOK)
        log.info(f"  MARKET SELL executed: {label} | shares={sell_amount} | {resp}")
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

# ── Redeem Service ────────────────────────────────────────────────────────────

def build_redeem_service(clob_client):
    """Build PolyWeb3Service for auto-redeem. Returns None if not configured."""
    if not POLY_WEB3_OK:
        return None
    builder_key  = os.getenv("POLY_BUILDER_API_KEY", "")
    builder_sec  = os.getenv("POLY_BUILDER_SECRET", "")
    builder_pass = os.getenv("POLY_BUILDER_PASSPHRASE", "")
    if not all([builder_key, builder_sec, builder_pass]):
        log.warning("Builder API keys not set — auto-redeem disabled")
        return None
    try:
        relayer = RelayClient(
            RELAYER_URL, POLYGON,
            os.getenv("POLY_PRIVATE_KEY"),
            BuilderConfig(
                local_builder_creds=BuilderApiKeyCreds(
                    key=builder_key,
                    secret=builder_sec,
                    passphrase=builder_pass,
                )
            ),
        )
        service = PolyWeb3Service(clob_client=clob_client, relayer_client=relayer)
        log.info("Auto-redeem service ready ✓")
        return service
    except Exception as e:
        log.error(f"Redeem service init failed: {e}")
        return None


def get_condition_id(market):
    return (
        market.get("conditionId")
        or market.get("condition_id")
        or market.get("questionID")
        or None
    )


def redeem_position(redeem_service, condition_id, asset, side):
    if redeem_service is None:
        return False
    try:
        if not redeem_service.is_condition_resolved(condition_id):
            return False
        result = redeem_service.redeem(condition_id=condition_id)
        log.info(f"  Redeem | [{asset.upper()} {side}] ✓ {result}")
        return True
    except Exception as e:
        log.error(f"  Redeem | [{asset.upper()} {side}] Failed: {e}")
        return False


# ── Resolution Sweep ──────────────────────────────────────────────────────────

def get_resolution_result(mkt):
    """
    Extract resolution result from Polymarket market object.
    Returns 'YES', 'NO', or None if not yet resolved.
    Handles all known Gamma API field formats.
    """
    # Direct result fields
    for field in ["result", "winner", "resolutionResult", "resolvedBy"]:
        v = mkt.get(field)
        if v and str(v).strip().upper() in ("YES", "NO"):
            return str(v).strip().upper()

    # Check if market is resolved via tokens
    tokens = mkt.get("tokens") or mkt.get("outcomes") or []
    for token in tokens:
        if isinstance(token, dict):
            if token.get("winner") is True:
                outcome = token.get("outcome") or token.get("name") or ""
                if outcome.strip().upper() in ("YES", "NO"):
                    return outcome.strip().upper()

    # Check resolved flag + active flag
    if mkt.get("resolved") is True or mkt.get("active") is False:
        # Try to infer from price — resolved YES token = 1.0
        tokens = mkt.get("tokens") or []
        for token in tokens:
            if isinstance(token, dict):
                price = float(token.get("price", 0) or 0)
                outcome = (token.get("outcome") or token.get("name") or "").strip().upper()
                if price >= 0.99 and outcome in ("YES", "NO"):
                    return outcome

    return None


def resolution_sweep(pnl, client, redeem_service=None):
    """Every 20 mins — resolve OPEN trades, update Sheet."""
    open_statuses = ("OPEN",)
    stale = [(i, t) for i, t in enumerate(pnl.trades) if t["status"] in open_statuses]
    if not stale:
        return
    server_ts      = get_server_time()
    resolved_count = 0
    for idx, trade in stale:
        asset  = trade["asset"]
        window = trade["window"]
        side   = trade["side"]
        actual = trade.get("actual_shares")
        cid    = trade.get("condition_id")
        if server_ts < window + WINDOW_SECS + 30:
            continue
        try:
            mkt = None
            result = None

            # Primary: use client.get_market(condition_id) — most reliable
            if cid:
                try:
                    mkt = client.get_market(cid)
                    if mkt and (getattr(mkt, "closed", False) or getattr(mkt, "resolved", False)):
                        tokens = getattr(mkt, "tokens", []) or []
                        for token in tokens:
                            winner = getattr(token, "winner", None)
                            outcome = getattr(token, "outcome", "") or ""
                            if winner is True and outcome.strip().upper() in ("YES", "NO"):
                                result = outcome.strip().upper()
                                break
                except Exception:
                    pass

            # Fallback: Gamma API slug lookup
            if not result:
                mkt2 = fetch_market_by_slug(build_slug(asset, window))
                if mkt2:
                    result = get_resolution_result(mkt2)
                    if not cid:
                        cid = get_condition_id(mkt2)

            if not result:
                continue

            won = (result == side.upper())
            log.info(f"  Sweep | [{asset.upper()} {side}] → {'WIN ✓' if won else 'LOSS ✗'} (resolved={result})")
            if won and redeem_service and cid:
                redeem_position(redeem_service, cid, asset, side)
            pnl.record_resolved(idx, won, actual_shares=actual)
            resolved_count += 1
        except Exception as e:
            log.error(f"  Sweep | [{asset.upper()} {side}] Error: {e}")
    if resolved_count:
        log.info(f"  Sweep | Done — {resolved_count} resolved.")



# ── Main Loop ─────────────────────────────────────────────────────────────────

def run():
    client = build_client()
    sheet  = connect_sheet()
    pnl    = PnLTracker(sheet=sheet)
    redeem_service = build_redeem_service(client)

    log.info("=" * 55)
    log.info(f"  {'DRY RUN MODE' if DRY_RUN else 'LIVE MODE'}")
    log.info("=" * 55)

    pending          = resolve_pending_on_startup(pnl)
    traded               = set(pending.keys())
    token_cache          = {}
    last_sweep           = 0   # resolution sweep every 20 mins
    active_positions = {}
    # phase: "neutral" → "pumped"/"dumped" → "volatile"
    # start = baseline price at min 5; peak/trough updated during leg tracking
    vol_state        = {asset: {"phase": "neutral", "start": None, "peak": None, "trough": None} for asset in ASSETS}
    direction        = {asset: None for asset in ASSETS}  # "YES"/"NO" per window, set by S1 monitor
    last_vol_reset   = 0   # tracks last window_start seen, for resetting vol_state
    for key, items in pending.items():
        active_positions[key] = [
            {
                "trade_idx":    i["trade_idx"],
                "side":         i["side"],
                "actual_shares": pnl.trades[i["trade_idx"]]["buy_shares"],
            }
            for i in items
        ]

    log.info(f"Bot started — {', '.join(a.upper() for a in ASSETS)}")
    log.info(f"S2-only | vol swing {VOL_SWING:.0%} | S2 entry min {S2_ENTRY_AFTER//60}-{S2_STOP_BUY_AT//60} | cheap {S2_BUY_MIN:.0%}-{S2_BUY_MAX:.0%} | TP1@{S2_TP1:.0%} TP2@{S2_TP2:.0%}")
    log.info(f"S1 monitor: direction signal at >={BUY_PRICE_MIN:.0%} (no trading)")

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = (window_start + WINDOW_SECS) - server_ts

            # ── Reset vol_state at the start of each new window ──────────
            if window_start != last_vol_reset:
                vol_state      = {asset: {"phase": "neutral", "start": None, "peak": None, "trough": None} for asset in ASSETS}
                direction      = {asset: None for asset in ASSETS}
                last_vol_reset = window_start

            # ── Resolution sweep every 20 mins ───────────────────────────
            if server_ts - last_sweep >= 1200:
                last_sweep = server_ts
                resolution_sweep(pnl, client, redeem_service)

            # ── Skip outside trading window 12:30–20:00 MYT ──────────────
            now_myt = datetime.now(MYT)
            h, m    = now_myt.hour, now_myt.minute
            in_skip = TRADING_HOURS and ((h < 12) or (h == 12 and m < 30) or (h >= 20))
            if in_skip:
                if h < 20:  # before window — sleep until 12:30 today
                    target = now_myt.replace(hour=12, minute=30, second=0, microsecond=0)
                else:       # after window — sleep until 12:30 tomorrow
                    target = (now_myt + timedelta(days=1)).replace(hour=12, minute=30, second=0, microsecond=0)
                wait = max(int((target - now_myt).total_seconds()), 60)
                log.info(f"  Skipping {h:02d}:{m:02d} MYT — resuming at 12:30 (wait {wait//3600}h {(wait%3600)//60}m)")
                time.sleep(wait)
                continue

            for asset in ASSETS:
                key = (asset, window_start)

                # ── Pre-cache tokens ──────────────────────────────────────────
                if key not in token_cache:
                    mkt = fetch_market_by_slug(build_slug(asset, window_start))
                    if mkt:
                        yt, nt = get_tokens(mkt)
                        if yt and nt:
                            cid = get_condition_id(mkt)
                            token_cache[key] = (yt, nt, cid)

                tokens = token_cache.get(key)
                if not tokens:
                    continue

                yes_token, no_token = tokens[0], tokens[1]
                cached_cid = tokens[2] if len(tokens) > 2 else None

                # ── Record orderbook to Google Sheets (only when holding a position) ──
                if S2_ENTRY_AFTER <= secs_into <= WINDOW_SECS:
                    has_position = any(
                        pa == asset for (pa, pw) in active_positions
                        if pw == window_start
                    )
                    if has_position:
                        yes_price_ob = get_midpoint(client, yes_token)
                        no_price_ob  = get_midpoint(client, no_token)
                        if yes_price_ob > 0 and no_price_ob > 0:
                            ob_record(asset, yes_price_ob, no_price_ob, "holding")

                # ── Monitor active positions for sell trigger ─────────────────
                for pos_key, positions in list(active_positions.items()):
                    pos_asset, pos_window = pos_key
                    pos_tokens = token_cache.get(pos_key)
                    if not pos_tokens:
                        mkt2 = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                        if mkt2:
                            yt2, nt2 = get_tokens(mkt2)
                            if yt2 and nt2:
                                cid2 = get_condition_id(mkt2)
                                token_cache[pos_key] = (yt2, nt2, cid2)
                                pos_tokens = (yt2, nt2, cid2)
                    if not pos_tokens:
                        continue
                    pyt, pnt = pos_tokens[0], pos_tokens[1]

                    for pos in list(positions):
                        idx      = pos["trade_idx"]
                        side     = pos["side"]
                        token_id = pyt if side == "YES" else pnt
                        opp_id   = pnt if side == "YES" else pyt
                        price    = get_midpoint(client, token_id)
                        if price <= 0:
                            continue


                        # ── Strategy 2: TP1 @ 15c (sell half), TP2 @ 40c (sell rest), no cut-loss ──
                        if pos.get("is_s2"):
                            if not pos.get("s2_tp1_done") and price >= S2_TP1:
                                half = round(pos.get("actual_shares", S2_AMOUNT) / 2, 4)
                                sp = market_sell(client, token_id, half, price, f"{pos_asset.upper()}-{side}-S2-TP1")
                                if sp is not None:
                                    ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                              f"*** S2 TP1 {side} @ {price:.2%} ***")
                                    pnl.record_sell(idx, sp, "S2-TP1", actual_shares=half)
                                    pos["actual_shares"] = round(pos.get("actual_shares", S2_AMOUNT) - half, 4)
                                    pos["s2_tp1_done"]   = True
                                    log.info(f"  [{pos_asset.upper()} {side}] S2 TP1 @ {price:.2%} — sold half, holding {pos['actual_shares']} shares")
                            elif pos.get("s2_tp1_done") and price >= S2_TP2:
                                sp = market_sell(client, token_id, pos.get("actual_shares"), price, f"{pos_asset.upper()}-{side}-S2-TP2")
                                if sp is not None:
                                    ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                              f"*** S2 TP2 {side} @ {price:.2%} ***")
                                    pnl.record_sell(idx, sp, "S2-TP2", actual_shares=pos.get("actual_shares"))
                                    positions.remove(pos)
                                    log.info(f"  [{pos_asset.upper()} {side}] S2 TP2 @ {price:.2%} — fully closed")
                            continue  # no cut-loss — hold to resolution if neither TP hit



                    # Resolve closed windows — only if market has officially resolved
                    # (price monitoring above continues to sell at 99.9c even after window)
                    if server_ts > pos_window + WINDOW_SECS + 30:
                        try:
                            mkt = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                            if mkt:
                                result = get_resolution_result(mkt)
                                if result:
                                    # Only resolve positions that haven't already sold at 99.9c
                                    remaining = [p for p in list(positions) if p in positions]
                                    for pos in remaining:
                                        won = (result.strip().upper() == pos["side"].upper())
                                        pnl.record_resolved(pos["trade_idx"], won)
                                    if remaining:
                                        del active_positions[pos_key]
                                        if pos_key in pending:
                                            del pending[pos_key]
                        except Exception as e:
                            log.error(f"  Resolution error: {e}")

                # ── Skip already-traded ───────────────────────────────────────
                if key in traded:
                    continue

                # ── Update volatility cycle state (track from min 5 onwards) ──
                if secs_into >= 300:
                    _vs_price = get_midpoint(client, yes_token)
                    if _vs_price > 0:
                        vs = vol_state[asset]
                        if vs["phase"] == "neutral":
                            if vs["start"] is None:
                                vs["start"] = _vs_price   # capture baseline at min 5
                            else:
                                move_up   = _vs_price - vs["start"]
                                move_down = vs["start"] - _vs_price
                                if move_up >= VOL_SWING:
                                    vs["phase"] = "pumped"
                                    vs["peak"]  = _vs_price
                                    log.info(f"  [{asset.upper()}] VOL: pump leg done (base {vs['start']:.2%} → {_vs_price:.2%})")
                                elif move_down >= VOL_SWING:
                                    vs["phase"]  = "dumped"
                                    vs["trough"] = _vs_price
                                    log.info(f"  [{asset.upper()}] VOL: dump leg done (base {vs['start']:.2%} → {_vs_price:.2%})")
                        elif vs["phase"] == "pumped":
                            if _vs_price > vs["peak"]:
                                vs["peak"] = _vs_price    # extend peak
                            elif vs["peak"] - _vs_price >= VOL_SWING:
                                vs["phase"] = "volatile"
                                log.info(f"  [{asset.upper()}] VOL: cycle complete — pump then dump (peak {vs['peak']:.2%} → {_vs_price:.2%})")
                        elif vs["phase"] == "dumped":
                            if _vs_price < vs["trough"]:
                                vs["trough"] = _vs_price  # extend trough
                            elif _vs_price - vs["trough"] >= VOL_SWING:
                                vs["phase"] = "volatile"
                                log.info(f"  [{asset.upper()}] VOL: cycle complete — dump then pump (trough {vs['trough']:.2%} → {_vs_price:.2%})")

                # ── Fetch prices once (used by S2 check + S1 monitor) ────────
                if secs_into >= 300:
                    yes_price = get_midpoint(client, yes_token)
                    no_price  = get_midpoint(client, no_token)
                    if yes_price <= 0 or no_price <= 0:
                        continue

                # ── Volatile: S2 only ─────────────────────────────────────────
                if vol_state[asset]["phase"] == "volatile":
                    if S2_ENTRY_AFTER <= secs_into <= S2_STOP_BUY_AT:
                        log.info(f"  [{asset.upper()}] VOLATILE — checking S2 cheap buy (YES={yes_price:.2%}, NO={no_price:.2%})")
                        s2_side, s2_price, s2_token = None, None, None
                        if S2_BUY_MIN <= yes_price <= S2_BUY_MAX:
                            s2_side, s2_price, s2_token = "YES", yes_price, yes_token
                        elif S2_BUY_MIN <= no_price <= S2_BUY_MAX:
                            s2_side, s2_price, s2_token = "NO", no_price, no_token

                        if s2_side:
                            log.info(f"  [{asset.upper()}] S2: {s2_side} @ {s2_price:.2%} — buying cheap")
                            fill, actual_shares = market_buy(client, s2_token, S2_AMOUNT, s2_price, f"{asset.upper()}-{s2_side}-S2")
                            if fill is not None:
                                ob_record(asset, yes_price, no_price, f"*** S2 BUY {s2_side} @ {s2_price:.2%} ***")
                                idx = pnl.record_buy(asset, window_start, s2_side, S2_AMOUNT, fill, "OPEN-S2", actual_shares=actual_shares)
                                pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": s2_side})
                                active_positions.setdefault(key, []).append({
                                    "trade_idx":    idx,
                                    "side":         s2_side,
                                    "is_s2":        True,
                                    "s2_tp1_done":  False,
                                    "actual_shares": actual_shares,
                                })
                        else:
                            log.info(f"  [{asset.upper()}] S2: no cheap side found — skipping window")
                    elif secs_into < S2_ENTRY_AFTER:
                        log.info(f"  [{asset.upper()}] VOLATILE before S2 window — skipping entire window")
                    else:
                        log.info(f"  [{asset.upper()}] VOLATILE but S2 window closed — skipping entire window")
                    traded.add(key)
                    continue

                # ── S1 direction monitor (no buying) ──────────────────────────
                if secs_into >= 300 and direction[asset] is None:
                    if BUY_PRICE_MIN <= yes_price <= BUY_PRICE_MAX:
                        direction[asset] = "YES"
                        log.info(f"  [{asset.upper()}] S1 SIGNAL: direction=YES @ {yes_price:.2%}")
                    elif BUY_PRICE_MIN <= no_price <= BUY_PRICE_MAX:
                        direction[asset] = "NO"
                        log.info(f"  [{asset.upper()}] S1 SIGNAL: direction=NO @ {no_price:.2%}")

                    if direction[asset] is not None:
                        set_dirs = {a: direction[a] for a in ASSETS if direction[a] is not None}
                        if len(set_dirs) >= 2:
                            yes_assets = [a.upper() for a, d in set_dirs.items() if d == "YES"]
                            no_assets  = [a.upper() for a, d in set_dirs.items() if d == "NO"]
                            if yes_assets and no_assets:
                                log.info(f"  PEER DIVERGENCE: YES={yes_assets} NO={no_assets}")

        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            pnl.print_summary()
            ob_flush()
            break
        except Exception as e:
            log.error(f"Unexpected error: {e}")

        # Flush orderbook buffer every 10 seconds
        if int(time.time()) % 10 == 0:
            ob_flush()

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    run()
