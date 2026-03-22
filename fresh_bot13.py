"""
Polymarket 15-Min Up/Down Bot — Fresh v13
Markets: BTC, ETH, SOL

Strategy:
  1. BUY — enter YES or NO when price hits 82-84c between min 10-13 of window
  2. SELL — exit at 97c (target)
  3. CUT LOSS — sell if price drops to 50% of buy price
  4. FLIP — after cut loss, immediately buy opposite side IF:
             opposite side is between 60c and 75c (sweet spot)
             below 60c = too risky/chaotic, skip
             above 75c = too expensive, skip
             only flip once per window (no chain flipping)
  5. ORDERBOOK — record every second price to Google Sheets Sheet2 (min 10-15 only, buying window is min 10-13)
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
    pip install py-clob-client python-dotenv requests colorama gspread google-auth poly-web3
"""

import os
import sys
import csv
import json
import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

MYT = timezone(timedelta(hours=8))  # Malaysia Time UTC+8

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
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderConfig
    from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
    from poly_web3 import RELAYER_URL, PolyWeb3Service
    POLY_WEB3_OK = True
except ImportError:
    POLY_WEB3_OK = False
    print("Warning: poly-web3 not installed — auto-redeem disabled. Run: pip install poly-web3")

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
        logging.FileHandler("fresh_bot13.log"),
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
_ob_handler = logging.FileHandler("fresh_bot13.log")
_ob_handler.setFormatter(logging.Formatter("%(asctime)s [ORDERBOOK] %(message)s"))
ob_logger.addHandler(_ob_handler)

def record_orderbook(asset, yes_price, no_price):
    """Record YES/NO prices silently to log file only."""
    ob_logger.info(f"{asset.upper()} | YES={yes_price:.4f} NO={no_price:.4f}")

# ── Settings ──────────────────────────────────────────────────────────────────

DRY_RUN         = False
ASSETS          = ["btc", "eth", "sol"]
BUY_SHARES      = 5
BUY_PRICE_MIN   = 0.82    # Buy if price >= 80c
BUY_PRICE_MAX   = 0.84    # Buy if price <= 85c
SELL_PRICE      = 0.97     # Sell main shares at 97c
ENTRY_AFTER     = 600     # Start buying after 10 minutes (600s)
STOP_BUY_AT     = 780     # Stop buying after 13 minutes (780s)
WINDOW_SECS     = 900     # 15-minute window
POLL_SECS       = 1

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"
PNL_FILE  = "fresh_bot13_pnl.csv"

SHEET_ID      = os.getenv("GOOGLE_SHEET_ID", "1X9EVZBcMUvRuloX2fi71SajlbpmVglSzAhfdsN6AGk4")
SWEEP_INTERVAL = 1200   # Resolution sweep every 20 minutes (seconds)


# ── Google Sheets ─────────────────────────────────────────────────────────────

def connect_sheet():
    """Connect to Google Sheet using service account JSON from env var."""
    global _ob_sheet
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
        _ob_sheet  = get_or_create_orderbook_sheet(gc)
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
            "sell_shares", "sell_price", "sell_revenue",
            "status", "pnl_usdc", "running_total"
        ]
        rows    = [headers]
        running = 0.0
        for t in trades:
            if t["status"] not in ("OPEN", "OPEN-FLIP"):
                running = round(running + t["pnl_usdc"], 4)
            rows.append([
                t["datetime"], t["asset"], t["window"], t["side"],
                t["buy_shares"], t["buy_price"], t["buy_cost"],
                t["sell_shares"], t["sell_price"], t["sell_revenue"],
                t["status"], t["pnl_usdc"],
                running if t["status"] not in ("OPEN", "OPEN-FLIP") else "OPEN"
            ])
        sheet.clear()
        sheet.update(rows, "A1")
        log.info(f"  Sheets | Synced {len(trades)} trades to Google Sheets ✓")
    except Exception as e:
        log.error(f"Sheet sync error: {e}")


def get_or_create_orderbook_sheet(gc_client):
    """Get or create the Orderbook tab — clears old data on startup."""
    try:
        spreadsheet = gc_client.open_by_key(SHEET_ID)
        try:
            ob_sheet = spreadsheet.worksheet("Orderbook")
        except Exception:
            ob_sheet = spreadsheet.add_worksheet(title="Orderbook", rows=10000, cols=6)
        # Always clear and reset headers on startup
        ob_sheet.clear()
        ob_sheet.update([["time", "asset", "yes", "no", "event", "pnl"]], "A1")
        log.info("  Sheets | Orderbook tab cleared and ready ✓")
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
                    "sell_shares":  float(row.get("sell_shares") or 0),
                    "sell_price":   float(row.get("sell_price") or 0),
                    "sell_revenue": float(row.get("sell_revenue") or 0),
                    "status":       row.get("status", "OPEN"),
                    "pnl_usdc":     float(row.get("pnl_usdc") or 0),
                }
                self.trades.append(trade)
                if trade["status"] in ("OPEN", "OPEN-FLIP"):
                    pending.append({
                        "trade_idx": len(self.trades) - 1,
                        "asset":     trade["asset"],
                        "window":    trade["window"],
                        "side":      trade["side"],
                        "status":    trade["status"],  # needed to restore is_flip on restart
                    })
        return pending

    def record_buy(self, asset, window, side, shares, price, trade_type="OPEN"):
        cost  = round(shares * price, 4)
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
            "status":       trade_type,  # OPEN or OPEN-FLIP
            "pnl_usdc":     0.0,
        }
        self.trades.append(trade)
        label = "FLIP BUY" if trade_type == "OPEN-FLIP" else "BUY"
        log.info(f"  PnL | [{asset.upper()}] {label} {shares} {side} @ {price:.2%} = ${cost:.4f} USDC")
        self._rewrite()
        return len(self.trades) - 1

    def record_sell(self, trade_idx, price, reason="SOLD-99c"):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] not in ("OPEN", "OPEN-FLIP"):
            return
        revenue = round(t["buy_shares"] * price, 4)
        pnl     = round(revenue - t["buy_cost"], 4)
        t["sell_shares"]  = t["buy_shares"]
        t["sell_price"]   = price
        t["sell_revenue"] = revenue
        t["pnl_usdc"]     = pnl
        # Append -FLIP suffix only if this was a flip trade and reason doesn't already have it
        is_flip = t["status"] == "OPEN-FLIP"
        base    = reason.replace("-FLIP", "")  # strip any accidental -FLIP from reason
        t["status"] = (base + "-FLIP") if is_flip else base
        log.info(f"  PnL | [{t['asset'].upper()}] {t['status']} {t['buy_shares']} {t['side']} @ {price:.2%} | pnl={'+' if pnl>=0 else ''}{pnl:.4f} USDC")
        self._rewrite()

    def record_resolved(self, trade_idx, won):
        if trade_idx >= len(self.trades):
            return
        t = self.trades[trade_idx]
        if t["status"] not in ("OPEN", "OPEN-FLIP"):
            return
        price   = 1.0 if won else 0.0
        revenue = round(t["buy_shares"] * price, 4)
        pnl     = round(revenue - t["buy_cost"], 4)
        t["sell_shares"]  = t["buy_shares"]
        t["sell_price"]   = price
        t["sell_revenue"] = revenue
        t["pnl_usdc"]     = pnl
        is_flip = t["status"] == "OPEN-FLIP"
        if won:
            t["status"] = "WIN-FLIP" if is_flip else "WIN"
        else:
            t["status"] = "LOSS-FLIP" if is_flip else "LOSS"
        settled = [x for x in self.trades if x["status"] not in ("OPEN", "OPEN-FLIP")]
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
        open_statuses = ("OPEN", "OPEN-FLIP")
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
                "status", "pnl_usdc", "running_total"
            ])
            for t in self.trades:
                if t["status"] not in ("OPEN", "OPEN-FLIP"):
                    running = round(running + t["pnl_usdc"], 4)
                writer.writerow([
                    t["datetime"], t["asset"], t["window"], t["side"],
                    t["buy_shares"], t["buy_price"], t["buy_cost"],
                    t["sell_shares"], t["sell_price"], t["sell_revenue"],
                    t["status"], t["pnl_usdc"],
                    running if t["status"] not in ("OPEN", "OPEN-FLIP") else "OPEN"
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
        signature_type=1, funder=funder or None,
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
    """On restart, load CSV and resolve any OPEN trades whose windows have closed."""
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


def build_redeem_service(clob_client):
    """Build PolyWeb3Service for redeem operations. Returns None if not configured."""
    if not POLY_WEB3_OK:
        return None
    builder_key  = os.getenv("POLY_BUILDER_API_KEY", "")
    builder_sec  = os.getenv("POLY_BUILDER_SECRET", "")
    builder_pass = os.getenv("POLY_BUILDER_PASSPHRASE", "")
    if not all([builder_key, builder_sec, builder_pass]):
        log.warning("Builder API keys not set — auto-redeem disabled")
        return None
    try:
        funder = os.getenv("POLY_FUNDER_ADDRESS", "")
        relayer = RelayClient(
            RELAYER_URL,
            POLYGON,
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
        log.info("poly-web3 redeem service ready ✓")
        return service
    except Exception as e:
        log.error(f"Redeem service init failed: {e}")
        return None


def get_condition_id(market):
    """Extract conditionId from a Gamma market object."""
    return (
        market.get("conditionId")
        or market.get("condition_id")
        or market.get("questionID")
        or None
    )


def redeem_position(redeem_service, condition_id, asset, side):
    """
    Attempt to redeem a resolved position via poly-web3.
    Returns True on success, False on failure.
    """
    if redeem_service is None:
        return False
    try:
        can_redeem = redeem_service.is_condition_resolved(condition_id)
        if not can_redeem:
            log.info(f"  Redeem | [{asset.upper()} {side}] Condition not resolved yet")
            return False
        result = redeem_service.redeem(condition_id=condition_id)
        log.info(f"  Redeem | [{asset.upper()} {side}] ✓ Redeemed — {result}")
        return True
    except Exception as e:
        log.error(f"  Redeem | [{asset.upper()} {side}] Failed: {e}")
        return False


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

# ── Resolution Sweep ──────────────────────────────────────────────────────────

def resolution_sweep(pnl, redeem_service=None):
    """
    Scan ALL OPEN/OPEN-FLIP trades, check Gamma API for resolution,
    redeem winning positions via poly-web3, update WIN/LOSS,
    sync Sheets, send Telegram summary.
    Called every 20 minutes from the main loop.
    """
    open_statuses = ("OPEN", "OPEN-FLIP")
    stale = [
        (i, t) for i, t in enumerate(pnl.trades)
        if t["status"] in open_statuses
    ]
    if not stale:
        log.info("  Sweep | No open positions to check.")
        return

    log.info(f"  Sweep | Checking {len(stale)} open position(s) for resolution...")
    server_ts = get_server_time()
    resolved_count = 0

    for idx, trade in stale:
        asset  = trade["asset"]
        window = trade["window"]
        side   = trade["side"]

        # Only attempt resolution if window has fully closed (+ 30s buffer)
        if server_ts < window + WINDOW_SECS + 30:
            log.info(f"  Sweep | [{asset.upper()} {side}] Window still live — skipping")
            continue

        try:
            mkt = fetch_market_by_slug(build_slug(asset, window))
            if not mkt:
                log.warning(f"  Sweep | [{asset.upper()} {side}] Market not found for window {window}")
                continue

            result = mkt.get("result") or mkt.get("winner") or mkt.get("resolutionResult")
            if not result:
                log.info(f"  Sweep | [{asset.upper()} {side}] Not resolved yet — still OPEN")
                continue

            won = (result.strip().upper() == side.upper())
            log.info(f"  Sweep | [{asset.upper()} {side}] Resolved → {'WIN ✓' if won else 'LOSS ✗'}")

            # Attempt on-chain redeem for winning positions
            if won:
                cid = get_condition_id(mkt)
                if cid:
                    redeem_position(redeem_service, cid, asset, side)
                else:
                    log.warning(f"  Sweep | [{asset.upper()} {side}] No conditionId found — skipping redeem")

            pnl.record_resolved(idx, won)
            resolved_count += 1

        except Exception as e:
            log.error(f"  Sweep | [{asset.upper()} {side}] Error: {e}")

    log.info(f"  Sweep | Done — {resolved_count} position(s) resolved.")



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
    traded           = set(pending.keys())
    token_cache      = {}
    active_positions = {}
    last_sweep       = 0   # timestamp of last resolution sweep
    for key, items in pending.items():
        active_positions[key] = [
            {
                "trade_idx": i["trade_idx"],
                "side":      i["side"],
                "is_flip":   i.get("status", "OPEN") == "OPEN-FLIP",
            }
            for i in items
        ]

    log.info(f"Bot started — {', '.join(a.upper() for a in ASSETS)}")
    log.info(f"Buy {BUY_PRICE_MIN:.0%}-{BUY_PRICE_MAX:.0%} between {ENTRY_AFTER//60}-{STOP_BUY_AT//60}min | Sell @ {SELL_PRICE:.0%} | Cut loss @ 50%")

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = (window_start + WINDOW_SECS) - server_ts

            # ── 20-min resolution sweep ───────────────────────────────────
            if server_ts - last_sweep >= SWEEP_INTERVAL:
                last_sweep = server_ts
                resolution_sweep(pnl, redeem_service)

            # ── Skip bad hours (MYT) ──────────────────────────────────────
            now_myt  = datetime.now(MYT)
            h, m     = now_myt.hour, now_myt.minute
            in_skip1 = (h == 2 and m >= 30) or (h == 3)          # 02:30-03:59
            in_skip2 = (h == 21) or (h == 22)                     # 21:00-22:59
            if in_skip1 or in_skip2:
                skip_end = "03:00" if in_skip1 else "23:00"
                log.info(f"  Skipping bad hour {h:02d}:{m:02d} MYT — resuming at {skip_end}")
                time.sleep(900)
                continue

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

                # ── Record orderbook to Google Sheets (min 10-15 only, within window) ───────
                if ENTRY_AFTER <= secs_into <= WINDOW_SECS:
                    yes_price_ob = get_midpoint(client, yes_token)
                    no_price_ob  = get_midpoint(client, no_token)
                    if yes_price_ob > 0 and no_price_ob > 0:
                        # Check if this asset has an active position
                        has_position = any(
                            pa == asset for (pa, pw) in active_positions
                            if pw == window_start
                        )
                        event = "holding" if has_position else "watching"
                        ob_record(asset, yes_price_ob, no_price_ob, event)

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

                        # Flip trade — sell at 99c only, NO cut loss, hold to resolution
                        if pos.get("is_flip"):
                            if price >= SELL_PRICE:
                                log.info(f"  [{pos_asset.upper()} {side}] FLIP SELL @ {price:.2%}!")
                                sp = market_sell(client, token_id, BUY_SHARES, price, f"{pos_asset.upper()}-{side}-FLIP")
                                if sp is not None:
                                    pnl.record_sell(idx, sp, "SOLD-99c")
                                    positions.remove(pos)
                            continue

                        buy_price      = pnl.trades[idx]["buy_price"]
                        cut_loss_price = round(buy_price * 0.50, 4)

                        # Cut loss instantly at 50% of buy price
                        if price <= cut_loss_price:
                            log.info(f"  [{pos_asset.upper()} {side}] CUT LOSS @ {price:.2%} (bought @ {buy_price:.2%})")
                            sp = market_sell(client, token_id, BUY_SHARES, price, f"{pos_asset.upper()}-{side}")
                            if sp is not None:
                                ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                         f"*** CUT-LOSS {side} @ {price:.2%} ***")
                                pnl.record_sell(idx, sp, "CUT-LOSS")
                                positions.remove(pos)

                                # Flip to opposite side — only if not already a flip
                                if not pos.get("is_flip"):
                                    opp_side  = "NO" if side == "YES" else "YES"
                                    opp_price = get_midpoint(client, opp_id)
                                    if opp_price < 0.50:
                                        log.info(f"  [{pos_asset.upper()}] FLIP skipped -- {opp_side} @ {opp_price:.2%} too cheap (<50c)")
                                        ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                                 f"FLIP SKIPPED {opp_side} @ {opp_price:.2%} too cheap")
                                    elif opp_price >= 0.75:
                                        log.info(f"  [{pos_asset.upper()}] FLIP skipped -- {opp_side} @ {opp_price:.2%} too expensive (>=75c)")
                                        ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                                 f"FLIP SKIPPED {opp_side} @ {opp_price:.2%} too expensive")
                                    else:
                                        log.info(f"  [{pos_asset.upper()}] FLIP -> buying {opp_side} @ {opp_price:.2%}")
                                        flip_fill = market_buy(client, opp_id, BUY_SHARES, opp_price, f"{pos_asset.upper()}-{opp_side}-FLIP")
                                        if flip_fill is not None:
                                            ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                                     f"*** FLIP BUY {opp_side} @ {opp_price:.2%} ***")
                                            flip_idx = pnl.record_buy(pos_asset, pos_window, opp_side, BUY_SHARES, flip_fill, "OPEN-FLIP")
                                            positions.append({
                                                "trade_idx": flip_idx,
                                                "side":       opp_side,
                                                "is_flip":    True,
                                            })
                                            pending.setdefault(pos_key, []).append({
                                                "trade_idx": flip_idx,
                                                "asset":     pos_asset,
                                                "side":      opp_side,
                                            })
                                            log.info(f"  [{pos_asset.upper()}] FLIP complete -- now holding {opp_side} @ {flip_fill:.2%}")
                                else:
                                    log.info(f"  [{pos_asset.upper()} {side}] Already flipped once -- no more flips this window")
                            continue

                        # Sell main at 98c
                        if price >= SELL_PRICE:
                            log.info(f"  [{pos_asset.upper()} {side}] TRIGGER SELL @ {price:.2%}!")
                            sp = market_sell(client, token_id, BUY_SHARES, price, f"{pos_asset.upper()}-{side}")
                            if sp is not None:
                                ob_record(pos_asset, get_midpoint(client, pyt), get_midpoint(client, pnt),
                                         f"*** SELL {side} @ {price:.2%} ***")
                                pnl.record_sell(idx, sp, "SOLD-99c")
                                positions.remove(pos)

                    # Resolve closed windows — only if market has officially resolved
                    # (price monitoring above continues to sell at 99.9c even after window)
                    if server_ts > pos_window + WINDOW_SECS + 30:
                        try:
                            mkt = fetch_market_by_slug(build_slug(pos_asset, pos_window))
                            if mkt:
                                result = mkt.get("result") or mkt.get("winner") or mkt.get("resolutionResult")
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

                # ── Only buy between 10-14 minutes ────────────────────────────
                if secs_into < ENTRY_AFTER:
                    continue
                if secs_into > STOP_BUY_AT:
                    continue

                # ── Check buy trigger ─────────────────────────────────────────
                yes_price = get_midpoint(client, yes_token)
                no_price  = get_midpoint(client, no_token)

                if BUY_PRICE_MIN <= yes_price <= BUY_PRICE_MAX:
                    log.info(f"  [{asset.upper()}] TRIGGER: YES @ {yes_price:.2%}")
                    fill = market_buy(client, yes_token, BUY_SHARES, yes_price, f"{asset.upper()}-YES")
                    if fill is not None:
                        ob_record(asset, yes_price, no_price, f"*** BUY YES @ {yes_price:.2%} ***")
                        idx = pnl.record_buy(asset, window_start, "YES", BUY_SHARES, fill)
                        pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": "YES"})
                        active_positions.setdefault(key, []).append({"trade_idx": idx, "side": "YES", "is_flip": False})
                        traded.add(key)

                elif BUY_PRICE_MIN <= no_price <= BUY_PRICE_MAX:
                    log.info(f"  [{asset.upper()}] TRIGGER: NO @ {no_price:.2%}")
                    fill = market_buy(client, no_token, BUY_SHARES, no_price, f"{asset.upper()}-NO")
                    if fill is not None:
                        ob_record(asset, yes_price, no_price, f"*** BUY NO @ {no_price:.2%} ***")
                        idx = pnl.record_buy(asset, window_start, "NO", BUY_SHARES, fill)
                        pending.setdefault(key, []).append({"trade_idx": idx, "asset": asset, "side": "NO"})
                        active_positions.setdefault(key, []).append({"trade_idx": idx, "side": "NO", "is_flip": False})
                        traded.add(key)

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
