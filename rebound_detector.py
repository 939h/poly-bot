"""
Polymarket 15-Min Crypto Up/Down — Rebound Detector
====================================================
Monitors both YES and NO token prices for BTC/ETH/SOL/XRP on Polymarket
15-min up/down markets.

State machine per token (asset × side = 8 machines):
  watching      → in_trough     : price < TROUGH_THRESHOLD (0.05)
  in_trough     → in_trough     : price < trough_min  (update rolling min)
  in_trough     → tracking_peak : price > REBOUND_MULT × trough_min
                                   → "signal" row written immediately
  tracking_peak → tracking_peak : track max price; "peak_update" row every 60s
  window end    → "peak_final" row written, then reset

Google Sheet (tab: "Rebound Signals") + CSV (rebound_signals.csv):
  datetime_utc8, asset, side, window_start,
  trough_price, trough_time,
  signal_price, signal_time, signal_ratio,
  peak_price, peak_time, peak_ratio, record_type

record_type values:
  "signal"      — written immediately when 2× rebound fires
  "peak_update" — written every 60s while tracking the peak
  "peak_final"  — written at window end with the absolute peak

Requirements:
    pip install py-clob-client requests python-dotenv colorama gspread google-auth

.env keys:
    GOOGLE_SHEET_ID=1X9...          # required for Sheets
    GOOGLE_CREDS_JSON={"type":...}  # service account JSON
    DRY_RUN=true                    # set false for live prices
    POLY_PRIVATE_KEY=0x...          # only needed when DRY_RUN=false
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
"""
import csv
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client requests python-dotenv")
    sys.exit(1)

try:
    import colorama
    from colorama import Fore, Style
    colorama.init()
    COLORS = True
except ImportError:
    COLORS = False

try:
    import gspread
    from google.oauth2.service_account import Credentials as GCredentials
    GSHEETS_OK = True
except ImportError:
    GSHEETS_OK = False
    print("Warning: gspread not installed — Google Sheets disabled. Run: pip install gspread google-auth")

load_dotenv()

# =============================================================================
#  USER SETTINGS
# =============================================================================
ASSETS           = ["btc", "eth", "sol"]
TROUGH_THRESHOLD = 0.10   # price must drop below this to begin tracking
REBOUND_MULT     = 1.5    # signal fires when price > REBOUND_MULT × trough_min
POLL_SECS        = 1.0
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"
SHEET_ID         = os.getenv("GOOGLE_SHEET_ID", "")
SHEET_TAB        = "Rebound Signals"

# =============================================================================
#  INTERNAL CONSTANTS
# =============================================================================
GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
INTERVAL    = "15m"
WINDOW_SECS = 900
CSV_FILE    = "rebound_signals.csv"
UTC8        = timezone(timedelta(hours=8))

SIDES    = ["yes", "no"]
ALL_KEYS = [f"{asset}_{side}" for asset in ASSETS for side in SIDES]

# =============================================================================
#  LOGGING
# =============================================================================
class _ColorFormatter(logging.Formatter):
    def format(self, record):
        msg = super().format(record)
        if not COLORS:
            return msg
        if "SIGNAL"  in msg: return Fore.GREEN  + Style.BRIGHT + msg + Style.RESET_ALL
        if "TROUGH"  in msg: return Fore.YELLOW + msg + Style.RESET_ALL
        if "PEAK"    in msg: return Fore.CYAN   + Style.BRIGHT + msg + Style.RESET_ALL
        if "WINDOW"  in msg: return Fore.CYAN   + msg + Style.RESET_ALL
        if "ERROR"   in msg: return Fore.RED    + msg + Style.RESET_ALL
        return msg

_fmt = "%(asctime)s [%(levelname)s] %(message)s"
_console = logging.StreamHandler(
    open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
)
_console.setFormatter(_ColorFormatter(_fmt))
_file = logging.FileHandler("rebound_detector.log", encoding="utf-8")
_file.setFormatter(logging.Formatter(_fmt))
logging.basicConfig(level=logging.INFO, handlers=[_console, _file], force=True)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# =============================================================================
#  INFRASTRUCTURE HELPERS
# =============================================================================

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
    if not raw or len(raw) < 2:
        return None, None
    return raw[0].strip(), raw[1].strip()


def get_midpoint(client, token_id):
    try:
        return float(client.get_midpoint(token_id)["mid"])
    except Exception:
        return 0.0


def build_client():
    if DRY_RUN:
        log.info("[DRY RUN] No CLOB client — prices will be simulated")
        return None
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        log.error("Missing .env keys: POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE")
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

# =============================================================================
#  RECORDING — CSV + Google Sheets
# =============================================================================
_CSV_HEADERS = [
    "datetime_utc8", "asset", "side", "window_start",
    "trough_price", "trough_time",
    "signal_price", "signal_time", "signal_ratio",
    "peak_price",   "peak_time",   "peak_ratio",
    "record_type",          # signal | peak_update | peak_final
    "signal_period_sec",    # signal_time - trough_time  (int)
    "peak_period_sec",      # peak_time   - signal_time  (int)
]

_sheet_ws        = None   # "Rebound Signals" worksheet
_sheet_row_count = 1      # tracks total rows written (header = 1); updated on connect + write
_window_colors   = {}     # window_str -> 0 (plain) or 1 (banded)
_color_flip      = [0]    # alternates 0/1 per new window

# Light blue for banded window rows; plain windows keep default white
BAND_COLOR = {"red": 0.851, "green": 0.918, "blue": 0.988}

# Column index of window_start in _CSV_HEADERS (used for banding)
_WIN_COL = 3  # datetime_utc8(0), asset(1), side(2), window_start(3)


def _window_color_idx(window_str):
    """Return 0 (no fill) or 1 (BAND_COLOR) for the given window string."""
    if window_str not in _window_colors:
        _window_colors[window_str] = _color_flip[0]
        _color_flip[0] ^= 1
    return _window_colors[window_str]


def _ts_str(ts):
    return datetime.fromtimestamp(ts, tz=UTC8).strftime("%Y-%m-%d %H:%M:%S")


def _make_row(key, s, window_start, record_type):
    now_str    = datetime.now(UTC8).strftime("%Y-%m-%d %H:%M:%S")
    window_str = datetime.fromtimestamp(window_start, tz=UTC8).strftime("%Y-%m-%d %H:%M:%S")
    asset, side = key.rsplit("_", 1)
    sig_ratio     = round(s["signal_price"] / s["trough_min"], 4) if s["trough_min"] else 0
    signal_period = int(s["signal_time"] - s["trough_time"]) if s["trough_time"] and s["signal_time"] else 0
    if record_type == "signal":
        # peak not yet tracked — leave peak columns blank
        peak_price_val  = ""
        peak_time_val   = ""
        peak_ratio_val  = ""
        peak_period_val = ""
    else:
        peak_ratio      = round(s["peak_price"] / s["trough_min"], 4) if s["trough_min"] else 0
        peak_price_val  = round(s["peak_price"], 4)
        peak_time_val   = _ts_str(s["peak_time"])
        peak_ratio_val  = peak_ratio
        peak_period_val = int(s["peak_time"] - s["signal_time"]) if s["signal_time"] and s["peak_time"] else 0
    return [
        now_str,
        asset.upper(),
        side.upper(),
        window_str,
        round(s["trough_min"],   4),  _ts_str(s["trough_time"]),
        round(s["signal_price"], 4),  _ts_str(s["signal_time"]),  sig_ratio,
        peak_price_val,               peak_time_val,              peak_ratio_val,
        record_type,
        signal_period,
        peak_period_val,
    ]


def reset_csv_if_needed():
    """
    Check CSV header against _CSV_HEADERS. If missing or mismatched,
    back up the old file and start a fresh one with correct headers.
    """
    if not os.path.isfile(CSV_FILE):
        return  # will be created on first write
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        first = next(csv.reader(f), None)
    if first == _CSV_HEADERS:
        return  # all good
    # Header mismatch — rename old file and create fresh one
    backup = CSV_FILE + ".bak"
    os.replace(CSV_FILE, backup)
    with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(_CSV_HEADERS)
    log.warning("[CSV] Header mismatch — old file backed up to %s, fresh CSV created", backup)


def write_rows(rows):
    """Append rows to Google Sheet (with window color banding) and CSV."""
    global _sheet_row_count
    if not rows:
        return

    # ── Google Sheets ─────────────────────────────────────────────────────
    if _sheet_ws is not None:
        try:
            prev = _sheet_row_count
            _sheet_ws.append_rows(rows, value_input_option="USER_ENTERED")
            _sheet_row_count += len(rows)

            # Group rows by window_start value to apply color per window
            win_groups = {}
            for offset, row in enumerate(rows):
                win = row[_WIN_COL] if len(row) > _WIN_COL else ""
                win_groups.setdefault(win, []).append(prev + 1 + offset)  # 1-indexed

            for win, row_nums in win_groups.items():
                if _window_color_idx(win) == 1:
                    r1, r2 = min(row_nums), max(row_nums)
                    _sheet_ws.format(f"A{r1}:Z{r2}", {"backgroundColor": BAND_COLOR})
        except Exception as e:
            log.error("[Sheets] write failed: %s", e)

    # ── CSV backup ────────────────────────────────────────────────────────
    file_exists = os.path.isfile(CSV_FILE)
    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(_CSV_HEADERS)
        writer.writerows(rows)


# =============================================================================
#  GOOGLE SHEETS
# =============================================================================

def _get_or_create_ws(spreadsheet, title, rows, cols, headers):
    """Get or create a worksheet; write headers if A1 is blank."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
        log.info("[Sheets] Created tab '%s'", title)
    if not ws.acell("A1").value:
        ws.append_row(headers)
        log.info("[Sheets] Wrote headers to '%s'", title)
    return ws


def connect_sheet():
    """Connect both worksheets; creates tabs + headers if missing."""
    global _sheet_ws, _sheet_row_count
    if not GSHEETS_OK:
        return None
    if not SHEET_ID:
        log.warning("GOOGLE_SHEET_ID not set — Sheets disabled")
        return None
    creds_json = os.getenv("GOOGLE_CREDS_JSON", "")
    if not creds_json:
        log.warning("GOOGLE_CREDS_JSON not set — Sheets disabled")
        return None
    try:
        creds_dict = json.loads(creds_json)
        creds      = GCredentials.from_service_account_info(
            creds_dict,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc          = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(SHEET_ID)
        _sheet_ws        = _get_or_create_ws(spreadsheet, SHEET_TAB, 10000, len(_CSV_HEADERS), _CSV_HEADERS)
        _sheet_row_count = len(_sheet_ws.col_values(1))
        log.info("[Sheets] Connected → '%s' (%d existing rows)", SHEET_TAB, _sheet_row_count)
        return _sheet_ws
    except Exception as e:
        log.error("[Sheets] Connect failed: %s", e)
        return None


# =============================================================================
#  TOKEN CACHE
# =============================================================================
token_cache = {}   # (asset, window_start) -> (yes_tok, no_tok)


def get_tokens_cached(asset, window_start):
    key = (asset, window_start)
    if key in token_cache:
        return token_cache[key]
    slug = build_slug(asset, window_start)
    mkt  = fetch_market_by_slug(slug)
    if not mkt:
        log.warning("[%s] Market not found: %s", asset.upper(), slug)
        token_cache[key] = (None, None)
        return (None, None)
    yes_tok, no_tok = get_tokens(mkt)
    token_cache[key] = (yes_tok, no_tok)
    log.info("[%s] Cached YES/NO tokens for window %d", asset.upper(), window_start)
    return (yes_tok, no_tok)

# =============================================================================
#  STATE MACHINE
# =============================================================================

def fresh_state():
    return {
        "phase":        "watching",   # watching | in_trough | tracking_peak
        "trough_min":   None,
        "trough_time":  None,
        "signal_price": None,
        "signal_time":  None,
        "peak_price":   None,
        "peak_time":    None,
    }


token_states = {key: fresh_state() for key in ALL_KEYS}
last_window  = None


def update_state(key, price, server_ts, window_start):
    """
    Advance the state machine. Returns "signal" if 2× just fired, else None.
    """
    s = token_states[key]
    asset, side = key.rsplit("_", 1)
    label = f"{asset.upper()} {side.upper()}"

    if s["phase"] == "tracking_peak":
        if price > s["peak_price"]:
            s["peak_price"] = price
            s["peak_time"]  = server_ts
            log.info("[%s] PEAK updated @ %.4f  (%.2fx trough)", label, price, price / s["trough_min"])
        return None

    if s["phase"] == "watching":
        if price < TROUGH_THRESHOLD:
            s["phase"]       = "in_trough"
            s["trough_min"]  = price
            s["trough_time"] = server_ts
            log.info("[%s] TROUGH entered @ %.4f", label, price)
        return None

    # in_trough
    if price < s["trough_min"]:
        s["trough_min"]  = price
        s["trough_time"] = server_ts
        log.info("[%s] TROUGH new low @ %.4f", label, price)

    ratio = price / s["trough_min"] if s["trough_min"] > 0 else 0.0
    if ratio > REBOUND_MULT:
        s["phase"]        = "tracking_peak"
        s["signal_price"] = price
        s["signal_time"]  = server_ts
        s["peak_price"]   = price
        s["peak_time"]    = server_ts
        log.info("[%s] SIGNAL REBOUND! price=%.4f trough=%.4f ratio=%.2fx",
                 label, price, s["trough_min"], ratio)
        return "signal"

    return None

# =============================================================================
#  DRY-RUN SIMULATION
# =============================================================================
_sim_prices = {key: 0.50 for key in ALL_KEYS}


def simulate_price(key):
    p = _sim_prices[key]
    r = random.random()
    if r < 0.02:
        p = max(0.01, p * random.uniform(0.05, 0.30))
    elif r < 0.10:
        p = min(0.99, p * random.uniform(2.5, 4.5))
    else:
        p = max(0.01, min(0.99, p * random.uniform(0.97, 1.03)))
    _sim_prices[key] = round(p, 4)
    return _sim_prices[key]

# =============================================================================
#  MAIN LOOP
# =============================================================================

def main():
    global last_window, token_states, token_cache, _sim_prices

    client           = build_client()
    rebounded_assets = set()
    reset_csv_if_needed()
    connect_sheet()

    log.info("=" * 60)
    log.info("rebound_detector  |  DRY_RUN=%s", DRY_RUN)
    log.info("Assets      : %s", ", ".join(a.upper() for a in ASSETS))
    log.info("Sides       : YES + NO")
    log.info("Trough cap  : %.2f", TROUGH_THRESHOLD)
    log.info("Rebound mult: %.1fx", REBOUND_MULT)
    log.info("Sheet output: %s (tab: %s)", SHEET_ID[:20] + "..." if SHEET_ID else "disabled", SHEET_TAB)
    log.info("CSV backup  : %s", CSV_FILE)
    log.info("=" * 60)

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = WINDOW_SECS - secs_into

            # ── New window — flush finals, then reset ──────────────────────
            if window_start != last_window:
                if last_window is not None:
                    # Write peak_final for all tracking tokens
                    rows = [
                        _make_row(k, s, last_window, "peak_final")
                        for k, s in token_states.items()
                        if s["phase"] == "tracking_peak"
                        and s["peak_price"] != s["signal_price"]
                    ]
                    if rows:
                        write_rows(rows)
                        log.info("[WINDOW] Wrote %d peak_final row(s)", len(rows))

                token_states     = {key: fresh_state() for key in ALL_KEYS}
                token_cache      = {}
                rebounded_assets = set()
                if DRY_RUN:
                    _sim_prices = {key: 0.50 for key in ALL_KEYS}
                log.info("[WINDOW] New window start=%d  secs_left=%d", window_start, secs_left)
                last_window = window_start

            # ── Per-token price fetch and state update ─────────────────────
            # Stop scanning 10s before window ends (flush happens at next window start)
            if secs_left <= 10:
                time.sleep(POLL_SECS)
                continue

            signal_rows = []
            for asset in ASSETS:
                yes_key = f"{asset}_yes"
                no_key  = f"{asset}_no"
                yes_phase = token_states[yes_key]["phase"]
                no_phase  = token_states[no_key]["phase"]

                # Skip asset only when already rebounded AND no side is tracking a peak
                if asset in rebounded_assets \
                        and yes_phase != "tracking_peak" \
                        and no_phase  != "tracking_peak":
                    continue

                if DRY_RUN:
                    yes_price = simulate_price(yes_key)
                    no_price  = simulate_price(no_key)
                else:
                    yes_tok, no_tok = get_tokens_cached(asset, window_start)
                    yes_price = get_midpoint(client, yes_tok) if yes_tok else 0.0
                    no_price  = get_midpoint(client, no_tok)  if no_tok  else 0.0

                for key, price in [(yes_key, yes_price), (no_key, no_price)]:
                    if price <= 0.025:
                        continue
                    result = update_state(key, price, server_ts, window_start)
                    if result == "signal":
                        rebounded_assets.add(asset)
                        signal_rows.append(_make_row(key, token_states[key], window_start, "signal"))

            if signal_rows:
                write_rows(signal_rows)
                log.info("[RECORD] Wrote %d signal row(s) immediately", len(signal_rows))

        except KeyboardInterrupt:
            log.info("Flushing final signals before exit...")
            rows = [
                _make_row(k, s, last_window, "peak_final")
                for k, s in token_states.items()
                if s["phase"] == "tracking_peak"
                and last_window is not None
                and s["peak_price"] != s["signal_price"]
            ]
            if rows:
                write_rows(rows)
            log.info("Stopped by user.")
            break
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
