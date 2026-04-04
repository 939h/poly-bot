"""
Polymarket 15-Min Crypto Up/Down — Rebound Detector
====================================================
Monitors both YES and NO token prices for BTC/ETH/SOL/XRP on Polymarket
15-min up/down markets.

State machine per token (asset × side = 8 machines):
  watching      → in_trough     : price < TROUGH_THRESHOLD (0.05)
  in_trough     → in_trough     : price < trough_min  (update rolling min)
  in_trough     → tracking_peak : price > REBOUND_MULT × trough_min
                                   (2x signal fires — log immediately)
  tracking_peak → tracking_peak : track max price until window ends
  window end    → write CSV row with full peak data, then reset

CSV output (rebound_signals.csv):
  datetime_utc8, asset, side, window_start,
  trough_price, trough_time,
  signal_price, signal_time, signal_ratio,
  peak_price, peak_time, peak_ratio

Requirements:
    pip install py-clob-client requests python-dotenv colorama

.env keys (only needed when DRY_RUN=false):
    POLY_PRIVATE_KEY=0x...
    POLY_API_KEY=...
    POLY_API_SECRET=...
    POLY_API_PASSPHRASE=...
    POLY_FUNDER_ADDRESS=0x...
    DRY_RUN=true
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

load_dotenv()

# =============================================================================
#  USER SETTINGS
# =============================================================================
ASSETS           = ["btc", "eth", "sol", "xrp"]
TROUGH_THRESHOLD = 0.05   # price must drop below this to begin tracking
REBOUND_MULT     = 2.0    # signal fires when price > REBOUND_MULT × trough_min
POLL_SECS        = 1.0
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() != "false"

# =============================================================================
#  INTERNAL CONSTANTS
# =============================================================================
GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
INTERVAL    = "15m"
WINDOW_SECS = 900
CSV_FILE    = "rebound_signals.csv"
UTC8        = timezone(timedelta(hours=8))

# Keys for per-token state: "btc_yes", "btc_no", "eth_yes", ...
SIDES     = ["yes", "no"]
ALL_KEYS  = [f"{asset}_{side}" for asset in ASSETS for side in SIDES]

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
#  INFRASTRUCTURE HELPERS  (patterns from panic_bot6.py)
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
    """Fetch from Gamma /markets; falls back to /events if not found."""
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
#  TOKEN CACHE  — stores (yes_tok, no_tok) per (asset, window_start)
# =============================================================================
token_cache = {}   # (asset, window_start) -> (yes_tok, no_tok)


def get_tokens_cached(asset, window_start):
    """Return (yes_tok, no_tok) for asset/window using cache. Either may be None."""
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
    """Return a clean per-token state dict for a new window."""
    return {
        "phase":        "watching",   # watching | in_trough | tracking_peak
        "trough_min":   None,         # lowest price while in_trough
        "trough_time":  None,         # unix timestamp of trough_min
        "signal_price": None,         # price at 2× trigger
        "signal_time":  None,         # unix timestamp of trigger
        "peak_price":   None,         # max price observed after trigger
        "peak_time":    None,         # unix timestamp of peak_price
    }


# Module-level runtime state — keyed by "asset_side" e.g. "btc_yes"
token_states = {key: fresh_state() for key in ALL_KEYS}
last_window  = None


def update_state(key, price, server_ts):
    """
    Advance the token state machine.
    Returns True if the 2× signal just fired (for logging), else False.
    In tracking_peak phase, silently updates peak_price.
    """
    s = token_states[key]
    label = key.upper()

    if s["phase"] == "tracking_peak":
        # Keep tracking max price until window resets
        if price > s["peak_price"]:
            s["peak_price"] = price
            s["peak_time"]  = server_ts
            log.info("[%s] PEAK updated @ %.4f  (ratio=%.2fx trough)",
                     label, price, price / s["trough_min"])
        return False

    if s["phase"] == "watching":
        if price < TROUGH_THRESHOLD:
            s["phase"]      = "in_trough"
            s["trough_min"] = price
            s["trough_time"]= server_ts
            log.info("[%s] TROUGH entered @ %.4f", label, price)
        return False

    # phase == "in_trough"
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
        log.info(
            "[%s] SIGNAL REBOUND! price=%.4f trough=%.4f ratio=%.2fx — tracking peak...",
            label, price, s["trough_min"], ratio,
        )
        return True

    return False

# =============================================================================
#  CSV RECORDING  — written at window end with full peak data
# =============================================================================
_CSV_HEADERS = [
    "datetime_utc8", "asset", "side", "window_start",
    "trough_price", "trough_time",
    "signal_price", "signal_time", "signal_ratio",
    "peak_price",   "peak_time",   "peak_ratio",
]


def _ts_str(ts):
    return datetime.fromtimestamp(ts, tz=UTC8).strftime("%Y-%m-%d %H:%M:%S")


def flush_completed_signals(window_start):
    """
    Called at window end. Write one CSV row for every token that reached
    tracking_peak this window, with its final recorded peak price.
    """
    file_exists = os.path.isfile(CSV_FILE)
    rows = []
    now_str    = datetime.now(UTC8).strftime("%Y-%m-%d %H:%M:%S")
    window_str = datetime.fromtimestamp(window_start, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for key, s in token_states.items():
        if s["phase"] != "tracking_peak":
            continue
        asset, side = key.rsplit("_", 1)
        sig_ratio  = round(s["signal_price"] / s["trough_min"], 4) if s["trough_min"] else 0
        peak_ratio = round(s["peak_price"]   / s["trough_min"], 4) if s["trough_min"] else 0
        rows.append([
            now_str,
            asset.upper(),
            side.upper(),
            window_str,
            round(s["trough_min"],   4),  _ts_str(s["trough_time"]),
            round(s["signal_price"], 4),  _ts_str(s["signal_time"]),  sig_ratio,
            round(s["peak_price"],   4),  _ts_str(s["peak_time"]),    peak_ratio,
        ])
        log.info(
            "[%s] PEAK FINAL  signal=%.4f peak=%.4f peak_ratio=%.2fx",
            key.upper(), s["signal_price"], s["peak_price"], peak_ratio,
        )

    if not rows:
        return

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(_CSV_HEADERS)
        writer.writerows(rows)

    log.info("[CSV] Wrote %d signal(s) for window %s", len(rows), window_str)

# =============================================================================
#  DRY-RUN PRICE SIMULATION  — separate walk per "asset_side" key
# =============================================================================
_sim_prices = {key: 0.50 for key in ALL_KEYS}


def simulate_price(key):
    """
    Random-walk price simulator for DRY_RUN mode.
    YES and NO are simulated independently so either side can trigger.
    """
    p = _sim_prices[key]
    r = random.random()
    if r < 0.02:
        p = max(0.01, p * random.uniform(0.05, 0.30))   # sharp dip → trough
    elif r < 0.10:
        p = min(0.99, p * random.uniform(2.5, 4.5))     # spike → rebound
    else:
        p = max(0.01, min(0.99, p * random.uniform(0.97, 1.03)))
    _sim_prices[key] = round(p, 4)
    return _sim_prices[key]

# =============================================================================
#  MAIN LOOP
# =============================================================================

def main():
    global last_window, token_states, token_cache, _sim_prices

    client = build_client()

    log.info("=" * 60)
    log.info("rebound_detector  |  DRY_RUN=%s", DRY_RUN)
    log.info("Assets      : %s", ", ".join(a.upper() for a in ASSETS))
    log.info("Sides       : YES + NO")
    log.info("Trough cap  : %.2f", TROUGH_THRESHOLD)
    log.info("Rebound mult: %.1fx", REBOUND_MULT)
    log.info("CSV output  : %s", CSV_FILE)
    log.info("=" * 60)

    while True:
        try:
            server_ts    = get_server_time()
            window_start = get_current_window_start(server_ts)
            secs_into    = server_ts - window_start
            secs_left    = WINDOW_SECS - secs_into

            # ── New window ─────────────────────────────────────────────────
            if window_start != last_window:
                # Flush completed signals before resetting
                if last_window is not None:
                    flush_completed_signals(last_window)

                token_states = {key: fresh_state() for key in ALL_KEYS}
                token_cache  = {}
                if DRY_RUN:
                    _sim_prices = {key: 0.50 for key in ALL_KEYS}
                log.info(
                    "[WINDOW] New window start=%d  secs_left=%d",
                    window_start, secs_left,
                )
                last_window = window_start

            # ── Per-token price fetch and state update ─────────────────────
            for asset in ASSETS:
                if DRY_RUN:
                    yes_price = simulate_price(f"{asset}_yes")
                    no_price  = simulate_price(f"{asset}_no")
                else:
                    yes_tok, no_tok = get_tokens_cached(asset, window_start)
                    yes_price = get_midpoint(client, yes_tok) if yes_tok else 0.0
                    no_price  = get_midpoint(client, no_tok)  if no_tok  else 0.0

                if yes_price > 0:
                    update_state(f"{asset}_yes", yes_price, server_ts)
                if no_price > 0:
                    update_state(f"{asset}_no",  no_price,  server_ts)

        except KeyboardInterrupt:
            log.info("Flushing final signals before exit...")
            if last_window is not None:
                flush_completed_signals(last_window)
            log.info("Stopped by user.")
            break
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
