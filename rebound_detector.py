"""
Polymarket 15-Min Crypto Up/Down — Rebound Detector
====================================================
Signal logic (per asset, per window):
  1. Watch YES token price.
  2. If price drops below TROUGH_THRESHOLD (0.05), enter "in_trough" state
     and start tracking the rolling minimum.
  3. If price later rises above REBOUND_MULT (2.0×) the rolling minimum,
     fire a REBOUND SIGNAL and record it to rebound_signals.csv.
  4. All state resets on each new 15-min window.

No trading — detection and recording only.

State machine per asset:
  watching  → in_trough : price < TROUGH_THRESHOLD
  in_trough → in_trough : price < trough_min  (update min)
  in_trough → rebounded : price > REBOUND_MULT × trough_min  (fire signal)
  rebounded → (frozen)  : no further transitions until next window

CSV output (rebound_signals.csv):
  datetime_utc8, asset, window_start, trough_price, trough_time,
  current_price, ratio

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
TROUGH_THRESHOLD = 0.05    # price must drop below this to begin tracking
REBOUND_MULT     = 2.0     # signal fires when price > REBOUND_MULT × trough_min
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
#  STATE MACHINE
# =============================================================================

def fresh_state():
    """Return a clean per-asset state dict for a new window."""
    return {
        "phase":       "watching",   # "watching" | "in_trough" | "rebounded"
        "trough_min":  None,         # lowest price seen while in_trough
        "trough_time": None,         # unix timestamp of trough_min observation
    }


# Module-level runtime state
asset_states = {asset: fresh_state() for asset in ASSETS}
token_cache  = {}   # (asset, window_start) -> yes_token_id
last_window  = None


def update_state(asset, price, server_ts):
    """
    Advance the asset state machine with a new price.
    Returns a signal dict if a REBOUND fires, else None.
    """
    s = asset_states[asset]

    if s["phase"] == "rebounded":
        return None  # already fired this window

    if s["phase"] == "watching":
        if price < TROUGH_THRESHOLD:
            s["phase"]       = "in_trough"
            s["trough_min"]  = price
            s["trough_time"] = server_ts
            log.info("[%s] TROUGH entered @ %.4f", asset.upper(), price)
        return None

    # phase == "in_trough"
    if price < s["trough_min"]:
        s["trough_min"]  = price
        s["trough_time"] = server_ts
        log.info("[%s] TROUGH new low @ %.4f", asset.upper(), price)

    ratio = price / s["trough_min"] if s["trough_min"] > 0 else 0.0
    if ratio > REBOUND_MULT:
        s["phase"] = "rebounded"
        log.info(
            "[%s] SIGNAL REBOUND! price=%.4f trough=%.4f ratio=%.2fx",
            asset.upper(), price, s["trough_min"], ratio,
        )
        return {
            "asset":         asset,
            "trough_price":  s["trough_min"],
            "trough_time":   s["trough_time"],
            "current_price": price,
            "ratio":         round(ratio, 4),
        }
    return None

# =============================================================================
#  CSV RECORDING
# =============================================================================
_CSV_HEADERS = [
    "datetime_utc8", "asset", "window_start",
    "trough_price", "trough_time", "current_price", "ratio",
]


def record_signal(signal, window_start):
    """Append one signal row to rebound_signals.csv."""
    file_exists = os.path.isfile(CSV_FILE)
    now_str     = datetime.now(UTC8).strftime("%Y-%m-%d %H:%M:%S")
    trough_dt   = datetime.fromtimestamp(signal["trough_time"], tz=UTC8).strftime("%Y-%m-%d %H:%M:%S")
    window_dt   = datetime.fromtimestamp(window_start, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(_CSV_HEADERS)
        writer.writerow([
            now_str,
            signal["asset"].upper(),
            window_dt,
            round(signal["trough_price"],  4),
            trough_dt,
            round(signal["current_price"], 4),
            signal["ratio"],
        ])
    log.info("[CSV] Recorded: %s trough=%.4f peak=%.4f ratio=%.2fx",
             signal["asset"].upper(), signal["trough_price"],
             signal["current_price"], signal["ratio"])

# =============================================================================
#  TOKEN CACHE
# =============================================================================

def get_yes_token(asset, window_start):
    """Return YES token_id for asset/window using cache. Returns None on failure."""
    key = (asset, window_start)
    if key in token_cache:
        return token_cache[key]
    slug = build_slug(asset, window_start)
    mkt  = fetch_market_by_slug(slug)
    if not mkt:
        log.warning("[%s] Market not found: %s", asset.upper(), slug)
        return None
    yes_tok, _ = get_tokens(mkt)
    if not yes_tok:
        log.warning("[%s] Token parse failed for %s", asset.upper(), slug)
        return None
    token_cache[key] = yes_tok
    log.info("[%s] Cached YES token for window %d", asset.upper(), window_start)
    return yes_tok

# =============================================================================
#  DRY-RUN PRICE SIMULATION
# =============================================================================
_sim_prices = {asset: 0.50 for asset in ASSETS}


def simulate_price(asset):
    """
    Random-walk price simulator for DRY_RUN mode.
    Occasionally creates sharp dips below TROUGH_THRESHOLD followed by
    rebounds, so the state machine can be exercised without live data.
    """
    p = _sim_prices[asset]
    r = random.random()
    if r < 0.02:
        # Sharp drop toward trough zone
        p = max(0.01, p * random.uniform(0.05, 0.30))
    elif r < 0.10:
        # Rebound spike — tests rebound detection
        p = min(0.99, p * random.uniform(2.5, 4.5))
    else:
        # Normal random walk ±3%
        p = max(0.01, min(0.99, p * random.uniform(0.97, 1.03)))
    _sim_prices[asset] = round(p, 4)
    return _sim_prices[asset]

# =============================================================================
#  MAIN LOOP
# =============================================================================

def main():
    global last_window, asset_states, token_cache, _sim_prices

    client = build_client()

    log.info("=" * 60)
    log.info("rebound_detector  |  DRY_RUN=%s", DRY_RUN)
    log.info("Assets      : %s", ", ".join(a.upper() for a in ASSETS))
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

            # ── New window — reset all state ──────────────────────────────
            if window_start != last_window:
                asset_states = {asset: fresh_state() for asset in ASSETS}
                token_cache  = {}
                if DRY_RUN:
                    _sim_prices = {asset: 0.50 for asset in ASSETS}
                log.info(
                    "[WINDOW] New window start=%d  secs_left=%d",
                    window_start, secs_left,
                )
                last_window = window_start

            # ── Per-asset price fetch and state update ────────────────────
            for asset in ASSETS:
                if DRY_RUN:
                    price = simulate_price(asset)
                else:
                    yes_tok = get_yes_token(asset, window_start)
                    if not yes_tok:
                        continue
                    price = get_midpoint(client, yes_tok)
                    if price <= 0:
                        continue

                signal = update_state(asset, price, server_ts)
                if signal:
                    record_signal(signal, window_start)

        except KeyboardInterrupt:
            log.info("Stopped by user.")
            break
        except Exception as e:
            log.error("Poll error: %s", e)

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
