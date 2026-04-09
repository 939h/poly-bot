"""
binance_ws.py
=============
Runs as a background thread alongside panic_bot6.py.
Connects to Binance WebSocket — ETHUSDT + SOLUSDT 15m candles.
Computes RSI(7) on real candle closes — same data as TradingView.
Writes into shared dict  rsi_data  which panic_bot6 reads every poll.

On startup: pre-fetches last 20 closed candles from Binance REST API
so RSI is ready immediately instead of waiting 2 hours.

Usage in panic_bot6.py:
    from binance_ws import rsi_data, start_rsi_feed
    start_rsi_feed()        # call once inside main(), after build_client()
"""

import json
import threading
import time
import logging
from collections import deque

import requests as _requests   # renamed to avoid conflict if user imports requests elsewhere

try:
    import websocket
except ImportError:
    raise ImportError("Run: pip install websocket-client")

log = logging.getLogger(__name__)

# ── Shared dict — panic_bot6 reads this every poll ───────────────────────────
# Keys  : "eth", "sol"
# Values: float RSI 0-100.  50.0 = warmup / not enough data (neutral)
rsi_data = {
    "eth": 50.0,
    "sol": 50.0,
}

# ── Config ────────────────────────────────────────────────────────────────────
RSI_PERIOD   = 5
CANDLE_LIMIT = 10   # rolling window of closes — must be > RSI_PERIOD + 1
SYMBOL_MAP   = {
    "ethusdt": "eth",
    "solusdt": "sol",
}
WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=ethusdt@kline_15m/solusdt@kline_15m"
)
BINANCE_REST = "https://api.binance.com/api/v3/klines"

# ── Internal state ────────────────────────────────────────────────────────────
_closes     = {
    "eth": deque(maxlen=CANDLE_LIMIT),
    "sol": deque(maxlen=CANDLE_LIMIT),
}
_live_close = {
    "eth": None,
    "sol": None,
}
_lock = threading.Lock()   # protects _closes and _live_close from race conditions


# ── RSI calculation ───────────────────────────────────────────────────────────

def _compute_rsi(closes, period=RSI_PERIOD):
    """
    Wilder-smoothed RSI.
    closes : list of floats, oldest → newest.
    Returns: float 0–100.  50.0 if not enough data.
    """
    closes = list(closes)
    if len(closes) < period + 1:
        return 50.0

    deltas   = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains    = [d if d > 0 else 0.0 for d in deltas]
    losses   = [abs(d) if d < 0 else 0.0 for d in deltas]

    # Seed: simple average of first `period` moves
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Wilder smoothing for remaining moves
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    # Edge cases
    if avg_gain == 0 and avg_loss == 0:
        return 50.0   # flat market — truly neutral
    if avg_loss == 0:
        return 100.0  # only gains — fully overbought

    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def _refresh_rsi(asset):
    """
    Recompute RSI from history + current live close.
    Thread-safe via _lock.
    Stores result in rsi_data[asset].
    """
    with _lock:
        closes = list(_closes[asset])
        live   = _live_close[asset]

    # Append unconfirmed live close so RSI reflects the current candle in real time
    if live is not None:
        closes = closes + [live]

    val = _compute_rsi(closes)
    rsi_data[asset] = val
    return val


# ── REST prefetch — runs once on startup ─────────────────────────────────────

def _prefetch_candles():
    """
    Fetch the last CANDLE_LIMIT closed 15m candles from Binance REST API.
    Pre-fills _closes so RSI is ready immediately on startup.
    Falls back silently — WebSocket warmup takes over if REST fails.
    """
    for symbol, asset in SYMBOL_MAP.items():
        try:
            resp = _requests.get(
                BINANCE_REST,
                params={
                    "symbol":   symbol.upper(),   # ETHUSDT / SOLUSDT
                    "interval": "15m",
                    "limit":    CANDLE_LIMIT + 1, # +1 because we skip the last open candle
                },
                timeout=10,
            )
            resp.raise_for_status()
            candles = resp.json()

            # Binance kline format:
            # [open_time, open, high, low, CLOSE, volume, close_time, ...]
            # Last candle in the list is the currently open (unfinished) candle — skip it
            closed_candles = candles[:-1]

            if not closed_candles:
                log.warning("[RSI-WS] %s prefetch returned no candles", asset.upper())
                continue

            with _lock:
                _closes[asset].clear()
                for c in closed_candles:
                    _closes[asset].append(float(c[4]))  # index 4 = close price

            rsi_val = _refresh_rsi(asset)
            log.info(
                "[RSI-WS] %s prefetched %d candles — RSI(%d)=%.1f  ready immediately",
                asset.upper(), len(_closes[asset]), RSI_PERIOD, rsi_val,
            )

        except _requests.exceptions.RequestException as e:
            log.warning(
                "[RSI-WS] %s REST prefetch failed: %s — will warm up via WebSocket",
                asset.upper(), e,
            )
        except (ValueError, KeyError, IndexError) as e:
            log.warning(
                "[RSI-WS] %s prefetch parse error: %s — will warm up via WebSocket",
                asset.upper(), e,
            )


# ── WebSocket callbacks ───────────────────────────────────────────────────────

def _on_open(ws):
    log.info("[RSI-WS] Connected to Binance — streaming ETH+SOL 15m candles")


def _on_message(ws, message):
    try:
        outer = json.loads(message)

        # Binance combined stream format: {"stream": "...", "data": {...}}
        data = outer.get("data", outer)   # fallback to outer if no "data" key
        k    = data.get("k")
        if not k:
            return

        symbol    = k.get("s", "").lower()   # e.g. "ethusdt"
        asset     = SYMBOL_MAP.get(symbol)
        if not asset:
            return

        close     = float(k.get("c", 0))        # current close price
        is_closed = bool(k.get("x", False))      # True = candle fully confirmed

        with _lock:
            _live_close[asset] = close

        if is_closed:
            # Candle confirmed — commit to history
            with _lock:
                _closes[asset].append(close)
                _live_close[asset] = None        # reset live close after commit
            rsi_val = _refresh_rsi(asset)
            log.info(
                "[RSI-WS] %s candle closed | close=%.4f | history=%d | RSI(%d)=%.1f",
                asset.upper(), close,
                len(_closes[asset]),
                RSI_PERIOD, rsi_val,
            )
        else:
            # Live tick — update RSI without committing
            rsi_val = _refresh_rsi(asset)
            log.debug(
                "[RSI-WS] %s tick | close=%.4f | RSI(%d)=%.1f",
                asset.upper(), close, RSI_PERIOD, rsi_val,
            )

    except (json.JSONDecodeError, TypeError, ValueError) as e:
        log.error("[RSI-WS] message parse error: %s", e)
    except Exception as e:
        log.error("[RSI-WS] unexpected error in on_message: %s", e)


def _on_error(ws, error):
    log.error("[RSI-WS] WebSocket error: %s", error)


def _on_close(ws, code, msg):
    log.warning("[RSI-WS] connection closed (code=%s msg=%s) — reconnecting in 5s", code, msg)


# ── Reconnect loop ────────────────────────────────────────────────────────────

def _run_forever():
    """
    Reconnect loop — keeps WebSocket alive forever.
    Survives network drops, Binance disconnects, and exceptions.
    """
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(
                ping_interval=30,   # send ping every 30s to keep connection alive
                ping_timeout=10,    # wait 10s for pong before declaring dead
            )
        except Exception as e:
            log.error("[RSI-WS] thread crashed: %s — restarting in 5s", e)
        time.sleep(5)   # wait before reconnect attempt


# ── Public API ────────────────────────────────────────────────────────────────

def start_rsi_feed():
    """
    1. Pre-fetches last 20 candles from Binance REST — RSI ready immediately.
    2. Launches WebSocket in a daemon background thread — updates RSI every tick.
    Call once inside main() after build_client().
    Returns immediately.
    """
    log.info("[RSI-WS] Prefetching historical candles...")
    _prefetch_candles()   # REST call — fills _closes, RSI ready before WebSocket starts

    t = threading.Thread(target=_run_forever, daemon=True, name="binance-rsi-ws")
    t.start()
    log.info(
        "[RSI-WS] Feed started — ETH RSI(%d)=%.1f  SOL RSI(%d)=%.1f",
        RSI_PERIOD, rsi_data["eth"],
        RSI_PERIOD, rsi_data["sol"],
    )
    return t
