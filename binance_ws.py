"""
binance_ws.py
=============
Runs as a background thread alongside fresh_bot23.py.
Connects to Binance WebSocket — ETHUSDT + SOLUSDT + BTCUSDT + XRPUSDT 15m candles.
Tracks candle open price, live close price, and 15m MACD histogram per asset.

Exports:
    candle_open   — dict {asset: float}  open price of current 15m candle
    live_close    — dict {asset: float|None}  latest tick close price
    macd_histogram — dict {asset: tuple(prior_hist, prev_hist, curr_hist)|None}
    get_macd_histogram(asset) — thread-safe MACD histogram lookup
    start_rsi_feed() — call once on startup

Usage in fresh_bot23.py:
    from binance_ws import candle_open, live_close, start_rsi_feed
    start_rsi_feed()
"""

import json
import threading
import time
import logging
from collections import deque

import requests as _requests

try:
    import websocket
except ImportError:
    raise ImportError("Run: pip install websocket-client")

log = logging.getLogger(__name__)

# ── Public shared dicts — fresh_bot23 reads these every poll ──────────────────
# candle_open: open price of the current 15m candle (set on first tick of new candle)
# live_close:  latest tick close price (None when candle just closed / not yet received)
# macd_histogram: latest Binance 15m MACD histogram pair (prev, current).
candle_open = {
    "eth": 0.0,
    "sol": 0.0,
    "btc": 0.0,
    "xrp": 0.0,
}
live_close = {
    "eth": None,
    "sol": None,
    "btc": None,
    "xrp": None,
}
macd_histogram = {
    "eth": None,
    "sol": None,
    "btc": None,
    "xrp": None,
}

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL_MAP = {
    "ethusdt": "eth",
    "solusdt": "sol",
    "btcusdt": "btc",
    "xrpusdt": "xrp",
}
WS_URL = (
    "wss://stream.binance.com:9443/stream"
    "?streams=ethusdt@kline_15m/solusdt@kline_15m/btcusdt@kline_15m/xrpusdt@kline_15m"
)
BINANCE_REST = "https://api.binance.com/api/v3/klines"
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_REST_LIMIT = 100

# ── Internal state ────────────────────────────────────────────────────────────
# _prev_live_close tracks whether the previous state was None (candle boundary)
# so we know when a new candle has opened and need to capture candle_open.
_prev_live_close = {
    "eth": None,
    "sol": None,
    "btc": None,
    "xrp": None,
}
_lock = threading.Lock()
_closed_closes = {asset: deque(maxlen=MACD_REST_LIMIT) for asset in SYMBOL_MAP.values()}


def _ema_series(values, period):
    """Return EMA values seeded from the first value in values."""
    if not values:
        return []
    multiplier = 2 / (period + 1)
    ema = float(values[0])
    series = [ema]
    for value in values[1:]:
        ema = (float(value) - ema) * multiplier + ema
        series.append(ema)
    return series


def _macd_hist_pair(closes):
    """Calculate the previous/current MACD histogram values from closes."""
    if len(closes) < MACD_SLOW + MACD_SIGNAL + 1:
        return None

    fast = _ema_series(closes, MACD_FAST)
    slow = _ema_series(closes, MACD_SLOW)
    dif = [f - s for f, s in zip(fast, slow)]
    dea = _ema_series(dif, MACD_SIGNAL)
    hist = [d - signal for d, signal in zip(dif, dea)]
    if len(hist) < 2:
        return None
    return (hist[-2], hist[-1])


def _update_macd_histogram(asset, current_close=None):
    """Refresh public MACD histogram state using closed candles plus live close."""
    closes = list(_closed_closes[asset])
    if current_close is not None:
        closes.append(float(current_close))
    macd_histogram[asset] = _macd_hist_pair(closes)


def get_macd_histogram(asset):
    """Thread-safe lookup for (previous_histogram, current_histogram)."""
    with _lock:
        pair = macd_histogram.get(asset)
        return tuple(pair) if pair is not None else None


# ── REST prefetch — seed candle_open from last closed candle on startup ───────

def _prefetch_candle_opens():
    """
    Fetch the most recent closed 15m candle from Binance REST for each asset.
    Seeds candle_open so gap guard works immediately on startup without
    waiting for the next candle boundary.
    Falls back silently — WebSocket warmup takes over if REST fails.
    """
    for symbol, asset in SYMBOL_MAP.items():
        try:
            resp = _requests.get(
                BINANCE_REST,
                params={
                    "symbol":   symbol.upper(),
                    "interval": "15m",
                    "limit":    MACD_REST_LIMIT,
                },
                timeout=10,
            )
            resp.raise_for_status()
            candles = resp.json()

            # Last candle is the currently open (unfinished) one — use its open/close.
            # Earlier candles are closed and seed the MACD history.
            if len(candles) >= 1:
                current = candles[-1]
                closed = candles[:-1]
                open_price = float(current[1])   # index 1 = open price
                close_price = float(current[4])  # index 4 = current close (live)
                with _lock:
                    _closed_closes[asset].clear()
                    _closed_closes[asset].extend(float(c[4]) for c in closed)
                    candle_open[asset]    = open_price
                    live_close[asset]     = close_price
                    _prev_live_close[asset] = close_price
                    _update_macd_histogram(asset, close_price)
                hist_pair = macd_histogram.get(asset)
                if hist_pair is not None:
                    log.info(
                        "[WS] %s prefetch — candle_open=%.4f  live_close=%.4f  macd_hist=%.8f/%.8f",
                        asset.upper(), open_price, close_price, hist_pair[0], hist_pair[1],
                    )
                else:
                    log.info(
                        "[WS] %s prefetch — candle_open=%.4f  live_close=%.4f",
                        asset.upper(), open_price, close_price,
                    )
        except Exception as e:
            log.warning(
                "[WS] %s prefetch failed: %s — will warm up via WebSocket",
                asset.upper(), e,
            )


# ── WebSocket callbacks ───────────────────────────────────────────────────────

def _on_open(ws):
    log.info("[WS] Connected to Binance — streaming ETH+SOL+BTC+XRP 15m candles")


def _on_message(ws, message):
    try:
        outer = json.loads(message)
        data  = outer.get("data", outer)
        k     = data.get("k")
        if not k:
            return

        symbol    = k.get("s", "").lower()
        asset     = SYMBOL_MAP.get(symbol)
        if not asset:
            return

        close     = float(k.get("c", 0))
        open_     = float(k.get("o", 0))
        is_closed = bool(k.get("x", False))

        with _lock:
            prev = _prev_live_close[asset]

            if is_closed:
                # Candle fully confirmed — reset so next tick is treated as new candle open
                live_close[asset]       = None
                _prev_live_close[asset] = None
                candle_open[asset]      = 0.0   # cleared; next tick will set new open
                _closed_closes[asset].append(close)
                _update_macd_histogram(asset)
                log.info(
                    "[WS] %s candle closed | close=%.4f",
                    asset.upper(), close,
                )
            else:
                # Live tick
                if prev is None:
                    # First tick after a candle close (or startup) — this IS the candle open
                    candle_open[asset] = open_   # use kline open field (most accurate)
                    log.info(
                        "[WS] %s new candle open=%.4f",
                        asset.upper(), open_,
                    )
                live_close[asset]       = close
                _prev_live_close[asset] = close
                _update_macd_histogram(asset, close)

    except (json.JSONDecodeError, TypeError, ValueError) as e:
        log.error("[WS] message parse error: %s", e)
    except Exception as e:
        log.error("[WS] unexpected error in on_message: %s", e)


def _on_error(ws, error):
    log.error("[WS] WebSocket error: %s", error)


def _on_close(ws, code, msg):
    log.warning("[WS] connection closed (code=%s msg=%s) — reconnecting in 5s", code, msg)


# ── Reconnect loop ────────────────────────────────────────────────────────────

def _run_forever():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            log.error("[WS] thread crashed: %s — restarting in 5s", e)
        time.sleep(5)


# ── Public API ────────────────────────────────────────────────────────────────

def start_rsi_feed():
    """
    Seed candle_open/live_close from REST then launch WebSocket daemon thread.
    Named start_rsi_feed for drop-in compatibility with panic_rsi import.
    Returns immediately.
    """
    log.info("[WS] Prefetching candle opens from Binance REST...")
    _prefetch_candle_opens()

    t = threading.Thread(target=_run_forever, daemon=True, name="binance-ws")
    t.start()
    log.info(
        "[WS] Feed started — opens: ETH=%.4f SOL=%.4f BTC=%.4f XRP=%.4f",
        candle_open["eth"], candle_open["sol"],
        candle_open["btc"], candle_open["xrp"],
    )
    return t
