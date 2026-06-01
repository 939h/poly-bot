"""
binance_ws.py
=============
Runs as a background thread alongside fresh_bot23.py.
Connects to Binance WebSocket — ETHUSDT + SOLUSDT + BTCUSDT + XRPUSDT 15m candles.
Tracks candle open price, live close price, 15m MACD histogram, Binance CVD, and RVOL per asset.

Exports:
    candle_open   — dict {asset: float}  open price of current 15m candle
    live_close    — dict {asset: float|None}  latest tick close price
    macd_histogram — dict {asset: tuple(prior_hist, prev_hist, curr_hist)|None}
    cvd_value — dict {asset: float} cumulative volume delta (session)
    cvd_value_window — dict {asset: float} per-15m-window cumulative volume delta
    cvd_slope — dict {asset: float} short-window cvd slope
    get_macd_histogram(asset) — thread-safe MACD histogram lookup
    get_ema_snapshot(asset) — thread-safe EMA(8)/EMA(25) lookup
    get_cvd_snapshot(asset) — thread-safe (cvd, slope) lookup
    get_volume_snapshot(asset, period=20, rvol_min=1.5) — current quote volume vs average
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
cvd_value = {
    "eth": 0.0,
    "sol": 0.0,
    "btc": 0.0,
    "xrp": 0.0,
}
cvd_slope = {
    "eth": 0.0,
    "sol": 0.0,
    "btc": 0.0,
    "xrp": 0.0,
}
cvd_value_window = {
    "eth": 0.0,
    "sol": 0.0,
    "btc": 0.0,
    "xrp": 0.0,
}
ema_values = {
    "eth": {"ema_fast": None, "ema_slow": None},
    "sol": {"ema_fast": None, "ema_slow": None},
    "btc": {"ema_fast": None, "ema_slow": None},
    "xrp": {"ema_fast": None, "ema_slow": None},
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
    "/ethusdt@aggTrade/solusdt@aggTrade/btcusdt@aggTrade/xrpusdt@aggTrade"
)
BINANCE_REST = "https://api.binance.com/api/v3/klines"
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_REST_LIMIT = 100
EMA_FAST_PERIOD = 8
EMA_SLOW_PERIOD = 25
CVD_SLOPE_WINDOW_SECS = 20
CANDLE_HISTORY_LIMIT = 60 #cvd display 

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
_cvd_points = {asset: deque(maxlen=300) for asset in SYMBOL_MAP.values()}
candle_history = {asset: deque(maxlen=CANDLE_HISTORY_LIMIT) for asset in SYMBOL_MAP.values()}


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


def _update_ema_values(asset, current_close=None):
    closes = list(_closed_closes[asset])
    if current_close is not None:
        closes.append(float(current_close))
    if len(closes) < 2:
        ema_values[asset] = {"ema_fast": None, "ema_slow": None}
        return
    fast_series = _ema_series(closes, EMA_FAST_PERIOD)
    slow_series = _ema_series(closes, EMA_SLOW_PERIOD)
    ema_values[asset] = {
        "ema_fast": float(fast_series[-1]) if fast_series else None,
        "ema_slow": float(slow_series[-1]) if slow_series else None,
    }



def _update_cvd(asset, qty, buyer_is_maker):
    # buyer_is_maker=True means taker sell; False means taker buy
    delta = -float(qty) if buyer_is_maker else float(qty)
    cvd_value[asset] = float(cvd_value.get(asset, 0.0)) + delta
    cvd_value_window[asset] = float(cvd_value_window.get(asset, 0.0)) + delta
    now_ts = time.time()
    pts = _cvd_points[asset]
    pts.append((now_ts, cvd_value[asset]))
    while pts and (now_ts - pts[0][0]) > CVD_SLOPE_WINDOW_SECS:
        pts.popleft()
    if len(pts) >= 2:
        dt = max(pts[-1][0] - pts[0][0], 1e-6)
        cvd_slope[asset] = (pts[-1][1] - pts[0][1]) / dt
    else:
        cvd_slope[asset] = 0.0


def get_cvd_snapshot(asset):
    with _lock:
        return (
            float(cvd_value.get(asset, 0.0)),
            float(cvd_value_window.get(asset, 0.0)),
            float(cvd_slope.get(asset, 0.0)),
        )

def get_macd_histogram(asset):
    """Thread-safe lookup for (previous_histogram, current_histogram)."""
    with _lock:
        pair = macd_histogram.get(asset)
        return tuple(pair) if pair is not None else None


def get_ema_snapshot(asset):
    with _lock:
        pair = ema_values.get(asset) or {}
        return (pair.get("ema_fast"), pair.get("ema_slow"))


def get_volume_snapshot(asset, period=20, rvol_min=1.5):
    """Return current quote-volume RVOL against the previous N candles."""
    period = max(1, int(period))
    with _lock:
        rows = list(candle_history.get(asset, []))

    if len(rows) < period + 1:
        return {
            "current": None,
            "average": None,
            "rvol": None,
            "above_average": False,
            "confirmed": False,
            "period": period,
            "rvol_min": float(rvol_min),
            "ready": False,
        }

    current = rows[-1]
    previous = rows[-period - 1:-1]
    current_volume = float(current.get("quote_volume", current.get("volume", 0.0)) or 0.0)
    volumes = [float(r.get("quote_volume", r.get("volume", 0.0)) or 0.0) for r in previous]
    avg_volume = sum(volumes) / len(volumes) if volumes else 0.0
    rvol = current_volume / avg_volume if avg_volume > 0 else 0.0
    return {
        "current": current_volume,
        "average": avg_volume,
        "rvol": rvol,
        "above_average": current_volume > avg_volume if avg_volume > 0 else False,
        "confirmed": rvol >= float(rvol_min) if avg_volume > 0 else False,
        "period": period,
        "rvol_min": float(rvol_min),
        "ready": True,
    }


def get_candle_history(asset, limit=18):
    with _lock:
        rows = list(candle_history.get(asset, []))
        rows = rows[-int(limit):] if limit else rows
        return [dict(r) for r in rows]


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
                    candle_history[asset].clear()
                    for c in candles:
                        candle_history[asset].append({
                            "ts": int(c[0]),
                            "open": float(c[1]),
                            "high": float(c[2]),
                            "low": float(c[3]),
                            "close": float(c[4]),
                            "volume": float(c[5]),
                            "quote_volume": float(c[7]),
                            "trades": int(c[8]),
                            "taker_buy_volume": float(c[9]),
                            "taker_buy_quote_volume": float(c[10]),
                            "closed": bool(int(c[6]) <= int(time.time() * 1000)),
                        })
                    candle_open[asset]    = open_price
                    live_close[asset]     = close_price
                    _prev_live_close[asset] = close_price
                    _update_macd_histogram(asset, close_price)
                    _update_ema_values(asset, close_price)
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
        event_type = data.get("e")

        if event_type == "aggTrade":
            symbol = data.get("s", "").lower()
            asset = SYMBOL_MAP.get(symbol)
            if not asset:
                return
            qty = float(data.get("q", 0.0))
            buyer_is_maker = bool(data.get("m", False))
            with _lock:
                _update_cvd(asset, qty, buyer_is_maker)
            return

        k = data.get("k")
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
                _update_ema_values(asset)
                cvd_value_window[asset] = 0.0
                k_ts = int(k.get("t", 0))
                row = {
                    "ts": k_ts,
                    "open": open_,
                    "high": float(k.get("h", 0)),
                    "low": float(k.get("l", 0)),
                    "close": close,
                    "volume": float(k.get("v", 0)),
                    "quote_volume": float(k.get("q", 0)),
                    "trades": int(k.get("n", 0)),
                    "taker_buy_volume": float(k.get("V", 0)),
                    "taker_buy_quote_volume": float(k.get("Q", 0)),
                    "closed": True,
                }
                if candle_history[asset] and int(candle_history[asset][-1].get("ts", 0)) == k_ts:
                    candle_history[asset][-1] = row
                else:
                    candle_history[asset].append(row)
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
                _update_ema_values(asset, close)
                k_ts = int(k.get("t", 0))
                row = {
                    "ts": k_ts,
                    "open": open_,
                    "high": float(k.get("h", 0)),
                    "low": float(k.get("l", 0)),
                    "close": close,
                    "volume": float(k.get("v", 0)),
                    "quote_volume": float(k.get("q", 0)),
                    "trades": int(k.get("n", 0)),
                    "taker_buy_volume": float(k.get("V", 0)),
                    "taker_buy_quote_volume": float(k.get("Q", 0)),
                    "closed": False,
                }
                if candle_history[asset] and int(candle_history[asset][-1].get("ts", 0)) == k_ts:
                    candle_history[asset][-1] = row
                else:
                    candle_history[asset].append(row)

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
