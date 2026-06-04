"""
kraken_ws.py
============
Kraken market-data adapter for CVD and RVOL only.

This module intentionally mirrors the CVD/RVOL snapshot functions exported by
binance_ws.py so momentum_v3.py can keep using Binance for candle_open,
live_close, EMA, and gap logic while sourcing CVD + RVOL from Kraken.

Exports:
    get_cvd_snapshot(asset) — thread-safe (session, window, slope) lookup
    get_volume_snapshot(asset, period=20, rvol_min=1.5) — Kraken 15m RVOL
    start_kraken_metrics_feed() — prefetch OHLC and launch Kraken WebSocket
"""

import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests as _requests
import websocket

log = logging.getLogger(__name__)

ASSETS = ("eth", "sol", "btc", "xrp")
WS_URL = "wss://ws.kraken.com/v2"
KRAKEN_OHLC_REST = "https://api.kraken.com/0/public/OHLC"
OHLC_INTERVAL_MINUTES = 15
CVD_SLOPE_WINDOW_SECS = 20
CANDLE_HISTORY_LIMIT = 120

WS_SYMBOL_TO_ASSET = {
    "ETH/USD": "eth",
    "SOL/USD": "sol",
    "BTC/USD": "btc",
    "XRP/USD": "xrp",
}
ASSET_TO_WS_SYMBOL = {asset: symbol for symbol, asset in WS_SYMBOL_TO_ASSET.items()}
REST_PAIR_BY_ASSET = {
    "eth": "ETHUSD",
    "sol": "SOLUSD",
    "btc": "XBTUSD",
    "xrp": "XRPUSD",
}

cvd_value = {asset: 0.0 for asset in ASSETS}
cvd_value_window = {asset: 0.0 for asset in ASSETS}
cvd_slope = {asset: 0.0 for asset in ASSETS}
candle_history = {asset: deque(maxlen=CANDLE_HISTORY_LIMIT) for asset in ASSETS}

_lock = threading.Lock()
_cvd_points = {asset: deque(maxlen=300) for asset in ASSETS}
_cvd_window_start = {asset: None for asset in ASSETS}
_started = False


def _parse_rfc3339_ms(value):
    if not value:
        return int(time.time() * 1000)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, tail = text.split(".", 1)
        if "+" in tail:
            frac, zone = tail.split("+", 1)
            text = f"{head}.{frac[:6]}+{zone}"
        elif "-" in tail:
            frac, zone = tail.split("-", 1)
            text = f"{head}.{frac[:6]}-{zone}"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return int(time.time() * 1000)


def _cvd_interval_start(ts_secs):
    interval = OHLC_INTERVAL_MINUTES * 60
    return int(ts_secs // interval) * interval


def _update_cvd(asset, qty, side, ts_secs=None):
    ts_secs = float(ts_secs if ts_secs is not None else time.time())
    window_start = _cvd_interval_start(ts_secs)
    if _cvd_window_start.get(asset) != window_start:
        _cvd_window_start[asset] = window_start
        cvd_value_window[asset] = 0.0

    # Kraken WebSocket v2 trade side is the taker side.
    delta = float(qty) if str(side).lower() == "buy" else -float(qty)
    cvd_value[asset] = float(cvd_value.get(asset, 0.0)) + delta
    cvd_value_window[asset] = float(cvd_value_window.get(asset, 0.0)) + delta

    pts = _cvd_points[asset]
    pts.append((ts_secs, cvd_value[asset]))
    while pts and (ts_secs - pts[0][0]) > CVD_SLOPE_WINDOW_SECS:
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


def _normalise_ohlc_row(row, closed=False):
    close = float(row.get("close", 0.0) or 0.0)
    volume = float(row.get("volume", 0.0) or 0.0)
    return {
        "ts": _parse_rfc3339_ms(row.get("interval_begin") or row.get("timestamp")),
        "open": float(row.get("open", 0.0) or 0.0),
        "high": float(row.get("high", 0.0) or 0.0),
        "low": float(row.get("low", 0.0) or 0.0),
        "close": close,
        "volume": volume,
        "quote_volume": volume * close,
        "trades": int(float(row.get("trades", 0) or 0)),
        "closed": bool(closed),
    }


def _upsert_ohlc(asset, row):
    history = candle_history[asset]
    ts = int(row.get("ts", 0))
    if history and int(history[-1].get("ts", 0)) == ts:
        history[-1] = row
        return
    if history:
        history[-1]["closed"] = True
    history.append(row)


def get_volume_snapshot(asset, period=20, rvol_min=1.5):
    """Return Kraken RVOL using current 15m quote-volume vs previous N candles."""
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
            "source": "kraken",
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
        "source": "kraken",
    }


def _prefetch_ohlc():
    for asset, pair in REST_PAIR_BY_ASSET.items():
        try:
            resp = _requests.get(
                KRAKEN_OHLC_REST,
                params={"pair": pair, "interval": OHLC_INTERVAL_MINUTES},
                timeout=10,
            )
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("error"):
                raise ValueError(", ".join(payload["error"]))
            result = payload.get("result") or {}
            rows = None
            for key, value in result.items():
                if key != "last" and isinstance(value, list):
                    rows = value
                    break
            if not rows:
                raise ValueError("empty OHLC response")
            with _lock:
                candle_history[asset].clear()
                for idx, raw in enumerate(rows[-CANDLE_HISTORY_LIMIT:]):
                    ts, open_, high, low, close, _vwap, volume, trades = raw
                    close_f = float(close)
                    volume_f = float(volume)
                    candle_history[asset].append({
                        "ts": int(ts) * 1000,
                        "open": float(open_),
                        "high": float(high),
                        "low": float(low),
                        "close": close_f,
                        "volume": volume_f,
                        "quote_volume": volume_f * close_f,
                        "trades": int(trades),
                        "closed": idx < len(rows[-CANDLE_HISTORY_LIMIT:]) - 1,
                    })
            log.info("[KRAKEN] %s OHLC prefetch loaded %d candles", asset.upper(), len(rows))
        except Exception as e:
            log.warning("[KRAKEN] %s OHLC prefetch failed: %s", asset.upper(), e)


def _handle_trade(data):
    for trade in data:
        symbol = trade.get("symbol")
        asset = WS_SYMBOL_TO_ASSET.get(symbol)
        if not asset:
            continue
        qty = float(trade.get("qty", 0.0) or 0.0)
        side = trade.get("side")
        ts_ms = _parse_rfc3339_ms(trade.get("timestamp"))
        if qty <= 0 or side not in ("buy", "sell"):
            continue
        with _lock:
            _update_cvd(asset, qty, side, ts_ms / 1000)


def _handle_ohlc(data):
    with _lock:
        for item in data:
            asset = WS_SYMBOL_TO_ASSET.get(item.get("symbol"))
            if not asset:
                continue
            row = _normalise_ohlc_row(item, closed=False)
            _upsert_ohlc(asset, row)


def _on_open(ws):
    symbols = list(WS_SYMBOL_TO_ASSET.keys())
    ws.send(json.dumps({
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": symbols},
        "req_id": 1,
    }))
    ws.send(json.dumps({
        "method": "subscribe",
        "params": {
            "channel": "ohlc",
            "symbol": symbols,
            "interval": OHLC_INTERVAL_MINUTES,
            "snapshot": True,
        },
        "req_id": 2,
    }))
    log.info("[KRAKEN] Connected — streaming CVD trades + %dm OHLC RVOL", OHLC_INTERVAL_MINUTES)


def _on_message(ws, message):
    try:
        payload = json.loads(message)
        channel = payload.get("channel")
        data = payload.get("data") or []
        if channel == "trade":
            _handle_trade(data)
        elif channel == "ohlc":
            _handle_ohlc(data)
        elif payload.get("method") == "subscribe" and not (payload.get("success") or payload.get("result", {}).get("success", True)):
            log.warning("[KRAKEN] subscribe failed: %s", payload)
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        log.error("[KRAKEN] message parse error: %s", e)
    except Exception as e:
        log.error("[KRAKEN] unexpected error in on_message: %s", e)


def _on_error(ws, error):
    log.error("[KRAKEN] WebSocket error: %s", error)


def _on_close(ws, code, msg):
    log.warning("[KRAKEN] connection closed (code=%s msg=%s) — reconnecting in 5s", code, msg)


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
            log.error("[KRAKEN] thread crashed: %s — restarting in 5s", e)
        time.sleep(5)


def start_kraken_metrics_feed():
    """Seed Kraken OHLC history and launch WebSocket daemon for CVD/RVOL."""
    global _started
    if _started:
        log.info("[KRAKEN] Metrics feed already started")
        return None
    _started = True
    log.info("[KRAKEN] Prefetching OHLC history for RVOL...")
    _prefetch_ohlc()
    t = threading.Thread(target=_run_forever, daemon=True, name="kraken-metrics-ws")
    t.start()
    log.info("[KRAKEN] Metrics feed started — CVD/RVOL source is Kraken")
    return t
