"""
kraken_ws.py
============
Kraken market-data adapter for price candles, EMA, CVD, and RVOL.

This module exposes the public objects/functions momentum_v3.py needs while
sourcing all market data from Kraken.

Exports:
    candle_open — dict {asset: float} current Kraken 15m candle open
    live_close — dict {asset: float|None} latest Kraken 15m close/update
    get_ema_snapshot(asset) — thread-safe EMA(8)/EMA(25) lookup
    get_candle_history(asset, limit=18) — thread-safe Kraken candle history
    get_cvd_snapshot(asset) — thread-safe (session, window, slope) lookup
    get_short_cvd_slope(asset, window_secs=15) — compatibility alias for mixed deployments
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
EMA_FAST_PERIOD = 8
EMA_SLOW_PERIOD = 25
CVD_SLOPE_WINDOW_SECS = 60
CANDLE_HISTORY_LIMIT = 1200

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

candle_open = {asset: 0.0 for asset in ASSETS}
live_close = {asset: None for asset in ASSETS}
ema_values = {asset: {"ema_fast": None, "ema_slow": None} for asset in ASSETS}
cvd_value = {asset: 0.0 for asset in ASSETS}
cvd_value_window = {asset: 0.0 for asset in ASSETS}
cvd_slope = {asset: 0.0 for asset in ASSETS}
candle_history = {asset: deque(maxlen=CANDLE_HISTORY_LIMIT) for asset in ASSETS}

_lock = threading.Lock()
_closed_closes = {asset: deque(maxlen=CANDLE_HISTORY_LIMIT) for asset in ASSETS}
_cvd_points = {asset: deque(maxlen=300) for asset in ASSETS}
_cvd_window_start = {asset: None for asset in ASSETS}
_started = False
_optimizer_cache = {}


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


def _ema_series(values, period):
    if not values:
        return []
    multiplier = 2 / (period + 1)
    ema = float(values[0])
    series = [ema]
    for value in values[1:]:
        ema = (float(value) - ema) * multiplier + ema
        series.append(ema)
    return series


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


def _refresh_price_state(asset):
    rows = list(candle_history.get(asset, []))
    if not rows:
        candle_open[asset] = 0.0
        live_close[asset] = None
        ema_values[asset] = {"ema_fast": None, "ema_slow": None}
        return

    current = rows[-1]
    candle_open[asset] = float(current.get("open", 0.0) or 0.0)
    live_close[asset] = float(current.get("close", 0.0) or 0.0)
    _closed_closes[asset].clear()
    _closed_closes[asset].extend(float(row.get("close", 0.0) or 0.0) for row in rows[:-1])
    _update_ema_values(asset, live_close[asset])


def get_ema_snapshot(asset):
    with _lock:
        pair = ema_values.get(asset) or {}
        return (pair.get("ema_fast"), pair.get("ema_slow"))


def get_candle_history(asset, limit=18):
    with _lock:
        rows = list(candle_history.get(asset, []))
        rows = rows[-int(limit):] if limit else rows
        return [dict(r) for r in rows]


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


def get_short_cvd_slope(asset, window_secs=15):
    """Compatibility alias for older momentum_v3 deployments.

    Current momentum_v3 snapshots CVD when its stop countdown starts and does
    not call this helper. Keeping the export prevents mixed-version deploys from
    crashing while both files roll out.
    """
    del window_secs
    return get_cvd_snapshot(asset)[2]


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
    else:
        if history:
            history[-1]["closed"] = True
        history.append(row)
    _refresh_price_state(asset)


def get_rvol_reversal_snapshot(asset, period=20, lookback=3, min_high_rvol=2, threshold=1.0):
    """Return an order-independent N-of-M RVOL setup and its fourth-window reversal rate."""
    period = max(1, int(period))
    lookback = max(1, int(lookback))
    min_high_rvol = max(1, min(lookback, int(min_high_rvol)))
    threshold = float(threshold)
    with _lock:
        rows = [dict(row) for row in candle_history.get(asset, []) if row.get("closed")]

    def quote_volume(row):
        return float(row.get("quote_volume", row.get("volume", 0.0)) or 0.0)

    def row_rvol(index):
        if index < period:
            return None
        average = sum(quote_volume(row) for row in rows[index - period:index]) / period
        return quote_volume(rows[index]) / average if average > 0 else 0.0

    def setup_before(index):
        if index < period + lookback:
            return None
        previous = rows[index - lookback:index]
        rvols = [row_rvol(i) for i in range(index - lookback, index)]
        net_move = float(previous[-1].get("close", 0.0) or 0.0) - float(previous[0].get("open", 0.0) or 0.0)
        if net_move == 0:
            return None
        side = "yes" if net_move < 0 else "no"
        return {
            "side": side,
            "high_rvol_count": sum(value is not None and value > threshold for value in rvols),
            "rvols": rvols,
            "net_move": net_move,
        }

    side_stats = {"yes": {"samples": 0, "wins": 0}, "no": {"samples": 0, "wins": 0}}
    for index in range(period + lookback, len(rows)):
        setup = setup_before(index)
        if not setup or setup["high_rvol_count"] < min_high_rvol:
            continue
        candle_move = float(rows[index].get("close", 0.0) or 0.0) - float(rows[index].get("open", 0.0) or 0.0)
        stats = side_stats[setup["side"]]
        stats["samples"] += 1
        if (setup["side"] == "yes" and candle_move > 0) or (setup["side"] == "no" and candle_move < 0):
            stats["wins"] += 1

    current = setup_before(len(rows))
    armed = bool(current and current["high_rvol_count"] >= min_high_rvol)
    current_stats = side_stats.get(current.get("side")) if current else None
    samples = current_stats["samples"] if current_stats else 0
    wins = current_stats["wins"] if current_stats else 0
    return {
        "armed": armed,
        "side": current.get("side") if current else None,
        "high_rvol_count": current.get("high_rvol_count", 0) if current else 0,
        "rvols": current.get("rvols", []) if current else [],
        "net_move": current.get("net_move", 0.0) if current else 0.0,
        "probability": wins / samples if samples else None,
        "samples": samples,
        "wins": wins,
        "period": period,
        "lookback": lookback,
        "min_high_rvol": min_high_rvol,
        "threshold": threshold,
    }


def get_golden_optimizer_snapshot(
    asset, period=20, current=None, thresholds=(0.9, 1.0, 1.1, 1.2),
    lookbacks=(3, 4, 5), gap_magnitudes=(1.5, 2.0, 3.0, 4.0, 5.0),
    validation_fraction=0.30, min_validation_samples=8,
):
    """Recommend golden OPPO settings in shadow mode using held-out Kraken candles."""
    period = max(1, int(period))
    with _lock:
        rows = [dict(row) for row in candle_history.get(asset, []) if row.get("closed")]
    current_key = tuple(sorted((current or {}).items()))
    cache_key = (asset, len(rows), rows[-1].get("ts") if rows else None, period, current_key, tuple(thresholds), tuple(lookbacks), tuple(gap_magnitudes), float(validation_fraction), int(min_validation_samples))
    cached = _optimizer_cache.get(cache_key)
    if cached is not None:
        return cached
    if len(rows) < period + max(lookbacks) + min_validation_samples:
        return {"mode": "shadow-recommend-only", "ready": False, "candles": len(rows)}

    def volume(row):
        return float(row.get("quote_volume", row.get("volume", 0.0)) or 0.0)

    rvols = []
    for index, row in enumerate(rows):
        if index < period:
            rvols.append(None)
            continue
        average = sum(volume(item) for item in rows[index - period:index]) / period
        rvols.append(volume(row) / average if average > 0 else 0.0)

    validation_start = max(period + max(lookbacks), int(len(rows) * (1 - float(validation_fraction))))

    def evaluate(config):
        sections = {"train": {"samples": 0, "wins": 0}, "validation": {"samples": 0, "wins": 0}}
        lookback = config["lookback"]
        for index in range(period + lookback, len(rows)):
            previous = rows[index - lookback:index]
            net_move = float(previous[-1].get("close", 0.0)) - float(previous[0].get("open", 0.0))
            if net_move == 0:
                continue
            high_count = sum(rvols[pos] is not None and rvols[pos] > config["threshold"] for pos in range(index - lookback, index))
            if high_count < config["min_high"]:
                continue
            result = rows[index]
            result_open = float(result.get("open", 0.0) or 0.0)
            result_close = float(result.get("close", 0.0) or 0.0)
            if result_open <= 0:
                continue
            # Historical proxy for the live gap guard: use the fourth candle's close gap.
            if abs(result_close - result_open) >= result_open * 0.001 * config["gap_magnitude"]:
                continue
            bucket = sections["validation" if index >= validation_start else "train"]
            bucket["samples"] += 1
            if (net_move < 0 and result_close > result_open) or (net_move > 0 and result_close < result_open):
                bucket["wins"] += 1
        for bucket in sections.values():
            bucket["rate"] = bucket["wins"] / bucket["samples"] if bucket["samples"] else None
        val = sections["validation"]
        # Conservative score rewards held-out wins while penalizing sparse candidates.
        sections["score"] = (val["wins"] - (val["samples"] - val["wins"])) if val["samples"] >= min_validation_samples else None
        return sections

    candidates = []
    for lookback in lookbacks:
        for min_high in range(2, int(lookback) + 1):
            for threshold in thresholds:
                for gap_magnitude in gap_magnitudes:
                    config = {"lookback": int(lookback), "min_high": min_high, "threshold": float(threshold), "gap_magnitude": float(gap_magnitude)}
                    metrics = evaluate(config)
                    if metrics["score"] is not None:
                        candidates.append({"config": config, **metrics})
    candidates.sort(key=lambda item: (item["score"], item["validation"]["rate"] or 0, item["validation"]["samples"]), reverse=True)
    current_result = evaluate(current) if current else None
    result = {
        "mode": "shadow-recommend-only",
        "ready": bool(candidates),
        "candles": len(rows),
        "validation_start_index": validation_start,
        "candidate_count": len(candidates),
        "recommendation": candidates[0] if candidates else None,
        "current": {"config": current, **current_result} if current and current_result else None,
        "note": "Kraken fourth-candle direction/gap proxy; does not auto-apply or replay Polymarket token prices",
    }
    if len(_optimizer_cache) > 100:
        _optimizer_cache.clear()
    _optimizer_cache[cache_key] = result
    return result


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
                selected_rows = rows[-CANDLE_HISTORY_LIMIT:]
                for idx, raw in enumerate(selected_rows):
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
                        "closed": idx < len(selected_rows) - 1,
                    })
                _refresh_price_state(asset)
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
    log.info("[KRAKEN] Connected — streaming trades, prices, EMA, and %dm OHLC RVOL", OHLC_INTERVAL_MINUTES)


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
    """Seed Kraken OHLC history and launch WebSocket daemon for all market data."""
    global _started
    if _started:
        log.info("[KRAKEN] Metrics feed already started")
        return None
    _started = True
    log.info("[KRAKEN] Prefetching OHLC history for RVOL...")
    _prefetch_ohlc()
    t = threading.Thread(target=_run_forever, daemon=True, name="kraken-metrics-ws")
    t.start()
    log.info(
        "[KRAKEN] Feed started — opens: ETH=%.4f SOL=%.4f BTC=%.4f XRP=%.4f",
        candle_open["eth"], candle_open["sol"], candle_open["btc"], candle_open["xrp"],
    )
    return t
