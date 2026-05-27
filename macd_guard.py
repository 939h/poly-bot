"""Standalone MACD guard helper (does not modify binance_ws.py)."""

from collections import deque
import requests

ASSET_TO_SYMBOL = {
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "btc": "BTCUSDT",
    "xrp": "XRPUSDT",
}
BINANCE_REST = "https://fapi.binance.com/fapi/v1/klines"
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_LIMIT = 120


def _ema(values, period):
    if not values:
        return []
    k = 2 / (period + 1)
    out = [float(values[0])]
    for v in values[1:]:
        out.append((float(v) - out[-1]) * k + out[-1])
    return out


def _macd_hist_triplet(closes):
    if len(closes) < MACD_SLOW + MACD_SIGNAL + 3:
        return None
    fast = _ema(closes, MACD_FAST)
    slow = _ema(closes, MACD_SLOW)
    dif = [f - s for f, s in zip(fast, slow)]
    dea = _ema(dif, MACD_SIGNAL)
    hist = [d - e for d, e in zip(dif, dea)]
    if len(hist) < 3:
        return None
    return (hist[-3], hist[-2], hist[-1])


class MacdGuard:
    def __init__(self, assets):
        self.assets = list(assets)
        self.closed = {a: deque(maxlen=MACD_LIMIT) for a in self.assets}
        self.last_candle_open = {a: None for a in self.assets}
        self.last_live = {a: None for a in self.assets}

    def warmup(self):
        for a in self.assets:
            sym = ASSET_TO_SYMBOL.get(a)
            if not sym:
                continue
            r = requests.get(BINANCE_REST, params={"symbol": sym, "interval": "15m", "limit": MACD_LIMIT}, timeout=10)
            r.raise_for_status()
            rows = r.json()
            # keep only closed candles (exclude last open candle)
            for c in rows[:-1]:
                self.closed[a].append(float(c[4]))
            if rows:
                self.last_candle_open[a] = float(rows[-1][1])
                self.last_live[a] = float(rows[-1][4])

    def update_live(self, asset, candle_open, live_close):
        if asset not in self.closed:
            return
        prev_open = self.last_candle_open.get(asset)
        if prev_open is not None and candle_open and prev_open != candle_open:
            # new candle started; previous live close becomes a closed bar
            if self.last_live.get(asset) is not None:
                self.closed[asset].append(float(self.last_live[asset]))
        if candle_open:
            self.last_candle_open[asset] = float(candle_open)
        if live_close is not None:
            self.last_live[asset] = float(live_close)

    def gate_ok(self, asset, side):
        tri = _macd_hist_triplet(list(self.closed.get(asset, [])))
        if tri is None:
            return False, "macd-insufficient-bars"
        b1, b2, b3 = [float(x) for x in tri]

        yes_a = (b1 > 0 and b2 > b1 and b3 > b2)  # 3 green hollow
        yes_b = (b1 < 0 and b2 > 0 and b3 > b2)    # red->green->green
        no_a = (b1 < 0 and b2 < b1 and b3 < b2)    # 3 solid red
        no_b = (b1 > 0 and b2 < 0 and b3 < b2)     # green->red->red

        if side == "yes":
            ok = yes_a or yes_b
            return ok, f"bars={b1:.6f},{b2:.6f},{b3:.6f}"
        ok = no_a or no_b
        return ok, f"bars={b1:.6f},{b2:.6f},{b3:.6f}"
