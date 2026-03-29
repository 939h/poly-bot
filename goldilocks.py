"""
Goldilocks Pro Sniper v3 (Dual-Side) — Live Execution
Markets: BTC, ETH, SOL, XRP — 15-min up/down windows

Strategy:
  - Monitors YES and NO sides every 0.5s
  - Detects sudden price dumps using rolling volatility (SD over 60s)
  - Buys whichever side dumped (dual-side capable)
  - Exits via trailing stop: profit >= 6c AND dip from peak >= 1c
  - Hard stops: time held > 15s OR loss < -12c
  - Global pause if 3+ assets dump simultaneously (crash guard)

Speed optimisations:
  - Token IDs pre-fetched every 30s (zero HTTP latency on BUY signal)
  - update_balance_allowance removed (saves ~300ms per order leg)
  - asyncio.to_thread() keeps CLOB calls non-blocking

Requirements:
    pip install py-clob-client python-dotenv requests aiohttp numpy
"""

import asyncio
import aiohttp
import time
import json
import math
import os
import requests
import numpy as np
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType, ApiCreds
    from py_clob_client.order_builder.constants import BUY, SELL
    from py_clob_client.constants import POLYGON
except ImportError:
    print("Run: pip install py-clob-client")
    exit(1)

# ── Optimized Settings (from Cross-Run Matrix) ───────────────────────────────
DRY_RUN          = True        # Set False for live trading
BUY_AMOUNT       = 2.0         # USDC per trade
FEE_BUFFER       = 0.98        # 2% buffer covers taker fee + rounding
ASSETS           = ["btc", "eth", "sol", "xrp"]
CHECK_INTERVAL   = 0.5
SD_LOOKBACK      = 60
BASE_THRESHOLD   = 0.06        # Optimized from 0.10 -> 0.06
PROFIT_TRIGGER   = 0.06        # Optimized from 0.05 -> 0.06
TRAILING_DROP    = 0.01        # Optimized from 0.015 -> 0.01
GLOBAL_PAUSE_DUR = 60
WINDOW_SECS      = 900

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API  = "https://clob.polymarket.com"

# ── State Management ──────────────────────────────────────────────────────────
price_histories  = {a: {'yes': deque(), 'no': deque()} for a in ASSETS}
active_trades    = {
    a: {
        "state":         "WATCHING",
        "side":          None,
        "entry_p":       0,
        "peak_p":        0,
        "time":          0,
        "token_id":      None,
        "actual_shares": 0.0,
    }
    for a in ASSETS
}
trigger_log      = deque()
crash_mode_until = 0
token_cache      = {}   # (asset, window_ts) -> (yes_token, no_token)
trade_history    = []   # [(pnl_usdc, reason), ...]

# ── Client Init ───────────────────────────────────────────────────────────────
def build_client():
    pk       = os.getenv("POLY_PRIVATE_KEY")
    api_key  = os.getenv("POLY_API_KEY")
    api_sec  = os.getenv("POLY_API_SECRET")
    api_pass = os.getenv("POLY_API_PASSPHRASE")
    funder   = os.getenv("POLY_FUNDER_ADDRESS", "")
    if not all([pk, api_key, api_sec, api_pass]):
        print("ERROR: Missing .env keys — need POLY_PRIVATE_KEY, POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE")
        exit(1)
    client = ClobClient(
        host=CLOB_API, chain_id=POLYGON, key=pk,
        creds=ApiCreds(api_key=api_key, api_secret=api_sec, api_passphrase=api_pass),
        signature_type=1, funder=funder or None,
    )
    print("✅ Connected to Polymarket CLOB")
    return client

# ── Token Helpers ─────────────────────────────────────────────────────────────
def _fetch_tokens_sync(asset, window_ts):
    """Fetch YES/NO clobTokenIds from Gamma API (sync, runs in thread)."""
    slug = f"{asset}-updown-15m-{window_ts}"
    try:
        r = requests.get(f"{GAMMA_API}/markets", params={"slug": slug}, timeout=5)
        data = r.json()
        markets = data if isinstance(data, list) else data.get("markets", [])
        if not markets:
            return None, None
        raw = markets[0].get("clobTokenIds") or markets[0].get("clob_token_ids", [])
        if isinstance(raw, str):
            raw = json.loads(raw)
        if not raw or len(raw) < 2:
            return None, None
        return raw[0].strip(), raw[1].strip()
    except Exception as e:
        print(f"  Token fetch error ({asset}): {e}")
        return None, None

async def prefetch_tokens_loop():
    """Background: pre-cache token IDs for current window every 30s."""
    while True:
        window_ts = int(time.time() // WINDOW_SECS) * WINDOW_SECS
        for asset in ASSETS:
            key = (asset, window_ts)
            if key not in token_cache:
                yes_t, no_t = await asyncio.to_thread(_fetch_tokens_sync, asset, window_ts)
                if yes_t and no_t:
                    token_cache[key] = (yes_t, no_t)
                    print(f"  📦 Tokens cached: {asset.upper()} window {window_ts}")
        await asyncio.sleep(30)

def get_cached_tokens(asset):
    """Return (yes_token, no_token) from cache — fallback fetch if missing."""
    window_ts = int(time.time() // WINDOW_SECS) * WINDOW_SECS
    key = (asset, window_ts)
    if key in token_cache:
        return token_cache[key]
    yes_t, no_t = _fetch_tokens_sync(asset, window_ts)
    if yes_t and no_t:
        token_cache[key] = (yes_t, no_t)
    return yes_t, no_t

# ── Order Execution ───────────────────────────────────────────────────────────
def _market_buy_sync(client, token_id, price, label):
    """Market BUY (sync). Returns (fill_price, actual_shares) or (None, None)."""
    actual = math.floor((BUY_AMOUNT / price) * FEE_BUFFER * 10) / 10
    if DRY_RUN:
        print(f"  [DRY RUN] 🟢 BUY {label} @ {price:.3f} | ${BUY_AMOUNT:.2f} -> {actual} shares")
        return price, actual
    try:
        order = client.create_market_order(MarketOrderArgs(token_id=token_id, amount=BUY_AMOUNT, side=BUY))
        resp  = client.post_order(order, OrderType.FOK)
        print(f"  ✅ BUY {label} @ {price:.3f} | ${BUY_AMOUNT:.2f} -> {actual} shares | {resp}")
        return price, actual
    except Exception as e:
        print(f"  ❌ BUY FAILED ({label}): {e}")
        return None, None

def _market_sell_sync(client, token_id, shares, price, label):
    """Market SELL (sync). Returns fill_price or None."""
    sell_amount = math.floor(shares * 10) / 10
    if sell_amount <= 0:
        sell_amount = shares
    if DRY_RUN:
        usdc = round(sell_amount * price, 4)
        print(f"  [DRY RUN] 🔴 SELL {sell_amount} {label} @ {price:.3f} | ~${usdc:.4f} USDC")
        return price
    try:
        order = client.create_market_order(MarketOrderArgs(token_id=token_id, amount=sell_amount, side=SELL))
        resp  = client.post_order(order, OrderType.FOK)
        print(f"  ✅ SELL {sell_amount} {label} @ {price:.3f} | {resp}")
        return price
    except Exception as e:
        print(f"  ❌ SELL FAILED ({label}): {e}")
        return None

async def market_buy(client, token_id, price, label):
    return await asyncio.to_thread(_market_buy_sync, client, token_id, price, label)

async def market_sell(client, token_id, shares, price, label):
    return await asyncio.to_thread(_market_sell_sync, client, token_id, shares, price, label)

# ── 15-Min Summary ───────────────────────────────────────────────────────────
async def print_15min_summary():
    while True:
        now  = datetime.now()
        wait = (15 - (now.minute % 15)) * 60 - now.second
        await asyncio.sleep(wait)

        count = len(trade_history)
        wins  = sum(1 for p, _ in trade_history if p > 0)
        total = sum(p for p, _ in trade_history)
        wr    = (wins / count * 100) if count else 0

        print("\n" + "=" * 45)
        print(f"  SUMMARY | {datetime.now().strftime('%H:%M')}")
        print("=" * 45)
        print(f"  Total Realized PnL : ${total:.4f} USDC")
        print(f"  Total Trades       : {count}")
        print(f"  Win Rate           : {wr:.1f}%")
        if count:
            print(f"  Avg PnL / Trade    : ${total/count:.4f} USDC")
        print("=" * 45 + "\n")

# ── Crash Guard ───────────────────────────────────────────────────────────────
async def check_global_correlation(asset_name):
    global crash_mode_until
    now = time.time()
    trigger_log.append((now, asset_name))
    while trigger_log and (now - trigger_log[0][0] > 5):
        trigger_log.popleft()
    unique_assets = len(set([t[1] for t in trigger_log]))
    if unique_assets >= 3:
        print(f"🚨 GLOBAL PAUSE ({unique_assets} assets dumping). Locked for 60s.")
        crash_mode_until = now + GLOBAL_PAUSE_DUR

# ── Asset Monitor ─────────────────────────────────────────────────────────────
async def monitor_asset(session, asset, client):
    state_info = active_trades[asset]
    try:
        timestamp = int(time.time() // 900) * 900
        slug = f"{asset}-updown-15m-{timestamp}"
        url = f"{GAMMA_API}/markets?slug={slug}"

        async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as resp:
            data = await resp.json()
            raw_prices = data[0]['outcomePrices']
            if isinstance(raw_prices, str):
                raw_prices = json.loads(raw_prices)
            current_prices = {
                'yes': float(raw_prices[0]),
                'no':  float(raw_prices[1])
            }
            now = time.time()

            for side in ['yes', 'no']:
                price_histories[asset][side].append((now, current_prices[side]))
                while price_histories[asset][side] and (now - price_histories[asset][side][0][0] > SD_LOOKBACK):
                    price_histories[asset][side].popleft()

            if state_info["state"] == "WATCHING":
                if now < crash_mode_until:
                    return

                for side in ['yes', 'no']:
                    prices = [p[1] for p in price_histories[asset][side]]
                    if len(prices) > 10:
                        volatility = np.std(prices)
                        dynamic_thresh = max(BASE_THRESHOLD, volatility * 2.5)
                        max_recent = max(prices)

                        if (max_recent - current_prices[side]) >= dynamic_thresh:
                            await check_global_correlation(asset)
                            if now >= crash_mode_until:
                                yes_t, no_t = get_cached_tokens(asset)
                                token_id = yes_t if side == 'yes' else no_t
                                if not token_id:
                                    print(f"  ⚠️ Token not available for {asset.upper()} — skipping")
                                    break

                                label = f"{asset.upper()}-{side.upper()}"
                                fill, actual_shares = await market_buy(client, token_id, current_prices[side], label)
                                if fill is not None:
                                    print(f"🎯 BUY {asset.upper()} {side.upper()} at {fill:.3f}")
                                    state_info.update({
                                        "state":         "HOLDING",
                                        "side":          side,
                                        "entry_p":       fill,
                                        "peak_p":        fill,
                                        "time":          now,
                                        "token_id":      token_id,
                                        "actual_shares": actual_shares,
                                    })
                                break

            elif state_info["state"] == "HOLDING":
                side = state_info["side"]
                price = current_prices[side]
                if price > state_info["peak_p"]:
                    state_info["peak_p"] = price

                profit        = price - state_info["entry_p"]
                dip_from_peak = state_info["peak_p"] - price
                time_held     = now - state_info["time"]

                label = f"{asset.upper()}-{side.upper()}"

                if profit >= PROFIT_TRIGGER and dip_from_peak >= TRAILING_DROP:
                    await market_sell(client, state_info["token_id"], state_info["actual_shares"], price, label)
                    pnl_usdc = round(state_info["actual_shares"] * price - BUY_AMOUNT, 4)
                    trade_history.append((pnl_usdc, "TAKE-PROFIT"))
                    print(f"💰 EXIT {asset.upper()} {side.upper()} at {price} | PnL: +{profit:.3f}")
                    state_info.update({"state": "WATCHING", "side": None, "entry_p": 0, "peak_p": 0, "time": 0, "token_id": None, "actual_shares": 0.0})
                elif time_held > 15 or profit < -0.12:  # Tightened Stop Loss slightly
                    await market_sell(client, state_info["token_id"], state_info["actual_shares"], price, label)
                    pnl_usdc = round(state_info["actual_shares"] * price - BUY_AMOUNT, 4)
                    trade_history.append((pnl_usdc, "STOP"))
                    print(f"⚠️ EXIT {asset.upper()} {side.upper()} (SAFE) at {price} | PnL: {profit:.3f}")
                    state_info.update({"state": "WATCHING", "side": None, "entry_p": 0, "peak_p": 0, "time": 0, "token_id": None, "actual_shares": 0.0})

    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"  ⚠️ [{asset.upper()}] monitor error: {e}")

# ── Poll Loop ─────────────────────────────────────────────────────────────────
async def _run_loop(session, client):
    """Polls all assets every CHECK_INTERVAL seconds."""
    iterations = 0
    while True:
        start_time = time.time()
        await asyncio.gather(*[monitor_asset(session, a, client) for a in ASSETS])
        iterations += 1
        if iterations % 120 == 0:  # every 60s (120 x 0.5s)
            status = []
            for a in ASSETS:
                s = active_trades[a]
                yes_len = len(price_histories[a]['yes'])
                no_len  = len(price_histories[a]['no'])
                if s["state"] == "HOLDING":
                    status.append(f"{a.upper()}:HOLDING-{s['side'].upper()}")
                else:
                    status.append(f"{a.upper()}:WATCH(y={yes_len},n={no_len})")
            print(f"  👁 {datetime.now().strftime('%H:%M:%S')} | {' | '.join(status)}")
        await asyncio.sleep(max(0, CHECK_INTERVAL - (time.time() - start_time)))

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    client = build_client()
    mode   = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"🚀 Pro Sniper v3 Optimized | {mode} | Ready for Deployment")
    print(f"   Buy=${BUY_AMOUNT} | Threshold={BASE_THRESHOLD} | Profit={PROFIT_TRIGGER} | Trail={TRAILING_DROP}")
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            print_15min_summary(),       # 15-min PnL summary
            prefetch_tokens_loop(),      # background token pre-caching
            _run_loop(session, client),  # main polling loop
        )

if __name__ == "__main__":
    asyncio.run(main())
