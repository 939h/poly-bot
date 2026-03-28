import asyncio
import aiohttp
import time
import numpy as np
from collections import deque

# ── Optimized Settings (from Cross-Run Matrix) ───────────────────────────────
ASSETS          = ["btc", "eth", "sol", "xrp"]
CHECK_INTERVAL  = 0.5       
SD_LOOKBACK     = 60         
BASE_THRESHOLD  = 0.06       # Optimized from 0.10 -> 0.06
PROFIT_TRIGGER  = 0.06       # Optimized from 0.05 -> 0.06
TRAILING_DROP   = 0.01       # Optimized from 0.015 -> 0.01
GLOBAL_PAUSE_DUR = 60        

# ── State Management ──────────────────────────────────────────────────────────
price_histories = {a: {'yes': deque(), 'no': deque()} for a in ASSETS}
active_trades = {a: {"state": "WATCHING", "side": None, "entry_p": 0, "peak_p": 0, "time": 0} for a in ASSETS}
trigger_log   = deque()      
crash_mode_until = 0         

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

async def monitor_asset(session, asset):
    state_info = active_trades[asset]
    try:
        timestamp = int(time.time() // 900) * 900
        slug = f"{asset}-updown-15m-{timestamp}"
        url = f"https://gamma-api.polymarket.com/markets?slug={slug}"
        
        async with session.get(url, timeout=2) as resp:
            data = await resp.json()
            current_prices = {
                'yes': float(data[0]['outcomePrices'][0]),
                'no': float(data[0]['outcomePrices'][1])
            }
            now = time.time()

            for side in ['yes', 'no']:
                price_histories[asset][side].append((now, current_prices[side]))
                while price_histories[asset][side] and (now - price_histories[asset][side][0][0] > SD_LOOKBACK):
                    price_histories[asset][side].popleft()

            if state_info["state"] == "WATCHING":
                if now < crash_mode_until: return

                for side in ['yes', 'no']:
                    prices = [p[1] for p in price_histories[asset][side]]
                    if len(prices) > 10:
                        volatility = np.std(prices)
                        dynamic_thresh = max(BASE_THRESHOLD, volatility * 2.5)
                        max_recent = max(prices)
                        
                        if (max_recent - current_prices[side]) >= dynamic_thresh:
                            await check_global_correlation(asset)
                            if now >= crash_mode_until:
                                print(f"🎯 BUY {asset.upper()} {side.upper()} at {current_prices[side]}")
                                state_info.update({"state": "HOLDING", "side": side, "entry_p": current_prices[side], "peak_p": current_prices[side], "time": now})
                                break 

            elif state_info["state"] == "HOLDING":
                side = state_info["side"]
                price = current_prices[side]
                if price > state_info["peak_p"]: state_info["peak_p"] = price

                profit = price - state_info["entry_p"]
                dip_from_peak = state_info["peak_p"] - price
                time_held = now - state_info["time"]

                if profit >= PROFIT_TRIGGER and dip_from_peak >= TRAILING_DROP:
                    print(f"💰 EXIT {asset.upper()} {side.upper()} at {price} | PnL: +{profit:.3f}")
                    state_info["state"] = "WATCHING"
                elif time_held > 15 or profit < -0.12: # Tightened Stop Loss slightly
                    print(f"⚠️ EXIT {asset.upper()} {side.upper()} (SAFE) at {price} | PnL: {profit:.3f}")
                    state_info["state"] = "WATCHING"

    except Exception: pass

async def main():
    print("🚀 Pro Sniper v3 Optimized | Ready for Deployment")
    async with aiohttp.ClientSession() as session:
        while True:
            start_time = time.time()
            await asyncio.gather(*[monitor_asset(session, a) for a in ASSETS])
            await asyncio.sleep(max(0, CHECK_INTERVAL - (time.time() - start_time)))

if __name__ == "__main__":
    asyncio.run(main())
