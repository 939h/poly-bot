import asyncio
import aiohttp
import time
import numpy as np
from datetime import datetime
from collections import deque

# ── 1. STATS TRACKER (Top of Code) ──────────────────────────────────────────
trade_history = [] 

# ── Settings ──────────────────────────────────────────────────────────────────
ASSETS          = ["btc", "eth", "sol", "xrp"]
CHECK_INTERVAL  = 0.5       
SD_LOOKBACK     = 60         
BASE_THRESHOLD  = 0.06       
PROFIT_TRIGGER  = 0.06       
TRAILING_DROP   = 0.01       
GLOBAL_PAUSE_DUR = 60        

# ── State Management ──────────────────────────────────────────────────────────
price_histories = {a: {'yes': deque(), 'no': deque()} for a in ASSETS}
active_trades = {a: {"state": "WATCHING", "side": None, "entry_p": 0, "peak_p": 0, "time": 0} for a in ASSETS}
trigger_log   = deque()      
crash_mode_until = 0         

# ── 2. NEW SUMMARY FUNCTION (Middle of Code) ────────────────────────────────
async def print_15min_summary():
    """Calculates and prints the dashboard to CMD every 15 mins."""
    while True:
        now = datetime.now()
        # Wait until the next :00, :15, :30, or :45 mark
        wait_seconds = (15 - (now.minute % 15)) * 60 - now.second
        await asyncio.sleep(wait_seconds)
        
        total_pnl = sum(trade_history)
        count = len(trade_history)
        wins = len([p for p in trade_history if p > 0])
        wr = (wins / count * 100) if count > 0 else 0
        
        print("\n" + "="*45)
        print(f"🕒 PERIODIC SUMMARY | {datetime.now().strftime('%H:%M')}")
        print("="*45)
        print(f"💰 Total Realized PnL : ${total_pnl:.4f}")
        print(f"📊 Total Trades       : {count}")
        print(f"🎯 Win Rate           : {wr:.1f}%")
        print(f"📈 Avg PnL / Trade    : ${total_pnl/count:.4f}" if count > 0 else "📈 Avg PnL / Trade  : $0")
        print("="*45 + "\n")

async def check_global_correlation(asset_name):
    global crash_mode_until
    now = time.time()
    trigger_log.append((now, asset_name))
    while trigger_log and (now - trigger_log[0][0] > 5):
        trigger_log.popleft()
    if len(set([t[1] for t in trigger_log])) >= 3:
        crash_mode_until = now + GLOBAL_PAUSE_DUR

async def monitor_asset(session, asset):
    state_info = active_trades[asset]
    try:
        timestamp = int(time.time() // 900) * 900
        url = f"https://gamma-api.polymarket.com/markets?slug={asset}-updown-15m-{timestamp}"
        
        async with session.get(url, timeout=2) as resp:
            data = await resp.json()
            current_prices = {'yes': float(data[0]['outcomePrices'][0]), 'no': float(data[0]['outcomePrices'][1])}
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
                        dynamic_thresh = max(BASE_THRESHOLD, np.std(prices) * 2.5)
                        if (max(prices) - current_prices[side]) >= dynamic_thresh:
                            await check_global_correlation(asset)
                            if now >= crash_mode_until:
                                print(f"🎯 BUY {asset.upper()} {side.upper()} @ {current_prices[side]}")
                                state_info.update({"state": "HOLDING", "side": side, "entry_p": current_prices[side], "peak_p": current_prices[side], "time": now})
                                break 

            elif state_info["state"] == "HOLDING":
                side = state_info["side"]
                price = current_prices[side]
                if price > state_info["peak_p"]: state_info["peak_p"] = price
                profit = price - state_info["entry_p"]
                
                # EXIT LOGIC
                if (profit >= PROFIT_TRIGGER and (state_info["peak_p"] - price) >= TRAILING_DROP) or (now - state_info["time"] > 15) or (profit < -0.12):
                    pnl = 2.0 * ((price / state_info["entry_p"]) - 1)
                    trade_history.append(pnl) # <── RECORD PNL HERE
                    print(f"💰 EXIT {asset.upper()} {side.upper()} | PnL: {pnl:+.4f}")
                    state_info["state"] = "WATCHING"
    except: pass

# ── 3. RUN IN PARALLEL (Bottom of Code) ──────────────────────────────────────
async def main():
    print("🚀 Pro Sniper v3 Active | Summary every 15m")
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            print_15min_summary(), # Start the timer
            *[monitor_asset(session, a) for a in ASSETS]
        )

if __name__ == "__main__":
    asyncio.run(main())
