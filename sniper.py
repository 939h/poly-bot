"""
Polymarket Dump Sniper — Built on Fresh_v7 Pattern
Strategy: 
1. Detect >$0.12 drop in 1 second.
2. Market Buy the 'glitch'.
3. Immediate Limit Sell at +$0.05 profit.
4. Exit if no bounce in 10 seconds.
"""

import os, time, logging
from datetime import datetime
from fresh_bot7 import ClobClient, PnLTracker, record_orderbook # Reusing your existing imports/classes

# --- Sniper Settings ---
DUMP_THRESHOLD = 0.12    # Drop needed to trigger ($0.12)
PROFIT_TARGET  = 0.05    # Target bounce profit ($0.05)
EXIT_TIMER     = 10      # Seconds to wait for bounce before cutting
POLL_SECS      = 1       # High-speed polling
ASSETS         = ["sol", "xrp", "matic"] # Mid-caps are better for 1s sniping

# Store last prices to detect the "Drop"
last_prices = {a: {"YES": 0.5, "NO": 0.5} for a in ASSETS}

def sniper_loop():
    pnl = PnLTracker() # Uses your existing GSheets/CSV logic
    log = logging.getLogger(__name__)

    while True:
        for asset in ASSETS:
            try:
                # 1. Fetch current prices (using your CLOB pattern)
                # Note: Replace with your actual price fetching logic from fresh_bot7
                yes_price, no_price = get_prices(asset) 
                
                # 2. Check for DUMP on either side
                for side, price in [("YES", yes_price), ("NO", no_price)]:
                    prev_p = last_prices[asset][side]
                    
                    if prev_p - price >= DUMP_THRESHOLD:
                        log.info(f"!!! DUMP DETECTED: {asset} {side} dropped {prev_p - price:.2f}")
                        
                        # 3. Execution (Reuse your market_buy logic)
                        fill = market_buy(asset, side, BUY_SHARES)
                        
                        if fill:
                            entry_time = time.time()
                            target_p = fill + PROFIT_TARGET
                            log.info(f"Targeting Bounce to: {target_p:.2f}")
                            
                            # 4. The "Sniper" Exit Logic
                            while (time.time() - entry_time) < EXIT_TIMER:
                                current_p = get_single_price(asset, side)
                                if current_p >= target_p:
                                    market_sell(asset, side, BUY_SHARES)
                                    log.info("WIN: Bounce hit target!")
                                    break
                                time.sleep(0.5)
                            else:
                                # 5. Emergency Cut Loss
                                market_sell(asset, side, BUY_SHARES)
                                log.info("LOSS: No bounce, exiting.")

                # Update last prices for next tick
                last_prices[asset]["YES"] = yes_price
                last_prices[asset]["NO"] = no_price
                
                # Silent log (Your existing pattern)
                record_orderbook(asset, yes_price, no_price)

            except Exception as e:
                log.error(f"Error in sniper loop: {e}")

        time.sleep(POLL_SECS)

if __name__ == "__main__":
    sniper_loop()
