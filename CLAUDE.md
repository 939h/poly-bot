# CLAUDE.md: Strategy Execution Protocol - Pro Sniper v3

## 🎯 MISSION OBJECTIVE
Execute mean-reversion hedges on Polymarket 15m crypto markets using high-frequency SD (Standard Deviation) triggers and global correlation filters.

## 🛠 CORE STRATEGY PARAMETERS (HARD-CODED)
- **Trigger:** Entry only when price drop >= max(0.06, 2.5 × σ) over 60s lookback.
- **Correlation Filter:** STRICT REQUIREMENT. Do not enter any trade unless >= 2 assets (BTC, ETH, SOL, XRP) signal a drop within a 5-second window.
- **Take Profit:** Activate Trailing Stop at +0.06 profit.
- **Trailing Stop:** Exit immediately if price drops 0.01 from peak after Profit Trigger.
- **Hard Stop Loss:** Exit immediately if PnL hits -0.12 or time in trade exceeds 15 seconds.

## ⚡ EXECUTION COMMANDS
- **Mode:** AUTONOMOUS. If "Auto Mode" is toggled on, execute all BUY and EXIT signals without awaiting manual "Yes/No" confirmation.
- **Simulation vs. Live:** Default to Simulation Mode unless I explicitly say "Switch to Live Funds."
- **Error Handling:** If an API error occurs, RETRY ONCE, then pause the specific asset for 60 seconds. Do not halt the entire bot.

## 📊 REPORTING & MONITORING
- **15-Min Dashboard:** Automatically print the Summary Table (PnL, Win Rate, Avg PnL) to the chat every 15 minutes.
- **Emergency Kill-Switch:** If I type "STOP" or "EXIT ALL", immediately close all open positions at market price and halt all monitoring.
