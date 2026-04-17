import os
import time
import math
import logging
import requests
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, Side
from py_clob_client.constants import POLYGON

# --- STRATEGY CONFIG ---
ORDER_PRICE = 0.001      # $0.005 limit buy
MIN_SHARES = 5           # Polymarket Minimum = 5 shares
BUY_AMOUNT_SHARES = 5  # Your target entry size in shares
CANCEL_HOUR_UTC = 14     # 14:00 UTC safety cutoff
PRICE_SAFETY_BUFFER = 0.003 

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("BracketBot")

class ShareBracketBot:
    def __init__(self):
        self.client = ClobClient(
            host="https://clob.polymarket.com",
            key=os.getenv("POLY_PRIVATE_KEY"),
            chain_id=POLYGON,
            creds={
                "api_key": os.getenv("POLY_API_KEY"),
                "api_secret": os.getenv("POLY_API_SECRET"),
                "api_passphrase": os.getenv("POLY_API_PASSPHRASE"),
            }
        )

    def get_eth_spot(self):
        try:
            r = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDC")
            return float(r.json()['price'])
        except Exception as e:
            log.error(f"Price fetch failed: {e}")
            return None

    def find_token_id(self, slug, outcome_label):
        """Fetches the correct CLOB token ID for the market side."""
        try:
            resp = requests.get(f"https://gamma-api.polymarket.com/markets?slug={slug}")
            data = resp.json()
            if not data: return None
            
            import json
            tokens = json.loads(data[0]['clobTokenIds'])
            outcomes = data[0]['outcomes']
            
            for i, outcome in enumerate(outcomes):
                if outcome.upper() == outcome_label.upper():
                    return tokens[i]
            return None
        except Exception:
            return None

    def place_tiered_sells(self, token_id, filled_shares):
        """
        Executes your 2x, 10x, 50x strategy based on shares.
        Ensures each tranche is >= 5 shares.
        """
        # Tranches: (Percentage of total, Price Multiple)
        plan = [(0.50, 2), (0.25, 10), (0.25, 50)]
        
        for pct, mult in plan:
            tranche_shares = math.floor(filled_shares * pct)
            
            # Polymarket Guard: Don't place if below 5 shares
            if tranche_shares < MIN_SHARES:
                log.warning(f"Tranche too small ({tranche_shares} shares). Skipping.")
                continue

            sell_price = min(round(ORDER_PRICE * mult, 3), 0.99)
            
            try:
                self.client.create_and_post_order(OrderArgs(
                    price=sell_price, 
                    size=tranche_shares, 
                    side=Side.SELL, 
                    token_id=token_id
                ))
                log.info(f"SET EXIT: {tranche_shares} shares @ ${sell_price}")
            except Exception as e:
                log.error(f"Sell order failed: {e}")

    def run_strategy(self):
        eth = self.get_eth_spot()
        if not eth: return

        # Calculate Brackets
        upper_val = math.ceil(eth / 100) * 100
        lower_val = math.floor(eth / 100) * 100 - 100
        today = time.strftime("%Y-%m-%d", time.gmtime())

        # Logic for Upper (YES) and Lower (NO)
        targets = [
            (f"eth-above-{upper_val}-{today}", "YES", upper_val),
            (f"eth-above-{lower_val}-{today}", "NO", lower_val)
        ]

        for slug, outcome, bracket_price in targets:
            # 1. Proximity Check
            if abs(eth - bracket_price) / bracket_price < PRICE_SAFETY_BUFFER:
                continue

            # 2. Token Discovery
            tid = self.find_token_id(slug, outcome)
            if not tid: continue

            # 3. Order Check (Reconciliation)
            if any(o['token_id'] == tid for o in self.client.get_open_orders()):
                continue

            # 4. Place Limit Buy (using share count)
            try:
                self.client.create_and_post_order(OrderArgs(
                    price=ORDER_PRICE,
                    size=BUY_AMOUNT_SHARES,
                    side=Side.BUY,
                    token_id=tid
                ))
                log.info(f"BUY PLACED: {BUY_AMOUNT_SHARES} shares on {slug}")
            except Exception as e:
                log.error(f"Buy error: {e}")

    def start(self):
        while True:
            try:
                hour = time.gmtime().tm_hour
                if hour >= CANCEL_HOUR_UTC:
                    self.client.cancel_all_orders()
                else:
                    self.run_strategy()
                
                # Logic to check for FILLS should go here 
                # to trigger place_tiered_sells()
                
                time.sleep(60)
            except KeyboardInterrupt: break

if __name__ == "__main__":
    ShareBracketBot().start()
