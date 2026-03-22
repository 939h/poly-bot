"""
approve.py — Set USDC allowance for Polymarket CLOB
Run this once before trading, or whenever you get 'not enough allowance' error.

Usage:
    python approve.py
"""

import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds

load_dotenv()

HOST       = "https://clob.polymarket.com"
CHAIN_ID   = 137  # Polygon

creds = ApiCreds(
    api_key        = os.getenv("POLY_API_KEY"),
    api_secret     = os.getenv("POLY_API_SECRET"),
    api_passphrase = os.getenv("POLY_API_PASSPHRASE"),
)

client = ClobClient(
    host       = HOST,
    chain_id   = CHAIN_ID,
    key        = os.getenv("POLY_PRIVATE_KEY"),
    creds      = creds,
    funder     = os.getenv("POLY_FUNDER_ADDRESS"),
)

print("Setting USDC allowance...")
try:
    client.set_allowance()
    print("Allowance set successfully!")
except Exception as e:
    print(f"Error: {e}")
