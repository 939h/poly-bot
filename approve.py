"""
approve.py — Set USDC allowance for Polymarket CLOB contracts
Run once before trading, or when you get 'not enough balance / allowance' error.

Requirements:
    pip install web3

Usage:
    python approve.py
"""

import os
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

# Polygon RPC
RPC_URL     = "https://polygon-bor-rpc.publicnode.com"
PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY")
PUBLIC_KEY  = os.getenv("POLY_FUNDER_ADDRESS")

# Polymarket contract addresses on Polygon
USDC_ADDRESS     = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e on Polygon
CTF_ADDRESS      = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # CTF Exchange
NEG_RISK_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"  # Neg Risk Adapter

MAX_INT = 2**256 - 1

ERC20_ABI = [{
    "name": "approve",
    "type": "function",
    "inputs": [
        {"name": "spender", "type": "address"},
        {"name": "amount",  "type": "uint256"}
    ],
    "outputs": [{"name": "", "type": "bool"}]
}]

CTF_ABI = [{
    "name": "setApprovalForAll",
    "type": "function",
    "inputs": [
        {"name": "operator", "type": "address"},
        {"name": "approved",  "type": "bool"}
    ],
    "outputs": []
}]

def send_tx(w3, contract_func, account, private_key, desc, nonce):
    try:
        tx = contract_func.build_transaction({
            "from":     account,
            "nonce":    nonce,
            "gas":      100000,
            "gasPrice": w3.eth.gas_price,
        })
        signed   = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash  = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt  = w3.eth.wait_for_transaction_receipt(tx_hash)
        status   = "OK" if receipt.status == 1 else "FAILED"
        print(f"  {desc} -> {status} (tx: {tx_hash.hex()[:16]}...)")
    except Exception as e:
        print(f"  {desc} -> ERROR: {e}")

def main():
    if not PRIVATE_KEY:
        print("ERROR: POLY_PRIVATE_KEY not set in Railway Variables")
        return
    if not PUBLIC_KEY:
        print("ERROR: POLY_FUNDER_ADDRESS not set in Railway Variables")
        return

    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    if not w3.is_connected():
        print("Failed to connect to Polygon RPC")
        return

    account = Web3.to_checksum_address(PUBLIC_KEY)
    print(f"Wallet: {account}")
    print("Setting allowances...\n")

    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI)
    ctf  = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS),  abi=CTF_ABI)
    neg  = w3.eth.contract(address=Web3.to_checksum_address(NEG_RISK_ADDRESS), abi=CTF_ABI)

    nonce = w3.eth.get_transaction_count(account)
    send_tx(w3, usdc.functions.approve(CTF_ADDRESS, MAX_INT),           account, PRIVATE_KEY, "USDC approve -> CTF Exchange",        nonce)
    send_tx(w3, usdc.functions.approve(NEG_RISK_ADDRESS, MAX_INT),      account, PRIVATE_KEY, "USDC approve -> Neg Risk Adapter",    nonce+1)
    send_tx(w3, ctf.functions.setApprovalForAll(CTF_ADDRESS, True),     account, PRIVATE_KEY, "CTF setApprovalForAll -> CTF Exchange", nonce+2)
    send_tx(w3, ctf.functions.setApprovalForAll(NEG_RISK_ADDRESS, True),account, PRIVATE_KEY, "CTF setApprovalForAll -> Neg Risk",   nonce+3)

    print("\nDone! You can now trade on Polymarket.")

if __name__ == "__main__":
    main()
