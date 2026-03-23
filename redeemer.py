"""
Auto-redeem resolved Polymarket positions.

Checks for winning positions in resolved markets and claims USDC.
Uses web3 to call ConditionalTokens.redeemPositions().
"""

import os
import json
import requests
import time
from datetime import datetime

# Set proxy if configured
PROXY_URL = os.getenv("PROXY_URL", "")
if PROXY_URL:
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    os.environ["ALL_PROXY"] = PROXY_URL

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"

# Polymarket contracts on Polygon
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
COLLATERAL = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC

# ConditionalTokens ABI (only redeemPositions)
CT_ABI = json.loads("""[{
    "inputs": [
        {"name": "collateralToken", "type": "address"},
        {"name": "parentCollectionId", "type": "bytes32"},
        {"name": "conditionId", "type": "bytes32"},
        {"name": "indexSets", "type": "uint256[]"}
    ],
    "name": "redeemPositions",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function"
}]""")


def get_user_address():
    """Get user's Polymarket address from CLOB client."""
    pk = os.getenv("POLY_PRIVATE_KEY")
    if not pk:
        return None
    try:
        from py_clob_client.client import ClobClient
        client = ClobClient(
            CLOB_HOST, key=pk, chain_id=137,
            signature_type=int(os.getenv("POLY_SIG_TYPE", "1")),
            funder=os.getenv("POLY_FUNDER"),
        )
        addr = client.get_address()
        print(f"  Wallet: {addr}", flush=True)
        return addr
    except Exception as e:
        print(f"  Error getting address: {e}", flush=True)
        return None


def find_redeemable_positions():
    """
    Find resolved markets where we might have winning positions.
    Returns list of conditionIds to try redeeming.
    """
    print("Checking for redeemable positions...", flush=True)
    
    # Get recent resolved temperature events
    try:
        resp = requests.get(f"{GAMMA_API}/events", params={
            "limit": 50,
            "tag_slug": "temperature",
            "closed": "true",
        }, timeout=30)
        events = resp.json()
    except Exception as e:
        print(f"  Error fetching events: {e}", flush=True)
        return []

    redeemable = []
    for event in events:
        markets = event.get("markets", [])
        for m in markets:
            cid = m.get("conditionId", "")
            resolved = m.get("closed", False)
            if not cid or not resolved:
                continue
            redeemable.append({
                "conditionId": cid,
                "question": m.get("question", "")[:60],
            })

    print(f"  Found {len(redeemable)} resolved markets", flush=True)
    return redeemable


def redeem_positions(redeemable):
    """
    Call redeemPositions on ConditionalTokens contract.
    """
    pk = os.getenv("POLY_PRIVATE_KEY")
    funder = os.getenv("POLY_FUNDER")
    if not pk:
        print("  No private key configured", flush=True)
        return

    try:
        from web3 import Web3
    except ImportError:
        print("  web3 not installed — run: pip install web3", flush=True)
        return

    # Connect to Polygon
    rpc_url = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        print("  Cannot connect to Polygon RPC", flush=True)
        return

    print(f"  Connected to Polygon (block {w3.eth.block_number})", flush=True)

    ct = w3.eth.contract(
        address=Web3.to_checksum_address(CONDITIONAL_TOKENS),
        abi=CT_ABI,
    )

    # Use funder address (proxy wallet) or derive from key
    account = w3.eth.account.from_key(pk)
    sender = funder if funder else account.address
    print(f"  Sender: {sender}", flush=True)

    redeemed = 0
    parent_collection = b'\x00' * 32  # Root collection

    for item in redeemable:
        cid_hex = item["conditionId"]
        
        # Ensure proper bytes32 format
        if cid_hex.startswith("0x"):
            condition_id = bytes.fromhex(cid_hex[2:])
        else:
            condition_id = bytes.fromhex(cid_hex)

        # Try redeeming both outcomes [1, 2] — contract will only redeem what we hold
        try:
            tx = ct.functions.redeemPositions(
                Web3.to_checksum_address(COLLATERAL),
                parent_collection,
                condition_id,
                [1, 2],
            ).build_transaction({
                "from": sender,
                "nonce": w3.eth.get_transaction_count(sender),
                "gas": 200000,
                "gasPrice": w3.eth.gas_price,
            })

            signed = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status == 1:
                redeemed += 1
                print(f"  ✓ Redeemed: {item['question']}", flush=True)
            else:
                print(f"  ✗ Failed: {item['question']}", flush=True)

        except Exception as e:
            err = str(e)
            if "execution reverted" in err.lower() or "nothing to redeem" in err.lower():
                pass  # No position in this market, skip silently
            else:
                print(f"  ✗ Error on {item['question'][:40]}: {err[:60]}", flush=True)

        time.sleep(0.5)

    print(f"\n  Redeemed {redeemed} positions", flush=True)
    return redeemed


def run_redeem():
    print("=" * 50, flush=True)
    print("AUTO-REDEEM", flush=True)
    print(f"{datetime.utcnow().isoformat()} UTC", flush=True)
    print("=" * 50, flush=True)

    addr = get_user_address()
    if not addr:
        print("No wallet configured.", flush=True)
        return

    redeemable = find_redeemable_positions()
    if not redeemable:
        print("Nothing to redeem.", flush=True)
        return

    redeem_positions(redeemable)
    print("=" * 50, flush=True)


if __name__ == "__main__":
    run_redeem()
