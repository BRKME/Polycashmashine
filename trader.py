"""
Trader — Autonomous order execution on Polymarket.

Takes edge signals from market_scanner, places orders via CLOB API.
Safety: max bet per trade, daily budget cap, dry-run mode.

Env vars:
  POLY_PRIVATE_KEY  — Polygon wallet private key
  POLY_FUNDER       — Polymarket proxy/funder address
  POLY_SIG_TYPE     — 0=EOA, 1=email/Magic, 2=browser proxy
  DRY_RUN           — "true" = no real orders (default)
  MAX_BET           — $ per trade (default 5)
  DAILY_BUDGET      — $ max per day (default 50)
"""

import os
import json
import time
from datetime import datetime, date
from dataclasses import dataclass, asdict
from typing import List

from market_scanner import EdgeSignal, fetch_weather_markets, find_edge_signals

MAX_BET = float(os.getenv("MAX_BET", "5"))
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET", "50"))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MIN_EDGE = float(os.getenv("MIN_EDGE", "15"))
MIN_EV = float(os.getenv("MIN_EV", "0.10"))

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137


@dataclass
class TradeResult:
    signal: dict
    action: str
    amount: float
    price: float
    order_id: str
    timestamp: str
    reason: str


def get_clob_client():
    pk = os.getenv("POLY_PRIVATE_KEY")
    if not pk:
        return None
    try:
        from py_clob_client.client import ClobClient
        client = ClobClient(
            CLOB_HOST, key=pk, chain_id=CHAIN_ID,
            signature_type=int(os.getenv("POLY_SIG_TYPE", "1")),
            funder=os.getenv("POLY_FUNDER"),
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        print("  CLOB client OK", flush=True)
        return client
    except Exception as e:
        print(f"  CLOB init error: {e}", flush=True)
        return None


def load_daily_spent():
    try:
        with open("daily_trades.json") as f:
            d = json.load(f)
        if d.get("date") == date.today().isoformat():
            return float(d.get("spent", 0))
    except Exception:
        pass
    return 0.0


def save_daily(spent, trades):
    with open("daily_trades.json", "w") as f:
        json.dump({"date": date.today().isoformat(), "spent": spent, "trades": trades}, f, indent=2)


def place_order(client, signal, amount):
    ts = datetime.utcnow().isoformat()
    sd = {
        "market": signal.market.question[:80],
        "bin": signal.bin_label, "side": signal.bet_side,
        "model": signal.model_prob, "market": signal.market_price,
        "edge": signal.edge, "ev": signal.expected_value,
    }

    if DRY_RUN or not client:
        tag = "DRY_RUN" if DRY_RUN else "NO_CLIENT"
        print(f"    [{tag}] {signal.bet_side} ${amount:.2f} on {signal.bin_label}", flush=True)
        return TradeResult(sd, tag, amount, signal.market_price, "", ts, tag)

    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        if not signal.token_id:
            return TradeResult(sd, "ERROR", 0, 0, "", ts, "No token_id")

        order = MarketOrderArgs(token_id=signal.token_id, amount=amount, side=BUY)
        signed = client.create_market_order(order)
        resp = client.post_order(signed, OrderType.FOK)

        oid = resp.get("orderID", "")
        if resp.get("success"):
            print(f"    FILLED: {signal.bet_side} ${amount:.2f} id={oid[:16]}", flush=True)
            return TradeResult(sd, f"BUY_{signal.bet_side}", amount, signal.market_price, oid, ts, "OK")
        else:
            err = resp.get("errorMsg", "rejected")
            print(f"    REJECTED: {err}", flush=True)
            return TradeResult(sd, "ERROR", 0, 0, "", ts, err)
    except Exception as e:
        print(f"    ERROR: {e}", flush=True)
        return TradeResult(sd, "ERROR", 0, 0, "", ts, str(e))


def run():
    print("=" * 60, flush=True)
    print("POLYMARKET WEATHER BOT", flush=True)
    print(f"{datetime.utcnow().isoformat()} UTC", flush=True)
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}", flush=True)
    print("=" * 60, flush=True)

    # 1. Scan
    print("\n[1/3] Scanning...", flush=True)
    markets = fetch_weather_markets()
    if not markets:
        print("No markets. Done.", flush=True)
        return

    # 2. Edge
    print("\n[2/3] Finding edge...", flush=True)
    signals = find_edge_signals(markets, min_edge=10.0)
    from market_scanner import print_signals
    print_signals(signals)

    if not signals:
        print("\nNo signals. Done.", flush=True)
        return

    # 3. Trade
    print("\n[3/3] Executing...", flush=True)
    signals = sorted(signals, key=lambda s: s.expected_value, reverse=True)

    spent = load_daily_spent()
    remaining = DAILY_BUDGET - spent
    client = None if DRY_RUN else get_clob_client()
    results = []
    traded = 0

    print(f"  Budget: ${remaining:.2f} remaining of ${DAILY_BUDGET:.2f}", flush=True)

    for sig in signals:
        if remaining < 1:
            print("  Budget exhausted.", flush=True)
            break
        if abs(sig.edge) < MIN_EDGE or sig.expected_value < MIN_EV:
            continue

        bet = min(MAX_BET * min(abs(sig.edge) / 30, 1.0), remaining)
        bet = round(bet, 2)
        if bet < 0.50:
            continue

        print(f"\n  {sig.bet_side} {sig.bin_label} | edge={sig.edge:+.1f}% EV={sig.expected_value:.3f} bet=${bet:.2f}", flush=True)
        print(f"    {sig.market.question[:70]}", flush=True)

        r = place_order(client, sig, bet)
        results.append(r)
        if r.action not in ("ERROR",):
            remaining -= bet
            spent += bet
            traded += 1
        time.sleep(1)

    save_daily(spent, [asdict(r) for r in results])
    print(f"\n  Trades: {traded} | Spent today: ${spent:.2f}", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    run()
