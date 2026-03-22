"""
Trader — Autonomous order execution on Polymarket.

Takes edge signals from market_scanner, places orders via CLOB API.
Safety: max bet per trade, daily budget cap, dry-run mode.

Env vars:
  POLY_PRIVATE_KEY  — Polygon wallet private key
  POLY_FUNDER       — Polymarket proxy/funder address
  POLY_SIG_TYPE     — 0=EOA, 1=email/Magic, 2=browser proxy
  PROXY_URL         — SOCKS5/HTTPS proxy (e.g. socks5://user:pass@ip:port)
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

# === Set proxy BEFORE any imports that use requests/httpx ===
PROXY_URL = os.getenv("PROXY_URL", "")
if PROXY_URL:
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    os.environ["ALL_PROXY"] = PROXY_URL
    print(f"  Proxy: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL[:30]}...", flush=True)

from market_scanner import EdgeSignal, fetch_weather_markets, find_edge_signals

MAX_BET = float(os.getenv("MAX_BET", "5"))
DAILY_BUDGET = float(os.getenv("DAILY_BUDGET", "50"))
MAX_TRADES = int(os.getenv("MAX_TRADES", "6"))  # Max trades per run
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
        if d.get("date") != date.today().isoformat():
            return 0.0
        # Only sum actual fills, not dry runs or errors
        trades = d.get("trades", [])
        total = sum(
            t.get("amount", 0) for t in trades
            if isinstance(t, dict) and str(t.get("action", "")).startswith("BUY_")
        )
        return total
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


def kelly_bet(signal, max_bet):
    """
    Kelly-criterion inspired bet sizing.
    
    Bet size scales with:
    1. Edge strength (higher edge = more confident)
    2. Model probability (mid-range 30-70% = most reliable)
    3. Cluster probability (higher = more confident in neighborhood)
    
    Half-Kelly for safety (never bet full Kelly).
    """
    edge_frac = abs(signal.edge) / 100  # e.g. 0.50 for 50% edge
    model_p = signal.model_prob
    cluster_p = signal.cluster_prob

    # Kelly fraction: edge / odds
    # For binary markets: kelly = model_prob - market_price
    if signal.bet_side == "YES":
        kelly = model_p - signal.market_price
    else:
        kelly = (1 - model_p) - (1 - signal.market_price)

    kelly = max(kelly, 0)

    # Confidence multiplier based on cluster probability
    if cluster_p >= 0.90:
        conf = 1.0       # High confidence: full half-Kelly
    elif cluster_p >= 0.70:
        conf = 0.7        # Medium: 70% of half-Kelly
    else:
        conf = 0.4        # Low: 40% of half-Kelly

    # Half-Kelly * confidence * max_bet
    bet = max_bet * kelly * 2.0 * conf  # kelly*2 because kelly is usually small

    # Clamp
    bet = max(1.0, min(bet, max_bet))
    return bet


TRADE_HISTORY_FILE = "trade_history.json"

def log_trade(signal, amount, result):
    """Append trade to persistent history for performance tracking."""
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "city": signal.market.city_id,
        "date": str(signal.market.target_date),
        "bin": signal.bin_label,
        "side": signal.bet_side,
        "model_prob": round(signal.model_prob, 3),
        "market_price": round(signal.market_price, 3),
        "edge": round(signal.edge, 1),
        "cluster_prob": round(signal.cluster_prob, 3),
        "ev": round(signal.expected_value, 3),
        "amount": amount,
        "order_id": result.order_id[:20] if result.order_id else "",
        "question": signal.market.question[:80],
    }

    history = []
    try:
        with open(TRADE_HISTORY_FILE) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    history.append(entry)

    # Keep last 500 trades
    if len(history) > 500:
        history = history[-500:]

    with open(TRADE_HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def print_history_stats():
    """Print performance summary from trade history."""
    try:
        with open(TRADE_HISTORY_FILE) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return

    if not history:
        return

    total = len(history)
    total_bet = sum(t["amount"] for t in history)
    avg_edge = sum(t["edge"] for t in history) / total
    avg_ev = sum(t["ev"] for t in history) / total

    # By side
    yes_trades = [t for t in history if t["side"] == "YES"]
    no_trades = [t for t in history if t["side"] == "NO"]

    # By city
    cities = {}
    for t in history:
        c = t["city"]
        if c not in cities:
            cities[c] = {"count": 0, "total": 0}
        cities[c]["count"] += 1
        cities[c]["total"] += t["amount"]

    city_str = ", ".join(f"{c}({v['count']})" for c, v in sorted(cities.items()))
    print(f"\n{'─' * 50}", flush=True)
    print(f"TRADE HISTORY ({total} trades, ${total_bet:.2f} total)", flush=True)
    print(f"  YES: {len(yes_trades)} | NO: {len(no_trades)}", flush=True)
    print(f"  Avg edge: {avg_edge:+.1f}% | Avg EV: {avg_ev:.3f}", flush=True)
    print(f"  Cities: {city_str}", flush=True)
    print(f"{'─' * 50}", flush=True)


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

    # Only load daily spent from LIVE trades (ignore dry-run history)
    spent = 0.0
    if not DRY_RUN:
        spent = load_daily_spent()
    remaining = DAILY_BUDGET - spent
    client = None if DRY_RUN else get_clob_client()
    if not DRY_RUN and not client:
        print("  FATAL: Could not init CLOB client. Aborting.", flush=True)
        return
    results = []
    traded = 0

    print(f"  Budget: ${remaining:.2f} remaining of ${DAILY_BUDGET:.2f}", flush=True)

    consecutive_errors = 0
    for sig in signals:
        if remaining < 1:
            print("  Budget exhausted.", flush=True)
            break
        if traded >= MAX_TRADES:
            print(f"  Max trades ({MAX_TRADES}) reached.", flush=True)
            break
        if consecutive_errors >= 3:
            print("  3 consecutive errors — stopping.", flush=True)
            break
        if abs(sig.edge) < MIN_EDGE or sig.expected_value < MIN_EV:
            continue

        bet = kelly_bet(sig, MAX_BET)
        bet = min(bet, remaining)
        bet = round(bet, 2)
        if bet < 1.00:
            continue

        confidence = "HIGH" if sig.cluster_prob >= 0.90 else "MED" if sig.cluster_prob >= 0.70 else "LOW"
        print(f"\n  {sig.bet_side} {sig.bin_label} | edge={sig.edge:+.1f}% EV={sig.expected_value:.3f} conf={confidence} bet=${bet:.2f}", flush=True)
        print(f"    {sig.market.question[:70]}", flush=True)

        r = place_order(client, sig, bet)
        results.append(r)
        if r.action.startswith("BUY_"):
            remaining -= bet
            spent += bet
            traded += 1
            consecutive_errors = 0
            # Log to persistent history
            log_trade(sig, bet, r)
        elif r.action == "ERROR":
            consecutive_errors += 1
        time.sleep(1)

    # Only persist daily state for live runs
    if not DRY_RUN:
        save_daily(spent, [asdict(r) for r in results if r.action.startswith("BUY_")])
    print(f"\n  Trades: {traded} | Spent today: ${spent:.2f}", flush=True)

    # Print performance history
    print_history_stats()
    print("=" * 60, flush=True)


if __name__ == "__main__":
    run()
