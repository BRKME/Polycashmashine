"""
Trader — Autonomous order execution on Polymarket via CLOB API.

Takes edge signals from market_scanner and places orders.
Risk management: max bet per signal, daily budget, min edge threshold.

Requirements:
- POLY_PRIVATE_KEY: Polygon wallet private key
- POLY_FUNDER: Polymarket proxy/funder address
- POLY_SIG_TYPE: Signature type (0=EOA, 1=email/Magic, 2=browser proxy)
"""

import os
import json
import time
from datetime import datetime, date
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from market_scanner import EdgeSignal, fetch_weather_markets, find_edge_signals


@dataclass
class TradeResult:
    """Result of an executed trade."""
    signal_bin: str
    market_question: str
    city: str
    target_date: str
    bet_side: str
    token_id: str
    amount: float
    price: float
    model_prob: float
    edge: float
    ev: float
    order_id: str
    status: str          # "SUCCESS", "FAILED", "DRY_RUN"
    error: str
    timestamp: str


# === Risk Management ===

class RiskManager:
    """
    Controls position sizing and daily limits.
    
    Rules:
    - Max bet per signal: $BET_SIZE (default $2)
    - Max daily total: $DAILY_BUDGET (default $20)
    - Min edge to trade: MIN_TRADE_EDGE (default 15%)
    - Max signals per city per day: 3
    - Never bet more than balance allows
    """
    
    def __init__(self):
        self.bet_size = float(os.getenv("BET_SIZE", "2"))
        self.daily_budget = float(os.getenv("DAILY_BUDGET", "20"))
        self.min_edge = float(os.getenv("MIN_TRADE_EDGE", "15"))
        self.spent_today = 0.0
        self.trades_today: Dict[str, int] = {}  # city → count
    
    def can_trade(self, signal: EdgeSignal) -> tuple:
        """Check if we should trade this signal. Returns (ok, reason)."""
        if abs(signal.edge) < self.min_edge:
            return False, f"Edge {signal.edge:.1f}% < min {self.min_edge}%"
        
        if self.spent_today + self.bet_size > self.daily_budget:
            return False, f"Daily budget exhausted (${self.spent_today:.0f}/${self.daily_budget:.0f})"
        
        city = signal.market.city_id
        city_count = self.trades_today.get(city, 0)
        if city_count >= 3:
            return False, f"Max 3 trades per city ({city})"
        
        if signal.expected_value <= 0:
            return False, f"Negative EV: {signal.expected_value:.3f}"
        
        return True, "OK"
    
    def record_trade(self, signal: EdgeSignal, amount: float):
        """Record a completed trade for budget tracking."""
        self.spent_today += amount
        city = signal.market.city_id
        self.trades_today[city] = self.trades_today.get(city, 0) + 1


# === CLOB Client ===

def create_clob_client():
    """
    Initialize Polymarket CLOB client.
    Returns None if credentials not configured (dry run mode).
    """
    private_key = os.getenv("POLY_PRIVATE_KEY")
    funder = os.getenv("POLY_FUNDER")
    sig_type = int(os.getenv("POLY_SIG_TYPE", "1"))
    
    if not private_key or not funder:
        print("  ⚠️ CLOB credentials not set — DRY RUN mode")
        return None
    
    try:
        from py_clob_client.client import ClobClient
        
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            signature_type=sig_type,
            funder=funder,
        )
        client.set_api_creds(client.create_or_derive_api_creds())
        print("  ✅ CLOB client initialized")
        return client
    except Exception as e:
        print(f"  ❌ CLOB client failed: {e}")
        return None


def place_order(client, signal: EdgeSignal, amount: float) -> TradeResult:
    """
    Place a single order on Polymarket.
    
    For YES: buy YES tokens at market price (limit order at slightly above)
    For NO: buy NO tokens (second token in the pair)
    """
    base_result = {
        "signal_bin": signal.bin_label,
        "market_question": signal.market.question[:100],
        "city": signal.market.city_id,
        "target_date": signal.market.target_date.isoformat(),
        "bet_side": signal.bet_side,
        "token_id": signal.token_id,
        "amount": amount,
        "price": signal.market_price,
        "model_prob": signal.model_prob,
        "edge": signal.edge,
        "ev": signal.expected_value,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if client is None:
        # Dry run — log what we WOULD do
        return TradeResult(
            **base_result,
            order_id="DRY_RUN",
            status="DRY_RUN",
            error="",
        )
    
    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        
        # Market order — fill or kill at best available price
        order_args = MarketOrderArgs(
            token_id=signal.token_id,
            amount=amount,
            side=BUY,
        )
        
        signed_order = client.create_market_order(order_args)
        resp = client.post_order(signed_order, OrderType.FOK)
        
        order_id = resp.get("orderID", "unknown")
        status = "SUCCESS" if resp.get("success") else "FAILED"
        error = resp.get("errorMsg", "")
        
        return TradeResult(
            **base_result,
            order_id=order_id,
            status=status,
            error=error,
        )
        
    except Exception as e:
        return TradeResult(
            **base_result,
            order_id="ERROR",
            status="FAILED",
            error=str(e),
        )


# === Main Trading Loop ===

def run_trading(dry_run: bool = True) -> List[TradeResult]:
    """
    Full trading pipeline:
    1. Scan markets for edge signals
    2. Filter by risk management
    3. Place orders (or dry run)
    4. Report results
    """
    print(f"\n{'═' * 60}")
    print(f"POLYMARKET WEATHER BOT — {'DRY RUN' if dry_run else 'LIVE TRADING'}")
    print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═' * 60}\n")
    
    # Init risk manager
    risk = RiskManager()
    print(f"Config: bet=${risk.bet_size}, budget=${risk.daily_budget}/day, min_edge={risk.min_edge}%")
    
    # Init CLOB client (None in dry run)
    client = None
    if not dry_run:
        client = create_clob_client()
        if client is None:
            print("  Falling back to DRY RUN — no credentials")
            dry_run = True
    
    # Step 1: Scan markets
    print(f"\n📡 Scanning markets...")
    markets = fetch_weather_markets()
    if not markets:
        print("No markets found. Exiting.")
        return []
    
    # Step 2: Find edge signals
    print(f"\n🔍 Analyzing edge...")
    signals = find_edge_signals(markets)
    
    if not signals:
        print("No edge signals found. Market is efficient today.")
        return []
    
    print(f"\n📊 Found {len(signals)} signals. Executing...")
    
    # Step 3: Execute trades
    results = []
    
    for signal in signals:
        # Risk check
        can_trade, reason = risk.can_trade(signal)
        
        if not can_trade:
            print(f"  ⏭️ SKIP {signal.bet_side} {signal.bin_label} ({signal.market.city_id}): {reason}")
            continue
        
        # Execute
        amount = risk.bet_size
        result = place_order(client, signal, amount)
        results.append(result)
        
        # Track spend
        if result.status in ("SUCCESS", "DRY_RUN"):
            risk.record_trade(signal, amount)
        
        # Log
        emoji = {"SUCCESS": "✅", "DRY_RUN": "🔵", "FAILED": "❌"}[result.status]
        print(
            f"  {emoji} {result.status} | {result.bet_side} {result.signal_bin} "
            f"({result.city}) | ${amount:.2f} | "
            f"model={result.model_prob:.0%} market={result.price:.0%} edge={result.edge:+.0f}%"
        )
        if result.error:
            print(f"     Error: {result.error}")
        
        time.sleep(1)  # Rate limit between orders
    
    # Summary
    print(f"\n{'─' * 60}")
    successes = [r for r in results if r.status in ("SUCCESS", "DRY_RUN")]
    failures = [r for r in results if r.status == "FAILED"]
    total_spent = sum(r.amount for r in successes)
    avg_ev = sum(r.ev for r in successes) / len(successes) if successes else 0
    
    print(f"Executed: {len(successes)} trades, ${total_spent:.2f} deployed")
    if failures:
        print(f"Failed: {len(failures)} trades")
    print(f"Avg EV: {avg_ev:+.3f} per $1")
    print(f"Budget remaining: ${risk.daily_budget - risk.spent_today:.2f}")
    
    return results


def save_trade_log(results: List[TradeResult], path: str = "trade_log.json"):
    """Append results to trade log."""
    # Load existing
    existing = []
    try:
        with open(path) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    
    # Append new
    for r in results:
        existing.append(asdict(r))
    
    # Save (keep last 500)
    existing = existing[-500:]
    with open(path, "w") as f:
        json.dump(existing, f, indent=2)
    
    print(f"Trade log saved: {len(results)} new entries → {path}")


if __name__ == "__main__":
    import sys
    
    # Default: dry run. Pass --live for real trading.
    live = "--live" in sys.argv
    
    if live and not os.getenv("POLY_PRIVATE_KEY"):
        print("❌ --live requires POLY_PRIVATE_KEY, POLY_FUNDER env vars")
        sys.exit(1)
    
    results = run_trading(dry_run=not live)
    if results:
        save_trade_log(results)
