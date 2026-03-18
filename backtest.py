"""
Backtest — Validate weather edge on historical data.

Strategy:
1. For each past day, get what the model predicted (historical forecast)
2. Get actual observed temperature
3. Calculate: model probability per bin vs naive uniform probability
4. Simulate betting on bins where model has >N% edge
5. Report P&L, hit rate, calibration

Key question: does the ensemble model reliably beat a naive market?
"""

import json
import time
import statistics
from datetime import date, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

from config import CITIES, BACKTEST_DAYS, MIN_EDGE_PERCENT
from weather_model import (
    fetch_historical_forecast,
    fetch_actual_temperature,
    BinProbability,
    ForecastResult,
)


@dataclass
class BacktestTrade:
    """One simulated trade."""
    city_id: str
    target_date: str
    bin_label: str
    bin_low: float
    bin_high: float
    model_prob: float      # what our model said
    naive_prob: float      # what a naive market would price
    edge: float            # model_prob - naive_prob
    bet_side: str          # "YES" or "NO"
    bet_amount: float      # simulated $
    actual_temp: float     # what actually happened
    outcome: str           # "WIN" or "LOSS"
    pnl: float             # profit or loss


@dataclass
class BacktestResult:
    """Full backtest summary."""
    cities: List[str]
    days_tested: int
    total_trades: int
    wins: int
    losses: int
    hit_rate: float
    total_pnl: float
    avg_pnl_per_trade: float
    avg_edge: float
    max_drawdown: float
    calibration: Dict     # model_prob_bucket → actual_hit_rate
    trades: List[BacktestTrade]


def simulate_market_prices(bins: list, ensemble_mean: float, ensemble_std: float) -> Dict[str, float]:
    """
    Simulate realistic Polymarket crowd pricing.
    
    The crowd roughly follows the weather forecast but with MORE uncertainty
    than the ensemble model (slower to update, emotional bias, round-number anchoring).
    We model the crowd as a gaussian with 1.5x the model's std deviation.
    
    Returns dict: bin_label → simulated market price (0..1).
    """
    import math
    crowd_std = max(ensemble_std * 1.5, 2.0)  # Crowd is less precise than model
    
    prices = {}
    total = 0
    for b in bins:
        bin_center = (b.bin_low + b.bin_high) / 2
        # Gaussian probability for this bin center
        z = (bin_center - ensemble_mean) / crowd_std if crowd_std > 0 else 0
        prob = math.exp(-0.5 * z * z)
        prices[b.label] = prob
        total += prob
    
    # Normalize to sum to 1.0
    if total > 0:
        for label in prices:
            prices[label] /= total
    
    return prices


def run_backtest(
    cities: Optional[List[str]] = None,
    days: int = BACKTEST_DAYS,
    min_edge: float = MIN_EDGE_PERCENT,
    bet_amount: float = 10.0,
) -> BacktestResult:
    """
    Run backtest across cities and days.
    
    For each day:
    1. Get historical forecast (what model predicted 1 day before)
    2. Get actual temperature (what happened)
    3. For each bin where model_prob > naive_prob + min_edge:
       - Bet YES at naive_prob price
       - If actual temp in bin → WIN ($1 payout - cost)
       - If not → LOSS (-cost)
    """
    if cities is None:
        cities = ["nyc"]  # Start with NYC — most liquid market

    trades: List[BacktestTrade] = []
    errors = 0

    end_date = date.today() - timedelta(days=2)  # Leave 2-day buffer for data availability
    start_date = end_date - timedelta(days=days)

    print(f"═══════════════════════════════════════════")
    print(f"BACKTEST: {start_date} → {end_date} ({days} days)")
    print(f"Cities: {', '.join(cities)}")
    print(f"Min edge: {min_edge}%")
    print(f"Bet amount: ${bet_amount}")
    print(f"═══════════════════════════════════════════\n")

    for city_id in cities:
        city_cfg = CITIES.get(city_id)
        if not city_cfg:
            print(f"Unknown city: {city_id}")
            continue

        print(f"📍 {city_cfg['name']} ({city_id})")
        city_trades = 0

        current_date = start_date
        while current_date <= end_date:
            # Fetch historical forecast
            forecast = fetch_historical_forecast(city_id, current_date)
            if not forecast:
                errors += 1
                current_date += timedelta(days=1)
                time.sleep(0.3)  # Rate limit
                continue

            # Fetch actual temperature
            actual = fetch_actual_temperature(city_id, current_date)
            if actual is None:
                errors += 1
                current_date += timedelta(days=1)
                time.sleep(0.3)
                continue

            # Simulate realistic market prices (gaussian crowd)
            market_prices = simulate_market_prices(
                forecast.bins, forecast.ensemble_mean, forecast.ensemble_std
            )

            # Find bins with real edge: model_prob vs simulated_market_price
            for b in forecast.bins:
                market_price = market_prices.get(b.label, 0)
                edge = (b.probability - market_price) * 100  # edge in percentage points

                # === YES BET: model is MORE confident than market ===
                if edge >= min_edge and b.probability >= 0.15:
                    # Buy YES at market price, payout $1 if correct
                    cost = market_price * bet_amount
                    in_bin = b.bin_low <= actual <= b.bin_high

                    if in_bin:
                        pnl = bet_amount - cost
                        outcome = "WIN"
                    else:
                        pnl = -cost
                        outcome = "LOSS"

                    trades.append(BacktestTrade(
                        city_id=city_id,
                        target_date=current_date.isoformat(),
                        bin_label=b.label,
                        bin_low=b.bin_low,
                        bin_high=b.bin_high,
                        model_prob=b.probability,
                        naive_prob=market_price,
                        edge=edge,
                        bet_side="YES",
                        bet_amount=bet_amount,
                        actual_temp=actual,
                        outcome=outcome,
                        pnl=pnl,
                    ))
                    city_trades += 1

                # === NO BET: market overprices a bin, model says unlikely ===
                # Only bet NO when market prices bin at >8% but model says <2%
                elif market_price > 0.08 and b.probability < 0.02:
                    cost = (1 - market_price) * bet_amount
                    in_bin = b.bin_low <= actual <= b.bin_high

                    if not in_bin:
                        pnl = bet_amount - cost
                        outcome = "WIN"
                    else:
                        pnl = -cost
                        outcome = "LOSS"

                    trades.append(BacktestTrade(
                        city_id=city_id,
                        target_date=current_date.isoformat(),
                        bin_label=b.label,
                        bin_low=b.bin_low,
                        bin_high=b.bin_high,
                        model_prob=b.probability,
                        naive_prob=market_price,
                        edge=(b.probability - market_price) * 100,
                        bet_side="NO",
                        bet_amount=bet_amount,
                        actual_temp=actual,
                        outcome=outcome,
                        pnl=pnl,
                    ))
                    city_trades += 1

            current_date += timedelta(days=1)
            time.sleep(0.5)  # Rate limit Open-Meteo (increased from 0.3)

        print(f"  Trades: {city_trades}")

    # Calculate results
    wins = sum(1 for t in trades if t.outcome == "WIN")
    losses = sum(1 for t in trades if t.outcome == "LOSS")
    total = len(trades)
    hit_rate = wins / total if total > 0 else 0
    total_pnl = sum(t.pnl for t in trades)
    avg_pnl = total_pnl / total if total > 0 else 0
    avg_edge = statistics.mean([t.edge for t in trades]) if trades else 0

    # Max drawdown
    cumulative = 0
    peak = 0
    max_dd = 0
    for t in trades:
        cumulative += t.pnl
        peak = max(peak, cumulative)
        dd = peak - cumulative
        max_dd = max(max_dd, dd)

    # Calibration: group by model probability buckets
    calibration = {}
    prob_buckets = [(0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
    for low, high in prob_buckets:
        bucket_trades = [t for t in trades if low <= t.model_prob < high]
        if bucket_trades:
            bucket_wins = sum(1 for t in bucket_trades if t.outcome == "WIN")
            calibration[f"{int(low*100)}-{int(high*100)}%"] = {
                "trades": len(bucket_trades),
                "wins": bucket_wins,
                "actual_rate": bucket_wins / len(bucket_trades),
                "avg_model_prob": statistics.mean([t.model_prob for t in bucket_trades]),
            }

    result = BacktestResult(
        cities=cities,
        days_tested=days,
        total_trades=total,
        wins=wins,
        losses=losses,
        hit_rate=hit_rate,
        total_pnl=total_pnl,
        avg_pnl_per_trade=avg_pnl,
        avg_edge=avg_edge,
        max_drawdown=max_dd,
        calibration=calibration,
        trades=trades,
    )

    return result


def print_results(result: BacktestResult) -> None:
    """Print backtest results in a readable format."""
    print(f"\n{'═' * 50}")
    print(f"BACKTEST RESULTS")
    print(f"{'═' * 50}")
    print(f"Cities: {', '.join(result.cities)}")
    print(f"Days tested: {result.days_tested}")
    print(f"Total trades: {result.total_trades}")
    print(f"Wins: {result.wins} | Losses: {result.losses}")
    print(f"Hit rate: {result.hit_rate:.1%}")
    print(f"Total P&L: ${result.total_pnl:,.2f}")
    print(f"Avg P&L per trade: ${result.avg_pnl_per_trade:,.2f}")
    print(f"Avg edge: {result.avg_edge:.1f}%")
    print(f"Max drawdown: ${result.max_drawdown:,.2f}")

    if result.calibration:
        print(f"\nCalibration (model probability vs actual hit rate):")
        for bucket, data in result.calibration.items():
            print(
                f"  {bucket:>10s}: "
                f"model={data['avg_model_prob']:.1%} → "
                f"actual={data['actual_rate']:.1%} "
                f"({data['wins']}/{data['trades']} trades)"
            )

    # Show sample trades
    if result.trades:
        print(f"\nSample trades (last 10):")
        for t in result.trades[-10:]:
            emoji = "✅" if t.outcome == "WIN" else "❌"
            print(
                f"  {emoji} {t.target_date} {t.city_id} "
                f"{t.bet_side} {t.bin_label} "
                f"model={t.model_prob:.0%} naive={t.naive_prob:.0%} "
                f"edge={t.edge:+.0f}% "
                f"actual={t.actual_temp:.0f} "
                f"P&L=${t.pnl:+.2f}"
            )

    # Verdict
    print(f"\n{'═' * 50}")
    if result.total_trades < 20:
        print("⚠️  INSUFFICIENT DATA — need more trades for statistical significance")
    elif result.hit_rate > 0.55 and result.total_pnl > 0:
        print("✅ PROMISING — positive edge detected, proceed to paper trading")
    elif result.total_pnl > 0:
        print("🟡 MARGINAL — positive P&L but low hit rate, needs optimization")
    else:
        print("❌ NO EDGE — model doesn't beat naive baseline")
    print(f"{'═' * 50}")


def save_results(result: BacktestResult, path: str = "backtest_results.json") -> None:
    """Save backtest results to JSON."""
    data = {
        "cities": result.cities,
        "days_tested": result.days_tested,
        "total_trades": result.total_trades,
        "wins": result.wins,
        "losses": result.losses,
        "hit_rate": result.hit_rate,
        "total_pnl": result.total_pnl,
        "avg_pnl_per_trade": result.avg_pnl_per_trade,
        "avg_edge": result.avg_edge,
        "max_drawdown": result.max_drawdown,
        "calibration": result.calibration,
        "trades": [asdict(t) for t in result.trades],
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nResults saved to {path}")


if __name__ == "__main__":
    import sys
    import os

    cities = sys.argv[1:] if len(sys.argv) > 1 else ["nyc"]
    days = int(os.getenv("BACKTEST_DAYS", "30"))

    result = run_backtest(cities=cities, days=days, min_edge=10.0)
    print_results(result)
    save_results(result)
