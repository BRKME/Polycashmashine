"""
Edge Analyzer — computes Brier(model) vs Brier(market) from collected snapshots.

Uses price_history.json (pre-resolution prices + model probs)
+ actual temperatures to determine outcomes.

Run after collecting ≥50 resolved snapshots (~5-7 days).
"""

import json
import statistics
from datetime import date, timedelta
from collections import defaultdict

from config import CITIES
from weather_model import fetch_actual_temperature

HISTORY_FILE = "price_history.json"


def load_history():
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"snapshots": []}


def run():
    print("=" * 70, flush=True)
    print("EDGE ANALYSIS — Model vs Market (collected data)", flush=True)
    print("=" * 70, flush=True)

    history = load_history()
    snapshots = history.get("snapshots", [])
    print(f"Total snapshots: {len(snapshots)}", flush=True)

    if len(snapshots) < 10:
        print("Not enough data. Need ≥10 snapshots. Collect more days.", flush=True)
        return

    # Group by (city, date) — use first snapshot per bin (earliest price)
    seen = set()
    unique = []
    for s in snapshots:
        key = (s["city"], s["date"], s["bin_low"], s["bin_high"])
        if key not in seen:
            seen.add(key)
            unique.append(s)

    print(f"Unique (city, date, bin): {len(unique)}", flush=True)

    # Get actual temps for resolved dates
    actual_cache = {}
    dates_needed = set()
    for s in unique:
        dt = date.fromisoformat(s["date"])
        if dt < date.today():  # Only resolved
            dates_needed.add((s["city"], s["date"]))

    print(f"Resolved (city, date) pairs: {len(dates_needed)}", flush=True)

    for city_id, dt_str in dates_needed:
        dt = date.fromisoformat(dt_str)
        actual = fetch_actual_temperature(city_id, dt)
        if actual is not None:
            actual_cache[f"{city_id}_{dt_str}"] = actual

    print(f"Actuals fetched: {len(actual_cache)}", flush=True)

    # Match outcomes
    results_by_city = defaultdict(list)
    for s in unique:
        cache_key = f"{s['city']}_{s['date']}"
        if cache_key not in actual_cache:
            continue

        actual_temp = actual_cache[cache_key]
        # Did actual land in this bin?
        outcome = 1 if s["bin_low"] <= actual_temp < s["bin_high"] else 0

        results_by_city[s["city"]].append({
            **s,
            "actual_temp": actual_temp,
            "outcome": outcome,
        })

    total_entries = sum(len(v) for v in results_by_city.values())
    print(f"Entries with outcomes: {total_entries}", flush=True)

    if total_entries < 10:
        print("Not enough resolved data yet.", flush=True)
        return

    # Compute Brier scores
    print(f"\n{'=' * 70}", flush=True)
    print(f"{'City':<12} {'Brier(M)':>10} {'Brier(Mkt)':>11} {'ΔBrier':>9} {'Winner':>8} {'PnL':>8} {'Trades':>7}", flush=True)
    print("-" * 70, flush=True)

    for city_id in sorted(results_by_city.keys()):
        entries = results_by_city[city_id]
        if len(entries) < 5:
            continue

        model_brier = sum((e["model_prob"] - e["outcome"])**2 for e in entries) / len(entries)
        market_brier = sum((e["market_price"] - e["outcome"])**2 for e in entries) / len(entries)
        delta = model_brier - market_brier

        # PnL: bet when model > market + 5%, price 20-60¢
        pnl = 0
        n_trades = 0
        for e in entries:
            edge = e["model_prob"] - e["market_price"]
            if edge >= 0.05 and 0.20 <= e["market_price"] <= 0.60:
                if e["outcome"] == 1:
                    pnl += 1.0 - e["market_price"]
                else:
                    pnl -= e["market_price"]
                n_trades += 1

        winner = "MODEL" if delta < 0 else "MARKET"
        print(f"{city_id:<12} {model_brier:>10.4f} {market_brier:>11.4f} {delta:>+9.4f} {winner:>8} ${pnl:>+7.2f} {n_trades:>7}", flush=True)

    print(f"\n  ΔBrier < 0 = model beats market", flush=True)
    print(f"  Positive PnL = profitable strategy", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    run()
