"""
Model Validation — the ONLY test that matters.

For each city (London, Paris, Miami):
1. Get 60 days of D+1 historical forecasts
2. Get actual temperatures
3. Generate model probabilities per bin (Gaussian CDF with calibrated std)
4. Check: did the actual temp land in each bin? (0 or 1)
5. Compute:
   - Reliability curve: model says X% → actually happens Y%
   - Brier score (lower = better)
   - Calibration error
   - Simulated PnL: if we bet YES when model_prob > market_price

Without historical Polymarket prices, we simulate market as
uniform-ish distribution (each bin gets ~1/N_bins probability).
This is conservative — real market may be worse or better.
"""

import json
import statistics
import time
from datetime import date, timedelta
from math import erf, sqrt
from collections import defaultdict

from config import CITIES, OPEN_METEO_HISTORICAL_URL, OPEN_METEO_HISTORICAL_FORECAST_URL
from weather_model import _get_with_retry

# Only validate cities where calibration shows promise
VALIDATE_CITIES = ["london", "paris", "miami", "tel_aviv", "dallas"]

DAYS = 60  # Out-of-sample period
FORECAST_MODEL = "gfs_seamless"


def normal_cdf(x, mu, sigma):
    return 0.5 * (1 + erf((x - mu) / (sigma * sqrt(2))))


def load_calibration():
    try:
        with open("calibration.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def get_historical_data(city_id, start, end):
    """Get both forecasts and actuals for a city."""
    city = CITIES[city_id]
    params_base = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    if city["unit"] == "fahrenheit":
        params_base["temperature_unit"] = "fahrenheit"

    # Actuals
    data_act = _get_with_retry(OPEN_METEO_HISTORICAL_URL, params_base, timeout=120)
    actuals = {}
    if data_act:
        times = data_act.get("daily", {}).get("time", [])
        temps = data_act.get("daily", {}).get("temperature_2m_max", [])
        actuals = {t: v for t, v in zip(times, temps) if v is not None}

    time.sleep(1)

    # Forecasts
    params_fc = {**params_base, "models": FORECAST_MODEL}
    data_fc = _get_with_retry(OPEN_METEO_HISTORICAL_FORECAST_URL, params_fc, timeout=120)
    forecasts = {}
    if data_fc:
        times = data_fc.get("daily", {}).get("time", [])
        temps = data_fc.get("daily", {}).get("temperature_2m_max", [])
        forecasts = {t: v for t, v in zip(times, temps) if v is not None}

    return forecasts, actuals


def generate_bin_probs(forecast_temp, city_id, cal_data):
    """Generate model bin probabilities using Gaussian CDF + calibration."""
    city = CITIES[city_id]
    unit = city["unit"]
    bin_width = city["bin_width"]

    # Get calibration
    bias = 0.0
    cal_std = None
    if cal_data:
        cc = cal_data.get("cities", {}).get(city_id)
        if cc:
            bias = cc.get("bias", 0.0)
            cal_std = cc.get("real_std", None)

    corrected = forecast_temp - bias

    # Use calibrated std or fallback
    if cal_std and cal_std > 0:
        sigma = cal_std
    else:
        sigma = 2.0 if unit == "fahrenheit" else 1.2

    # Generate bins centered on forecast
    sym = "°F" if unit == "fahrenheit" else "°C"
    center = round(corrected / bin_width) * bin_width
    n_bins = 15
    start = center - (n_bins // 2) * bin_width

    bins = []
    for i in range(n_bins):
        low = start + i * bin_width
        high = low + bin_width
        prob = normal_cdf(high, corrected, sigma) - normal_cdf(low, corrected, sigma)
        prob = max(prob, 0.001)
        bins.append({
            "low": low,
            "high": high,
            "label": f"{int(low)}-{int(high)}{sym}",
            "model_prob": prob,
        })

    return bins, corrected, sigma


def run_validation():
    print("=" * 70, flush=True)
    print("MODEL VALIDATION — Does model beat market?", flush=True)
    print("=" * 70, flush=True)

    cal_data = load_calibration()
    if cal_data:
        print(f"Calibration: {cal_data.get('generated', '?')}, {len(cal_data.get('cities', {}))} cities", flush=True)
    else:
        print("WARNING: No calibration.json found, using fallback", flush=True)

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=DAYS)
    print(f"Period: {start} → {end} ({DAYS} days)", flush=True)

    all_results = {}

    for city_id in VALIDATE_CITIES:
        if city_id not in CITIES:
            continue

        city = CITIES[city_id]
        unit = city["unit"]
        bin_width = city["bin_width"]
        sym = "°F" if unit == "fahrenheit" else "°C"

        print(f"\n{'─' * 70}", flush=True)
        print(f"City: {city_id.upper()} (bin={bin_width}{sym})", flush=True)

        forecasts, actuals = get_historical_data(city_id, start, end)
        print(f"  Data: {len(forecasts)} forecasts, {len(actuals)} actuals", flush=True)

        if len(forecasts) < 20 or len(actuals) < 20:
            print(f"  SKIP: insufficient data", flush=True)
            continue

        # For each day: generate model probs, check which bin actual falls in
        model_brier_sum = 0
        uniform_brier_sum = 0
        n_predictions = 0
        
        # Reliability buckets: model_prob range → (hits, total)
        reliability = defaultdict(lambda: {"hits": 0, "total": 0})
        
        # PnL simulation: bet YES when model_prob > 1/n_bins (uniform assumption)
        pnl_trades = []

        for dt_str, fc_temp in forecasts.items():
            if dt_str not in actuals:
                continue
            actual_temp = actuals[dt_str]

            bins, corrected, sigma = generate_bin_probs(fc_temp, city_id, cal_data)

            for b in bins:
                # Did actual land in this bin?
                hit = 1 if b["low"] <= actual_temp < b["high"] else 0
                mp = b["model_prob"]

                # Brier score: (forecast - outcome)^2
                model_brier_sum += (mp - hit) ** 2
                
                # Uniform baseline: 1/n_bins
                uniform_prob = 1.0 / len(bins)
                uniform_brier_sum += (uniform_prob - hit) ** 2

                n_predictions += 1

                # Reliability: bucket by model probability
                bucket = round(mp * 20) / 20  # 5% buckets
                bucket = min(bucket, 0.95)
                reliability[bucket]["total"] += 1
                reliability[bucket]["hits"] += hit

                # PnL: simulate betting YES when model > uniform + edge_threshold
                edge_threshold = 0.05  # 5% edge minimum
                simulated_market = uniform_prob  # Conservative: assume market = uniform
                if mp > simulated_market + edge_threshold and 0.15 <= mp <= 0.65:
                    # Bet $1 at market price, win $(1-price) or lose $price
                    entry_price = simulated_market
                    if hit:
                        profit = 1.0 - entry_price
                    else:
                        profit = -entry_price
                    pnl_trades.append({
                        "date": dt_str,
                        "bin": b["label"],
                        "model": round(mp, 3),
                        "market": round(simulated_market, 3),
                        "hit": hit,
                        "profit": round(profit, 3),
                    })

        if n_predictions == 0:
            continue

        model_brier = model_brier_sum / n_predictions
        uniform_brier = uniform_brier_sum / n_predictions
        brier_skill = 1 - (model_brier / uniform_brier) if uniform_brier > 0 else 0

        print(f"\n  Brier Score:", flush=True)
        print(f"    Model:   {model_brier:.4f}", flush=True)
        print(f"    Uniform: {uniform_brier:.4f}", flush=True)
        print(f"    Skill:   {brier_skill:.3f} ({'model wins' if brier_skill > 0 else 'model LOSES'})", flush=True)

        # Reliability curve
        print(f"\n  Reliability (model_prob → actual_freq):", flush=True)
        print(f"    {'Predicted':>10} {'Actual':>10} {'N':>6} {'Gap':>8}", flush=True)
        cal_error_sum = 0
        cal_n = 0
        for bucket in sorted(reliability.keys()):
            r = reliability[bucket]
            if r["total"] < 5:
                continue
            actual_freq = r["hits"] / r["total"]
            gap = actual_freq - bucket
            cal_error_sum += abs(gap) * r["total"]
            cal_n += r["total"]
            marker = " ←MISCAL" if abs(gap) > 0.10 else ""
            print(f"    {bucket:>9.0%} {actual_freq:>9.1%} {r['total']:>6d} {gap:>+7.1%}{marker}", flush=True)

        if cal_n > 0:
            avg_cal_error = cal_error_sum / cal_n
            print(f"    Avg calibration error: {avg_cal_error:.1%}", flush=True)

        # PnL
        if pnl_trades:
            total_pnl = sum(t["profit"] for t in pnl_trades)
            n_wins = sum(1 for t in pnl_trades if t["hit"])
            winrate = n_wins / len(pnl_trades)
            avg_profit = total_pnl / len(pnl_trades)
            print(f"\n  Simulated PnL (vs uniform market, ≥5% edge, 20-65¢):", flush=True)
            print(f"    Trades: {len(pnl_trades)}", flush=True)
            print(f"    Winrate: {winrate:.1%}", flush=True)
            print(f"    Total PnL: ${total_pnl:+.2f} per $1 bets", flush=True)
            print(f"    Avg profit/trade: ${avg_profit:+.3f}", flush=True)
            print(f"    ROI: {total_pnl / len(pnl_trades) * 100:+.1f}%", flush=True)
        else:
            print(f"\n  No PnL trades generated (no signals passed filters)", flush=True)

        all_results[city_id] = {
            "model_brier": round(model_brier, 4),
            "uniform_brier": round(uniform_brier, 4),
            "brier_skill": round(brier_skill, 3),
            "n_predictions": n_predictions,
            "n_pnl_trades": len(pnl_trades),
            "total_pnl": round(sum(t["profit"] for t in pnl_trades), 2) if pnl_trades else 0,
        }

        time.sleep(1)

    # Summary
    print(f"\n{'=' * 70}", flush=True)
    print(f"SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"{'City':<12} {'Brier(M)':>10} {'Brier(U)':>10} {'Skill':>8} {'PnL':>8} {'Trades':>7}", flush=True)
    print("-" * 60, flush=True)
    for city_id, r in sorted(all_results.items()):
        skill_str = f"{r['brier_skill']:+.3f}"
        pnl_str = f"${r['total_pnl']:+.2f}"
        print(f"{city_id:<12} {r['model_brier']:>10.4f} {r['uniform_brier']:>10.4f} {skill_str:>8} {pnl_str:>8} {r['n_pnl_trades']:>7}", flush=True)

    print(f"\nBrier Skill > 0 = model beats uniform baseline", flush=True)
    print(f"Positive PnL = profitable after simulated bets", flush=True)
    print("=" * 70, flush=True)

    with open("validation_results.json", "w") as f:
        json.dump(all_results, f, indent=2)


if __name__ == "__main__":
    run_validation()
