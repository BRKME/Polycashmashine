"""
Price Collector — snapshots market prices + model probs for future validation.

Runs alongside each scan. Saves timestamped records to price_history.json.
After markets resolve, we match outcomes → compute real Brier(model) vs Brier(market).

This is the ONLY way to get pre-resolution market prices.
"""

import json
import time
from datetime import datetime, date, timedelta, timezone
from collections import defaultdict
from math import erf, sqrt

from config import CITIES
from market_scanner import fetch_weather_markets, match_city, parse_date_from_question
from weather_model import fetch_ensemble_forecast

HISTORY_FILE = "price_history.json"
VALIDATE_CITIES = ["london", "paris", "miami", "tel_aviv", "dallas"]


def normal_cdf(x, mu, sigma):
    return 0.5 * (1 + erf((x - mu) / (sigma * sqrt(2))))


def load_calibration():
    try:
        with open("calibration.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def load_history():
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"snapshots": [], "outcomes": {}}


def save_history(history):
    # Keep last 2000 snapshots
    if len(history["snapshots"]) > 2000:
        history["snapshots"] = history["snapshots"][-2000:]
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def collect():
    """Snapshot current market prices + model probs for all D+1 markets."""
    print("=" * 60, flush=True)
    print("PRICE COLLECTOR", flush=True)
    print(f"{datetime.now(timezone.utc).isoformat()} UTC", flush=True)
    print("=" * 60, flush=True)

    cal_data = load_calibration()
    history = load_history()

    # Fetch markets
    markets = fetch_weather_markets()
    if not markets:
        print("No markets.", flush=True)
        return

    # Group by (city, date), only D+1
    tomorrow = date.today() + timedelta(days=1)
    groups = defaultdict(list)
    for m in markets:
        if m.city_id in VALIDATE_CITIES and m.target_date == tomorrow:
            groups[(m.city_id, m.target_date)].append(m)

    print(f"  D+1 groups: {len(groups)}", flush=True)

    now_str = datetime.now(timezone.utc).isoformat()
    new_snapshots = 0

    for (city_id, target_date), city_markets in groups.items():
        # Fetch forecast once
        forecast = fetch_ensemble_forecast(city_id, target_date, model="gfs_seamless")
        if not forecast:
            continue

        # Get calibration
        bias = 0.0
        cal_std = None
        if cal_data:
            cc = cal_data.get("cities", {}).get(city_id)
            if cc:
                bias = cc.get("bias", 0.0)
                cal_std = cc.get("real_std", None)

        corrected_mean = forecast.ensemble_mean  # Already bias-corrected in weather_model
        sigma = forecast.ensemble_std

        for market in city_markets:
            # Get YES price
            import re
            question = market.question.lower()
            temp_match = re.search(r'be (?:between )?(\d+)(?:[°\s]|$)', question)
            if not temp_match:
                continue

            range_match = re.search(r'between\s+(\d+)\s*[-–]\s*(\d+)', question)
            is_below = "or below" in question
            is_above = "or higher" in question

            if range_match:
                bin_low = float(range_match.group(1))
                bin_high = float(range_match.group(2)) + 0.99
            elif is_below:
                bin_low = float(temp_match.group(1)) - 20
                bin_high = float(temp_match.group(1)) + 0.99
            elif is_above:
                bin_low = float(temp_match.group(1))
                bin_high = float(temp_match.group(1)) + 20
            else:
                bin_low = float(temp_match.group(1))
                bin_high = float(temp_match.group(1)) + 0.99

            # Model probability
            model_prob = normal_cdf(bin_high, corrected_mean, sigma) - normal_cdf(bin_low, corrected_mean, sigma)
            model_prob = max(model_prob, 0.001)

            # Market price (YES)
            yes_price = 0
            for b in market.bins:
                if b.label.lower() in ("yes", "y"):
                    yes_price = b.price
                    break
            if yes_price == 0 and market.bins:
                yes_price = market.bins[0].price

            snapshot = {
                "timestamp": now_str,
                "city": city_id,
                "date": target_date.isoformat(),
                "question": market.question[:80],
                "bin_low": bin_low,
                "bin_high": bin_high,
                "market_price": round(yes_price, 4),
                "model_prob": round(model_prob, 4),
                "model_mean": round(corrected_mean, 1),
                "model_std": round(sigma, 2),
            }
            history["snapshots"].append(snapshot)
            new_snapshots += 1

        time.sleep(0.5)

    save_history(history)
    print(f"  New snapshots: {new_snapshots}", flush=True)
    print(f"  Total in history: {len(history['snapshots'])}", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    collect()
