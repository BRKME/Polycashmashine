"""
Position Monitor — re-evaluate open positions, signal exits.

Every run:
1. Load open positions (from trade_history.json)
2. Re-fetch model forecast for each position
3. Compare current model_prob vs entry price
4. Signal: HOLD, SELL (cut loss), or TAKE_PROFIT

This enables dynamic trading:
- Buy when models agree and market is behind
- Sell when models shift against us (before resolution)
- Take profit when market caught up to our price
"""

import json
import time
import re
from datetime import datetime, date, timedelta, timezone
from math import erf, sqrt
from typing import List
from dataclasses import dataclass

from config import CITIES
from weather_model import fetch_ensemble_forecast


HISTORY_FILE = "trade_history.json"


def normal_cdf(x, mu, sigma):
    return 0.5 * (1 + erf((x - mu) / (sigma * sqrt(2))))


def load_calibration():
    try:
        with open("calibration.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


@dataclass
class PositionAction:
    city: str
    target_date: str
    bin_label: str
    side: str
    entry_price: float
    current_model_prob: float
    action: str          # HOLD, SELL, TAKE_PROFIT
    reason: str
    urgency: str         # HIGH, MEDIUM, LOW


def load_open_positions():
    """Load positions that haven't resolved yet."""
    try:
        with open(HISTORY_FILE) as f:
            trades = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

    today = date.today()
    open_pos = []
    for t in trades:
        trade_date = date.fromisoformat(t.get("date", "2020-01-01"))
        if trade_date >= today:  # Not yet resolved
            open_pos.append(t)
    return open_pos


def evaluate_position(pos, cal_data):
    """Re-evaluate a single position with latest forecast."""
    city_id = pos.get("city")
    target_date = date.fromisoformat(pos.get("date"))
    entry_price = pos.get("market_price", 0)
    side = pos.get("side", "YES")

    city = CITIES.get(city_id)
    if not city:
        return None

    # Get latest forecast
    forecast = fetch_ensemble_forecast(city_id, target_date, model="gfs_seamless")
    if not forecast:
        return None

    # Parse bin from position
    bin_label = pos.get("bin", "")
    match = re.match(r'(\d+)-(\d+)', bin_label)
    if not match:
        return None

    bin_low = float(match.group(1))
    bin_high = float(match.group(2)) + 0.99

    # Compute current model probability
    corrected_mean = forecast.ensemble_mean
    sigma = forecast.ensemble_std

    current_prob = normal_cdf(bin_high, corrected_mean, sigma) - normal_cdf(bin_low, corrected_mean, sigma)
    current_prob = max(current_prob, 0.001)

    # Decision logic
    if side == "YES":
        # We bought YES. Profitable if model still thinks YES is likely.
        prob_drop = pos.get("model_prob", current_prob) - current_prob

        if current_prob < entry_price * 0.7:
            # Model now thinks <70% of our entry price → CUT LOSS
            return PositionAction(
                city=city_id, target_date=pos["date"],
                bin_label=bin_label, side=side,
                entry_price=entry_price,
                current_model_prob=current_prob,
                action="SELL",
                reason=f"Model dropped to {current_prob:.0%} (entry was {entry_price:.0%})",
                urgency="HIGH",
            )
        elif current_prob > entry_price * 1.5 and current_prob > 0.60:
            # Model very confident AND above our entry → could take profit
            return PositionAction(
                city=city_id, target_date=pos["date"],
                bin_label=bin_label, side=side,
                entry_price=entry_price,
                current_model_prob=current_prob,
                action="TAKE_PROFIT",
                reason=f"Model at {current_prob:.0%}, entry was {entry_price:.0%}",
                urgency="LOW",
            )
        else:
            return PositionAction(
                city=city_id, target_date=pos["date"],
                bin_label=bin_label, side=side,
                entry_price=entry_price,
                current_model_prob=current_prob,
                action="HOLD",
                reason=f"Model at {current_prob:.0%}, entry {entry_price:.0%}",
                urgency="LOW",
            )

    return None


def monitor():
    print("=" * 60, flush=True)
    print("POSITION MONITOR", flush=True)
    print(f"{datetime.now(timezone.utc).isoformat()} UTC", flush=True)
    print("=" * 60, flush=True)

    cal_data = load_calibration()
    positions = load_open_positions()

    if not positions:
        print("  No open positions.", flush=True)
        return []

    print(f"  Open positions: {len(positions)}", flush=True)

    actions = []
    for pos in positions:
        action = evaluate_position(pos, cal_data)
        if action:
            actions.append(action)
            emoji = {"SELL": "🔴", "TAKE_PROFIT": "🟢", "HOLD": "⚪"}.get(action.action, "?")
            print(f"\n  {emoji} {action.action} | {action.city} {action.target_date} {action.bin_label}", flush=True)
            print(f"    {action.reason}", flush=True)
        time.sleep(0.5)

    # Summary
    sells = [a for a in actions if a.action == "SELL"]
    profits = [a for a in actions if a.action == "TAKE_PROFIT"]
    holds = [a for a in actions if a.action == "HOLD"]

    print(f"\n  Summary: {len(sells)} SELL, {len(profits)} TAKE_PROFIT, {len(holds)} HOLD", flush=True)
    print("=" * 60, flush=True)
    return actions


if __name__ == "__main__":
    monitor()
