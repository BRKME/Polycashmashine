"""
Edge V2 — Multi-model consensus + forecast shift detection.

Three sources of real edge:

1. MULTI-MODEL CONSENSUS
   GFS and ECMWF both point to same bin → higher confidence than market.
   Market prices are set by humans who may follow only one model.

2. FORECAST SHIFT
   Latest model run shifted significantly from what market prices reflect.
   Market makers update slowly → window of opportunity.

3. ENSEMBLE TIGHTNESS
   When ensemble members are tightly clustered → model is very confident.
   Combined with multi-model agreement → strongest signal.
"""

import json
import time
import re
from datetime import datetime, date, timedelta, timezone
from math import erf, sqrt
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Optional

from config import CITIES, OPEN_METEO_ENSEMBLE_URL
from weather_model import _get_with_retry
from market_scanner import (
    WeatherMarket, EdgeSignal, fetch_weather_markets,
    match_city, parse_date_from_question,
)


def normal_cdf(x, mu, sigma):
    return 0.5 * (1 + erf((x - mu) / (sigma * sqrt(2))))


def load_calibration():
    try:
        with open("calibration.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


@dataclass
class ModelForecast:
    model_name: str
    mean: float
    std: float
    n_members: int


@dataclass
class MultiModelSignal:
    market: WeatherMarket
    bin_label: str
    token_id: str
    # Multi-model data
    gfs_prob: float
    ecmwf_prob: float
    consensus_prob: float      # Average of models
    model_agreement: float     # How much models agree (0-1)
    # Market comparison
    market_price: float
    edge: float                # consensus_prob - market_price
    # Confidence metrics
    ensemble_tightness: float  # Inverse of avg std (higher = tighter)
    signal_strength: str       # "STRONG", "MODERATE", "WEAK"
    bet_side: str


def fetch_multi_model_forecast(city_id: str, target_date: date, cal_data: dict) -> dict:
    """
    Fetch forecasts from BOTH GFS and ECMWF.
    Returns {model_name: ModelForecast}.
    """
    city = CITIES.get(city_id)
    if not city:
        return {}

    days_ahead = (target_date - date.today()).days
    if days_ahead < 0:
        return {}

    results = {}
    for model_name in ["gfs_seamless", "ecmwf_ifs025"]:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": "temperature_2m_max",
            "timezone": city["timezone"],
            "forecast_days": max(days_ahead + 1, 2),
            "models": model_name,
        }
        if city["unit"] == "fahrenheit":
            params["temperature_unit"] = "fahrenheit"

        data = _get_with_retry(OPEN_METEO_ENSEMBLE_URL, params, timeout=60)
        if not data:
            continue

        daily = data.get("daily", {})
        times = daily.get("time", [])
        target_str = target_date.isoformat()

        if target_str not in times:
            continue
        idx = times.index(target_str)

        # Collect ensemble members
        member_values = []
        for key, values in daily.items():
            if key.startswith("temperature_2m_max") and key != "temperature_2m_max":
                if idx < len(values) and values[idx] is not None:
                    member_values.append(values[idx])

        if not member_values:
            main_val = daily.get("temperature_2m_max", [])
            if idx < len(main_val) and main_val[idx] is not None:
                member_values = [main_val[idx]]

        if member_values:
            import statistics
            mean_t = statistics.mean(member_values)
            std_t = statistics.stdev(member_values) if len(member_values) > 1 else 1.0

            # Apply bias correction from calibration
            bias = 0.0
            if cal_data:
                cc = cal_data.get("cities", {}).get(city_id)
                if cc:
                    bias = cc.get("bias", 0.0)

            results[model_name] = ModelForecast(
                model_name=model_name,
                mean=mean_t - bias,
                std=std_t,
                n_members=len(member_values),
            )

        time.sleep(0.3)

    return results


def compute_bin_prob(bin_low, bin_high, mean, cal_std, raw_std):
    """
    Compute bin probability using calibrated std.
    Uses the LARGER of calibrated std and raw ensemble std.
    """
    sigma = max(cal_std, raw_std) if cal_std else raw_std
    sigma = max(sigma, 0.5)  # Floor
    prob = normal_cdf(bin_high, mean, sigma) - normal_cdf(bin_low, mean, sigma)
    return max(prob, 0.001)


def find_multi_model_signals(
    markets: List[WeatherMarket],
    min_edge: float = 8.0,  # Lower threshold — multi-model confidence compensates
) -> List[MultiModelSignal]:
    """
    Find edge using multi-model consensus.

    Signal strength:
    - STRONG: both models agree, edge > 15%, tight ensembles
    - MODERATE: both models agree, edge > 10%
    - WEAK: models diverge or edge < 10%
    """
    cal_data = load_calibration()
    signals = []

    # Group by (city, date), D+1 only
    groups = defaultdict(list)
    tomorrow = date.today() + timedelta(days=1)
    for m in markets:
        if m.target_date == tomorrow:
            groups[(m.city_id, m.target_date)].append(m)

    print(f"\n  Multi-model scan: {len(groups)} city-date pairs (D+1 only)", flush=True)

    for (city_id, target_date), city_markets in groups.items():
        # Fetch BOTH models
        forecasts = fetch_multi_model_forecast(city_id, target_date, cal_data)

        if len(forecasts) < 2:
            print(f"  {city_id}: only {len(forecasts)} model(s), skip", flush=True)
            continue

        gfs = forecasts.get("gfs_seamless")
        ecmwf = forecasts.get("ecmwf_ifs025")

        if not gfs or not ecmwf:
            continue

        # Model agreement: how close are the means?
        city = CITIES[city_id]
        bin_width = city["bin_width"]
        mean_diff = abs(gfs.mean - ecmwf.mean)
        agreement = max(0, 1.0 - mean_diff / (bin_width * 2))

        # Consensus mean (weighted by inverse std — tighter model gets more weight)
        gfs_w = 1.0 / max(gfs.std, 0.3)
        ecmwf_w = 1.0 / max(ecmwf.std, 0.3)
        total_w = gfs_w + ecmwf_w
        consensus_mean = (gfs.mean * gfs_w + ecmwf.mean * ecmwf_w) / total_w
        
        # Use calibrated std for probabilities
        cal_std = None
        if cal_data:
            cc = cal_data.get("cities", {}).get(city_id)
            if cc:
                cal_std = cc.get("real_std")

        avg_raw_std = (gfs.std + ecmwf.std) / 2
        tightness = 1.0 / max(avg_raw_std, 0.3)

        print(f"  {city_id}: GFS={gfs.mean:.1f}±{gfs.std:.1f} ECMWF={ecmwf.mean:.1f}±{ecmwf.std:.1f} agree={agreement:.0%}", flush=True)

        # Evaluate each market/bin
        for market in city_markets:
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

            # Probabilities from each model
            gfs_prob = compute_bin_prob(bin_low, bin_high, gfs.mean, cal_std, gfs.std)
            ecmwf_prob = compute_bin_prob(bin_low, bin_high, ecmwf.mean, cal_std, ecmwf.std)
            consensus_prob = compute_bin_prob(bin_low, bin_high, consensus_mean, cal_std, avg_raw_std)

            # Market price
            yes_price = 0
            for b in market.bins:
                if b.label.lower() in ("yes", "y"):
                    yes_price = b.price
                    break
            if yes_price == 0 and market.bins:
                yes_price = market.bins[0].price

            edge = (consensus_prob - yes_price) * 100

            # Signal strength
            if agreement >= 0.8 and abs(edge) >= 15 and tightness > 1.5:
                strength = "STRONG"
            elif agreement >= 0.6 and abs(edge) >= 10:
                strength = "MODERATE"
            else:
                strength = "WEAK"

            # Filter: YES only, consensus edge, price range, models must agree
            if (edge >= min_edge
                    and consensus_prob >= 0.15
                    and 0.15 <= yes_price <= 0.65
                    and agreement >= 0.5
                    and strength != "WEAK"):

                ev = consensus_prob * (1 - yes_price) - (1 - consensus_prob) * yes_price
                if ev <= 0:
                    continue

                token_id = market.bins[0].token_id if market.bins else ""
                signals.append(MultiModelSignal(
                    market=market,
                    bin_label=f"{int(bin_low)}-{int(bin_high)}",
                    token_id=token_id,
                    gfs_prob=gfs_prob,
                    ecmwf_prob=ecmwf_prob,
                    consensus_prob=consensus_prob,
                    model_agreement=agreement,
                    market_price=yes_price,
                    edge=edge,
                    ensemble_tightness=tightness,
                    signal_strength=strength,
                    bet_side="YES",
                ))

        time.sleep(0.5)

    # Dedup: one per (city, date)
    best = {}
    for s in signals:
        key = (s.market.city_id, s.market.target_date)
        if key not in best or s.edge > best[key].edge:
            best[key] = s

    signals = sorted(best.values(), key=lambda s: s.edge, reverse=True)
    return signals


def print_multi_signals(signals: List[MultiModelSignal]):
    print(f"\n{'═' * 70}", flush=True)
    print(f"MULTI-MODEL SIGNALS ({len(signals)} found)", flush=True)
    print(f"{'═' * 70}", flush=True)

    for s in signals:
        print(f"\n  [{s.signal_strength}] {s.bet_side} {s.bin_label} | Edge: {s.edge:+.1f}%", flush=True)
        print(f"    {s.market.question[:65]}", flush=True)
        print(f"    GFS: {s.gfs_prob:.0%} | ECMWF: {s.ecmwf_prob:.0%} | Consensus: {s.consensus_prob:.0%}", flush=True)
        print(f"    Market: {s.market_price:.0%} | Agreement: {s.model_agreement:.0%} | Tightness: {s.ensemble_tightness:.1f}", flush=True)

    if not signals:
        print("  No signals — models agree with market or disagree with each other.", flush=True)


if __name__ == "__main__":
    print("=" * 70, flush=True)
    print("EDGE V2 — Multi-Model Consensus Scanner", flush=True)
    print(f"{datetime.now(timezone.utc).isoformat()} UTC", flush=True)
    print("=" * 70, flush=True)

    markets = fetch_weather_markets()
    if markets:
        signals = find_multi_model_signals(markets)
        print_multi_signals(signals)
    else:
        print("No markets found.", flush=True)
