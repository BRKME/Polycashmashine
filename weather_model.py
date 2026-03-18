"""
Weather Model — Fetch ensemble forecasts and calculate temperature bin probabilities.

Uses Open-Meteo Ensemble API (GFS 31 members + ECMWF IFS 51 members).
For each forecast day, counts how many ensemble members predict
the daily max temperature falling into each temperature bin.

This is the core edge: ensemble probability vs Polymarket market price.
"""

import requests
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from config import (
    CITIES, ENSEMBLE_MODELS,
    OPEN_METEO_ENSEMBLE_URL, OPEN_METEO_HISTORICAL_URL,
    OPEN_METEO_HISTORICAL_FORECAST_URL,
)


@dataclass
class BinProbability:
    """Probability that max temperature falls in a specific bin."""
    bin_low: float       # e.g., 36.0
    bin_high: float      # e.g., 37.99
    label: str           # e.g., "36-37°F"
    probability: float   # 0.0 to 1.0 (from ensemble members)
    member_count: int    # how many ensemble members fall in this bin
    total_members: int   # total ensemble members


@dataclass
class ForecastResult:
    """Full forecast for one city on one date."""
    city_id: str
    target_date: date
    forecast_time: datetime          # when the forecast was made
    bins: List[BinProbability]
    ensemble_mean: float             # mean of all members
    ensemble_std: float              # spread — uncertainty indicator
    total_members: int
    model: str


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9 / 5 + 32


def make_temperature_bins(
    center: float, unit: str, bin_width: float, n_bins: int = 15
) -> List[Tuple[float, float, str]]:
    """
    Generate temperature bins centered around an expected value.
    Returns list of (low, high, label) tuples.
    """
    symbol = "°F" if unit == "fahrenheit" else "°C"
    # Start from well below center
    start = center - (n_bins // 2) * bin_width
    # Align to even numbers for clean bins
    start = int(start // bin_width) * bin_width

    bins = []
    for i in range(n_bins):
        low = start + i * bin_width
        high = low + bin_width - 0.01  # e.g., 36.0 - 37.99
        label = f"{int(low)}-{int(low + bin_width - 1)}{symbol}"
        bins.append((low, high, label))
    return bins


def fetch_ensemble_forecast(
    city_id: str,
    target_date: Optional[date] = None,
    model: str = "gfs_seamless",
) -> Optional[ForecastResult]:
    """
    Fetch ensemble forecast for a city.
    Returns temperature probabilities per bin for target_date.
    """
    city = CITIES.get(city_id)
    if not city:
        return None

    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    # Calculate forecast days needed
    days_ahead = (target_date - date.today()).days
    if days_ahead < 0:
        return None  # Can't forecast the past via live API

    forecast_days = max(days_ahead + 1, 2)

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "forecast_days": forecast_days,
        "models": model,
    }

    # Temperature unit
    if city["unit"] == "fahrenheit":
        params["temperature_unit"] = "fahrenheit"

    try:
        resp = requests.get(OPEN_METEO_ENSEMBLE_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching ensemble for {city_id}: {e}")
        return None

    # Parse ensemble members
    daily = data.get("daily", {})
    times = daily.get("time", [])

    # Find target date index
    target_str = target_date.isoformat()
    if target_str not in times:
        print(f"  Target date {target_str} not in forecast range")
        return None

    idx = times.index(target_str)

    # Collect all ensemble member values for this date
    member_values = []
    for key, values in daily.items():
        if key.startswith("temperature_2m_max") and key != "temperature_2m_max":
            # Keys like temperature_2m_max_member01, etc.
            if idx < len(values) and values[idx] is not None:
                member_values.append(values[idx])

    # If no member keys, try the main key (non-ensemble)
    if not member_values:
        main_val = daily.get("temperature_2m_max", [])
        if idx < len(main_val) and main_val[idx] is not None:
            member_values = [main_val[idx]]

    if not member_values:
        print(f"  No ensemble data for {city_id} on {target_date}")
        return None

    # Calculate ensemble statistics
    import statistics
    mean_temp = statistics.mean(member_values)
    std_temp = statistics.stdev(member_values) if len(member_values) > 1 else 0

    # Build temperature bins
    bins_def = make_temperature_bins(
        center=mean_temp,
        unit=city["unit"],
        bin_width=city["bin_width"],
    )

    # Count members per bin
    total = len(member_values)
    bin_results = []
    for low, high, label in bins_def:
        count = sum(1 for v in member_values if low <= v <= high)
        prob = count / total if total > 0 else 0
        bin_results.append(BinProbability(
            bin_low=low,
            bin_high=high,
            label=label,
            probability=prob,
            member_count=count,
            total_members=total,
        ))

    return ForecastResult(
        city_id=city_id,
        target_date=target_date,
        forecast_time=datetime.utcnow(),
        bins=bin_results,
        ensemble_mean=mean_temp,
        ensemble_std=std_temp,
        total_members=total,
        model=model,
    )


def fetch_actual_temperature(
    city_id: str,
    target_date: date,
) -> Optional[float]:
    """
    Fetch actual observed max temperature for a past date.
    Uses Open-Meteo Historical Weather API.
    """
    city = CITIES.get(city_id)
    if not city:
        return None

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
    }
    if city["unit"] == "fahrenheit":
        params["temperature_unit"] = "fahrenheit"

    try:
        resp = requests.get(OPEN_METEO_HISTORICAL_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching actual temp for {city_id}: {e}")
        return None

    temps = data.get("daily", {}).get("temperature_2m_max", [])
    if temps and temps[0] is not None:
        return temps[0]
    return None


def fetch_historical_forecast(
    city_id: str,
    target_date: date,
    model: str = "ecmwf_ifs025",
) -> Optional[ForecastResult]:
    """
    Fetch what the ensemble model predicted for a PAST date.
    Uses Open-Meteo Historical Forecast API.
    This is the key data source for backtesting.
    """
    city = CITIES.get(city_id)
    if not city:
        return None

    # Fetch the forecast that was available 1 day before target
    forecast_date = target_date - timedelta(days=1)

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "start_date": forecast_date.isoformat(),
        "end_date": target_date.isoformat(),
        "models": model,
    }
    if city["unit"] == "fahrenheit":
        params["temperature_unit"] = "fahrenheit"

    try:
        resp = requests.get(
            OPEN_METEO_HISTORICAL_FORECAST_URL, params=params, timeout=60
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching historical forecast for {city_id}: {e}")
        return None

    daily = data.get("daily", {})
    times = daily.get("time", [])
    target_str = target_date.isoformat()

    if target_str not in times:
        return None

    idx = times.index(target_str)

    # Historical forecast API returns deterministic (single value), not ensemble
    # We use this as "best estimate" and simulate ensemble spread from historical std
    temp_val = daily.get("temperature_2m_max", [])
    if not temp_val or idx >= len(temp_val) or temp_val[idx] is None:
        return None

    forecast_temp = temp_val[idx]

    # For historical backtest, we create a synthetic ensemble
    # using the forecast value + typical model uncertainty
    # Typical GFS/ECMWF 24h temperature forecast std: ~1.5-2.5°F / ~1-1.5°C
    import random
    random.seed(int(target_date.toordinal()) + hash(city_id))
    
    if city["unit"] == "fahrenheit":
        std = 2.0  # °F typical 24h uncertainty
    else:
        std = 1.2  # °C typical 24h uncertainty

    n_members = 51
    member_values = [random.gauss(forecast_temp, std) for _ in range(n_members)]

    import statistics
    mean_temp = statistics.mean(member_values)
    std_temp = statistics.stdev(member_values)

    bins_def = make_temperature_bins(
        center=mean_temp,
        unit=city["unit"],
        bin_width=city["bin_width"],
    )

    total = len(member_values)
    bin_results = []
    for low, high, label in bins_def:
        count = sum(1 for v in member_values if low <= v <= high)
        prob = count / total if total > 0 else 0
        bin_results.append(BinProbability(
            bin_low=low, bin_high=high, label=label,
            probability=prob, member_count=count, total_members=total,
        ))

    return ForecastResult(
        city_id=city_id,
        target_date=target_date,
        forecast_time=datetime.combine(forecast_date, datetime.min.time()),
        bins=bin_results,
        ensemble_mean=mean_temp,
        ensemble_std=std_temp,
        total_members=total,
        model=model,
    )


if __name__ == "__main__":
    # Quick test: tomorrow's forecast for NYC
    print("Fetching ensemble forecast for NYC (tomorrow)...")
    result = fetch_ensemble_forecast("nyc", model="gfs_seamless")
    if result:
        print(f"\nCity: {result.city_id}")
        print(f"Date: {result.target_date}")
        print(f"Model: {result.model}")
        print(f"Members: {result.total_members}")
        print(f"Mean: {result.ensemble_mean:.1f}°F")
        print(f"Std: {result.ensemble_std:.1f}°F")
        print(f"\nBin probabilities:")
        for b in result.bins:
            if b.probability > 0:
                bar = "█" * int(b.probability * 40)
                print(f"  {b.label:>10s}  {b.probability:5.1%}  {bar}  ({b.member_count}/{b.total_members})")
