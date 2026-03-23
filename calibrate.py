"""
Calibration — compute per-city forecast bias and error from history.

For each city:
1. Fetch 90 days of historical forecasts (D+1)
2. Fetch actual observed temperatures
3. Compute: bias, MAE, RMSE, real std of error
4. Save to calibration.json

This is the FOUNDATION. Without calibration, probabilities are fiction.
"""

import json
import statistics
import time
from datetime import date, timedelta
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

from config import CITIES, OPEN_METEO_HISTORICAL_URL, OPEN_METEO_HISTORICAL_FORECAST_URL
from weather_model import _get_with_retry

# Use only these — remove exotic cities until we have data
CALIBRATION_CITIES = [
    "nyc", "london", "paris", "seoul", "tokyo",
    "chicago", "dallas", "atlanta", "miami",
    "tel_aviv", "hong_kong", "shanghai", "warsaw",
]

DAYS_HISTORY = 90
FORECAST_MODEL = "gfs_seamless"


@dataclass
class CityCalibration:
    city_id: str
    n_days: int
    bias: float          # mean(forecast - actual), positive = model runs hot
    mae: float           # mean absolute error
    rmse: float          # root mean square error
    real_std: float      # std of (forecast - actual) = true uncertainty
    mean_actual: float   # average actual temp (for sanity check)
    mean_forecast: float


def fetch_actuals_batch(city_id: str, start: date, end: date) -> Dict[str, float]:
    """Fetch actual observed temperatures for date range."""
    city = CITIES.get(city_id)
    if not city:
        return {}

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    if city["unit"] == "fahrenheit":
        params["temperature_unit"] = "fahrenheit"

    data = _get_with_retry(OPEN_METEO_HISTORICAL_URL, params)
    if not data:
        return {}

    times = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    return {t: v for t, v in zip(times, temps) if v is not None}


def fetch_forecasts_batch(city_id: str, start: date, end: date, model: str = FORECAST_MODEL) -> Dict[str, float]:
    """
    Fetch historical D+1 forecasts for date range.
    Uses Open-Meteo Historical Forecast API.
    """
    city = CITIES.get(city_id)
    if not city:
        return {}

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "models": model,
    }
    if city["unit"] == "fahrenheit":
        params["temperature_unit"] = "fahrenheit"

    data = _get_with_retry(OPEN_METEO_HISTORICAL_FORECAST_URL, params, timeout=120)
    if not data:
        return {}

    times = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    return {t: v for t, v in zip(times, temps) if v is not None}


def calibrate_city(city_id: str) -> Optional[CityCalibration]:
    """Run calibration for one city."""
    print(f"\n  Calibrating {city_id}...", flush=True)

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=DAYS_HISTORY)

    # Fetch data
    actuals = fetch_actuals_batch(city_id, start, end)
    time.sleep(0.5)
    forecasts = fetch_forecasts_batch(city_id, start, end)
    time.sleep(0.5)

    if not actuals or not forecasts:
        print(f"    No data: actuals={len(actuals)}, forecasts={len(forecasts)}", flush=True)
        return None

    # Match forecast → actual for same dates
    errors = []
    for dt_str, forecast_val in forecasts.items():
        if dt_str in actuals:
            actual_val = actuals[dt_str]
            errors.append({
                "date": dt_str,
                "forecast": forecast_val,
                "actual": actual_val,
                "error": forecast_val - actual_val,
            })

    if len(errors) < 20:
        print(f"    Too few matched days: {len(errors)}", flush=True)
        return None

    error_vals = [e["error"] for e in errors]
    forecast_vals = [e["forecast"] for e in errors]
    actual_vals = [e["actual"] for e in errors]

    bias = statistics.mean(error_vals)
    mae = statistics.mean(abs(e) for e in error_vals)
    rmse = (sum(e**2 for e in error_vals) / len(error_vals)) ** 0.5
    real_std = statistics.stdev(error_vals)

    cal = CityCalibration(
        city_id=city_id,
        n_days=len(errors),
        bias=round(bias, 2),
        mae=round(mae, 2),
        rmse=round(rmse, 2),
        real_std=round(real_std, 2),
        mean_actual=round(statistics.mean(actual_vals), 1),
        mean_forecast=round(statistics.mean(forecast_vals), 1),
    )

    unit = CITIES[city_id]["unit"]
    sym = "°F" if unit == "fahrenheit" else "°C"
    print(f"    Days: {cal.n_days} | Bias: {cal.bias:+.2f}{sym} | MAE: {cal.mae:.2f}{sym} | Std: {cal.real_std:.2f}{sym}", flush=True)

    return cal


def run_calibration():
    """Run full calibration for all cities, save to file."""
    print("=" * 60, flush=True)
    print("CALIBRATION — forecast vs actual (90 days)", flush=True)
    print("=" * 60, flush=True)

    results = {}
    for city_id in CALIBRATION_CITIES:
        if city_id not in CITIES:
            continue
        cal = calibrate_city(city_id)
        if cal:
            results[city_id] = asdict(cal)

    # Save
    output = {
        "generated": date.today().isoformat(),
        "model": FORECAST_MODEL,
        "days": DAYS_HISTORY,
        "cities": results,
    }

    with open("calibration.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'=' * 60}", flush=True)
    print(f"Calibration complete: {len(results)} cities", flush=True)
    print(f"Saved to calibration.json", flush=True)

    # Summary table
    print(f"\n{'City':<12} {'Bias':>7} {'MAE':>7} {'RMSE':>7} {'Std':>7} {'Days':>5}", flush=True)
    print("-" * 50, flush=True)
    for city_id, cal in sorted(results.items()):
        unit = CITIES[city_id]["unit"]
        sym = "°F" if unit == "fahrenheit" else "°C"
        print(f"{city_id:<12} {cal['bias']:>+6.2f}{sym} {cal['mae']:>5.2f}{sym} {cal['rmse']:>5.2f}{sym} {cal['real_std']:>5.2f}{sym} {cal['n_days']:>5d}", flush=True)

    print("=" * 60, flush=True)
    return results


if __name__ == "__main__":
    run_calibration()
