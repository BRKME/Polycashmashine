"""
Model vs Market — the ONLY test that matters.

Fetches RESOLVED temperature markets from Polymarket with real prices,
compares model_prob vs market_prob vs actual outcome.

If Brier(model) < Brier(market) → model has edge.
If not → no edge, don't trade.
"""

import json
import re
import time
import statistics
from datetime import date, timedelta
from math import erf, sqrt
from collections import defaultdict

import requests

from config import CITIES, OPEN_METEO_HISTORICAL_FORECAST_URL, GAMMA_API_URL
from weather_model import _get_with_retry

VALIDATE_CITIES = ["london", "paris", "miami", "tel_aviv", "dallas"]
FORECAST_MODEL = "gfs_seamless"


def normal_cdf(x, mu, sigma):
    return 0.5 * (1 + erf((x - mu) / (sigma * sqrt(2))))


def load_calibration():
    try:
        with open("calibration.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def fetch_resolved_markets(n_pages=5):
    """Fetch resolved temperature events with real market prices."""
    print("Fetching resolved temperature markets from Polymarket...", flush=True)
    
    events_url = f"{GAMMA_API_URL}/events"
    all_markets = []

    for offset in range(0, n_pages * 20, 20):
        try:
            params = {
                "limit": 20,
                "offset": offset,
                "closed": "true",
                "tag_slug": "temperature",
            }
            resp = requests.get(events_url, params=params, timeout=60)
            resp.raise_for_status()
            events = resp.json()
            if not events:
                break

            for event in events:
                title = event.get("title", "")
                event_markets = event.get("markets", [])
                for m in event_markets:
                    if not m.get("question"):
                        m["question"] = title
                    m["_event_title"] = title
                    all_markets.append(m)

            print(f"  Page {offset//20 + 1}: {len(events)} events, {len(all_markets)} markets", flush=True)
            if len(events) < 20:
                break
            time.sleep(0.5)
        except Exception as e:
            print(f"  Error: {e}", flush=True)
            break

    print(f"  Total: {len(all_markets)} resolved markets", flush=True)
    return all_markets


def parse_city(question):
    """Match question to city_id."""
    q = question.lower()
    patterns = {
        "nyc": [r"new york"],
        "london": [r"london"],
        "paris": [r"paris"],
        "seoul": [r"seoul"],
        "tokyo": [r"tokyo"],
        "chicago": [r"chicago"],
        "dallas": [r"dallas"],
        "atlanta": [r"atlanta"],
        "miami": [r"miami"],
        "tel_aviv": [r"tel.?aviv"],
        "hong_kong": [r"hong.?kong"],
        "shanghai": [r"shanghai"],
        "warsaw": [r"warsaw"],
    }
    for city_id, pats in patterns.items():
        for p in pats:
            if re.search(p, q):
                return city_id
    return None


def parse_date_from_question(question):
    """Extract date from question text."""
    months = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }
    q = question.lower()
    for month_name, month_num in months.items():
        m = re.search(rf'{month_name}\s+(\d{{1,2}})', q)
        if m:
            day = int(m.group(1))
            year_m = re.search(r'20\d{2}', q)
            year = int(year_m.group()) if year_m else 2026
            try:
                return date(year, month_num, day)
            except ValueError:
                pass
    return None


def parse_temp_from_question(question):
    """Extract temperature bin from question."""
    q = question.lower()
    
    range_m = re.search(r'between\s+(\d+)\s*[-–]\s*(\d+)', q)
    if range_m:
        return float(range_m.group(1)), float(range_m.group(2)) + 0.99
    
    below_m = re.search(r'be\s+(\d+).*or below', q)
    if below_m:
        t = float(below_m.group(1))
        return t - 20, t + 0.99
    
    above_m = re.search(r'be\s+(\d+).*or higher', q)
    if above_m:
        t = float(above_m.group(1))
        return t, t + 20
    
    single_m = re.search(r'be\s+(\d+)\s*[°]', q)
    if single_m:
        t = float(single_m.group(1))
        return t, t + 0.99
    
    return None, None


def get_market_price(market):
    """Get YES price from market data = market-implied probability."""
    outcomes_raw = market.get("outcomes", "[]")
    prices_raw = market.get("outcomePrices", "[]")
    
    if isinstance(outcomes_raw, str):
        outcomes = json.loads(outcomes_raw)
    else:
        outcomes = outcomes_raw or []
    
    if isinstance(prices_raw, str):
        prices = json.loads(prices_raw)
    else:
        prices = prices_raw or []
    
    for i, o in enumerate(outcomes):
        if o.lower() == "yes" and i < len(prices):
            return float(prices[i])
    
    if prices:
        return float(prices[0])
    return None


def get_outcome(market):
    """Check if YES won (1) or NO won (0)."""
    outcomes_raw = market.get("outcomes", "[]")
    prices_raw = market.get("outcomePrices", "[]")
    
    if isinstance(outcomes_raw, str):
        outcomes = json.loads(outcomes_raw)
    else:
        outcomes = outcomes_raw or []
    
    if isinstance(prices_raw, str):
        prices = json.loads(prices_raw)
    else:
        prices = prices_raw or []
    
    # Resolved market: YES price = 1.0 if YES won, 0.0 if NO won
    for i, o in enumerate(outcomes):
        if o.lower() == "yes" and i < len(prices):
            p = float(prices[i])
            if p >= 0.95:
                return 1
            elif p <= 0.05:
                return 0
    return None


def get_forecast_prob(city_id, target_date, bin_low, bin_high, cal_data):
    """Get model probability for a specific bin."""
    city = CITIES.get(city_id)
    if not city:
        return None

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max",
        "timezone": city["timezone"],
        "start_date": (target_date - timedelta(days=1)).isoformat(),
        "end_date": target_date.isoformat(),
        "models": FORECAST_MODEL,
    }
    if city["unit"] == "fahrenheit":
        params["temperature_unit"] = "fahrenheit"

    data = _get_with_retry(OPEN_METEO_HISTORICAL_FORECAST_URL, params, timeout=60)
    if not data:
        return None

    times = data.get("daily", {}).get("time", [])
    temps = data.get("daily", {}).get("temperature_2m_max", [])
    target_str = target_date.isoformat()

    if target_str not in times:
        return None

    idx = times.index(target_str)
    if idx >= len(temps) or temps[idx] is None:
        return None

    forecast_temp = temps[idx]

    # Apply calibration
    bias = 0.0
    cal_std = None
    if cal_data:
        cc = cal_data.get("cities", {}).get(city_id)
        if cc:
            bias = cc.get("bias", 0.0)
            cal_std = cc.get("real_std", None)

    corrected = forecast_temp - bias
    
    if cal_std and cal_std > 0:
        sigma = cal_std
    else:
        sigma = 2.0 if city["unit"] == "fahrenheit" else 1.2

    prob = normal_cdf(bin_high, corrected, sigma) - normal_cdf(bin_low, corrected, sigma)
    return max(prob, 0.001)


def run():
    print("=" * 70, flush=True)
    print("MODEL vs MARKET — Real Polymarket prices", flush=True)
    print("=" * 70, flush=True)

    cal_data = load_calibration()
    markets = fetch_resolved_markets(n_pages=8)

    if not markets:
        print("No resolved markets found.", flush=True)
        return

    # Parse and filter
    valid = []
    for m in markets:
        city_id = parse_city(m.get("question", ""))
        if not city_id or city_id not in VALIDATE_CITIES:
            continue

        target_date = parse_date_from_question(m.get("question", ""))
        if not target_date:
            continue

        # Only last 30 days for relevance
        if (date.today() - target_date).days > 30:
            continue

        bin_low, bin_high = parse_temp_from_question(m.get("question", ""))
        if bin_low is None:
            continue

        market_price = get_market_price(m)
        outcome = get_outcome(m)

        if market_price is None or outcome is None:
            continue

        valid.append({
            "city_id": city_id,
            "date": target_date,
            "bin_low": bin_low,
            "bin_high": bin_high,
            "market_price": market_price,
            "outcome": outcome,
            "question": m.get("question", "")[:60],
        })

    print(f"\n  Valid resolved markets: {len(valid)}", flush=True)

    if not valid:
        print("No valid data for comparison.", flush=True)
        return

    # Get model probabilities (batch by unique city+date)
    forecast_cache = {}
    results_by_city = defaultdict(list)

    for i, v in enumerate(valid):
        cache_key = f"{v['city_id']}_{v['date']}"
        
        model_prob = get_forecast_prob(
            v["city_id"], v["date"], v["bin_low"], v["bin_high"], cal_data
        )
        
        if model_prob is None:
            continue

        entry = {
            **v,
            "model_prob": model_prob,
        }
        results_by_city[v["city_id"]].append(entry)

        if (i + 1) % 20 == 0:
            print(f"  Processed {i+1}/{len(valid)}...", flush=True)
            time.sleep(0.3)

    # Analysis per city
    print(f"\n{'=' * 70}", flush=True)
    print(f"RESULTS — Model vs Market (real prices)", flush=True)
    print(f"{'=' * 70}", flush=True)

    summary = {}
    for city_id in VALIDATE_CITIES:
        entries = results_by_city.get(city_id, [])
        if len(entries) < 10:
            continue

        model_brier = sum((e["model_prob"] - e["outcome"])**2 for e in entries) / len(entries)
        market_brier = sum((e["market_price"] - e["outcome"])**2 for e in entries) / len(entries)
        delta_brier = model_brier - market_brier  # Negative = model wins

        # PnL: bet YES when model > market + threshold
        pnl_trades = []
        for e in entries:
            edge = e["model_prob"] - e["market_price"]
            # Only bet where model has ≥5% edge and price in 20-60¢ range
            if edge >= 0.05 and 0.20 <= e["market_price"] <= 0.60:
                if e["outcome"] == 1:
                    profit = 1.0 - e["market_price"]
                else:
                    profit = -e["market_price"]
                pnl_trades.append({**e, "profit": profit, "edge": edge})

        city = CITIES[city_id]
        sym = "°F" if city["unit"] == "fahrenheit" else "°C"
        
        print(f"\n{'─' * 70}", flush=True)
        print(f"{city_id.upper()} ({len(entries)} resolved bins)", flush=True)
        print(f"  Brier(model):  {model_brier:.4f}", flush=True)
        print(f"  Brier(market): {market_brier:.4f}", flush=True)
        print(f"  ΔBrier:        {delta_brier:+.4f} ({'MODEL WINS' if delta_brier < 0 else 'MARKET WINS'})", flush=True)

        if pnl_trades:
            total_pnl = sum(t["profit"] for t in pnl_trades)
            n_wins = sum(1 for t in pnl_trades if t["outcome"] == 1)
            winrate = n_wins / len(pnl_trades) if pnl_trades else 0
            avg_edge = statistics.mean(t["edge"] for t in pnl_trades)
            print(f"\n  Tradeable signals (model > market + 5%, 20-60¢):", flush=True)
            print(f"    Trades:    {len(pnl_trades)}", flush=True)
            print(f"    Winrate:   {winrate:.1%}", flush=True)
            print(f"    Avg edge:  {avg_edge:.1%}", flush=True)
            print(f"    Total PnL: ${total_pnl:+.2f}", flush=True)
            print(f"    ROI:       {total_pnl/len(pnl_trades)*100:+.1f}%", flush=True)
        else:
            print(f"  No tradeable signals found", flush=True)

        summary[city_id] = {
            "n": len(entries),
            "model_brier": round(model_brier, 4),
            "market_brier": round(market_brier, 4),
            "delta_brier": round(delta_brier, 4),
            "model_wins": delta_brier < 0,
            "n_trades": len(pnl_trades),
            "pnl": round(sum(t["profit"] for t in pnl_trades), 2) if pnl_trades else 0,
        }

    # Final summary
    print(f"\n{'=' * 70}", flush=True)
    print(f"{'City':<12} {'Brier(M)':>10} {'Brier(Mkt)':>11} {'ΔBrier':>9} {'Winner':>8} {'PnL':>8} {'Trades':>7}", flush=True)
    print("-" * 70, flush=True)
    for city_id, s in sorted(summary.items()):
        winner = "MODEL" if s["model_wins"] else "MARKET"
        print(f"{city_id:<12} {s['model_brier']:>10.4f} {s['market_brier']:>11.4f} {s['delta_brier']:>+9.4f} {winner:>8} ${s['pnl']:>+7.2f} {s['n_trades']:>7}", flush=True)

    print(f"\nΔBrier < 0 = model is better calibrated than market", flush=True)
    print(f"Positive PnL = profitable vs real market prices", flush=True)
    print("=" * 70, flush=True)

    with open("validation_vs_market.json", "w") as f:
        json.dump(summary, f, indent=2)


if __name__ == "__main__":
    run()
