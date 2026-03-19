"""
Market Scanner — Fetch real Polymarket weather market prices and find edge.

Connects ensemble forecast probabilities to actual Polymarket prices.
This is the real validation: model vs live market, not simulation.

Workflow:
1. Fetch active weather markets from Gamma API
2. Match to our supported cities
3. Get ensemble forecast for each market's date
4. Compare model probability vs market price per bin
5. Report edge opportunities
"""

import re
import json
import time
import requests
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

from config import CITIES, GAMMA_API_URL, MIN_EDGE_PERCENT
from weather_model import fetch_ensemble_forecast, BinProbability, ForecastResult


@dataclass
class MarketBin:
    """One outcome/bin in a Polymarket weather market."""
    token_id: str
    label: str          # e.g., "36-37°F" or "36°F-37°F"
    price: float        # current market price (0..1)
    bin_low: float      # parsed lower bound
    bin_high: float     # parsed upper bound


@dataclass
class WeatherMarket:
    """A Polymarket weather market with parsed bins."""
    condition_id: str
    question: str
    slug: str
    city_id: str        # matched city from our config
    target_date: date
    end_date: str
    bins: List[MarketBin]
    volume: float


@dataclass
class EdgeSignal:
    """An edge opportunity: model prob vs market price."""
    market: WeatherMarket
    bin_label: str
    token_id: str
    model_prob: float
    market_price: float
    edge: float          # model_prob - market_price (positive = underpriced YES)
    cluster_prob: float  # 3-bin cluster probability
    bet_side: str        # "YES" or "NO"
    expected_value: float  # EV per $1 bet


# City name patterns for matching market questions to our config
CITY_PATTERNS = {
    "nyc": [r"new york", r"\bnyc\b", r"laguardia"],
    "tel_aviv": [r"tel aviv"],
    "seoul": [r"seoul"],
    "london": [r"london", r"heathrow"],
    "shanghai": [r"shanghai"],
}


def parse_temperature_range(label: str) -> Optional[Tuple[float, float]]:
    """
    Parse temperature range from Polymarket outcome label.
    Examples: "36-37°F", "36°F-37°F", "36-37", "< 30°F", ">= 60°F"
    """
    # Pattern: "36-37°F" or "36°F-37°F"
    m = re.search(r'(\d+)\s*°?[FC]?\s*[-–]\s*(\d+)', label)
    if m:
        return float(m.group(1)), float(m.group(2))

    # Pattern: "< 30°F" (below range)
    m = re.search(r'[<≤]\s*(\d+)', label)
    if m:
        val = float(m.group(1))
        return val - 10, val - 0.01  # Approximate lower bound

    # Pattern: ">= 60°F" or "> 60°F" (above range)
    m = re.search(r'[>≥]\s*(\d+)', label)
    if m:
        val = float(m.group(1))
        return val, val + 10  # Approximate upper bound

    return None


def parse_date_from_question(question: str) -> Optional[date]:
    """
    Extract target date from market question.
    E.g., "Highest temperature in NYC on March 20?" → 2026-03-20
    """
    # Pattern: "on March 20" / "on March 20, 2026"
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
    }

    for month_name, month_num in months.items():
        pattern = rf'{month_name}\s+(\d{{1,2}})(?:\s*,?\s*(\d{{4}}))?'
        m = re.search(pattern, question.lower())
        if m:
            day = int(m.group(1))
            year = int(m.group(2)) if m.group(2) else date.today().year
            try:
                return date(year, month_num, day)
            except ValueError:
                continue

    return None


def match_city(question: str) -> Optional[str]:
    """Match a market question to one of our configured cities."""
    q_lower = question.lower()
    for city_id, patterns in CITY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, q_lower):
                return city_id
    return None


def fetch_weather_markets() -> List[WeatherMarket]:
    """
    Fetch active weather/temperature markets from Polymarket Gamma API.
    Returns parsed markets with bin prices.
    """
    print("Fetching weather markets from Polymarket...")

    # Search for temperature markets
    url = f"{GAMMA_API_URL}/markets"
    params = {
        "limit": 100,
        "active": "true",
        "closed": "false",
        "tag": "temperature",
    }

    markets = []

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  Error fetching markets: {e}")
        # Fallback: search by keyword
        try:
            params2 = {
                "limit": 200,
                "active": "true",
                "closed": "false",
            }
            resp = requests.get(url, params=params2, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Filter by weather keywords
            data = [m for m in data if any(
                kw in m.get("question", "").lower()
                for kw in ["temperature", "highest temp", "lowest temp"]
            )]
        except Exception as e2:
            print(f"  Fallback search also failed: {e2}")
            return []

    print(f"  Found {len(data)} temperature-related markets")

    for m in data:
        question = m.get("question", "")
        city_id = match_city(question)
        if not city_id:
            continue

        target_date = parse_date_from_question(question)
        if not target_date:
            continue

        # Skip markets that already resolved
        if target_date < date.today():
            continue

        # Parse outcomes/bins
        outcomes = m.get("outcomes", [])
        prices = m.get("outcomePrices", [])
        tokens = m.get("clobTokenIds", [])

        if not outcomes or not prices:
            continue

        # Parse prices (can be string or float)
        try:
            if isinstance(prices, str):
                prices = json.loads(prices)
            prices = [float(p) for p in prices]
        except (ValueError, json.JSONDecodeError):
            continue

        # Parse token IDs
        try:
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
        except (ValueError, json.JSONDecodeError):
            tokens = [""] * len(outcomes)

        bins = []
        for i, outcome_label in enumerate(outcomes):
            price = prices[i] if i < len(prices) else 0
            token_id = tokens[i] if i < len(tokens) else ""

            temp_range = parse_temperature_range(outcome_label)
            if temp_range:
                bin_low, bin_high = temp_range
            else:
                bin_low, bin_high = 0, 0

            bins.append(MarketBin(
                token_id=token_id,
                label=outcome_label,
                price=price,
                bin_low=bin_low,
                bin_high=bin_high,
            ))

        if not bins:
            continue

        markets.append(WeatherMarket(
            condition_id=m.get("conditionId", ""),
            question=question,
            slug=m.get("slug", ""),
            city_id=city_id,
            target_date=target_date,
            end_date=m.get("endDate", ""),
            bins=bins,
            volume=float(m.get("volume", 0) or 0),
        ))

    print(f"  Matched {len(markets)} markets to supported cities")
    return markets


def find_edge_signals(
    markets: List[WeatherMarket],
    min_edge: float = MIN_EDGE_PERCENT,
    model: str = "gfs_seamless",
) -> List[EdgeSignal]:
    """
    Compare model forecasts with real market prices.
    Returns list of edge opportunities sorted by edge size.
    """
    signals = []

    for market in markets:
        print(f"\n  Analyzing: {market.question[:70]}")
        print(f"  City: {market.city_id}, Date: {market.target_date}")

        # Get ensemble forecast
        forecast = fetch_ensemble_forecast(
            market.city_id,
            market.target_date,
            model=model,
        )
        if not forecast:
            print(f"  ⚠️ Could not get forecast, skipping")
            continue

        print(f"  Model: mean={forecast.ensemble_mean:.1f}, std={forecast.ensemble_std:.1f}, members={forecast.total_members}")

        # Match market bins to model bins
        for mb in market.bins:
            if mb.bin_low == 0 and mb.bin_high == 0:
                continue  # Unparsed bin

            # Find matching model bin
            model_prob = 0
            model_idx = -1
            for j, fb in enumerate(forecast.bins):
                # Check if bins overlap
                if fb.bin_low <= mb.bin_high and fb.bin_high >= mb.bin_low:
                    model_prob += fb.probability
                    if model_idx == -1:
                        model_idx = j

            # Calculate cluster probability (3-bin)
            cluster_prob = model_prob
            if model_idx >= 0:
                if model_idx > 0:
                    cluster_prob += forecast.bins[model_idx - 1].probability
                if model_idx < len(forecast.bins) - 1:
                    cluster_prob += forecast.bins[model_idx + 1].probability

            edge = (model_prob - mb.price) * 100

            # === YES signal: model > market ===
            if edge >= min_edge and model_prob >= 0.25 and cluster_prob >= 0.55:
                ev = model_prob * (1 - mb.price) - (1 - model_prob) * mb.price
                signals.append(EdgeSignal(
                    market=market,
                    bin_label=mb.label,
                    token_id=mb.token_id,
                    model_prob=model_prob,
                    market_price=mb.price,
                    edge=edge,
                    cluster_prob=cluster_prob,
                    bet_side="YES",
                    expected_value=ev,
                ))

            # === NO signal: market overprices, model disagrees ===
            elif mb.price > 0.08 and model_prob < 0.02:
                ev = (1 - model_prob) * mb.price - model_prob * (1 - mb.price)
                signals.append(EdgeSignal(
                    market=market,
                    bin_label=mb.label,
                    token_id=mb.token_id,
                    model_prob=model_prob,
                    market_price=mb.price,
                    edge=edge,
                    cluster_prob=cluster_prob,
                    bet_side="NO",
                    expected_value=ev,
                ))

        time.sleep(0.5)

    # Sort by absolute edge
    signals.sort(key=lambda s: abs(s.edge), reverse=True)
    return signals


def print_signals(signals: List[EdgeSignal]) -> None:
    """Print edge signals in a readable format."""
    print(f"\n{'═' * 60}")
    print(f"LIVE EDGE SIGNALS ({len(signals)} found)")
    print(f"{'═' * 60}")

    if not signals:
        print("No edge signals found — market is efficient today.")
        return

    for s in signals:
        emoji = "🟢" if s.bet_side == "YES" else "🔴"
        print(f"\n{emoji} {s.bet_side} {s.bin_label} | Edge: {s.edge:+.1f}%")
        print(f"   Market: {s.market.question[:65]}")
        print(f"   Model: {s.model_prob:.0%} | Market: {s.market_price:.0%} | Cluster: {s.cluster_prob:.0%}")
        print(f"   EV: {s.expected_value:+.3f} per $1 | Token: {s.token_id[:20]}...")
        print(f"   Date: {s.market.target_date}")

    # Summary
    yes_signals = [s for s in signals if s.bet_side == "YES"]
    no_signals = [s for s in signals if s.bet_side == "NO"]
    print(f"\n{'─' * 60}")
    print(f"Summary: {len(yes_signals)} YES signals, {len(no_signals)} NO signals")
    if yes_signals:
        avg_edge = sum(s.edge for s in yes_signals) / len(yes_signals)
        avg_ev = sum(s.expected_value for s in yes_signals) / len(yes_signals)
        print(f"  YES avg edge: {avg_edge:+.1f}%, avg EV: {avg_ev:+.3f}")
    if no_signals:
        avg_ev = sum(s.expected_value for s in no_signals) / len(no_signals)
        print(f"  NO avg EV: {avg_ev:+.3f}")


def save_signals(signals: List[EdgeSignal], path: str = "live_signals.json") -> None:
    """Save signals to JSON for later processing."""
    data = []
    for s in signals:
        data.append({
            "market_question": s.market.question,
            "city": s.market.city_id,
            "target_date": s.market.target_date.isoformat(),
            "condition_id": s.market.condition_id,
            "token_id": s.token_id,
            "bin_label": s.bin_label,
            "bet_side": s.bet_side,
            "model_prob": s.model_prob,
            "market_price": s.market_price,
            "edge": s.edge,
            "cluster_prob": s.cluster_prob,
            "expected_value": s.expected_value,
            "scanned_at": datetime.utcnow().isoformat(),
        })
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nSignals saved to {path}")


if __name__ == "__main__":
    markets = fetch_weather_markets()

    if not markets:
        print("\nNo weather markets found. Try running during market hours.")
    else:
        for m in markets[:5]:
            print(f"\n  {m.question[:70]}")
            print(f"  City: {m.city_id}, Date: {m.target_date}, Bins: {len(m.bins)}")
            for b in m.bins[:3]:
                print(f"    {b.label}: {b.price:.0%}")

        signals = find_edge_signals(markets)
        print_signals(signals)
        save_signals(signals)
