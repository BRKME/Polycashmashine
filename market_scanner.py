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
from datetime import datetime, date, timedelta, timezone
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
    "nyc": [r"new york", r"\bnyc\b", r"laguardia", r"\bn\.?y\.?\b"],
    "tel_aviv": [r"tel.?aviv"],
    "seoul": [r"seoul", r"incheon"],
    "london": [r"london", r"heathrow"],
    "shanghai": [r"shanghai", r"pudong"],
    "hong_kong": [r"hong.?kong"],
    "warsaw": [r"warsaw"],
    "tokyo": [r"tokyo", r"narita", r"haneda"],
    "la": [r"los angeles", r"\bla\b.*temp", r"\blax\b"],
    "chicago": [r"chicago", r"o.?hare"],
    "miami": [r"miami"],
    "paris": [r"paris", r"orly"],
    "dallas": [r"dallas", r"\bdfw\b"],
    "atlanta": [r"atlanta"],
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
    Uses tag_slug=temperature — confirmed working via diagnostic.
    """
    print("Fetching weather markets from Polymarket...")

    events_url = f"{GAMMA_API_URL}/events"
    data = []

    # Fetch temperature events in batches to find active (non-closed) markets
    # Today's may be closed, so we need enough to reach tomorrow's
    for offset in range(0, 100, 20):
        try:
            params = {
                "limit": 20,
                "offset": offset,
                "active": "true",
                "closed": "false",
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
                    data.append(m)

            print(f"  Page {offset//20 + 1}: {len(events)} events, {len(data)} markets", flush=True)

            if len(events) < 20:
                break
            time.sleep(0.5)
        except Exception as e:
            print(f"  Error at offset {offset}: {e}", flush=True)
            break

    print(f"  Total: {len(data)} temperature markets found")

    # Debug: sample
    if data:
        print(f"  Sample questions:")
        for m in data[:5]:
            print(f"    {m.get('question', 'N/A')[:75]}")

    markets = []
    unmatched_city = 0
    unmatched_date = 0
    past_dates = 0
    unmatched_samples = []

    for m in data:
        question = m.get("question", "")
        city_id = match_city(question)
        if not city_id:
            unmatched_city += 1
            if len(unmatched_samples) < 10:
                unmatched_samples.append(question[:80])
            continue

        target_date = parse_date_from_question(question)
        if not target_date:
            unmatched_date += 1
            continue

        # Skip markets that already resolved
        if target_date < date.today():
            past_dates += 1
            continue

        # Skip closed markets (endDate passed, no more trading)
        is_closed = m.get("closed")
        if isinstance(is_closed, str):
            is_closed = is_closed.lower() == "true"
        if is_closed:
            past_dates += 1
            continue

        # Skip markets closing within 2 hours (orderbook may be gone)
        end_date_str = m.get("endDate", "")
        if end_date_str:
            try:
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                hours_left = (end_dt - now).total_seconds() / 3600
                if hours_left < 2:
                    past_dates += 1
                    continue
            except (ValueError, TypeError):
                pass

        # Parse outcomes (can be JSON string)
        outcomes = m.get("outcomes", [])
        prices = m.get("outcomePrices", [])
        tokens = m.get("clobTokenIds", [])

        try:
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
        except (ValueError, json.JSONDecodeError):
            outcomes = []

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

    print(f"\n  Filtering summary:")
    print(f"    Unmatched city: {unmatched_city}")
    if unmatched_samples:
        print(f"    Unmatched examples:")
        for s in unmatched_samples[:5]:
            print(f"      '{s}'")
    print(f"    Unmatched date: {unmatched_date}")
    print(f"    Past dates: {past_dates}")
    print(f"  Matched {len(markets)} markets to supported cities")
    return markets


def find_edge_signals(
    markets: List[WeatherMarket],
    min_edge: float = MIN_EDGE_PERCENT,
    model: str = "gfs_seamless",
) -> List[EdgeSignal]:
    """
    Compare model forecasts with real market prices.
    
    KEY INSIGHT: Each Polymarket temperature "market" is a single YES/NO question
    like "Will the highest temperature in London be 14°C on March 20?"
    The YES price IS the market's implied probability for that bin.
    
    We group markets by (city, date), fetch ONE forecast per group,
    then compare each bin's YES price to model probability.
    """
    signals = []

    # Group markets by (city_id, target_date) to avoid redundant API calls
    from collections import defaultdict
    groups = defaultdict(list)
    for market in markets:
        key = (market.city_id, market.target_date)
        groups[key].append(market)

    print(f"\n  Grouped into {len(groups)} (city, date) pairs")

    # Cache forecasts
    forecast_cache = {}

    for (city_id, target_date), group_markets in groups.items():
        # Fetch ONE ensemble forecast per (city, date)
        cache_key = f"{city_id}_{target_date}"
        if cache_key not in forecast_cache:
            print(f"\n  Fetching forecast: {city_id} {target_date} ({len(group_markets)} bins)")
            forecast = fetch_ensemble_forecast(city_id, target_date, model=model)
            forecast_cache[cache_key] = forecast
            if forecast:
                print(f"  Model: mean={forecast.ensemble_mean:.1f}, std={forecast.ensemble_std:.1f}, members={forecast.total_members}")
            else:
                print(f"  ⚠️ No forecast available")
                continue
            time.sleep(0.5)

        forecast = forecast_cache[cache_key]
        if not forecast:
            continue

        # Each market in this group is one temperature bin (YES/NO question)
        # Extract the temperature from the question and match to model
        for market in group_markets:
            # Parse the temperature value from the question
            # "Will the highest temperature in London be 14°C on March 20?" → 14
            question = market.question.lower()
            temp_match = re.search(r'be (?:between )?(\d+)(?:[°\s]|$)', question)
            if not temp_match:
                continue
            
            bin_temp = float(temp_match.group(1))
            
            # Determine if it's a range ("between 44-45°F") or single value ("be 14°C")
            range_match = re.search(r'between\s+(\d+)\s*[-–]\s*(\d+)', question)
            is_below = "or below" in question or "or lower" in question
            is_above = "or higher" in question or "or above" in question
            
            if range_match:
                bin_low = float(range_match.group(1))
                bin_high = float(range_match.group(2)) + 0.99
            elif is_below:
                bin_low = bin_temp - 20
                bin_high = bin_temp + 0.99
            elif is_above:
                bin_low = bin_temp
                bin_high = bin_temp + 20
            else:
                # Single degree: "be 14°C" means 14.0-14.99
                bin_low = bin_temp
                bin_high = bin_temp + 0.99

            # Calculate model probability for this bin
            model_prob = 0
            model_idx = -1
            for j, fb in enumerate(forecast.bins):
                if fb.bin_low <= bin_high and fb.bin_high >= bin_low:
                    model_prob += fb.probability
                    if model_idx == -1:
                        model_idx = j

            # Cluster probability (this bin + neighbors)
            cluster_prob = model_prob
            if model_idx >= 0:
                if model_idx > 0:
                    cluster_prob += forecast.bins[model_idx - 1].probability
                if model_idx < len(forecast.bins) - 1:
                    cluster_prob += forecast.bins[model_idx + 1].probability

            # YES price = market's implied probability for this bin
            # For YES/NO markets, the YES price is in the first bin
            yes_price = 0
            for b in market.bins:
                if b.label.lower() in ("yes", "y"):
                    yes_price = b.price
                    break
            # Fallback: try first bin price
            if yes_price == 0 and market.bins:
                yes_price = market.bins[0].price

            edge = (model_prob - yes_price) * 100

            # === YES signal: model more confident than market ===
            # Require market_price >= 10¢ to avoid low-liquidity tail bets
            if (edge >= min_edge and model_prob >= 0.25 
                    and cluster_prob >= 0.55 and yes_price >= 0.10):
                ev = model_prob * (1 - yes_price) - (1 - model_prob) * yes_price
                token_id = market.bins[0].token_id if market.bins else ""
                signals.append(EdgeSignal(
                    market=market,
                    bin_label=f"{int(bin_low)}-{int(bin_high)}",
                    token_id=token_id,
                    model_prob=model_prob,
                    market_price=yes_price,
                    edge=edge,
                    cluster_prob=cluster_prob,
                    bet_side="YES",
                    expected_value=ev,
                ))

            # === NO signal: market overprices, model says unlikely ===
            elif yes_price > 0.08 and model_prob < 0.02:
                ev = (1 - model_prob) * yes_price - model_prob * (1 - yes_price)
                token_id = market.bins[1].token_id if len(market.bins) > 1 else ""
                signals.append(EdgeSignal(
                    market=market,
                    bin_label=f"{int(bin_low)}-{int(bin_high)}",
                    token_id=token_id,
                    model_prob=model_prob,
                    market_price=yes_price,
                    edge=edge,
                    cluster_prob=cluster_prob,
                    bet_side="NO",
                    expected_value=ev,
                ))

    # === Dedup: only ONE signal per (city, date) ===
    # YES + NO on same city/date can BOTH lose — pick only the best one
    best_per_city = {}
    for s in signals:
        key = (s.market.city_id, s.market.target_date)
        if key not in best_per_city or s.expected_value > best_per_city[key].expected_value:
            best_per_city[key] = s

    removed = len(signals) - len(best_per_city)
    if removed > 0:
        print(f"\n  Dedup: 1 best signal per (city,date), removed {removed}")

    signals = list(best_per_city.values())

    # Sort by EV (best first)
    signals.sort(key=lambda s: s.expected_value, reverse=True)
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
        # Show sample — group by (city, date)
        from collections import defaultdict
        groups = defaultdict(list)
        for m in markets:
            groups[(m.city_id, m.target_date)].append(m)
        
        print(f"\n  {len(groups)} city-date pairs:")
        for (city, dt), mlist in list(groups.items())[:5]:
            print(f"    {city} {dt}: {len(mlist)} bins")

        signals = find_edge_signals(markets)
        print_signals(signals)
        save_signals(signals)
