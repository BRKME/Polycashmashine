"""
Esports Price Collector — structural mispricing test.

Goal: Is Pinnacle (vig-adjusted) systematically more accurate than Polymarket
on esports markets? NOT latency arbitrage — calibration comparison.

Every 30 min:
1. Fetch esports matches from Polymarket (Gamma API)
2. Fetch Pinnacle odds from OddsPapi (free tier)
3. Fuzzy match teams
4. Record: Pinnacle vig-adjusted prob, Polymarket bid/ask/depth, timestamp
5. After match resolves: compare who was closer

Fields per observation (per expert requirements):
- match_id (strict, from Polymarket conditionId)
- t_snapshot (ISO timestamp)
- pinnacle_raw_odds (decimal)
- pinnacle_vig_adjusted_prob (devigged)
- polymarket_yes_price (last trade or mid)
- polymarket_bid / polymarket_ask / polymarket_spread
- polymarket_bid_depth / polymarket_ask_depth ($ at best level)
- game_title (cs2, lol, dota2, valorant)
- team_a / team_b
- tournament
- match_start_time
- hours_to_start (for T-24h/T-6h/T-1h bucketing)
- outcome (filled post-resolution: 1=team_a won, 0=team_b won)

Run: python esports_collector.py
Env: ODDSPAPI_KEY (API key from the-odds-api.com)
"""

import os
import json
import re
import time
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Polymarket
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# The Odds API (the-odds-api.com)
ODDS_API_KEY = os.getenv("ODDSPAPI_KEY", "")  # Same secret name for simplicity
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Esport sport keys (will be discovered dynamically)
ESPORT_SPORTS = []  # Populated at runtime

HISTORY_FILE = "esports_price_history.json"


def load_history():
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"snapshots": [], "matches": {}}


def save_history(history):
    if len(history["snapshots"]) > 5000:
        history["snapshots"] = history["snapshots"][-5000:]
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def devig_odds(odds_a, odds_b):
    """Remove vig from decimal odds → fair probabilities."""
    impl_a = 1.0 / odds_a
    impl_b = 1.0 / odds_b
    total = impl_a + impl_b  # > 1.0 due to vig
    return impl_a / total, impl_b / total


def fetch_polymarket_esports():
    """Fetch active esports markets from Polymarket."""
    print("  Fetching Polymarket esports...", flush=True)
    markets = []

    for tag in ["esports", "counter-strike", "league-of-legends", "dota-2", "valorant"]:
        try:
            resp = requests.get(f"{GAMMA_API}/events", params={
                "limit": 50,
                "active": "true",
                "closed": "false",
                "tag_slug": tag,
            }, timeout=30)
            if resp.status_code == 200:
                events = resp.json()
                for event in events:
                    event_markets = event.get("markets", [])
                    for m in event_markets:
                        m["_event_title"] = event.get("title", "")
                        m["_tag"] = tag
                        markets.append(m)
            time.sleep(0.3)
        except Exception as e:
            print(f"    Tag {tag} error: {e}", flush=True)

    # Deduplicate by conditionId
    seen = set()
    unique = []
    for m in markets:
        cid = m.get("conditionId", "")
        if cid and cid not in seen:
            seen.add(cid)
            unique.append(m)

    print(f"  Polymarket: {len(unique)} unique esports markets", flush=True)
    return unique


def fetch_polymarket_orderbook(token_id):
    """Fetch bid/ask/depth for a specific token."""
    try:
        resp = requests.get(f"{CLOB_API}/book", params={"token_id": token_id}, timeout=10)
        if resp.status_code == 200:
            book = resp.json()
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            
            best_bid = float(bids[0]["price"]) if bids else 0
            best_ask = float(asks[0]["price"]) if asks else 0
            bid_depth = float(bids[0].get("size", 0)) if bids else 0
            ask_depth = float(asks[0].get("size", 0)) if asks else 0
            
            return {
                "bid": best_bid,
                "ask": best_ask,
                "spread": round(best_ask - best_bid, 4) if best_ask > best_bid else 0,
                "bid_depth": bid_depth,
                "ask_depth": ask_depth,
            }
    except Exception:
        pass
    return None


def discover_esports():
    """Discover available esports sport keys from The Odds API."""
    if not ODDS_API_KEY:
        print("  WARNING: No ODDSPAPI_KEY set", flush=True)
        return []

    try:
        resp = requests.get(f"{ODDS_API_BASE}/sports", params={
            "apiKey": ODDS_API_KEY,
            "all": "true",
        }, timeout=15)
        print(f"    Sports API status: {resp.status_code}", flush=True)
        if resp.status_code == 200:
            sports = resp.json()
            esports = [s for s in sports if "esport" in s.get("group", "").lower() 
                       or "esport" in s.get("key", "").lower()]
            print(f"    All sports: {len(sports)}, esports: {len(esports)}", flush=True)
            for e in esports:
                print(f"      {e['key']}: {e.get('title', '')} ({e.get('group', '')})", flush=True)
            return esports
        else:
            print(f"    Error: {resp.text[:200]}", flush=True)
    except Exception as e:
        print(f"    Discover error: {e}", flush=True)
    return []


def fetch_pinnacle_odds(sport_key):
    """Fetch Pinnacle odds for a sport from The Odds API."""
    if not ODDS_API_KEY:
        return []

    try:
        resp = requests.get(f"{ODDS_API_BASE}/sports/{sport_key}/odds", params={
            "apiKey": ODDS_API_KEY,
            "regions": "eu",
            "markets": "h2h",
            "bookmakers": "pinnacle",
            "oddsFormat": "decimal",
        }, timeout=30)
        print(f"    {sport_key} odds status: {resp.status_code}", flush=True)

        if resp.status_code == 200:
            data = resp.json()
            print(f"    {sport_key}: {len(data)} events with odds", flush=True)
            if data:
                first = data[0]
                print(f"    Sample: {first.get('home_team', '?')} vs {first.get('away_team', '?')}", flush=True)
            return data
        elif resp.status_code == 422:
            print(f"    Sport '{sport_key}' not available or out of season", flush=True)
        else:
            print(f"    Error: {resp.text[:150]}", flush=True)
    except Exception as e:
        print(f"    Odds error: {e}", flush=True)
    return []


def normalize_team(name):
    """Normalize team name for fuzzy matching."""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [" esports", " gaming", " team", " club", " e-sports", " academy"]:
        name = name.replace(suffix, "")
    # Remove special chars
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return name.strip()


def match_markets(poly_markets, odds_events):
    """Match Polymarket markets to The Odds API events by team names."""
    matched = []

    # Index odds events by normalized team names
    odds_index = {}
    for event in odds_events:
        t1 = normalize_team(event.get("home_team", ""))
        t2 = normalize_team(event.get("away_team", ""))
        if t1 and t2:
            key = tuple(sorted([t1, t2]))
            odds_index[key] = event

    for m in poly_markets:
        title = m.get("_event_title", "") or m.get("question", "")
        vs_match = re.search(r'(?::\s*)?(.+?)\s+vs\.?\s+(.+?)(?:\s*\(|$)', title, re.IGNORECASE)
        if not vs_match:
            continue

        t1_raw = vs_match.group(1).strip()
        t2_raw = vs_match.group(2).strip()
        t1_norm = normalize_team(t1_raw)
        t2_norm = normalize_team(t2_raw)
        key = tuple(sorted([t1_norm, t2_norm]))

        if key in odds_index:
            matched.append({
                "polymarket": m,
                "odds_event": odds_index[key],
                "team_a": t1_raw,
                "team_b": t2_raw,
            })

    return matched


def collect():
    """Main collection routine."""
    now = datetime.now(timezone.utc)
    print("=" * 60, flush=True)
    print("ESPORTS PRICE COLLECTOR", flush=True)
    print(f"{now.isoformat()} UTC", flush=True)
    print("=" * 60, flush=True)

    history = load_history()

    # 1. Fetch Polymarket esports
    poly_markets = fetch_polymarket_esports()

    if not poly_markets:
        print("No esports markets on Polymarket.", flush=True)
        save_history(history)
        return

    # 2. Discover and fetch esports odds from The Odds API
    print(f"\n  Discovering esports on The Odds API...", flush=True)
    esports = discover_esports()

    all_odds_events = []
    for sport in esports:
        sport_key = sport["key"]
        if not sport.get("active", False):
            continue
        events = fetch_pinnacle_odds(sport_key)
        for e in events:
            e["_sport_key"] = sport_key
            e["_game"] = sport.get("title", sport_key)
        all_odds_events.extend(events)
        time.sleep(0.5)

    print(f"  Total odds events: {len(all_odds_events)}", flush=True)

    # 3. Match
    matched = match_markets(poly_markets, all_odds_events)
    print(f"  Matched: {len(matched)} pairs", flush=True)

    # 4. Record snapshots
    new_snapshots = 0
    for pair in matched:
        poly = pair["polymarket"]
        event = pair["odds_event"]

        # Extract Pinnacle odds from The Odds API format
        odds_a, odds_b = None, None
        home_team = event.get("home_team", "")
        away_team = event.get("away_team", "")
        
        for bk in event.get("bookmakers", []):
            if bk.get("key") == "pinnacle":
                for mkt in bk.get("markets", []):
                    if mkt.get("key") == "h2h":
                        outcomes = mkt.get("outcomes", [])
                        for o in outcomes:
                            if o.get("name") == home_team:
                                odds_a = float(o.get("price", 0))
                            elif o.get("name") == away_team:
                                odds_b = float(o.get("price", 0))
        
        # Fallback: use first bookmaker if no Pinnacle
        if not odds_a or not odds_b:
            for bk in event.get("bookmakers", []):
                for mkt in bk.get("markets", []):
                    if mkt.get("key") == "h2h":
                        outcomes = mkt.get("outcomes", [])
                        if len(outcomes) >= 2:
                            odds_a = float(outcomes[0].get("price", 0))
                            odds_b = float(outcomes[1].get("price", 0))
                if odds_a and odds_b:
                    break

        if not odds_a or not odds_b or odds_a <= 1 or odds_b <= 1:
            continue

        # Devig
        fair_a, fair_b = devig_odds(odds_a, odds_b)

        # Polymarket price
        outcomes_raw = poly.get("outcomes", "[]")
        prices_raw = poly.get("outcomePrices", "[]")
        tokens_raw = poly.get("clobTokenIds", "[]")

        if isinstance(outcomes_raw, str):
            outcomes_raw = json.loads(outcomes_raw)
        if isinstance(prices_raw, str):
            prices_raw = json.loads(prices_raw)
        if isinstance(tokens_raw, str):
            tokens_raw = json.loads(tokens_raw)

        yes_price = float(prices_raw[0]) if prices_raw else 0
        yes_token = tokens_raw[0] if tokens_raw else ""

        # Orderbook depth (skip if no token)
        book = None
        if yes_token:
            book = fetch_polymarket_orderbook(yes_token)
            time.sleep(0.2)

        # Match start time
        start_time = event.get("commence_time", "")
        hours_to_start = None
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                hours_to_start = round((start_dt - now).total_seconds() / 3600, 1)
            except:
                pass

        snapshot = {
            "t_snapshot": now.isoformat(),
            "match_id": poly.get("conditionId", "")[:20],
            "game": event.get("_game", ""),
            "team_a": pair["team_a"],
            "team_b": pair["team_b"],
            "tournament": event.get("sport_title", "")[:40],
            "match_start": start_time,
            "hours_to_start": hours_to_start,
            # Sharp book odds
            "pin_odds_a": odds_a,
            "pin_odds_b": odds_b,
            "pin_vig_pct": round((1/odds_a + 1/odds_b - 1) * 100, 1),
            "pin_fair_a": round(fair_a, 4),
            "pin_fair_b": round(fair_b, 4),
            # Polymarket
            "poly_yes_price": round(yes_price, 4),
            "poly_bid": book["bid"] if book else None,
            "poly_ask": book["ask"] if book else None,
            "poly_spread": book["spread"] if book else None,
            "poly_bid_depth": book["bid_depth"] if book else None,
            "poly_ask_depth": book["ask_depth"] if book else None,
            # Derived
            "diff_pct": round((fair_a - yes_price) * 100, 1),
            "event_title": poly.get("_event_title", "")[:60],
            "outcome": None,
        }

        history["snapshots"].append(snapshot)
        new_snapshots += 1

        if abs(snapshot["diff_pct"]) >= 5:
            print(f"\n  >>> {snapshot['game']}: {pair['team_a']} vs {pair['team_b']}", flush=True)
            print(f"      Sharp fair: {fair_a:.0%} | Polymarket: {yes_price:.0%} | Diff: {snapshot['diff_pct']:+.1f}%", flush=True)
            if book:
                print(f"      Spread: {book['spread']:.2f} | Bid depth: ${book['bid_depth']:.0f} | Ask depth: ${book['ask_depth']:.0f}", flush=True)

    save_history(history)

    # Summary
    total = len(history["snapshots"])
    print(f"\n  New snapshots: {new_snapshots}", flush=True)
    print(f"  Total in history: {total}", flush=True)

    # Stats on differences
    recent = [s for s in history["snapshots"] if s.get("diff_pct") is not None]
    if recent:
        diffs = [abs(s["diff_pct"]) for s in recent]
        avg_diff = sum(diffs) / len(diffs)
        big_diffs = sum(1 for d in diffs if d >= 5)
        print(f"  Avg |diff|: {avg_diff:.1f}%", flush=True)
        print(f"  Diffs ≥5%: {big_diffs}/{len(diffs)} ({big_diffs/len(diffs)*100:.0f}%)", flush=True)

    print("=" * 60, flush=True)


if __name__ == "__main__":
    collect()
