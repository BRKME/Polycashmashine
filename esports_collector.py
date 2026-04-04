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

# The Odds API (the-odds-api.com) — traditional sports
ODDS_API_KEY = os.getenv("ODDSPAPI_KEY", "")
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# OddsPapi (oddspapi.io) — esports with Pinnacle
ODDSPAPI_KEY = os.getenv("ODDSPAPI_KEY_V2", "")
ODDSPAPI_BASE = "https://api.oddspapi.io/v4"

# OddsPapi esport sport IDs
ESPORT_IDS = {
    "cs2": 17,
    "lol": 18,
    "dota2": 16,
    "valorant": 61,
}

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


def fetch_esports_odds():
    """Fetch esports odds from OddsPapi (Pinnacle + others)."""
    if not ODDSPAPI_KEY:
        print("  WARNING: No ODDSPAPI_KEY_V2 set", flush=True)
        return []

    all_fixtures = []
    for game, sport_id in ESPORT_IDS.items():
        print(f"  Fetching {game} (id={sport_id})...", flush=True)
        try:
            # OddsPapi requires from/to date range
            from_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            to_date = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
            
            resp = requests.get(f"{ODDSPAPI_BASE}/fixtures", params={
                "apiKey": ODDSPAPI_KEY,
                "sportId": sport_id,
                "from": from_date,
                "to": to_date,
            }, timeout=30)
            print(f"    Status: {resp.status_code}", flush=True)

            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    print(f"    Fixtures: {len(data)}", flush=True)
                    for f in data:
                        f["_game"] = game
                    all_fixtures.extend(data)
                    if data:
                        first = data[0]
                        print(f"    Keys: {list(first.keys())[:8]}", flush=True)
                        parts = first.get("participants", [])
                        if parts:
                            print(f"    Sample: {parts[0].get('name','?')} vs {parts[1].get('name','?') if len(parts)>1 else '?'}", flush=True)
                elif isinstance(data, dict):
                    print(f"    Dict response: {list(data.keys())[:5]}", flush=True)
                    print(f"    Snippet: {str(data)[:150]}", flush=True)
            elif resp.status_code == 401:
                print(f"    Auth error: {resp.text[:100]}", flush=True)
                return []  # No point trying other sports
            else:
                print(f"    Error: {resp.text[:150]}", flush=True)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
        time.sleep(2)  # Avoid rate limiting

    return all_fixtures


def extract_pinnacle_odds(fixture):
    """Extract Pinnacle h2h odds from OddsPapi fixture."""
    markets = fixture.get("markets", [])
    bookmakers = fixture.get("bookmakers", fixture.get("odds", []))
    
    # Try different response structures
    # Structure 1: fixture.markets[].outcomes[]
    for mkt in markets:
        market_id = mkt.get("marketId", mkt.get("id", 0))
        if market_id == 171 or mkt.get("key") == "match_winner":
            outcomes = mkt.get("outcomes", [])
            if len(outcomes) >= 2:
                return float(outcomes[0].get("price", 0)), float(outcomes[1].get("price", 0))
    
    # Structure 2: fixture.odds.pinnacle or fixture.bookmakers
    if isinstance(bookmakers, list):
        for bk in bookmakers:
            bk_key = bk.get("key", bk.get("bookmaker", "")).lower()
            if "pinnacle" in bk_key:
                for mkt in bk.get("markets", []):
                    outcomes = mkt.get("outcomes", [])
                    if len(outcomes) >= 2:
                        return float(outcomes[0].get("price", 0)), float(outcomes[1].get("price", 0))
    
    return None, None


def normalize_team(name):
    """Normalize team name for fuzzy matching."""
    name = name.lower().strip()
    # Remove common suffixes
    for suffix in [" esports", " gaming", " team", " club", " e-sports", " academy"]:
        name = name.replace(suffix, "")
    # Remove special chars
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return name.strip()


def match_markets(poly_markets, fixtures):
    """Match Polymarket markets to OddsPapi fixtures by team names."""
    matched = []

    # Index fixtures by normalized team names
    fix_index = {}
    for fix in fixtures:
        # OddsPapi uses "participants" array
        parts = fix.get("participants", [])
        if len(parts) >= 2:
            t1 = normalize_team(parts[0].get("name", ""))
            t2 = normalize_team(parts[1].get("name", ""))
        else:
            # The Odds API fallback
            t1 = normalize_team(fix.get("home_team", ""))
            t2 = normalize_team(fix.get("away_team", ""))
        
        if t1 and t2:
            key = tuple(sorted([t1, t2]))
            fix_index[key] = fix

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

        if key in fix_index:
            matched.append({
                "polymarket": m,
                "fixture": fix_index[key],
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

    # 2. Fetch esports odds from OddsPapi (Pinnacle + others)
    print(f"\n  Fetching esports odds from OddsPapi...", flush=True)
    all_fixtures = fetch_esports_odds()
    print(f"  Total fixtures: {len(all_fixtures)}", flush=True)

    # 3. Match
    matched = match_markets(poly_markets, all_fixtures)
    print(f"  Matched: {len(matched)} pairs", flush=True)

    # 4. Record snapshots
    new_snapshots = 0
    for pair in matched:
        poly = pair["polymarket"]
        fix = pair["fixture"]

        # Extract odds
        odds_a, odds_b = extract_pinnacle_odds(fix)

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

        # Orderbook depth
        book = None
        if yes_token:
            book = fetch_polymarket_orderbook(yes_token)
            time.sleep(0.2)

        # Match start time
        start_time = fix.get("commence_time", fix.get("startTime", ""))
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
            "game": fix.get("_game", ""),
            "team_a": pair["team_a"],
            "team_b": pair["team_b"],
            "tournament": fix.get("sport_title", fix.get("tournament", {}).get("name", ""))[:40],
            "match_start": start_time,
            "hours_to_start": hours_to_start,
            "pin_odds_a": odds_a,
            "pin_odds_b": odds_b,
            "pin_vig_pct": round((1/odds_a + 1/odds_b - 1) * 100, 1),
            "pin_fair_a": round(fair_a, 4),
            "pin_fair_b": round(fair_b, 4),
            "poly_yes_price": round(yes_price, 4),
            "poly_bid": book["bid"] if book else None,
            "poly_ask": book["ask"] if book else None,
            "poly_spread": book["spread"] if book else None,
            "poly_bid_depth": book["bid_depth"] if book else None,
            "poly_ask_depth": book["ask_depth"] if book else None,
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
