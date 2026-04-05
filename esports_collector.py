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
                        print(f"    Keys: {list(first.keys())}", flush=True)
                        # Print full first fixture for structure discovery
                        if game == "cs2":
                            print(f"    FULL FIRST: {json.dumps(first, indent=2)[:500]}", flush=True)
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


def fetch_fixture_odds(fixture_id):
    """Fetch odds for a specific fixture from OddsPapi."""
    try:
        resp = requests.get(f"{ODDSPAPI_BASE}/odds", params={
            "apiKey": ODDSPAPI_KEY,
            "fixtureId": fixture_id,
        }, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def extract_pinnacle_odds(odds_data):
    """Extract match-winner odds from OddsPapi bookmakerOdds structure.
    
    Real structure:
    bookmakerOdds.{bk}.markets.{mktId}.outcomes.{outId}.players.{idx}
      .bookmakerOutcomeId = "home"/"away"
      .price = 1.50
    """
    bk_odds = odds_data.get("bookmakerOdds", {})
    
    # Priority order for bookmakers
    priority = ["pinnacle", "betway", "bet365", "unibet", "1xbet", "stake", "bcgame", "roobet"]
    
    # Try priority books first, then any
    bk_keys_to_try = [k for k in priority if k in bk_odds]
    bk_keys_to_try += [k for k in bk_odds if k not in priority]
    
    for bk_key in bk_keys_to_try:
        bk = bk_odds.get(bk_key)
        if not isinstance(bk, dict) or not bk.get("bookmakerIsActive"):
            continue
        
        markets = bk.get("markets", {})
        
        for mkt_id, mkt_data in markets.items():
            if not isinstance(mkt_data, dict):
                continue
            
            outcomes = mkt_data.get("outcomes", {})
            
            # Collect all prices from this market
            home_price = None
            away_price = None
            all_prices = []
            
            for out_id, out_data in outcomes.items():
                if not isinstance(out_data, dict):
                    continue
                
                # Check if price is directly on outcome
                if "price" in out_data:
                    all_prices.append(float(out_data["price"]))
                    oid = (out_data.get("bookmakerOutcomeId") or "").lower()
                    if oid in ("home", "1", "team1"):
                        home_price = float(out_data["price"])
                    elif oid in ("away", "2", "team2"):
                        away_price = float(out_data["price"])
                
                # Check players nested structure
                players = out_data.get("players", {})
                if isinstance(players, dict):
                    for pid, pdata in players.items():
                        if not isinstance(pdata, dict):
                            continue
                        if "price" in pdata and pdata.get("active", True):
                            price = float(pdata["price"])
                            all_prices.append(price)
                            oid = (pdata.get("bookmakerOutcomeId") or "").lower()
                            if oid in ("home", "1", "team1"):
                                home_price = price
                            elif oid in ("away", "2", "team2"):
                                away_price = price
            
            # Return if we found two prices
            if home_price and away_price and home_price > 1 and away_price > 1:
                return home_price, away_price, bk_key
            
            # Fallback: if we have exactly 2 prices
            valid = [p for p in all_prices if p > 1]
            if len(valid) >= 2:
                return valid[0], valid[1], bk_key
    
    return None, None, None


def normalize_team(name):
    """Normalize team name for fuzzy matching."""
    name = name.lower().strip()
    for suffix in [" esports", " gaming", " team", " club", " e-sports", " academy"]:
        name = name.replace(suffix, "")
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return name.strip()


def extract_teams(title):
    """Extract team names from event title, stripping game prefix."""
    # Strip "Counter-Strike: ", "LoL: ", "Dota 2: " etc
    stripped = re.sub(r'^[^:]+:\s*', '', title)
    vs = re.search(r'(.+?)\s+vs\.?\s+(.+?)(?:\s*\(|$)', stripped, re.IGNORECASE)
    if vs:
        return vs.group(1).strip(), vs.group(2).strip()
    return None, None


def match_markets(poly_markets, fixtures):
    """Match Polymarket markets to OddsPapi fixtures by team names."""
    matched = []

    fix_index = {}
    for fix in fixtures:
        t1 = normalize_team(fix.get("participant1Name", ""))
        t2 = normalize_team(fix.get("participant2Name", ""))
        if t1 and t2:
            key = tuple(sorted([t1, t2]))
            fix_index[key] = fix

    print(f"    Indexed {len(fix_index)} unique fixture pairs", flush=True)
    for i, (key, fix) in enumerate(fix_index.items()):
        if i < 5:
            print(f"    OA: {fix.get('participant1Name')} vs {fix.get('participant2Name')} (hasOdds={fix.get('hasOdds')})", flush=True)

    poly_parsed = 0
    for m in poly_markets:
        title = m.get("_event_title", "") or m.get("question", "")
        t1_raw, t2_raw = extract_teams(title)
        if not t1_raw:
            continue
        poly_parsed += 1

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

    print(f"    Polymarket parsed: {poly_parsed} vs-matches", flush=True)
    # Show samples
    for m in poly_markets[:10]:
        title = m.get("_event_title", "") or m.get("question", "")
        t1, t2 = extract_teams(title)
        if t1:
            n1, n2 = normalize_team(t1), normalize_team(t2)
            hit = "✓" if tuple(sorted([n1, n2])) in fix_index else ""
            print(f"      PM: '{n1}' vs '{n2}' {hit}", flush=True)

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
    
    # Count fixtures with odds
    with_odds = sum(1 for f in all_fixtures if f.get("hasOdds"))
    print(f"  Fixtures with odds: {with_odds}/{len(all_fixtures)}", flush=True)

    # Debug: fetch odds for a fixture WITH odds
    if all_fixtures and ODDSPAPI_KEY:
        # Find first fixture with hasOdds=true
        odds_fixture = None
        for f in all_fixtures:
            if f.get("hasOdds"):
                odds_fixture = f
                break
        
        if odds_fixture:
            fid = odds_fixture.get("fixtureId", "")
            p1 = odds_fixture.get("participant1Name", "?")
            p2 = odds_fixture.get("participant2Name", "?")
            print(f"\n  Debug: odds for {p1} vs {p2} (id={fid})...", flush=True)
            try:
                resp = requests.get(f"{ODDSPAPI_BASE}/odds", params={
                    "apiKey": ODDSPAPI_KEY,
                    "fixtureId": fid,
                }, timeout=15)
                print(f"    Status: {resp.status_code}", flush=True)
                if resp.status_code == 200:
                    data = resp.json()
                    print(f"    Keys: {list(data.keys())[:10]}", flush=True)
                    # Look for odds/bookmakers/markets
                    for key in ["odds", "bookmakers", "markets", "pinnacle"]:
                        if key in data:
                            val = data[key]
                            print(f"    {key}: {json.dumps(val, indent=2)[:300]}", flush=True)
                    # Print full if small
                    data_str = json.dumps(data, indent=2)
                    if len(data_str) < 600:
                        print(f"    FULL: {data_str}", flush=True)
                    else:
                        print(f"    Truncated: {data_str[:500]}...", flush=True)
            except Exception as e:
                print(f"    Error: {e}", flush=True)
            time.sleep(1)

    # 3. DEBUG: Show team names from both sides before matching
    print(f"\n  === TEAM NAME COMPARISON ===", flush=True)
    
    # OddsPapi teams
    odds_names = set()
    for f in all_fixtures:
        n1 = normalize_team(f.get("participant1Name", ""))
        n2 = normalize_team(f.get("participant2Name", ""))
        if n1: odds_names.add(n1)
        if n2: odds_names.add(n2)
    print(f"  OddsPapi unique teams: {len(odds_names)}", flush=True)
    for n in sorted(odds_names)[:10]:
        print(f"    OA: '{n}'", flush=True)

    # Polymarket teams
    poly_names = set()
    for m in poly_markets[:200]:
        title = m.get("_event_title", "") or m.get("question", "")
        t1, t2 = extract_teams(title)
        if t1:
            n1 = normalize_team(t1)
            n2 = normalize_team(t2)
            if n1: poly_names.add(n1)
            if n2: poly_names.add(n2)
    print(f"  Polymarket unique teams: {len(poly_names)}", flush=True)
    for n in sorted(poly_names)[:10]:
        print(f"    PM: '{n}'", flush=True)

    # Intersection
    overlap = odds_names & poly_names
    print(f"  OVERLAP: {len(overlap)} teams", flush=True)
    for n in sorted(overlap)[:10]:
        print(f"    BOTH: '{n}'", flush=True)

    # 4. Match
    matched = match_markets(poly_markets, all_fixtures)
    print(f"  Matched: {len(matched)} pairs", flush=True)

    # 5. Record snapshots — fetch odds for matched fixtures with hasOdds=true
    new_snapshots = 0
    odds_fetched = 0
    MAX_ODDS_FETCHES = 30  # Limit API calls per run

    # Deduplicate matched by fixture ID
    seen_fixtures = set()
    unique_matched = []
    for pair in matched:
        fid = pair["fixture"].get("fixtureId", "")
        if fid not in seen_fixtures and pair["fixture"].get("hasOdds"):
            seen_fixtures.add(fid)
            unique_matched.append(pair)

    print(f"  Unique matched with odds: {len(unique_matched)}", flush=True)

    for pair in unique_matched[:MAX_ODDS_FETCHES]:
        poly = pair["polymarket"]
        fix = pair["fixture"]
        fid = fix.get("fixtureId", "")

        # Fetch odds for this fixture
        odds_data = fetch_fixture_odds(fid)
        odds_fetched += 1
        if not odds_data:
            continue

        # Debug: show bookmakerOdds structure for first few
        if odds_fetched <= 3:
            bk_odds = odds_data.get("bookmakerOdds", {})
            print(f"\n  Debug odds #{odds_fetched}: {fix.get('participant1Name')} vs {fix.get('participant2Name')}", flush=True)
            print(f"    Bookmakers: {list(bk_odds.keys())[:8]}", flush=True)
            for bk_key in ["pinnacle", "betway", "bet365"]:
                bk = bk_odds.get(bk_key)
                if bk:
                    markets = bk.get("markets", {})
                    print(f"    {bk_key}: active={bk.get('bookmakerIsActive')}, markets={list(markets.keys())[:5]}", flush=True)
                    for mk, mv in list(markets.items())[:2]:
                        outcomes = mv.get("outcomes", {}) if isinstance(mv, dict) else {}
                        for oid, odata in list(outcomes.items())[:1]:
                            players = odata.get("players", {}) if isinstance(odata, dict) else {}
                            print(f"      market {mk}, outcome {oid}: players keys={list(players.keys())[:3]}", flush=True)
                            for pid, pdata in list(players.items())[:2]:
                                if isinstance(pdata, dict):
                                    print(f"        player {pid}: price={pdata.get('price')}, outcomeId={pdata.get('bookmakerOutcomeId')}, active={pdata.get('active')}", flush=True)
            # If no priority books, show first available
            if not any(bk_odds.get(k) for k in ["pinnacle", "betway", "bet365"]):
                first_bk = list(bk_odds.keys())[0] if bk_odds else "none"
                if first_bk != "none":
                    bk = bk_odds[first_bk]
                    markets = bk.get("markets", {})
                    print(f"    {first_bk}: active={bk.get('bookmakerIsActive')}, markets={list(markets.keys())[:5]}", flush=True)
                    for mk, mv in list(markets.items())[:1]:
                        outcomes = mv.get("outcomes", {}) if isinstance(mv, dict) else {}
                        for oid, odata in list(outcomes.items())[:1]:
                            players = odata.get("players", {}) if isinstance(odata, dict) else {}
                            print(f"      market {mk}, outcome {oid}: players keys={list(players.keys())[:3]}", flush=True)
                            for pid, pdata in list(players.items())[:2]:
                                if isinstance(pdata, dict):
                                    print(f"        player {pid}: price={pdata.get('price')}, outcomeId={pdata.get('bookmakerOutcomeId')}", flush=True)

        odds_a, odds_b, bk_source = extract_pinnacle_odds(odds_data)

        if not odds_a or not odds_b or odds_a <= 1 or odds_b <= 1:
            if odds_fetched <= 3:
                print(f"    EXTRACT FAILED: odds_a={odds_a}, odds_b={odds_b}, source={bk_source}", flush=True)
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
            "tournament": fix.get("tournamentName", "")[:40],
            "match_start": start_time,
            "hours_to_start": hours_to_start,
            "odds_source": bk_source,
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
            print(f"      {bk_source} fair: {fair_a:.0%} | Polymarket: {yes_price:.0%} | Diff: {snapshot['diff_pct']:+.1f}%", flush=True)
            if book:
                print(f"      Spread: {book['spread']:.2f} | Bid depth: ${book['bid_depth']:.0f} | Ask depth: ${book['ask_depth']:.0f}", flush=True)

        time.sleep(1)  # Rate limit between odds fetches

    save_history(history)

    # Summary
    total = len(history["snapshots"])
    print(f"\n  Odds fetched: {odds_fetched}", flush=True)
    print(f"  New snapshots: {new_snapshots}", flush=True)
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
