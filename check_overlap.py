"""Quick check: which The Odds API sports overlap with Polymarket."""
import os
import re
import json
import requests
import time

ODDS_KEY = os.getenv("ODDSPAPI_KEY", "")
ODDS_BASE = "https://api.the-odds-api.com/v4"
GAMMA = "https://gamma-api.polymarket.com"

def normalize(name):
    return re.sub(r'[^a-z0-9\s]', '', name.lower().strip())

# 1. Get all active sports from The Odds API
print("=== The Odds API: active sports ===", flush=True)
resp = requests.get(f"{ODDS_BASE}/sports", params={"apiKey": ODDS_KEY}, timeout=15)
sports = [s for s in resp.json() if s.get("active")]
print(f"Active sports: {len(sports)}", flush=True)

# Group by group
from collections import defaultdict
by_group = defaultdict(list)
for s in sports:
    by_group[s["group"]].append(s)

for grp in sorted(by_group):
    keys = [s["key"] for s in by_group[grp]]
    print(f"  {grp}: {', '.join(keys[:5])}", flush=True)

# 2. Get Polymarket sport tags
print("\n=== Polymarket: sport event samples ===", flush=True)
poly_teams = set()
sport_tags = ["basketball", "baseball", "soccer", "tennis", "hockey", 
              "mma", "boxing", "cricket", "football", "rugby"]

for tag in sport_tags:
    try:
        resp = requests.get(f"{GAMMA}/events", params={
            "limit": 5, "active": "true", "closed": "false", "tag_slug": tag,
        }, timeout=15)
        events = resp.json()
        if events:
            print(f"\n  Tag '{tag}': {len(events)} events", flush=True)
            for e in events[:3]:
                title = e.get("title", "")[:70]
                print(f"    {title}", flush=True)
        time.sleep(0.3)
    except:
        pass

# 3. Try matching a few sports — fetch odds and Polymarket events, compare
print("\n=== Quick match test ===", flush=True)
test_sports = ["basketball_nba", "baseball_mlb", "icehockey_nhl", 
               "mma_mixed_martial_arts", "soccer_epl"]

for sport_key in test_sports:
    try:
        resp = requests.get(f"{ODDS_BASE}/sports/{sport_key}/odds", params={
            "apiKey": ODDS_KEY, "regions": "eu", "markets": "h2h",
            "bookmakers": "pinnacle", "oddsFormat": "decimal",
        }, timeout=15)
        if resp.status_code != 200:
            continue
        events = resp.json()
        if not events:
            continue

        print(f"\n  {sport_key}: {len(events)} events with Pinnacle odds", flush=True)
        for ev in events[:3]:
            home = ev.get("home_team", "?")
            away = ev.get("away_team", "?")
            # Get Pinnacle odds
            for bk in ev.get("bookmakers", []):
                if bk["key"] == "pinnacle":
                    for mkt in bk.get("markets", []):
                        outcomes = mkt.get("outcomes", [])
                        if len(outcomes) >= 2:
                            o1 = outcomes[0]
                            o2 = outcomes[1]
                            print(f"    {home} vs {away}: {o1['price']:.2f} / {o2['price']:.2f}", flush=True)
        time.sleep(0.5)
    except Exception as e:
        print(f"  {sport_key} error: {e}", flush=True)

print("\n=== DONE ===", flush=True)
