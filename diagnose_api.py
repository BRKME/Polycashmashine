"""
Diagnostic: find how Polymarket weather markets are accessible via API.

Tries every known endpoint and parameter combination.
Run: python diagnose_api.py
"""
import requests
import json
import time

GAMMA = "https://gamma-api.polymarket.com"

def try_endpoint(name, url, params):
    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"URL: {url}")
    print(f"Params: {params}")
    try:
        resp = requests.get(url, params=params, timeout=30)
        print(f"Status: {resp.status_code}")
        data = resp.json()
        
        if isinstance(data, list):
            print(f"Results: {len(data)}")
            for item in data[:3]:
                title = item.get("title", item.get("question", "N/A"))
                slug = item.get("slug", "N/A")
                print(f"  - {title[:80]}")
                print(f"    slug={slug[:60]}")
        elif isinstance(data, dict):
            print(f"Keys: {list(data.keys())[:10]}")
            if "title" in data or "question" in data:
                print(f"  Title: {data.get('title', data.get('question', 'N/A'))[:80]}")
        else:
            print(f"Type: {type(data)}")
            
    except Exception as e:
        print(f"ERROR: {e}")

# === Test 1: Direct slug lookup ===
# We know this market exists: highest-temperature-in-nyc-on-march-18-2026
try_endpoint(
    "Direct event slug lookup",
    f"{GAMMA}/events",
    {"slug": "highest-temperature-in-nyc-on-march-22-2026"}
)

# === Test 2: Slug with wildcard ===
try_endpoint(
    "Events with slug contains 'temperature'",
    f"{GAMMA}/events",
    {"slug": "highest-temperature", "limit": 5}
)

# === Test 3: Markets with slug filter ===
try_endpoint(
    "Markets slug contains temperature",
    f"{GAMMA}/markets",
    {"slug": "temperature", "limit": 5}
)

# === Test 4: Text search ===
try_endpoint(
    "Events text_query=temperature",
    f"{GAMMA}/events",
    {"limit": 5, "active": "true", "text_query": "temperature"}
)

# === Test 5: Search endpoint ===
try_endpoint(
    "Search endpoint",
    f"{GAMMA}/search",
    {"query": "temperature", "limit": 5}
)

# === Test 6: Markets with title filter ===
try_endpoint(
    "Markets title=temperature",
    f"{GAMMA}/markets",
    {"limit": 5, "active": "true", "title": "temperature"}
)

# === Test 7: Events tag_slug ===
try_endpoint(
    "Events tag_slug=temperature",
    f"{GAMMA}/events",
    {"limit": 5, "active": "true", "tag_slug": "temperature"}
)

# === Test 8: Events tag_slug=weather ===
try_endpoint(
    "Events tag_slug=weather",
    f"{GAMMA}/events",
    {"limit": 5, "active": "true", "tag_slug": "weather"}
)

# === Test 9: Events tag_slug=daily-temperature ===
try_endpoint(
    "Events tag_slug=daily-temperature",
    f"{GAMMA}/events",
    {"limit": 5, "active": "true", "tag_slug": "daily-temperature"}
)

# === Test 10: Fetch known market directly ===
# From earlier: condition IDs are available via slug
try_endpoint(
    "Single event by known slug",
    f"{GAMMA}/events/highest-temperature-in-nyc-on-march-22-2026",
    {}
)

# === Test 11: Try /tags endpoint ===
try_endpoint(
    "Tags endpoint",
    f"{GAMMA}/tags",
    {"limit": 30}
)

# === Test 12: Events sorted by creation date (newest first) ===
try_endpoint(
    "Newest events (limit=5)",
    f"{GAMMA}/events",
    {"limit": 5, "active": "true", "order": "createdAt", "_sort": "createdAt:desc"}
)

print(f"\n{'='*60}")
print("DONE")
