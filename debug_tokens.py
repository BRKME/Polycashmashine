"""Debug: compare Gamma API token IDs with CLOB orderbook."""
import requests
import json

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

# Fetch one temperature event
print("=== Fetching 1 temperature event from Gamma ===")
resp = requests.get(f"{GAMMA}/events", params={
    "limit": 1, "active": "true", "tag_slug": "temperature",
}, timeout=30)
events = resp.json()

if not events:
    print("No events found")
    exit()

event = events[0]
print(f"Event: {event['title']}")
print(f"Event slug: {event.get('slug')}")

markets = event.get("markets", [])
print(f"Markets in event: {len(markets)}")

# Examine first 2 markets in detail
for i, m in enumerate(markets[:2]):
    print(f"\n--- Market {i} ---")
    print(f"  question: {m.get('question', 'N/A')[:80]}")
    print(f"  conditionId: {m.get('conditionId', 'N/A')[:30]}...")
    print(f"  slug: {m.get('slug', 'N/A')[:60]}")
    
    # Raw outcomes
    outcomes_raw = m.get("outcomes")
    print(f"  outcomes (raw type={type(outcomes_raw).__name__}): {str(outcomes_raw)[:100]}")
    
    # Parse if string
    if isinstance(outcomes_raw, str):
        outcomes = json.loads(outcomes_raw)
    else:
        outcomes = outcomes_raw or []
    print(f"  outcomes (parsed): {outcomes}")
    
    # Raw clobTokenIds
    tokens_raw = m.get("clobTokenIds")
    print(f"  clobTokenIds (raw type={type(tokens_raw).__name__}): {str(tokens_raw)[:120]}")
    
    if isinstance(tokens_raw, str):
        tokens = json.loads(tokens_raw)
    else:
        tokens = tokens_raw or []
    
    for j, tok in enumerate(tokens):
        label = outcomes[j] if j < len(outcomes) else f"idx{j}"
        print(f"  Token [{label}]: {tok}")
        
        # Try CLOB orderbook lookup
        try:
            r = requests.get(f"{CLOB}/book", params={"token_id": tok}, timeout=10)
            print(f"    CLOB /book status: {r.status_code}")
            if r.status_code == 200:
                book = r.json()
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                print(f"    Orderbook: {len(bids)} bids, {len(asks)} asks")
                if bids:
                    print(f"    Best bid: {bids[0].get('price')}")
                if asks:
                    print(f"    Best ask: {asks[0].get('price')}")
            else:
                print(f"    Response: {r.text[:100]}")
        except Exception as e:
            print(f"    CLOB error: {e}")

    # Also try: fetch market directly from CLOB
    cid = m.get("conditionId", "")
    if cid:
        print(f"\n  Trying CLOB /markets/{cid[:20]}...")
        try:
            r = requests.get(f"{CLOB}/markets/{cid}", timeout=10)
            print(f"    Status: {r.status_code}")
            if r.status_code == 200:
                cm = r.json()
                print(f"    CLOB tokens: {json.dumps(cm.get('tokens', []))[:200]}")
            else:
                print(f"    Response: {r.text[:100]}")
        except Exception as e:
            print(f"    Error: {e}")

print("\n=== DONE ===")
