"""Debug: find active temperature markets and check CLOB orderbooks."""
import requests
import json
from datetime import date

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

# Fetch temperature events
print("=== Fetching temperature events ===", flush=True)
resp = requests.get(f"{GAMMA}/events", params={
    "limit": 20, "active": "true", "closed": "false", "tag_slug": "temperature",
}, timeout=60)
events = resp.json()
print(f"Got {len(events)} events", flush=True)

# Find events with future dates (march 21+)
today = date.today().isoformat()
print(f"Today: {today}", flush=True)

for event in events[:10]:
    title = event.get("title", "")
    slug = event.get("slug", "")
    markets = event.get("markets", [])
    
    # Skip old/resolved
    if "december" in title.lower() or "january" in title.lower():
        continue
    if "march 2" not in title.lower() and "march 3" not in title.lower():
        continue
    
    print(f"\n{'='*60}", flush=True)
    print(f"Event: {title}", flush=True)
    print(f"Slug: {slug}", flush=True)
    print(f"Markets: {len(markets)}", flush=True)
    
    # Check first 3 markets
    for i, m in enumerate(markets[:3]):
        q = m.get("question", "")
        cid = m.get("conditionId", "")
        active = m.get("active", None)
        closed = m.get("closed", None)
        end_date = m.get("endDate", "")
        
        # Parse tokens
        tokens_raw = m.get("clobTokenIds", "[]")
        tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else (tokens_raw or [])
        outcomes_raw = m.get("outcomes", "[]")
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
        prices_raw = m.get("outcomePrices", "[]")
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
        
        print(f"\n  Market {i}: {q[:70]}", flush=True)
        print(f"    active={active}, closed={closed}, endDate={end_date[:20]}", flush=True)
        print(f"    conditionId: {cid[:40]}...", flush=True)
        
        for j, tok in enumerate(tokens):
            label = outcomes[j] if j < len(outcomes) else f"idx{j}"
            price = prices[j] if j < len(prices) else "?"
            print(f"    Token [{label}] price={price}: {tok[:40]}...", flush=True)
            
            # Check CLOB orderbook
            try:
                r = requests.get(f"{CLOB}/book", params={"token_id": tok}, timeout=10)
                if r.status_code == 200:
                    book = r.json()
                    bids = book.get("bids", [])
                    asks = book.get("asks", [])
                    print(f"      BOOK: {len(bids)} bids, {len(asks)} asks", flush=True)
                    if bids:
                        print(f"      Best bid: {bids[0]}", flush=True)
                    if asks:
                        print(f"      Best ask: {asks[0]}", flush=True)
                else:
                    print(f"      BOOK: {r.status_code} {r.text[:60]}", flush=True)
            except Exception as e:
                print(f"      BOOK error: {e}", flush=True)
        
        # Also check CLOB /markets/
        if cid:
            try:
                r = requests.get(f"{CLOB}/markets/{cid}", timeout=10)
                if r.status_code == 200:
                    cm = r.json()
                    ctokens = cm.get("tokens", [])
                    print(f"    CLOB /markets: {len(ctokens)} tokens, active={cm.get('active')}, closed={cm.get('closed')}", flush=True)
                    # Check if CLOB tokens differ from Gamma tokens
                    for ct in ctokens:
                        ct_id = ct.get("token_id", "")
                        ct_outcome = ct.get("outcome", "")
                        in_gamma = ct_id in tokens
                        print(f"      CLOB token [{ct_outcome}]: {ct_id[:40]}... match_gamma={in_gamma}", flush=True)
                else:
                    print(f"    CLOB /markets: {r.status_code}", flush=True)
            except Exception as e:
                print(f"    CLOB /markets error: {e}", flush=True)
    
    # Only check first matching event in detail
    break

print("\n=== DONE ===", flush=True)
