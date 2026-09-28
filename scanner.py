import http.server
import socketserver
import urllib.request
import urllib.parse
import urllib.error
import json
import re
from datetime import datetime, timezone
import concurrent.futures
import os
import sys
import time
import threading
import copy

# Ensure UTF-8 output on Windows
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

PORT = 5000
OPENSEA_GQL = "https://gql.opensea.io/graphql"
ROBINHOOD_COLLECTIONS_URL = "https://opensea.io/collections/chain/robinhood"
ROBINHOOD_RPC = "https://rpc.mainnet.chain.robinhood.com"
SEADROP_CONTRACT = "0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"
TOPIC_PUBLIC_DROP = "0x3e30d8e1f739ea4795c481b21c23f905e938b80339305f3508e43c558e5dead3"

DROP_QUERY = """
query GetDropDetails($slug: String!) {
  collectionBySlug(slug: $slug) {
    ... on Collection {
      name
      slug
      description
      imageUrl
      bannerImageUrl
      twitterUsername
      discordUrl
      externalUrl
      telegramUrl
      isVerified
      owner {
        address
      }
    }
  }
  collectionActivity(collectionSlug: $slug, limit: 1) {
    items {
      ... on Mint {
        item {
          imageUrl
        }
      }
      ... on Sale {
        item {
          imageUrl
        }
      }
      ... on Transfer {
        item {
          imageUrl
        }
      }
    }
  }
  dropBySlug(slug: $slug) {
    __typename
    ... on Erc721SeaDropV1 {
      maxSupply
      totalSupply
      stages {
        stageIndex
        label
        startTime
        endTime
        maxTotalMintableByWallet
        price {
          usd
          token {
            unit
            symbol
          }
        }
      }
    }
    ... on Erc1155SeaDropV2 {
      tokenSupply {
        totalSupply
        maxSupply
      }
      stages {
        stageIndex
        label
        startTime
        endTime
        maxTotalMintableByWallet
        price {
          usd
          token {
            unit
            symbol
          }
        }
      }
    }
  }
}
"""

CACHE_FILE = os.path.join(os.path.dirname(__file__), "scan_cache.json")
HISTORY_FILE = os.path.join(os.path.dirname(__file__), "scan_history.json")
scan_cache = {
    "last_updated": None,
    "scan_mode": "deep_onchain",
    "collections": []
}
cache_lock = threading.RLock()
scan_lock = threading.Lock()
scan_state = {"running": False, "started_at": None, "finished_at": None, "error": None,
              "mode": None, "onchain_upcoming": 0, "onchain_live": 0,
              "resolved": 0, "candidates": 0, "results": 0, "lookup_errors": 0}

def load_history():
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (OSError, ValueError):
        return []

def record_history(previous, current, observed_at):
    before = {c.get("slug"): c for c in previous if c.get("slug")}
    events = load_history()
    for c in current:
        old = before.get(c.get("slug"))
        if not old:
            events.append({"at": observed_at, "slug": c["slug"], "type": "discovered",
                           "value": c.get("overallStatus")})
            continue
        for field, kind in (("overallStatus", "status"), ("itemsMinted", "minted"),
                            ("maxSupply", "supply")):
            if old.get(field) != c.get(field):
                events.append({"at": observed_at, "slug": c["slug"], "type": kind,
                               "from": old.get(field), "value": c.get(field)})
        old_stage = next((s for s in old.get("stages", []) if s.get("status") == "LIVE"), None)
        new_stage = next((s for s in c.get("stages", []) if s.get("status") == "LIVE"), None)
        old_price = old_stage.get("priceToken") if old_stage else None
        new_price = new_stage.get("priceToken") if new_stage else None
        if old_price != new_price:
            events.append({"at": observed_at, "slug": c["slug"], "type": "price",
                           "from": old_price, "value": new_price})
    atomic_json_write(HISTORY_FILE, events[-5000:])

def atomic_json_write(path, data):
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temporary, path)

def parse_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

def refresh_status(collection, now=None):
    """Recalculate time-sensitive fields without waiting for a network scan."""
    now = now or datetime.now(timezone.utc)
    upcoming = []
    live = False
    for stage in collection.get("stages") or []:
        start = parse_time(stage.get("startTime"))
        end = parse_time(stage.get("endTime"))
        if collection.get("isSoldOut"):
            stage["status"] = "ENDED"
        elif start and now < start:
            stage["status"] = "UPCOMING"
            upcoming.append(start)
        elif start and start <= now and (end is None or now <= end):
            stage["status"] = "LIVE"
            live = True
        else:
            stage["status"] = "ENDED"
    collection["overallStatus"] = ("SOLD_OUT" if collection.get("isSoldOut") else
                                   "LIVE" if live else "UPCOMING" if upcoming else "ENDED")
    collection["earliestUpcoming"] = min(upcoming).isoformat() if upcoming else None
    collection["isPublicFree"] = any(s.get("isFree") and "public" in (s.get("label") or "").lower()
                                     for s in collection.get("stages") or [])
    return collection

def cache_snapshot():
    with cache_lock:
        snapshot = copy.deepcopy(scan_cache)
    now = datetime.now(timezone.utc)
    snapshot["collections"] = [refresh_status(c, now) for c in snapshot.get("collections", [])]
    updated = parse_time(snapshot.get("last_updated"))
    snapshot["age_seconds"] = max(0, int((now - updated).total_seconds())) if updated else None
    snapshot["stale"] = updated is None or snapshot["age_seconds"] > 900
    snapshot["scan"] = dict(scan_state)
    return snapshot

def start_scan(mode):
    if mode not in ("quick", "deep"):
        return False
    if not scan_lock.acquire(blocking=False):
        return False
    scan_state.update(running=True, started_at=datetime.now(timezone.utc).isoformat(),
                      finished_at=None, error=None, mode=mode, onchain_upcoming=0,
                      onchain_live=0, resolved=0, candidates=0, results=0, lookup_errors=0)
    def worker():
        try:
            run_deep_scan(mode)
        except Exception as exc:
            scan_state["error"] = str(exc)
            print(f"Scan failed: {exc}")
        finally:
            scan_state["running"] = False
            scan_state["finished_at"] = datetime.now(timezone.utc).isoformat()
            scan_lock.release()
    threading.Thread(target=worker, daemon=True).start()
    return True

if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            scan_cache = json.load(f)
    except Exception:
        pass

def fetch_onchain_contracts(blocks_back=1000000):
    """
    Directly scans the SeaDrop smart contract event logs on Robinhood Chain.
    Extracts all on-chain PublicDropUpdated events and identifies Upcoming & Live mints.
    """
    try:
        req = urllib.request.Request(
            ROBINHOOD_RPC,
            data=json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'eth_blockNumber', 'params': []}).encode(),
            headers={'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            curr_block = int(json.loads(resp.read().decode())['result'], 16)
    except Exception as e:
        print(f"Error getting block: {e}")
        raise RuntimeError(f"RPC block number unavailable: {e}") from e

    last_scanned = scan_cache.get("last_scanned_block")
    from_block = max(0, curr_block - blocks_back + 1)
    if isinstance(last_scanned, int) and last_scanned <= curr_block:
        from_block = max(from_block, last_scanned - 100)
    chunk_size = 10000
    num_chunks = (curr_block - from_block) // chunk_size + 1
    all_contracts = {}
    failed_chunks = 0

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Scanning {blocks_back} on-chain blocks in {num_chunks} chunks...")

    def get_logs(start, end):
        payload = {'jsonrpc': '2.0', 'id': 2, 'method': 'eth_getLogs', 'params': [{
            'address': SEADROP_CONTRACT, 'fromBlock': hex(start), 'toBlock': hex(end),
            'topics': [TOPIC_PUBLIC_DROP]}]}
        req = urllib.request.Request(ROBINHOOD_RPC, data=json.dumps(payload).encode(),
                                     headers={'Content-Type': 'application/json', 'User-Agent': 'MintRadar/2.0'})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                response = json.loads(resp.read().decode())
            if 'error' in response:
                raise RuntimeError(str(response['error']))
            if not isinstance(response.get('result'), list):
                raise RuntimeError('RPC returned no log list')
            return response['result']
        except Exception:
            if end - start < 999:
                raise
            middle = (start + end) // 2
            return get_logs(start, middle) + get_logs(middle + 1, end)

    for i in range(num_chunks):
        to_b = min(curr_block, from_block + (i + 1) * chunk_size - 1)
        from_b = from_block + i * chunk_size
        try:
            for l in get_logs(from_b, to_b):
                addr = "0x" + l['topics'][1][26:].lower()
                raw = bytes.fromhex(l['data'][2:])
                if len(raw) >= 96:
                    st = int.from_bytes(raw[32:64], 'big')
                    et = int.from_bytes(raw[64:96], 'big')
                    mint_price_wei = int.from_bytes(raw[0:32], 'big')
                    if addr not in all_contracts or int(l['blockNumber'], 16) > int(all_contracts[addr]['block'], 16):
                        all_contracts[addr] = {
                            'contract': addr,
                            'startTime': st,
                            'endTime': et,
                            'mintPriceEth': mint_price_wei / 1e18,
                            'block': l['blockNumber']
                        }
        except Exception as e:
            print(f"Error in chunk {i}: {e}")
            failed_chunks += 1
        time.sleep(0.03)

    if failed_chunks:
        raise RuntimeError(f"RPC scan incomplete: {failed_chunks}/{num_chunks} block ranges failed")

    now_ts = int(datetime.now(timezone.utc).timestamp())
    upcoming = [c for c in all_contracts.values() if c['startTime'] > now_ts]
    upcoming.sort(key=lambda x: x['startTime']) # soonest upcoming first

    live = [c for c in all_contracts.values() if c['startTime'] <= now_ts <= c['endTime']]
    live.sort(key=lambda x: -int(x['block'], 16)) # most recent block first

    print(f"[{datetime.now().strftime('%H:%M:%S')}] On-chain events found: {len(upcoming)} UPCOMING, {len(live)} LIVE.")
    return upcoming, live, curr_block

def resolve_slug(addr):
    """Resolves an on-chain NFT contract address to OpenSea collection slug."""
    url = f"https://opensea.io/assets/robinhood/{addr}"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
    try:
        with urllib.request.urlopen(req, timeout=6) as resp:
            final_url = resp.geturl()
            if "/collection/" in final_url:
                m = re.search(r'/collection/([a-zA-Z0-9-_]+)', final_url)
                if m:
                    return (addr, m.group(1))
    except Exception:
        pass
    return (addr, None)

def fetch_web_catalog_slugs():
    """Scrapes collection slugs from OpenSea Robinhood chain pages."""
    req = urllib.request.Request(
        ROBINHOOD_COLLECTIONS_URL,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
            slugs = set(re.findall(r'/collection/([a-zA-Z0-9-_]+)', html))
            ignore = {"create", "manage", "edit", "overview", "analytics", "activity", "drops", "chain"}
            return [s for s in slugs if s not in ignore and not s.startswith("chain/")]
    except Exception:
        return []

def inspect_slug(slug, hint_contract=None):
    """Queries OpenSea GraphQL for drop details of a given slug."""
    payload = {
        "query": DROP_QUERY,
        "variables": {"slug": slug}
    }
    req = urllib.request.Request(
        OPENSEA_GQL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-App-Id": "opensea-web",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    )
    try:
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=8) as resp:
                    raw = json.loads(resp.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as error:
                if error.code != 429 or attempt == 2:
                    raise
                time.sleep(min(20, int(error.headers.get("Retry-After", "2")) or 2))
        data = raw.get("data") or {}
        if raw.get("errors") and not data.get("dropBySlug"):
            raise RuntimeError(str(raw["errors"][0].get("message", "GraphQL error")))
        if not data:
            raise RuntimeError("OpenSea returned no data")
        if data:
            drop = data.get("dropBySlug")
            coll = data.get("collectionBySlug") or {}
            activity = (data.get("collectionActivity") or {}).get("items") or []
            
            contract_address = hint_contract
            if not contract_address:
                for item in activity:
                    img_url = (item.get("item") or {}).get("imageUrl") or ""
                    m = re.search(r'0x[a-fA-F0-9]{40}', img_url)
                    if m:
                        contract_address = m.group(0)
                        break

            if drop and drop.get("stages") and len(drop["stages"]) > 0:
                stages = drop["stages"]
                
                # Check supply and sold out status
                max_supply = drop.get("maxSupply")
                total_supply = drop.get("totalSupply")
                if drop.get("__typename") == "Erc1155SeaDropV2":
                    ts_list = drop.get("tokenSupply", [])
                    if ts_list:
                        total_supply = ts_list[0].get("totalSupply")
                        max_supply = ts_list[0].get("maxSupply")
                
                is_sold_out = False
                remaining_supply = None
                mint_percent = 0.0
                if max_supply is not None and total_supply is not None:
                    remaining_supply = max(0, max_supply - total_supply)
                    if max_supply > 0:
                        mint_percent = round((total_supply / max_supply) * 100, 1)
                    if total_supply >= max_supply and max_supply > 0:
                        is_sold_out = True
                
                # Analyze overall status
                now = datetime.now(timezone.utc)
                has_live = False
                has_upcoming = False
                earliest_upcoming = None
                has_free = False
                
                parsed_stages = []
                for st in stages:
                    st_time_str = st.get("startTime")
                    end_time_str = st.get("endTime")
                    
                    st_time = parse_time(st_time_str)
                    end_time = parse_time(end_time_str)
                    
                    price_data = st.get("price") or {}
                    price_usd = price_data.get("usd")
                    price_token = (price_data.get("token") or {}).get("unit")
                    token_sym = ((st.get("price") or {}).get("token") or {}).get("symbol") or "ETH"
                    
                    is_free = price_token is not None and float(price_token) == 0
                    if is_free:
                        has_free = True

                    stage_status = "ENDED"
                    if is_sold_out:
                        stage_status = "ENDED"
                    elif st_time and end_time:
                        if now < st_time:
                            stage_status = "UPCOMING"
                            has_upcoming = True
                            if not earliest_upcoming or st_time < earliest_upcoming:
                                earliest_upcoming = st_time
                        elif st_time <= now <= end_time:
                            stage_status = "LIVE"
                            has_live = True
                        else:
                            stage_status = "ENDED"
                    elif st_time and not end_time:
                        if now < st_time:
                            stage_status = "UPCOMING"
                            has_upcoming = True
                            if not earliest_upcoming or st_time < earliest_upcoming:
                                earliest_upcoming = st_time
                        else:
                            stage_status = "LIVE"
                            has_live = True
                            
                    parsed_stages.append({
                        "label": st.get("label") or f"Stage {st.get('stageIndex', 0)}",
                        "status": stage_status,
                        "startTime": st_time_str,
                        "endTime": end_time_str,
                        "isFree": is_free,
                        "priceUsd": round(float(price_usd), 3) if price_usd is not None else None,
                        "priceToken": price_token,
                        "tokenSymbol": token_sym,
                        "maxPerWallet": st.get("maxTotalMintableByWallet")
                    })
                
                overall_status = "ENDED"
                if is_sold_out:
                    overall_status = "SOLD_OUT"
                elif has_live:
                    overall_status = "LIVE"
                elif has_upcoming:
                    overall_status = "UPCOMING"
                
                twitter = coll.get("twitterUsername") or None
                discord = coll.get("discordUrl") or None
                website = coll.get("externalUrl") or None
                telegram = coll.get("telegramUrl") or None
                is_verified = bool(coll.get("isVerified"))
                desc_text = coll.get("description") or ""
                has_social = bool(twitter or discord or website or telegram or re.search(r'(t\.me|twitter\.com|x\.com|discord\.gg)', desc_text, re.I))

                is_public_free = any(st.get("isFree") and "public" in (st.get("label") or "").lower()
                                     for st in parsed_stages)

                return {
                    "slug": slug,
                    "name": coll.get("name") or slug.replace("-", " ").title(),
                    "description": desc_text,
                    "imageUrl": coll.get("imageUrl") or "",
                    "openseaUrl": f"https://opensea.io/collection/{slug}/overview",
                    "contractAddress": contract_address,
                    "blockscoutUrl": f"https://robinhoodchain.blockscout.com/address/{contract_address}" if contract_address else None,
                    "chain": "Robinhood Chain",
                    "chainIdentifier": "robinhood",
                    "overallStatus": overall_status,
                    "isSoldOut": is_sold_out,
                    "itemsMinted": total_supply,
                    "maxSupply": max_supply,
                    "remainingSupply": remaining_supply,
                    "mintPercent": mint_percent,
                    "hasFree": has_free,
                    "isPublicFree": is_public_free,
                    "earliestUpcoming": earliest_upcoming.isoformat() if earliest_upcoming else None,
                    "stages": parsed_stages,
                    "twitterUsername": twitter,
                    "discordUrl": discord,
                    "externalUrl": website,
                    "telegramUrl": telegram,
                    "isVerified": is_verified,
                    "hasSocial": has_social
                }
    except Exception as exc:
        scan_state["lookup_errors"] = scan_state.get("lookup_errors", 0) + 1
        print(f"Drop lookup failed for {slug}: {exc}")
    return None

def run_deep_scan(mode="deep"):
    """
    Executes a comprehensive deep scan:
    1. Direct on-chain SeaDrop logs (finds hidden & future scheduled drops up to days/weeks back).
    2. OpenSea web catalog slugs.
    3. Previously cached drops (preserves any future drops already discovered).
    4. Resolves and inspects all candidates in parallel.
    """
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] === STARTING DEEP ON-CHAIN SCAN ===")
    
    blocks = 1000000 if mode == "deep" else 30000
    upcoming_onchain, live_onchain, current_block = fetch_onchain_contracts(blocks_back=blocks)
    scan_state.update(onchain_upcoming=len(upcoming_onchain), onchain_live=len(live_onchain))
    
    # Priority contract resolution:
    # 1. All Upcoming contracts (these are the hidden future mints!)
    # 2. Top 40 recent Live contracts
    target_contracts = [c['contract'] for c in upcoming_onchain]
    for c in live_onchain[:40]:
        if c['contract'] not in target_contracts:
            target_contracts.append(c['contract'])

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Resolving {len(target_contracts)} priority on-chain contracts to OpenSea slugs...")
    slug_contract_map = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        resolved = list(executor.map(resolve_slug, target_contracts))
        for addr, slug in resolved:
            if slug:
                slug_contract_map[slug] = addr

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully resolved {len(slug_contract_map)} on-chain collections!")
    scan_state["resolved"] = len(slug_contract_map)

    # 3. Add catalog collections
    for s in fetch_web_catalog_slugs():
        if s not in slug_contract_map:
            slug_contract_map[s] = None

    # 4. Preserve existing future/active cached drops
    for c in scan_cache.get("collections", []):
        s = c.get("slug")
        addr = c.get("contractAddress")
        current = refresh_status(copy.deepcopy(c))
        if s and current.get("overallStatus") in ("LIVE", "UPCOMING") and s not in slug_contract_map:
            slug_contract_map[s] = addr

    # Baseline collections
    for b in ["hood-monarchs", "facetsart", "gta6-fan-collection", "rare-friends-genesis"]:
        if b not in slug_contract_map:
            slug_contract_map[b] = None

    all_slug_items = list(slug_contract_map.items())
    scan_state["candidates"] = len(all_slug_items)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inspecting drop stages & supply for {len(all_slug_items)} collections...")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(inspect_slug, slug, contract): slug for slug, contract in all_slug_items}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.append(res)

    # Sorting:
    # 0. LIVE (available, in stock)
    # 1. UPCOMING (not sold out, scheduled for future, sorted by soonest start)
    # 2. SOLD_OUT
    # 3. ENDED
    def sort_key(item):
        st = item["overallStatus"]
        if st == "LIVE" and not item["isSoldOut"]:
            return (0, -(item.get("remainingSupply") or 0))
        elif st == "UPCOMING" and not item["isSoldOut"]:
            return (1, item["earliestUpcoming"] or "9999")
        elif item["isSoldOut"]:
            return (2, 0)
        else:
            return (3, 0)

    results.sort(key=sort_key)
    scan_state["results"] = len(results)
    if not results:
        raise RuntimeError("No drop details returned; cache was preserved")
    if scan_state["lookup_errors"] > max(3, len(all_slug_items) // 10):
        raise RuntimeError(f"OpenSea lookup failed for {scan_state['lookup_errors']} collections; cache was preserved")

    with cache_lock:
        old_collections = scan_cache.get("collections", [])
        updated_at = datetime.now(timezone.utc).isoformat()
        scan_cache["last_updated"] = updated_at
        scan_cache["scan_mode"] = mode
        scan_cache["last_scanned_block"] = current_block
        scan_cache["collections"] = results
        atomic_json_write(CACHE_FILE, scan_cache)
        try:
            record_history(old_collections, results, updated_at)
        except Exception as exc:
            print(f"History write failed: {exc}")

    live_count = len([c for c in results if c['overallStatus'] == 'LIVE' and not c['isSoldOut']])
    upcoming_count = len([c for c in results if c['overallStatus'] == 'UPCOMING' and not c['isSoldOut']])
    sold_count = len([c for c in results if c['isSoldOut']])
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Deep Scan finished: {live_count} Live, {upcoming_count} Upcoming, {sold_count} Sold Out, Total: {len(results)}\n")
    return scan_cache

class MintRadarHandler(http.server.SimpleHTTPRequestHandler):
    def send_json(self, data, status=200):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length < 0 or length > 16384:
            raise ValueError("Invalid request size")
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        
        if parsed.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            html_file = os.path.join(os.path.dirname(__file__), "index.html")
            with open(html_file, "rb") as f:
                self.wfile.write(f.read())
            return

        if parsed.path == "/api/scan":
            q = urllib.parse.parse_qs(parsed.query)
            mode = q.get("mode", ["deep"])[0]
            if mode not in ("quick", "deep"):
                return self.send_json({"error": "Unknown scan mode"}, 400)
            started = start_scan(mode)
            self.send_json({"started": started, "scan": dict(scan_state)}, 202 if started else 409)
            return

        if parsed.path == "/api/cached":
            self.send_json(cache_snapshot())
            return

        if parsed.path == "/api/status":
            self.send_json(dict(scan_state))
            return

        if parsed.path == "/api/history":
            slug = urllib.parse.parse_qs(parsed.query).get("slug", [""])[0]
            if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", slug):
                return self.send_json({"error": "Invalid collection slug"}, 400)
            events = [e for e in load_history() if e.get("slug") == slug]
            self.send_json({"events": events[-30:][::-1]})
            return

        self.send_error(404, "Not found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/scan":
            try:
                mode = self.read_json().get("mode", "quick")
                if mode not in ("quick", "deep"):
                    return self.send_json({"error": "Unknown scan mode"}, 400)
                started = start_scan(mode)
                return self.send_json({"started": started, "scan": dict(scan_state)}, 202 if started else 409)
            except Exception as exc:
                return self.send_json({"error": str(exc)}, 400)

        if parsed.path == "/api/check-slug":
            try:
                data = self.read_json()
                target = data.get("slug", "").strip()
                if len(target) > 256:
                    return self.send_json({"error": "Identifier too long"}, 400)
                if "opensea.io/collection/" in target:
                    m = re.search(r'opensea\.io/collection/([a-zA-Z0-9-_]+)', target)
                    if m:
                        target = m.group(1)
                
                hint_contract = None
                if target.startswith("0x") and len(target) == 42:
                    hint_contract = target
                    _, resolved_slug = resolve_slug(hint_contract)
                    if resolved_slug:
                        target = resolved_slug

                if not re.fullmatch(r"[a-zA-Z0-9_-]{1,128}", target):
                    return self.send_json({"error": "Invalid collection slug"}, 400)

                result = inspect_slug(target, hint_contract)
                self.send_json({"success": True, "data": refresh_status(result) if result else None})
            except Exception as e:
                self.send_json({"success": False, "error": str(e)}, 400)
            return
            
        self.send_response(404)
        self.end_headers()

def start_server():
    http.server.ThreadingHTTPServer.allow_reuse_address = True
    with http.server.ThreadingHTTPServer(("127.0.0.1", PORT), MintRadarHandler) as httpd:
        print(f"\n" + "="*60)
        print(f"🚀 OpenSea Robinhood Mint Radar (Deep Scanner Engine) is running!")
        print(f"🌐 Open in your browser: http://localhost:{PORT}")
        print(f"="*60 + "\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopping server...")
            httpd.server_close()

if __name__ == "__main__":
    start_server()
