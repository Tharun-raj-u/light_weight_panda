"""Test suite for LinkedIn Profile Finder API v5.0."""
import httpx
import time
import sys
import urllib.parse

# Must match linkedin_finder.OPNXNG_SEARCH_URL (primary engine: OpnXNG).
EXPECTED_OPNXNG_SEARCH = "https://opnxng.com/search"

BASE = "http://localhost:8888"
passed = 0
failed = 0

# Compose starts containers before uvicorn is accepting; wait before tests.
WAIT_API_SEC = 120


def wait_for_api() -> bool:
    print(f"\n[0] Waiting for API at {BASE} (up to {WAIT_API_SEC}s)…")
    t0 = time.time()
    deadline = t0 + WAIT_API_SEC
    while time.time() < deadline:
        try:
            r = httpx.get(f"{BASE}/health", timeout=3)
            if r.status_code == 200:
                print(f"  ✅ API ready  ({int(time.time() - t0)}s elapsed)")
                return True
        except Exception:
            pass
        time.sleep(0.75)
    print("  ❌ Timed out — is `docker compose up -d` running and port 8888 published?")
    return False


def report(name: str, ok: bool, detail: str = ""):
    global passed, failed
    if ok:
        passed += 1
        print(f"  ✅ {name}: PASSED  {detail}")
    else:
        failed += 1
        print(f"  ❌ {name}: FAILED  {detail}")


print("=" * 62)
print("  LinkedIn Profile Finder API v5.0 — Test Suite")
print(f"  OpnXNG search: {EXPECTED_OPNXNG_SEARCH}?q=…")
print("=" * 62)

if not wait_for_api():
    sys.exit(1)

# ---- 1: Health ----
print("\n[1] GET /health")
try:
    r = httpx.get(f"{BASE}/health", timeout=10)
    d = r.json()
    eng = d.get("engines") or []
    ok = (
        r.status_code == 200
        and "opnxng" in eng
        and "yahoo" in eng
    )
    report("/health", ok,
           f"browser={d.get('browser_available')}  engines={eng}")
except Exception as e:
    report("/health", False, str(e))

# ---- 2: Swagger docs ----
print("\n[2] GET /docs")
try:
    r = httpx.get(f"{BASE}/docs", timeout=10)
    report("/docs", r.status_code == 200, f"{len(r.text)} chars")
except Exception as e:
    report("/docs", False, str(e))

# ---- 3: OpenAPI schema ----
print("\n[3] GET /openapi.json")
try:
    r = httpx.get(f"{BASE}/openapi.json", timeout=10)
    d = r.json()
    paths = list(d["paths"].keys())
    report("/openapi.json", r.status_code == 200, f"paths={paths}")
except Exception as e:
    report("/openapi.json", False, str(e))

# ---- 4: POST /search — Satya Nadella ----
print("\n[4] POST /search — Satya Nadella at Microsoft")
try:
    r = httpx.post(f"{BASE}/search", json={
        "name": "Satya Nadella",
        "company": "Microsoft",
        "location": "Seattle",
    }, timeout=30)
    d = r.json()
    url = d.get("linkedin_url") or ""
    ok = r.status_code == 200 and "linkedin.com/in/" in url
    report("search (Nadella)", ok,
           f"url={url}  conf={d.get('confidence')}  engine={d.get('engine')}  {d.get('search_time_ms')}ms")
except Exception as e:
    report("search (Nadella)", False, str(e))

# ---- 5: POST /search — Sundar Pichai ----
print("\n[5] POST /search — Sundar Pichai at Google")
try:
    r = httpx.post(f"{BASE}/search", json={
        "name": "Sundar Pichai",
        "company": "Google",
    }, timeout=30)
    d = r.json()
    url = d.get("linkedin_url") or ""
    ok = r.status_code == 200 and "linkedin.com/in/" in url
    report("search (Pichai)", ok, f"url={url}  conf={d.get('confidence')}")
except Exception as e:
    report("search (Pichai)", False, str(e))

# ---- 6: Cache hit ----
print("\n[6] POST /search — cache hit (repeat Nadella)")
try:
    t0 = time.time()
    r = httpx.post(f"{BASE}/search", json={
        "name": "Satya Nadella",
        "company": "Microsoft",
        "location": "Seattle",
    }, timeout=10)
    elapsed = int((time.time() - t0) * 1000)
    d = r.json()
    ok = r.status_code == 200 and d.get("cached") is True and d.get("search_time_ms", 9999) < 50
    report("cache hit", ok,
           f"cached={d.get('cached')}  search_time={d.get('search_time_ms')}ms  http={elapsed}ms")
except Exception as e:
    report("cache hit", False, str(e))

# ---- 7: POST /search/custom — raw query ----
print("\n[7] POST /search/custom — raw query")
try:
    r = httpx.post(f"{BASE}/search/custom", json={
        "query": "Satya Nadella CEO Microsoft linkedin.com/in",
    }, timeout=30)
    d = r.json()
    url = d.get("linkedin_url") or ""
    all_urls = d.get("all_linkedin_urls", [])
    ok = r.status_code == 200 and "linkedin.com/in/" in url
    report("custom search", ok,
           f"url={url}  all_count={len(all_urls)}  {d.get('search_time_ms')}ms")
except Exception as e:
    report("custom search", False, str(e))

# ---- 8: POST /search/batch — 3 people ----
print("\n[8] POST /search/batch — 3 people")
try:
    r = httpx.post(f"{BASE}/search/batch", json={
        "queries": [
            {"name": "Tim Cook", "company": "Apple", "location": "Cupertino"},
            {"name": "Jensen Huang", "company": "NVIDIA"},
            {"name": "Satya Nadella", "company": "Microsoft", "location": "Seattle"},
        ]
    }, timeout=60)
    d = r.json()
    ok = r.status_code == 200 and d.get("total") == 3
    report("batch search", ok,
           f"total={d.get('total')}  found={d.get('found')}  {d.get('total_time_ms')}ms")
    for item in d.get("results", []):
        u = item.get("linkedin_url") or "(not found)"
        print(f"       {item['name']:20s}  {item.get('confidence',''):6s}  {u}")
except Exception as e:
    report("batch search", False, str(e))

# ---- 9: Cache stats ----
print("\n[9] GET /cache/stats")
try:
    r = httpx.get(f"{BASE}/cache/stats", timeout=10)
    d = r.json()
    ok = r.status_code == 200 and d.get("size", 0) > 0
    report("cache stats", ok, f"size={d.get('size')}  max={d.get('max_size')}")
except Exception as e:
    report("cache stats", False, str(e))

# ---- 10: Validation error — missing name ----
print("\n[10] POST /search — missing required 'name'")
try:
    r = httpx.post(f"{BASE}/search", json={"company": "Microsoft"}, timeout=10)
    ok = r.status_code == 422
    report("validation error", ok, f"status={r.status_code}")
except Exception as e:
    report("validation error", False, str(e))

# ---- 11: Name-only ----
print("\n[11] POST /search — name only")
try:
    r = httpx.post(f"{BASE}/search", json={"name": "Elon Musk"}, timeout=30)
    d = r.json()
    url = d.get("linkedin_url") or ""
    ok = r.status_code == 200
    report("name-only", ok, f"url={url}  conf={d.get('confidence')}")
except Exception as e:
    report("name-only", False, str(e))

# ---- 12: OpnXNG URL in code + live https://opnxng.com/search ----
print("\n[12] OpnXNG — configured URL matches & public search responds")
try:
    from linkedin_finder import OPNXNG_SEARCH_URL

    ok_cfg = OPNXNG_SEARCH_URL.rstrip("/") == EXPECTED_OPNXNG_SEARCH.rstrip("/")
    probe_url = f"{OPNXNG_SEARCH_URL.rstrip('/')}?q={urllib.parse.quote('linkedin test')}"
    r = httpx.get(probe_url, timeout=20, follow_redirects=True)
    # Any HTTP status from the server (e.g. 200 HTML, 429 rate limit) means the endpoint is reachable.
    ok_http = r.status_code < 600
    ok = ok_cfg and ok_http
    snippet = (r.text or "")[:120].replace("\n", " ")
    report(
        "OpnXNG search URL",
        ok,
        f"url={OPNXNG_SEARCH_URL}  GET {probe_url[:56]}…  "
        f"http={r.status_code}  bytes={len(r.content)}  head={snippet!r}",
    )
except Exception as e:
    report("OpnXNG search URL", False, str(e))

# ---- Summary ----
total = passed + failed
print("\n" + "=" * 62)
print(f"  Results: {passed}/{total} passed, {failed} failed")
if failed == 0:
    print("  🎉 All tests passed!")
else:
    print("  ⚠️  Some tests failed")
print("=" * 62)

sys.exit(0 if failed == 0 else 1)
