"""Test suite for the Search API."""
import httpx
import json
import sys

BASE = "http://localhost:8888"
passed = 0
failed = 0


def report(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"  ✅ {name}: PASSED  {detail}")
    else:
        failed += 1
        print(f"  ❌ {name}: FAILED  {detail}")


print("=" * 60)
print("  Search API — Test Suite")
print("=" * 60)

# --- Test 1: Health ---
print("\n[1] GET /health")
try:
    r = httpx.get(f"{BASE}/health", timeout=10)
    data = r.json()
    report("/health", r.status_code == 200, f"status={data['status']}")
except Exception as e:
    report("/health", False, str(e))

# --- Test 2: Swagger docs ---
print("\n[2] GET /docs")
try:
    r = httpx.get(f"{BASE}/docs", timeout=10)
    report("/docs", r.status_code == 200, f"{len(r.text)} chars")
except Exception as e:
    report("/docs", False, str(e))

# --- Test 3: OpenAPI schema ---
print("\n[3] GET /openapi.json")
try:
    r = httpx.get(f"{BASE}/openapi.json", timeout=10)
    data = r.json()
    paths = list(data.get("paths", {}).keys())
    report("/openapi.json", r.status_code == 200, f"endpoints={paths}")
except Exception as e:
    report("/openapi.json", False, str(e))

# --- Test 4: Search (DuckDuckGo) ---
print("\n[4] GET /search?q=python+programming&engine=duckduckgo")
try:
    r = httpx.get(
        f"{BASE}/search",
        params={"q": "python programming", "num": "5", "engine": "duckduckgo"},
        timeout=30,
    )
    data = r.json()
    n = data.get("total_results", 0)
    eng = data.get("engine", "?")
    ok = r.status_code == 200 and n > 0
    report("/search (duckduckgo)", ok, f"engine={eng}, results={n}")
    if ok:
        for item in data["results"][:3]:
            print(f"       #{item['position']}: {item['title'][:70]}")
            print(f"           {item['url'][:80]}")
except Exception as e:
    report("/search (duckduckgo)", False, str(e))

# --- Test 5: Search (Google) ---
print("\n[5] GET /search?q=hello+world&engine=google")
try:
    r = httpx.get(
        f"{BASE}/search",
        params={"q": "hello world", "num": "5", "engine": "google"},
        timeout=30,
    )
    data = r.json()
    n = data.get("total_results", 0)
    eng = data.get("engine", "?")
    ok = r.status_code == 200
    report("/search (google)", ok, f"engine={eng}, results={n}")
    if n > 0:
        for item in data["results"][:3]:
            print(f"       #{item['position']}: {item['title'][:70]}")
except Exception as e:
    report("/search (google)", False, str(e))

# --- Test 6: Search (auto mode) ---
print("\n[6] GET /search?q=fastapi+tutorial")
try:
    r = httpx.get(
        f"{BASE}/search",
        params={"q": "fastapi tutorial", "num": "3"},
        timeout=30,
    )
    data = r.json()
    n = data.get("total_results", 0)
    eng = data.get("engine", "?")
    ok = r.status_code == 200 and n > 0
    report("/search (auto)", ok, f"engine={eng}, results={n}")
except Exception as e:
    report("/search (auto)", False, str(e))

# --- Test 7: Fetch ---
print("\n[7] GET /fetch?url=https://example.com")
try:
    r = httpx.get(
        f"{BASE}/fetch",
        params={"url": "https://example.com"},
        timeout=15,
    )
    data = r.json()
    title = data.get("title", "")
    ok = r.status_code == 200 and title == "Example Domain"
    report("/fetch", ok, f"title='{title}'")
except Exception as e:
    report("/fetch", False, str(e))

# --- Test 8: Error handling (invalid engine) ---
print("\n[8] GET /search?q=test&engine=invalid")
try:
    r = httpx.get(
        f"{BASE}/search",
        params={"q": "test", "engine": "invalid"},
        timeout=10,
    )
    ok = r.status_code == 400
    report("error handling (bad engine)", ok, f"status={r.status_code}")
except Exception as e:
    report("error handling", False, str(e))

# --- Summary ---
total = passed + failed
print("\n" + "=" * 60)
print(f"  Results: {passed}/{total} passed, {failed} failed")
if failed == 0:
    print("  🎉 All tests passed!")
else:
    print("  ⚠️  Some tests failed")
print("=" * 60)

sys.exit(0 if failed == 0 else 1)
