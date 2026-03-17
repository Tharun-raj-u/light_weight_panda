"""Quick benchmark — measures cached & uncached RPS against the API."""
import asyncio
import time
import httpx

BASE = "http://localhost:8888"


async def timed_batch(client, url, n, json_body=None):
    """Fire *n* requests, return (ok, limited, errors, elapsed)."""
    async def one():
        try:
            if json_body:
                r = await client.post(url, json=json_body)
            else:
                r = await client.get(url)
            return r.status_code
        except Exception:
            return 0

    t0 = time.perf_counter()
    codes = await asyncio.gather(*(one() for _ in range(n)))
    elapsed = time.perf_counter() - t0

    ok = sum(1 for c in codes if 200 <= c < 300)
    limited = sum(1 for c in codes if c == 429)
    errs = n - ok - limited
    rps = ok / elapsed if elapsed > 0 else 0
    print(f"   {ok:>4d} OK  |  {limited:>3d} rate-limited  |  {errs:>3d} errors  |  {elapsed:.2f}s  →  {rps:.0f} RPS")
    return ok, limited, errs, elapsed


async def main():
    limits = httpx.Limits(max_connections=300, max_keepalive_connections=100)
    async with httpx.AsyncClient(timeout=60, verify=False, limits=limits) as c:

        # Warm cache
        print("Warming cache (1 request)...")
        await c.post(f"{BASE}/search", json={"name": "Satya Nadella", "company": "Microsoft"})

        # Test 1: Cached — increasing concurrency
        for n in [50, 100, 200]:
            print(f"\n[Cached] {n} concurrent requests (same query):")
            await timed_batch(c, f"{BASE}/search", n,
                              json_body={"name": "Satya Nadella", "company": "Microsoft"})

        # Test 2: Health endpoint (lightweight)
        for n in [50, 100]:
            print(f"\n[Health] {n} concurrent requests:")
            await timed_batch(c, f"{BASE}/health", n)

        # Test 3: Uncached batch — 10 unique people
        print("\n[Batch] 10 unique uncached people (real search):")
        people = [
            {"name": "Bill Gates", "company": "Microsoft"},
            {"name": "Mark Zuckerberg", "company": "Meta"},
            {"name": "Jeff Bezos", "company": "Amazon"},
            {"name": "Lisa Su", "company": "AMD"},
            {"name": "Pat Gelsinger", "company": "Intel"},
            {"name": "Andy Jassy", "company": "Amazon"},
            {"name": "Arvind Krishna", "company": "IBM"},
            {"name": "Brad Smith", "company": "Microsoft"},
            {"name": "Ruth Porat", "company": "Alphabet"},
            {"name": "Amy Hood", "company": "Microsoft"},
        ]
        t0 = time.perf_counter()
        r = await c.post(f"{BASE}/search/batch", json={"queries": people}, timeout=120)
        elapsed = time.perf_counter() - t0
        data = r.json()
        print(f"   Found: {data['found']}/{data['total']}  |  {elapsed:.2f}s  →  {data['total']/elapsed:.1f} lookups/s")
        for res in data["results"]:
            tag = "✅" if res["linkedin_url"] else "❌"
            print(f"     {tag} {res['name']:20s} {res.get('confidence',''):6s} {res.get('linkedin_url','N/A')}")

        # Test 4: Cached repeat of batch
        print("\n[Batch cached] Same 10 people (now cached):")
        t0 = time.perf_counter()
        r = await c.post(f"{BASE}/search/batch", json={"queries": people})
        elapsed = time.perf_counter() - t0
        data = r.json()
        cached = sum(1 for res in data["results"] if res["cached"])
        print(f"   Cached: {cached}/{data['total']}  |  {elapsed:.3f}s  →  {data['total']/elapsed:.0f} lookups/s")


if __name__ == "__main__":
    asyncio.run(main())
