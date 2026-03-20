"""
LinkedIn Profile Finder API  —  v5.0  (Go CDP proxy + multi-browser)
=====================================================================

Accepts a person's name (+ optional company / location) and returns the
most-likely LinkedIn profile URL with a confidence score.

Architecture
------------
  Client  →  FastAPI (:8888)  →  Go CDP Proxy (:9333)  →  N × Lightpanda browsers

Search flow
-----------
  1. Check in-memory TTL cache → hit = instant return.
  2. Build query: "{name} LinkedIn {company} {location}".
  3. Race DuckDuckGo & Startpage in parallel via ``asyncio.wait(FIRST_COMPLETED)``.
     First engine to return a HIGH-confidence match wins; the loser is cancelled.
  4. Each engine URL is fetched as ``POST /fetch`` to the Go CDP proxy, which
     round-robins across 10 Lightpanda browser containers (30 concurrent
     sessions each = 300 total capacity).
  5. Parse HTML with BeautifulSoup + lxml; extract ``linkedin.com/in/`` URLs.
  6. Score candidates with word-boundary name matching + company match.
  7. If no result, retry with ``site:`` and keyword-style queries (3 passes).
  8. Cache result for 1 hour.

Endpoints
---------
  POST /search          — lookup by name / company / location
  POST /search/custom   — raw query string
  POST /search/batch    — up to 100 concurrent lookups
  GET  /health          — status, cache size, uptime
  GET  /cache/stats     — cache statistics
  DELETE /cache         — clear cache
  GET  /docs            — Swagger UI

Quick start
-----------
  docker compose up -d --build          # 10 browsers + Go proxy
  pip install fastapi uvicorn httpx beautifulsoup4 cachetools lxml
  python linkedin_finder.py             # API on http://localhost:8888
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
import urllib.parse
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import uvicorn
from bs4 import BeautifulSoup
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ============================================================================
# Configuration
# ============================================================================

API_PORT = 8888

import os as _os
CDP_PROXY_URL = _os.environ.get("CDP_PROXY_URL", "http://127.0.0.1:9333")
CDP_FETCH_TIMEOUT = 10.0                  # seconds per page load

CACHE_MAX_SIZE = 10_000
CACHE_TTL_SECONDS = 3600

# ============================================================================
# Pydantic Models  — Request / Response
# ============================================================================


class SearchRequest(BaseModel):
    """Single LinkedIn lookup request."""
    name: str = Field(
        ..., min_length=1, max_length=200,
        description="Person's full name",
        examples=["Satya Nadella"],
    )
    company: str = Field(
        "", max_length=200,
        description="Company name (optional but recommended)",
        examples=["Microsoft"],
    )
    location: str = Field(
        "", max_length=200,
        description="City / region (optional)",
        examples=["Seattle"],
    )


class CustomSearchRequest(BaseModel):
    """Free-form search — you supply the exact query string."""
    query: str = Field(
        ..., min_length=1, max_length=500,
        description="Raw search query sent to the engines as-is",
        examples=["Satya Nadella CEO Microsoft linkedin.com/in"],
    )


class LinkedInResult(BaseModel):
    """Result for a single person lookup."""
    name: str
    company: str
    location: str
    linkedin_url: Optional[str] = None
    profile_name: Optional[str] = None
    profile_headline: Optional[str] = None
    confidence: str = "low"
    engine: Optional[str] = None
    cached: bool = False
    search_time_ms: int = 0


class CustomSearchResult(BaseModel):
    """Result for a custom / raw query."""
    query: str
    linkedin_url: Optional[str] = None
    all_linkedin_urls: list[str] = []
    confidence: str = "low"
    engine: Optional[str] = None
    cached: bool = False
    search_time_ms: int = 0


class BatchRequest(BaseModel):
    """Batch of up to 100 lookup requests."""
    queries: list[SearchRequest] = Field(..., min_length=1, max_length=100)


class BatchResponse(BaseModel):
    total: int
    found: int
    results: list[LinkedInResult]
    total_time_ms: int


class HealthResponse(BaseModel):
    status: str
    cache_size: int
    cache_max: int
    uptime_seconds: int
    engines: list[str]
    browser_available: bool


# ============================================================================
# Global State
# ============================================================================

_proxy_client: Optional[httpx.AsyncClient] = None
_cache: TTLCache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL_SECONDS)
_cache_lock: asyncio.Lock = asyncio.Lock()
_start_time: float = 0
_browser_available: bool = False
_browser_check_time: float = 0
_BROWSER_CHECK_INTERVAL = 30.0   # re-probe Lightpanda every 30 s


def _cache_key(*parts: str) -> str:
    raw = "|".join(p.lower().strip() for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


# ============================================================================
# LinkedIn URL helpers
# ============================================================================

_LINKEDIN_PROFILE = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/in/([a-zA-Z0-9\-_%]+)", re.I,
)
_LINKEDIN_COMPANY = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/company/", re.I,
)


def _extract_linkedin_urls(text: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for slug in _LINKEDIN_PROFILE.findall(text):
        slug = slug.rstrip("/").split("?")[0]
        url = f"https://www.linkedin.com/in/{slug}"
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _clean_linkedin_url(raw: str) -> Optional[str]:
    m = _LINKEDIN_PROFILE.search(raw)
    if not m:
        return None
    slug = m.group(1).rstrip("/").split("?")[0]
    return f"https://www.linkedin.com/in/{slug}"


def _name_hit_ratio(name: str, text: str) -> tuple[int, int, float]:
    """Return (hits, total, ratio) — counts name parts that appear in *text*.

    Uses word-boundary matching so 'babu' doesn't match inside 'dsbabu'.
    """
    parts = name.lower().split()
    if not parts:
        return 0, 0, 0.0
    low = text.lower()
    hits = 0
    for p in parts:
        # word-boundary check: avoids substring false positives
        if re.search(r'(?<![a-z])' + re.escape(p) + r'(?![a-z])', low):
            hits += 1
    return hits, len(parts), hits / len(parts)


def _compute_confidence(
    name: str, company: str,
    title_snippet: str, url: str = "",
) -> str:
    """Score a candidate.  *title_snippet* is title + snippet text only;
    *url* is scored separately so URL-substring matches don't inflate
    the name confidence."""
    hits, total, ratio = _name_hit_ratio(name, title_snippet)
    co_hit = bool(company) and company.lower() in title_snippet.lower()

    # If nothing matches in title/snippet, try the URL slug as a last resort
    # but cap at 'low' — URL substrings are unreliable.
    if hits == 0:
        url_hits, _, url_ratio = _name_hit_ratio(name, url)
        if url_hits == 0:
            return "none"          # no match at all
        return "low"               # URL-only match

    # At least some name parts matched in title/snippet
    if ratio >= 0.5 and co_hit:
        return "high"
    if hits >= 2 and ratio >= 0.5:
        return "high" if co_hit else "medium"
    if hits >= 1 and co_hit:
        return "medium"
    if hits >= 1 and total <= 2:
        return "medium"            # short names (e.g. 2 parts) — 1 hit OK
    return "low"


def _build_query(name: str, company: str, location: str) -> str:
    parts = [name, "LinkedIn"]
    if company:
        parts.append(company)
    if location:
        parts.append(location)
    return " ".join(parts)


# ============================================================================
# Go CDP proxy helper
# ============================================================================


async def _cdp_fetch_html(url: str, timeout: float = CDP_FETCH_TIMEOUT) -> str:
    """Fetch rendered HTML via the Go CDP proxy (round-robins across browsers)."""
    resp = await _proxy_client.post(
        f"{CDP_PROXY_URL}/fetch",
        json={"url": url, "timeout": timeout},
        timeout=timeout + 3,
    )
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"CDP proxy: {data['error']}")
    return data.get("html", "")


# ============================================================================
# Search backends — Browser only (Lightpanda CDP)
# ============================================================================


async def _search_duckduckgo(query: str) -> list[dict]:
    """Search DuckDuckGo via the Lightpanda browser."""
    url = f"https://duckduckgo.com/?q={urllib.parse.quote(query)}&t=h_&ia=web"
    return _parse_duckduckgo_html(await _cdp_fetch_html(url))


async def _search_startpage(query: str) -> list[dict]:
    """Search Startpage via the Lightpanda browser."""
    url = f"https://www.startpage.com/sp/search?query={urllib.parse.quote(query)}&cat=web"
    return _parse_startpage_html(await _cdp_fetch_html(url))


async def _safe_search(fn, query: str) -> list[dict]:
    try:
        return await fn(query)
    except Exception:
        return []


_ENGINES: list[tuple[str, object]] = [
    ("duckduckgo", _search_duckduckgo),
    ("startpage", _search_startpage),
]


async def _race_engines(
    name: str, company: str, query: str,
) -> Optional[dict]:
    """Launch engines concurrently; return early on first HIGH-confidence hit.

    Instead of waiting for every engine to finish (``asyncio.gather``), this
    uses ``asyncio.wait(FIRST_COMPLETED)`` so the moment *any* engine yields
    a HIGH-confidence candidate the remaining tasks are cancelled and we
    return immediately.  If no single engine gives HIGH, all collected
    results are merged and the best candidate is returned.
    """
    pending: dict[asyncio.Task, str] = {}
    for ename, fn in _ENGINES:
        t = asyncio.create_task(_safe_search(fn, query))
        pending[t] = ename

    collected: list[tuple[str, list[dict]]] = []
    best: Optional[dict] = None

    try:
        while pending:
            done, _ = await asyncio.wait(
                pending.keys(), return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                ename = pending.pop(task)
                results = task.result()
                collected.append((ename, results))
                candidate = _pick_best_linkedin(name, company, (ename, results))
                if candidate and candidate["confidence"] == "high":
                    best = candidate
                    # HIGH found — cancel remaining tasks and return
                    for t in pending:
                        t.cancel()
                    return best
    finally:
        for t in pending:
            t.cancel()

    # No single engine gave HIGH — combine all results
    if collected:
        best = _pick_best_linkedin(name, company, *collected)
    return best


# ============================================================================
# HTML parsers
# ============================================================================


def _parse_duckduckgo_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    for item in soup.select(".result.results_links"):
        title_el = item.select_one(".result__a")
        snippet_el = item.select_one(".result__snippet")
        if not title_el:
            continue
        href = title_el.get("href", "")
        if "uddg=" in href:
            m = re.search(r"uddg=([^&]+)", href)
            if m:
                href = urllib.parse.unquote(m.group(1))
        results.append({
            "title": title_el.get_text(strip=True),
            "url": href,
            "snippet": snippet_el.get_text(strip=True) if snippet_el else "",
        })

    if not results:
        for a in soup.select("a[data-testid='result-title-a']"):
            href = a.get("href", "")
            if href:
                results.append({"title": a.get_text(strip=True), "url": href, "snippet": ""})

    if not results:
        for a in soup.select("a[href*='linkedin.com/in/']"):
            href = a.get("href", "")
            if "uddg=" in href:
                m = re.search(r"uddg=([^&]+)", href)
                if m:
                    href = urllib.parse.unquote(m.group(1))
            results.append({"title": a.get_text(strip=True) or href, "url": href, "snippet": ""})

    if not results:
        for li_url in _extract_linkedin_urls(html):
            results.append({"title": "", "url": li_url, "snippet": ""})

    return results


def _parse_startpage_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    results: list[dict] = []

    for item in soup.select(".result"):
        link_el = item.select_one("a.result-link") or item.find("a", href=True)
        if not link_el:
            continue
        href = link_el.get("href", "")
        if not href or href.startswith(("#", "javascript")):
            continue
        title_el = item.select_one(".result-title, h3")
        title = title_el.get_text(strip=True) if title_el else link_el.get_text(strip=True)
        snippet_el = item.select_one(".result-snippet, p")
        snippet = snippet_el.get_text(strip=True) if snippet_el else ""
        results.append({"title": title, "url": href, "snippet": snippet})

    if not results:
        for item in soup.select(".w-gl__result"):
            link_el = item.select_one("a[href]")
            if link_el:
                results.append({"title": link_el.get_text(strip=True),
                                "url": link_el.get("href", ""), "snippet": ""})

    if not results:
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "linkedin.com/in/" in href and href not in seen:
                seen.add(href)
                results.append({"title": a.get_text(strip=True) or href,
                                "url": href, "snippet": ""})
    return results


# ============================================================================
# Core lookup logic
# ============================================================================


def _pick_best_linkedin(
    name: str, company: str,
    *engine_results: tuple[str, list[dict]],
) -> Optional[dict]:
    """Pick the best LinkedIn candidate from search engine results."""
    candidates: list[dict] = []
    for engine, results in engine_results:
        for r in results:
            url, title, snippet = r.get("url", ""), r.get("title", ""), r.get("snippet", "")
            full = f"{title} {snippet} {url}"
            if _LINKEDIN_COMPANY.search(url):
                continue
            li_url = _clean_linkedin_url(url)
            if not li_url:
                found = _extract_linkedin_urls(full)
                li_url = found[0] if found else None
            if not li_url:
                continue
            title_snippet = f"{title} {snippet}"
            conf = _compute_confidence(name, company, title_snippet, url)
            if conf == "none":
                continue               # no name match at all — skip
            pname = None
            if " - " in title:
                pname = title.split(" - ")[0].strip()
            elif " | " in title:
                pname = title.split(" | ")[0].strip()
            candidates.append({
                "url": li_url, "profile_name": pname,
                "snippet": (snippet or title)[:300],
                "confidence": conf, "engine": engine,
            })
    if not candidates:
        return None
    prio = {"high": 0, "medium": 1, "low": 2}
    candidates.sort(key=lambda c: prio.get(c["confidence"], 3))
    return candidates[0]


async def _find_linkedin(name: str, company: str, location: str) -> LinkedInResult:
    """Multi-pass search across engines with early-return on HIGH."""
    t0 = time.monotonic()

    ck = _cache_key(name, company, location)
    async with _cache_lock:
        cached = _cache.get(ck)
    if cached is not None:
        return LinkedInResult(**{**cached, "cached": True,
                                 "search_time_ms": int((time.monotonic() - t0) * 1000)})

    # Pass 1 — broad: "{name} LinkedIn {company} {location}"
    query = _build_query(name, company, location)
    best = await _race_engines(name, company, query)

    # Pass 2 — site: operator
    if best is None:
        sq = f"site:linkedin.com/in/ {name}" + (f" {company}" if company else "")
        best = await _race_engines(name, company, sq)

    # Pass 3 — linkedin.com/in as keyword (best for lesser-known people)
    if best is None:
        parts = [name]
        if company:
            parts.append(company)
        if location:
            parts.append(location)
        parts.append("linkedin.com/in")
        kq = " ".join(parts)
        best = await _race_engines(name, company, kq)

    elapsed = int((time.monotonic() - t0) * 1000)
    result = LinkedInResult(
        name=name, company=company, location=location,
        linkedin_url=best["url"] if best else None,
        profile_name=best.get("profile_name") if best else None,
        profile_headline=best.get("snippet") if best else None,
        confidence=best["confidence"] if best else "low",
        engine=best.get("engine") if best else None,
        cached=False, search_time_ms=elapsed,
    )

    data = result.model_dump()
    data.pop("cached"); data.pop("search_time_ms")
    async with _cache_lock:
        _cache[ck] = data
    return result


async def _find_linkedin_custom(query: str) -> CustomSearchResult:
    """Run a raw user-supplied query through all engines."""
    t0 = time.monotonic()

    ck = _cache_key("__custom__", query)
    async with _cache_lock:
        cached = _cache.get(ck)
    if cached is not None:
        return CustomSearchResult(**{**cached, "cached": True,
                                     "search_time_ms": int((time.monotonic() - t0) * 1000)})

    ddg, sp = await asyncio.gather(
        _safe_search(_search_duckduckgo, query),
        _safe_search(_search_startpage, query),
    )

    all_urls: list[str] = []
    best_url: Optional[str] = None
    best_conf: str = "low"
    best_engine: Optional[str] = None

    for engine, results in [("duckduckgo", ddg), ("startpage", sp)]:
        for r in results:
            url = r.get("url", "")
            if _LINKEDIN_COMPANY.search(url):
                continue
            li = _clean_linkedin_url(url)
            if not li:
                found = _extract_linkedin_urls(f"{r.get('title','')} {r.get('snippet','')} {url}")
                li = found[0] if found else None
            if li and li not in all_urls:
                all_urls.append(li)
                if best_url is None:
                    best_url = li
                    best_engine = engine
                    best_conf = "medium"

    elapsed = int((time.monotonic() - t0) * 1000)
    result = CustomSearchResult(
        query=query,
        linkedin_url=best_url,
        all_linkedin_urls=all_urls,
        confidence=best_conf if best_url else "low",
        engine=best_engine,
        cached=False, search_time_ms=elapsed,
    )

    data = result.model_dump()
    data.pop("cached"); data.pop("search_time_ms")
    async with _cache_lock:
        _cache[ck] = data
    return result


# ============================================================================
# FastAPI App
# ============================================================================


async def _check_browser() -> bool:
    """Check if the Go CDP proxy is reachable."""
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get(f"{CDP_PROXY_URL}/health")
            return r.status_code == 200
    except Exception:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _proxy_client, _start_time, _browser_available
    _start_time = time.time()
    _proxy_client = httpx.AsyncClient(
        timeout=httpx.Timeout(CDP_FETCH_TIMEOUT + 5),
        limits=httpx.Limits(max_connections=300, max_keepalive_connections=100),
    )
    _browser_available = await _check_browser()

    print()
    print("=" * 62)
    print("  🔗  LinkedIn Profile Finder API v5.0 (Go proxy + multi-browser)")
    print("=" * 62)
    brow = f"✅ Go CDP proxy @ {CDP_PROXY_URL}" if _browser_available else "❌ Not found — proxy required!"
    print(f"  Port            : {API_PORT}")
    print(f"  Engines         : DuckDuckGo, Startpage  (race pattern)")
    print(f"  CDP proxy       : {brow}")
    print(f"  Cache           : {CACHE_MAX_SIZE} entries, {CACHE_TTL_SECONDS}s TTL")
    print()
    print("  Endpoints:")
    print("    POST /search           — lookup by name/company/location")
    print("    POST /search/custom    — lookup by raw query string")
    print("    POST /search/batch     — up to 100 lookups in one call")
    print("    GET  /health")
    print("    GET  /docs             — Swagger UI")
    print("=" * 62)
    print()
    if not _browser_available:
        print("  ⚠️  WARNING: Go CDP proxy not detected!")
        print("  Run: docker compose up -d --build")
        print()
    yield
    await _proxy_client.aclose()


app = FastAPI(
    title="LinkedIn Profile Finder API",
    description=__doc__,
    version="5.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

_req_times: deque[float] = deque()
_rate_lock = asyncio.Lock()
MAX_RPS = 500


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    now = time.time()
    async with _rate_lock:
        while _req_times and _req_times[0] < now - 1.0:
            _req_times.popleft()          # O(1) instead of list.pop(0) O(n)
        if len(_req_times) >= MAX_RPS:
            return JSONResponse(status_code=429,
                                content={"detail": f"Rate limit exceeded ({MAX_RPS} RPS)"})
        _req_times.append(now)
    return await call_next(request)


# ---------- Endpoints ----------


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health():
    """Health check — refreshes browser flag at most every 30 s."""
    global _browser_available, _browser_check_time
    now = time.time()
    if now - _browser_check_time > _BROWSER_CHECK_INTERVAL:
        _browser_available = await _check_browser()
        _browser_check_time = now
    return HealthResponse(
        status="ok", cache_size=len(_cache), cache_max=CACHE_MAX_SIZE,
        uptime_seconds=int(time.time() - _start_time),
        engines=["duckduckgo", "startpage"],
        browser_available=_browser_available,
    )


@app.post("/search", response_model=LinkedInResult, tags=["Search"])
async def search(req: SearchRequest):
    """
    Find a person's LinkedIn profile.

    **Request body:**
    ```json
    {
      "name": "Satya Nadella",          // required
      "company": "Microsoft",           // optional
      "location": "Seattle"             // optional
    }
    ```

    The system builds a query like `Satya Nadella LinkedIn Microsoft Seattle`,
    sends it through DuckDuckGo & Startpage (via the Lightpanda browser),
    then extracts and scores the best linkedin.com/in/ URL.
    """
    try:
        return await _find_linkedin(req.name, req.company, req.location)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/search/custom", response_model=CustomSearchResult, tags=["Search"])
async def search_custom(req: CustomSearchRequest):
    """
    Custom / raw search — you control the exact query string.

    **Request body:**
    ```json
    {
      "query": "Satya Nadella CEO Microsoft linkedin.com/in"
    }
    ```

    The query is forwarded as-is to both engines.  All discovered
    `linkedin.com/in/` URLs are returned in `all_linkedin_urls`,
    with the best one in `linkedin_url`.

    Use this when the standard name+company lookup isn't specific
    enough, or when you want to pass advanced operators like
    `site:linkedin.com/in/`.
    """
    try:
        return await _find_linkedin_custom(req.query)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/search/batch", response_model=BatchResponse, tags=["Search"])
async def search_batch(req: BatchRequest):
    """
    Batch lookup — up to 100 people in one request.

    **Request body:**
    ```json
    {
      "queries": [
        {"name": "Satya Nadella", "company": "Microsoft"},
        {"name": "Sundar Pichai", "company": "Google"}
      ]
    }
    ```

    All lookups run concurrently for maximum throughput.
    """
    t0 = time.monotonic()
    tasks = [_find_linkedin(q.name, q.company, q.location) for q in req.queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    final: list[LinkedInResult] = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            final.append(LinkedInResult(
                name=req.queries[i].name, company=req.queries[i].company,
                location=req.queries[i].location, confidence="low"))
        else:
            final.append(r)

    return BatchResponse(
        total=len(final),
        found=sum(1 for r in final if r.linkedin_url),
        results=final,
        total_time_ms=int((time.monotonic() - t0) * 1000),
    )


@app.get("/cache/stats", tags=["System"])
async def cache_stats():
    """Current cache statistics."""
    return {"size": len(_cache), "max_size": CACHE_MAX_SIZE, "ttl_seconds": CACHE_TTL_SECONDS}


@app.delete("/cache", tags=["System"])
async def cache_clear():
    """Clear the entire result cache."""
    async with _cache_lock:
        _cache.clear()
    return {"status": "cleared"}


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    import os
    workers = int(os.environ.get("WORKERS", 1))
    uvicorn.run(
        "linkedin_finder:app",
        host="0.0.0.0",
        port=API_PORT,
        workers=workers,
        reload=False,
        log_level="warning",
    )
