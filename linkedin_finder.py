"""
LinkedIn Profile Finder API  —  v4.1  (Browser-only, race mode)
=====================================================

## What it does
Accepts a person's **name** (+ optional company / location) and returns
the most-likely **LinkedIn profile URL** with a confidence score.

## Architecture

    ┌──────────┐   POST /search     ┌────────────────┐
    │  Client  │ ─────────────────▶ │  FastAPI (8888) │
    └──────────┘   JSON body        └───────┬────────┘
                                            │
                      ┌─────────────────────┼─────────────────────┐
                      │  1. check in-memory cache (TTLCache)      │
                      │     hit → return instantly                 │
                      │  2. build query: "{name} LinkedIn {co}"   │
                      └─────────────────────┬─────────────────────┘
                                            │ miss
                      ┌─────────────────────┴─────────────────────┐
                      ▼                                           ▼
            ┌─────────────────┐                         ┌─────────────────┐
            │   DuckDuckGo    │     ◀── race ──▶        │    Startpage    │
            │  (Lightpanda)   │   first HIGH wins       │  (Lightpanda)   │
            └────────┬────────┘   & cancels loser       └────────┬────────┘
                     │                                           │
                     ▼                                           ▼
               ┌───────────┐                              ┌───────────┐
               │ Lightpanda│                              │ Lightpanda│
               │ CDP (9222)│                              │ CDP (9222)│
               └───────────┘                              └───────────┘

    3. Parse HTML (lxml) → extract all linkedin.com/in/ URLs
       (supports country subdomains: in.linkedin.com, es.linkedin.com, …)
    4. Score candidates — word-boundary name match + company match
       → high / medium / low / none
    5. First engine to return HIGH confidence wins → cancel the other
       If neither is HIGH, combine results and pick best
    6. Store in cache (1 h TTL)

    Three search passes if needed:
      Pass 1 — broad: "{name} LinkedIn {company} {location}"
      Pass 2 — site:  "site:linkedin.com/in/ {name} {company}"
      Pass 3 — keyword: "{name} {company} {location} linkedin.com/in"

## Search flow detail

  **All searches go through the Lightpanda browser (CDP)**
    Two engines (DuckDuckGo + Startpage) race in parallel.  The first
    engine to return a HIGH-confidence match wins; the slower engine is
    cancelled immediately (``asyncio.wait(FIRST_COMPLETED)``).

    For each engine a fresh WebSocket is opened to the Lightpanda Docker
    container (ws://127.0.0.1:9222/).  Via the Chrome DevTools Protocol:
      Target.createBrowserContext → Target.createTarget → attachToTarget
      → Page.navigate → wait for DOMContentLoaded/load → DOM.getOuterHTML
    The returned HTML is parsed with BeautifulSoup + lxml.

## Available API Endpoints
  ─────────────────────────────────────────────────────────────────────

  ### POST /search                              [tag: Search]
    Find a person's LinkedIn profile by name, company, and location.

    Request body (JSON):
      {
        "name":     "Satya Nadella",     // required  (1-200 chars)
        "company":  "Microsoft",         // optional  (max 200 chars)
        "location": "Seattle"            // optional  (max 200 chars)
      }

    Response body (JSON):
      {
        "name":              "Satya Nadella",
        "company":           "Microsoft",
        "location":          "Seattle",
        "linkedin_url":      "https://www.linkedin.com/in/satyanadella",
        "profile_name":      "Satya Nadella",
        "profile_headline":  "Chairman and CEO at Microsoft",
        "confidence":        "high",          // high | medium | low
        "engine":            "duckduckgo",    // duckduckgo | startpage | google
        "cached":            false,
        "search_time_ms":    3651
      }

  ### POST /search/custom                       [tag: Search]
    Free-form search — you supply the exact query string.

    Request body (JSON):
      {
        "query": "Satya Nadella CEO Microsoft linkedin.com/in"
                                          // required  (1-500 chars)
      }

    Response body (JSON):
      {
        "query":              "Satya Nadella CEO Microsoft linkedin.com/in",
        "linkedin_url":       "https://www.linkedin.com/in/satyanadella",
        "all_linkedin_urls":  [
          "https://www.linkedin.com/in/satyanadella",
          "https://www.linkedin.com/in/othermatch"
        ],
        "confidence":         "medium",
        "engine":             "duckduckgo",
        "cached":             false,
        "search_time_ms":     4092
      }

  ### POST /search/batch                        [tag: Search]
    Batch lookup — up to 100 people in one request, all concurrent.

    Request body (JSON):
      {
        "queries": [
          {"name": "Satya Nadella", "company": "Microsoft"},
          {"name": "Sundar Pichai", "company": "Google"}
        ]                                 // 1-100 items
      }

    Response body (JSON):
      {
        "total":          2,
        "found":          2,
        "results":        [ ...LinkedInResult objects... ],
        "total_time_ms":  15980
      }

  ### GET /health                               [tag: System]
    Health check. Browser flag refreshes at most every 30 s.

    Response body (JSON):
      {
        "status":             "ok",
        "cache_size":         42,
        "cache_max":          10000,
        "uptime_seconds":     3600,
        "engines":            ["duckduckgo", "startpage"],
        "browser_available":  true
      }

  ### GET /cache/stats                          [tag: System]
    Current cache statistics.

    Response body (JSON):
      {
        "size":         42,
        "max_size":     10000,
        "ttl_seconds":  3600
      }

  ### DELETE /cache                             [tag: System]
    Clear the entire result cache.

    Response body (JSON):
      { "status": "cleared" }

  ### GET /docs                                 [built-in]
    Swagger UI — interactive API documentation (auto-generated).

  ### GET /openapi.json                         [built-in]
    OpenAPI 3.1 schema (machine-readable).

  ─────────────────────────────────────────────────────────────────────

## Error Responses

    422  — Validation error (missing/invalid fields).
           Body: {"detail": [{"loc":["body","name"],"msg":"...","type":"..."}]}

    429  — Rate limit exceeded (500 requests/second).
           Body: {"detail": "Rate limit exceeded (500 RPS)"}

    500  — Internal server error (upstream search failure).
           Body: {"detail": "error description"}

## Performance (benchmarked 2026-03-17)

  | Scenario                | Throughput              |
  |-------------------------|-------------------------|
  | Cached lookups          | 126 – 155 RPS           |
  | Health endpoint         | 138 – 155 RPS           |
  | Cached batch (10)       | 3 600 lookups/s         |
  | Uncached batch (10)     | 10/10 found in ~13 s    |
  | Single uncached lookup  | 3 – 4 s                 |

  **Configuration tuning:**
    CDP_MAX_CONCURRENT   = 30   (parallel Lightpanda sessions)
    MAX_RPS              = 500  (rate limiter cap)
    CACHE_TTL_SECONDS    = 3600 (1 hour)
    CACHE_MAX_SIZE       = 10000

  For multi-worker scaling set WORKERS=N env var (default 1).
  Note: each worker keeps its own in-memory cache.

## Prerequisites

    docker run -d --name lightpanda --restart=always \\
               -p 9222:9222 lightpanda/browser:nightly
    pip install fastapi uvicorn httpx websockets beautifulsoup4 cachetools lxml

    ⚠️  The Lightpanda browser is REQUIRED — there is no HTTP fallback.

## Quick Start

    python linkedin_finder.py

    # Single lookup
    curl -X POST http://localhost:8888/search \\
         -H "Content-Type: application/json" \\
         -d '{"name": "Satya Nadella", "company": "Microsoft"}'

    # Custom raw search query
    curl -X POST http://localhost:8888/search/custom \\
         -H "Content-Type: application/json" \\
         -d '{"query": "Satya Nadella CEO Microsoft linkedin.com/in"}'

    # Batch (up to 100)
    curl -X POST http://localhost:8888/search/batch \\
         -H "Content-Type: application/json" \\
         -d '{"queries": [{"name":"Satya Nadella","company":"Microsoft"},
                          {"name":"Sundar Pichai","company":"Google"}]}'

    # Health check
    curl http://localhost:8888/health

    # Cache stats / clear
    curl http://localhost:8888/cache/stats
    curl -X DELETE http://localhost:8888/cache

    # Interactive docs
    open http://localhost:8888/docs
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import urllib.parse
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import uvicorn
import websockets
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

CDP_WS_URL = "ws://127.0.0.1:9222/"
CDP_MAX_CONCURRENT = 30          # parallel browser sessions
CDP_PAGE_TIMEOUT = 10.0           # seconds per page load

CACHE_MAX_SIZE = 10_000
CACHE_TTL_SECONDS = 3600

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

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

_cdp_semaphore: Optional[asyncio.Semaphore] = None
_cache: TTLCache = TTLCache(maxsize=CACHE_MAX_SIZE, ttl=CACHE_TTL_SECONDS)
_cache_lock: asyncio.Lock = asyncio.Lock()
_start_time: float = 0
_ua_counter: int = 0
_browser_available: bool = False
_browser_check_time: float = 0
_BROWSER_CHECK_INTERVAL = 30.0   # re-probe Lightpanda every 30 s


def _next_ua() -> str:
    global _ua_counter
    _ua_counter += 1
    return USER_AGENTS[_ua_counter % len(USER_AGENTS)]


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
# Lightpanda CDP helper
# ============================================================================


async def _cdp_fetch_html(url: str, timeout: float = CDP_PAGE_TIMEOUT) -> str:
    """Navigate Lightpanda to *url* via CDP and return the rendered HTML."""
    async with _cdp_semaphore:
        msg_id = 0

        async def send(ws, method, params=None, session_id=None):
            nonlocal msg_id
            msg_id += 1
            msg: dict = {"id": msg_id, "method": method}
            if params:
                msg["params"] = params
            if session_id:
                msg["sessionId"] = session_id
            await ws.send(json.dumps(msg))
            return msg_id

        async def recv_until(ws, target_id, deadline):
            events: list[dict] = []
            while time.monotonic() < deadline:
                remaining = max(0.1, deadline - time.monotonic())
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 3))
                    data = json.loads(raw)
                    if data.get("id") == target_id:
                        return data, events
                    events.append(data)
                except asyncio.TimeoutError:
                    continue
            return None, events

        deadline = time.monotonic() + timeout
        async with websockets.connect(
            CDP_WS_URL, max_size=10 * 1024 * 1024,
            open_timeout=5, close_timeout=1,
        ) as ws:
            rid = await send(ws, "Target.createBrowserContext")
            resp, _ = await recv_until(ws, rid, deadline)
            if resp is None or "error" in resp:
                raise RuntimeError(f"createBrowserContext failed: {resp}")
            ctx_id = resp["result"]["browserContextId"]

            try:
                rid = await send(ws, "Target.createTarget",
                                 {"url": "about:blank", "browserContextId": ctx_id})
                resp, _ = await recv_until(ws, rid, deadline)
                target_id = resp["result"]["targetId"]

                rid = await send(ws, "Target.attachToTarget",
                                 {"targetId": target_id, "flatten": True})
                resp, _ = await recv_until(ws, rid, deadline)
                session_id = resp["result"]["sessionId"]

                rid = await send(ws, "Page.enable", session_id=session_id)
                await recv_until(ws, rid, deadline)
                rid = await send(ws, "Page.setLifecycleEventsEnabled",
                                 {"enabled": True}, session_id=session_id)
                await recv_until(ws, rid, deadline)

                rid = await send(ws, "Page.navigate", {"url": url},
                                 session_id=session_id)
                await recv_until(ws, rid, deadline)

                _READY_EVENTS = {
                    "networkIdle", "networkAlmostIdle",
                    "DOMContentLoaded", "load",
                }
                while time.monotonic() < deadline:
                    remaining = max(0.1, deadline - time.monotonic())
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(), timeout=min(remaining, 3))
                        data = json.loads(raw)
                        if data.get("method") == "Page.lifecycleEvent":
                            evt = data.get("params", {}).get("name", "")
                            if evt in _READY_EVENTS:
                                break
                    except asyncio.TimeoutError:
                        break

                rid = await send(ws, "DOM.getDocument", {"depth": 0},
                                 session_id=session_id)
                resp, _ = await recv_until(ws, rid, deadline)
                root_id = resp["result"]["root"]["nodeId"]

                rid = await send(ws, "DOM.getOuterHTML", {"nodeId": root_id},
                                 session_id=session_id)
                resp, _ = await recv_until(ws, rid, deadline)
                return resp["result"]["outerHTML"]

            finally:
                try:
                    rid = await send(ws, "Target.closeTarget", {"targetId": target_id})
                    await recv_until(ws, rid, time.monotonic() + 0.5)
                except Exception:
                    pass
                try:
                    rid = await send(ws, "Target.disposeBrowserContext",
                                     {"browserContextId": ctx_id})
                    await recv_until(ws, rid, time.monotonic() + 0.5)
                except Exception:
                    pass


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
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            r = await c.get("http://127.0.0.1:9222/json/version")
            return r.status_code == 200
    except Exception:
        return False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cdp_semaphore, _start_time, _browser_available
    _start_time = time.time()
    _cdp_semaphore = asyncio.Semaphore(CDP_MAX_CONCURRENT)
    _browser_available = await _check_browser()

    print()
    print("=" * 62)
    print("  🔗  LinkedIn Profile Finder API v4.0 (Browser-only)")
    print("=" * 62)
    brow = f"✅ Lightpanda CDP @ {CDP_WS_URL}" if _browser_available else "❌ Not found — browser required!"
    print(f"  Port            : {API_PORT}")
    print(f"  Engines         : DuckDuckGo, Startpage  (race pattern)")
    print(f"  Browser         : {brow}")
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
        print("  ⚠️  WARNING: Lightpanda browser not detected!")
        print("  Run: docker run -d --name lightpanda --restart=always -p 9222:9222 lightpanda/browser:nightly")
        print()
    yield


app = FastAPI(
    title="LinkedIn Profile Finder API",
    description=__doc__,
    version="4.1.0",
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
