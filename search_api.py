"""
Search API — Standalone Edition

A FastAPI endpoint that performs web searches and returns structured results.

Search backends (tried in order):
  1. Lightpanda CDP browser   — if running on port 9222 (optional)
  2. Google HTML scraping      — httpx + BeautifulSoup
  3. DuckDuckGo HTML scraping  — httpx + BeautifulSoup (most reliable)

Usage:
    python search_api.py
    GET http://localhost:8888/search?q=python+programming
    GET http://localhost:8888/docs
"""

import asyncio
import json
import re
import urllib.parse
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import uvicorn
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_PORT = 8888

LIGHTPANDA_HOST = "127.0.0.1"
LIGHTPANDA_PORT = 9222
LIGHTPANDA_WS_URL = f"ws://{LIGHTPANDA_HOST}:{LIGHTPANDA_PORT}/"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

HTTP_TIMEOUT = 15  # seconds


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class SearchResult(BaseModel):
    """A single search result."""
    position: int
    title: str
    url: str
    snippet: str


class SearchResponse(BaseModel):
    """Response from the /search endpoint."""
    query: str
    engine: str
    total_results: int
    results: list[SearchResult]


class HealthResponse(BaseModel):
    """Response from the /health endpoint."""
    status: str
    search_engine: str
    browser_connected: bool
    browser_url: str


# ---------------------------------------------------------------------------
# Search Backends
# ---------------------------------------------------------------------------

async def _check_browser_available() -> bool:
    """Check if the Lightpanda browser is reachable."""
    try:
        import websockets
        ws = await asyncio.wait_for(
            websockets.connect(LIGHTPANDA_WS_URL, close_timeout=2),
            timeout=3,
        )
        await ws.close()
        return True
    except Exception:
        return False


# ---- Backend 1: DuckDuckGo HTML (no JS needed, most reliable) ----

async def _search_duckduckgo(query: str, num_results: int = 10) -> list[SearchResult]:
    """
    Search via DuckDuckGo HTML-only interface.
    No JS required — pure HTTP + HTML parsing.
    """
    async with httpx.AsyncClient(
        headers=HTTP_HEADERS,
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        verify=False,
    ) as client:
        resp = await client.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
        )
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[SearchResult] = []

    for i, item in enumerate(soup.select(".result.results_links"), start=1):
        if i > num_results:
            break

        title_el = item.select_one(".result__a")
        snippet_el = item.select_one(".result__snippet")

        title = title_el.get_text(strip=True) if title_el else ""
        href = title_el.get("href", "") if title_el else ""
        snippet = snippet_el.get_text(strip=True) if snippet_el else ""

        # DuckDuckGo wraps URLs in a redirect — extract the real URL
        if "uddg=" in href:
            match = re.search(r"uddg=([^&]+)", href)
            if match:
                href = urllib.parse.unquote(match.group(1))

        if title and href:
            results.append(SearchResult(
                position=len(results) + 1,
                title=title,
                url=href,
                snippet=snippet,
            ))

    return results


# ---- Backend 2: Google via httpx + BeautifulSoup ----

async def _search_google(query: str, num_results: int = 10) -> list[SearchResult]:
    """Search Google by scraping the HTML results page."""
    encoded_q = urllib.parse.quote_plus(query)
    url = f"https://www.google.com/search?q={encoded_q}&num={num_results}&hl=en"

    async with httpx.AsyncClient(
        headers=HTTP_HEADERS,
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        verify=False,
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[SearchResult] = []
    pos = 0

    for item in soup.select("div.g"):
        title_el = item.select_one("h3")
        link_el = item.select_one("a[href]")
        snippet_el = item.select_one(
            "div.VwiC3b, div[data-sncf], span.aCOpRe, "
            "div[style*='-webkit-line-clamp']"
        )

        if title_el and link_el:
            href = link_el.get("href", "")
            if href.startswith("/url?q="):
                href = urllib.parse.unquote(href[7:].split("&")[0])

            pos += 1
            results.append(SearchResult(
                position=pos,
                title=title_el.get_text(strip=True),
                url=href,
                snippet=snippet_el.get_text(strip=True) if snippet_el else "",
            ))
            if pos >= num_results:
                break

    return results


# ---- Backend 3: Lightpanda CDP browser (optional) ----

async def _search_cdp(query: str) -> list[SearchResult]:
    """Search via Lightpanda browser using CDP WebSocket."""
    import websockets

    encoded_q = urllib.parse.quote_plus(query)
    search_url = f"https://www.google.com/search?q={encoded_q}"
    msg_id = 0

    async def cdp_send(ws, method, params=None):
        nonlocal msg_id
        msg_id += 1
        msg = {"id": msg_id, "method": method}
        if params:
            msg["params"] = params
        await ws.send(json.dumps(msg))
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
            resp = json.loads(raw)
            if resp.get("id") == msg_id:
                if "error" in resp:
                    raise RuntimeError(f"CDP error: {resp['error']}")
                return resp.get("result", {})

    ws = await websockets.connect(LIGHTPANDA_WS_URL, max_size=10 * 1024 * 1024)
    try:
        await cdp_send(ws, "Page.enable")
        await cdp_send(ws, "DOM.enable")
        await cdp_send(ws, "Runtime.enable")
        await cdp_send(ws, "Page.navigate", {"url": search_url})
        await asyncio.sleep(5)

        js_code = """
        (() => {
            const results = [];
            const items = document.querySelectorAll('div.g');
            let pos = 0;
            for (const item of items) {
                const h3 = item.querySelector('h3');
                const a = item.querySelector('a[href]');
                const sn = item.querySelector('div.VwiC3b, div[data-sncf], span.aCOpRe');
                if (h3 && a) {
                    pos++;
                    results.push({
                        position: pos,
                        title: h3.textContent || '',
                        url: a.href || '',
                        snippet: sn ? sn.textContent || '' : ''
                    });
                }
            }
            return JSON.stringify(results);
        })()
        """
        result = await cdp_send(ws, "Runtime.evaluate", {
            "expression": js_code,
            "returnByValue": True,
        })
        raw = result.get("result", {}).get("value", "[]")
        items = json.loads(raw) if isinstance(raw, str) else (raw or [])
        return [SearchResult(**item) for item in items]
    finally:
        await ws.close()


# ---- Unified search function ----

async def perform_search(
    query: str,
    num_results: int = 10,
    engine: str = "auto",
) -> tuple[list[SearchResult], str]:
    """
    Perform a search. Returns (results, engine_used).
    engine: "auto" | "duckduckgo" | "google" | "cdp"
    """
    errors: list[str] = []

    if engine in ("auto", "cdp"):
        if await _check_browser_available():
            try:
                results = await _search_cdp(query)
                if results:
                    return results, "cdp (Lightpanda)"
            except Exception as e:
                errors.append(f"cdp: {e}")

    if engine in ("auto", "google"):
        try:
            results = await _search_google(query, num_results)
            if results:
                return results, "google"
        except Exception as e:
            errors.append(f"google: {e}")

    if engine in ("auto", "duckduckgo"):
        try:
            results = await _search_duckduckgo(query, num_results)
            if results:
                return results, "duckduckgo"
        except Exception as e:
            errors.append(f"duckduckgo: {e}")

    # If a specific engine was chosen and it failed
    if engine != "auto" and errors:
        raise RuntimeError(f"Search engine '{engine}' failed: {'; '.join(errors)}")

    # auto mode: all backends failed
    if errors:
        raise RuntimeError(f"All search backends failed: {'; '.join(errors)}")

    return [], engine


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown logic."""
    browser_ok = await _check_browser_available()
    print()
    print("=" * 50)
    print("  🔍  Search API v2.0  ")
    print("=" * 50)
    print(f"  Lightpanda browser : {'✅ connected' if browser_ok else '⬜ not running (optional)'}")
    print(f"  Search backends    : DuckDuckGo, Google" + (", Lightpanda CDP" if browser_ok else ""))
    print()
    print(f"  Endpoints:")
    print(f"    GET /search?q=<query>              — web search")
    print(f"    GET /search?q=<query>&engine=duckduckgo")
    print(f"    GET /fetch?url=<url>               — fetch any page")
    print(f"    GET /health                        — status check")
    print(f"    GET /docs                          — Swagger UI")
    print("=" * 50)
    print()
    yield
    print("Search API shutting down...")


app = FastAPI(
    title="Search API",
    description=(
        "A REST API that performs web searches using multiple backends: "
        "DuckDuckGo HTML scraping, Google HTML scraping, and optionally "
        "the Lightpanda headless browser via CDP."
    ),
    version="2.0.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Check API status and backend availability."""
    browser_ok = await _check_browser_available()
    return HealthResponse(
        status="ok",
        search_engine="duckduckgo + google" + (" + cdp" if browser_ok else ""),
        browser_connected=browser_ok,
        browser_url=LIGHTPANDA_WS_URL,
    )


@app.get("/search", response_model=SearchResponse, tags=["Search"])
async def search(
    q: str = Query(..., min_length=1, max_length=500, description="Search query"),
    num: int = Query(10, ge=1, le=30, description="Max number of results"),
    engine: str = Query(
        "auto",
        description="Search engine: auto, google, duckduckgo, cdp",
    ),
):
    """
    Perform a web search and return structured results.

    **Engines:**
    - `auto` (default): tries CDP → Google → DuckDuckGo
    - `google`: scrape Google directly
    - `duckduckgo`: scrape DuckDuckGo (most reliable)
    - `cdp`: use Lightpanda browser (must be running)
    """
    if engine not in ("auto", "google", "duckduckgo", "cdp"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid engine '{engine}'. Use: auto, google, duckduckgo, cdp",
        )

    try:
        results, engine_used = await perform_search(q, num, engine)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Search failed: {exc}"
        ) from exc

    return SearchResponse(
        query=q,
        engine=engine_used,
        total_results=len(results),
        results=results,
    )


@app.get("/fetch", tags=["Fetch"])
async def fetch_page(
    url: str = Query(..., description="URL to fetch"),
):
    """Fetch any URL and return its title + text content."""
    try:
        async with httpx.AsyncClient(
            headers=HTTP_HEADERS,
            timeout=HTTP_TIMEOUT,
            follow_redirects=True,
            verify=False,
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Upstream returned {exc.response.status_code}",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=502, detail=f"Request failed: {exc}"
        ) from exc

    soup = BeautifulSoup(resp.text, "html.parser")

    # Remove script/style tags for clean text
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = soup.title.get_text(strip=True) if soup.title else ""
    text = soup.get_text(separator="\n", strip=True)

    return {
        "url": str(resp.url),
        "status_code": resp.status_code,
        "title": title,
        "content": text,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "search_api:app",
        host="0.0.0.0",
        port=API_PORT,
        reload=False,
        log_level="info",
    )
