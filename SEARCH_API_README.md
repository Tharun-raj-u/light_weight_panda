# Lightpanda Google Search API

A simple REST API that uses the [Lightpanda](https://github.com/nicholasgasior/lightpanda) headless browser to perform Google searches via Chrome DevTools Protocol (CDP).

## Architecture

```
Client (curl/browser)  →  FastAPI (Python)  →  Lightpanda Browser (CDP/WebSocket)  →  Google
        ↑                       |
        └── JSON results ───────┘
```

## Prerequisites

1. **Lightpanda browser** built and available (the Zig project in `./browser/`)
2. **Python 3.10+** with the virtual environment activated

## Setup

```bash
# Activate the Python virtual environment
.\.venv\Scripts\Activate.ps1    # Windows
# source .venv/bin/activate     # Linux/macOS

# Install dependencies
pip install fastapi uvicorn websockets pydantic
```

## Running

### Step 1: Start the Lightpanda Browser (CDP Server)

```bash
# From the browser directory, build and run:
cd browser
zig build
./zig-out/bin/lightpanda serve --host 127.0.0.1 --port 9222
```

### Step 2: Start the Search API

```bash
python search_api.py
```

The API server will start at **http://localhost:8000**.

## API Endpoints

### `GET /search?q=<query>` — Google Search

Performs a Google search and returns structured results.

**Parameters:**
| Parameter | Type   | Required | Description       |
|-----------|--------|----------|-------------------|
| `q`       | string | Yes      | The search query  |

**Example:**
```bash
curl "http://localhost:8000/search?q=python+programming"
```

**Response:**
```json
{
  "query": "python programming",
  "total_results": 10,
  "results": [
    {
      "position": 1,
      "title": "Welcome to Python.org",
      "url": "https://www.python.org/",
      "snippet": "The official home of the Python Programming Language..."
    },
    {
      "position": 2,
      "title": "Python (programming language) - Wikipedia",
      "url": "https://en.wikipedia.org/wiki/Python_(programming_language)",
      "snippet": "Python is a high-level, general-purpose programming language..."
    }
  ]
}
```

### `GET /fetch?url=<url>` — Fetch Any URL

Fetches any URL and returns the page title and text content.

**Parameters:**
| Parameter | Type   | Required | Default | Description                       |
|-----------|--------|----------|---------|-----------------------------------|
| `url`     | string | Yes      | —       | The URL to fetch                  |
| `wait`    | int    | No       | 3       | Seconds to wait for page load (1-30) |

**Example:**
```bash
curl "http://localhost:8000/fetch?url=https://example.com&wait=2"
```

**Response:**
```json
{
  "url": "https://example.com",
  "title": "Example Domain",
  "content": "Example Domain\nThis domain is for use in illustrative examples..."
}
```

### `GET /health` — Health Check

Check API status and browser connectivity.

```bash
curl http://localhost:8000/health
```

**Response:**
```json
{
  "status": "ok",
  "browser_connected": true,
  "browser_url": "ws://127.0.0.1:9222/"
}
```

### Interactive API Docs

FastAPI provides auto-generated interactive docs:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Configuration

Edit the constants at the top of `search_api.py`:

```python
LIGHTPANDA_HOST = "127.0.0.1"   # Browser host
LIGHTPANDA_PORT = 9222           # Browser CDP port
PAGE_LOAD_WAIT  = 5              # Seconds to wait for page load
```
