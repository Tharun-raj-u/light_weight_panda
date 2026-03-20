# ── LinkedIn Profile Finder API ──────────────────────────
FROM python:3.13-slim

# Prevent .pyc files and enable unbuffered stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY linkedin_finder.py .

# Default env vars (can be overridden in docker-compose)
ENV CDP_PROXY_URL="http://cdp-proxy:9333" \
    WORKERS=1

EXPOSE 8888

CMD ["python", "linkedin_finder.py"]
