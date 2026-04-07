# ──────────────────────────────────────────────────────────────────────────────
# Hospital Operations Dashboard — Dockerfile
# Base: python:3.11-slim (Debian Bookworm slim)
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Prevent Python from buffering stdout/stderr (important for Railway logs)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies needed by psycopg2-binary
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project source
COPY . .

# Railway dynamically assigns $PORT; Dash will listen on it.
# Default fallback for local docker run:
EXPOSE 8050

# Production server: 2 workers, 120s timeout (Dash callbacks can take a moment)
# $PORT is set by Railway; falls back to 8050 locally
CMD gunicorn \
    --bind "0.0.0.0:${PORT:-8050}" \
    --workers 2 \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    app.app:server
