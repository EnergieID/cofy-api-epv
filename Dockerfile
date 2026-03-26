# --- Build stage ---
FROM python:3.12-alpine AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock* ./

# Install dependencies only (not the project itself yet)
RUN uv sync --frozen --no-dev --no-install-project

# Copy source code
COPY . .

# Install the project itself
RUN uv sync --frozen --no-dev \
    && find /app/.venv -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# --- Final stage (no uv, no build deps) ---
FROM python:3.12-alpine

WORKDIR /app

# Copy the entire virtual environment and source code from the builder
COPY --from=builder /app /app

ARG VERSION=dev
ENV APP_VERSION=${VERSION}

# Most cloud platforms inject a PORT env var — default to 8080 locally
CMD ["sh", "-c", "/app/.venv/bin/uvicorn main:cofy --host 0.0.0.0 --port ${PORT:-8080}"]