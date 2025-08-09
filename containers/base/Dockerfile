FROM ghcr.io/astral-sh/uv:python3.12-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src/ ./src/

RUN uv sync --frozen --no-dev
