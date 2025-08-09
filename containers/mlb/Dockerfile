FROM your-registry/sportsflow-base:latest

RUN uv sync --frozen --no-dev --extra mlb

ENTRYPOINT ["uv", "run", "python", "-m", "sportsflow.mlb.entrypoint"]
