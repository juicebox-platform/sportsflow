FROM apache/airflow:3.0.2-python3.12

USER root

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /workspace
