# syntax=docker/dockerfile:1.7
# Real ingestion image used by docker-compose.yml.
# Build context is the repo root (see compose: build.context = ../).
FROM openmetadata/ingestion:1.12.6

WORKDIR /opt/airflow/ingestion

COPY --from=ghcr.io/astral-sh/uv:0.6.17 /uv /uvx /bin/
ENV UV_CACHE_DIR=/tmp/uv-cache
ENV UV_LINK_MODE=copy

COPY duckdb-connector/connector connector
COPY duckdb-connector/pyproject.toml .

USER root
RUN --mount=type=cache,target=/tmp/uv-cache \
    uv pip install --system .
USER airflow
