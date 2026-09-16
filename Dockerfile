# syntax=docker/dockerfile:1

FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config

RUN python -m pip install --no-cache-dir .


FROM base AS test

COPY tests ./tests
COPY scripts ./scripts

RUN python -m pip install --no-cache-dir ".[dev]" \
    && python -m ruff check src tests scripts \
    && python -m pytest


FROM base AS runtime

RUN groupadd --gid 10001 pipeline \
    && useradd \
        --uid 10001 \
        --gid pipeline \
        --create-home \
        pipeline \
    && mkdir -p /app/data \
    && chown -R pipeline:pipeline /app

USER pipeline

VOLUME ["/app/data"]

ENTRYPOINT ["taxi-pipeline"]
CMD ["--help"]