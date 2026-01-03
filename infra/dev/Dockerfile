# syntax=docker/dockerfile:1

ARG PY_VERSION=3.12
FROM python:${PY_VERSION}-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip

# Фиксированные версии из реального окружения (14-12-2025)
RUN python3 -m pip install \
    pandas==2.3.3 \
    numpy==2.2.6 \
    openpyxl==3.1.5 \
    clickhouse-driver==0.2.10 \
    clickhouse-connect==0.10.0 \
    PyYAML==6.0.3

WORKDIR /app
CMD ["bash"]
