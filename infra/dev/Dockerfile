# syntax=docker/dockerfile:1

ARG PY_VERSION=3.10
FROM python:${PY_VERSION}-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip

RUN python3 -m pip install \
    pandas numpy openpyxl \
    clickhouse-driver clickhouse-connect \
    PyYAML

WORKDIR /app
CMD ["bash"]