# syntax=docker/dockerfile:1

ARG PY_VERSION=3.10
ARG CUDA_VERSION=12.2.0

FROM nvidia/cuda:${CUDA_VERSION}-runtime-ubuntu22.04

ARG PY_VERSION
ARG PYFLAMEGPU_WHEEL_URL

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python${PY_VERSION} python3-pip python3-venv \
    build-essential curl ca-certificates git pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip

# Base Python deps
RUN python3 -m pip install \
    pandas numpy openpyxl \
    clickhouse-driver clickhouse-connect \
    PyYAML

# cuDF from NVIDIA PyPI (requires PIP_EXTRA_INDEX_URL)
RUN python3 -m pip install cudf-cu12==25.6.0

# Optional: FLAME GPU from wheel URL if provided
RUN if [ -n "$PYFLAMEGPU_WHEEL_URL" ]; then \
      echo "Installing pyflamegpu from $PYFLAMEGPU_WHEEL_URL" && \
      python3 -m pip install "$PYFLAMEGPU_WHEEL_URL"; \
    else \
      echo "Skipping pyflamegpu installation (provide PYFLAMEGPU_WHEEL_URL)"; \
    fi

WORKDIR /app
CMD ["bash"]