# syntax=docker/dockerfile:1

ARG PY_VERSION=3.12
ARG CUDA_VERSION=13.0.0

# CUDA 13.0 — поддерживает RTX 30xx/40xx/50xx (включая Blackwell sm_120)
FROM nvidia/cuda:${CUDA_VERSION}-runtime-ubuntu24.04

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

# Фиксированные версии из реального окружения (14-12-2025)
RUN python3 -m pip install \
    pandas==2.3.3 \
    numpy==2.2.6 \
    openpyxl==3.1.5 \
    clickhouse-driver==0.2.10 \
    clickhouse-connect==0.10.0 \
    PyYAML==6.0.3

# cuDF для CUDA 13 (требует PIP_EXTRA_INDEX_URL=https://pypi.nvidia.com)
# Примечание: cudf-cu13 может быть недоступен, проверьте актуальную версию
RUN python3 -m pip install cudf-cu12==25.12.0 || echo "cudf installation skipped"

# FLAME GPU 2 для CUDA 13.0
# Устанавливается через extra-index-url
RUN if [ -n "$PYFLAMEGPU_WHEEL_URL" ]; then \
      echo "Installing pyflamegpu from $PYFLAMEGPU_WHEEL_URL" && \
      python3 -m pip install "$PYFLAMEGPU_WHEEL_URL"; \
    else \
      echo "Installing pyflamegpu from FLAME GPU index (cuda130)" && \
      python3 -m pip install --extra-index-url https://whl.flamegpu.com/whl/cuda130/ pyflamegpu==2.0.0rc4+cuda130 || \
      echo "pyflamegpu installation skipped - provide PYFLAMEGPU_WHEEL_URL if needed"; \
    fi

WORKDIR /app
CMD ["bash"]
