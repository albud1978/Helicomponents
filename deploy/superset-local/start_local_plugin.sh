#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.yml"
PLUGIN_COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.plugin.yml"
ENV_FILE="${ROOT_DIR}/deploy/superset-local/.env"
BUILD_SCRIPT="${ROOT_DIR}/deploy/superset-local/scripts/build_superset_with_plugin.sh"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[start-plugin] .env not found, creating from .env.example"
  cp "${ROOT_DIR}/deploy/superset-local/.env.example" "${ENV_FILE}"
fi

set -a
source "${ENV_FILE}"
set +a

PLUGIN_IMAGE="${SUPERSET_PLUGIN_IMAGE:-local/superset-echarts6:latest}"

if ! docker image inspect "${PLUGIN_IMAGE}" >/dev/null 2>&1; then
  echo "[start-plugin] Plugin image ${PLUGIN_IMAGE} not found locally."
  echo "[start-plugin] Building plugin image first..."
  "${BUILD_SCRIPT}"
fi

docker compose \
  --env-file "${ENV_FILE}" \
  -f "${BASE_COMPOSE_FILE}" \
  -f "${PLUGIN_COMPOSE_FILE}" \
  up -d

echo "[start-plugin] Superset with plugin is starting at http://localhost:${SUPERSET_PORT:-8088}"
