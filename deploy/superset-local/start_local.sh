#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.yml"
ENV_FILE="${ROOT_DIR}/deploy/superset-local/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[start] .env not found, creating from .env.example"
  cp "${ROOT_DIR}/deploy/superset-local/.env.example" "${ENV_FILE}"
fi

docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d --build
echo "[start] Superset local is starting at http://localhost:${SUPERSET_PORT:-8088}"
