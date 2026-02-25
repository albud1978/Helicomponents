#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.yml"
ENV_FILE="${ROOT_DIR}/deploy/superset-local/.env"

if [[ -f "${ENV_FILE}" ]]; then
  docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" down
else
  docker compose -f "${COMPOSE_FILE}" down
fi
