#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.yml"

docker compose -f "${COMPOSE_FILE}" exec superset \
  python /app/deploy/superset-local/scripts/bootstrap_datasource_rbac.py
