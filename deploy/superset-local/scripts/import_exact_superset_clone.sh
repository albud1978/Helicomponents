#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ENV_FILE="${ROOT_DIR}/deploy/superset-local/.env"
COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.yml"
PLUGIN_COMPOSE_FILE="${ROOT_DIR}/deploy/superset-local/docker-compose.plugin.yml"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[import-exact] ERROR: .env not found: ${ENV_FILE}" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <artifact_dir>" >&2
  exit 1
fi

ARTIFACT_DIR="$1"
if [[ ! -d "${ARTIFACT_DIR}" ]]; then
  echo "[import-exact] ERROR: artifact dir does not exist: ${ARTIFACT_DIR}" >&2
  exit 1
fi

IMAGE_TAR="${ARTIFACT_DIR}/superset-image.tar"
META_DUMP="${ARTIFACT_DIR}/superset_meta.dump"
HOME_TGZ="${ARTIFACT_DIR}/superset_home.tar.gz"
IMAGE_REF_FILE="${ARTIFACT_DIR}/image_ref.txt"

for required in "${IMAGE_TAR}" "${META_DUMP}"; do
  if [[ ! -f "${required}" ]]; then
    echo "[import-exact] ERROR: required artifact missing: ${required}" >&2
    exit 1
  fi
done

set -a
source "${ENV_FILE}"
set +a

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-superset-db-local}"
POSTGRES_USER="${POSTGRES_USER:-superset}"
POSTGRES_DB="${POSTGRES_DB:-superset}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-superset}"

if [[ -f "${IMAGE_REF_FILE}" ]]; then
  IMAGE_REF="$(tr -d '\r\n' < "${IMAGE_REF_FILE}")"
else
  IMAGE_REF="${SUPERSET_PLUGIN_IMAGE:-local/superset-echarts6:latest}"
fi

echo "[import-exact] Loading image from ${IMAGE_TAR}..."
docker load -i "${IMAGE_TAR}" >/dev/null
echo "[import-exact] Image ref: ${IMAGE_REF}"

echo "[import-exact] Starting DB/Redis..."
docker compose --env-file "${ENV_FILE}" -f "${COMPOSE_FILE}" up -d db redis

echo "[import-exact] Waiting for Postgres..."
for _ in $(seq 1 30); do
  if docker exec "${POSTGRES_CONTAINER}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[import-exact] Recreating Superset metadata DB..."
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${POSTGRES_CONTAINER}" \
  psql -U "${POSTGRES_USER}" -d postgres -c "DROP DATABASE IF EXISTS \"${POSTGRES_DB}\";"
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${POSTGRES_CONTAINER}" \
  psql -U "${POSTGRES_USER}" -d postgres -c "CREATE DATABASE \"${POSTGRES_DB}\" OWNER \"${POSTGRES_USER}\";"

docker cp "${META_DUMP}" "${POSTGRES_CONTAINER}:/tmp/superset_meta.dump"
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${POSTGRES_CONTAINER}" \
  pg_restore -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --no-owner --no-privileges /tmp/superset_meta.dump
docker exec "${POSTGRES_CONTAINER}" rm -f /tmp/superset_meta.dump

echo "[import-exact] Starting Superset with imported image..."
SUPERSET_PLUGIN_IMAGE="${IMAGE_REF}" docker compose \
  --env-file "${ENV_FILE}" \
  -f "${COMPOSE_FILE}" \
  -f "${PLUGIN_COMPOSE_FILE}" \
  up -d

if [[ -f "${HOME_TGZ}" ]]; then
  echo "[import-exact] Restoring superset_home..."
  docker cp "${HOME_TGZ}" superset-local:/tmp/superset_home.tar.gz
  docker exec superset-local sh -lc \
    "rm -rf /app/superset_home/* && tar -xzf /tmp/superset_home.tar.gz -C /app/superset_home && rm -f /tmp/superset_home.tar.gz"
  docker restart superset-local >/dev/null
fi

echo "[import-exact] Waiting for Superset health..."
for _ in $(seq 1 40); do
  if curl -fsS "http://127.0.0.1:${SUPERSET_PORT:-8088}/health" >/dev/null 2>&1; then
    echo "[import-exact] DONE: Superset is healthy"
    exit 0
  fi
  sleep 2
done

echo "[import-exact] WARNING: health check timed out, inspect logs:"
echo "  docker logs --tail 200 superset-local"
exit 1
