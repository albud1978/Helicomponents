#!/usr/bin/env bash
set -euo pipefail

echo "[init] Running Superset DB migrations..."
superset db upgrade

echo "[init] Creating admin user (idempotent)..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USERNAME}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME}" \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo "[init] Finalizing Superset init..."
superset init

if [[ "${BOOTSTRAP_DATASOURCE:-1}" == "1" ]]; then
  echo "[init] Bootstrapping ClickHouse datasource and local roles..."
  python /app/deploy/superset-local/scripts/bootstrap_datasource_rbac.py
fi

echo "[init] Done."
