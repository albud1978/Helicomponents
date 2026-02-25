#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
ENV_FILE="${ROOT_DIR}/deploy/superset-local/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[export-exact] ERROR: .env not found: ${ENV_FILE}" >&2
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

SUPERSET_CONTAINER="${SUPERSET_CONTAINER:-superset-local}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-superset-db-local}"
POSTGRES_USER="${POSTGRES_USER:-superset}"
POSTGRES_DB="${POSTGRES_DB:-superset}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-superset}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT_DIR="${1:-${ROOT_DIR}/output/superset_exact_clone_${STAMP}}"
IMAGE_REF="${EXACT_IMAGE_REF:-local/superset-exact:${STAMP}}"

mkdir -p "${OUTPUT_DIR}"

echo "[export-exact] Output dir: ${OUTPUT_DIR}"
echo "[export-exact] Committing container ${SUPERSET_CONTAINER} -> ${IMAGE_REF}"
docker commit "${SUPERSET_CONTAINER}" "${IMAGE_REF}" >/dev/null

echo "[export-exact] Saving image..."
docker save -o "${OUTPUT_DIR}/superset-image.tar" "${IMAGE_REF}"
echo "${IMAGE_REF}" > "${OUTPUT_DIR}/image_ref.txt"

echo "[export-exact] Dumping Superset metadata DB..."
docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${POSTGRES_CONTAINER}" \
  pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -Fc -f /tmp/superset_meta.dump
docker cp "${POSTGRES_CONTAINER}:/tmp/superset_meta.dump" "${OUTPUT_DIR}/superset_meta.dump"
docker exec "${POSTGRES_CONTAINER}" rm -f /tmp/superset_meta.dump

echo "[export-exact] Archiving superset_home..."
docker exec "${SUPERSET_CONTAINER}" sh -lc \
  "tar -czf /tmp/superset_home.tar.gz -C /app/superset_home ."
docker cp "${SUPERSET_CONTAINER}:/tmp/superset_home.tar.gz" "${OUTPUT_DIR}/superset_home.tar.gz"
docker exec "${SUPERSET_CONTAINER}" rm -f /tmp/superset_home.tar.gz

cat > "${OUTPUT_DIR}/manifest.txt" <<EOF
created_utc=${STAMP}
image_ref=${IMAGE_REF}
superset_container=${SUPERSET_CONTAINER}
postgres_container=${POSTGRES_CONTAINER}
postgres_db=${POSTGRES_DB}
postgres_user=${POSTGRES_USER}
files=superset-image.tar,superset_meta.dump,superset_home.tar.gz,image_ref.txt
EOF

echo "[export-exact] DONE"
echo "[export-exact] Artifacts:"
echo "  - ${OUTPUT_DIR}/superset-image.tar"
echo "  - ${OUTPUT_DIR}/superset_meta.dump"
echo "  - ${OUTPUT_DIR}/superset_home.tar.gz"
echo "  - ${OUTPUT_DIR}/image_ref.txt"
