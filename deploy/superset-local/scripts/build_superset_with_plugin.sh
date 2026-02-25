#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/.superset-build}"
USER_SUPERSET_SRC_DIR="${SUPERSET_SRC_DIR:-}"
SUPERSET_SRC_DIR="${SUPERSET_SRC_DIR:-}"
PLUGIN_SRC_DIR="${PLUGIN_SRC_DIR:-$ROOT_DIR/superset-frontend/plugins/plugin-chart-echarts6-gantt}"
SUPERSET_GIT_REF="${SUPERSET_GIT_REF:-}"
SKIP_DOCKER_BUILD="${SKIP_DOCKER_BUILD:-0}"
OUTPUT_IMAGE="${OUTPUT_IMAGE:-${SUPERSET_PLUGIN_IMAGE:-local/superset-echarts6:latest}}"

detect_latest_stable_tag() {
  git ls-remote --tags --refs https://github.com/apache/superset.git \
    | awk '{print $2}' \
    | sed 's#refs/tags/##' \
    | grep -E '^v?[0-9]+\.[0-9]+\.[0-9]+$' \
    | sed 's/^v//' \
    | awk -F. '$1 < 100' \
    | sort -V \
    | tail -1
}

safe_remove_src_dir() {
  if rm -rf "${SUPERSET_SRC_DIR}" 2>/dev/null; then
    return 0
  fi
  echo "[build] Falling back to containerized cleanup for ${SUPERSET_SRC_DIR}..."
  docker run --rm \
    -v "${BUILD_DIR}:/work" \
    alpine:3.20 \
    sh -lc "rm -rf /work/apache-superset"
}

if [[ -z "${SUPERSET_GIT_REF}" ]]; then
  echo "[build] SUPERSET_GIT_REF is not set, detecting latest stable tag..."
  SUPERSET_GIT_REF="$(detect_latest_stable_tag)"
  if [[ -z "${SUPERSET_GIT_REF}" ]]; then
    echo "[build] ERROR: failed to detect latest stable Superset tag" >&2
    exit 1
  fi
fi

if [[ -z "${SUPERSET_SRC_DIR}" ]]; then
  SUPERSET_SRC_DIR="${BUILD_DIR}/apache-superset-${SUPERSET_GIT_REF}-$(date +%s)"
fi

echo "[build] Superset git ref: ${SUPERSET_GIT_REF}"
echo "[build] Build dir: ${BUILD_DIR}"
echo "[build] Superset src dir: ${SUPERSET_SRC_DIR}"
echo "[build] Output image: ${OUTPUT_IMAGE}"

if [[ ! -f "${PLUGIN_SRC_DIR}/package.json" || ! -d "${PLUGIN_SRC_DIR}/src" ]]; then
  echo "[build] ERROR: plugin source is missing at ${PLUGIN_SRC_DIR}" >&2
  echo "[build] Expected files: package.json and src/**" >&2
  exit 1
fi

mkdir -p "${BUILD_DIR}"
if [[ -n "${USER_SUPERSET_SRC_DIR}" && -d "${SUPERSET_SRC_DIR}" ]]; then
  safe_remove_src_dir
fi

echo "[build] Cloning apache/superset..."
git clone --depth 1 --branch "${SUPERSET_GIT_REF}" https://github.com/apache/superset.git "${SUPERSET_SRC_DIR}"

echo "[build] Copying plugin scaffold..."
mkdir -p "${SUPERSET_SRC_DIR}/superset-frontend/plugins/plugin-chart-echarts6-gantt"
cp -R "${PLUGIN_SRC_DIR}/." "${SUPERSET_SRC_DIR}/superset-frontend/plugins/plugin-chart-echarts6-gantt/"

echo "[build] Registering plugin..."
python "${ROOT_DIR}/deploy/superset-local/scripts/register_plugin_in_superset.py" --superset-src "${SUPERSET_SRC_DIR}"
python "${ROOT_DIR}/deploy/superset-local/scripts/patch_webpack_proxy_zstd.py" --superset-src "${SUPERSET_SRC_DIR}"

echo "[build] Installing frontend deps and building assets..."
if command -v npm >/dev/null 2>&1; then
  (
    cd "${SUPERSET_SRC_DIR}/superset-frontend"
    npm install
    npm run build
  )
else
  echo "[build] npm not found on host, using node:20-bullseye container..."
  docker run --rm \
    -u "$(id -u):$(id -g)" \
    -v "${SUPERSET_SRC_DIR}:/src" \
    -w /src/superset-frontend \
    node:20-bullseye \
    bash -lc "npm install && npm run build"
fi

if [[ "${SKIP_DOCKER_BUILD}" != "1" ]]; then
  echo "[build] Building docker image..."
  (
    cd "${SUPERSET_SRC_DIR}"
    docker build -t "${OUTPUT_IMAGE}" .
  )
else
  echo "[build] SKIP_DOCKER_BUILD=1, skipping docker image build"
fi

echo "[build] Done. Image: ${OUTPUT_IMAGE}"
