#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/.build/lambda"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

uv pip install \
  --target "${BUILD_DIR}" \
  --python-version 3.14 \
  --python-platform x86_64-manylinux2014 \
  -r "${PROJECT_DIR}/requirements.txt"
cp -R "${PROJECT_DIR}/agentic_batch_data_reliability" "${BUILD_DIR}/"

echo "Packaged Lambda dependencies at ${BUILD_DIR}"
