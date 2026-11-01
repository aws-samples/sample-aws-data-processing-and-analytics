#!/usr/bin/env bash
set -euo pipefail

STAGE="${1:-dev}"
REGION="${2:-${AWS_REGION:-us-east-1}}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"${PROJECT_DIR}/scripts/revoke_lake_formation_permissions.sh" "${STAGE}" "${REGION}"

(
  cd "${PROJECT_DIR}/infrastructure"
  STAGE="${STAGE}" uv run cdk destroy "abdr-${STAGE}" --region "${REGION}" --force
)

echo "Stack removed. The shared regional s3tablescatalog was intentionally retained."
