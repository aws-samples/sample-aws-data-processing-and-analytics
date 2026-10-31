#!/usr/bin/env bash
set -euo pipefail

REGION="${1:?Usage: enable_s3_tables_integration.sh REGION}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"

if aws glue get-catalog --region "${REGION}" --catalog-id s3tablescatalog >/dev/null 2>&1; then
  echo "Shared s3tablescatalog already exists in ${REGION}; leaving it unchanged."
  exit 0
fi

REQUEST_FILE="$(mktemp)"
trap 'rm -f "${REQUEST_FILE}"' EXIT
sed \
  -e "s/REGION_PLACEHOLDER/${REGION}/g" \
  -e "s/ACCOUNT_PLACEHOLDER/${ACCOUNT_ID}/g" \
  "$(dirname "${BASH_SOURCE[0]}")/s3_tables_catalog.json.template" > "${REQUEST_FILE}"

aws glue create-catalog --region "${REGION}" --cli-input-json "file://${REQUEST_FILE}"
echo "Created shared s3tablescatalog in ${REGION}. Cleanup intentionally retains it."
