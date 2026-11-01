#!/usr/bin/env bash
set -euo pipefail

STAGE="${1:-dev}"
REGION="${2:-${AWS_REGION:-us-east-1}}"
STACK_NAME="abdr-${STAGE}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
TABLE_BUCKET_NAME="${STACK_NAME}-curated-${ACCOUNT_ID}"
CATALOG_ID="${ACCOUNT_ID}:s3tablescatalog/${TABLE_BUCKET_NAME}"
NAMESPACE="order_processing"
TABLE_NAME="orders"

stack_output() {
  local key="$1"
  aws cloudformation describe-stacks \
    --region "${REGION}" \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='${key}'].OutputValue | [0]" \
    --output text
}

grant_catalog_access() {
  local principal="$1"
  aws lakeformation grant-permissions \
    --region "${REGION}" \
    --principal "DataLakePrincipalIdentifier=${principal}" \
    --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID}\"}}" \
    --permissions DESCRIBE
  aws lakeformation grant-permissions \
    --region "${REGION}" \
    --principal "DataLakePrincipalIdentifier=${principal}" \
    --resource "{\"Database\":{\"CatalogId\":\"${CATALOG_ID}\",\"Name\":\"${NAMESPACE}\"}}" \
    --permissions DESCRIBE
}

GLUE_ROLE_ARN="$(stack_output OrdersEtlRoleArn)"
VALIDATION_ROLE_ARN="$(stack_output ValidationRoleArn)"

if [[ "${GLUE_ROLE_ARN}" == "None" || "${VALIDATION_ROLE_ARN}" == "None" ]]; then
  echo "Required stack role outputs were not found in ${STACK_NAME}." >&2
  exit 1
fi

grant_catalog_access "${GLUE_ROLE_ARN}"
aws lakeformation grant-permissions \
  --region "${REGION}" \
  --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
  --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"${NAMESPACE}\",\"Name\":\"${TABLE_NAME}\"}}" \
  --permissions ALL

grant_catalog_access "${VALIDATION_ROLE_ARN}"
aws lakeformation grant-permissions \
  --region "${REGION}" \
  --principal "DataLakePrincipalIdentifier=${VALIDATION_ROLE_ARN}" \
  --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"${NAMESPACE}\",\"Name\":\"${TABLE_NAME}\"}}" \
  --permissions SELECT DESCRIBE

echo "Granted scoped Lake Formation permissions for ${STACK_NAME}."
