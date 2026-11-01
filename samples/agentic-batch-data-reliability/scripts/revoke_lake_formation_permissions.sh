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

revoke_catalog_access() {
  local principal="$1"
  aws lakeformation revoke-permissions \
    --region "${REGION}" \
    --principal "DataLakePrincipalIdentifier=${principal}" \
    --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID}\"}}" \
    --permissions DESCRIBE
  aws lakeformation revoke-permissions \
    --region "${REGION}" \
    --principal "DataLakePrincipalIdentifier=${principal}" \
    --resource "{\"Database\":{\"CatalogId\":\"${CATALOG_ID}\",\"Name\":\"${NAMESPACE}\"}}" \
    --permissions DESCRIBE
}

GLUE_ROLE_ARN="$(stack_output OrdersEtlRoleArn)"
VALIDATION_ROLE_ARN="$(stack_output ValidationRoleArn)"

revoke_catalog_access "${GLUE_ROLE_ARN}"
aws lakeformation revoke-permissions \
  --region "${REGION}" \
  --principal "DataLakePrincipalIdentifier=${GLUE_ROLE_ARN}" \
  --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"${NAMESPACE}\",\"Name\":\"${TABLE_NAME}\"}}" \
  --permissions ALL

revoke_catalog_access "${VALIDATION_ROLE_ARN}"
aws lakeformation revoke-permissions \
  --region "${REGION}" \
  --principal "DataLakePrincipalIdentifier=${VALIDATION_ROLE_ARN}" \
  --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"${NAMESPACE}\",\"Name\":\"${TABLE_NAME}\"}}" \
  --permissions SELECT DESCRIBE

echo "Revoked scoped Lake Formation permissions for ${STACK_NAME}."
