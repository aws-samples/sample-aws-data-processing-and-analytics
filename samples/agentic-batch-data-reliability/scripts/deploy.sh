#!/usr/bin/env bash
set -euo pipefail

STAGE="${1:-dev}"
REGION="${2:-${AWS_REGION:-us-east-1}}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${BEDROCK_MODEL_ID:?Set BEDROCK_MODEL_ID to a tool-capable model or inference-profile ID}"
: "${BEDROCK_MODEL_ARN:?Set BEDROCK_MODEL_ARN to the exact resource ARN allowed for invocation}"

if [[ ! "${STAGE}" =~ ^(dev|test|prod)$ ]]; then
  echo "Stage must be dev, test, or prod" >&2
  exit 2
fi
if [[ ! -d "${PROJECT_DIR}/.build/lambda" ]]; then
  "${PROJECT_DIR}/scripts/package_agent.sh"
fi

export STAGE
export ABDR_LAMBDA_ASSET="${PROJECT_DIR}/.build/lambda"
(
  cd "${PROJECT_DIR}/infrastructure"
  uv run cdk deploy "abdr-${STAGE}" \
    --region "${REGION}" \
    --require-approval broadening \
    --parameters "BedrockModelId=${BEDROCK_MODEL_ID}" \
    --parameters "BedrockModelArn=${BEDROCK_MODEL_ARN}"
)

"${PROJECT_DIR}/scripts/enable_s3_tables_integration.sh" "${REGION}"
"${PROJECT_DIR}/scripts/grant_lake_formation_permissions.sh" "${STAGE}" "${REGION}"
echo "Deployment complete. The regional s3tablescatalog is shared and will not be deleted."
