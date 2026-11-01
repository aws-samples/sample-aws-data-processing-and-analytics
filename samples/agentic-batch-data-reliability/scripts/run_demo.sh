#!/usr/bin/env bash
set -euo pipefail

STAGE="${1:-dev}"
REGION="${2:-${AWS_REGION:-us-east-1}}"
SCENARIO="${SCENARIO:-missing_records}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STACK_NAME="abdr-${STAGE}"
BATCH_ID="batch-$(date -u +%Y%m%dT%H%M%SZ)"
DEMO_DIR="$(mktemp -d)"
trap 'rm -rf "${DEMO_DIR}"' EXIT

stack_output() {
  aws cloudformation describe-stacks \
    --region "${REGION}" \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue | [0]" \
    --output text
}

RAW_BUCKET="$(stack_output RawBucketName)"
REPORT_BUCKET="$(stack_output ReportBucketName)"
JOB_NAME="$(stack_output OrdersJobName)"

uv run --project "${PROJECT_DIR}" abdr-generate \
  --batch-id "${BATCH_ID}" \
  --rows "${ROWS:-100}" \
  --seed "${SEED:-42}" \
  --scenario "${SCENARIO}" \
  --defer-injection \
  --output "${DEMO_DIR}"

aws s3 cp "${DEMO_DIR}/orders.jsonl" \
  "s3://${RAW_BUCKET}/raw/orders/batch_id=${BATCH_ID}/orders.jsonl" --region "${REGION}"
aws s3 cp "${DEMO_DIR}/manifest.json" \
  "s3://${RAW_BUCKET}/manifests/batch_id=${BATCH_ID}/manifest.json" --region "${REGION}"

MANIFEST_URI="s3://${RAW_BUCKET}/manifests/batch_id=${BATCH_ID}/manifest.json"
RUN_ID="$(aws glue start-job-run \
  --region "${REGION}" \
  --job-name "${JOB_NAME}" \
  --arguments "{\"--batch_id\":\"${BATCH_ID}\",\"--manifest_uri\":\"${MANIFEST_URI}\",\"--failure_scenario\":\"${SCENARIO}\"}" \
  --query JobRunId --output text)"

echo "Started ${JOB_NAME} run ${RUN_ID} for ${BATCH_ID} (${SCENARIO})."
echo "EventBridge starts validation after Glue reports SUCCEEDED."
echo "Validation and approval artifacts: s3://${REPORT_BUCKET}/reports/"
