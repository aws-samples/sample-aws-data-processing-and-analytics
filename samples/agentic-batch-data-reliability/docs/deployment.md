# Deployment

## Prerequisites

- An AWS account and AWS CLI v2 credentials
- Python 3.11 or later
- A CDK-supported Node.js release and AWS CDK CLI v2.1146.1 or later
- `uv`
- Permissions to deploy the resources described in `docs/security.md`
- Lake Formation data-lake administrator access, or permission to grant access on the sample's
  S3 Tables child catalog. IAM `AdministratorAccess` alone does not grant Lake Formation access.
- A tool-capable Amazon Bedrock model enabled in the deployment Region
- A Region where S3 Tables, Glue 5.1, Athena, Step Functions, and Bedrock are available

S3 Tables analytics integration currently supports IAM access control in the Regions listed in the
[official integration overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integration-overview.html).
Check that page at deployment time because Region support can change.

## Install and validate locally

```bash
cd samples/agentic-batch-data-reliability
uv sync --dev
uv pip install -r infrastructure/requirements-dev.txt
make test
make lint
make synth
```

## Configure the model

Supply both the runtime ID and exact IAM resource ARN. This avoids granting the investigation Lambda
permission to invoke every model in the account.

```bash
export BEDROCK_MODEL_ID='<tool-capable-model-or-inference-profile-id>'
export BEDROCK_MODEL_ARN='<exact-model-or-inference-profile-arn>'
export BEDROCK_FOUNDATION_MODEL_ARN='<exact-underlying-foundation-model-arn>'
```

Inference-profile calls require authorization for both the profile and its underlying foundation
model. If invoking a foundation model directly, set both ARN variables to that model ARN. Do not
copy an example model ID blindly. Availability and inference-profile requirements vary by Region
and account.

## Deploy

```bash
make deploy STAGE=dev AWS_REGION=us-east-1
```

The command packages Linux-compatible Lambda dependencies, deploys `abdr-dev`, checks the shared
regional `s3tablescatalog`, and grants the deployed Glue and validation roles access only to the
sample's child catalog, namespace, and table. If the shared catalog is absent, the helper creates it
using the current AWS analytics integration shape. It never changes or deletes a pre-existing
integration.

The operator needs `glue:CreateCatalog` and `glue:PassConnection` only when the shared integration is
missing. Review the generated CloudFormation change set when CDK requests approval.
The post-deployment grant step requires a Lake Formation data-lake administrator (or a principal
with grantable permissions on the child catalog). Have the account's Lake Formation administrator
run the following if deployment and data-governance duties are separated:

```bash
./scripts/grant_lake_formation_permissions.sh dev us-east-1
```

## Outputs

```bash
aws cloudformation describe-stacks \
  --stack-name abdr-dev \
  --query 'Stacks[0].Outputs'
```

Important outputs include raw/report bucket names, primary and recovery Glue job names, the state
machine ARN, and the S3 Tables child catalog.

## Manual S3 Tables verification

```bash
aws glue get-catalog --catalog-id s3tablescatalog --region us-east-1
aws s3tables list-namespaces \
  --table-bucket-arn \
  "arn:aws:s3tables:us-east-1:${AWS_ACCOUNT_ID}:bucket/abdr-dev-curated-${AWS_ACCOUNT_ID}"
```

In Athena, select catalog `s3tablescatalog/abdr-dev-curated-<account-id>`, database
`order_processing`, and table `orders`. Do not specify a normal S3 `LOCATION` for the S3 table.

## Deployment verification not performed by local tests

Local synthesis does not prove service availability, Bedrock model access, Lake Formation settings,
Glue-to-S3-Tables writes, Athena federation, EventBridge delivery, or callback execution. Run the AWS
integration checklist in `docs/demo.md` before presenting the sample as end-to-end verified.
