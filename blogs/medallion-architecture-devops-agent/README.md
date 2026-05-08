# Medallion Architecture — Autonomous Troubleshooting with AWS DevOps Agent

This project deploys a medallion data pipeline (Bronze → Silver → Gold) using AWS Glue, Amazon Redshift, and Amazon MWAA, with autonomous incident detection and troubleshooting powered by [AWS DevOps Agent](https://aws.amazon.com/devops-agent/).

## Architecture

- **Bronze Layer**: Generates 50,000 synthetic e-commerce order records → Amazon S3 (Parquet)
- **Silver Layer**: Cleanses and transforms data → Amazon S3 + Amazon Redshift
- **Gold Layer**: Aggregates business metrics → Amazon Redshift
- **Orchestration**: Amazon MWAA (Apache Airflow) triggers the pipeline DAG
- **Incident Response**: EventBridge detects Glue job failures → Lambda → DevOps Agent webhook

## Prerequisites

- An AWS account with permissions to create IAM roles, VPCs, Glue jobs, Redshift clusters, and MWAA environments
- An [AWS DevOps Agent](https://aws.amazon.com/devops-agent/) Agent Space with a webhook URL configured
- AWS CLI v2 (for CLI deployment) or access to the AWS Management Console

## Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `WebhookUrl` | Yes | Your DevOps Agent webhook URL (from Agent Space settings) |
| `WebhookSecret` | Yes | Webhook secret for authentication (min 16 characters) |

> **Note:** Glue scripts and the MWAA DAG are automatically downloaded from this GitHub repository during stack creation via a Custom Resource Lambda. No manual script upload is required.

## Deployment

### Option A: AWS Management Console (Recommended)

1. Download the [CloudFormation template](cloudformation/blog-medallion-stack.yaml)
2. Open the [AWS CloudFormation Console](https://console.aws.amazon.com/cloudformation/home#/stacks/create)
3. Select **Create stack** → **Upload a template file** → choose the downloaded `blog-medallion-stack.yaml` → **Next**
4. Stack name: `medallion-troubleshooting`
5. Fill in parameters:
   - **WebhookUrl** — Your DevOps Agent webhook URL
   - **WebhookSecret** — Your webhook secret (min 16 chars)
6. Click **Next** → check **I acknowledge that AWS CloudFormation might create IAM resources with custom names** → **Submit**

### Option B: AWS CLI

```bash
aws cloudformation create-stack \
  --stack-name medallion-troubleshooting \
  --template-body file://cloudformation/blog-medallion-stack.yaml \
  --parameters \
    ParameterKey=WebhookUrl,ParameterValue=YOUR-WEBHOOK-URL \
    ParameterKey=WebhookSecret,ParameterValue=YOUR-WEBHOOK-SECRET \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-west-2
```

## What Gets Deployed

The stack creates the following resources:

- **VPC** with private subnets, NAT Gateway, and VPC endpoints
- **S3 Buckets** for Glue scripts, data lake storage, and MWAA DAGs
- **AWS Glue** — 3 ETL jobs (Bronze, Silver, Gold) with IAM roles
- **Amazon Redshift** — Single-node cluster (ra3.large) with auto-generated credentials in Secrets Manager
- **Amazon MWAA** — Airflow environment with pipeline DAG
- **EventBridge Rule** — Triggers on Glue job failures
- **Lambda Functions** — Webhook executor (forwards events to DevOps Agent) and script populator (auto-downloads scripts from GitHub)
- **SQS Dead Letter Queues** — For Lambda error handling

## How It Works

1. **Stack creation** → Lambda auto-downloads Glue scripts and DAG from this GitHub repo to S3
2. **MWAA DAG** triggers the Bronze → Silver → Gold pipeline on a schedule
3. **If a Glue job fails** → EventBridge captures the failure → Lambda sends the event to your DevOps Agent webhook
4. **DevOps Agent** autonomously investigates the failure — correlating logs, metrics, and code to identify root cause and provide a mitigation plan

## Files

| File | Description |
|---|---|
| `cloudformation/blog-medallion-stack.yaml` | Full CloudFormation stack |
| `glue-scripts/bronze_layer_ingestion.py` | Bronze layer — raw data generation |
| `glue-scripts/silver_layer_transform.py` | Silver layer — cleansing and Redshift load |
| `glue-scripts/gold_layer_aggregate.py` | Gold layer — business aggregation |
| `dags/medallion_pipeline.py` | MWAA DAG orchestrating the pipeline |

## Cleanup

```bash
aws cloudformation delete-stack --stack-name medallion-troubleshooting --region us-west-2
```

## Security

See [CONTRIBUTING](../../CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE) file.
