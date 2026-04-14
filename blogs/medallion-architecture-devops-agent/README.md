# Medallion Architecture — Autonomous Troubleshooting with AWS DevOps Agent

This project deploys a medallion data pipeline (Bronze → Silver → Gold) using AWS Glue, Amazon Redshift, and Amazon MWAA, with autonomous incident detection and troubleshooting powered by AWS DevOps Agent.

## Architecture

- **Bronze Layer**: Generates 50,000 synthetic e-commerce order records → Amazon S3 (Parquet)
- **Silver Layer**: Cleanses and transforms data → Amazon S3 + Amazon Redshift
- **Gold Layer**: Aggregates business metrics → Amazon Redshift
- **Orchestration**: Amazon MWAA (Apache Airflow) triggers the pipeline DAG
- **Incident Response**: EventBridge detects Glue job failures → Lambda → DevOps Agent webhook

## Deployment

```bash
aws cloudformation create-stack \
  --stack-name medallion-demo \
  --template-body file://cloudformation/blog-medallion-stack.yaml \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameters ParameterKey=WebhookUrl,ParameterValue=<YOUR_WEBHOOK_URL> \
               ParameterKey=WebhookSecret,ParameterValue=<YOUR_WEBHOOK_SECRET> \
  --region us-west-2
```

## Files

| File | Description |
|---|---|
| `cloudformation/blog-medallion-stack.yaml` | Full CloudFormation stack |
| `glue-scripts/bronze_layer_ingestion.py` | Bronze layer — raw data generation |
| `glue-scripts/silver_layer_transform.py` | Silver layer — cleansing and Redshift load |
| `glue-scripts/gold_layer_aggregate.py` | Gold layer — business aggregation |
| `dags/medallion_pipeline.py` | MWAA DAG orchestrating the pipeline |

## Security

See [CONTRIBUTING](../../CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE) file.
