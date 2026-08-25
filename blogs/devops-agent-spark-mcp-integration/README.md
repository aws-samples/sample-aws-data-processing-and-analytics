# Integrate the Apache Spark Troubleshooting Agent for Amazon EMR with AWS DevOps Agent

This repository contains companion artifacts for the AWS Big Data Blog post: **"Integrate the Apache Spark Troubleshooting Agent for Amazon EMR with AWS DevOps Agent."**

The demo shows how to register the Apache Spark Troubleshooting Agent for Amazon EMR — a managed Model Context Protocol (MCP) server — as a custom capability provider inside AWS DevOps Agent, connected over AWS PrivateLink. When a Spark job fails, a CloudWatch alarm fires and the agent investigates end-to-end, identifying the root cause at the source-code line level.

## Architecture

The CloudFormation template creates:
- A dedicated VPC with two subnets. The Availability Zones are discovered dynamically at deploy time by a Lambda-backed custom resource — see `scripts/az_discovery.py` — so the same template works in any account regardless of the account's physical-AZ-to-logical-AZ mapping.
- An Interface VPC Endpoint for SageMaker Unified Studio MCP (private DNS enabled)
- An IAM role (`SparkTroubleshootingRole`) trusted by AWS DevOps Agent
- IAM policies for MCP invoke, EMR/EMR Serverless/Glue read access, and S3 access
- An EMR Serverless application (`analytics-events-platform`)
- An S3 bucket for demo artifacts (script, data, and Spark logs)
- A Lambda-backed custom resource — see `scripts/bucket_cleanup.py` — that empties the demo bucket automatically on stack delete, so `cloudformation delete-stack` succeeds without a manual `aws s3 rm --recursive` step
- A CloudWatch alarm that fires on failed jobs

## Prerequisites

- An AWS account with access to AWS DevOps Agent (us-east-1 only)
- AWS CLI v2 installed and configured

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/aws-samples/sample-aws-data-processing-and-analytics.git
cd sample-aws-data-processing-and-analytics/blogs/devops-agent-spark-mcp-integration
```

### 2. Deploy the CloudFormation stack

```bash
aws cloudformation create-stack \
  --stack-name spark-troubleshooting-demo \
  --template-body file://cloudformation/spark-troubleshooting-devops-agent-blog.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

Wait for the stack to complete:

```bash
aws cloudformation wait stack-create-complete --stack-name spark-troubleshooting-demo --region us-east-1
```

Capture the stack outputs:

```bash
aws cloudformation describe-stacks --stack-name spark-troubleshooting-demo --region us-east-1 --query 'Stacks[0].Outputs'
```

### 3. Copy demo artifacts to your bucket

```bash
# Get your bucket name from the stack outputs
DEMO_BUCKET=$(aws cloudformation describe-stacks --stack-name spark-troubleshooting-demo --region us-east-1 --query 'Stacks[0].Outputs[?OutputKey==`DemoBucket`].OutputValue' --output text)

# Copy the script
aws s3 cp scripts/customer_events_aggregator.py s3://$DEMO_BUCKET/customer_events_aggregator.py

# Copy the Parquet data
aws s3 cp data/ s3://$DEMO_BUCKET/data/ --recursive
```

### 4. Configure AWS DevOps Agent

Follow the blog post to:
1. Create a private connection pointing to the VPC and subnets from the stack outputs
2. Register the Apache Spark Troubleshooting MCP server as a capability provider using the `TroubleshootingRoleArn` and `MCPEndpointURL` from the stack outputs
3. Create an agent space and attach the MCP capability provider

### 5. Submit the failing job

Use the `DemoSubmitJobCommand` from the stack outputs, or run:

```bash
APP_ID=$(aws cloudformation describe-stacks --stack-name spark-troubleshooting-demo --region us-east-1 --query 'Stacks[0].Outputs[?OutputKey==`DemoApplicationId`].OutputValue' --output text)
EXEC_ROLE=$(aws cloudformation describe-stacks --stack-name spark-troubleshooting-demo --region us-east-1 --query 'Stacks[0].Outputs[?OutputKey==`DemoExecutionRoleArn`].OutputValue' --output text)

aws emr-serverless start-job-run \
  --region us-east-1 \
  --application-id $APP_ID \
  --execution-role-arn $EXEC_ROLE \
  --name daily-customer-events-rollup \
  --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"s3://$DEMO_BUCKET/customer_events_aggregator.py\",\"entryPointArguments\":[\"$DEMO_BUCKET\"],\"sparkSubmitParameters\":\"--conf spark.executor.cores=2 --conf spark.executor.memory=1g --conf spark.executor.pyspark.memory=256m --conf spark.executor.instances=2\"}}" \
  --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://$DEMO_BUCKET/logs/\"}}}"
```

### 6. Investigate with AWS DevOps Agent

After the job fails (~3–4 minutes), the CloudWatch alarm fires. In AWS DevOps Agent, navigate to Operator Access → Incidents and start an investigation referencing the alarm name from the stack outputs.

## Clean Up

Order matters here. The AWS DevOps Agent private connection provisions network interfaces (ENIs) into the stack's subnets. If those ENIs still exist at stack-delete time, CloudFormation fails to delete the subnets. Remove the private connection first, then delete the stack.

1. **In the AWS DevOps Agent console**, in your agent space's MCP Server section, choose **Remove** for the `spark-troubleshooting` capability provider.
2. **In Capability Providers**, choose **Deregister** for `spark-troubleshooting`.
3. **In Capability Providers → Private connections**, choose **Delete** for the private connection. Wait until it's gone from the console list (~1–2 minutes).
4. **(Optional) Delete the agent space** if you no longer need it.
5. **Then run**:

    ```bash
    aws cloudformation delete-stack --stack-name spark-troubleshooting-demo --region us-east-1
    aws cloudformation wait stack-delete-complete --stack-name spark-troubleshooting-demo --region us-east-1
    ```

No manual `aws s3 rm` is required — a Lambda-backed custom resource empties the demo bucket automatically as part of the stack delete.

## Repository Structure

```
blogs/devops-agent-spark-mcp-integration/
├── README.md
├── cloudformation/
│   └── spark-troubleshooting-devops-agent-blog.yaml
├── scripts/
│   ├── customer_events_aggregator.py
│   ├── az_discovery.py
│   └── bucket_cleanup.py
└── data/
    ├── part-00000-50ad5776-dac8-4d0a-b490-923ac93f9f7e-c000.snappy.parquet
    └── part-00001-50ad5776-dac8-4d0a-b490-923ac93f9f7e-c000.snappy.parquet
```

| Directory | Contents |
|-----------|----------|
| `cloudformation/` | CloudFormation template for the full demo infrastructure |
| `scripts/` | `customer_events_aggregator.py` is the deliberately-failing PySpark script the demo runs. `az_discovery.py` and `bucket_cleanup.py` are readable source-of-truth copies of the two custom-resource Lambdas whose code is embedded inline in the CloudFormation template (`ZipFile`). The customer does not upload these two files anywhere; they exist only for humans reading the repo. |
| `data/` | Sample customer events Parquet data |

## Region Support

This demo is restricted to **us-east-1** only. The SageMaker Unified Studio MCP endpoint and supported Availability Zones are region-specific.

## Security

See [CONTRIBUTING](https://github.com/aws-samples/sample-aws-data-processing-and-analytics/blob/main/CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](https://github.com/aws-samples/sample-aws-data-processing-and-analytics/blob/main/LICENSE) file.
