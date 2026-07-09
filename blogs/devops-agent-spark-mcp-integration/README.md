# Integrate the Apache Spark Troubleshooting Agent for Amazon EMR with AWS DevOps Agent

This repository contains companion artifacts for the AWS Big Data Blog post: **"Integrate the Apache Spark Troubleshooting Agent for Amazon EMR with AWS DevOps Agent."**

The demo shows how to register the Apache Spark Troubleshooting Agent for Amazon EMR — a managed Model Context Protocol (MCP) server — as a custom capability provider inside AWS DevOps Agent, connected over AWS PrivateLink. When a Spark job fails, a CloudWatch alarm fires and the agent investigates end-to-end, identifying the root cause at the source-code line level.

## Architecture

The CloudFormation template creates:
- A dedicated VPC with two subnets in SMUS-supported Availability Zones
- An Interface VPC Endpoint for SageMaker Unified Studio MCP (private DNS enabled)
- An IAM role (`SparkTroubleshootingRole`) trusted by AWS DevOps Agent
- IAM policies for MCP invoke, EMR/EMR Serverless/Glue read access, and S3 access
- An EMR Serverless application (`analytics-events-platform`)
- An S3 bucket for demo artifacts (script, data, and Spark logs)
- A CloudWatch alarm that fires on failed jobs

## Prerequisites

- An AWS account with access to AWS DevOps Agent (us-east-1 only)
- AWS CLI v2 installed and configured

## Setup Instructions

### 1. Deploy the CloudFormation stack

```bash
aws cloudformation create-stack \
  --stack-name spark-troubleshooting-demo \
  --template-url https://raw.githubusercontent.com/aws-samples/sample-aws-data-processing-and-analytics/main/blogs/devops-agent-spark-mcp-integration/cloudformation/spark-troubleshooting-devops-agent-blog.yaml \
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

### 2. Check supported Availability Zones

The SMUS VPC endpoint service may not support all Availability Zones in every account. If the stack fails with an AZ error, check supported AZs:

```bash
aws ec2 describe-vpc-endpoint-services \
  --service-names com.amazonaws.us-east-1.sagemaker-unified-studio-mcp \
  --region us-east-1 \
  --query 'ServiceDetails[0].AvailabilityZones'
```

Then redeploy with supported AZs by updating the subnet configuration in the template.

### 3. Copy demo artifacts to your bucket

Clone this repository and copy the PySpark script and Parquet data into the S3 bucket created by the stack:

```bash
git clone https://github.com/aws-samples/sample-aws-data-processing-and-analytics.git
cd sample-aws-data-processing-and-analytics/blogs/devops-agent-spark-mcp-integration

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

After the job fails (~2 minutes), the CloudWatch alarm fires. In AWS DevOps Agent, navigate to Operator Access → Incidents and start an investigation referencing the alarm name from the stack outputs.

## Clean Up

```bash
# Empty the S3 bucket
aws s3 rm s3://$DEMO_BUCKET --recursive

# Delete the stack
aws cloudformation delete-stack --stack-name spark-troubleshooting-demo --region us-east-1
aws cloudformation wait stack-delete-complete --stack-name spark-troubleshooting-demo --region us-east-1
```

Also remove the private connection, capability provider, and agent space from the AWS DevOps Agent console.

## Repository Structure

```
blogs/devops-agent-spark-mcp-integration/
├── README.md
├── cloudformation/
│   └── spark-troubleshooting-devops-agent-blog.yaml
├── scripts/
│   └── customer_events_aggregator.py
└── data/
    ├── part-00000-50ad5776-dac8-4d0a-b490-923ac93f9f7e-c000.snappy.parquet
    └── part-00001-50ad5776-dac8-4d0a-b490-923ac93f9f7e-c000.snappy.parquet
```

| Directory | Contents |
|-----------|----------|
| `cloudformation/` | CloudFormation template for the full demo infrastructure |
| `scripts/` | Deliberately-failing PySpark script (demo workload) |
| `data/` | Sample customer events Parquet data |

## Region Support

This demo is restricted to **us-east-1** only. The SageMaker Unified Studio MCP endpoint and supported Availability Zones are region-specific.

## Security

See [CONTRIBUTING](https://github.com/aws-samples/sample-aws-data-processing-and-analytics/blob/main/CONTRIBUTING.md) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](https://github.com/aws-samples/sample-aws-data-processing-and-analytics/blob/main/LICENSE) file.
