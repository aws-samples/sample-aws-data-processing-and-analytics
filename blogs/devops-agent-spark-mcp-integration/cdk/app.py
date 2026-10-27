#!/usr/bin/env python3
import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks

from spark_troubleshooting.stack import SparkTroubleshootingStack

app = cdk.App()

# The stack ID doubles as the CloudFormation stack name, matching the name used
# in the blog post (spark-troubleshooting-demo).
SparkTroubleshootingStack(
    app,
    "spark-troubleshooting-demo",
    description=(
        "Integrate the Apache Spark Troubleshooting Agent for Amazon EMR with AWS "
        "DevOps Agent over AWS PrivateLink. Companion CDK app for the AWS Big Data "
        "Blog post. Creates a VPC + subnets, an Interface VPC Endpoint for SageMaker "
        "Unified Studio MCP, the SigV4 IAM role for AWS DevOps Agent, and a demo EMR "
        "Serverless workload. us-east-1 only."
    ),
)

cdk.Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

app.synth()
