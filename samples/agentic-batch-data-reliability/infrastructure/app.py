#!/usr/bin/env python3
"""CDK entry point for AWS Data Reliability Copilot."""

import os

import aws_cdk as cdk
from abdr_cdk.stacks.data_reliability_stack import DataReliabilityStack
from cdk_nag import AwsSolutionsChecks

stage = os.getenv("STAGE", "dev")
if stage not in {"dev", "test", "prod"}:
    raise ValueError("STAGE must be one of: dev, test, prod")

app = cdk.App()
stack = DataReliabilityStack(
    app,
    f"abdr-{stage}",
    stage=stage,
    description=(
        "AWS Data Reliability Copilot: deterministic batch reconciliation, evidence-grounded "
        "investigation, human approval, and selective recovery using Amazon S3 Tables."
    ),
)
cdk.Aspects.of(stack).add(AwsSolutionsChecks(verbose=True))
app.synth()
