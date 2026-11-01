import os
import sys
from pathlib import Path

os.environ.setdefault("JSII_RUNTIME_PACKAGE_CACHE_ROOT", "/tmp/abdr-jsii-test-cache")
os.environ.setdefault("JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION", "1")

import aws_cdk as cdk  # noqa: E402
from aws_cdk.assertions import Annotations, Match, Template  # noqa: E402
from cdk_nag import AwsSolutionsChecks  # noqa: E402

PROJECT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_DIR / "infrastructure"))

from abdr_cdk.stacks.data_reliability_stack import DataReliabilityStack  # noqa: E402


def template():
    app = cdk.App()
    stack = DataReliabilityStack(app, "abdr-test", stage="test")
    return Template.from_stack(stack)


def test_stack_contains_s3_table_reliability_workflow_and_trigger():
    synthesized = template()

    synthesized.resource_count_is("AWS::S3Tables::TableBucket", 1)
    synthesized.resource_count_is("AWS::S3Tables::Namespace", 1)
    synthesized.resource_count_is("AWS::S3Tables::Table", 1)
    synthesized.resource_count_is("AWS::Glue::Job", 2)
    synthesized.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    synthesized.resource_count_is("AWS::Events::Rule", 1)


def test_recovery_verification_uses_glue_integration_run_id():
    state_machines = template().find_resources("AWS::StepFunctions::StateMachine")
    assert len(state_machines) == 1
    definition = str(next(iter(state_machines.values()))["Properties"]["DefinitionString"])

    assert "$.recovery_job.Id" in definition
    assert "$.recovery_job.JobRunId" not in definition


def test_athena_workgroup_enforces_scan_and_encryption_controls():
    template().has_resource_properties(
        "AWS::Athena::WorkGroup",
        {
            "WorkGroupConfiguration": {
                "EnforceWorkGroupConfiguration": True,
                "BytesScannedCutoffPerQuery": 1_073_741_824,
                "ResultConfiguration": {"EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}},
            }
        },
    )


def test_raw_table_uses_injected_partition_projection_for_batch_scans():
    template().has_resource_properties(
        "AWS::Glue::Table",
        {
            "TableInput": {
                "Name": "orders",
                "Parameters": {
                    "projection.enabled": "true",
                    "projection.batch_id.type": "injected",
                },
                "PartitionKeys": [{"Name": "batch_id", "Type": "string"}],
            }
        },
    )


def test_lambda_functions_do_not_require_account_concurrency_reservations():
    functions = template().find_resources(
        "AWS::Lambda::Function", {"Properties": {"Runtime": "python3.14"}}
    )

    assert len(functions) == 5
    assert all(
        "ReservedConcurrentExecutions" not in resource["Properties"]
        for resource in functions.values()
    )


def test_bedrock_permission_uses_parameterized_exact_resource():
    synthesized = template()
    for parameter in ("BedrockModelArn", "BedrockFoundationModelArns"):
        synthesized.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": [
                                        "bedrock:InvokeModel",
                                        "bedrock:InvokeModelWithResponseStream",
                                    ],
                                    "Resource": {"Ref": parameter},
                                }
                            )
                        ]
                    )
                }
            },
        )


def test_validation_role_can_request_lake_formation_data_access():
    template().has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Action": Match.array_with(
                                    ["lakeformation:GetDataAccess"]
                                ),
                                "Resource": "*",
                            }
                        )
                    ]
                )
            }
        },
    )


def test_validation_role_can_read_raw_orders_for_athena_queries():
    template().has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Action": Match.array_with(["s3:GetObject*"]),
                                "Resource": Match.array_with(
                                    [
                                        {
                                            "Fn::Join": [
                                                "",
                                                [
                                                    {
                                                        "Fn::GetAtt": [
                                                            "RawDataBucket57F26C03",
                                                            "Arn",
                                                        ]
                                                    },
                                                    "/raw/orders/*",
                                                ],
                                            ]
                                        }
                                    ]
                                ),
                            }
                        )
                    ]
                )
            }
        },
    )


def test_stack_has_no_unsuppressed_aws_solutions_errors():
    app = cdk.App()
    stack = DataReliabilityStack(app, "abdr-nag-test", stage="test")
    cdk.Aspects.of(stack).add(AwsSolutionsChecks(verbose=True))
    app.synth()

    errors = Annotations.from_stack(stack).find_error("*", Match.any_value())
    assert errors == []
