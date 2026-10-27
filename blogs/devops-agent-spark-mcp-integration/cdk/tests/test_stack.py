"""Assertions for the CDK port of the Spark troubleshooting / DevOps Agent demo."""
import filecmp
from pathlib import Path

import aws_cdk as cdk
import pytest
import yaml
from aws_cdk.assertions import Annotations, Match, Template
from cdk_nag import AwsSolutionsChecks

from spark_troubleshooting.stack import SparkTroubleshootingStack

BLOG_DIR = Path(__file__).resolve().parents[2]
CFN_TEMPLATE = BLOG_DIR / "cloudformation" / "spark-troubleshooting-devops-agent-blog.yaml"


@pytest.fixture(scope="module")
def app_and_stack():
    app = cdk.App()
    stack = SparkTroubleshootingStack(app, "spark-troubleshooting-demo")
    cdk.Aspects.of(app).add(AwsSolutionsChecks())
    return app, stack


@pytest.fixture(scope="module")
def stack(app_and_stack):
    return app_and_stack[1]


@pytest.fixture(scope="module")
def template(stack):
    return Template.from_stack(stack)


def _cfn_template_sections():
    """Parse the original CloudFormation template, ignoring intrinsic-function tags."""

    class Loader(yaml.SafeLoader):
        pass

    Loader.add_multi_constructor("!", lambda loader, suffix, node: None)
    return yaml.load(CFN_TEMPLATE.read_text(), Loader=Loader)


# --- parity with cloudformation/spark-troubleshooting-devops-agent-blog.yaml ---


# Outputs that exist only in the CDK app (see the agent space role below).
CDK_ONLY_OUTPUTS = {"AgentSpaceRoleName", "AgentSpaceRoleArn"}


def test_outputs_match_cloudformation_template(template):
    expected = set(_cfn_template_sections()["Outputs"])
    assert set(template.to_json()["Outputs"]) == expected | CDK_ONLY_OUTPUTS


def test_parameters_match_cloudformation_template(template):
    expected = set(_cfn_template_sections()["Parameters"])
    actual = set(template.to_json()["Parameters"]) - {"BootstrapVersion"}
    assert actual == expected


def test_region_rule_restricts_to_us_east_1(template):
    rule = template.to_json()["Rules"]["RegionMustBeUsEast1"]
    assert rule["Assertions"][0]["Assert"] == {
        "Fn::Equals": [{"Ref": "AWS::Region"}, "us-east-1"]
    }


# --- networking ---


def test_vpc_and_subnets(template):
    template.has_resource_properties(
        "AWS::EC2::VPC", {"EnableDnsSupport": True, "EnableDnsHostnames": True}
    )
    template.resource_count_is("AWS::EC2::Subnet", 2)
    # Subnet AZs come from the discovery custom resource, never hardcoded.
    template.has_resource_properties(
        "AWS::EC2::Subnet",
        {"AvailabilityZone": {"Fn::GetAtt": ["SMUSAzDiscovery", "SubnetAZ1"]}},
    )
    template.has_resource_properties(
        "AWS::EC2::Subnet",
        {"AvailabilityZone": {"Fn::GetAtt": ["SMUSAzDiscovery", "SubnetAZ2"]}},
    )


def test_interface_endpoint_with_private_dns(template):
    template.has_resource_properties(
        "AWS::EC2::VPCEndpoint",
        {
            "VpcEndpointType": "Interface",
            "PrivateDnsEnabled": True,
            "ServiceName": {
                "Fn::Join": [
                    "",
                    ["com.amazonaws.", {"Ref": "AWS::Region"}, ".sagemaker-unified-studio-mcp"],
                ]
            },
        },
    )


def test_endpoint_security_group_allows_only_https_from_vpc(template):
    template.has_resource_properties(
        "AWS::EC2::SecurityGroup",
        {
            "SecurityGroupIngress": [
                Match.object_like(
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 443,
                        "ToPort": 443,
                        "CidrIp": {"Fn::GetAtt": ["DemoVPC", "CidrBlock"]},
                    }
                )
            ]
        },
    )


# --- custom-resource Lambdas use the scripts/ sources ---


def test_az_discovery_lambda(template):
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Handler": "az_discovery.lambda_handler",
            "Runtime": "python3.14",
            "ReservedConcurrentExecutions": 1,
            "Timeout": 30,
        },
    )
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {"Action": "ec2:DescribeVpcEndpointServices", "Resource": "*"}
                        )
                    ]
                )
            }
        },
    )


def test_bucket_cleanup_runs_before_bucket_delete(template):
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Handler": "bucket_cleanup.lambda_handler",
            "Runtime": "python3.14",
            "ReservedConcurrentExecutions": 1,
            "Timeout": 300,
        },
    )
    template.has_resource(
        "AWS::CloudFormation::CustomResource",
        {
            "Properties": {"BucketName": {"Ref": Match.string_like_regexp("^DemoBucket")}},
            "DependsOn": Match.array_with([Match.string_like_regexp("^DemoBucket")]),
        },
    )


# --- demo bucket, script and data ---


def test_demo_bucket_is_private_encrypted_and_versioned(template):
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketName": {
                "Fn::Join": [
                    "",
                    ["spark-troubleshooting-", {"Ref": "AWS::AccountId"}, "-", {"Ref": "AWS::Region"}],
                ]
            },
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            },
            "OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
            "VersioningConfiguration": {"Status": "Enabled"},
        },
    )
    template.has_resource_properties(
        "AWS::S3::BucketPolicy",
        {
            "PolicyDocument": {
                "Statement": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "Effect": "Deny",
                                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                            }
                        )
                    ]
                )
            }
        },
    )


def test_script_and_data_are_deployed_without_pruning(template):
    template.resource_count_is("Custom::CDKBucketDeployment", 2)
    # Script lands at the bucket root; pruning would delete the logs/ prefix.
    template.has_resource_properties(
        "Custom::CDKBucketDeployment",
        {"Prune": False, "DestinationBucketKeyPrefix": Match.absent()},
    )
    template.has_resource_properties(
        "Custom::CDKBucketDeployment",
        {"Prune": False, "DestinationBucketKeyPrefix": "data/"},
    )


def test_packaged_assets_are_the_blog_folders_own_files(app_and_stack, template):
    """The CDK app ships the repo's scripts/ and data/ files, not copies."""
    app, _ = app_and_stack
    out_dir = Path(app.synth().directory)
    packaged = {}
    for asset_dir in out_dir.glob("asset.*"):
        if asset_dir.is_dir():
            for f in asset_dir.iterdir():
                packaged[f.name] = f

    expected = {
        **{p.name: p for p in (BLOG_DIR / "scripts").glob("*.py")},
        **{p.name: p for p in (BLOG_DIR / "data").iterdir()},
    }
    assert expected, "scripts/ and data/ should not be empty"
    for name, source in expected.items():
        assert name in packaged, f"{name} is not packaged by the CDK app"
        assert filecmp.cmp(source, packaged[name], shallow=False), f"{name} differs from source"


# --- IAM ---


def test_troubleshooting_role_trust_and_name(template):
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": {"Ref": "TroubleshootingRoleName"},
            "AssumeRolePolicyDocument": {
                "Statement": [
                    Match.object_like(
                        {
                            "Principal": {"Service": "aidevops.amazonaws.com"},
                            "Condition": {
                                "StringEquals": {"aws:SourceAccount": {"Ref": "AWS::AccountId"}},
                                "ArnLike": Match.object_like({"aws:SourceArn": Match.any_value()}),
                            },
                        }
                    ),
                    Match.object_like({"Principal": {"AWS": Match.any_value()}}),
                ],
                "Version": "2012-10-17",
            },
        },
    )


@pytest.mark.parametrize(
    "policy_name",
    [
        "MCPInvokeAccess",
        "EMRReadAccess",
        "EMRServerlessReadAccess",
        "GlueReadAccess",
        "SourceCodeAndEventLogsAccess",
    ],
)
def test_troubleshooting_policies_attached_to_role(template, policy_name):
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyName": policy_name,
            "Roles": [
                Match.object_like(
                    {"Ref": Match.string_like_regexp("^SparkTroubleshootingRole")}
                )
            ],
        },
    )


def test_mcp_invoke_actions(template):
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyName": "MCPInvokeAccess",
            "PolicyDocument": {
                "Statement": [
                    Match.object_like(
                        {
                            "Action": [
                                "sagemaker-unified-studio-mcp:InvokeMcp",
                                "sagemaker-unified-studio-mcp:CallReadOnlyTool",
                                "sagemaker-unified-studio-mcp:CallPrivilegedTool",
                            ]
                        }
                    )
                ]
            },
        },
    )


def test_demo_execution_role(template):
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": "EMRServerlessExecutionRole-AnalyticsEvents",
            "AssumeRolePolicyDocument": {
                "Statement": [
                    Match.object_like({"Principal": {"Service": "emr-serverless.amazonaws.com"}})
                ],
                "Version": "2012-10-17",
            },
        },
    )


AGENT_SPACE_ROLE_NAME = "DevOpsAgentRole-AgentSpace-SparkTroubleshooting"


def test_agent_space_role_trust_and_managed_policy(template):
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": AGENT_SPACE_ROLE_NAME,
            "AssumeRolePolicyDocument": {
                "Statement": [
                    Match.object_like(
                        {
                            "Principal": {"Service": "aidevops.amazonaws.com"},
                            "Condition": {
                                "StringEquals": {"aws:SourceAccount": {"Ref": "AWS::AccountId"}},
                                "ArnLike": {"aws:SourceArn": Match.any_value()},
                            },
                        }
                    )
                ],
                "Version": "2012-10-17",
            },
            "ManagedPolicyArns": [
                {
                    "Fn::Join": [
                        "",
                        [
                            "arn:",
                            {"Ref": "AWS::Partition"},
                            ":iam::aws:policy/AIDevOpsAgentAccessPolicy",
                        ],
                    ]
                }
            ],
        },
    )


def test_agent_space_role_can_only_read_demo_bucket_and_job_runs(template):
    policies = template.find_resources(
        "AWS::IAM::Policy", {"Properties": {"PolicyName": "SparkDemoAgentReadAccess"}}
    )
    assert len(policies) == 1
    statements = next(iter(policies.values()))["Properties"]["PolicyDocument"]["Statement"]
    actions = sorted(
        a
        for s in statements
        for a in ([s["Action"]] if isinstance(s["Action"], str) else s["Action"])
    )
    assert actions == [
        "emr-serverless:GetJobRun",
        "iam:CreateServiceLinkedRole",
        "s3:GetObject",
        "s3:ListBucket",
    ]
    assert all(s["Effect"] == "Allow" for s in statements)
    # No statement may use a bare "*" resource.
    assert all(s["Resource"] != "*" for s in statements)


# --- demo workload and alarm ---


def test_emr_serverless_application(template):
    template.has_resource_properties(
        "AWS::EMRServerless::Application",
        {
            "Name": "analytics-events-platform",
            "ReleaseLabel": "emr-7.5.0",
            "Type": "SPARK",
            "MaximumCapacity": {"Cpu": "16 vCPU", "Memory": "64 GB", "Disk": "42 GB"},
            "AutoStopConfiguration": {"Enabled": True, "IdleTimeoutMinutes": 15},
            "MonitoringConfiguration": {
                "ManagedPersistenceMonitoringConfiguration": {"Enabled": True},
                "S3MonitoringConfiguration": Match.object_like({"LogUri": Match.any_value()}),
            },
        },
    )


def test_failed_jobs_alarm(template):
    template.has_resource_properties(
        "AWS::CloudWatch::Alarm",
        {
            "AlarmName": {
                "Fn::Join": [
                    "",
                    [{"Fn::GetAtt": ["DemoApplication", "ApplicationId"]}, "-FailedJobs"],
                ]
            },
            "Namespace": "AWS/EMRServerless",
            "MetricName": "FailedJobs",
            "Dimensions": [
                {"Name": "ApplicationId", "Value": {"Fn::GetAtt": ["DemoApplication", "ApplicationId"]}}
            ],
            "Statistic": "Sum",
            "Period": 60,
            # 1 breaching period out of the last 5: tolerates the delayed
            # publication of EMR Serverless metrics.
            "EvaluationPeriods": 5,
            "DatapointsToAlarm": 1,
            "Threshold": 1,
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": "notBreaching",
        },
    )


def test_stack_outputs_reference_expected_values(template):
    outputs = template.to_json()["Outputs"]
    assert outputs["MCPServiceName"]["Value"] == "sagemaker-unified-studio-mcp"
    assert "spark-troubleshooting/mcp" in str(outputs["MCPEndpointURL"]["Value"])
    submit = str(outputs["DemoSubmitJobCommand"]["Value"])
    assert "--name daily-customer-events-rollup" in submit
    assert "spark.executor.pyspark.memory=256m" in submit


# --- cdk-nag ---


def test_no_unsuppressed_cdk_nag_findings(stack, template):
    annotations = Annotations.from_stack(stack)
    pattern = Match.string_like_regexp("AwsSolutions-.*")
    assert annotations.find_error("*", pattern) == []
    assert annotations.find_warning("*", pattern) == []
