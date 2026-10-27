"""CDK port of cloudformation/spark-troubleshooting-devops-agent-blog.yaml.

Integrates the Apache Spark Troubleshooting Agent for Amazon EMR with AWS
DevOps Agent over AWS PrivateLink. Resource names and stack outputs
intentionally match the CloudFormation template so the blog post's
walkthrough applies unchanged to either deployment path. The one addition is
the agent space role (and its two outputs), see the README.
"""
from pathlib import Path

from aws_cdk import (
    Aws,
    CfnOutput,
    CfnParameter,
    CfnResource,
    CfnRule,
    CfnRuleAssertion,
    CfnTag,
    CustomResource,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    Tags,
    aws_cloudwatch as cloudwatch,
    aws_ec2 as ec2,
    aws_emrserverless as emrserverless,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from cdk_nag import NagPackSuppression, NagSuppressions
from constructs import Construct

# The blog folder's own scripts/ and data/ are used as-is:
#  * az_discovery.py and bucket_cleanup.py are the two custom-resource Lambdas.
#    The CloudFormation template embeds inline copies; here the files
#    themselves are packaged, so there is a single copy to maintain.
#  * customer_events_aggregator.py and data/ are uploaded to the demo bucket
#    during deployment (the CloudFormation walkthrough does this with aws s3 cp).
BLOG_DIR = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = BLOG_DIR / "scripts"
DATA_DIR = BLOG_DIR / "data"

PURPOSE = "SparkTroubleshootingDevOpsAgent"
PURPOSE_DEMO = "SparkTroubleshootingDevOpsAgentDemo"


class SparkTroubleshootingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("Purpose", PURPOSE)

        # ------------------------------------------------------------------
        # Parameters and region guardrail
        #
        # The SMUS service endpoint name and the SageMaker Unified Studio MCP
        # endpoint hostname are region-specific, so deployment is restricted
        # to us-east-1. The stack is environment-agnostic and enforces this
        # with a CloudFormation Rule, exactly like the original template.
        # ------------------------------------------------------------------
        role_name_param = CfnParameter(
            self,
            "TroubleshootingRoleNameParameter",
            type="String",
            default="SparkTroubleshootingRole",
            description=(
                "Name of the IAM role created for Spark troubleshooting. AWS DevOps "
                "Agent assumes this role when calling the Apache Spark Troubleshooting "
                "Agent for Amazon EMR (via the SageMaker Unified Studio MCP server)."
            ),
            allowed_pattern=r"^[a-zA-Z0-9+=,.@_-]+$",
            constraint_description="Must be a valid IAM role name",
        )
        # The template has a parameter and an output that share this name.
        # CDK construct IDs must be unique, so only the logical ID is pinned.
        role_name_param.override_logical_id("TroubleshootingRoleName")

        vpc_cidr_param = CfnParameter(
            self,
            "VpcCidr",
            type="String",
            default="10.20.0.0/16",
            description=(
                "CIDR block for the dedicated VPC created by this stack. Must not "
                "overlap with any existing VPC in the account that you intend to peer with."
            ),
            allowed_pattern=r"^(\d{1,3}\.){3}\d{1,3}/\d{1,2}$",
            constraint_description="Must be a valid IPv4 CIDR (e.g., 10.20.0.0/16)",
        )

        CfnRule(
            self,
            "RegionMustBeUsEast1",
            assertions=[
                CfnRuleAssertion(
                    assert_=Fn.condition_equals(Aws.REGION, "us-east-1"),
                    assert_description=(
                        "This stack is currently published for us-east-1 only. To deploy "
                        "in other regions, fork it and update the SMUS service endpoint "
                        "hostname."
                    ),
                )
            ],
        )

        # ------------------------------------------------------------------
        # Networking
        #
        # Dedicated VPC with two isolated subnets in SMUS-supported AZs. No
        # internet gateway, NAT gateway or custom routes are needed: the MCP
        # service is reached through an Interface VPC Endpoint, and the AWS
        # DevOps Agent private connection (created in the console) is a managed
        # VPC Lattice resource gateway that injects its own routes.
        #
        # L1 constructs are used here on purpose. The subnet AZs are only known
        # at deploy time (see az_discovery below), which the ec2.Vpc L2
        # construct cannot consume.
        # ------------------------------------------------------------------
        vpc = ec2.CfnVPC(
            self,
            "DemoVPC",
            cidr_block=vpc_cidr_param.value_as_string,
            enable_dns_support=True,
            enable_dns_hostnames=True,
            tags=[CfnTag(key="Name", value="spark-troubleshooting-devops-agent-vpc")],
        )

        # AWS randomizes the physical-AZ-to-logical-AZ mapping per account, so
        # the AZs that support the SMUS endpoint differ between accounts. This
        # custom resource asks ec2:DescribeVpcEndpointServices at deploy time.
        az_log_group = logs.LogGroup(
            self,
            "SMUSAzDiscoveryLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        az_function = lambda_.Function(
            self,
            "SMUSAzDiscoveryFunction",
            description=(
                "Returns two Availability Zones that support the SageMaker Unified "
                "Studio MCP endpoint service in this account."
            ),
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="az_discovery.lambda_handler",
            code=lambda_.Code.from_asset(
                str(SCRIPTS_DIR), exclude=["*", "!az_discovery.py"]
            ),
            timeout=Duration.seconds(30),
            # Fires once per stack create/update/delete; a cap of 1 is enough.
            reserved_concurrent_executions=1,
            log_group=az_log_group,
        )
        az_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ec2:DescribeVpcEndpointServices"],
                resources=["*"],
            )
        )
        az_discovery = CustomResource(
            self, "SMUSAzDiscovery", service_token=az_function.function_arn
        )

        cidrs = Fn.cidr(vpc_cidr_param.value_as_string, 4, "8")
        subnet_1 = ec2.CfnSubnet(
            self,
            "DemoSubnet1",
            vpc_id=vpc.ref,
            availability_zone=az_discovery.get_att_string("SubnetAZ1"),
            cidr_block=Fn.select(0, cidrs),
            tags=[CfnTag(key="Name", value="spark-troubleshooting-subnet-1")],
        )
        subnet_2 = ec2.CfnSubnet(
            self,
            "DemoSubnet2",
            vpc_id=vpc.ref,
            availability_zone=az_discovery.get_att_string("SubnetAZ2"),
            cidr_block=Fn.select(1, cidrs),
            tags=[CfnTag(key="Name", value="spark-troubleshooting-subnet-2")],
        )

        # ------------------------------------------------------------------
        # PrivateLink
        #
        # Private DNS is enabled so sagemaker-unified-studio-mcp.us-east-1.api.aws
        # resolves to the endpoint's private ENIs from inside the VPC.
        # ------------------------------------------------------------------
        endpoint_sg = ec2.CfnSecurityGroup(
            self,
            "SMUSVPCESecurityGroup",
            group_description=(
                "Inbound 443 from the VPC for the SageMaker Unified Studio MCP "
                "Interface VPC Endpoint."
            ),
            vpc_id=vpc.ref,
            security_group_ingress=[
                ec2.CfnSecurityGroup.IngressProperty(
                    ip_protocol="tcp",
                    from_port=443,
                    to_port=443,
                    cidr_ip=vpc.attr_cidr_block,
                    description="HTTPS from VPC CIDR (DevOps Agent resource gateway lives here)",
                )
            ],
        )
        Tags.of(endpoint_sg).add("Purpose", "SMUSPrivateLink", priority=200)

        vpc_endpoint = ec2.CfnVPCEndpoint(
            self,
            "SMUSVPCEndpoint",
            vpc_id=vpc.ref,
            service_name=f"com.amazonaws.{Aws.REGION}.sagemaker-unified-studio-mcp",
            vpc_endpoint_type="Interface",
            subnet_ids=[subnet_1.ref, subnet_2.ref],
            security_group_ids=[endpoint_sg.ref],
            private_dns_enabled=True,
        )

        # ------------------------------------------------------------------
        # Demo bucket
        #
        # Holds the PySpark script (root), Parquet data (data/) and Spark logs
        # (logs/).
        #
        # The name embeds the account ID and region, which makes it unique per
        # account and rules out bucket squatting. (The amzn-s3-demo- prefix
        # suggested by scanners is reserved by AWS and cannot be created from
        # customer accounts.)
        # ------------------------------------------------------------------
        demo_bucket = s3.Bucket(
            self,
            "DemoBucket",
            bucket_name=f"spark-troubleshooting-{Aws.ACCOUNT_ID}-{Aws.REGION}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(demo_bucket).add("Purpose", PURPOSE_DEMO, priority=200)

        # ------------------------------------------------------------------
        # Bucket cleanup (custom resource)
        #
        # CloudFormation refuses to delete a non-empty bucket, and this one
        # accumulates Spark logs. On stack delete, scripts/bucket_cleanup.py
        # empties it (all object versions and delete markers) so that
        # `cdk destroy` needs no manual `aws s3 rm --recursive`.
        #
        # The explicit dependency on the bucket makes this resource's Delete
        # event fire before the bucket is deleted.
        # ------------------------------------------------------------------
        cleanup_log_group = logs.LogGroup(
            self,
            "BucketCleanupLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        cleanup_function = lambda_.Function(
            self,
            "BucketCleanupFunction",
            description="Empties the demo S3 bucket on stack delete so CloudFormation can then delete the bucket itself.",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="bucket_cleanup.lambda_handler",
            code=lambda_.Code.from_asset(
                str(SCRIPTS_DIR), exclude=["*", "!bucket_cleanup.py"]
            ),
            timeout=Duration.seconds(300),
            # Fires once per stack create/update/delete; a cap of 1 is enough.
            reserved_concurrent_executions=1,
            log_group=cleanup_log_group,
        )
        cleanup_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:ListBucketVersions"],
                resources=[demo_bucket.bucket_arn],
            )
        )
        cleanup_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:DeleteObject", "s3:DeleteObjectVersion"],
                resources=[demo_bucket.arn_for_objects("*")],
            )
        )
        bucket_cleanup = CustomResource(
            self,
            "BucketCleanup",
            service_token=cleanup_function.function_arn,
            # Read by the Lambda from event.ResourceProperties.BucketName.
            properties={"BucketName": demo_bucket.bucket_name},
        )
        bucket_cleanup.node.add_dependency(demo_bucket)
        # cfn-lint W3005 flags the DependsOn as redundant because BucketName
        # already references the bucket. The reference only orders creation;
        # the explicit DependsOn is what guarantees this resource is deleted
        # (and the bucket emptied) before the bucket itself. Deliberate.
        assert isinstance(bucket_cleanup.node.default_child, CfnResource)
        bucket_cleanup.node.default_child.add_metadata(
            "cfn-lint", {"config": {"ignore_checks": ["W3005"]}}
        )

        # ------------------------------------------------------------------
        # Demo script and data
        #
        # Uploads the PySpark script and the Parquet dataset from this blog
        # folder into the bucket, replacing the manual `aws s3 cp` steps.
        #
        # prune=False is required: the script lands at the bucket root, and
        # pruning would delete the logs/ prefix that EMR Serverless writes to.
        # ------------------------------------------------------------------
        deployment_memory_mb = 512  # the Parquet files are ~10 MB each
        s3deploy.BucketDeployment(
            self,
            "DeployDemoScript",
            sources=[
                s3deploy.Source.asset(
                    str(SCRIPTS_DIR),
                    exclude=["*", "!customer_events_aggregator.py"],
                )
            ],
            destination_bucket=demo_bucket,
            prune=False,
            memory_limit=deployment_memory_mb,
        )
        s3deploy.BucketDeployment(
            self,
            "DeployDemoData",
            sources=[s3deploy.Source.asset(str(DATA_DIR))],
            destination_bucket=demo_bucket,
            destination_key_prefix="data/",
            prune=False,
            memory_limit=deployment_memory_mb,
        )

        # ------------------------------------------------------------------
        # AWS DevOps Agent troubleshooting role
        #
        # Trust: the AWS DevOps Agent service principal with SourceAccount and
        # SourceArn confused-deputy guards, as recommended by the capability
        # provider wizard.
        # ------------------------------------------------------------------
        troubleshooting_role = iam.Role(
            self,
            "SparkTroubleshootingRole",
            role_name=role_name_param.value_as_string,
            description=(
                "Role assumed by AWS DevOps Agent to invoke the Apache Spark "
                "Troubleshooting Agent for Amazon EMR (via the SageMaker Unified "
                "Studio MCP server)."
            ),
            assumed_by=iam.PrincipalWithConditions(
                iam.ServicePrincipal("aidevops.amazonaws.com"),
                {
                    "StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID},
                    "ArnLike": {
                        "aws:SourceArn": f"arn:{Aws.PARTITION}:aidevops:{Aws.REGION}:{Aws.ACCOUNT_ID}:service/*"
                    },
                },
            ),
        )
        assert troubleshooting_role.assume_role_policy is not None
        troubleshooting_role.assume_role_policy.add_statements(
            iam.PolicyStatement(
                principals=[iam.AccountRootPrincipal()],
                actions=["sts:AssumeRole"],
            )
        )
        Tags.of(troubleshooting_role).add("ManagedBy", "CDK")

        # SMUS MCP invoke permissions.
        mcp_invoke_access = iam.Policy(
            self,
            "MCPInvokeAccess",
            policy_name="MCPInvokeAccess",
            roles=[troubleshooting_role],
            statements=[
                iam.PolicyStatement(
                    sid="InvokeSMUSMcp",
                    actions=[
                        "sagemaker-unified-studio-mcp:InvokeMcp",
                        "sagemaker-unified-studio-mcp:CallReadOnlyTool",
                        "sagemaker-unified-studio-mcp:CallPrivilegedTool",
                    ],
                    resources=["*"],
                )
            ],
        )
        # cfn-lint W3037 does not know the newly released
        # sagemaker-unified-studio-mcp IAM prefix yet. The actions are valid;
        # AWS DevOps Agent uses exactly these to invoke the MCP server.
        assert isinstance(mcp_invoke_access.node.default_child, CfnResource)
        mcp_invoke_access.node.default_child.add_metadata(
            "cfn-lint", {"config": {"ignore_checks": ["W3037"]}}
        )

        # EMR-on-EC2 read permissions. Cluster-scoped APIs are limited to the
        # current account and region; ListClusters and DescribeJobFlows do not
        # support resource-level permissions and require "*". S3 access to
        # cluster logs is intentionally omitted because the demo runs on EMR
        # Serverless. Add a statement scoped to your cluster-logs bucket if
        # you extend this role for EMR-on-EC2.
        emr_read_access = iam.Policy(
            self,
            "EMRReadAccess",
            policy_name="EMRReadAccess",
            roles=[troubleshooting_role],
            statements=[
                iam.PolicyStatement(
                    sid="EMREC2PerClusterRead",
                    actions=[
                        "elasticmapreduce:DescribeCluster",
                        "elasticmapreduce:DescribeStep",
                        "elasticmapreduce:ListSteps",
                        "elasticmapreduce:CreatePersistentAppUI",
                        "elasticmapreduce:DescribePersistentAppUI",
                        "elasticmapreduce:GetPersistentAppUIPresignedURL",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:elasticmapreduce:{Aws.REGION}:{Aws.ACCOUNT_ID}:cluster/*"
                    ],
                ),
                iam.PolicyStatement(
                    sid="EMREC2AccountWideList",
                    actions=[
                        "elasticmapreduce:ListClusters",
                        "elasticmapreduce:DescribeJobFlows",
                    ],
                    resources=["*"],
                ),
            ],
        )

        emr_serverless_read_access = iam.Policy(
            self,
            "EMRServerlessReadAccess",
            policy_name="EMRServerlessReadAccess",
            roles=[troubleshooting_role],
            statements=[
                iam.PolicyStatement(
                    sid="EMRServerlessDescribeAndList",
                    actions=[
                        "emr-serverless:GetJobRun",
                        "emr-serverless:GetApplication",
                        "emr-serverless:ListApplications",
                        "emr-serverless:ListJobRuns",
                        "emr-serverless:ListJobRunAttempts",
                        "emr-serverless:GetDashboardForJobRun",
                        "emr-serverless:ListTagsForResource",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    sid="EMRServerlessCloudWatchLogs",
                    actions=["logs:GetLogEvents", "logs:FilterLogEvents"],
                    resources=[
                        f"arn:{Aws.PARTITION}:logs:*:{Aws.ACCOUNT_ID}:log-group:/aws/emr-serverless/*"
                    ],
                ),
            ],
        )

        glue_read_access = iam.Policy(
            self,
            "GlueReadAccess",
            policy_name="GlueReadAccess",
            roles=[troubleshooting_role],
            statements=[
                iam.PolicyStatement(
                    sid="GlueJobReadAccess",
                    actions=[
                        "glue:GetJob",
                        "glue:GetJobRun",
                        "glue:GetJobRuns",
                        "glue:GetJobs",
                        "glue:BatchGetJobs",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    sid="GlueCloudWatchLogs",
                    actions=["logs:GetLogEvents", "logs:FilterLogEvents"],
                    resources=[
                        f"arn:{Aws.PARTITION}:logs:*:{Aws.ACCOUNT_ID}:log-group:/aws-glue/*"
                    ],
                ),
                iam.PolicyStatement(
                    sid="GlueSparkWebUI",
                    actions=[
                        "glue:RequestLogParsing",
                        "glue:GetLogParsingStatus",
                        "glue:GetEnvironment",
                        "glue:GetStage",
                        "glue:GetStages",
                        "glue:GetStageFiles",
                        "glue:BatchGetStageFiles",
                        "glue:GetStageAttempt",
                        "glue:GetStageAttemptTaskList",
                        "glue:GetStageAttemptTaskSummary",
                        "glue:GetExecutors",
                        "glue:GetExecutorsThreads",
                        "glue:GetStorage",
                        "glue:GetStorageUnit",
                        "glue:GetQueries",
                        "glue:GetQuery",
                        "glue:GetDashboardUrl",
                    ],
                    resources=["*"],
                ),
            ],
        )

        # The agent reads the PySpark source and Spark event logs from the demo
        # bucket to produce line-numbered root cause analysis.
        source_code_access = iam.Policy(
            self,
            "SourceCodeAndEventLogsAccess",
            policy_name="SourceCodeAndEventLogsAccess",
            roles=[troubleshooting_role],
            statements=[
                iam.PolicyStatement(
                    sid="DemoBucketRead",
                    actions=["s3:GetObject", "s3:ListBucket"],
                    resources=[
                        demo_bucket.bucket_arn,
                        demo_bucket.arn_for_objects("*"),
                    ],
                )
            ],
        )

        # ------------------------------------------------------------------
        # Demo workload
        # ------------------------------------------------------------------
        execution_role = iam.Role(
            self,
            "DemoExecutionRole",
            role_name="EMRServerlessExecutionRole-AnalyticsEvents",
            description=(
                "Execution role assumed by the analytics-events-platform EMR "
                "Serverless application's Spark drivers and executors."
            ),
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com"),
            inline_policies={
                "AnalyticsEventsExecutionAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            sid="DemoBucketRead",
                            actions=["s3:GetObject", "s3:ListBucket"],
                            resources=[
                                demo_bucket.bucket_arn,
                                demo_bucket.arn_for_objects("*"),
                            ],
                        ),
                        iam.PolicyStatement(
                            sid="DemoBucketLogsWrite",
                            actions=["s3:PutObject", "s3:DeleteObject"],
                            resources=[demo_bucket.arn_for_objects("logs/*")],
                        ),
                        iam.PolicyStatement(
                            sid="EMRServerlessCloudWatchLogs",
                            actions=[
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                                "logs:DescribeLogStreams",
                            ],
                            resources=[
                                f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/emr-serverless/*"
                            ],
                        ),
                    ]
                )
            },
        )

        demo_application = emrserverless.CfnApplication(
            self,
            "DemoApplication",
            name="analytics-events-platform",
            release_label="emr-7.5.0",
            type="SPARK",
            maximum_capacity=emrserverless.CfnApplication.MaximumAllowedResourcesProperty(
                cpu="16 vCPU", memory="64 GB", disk="42 GB"
            ),
            auto_stop_configuration=emrserverless.CfnApplication.AutoStopConfigurationProperty(
                enabled=True, idle_timeout_minutes=15
            ),
            monitoring_configuration=emrserverless.CfnApplication.MonitoringConfigurationProperty(
                managed_persistence_monitoring_configuration=emrserverless.CfnApplication.ManagedPersistenceMonitoringConfigurationProperty(
                    enabled=True
                ),
                s3_monitoring_configuration=emrserverless.CfnApplication.S3MonitoringConfigurationProperty(
                    log_uri=f"s3://{demo_bucket.bucket_name}/logs/"
                ),
            ),
        )
        Tags.of(demo_application).add("Purpose", PURPOSE_DEMO, priority=200)

        # ------------------------------------------------------------------
        # CloudWatch alarm
        #
        # Fires when the EMR Serverless application has any failed job in a
        # 5-minute window. It has no actions on purpose: the blog reader
        # references it by name in the AWS DevOps Agent chat to start an
        # investigation.
        #
        # EMR Serverless publishes its metrics in delayed batches, 1-3+
        # minutes after the fact. An alarm that evaluates only the latest
        # 60-second period can finish judging that minute before the late
        # FailedJobs datapoint arrives, count the gap as not breaching, and
        # never revisit it. Looking back over 5 periods (1 breaching period
        # is enough) catches the late datapoint.
        # ------------------------------------------------------------------
        failed_jobs_alarm = cloudwatch.Alarm(
            self,
            "FailedJobsAlarm",
            alarm_name=f"{demo_application.attr_application_id}-FailedJobs",
            alarm_description=(
                "Fires when the analytics-events-platform EMR Serverless application "
                "has at least one failed job within a 5-minute window. Used by the AWS "
                "DevOps Agent + Apache Spark Troubleshooting Agent for Amazon EMR "
                "integration blog post to trigger an investigation from the agent chat."
            ),
            metric=cloudwatch.Metric(
                namespace="AWS/EMRServerless",
                metric_name="FailedJobs",
                dimensions_map={"ApplicationId": demo_application.attr_application_id},
                statistic="Sum",
                period=Duration.seconds(60),
            ),
            evaluation_periods=5,
            datapoints_to_alarm=1,
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        Tags.of(failed_jobs_alarm).add("Purpose", PURPOSE_DEMO, priority=200)

        # ------------------------------------------------------------------
        # AWS DevOps Agent agent-space role
        #
        # The agent's own AWS API calls during an investigation run as this
        # role, not as SparkTroubleshootingRole (which only signs the MCP
        # calls). The role the console auto-creates uses the AWS-managed
        # AIDevOpsAgentAccessPolicy, which cannot read the demo bucket's logs
        # or EMR Serverless job runs, and its name gets a random suffix, so a
        # stack cannot extend it afterwards.
        #
        # Creating the role here with a fixed name lets the reader choose it
        # with "Assign an existing role" when creating the agent space, so no
        # second deployment is needed.
        # ------------------------------------------------------------------
        agent_space_role = iam.Role(
            self,
            "AgentSpaceRole",
            role_name="DevOpsAgentRole-AgentSpace-SparkTroubleshooting",
            description=(
                "Role for the AWS DevOps Agent agent space. Same trust and managed "
                "policy as the console's auto-created role, plus read access to the "
                "demo bucket and EMR Serverless job runs."
            ),
            assumed_by=iam.PrincipalWithConditions(
                iam.ServicePrincipal("aidevops.amazonaws.com"),
                {
                    "StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID},
                    "ArnLike": {
                        "aws:SourceArn": f"arn:{Aws.PARTITION}:aidevops:{Aws.REGION}:{Aws.ACCOUNT_ID}:agentspace/*"
                    },
                },
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AIDevOpsAgentAccessPolicy")
            ],
        )
        agent_space_read_access = iam.Policy(
            self,
            "AgentSpaceReadAccess",
            policy_name="SparkDemoAgentReadAccess",
            roles=[agent_space_role],
            statements=[
                iam.PolicyStatement(
                    sid="ReadDemoBucketLogs",
                    actions=["s3:ListBucket"],
                    resources=[demo_bucket.bucket_arn],
                ),
                iam.PolicyStatement(
                    sid="ReadDemoBucketObjects",
                    actions=["s3:GetObject"],
                    resources=[demo_bucket.arn_for_objects("*")],
                ),
                iam.PolicyStatement(
                    sid="ReadDemoJobRuns",
                    actions=["emr-serverless:GetJobRun"],
                    resources=[
                        f"arn:{Aws.PARTITION}:emr-serverless:{Aws.REGION}:{Aws.ACCOUNT_ID}:/applications/{demo_application.attr_application_id}/jobruns/*"
                    ],
                ),
                # Mirrors the console role's AIDevOpsCreateServiceLinkedRolePolicy.
                iam.PolicyStatement(
                    sid="CreateResourceExplorerServiceLinkedRole",
                    actions=["iam:CreateServiceLinkedRole"],
                    resources=[
                        f"arn:{Aws.PARTITION}:iam::{Aws.ACCOUNT_ID}:role/aws-service-role/resource-explorer-2.amazonaws.com/AWSServiceRoleForResourceExplorer"
                    ],
                ),
            ],
        )

        # ------------------------------------------------------------------
        # Outputs (same keys as the CloudFormation template)
        # ------------------------------------------------------------------
        CfnOutput(
            self,
            "TroubleshootingRoleArn",
            description="ARN of the IAM role to assign in the AWS DevOps Agent capability provider wizard (Authorization Configuration step).",
            value=troubleshooting_role.role_arn,
            export_name=f"{self.stack_name}-TroubleshootingRoleArn",
        )
        CfnOutput(
            self,
            "TroubleshootingRoleName",
            description="Name of the troubleshooting role",
            value=troubleshooting_role.role_name,
            export_name=f"{self.stack_name}-TroubleshootingRoleName",
        )
        CfnOutput(
            self,
            "MCPEndpointURL",
            description="SMUS Spark troubleshooting MCP endpoint to register in DevOps Agent capability provider (MCP server details step).",
            value=f"https://sagemaker-unified-studio-mcp.{Aws.REGION}.api.aws/spark-troubleshooting/mcp",
        )
        CfnOutput(
            self,
            "MCPServiceName",
            description="AWS service name to enter in the DevOps Agent SigV4 authorization step.",
            value="sagemaker-unified-studio-mcp",
        )
        CfnOutput(
            self,
            "DemoVpcId",
            description="ID of the dedicated VPC. Use this when configuring the AWS DevOps Agent private connection in the console.",
            value=vpc.ref,
            export_name=f"{self.stack_name}-DemoVpcId",
        )
        CfnOutput(
            self,
            "DemoSubnetIds",
            description="Subnet IDs to use when configuring the AWS DevOps Agent private connection.",
            value=f"{subnet_1.ref},{subnet_2.ref}",
        )
        CfnOutput(
            self,
            "SMUSVpcEndpointId",
            description="ID of the SageMaker Unified Studio MCP Interface VPC Endpoint.",
            value=vpc_endpoint.ref,
            export_name=f"{self.stack_name}-SMUSVpcEndpointId",
        )
        CfnOutput(
            self,
            "SMUSVpcEndpointSecurityGroupId",
            description="Security group attached to the SMUS VPC endpoint. Use the same security group when configuring the AWS DevOps Agent private connection.",
            value=endpoint_sg.ref,
        )
        CfnOutput(
            self,
            "DemoApplicationId",
            description="EMR Serverless application ID",
            value=demo_application.attr_application_id,
        )
        # The bucket resource and its output share the logical ID "DemoBucket"
        # in the template, so the output's logical ID is pinned explicitly.
        bucket_output = CfnOutput(
            self,
            "DemoBucketName",
            description="S3 bucket for demo artifacts (PySpark script under root, Parquet data under data/, Spark logs under logs/). The CDK app uploads the script and data from this blog folder during deployment.",
            value=demo_bucket.bucket_name,
        )
        bucket_output.override_logical_id("DemoBucket")
        CfnOutput(
            self,
            "DemoExecutionRoleArn",
            description="EMR Serverless job execution role ARN",
            value=execution_role.role_arn,
        )
        CfnOutput(
            self,
            "FailedJobsAlarmName",
            description="Name of the CloudWatch alarm that fires when the demo job fails. Reference this in the AWS DevOps Agent chat (e.g., 'Investigate alarm <name>').",
            value=failed_jobs_alarm.alarm_name,
        )
        CfnOutput(
            self,
            "FailedJobsAlarmArn",
            description="ARN of the FailedJobs CloudWatch alarm",
            value=failed_jobs_alarm.alarm_arn,
        )
        # Not in the CloudFormation template. Select this role with "Assign an
        # existing role" when creating the agent space.
        CfnOutput(
            self,
            "AgentSpaceRoleName",
            description="Name of the IAM role to choose with 'Assign an existing role' when creating the AWS DevOps Agent agent space. It can read the demo bucket logs and EMR Serverless job runs, which the auto-created role cannot.",
            value=agent_space_role.role_name,
        )
        CfnOutput(
            self,
            "AgentSpaceRoleArn",
            description="ARN of the agent space role.",
            value=agent_space_role.role_arn,
        )
        job_driver = (
            '{"sparkSubmit":{"entryPoint":"s3://%s/customer_events_aggregator.py",'
            '"entryPointArguments":["%s"],"sparkSubmitParameters":"--conf '
            "spark.executor.cores=2 --conf spark.executor.memory=1g --conf "
            "spark.executor.pyspark.memory=256m --conf spark.executor.instances=2\"}}"
        ) % (demo_bucket.bucket_name, demo_bucket.bucket_name)
        CfnOutput(
            self,
            "DemoSubmitJobCommand",
            description="Sample command to submit the failing job. Replace <DemoBucket> with the DemoBucket output value if copy-pasting manually.",
            value=" ".join(
                [
                    "aws emr-serverless start-job-run",
                    f"--region {Aws.REGION}",
                    f"--application-id {demo_application.attr_application_id}",
                    f"--execution-role-arn {execution_role.role_arn}",
                    "--name daily-customer-events-rollup",
                    f"--job-driver '{job_driver}'",
                    '--configuration-overrides \'{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"s3://%s/logs/"}}}\''
                    % demo_bucket.bucket_name,
                ]
            ),
        )

        self._add_nag_suppressions(
            az_function=az_function,
            cleanup_function=cleanup_function,
            vpc=vpc,
            endpoint_sg=endpoint_sg,
            demo_bucket=demo_bucket,
            execution_role=execution_role,
            agent_space_role=agent_space_role,
            policies={
                "MCPInvokeAccess": mcp_invoke_access,
                "EMRReadAccess": emr_read_access,
                "EMRServerlessReadAccess": emr_serverless_read_access,
                "GlueReadAccess": glue_read_access,
                "SourceCodeAndEventLogsAccess": source_code_access,
                "AgentSpaceReadAccess": agent_space_read_access,
            },
        )

    def _add_nag_suppressions(
        self,
        *,
        az_function,
        cleanup_function,
        vpc,
        endpoint_sg,
        demo_bucket,
        execution_role,
        agent_space_role,
        policies,
    ) -> None:
        """cdk-nag (AwsSolutions) suppressions, each with its justification."""
        lambda_basic_execution = (
            "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )
        for function, wildcard_reason in (
            (
                az_function,
                "ec2:DescribeVpcEndpointServices does not support resource-level permissions.",
            ),
            (
                cleanup_function,
                "Must delete every object and version in the demo bucket (bucket/*) on stack delete.",
            ),
        ):
            NagSuppressions.add_resource_suppressions(
                function,
                [
                    NagPackSuppression(
                        id="AwsSolutions-IAM4",
                        reason="AWSLambdaBasicExecutionRole is the standard managed policy for Lambda log delivery; same choice as the CloudFormation template.",
                        applies_to=[lambda_basic_execution],
                    ),
                    NagPackSuppression(
                        id="AwsSolutions-IAM5",
                        reason=wildcard_reason,
                    ),
                ],
                apply_to_children=True,
            )
        NagSuppressions.add_resource_suppressions(
            endpoint_sg,
            [
                NagPackSuppression(
                    id="CdkNagValidationFailure",
                    reason="AwsSolutions-EC23 could not evaluate the ingress CIDR because it is the VPC's CIDR resolved at deploy time. It is the VPC CIDR only (port 443), never 0.0.0.0/0.",
                )
            ],
        )
        NagSuppressions.add_resource_suppressions(
            vpc,
            [
                NagPackSuppression(
                    id="AwsSolutions-VPC7",
                    reason="Isolated demo VPC with no internet or NAT route; its only traffic is HTTPS to the MCP interface endpoint. Flow logs are out of scope for a short-lived demo.",
                )
            ],
        )
        NagSuppressions.add_resource_suppressions(
            demo_bucket,
            [
                NagPackSuppression(
                    id="AwsSolutions-S1",
                    reason="Server access logging needs a second bucket to receive logs, which is out of scope for this short-lived demo bucket.",
                )
            ],
        )
        NagSuppressions.add_resource_suppressions(
            execution_role,
            [
                NagPackSuppression(
                    id="AwsSolutions-IAM5",
                    reason="Scoped to objects under the demo bucket (logs/ for writes) and to EMR Serverless log groups; the log group and object names are generated at run time.",
                )
            ],
            apply_to_children=True,
        )
        wildcard_reasons = {
            "MCPInvokeAccess": "The sagemaker-unified-studio-mcp actions are invoked against the service endpoint, which has no resource ARN to scope to.",
            "EMRReadAccess": "Cluster-scoped read APIs are limited to this account and region; ListClusters and DescribeJobFlows do not support resource-level permissions.",
            "EMRServerlessReadAccess": "Read-only describe/list APIs across the account's EMR Serverless applications; log access is limited to /aws/emr-serverless/ log groups.",
            "GlueReadAccess": "Read-only Glue job and Spark UI APIs, which do not support resource-level permissions; log access is limited to /aws-glue/ log groups.",
            "SourceCodeAndEventLogsAccess": "Read-only access to every object in the demo bucket, so the agent can read the PySpark source and Spark event logs.",
            "AgentSpaceReadAccess": "Read-only: GetObject on the demo bucket's objects (log file names are generated by EMR Serverless) and GetJobRun on this application's job runs (job run IDs are generated at run time).",
        }
        for name, policy in policies.items():
            NagSuppressions.add_resource_suppressions(
                policy,
                [NagPackSuppression(id="AwsSolutions-IAM5", reason=wildcard_reasons[name])],
            )

        NagSuppressions.add_resource_suppressions(
            agent_space_role,
            [
                NagPackSuppression(
                    id="AwsSolutions-IAM4",
                    reason="AIDevOpsAgentAccessPolicy is the AWS-managed policy that the AWS DevOps Agent console attaches to the agent space role it creates.",
                    applies_to=[
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/AIDevOpsAgentAccessPolicy"
                    ],
                )
            ],
        )

        # The singleton Lambda that CDK provisions to back BucketDeployment.
        # It is owned and managed by CDK, not by this stack's code. Its
        # construct ID embeds a hash and the memory size, so it is found by
        # prefix rather than by exact ID.
        provider = next(
            child
            for child in self.node.children
            if child.node.id.startswith("Custom::CDKBucketDeployment")
        )
        NagSuppressions.add_resource_suppressions(
            provider,
            [
                NagPackSuppression(
                    id="AwsSolutions-IAM4",
                    reason="Role is owned and managed by the CDK BucketDeployment provider.",
                ),
                NagPackSuppression(
                    id="AwsSolutions-IAM5",
                    reason="The CDK BucketDeployment provider needs read/write on the destination bucket's objects and read on the CDK asset bucket.",
                ),
                NagPackSuppression(
                    id="AwsSolutions-L1",
                    reason="Function is owned and managed by the CDK BucketDeployment provider.",
                ),
            ],
            apply_to_children=True,
        )
