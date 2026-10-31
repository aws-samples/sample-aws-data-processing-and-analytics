"""Infrastructure for AWS Data Reliability Copilot."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from aws_cdk import (
    Aws,
    CfnOutput,
    CfnParameter,
    CfnTag,
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
)
from aws_cdk import (
    aws_athena as athena,
)
from aws_cdk import (
    aws_events as events,
)
from aws_cdk import (
    aws_events_targets as event_targets,
)
from aws_cdk import (
    aws_glue as glue,
)
from aws_cdk import (
    aws_iam as iam,
)
from aws_cdk import (
    aws_lambda as lambda_,
)
from aws_cdk import (
    aws_logs as logs,
)
from aws_cdk import (
    aws_s3 as s3,
)
from aws_cdk import (
    aws_s3_assets as s3_assets,
)
from aws_cdk import (
    aws_s3tables as s3tables,
)
from aws_cdk import (
    aws_stepfunctions as sfn,
)
from aws_cdk import (
    aws_stepfunctions_tasks as tasks,
)
from cdk_nag import NagSuppressions
from constructs import Construct

PROJECT_DIR = Path(__file__).resolve().parents[3]
GLUE_SCRIPT = PROJECT_DIR / "glue" / "orders_etl.py"
LAMBDA_HANDLER_PREFIX = "agentic_batch_data_reliability.orchestration.handlers"


class DataReliabilityStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, stage: str, **kwargs: Any) -> None:
        super().__init__(scope, construct_id, **kwargs)
        prefix = f"abdr-{stage}"
        raw_database_name = f"abdr_{stage}_raw"
        table_bucket_name = f"{prefix}-curated-{Aws.ACCOUNT_ID}"
        namespace_name = "order_processing"

        model_id = CfnParameter(
            self,
            "BedrockModelId",
            type="String",
            description="Bedrock model or inference-profile ID used by the Strands agent.",
            allowed_pattern=r"^[A-Za-z0-9._:-]+$",
        )
        model_arn = CfnParameter(
            self,
            "BedrockModelArn",
            type="String",
            description="Exact Bedrock model or inference-profile ARN allowed for invocation.",
            allowed_pattern=r"^arn:[a-z0-9-]+:bedrock:[a-z0-9-]*:[0-9]*:.+$",
        )

        Tags.of(self).add("Project", "aws-data-reliability-copilot")
        Tags.of(self).add("Environment", stage)
        Tags.of(self).add("ManagedBy", "aws-cdk")

        access_log_bucket = s3.Bucket(
            self,
            "AccessLogBucket",
            bucket_name=f"{prefix}-access-logs-{Aws.ACCOUNT_ID}-{Aws.REGION}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(30))],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        raw_bucket = self._data_bucket(
            "RawDataBucket",
            f"{prefix}-raw-{Aws.ACCOUNT_ID}-{Aws.REGION}",
            access_log_bucket,
        )
        report_bucket = self._data_bucket(
            "ReportBucket",
            f"{prefix}-artifacts-{Aws.ACCOUNT_ID}-{Aws.REGION}",
            access_log_bucket,
        )

        table_bucket = s3tables.CfnTableBucket(
            self,
            "CuratedTableBucket",
            table_bucket_name=table_bucket_name,
            encryption_configuration=s3tables.CfnTableBucket.EncryptionConfigurationProperty(
                sse_algorithm="AES256"
            ),
            unreferenced_file_removal=s3tables.CfnTableBucket.UnreferencedFileRemovalProperty(
                status="Enabled", unreferenced_days=3, noncurrent_days=3
            ),
            tags=[
                CfnTag(key="Project", value="aws-data-reliability-copilot"),
                CfnTag(key="Environment", value=stage),
            ],
        )
        table_bucket.apply_removal_policy(RemovalPolicy.DESTROY)
        namespace = s3tables.CfnNamespace(
            self,
            "OrderProcessingNamespace",
            namespace=namespace_name,
            table_bucket_arn=table_bucket.attr_table_bucket_arn,
        )
        namespace.add_resource_dependency(table_bucket)

        fields = [
            (1, "order_id", "string", True),
            (2, "customer_id", "string", True),
            (3, "order_timestamp", "timestamp", True),
            (4, "order_amount", "decimal(12,2)", True),
            (5, "status", "string", True),
            (6, "batch_id", "string", True),
            (7, "ingestion_timestamp", "timestamp", True),
            (8, "source_file", "string", True),
            (9, "processing_timestamp", "timestamp", True),
            (10, "pipeline_run_id", "string", True),
        ]
        orders_table = s3tables.CfnTable(
            self,
            "CuratedOrdersTable",
            table_bucket_arn=table_bucket.attr_table_bucket_arn,
            namespace=namespace_name,
            table_name="orders",
            open_table_format="ICEBERG",
            iceberg_metadata=s3tables.CfnTable.IcebergMetadataProperty(
                iceberg_schema=s3tables.CfnTable.IcebergSchemaProperty(
                    schema_field_list=[
                        s3tables.CfnTable.SchemaFieldProperty(
                            id=field_id, name=name, type=field_type, required=required
                        )
                        for field_id, name, field_type, required in fields
                    ]
                ),
                iceberg_partition_spec=s3tables.CfnTable.IcebergPartitionSpecProperty(
                    spec_id=0,
                    fields=[
                        s3tables.CfnTable.IcebergPartitionFieldProperty(
                            field_id=1000,
                            name="batch_id",
                            source_id=6,
                            transform="identity",
                        )
                    ],
                ),
                table_properties={
                    "format-version": "2",
                    "write.parquet.compression-codec": "zstd",
                },
            ),
            snapshot_management=s3tables.CfnTable.SnapshotManagementProperty(
                status="enabled", max_snapshot_age_hours=168, min_snapshots_to_keep=3
            ),
            tags=[CfnTag(key="Project", value="aws-data-reliability-copilot")],
        )
        orders_table.add_resource_dependency(namespace)
        orders_table.apply_removal_policy(RemovalPolicy.DESTROY)

        raw_database = glue.CfnDatabase(
            self,
            "RawDatabase",
            catalog_id=Aws.ACCOUNT_ID,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=raw_database_name,
                description="Raw JSON order batches for deterministic reconciliation.",
            ),
        )
        raw_table = glue.CfnTable(
            self,
            "RawOrdersTable",
            catalog_id=Aws.ACCOUNT_ID,
            database_name=raw_database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="orders",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "classification": "json",
                    "EXTERNAL": "TRUE",
                    "projection.enabled": "true",
                    "projection.batch_id.type": "injected",
                    "storage.location.template": (
                        f"s3://{raw_bucket.bucket_name}/raw/orders/batch_id=${{batch_id}}/"
                    ),
                },
                partition_keys=[glue.CfnTable.ColumnProperty(name="batch_id", type="string")],
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{raw_bucket.bucket_name}/raw/orders/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                        parameters={"ignore.malformed.json": "false"},
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="order_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="customer_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="order_timestamp", type="string"),
                        glue.CfnTable.ColumnProperty(name="order_amount", type="decimal(12,2)"),
                        glue.CfnTable.ColumnProperty(name="status", type="string"),
                    ],
                ),
            ),
        )
        raw_table.add_resource_dependency(raw_database)

        workgroup = athena.CfnWorkGroup(
            self,
            "ReconciliationWorkgroup",
            name=f"{prefix}-reconciliation",
            description="Batch-scoped deterministic reconciliation with a scan cutoff.",
            recursive_delete_option=True,
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=1_073_741_824,
                requester_pays_enabled=False,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{report_bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
            ),
        )

        glue_role = iam.Role(
            self,
            "OrdersEtlRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Least-privilege execution role for the orders S3 Tables ETL jobs.",
        )
        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        )
        raw_bucket.grant_read(glue_role, "raw/orders/*")
        report_bucket.grant_read_write(glue_role, "spark-events/*")
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3tables:GetTableBucket",
                    "s3tables:GetNamespace",
                    "s3tables:GetTable",
                    "s3tables:GetTableData",
                    "s3tables:PutTableData",
                    "s3tables:UpdateTableMetadataLocation",
                ],
                resources=[table_bucket.attr_table_bucket_arn, orders_table.attr_table_arn],
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetCatalog", "glue:GetDatabase", "glue:GetTable"],
                resources=[
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog/s3tablescatalog/*",
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/*",
                    f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/*/*",
                ],
            )
        )
        glue_role.add_to_policy(
            iam.PolicyStatement(actions=["lakeformation:GetDataAccess"], resources=["*"])
        )

        script_asset = s3_assets.Asset(self, "OrdersEtlScript", path=str(GLUE_SCRIPT))
        script_asset.grant_read(glue_role)
        spark_conf = " ".join(
            [
                "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "--conf spark.sql.defaultCatalog=s3tables",
                "--conf spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog",
                "--conf spark.sql.catalog.s3tables.catalog-impl="
                "org.apache.iceberg.aws.glue.GlueCatalog",
                "--conf spark.sql.catalog.s3tables.glue.id="
                f"{Aws.ACCOUNT_ID}:s3tablescatalog/{table_bucket_name}",
                f"--conf spark.sql.catalog.s3tables.warehouse=s3://{table_bucket_name}/warehouse/",
            ]
        )
        common_job_arguments = {
            "--job-language": "python",
            "--datalake-formats": "iceberg",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-metrics": "true",
            "--enable-observability-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://{report_bucket.bucket_name}/spark-events/",
            "--conf": spark_conf,
            "--raw_bucket": raw_bucket.bucket_name,
            "--source_prefix": "raw/orders",
            "--account_id": Aws.ACCOUNT_ID,
            "--table_bucket_name": table_bucket_name,
            "--namespace": namespace_name,
            "--table_name": "orders",
            "--random_seed": "42",
            "--write_mode": "replace_batch",
        }

        def glue_job(logical_id: str, name: str) -> glue.CfnJob:
            return glue.CfnJob(
                self,
                logical_id,
                name=name,
                role=glue_role.role_arn,
                glue_version="5.1",
                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl", python_version="3", script_location=script_asset.s3_object_url
                ),
                default_arguments={
                    **common_job_arguments,
                    "--batch_id": "required-at-runtime",
                    "--failure_scenario": "healthy",
                },
                execution_class="FLEX",
                worker_type="G.1X",
                number_of_workers=2,
                max_retries=0,
                timeout=30,
                execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=2),
                description="Idempotent batch ETL into the curated Amazon S3 table.",
            )

        orders_job = glue_job("OrdersEtlJob", f"{prefix}-orders-etl")
        recovery_job = glue_job("OrdersRecoveryEtlJob", f"{prefix}-orders-recovery-etl")
        orders_job.add_resource_dependency(orders_table)
        recovery_job.add_resource_dependency(orders_table)

        lambda_asset_path = Path(os.getenv("ABDR_LAMBDA_ASSET", str(PROJECT_DIR)))

        def function(
            logical_id: str,
            handler_module: str,
            *,
            timeout: Duration | None = None,
            memory_size: int = 512,
            environment: dict[str, str] | None = None,
        ) -> lambda_.Function:
            log_group = logs.LogGroup(
                self,
                f"{logical_id}LogGroup",
                retention=logs.RetentionDays.ONE_WEEK,
                removal_policy=RemovalPolicy.DESTROY,
            )
            return lambda_.Function(
                self,
                logical_id,
                function_name=(
                    f"{prefix}-{handler_module.replace('_handler', '').replace('_', '-')}"
                ),
                runtime=lambda_.Runtime.PYTHON_3_14,
                handler=f"{LAMBDA_HANDLER_PREFIX}.{handler_module}.lambda_handler",
                code=lambda_.Code.from_asset(
                    str(lambda_asset_path),
                    exclude=[".venv", ".build", "infrastructure/cdk.out", "tests", "docs"],
                ),
                timeout=timeout or Duration.minutes(2),
                memory_size=memory_size,
                environment=environment,
                log_group=log_group,
                tracing=lambda_.Tracing.ACTIVE,
            )

        validation_function = function(
            "ValidationFunction",
            "validation_handler",
            timeout=Duration.minutes(5),
            environment={
                "RAW_DATABASE": raw_database_name,
                "TABLE_BUCKET_NAME": table_bucket_name,
                "TABLE_NAMESPACE": namespace_name,
                "ATHENA_WORKGROUP": workgroup.name,
                "REPORT_BUCKET": report_bucket.bucket_name,
                "ATHENA_TIMEOUT_SECONDS": "120",
                "FRESHNESS_SLA_MINUTES": "1440",
            },
        )
        raw_bucket.grant_read(validation_function, "manifests/*")
        report_bucket.grant_read_write(validation_function)
        validation_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                ],
                resources=[
                    f"arn:{Aws.PARTITION}:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:workgroup/{workgroup.name}"
                ],
            )
        )
        validation_function.add_to_role_policy(
            iam.PolicyStatement(actions=["glue:GetJobRun"], resources=["*"])
        )
        validation_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetCatalog",
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetPartitions",
                    "lakeformation:GetDataAccess",
                    "s3tables:GetTableBucket",
                    "s3tables:GetNamespace",
                    "s3tables:GetTable",
                    "s3tables:GetTableData",
                ],
                resources=["*"],
            )
        )

        investigation_function = function(
            "InvestigationFunction",
            "investigation_handler",
            timeout=Duration.minutes(15),
            memory_size=2048,
            environment={
                "REPORT_BUCKET": report_bucket.bucket_name,
                "BEDROCK_MODEL_ID": model_id.value_as_string,
                "GLUE_JOB_NAME": orders_job.ref,
                "GLUE_LOG_GROUP": "/aws-glue/jobs/logs-v2",
                "AGENT_MAX_TURNS": "12",
                "AGENT_MAX_OUTPUT_TOKENS": "4000",
            },
        )
        report_bucket.grant_read_write(investigation_function, "reports/*")
        investigation_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
                resources=[model_arn.value_as_string],
            )
        )
        investigation_function.add_to_role_policy(
            iam.PolicyStatement(actions=["glue:GetJob", "glue:GetJobRun"], resources=["*"])
        )
        investigation_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["logs:StartQuery", "logs:GetQueryResults", "logs:StopQuery"],
                resources=[
                    f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws-glue/jobs/logs-v2:*"
                ],
            )
        )
        investigation_function.add_to_role_policy(
            iam.PolicyStatement(actions=["athena:GetQueryExecution"], resources=["*"])
        )

        plan_function = function("PlanFunction", "plan_handler")
        approval_function = function(
            "ApprovalFunction",
            "approval_handler",
            environment={"REPORT_BUCKET": report_bucket.bucket_name},
        )
        report_bucket.grant_put(approval_function, "reports/approvals/*")
        recovery_function = function("RecoveryFunction", "recovery_handler")

        workflow_log_group = logs.LogGroup(
            self,
            "ReliabilityWorkflowLogGroup",
            log_group_name=f"/aws/vendedlogs/states/{prefix}-reliability-recovery",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        definition = self._workflow_definition(
            validation_function=validation_function,
            investigation_function=investigation_function,
            plan_function=plan_function,
            approval_function=approval_function,
            recovery_function=recovery_function,
            recovery_job_name=recovery_job.ref,
            common_job_arguments=common_job_arguments,
        )
        state_machine = sfn.StateMachine(
            self,
            "ReliabilityRecoveryStateMachine",
            state_machine_name=f"{prefix}-reliability-recovery",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            state_machine_type=sfn.StateMachineType.STANDARD,
            timeout=Duration.days(2),
            logs=sfn.LogOptions(
                destination=workflow_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=False,
            ),
            tracing_enabled=True,
        )

        lambda_functions = [
            validation_function,
            investigation_function,
            plan_function,
            approval_function,
            recovery_function,
        ]
        for lambda_function in lambda_functions:
            if lambda_function.role is not None:
                NagSuppressions.add_resource_suppressions(
                    lambda_function.role,
                    [
                        {
                            "id": "AwsSolutions-IAM4",
                            "reason": (
                                "The CDK-generated AWSLambdaBasicExecutionRole grants the standard "
                                "CloudWatch Logs permissions required by this Lambda function."
                            ),
                        },
                        {
                            "id": "AwsSolutions-IAM5",
                            "reason": (
                                "CDK grants and X-Ray/CloudWatch APIs require wildcard child "
                                "resources or actions. Business data and Bedrock access remain "
                                "scoped in the explicit policies."
                            ),
                        },
                    ],
                    apply_to_children=True,
                )
        NagSuppressions.add_resource_suppressions(
            state_machine.role,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "The CDK Step Functions integrations generate wildcard child-resource "
                        "permissions for Lambda invocation, Glue job polling, logging, and X-Ray; "
                        "the workflow definition fixes the callable functions and recovery job."
                    ),
                }
            ],
            apply_to_children=True,
        )

        events.Rule(
            self,
            "GlueJobSucceededRule",
            rule_name=f"{prefix}-on-glue-success",
            description="Starts deterministic validation after the primary Glue job succeeds.",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={"jobName": [orders_job.ref], "state": ["SUCCEEDED"]},
            ),
            targets=[
                event_targets.SfnStateMachine(
                    state_machine,
                    input=events.RuleTargetInput.from_event_path("$"),
                    retry_attempts=2,
                    max_event_age=Duration.hours(2),
                )
            ],
        )

        NagSuppressions.add_resource_suppressions(
            glue_role,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": (
                        "AWSGlueServiceRole is the AWS-managed baseline required by Glue jobs."
                    ),
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "Glue and Lake Formation catalog discovery require wildcard "
                        "catalog/table scopes; "
                        "S3 Tables data writes remain scoped to this table bucket and table ARN."
                    ),
                },
            ],
            apply_to_children=True,
        )
        NagSuppressions.add_resource_suppressions(
            access_log_bucket,
            [
                {
                    "id": "AwsSolutions-S1",
                    "reason": (
                        "This is the dedicated server-access-log destination; enabling logging "
                        "on it would recursively create more access logs."
                    ),
                }
            ],
        )
        for job in (orders_job, recovery_job):
            NagSuppressions.add_resource_suppressions(
                job,
                [
                    {
                        "id": "AwsSolutions-GL1",
                        "reason": (
                            "The sample processes synthetic data and uses service-managed "
                            "CloudWatch Logs encryption; production workloads can add a KMS-backed "
                            "Glue security configuration."
                        ),
                    },
                    {
                        "id": "AwsSolutions-GL3",
                        "reason": (
                            "Glue bookmarks are intentionally disabled because each run replaces "
                            "one explicit batch_id partition idempotently."
                        ),
                    },
                ],
            )

        CfnOutput(self, "RawBucketName", value=raw_bucket.bucket_name)
        CfnOutput(self, "ReportBucketName", value=report_bucket.bucket_name)
        CfnOutput(self, "TableBucketName", value=table_bucket_name)
        CfnOutput(self, "OrdersJobName", value=orders_job.ref)
        CfnOutput(self, "RecoveryJobName", value=recovery_job.ref)
        CfnOutput(self, "OrdersEtlRoleArn", value=glue_role.role_arn)
        if validation_function.role is not None:
            CfnOutput(self, "ValidationRoleArn", value=validation_function.role.role_arn)
        CfnOutput(self, "StateMachineArn", value=state_machine.state_machine_arn)
        CfnOutput(
            self,
            "S3TablesCatalog",
            value=f"s3tablescatalog/{table_bucket_name}",
            description="Shared regional integration must be enabled before deployment.",
        )

    def _data_bucket(
        self, logical_id: str, bucket_name: str, access_log_bucket: s3.IBucket
    ) -> s3.Bucket:
        return s3.Bucket(
            self,
            logical_id,
            bucket_name=bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=True,
            server_access_logs_bucket=access_log_bucket,
            server_access_logs_prefix=f"{logical_id.lower()}/",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

    def _workflow_definition(
        self,
        *,
        validation_function: lambda_.Function,
        investigation_function: lambda_.Function,
        plan_function: lambda_.Function,
        approval_function: lambda_.Function,
        recovery_function: lambda_.Function,
        recovery_job_name: str,
        common_job_arguments: dict[str, str],
    ) -> sfn.IChainable:
        initial_validation = tasks.LambdaInvoke(
            self,
            "ValidateBatch",
            lambda_function=validation_function,
            payload_response_only=True,
            task_timeout=sfn.Timeout.duration(Duration.minutes(6)),
        )
        investigation = tasks.LambdaInvoke(
            self,
            "InvestigateIncident",
            lambda_function=investigation_function,
            payload_response_only=True,
            task_timeout=sfn.Timeout.duration(Duration.minutes(15)),
        )
        plan = tasks.LambdaInvoke(
            self,
            "BuildRecoveryPlan",
            lambda_function=plan_function,
            payload_response_only=True,
        )
        approval = tasks.LambdaInvoke(
            self,
            "WaitForHumanApproval",
            lambda_function=approval_function,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object(
                {
                    "task_token": sfn.JsonPath.task_token,
                    "workflow_context": sfn.JsonPath.object_at("$"),
                }
            ),
            task_timeout=sfn.Timeout.duration(Duration.hours(24)),
        )
        prepare_recovery = tasks.LambdaInvoke(
            self,
            "ValidateApprovedRecovery",
            lambda_function=recovery_function,
            payload_response_only=True,
        )
        recovery_arguments: dict[str, Any] = {
            **common_job_arguments,
            "--batch_id": sfn.JsonPath.string_at("$.batch_id"),
            "--failure_scenario": "healthy",
            "--incident_id": sfn.JsonPath.string_at("$.incident_id"),
            "--recovery_execution_id": sfn.JsonPath.string_at("$.recovery_execution_id"),
        }
        run_recovery = tasks.GlueStartJobRun(
            self,
            "RunSelectiveBatchRecovery",
            glue_job_name=recovery_job_name,
            arguments=sfn.TaskInput.from_object(recovery_arguments),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            task_timeout=sfn.Timeout.duration(Duration.minutes(40)),
            result_path="$.recovery_job",
        )
        verify = tasks.LambdaInvoke(
            self,
            "VerifyBatchRecovery",
            lambda_function=validation_function,
            payload=sfn.TaskInput.from_object(
                {
                    "batch_id": sfn.JsonPath.string_at("$.batch_id"),
                    "pipeline_run_id": sfn.JsonPath.string_at("$.recovery_job.JobRunId"),
                    "manifest_uri": sfn.JsonPath.string_at("$.manifest_uri"),
                    "validation_kind": "recovery",
                }
            ),
            payload_response_only=True,
            task_timeout=sfn.Timeout.duration(Duration.minutes(6)),
        )
        healthy = sfn.Succeed(self, "BatchReliable")
        duplicate = sfn.Succeed(self, "DuplicateEventIgnored")
        resolved = sfn.Succeed(self, "IncidentResolved")
        unresolved = sfn.Fail(
            self, "RecoveryVerificationFailed", error="ReliabilityChecksFailedAfterRecovery"
        )
        validation_error = sfn.Fail(
            self, "ValidationInfrastructureError", error="DeterministicValidationError"
        )
        verify_choice = (
            sfn.Choice(self, "RecoveryPassed")
            .when(sfn.Condition.string_equals("$.validation_status", "PASS"), resolved)
            .otherwise(unresolved)
        )
        failure_path = (
            investigation.next(plan)
            .next(approval)
            .next(prepare_recovery)
            .next(run_recovery)
            .next(verify)
            .next(verify_choice)
        )
        choice = (
            sfn.Choice(self, "ValidationPassed")
            .when(sfn.Condition.string_equals("$.validation_status", "PASS"), healthy)
            .when(sfn.Condition.string_equals("$.validation_status", "DUPLICATE"), duplicate)
            .when(sfn.Condition.string_equals("$.validation_status", "ERROR"), validation_error)
            .otherwise(failure_path)
        )
        return initial_validation.next(choice)
