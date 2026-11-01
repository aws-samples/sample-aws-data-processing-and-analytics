# Implementation plan

## Repository findings

The repository separates blog companion code, independently deployable samples, and workshops. Its
current Python sample uses a root-level import package, `pyproject.toml`, pytest, Ruff, self-contained
documentation, and the repository-level MIT-0 license. Infrastructure examples use explicit resource
descriptions, CDK assertions, `cdk-nag`, bounded retention, and account/Region-qualified S3 names.

No existing sample implements deterministic source-to-target business-key reconciliation, S3 Tables,
human-approved batch recovery, or a Strands investigation agent. The Medallion blog detects execution
failures; it does not detect a successful job that produced incorrect data.

## Compatibility decisions

Verified against official documentation on 2026-10-29:

- Amazon S3 Tables is represented with the CloudFormation-supported `AWS::S3Tables::TableBucket`,
  `Namespace`, and `Table` resources through stable CDK L1 constructs.
- AWS Glue 5.1 includes Apache Iceberg 1.10.0 and supports S3 Tables. The job uses native Iceberg
  support through `--datalake-formats=iceberg`; no legacy S3 Tables catalog JAR is used.
- The recommended AWS analytics integration creates the shared regional `s3tablescatalog` and maps a
  table bucket to a child catalog, a namespace to a database, and a table to a Glue table.
- Athena addresses the curated table using catalog `s3tablescatalog/<table-bucket>`, namespace
  `order_processing`, and table `orders`.
- The AWS Labs Data Processing MCP tools used are `manage_aws_glue_jobs`,
  `manage_aws_athena_query_executions`, and `manage_aws_athena_databases_and_tables`.
- The AWS Labs CloudWatch MCP tools used are `describe_log_groups`, `analyze_log_group`,
  `execute_log_insights_query`, `get_logs_insight_query_results`, and
  `cancel_logs_insight_query`.
- Both MCP subprocesses start in their default read-only modes. Neither `--allow-write` nor
  `--allow-sensitive-data-access` is enabled.

Sources:

- [S3 Tables with Glue](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue.html)
- [S3 Tables analytics integration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integration-overview.html)
- [S3 Tables with Athena](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-athena.html)
- [Glue Iceberg support](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
- [AWS Data Processing MCP](https://github.com/awslabs/mcp/tree/main/src/aws-dataprocessing-mcp-server)
- [CloudWatch MCP](https://github.com/awslabs/mcp/tree/main/src/cloudwatch-mcp-server)
- [Strands Agents Python SDK](https://github.com/strands-agents/sdk-python)

## Milestones

### Milestone 0 — analysis

- Record repository patterns and current service compatibility.
- Define naming, security boundaries, shared-resource behavior, and test levels.

### Milestone 1 — infrastructure and data

- Create raw/report buckets, S3 table bucket, namespace, and Iceberg table.
- Add deterministic generator, independent manifest, Glue 5.1 ETL, and idempotent batch writes.
- Add CDK synthesis and security assertions.

### Milestone 2 — deterministic reliability

- Implement exact counts, business-key comparisons, duplicates, nulls, freshness, and schema checks.
- Store reports in S3 and distinguish `FAIL` from query/infrastructure `ERROR`.
- Prove locally that equal counts can still contain missing keys.

### Milestone 3 — investigation agent

- Implement Strands structured output and bounded custom tools.
- Connect verified AWS Labs MCP subprocesses for local/container investigation.
- Add a deployed read-only evidence adapter for Lambda, where `uvx` stdio processes are unavailable.
- Add prompt-injection, missing-evidence, timeout, and schema validation tests.

### Milestone 4 — orchestration and recovery

- Trigger from Glue success through EventBridge.
- Branch on deterministic validation status.
- Wait on an explicit Step Functions task-token callback.
- Re-run a separate recovery job with healthy logic and replace only the affected batch partition.
- Revalidate and resolve only after all required checks pass.

### Milestone 5 — review readiness

- Run unit tests, Ruff, CDK synthesis, CDK assertions, and `cdk-nag`.
- Complete deployment, demo, cost, security, cleanup, and contribution documentation.
- Report AWS integration tests as unverified until run in a configured account.

## Acceptance boundaries

Local tests can verify generation, injection, reconciliation, safety policy, structured schemas, SQL
construction, CDK synthesis, and IAM/resource definitions. Glue, S3 Tables, Athena, Bedrock, MCP
subprocesses, EventBridge delivery, approval callbacks, and recovery must be tested in a deployed AWS
account before claiming the end-to-end path works.
