# Troubleshooting

## CDK reports a cloud assembly schema mismatch

Install AWS CDK CLI v2.1146.1 or later. The Python infrastructure dependency emits cloud assembly
schema 55, which older v2 CLIs cannot read. Keep the CLI at least as current as `aws-cdk-lib`.

## Curated table is absent in Athena

- Verify `aws glue get-catalog --catalog-id s3tablescatalog` in the same Region.
- Select child catalog `s3tablescatalog/abdr-<stage>-curated-<account-id>`, not `AwsDataCatalog`.
- Confirm every namespace, table, and column is lowercase.
- Check IAM and, in Regions that require it, Lake Formation grants.

## Glue cannot resolve the S3 Tables catalog

- Confirm Glue version is 5.1 and `--datalake-formats=iceberg` is present.
- Inspect the effective `--conf` for the account ID and table-bucket child catalog.
- Do not add the legacy S3 Tables catalog JAR; this sample uses the analytics integration.

## Validation returns ERROR

An Athena timeout or API failure is intentionally `ERROR`, not `FAIL`. Inspect the validation JSON,
query execution status, workgroup scan cutoff, result-bucket permissions, and catalog access.

## No workflow execution after Glue succeeds

Check the `abdr-<stage>-on-glue-success` EventBridge rule and confirm the run used the primary orders
job, not the separate recovery job.

## Agent fails before producing a report

- Confirm the model ID and ARN refer to the same tool-capable Bedrock resource.
- Confirm model access in the deployment Region.
- Check Lambda package size and that deployment used `make deploy`, which vendors Strands.
- Inspect the investigation Lambda log group; do not paste raw sensitive logs into prompts.

## Local MCP tools do not start

Confirm `uvx`, AWS credentials, and Python 3.10+ are available. The servers intentionally omit
`--allow-write` and `--allow-sensitive-data-access`. Tool names and behavior can change upstream;
review the AWS Labs READMEs before upgrading package versions in a production fork.

## Approval appears stuck

Find the encrypted request under `reports/approvals/`, verify that the workflow is still within its
24-hour timeout, and submit exactly one `abdr-approve` decision. A task token cannot be reused.
