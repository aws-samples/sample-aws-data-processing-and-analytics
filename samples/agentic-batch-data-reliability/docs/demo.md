# Demo and integration test

## Healthy batch

```bash
SCENARIO=healthy make demo STAGE=dev AWS_REGION=us-east-1
```

Expected deployed behavior: Glue succeeds, validation returns `PASS`, and the workflow ends at
`BatchReliable` without creating an incident.

## Missing records

```bash
SCENARIO=missing_records make demo STAGE=dev AWS_REGION=us-east-1
```

The raw batch remains complete. Glue deterministically filters part of it, successfully commits the
remaining partition, and emits `SUCCEEDED`. Athena must report missing keys and `FAIL`.

## Duplicate records

```bash
SCENARIO=duplicate_records make demo STAGE=dev AWS_REGION=us-east-1
```

Athena must detect duplicate target keys independently of Glue status.

## Investigation and approval

Find the request after the agent and planner finish:

```bash
aws s3 ls "s3://<report-bucket>/reports/approvals/" --recursive
uv run abdr-approve \
  "s3://<report-bucket>/reports/approvals/incident_id=<incident-id>/request.json" \
  --approve \
  --region us-east-1
```

Use `--reject` to reject. Treat the approval object as sensitive because its task token authorizes one
workflow callback. The workflow times out after 24 hours if no decision is supplied.

## Full AWS checklist

Record real identifiers and results without committing sensitive data:

1. Confirm the S3 table bucket, namespace, table, and `s3tablescatalog` child catalog.
2. Run a healthy batch and query the curated table in Athena.
3. Confirm every healthy validation rule is `PASS`.
4. Run missing records and confirm Glue is `SUCCEEDED` but validation is `FAIL`.
5. Confirm bounded validation evidence includes exact missing-key count and a sample.
6. Confirm the investigation report separates facts, hypotheses, and limitations.
7. Inspect that no agent role has S3 Tables write or Step Functions callback permission.
8. Approve the exact batch and observe the separate recovery Glue job.
9. Confirm only that `batch_id` changed and the Iceberg commit succeeded.
10. Confirm post-recovery validation returns `PASS` before `IncidentResolved`.
11. Repeat a same-batch recovery and confirm row counts do not increase.
12. Reject another request and confirm no recovery job starts.
13. Run cleanup and inspect retained shared integration.

## Local deterministic example

`examples/incident_reports/missing-records.json` is produced from local fixtures and explicitly says
that it is not an AWS execution. Do not substitute fabricated CloudWatch logs or query IDs in docs.
