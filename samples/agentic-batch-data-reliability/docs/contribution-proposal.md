# Contribution proposal

## Proposed title

AWS Data Reliability Copilot

## Repository location

`samples/agentic-batch-data-reliability/`

## Problem

Batch operations often monitor job status but miss silent data defects. A Glue job can report success
while producing missing, duplicate, stale, schema-incompatible, or otherwise inconsistent records.

## Contribution

This sample demonstrates a reusable control pattern:

- deterministic source/target validation with exact business-key checks;
- Amazon S3 Tables as the curated Iceberg layer;
- evidence-grounded investigation using Strands, Bedrock, and verified AWS Labs MCP tools;
- explicit human approval through a Step Functions callback;
- idempotent replacement of only one Iceberg `batch_id` partition; and
- independent post-recovery verification.

The model has no recovery permission and cannot authorize its own recommendation.

## Review evidence requested before merge

- Unit, formatting, CDK synthesis, CDK assertion, and `cdk-nag` results
- A real AWS healthy/missing/duplicate run with identifiers redacted as appropriate
- Proof that Glue succeeded for the missing-record scenario while reconciliation failed
- Approval/rejection and post-recovery verification evidence
- Cleanup evidence and confirmation that shared `s3tablescatalog` was retained
- Security review of wildcard IAM statements and approval-token access

## License and scope

The contribution uses the repository MIT-0 license, modifies only its sample folder plus the repository
sample index, and introduces no continuously running infrastructure.
