# Cost considerations

The sample has no continuously running compute. Charges can still accrue from:

- AWS Glue worker time for primary and recovery jobs
- Athena bytes scanned by validation queries
- Amazon S3 and S3 Tables storage, requests, maintenance, and snapshots
- AWS Glue Data Catalog integration requests/storage
- Amazon Bedrock input/output tokens
- Lambda invocations and duration
- Step Functions state transitions
- CloudWatch Logs ingestion, queries, and retention

Cost controls in the sample include two G.1X workers, 30-minute Glue timeouts, FLEX execution, a
1-GiB Athena per-query scan cutoff, batch predicates, 100-row evidence samples, one-week logs, bounded
agent turns/tokens, and no always-on container or database.

Pricing changes by Region and over time. Estimate with the current AWS Pricing Calculator and official
pricing pages before deployment; this repository intentionally does not embed a dollar estimate that
will become stale.

S3 snapshots and the shared Glue integration can continue to incur charges after a demo. The stack
deletes its table resources; it retains the shared `s3tablescatalog` because other workloads may use it.
