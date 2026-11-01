# Example incident: missing records

This example is generated from deterministic local fixtures; it is not represented as a deployed
AWS execution.

- Batch: `batch-example-001`
- Severity: `CRITICAL`
- Observed source count: 100
- Observed target count: 90
- Missing business keys: 10
- Glue execution status: `SUCCEEDED`
- Recommended action: replace only the affected `batch_id` after human approval
- Approval status: `PENDING`

The incident remains open until the approved recovery completes and every critical deterministic
reconciliation rule returns `PASS`.
