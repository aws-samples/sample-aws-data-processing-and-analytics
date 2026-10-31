# Naming conventions

The display name is **AWS Data Reliability Copilot**. The repository slug remains
`agentic-batch-data-reliability` to describe the implementation pattern.

| Surface | Convention | Example |
|---|---|---|
| Repository directories | kebab-case | `missing-records/` |
| Python packages and fields | snake_case | `validation_id` |
| Python/CDK classes | PascalCase | `DataReliabilityStack` |
| Physical AWS resources | lowercase kebab-case | `abdr-dev-orders-etl` |
| SQL identifiers | lowercase snake_case | `order_processing.orders` |
| Enum values | UPPER_SNAKE_CASE | `PASS`, `CRITICAL` |

AWS physical resources use `abdr-<stage>-<purpose>`. General S3 buckets append account and Region;
the S3 table bucket appends the account ID so repeated deployments and account-local child catalogs
have a collision-resistant name. The supported stages are `dev`, `test`, and `prod`.

Stable operational IDs use a type prefix and a deterministic digest:

```text
val-<20 hex characters>
inc-<20 hex characters>
rec-<20 hex characters>
```

Batch IDs are supplied by the producer and must be non-empty, bounded, and contain no whitespace.
