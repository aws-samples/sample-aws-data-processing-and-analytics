# Cleanup

```bash
make destroy STAGE=dev AWS_REGION=us-east-1
```

The stack uses destroy policies for sample-owned S3, S3 Tables, Glue, Lambda, Step Functions,
EventBridge, Athena, and log resources. The S3 buckets have stack-managed emptying for their versions.

The command intentionally does **not** delete `s3tablescatalog`. It is a shared regional integration
that may predate this stack or serve other table buckets. To remove it, an account administrator must
first verify that no other workload uses it and follow the current official S3 Tables integration
cleanup procedure. That separate destructive action is outside this sample's cleanup automation.

After deletion, verify the stack is gone and inspect for resources retained because of a failed service
deletion. S3 Tables snapshot/maintenance activity can continue until its table and bucket are removed.
