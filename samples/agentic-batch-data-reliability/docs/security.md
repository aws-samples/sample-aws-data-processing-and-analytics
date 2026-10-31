# Security

## Control separation

| Component | Allowed | Explicitly disallowed |
|---|---|---|
| Validator | Read source/target, execute bounded Athena queries, write reports | Recover data |
| Agent | Read reports and operational evidence, invoke one configured Bedrock model | S3 Tables writes, Glue starts, approval callbacks |
| Planner | Construct a typed proposal | AWS API calls |
| Approval operator | Read one request and submit its callback | Change the recovery scope |
| Recovery workflow | Start the dedicated recovery job after approval | Unapproved or full-table overwrite |

## IAM notes

- S3 buckets block all public access, require TLS, use S3-managed encryption, enable versioning, and
  send access logs to a dedicated bucket with 30-day retention.
- The S3 table bucket uses SSE-S3 and S3 Tables maintenance.
- The Glue role scopes S3 Tables data operations to the deployed bucket and `orders` table ARNs.
- `lakeformation:GetDataAccess` requires `Resource: *`; this is documented and suppressed in
  `cdk-nag` only for that technical requirement.
- Federated Glue child resources and some read-only Glue/Athena APIs do not support useful resource
  scoping. Wildcards are limited to read operations and documented in the stack.
- Bedrock invocation is scoped to the exact ARN supplied at deployment.
- Step Functions logs exclude execution data so task tokens and report content are not copied to logs.
- Bounded Step Functions retries, Lambda timeouts, Athena scan limits, and agent turn/token limits
  constrain accidental cost and request fan-out. The sample does not reserve Lambda concurrency so it
  remains deployable in accounts with the default low concurrency quota; production deployments can
  add reservations after checking the account's required unreserved pool.

## Prompt injection

Logs, SQL results, and stored JSON are untrusted. The agent prompt forbids following retrieved
instructions. Tools return bounded structured responses, raw payload access is excluded, MCP servers
run read-only without sensitive-data mode, and the output schema fixes approval/recovery fields to
`PENDING`/`NOT_STARTED`.

These controls reduce risk but do not prove that a model cannot be manipulated. The deterministic
approval and recovery boundaries remain mandatory even when the agent output appears trustworthy.

## Approval token

The callback task token is stored in the encrypted approval request object. Access to
`reports/approvals/*` authorizes a decision and must be restricted to designated operators. Do not
send approval URIs or their contents through chat, issue trackers, or logs.

## Production adaptations

- Use customer-managed KMS keys when organizational policy requires them.
- Separate approval operators from deployers and data writers.
- Add per-batch ownership/locking for multiple schedulers.
- Add Amazon Bedrock Guardrails appropriate to the data classification.
- Route approval through an authenticated application rather than direct S3 object access.
- Increase retention only under an explicit records policy.
