# Deploy: MSK Diagnostic MCP for AWS DevOps Agent

This directory deploys the MCP server as a VPC-attached Lambda behind a Function URL, ready to register with AWS DevOps Agent.

> **This is sample code.** Test in a non-production account first.

## Architecture

```
DevOps Agent ──(SigV4 over HTTPS)──► Lambda Function URL ──► VPC-attached Lambda
                                                                    │
                                                                    │ Kafka wire (IAM SASL)
                                                                    ▼
                                                            MSK Brokers :9098
```

- **Transport:** Streamable HTTP via [Lambda Web Adapter](https://github.com/awslabs/aws-lambda-web-adapter). The Function URL runs `RESPONSE_STREAM` mode.
- **Auth (ingress):** `AWS_IAM` on the Function URL. DevOps Agent signs requests with SigV4.
- **Auth (egress to MSK):** IAM SASL/OAUTHBEARER using the Lambda execution role.
- **Isolation:** the Lambda is scoped by IAM to a single cluster ARN AND runs inside your VPC subnets — either boundary alone stops arbitrary broker access.

## Prerequisites

- AWS SAM CLI: `brew install aws-sam-cli` (or `pip install aws-sam-cli`)
- Python 3.12 on your workstation (matches Lambda runtime — needed for the local wheel build during `sam build`)
- IAM permissions to create Lambda functions, IAM roles, layers, Function URLs, and security groups
- One or more MSK clusters with IAM authentication enabled
- A VPC + subnets that can reach the target cluster(s) on port 9098 (typically the same VPC as the cluster, or a peered VPC)

The template auto-creates the Lambda's own security group. Optionally, if you provide the MSK cluster's security group id, the template also injects the ingress rule (port 9098) into it for you.

## Deploy

```bash
cd samples/msk-diagnostic-mcp/deploy

# --use-container ensures Linux wheels (librdkafka native binary matches Lambda's runtime)
sam build --use-container

sam deploy --guided \
  --stack-name msk-diagnostic-mcp \
  --parameter-overrides \
      StageName=dev \
      AllowedClusterArns='arn:aws:kafka:us-east-1:123456789012:cluster/foo/uuid-a,arn:aws:kafka:us-east-1:123456789012:cluster/bar/uuid-b' \
      VpcId=vpc-xxx \
      VpcSubnetIds=subnet-aaa,subnet-bbb \
      MskClusterSecurityGroupId=sg-msk-cluster \
      AllowSensitiveDataAccess=false
```

### Multi-cluster

Pass one or more MSK cluster ARNs to `AllowedClusterArns` (comma-separated). The Lambda accepts a `cluster_arn` argument per tool call and rejects any ARN not in this allowlist. Networking is still bounded by the Lambda's `VpcConfig` — all listed clusters must be reachable from `VpcSubnetIds`.

### Dev shortcut

For dev/staging you can pass `AllowedClusterArns='*'` to skip the allowlist. `StageName=prod` **refuses** `'*'` via a SAM `Rules` assertion.

### Outputs

- `FunctionUrl` — register this with DevOps Agent (below)
- `LambdaSecurityGroupId` — if you did not pass `MskClusterSecurityGroupId`, add this SG as an ingress source on port 9098 in your MSK cluster's SG manually

## Register with AWS DevOps Agent

1. In the DevOps Agent console, create a **Private Connection**.
2. Service Name: `lambda`.
3. Endpoint: the `FunctionUrl` from the CloudFormation outputs above.
4. Auth: SigV4 (AWS_IAM). DevOps Agent uses the caller's own IAM identity to sign — grant the appropriate DevOps Agent role permission to invoke the Function URL via `lambda:InvokeFunctionUrl` on this function's ARN.
5. Register the MCP tools — the agent will discover the 8 tools automatically via MCP `tools/list`.

## About sensitive data access

`read_topic_data` returns message payloads. It's disabled by default. To enable, redeploy with:

```bash
sam deploy --parameter-overrides AllowSensitiveDataAccess=true ... (other params)
```

The flag becomes the `MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS=true` environment variable on the Lambda. Off by default because message payloads may contain PII, credentials, or other sensitive data.

## Troubleshooting

**Cold start is slow (3–5s).** Expected for VPC-attached Python Lambda with librdkafka. Warm invocations are 100–500ms. Consider Provisioned Concurrency if you need consistently fast responses.

**Timeouts on `read_topic_data` at `position=latest`.** The tool blocks up to its `timeout_seconds` parameter waiting for messages on idle partitions. If the tool call is exceeding the Lambda's own 30s timeout, either shorten the tool's `timeout_seconds` argument or increase `Globals.Function.Timeout` in `template.yaml`.

**"Broker transport failure"** — network path issue. Check that the Lambda's SGs allow egress to the MSK cluster's SG on 9098, and that the subnets have routes (NAT gateway if using public endpoints, or direct route for private endpoints).

## What lives where

```
deploy/
├── template.yaml                       SAM template (Lambda, layer, Function URL, IAM)
├── src/
│   ├── run.sh                          Lambda Web Adapter entrypoint
│   └── lambda_server.py                Imports mcp from sibling package, runs streamable-http
├── layers/dependencies/
│   ├── requirements.txt                Runtime deps (mcp, confluent-kafka, boto3, ...)
│   └── Makefile                        SAM build hook: pip-installs deps + sibling package
└── README.md                           You are here
```

The Lambda reuses the exact same `server.py` and tool logic from the sibling `amazon_msk_diagnostic_mcp/` package — no forked copy. The Makefile installs the sibling package into the layer at `sam build` time.
