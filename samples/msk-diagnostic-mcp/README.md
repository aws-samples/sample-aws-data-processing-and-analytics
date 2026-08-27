# Amazon MSK Diagnostic MCP Server

A sample [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that lets an AI agent inspect an Amazon MSK cluster at the Kafka broker level, using the caller's own AWS IAM credentials.

The AWS-provided MSK MCP server and the generic AWS API MCP server both wrap the MSK **control plane** (`ListClusters`, `DescribeCluster`, `ListNodes`, etc.). This sample covers the **data plane** — the operations that require speaking the Kafka wire protocol on port 9098 directly to the brokers, which no AWS API exposes. Those are the operations an on-call engineer typically reaches for the Kafka CLI to perform.

**This is sample code.** It is provided as a starting point for building your own MSK-aware AI diagnostics workflow. It is not a supported AWS product.

## What it can do

Eight read-only tools, all requiring an MSK cluster ARN as input:

| Tool | What it answers |
|---|---|
| `list_topics` | What topics exist on this cluster? |
| `describe_topics` | For these topics, what is the partition layout (leader, replicas, in-sync replicas, under-replicated flag)? |
| `list_consumer_groups` | What consumer groups exist, in what state (STABLE, EMPTY, DEAD, …)? |
| `describe_consumer_groups` | For these groups, who are the members, what are they assigned, and how far behind (per-partition lag) are they? |
| `describe_topic_configs` | Which topics have dynamic configuration overrides that differ from cluster defaults? |
| `describe_broker_configs` | Which brokers have dynamic configuration overrides applied post-boot? |
| `get_topic_offsets` | For these topics, what are the earliest/latest offsets per partition, and how many messages are retained? |
| `read_topic_data` | *(Sensitive — disabled by default.)* Read message payloads from a topic. |

### Sensitive data access

`read_topic_data` returns actual Kafka message contents, which may contain PII, credentials, or other sensitive data. It is **disabled by default** and only enabled if the server operator explicitly opts in at startup with the environment variable `MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS=true` or the CLI flag `--allow-sensitive-data-access=true`. When disabled, the tool raises immediately without contacting the broker.

## Prerequisites

- Python 3.10+
- [`uv`](https://docs.astral.sh/uv/) — used by most MCP clients to launch Python-based servers
- AWS credentials configured (via `~/.aws/config`, environment variables, or the SDK default provider chain) for the account that owns the MSK cluster
- Network reachability from wherever the MCP server runs to the MSK brokers on port 9098 (typically means running inside the customer's VPC, over VPN, or through a public bootstrap endpoint with appropriate security-group rules)
- IAM permissions on the caller for the operations you want to perform — at minimum `kafka-cluster:Connect` and `kafka-cluster:DescribeCluster`, plus the relevant `Describe*` / `ReadData` actions

## Install and run

Install and run directly from this repository without cloning:

```bash
uvx --from git+https://github.com/aws-samples/sample-aws-data-processing-and-analytics.git#subdirectory=samples/msk-diagnostic-mcp amazon-msk-diagnostic-mcp
```

Or clone and run locally:

```bash
git clone https://github.com/aws-samples/sample-aws-data-processing-and-analytics.git
cd sample-aws-data-processing-and-analytics/samples/msk-diagnostic-mcp
uv run amazon-msk-diagnostic-mcp
```

## Configure your MCP client

Add the server to your MCP client's config. Examples below use the `uvx` invocation, which auto-installs on first launch.

### Claude Code (`~/.claude/claude_code_config.json`)

```json
{
  "mcpServers": {
    "amazon-msk-diagnostic-mcp": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/aws-samples/sample-aws-data-processing-and-analytics.git#subdirectory=samples/msk-diagnostic-mcp",
        "amazon-msk-diagnostic-mcp"
      ]
    }
  }
}
```

### Kiro (`~/.kiro/settings/mcp.json`)

Same shape as Claude Code. To enable the sensitive-data tool, add an `env` block:

```json
{
  "mcpServers": {
    "amazon-msk-diagnostic-mcp": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/aws-samples/sample-aws-data-processing-and-analytics.git#subdirectory=samples/msk-diagnostic-mcp",
        "amazon-msk-diagnostic-mcp"
      ],
      "env": {
        "MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS": "true"
      }
    }
  }
}
```

Pair it with the AWS API MCP server (or the AWS-provided MSK control-plane MCP) so the agent can resolve cluster names to ARNs automatically.

## Sample prompts

With the server registered and an MSK cluster available to the caller's IAM principal, try prompts like:

- *"Are any consumer groups lagging on my `<cluster-name>` cluster? If so, which ones and by how much?"*
- *"Show me the partition skew across `hot-topic-01`, `hot-topic-05`, and `hot-topic-10`."*
- *"Which topics on `<cluster-name>` have non-default configuration overrides?"*
- *"The `stuck-consumer` group is way behind. Show me what messages it's about to process next."* (requires `MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS=true`)

The agent will typically resolve the cluster name to an ARN via the AWS API MCP, confirm the ARN with you, and then chain multiple tools on this server to build the answer.

## Design notes

- **Authentication**: the server holds no credentials of its own. Every broker connection uses the caller's AWS credentials from the SDK provider chain, from which an IAM SASL `OAUTHBEARER` token is minted per session. Brokers see the caller's IAM principal, not a shared server identity.
- **Read-only**: no tool performs writes. `read_topic_data` uses a random one-shot consumer group with auto-commit disabled and manual partition assignment, so it never affects any existing group's committed offsets.
- **Response shape**: every tool returns a `summary` object alongside the detail list, so agents can triage without parsing full payloads.
- **Config filtering**: `describe_topic_configs` and `describe_broker_configs` return only dynamic overrides (`DYNAMIC_TOPIC_CONFIG`, `DYNAMIC_BROKER_CONFIG`, `DYNAMIC_DEFAULT_BROKER_CONFIG`, `GROUP_CONFIG`) — cluster defaults and static broker properties are filtered out to avoid drowning the agent in inherited settings.

## Not covered by this sample

- **Log-dir inspection.** `describe_log_dirs` is a native Kafka Admin API operation but is not exposed by the Python client this sample uses (`confluent-kafka-python`). Adding it would require a second Kafka client with its own auth configuration; that trade-off was left out of scope for this sample.
- **Writes.** Everything is read-only. Reset operations, config changes, and topic creation must go through the MSK control plane or the Kafka CLI directly.
- **Multi-cluster fan-out.** Each tool call targets a single cluster ARN. Agents that need to compare clusters can call the tools once per cluster.

