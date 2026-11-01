"""Read-only AWS Labs MCP subprocesses with a reviewed tool allowlist."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import Any

VERIFIED_TOOL_NAMES = {
    "manage_aws_glue_jobs",
    "manage_aws_athena_query_executions",
    "manage_aws_athena_databases_and_tables",
    "describe_log_groups",
    "analyze_log_group",
    "execute_log_insights_query",
    "get_logs_insight_query_results",
    "cancel_logs_insight_query",
}
DATA_PROCESSING_MCP_SPEC = os.getenv(
    "AWS_DATA_PROCESSING_MCP_SPEC", "awslabs.aws-dataprocessing-mcp-server@0.2.1"
)
CLOUDWATCH_MCP_SPEC = os.getenv("AWS_CLOUDWATCH_MCP_SPEC", "awslabs.cloudwatch-mcp-server@0.3.1")


def _tool_name(tool: Any) -> str | None:
    name = getattr(tool, "tool_name", None)
    if name:
        return str(name)
    spec = getattr(tool, "tool_spec", None)
    if isinstance(spec, dict):
        return spec.get("name")
    return None


@contextmanager
def read_only_aws_tools(region: str) -> Iterator[list[Any]]:
    """Start official stdio servers without write or sensitive-data flags."""

    from mcp import StdioServerParameters, stdio_client
    from strands.tools.mcp import MCPClient

    common_env = {
        "AWS_REGION": region,
        "FASTMCP_LOG_LEVEL": "ERROR",
    }
    if profile := os.getenv("AWS_PROFILE"):
        common_env["AWS_PROFILE"] = profile

    clients = [
        MCPClient(
            lambda: stdio_client(
                StdioServerParameters(
                    command="uvx",
                    args=[DATA_PROCESSING_MCP_SPEC],
                    env=common_env,
                )
            )
        ),
        MCPClient(
            lambda: stdio_client(
                StdioServerParameters(
                    command="uvx",
                    args=[CLOUDWATCH_MCP_SPEC],
                    env=common_env,
                )
            )
        ),
    ]
    with ExitStack() as stack:
        tools: list[Any] = []
        for client in clients:
            stack.enter_context(client)
            tools.extend(
                tool for tool in client.list_tools_sync() if _tool_name(tool) in VERIFIED_TOOL_NAMES
            )
        yield tools
