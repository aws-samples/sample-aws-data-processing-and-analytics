"""Lambda entry point: run the FastMCP server over Streamable HTTP.

Reuses the same `mcp` instance and 8 tools defined in the sibling
``amazon_msk_diagnostic_mcp/server.py`` — no tool logic is duplicated. The
package is installed into ``/opt/python`` by the dependencies layer.

Wrapped by Lambda Web Adapter (LWA), which forwards HTTP requests from the
Function URL to this process on ``AWS_LWA_PORT`` (default 8000).
"""

import os

from amazon_msk_diagnostic_mcp import server as msk_server


def _sensitive_from_env() -> bool:
    return os.environ.get('MSK_MCP_ALLOW_SENSITIVE_DATA_ACCESS', '').lower() in (
        'true',
        '1',
        'yes',
    )


if __name__ == '__main__':
    # Honour the env-var gates. The CLI-flag path in the sibling module's
    # main() does not run on Lambda; set the module-level flags directly.
    msk_server._ALLOW_SENSITIVE_DATA_ACCESS = _sensitive_from_env()
    msk_server._ALLOWED_CLUSTER_ARNS = msk_server._parse_cluster_allowlist(
        os.environ.get('MSK_MCP_ALLOWED_CLUSTER_ARNS', '')
    )

    # mcp.server.fastmcp's FastMCP.run() takes only `transport` — host/port
    # and transport-security are configured via mcp.settings.
    msk_server.mcp.settings.host = '0.0.0.0'
    msk_server.mcp.settings.port = int(os.environ.get('AWS_LWA_PORT', '8000'))

    # DNS-rebinding protection defaults to allowing only localhost hosts,
    # which rejects requests routed through the Lambda Function URL host.
    # Ingress auth (AWS_IAM SigV4 on the Function URL) protects us against
    # unauthenticated callers, so DNS-rebinding checks here are redundant.
    msk_server.mcp.settings.transport_security.enable_dns_rebinding_protection = False

    msk_server.mcp.run(transport='streamable-http')
