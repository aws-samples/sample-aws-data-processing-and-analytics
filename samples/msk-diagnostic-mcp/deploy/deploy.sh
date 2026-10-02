#!/usr/bin/env bash
# Build and deploy the MSK Diagnostic MCP as a VPC-attached Lambda.
#
# Handles one wrinkle: `sam build --use-container` only mounts the layer's
# ContentUri (layers/dependencies/) into the build container, so it can't
# reach our sibling amazon_msk_diagnostic_mcp package via a relative path.
# We stage the package inside the layer dir before building, and clean it
# up afterwards.
#
# Usage:
#   ./deploy.sh                             # build only (no deploy)
#   ./deploy.sh --guided                    # first-time interactive deploy
#   ./deploy.sh --parameter-overrides ...   # non-interactive deploy

set -euo pipefail

cd "$(dirname "$0")"
DEPLOY_DIR="$PWD"
SAMPLE_ROOT="$(cd .. && pwd)"

STAGED_PKG="layers/dependencies/amazon_msk_diagnostic_mcp"

cleanup() {
  rm -rf "$STAGED_PKG"
}
trap cleanup EXIT

echo "[deploy.sh] Staging amazon_msk_diagnostic_mcp into layer source"
rm -rf "$STAGED_PKG"
cp -r "$SAMPLE_ROOT/amazon_msk_diagnostic_mcp" "$STAGED_PKG"

# Check for a container runtime (Docker or Finch).
if [ -z "${DOCKER_HOST:-}" ] && command -v finch >/dev/null 2>&1; then
  FINCH_SOCK="/Applications/Finch/lima/data/finch/sock/finch.sock"
  if [ -S "$FINCH_SOCK" ]; then
    echo "[deploy.sh] Using Finch as container runtime"
    export DOCKER_HOST="unix://$FINCH_SOCK"
  fi
fi

echo "[deploy.sh] sam build --use-container"
sam build --use-container

if [ $# -eq 0 ]; then
  echo "[deploy.sh] Build only (no deploy args). Run:"
  echo "  $0 --guided"
  echo "or"
  echo "  $0 --parameter-overrides AllowedClusterArns=... VpcId=... VpcSubnetIds=... ..."
  exit 0
fi

echo "[deploy.sh] sam deploy $*"
sam deploy "$@"
