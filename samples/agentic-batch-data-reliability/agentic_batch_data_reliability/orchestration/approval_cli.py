"""Approve or reject one pending Step Functions recovery callback."""

from __future__ import annotations

import argparse
import json
from typing import Any


def _parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://") or "/" not in uri[5:]:
        raise ValueError("approval request must be an s3://bucket/key URI")
    bucket, key = uri[5:].split("/", 1)
    return bucket, key


def decide(request: dict[str, Any], *, approve: bool, stepfunctions_client: Any) -> None:
    if request.get("approval_status") != "PENDING":
        raise ValueError("Approval request is not pending")
    token = request["task_token"]
    if approve:
        output = {**request["workflow_context"], "approval_status": "APPROVED"}
        stepfunctions_client.send_task_success(taskToken=token, output=json.dumps(output))
    else:
        stepfunctions_client.send_task_failure(
            taskToken=token,
            error="RecoveryRejected",
            cause="A human reviewer rejected the proposed batch recovery.",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request_uri")
    decision = parser.add_mutually_exclusive_group(required=True)
    decision.add_argument("--approve", action="store_true")
    decision.add_argument("--reject", action="store_true")
    parser.add_argument("--region")
    args = parser.parse_args()

    import boto3

    session = boto3.session.Session(region_name=args.region)
    bucket, key = _parse_uri(args.request_uri)
    response = session.client("s3").get_object(Bucket=bucket, Key=key)
    request = json.loads(response["Body"].read(1_000_001))
    print(json.dumps({k: v for k, v in request.items() if k != "task_token"}, indent=2))
    decide(request, approve=args.approve, stepfunctions_client=session.client("stepfunctions"))
    print("Decision submitted.")


if __name__ == "__main__":
    main()
