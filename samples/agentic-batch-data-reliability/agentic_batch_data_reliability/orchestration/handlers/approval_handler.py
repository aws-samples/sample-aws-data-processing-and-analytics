"""Persist an encrypted approval request and return while Step Functions waits."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    import boto3

    workflow_context = event["workflow_context"]
    request = {
        "incident_id": workflow_context["incident_id"],
        "batch_id": workflow_context["batch_id"],
        "approval_status": "PENDING",
        "recovery_plan": workflow_context["recovery_plan"],
        "workflow_context": workflow_context,
        "task_token": event["task_token"],
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "warning": (
            "The task token authorizes one workflow callback; restrict access to this object."
        ),
    }
    key = f"reports/approvals/incident_id={workflow_context['incident_id']}/request.json"
    boto3.client("s3").put_object(
        Bucket=os.environ["REPORT_BUCKET"],
        Key=key,
        Body=(json.dumps(request, indent=2, sort_keys=True) + "\n").encode(),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return {"approval_request_uri": f"s3://{os.environ['REPORT_BUCKET']}/{key}"}
