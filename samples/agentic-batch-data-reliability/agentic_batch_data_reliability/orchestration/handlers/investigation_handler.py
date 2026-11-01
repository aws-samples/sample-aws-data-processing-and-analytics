"""Deployed Strands agent entry point for the Step Functions workflow."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from ...agent.investigator import investigate
from ...agent.tools.aws_evidence import AwsEvidenceToolset
from ...agent.tools.reliability_tools import ReliabilityToolset


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    import boto3

    report_bucket = os.environ["REPORT_BUCKET"]
    session = boto3.session.Session(region_name=os.environ["AWS_REGION"])
    s3_client = session.client("s3")
    report = investigate(
        incident_id=event["incident_id"],
        batch_id=event["batch_id"],
        validation_id=event["validation_id"],
        validation_report_uri=event["validation_report_uri"],
        toolset=ReliabilityToolset(s3_client, report_bucket),
        aws_evidence_toolset=AwsEvidenceToolset(
            glue_client=session.client("glue"),
            logs_client=session.client("logs"),
            athena_client=session.client("athena"),
            glue_job_name=os.environ["GLUE_JOB_NAME"],
            glue_log_group=os.environ["GLUE_LOG_GROUP"],
        ),
        model_id=os.environ["BEDROCK_MODEL_ID"],
        region=os.environ["AWS_REGION"],
        # Lambda has no uvx subprocess. The same agent runs with the verified MCP
        # providers through `abdr-investigate`; deployed tools remain read-only.
        use_mcp=False,
        max_turns=int(os.getenv("AGENT_MAX_TURNS", "12")),
        max_output_tokens=int(os.getenv("AGENT_MAX_OUTPUT_TOKENS", "4000")),
    )
    payload = report.model_dump(mode="json")
    payload["timestamps"].setdefault(
        "investigation_completed_at", datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )
    key = f"reports/incidents/incident_id={event['incident_id']}/investigation.json"
    s3_client.put_object(
        Bucket=report_bucket,
        Key=key,
        Body=(json.dumps(payload, indent=2, sort_keys=True) + "\n").encode(),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return {
        **event,
        "investigation_report_uri": f"s3://{report_bucket}/{key}",
        "investigation": payload,
    }
