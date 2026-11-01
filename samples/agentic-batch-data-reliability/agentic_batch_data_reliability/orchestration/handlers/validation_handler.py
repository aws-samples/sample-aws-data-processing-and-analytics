"""Lambda handler that runs deterministic Athena reconciliation after Glue succeeds."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any

from ...reliability.reconciliation.athena import (
    AthenaQueryRunner,
    TableReference,
    build_reconciliation_queries,
)
from ...reliability.schemas import Severity, ValidationReport, ValidationResult, ValidationStatus
from ...reliability.validator import persist_validation_report


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256(":".join(parts).encode()).hexdigest()[:20]
    return f"{prefix}-{digest}"


def _integer(rows: list[list[str | None]]) -> int:
    if not rows or not rows[0] or rows[0][0] is None:
        raise ValueError("Athena count query returned no value")
    return int(rows[0][0])


def _manifest(s3_client: Any, uri: str) -> dict[str, Any]:
    if not uri.startswith("s3://") or "/" not in uri[5:]:
        raise ValueError("manifest_uri must be an s3://bucket/key URI")
    bucket, key = uri[5:].split("/", 1)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    if response.get("ContentLength", 0) > 1_000_000:
        raise ValueError("Manifest is unexpectedly large")
    return json.loads(response["Body"].read(1_000_001))


def _claim_incident(
    s3_client: Any, *, bucket: str, incident_id: str, batch_id: str, validation_id: str
) -> bool:
    """Atomically claim a stable incident; return false for a duplicate delivery."""

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=f"reports/incidents/incident_id={incident_id}/claim.json",
            Body=json.dumps(
                {
                    "incident_id": incident_id,
                    "batch_id": batch_id,
                    "validation_id": validation_id,
                },
                sort_keys=True,
            ).encode(),
            ContentType="application/json",
            ServerSideEncryption="AES256",
            IfNoneMatch="*",
        )
        return True
    except Exception as error:
        response = getattr(error, "response", {})
        error_code = response.get("Error", {}).get("Code")
        if error_code in {"PreconditionFailed", "ConditionalRequestConflict", "412", "409"}:
            return False
        raise


def _job_context(event: dict[str, Any], glue_client: Any) -> dict[str, str]:
    if "batch_id" in event and "pipeline_run_id" in event:
        return {
            "batch_id": str(event["batch_id"]),
            "pipeline_run_id": str(event["pipeline_run_id"]),
            "manifest_uri": str(event["manifest_uri"]),
        }
    detail = event.get("detail", event)
    job_name = detail.get("jobName") or detail.get("job_name")
    job_run_id = detail.get("jobRunId") or detail.get("job_run_id")
    if not job_name or not job_run_id:
        raise ValueError("Expected Glue jobName and jobRunId in workflow input")
    run = glue_client.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)[
        "JobRun"
    ]
    arguments = run.get("Arguments", {})
    return {
        "batch_id": arguments["--batch_id"],
        "pipeline_run_id": job_run_id,
        "manifest_uri": arguments["--manifest_uri"],
    }


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    import boto3

    region = os.environ["AWS_REGION"]
    session = boto3.session.Session(region_name=region)
    s3_client = session.client("s3")
    glue_client = session.client("glue")
    context = _job_context(event, glue_client)
    batch_id = context["batch_id"]
    pipeline_run_id = context["pipeline_run_id"]
    validation_kind = str(event.get("validation_kind", "initial"))
    validation_id = _stable_id("val", pipeline_run_id, validation_kind)
    manifest = _manifest(s3_client, context["manifest_uri"])

    source = TableReference("AwsDataCatalog", os.environ["RAW_DATABASE"], "orders")
    target_catalog = f"s3tablescatalog/{os.environ['TABLE_BUCKET_NAME']}"
    target = TableReference(target_catalog, os.environ["TABLE_NAMESPACE"], "orders")
    queries = build_reconciliation_queries(source=source, target=target, batch_id=batch_id)
    runner = AthenaQueryRunner(
        session.client("athena"),
        workgroup=os.environ["ATHENA_WORKGROUP"],
        output_location=f"s3://{os.environ['REPORT_BUCKET']}/athena-results/",
        timeout_seconds=int(os.getenv("ATHENA_TIMEOUT_SECONDS", "120")),
    )

    outputs: dict[str, Any] = {}
    query_ids: dict[str, str] = {}
    try:
        for name, sql in queries.items():
            query = runner.run(sql, catalog="AwsDataCatalog", database=os.environ["RAW_DATABASE"])
            outputs[name] = query.rows
            query_ids[name] = query.query_execution_id
    except (RuntimeError, TimeoutError, ValueError) as error:
        error_result = ValidationResult(
            validation_id=validation_id,
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            source_table=source.sql(),
            target_table=target.sql(),
            rule_id="athena_execution",
            rule_name="Athena reconciliation queries execute successfully",
            expected_value="SUCCEEDED",
            actual_value="ERROR",
            status=ValidationStatus.ERROR,
            severity=Severity.HIGH,
            evidence={"error_type": type(error).__name__, "message": str(error)[:1000]},
        )
        report = ValidationReport(
            validation_id=validation_id,
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            status=ValidationStatus.ERROR,
            results=[error_result],
        )
        uri = persist_validation_report(
            report, s3_client=s3_client, bucket=os.environ["REPORT_BUCKET"]
        )
        return {
            **context,
            "validation_id": validation_id,
            "validation_status": "ERROR",
            "validation_report_uri": uri,
            "failed_rule_ids": ["athena_execution"],
        }

    source_count = _integer(outputs["source_row_count"])
    target_count = _integer(outputs["target_row_count"])
    source_keys = _integer(outputs["source_distinct_keys"])
    target_keys = _integer(outputs["target_distinct_keys"])
    missing_count = _integer(outputs["missing_business_key_count"])
    unexpected_count = _integer(outputs["unexpected_business_key_count"])
    duplicate_count = _integer(outputs["duplicate_business_key_count"])
    null_count = _integer(outputs["required_field_nullability"])

    def rule(
        rule_id: str,
        name: str,
        expected: Any,
        actual: Any,
        *,
        severity: Severity,
        query_name: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> ValidationResult:
        passed = expected == actual
        return ValidationResult(
            validation_id=validation_id,
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            source_table=source.sql(),
            target_table=target.sql(),
            rule_id=rule_id,
            rule_name=name,
            expected_value=expected,
            actual_value=actual,
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            severity=Severity.INFO if passed else severity,
            evidence=evidence or {},
            query_execution_id=query_ids.get(query_name or ""),
        )

    expected_fields = set(manifest.get("required_fields", []))
    required_fields = {
        "order_id",
        "customer_id",
        "order_timestamp",
        "order_amount",
        "status",
        "batch_id",
    }
    generated_at = datetime.fromisoformat(manifest["generation_timestamp"].replace("Z", "+00:00"))
    age_minutes = int((datetime.now(UTC) - generated_at).total_seconds() // 60)
    freshness_sla = int(os.getenv("FRESHNESS_SLA_MINUTES", "1440"))
    results = [
        rule(
            "manifest_row_count",
            "Source rows match the independent manifest",
            int(manifest["expected_row_count"]),
            source_count,
            severity=Severity.CRITICAL,
            query_name="source_row_count",
        ),
        rule(
            "row_count_match",
            "Source and target row counts match",
            source_count,
            target_count,
            severity=Severity.HIGH,
            query_name="target_row_count",
        ),
        rule(
            "business_key_count_match",
            "Distinct source and target key counts match",
            source_keys,
            target_keys,
            severity=Severity.HIGH,
            query_name="target_distinct_keys",
        ),
        rule(
            "missing_business_keys",
            "No source keys are missing from the target",
            0,
            missing_count,
            severity=Severity.CRITICAL,
            query_name="missing_business_key_count",
            evidence={"sample_keys": [row[0] for row in outputs["missing_business_key_sample"]]},
        ),
        rule(
            "unexpected_business_keys",
            "No unexpected target keys exist",
            0,
            unexpected_count,
            severity=Severity.HIGH,
            query_name="unexpected_business_key_count",
            evidence={"sample_keys": [row[0] for row in outputs["unexpected_business_key_sample"]]},
        ),
        rule(
            "duplicate_business_keys",
            "Target keys are unique within the batch",
            0,
            duplicate_count,
            severity=Severity.CRITICAL,
            query_name="duplicate_business_key_count",
            evidence={"sample_rows": outputs["duplicate_business_key_sample"]},
        ),
        rule(
            "required_field_nullability",
            "Required target fields contain no nulls",
            0,
            null_count,
            severity=Severity.HIGH,
            query_name="required_field_nullability",
        ),
        rule(
            "schema_compatibility",
            "Manifest required fields match the supported schema",
            sorted(required_fields),
            sorted(expected_fields),
            severity=Severity.HIGH,
        ),
        rule(
            "batch_freshness",
            "Batch arrived within its freshness SLA",
            True,
            age_minutes <= freshness_sla,
            severity=Severity.HIGH,
            evidence={"age_minutes": age_minutes, "sla_minutes": freshness_sla},
        ),
    ]
    status = "PASS" if all(item.status is ValidationStatus.PASS for item in results) else "FAIL"
    report = ValidationReport(
        validation_id=validation_id,
        batch_id=batch_id,
        pipeline_run_id=pipeline_run_id,
        status=ValidationStatus(status),
        results=results,
        source_count=source_count,
        target_count=target_count,
        missing_key_count=missing_count,
        unexpected_key_count=unexpected_count,
        duplicate_key_count=duplicate_count,
    )
    uri = persist_validation_report(report, s3_client=s3_client, bucket=os.environ["REPORT_BUCKET"])
    incident_id = _stable_id("inc", batch_id, validation_id) if status == "FAIL" else None
    duplicate_event = False
    if incident_id and validation_kind == "initial":
        duplicate_event = not _claim_incident(
            s3_client,
            bucket=os.environ["REPORT_BUCKET"],
            incident_id=incident_id,
            batch_id=batch_id,
            validation_id=validation_id,
        )
    return {
        **context,
        "validation_id": validation_id,
        "validation_status": "DUPLICATE" if duplicate_event else status,
        "validation_report_uri": uri,
        "failed_rule_ids": report.failed_rule_ids,
        "incident_id": incident_id,
        "duplicate_event": duplicate_event,
    }
