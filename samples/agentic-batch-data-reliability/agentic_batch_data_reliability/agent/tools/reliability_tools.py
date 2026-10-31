"""Bounded, structured custom tools for reliability-specific evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from ...recovery.planner import build_recovery_plan as deterministic_recovery_plan
from ...recovery.verifier import final_reliability_status


class S3Client(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass
class ReliabilityToolset:
    s3_client: S3Client
    report_bucket: str
    max_report_bytes: int = 1_000_000

    def _load_json(self, key: str) -> dict[str, Any]:
        if not key.startswith("reports/") or ".." in key:
            raise ValueError("Only bounded report keys under reports/ are allowed")
        response = self.s3_client.get_object(Bucket=self.report_bucket, Key=key)
        declared_length = response.get("ContentLength", 0)
        if declared_length and declared_length > self.max_report_bytes:
            raise ValueError("Report exceeds the configured tool response limit")
        payload = response["Body"].read(self.max_report_bytes + 1)
        if len(payload) > self.max_report_bytes:
            raise ValueError("Report exceeds the configured tool response limit")
        return json.loads(payload)

    def get_batch_reconciliation(self, batch_id: str, validation_id: str) -> dict[str, Any]:
        key = f"reports/validations/batch_id={batch_id}/validation_id={validation_id}.json"
        report = self._load_json(key)
        return {
            "batch_id": batch_id,
            "validation_id": validation_id,
            "pipeline_run_id": report.get("pipeline_run_id"),
            "source_count": report.get("source_count"),
            "target_count": report.get("target_count"),
            "missing_key_count": report.get("missing_key_count"),
            "unexpected_key_count": report.get("unexpected_key_count"),
            "duplicate_key_count": report.get("duplicate_key_count"),
            "failed_rules": report.get("failed_rule_ids", []),
            "evidence_references": [f"s3://{self.report_bucket}/{key}"],
        }

    def analyze_batch_impact(self, batch_id: str, affected_dataset: str) -> dict[str, Any]:
        if affected_dataset != "orders":
            return {
                "batch_id": batch_id,
                "confirmed_affected_rows": None,
                "potentially_affected_datasets": [],
                "severity": "UNKNOWN",
                "limitations": ["No configured lineage metadata exists for this dataset."],
            }
        return {
            "batch_id": batch_id,
            "confirmed_affected_rows": None,
            "potentially_affected_datasets": [],
            "severity": "UNKNOWN",
            "limitations": [
                "This sample configures no downstream datasets; impact is limited to "
                "confirmed evidence."
            ],
        }

    def build_recovery_plan(
        self, batch_id: str, incident_id: str, failed_rule_ids: list[str]
    ) -> dict[str, Any]:
        return deterministic_recovery_plan(
            batch_id=batch_id, incident_id=incident_id, failed_rule_ids=failed_rule_ids
        ).model_dump(mode="json")

    def verify_batch_recovery(
        self, batch_id: str, recovery_execution_id: str, validation_id: str
    ) -> dict[str, Any]:
        key = f"reports/validations/batch_id={batch_id}/validation_id={validation_id}.json"
        report = self._load_json(key)
        return {
            "batch_id": batch_id,
            "recovery_execution_id": recovery_execution_id,
            "reconciliation_status": report.get("status"),
            "failed_checks": report.get("failed_rule_ids", []),
            "corrected_row_count": report.get("target_count"),
            "remaining_inconsistencies": len(report.get("failed_rule_ids", [])),
            "final_reliability_status": final_reliability_status(report),
            "evidence_references": [f"s3://{self.report_bucket}/{key}"],
        }
