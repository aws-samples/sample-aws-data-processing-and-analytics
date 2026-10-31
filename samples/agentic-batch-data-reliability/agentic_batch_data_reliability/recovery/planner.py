"""Deterministic, batch-scoped recovery planning."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, field_validator


class RecoveryPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident_id: str
    batch_id: str
    strategy: str
    affected_scope: dict[str, Any]
    expected_changes: list[str]
    preconditions: list[str]
    risks: list[str]
    verification_steps: list[str]
    approval_required: bool = True

    @field_validator("batch_id", "incident_id")
    @classmethod
    def identifier_is_safe(cls, value: str) -> str:
        if not value or len(value) > 128 or any(ch.isspace() for ch in value):
            raise ValueError("identifier must be non-empty, bounded, and contain no whitespace")
        return value


def build_recovery_plan(
    *, incident_id: str, batch_id: str, failed_rule_ids: list[str]
) -> RecoveryPlan:
    """Select a safe strategy from deterministic validation findings."""

    supported = {
        "row_count_match",
        "business_key_count_match",
        "missing_business_keys",
        "unexpected_business_keys",
        "duplicate_business_keys",
        "required_field_nullability",
        "schema_compatibility",
    }
    unknown = sorted(set(failed_rule_ids) - supported)
    if unknown:
        raise ValueError(f"No automatic recovery strategy for rules: {unknown}")

    strategy = "replace_affected_batch"
    risks = [
        "A concurrent writer could update the same batch before the Iceberg commit.",
        "Source defects will be reproduced unless pre-recovery schema validation passes.",
    ]
    if "schema_compatibility" in failed_rule_ids:
        risks.insert(
            0, "Recovery is blocked until the source schema is corrected or explicitly mapped."
        )

    return RecoveryPlan(
        incident_id=incident_id,
        batch_id=batch_id,
        strategy=strategy,
        affected_scope={"batch_id": batch_id},
        expected_changes=[
            "Delete only target rows whose batch_id matches the approved batch.",
            "Insert corrected rows for the same batch in one Iceberg transaction.",
        ],
        preconditions=[
            "Human approval status is APPROVED for this incident and plan.",
            "The approved batch_id matches the workflow batch_id.",
            "The source manifest and required schema are valid.",
            "No recovery for this incident is already RUNNING or SUCCEEDED.",
        ],
        risks=risks,
        verification_steps=[
            "Run all deterministic reconciliation rules for the recovered batch.",
            "Require every critical rule to return PASS.",
            "Keep the incident open if any rule returns FAIL or ERROR.",
        ],
    )
