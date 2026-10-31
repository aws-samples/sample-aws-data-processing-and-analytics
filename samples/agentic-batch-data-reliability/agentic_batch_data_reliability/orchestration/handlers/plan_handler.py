"""Create a deterministic recovery plan from validated workflow data."""

from __future__ import annotations

from typing import Any

from ...recovery.planner import build_recovery_plan


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    plan = build_recovery_plan(
        incident_id=event["incident_id"],
        batch_id=event["batch_id"],
        failed_rule_ids=event["failed_rule_ids"],
    )
    return {**event, "recovery_plan": plan.model_dump(mode="json")}
