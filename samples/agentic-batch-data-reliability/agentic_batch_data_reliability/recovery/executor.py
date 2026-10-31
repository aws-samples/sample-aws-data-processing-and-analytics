"""Recovery argument construction; the agent never calls this module directly."""

from __future__ import annotations

from typing import Any

from .planner import RecoveryPlan


def build_glue_recovery_arguments(
    plan: RecoveryPlan, *, approval_status: str, recovery_execution_id: str
) -> dict[str, str]:
    if approval_status != "APPROVED":
        raise PermissionError("Recovery requires explicit APPROVED status")
    if plan.affected_scope != {"batch_id": plan.batch_id}:
        raise ValueError("Recovery scope must contain exactly one batch_id")
    return {
        "--batch_id": plan.batch_id,
        "--failure_scenario": "healthy",
        "--write_mode": "replace_batch",
        "--incident_id": plan.incident_id,
        "--recovery_execution_id": recovery_execution_id,
    }


def validate_recovery_event(event: dict[str, Any]) -> RecoveryPlan:
    plan = RecoveryPlan.model_validate(event["recovery_plan"])
    if event.get("batch_id") != plan.batch_id:
        raise ValueError("Workflow batch_id does not match approved recovery plan")
    if event.get("incident_id") != plan.incident_id:
        raise ValueError("Workflow incident_id does not match approved recovery plan")
    return plan
