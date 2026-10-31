"""Validate an approved plan and construct Glue recovery arguments."""

from __future__ import annotations

import hashlib
from typing import Any

from ...recovery.executor import build_glue_recovery_arguments, validate_recovery_event


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    plan = validate_recovery_event(event)
    recovery_id = (
        "rec-" + hashlib.sha256(f"{plan.incident_id}:{plan.batch_id}".encode()).hexdigest()[:20]
    )
    arguments = build_glue_recovery_arguments(
        plan,
        approval_status=event["approval_status"],
        recovery_execution_id=recovery_id,
    )
    return {**event, "recovery_execution_id": recovery_id, "glue_arguments": arguments}
