"""Post-recovery completion policy."""

from __future__ import annotations

from typing import Any


def final_reliability_status(validation_report: dict[str, Any]) -> str:
    statuses = [item.get("status") for item in validation_report.get("results", [])]
    if not statuses or any(status == "ERROR" for status in statuses):
        return "ERROR"
    if all(status in {"PASS", "SKIPPED"} for status in statuses):
        return "RESOLVED"
    return "UNRESOLVED"
