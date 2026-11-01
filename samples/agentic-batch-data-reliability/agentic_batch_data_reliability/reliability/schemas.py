"""Stable machine-readable contracts for validation and incident handling."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class ValidationStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


class Severity(StrEnum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class ValidationResult:
    validation_id: str
    batch_id: str
    pipeline_run_id: str
    source_table: str
    target_table: str
    rule_id: str
    rule_name: str
    expected_value: Any
    actual_value: Any
    status: ValidationStatus
    severity: Severity
    evidence: dict[str, Any] = field(default_factory=dict)
    query_execution_id: str | None = None
    validation_timestamp: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat().replace("+00:00", "Z")
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationReport:
    validation_id: str
    batch_id: str
    pipeline_run_id: str
    status: ValidationStatus
    results: list[ValidationResult]
    source_count: int | None = None
    target_count: int | None = None
    missing_key_count: int | None = None
    unexpected_key_count: int | None = None
    duplicate_key_count: int | None = None

    @property
    def failed_rule_ids(self) -> list[str]:
        return [result.rule_id for result in self.results if result.status is ValidationStatus.FAIL]

    def to_dict(self) -> dict[str, Any]:
        return {
            "validation_id": self.validation_id,
            "batch_id": self.batch_id,
            "pipeline_run_id": self.pipeline_run_id,
            "status": self.status,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "missing_key_count": self.missing_key_count,
            "unexpected_key_count": self.unexpected_key_count,
            "duplicate_key_count": self.duplicate_key_count,
            "failed_rule_ids": self.failed_rule_ids,
            "results": [result.to_dict() for result in self.results],
        }
