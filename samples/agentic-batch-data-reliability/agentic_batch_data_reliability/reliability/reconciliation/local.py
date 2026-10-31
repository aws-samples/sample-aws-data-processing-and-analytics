"""Pure-Python reconciliation used for fast tests and local demonstrations."""

from __future__ import annotations

from collections import Counter
from typing import Any

from ..schemas import Severity, ValidationReport, ValidationResult, ValidationStatus

REQUIRED_FIELDS = (
    "order_id",
    "customer_id",
    "order_timestamp",
    "order_amount",
    "status",
    "batch_id",
)


def reconcile_records(
    source_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
    *,
    batch_id: str,
    validation_id: str,
    pipeline_run_id: str,
    source_table: str = "local.source_orders",
    target_table: str = "local.curated_orders",
) -> ValidationReport:
    """Compare counts, exact key sets, duplicates, and required-field nulls."""

    source = [row for row in source_records if row.get("batch_id") == batch_id]
    target = [row for row in target_records if row.get("batch_id") == batch_id]
    source_keys = {str(row.get("order_id")) for row in source if row.get("order_id") is not None}
    target_keys = {str(row.get("order_id")) for row in target if row.get("order_id") is not None}
    target_key_counts = Counter(str(row.get("order_id")) for row in target)
    missing_keys = sorted(source_keys - target_keys)
    unexpected_keys = sorted(target_keys - source_keys)
    duplicate_keys = sorted(key for key, count in target_key_counts.items() if count > 1)
    null_violations = sum(
        1 for row in target for field in REQUIRED_FIELDS if row.get(field) is None
    )

    def result(
        rule_id: str,
        rule_name: str,
        expected: Any,
        actual: Any,
        passed: bool,
        severity: Severity,
        evidence: dict[str, Any] | None = None,
    ) -> ValidationResult:
        return ValidationResult(
            validation_id=validation_id,
            batch_id=batch_id,
            pipeline_run_id=pipeline_run_id,
            source_table=source_table,
            target_table=target_table,
            rule_id=rule_id,
            rule_name=rule_name,
            expected_value=expected,
            actual_value=actual,
            status=ValidationStatus.PASS if passed else ValidationStatus.FAIL,
            severity=Severity.INFO if passed else severity,
            evidence=evidence or {},
        )

    results = [
        result(
            "row_count_match",
            "Source and target row counts match",
            len(source),
            len(target),
            len(source) == len(target),
            Severity.HIGH,
        ),
        result(
            "business_key_count_match",
            "Distinct business-key counts match",
            len(source_keys),
            len(target_keys),
            len(source_keys) == len(target_keys),
            Severity.HIGH,
        ),
        result(
            "missing_business_keys",
            "No source business keys are missing from the target",
            0,
            len(missing_keys),
            not missing_keys,
            Severity.CRITICAL,
            {"sample_keys": missing_keys[:100], "sample_truncated": len(missing_keys) > 100},
        ),
        result(
            "unexpected_business_keys",
            "No unexpected business keys exist in the target",
            0,
            len(unexpected_keys),
            not unexpected_keys,
            Severity.HIGH,
            {"sample_keys": unexpected_keys[:100], "sample_truncated": len(unexpected_keys) > 100},
        ),
        result(
            "duplicate_business_keys",
            "Target business keys are unique within the batch",
            0,
            len(duplicate_keys),
            not duplicate_keys,
            Severity.CRITICAL,
            {"sample_keys": duplicate_keys[:100], "sample_truncated": len(duplicate_keys) > 100},
        ),
        result(
            "required_field_nullability",
            "Required target fields contain no nulls",
            0,
            null_violations,
            null_violations == 0,
            Severity.HIGH,
        ),
    ]
    status = (
        ValidationStatus.PASS
        if all(item.status is ValidationStatus.PASS for item in results)
        else ValidationStatus.FAIL
    )
    return ValidationReport(
        validation_id=validation_id,
        batch_id=batch_id,
        pipeline_run_id=pipeline_run_id,
        status=status,
        results=results,
        source_count=len(source),
        target_count=len(target),
        missing_key_count=len(missing_keys),
        unexpected_key_count=len(unexpected_keys),
        duplicate_key_count=len(duplicate_keys),
    )
