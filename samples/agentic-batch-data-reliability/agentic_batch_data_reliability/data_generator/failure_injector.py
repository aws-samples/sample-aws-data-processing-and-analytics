"""Deterministic, independently testable failure injection for order batches."""

from __future__ import annotations

import hashlib
from copy import deepcopy
from enum import StrEnum
from typing import Any


class Scenario(StrEnum):
    HEALTHY = "healthy"
    MISSING_RECORDS = "missing_records"
    DUPLICATE_RECORDS = "duplicate_records"
    SCHEMA_DRIFT = "schema_drift"
    STALE_BATCH = "stale_batch"


def _score(order_id: str, seed: int) -> int:
    value = f"{seed}:{order_id}".encode()
    return int(hashlib.sha256(value).hexdigest()[:16], 16)


def inject_failure(
    records: list[dict[str, Any]],
    scenario: Scenario | str,
    *,
    seed: int = 42,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return a modified copy plus injection metadata; never alter the manifest."""

    selected = Scenario(scenario)
    output = deepcopy(records)
    metadata: dict[str, Any] = {
        "scenario_id": selected.value,
        "input_record_count": len(records),
        "affected_order_ids": [],
    }

    if selected is Scenario.HEALTHY:
        pass
    elif selected is Scenario.MISSING_RECORDS:
        affected = [r for r in output if _score(str(r["order_id"]), seed) % 10 == 0]
        if output and not affected:
            affected = [min(output, key=lambda row: _score(str(row["order_id"]), seed))]
        affected_ids = {str(r["order_id"]) for r in affected}
        output = [r for r in output if str(r["order_id"]) not in affected_ids]
        metadata["affected_order_ids"] = sorted(affected_ids)
    elif selected is Scenario.DUPLICATE_RECORDS:
        count = max(1, len(output) // 20) if output else 0
        affected = sorted(output, key=lambda row: _score(str(row["order_id"]), seed))[:count]
        output.extend(deepcopy(affected))
        metadata["affected_order_ids"] = sorted(str(r["order_id"]) for r in affected)
    elif selected is Scenario.SCHEMA_DRIFT:
        for record in output:
            record["amount"] = record.pop("order_amount")
        metadata["removed_fields"] = ["order_amount"]
        metadata["added_fields"] = ["amount"]
        metadata["affected_order_ids"] = sorted(str(r["order_id"]) for r in output)
    elif selected is Scenario.STALE_BATCH:
        output = []
        metadata["affected_order_ids"] = sorted(str(r["order_id"]) for r in records)

    metadata["output_record_count"] = len(output)
    return output, metadata
