from datetime import UTC, datetime

import pytest

from agentic_batch_data_reliability.data_generator.failure_injector import (
    Scenario,
    inject_failure,
)
from agentic_batch_data_reliability.data_generator.generate_orders import (
    build_manifest,
    generate_orders,
)


def test_generation_is_deterministic_for_seed_and_batch():
    first = generate_orders(batch_id="batch-test", row_count=25, seed=7)
    second = generate_orders(batch_id="batch-test", row_count=25, seed=7)
    assert first == second
    assert len({row["order_id"] for row in first}) == 25


def test_generation_rejects_negative_rows():
    with pytest.raises(ValueError, match="non-negative"):
        generate_orders(batch_id="batch-test", row_count=-1)


@pytest.mark.parametrize(
    ("scenario", "count_relation"),
    [
        (Scenario.HEALTHY, "same"),
        (Scenario.MISSING_RECORDS, "less"),
        (Scenario.DUPLICATE_RECORDS, "more"),
    ],
)
def test_failure_injection_is_deterministic(scenario, count_relation):
    expected = generate_orders(batch_id="batch-test", row_count=100, seed=42)
    first, metadata = inject_failure(expected, scenario, seed=42)
    second, _ = inject_failure(expected, scenario, seed=42)
    assert first == second
    if count_relation == "same":
        assert len(first) == len(expected)
    elif count_relation == "less":
        assert len(first) < len(expected)
    else:
        assert len(first) > len(expected)
    assert metadata["scenario_id"] == scenario.value


def test_schema_drift_removes_required_field_without_mutating_source():
    expected = generate_orders(batch_id="batch-test", row_count=2)
    actual, _ = inject_failure(expected, Scenario.SCHEMA_DRIFT)
    assert "order_amount" in expected[0]
    assert "order_amount" not in actual[0]
    assert "amount" in actual[0]


def test_manifest_remains_independent_of_injected_records():
    expected = generate_orders(batch_id="batch-test", row_count=100)
    actual, _ = inject_failure(expected, Scenario.MISSING_RECORDS)
    manifest = build_manifest(
        expected,
        batch_id="batch-test",
        seed=42,
        scenario=Scenario.MISSING_RECORDS,
        input_file="orders.jsonl",
        generated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert manifest["expected_row_count"] == 100
    assert manifest["expected_row_count"] != len(actual)
    assert manifest["scenario_id"] == "missing_records"
