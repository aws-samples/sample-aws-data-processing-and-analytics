from copy import deepcopy

from agentic_batch_data_reliability.data_generator.generate_orders import generate_orders
from agentic_batch_data_reliability.reliability.reconciliation.local import reconcile_records


def reconcile(source, target):
    return reconcile_records(
        source,
        target,
        batch_id="batch-test",
        validation_id="val-test",
        pipeline_run_id="run-test",
    )


def test_healthy_batch_passes():
    source = generate_orders(batch_id="batch-test", row_count=10)
    report = reconcile(source, deepcopy(source))
    assert report.status == "PASS"
    assert not report.failed_rule_ids


def test_missing_key_fails_even_when_counts_match():
    source = generate_orders(batch_id="batch-test", row_count=10)
    target = deepcopy(source[1:])
    replacement = deepcopy(source[-1])
    replacement["order_id"] = "ord-unexpected"
    target.append(replacement)
    report = reconcile(source, target)
    assert report.source_count == report.target_count
    assert "missing_business_keys" in report.failed_rule_ids
    assert "unexpected_business_keys" in report.failed_rule_ids


def test_duplicate_business_key_is_detected():
    source = generate_orders(batch_id="batch-test", row_count=10)
    target = deepcopy(source) + [deepcopy(source[0])]
    report = reconcile(source, target)
    assert report.duplicate_key_count == 1
    assert "duplicate_business_keys" in report.failed_rule_ids


def test_null_violation_is_detected():
    source = generate_orders(batch_id="batch-test", row_count=2)
    target = deepcopy(source)
    target[0]["customer_id"] = None
    report = reconcile(source, target)
    assert "required_field_nullability" in report.failed_rule_ids
