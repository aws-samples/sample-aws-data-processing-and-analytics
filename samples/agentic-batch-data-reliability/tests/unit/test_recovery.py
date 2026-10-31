import pytest

from agentic_batch_data_reliability.recovery.executor import build_glue_recovery_arguments
from agentic_batch_data_reliability.recovery.planner import build_recovery_plan
from agentic_batch_data_reliability.recovery.verifier import final_reliability_status


def test_recovery_plan_is_batch_scoped_and_requires_approval():
    plan = build_recovery_plan(
        incident_id="inc-1", batch_id="batch-1", failed_rule_ids=["missing_business_keys"]
    )
    assert plan.affected_scope == {"batch_id": "batch-1"}
    assert plan.approval_required is True
    assert plan.strategy == "replace_affected_batch"


def test_recovery_execution_rejects_unapproved_plan():
    plan = build_recovery_plan(
        incident_id="inc-1", batch_id="batch-1", failed_rule_ids=["duplicate_business_keys"]
    )
    with pytest.raises(PermissionError, match="APPROVED"):
        build_glue_recovery_arguments(
            plan, approval_status="PENDING", recovery_execution_id="rec-1"
        )


def test_approved_recovery_arguments_match_glue_runtime_names():
    plan = build_recovery_plan(
        incident_id="inc-1", batch_id="batch-1", failed_rule_ids=["missing_business_keys"]
    )
    arguments = build_glue_recovery_arguments(
        plan, approval_status="APPROVED", recovery_execution_id="rec-1"
    )

    assert arguments == {
        "--batch_id": "batch-1",
        "--failure_scenario": "healthy",
        "--write_mode": "replace_batch",
        "--incident_id": "inc-1",
        "--recovery_execution_id": "rec-1",
    }


def test_unknown_rule_blocks_recovery_plan():
    with pytest.raises(ValueError, match="No automatic recovery"):
        build_recovery_plan(
            incident_id="inc-1", batch_id="batch-1", failed_rule_ids=["unknown_rule"]
        )


def test_post_recovery_status_requires_all_checks_to_pass():
    assert final_reliability_status({"results": [{"status": "PASS"}]}) == "RESOLVED"
    assert final_reliability_status({"results": [{"status": "FAIL"}]}) == "UNRESOLVED"
    assert final_reliability_status({"results": [{"status": "ERROR"}]}) == "ERROR"
