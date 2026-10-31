from unittest.mock import Mock

from agentic_batch_data_reliability.orchestration.approval_cli import decide


def request():
    return {
        "incident_id": "inc-1",
        "batch_id": "batch-1",
        "approval_status": "PENDING",
        "task_token": "token",
        "recovery_plan": {"batch_id": "batch-1"},
        "workflow_context": {
            "incident_id": "inc-1",
            "batch_id": "batch-1",
            "recovery_plan": {"batch_id": "batch-1"},
        },
    }


def test_approve_sends_success_callback():
    client = Mock()
    decide(request(), approve=True, stepfunctions_client=client)
    client.send_task_success.assert_called_once()
    client.send_task_failure.assert_not_called()


def test_reject_sends_failure_callback():
    client = Mock()
    decide(request(), approve=False, stepfunctions_client=client)
    client.send_task_failure.assert_called_once()
    client.send_task_success.assert_not_called()
