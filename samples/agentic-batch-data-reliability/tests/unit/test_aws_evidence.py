from unittest.mock import Mock, patch

import pytest

from agentic_batch_data_reliability.agent.tools.aws_evidence import AwsEvidenceToolset


def toolset(*, logs_client=None):
    return AwsEvidenceToolset(
        glue_client=Mock(),
        logs_client=logs_client or Mock(),
        athena_client=Mock(),
        glue_job_name="abdr-dev-orders-etl",
        glue_log_group="/aws-glue/jobs/logs-v2",
    )


def test_logs_query_timeout_is_bounded_and_cancelled():
    logs = Mock()
    logs.start_query.return_value = {"queryId": "query-1"}
    logs.get_query_results.return_value = {"status": "Running", "results": []}

    with patch("agentic_batch_data_reliability.agent.tools.aws_evidence.time.sleep") as sleep:
        with pytest.raises(TimeoutError, match="exceeded 30 seconds"):
            toolset(logs_client=logs).get_glue_job_logs("jr_123")

    assert logs.get_query_results.call_count == 30
    assert sleep.call_count == 30
    logs.stop_query.assert_called_once_with(queryId="query-1")


def test_logs_query_terminal_failure_is_not_treated_as_evidence():
    logs = Mock()
    logs.start_query.return_value = {"queryId": "query-2"}
    logs.get_query_results.return_value = {"status": "Failed", "results": []}

    with pytest.raises(RuntimeError, match="query Failed"):
        toolset(logs_client=logs).get_glue_job_logs("jr_456")


def test_athena_evidence_excludes_query_text_and_result_rows():
    tools = toolset()
    tools.athena_client.get_query_execution.return_value = {
        "QueryExecution": {
            "Query": "SELECT secret FROM sensitive_table",
            "Status": {"State": "SUCCEEDED"},
            "Statistics": {"DataScannedInBytes": 1024},
            "WorkGroup": "abdr-dev-reconciliation",
            "StatementType": "DML",
        }
    }

    result = tools.get_athena_query_execution("query_123")

    assert "Query" not in result
    assert result["data_scanned_bytes"] == 1024


@pytest.mark.parametrize("unsafe_id", ["", "id/with/slash", "id.*", "x" * 256])
def test_evidence_identifiers_are_allowlisted(unsafe_id):
    with pytest.raises(ValueError):
        toolset().get_glue_job_run(unsafe_id)
