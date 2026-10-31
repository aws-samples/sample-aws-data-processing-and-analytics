from unittest.mock import MagicMock

import pytest

from agentic_batch_data_reliability.reliability.reconciliation.athena import (
    AthenaQueryRunner,
    TableReference,
    build_reconciliation_queries,
    quote_identifier,
    quote_literal,
)


def test_query_runner_respects_athena_max_results_limit():
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": "query-1"}
    client.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": "SUCCEEDED"},
            "Statistics": {"DataScannedInBytes": 42},
        }
    }
    client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "value"}]},
                {"Data": [{"VarCharValue": "1"}]},
            ]
        }
    }

    result = AthenaQueryRunner(
        client,
        workgroup="test-workgroup",
        output_location="s3://test-results/",
        poll_seconds=0,
    ).run("SELECT 1", catalog="AwsDataCatalog", database="default")

    assert result.rows == [["1"]]
    client.get_query_results.assert_called_once_with(
        QueryExecutionId="query-1", MaxResults=1000
    )


def test_queries_are_batch_scoped_and_key_level():
    queries = build_reconciliation_queries(
        source=TableReference("AwsDataCatalog", "abdr_dev_raw", "orders"),
        target=TableReference("s3tablescatalog/abdr-dev-curated", "order_processing", "orders"),
        batch_id="batch-001",
    )
    assert all("batch-001" in query for query in queries.values())
    assert "LEFT JOIN" in queries["missing_business_key_count"]
    assert "HAVING count(*) > 1" in queries["duplicate_business_key_sample"]


def test_literal_escapes_quotes():
    assert quote_literal("batch'oops") == "'batch''oops'"


@pytest.mark.parametrize("value", ["orders; DROP TABLE x", "BadName", "has-hyphen", ""])
def test_unsafe_identifier_is_rejected(value):
    with pytest.raises(ValueError):
        quote_identifier(value)


def test_control_characters_in_literal_are_rejected():
    with pytest.raises(ValueError):
        quote_literal("batch\nmalicious")
