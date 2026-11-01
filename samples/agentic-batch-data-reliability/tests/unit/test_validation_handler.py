from unittest.mock import Mock

from botocore.exceptions import ClientError

from agentic_batch_data_reliability.orchestration.handlers.validation_handler import (
    _claim_incident,
)


def test_incident_claim_is_created_with_conditional_write():
    s3 = Mock()

    claimed = _claim_incident(
        s3,
        bucket="report-bucket",
        incident_id="inc-1",
        batch_id="batch-1",
        validation_id="val-1",
    )

    assert claimed is True
    assert s3.put_object.call_args.kwargs["IfNoneMatch"] == "*"


def test_duplicate_incident_claim_is_ignored():
    s3 = Mock()
    s3.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed", "Message": "already exists"}},
        "PutObject",
    )

    claimed = _claim_incident(
        s3,
        bucket="report-bucket",
        incident_id="inc-1",
        batch_id="batch-1",
        validation_id="val-1",
    )

    assert claimed is False
