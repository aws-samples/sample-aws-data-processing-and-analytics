"""Validation orchestration and durable report helpers."""

from __future__ import annotations

import json
from typing import Any, Protocol

from .schemas import ValidationReport


class ObjectStore(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...


def persist_validation_report(
    report: ValidationReport,
    *,
    s3_client: ObjectStore,
    bucket: str,
    prefix: str = "reports/validations",
) -> str:
    key = f"{prefix}/batch_id={report.batch_id}/validation_id={report.validation_id}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n").encode(),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return f"s3://{bucket}/{key}"
