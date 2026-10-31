"""Bounded Athena SQL plans and execution helpers for batch reconciliation."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Protocol

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,254}$")


class AthenaClient(Protocol):
    def start_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_query_execution(self, **kwargs: Any) -> dict[str, Any]: ...

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]: ...


def quote_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def quote_catalog(value: str) -> str:
    if not value or '"' in value or any(ord(ch) < 32 for ch in value):
        raise ValueError("Unsafe Athena catalog name")
    return f'"{value}"'


def quote_literal(value: str) -> str:
    if not value or len(value) > 256 or any(ord(ch) < 32 for ch in value):
        raise ValueError("Unsafe SQL literal")
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class TableReference:
    catalog: str
    database: str
    table: str

    def sql(self) -> str:
        return ".".join(
            (
                quote_catalog(self.catalog),
                quote_identifier(self.database),
                quote_identifier(self.table),
            )
        )


def build_reconciliation_queries(
    *, source: TableReference, target: TableReference, batch_id: str, sample_limit: int = 100
) -> dict[str, str]:
    if not 1 <= sample_limit <= 1000:
        raise ValueError("sample_limit must be between 1 and 1000")
    batch = quote_literal(batch_id)
    source_sql = source.sql()
    target_sql = target.sql()
    scope = f"batch_id = {batch}"
    return {
        "source_row_count": f"SELECT count(*) AS value FROM {source_sql} WHERE {scope}",
        "target_row_count": f"SELECT count(*) AS value FROM {target_sql} WHERE {scope}",
        "source_distinct_keys": (
            f"SELECT count(DISTINCT order_id) AS value FROM {source_sql} WHERE {scope}"
        ),
        "target_distinct_keys": (
            f"SELECT count(DISTINCT order_id) AS value FROM {target_sql} WHERE {scope}"
        ),
        "missing_business_key_count": (
            f"SELECT count(*) AS value FROM {source_sql} s LEFT JOIN {target_sql} t "
            f"ON s.batch_id = t.batch_id AND s.order_id = t.order_id "
            f"WHERE s.batch_id = {batch} AND t.order_id IS NULL"
        ),
        "missing_business_key_sample": (
            f"SELECT s.order_id FROM {source_sql} s LEFT JOIN {target_sql} t "
            f"ON s.batch_id = t.batch_id AND s.order_id = t.order_id "
            f"WHERE s.batch_id = {batch} AND t.order_id IS NULL ORDER BY s.order_id "
            f"LIMIT {sample_limit}"
        ),
        "unexpected_business_key_count": (
            f"SELECT count(*) AS value FROM {target_sql} t LEFT JOIN {source_sql} s "
            f"ON s.batch_id = t.batch_id AND s.order_id = t.order_id "
            f"WHERE t.batch_id = {batch} AND s.order_id IS NULL"
        ),
        "unexpected_business_key_sample": (
            f"SELECT t.order_id FROM {target_sql} t LEFT JOIN {source_sql} s "
            f"ON s.batch_id = t.batch_id AND s.order_id = t.order_id "
            f"WHERE t.batch_id = {batch} AND s.order_id IS NULL ORDER BY t.order_id "
            f"LIMIT {sample_limit}"
        ),
        "duplicate_business_key_count": (
            f"SELECT count(*) AS value FROM (SELECT order_id FROM {target_sql} WHERE {scope} "
            "GROUP BY order_id HAVING count(*) > 1)"
        ),
        "duplicate_business_key_sample": (
            f"SELECT order_id, count(*) AS duplicate_count FROM {target_sql} WHERE {scope} "
            f"GROUP BY order_id HAVING count(*) > 1 ORDER BY order_id LIMIT {sample_limit}"
        ),
        "required_field_nullability": (
            f"SELECT count(*) AS value FROM {target_sql} WHERE {scope} AND "
            "(order_id IS NULL OR customer_id IS NULL OR order_timestamp IS NULL OR "
            "order_amount IS NULL OR status IS NULL OR batch_id IS NULL)"
        ),
    }


@dataclass(frozen=True)
class AthenaQueryResult:
    query_execution_id: str
    rows: list[list[str | None]]
    data_scanned_bytes: int


class AthenaQueryRunner:
    """Execute one bounded query and distinguish query ERROR from a rule FAIL."""

    def __init__(
        self,
        client: AthenaClient,
        *,
        workgroup: str,
        output_location: str,
        timeout_seconds: int = 120,
        poll_seconds: float = 1.0,
    ) -> None:
        self.client = client
        self.workgroup = workgroup
        self.output_location = output_location
        self.timeout_seconds = timeout_seconds
        self.poll_seconds = poll_seconds

    def run(self, sql: str, *, catalog: str, database: str) -> AthenaQueryResult:
        started = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Catalog": catalog, "Database": database},
            ResultConfiguration={"OutputLocation": self.output_location},
            WorkGroup=self.workgroup,
        )
        query_id = started["QueryExecutionId"]
        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            execution = self.client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
            status = execution["Status"]["State"]
            if status == "SUCCEEDED":
                response = self.client.get_query_results(QueryExecutionId=query_id, MaxResults=1000)
                raw_rows = response["ResultSet"]["Rows"][1:]
                rows = [
                    [column.get("VarCharValue") for column in row.get("Data", [])]
                    for row in raw_rows
                ]
                scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
                return AthenaQueryResult(query_id, rows, scanned)
            if status in {"FAILED", "CANCELLED"}:
                reason = execution["Status"].get("StateChangeReason", "No reason returned")
                raise RuntimeError(f"Athena query {query_id} {status}: {reason}")
            time.sleep(self.poll_seconds)
        raise TimeoutError(f"Athena query {query_id} exceeded {self.timeout_seconds} seconds")
