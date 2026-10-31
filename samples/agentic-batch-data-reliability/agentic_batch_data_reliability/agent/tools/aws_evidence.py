"""Bounded read-only AWS evidence adapter for runtimes that cannot host stdio MCP."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

SAFE_ID = re.compile(r"^[A-Za-z0-9_-]{1,255}$")


@dataclass
class AwsEvidenceToolset:
    glue_client: Any
    logs_client: Any
    athena_client: Any
    glue_job_name: str
    glue_log_group: str

    def get_glue_job_run(self, job_run_id: str) -> dict[str, Any]:
        if not SAFE_ID.fullmatch(job_run_id):
            raise ValueError("Invalid Glue job run ID")
        run = self.glue_client.get_job_run(
            JobName=self.glue_job_name,
            RunId=job_run_id,
            PredecessorsIncluded=False,
        )["JobRun"]
        allowed_arguments = {
            key: value
            for key, value in run.get("Arguments", {}).items()
            if key in {"--batch_id", "--failure_scenario", "--write_mode"}
        }
        return {
            "job_name": self.glue_job_name,
            "job_run_id": run.get("Id"),
            "state": run.get("JobRunState"),
            "started_on": str(run.get("StartedOn")),
            "completed_on": str(run.get("CompletedOn")),
            "execution_time_seconds": run.get("ExecutionTime"),
            "glue_version": run.get("GlueVersion"),
            "arguments": allowed_arguments,
            "error_message": str(run.get("ErrorMessage", ""))[:1000],
        }

    def get_glue_job_logs(self, job_run_id: str, lookback_minutes: int = 120) -> dict[str, Any]:
        if not SAFE_ID.fullmatch(job_run_id):
            raise ValueError("Invalid Glue job run ID")
        if not 1 <= lookback_minutes <= 1440:
            raise ValueError("lookback_minutes must be between 1 and 1440")
        end = datetime.now(UTC)
        start = end - timedelta(minutes=lookback_minutes)
        query = self.logs_client.start_query(
            logGroupName=self.glue_log_group,
            startTime=int(start.timestamp()),
            endTime=int(end.timestamp()),
            queryString=(
                "fields @timestamp, @message "
                f"| filter @message like /{job_run_id}/ "
                "| sort @timestamp desc | limit 100"
            ),
            limit=100,
        )
        query_id = query["queryId"]
        for _ in range(30):
            response = self.logs_client.get_query_results(queryId=query_id)
            if response["status"] == "Complete":
                rows = [
                    {item["field"]: item["value"][:2000] for item in row}
                    for row in response.get("results", [])
                ]
                return {"query_id": query_id, "rows": rows, "truncated": len(rows) == 100}
            if response["status"] in {"Failed", "Cancelled", "Timeout"}:
                raise RuntimeError(f"CloudWatch Logs Insights query {response['status']}")
            time.sleep(1)
        self.logs_client.stop_query(queryId=query_id)
        raise TimeoutError("CloudWatch Logs Insights query exceeded 30 seconds")

    def get_athena_query_execution(self, query_execution_id: str) -> dict[str, Any]:
        if not SAFE_ID.fullmatch(query_execution_id):
            raise ValueError("Invalid Athena query execution ID")
        query = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)[
            "QueryExecution"
        ]
        return {
            "query_execution_id": query_execution_id,
            "state": query["Status"]["State"],
            "state_change_reason": query["Status"].get("StateChangeReason", "")[:1000],
            "data_scanned_bytes": query.get("Statistics", {}).get("DataScannedInBytes", 0),
            "workgroup": query.get("WorkGroup"),
            "statement_type": query.get("StatementType"),
        }
