"""Regression checks for AWS Glue built-in runtime arguments."""

from __future__ import annotations

import ast
from pathlib import Path

GLUE_SCRIPT = Path(__file__).parents[2] / "glue" / "orders_etl.py"


def test_job_run_id_is_not_registered_with_get_resolved_options() -> None:
    """Glue already registers JOB_RUN_ID, so requesting it again breaks argparse."""

    tree = ast.parse(GLUE_SCRIPT.read_text(encoding="utf-8"))
    required_arguments: list[str] | None = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "REQUIRED_ARGUMENTS"
            for target in node.targets
        ):
            required_arguments = ast.literal_eval(node.value)
            break

    assert required_arguments is not None
    assert "JOB_RUN_ID" not in required_arguments
    assert "--JOB_RUN_ID" in GLUE_SCRIPT.read_text(encoding="utf-8")
