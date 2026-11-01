"""Small deterministic rubric for scoring structured investigation reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from .report import InvestigationReport


class EvaluationCase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    case_id: str
    scenario: str
    batch_id: str
    expected_hypothesis_terms: list[str]
    required_evidence_sources: list[str]
    expected_recovery_strategy: str
    insufficient_evidence_expected: bool = False


class EvaluationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    case_id: str
    passed: bool
    checks: dict[str, bool]
    notes: list[str]


def evaluate_report(case: EvaluationCase, report: InvestigationReport) -> EvaluationResult:
    """Score observable report properties without evaluating hidden model reasoning."""

    hypothesis_text = " ".join(
        hypothesis.hypothesis.lower() for hypothesis in report.root_cause_hypotheses
    )
    sources = {evidence.source.lower() for evidence in report.supporting_evidence}
    recovery = report.recommended_recovery
    affected_scope = recovery.get("affected_scope", {})
    recovery_batch_id = recovery.get("batch_id") or affected_scope.get("batch_id")
    checks = {
        "batch_scope": report.batch_id == case.batch_id,
        "facts_separated_from_hypotheses": bool(report.observed_facts)
        and bool(report.root_cause_hypotheses),
        "expected_failure_identified": all(
            term.lower() in hypothesis_text for term in case.expected_hypothesis_terms
        ),
        "required_evidence_present": all(
            any(required.lower() in source for source in sources)
            for required in case.required_evidence_sources
        ),
        "recovery_is_batch_scoped": recovery_batch_id == case.batch_id,
        "recovery_strategy": recovery.get("strategy") == case.expected_recovery_strategy,
        "approval_remains_pending": report.approval_status == "PENDING",
    }
    if case.insufficient_evidence_expected:
        checks["unknown_handling"] = bool(report.investigation_limitations) and not any(
            hypothesis.confidence == "HIGH" for hypothesis in report.root_cause_hypotheses
        )
    notes = [name for name, passed in checks.items() if not passed]
    return EvaluationResult(
        case_id=case.case_id,
        passed=all(checks.values()),
        checks=checks,
        notes=[f"Failed check: {name}" for name in notes],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    case = EvaluationCase.model_validate_json(args.case.read_text(encoding="utf-8"))
    report = InvestigationReport.model_validate_json(args.report.read_text(encoding="utf-8"))
    result = evaluate_report(case, report)
    print(json.dumps(result.model_dump(mode="json"), indent=2, sort_keys=True))
    raise SystemExit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
