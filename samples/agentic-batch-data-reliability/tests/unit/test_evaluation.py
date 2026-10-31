from agentic_batch_data_reliability.agent.evaluation import EvaluationCase, evaluate_report
from agentic_batch_data_reliability.agent.report import InvestigationReport


def report_payload():
    return {
        "incident_id": "inc-1",
        "batch_id": "batch-1",
        "severity": "HIGH",
        "affected_dataset": "orders",
        "validation_findings": ["Ten source keys are missing from the target."],
        "observed_facts": ["Glue completed and reconciliation found ten missing keys."],
        "root_cause_hypotheses": [
            {
                "hypothesis": "A transformation filter excluded the records.",
                "confidence": "HIGH",
                "supporting_evidence": ["reconciliation", "glue-run"],
                "contradicting_evidence": [],
            }
        ],
        "supporting_evidence": [
            {"source": "reconciliation", "reference": "val-1", "summary": "Ten missing."},
            {"source": "glue", "reference": "jr-1", "summary": "Run succeeded."},
        ],
        "investigation_limitations": [],
        "recommended_recovery": {
            "strategy": "replace_affected_batch",
            "affected_scope": {"batch_id": "batch-1"},
        },
        "approval_status": "PENDING",
        "recovery_status": "NOT_STARTED",
        "verification_status": "NOT_STARTED",
        "timestamps": {},
    }


def test_grounded_report_passes_evaluation_rubric():
    case = EvaluationCase(
        case_id="missing",
        scenario="missing_records",
        batch_id="batch-1",
        expected_hypothesis_terms=["filter"],
        required_evidence_sources=["reconciliation", "glue"],
        expected_recovery_strategy="replace_affected_batch",
    )

    result = evaluate_report(case, InvestigationReport.model_validate(report_payload()))

    assert result.passed is True


def test_out_of_scope_recovery_fails_evaluation_rubric():
    payload = report_payload()
    payload["recommended_recovery"]["affected_scope"] = {"batch_id": "another-batch"}
    case = EvaluationCase(
        case_id="scope",
        scenario="missing_records",
        batch_id="batch-1",
        expected_hypothesis_terms=["filter"],
        required_evidence_sources=["reconciliation"],
        expected_recovery_strategy="replace_affected_batch",
    )

    result = evaluate_report(case, InvestigationReport.model_validate(payload))

    assert result.passed is False
    assert result.checks["recovery_is_batch_scoped"] is False
