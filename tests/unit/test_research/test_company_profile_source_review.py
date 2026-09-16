from __future__ import annotations

import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.live_plan import record_company_profile_live_plan
from research.company_profile.live_run import (
    load_live_run_report,
    persist_live_run_report,
    record_live_run_report,
    select_live_run_targets,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    CompanyProfileTaskControl,
    record_published_source_review,
)
from research.company_profile.source_review import (
    SOURCE_REVIEW_SCHEMA_VERSION,
    CompanyProfileSourceReviewReport,
    FixtureGuardResult,
    SemanticFinding,
    StructuralCheck,
    record_source_review_report,
    source_review_schema_manifest,
)
from tests.unit.test_research.test_company_profile_live_run import _registry


def _live_run():
    plan = record_company_profile_live_plan()
    registry = _registry()
    selected = select_live_run_targets(registry, plan)
    return record_live_run_report(
        plan=plan,
        registry=registry,
        selected_instrument_ids=selected,
        delivered_instrument_ids=("600000.SH",),
        knowledge_cutoff="2026-08-30",
        incomplete_supplement_ids=("600036.SH",),
    )


def _finding(
    *,
    instrument_id: str = "600000.SH",
    aspect: str = "core_skeleton",
    disclosure_id: str = "principal-business",
    disclosed_in_source: bool = True,
    present_in_delivery: bool = True,
    fact_accurate: bool | None = True,
    critical_numeric_error: bool = False,
) -> SemanticFinding:
    return SemanticFinding(
        instrument_id=instrument_id,
        aspect=aspect,
        kind="semantic",
        source="independently_read_official_report",
        disclosure_id=disclosure_id,
        disclosed_in_source=disclosed_in_source,
        present_in_delivery=present_in_delivery,
        fact_accurate=fact_accurate,
        critical_numeric_error=critical_numeric_error,
    )


def test_structure_only_review_leaves_semantic_metrics_unassessed():
    report = record_source_review_report(
        live_run=_live_run(),
        structural_checks=(
            StructuralCheck(
                instrument_id="600000.SH",
                aspect="core_skeleton",
                kind="structural",
                method="evidence_id_existence",
                passed=True,
            ),
        ),
        fixture_guards=(
            FixtureGuardResult(
                channel="fixture_guard",
                name="stage5_negative_guard",
                passed=True,
            ),
        ),
    )

    assert report.schema_version == SOURCE_REVIEW_SCHEMA_VERSION
    assert report.production_authorization == PRODUCTION_AUTHORIZATION
    assert report.source_recall.status == "unassessed"
    assert report.source_recall.value is None
    assert report.source_accuracy.status == "unassessed"
    assert report.critical_numeric_errors.status == "unassessed"
    assert report.critical_numeric_errors.value is None
    assert report.occupied_strata_reviewed.status == "unassessed"
    assert report.occupied_strata_reviewed.value is None
    assert report.workload.tokens_used.status == "unassessed"
    assert report.workload.human_review_minutes.value is None
    assert report.expansion_gates_met is False
    assert report.scale_quality_claim_allowed is False
    assert report.zero_recurrence_claimed is False
    assert report.delivery_coverage.semantic_status == "not_a_source_metric"
    assert report.fixture_guards[0].passed is True

    payload = json.loads(report.model_dump_json())
    payload["source_recall"] = {
        "status": "unassessed",
        "value": 0.0,
    }
    with pytest.raises(ValidationError):
        CompanyProfileSourceReviewReport.model_validate_json(json.dumps(payload))
    payload["source_recall"] = {
        "status": "assessed",
        "numerator": 0,
        "denominator": 1,
        "value": 0.0,
    }
    payload["source_accuracy"] = payload["source_recall"]
    payload["critical_numeric_errors"] = {"status": "assessed", "value": 0}
    with pytest.raises(ValidationError, match="structure-only"):
        CompanyProfileSourceReviewReport.model_validate_json(json.dumps(payload))


def test_independent_semantic_review_reports_recall_accuracy_and_workload():
    live_run = _live_run()
    report = record_source_review_report(
        live_run=live_run,
        semantic_findings=(
            _finding(aspect="core_skeleton", disclosure_id="principal-business"),
            _finding(
                aspect="important_disclosure",
                disclosure_id="net-fee-income",
            ),
            _finding(
                instrument_id="600036.SH",
                aspect="commodity_role",
                disclosure_id="no-commodity-role",
                disclosed_in_source=True,
                present_in_delivery=False,
                fact_accurate=None,
            ),
        ),
        tokens_used=1200,
        elapsed_seconds=45.0,
        human_review_minutes=30.0,
    )

    assert report.source_recall.status == "assessed"
    assert report.source_recall.numerator == 2
    assert report.source_recall.denominator == 3
    assert report.source_accuracy.status == "assessed"
    assert report.source_accuracy.numerator == 2
    assert report.source_accuracy.denominator == 2
    assert report.critical_numeric_errors.status == "assessed"
    assert report.critical_numeric_errors.value == 0
    assert report.independently_reviewed_reports == 2
    assert report.occupied_strata_reviewed.status == "unassessed"
    assert report.workload.tokens_used.value == 1200
    assert report.workload.elapsed_seconds.value == 45.0
    assert report.workload.human_review_minutes.value == 30.0
    assert report.expansion_gates_met is False
    assert {item.aspect for item in report.semantic_findings} == {
        "core_skeleton",
        "important_disclosure",
        "commodity_role",
    }

    with pytest.raises(ValidationError, match="live-run sample"):
        record_source_review_report(
            live_run=live_run,
            semantic_findings=(_finding(instrument_id="000001.SZ"),),
        )
    with pytest.raises(ValidationError, match="occupied strata"):
        record_source_review_report(
            live_run=live_run,
            occupied_strata_reviewed=1,
        )
    perfect = record_source_review_report(
        live_run=live_run,
        semantic_findings=(
            _finding(aspect="core_skeleton", disclosure_id="principal-business"),
            _finding(
                instrument_id="600036.SH",
                aspect="important_disclosure",
                disclosure_id="net-fee-income",
            ),
        ),
        occupied_strata_reviewed=2,
    )
    assert perfect.source_recall.value == 1.0
    assert perfect.source_accuracy.value == 1.0
    assert perfect.occupied_strata_reviewed.value == 2
    assert perfect.expansion_gates_met is True
    short_stratum = record_source_review_report(
        live_run=live_run,
        semantic_findings=perfect.semantic_findings,
        occupied_strata_reviewed=1,
    )
    assert short_stratum.expansion_gates_met is False


def test_published_owner_persists_source_review_beside_live_run(tmp_path):
    live_run = _live_run()
    persist_live_run_report(live_run, tmp_path / "checkpoints")
    control = CompanyProfileTaskControl(tmp_path / "checkpoints")
    control.begin(action="run", run_id="live-run-1", parameters={})
    control.finish(
        state="completed",
        result={"live_run": live_run.model_dump(mode="json")},
    )
    result = record_published_source_review(
        checkpoint_root=tmp_path / "checkpoints",
        structural_checks=(
            StructuralCheck(
                instrument_id="600000.SH",
                aspect="important_disclosure",
                kind="structural",
                method="page_reference",
                passed=True,
            ),
        ),
        semantic_findings=(
            _finding(),
            _finding(
                aspect="important_disclosure",
                disclosure_id="interest-income",
            ),
            _finding(
                aspect="commodity_role",
                disclosure_id="copper-input-absent",
                disclosed_in_source=False,
                present_in_delivery=False,
                fact_accurate=None,
            ),
        ),
        occupied_strata_reviewed=1,
        human_review_minutes=20.0,
    )
    path = tmp_path / "checkpoints" / "reports" / f"{SOURCE_REVIEW_SCHEMA_VERSION}.json"

    assert result["action"] == "source_review"
    assert result["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert result["source_review"]["source_recall"]["status"] == "assessed"
    assert result["source_review"]["source_recall"]["value"] == 1.0
    assert result["source_review"]["occupied_strata_reviewed"]["value"] == 1
    assert result["source_review"]["expansion_gates_met"] is False
    assert result["source_review"]["scale_quality_claim_allowed"] is False
    assert result["control"]["latest_result"]["source_review"]["schema_version"] == (
        SOURCE_REVIEW_SCHEMA_VERSION
    )
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8"))[
        "schema_version"
    ] == SOURCE_REVIEW_SCHEMA_VERSION
    assert load_live_run_report(tmp_path / "checkpoints").selected_instrument_ids == (
        live_run.selected_instrument_ids
    )
    with pytest.raises(ValueError, match="persisted live-run report"):
        record_published_source_review(checkpoint_root=tmp_path / "missing")


def test_source_review_module_has_no_llm_or_legacy_writer_entry():
    from research.company_profile import source_review

    source = inspect.getsource(source_review)
    assert "openai" not in source.lower()
    assert "CompanyProfileTaskService" not in source
    assert "execute_published_task" not in source
    manifest = source_review_schema_manifest()
    assert manifest["production_authorization"] == "not_authorized"
    assert manifest["unassessed_is_not_zero"] is True
    assert manifest["zero_recurrence_claimed"] is False
