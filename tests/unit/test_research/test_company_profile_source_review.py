from __future__ import annotations

import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.candidate_registry import build_a_share_candidate_registry
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


def _live_run(registry=None):
    plan = record_company_profile_live_plan()
    registry = registry or _registry()
    selected = select_live_run_targets(registry, plan)
    return record_live_run_report(
        plan=plan,
        registry=registry,
        selected_instrument_ids=selected,
        delivered_instrument_ids=selected[:1],
        knowledge_cutoff="2026-08-30",
        incomplete_supplement_ids=selected[1:],
    )


def _two_stratum_registry():
    return build_a_share_candidate_registry(
        as_of="2026-08-30",
        universe_coverage_guarantee="full_market",
        eligible_instruments=(
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "name": "浦发银行",
            },
            {
                "instrument_id": "000001.SZ",
                "exchange": "SZSE",
                "name": "平安银行",
            },
        ),
        asset_coverage={
            "600000.SH": {"status": "available", "fiscal_year": 2025},
            "000001.SZ": {"status": "available", "fiscal_year": 2025},
        },
        industry_memberships={
            "600000.SH": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
            "000001.SZ": {"sw_l1_name": "银行", "taxonomy_system": "sw"},
        },
        effective_reports={
            "600000.SH": {
                "asset_id": "asset-600000-2025",
                "fiscal_year": 2025,
                "report_period": "2025-12-31",
                "availability": "local_valid",
                "decision_state": "effective",
                "published_at": "2026-03-20T00:00:00+08:00",
            },
            "000001.SZ": {
                "asset_id": "asset-000001-2025",
                "fiscal_year": 2025,
                "report_period": "2025-12-31",
                "availability": "local_valid",
                "decision_state": "effective",
                "published_at": "2026-03-21T00:00:00+08:00",
            },
        },
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


def _complete_findings(*instrument_ids: str) -> tuple[SemanticFinding, ...]:
    findings: list[SemanticFinding] = []
    for instrument_id in instrument_ids:
        findings.extend(
            (
                _finding(
                    instrument_id=instrument_id,
                    aspect="core_skeleton",
                    disclosure_id=f"{instrument_id}-principal-business",
                ),
                _finding(
                    instrument_id=instrument_id,
                    aspect="important_disclosure",
                    disclosure_id=f"{instrument_id}-net-fee-income",
                ),
                _finding(
                    instrument_id=instrument_id,
                    aspect="commodity_role",
                    disclosure_id=f"{instrument_id}-no-commodity-role",
                ),
            )
        )
    return tuple(findings)


def _write_legacy_live_run(live_run, root):
    payload = json.loads(live_run.model_dump_json())
    del payload["selected_strata"]
    path = root / "reports" / "company_profile_live_run.v1.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


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
    with pytest.raises(ValidationError, match="follow semantic findings"):
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
    assert report.occupied_strata_reviewed.status == "assessed"
    assert report.occupied_strata_reviewed.value == 1
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


def test_occupied_strata_follow_live_run_sample_layers():
    same_layer = record_source_review_report(
        live_run=_live_run(),
        semantic_findings=_complete_findings("600000.SH", "600036.SH"),
        tokens_used=100,
        elapsed_seconds=10.0,
    )
    two_layers = record_source_review_report(
        live_run=_live_run(_two_stratum_registry()),
        semantic_findings=_complete_findings("600000.SH", "000001.SZ"),
        tokens_used=100,
        elapsed_seconds=10.0,
    )

    assert same_layer.occupied_strata_reviewed.value == 1
    assert same_layer.expansion_gates_met is False
    assert two_layers.occupied_strata_reviewed.value == 2
    assert two_layers.expansion_gates_met is True

    payload = json.loads(same_layer.model_dump_json())
    payload["occupied_strata_reviewed"] = {"status": "assessed", "value": 2}
    payload["expansion_gates_met"] = True
    with pytest.raises(ValidationError, match="live-run sample layers"):
        CompanyProfileSourceReviewReport.model_validate_json(json.dumps(payload))


def test_legacy_v1_live_run_enters_official_source_review_without_passing_gates(
    tmp_path,
):
    live_run = _live_run(_two_stratum_registry())
    _write_legacy_live_run(live_run, tmp_path / "checkpoints")
    loaded = load_live_run_report(tmp_path / "checkpoints")
    result = record_published_source_review(
        checkpoint_root=tmp_path / "checkpoints",
        semantic_findings=_complete_findings("600000.SH", "000001.SZ"),
        tokens_used=100,
        elapsed_seconds=10.0,
    )

    assert loaded.selected_instrument_ids == live_run.selected_instrument_ids
    assert loaded.selected_strata == ()
    assert result["source_review"]["occupied_strata_reviewed"]["status"] == (
        "unassessed"
    )
    assert result["source_review"]["occupied_strata_reviewed"]["value"] is None
    assert result["source_review"]["expansion_gates_met"] is False
    payload = json.loads(json.dumps(result["source_review"]))
    payload["occupied_strata_reviewed"] = {"status": "assessed", "value": 2}
    payload["expansion_gates_met"] = True
    with pytest.raises(ValidationError, match="live-run sample layers"):
        CompanyProfileSourceReviewReport.model_validate_json(json.dumps(payload))


def test_unassessed_delivered_facts_block_source_accuracy():
    report = record_source_review_report(
        live_run=_live_run(_two_stratum_registry()),
        semantic_findings=(
            *_complete_findings("600000.SH"),
            _finding(
                instrument_id="000001.SZ",
                aspect="core_skeleton",
                disclosure_id="000001.SZ-principal-business",
            ),
            _finding(
                instrument_id="000001.SZ",
                aspect="important_disclosure",
                disclosure_id="000001.SZ-net-fee-income",
                fact_accurate=None,
            ),
            _finding(
                instrument_id="000001.SZ",
                aspect="commodity_role",
                disclosure_id="000001.SZ-no-commodity-role",
            ),
        ),
        tokens_used=100,
        elapsed_seconds=10.0,
    )

    assert report.source_accuracy.status == "unassessed"
    assert report.source_accuracy.value is None
    assert report.expansion_gates_met is False


def test_expansion_gates_require_plan_cost_caps_and_three_aspects():
    live_run = _live_run(_two_stratum_registry())
    findings = _complete_findings("600000.SH", "000001.SZ")
    over_budget = record_source_review_report(
        live_run=live_run,
        semantic_findings=findings,
        tokens_used=999999,
        elapsed_seconds=999999.0,
    )
    unassessed_cost = record_source_review_report(
        live_run=live_run,
        semantic_findings=findings,
    )
    skeleton_only = record_source_review_report(
        live_run=live_run,
        semantic_findings=(
            _finding(instrument_id="600000.SH"),
            _finding(
                instrument_id="000001.SZ",
                disclosure_id="000001.SZ-principal-business",
            ),
        ),
        tokens_used=100,
        elapsed_seconds=10.0,
    )

    assert over_budget.expansion_gates_met is False
    assert unassessed_cost.expansion_gates_met is False
    assert skeleton_only.expansion_gates_met is False


def test_public_json_cannot_contradict_semantic_findings():
    report = record_source_review_report(
        live_run=_live_run(_two_stratum_registry()),
        semantic_findings=(
            *_complete_findings("600000.SH"),
            _finding(
                instrument_id="000001.SZ",
                aspect="core_skeleton",
                disclosure_id="000001.SZ-principal-business",
            ),
            _finding(
                instrument_id="000001.SZ",
                aspect="important_disclosure",
                disclosure_id="000001.SZ-net-fee-income",
            ),
            _finding(
                instrument_id="000001.SZ",
                aspect="commodity_role",
                disclosure_id="000001.SZ-no-commodity-role",
                present_in_delivery=False,
                fact_accurate=None,
            ),
        ),
        tokens_used=100,
        elapsed_seconds=10.0,
    )
    payload = json.loads(report.model_dump_json())

    assert report.source_recall.numerator == 5
    assert report.source_recall.denominator == 6
    assert report.expansion_gates_met is False

    payload["source_recall"] = {
        "status": "assessed",
        "numerator": 2,
        "denominator": 2,
        "value": 1.0,
    }
    payload["expansion_gates_met"] = True
    with pytest.raises(ValidationError, match="follow semantic findings"):
        CompanyProfileSourceReviewReport.model_validate_json(json.dumps(payload))


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
