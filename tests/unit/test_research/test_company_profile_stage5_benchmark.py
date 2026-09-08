from __future__ import annotations

import ast
import importlib
import json
import socket
import sqlite3
from pathlib import Path

import pytest

import research.company_profile.stage5_benchmark as stage5_benchmark_module
from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    PreparedEvidence,
)
from research.company_profile.models import (
    AssertionClass,
    ChapterTask,
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    Evidence,
    IdentityClass,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.projection import project_research_view
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope
from research.company_profile.stage5_benchmark import (
    Stage5GoldAnnotationResult,
    Stage5NegativeCaseResult,
    _annotation_match_status,
    _evaluate_annotation,
    _evaluate_negative_case,
    _has_affirmative_subject_basis,
    _period_matches,
    _record_matches_annotation,
    _subject_match_status,
    evaluate_committed_stage5_run,
)
from research.company_profile.stage5_bundle import (
    Stage5BenchmarkResult,
    Stage5OverallStatus,
    Stage5ReportBundle,
    Stage5ReportStatus,
    Stage5RunBundle,
    Stage5RunBundleStore,
    Stage5ScopeResult,
)
from scripts import evaluate_company_profile_stage5_run as benchmark_operator

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GOLD_PATH = (
    REPOSITORY_ROOT
    / "docs/development/company_profile_manufacturing_materials_gold_annotations.v1.json"
)
STAGE5_MODULES = (
    "research.company_profile.stage5",
    "research.company_profile.stage5_bundle",
    "research.company_profile.stage5_provider",
    "research.company_profile.stage5_service",
)


def test_approved_gold_and_negative_cases_are_evaluated_only_after_commit(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-evaluation"))

    benchmark = evaluate_committed_stage5_run(
        run_path,
        gold_path=GOLD_PATH,
    )

    assert benchmark.run_id == "post-run-evaluation"
    assert benchmark.decision == "hold"
    assert len(benchmark.annotation_results) == 24
    assert len(benchmark.negative_case_results) == 19
    assert all(item.reason for item in benchmark.negative_case_results)
    assert benchmark.gold_evaluation_only is True
    assert benchmark.production_authorization == "not_authorized"
    expected_case_ids = {
        item["case_id"]
        for item in json.loads(GOLD_PATH.read_text(encoding="utf-8"))[
            "contract_negative_cases"
        ]
    }
    assert {
        item.case_id for item in benchmark.negative_case_results
    } == expected_case_ids


def test_totals_only_legal_empty_is_evaluated_from_committed_output(
    tmp_path: Path,
) -> None:
    run_path = _commit_totals_only_run(tmp_path, prohibited_relationship=False)

    benchmark = evaluate_committed_stage5_run(run_path, gold_path=GOLD_PATH)
    result = _negative_results(benchmark)["mm-neg-counterparty-coverage-backfill"]

    assert result.evaluated is True
    assert result.passed is True
    assert any(
        target.startswith("scope:") for target in result.inspected_runtime_target_ids
    )


def test_totals_only_aggregate_relationship_fails_from_committed_output(
    tmp_path: Path,
) -> None:
    run_path = _commit_totals_only_run(tmp_path, prohibited_relationship=True)

    benchmark = evaluate_committed_stage5_run(run_path, gold_path=GOLD_PATH)
    result = _negative_results(benchmark)["mm-neg-counterparty-coverage-backfill"]

    assert result.evaluated is True
    assert result.passed is False
    assert any(
        target.startswith("record:") for target in result.inspected_runtime_target_ids
    )


def test_consolidation_adjustment_row_name_is_affirmative_subject_evidence() -> None:
    record = {
        "row_class": "consolidation_adjustment",
        "subject_basis": "direct_source_wording",
        "source_native": {"name": "合并抵消项"},
        "evidence": [{"anchor": {"bounded_quote": ""}}],
    }

    assert _has_affirmative_subject_basis(record) is True


def test_totals_only_provider_failure_is_not_claimed_as_semantic_violation() -> None:
    scope = {
        "prepared_scope": {
            "page_contexts": [{"text": "前五名客户销售额占年度销售总额58.14%"}],
        },
        "task_result": {
            "records": [],
            "dispositions": [],
            "coverage": [],
            "human_review_items": [{"reason_codes": ["provider_unavailable"]}],
        },
    }
    result = _evaluate_negative_case(
        "mm-neg-counterparty-coverage-backfill",
        {"reports": [{"sample_id": "manufacturing-materials-603659-2025", "scope_results": [
            {"scope_id": "top_five_customer_totals_only", **scope}
        ]}]},
    )
    assert result.evaluated is False
    assert result.passed is False


def test_third_party_guard_does_not_reject_company_direct_sale() -> None:
    activity = {
        "record_id": "company-sale",
        "object_type": "Activity",
        "action": "sells",
        "activity_actor": "公司",
        "source_actor": "公司",
        "object_name": "航空产品",
    }
    scope = {
        "prepared_scope": {
            "page_contexts": [{"text": "军贸公司向国外最终用户销售；军用航空产品采取直销模式。"}],
        },
        "task_result": {
            "records": [activity],
            "dispositions": [{"status": "accepted_for_review", "target_id": "company-sale"}],
        },
    }
    result = _evaluate_negative_case(
        "mm-neg-third-party-action-actor",
        {"reports": [{"sample_id": "manufacturing-materials-302132-2025-regime", "scope_results": [
            {"scope_id": "business_overview", **scope}
        ]}]},
    )
    assert result.evaluated is True
    assert result.passed is True


def test_negative_cases_are_not_reported_as_passed_when_not_evaluated(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-unevaluated"))

    benchmark = evaluate_committed_stage5_run(
        run_path,
        gold_path=GOLD_PATH,
    )
    results = {item.case_id: item for item in benchmark.negative_case_results}

    assert benchmark.decision == "hold"
    missing_trigger = results["mm-neg-sales-amount-as-volume"]
    assert missing_trigger.evaluated is False
    assert missing_trigger.passed is False
    assert missing_trigger.inspected_runtime_target_ids == ()


def test_fixture_guard_failure_forces_post_run_hold(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-fixture-failure"))
    monkeypatch.setattr(
        stage5_benchmark_module,
        "_evaluate_annotation",
        lambda annotation, report: Stage5GoldAnnotationResult(
            annotation_id=annotation["annotation_id"],
            sample_id=annotation["sample_id"],
            field_id=annotation["field_id"],
            passed=True,
            match_status="exact_match",
        ),
    )
    monkeypatch.setattr(
        stage5_benchmark_module,
        "_evaluate_negative_case",
        lambda case_id, manifest: Stage5NegativeCaseResult(
            case_id=case_id,
            evaluated=True,
            passed=True,
            reason="guarded",
        ),
    )
    monkeypatch.setattr(
        stage5_benchmark_module,
        "evaluate_fixture_guards",
        lambda: (
            Stage5NegativeCaseResult(
                case_id="fixture-failed",
                evaluated=True,
                passed=False,
                reason="fixture guard did not block the invalid candidate",
                source="fixture_guard",
            ),
        ),
    )

    benchmark = stage5_benchmark_module.evaluate_committed_stage5_run(
        run_path,
        gold_path=GOLD_PATH,
    )

    assert benchmark.decision == "hold"
    assert benchmark.fixture_guard_results[0].passed is False


def test_same_year_report_end_matches_only_duration_gold_period() -> None:
    report = {"report_period": "2025-12-31"}
    duration = {
        "reported_period": "2025-12-31",
        "period_type": "duration",
        "report": report,
    }
    instant = {
        "reported_period": "2025-12-31",
        "period_type": "instant",
        "report": report,
    }
    narrower = {
        "reported_period": "2025年1-6月",
        "period_type": "duration",
        "report": report,
    }
    mid_year = {
        "reported_period": "2025-06-30",
        "period_type": "duration",
        "report": {"report_period": "2025-06-30"},
    }

    assert _period_matches(duration, "2025") is True
    assert _period_matches(instant, "2025") is False
    assert _period_matches(narrower, "2025") is False
    assert _period_matches(mid_year, "2025") is False
    assert _period_matches(instant, "2025-12-31") is True


def test_supported_non_group_subject_refinement_is_closed() -> None:
    annotation = {
        "subject_strictness": "allow_supported_non_group_refinement",
        "semantic": {"subject_scope": "unclear"},
    }
    supported_segment = {
        "subject_scope": "business_segment",
        "segment_dimension": "product",
        "segment_label": "羟胺盐",
        "evidence": [{"anchor": {"row_label": "羟胺盐"}}],
    }
    unsupported_segment = {
        **supported_segment,
        "evidence": [{"anchor": {"row_label": "其他产品"}}],
    }
    consolidated = {
        **supported_segment,
        "subject_scope": "consolidated_group",
        "subject_basis": "direct_source_wording",
    }
    unsupported_issuer = {
        "subject_scope": "issuer",
        "subject_name": "锦华新材",
        "subject_basis": "numeric_reconciliation_to_consolidated_statement",
        "evidence": [{"anchor": {"bounded_quote": "锦华新材"}}],
    }

    assert (
        _subject_match_status(supported_segment, annotation)
        == "accepted_with_uncertainty"
    )
    assert _subject_match_status(unsupported_segment, annotation) == "failed"
    assert _subject_match_status(consolidated, annotation) == "failed"
    assert _subject_match_status(unsupported_issuer, annotation) == "failed"


def test_chengfei_transfer_effective_equivalence_is_directional_and_audited() -> None:
    annotation = _chengfei_event_annotation()
    record = _chengfei_event_record()
    report = {
        "scope_results": [
            {
                "task_result": {
                    "records": [record],
                    "dispositions": [
                        {
                            "status": "accepted_for_review",
                            "target_id": record["record_id"],
                        }
                    ],
                }
            }
        ]
    }

    assert _record_matches_annotation(record, annotation) is True
    assert _annotation_match_status(record, annotation) == "semantic_match"
    result = _evaluate_annotation(annotation, report)
    assert result.passed is True
    assert result.runtime_target_id == record["record_id"]
    assert result.match_rule == "chengfei_transfer_effective_directional_equivalence"


@pytest.mark.parametrize(
    "record_update,annotation_update",
    [
        ({"event_type": "重大资产重组"}, {}),
        ({"event_date": "2023-02-01"}, {}),
        ({"regime_effective_at": "2025-01-17"}, {}),
        (
            {
                "evidence": [
                    {
                        "page": 50,
                        "anchor": {
                            "bounded_quote": "2023年，公司启动收购成飞100%股权重大资产重组项目"
                        },
                    }
                ]
            },
            {},
        ),
        (
            {"event_type": "major_asset_restructuring_effective"},
            {"semantic": {"event_type": "equity_transfer"}},
        ),
    ],
)
def test_chengfei_event_equivalence_rejects_unlisted_direction_date_or_anchor(
    record_update: dict,
    annotation_update: dict,
) -> None:
    record = {**_chengfei_event_record(), **record_update}
    annotation = _chengfei_event_annotation()
    annotation.update(annotation_update)

    assert _record_matches_annotation(record, annotation) is False


def test_post_run_benchmark_operator_writes_one_atomic_result(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-command"))

    assert (
        benchmark_operator.main(
            [
                "--run-directory",
                str(run_path),
                "--gold-path",
                str(GOLD_PATH),
            ]
        )
        == 0
    )

    destination = run_path / "post-run-benchmark.json"
    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["run_id"] == "post-run-command"
    assert len(payload["annotation_results"]) == 24
    assert len(payload["negative_case_results"]) == 19
    assert not tuple(run_path.glob(".post-run-benchmark-*.tmp"))
    with pytest.raises(FileExistsError):
        benchmark_operator.main(
            [
                "--run-directory",
                str(run_path),
                "--gold-path",
                str(GOLD_PATH),
            ]
        )


def test_offline_benchmark_operator_uses_independent_identity_and_output(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("offline-input-run"))
    manifest_path = run_path / "manifest.json"
    manifest_before = manifest_path.read_bytes()
    source_benchmark = run_path / "post-run-benchmark.json"
    source_benchmark.write_text('{"baseline":true}\n', encoding="utf-8")
    source_benchmark_before = source_benchmark.read_bytes()
    destination = tmp_path / "evaluations" / "offline-evaluation.json"

    assert (
        benchmark_operator.main(
            [
                "--run-directory",
                str(run_path),
                "--gold-path",
                str(GOLD_PATH),
                "--output-path",
                str(destination),
                "--evaluation-id",
                "period-event-semantics-20260908-a",
            ]
        )
        == 0
    )

    payload = json.loads(destination.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "company_profile_stage5_offline_evaluation.v1"
    assert payload["evaluation_id"] == "period-event-semantics-20260908-a"
    assert payload["runtime_source"]["run_id"] == "offline-input-run"
    assert len(payload["runtime_source"]["manifest_sha256"]) == 64
    assert len(payload["runtime_source"]["source_benchmark_sha256"]) == 64
    assert payload["benchmark"]["production_authorization"] == "not_authorized"
    assert manifest_path.read_bytes() == manifest_before
    assert source_benchmark.read_bytes() == source_benchmark_before


def test_stage5_runtime_modules_do_not_import_gold_adapter_or_legacy_paths() -> None:
    prohibited_modules = (
        "business_profile_deterministic_extraction",
        "business_profile_semantic_runtime",
        "effective_annual_reports",
        "data_manager",
        "scheduler.tasks",
        "telegram",
        "api.routes",
        "dcf",
    )
    for module_name in STAGE5_MODULES:
        module = importlib.import_module(module_name)
        source = Path(module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        assert "_adapt_observed_gold" not in source
        assert all(
            not any(term in imported_name for imported_name in imported)
            for term in prohibited_modules
        )


def test_stage5_import_preparation_failure_and_cleanup_have_no_network_or_database(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        socket.socket,
        "connect",
        lambda *args, **kwargs: calls.append("network"),
    )
    monkeypatch.setattr(
        sqlite3,
        "connect",
        lambda *args, **kwargs: calls.append("database"),
    )
    bundle_module = None
    for module_name in STAGE5_MODULES:
        reloaded = importlib.reload(importlib.import_module(module_name))
        if module_name == "research.company_profile.stage5_bundle":
            bundle_module = reloaded

    assert bundle_module is not None
    store = bundle_module.Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    abandoned = store.output_root / ".stage5-tmp-no-side-effects-candidate"
    abandoned.mkdir()
    (abandoned / "candidate.json").write_text("{}", encoding="utf-8")
    store.record_failure(
        "no-side-effects",
        (
            bundle_module.Stage5FailureDiagnostic(
                code="test_failure",
                message="bounded failure",
            ),
        ),
    )
    audit = store.audit_garbage(remove=True)

    assert calls == []
    assert audit.abandoned_paths == ()
    assert not abandoned.exists()


def _minimal_run_bundle(run_id: str) -> Stage5RunBundle:
    report = ReportIdentity(
        instrument_id="300750.SZ",
        report_id="asset_3b09f6c831975c7177b6bb3287cab781",
        document_version="ver_09c0e677ec8192dc4fc12cb620069f29",
        report_period="2025-12-31",
        published_at="2026-03-09T16:00:00+00:00",
    )
    evidence = Evidence(
        evidence_id="stage5-post-run-evidence",
        report=report,
        page=14,
        section_title="主要业务",
        anchor=TextAnchor(bounded_quote="公司主要从事动力电池研发、生产和销售。"),
    )
    prepared = PreparedRequestScope(
        sample_id="manufacturing-materials-300750-2025",
        scope_id="business_overview",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        field_ids=("business_overview_source",),
        report=report,
        evidence_bundle=(PreparedEvidence(evidence=evidence),),
        page_contexts=(
            PreparedPageContext(
                page=14,
                text="公司主要从事动力电池研发、生产和销售。",
                text_hash="a" * 64,
                extraction_method="pypdf",
                quality_status="usable",
            ),
        ),
        plan_version="manufacturing_materials.2026-09-04.1",
    )
    task_result = CompanyProfileTaskResult(
        request_id=f"{run_id}:business_overview",
        records=(),
        dispositions=(),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    scope_result = Stage5ScopeResult(
        scope_id=prepared.scope_id,
        request_id=task_result.request_id,
        prepared_scope=prepared,
        task_result=task_result,
    )
    report_bundle = Stage5ReportBundle(
        run_id=run_id,
        sample_id=prepared.sample_id,
        company_name="宁德时代",
        report=report,
        sample_manifest_revision="manufacturing_materials.2026-09-03.4",
        evidence_plan_version=prepared.plan_version,
        evidence_plan_hash="b" * 64,
        scope_results=(scope_result,),
        research_view=project_research_view(
            company_name="宁德时代",
            report=report,
            task_results=(task_result,),
        ),
        report_status=Stage5ReportStatus.HOLD,
        benchmark=Stage5BenchmarkResult(decision="not_evaluated"),
        created_at="2026-09-04T00:00:00+00:00",
    )
    return Stage5RunBundle(
        run_id=run_id,
        sample_manifest_revision=report_bundle.sample_manifest_revision,
        evidence_plan_version=report_bundle.evidence_plan_version,
        reports=(report_bundle,),
        overall_status=Stage5OverallStatus.HOLD,
        created_at="2026-09-04T00:00:00+00:00",
    )


def _chengfei_event_annotation() -> dict:
    return {
        "annotation_id": "mm-302132-regime-effective",
        "sample_id": "manufacturing-materials-302132-2025-regime",
        "field_id": "business_regime",
        "coverage_status": "observed",
        "subject_strictness": "must_equal",
        "source_native": {"value": "2025-01-06"},
        "semantic": {
            "object_type": "BusinessEvent",
            "event_type": "major_asset_restructuring_effective",
            "subject_scope": "unclear",
            "period": "2025",
            "regime_effective_at": "2025-01-06",
        },
        "evidence": {
            "page": 59,
            "physical_anchor": {
                "bounded_quote": "截至2025年1月6日，公司已完成股权过户并纳入公司合并报表范围"
            },
        },
    }


def _chengfei_event_record() -> dict:
    return {
        "record_id": "stage5-chengfei-transfer",
        "field_id": "business_regime",
        "object_type": "BusinessEvent",
        "event_type": "equity_transfer",
        "subject_scope": "unclear",
        "reported_period": "2025",
        "period_type": "event",
        "event_date": "2025-01-06",
        "regime_effective_at": "2025-01-06",
        "source_native": {"name": "equity_transfer"},
        "evidence": [
            {
                "page": 59,
                "anchor": {
                    "bounded_quote": (
                        "截至2025年1月6日，公司已完成股权过户并纳入公司合并报表范围"
                    )
                },
            }
        ],
    }


def _negative_results(benchmark):
    return {item.case_id: item for item in benchmark.negative_case_results}


def _commit_totals_only_run(tmp_path: Path, *, prohibited_relationship: bool) -> Path:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    run_path = store.commit(_minimal_run_bundle("post-run-totals-only"))
    manifest_path = run_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    scope = manifest["reports"][0]["scope_results"][0]
    scope["scope_id"] = "top_five_customer_totals_only"
    scope["prepared_scope"]["scope_id"] = scope["scope_id"]
    evidence = Evidence.model_validate_json(
        json.dumps(
            scope["prepared_scope"]["evidence_bundle"][0]["evidence"],
            ensure_ascii=False,
        )
    )
    coverage = CoverageResult(
        field_id="counterparty_relationship",
        chapter_task=ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
        requirement_level=RequirementLevel.CONDITIONAL,
        status=CoverageStatus.NOT_DISCLOSED,
        reason_code=CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
        evidence=(evidence,),
    )
    scope["task_result"]["coverage"] = [coverage.model_dump(mode="json")]
    if prohibited_relationship:
        record = Relationship(
            record_id="stage55-prohibited-top-five-aggregate",
            field_id="counterparty_relationship",
            chapter_task=ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            report=evidence.report,
            subject_scope=SubjectScope.UNCLEAR,
            reported_period="2025年度",
            period_type=PeriodType.DURATION,
            assertion_class=AssertionClass.REPORTED_FACT,
            evidence=(evidence,),
            source_native=SourceNativeValue(name="前五名客户合计"),
            relation_type=RelationshipType.CUSTOMER,
            object_name="前五名客户合计",
            identity_class=IdentityClass.REPORT_LOCAL_AGGREGATE,
        )
        scope["task_result"]["records"] = [record.model_dump(mode="json")]
        scope["task_result"]["dispositions"] = [
            {
                "field_id": "counterparty_relationship",
                "reason_codes": [],
                "status": "accepted_for_review",
                "target_id": record.record_id,
            }
        ]
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return run_path
