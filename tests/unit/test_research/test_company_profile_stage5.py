from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    PreparedEvidence,
)
from research.company_profile.models import (
    ChapterTask,
    Evidence,
    ReportIdentity,
    TextAnchor,
)
from research.company_profile.projection import project_research_view
from research.company_profile.stage5 import (
    APPROVED_STAGE5_SAMPLES,
    EvidencePreparationError,
    PreparationFailureCode,
    PreparedPageContext,
    PreparedRequestScope,
    Stage5EvidencePreparer,
    load_stage5_evidence_plan,
    load_stage5_sample_manifest,
)
from research.company_profile.stage5_bundle import (
    Stage5BenchmarkResult,
    Stage5FailureDiagnostic,
    Stage5OverallStatus,
    Stage5ReportBundle,
    Stage5ReportStatus,
    Stage5RunBundle,
    Stage5RunBundleStore,
    Stage5ScopeResult,
)
from research.document_processing.pdf import PdfRouter, PypdfNativeAdapter

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SAMPLE_MANIFEST = (
    REPOSITORY_ROOT
    / "docs/development/company_profile_manufacturing_materials_sample_manifest.v1.json"
)
EVIDENCE_PLAN = (
    REPOSITORY_ROOT
    / "research/company_profile/evidence_plans/manufacturing_materials.v1.json"
)


def test_stage5_manifest_is_a_verified_four_report_closed_set() -> None:
    manifest = load_stage5_sample_manifest(
        SAMPLE_MANIFEST,
        repository_root=REPOSITORY_ROOT,
    )

    assert {item.sample_id for item in manifest.reports} == set(
        APPROVED_STAGE5_SAMPLES
    )
    assert {item.report.instrument_id for item in manifest.reports} == {
        "300750.SZ",
        "603659.SH",
        "920015.BJ",
        "302132.SZ",
    }
    assert all(item.local_path.is_file() for item in manifest.reports)
    assert all(
        item.production_authorization == "not_authorized"
        for item in manifest.reports
    )


def test_stage5_manifest_rejects_an_unapproved_sample(tmp_path: Path) -> None:
    payload = json.loads(SAMPLE_MANIFEST.read_text(encoding="utf-8"))
    payload["reports"][0]["sample_id"] = "manufacturing-materials-000001-2025"
    candidate = tmp_path / "manifest.json"
    candidate.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(EvidencePreparationError) as exc_info:
        load_stage5_sample_manifest(candidate, repository_root=REPOSITORY_ROOT)

    assert exc_info.value.code == PreparationFailureCode.MANIFEST_INVALID


def test_stage5_manifest_rejects_hash_mismatch(tmp_path: Path) -> None:
    payload = json.loads(SAMPLE_MANIFEST.read_text(encoding="utf-8"))
    payload["reports"][0]["report_identity"]["content_hash"] = "0" * 64
    candidate = tmp_path / "manifest.json"
    candidate.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(EvidencePreparationError) as exc_info:
        load_stage5_sample_manifest(candidate, repository_root=REPOSITORY_ROOT)

    assert exc_info.value.code == PreparationFailureCode.HASH_MISMATCH


def test_stage5_evidence_plan_has_six_tasks_per_report_and_no_gold_answers() -> None:
    plan = load_stage5_evidence_plan(EVIDENCE_PLAN)

    assert {item.sample_id for item in plan.reports} == set(APPROVED_STAGE5_SAMPLES)
    assert all({task.chapter_task for task in item.tasks} == set(ChapterTask) for item in plan.reports)
    assert all(
        all(
            tuple(sorted(set(scope.pages))) == scope.pages
            for task in item.tasks
            for scope in task.request_scopes
        )
        for item in plan.reports
    )


def test_stage5_evidence_plan_rejects_runtime_semantic_defaults(
    tmp_path: Path,
) -> None:
    payload = json.loads(EVIDENCE_PLAN.read_text(encoding="utf-8"))
    payload["reports"][0]["tasks"][0]["request_scopes"][0][
        "subject_scope"
    ] = "consolidated_group"
    candidate = tmp_path / "plan.json"
    candidate.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(EvidencePreparationError) as exc_info:
        load_stage5_evidence_plan(candidate)

    assert exc_info.value.code == PreparationFailureCode.PLAN_INVALID
    assert "subject_scope" in str(exc_info.value)


def test_stage5_preparation_reads_all_planned_pages_without_provider_calls() -> None:
    manifest = load_stage5_sample_manifest(
        SAMPLE_MANIFEST,
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_stage5_evidence_plan(EVIDENCE_PLAN)
    preparer = Stage5EvidencePreparer(PdfRouter(native=PypdfNativeAdapter()))

    prepared = {
        sample_id: preparer.prepare_report(
            manifest=manifest,
            evidence_plan=plan,
            sample_id=sample_id,
        )
        for sample_id in APPROVED_STAGE5_SAMPLES
    }

    assert {sample_id: len(scopes) for sample_id, scopes in prepared.items()} == {
        "manufacturing-materials-300750-2025": 9,
        "manufacturing-materials-603659-2025": 10,
        "manufacturing-materials-920015-2025": 12,
        "manufacturing-materials-302132-2025-regime": 12,
    }
    assert all(
        scope.production_authorization == "not_authorized"
        and scope.evidence_bundle
        and scope.page_contexts
        for scopes in prepared.values()
        for scope in scopes
    )
    assert all(
        evidence.evidence.report == scope.report
        for scopes in prepared.values()
        for scope in scopes
        for evidence in scope.evidence_bundle
    )


def test_stage5_evidence_plans_cover_the_frozen_cross_report_contract() -> None:
    plan = load_stage5_evidence_plan(EVIDENCE_PLAN)
    scopes = {
        report.sample_id: {
            scope.scope_id: scope
            for task in report.tasks
            for scope in task.request_scopes
        }
        for report in plan.reports
    }

    adjustment = scopes["manufacturing-materials-603659-2025"][
        "segment_product_and_adjustment"
    ]
    assert "合并抵消项" in adjustment.anchor_terms
    assert set(adjustment.required_headers) == {"营业收入", "营业成本", "毛利率"}

    processing = scopes["manufacturing-materials-603659-2025"][
        "capacity_and_processing_narrative"
    ]
    assert "processing_volume" in processing.field_ids
    assert "涂覆加工量" in processing.anchor_terms
    inventory = scopes["manufacturing-materials-603659-2025"]["product_volume_table"]
    assert inventory.required_footnotes == (
        "包含已发出至客户但尚未确认收入的发出商品",
    )

    capacity = scopes["manufacturing-materials-920015-2025"]["capacity_table"]
    assert capacity.pages == (49, 50)
    assert set(capacity.required_headers) == {"设计产能", "产能利用率", "在建产能"}
    absent_quantities = scopes["manufacturing-materials-302132-2025-regime"][
        "classified_volume_not_available"
    ]
    assert {
        "production_capacity",
        "production_volume",
        "sales_volume",
        "inventory_volume",
        "processing_volume",
    }.issubset(absent_quantities.field_ids)
    assert "无法进行分类统计" in absent_quantities.anchor_terms

    named_rows = scopes["manufacturing-materials-920015-2025"][
        "customer_ranking_rows"
    ]
    totals_only = scopes["manufacturing-materials-603659-2025"][
        "top_five_customer_totals_only"
    ]
    contract = scopes["manufacturing-materials-300750-2025"][
        "major_sales_contract"
    ]
    assert named_rows.scope_id != totals_only.scope_id != contract.scope_id
    assert contract.required_footnotes == ("基于双方保密协议约定",)

    comparison = scopes["manufacturing-materials-302132-2025-regime"][
        "same_control_comparison_basis"
    ]
    effective = scopes["manufacturing-materials-302132-2025-regime"][
        "equity_transfer_effective"
    ]
    assert {"同一控制下企业合并", "调整前", "调整后"}.issubset(
        comparison.anchor_terms
    )
    assert effective.pages == (58, 59)
    assert effective.printed_page_labels == {"58": "57", "59": "58"}
    assert plan.page_coordinate_system == "one_based_pdf_physical_page"


def test_stage5_preparation_reports_missing_header_before_provider(
    tmp_path: Path,
) -> None:
    payload = json.loads(EVIDENCE_PLAN.read_text(encoding="utf-8"))
    payload["reports"][0]["tasks"][1]["request_scopes"][0][
        "required_headers"
    ].append("不存在的表头")
    candidate = tmp_path / "plan.json"
    candidate.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    manifest = load_stage5_sample_manifest(
        SAMPLE_MANIFEST,
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_stage5_evidence_plan(candidate)

    with pytest.raises(EvidencePreparationError) as exc_info:
        Stage5EvidencePreparer(PdfRouter(native=PypdfNativeAdapter())).prepare_report(
            manifest=manifest,
            evidence_plan=plan,
            sample_id="manufacturing-materials-300750-2025",
        )

    assert exc_info.value.code == PreparationFailureCode.HEADER_MISSING
    assert exc_info.value.scope_id == "segment_product_industry_region"


def test_stage5_bundle_store_atomically_commits_and_never_overwrites(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    bundle = _minimal_run_bundle("bundle-1")

    destination = store.commit(bundle)

    assert destination == tmp_path / "isolated" / "run-bundle-1"
    assert (destination / "manifest.json").is_file()
    assert (destination / "reports" / f"{bundle.reports[0].sample_id}.json").is_file()
    assert not list((tmp_path / "isolated").glob(".stage5-tmp-*"))
    with pytest.raises(FileExistsError):
        store.commit(bundle)


def test_stage5_failure_cleanup_retains_only_bounded_non_reusable_manifest(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    abandoned = store.output_root / ".stage5-tmp-failed-1-candidate"
    abandoned.mkdir()
    (abandoned / "candidate.json").write_text("{}", encoding="utf-8")

    failure_path = store.record_failure(
        "failed-1",
        (
            Stage5FailureDiagnostic(
                code="provider_unavailable",
                message="gateway unavailable",
                sample_id="manufacturing-materials-300750-2025",
            ),
        ),
    )

    assert not abandoned.exists()
    payload = json.loads(failure_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["reusable"] is False
    assert {path.name for path in store.output_root.iterdir()} == {
        "run-failed-1.failed.json"
    }


def test_stage5_garbage_audit_removes_only_abandoned_temp_paths(
    tmp_path: Path,
) -> None:
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    committed = store.commit(_minimal_run_bundle("retained-1"))
    abandoned = store.output_root / ".stage5-tmp-abandoned"
    abandoned.mkdir()

    audit = store.audit_garbage(remove=True)

    assert audit.abandoned_paths == (abandoned,)
    assert audit.removed_paths == (abandoned,)
    assert committed in audit.retained_bundle_paths
    assert committed.is_dir()


def test_stage5_bundle_store_rejects_production_data_root() -> None:
    with pytest.raises(ValueError, match="production data"):
        Stage5RunBundleStore(
            REPOSITORY_ROOT / "data/company_profile_stage5",
            repository_root=REPOSITORY_ROOT,
        )


def _minimal_run_bundle(run_id: str) -> Stage5RunBundle:
    report = ReportIdentity(
        instrument_id="300750.SZ",
        report_id="asset_3b09f6c831975c7177b6bb3287cab781",
        document_version="ver_09c0e677ec8192dc4fc12cb620069f29",
        report_period="2025-12-31",
        published_at="2026-03-09T16:00:00+00:00",
    )
    evidence = Evidence(
        evidence_id="stage5-test-evidence",
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
        provider_calls=(),
    )
    scope_result = Stage5ScopeResult(
        scope_id=prepared.scope_id,
        request_id=task_result.request_id,
        prepared_scope=prepared,
        task_result=task_result,
        provider_call_types=(),
    )
    view = project_research_view(
        company_name="宁德时代",
        report=report,
        task_results=(task_result,),
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
        research_view=view,
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
        retained_bundle_ids=(),
        created_at="2026-09-04T00:00:00+00:00",
    )
