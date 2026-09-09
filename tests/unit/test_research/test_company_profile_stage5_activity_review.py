from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
    HumanReviewItem,
    PreparedEvidence,
)
from research.company_profile.models import (
    Activity,
    ActivityAction,
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    Evidence,
    PeriodType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.projection import project_research_view
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope
from research.company_profile.stage5_bundle import (
    Stage5ActivityReviewDecision,
    Stage5BenchmarkResult,
    Stage5OfflineActivityReviewRequest,
    Stage5OverallStatus,
    Stage5ReportBundle,
    Stage5ReportStatus,
    Stage5RunBundle,
    Stage5RunBundleStore,
    Stage5ScopeResult,
)
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
)
from scripts import apply_company_profile_stage5_activity_reviews as review_operator

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SOURCE_TEXT = (
    "公司主营范围包括制造、加工、销售钢铁冶金产品、金属制品、焦炭、"
    "煤化工产品（危险化学品除外）、技术开发、转让、引进及咨询服务。"
)


def test_offline_activity_review_closes_hold_without_promoting_subject(
    tmp_path: Path,
) -> None:
    run_path, request = _committed_held_activity_run(tmp_path)
    before_manifest = _sha256(run_path / "manifest.json")
    before_report = _sha256(run_path / "reports/manufacturing-materials-oos-000717-2025.json")

    result = ManufacturingMaterialsProfileSliceService().apply_committed_activity_reviews(
        run_directory=run_path,
        review_request=request,
    )

    assert result.original_report_status == Stage5ReportStatus.HOLD
    assert result.derived_report_status == Stage5ReportStatus.USABLE_WITH_CAVEATS
    assert result.accepted_record_count == 2
    assert result.remaining_human_review_count == 0
    assert result.provider_calls == 0
    assert result.production_authorization == "not_authorized"
    assert len(result.report.research_view.activities) == 1
    activity = next(
        record
        for scope in result.report.scope_results
        for record in scope.task_result.records
        if isinstance(record, Activity)
    )
    assert activity.actor_basis == SubjectBasis.DIRECT_GRAMMATICAL_ACTOR
    assert activity.subject_scope == SubjectScope.UNCLEAR
    overview = next(
        scope for scope in result.report.scope_results if scope.scope_id == "business_overview"
    )
    assert overview.task_result.task_complete is True
    assert any(
        item.field_id == "explicit_activity" and item.status == CoverageStatus.OBSERVED
        for item in overview.task_result.coverage
    )
    assert _sha256(run_path / "manifest.json") == before_manifest
    assert _sha256(run_path / "reports/manufacturing-materials-oos-000717-2025.json") == before_report


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("manifest_hash", "manifest hash mismatch"),
        ("evidence", "Evidence mismatch"),
        ("actor", "actor mismatch"),
        ("target", "targets are missing"),
        ("source_text", "source text mismatch"),
    ],
)
def test_offline_activity_review_fails_closed_for_mismatched_decision(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    run_path, request = _committed_held_activity_run(tmp_path)
    decision = request.decisions[0]
    if mutation == "manifest_hash":
        request = request.model_copy(update={"source_manifest_sha256": "0" * 64})
    elif mutation == "evidence":
        request = request.model_copy(
            update={"decisions": (decision.model_copy(update={"evidence_id": "other"}),)}
        )
    elif mutation == "actor":
        request = request.model_copy(
            update={
                "decisions": (
                    decision.model_copy(
                        update={"activity_actor": "上市公司", "source_actor": "上市公司"}
                    ),
                )
            }
        )
    elif mutation == "target":
        request = request.model_copy(
            update={"decisions": (decision.model_copy(update={"review_id": "missing"}),)}
        )
    else:
        request = request.model_copy(
            update={"decisions": (decision.model_copy(update={"source_text": "不存在"}),)}
        )

    with pytest.raises(ValueError, match=message):
        ManufacturingMaterialsProfileSliceService().apply_committed_activity_reviews(
            run_directory=run_path,
            review_request=request,
        )


def test_offline_activity_review_rejects_wrong_prior_basis_or_subject(
    tmp_path: Path,
) -> None:
    direct_path, direct_request = _committed_held_activity_run(
        tmp_path / "direct",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
    )
    with pytest.raises(ValueError, match="prior actor basis mismatch"):
        ManufacturingMaterialsProfileSliceService().apply_committed_activity_reviews(
            run_directory=direct_path,
            review_request=direct_request,
        )

    issuer_path, issuer_request = _committed_held_activity_run(
        tmp_path / "issuer",
        subject_scope=SubjectScope.ISSUER,
    )
    with pytest.raises(ValueError, match="cannot promote subject scope"):
        ManufacturingMaterialsProfileSliceService().apply_committed_activity_reviews(
            run_directory=issuer_path,
            review_request=issuer_request,
        )


def test_offline_activity_review_rejects_duplicate_source_review_id(
    tmp_path: Path,
) -> None:
    run_path, request = _committed_held_activity_run(
        tmp_path,
        duplicate_review_id=True,
    )

    with pytest.raises(ValueError, match="targets are not unique"):
        ManufacturingMaterialsProfileSliceService().apply_committed_activity_reviews(
            run_directory=run_path,
            review_request=request,
        )


def test_activity_review_operator_writes_once_outside_source_run(tmp_path: Path) -> None:
    run_path, request = _committed_held_activity_run(tmp_path)
    request_path = tmp_path / "decision.json"
    request_path.write_text(request.model_dump_json(indent=2), encoding="utf-8")
    output_path = tmp_path / "reviews/result.json"

    assert review_operator.main(
        [
            "--run-directory",
            str(run_path),
            "--review-request",
            str(request_path),
            "--output-path",
            str(output_path),
        ]
    ) == 0
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["derived_report_status"] == "usable_with_caveats"
    assert payload["provider_calls"] == 0
    assert not list(output_path.parent.glob(".result.json-*.tmp"))
    with pytest.raises(FileExistsError):
        review_operator.main(
            [
                "--run-directory",
                str(run_path),
                "--review-request",
                str(request_path),
                "--output-path",
                str(output_path),
            ]
        )
    with pytest.raises(ValueError, match="outside the input run"):
        review_operator.main(
            [
                "--run-directory",
                str(run_path),
                "--review-request",
                str(request_path),
                "--output-path",
                str(run_path / "review.json"),
            ]
        )


def _committed_held_activity_run(
    tmp_path: Path,
    *,
    actor_basis: SubjectBasis = SubjectBasis.EXPLICIT_ECONOMIC_RELATIONSHIP,
    subject_scope: SubjectScope = SubjectScope.UNCLEAR,
    duplicate_review_id: bool = False,
) -> tuple[Path, Stage5OfflineActivityReviewRequest]:
    report = ReportIdentity(
        instrument_id="000717.SZ",
        report_id="1225158456",
        document_version="94388fcdf94cbd7f53188c95e3a8efd66348a3a0b2d337db904a88690eef5c55",
        report_period="2025-12-31",
        published_at="2026-04-24T16:00:00+00:00",
    )
    evidence = Evidence(
        evidence_id="stage5-evidence-review",
        report=report,
        page=10,
        section_title="主要业务",
        anchor=TextAnchor(bounded_quote=SOURCE_TEXT),
    )
    activity = Activity(
        record_id="stage5-held-activity",
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=report,
        subject_scope=subject_scope,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="钢铁产品", value="制造"),
        action=ActivityAction.PRODUCES,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=actor_basis,
        object_name="钢铁产品",
        source_verb="制造",
    )
    overview = BusinessOverview(
        record_id="stage5-overview",
        field_id="business_overview_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="主要业务"),
        source_text=SOURCE_TEXT,
    )
    scopes = [
        _overview_scope(report, evidence, overview, activity),
        _legal_empty_scope(report, evidence, "segment", ChapterTask.EXTRACT_SEGMENT_FINANCIALS, "segment_dimension", RequirementLevel.REQUIRED),
        _legal_empty_scope(report, evidence, "quantity", ChapterTask.EXTRACT_OPERATING_QUANTITIES, "sales_volume", RequirementLevel.CONDITIONAL),
        _legal_empty_scope(report, evidence, "materials", ChapterTask.EXTRACT_MATERIAL_INPUTS, "material_input", RequirementLevel.CONDITIONAL),
        _legal_empty_scope(report, evidence, "counterparties", ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION, "counterparty_relationship", RequirementLevel.CONDITIONAL),
        _legal_empty_scope(report, evidence, "regime", ChapterTask.EXTRACT_BUSINESS_REGIME, "business_regime", RequirementLevel.REQUIRED, not_applicable=True),
    ]
    if duplicate_review_id:
        overview_scope = scopes[0]
        candidate_review = overview_scope.task_result.human_review_items[0]
        scopes[0] = overview_scope.model_copy(
            update={
                "task_result": overview_scope.task_result.model_copy(
                    update={
                        "human_review_items": (
                            *overview_scope.task_result.human_review_items,
                            candidate_review.model_copy(),
                        )
                    }
                )
            }
        )
    task_results = [scope.task_result for scope in scopes]
    report_bundle = Stage5ReportBundle(
        run_id="held-activity-run",
        sample_id="manufacturing-materials-oos-000717-2025",
        company_name="中南钢铁",
        report=report,
        sample_manifest_revision="manufacturing-materials-second-oos-manifest-20260908-v1",
        evidence_plan_version="manufacturing_materials_oos.2026-09-09.5",
        evidence_plan_hash="b" * 64,
        scope_results=tuple(scopes),
        research_view=project_research_view(
            company_name="中南钢铁", report=report, task_results=task_results
        ),
        report_status=Stage5ReportStatus.HOLD,
        benchmark=Stage5BenchmarkResult(decision="hold"),
        created_at="2026-09-09T00:00:00+00:00",
    )
    bundle = Stage5RunBundle(
        run_id=report_bundle.run_id,
        sample_manifest_revision=report_bundle.sample_manifest_revision,
        evidence_plan_version=report_bundle.evidence_plan_version,
        reports=(report_bundle,),
        overall_status=Stage5OverallStatus.HOLD,
        created_at="2026-09-09T00:00:00+00:00",
    )
    store = Stage5RunBundleStore(tmp_path / "runs", repository_root=REPOSITORY_ROOT)
    run_path = store.commit(bundle)
    manifest_hash = _sha256(run_path / "manifest.json")
    report_hash = _sha256(
        run_path / "reports/manufacturing-materials-oos-000717-2025.json"
    )
    decision = Stage5ActivityReviewDecision(
        review_id="review:stage5-held-activity",
        target_id=activity.record_id,
        evidence_id=evidence.evidence_id,
        activity_actor="公司",
        source_actor="公司",
        source_text=SOURCE_TEXT,
        reason="公司是制造的直接语法主语，研究接受但主体保持 unclear。",
        reviewer="unit-test",
        reviewed_at="2026-09-09T00:00:00+00:00",
    )
    request = Stage5OfflineActivityReviewRequest(
        adjudication_id="activity-review-test",
        source_run_id=bundle.run_id,
        sample_id=report_bundle.sample_id,
        source_manifest_sha256=manifest_hash,
        source_report_sha256=report_hash,
        decisions=(decision,),
    )
    return run_path, request


def _overview_scope(
    report: ReportIdentity,
    evidence: Evidence,
    overview: BusinessOverview,
    activity: Activity,
) -> Stage5ScopeResult:
    prepared = _prepared_scope(
        report,
        evidence,
        "business_overview",
        ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        ("business_overview_source", "explicit_activity"),
    )
    result = CompanyProfileTaskResult(
        request_id="held-activity-run:business_overview",
        records=(overview, activity),
        dispositions=(
            Disposition(
                target_id=overview.record_id,
                field_id=overview.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
            Disposition(
                target_id=activity.record_id,
                field_id=activity.field_id,
                status=DispositionStatus.BLOCKED,
                reason_codes=(ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED,),
            ),
        ),
        coverage=(
            CoverageResult(
                field_id="business_overview_source",
                chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                requirement_level=RequirementLevel.REQUIRED,
                status=CoverageStatus.OBSERVED,
            ),
            CoverageResult(
                field_id="explicit_activity",
                chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                requirement_level=RequirementLevel.CONDITIONAL,
                status=CoverageStatus.UNCLEAR,
                reason_code=CoverageReasonCode.CANDIDATE_UNRESOLVED,
                reason="candidate did not pass",
            ),
        ),
        human_review_items=(
            HumanReviewItem(
                review_id="review:stage5-held-activity",
                field_id="explicit_activity",
                candidate=activity,
                evidence=activity.evidence,
                reason_codes=(ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED,),
                allowed_actions=("accept_for_research_review", "reject", "hold", "request_repair"),
            ),
            HumanReviewItem(
                review_id="held-activity-run:coverage:explicit_activity",
                field_id="explicit_activity",
                evidence=(),
                reason_codes=(ContractErrorCode.REQUIRED_COVERAGE_MISSING,),
                allowed_actions=("accept_for_research_review", "reject", "hold", "request_repair"),
            ),
        ),
        task_complete=False,
    )
    return Stage5ScopeResult(
        scope_id=prepared.scope_id,
        request_id=result.request_id,
        prepared_scope=prepared,
        task_result=result,
    )


def _legal_empty_scope(
    report: ReportIdentity,
    evidence: Evidence,
    scope_id: str,
    chapter: ChapterTask,
    field_id: str,
    requirement: RequirementLevel,
    *,
    not_applicable: bool = False,
) -> Stage5ScopeResult:
    prepared = _prepared_scope(report, evidence, scope_id, chapter, (field_id,))
    status = CoverageStatus.NOT_APPLICABLE if not_applicable else CoverageStatus.NOT_DISCLOSED
    reason_code = (
        CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE
        if not_applicable
        else CoverageReasonCode.SOURCE_REASON_UNSPECIFIED
    )
    result = CompanyProfileTaskResult(
        request_id=f"held-activity-run:{scope_id}",
        records=(),
        dispositions=(),
        coverage=(
            CoverageResult(
                field_id=field_id,
                chapter_task=chapter,
                requirement_level=requirement,
                status=status,
                reason_code=reason_code,
                reason="legal empty fixture",
            ),
        ),
        human_review_items=(),
        task_complete=True,
    )
    return Stage5ScopeResult(
        scope_id=scope_id,
        request_id=result.request_id,
        prepared_scope=prepared,
        task_result=result,
    )


def _prepared_scope(
    report: ReportIdentity,
    evidence: Evidence,
    scope_id: str,
    chapter: ChapterTask,
    field_ids: tuple[str, ...],
) -> PreparedRequestScope:
    return PreparedRequestScope(
        sample_id="manufacturing-materials-oos-000717-2025",
        scope_id=scope_id,
        chapter_task=chapter,
        field_ids=field_ids,
        report=report,
        evidence_bundle=(PreparedEvidence(evidence=evidence),),
        page_contexts=(
            PreparedPageContext(
                page=evidence.page,
                text=SOURCE_TEXT,
                text_hash="a" * 64,
                extraction_method="pypdf",
                quality_status="usable",
            ),
        ),
        plan_version="manufacturing_materials_oos.2026-09-09.5",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
