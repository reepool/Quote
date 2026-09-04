from __future__ import annotations

import json
from pathlib import Path

from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
    HumanReviewItem,
)
from research.company_profile.models import (
    AssertionClass,
    BusinessOverview,
    BusinessRegime,
    CapacityKind,
    ChapterTask,
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    IdentityClass,
    LogicalSlot,
    Measurement,
    MetricType,
    PeriodType,
    ProcessingDirection,
    Relationship,
    RelationshipType,
    RequirementLevel,
    Segment,
    SourceNativeValue,
    SubjectScope,
)
from research.company_profile.projection import project_research_view
from research.company_profile.stage5 import (
    PreparedRequestScope,
    Stage5EvidencePreparer,
    load_stage5_evidence_plan,
    load_stage5_sample_manifest,
)
from research.company_profile.stage5_bundle import Stage5RunBundleStore
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
    Stage5SemanticInput,
    _normalize_review_actions,
    _suppress_same_scope_legal_empty_relationships,
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


def test_stage5_fake_single_report_commits_research_view_and_legal_empty(
    tmp_path: Path,
) -> None:
    manifest = load_stage5_sample_manifest(
        SAMPLE_MANIFEST,
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_stage5_evidence_plan(EVIDENCE_PLAN)
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    service = ManufacturingMaterialsProfileSliceService(
        evidence_preparer=Stage5EvidencePreparer(
            PdfRouter(native=PypdfNativeAdapter())
        )
    )

    execution = service.run_semantic_slice(
        run_id="fake-603659",
        manifest=manifest,
        evidence_plan=plan,
        evidence_plan_path=EVIDENCE_PLAN,
        store=store,
        provider_factory=lambda _: None,
        semantic_input_factory=_fake_603659_semantic_input,
        sample_ids=("manufacturing-materials-603659-2025",),
    )

    assert execution.overall_status == "hold"
    assert execution.report_statuses == {
        "manufacturing-materials-603659-2025": "complete"
    }
    manifest_payload = json.loads(
        (execution.output_path / "manifest.json").read_text(encoding="utf-8")
    )
    report = manifest_payload["reports"][0]
    view = report["research_view"]
    assert view["production_authorization"] == "not_authorized"
    assert view["business_overview"] is not None
    assert view["segments"]
    assert any(
        item["details"].get("metric_type") == "processing_volume"
        for item in view["operating_measurements"]
    )
    assert view["counterparties"] == []
    assert any(
        item["field_id"] == "counterparty_relationship"
        and item["status"] == "not_disclosed"
        for item in view["coverage"]
    )
    assert not list(store.output_root.glob(".stage5-tmp-*"))


def test_stage5_preparation_only_writes_no_provider_or_semantic_output(
    tmp_path: Path,
) -> None:
    manifest = load_stage5_sample_manifest(
        SAMPLE_MANIFEST,
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_stage5_evidence_plan(EVIDENCE_PLAN)
    store = Stage5RunBundleStore(
        tmp_path / "isolated",
        repository_root=REPOSITORY_ROOT,
    )
    service = ManufacturingMaterialsProfileSliceService(
        evidence_preparer=Stage5EvidencePreparer(
            PdfRouter(native=PypdfNativeAdapter())
        )
    )

    execution = service.run_preparation_only(
        run_id="prepare-four",
        manifest=manifest,
        evidence_plan=plan,
        evidence_plan_path=EVIDENCE_PLAN,
        store=store,
    )

    payload = json.loads(
        (execution.output_path / "manifest.json").read_text(encoding="utf-8")
    )
    assert execution.overall_status == "prepared"
    assert payload["provider_calls"] == 0
    assert len(payload["scopes"]) == 43
    assert {item["sample_id"] for item in payload["scopes"]} == {
        "manufacturing-materials-300750-2025",
        "manufacturing-materials-603659-2025",
        "manufacturing-materials-920015-2025",
        "manufacturing-materials-302132-2025-regime",
    }
    assert "records" not in payload
    assert "research_view" not in payload


def test_stage5_request_scope_legal_empty_suppresses_only_same_scope_relationship() -> None:
    manifest = load_stage5_sample_manifest(
        SAMPLE_MANIFEST,
        repository_root=REPOSITORY_ROOT,
    )
    plan = load_stage5_evidence_plan(EVIDENCE_PLAN)
    scopes = Stage5EvidencePreparer(
        PdfRouter(native=PypdfNativeAdapter())
    ).prepare_report(
        manifest=manifest,
        evidence_plan=plan,
        sample_id="manufacturing-materials-603659-2025",
    )
    scope = next(
        item for item in scopes if item.scope_id == "top_five_customer_totals_only"
    )
    evidence = scope.evidence_bundle[0].evidence
    relationship = Relationship(
        record_id="603659-invented-top-five-aggregate",
        field_id="counterparty_relationship",
        chapter_task=scope.chapter_task,
        report=scope.report,
        subject_scope=SubjectScope.UNCLEAR,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="前五名客户合计"),
        relation_type=RelationshipType.CUSTOMER,
        object_name="前五名客户合计",
        identity_class=IdentityClass.REPORT_LOCAL_AGGREGATE,
    )
    accepted = CompanyProfileTaskResult(
        request_id="same-scope",
        records=(relationship,),
        dispositions=(
            Disposition(
                target_id=relationship.record_id,
                field_id=relationship.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
        ),
        coverage=(_not_disclosed_counterparty(scope),),
        human_review_items=(),
        task_complete=True,
    )

    suppressed = _suppress_same_scope_legal_empty_relationships(accepted)
    same_scope_view = project_research_view(
        company_name="璞泰来",
        report=scope.report,
        task_results=(suppressed,),
    )
    independent_scope_view = project_research_view(
        company_name="璞泰来",
        report=scope.report,
        task_results=(
            accepted.model_copy(
                update={"request_id": "independent-source-scope", "coverage": ()}
            ),
        ),
    )

    assert suppressed.dispositions[0].status == DispositionStatus.BLOCKED
    assert suppressed.dispositions[0].reason_codes == (
        ContractErrorCode.PROHIBITED_INFERENCE,
    )
    assert same_scope_view.counterparties == ()
    assert len(independent_scope_view.counterparties) == 1

    review = HumanReviewItem(
        review_id="research-review",
        field_id=relationship.field_id,
        candidate=relationship,
        evidence=relationship.evidence,
        reason_codes=(ContractErrorCode.SUBJECT_UNSUPPORTED,),
    )
    normalized = _normalize_review_actions(
        accepted.model_copy(update={"human_review_items": (review,)})
    )
    assert normalized.human_review_items[0].allowed_actions == (
        "accept_for_research_review",
        "reject",
        "hold",
        "request_repair",
    )


def _fake_603659_semantic_input(scope: PreparedRequestScope) -> Stage5SemanticInput:
    evidence = scope.evidence_bundle[0].evidence
    common = {
        "chapter_task": scope.chapter_task,
        "report": scope.report,
        "subject_scope": SubjectScope.UNCLEAR,
        "reported_period": "2025",
        "assertion_class": AssertionClass.REPORTED_FACT,
        "evidence": (evidence,),
    }
    candidates = []
    coverage = []
    if scope.scope_id == "business_overview":
        quote = evidence.anchor.bounded_quote
        candidates.append(
            BusinessOverview(
                record_id="603659-overview",
                field_id="business_overview_source",
                period_type=PeriodType.DURATION,
                source_native=SourceNativeValue(name="主要业务"),
                source_text=quote,
                **common,
            )
        )
        coverage.append(_legal_empty(scope, "explicit_activity"))
    elif scope.scope_id == "segment_product_and_adjustment":
        candidates.extend(
            (
                Segment(
                    record_id="603659-segment",
                    field_id="segment_dimension",
                    period_type=PeriodType.DURATION,
                    source_native=SourceNativeValue(name="新能源电池材料与服务"),
                    dimension="product",
                    label="新能源电池材料与服务",
                    **common,
                ),
                _measurement(
                    "603659-revenue",
                    "operating_revenue",
                    MetricType.OPERATING_REVENUE,
                    LogicalSlot.REVENUE,
                    "11792842608.70",
                    "元",
                    "营业收入",
                    common,
                ),
                _measurement(
                    "603659-cost",
                    "operating_cost",
                    MetricType.OPERATING_COST,
                    LogicalSlot.COST,
                    "7909390929.81",
                    "元",
                    "营业成本",
                    common,
                ),
                _measurement(
                    "603659-margin",
                    "gross_margin_reported",
                    MetricType.GROSS_MARGIN_REPORTED,
                    LogicalSlot.GROSS_MARGIN,
                    "32.93",
                    "%",
                    "毛利率",
                    common,
                ),
            )
        )
    elif scope.scope_id == "capacity_and_processing_narrative":
        candidates.extend(
            (
                _measurement(
                    "603659-capacity",
                    "production_capacity",
                    MetricType.PRODUCTION_CAPACITY,
                    LogicalSlot.CAPACITY,
                    "140",
                    "亿㎡",
                    "有效产能",
                    common,
                    capacity_kind=CapacityKind.EFFECTIVE_CAPACITY,
                ),
                _measurement(
                    "603659-processing",
                    "processing_volume",
                    MetricType.PROCESSING_VOLUME,
                    LogicalSlot.PROCESSING_VOLUME,
                    "109.42",
                    "亿㎡",
                    "涂覆加工量（销量）",
                    common,
                    processing_direction=ProcessingDirection.EXTERNAL_SERVICE_PROVIDED,
                ),
            )
        )
    elif scope.scope_id == "product_volume_table":
        candidates.append(
            _measurement(
                "603659-sales",
                "sales_volume",
                MetricType.SALES_VOLUME,
                LogicalSlot.SALES_VOLUME,
                "1094249.25",
                "万㎡",
                "销售量",
                common,
            )
        )
        coverage.extend(
            _legal_empty(scope, field_id)
            for field_id in ("production_volume", "inventory_volume")
        )
    elif scope.scope_id == "material_risk_disclosure":
        candidates.append(
            Relationship(
                record_id="603659-material",
                field_id="material_input",
                period_type=PeriodType.DURATION,
                source_native=SourceNativeValue(name="钢材"),
                relation_type=RelationshipType.MATERIAL_INPUT,
                object_name="钢材",
                **common,
            )
        )
    elif scope.scope_id == "top_five_customer_totals_only":
        candidates.append(
            _measurement(
                "603659-customer-share",
                "customer_concentration",
                MetricType.DISCLOSED_SHARE,
                LogicalSlot.DISCLOSED_SHARE,
                "58.14",
                "%",
                "占年度销售总额",
                common,
            )
        )
        coverage.append(_not_disclosed_counterparty(scope))
    elif scope.scope_id == "top_five_supplier_totals_only":
        candidates.append(
            _measurement(
                "603659-supplier-share",
                "supplier_concentration",
                MetricType.DISCLOSED_SHARE,
                LogicalSlot.DISCLOSED_SHARE,
                "13.98",
                "%",
                "占年度采购总额",
                common,
            )
        )
        coverage.append(_not_disclosed_counterparty(scope))
    elif scope.scope_id == "reported_business_change":
        candidates.append(
            BusinessRegime(
                record_id="603659-regime",
                field_id="business_regime",
                period_type=PeriodType.DURATION,
                source_native=SourceNativeValue(name="主营业务重大变化不适用"),
                regime_label="stable",
                effective_from="2025-01-01",
                **common,
            )
        )
    else:
        coverage.extend(_legal_empty(scope, field_id) for field_id in scope.field_ids)
    return Stage5SemanticInput(
        deterministic_candidates=tuple(candidates),
        provided_coverage=tuple(coverage),
    )


def _measurement(
    record_id: str,
    field_id: str,
    metric_type: MetricType,
    logical_slot: LogicalSlot,
    value: str,
    unit: str,
    header: str,
    common: dict,
    **extra,
) -> Measurement:
    return Measurement(
        record_id=record_id,
        field_id=field_id,
        period_type=PeriodType.DURATION,
        source_native=SourceNativeValue(
            name="新能源电池材料与服务",
            value=value,
            unit=unit,
            header=header,
        ),
        metric_type=metric_type,
        logical_slot=logical_slot,
        measured_object="新能源电池材料与服务",
        **extra,
        **common,
    )


def _legal_empty(scope: PreparedRequestScope, field_id: str) -> CoverageResult:
    requirement = (
        RequirementLevel.REQUIRED
        if field_id in {"business_overview_source", "segment_dimension", "business_regime"}
        else RequirementLevel.CONDITIONAL
    )
    return CoverageResult(
        field_id=field_id,
        chapter_task=scope.chapter_task,
        requirement_level=requirement,
        status=CoverageStatus.NOT_APPLICABLE,
        evidence=(scope.evidence_bundle[0].evidence,),
    )


def _not_disclosed_counterparty(scope: PreparedRequestScope) -> CoverageResult:
    return CoverageResult(
        field_id="counterparty_relationship",
        chapter_task=ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
        requirement_level=RequirementLevel.CONDITIONAL,
        status=CoverageStatus.NOT_DISCLOSED,
        reason_code=CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
        reason="complete top-five section reports totals but no names",
        evidence=(scope.evidence_bundle[0].evidence,),
    )
