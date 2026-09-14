from __future__ import annotations

from research.company_profile import (
    ActivityAction,
    AssertionClass,
    ChapterTask,
    CompanyProfileSemanticService,
    CompanyProfileTaskResult,
    Disposition,
    DispositionStatus,
    Measurement,
    MetricType,
    PeriodType,
    ReportIdentity,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.acceptance_policy import (
    accepted_has_illegal_group_promotion,
    apply_report_default_group_scope,
    derive_confidence,
    usage_policy,
)
from research.company_profile.contracts import ContractErrorCode
from research.company_profile.core_assessment_projection import project_core_assessment
from research.company_profile.models import BusinessOverview, Evidence, LogicalSlot
from research.company_profile.projection import project_research_view
from research.company_profile.stage5_benchmark import (
    _annotation_match_status,
    _evaluate_annotation,
    _record_matches_annotation,
    _subject_match_status,
)
from tests.unit.test_research.test_company_profile_semantic_workflow import (
    _record,
    _replace_record,
    _request,
)


def _measurement_draft(**updates):
    draft = {
        "object_type": "Measurement",
        "field_id": "operating_revenue",
        "subject_scope": "unclear",
        "measured_object": "产品",
        "source_native": {"name": "产品", "value": "100", "unit": "元"},
    }
    draft.update(updates)
    return draft


def _activity_draft(**updates):
    draft = {
        "object_type": "Activity",
        "field_id": "explicit_activity",
        "subject_scope": "unclear",
        "activity_actor": "公司",
        "source_actor": "公司",
        "object_name": "动力电池",
    }
    draft.update(updates)
    return draft


def _capacity_pair(*, runtime_value="40", runtime_unit="kt/a"):
    record = {
        "record_id": "capacity",
        "field_id": "capacity_under_construction",
        "object_type": "Measurement",
        "metric_type": "capacity_under_construction",
        "logical_slot": "capacity_under_construction",
        "measured_object": "羟胺盐",
        "subject_scope": "unclear",
        "reported_period": "2025",
        "source_native": {
            "name": "羟胺盐",
            "value": runtime_value,
            "unit": runtime_unit,
            "header": "在建产能",
        },
        "evidence": [
            {
                "page": 49,
                "anchor": {
                    "anchor_type": "table",
                    "row_label": "羟胺盐",
                    "cell_locator": "page49/羟胺盐/在建产能",
                },
            }
        ],
    }
    annotation = {
        "annotation_id": "gold-capacity",
        "sample_id": "sample",
        "field_id": "capacity_under_construction",
        "coverage_status": "observed",
        "subject_strictness": "must_equal",
        "semantic": {
            "object_type": "Measurement",
            "metric_type": "capacity_under_construction",
            "logical_slot": "capacity_under_construction",
            "subject_scope": "unclear",
            "period": "2025",
        },
        "source_native": {
            "name": "羟胺盐",
            "value": "40000",
            "unit": "吨/年",
            "header": "在建产能",
        },
        "evidence": {
            "page": 49,
            "physical_anchor": {
                "row_label": "羟胺盐",
                "cell_locator": "page49/羟胺盐/在建产能",
            },
        },
    }
    return record, annotation


def _observed_report(record):
    return {
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


def _report_and_evidence(*, quote="公司主要从事动力电池研发、生产和销售。"):
    report = ReportIdentity(
        instrument_id="300750.SZ",
        report_id="r",
        document_version="v",
        report_period="2025",
        published_at="2026-01-01",
    )
    evidence = Evidence(
        evidence_id="e",
        report=report,
        page=1,
        section_title="主要业务",
        anchor=TextAnchor(bounded_quote=quote),
    )
    return report, evidence


def test_unqualified_company_wording_uses_report_default_group_scope():
    draft = apply_report_default_group_scope(_measurement_draft())
    assert draft["subject_scope"] == "consolidated_group"
    assert draft["subject_basis"] == "report_default_group_scope"
    assert draft["source_native"]["name"] == "产品"


def test_extract_keeps_explicit_issuer_and_segment_ahead_of_default_group():
    issuer = apply_report_default_group_scope(
        _measurement_draft(
            subject_scope="issuer",
            subject_basis="direct_source_wording",
            subject_name="宁德时代",
        )
    )
    assert issuer["subject_scope"] == "issuer"
    assert issuer["subject_basis"] == "direct_source_wording"

    segment = apply_report_default_group_scope(
        {
            "object_type": "Segment",
            "subject_scope": "unclear",
            "label": "动力电池",
        }
    )
    assert segment["subject_scope"] == "unclear"
    assert segment.get("subject_basis") != "report_default_group_scope"


def test_subject_conflict_stays_unclear_instead_of_default_group():
    draft = apply_report_default_group_scope(
        _measurement_draft(uncertainty=["母公司口径与合并口径冲突，无法归属"])
    )
    assert draft["subject_scope"] == "unclear"
    assert draft.get("subject_basis") != "report_default_group_scope"


def test_prelabeled_default_group_with_conflict_is_reverted_to_unclear():
    draft = apply_report_default_group_scope(
        _measurement_draft(
            subject_scope="consolidated_group",
            subject_basis="report_default_group_scope",
            uncertainty=["母公司与合并主体冲突，无法归属"],
        )
    )
    assert draft["subject_scope"] == "unclear"
    assert draft.get("subject_basis") != "report_default_group_scope"


def test_parent_local_evidence_blocks_default_group_without_uncertainty():
    draft = apply_report_default_group_scope(
        _measurement_draft(
            subject_scope="consolidated_group",
            subject_basis="report_default_group_scope",
            source_native={"name": "母公司在建产能", "value": "40000", "unit": "吨/年"},
            evidence=[
                {
                    "page": 49,
                    "anchor": {
                        "bounded_quote": "母公司财务报表／母公司在建产能 40,000 吨/年",
                        "row_label": "母公司在建产能",
                    },
                }
            ],
        )
    )
    assert draft["subject_scope"] == "unclear"
    assert draft.get("subject_basis") != "report_default_group_scope"


def test_same_page_parent_data_does_not_taint_company_only_local_quote():
    draft = apply_report_default_group_scope(
        _measurement_draft(
            evidence=[
                {
                    "page": 14,
                    "section_title": "主要业务",
                    "anchor": {
                        "bounded_quote": "公司主要从事动力电池研发、生产和销售。",
                    },
                }
            ],
        )
    )
    assert draft["subject_scope"] == "consolidated_group"
    assert draft["subject_basis"] == "report_default_group_scope"


def test_third_party_actor_is_not_transferred_to_listed_company_group():
    draft = apply_report_default_group_scope(
        _activity_draft(activity_actor="军贸公司", source_actor="军贸公司")
    )
    assert draft["activity_actor"] == "军贸公司"
    assert draft["source_actor"] == "军贸公司"
    assert draft["subject_scope"] == "unclear"
    assert draft.get("subject_basis") != "report_default_group_scope"


def test_legal_default_group_is_not_illegal_promotion():
    record = _default_group_measurement()
    task = CompanyProfileTaskResult(
        request_id="default-group",
        records=(record,),
        dispositions=(
            Disposition(
                target_id=record.record_id,
                field_id=record.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    assert accepted_has_illegal_group_promotion([task]) is False
    assert usage_policy(record)["consolidated_aggregation"] is True
    assert derive_confidence(record) == "high"


def test_verify_accepts_legal_default_group_without_group_wording():
    record = _replace_record(
        _record("sales_volume"),
        subject_scope=SubjectScope.CONSOLIDATED_GROUP.value,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE.value,
        uncertainty=(),
    )
    result = CompanyProfileSemanticService().run_task(_request((record,)))
    assert result.dispositions[0].status == DispositionStatus.ACCEPTED_FOR_REVIEW
    assert ContractErrorCode.SUBJECT_UNSUPPORTED not in result.dispositions[0].reason_codes
    assert result.accepted_records()[0].subject_basis == (
        SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE
    )


def test_verify_blocks_prelabeled_default_group_with_subject_conflict():
    record = _replace_record(
        _record("sales_volume"),
        subject_scope=SubjectScope.CONSOLIDATED_GROUP.value,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE.value,
        uncertainty=("母公司与合并主体冲突，无法归属",),
    )
    result = CompanyProfileSemanticService().run_task(_request((record,)))
    assert result.dispositions[0].status == DispositionStatus.BLOCKED
    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.SUBJECT_UNSUPPORTED,
    )


def test_verify_blocks_default_group_when_local_evidence_is_parent_only():
    base = _record("sales_volume")
    evidence = base.evidence[0].model_dump(mode="json")
    evidence["section_title"] = "母公司财务报表"
    evidence["anchor"]["row_label"] = "母公司在建产能"
    record = _replace_record(
        base,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP.value,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE.value,
        uncertainty=(),
        source_native=base.source_native.model_copy(
            update={"name": "母公司在建产能"}
        ).model_dump(mode="json"),
        evidence=(evidence,),
    )
    result = CompanyProfileSemanticService().run_task(_request((record,)))
    assert result.dispositions[0].status == DispositionStatus.BLOCKED
    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.SUBJECT_UNSUPPORTED,
    )


def test_verify_does_not_transfer_third_party_actor_via_default_group():
    activity = _replace_record(
        _record("explicit_activity"),
        record_id="third-party-default",
        action=ActivityAction.SELLS.value,
        activity_actor="军贸公司",
        source_actor="军贸公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR.value,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP.value,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE.value,
    )
    result = CompanyProfileSemanticService().run_task(_request((activity,)))
    assert result.dispositions[0].status == DispositionStatus.BLOCKED
    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.SUBJECT_UNSUPPORTED,
    )


def test_numeric_error_is_failed_and_not_hidden_by_gold_contract_conflict():
    record, annotation = _capacity_pair(runtime_value="1", runtime_unit="吨/年")
    record["subject_scope"] = "consolidated_group"
    record["subject_basis"] = "report_default_group_scope"
    assert _record_matches_annotation(record, annotation) is False
    assert _annotation_match_status(record, annotation) == "failed"
    result = _evaluate_annotation(annotation, _observed_report(record))
    assert result.passed is False
    assert result.match_status == "failed"


def test_default_group_cannot_overwrite_gold_issuer_or_segment():
    record, annotation = _capacity_pair()
    record["subject_scope"] = "consolidated_group"
    record["subject_basis"] = "report_default_group_scope"
    annotation["semantic"]["subject_scope"] = "issuer"
    assert _subject_match_status(record, annotation) == "failed"
    assert _annotation_match_status(record, annotation) == "failed"

    annotation["semantic"]["subject_scope"] = "business_segment"
    assert _subject_match_status(record, annotation) == "failed"
    assert _annotation_match_status(record, annotation) == "failed"


def test_illegal_default_group_with_parent_evidence_is_gold_failed():
    record, annotation = _capacity_pair()
    record["subject_scope"] = "consolidated_group"
    record["subject_basis"] = "report_default_group_scope"
    record["evidence"][0]["anchor"]["bounded_quote"] = (
        "母公司财务报表／母公司在建产能 40,000 吨/年"
    )
    assert _record_matches_annotation(record, annotation) is True
    assert _subject_match_status(record, annotation) == "failed"
    assert _annotation_match_status(record, annotation) == "failed"
    result = _evaluate_annotation(annotation, _observed_report(record))
    assert result.passed is False
    assert result.match_status == "failed"


def test_illegal_default_group_with_subject_conflict_is_gold_failed():
    record, annotation = _capacity_pair()
    record["subject_scope"] = "consolidated_group"
    record["subject_basis"] = "report_default_group_scope"
    record["uncertainty"] = ["母公司与合并主体冲突，无法归属"]
    assert _subject_match_status(record, annotation) == "failed"
    assert _annotation_match_status(record, annotation) == "failed"
    result = _evaluate_annotation(annotation, _observed_report(record))
    assert result.passed is False
    assert result.match_status == "failed"


def test_legal_default_group_vs_historical_gold_subject_is_contract_conflict():
    record, annotation = _capacity_pair()
    record["subject_scope"] = "consolidated_group"
    record["subject_basis"] = "report_default_group_scope"
    assert _record_matches_annotation(record, annotation) is True
    assert _subject_match_status(record, annotation) == "gold_contract_conflict"
    assert _annotation_match_status(record, annotation) == "gold_contract_conflict"
    result = _evaluate_annotation(annotation, _observed_report(record))
    assert result.passed is False
    assert result.match_status == "gold_contract_conflict"
    assert result.runtime_target_id == "capacity"


def test_legal_default_group_is_not_unsupported_promotion_under_strictness():
    record, annotation = _capacity_pair()
    record["subject_scope"] = "consolidated_group"
    record["subject_basis"] = "report_default_group_scope"
    annotation["subject_strictness"] = "allow_unclear_if_not_promoted"
    assert _annotation_match_status(record, annotation) == "gold_contract_conflict"

    unclear, gold = _capacity_pair()
    gold["semantic"]["subject_scope"] = "business_segment"
    gold["subject_strictness"] = "allow_unclear_if_not_promoted"
    assert _annotation_match_status(unclear, gold) == "accepted_with_uncertainty"


def test_projection_and_core_assessment_keep_legal_default_group():
    report, evidence = _report_and_evidence()
    overview = BusinessOverview(
        record_id="overview-default",
        field_id="business_overview_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=report,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="主要业务"),
        source_text="公司主要从事动力电池研发、生产和销售。",
    )
    measurement = Measurement(
        record_id="revenue-default",
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        report=report,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="产品", value="100", unit="元"),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object="产品",
    )
    result = CompanyProfileTaskResult(
        request_id="default-group-view",
        records=(overview, measurement),
        dispositions=(
            Disposition(
                target_id=overview.record_id,
                field_id=overview.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
            Disposition(
                target_id=measurement.record_id,
                field_id=measurement.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    view = project_research_view(
        company_name="宁德时代",
        report=report,
        task_results=(result,),
    )
    assert view.business_overview is not None
    assert view.business_overview.subject_scope == "consolidated_group"
    assert view.business_overview.details["subject_basis"] == (
        "report_default_group_scope"
    )
    assert view.operating_measurements[0].details["usage"]["consolidated_aggregation"]
    assessment = project_core_assessment(report=report, task_results=(result,))
    assert assessment.principal_business.answered is True
    assert "overview-default" in assessment.principal_business.supporting_record_ids


def _default_group_measurement():
    report, evidence = _report_and_evidence(quote="营业收入 100 元")
    return Measurement(
        record_id="m",
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        report=report,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="产品", value="100", unit="元"),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object="产品",
    )
