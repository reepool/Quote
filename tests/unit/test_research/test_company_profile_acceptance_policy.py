from __future__ import annotations

from types import SimpleNamespace

import pytest

from research.company_profile.acceptance_policy import derive_confidence, usage_policy
from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
    HumanReviewItem,
)
from research.company_profile.models import (
    Activity,
    ActivityAction,
    AssertionClass,
    ChapterTask,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    PeriodType,
    ReportIdentity,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.stage5 import APPROVED_STAGE5_SAMPLES
from research.company_profile.stage5_benchmark import (
    Stage5NegativeCaseResult,
    _annotation_match_status,
    _evaluate_annotation,
    _post_run_slice_status,
    _record_matches_annotation,
    evaluate_fixture_guards,
)
from research.company_profile.stage5_bundle import (
    Stage5BenchmarkResult,
    Stage5OverallStatus,
    Stage5ReportStatus,
)
from research.company_profile.stage5_service import (
    _derive_report_status,
    _overall_status,
)


@pytest.fixture
def reference_measurement():
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
        section_title="s",
        anchor=TextAnchor(bounded_quote="营业收入 100 元"),
    )
    return Measurement(
        record_id="m",
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        report=report,
        subject_scope=SubjectScope.UNCLEAR,
        subject_basis=None,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="产品", value="100", unit="元"),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object="产品",
    )


def test_unclear_measurement_is_research_visible_but_not_group_comparable(
    reference_measurement,
):
    policy = usage_policy(reference_measurement)
    assert policy["display"] is True
    assert policy["consolidated_aggregation"] is False
    assert policy["ranking"] is False
    assert policy["restriction_reason"] == (
        "unclear_subject_scope_restricted_from_consolidated_use"
    )
    assert derive_confidence(reference_measurement) == "medium"


def test_reconciled_group_measurement_unlocks_group_uses(reference_measurement):
    record = reference_measurement.model_copy(
        update={
            "subject_scope": SubjectScope.CONSOLIDATED_GROUP,
            "subject_basis": (
                SubjectBasis.NUMERIC_RECONCILIATION_TO_CONSOLIDATED_STATEMENT
            ),
        }
    )
    policy = usage_policy(record)
    assert policy["consolidated_aggregation"] is True
    assert policy["cross_company_comparison"] is True
    assert derive_confidence(record) == "high"


def test_business_segment_measurement_is_not_reinterpreted_as_group_total(
    reference_measurement,
):
    record = reference_measurement.model_copy(
        update={"subject_scope": SubjectScope.BUSINESS_SEGMENT}
    )
    policy = usage_policy(record)
    assert policy["display"] is True
    assert policy["consolidated_aggregation"] is False
    assert policy["restriction_reason"] == "subject_scope_not_group_comparable"


def test_unclear_activity_remains_available_for_research(reference_measurement):
    activity = Activity(
        record_id="a",
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=reference_measurement.report,
        subject_scope=SubjectScope.UNCLEAR,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=reference_measurement.evidence,
        source_native=SourceNativeValue(name="动力电池"),
        action=ActivityAction.PRODUCES,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name="动力电池",
        source_verb="生产",
    )
    assert usage_policy(activity)["display"] is True
    assert derive_confidence(activity) == "medium"


def test_fixture_guards_execute_existing_contracts_without_provider():
    results = evaluate_fixture_guards()
    assert len(results) == 4
    assert all(
        item.source == "fixture_guard" and item.evaluated and item.passed
        for item in results
    )
    assert {item.case_id for item in results} == {
        "mm-neg-inventory-value-as-volume",
        "mm-neg-required-page-omitted",
        "mm-neg-required-page-unreadable",
        "mm-neg-unit-ambiguous",
    }


def _record_and_annotation(*, runtime_value="40", runtime_unit="kt/a"):
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


def test_gold_matcher_accepts_only_closed_unit_conversion_and_punctuation():
    record, annotation = _record_and_annotation()
    assert _record_matches_annotation(record, annotation) is True
    assert _annotation_match_status(record, annotation) == "semantic_match"

    record["source_native"]["value"] = "40,000"
    record["source_native"]["unit"] = "吨/年"
    assert _record_matches_annotation(record, annotation) is True

    record["source_native"]["unit"] = "kg/day"
    assert _record_matches_annotation(record, annotation) is False


def test_gold_matcher_preserves_numeric_sign():
    record, annotation = _record_and_annotation(
        runtime_value="-40,000", runtime_unit="吨/年"
    )
    assert _record_matches_annotation(record, annotation) is False


def test_gold_matcher_classifies_year_format_difference_as_semantic():
    record, annotation = _record_and_annotation(
        runtime_value="40000", runtime_unit="吨/年"
    )
    record["reported_period"] = "2025年"

    assert _record_matches_annotation(record, annotation) is True
    assert _annotation_match_status(record, annotation) == "semantic_match"


def test_gold_matcher_accepts_bounded_period_dimension_and_percent_forms():
    record, annotation = _record_and_annotation(
        runtime_value="44.00%", runtime_unit="%"
    )
    record.update(
        {
            "field_id": "gross_margin_reported",
            "metric_type": "gross_margin_reported",
            "logical_slot": "gross_margin",
            "reported_period": "2025年",
            "segment_dimension": "分产品",
        }
    )
    record["source_native"].update(
        {"value": "44.00%", "unit": "%", "header": "毛利率%"}
    )
    annotation.update(
        {
            "field_id": "gross_margin_reported",
            "semantic": {
                "object_type": "Measurement",
                "metric_type": "gross_margin_reported",
                "logical_slot": "gross_margin",
                "subject_scope": "unclear",
                "period": "2025",
                "segment_dimension": "product",
            },
            "source_native": {
                "name": "羟胺盐",
                "value": "44.00",
                "unit": "%",
                "header": "毛利率",
            },
        }
    )

    assert _record_matches_annotation(record, annotation) is True
    assert _annotation_match_status(record, annotation) == "semantic_match"


def test_gold_matcher_uses_continuation_page_and_expected_completion_qualifier():
    record, annotation = _record_and_annotation(
        runtime_value="40,000", runtime_unit="吨/年"
    )
    record["reported_period"] = "2025年度"
    record["source_native"].update(
        {
            "name": "新增羟胺盐产能",
            "qualifier": "预计完工时间2026年",
        }
    )
    record["evidence"][0]["page"] = 50
    record["evidence"][0]["continuation_pages"] = [49]
    annotation["semantic"]["period"] = "2026_expected_completion"
    annotation["source_native"]["name"] = "羟胺盐在建产能"

    assert _record_matches_annotation(record, annotation) is True


def test_gold_matcher_accepts_compound_row_label_and_normalized_quote():
    record, annotation = _record_and_annotation(runtime_value="661", runtime_unit="GWh")
    record.update(
        {
            "field_id": "sales_volume",
            "metric_type": "sales_volume",
            "logical_slot": "sales_volume",
            "measured_object": "电池系统",
        }
    )
    record["source_native"].update({"name": "销售量", "value": "661", "unit": "GWh"})
    record["evidence"][0]["anchor"] = {
        "anchor_type": "text",
        "bounded_quote": "电池系统 销售量 GWh 661",
    }
    annotation.update(
        {
            "field_id": "sales_volume",
            "semantic": {
                "object_type": "Measurement",
                "metric_type": "sales_volume",
                "logical_slot": "sales_volume",
                "subject_scope": "unclear",
                "period": "2025",
            },
            "source_native": {
                "name": "电池系统销售量",
                "value": "661",
                "unit": "GWh",
                "header": "2025年",
            },
            "evidence": {
                "page": 49,
                "physical_anchor": {
                    "row_label": "电池系统/销售量",
                    "bounded_quote": "电池系统销售量GWh661",
                },
            },
        }
    )

    assert _record_matches_annotation(record, annotation) is True


def test_inventory_period_can_reconcile_to_report_period_date():
    record, annotation = _record_and_annotation(
        runtime_value="39,299.86", runtime_unit="吨"
    )
    record.update(
        {
            "field_id": "inventory_volume",
            "metric_type": "inventory_volume",
            "logical_slot": "inventory_volume",
            "measured_object": "负极材料",
            "reported_period": "2025年度",
            "report": {"report_period": "2025-12-31"},
        }
    )
    record["source_native"].update(
        {
            "name": "负极材料库存量",
            "value": "39,299.86",
            "unit": "吨",
        }
    )
    record["evidence"][0]["anchor"]["row_label"] = "负极材料"
    annotation.update(
        {
            "field_id": "inventory_volume",
            "semantic": {
                "object_type": "Measurement",
                "metric_type": "inventory_volume",
                "logical_slot": "inventory_volume",
                "subject_scope": "unclear",
                "period": "2025-12-31",
            },
            "source_native": {
                "name": "负极材料库存量",
                "value": "39299.86",
                "unit": "吨",
                "header": "库存量",
            },
            "evidence": {
                "page": 49,
                "physical_anchor": {"row_label": "负极材料"},
            },
        }
    )

    assert _record_matches_annotation(record, annotation) is True


def test_gold_matcher_keeps_distinct_physical_anchors_separate():
    record, annotation = _record_and_annotation()
    record["source_native"]["name"] = "另一个项目"
    record["measured_object"] = "另一个项目"
    record["evidence"][0]["anchor"]["row_label"] = "另一个项目"
    record["evidence"][0]["anchor"]["cell_locator"] = "page49/另一个项目/在建产能"
    assert _record_matches_annotation(record, annotation) is False

    record, annotation = _record_and_annotation()
    record["evidence"][0]["anchor"]["cell_locator"] = "page49/羟胺盐/设计产能"
    assert _record_matches_annotation(record, annotation) is False


def test_gold_matcher_requires_page_and_anchor_on_same_evidence():
    record, annotation = _record_and_annotation()
    record["evidence"] = [
        {
            "page": 49,
            "anchor": {
                "anchor_type": "table",
                "row_label": "另一个项目",
                "cell_locator": "page49/另一个项目/在建产能",
            },
        },
        {
            "page": 50,
            "anchor": {
                "anchor_type": "table",
                "row_label": "羟胺盐",
                "cell_locator": "page49/羟胺盐/在建产能",
            },
        },
    ]
    assert _record_matches_annotation(record, annotation) is False


def test_gold_subject_strictness_allows_unclear_without_promotion():
    record, annotation = _record_and_annotation(
        runtime_value="40000", runtime_unit="吨/年"
    )
    annotation["semantic"]["subject_scope"] = "business_segment"
    annotation["subject_strictness"] = "allow_unclear_if_not_promoted"
    assert _annotation_match_status(record, annotation) == "accepted_with_uncertainty"

    annotation["semantic"].update(
        {
            "subject_scope": "consolidated_group",
            "subject_basis": "numeric_reconciliation_to_consolidated_statement",
        }
    )
    assert _annotation_match_status(record, annotation) == "accepted_with_uncertainty"


def test_relationship_matcher_uses_identity_and_anchor_not_attached_amount():
    record = {
        "record_id": "customer",
        "field_id": "counterparty_relationship",
        "object_type": "Relationship",
        "relation_type": "customer",
        "identity_class": "report_local_anonymous",
        "object_name": "客户 A",
        "subject_scope": "unclear",
        "reported_period": "2025年度",
        "source_native": {
            "name": "客户 A",
            "value": None,
            "unit": None,
            "header": None,
        },
        "evidence": [
            {
                "page": 27,
                "anchor": {
                    "anchor_type": "text",
                    "bounded_quote": "锂电池供应 客户 A(1) 本报告期履行金额 58,159,202",
                },
            }
        ],
    }
    annotation = {
        "annotation_id": "customer",
        "sample_id": "sample",
        "field_id": "counterparty_relationship",
        "coverage_status": "observed",
        "subject_strictness": "must_equal",
        "semantic": {
            "object_type": "Relationship",
            "relation_type": "customer",
            "identity_class": "report_local_anonymous",
            "subject_scope": "unclear",
            "period": "2025",
        },
        "source_native": {
            "name": "客户 A(1)",
            "value": "58159202",
            "unit": "千元",
            "header": "本报告期履行金额",
        },
        "evidence": {
            "page": 27,
            "physical_anchor": {"bounded_quote": "客户 A(1)"},
        },
    }

    assert _record_matches_annotation(record, annotation) is True
    assert _annotation_match_status(record, annotation) == "semantic_match"


def test_gold_contract_conflict_is_not_hidden_by_matcher():
    annotation = {
        "annotation_id": "contract-conflict",
        "sample_id": "sample",
        "field_id": "sales_volume",
        "coverage_status": "not_applicable",
        "evidence": {"page": 15},
    }
    report = {
        "scope_results": [
            {
                "task_result": {
                    "coverage": [
                        {
                            "field_id": "sales_volume",
                            "status": "not_disclosed",
                            "evidence": [{"page": 15}],
                        }
                    ]
                }
            }
        ]
    }
    report["scope_results"][0]["task_result"]["coverage"][0]["evidence"][0]["page"] = 16
    result = _evaluate_annotation(annotation, report)
    assert result.passed is False
    assert result.match_status == "failed"

    report["scope_results"][0]["task_result"]["coverage"][0]["evidence"][0]["page"] = 15
    result = _evaluate_annotation(annotation, report)
    assert result.passed is False
    assert result.match_status == "gold_contract_conflict"


def test_report_state_distinguishes_allowed_caveat_from_execution_failure(
    reference_measurement,
):
    accepted = CompanyProfileTaskResult(
        request_id="accepted",
        records=(reference_measurement,),
        dispositions=(
            Disposition(
                target_id=reference_measurement.record_id,
                field_id=reference_measurement.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    assert (
        _derive_report_status(
            task_results=[accepted],
            scope_results=[],
            benchmark=Stage5BenchmarkResult(decision="pass"),
        )
        == Stage5ReportStatus.USABLE_WITH_CAVEATS
    )

    failed = accepted.model_copy(
        update={
            "human_review_items": (
                HumanReviewItem(
                    review_id="deadline",
                    field_id="operating_revenue",
                    evidence=reference_measurement.evidence,
                    reason_codes=(ContractErrorCode.DEADLINE_EXCEEDED,),
                ),
            ),
            "task_complete": False,
        }
    )
    assert (
        _derive_report_status(
            task_results=[failed],
            scope_results=[],
            benchmark=Stage5BenchmarkResult(decision="hold"),
        )
        == Stage5ReportStatus.FAILED
    )


def test_runtime_bundle_waits_for_post_run_negative_evaluation():
    reports = tuple(
        SimpleNamespace(
            sample_id=sample_id,
            report_status=Stage5ReportStatus.USABLE_WITH_CAVEATS,
            review_decisions=(),
        )
        for sample_id in APPROVED_STAGE5_SAMPLES
    )
    assert _overall_status(reports, tuple(APPROVED_STAGE5_SAMPLES)) == (
        Stage5OverallStatus.HOLD
    )


def test_post_run_negative_evaluation_can_form_research_slice_usable():
    manifest = {
        "overall_status": "hold",
        "reports": [
            {
                "sample_id": sample_id,
                "report_status": "usable_with_caveats",
            }
            for sample_id in APPROVED_STAGE5_SAMPLES
        ],
    }
    passed = Stage5NegativeCaseResult(
        case_id="evaluated-0",
        evaluated=True,
        passed=True,
        reason="guarded",
    )
    real_results = tuple(
        passed.model_copy(
            update={
                "case_id": f"evaluated-{index}",
                "evaluated": index != 18,
                "passed": index != 18,
                "reason": "guarded" if index != 18 else "not triggered",
            }
        )
        for index in range(19)
    )
    fixtures = evaluate_fixture_guards()

    assert (
        _post_run_slice_status(
            manifest,
            fixture_guard_results=fixtures,
            negative_case_results=real_results,
        )
        == "research_slice_usable"
    )

    failed = real_results[0].model_copy(update={"passed": False})
    assert (
        _post_run_slice_status(
            manifest,
            fixture_guard_results=fixtures,
            negative_case_results=(failed, *real_results[1:]),
        )
        == "hold"
    )
    assert (
        _post_run_slice_status(
            manifest,
            fixture_guard_results=fixtures[:-1],
            negative_case_results=real_results,
        )
        == "hold"
    )


def test_report_default_group_scope_is_group_comparable_without_rewriting_source_wording(
    reference_measurement,
):
    record = reference_measurement.model_copy(
        update={
            "subject_scope": SubjectScope.CONSOLIDATED_GROUP,
            "subject_basis": SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE,
        }
    )
    assert record.source_native.name == "产品"
    assert usage_policy(record)["consolidated_aggregation"] is True
    assert derive_confidence(record) == "high"
    issuer_record = record.model_copy(
        update={
            "subject_scope": SubjectScope.ISSUER,
            "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
        }
    )
    assert usage_policy(issuer_record)["consolidated_aggregation"] is False
