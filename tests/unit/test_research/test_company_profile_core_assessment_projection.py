from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from pydantic import TypeAdapter

from research.company_profile import ChapterTask
from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
)
from research.company_profile.core_assessment_projection import (
    COMMON_CORE_MAPPING_VERSION,
    core_assessment_schema_manifest,
    project_core_assessment,
)
from research.company_profile.models import (
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    ReportIdentity,
    RequirementLevel,
    SemanticRecord,
    TableAnchor,
)
from research.company_profile.projection import project_research_view

ROOT = Path(__file__).resolve().parents[3]
REFERENCE_INPUT = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_input.json"
)
RECORD_ADAPTER = TypeAdapter(SemanticRecord)


def _validate_json(model, payload):
    return model.model_validate_json(json.dumps(payload, ensure_ascii=False))


def _record(payload):
    return RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))


def _reference_bundle():
    payload = json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))
    report = _validate_json(ReportIdentity, payload["report"])
    result = CompanyProfileTaskResult(
        request_id="reference-profile",
        records=tuple(_record(item) for item in payload["records"]),
        dispositions=tuple(
            _validate_json(Disposition, item) for item in payload["dispositions"]
        ),
        coverage=tuple(
            _validate_json(CoverageResult, item) for item in payload["coverage"]
        ),
        human_review_items=(),
        task_complete=True,
    )
    return payload, report, result


def _overview_only_result():
    _payload, report, result = _reference_bundle()
    keep = {"cp-300750-overview"}
    records = [item for item in result.records if item.record_id in keep]
    dispositions = [
        item for item in result.dispositions if item.target_id in keep
    ]
    return report, CompanyProfileTaskResult(
        request_id="overview-only",
        records=tuple(records),
        dispositions=tuple(dispositions),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )


def test_mapping_reuses_six_chapter_tasks_and_registers_projection_schema():
    assert [item.value for item in ChapterTask] == [
        "extract_business_overview",
        "extract_segment_financials",
        "extract_operating_quantities",
        "extract_material_inputs",
        "extract_counterparties_and_concentration",
        "extract_business_regime",
    ]
    manifest = core_assessment_schema_manifest()
    assert manifest["mapping_version"] == COMMON_CORE_MAPPING_VERSION
    assert tuple(manifest["dimension_ids"]) == (
        "principal_business",
        "products_services",
        "revenue_model",
    )
    assert "extract_revenue_model" not in json.dumps(manifest)


def test_one_overview_does_not_auto_answer_all_three_dimensions():
    report, result = _overview_only_result()
    assessment = project_core_assessment(report=report, task_results=(result,))
    assert assessment.mapping_version == COMMON_CORE_MAPPING_VERSION
    assert assessment.principal_business.answered is True
    assert "cp-300750-overview" in assessment.principal_business.supporting_record_ids
    assert assessment.products_services.answered is True
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "overview_lacks_dimension"
    assert assessment.core_complete is False


def test_company_wide_revenue_total_does_not_establish_revenue_model():
    payload, report, result = _reference_bundle()
    total = deepcopy(
        next(item for item in payload["records"] if item["record_id"] == "cp-300750-revenue")
    )
    total["record_id"] = "cp-300750-total-revenue"
    total["measured_object"] = "公司合计"
    total["segment_dimension"] = None
    total["segment_label"] = None
    total["source_native"] = {
        "name": "营业总收入",
        "value": "999",
        "unit": "千元",
        "header": "营业总收入",
    }
    keep = {"cp-300750-overview"}
    records = [item for item in result.records if item.record_id in keep]
    records.append(_record(total))
    dispositions = [item for item in result.dispositions if item.target_id in keep]
    dispositions.append(
        Disposition(
            target_id="cp-300750-total-revenue",
            field_id="operating_revenue",
            status=DispositionStatus.ACCEPTED_FOR_REVIEW,
        )
    )
    isolated = CompanyProfileTaskResult(
        request_id="total-only",
        records=tuple(records),
        dispositions=tuple(dispositions),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    assessment = project_core_assessment(report=report, task_results=(isolated,))
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "numeric_total_only"


def test_segment_operating_revenue_answers_revenue_model_independently():
    _payload, report, result = _reference_bundle()
    assessment = project_core_assessment(report=report, task_results=(result,))
    assert assessment.principal_business.answered is True
    assert assessment.products_services.answered is True
    assert assessment.revenue_model.answered is True
    assert "cp-300750-revenue" in assessment.revenue_model.supporting_record_ids
    assert assessment.revenue_model.evidence_ids
    assert assessment.core_complete is True


def _accepted_records(report, records):
    return CompanyProfileTaskResult(
        request_id="review-counterexample",
        records=tuple(records),
        dispositions=tuple(
            Disposition(
                target_id=record.record_id,
                field_id=record.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            )
            for record in records
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )


def _overview_record(source_text: str, record_id: str = "cp-review-overview"):
    payload, report, _result = _reference_bundle()
    item = deepcopy(
        next(row for row in payload["records"] if row["record_id"] == "cp-300750-overview")
    )
    item["record_id"] = record_id
    item["source_text"] = source_text
    item["evidence"][0]["anchor"]["bounded_quote"] = source_text
    return report, _record(item)


def _segment_record(**overrides):
    payload, _report, _result = _reference_bundle()
    item = deepcopy(
        next(row for row in payload["records"] if row["record_id"] == "cp-300750-segment")
    )
    item.update(overrides)
    return _record(item)


def _activity_record(**overrides):
    payload, _report, _result = _reference_bundle()
    item = deepcopy(
        next(row for row in payload["records"] if row["record_id"] == "cp-300750-produces")
    )
    item.update(overrides)
    return _record(item)


def _revenue_record(**overrides):
    payload, _report, _result = _reference_bundle()
    item = deepcopy(
        next(row for row in payload["records"] if row["record_id"] == "cp-300750-revenue")
    )
    item.update(overrides)
    return _record(item)


def test_bank_fee_expense_does_not_answer_revenue_model():
    report, overview = _overview_record(
        "公司主要从事电池研发、生产和销售。报告期内支付银行手续费100万元。"
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.principal_business.answered is True
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "overview_lacks_dimension"


def test_sales_proceeds_narrative_answers_revenue_model():
    report, overview = _overview_record(
        "主要产品为工业设备，通过向客户销售设备取得货款"
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.products_services.answered is True
    assert assessment.revenue_model.answered is True
    assert assessment.revenue_model.supporting_record_ids == (overview.record_id,)


def test_fee_income_clause_still_answers_revenue_model():
    report, overview = _overview_record(
        "公司营业收入主要来源于向客户收取的技术服务费。"
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.revenue_model.answered is True


def test_free_customer_service_does_not_answer_revenue_model():
    report, overview = _overview_record("向客户提供免费安装服务")
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "overview_lacks_dimension"


def test_third_party_commission_does_not_answer_revenue_model():
    report, overview = _overview_record(
        "销售代理商向公司收取佣金，公司支付相关费用"
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "overview_lacks_dimension"


def test_free_add_on_does_not_veto_sales_proceeds():
    report, overview = _overview_record(
        "通过向客户销售设备取得货款，并提供免费安装服务"
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.revenue_model.answered is True


def test_negated_receipt_alone_does_not_answer_revenue_model():
    report, overview = _overview_record("报告期内尚未取得货款")
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [overview]),),
    )
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "overview_lacks_dimension"


def test_unpaid_receivable_does_not_drop_established_revenue_model():
    report, sales = _overview_record(
        "通过向客户销售设备取得货款，报告期内尚未回款",
        record_id="cp-review-sales",
    )
    _report, unpaid = _overview_record(
        "报告期内尚未取得货款",
        record_id="cp-review-unpaid",
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [sales, unpaid]),),
    )
    assert assessment.revenue_model.answered is True
    assert sales.record_id in assessment.revenue_model.supporting_record_ids
    assert unpaid.record_id not in assessment.revenue_model.supporting_record_ids


def test_region_segment_does_not_answer_products_services():
    report, _overview = _overview_record("公司主要从事电池研发、生产和销售。")
    region = _segment_record(
        record_id="cp-review-region",
        dimension="分地区",
        label="境内",
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [region]),),
    )
    assert assessment.products_services.answered is False
    assert assessment.products_services.missing_reason == "no_qualifying_source"


def test_purchase_activity_does_not_answer_products_services():
    _payload, report, _result = _reference_bundle()
    purchase = _activity_record(
        record_id="cp-review-purchase",
        action="purchases",
        object_name="碳酸锂",
        source_verb="采购",
        source_native={"name": "碳酸锂", "header": "主要业务"},
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [purchase]),),
    )
    assert assessment.products_services.answered is False
    assert assessment.products_services.missing_reason == "no_qualifying_source"


def test_elimination_revenue_does_not_answer_revenue_model():
    _payload, report, _result = _reference_bundle()
    elimination = _revenue_record(
        record_id="cp-review-elimination",
        measured_object="分部间抵销",
        segment_dimension="adjustment",
        segment_label="抵销",
        row_class="consolidation_adjustment",
        source_native={
            "name": "分部间抵销",
            "value": "-100",
            "unit": "千元",
            "header": "营业收入",
        },
    )
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [elimination]),),
    )
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "no_qualifying_source"


def test_blocked_activity_does_not_drop_accepted_overview():
    report, result = _overview_only_result()
    activity = _activity_record(record_id="blocked-activity")
    mixed = CompanyProfileTaskResult(
        request_id="overview-keeps",
        records=result.records + (activity,),
        dispositions=result.dispositions
        + (
            Disposition(
                target_id=activity.record_id,
                field_id=activity.field_id,
                status=DispositionStatus.BLOCKED,
                reason_codes=(ContractErrorCode.ACTION_NOT_ALLOWED,),
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=False,
    )
    assessment = project_core_assessment(report=report, task_results=(mixed,))
    view = project_research_view(
        company_name="宁德时代",
        report=report,
        task_results=(mixed,),
    )
    assert assessment.principal_business.answered is True
    assert "cp-300750-overview" in assessment.principal_business.supporting_record_ids
    assert view.business_overview is not None
    assert view.business_overview.record_id == "cp-300750-overview"
    assert activity.record_id not in {
        item.record_id for item in view.activities
    }


def test_numeric_rows_without_principal_business_are_not_a_complete_core():
    _payload, report, _result = _reference_bundle()
    revenue = _revenue_record(record_id="only-revenue")
    assessment = project_core_assessment(
        report=report,
        task_results=(_accepted_records(report, [revenue]),),
    )
    assert assessment.principal_business.answered is False
    assert assessment.revenue_model.answered is True
    assert assessment.core_complete is False


def test_research_view_keeps_distinct_coverage_gaps():
    _payload, report, _result = _reference_bundle()
    battery = _coverage_row(report, evidence_id="battery-sales", row_label="动力电池")
    storage = _coverage_row(
        report,
        evidence_id="storage-sales",
        row_label="储能电池",
        status=CoverageStatus.UNCLEAR,
        reason_code=CoverageReasonCode.REQUIRED_RESULT_MISSING,
    )
    result = CompanyProfileTaskResult(
        request_id="coverage-gaps",
        records=(),
        dispositions=(),
        coverage=(battery, storage),
        human_review_items=(),
        task_complete=False,
    )
    view = project_research_view(
        company_name="宁德时代",
        report=report,
        task_results=(result,),
    )
    statuses = {item["status"] for item in view.coverage}
    objects = {
        item["evidence"][0]["anchor"]["row_label"]
        for item in view.coverage
        if item["field_id"] == "sales_volume"
    }
    assert statuses == {"observed", "unclear"}
    assert objects == {"动力电池", "储能电池"}


def test_blocked_overview_does_not_support_any_dimension():
    report, result = _overview_only_result()
    blocked = CompanyProfileTaskResult(
        request_id="blocked",
        records=result.records,
        dispositions=(
            Disposition(
                target_id="cp-300750-overview",
                field_id="business_overview_source",
                status=DispositionStatus.BLOCKED,
                reason_codes=(ContractErrorCode.SOURCE_VALUE_MUTATION,),
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    assessment = project_core_assessment(report=report, task_results=(blocked,))
    assert assessment.principal_business.answered is False
    assert assessment.products_services.answered is False
    assert assessment.revenue_model.answered is False
    assert assessment.principal_business.missing_reason == "no_accepted_evidence"
    assert assessment.core_complete is False


def _coverage_row(
    report,
    *,
    evidence_id: str,
    row_label: str,
    status=CoverageStatus.OBSERVED,
    reason_code=None,
):
    evidence = _revenue_record().evidence[0].model_copy(
        update={
            "evidence_id": evidence_id,
            "anchor": TableAnchor(
                row_label=row_label,
                column_header="销售量",
                cell_locator=f"page49/{row_label}/销售量",
            ),
        }
    )
    return CoverageResult(
        field_id="sales_volume",
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        requirement_level=RequirementLevel.CONDITIONAL,
        status=status,
        reason_code=reason_code,
        evidence=(evidence,),
    )
