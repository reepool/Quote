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
    CoverageResult,
    ReportIdentity,
    SemanticRecord,
)

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
