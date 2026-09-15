from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from research.company_profile.commodity_exposure import (
    AssessmentStatus,
    CommodityExposureAssessment,
    CommodityRole,
    MappingStatus,
    MarketLinkStatus,
    assess_commodity_exposures,
    commodity_exposure_schema_manifest,
)
from research.company_profile.models import (
    Activity,
    ActivityAction,
    AssertionClass,
    ChapterTask,
    Evidence,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.runtime import _FIELD_CONTRACT
from research.company_profile.stage5_service import (
    _FIELD_CONTRACT as STAGE5_FIELDS,
)


def _report(*, report_id: str = "asset-commodity-3.3") -> ReportIdentity:
    return ReportIdentity(
        instrument_id="000878.SZ",
        report_id=report_id,
        document_version="ver-1",
        report_period="2025-12-31",
        published_at="2026-03-20T08:00:00+08:00",
    )


def _evidence(quote: str = "阴极铜") -> Evidence:
    return Evidence(
        evidence_id=f"ev-{quote}",
        report=_report(),
        page=20,
        section_title="主营业务",
        anchor=TextAnchor(bounded_quote=quote),
    )


def _activity(
    object_name: str,
    *,
    action: ActivityAction = ActivityAction.SELLS,
    record_id: str,
    source_verb: str = "销售",
) -> Activity:
    return Activity(
        record_id=record_id,
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(object_name),),
        source_native=SourceNativeValue(name=object_name),
        action=action,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name=object_name,
        source_verb=source_verb,
    )


def _relationship(object_name: str, *, record_id: str) -> Relationship:
    return Relationship(
        record_id=record_id,
        field_id="material_input",
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(object_name),),
        source_native=SourceNativeValue(name=object_name),
        relation_type=RelationshipType.MATERIAL_INPUT,
        object_name=object_name,
    )


def _assess(
    records: tuple[Activity | Relationship, ...],
    *,
    status: str = AssessmentStatus.ASSESSED,
    evidence: tuple[Evidence, ...] | None = None,
) -> CommodityExposureAssessment:
    checked = evidence if evidence is not None else tuple(
        item.evidence[0] for item in records
    )
    return assess_commodity_exposures(
        report=_report(),
        assessment_status=status,
        accepted_records=records,
        checked_evidence=checked,
    )


def _assert_not_zero_claim(assessment: CommodityExposureAssessment) -> None:
    dumped = json.loads(assessment.model_dump_json())
    assert "zero_exposure" not in dumped
    assert "net_exposure" not in dumped
    assert "net_profit_direction" not in dumped
    CommodityExposureAssessment.model_validate_json(json.dumps(dumped))


def test_sales_and_input_roles_coexist_without_netting():
    sale = _activity("阴极铜", record_id="act-sale")
    purchase = _activity(
        "阴极铜",
        action=ActivityAction.PURCHASES,
        record_id="act-buy",
        source_verb="采购",
    )
    assessment = _assess((sale, purchase))
    roles = {item.role for item in assessment.exposures}

    assert assessment.assessment_status == AssessmentStatus.ASSESSED
    assert len(assessment.exposures) == 2
    assert roles == {
        CommodityRole.PRODUCT_SALES,
        CommodityRole.RAW_MATERIAL_INPUT,
    }
    assert {item.commodity_id for item in assessment.exposures} == {
        "COMMODITY.metal.refined_copper"
    }
    assert all(
        item.market_link_status == MarketLinkStatus.NOT_LINKED
        for item in assessment.exposures
    )
    _assert_not_zero_claim(assessment)


def test_ambiguous_mapping_is_listed_separately_without_guessing():
    sale = _activity("阴极铜", record_id="act-sale")
    vague = _activity("铜产品", record_id="act-vague")
    assessment = _assess((sale, vague))
    by_name = {item.source_native_name: item for item in assessment.exposures}

    assert set(by_name) == {"阴极铜", "铜产品"}
    assert by_name["阴极铜"].mapping_status == MappingStatus.MAPPED
    assert by_name["阴极铜"].commodity_id == "COMMODITY.metal.refined_copper"
    assert by_name["铜产品"].mapping_status == MappingStatus.AMBIGUOUS
    assert by_name["铜产品"].commodity_id is None
    assert by_name["铜产品"].role == CommodityRole.PRODUCT_SALES


def test_unlinked_market_does_not_drop_an_established_association():
    material = _relationship("铜精矿", record_id="rel-feed")
    assessment = _assess((material,))
    exposure = assessment.exposures[0]
    dumped = json.loads(exposure.model_dump_json())

    assert exposure.role == CommodityRole.RAW_MATERIAL_INPUT
    assert exposure.market_link_status == MarketLinkStatus.NOT_LINKED
    assert exposure.market_series_id is None
    assert dumped["market_link_status"] == MarketLinkStatus.NOT_LINKED
    assert len(assessment.exposures) == 1


def test_empty_assessment_states_are_not_zero_exposure():
    common = _activity(
        "行业常识金属",
        action=ActivityAction.OPERATES,
        record_id="act-common",
        source_verb="经营",
    )
    unread = _assess((common,), status=AssessmentStatus.NOT_ASSESSED, evidence=())
    none_found = _assess((common,))
    failed = _assess(
        (common,),
        status=AssessmentStatus.EXTRACTION_FAILED,
        evidence=(),
    )

    assert unread.assessment_status == AssessmentStatus.NOT_ASSESSED
    assert unread.exposures == ()
    assert unread.checked_evidence_ids == ()
    assert none_found.assessment_status == AssessmentStatus.ASSESSED
    assert none_found.exposures == ()
    assert none_found.checked_evidence_ids == ("ev-行业常识金属",)
    assert failed.assessment_status == AssessmentStatus.EXTRACTION_FAILED
    assert failed.exposures == ()
    for item in (unread, none_found, failed):
        _assert_not_zero_claim(item)
    with pytest.raises(ValidationError, match="checked evidence"):
        assess_commodity_exposures(
            report=_report(),
            assessment_status=AssessmentStatus.ASSESSED,
            accepted_records=(common,),
            checked_evidence=(),
        )


def test_unread_or_failed_status_does_not_publish_a_completed_list():
    sale = _activity("阴极铜", record_id="act-sale")
    unread = _assess((sale,), status=AssessmentStatus.NOT_ASSESSED)
    failed = _assess((sale,), status=AssessmentStatus.EXTRACTION_FAILED, evidence=())

    assert unread.exposures == ()
    assert unread.checked_evidence_ids == ()
    assert failed.exposures == ()
    _assert_not_zero_claim(unread)
    _assert_not_zero_claim(failed)


def test_published_json_rejects_contradictory_assessment_states():
    sale = _activity("阴极铜", record_id="act-sale")
    assessed = _assess((sale,))
    payload = json.loads(assessed.model_dump_json())
    unread_with_exposures = {**payload, "assessment_status": "not_assessed"}
    unread_with_checked = {
        **payload,
        "assessment_status": "not_assessed",
        "exposures": [],
    }
    failed_with_exposures = {**payload, "assessment_status": "extraction_failed"}

    with pytest.raises(ValidationError, match="not_assessed"):
        CommodityExposureAssessment.model_validate_json(
            json.dumps(unread_with_exposures)
        )
    with pytest.raises(ValidationError, match="not_assessed"):
        CommodityExposureAssessment.model_validate_json(
            json.dumps(unread_with_checked)
        )
    with pytest.raises(ValidationError, match="extraction_failed"):
        CommodityExposureAssessment.model_validate_json(
            json.dumps(failed_with_exposures)
        )
    CommodityExposureAssessment.model_validate_json(json.dumps(payload))
    CommodityExposureAssessment.model_validate_json(
        json.dumps(
            {
                **payload,
                "assessment_status": "not_assessed",
                "exposures": [],
                "checked_evidence_ids": [],
            }
        )
    )
    CommodityExposureAssessment.model_validate_json(
        json.dumps(
            {
                **payload,
                "assessment_status": "extraction_failed",
                "exposures": [],
            }
        )
    )


def test_assessment_stays_id_only_and_off_stage5():
    sale = _activity("阴极铜", record_id="act-sale")
    assessment = _assess((sale,))
    dumped = json.loads(assessment.model_dump_json())
    manifest = commodity_exposure_schema_manifest()

    assert "accepted_records" not in dumped
    assert "checked_evidence" not in dumped
    assert dumped["exposures"][0]["source_record_ids"] == ["act-sale"]
    assert manifest["empty_association_is_not_zero_exposure"] is True
    assert "energy_consumption" not in STAGE5_FIELDS
    assert "hedge_underlying" not in STAGE5_FIELDS
    assert "energy_consumption" not in _FIELD_CONTRACT
    assert "hedge_underlying" not in _FIELD_CONTRACT
