from __future__ import annotations

import json

import pytest

from research.company_profile.commodity_exposure import (
    AssessmentStatus,
    CommodityExposureAssessment,
    CommodityRole,
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
from research.company_profile.reads import (
    COMMODITY_CONSUMER_AUTHORIZATION,
    _commodity_exposure_view,
    commodity_consumer_authorized,
)
from research.company_profile.runtime import json_compatible


def _report() -> ReportIdentity:
    return ReportIdentity(
        instrument_id="000878.SZ",
        report_id="asset-commodity-3.4",
        document_version="ver-1",
        report_period="2025-12-31",
        published_at="2026-03-20T08:00:00+08:00",
    )


def _evidence(quote: str) -> Evidence:
    return Evidence(
        evidence_id=f"ev-{quote}",
        report=_report(),
        page=20,
        section_title="主营业务",
        anchor=TextAnchor(bounded_quote=quote),
    )


def _sale() -> Activity:
    return Activity(
        record_id="act-sale",
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        knowledge_time="2026-03-20T08:00:00+08:00",
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence("阴极铜"),),
        source_native=SourceNativeValue(name="阴极铜"),
        action=ActivityAction.SELLS,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name="阴极铜",
        source_verb="销售",
    )


def _purchase() -> Activity:
    return Activity(
        record_id="act-buy",
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        knowledge_time="2026-03-20T08:00:00+08:00",
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence("采购阴极铜"),),
        source_native=SourceNativeValue(name="阴极铜"),
        action=ActivityAction.PURCHASES,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name="阴极铜",
        source_verb="采购",
    )


def _material() -> Relationship:
    return Relationship(
        record_id="rel-feed",
        field_id="material_input",
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        knowledge_time="2026-03-20T08:00:00+08:00",
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence("铜精矿"),),
        source_native=SourceNativeValue(name="铜精矿"),
        relation_type=RelationshipType.MATERIAL_INPUT,
        object_name="铜精矿",
    )


def test_query_profile_keeps_commodity_period_and_denies_sensitive_consumers():
    checkpoint = {
        "accepted_records": [
            json_compatible(_sale()),
            json_compatible(_purchase()),
            json_compatible(_material()),
        ]
    }
    view = _commodity_exposure_view(
        report=_report(),
        checkpoint=checkpoint,
        knowledge_time="2026-03-20T08:00:00+08:00",
    )
    assessment = CommodityExposureAssessment.model_validate_json(
        json.dumps(view["assessment"])
    )
    roles = {item.role for item in assessment.exposures}

    assert view["reported_period"] == "2025-12-31"
    assert view["knowledge_time"] == "2026-03-20T08:00:00+08:00"
    assert assessment.assessment_status == AssessmentStatus.ASSESSED
    assert roles == {
        CommodityRole.PRODUCT_SALES,
        CommodityRole.RAW_MATERIAL_INPUT,
    }
    assert all(item.reported_period == "2025" for item in assessment.exposures)
    assert all(
        item.knowledge_time == "2026-03-20T08:00:00+08:00"
        for item in assessment.exposures
    )
    assert view["consumer_authorization"] == COMMODITY_CONSUMER_AUTHORIZATION
    assert commodity_consumer_authorized("dcf") is False
    assert commodity_consumer_authorized("trading") is False
    assert commodity_consumer_authorized("price_sensitivity") is False
    with pytest.raises(ValueError, match="unsupported commodity consumer"):
        commodity_consumer_authorized("research_display")
    dumped = json.dumps(view)
    assert "net_profit_direction" not in dumped
    assert "zero_exposure" not in dumped
    assert "accepted_records" not in view["assessment"]


def test_empty_checkpoint_is_unread_not_zero_and_still_unauthorized():
    view = _commodity_exposure_view(
        report=_report(),
        checkpoint={},
        knowledge_time="2026-03-20T08:00:00+08:00",
    )
    assessment = CommodityExposureAssessment.model_validate_json(
        json.dumps(view["assessment"])
    )

    assert assessment.assessment_status == AssessmentStatus.NOT_ASSESSED
    assert assessment.exposures == ()
    assert assessment.checked_evidence_ids == ()
    assert view["consumer_authorization"]["dcf"] is False
    assert view["consumer_authorization"]["trading"] is False
    assert view["consumer_authorization"]["price_sensitivity"] is False
