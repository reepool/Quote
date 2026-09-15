from __future__ import annotations

import inspect
import json

from research.company_profile.commodity_exposure import (
    COMMODITY_EXPOSURE_WRITER,
    AssessmentStatus,
    CommodityRole,
    MappingStatus,
    MarketLinkStatus,
    assess_commodity_exposures,
    commodity_exposure_schema_manifest,
    commodity_publication_owner,
    derive_commodity_role,
    project_commodity_exposures,
)
from research.company_profile.models import (
    PRODUCTION_AUTHORIZATION,
    Activity,
    ActivityAction,
    AssertionClass,
    ChapterTask,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import (
    COMMON_CORE_WRITER_NAME,
    CompanyProfileResearchWriter,
)


def _report() -> ReportIdentity:
    return ReportIdentity(
        instrument_id="000878.SZ",
        report_id="asset-commodity-3.5",
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


def _activity(
    object_name: str,
    *,
    record_id: str,
    action: ActivityAction = ActivityAction.SELLS,
    source_verb: str = "销售",
    reported_period: str = "2025",
) -> Activity:
    return Activity(
        record_id=record_id,
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period=reported_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(record_id),),
        source_native=SourceNativeValue(name=object_name),
        action=action,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name=object_name,
        source_verb=source_verb,
    )


def _material(*, record_id: str = "rel-feed", reported_period: str = "2025") -> Relationship:
    return Relationship(
        record_id=record_id,
        field_id="material_input",
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period=reported_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(record_id),),
        source_native=SourceNativeValue(name="铜精矿"),
        relation_type=RelationshipType.MATERIAL_INPUT,
        object_name="铜精矿",
    )


def _volume(*, record_id: str, reported_period: str) -> Measurement:
    return Measurement(
        record_id=record_id,
        field_id="sales_volume",
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period=reported_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(record_id),),
        source_native=SourceNativeValue(name="阴极铜", value="10", unit="万吨"),
        metric_type=MetricType.SALES_VOLUME,
        logical_slot=LogicalSlot.SALES_VOLUME,
        measured_object="阴极铜",
    )


def test_copper_input_without_quantity_or_market_still_associates():
    material = _material()
    exposures = project_commodity_exposures((material,))

    assert len(exposures) == 1
    exposure = exposures[0]
    assert exposure.role == CommodityRole.RAW_MATERIAL_INPUT
    assert exposure.commodity_id == "COMMODITY.metal.copper_concentrate"
    assert exposure.mapping_status == MappingStatus.MAPPED
    assert exposure.measurement_record_ids == ()
    assert exposure.market_link_status == MarketLinkStatus.NOT_LINKED
    assert exposure.market_series_id is None
    assert "net_profit_direction" not in json.loads(exposure.model_dump_json())


def test_industry_common_sense_cannot_create_a_commodity_association():
    common = _activity(
        "行业常识金属",
        record_id="act-common",
        action=ActivityAction.OPERATES,
        source_verb="经营",
    )
    produces = _activity(
        "阴极铜",
        record_id="act-produce",
        action=ActivityAction.PRODUCES,
        source_verb="生产",
    )

    assert derive_commodity_role(common) is None
    assert derive_commodity_role(produces) is None
    assert project_commodity_exposures((common, produces)) == ()


def test_sales_and_purchases_are_kept_without_netting():
    sale = _activity("阴极铜", record_id="act-sale")
    purchase = _activity(
        "阴极铜",
        record_id="act-buy",
        action=ActivityAction.PURCHASES,
        source_verb="采购",
    )
    assessment = assess_commodity_exposures(
        report=_report(),
        assessment_status=AssessmentStatus.ASSESSED,
        accepted_records=(sale, purchase),
        checked_evidence=(sale.evidence[0], purchase.evidence[0]),
    )
    dumped = json.loads(assessment.model_dump_json())

    assert {item.role for item in assessment.exposures} == {
        CommodityRole.PRODUCT_SALES,
        CommodityRole.RAW_MATERIAL_INPUT,
    }
    assert {item.commodity_id for item in assessment.exposures} == {
        "COMMODITY.metal.refined_copper"
    }
    assert len(assessment.exposures) == 2
    assert "net_exposure" not in dumped
    assert "net_profit_direction" not in dumped


def test_different_reported_periods_are_not_merged():
    prior_input = _material(record_id="rel-2024", reported_period="2024")
    current_input = _material(record_id="rel-2025", reported_period="2025")
    current_sale = _activity("阴极铜", record_id="act-2025", reported_period="2025")
    prior_volume = _volume(record_id="meas-2024", reported_period="2024")
    exposures = project_commodity_exposures(
        (prior_input, current_input, current_sale, prior_volume)
    )
    by_source = {item.source_record_ids: item for item in exposures}

    assert len(exposures) == 4
    assert {item.reported_period for item in exposures} == {"2024", "2025"}
    assert by_source[("rel-2024",)].exposure_id != by_source[("rel-2025",)].exposure_id
    assert by_source[("act-2025",)].measurement_record_ids == ()
    assert by_source[("meas-2024",)].role == CommodityRole.PRODUCT_SALES
    assert by_source[("meas-2024",)].reported_period == "2024"
    assert "meas-2024" not in by_source[("act-2025",)].measurement_record_ids


def test_commodity_publication_has_one_unauthorized_new_contract_owner():
    owner = commodity_publication_owner()
    manifest = commodity_exposure_schema_manifest()
    module_source = inspect.getsource(
        __import__(
            "research.company_profile.commodity_exposure",
            fromlist=["commodity_publication_owner"],
        )
    )

    assert owner["writer"] == COMMON_CORE_WRITER_NAME == COMMODITY_EXPOSURE_WRITER
    assert owner["writer"] == manifest["writer"]
    assert owner["read_owner"] == CompanyProfileReadService.read_owner
    assert owner["read_owner"] == manifest["read_owner"]
    assert owner["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert owner["legacy_writer"] is None
    assert owner["legacy_producer"] is None
    assert manifest["legacy_writer"] is None
    assert CompanyProfileResearchWriter.writer_name == owner["writer"]
    assert "business_profile_exposure_production" not in module_source
    assert "def persist" not in module_source
    assert "sqlite" not in module_source
