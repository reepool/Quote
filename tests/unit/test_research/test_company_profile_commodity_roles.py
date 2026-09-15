from __future__ import annotations

import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.commodity_exposure import (
    COMMODITY_EXPOSURE_POLICY_VERSION,
    COMMODITY_ROLE_RULE_VERSION,
    AssessmentStatus,
    CommodityExposure,
    CommodityExposureAssessment,
    CommodityRole,
    MappingStatus,
    MarketLinkStatus,
    derive_commodity_role,
    project_commodity_exposures,
)
from research.company_profile.models import (
    Activity,
    ActivityAction,
    AssertionClass,
    BusinessEvent,
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
from research.company_profile.runtime import _FIELD_CONTRACT
from research.company_profile.stage5_service import (
    _FIELD_CONTRACT as STAGE5_FIELDS,
)


def _report() -> ReportIdentity:
    return ReportIdentity(
        instrument_id="000878.SZ",
        report_id="asset-commodity-3.2",
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
    object_name: str = "阴极铜",
    *,
    action: ActivityAction = ActivityAction.SELLS,
    record_id: str = "act-role",
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


def _relationship(
    object_name: str = "铜精矿",
    *,
    record_id: str = "rel-role",
    source_verb: str | None = None,
) -> Relationship:
    native = SourceNativeValue(name=object_name)
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
        source_native=native,
        relation_type=RelationshipType.MATERIAL_INPUT,
        object_name=object_name,
    )


def _measurement(
    object_name: str = "阴极铜",
    *,
    record_id: str = "meas-role",
    metric: MetricType = MetricType.SALES_VOLUME,
    slot: LogicalSlot = LogicalSlot.SALES_VOLUME,
    value: str = "10",
) -> Measurement:
    return Measurement(
        record_id=record_id,
        field_id="sales_volume",
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(object_name),),
        source_native=SourceNativeValue(name=object_name, value=value, unit="万吨"),
        metric_type=metric,
        logical_slot=slot,
        measured_object=object_name,
    )


def _hedge_event(object_name: str = "阴极铜") -> BusinessEvent:
    return BusinessEvent(
        record_id="evt-hedge",
        field_id="business_regime_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.EVENT,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence("套保"),),
        source_native=SourceNativeValue(name=object_name),
        event_type="commodity_hedge",
        description="阴极铜期货套保",
    )


def test_sales_activity_derives_product_sales_without_profit_direction():
    activity = _activity()
    quantity = _measurement()
    exposures = project_commodity_exposures((activity, quantity))

    assert len(exposures) == 1
    exposure = exposures[0]
    dumped = json.loads(exposure.model_dump_json())
    assert exposure.role == CommodityRole.PRODUCT_SALES
    assert exposure.assertion_class == AssertionClass.DETERMINISTIC_DERIVATION.value
    assert exposure.policy_version == COMMODITY_EXPOSURE_POLICY_VERSION
    assert COMMODITY_ROLE_RULE_VERSION in exposure.mapping_version
    assert exposure.commodity_id == "COMMODITY.metal.refined_copper"
    assert exposure.mapping_status == MappingStatus.MAPPED
    assert exposure.measurement_record_ids == ("meas-role",)
    assert exposure.market_link_status == MarketLinkStatus.NOT_LINKED
    assert "net_profit_direction" not in dumped
    assert "source_native" not in dumped
    assert dumped.get("measurement_record_ids") == ["meas-role"]
    with pytest.raises(ValidationError):
        CommodityExposure.model_validate({**dumped, "net_profit_direction": "positive"})


def test_material_input_without_quantity_is_raw_material_not_energy():
    material = _relationship("铜精矿")
    exposures = project_commodity_exposures((material,))

    assert len(exposures) == 1
    exposure = exposures[0]
    assert exposure.role == CommodityRole.RAW_MATERIAL_INPUT
    assert exposure.measurement_record_ids == ()
    assert exposure.commodity_id == "COMMODITY.metal.copper_concentrate"
    assert derive_commodity_role(material) == CommodityRole.RAW_MATERIAL_INPUT


def test_energy_input_uses_consumption_evidence_not_catalog_family():
    power = _relationship("电力", record_id="rel-power")
    oil_sale = _activity("原油", record_id="act-oil")
    exposures = project_commodity_exposures((power, oil_sale))
    roles = {item.source_native_name: item.role for item in exposures}

    assert roles["电力"] == CommodityRole.ENERGY_CONSUMPTION
    assert roles["原油"] == CommodityRole.PRODUCT_SALES
    assert "energy_consumption" not in STAGE5_FIELDS
    assert "energy_consumption" not in _FIELD_CONTRACT


def test_hedge_event_is_hedge_underlying_and_plain_event_is_not():
    hedge = _hedge_event()
    plain = BusinessEvent(
        record_id="evt-plain",
        field_id="business_regime_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        report=_report(),
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.EVENT,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence("扩建"),),
        source_native=SourceNativeValue(name="阴极铜"),
        event_type="capacity_expansion",
        description="阴极铜产能扩建",
    )
    exposures = project_commodity_exposures((hedge, plain))

    assert [item.role for item in exposures] == [CommodityRole.HEDGE_UNDERLYING]
    assert exposures[0].source_record_ids == ("evt-hedge",)
    assert derive_commodity_role(plain) is None


def test_industry_common_sense_and_production_do_not_invent_roles():
    common = _activity("行业常识金属", action=ActivityAction.OPERATES, source_verb="经营")
    produces = _activity("阴极铜", action=ActivityAction.PRODUCES, source_verb="生产")

    assert derive_commodity_role(common) is None
    assert derive_commodity_role(produces) is None
    assert project_commodity_exposures((common, produces)) == ()


def test_quantity_only_sales_measurement_can_carry_product_sales_role():
    quantity = _measurement("阴极铜")
    exposures = project_commodity_exposures((quantity,))

    assert len(exposures) == 1
    assert exposures[0].role == CommodityRole.PRODUCT_SALES
    assert exposures[0].source_record_ids == ("meas-role",)
    assert exposures[0].measurement_record_ids == ("meas-role",)


def test_roles_are_not_converted_into_assessment_profit_or_zero_exposure():
    exposures = project_commodity_exposures((_activity(),))
    assessment = CommodityExposureAssessment(
        report=_report(),
        assessment_status=AssessmentStatus.ASSESSED,
        checked_evidence_ids=("ev-阴极铜",),
        exposures=exposures,
    )
    dumped = json.loads(assessment.model_dump_json())

    assert assessment.exposures[0].role == CommodityRole.PRODUCT_SALES
    assert "net_profit_direction" not in dumped
    assert dumped["exposures"][0]["role"] != "positive"


def test_role_derivation_does_not_reuse_legacy_exposure_producer():
    module_source = inspect.getsource(
        __import__(
            "research.company_profile.commodity_exposure",
            fromlist=["project_commodity_exposures"],
        )
    )
    role_source = inspect.getsource(derive_commodity_role)
    project_source = inspect.getsource(project_commodity_exposures)
    assert "business_profile_exposure_production" not in module_source
    assert "positive" not in role_source
    assert "feedstock_cost" not in role_source
    assert "energy_cost" not in project_source
