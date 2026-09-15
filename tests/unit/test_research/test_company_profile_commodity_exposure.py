from __future__ import annotations

import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.commodity_exposure import (
    COMMODITY_EXPOSURE_SCHEMA_VERSION,
    AssessmentStatus,
    CommodityExposure,
    CommodityExposureAssessment,
    CommodityRole,
    MappingStatus,
    MarketLinkStatus,
    build_exposure_id,
    commodity_exposure_schema_manifest,
    measurement_record_ids,
    resolve_catalog_mapping,
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
    ObjectType,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
    semantic_record_json_schema,
)
from research.company_profile.runtime import _FIELD_CONTRACT
from research.company_profile.stage5_service import (
    _FIELD_CONTRACT as STAGE5_FIELDS,
)
from research.company_profile.stage5_service import (
    _validate_prepared_field_contract,
)


def _report() -> ReportIdentity:
    return ReportIdentity(
        instrument_id="000878.SZ",
        report_id="asset-commodity-3.1",
        document_version="ver-1",
        report_period="2025-12-31",
        published_at="2026-03-20T08:00:00+08:00",
    )


def _evidence() -> Evidence:
    report = _report()
    return Evidence(
        evidence_id="ev-commodity",
        report=report,
        page=20,
        section_title="主营业务",
        anchor=TextAnchor(bounded_quote="阴极铜"),
    )


def _activity(object_name: str = "阴极铜") -> Activity:
    report = _report()
    return Activity(
        record_id="act-copper",
        field_id="explicit_activity",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=report,
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(),),
        source_native=SourceNativeValue(name=object_name),
        action=ActivityAction.SELLS,
        activity_actor="公司",
        source_actor="公司",
        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
        object_name=object_name,
        source_verb="销售",
    )


def _relationship(object_name: str = "铜精矿") -> Relationship:
    report = _report()
    return Relationship(
        record_id="rel-copper",
        field_id="material_input",
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        report=report,
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(),),
        source_native=SourceNativeValue(name=object_name),
        relation_type=RelationshipType.MATERIAL_INPUT,
        object_name=object_name,
    )


def _measurement() -> Measurement:
    report = _report()
    return Measurement(
        record_id="meas-copper",
        field_id="sales_volume",
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        report=report,
        subject_scope=SubjectScope.ISSUER,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(),),
        source_native=SourceNativeValue(name="阴极铜", value="10", unit="万吨"),
        metric_type=MetricType.SALES_VOLUME,
        logical_slot=LogicalSlot.SALES_VOLUME,
        measured_object="阴极铜",
    )


def test_schema_manifest_registers_projection_without_stage5_extract_ids():
    manifest = commodity_exposure_schema_manifest()
    schema = semantic_record_json_schema()

    assert manifest["schema_version"] == COMMODITY_EXPOSURE_SCHEMA_VERSION
    assert manifest["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert manifest["stage5_object_types"] == ()
    assert manifest["stage5_field_ids"] == ()
    assert CommodityRole.ENERGY_CONSUMPTION in manifest["roles"]
    assert CommodityRole.HEDGE_UNDERLYING in manifest["roles"]
    assert "CommodityExposure" not in ObjectType.__members__
    assert "CommodityExposure" not in json.dumps(schema)
    assert "energy_consumption" not in _FIELD_CONTRACT
    assert "hedge_underlying" not in _FIELD_CONTRACT
    assert "energy_consumption" not in STAGE5_FIELDS
    assert "hedge_underlying" not in STAGE5_FIELDS
    assert "net_profit_direction" in manifest["forbidden_fields"]
    exposure_props = manifest["exposure_schema"]["properties"]
    assert exposure_props["source_record_ids"]["minItems"] == 1
    assert exposure_props["evidence_ids"]["minItems"] == 1
    assert exposure_props["measurement_record_ids"].get("minItems", 0) == 0


def test_mapped_exposure_roundtrip_and_rejects_profit_direction():
    report = _report()
    exposure = CommodityExposure(
        exposure_id=build_exposure_id(
            source_record_ids=("act-copper",),
            role=CommodityRole.PRODUCT_SALES,
            source_native_name="阴极铜",
            commodity_id="COMMODITY.metal.refined_copper",
            reported_period="2025",
        ),
        report=report,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        knowledge_time="2026-03-20",
        subject_scope=SubjectScope.ISSUER,
        subject_basis=SubjectBasis.DIRECT_SOURCE_WORDING,
        business_object="阴极铜",
        source_record_ids=("act-copper",),
        evidence_ids=("ev-commodity",),
        measurement_record_ids=("meas-copper",),
        source_native_name="阴极铜",
        commodity_id="COMMODITY.metal.refined_copper",
        mapping_status=MappingStatus.MAPPED,
        role=CommodityRole.PRODUCT_SALES,
        mapping_version="catalog-v1",
        market_link_status=MarketLinkStatus.NOT_LINKED,
    )
    restored = CommodityExposure.model_validate_json(exposure.model_dump_json())

    assert restored.schema_version == COMMODITY_EXPOSURE_SCHEMA_VERSION
    assert restored.assertion_class == AssertionClass.DETERMINISTIC_DERIVATION.value
    assert restored.measurement_record_ids == ("meas-copper",)
    with pytest.raises(ValidationError):
        CommodityExposure.model_validate(
            {
                **exposure.model_dump(mode="json"),
                "net_profit_direction": "negative",
            }
        )


def test_mapping_and_market_link_invariants():
    report = _report()
    pending = CommodityExposure(
        exposure_id="commodity-exposure-pending",
        report=report,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        subject_scope=SubjectScope.ISSUER,
        source_record_ids=("act-unknown",),
        evidence_ids=("ev-commodity",),
        source_native_name="未知矿",
        mapping_status=MappingStatus.PENDING,
        role=CommodityRole.UNKNOWN,
        mapping_version="catalog-v1",
    )
    base = json.loads(pending.model_dump_json())
    assert CommodityExposure.model_validate_json(pending.model_dump_json()).commodity_id is None
    with pytest.raises(ValidationError, match="commodity_id"):
        CommodityExposure.model_validate_json(
            json.dumps({**base, "commodity_id": "COMMODITY.x"})
        )
    with pytest.raises(ValidationError, match="commodity_id"):
        CommodityExposure.model_validate_json(
            json.dumps({**base, "mapping_status": "mapped", "commodity_id": None})
        )
    with pytest.raises(ValidationError, match="market_series"):
        CommodityExposure.model_validate_json(
            json.dumps({**base, "market_link_status": "linked", "market_series_id": None})
        )
    with pytest.raises(ValidationError, match="source_record|at least 1"):
        CommodityExposure.model_validate_json(
            json.dumps({**base, "role": "hedge_underlying", "source_record_ids": []})
        )


def test_source_and_evidence_ids_are_required():
    report = _report()
    payload = {
        "exposure_id": "commodity-exposure-pending",
        "report": json.loads(report.model_dump_json()),
        "reported_period": "2025",
        "period_type": PeriodType.DURATION.value,
        "subject_scope": SubjectScope.ISSUER.value,
        "source_record_ids": ["act-unknown"],
        "evidence_ids": ["ev-commodity"],
        "source_native_name": "未知矿",
        "mapping_status": "pending",
        "role": "unknown",
        "mapping_version": "catalog-v1",
    }
    CommodityExposure.model_validate_json(json.dumps(payload))
    with pytest.raises(ValidationError, match="source_record|at least 1"):
        CommodityExposure.model_validate_json(
            json.dumps({**payload, "source_record_ids": []})
        )
    with pytest.raises(ValidationError, match="evidence|at least 1"):
        CommodityExposure.model_validate_json(
            json.dumps({**payload, "evidence_ids": []})
        )


def test_assessed_empty_list_requires_checked_evidence():
    report = _report()
    with pytest.raises(ValidationError, match="checked evidence"):
        CommodityExposureAssessment(
            report=report,
            assessment_status=AssessmentStatus.ASSESSED,
            checked_evidence_ids=(),
            exposures=(),
        )
    assessed = CommodityExposureAssessment(
        report=report,
        assessment_status=AssessmentStatus.ASSESSED,
        checked_evidence_ids=("ev-commodity",),
        exposures=(),
    )
    unread = CommodityExposureAssessment(
        report=report,
        assessment_status=AssessmentStatus.NOT_ASSESSED,
        exposures=(),
    )
    foreign = CommodityExposure(
        exposure_id="commodity-exposure-foreign",
        report=ReportIdentity(
            instrument_id="000878.SZ",
            report_id="asset-commodity-other",
            document_version="ver-1",
            report_period="2025-12-31",
            published_at="2026-03-20T08:00:00+08:00",
        ),
        reported_period="2025",
        period_type=PeriodType.DURATION,
        subject_scope=SubjectScope.ISSUER,
        source_record_ids=("act-other",),
        evidence_ids=("ev-other",),
        source_native_name="阴极铜",
        mapping_status=MappingStatus.PENDING,
        role=CommodityRole.UNKNOWN,
        mapping_version="catalog-v1",
    )
    with pytest.raises(ValidationError, match="same report"):
        CommodityExposureAssessment(
            report=report,
            assessment_status=AssessmentStatus.ASSESSED,
            checked_evidence_ids=("ev-commodity",),
            exposures=(foreign,),
        )
    assert assessed.exposures == ()
    assert unread.assessment_status == AssessmentStatus.NOT_ASSESSED


def test_catalog_adapts_activity_and_relationship_names():
    mapped = resolve_catalog_mapping(_activity("阴极铜"))
    ambiguous = resolve_catalog_mapping(_activity("铜产品"))
    pending = resolve_catalog_mapping(_activity("行业常识金属"))
    material = resolve_catalog_mapping(_relationship("铜精矿"))
    measured = resolve_catalog_mapping(_measurement())

    assert mapped.mapping_status == MappingStatus.MAPPED
    assert mapped.commodity_id == "COMMODITY.metal.refined_copper"
    assert ambiguous.mapping_status == MappingStatus.AMBIGUOUS
    assert ambiguous.commodity_id is None
    assert pending.mapping_status == MappingStatus.PENDING
    assert material.mapping_status == MappingStatus.MAPPED
    assert material.commodity_id == "COMMODITY.metal.copper_concentrate"
    assert measured.mapping_status == MappingStatus.MAPPED
    assert measured.commodity_id == "COMMODITY.metal.refined_copper"
    assert measured.source_native_name == "阴极铜"


def test_mapped_input_without_quantity_or_market_stays_usable():
    report = _report()
    exposure = CommodityExposure(
        exposure_id=build_exposure_id(
            source_record_ids=("rel-copper",),
            role=CommodityRole.RAW_MATERIAL_INPUT,
            source_native_name="铜精矿",
            commodity_id="COMMODITY.metal.copper_concentrate",
            reported_period="2025",
        ),
        report=report,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        subject_scope=SubjectScope.ISSUER,
        business_object="铜精矿",
        source_record_ids=("rel-copper",),
        evidence_ids=("ev-commodity",),
        source_native_name="铜精矿",
        commodity_id="COMMODITY.metal.copper_concentrate",
        mapping_status=MappingStatus.MAPPED,
        role=CommodityRole.RAW_MATERIAL_INPUT,
        mapping_version="catalog-v1",
    )
    assert exposure.measurement_record_ids == ()
    assert exposure.market_series_id is None
    assert exposure.market_link_status == MarketLinkStatus.NOT_LINKED
    assert exposure.commodity_id == "COMMODITY.metal.copper_concentrate"


def test_projection_role_is_rejected_as_stage5_field_id():
    class _Scope:
        scope_id = "scope-commodity-role"
        field_ids = ("energy_consumption",)

    with pytest.raises(ValueError, match="unknown stage-five checklist fields"):
        _validate_prepared_field_contract({"sample-commodity": (_Scope(),)})


def test_quantity_uses_measurement_ids_not_activity_values():
    activity = _activity()
    measurement = _measurement()
    ids = measurement_record_ids((activity, measurement))
    source = inspect.getsource(resolve_catalog_mapping)
    module_source = inspect.getsource(
        __import__(
            "research.company_profile.commodity_exposure",
            fromlist=["resolve_catalog_mapping"],
        )
    )

    assert ids == ("meas-copper",)
    assert activity.source_native.value is None
    assert "business_profile_exposure_production" not in module_source
    assert "sales_volume" not in source
    assert "ActivityAction" not in source or "object_name" in source


def test_legacy_exposure_roles_are_not_stage5_field_ids():
    manifest = commodity_exposure_schema_manifest()
    assert "revenue" not in manifest["roles"]
    assert "feedstock_cost" not in manifest["roles"]
    assert "energy_cost" not in manifest["roles"]
    assert "energy_consumption" not in STAGE5_FIELDS
    assert "hedge_underlying" not in STAGE5_FIELDS
    assert "energy_consumption" not in _FIELD_CONTRACT
    assert "hedge_underlying" not in _FIELD_CONTRACT
