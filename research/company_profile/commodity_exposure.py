"""Derived commodity-exposure schema and catalog adaptation.

This is the unique owner for company_profile_commodity_exposure.v1. It does
not add Stage 5 extract objects or field_ids, and it does not copy quantities
from legacy Activity numeric fields. Empty, unread, and failed association
lists are coverage states, not a zero-exposure claim. Writer activation and
query surfaces stay out of this slice.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.models import (
    PRODUCTION_AUTHORIZATION,
    Activity,
    ActivityAction,
    AssertionClass,
    BusinessEvent,
    Evidence,
    Measurement,
    MetricType,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    SemanticRecord,
    SubjectBasis,
    SubjectScope,
)

COMMODITY_EXPOSURE_SCHEMA_VERSION = "company_profile_commodity_exposure.v1"
COMMODITY_EXPOSURE_POLICY_VERSION = "company_profile_commodity_exposure.v1"
COMMODITY_ROLE_RULE_VERSION = "company_profile_commodity_role.v1"
_FORBIDDEN_PROJECTION_FIELDS = frozenset(
    {
        "net_profit_direction",
        "net_exposure",
        "elasticity",
        "price_sensitivity",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class MappingStatus:
    MAPPED = "mapped"
    PENDING = "pending"
    AMBIGUOUS = "ambiguous"


class CommodityRole:
    PRODUCT_SALES = "product_sales"
    RAW_MATERIAL_INPUT = "raw_material_input"
    ENERGY_CONSUMPTION = "energy_consumption"
    HEDGE_UNDERLYING = "hedge_underlying"
    UNKNOWN = "unknown"


_ENERGY_CONSUMPTION_VERBS = frozenset({"消耗", "耗用", "用电", "耗电"})
_ENERGY_CONSUMPTION_OBJECTS = frozenset({"电力", "蒸汽", "用电量", "电"})
_HEDGE_MARKERS = ("套保", "套期", "hedge")
_HEDGE_NEGATIONS = (
    "未开展",
    "未进行",
    "未从事",
    "未参与",
    "未做",
    "不开展",
    "不进行",
    "无套保",
    "无套期",
    "没有开展",
    "没有进行",
    "不存在套保",
    "不存在套期",
    "no hedge",
    "not hedge",
    "without hedge",
)
_CLAUSE_SPLIT = re.compile(r"[,，;；。.!！?？]+")
_ROLE_SOURCE_METRICS = frozenset({MetricType.SALES_VOLUME})
_ROLE_QUANTITY_METRICS: dict[str, frozenset[MetricType]] = {
    CommodityRole.PRODUCT_SALES: frozenset({MetricType.SALES_VOLUME}),
    CommodityRole.RAW_MATERIAL_INPUT: frozenset(),
    CommodityRole.ENERGY_CONSUMPTION: frozenset(),
    CommodityRole.HEDGE_UNDERLYING: frozenset(),
}


class MarketLinkStatus:
    NOT_LINKED = "not_linked"
    LINKED = "linked"
    AMBIGUOUS = "ambiguous"


class AssessmentStatus:
    NOT_ASSESSED = "not_assessed"
    ASSESSED = "assessed"
    EXTRACTION_FAILED = "extraction_failed"


def _reject_blank_ids(ids: Sequence[str], *, label: str) -> None:
    if any(not str(item).strip() for item in ids):
        raise ValueError(f"{label} cannot be blank")


def _accepted_index(
    records: Sequence[SemanticRecord],
) -> dict[str, SemanticRecord]:
    index: dict[str, SemanticRecord] = {}
    for record in records:
        key = str(record.record_id).strip()
        if not key:
            raise ValueError("accepted record_id cannot be blank")
        existing = index.get(key)
        if existing is not None and existing != record:
            raise ValueError("accepted records have conflicting record_id")
        index[key] = record
    return index


def _unique_evidence_index(
    items: Sequence[Evidence],
    *,
    label: str,
) -> dict[str, Evidence]:
    index: dict[str, Evidence] = {}
    for item in items:
        key = str(item.evidence_id).strip()
        if not key:
            raise ValueError(f"{label} cannot be blank")
        existing = index.get(key)
        if existing is not None and existing != item:
            raise ValueError(f"conflicting evidence for {key}")
        index[key] = item
    return index


def _validate_exposure_references(
    exposure: CommodityExposure,
    accepted_records: Sequence[SemanticRecord],
) -> None:
    index = _accepted_index(accepted_records)
    sources = []
    for record_id in exposure.source_record_ids:
        record = index.get(record_id)
        if record is None:
            raise ValueError(
                "source_record_ids must resolve to accepted new-model facts"
            )
        if record.report != exposure.report:
            raise ValueError("source_record_ids must belong to the same report")
        sources.append(record)
    measurements = []
    for record_id in exposure.measurement_record_ids:
        record = index.get(record_id)
        if record is None or not isinstance(record, Measurement):
            raise ValueError(
                "measurement_record_ids must resolve to same-report Measurement"
            )
        if record.report != exposure.report:
            raise ValueError("measurement_record_ids must belong to the same report")
        measurements.append(record)
    owned_evidence = _unique_evidence_index(
        tuple(
            item
            for record in (*sources, *measurements)
            for item in record.evidence
        ),
        label="evidence_ids",
    )
    for evidence_id in exposure.evidence_ids:
        evidence = owned_evidence.get(evidence_id)
        if evidence is None:
            raise ValueError("evidence_ids must belong to the referenced source facts")
        if evidence.report != exposure.report:
            raise ValueError("evidence_ids must belong to the same report")


def _validate_assessment_references(
    assessment: CommodityExposureAssessment,
    checked_evidence: Sequence[Evidence],
) -> None:
    by_id = _unique_evidence_index(checked_evidence, label="checked_evidence_ids")
    if any(item.report != assessment.report for item in checked_evidence):
        raise ValueError("checked evidence must belong to the same report")
    for evidence_id in assessment.checked_evidence_ids:
        evidence = by_id.get(evidence_id)
        if evidence is None:
            raise ValueError(
                "checked_evidence_ids must resolve to this-report Evidence"
            )


class CommodityCatalogMapping(_StrictModel):
    source_native_name: str = Field(min_length=1)
    mapping_status: Literal["mapped", "pending", "ambiguous"]
    commodity_id: str | None = None
    product_ids: tuple[str, ...] = ()
    catalog_version: str = Field(min_length=1)
    mapping_version: str = Field(min_length=1)

    @model_validator(mode="after")
    def _mapping_identity(self) -> CommodityCatalogMapping:
        if self.mapping_status == MappingStatus.MAPPED:
            if not self.commodity_id:
                raise ValueError("mapped commodity requires commodity_id")
        elif self.commodity_id is not None:
            raise ValueError("unmapped commodity cannot carry commodity_id")
        return self


class CommodityExposure(_StrictModel):
    schema_version: Literal["company_profile_commodity_exposure.v1"] = (
        COMMODITY_EXPOSURE_SCHEMA_VERSION
    )
    exposure_id: str = Field(min_length=1)
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    report: ReportIdentity
    reported_period: str = Field(min_length=1)
    period_type: PeriodType
    knowledge_time: str | None = None
    subject_scope: SubjectScope
    subject_basis: SubjectBasis | None = None
    business_object: str | None = None
    source_record_ids: tuple[str, ...] = Field(min_length=1)
    evidence_ids: tuple[str, ...] = Field(min_length=1)
    measurement_record_ids: tuple[str, ...] = ()
    source_native_name: str = Field(min_length=1)
    commodity_id: str | None = None
    mapping_status: Literal["mapped", "pending", "ambiguous"]
    role: Literal[
        "product_sales",
        "raw_material_input",
        "energy_consumption",
        "hedge_underlying",
        "unknown",
    ]
    assertion_class: Literal["deterministic_derivation"] = (
        AssertionClass.DETERMINISTIC_DERIVATION.value
    )
    mapping_version: str = Field(min_length=1)
    policy_version: Literal["company_profile_commodity_exposure.v1"] = (
        COMMODITY_EXPOSURE_POLICY_VERSION
    )
    market_series_id: str | None = None
    market_link_status: Literal["not_linked", "linked", "ambiguous"] = (
        MarketLinkStatus.NOT_LINKED
    )
    uncertainty: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _contract_invariants(self) -> CommodityExposure:
        _reject_blank_ids(self.source_record_ids, label="source_record_ids")
        _reject_blank_ids(self.evidence_ids, label="evidence_ids")
        _reject_blank_ids(self.measurement_record_ids, label="measurement_record_ids")
        if self.mapping_status == MappingStatus.MAPPED:
            if not self.commodity_id:
                raise ValueError("mapped commodity requires commodity_id")
        elif self.commodity_id is not None:
            raise ValueError("unmapped commodity cannot carry commodity_id")
        if self.market_link_status == MarketLinkStatus.LINKED:
            if not self.market_series_id:
                raise ValueError("linked market series requires market_series_id")
        elif self.market_series_id is not None:
            raise ValueError("unlinked market series cannot carry market_series_id")
        if self.role == CommodityRole.HEDGE_UNDERLYING and not self.source_record_ids:
            raise ValueError("hedge_underlying requires accepted hedge source records")
        return self


class CommodityExposureAssessment(_StrictModel):
    schema_version: Literal["company_profile_commodity_exposure.v1"] = (
        COMMODITY_EXPOSURE_SCHEMA_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    report: ReportIdentity
    assessment_status: Literal["not_assessed", "assessed", "extraction_failed"]
    checked_evidence_ids: tuple[str, ...] = ()
    exposures: tuple[CommodityExposure, ...] = ()

    @model_validator(mode="after")
    def _empty_list_is_not_zero_exposure(self) -> CommodityExposureAssessment:
        _reject_blank_ids(self.checked_evidence_ids, label="checked_evidence_ids")
        if any(item.report != self.report for item in self.exposures):
            raise ValueError("assessment exposures must belong to the same report")
        if (
            self.assessment_status == AssessmentStatus.ASSESSED
            and not self.exposures
            and not self.checked_evidence_ids
        ):
            raise ValueError(
                "assessed empty exposure list requires checked evidence scope"
            )
        return self


def bind_commodity_exposure(
    exposure: CommodityExposure,
    *,
    accepted_records: Sequence[SemanticRecord],
) -> CommodityExposure:
    """Resolve published IDs against accepted facts without storing the facts."""

    _validate_exposure_references(exposure, accepted_records)
    return exposure


def bind_commodity_exposure_assessment(
    assessment: CommodityExposureAssessment,
    *,
    checked_evidence: Sequence[Evidence] = (),
) -> CommodityExposureAssessment:
    """Resolve checked_evidence_ids against this-report Evidence objects."""

    _validate_assessment_references(assessment, checked_evidence)
    return assessment


def commodity_exposure_schema_manifest() -> dict[str, Any]:
    """Register the read/write JSON schema without enabling a writer."""

    return {
        "schema_version": COMMODITY_EXPOSURE_SCHEMA_VERSION,
        "policy_version": COMMODITY_EXPOSURE_POLICY_VERSION,
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "stage5_object_types": (),
        "stage5_field_ids": (),
        "roles": (
            CommodityRole.PRODUCT_SALES,
            CommodityRole.RAW_MATERIAL_INPUT,
            CommodityRole.ENERGY_CONSUMPTION,
            CommodityRole.HEDGE_UNDERLYING,
            CommodityRole.UNKNOWN,
        ),
        "mapping_statuses": (
            MappingStatus.MAPPED,
            MappingStatus.PENDING,
            MappingStatus.AMBIGUOUS,
        ),
        "assessment_statuses": (
            AssessmentStatus.NOT_ASSESSED,
            AssessmentStatus.ASSESSED,
            AssessmentStatus.EXTRACTION_FAILED,
        ),
        "empty_association_is_not_zero_exposure": True,
        "forbidden_fields": tuple(sorted(_FORBIDDEN_PROJECTION_FIELDS)),
        "exposure_schema": CommodityExposure.model_json_schema(),
        "assessment_schema": CommodityExposureAssessment.model_json_schema(),
    }


def build_exposure_id(
    *,
    source_record_ids: Sequence[str],
    role: str,
    source_native_name: str,
    commodity_id: str | None,
    reported_period: str,
    policy_version: str = COMMODITY_EXPOSURE_POLICY_VERSION,
) -> str:
    payload = {
        "source_record_ids": list(source_record_ids),
        "role": role,
        "source_native_name": source_native_name,
        "commodity_id": commodity_id,
        "reported_period": reported_period,
        "policy_version": policy_version,
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"commodity-exposure-{digest[:24]}"


def measurement_record_ids(records: Sequence[Any]) -> tuple[str, ...]:
    """Keep quantity only as Measurement references."""

    return tuple(
        str(item.record_id)
        for item in records
        if isinstance(item, Measurement)
    )


def _catalog_source_name(
    source: Activity | Measurement | Relationship | BusinessEvent,
) -> str:
    if isinstance(source, Measurement):
        return str(source.measured_object).strip()
    if isinstance(source, BusinessEvent):
        return str(source.source_native.name or "").strip()
    return str(source.object_name).strip()


def resolve_catalog_mapping(
    source: Activity | Measurement | Relationship | BusinessEvent,
    *,
    catalog: Any | None = None,
    knowledge_time: str | None = None,
) -> CommodityCatalogMapping:
    """Map a new-model Activity, Measurement, Relationship, or hedge event name."""

    from research.business_profile_product_catalog import load_business_product_catalog

    active = catalog or load_business_product_catalog(
        document_date=(str(knowledge_time)[:10] if knowledge_time else None)
    )
    name = _catalog_source_name(source)
    resolution = active.resolve_alias(name)
    product_ids = tuple(resolution.product_ids)
    commodity_ids = []
    for product_id in product_ids:
        commodity_ids.extend(
            mapping.commodity_id
            for mapping in active.commodity_candidates(product_id)
        )
    unique_commodities = tuple(dict.fromkeys(commodity_ids))
    if len(product_ids) == 1 and len(unique_commodities) == 1:
        status = MappingStatus.MAPPED
        commodity_id = unique_commodities[0]
    elif product_ids and (
        len(product_ids) > 1 or len(unique_commodities) > 1
    ):
        status = MappingStatus.AMBIGUOUS
        commodity_id = None
    else:
        status = MappingStatus.PENDING
        commodity_id = None
    return CommodityCatalogMapping(
        source_native_name=name,
        mapping_status=status,
        commodity_id=commodity_id,
        product_ids=product_ids,
        catalog_version=str(active.catalog_version),
        mapping_version=str(active.catalog_version),
    )


def _contains_markers(text: str, markers: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(marker in text or marker.lower() in lowered for marker in markers)


def _statement_clauses(*texts: str) -> tuple[str, ...]:
    clauses: list[str] = []
    for text in texts:
        value = str(text or "").strip()
        if not value:
            continue
        parts = [part.strip() for part in _CLAUSE_SPLIT.split(value) if part.strip()]
        clauses.extend(parts)
    return tuple(clauses)


def _is_hedge_fact(source: SemanticRecord) -> bool:
    if isinstance(source, BusinessEvent):
        statements = (source.description, source.source_native.name or "")
    elif isinstance(source, Activity):
        statements = (
            " ".join(
                part
                for part in (
                    source.source_verb,
                    source.object_name,
                    source.source_native.name or "",
                )
                if str(part or "").strip()
            ),
        )
    else:
        return False
    hedge_clauses = [
        clause
        for clause in _statement_clauses(*statements)
        if _contains_markers(clause, _HEDGE_MARKERS)
    ]
    return any(
        not _contains_markers(clause, _HEDGE_NEGATIONS) for clause in hedge_clauses
    )


def _is_energy_consumption_evidence(source: SemanticRecord) -> bool:
    name = _catalog_source_name(source) if isinstance(
        source, (Activity, Measurement, Relationship, BusinessEvent)
    ) else ""
    verb = getattr(source, "source_verb", "") or ""
    qualifier = ""
    native = getattr(source, "source_native", None)
    if native is not None:
        qualifier = str(native.qualifier or "")
        if not name:
            name = str(native.name or "").strip()
    if any(token in verb for token in _ENERGY_CONSUMPTION_VERBS):
        return True
    if "能源" in qualifier or "燃料消耗" in qualifier:
        return True
    return name in _ENERGY_CONSUMPTION_OBJECTS


def derive_commodity_role(source: SemanticRecord) -> str | None:
    """Return an evidence-backed role, or None when the fact does not support one."""

    if _is_hedge_fact(source):
        return CommodityRole.HEDGE_UNDERLYING
    if isinstance(source, Activity):
        if source.action == ActivityAction.SELLS:
            return CommodityRole.PRODUCT_SALES
        if source.action == ActivityAction.PURCHASES:
            if _is_energy_consumption_evidence(source):
                return CommodityRole.ENERGY_CONSUMPTION
            return CommodityRole.RAW_MATERIAL_INPUT
        return None
    if isinstance(source, Relationship):
        if source.relation_type == RelationshipType.MATERIAL_INPUT:
            if _is_energy_consumption_evidence(source):
                return CommodityRole.ENERGY_CONSUMPTION
            return CommodityRole.RAW_MATERIAL_INPUT
        return None
    if isinstance(source, Measurement):
        if source.metric_type in _ROLE_SOURCE_METRICS:
            return CommodityRole.PRODUCT_SALES
        return None
    return None


def _compatible_measurements(
    source: SemanticRecord,
    records: Sequence[SemanticRecord],
    role: str,
) -> tuple[Measurement, ...]:
    allowed = _ROLE_QUANTITY_METRICS.get(role, frozenset())
    if not allowed:
        return ()
    name = _catalog_source_name(source) if isinstance(
        source, (Activity, Measurement, Relationship, BusinessEvent)
    ) else ""
    attached: list[Measurement] = []
    for item in records:
        if not isinstance(item, Measurement):
            continue
        if item.record_id == getattr(source, "record_id", None):
            if item.metric_type in allowed:
                attached.append(item)
            continue
        if (
            item.report == source.report
            and item.reported_period == source.reported_period
            and item.subject_scope == source.subject_scope
            and item.subject_name == source.subject_name
            and item.measured_object == name
            and item.metric_type in allowed
        ):
            attached.append(item)
    return tuple(attached)


def _project_one_exposure(
    source: SemanticRecord,
    records: Sequence[SemanticRecord],
    *,
    catalog: Any | None,
) -> CommodityExposure:
    role = derive_commodity_role(source)
    if role is None:
        raise ValueError("source does not support a commodity role")
    name = _catalog_source_name(source)
    if not name:
        raise ValueError("commodity role requires a source-native name")
    mapping = resolve_catalog_mapping(
        source,
        catalog=catalog,
        knowledge_time=source.knowledge_time,
    )
    quantities = _compatible_measurements(source, records, role)
    if (
        isinstance(source, Measurement)
        and source not in quantities
        and source.metric_type in _ROLE_QUANTITY_METRICS.get(role, frozenset())
    ):
        quantities = (source, *quantities)
    evidence_ids = tuple(item.evidence_id for item in source.evidence)
    exposure = CommodityExposure(
        exposure_id=build_exposure_id(
            source_record_ids=(source.record_id,),
            role=role,
            source_native_name=name,
            commodity_id=mapping.commodity_id,
            reported_period=source.reported_period,
        ),
        report=source.report,
        reported_period=source.reported_period,
        period_type=source.period_type,
        knowledge_time=source.knowledge_time,
        subject_scope=source.subject_scope,
        subject_basis=source.subject_basis,
        business_object=name,
        source_record_ids=(source.record_id,),
        evidence_ids=evidence_ids,
        measurement_record_ids=measurement_record_ids(quantities),
        source_native_name=name,
        commodity_id=mapping.commodity_id,
        mapping_status=mapping.mapping_status,
        role=role,
        mapping_version=f"{mapping.mapping_version}+{COMMODITY_ROLE_RULE_VERSION}",
        market_link_status=MarketLinkStatus.NOT_LINKED,
        uncertainty=source.uncertainty,
    )
    return bind_commodity_exposure(exposure, accepted_records=(*records, source))


def project_commodity_exposures(
    accepted_records: Sequence[SemanticRecord],
    *,
    catalog: Any | None = None,
) -> tuple[CommodityExposure, ...]:
    """Derive evidence-backed roles without inventing profit direction or quantities."""

    primaries: list[SemanticRecord] = []
    attached_measurement_ids: set[str] = set()
    for record in accepted_records:
        if isinstance(record, Measurement):
            continue
        role = derive_commodity_role(record)
        if role is None or not _catalog_source_name(record):
            continue
        primaries.append(record)
        attached_measurement_ids.update(
            item.record_id
            for item in _compatible_measurements(record, accepted_records, role)
        )
    leftovers = [
        record
        for record in accepted_records
        if isinstance(record, Measurement)
        and derive_commodity_role(record) is not None
        and record.record_id not in attached_measurement_ids
    ]
    exposures = [
        _project_one_exposure(source, accepted_records, catalog=catalog)
        for source in (*primaries, *leftovers)
    ]
    return tuple(exposures)


def assess_commodity_exposures(
    *,
    report: ReportIdentity,
    assessment_status: str,
    accepted_records: Sequence[SemanticRecord] = (),
    checked_evidence: Sequence[Evidence] = (),
    catalog: Any | None = None,
) -> CommodityExposureAssessment:
    """Assemble the report-level association list without netting or zero claims."""

    if assessment_status not in {
        AssessmentStatus.NOT_ASSESSED,
        AssessmentStatus.ASSESSED,
        AssessmentStatus.EXTRACTION_FAILED,
    }:
        raise ValueError(f"unsupported assessment_status: {assessment_status}")
    scoped_records = tuple(
        item for item in accepted_records if item.report == report
    )
    scoped_evidence = tuple(
        item for item in checked_evidence if item.report == report
    )
    exposures = (
        project_commodity_exposures(scoped_records, catalog=catalog)
        if assessment_status == AssessmentStatus.ASSESSED
        else ()
    )
    checked_ids = (
        ()
        if assessment_status == AssessmentStatus.NOT_ASSESSED
        else tuple(dict.fromkeys(item.evidence_id for item in scoped_evidence))
    )
    assessment = CommodityExposureAssessment(
        report=report,
        assessment_status=assessment_status,
        checked_evidence_ids=checked_ids,
        exposures=exposures,
    )
    return bind_commodity_exposure_assessment(
        assessment,
        checked_evidence=scoped_evidence,
    )
