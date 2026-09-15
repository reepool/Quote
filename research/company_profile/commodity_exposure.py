"""Derived commodity-exposure schema and catalog adaptation.

This is the unique owner for company_profile_commodity_exposure.v1. It does
not add Stage 5 extract objects or field_ids, and it does not copy quantities
from legacy Activity numeric fields. Writer activation stays out of this slice.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from research.company_profile.models import (
    PRODUCTION_AUTHORIZATION,
    Activity,
    AssertionClass,
    Evidence,
    Measurement,
    PeriodType,
    Relationship,
    ReportIdentity,
    SemanticRecord,
    SubjectBasis,
    SubjectScope,
)

COMMODITY_EXPOSURE_SCHEMA_VERSION = "company_profile_commodity_exposure.v1"
COMMODITY_EXPOSURE_POLICY_VERSION = "company_profile_commodity_exposure.v1"
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
    accepted_records: tuple[SemanticRecord, ...] = Field(min_length=1)
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
        index = _accepted_index(self.accepted_records)
        sources = []
        for record_id in self.source_record_ids:
            record = index.get(record_id)
            if record is None:
                raise ValueError(
                    "source_record_ids must resolve to accepted new-model facts"
                )
            if record.report != self.report:
                raise ValueError("source_record_ids must belong to the same report")
            sources.append(record)
        measurements = []
        for record_id in self.measurement_record_ids:
            record = index.get(record_id)
            if record is None or not isinstance(record, Measurement):
                raise ValueError(
                    "measurement_record_ids must resolve to same-report Measurement"
                )
            if record.report != self.report:
                raise ValueError(
                    "measurement_record_ids must belong to the same report"
                )
            measurements.append(record)
        owned_evidence = {
            item.evidence_id: item
            for record in (*sources, *measurements)
            for item in record.evidence
        }
        for evidence_id in self.evidence_ids:
            evidence = owned_evidence.get(evidence_id)
            if evidence is None:
                raise ValueError(
                    "evidence_ids must belong to the referenced source facts"
                )
            if evidence.report != self.report:
                raise ValueError("evidence_ids must belong to the same report")
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
    checked_evidence: tuple[Evidence, ...] = ()
    exposures: tuple[CommodityExposure, ...] = ()

    @model_validator(mode="after")
    def _empty_list_is_not_zero_exposure(self) -> CommodityExposureAssessment:
        _reject_blank_ids(self.checked_evidence_ids, label="checked_evidence_ids")
        if any(item.report != self.report for item in self.exposures):
            raise ValueError("assessment exposures must belong to the same report")
        if any(item.report != self.report for item in self.checked_evidence):
            raise ValueError("checked evidence must belong to the same report")
        if any(not str(item.evidence_id).strip() for item in self.checked_evidence):
            raise ValueError("checked evidence_id cannot be blank")
        by_id = {item.evidence_id: item for item in self.checked_evidence}
        for evidence_id in self.checked_evidence_ids:
            evidence = by_id.get(evidence_id)
            if evidence is None:
                raise ValueError(
                    "checked_evidence_ids must resolve to this-report Evidence"
                )
        if (
            self.assessment_status == AssessmentStatus.ASSESSED
            and not self.exposures
            and not self.checked_evidence_ids
        ):
            raise ValueError(
                "assessed empty exposure list requires checked evidence scope"
            )
        return self


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


def _catalog_source_name(source: Activity | Measurement | Relationship) -> str:
    if isinstance(source, Measurement):
        return str(source.measured_object).strip()
    return str(source.object_name).strip()


def resolve_catalog_mapping(
    source: Activity | Measurement | Relationship,
    *,
    catalog: Any | None = None,
    knowledge_time: str | None = None,
) -> CommodityCatalogMapping:
    """Map a new-model Activity, Measurement, or Relationship name through the catalog."""

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
