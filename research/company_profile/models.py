"""Strict, in-memory company-profile semantic objects for stage four.

This module deliberately has no storage, network, scheduler, or legacy business-profile
imports.  Production publication is not authorized in stage four.
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

SEMANTIC_OBJECT_SCHEMA_VERSION = "company_profile_semantic_object.v1"
RESEARCH_VIEW_SCHEMA_VERSION = "company_profile_research_view.v1"
PRODUCTION_AUTHORIZATION = "not_authorized"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _StringEnum(str, Enum):
    def __str__(self) -> str:
        return self.value


class ObjectType(_StringEnum):
    BUSINESS_OVERVIEW = "BusinessOverview"
    SEGMENT = "Segment"
    ACTIVITY = "Activity"
    MEASUREMENT = "Measurement"
    RELATIONSHIP = "Relationship"
    BUSINESS_EVENT = "BusinessEvent"
    BUSINESS_REGIME = "BusinessRegime"
    INDUSTRY_PACKAGE_ASSIGNMENT = "IndustryPackageAssignment"


class SubjectScope(_StringEnum):
    CONSOLIDATED_GROUP = "consolidated_group"
    ISSUER = "issuer"
    NAMED_SUBSIDIARY = "named_subsidiary"
    BUSINESS_SEGMENT = "business_segment"
    UNCLEAR = "unclear"


class SubjectBasis(_StringEnum):
    DIRECT_SOURCE_WORDING = "direct_source_wording"
    NUMERIC_RECONCILIATION_TO_CONSOLIDATED_STATEMENT = (
        "numeric_reconciliation_to_consolidated_statement"
    )
    DIRECT_GRAMMATICAL_ACTOR = "direct_grammatical_actor"
    EXPLICIT_ECONOMIC_RELATIONSHIP = "explicit_economic_relationship"
    UNCLEAR = "unclear"


class AssertionClass(_StringEnum):
    REPORTED_FACT = "reported_fact"
    DETERMINISTIC_DERIVATION = "deterministic_derivation"
    RESEARCH_ASSUMPTION = "research_assumption"


class RequirementLevel(_StringEnum):
    REQUIRED = "required"
    CONDITIONAL = "conditional"
    OPTIONAL = "optional"


class CoverageStatus(_StringEnum):
    OBSERVED = "observed"
    NOT_DISCLOSED = "not_disclosed"
    NOT_APPLICABLE = "not_applicable"
    EXTRACTION_FAILED = "extraction_failed"
    UNCLEAR = "unclear"


class CoverageReasonCode(_StringEnum):
    EXPLICIT_CONFIDENTIALITY = "explicit_confidentiality"
    EXPLICIT_DISCLOSURE_EXEMPTION = "explicit_disclosure_exemption"
    SOURCE_REASON_UNSPECIFIED = "source_reason_unspecified"
    TABLE_CONTEXT_INCOMPLETE = "table_context_incomplete"
    SOURCE_UNREADABLE = "source_unreadable"
    COVERAGE_BUDGET_EXHAUSTED = "coverage_budget_exhausted"
    UNIT_AMBIGUOUS = "unit_ambiguous"
    REQUIRED_RESULT_MISSING = "required_result_missing"
    CANDIDATE_UNRESOLVED = "candidate_unresolved"
    SOURCE_EXPLICITLY_NOT_APPLICABLE = "source_explicitly_not_applicable"


class ChapterTask(_StringEnum):
    EXTRACT_BUSINESS_OVERVIEW = "extract_business_overview"
    EXTRACT_SEGMENT_FINANCIALS = "extract_segment_financials"
    EXTRACT_OPERATING_QUANTITIES = "extract_operating_quantities"
    EXTRACT_MATERIAL_INPUTS = "extract_material_inputs"
    EXTRACT_COUNTERPARTIES_AND_CONCENTRATION = (
        "extract_counterparties_and_concentration"
    )
    EXTRACT_BUSINESS_REGIME = "extract_business_regime"


class ActivityAction(_StringEnum):
    DEVELOPS = "develops"
    PRODUCES = "produces"
    PROCESSES = "processes"
    SELLS = "sells"
    PURCHASES = "purchases"
    PROVIDES_SERVICE = "provides_service"
    OPERATES = "operates"


class MetricType(_StringEnum):
    OPERATING_REVENUE = "operating_revenue"
    OPERATING_COST = "operating_cost"
    GROSS_MARGIN_REPORTED = "gross_margin_reported"
    PRODUCTION_CAPACITY = "production_capacity"
    CAPACITY_UNDER_CONSTRUCTION = "capacity_under_construction"
    CAPACITY_UTILIZATION = "capacity_utilization"
    PRODUCTION_VOLUME = "production_volume"
    SALES_VOLUME = "sales_volume"
    INVENTORY_VOLUME = "inventory_volume"
    PROCESSING_VOLUME = "processing_volume"
    CUSTOMER_SALES_AMOUNT = "customer_sales_amount"
    SUPPLIER_PURCHASE_AMOUNT = "supplier_purchase_amount"
    DISCLOSED_SHARE = "disclosed_share"


class LogicalSlot(_StringEnum):
    REVENUE = "revenue"
    COST = "cost"
    GROSS_MARGIN = "gross_margin"
    CAPACITY = "capacity"
    CAPACITY_UNDER_CONSTRUCTION = "capacity_under_construction"
    CAPACITY_UTILIZATION = "capacity_utilization"
    PRODUCTION_VOLUME = "production_volume"
    SALES_VOLUME = "sales_volume"
    INVENTORY_VOLUME = "inventory_volume"
    PROCESSING_VOLUME = "processing_volume"
    CUSTOMER_SALES_AMOUNT = "customer_sales_amount"
    SUPPLIER_PURCHASE_AMOUNT = "supplier_purchase_amount"
    DISCLOSED_SHARE = "disclosed_share"


class CapacityKind(_StringEnum):
    REPORT_PERIOD_CAPACITY = "report_period_capacity"
    EFFECTIVE_CAPACITY = "effective_capacity"
    DESIGN_CAPACITY = "design_capacity"
    SOURCE_NATIVE_OTHER = "source_native_other"
    UNCLEAR = "unclear"


class ComparisonBasis(_StringEnum):
    CURRENT_PERIOD_AFTER_RESTRUCTURING = "current_period_after_restructuring"
    SAME_CONTROL_RESTATED = "same_control_restated"
    ORIGINAL_AS_PUBLISHED = "original_as_published"
    SOURCE_NATIVE_OTHER = "source_native_other"
    UNCLEAR = "unclear"


class ProcessingDirection(_StringEnum):
    EXTERNAL_SERVICE_PROVIDED = "external_service_provided"


class IdentityClass(_StringEnum):
    NAMED = "named"
    REPORT_LOCAL_ANONYMOUS = "report_local_anonymous"
    REPORT_LOCAL_AGGREGATE = "report_local_aggregate"


class RowClass(_StringEnum):
    CONSOLIDATION_ADJUSTMENT = "consolidation_adjustment"


class RelationshipType(_StringEnum):
    CUSTOMER = "customer"
    SUPPLIER = "supplier"
    MATERIAL_INPUT = "material_input"
    RELATED_PARTY = "related_party"
    CONTRACT_COUNTERPARTY = "contract_counterparty"


class PeriodType(_StringEnum):
    DURATION = "duration"
    INSTANT = "instant"
    EVENT = "event"
    EXPECTED = "expected"


class DataStatus(_StringEnum):
    RESEARCH_FIXTURE = "research_fixture"


class ReportIdentity(_StrictModel):
    instrument_id: str = Field(min_length=1)
    report_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    report_period: str = Field(min_length=1)
    published_at: str = Field(min_length=1)
    document_type: str = "annual_report"


class TableAnchor(_StrictModel):
    anchor_type: Literal["table"] = "table"
    table_label: str | None = None
    row_label: str | None = None
    column_header: str | None = None
    cell_locator: str | None = None

    @model_validator(mode="after")
    def _has_physical_location(self) -> TableAnchor:
        if not self.row_label and not self.cell_locator:
            raise ValueError("table anchor requires row_label or cell_locator")
        return self


class TextAnchor(_StrictModel):
    anchor_type: Literal["text"] = "text"
    bounded_quote: str = Field(min_length=1)
    context_before: str | None = None
    context_after: str | None = None
    match_index: int = Field(default=0, ge=0)


PhysicalAnchor: TypeAlias = Annotated[
    TableAnchor | TextAnchor,
    Field(discriminator="anchor_type"),
]


class Evidence(_StrictModel):
    evidence_id: str = Field(min_length=1)
    report: ReportIdentity
    page: int = Field(ge=1)
    printed_page_label: str | None = None
    section_title: str = Field(min_length=1)
    continuation_pages: tuple[int, ...] = ()
    subject_evidence_pages: tuple[int, ...] = ()
    anchor: PhysicalAnchor

    @model_validator(mode="after")
    def _pages_are_physical_and_positive(self) -> Evidence:
        if any(
            page < 1
            for page in (*self.continuation_pages, *self.subject_evidence_pages)
        ):
            raise ValueError("evidence pages use one-based PDF physical coordinates")
        return self


class SourceNativeValue(_StrictModel):
    name: str | None = None
    value: str | None = None
    unit: str | None = None
    header: str | None = None
    qualifier: str | None = None
    footnote_refs: tuple[str, ...] = ()
    source_aliases: tuple[str, ...] = ()


class SourceFact(_StrictModel):
    schema_version: Literal["company_profile_semantic_object.v1"] = (
        SEMANTIC_OBJECT_SCHEMA_VERSION
    )
    record_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    chapter_task: ChapterTask
    report: ReportIdentity
    subject_scope: SubjectScope
    subject_name: str | None = None
    subject_basis: SubjectBasis | None = None
    reported_period: str = Field(min_length=1)
    period_type: PeriodType
    knowledge_time: str | None = None
    assertion_class: AssertionClass
    evidence: tuple[Evidence, ...] = Field(min_length=1)
    source_native: SourceNativeValue
    uncertainty: tuple[str, ...] = ()
    data_status: Literal["research_fixture"] = DataStatus.RESEARCH_FIXTURE.value

    @model_validator(mode="after")
    def _evidence_matches_report(self) -> SourceFact:
        if any(item.report != self.report for item in self.evidence):
            raise ValueError("all evidence must belong to the record report")
        if (
            self.subject_scope == SubjectScope.CONSOLIDATED_GROUP
            and self.subject_basis
            not in {
                SubjectBasis.DIRECT_SOURCE_WORDING,
                SubjectBasis.NUMERIC_RECONCILIATION_TO_CONSOLIDATED_STATEMENT,
            }
        ):
            raise ValueError("consolidated_group requires affirmative subject_basis")
        return self

    def occurrence_material(self) -> dict[str, Any]:
        evidence_material: list[dict[str, Any]] = []
        for item in self.evidence:
            anchor = item.anchor
            if isinstance(anchor, TableAnchor):
                anchor_material: dict[str, Any] = {
                    "anchor_type": "table",
                    "table_label": anchor.table_label,
                    "row_label": anchor.row_label,
                    "column_header": anchor.column_header,
                    "cell_locator": anchor.cell_locator,
                }
            else:
                anchor_material = {
                    "anchor_type": "text",
                    "quote_hash": _normalized_text_hash(anchor.bounded_quote),
                    "context_before": _normalize_text(anchor.context_before or ""),
                    "context_after": _normalize_text(anchor.context_after or ""),
                    "match_index": anchor.match_index,
                }
            evidence_material.append(
                {
                    "page": item.page,
                    "anchor": anchor_material,
                }
            )
        evidence_material.sort(
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        material: dict[str, Any] = {
            "instrument_id": self.report.instrument_id,
            "document_version": self.report.document_version,
            "report_period": self.report.report_period,
            "evidence": evidence_material,
        }
        logical_slot = getattr(self, "logical_slot", None)
        if logical_slot is not None:
            material["logical_slot"] = _enum_value(logical_slot)
        return material

    def occurrence_id(self) -> str:
        return _stable_hash(self.occurrence_material())

    def semantic_content_fingerprint(self) -> str:
        payload = self.model_dump(
            mode="json",
            exclude={"record_id", "report", "evidence", "schema_version"},
        )
        return _stable_hash(payload)


class BusinessOverview(SourceFact):
    object_type: Literal["BusinessOverview"] = ObjectType.BUSINESS_OVERVIEW.value
    source_text: str = Field(min_length=1)

    @model_validator(mode="after")
    def _source_text_is_evidence_text(self) -> BusinessOverview:
        quotes = [
            item.anchor.bounded_quote
            for item in self.evidence
            if isinstance(item.anchor, TextAnchor)
        ]
        normalized_source = _normalize_text(self.source_text)
        normalized_joined = _normalize_text(" ".join(quotes))
        # A model may quote a contiguous paragraph from a bounded page excerpt
        # instead of echoing the entire excerpt. Keep the source-native guard
        # strict: the returned text must remain a contiguous substring of the
        # supplied Evidence text after whitespace normalization.
        if not quotes or not normalized_source or normalized_source not in normalized_joined:
            raise ValueError("business overview source_text must match text evidence")
        return self


class Segment(SourceFact):
    object_type: Literal["Segment"] = ObjectType.SEGMENT.value
    dimension: str = Field(min_length=1)
    label: str = Field(min_length=1)
    row_class: RowClass | None = None

    @model_validator(mode="after")
    def _adjustment_is_not_an_ordinary_product(self) -> Segment:
        if (
            self.row_class == RowClass.CONSOLIDATION_ADJUSTMENT
            and self.dimension != "adjustment"
        ):
            raise ValueError("consolidation_adjustment must use adjustment dimension")
        if self.dimension == "adjustment" and self.row_class is None:
            raise ValueError("adjustment segment requires row_class")
        return self


class Activity(SourceFact):
    object_type: Literal["Activity"] = ObjectType.ACTIVITY.value
    action: ActivityAction
    activity_actor: str = Field(min_length=1)
    source_actor: str = Field(min_length=1)
    actor_basis: SubjectBasis
    object_name: str = Field(min_length=1)
    source_verb: str = Field(min_length=1)

    @model_validator(mode="after")
    def _actor_basis_is_supported(self) -> Activity:
        if self.actor_basis not in {
            SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
            SubjectBasis.EXPLICIT_ECONOMIC_RELATIONSHIP,
        }:
            raise ValueError("activity_actor requires direct actor evidence")
        return self


_METRIC_SLOT = {
    MetricType.OPERATING_REVENUE: LogicalSlot.REVENUE,
    MetricType.OPERATING_COST: LogicalSlot.COST,
    MetricType.GROSS_MARGIN_REPORTED: LogicalSlot.GROSS_MARGIN,
    MetricType.PRODUCTION_CAPACITY: LogicalSlot.CAPACITY,
    MetricType.CAPACITY_UNDER_CONSTRUCTION: LogicalSlot.CAPACITY_UNDER_CONSTRUCTION,
    MetricType.CAPACITY_UTILIZATION: LogicalSlot.CAPACITY_UTILIZATION,
    MetricType.PRODUCTION_VOLUME: LogicalSlot.PRODUCTION_VOLUME,
    MetricType.SALES_VOLUME: LogicalSlot.SALES_VOLUME,
    MetricType.INVENTORY_VOLUME: LogicalSlot.INVENTORY_VOLUME,
    MetricType.PROCESSING_VOLUME: LogicalSlot.PROCESSING_VOLUME,
    MetricType.CUSTOMER_SALES_AMOUNT: LogicalSlot.CUSTOMER_SALES_AMOUNT,
    MetricType.SUPPLIER_PURCHASE_AMOUNT: LogicalSlot.SUPPLIER_PURCHASE_AMOUNT,
    MetricType.DISCLOSED_SHARE: LogicalSlot.DISCLOSED_SHARE,
}
_CURRENCY_UNITS = {"元", "千元", "万元", "亿元"}
_VOLUME_METRICS = {
    MetricType.PRODUCTION_CAPACITY,
    MetricType.CAPACITY_UNDER_CONSTRUCTION,
    MetricType.PRODUCTION_VOLUME,
    MetricType.SALES_VOLUME,
    MetricType.INVENTORY_VOLUME,
    MetricType.PROCESSING_VOLUME,
}


class Measurement(SourceFact):
    object_type: Literal["Measurement"] = ObjectType.MEASUREMENT.value
    metric_type: MetricType
    logical_slot: LogicalSlot
    measured_object: str = Field(min_length=1)
    segment_dimension: str | None = None
    segment_label: str | None = None
    capacity_kind: CapacityKind | None = None
    processing_direction: ProcessingDirection | None = None
    row_class: RowClass | None = None
    is_restated_comparative: bool = False
    comparison_basis: ComparisonBasis | None = None
    relationship_context: str | None = None

    @model_validator(mode="after")
    def _metric_invariants(self) -> Measurement:
        if _METRIC_SLOT[self.metric_type] != self.logical_slot:
            raise ValueError("metric_type and logical_slot must use the frozen mapping")
        if self.source_native.value is None:
            raise ValueError("measurement requires a source-native value")
        if (
            self.metric_type in _VOLUME_METRICS
            and self.source_native.unit in _CURRENCY_UNITS
        ):
            raise ValueError("physical volume cannot use a currency unit")
        if self.metric_type == MetricType.PRODUCTION_CAPACITY:
            if self.capacity_kind is None:
                raise ValueError("production_capacity requires capacity_kind")
        elif self.capacity_kind is not None:
            raise ValueError("capacity_kind is only valid for production_capacity")
        if self.metric_type == MetricType.PROCESSING_VOLUME:
            if (
                self.processing_direction
                != ProcessingDirection.EXTERNAL_SERVICE_PROVIDED
            ):
                raise ValueError("processing_volume requires external_service_provided")
        elif self.processing_direction is not None:
            raise ValueError("processing_direction is only valid for processing_volume")
        if self.is_restated_comparative and self.comparison_basis is None:
            raise ValueError("restated comparative requires comparison_basis")
        if (
            self.row_class == RowClass.CONSOLIDATION_ADJUSTMENT
            and not self.segment_label
        ):
            raise ValueError("adjustment measurement requires segment_label")
        return self


class Relationship(SourceFact):
    object_type: Literal["Relationship"] = ObjectType.RELATIONSHIP.value
    relation_type: RelationshipType
    object_name: str = Field(min_length=1)
    identity_class: IdentityClass | None = None
    external_entity_id: str | None = None

    @model_validator(mode="after")
    def _identity_is_report_local_when_masked(self) -> Relationship:
        if (
            self.relation_type
            in {
                RelationshipType.CUSTOMER,
                RelationshipType.SUPPLIER,
                RelationshipType.CONTRACT_COUNTERPARTY,
            }
            and self.identity_class is None
        ):
            raise ValueError("counterparty relationship requires identity_class")
        if (
            self.identity_class
            in {
                IdentityClass.REPORT_LOCAL_ANONYMOUS,
                IdentityClass.REPORT_LOCAL_AGGREGATE,
            }
            and self.external_entity_id is not None
        ):
            raise ValueError("report-local identity cannot carry an external entity id")
        return self


class BusinessEvent(SourceFact):
    object_type: Literal["BusinessEvent"] = ObjectType.BUSINESS_EVENT.value
    event_type: str = Field(min_length=1)
    description: str = Field(min_length=1)
    event_date: str | None = None
    regime_effective_at: str | None = None
    comparison_basis: ComparisonBasis | None = None


class BusinessRegime(SourceFact):
    object_type: Literal["BusinessRegime"] = ObjectType.BUSINESS_REGIME.value
    regime_label: str = Field(min_length=1)
    effective_from: str = Field(min_length=1)
    effective_to: str | None = None

    @model_validator(mode="after")
    def _does_not_apply_retroactively(self) -> BusinessRegime:
        if (
            self.effective_from[:4].isdigit()
            and self.reported_period[:4].isdigit()
            and int(self.effective_from[:4]) > int(self.reported_period[:4])
        ):
            raise ValueError("business regime cannot apply retroactively")
        return self


class IndustryPackageAssignment(SourceFact):
    object_type: Literal["IndustryPackageAssignment"] = (
        ObjectType.INDUSTRY_PACKAGE_ASSIGNMENT.value
    )
    package_name: str = Field(min_length=1)
    package_version: str = Field(min_length=1)
    effective_from: str = Field(min_length=1)
    effective_to: str | None = None

    @model_validator(mode="after")
    def _does_not_apply_retroactively(self) -> IndustryPackageAssignment:
        if (
            self.effective_from[:4].isdigit()
            and self.reported_period[:4].isdigit()
            and int(self.effective_from[:4]) > int(self.reported_period[:4])
        ):
            raise ValueError("industry package cannot apply retroactively")
        return self


SemanticRecord: TypeAlias = Annotated[
    BusinessOverview
    | Segment
    | Activity
    | Measurement
    | Relationship
    | BusinessEvent
    | BusinessRegime
    | IndustryPackageAssignment,
    Field(discriminator="object_type"),
]
SEMANTIC_RECORD_ADAPTER = TypeAdapter(SemanticRecord)


class CoverageResult(_StrictModel):
    schema_version: Literal["company_profile_coverage_result.v1"] = (
        "company_profile_coverage_result.v1"
    )
    field_id: str = Field(min_length=1)
    chapter_task: ChapterTask
    requirement_level: RequirementLevel
    status: CoverageStatus
    reason_code: CoverageReasonCode | None = None
    reason: str | None = None
    evidence: tuple[Evidence, ...] = ()
    reason_evidence_text: str | None = None

    @model_validator(mode="after")
    def _coverage_reason_is_typed(self) -> CoverageResult:
        if self.status == CoverageStatus.NOT_DISCLOSED:
            allowed = {
                CoverageReasonCode.EXPLICIT_CONFIDENTIALITY,
                CoverageReasonCode.EXPLICIT_DISCLOSURE_EXEMPTION,
                CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
            }
            if self.reason_code not in allowed:
                raise ValueError("not_disclosed requires a closed reason_code")
            if (
                self.reason_code
                in {
                    CoverageReasonCode.EXPLICIT_CONFIDENTIALITY,
                    CoverageReasonCode.EXPLICIT_DISCLOSURE_EXEMPTION,
                }
                and not self.reason_evidence_text
            ):
                raise ValueError("explicit disclosure reason requires source wording")
        if (
            self.status == CoverageStatus.EXTRACTION_FAILED
            and self.reason_code
            not in {
                CoverageReasonCode.TABLE_CONTEXT_INCOMPLETE,
                CoverageReasonCode.SOURCE_UNREADABLE,
                CoverageReasonCode.COVERAGE_BUDGET_EXHAUSTED,
                CoverageReasonCode.UNIT_AMBIGUOUS,
            }
        ):
            raise ValueError("extraction_failed requires a typed failure reason")
        if self.status == CoverageStatus.UNCLEAR and self.reason_code is None:
            raise ValueError("unclear coverage requires a reason_code")
        return self


def semantic_record_json_schema() -> dict[str, Any]:
    """Return the sole runtime JSON Schema for semantic record responses."""

    return SEMANTIC_RECORD_ADAPTER.json_schema()


def _normalize_text(value: str) -> str:
    normalized = re.sub(r"\s+", " ", value).strip()
    return re.sub(
        r"(?<=[\u3400-\u9fff，。；：、（）])\s+(?=[\u3400-\u9fff，。；：、（）])",
        "",
        normalized,
    )


def _normalized_text_hash(value: str) -> str:
    return hashlib.sha256(_normalize_text(value).encode("utf-8")).hexdigest()


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _stable_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
