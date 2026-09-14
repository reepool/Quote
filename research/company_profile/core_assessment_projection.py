"""Program-owned three-dimension core assessment.

This projection evaluates accepted records only. It does not add ChapterTask
values, request field IDs, or LLM payload fields.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .contracts import CompanyProfileTaskResult, DispositionStatus
from .models import (
    PRODUCTION_AUTHORIZATION,
    Activity,
    BusinessOverview,
    Evidence,
    Measurement,
    MetricType,
    ReportIdentity,
    RowClass,
    Segment,
    SemanticRecord,
    TableAnchor,
    TextAnchor,
)

COMMON_CORE_MAPPING_VERSION = "company_profile_common_core_mapping.v1"
CORE_DIMENSION_IDS = (
    "principal_business",
    "products_services",
    "revenue_model",
)
_PRINCIPAL_PATTERN = re.compile(r"(主营|主要从事|主要业务|经营模式)")
_PRODUCT_PATTERN = re.compile(
    r"(主要产品|主要服务|业务线|产品包括|服务包括|"
    r"从事.{1,40}(?:的研发|的生产|的制造|的加工|的销售|服务))"
)
_REVENUE_NARRATIVE_PATTERN = re.compile(
    r"(收入来[源于自]|营业收入构成|主营业务收入|服务费|手续费|佣金|"
    r"经纪业务|利息净收入|保费|分成收入)"
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CoreDimensionAssessment(_StrictModel):
    dimension_id: Literal[
        "principal_business", "products_services", "revenue_model"
    ]
    answered: bool
    supporting_record_ids: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    excerpt: str | None = None
    anchor: dict[str, Any] | None = None
    missing_reason: (
        Literal[
            "no_accepted_evidence",
            "overview_lacks_dimension",
            "numeric_total_only",
        ]
        | None
    ) = None


class CompanyProfileCoreAssessment(_StrictModel):
    mapping_version: Literal["company_profile_common_core_mapping.v1"] = (
        COMMON_CORE_MAPPING_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    report: ReportIdentity
    principal_business: CoreDimensionAssessment
    products_services: CoreDimensionAssessment
    revenue_model: CoreDimensionAssessment
    core_complete: bool

    @property
    def dimensions(self) -> tuple[CoreDimensionAssessment, ...]:
        return (
            self.principal_business,
            self.products_services,
            self.revenue_model,
        )


def core_assessment_schema_manifest() -> dict[str, Any]:
    schema = CompanyProfileCoreAssessment.model_json_schema()
    return {
        "mapping_version": COMMON_CORE_MAPPING_VERSION,
        "dimension_ids": CORE_DIMENSION_IDS,
        "chapter_tasks": (
            "extract_business_overview",
            "extract_segment_financials",
        ),
        "source_field_ids": (
            "business_overview_source",
            "explicit_activity",
            "segment_dimension",
            "operating_revenue",
        ),
        "title": schema.get("title", "CompanyProfileCoreAssessment"),
        "schema": schema,
    }


def project_core_assessment(
    *,
    report: ReportIdentity,
    task_results: Sequence[CompanyProfileTaskResult],
) -> CompanyProfileCoreAssessment:
    """Evaluate the common core from accepted same-report records."""

    accepted: list[SemanticRecord] = []
    for result in task_results:
        statuses = {item.target_id: item.status for item in result.dispositions}
        for record in result.records:
            if record.report != report:
                raise ValueError("core assessment cannot mix records from another report")
            if (
                statuses.get(record.record_id) == DispositionStatus.ACCEPTED_FOR_REVIEW
                and record.data_status == "research_fixture"
            ):
                accepted.append(record)

    principal = _assess_principal_business(accepted)
    products = _assess_products_services(accepted)
    revenue = _assess_revenue_model(accepted)
    return CompanyProfileCoreAssessment(
        report=report,
        principal_business=principal,
        products_services=products,
        revenue_model=revenue,
        core_complete=principal.answered
        and products.answered
        and revenue.answered,
    )


def _assess_principal_business(
    records: Sequence[SemanticRecord],
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    for record in records:
        if not isinstance(record, BusinessOverview):
            continue
        if record.field_id != "business_overview_source":
            continue
        if _PRINCIPAL_PATTERN.search(record.source_text):
            supports.append(record)
    if supports:
        return _answered("principal_business", supports)
    if any(
        isinstance(record, BusinessOverview)
        and record.field_id == "business_overview_source"
        for record in records
    ):
        return _unanswered("principal_business", "overview_lacks_dimension")
    return _unanswered("principal_business", "no_accepted_evidence")


def _assess_products_services(
    records: Sequence[SemanticRecord],
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    for record in records:
        if isinstance(record, Activity) and record.field_id == "explicit_activity":
            if record.object_name.strip():
                supports.append(record)
            continue
        if isinstance(record, Segment) and record.field_id == "segment_dimension":
            if record.row_class == RowClass.CONSOLIDATION_ADJUSTMENT:
                continue
            if record.label.strip():
                supports.append(record)
            continue
        if (
            isinstance(record, BusinessOverview)
            and record.field_id == "business_overview_source"
            and _PRODUCT_PATTERN.search(record.source_text)
        ):
            supports.append(record)
    if supports:
        return _answered("products_services", supports)
    if any(
        isinstance(record, BusinessOverview)
        and record.field_id == "business_overview_source"
        for record in records
    ):
        return _unanswered("products_services", "overview_lacks_dimension")
    return _unanswered("products_services", "no_accepted_evidence")


def _assess_revenue_model(
    records: Sequence[SemanticRecord],
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    totals: list[SemanticRecord] = []
    for record in records:
        if (
            isinstance(record, BusinessOverview)
            and record.field_id == "business_overview_source"
            and _REVENUE_NARRATIVE_PATTERN.search(record.source_text)
        ):
            supports.append(record)
            continue
        if (
            isinstance(record, Measurement)
            and record.field_id == "operating_revenue"
            and record.metric_type == MetricType.OPERATING_REVENUE
        ):
            if record.segment_dimension and record.segment_label:
                supports.append(record)
            else:
                totals.append(record)
    if supports:
        return _answered("revenue_model", supports)
    if totals:
        return _unanswered("revenue_model", "numeric_total_only")
    if any(
        isinstance(record, BusinessOverview)
        and record.field_id == "business_overview_source"
        for record in records
    ):
        return _unanswered("revenue_model", "overview_lacks_dimension")
    return _unanswered("revenue_model", "no_accepted_evidence")


def _answered(
    dimension_id: Literal[
        "principal_business", "products_services", "revenue_model"
    ],
    records: Sequence[SemanticRecord],
) -> CoreDimensionAssessment:
    primary = records[0]
    excerpt, anchor = _excerpt_and_anchor(primary)
    evidence_ids = tuple(
        dict.fromkeys(
            evidence.evidence_id
            for record in records
            for evidence in record.evidence
        )
    )
    return CoreDimensionAssessment(
        dimension_id=dimension_id,
        answered=True,
        supporting_record_ids=tuple(record.record_id for record in records),
        evidence_ids=evidence_ids,
        excerpt=excerpt,
        anchor=anchor,
        missing_reason=None,
    )


def _unanswered(
    dimension_id: Literal[
        "principal_business", "products_services", "revenue_model"
    ],
    reason: Literal[
        "no_accepted_evidence",
        "overview_lacks_dimension",
        "numeric_total_only",
    ],
) -> CoreDimensionAssessment:
    return CoreDimensionAssessment(
        dimension_id=dimension_id,
        answered=False,
        missing_reason=reason,
    )


def _excerpt_and_anchor(record: SemanticRecord) -> tuple[str | None, dict[str, Any] | None]:
    if isinstance(record, BusinessOverview):
        excerpt = record.source_text
    elif isinstance(record, Activity):
        excerpt = record.object_name
    elif isinstance(record, Segment):
        excerpt = record.label
    elif isinstance(record, Measurement):
        excerpt = record.segment_label or record.measured_object
    else:
        excerpt = None
    evidence = record.evidence[0] if record.evidence else None
    return excerpt, _anchor_payload(evidence)


def _anchor_payload(evidence: Evidence | None) -> dict[str, Any] | None:
    if evidence is None:
        return None
    payload: dict[str, Any] = {
        "evidence_id": evidence.evidence_id,
        "page": evidence.page,
        "section_title": evidence.section_title,
    }
    if isinstance(evidence.anchor, TextAnchor):
        payload["anchor_type"] = "text"
        payload["bounded_quote"] = evidence.anchor.bounded_quote
    elif isinstance(evidence.anchor, TableAnchor):
        payload["anchor_type"] = "table"
        payload["row_label"] = evidence.anchor.row_label
        payload["cell_locator"] = evidence.anchor.cell_locator
    return payload
