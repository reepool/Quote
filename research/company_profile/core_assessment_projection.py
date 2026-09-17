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
    ActivityAction,
    BusinessOverview,
    CoverageResult,
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
_PRINCIPAL_PATTERN = re.compile(
    r"(主营|主要从事|主要业务|经营模式|经营范围|金融服务)"
)
_PRODUCT_PATTERN = re.compile(
    r"(主要产品|主要服务|业务线|产品包括|服务包括|经营范围|"
    r"从事.{1,40}(?:的研发|的生产|的制造|的加工|的销售|服务))"
)
_STATEMENT_SPLIT = re.compile(r"[。；;，,\n]+")
_REVENUE_INFLOW_PATTERN = re.compile(
    r"(收入来[源于自]|营业收入构成|主营业务收入|"
    r"通过.{0,30}(?:销售|提供).{0,30}(?:取得|获得|收取)|"
    r"取得货款|"
    r"向客户(?:销售|提供).{0,24}(?:取得|获得|收取)|"
    r"向客户收取|"
    r"(?:公司|本公司)(?:收取|取得|获得).{0,16}(?:货款|价款|服务费|手续费|佣金|保费)|"
    r"利息净收入|分成收入|经纪业务收入|保费收入|"
    r"手续费及佣金(?:净)?收入|(?:服务费|手续费|佣金)收入)"
)
_REVENUE_BLOCK_PATTERN = re.compile(
    r"(免费|无偿|不收取|未收取|并不收取|无需(?:支付|收取)|向(?:公司|本公司)收取)"
)
_REVENUE_NEGATION_PREFIX = re.compile(r"(尚未|还未|仍未|并未|没有|未|不)$")
_PRODUCT_ACTIONS = frozenset(
    {
        ActivityAction.DEVELOPS,
        ActivityAction.PRODUCES,
        ActivityAction.PROCESSES,
        ActivityAction.SELLS,
        ActivityAction.PROVIDES_SERVICE,
        ActivityAction.OPERATES,
    }
)
_REGION_DIMENSION = re.compile(
    r"(分地区|地区|区域|地域|geographic|region)", re.IGNORECASE
)
_REGION_LABEL = re.compile(
    r"^(?:境内|境外|国内|国外|中国境内|中国境外|"
    r"华东|华北|华南|华中|东北|西北|西南|海外|"
    r"国外地区|国内地区)(?:地区|区域)?$"
)
_TOTAL_LABEL = re.compile(
    r"^(?:合计|总计|小计|汇总|总额|公司合计|total)$", re.IGNORECASE
)
_ELIMINATION_LABEL = re.compile(r"(抵销|抵消|elimination)", re.IGNORECASE)


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
            "no_qualifying_source",
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


def overview_dimension_hits(text: str) -> tuple[str, ...]:
    """Return core dimensions supported by one overview narrative."""

    hits: list[str] = []
    if _PRINCIPAL_PATTERN.search(text):
        hits.append("principal_business")
    if _PRODUCT_PATTERN.search(text):
        hits.append("products_services")
    if _overview_states_revenue(text):
        hits.append("revenue_model")
    return tuple(hits)


def core_record_is_reusable(record: SemanticRecord) -> bool:
    """Return whether one accepted record can stand in for its own core field."""

    if (
        isinstance(record, BusinessOverview)
        and record.field_id == "business_overview_source"
        and overview_dimension_hits(record.source_text)
    ):
        return True
    if (
        isinstance(record, Activity)
        and record.field_id == "explicit_activity"
        and record.action in _PRODUCT_ACTIONS
        and record.object_name.strip()
    ):
        return True
    if (
        isinstance(record, Segment)
        and record.field_id == "segment_dimension"
        and not _is_skeleton_noise_segment(
            dimension=record.dimension,
            label=record.label,
            row_class=record.row_class,
        )
    ):
        return True
    if not isinstance(record, Measurement) or record.field_id != "operating_revenue":
        return False
    if _is_income_mix_measurement(record):
        return True
    if _is_company_total_measurement(record):
        return True
    return (
        record.metric_type == MetricType.OPERATING_REVENUE
        and bool(record.segment_dimension)
        and bool(record.segment_label)
        and not _is_skeleton_noise_segment(
            dimension=record.segment_dimension,
            label=record.segment_label,
            row_class=record.row_class,
        )
    )


def resolved_core_field_ids(records: Sequence[SemanticRecord]) -> frozenset[str]:
    """Return core field IDs that have at least one reusable accepted record."""

    return frozenset(
        record.field_id
        for record in records
        if core_record_is_reusable(record)
    )


_PERIOD_UNKNOWN = ""


def coverage_reconciliation_identity(
    coverage: CoverageResult,
    *,
    accepted_records: Sequence[SemanticRecord] = (),
    report_period: str | None = None,
) -> tuple[str, ...]:
    """Return field/source identity for one coverage row.

    Identity is this coverage's own physical source: field, column, object,
    obligation, report version, page, and table. Accepted records and
    ``report_period`` do not rewrite the key. Page and table are first-class
    and do not come from ``cell_locator``.
    """

    del report_period, accepted_records
    return (
        coverage.field_id,
        coverage.field_id,
        _evidence_period_key(coverage.evidence),
        _evidence_object_key(coverage.evidence),
        coverage.requirement_level.value,
        _evidence_report_key(coverage.evidence),
        _evidence_page_key(coverage.evidence),
        _evidence_table_key(coverage.evidence),
    )


def coverage_identity_can_close(identity: tuple[str, ...]) -> bool:
    """Unknown page, table, column, or object must not close another scope."""

    return all(identity[index] for index in (2, 3, 5, 6, 7))


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
    rejected = False
    for record in records:
        if isinstance(record, Activity) and record.field_id == "explicit_activity":
            if record.action in _PRODUCT_ACTIONS and record.object_name.strip():
                supports.append(record)
            elif record.object_name.strip():
                rejected = True
            continue
        if isinstance(record, Segment) and record.field_id == "segment_dimension":
            if _is_skeleton_noise_segment(
                dimension=record.dimension,
                label=record.label,
                row_class=record.row_class,
            ):
                rejected = True
                continue
            if record.dimension not in {"industry", "product"}:
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
    if rejected:
        return _unanswered("products_services", "no_qualifying_source")
    return _unanswered("products_services", "no_accepted_evidence")


def _assess_revenue_model(
    records: Sequence[SemanticRecord],
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    totals: list[SemanticRecord] = []
    rejected = False
    for record in records:
        if (
            isinstance(record, BusinessOverview)
            and record.field_id == "business_overview_source"
            and _overview_states_revenue(record.source_text)
        ):
            supports.append(record)
            continue
        if isinstance(record, Measurement) and record.field_id == "operating_revenue":
            if _is_income_mix_measurement(record):
                supports.append(record)
                continue
            if record.metric_type != MetricType.OPERATING_REVENUE:
                continue
            if _is_company_total_measurement(record) or not (
                record.segment_dimension and record.segment_label
            ):
                totals.append(record)
                continue
            if _is_skeleton_noise_segment(
                dimension=record.segment_dimension,
                label=record.segment_label,
                row_class=record.row_class,
            ):
                rejected = True
                continue
            supports.append(record)
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
    if rejected:
        return _unanswered("revenue_model", "no_qualifying_source")
    return _unanswered("revenue_model", "no_accepted_evidence")


def _overview_states_revenue(text: str) -> bool:
    for statement in _STATEMENT_SPLIT.split(text):
        statement = statement.strip()
        if not statement:
            continue
        if _REVENUE_BLOCK_PATTERN.search(statement):
            continue
        match = _REVENUE_INFLOW_PATTERN.search(statement)
        if match is None:
            continue
        if _REVENUE_NEGATION_PREFIX.search(statement[: match.start()]):
            continue
        return True
    return False


def _is_company_total_measurement(record: SemanticRecord) -> bool:
    return (
        isinstance(record, Measurement)
        and record.metric_type == MetricType.OPERATING_REVENUE
        and record.measured_object in {"营业收入合计", "营业总收入", "营业收入"}
        and not record.segment_dimension
        and not record.segment_label
    )


def _is_income_mix_measurement(record: SemanticRecord) -> bool:
    if not isinstance(record, Measurement):
        return False
    if (
        record.metric_type == MetricType.DISCLOSED_SHARE
        and record.measured_object == "利息净收入"
        and record.relationship_context == "营业收入"
    ):
        return True
    return (
        record.metric_type == MetricType.OPERATING_REVENUE
        and record.segment_dimension == "income_item"
        and record.segment_label == "利息净收入"
        and record.measured_object == "利息净收入"
    )


def _is_skeleton_noise_segment(
    *,
    dimension: str | None,
    label: str | None,
    row_class: RowClass | None,
) -> bool:
    if row_class == RowClass.CONSOLIDATION_ADJUSTMENT:
        return True
    dim = (dimension or "").strip()
    lab = (label or "").strip()
    if dim == "adjustment":
        return True
    if _REGION_DIMENSION.search(dim) or _REGION_LABEL.fullmatch(lab):
        return True
    if _TOTAL_LABEL.fullmatch(dim) or _TOTAL_LABEL.fullmatch(lab):
        return True
    return bool(_ELIMINATION_LABEL.search(dim) or _ELIMINATION_LABEL.search(lab))


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
        "no_qualifying_source",
    ],
) -> CoreDimensionAssessment:
    return CoreDimensionAssessment(
        dimension_id=dimension_id,
        answered=False,
        missing_reason=reason,
    )


def _evidence_report_key(evidence: Sequence[Evidence]) -> str:
    parts = [
        f"{item.report.report_id}|{item.report.document_version}"
        for item in evidence
    ]
    return "|".join(parts) if parts else _PERIOD_UNKNOWN


def _evidence_page_key(evidence: Sequence[Evidence]) -> str:
    return "|".join(str(item.page) for item in evidence)


def _evidence_table_key(evidence: Sequence[Evidence]) -> str:
    parts: list[str] = []
    for item in evidence:
        label = getattr(item.anchor, "table_label", None)
        if not label or not str(label).strip():
            return _PERIOD_UNKNOWN
        parts.append(str(label).strip())
    return "|".join(parts) if parts else _PERIOD_UNKNOWN


def _evidence_period_key(evidence: Sequence[Evidence]) -> str:
    parts: list[str] = []
    for item in evidence:
        header = getattr(item.anchor, "column_header", None)
        locator = getattr(item.anchor, "cell_locator", None)
        if header and str(header).strip():
            parts.append(str(header).strip())
        if locator and str(locator).strip():
            parts.append(str(locator).strip())
    return "|".join(parts) if parts else _PERIOD_UNKNOWN


def _evidence_object_key(evidence: Sequence[Evidence]) -> str:
    parts: list[str] = []
    for item in evidence:
        row = getattr(item.anchor, "row_label", None)
        if row:
            parts.append(str(row).strip())
            continue
        quote = getattr(item.anchor, "bounded_quote", None)
        if quote:
            parts.append(re.sub(r"\s+", " ", str(quote).strip()))
        else:
            parts.append(f"page:{item.page}")
    return "|".join(part for part in parts if part)


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
