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
    r"(主营|主要从事|主要业务|主要经营|经营模式|经营范围|金融服务)"
)
_COMPLETE_COMPANY_BUSINESS = re.compile(
    r"(?<![\u4e00-\u9fff])(?:本公司|公司)(?:业务覆盖|专注于).{2,}"
)
_STEEL_BUSINESS = re.compile(r"公司是.{8,260}主要产品有.{2,80}")
_NARRATIVE_BUSINESS = re.compile(
    r"作为[^。]{2,80}(?:上市公司|工业企业)[^。]{2,}研发制造"
)
_SPEC_PRODUCT = re.compile(r"^(?:厚度|宽度|长度)")
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
    r"手续费及佣金(?:净)?收入|(?:服务费|手续费|佣金)收入|"
    r"收取车辆通行费|通行费收入|"
    r"为客户提供[^。；;，]{2,40}服务|收取[^。；;，]{2,30}费)"
)
_REVENUE_BLOCK_PATTERN = re.compile(
    r"(免费|无偿|不收取|未收取|并不收取|无需(?:支付|收取)|向(?:公司|本公司)收取|"
    r"第三方|代第三方|代收|(?<!向)客户[^。；;]{0,8}收取)"
)
_REVENUE_NEGATION_PREFIX = re.compile(r"(尚未|还未|仍未|并未|没有|未|不|拟|计划)$")
_REPAIR_REVENUE_INFLOW_PATTERN = re.compile(
    r"(营业收入主要来源于|" + _REVENUE_INFLOW_PATTERN.pattern[1:]
)
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
    repair_revenue_sentence: bool = False,
    named_role_repair: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
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

    principal = _assess_principal_business(
        accepted,
        named_role_repair=named_role_repair,
        core_answer_repair=core_answer_repair,
        source_delivery_repair=source_delivery_repair,
    )
    products = _assess_products_services(
        accepted,
        core_answer_repair=core_answer_repair,
        source_delivery_repair=source_delivery_repair,
    )
    revenue = _assess_revenue_model(
        accepted,
        repair_revenue_sentence=repair_revenue_sentence,
        core_answer_repair=core_answer_repair,
        source_delivery_repair=source_delivery_repair,
    )
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
    *,
    named_role_repair: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    for record in records:
        if not isinstance(record, BusinessOverview):
            continue
        if record.field_id != "business_overview_source":
            continue
        text = re.sub(r"\s+", "", record.source_text or "")
        if _PRINCIPAL_PATTERN.search(text) or (
            named_role_repair and _COMPLETE_COMPANY_BUSINESS.search(text)
        ) or (core_answer_repair and _STEEL_BUSINESS.search(text)) or (
            source_delivery_repair and _NARRATIVE_BUSINESS.search(text)
        ):
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
    *,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    rejected = False
    for record in records:
        if isinstance(record, Activity) and record.field_id == "explicit_activity":
            if core_answer_repair and _SPEC_PRODUCT.match(record.object_name.strip()):
                continue
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
            and (
                _PRODUCT_PATTERN.search(record.source_text)
                or (
                    source_delivery_repair
                    and re.search(r"主要业务是|主要从事|主营业务为|主要经营|研发制造", record.source_text or "")
                )
            )
        ):
            supports.append(record)
    if core_answer_repair or source_delivery_repair:
        series = [
            record
            for record in supports
            if isinstance(record, BusinessOverview)
            and re.search(r"主要产品有|主要业务是|主要从事|主营业务为|主要经营|研发制造", record.source_text or "")
        ]
        if series:
            supports = [*series, *[record for record in supports if record not in series]]
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
    *,
    repair_revenue_sentence: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> CoreDimensionAssessment:
    supports: list[SemanticRecord] = []
    totals: list[SemanticRecord] = []
    rejected = False
    for record in records:
        if (
            isinstance(record, BusinessOverview)
            and record.field_id == "business_overview_source"
            and _overview_states_revenue(
                record.source_text,
                repair_revenue_sentence=repair_revenue_sentence,
            )
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
        answered = _answered("revenue_model", supports)
        if repair_revenue_sentence:
            pieces = [
                record.source_text.strip()
                for record in supports
                if isinstance(record, BusinessOverview) and record.source_text.strip()
            ]
            if pieces:
                answered = answered.model_copy(update={"excerpt": "\n".join(pieces)})
        if core_answer_repair:
            structure = _revenue_structure_excerpt(supports)
            if structure:
                answered = answered.model_copy(update={"excerpt": structure})
        if source_delivery_repair:
            industry = _industry_revenue_excerpt(supports)
            if industry:
                answered = answered.model_copy(update={"excerpt": industry})
        return answered
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


def _industry_revenue_excerpt(records: Sequence[SemanticRecord]) -> str:
    """The current-period industry lines, without eliminations or the total."""

    wanted = ("勘探及开发", "炼油", "营销及分销", "化工")
    found: list[str] = []
    for record in records:
        if not isinstance(record, Measurement):
            continue
        if record.segment_dimension != "industry":
            continue
        label = (record.segment_label or "").strip()
        if label in wanted and label not in found:
            found.append(label)
    if len(found) < 2:
        return ""
    return "、".join(found)


def _revenue_structure_excerpt(records: Sequence[SemanticRecord]) -> str:
    """Parent revenue lines followed by the child lines recorded under them."""

    parents: list[str] = []
    children: dict[str, list[str]] = {}
    for record in records:
        if not isinstance(record, Measurement):
            continue
        if record.segment_dimension != "revenue_composition":
            continue
        label = (record.segment_label or "").strip()
        if not label:
            continue
        header = (record.source_native.header or "").strip()
        if header:
            bucket = children.setdefault(header, [])
            if label not in bucket:
                bucket.append(label)
        elif label not in parents:
            parents.append(label)
    lines: list[str] = []
    for parent in parents:
        names = children.get(parent) or []
        lines.append(f"{parent}：" + "、".join(names) if names else parent)
    return "\n".join(lines)


def _overview_states_revenue(
    text: str,
    *,
    repair_revenue_sentence: bool = False,
) -> bool:
    source = re.sub(r"[\r\n]+", "", text) if repair_revenue_sentence else text
    pattern = (
        _REPAIR_REVENUE_INFLOW_PATTERN
        if repair_revenue_sentence
        else _REVENUE_INFLOW_PATTERN
    )
    for sentence in re.split(r"[。；;\n]", source):
        if not sentence.strip():
            continue
        # A collection-denial clause (不收取/未收取/无收费) denies the
        # revenue for the whole sentence, collection by a third party is
        # never company revenue, and a planned collection (尚未/拟/计划/
        # 预期/将+收取/形成) anywhere in the sentence stops a service clause
        # from borrowing it; free wording only constrains its own clause.
        if re.search(r"(?:不收取|未收取|不再收取|无收费)", sentence):
            continue
        if re.search(r"第三方|代第三方|代收", sentence):
            continue
        if re.search(r"(?:尚未|拟|计划|预期|将)[^。；;]{0,12}(?:收取|形成)", sentence):
            continue
        # The subject may sit in an earlier clause of the same sentence
        # ("公司……，按照收费标准收取车辆通行费"), so the company subject is
        # judged at sentence level while the inflow keywords stay clause-level.
        company_subject = re.search(r"本公司|本集团|公司", sentence) is not None
        for statement in _STATEMENT_SPLIT.split(sentence):
            statement = statement.strip()
            if not statement:
                continue
            if _REVENUE_BLOCK_PATTERN.search(statement):
                continue
            match = pattern.search(statement)
            if match is None:
                continue
            if _REVENUE_NEGATION_PREFIX.search(statement[: match.start()]):
                continue
            if re.search(r"(?:尚未|拟|计划|预期|将)[^。；;]{0,12}(?:收取|形成)", statement):
                continue
            if "通行费" in statement and not _toll_statement_is_company_revenue(
                sentence, company_subject
            ):
                continue
            if (
                "为客户提供" in statement
                and not re.search(r"收取|实现|取得|获得", sentence)
            ):
                continue
            return True
    return False


def _toll_statement_is_company_revenue(sentence: str, company_subject: bool) -> bool:
    """A toll statement counts only with the company subject (stated in the
    sentence or carried over from its opening clause) and an actual,
    affirmative collection action — planned, unformed, or third-party tolls
    are not company revenue."""

    if not company_subject:
        return False
    if re.search(r"客户|第三方|代收", sentence):
        return False
    if re.search(r"(?:尚未|拟|计划|预期|将)[^。；;]{0,12}(?:收取|形成)", sentence):
        return False
    return bool(re.search(r"收取|实现|取得|获得", sentence))


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
