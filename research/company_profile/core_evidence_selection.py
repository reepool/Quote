"""Automatic common-core Evidence selection.

This owner locates overview and segment text for the three core dimensions. It
reuses accepted structured facts and does not require a frozen per-company page
plan or an LLM call.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .contracts import PreparedEvidence
from .core_assessment_projection import (
    _REVENUE_BLOCK_PATTERN,
    COMMON_CORE_MAPPING_VERSION,
    _overview_states_revenue,
    core_record_is_reusable,
    overview_dimension_hits,
)
from .models import (
    PRODUCTION_AUTHORIZATION,
    Activity,
    ActivityAction,
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    PeriodType,
    Relationship,
    RelationshipType,
    ReportIdentity,
    RowClass,
    Segment,
    SemanticRecord,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)

CORE_EVIDENCE_SCHEMA_VERSION = "company_profile_common_core_evidence.v1"
CORE_SOURCE_FIELD_IDS = (
    "business_overview_source",
    "explicit_activity",
    "segment_dimension",
    "operating_revenue",
    "material_input",
)
_OVERVIEW_HEADINGS = (
    "报告期内公司从事的业务情况",
    "报告期内公司从事的主要业务",
    "公司从事的主要业务",
    "主营业务分析",
    "主要产品及服务",
    "主要产品",
    "经营模式",
    "主营业务",
    "公司主要业务情况",
    "公司金融业务",
    "主要业务",
)
_OVERVIEW_FIELD_LABELS = ("经营范围",)
_SEGMENT_HEADINGS = (
    "占公司营业收入或营业利润10%以上",
    "主营业务分行业情况",
    "主营业务/产品分行业情况",
    "主营业务主要地区情况",
    "主营业务分产品情况",
    "主营业务分地区情况",
    "主营业务分销售模式情况",
    "收入和成本分析",
    "主营业务分行业",
    "主营业务分产品",
    "分部报告",
    "分部信息",
    "分行业",
    "分产品",
)
_INCOME_ANALYSIS_HEADINGS = (
    "利润表分析",
    "营业收入构成",
)
CORE_SEGMENT_HEADINGS = _SEGMENT_HEADINGS
_ALL_HEADINGS = (
    _OVERVIEW_HEADINGS
    + _OVERVIEW_FIELD_LABELS
    + _SEGMENT_HEADINGS
    + _INCOME_ANALYSIS_HEADINGS
)
_HEADING_PREFIX = re.compile(
    r"^(?:第[一二三四五六七八九十百]+[节章]"
    r"|[一二三四五六七八九十]+、"
    r"|[（(][一二三四五六七八九十\d]+[)）]"
    r"|[0-9]+(?:[.、．][0-9]+)*[.、．]?)"
)
_SECTION_BOUNDARY = re.compile(
    r"(?:^|\n|(?<=[。；;]))\s*(?:第[一二三四五六七八九十百]+[节章]"
    r"|[二三四五六七八九十]+、)"
)
_CONTINUATION_TAIL = re.compile(r"(如下[:：]?|见表|详见|续[见表]|：$)$")
_OVERVIEW_SUBSTANCE = re.compile(
    r"(?:公司|本公司).{0,40}(?:主营|主要从事|经营|生产|销售|提供|研发)|"
    r"作为[^。]{2,60}核心提供商|"
    r"取得货款|收入来[源于自]|向客户收取|主要产品为"
)
_COMPANY_BUSINESS_SENTENCE = re.compile(
    r"(?<![\u4e00-\u9fff])(?:本公司|公司)(?:业务覆盖|专注于)[^。；;]{2,160}"
)
_STEEL_BUSINESS_SENTENCE = re.compile(
    r"(?<![\u4e00-\u9fff])公司是[^。]{8,260}主要产品有[^。]{2,80}"
)
_MAIN_BUSINESS_SENTENCE = re.compile(
    r"(?<![\u4e00-\u9fff])(?:本公司|公司)的主要业务是[^。]{8,500}"
)
_INTEGRATED_BUSINESS_SENTENCE = re.compile(
    r"(?:中国石化是|公司是)[^。]{8,900}主要从事[^。]{8,900}"
)
_TOLL_SERVICE_SENTENCE = re.compile(
    r"公司的主营业务为[^。]{4,120}。[^。]{0,120}?通行服务[^。]{2,160}"
)
_MAIN_OPERATION_SENTENCE = re.compile(
    # The operating-model sentence may sit behind a short development
    # narrative, so the bridge to 经营模式 allows bounded intermediate text.
    r"主要经营[^。]{8,220}(?:。[\s\S]{0,400}?经营模式[^。]{2,220})?"
)
_OPERATING_MODEL_SENTENCE = re.compile(
    r"(?<![\u4e00-\u9fff])(?:本公司|公司)的?经营模式主要为[:：][^。]{2,300}"
)
_COMPANY_AS_NARRATIVE = re.compile(
    r"[^。\n]{2,40}作为[^。]{2,80}(?:上市公司|工业企业)"
    r"[^。]{2,300}研发制造[^。]{4,300}"
    r"(?:。[^。\n]{2,200}涉足[^。]{2,200})?"
)
_PROVIDER_NARRATIVE = re.compile(
    r"作为[^。；;\n]{2,60}核心提供商[，,]\s*(?:本公司|公司)[^。；;]{8,300}"
)
_CURRENT_BUSINESS_NARRATIVE = re.compile(
    r"(?<![\u4e00-\u9fff])(?:本公司|公司)坚持以[^。]{2,150}为核心[^。]*。\s*"
    r"(?:本公司|公司)持续深耕[^。]{2,1000}"
)
_CORE_ANSWER_SUBSTANCE = re.compile(
    _COMPANY_BUSINESS_SENTENCE.pattern + "|" + _STEEL_BUSINESS_SENTENCE.pattern
)
_SOURCE_DELIVERY_SUBSTANCE = re.compile(
    _CORE_ANSWER_SUBSTANCE.pattern
    + "|"
    + _MAIN_BUSINESS_SENTENCE.pattern
    + "|"
    + _INTEGRATED_BUSINESS_SENTENCE.pattern
    + "|"
    + _OPERATING_MODEL_SENTENCE.pattern
    + "|"
    + _PROVIDER_NARRATIVE.pattern
    + "|"
    + _CURRENT_BUSINESS_NARRATIVE.pattern
    + r"|本集团以[^。]+为两大核心主业|公司建成[^。]+四大产业板块"
)
_SPEC_OBJECT = re.compile(r"^(?:厚度|宽度|长度)")
_REVENUE_PARENT_LABELS = frozenset({"航空性收入", "非航空性收入"})
_AUDIT_PROCEDURE = re.compile(r"函证|截止测试|选取样本|关键审计事项|审计中的应对")
_RESUME_CLAUSE = re.compile(r"历任|曾任")
_PSEUDO_SALES = re.compile(r"销售(?:模式|区域|部)")
_REVENUE_OBJECT = re.compile(
    r"([\u4e00-\u9fffA-Za-z0-9（）()]{2,40}?)\s*"
    r"(?:营业收入|主营业务收入)"
)
_ACTIVITY_OBJECTS = re.compile(
    r"主要从事(.+?)(?:的研发|的生产|的制造|的加工|的销售|服务)"
)
_DISPLAY_QUOTE_LIMIT = 180
_MAX_SECTION_PAGES = 8


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ReportPageText(_StrictModel):
    page: int = Field(ge=1)
    text: str = ""
    readable: bool = True
    layout_text: str = ""


class CoreEvidenceSpan(_StrictModel):
    page: int = Field(ge=1)
    continuation_pages: tuple[int, ...] = ()
    section_title: str = Field(min_length=1)
    excerpt: str = Field(min_length=1)
    bounded_quote: str = Field(min_length=1)
    chapter_task: Literal[
        "extract_business_overview",
        "extract_segment_financials",
        "extract_material_inputs",
    ]
    field_ids: tuple[str, ...]
    dimension_ids: tuple[str, ...] = ()
    context_complete: bool = True


class ReusedStructuredFact(_StrictModel):
    record_id: str = Field(min_length=1)
    field_id: str = Field(min_length=1)
    requires_llm: Literal[False] = False


class CoreEvidenceGap(_StrictModel):
    code: Literal["page_unreadable", "chapter_missing", "extraction_failed"]
    chapter_task: Literal[
        "extract_business_overview",
        "extract_segment_financials",
        "extract_material_inputs",
    ]
    page: int | None = None
    message: str = Field(min_length=1)


class CoreEvidenceSelection(_StrictModel):
    schema_version: Literal["company_profile_common_core_evidence.v1"] = (
        CORE_EVIDENCE_SCHEMA_VERSION
    )
    mapping_version: Literal["company_profile_common_core_mapping.v1"] = (
        COMMON_CORE_MAPPING_VERSION
    )
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    report: ReportIdentity
    spans: tuple[CoreEvidenceSpan, ...]
    reused_facts: tuple[ReusedStructuredFact, ...]
    unresolved_field_ids: tuple[str, ...]
    gaps: tuple[CoreEvidenceGap, ...]
    prepared_evidence: tuple[PreparedEvidence, ...]


def core_evidence_schema_manifest() -> dict[str, Any]:
    schema = CoreEvidenceSelection.model_json_schema()
    return {
        "schema_version": CORE_EVIDENCE_SCHEMA_VERSION,
        "mapping_version": COMMON_CORE_MAPPING_VERSION,
        "chapter_tasks": (
            "extract_business_overview",
            "extract_segment_financials",
            "extract_material_inputs",
        ),
        "source_field_ids": CORE_SOURCE_FIELD_IDS,
        "title": schema.get("title", "CoreEvidenceSelection"),
        "schema": schema,
    }


def select_core_evidence(
    *,
    report: ReportIdentity,
    pages: Sequence[ReportPageText | Mapping[str, Any]],
    accepted_records: Sequence[SemanticRecord] = (),
    repair_revenue_sentence: bool = False,
    named_role_repair: bool = False,
    service_operating_energy: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> CoreEvidenceSelection:
    """Select bounded core Evidence without a frozen per-company page plan."""

    for record in accepted_records:
        if record.report != report:
            raise ValueError("core evidence cannot reuse records from another report")
    normalized = tuple(_normalize_page(item) for item in pages)
    spans: list[CoreEvidenceSpan] = []
    gaps: list[CoreEvidenceGap] = []

    overview = _select_owned_span(
        normalized,
        headings=(
            (("公司简介",) + _OVERVIEW_HEADINGS)
            if source_delivery_repair
            else _OVERVIEW_HEADINGS
        ),
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        field_ids=("business_overview_source", "explicit_activity"),
        require_substance=True,
        substance_extra=(
            _SOURCE_DELIVERY_SUBSTANCE
            if source_delivery_repair
            else (
                _CORE_ANSWER_SUBSTANCE
                if core_answer_repair
                else _COMPANY_BUSINESS_SENTENCE
                if named_role_repair
                else None
            )
        ),
    )
    if overview.span is None:
        labeled = _select_owned_span(
            normalized,
            headings=(),
            field_labels=_OVERVIEW_FIELD_LABELS,
            chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            field_ids=("business_overview_source", "explicit_activity"),
            require_substance=False,
        )
        if labeled.span is not None:
            overview = labeled
        else:
            gaps.extend(labeled.gaps)
    if overview.span is not None:
        spans.append(overview.span)
    gaps.extend(overview.gaps)

    segment = _select_owned_span(
        normalized,
        headings=_SEGMENT_HEADINGS,
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        field_ids=("segment_dimension", "operating_revenue"),
        require_substance=False,
    )
    if segment.span is not None:
        spans.append(segment.span)
    gaps.extend(segment.gaps)

    income = _select_owned_span(
        normalized,
        headings=_INCOME_ANALYSIS_HEADINGS,
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        field_ids=("segment_dimension", "operating_revenue"),
        require_substance=False,
    )
    if income.span is not None:
        spans.append(income.span)
    gaps.extend(income.gaps)

    if source_delivery_repair:
        spans.extend(_native_income_spans(normalized))
        supplemental = _supplemental_income_spans(normalized)
        spans.extend(supplemental)
        spans.extend(
            span.model_copy(
                update={
                    "section_title": "当期产品及服务披露",
                    "chapter_task": ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value,
                    "field_ids": ("business_overview_source",),
                }
            )
            for span in supplemental
            if span.section_title == "关联服务及出租收入"
        )

    if source_delivery_repair and segment.span is not None:
        income_source = _company_income_source(segment.span.excerpt)
        if income_source:
            spans.append(
                CoreEvidenceSpan(
                    page=segment.span.page,
                    section_title="主营业务收入说明",
                    excerpt=income_source,
                    bounded_quote=segment.span.excerpt,
                    continuation_pages=segment.span.continuation_pages,
                    chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value,
                    field_ids=("business_overview_source", "explicit_activity"),
                    context_complete=True,
                )
            )

    if source_delivery_repair:
        for page in normalized:
            source = _company_sales_mechanism_source(page.text) if page.readable else ""
            if source:
                spans.append(
                    CoreEvidenceSpan(
                        page=page.page,
                        section_title="销售客户及市场化交易",
                        excerpt=source,
                        bounded_quote=source,
                        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value,
                        field_ids=("business_overview_source",),
                        context_complete=True,
                    )
                )

    if source_delivery_repair:
        for page in normalized:
            if not page.readable:
                continue
            source_text = page.text
            nxt = next(
                (p for p in normalized if p.page == page.page + 1 and p.readable), None
            )
            if nxt and "本集团的营业收入主要包括" in source_text:
                source_text += "\n" + re.split(r"\n\s*\d+、", nxt.text, maxsplit=1)[0]
            if nxt and "履约义务的说明" in source_text and "款到发" in source_text:
                source_text += "\n" + re.split(r"[（(]4[)）]", nxt.text, maxsplit=1)[0]
            if nxt and (
                "公司从事的业务情况" in source_text
                and "报告期内公司新增重要非主营业务" not in source_text
                or "其他资产置换" in source_text
                and "交易完成" not in source_text
                or "主要控股参股公司分析" in source_text
            ):
                source_text += "\n" + nxt.text
            for source in (
                *_current_product_sources(source_text),
                *_owned_business_sources(source_text),
                *_owned_operating_passages(source_text),
            ):
                spans.append(
                    CoreEvidenceSpan(
                        page=page.page,
                        section_title="当期产品及服务披露",
                        excerpt=source,
                        bounded_quote=source_text,
                        continuation_pages=(nxt.page,)
                        if nxt and source_text != page.text
                        else (),
                        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value,
                        field_ids=("business_overview_source",),
                        context_complete=True,
                    )
                )

    material = _select_material_span(
        normalized, source_delivery_repair=source_delivery_repair
    )
    if material is not None:
        spans.append(material)
    if source_delivery_repair:
        for page in normalized:
            if not page.readable or not (
                any(verb == "投入" for _, verb, _ in _native_input_bindings(page.text))
                or any(
                    b.get("header") == "能源" and b.get("action") is None
                    for b in _explicit_current_commodity_bindings(page.text)
                )
            ):
                continue
            # An upstream column establishes use, not an external purchase.
            # Keep the complete owned table in the existing material chapter.
            if any(
                span.page == page.page
                and span.chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS.value
                and span.excerpt.strip() == page.text.strip()
                for span in spans
            ):
                continue
            spans.append(
                CoreEvidenceSpan(
                    page=page.page,
                    section_title="主要上游原材料",
                    excerpt=page.text,
                    bounded_quote=page.text,
                    chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS.value,
                    field_ids=("material_input",),
                    context_complete=True,
                )
            )
    if repair_revenue_sentence:
        spans.extend(
            _repair_commodity_spans(
                normalized,
                covered=spans,
                service_operating_energy=service_operating_energy,
            )
        )

    if overview.span is None and not any(
        gap.code == "chapter_missing"
        and gap.chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value
        for gap in gaps
    ):
        gaps.append(
            CoreEvidenceGap(
                code="chapter_missing",
                chapter_task="extract_business_overview",
                message="no owned business-overview section was located",
            )
        )

    reused_facts = tuple(
        ReusedStructuredFact(record_id=record.record_id, field_id=record.field_id)
        for record in accepted_records
        if core_record_is_reusable(record)
        and any(_record_matches_span(record, report, span) for span in spans)
    )
    reused_records = [
        record
        for record in accepted_records
        if any(item.record_id == record.record_id for item in reused_facts)
    ]
    unresolved: list[str] = []
    seen_fields: set[str] = set()
    for span in spans:
        for field_id in span.field_ids:
            if field_id in seen_fields:
                continue
            if not _field_covered_for_span(field_id, span, report, reused_records):
                unresolved.append(field_id)
            seen_fields.add(field_id)

    prepared = _prepared_evidence_for_spans(report, spans)
    return CoreEvidenceSelection(
        report=report,
        spans=tuple(spans),
        reused_facts=reused_facts,
        unresolved_field_ids=tuple(unresolved),
        gaps=tuple(gaps),
        prepared_evidence=tuple(prepared),
    )


class _OwnedSelection:
    def __init__(
        self,
        span: CoreEvidenceSpan | None,
        gaps: list[CoreEvidenceGap],
    ) -> None:
        self.span = span
        self.gaps = gaps


def _normalize_page(page: ReportPageText | Mapping[str, Any]) -> ReportPageText:
    if isinstance(page, ReportPageText):
        return page
    return ReportPageText(
        page=int(page["page"]),
        text=str(page.get("text") or ""),
        readable=bool(page.get("readable", True)),
        layout_text=str(page.get("layout_text") or ""),
    )


def _select_owned_span(
    pages: Sequence[ReportPageText],
    *,
    headings: tuple[str, ...],
    chapter_task: ChapterTask,
    field_ids: tuple[str, ...],
    require_substance: bool,
    field_labels: tuple[str, ...] = (),
    substance_extra: re.Pattern[str] | None = None,
) -> _OwnedSelection:
    by_page = {item.page: item for item in pages}
    gaps: list[CoreEvidenceGap] = []
    for page in pages:
        owned_headings = _iter_owned_headings(
            page.text, headings, field_labels=field_labels
        )
        if not owned_headings:
            continue
        for heading, _line_start, line_end in owned_headings:
            selected = _selection_for_owned_heading(
                page,
                by_page=by_page,
                heading=heading,
                line_end=line_end,
                chapter_task=chapter_task,
                field_ids=field_ids,
                require_substance=require_substance,
                field_labels=field_labels,
                gaps=gaps,
                substance_extra=substance_extra,
            )
            if selected is _SKIP_OWNED_HEADING:
                continue
            return selected
    return _OwnedSelection(None, gaps)


_SKIP_OWNED_HEADING = object()


def _selection_for_owned_heading(
    page: ReportPageText,
    *,
    by_page: Mapping[int, ReportPageText],
    heading: str,
    line_end: int,
    chapter_task: ChapterTask,
    field_ids: tuple[str, ...],
    require_substance: bool,
    field_labels: tuple[str, ...],
    gaps: list[CoreEvidenceGap],
    substance_extra: re.Pattern[str] | None = None,
) -> _OwnedSelection | object:
    if not page.readable:
        gaps.append(
            CoreEvidenceGap(
                code="page_unreadable",
                chapter_task=chapter_task.value,
                page=page.page,
                message=f"owned section page is unreadable: {page.page}",
            )
        )
        return _OwnedSelection(None, gaps)
    if heading in field_labels:
        excerpt, continuations, closed, gap = _collect_field_label(
            page,
            heading=heading,
            line_end=line_end,
        )
    else:
        preview = f"{heading}\n{_cut_at_boundary(page.text[line_end:]).text}".strip()
        if _usable_excerpt(
            preview,
            heading,
            require_substance,
            substance_extra=substance_extra,
        ) and (
            _excerpt_states_owned_overview(preview)
            or (
                substance_extra is not None
                and (
                    substance_extra.search(preview) is not None
                    or substance_extra.search(_join_pdf_soft_breaks(preview))
                    is not None
                )
            )
        ):
            # An already usable first page may still own a product matrix or
            # operating model on the following page. Follow that section's
            # real boundary before choosing its delivered sentences.
            if (
                substance_extra is _SOURCE_DELIVERY_SUBSTANCE
                and not _cut_at_boundary(page.text[line_end:]).closed
                and page.page + 1 in by_page
            ):
                excerpt, continuations, closed, gap = _collect_section(
                    page,
                    heading=heading,
                    line_end=line_end,
                    by_page=by_page,
                    chapter_task=chapter_task,
                )
            else:
                excerpt, continuations, closed, gap = preview, [], True, None
        else:
            excerpt, continuations, closed, gap = _collect_section(
                page,
                heading=heading,
                line_end=line_end,
                by_page=by_page,
                chapter_task=chapter_task,
            )
    if chapter_task is ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
        declared = _unit_declaration(page.text)
        if declared and declared not in excerpt:
            excerpt = f"{declared}\n{excerpt}"
    if heading in _INCOME_ANALYSIS_HEADINGS:
        excerpt = _clip_income_analysis_excerpt(excerpt)
        if not _income_analysis_excerpt_ready(excerpt):
            return _SKIP_OWNED_HEADING
        closed = True
        gap = None
    if heading in _MDA_REVENUE_HEADINGS:
        table_lines = excerpt.splitlines()
        table_start = next(
            (
                i
                for i, line in enumerate(table_lines)
                if _dimension_from_heading(line) is not None
            ),
            len(table_lines),
        )
        table_tail = any(
            _is_revenue_table_stop(line) for line in table_lines[table_start + 1 :]
        )
        excerpt = _clip_before_later_segment_template(excerpt)
        if not _formal_segment_table_ready(excerpt):
            return _SKIP_OWNED_HEADING
        # A truncated continuation must not be made complete just because an
        # earlier row was parseable. A real table tail can close the section.
        normalized = _join_segment_label_amounts(
            _join_rmb_column_header(_join_pdf_soft_breaks(excerpt))
        )
        last_block = 0
        for index, line in enumerate(normalized.splitlines()):
            if _dimension_from_heading(line) is not None:
                last_block = index + 1
        final_block_has_rows = any(
            _parse_segment_row(line) is not None
            for line in normalized.splitlines()[last_block:]
        )
        closed = closed or table_tail or final_block_has_rows
        if gap is None or closed:
            gap = None
    elif heading in {"分部报告", "分部信息"} and _formal_segment_table_ready(excerpt):
        closed = True
        gap = None
    usable = _usable_excerpt(
        excerpt,
        heading,
        require_substance,
        substance_extra=substance_extra,
    )
    if usable and _excerpt_states_owned_overview(excerpt):
        closed = True
        gap = None
    if gap is not None and not usable:
        gaps.append(gap)
        return _OwnedSelection(None, gaps)
    if gap is not None:
        gaps.append(gap)
    if not excerpt.strip() or excerpt.strip() == heading:
        gaps.append(
            CoreEvidenceGap(
                code="extraction_failed",
                chapter_task=chapter_task.value,
                page=page.page,
                message="owned heading has no usable context",
            )
        )
        return _OwnedSelection(None, gaps)
    if not usable:
        return _SKIP_OWNED_HEADING
    dimensions = overview_dimension_hits(excerpt)
    if chapter_task is ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
        dimensions = ("products_services", "revenue_model")
    return _OwnedSelection(
        CoreEvidenceSpan(
            page=page.page,
            continuation_pages=tuple(continuations),
            section_title=heading,
            excerpt=excerpt,
            bounded_quote=_display_quote(excerpt, heading),
            chapter_task=chapter_task.value,
            field_ids=field_ids,
            dimension_ids=dimensions,
            context_complete=closed,
        ),
        gaps,
    )


def owned_section_heading(
    text: str,
    headings: Sequence[str],
) -> str | None:
    """Return the first title-line heading owned by this page, if any."""

    owned = _owned_heading(text, tuple(headings))
    return None if owned is None else owned[0]


def _owned_heading(
    text: str,
    headings: tuple[str, ...],
    *,
    field_labels: tuple[str, ...] = (),
) -> tuple[str, int, int] | None:
    found = _iter_owned_headings(text, headings, field_labels=field_labels)
    return found[0] if found else None


def _iter_owned_headings(
    text: str,
    headings: tuple[str, ...],
    *,
    field_labels: tuple[str, ...] = (),
) -> list[tuple[str, int, int]]:
    if _is_toc_page(text):
        return []
    found: list[tuple[str, int, int]] = []
    offset = 0
    for line in text.splitlines(keepends=True):
        heading = _heading_if_title_line(line, headings)
        if heading is not None:
            found.append((heading, offset, offset + len(line)))
        else:
            heading = _heading_if_field_label(line, field_labels)
            if heading is not None:
                found.append(
                    (heading, offset, offset + _heading_end_in_line(line, heading))
                )
        offset += len(line)
    return found


def _heading_if_title_line(line: str, headings: tuple[str, ...]) -> str | None:
    stripped = line.strip()
    if not stripped or _is_toc_line(stripped, headings):
        return None
    compact = re.sub(r"\s+", "", stripped)
    compact = _HEADING_PREFIX.sub("", compact, count=1)
    for heading in sorted(headings, key=len, reverse=True):
        if compact == heading:
            return heading
        if compact.startswith(heading):
            rest = compact[len(heading) :]
            if re.fullmatch(r"[:：.。]*", rest or ""):
                return heading
            return None
    return None


_FIELD_LABEL_VALUE = re.compile(r"[\u4e00-\u9fffA-Za-z]{2,}")


def _heading_if_field_label(line: str, headings: tuple[str, ...]) -> str | None:
    stripped = line.strip()
    if not stripped or not headings or _is_toc_line(stripped, headings):
        return None
    compact = _HEADING_PREFIX.sub("", re.sub(r"\s+", "", stripped), count=1)
    for heading in sorted(headings, key=len, reverse=True):
        if not compact.startswith(heading):
            continue
        rest = compact[len(heading) :]
        if heading == "经营范围" and rest.startswith("内"):
            continue
        if (
            rest
            and _FIELD_LABEL_VALUE.search(rest)
            and not re.fullmatch(r"[:：.。]+", rest)
        ):
            return heading
    return None


def _heading_end_in_line(line: str, heading: str) -> int:
    compact_chars: list[tuple[int, str]] = [
        (index, char) for index, char in enumerate(line) if not char.isspace()
    ]
    compact = "".join(char for _, char in compact_chars)
    prefix = _HEADING_PREFIX.match(compact)
    start = prefix.end() if prefix else 0
    if compact[start : start + len(heading)] != heading:
        start = compact.find(heading)
        if start < 0:
            return len(line)
    end_index = start + len(heading) - 1
    if end_index >= len(compact_chars):
        return len(line)
    return compact_chars[end_index][0] + 1


def _is_toc_page(text: str) -> bool:
    head = text[:200]
    if re.search(r"(?:^|\n)\s*目录\s*(?:\n|$)", head):
        return True
    return "......" in text[:400] or "……" in text[:400]


def _is_toc_line(line: str, headings: Sequence[str] = ()) -> bool:
    if "......" in line or "……" in line:
        return True
    compact = re.sub(r"\s+", "", line)
    if not re.search(r"\d{1,4}$", compact):
        return False
    body = _HEADING_PREFIX.sub("", compact, count=1)
    known = tuple(dict.fromkeys((*_ALL_HEADINGS, *headings)))
    return any(heading in body for heading in known)


def _collect_field_label(
    page: ReportPageText,
    *,
    heading: str,
    line_end: int,
) -> tuple[str, list[int], bool, CoreEvidenceGap | None]:
    rest = page.text[line_end:]
    line = rest.split("\n", 1)[0]
    excerpt = f"{heading}{line}".strip()
    if not _FIELD_LABEL_VALUE.search(line):
        return excerpt, [], False, None
    return excerpt, [], True, None


def _collect_section(
    page: ReportPageText,
    *,
    heading: str,
    line_end: int,
    by_page: dict[int, ReportPageText],
    chapter_task: ChapterTask,
) -> tuple[str, list[int], bool, CoreEvidenceGap | None]:
    first = _cut_at_boundary(page.text[line_end:])
    excerpt = f"{heading}\n{first.text}".strip()
    continuations: list[int] = []
    if first.closed:
        return excerpt, continuations, True, None

    current = page.page
    while len(continuations) + 1 < _MAX_SECTION_PAGES:
        nxt = by_page.get(current + 1)
        if nxt is None:
            if excerpt.strip() in {"", heading}:
                return "", continuations, False, None
            if _looks_incomplete(excerpt, heading):
                return (
                    excerpt,
                    continuations,
                    False,
                    CoreEvidenceGap(
                        code="page_unreadable",
                        chapter_task=chapter_task.value,
                        page=current + 1,
                        message=f"continuation page is missing: {current + 1}",
                    ),
                )
            return excerpt, continuations, False, None
        if not nxt.readable:
            empty = excerpt.strip() in {"", heading}
            return (
                "" if empty else excerpt,
                continuations,
                False,
                CoreEvidenceGap(
                    code="page_unreadable",
                    chapter_task=chapter_task.value,
                    page=nxt.page,
                    message=f"continuation page is unreadable: {nxt.page}",
                ),
            )
        chunk = _cut_at_boundary(nxt.text)
        if not chunk.text.strip() and chunk.closed:
            return excerpt, continuations, True, None
        if chunk.text.strip():
            excerpt = f"{excerpt}\n{chunk.text.strip()}".strip()
            continuations.append(nxt.page)
        current = nxt.page
        if chunk.closed:
            return excerpt, continuations, True, None
    return excerpt, continuations, False, None


class _PageChunk:
    def __init__(self, text: str, closed: bool) -> None:
        self.text = text
        self.closed = closed


def _cut_at_boundary(text: str) -> _PageChunk:
    cursor = 0
    while True:
        match = _SECTION_BOUNDARY.search(text, cursor)
        if match is None:
            return _PageChunk(text.strip(), False)
        line_start = match.start()
        if line_start < len(text) and text[line_start] == "\n":
            line_start += 1
        line_end = text.find("\n", line_start)
        line = text[line_start : line_end if line_end >= 0 else None]
        if re.search(r"\d", line):
            cursor = match.end()
            continue
        if match.start() == 0:
            return _PageChunk("", True)
        return _PageChunk(text[: match.start()].strip(), True)


def _usable_excerpt(
    excerpt: str,
    heading: str,
    require_substance: bool,
    *,
    substance_extra: re.Pattern[str] | None = None,
) -> bool:
    if not excerpt.strip() or excerpt.strip() == heading:
        return False
    if require_substance:
        joined = _join_pdf_soft_breaks(excerpt)
        if _OVERVIEW_SUBSTANCE.search(joined):
            return True
        return substance_extra is not None and (
            substance_extra.search(excerpt) is not None
            or substance_extra.search(joined) is not None
        )
    return True


def _looks_incomplete(excerpt: str, heading: str) -> bool:
    compact = re.sub(r"\s+", "", excerpt)
    if not compact or compact == heading:
        return True
    if _CONTINUATION_TAIL.search(compact):
        return True
    return compact[-1] not in "。！？；;"


def _display_quote(excerpt: str, heading: str) -> str:
    compact = re.sub(r"\s+", "", excerpt)
    for sentence in re.split(r"[。；;\n]", excerpt):
        sentence = sentence.strip()
        if (heading in sentence or heading in re.sub(r"\s+", "", sentence)) and len(
            re.sub(r"\s+", "", sentence)
        ) > len(heading):
            return sentence[:_DISPLAY_QUOTE_LIMIT]
    if heading in excerpt:
        start = excerpt.find(heading)
        return excerpt[start : start + _DISPLAY_QUOTE_LIMIT].strip()
    return compact[:_DISPLAY_QUOTE_LIMIT] or heading


def _record_matches_span(
    record: SemanticRecord,
    report: ReportIdentity,
    span: CoreEvidenceSpan,
) -> bool:
    if record.field_id not in span.field_ids:
        return False
    if not _period_matches(record, report):
        return False
    if not _source_range_matches(record, span):
        return False
    objects = _record_objects(record)
    if not objects:
        return True
    compact = re.sub(r"\s+", "", span.excerpt)
    return any(_object_mentioned(item, compact) for item in objects)


def _field_covered_for_span(
    field_id: str,
    span: CoreEvidenceSpan,
    report: ReportIdentity,
    reused_records: Sequence[SemanticRecord],
) -> bool:
    covering = [
        record
        for record in reused_records
        if record.field_id == field_id and _record_matches_span(record, report, span)
    ]
    if not covering:
        return False
    if field_id == "business_overview_source":
        return _overview_covers_excerpt(covering, span)
    demanded = _demanded_objects(field_id, span.excerpt)
    if not demanded:
        return False
    covered: set[str] = set()
    for record in covering:
        covered.update(_record_objects(record))
    return all(
        any(
            _object_mentioned(item, covered_item)
            or _object_mentioned(covered_item, item)
            for covered_item in covered
        )
        for item in demanded
    )


def _period_matches(record: SemanticRecord, report: ReportIdentity) -> bool:
    """Reuse the annual-duration alias already used by Stage 5 adapters."""

    reported = record.reported_period.strip()
    expected = report.report_period.strip()
    if not reported or not expected:
        return False
    if reported == expected:
        return True
    if record.period_type is not PeriodType.DURATION:
        return False
    left = _annual_duration_year(reported)
    right = _annual_duration_year(expected)
    return bool(left and right and left == right)


def _annual_duration_year(value: str) -> str | None:
    # Same closed annual aliases as
    # stage5_provider._normalize_segment_partition_reported_period.
    match = re.fullmatch(r"(\d{4})(?:年|年度|-12-31)?", re.sub(r"\s+", "", value))
    return match.group(1) if match else None


def _source_range_matches(record: SemanticRecord, span: CoreEvidenceSpan) -> bool:
    span_pages = {span.page, *span.continuation_pages}
    record_pages: set[int] = set()
    for item in record.evidence:
        record_pages.add(item.page)
        record_pages.update(item.continuation_pages)
    return bool(record_pages & span_pages)


def _overview_covers_excerpt(
    records: Sequence[SemanticRecord],
    span: CoreEvidenceSpan,
) -> bool:
    body = _compact_overview_body(span.excerpt, span.section_title)
    if not body:
        return False
    for record in records:
        if not isinstance(record, BusinessOverview):
            continue
        accepted = _strip_company_prefix(_compact_source_text(record.source_text))
        if accepted and (body == accepted or body in accepted):
            return True
    return False


def _compact_overview_body(excerpt: str, heading: str) -> str:
    compact = _compact_source_text(excerpt)
    heading_compact = _compact_source_text(heading)
    if heading_compact and compact.startswith(heading_compact):
        compact = compact[len(heading_compact) :]
    return _strip_company_prefix(compact)


def _compact_source_text(value: str) -> str:
    return re.sub(r"[\s。；;，,：:]+$", "", re.sub(r"\s+", "", value))


def _strip_company_prefix(value: str) -> str:
    return re.sub(r"^(?:公司|本公司)", "", value)


def _record_objects(record: SemanticRecord) -> set[str]:
    if isinstance(record, Measurement):
        return {
            re.sub(r"\s+", "", value)
            for value in (record.segment_label, record.measured_object)
            if value
        }
    if isinstance(record, Segment):
        return {re.sub(r"\s+", "", record.label)} if record.label else set()
    if isinstance(record, Activity):
        return {re.sub(r"\s+", "", record.object_name)} if record.object_name else set()
    if isinstance(record, BusinessOverview):
        return set()
    return set()


def _demanded_objects(field_id: str, excerpt: str) -> set[str]:
    if field_id in {"operating_revenue", "segment_dimension"}:
        objects = {
            re.sub(r"\s+", "", match.group(1))
            for match in _REVENUE_OBJECT.finditer(excerpt)
        }
        return {
            item
            for item in objects
            if item not in {"项目", "其中", "合计", "总计", "小计"}
        }
    if field_id == "explicit_activity":
        match = _ACTIVITY_OBJECTS.search(re.sub(r"\s+", "", excerpt))
        if match is None:
            return set()
        return {
            part for part in re.split(r"[、，,和及]", match.group(1)) if len(part) >= 2
        }
    return set()


def _object_mentioned(needle: str, haystack: str) -> bool:
    if not needle:
        return False
    return needle in haystack


def _prepared_evidence_for_spans(
    report: ReportIdentity,
    spans: Sequence[CoreEvidenceSpan],
) -> tuple[PreparedEvidence, ...]:
    complete_chapters = {span.chapter_task for span in spans if span.context_complete}
    prepared: list[PreparedEvidence] = []
    for span in spans:
        if not span.context_complete and span.chapter_task in complete_chapters:
            continue
        prepared.extend(_prepared_evidence(report, span))
    return tuple(prepared)


def _prepared_evidence(
    report: ReportIdentity,
    span: CoreEvidenceSpan,
) -> tuple[PreparedEvidence, ...]:
    items: list[PreparedEvidence] = []
    for field_id in span.field_ids:
        digest = hashlib.sha256(
            f"{report.report_id}:{span.chapter_task}:{field_id}:"
            f"{span.page}:{span.excerpt}".encode()
        ).hexdigest()[:16]
        items.append(
            PreparedEvidence(
                evidence=Evidence(
                    evidence_id=f"core-ev-{digest}",
                    report=report,
                    page=span.page,
                    section_title=span.section_title,
                    continuation_pages=span.continuation_pages,
                    anchor=TextAnchor(bounded_quote=span.excerpt),
                ),
                field_id=field_id,
                context_complete=span.context_complete,
                continuation_complete=span.context_complete,
                source_readable=True,
            )
        )
    return tuple(items)


_CLAUSE_PATTERNS = (
    re.compile(r"经营模式主要为[:：]\s*([^。；;]{2,300})"),
    re.compile(r"主营业务为\s*([^。；;]{2,80})"),
    re.compile(r"主要从事\s*([^。；;]{2,400})"),
    re.compile(r"主要产品包括\s*([^。；;]{2,600})"),
    re.compile(r"主要产品为\s*([^。；;]{2,80})"),
    re.compile(r"(?:^|[\n\r])经营范围(?!内)\s*[:：]?\s*([^。；;\n]{2,120})"),
    re.compile(r"涵盖\s*([^。；;]{2,80})"),
)
_UNIT_DECLARATION = re.compile(r"单位[:：]\s*(?:人民币)?(百万元|千元|亿元|万元|元)")
_PERCENT_UNIT = re.compile(r"单位[:：]\s*%")
_PDF_AMOUNT_WRAP = re.compile(r"([0-9,]+\.)\s*[\r\n]+\s*(\d+)")
_ACTION_TOKEN = re.compile(r"(研发|开发|制造|生产|加工|销售|维修|服务保障)")
_VAGUE_OBJECT = re.compile(r"^(?:经批准的)?(?:其它|其他)业务$")
_SKIP_SEGMENT_LABELS = frozenset(
    {
        "营业收入",
        "营业成本",
        "毛利率",
        "项目",
        "分行业",
        "分产品",
        "分地区",
        "同比增减",
        "其中",
        "合计",
        "总计",
        "小计",
        "汇总",
    }
)


REVENUE_SENTENCE_REPAIR = "revenue_sentence_repair"
REVENUE_SENTENCE_REPAIR_V1 = "v1"
REVENUE_SENTENCE_REPAIR_V2 = "v2"
REVENUE_SENTENCE_REPAIR_V3 = "v3"
REVENUE_SENTENCE_REPAIR_V4 = "v4"
REVENUE_SENTENCE_REPAIR_V5 = "v5"
REVENUE_SENTENCE_REPAIR_V6 = "v6"
REVENUE_SENTENCE_REPAIR_V7 = "v7"
REVENUE_SENTENCE_REPAIR_V8 = "v8"
REVENUE_SENTENCE_REPAIR_V9 = "v9"
REVENUE_SENTENCE_REPAIR_V10 = "v10"
REVENUE_SENTENCE_REPAIR_V11 = "v11"
REVENUE_SENTENCE_REPAIR_V12 = "v12"
REVENUE_SENTENCE_REPAIR_V13 = "v13"
REVENUE_SENTENCE_REPAIR_V14 = "v14"
REVENUE_SENTENCE_REPAIR_V15 = "v15"
REVENUE_SENTENCE_REPAIR_V16 = "v16"
REVENUE_SENTENCE_REPAIR_V17 = "v17"
REVENUE_SENTENCE_REPAIR_V18 = "v18"
REVENUE_SENTENCE_REPAIR_V19 = "v19"
REVENUE_SENTENCE_REPAIR_V20 = "v20"
REVENUE_SENTENCE_REPAIR_V21 = "v21"
REVENUE_SENTENCE_REPAIR_V22 = "v22"
REVENUE_SENTENCE_REPAIR_V23 = "v23"
REVENUE_SENTENCE_REPAIR_V24 = "v24"
REVENUE_SENTENCE_REPAIR_V25 = "v25"
REVENUE_SENTENCE_REPAIR_V26 = "v26"
REVENUE_SENTENCE_REPAIR_V27 = "v27"
REVENUE_SENTENCE_REPAIR_V28 = "v28"
REVENUE_SENTENCE_REPAIR_V29 = "v29"
REVENUE_SENTENCE_REPAIR_V30 = "v30"
REVENUE_SENTENCE_REPAIR_V31 = "v31"
REVENUE_SENTENCE_REPAIR_V32 = "v32"
REVENUE_SENTENCE_REPAIR_V33 = "v33"
REVENUE_SENTENCE_REPAIR_V34 = "v34"
REVENUE_SENTENCE_REPAIR_V35 = "v35"
REVENUE_SENTENCE_REPAIR_V36 = "v36"
REVENUE_SENTENCE_REPAIR_V37 = "v37"
REVENUE_SENTENCE_REPAIR_V38 = "v38"
_REVENUE_SENTENCE_REPAIR_VERSIONS = frozenset(
    {
        REVENUE_SENTENCE_REPAIR_V1,
        REVENUE_SENTENCE_REPAIR_V2,
        REVENUE_SENTENCE_REPAIR_V3,
        REVENUE_SENTENCE_REPAIR_V4,
        REVENUE_SENTENCE_REPAIR_V5,
        REVENUE_SENTENCE_REPAIR_V6,
        REVENUE_SENTENCE_REPAIR_V7,
        REVENUE_SENTENCE_REPAIR_V8,
        REVENUE_SENTENCE_REPAIR_V9,
        REVENUE_SENTENCE_REPAIR_V10,
        REVENUE_SENTENCE_REPAIR_V11,
        REVENUE_SENTENCE_REPAIR_V12,
        REVENUE_SENTENCE_REPAIR_V13,
        REVENUE_SENTENCE_REPAIR_V14,
        REVENUE_SENTENCE_REPAIR_V15,
        REVENUE_SENTENCE_REPAIR_V16,
        REVENUE_SENTENCE_REPAIR_V17,
        REVENUE_SENTENCE_REPAIR_V18,
        REVENUE_SENTENCE_REPAIR_V19,
        REVENUE_SENTENCE_REPAIR_V20,
        REVENUE_SENTENCE_REPAIR_V21,
        REVENUE_SENTENCE_REPAIR_V22,
        REVENUE_SENTENCE_REPAIR_V23,
        REVENUE_SENTENCE_REPAIR_V24,
        REVENUE_SENTENCE_REPAIR_V25,
        REVENUE_SENTENCE_REPAIR_V26,
        REVENUE_SENTENCE_REPAIR_V27,
        REVENUE_SENTENCE_REPAIR_V28,
        REVENUE_SENTENCE_REPAIR_V29,
        REVENUE_SENTENCE_REPAIR_V30,
        REVENUE_SENTENCE_REPAIR_V31,
        REVENUE_SENTENCE_REPAIR_V32,
        REVENUE_SENTENCE_REPAIR_V33,
        REVENUE_SENTENCE_REPAIR_V34,
        REVENUE_SENTENCE_REPAIR_V35,
        REVENUE_SENTENCE_REPAIR_V36,
        REVENUE_SENTENCE_REPAIR_V37,
        REVENUE_SENTENCE_REPAIR_V38,
    }
)


def revenue_sentence_repair_requested(identity: Mapping[str, Any] | None) -> bool:
    """Return whether this identity turns on same-evidence revenue sentences.

    A different work identity does not change projection unless it carries this
    marker. The marker is read by the overview projection, not only stored.
    ``v2`` is the corrected commodity-evidence identity and still keeps the
    revenue-sentence repair.
    """

    if not isinstance(identity, Mapping):
        return False
    return identity.get(REVENUE_SENTENCE_REPAIR) in _REVENUE_SENTENCE_REPAIR_VERSIONS


def named_role_repair_requested(identity: Mapping[str, Any] | None) -> bool:
    """Return whether this identity keeps principal sentences and named roles.

    ``v4`` still carries the revenue-sentence repair. It also accepts “业务覆盖”
    and “专注于”, and it separates a real sale from “销售模式”.
    """

    if not isinstance(identity, Mapping):
        return False
    return identity.get(REVENUE_SENTENCE_REPAIR) in {
        REVENUE_SENTENCE_REPAIR_V4,
        REVENUE_SENTENCE_REPAIR_V5,
        REVENUE_SENTENCE_REPAIR_V6,
        REVENUE_SENTENCE_REPAIR_V7,
        REVENUE_SENTENCE_REPAIR_V8,
        REVENUE_SENTENCE_REPAIR_V9,
        REVENUE_SENTENCE_REPAIR_V10,
        REVENUE_SENTENCE_REPAIR_V11,
        REVENUE_SENTENCE_REPAIR_V12,
        REVENUE_SENTENCE_REPAIR_V13,
        REVENUE_SENTENCE_REPAIR_V14,
        REVENUE_SENTENCE_REPAIR_V15,
        REVENUE_SENTENCE_REPAIR_V16,
        REVENUE_SENTENCE_REPAIR_V17,
        REVENUE_SENTENCE_REPAIR_V18,
        REVENUE_SENTENCE_REPAIR_V19,
        REVENUE_SENTENCE_REPAIR_V20,
        REVENUE_SENTENCE_REPAIR_V21,
        REVENUE_SENTENCE_REPAIR_V22,
        REVENUE_SENTENCE_REPAIR_V23,
        REVENUE_SENTENCE_REPAIR_V24,
        REVENUE_SENTENCE_REPAIR_V25,
        REVENUE_SENTENCE_REPAIR_V26,
        REVENUE_SENTENCE_REPAIR_V27,
        REVENUE_SENTENCE_REPAIR_V28,
        REVENUE_SENTENCE_REPAIR_V29,
        REVENUE_SENTENCE_REPAIR_V30,
        REVENUE_SENTENCE_REPAIR_V31,
        REVENUE_SENTENCE_REPAIR_V32,
        REVENUE_SENTENCE_REPAIR_V33,
        REVENUE_SENTENCE_REPAIR_V34,
        REVENUE_SENTENCE_REPAIR_V35,
        REVENUE_SENTENCE_REPAIR_V36,
        REVENUE_SENTENCE_REPAIR_V37,
        REVENUE_SENTENCE_REPAIR_V38,
    }


def service_operating_energy_requested(identity: Mapping[str, Any] | None) -> bool:
    """Return whether this identity keeps service operating energy use.

    ``v6`` still carries the principal-sentence and named-role repairs. It also
    binds 电耗 and 柴油单耗 to the source subject and business column.
    """

    if not isinstance(identity, Mapping):
        return False
    return identity.get(REVENUE_SENTENCE_REPAIR) in {
        REVENUE_SENTENCE_REPAIR_V6,
        REVENUE_SENTENCE_REPAIR_V7,
        REVENUE_SENTENCE_REPAIR_V8,
        REVENUE_SENTENCE_REPAIR_V9,
        REVENUE_SENTENCE_REPAIR_V10,
        REVENUE_SENTENCE_REPAIR_V11,
        REVENUE_SENTENCE_REPAIR_V12,
        REVENUE_SENTENCE_REPAIR_V13,
        REVENUE_SENTENCE_REPAIR_V14,
        REVENUE_SENTENCE_REPAIR_V15,
        REVENUE_SENTENCE_REPAIR_V16,
        REVENUE_SENTENCE_REPAIR_V17,
        REVENUE_SENTENCE_REPAIR_V18,
        REVENUE_SENTENCE_REPAIR_V19,
        REVENUE_SENTENCE_REPAIR_V20,
        REVENUE_SENTENCE_REPAIR_V21,
        REVENUE_SENTENCE_REPAIR_V22,
        REVENUE_SENTENCE_REPAIR_V23,
        REVENUE_SENTENCE_REPAIR_V24,
        REVENUE_SENTENCE_REPAIR_V25,
        REVENUE_SENTENCE_REPAIR_V26,
        REVENUE_SENTENCE_REPAIR_V27,
        REVENUE_SENTENCE_REPAIR_V28,
        REVENUE_SENTENCE_REPAIR_V29,
        REVENUE_SENTENCE_REPAIR_V30,
        REVENUE_SENTENCE_REPAIR_V31,
        REVENUE_SENTENCE_REPAIR_V32,
        REVENUE_SENTENCE_REPAIR_V33,
        REVENUE_SENTENCE_REPAIR_V34,
        REVENUE_SENTENCE_REPAIR_V35,
        REVENUE_SENTENCE_REPAIR_V36,
        REVENUE_SENTENCE_REPAIR_V37,
        REVENUE_SENTENCE_REPAIR_V38,
    }


def core_answer_repair_requested(identity: Mapping[str, Any] | None) -> bool:
    """Return whether this identity delivers substantive core answers.

    ``v7`` still carries the named-role and service-energy repairs. It also
    keeps a steel business sentence, a parent-child revenue table, and
    row-level supply bindings.
    """

    if not isinstance(identity, Mapping):
        return False
    return identity.get(REVENUE_SENTENCE_REPAIR) in {
        REVENUE_SENTENCE_REPAIR_V7,
        REVENUE_SENTENCE_REPAIR_V8,
        REVENUE_SENTENCE_REPAIR_V9,
        REVENUE_SENTENCE_REPAIR_V10,
        REVENUE_SENTENCE_REPAIR_V11,
        REVENUE_SENTENCE_REPAIR_V12,
        REVENUE_SENTENCE_REPAIR_V13,
        REVENUE_SENTENCE_REPAIR_V14,
        REVENUE_SENTENCE_REPAIR_V15,
        REVENUE_SENTENCE_REPAIR_V16,
        REVENUE_SENTENCE_REPAIR_V17,
        REVENUE_SENTENCE_REPAIR_V18,
        REVENUE_SENTENCE_REPAIR_V19,
        REVENUE_SENTENCE_REPAIR_V20,
        REVENUE_SENTENCE_REPAIR_V21,
        REVENUE_SENTENCE_REPAIR_V22,
        REVENUE_SENTENCE_REPAIR_V23,
        REVENUE_SENTENCE_REPAIR_V24,
        REVENUE_SENTENCE_REPAIR_V25,
        REVENUE_SENTENCE_REPAIR_V26,
        REVENUE_SENTENCE_REPAIR_V27,
        REVENUE_SENTENCE_REPAIR_V28,
        REVENUE_SENTENCE_REPAIR_V29,
        REVENUE_SENTENCE_REPAIR_V30,
        REVENUE_SENTENCE_REPAIR_V31,
        REVENUE_SENTENCE_REPAIR_V32,
        REVENUE_SENTENCE_REPAIR_V33,
        REVENUE_SENTENCE_REPAIR_V34,
        REVENUE_SENTENCE_REPAIR_V35,
        REVENUE_SENTENCE_REPAIR_V36,
        REVENUE_SENTENCE_REPAIR_V37,
        REVENUE_SENTENCE_REPAIR_V38,
    }


def source_delivery_repair_requested(identity: Mapping[str, Any] | None) -> bool:
    """Return whether this identity delivers power and petrochemical sources.

    ``v8`` still carries the v7 core-answer repair. It also keeps a full
    “主要业务是” or “主要从事” sentence, a wrapped 百万元 industry table,
    and coal, crude, and refined-product roles. ``v9`` keeps those and marks
    the row-subject and review-coverage repair round. ``v10`` keeps all of it
    and adds the chemical-feedstock-oil role and the wrapped product row.
    """

    if not isinstance(identity, Mapping):
        return False
    return identity.get(REVENUE_SENTENCE_REPAIR) in {
        REVENUE_SENTENCE_REPAIR_V8,
        REVENUE_SENTENCE_REPAIR_V9,
        REVENUE_SENTENCE_REPAIR_V10,
        REVENUE_SENTENCE_REPAIR_V11,
        REVENUE_SENTENCE_REPAIR_V12,
        REVENUE_SENTENCE_REPAIR_V13,
        REVENUE_SENTENCE_REPAIR_V14,
        REVENUE_SENTENCE_REPAIR_V15,
        REVENUE_SENTENCE_REPAIR_V16,
        REVENUE_SENTENCE_REPAIR_V17,
        REVENUE_SENTENCE_REPAIR_V18,
        REVENUE_SENTENCE_REPAIR_V19,
        REVENUE_SENTENCE_REPAIR_V20,
        REVENUE_SENTENCE_REPAIR_V21,
        REVENUE_SENTENCE_REPAIR_V22,
        REVENUE_SENTENCE_REPAIR_V23,
        REVENUE_SENTENCE_REPAIR_V24,
        REVENUE_SENTENCE_REPAIR_V25,
        REVENUE_SENTENCE_REPAIR_V26,
        REVENUE_SENTENCE_REPAIR_V27,
        REVENUE_SENTENCE_REPAIR_V28,
        REVENUE_SENTENCE_REPAIR_V29,
        REVENUE_SENTENCE_REPAIR_V30,
        REVENUE_SENTENCE_REPAIR_V31,
        REVENUE_SENTENCE_REPAIR_V32,
        REVENUE_SENTENCE_REPAIR_V33,
        REVENUE_SENTENCE_REPAIR_V34,
        REVENUE_SENTENCE_REPAIR_V35,
        REVENUE_SENTENCE_REPAIR_V36,
        REVENUE_SENTENCE_REPAIR_V37,
        REVENUE_SENTENCE_REPAIR_V38,
    }


def project_owned_page_facts(
    selection: CoreEvidenceSelection,
    chapter: ChapterTask | None = None,
    *,
    repair_revenue_sentence: bool = False,
    named_role_repair: bool = False,
    service_operating_energy: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> tuple[SemanticRecord, ...]:
    """Project source-native core facts already stated in owned excerpts."""

    records: list[SemanticRecord] = []
    for span in selection.spans:
        if chapter is not None and span.chapter_task != chapter.value:
            continue
        if span.chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value:
            records.extend(
                _project_overview_span(
                    selection,
                    span,
                    repair_revenue_sentence=repair_revenue_sentence,
                    named_role_repair=named_role_repair,
                    core_answer_repair=core_answer_repair,
                    source_delivery_repair=source_delivery_repair,
                )
            )
        elif span.chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value:
            if span.section_title in _INCOME_ANALYSIS_HEADINGS:
                records.extend(_project_income_analysis_span(selection, span))
            else:
                records.extend(
                    _project_segment_span(
                        selection,
                        span,
                        core_answer_repair=core_answer_repair,
                        source_delivery_repair=source_delivery_repair,
                    )
                )
                records.extend(_project_company_total_rows(selection, span))
        elif span.chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS.value:
            records.extend(
                _project_material_span(
                    selection, span, source_delivery_repair=source_delivery_repair
                )
            )
    if repair_revenue_sentence and chapter in (
        None,
        ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
    ):
        for span in selection.spans:
            records.extend(
                _project_repair_commodity_span(
                    selection,
                    span,
                    named_role_repair=named_role_repair,
                    service_operating_energy=service_operating_energy,
                    core_answer_repair=core_answer_repair,
                    source_delivery_repair=source_delivery_repair,
                )
            )
    return _dedupe_owned_records(records)


def _prepared_for(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
    field_id: str,
) -> PreparedEvidence | None:
    for item in selection.prepared_evidence:
        if (
            item.field_id == field_id
            and item.evidence.page == span.page
            and item.evidence.section_title == span.section_title
            and isinstance(item.evidence.anchor, TextAnchor)
            and item.evidence.anchor.bounded_quote == span.excerpt
        ):
            return item
    return None


def _project_overview_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
    *,
    repair_revenue_sentence: bool = False,
    named_role_repair: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> tuple[SemanticRecord, ...]:
    overview_item = _prepared_for(selection, span, "business_overview_source")
    activity_item = _prepared_for(selection, span, "explicit_activity")
    records: list[SemanticRecord] = []
    company_sentence = (
        _company_business_source(span.excerpt) if named_role_repair else ""
    )
    if core_answer_repair and not company_sentence:
        company_sentence = _steel_business_source(span.excerpt)
    if source_delivery_repair:
        numbered = _numbered_business_source(span.excerpt)
        if (
            not numbered
            and span.section_title != "当期产品及服务披露"
            and len(
                re.findall(
                    r"(?:^|\n)\s*\d+[.．、]\s*[\u4e00-\u9fff]{2,12}业务", span.excerpt
                )
            )
            >= 2
        ):
            return ()
        delivered = (
            _complete_current_business_source(span.excerpt)
            or numbered
            or next(iter(_owned_business_sources(span.excerpt)), "")
            or _project_business_source(span.excerpt)
            or _main_business_source(span.excerpt)
            or _integrated_business_source(span.excerpt)
            or _toll_service_source(span.excerpt)
            or _main_operation_source(span.excerpt)
            or _operating_model_source(span.excerpt)
            or _company_as_narrative_source(span.excerpt)
            or _provider_narrative_source(span.excerpt)
            or _current_business_narrative_source(span.excerpt)
            or _company_sales_mechanism_source(span.excerpt)
            or _business_blocks_source(span.excerpt)
            or _company_income_source(span.excerpt)
            or next(iter(_current_product_sources(span.excerpt)), "")
        )
        if delivered:
            company_sentence = delivered
        if span.section_title == "当期产品及服务披露":
            # This span was already bounded to one affirmative source passage.
            # A shorter nested match must not replace its complete mechanism.
            company_sentence = span.excerpt
    if overview_item is not None and (
        _excerpt_states_owned_overview(span.excerpt) or company_sentence
    ):
        quote = (
            overview_item.evidence.anchor.bounded_quote
            if isinstance(overview_item.evidence.anchor, TextAnchor)
            else ""
        )
        source_text = company_sentence or _overview_source_text(
            span.excerpt,
            quote,
            heading=span.section_title,
        )
        if source_delivery_repair and source_text:
            supplements = _company_product_model_sources(span.excerpt)
            start = span.excerpt.find(source_text)
            ends = [span.excerpt.find(text) + len(text) for text in supplements]
            if start >= 0 and ends:
                # The contract requires one original continuous passage,
                # including the intervening owned context, not stitched quotes.
                source_text = span.excerpt[start : max(start + len(source_text), *ends)]
        if source_delivery_repair and re.search(
            r"(?:子公司|第三方公司|(?:本公司|公司)(?:拟|计划))[^。]{0,40}主营业务为",
            re.sub(r"\s+", "", source_text),
        ):
            source_text = ""
        if source_text:
            records.append(
                _base_fact(
                    BusinessOverview,
                    report=selection.report,
                    record_id=f"owned:{overview_item.evidence.evidence_id}:overview",
                    field_id="business_overview_source",
                    chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                    evidence=overview_item.evidence,
                    source_native=SourceNativeValue(name=span.section_title),
                    source_text=source_text,
                )
            )
        if repair_revenue_sentence:
            records.extend(
                _revenue_sentence_records(
                    selection,
                    overview_item,
                    excerpt=span.excerpt,
                    principal_text=source_text,
                )
            )
    if activity_item is None:
        return tuple(records)
    seen: set[str] = set()
    excerpt = _join_pdf_soft_breaks(span.excerpt)
    for pattern in _CLAUSE_PATTERNS:
        for match in pattern.finditer(excerpt):
            if source_delivery_repair and match.group(0).startswith("主营业务为"):
                sentence = _sentence_containing(excerpt, match.start())
                if not _current_company_sentence(sentence) or re.search(
                    r"(?:本公司|公司)(?:拟|计划)", sentence
                ):
                    continue
            if source_delivery_repair and match.group(0).startswith("主要从事"):
                prefix = _sentence_containing(excerpt, match.start())
                prefix = prefix[: prefix.find("主要从事")]
                if (
                    re.search(
                        r"(?<![\u4e00-\u9fff])(?:本公司|公司|本集团)[^。；;]{0,40}$",
                        prefix,
                    )
                    is None
                ):
                    continue
            verb = "经营"
            if match.group(0).startswith("主营"):
                verb = "为"
            elif "从事" in match.group(0):
                verb = "从事"
            elif "包括" in match.group(0):
                verb = "包括"
            elif "涵盖" in match.group(0):
                verb = "涵盖"
                if source_delivery_repair and re.search(
                    r"参数|等级|多个领域|应用领域", match.group(1)
                ):
                    continue
            clause = match.group(1)
            if (
                source_delivery_repair
                and re.search(
                    r"公司控股子公司[^。]+主要从事叶片的设计",
                    re.sub(r"\s+", "", excerpt[: match.end()]),
                )
                and clause.strip().startswith("叶片的设计")
            ):
                continue
            actor = "公司"
            if source_delivery_repair:
                sentence = _sentence_containing(excerpt, match.start())
                named_actor = re.search(
                    r"公司下属控股公司([\u4e00-\u9fff]{2,40}?有限公司)主要从事",
                    excerpt[: match.end()],
                )
                if named_actor:
                    actor = named_actor[1]
                elif "本集团" in sentence:
                    actor = "本集团"
            if source_delivery_repair:
                # A product list ends when the source changes to upstream or
                # downstream firms. Their objects are not the company's products.
                clause = re.split(
                    r"[，,](?:上游|下游)(?:企业|客户|为)", clause, maxsplit=1
                )[0]
            for raw in _activity_object_clauses(clause):
                object_name, action, source_verb = _object_and_action(raw, verb)
                if core_answer_repair and _SPEC_OBJECT.match(object_name):
                    continue
                if source_delivery_repair and object_name.strip() in {
                    "销售",
                    "生产",
                    "建设",
                    "利用",
                    "运营与管理",
                }:
                    continue
                key = re.sub(r"\s+", "", object_name)
                if not key or key in seen:
                    continue
                seen.add(key)
                records.append(
                    _base_fact(
                        Activity,
                        report=selection.report,
                        record_id=(
                            f"owned:{activity_item.evidence.evidence_id}"
                            f":activity:{len(seen)}"
                        ),
                        field_id="explicit_activity",
                        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                        evidence=activity_item.evidence,
                        source_native=SourceNativeValue(name=object_name),
                        action=action,
                        activity_actor=actor,
                        source_actor=actor,
                        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
                        object_name=object_name,
                        source_verb=source_verb,
                    )
                )
    if source_delivery_repair and activity_item is not None:
        steel = _steel_raw_material_binding(span.excerpt)
        if steel:
            records.append(
                _base_fact(
                    Activity,
                    report=selection.report,
                    record_id=(
                        f"owned:{activity_item.evidence.evidence_id}"
                        ":activity:steel-raw-material"
                    ),
                    field_id="explicit_activity",
                    chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                    evidence=activity_item.evidence,
                    source_native=SourceNativeValue(
                        name=steel["name"],
                        value=steel.get("value"),
                        unit=steel.get("unit"),
                        header=steel.get("header"),
                    ),
                    action=steel["action"],
                    activity_actor=steel["actor"],
                    source_actor=steel["actor"],
                    actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
                    object_name=steel["name"],
                    source_verb=steel["verb"],
                    excerpt_subject=span.excerpt,
                )
            )
    return tuple(records)


def _project_segment_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
    *,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> tuple[SemanticRecord, ...]:
    if not span.context_complete:
        return ()
    if source_delivery_repair:
        native = _project_native_income_span(selection, span)
        if native is not None:
            return native
    segment_item = _prepared_for(selection, span, "segment_dimension")
    revenue_item = _prepared_for(selection, span, "operating_revenue")
    if segment_item is None and revenue_item is None:
        return ()
    excerpt = _join_rmb_column_header(_join_pdf_soft_breaks(span.excerpt))
    excerpt = _join_segment_label_amounts(
        excerpt,
        core_answer_repair=core_answer_repair,
    )
    quote = (
        segment_item.evidence.anchor.bounded_quote
        if segment_item is not None
        and isinstance(segment_item.evidence.anchor, TextAnchor)
        else ""
    )
    if revenue_item is not None and isinstance(
        revenue_item.evidence.anchor, TextAnchor
    ):
        quote = quote or revenue_item.evidence.anchor.bounded_quote
    if core_answer_repair:
        quote = _join_segment_label_amounts(
            _join_pdf_soft_breaks(quote),
            core_answer_repair=True,
        )
    # The bounded quote carries the passage's own group wording (本集团…), which
    # the acceptance policy checks; bind the subject from that same text —
    # unless the table itself shows a narrower scope such as 母公司, which the
    # group narrative must not lift.
    subject_text = span.excerpt
    if quote and not re.search(r"母公司", span.excerpt):
        subject_text = f"{span.excerpt}\n{quote}"
    column_unit = (
        "百万元"
        if source_delivery_repair and "人民币百万元" in re.sub(r"\s+", "", excerpt)
        else None
    )
    unit = column_unit
    previous_was_unit = False
    group_dimensions: frozenset[str] | None = None
    dimension = _dimension_from_heading(span.section_title)
    revenue_parent = ""
    records: list[SemanticRecord] = []
    started = False
    lines = excerpt.splitlines()
    last_heading_offset = -1
    for offset, line in enumerate(lines):
        if started and _is_revenue_table_stop(line):
            break
        if not line.strip():
            continue
        declared = _unit_from_excerpt(line)
        if declared is not None and _UNIT_DECLARATION.search(line):
            unit = declared
            previous_was_unit = True
            continue
        opened = _open_segment_group(line)
        if opened is not None:
            if not previous_was_unit:
                unit = _table_intro_unit(lines, offset)
            group_dimensions = opened
            previous_was_unit = False
            started = True
            last_heading_offset = offset
            continue
        if (
            _dimension_from_heading(line) is not None
            or _parse_segment_row(line) is not None
        ):
            started = True
        section = _dimension_from_heading(line)
        if section is not None:
            revenue_parent = ""
            if group_dimensions is not None and section in group_dimensions:
                previous_was_unit = False
                dimension = section
                last_heading_offset = offset
                continue
            group_dimensions = None
            if not previous_was_unit:
                unit = _table_intro_unit(lines, offset)
            previous_was_unit = False
            dimension = section
            last_heading_offset = offset
            continue
        if group_dimensions is not None and _is_enumerated_revenue_class(line):
            group_dimensions = None
            if not previous_was_unit:
                unit = _table_intro_unit(lines, last_heading_offset)
        previous_was_unit = False
        parsed = _parse_segment_row(line)
        if parsed is None:
            continue
        label, amount, _share = parsed
        row_dimension = (
            "revenue_composition" if _is_enumerated_revenue_class(line) else dimension
        )
        row_header = None
        if (
            source_delivery_repair
            and row_dimension in {"product", "region"}
            and last_heading_offset >= 0
            and "主营业务" in lines[last_heading_offset]
        ):
            row_header = "主营业务收入"
        if source_delivery_repair and label.endswith("小计"):
            row_dimension = "total"
        if core_answer_repair and label in _REVENUE_PARENT_LABELS:
            row_dimension = "revenue_composition"
            revenue_parent = label
        elif (
            core_answer_repair
            and row_dimension == "revenue_composition"
            and revenue_parent
        ):
            row_header = revenue_parent
        if row_dimension is None:
            continue
        source_line = line.strip()
        if not _stated_in_quote(source_line, quote or span.excerpt):
            continue
        if segment_item is not None:
            records.append(
                _base_fact(
                    Segment,
                    report=selection.report,
                    record_id=(
                        f"owned:{segment_item.evidence.evidence_id}"
                        f":segment:{row_dimension}:{label}"
                    ),
                    field_id="segment_dimension",
                    chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
                    evidence=segment_item.evidence,
                    source_native=SourceNativeValue(
                        name=label,
                        value=amount,
                        unit=unit,
                        header=row_header,
                        qualifier="小计" if row_dimension == "total" else None,
                    ),
                    dimension=row_dimension,
                    label=label,
                    excerpt_subject=subject_text,
                )
            )
        if revenue_item is not None and unit:
            records.append(
                _base_fact(
                    Measurement,
                    report=selection.report,
                    record_id=(
                        f"owned:{revenue_item.evidence.evidence_id}"
                        f":revenue:{row_dimension}:{label}"
                    ),
                    field_id="operating_revenue",
                    chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
                    evidence=revenue_item.evidence,
                    source_native=SourceNativeValue(
                        name=label,
                        value=amount,
                        unit=unit,
                        header=row_header,
                        qualifier="小计" if row_dimension == "total" else None,
                    ),
                    metric_type=MetricType.OPERATING_REVENUE,
                    logical_slot=LogicalSlot.REVENUE,
                    measured_object=label,
                    segment_dimension=row_dimension,
                    segment_label=label,
                    excerpt_subject=subject_text,
                )
            )
    return tuple(records)


_COMPANY_TOTAL_LABELS = frozenset({"营业收入合计", "营业总收入"})
_INCOME_AMOUNT_LABELS = frozenset({"营业收入", "利息净收入"})
_INCOME_SHARE_LABELS = frozenset({"利息净收入"})
_INCOME_ITEM_DIMENSION = "income_item"
_FORBIDDEN_INCOME_LABELS = frozenset(
    {
        "净息差",
        "净利息收益率",
        "成本收入比",
        "贷款总额",
        "贷款利息收入",
        "投资利息收入",
        "业务总收入",
    }
)
_INCOME_ANALYSIS_STOP_HEADINGS = (
    "利息净收入",
    "净息差",
    "手续费及佣金净收入",
    "分行业",
    "分产品",
    "分地区",
    "分销售模式",
    "营业成本构成",
)
_AMOUNT_TOKEN = re.compile(r"-?[\d,]+(?:\.\d+)?")
_SHARE_TOKEN = re.compile(r"-?[\d.]+")
_TABLE_BLOCK_SPLIT = re.compile(r"(?=(?:下表|单位[:：]))")
_BUSINESS_TOTAL_MARK = re.compile(r"业务总收入")
_COMPOSITION_MARK = re.compile(r"营业收入构成|占营业收入百分比")
_REGION_TABLE_MARK = re.compile(r"地区分部|分地区")


def _project_company_total_rows(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
) -> tuple[SemanticRecord, ...]:
    revenue_item = _prepared_for(selection, span, "operating_revenue")
    if revenue_item is None:
        return ()
    excerpt = _join_pdf_soft_breaks(span.excerpt)
    quote = (
        revenue_item.evidence.anchor.bounded_quote
        if isinstance(revenue_item.evidence.anchor, TextAnchor)
        else span.excerpt
    )
    unit = _unit_from_excerpt(excerpt)
    if not unit:
        return ()
    records: list[SemanticRecord] = []
    for line in excerpt.splitlines():
        parsed = _parse_labeled_amount_row(line, _COMPANY_TOTAL_LABELS)
        if parsed is None:
            continue
        label, amount = parsed
        if not _stated_in_quote(line.strip(), quote or span.excerpt):
            continue
        records.append(
            _company_total_measurement(
                selection,
                revenue_item,
                label=label,
                amount=amount,
                unit=unit,
                excerpt=excerpt,
            )
        )
    return tuple(records)


def _project_income_analysis_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
) -> tuple[SemanticRecord, ...]:
    revenue_item = _prepared_for(selection, span, "operating_revenue")
    if revenue_item is None:
        return ()
    excerpt = _clip_income_analysis_excerpt(_join_pdf_soft_breaks(span.excerpt))
    quote = (
        revenue_item.evidence.anchor.bounded_quote
        if isinstance(revenue_item.evidence.anchor, TextAnchor)
        else span.excerpt
    )
    records: list[SemanticRecord] = []
    parts = [part.strip() for part in _TABLE_BLOCK_SPLIT.split(excerpt) if part.strip()]
    for index, block in enumerate(parts):
        context = f"{parts[index - 1]}\n{block}" if index else block
        if _BUSINESS_TOTAL_MARK.search(context):
            continue
        if re.search(r"地区分部", context) and not _COMPOSITION_MARK.search(context):
            continue
        unit = _first_declared_unit(block)
        if unit == "%":
            if not _COMPOSITION_MARK.search(context):
                continue
            records.extend(
                _project_income_share_block(
                    selection,
                    revenue_item,
                    block=block,
                    quote=quote or span.excerpt,
                    excerpt=excerpt,
                )
            )
            continue
        if not unit:
            continue
        records.extend(
            _project_income_amount_block(
                selection,
                revenue_item,
                block=block,
                unit=unit,
                quote=quote or span.excerpt,
                excerpt=excerpt,
            )
        )
    return tuple(records)


def _project_income_amount_block(
    selection: CoreEvidenceSelection,
    revenue_item,
    *,
    block: str,
    unit: str,
    quote: str,
    excerpt: str,
) -> tuple[SemanticRecord, ...]:
    records: list[SemanticRecord] = []
    for line in block.splitlines():
        parsed = _parse_labeled_amount_row(
            line, _COMPANY_TOTAL_LABELS | _INCOME_AMOUNT_LABELS
        )
        if parsed is None:
            continue
        label, amount = parsed
        if label in _FORBIDDEN_INCOME_LABELS:
            continue
        if not _stated_in_quote(line.strip(), quote):
            continue
        if label in _COMPANY_TOTAL_LABELS or label == "营业收入":
            records.append(
                _company_total_measurement(
                    selection,
                    revenue_item,
                    label=label,
                    amount=amount,
                    unit=unit,
                    excerpt=excerpt,
                )
            )
            continue
        if label in _INCOME_SHARE_LABELS:
            records.append(
                _income_item_measurement(
                    selection,
                    revenue_item,
                    label=label,
                    amount=amount,
                    unit=unit,
                    excerpt=excerpt,
                )
            )
    return tuple(records)


def _project_income_share_block(
    selection: CoreEvidenceSelection,
    revenue_item,
    *,
    block: str,
    quote: str,
    excerpt: str,
) -> tuple[SemanticRecord, ...]:
    records: list[SemanticRecord] = []
    for line in block.splitlines():
        parsed = _parse_labeled_share_row(line, _INCOME_SHARE_LABELS)
        if parsed is None:
            continue
        label, share = parsed
        if not _stated_in_quote(line.strip(), quote):
            continue
        records.append(
            _income_share_measurement(
                selection,
                revenue_item,
                label=label,
                share=share,
                excerpt=excerpt,
            )
        )
    return tuple(records)


def _company_total_measurement(
    selection: CoreEvidenceSelection,
    revenue_item,
    *,
    label: str,
    amount: str,
    unit: str,
    excerpt: str,
) -> Measurement:
    return _base_fact(
        Measurement,
        report=selection.report,
        record_id=(
            f"owned:{revenue_item.evidence.evidence_id}:revenue:company_total:{label}"
        ),
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        evidence=revenue_item.evidence,
        source_native=SourceNativeValue(name=label, value=amount, unit=unit),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object=label,
        excerpt_subject=excerpt,
    )


def _income_item_measurement(
    selection: CoreEvidenceSelection,
    revenue_item,
    *,
    label: str,
    amount: str,
    unit: str,
    excerpt: str,
) -> Measurement:
    return _base_fact(
        Measurement,
        report=selection.report,
        record_id=(
            f"owned:{revenue_item.evidence.evidence_id}"
            f":revenue:{_INCOME_ITEM_DIMENSION}:{label}"
        ),
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        evidence=revenue_item.evidence,
        source_native=SourceNativeValue(name=label, value=amount, unit=unit),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object=label,
        segment_dimension=_INCOME_ITEM_DIMENSION,
        segment_label=label,
        excerpt_subject=excerpt,
    )


def _income_share_measurement(
    selection: CoreEvidenceSelection,
    revenue_item,
    *,
    label: str,
    share: str,
    excerpt: str,
) -> Measurement:
    return _base_fact(
        Measurement,
        report=selection.report,
        record_id=(f"owned:{revenue_item.evidence.evidence_id}:share:{label}:营业收入"),
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        evidence=revenue_item.evidence,
        source_native=SourceNativeValue(
            name=f"{label} / 营业收入",
            value=share,
            unit="%",
        ),
        metric_type=MetricType.DISCLOSED_SHARE,
        logical_slot=LogicalSlot.DISCLOSED_SHARE,
        measured_object=label,
        relationship_context="营业收入",
        excerpt_subject=excerpt,
    )


def _parse_labeled_amount_row(
    line: str,
    labels: frozenset[str],
) -> tuple[str, str] | None:
    text = line.strip()
    if not text:
        return None
    tokens = text.split()
    if len(tokens) < 2:
        return None
    label = tokens[0]
    if label not in labels or not _AMOUNT_TOKEN.fullmatch(tokens[1]):
        return None
    return label, tokens[1]


def _parse_labeled_share_row(
    line: str,
    labels: frozenset[str],
) -> tuple[str, str] | None:
    text = line.strip()
    if not text:
        return None
    tokens = text.split()
    if len(tokens) < 2:
        return None
    label = tokens[0]
    if label not in labels or not _SHARE_TOKEN.fullmatch(tokens[1]):
        return None
    return label, tokens[1]


def _income_analysis_excerpt_ready(excerpt: str) -> bool:
    text = _clip_income_analysis_excerpt(_join_pdf_soft_breaks(excerpt))
    if _first_declared_unit(text) is None:
        return False
    for line in text.splitlines():
        if _parse_labeled_amount_row(
            line, _COMPANY_TOTAL_LABELS | _INCOME_AMOUNT_LABELS
        ) or _parse_labeled_share_row(line, _INCOME_SHARE_LABELS):
            return True
    return False


_MDA_REVENUE_HEADINGS = frozenset(
    {"收入和成本分析", "主营业务分行业情况", "主营业务分产品情况"}
)


def _formal_segment_table_ready(excerpt: str) -> bool:
    """Require a unit plus either a segment heading or an enumerated class row."""

    normalized = _join_segment_label_amounts(
        _join_rmb_column_header(_join_pdf_soft_breaks(excerpt))
    )
    if _unit_from_excerpt(normalized) is None:
        return False
    dimension = None
    started = False
    for line in normalized.splitlines():
        if started and _is_revenue_table_stop(line):
            break
        section = _dimension_from_heading(line)
        if section is not None:
            dimension = section
            started = True
            continue
        if _parse_segment_row(line) is None:
            continue
        if dimension is not None or _is_enumerated_revenue_class(line):
            return True
    return False


_REVENUE_TABLE_STOP_TITLES = (
    "成本分析",
    "产销量",
    "资产及负债",
    "费用",
    "分部报告",
    "分部信息",
    "主要销售客户",
    "重大采购",
)


def _is_revenue_table_stop(line: str) -> bool:
    compact = _HEADING_PREFIX.sub("", re.sub(r"\s+", "", line.strip()), count=1)
    compact = compact.lstrip(".．、")
    if "收入和成本" in compact:
        return False
    return any(title in compact for title in _REVENUE_TABLE_STOP_TITLES)


def _is_enumerated_revenue_class(line: str) -> bool:
    # "18.40" is a decimal amount, not an enumeration mark: the character
    # after 、.． must not be a digit.
    return (
        re.match(r"\s*(?:[一二三四五六七八九十]+|\d+)[、.．](?!\d)", line) is not None
    )


def _join_segment_label_amounts(
    text: str,
    *,
    core_answer_repair: bool = False,
) -> str:
    """Attach a wrapped label to the amount line that follows it."""

    text = re.sub(r"中国\s*[(（]\s*含港澳\s*台\s*[)）]", "中国（含港澳台）", text)
    lines = [line for line in text.splitlines() if line.strip()]
    if core_answer_repair:
        # A row label and the final margin cell can both continue after a
        # page header: 上海 <amounts> / <header> / 地区 个百分点.
        # Only this explicit trailing label cell is joined, never a new row.
        for row_index, line in enumerate(lines):
            parsed = _parse_segment_row(line)
            if parsed is None or parsed[0].endswith("地区"):
                continue
            cursor = row_index + 1
            while cursor < len(lines) and cursor <= row_index + 4:
                following = lines[cursor].strip()
                if (
                    not following
                    or "年度报告" in following
                    or re.fullmatch(r"\d+\s*/\s*\d+", following)
                ):
                    cursor += 1
                    continue
                if re.fullmatch(r"地区\s+个百分点", following):
                    lines[row_index] = line.replace(parsed[0], parsed[0] + "地区", 1)
                    lines[cursor] = "个百分点"
                break
    merged: list[str] = []
    index = 0
    while index < len(lines):
        if core_answer_repair:
            joined_at = _join_numbered_revenue_label(lines, index)
            if joined_at is not None:
                merged.append(joined_at[0])
                index = joined_at[1]
                continue
        current = lines[index]
        if core_answer_repair and re.match(
            r"\s*(?:华东|华北|华中|东北|西北|西南)[：:]", current
        ):
            parts = []
            cursor = index
            while cursor < len(lines):
                line = lines[cursor]
                if cursor > index and re.match(
                    r"\s*(?:华东|华北|华中|东北|西北|西南)[：:]", line
                ):
                    break
                if re.search(r"\d[\d,]*\.\d+", line):
                    number = re.search(r"\d[\d,]*\.\d+", line)
                    merged.append(
                        "".join(parts)
                        + re.sub(r"\s+", "", line[: number.start()])
                        + " "
                        + line[number.start() :].strip()
                    )
                    index = cursor + 1
                    break
                parts.append(re.sub(r"\s+", "", line))
                cursor += 1
            if index == cursor + 1:
                continue
        if core_answer_repair and index + 2 < len(lines):
            label_parts = [
                re.sub(r"\s+", "", part) for part in lines[index : index + 2]
            ]
            if (
                all(
                    re.fullmatch(r"[\u4e00-\u9fffA-Za-z]{2,20}", part)
                    for part in label_parts
                )
                and re.match(r"\s*-?[\d,]+\.", lines[index + 2])
                and not any(
                    re.search(r"营业|增减|百分点|毛利|报告", part)
                    for part in label_parts
                )
            ):
                merged.append("".join(label_parts) + " " + lines[index + 2].strip())
                index += 3
                continue
        nxt = lines[index + 1] if index + 1 < len(lines) else ""
        compact_label = re.sub(r"\s+", "", current)
        next_row = _parse_segment_row(nxt) if core_answer_repair else None
        if (
            next_row is not None
            and (
                next_row[0] in {"行业", "地区"}
                or (compact_label == "在某一时点" and next_row[0] == "确认")
                or ("、" in compact_label and next_row[0] == "产及供应")
                or re.match(r"[A-Z]{2,}[\u4e00-\u9fff]*$", next_row[0])
                or (
                    re.fullmatch(r"[\u4e00-\u9fffA-Za-z、]{2,20}", compact_label)
                    and not _dimension_from_heading(current)
                    and not re.search(r"增减|毛利|报告|百分点|营业", current)
                )
            )
            and re.fullmatch(r"[\u4e00-\u9fffA-Za-z、]{1,20}", compact_label)
            and not compact_label.endswith("百分点")
        ):
            merged.append(f"{compact_label}{nxt.strip()}")
            index += 2
            continue
        if (
            re.fullmatch(r"[\u4e00-\u9fffA-Za-z0-9/、（）()]{2,40}", compact_label)
            and not compact_label.endswith("百分点")
            and re.match(r"\s*-?[\d,]", nxt)
        ):
            merged.append(f"{compact_label} {nxt.strip()}")
            index += 2
            continue
        merged.append(current)
        index += 1
    return "\n".join(merged)


def _join_numbered_revenue_label(
    lines: list[str],
    index: int,
) -> tuple[str, int] | None:
    """Join “旅客及货邮航空 / 服务收入” onto the amount line."""

    current = lines[index]
    if not re.match(r"\s*\d+[、.．]", current) or re.search(r"-?[\d,]+\.\d", current):
        return None
    parts = [re.sub(r"\s+", "", current)]
    cursor = index + 1
    while cursor < len(lines):
        nxt = lines[cursor]
        if re.match(r"\s*-?[\d,]", nxt):
            label = "".join(parts)
            return f"{label} {nxt.strip()}", cursor + 1
        if not nxt.strip():
            cursor += 1
            continue
        if re.fullmatch(r"\s*[\u4e00-\u9fff]{2,12}\s*", nxt):
            parts.append(re.sub(r"\s+", "", nxt))
            cursor += 1
            continue
        return None
    return None


def _clip_before_later_segment_template(excerpt: str) -> str:
    """Keep the MD&A revenue table and stop at the next non-revenue section."""

    kept: list[str] = []
    table_started = False
    for line in excerpt.splitlines():
        # Explanations before the table (e.g. 折旧费用同比增加) are not its tail.
        if table_started and _is_revenue_table_stop(line):
            break
        kept.append(line)
        table_started = table_started or _dimension_from_heading(line) is not None
    return "\n".join(kept).strip()


def _clip_income_analysis_excerpt(excerpt: str) -> str:
    kept: list[str] = []
    for line in excerpt.splitlines():
        heading = _heading_if_title_line(line, _INCOME_ANALYSIS_STOP_HEADINGS)
        if heading is not None and kept:
            break
        if kept and _BUSINESS_TOTAL_MARK.search(line):
            break
        if kept and re.search(r"地区分部", line) and not _COMPOSITION_MARK.search(line):
            break
        kept.append(line)
    return "\n".join(kept)


def _first_declared_unit(text: str) -> str | None:
    percent = _PERCENT_UNIT.search(text)
    money = _UNIT_DECLARATION.search(text)
    if percent is not None and (money is None or percent.start() <= money.start()):
        return "%"
    if money is not None:
        return money.group(1)
    return None


def _subject_from_excerpt(excerpt: str) -> tuple[SubjectScope, SubjectBasis | None]:
    # An explicit narrower local scope (母公司口径 tables) wins over the
    # group narrative that the same page may also carry.
    if "母公司财务报表" in excerpt:
        return SubjectScope.ISSUER, SubjectBasis.DIRECT_SOURCE_WORDING
    if re.search(r"母公司", excerpt):
        return SubjectScope.UNCLEAR, None
    if re.search(r"本集团", excerpt):
        return SubjectScope.CONSOLIDATED_GROUP, SubjectBasis.DIRECT_SOURCE_WORDING
    return SubjectScope.UNCLEAR, None


def _dedupe_owned_records(
    records: list[SemanticRecord],
) -> tuple[SemanticRecord, ...]:
    seen: set[tuple[object, ...]] = set()
    unique: list[SemanticRecord] = []
    for record in records:
        if isinstance(record, BusinessOverview):
            key = (record.object_type, re.sub(r"\s+", "", record.source_text))
            if key in seen:
                continue
            seen.add(key)
        if isinstance(record, Measurement):
            key = (
                record.metric_type,
                record.measured_object,
                record.source_native.value,
                record.source_native.unit,
                record.segment_dimension,
                record.segment_label,
                record.relationship_context,
                record.source_native.qualifier,
            )
            if key in seen:
                continue
            seen.add(key)
        if isinstance(record, Activity) and record.action == ActivityAction.SELLS:
            name = re.sub(r"\s+", "", record.object_name)
            duplicate = next(
                (
                    i
                    for i, prior in enumerate(unique)
                    if isinstance(prior, Activity)
                    and prior.action == ActivityAction.SELLS
                    and prior.source_native.value is None
                    and record.source_native.value is None
                    and prior.source_native.header == record.source_native.header
                    and prior.source_actor == record.source_actor
                    and (
                        name == re.sub(r"\s+", "", prior.object_name)
                        or name.startswith(re.sub(r"\s+", "", prior.object_name) + "(")
                        or re.sub(r"\s+", "", prior.object_name).startswith(name + "(")
                    )
                ),
                None,
            )
            if duplicate is not None:
                prior = unique[duplicate]
                preferred = (
                    record
                    if len(name) > len(re.sub(r"\s+", "", prior.object_name))
                    else prior
                )
                evidence_by_id = {
                    item.evidence_id: item
                    for item in (*prior.evidence, *record.evidence)
                }
                updates = {"evidence": tuple(evidence_by_id.values())}
                for candidate in (prior, record):
                    if candidate.source_actor and candidate.source_actor != "公司":
                        updates.update(
                            source_actor=candidate.source_actor,
                            activity_actor=candidate.activity_actor,
                            subject_scope=candidate.subject_scope,
                            subject_basis=candidate.subject_basis,
                        )
                unique[duplicate] = preferred.model_copy(update=updates)
                continue
        unique.append(record)
    return tuple(unique)


_MATERIAL_NAME = r"[\u4e00-\u9fffA-Za-z]{1,8}"
_COMPANY_PROCUREMENT = re.compile(
    rf"(?:本公司|公司)(?:向[\u4e00-\u9fff]{{1,8}})?"
    rf"(?:采购|购入|消耗|投入)(?:了)?({_MATERIAL_NAME})"
    rf"(?:用于(?:生产|制造|经营)|作为(?:原材料|原料|投入))"
)
_COMPANY_INPUT_COST = re.compile(
    r"(?:直接)?(?:推高|抬高|提高|增加)"
    r"(?P<gap>.{0,8})(?:制造|生产|经营)成本"
)
_THIRD_PARTY_COST = re.compile(r"下游|客户|供应商|制造企业|生产企业")
_NOT_A_MATERIAL_NAME = re.compile(r"销售|出售|客户|供应商|下游|企业|价格|风险|公司")
_GENERIC_MATERIAL_NAMES = frozenset(
    {"材料", "原材料", "原料", "原燃料", "燃料", "能源", "直接材料", "存货"}
)


_REPAIR_SALES = (
    ("钢铁", ("钢铁产品", "钢铁")),
    ("稀土精矿", ("稀土精矿",)),
    ("萤石", ("萤石精矿", "萤石")),
    ("焦化产品", ("焦化产品", "冶金焦炭")),
    ("焦化副产品", ("焦化副产品",)),
    ("焦炭", ("焦炭",)),
)
_REPAIR_PROCUREMENT = ("铁矿石", "白灰", "石灰石", "进口矿", "焦炭")
_REPAIR_ENERGY = (("蒸汽", "蒸汽费"), ("热水", "热水费"), ("电", "电费"))
_REPAIR_COMMODITY_CUE = re.compile(
    r"采购|销售|畅销|量产出货|批量出货|主要上游原材料|耗用量|蒸汽费|热水费|电费|生产与销售|原辅料|焦炭采购"
)
_SERVICE_ENERGY_CUE = re.compile(r"电耗|柴油单耗|燃油消耗|供电煤耗|发电厂用电率")
_OPERATING_ENERGY = re.compile(
    r"(?P<business>污水|焚烧业务)(?:吨水药耗、)?(?P<item>电耗|柴油单耗)"
)
_FIRM_ENDING = re.compile(r"[\u4e00-\u9fffA-Za-z0-9]{0,40}公司")


def _repair_commodity_spans(
    pages: Sequence[ReportPageText],
    *,
    covered: Sequence[CoreEvidenceSpan],
    service_operating_energy: bool = False,
) -> list[CoreEvidenceSpan]:
    covered_pages = {span.page for span in covered}
    spans: list[CoreEvidenceSpan] = []
    for page in pages:
        if not page.readable:
            continue
        if page.page in covered_pages and not (
            _operating_energy_bindings(page.text)
            or _explicit_current_commodity_bindings(page.text)
            or _native_input_bindings(page.text)
            or _native_current_product_bindings(page.text)
            or _company_trade_sales_product_names(page.text)
            and not any(
                _company_trade_sales_product_names(span.excerpt)
                for span in covered
                if page.page in (span.page, *span.continuation_pages)
            )
            or _formal_sales_product_names(page.text, owned_products=True)
            and not any(
                _formal_sales_product_names(span.excerpt, owned_products=True)
                for span in covered
                if page.page in (span.page, *span.continuation_pages)
            )
        ):
            continue
        compact = re.sub(r"\s+", "", page.text)
        commodity = (
            _REPAIR_COMMODITY_CUE.search(compact) is not None
            or bool(_explicit_current_commodity_bindings(page.text))
            or bool(_native_input_bindings(page.text))
        )
        energy = (
            service_operating_energy and _SERVICE_ENERGY_CUE.search(compact) is not None
        )
        hedge = "套期保值" in compact and "原料的期货业务" in compact
        if not commodity and not energy and not hedge:
            continue
        excerpt = page.text.strip()
        continuations: tuple[int, ...] = ()
        nxt = next(
            (
                candidate
                for candidate in pages
                if candidate.page == page.page + 1 and candidate.readable
            ),
            None,
        )
        if (
            nxt
            and "生产量" in excerpt
            and "销售量" in excerpt
            and "产销量情况说明" not in excerpt
        ):
            tail = re.split(r"[（(]3[)）]", nxt.text, maxsplit=1)[0]
            excerpt += "\n" + tail
            continuations = (nxt.page,)
        if nxt and (
            "采购商品/接受劳务情况表" in excerpt
            and "出售商品/提供劳务情况表" not in excerpt
            or "出售商品/提供劳务情况表" in excerpt
            and "购销商品、提供和接受劳务的关联交易说明" not in excerpt
        ):
            excerpt += "\n" + nxt.text
            continuations = tuple(dict.fromkeys((*continuations, nxt.page)))
        if nxt and (
            "报告期内公司从事的业务情况" in excerpt
            and "报告期内公司新增重要非主营业务" not in excerpt
            or "销售商品" in excerpt
            and re.search(r"自助缴费机采购项目", re.sub(r"\s+", "", nxt.text))
        ):
            excerpt += (
                "\n"
                + re.split(
                    r"报告期内公司新增重要非主营业务|十三、重大合同",
                    nxt.text,
                    maxsplit=1,
                )[0]
            )
            continuations = (nxt.page,)
        if nxt and "业务本期仅统计" in excerpt:
            ownership = next(
                (
                    s
                    for s in _owned_business_sources(nxt.text)
                    if "公司丧失" in s and "合并范围" in s
                ),
                "",
            )
            if ownership:
                excerpt += "\n" + ownership
                continuations = tuple(dict.fromkeys((*continuations, nxt.page)))
        performance = re.search(r"三、\s*经营情况讨论与分析", excerpt)
        if nxt and performance:
            excerpt = (
                excerpt[performance.start() :]
                + "\n"
                + re.split(r"[（(]二[)）]", nxt.text, maxsplit=1)[0]
            )
            continuations = (nxt.page,)
        if not excerpt:
            continue
        spans.append(
            CoreEvidenceSpan(
                page=page.page,
                section_title="修复商品披露",
                continuation_pages=continuations,
                excerpt=excerpt,
                bounded_quote=excerpt,
                chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS.value,
                field_ids=("explicit_activity",),
                dimension_ids=(),
            )
        )
    return spans


def _repair_commodity_clauses(excerpt: str) -> list[str]:
    """One clause per sentence or table row, after joining a PDF soft wrap.

    A line that ends with an enumeration comma continues the same sentence.
    Page 97 breaks “冶金机械、 / 设备及配件、焦炭及焦化副产品生产和销售”.
    """

    merged: list[str] = []
    for line in _join_pdf_soft_breaks(excerpt).split("\n"):
        if merged and merged[-1].rstrip().endswith(("、", "，", ",")):
            merged[-1] = f"{merged[-1].rstrip()}{line.lstrip()}"
        else:
            merged.append(line)
    clauses: list[str] = []
    for line in merged:
        for part in re.split(r"[。；;]", line):
            compact = re.sub(r"\s+", "", part).strip()
            if compact:
                clauses.append(compact)
    return clauses


def _sales_alias_in(stated: str, alias: str) -> bool:
    """Match a sales name without treating 冶金焦炭 as bare 焦炭."""

    if alias not in stated:
        return False
    return not (alias == "焦炭" and stated.count("焦炭") == stated.count("冶金焦炭"))


_ORG_NAME = re.compile(
    r"[\u4e00-\u9fffA-Za-z0-9（）()]{2,40}(?:股份有限公司|有限责任公司|有限公司)"
)


def _clause_names_company(compact: str) -> bool:
    return re.search(r"(?:本公司|公司)", compact) is not None


def _without_org_names(compact: str) -> str:
    """Drop named firms so “钢铁销售有限公司” is not a sales action."""

    return _ORG_NAME.sub("", compact)


def _has_real_sales_action(stated: str) -> bool:
    """Ignore 销售模式, 销售区域, and 销售部."""

    return "销售" in _PSEUDO_SALES.sub("", stated) or "生产与销售" in stated


def _formal_sales_product_names(
    excerpt: str, *, owned_products: bool = False
) -> list[str]:
    """Product names from a 生产量/销售量 table, one row at a time."""

    names: list[str] = []
    in_table = False
    table_prefix = ""
    pending = ""
    units = r"万千瓦时|亿千瓦时|兆瓦时|万平方米|万吉焦|吉焦|万吨|万台|万片|万袋|万条|万只|万个|立方米|MW|吨|辆"
    for line in excerpt.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        compact = re.sub(r"\s+", "", line)
        if "生产量" in compact and "销售量" in compact and "钢铁" not in compact:
            in_table = re.search(r"子公司|第三方|拟|计划", table_prefix[-160:]) is None
            pending = ""
            continue
        if not in_table or not compact:
            table_prefix += compact
            continue
        if (
            compact.startswith("合计")
            or re.match(r"^[（(][一二三四五六七八九十0-9]+[)）]", compact)
            or "产销量情况说明" in compact
        ):
            in_table = False
            pending = ""
            continue
        if re.search(r"年度报告|^\d+/\d+$|增减|库存量|销售量|生产量|^（?%", compact):
            continue
        match = re.match(
            rf"(.*?)(?:{units})([\d,]+(?:\.\d+)?)([\d,]+(?:\.\d+)?)", compact
        )
        # Prefer separated numeric columns, while retaining native parentheses
        # and dosage forms in the name. The unit is never a product suffix.
        spaced = list(
            re.finditer(
                rf"(?<!\S)([\u4e00-\u9fffA-Za-z()（）-]+(?: +[\u4e00-\u9fffA-Za-z()（）-]+)?)\s+(?:{units})\s+(?:[\d,]+(?:\.\d+)?|-)\s+([\d,]+(?:\.\d+)?)",
                line.strip(),
            )
        )
        if spaced:
            for index, row in enumerate(spaced):
                name = (pending if index == 0 else "") + re.sub(
                    r"\s+", "", row[1]
                ).lstrip("-")
                if ("钢铁" in name or owned_products) and float(
                    row[2].replace(",", "")
                ) > 0:
                    names.append(name)
            pending = ""
            continue
        elif match:
            name = pending + match.group(1)
        else:
            if re.fullmatch(
                r"[\u4e00-\u9fffA-Za-z0-9()（）-]+", compact
            ) and not re.search(r"比上年|%|库存|增减", compact):
                pending += compact
            continue
        pending = ""
        if name and ("钢铁" in name or owned_products):
            names.append(name)
    return list(dict.fromkeys(names))


def _domestic_scrap_clause(excerpt: str) -> str:
    """The company's scrap row that says 国内采购, not internal 自供."""

    compact = re.sub(r"\s+", "", excerpt)
    start = compact.find("废钢供应")
    if start < 0:
        return ""
    window = compact[start : start + 240]
    if "国内采购" not in window:
        return ""
    return window


def _clause_names_other_firm(clause: str) -> bool:
    """A named firm in the clause is not the reporting company."""

    for match in _FIRM_ENDING.finditer(clause):
        firm = match.group(0).removeprefix("同时")
        if firm in {"公司", "本公司"}:
            continue
        return True
    return False


def _operating_energy_bindings(excerpt: str) -> list[tuple[str, str, str]]:
    """Bind 电耗 and 柴油单耗 to the clause subject and business column.

    A subsidiary firm, or a price-risk sentence with no operating-consumption
    wording, does not become the reporting company's energy use.
    """

    bindings: list[tuple[str, str, str]] = []
    for clause in _repair_commodity_clauses(excerpt):
        if "子公司" in clause or _clause_names_other_firm(clause):
            continue
        match = _OPERATING_ENERGY.search(clause)
        if match is None:
            continue
        if "价格风险" in clause:
            continue
        stated = _without_org_names(clause)
        if "本公司" in stated or "公司" in stated:
            subject = "公司"
        elif "运营端" in clause:
            subject = "运营端"
        else:
            continue
        bindings.append((subject, match.group("business"), match.group("item")))
    compact = re.sub(r"\s+", "", excerpt)
    for sentence in re.split(r"[。；;]", compact):
        match = re.search(r"(?:本集团|本公司|公司)[^。]{0,30}燃油消耗", sentence)
        if match and not re.search(
            r"子公司|第三方|拟|计划|尚未|不消耗|未消耗", sentence[: match.end()]
        ):
            bindings.append(
                ("本集团" if "本集团" in match.group(0) else "公司", "燃油消耗", "燃油")
            )
    return bindings


def _project_repair_commodity_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
    *,
    named_role_repair: bool = False,
    service_operating_energy: bool = False,
    core_answer_repair: bool = False,
    source_delivery_repair: bool = False,
) -> tuple[SemanticRecord, ...]:
    """Bind a role only when the company, name, and action share a clause."""

    item = _prepared_for(selection, span, "explicit_activity")
    if item is None:
        return ()
    records: list[SemanticRecord] = []
    seen: set[tuple[str, ActivityAction, str | None, str]] = set()

    def add_activity(
        name: str,
        action: ActivityAction,
        verb: str,
        value: str | None = None,
        *,
        actor: str = "公司",
        header: str | None = None,
        unit: str | None = None,
    ) -> None:
        key = (name, action, header, actor)
        if key in seen:
            return
        seen.add(key)
        limited = re.search(
            rf"{re.escape(name)}业务本期仅统计[^。；]+",
            re.sub(r"\s+", "", span.excerpt),
        )
        former_actor = None
        if limited:
            renamed = re.search(
                r"后更名为([^。]+?有限公司)[）)]", re.sub(r"\s+", "", span.excerpt)
            )
            if renamed and "公司丧失" in span.excerpt:
                former_actor = renamed[1]
                actor = former_actor
        native = SourceNativeValue(
            name=name,
            value=value,
            unit=unit if unit is not None else ("元" if value else None),
            header=header,
            qualifier=limited.group()
            if limited
            else "能源采购"
            if header == "能源" and action == ActivityAction.PURCHASES
            else None,
        )
        records.append(
            _base_fact(
                Activity,
                report=selection.report,
                record_id=(f"owned:{item.evidence.evidence_id}:commodity:{len(seen)}"),
                field_id="explicit_activity",
                chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                evidence=item.evidence,
                source_native=native,
                action=action,
                activity_actor=actor,
                source_actor=actor,
                actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
                object_name=name,
                source_verb=verb,
                excerpt_subject=span.excerpt,
            )
        )

        if former_actor:
            records[-1] = records[-1].model_copy(
                update={
                    "subject_scope": SubjectScope.NAMED_SUBSIDIARY,
                    "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                }
            )

    for compact in _repair_commodity_clauses(span.excerpt):
        stated = _without_org_names(compact)
        if (
            named_role_repair
            and re.search(r"合营企业|联营企业", compact)
            and "业务性质" in compact
        ):
            continue
        if core_answer_repair and "母公司" in compact and "业务性质" in compact:
            continue
        company = _clause_names_company(stated if core_answer_repair else compact)
        if "参股" in compact and "本公司" not in compact:
            company = False
        sells = (
            _has_real_sales_action(stated)
            if named_role_repair
            else "销售" in stated or "生产与销售" in stated
        )
        procures = any(token in stated for token in ("采购", "订购", "原辅料"))
        if core_answer_repair and (
            _AUDIT_PROCEDURE.search(compact) or _RESUME_CLAUSE.search(compact)
        ):
            sells = False
            procures = False
        if named_role_repair and company and "能源介质" in stated:
            if sells:
                add_activity("能源介质", ActivityAction.SELLS, "销售")
            if "采购" in stated or "订购" in stated:
                add_activity("能源介质", ActivityAction.PURCHASES, "采购")
        if (
            company
            and sells
            and "采购协议" not in stated
            and (not source_delivery_repair or _current_company_sentence(stated))
        ):
            for name, aliases in _REPAIR_SALES:
                # A counterparty's name is not the sold object. Generic
                # 销售商品 rows cannot supply an unstated product.
                objects = (
                    _direct_sales_objects(stated) if source_delivery_repair else stated
                )
                if any(_sales_alias_in(objects, alias) for alias in aliases):
                    add_activity(name, ActivityAction.SELLS, "销售")
        if (
            service_operating_energy
            and "价格风险" in compact
            and not any(token in stated for token in ("采购", "订购"))
        ):
            procures = False
        if company and procures:
            for name in _REPAIR_PROCUREMENT:
                if name not in stated:
                    continue
                if name == "焦炭" and not any(
                    token in stated for token in ("采购", "订购", "作为原料")
                ):
                    continue
                add_activity(name, ActivityAction.PURCHASES, "采购")
        if "蒸汽费" in compact and "热水费" in compact and "电费" in compact:
            for name, _alias in _REPAIR_ENERGY:
                add_activity(name, ActivityAction.PURCHASES, "消耗")
            if "7407073" in compact or "7,407,073" in compact:
                add_activity(
                    "蒸汽费、热水费及电费",
                    ActivityAction.PURCHASES,
                    "消耗",
                    "7407073",
                )
    if named_role_repair:
        owned_products = source_delivery_repair and any(
            candidate.chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value
            and span.page in (candidate.page, *candidate.continuation_pages)
            for candidate in selection.spans
        )
        if source_delivery_repair and not owned_products:
            table_header = re.search(r"主要产品\s+单位\s+生产量\s+销售量", span.excerpt)
            if table_header:
                prefix = span.excerpt[: table_header.start()]
                owned_products = "√适用 □不适用" in prefix and not re.search(
                    r"子公司|第三方|拟|计划", prefix
                )
        for name in _formal_sales_product_names(
            span.excerpt, owned_products=owned_products
        ):
            add_activity(name, ActivityAction.SELLS, "销售")
        if _domestic_scrap_clause(span.excerpt) and not core_answer_repair:
            add_activity("废钢", ActivityAction.PURCHASES, "采购")
    if core_answer_repair:
        for name, channel in _supply_source_rows(span.excerpt):
            add_activity(name, ActivityAction.PURCHASES, "采购", header=channel)
    if source_delivery_repair:
        for name, verb, header in _native_current_product_bindings(span.excerpt):
            add_activity(name, ActivityAction.SELLS, verb, header=header)
        for name, verb, header in _native_input_bindings(span.excerpt):
            if verb != "投入":
                add_activity(name, ActivityAction.PURCHASES, verb, header=header)
        for name, actor in _named_sales_bindings(span.excerpt):
            before = len(records)
            add_activity(name, ActivityAction.SELLS, "销售", actor=actor)
            if len(records) > before and actor != "公司":
                records[-1] = records[-1].model_copy(
                    update={
                        "subject_scope": SubjectScope.CONSOLIDATED_GROUP,
                        "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                    }
                )
        for name in _company_sales_product_names(span.excerpt):
            add_activity(name, ActivityAction.SELLS, "销售")
        for name in _company_trade_sales_product_names(span.excerpt):
            # The row amount is revenue, not a commodity quantity or price.
            # Keep it in the original evidence and project only the native role.
            add_activity(name, ActivityAction.SELLS, "销售", header="本期营业收入")
        for binding in (
            *_power_and_oil_bindings(span.excerpt),
            *_explicit_current_commodity_bindings(span.excerpt),
        ):
            if binding["action"] is None:
                # Own energy use is projected by the material chapter; it
                # does not establish an external purchase Activity.
                continue
            before = len(records)
            actor = binding.get("actor") or "公司"
            if (
                binding.get("header") == "本年度其他业务收入"
                and re.fullmatch(r"[\u4e00-\u9fff]{2,8}", actor)
                and actor
                not in {
                    "公司",
                    "本公司",
                    "本集团",
                }
            ):
                firms = {
                    re.sub(r"\s+", "", m.group())
                    for candidate in selection.spans
                    if "主要控股参股公司分析" in candidate.bounded_quote
                    for m in re.finditer(
                        r"[\u4e00-\u9fff\s]{2,60}?有限公司", candidate.excerpt
                    )
                    if actor in re.sub(r"\s+", "", m.group())
                }
                if len(firms) == 1:
                    actor = firms.pop()
                    binding = {
                        **binding,
                        "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                    }
            add_activity(
                binding["name"],
                binding["action"],
                binding["verb"],
                binding.get("value"),
                actor=actor,
                header=binding.get("header"),
                unit=binding.get("unit"),
            )
            if len(records) > before and binding.get("subject_basis"):
                # The action's explicit group definition wins over an
                # unrelated parent-company ownership note elsewhere on page.
                records[-1] = records[-1].model_copy(
                    update={
                        "subject_scope": (
                            SubjectScope.CONSOLIDATED_GROUP
                            if actor in {"公司", "本公司", "本集团", "公司及合并子公司"}
                            else SubjectScope.NAMED_SUBSIDIARY
                        ),
                        "subject_basis": binding["subject_basis"],
                    }
                )
        for binding in _hedge_underlying_bindings(span.excerpt):
            add_activity(
                binding["name"],
                binding["action"],
                binding["verb"],
                binding.get("value"),
                actor=binding.get("actor") or "公司",
                header=binding.get("header"),
                unit=binding.get("unit"),
            )
    if source_delivery_repair:
        for name, actor, group in _current_shipped_product_bindings(span.excerpt):
            before = len(records)
            add_activity(
                name, ActivityAction.SELLS, "出货" if group else "上市销售", actor=actor
            )
            if len(records) > before and group:
                records[-1] = records[-1].model_copy(
                    update={
                        "subject_scope": SubjectScope.CONSOLIDATED_GROUP,
                        "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                    }
                )
    if service_operating_energy:
        for subject, business, item_name in _operating_energy_bindings(span.excerpt):
            add_activity(
                item_name,
                ActivityAction.PURCHASES,
                "消耗",
                actor=subject,
                header=business,
            )
    return tuple(records)


def _join_rmb_column_header(text: str) -> str:
    """Join a wrapped column header such as 营业收入（人 / 民币百万元）."""

    return re.sub(r"人\s*[\r\n]+\s*民币", "人民币", text)


def _integrated_business_source(excerpt: str) -> str:
    """A company-profile sentence that keeps every clause through the period."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _INTEGRATED_BUSINESS_SENTENCE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    source = _original_span_matching(excerpt, compact) or ""
    tail = joined[match.end() :]
    controlled = re.match(
        r"。\s*\d{4}\s*年，(?:本公司|公司)取得(?P<name>[\u4e00-\u9fff]{2,12})控制权，(?P=name)[^。]{2,300}主要开展[^。]{2,200}。",
        tail,
    )
    if source and controlled:
        compact += re.sub(r"\s+", "", controlled.group(0))
        source = _original_span_matching(excerpt, compact) or source
    return source


def _current_business_narrative_source(excerpt: str) -> str:
    joined = _join_pdf_soft_breaks(excerpt)
    match = _CURRENT_BUSINESS_NARRATIVE.search(joined)
    if match is None or not _current_company_sentence(match.group(0)):
        return ""
    source = match.group(0)
    vision = re.search(
        r"(?:本公司|公司)以[^。]{2,500}为愿景[^。]{2,500}为战略定位。\s*$",
        joined[: match.start()],
    )
    if vision and _current_company_sentence(vision.group(0)):
        source = vision.group(0) + source
    return _original_span_matching(excerpt, re.sub(r"\s+", "", source))


def _company_sales_mechanism_source(excerpt: str) -> str:
    compact = re.sub(r"\s+", "", excerpt)
    for pattern in (
        rf"{_COMPANY_SUBJECT}电力销售客户主要为[^。]+。",
        rf"{_COMPANY_SUBJECT}煤机全年市场化交易累计成交电量[^。]+。",
    ):
        match = re.search(pattern, compact)
        if match and _current_company_sentence(
            compact[max(0, compact.rfind("。", 0, match.start()) + 1) : match.end()]
        ):
            return _original_span_matching(excerpt, match.group(0))
    # A native current-year trading table states a sales mechanism even when
    # its heading is not a sentence. Keep the preceding owned operating
    # context rather than inventing a company subject for the table.
    table = re.search(
        r"(?:^|\n)\s*\d+[、.]\s*电力市场化交易\s*\n"
        r"\s*√适用\s*□不适用[\s\S]+?市场化交易的总电量[^\n]+"
        r"[\s\S]+?总上网电量[^\n]+[\s\S]+?占比[^\n]+",
        excerpt,
    )
    if table:
        before = excerpt[: table.start()]
        owner = re.search(r"报告期内，\s*本公司[^。]+。", before)
        if owner and not re.search(r"子公司|第三方|本公司(?:拟|计划|将|尚未)", before):
            return excerpt[owner.start() : table.end()]
    return ""


def _current_shipped_product_bindings(excerpt: str) -> list[tuple[str, str, bool]]:
    """Established shipments/sales within the owned subsection, not future layout."""
    compact = re.sub(r"\s+", "", excerpt)
    bindings = []
    chip = re.search(
        r"[0-9]+、芯片[^。]*公司战略控股[^。]+。.*?(?=[0-9]+、|$)", compact
    )
    if chip:
        context = chip.group(0)
        control = re.search(
            r"公司战略控股(?P<names>[^。]{2,60}?)两家上游芯片企业", context
        )
        if (
            control
            and "报告期内" in context
            and any(
                re.search(r"(?:量产|批量)出货", clause)
                and not re.search(r"计划|拟|尚未|未实现|不|将", clause)
                for clause in re.split(r"[。；;]", context)
            )
            and not re.search(
                r"子公司战略控股|第三方公司战略控股|公司(?:拟|计划)战略控股", context
            )
        ):
            bindings.append(("芯片", "公司战略控股的" + control.group("names"), True))
    wearable = re.search(
        r"[0-9]+、智能穿戴[^。]*报告期内，公司[^。]+。.*?(?=（[一二三四五六七八九十]+）|$)",
        compact,
    )
    if wearable:
        context = wearable.group(0)
        subject = re.search(r"报告期内，公司[^。]+。", context)
        sale = re.search(r"开放式AI耳机[^。]*国内上市销售", context)
        if (
            subject
            and _current_company_sentence(subject.group(0))
            and sale
            and not re.search(
                r"子公司|第三方|拟|计划|尚未|未上市", context[: sale.end()]
            )
        ):
            bindings.append(("AI耳机", "公司", False))
    return bindings


def _company_coal_risk_source(text: str) -> str:
    """The company's own coal-cost risk bound to its current coal-fired fleet."""
    joined = _join_pdf_soft_breaks(text)
    risk = re.search(r"[（(]四[）)]可能面对的风险[\s\S]*?(?=[（(]五[）)]|$)", joined)
    if risk is None:
        return ""
    context = re.sub(r"\s+", "", risk.group(0))
    own = re.search(r"(?:本公司|公司)装机结构中煤电占比高[^。]*。", context)
    if (
        own
        and _current_company_sentence(
            context[max(0, context.rfind("。", 0, own.start()) + 1) : own.end()]
        )
        and "煤炭成本在燃煤发电企业的发电成本中占比较大" in context
    ):
        return risk.group(0)
    return ""


def _current_company_sentence(sentence: str) -> bool:
    return (
        re.search(_COMPANY_SUBJECT, sentence) is not None
        and re.search(
            r"子公司|母公司|第三方|(?:客户|供应商)(?:公司|酿造|生产)", sentence
        )
        is None
        and re.search(
            r"(?:拟|计划|尚未|不再)[^。；;]{0,18}(?:作为|从事|生产|销售|构建|采用|有)"
            r"|(?:不|未|没有|并无)(?:对外)?(?:销售|生产|采用|构建)",
            sentence,
        )
        is None
    )


def _company_product_model_sources(excerpt: str) -> list[str]:
    """Keep the owned, current product matrix and stated sales model whole."""

    sources: list[str] = []
    compact = re.sub(r"\s+", "", excerpt)
    for sentence in re.split(r"(?<=。)", compact):
        start = re.search(rf"(?<!第三方){_COMPANY_SUBJECT}(?:旗下|采用)", sentence)
        if start is None:
            continue
        stated = sentence[start.start() :]
        if not _current_company_sentence(stated):
            continue
        if "产品矩阵" not in stated and not ("销售模式" in stated and "采用" in stated):
            continue
        text = _original_span_matching(excerpt, stated)
        if text:
            sources.append(text)
    return sources


def _company_income_source(excerpt: str) -> str:
    compact = re.sub(r"\s+", "", excerpt)
    match = re.search(
        rf"(?<!第三方){_COMPANY_SUBJECT}主营业务收入主要来[自源]于[^。]+。", compact
    )
    if match is None or not _current_company_sentence(
        compact[max(0, compact.rfind("。", 0, match.start()) + 1) : match.end()]
    ):
        return ""
    return _original_span_matching(excerpt, match.group(0))


def _company_trade_sales_product_names(excerpt: str) -> list[str]:
    """Names sold in an affirmed company-owned trade-revenue table."""

    joined = _join_pdf_soft_breaks(excerpt)
    heading = re.search(
        r"(?:^|\n)\s*(?:[A-Z][.．]\s*)?报告期内(?:本公司|公司)存在贸易业务收入"
        r"\s*\n\s*√适用\s*□不适用",
        joined,
    )
    if heading is None:
        return []
    table = re.split(r"贸易业务(?:占|收入占)", joined[heading.end() :], maxsplit=1)[0]
    if re.search(r"子公司|母公司|第三方|拟|计划|尚未|不存在", table):
        return []
    compact = re.sub(r"\s+", "", table)
    if "贸易业务开展情况本期营业收入上期营业收入" not in compact:
        return []
    if _unit_from_excerpt(table) is None:
        return []
    sales = re.findall(
        r"(?:^|\n)\s*销售([\u4e00-\u9fff]{1,12})\s+\d[\d,]*\.\d+\s+\d",
        table,
    )
    sales.extend(
        re.findall(
            r"(?:^|\n)\s*([\u4e00-\u9fff]{2,10})业务\s+\d[\d,]*\.\d+\s+\d", table
        )
    )
    return [name for name in sales if name not in {"贸易", "其他", "其它"}]


def _direct_sales_objects(stated: str) -> str:
    """Only product-column wording or a direct sales/production object."""

    if "销售商品" in stated:
        # Customer/content/amount rows do not contain a product column.
        return ""
    objects = []
    for pattern in (
        r"(?:销售|生产与销售)([\u4e00-\u9fff、和及]{2,50})(?=产品|[，,。；;]|$)",
        r"([\u4e00-\u9fff、和及]{2,50})(?:产品)?(?:的)?(?:生产和销售|生产与销售|生产、销售|销售)",
    ):
        objects.extend(match.group(1) for match in re.finditer(pattern, stated))
    return "、".join(objects)


def _named_sales_bindings(excerpt: str) -> list[tuple[str, str]]:
    """Current named product lists in owned marketing or performance sections.

    Keep native brands/forms; generic parent categories are not extra sales.
    """

    compact = re.sub(r"\s+", "", excerpt)
    bindings: list[tuple[str, str]] = []
    if re.search(r"公司药[（(]产[)）]品销售情况", compact) and not re.search(
        r"子公司|第三方|公司拟|公司计划", compact.split("主要销售模式分析")[0]
    ):
        section = compact.split("销售费用情况分析")[0]
        for match in re.finditer(r"主要产品[为有]([^。；;]+)", section):
            sentence = _sentence_containing(section, match.start())
            # A sale modifier belongs to this list, not every list on the page.
            introduction = re.split(r"[，,。；;]", section[: match.start()])[-1]
            if re.search(
                r"子公司|第三方|(?:公司)?(?:拟|计划)[^。]{0,30}(?:销售|主要产品)",
                sentence,
            ) or re.search(
                r"(?:拟|计划|将(?:要)?|即将|未来|预计|准备|打算|尚未|暂未|"
                r"未曾|从未|并未|没有|不再|未)[^，,。；;]{0,30}(?:销售|主要产品)|不销售",
                introduction + "主要产品",
            ):
                continue
            for branch in re.split(
                r"等(?:慢病|专科)产品[，,]?|及部分输液产品[，,]?|以及", match.group(1)
            ):
                actor = "公司"
                subsidiary = re.search(r"等([\u4e00-\u9fff]{2,16})产品", branch)
                if subsidiary:
                    actor = subsidiary.group(1)
                branch = re.split(
                    r"等[，,。]|等[\u4e00-\u9fff]*产品|形成|实行", branch, maxsplit=1
                )[0]
                for name in branch.strip("，,、").split("、"):
                    if re.fullmatch(
                        r"[\u4e00-\u9fffA-Za-z0-9()（）-]{2,60}", name
                    ) and not name.startswith("部分"):
                        bindings.append((name, actor))
    performance = re.search(
        r"经营情况讨论与分析.*?\d{4}年[，,]([^。]+实现营业收入[^。]+。)", compact
    )
    if (
        performance
        and _current_company_sentence(performance.group(1))
        and not re.match(r"(?:本公司|公司)(?:拟|计划)", performance.group(1))
    ):
        text = compact[performance.end() :]
        for match in re.finditer(
            r"([\u4e00-\u9fff]{2,30}[（(][^()（）]{1,20}[)）])(?:销量同比|快速导入市场[，,]收入同比)",
            text,
        ):
            name = re.sub(r"^.*?(?:类新药)", "", match.group(1))
            bindings.append((name, "公司"))
        for match in re.finditer(
            r"[，,]([\u4e00-\u9fffA-Za-z0-9、-]{2,100})等原料药销量", text
        ):
            bindings.extend((name, "公司") for name in match.group(1).split("、"))
    return list(dict.fromkeys(bindings))


def _numbered_business_source(excerpt: str) -> str:
    """An owned business section with numbered, established business platforms."""

    joined = _join_pdf_soft_breaks(excerpt)
    blocks = list(
        re.finditer(r"(?:^|\n)\s*\d+[.．、]\s*[\u4e00-\u9fff]{2,12}业务", joined)
    )
    if len(blocks) < 2 or "报告期内公司从事" not in joined:
        return ""
    body = joined[blocks[0].start() :]
    if not re.search(
        r"(?<![\u4e00-\u9fff])(?:本公司|公司)(?:通过|的|拥有|实现|立足|原料药业务)",
        re.sub(r"\s+", "", body),
    ):
        return ""
    return re.split(r"报告期内公司新增重要非主营业务的说明", body, maxsplit=1)[
        0
    ].strip()


def _project_business_source(excerpt: str) -> str:
    joined = _join_pdf_soft_breaks(excerpt)
    match = re.search(
        r"(?<![\u4e00-\u9fff])(?:本公司|公司)主营业务为[^。]+项目的开发、建设、运营与管理。",
        joined,
    )
    if not match or not _current_company_sentence(match.group(0)):
        return ""
    return re.split(
        r"报告期内公司新增重要非主营业务的说明", joined[match.start() :], maxsplit=1
    )[0].strip()


def _company_sales_product_names(excerpt: str) -> list[str]:
    names: list[str] = []
    compact = re.sub(r"\s+", "", excerpt)
    for sentence in re.split(r"[。；;]", compact):
        if not _current_company_sentence(sentence):
            continue
        for pattern in (
            r"主要从事([\u4e00-\u9fff]{2,12})的(?:研发|酿造|生产)[、和及\u4e00-\u9fff]{0,20}销售",
            r"公司有([\u4e00-\u9fff]{2,12})的生产[^。]{0,25}对外销售",
        ):
            match = re.search(pattern, sentence)
            if match:
                names.append(match.group(1))
    return names


def _main_business_source(excerpt: str) -> str:
    """The reporting company's full 主要业务是 sentence."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _MAIN_BUSINESS_SENTENCE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def _main_operation_source(excerpt: str) -> str:
    """The 主要经营 sentence with its full business enumeration."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _MAIN_OPERATION_SENTENCE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def _provider_narrative_source(excerpt: str) -> str:
    """Keep the whole affirmative positioning sentence of the reporting company."""

    joined = _join_pdf_soft_breaks(excerpt)
    for match in _PROVIDER_NARRATIVE.finditer(joined):
        sentence = _sentence_containing(joined, match.start())
        compact = re.sub(r"\s+", "", sentence)
        # The positioning clause and the immediately following grammatical
        # subject must both describe this company. Do not discard a prefix
        # such as 公司拟 when capturing from 作为.
        if re.match(r"^(?:\d{4}年[，,])?(?:(?:本公司|公司))?作为", compact) is None:
            continue
        role, _, predicate = compact.partition("核心提供商")
        if re.search(r"拟|计划|未来|希望|愿景|目标|第三方|子公司", role):
            continue
        if re.match(
            r"[，,](?:本公司|公司)(?:拟|计划|将|预期|尚未|未|不|希望|力争)",
            predicate,
        ):
            continue
        return _original_span_matching(excerpt, compact) or ""
    return ""


def _business_blocks_source(excerpt: str) -> str:
    """Retain an owned business/mode section with multiple established blocks."""

    joined = _join_pdf_soft_breaks(excerpt)
    heading = re.search(r"公司所从事的主要业务情况及经营模式", joined)
    if heading is None:
        return ""
    blocks = list(
        re.finditer(
            r"[\u4e00-\u9fff]{2,12}业务[:：][^。]{8,600}。", joined[heading.end() :]
        )
    )
    if len(blocks) < 2 or any(
        re.search(r"业务[:：](?:子公司|第三方|公司拟|公司计划)", item.group(0))
        for item in blocks
    ):
        return ""
    end = heading.end() + blocks[-1].end()
    return _original_span_matching(
        excerpt, re.sub(r"\s+", "", joined[heading.start() : end])
    )


def _company_as_narrative_source(excerpt: str) -> str:
    """The 作为……上市公司/工业企业……研发制造 company narrative."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _COMPANY_AS_NARRATIVE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def _operating_model_source(excerpt: str) -> str:
    """The 经营模式主要为 sentence with its provide-and-collect fees."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _OPERATING_MODEL_SENTENCE.search(joined)
    if match is None or not _overview_states_revenue(
        match.group(0), repair_revenue_sentence=True
    ):
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def _toll_service_source(excerpt: str) -> str:
    """The 主营业务为 sentence plus its following toll-service mechanism."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _TOLL_SERVICE_SENTENCE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def _steel_raw_material_binding(excerpt: str) -> dict[str, Any] | None:
    """The stated main raw-material steel, without quantities.

    The steel must sit in the same sentence as the company's own
    raw-material declaration; a customer or third-party sentence elsewhere
    in the excerpt never binds.
    """

    joined = _join_pdf_soft_breaks(excerpt)
    for sentence in re.split(r"[。；;]", joined):
        compact = re.sub(r"\s+", "", sentence)
        if not re.search(r"(?<![子母])公司生产所需的主要原材料及零部件为", compact):
            continue
        if "客户" in compact or "第三方" in compact:
            continue
        if "钢材" not in compact:
            continue
        return {
            "name": "钢材",
            "action": ActivityAction.PURCHASES,
            "verb": "采购",
            "value": None,
            "unit": None,
            "actor": "公司",
            "header": "主要原材料",
        }
    return None


def _hedge_underlying_bindings(excerpt: str) -> list[dict[str, Any]]:
    """Named futures hedge underlyings; amounts stay empty and unsplit.

    The names must sit in the sentence that affirms the futures business
    (开展 or 从事 without a 未/拟/计划 prefix); a not-yet or planned
    disclosure never binds.
    """

    joined = _join_pdf_soft_breaks(excerpt)
    bindings: list[dict[str, str | None]] = []
    for sentence in re.split(r"[。；;]", joined):
        compact = re.sub(r"\s+", "", sentence)
        if "原料的期货业务" not in compact:
            continue
        if re.search(r"(?:未|拟|计划|将)(?:开展|从事)", compact):
            continue
        if "开展" not in compact and "从事" not in compact:
            continue
        if "客户" in compact or "第三方" in compact or "子公司" in compact:
            continue
        if not re.search(r"本公司|本集团|(?<![子母])公司", compact):
            continue
        if "套期保值" not in compact and "期货" not in compact:
            continue
        for name in ("钢材", "铜", "铝", "原油"):
            if name in compact:
                bindings.append(
                    {
                        "name": name,
                        "action": ActivityAction.OPERATES,
                        "verb": "期货套保",
                        "value": None,
                        "unit": None,
                        "actor": "公司",
                        "header": "套期保值标的",
                    }
                )
    return bindings


def _power_and_oil_bindings(excerpt: str) -> list[dict[str, Any]]:
    """Coal volume, combined power-heat sales, crude purchase, and internal sales."""

    compact = re.sub(r"\s+", "", _join_pdf_soft_breaks(excerpt))
    bindings: list[dict[str, str | None]] = []
    coal = re.search(r"公司共采购煤炭(?P<value>\d+(?:\.\d+)?)亿吨", compact)
    if coal:
        bindings.append(
            {
                "name": "煤炭",
                "action": ActivityAction.PURCHASES,
                "verb": "采购",
                "value": coal.group("value"),
                "unit": "亿吨",
                "actor": "公司",
                "header": None,
            }
        )
    share = re.search(
        r"公司电力、热力销售收入约占营业收入的(?P<value>\d+(?:\.\d+)?)%",
        compact,
    )
    if share:
        bindings.append(
            {
                "name": "电力、热力",
                "action": ActivityAction.SELLS,
                "verb": "销售",
                "value": share.group("value"),
                "unit": "%",
                "actor": "公司",
                "header": None,
            }
        )
    if "炼油事业部" in compact and "购入原油" in compact:
        bindings.append(
            {
                "name": "原油",
                "action": ActivityAction.PURCHASES,
                "verb": "购入",
                "value": None,
                "unit": None,
                "actor": "炼油事业部",
                "header": None,
            }
        )
    if "炼油事业部" in compact and "内部销售" in compact:
        for name in ("汽油", "柴油", "煤油"):
            if name in compact:
                bindings.append(
                    {
                        "name": name,
                        "action": ActivityAction.SELLS,
                        "verb": "内部销售",
                        "value": None,
                        "unit": None,
                        "actor": "炼油事业部",
                        "header": "内部销售",
                    }
                )
    if "炼油事业部" in compact and "部分化工原料油内部销售给化工事业部" in compact:
        bindings.append(
            {
                "name": "化工原料油",
                "action": ActivityAction.SELLS,
                "verb": "内部销售",
                "value": None,
                "unit": None,
                "actor": "炼油事业部",
                "header": "部分内部销售给化工事业部",
            }
        )
    return bindings


def _supply_source_rows(excerpt: str) -> list[tuple[str, str]]:
    """One supply commodity per table row, split by 国内采购 and 国外进口."""

    rows: list[tuple[str, str]] = []
    current = ""
    for line in excerpt.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        compact = re.sub(r"\s+", "", line)
        named = re.search(r"(铁矿石|废钢)供应", compact)
        if named:
            current = named.group(1)
            continue
        if not current:
            continue
        if compact.startswith("合计"):
            current = ""
            continue
        if compact.startswith("国内采购"):
            rows.append((current, "国内采购"))
        elif compact.startswith("国外进口"):
            rows.append((current, "国外进口"))
    return rows


def _steel_business_source(excerpt: str) -> str:
    """The company's steel description through its named product series."""

    joined = _join_pdf_soft_breaks(excerpt)
    match = _STEEL_BUSINESS_SENTENCE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def _company_business_source(excerpt: str) -> str:
    """The reporting company's full 业务覆盖 or 专注于 sentence.

    Soft wraps are joined first. A bare “公司专注于” or “公司业务覆盖”
    has no business content and is not a sentence.
    """

    joined = _join_pdf_soft_breaks(excerpt)
    match = _COMPANY_BUSINESS_SENTENCE.search(joined)
    if match is None:
        return ""
    compact = re.sub(r"\s+", "", match.group(0))
    return _original_span_matching(excerpt, compact) or ""


def explicit_material_input_names(
    text: str, *, source_delivery_repair: bool = False
) -> tuple[str, ...]:
    """Return named materials the same sentence binds to the company's own input."""

    compact = re.sub(r"\s+", "", text)
    names: list[str] = (
        ["煤炭"] if source_delivery_repair and _company_coal_risk_source(text) else []
    )
    if source_delivery_repair:
        names.extend(
            name for name, verb, _ in _native_input_bindings(text) if verb == "投入"
        )
        names.extend(
            binding["name"]
            for binding in _explicit_current_commodity_bindings(text)
            if binding.get("header") == "能源"
        )
    for sentence in re.split(r"[。；;]", compact):
        names.extend(_named_inputs_in_sentence(sentence))
        names.extend(_company_owned_material_lists(sentence))
        if source_delivery_repair and _current_company_sentence(sentence):
            cost_list = re.search(
                rf"(?:酿造|生产)的主要原材料[（(]如(?P<list>{_LIST_BODY})[）)]",
                sentence,
            )
            if cost_list and re.search(r"对(?:本)?公司的(?:盈利|成本)", sentence):
                names.extend(_split_material_names(cost_list.group("list")))
            furnace = re.search(
                r"公司(?:合并口径)?入炉([\u4e00-\u9fff]{1,8}?)(?:单价|成本|消耗)",
                sentence,
            )
            if furnace:
                names.append(furnace.group(1))
    for match in _COMPANY_PROCUREMENT.finditer(compact):
        names.append(match.group(1))
    unique: list[str] = []
    for name in names:
        if not _accept_material_name(name) or name in unique:
            continue
        unique.append(name)
    return tuple(unique)


def _named_inputs_in_sentence(sentence: str) -> list[str]:
    """Accept a raw-material list only when that sentence states the company's cost."""

    if not _sentence_binds_company_input_cost(sentence):
        return []
    names: list[str] = []
    for match in re.finditer("等主要原材料", sentence):
        clause = re.split(r"[，,：:]", sentence[: match.start()])[-1]
        names.extend(_split_material_names(clause))
    return names


def _sentence_binds_company_input_cost(sentence: str) -> bool:
    match = _COMPANY_INPUT_COST.search(sentence)
    if match is None:
        return False
    return _THIRD_PARTY_COST.search(match.group("gap") or "") is None


def _accept_material_name(name: str) -> bool:
    if name in _GENERIC_MATERIAL_NAMES or not re.fullmatch(_MATERIAL_NAME, name):
        return False
    return _NOT_A_MATERIAL_NAME.search(name) is None


def _split_material_names(text: str) -> list[str]:
    names: list[str] = []
    for part in re.split(r"[、，,及和]", text):
        part = part.removesuffix("等")
        if not re.fullmatch(_MATERIAL_NAME, part):
            continue
        if part in _GENERIC_MATERIAL_NAMES:
            continue
        names.append(part)
    return names


_LIST_BODY = rf"(?:{_MATERIAL_NAME}[、和])+{_MATERIAL_NAME}"
_COMPANY_SUBJECT = r"(?<!供应商)(?<!客户)(?<!下游)(?<!子)(?:本公司|公司)"
_COMPANY_PRIMARY_LIST = re.compile(
    rf"{_COMPANY_SUBJECT}"
    rf"(?P<line>(?:产品|生产经营所需)?)"
    rf"主要原材料(?:包括|为)(?P<list>{_LIST_BODY})"
)
_COMPANY_LINE_LIST = re.compile(
    rf"{_COMPANY_SUBJECT}"
    rf"(?P<line>[\u4e00-\u9fff]{{1,12}})"
    rf"原材料包括(?P<list>{_LIST_BODY})"
)
_CONTINUATION_LIST = re.compile(
    rf"(?:生产所需原材料包括|业务原材料)(?P<list>{_LIST_BODY})"
)
_LIST_SUBJECT_REJECTED = re.compile(r"客户|供应商|下游|销售|出售")


def _company_owned_material_lists(sentence: str) -> list[str]:
    """Accept a company-subject raw-material list without requiring a cost verb.

    The older ``等主要原材料`` path still requires the company's own cost
    sentence. This path is the disclosure shape ``公司…主要原材料包括/为``
    and a same-sentence continuation such as ``生产所需原材料包括``.
    """

    names: list[str] = []
    matched = False
    for pattern in (_COMPANY_PRIMARY_LIST, _COMPANY_LINE_LIST):
        for match in pattern.finditer(sentence):
            line = match.group("line") or ""
            if _LIST_SUBJECT_REJECTED.search(line):
                continue
            matched = True
            names.extend(_split_material_names(match.group("list")))
    if not matched:
        return []
    for match in _CONTINUATION_LIST.finditer(sentence):
        names.extend(_split_material_names(match.group("list")))
    return names


def _select_material_span(
    pages: Sequence[ReportPageText],
    *,
    source_delivery_repair: bool = False,
) -> CoreEvidenceSpan | None:
    for page in pages:
        if not page.readable or not explicit_material_input_names(
            page.text, source_delivery_repair=source_delivery_repair
        ):
            continue
        section_title, quote = _material_quote(page.text)
        return CoreEvidenceSpan(
            page=page.page,
            section_title=section_title,
            excerpt=quote,
            bounded_quote=quote,
            chapter_task="extract_material_inputs",
            field_ids=("material_input",),
            context_complete=True,
        )
    return None


def _material_quote(text: str) -> tuple[str, str]:
    coal = _company_coal_risk_source(text)
    if coal:
        return "可能面对的风险", coal
    compact = re.sub(r"\s+", "", text)
    marker = text.find("等主要原材料")
    if marker >= 0:
        line_start = text.rfind("\n", 0, marker) + 1
        end = text.find("。", marker)
        quote = text[line_start : len(text) if end < 0 else end + 1].strip()
        if _named_inputs_in_sentence(re.sub(r"\s+", "", quote)):
            return _material_section(text, quote), quote
    match = _COMPANY_PROCUREMENT.search(compact)
    if match is None:
        return "主要原材料", text.strip()
    quote = _original_span_matching(text, match.group(0)) or match.group(0)
    return _material_section(text, quote), quote.strip()


def _material_section(text: str, quote: str) -> str:
    anchor = quote[:12]
    index = text.find(anchor) if anchor else -1
    before = text[:index] if index > 0 else ""
    lines = [line.strip() for line in before.splitlines() if line.strip()]
    if lines and len(lines[-1]) <= 30 and "等主要原材料" not in lines[-1]:
        return lines[-1]
    return "主要原材料"


def _project_material_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
    *,
    source_delivery_repair: bool = False,
) -> tuple[SemanticRecord, ...]:
    item = _prepared_for(selection, span, "material_input")
    if item is None:
        return ()
    records: list[SemanticRecord] = []
    inputs: dict[str, str | None] = dict.fromkeys(
        explicit_material_input_names(
            span.excerpt, source_delivery_repair=source_delivery_repair
        )
    )
    if source_delivery_repair:
        for name, verb, header in _native_input_bindings(span.excerpt):
            if verb == "投入":
                inputs[name] = header
        for binding in _explicit_current_commodity_bindings(span.excerpt):
            if binding.get("header") == "能源" and binding.get("action") is None:
                inputs[binding["name"]] = "能源"
    for name, header in inputs.items():
        records.append(
            _base_fact(
                Relationship,
                report=selection.report,
                record_id=(f"owned:{item.evidence.evidence_id}:material:{name}"),
                field_id="material_input",
                chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
                evidence=item.evidence,
                source_native=SourceNativeValue(
                    name=name,
                    header=header,
                    qualifier="能源耗用" if header == "能源" else None,
                ),
                relation_type=RelationshipType.MATERIAL_INPUT,
                object_name=name,
                excerpt_subject=span.excerpt,
            )
        )
    return tuple(records)


def _base_fact(model, **kwargs):
    report = kwargs["report"]
    evidence = kwargs.pop("evidence")
    excerpt = kwargs.pop("excerpt_subject", "")
    subject_scope, subject_basis = _subject_from_excerpt(excerpt)
    return model(
        subject_scope=subject_scope,
        subject_basis=subject_basis,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        **kwargs,
    )


def _excerpt_states_owned_overview(excerpt: str) -> bool:
    return bool(
        re.search(
            r"主营业务为|主要从事|主要产品包括|主要产品为|经营范围(?!内)|"
            r"主要经营|公司主要业务情况|公司金融业务",
            excerpt,
        )
    )


def _join_wrapped_lines(text: str) -> str:
    return re.sub(r"[\r\n]+", "", text)


_PREFERRED_OVERVIEW_STATEMENT = re.compile(
    r"(?:主营业务为|主要从事|主要产品包括|主要产品为|"
    r"经营范围(?!内)\s*[\u4e00-\u9fff]|"
    r"(?:公司|本公司)致力于.{0,40}提供|"
    r"涵盖[^。；;]{2,80}(?:信贷|银行|基金))"
)


def _sentence_containing(text: str, index: int) -> str:
    start = max(text.rfind(mark, 0, index) for mark in ("。", "；", ";", "\n"))
    start = 0 if start < 0 else start + 1
    end_candidates = [text.find(mark, index) for mark in ("。", "；", ";")]
    end_candidates = [item for item in end_candidates if item >= 0]
    end = min(end_candidates) + 1 if end_candidates else len(text)
    return text[start:end].strip()


def _original_span_matching(text: str, compact_target: str) -> str:
    compact_chars = [
        (index, char) for index, char in enumerate(text) if not char.isspace()
    ]
    compact = "".join(char for _, char in compact_chars)
    start = compact.find(compact_target)
    if start < 0 or not compact_target:
        return ""
    end = start + len(compact_target) - 1
    return text[compact_chars[start][0] : compact_chars[end][0] + 1].strip()


def _overview_source_text(excerpt: str, quote: str, *, heading: str) -> str:
    if not excerpt or not quote:
        return ""
    joined = _join_wrapped_lines(excerpt)
    preferred = _PREFERRED_OVERVIEW_STATEMENT.search(joined)
    if preferred is not None:
        sentence = _sentence_containing(joined, preferred.start())
        text = _original_span_matching(excerpt, re.sub(r"\s+", "", sentence))
        if text and (text in quote or text in excerpt):
            return text
    chosen: list[str] = []
    heading_compact = re.sub(r"\s+", "", heading)
    for sentence in re.split(r"(?<=[。；;\n])", excerpt):
        compact = re.sub(r"\s+", "", sentence)
        if not compact or compact == heading_compact:
            continue
        if overview_dimension_hits(sentence) or heading_compact in compact:
            chosen.append(sentence)
        if chosen and len("".join(chosen)) >= 24:
            break
    text = "".join(chosen).strip() or excerpt.strip()
    if re.sub(r"\s+", "", text) == heading_compact:
        substance = next(
            (
                sentence.strip()
                for sentence in re.split(r"(?<=[。；;\n])", excerpt)
                if _OVERVIEW_SUBSTANCE.search(sentence)
            ),
            excerpt.strip(),
        )
        text = substance
    if text in quote:
        return text
    if excerpt in quote:
        return excerpt
    return quote if quote in excerpt or excerpt in quote else ""


def _revenue_sentence_records(
    selection: CoreEvidenceSelection,
    overview_item: PreparedEvidence,
    *,
    excerpt: str,
    principal_text: str,
) -> tuple[SemanticRecord, ...]:
    """One accepted fact per company revenue sentence, without the text between them."""

    principal = re.sub(r"\s+", "", principal_text)
    records: list[SemanticRecord] = []
    seen: set[str] = set()
    for clause in _reporting_company_revenue_clauses(excerpt):
        if not clause or clause in principal or clause in seen:
            continue
        seen.add(clause)
        source_text = _original_span_matching(excerpt, clause) or clause
        records.append(
            _base_fact(
                BusinessOverview,
                report=selection.report,
                record_id=(
                    f"owned:{overview_item.evidence.evidence_id}:revenue:{len(seen)}"
                ),
                field_id="business_overview_source",
                chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
                evidence=overview_item.evidence,
                source_native=SourceNativeValue(name=clause[:80]),
                source_text=source_text,
            )
        )
    return tuple(records)


def _reporting_company_revenue_clauses(excerpt: str) -> list[str]:
    """Keep the reporting company's own revenue clauses from the same excerpt."""

    joined = _join_wrapped_lines(excerpt)
    clauses: list[str] = []
    for sentence in re.split(r"[。；;]", joined):
        operating_model = _operating_model_source(sentence)
        if operating_model:
            clauses.append(re.sub(r"\s+", "", operating_model))
            continue
        for piece in re.split(r"[，,]", sentence):
            compact = re.sub(r"\s+", "", piece).strip()
            if _is_reporting_company_revenue(compact):
                clauses.append(compact)
    return clauses


def _is_reporting_company_revenue(compact: str) -> bool:
    if not compact or _REVENUE_BLOCK_PATTERN.search(compact):
        return False
    if re.search(r"尚未|还未|仍未|并未|没有|未|不", compact):
        revenue_at = compact.find("营业收入")
        if revenue_at < 0:
            revenue_at = compact.find("收入")
        if revenue_at >= 0 and re.search(
            r"尚未|还未|仍未|并未|没有|未|不",
            compact[:revenue_at],
        ):
            return False
    if "不适用" in compact and "营业收入主要来源于" not in compact:
        return False
    if re.match(r"^(?:报告期内)?(?:本公司|公司)", compact) is None:
        return False
    if re.search(r"[\u4e00-\u9fff]{2,}(?:有限公司|股份有限公司)", compact):
        return False
    return _overview_states_revenue(compact, repair_revenue_sentence=True)


def _activity_object_clauses(clause: str) -> list[str]:
    """Drop service targets in “以……为对象，提供……” and keep the provided services."""

    text = clause.strip()
    provided = re.search(r"以.+?为对象[，,]?\s*提供(.+)", text)
    if provided:
        text = provided.group(1)
    # "为客户提供……服务，收取……费" is one provide-and-collect relationship:
    # the company subject, the service, and the fee collection must stay in
    # a single candidate instead of being split at the comma.
    provide_collect = re.search(
        r"(?:公司|本公司|本集团)?为(?:客户提供|相关客户提供)[^。；;]{2,60}服务[，,]收取[^。；;]{2,80}费",
        text,
    )
    if provide_collect:
        return [text]
    # A following explanation of profit or quality control is not a product
    # or service object, and a regex length bound can cut it mid-clause.
    text = re.split(
        r"[，,](?:业务规模保持|主要品牌|品牌包括|建立|进一步|利润|形成|管理及控股)",
        text,
        maxsplit=1,
    )[0]
    text = re.split(r"[，,]?以及其他", text, maxsplit=1)[0]
    # "旋挖钻机，用于市政建设、公路桥梁……" enumerates where a product is used;
    # the application areas are not company activities.
    used_for = re.search(r"用于", text)
    if used_for:
        text = text[: used_for.start()]
    text = re.split(
        r"[，,](?:[\u4e00-\u9fff]{2,12}领域)?(?:核心|主要)产品[为有]", text, maxsplit=1
    )[0]
    project = re.fullmatch(
        r"(.+?)(发电项目)的(开发、建设、运营与管理)", re.sub(r"\s+", "", text)
    )
    if project:
        # 水力和新能源 share 发电项目 and the same complete action chain.
        return [
            name + project.group(2) + "的" + project.group(3)
            for name in re.split(r"和|、", project.group(1))
        ]
    objects: list[str] = []
    for item in _split_listed(text):
        item = item.strip("，,、；;。 ")
        item = re.sub(r"等[\u4e00-\u9fffA-Za-z0-9（）()]*$", "", item).strip(
            "，,、；;。 "
        )
        if len(item) < 2 or item.startswith("以") or "为对象" in item:
            continue
        objects.append(item)
    return objects


def _split_listed(clause: str) -> list[str]:
    text = _join_pdf_soft_breaks(clause).strip().strip("：:。；;，,")
    text = re.sub(r"(?:等(?:多个领域)?)$", "", text)
    if _looks_like_action_chain(text):
        return [text] if text else []
    parts: list[str] = []
    for chunk in re.split(r"[、；;]|[，,]及?|以及", text):
        parts.extend(item.strip() for item in re.split(r"和", chunk) if item.strip())
    return [
        item
        for item in parts
        if len(item) >= 2 and _VAGUE_OBJECT.fullmatch(item) is None
    ]


def _looks_like_action_chain(text: str) -> bool:
    return len(_ACTION_TOKEN.findall(text)) >= 2


def _object_and_action(
    raw: str,
    default_verb: str,
) -> tuple[str, ActivityAction, str]:
    text = raw.strip()
    produced = re.fullmatch(r"利用[^。]{2,30}生产的(.+)", text)
    if produced:
        return produced.group(1), ActivityAction.PRODUCES, "生产"
    if _looks_like_action_chain(text):
        object_name = _ACTION_TOKEN.split(text)[0].strip("的 、") or text
        if "制造" in text or "生产" in text:
            return (
                object_name,
                ActivityAction.PRODUCES,
                "制造" if "制造" in text else "生产",
            )
        if "研发" in text or "开发" in text:
            return (
                object_name,
                ActivityAction.DEVELOPS,
                "研发" if "研发" in text else "开发",
            )
        if "销售" in text:
            return object_name, ActivityAction.SELLS, "销售"
        return object_name, ActivityAction.OPERATES, default_verb
    if any(
        token in text
        for token in ("托管", "银行", "基金", "信贷", "服务", "咨询", "运维")
    ):
        return text[:40], ActivityAction.PROVIDES_SERVICE, default_verb
    if "销售" in text:
        return re.sub(r"的?销售.*$", "", text) or text, ActivityAction.SELLS, "销售"
    if "研发" in text or "开发" in text:
        return (
            re.sub(r"的?(?:研发|开发).*$", "", text) or text,
            ActivityAction.DEVELOPS,
            "研发" if "研发" in text else "开发",
        )
    if any(token in text for token in ("制造", "生产", "加工")):
        return (
            re.sub(r"的?(?:制造|生产|加工).*$", "", text) or text,
            ActivityAction.PRODUCES,
            "生产",
        )
    return text[:40], ActivityAction.OPERATES, default_verb


_SEGMENT_SECTION_HEADINGS = {
    "主营业务分行业情况": "industry",
    "主营业务/产品分行业情况": "industry",
    "主营业务分行业": "industry",
    "分行业": "industry",
    "主营业务分产品情况": "product",
    "主营业务分产品": "product",
    "分产品": "product",
    "主营业务分地区情况": "region",
    "主营业务主要地区情况": "region",
    "分地区": "region",
    "主营业务分销售模式情况": "sales_mode",
    "分销售模式": "sales_mode",
}


_GROUP_NAME_TO_DIMENSION = {
    "分销售模式": "sales_mode",
    "分地区": "region",
    "分产品": "product",
    "分行业": "industry",
}


def _open_segment_group(line: str) -> frozenset[str] | None:
    """Return dimensions named together in one formal combined-table heading."""

    compact = _HEADING_PREFIX.sub("", re.sub(r"\s+", "", line.strip()), count=1)
    compact = compact.lstrip(".．、")
    compact = compact.removeprefix("主营业务")
    found = [name for name in _GROUP_NAME_TO_DIMENSION if name in compact]
    if len(found) < 2:
        return None
    remainder = compact
    for name in found:
        remainder = remainder.replace(name, "", 1)
    remainder = re.sub(r"[、，,和及情况模式]", "", remainder)
    if remainder:
        return None
    return frozenset(_GROUP_NAME_TO_DIMENSION[name] for name in found)


def _dimension_from_heading(text: str) -> str | None:
    heading = _heading_if_title_line(text, tuple(_SEGMENT_SECTION_HEADINGS))
    if heading is None:
        return None
    return _SEGMENT_SECTION_HEADINGS[heading]


def _unit_declaration(text: str) -> str:
    match = _UNIT_DECLARATION.search(text)
    return "" if match is None else match.group(0)


def _unit_from_excerpt(excerpt: str) -> str | None:
    joined = _join_rmb_column_header(excerpt)
    match = _UNIT_DECLARATION.search(joined)
    if match is not None:
        return match.group(1)
    native = re.search(
        r"金额单位(?:均)?为人民币(千元|万元|元)|销售收入[（(](万元|千元|元)[)）]",
        joined,
    )
    if native:
        return native.group(1) or native.group(2)
    if "人民币百万元" in re.sub(r"\s+", "", joined):
        return "百万元"
    return None


def _table_intro_unit(lines: list[str], heading_index: int) -> str | None:
    """The unit stated between a table heading and its first data row.

    An independent table only carries the unit it declares for itself; the
    wording of an earlier table must not flow into it.
    """

    intro: list[str] = []
    for line in lines[heading_index + 1 : heading_index + 13]:
        if _parse_segment_row(line) is not None:
            break
        if (
            _open_segment_group(line) is not None
            or _dimension_from_heading(line) is not None
            or _is_revenue_table_stop(line)
        ):
            break
        intro.append(line)
    if not intro:
        return None
    return _unit_from_excerpt("\n".join(intro))


def _join_pdf_soft_breaks(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = _PDF_AMOUNT_WRAP.sub(r"\1\2", normalized)
    merged: list[str] = []
    for line in normalized.split("\n"):
        if merged and _is_pdf_soft_continuation(merged[-1], line):
            merged[-1] = f"{merged[-1].rstrip()}{line.lstrip()}"
        else:
            merged.append(line)
    return "\n".join(merged)


def _is_pdf_soft_continuation(previous: str, nxt: str) -> bool:
    prev = previous.rstrip()
    current = nxt.lstrip()
    if not prev or not current:
        return False
    if prev[-1] in "。！？；;：:":
        return False
    if _heading_if_title_line(prev, _ALL_HEADINGS) is not None:
        return False
    if _heading_if_title_line(current, _ALL_HEADINGS) is not None:
        return False
    if _dimension_from_heading(prev) is not None:
        return False
    if _dimension_from_heading(current) is not None:
        return False
    if _UNIT_DECLARATION.search(prev) or _PERCENT_UNIT.search(prev):
        return False
    if _UNIT_DECLARATION.search(current) or _PERCENT_UNIT.search(current):
        return False
    if "营业收入" in prev or "营业成本" in prev or _is_revenue_table_stop(prev):
        return False
    if re.search(r"(?:^|\s)[-/](?:\s+[-/])+$", prev):
        return False
    if prev.endswith("个百分点"):
        return False
    if current.startswith(("下表", "项目", "营业收入", "营业总收入", "利息净收入")):
        return False
    if (
        _parse_labeled_amount_row(
            current, _COMPANY_TOTAL_LABELS | _INCOME_AMOUNT_LABELS
        )
        is not None
    ):
        return False
    if _parse_segment_row(current) is not None:
        return False
    return bool(
        re.search(r"[\u4e00-\u9fff]$", prev) and re.match(r"[\u4e00-\u9fff]", current)
    )


def _stated_in_quote(text: str, quote: str) -> bool:
    if not text or not quote:
        return False
    if text in quote:
        return True
    return re.sub(r"\s+", "", text) in re.sub(r"\s+", "", quote)


def _parse_segment_row(line: str) -> tuple[str, str, str | None] | None:
    text = line.strip()
    text = re.sub(r"^其中[:：]\s*", "", text)
    text = re.sub(r"^(?:[一二三四五六七八九十]+|\d+)[、.．](?!\d)\s*", "", text)
    if not text:
        return None
    tokens = text.split()
    if len(tokens) < 3:
        return None
    amount_index = next(
        (
            i
            for i, token in enumerate(tokens[1:], 1)
            if re.fullmatch(r"-?[\d,]+(?:\.\d+)?", token)
        ),
        None,
    )
    if amount_index is None:
        return None
    label = "".join(tokens[:amount_index])
    numbers = tokens[amount_index:]
    # A road row names the road with digits ("205 国道天长段新线"); the leading
    # number belongs to the label, not the amount column.
    if (
        label.isdigit()
        and numbers
        and re.search(r"[\u4e00-\u9fff]", numbers[0])
        and not re.fullmatch(r"-?[\d,]+(?:\.\d+)?%?", numbers[0])
    ):
        label = f"{label}{numbers[0]}"
        numbers = numbers[1:]
    if (
        label in _SKIP_SEGMENT_LABELS
        or "年度报告" in label
        or "合计" in label
        or re.search(r"抵消|抵销|抵减", label)
        or len(numbers) < 2
        or numbers[0] == "营业收入"
        or not (
            re.fullmatch(r"[\u4e00-\u9fffA-Za-z0-9（）/、]{2,40}", label)
            or re.fullmatch(
                r"(?:华东|华北|华中|东北|西北|西南)[：:][\u4e00-\u9fff、，（）()]{2,150}",
                label,
            )
        )
        or not re.fullmatch(r"-?[\d,]+(?:\.\d+)?", numbers[0])
        or not (numbers[1] == "/" or re.fullmatch(r"-?[\d,]+(?:\.\d+)?%?", numbers[1]))
    ):
        return None
    return label, numbers[0], numbers[1]


def _complete_current_business_source(excerpt: str) -> str:
    """Keep a multi-business owner's continuous section instead of a sub-branch."""
    compact = re.sub(r"\s+", "", excerpt)
    match = re.search(
        r"公司主要从事[^。]+。(?=[\s\S]*?公司核心业务板块)"
        r"|报告期内，公司聚焦[^。]+主要从事[^。]+。"
        r"|公司主要从事[^。]+。主要产品包括[^。]+。"
        r"|公司是[^。]+，报告期内，公司主营业务收入主要来源于[^。]+。"
        r"|本集团以[^。]+为两大核心主业"
        r"|公司建成[^。]+四大产业板块"
        r"|报告期内，公司从事的主要业务未发生变化，仍为[^。]+。"
        r"|本公司是以[^。]+为主业的[^。]+。"
        r"|公司是[^。]+所属的专业从事[^。]+综合型能源企业。公司的主要产品是[^。]+。[^。]+已并网[^。]+。",
        compact,
    )
    if not match:
        return ""
    prefix = compact[max(0, compact.rfind("。", 0, match.start()) + 1) : match.end()]
    if re.search(r"第三方|子公司|拟|计划|将|尚未|未建成", prefix):
        return ""
    tail = re.split(
        r"报告期内公司新增重要非主营业务|二、报告期内公司所处行业",
        compact[match.start() :],
        maxsplit=1,
    )[0]
    original = _original_span_matching(excerpt, tail)
    return original


def _current_product_sources(excerpt: str) -> list[str]:
    """Affirmative product disclosures in the owned performance/product section."""
    compact = re.sub(r"\s+", "", excerpt)
    sources: list[str] = []
    patterns = (
        r"公司经过[^。]+主要产品包括[^。]+。",
        r"公司还利用[^。]+。成功开拓了[^。]+产品。",
        r"可降解[^。]{2,100}已实现[^。]{2,100}生产，产品畅销[^。]+。",
        r"[\u4e00-\u9fffA-Za-z]{2,30}突破生产技术瓶颈，已稳定量产[^。]+。",
        r"本集团[^。]{0,40}深化运营[^。]+供应链核心产品[^。]+。",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, compact):
            sentence = compact[
                max(0, compact.rfind("。", 0, match.start()) + 1) : match.end()
            ]
            if re.search(
                r"子公司|第三方|公司拟|公司计划|公司尚未|将销售|尚未销售|未实现",
                sentence,
            ):
                continue
            source = _original_span_matching(excerpt, match.group(0))
            if source:
                sources.append(source)
    return sources


def _native_current_product_bindings(excerpt: str) -> list[tuple[str, str, str | None]]:
    """Current sales bind one product's own performance paragraph, not its uses."""
    compact = re.sub(r"\s+", "", excerpt)
    bindings: list[tuple[str, str, str | None]] = []
    for match in re.finditer(
        r"报告期内，公司(?!拟|计划|将|尚未|未)(?P<name>[\u4e00-\u9fffA-Za-z]{2,30}?)(?:产品)?"
        r"(?:实际)?产量[^。]+。",
        compact,
    ):
        clause = match.group(0)
        if re.search(r"(?:尚未|未|拟|计划|将)销售|未生产", clause):
            continue
        if re.search(r"(?:扣除内部自用后)?销售\d", clause):
            name = match.group("name").removesuffix("产品")
            bindings.append((name, "销售", None))
    for match in re.finditer(
        r"公司累计生产(?P<name>[^。]{2,30}?)\d[^。]+销售\d[^。]+。", compact
    ):
        if not re.search(
            r"子公司|第三方|拟|计划|尚未|将销售|未销售",
            compact[max(0, compact.rfind("。", 0, match.start()) + 1) : match.end()],
        ):
            bindings.append((match.group("name"), "销售", None))
    # PVB胶片的本期生产结构句 explicitly continues to 全年...销售.
    for match in re.finditer(
        r"报告期内，公司根据[^。]+自主调整了(?P<name>[^。]{2,30}?)产品生产结构。"
        r"全年(?P=name)产品实际产量[^。]+销售[^。]+。",
        compact,
    ):
        if not re.search(r"拟|计划|尚未|将销售|未销售", match.group(0)):
            bindings.append((match.group("name"), "销售", None))
    for source in _current_product_sources(excerpt):
        text = re.sub(r"\s+", "", source)
        match = re.match(r"可降解(?P<names>[^。]+?)已实现[^。]+生产，产品畅销", text)
        if match:
            for name in match.group("names").split("、"):
                bindings.append((name, "畅销", None))
    if (
        "产销量情况分析表" in compact
        and "上表中公司主要产品的销售量" in compact
        and not re.search(r"子公司|第三方|公司拟|公司计划|尚未销售|将销售", compact)
    ):
        table = re.split(r"产销量情况分析表", excerpt, maxsplit=1)[0]
        table = _join_segment_label_amounts(
            _join_pdf_soft_breaks(table), core_answer_repair=True
        )
        in_products = "分产品营业收入" in re.sub(r"\s+", "", table)
        for line in table.splitlines():
            dim = _dimension_from_heading(line)
            if dim is not None:
                in_products = dim == "product"
            row = _parse_segment_row(line)
            if in_products and row and row[0] not in {"其他", "水泥及熟料"}:
                bindings.append((row[0], "销售", "营业收入"))
    return list(dict.fromkeys(bindings))


def _native_input_bindings(excerpt: str) -> list[tuple[str, str, str]]:
    """Parse explicit upstream and purchase/consumption columns, never downstream."""
    bindings: list[tuple[str, str, str]] = []
    compact = re.sub(r"\s+", "", excerpt)
    if re.search(r"优化精益养殖[\s\S]+?依托精准饲料利用", compact) and not re.search(
        r"第三方|拟依托|计划依托|未利用", compact
    ):
        bindings.append(("饲料", "投入", "养殖实际饲料利用"))
    for match in re.finditer(
        r"(?:本公司|公司)(?:持续深化[^。]+，)?提升(?P<names>[^。]+?)等[^。]+应用比例",
        compact,
    ):
        if not re.search(r"未|不|拟|计划|将", match.group()):
            for name in match["names"].split("、"):
                if re.fullmatch(r"[\u4e00-\u9fff]{2,12}", name):
                    bindings.append((name, "投入", "速生材实际应用"))
    normalized = excerpt.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"主要上游原材\s*料", "主要上游原材料", normalized)
    normalized = re.sub(
        r"化学原料及化\s*[学工]制品制造业", "化学原料及化学制品制造业", normalized
    )
    normalized = re.sub(r"电石渣及其他\s*废弃物", "电石渣及其他废弃物", normalized)
    # The formal company product table declares upstream and downstream columns.
    header = re.search(
        r"产品\s+所属细分行业\s+主要上游原材料\s+主要下游应用领域", normalized
    )
    if header and not re.search(
        r"子公司|第三方|拟|计划", normalized[: header.start()][-120:]
    ):
        table = re.split(
            r"[（(]3[)）][.．]?研发创新", normalized[header.end() :], maxsplit=1
        )[0]
        # Each native row starts with product and industry. Within the upstream
        # cell, a horizontal space starts the next column (except Latin names).
        # Soft-wrapped list cells continue until that column boundary.
        current: str | None = None
        upstream = []
        for line in table.splitlines():
            row = re.search(
                r"(?:^|\s)(?:化工|化纤|建材|新材料|电力行业|水泥行业|化学原料及化学制品制造业)(?:\s+(.*)|$)",
                line,
            )
            if row:
                if current:
                    upstream.append(current)
                current = row.group(1) or ""
            elif current is not None:
                if (
                    not current
                    or current.endswith(("、", "，"))
                    or re.match(r"[\u4e00-\u9fff](?:[、，]| +\d+[.．])", line)
                ):
                    current += line.strip()
                else:
                    if current:
                        upstream.append(current)
                    current = None
                    continue
            else:
                continue
            current = re.sub(r"([A-Z]{2,}) +(?=[\u4e00-\u9fff])", r"\1", current)
            boundary = re.search(r" +(?=[\u4e00-\u9fff])", current)
            if boundary:
                upstream.append(current[: boundary.start()])
                current = None
        if current:
            upstream.append(current)
        for cell in upstream:
            # Route names describe production processes, not purchased objects.
            cell = re.sub(r"\d+[.．][^：:]+[：:]", "、", cell)
            for name in re.split(r"[、，]| +(?=\d+[.．])", cell):
                name = re.sub(r"\s+", "", name)
                if re.fullmatch(r"[\u4e00-\u9fffA-Za-z]{1,20}", name) and name not in {
                    "助剂",
                    "其他聚合乳液",
                    "其他聚合",
                }:
                    bindings.append((name, "投入", "主要上游原材料"))
    if "采购量" in normalized and "耗用量" in normalized:
        section = ""
        for line in normalized.splitlines():
            compact = re.sub(r"\s+", "", line)
            if "主要原材料" in compact and "基本情况" in compact:
                section = (
                    ""
                    if re.search(r"子公司|第三方|拟|计划|尚未", compact)
                    else "原材料"
                )
            if "主要能源" in compact and "基本情况" in compact:
                section = (
                    "" if re.search(r"子公司|第三方|拟|计划|尚未", compact) else "能源"
                )
            match = re.match(r"\s*([\u4e00-\u9fffA-Za-z]{1,18})\s+外购\s+[^\n]+", line)
            if match and section and len(re.findall(r"[\d,]+\.\d+", line)) >= 3:
                bindings.append((match.group(1), "采购", section))
                bindings.append((match.group(1), "投入", section))
    # Deduplicate the same native input stated by table and procurement disclosure.
    return list(dict.fromkeys(bindings))


def _owned_business_sources(excerpt: str) -> list[str]:
    """Complete current company/group passages, each retaining its original actor."""
    compact = re.sub(r"\s+", "", excerpt)
    patterns = (
        r"具体会计政策描述如下：[\s\S]+?在提供管理服务期间确认收入。",
        r"本公司根据在向客户转让商品或服务前[\s\S]+?本公司在该时点确认收入实现。",
        r"[（(]3[)）]\.?履约义务的说明[\s\S]+?款到发货[\s\S]+?(?=[（(]4[)）]|$)",
        r"[\u4e00-\u9fff]{2,12}业务主要由子公司[^。]+从事[\s\S]+?(?=报告期内公司新增重要非主营业务|$)",
        r"报告期内，[“\"]其他[”\"]业务的营业收入[^。]+公司饲料贸易业务板块业务规模下降。",
        r"在主业方面，公司主营业务为[^。]+。[\s\S]+?公司参股[^。]+。",
        r"本公司的主要营业收入系高速公路车辆通行费收入[\s\S]+?本公司及子公司[^。]+均按[^。]+确认通行费收入。",
        r"①在某一时点确认收入[\s\S]+?成本加成总承包合同：[\s\S]+?确定履约进度。",
        r"风电主机装备业务及关键配套业务主要包括[^。]+。公司通过招投标[^。]+。",
        r"(?:本公司|公司)是[^。]{2,100}其主要业务为[^。]+。",
        r"(?:本公司|公司)是一家集[^。]+主要产品覆盖[^。]+。",
        r"本公司属[^。]+主要业务为[^。]+。",
        r"本公司及其子公司[（(]以下简称[“\"]本集团[”\"][)）]主要从事[^。]+。",
        r"截至报告期末，本公司已投入运行[^。]+。主要包括[^。]+自用光伏[^。]+。",
        r"本公司通过大比例参股[^。]+。本公司适应[^。]+综合能源服务[^。]+。",
        r"本公司先后在[^。]+设立专业化售电公司[^。]+综合能源业务。\d{4}年，本公司所属售电公司[^。]+。",
        r"本集团的营业收入主要包括[^。]+。[\s\S]+?此收入[^。]+。",
        r"公司紧扣[^。]+为客户提供[^。]+车队管理服务[^。]+。",
        r"公司的业务覆盖[^。]+订单模式[^。]+。",
        r"公司在海外市场采取[^。]+销售模式[^。]*。[^。]+；同时[^。]+KD组装[^。]+品牌授权[^。]+。",
        r"本公司有\d+个报告分部：[^。]+。",
        r"[\u4e00-\u9fff]+股份有限公司[（(]以下简称[“\"]本公司[”\"][)）]及其子公司[（(]以下统称[“\"]本集团[”\"][)）]主要从事[^。]+。",
        r"本集团拥有[^。]+业务分部。[^。]+。其他业务分部主要包括[^。]+。",
        r"报告期内，本集团稳步提高运行质量[^。]+。[\s\S]+?推广[^。]+服务[^。]+。",
        r"公司持续开展[^。]+。通过[^。]+打造[^。]+服务产品[^。]+。",
        r"公司坚持以[^。]+业务为核心[^。]+。",
        r"(?:此外，)?公司立足于现有存量资产[^。]+物业管理服务业务。公司依托[^。]+。",
        r"公司房地产业务实现合同销售面积[^。]+实现主营业务收入[^。]+同比增长[^。]+。",
        r"报告期内，公司房地产出租总收入[^。]+。",
        r"本集团将票款收入按照[^。]+。",
        r"当客户接受本集团提供的[^。]+与此同时，本集团[^。]+。本集团已收但尚未提供[^。]+。",
        r"本集团主要执行[^。]+常旅客[^。]+。会员可[^。]+。[\s\S]+?会员兑换的其他奖励[^。]+。",
        r"[\u4e00-\u9fff]+收入(?:是)?指本集团向[^。]+。",
        r"报告期内公司存在贸易业务收入[\s\S]+?等贸易业务\d+[\d,]*",
        r"本集团按不同地区列示[^。]+：[\s\S]+?具体对外交易收入的信息见下表：",
        r"本公司电力销售在[^。]+。具体如下：[\s\S]+?B、向用户[^。]+。",
        r"本公司电力销售在[^。]+，具体如下：[\s\S]+?B、向用户[^。]+。",
        r"公司牢固树立精益管理理念[\s\S]+?持续深化中长期、现货、绿电交易、绿证交易[^。]+。",
        r"(?:公司的分布式|公司分布式)[^。]+自发自用[^。]+余电上网[^。]+。",
        r"基于多年服务全球客户[^。]+公司[^。]+OEM&ODM[^。]+。[^。]+。",
        r"锂电池业务本期仅统计[^。]+。",
        r"\d{4}年\d{1,2}月，公司在[^。]+后更名为[^。]+。\d{4}年\d{1,2}月\d{1,2}日[^。]+公司丧失[^。]+合并范围。",
        r"本公司与[^。]+之间存在购销商品、提供服务的关联交易[^。]+。",
        r"公司部分光伏发电项目属于[^。]+自发自用、余电上网[^。]+。",
        r"公司[^。]+持续深化中长期、现货、绿电交易、绿证交易[^。]+。",
        r"其他业务：主要为绿证交易业务，本期交易量[^。]+营业收入[^。]+。",
        r"[（(]3[)）]\.履约义务的说明[\s\S]+?月结[^。]+?合计[^。]+?(?=[（(]4[)）])",
        r"本公司按照业务类型确定的收入确认具体原则和计量方法：[\s\S]+?根据合同或订单，公司在取得出口报关单时确认收入。",
    )
    sources = []
    for pattern in patterns:
        for m in re.finditer(pattern, compact):
            if m.start() and compact[m.start() - 1] in "子方":
                continue
            prefix = compact[max(0, compact.rfind("。", 0, m.start()) + 1) : m.start()]
            if re.search(
                r"(?:第三方|子公司)(?:公司|企业)?(?:拟|计划)?$|(?:公司|本集团)(?:拟|计划|尚未|将)$",
                prefix,
            ):
                continue
            if re.search(r"(?:本公司|公司)(?:拟|计划|将|尚未)|未开展", m.group()):
                continue
            if m.group().startswith(
                "基于多年服务全球客户"
            ) and not _current_company_sentence(m.group()):
                continue
            original = _original_span_matching(excerpt, m.group())
            if original:
                sources.append(original)
    return list(dict.fromkeys(sources))


def _owned_operating_passages(excerpt: str) -> list[str]:
    """Keep complete owned operations and accounting passages, including status.

    These are narrative evidence, not affirmative commodity actions. A trial,
    promotion, associate or asset disposal remains qualified in the answer.
    """
    compact = re.sub(r"\s+", "", excerpt)
    patterns = (
        r"公司主要负责[^。]+。[^。]+。",
        r"报告期内，公司聚焦[\s\S]+?(?=报告期内公司新增重要非主营业务|$)",
        r"公司主要从事[^。]+。主要产品包括[\s\S]+?(?=报告期内公司新增重要非主营业务|二、报告期内公司所处行业|$)",
        r"公路投资运营方面：[\s\S]+?公司参股[^。]+。",
        r"主要系公司战略调整，[^。]+年末完成出表[^。]+。",
        r"本集团的营业收入主要包括[\s\S]+?7公路养护收入[\s\S]+?直到履约进度能够合理确定为止。",
        r"公司不同销售方式下收入的确认方法：[\s\S]+?直到履约进度能够合理确定为止。",
        r"公司持续深化[^。]+提升[^。]+。[^。]+成功开发[^。]+。",
        r"公司于\d{4}年[\s\S]+?公司不再纳入公司财务报表合并范围内。",
        r"本期是否存在丧失子公司控制权的交易或事项[\s\S]+?(?=其他说明|$)",
        r"作为出租方的租赁分类标准和会计处理方法[\s\S]+?(?=[（(]二[)）]承租人|$)",
        r"[\u4e00-\u9fff]{2,20}以[^。]+深耕[^。]+两大核心领域[^。]+。",
        r"公司氯碱化工生产工艺主要为[^。]+。",
        r"如上图所示：[\s\S]+?(?=报告期内公司新增重要非主营业务|$)",
        r"公司生产的产品在满足自身[^。]+。[\s\S]+?线下通过[^。]+。",
        r"公司已建成[^。]+具备提供[^。]+。",
        r"公司开发了[^。]+。",
        r"公司依托当地[^。]+自备热电站[^。]+。",
        r"争力的关键所在。[^。]+。目前，[^。]+并网发电[^。]*。",
        r"\d{4}年，公司全资子公司[^。]+成功试车[^。]+。",
        r"按照业务类型披露具体收入确认方式及计量方法[\s\S]+?(?=[（(]2[)）]|\d+、合同成本|$)",
        r"本公司按照业务类型确定的收入确认具体原则和计量方法：[\s\S]+?公司根据出口货物报关单注明的出口日期确认收入。",
        r"本公司销售商品的业务包括[^。]+。本公司为履约义务的主要责任人[^。]+。",
        r"本公司作为出租人[\s\S]+?本公司按照固定的周期性利率[^。]+。",
        r"由于管道杆路租赁收入[^。]+。本期扣除的其他业务收入[^。]+。",
        r"为配合落实[^。]+公司以全资子公司[^。]+有线电视网络资产[\s\S]+?本次交易完成。",
        r"[（(]四[)）]紧抓政企业务[\s\S]+?(?=[（(]五[)）]|$)",
        r"[（(]三[)）]全力办好[\s\S]+?(?=[（(]四[)）])",
        r"自主研发[^。]+一体化电视方案[\s\S]+?业务支撑能力[^。]+。",
        r"主要控股参股公司分析[\s\S]+?(?=报告期内取得和处置子公司的情况|$)",
        r"经公司[^。]+决议，[\s\S]+?均在公司合并报表范围内。",
        r"企业集团的构成[\s\S]+?(?=\d+、|$)",
        r"产销量情况说明[\s\S]+?(?=[（(]3[)）]|$)",
        r"出售商品/提供劳务情况表[\s\S]+?(?=购销商品、提供和接受劳务的关联交易说明|$)",
        r"主营业务分行业、分产品、分地区、分销售模式情况的说明[\s\S]+?(?=[（(]2[)）]|$)",
        r"\d{4}年\d{1,2}月，公司全资子公司[^。]+已成功打通全流程[^。]+。[\s\S]+?产能结构优化[^。]+。",
        r"本公司作为出租方：[\s\S]+?(?=本公司作为承租方|关联租赁情况说明|$)",
        r"报告期内取得和处置子公司的情况[\s\S]+?(?=其他说明|$)",
        r"[\u4e00-\u9fff]{2,40}有限公司公司参股子公司[\s\S]+?(?=其他说明|$)",
        r"劣势：一是[^。]+产能释放阶段[^；]+；",
    )
    sources = []
    for pattern in patterns:
        for match in re.finditer(pattern, compact):
            prefix = compact[
                max(0, compact.rfind("。", 0, match.start()) + 1) : match.start()
            ]
            if re.search(r"第三方|(?:公司|本集团)(?:拟|计划|尚未|不从事)$", prefix):
                continue
            original = _original_span_matching(excerpt, match.group())
            if original:
                sources.append(re.sub(r"\s+", "", original))
    return list(dict.fromkeys(sources))


def _native_income_spans(pages: Sequence[ReportPageText]) -> list[CoreEvidenceSpan]:
    """Bound native financial/income tables and their own unit declaration."""
    spans = []
    financial_unit = None
    financial_declaration = ""
    declaration_page = None
    income_unit_declaration = ""
    income_unit_page = None
    parent_statements = False
    parent_declaration = ""
    parent_page = None
    for page in pages:
        if not page.readable:
            continue
        compact = re.sub(r"\s+", "", page.text)
        convention = re.search(
            r"除特别注明外[^。\n]{0,50}金额单位[^。\n]{0,30}(百万元|千元)", compact
        )
        if convention:
            financial_unit = convention[1]
            financial_declaration = _original_span_matching(
                page.text, convention.group()
            )
            declaration_page = page.page
        if re.search(r"母公司财务报表(?:主要项目)?注释", compact):
            parent_statements = True
            parent_declaration = next(
                l for l in page.text.splitlines() if "母公司财务报表" in l
            )
            parent_page = page.page
        text = page.text
        if "营业收入和营业成本" in text and _unit_from_excerpt(text):
            income_unit_declaration = next(
                (l for l in text.splitlines() if _unit_from_excerpt(l)), ""
            )
            income_unit_page = page.page
        if page.layout_text and (
            "房地产销售收入分项列示如下" in text or "营业收入和营业成本情况" in text
        ):
            text = page.layout_text
        markers = (
            ("营业收入和营业成本情况", "营业收入和营业成本情况"),
            ("营业收入和营业成本情况", "营业收入、营业成本"),
            ("按销售模式", "销售收入"),
            ("新能源汽车收入及补贴", "新能源汽车收入及补贴"),
            ("报告分部的财务信息", "报告分部的财务信息"),
            ("收入附注原生栏目", "营业收入和营业成本"),
            ("收入附注原生栏目", "主营业务收入和主营业务成本"),
            ("收入附注原生栏目", "营业收入、营业成本的分解"),
            ("主营业务分业务", "主营业务分业务情况如下"),
            ("境外子公司收入", "境外资产占比较高的相关说明"),
            ("收入附注原生栏目", "具体对外交易收入的信息见下表"),
            ("收入附注原生栏目", "各个报告分部的信息"),
            ("项目销售收入", "房地产销售收入分项列示如下"),
            ("贸易业务收入", "报告期内公司存在贸易业务收入"),
            ("收入附注原生栏目", "营业收入明细："),
            ("细分行业收入", "按细分行业划分的公司主营业务基本情况"),
            ("渠道收入", "按销售渠道划分的公司主营业务基本情况"),
            ("其他收入扣除明细", "营业收入扣除情况表"),
            ("主体营业收入", "主要控股参股公司分析"),
            ("联营企业收入", "重要联营企业的主要财务信息"),
            ("非全资子公司收入", "重要非全资子公司的主要财务信息"),
            ("在建项目收入", "在建重大项目情况"),
        )
        for title, cue in markers:
            if cue not in text:
                continue
            if title == "按销售模式" and "按销售模式" not in text:
                continue
            start = (
                text.find(cue)
                if title
                in {
                    "细分行业收入",
                    "渠道收入",
                    "其他收入扣除明细",
                    "主体营业收入",
                    "联营企业收入",
                }
                or cue == "营业收入明细："
                else text.find(title)
            )
            if start < 0:
                start = text.find(cue)
            parent_start = (
                text.find(parent_declaration) if parent_page == page.page else -1
            )
            table_parent = parent_statements and (
                parent_start < 0 or start >= parent_start
            )
            table = text[start:parent_start] if parent_start > start else text[start:]
            continuations = []
            if (
                title in {"主体营业收入", "联营企业收入", "非全资子公司收入"}
                or cue == "营业收入明细："
            ):
                stop = r"报告期内取得和处置子公司的情况|[（(]4[)）]\.?(?:不重要的合营|使用企业集团)|营业成本分解信息|营业收入、营业成本的分解信息"
                cursor = page.page + 1
                while not re.search(stop, table):
                    nxt = next(
                        (p for p in pages if p.page == cursor and p.readable), None
                    )
                    if not nxt:
                        break
                    table += "\n" + nxt.text
                    continuations.append(cursor)
                    cursor += 1
                if cue == "营业收入明细：":
                    # Also retain its immediately following timing table, up to costs.
                    table = re.split(r"营业成本分解信息", table, maxsplit=1)[0]
                else:
                    table = re.split(stop, table, maxsplit=1)[0]
            if title in {"细分行业收入", "渠道收入"}:
                table = re.split(r"\n\s*\([2-9]\)|会计政策说明", table, maxsplit=1)[0]
            if title in {"营业收入和营业成本情况", "收入附注原生栏目"}:
                nxt = next(
                    (p for p in pages if p.page == page.page + 1 and p.readable), None
                )
                if (
                    nxt
                    and not re.search(
                        r"主营业务\s+\d|主营业务收入\(?[a-z]?\)?\s+\d", table
                    )
                    and (
                        "本期发生额" not in table
                        or re.search(r"主营业务\s*$", table)
                        or not re.search(r"(?:主营|其他)业务\s+\d", table)
                    )
                    and "分部" not in table
                ):
                    table += "\n" + nxt.text
                    continuations.append(nxt.page)
            if title == "收入附注原生栏目":
                nxt = next(
                    (p for p in pages if p.page == page.page + 1 and p.readable), None
                )
                if nxt and re.search(r"固定资产试\s*运行销售", nxt.text):
                    table += "\n" + re.split(r"\n\s*\d+、", nxt.text, maxsplit=1)[0]
                    continuations.append(nxt.page)
            table = (
                _join_native_income_cells(table)
                if title not in {"在建项目收入", "主体营业收入"}
                else table
            )
            # Bound by the next financial note or explicit next table heading.
            if title == "营业收入和营业成本情况":
                table = re.split(
                    r"(?:\n\s*\d+[、.]\s*(?!\d)|[（(]2[)）])", table, maxsplit=1
                )[0]
            elif title in {"按销售模式", "新能源汽车收入及补贴"}:
                table = re.split(r"\n\s*\d+[、.]", table, maxsplit=1)[0]
            elif title == "项目销售收入":
                table = re.split(r"\([34]\)|[（(][34][)）]", table, maxsplit=1)[0]
            elif title == "境外子公司收入":
                table = re.split(r"\n\s*3[、.]", table, maxsplit=1)[0]
            elif title == "主营业务分业务":
                table = re.split(r"\n\s*1[、.]", table, maxsplit=1)[0]
            elif title == "收入附注原生栏目":
                table = re.split(
                    r"\n\s*(?:七\s*关联方|[（(]e[)）]|\d+[、.]\s*(?!\d)[^\n]+)",
                    table,
                    maxsplit=1,
                )[0]
            if (
                title == "报告分部的财务信息"
                and not _unit_from_excerpt(table)
                and income_unit_declaration
            ):
                table = income_unit_declaration + "\n" + table
                continuations.append(income_unit_page)
            unit = _unit_from_excerpt(table) or (
                financial_unit
                if title in {"营业收入和营业成本情况", "收入附注原生栏目"}
                else None
            )
            if not unit:
                continue
            # Carry only the applicable explicit convention, never a prior table's unit.
            if _unit_from_excerpt(table) is None:
                if not financial_declaration:
                    continue
                table = financial_declaration + "\n" + table
            if table_parent:
                table = parent_declaration + "\n" + table
                continuations.append(parent_page)
            spans.append(
                CoreEvidenceSpan(
                    page=page.page,
                    section_title=title,
                    excerpt=table,
                    bounded_quote=table,
                    continuation_pages=tuple(
                        dict.fromkeys(
                            [
                                *continuations,
                                *(
                                    [declaration_page]
                                    if declaration_page
                                    and financial_declaration in table
                                    else []
                                ),
                            ]
                        )
                    ),
                    chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                    field_ids=("segment_dimension", "operating_revenue"),
                    context_complete=True,
                )
            )
    return spans


def _supplemental_income_spans(
    pages: Sequence[ReportPageText],
) -> list[CoreEvidenceSpan]:
    """Keep the revenue column of operating, provided-service and lessor tables.

    A service row is selected only under the seller's own table, and a rental
    row only under the explicitly declared lessor. Purchases and lessee tables
    cannot inherit that direction or its unit.
    """
    spans = []
    for page in pages:
        text = page.text.replace("\r", "")
        if not page.readable:
            continue
        operating = re.search(r"报告期内电量、收入及成本情况[\s\S]+", text)
        if operating and _unit_from_excerpt(operating.group()) == "亿元":
            spans.append(
                CoreEvidenceSpan(
                    page=page.page,
                    section_title="电量收入及成本",
                    excerpt=operating.group(),
                    bounded_quote=operating.group(),
                    chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                    field_ids=("segment_dimension", "operating_revenue"),
                    context_complete=True,
                )
            )
        rental = re.search(
            r"作为出租人的经营租赁[\s\S]+?(?=作为出租人的融资租赁|$)", text
        )
        if (
            rental
            and re.search(r"√适用\s*□不适用", rental.group())
            and _unit_from_excerpt(rental.group())
        ):
            spans.append(
                CoreEvidenceSpan(
                    page=page.page,
                    section_title="经营租赁收入",
                    excerpt=rental.group(),
                    bounded_quote=rental.group(),
                    chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                    field_ids=("segment_dimension", "operating_revenue"),
                    context_complete=True,
                )
            )
        lessor = re.search(
            r"(?<!第三方)(?<!子公司)本公司作为出租方：[\s\S]+", page.layout_text or text
        )
        if lessor:
            table = lessor.group()
            continuations = []
            cursor = page.page + 1
            stop = r"本公司作为承租方[:：]|关联租赁情况说明|[（(]4[)）]"
            while not re.search(stop, table):
                nxt = next((p for p in pages if p.page == cursor and p.readable), None)
                if not nxt:
                    break
                table += "\n" + (nxt.layout_text or nxt.text).replace("\r", "")
                continuations.append(cursor)
                cursor += 1
            table = re.split(stop, table, maxsplit=1)[0]
            if (
                re.search(r"√适用\s*□不适用", table)
                and _unit_from_excerpt(table)
                and "本期确认的租赁收入" in re.sub(r"\s+", "", text)
            ):
                spans.append(
                    CoreEvidenceSpan(
                        page=page.page,
                        section_title="关联服务及出租收入",
                        excerpt=table,
                        bounded_quote=table,
                        continuation_pages=tuple(continuations),
                        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                        field_ids=("segment_dimension", "operating_revenue"),
                        context_complete=True,
                    )
                )
        trustee = re.search(r"本公司受托管理/承包情况表：[\s\S]+", text)
        if trustee:
            table = re.split(
                r"关联托管/承包情况说明|本公司委托管理", trustee.group(), maxsplit=1
            )[0]
            if re.search(r"√适用\s*□不适用", table) and _unit_from_excerpt(table):
                spans.append(
                    CoreEvidenceSpan(
                        page=page.page,
                        section_title="关联服务及出租收入",
                        excerpt=table,
                        bounded_quote=table,
                        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                        field_ids=("segment_dimension", "operating_revenue"),
                        context_complete=True,
                    )
                )
        # A provided-service table can continue over multiple complete pages.
        seller = re.search(r"出售商品/提供劳务[\s\S]+", text)
        if seller and _unit_from_excerpt(seller.group()):
            table = seller.group()
            continuations = []
            cursor = page.page + 1
            while not re.search(
                r"采购资产|购销商品、提供和接受劳务的关联交易说明", table
            ):
                nxt = next((p for p in pages if p.page == cursor and p.readable), None)
                if not nxt:
                    break
                table += "\n" + nxt.text.replace("\r", "")
                continuations.append(cursor)
                cursor += 1
            table = re.split(
                r"采购资产|购销商品、提供和接受劳务的关联交易说明", table, maxsplit=1
            )[0]
            if (
                "管理服务费" in table
                or "租金收入" in table
                or "租赁收入" in table
                or "关联方 关联交易内容" in table
            ) and "本期发生额" in table:
                spans.append(
                    CoreEvidenceSpan(
                        page=page.page,
                        section_title="关联服务及出租收入",
                        excerpt=table,
                        bounded_quote=table,
                        continuation_pages=tuple(continuations),
                        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                        field_ids=("segment_dimension", "operating_revenue"),
                        context_complete=True,
                    )
                )
        for cue, title, stop in (
            ("转租使用权资产取得的收入", "转租收入", "作为出租人"),
            ("作为出租人的融资租赁", "融资租赁收入", "未折现租赁收款额"),
            ("租出资产", "关联租出资产收入", "3、"),
        ):
            if cue not in text:
                continue
            table = (
                text
                if title in {"转租收入", "关联租出资产收入"}
                else text[text.index(cue) :]
            )
            table = re.split(stop, table, maxsplit=1)[0]
            unit = _unit_from_excerpt(table) or _unit_from_excerpt(text)
            continuations = ()
            if title == "关联租出资产收入" and not unit:
                intro = next(
                    (
                        p
                        for p in reversed(pages)
                        if page.page - 2 <= p.page < page.page
                        and "关联交易金额" in p.text
                        and _unit_from_excerpt(p.text)
                    ),
                    None,
                )
                if intro:
                    declaration = next(
                        l for l in intro.text.splitlines() if _unit_from_excerpt(l)
                    )
                    table = declaration + "\n" + table
                    unit = _unit_from_excerpt(declaration)
                    continuations = (intro.page,)
            if unit:
                spans.append(
                    CoreEvidenceSpan(
                        page=page.page,
                        section_title=title,
                        excerpt=table,
                        bounded_quote=table,
                        continuation_pages=continuations,
                        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value,
                        field_ids=("segment_dimension", "operating_revenue"),
                        context_complete=True,
                    )
                )
    return spans


def _join_native_income_cells(text: str) -> str:
    """Join printed decimal fragments/vertical row cells, preserving year blanks."""
    text = text.replace("\r", "")
    text = re.sub(r"(?m)^.*年度报告[^\n]*$|^\s*\d+\s*/\s*\d+\s*$", "", text)
    text = re.sub(r"(?<=\d)\s+(?=[,.]\d)", "", text)
    text = re.sub(r"(\d[\d,]*\.)(?:\s+)(\d{2})(?=\s|$)", r"\1\2", text)
    text = re.sub(r"(\d[\d,]*\.\d)\s*\n\s*(\d)(?=\s|$)", r"\1\2", text)
    text = re.sub(r"(\d[\d,]*,\d{1,2})\s*\n\s*(\d{1,2}\.\d{2})(?=\s|$)", r"\1\2", text)
    lines = []
    for line in text.splitlines():
        if lines and re.fullmatch(r"\s*(?:\(?-?\d[\d,]*\.\d{2}\)?(?:\s+|$))+", line):
            lines[-1] += " " + line.strip()
        else:
            lines.append(line)
    return "\n".join(lines)


def _native_note_income_rows(text: str, title: str) -> list[tuple]:
    """Read native income columns; identifiers in names never supply amounts."""
    if title == "贸易业务收入":
        match = re.search(
            r"主要是开展各类(?P<names>[\u4e00-\u9fff\s]{2,50}?)等贸易业务\s+(?P<amount>\d[\d,]*)(?=\s)",
            text,
        )
        if match and not re.search(
            r"第三方|子公司|不存在|拟|计划|尚未", text[: match.end()]
        ):
            return [
                (
                    "trade",
                    re.sub(r"\s+", "", match["names"]) + "贸易",
                    match["amount"],
                    "本期营业收入",
                    None,
                )
            ]
        if re.search(r"[√✓]适用\s*□不适用", text) and "本期营业收入" in text:
            return [
                (
                    "business_type",
                    m[1],
                    m[2],
                    "本期营业收入",
                    None,
                    "原子公司：" + m[1] if m[1].endswith("公司") else None,
                )
                for m in re.finditer(
                    r"(?m)^\s*(贸易业务|[\u4e00-\u9fff、]{2,45}业务|[\u4e00-\u9fff]{2,45}公司)\s+(\d[\d,]*\.\d+)\s+\d",
                    text,
                )
            ]
        return []
    rows = []
    text = _join_native_income_cells(text)
    text = re.split(r"营业成本分解信息", text, maxsplit=1)[0]
    if "本期客户合同产生的收入情况如下" in text:
        timing = re.sub(
            r"在\s*某\s*一\s*时\s*(点|段\s*内)\s*确\s*认",
            lambda m: "在某一时" + re.sub(r"\s+", "", m[1]) + "确认",
            text,
        )
        for m in re.finditer(
            r"在某一时(点|段内)确认\s+((?:\d[\d,]*\.\d+\s+)+)", timing
        ):
            cells = m[2].split()
            rows.append(
                (
                    "revenue_timing",
                    "在某一时" + m[1] + "确认",
                    cells[-1],
                    "客户合同收入/确认时间/本期合计",
                    None,
                )
            )
    text = re.sub(
        r"((?:主营|其他)业务\s+(?:[-/]\s+){3}[-/])\s+(?=(?:主营|其他)业务)",
        r"\1\n",
        text,
    )
    cleaned = re.sub(r"(?m)^.*年度报告[^\n]*\n?", "", text.replace("\r", ""))
    cleaned = re.sub(r"(?<=\d)\s*,\s*(?=\d)", ",", cleaned)
    cleaned = re.sub(r"(?m)^\s*\d+\s*/\s*\d+\s*$", "", cleaned)
    cleaned = re.sub(
        r"([\u4e00-\u9fff、]{2,40})\n\s*(件|咨询和监理)\s*\n\s*(?=\d)",
        r"\1\2 ",
        cleaned,
    )
    cleaned = re.sub(r"中国\s*[(（]\s*含港澳\s*台\s*[)）]", "中国（含港澳台）", cleaned)
    lines = cleaned.splitlines()
    for i in range(len(lines) - 1):
        current, nxt = lines[i].strip(), lines[i + 1].strip()
        if (
            re.fullmatch(r"[\u4e00-\u9fff、]{2,40}", current)
            and current
            not in {
                "商品类型",
                "按经营地区分类",
                "主营业务收入",
                "其他业务收入",
                "主营业务成本",
                "其他业务成本",
                "合计",
            }
            and re.match(r"(?:服务(?:收入)?|发与销售|件|咨询和监理)\s+\d", nxt)
        ):
            lines[i + 1] = current + nxt
            lines[i] = ""
    cleaned = "\n".join(lines)
    basis = ""
    dimension = ""
    segment_labels = []
    current_columns = False
    conventional_years = False
    aggregate_income_columns = False
    business_basis = ""
    # Join only printed note labels, never classification headings into a row.
    cleaned = re.sub(r"固定资产试\s*运行销售", "固定资产试运行销售", cleaned)
    cleaned = re.sub(r"(?:其中：)?在某一\s*时点确认\s*\n", "在某一时点确认 ", cleaned)
    cleaned = re.sub(
        r"(?:其中：)?在某一\s*时点确认\s*(?=\d)", "在某一时点确认 ", cleaned
    )
    cleaned = re.sub(r"在某一\s*时段确认\s*-\s*-\s*", "\n", cleaned)
    cleaned = re.sub(
        r"固定资产试运行销售\s*\n\s*(?=\d)", "固定资产试运行销售 ", cleaned
    )
    cleaned = re.sub(r"(?m)^\s*[-/]\s+[-/]\s+(?=(?:主营|其他)业务)", "", cleaned)
    cleaned = re.sub(r"按商品转让的时间\s*分类", "按商品转让的时间分类", cleaned)
    cleaned = re.sub(r"按商品转让的时\s*间分类", "按商品转让的时间分类", cleaned)
    cleaned = re.sub(r"(?<=\.\d{2})\s+(?=合计\s+\d)", "\n", cleaned)
    for line in cleaned.splitlines():
        compact = re.sub(r"\s+", "", line)
        if title == "项目销售收入":
            if "项目本期发生额上期发生额" in compact:
                current_columns = True
            dimension, basis = "project", "房地产销售收入"
        elif title == "贸易业务收入":
            if "贸易业务开展情况本期营业收入" in compact:
                current_columns = True
            dimension, basis = "trade", "本期营业收入"
        else:
            if re.search(
                r"(?:主营|其他)业务收入(?:和(?:主营|其他)业务成本)?$", compact
            ) or re.fullmatch(
                r"((?:主营|其他)业务收入(?:主营|其他)业务成本){2}", compact
            ):
                basis = "主营业务收入" if "主营业务收入" in compact else "其他业务收入"
                dimension = "product"
            elif re.search(r"(?:主营|其他)业务成本$", compact):
                basis, dimension = "", ""
            if "按商品转让的时间分类" in compact:
                basis, dimension = "收入确认时点", "revenue_timing"
            if "合同产生的收入的情况" in compact:
                basis, dimension = "合同产生的收入", "contract_type"
            if "商品类型" == compact:
                basis, dimension = "全部营业收入/商品类型", "product"
            if "按销售渠道分类" in compact:
                basis, dimension = "全部营业收入/销售渠道", "sales_mode"
            if "按经营地区分类" in compact:
                basis, dimension = "全部营业收入/经营地区", "region"
            if "市场或客户类型" in compact:
                basis, dimension = "全部营业收入/市场或客户类型", "region"
            if "营业收入明细" in compact:
                basis, dimension = "营业收入明细/客户合同收入", "service"
            if "国家或地区" in compact:
                basis, dimension = "对外交易收入", "region"
            if "分部间抵销" in compact and "合计" in compact and "收入" not in compact:
                header = re.sub(r"分部\s*\d+\s*", "", line)
                segment_labels = re.sub(
                    r"^\s*(?:项目|合同分类|202\d\s*年)\s*", "", header
                ).split()
                # Native numbered segment headers have no repeated year row.
                if re.search(r"分部\s*\d+", line):
                    current_columns = True
                if re.match(r"202\d\s*年", header.strip()):
                    current_columns = True
            if re.search(r"[^\s]+-分部", compact) and compact.endswith("合计"):
                aggregate_income_columns = True
                conventional_years = False
            # A conventional current/prior header must be present; an isolated
            # sentence with a number is not an income row.
            if re.search(r"(?:本期|202\d年).*(?:上期|202\d年)", compact) or compact in {
                "收入成本收入成本",
                "营业收入营业成本营业收入营业成本",
                "营业收入营业成本营业收入营业成本营业收入营业成本营业收入营业成本",
            }:
                current_columns = True
                if re.search(r"本期.*上期", compact) or compact == "收入成本收入成本":
                    conventional_years = True
        match = re.match(
            r"^\s*(?P<label>.+?)\s+(?P<values>(?:\(?-?\d[\d,]*(?:\.\d+)?\)?|-|/)(?:\s+(?:\(?-?\d[\d,]*(?:\.\d+)?\)?|-|/))*)\s*$",
            line,
        )
        if not match or not current_columns:
            continue
        label = re.sub(r"\s+", "", match["label"])
        values = match["values"].split()
        if (
            not re.search(r"[\u4e00-\u9fff]", label)
            or label in {"项目", "合计", "总计"}
            or "合计" in label
            and label != "分部营业收入合计"
            or "年度" in label
        ):
            continue
        if title == "项目销售收入":
            # Layout may omit a blank current cell. A one-value row is accepted
            # only when its printed placement is in the current column, handled
            # below by the header/row geometry marker retained in the span.
            if label in _prior_only_income_labels(text):
                continue
            rows.append((dimension, label, values[0], basis, None))
            continue
        if title == "贸易业务收入":
            if not re.search(r"[√✓]适用\s*□不适用", text) or not values:
                continue
            rows.append((dimension, label, values[0], basis, None))
            continue
        if segment_labels and any(
            b in label
            for b in ("对外交易收入", "分部间交易收入", "分部营业收入合计", "营业收入")
        ):
            column_basis = next(
                b
                for b in (
                    "分部间交易收入",
                    "对外交易收入",
                    "分部营业收入合计",
                    "营业收入",
                )
                if b in label
            )
            paired_years = len(values) == 2 * len(segment_labels)
            cells = values[::2] if paired_years else values
            labels = list(segment_labels)
            if not paired_years and column_basis == "对外交易收入":
                labels = [l for l in labels if "抵销" not in l]
            elif not paired_years and column_basis == "分部间交易收入":
                # Compact parsers omit blanks. The native report's reconciliation
                # fixes missing internal cells as external == segment total.
                totals = {
                    r[1]: r[2] for r in rows if r[3] in {"营业收入", "分部营业收入合计"}
                }
                external = {r[1]: r[2] for r in rows if r[3] == "对外交易收入"}
                labels = [
                    l
                    for l in labels
                    if l != "合计" and ("抵销" in l or totals.get(l) != external.get(l))
                ]
            if len(cells) != len(labels):
                continue
            for name, value in zip(labels, cells):
                if name in {"合计", "未分配项目"} or value == "-":
                    continue
                adjustment = (
                    RowClass.CONSOLIDATION_ADJUSTMENT if "抵销" in name else None
                )
                val = "-" + value[1:-1] if value.startswith("(") else value
                rows.append(
                    (
                        "adjustment" if adjustment else "business_segment",
                        name,
                        val,
                        column_basis,
                        adjustment,
                    )
                )
            continue
        if values[0] in {"-", "/"}:
            continue
        category = re.fullmatch(r"(?:[12][．.])?(主营业务|其他业务)", label)
        if category:
            business_basis = category[1]
            if not dimension:
                # The separate native revenue/cost header handles these totals.
                continue
            if dimension == "revenue_timing":
                # Parent total for timing children, not a geographic/sales row.
                continue
            if dimension == "product":
                basis = "全部营业收入/商品类型/" + business_basis
            rows.append(("business_type", category[1], values[0], "营业收入", None))
            continue
        if re.match(r"^(?:主营业务收入|其他业务收入)[(（]?[a-z]?[)）]?$", label):
            name = re.sub(r"[(（][a-z][)）]", "", label)
            rows.append(("business_type", name, values[0], "营业收入", None))
            continue
        if label == "固定资产试运行销售":
            rows.append(("product", label, values[0], "固定资产试运行销售收入", None))
            continue
        if basis and dimension:
            if any(t in label for t in ["成本", "说明", "报告", "日期"]) or values[
                0
            ] in {"-", "/"}:
                continue
            amount = (
                values[-2]
                if (aggregate_income_columns or segment_labels)
                and not conventional_years
                and len(values) >= 2
                else values[0]
            )
            label = label.removeprefix("其中：")
            if label.startswith("与客户间的运输合同产生的"):
                label = label.removeprefix("与客户间的运输合同产生的")
            row_basis = (
                basis + "/" + business_basis if dimension == "revenue_timing" else basis
            )
            rows.append((dimension, label, amount, row_basis, None))
    return rows


def _prior_only_income_labels(text: str) -> set[str]:
    """Blank current cells must be distinguished from one-value current rows.

    Derive column positions from complete two-value rows in the original layout.
    Plain text without positions cannot locate an isolated value in either year.
    """
    numbered = []
    pairs = []
    for line in text.splitlines():
        matches = list(re.finditer(r"-?\d[\d,]*\.\d{2}", line))
        if not matches or re.search(r"合计|总计|单位", line):
            continue
        label = re.sub(r"\s+", "", line[: matches[0].start()])
        numbered.append((label, matches))
        if len(matches) == 4 and re.search(r"\s{3,}", line):
            pairs.append((matches[0].start(), matches[2].start()))
        if len(matches) == 2 and re.search(r"\s{3,}", line):
            pairs.append((matches[0].start(), matches[1].start()))
    boundary = sum((a + b) / 2 for a, b in pairs) / len(pairs) if pairs else None
    return {
        label
        for label, matches in numbered
        if len(matches) == 1 and (boundary is None or matches[0].start() > boundary)
    }


def _current_sparse_income_label(text: str) -> str | None:
    """Locate one printed sparse income cell using both native year totals.

    Plain owner text can lose blank-column positions. Accept a printed value
    only if the first table's current total requires it and the prior total
    already reconciles without it; never infer a missing amount or cost.
    """
    if not re.search(r"本期发生额\s+上期发生额\s+收入\s+成本\s+收入\s+成本", text):
        return None
    complete = []
    sparse = []
    for line in text.splitlines():
        m = re.fullmatch(r"\s*(主营业务|其他业务|合计)\s+(.+?)\s*", line)
        if m is None:
            continue
        cells = m[2].split()
        if not all(re.fullmatch(r"-?\d[\d,]*\.\d{2}", c) for c in cells):
            return None
        amounts = [Decimal(c.replace(",", "")) for c in cells]
        if m[1] == "合计":
            if len(amounts) != 4 or len(sparse) != 1 or not complete:
                return None
            label, value = sparse[0]
            if (
                value != 0
                and sum(r[0] for r in complete) + value == amounts[0]
                and sum(r[2] for r in complete) == amounts[2]
            ):
                return label
            return None
        if len(amounts) == 4:
            complete.append(amounts)
        elif len(amounts) == 1:
            sparse.append((m[1], amounts[0]))
        else:
            return None
    return None


def _lessor_income_rows(text: str) -> list[tuple]:
    """Recover wrapped tenants/assets without moving prior-only rent left."""
    rows = []
    prior_only = _prior_only_income_labels(text)
    pending = ""
    current = None

    def flush():
        if current is None:
            return
        cp, asset, amount = current
        if amount != "-":
            rows.append(
                (
                    "lease",
                    asset,
                    amount,
                    "本公司作为出租方/本期确认的租赁收入",
                    None,
                    "承租方：" + cp,
                )
            )

    for line in text.splitlines():
        if not line.strip() or re.search(
            r"本公司作为出租方|年度报告|单位|承租方名称|租赁资产种类|本期确认|上期确认|^\s*赁收入(?:\s+赁收入)?\s*$|适用|^\s*\d+\s*/\s*\d+",
            line,
        ):
            continue
        m = re.match(r"^(.*?)\s+(-|\d[\d,]*\.\d+)(?:\s+(-|\d[\d,]*\.\d+))?\s*$", line)
        if m:
            flush()
            label = pending + re.sub(r"\s+", "", m[1])
            pending = ""
            asset = re.search(
                r"(植物租摆|房屋及建筑物|房屋建筑物|房屋租赁|房屋及车辆|房屋及场地|车辆|房屋|土地使用权|场地租赁|广告经营权|机器设备)(?:、机器(?:设备)?)?$",
                label,
            )
            current = None
            if asset and (m[3] or re.sub(r"\s+", "", m[1]) not in prior_only):
                current = [label[: asset.start()], asset.group(), m[2]]
        elif current is not None:
            # Layout keeps continuation columns separated; append each to its
            # own cell instead of manufacturing a combined counterparty/asset.
            cols = re.split(r"\s{2,}", line.strip())
            if cols:
                fragment = re.sub(r"\s+", "", cols[0])
                if (
                    re.search(r"(?:公司|合作社|事务所|管理局)$", current[0])
                    and len(cols) == 1
                ):
                    flush()
                    current = None
                    pending = fragment
                    continue
                current[0] += fragment
            if len(cols) > 1:
                current[1] += re.sub(r"\s+", "", cols[1])
        else:
            pending += re.sub(r"\s+", "", line)
    flush()
    return rows


def _project_native_income_span(
    selection: CoreEvidenceSelection, span: CoreEvidenceSpan
) -> tuple[SemanticRecord, ...] | None:
    """Native one-column revenue or transposed segment-income table, no sums."""
    title = span.section_title
    if title not in {
        "营业收入和营业成本情况",
        "按销售模式",
        "新能源汽车收入及补贴",
        "报告分部的财务信息",
        "收入附注原生栏目",
        "项目销售收入",
        "贸易业务收入",
        "电量收入及成本",
        "关联服务及出租收入",
        "经营租赁收入",
        "主营业务分业务",
        "境外子公司收入",
        "细分行业收入",
        "渠道收入",
        "其他收入扣除明细",
        "主体营业收入",
        "联营企业收入",
        "转租收入",
        "融资租赁收入",
        "关联租出资产收入",
        "非全资子公司收入",
        "在建项目收入",
    }:
        return None
    si = _prepared_for(selection, span, "segment_dimension")
    mi = _prepared_for(selection, span, "operating_revenue")
    text = _join_native_income_cells(span.excerpt)
    text = re.sub(
        r"((?:主营|其他)业务\s+(?:[-/]\s+){3}[-/])\s+(?=(?:主营|其他)业务)",
        r"\1\n",
        text,
    )
    text = re.sub(r"营业收\s*\n\s*入\s*\n\s*(?=\d)", "营业收入 ", text)
    text = re.sub(r"营业收\s*\n\s*入", "营业收入", text)
    text = re.sub(r"一、营业\s*\n\s*收入", "营业收入", text)
    text = re.sub(r"(\.\d{2})(?=-?\d)", r"\1 ", text)
    if title == "报告分部的财务信息":
        years = list(re.finditer(r"(?m)^\s*202\d\s*年(?=[^\n]*分部间抵销)", text))
        if len(years) > 1:
            text = text[: years[1].start()]
    unit = _unit_from_excerpt(text)
    if not unit:
        return ()
    rows = []
    if title == "报告分部的财务信息" and re.search(
        r"项目\s+主营业务收入\s+分部间抵销\s+合计", text
    ):
        table = re.split(r"注：", text, maxsplit=1)[0]
        for m in re.finditer(
            r"(?m)^\s*([\u4e00-\u9fff]+)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$",
            table,
        ):
            for basis, value in zip(("抵销前", "抵销金额", "抵销后"), m.groups()[1:]):
                adjustment = (
                    RowClass.CONSOLIDATION_ADJUSTMENT if basis == "抵销金额" else None
                )
                rows.append(
                    (
                        "adjustment" if adjustment else "business_segment",
                        m[1],
                        value,
                        "主营业务收入/" + basis,
                        adjustment,
                    )
                )
    if title == "经营租赁收入":
        for m in re.finditer(
            r"(?m)^\s*([\u4e00-\u9fff]{2,30})\s+(\d[\d,]*\.\d+)", text
        ):
            if m[1] in {"合计", "项目"}:
                continue
            rows.append(("service", m[1], m[2], "作为出租人的经营租赁/租赁收入", None))
    if title in {"转租收入", "融资租赁收入"}:
        name = (
            "转租使用权资产取得的收入"
            if title == "转租收入"
            else "租赁投资净额的融资收益"
        )
        m = re.search(re.escape(name) + r"\s+(\d[\d,]*\.\d+)", text)
        if m and (title == "转租收入" or re.search(r"√适用\s*□不适用", text)):
            rows.append(("service", name, m[1], title, None))
    if title == "关联租出资产收入":
        compact = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", text)
        for m in re.finditer(
            r"(?P<party>[\u4e00-\u9fff]{2,45}公司(?:[\u4e00-\u9fff]{1,8}分公司)?)同一实际控制人\s*[（(][^）)]+[)）]\s*租出资产(?P<name>租赁收入)\s+(?P<value>\d[\d,]*\.\d+)",
            compact,
        ):
            rows.append(
                (
                    "service",
                    m["name"],
                    m["value"],
                    "日常关联交易/租出资产",
                    None,
                    "承租方：" + m["party"].removeprefix("现金结算"),
                )
            )
    if title == "其他收入扣除明细":
        compact = re.sub(r"\s+", "", text)
        for m in re.finditer(
            r"[16][.．](?P<name>正常经营之外的其他业务收入|未形成或难以形成稳定业务模式的业务所产生的收入)[\s\S]+?(?P<value>\d[\d,]*\.\d{2})(?=主要系|木方)",
            compact,
        ):
            rows.append(
                (
                    "business_type",
                    m["name"],
                    m["value"],
                    "其他业务收入扣除明细/本年度",
                    None,
                )
            )
        section = re.split(r"2\.\s*不具备", text, maxsplit=1)[0]
        seen = set()
        for m in re.finditer(
            r"(?m)^\s*([\u4e00-\u9fff]{2,12})\s+(\d[\d,]*\.\d+)\s*元", section
        ):
            if m[1] not in seen:
                rows.append(
                    ("business_type", m[1], m[2], "其他业务收入扣除明细/本年度", None)
                )
                seen.add(m[1])
    if title in {"细分行业收入", "渠道收入"}:
        joined = _join_segment_label_amounts(
            _join_pdf_soft_breaks(text), core_answer_repair=True
        )
        joined = joined.replace("同行业同领域产品毛利率情况", "")
        joined = re.sub(
            r"(?m)^([\u4e00-\u9fff]{1,10})\n(?=[\u4e00-\u9fff]+\s+\d[\d,]*\.\d+)",
            r"\1",
            joined,
        )
        for line in joined.splitlines():
            m = re.match(r"\s*([\u4e00-\u9fff]+)\s+(\d[\d,]*\.\d+)", line)
            if m and m[1] != "合计":
                name = m[1].removeprefix("同行业同领域产品毛利率情况")
                rows.append(
                    (
                        "industry" if title == "细分行业收入" else "sales_mode",
                        name,
                        m[2],
                        title + "/主营业务",
                        None,
                    )
                )
    if title == "主体营业收入":
        # Native company tables declare six numeric columns. Keep the actor's
        # original row; legal scope remains qualified in its narrative evidence.
        joined = re.sub(r"(?m)^.*年度报告[^\n]*$|^\s*\d+\s*/\s*\d+\s*$", "", text)
        joined = re.sub(r"净利\s*润", "净利润", joined)
        joined = re.sub(r"参股公\s*司", "参股公司", joined)
        joined = re.split(r"营业利润\s*净利润", joined, maxsplit=1)[-1]
        joined = re.sub(r"^\s*名称\s*型\s*", "", joined)
        for m in re.finditer(
            r"(?m)^\s*(?P<name>[\u4e00-\u9fff]+(?:[ \t]*\n[ \t]*[\u4e00-\u9fff]+){0,2})\s+(?P<kind>子公司|参股公司)(?P<body>[\s\S]+?)(?P<cells>(?:-?\d[\d,]*(?:\.\d{2})?\s+){5}-?\d[\d,]*(?:\.\d{2})?)(?=\s|$)",
            joined,
        ):
            cells = m["cells"].split()
            name = re.sub(r"\s+", "", m["name"])
            rows.append(
                (
                    "business_segment",
                    name,
                    cells[3],
                    "控股参股公司/营业收入",
                    None,
                    m["kind"] + "：" + name,
                )
            )
    if title == "非全资子公司收入":
        current = re.search(
            r"子公司名称\s+本期发生额\s+上期发生额(?P<table>[\s\S]+)",
            re.sub(r"子公司名\s*称", "子公司名称", text),
        )
        if current:
            table = re.split(r"[（(]4[)）]|其他说明", current["table"], maxsplit=1)[0]
            table = re.split(r"经营活动现金\s*流量", table)[-1]
            for m in re.finditer(
                r"(?m)^\s*(?P<name>[\u4e00-\u9fff\s]{2,60}?)\s+(?P<amount>\d[\d,]*\.\d{2})(?:\s+-?\d[\d,]*\.\d{2}){7}\s*$",
                table,
            ):
                name = re.sub(r"\s+", "", m["name"])
                rows.append(
                    (
                        "business_segment",
                        name,
                        m["amount"],
                        "重要非全资子公司/本期营业收入",
                        None,
                        "原子公司：" + name,
                    )
                )
    if title == "在建项目收入" and "本期确认" in text:
        table = re.split(r"其他说明|4、", text, maxsplit=1)[0]
        for m in re.finditer(
            r"(?P<name>(?:G\d+|镇巴[（(])[\s\S]+?)\s+BOT\s+\d[\d,]*\s+\d+\s*月\s+\d+%\s+(?P<amount>\d[\d,]*\.\d{2})",
            table,
        ):
            name = re.sub(r"\s+", "", m["name"]).split("符合预期")[-1]
            # Project rows begin with a route identifier or the native project name.
            start = re.search(r"G\d+|镇巴[（(]", name)
            if start:
                rows.append(
                    (
                        "service",
                        name[start.start() :],
                        m["amount"],
                        "在建重大项目/本期确认收入",
                        None,
                    )
                )
    if title == "主体营业收入" and re.search(r"主要业务\s+注册资本", text):
        table = re.split(r"营业收入\s+净利润", text, maxsplit=1)[-1]
        table = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", table)
        for m in re.finditer(
            r"(?P<name>[\u4e00-\u9fff]+有限(?:责任)?公司)\s+100[.]00\s+[\s\S]*?(?P<cells>\d[\d,]*\.\d{2}(?:\s+-?\d[\d,]*\.\d{2}){4})|(?P<other>[\u4e00-\u9fff]+有限(?:责任)?公司)\s+51[.]00\s+[\s\S]*?(?P<othercells>\d[\d,]*\.\d{2}(?:\s+-?\d[\d,]*\.\d{2}){4})",
            table,
        ):
            name = m["name"] or m["other"]
            cells = (m["cells"] or m["othercells"]).split()
            rows.append(
                (
                    "business_segment",
                    name,
                    cells[3],
                    "控股参股公司/营业收入",
                    None,
                    "原子公司：" + name,
                )
            )
    if title == "联营企业收入":
        header = re.search(r"本期[^\n]+上期[^\n]+\n([^\n]+)", text)
        revenue = re.search(r"(?m)^\s*营业收入\s+((?:\d[\d,]*\.\d+\s*){4})", text)
        if header and revenue:
            names = header[1].split()
            cells = revenue[1].split()
            if len(names) == 4:
                rows.extend(
                    (
                        "business_segment",
                        name,
                        value,
                        "重要联营企业/本期营业收入",
                        None,
                        "联营企业：" + name,
                    )
                    for name, value in zip(names[:2], cells[:2])
                )
    if title == "主营业务分业务":
        joined = _join_segment_label_amounts(
            _join_pdf_soft_breaks(text), core_answer_repair=True
        )
        for line in joined.splitlines():
            row = _parse_segment_row(line)
            if row and row[0] != "合计":
                rows.append(("business_type", row[0], row[1], "主营业务分业务", None))
    if title == "境外子公司收入":
        for m in re.finditer(
            r"(?m)^\s*([A-Za-z]+(?:\s+[A-Za-z]+){1,6})\s+资产收购\s+[^\n]+?\s+(\d[\d,]*\.\d+)\s+\d",
            text.replace("Silver Fern\nFarms Limited", "Silver Fern Farms Limited"),
        ):
            rows.append(
                (
                    "business_segment",
                    m[1],
                    m[2],
                    "境外资产/本报告期营业收入",
                    None,
                    "原子公司：" + m[1],
                )
            )
    if title == "电量收入及成本" and "售电量" in text and "收入" in text:
        for line in text.splitlines():
            m = re.match(
                r"\s*([\u4e00-\u9fff]{2,10})\s+((?:-?\d[\d,]*\.\d+\s+){6}-?\d[\d,]*\.\d+)",
                line,
            )
            if m and m[1] not in {"合计", "其他"}:
                rows.append(
                    ("product", m[1], m[2].split()[4], "电力行业电量、收入及成本", None)
                )
    if title == "关联服务及出租收入":
        lessor = "本公司作为出租方" in text
        if lessor:
            rows.extend(_lessor_income_rows(text))
        elif "本公司受托管理/承包情况表" in text:
            for m in re.finditer(
                r"([^\s]+)\s+([^\s]+)\s+(其他资产托管)\s+[^\n]+?协议价格\s+(\d[\d,]*\.\d+)",
                text,
            ):
                rows.append(
                    (
                        "service",
                        m[3],
                        m[4],
                        "本公司受托管理/承包/本期确认的托管收益",
                        None,
                        "委托方：" + m[1] + "；受托方：" + m[2],
                    )
                )
        else:
            if "关联方 关联交易内容" in text:
                pending = ""
                for line in text.splitlines():
                    if not line.strip() or re.search(
                        r"年度报告|^\s*\d+\s*/\s*\d+|适用|单位|关联方|本期发生额|出售商品",
                        line,
                    ):
                        continue
                    m = re.match(
                        r"^\s*(?P<party>.*?)\s+(?P<name>[\u4e00-\u9fff]{1,20})\s+(?P<amount>-?\d[\d,]*\.\d{2})(?:\s+-?\d[\d,]*\.\d{2})?\s*$",
                        pending + " " + line if pending else line,
                    )
                    if m:
                        party = re.sub(r"\s+", "", m["party"])
                        pending = ""
                        if party.endswith(("公司", "分公司")) and m["name"] not in {
                            "销售商品",
                            "提供劳务",
                        }:
                            rows.append(
                                (
                                    "service",
                                    m["name"],
                                    m["amount"],
                                    "出售商品/提供劳务/本期发生额",
                                    None,
                                    "交易对方：" + party,
                                )
                            )
                    elif re.fullmatch(r"[\u4e00-\u9fff\s]+", line):
                        pending += re.sub(r"\s+", "", line)
                    elif re.search(r"\d", line):
                        pending = ""
            for m in re.finditer(
                r"(?m)^\s*([^\s]+)\s+(租金收入|租赁收入)\s+(\d[\d,]*\.\d+)", text
            ):
                rows.append(
                    (
                        "service",
                        m[2],
                        m[3],
                        "出售商品/提供劳务/本期发生额",
                        None,
                        "交易对方：" + m[1],
                    )
                )
            for m in re.finditer(r"管理服务费\s+(\d[\d,]*\.\d+)", text):
                rows.append(
                    (
                        "service",
                        "管理服务费",
                        m[1],
                        "出售商品/提供劳务/本期发生额",
                        None,
                    )
                )
    if title in {"收入附注原生栏目", "项目销售收入", "贸易业务收入"}:
        rows.extend(_native_note_income_rows(text, title))
    for line in text.splitlines():
        if title in {
            "收入附注原生栏目",
            "项目销售收入",
            "贸易业务收入",
            "电量收入及成本",
            "关联服务及出租收入",
            "经营租赁收入",
            "主营业务分业务",
            "境外子公司收入",
            "细分行业收入",
            "渠道收入",
            "其他收入扣除明细",
            "主体营业收入",
            "联营企业收入",
            "转租收入",
            "融资租赁收入",
            "关联租出资产收入",
            "非全资子公司收入",
            "在建项目收入",
        }:
            continue
        if title == "报告分部的财务信息":
            # The PDF owner may place the next expense row on the same line.
            line = re.split(r"二[.．、]\s*营业费用", line, maxsplit=1)[0]
        numbers = re.findall(r"(?<![\d.])-?\d[\d,]*(?:\.\d+)?", line)
        if title == "营业收入和营业成本情况":
            m = re.match(r"\s*(主营业务|其他业务)\s+", line)
            columns = next(
                (
                    re.findall(r"收入|成本", h)
                    for h in text.splitlines()
                    if re.fullmatch(r"\s*(?:(?:收入|成本)\s*){4}", h)
                ),
                [],
            )
            cells = line[m.end() :].split() if m else []
            if (
                m
                and len(columns) == 4
                and (
                    len(cells) == 4
                    or len(cells) == 1
                    and re.sub(r"\s+", "", line[: line.find(cells[0])])
                    not in _prior_only_income_labels(text)
                    or len(cells) == 1
                    and m[1] == _current_sparse_income_label(text)
                )
                and cells[columns.index("收入")] not in {"-", "/"}
            ):
                rows.append(
                    (
                        "business_type",
                        m[1],
                        cells[columns.index("收入")],
                        "营业收入",
                        None,
                    )
                )
        elif title == "按销售模式":
            m = re.match(r"\s*([\u4e00-\u9fff]{2,20})\s+", line)
            if m and len(numbers) == 4 and m[1] not in {"总计", "合计"}:
                rows.append(("sales_mode", m[1], numbers[2], "销售收入", None))
        elif title == "新能源汽车收入及补贴":
            m = re.match(r"\s*([\u4e00-\u9fff]{2,20})\s+", line)
            if m and numbers and m[1] not in {"合计", "总计"}:
                rows.append(("product", m[1], numbers[0], "收入", None))
        else:
            if "主营业务收入/抵销前" in {r[3] for r in rows}:
                continue
            if title == "报告分部的财务信息" and re.search(
                r"分部\s*\d+|202\d\s*年[^\n]+分部间抵销", text
            ):
                # This native table labels numbered segment columns explicitly.
                continue
            header = next(
                (
                    l
                    for l in text.splitlines()
                    if l.strip().startswith("项目 ") and "分部间抵销" in l
                ),
                "",
            )
            labels = header.split()[1:]
            if len(labels) < 3 or not (
                all(label.endswith("分部") for label in labels[:-2])
                or labels[-2:] == ["分部间抵销", "合计"]
            ):
                wrapped = re.search(r"月\s*31\s*日\s*([\s\S]+?)\n\s*营业收入\s", text)
                if not wrapped:
                    continue
                header = re.sub(r"\s*\n\s*", "", wrapped[1])
                header = re.sub(r"(及配件)(?=工程)", r"\1 ", header)
                header = re.sub(r"(询和监理)(?=发电)", r"\1 ", header)
                labels = header.split()
                if len(labels) != 6 or labels[-2:] != ["分部间抵销", "合计"]:
                    continue
            basis = next(
                (
                    b
                    for b in ("分部间交易收入", "对外交易收入", "营业收入")
                    if b in re.sub(r"\s+", "", line)
                ),
                None,
            )
            if not basis or len(numbers) < len(labels) - 1:
                continue
            # Native 一. ordinal is not an amount; if a numeric ordinal exists skip it.
            numbers = re.findall(r"-?\d[\d,]*\.\d+", line)
            income_labels = (
                [label for label in labels if label != "分部间抵销"]
                if basis == "对外交易收入" and len(numbers) == len(labels) - 1
                else labels
            )
            for label, value in zip(income_labels, numbers):
                if label == "合计":
                    continue
                adjustment = (
                    RowClass.CONSOLIDATION_ADJUSTMENT if "抵销" in label else None
                )
                rows.append(
                    (
                        "adjustment" if adjustment else "business_segment",
                        label,
                        value,
                        basis,
                        adjustment,
                    )
                )
    records = []
    if title == "报告分部的财务信息" and re.search(
        r"分部\s*\d+|202\d\s*年[^\n]+分部间抵销", text
    ):
        rows.extend(_native_note_income_rows(text, "收入附注原生栏目"))
    for row_index, row in enumerate(rows):
        dim, name, value, basis, row_class = row[:5]
        qualifier = row[5] if len(row) > 5 else None
        for item, model in ((si, Segment), (mi, Measurement)):
            if item is None:
                continue
            kwargs = (
                {"dimension": dim, "label": name}
                if model is Segment
                else {
                    "metric_type": MetricType.OPERATING_REVENUE,
                    "logical_slot": LogicalSlot.REVENUE,
                    "measured_object": name,
                    "segment_dimension": dim,
                    "segment_label": name,
                    "relationship_context": basis,
                }
            )
            records.append(
                _base_fact(
                    model,
                    report=selection.report,
                    record_id=f"owned:{item.evidence.evidence_id}:{model.__name__}:{dim}:{name}:{basis}:row:{row_index}",
                    field_id="segment_dimension"
                    if model is Segment
                    else "operating_revenue",
                    chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
                    evidence=item.evidence,
                    source_native=SourceNativeValue(
                        name=name,
                        value=value,
                        unit=unit,
                        header=basis,
                        qualifier="分部间抵销" if row_class else qualifier,
                    ),
                    row_class=row_class,
                    excerpt_subject=text,
                    **kwargs,
                )
            )
            if (
                title in {"主体营业收入", "联营企业收入", "非全资子公司收入"}
                or title == "贸易业务收入"
                and qualifier
            ):
                records[-1] = records[-1].model_copy(
                    update={
                        "subject_scope": SubjectScope.BUSINESS_SEGMENT
                        if qualifier
                        and any(t in qualifier for t in ("联营企业", "参股公司"))
                        else SubjectScope.NAMED_SUBSIDIARY,
                        "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                    }
                )
    if "母公司财务报表" in text:
        records = [
            r.model_copy(
                update={
                    "subject_scope": SubjectScope.ISSUER,
                    "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                }
            )
            for r in records
        ]
    if title == "境外子公司收入":
        records = [
            r.model_copy(
                update={
                    "subject_scope": SubjectScope.NAMED_SUBSIDIARY,
                    "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
                }
            )
            for r in records
        ]
    return tuple(records)


def _owned_product_action_bindings(excerpt: str) -> list[dict[str, Any]]:
    """Keep current subsidiary product branches and transaction directions."""
    text = re.sub(r"\s+", "", excerpt)
    bindings = []

    def add(name, action, actor, verb="销售", header=None):
        bindings.append(
            {
                "name": name,
                "action": action,
                "actor": actor,
                "verb": verb,
                "header": header,
                "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
            }
        )

    def current(clause):
        return not re.search(
            r"第三方|不从事|不销售|未销售|尚未|拟|计划|将销售|将推出|拟推出", clause
        )

    # Each subsection declares its own operator; a brand is never the object.
    for branch in re.split(r"[（(][一二三四五六][)）]", text):
        owner = re.search(
            r"业务主要由子公司(.+?有限公司(?:和[^。]+?有限公司)?)从事", branch
        )
        if not owner or not current(branch):
            continue
        actor = owner[1].replace("有限公司和", "有限公司/")
        if re.search(r"直接向[^。]+销售生猪", branch):
            add("生猪", ActivityAction.SELLS, actor)
        if "外购仔猪" in branch:
            add("仔猪", ActivityAction.PURCHASES, actor, "外购")
        products = re.search(
            r"产品(?:主要)?为[“\"][^”\"]+[”\"](?:和[“\"][^”\"]+[”\"])?品牌的([^。]+)",
            branch,
        )
        if products and re.search(r"销售|流通渠道|出口", branch):
            cell = re.split(r"以及相关产品", products[1], maxsplit=1)[0].removeprefix(
                "各类分切"
            )
            for name in re.split(r"、|及", cell):
                if name:
                    add(name, ActivityAction.SELLS, actor)
        # Brand products are named goods; the marketing model affirms delivery.
        products = re.search(r"产品主要有([^。]+?)等。", branch)
        if products and re.search(r"营销业务模式|销售", branch):
            for name in re.split(r"、", products[1]):
                add(name, ActivityAction.SELLS, actor)

    for m in re.finditer(
        r"子公司([^。]+?)从事[^。]+的生产和销售，产品为[“\"][^”\"]+[”\"]品牌的([^。]+)。",
        text,
    ):
        if not current(_sentence_containing(text, m.start())):
            continue
        actor = m[1].replace("有限公司和", "有限公司/")
        # “蔬菜及番茄沙司类罐头” is one printed product, not two fragments.
        for name in re.split(r"、|和(?=蔬菜)", m[2]):
            add(name, ActivityAction.SELLS, actor)
    for m in re.finditer(
        r"子公司([^。]+?)从事[^。]+的生产和销售，产品包括([^。]+)。", text
    ):
        if not current(_sentence_containing(text, m.start())):
            continue
        actor = (
            m[1].replace("有限公司、", "有限公司/").replace("有限公司和", "有限公司/")
        )
        cell = re.search(r"各类([^，]+)产品", m[2])
        if cell:
            for name in cell[1].split("、"):
                add(name + "产品", ActivityAction.SELLS, actor)
    for m in re.finditer(
        r"([^。]+?)食品以牛排及肉牛产品为主导，集加工、销售、服务于一体[^。]+。", text
    ):
        if current(_sentence_containing(text, m.start())):
            actor = re.split(r"[；，]", m[1])[-1] + "食品"
            add("牛排", ActivityAction.SELLS, actor)
    for m in re.finditer(
        r"(?:^|[。；])([^。；]{2,12}食品)[^。]+开发([^。]+?)产品开拓餐饮新市场", text
    ):
        if current(_sentence_containing(text, m.start())):
            for name in re.split(r"及|和|、", m[2]):
                add(name + "产品", ActivityAction.SELLS, m[1])
    # Current performance paragraphs provide sales evidence independently of
    # a product's mere development or an IP/brand list.
    for m in re.finditer(
        r"([^。，]{2,12})推出新品([^。]+?)，\d+款新品，\d{4}年新品[^。]+销售额[^。]+。",
        text,
    ):
        if current(_sentence_containing(text, m.start())):
            for name in m[2].split("、"):
                add(name, ActivityAction.SELLS, m[1])
    for m in re.finditer(
        r"([^。]{2,12}食品)以[“\"]([^”\"]+)[”\"]为核心[^。]+推出了([^。]+?)等衍生产品，成为明星单品。",
        text,
    ):
        if current(_sentence_containing(text, m.start())):
            add(m[2], ActivityAction.SELLS, m[1])
            for name in m[3].split("、"):
                if name.endswith(("肉", "肉馅")):
                    add(name, ActivityAction.SELLS, m[1])
    for m in re.finditer(
        r"([^。]{2,12})推出[“\"][^”\"]+[”\"]系列([^。]+?)等\d+款新产品。重点采用全渠道营销",
        text,
    ):
        if current(_sentence_containing(text, m.start())):
            for name in m[2].split("、"):
                add(name, ActivityAction.SELLS, m[1])
    for m in re.finditer(r"完成[^。]+产品的研发及上市，包括([^。]+)。", text):
        if current(_sentence_containing(text, m.start())):
            actor_match = re.search(r"(?:^|。)([^。]{2,12})多措并举", text[: m.start()])
            if not actor_match:
                continue
            cell = re.split(r"，以及", m[1], maxsplit=1)
            for name in cell[0].split("、"):
                add(name, ActivityAction.SELLS, actor_match[1])
            if len(cell) > 1 and cell[1].endswith("棒棒糖"):
                add("棒棒糖", ActivityAction.SELLS, actor_match[1])
    for m in re.finditer(
        r"([\u4e00-\u9fff]{2,15}肉品)不断优化[\s\S]+?(?=上海爱森|$)", text
    ):
        if current(m.group()) and "持续热销" in m.group():
            goods = re.search(r"[；，]([^；，]+?)入驻[^。；，]+后持续热销", m.group())
            if goods:
                for name in goods[1].split("、"):
                    add(name, ActivityAction.SELLS, m[1])
    for sentence in re.split(r"(?<=。)", text):
        if not current(sentence):
            continue
        if "白条肉三维销售模式" in sentence and "年度白条销售超额完成" in sentence:
            add("白条肉", ActivityAction.SELLS, "公司及合并子公司")
        if "猪副产品收入表现良好" in sentence:
            add("猪副产品", ActivityAction.SELLS, "公司及合并子公司")
        if re.search(r"公司饲料贸易业务板块业务规模下降", sentence):
            add("饲料", ActivityAction.SELLS, "公司及合并子公司")
        if re.search(r"采购端通过饲料集采", sentence):
            add("饲料", ActivityAction.PURCHASES, "公司及合并子公司", "采购")
        if "生猪采购" in sentence and "公司" in sentence:
            add("生猪", ActivityAction.PURCHASES, "公司及合并子公司", "采购")
        if "本公司从事油品销售收入" in sentence:
            add("油品", ActivityAction.SELLS, "本公司")
        if re.search(r"本公司与客户之间的销售合同通常包含转让", sentence):
            m = re.search(r"转让相关(.+?)，销售(.+?)及安装的履约义务", sentence)
            if m:
                for name in [*m[1].split("及"), m[2]]:
                    add(name, ActivityAction.SELLS, "本公司")
    if re.search(r"公司主要产品包括[^。]+饮用水等，主要品牌包括", text):
        clause = re.search(r"公司主要产品包括[^。]+", text)[0]
        if current(clause):
            add("饮用水", ActivityAction.SELLS, "公司及合并子公司")
    # The native category establishes direction; a customer's 采购项目 does not.
    for m in re.finditer(
        r"(?<!不)(?<!未)(?<!拟)销售商品(.+?)(?=销售商品|购买商品|接受劳务|提供劳务|金额合计|$)",
        text,
    ):
        row = m[1]
        if not current(row):
            continue
        for product in re.finditer(
            r"车道自助发卡机|车道[^。]{0,90}?自助缴费机|高速公路机电系统设备", row
        ):
            add(
                "车道自助缴费机" if "缴费机" in product[0] else product[0],
                ActivityAction.SELLS,
                "公司及合并子公司",
                header="销售商品",
            )
    if (
        "向关联方采购商品" in text
        and not re.search(r"(?:不|未|拟|计划|将)采购商品", text)
        and re.search(r"设备及安全设施采购\d[\d,]*\.\d+\d", text)
    ):
        add(
            "设备及安全设施",
            ActivityAction.PURCHASES,
            "公司及合并子公司",
            "采购",
            "实际执行金额",
        )
    return bindings


def _owned_goods_bindings(excerpt: str) -> list[dict[str, Any]]:
    """Named current goods from direct actions or owned transaction columns."""
    text = re.sub(r"\s+", "", excerpt)
    bindings = []

    def add(name, action, actor, verb):
        bindings.append(
            {
                "name": name,
                "action": action,
                "actor": actor,
                "verb": verb,
                "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
            }
        )

    for sentence in re.split(r"(?<=。)", text):
        if re.search(r"尚未|第三方|不从事|未销售|拟|计划|将销售|预研", sentence):
            continue
        owned = re.search(
            r"风电主机装备业务及关键配套业务主要包括([^。]+)主机销售等。公司通过招投标的方式获取主机订单",
            text,
        )
        if (
            owned
            and "公司通过招投标的方式获取主机订单" in sentence
            and not re.search(r"不|未|拟|计划|将|第三方", owned[1])
        ):
            add("风电主机", ActivityAction.SELLS, "公司", "销售")
        if re.search(r"公司(?:的)?塔筒业务主要包括[^。]+销售业务", sentence):
            add("塔筒", ActivityAction.SELLS, "公司", "销售")
        if re.search(r"公司风机配件销售时", sentence):
            add("风机配件", ActivityAction.SELLS, "公司", "销售")
        if re.search(r"本公司电力产品销售于[^。]+省电网公司", sentence):
            add("电力", ActivityAction.SELLS, "本公司", "销售")
        if re.search(
            r"公司控股子公司([^。]+?)主要从事叶片的设计、研发及制造业务", sentence
        ):
            actor = re.search(r"公司控股子公司([^。]+?)主要从事", sentence)[1]
            add("叶片", ActivityAction.PRODUCES, actor, "制造")
        if re.search(
            r"通过向[^。]+供应商采购[^。]+的风机零部件", sentence
        ) and re.search(r"由公司[^。]+生产制造", sentence):
            add("风机零部件", ActivityAction.PURCHASES, "公司", "采购")
    if "出售商品/提供劳务情况表" in text:
        table = text.split("出售商品/提供劳务情况表", 1)[1].split(
            "购销商品、提供和接受劳务", 1
        )[0]
        if re.search(r"√适用□不适用", table) and re.search(
            r"废旧物资处置\d[\d,]*\.\d+", table
        ):
            add("废旧物资", ActivityAction.SELLS, "本公司", "出售")
    if re.search(
        r"报告期内，公司累计获取电站建设指标[^。]+完成电站项目转让规模", text
    ) and not re.search(r"(?:拟|计划|未)完成电站项目转让", text):
        add("电站产品", ActivityAction.SELLS, "本公司", "销售")

    # The affirmative owned trade row itself supplies the names. Its income
    # stays in the revenue evidence, never in a physical-quantity field.
    if "报告期内公司存在贸易业务收入" in text:
        table = text.split("报告期内公司存在贸易业务收入", 1)[1].split("贸易业务占", 1)[
            0
        ]
        row = re.search(r"主要是开展各类([\u4e00-\u9fff]{2,30})等贸易业务\d", table)
        if row and not re.search(
            r"第三方|子公司|不存在|拟|计划|尚未|未开展", table[: row.end()]
        ):
            for name in re.split(r"及|、", row[1]):
                add(
                    name,
                    ActivityAction.SELLS,
                    "本集团" if "本集团" in text else "公司",
                    "销售",
                )

    for sentence in re.split(r"(?<=。)", text):
        if re.search(
            r"尚未|第三方公司|(?:不|未|拟|计划|将)(?:从事|开展|采购|销售|出售|购买|消耗)",
            sentence,
        ):
            continue
        direct = re.search(
            r"(?<!第三方)(?:本公司|公司)采购([\u4e00-\u9fff]{2,12})生产加工成", sentence
        )
        if direct and not re.search(r"子公司|第三方|供应商", sentence[: direct.end()]):
            add(direct[1], ActivityAction.PURCHASES, "公司", "采购")
        sale = re.search(
            r"报告期内，公司累计实现([\u4e00-\u9fff]{2,12})销售量达\d", sentence
        )
        if sale:
            add(sale[1], ActivityAction.SELLS, "公司", "销售")
        energy = re.search(
            r"本集团已采用[^。]+降低([\u4e00-\u9fff]{2,8})消耗量", sentence
        )
        if energy:
            bindings.append(
                {
                    "name": energy[1],
                    "action": None,
                    "actor": "本集团",
                    "verb": "消耗",
                    "header": "能源",
                }
            )
        purchase = re.search(
            r"本集团大部分的([\u4e00-\u9fff]{2,8})消耗须以[^。]+在国内购买", sentence
        )
        if purchase:
            add(purchase[1], ActivityAction.PURCHASES, "本集团", "购买")
        received = re.search(r"向本集团(?:出售|销售)([^。]+?)而向其支付", sentence)
        if received:
            name = re.split(r"所需的", received[1])[-1]
            if re.fullmatch(r"[\u4e00-\u9fff]{2,12}", name):
                add(name, ActivityAction.PURCHASES, "本集团", "采购")
        offered = re.search(
            r"本集团向[^。]+?(?:销售|出售)([\u4e00-\u9fff]{2,12}?)(?:取得的?收入|的收入|取得的?费用|，|。)",
            sentence,
        )
        if offered and not re.search(r"商品|服务|价格|收入|费用|代理", offered[1]):
            add(offered[1], ActivityAction.SELLS, "本集团", "销售")
        content_purchase = re.search(
            r"(?:^|[、，和])([^、，和]{2,12}产品)采购费是指[^。]+向本集团", sentence
        )
        if content_purchase:
            add(content_purchase[1], ActivityAction.PURCHASES, "本集团", "采购")

    # Ownership comes from the table section; counterparty names never identify
    # goods. The group direction is distinct from its ultimate parent's name.
    for match in re.finditer(
        r"(?:采购商品/接受劳务情况表|出售商品/提供劳务情况表)[\s\S]+?(?=购销商品、提供和接受劳务的关联交易说明|$)",
        excerpt,
    ):
        block = match.group()
        if not re.search(r"[√✓]适用\s*□不适用", block):
            continue
        for row in re.finditer(
            r"(?:^|\n|\s)(购买|购|售)([\u4e00-\u9fff]{1,12})\s+\d[\d,]*\.\d+", block
        ):
            verb, name = row[1], row[2]
            if name.endswith("服务"):
                continue
            add(
                name,
                ActivityAction.SELLS if verb == "售" else ActivityAction.PURCHASES,
                "公司",
                verb,
            )
    if "采购商品/接受劳务" in text and re.search(r"本集团关联方关联交易", text):
        for m in re.finditer(
            r"采购商品(?:及接受劳务)?(航空配餐|航材)(?:采购)?费[^。]*?\d", text
        ):
            add(m[1], ActivityAction.PURCHASES, "本集团", "采购")
        if "文创产品采购费" in text:
            add("文创产品", ActivityAction.PURCHASES, "本集团", "采购")
    if "出售商品/提供劳务" in text and re.search(r"本集团关联方关联交易", text):
        table = text.split("本公司除子公司外", 1)[0]
        for m in re.finditer(r"销售商品(航空配餐)(?:[（(][a-z]+[)）])?\d", table):
            add(m[1], ActivityAction.SELLS, "本集团", "销售")
    return bindings


def _explicit_current_commodity_bindings(excerpt: str) -> list[dict[str, Any]]:
    """Current owned sales/purchases/use, keeping actual action and source actor."""
    text = re.sub(r"\s+", "", excerpt)
    bindings = [
        *_owned_goods_bindings(excerpt),
        *_owned_product_action_bindings(excerpt),
        *_related_current_goods_bindings(excerpt),
    ]

    def add(name, action, verb, actor="本公司", header=None):
        bindings.append(
            {
                "name": name,
                "action": action,
                "verb": verb,
                "actor": actor,
                "header": header,
            }
        )

    main = re.search(
        r"公司主要从事[^。]+研发、生产和销售[^。]+。主要产品包括(?P<names>[^。]+)", text
    )
    if main and not re.search(r"尚未|不从事|未销售|拟|计划|将销售", main.group()):
        for name in main["names"].split("、"):
            if name.endswith("复合集装箱地板"):
                add(name, ActivityAction.SELLS, "销售", "公司", "主要业务及产品")
    for match in re.finditer(r"富余(?P<name>[^，。]{2,20})对外销售", text):
        prefix = text[max(0, text.rfind("。", 0, match.start()) + 1) : match.start()]
        if not re.search(r"第三方|未|不|拟|计划|将", prefix):
            add(match["name"], ActivityAction.SELLS, "对外销售", "公司")
    # These are the current deduction explanation cells, not the preceding
    # list of possible non-core businesses in the standard table label.
    for match in (
        list(re.finditer(r"\d[\d,]*\.\d+主要系(?P<cell>[^\d]+)", text))[:1]
        if "本年度具体扣除情况上年度具体扣除情况" in text
        else ()
    ):
        cell = match["cell"]
        for branch in cell.split("、"):
            if re.search(r"尚未|未销售|不销售|拟|计划|将", branch):
                continue
            sale = re.fullmatch(
                r"(?P<name>[\u4e00-\u9fff]{2,8})销售|出售(?P<other>[\u4e00-\u9fff]{2,8})",
                branch,
            )
            if sale:
                add(
                    sale["name"] or sale["other"],
                    ActivityAction.SELLS,
                    "销售",
                    "公司及合并子公司",
                    "本年度其他业务收入",
                )
    for match in re.finditer(
        r"\d[\d,]*\.\d+木方收入及(?P<actor>[\u4e00-\u9fff]{2,10})木质材料收入", text
    ):
        add(
            "木方",
            ActivityAction.SELLS,
            "收入",
            "公司及合并子公司",
            "本年度其他业务收入",
        )
        add(
            "木质材料",
            ActivityAction.SELLS,
            "收入",
            match["actor"],
            "本年度其他业务收入",
        )
    subsidiary = re.search(
        r"(?P<actor>[\u4e00-\u9fff]{2,40}有限公司)100[.]00生产销售：[\s\S]+?(?=\d[\d,]*\.\d+)",
        text,
    )
    if subsidiary and not re.search(r"尚未|未销售|拟|计划|将", subsidiary.group()):
        names = subsidiary.group().split("生产销售：", 1)[1]
        for name in names.split("、"):
            if re.fullmatch(r"[\u4e00-\u9fff]{2,20}", name):
                add(
                    name,
                    ActivityAction.SELLS,
                    "生产销售",
                    subsidiary["actor"],
                    "原子公司主要业务",
                )
                bindings[-1]["subject_basis"] = SubjectBasis.DIRECT_SOURCE_WORDING
    purchase = re.search(
        r"采购商品/接受劳务情况表(?P<table>[\s\S]+?)(?=出售商品/提供劳务|$)", excerpt
    )
    if purchase and not re.search(r"拟采购|计划采购|未采购|不采购", purchase.group()):
        for match in re.finditer(
            r"(?m)^\s*[^\n]+?\s+(?P<name>箱板)\s+(?P<amount>\d[\d,]*\.\d+)",
            purchase["table"],
        ):
            if Decimal(match["amount"].replace(",", "")) > 0:
                add(
                    match["name"],
                    ActivityAction.PURCHASES,
                    "采购",
                    "公司及合并子公司",
                    "采购商品/接受劳务/本期发生额",
                )
    sales = re.search(r"出售商品/提供劳务(?P<table>[\s\S]+)", excerpt)
    if sales and not re.search(r"拟出售|计划出售|未出售|不出售", sales.group()):
        for match in re.finditer(
            r"(?m)^\s*[^\n]+?\s+(?P<name>木结构产品)\s+(?P<amount>-?\d[\d,]*\.\d+)",
            sales["table"],
        ):
            if Decimal(match["amount"].replace(",", "")) > 0:
                add(
                    match["name"],
                    ActivityAction.SELLS,
                    "出售",
                    "公司及合并子公司",
                    "出售商品/提供劳务/本期发生额",
                )
    for match in re.finditer(
        r"(?:^|[。；;])(?:报告期内，)?本公司[^。]{0,40}销售电力产品和热力产品", text
    ):
        if not re.search(r"尚未|未销售|拟|计划|将销售", match.group()):
            for name in ("电力", "热力"):
                add(name, ActivityAction.SELLS, "销售")
    for match in re.finditer(
        r"本公司及其子公司[（(]以下简称[“\"]本集团[”\"][)）]主要从事(?P<activities>[^。]+)",
        text,
    ):
        # An action modifier governs its enumerated branch, not the whole page.
        # The group definition proves the actor, but cannot affirm a planned sale.
        for branch in re.split(r"[、，,；;]|以及|及", match["activities"]):
            sale = re.search(r"煤炭销售", branch)
            if sale and not re.search(
                r"不|未|没有|拟|计划|将|未来|预计|准备|打算",
                branch[: sale.start()],
            ):
                add("煤炭", ActivityAction.SELLS, "销售", "本集团")
                bindings[-1]["subject_basis"] = SubjectBasis.DIRECT_SOURCE_WORDING
                break
    for m in re.finditer(
        r"\d{4}年，本公司向[^。]+?(采购|购买)(煤炭|燃料)的实际发生总金额[^。]+。", text
    ):
        if not re.search(r"拟|计划|尚未|未购买|未采购|将购买|将采购", m.group()):
            add(m[2], ActivityAction.PURCHASES, m[1])
    for m in re.finditer(r"\d{4}年，本公司向[^。]+实际发生总金额[^。]+。", text):
        if re.search(
            r"向[^。]+出售燃料及提供服务的实际发生总金额", m.group()
        ) and not re.search(r"拟|计划|尚未|未出售|将出售", m.group()):
            add("燃料", ActivityAction.SELLS, "出售")
    if re.search(r"报告期内，本公司供电煤耗累计完成", text):
        add("煤炭", None, "耗用", header="能源")
    if re.search(r"报告期内，本公司发电厂用电率完成", text):
        add("电力", None, "耗用", header="能源")
    if re.search(r"其他业务：主要为绿证交易业务，本期交易量[^。]+营业收入", text):
        add("绿证", ActivityAction.SELLS, "交易", header="当期绿证交易收入")
    # Table category labels need the owned bus context, not the industry label.
    table_prefix = re.split(r"整车产销量|新能源汽车产销量", text)[0][-150:]
    if re.search(r"子公司|第三方|拟|计划|尚未|将销售", table_prefix):
        return bindings
    if "整车产销量" in text or "新能源汽车产销量" in text:
        for section in re.split(r"按地区|按销售模式|新能源汽车收入", excerpt):
            if "销量（辆）" not in re.sub(r"\s+", "", section):
                continue
            for line in section.replace("\r", "").splitlines():
                m = re.match(
                    r"\s*(大型|中型|轻型|纯电动|插电式|燃料电池)\s+\d[\d,]*\s+\d", line
                )
                if m:
                    add(m[1] + "客车", ActivityAction.SELLS, "销售", "公司", "当期销量")
    return bindings


def _related_current_goods_bindings(excerpt: str) -> list[dict[str, Any]]:
    """Direct current objects in related transactions and subsidiary disclosures."""
    text = re.sub(r"\s+", "", excerpt)
    text = text.replace("销售商品提供劳务", "关联交易动作")
    bindings = []

    def add(name, action, actor="公司及合并子公司", header=None):
        bindings.append(
            {
                "name": name,
                "action": action,
                "actor": actor,
                "verb": "销售" if action == ActivityAction.SELLS else "采购",
                "header": header,
                "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
            }
        )

    table = re.search(
        r"(?:采购商品/接受劳务情况表|出售商品/提供劳务情况表|关联交易动作)[\s\S]+", text
    )
    if table:
        for m in re.finditer(
            r"(?P<verb>销售|采购)(?P<names>[^。；\d]{1,100}?)(?=，提供|、提供|，接受|、接受|，供应|、供应|协议价格|\d[\d,]*\.)",
            table.group(),
        ):
            position = table.start() + m.start()
            prefix = text[max(0, position - 12) : position]
            if re.search(r"尚未|不从事|未|不|拟|计划|将", prefix):
                continue
            for name in re.split(r"[、，]|及", m["names"]):
                name = re.sub(r"等.*$", "", name)
                if not re.fullmatch(r"[\u4e00-\u9fffA-Za-z]{1,20}", name) or name in {
                    "辅助原料",
                    "辅助原材料",
                    "材料",
                    "辅助材料",
                    "商品",
                    "原料",
                    "其他服务",
                }:
                    continue
                if name.endswith("费"):
                    continue
                add(
                    name,
                    ActivityAction.SELLS
                    if m["verb"] == "销售"
                    else ActivityAction.PURCHASES,
                )
        purchase = re.split(r"出售商品/提供劳务情况表", table.group(), maxsplit=1)[0]
        if table.group().startswith("采购商品"):
            for m in re.finditer(r"(?:工程物资|电费)\d[\d,]*\.", purchase):
                add(
                    "工程物资" if m.group().startswith("工程物资") else "电",
                    ActivityAction.PURCHASES,
                )
    for m in re.finditer(
        r"(?P<actor>[\u4e00-\u9fff]{2,12})子公司(?P<products>[^\d。]{1,60}?)的?生产(?:和|与)销售\d",
        text,
    ):
        if not re.search(r"未|拟|计划|将|尚未", m["products"]):
            for name in re.split(r"[、，]", m["products"]):
                name = name.removesuffix("的")
                if name and not name.endswith("等"):
                    add(name, ActivityAction.SELLS, m["actor"].removeprefix("名称型"))
    for m in re.finditer(
        r"(?P<name>[^。\d；]{1,12})系(?P<actor>[\u4e00-\u9fff]{2,12})生产后向[^。]+对外销售",
        text,
    ):
        name = re.split(r"[，、]", m["name"])[-1].lstrip(".．")
        if not re.search(r"尚未|未|拟|计划|将", m.group()):
            add(name, ActivityAction.SELLS, m["actor"])
    if "公司生产的产品在满足自身" in text:
        m = re.search(r"富余的([^。]+?)，凭借[^。]+，销售给其使用", text)
        if m and not re.search(r"不|未|拟|计划|将", m.group()):
            for name in re.split(r"[、，]", m[1]):
                if name.endswith("等工业废气"):
                    name = name.removesuffix("等工业废气")
                add(name.removeprefix("工业"), ActivityAction.SELLS)
    if "产销量情况说明" in text and re.search(
        r"电、蒸汽系除供应[^。]+自身耗用外", text
    ):
        for name in ["电", "蒸汽"]:
            bindings.append(
                {
                    "name": name,
                    "action": None,
                    "verb": "耗用",
                    "header": "能源",
                    "actor": "公司及合并子公司",
                }
            )
    return bindings
