"""Automatic common-core Evidence selection.

This owner locates overview and segment text for the three core dimensions. It
reuses accepted structured facts and does not require a frozen per-company page
plan or an LLM call.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .contracts import PreparedEvidence
from .core_assessment_projection import (
    COMMON_CORE_MAPPING_VERSION,
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
    r"取得货款|收入来[源于自]|向客户收取|主要产品为"
)
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
        headings=_OVERVIEW_HEADINGS,
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        field_ids=("business_overview_source", "explicit_activity"),
        require_substance=True,
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

    material = _select_material_span(normalized)
    if material is not None:
        spans.append(material)

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
        if any(
            item.record_id == record.record_id for item in reused_facts
        )
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
    )


def _select_owned_span(
    pages: Sequence[ReportPageText],
    *,
    headings: tuple[str, ...],
    chapter_task: ChapterTask,
    field_ids: tuple[str, ...],
    require_substance: bool,
    field_labels: tuple[str, ...] = (),
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
            preview, heading, require_substance
        ) and _excerpt_states_owned_overview(preview):
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
        excerpt = _clip_before_later_segment_template(excerpt)
        if not _formal_segment_table_ready(excerpt):
            return _SKIP_OWNED_HEADING
        closed = True
        gap = None
    elif heading in {"分部报告", "分部信息"} and _formal_segment_table_ready(excerpt):
        closed = True
        gap = None
    usable = _usable_excerpt(excerpt, heading, require_substance)
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
                found.append((heading, offset, offset + _heading_end_in_line(line, heading)))
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
        if rest and _FIELD_LABEL_VALUE.search(rest) and not re.fullmatch(
            r"[:：.。]+", rest
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


def _usable_excerpt(excerpt: str, heading: str, require_substance: bool) -> bool:
    if not excerpt.strip() or excerpt.strip() == heading:
        return False
    if require_substance:
        return _OVERVIEW_SUBSTANCE.search(_join_pdf_soft_breaks(excerpt)) is not None
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
        if (
            (heading in sentence or heading in re.sub(r"\s+", "", sentence))
            and len(re.sub(r"\s+", "", sentence)) > len(heading)
        ):
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
        if record.field_id == field_id
        and _record_matches_span(record, report, span)
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
        any(_object_mentioned(item, covered_item) or _object_mentioned(covered_item, item)
            for covered_item in covered)
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
            part
            for part in re.split(r"[、，,和及]", match.group(1))
            if len(part) >= 2
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
    complete_chapters = {
        span.chapter_task for span in spans if span.context_complete
    }
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
    re.compile(r"主营业务为\s*([^。；;]{2,80})"),
    re.compile(r"主要从事\s*([^。；;]{2,400})"),
    re.compile(r"主要产品包括\s*([^。；;]{2,80})"),
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


def project_owned_page_facts(
    selection: CoreEvidenceSelection,
    chapter: ChapterTask | None = None,
) -> tuple[SemanticRecord, ...]:
    """Project source-native core facts already stated in owned excerpts."""

    records: list[SemanticRecord] = []
    for span in selection.spans:
        if chapter is not None and span.chapter_task != chapter.value:
            continue
        if span.chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value:
            records.extend(_project_overview_span(selection, span))
        elif span.chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value:
            if span.section_title in _INCOME_ANALYSIS_HEADINGS:
                records.extend(_project_income_analysis_span(selection, span))
            else:
                records.extend(_project_segment_span(selection, span))
                records.extend(_project_company_total_rows(selection, span))
        elif span.chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS.value:
            records.extend(_project_material_span(selection, span))
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
        ):
            return item
    return None


def _project_overview_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
) -> tuple[SemanticRecord, ...]:
    overview_item = _prepared_for(selection, span, "business_overview_source")
    activity_item = _prepared_for(selection, span, "explicit_activity")
    records: list[SemanticRecord] = []
    if overview_item is not None and _excerpt_states_owned_overview(span.excerpt):
        source_text = _overview_source_text(
            span.excerpt,
            overview_item.evidence.anchor.bounded_quote
            if isinstance(overview_item.evidence.anchor, TextAnchor)
            else "",
            heading=span.section_title,
        )
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
    if activity_item is None:
        return tuple(records)
    seen: set[str] = set()
    excerpt = _join_pdf_soft_breaks(span.excerpt)
    for pattern in _CLAUSE_PATTERNS:
        for match in pattern.finditer(excerpt):
            verb = "经营"
            if match.group(0).startswith("主营"):
                verb = "为"
            elif "从事" in match.group(0):
                verb = "从事"
            elif "包括" in match.group(0):
                verb = "包括"
            elif "涵盖" in match.group(0):
                verb = "涵盖"
            for raw in _activity_object_clauses(match.group(1)):
                object_name, action, source_verb = _object_and_action(raw, verb)
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
                        activity_actor="公司",
                        source_actor="公司",
                        actor_basis=SubjectBasis.DIRECT_GRAMMATICAL_ACTOR,
                        object_name=object_name,
                        source_verb=source_verb,
                    )
                )
    return tuple(records)


def _project_segment_span(
    selection: CoreEvidenceSelection,
    span: CoreEvidenceSpan,
) -> tuple[SemanticRecord, ...]:
    if not span.context_complete:
        return ()
    segment_item = _prepared_for(selection, span, "segment_dimension")
    revenue_item = _prepared_for(selection, span, "operating_revenue")
    if segment_item is None and revenue_item is None:
        return ()
    excerpt = _join_segment_label_amounts(_join_pdf_soft_breaks(span.excerpt))
    quote = (
        segment_item.evidence.anchor.bounded_quote
        if segment_item is not None and isinstance(segment_item.evidence.anchor, TextAnchor)
        else ""
    )
    if revenue_item is not None and isinstance(revenue_item.evidence.anchor, TextAnchor):
        quote = quote or revenue_item.evidence.anchor.bounded_quote
    unit = None
    previous_was_unit = False
    group_dimensions: frozenset[str] | None = None
    dimension = _dimension_from_heading(span.section_title)
    records: list[SemanticRecord] = []
    started = False
    for line in excerpt.splitlines():
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
                unit = None
            group_dimensions = opened
            previous_was_unit = False
            started = True
            continue
        if _dimension_from_heading(line) is not None or _parse_segment_row(line) is not None:
            started = True
        section = _dimension_from_heading(line)
        if section is not None:
            if group_dimensions is not None and section in group_dimensions:
                previous_was_unit = False
                dimension = section
                continue
            group_dimensions = None
            if not previous_was_unit:
                unit = None
            previous_was_unit = False
            dimension = section
            continue
        if group_dimensions is not None and _is_enumerated_revenue_class(line):
            group_dimensions = None
            if not previous_was_unit:
                unit = None
        previous_was_unit = False
        parsed = _parse_segment_row(line)
        if parsed is None:
            continue
        label, amount, _share = parsed
        row_dimension = (
            "revenue_composition" if _is_enumerated_revenue_class(line) else dimension
        )
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
                    ),
                    dimension=row_dimension,
                    label=label,
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
                    ),
                    metric_type=MetricType.OPERATING_REVENUE,
                    logical_slot=LogicalSlot.REVENUE,
                    measured_object=label,
                    segment_dimension=row_dimension,
                    segment_label=label,
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
            f"owned:{revenue_item.evidence.evidence_id}"
            f":revenue:company_total:{label}"
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
        record_id=(
            f"owned:{revenue_item.evidence.evidence_id}"
            f":share:{label}:营业收入"
        ),
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

    normalized = _join_segment_label_amounts(_join_pdf_soft_breaks(excerpt))
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
    return re.match(r"\s*(?:[一二三四五六七八九十]+|\d+)[、.．]", line) is not None


def _join_segment_label_amounts(text: str) -> str:
    """Attach a wrapped label to the amount line that follows it."""

    lines = text.splitlines()
    merged: list[str] = []
    index = 0
    while index < len(lines):
        current = lines[index]
        nxt = lines[index + 1] if index + 1 < len(lines) else ""
        if (
            re.fullmatch(r"\s*[\u4e00-\u9fff]{2,12}\s*", current)
            and re.match(r"\s*-?[\d,]", nxt)
        ):
            merged.append(f"{current.strip()} {nxt.strip()}")
            index += 2
            continue
        merged.append(current)
        index += 1
    return "\n".join(merged)


def _clip_before_later_segment_template(excerpt: str) -> str:
    """Keep the MD&A revenue table and stop at the next non-revenue section."""

    kept: list[str] = []
    for line in excerpt.splitlines():
        if kept and _is_revenue_table_stop(line):
            break
        kept.append(line)
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
    if re.search(r"本集团", excerpt):
        return SubjectScope.CONSOLIDATED_GROUP, SubjectBasis.DIRECT_SOURCE_WORDING
    return SubjectScope.UNCLEAR, None


def _dedupe_owned_records(
    records: list[SemanticRecord],
) -> tuple[SemanticRecord, ...]:
    seen: set[tuple[object, ...]] = set()
    unique: list[SemanticRecord] = []
    for record in records:
        if isinstance(record, Measurement):
            key = (
                record.metric_type,
                record.measured_object,
                record.source_native.value,
                record.source_native.unit,
                record.segment_dimension,
                record.segment_label,
                record.relationship_context,
            )
            if key in seen:
                continue
            seen.add(key)
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


def explicit_material_input_names(text: str) -> tuple[str, ...]:
    """Return named materials the same sentence binds to the company's own input."""

    compact = re.sub(r"\s+", "", text)
    names: list[str] = []
    for sentence in re.split(r"[。；;]", compact):
        names.extend(_named_inputs_in_sentence(sentence))
        names.extend(_company_owned_material_lists(sentence))
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
) -> CoreEvidenceSpan | None:
    for page in pages:
        if not page.readable or not explicit_material_input_names(page.text):
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
) -> tuple[SemanticRecord, ...]:
    item = _prepared_for(selection, span, "material_input")
    if item is None:
        return ()
    records: list[SemanticRecord] = []
    for name in explicit_material_input_names(span.excerpt):
        records.append(
            _base_fact(
                Relationship,
                report=selection.report,
                record_id=(
                    f"owned:{item.evidence.evidence_id}:material:{name}"
                ),
                field_id="material_input",
                chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
                evidence=item.evidence,
                source_native=SourceNativeValue(name=name),
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
            r"公司主要业务情况|公司金融业务",
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


def _activity_object_clauses(clause: str) -> list[str]:
    """Drop service targets in “以……为对象，提供……” and keep the provided services."""

    text = clause.strip()
    provided = re.search(r"以.+?为对象[，,]?\s*提供(.+)", text)
    if provided:
        text = provided.group(1)
    objects: list[str] = []
    for item in _split_listed(text):
        item = item.strip("，,、；;。 ")
        item = re.sub(r"等[\u4e00-\u9fffA-Za-z0-9（）()]*$", "", item).strip("，,、；;。 ")
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
    for chunk in re.split(r"[、；;]|以及", text):
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
    if _looks_like_action_chain(text):
        object_name = _ACTION_TOKEN.split(text)[0].strip("的 、") or text
        if "制造" in text or "生产" in text:
            return object_name, ActivityAction.PRODUCES, "制造" if "制造" in text else "生产"
        if "研发" in text or "开发" in text:
            return object_name, ActivityAction.DEVELOPS, "研发"
        if "销售" in text:
            return object_name, ActivityAction.SELLS, "销售"
        return object_name, ActivityAction.OPERATES, default_verb
    if any(token in text for token in ("托管", "银行", "基金", "信贷", "服务", "咨询", "运维")):
        return text[:40], ActivityAction.PROVIDES_SERVICE, default_verb
    if "销售" in text:
        return re.sub(r"的?销售.*$", "", text) or text, ActivityAction.SELLS, "销售"
    if "研发" in text or "开发" in text:
        return re.sub(r"的?(?:研发|开发).*$", "", text) or text, ActivityAction.DEVELOPS, "研发"
    if any(token in text for token in ("制造", "生产", "加工")):
        return (
            re.sub(r"的?(?:制造|生产|加工).*$", "", text) or text,
            ActivityAction.PRODUCES,
            "生产",
        )
    return text[:40], ActivityAction.OPERATES, default_verb


_SEGMENT_SECTION_HEADINGS = {
    "主营业务分行业情况": "industry",
    "主营业务分行业": "industry",
    "分行业": "industry",
    "主营业务分产品情况": "product",
    "主营业务分产品": "product",
    "分产品": "product",
    "主营业务分地区情况": "region",
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
    remainder = re.sub(r"[、，,和及情况]", "", remainder)
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
    match = _UNIT_DECLARATION.search(excerpt)
    return None if match is None else match.group(1)


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
    if prev.endswith("个百分点"):
        return False
    if current.startswith(("下表", "项目", "营业收入", "营业总收入", "利息净收入")):
        return False
    if _parse_labeled_amount_row(
        current, _COMPANY_TOTAL_LABELS | _INCOME_AMOUNT_LABELS
    ) is not None:
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
    text = re.sub(r"^(?:[一二三四五六七八九十]+|\d+)[、.．]\s*", "", text)
    if not text:
        return None
    tokens = text.split()
    if len(tokens) < 3:
        return None
    label = tokens[0]
    if (
        label in _SKIP_SEGMENT_LABELS
        or "合计" in label
        or tokens[1] == "营业收入"
        or not re.fullmatch(r"[\u4e00-\u9fffA-Za-z0-9（）]{2,20}", label)
        or not re.fullmatch(r"-?[\d,]+(?:\.\d+)?", tokens[1])
        or not re.fullmatch(r"-?[\d,]+(?:\.\d+)?%?", tokens[2])
    ):
        return None
    return label, tokens[1], tokens[2]
