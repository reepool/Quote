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
    BusinessOverview,
    ChapterTask,
    Evidence,
    Measurement,
    ReportIdentity,
    Segment,
    SemanticRecord,
    TextAnchor,
)

CORE_EVIDENCE_SCHEMA_VERSION = "company_profile_common_core_evidence.v1"
CORE_SOURCE_FIELD_IDS = (
    "business_overview_source",
    "explicit_activity",
    "segment_dimension",
    "operating_revenue",
)
_OVERVIEW_HEADINGS = (
    "报告期内公司从事的主要业务",
    "公司从事的主要业务",
    "主营业务分析",
    "主要产品及服务",
    "主要产品",
    "经营模式",
    "主营业务",
    "主要业务",
)
_SEGMENT_HEADINGS = (
    "占公司营业收入或营业利润10%以上",
    "主营业务分行业",
    "主营业务分产品",
    "分部报告",
    "分部信息",
    "分行业",
    "分产品",
)
_ALL_HEADINGS = _OVERVIEW_HEADINGS + _SEGMENT_HEADINGS
_HEADING_PREFIX = re.compile(
    r"^(?:第[一二三四五六七八九十百]+[节章]"
    r"|[一二三四五六七八九十]+、"
    r"|[（(][一二三四五六七八九十\d]+[)）]"
    r"|[0-9]+[.、．])"
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
        "extract_business_overview", "extract_segment_financials"
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
        "extract_business_overview", "extract_segment_financials"
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

    prepared: list[PreparedEvidence] = []
    for span in spans:
        prepared.extend(_prepared_evidence(report, span))
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
) -> _OwnedSelection:
    by_page = {item.page: item for item in pages}
    gaps: list[CoreEvidenceGap] = []
    for page in pages:
        owned = _owned_heading(page.text, headings)
        if owned is None:
            continue
        heading, _line_start, line_end = owned
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
        excerpt, continuations, closed, gap = _collect_section(
            page,
            heading=heading,
            line_end=line_end,
            by_page=by_page,
            chapter_task=chapter_task,
        )
        usable = _usable_excerpt(excerpt, heading, require_substance)
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
            continue
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
    return _OwnedSelection(None, gaps)


def _owned_heading(
    text: str,
    headings: tuple[str, ...],
) -> tuple[str, int, int] | None:
    if _is_toc_page(text):
        return None
    offset = 0
    for line in text.splitlines(keepends=True):
        heading = _heading_if_title_line(line, headings)
        if heading is not None:
            return heading, offset, offset + len(line)
        offset += len(line)
    return None


def _heading_if_title_line(line: str, headings: tuple[str, ...]) -> str | None:
    stripped = line.strip()
    if not stripped or _is_toc_line(stripped):
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


def _is_toc_page(text: str) -> bool:
    head = text[:200]
    if re.search(r"(?:^|\n)\s*目录\s*(?:\n|$)", head):
        return True
    return "......" in text[:400] or "……" in text[:400]


def _is_toc_line(line: str) -> bool:
    if "......" in line or "……" in line:
        return True
    compact = re.sub(r"\s+", "", line)
    if not re.search(r"\d{1,4}$", compact):
        return False
    body = _HEADING_PREFIX.sub("", compact, count=1)
    return any(heading in body for heading in _ALL_HEADINGS)


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
    match = _SECTION_BOUNDARY.search(text)
    if match is None:
        return _PageChunk(text.strip(), False)
    if match.start() == 0:
        return _PageChunk("", True)
    return _PageChunk(text[: match.start()].strip(), True)


def _usable_excerpt(excerpt: str, heading: str, require_substance: bool) -> bool:
    if not excerpt.strip() or excerpt.strip() == heading:
        return False
    if require_substance:
        return _OVERVIEW_SUBSTANCE.search(excerpt) is not None
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
    if not _period_matches(record.reported_period, report.report_period):
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
    demanded = _demanded_objects(field_id, span.excerpt)
    if not demanded:
        return True
    covered: set[str] = set()
    for record in covering:
        covered.update(_record_objects(record))
    return all(
        any(_object_mentioned(item, covered_item) or _object_mentioned(covered_item, item)
            for covered_item in covered)
        for item in demanded
    )


def _period_matches(reported_period: str, report_period: str) -> bool:
    reported = re.sub(r"\s+", "", reported_period)
    expected = re.sub(r"\s+", "", report_period)
    if not reported or not expected:
        return False
    return reported in expected or expected.startswith(reported) or reported.startswith(
        expected[:4]
    )


def _source_range_matches(record: SemanticRecord, span: CoreEvidenceSpan) -> bool:
    span_pages = {span.page, *span.continuation_pages}
    record_pages: set[int] = set()
    for item in record.evidence:
        record_pages.add(item.page)
        record_pages.update(item.continuation_pages)
    if record_pages & span_pages:
        return True
    compact = re.sub(r"\s+", "", span.excerpt)
    return any(_object_mentioned(item, compact) for item in _record_objects(record))


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
