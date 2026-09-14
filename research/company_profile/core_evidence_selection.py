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
    overview_dimension_hits,
    resolved_core_field_ids,
)
from .models import (
    PRODUCTION_AUTHORIZATION,
    ChapterTask,
    Evidence,
    ReportIdentity,
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
_NEXT_HEADING = re.compile(
    r"(?:^|\n)\s*(?:第[一二三四五六七八九十]+节|[二三四五六七八九十]、)"
)
_CONTINUATION_TAIL = re.compile(r"(如下[:：]?|见表|详见|续[见表]|：$)$")
_OVERVIEW_SUBSTANCE = re.compile(
    r"(?:公司|本公司).{0,40}(?:主营|主要从事|经营|生产|销售|提供|研发)|"
    r"取得货款|收入来[源于自]|向客户收取|主要产品为"
)


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
    reused_fields = resolved_core_field_ids(accepted_records)
    reused_facts = tuple(
        ReusedStructuredFact(record_id=record.record_id, field_id=record.field_id)
        for record in accepted_records
        if record.field_id in reused_fields
    )
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

    covered_fields = set(reused_fields)
    unresolved: list[str] = []
    for span in spans:
        for field_id in span.field_ids:
            if field_id not in covered_fields:
                unresolved.append(field_id)
                covered_fields.add(field_id)

    prepared = tuple(_prepared_evidence(report, span) for span in spans)
    return CoreEvidenceSelection(
        report=report,
        spans=tuple(spans),
        reused_facts=reused_facts,
        unresolved_field_ids=tuple(unresolved),
        gaps=tuple(gaps),
        prepared_evidence=prepared,
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
        heading = _first_heading(page.text, headings)
        if heading is None:
            continue
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
        excerpt, continuation, missing_continuation = _excerpt_with_context(
            page, heading, by_page
        )
        if missing_continuation:
            gaps.append(
                CoreEvidenceGap(
                    code="page_unreadable",
                    chapter_task=chapter_task.value,
                    page=continuation,
                    message=f"continuation page is missing: {continuation}",
                )
            )
            return _OwnedSelection(None, gaps)
        if continuation is not None and not by_page[continuation].readable:
            gaps.append(
                CoreEvidenceGap(
                    code="page_unreadable",
                    chapter_task=chapter_task.value,
                    page=continuation,
                    message=f"continuation page is unreadable: {continuation}",
                )
            )
            return _OwnedSelection(None, gaps)
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
        if require_substance and not _OVERVIEW_SUBSTANCE.search(excerpt):
            continue
        dimensions = overview_dimension_hits(excerpt)
        if chapter_task is ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
            dimensions = ("products_services", "revenue_model")
        quote = _bounded_quote(excerpt, heading)
        return _OwnedSelection(
            CoreEvidenceSpan(
                page=page.page,
                continuation_pages=() if continuation is None else (continuation,),
                section_title=heading,
                excerpt=excerpt,
                bounded_quote=quote,
                chapter_task=chapter_task.value,
                field_ids=field_ids,
                dimension_ids=dimensions,
            ),
            gaps,
        )
    return _OwnedSelection(None, gaps)


def _first_heading(text: str, headings: tuple[str, ...]) -> str | None:
    compact = text.replace(" ", "")
    for heading in headings:
        if heading in text or heading in compact:
            return heading
    return None


def _excerpt_with_context(
    page: ReportPageText,
    heading: str,
    by_page: dict[int, ReportPageText],
) -> tuple[str, int | None, bool]:
    start = page.text.find(heading)
    if start < 0:
        compact = page.text.replace(" ", "")
        # Fall back to the full readable page when spacing differs.
        body = page.text if heading in compact else ""
    else:
        body = page.text[start:]
    split = _NEXT_HEADING.search(body[len(heading) :])
    if split is not None:
        body = body[: len(heading) + split.start()]
    excerpt = body.strip()
    compact_excerpt = re.sub(r"\s+", "", excerpt)
    needs_continuation = bool(_CONTINUATION_TAIL.search(compact_excerpt))
    short_body = len(excerpt) < len(heading) + 20
    continuation = None
    next_page = by_page.get(page.page + 1)
    if needs_continuation and next_page is None:
        return excerpt, page.page + 1, True
    if next_page is not None and (needs_continuation or short_body):
        continuation = next_page.page
        if next_page.readable and next_page.text.strip():
            excerpt = f"{excerpt}\n{next_page.text.strip()}".strip()
    return excerpt, continuation, False


def _bounded_quote(excerpt: str, heading: str) -> str:
    compact = re.sub(r"\s+", "", excerpt)
    for sentence in re.split(r"[。；;\n]", excerpt):
        sentence = sentence.strip()
        if (
            (heading in sentence or heading in re.sub(r"\s+", "", sentence))
            and len(re.sub(r"\s+", "", sentence)) > len(heading)
        ):
            return sentence[:180]
    if heading in excerpt:
        start = excerpt.find(heading)
        return excerpt[start : start + 180].strip()
    return compact[:180] or heading


def _prepared_evidence(report: ReportIdentity, span: CoreEvidenceSpan) -> PreparedEvidence:
    digest = hashlib.sha256(
        f"{report.report_id}:{span.chapter_task}:{span.page}:{span.bounded_quote}".encode()
    ).hexdigest()[:16]
    return PreparedEvidence(
        evidence=Evidence(
            evidence_id=f"core-ev-{digest}",
            report=report,
            page=span.page,
            section_title=span.section_title,
            continuation_pages=span.continuation_pages,
            anchor=TextAnchor(bounded_quote=span.bounded_quote),
        ),
        field_id=span.field_ids[0],
        context_complete=True,
        source_readable=True,
    )
