"""Bounded report reading used by the existing company-profile runtime.

The inventory sees source only; synthesis and verification use independent
contexts. All semantic decisions come from those contexts, not phrase gates.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from utils.llm.budget import (
    DEVELOPMENT_PAYLOAD_BYTE_LIMIT,
    DEVELOPMENT_RECOVERY_RESERVATION,
    LlmRunBudget,
)
from utils.llm.errors import LlmError

from .contracts import (
    ChecklistItem,
    ReportSemanticRequest,
    ReportSemanticRequestV3,
    ReportSemanticResponse,
    ReportSemanticResponseV3,
    ReportSourceCondition,
    ReportViewCheck,
    ReportViewClaim,
    SemanticProviderError,
)
from .core_assessment_projection import (
    CompanyProfileCoreAssessment,
    CoreDimensionAssessment,
)
from .models import (
    Activity,
    Evidence,
    Measurement,
    ReportIdentity,
    SemanticRecord,
    TextAnchor,
)
from .reading_budget import (
    _evidence_in_fragments,
    partial_slice_plan,
    records_for_fragments,
    request_reservation,
    reused_map_receipts,
    send_admission_hold,
    stage_reservations,
)

DISCOVERY_CHUNK_CHARS = 24_000
DISCOVERY_CHUNK_OVERLAP = 1_000
# Gateway framing, schema and the longest configured model name sit outside the
# compact user JSON. 12 KiB keeps a packed request inside the 65536-byte cap.
DEVELOPMENT_WIRE_MARGIN = 12_000
REPORT_POLICY = "company_profile_llm_led.v2"
REPORT_PROMPT = "company_profile_report_reading.2026-10-16.v1"
COMPACT_REPORT_PROMPT = "company_profile_report_reading.2026-10-17.v2"
DIMENSIONS = (
    "principal_business",
    "products_services",
    "revenue_model",
    "output_exposure",
    "input_exposure",
    "supply_chain_role",
    "upstream_downstream",
)
VIEWS = {
    "company_profile": DIMENSIONS[:3],
    "product_exposure": DIMENSIONS[3:5],
    "supply_chain_position": DIMENSIONS[5:],
}


class CompanyProfileCoreAssessmentV2(CompanyProfileCoreAssessment):
    mapping_version: Literal["company_profile_common_core_mapping.v2"] = (
        "company_profile_common_core_mapping.v2"
    )


class ReportInterpretation(BaseModel):
    """Versioned evidence-linked reading projection, persisted by the original writer."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal["company_profile_report_interpretation.v2"] = (
        "company_profile_report_interpretation.v2"
    )
    report: ReportIdentity
    processing_identity: dict[str, Any]
    execution_mode: Literal["offline_fake", "provider"]
    page_coverage: tuple[dict[str, Any], ...] = ()
    source_conditions: tuple[ReportSourceCondition, ...] = ()
    claims: tuple[ReportViewClaim, ...] = ()
    records: tuple[SemanticRecord, ...] = ()
    checks: tuple[ReportViewCheck, ...] = ()
    record_dispositions: tuple[dict[str, Any], ...] = ()
    call_receipts: tuple[dict[str, Any], ...] = ()
    gaps: tuple[str, ...] = ()
    verified_dimensions: tuple[str, ...] = ()
    draft_complete: bool = False
    semantic_complete: bool = False
    real_semantic_complete: bool = False
    elapsed_seconds: float = Field(default=0, ge=0)
    tokens_charged: int = Field(default=0, ge=0)
    budget_ledger: dict[str, Any] = Field(default_factory=dict)
    # Usage/cost absence is unknown, never zero measured expenditure.
    cost_status: Literal["unknown"] = "unknown"


def llm_led(identity: Mapping[str, Any]) -> bool:
    return identity.get("semantic_policy") == REPORT_POLICY


def offline_processing_identity() -> dict[str, Any]:
    return {
        "semantic_policy": REPORT_POLICY,
        "prompt_version": REPORT_PROMPT,
        "preprocessing_version": "full_page_chunks.v1",
        "model": "offline_fake",
        "execution_mode": "offline_fake",
        "semantic_reuse": False,
    }


def direct_core_processing_identity() -> dict[str, Any]:
    """Core-chapter reading: navigate, then write claims from the chapter text."""
    identity = compact_processing_identity(discovery_scope="page_map")
    identity["reading_strategy"] = "direct_core_v1"
    identity["prompt_version"] = "company_profile_report_reading.2026-10-19.direct-core.v1"
    identity["product_contract"] = (
        "company_profile_product_contract.2026-10-18.llm-led.v3"
    )
    identity["logical_request_timeout_seconds"] = 120
    identity["max_route_attempts"] = 2
    identity["max_recoveries"] = 1
    return identity


def layered_processing_identity() -> dict[str, Any]:
    """Full-report navigation, then core context, not every unread footnote."""
    identity = compact_processing_identity(discovery_scope="page_map")
    identity["reading_strategy"] = "layered_v3"
    identity["product_contract"] = (
        "company_profile_product_contract.2026-10-18.llm-led.v3"
    )
    return identity


def compact_processing_identity(*, discovery_scope: str) -> dict[str, Any]:
    if discovery_scope not in {"full_text", "page_map"}:
        raise ValueError("unknown compact discovery scope")
    return {
        **offline_processing_identity(),
        "prompt_version": COMPACT_REPORT_PROMPT,
        "preprocessing_version": (
            "uniform_page_map.v1"
            if discovery_scope == "page_map"
            else "full_page_chunks.v1"
        ),
        "wire_protocol": "company_profile_report_request.v3",
        "discovery_scope": discovery_scope,
    }


def preview_spans(text: str) -> tuple[tuple[int, int], ...]:
    """Uniform head/middle/end samples. Offsets do not depend on wording."""
    offsets = tuple(
        dict.fromkeys((0, max(0, len(text) // 2 - 8), max(0, len(text) - 16)))
    )
    return tuple(
        (offset, min(len(text), offset + (48 if offset == 0 else 16)))
        for offset in offsets
    )


def unsampled_ranges(text: str) -> tuple[tuple[int, int], ...]:
    """Contiguous body outside the structure preview. Empty when the page is fully sampled."""
    merged: list[list[int]] = []
    for start, end in preview_spans(text):
        if start >= end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    ranges: list[tuple[int, int]] = []
    cursor = 0
    for start, end in merged:
        if cursor < start:
            ranges.append((cursor, start))
        cursor = end
    if cursor < len(text):
        ranges.append((cursor, len(text)))
    return tuple(ranges)


def page_map(sources: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    """Index every page uniformly; samples are explicitly partial, never evidence.

    No company, chapter name, keyword or frozen acceptance answer affects which
    page is visible. Original offsets and text lengths expose what was omitted.
    Omitted spans stay eligible for a later condition-capable discovery request.
    """
    result = []
    for source in sources:
        text = source["text"]
        spans = preview_spans(text)
        result.append(
            {
                "page": source["page"],
                "readable": source["readable"],
                "source_characters": len(text),
                "scope": "structure_preview_only",
                "sample_offsets": [start for start, _ in spans],
                "text": "\n…\n".join(text[start:end] for start, end in spans),
            }
        )
    return tuple(result)


def compact_wire_bytes(
    source_pages: Sequence[Mapping[str, Any]],
    *,
    purpose: str,
    source_scope: str,
    report: ReportIdentity,
    identity: Mapping[str, Any],
    max_output_tokens: int,
) -> int:
    """UTF-8 size of the compact user payload, excluding gateway framing."""
    from .report_wire import compact_request

    request = ReportSemanticRequestV3(
        request_id="size-check",
        purpose=purpose,
        report=report,
        processing_identity=dict(identity),
        source_pages=tuple(source_pages),
        source_scope=source_scope,
        max_output_tokens=max_output_tokens,
        timeout_seconds=30,
    )
    return len(
        json.dumps(
            compact_request(request), ensure_ascii=False, separators=(",", ":")
        ).encode()
    )


def fits_card_reservation(
    source_pages: Sequence[Mapping[str, Any]],
    *,
    reservation_limit: int,
    purpose: str,
    source_scope: str,
    report: ReportIdentity,
    identity: Mapping[str, Any],
    max_output_tokens: int,
) -> bool:
    """True when this exact request's payload, framing and output cap fit the card."""
    if not source_pages:
        return False
    request = ReportSemanticRequestV3(
        request_id="card-fit",
        purpose=purpose,
        report=report,
        processing_identity=dict(identity),
        source_pages=tuple(source_pages),
        source_scope=source_scope,
        max_output_tokens=max_output_tokens,
        timeout_seconds=30,
    )
    return request_reservation(request) <= int(reservation_limit)


def fits_development_payload(
    source_pages: Sequence[Mapping[str, Any]], **kwargs
) -> bool:
    """Keep one gateway payload inside the byte cap and the caller's reservation.

    The public default remains one resend inside half of the 120000 ledger.
    A card can pass ``reservation_limit`` without changing that public cap.
    Callers still record every page and offset.
    """
    reservation_limit = int(
        kwargs.pop("reservation_limit", None) or DEVELOPMENT_RECOVERY_RESERVATION
    )
    estimated_payload = (
        compact_wire_bytes(source_pages, **kwargs) + DEVELOPMENT_WIRE_MARGIN
    )
    output = int(kwargs.get("max_output_tokens") or 0)
    return (
        estimated_payload <= DEVELOPMENT_PAYLOAD_BYTE_LIMIT
        and estimated_payload + 256 + output <= reservation_limit
    )


def cited_quote_pages(evidence: Sequence[Evidence]) -> tuple[dict[str, Any], ...]:
    """Pages made only of quotes already read, so synthesis does not resend bodies."""
    grouped: dict[int, list[str]] = {}
    for item in evidence:
        if not isinstance(item.anchor, TextAnchor) or not item.anchor.bounded_quote:
            continue
        quote = item.anchor.bounded_quote
        for page in (item.page, *item.continuation_pages, *item.subject_evidence_pages):
            bucket = grouped.setdefault(int(page), [])
            if quote not in bucket:
                bucket.append(quote)
    return tuple(
        {"page": page, "text": "\n".join(quotes), "readable": True}
        for page, quotes in sorted(grouped.items())
    )


def pack_fitting_batches(
    items: Sequence[Mapping[str, Any]],
    fits,
    *,
    scope_break: bool = False,
    split_mode: str = "midpoint",
) -> list[list[dict[str, Any]]]:
    """Greedy batches that preserve every character and every page."""

    def pieces_of(item: Mapping[str, Any]) -> list[dict[str, Any]]:
        fragment = dict(item)
        if fits([fragment]):
            return [fragment]
        text = str(fragment["text"])
        if split_mode == "boundary":
            best = 0
            low = 1
            high = len(text) - 1
            while low <= high:
                mid = (low + high) // 2
                cut = max(text.rfind(mark, 0, mid + 1) for mark in "\n。！？；")
                if cut <= 0:
                    low = mid + 1
                    continue
                end = cut + 1
                if fits([{**fragment, "text": text[:end]}]):
                    best = end
                    low = mid + 1
                else:
                    high = cut - 1
            if best <= 0:
                raise ValueError(
                    f"fragment_has_no_safe_boundary:{fragment.get('page')}"
                )
            left = {**fragment, "text": text[:best]}
            right = {
                **fragment,
                "text": text[best:],
                "offset": int(fragment.get("offset") or 0) + best,
            }
            return [left, *pieces_of(right)]
        if len(text) <= 1:
            raise ValueError("single_source_fragment_exceeds_payload_limit")
        midpoint = len(text) // 2
        left = {**fragment, "text": text[:midpoint]}
        right = {
            **fragment,
            "text": text[midpoint:],
            "offset": int(fragment.get("offset") or 0) + midpoint,
        }
        return pieces_of(left) + pieces_of(right)

    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for item in items:
        for piece in pieces_of(item):
            scope_changed = (
                scope_break
                and current
                and current[-1].get("body_scope") != piece.get("body_scope")
            )
            if current and (scope_changed or not fits([*current, piece])):
                batches.append(current)
                current = []
            current.append(piece)
    if current:
        batches.append(current)
    return batches


def structural_discovery_chunks(
    sources: Sequence[Mapping[str, Any]],
    full_page_ids: set[int],
    *,
    payload_fits=None,
) -> list[list[dict[str, Any]]]:
    """Pack full selected pages and the unsampled body of every other page.

    Page-map samples are not repeated here. No title, keyword or company identity
    chooses a span.
    """
    queued: list[dict[str, Any]] = []
    step = DISCOVERY_CHUNK_CHARS - DISCOVERY_CHUNK_OVERLAP

    def enqueue(page: Mapping[str, Any], start: int, end: int, body_scope: str) -> None:
        span = str(page["text"])[start:end]
        for local in range(0, len(span), step):
            queued.append(
                {
                    "page": page["page"],
                    "readable": True,
                    "text": span[local : local + DISCOVERY_CHUNK_CHARS],
                    "offset": start + local,
                    "body_scope": body_scope,
                }
            )
            if local + DISCOVERY_CHUNK_CHARS >= len(span):
                break

    for page in sources:
        if not page["readable"]:
            continue
        if page["page"] in full_page_ids:
            enqueue(page, 0, len(page["text"]), "full_page")
        else:
            for span_start, span_end in unsampled_ranges(str(page["text"])):
                enqueue(page, span_start, span_end, "unsampled_span")

    def fits(batch: Sequence[Mapping[str, Any]]) -> bool:
        if sum(len(str(item["text"])) for item in batch) > DISCOVERY_CHUNK_CHARS:
            return False
        return True if payload_fits is None else payload_fits(batch)

    return pack_fitting_batches(queued, fits, scope_break=False)


def _pages(pages: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    result = []
    seen = set()
    for item in pages:
        page = int(item["page"])
        if page < 1 or page in seen:
            raise ValueError("report pages must have unique positive physical numbers")
        seen.add(page)
        text = str(item.get("text") or "")
        result.append(
            {
                "page": page,
                "text": text,
                "readable": bool(item.get("readable", True)) and bool(text.strip()),
            }
        )
    return tuple(sorted(result, key=lambda item: item["page"]))


def _nonspace_index(text: str) -> tuple[str, list[int]]:
    chars: list[str] = []
    indexes: list[int] = []
    for index, char in enumerate(text):
        if char.isspace():
            continue
        chars.append(char)
        indexes.append(index)
    return "".join(chars), indexes


def _spans_in_text(quote: str, text: str) -> list[tuple[int, int]] | None:
    """Unique or ambiguous spans. None means the characters do not match.

    Exact source matches win. Otherwise only whitespace may differ. A different
    digit, punctuation mark or word is not a location.
    """
    if not quote.strip():
        return []
    exact: list[tuple[int, int]] = []
    start = 0
    while True:
        found = text.find(quote, start)
        if found < 0:
            break
        exact.append((found, found + len(quote)))
        start = found + 1
    if exact:
        return exact
    wanted, _ignored = _nonspace_index(quote)
    if not wanted:
        return []
    folded, indexes = _nonspace_index(text)
    spans: list[tuple[int, int]] = []
    start = 0
    while True:
        found = folded.find(wanted, start)
        if found < 0:
            break
        spans.append((indexes[found], indexes[found + len(wanted) - 1] + 1))
        start = found + 1
    return spans


def _line_spans(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    start = 0
    for index, char in enumerate(text):
        if char == "\n":
            spans.append((start, index))
            start = index + 1
    spans.append((start, len(text)))
    return spans


def _line_text(text: str, span: tuple[int, int]) -> str:
    return text[span[0] : span[1]].replace("\r", "").strip()


def _ends_sentence(line: str) -> bool:
    return bool(line) and line.endswith(("。", "！", "？"))


def _is_heading_line(line: str) -> bool:
    # A full PDF wrap is about forty characters and must stay inside the paragraph.
    return 1 < len(line) <= 20 and not line.endswith(("。", "；", "！", "？"))


def _bounded_governing_context(
    text: str, paragraph_start: int, paragraph_end: int, start: int, end: int
) -> str:
    """Keep the opening sentence and the quote's sentence inside a long paragraph."""
    body = text[paragraph_start:paragraph_end]
    opening_end = len(body)
    for mark in ("。", "！", "？"):
        found = body.find(mark)
        if 0 <= found < opening_end:
            opening_end = found + 1
    local_start = max(0, start - paragraph_start)
    local_end = max(local_start, end - paragraph_start)
    sentence_start = 0
    for mark in ("。", "！", "？"):
        found = body.rfind(mark, 0, local_start)
        if found >= sentence_start:
            sentence_start = found + 1
    sentence_end = len(body)
    for mark in ("。", "！", "？"):
        found = body.find(mark, local_end)
        if 0 <= found < sentence_end:
            sentence_end = found + 1
    if sentence_start <= opening_end:
        return body[:sentence_end].strip("\r\n ")
    return (body[:opening_end] + body[sentence_start:sentence_end]).strip("\r\n ")


def _heading_and_paragraph(text: str, start: int, end: int) -> tuple[str, str]:
    """Restore the paragraph that governs a quote.

    A single PDF newline is a wrap, not a paragraph boundary. Boundaries are a
    blank line, a short heading, or a line that already ended a sentence.
    """
    lines = _line_spans(text)
    index = 0
    for position, span in enumerate(lines):
        if span[0] <= start <= span[1]:
            index = position
            break
    para_start = index
    while para_start > 0:
        previous = _line_text(text, lines[para_start - 1])
        if not previous or _ends_sentence(previous) or _is_heading_line(previous):
            break
        para_start -= 1
    para_end = index
    while para_end + 1 < len(lines):
        current = _line_text(text, lines[para_end])
        following = _line_text(text, lines[para_end + 1])
        if not following or _ends_sentence(current) or _is_heading_line(following):
            break
        para_end += 1
    paragraph_start = lines[para_start][0]
    paragraph_end = lines[para_end][1]
    paragraph = text[paragraph_start:paragraph_end].strip("\r\n ")
    if len(paragraph) > 800:
        paragraph = _bounded_governing_context(
            text, paragraph_start, paragraph_end, start, end
        )
    heading = ""
    for span in reversed(lines[:para_start]):
        stripped = _line_text(text, span)
        if not stripped:
            if heading:
                break
            continue
        if _is_heading_line(stripped):
            heading = stripped
        break
    return heading, paragraph


def locate_source_quote(
    quote: str, pages: Sequence[Mapping[str, Any]], *, page_number: int
) -> tuple[str, int, str, str] | None:
    """Locate one quote inside the text that was actually sent.

    Returns the continuous source text, its page offset, a nearby heading and
    the containing paragraph. Multiple matches stay unlocated.
    """
    hits: list[tuple[str, int, str, str]] = []
    for fragment in pages:
        if int(fragment["page"]) != int(page_number):
            continue
        if fragment.get("readable", True) is False:
            continue
        text = str(fragment.get("text") or "")
        spans = _spans_in_text(quote, text)
        if not spans:
            continue
        base = int(fragment.get("offset") or 0)
        for start, end in spans:
            heading, paragraph = _heading_and_paragraph(text, start, end)
            hits.append((text[start:end], base + start, heading, paragraph))
    if len(hits) != 1:
        return None
    return hits[0]


def _bind_source_evidence(
    evidence: Evidence, report: ReportIdentity, pages: Sequence[Mapping[str, Any]]
) -> Evidence | None:
    """Backfill one sent-page location. Meaning is not decided here."""
    if evidence.report != report or not isinstance(evidence.anchor, TextAnchor):
        return None
    sent_pages = {int(page["page"]) for page in pages if page.get("readable", True)}
    if any(
        page not in sent_pages
        for page in (*evidence.continuation_pages, *evidence.subject_evidence_pages)
    ):
        return None
    model_quote = evidence.anchor.model_quote or evidence.anchor.bounded_quote
    located = locate_source_quote(model_quote, pages, page_number=evidence.page)
    if located is None:
        return None
    source, offset, heading, paragraph = located
    anchor = evidence.anchor.model_copy(
        update={
            "bounded_quote": source,
            "model_quote": None if model_quote == source else model_quote,
            "source_offset": offset,
            "context_before": paragraph or evidence.anchor.context_before,
        }
    )
    return evidence.model_copy(
        update={
            "anchor": anchor,
            "section_title": heading or evidence.section_title,
        }
    )


def _located_conditions(
    response_conditions: Sequence[ReportSourceCondition],
    report: ReportIdentity,
    pages: Sequence[Mapping[str, Any]],
    *,
    request_id: str,
    seen: set[str],
) -> tuple[list[ReportSourceCondition], list[str]]:
    """Keep conditions whose quotes locate. One bad quote does not drop the rest."""
    accepted: list[ReportSourceCondition] = []
    gaps: list[str] = []
    for condition in response_conditions:
        bound: list[Evidence] = []
        failed = False
        for evidence in condition.evidence:
            located = _bind_source_evidence(evidence, report, pages)
            if located is None:
                failed = True
                break
            bound.append(located)
        scoped_id = f"{request_id}:{condition.condition_id}"
        if failed:
            gaps.append(f"evidence_not_located:{scoped_id}")
            if condition.major:
                gaps.append(f"major_evidence_not_located:{scoped_id}")
            continue
        if scoped_id in seen:
            raise ValueError("duplicate_source_condition")
        seen.add(scoped_id)
        accepted.append(
            condition.model_copy(
                update={"condition_id": scoped_id, "evidence": tuple(bound)}
            )
        )
    return accepted, gaps


def _evidence_valid(
    evidence: Evidence, report: ReportIdentity, pages: Sequence[Mapping[str, Any]]
) -> bool:
    # Location is a hard constraint. Semantic support is a later verification.
    return _bind_source_evidence(evidence, report, pages) is not None


def interpret_report(
    *,
    report: ReportIdentity,
    pages: Sequence[Mapping[str, Any]],
    processing_identity: Mapping[str, Any],
    provider: Any,
    work_id: str,
    token_budget: int,
    started_at: float | None = None,
    structured_records: Sequence[SemanticRecord] = (),
    active_checklist: Sequence[ChecklistItem] = (),
    execution_budget: LlmRunBudget | None = None,
    on_progress: Any = None,
) -> ReportInterpretation:
    """Run source inventory → linked views → source-based verify within one report.

    This first slice deliberately does not reuse semantic outputs. Enabling real
    execution requires A0 plus physical-attempt/price admission; offline mode is
    executable now and cannot claim real semantic completion.
    """
    start = time.monotonic() if started_at is None else started_at
    if processing_identity.get("reading_strategy") == "direct_core_v1":
        from research.company_profile.direct_reading import interpret_direct_core

        return interpret_direct_core(
            report=report,
            pages=pages,
            processing_identity=processing_identity,
            provider=provider,
            work_id=work_id,
            token_budget=token_budget,
            started_at=start,
            structured_records=structured_records,
            execution_budget=execution_budget,
            on_progress=on_progress,
        )
    identity = dict(processing_identity)
    compact = identity.get("wire_protocol") == "company_profile_report_request.v3"
    structural = compact and identity.get("discovery_scope") == "page_map"
    map_checked = False
    mode = identity.get("execution_mode", "offline_fake")
    if mode not in {"offline_fake", "provider"}:
        raise ValueError("unknown report execution mode")
    sources = _pages(pages)
    coverage = [
        {
            "page": page["page"],
            "status": "not_read",
            "chunks": [],
            **({"structure_indexed": True, "map_checked": False} if structural else {}),
        }
        for page in sources
    ]
    gaps: list[str] = []
    conditions: list[ReportSourceCondition] = []
    structured = {
        item.record_id: item for item in structured_records if item.report == report
    }
    structured_evidence = {
        e.evidence_id: e for item in structured.values() for e in item.evidence
    }
    claims: tuple[ReportViewClaim, ...] = ()
    records: tuple[SemanticRecord, ...] = ()
    checks: tuple[ReportViewCheck, ...] = ()
    batch_settled = False
    dispositions: list[dict[str, Any]] = []
    quarantined: set[str] = set()
    receipts: list[dict[str, Any]] = []
    read_pages: set[int] = set()
    read_fragments: list[dict[str, Any]] = []
    necessary_pages: set[int] = set()
    tokens = 0
    seen_condition_ids: set[str] = set()
    limit = min(max(0, token_budget), 120_000)
    partial = bool(identity.get("partial_slice"))
    layered = identity.get("reading_strategy") == "layered_v3"
    partial_plan: dict[str, Any] | None = None
    request_cap = int(identity.get("max_physical_requests") or (8 if partial else 16))
    reservation_limit = int(identity.get("request_reservation_limit") or (24_000 if layered else 0))

    allowed = {
        (item.chapter_task, item.field_id): item
        for item in active_checklist
        if item.active
    }

    def record_scope_valid(record: SemanticRecord) -> bool:
        scope = allowed.get((record.chapter_task, record.field_id))
        if scope is None or scope.object_type.value != record.object_type:
            return False
        if isinstance(record, Measurement):
            return record.metric_type in scope.allowed_metric_types
        if isinstance(record, Activity):
            return record.action in scope.allowed_actions
        return True

    def fragment_identity(pages: Sequence[Mapping[str, Any]]) -> tuple:
        return tuple(
            (
                int(page["page"]),
                int(page.get("offset") or 0),
                str(page.get("scope") or page.get("body_scope") or ""),
                len(str(page.get("text") or "")) or int(page.get("length") or 0),
            )
            for page in pages
        )

    def reused_stage(purpose: str, source_pages, source_scope: str):
        # Layered reading does not reuse maps, conditions, synthesis or
        # verification from another run. Page identity is not semantic identity.
        if layered:
            return None
        wanted = fragment_identity(source_pages)
        for item in identity.get("resumed_receipts") or ():
            if item.get("status") != "success" or item.get("purpose") != purpose:
                continue
            if str(item.get("source_scope") or "full_context") != source_scope:
                continue
            saved_pages = item.get("source_fragments") or [
                {"page": page, "text": ""} for page in item.get("source_pages") or []
            ]
            saved_identity = fragment_identity(saved_pages)
            same_pages = [int(page) for page in item.get("source_pages") or []] == [
                int(page["page"]) for page in source_pages
            ]
            if saved_identity != wanted and not same_pages:
                continue
            return item
        return None

    def publish_progress() -> None:
        if on_progress is None:
            return
        on_progress(
            ReportInterpretation(
                report=report,
                processing_identity=identity,
                execution_mode=mode if mode in {"offline_fake", "provider"} else "offline_fake",
                page_coverage=tuple(coverage),
                source_conditions=tuple(conditions),
                claims=claims,
                records=records,
                checks=checks,
                record_dispositions=tuple(dispositions),
                call_receipts=tuple(receipts),
                gaps=tuple(gaps),
                tokens_charged=max(0, tokens),
                budget_ledger=(
                    execution_budget.snapshot() if execution_budget is not None else {}
                ),
                elapsed_seconds=max(0.0, time.monotonic() - start),
            )
        )

    def remember_read(purpose: str, source_scope: str, source_pages) -> None:
        if purpose == "source_inventory" and source_scope == "full_context":
            read_fragments.extend(dict(page) for page in source_pages)

    def invoke(purpose, source_pages, *, source_scope="full_context", **payload):
        nonlocal tokens
        saved = reused_stage(purpose, source_pages, source_scope)
        if saved is not None:
            payload_json = dict(saved.get("response_payload") or {})
            payload_json["request_id"] = f"{work_id}:report:{len(receipts) + 1}"
            payload_json["purpose"] = purpose
            response_type = (
                ReportSemanticResponseV3 if compact else ReportSemanticResponse
            )
            # Same JSON decoder as a live response. Strict mode still rejects
            # unknown fields and wrong JSON types; it does accept JSON arrays
            # as tuples. model_validate on the already-parsed dict does not.
            response = response_type.model_validate_json(
                json.dumps(payload_json, ensure_ascii=False)
            )
            receipts.append(
                {
                    **saved,
                    "request_id": response.request_id,
                    "physical_request": False,
                    "reused_stage": True,
                    "status": "success",
                }
            )
            remember_read(purpose, source_scope, source_pages)
            publish_progress()
            return response
        remaining = (
            execution_budget.remaining()
            if execution_budget
            else 900 - (time.monotonic() - start)
        )
        if (
            remaining <= 0
            or sum(1 for item in receipts if item.get("physical_request", True))
            >= request_cap
        ):
            raise ValueError("report_deadline_or_request_budget_exhausted")
        request_type = ReportSemanticRequestV3 if compact else ReportSemanticRequest
        request = request_type(
            request_id=f"{work_id}:report:{len(receipts) + 1}",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(source_pages),
            max_output_tokens=(
                (
                    1000
                    if source_scope == "page_map"
                    else 2500
                    if purpose != "verify_views"
                    else 2000
                )
                if compact
                else 4000
            ),
            timeout_seconds=float(remaining),
            **({"source_scope": source_scope} if compact else {}),
            **payload,
        )
        if partial and partial_plan is not None and execution_budget is not None:
            units = request_reservation(request)
            snapshot = execution_budget.snapshot()
            charged = int(snapshot["charged_tokens"])
            hold = send_admission_hold(
                partial_plan, snapshot["attempts"], purpose, units
            )
            if charged + units + hold > int(
                identity.get("ledger_tokens") or execution_budget.max_tokens
            ):
                raise ValueError("partial_tail_hold_would_be_exhausted")
            ceiling = partial_plan["ceilings"].get(purpose)
            if (
                purpose != "source_inventory"
                and ceiling is not None
                and units > ceiling
            ):
                gaps.append(f"stage_estimate_exceeded:{purpose}:{units}>{ceiling}")
        if layered and execution_budget is not None:
            units = request_reservation(request)
            if reservation_limit and units > reservation_limit:
                raise ValueError("card_request_reservation_exceeded")
            snapshot = execution_budget.snapshot()
            charged = int(snapshot["charged_tokens"])
            ledger = int(identity.get("ledger_tokens") or execution_budget.max_tokens)
            if charged + units > ledger:
                raise ValueError("partial_tail_hold_would_be_exhausted")
            # The first window can fill with body reads when a later window is
            # reserved for the tail. A tail window keeps one measured synthesis,
            # verification and recovery before sending more discovery.
            if identity.get("protect_tail", True) and purpose == "source_inventory":
                quote_pages = (
                    cited_quote_pages(
                        tuple(
                            evidence
                            for condition in conditions
                            for evidence in condition.evidence
                        )
                    )
                    if conditions
                    else tuple(source_pages)
                )
                related = records_for_fragments(
                    tuple(structured.values()), quote_pages or tuple(source_pages)
                )
                measured = stage_reservations(
                    quote_pages or tuple(source_pages),
                    report=report,
                    identity=identity,
                    conditions=tuple(conditions),
                    claims=tuple(claims),
                    records=related,
                )
                hold = send_admission_hold(
                    measured,
                    [
                        *getattr(execution_budget, "prior_logical_attempts", ()),
                        *snapshot["attempts"],
                    ],
                    purpose,
                    units,
                    recovery_policy=getattr(execution_budget, "recovery_policy", None),
                )
                if charged + units + hold > ledger:
                    raise ValueError("partial_tail_hold_would_be_exhausted")
        if compact and purpose != "source_inventory":
            from .report_wire import compact_request

            followup_wire = len(
                json.dumps(
                    compact_request(request),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            )
            estimated_followup = followup_wire + DEVELOPMENT_WIRE_MARGIN
            if (
                estimated_followup > DEVELOPMENT_PAYLOAD_BYTE_LIMIT
                or estimated_followup + 256 + request.max_output_tokens
                > DEVELOPMENT_RECOVERY_RESERVATION
            ):
                raise ValueError("followup_payload_exceeds_recovery_room")
        # UTF-8 bytes are a conservative preflight allowance for unknown usage.
        reserve = (
            len(request.model_dump_json().encode("utf-8")) + request.max_output_tokens
        )
        if execution_budget is None and tokens + reserve > limit:
            raise ValueError("report_token_budget_exhausted")
        if execution_budget is None:
            tokens += reserve
        before = len(getattr(provider, "traces", ()))
        receipt = {
            "request_id": request.request_id,
            "purpose": purpose,
            "status": "failed",
            "usage": None,
            "reserved_tokens": reserve,
            "prompt_version": identity.get("prompt_version"),
            "execution_mode": mode,
            "source_pages": [page["page"] for page in source_pages],
            "source_fragments": [
                {
                    "page": page["page"],
                    "offset": int(page.get("offset") or 0),
                    "scope": page.get("scope") or page.get("body_scope") or "",
                    "length": len(str(page.get("text") or "")),
                }
                for page in source_pages
            ],
            "source_scope": source_scope,
        }
        receipts.append(receipt)
        try:
            response = provider.interpret(request)
            response = (
                response
                if isinstance(response, ReportSemanticResponse)
                else (
                    ReportSemanticResponseV3 if compact else ReportSemanticResponse
                ).model_validate_json(json.dumps(response, ensure_ascii=False))
            )
            if response.request_id != request.request_id or response.purpose != purpose:
                raise ValueError("report_response_identity_mismatch")
            receipt["response_payload"] = response.model_dump(mode="json")
            receipt["status"] = "success"
            remember_read(purpose, source_scope, source_pages)
            publish_progress()
            return response
        finally:
            traces = tuple(getattr(provider, "traces", ()))[before:]
            receipt["gateway_traces"] = [
                (
                    item.model_dump(mode="json")
                    if hasattr(item, "model_dump")
                    else dict(item)
                )
                for item in traces
            ]
            if traces:
                total = [getattr(item, "total_tokens", None) for item in traces]
                if all(value is not None for value in total):
                    actual = sum(total)
                    receipt["usage"] = {"total_tokens": actual}
                    if execution_budget is None:
                        tokens += actual - reserve
            if execution_budget is not None:
                tokens = execution_budget.snapshot()["charged_tokens"]
            if tokens > limit:
                raise ValueError("report_token_budget_exceeded")

    try:
        if provider is None or not callable(getattr(provider, "interpret", None)):
            raise ValueError("semantic_provider_unavailable")
        # A fake provider must identify itself; a real gateway cannot accidentally
        # be called under an offline test identity. Real admission is a later A0 binding.
        if mode == "provider" and (
            execution_budget is None
            or getattr(provider, "run_budget", None) is not execution_budget
            or getattr(provider, "execution_authorization", {}).get("status")
            != "authorized"
        ):
            raise ValueError("a0_real_execution_binding_required")
        if mode == "offline_fake" and not getattr(provider, "offline_only", False):
            raise ValueError("offline_identity_requires_fake_provider")
        if identity.get("prompt_version") != (
            COMPACT_REPORT_PROMPT if compact else REPORT_PROMPT
        ):
            raise ValueError("unregistered_report_prompt_version")
        by_page = {item["page"]: item for item in coverage}
        if {p["page"] for p in sources} != set(
            range(1, max((p["page"] for p in sources), default=0) + 1)
        ):
            gaps.append("physical_pages_missing")
        discovery_sources = sources
        partial_fragments = None
        if structural and partial:
            raw_navigation = identity.get("navigation_receipts")
            if raw_navigation is None and identity.get("navigation_query"):
                raw_navigation = json.loads(
                    Path(identity["navigation_query"]).read_text()
                )["profiles"][0]["three_views"]["call_receipts"]
            accepted, requested = reused_map_receipts(
                raw_navigation or (),
                prompt_version=COMPACT_REPORT_PROMPT,
                page_count=len(sources),
            )
            for item in accepted:
                payload = item.get("response_payload") or {}
                chosen = set(payload.get("requested_pages") or [])
                for page in item.get("source_pages") or []:
                    by_page[int(page)].update(
                        structure_indexed=True,
                        map_checked=True,
                        map_selected=int(page) in chosen,
                        status="partially_checked",
                    )
                receipts.append(
                    {
                        **item,
                        "physical_request": False,
                        "reused_navigation": True,
                        "parent_attempt": identity.get("parent_attempt"),
                    }
                )
            map_checked = True
            plan = partial_slice_plan(
                sources,
                sorted(requested),
                report=report,
                identity=identity,
                ledger_units=int(identity.get("ledger_tokens") or 62_220),
                records=tuple(structured.values()),
                explicit_span=identity.get("diagnostic_span"),
            )
            spare = max(0, int(plan["ledger_units"]) - int(plan["total_units"]))
            partial_plan = {
                **plan,
                "ceilings": {
                    "source_inventory": plan["body_units"],
                    "synthesize_views": plan["synthesis_units"] + spare,
                    "verify_views": plan["verification_units"] + spare,
                },
            }
            partial_fragments = [plan["fragments"]]
            if plan.get("span_start") is not None:
                gaps.append(
                    "diagnostic_span:"
                    f"{plan['pages'][-1]}:{plan['span_start']}:{plan['tail_offset']}"
                )
            if plan["tail_offset"] is not None:
                gaps.append(
                    f"slice_tail_unread:{plan['pages'][-1]}:{plan['tail_offset']}"
                )
        elif structural and layered and (
            identity.get("navigation_receipts") or identity.get("navigation_query")
        ):
            raw_navigation = identity.get("navigation_receipts")
            if raw_navigation is None:
                raw_navigation = json.loads(
                    Path(identity["navigation_query"]).read_text()
                )["profiles"][0]["three_views"]["call_receipts"]
            accepted, requested = reused_map_receipts(
                raw_navigation or (),
                prompt_version=COMPACT_REPORT_PROMPT,
                page_count=len(sources),
            )
            for item in accepted:
                payload = item.get("response_payload") or {}
                chosen = set(payload.get("requested_pages") or [])
                for page in item.get("source_pages") or []:
                    by_page[int(page)].update(
                        structure_indexed=True,
                        map_checked=True,
                        map_selected=int(page) in chosen,
                        status="partially_checked",
                    )
                receipts.append(
                    {
                        **item,
                        "physical_request": False,
                        "reused_navigation": True,
                        "development_continuation": True,
                        "parent_attempt": identity.get("parent_attempt"),
                    }
                )
            map_checked = True
            discovery_sources = tuple(
                page
                for page in sources
                if page["readable"] and int(page["page"]) in requested
            )
        elif structural:
            requested: set[int] = set()
            available = {p["page"] for p in sources if p["readable"]}
            map_options = {
                "purpose": "source_inventory",
                "source_scope": "page_map",
                "report": report,
                "identity": identity,
                "max_output_tokens": 1000,
            }
            if layered and reservation_limit:
                map_options["reservation_limit"] = reservation_limit
            map_batches = pack_fitting_batches(
                page_map(sources),
                lambda batch: fits_development_payload(batch, **map_options),
                split_mode="boundary" if layered else "midpoint",
            )
            for batch in map_batches:
                response = invoke(
                    "source_inventory", tuple(batch), source_scope="page_map"
                )
                if response.source_conditions:
                    raise ValueError("page_map_cannot_establish_source_conditions")
                requested.update(response.requested_pages)
                # A later batch can fail. Keep this batch's preview and selection
                # without treating the sample as a full-text read.
                for page in batch:
                    by_page[page["page"]].update(
                        structure_indexed=True,
                        map_checked=True,
                        map_selected=page["page"] in response.requested_pages,
                        status="partially_checked",
                    )
            map_checked = all(
                by_page[page["page"]].get("map_checked") for page in sources
            )
            if not requested or not requested <= available:
                raise ValueError("page_map_requires_valid_full_context_selection")
            # Historical reading pulls native-fact pages into the body. Layered
            # reading keeps those facts in storage and does not mark them read
            # just because they exist.
            if not layered:
                requested |= {
                    page
                    for e in structured_evidence.values()
                    for page in (
                        e.page,
                        *e.continuation_pages,
                        *e.subject_evidence_pages,
                    )
                }
            if layered:
                discovery_sources = tuple(
                    p
                    for p in sources
                    if p["readable"] and int(p["page"]) in requested
                )
            else:
                discovery_sources = tuple(
                    p
                    for p in sources
                    if p["readable"] and any(abs(p["page"] - n) <= 1 for n in requested)
                )
        if not structural:
            for page in discovery_sources:
                if not page["readable"]:
                    by_page[page["page"]]["status"] = "extraction_failed"
                    gaps.append(f"page_unreadable:{page['page']}")
            full_ids = {page["page"] for page in discovery_sources if page["readable"]}
        else:
            # Historical page-map reading still samples every other page. The
            # layered contract reads the pages the navigation actually selected.
            full_ids = {page["page"] for page in discovery_sources}
        if partial_fragments is not None:
            chunks = partial_fragments
        elif layered:
            def layered_fit(batch, *, source_scope: str, max_output: int) -> bool:
                return fits_card_reservation(
                    batch,
                    reservation_limit=reservation_limit or 24_000,
                    purpose="source_inventory",
                    source_scope=source_scope,
                    report=report,
                    identity=identity,
                    max_output_tokens=max_output,
                )

            pieces: list[dict[str, Any]] = []
            for page in discovery_sources:
                whole = {
                    "page": page["page"],
                    "readable": True,
                    "text": page["text"],
                    "offset": 0,
                    "body_scope": "full_page",
                }
                try:
                    page_batches = pack_fitting_batches(
                        [whole],
                        lambda batch: layered_fit(
                            batch, source_scope="full_context", max_output=2500
                        ),
                        split_mode="boundary",
                    )
                except ValueError as exc:
                    if not str(exc).startswith("fragment_has_no_safe_boundary"):
                        raise
                    necessary_pages.add(int(page["page"]))
                    gaps.append(f"necessary_fragment_unread:{int(page['page'])}")
                    continue
                for batch in page_batches:
                    pieces.extend(batch)
            chunks = pack_fitting_batches(
                pieces,
                lambda batch: layered_fit(
                    batch, source_scope="full_context", max_output=2500
                ),
                split_mode="boundary",
            )
        else:
            chunks = structural_discovery_chunks(
                sources,
                full_ids,
                payload_fits=(
                    (
                        lambda batch: fits_development_payload(
                            batch,
                            purpose="source_inventory",
                            source_scope="full_context",
                            report=report,
                            identity=identity,
                            max_output_tokens=2500,
                        )
                    )
                    if compact
                    else None
                ),
            )
        full_expected = {page["page"]: 0 for page in sources}
        for chunk in chunks:
            for fragment in chunk:
                if fragment["body_scope"] == "full_page":
                    full_expected[fragment["page"]] += 1
        supplements = 0
        read_pages.clear()
        continuable = {
            "partial_tail_hold_would_be_exhausted",
            "card_request_reservation_exceeded",
            "logical_recovery_already_used",
            "card_recovery_exhausted",
        }

        def mark_necessary(chunk_fragments) -> None:
            for fragment in chunk_fragments:
                page_no = int(fragment["page"])
                necessary_pages.add(page_no)
                gap = f"necessary_unread:{page_no}"
                if gap not in gaps:
                    gaps.append(gap)

        for index, chunk in enumerate(chunks):
            try:
                response = invoke(
                    "source_inventory",
                    tuple(
                        {
                            "page": fragment["page"],
                            "text": fragment["text"],
                            "readable": True,
                            "offset": fragment["offset"],
                            "scope": fragment["body_scope"],
                        }
                        for fragment in chunk
                    ),
                )
            except (ValueError, LlmError) as exc:
                message = str(exc)
                # A hard stop keeps the pages already read and does not send the
                # rest. Their absence stays a necessary gap, then synthesis of
                # what was read can still be saved.
                if isinstance(exc, LlmError) and exc.code in {
                    "deadline_exceeded",
                    "cancelled",
                }:
                    for later in chunks[index:]:
                        mark_necessary(later)
                    break
                if message not in continuable and not (
                    isinstance(exc, LlmError)
                    and exc.code
                    not in {"deadline_exceeded", "cancelled", "configuration_error"}
                ):
                    raise
                mark_necessary(chunk)
                continue
            for fragment in chunk:
                page_state = by_page[fragment["page"]]
                page_state["chunks"].append(
                    {
                        "offset": fragment["offset"],
                        "length": len(fragment["text"]),
                        "scope": fragment["body_scope"],
                        "request_id": receipts[-1]["request_id"],
                    }
                )
                if fragment["body_scope"] == "unsampled_span":
                    page_state["residue_discovered"] = True
                    if page_state["status"] != "read":
                        page_state["status"] = "residue_discovered"
                else:
                    page_state["full_chunks"] = page_state.get("full_chunks", 0) + 1
                    if page_state["full_chunks"] == full_expected[fragment["page"]]:
                        page_state["status"] = "read"
                        read_pages.add(int(fragment["page"]))
            accepted, location_gaps = _located_conditions(
                response.source_conditions,
                report,
                chunk,
                request_id=receipts[-1]["request_id"],
                seen=seen_condition_ids,
            )
            gaps.extend(location_gaps)
            conditions.extend(accepted)
            if compact and not layered and response.requested_pages:
                pending = (
                    set(response.requested_pages)
                    - read_pages
                    - {
                        fragment["page"]
                        for later in chunks
                        for fragment in later
                        if fragment["body_scope"] == "full_page"
                    }
                )
                if not pending <= {p["page"] for p in sources if p["readable"]}:
                    raise ValueError("requested_source_page_unavailable")
                if pending and not partial:
                    if supplements >= 1:
                        raise ValueError("source_supplement_limit_reached")
                    supplements += 1
                    extra = [p for p in sources if p["page"] in pending]
                    chunks.append(
                        [
                            {
                                "page": p["page"],
                                "readable": True,
                                "text": p["text"],
                                "offset": 0,
                                "body_scope": "full_page",
                            }
                            for p in extra
                        ]
                    )
                    for p in extra:
                        full_expected[p["page"]] += 1
        if layered:
            unread_previews = [
                fragment
                for fragment in page_map(sources)
                if int(fragment["page"]) not in read_pages
            ]
            requested_extra: set[int] = set()
            if unread_previews:
                preview_batches = pack_fitting_batches(
                    unread_previews,
                    lambda batch: layered_fit(
                        batch, source_scope="page_map", max_output=1000
                    ),
                    split_mode="boundary",
                )
                for batch_index, batch in enumerate(preview_batches):
                    try:
                        gap_response = invoke(
                            "source_inventory",
                            tuple(batch),
                            source_scope="page_map",
                        )
                    except (ValueError, LlmError) as exc:
                        message = str(exc)
                        if (
                            not (
                                isinstance(exc, LlmError)
                                and exc.code in {"deadline_exceeded", "cancelled"}
                            )
                            and message not in continuable
                            and not (
                                isinstance(exc, LlmError)
                                and exc.code
                                not in {
                                    "deadline_exceeded",
                                    "cancelled",
                                    "configuration_error",
                                }
                            )
                        ):
                            raise
                        # Batches not sent are unfinished gap checks. They are
                        # not ordinary unread notes and cannot be skipped.
                        for later in preview_batches[batch_index:]:
                            for fragment in later:
                                gap = f"gap_check_incomplete:{int(fragment['page'])}"
                                if gap not in gaps:
                                    gaps.append(gap)
                        break
                    if gap_response.source_conditions:
                        raise ValueError("page_map_cannot_establish_source_conditions")
                    requested_extra.update(
                        int(page)
                        for page in gap_response.requested_pages
                        if int(page) not in read_pages
                    )
            extra_pages = [
                page
                for page in sources
                if page["readable"] and int(page["page"]) in requested_extra
            ]
            supplement_chunks: list[list[dict[str, Any]]] = []
            if extra_pages:
                supplement_pieces: list[dict[str, Any]] = []
                for page in extra_pages:
                    whole = {
                        "page": page["page"],
                        "readable": True,
                        "text": page["text"],
                        "offset": 0,
                        "body_scope": "full_page",
                    }
                    try:
                        page_batches = pack_fitting_batches(
                            [whole],
                            lambda batch: layered_fit(
                                batch, source_scope="full_context", max_output=2500
                            ),
                            split_mode="boundary",
                        )
                    except ValueError as exc:
                        if not str(exc).startswith("fragment_has_no_safe_boundary"):
                            raise
                        necessary_pages.add(int(page["page"]))
                        gaps.append(
                            f"necessary_fragment_unread:{int(page['page'])}"
                        )
                        continue
                    for batch in page_batches:
                        supplement_pieces.extend(batch)
                if supplement_pieces:
                    supplement_chunks = pack_fitting_batches(
                        supplement_pieces,
                        lambda batch: layered_fit(
                            batch, source_scope="full_context", max_output=2500
                        ),
                        split_mode="boundary",
                    )
            planned_extra: dict[int, int] = {}
            for chunk in supplement_chunks:
                for fragment in chunk:
                    page_no = int(fragment["page"])
                    planned_extra[page_no] = planned_extra.get(page_no, 0) + 1
            sent_extra: dict[int, int] = {}
            for index, chunk in enumerate(supplement_chunks):
                try:
                    response = invoke(
                        "source_inventory",
                        tuple(
                            {
                                "page": fragment["page"],
                                "text": fragment["text"],
                                "readable": True,
                                "offset": fragment["offset"],
                                "scope": fragment["body_scope"],
                            }
                            for fragment in chunk
                        ),
                    )
                except (ValueError, LlmError) as exc:
                    message = str(exc)
                    if message not in continuable and not (
                        isinstance(exc, LlmError)
                        and exc.code
                        not in {
                            "deadline_exceeded",
                            "cancelled",
                            "configuration_error",
                        }
                    ):
                        raise
                    for later in supplement_chunks[index:]:
                        mark_necessary(later)
                    break
                for fragment in chunk:
                    page_no = int(fragment["page"])
                    sent_extra[page_no] = sent_extra.get(page_no, 0) + 1
                    if sent_extra[page_no] == planned_extra[page_no]:
                        read_pages.add(page_no)
                        by_page[page_no]["status"] = "read"
                accepted, location_gaps = _located_conditions(
                    response.source_conditions,
                    report,
                    chunk,
                    request_id=receipts[-1]["request_id"],
                    seen=seen_condition_ids,
                )
                gaps.extend(location_gaps)
                conditions.extend(accepted)
            for page_no in requested_extra:
                if page_no not in read_pages:
                    necessary_pages.add(page_no)
                    gap = (
                        f"necessary_fragment_unread:{page_no}"
                        if any(
                            int(fragment["page"]) == page_no
                            for fragment in read_fragments
                        )
                        else f"necessary_unread:{page_no}"
                    )
                    if gap not in gaps:
                        gaps.append(gap)
            for source in sources:
                page_no = int(source["page"])
                if not source["readable"] or page_no in read_pages:
                    continue
                if page_no in necessary_pages or any(
                    gap == f"gap_check_incomplete:{page_no}" for gap in gaps
                ):
                    continue
                gaps.append(f"supplement_unread:{page_no}")
        related_records = tuple(structured.values())
        if layered:
            related_records = records_for_fragments(
                structured.values(), read_fragments
            )
            related_ids = {item.record_id for item in related_records}
            for record in structured.values():
                if record.record_id not in related_ids:
                    gaps.append(f"native_fact_outside_read_span:{record.record_id}")
        if partial and partial_fragments is not None:
            sent_fragments = [
                fragment for chunk in partial_fragments for fragment in chunk
            ]
            related_records = records_for_fragments(
                structured.values(), sent_fragments
            )
            related_ids = {item.record_id for item in related_records}
            for record in structured.values():
                if record.record_id not in related_ids:
                    gaps.append(f"native_fact_outside_read_span:{record.record_id}")
        # Full reading may follow citations onto adjacent pages. A bounded span
        # sends only quotes that sit inside the text already read.
        if partial and partial_fragments is not None:
            read_pages = {int(fragment["page"]) for fragment in sent_fragments}
            cited_evidence = []
            for item in (
                *(e for condition in conditions for e in condition.evidence),
                *(e for record in related_records for e in record.evidence),
            ):
                if not _evidence_in_fragments(item, sent_fragments):
                    continue
                cited_evidence.append(
                    item.model_copy(
                        update={
                            "continuation_pages": tuple(
                                page
                                for page in item.continuation_pages
                                if int(page) in read_pages
                            ),
                            "subject_evidence_pages": tuple(
                                page
                                for page in item.subject_evidence_pages
                                if int(page) in read_pages
                            ),
                        }
                    )
                )
            cited_evidence = tuple(cited_evidence)
            selected = tuple(
                {
                    "page": int(fragment["page"]),
                    "text": str(fragment["text"]),
                    "readable": True,
                }
                for fragment in sent_fragments
            )
        else:
            cited_evidence = (
                *(e for c in conditions for e in c.evidence),
                *structured_evidence.values(),
            )
            cited = {
                page
                for e in cited_evidence
                for page in (
                    e.page,
                    *e.continuation_pages,
                    *e.subject_evidence_pages,
                )
            }
            selected = tuple(
                p
                for p in sources
                if p["readable"]
                and any(abs(p["page"] - page) <= 1 for page in cited)
            )
        if not selected:
            raise ValueError("no_substantive_source_conditions")
        quote_pages = cited_quote_pages(cited_evidence)
        if not quote_pages:
            raise ValueError("no_substantive_source_conditions")
        # Discovery already sent every body span. Follow-up requests carry the
        # quotes and conditions, not the same pages again. A layered card
        # request stays within 24000 by grouping independent conditions.
        card_limit = reservation_limit or (24_000 if layered else 0)

        def followup_units(purpose: str, group, pages, claims_group=()) -> int:
            return request_reservation(
                ReportSemanticRequestV3(
                    request_id="followup-fit",
                    purpose=purpose,
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(pages),
                    source_scope="full_context",
                    source_conditions=tuple(group),
                    claims=tuple(claims_group),
                    records=related_records,
                    max_output_tokens=2500 if purpose != "verify_views" else 2000,
                    timeout_seconds=30,
                )
            )

        def groups_under_limit(items, purpose: str, claims_for=None):
            groups: list[list] = []
            current: list = []
            for item in items:
                trial = [*current, item]
                pages = cited_quote_pages(
                    tuple(evidence for condition in trial for evidence in condition.evidence)
                ) or quote_pages[:1]
                claims_group = claims_for(trial) if claims_for else ()
                if (
                    card_limit
                    and current
                    and followup_units(purpose, trial, pages, claims_group) > card_limit
                ):
                    groups.append(current)
                    current = [item]
                else:
                    current = trial
            if current:
                groups.append(current)
            return groups

        if layered and card_limit:
            batch_groups = groups_under_limit(conditions, "synthesize_views")
            claims, candidates = (), ()
        else:
            batch_groups = None
        if batch_groups is None:
            response = invoke(
                "synthesize_views",
                quote_pages,
                source_conditions=tuple(conditions),
                records=related_records,
                active_checklist=tuple(active_checklist),
            )
            claims, candidates = response.claims, response.records
        # Reliable native facts are inputs to verification and delivery, not a
        # model transcription obligation. Keep rejected variants separate below.
        condition_map = {item.condition_id: item for item in conditions}
        invalid_records = set()
        merged = {}
        for record in (*related_records, *candidates):
            scope_valid = record_scope_valid(record)
            original = structured.get(record.record_id)
            numeric_valid = (
                original == record
                if original is not None
                else not isinstance(record, Measurement)
                and record.source_native.value is None
                and record.source_native.unit is None
            )
            period_valid = (
                structured.get(record.record_id) == record
                or record.reported_period == report.report_period
                or report.document_type == "annual_report"
                and record.reported_period == report.report_period[:4]
                or any(
                    isinstance(e.anchor, TextAnchor)
                    and record.reported_period in e.anchor.bounded_quote
                    for e in record.evidence
                )
            )
            if (
                not scope_valid
                or not numeric_valid
                or record.report != report
                or not period_valid
                or not all(
                    _evidence_valid(e, report, selected)
                    or structured_evidence.get(e.evidence_id) == e
                    for e in record.evidence
                )
            ):
                invalid_records.add(record.record_id)
                gaps.append(f"invalid_record_identity_or_evidence:{record.record_id}")
                dispositions.append(
                    {
                        "record_id": record.record_id,
                        "status": "candidate_rejected",
                        "reason": "native_identity_value_or_evidence_changed",
                        "candidate": record.model_dump(mode="json"),
                        "original_preserved": original is not None,
                    }
                )
            else:
                merged[record.record_id] = record
        # Verify the preserved original too. A rejected variant must not mask a
        # genuine source conflict in the reliable fact under the same record ID.
        records = tuple(merged.values())
        record_map = {record.record_id: record for record in records}
        invalid_claims = set()
        outside_ids = set(structured) - {item.record_id for item in related_records}
        for claim in claims:
            if any(key in outside_ids for key in claim.supporting_record_ids):
                invalid_claims.add(claim.claim_id)
                gaps.append(f"claim_dependency_outside_read_span:{claim.claim_id}")
            if (
                not all(key in condition_map for key in claim.condition_ids)
                or not all(
                    key in record_map and key not in invalid_records
                    for key in claim.supporting_record_ids
                )
                or not all(_evidence_valid(e, report, selected) for e in claim.evidence)
                or claim.status == "observed"
                and (not claim.evidence or not claim.condition_ids)
            ):
                invalid_claims.add(claim.claim_id)
                gaps.append(f"invalid_claim_reference:{claim.claim_id}")
        request_claims = claims
        if partial and partial_fragments is not None:
            limited_claims = []
            for claim in claims:
                kept = tuple(
                    item
                    for item in claim.evidence
                    if _evidence_in_fragments(item, sent_fragments)
                )
                if len(kept) != len(claim.evidence):
                    invalid_claims.add(claim.claim_id)
                    gaps.append(f"claim_evidence_outside_read_span:{claim.claim_id}")
                    limited_claims.append(
                        claim.model_copy(
                            update={
                                "evidence": kept,
                                "status": "unclear",
                                "limitation": "evidence_outside_read_span",
                            }
                        )
                    )
                else:
                    limited_claims.append(claim)
            request_claims = tuple(limited_claims)
        # Verify has original source plus source-first conditions and the complete
        # draft. Rejected variants remain in dispositions, outside the fact set.
        def verify_units(group_claims, group_conditions, group_records) -> int:
            pages = cited_quote_pages(
                tuple(
                    evidence
                    for item in (*group_conditions, *group_claims)
                    for evidence in item.evidence
                )
            ) or quote_pages[:1]
            return request_reservation(
                ReportSemanticRequestV3(
                    request_id="verify-fit",
                    purpose="verify_views",
                    report=report,
                    processing_identity=identity,
                    source_pages=tuple(pages),
                    source_scope="full_context",
                    source_conditions=tuple(group_conditions),
                    claims=tuple(group_claims),
                    records=tuple(group_records),
                    max_output_tokens=2000,
                    timeout_seconds=30,
                )
            )

        def pack_objects(items, with_records=()):
            groups: list[list] = []
            current: list = []
            for item in items:
                trial = [*current, item]
                conds = [
                    condition
                    for condition in conditions
                    if condition.condition_id
                    in {key for claim in trial for key in claim.condition_ids}
                ] or list(conditions[:1] if not trial else [])
                if (
                    card_limit
                    and current
                    and verify_units(trial, conds, with_records) > card_limit
                ):
                    groups.append(current)
                    current = [item]
                else:
                    current = trial
            if current:
                groups.append(current)
            return groups

        if batch_groups is not None:
            settled: list[ReportViewClaim] = []
            settled_checks: list[ReportViewCheck] = []
            for group in batch_groups:
                pages = cited_quote_pages(
                    tuple(
                        evidence
                        for condition in group
                        for evidence in condition.evidence
                    )
                ) or quote_pages[:1]
                if followup_units("synthesize_views", group, pages) > card_limit:
                    gaps.append(
                        f"followup_exceeds_card_reservation:{group[0].condition_id}"
                    )
                    if group[0].major:
                        gaps.append(
                            f"major_evidence_not_located:{group[0].condition_id}"
                        )
                    continue
                if execution_budget is not None and settled:
                    charged = int(execution_budget.snapshot()["charged_tokens"])
                    ledger = int(
                        identity.get("ledger_tokens") or execution_budget.max_tokens
                    )
                    synth_units = followup_units("synthesize_views", group, pages)
                    verify_estimate = verify_units((), group, ())
                    if charged + synth_units + verify_estimate + synth_units > ledger:
                        gaps.append("batch_verification_hold")
                        break
                try:
                    response = invoke(
                        "synthesize_views",
                        pages,
                        source_conditions=tuple(group),
                        records=related_records,
                        active_checklist=tuple(active_checklist),
                    )
                except (SemanticProviderError, LlmError, ValueError, TypeError) as exc:
                    gaps.append(f"semantic_incomplete:{str(exc)[:240]}")
                    break
                claims = (
                    *settled,
                    *(
                        claim.model_copy(
                            update={
                                "status": "unclear",
                                "limitation": "pending_verification",
                            }
                        )
                        for claim in response.claims
                    ),
                )
                publish_progress()
                try:
                    referenced_records = tuple(
                        record_map[key]
                        for claim in response.claims
                        for key in claim.supporting_record_ids
                        if key in record_map and key not in invalid_records
                    )
                    verified = invoke(
                        "verify_views",
                        pages,
                        source_conditions=tuple(group),
                        claims=tuple(response.claims),
                        records=referenced_records,
                        active_checklist=tuple(active_checklist),
                    )
                except (SemanticProviderError, LlmError, ValueError, TypeError) as exc:
                    gaps.append(f"semantic_incomplete:{str(exc)[:240]}")
                    break
                verdicts = {
                    (item.target_type, item.target_id): item.status
                    for item in verified.checks
                }
                targets = (
                    {("condition", item.condition_id) for item in group}
                    | {("claim", item.claim_id) for item in response.claims}
                    | {("record", item.record_id) for item in referenced_records}
                )
                if {
                    (item.target_type, item.target_id) for item in verified.checks
                } != targets:
                    gaps.append("semantic_verify_targets_missing_or_extra")
                    break
                group_ids = {item.condition_id for item in group}
                promoted: list[ReportViewClaim] = []
                for claim in response.claims:
                    blocked = (
                        verdicts.get(("claim", claim.claim_id)) != "pass"
                        or any(
                            verdicts.get(("condition", key)) != "pass"
                            for key in claim.condition_ids
                        )
                        or not claim.evidence
                        or not claim.condition_ids
                        or not set(claim.condition_ids) <= group_ids
                        or any(
                            verdicts.get(("record", key)) != "pass"
                            for key in claim.supporting_record_ids
                        )
                        or not all(
                            key in record_map and key not in invalid_records
                            for key in claim.supporting_record_ids
                        )
                        or any(key in outside_ids for key in claim.supporting_record_ids)
                        or not all(
                            _evidence_valid(evidence, report, pages)
                            for evidence in claim.evidence
                        )
                    )
                    if blocked:
                        promoted.append(
                            claim.model_copy(
                                update={
                                    "status": "unclear",
                                    "answer": "结论未通过证据或语义核验。",
                                    "limitation": "claim_or_dependency_not_verified",
                                }
                            )
                        )
                        gaps.append(f"claim_not_verified:{claim.claim_id}")
                    else:
                        promoted.append(claim)
                settled.extend(promoted)
                settled_checks.extend(verified.checks)
                claims = tuple(settled)
                checks = tuple(settled_checks)
                try:
                    publish_progress()
                except (OSError, ValueError) as exc:
                    gaps.append(f"publication_not_saved:{type(exc).__name__}")
                    break
            batch_settled = True
        if not batch_settled and layered and card_limit and (
            verify_units(request_claims, conditions, records) > card_limit
        ):
            check_rows = []
            for group in pack_objects(request_claims):
                conds = tuple(
                    condition
                    for condition in conditions
                    if condition.condition_id
                    in {key for claim in group for key in claim.condition_ids}
                )
                pages = cited_quote_pages(
                    tuple(
                        evidence
                        for item in (*conds, *group)
                        for evidence in item.evidence
                    )
                ) or quote_pages[:1]
                response = invoke(
                    "verify_views",
                    pages,
                    source_conditions=conds,
                    claims=tuple(group),
                    records=(),
                    active_checklist=tuple(active_checklist),
                )
                check_rows.extend(response.checks)
            record_groups: list[list] = []
            current_records: list = []
            for record in records:
                trial = [*current_records, record]
                if (
                    card_limit
                    and current_records
                    and verify_units((), (), trial) > card_limit
                ):
                    record_groups.append(current_records)
                    current_records = [record]
                else:
                    current_records = trial
            if current_records:
                record_groups.append(current_records)
            for group in record_groups:
                if not group:
                    continue
                response = invoke(
                    "verify_views",
                    quote_pages[:1],
                    source_conditions=(),
                    claims=(),
                    records=tuple(group),
                    active_checklist=tuple(active_checklist),
                )
                check_rows.extend(response.checks)
            checks = tuple(check_rows)
        elif not batch_settled:
            response = invoke(
                "verify_views",
                quote_pages,
                source_conditions=tuple(conditions),
                claims=request_claims,
                records=records,
                active_checklist=tuple(active_checklist),
            )
            checks = response.checks
        # Retain explicit conflicts even if another verification target is missing.
        # Pipeline failure may keep reliable independent facts, never these facts.
        for check in checks:
            if (
                check.target_type == "record"
                and check.status != "pass"
                and check.target_id in record_map
            ):
                key = check.target_id
                quarantined.add(key)
                gaps.append(f"record_semantic_conflict:{key}")
                dispositions.append(
                    {
                        "record_id": key,
                        "status": "quarantined",
                        "reason": check.explanation,
                        "candidate": record_map[key].model_dump(mode="json"),
                        "original_preserved": False,
                    }
                )
        targets = (
            {("condition", item.condition_id) for item in conditions}
            | {("claim", item.claim_id) for item in claims}
            | {("record", item.record_id) for item in records}
        )
        if (
            not batch_settled
            and {(item.target_type, item.target_id) for item in checks} != targets
        ):
            raise ValueError("semantic_verify_targets_missing_or_extra")
        if batch_settled:
            verdicts = {
                (item.target_type, item.target_id): item.status for item in checks
            }
        else:
            verdicts = {
                (item.target_type, item.target_id): item.status for item in checks
            }
        accepted = {}
        for record in records:
            key = record.record_id
            if key in quarantined:
                continue
            accepted[key] = record
        records = tuple(accepted.values())
        accepted_ids = {item.record_id for item in records}
        safe_claims = []
        for claim in claims:
            dependency_blocked = bool(claim.supporting_record_ids) and not set(
                claim.supporting_record_ids
            ) <= accepted_ids
            if batch_settled and not dependency_blocked:
                safe_claims.append(claim)
                continue
            if batch_settled and dependency_blocked:
                safe_claims.append(
                    claim.model_copy(
                        update={
                            "status": "unclear",
                            "answer": "结论未通过证据或语义核验。",
                            "limitation": "claim_or_dependency_not_verified",
                        }
                    )
                )
                gaps.append(f"claim_not_verified:{claim.claim_id}")
                continue
            blocked = (
                claim.claim_id in invalid_claims
                or verdicts[("claim", claim.claim_id)] != "pass"
                or not set(claim.supporting_record_ids) <= accepted_ids
                or any(
                    verdicts.get(("condition", key)) != "pass"
                    for key in claim.condition_ids
                )
            )
            if blocked:
                safe_claims.append(
                    claim.model_copy(
                        update={
                            "status": "unclear",
                            "answer": "结论未通过证据或语义核验。",
                            "limitation": "claim_or_dependency_not_verified",
                        }
                    )
                )
                gaps.append(f"claim_not_verified:{claim.claim_id}")
            else:
                safe_claims.append(claim)
        claims = tuple(safe_claims)
        for condition in conditions:
            if condition.major and verdicts.get(("condition", condition.condition_id)) != "pass":
                gaps.append(f"major_condition_not_delivered:{condition.condition_id}")
        claimed_ids = {key for claim in claims for key in claim.condition_ids}
        for condition in conditions:
            if condition.condition_id not in claimed_ids:
                gaps.append(f"condition_not_synthesized:{condition.condition_id}")
        if partial and partial_plan is not None:
            committed = (conditions, claims, records, checks)
            remaining_ids = partial_plan.get("unread_selected_pages") or []
            if remaining_ids and identity.get("continue_after_verify"):
                try:
                    continuation = partial_slice_plan(
                        sources,
                        remaining_ids,
                        report=report,
                        identity=identity,
                        ledger_units=int(identity.get("ledger_tokens") or 62_220),
                        records=related_records,
                    )
                    charged_now = (
                        execution_budget.snapshot()["charged_tokens"]
                        if execution_budget is not None
                        else tokens
                    )
                    if (
                        charged_now + continuation["total_units"]
                        <= int(identity.get("ledger_tokens") or limit)
                        and sum(
                            1 for item in receipts if item.get("physical_request", True)
                        )
                        + 3
                        <= request_cap
                    ):
                        invoke(
                            "source_inventory",
                            tuple(continuation["fragments"]),
                        )
                except (SemanticProviderError, LlmError, ValueError, TypeError) as exc:
                    conditions, claims, records, checks = committed
                    gaps.append(f"semantic_incomplete:{str(exc)[:240]}")
    except (SemanticProviderError, LlmError, ValueError, TypeError) as exc:
        gaps.append(f"semantic_incomplete:{str(exc)[:240]}")
        if layered:
            verified = {
                item.target_id
                for item in checks
                if item.target_type == "claim" and item.status == "pass"
            }
            checked = {
                item.target_id
                for item in checks
                if item.target_type == "claim"
            }
            claims = tuple(
                claim
                if claim.claim_id in checked
                or claim.claim_id in verified
                or claim.limitation == "pending_verification"
                else claim.model_copy(
                    update={
                        "status": "unclear",
                        "answer": "语义流程未完成。",
                        "limitation": "semantic_pipeline_incomplete",
                    }
                )
                for claim in claims
            )
        elif not partial:
            records = tuple(
                item
                for item in structured.values()
                if record_scope_valid(item) and item.record_id not in quarantined
            )
            claims = tuple(
                claim.model_copy(
                    update={
                        "status": "unclear",
                        "answer": "语义流程未完成。",
                        "limitation": "semantic_pipeline_incomplete",
                    }
                )
                for claim in claims
            )
        else:
            if not checks:
                records = ()
            claims = tuple(
                claim.model_copy(
                    update={
                        "status": "unclear",
                        "answer": "语义流程未完成。",
                        "limitation": "semantic_pipeline_incomplete",
                    }
                )
                for claim in claims
            )

    states = {item["page"]: item for item in coverage}
    if structural and not layered:
        for source in sources:
            state = states[source["page"]]
            if (
                source["readable"]
                and unsampled_ranges(source["text"])
                and state["status"] != "read"
                and not state.get("residue_discovered")
            ):
                gaps.append(f"unsampled_body_not_discovered:{source['page']}")

    def structurally_covered(source: Mapping[str, Any]) -> bool:
        state = states[source["page"]]
        if state["status"] == "read":
            return True
        if layered and int(source["page"]) in necessary_pages:
            return False
        if layered and int(source["page"]) not in read_pages:
            return bool(state.get("map_checked"))
        if unsampled_ranges(source["text"]):
            return bool(state.get("residue_discovered"))
        return bool(state.get("map_checked"))

    valid_dimensions = []
    all_pages_read = (
        bool(coverage)
        and "physical_pages_missing" not in gaps
        and (
            all(page["status"] == "read" for page in coverage)
            if not structural
            else map_checked
            and bool(conditions)
            and not any(gap.startswith("semantic_incomplete:") for gap in gaps)
            and not any(
                gap.startswith("unsampled_body_not_discovered:") for gap in gaps
            )
            and not any(
                gap.startswith(
                    (
                        "necessary_unread:",
                        "necessary_fragment_unread:",
                        "gap_check_incomplete:",
                        "major_evidence_not_located:",
                    )
                )
                for gap in gaps
            )
            and all(p["readable"] for p in sources)
            and all(structurally_covered(source) for source in sources)
        )
    )
    verdicts = {(item.target_type, item.target_id): item.status for item in checks}
    for dimension in DIMENSIONS:
        dimension_claims = [item for item in claims if item.dimension == dimension]
        major = [
            item for item in conditions if item.dimension == dimension and item.major
        ]
        delivered_ids = {
            key
            for item in dimension_claims
            if item.status == "observed"
            for key in item.condition_ids
        }
        if (
            all_pages_read
            and dimension_claims
            and all(
                item.status in {"observed", "not_disclosed", "not_applicable"}
                for item in dimension_claims
            )
            and all(
                item.condition_id in delivered_ids
                and verdicts.get(("condition", item.condition_id)) == "pass"
                for item in major
            )
            and (
                dimension not in DIMENSIONS[:4] + ("supply_chain_role",)
                or any(item.status == "observed" for item in dimension_claims)
            )
        ):
            valid_dimensions.append(dimension)
    complete = all(dimension in valid_dimensions for dimension in DIMENSIONS)
    return ReportInterpretation(
        report=report,
        processing_identity=identity,
        execution_mode=mode,
        page_coverage=tuple(coverage),
        source_conditions=tuple(conditions),
        claims=claims,
        records=records,
        checks=checks,
        record_dispositions=tuple(dispositions),
        call_receipts=tuple(receipts),
        gaps=tuple(gaps),
        verified_dimensions=tuple(valid_dimensions),
        draft_complete=complete,
        semantic_complete=complete and mode == "provider",
        real_semantic_complete=complete and mode == "provider",
        tokens_charged=max(0, tokens),
        budget_ledger=execution_budget.snapshot() if execution_budget else {},
        elapsed_seconds=max(0.0, time.monotonic() - start),
    )


def core_assessment(result: ReportInterpretation) -> CompanyProfileCoreAssessmentV2:
    dimensions = {}
    for dimension in DIMENSIONS[:3]:
        claims = [
            item
            for item in result.claims
            if item.dimension == dimension and item.status == "observed"
        ]
        answered = dimension in result.verified_dimensions
        dimensions[dimension] = CoreDimensionAssessment(
            dimension_id=dimension,
            answered=answered,
            supporting_record_ids=tuple(
                dict.fromkeys(
                    key for item in claims for key in item.supporting_record_ids
                )
            ),
            evidence_ids=tuple(
                dict.fromkeys(e.evidence_id for item in claims for e in item.evidence)
            ),
            excerpt="\n".join(item.answer for item in claims) if answered else None,
            missing_reason=None if answered else "no_qualifying_source",
        )
    return CompanyProfileCoreAssessmentV2(
        mapping_version="company_profile_common_core_mapping.v2",
        report=result.report,
        **dimensions,
        core_complete=result.real_semantic_complete
        and all(item.answered for item in dimensions.values()),
    )


def research_views(result: ReportInterpretation) -> dict[str, Any]:
    """Render the same persisted projection for query and export."""
    views = {}
    for name, dimensions in VIEWS.items():
        claims = [item for item in result.claims if item.dimension in dimensions]
        views[name] = {
            "assessment_status": (
                "assessed"
                if all(d in result.verified_dimensions for d in dimensions)
                else "extraction_failed"
            ),
            "claims": [
                {
                    **item.model_dump(mode="json"),
                    "review_state": (
                        "verified"
                        if (
                            item.status == "observed"
                            and item.claim_id
                            in {
                                check.target_id
                                for check in result.checks
                                if check.target_type == "claim" and check.status == "pass"
                            }
                            and item.limitation
                            not in {
                                "pending_verification",
                                "claim_or_dependency_not_verified",
                                "evidence_not_located",
                            }
                        )
                        else "pending_verification"
                        if item.status == "observed"
                        else "not_reliable"
                    ),
                }
                for item in claims
            ],
            "checked_pages": [item for item in result.page_coverage],
        }
    return {
        "schema_version": "company_profile_three_views.v2",
        "views": views,
        "semantic_complete": result.semantic_complete,
        "draft_complete": result.draft_complete,
        "real_semantic_complete": result.real_semantic_complete,
        "execution_mode": result.execution_mode,
        "gaps": list(result.gaps),
        "call_receipts": list(result.call_receipts),
        "record_dispositions": list(result.record_dispositions),
        "budget_ledger": result.budget_ledger,
        "classification": (
            "offline_fixture"
            if result.execution_mode == "offline_fake"
            else (
                "limited"
                if not result.semantic_complete
                else "pending_independent_review"
            )
        ),
    }
