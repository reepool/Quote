"""Reservation plans for a bounded report slice and the full-report gap.

Units are UTF-8 gateway payload bytes plus 256 bytes of framing plus the
requested output cap. They are not a tokenizer reading and not a fee.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from research.company_profile.contracts import ReportSemanticRequestV3
from research.company_profile.report_wire import (
    compact_request,
    instructions,
    response_schema,
)
from utils.llm import LlmClient, LlmConfig

PARTIAL_LEDGER_UNITS = 62_220


_PROFILE_CACHE = None


def _profiles():
    global _PROFILE_CACHE
    if _PROFILE_CACHE is None:
        config = LlmConfig.from_mapping(
            json.loads(Path("config/13_llm.json").read_text())["llm"]
        )
        _PROFILE_CACHE = tuple(config.concrete_profiles_for("semantic_extraction"))
    return _PROFILE_CACHE


def reservation_units(payload: Mapping[str, Any], max_output: int) -> int:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    return len(encoded) + 256 + max_output


def request_reservation(request: ReportSemanticRequestV3) -> int:
    """Largest physical reservation of this request across the current pool."""
    bounds = []
    for profile in _profiles():
        payload = LlmClient._build_payload(
            model=profile.model,
            messages=[
                {"role": "system", "content": instructions(request)},
                {
                    "role": "user",
                    "content": json.dumps(
                        compact_request(request),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                },
            ],
            schema=response_schema(request),
            schema_name="company_profile_report_response",
            mode="json_object",
            temperature=0,
            max_output_tokens=request.max_output_tokens,
            max_output_tokens_field=profile.max_output_tokens_field,
            stream=profile.stream,
            stream_include_usage=profile.stream_include_usage,
        )
        output = payload.get("max_completion_tokens", payload.get("max_tokens"))
        bounds.append(reservation_units(payload, int(output)))
    return max(bounds)


def contiguous_runs(page_ids: Sequence[int]) -> list[list[int]]:
    ordered = sorted({int(page) for page in page_ids})
    runs: list[list[int]] = []
    current: list[int] = []
    for page in ordered:
        if current and page != current[-1] + 1:
            runs.append(current)
            current = []
        current.append(page)
    if current:
        runs.append(current)
    return runs


def trim_incomplete_tail(text: str) -> str:
    """Drop a dangling heading or unfinished sentence at the end of a slice."""
    if not text:
        return text
    if text.rstrip()[-1:] in "。！？":
        return text
    cut = max(text.rfind(mark) for mark in "。！？")
    if cut < 0:
        return ""
    return text[: cut + 1]


def _request(
    pages: Sequence[Mapping[str, Any]],
    *,
    purpose: str,
    report: Any,
    identity: Mapping[str, Any],
    max_output: int,
    **extra: Any,
) -> ReportSemanticRequestV3:
    return ReportSemanticRequestV3(
        request_id="partial-size",
        purpose=purpose,
        report=report,
        processing_identity=dict(identity),
        source_pages=tuple(pages),
        source_scope="full_context",
        max_output_tokens=max_output,
        timeout_seconds=30,
        **extra,
    )


def _recovery_consumed(item: Mapping[str, Any]) -> bool:
    """One window shares one recovery.

    A non-success status consumes it. So does a later physical send of the
    same request: HTTP 200 followed by schema repair is recorded as another
    success with ``attempt`` greater than 1, and that second send is the
    recovery. It is not restored just because both rows say success.
    """
    status = str(item.get("status") or "")
    if status not in {"", "reserved", "success"}:
        return True
    try:
        attempt = int(item.get("attempt") or 1)
    except (TypeError, ValueError):
        attempt = 1
    return attempt > 1


def remaining_tail_units(
    plan: Mapping[str, int], attempts: Sequence[Mapping[str, Any]], purpose: str
) -> int:
    """Planning hold: later stage estimates plus the planned recovery figure.

    This is a forecast. It is not the send decision for a request whose actual
    payload is already known.
    """
    recovery = (
        0
        if any(_recovery_consumed(item) for item in attempts)
        else int(plan["recovery_units"])
    )
    if purpose == "source_inventory":
        return int(plan["synthesis_units"]) + int(plan["verification_units"]) + recovery
    if purpose == "synthesize_views":
        return int(plan["verification_units"]) + recovery
    return 0


def send_admission_hold(
    plan: Mapping[str, int],
    attempts: Sequence[Mapping[str, Any]],
    purpose: str,
    current_units: int,
) -> int:
    """Future stage estimates plus one recovery of the request about to be sent.

    Recovery follows the actual payload. A stale plan figure is not added on
    top, and a failure that already consumed the shared recovery does not
    reserve a second one. The earlier stage estimate is not part of this hold.
    """
    recovery = 0 if any(_recovery_consumed(item) for item in attempts) else int(current_units)
    if purpose == "source_inventory":
        return int(plan["synthesis_units"]) + int(plan["verification_units"]) + recovery
    if purpose == "synthesize_views":
        return int(plan["verification_units"]) + recovery
    return recovery


def _evidence_in_fragments(evidence: Any, fragments: Sequence[Mapping[str, Any]]) -> bool:
    """A fact is in this read when its quote sits inside a fragment actually sent."""
    page = getattr(evidence, "page", None)
    anchor = getattr(evidence, "anchor", None)
    quote = getattr(anchor, "bounded_quote", None) if anchor is not None else None
    if page is None or not quote:
        return False
    return any(
        int(fragment["page"]) == int(page) and str(quote) in str(fragment["text"])
        for fragment in fragments
    )


def records_for_fragments(
    records: Sequence[Any], fragments: Sequence[Mapping[str, Any]]
) -> tuple[Any, ...]:
    """Native facts whose evidence is inside the read span.

    Facts cited only on other pages stay out of this slice. Callers keep the
    original records in storage and do not mark them verified.
    """
    return tuple(
        record
        for record in records
        if any(
            _evidence_in_fragments(item, fragments)
            for item in (getattr(record, "evidence", ()) or ())
        )
    )


def executable_reservations(
    fragments: Sequence[Mapping[str, Any]],
    *,
    report: Any,
    identity: Mapping[str, Any],
    records: Sequence[Any] = (),
) -> dict[str, int]:
    """Send limit for one span: measured payload plus one copy of the read text.

    The shell is the request that will actually be built for this span, including
    its schema, instructions and output cap. The extra citation bytes are the
    UTF-8 size of the text just read, which is the most quote text a later
    request can add after duplicate quotes collapse. This is not a simulated
    condition count and not an empty follow-up. A later real response that
    exceeds the ceiling is kept and not forwarded.
    """
    citation = sum(len(str(item.get("text") or "").encode()) for item in fragments)
    related = records_for_fragments(records, fragments)
    body = request_reservation(
        _request(
            fragments,
            purpose="source_inventory",
            report=report,
            identity=identity,
            max_output=2500,
        )
    )
    synthesis_shell = request_reservation(
        _request(
            fragments,
            purpose="synthesize_views",
            report=report,
            identity=identity,
            max_output=2500,
            records=related,
        )
    )
    verification_shell = request_reservation(
        _request(
            fragments,
            purpose="verify_views",
            report=report,
            identity=identity,
            max_output=2000,
            records=related,
        )
    )
    synthesis = synthesis_shell + citation
    verification = verification_shell + citation
    recovery = max(body, synthesis, verification)
    return {
        "body_units": body,
        "synthesis_shell_units": synthesis_shell,
        "verification_shell_units": verification_shell,
        "citation_bytes": citation,
        "synthesis_units": synthesis,
        "verification_units": verification,
        "recovery_units": recovery,
        "total_units": body + synthesis + verification + recovery,
        "record_count": len(related),
    }


def stage_reservations(
    pages: Sequence[Mapping[str, Any]],
    *,
    report: Any,
    identity: Mapping[str, Any],
    conditions: Sequence[Any] = (),
    claims: Sequence[Any] = (),
    records: Sequence[Any] = (),
) -> dict[str, int]:
    """Measure one concrete request. Callers pass the objects they will send."""
    body = request_reservation(
        _request(
            pages,
            purpose="source_inventory",
            report=report,
            identity=identity,
            max_output=2500,
        )
    )
    synthesis = request_reservation(
        _request(
            pages,
            purpose="synthesize_views",
            report=report,
            identity=identity,
            max_output=2500,
            source_conditions=tuple(conditions),
            records=tuple(records),
        )
    )
    verification = request_reservation(
        _request(
            pages,
            purpose="verify_views",
            report=report,
            identity=identity,
            max_output=2000,
            source_conditions=tuple(conditions),
            claims=tuple(claims),
            records=tuple(records),
        )
    )
    recovery = max(body, synthesis, verification)
    return {
        "body_units": body,
        "synthesis_units": synthesis,
        "verification_units": verification,
        "recovery_units": recovery,
        "total_units": body + synthesis + verification + recovery,
        "condition_count": len(tuple(conditions)),
        "claim_count": len(tuple(claims)),
        "record_count": len(tuple(records)),
    }


def reused_map_receipts(
    receipts: Sequence[Mapping[str, Any]],
    *,
    prompt_version: str,
    page_count: int,
) -> tuple[list[dict[str, Any]], set[int]]:
    """Map receipts may guide a later slice. They are not body evidence."""
    selected: set[int] = set()
    accepted = []
    for item in receipts:
        if item.get("source_scope") != "page_map":
            continue
        if item.get("status") != "success":
            raise ValueError("navigation_map_batch_did_not_succeed")
        if item.get("prompt_version") != prompt_version:
            raise ValueError("navigation_prompt_mismatch")
        payload = item.get("response_payload") or {}
        if payload.get("purpose") not in {None, "source_inventory"}:
            raise ValueError("navigation_purpose_mismatch")
        if (
            payload.get("source_conditions")
            or payload.get("claims")
            or payload.get("records")
        ):
            raise ValueError("navigation_cannot_supply_body_evidence")
        pages = [int(page) for page in item.get("source_pages") or []]
        if not pages or any(page < 1 or page > page_count for page in pages):
            raise ValueError("navigation_page_outside_report")
        requested = [int(page) for page in payload.get("requested_pages") or []]
        if any(page < 1 or page > page_count for page in requested):
            raise ValueError("navigation_selection_outside_report")
        selected.update(requested)
        accepted.append(dict(item))
    if not accepted or not selected:
        raise ValueError("navigation_requires_successful_map_selection")
    covered = {page for item in accepted for page in item.get("source_pages") or []}
    if covered != set(range(1, page_count + 1)):
        raise ValueError("navigation_does_not_cover_every_physical_page")
    return accepted, selected


def partial_slice_plan(
    pages: Sequence[Mapping[str, Any]],
    selected_ids: Sequence[int],
    *,
    report: Any,
    identity: Mapping[str, Any],
    ledger_units: int = PARTIAL_LEDGER_UNITS,
    records: Sequence[Any] = (),
    explicit_span: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fit a read span inside the ledger using the executable citation ceiling.

    An explicit span is the caller's bounded text, not a production page picker.
    Without one, the longest whole-page prefix of the first selected run is used.
    Unrelated native facts are not part of the ceiling. Unread pages stay outside.
    """
    by_page = {int(page["page"]): page for page in pages}
    runs = contiguous_runs(selected_ids)
    if not runs and explicit_span is None:
        raise ValueError("partial_slice_requires_a_selected_run")
    chosen: list[int] = []
    plan: dict[str, int] | None = None

    def sized(candidate: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        return executable_reservations(
            candidate, report=report, identity=identity, records=records
        )

    if explicit_span is not None:
        page_no = int(explicit_span["page"])
        source = by_page[page_no]
        text = str(source["text"])
        start = int(explicit_span["start"])
        end = int(explicit_span["end"])
        if not 0 <= start < end <= len(text):
            raise ValueError("diagnostic_span_outside_page")
        fragment_text = text[start:end]
        expected = explicit_span.get("sha256")
        if (
            expected
            and hashlib.sha256(fragment_text.encode()).hexdigest() != expected
        ):
            raise ValueError("diagnostic_span_hash_mismatch")
        if not fragment_text.strip():
            raise ValueError("partial_slice_has_no_complete_sentence")
        fragments = [
            {
                "page": page_no,
                "text": fragment_text,
                "readable": True,
                "offset": start,
                "body_scope": "bounded_span",
            }
        ]
        plan = sized(fragments)
        if plan["total_units"] > ledger_units:
            raise ValueError("partial_slice_does_not_fit_remaining_ledger")
        sent = {page_no}
        return {
            "pages": [page_no],
            "fragments": fragments,
            "unread_selected_pages": [
                page for page in sorted(set(selected_ids)) if page not in sent
            ],
            "tail_offset": end,
            "span_start": start,
            "ledger_units": ledger_units,
            **plan,
        }

    for count in range(1, len(runs[0]) + 1):
        candidate = [by_page[page] for page in runs[0][:count]]
        measured = sized(candidate)
        if measured["total_units"] <= ledger_units:
            chosen = runs[0][:count]
            plan = measured
        else:
            break
    if not chosen or plan is None:
        raise ValueError("partial_slice_does_not_fit_remaining_ledger")
    fragments = []
    tail_offset = None
    for index, page_no in enumerate(chosen):
        source = by_page[page_no]
        text = str(source["text"])
        scope = "full_page"
        if index == len(chosen) - 1:
            trimmed = trim_incomplete_tail(text)
            if trimmed != text:
                text = trimmed
                scope = "bounded_span"
                tail_offset = len(text)
        if not text.strip():
            raise ValueError("partial_slice_has_no_complete_sentence")
        fragments.append(
            {
                "page": page_no,
                "text": text,
                "readable": True,
                "offset": 0,
                "body_scope": scope,
            }
        )
    if any(item["body_scope"] == "bounded_span" for item in fragments):
        plan = sized(fragments)
        if plan["total_units"] > ledger_units:
            raise ValueError("partial_slice_does_not_fit_remaining_ledger")
    sent = set(chosen)
    return {
        "pages": chosen,
        "fragments": fragments,
        "unread_selected_pages": [
            page for page in sorted(set(selected_ids)) if page not in sent
        ],
        "tail_offset": tail_offset,
        "ledger_units": ledger_units,
        **plan,
    }
