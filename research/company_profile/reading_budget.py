"""Reservation plans for a bounded report slice and the full-report gap.

Units are UTF-8 gateway payload bytes plus 256 bytes of framing plus the
requested output cap. They are not a tokenizer reading and not a fee.
"""

from __future__ import annotations

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
    """One window shares one recovery. Any settled failure consumes it.

    ``failed_or_cancelled`` covers timeouts. ``http_failure`` is what the live
    client records for a non-2xx response. A failure that happens to include
    usage, or a later parse/repair send, does not restore a fresh recovery.
    """
    status = str(item.get("status") or "")
    return status not in {"", "reserved", "success"}


def remaining_tail_units(
    plan: Mapping[str, int], attempts: Sequence[Mapping[str, Any]], purpose: str
) -> int:
    """Hold synthesis, verification and one recovery until that recovery is used."""
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


def stage_reservations(
    pages: Sequence[Mapping[str, Any]],
    *,
    report: Any,
    identity: Mapping[str, Any],
    conditions: Sequence[Any] = (),
    claims: Sequence[Any] = (),
    records: Sequence[Any] = (),
) -> dict[str, int]:
    """Body plus follow-ups that carry the supplied conditions, claims and facts.

    Empty follow-ups are not an upper bound. Callers that have not yet read the
    body pass protocol-shaped objects from ``protocol_followup``.
    """
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


def protocol_followup(
    pages: Sequence[Mapping[str, Any]],
    *,
    report: Any,
    identity: Mapping[str, Any],
    records: Sequence[Any] = (),
):
    """Protocol-shaped conditions and claims for sizing, not a business reading."""
    from .contracts import ReportSemanticResponseV3
    from .report_wire import compact_request, expand_response
    from .resource_probe import protocol_reply

    inventory = _request(
        pages,
        purpose="source_inventory",
        report=report,
        identity=identity,
        max_output=2500,
    )
    conditions = ReportSemanticResponseV3.model_validate_json(
        json.dumps(
            expand_response(protocol_reply(compact_request(inventory)), inventory)
        )
    ).source_conditions
    synthesis = _request(
        pages,
        purpose="synthesize_views",
        report=report,
        identity=identity,
        max_output=2500,
        source_conditions=conditions,
        records=tuple(records),
    )
    claims = ReportSemanticResponseV3.model_validate_json(
        json.dumps(
            expand_response(protocol_reply(compact_request(synthesis)), synthesis)
        )
    ).claims
    return conditions, claims


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
) -> dict[str, Any]:
    """Longest whole-page prefix of the first selected run that leaves a tail.

    The tail is nonempty synthesis and verification plus one recovery. Empty
    follow-up requests are not the ceiling. Unread pages stay outside the slice.
    """
    by_page = {int(page["page"]): page for page in pages}
    runs = contiguous_runs(selected_ids)
    if not runs:
        raise ValueError("partial_slice_requires_a_selected_run")
    chosen: list[int] = []
    plan: dict[str, int] | None = None

    def sized(candidate: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        conditions, claims = protocol_followup(
            candidate, report=report, identity=identity, records=records
        )
        return stage_reservations(
            candidate,
            report=report,
            identity=identity,
            conditions=conditions,
            claims=claims,
            records=records,
        )

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
