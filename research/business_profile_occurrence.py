"""Canonical source and semantic identity material for business-profile records."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


def normalize_occurrence_material(
    *,
    instrument_id: Any,
    report_period: Any,
    source_document_id: Any,
    evidence_id: Any = None,
    page_number: Any = None,
    section_id: Any = None,
    source_row_key: Any = None,
    contract_reference_raw: Any = None,
    subject_scope: Any = None,
    object_raw: Any = None,
    object_id: Any = None,
    action_or_relationship: Any = None,
    segment_id: Any = None,
    occurrence_ordinal: Any = None,
    table_id: Any = None,
    metric_slot: Any = None,
    normalized_quote: Any = None,
    context_before: Any = None,
    context_after: Any = None,
    narrative_match_ordinal: Any = None,
    normalization_policy_version: Any = "source-v2",
) -> dict[str, Any]:
    """Return immutable source coordinates used for occurrence identity.

    ``evidence_id`` and the remaining historical semantic arguments stay in
    the signature for callers during the migration, but deliberately do not
    participate in the returned material.  Evidence records, LLM scopes and
    normalized objects are regenerated/selected values, not physical source
    identity.
    """

    def clean(value: Any) -> Any:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    base = {
        "instrument_id": clean(instrument_id),
        "report_period": clean(report_period),
        "source_document_id": clean(source_document_id),
    }
    page = int(page_number) if str(page_number or "").isdigit() else None
    row = clean(source_row_key)
    table = clean(table_id) or clean(section_id)
    slot = clean(metric_slot)
    if table or slot:
        base.update(
            {
                "source_kind": "table",
                "page_number": page,
                "table_id": table,
                "row_locator": row,
                "metric_slot": slot,
            }
        )
        return base

    def normalized_text(value: Any) -> str | None:
        value = clean(value)
        return " ".join(value.split()) if value else None

    quote = normalized_text(normalized_quote)
    before = normalized_text(context_before)
    after = normalized_text(context_after)
    ordinal = (
        int(narrative_match_ordinal)
        if str(narrative_match_ordinal or "").isdigit()
        else None
    )
    base.update(
        {
            "source_kind": "narrative",
            "page_number": page,
            "normalization_policy_version": clean(normalization_policy_version),
            "normalized_quote_hash": (
                hashlib.sha256(quote.encode("utf-8")).hexdigest() if quote else None
            ),
            "context_before": before,
            "context_after": after,
            "same_page_match_ordinal": ordinal,
        }
    )
    return base


def occurrence_material_from_exact_evidence(
    *,
    instrument_id: Any,
    report_period: Any,
    source_document_id: Any,
    exact_evidence: Mapping[str, Any] | None,
    source_row_key: Any = None,
    metric_slot: Any = None,
    normalized_quote: Any = None,
    narrative_match_ordinal: Any = None,
) -> dict[str, Any]:
    """Build current occurrence material from persisted exact evidence.

    Evidence IDs and parser offsets are intentionally ignored.  A persisted
    row locator makes the assertion tabular; otherwise the normalized quote
    and bounded section anchor form the narrative locator.
    """

    evidence = dict(exact_evidence or {})
    spans = evidence.get("evidence_spans")
    span = (
        dict(spans[0])
        if isinstance(spans, list) and spans and isinstance(spans[0], Mapping)
        else evidence
    )
    page_number = span.get("page_number") or evidence.get("page_number")
    section_id = span.get("section_id") or evidence.get("section_id")
    quote = normalized_quote or span.get("quote") or evidence.get("quote")
    quote_hash = span.get("quote_hash") or evidence.get("quote_hash")
    return normalize_occurrence_material(
        instrument_id=instrument_id,
        report_period=report_period,
        source_document_id=(
            source_document_id
            or span.get("source_document_id")
            or evidence.get("source_document_id")
        ),
        page_number=page_number,
        section_id=section_id,
        source_row_key=source_row_key,
        table_id=section_id if str(source_row_key or "").strip() else None,
        metric_slot=metric_slot,
        normalized_quote=quote or quote_hash,
        context_before=section_id,
        narrative_match_ordinal=(
            narrative_match_ordinal
            if narrative_match_ordinal is not None
            else span.get("same_page_match_ordinal")
            or evidence.get("same_page_match_ordinal")
        ),
    )


def semantic_content_fingerprint(
    *,
    subject_scope: Any = None,
    action_or_relationship: Any = None,
    object_raw: Any = None,
    object_id: Any = None,
    segment_id: Any = None,
    contract_reference_raw: Any = None,
    value: Any = None,
    unit: Any = None,
) -> str:
    """Hash model interpretation separately from immutable source identity."""

    payload = {
        "subject_scope": str(subject_scope or "").strip() or None,
        "action_or_relationship": str(action_or_relationship or "").strip() or None,
        "object_raw": str(object_raw or "").strip() or None,
        "object_id": str(object_id or "").strip() or None,
        "segment_id": str(segment_id or "").strip() or None,
        "contract_reference_raw": str(contract_reference_raw or "").strip() or None,
        "value": value,
        "unit": str(unit or "").strip() or None,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "semantic:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]


def semantic_content_fingerprint_from_record(
    row: Mapping[str, Any], metadata: Mapping[str, Any] | None = None
) -> str:
    """Return the persisted semantic fingerprint or the canonical row fallback.

    Migration audit and apply must use identical fallback semantics for legacy
    rows that predate the persisted fingerprint field.  The fallback only uses
    model interpretation/value fields and never physical evidence identity.
    """

    parsed_metadata: Mapping[str, Any] = metadata or {}
    persisted = parsed_metadata.get("semantic_content_fingerprint")
    if persisted:
        return str(persisted)
    return semantic_content_fingerprint(
        subject_scope=row.get("subject_scope"),
        action_or_relationship=(
            row.get("action")
            or row.get("fact_type")
            or row.get("relationship_type")
        ),
        object_raw=row.get("object_raw") or row.get("fact_scope"),
        object_id=row.get("object_id"),
        segment_id=row.get("segment_id"),
        contract_reference_raw=parsed_metadata.get("contract_reference_raw"),
        value=row.get("value") if row.get("value") is not None else row.get("value_raw"),
        unit=row.get("unit") if row.get("unit") is not None else row.get("unit_raw"),
    )


def occurrence_identity_key(material: Mapping[str, Any], *, prefix: str = "occurrence") -> str:
    payload = json.dumps(dict(material), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]}"
