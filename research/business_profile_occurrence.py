"""Canonical occurrence identity material for business-profile records.

The material is deliberately JSON-like so semantic conversion, replay and
governance can hash the same source lineage without depending on model IDs.
"""

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
) -> dict[str, Any]:
    """Return normalized immutable source material used for an occurrence key."""

    def clean(value: Any) -> Any:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    return {
        "instrument_id": clean(instrument_id),
        "report_period": clean(report_period),
        "source_document_id": clean(source_document_id),
        "evidence_id": clean(evidence_id),
        "page_number": int(page_number) if str(page_number or "").isdigit() else None,
        "section_id": clean(section_id),
        "source_row_key": clean(source_row_key),
        "contract_reference_raw": clean(contract_reference_raw),
        "subject_scope": clean(subject_scope),
        "object_raw": clean(object_raw),
        "object_id": clean(object_id),
        "action_or_relationship": clean(action_or_relationship),
        "segment_id": clean(segment_id),
        "occurrence_ordinal": int(occurrence_ordinal)
        if str(occurrence_ordinal or "").isdigit()
        else None,
    }


def occurrence_identity_key(material: Mapping[str, Any], *, prefix: str = "occurrence") -> str:
    payload = json.dumps(dict(material), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{prefix}:{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:32]}"
