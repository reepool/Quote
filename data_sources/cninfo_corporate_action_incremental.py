"""Candidate selection for incremental CNInfo corporate-action refreshes."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, Mapping, Sequence


_REASON_PRIORITY = {
    "explicit": 0,
    "retry_indeterminate": 10,
    "deferred_announcement": 15,
    "recent_event": 20,
    "announcement_activity": 30,
    "safety_sweep": 40,
}


@dataclass(frozen=True)
class CorporateActionRefreshCandidate:
    """One active instrument selected for a structured CNInfo refresh."""

    instrument_id: str
    symbol: str
    exchange: str
    reasons: tuple[str, ...]
    priority: int


def normalize_active_instruments(
    instruments: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, str]]:
    """Build a stable active-instrument index from database rows."""
    result: Dict[str, Dict[str, str]] = {}
    for row in instruments:
        instrument_id = str(row.get("instrument_id") or "").strip()
        if not instrument_id:
            continue
        exchange = str(row.get("exchange") or "").strip().upper()
        if not exchange:
            exchange = (
                "SSE" if instrument_id.endswith(".SH")
                else "SZSE" if instrument_id.endswith(".SZ")
                else "BSE" if instrument_id.endswith(".BJ")
                else ""
            )
        symbol = str(row.get("symbol") or instrument_id.split(".")[0]).strip()
        result[instrument_id] = {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "exchange": exchange,
        }
    return result


def build_symbol_index(
    instruments: Mapping[str, Mapping[str, str]],
) -> Dict[str, str]:
    """Map provider symbols to canonical instrument IDs."""
    result: Dict[str, str] = {}
    for instrument_id, row in instruments.items():
        symbol = str(row.get("symbol") or "").strip()
        values = {symbol, instrument_id.split(".")[0]}
        for value in values:
            if value:
                result.setdefault(value, instrument_id)
    return result


def select_rotating_safety_instruments(
    instrument_ids: Sequence[str],
    *,
    as_of_date: date,
    sample_size: int,
) -> list[str]:
    """Select one deterministic market slice for silent-correction recovery."""
    ordered = sorted({str(item).strip() for item in instrument_ids if str(item).strip()})
    bounded_size = max(0, int(sample_size))
    if not ordered or bounded_size <= 0:
        return []
    if bounded_size >= len(ordered):
        return ordered
    bucket_count = (len(ordered) + bounded_size - 1) // bounded_size
    bucket_index = as_of_date.toordinal() % bucket_count
    start = bucket_index * bounded_size
    return ordered[start : start + bounded_size]


def build_incremental_refresh_candidates(
    *,
    active_instruments: Mapping[str, Mapping[str, str]],
    explicit_ids: Iterable[str] = (),
    retry_ids: Iterable[str] = (),
    deferred_announcement_ids: Iterable[str] = (),
    recent_event_ids: Iterable[str] = (),
    announcement_ids: Iterable[str] = (),
    safety_ids: Iterable[str] = (),
    max_candidates: int = 1000,
) -> Dict[str, Any]:
    """Merge prioritized candidate reasons and apply a bounded non-explicit cap."""
    reasons_by_id: Dict[str, set[str]] = {}
    unknown_ids = set()

    def add(values: Iterable[str], reason: str) -> None:
        for raw_value in values:
            instrument_id = str(raw_value or "").strip()
            if not instrument_id:
                continue
            if instrument_id not in active_instruments:
                unknown_ids.add(instrument_id)
                continue
            reasons_by_id.setdefault(instrument_id, set()).add(reason)

    add(explicit_ids, "explicit")
    add(retry_ids, "retry_indeterminate")
    add(deferred_announcement_ids, "deferred_announcement")
    add(recent_event_ids, "recent_event")
    add(announcement_ids, "announcement_activity")
    add(safety_ids, "safety_sweep")

    candidates = []
    for instrument_id, reasons in reasons_by_id.items():
        ordered_reasons = tuple(sorted(reasons, key=lambda item: _REASON_PRIORITY[item]))
        row = active_instruments[instrument_id]
        candidates.append(CorporateActionRefreshCandidate(
            instrument_id=instrument_id,
            symbol=str(row.get("symbol") or instrument_id.split(".")[0]),
            exchange=str(row.get("exchange") or ""),
            reasons=ordered_reasons,
            priority=min(_REASON_PRIORITY[item] for item in ordered_reasons),
        ))
    candidates.sort(key=lambda item: (item.priority, item.instrument_id))

    explicit = [item for item in candidates if "explicit" in item.reasons]
    automatic = [item for item in candidates if "explicit" not in item.reasons]
    automatic_limit = max(0, int(max_candidates))
    selected = explicit + automatic[:automatic_limit]
    selected.sort(key=lambda item: (item.priority, item.instrument_id))
    deferred = automatic[automatic_limit:]
    reason_counts = Counter(
        reason for item in selected for reason in item.reasons
    )
    deferred_reason_counts = Counter(
        reason for item in deferred for reason in item.reasons
    )
    deferred_by_reason = {
        reason: [
            item.instrument_id for item in deferred if reason in item.reasons
        ]
        for reason in sorted(deferred_reason_counts)
    }
    return {
        "candidates": [
            {
                "instrument_id": item.instrument_id,
                "symbol": item.symbol,
                "exchange": item.exchange,
                "reasons": list(item.reasons),
                "priority": item.priority,
            }
            for item in selected
        ],
        "candidate_ids": [item.instrument_id for item in selected],
        "candidate_count": len(selected),
        "explicit_count": len(explicit),
        "automatic_count": len(selected) - len(explicit),
        "deferred_count": len(deferred),
        "deferred_ids": [item.instrument_id for item in deferred[:50]],
        "deferred_candidate_ids": [item.instrument_id for item in deferred],
        "reason_counts": dict(sorted(reason_counts.items())),
        "deferred_reason_counts": dict(sorted(deferred_reason_counts.items())),
        "deferred_by_reason": deferred_by_reason,
        "unknown_ids": sorted(unknown_ids),
    }
