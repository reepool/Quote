"""Shared write guards for shareholder snapshots."""

from __future__ import annotations

from typing import Any, Dict, Optional, Set

from research.providers.base import ShareholderSnapshot


def coverage_scope_of(snapshot_json: Optional[Dict[str, Any]]) -> Set[str]:
    """Return normalized coverage_scope values from a snapshot JSON payload."""
    if not isinstance(snapshot_json, dict):
        return set()
    return {
        str(item).strip()
        for item in snapshot_json.get("coverage_scope", []) or []
        if str(item).strip()
    }


def stored_snapshot_json(existing_snapshot: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Extract snapshot JSON from a storage row."""
    if not isinstance(existing_snapshot, dict):
        return None
    snapshot_json = existing_snapshot.get("snapshot")
    return snapshot_json if isinstance(snapshot_json, dict) else None


def incoming_shareholder_snapshot_is_weaker(
    existing_snapshot: Optional[Dict[str, Any]],
    incoming: ShareholderSnapshot,
    required_scope: Set[str],
) -> bool:
    """Return True when incoming must not replace an already complete local snapshot."""
    existing_json = stored_snapshot_json(existing_snapshot)
    if existing_json is None:
        return False
    if required_scope and not required_scope.issubset(coverage_scope_of(existing_json)):
        return False

    incoming_json = incoming.snapshot_json if isinstance(incoming.snapshot_json, dict) else {}
    if required_scope and not required_scope.issubset(coverage_scope_of(incoming_json)):
        return True

    existing_holder_date = _holder_count_report_date(existing_json) or _normalize_report_date(
        existing_snapshot.get("holder_count_report_date") if existing_snapshot else None
    )
    incoming_holder_date = _holder_count_report_date(incoming_json) or _normalize_report_date(
        incoming.holder_count_report_date
    )
    existing_top_date = _top_holders_report_date(existing_json) or _normalize_report_date(
        existing_snapshot.get("top_holders_report_date") if existing_snapshot else None
    )
    incoming_top_date = _top_holders_report_date(incoming_json) or _normalize_report_date(
        incoming.top_holders_report_date
    )
    return _report_date_regressed(
        existing_holder_date,
        incoming_holder_date,
    ) or _report_date_regressed(existing_top_date, incoming_top_date)


def _holder_count_report_date(snapshot_json: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(snapshot_json, dict):
        return None
    holder_count = snapshot_json.get("holder_count")
    if isinstance(holder_count, dict):
        return _normalize_report_date(holder_count.get("report_date"))
    return None


def _top_holders_report_date(snapshot_json: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(snapshot_json, dict):
        return None
    dates = []
    top_holders = snapshot_json.get("top_holders") or []
    if isinstance(top_holders, list):
        dates.extend(
            _normalize_report_date(item.get("report_date"))
            for item in top_holders
            if isinstance(item, dict)
        )
    present = [item for item in dates if item]
    return max(present) if present else None


def _report_date_regressed(
    existing_date: Optional[str],
    incoming_date: Optional[str],
) -> bool:
    if existing_date and incoming_date and incoming_date < existing_date:
        return True
    return bool(existing_date and not incoming_date)


def _normalize_report_date(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    digits = "".join(character for character in str(value).strip() if character.isdigit())
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return None
