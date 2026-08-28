"""Shared write guards for shareholder snapshots."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from research.providers.base import ShareholderSnapshot


MAIN_BOARD_TOP10_EXCHANGES = frozenset({"SSE", "SZSE"})
REQUIRED_MAIN_BOARD_TOP_HOLDERS = 10


def top_holders_satisfy_required_scope(exchange: str, top_holders: Any) -> bool:
    """Return whether a top-holder list satisfies the exchange coverage rule."""
    holders = top_holders if isinstance(top_holders, list) else []
    holders = [item for item in holders if isinstance(item, dict)]
    if not holders:
        return False
    ranks = [item.get("rank") for item in holders]
    names = [str(item.get("holder_name") or "").strip() for item in holders]
    dates = {_normalize_report_date(item.get("report_date")) for item in holders}
    if not all(names) or len(set(names)) != len(names) or None in dates or len(dates) != 1:
        return False
    if str(exchange or "").strip().upper() in MAIN_BOARD_TOP10_EXCHANGES:
        return len(holders) == REQUIRED_MAIN_BOARD_TOP_HOLDERS and set(ranks) == set(
            range(1, REQUIRED_MAIN_BOARD_TOP_HOLDERS + 1)
        )
    return all(isinstance(rank, int) and rank > 0 for rank in ranks)


def actual_shareholder_coverage_scope(
    *,
    exchange: str,
    snapshot_json: Optional[Dict[str, Any]] = None,
    holder_count: Any = None,
) -> Set[str]:
    """Return coverage actually present, not just claimed coverage_scope values."""
    payload = snapshot_json if isinstance(snapshot_json, dict) else {}
    actual: Set[str] = set()

    holder_value = holder_count
    holder_blob = payload.get("holder_count")
    if holder_value is None and isinstance(holder_blob, dict):
        holder_value = holder_blob.get("value")
    if _valid_holder_count(holder_value) and _holder_count_report_date(payload):
        actual.add("holder_count")

    if top_holders_satisfy_required_scope(exchange, payload.get("top_holders") or []):
        actual.add("top10_holders")

    if _has_actual_ownership_control(payload):
        actual.add("reference_only_ownership_clues")
    return actual


def build_shareholder_coverage_scope(
    *,
    exchange: str,
    holder_count: Any,
    holder_count_report_date: Any = None,
    top_holders: Any,
    has_ownership_clues: bool,
) -> List[str]:
    """Build coverage_scope using exchange-specific top10 completeness."""
    scope: List[str] = []
    if _valid_holder_count(holder_count) and _normalize_report_date(holder_count_report_date):
        scope.append("holder_count")
    if top_holders_satisfy_required_scope(exchange, top_holders):
        scope.append("top10_holders")
    if has_ownership_clues:
        scope.append("reference_only_ownership_clues")
    return scope


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

    exchange = str(
        (existing_snapshot or {}).get("exchange")
        or incoming.exchange
        or ""
    )
    incoming_json = incoming.snapshot_json if isinstance(incoming.snapshot_json, dict) else {}
    existing_actual = actual_shareholder_coverage_scope(
        exchange=exchange,
        snapshot_json=existing_json,
        holder_count=(
            existing_snapshot.get("holder_count") if existing_snapshot else None
        ),
    )
    incoming_actual = actual_shareholder_coverage_scope(
        exchange=str(incoming.exchange or exchange),
        snapshot_json=incoming_json,
        holder_count=incoming.holder_count,
    )
    existing_tops = (
        existing_json.get("top_holders") if isinstance(existing_json, dict) else []
    ) or []
    incoming_tops = (
        incoming_json.get("top_holders") if isinstance(incoming_json, dict) else []
    ) or []

    if required_scope and required_scope.issubset(existing_actual):
        if required_scope and not required_scope.issubset(incoming_actual):
            return True
    elif required_scope and required_scope.issubset(incoming_actual):
        return False
    else:
        if set(existing_actual) - set(incoming_actual):
            return True
        if len(incoming_tops) > len(existing_tops if isinstance(existing_tops, list) else []):
            return False
        if len(incoming_tops if isinstance(incoming_tops, list) else []) < len(
            existing_tops if isinstance(existing_tops, list) else []
        ):
            return True
        return False

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
    present = {item for item in dates if item}
    return next(iter(present)) if len(present) == 1 else None


def _valid_holder_count(value: Any) -> bool:
    try:
        return int(value) >= 0
    except (TypeError, ValueError):
        return False


def _has_actual_ownership_control(snapshot_json: Dict[str, Any]) -> bool:
    """Return whether a source provided a dedicated ownership/control field.

    Top-holder rank is not an actual-controller assertion and is deliberately
    excluded here.
    """
    ownership = snapshot_json.get("ownership_clues")
    if not isinstance(ownership, dict):
        return False
    return any(
        str(ownership.get(key) or "").strip()
        for key in (
            "control_owner_name",
            "actual_controller_name",
            "controller_name",
            "controlling_shareholder_name",
            "control_method",
            "ownership_control_method",
        )
    )


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
