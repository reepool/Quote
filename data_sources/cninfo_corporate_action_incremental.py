"""Candidate selection for incremental CNInfo corporate-action refreshes."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import unicodedata
from typing import Any, Dict, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from data_sources.cninfo_special_action_resolution import (
    classify_cninfo_announcement_title_prefilter,
)


_REASON_PRIORITY = {
    "explicit": 0,
    "retry_indeterminate": 10,
    "deferred_announcement": 15,
    "recent_event": 20,
    "announcement_activity": 30,
    "safety_sweep": 40,
}

DAILY_TITLE_TRIGGER_POLICY_VERSION = (
    "cninfo_corporate_action_daily_title_trigger_v2"
)
_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_DAILY_ACTION_SUBJECT_MARKERS = (
    "权益分派",
    "利润分配",
    "现金红利",
    "分红",
    "派息",
    "送股",
    "转增",
    "配股",
    "缩股",
    "股权分置",
    "股改",
    "对价",
    "重整",
    "补偿",
    "补偿股份",
    "股份补偿",
    "股份赠与",
    "债转股",
    "以股抵债",
    "偿债",
    "清偿债务",
    "非对称",
    "定向赠送",
    "股份注销",
    "回购注销",
    "库存股注销",
    "减少注册资本",
    "减资",
)
_DAILY_IMPLEMENTATION_MARKERS = (
    "实施",
    "完成",
    "完毕",
    "派发",
    "派息",
    "股权登记",
    "到账",
    "除权",
    "除息",
    "复牌",
    "发行",
    "上市",
    "执行",
)
_DAILY_EXCEPTIONAL_ACTION_MARKERS = (
    "重整",
    "补偿",
    "股权分置",
    "股改",
    "对价",
    "债转股",
    "以股抵债",
    "缩股",
    "偿债",
    "清偿债务",
    "非对称",
    "定向赠送",
)
_SEMANTIC_REASON_PRIORITY = {
    "incomplete_structured_event": 0,
    "exceptional_implementation_title": 10,
    "current_run_tdx_conflict": 20,
}


@dataclass(frozen=True)
class CorporateActionRefreshCandidate:
    """One active instrument selected for a structured CNInfo refresh."""

    instrument_id: str
    symbol: str
    exchange: str
    reasons: tuple[str, ...]
    priority: int


def resolve_daily_announcement_window(
    *,
    run_at: datetime,
    schedule_mode: str,
    previous_trading_day: date | datetime | None = None,
) -> Dict[str, Any]:
    """Resolve the minimum complete announcement interval for one daily run."""
    normalized_mode = str(schedule_mode or "").strip().lower()
    if normalized_mode not in {"calendar_daily", "trading_day"}:
        raise ValueError(
            "announcement_schedule_mode must be calendar_daily or trading_day"
        )
    normalized_run_at = run_at
    if normalized_run_at.tzinfo is None:
        normalized_run_at = normalized_run_at.replace(tzinfo=_SHANGHAI_TZ)
    else:
        normalized_run_at = normalized_run_at.astimezone(_SHANGHAI_TZ)
    run_date = normalized_run_at.date()
    if normalized_mode == "calendar_daily":
        start_date = run_date - timedelta(days=1)
    else:
        if isinstance(previous_trading_day, datetime):
            previous_trading_day = previous_trading_day.date()
        if previous_trading_day is None or previous_trading_day >= run_date:
            raise ValueError(
                "trading_day announcement mode requires a prior trading day"
            )
        start_date = previous_trading_day
    return {
        "schedule_mode": normalized_mode,
        "start_date": start_date,
        "end_date": run_date,
        "run_at": normalized_run_at,
    }


def classify_daily_corporate_action_title(title: Any) -> Dict[str, Any]:
    """Return whether a title is a useful structured-action refresh trigger."""
    prefilter = classify_cninfo_announcement_title_prefilter(title)
    if prefilter.get("excluded"):
        return {
            "selected": False,
            "reason": f"prefilter:{prefilter.get('reason') or 'excluded'}",
            "subject_markers": [],
            "implementation_markers": [],
            "exceptional_markers": [],
            "requires_semantic_review": False,
            "prefilter": prefilter,
            "policy_version": DAILY_TITLE_TRIGGER_POLICY_VERSION,
        }
    normalized_title = unicodedata.normalize("NFKC", str(title or "")).strip()
    subject_markers = [
        marker for marker in _DAILY_ACTION_SUBJECT_MARKERS
        if marker in normalized_title
    ]
    if (
        not subject_markers
        and "回购" in normalized_title
        and "注销" in normalized_title
    ):
        subject_markers.append("回购+注销")
    implementation_markers = [
        marker for marker in _DAILY_IMPLEMENTATION_MARKERS
        if marker in normalized_title
    ]
    exceptional_markers = [
        marker for marker in _DAILY_EXCEPTIONAL_ACTION_MARKERS
        if marker in normalized_title
    ]
    selected = bool(subject_markers and implementation_markers)
    return {
        "selected": selected,
        "reason": (
            "corporate_action_implementation"
            if selected
            else "missing_subject_marker"
            if not subject_markers
            else "missing_implementation_marker"
        ),
        "subject_markers": subject_markers,
        "implementation_markers": implementation_markers,
        "exceptional_markers": exceptional_markers,
        "requires_semantic_review": bool(selected and exceptional_markers),
        "prefilter": prefilter,
        "policy_version": DAILY_TITLE_TRIGGER_POLICY_VERSION,
    }


def select_daily_semantic_anomalies(
    events: Sequence[Mapping[str, Any]],
    *,
    exceptional_markers_by_event: Mapping[str, Sequence[str]] | None = None,
    conflict_event_keys: Iterable[str] = (),
    changed_event_keys: Iterable[str] = (),
    priority_event_keys: Iterable[str] = (),
    max_events: int = 50,
) -> Dict[str, Any]:
    """Select a bounded, deterministic set of current-run semantic anomalies."""
    exceptional_markers_by_event = exceptional_markers_by_event or {}
    conflict_keys = {
        str(item).strip() for item in conflict_event_keys if str(item).strip()
    }
    changed_keys = {
        str(item).strip() for item in changed_event_keys if str(item).strip()
    }
    priority_keys = {
        str(item).strip() for item in priority_event_keys if str(item).strip()
    }
    candidates: list[Dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for event in events:
        event_key = str(event.get("source_event_key") or "").strip()
        instrument_id = str(event.get("instrument_id") or "").strip()
        if not event_key or not instrument_id:
            continue
        exceptional_markers = sorted({
            str(item).strip()
            for item in exceptional_markers_by_event.get(event_key, ())
            if str(item).strip()
        })
        if (
            bool(event.get("resolution_is_terminal"))
            and event_key not in changed_keys
            and not exceptional_markers
        ):
            continue
        reasons: list[str] = []
        quality_status = str(event.get("quality_status") or "").strip()
        if quality_status.startswith("partial_"):
            reasons.append("incomplete_structured_event")
        if exceptional_markers:
            reasons.append("exceptional_implementation_title")
        if (
            event_key in conflict_keys
            and (event_key in changed_keys or event_key in priority_keys)
        ):
            reasons.append("current_run_tdx_conflict")
        if not reasons:
            continue
        ordered_reasons = sorted(
            set(reasons),
            key=lambda item: (_SEMANTIC_REASON_PRIORITY[item], item),
        )
        reason_counts.update(ordered_reasons)
        candidates.append({
            "instrument_id": instrument_id,
            "source_event_key": event_key,
            "quality_status": quality_status,
            "reason_codes": ordered_reasons,
            "exceptional_markers": exceptional_markers,
            "priority": min(
                _SEMANTIC_REASON_PRIORITY[item] for item in ordered_reasons
            ),
        })
    candidates.sort(
        key=lambda item: (
            0 if str(item["source_event_key"]) in priority_keys else 1,
            int(item["priority"]),
            str(item["instrument_id"]),
            str(item["source_event_key"]),
        )
    )
    limit = max(0, int(max_events))
    selected = candidates[:limit]
    deferred = candidates[limit:]
    return {
        "candidate_count": len(candidates),
        "selected_count": len(selected),
        "deferred_count": len(deferred),
        "reason_counts": dict(sorted(reason_counts.items())),
        "candidates": selected,
        "deferred": deferred,
        "source_event_keys": [
            str(item["source_event_key"]) for item in selected
        ],
        "deferred_source_event_keys": [
            str(item["source_event_key"]) for item in deferred
        ],
    }


def associate_exceptional_announcements(
    events: Sequence[Mapping[str, Any]],
    *,
    exceptional_announcements_by_instrument: Mapping[
        str, Sequence[Mapping[str, Any]]
    ],
    max_date_distance_days: int = 1,
) -> Dict[str, Any]:
    """Associate each exceptional announcement with one bounded source event."""
    events_by_instrument: Dict[str, list[Mapping[str, Any]]] = {}
    for event in events:
        instrument_id = str(event.get("instrument_id") or "").strip()
        event_key = str(event.get("source_event_key") or "").strip()
        if not instrument_id or not event_key:
            continue
        events_by_instrument.setdefault(instrument_id, []).append(event)

    def _coerce_date(value: Any) -> date | None:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None

    def _match_event(
        candidates: Sequence[Mapping[str, Any]],
        announcement_date: date,
    ) -> Mapping[str, Any] | None:
        scored: list[tuple[int, int, Mapping[str, Any]]] = []
        for event in candidates:
            event_date_scores = [
                (abs((parsed - announcement_date).days), priority)
                for priority, field_name in enumerate((
                    "announcement_date",
                    "record_date",
                    "ex_date",
                    "pay_date",
                    "share_arrival_date",
                ))
                if (parsed := _coerce_date(event.get(field_name))) is not None
            ]
            if event_date_scores:
                distance, priority = min(event_date_scores)
                scored.append((distance, priority, event))
        bounded = [
            item
            for item in scored
            if item[0] <= max(0, int(max_date_distance_days))
        ]
        if not bounded:
            return None
        best_score = min((item[0], item[1]) for item in bounded)
        best = [
            item[2]
            for item in bounded
            if (item[0], item[1]) == best_score
        ]
        return best[0] if len(best) == 1 else None

    event_markers: Dict[str, set[str]] = {}
    announcement_keys_by_event: Dict[str, set[str]] = {}
    matched_instruments: set[str] = set()
    unmatched_announcements: list[Dict[str, Any]] = []
    for instrument_id, announcements in sorted(
        exceptional_announcements_by_instrument.items()
    ):
        normalized_instrument_id = str(instrument_id or "").strip()
        for announcement in announcements or ():
            announcement_date = _coerce_date(
                announcement.get("announcement_date")
            )
            announcement_key = str(
                announcement.get("announcement_key") or ""
            ).strip()
            markers = {
                str(item).strip()
                for item in (
                    announcement.get("exceptional_markers") or ()
                )
                if str(item).strip()
            }
            matched_event = (
                _match_event(
                    events_by_instrument.get(normalized_instrument_id, ()),
                    announcement_date,
                )
                if normalized_instrument_id and announcement_date is not None
                else None
            )
            if matched_event is None:
                unmatched_announcements.append({
                    **dict(announcement),
                    "instrument_id": normalized_instrument_id,
                })
                continue
            event_key = str(
                matched_event.get("source_event_key") or ""
            ).strip()
            if not event_key:
                continue
            event_markers.setdefault(event_key, set()).update(markers)
            if announcement_key:
                announcement_keys_by_event.setdefault(
                    event_key, set()
                ).add(announcement_key)
            matched_instruments.add(normalized_instrument_id)

    unmatched_instruments = {
        str(item.get("instrument_id") or "").strip()
        for item in unmatched_announcements
        if str(item.get("instrument_id") or "").strip()
    }
    return {
        "exceptional_markers_by_event": {
            event_key: sorted(markers)
            for event_key, markers in sorted(event_markers.items())
        },
        "announcement_keys_by_event": {
            event_key: sorted(keys)
            for event_key, keys in sorted(announcement_keys_by_event.items())
        },
        "matched_instrument_ids": sorted(matched_instruments),
        "unmatched_instrument_ids": sorted(unmatched_instruments),
        "unmatched_announcements": unmatched_announcements,
    }


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
