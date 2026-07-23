"""Bounded CNInfo announcement discovery for special corporate actions."""

from __future__ import annotations

import html
import json
import math
import re
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


_TITLE_TAG_RE = re.compile(r"<[^>]+>")
_SHARE_REFORM_MARKERS = ("股权分置", "股改", "对价")
_RESTRUCTURING_MARKERS = ("重整", "重组", "资本公积", "转增股本")
_EXECUTION_MARKERS = (
    "实施",
    "执行",
    "复牌",
    "上市",
    "到账",
    "派发",
    "派息",
    "除权",
    "除息",
    "股权登记",
    "对价",
    "缴款",
    "配股",
)
_GENERIC_ACTION_MARKERS = (
    "权益分派",
    "利润分配",
    "现金红利",
    "派息",
    "送股",
    "转增",
    "分红",
    "配股",
)
_TITLE_EXCLUDES = ("取消", "终止", "不予实施", "不实施")

IMPLEMENTATION_GRADE_ANNOUNCEMENT_ROLES = frozenset({
    "implementation",
    "implementation_completion",
    "record_date_notice",
    "share_arrival_notice",
    "rights_issue",
    "share_reform",
    "compensation_share_distribution",
})


@dataclass(frozen=True)
class SpecialActionSearchTarget:
    """One unresolved CNInfo event and its bounded announcement search window."""

    instrument_id: str
    source_event_key: str
    source_profile: str
    event_class: str
    start_date: date
    end_date: date
    search_basis: str
    source_anchor_dates: List[date]
    row: Mapping[str, Any]
    candidate_effective_dates: tuple[date, ...] = ()
    anchor_roles: tuple[str, ...] = ()


def parse_date(value: Any) -> Optional[date]:
    """Parse database, ISO, and date values without treating NaT as valid."""
    try:
        if value is not None and value != value:
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def best_structured_anchor(row: Mapping[str, Any]) -> Optional[date]:
    """Return the best date for archive-availability policy decisions."""
    implementation_dates = [
        parsed
        for field_name in ("record_date", "share_arrival_date")
        if (parsed := parse_date(row.get(field_name))) is not None
    ]
    if implementation_dates:
        return max(implementation_dates)
    announcement_date = parse_date(row.get("announcement_date"))
    if announcement_date is not None:
        return announcement_date
    fiscal_period = str(row.get("fiscal_period") or "")
    match = re.search(r"(19|20)\d{2}", fiscal_period)
    return date(int(match.group(0)), 12, 31) if match else None


def is_implementation_grade_decision(decision: Mapping[str, Any]) -> bool:
    """Return whether a title decision may proceed to semantic extraction."""
    return (
        str(decision.get("relevance") or "")
        in {"relevant", "possibly_relevant"}
        and str(decision.get("announcement_role") or "")
        in IMPLEMENTATION_GRADE_ANNOUNCEMENT_ROLES
    )


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _raw_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    payload = row.get("raw_payload")
    if isinstance(payload, Mapping):
        return dict(payload)
    payload = row.get("raw_payload_json")
    if not payload:
        return {}
    try:
        parsed = json.loads(str(payload))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def classify_special_action(row: Mapping[str, Any]) -> Optional[str]:
    """Classify partial events that warrant official announcement discovery."""
    quality_status = str(row.get("quality_status") or "")
    if quality_status not in {
        "partial_missing_ex_date",
        "partial_missing_fields",
        "partial_missing_economic_fields",
        "partial_zero_effect",
    }:
        return None
    if (
        quality_status == "partial_missing_ex_date"
        and parse_date(row.get("ex_date")) is not None
    ):
        return None
    payload = _raw_payload(row)
    text = " ".join(
        str(value or "")
        for value in (
            row.get("description"),
            payload.get("分红类型"),
            payload.get("实施方案分红说明"),
        )
    )
    if any(marker in text for marker in _SHARE_REFORM_MARKERS):
        return "share_reform"
    if any(marker in text for marker in _RESTRUCTURING_MARKERS):
        return "restructuring_capitalization"
    economic_effect = (
        _number(row.get("cash_dividend_per_share"))
        + _number(row.get("bonus_shares_per_share"))
        + _number(row.get("capitalization_shares_per_share"))
        + _number(row.get("rights_shares_per_share"))
        + _number(row.get("rights_price"))
    )
    if economic_effect > 0 or any(
        marker in text for marker in _GENERIC_ACTION_MARKERS
    ):
        return "missing_date_distribution"
    return None


def _candidate_effective_dates(
    row: Mapping[str, Any],
    trading_days: Sequence[date],
) -> tuple[date, ...]:
    ordered = sorted({value for value in trading_days if value is not None})
    if not ordered:
        return ()
    record_date = parse_date(row.get("record_date"))
    arrival_date = parse_date(row.get("share_arrival_date"))
    candidates = []
    if record_date is not None:
        index = bisect_right(ordered, record_date)
        if index < len(ordered):
            next_trading_day = ordered[index]
            if arrival_date is None or next_trading_day <= arrival_date:
                candidates.append(next_trading_day)
    elif arrival_date is not None:
        arrival_index = bisect_left(ordered, arrival_date)
        if arrival_index < len(ordered) and ordered[arrival_index] == arrival_date:
            candidates.append(arrival_date)
        if arrival_index > 0:
            candidates.append(ordered[arrival_index - 1])
    return tuple(sorted(set(candidates)))


def _cluster_role_dates(
    row: Mapping[str, Any],
    *,
    max_anchor_gap_days: int,
) -> list[list[tuple[str, date]]]:
    role_dates = sorted(
        (
            (field_name, parsed)
            for field_name in (
                "record_date", "share_arrival_date", "announcement_date", "ex_date"
            )
            if (parsed := parse_date(row.get(field_name))) is not None
        ),
        key=lambda item: (item[1], item[0]),
    )
    clusters: list[list[tuple[str, date]]] = []
    for role_date in role_dates:
        if (
            not clusters
            or (role_date[1] - clusters[-1][-1][1]).days
            > max(1, int(max_anchor_gap_days))
        ):
            clusters.append([role_date])
        else:
            clusters[-1].append(role_date)
    role_weights = {
        "ex_date": 100,
        "record_date": 20,
        "share_arrival_date": 15,
        "announcement_date": 5,
    }
    return sorted(
        clusters,
        key=lambda cluster: (
            -sum(role_weights[role] for role, _ in cluster),
            min(value for _, value in cluster),
        ),
    )


def build_search_targets(
    row: Mapping[str, Any],
    *,
    adjacent_dates: Sequence[date] = (),
    trading_days: Sequence[date] = (),
    window_before_days: int = 10,
    window_after_days: int = 30,
    max_window_days: int = 180,
    max_anchor_gap_days: int = 60,
) -> List[SpecialActionSearchTarget]:
    """Build independent bounded windows from nearby date-role clusters."""
    event_class = classify_special_action(row)
    instrument_id = str(row.get("instrument_id") or "").strip()
    source_event_key = str(row.get("source_event_key") or "").strip()
    source_profile = str(row.get("source_profile") or "").strip()
    if not event_class or not all((instrument_id, source_event_key, source_profile)):
        return []
    candidate_dates = _candidate_effective_dates(row, trading_days)
    targets: List[SpecialActionSearchTarget] = []
    for cluster in _cluster_role_dates(
        row, max_anchor_gap_days=max_anchor_gap_days
    ):
        anchors = sorted({value for _, value in cluster})
        roles = tuple(sorted({role for role, _ in cluster}))
        start = anchors[0] - timedelta(days=max(0, int(window_before_days)))
        end = anchors[-1] + timedelta(days=max(0, int(window_after_days)))
        if end < start or (end - start).days > max(1, int(max_window_days)):
            continue
        targets.append(SpecialActionSearchTarget(
            instrument_id=instrument_id,
            source_event_key=source_event_key,
            source_profile=source_profile,
            event_class=event_class,
            start_date=start,
            end_date=end,
            search_basis="role_cluster:" + "+".join(roles),
            source_anchor_dates=anchors,
            row=row,
            candidate_effective_dates=candidate_dates,
            anchor_roles=roles,
        ))
    if targets:
        return targets
    nearby = sorted({value for value in adjacent_dates if value is not None})
    previous = nearby[0] if len(nearby) >= 2 else None
    following = nearby[-1] if nearby else None
    if previous is None or following is None or following <= previous:
        return []
    start = previous + timedelta(days=1)
    end = following - timedelta(days=1)
    if end < start or (end - start).days > max(1, int(max_window_days)):
        return []
    return [SpecialActionSearchTarget(
        instrument_id=instrument_id,
        source_event_key=source_event_key,
        source_profile=source_profile,
        event_class=event_class,
        start_date=start,
        end_date=end,
        search_basis="adjacent_corporate_action_dates",
        source_anchor_dates=[previous, following],
        row=row,
        candidate_effective_dates=candidate_dates,
        anchor_roles=("adjacent_event",),
    )]


def build_search_target(
    row: Mapping[str, Any],
    *,
    adjacent_dates: Sequence[date] = (),
    trading_days: Sequence[date] = (),
    window_before_days: int = 10,
    window_after_days: int = 30,
    max_window_days: int = 180,
    max_anchor_gap_days: int = 60,
) -> Optional[SpecialActionSearchTarget]:
    """Return the highest-priority search window for compatibility callers."""
    targets = build_search_targets(
        row,
        adjacent_dates=adjacent_dates,
        trading_days=trading_days,
        window_before_days=window_before_days,
        window_after_days=window_after_days,
        max_window_days=max_window_days,
        max_anchor_gap_days=max_anchor_gap_days,
    )
    return targets[0] if targets else None


def announcement_match_reasons(
    target: SpecialActionSearchTarget,
    title: str,
) -> List[str]:
    """Return auditable title-match reasons without resolving an effective date."""
    normalized = html.unescape(_TITLE_TAG_RE.sub("", str(title or ""))).strip()
    if not normalized or any(marker in normalized for marker in _TITLE_EXCLUDES):
        return []
    class_markers = {
        "share_reform": _SHARE_REFORM_MARKERS,
        "restructuring_capitalization": _RESTRUCTURING_MARKERS,
        "missing_date_distribution": _GENERIC_ACTION_MARKERS,
    }[target.event_class]
    action_hits = [marker for marker in class_markers if marker in normalized]
    execution_hits = [marker for marker in _EXECUTION_MARKERS if marker in normalized]
    if not action_hits or not execution_hits:
        return []
    return [
        f"event_class:{target.event_class}",
        *(f"action_term:{marker}" for marker in action_hits),
        *(f"execution_term:{marker}" for marker in execution_hits),
    ]


def build_candidate_evidence(
    target: SpecialActionSearchTarget,
    records: Iterable[Any],
) -> List[Dict[str, Any]]:
    """Normalize matching announcement metadata into candidate-only evidence."""
    candidates: Dict[str, Dict[str, Any]] = {}
    for record in records:
        announcement_id = str(
            getattr(record, "source_announcement_id", "")
            or getattr(record, "announcement_id", "")
            or ""
        ).strip()
        title = str(getattr(record, "title", "") or "").strip()
        reasons = announcement_match_reasons(target, title)
        if not announcement_id or not reasons:
            continue
        attachments = tuple(getattr(record, "attachments", ()) or ())
        attachment = attachments[0] if attachments else None
        evidence_url = (
            None
            if attachment is None
            else attachment.resolved_url or attachment.source_url
        )
        if evidence_url is None:
            evidence_url = getattr(record, "adjunct_url", None)
        raw_payload = getattr(record, "raw_payload", {}) or {}
        candidates[announcement_id] = {
            "instrument_id": target.instrument_id,
            "source_event_key": target.source_event_key,
            "observation_source": "cninfo",
            "source_profile": target.source_profile,
            "evidence_source": "cninfo_announcement_metadata",
            "evidence_key": announcement_id,
            "resolution_status": "candidate",
            "effective_date": None,
            "date_basis": None,
            "announcement_id": announcement_id,
            "announcement_title": html.unescape(
                _TITLE_TAG_RE.sub("", title)
            ).strip(),
            "announcement_time": (
                getattr(record, "published_at", None)
                or getattr(record, "announcement_time", None)
            ),
            "evidence_url": evidence_url,
            "confidence": None,
            "raw_payload": {
                "event_class": target.event_class,
                "search_basis": target.search_basis,
                "search_start_date": target.start_date.isoformat(),
                "search_end_date": target.end_date.isoformat(),
                "source_anchor_dates": [
                    value.isoformat() for value in target.source_anchor_dates
                ],
                "selection_reasons": reasons,
                "announcement": dict(raw_payload),
            },
        }
    return sorted(candidates.values(), key=lambda item: item["evidence_key"])


def build_classified_announcement_evidence(
    target: SpecialActionSearchTarget,
    records: Iterable[Any],
    *,
    decisions: Mapping[str, Mapping[str, Any]],
    applicability: Mapping[str, Any],
    lineage: Sequence[Mapping[str, Any]],
    search_windows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Persist every LLM title decision as candidate or rejected evidence."""
    rows = []
    for record in records:
        announcement_id = str(
            getattr(record, "source_announcement_id", "")
            or getattr(record, "announcement_id", "")
            or ""
        ).strip()
        title = str(getattr(record, "title", "") or "").strip()
        decision = decisions.get(announcement_id)
        if not announcement_id or not title or not decision:
            continue
        resolution_status = (
            "candidate"
            if is_implementation_grade_decision(decision)
            else "rejected"
        )
        attachments = tuple(getattr(record, "attachments", ()) or ())
        attachment = attachments[0] if attachments else None
        evidence_url = (
            None
            if attachment is None
            else attachment.resolved_url or attachment.source_url
        )
        raw_payload = getattr(record, "raw_payload", {}) or {}
        rows.append({
            "instrument_id": target.instrument_id,
            "source_event_key": target.source_event_key,
            "observation_source": "cninfo",
            "source_profile": target.source_profile,
            "evidence_source": "cninfo_announcement_metadata",
            "evidence_key": announcement_id,
            "resolution_status": resolution_status,
            "effective_date": None,
            "date_basis": None,
            "announcement_id": announcement_id,
            "announcement_title": html.unescape(
                _TITLE_TAG_RE.sub("", title)
            ).strip(),
            "announcement_time": getattr(record, "published_at", None),
            "evidence_url": evidence_url,
            "confidence": decision.get("confidence"),
            "raw_payload": {
                "event_class": target.event_class,
                "candidate_effective_dates": [
                    value.isoformat() for value in target.candidate_effective_dates
                ],
                "search_windows": list(search_windows),
                "title_classification": dict(decision),
                "event_applicability": dict(applicability),
                "llm_lineage": [dict(item) for item in lineage],
                "lexical_diagnostics": announcement_match_reasons(target, title),
                "announcement": dict(raw_payload),
            },
        })
    return sorted(rows, key=lambda item: item["evidence_key"])
