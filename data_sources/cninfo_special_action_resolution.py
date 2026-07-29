"""Bounded CNInfo announcement discovery for special corporate actions."""

from __future__ import annotations

import html
import json
import math
import re
import unicodedata
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


_TITLE_TAG_RE = re.compile(r"<[^>]+>")
_SHARE_REFORM_MARKERS = ("股权分置", "股改", "对价")
_COMPENSATION_MARKERS = ("赠与", "补偿股份", "业绩承诺股份", "股份过户")
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
    "赠与",
    "补偿股份",
    "业绩承诺股份",
    "股份过户",
)
_TITLE_EXCLUDES = ("取消", "终止", "不予实施", "不实施")
_ANNUAL_PERIOD_MARKERS = ("年度", "年报")
_INTERIM_PERIOD_MARKERS = ("半年", "中报", "中期")
_TITLE_PREFILTER_STRONG_EXCLUSIONS = {
    "法律意见书": "legal_opinion",
    "表决结果": "voting_result",
    "补充流动资金": "working_capital",
    "季度报告": "quarterly_report",
    "独立意见": "independent_opinion",
    "回复": "reply",
    "权益变动报告书": "ownership_change_report",
    "评估报告": "valuation_report",
    "年度财务报告": "annual_financial_report",
    "年度报告": "annual_report",
    "裁定书": "court_ruling",
    "质押": "share_pledge",
}
_TITLE_PREFILTER_ROLE_MARKERS = {
    "董事会": "board_material",
    "监事会": "supervisory_board_material",
    "独立董事": "independent_director_material",
    "股东大会": "shareholder_meeting_material",
}
_TITLE_PREFILTER_IMPLEMENTATION_PROTECTION = (
    "实施公告",
    "实施方案",
    "实施完成",
    "实施完毕",
    "权益分派实施",
    "除权除息",
    "股份到账",
    "派发",
    "复牌",
    "上市公告",
)

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


def classify_cninfo_announcement_title_prefilter(
    title: Any,
) -> Dict[str, Any]:
    """Classify clearly non-implementation titles without external work."""
    normalized_title = unicodedata.normalize(
        "NFKC",
        html.unescape(_TITLE_TAG_RE.sub("", str(title or ""))),
    ).strip()
    protected = [
        marker for marker in _TITLE_PREFILTER_IMPLEMENTATION_PROTECTION
        if marker in normalized_title
    ]
    if not protected and "方案实施" in normalized_title:
        protected = ["方案实施"]
    for marker, reason in _TITLE_PREFILTER_STRONG_EXCLUSIONS.items():
        if marker in normalized_title and not protected:
            return {
                "excluded": True,
                "reason": reason,
                "matched_keywords": [marker],
                "protected_markers": [],
            }
    if "国资委批准" in normalized_title and not protected:
        return {
            "excluded": True,
            "reason": "sasac_approval",
            "matched_keywords": ["国资委批准"],
            "protected_markers": [],
        }
    if "过户" in normalized_title and not protected:
        return {
            "excluded": True,
            "reason": "transfer_registration",
            "matched_keywords": ["过户"],
            "protected_markers": [],
        }
    if not protected:
        role_hits = [
            marker for marker in _TITLE_PREFILTER_ROLE_MARKERS
            if marker in normalized_title
        ]
        if role_hits:
            return {
                "excluded": True,
                "reason": _TITLE_PREFILTER_ROLE_MARKERS[role_hits[0]],
                "matched_keywords": role_hits,
                "protected_markers": [],
            }
    return {
        "excluded": False,
        "reason": None,
        "matched_keywords": [],
        "protected_markers": protected,
    }


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


def classify_special_action(
    row: Mapping[str, Any],
    *,
    allow_complete_event: bool = False,
) -> Optional[str]:
    """Classify partial events that warrant official announcement discovery."""
    quality_status = str(row.get("quality_status") or "")
    if (
        not quality_status.startswith("partial_")
        and not allow_complete_event
    ):
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
    allow_complete_event: bool = False,
) -> List[SpecialActionSearchTarget]:
    """Build independent bounded windows from nearby date-role clusters."""
    event_class = classify_special_action(
        row,
        allow_complete_event=allow_complete_event,
    )
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
    allow_complete_event: bool = False,
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
        allow_complete_event=allow_complete_event,
    )
    return targets[0] if targets else None


def announcement_match_reasons(
    target: SpecialActionSearchTarget,
    title: str,
) -> List[str]:
    """Return auditable title-match reasons without resolving an effective date."""
    diagnostics = deterministic_title_match(
        target.event_class,
        target.row.get("fiscal_period"),
        title,
        target.row.get("action_type"),
    )
    if diagnostics["status"] != "accepted":
        return []
    if not diagnostics["lexical_match"]:
        return []
    return list(diagnostics["selection_reasons"])


def deterministic_title_match(
    event_class: str,
    fiscal_period: Any,
    title: Any,
    action_type: Any = None,
) -> Dict[str, Any]:
    """Apply deterministic title gates and return an auditable decision."""
    normalized = unicodedata.normalize(
        "NFKC",
        html.unescape(_TITLE_TAG_RE.sub("", str(title or ""))),
    ).strip()
    if not normalized:
        return {
            "status": "rejected",
            "reason": "empty_title",
            "selection_reasons": [],
            "lexical_match": False,
        }
    mismatch = period_mismatch_reason(fiscal_period, normalized)
    if mismatch:
        return {
            "status": "rejected",
            "reason": mismatch,
            "selection_reasons": [],
            "lexical_match": False,
        }
    class_markers = {
        "share_reform": _SHARE_REFORM_MARKERS + _COMPENSATION_MARKERS,
        "restructuring_capitalization": _RESTRUCTURING_MARKERS,
        "missing_date_distribution": _GENERIC_ACTION_MARKERS,
    }.get(str(event_class or "").strip(), _GENERIC_ACTION_MARKERS)
    action_hits = [marker for marker in class_markers if marker in normalized]
    execution_hits = [marker for marker in _EXECUTION_MARKERS if marker in normalized]
    exclusion_hits = [marker for marker in _TITLE_EXCLUDES if marker in normalized]
    normalized_action_type = str(action_type or "").strip().lower()
    if (
        "配股" in action_hits
        and normalized_action_type != "rights"
        and not any(marker != "配股" for marker in action_hits)
    ):
        return {
            "status": "rejected",
            "reason": "incompatible_action_term:rights_issue_for_non_rights_event",
            "selection_reasons": [],
            "lexical_match": False,
        }
    reasons = [
        f"event_class:{event_class}",
        *(f"action_term:{marker}" for marker in action_hits),
        *(f"execution_term:{marker}" for marker in execution_hits),
    ]
    return {
        "status": "accepted",
        "reason": None,
        "selection_reasons": reasons,
        "lexical_match": bool(
            action_hits and execution_hits and not exclusion_hits
        ),
        "lexical_exclusion_hits": exclusion_hits,
    }


def _period_mismatch_reason(
    target: SpecialActionSearchTarget,
    title: str,
) -> Optional[str]:
    """Return a deterministic period mismatch for an announcement title.

    CNInfo often returns several historical notices in one search window.  A
    generic LLM title decision is insufficient to distinguish a half-year
    observation from an annual notice, so explicit period wording is treated
    as a hard candidate boundary.
    """
    return period_mismatch_reason(
        target.row.get("fiscal_period"),
        title,
    )


def period_mismatch_reason(
    fiscal_period: Any,
    title: Any,
) -> Optional[str]:
    """Return a hard period mismatch between an event and announcement title.

    This helper is intentionally independent of a search target so the
    operator audit can re-evaluate stale persisted candidates without
    re-running the network discovery stage.
    """
    fiscal_period = unicodedata.normalize(
        "NFKC",
        str(fiscal_period or ""),
    ).strip()
    if not fiscal_period:
        return None
    title_text = unicodedata.normalize("NFKC", str(title or "")).strip()
    is_interim_event = any(marker in fiscal_period for marker in _INTERIM_PERIOD_MARKERS)
    is_annual_event = (
        not is_interim_event
        and any(marker in fiscal_period for marker in _ANNUAL_PERIOD_MARKERS)
    )
    title_is_interim = any(marker in title_text for marker in _INTERIM_PERIOD_MARKERS)
    title_is_annual = (
        not title_is_interim
        and any(marker in title_text for marker in _ANNUAL_PERIOD_MARKERS)
    )
    if is_interim_event and title_is_annual:
        return "period_mismatch:interim_event_with_annual_notice"
    if is_annual_event and title_is_interim:
        return "period_mismatch:annual_event_with_interim_notice"
    fiscal_year = re.search(r"(?:19|20)\d{2}", fiscal_period)
    title_years = set(re.findall(r"(?:19|20)\d{2}", title_text))
    if fiscal_year and title_years and fiscal_year.group(0) not in title_years:
        return (
            "period_mismatch:fiscal_year="
            + fiscal_year.group(0)
            + ":title_year="
            + ",".join(sorted(title_years))
        )
    return None


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


def build_prefiltered_announcement_evidence(
    target: SpecialActionSearchTarget,
    filtered_records: Iterable[tuple[Any, Mapping[str, Any]]],
    *,
    search_windows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Persist deterministic title exclusions as rejected CNInfo evidence."""
    rows = []
    for record, filter_decision in filtered_records:
        announcement_id = str(
            getattr(record, "source_announcement_id", "")
            or getattr(record, "announcement_id", "")
            or ""
        ).strip()
        title = str(getattr(record, "title", "") or "").strip()
        if not announcement_id or not title:
            continue
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
            "resolution_status": "rejected",
            "effective_date": None,
            "date_basis": None,
            "announcement_id": announcement_id,
            "announcement_title": html.unescape(
                _TITLE_TAG_RE.sub("", title)
            ).strip(),
            "announcement_time": getattr(record, "published_at", None),
            "evidence_url": evidence_url,
            "confidence": 1.0,
            "raw_payload": {
                "event_class": target.event_class,
                "search_windows": list(search_windows),
                "title_prefilter": {
                    **dict(filter_decision),
                    "policy_version": "cninfo_title_prefilter_v1",
                },
                "announcement": dict(raw_payload),
            },
        })
    return sorted(rows, key=lambda item: item["evidence_key"])


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
        normalized_title = unicodedata.normalize(
            "NFKC",
            html.unescape(_TITLE_TAG_RE.sub("", title)),
        ).strip()
        deterministic = deterministic_title_match(
            target.event_class,
            target.row.get("fiscal_period"),
            normalized_title,
            target.row.get("action_type"),
        )
        resolution_status = (
            "candidate"
            if is_implementation_grade_decision(decision)
            else "rejected"
        )
        if deterministic["status"] != "accepted":
            resolution_status = "rejected"
        lexical_diagnostics = list(deterministic["selection_reasons"])
        rejection_reason = (
            deterministic["reason"]
            if deterministic["status"] != "accepted"
            else None
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
                "deterministic_match": {
                    "status": "rejected" if rejection_reason else "accepted",
                    "reason": rejection_reason,
                },
                "event_applicability": dict(applicability),
                "llm_lineage": [dict(item) for item in lineage],
                "lexical_diagnostics": lexical_diagnostics,
                "announcement": dict(raw_payload),
            },
        })
    return sorted(rows, key=lambda item: item["evidence_key"])
