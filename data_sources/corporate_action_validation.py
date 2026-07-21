"""Layered validation helpers for A-share corporate actions."""

from __future__ import annotations

import html
import math
import re
from bisect import bisect_right
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


_TITLE_TAG_RE = re.compile(r"<[^>]+>")
_IMPLEMENTED_STATUS_MARKERS = ("实施", "已实施")
_OFFICIAL_TITLE_EXCLUDES = ("可转债", "转股价格", "停止转股", "提示性公告")


def _value(item: Any, key: str, default: Any = None) -> Any:
    if isinstance(item, Mapping):
        return item.get(key, default)
    return getattr(item, key, default)


def parse_date(value: Any) -> Optional[date]:
    """Parse provider date values into a local calendar date."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    for candidate in (text[:10], text[:8]):
        for pattern in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(candidate, pattern).date()
            except ValueError:
                continue
    return None


def _cninfo_local_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return parse_date(value)
    if parsed.tzinfo is None:
        return parsed.date()
    return parsed.astimezone(timezone(timedelta(hours=8))).date()


def _float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if not math.isfinite(number) else number


def _positive_float(value: Any) -> Optional[float]:
    number = _float(value)
    return number if number > 0 else None


def _instrument_exchange(instrument_id: str) -> Optional[str]:
    normalized = str(instrument_id or "").upper()
    if normalized.endswith(".SH"):
        return "SSE"
    if normalized.endswith(".SZ"):
        return "SZSE"
    if normalized.endswith((".BJ", ".BSE")):
        return "BSE"
    return None


def _infer_instrument_id(symbol: str) -> Optional[str]:
    code = str(symbol or "").strip().zfill(6)
    if not code.isdigit() or len(code) != 6:
        return None
    if code.startswith(("4", "8", "9")):
        return f"{code}.BJ"
    if code.startswith("6"):
        return f"{code}.SH"
    return f"{code}.SZ"


def normalize_tdx_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    """Normalize persisted TDX XDXR rows while retaining per-10-share units."""
    events: Dict[tuple[str, date], Dict[str, Any]] = {}
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        ex_date = parse_date(row.get("ex_date"))
        if not instrument_id or ex_date is None or not start_date <= ex_date <= end_date:
            continue
        events[(instrument_id, ex_date)] = {
            "instrument_id": instrument_id,
            "ex_date": ex_date,
            "cash_per_10": _float(row.get("fenhong")),
            "bonus_per_10": _float(row.get("songzhuangu")),
            "rights_per_10": _float(row.get("peigu")),
            "rights_price": _float(row.get("peigujia")),
            "factor": _positive_float(row.get("factor")),
            "validation_result": str(row.get("validation_result") or ""),
            "source": "tdx_xdxr",
        }
    return sorted(events.values(), key=lambda item: (item["instrument_id"], item["ex_date"]))


def normalize_eastmoney_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    symbol_to_instrument: Mapping[str, str],
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    """Normalize implemented Eastmoney distributions returned through AkShare."""
    events: Dict[tuple[str, date], Dict[str, Any]] = {}
    for row in rows:
        status = str(row.get("方案进度") or "").strip()
        if not any(marker in status for marker in _IMPLEMENTED_STATUS_MARKERS):
            continue
        ex_date = parse_date(row.get("除权除息日"))
        if ex_date is None or not start_date <= ex_date <= end_date:
            continue
        symbol = str(row.get("代码") or "").strip().zfill(6)
        instrument_id = symbol_to_instrument.get(symbol) or _infer_instrument_id(symbol)
        if not instrument_id:
            continue
        bonus_total = row.get("送转股份-送转总比例")
        if bonus_total in (None, ""):
            bonus_total = _float(row.get("送转股份-送转比例")) + _float(
                row.get("送转股份-转股比例")
            )
        normalized = {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "name": str(row.get("名称") or "").strip(),
            "ex_date": ex_date,
            "record_date": parse_date(row.get("股权登记日")),
            "cash_per_10": _float(row.get("现金分红-现金分红比例")),
            "bonus_per_10": _float(bonus_total),
            "plan_status": status,
            "proposal_date": parse_date(row.get("预案公告日")),
            "latest_announcement_date": parse_date(row.get("最新公告日期")),
            "report_period": str(row.get("_report_period") or ""),
            "source": "eastmoney_stock_fhps",
            "adapter": "akshare.stock_fhps_em",
        }
        key = (instrument_id, ex_date)
        previous = events.get(key)
        if previous is None or (
            normalized["latest_announcement_date"] or date.min
        ) >= (previous["latest_announcement_date"] or date.min):
            events[key] = normalized
    return sorted(events.values(), key=lambda item: (item["instrument_id"], item["ex_date"]))


def _session_distance(
    left: date,
    right: date,
    sessions: Sequence[date],
) -> Optional[int]:
    if left == right:
        return 0
    if not sessions:
        return None
    if right > left:
        return bisect_right(sessions, right) - bisect_right(sessions, left)
    return -(bisect_right(sessions, left) - bisect_right(sessions, right))


def _field_differences(
    tdx_event: Mapping[str, Any],
    reference_event: Mapping[str, Any],
) -> Dict[str, float]:
    return {
        "cash_per_10": abs(
            _float(tdx_event.get("cash_per_10"))
            - _float(reference_event.get("cash_per_10"))
        ),
        "bonus_per_10": abs(
            _float(tdx_event.get("bonus_per_10"))
            - _float(reference_event.get("bonus_per_10"))
        ),
    }


def reconcile_event_fields(
    tdx_events: Sequence[Mapping[str, Any]],
    reference_events: Sequence[Mapping[str, Any]],
    *,
    trading_sessions_by_exchange: Optional[Mapping[str, Sequence[date]]] = None,
    field_tolerance: float = 0.0001,
    max_trading_session_shift: int = 3,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Reconcile explicit cash and bonus fields independently from factor paths."""
    sessions_by_exchange = trading_sessions_by_exchange or {}
    comparable_tdx = [
        dict(item)
        for item in tdx_events
        if _float(item.get("cash_per_10")) > 0
        or _float(item.get("bonus_per_10")) > 0
    ]
    unsupported = [
        dict(item)
        for item in tdx_events
        if _float(item.get("rights_per_10")) > 0
        and _float(item.get("cash_per_10")) <= 0
        and _float(item.get("bonus_per_10")) <= 0
    ]
    references = [dict(item) for item in reference_events]
    used_tdx: set[int] = set()
    used_ref: set[int] = set()
    exact_matches: List[Dict[str, Any]] = []
    shifted_matches: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []

    for tdx_idx, tdx_event in enumerate(comparable_tdx):
        for ref_idx, reference_event in enumerate(references):
            if ref_idx in used_ref:
                continue
            if tdx_event["instrument_id"] != reference_event["instrument_id"]:
                continue
            if tdx_event["ex_date"] != reference_event["ex_date"]:
                continue
            differences = _field_differences(tdx_event, reference_event)
            detail = {
                "instrument_id": tdx_event["instrument_id"],
                "tdx_ex_date": tdx_event["ex_date"],
                "reference_ex_date": reference_event["ex_date"],
                "tdx_cash_per_10": tdx_event.get("cash_per_10", 0.0),
                "reference_cash_per_10": reference_event.get("cash_per_10", 0.0),
                "tdx_bonus_per_10": tdx_event.get("bonus_per_10", 0.0),
                "reference_bonus_per_10": reference_event.get("bonus_per_10", 0.0),
                "cash_difference": differences["cash_per_10"],
                "bonus_difference": differences["bonus_per_10"],
                "source": reference_event.get("source"),
                "adapter": reference_event.get("adapter"),
                "trading_session_distance": 0,
            }
            used_tdx.add(tdx_idx)
            used_ref.add(ref_idx)
            if max(differences.values()) <= field_tolerance:
                detail["reason"] = "exact_event_field_match"
                exact_matches.append(detail)
            else:
                detail["reason"] = "same_date_event_field_conflict"
                conflicts.append(detail)
            break

    shifted_candidates = []
    for tdx_idx, tdx_event in enumerate(comparable_tdx):
        if tdx_idx in used_tdx:
            continue
        exchange = _instrument_exchange(str(tdx_event["instrument_id"])) or ""
        sessions = sessions_by_exchange.get(exchange, [])
        for ref_idx, reference_event in enumerate(references):
            if ref_idx in used_ref:
                continue
            if tdx_event["instrument_id"] != reference_event["instrument_id"]:
                continue
            distance = _session_distance(
                tdx_event["ex_date"], reference_event["ex_date"], sessions
            )
            if distance is None or abs(distance) > max_trading_session_shift:
                continue
            differences = _field_differences(tdx_event, reference_event)
            if max(differences.values()) > field_tolerance:
                continue
            shifted_candidates.append((
                abs(distance),
                abs((reference_event["ex_date"] - tdx_event["ex_date"]).days),
                tdx_idx,
                ref_idx,
                distance,
                differences,
            ))
    for _, _, tdx_idx, ref_idx, distance, differences in sorted(shifted_candidates):
        if tdx_idx in used_tdx or ref_idx in used_ref:
            continue
        used_tdx.add(tdx_idx)
        used_ref.add(ref_idx)
        tdx_event = comparable_tdx[tdx_idx]
        reference_event = references[ref_idx]
        shifted_matches.append({
            "instrument_id": tdx_event["instrument_id"],
            "tdx_ex_date": tdx_event["ex_date"],
            "reference_ex_date": reference_event["ex_date"],
            "tdx_cash_per_10": tdx_event.get("cash_per_10", 0.0),
            "reference_cash_per_10": reference_event.get("cash_per_10", 0.0),
            "tdx_bonus_per_10": tdx_event.get("bonus_per_10", 0.0),
            "reference_bonus_per_10": reference_event.get("bonus_per_10", 0.0),
            "cash_difference": differences["cash_per_10"],
            "bonus_difference": differences["bonus_per_10"],
            "trading_session_distance": distance,
            "source": reference_event.get("source"),
            "adapter": reference_event.get("adapter"),
            "reason": "shifted_event_field_match",
        })

    tdx_only = [
        {**event, "reason": "tdx_event_missing_from_eastmoney"}
        for idx, event in enumerate(comparable_tdx)
        if idx not in used_tdx
    ]
    reference_only = [
        {**event, "reason": "eastmoney_event_missing_from_tdx"}
        for idx, event in enumerate(references)
        if idx not in used_ref
    ]
    totals = {
        "tdx_comparable_events": len(comparable_tdx),
        "eastmoney_implemented_events": len(references),
        "exact_event_field_matches": len(exact_matches),
        "shifted_event_field_matches": len(shifted_matches),
        "event_field_conflicts": len(conflicts),
        "tdx_event_only": len(tdx_only),
        "eastmoney_event_only": len(reference_only),
        "unsupported_rights_only_tdx_events": len(unsupported),
    }
    status = (
        "partial"
        if conflicts or tdx_only or reference_only
        else "success"
    )
    return {
        "status": status,
        "source": "eastmoney_stock_fhps",
        "adapter": "akshare.stock_fhps_em",
        "field_unit": "per_10_shares",
        "matching_policy": {
            "field_tolerance": field_tolerance,
            "max_trading_session_shift": max_trading_session_shift,
        },
        "totals": totals,
        "exact_match_samples": exact_matches[:sample_limit],
        "shifted_match_samples": shifted_matches[:sample_limit],
        "field_conflict_samples": conflicts[:sample_limit],
        "tdx_only_samples": tdx_only[:sample_limit],
        "eastmoney_only_samples": reference_only[:sample_limit],
        "unsupported_samples": unsupported[:sample_limit],
        "follow_up_instrument_ids": sorted({
            str(item["instrument_id"])
            for item in conflicts + tdx_only + reference_only
        }),
    }


def _provider_day_factors(
    rows: Iterable[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
) -> Dict[tuple[str, str], List[tuple[date, float]]]:
    groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        source = str(row.get("source") or "unknown").strip().lower()
        ex_date = parse_date(row.get("ex_date"))
        if instrument_id and ex_date is not None and ex_date <= end_date:
            groups.setdefault((instrument_id, source), []).append({**row, "ex_date": ex_date})

    result: Dict[tuple[str, str], List[tuple[date, float]]] = {}
    for key, items in groups.items():
        previous_cumulative: Optional[float] = None
        day_factors: List[tuple[date, float]] = []
        for item in sorted(items, key=lambda value: value["ex_date"]):
            cumulative = _positive_float(item.get("cumulative_factor"))
            stored_factor = _positive_float(item.get("factor"))
            day_factor: Optional[float] = None
            if cumulative is not None and previous_cumulative is not None:
                day_factor = cumulative / previous_cumulative
            elif stored_factor is not None and abs(stored_factor - 1.0) > 1e-12:
                day_factor = stored_factor
            elif cumulative is not None and abs(cumulative - 1.0) > 1e-12:
                day_factor = cumulative
            if cumulative is not None:
                previous_cumulative = cumulative
            if (
                day_factor is not None
                and day_factor > 0
                and start_date <= item["ex_date"] <= end_date
                and abs(day_factor - 1.0) > 1e-12
            ):
                day_factors.append((item["ex_date"], day_factor))
        result[key] = day_factors
    return result


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * min(1.0, max(0.0, quantile))
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def compare_cumulative_factor_paths(
    tdx_events: Sequence[Mapping[str, Any]],
    reference_factor_rows: Sequence[Mapping[str, Any]],
    *,
    start_date: date,
    end_date: date,
    reference_sources: Sequence[str],
    instrument_ids: Optional[Sequence[str]] = None,
    acceptable_error_pct: float = 0.1,
    warning_error_pct: float = 0.5,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Compare unit-baseline cumulative paths at year-end and latest anchors."""
    tdx_by_instrument: Dict[str, List[tuple[date, float]]] = {}
    for event in tdx_events:
        factor = _positive_float(event.get("factor"))
        ex_date = parse_date(event.get("ex_date"))
        instrument_id = str(event.get("instrument_id") or "").strip()
        if factor and ex_date and instrument_id and not str(
            event.get("validation_result") or ""
        ).startswith("pending_"):
            tdx_by_instrument.setdefault(instrument_id, []).append((ex_date, factor))
    for items in tdx_by_instrument.values():
        items.sort()

    source_names = [str(item).strip().lower() for item in reference_sources if str(item).strip()]
    reference_paths = _provider_day_factors(
        reference_factor_rows,
        start_date=start_date,
        end_date=end_date,
    )
    universe = set(tdx_by_instrument)
    universe.update(key[0] for key in reference_paths)
    anchors = [
        min(end_date, date(year, 12, 31))
        for year in range(start_date.year, end_date.year + 1)
        if min(end_date, date(year, 12, 31)) >= start_date
    ]
    if end_date not in anchors:
        anchors.append(end_date)
    anchors = sorted(set(anchors))

    acceptable_threshold = acceptable_error_pct / 100.0
    warning_threshold = warning_error_pct / 100.0
    path_summaries: List[Dict[str, Any]] = []
    anchor_conflicts: List[Dict[str, Any]] = []
    unavailable: List[Dict[str, Any]] = []

    for instrument_id in sorted(universe):
        tdx_factors = tdx_by_instrument.get(instrument_id, [])
        for source in source_names:
            reference_factors = reference_paths.get((instrument_id, source))
            if reference_factors is None:
                unavailable.append({
                    "instrument_id": instrument_id,
                    "source": source,
                    "reason": "reference_factor_path_unavailable",
                })
                continue
            tdx_cumulative = 1.0
            reference_cumulative = 1.0
            tdx_index = 0
            reference_index = 0
            anchor_rows: List[Dict[str, Any]] = []
            for anchor in anchors:
                while tdx_index < len(tdx_factors) and tdx_factors[tdx_index][0] <= anchor:
                    tdx_cumulative *= tdx_factors[tdx_index][1]
                    tdx_index += 1
                while (
                    reference_index < len(reference_factors)
                    and reference_factors[reference_index][0] <= anchor
                ):
                    reference_cumulative *= reference_factors[reference_index][1]
                    reference_index += 1
                error = (
                    abs((tdx_cumulative / reference_cumulative) - 1.0)
                    if reference_cumulative > 0
                    else math.inf
                )
                classification = (
                    "acceptable"
                    if error <= acceptable_threshold
                    else ("warning" if error <= warning_threshold else "conflict")
                )
                anchor_row = {
                    "instrument_id": instrument_id,
                    "source": source,
                    "anchor_date": anchor,
                    "tdx_cumulative_factor": tdx_cumulative,
                    "reference_cumulative_factor": reference_cumulative,
                    "error_pct": error * 100.0,
                    "classification": classification,
                }
                anchor_rows.append(anchor_row)
                if classification == "conflict":
                    anchor_conflicts.append(anchor_row)
            latest = anchor_rows[-1]
            path_summaries.append({
                "instrument_id": instrument_id,
                "source": source,
                "latest_anchor_date": latest["anchor_date"],
                "latest_error_pct": latest["error_pct"],
                "latest_classification": latest["classification"],
                "max_error_pct": max(row["error_pct"] for row in anchor_rows),
                "conflict_anchor_count": sum(
                    row["classification"] == "conflict" for row in anchor_rows
                ),
                "warning_anchor_count": sum(
                    row["classification"] == "warning" for row in anchor_rows
                ),
            })

    latest_errors = [item["latest_error_pct"] for item in path_summaries]
    latest_distribution = Counter(
        item["latest_classification"] for item in path_summaries
    )
    instruments_with_reference = {key[0] for key in reference_paths}
    instruments_without_reference = sorted(
        set(tdx_by_instrument) - instruments_with_reference
    )
    status = (
        "partial"
        if latest_distribution.get("conflict", 0)
        or latest_distribution.get("warning", 0)
        or anchor_conflicts
        or unavailable
        or instruments_without_reference
        else ("unavailable" if not path_summaries else "success")
    )
    return {
        "status": status,
        "reference_sources": source_names,
        "anchor_policy": "calendar_year_end_and_latest",
        "thresholds": {
            "acceptable_error_pct": acceptable_error_pct,
            "warning_error_pct": warning_error_pct,
        },
        "totals": {
            "instrument_source_paths_compared": len(path_summaries),
            "latest_acceptable": latest_distribution.get("acceptable", 0),
            "latest_warning": latest_distribution.get("warning", 0),
            "latest_conflict": latest_distribution.get("conflict", 0),
            "historical_conflict_anchors": len(anchor_conflicts),
            "reference_paths_unavailable": len(unavailable),
            "instruments_without_reference_path": len(
                instruments_without_reference
            ),
            "latest_error_p50_pct": _percentile(latest_errors, 0.50),
            "latest_error_p95_pct": _percentile(latest_errors, 0.95),
            "latest_error_max_pct": max(latest_errors, default=0.0),
        },
        "path_samples": sorted(
            path_summaries,
            key=lambda item: item["max_error_pct"],
            reverse=True,
        )[:sample_limit],
        "anchor_conflict_samples": sorted(
            anchor_conflicts,
            key=lambda item: item["error_pct"],
            reverse=True,
        )[:sample_limit],
        "unavailable_samples": unavailable[:sample_limit],
        "instruments_without_reference_samples": instruments_without_reference[
            :sample_limit
        ],
    }


def normalize_official_implementation_announcements(
    records: Iterable[Any],
    *,
    symbol_to_instrument: Mapping[str, str],
) -> List[Dict[str, Any]]:
    """Normalize official implementation-announcement metadata only."""
    normalized: Dict[tuple[str, str], Dict[str, Any]] = {}
    for record in records:
        raw_title = str(_value(record, "title", "") or "")
        title = html.unescape(_TITLE_TAG_RE.sub("", raw_title)).strip()
        if "权益分派" not in title or "实施公告" not in title:
            continue
        if any(marker in title for marker in _OFFICIAL_TITLE_EXCLUDES):
            continue
        announcement_id = str(
            _value(record, "source_announcement_id", "")
            or _value(record, "announcement_id", "")
            or ""
        ).strip()
        symbols = _value(record, "symbols", []) or []
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip().zfill(6)
            instrument_id = symbol_to_instrument.get(symbol) or _infer_instrument_id(symbol)
            if not instrument_id:
                continue
            item = {
                "instrument_id": instrument_id,
                "symbol": symbol,
                "announcement_id": announcement_id,
                "announcement_date": _cninfo_local_date(
                    _value(record, "published_at")
                    or _value(record, "announcement_time")
                ),
                "title": title,
                "adjunct_url": _official_attachment_url(record),
                "source": "cninfo_announcement_metadata",
                "evidence_scope": "implementation_announcement_exists",
            }
            normalized[(instrument_id, announcement_id)] = item
    return sorted(
        normalized.values(),
        key=lambda item: (item["instrument_id"], item["announcement_date"] or date.min),
    )


def _official_attachment_url(record: Any) -> Optional[str]:
    attachments = _value(record, "attachments", ()) or ()
    if attachments:
        attachment = attachments[0]
        return _value(attachment, "resolved_url") or _value(
            attachment, "source_url"
        )
    return _value(record, "adjunct_url")


def match_official_announcement_evidence(
    events: Sequence[Mapping[str, Any]],
    announcements: Sequence[Mapping[str, Any]],
    *,
    lookback_days: int = 180,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Match official metadata as existence evidence, not amount validation."""
    by_instrument: Dict[str, List[Mapping[str, Any]]] = {}
    for item in announcements:
        by_instrument.setdefault(str(item.get("instrument_id")), []).append(item)
    matched: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []
    for event in events:
        ex_date = parse_date(event.get("ex_date"))
        instrument_id = str(event.get("instrument_id") or "")
        if ex_date is None or not instrument_id:
            continue
        candidates = [
            item
            for item in by_instrument.get(instrument_id, [])
            if item.get("announcement_date") is not None
            and ex_date - timedelta(days=lookback_days)
            <= item["announcement_date"]
            <= ex_date
        ]
        if candidates:
            evidence = max(candidates, key=lambda item: item["announcement_date"])
            matched.append({
                "instrument_id": instrument_id,
                "ex_date": ex_date,
                "announcement_id": evidence.get("announcement_id"),
                "announcement_date": evidence.get("announcement_date"),
                "title": evidence.get("title"),
                "adjunct_url": evidence.get("adjunct_url"),
                "evidence_scope": "implementation_announcement_exists",
                "reason": "official_implementation_announcement_found",
            })
        else:
            unmatched.append({
                "instrument_id": instrument_id,
                "ex_date": ex_date,
                "reason": "official_implementation_announcement_not_found_in_scan",
            })
    return {
        "status": "partial" if unmatched else "success",
        "evidence_scope": "announcement_existence_only",
        "totals": {
            "events_checked": len(matched) + len(unmatched),
            "official_announcement_evidence_found": len(matched),
            "official_announcement_evidence_not_found": len(unmatched),
            "official_announcements_scanned": len(announcements),
        },
        "matched_samples": matched[:sample_limit],
        "unmatched_samples": unmatched[:sample_limit],
    }
