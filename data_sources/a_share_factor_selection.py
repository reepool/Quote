"""Deterministic three-source selection for A-share factor candidates."""

from __future__ import annotations

import math
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from data_sources.a_share_factor_source_overrides import (
    ReviewedFactorSourceOverride,
)

BAOSTOCK_SINA_SOURCE = "baostock_sina_composite"
SOURCE_ORDER = ("cninfo", "tdx", BAOSTOCK_SINA_SOURCE)

FACTOR_DIFFERENCE_BUCKETS = (
    ("le_0_01_pct", 0.0001),
    ("0_01_to_0_1_pct", 0.001),
    ("0_1_to_0_5_pct", 0.005),
    ("0_5_to_1_pct", 0.01),
)

LEGACY_NO_CHANGE_TOLERANCE = 1e-12
LEGACY_SOURCE_SWITCH_REL_TOLERANCE = 0.001


def _factor_difference_buckets(
    differences: Iterable[float],
) -> Dict[str, int]:
    counts = {
        label: 0
        for label, _ in FACTOR_DIFFERENCE_BUCKETS
    }
    counts["gt_1_pct"] = 0
    for difference in differences:
        for label, upper_bound in FACTOR_DIFFERENCE_BUCKETS:
            if difference <= upper_bound:
                counts[label] += 1
                break
        else:
            counts["gt_1_pct"] += 1
    return counts


def _date(value: Any) -> Optional[date]:
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


def _positive(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def normalize_baostock_sina_composite_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    no_change_tolerance: float = LEGACY_NO_CHANGE_TOLERANCE,
    source_switch_rel_tolerance: float = (
        LEGACY_SOURCE_SWITCH_REL_TOLERANCE
    ),
) -> List[Dict[str, Any]]:
    """Convert BaoStock-Sina cumulative levels into adjacent event ratios.

    BaoStock history may store its cumulative level in both ``factor`` and
    ``cumulative_factor``. Sina tail rows normally rebase onto that cumulative
    chain, but older direct writers may preserve the Sina absolute cumulative
    basis. At a provider switch, a materially inconsistent cumulative ratio is
    therefore replaced by the stored adjacent event factor. The returned rows
    rebuild one internal cumulative chain while retaining the provider level
    for audit; the source table is not changed.
    """

    grouped: Dict[str, Dict[date, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        ex_date = _date(row.get("ex_date"))
        if instrument_id and ex_date is not None:
            grouped[instrument_id][ex_date] = row

    normalized: List[Dict[str, Any]] = []
    for instrument_id, dated_rows in sorted(grouped.items()):
        previous_cumulative: Optional[float] = None
        previous_upstream_source: Optional[str] = None
        normalized_cumulative = 1.0
        for ex_date, row in sorted(dated_rows.items()):
            cumulative = _positive(row.get("cumulative_factor"))
            stored_factor = _positive(row.get("factor"))
            upstream_source = str(row.get("source") or "unknown").strip().lower()
            if cumulative is None:
                normalized.append({
                    **dict(row),
                    "instrument_id": instrument_id,
                    "ex_date": ex_date,
                    "normalized_factor": None,
                    "source": BAOSTOCK_SINA_SOURCE,
                    "upstream_source": upstream_source,
                    "source_profile": BAOSTOCK_SINA_SOURCE,
                    "composite_normalization_method": "invalid_cumulative",
                    "composite_basis_conflict": False,
                })
                previous_cumulative = None
                previous_upstream_source = None
                normalized_cumulative = 1.0
                continue

            cumulative_ratio = (
                cumulative / previous_cumulative
                if previous_cumulative is not None
                else None
            )
            provider_switched = (
                previous_upstream_source is not None
                and upstream_source != previous_upstream_source
            )
            initial_source_unbridged = (
                previous_cumulative is None
                and upstream_source != "baostock"
                and stored_factor is None
            )
            if initial_source_unbridged:
                normalized.append({
                    **dict(row),
                    "instrument_id": instrument_id,
                    "ex_date": ex_date,
                    "normalized_factor": None,
                    "provider_cumulative_factor": cumulative,
                    "source": BAOSTOCK_SINA_SOURCE,
                    "upstream_source": upstream_source,
                    "source_profile": BAOSTOCK_SINA_SOURCE,
                    "composite_normalization_method": (
                        "invalid_initial_source_factor"
                    ),
                    "composite_basis_conflict": False,
                    "composite_cumulative_ratio": None,
                    "composite_stored_factor": None,
                })
                previous_cumulative = cumulative
                previous_upstream_source = upstream_source
                normalized_cumulative = 1.0
                continue
            if provider_switched and stored_factor is None:
                normalized.append({
                    **dict(row),
                    "instrument_id": instrument_id,
                    "ex_date": ex_date,
                    "normalized_factor": None,
                    "provider_cumulative_factor": cumulative,
                    "source": BAOSTOCK_SINA_SOURCE,
                    "upstream_source": upstream_source,
                    "source_profile": BAOSTOCK_SINA_SOURCE,
                    "composite_normalization_method": (
                        "invalid_source_switch_factor"
                    ),
                    "composite_basis_conflict": True,
                    "composite_cumulative_ratio": cumulative_ratio,
                    "composite_stored_factor": None,
                })
                previous_cumulative = cumulative
                previous_upstream_source = upstream_source
                normalized_cumulative = 1.0
                continue
            basis_conflict = bool(
                provider_switched
                and cumulative_ratio is not None
                and stored_factor is not None
                and not math.isclose(
                    cumulative_ratio,
                    stored_factor,
                    rel_tol=max(0.0, source_switch_rel_tolerance),
                    abs_tol=max(0.0, no_change_tolerance),
                )
            )
            if basis_conflict:
                factor = stored_factor
                normalization_method = "stored_factor_at_source_switch"
            elif cumulative_ratio is not None:
                factor = cumulative_ratio
                normalization_method = "cumulative_ratio"
            elif upstream_source == "baostock":
                factor = cumulative
                normalization_method = "initial_cumulative"
            elif stored_factor is not None:
                factor = stored_factor
                normalization_method = "initial_stored_factor"
            else:
                factor = cumulative
                normalization_method = "initial_cumulative"

            previous_cumulative = cumulative
            previous_upstream_source = upstream_source
            normalized_cumulative *= factor
            if abs(factor - 1.0) <= max(0.0, no_change_tolerance):
                continue
            normalized.append({
                **dict(row),
                "instrument_id": instrument_id,
                "ex_date": ex_date,
                "factor": factor,
                "normalized_factor": factor,
                "cumulative_factor": normalized_cumulative,
                "provider_cumulative_factor": cumulative,
                "source": BAOSTOCK_SINA_SOURCE,
                "upstream_source": upstream_source,
                "source_profile": BAOSTOCK_SINA_SOURCE,
                "composite_normalization_method": normalization_method,
                "composite_basis_conflict": basis_conflict,
                "composite_cumulative_ratio": cumulative_ratio,
                "composite_stored_factor": stored_factor,
            })
    return normalized


def build_continuity_segments(
    *,
    instrument_id: str,
    start_date: date,
    end_date: date,
    lineage: Optional[Mapping[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Split a requested range at reviewed non-continuous transitions."""

    boundaries = sorted({
        parsed
        for transition in (lineage or {}).get("transitions") or ()
        if isinstance(transition, Mapping)
        and str(transition.get("price_continuity") or "").strip().lower()
        == "non_continuous"
        if (parsed := _date(transition.get("effective_date"))) is not None
        and start_date < parsed <= end_date
    })
    segments: List[Dict[str, Any]] = []
    segment_start = start_date
    for index, boundary in enumerate([*boundaries, None], start=1):
        segment_end = (
            boundary - timedelta(days=1) if boundary is not None else end_date
        )
        segments.append({
            "instrument_id": instrument_id,
            "segment_id": f"{instrument_id}:{index}",
            "start_date": segment_start,
            "end_date": segment_end,
            "reset_at_start": index > 1,
        })
        if boundary is not None:
            segment_start = boundary
    return segments


def _source_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    default_source: str,
) -> Tuple[Dict[str, List[Dict[str, Any]]], set[str]]:
    grouped: Dict[str, Dict[date, Dict[str, Any]]] = defaultdict(dict)
    invalid_instruments: set[str] = set()
    for source_row in rows:
        instrument_id = str(
            source_row.get("instrument_id") or ""
        ).strip()
        ex_date = _date(
            source_row.get("effective_date", source_row.get("ex_date"))
        )
        factor = _positive(
            source_row.get("normalized_factor", source_row.get("factor"))
        )
        if not instrument_id:
            continue
        if ex_date is None:
            continue
        if factor is None:
            invalid_instruments.add(instrument_id)
            continue
        row = dict(source_row)
        row.update({
            "instrument_id": instrument_id,
            "ex_date": ex_date,
            "factor": factor,
            "source": str(
                source_row.get("source") or default_source
            ).strip().lower(),
        })
        grouped[instrument_id][ex_date] = row
    return {
        instrument_id: [
            dated_rows[key] for key in sorted(dated_rows)
        ]
        for instrument_id, dated_rows in grouped.items()
    }, invalid_instruments


def _exchange(instrument_id: str) -> Optional[str]:
    normalized = instrument_id.upper()
    if normalized.endswith(".SH"):
        return "SSE"
    if normalized.endswith(".SZ"):
        return "SZSE"
    if normalized.endswith((".BJ", ".BSE")):
        return "BSE"
    return None


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


def compare_segment_paths(
    left_rows: Sequence[Mapping[str, Any]],
    right_rows: Sequence[Mapping[str, Any]],
    *,
    market_sessions: Sequence[date] = (),
    factor_relative_tolerance: float = 0.001,
    cumulative_relative_tolerance: float = 0.001,
    max_session_shift: int = 3,
) -> Dict[str, Any]:
    """Require both event-level and cumulative-path agreement."""

    left = sorted(left_rows, key=lambda row: row["ex_date"])
    right = sorted(right_rows, key=lambda row: row["ex_date"])
    if not left and not right:
        return {
            "agrees": True,
            "event_matches": 0,
            "exact_matches": 0,
            "shifted_matches": 0,
            "event_conflicts": 0,
            "left_only": 0,
            "right_only": 0,
            "factor_difference_buckets": _factor_difference_buckets(()),
            "max_cumulative_relative_error": 0.0,
        }

    # Ordered dynamic matching prevents adjacent shifted events from consuming
    # one another before factor compatibility is considered.
    matches_by_state: Dict[
        Tuple[int, int], Tuple[int, float, float, Tuple[Tuple[int, int], ...]]
    ] = {(0, 0): (0, 0.0, 0.0, ())}
    for left_index in range(len(left) + 1):
        for right_index in range(len(right) + 1):
            current = matches_by_state.get((left_index, right_index))
            if current is None:
                continue
            for next_state in (
                (left_index + 1, right_index),
                (left_index, right_index + 1),
            ):
                if (
                    next_state[0] <= len(left)
                    and next_state[1] <= len(right)
                ):
                    existing = matches_by_state.get(next_state)
                    if existing is None or current[:3] > existing[:3]:
                        matches_by_state[next_state] = current
            if left_index >= len(left) or right_index >= len(right):
                continue
            left_row = left[left_index]
            right_row = right[right_index]
            distance = _session_distance(
                left_row["ex_date"],
                right_row["ex_date"],
                market_sessions,
            )
            if distance is None:
                if left_row["ex_date"] != right_row["ex_date"]:
                    continue
                distance = 0
            if abs(distance) > max_session_shift:
                continue
            factor_error = abs(
                float(left_row["factor"]) / float(right_row["factor"]) - 1.0
            )
            if factor_error > factor_relative_tolerance:
                continue
            candidate = (
                current[0] + 1,
                current[1] - abs(distance),
                current[2] - factor_error,
                (*current[3], (left_index, right_index)),
            )
            next_state = (left_index + 1, right_index + 1)
            existing = matches_by_state.get(next_state)
            if existing is None or candidate[:3] > existing[:3]:
                matches_by_state[next_state] = candidate

    matches = list(matches_by_state[(len(left), len(right))][3])
    matched_left = {left_index for left_index, _ in matches}
    matched_right = {right_index for _, right_index in matches}
    exact_matches = 0
    shifted_matches = 0
    factor_differences = []
    for left_index, right_index in matches:
        if left[left_index]["ex_date"] == right[right_index]["ex_date"]:
            exact_matches += 1
        else:
            shifted_matches += 1
        factor_differences.append(abs(
            float(left[left_index]["factor"])
            / float(right[right_index]["factor"])
            - 1.0
        ))
    unmatched_left = [
        index for index in range(len(left)) if index not in matched_left
    ]
    unmatched_right = [
        index for index in range(len(right)) if index not in matched_right
    ]
    used_unmatched_right: set[int] = set()
    conflicts = 0
    for left_index in unmatched_left:
        candidates: List[Tuple[int, int]] = []
        for right_index in unmatched_right:
            if right_index in used_unmatched_right:
                continue
            distance = _session_distance(
                left[left_index]["ex_date"],
                right[right_index]["ex_date"],
                market_sessions,
            )
            if distance is None:
                if left[left_index]["ex_date"] != right[right_index]["ex_date"]:
                    continue
                distance = 0
            if abs(distance) <= max_session_shift:
                candidates.append((abs(distance), right_index))
        if candidates:
            _, right_index = min(candidates)
            used_unmatched_right.add(right_index)
            conflicts += 1
            factor_differences.append(abs(
                float(left[left_index]["factor"])
                / float(right[right_index]["factor"])
                - 1.0
            ))

    left_only = len(left) - len(matches) - conflicts
    right_only = len(right) - len(matches) - conflicts
    left_cumulative = 1.0
    right_cumulative = 1.0
    cumulative_errors: List[float] = []
    for left_index, right_index in sorted(matches):
        left_cumulative *= float(left[left_index]["factor"])
        right_cumulative *= float(right[right_index]["factor"])
        cumulative_errors.append(
            abs(left_cumulative / right_cumulative - 1.0)
        )
    max_cumulative_error = max(cumulative_errors, default=0.0)
    agrees = (
        conflicts == 0
        and left_only == 0
        and right_only == 0
        and max_cumulative_error <= cumulative_relative_tolerance
    )
    return {
        "agrees": agrees,
        "event_matches": len(matches),
        "exact_matches": exact_matches,
        "shifted_matches": shifted_matches,
        "event_conflicts": conflicts,
        "left_only": left_only,
        "right_only": right_only,
        "factor_difference_buckets": _factor_difference_buckets(
            factor_differences
        ),
        "max_cumulative_relative_error": max_cumulative_error,
    }


def build_three_source_canonical_candidate(
    *,
    cninfo_rows: Iterable[Mapping[str, Any]],
    tdx_rows: Iterable[Mapping[str, Any]],
    baostock_sina_rows: Iterable[Mapping[str, Any]],
    target_instruments: Sequence[str],
    series_version: str,
    start_date: date,
    end_date: date,
    complete_instruments_by_source: Mapping[str, Sequence[str]],
    zero_event_complete_instruments_by_source: Optional[
        Mapping[str, Sequence[str]]
    ] = None,
    lineage_by_instrument: Optional[
        Mapping[str, Mapping[str, Any]]
    ] = None,
    lifecycle_bounds_by_instrument: Optional[
        Mapping[str, Mapping[str, Any]]
    ] = None,
    special_event_dates_by_instrument: Optional[
        Mapping[str, Sequence[Any]]
    ] = None,
    sessions_by_exchange: Optional[Mapping[str, Sequence[Any]]] = None,
    reviewed_source_overrides: Optional[
        Mapping[str, ReviewedFactorSourceOverride]
    ] = None,
    factor_relative_tolerance: float = 0.001,
    cumulative_relative_tolerance: float = 0.001,
    max_session_shift: int = 3,
    sample_limit: int = 20,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Select one source-complete path per continuity segment."""

    source_results = {
        "cninfo": _source_rows(cninfo_rows, default_source="cninfo"),
        "tdx": _source_rows(tdx_rows, default_source="tdx"),
        BAOSTOCK_SINA_SOURCE: _source_rows(
            baostock_sina_rows,
            default_source=BAOSTOCK_SINA_SOURCE,
        ),
    }
    paths = {
        source: result[0] for source, result in source_results.items()
    }
    invalid_path_instruments = {
        source: result[1] for source, result in source_results.items()
    }
    completed = {
        source: {
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        }
        - invalid_path_instruments.get(source, set())
        for source, instrument_ids
        in complete_instruments_by_source.items()
    }
    zero_event_completed = {
        source: {
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        }
        for source, instrument_ids
        in (zero_event_complete_instruments_by_source or {}).items()
    }
    special_dates = {
        instrument_id: {
            parsed for value in values if (parsed := _date(value)) is not None
        }
        for instrument_id, values
        in (special_event_dates_by_instrument or {}).items()
    }
    sessions = {
        exchange: sorted({
            parsed for value in values if (parsed := _date(value)) is not None
        })
        for exchange, values in (sessions_by_exchange or {}).items()
    }
    candidate_rows: List[Dict[str, Any]] = []
    decisions: List[Dict[str, Any]] = []
    selection_counts: Dict[str, int] = defaultdict(int)
    confidence_counts: Dict[str, int] = defaultdict(int)
    agreement_counts: Dict[str, int] = defaultdict(int)
    blocked = 0
    low_confidence = 0
    historical_single_source = 0

    for instrument_id in sorted(set(target_instruments)):
        lifecycle = (
            lifecycle_bounds_by_instrument or {}
        ).get(instrument_id) or {}
        lifecycle_start = max(
            start_date,
            _date(lifecycle.get("start_date"))
            or _date(lifecycle.get("listed_date"))
            or start_date,
        )
        lifecycle_end = min(
            end_date,
            _date(lifecycle.get("end_date"))
            or _date(lifecycle.get("delisted_date"))
            or end_date,
        )
        if lifecycle_end < lifecycle_start:
            continue
        lifecycle_ended = bool(
            lifecycle.get("lifecycle_ended")
            or (
                _date(lifecycle.get("delisted_date")) is not None
                and _date(lifecycle.get("delisted_date")) <= end_date
            )
        )
        for segment in build_continuity_segments(
            instrument_id=instrument_id,
            start_date=lifecycle_start,
            end_date=lifecycle_end,
            lineage=(lineage_by_instrument or {}).get(instrument_id),
        ):
            source_segment_rows = {
                source: [
                    row for row in paths[source].get(instrument_id, [])
                    if row["ex_date"] <= segment["end_date"]
                    and (
                        row["ex_date"] > segment["start_date"]
                        if segment["reset_at_start"]
                        else row["ex_date"] >= segment["start_date"]
                    )
                ]
                for source in SOURCE_ORDER
            }
            excluded_boundary_event_counts = {
                source: sum(
                    1
                    for row in paths[source].get(instrument_id, [])
                    if segment["reset_at_start"]
                    and row["ex_date"] == segment["start_date"]
                )
                for source in SOURCE_ORDER
            }
            eligible = {
                source: (
                    instrument_id in completed.get(source, set())
                    and (
                        bool(source_segment_rows[source])
                        or instrument_id
                        in zero_event_completed.get(source, set())
                    )
                )
                for source in SOURCE_ORDER
            }
            invalid_sources = sorted(
                source
                for source in SOURCE_ORDER
                if instrument_id
                in invalid_path_instruments.get(source, set())
            )
            pairwise: Dict[str, Dict[str, Any]] = {}
            for left, right in (
                ("cninfo", "tdx"),
                ("cninfo", BAOSTOCK_SINA_SOURCE),
                ("tdx", BAOSTOCK_SINA_SOURCE),
            ):
                key = f"{left}__{right}"
                if eligible[left] and eligible[right]:
                    pairwise[key] = compare_segment_paths(
                        source_segment_rows[left],
                        source_segment_rows[right],
                        market_sessions=sessions.get(
                            _exchange(instrument_id) or "", ()
                        ),
                        factor_relative_tolerance=(
                            factor_relative_tolerance
                        ),
                        cumulative_relative_tolerance=(
                            cumulative_relative_tolerance
                        ),
                        max_session_shift=max_session_shift,
                    )
                else:
                    pairwise[key] = {
                        "agrees": False,
                        "reason": "source_incomplete",
                    }
            agreeing_pairs = sorted(
                key
                for key, comparison in pairwise.items()
                if comparison.get("agrees")
            )
            for key in agreeing_pairs:
                agreement_counts[key] += 1
            is_special = any(
                segment["start_date"] <= special <= segment["end_date"]
                for special in special_dates.get(instrument_id, set())
            )
            selected_source: Optional[str] = None
            confidence = "blocked"
            reason = "cninfo_incomplete"
            override_evidence: Optional[Dict[str, Any]] = None
            historical_segment_ended = lifecycle_ended
            cninfo_empty_contradicted = bool(
                eligible["cninfo"]
                and not source_segment_rows["cninfo"]
                and eligible["tdx"]
                and source_segment_rows["tdx"]
            )
            use_historical_tdx_fallback = bool(
                not is_special
                and historical_segment_ended
                and not source_segment_rows["cninfo"]
                and eligible["tdx"]
                and source_segment_rows["tdx"]
                and not pairwise[
                    f"tdx__{BAOSTOCK_SINA_SOURCE}"
                ].get("agrees")
            )
            reviewed_override = (
                reviewed_source_overrides or {}
            ).get(instrument_id)
            if reviewed_override is not None:
                override_source = reviewed_override.selected_source
                override_evidence = reviewed_override.as_selection_evidence()
                if (
                    eligible.get(override_source, False)
                    and paths.get(override_source, {}).get(instrument_id)
                ):
                    selected_source = override_source
                    confidence = "reviewed_source_override"
                    reason = reviewed_override.reason
                else:
                    reason = "reviewed_source_override_ineligible"
                    blocked += 1
            elif use_historical_tdx_fallback:
                selected_source = "tdx"
                confidence = "historical_single_source"
                reason = (
                    "tdx_historical_with_baostock_sina_conflict_fallback"
                    if eligible[BAOSTOCK_SINA_SOURCE]
                    else "tdx_historical_single_source_fallback"
                )
                historical_single_source += 1
                low_confidence += 1
            elif eligible["cninfo"] and not cninfo_empty_contradicted:
                if is_special:
                    selected_source = "cninfo"
                    confidence = "governed_special"
                    reason = "governed_special_action_cninfo_policy"
                elif (
                    pairwise["cninfo__tdx"].get("agrees")
                    and pairwise[
                        f"cninfo__{BAOSTOCK_SINA_SOURCE}"
                    ].get("agrees")
                ):
                    selected_source = "cninfo"
                    confidence = "high"
                    reason = "three_source_consensus"
                elif pairwise["cninfo__tdx"].get("agrees"):
                    selected_source = "cninfo"
                    confidence = "high"
                    reason = "cninfo_tdx_consensus"
                elif pairwise[
                    f"cninfo__{BAOSTOCK_SINA_SOURCE}"
                ].get("agrees"):
                    selected_source = "cninfo"
                    confidence = "high"
                    reason = "cninfo_baostock_sina_consensus"
                elif pairwise[
                    f"tdx__{BAOSTOCK_SINA_SOURCE}"
                ].get("agrees"):
                    selected_source = "tdx"
                    confidence = "independent_consensus"
                    reason = "tdx_baostock_sina_consensus_over_cninfo"
                else:
                    selected_source = "cninfo"
                    confidence = "low"
                    reason = "no_eligible_consensus_cninfo_fallback"
                    low_confidence += 1
            elif (
                not is_special
                and pairwise[
                    f"tdx__{BAOSTOCK_SINA_SOURCE}"
                ].get("agrees")
            ):
                selected_source = "tdx"
                confidence = "independent_consensus"
                reason = (
                    "tdx_baostock_sina_consensus_without_complete_cninfo"
                )
            else:
                blocked += 1

            decision = {
                **segment,
                "selected_source": selected_source,
                "confidence": confidence,
                "reason": reason,
                "reviewed_source_override": override_evidence,
                "special_action": is_special,
                "cninfo_empty_contradicted": cninfo_empty_contradicted,
                "historical_segment_ended": historical_segment_ended,
                "lifecycle_start": lifecycle_start,
                "lifecycle_end": lifecycle_end,
                "eligible_sources": sorted(
                    source for source, value in eligible.items() if value
                ),
                "invalid_sources": invalid_sources,
                "agreeing_pairs": agreeing_pairs,
                "pairwise": pairwise,
                "source_event_counts": {
                    source: len(rows)
                    for source, rows in source_segment_rows.items()
                },
                "excluded_boundary_event_counts": {
                    source: count
                    for source, count
                    in excluded_boundary_event_counts.items()
                    if count
                },
            }
            decisions.append(decision)
            confidence_counts[confidence] += 1
            if selected_source is None:
                continue
            selection_counts[selected_source] += 1
            cumulative = 1.0
            selected_rows = source_segment_rows[selected_source]
            evidence_count = len({
                source
                for source in SOURCE_ORDER
                if eligible[source]
            })
            for row in selected_rows:
                cumulative *= float(row["factor"])
                candidate_rows.append({
                    "instrument_id": instrument_id,
                    "ex_date": datetime.combine(
                        row["ex_date"], datetime.min.time()
                    ),
                    "series_version": series_version,
                    "factor": float(row["factor"]),
                    "cumulative_factor": cumulative,
                    "selected_source": selected_source,
                    "source_profile": str(
                        row.get("source_profile")
                        or f"{selected_source}_event_path"
                    ),
                    "quality_status": confidence,
                    "evidence_count": evidence_count,
                    "segment_id": segment["segment_id"],
                    "segment_start": segment["start_date"],
                    "segment_end": segment["end_date"],
                })

    conflict_samples = [
        decision
        for decision in decisions
        if decision["confidence"] in {
            "low", "historical_single_source"
        }
    ][:max(0, int(sample_limit))]
    blocked_decisions = [
        decision
        for decision in decisions
        if decision["confidence"] == "blocked"
    ][:max(0, int(sample_limit))]
    reviewed_source_override_samples = [
        decision
        for decision in decisions
        if decision["confidence"] == "reviewed_source_override"
    ][:max(0, int(sample_limit))]
    pairwise_reconciliation: Dict[str, Dict[str, Any]] = {}
    for pair_name in (
        "cninfo__tdx",
        f"cninfo__{BAOSTOCK_SINA_SOURCE}",
        f"tdx__{BAOSTOCK_SINA_SOURCE}",
    ):
        comparisons = [
            decision["pairwise"][pair_name]
            for decision in decisions
            if decision["pairwise"][pair_name].get("reason")
            != "source_incomplete"
        ]
        bucket_counts = {
            label: sum(
                int(
                    comparison.get(
                        "factor_difference_buckets", {}
                    ).get(label, 0)
                )
                for comparison in comparisons
            )
            for label, _ in FACTOR_DIFFERENCE_BUCKETS
        }
        bucket_counts["gt_1_pct"] = sum(
            int(
                comparison.get(
                    "factor_difference_buckets", {}
                ).get("gt_1_pct", 0)
            )
            for comparison in comparisons
        )
        pairwise_reconciliation[pair_name] = {
            "compared_segments": len(comparisons),
            "exact_matches": sum(
                int(item.get("exact_matches", 0))
                for item in comparisons
            ),
            "shifted_matches": sum(
                int(item.get("shifted_matches", 0))
                for item in comparisons
            ),
            "conflicts": sum(
                int(item.get("event_conflicts", 0))
                for item in comparisons
            ),
            "left_only": sum(
                int(item.get("left_only", 0))
                for item in comparisons
            ),
            "right_only": sum(
                int(item.get("right_only", 0))
                for item in comparisons
            ),
            "factor_difference_buckets": bucket_counts,
        }
    summary = {
        "series_version": series_version,
        "source_selection_status": (
            "blocked" if blocked else "selected"
        ),
        "candidate_built": True,
        "row_count": len(candidate_rows),
        "instrument_count": len(set(target_instruments)),
        "segment_count": len(decisions),
        "blocked_segment_count": blocked,
        "low_confidence_segment_count": low_confidence,
        "historical_single_source_segment_count": (
            historical_single_source
        ),
        "selection_counts": dict(selection_counts),
        "confidence_counts": dict(confidence_counts),
        "agreement_counts": dict(agreement_counts),
        "pairwise_reconciliation": pairwise_reconciliation,
        "invalid_path_instruments_by_source": {
            source: sorted(instrument_ids)
            for source, instrument_ids
            in invalid_path_instruments.items()
            if instrument_ids
        },
        "decisions": decisions,
        "blocked_decisions": blocked_decisions,
        "reviewed_source_override_samples": (
            reviewed_source_override_samples
        ),
        "conflict_samples": conflict_samples,
        "promotion_eligible": blocked == 0,
    }
    return candidate_rows, summary
