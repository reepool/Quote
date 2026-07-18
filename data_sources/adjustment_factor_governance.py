"""Normalization and quality helpers for multi-source adjustment factors."""

from __future__ import annotations

import math
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime
from itertools import combinations
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


NORMALIZATION_VERSION = "event_ratio_v1"
DEFAULT_SERIES_VERSION = "a_share_event_product_v1"


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


def normalize_source_path(
    rows: Iterable[Mapping[str, Any]],
    *,
    source_profile: str = "default",
    normalization_version: str = NORMALIZATION_VERSION,
    ratio_tolerance_pct: float = 0.1,
) -> List[Dict[str, Any]]:
    """Normalize provider rows while retaining provider cumulative values."""
    grouped: Dict[Tuple[str, str], Dict[date, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        source = str(row.get("source") or "unknown").strip().lower()
        ex_date = _date(row.get("ex_date"))
        if instrument_id and ex_date:
            grouped[(instrument_id, source)][ex_date] = row

    normalized: List[Dict[str, Any]] = []
    for (instrument_id, source), dated_rows in sorted(grouped.items()):
        previous_cumulative: Optional[float] = None
        for ex_date, row in sorted(dated_rows.items()):
            provider_factor = _positive(row.get("factor"))
            provider_cumulative = _positive(row.get("cumulative_factor"))
            ratio = None
            if previous_cumulative and provider_cumulative:
                ratio = provider_cumulative / previous_cumulative
            normalized_factor = ratio or provider_factor or provider_cumulative
            quality_status = "valid"
            ratio_diff_pct = None
            if normalized_factor is None:
                quality_status = "invalid"
            elif ratio and provider_factor:
                ratio_diff_pct = abs(ratio / provider_factor - 1.0) * 100.0
                if ratio_diff_pct > ratio_tolerance_pct:
                    quality_status = "provider_factor_conflict"

            normalized.append({
                "instrument_id": instrument_id,
                "ex_date": datetime.combine(ex_date, datetime.min.time()),
                "source": source,
                "source_profile": str(row.get("source_profile") or source_profile),
                "provider_factor": provider_factor,
                "provider_cumulative_factor": provider_cumulative,
                "normalized_factor": normalized_factor,
                "normalization_version": normalization_version,
                "quality_status": quality_status,
                "ratio_diff_pct": ratio_diff_pct,
                "raw_payload": dict(row),
            })
            if provider_cumulative:
                previous_cumulative = provider_cumulative
    return normalized


def build_canonical_series(
    observations: Iterable[Mapping[str, Any]],
    *,
    series_version: str = DEFAULT_SERIES_VERSION,
    source_priority: Sequence[str] = ("akshare", "tdx_xdxr", "baostock"),
    target_instruments: Optional[Sequence[str]] = None,
    completed_sources: Optional[Mapping[str, Sequence[str]]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build one source-complete unit-baseline path per instrument."""
    by_instrument_source: Dict[Tuple[str, str], List[Mapping[str, Any]]] = defaultdict(list)
    for row in observations:
        instrument_id = str(row.get("instrument_id") or "").strip()
        source = str(row.get("source") or "unknown").strip().lower()
        if instrument_id and _date(row.get("ex_date")):
            by_instrument_source[(instrument_id, source)].append(row)

    instruments = sorted(
        set(target_instruments or []) or {key[0] for key in by_instrument_source}
    )
    completed_by_source = {
        str(source).lower(): set(instrument_ids)
        for source, instrument_ids in (completed_sources or {}).items()
    }
    canonical: List[Dict[str, Any]] = []
    source_counts: Dict[str, int] = defaultdict(int)
    conflicts = 0
    samples: List[Dict[str, Any]] = []

    for instrument_id in instruments:
        selected_source = None
        for source in source_priority:
            source = str(source).lower()
            if completed_by_source and instrument_id not in completed_by_source.get(source, set()):
                continue
            if by_instrument_source.get((instrument_id, source)):
                selected_source = source
                break
        if selected_source is None:
            continue
        source_rows = sorted(
            by_instrument_source[(instrument_id, selected_source)],
            key=lambda item: _date(item.get("ex_date")) or date.min,
        )
        cumulative = 1.0
        built_count = 0
        for row in source_rows:
            factor = _positive(row.get("normalized_factor"))
            if factor is None:
                conflicts += 1
                if len(samples) < 20:
                    samples.append({
                        "instrument_id": instrument_id,
                        "ex_date": str(row.get("ex_date"))[:10],
                        "source": selected_source,
                        "reason": "invalid_normalized_factor",
                    })
                continue
            if str(row.get("quality_status") or "") not in {"valid", "all_pass"}:
                conflicts += 1
                if len(samples) < 20:
                    samples.append({
                        "instrument_id": instrument_id,
                        "ex_date": str(row.get("ex_date"))[:10],
                        "source": selected_source,
                        "reason": str(row.get("quality_status") or "unvalidated"),
                    })
            cumulative *= factor
            canonical.append({
                "instrument_id": instrument_id,
                "ex_date": datetime.combine(_date(row.get("ex_date")) or date.min, datetime.min.time()),
                "series_version": series_version,
                "factor": factor,
                "cumulative_factor": cumulative,
                "selected_source": selected_source,
                "source_profile": str(row.get("source_profile") or "default"),
                "quality_status": str(row.get("quality_status") or "unvalidated"),
                "evidence_count": 1,
            })
            built_count += 1
        if built_count:
            source_counts[selected_source] += 1

    summary = {
        "series_version": series_version,
        "instrument_count": len(instruments),
        "built_instruments": sum(source_counts.values()),
        "covered_instruments": sum(
            1
            for instrument_id in instruments
            if not completed_by_source
            or any(
                instrument_id in completed_by_source.get(str(source).lower(), set())
                for source in source_priority
            )
        ),
        "row_count": len(canonical),
        "conflict_count": conflicts,
        "source_counts": dict(source_counts),
        "samples": samples,
        "promotion_eligible": bool(canonical) and conflicts == 0,
    }
    return canonical, summary


def rebase_legacy_tail(
    rows: Iterable[Mapping[str, Any]],
    *,
    latest_date: Optional[Any],
    latest_cumulative_factor: float = 1.0,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Rebase new event ratios onto one existing legacy cumulative tail."""
    stats = {"rebased": 0, "historical_skipped": 0, "invalid": 0}
    prepared: List[Dict[str, Any]] = []
    parsed_latest_date = _date(latest_date)
    cumulative = _positive(latest_cumulative_factor) or 1.0
    for item in sorted(rows, key=lambda value: _date(value.get("ex_date")) or date.min):
        ex_date = _date(item.get("ex_date"))
        event_factor = _positive(item.get("normalized_factor", item.get("factor")))
        if ex_date is None or event_factor is None:
            stats["invalid"] += 1
            continue
        if parsed_latest_date is not None and ex_date <= parsed_latest_date:
            stats["historical_skipped"] += 1
            continue
        cumulative *= event_factor
        row = dict(item.get("raw_payload") or item)
        row.update({
            "instrument_id": str(item.get("instrument_id") or row.get("instrument_id") or ""),
            "ex_date": datetime.combine(ex_date, datetime.min.time()),
            "factor": event_factor,
            "cumulative_factor": cumulative,
            "source": item.get("source") or row.get("source"),
        })
        prepared.append(row)
        parsed_latest_date = ex_date
        stats["rebased"] += 1
    return prepared, stats


def compare_normalized_cumulative_paths(
    candidate_rows: Iterable[Mapping[str, Any]],
    reference_rows: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Compare qfq-equivalent multipliers after removing provider scale."""

    def _group(rows: Iterable[Mapping[str, Any]]) -> Dict[str, List[Tuple[date, float]]]:
        grouped: Dict[str, Dict[date, float]] = defaultdict(dict)
        for row in rows:
            instrument_id = str(row.get("instrument_id") or "").strip()
            ex_date = _date(row.get("ex_date"))
            cumulative = _positive(row.get("cumulative_factor"))
            if instrument_id and ex_date and cumulative:
                grouped[instrument_id][ex_date] = cumulative
        return {
            instrument_id: sorted(values.items())
            for instrument_id, values in grouped.items()
        }

    candidate = _group(candidate_rows)
    reference = _group(reference_rows)
    comparisons: List[Dict[str, Any]] = []
    endpoint_mismatches: List[Dict[str, Any]] = []
    comparable_instruments = sorted(set(candidate) & set(reference))
    for instrument_id in comparable_instruments:
        candidate_items = candidate[instrument_id]
        reference_items = reference[instrument_id]
        candidate_dates = [item[0] for item in candidate_items]
        reference_dates = [item[0] for item in reference_items]
        common_latest_date = min(candidate_dates[-1], reference_dates[-1])
        if candidate_dates[-1] != reference_dates[-1]:
            endpoint_mismatches.append({
                "instrument_id": instrument_id,
                "candidate_latest_date": candidate_dates[-1].isoformat(),
                "reference_latest_date": reference_dates[-1].isoformat(),
                "common_latest_date": common_latest_date.isoformat(),
            })
        candidate_anchor_index = bisect_right(
            candidate_dates, common_latest_date
        ) - 1
        reference_anchor_index = bisect_right(
            reference_dates, common_latest_date
        ) - 1
        candidate_latest = (
            candidate_items[candidate_anchor_index][1]
            if candidate_anchor_index >= 0 else 1.0
        )
        reference_latest = (
            reference_items[reference_anchor_index][1]
            if reference_anchor_index >= 0 else 1.0
        )
        comparison_dates = sorted(
            comparison_date
            for comparison_date in set(candidate_dates) | set(reference_dates)
            if comparison_date <= common_latest_date
        )
        for comparison_date in comparison_dates:
            candidate_index = bisect_right(candidate_dates, comparison_date) - 1
            reference_index = bisect_right(reference_dates, comparison_date) - 1
            candidate_cumulative = (
                candidate_items[candidate_index][1] if candidate_index >= 0 else 1.0
            )
            reference_cumulative = (
                reference_items[reference_index][1] if reference_index >= 0 else 1.0
            )
            candidate_qfq = candidate_cumulative / candidate_latest
            reference_qfq = reference_cumulative / reference_latest
            difference_pct = abs(candidate_qfq / reference_qfq - 1.0) * 100.0
            comparisons.append({
                "instrument_id": instrument_id,
                "date": comparison_date.isoformat(),
                "candidate_qfq_multiplier": candidate_qfq,
                "reference_qfq_multiplier": reference_qfq,
                "difference_pct": difference_pct,
            })

    differences = [item["difference_pct"] for item in comparisons]

    def percentile(values: Sequence[float], quantile: float) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        position = (len(ordered) - 1) * quantile
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight

    comparison_count = len(differences)
    return {
        "comparable_instruments": len(comparable_instruments),
        "comparison_points": len(comparisons),
        "max_adjusted_price_error_pct": max(differences, default=None),
        "mean_adjusted_price_error_pct": (
            sum(differences) / len(differences) if differences else None
        ),
        "median_adjusted_price_error_pct": percentile(differences, 0.5),
        "p95_adjusted_price_error_pct": percentile(differences, 0.95),
        "over_0_1_pct": sum(value > 0.1 for value in differences),
        "over_0_5_pct": sum(value > 0.5 for value in differences),
        "over_1_pct": sum(value > 1.0 for value in differences),
        "over_0_1_ratio": (
            sum(value > 0.1 for value in differences) / comparison_count
            if comparison_count else None
        ),
        "over_0_5_ratio": (
            sum(value > 0.5 for value in differences) / comparison_count
            if comparison_count else None
        ),
        "over_1_ratio": (
            sum(value > 1.0 for value in differences) / comparison_count
            if comparison_count else None
        ),
        "endpoint_mismatch_instruments": len(endpoint_mismatches),
        "endpoint_mismatch_samples": endpoint_mismatches[:sample_limit],
        "samples": sorted(
            comparisons,
            key=lambda item: item["difference_pct"],
            reverse=True,
        )[:sample_limit],
    }


def build_factor_source_benchmark(
    baseline_rows: Iterable[Mapping[str, Any]],
    reference_paths: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    target_instruments: Optional[Sequence[str]] = None,
    baseline_covered_instruments: Optional[Sequence[str]] = None,
    reference_covered_instruments: Optional[
        Mapping[str, Sequence[str]]
    ] = None,
    baseline_source_profile: str = "cninfo_event_derived_v1",
    full_market_scope: bool = False,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Build source-neutral coverage and normalized path comparison evidence."""

    baseline = [dict(row) for row in baseline_rows]
    references = {
        str(source): [dict(row) for row in rows]
        for source, rows in reference_paths.items()
    }
    reference_covered = {
        str(source): {
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        }
        for source, instrument_ids in (reference_covered_instruments or {}).items()
    }
    universe = {
        str(instrument_id).strip()
        for instrument_id in (target_instruments or [])
        if str(instrument_id).strip()
    }

    def instrument_ids(rows: Iterable[Mapping[str, Any]]) -> set[str]:
        return {
            str(row.get("instrument_id") or "").strip()
            for row in rows
            if str(row.get("instrument_id") or "").strip()
            and _date(row.get("ex_date")) is not None
            and _positive(row.get("cumulative_factor")) is not None
        }

    baseline_instruments = instrument_ids(baseline)
    baseline_covered = (
        set(baseline_instruments)
        if baseline_covered_instruments is None
        else {
            str(instrument_id).strip()
            for instrument_id in baseline_covered_instruments
            if str(instrument_id).strip()
        }
    )
    if not universe:
        universe = set(baseline_instruments)
        for rows in references.values():
            universe.update(instrument_ids(rows))
    universe_count = len(universe)
    source_reports: Dict[str, Dict[str, Any]] = {}
    for source, rows in sorted(references.items()):
        path_instruments = instrument_ids(rows)
        covered = path_instruments | reference_covered.get(source, set())
        if universe:
            covered &= universe
        comparison = compare_normalized_cumulative_paths(
            baseline,
            rows,
            sample_limit=sample_limit,
        )
        comparable = int(comparison.get("comparable_instruments") or 0)
        source_reports[source] = {
            "available_instruments": len(covered),
            "coverage_ratio": (
                len(covered) / universe_count if universe_count else 0.0
            ),
            "path_instruments": len(path_instruments),
            "comparable_instruments": comparable,
            "comparable_ratio": (
                comparable / universe_count if universe_count else 0.0
            ),
            **comparison,
        }

    all_paths = {baseline_source_profile: baseline, **references}
    pairwise_comparisons: Dict[str, Dict[str, Any]] = {}
    for left_source, right_source in combinations(sorted(all_paths), 2):
        left_rows = all_paths[left_source]
        right_rows = all_paths[right_source]
        comparison = compare_normalized_cumulative_paths(
            left_rows,
            right_rows,
            sample_limit=sample_limit,
        )
        pairwise_comparisons[f"{left_source}__vs__{right_source}"] = {
            "left_source": left_source,
            "right_source": right_source,
            **comparison,
        }

    return {
        "status": (
            "empty" if not baseline_covered
            else "complete"
            if universe and baseline_covered == universe
            else "partial"
        ),
        "source_selection_status": "deferred",
        "selected_primary_source": None,
        "baseline_source_profile": baseline_source_profile,
        "full_market_scope": bool(full_market_scope),
        "universe_instruments": universe_count,
        "baseline_instruments": len(baseline_instruments),
        "baseline_covered_instruments": len(baseline_covered),
        "baseline_coverage_ratio": (
            len(baseline_covered) / universe_count if universe_count else 0.0
        ),
        "reference_sources": source_reports,
        "pairwise_comparisons": pairwise_comparisons,
    }


def build_event_product_path(
    rows: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Rebuild cumulative paths solely from ordered adjacent event factors."""
    grouped: Dict[str, Dict[date, float]] = defaultdict(dict)
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        ex_date = _date(row.get("ex_date"))
        factor = _positive(row.get("factor"))
        if instrument_id and ex_date and factor:
            grouped[instrument_id][ex_date] = factor

    rebuilt: List[Dict[str, Any]] = []
    for instrument_id, events in sorted(grouped.items()):
        cumulative = 1.0
        for ex_date, factor in sorted(events.items()):
            cumulative *= factor
            rebuilt.append({
                "instrument_id": instrument_id,
                "ex_date": datetime.combine(ex_date, datetime.min.time()),
                "factor": factor,
                "cumulative_factor": cumulative,
            })
    return rebuilt


def reconcile_factor_events(
    candidate_rows: Iterable[Mapping[str, Any]],
    tdx_rows: Iterable[Mapping[str, Any]],
    *,
    sessions_by_exchange: Optional[Mapping[str, Sequence[Any]]] = None,
    factor_tolerance_pct: float = 0.5,
    max_session_distance: int = 3,
    sample_limit: int = 20,
) -> Dict[str, Any]:
    """Reconcile canonical event ratios with TDX exact/shifted event evidence."""

    def _exchange(instrument_id: str) -> Optional[str]:
        normalized = instrument_id.upper()
        if normalized.endswith(".SH"):
            return "SSE"
        if normalized.endswith(".SZ"):
            return "SZSE"
        if normalized.endswith((".BJ", ".BSE")):
            return "BSE"
        return None

    def _events(rows: Iterable[Mapping[str, Any]], factor_key: str) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, Dict[date, Dict[str, Any]]] = defaultdict(dict)
        for row in rows:
            instrument_id = str(row.get("instrument_id") or "").strip()
            ex_date = _date(row.get("ex_date"))
            factor = _positive(row.get(factor_key))
            if instrument_id and ex_date and factor:
                grouped[instrument_id][ex_date] = {
                    "instrument_id": instrument_id,
                    "ex_date": ex_date,
                    "factor": factor,
                }
        return {
            instrument_id: [values[key] for key in sorted(values)]
            for instrument_id, values in grouped.items()
        }

    candidate = _events(candidate_rows, "factor")
    tdx = _events(tdx_rows, "factor")
    sessions = {
        exchange: sorted(parsed for value in values if (parsed := _date(value)))
        for exchange, values in (sessions_by_exchange or {}).items()
    }

    exact_matches: List[Dict[str, Any]] = []
    shifted_matches: List[Dict[str, Any]] = []
    conflicts: List[Dict[str, Any]] = []
    candidate_only: List[Dict[str, Any]] = []
    tdx_only: List[Dict[str, Any]] = []

    def _difference(left: float, right: float) -> float:
        return abs(left / right - 1.0) * 100.0

    def _session_distance(left: date, right: date, market_sessions: Sequence[date]) -> int:
        if left == right:
            return 0
        if right > left:
            return bisect_right(market_sessions, right) - bisect_right(market_sessions, left)
        return -(bisect_right(market_sessions, left) - bisect_right(market_sessions, right))

    for instrument_id in sorted(set(candidate) | set(tdx)):
        candidate_items = candidate.get(instrument_id, [])
        tdx_items = tdx.get(instrument_id, [])
        used_candidate: set[int] = set()
        used_tdx: set[int] = set()

        exact_candidates: List[Tuple[float, int, int]] = []
        for candidate_index, candidate_event in enumerate(candidate_items):
            for tdx_index, tdx_event in enumerate(tdx_items):
                if candidate_event["ex_date"] != tdx_event["ex_date"]:
                    continue
                difference = _difference(candidate_event["factor"], tdx_event["factor"])
                if difference <= factor_tolerance_pct:
                    exact_candidates.append((difference, candidate_index, tdx_index))
        for difference, candidate_index, tdx_index in sorted(exact_candidates):
            if candidate_index in used_candidate or tdx_index in used_tdx:
                continue
            candidate_event = candidate_items[candidate_index]
            tdx_event = tdx_items[tdx_index]
            exact_matches.append({
                "instrument_id": instrument_id,
                "candidate_ex_date": candidate_event["ex_date"].isoformat(),
                "tdx_ex_date": tdx_event["ex_date"].isoformat(),
                "candidate_factor": candidate_event["factor"],
                "tdx_factor": tdx_event["factor"],
                "factor_diff_pct": difference,
                "trading_session_distance": 0,
                "reason": "exact_factor_match",
            })
            used_candidate.add(candidate_index)
            used_tdx.add(tdx_index)

        market_sessions = sessions.get(_exchange(instrument_id) or "", [])
        shifted_candidates: List[Tuple[int, float, int, int, int]] = []
        for candidate_index, candidate_event in enumerate(candidate_items):
            if candidate_index in used_candidate:
                continue
            for tdx_index, tdx_event in enumerate(tdx_items):
                if tdx_index in used_tdx or not market_sessions:
                    continue
                distance = _session_distance(tdx_event["ex_date"], candidate_event["ex_date"], market_sessions)
                if abs(distance) > max_session_distance:
                    continue
                difference = _difference(candidate_event["factor"], tdx_event["factor"])
                if difference > factor_tolerance_pct:
                    continue
                shifted_candidates.append((
                    abs(distance),
                    difference,
                    distance,
                    candidate_index,
                    tdx_index,
                ))
        for _, difference, distance, candidate_index, tdx_index in sorted(shifted_candidates):
            if candidate_index in used_candidate or tdx_index in used_tdx:
                continue
            candidate_event = candidate_items[candidate_index]
            tdx_event = tdx_items[tdx_index]
            shifted_matches.append({
                "instrument_id": instrument_id,
                "candidate_ex_date": candidate_event["ex_date"].isoformat(),
                "tdx_ex_date": tdx_event["ex_date"].isoformat(),
                "candidate_factor": candidate_event["factor"],
                "tdx_factor": tdx_event["factor"],
                "factor_diff_pct": difference,
                "trading_session_distance": distance,
                "reason": "shifted_factor_match",
            })
            used_candidate.add(candidate_index)
            used_tdx.add(tdx_index)

        conflict_candidates: List[Tuple[int, float, int, int, int]] = []
        for candidate_index, candidate_event in enumerate(candidate_items):
            if candidate_index in used_candidate:
                continue
            for tdx_index, tdx_event in enumerate(tdx_items):
                if tdx_index in used_tdx:
                    continue
                if candidate_event["ex_date"] == tdx_event["ex_date"]:
                    distance = 0
                elif market_sessions:
                    distance = _session_distance(
                        tdx_event["ex_date"], candidate_event["ex_date"], market_sessions
                    )
                    if abs(distance) > max_session_distance:
                        continue
                else:
                    continue
                conflict_candidates.append((
                    abs(distance),
                    _difference(candidate_event["factor"], tdx_event["factor"]),
                    distance,
                    candidate_index,
                    tdx_index,
                ))
        for _, difference, distance, candidate_index, tdx_index in sorted(conflict_candidates):
            if candidate_index in used_candidate or tdx_index in used_tdx:
                continue
            candidate_event = candidate_items[candidate_index]
            tdx_event = tdx_items[tdx_index]
            conflicts.append({
                "instrument_id": instrument_id,
                "candidate_ex_date": candidate_event["ex_date"].isoformat(),
                "tdx_ex_date": tdx_event["ex_date"].isoformat(),
                "candidate_factor": candidate_event["factor"],
                "tdx_factor": tdx_event["factor"],
                "factor_diff_pct": difference,
                "trading_session_distance": distance,
                "reason": "exact_date_factor_conflict" if distance == 0 else "nearby_factor_conflict",
            })
            used_candidate.add(candidate_index)
            used_tdx.add(tdx_index)

        candidate_only.extend(
            event for index, event in enumerate(candidate_items) if index not in used_candidate
        )
        tdx_only.extend(event for index, event in enumerate(tdx_items) if index not in used_tdx)

    total_evidence = len(exact_matches) + len(shifted_matches) + len(conflicts) + len(candidate_only) + len(tdx_only)
    discrepancy_count = len(conflicts) + len(candidate_only) + len(tdx_only)
    serialize = lambda item: {
        **item,
        "ex_date": item.get("ex_date").isoformat() if isinstance(item.get("ex_date"), date) else item.get("ex_date"),
    }
    return {
        "candidate_events": sum(len(items) for items in candidate.values()),
        "tdx_events": sum(len(items) for items in tdx.values()),
        "exact_matches": len(exact_matches),
        "shifted_matches": len(shifted_matches),
        "factor_conflicts": len(conflicts),
        "candidate_only": len(candidate_only),
        "tdx_only": len(tdx_only),
        "discrepancy_ratio": discrepancy_count / total_evidence if total_evidence else None,
        "samples": [
            *conflicts[:sample_limit],
            *[serialize(item) | {"reason": "candidate_event_unmatched"} for item in candidate_only[:sample_limit]],
            *[serialize(item) | {"reason": "tdx_event_unmatched"} for item in tdx_only[:sample_limit]],
        ][:sample_limit],
    }


def source_transition_metrics(rows: Iterable[Mapping[str, Any]], *, sample_limit: int = 20) -> Dict[str, Any]:
    """Measure raw cumulative discontinuities at provider transitions."""
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        if instrument_id and _date(row.get("ex_date")):
            grouped[instrument_id].append(row)

    transitions = []
    for instrument_id, items in grouped.items():
        previous = None
        for row in sorted(items, key=lambda item: _date(item.get("ex_date")) or date.min):
            if previous and previous.get("source") != row.get("source"):
                previous_cum = _positive(previous.get("cumulative_factor"))
                cumulative = _positive(row.get("cumulative_factor"))
                factor = _positive(row.get("factor"))
                if previous_cum and cumulative and factor:
                    observed_ratio = cumulative / previous_cum
                    diff_pct = abs(observed_ratio / factor - 1.0) * 100.0
                    transitions.append({
                        "instrument_id": instrument_id,
                        "ex_date": str(row.get("ex_date"))[:10],
                        "from_source": previous.get("source"),
                        "to_source": row.get("source"),
                        "observed_ratio": observed_ratio,
                        "event_factor": factor,
                        "difference_pct": diff_pct,
                    })
            previous = row
    return {
        "transitions": len(transitions),
        "over_0_1_pct": sum(item["difference_pct"] > 0.1 for item in transitions),
        "over_1_pct": sum(item["difference_pct"] > 1.0 for item in transitions),
        "over_5_pct": sum(item["difference_pct"] > 5.0 for item in transitions),
        "max_difference_pct": max((item["difference_pct"] for item in transitions), default=0.0),
        "samples": sorted(transitions, key=lambda item: item["difference_pct"], reverse=True)[:sample_limit],
    }
