"""Bounded operational helpers for A-share canonical factor series."""

from __future__ import annotations

from collections import Counter, defaultdict
import math
from typing import Any, Dict, Iterable, Mapping, Sequence


UNBOUNDED_REPORT_FIELDS = frozenset({"decisions", "blocked_decisions"})


def extract_canonical_report_decisions(
    report: Mapping[str, Any],
) -> list[Dict[str, Any]]:
    """Return legacy decisions from either supported report location."""

    collections = [report.get("decisions")]
    candidate = report.get("candidate")
    if isinstance(candidate, Mapping):
        collections.append(candidate.get("decisions"))

    decisions: list[Dict[str, Any]] = []
    prior_identities: Dict[tuple[str, str], Dict[str, Any]] = {}
    for collection in collections:
        current: list[Dict[str, Any]] = []
        for value in collection or ():
            if not isinstance(value, Mapping):
                raise ValueError("canonical decision payload must be an object")
            decision = dict(value)
            identity = (
                str(decision.get("instrument_id") or "").strip(),
                str(decision.get("segment_id") or "").strip(),
            )
            existing = prior_identities.get(identity) if all(identity) else None
            if existing is not None:
                if existing != decision:
                    raise ValueError(
                        "conflicting canonical decision payload for "
                        f"identity={identity}"
                    )
                continue
            decisions.append(decision)
            current.append(decision)
        for decision in current:
            identity = (
                str(decision.get("instrument_id") or "").strip(),
                str(decision.get("segment_id") or "").strip(),
            )
            if all(identity):
                prior_identities.setdefault(identity, decision)
    return decisions


def _bound_report_collections(
    value: Any,
    *,
    sample_limit: int,
) -> Any:
    """Recursively cap audit collections while retaining their original counts."""

    if isinstance(value, Mapping):
        bounded: Dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key)
            if isinstance(item, list) and len(item) > sample_limit:
                bounded[normalized_key] = [
                    _bound_report_collections(
                        entry,
                        sample_limit=sample_limit,
                    )
                    for entry in item[:sample_limit]
                ]
                bounded[f"{normalized_key}_total_count"] = len(item)
            else:
                bounded[normalized_key] = _bound_report_collections(
                    item,
                    sample_limit=sample_limit,
                )
        return bounded
    if isinstance(value, list):
        return [
            _bound_report_collections(item, sample_limit=sample_limit)
            for item in value[:sample_limit]
        ]
    return value


def compact_canonical_report(
    report: Mapping[str, Any],
    *,
    sample_limit: int = 50,
) -> Dict[str, Any]:
    """Return a report that never embeds the full decision collection."""

    decisions = extract_canonical_report_decisions(report)
    limit = max(0, int(sample_limit))
    bounded_report = {
        key: value
        for key, value in report.items()
        if key not in UNBOUNDED_REPORT_FIELDS
    }
    candidate = bounded_report.get("candidate")
    if isinstance(candidate, Mapping):
        bounded_report["candidate"] = {
            key: value
            for key, value in candidate.items()
            if key not in UNBOUNDED_REPORT_FIELDS
        }
    compact = _bound_report_collections(
        bounded_report,
        sample_limit=limit,
    )
    compact["decision_count"] = int(
        len(decisions)
        if decisions
        else report.get("decision_count") or 0
    )
    compact["decision_storage"] = "adjustment_factor_decisions"
    compact["report_format"] = "canonical_summary_v2"
    if decisions:
        blocked_samples = [
            item for item in decisions
            if str(item.get("confidence") or "").strip() == "blocked"
        ]
    else:
        existing_samples = report.get("blocked_decision_samples")
        blocked_samples = (
            list(existing_samples)
            if isinstance(existing_samples, (list, tuple))
            else []
        )
    compact["blocked_decision_samples"] = blocked_samples[:limit]
    return compact


def summarize_canonical_decisions(
    decisions: Iterable[Mapping[str, Any]],
    *,
    instrument_statuses: Sequence[Mapping[str, Any]] = (),
    sample_limit: int = 50,
) -> Dict[str, Any]:
    """Derive stable aggregate metrics from complete decision and status sets."""

    decision_rows = [dict(item) for item in decisions]
    selection_counts: Counter[str] = Counter()
    confidence_counts: Counter[str] = Counter()
    agreement_counts: Counter[str] = Counter()
    pairwise: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "compared_segments": 0,
            "exact_matches": 0,
            "shifted_matches": 0,
            "conflicts": 0,
            "left_only": 0,
            "right_only": 0,
            "factor_difference_buckets": Counter(),
        }
    )
    for decision in decision_rows:
        source = str(decision.get("selected_source") or "").strip()
        confidence = str(decision.get("confidence") or "unknown").strip()
        if source:
            selection_counts[source] += 1
        confidence_counts[confidence] += 1
        for pair in decision.get("agreeing_pairs") or []:
            agreement_counts[str(pair)] += 1
        for pair_name, comparison in (decision.get("pairwise") or {}).items():
            if comparison.get("reason") == "source_incomplete":
                continue
            target = pairwise[str(pair_name)]
            target["compared_segments"] += 1
            for source_key, target_key in (
                ("exact_matches", "exact_matches"),
                ("shifted_matches", "shifted_matches"),
                ("event_conflicts", "conflicts"),
                ("left_only", "left_only"),
                ("right_only", "right_only"),
            ):
                target[target_key] += int(comparison.get(source_key) or 0)
            target["factor_difference_buckets"].update(
                comparison.get("factor_difference_buckets") or {}
            )

    status_rows = [dict(item) for item in instrument_statuses]
    complete_statuses = {"complete_with_events", "complete_no_events"}
    complete_count = sum(
        str(item.get("coverage_status") or "") in complete_statuses
        for item in status_rows
    )
    instrument_count = len(status_rows)
    coverage_ratio = (
        complete_count / instrument_count if instrument_count else 0.0
    )
    blocked = confidence_counts.get("blocked", 0)
    low = confidence_counts.get("low", 0)
    historical = confidence_counts.get("historical_single_source", 0)
    conflict_samples = [
        item for item in decision_rows
        if str(item.get("confidence") or "") in {
            "blocked", "low", "historical_single_source"
        }
    ][:max(0, int(sample_limit))]
    return {
        "segment_count": len(decision_rows),
        "decision_count": len(decision_rows),
        "selection_counts": dict(sorted(selection_counts.items())),
        "confidence_counts": dict(sorted(confidence_counts.items())),
        "agreement_counts": dict(sorted(agreement_counts.items())),
        "pairwise_reconciliation": {
            key: {
                **value,
                "factor_difference_buckets": dict(
                    sorted(value["factor_difference_buckets"].items())
                ),
            }
            for key, value in sorted(pairwise.items())
        },
        "blocked_segment_count": blocked,
        "low_confidence_segment_count": low,
        "historical_single_source_segment_count": historical,
        "conflict_count": blocked,
        "conflict_samples": conflict_samples,
        "coverage_ratio": coverage_ratio,
        "overall_completeness": {
            "status": (
                "success"
                if instrument_count and complete_count == instrument_count
                else "partial"
            ),
            "instrument_count": instrument_count,
            "complete_instrument_count": complete_count,
            "incomplete_instrument_count": instrument_count - complete_count,
        },
    }


def qualify_composite_paths(
    normalized_rows: Iterable[Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Describe factor-path integrity without asserting XDXR completeness."""

    grouped: Dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in normalized_rows:
        instrument_id = str(row.get("instrument_id") or "").strip()
        if instrument_id:
            grouped[instrument_id].append(row)
    result: Dict[str, Dict[str, Any]] = {}
    for instrument_id, rows in grouped.items():
        invalid = []
        for row in rows:
            try:
                factor = float(row.get("normalized_factor"))
                cumulative = float(row.get("cumulative_factor"))
            except (TypeError, ValueError):
                invalid.append(row)
                continue
            if (
                not math.isfinite(factor)
                or factor <= 0
                or not math.isfinite(cumulative)
                or cumulative <= 0
                or str(
                    row.get("composite_normalization_method") or ""
                ).startswith("invalid_")
            ):
                invalid.append(row)
        eligible = bool(rows) and not invalid
        ordered = sorted(rows, key=lambda row: str(row.get("ex_date") or ""))
        result[instrument_id] = {
            "path_eligible": eligible,
            "event_completeness": "not_asserted",
            "row_count": len(rows),
            "invalid_row_count": len(invalid),
            "first_factor_date": (
                str(ordered[0].get("ex_date")) if ordered else None
            ),
            "last_factor_date": (
                str(ordered[-1].get("ex_date")) if ordered else None
            ),
            "upstream_sources": sorted({
                str(row.get("upstream_source") or "unknown") for row in rows
            }),
            "normalization_methods": sorted({
                str(row.get("composite_normalization_method") or "unknown")
                for row in rows
            }),
        }
    return result
