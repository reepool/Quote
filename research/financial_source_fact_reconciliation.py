"""Official-versus-fallback source fact reconciliation.

Pure comparison logic for the financial disclosure field-level fill path: the
fallback run compares the bundle facts it fetched for filling against the
official structured facts already available for the same instrument-period,
marks beyond-tolerance discrepancies and single-sourced definitional sanity
anomalies, and performs no network access of its own.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

DEFAULT_COMPARISON_SCOPE: tuple[str, ...] = (
    "revenue",
    "net_income_parent",
    "equity_parent",
    "total_assets",
    "total_liabilities",
)
DEFAULT_RELATIVE_TOLERANCE = 0.005
DEFAULT_ABSOLUTE_FLOOR_CNY = 1.0
DEFAULT_SANITY_MINORITY_SHARE_MAX = 0.5

RECORD_KIND_SOURCE_DIFFERENCE = "source_difference"
RECORD_KIND_SANITY_REVIEW = "sanity_review"


@dataclass(frozen=True)
class SourceFactComparisonRecord:
    """One marked outcome of the same-fetch source comparison."""

    instrument_id: str
    report_period: str
    canonical_fact: str
    record_kind: str
    official_source: Optional[str] = None
    official_value: Optional[float] = None
    fallback_source: Optional[str] = None
    fallback_value: Optional[float] = None
    relative_diff: Optional[float] = None
    detail: Optional[Dict[str, Any]] = None

    def as_storage_row(self) -> Dict[str, Any]:
        payload = asdict(self)
        return {
            "instrument_id": payload["instrument_id"],
            "report_period": payload["report_period"],
            "canonical_fact": payload["canonical_fact"],
            "record_kind": payload["record_kind"],
            "official_source": payload["official_source"],
            "official_value": payload["official_value"],
            "fallback_source": payload["fallback_source"],
            "fallback_value": payload["fallback_value"],
            "relative_diff": payload["relative_diff"],
            "detail_json": payload.get("detail") or {},
        }


def _relative_difference(
    official_value: float,
    fallback_value: float,
) -> float:
    denominator = max(abs(official_value), abs(fallback_value))
    if denominator <= 0:
        return 0.0
    return abs(official_value - fallback_value) / denominator


def values_match_within_tolerance(
    official_value: float,
    fallback_value: float,
    *,
    relative_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
    absolute_floor_cny: float = DEFAULT_ABSOLUTE_FLOOR_CNY,
) -> bool:
    """Return True when both values agree under the configured tolerance."""
    difference = abs(official_value - fallback_value)
    allowed = max(
        float(absolute_floor_cny),
        float(relative_tolerance) * max(abs(official_value), abs(fallback_value)),
    )
    return difference <= allowed


def compare_official_and_fallback_facts(
    *,
    instrument_id: str,
    report_period: str,
    official_facts: Mapping[str, float],
    fallback_facts: Mapping[str, float],
    official_source: str,
    fallback_source: str,
    scope: Sequence[str] = DEFAULT_COMPARISON_SCOPE,
    relative_tolerance: float = DEFAULT_RELATIVE_TOLERANCE,
    absolute_floor_cny: float = DEFAULT_ABSOLUTE_FLOOR_CNY,
) -> List[SourceFactComparisonRecord]:
    """Compare canonical facts present on both sides under the tolerance.

    Facts missing on either side are skipped and never flagged; facts beyond
    tolerance become `source_difference` records carrying both values.
    """
    records: List[SourceFactComparisonRecord] = []
    for canonical_fact in scope:
        official_value = official_facts.get(canonical_fact)
        fallback_value = fallback_facts.get(canonical_fact)
        if official_value is None or fallback_value is None:
            continue
        if values_match_within_tolerance(
            float(official_value),
            float(fallback_value),
            relative_tolerance=relative_tolerance,
            absolute_floor_cny=absolute_floor_cny,
        ):
            continue
        records.append(
            SourceFactComparisonRecord(
                instrument_id=instrument_id,
                report_period=report_period,
                canonical_fact=str(canonical_fact),
                record_kind=RECORD_KIND_SOURCE_DIFFERENCE,
                official_source=official_source,
                official_value=float(official_value),
                fallback_source=fallback_source,
                fallback_value=float(fallback_value),
                relative_diff=_relative_difference(
                    float(official_value),
                    float(fallback_value),
                ),
                detail={
                    "relative_tolerance": float(relative_tolerance),
                    "absolute_floor_cny": float(absolute_floor_cny),
                },
            )
        )
    return records


def evaluate_single_sourced_sanity(
    *,
    instrument_id: str,
    report_period: str,
    filled_fact: str,
    filled_value: float,
    fallback_source: str,
    reference_fact: str,
    reference_value: float,
    reference_source: str,
    minority_share_max: float = DEFAULT_SANITY_MINORITY_SHARE_MAX,
) -> Optional[SourceFactComparisonRecord]:
    """Check a fallback-filled field against its definitional reference fact.

    For an equity-parent fill the implied minority interest
    (`reference_value - filled_value`) must be non-negative and within the
    configured loose share of the reference value. Violations become
    review-only `sanity_review` records and never block the write.
    """
    reference = float(reference_value)
    implied_minority = reference - float(filled_value)
    if implied_minority >= 0 and implied_minority <= (
        float(minority_share_max) * abs(reference)
    ):
        return None
    return SourceFactComparisonRecord(
        instrument_id=instrument_id,
        report_period=report_period,
        canonical_fact=str(filled_fact),
        record_kind=RECORD_KIND_SANITY_REVIEW,
        official_source=reference_source,
        official_value=reference,
        fallback_source=fallback_source,
        fallback_value=float(filled_value),
        relative_diff=(
            abs(implied_minority) / abs(reference) if reference else None
        ),
        detail={
            "check": "implied_minority_interest_share",
            "implied_minority_interest": implied_minority,
            "minority_share_max": float(minority_share_max),
            "reference_fact": str(reference_fact),
        },
    )


def summarize_comparison_records(
    records: Sequence[SourceFactComparisonRecord],
    *,
    report_top_n: int = 5,
) -> Dict[str, Any]:
    """Aggregate records into report-ready counts and bounded samples."""
    differences = [
        record
        for record in records
        if record.record_kind == RECORD_KIND_SOURCE_DIFFERENCE
    ]
    sanity = [
        record
        for record in records
        if record.record_kind == RECORD_KIND_SANITY_REVIEW
    ]

    def render(record: SourceFactComparisonRecord) -> Dict[str, Any]:
        return {
            "instrument_id": record.instrument_id,
            "report_period": record.report_period,
            "canonical_fact": record.canonical_fact,
            "official_source": record.official_source,
            "official_value": record.official_value,
            "fallback_source": record.fallback_source,
            "fallback_value": record.fallback_value,
            "relative_diff": record.relative_diff,
            "detail": record.detail,
        }

    return {
        "compared_records": len(records),
        "source_differences": len(differences),
        "sanity_reviews": len(sanity),
        "samples": [render(record) for record in differences[:report_top_n]],
        "sanity_samples": [render(record) for record in sanity[:report_top_n]],
    }


_AGGREGATE_COUNT_KEYS: tuple[str, ...] = (
    "compared_facts",
    "consistent_facts",
    "source_differences",
    "sanity_reviews",
    "merged_instruments",
    "merged_official_fields",
    "filled_fallback_fields",
    "cleaned_fallback_facts",
    "targets_with_official_rows",
)


def merge_reconciliation_summaries(
    summaries: Sequence[Optional[Mapping[str, Any]]],
    *,
    report_top_n: int = 5,
) -> Optional[Dict[str, Any]]:
    """Merge per-scope reconciliation summaries into one report summary.

    One aggregation contract for every level that combines summaries —
    exchanges within a sync call, report periods within a repair run, and
    profiles within an incremental run: additive counts, summed official
    presence counts, and samples concatenated then bounded once by
    ``report_top_n`` so that single knob is the end-to-end report bound.
    """
    merged_summaries = [summary for summary in summaries if summary]
    if not merged_summaries:
        return None
    merged: Dict[str, Any] = {
        "enabled": any(
            bool(summary.get("enabled")) for summary in merged_summaries
        ),
        "scope_canonical_facts": list(
            merged_summaries[0].get("scope_canonical_facts") or []
        ),
    }
    for key in _AGGREGATE_COUNT_KEYS:
        merged[key] = sum(
            int(summary.get(key, 0) or 0) for summary in merged_summaries
        )
    presence_totals: Dict[str, int] = {}
    for summary in merged_summaries:
        for canonical, count in (
            summary.get("official_present_core_fact_counts") or {}
        ).items():
            presence_totals[str(canonical)] = (
                presence_totals.get(str(canonical), 0) + int(count or 0)
            )
    merged["official_present_core_fact_counts"] = presence_totals
    bound = max(0, int(report_top_n))
    for key in ("samples", "sanity_samples"):
        combined: List[Dict[str, Any]] = []
        for summary in merged_summaries:
            combined.extend(summary.get(key) or [])
        merged[key] = combined[:bound]
    return merged
