#!/usr/bin/env python
"""Compare official financial source profiles on the same bounded sample.

The command writes only isolated /tmp SQLite databases. It is intended to
compare speed, stability, and core-fact consistency across official profiles
before production promotion decisions.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.dev_validation.validate_sse_official_financial_json_batches_live import (  # noqa: E402
    parse_instrument_ids,
    parse_report_periods,
    resolve_instrument_ids,
    run_batches,
)
from scripts.dev_validation.audit_financial_numeric_fact_coverage import (  # noqa: E402
    DEFAULT_REQUIRED_CANONICAL_FACTS,
    audit_financial_numeric_fact_coverage,
)
from scripts.research_cli_support import json_ready  # noqa: E402
from scripts.research_financial_statements_rollout_validation import (  # noqa: E402
    normalize_report_periods,
)


DEFAULT_CORE_FIELDS = [
    "revenue",
    "net_income",
    "total_assets",
    "total_liabilities",
    "equity",
    "operating_cf",
]


def parse_sources(raw: Optional[str]) -> List[str]:
    if not raw:
        return ["sse", "cninfo"]
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def parse_core_fields(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_CORE_FIELDS)
    return [part.strip() for part in raw.split(",") if part.strip()]


def parse_required_canonical_facts(raw: Optional[str]) -> List[str]:
    if not raw:
        return list(DEFAULT_REQUIRED_CANONICAL_FACTS)
    return [part.strip() for part in raw.split(",") if part.strip()]


def default_output_dir() -> Path:
    return Path("/tmp") / f"quote_official_financial_profile_compare_{os.getpid()}"


async def run_comparison(
    *,
    exchange: str,
    sources: List[str],
    instrument_ids: List[str],
    report_periods: List[str],
    output_dir: Path,
    batch_size: int,
    batch_timeout_seconds: float,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
    repeat: int,
    core_fields: List[str],
    required_canonical_facts: List[str],
    relative_tolerance: float,
    absolute_tolerance: float,
    include_core_fact_details: bool = False,
) -> Dict[str, Any]:
    if not sources:
        raise ValueError("At least one source is required")
    if not instrument_ids:
        raise ValueError("At least one instrument id is required")
    if not report_periods:
        raise ValueError("At least one report period is required")
    if repeat < 1:
        raise ValueError("repeat must be positive")

    output_dir.mkdir(parents=True, exist_ok=True)
    normalized_periods = normalize_report_periods(report_periods)
    profile_runs: Dict[str, List[Dict[str, Any]]] = {source: [] for source in sources}
    started_at = time.perf_counter()

    for iteration in range(1, repeat + 1):
        for source in sources:
            db_path = output_dir / f"{source}_{iteration}.db"
            checkpoint_path = output_dir / f"{source}_{iteration}.checkpoint.json"
            result = await run_batches(
                instrument_ids=instrument_ids,
                exchange=exchange,
                official_source=source,
                report_periods=report_periods,
                db_path=db_path,
                batch_size=batch_size,
                batch_timeout_seconds=batch_timeout_seconds,
                request_timeout_seconds=request_timeout_seconds,
                request_interval_seconds=request_interval_seconds,
                checkpoint_path=checkpoint_path,
                include_batch_details=False,
            )
            profile_runs[source].append(
                {
                    "iteration": iteration,
                    "db_path": str(db_path),
                    "checkpoint_path": str(checkpoint_path),
                    "summary": _profile_run_summary(result),
                    "core_facts": _load_core_facts(
                        db_path,
                        instrument_ids=instrument_ids,
                        report_periods=normalized_periods,
                        core_fields=core_fields,
                    ),
                    "numeric_fact_coverage": _load_numeric_fact_coverage(
                        db_path,
                        instrument_ids=instrument_ids,
                        report_periods=normalized_periods,
                        required_canonical_facts=required_canonical_facts,
                    ),
                }
            )

    profiles = {
        source: _aggregate_profile_runs(
            source,
            runs,
            core_fields=core_fields,
        )
        for source, runs in profile_runs.items()
    }
    baseline_source = sources[0]
    comparisons = [
        compare_core_facts(
            baseline_source=baseline_source,
            other_source=source,
            baseline_facts=profiles[baseline_source]["latest_core_facts"],
            other_facts=profiles[source]["latest_core_facts"],
            core_fields=core_fields,
            relative_tolerance=relative_tolerance,
            absolute_tolerance=absolute_tolerance,
        )
        for source in sources[1:]
    ]
    profile_summary = {
        source: {
            "source_profile": profile["source_profile"],
            "elapsed_seconds_avg": profile["elapsed_seconds_avg"],
            "throughput_instrument_periods_per_minute_avg": profile[
                "throughput_instrument_periods_per_minute_avg"
            ],
            "failed_instrument_period_count": profile[
                "failed_instrument_period_count"
            ],
            "total_numeric_facts_written_avg": profile[
                "total_numeric_facts_written_avg"
            ],
            "latest_core_fact_coverage": profile["latest_core_fact_coverage"],
            "latest_core_fact_semantic_warning_count": profile[
                "latest_core_fact_semantic_warning_count"
            ],
            "latest_numeric_fact_coverage": profile[
                "latest_numeric_fact_coverage_summary"
            ],
        }
        for source, profile in profiles.items()
    }
    output_profiles = {source: dict(profile) for source, profile in profiles.items()}
    if not include_core_fact_details:
        for profile in output_profiles.values():
            profile.pop("latest_core_facts", None)

    return {
        "status": (
            "passed"
            if all(profile["failed_instrument_period_count"] == 0 for profile in profiles.values())
            else "degraded"
        ),
        "write_enabled": False,
        "storage_target": {
            "kind": "temp_sqlite",
            "output_dir": str(output_dir),
        },
        "exchange": exchange,
        "sources": sources,
        "baseline_source": baseline_source,
        "report_periods": normalized_periods,
        "instrument_ids": instrument_ids,
        "instrument_count": len(instrument_ids),
        "instrument_period_count": len(instrument_ids) * len(normalized_periods),
        "repeat": repeat,
        "core_fields": core_fields,
        "required_canonical_facts": required_canonical_facts,
        "request_policy": {
            "batch_size": batch_size,
            "batch_timeout_seconds": batch_timeout_seconds,
            "request_timeout_seconds": request_timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
            "concurrency_assumption": "sequential_profile_runs",
        },
        "profiles": output_profiles,
        "comparisons": comparisons,
        "assessment": build_assessment(
            profiles=profile_summary,
            comparisons=comparisons,
        ),
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
    }


def _profile_run_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": result.get("status"),
        "source": result.get("source") or result.get("official_source"),
        "source_profile": result.get("source_profile"),
        "parser_profile": result.get("parser_profile"),
        "structured_json_fact_parser": result.get("structured_json_fact_parser"),
        "elapsed_seconds": result.get("elapsed_seconds"),
        "throughput_instrument_periods_per_minute": result.get(
            "throughput_instrument_periods_per_minute"
        ),
        "batch_count": result.get("batch_count"),
        "passed_batch_count": result.get("passed_batch_count"),
        "failed_batch_count": result.get("failed_batch_count"),
        "failed_instrument_period_count": result.get(
            "failed_instrument_period_count"
        ),
        "total_source_manifests_written": result.get(
            "total_source_manifests_written"
        ),
        "total_numeric_facts_written": result.get("total_numeric_facts_written"),
        "total_core_facts_written": result.get("total_core_facts_written"),
    }


def _aggregate_profile_runs(
    source: str,
    runs: List[Dict[str, Any]],
    *,
    core_fields: List[str],
) -> Dict[str, Any]:
    summaries = [run["summary"] for run in runs]
    elapsed_values = [
        float(item.get("elapsed_seconds") or 0.0)
        for item in summaries
        if item.get("elapsed_seconds") is not None
    ]
    throughput_values = [
        float(item.get("throughput_instrument_periods_per_minute") or 0.0)
        for item in summaries
        if item.get("throughput_instrument_periods_per_minute") is not None
    ]
    latest_core_facts = runs[-1]["core_facts"] if runs else {}
    latest_numeric_coverage = runs[-1].get("numeric_fact_coverage") if runs else None
    semantic_warnings = [
        {
            "instrument_period": key,
            **warning,
        }
        for key, row in sorted(latest_core_facts.items())
        for warning in row.get("semantic_warnings", [])
    ]
    profile = {
        "source": source,
        "source_profile": _first_present(summaries, "source_profile"),
        "parser_profile": _first_present(summaries, "parser_profile"),
        "run_count": len(runs),
        "status_distribution": _count_values(item.get("status") for item in summaries),
        "elapsed_seconds_avg": _round_or_none(_avg(elapsed_values)),
        "elapsed_seconds_min": _round_or_none(min(elapsed_values) if elapsed_values else None),
        "elapsed_seconds_max": _round_or_none(max(elapsed_values) if elapsed_values else None),
        "throughput_instrument_periods_per_minute_avg": _round_or_none(
            _avg(throughput_values)
        ),
        "failed_instrument_period_count": sum(
            int(item.get("failed_instrument_period_count") or 0)
            for item in summaries
        ),
        "total_source_manifests_written_avg": _round_or_none(
            _avg([float(item.get("total_source_manifests_written") or 0) for item in summaries])
        ),
        "total_numeric_facts_written_avg": _round_or_none(
            _avg([float(item.get("total_numeric_facts_written") or 0) for item in summaries])
        ),
        "total_core_facts_written_avg": _round_or_none(
            _avg([float(item.get("total_core_facts_written") or 0) for item in summaries])
        ),
        "latest_core_fact_coverage": _core_fact_coverage(
            latest_core_facts,
            core_fields=core_fields,
        ),
        "latest_core_fact_semantic_warning_count": len(semantic_warnings),
        "latest_core_fact_semantic_warning_sample": semantic_warnings[:20],
        "latest_numeric_fact_coverage": latest_numeric_coverage,
        "latest_numeric_fact_coverage_summary": _numeric_coverage_summary(
            latest_numeric_coverage
        ),
        "latest_core_facts": latest_core_facts,
        "latest_core_fact_sample": dict(list(sorted(latest_core_facts.items()))[:5]),
        "runs": [
            {
                "iteration": run["iteration"],
                "db_path": run["db_path"],
                "checkpoint_path": run["checkpoint_path"],
                "summary": run["summary"],
            }
            for run in runs
        ],
    }
    return profile


def _load_numeric_fact_coverage(
    db_path: Path,
    *,
    instrument_ids: List[str],
    report_periods: List[str],
    required_canonical_facts: List[str],
) -> Dict[str, Any]:
    if not db_path.exists():
        return {
            "status": "error",
            "error": f"database does not exist: {db_path}",
            "required_canonical_facts": required_canonical_facts,
        }
    try:
        return audit_financial_numeric_fact_coverage(
            db_path=db_path,
            instrument_ids=instrument_ids,
            report_periods=report_periods,
            required_canonical_facts=required_canonical_facts,
        )
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "required_canonical_facts": required_canonical_facts,
        }


def _numeric_coverage_summary(coverage: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(coverage, dict):
        return None
    summary = coverage.get("summary") or {}
    return {
        "status": coverage.get("status"),
        "required_canonical_facts": coverage.get("required_canonical_facts", []),
        "instrument_period_count": summary.get("instrument_period_count"),
        "numeric_fact_count": summary.get("numeric_fact_count"),
        "unmapped_field_count": summary.get("unmapped_field_count"),
        "missing_required_canonical_fact_count": summary.get(
            "missing_required_canonical_fact_count"
        ),
        "canonical_unit_conflict_count": summary.get(
            "canonical_unit_conflict_count"
        ),
        "semantic_warning_count": summary.get("semantic_warning_count"),
    }


def _load_core_facts(
    db_path: Path,
    *,
    instrument_ids: List[str],
    report_periods: List[str],
    core_fields: List[str],
) -> Dict[str, Dict[str, Any]]:
    if not db_path.exists():
        return {}
    allowed_fields = set(DEFAULT_CORE_FIELDS) | {
        "operating_profit",
        "pre_tax_profit",
        "gross_profit",
        "total_cf",
        "shares_outstanding",
    }
    selected_fields = [field for field in core_fields if field in allowed_fields]
    if not selected_fields:
        return {}
    placeholders_ids = ",".join("?" for _ in instrument_ids)
    placeholders_periods = ",".join("?" for _ in report_periods)
    sql = (
        "SELECT instrument_id, report_period, source, source_mode, lineage_json, "
        + ", ".join(selected_fields)
        + " FROM financial_core_facts_hot "
        + f"WHERE instrument_id IN ({placeholders_ids}) "
        + f"AND report_period IN ({placeholders_periods})"
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, [*instrument_ids, *report_periods]).fetchall()
    facts: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row['instrument_id']}|{row['report_period']}"
        lineage = _loads_json(row["lineage_json"])
        facts[key] = {
            "instrument_id": row["instrument_id"],
            "report_period": row["report_period"],
            "source": row["source"],
            "source_mode": row["source_mode"],
            "semantic_warnings": lineage.get("core_fact_warnings") or [],
            "fields": {field: row[field] for field in selected_fields},
        }
    return facts


def compare_core_facts(
    *,
    baseline_source: str,
    other_source: str,
    baseline_facts: Dict[str, Dict[str, Any]],
    other_facts: Dict[str, Dict[str, Any]],
    core_fields: List[str],
    relative_tolerance: float,
    absolute_tolerance: float,
) -> Dict[str, Any]:
    keys = sorted(set(baseline_facts) | set(other_facts))
    mismatches: List[Dict[str, Any]] = []
    compared_field_count = 0
    matched_field_count = 0
    missing_in_baseline = sorted(set(other_facts) - set(baseline_facts))
    missing_in_other = sorted(set(baseline_facts) - set(other_facts))
    for key in keys:
        baseline = baseline_facts.get(key, {}).get("fields", {})
        other = other_facts.get(key, {}).get("fields", {})
        for field in core_fields:
            baseline_value = baseline.get(field)
            other_value = other.get(field)
            if baseline_value is None and other_value is None:
                continue
            compared_field_count += 1
            if _values_close(
                baseline_value,
                other_value,
                relative_tolerance=relative_tolerance,
                absolute_tolerance=absolute_tolerance,
            ):
                matched_field_count += 1
                continue
            mismatches.append(
                {
                    "instrument_period": key,
                    "field": field,
                    baseline_source: baseline_value,
                    other_source: other_value,
                    "absolute_diff": _absolute_diff(baseline_value, other_value),
                    "relative_diff": _relative_diff(baseline_value, other_value),
                }
            )
    return {
        "baseline_source": baseline_source,
        "other_source": other_source,
        "instrument_period_count": len(keys),
        "missing_in_baseline": missing_in_baseline,
        "missing_in_other": missing_in_other,
        "compared_field_count": compared_field_count,
        "matched_field_count": matched_field_count,
        "mismatch_count": len(mismatches),
        "match_ratio": (
            round(matched_field_count / compared_field_count, 6)
            if compared_field_count
            else None
        ),
        "mismatches": mismatches[:100],
        "truncated_mismatch_count": max(0, len(mismatches) - 100),
    }


def build_assessment(
    *,
    profiles: Dict[str, Dict[str, Any]],
    comparisons: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build a compact operational assessment from profile metrics."""
    speed_rank = sorted(
        [
            {
                "source": source,
                "source_profile": profile.get("source_profile"),
                "elapsed_seconds_avg": profile.get("elapsed_seconds_avg"),
                "throughput_instrument_periods_per_minute_avg": profile.get(
                    "throughput_instrument_periods_per_minute_avg"
                ),
            }
            for source, profile in profiles.items()
            if profile.get("elapsed_seconds_avg") is not None
        ],
        key=lambda item: float(item.get("elapsed_seconds_avg") or 0.0),
    )
    stability_rank = sorted(
        [
            {
                "source": source,
                "source_profile": profile.get("source_profile"),
                "failed_instrument_period_count": profile.get(
                    "failed_instrument_period_count"
                ),
            }
            for source, profile in profiles.items()
        ],
        key=lambda item: int(item.get("failed_instrument_period_count") or 0),
    )
    consistency = [
        {
            "baseline_source": item.get("baseline_source"),
            "other_source": item.get("other_source"),
            "match_ratio": item.get("match_ratio"),
            "mismatch_count": item.get("mismatch_count"),
            "mismatch_fields": sorted(
                {
                    str(mismatch.get("field"))
                    for mismatch in item.get("mismatches", [])
                    if mismatch.get("field")
                }
            ),
        }
        for item in comparisons
    ]
    warnings: List[str] = []
    for source, profile in profiles.items():
        if int(profile.get("latest_core_fact_semantic_warning_count") or 0) > 0:
            warnings.append(f"{source}_core_fact_semantic_warnings_present")
        coverage = profile.get("latest_numeric_fact_coverage") or {}
        if coverage.get("status") and coverage.get("status") != "passed":
            warnings.append(f"{source}_numeric_fact_coverage_requires_review")
    for item in consistency:
        mismatch_fields = set(item.get("mismatch_fields") or [])
        if mismatch_fields == {"equity"}:
            warnings.append(
                "equity_mismatch_likely_parent_vs_total_equity_semantics"
            )
        elif mismatch_fields:
            warnings.append("core_fact_mismatch_requires_review")
    return {
        "speed_rank": speed_rank,
        "stability_rank": stability_rank,
        "core_fact_consistency": consistency,
        "warnings": sorted(set(warnings)),
        "provisional_recommendation": _provisional_recommendation(
            speed_rank=speed_rank,
            stability_rank=stability_rank,
            consistency=consistency,
        ),
    }


def _provisional_recommendation(
    *,
    speed_rank: List[Dict[str, Any]],
    stability_rank: List[Dict[str, Any]],
    consistency: List[Dict[str, Any]],
) -> str:
    if not speed_rank:
        return "insufficient_speed_evidence"
    fastest = str(speed_rank[0].get("source") or "")
    has_mismatch = any(int(item.get("mismatch_count") or 0) for item in consistency)
    has_failures = any(
        int(item.get("failed_instrument_period_count") or 0) > 0
        for item in stability_rank
    )
    if fastest == "sse":
        if has_mismatch:
            return "keep_sse_commonquery_as_sse_default_and_use_cninfo_for_cross_check_after_semantic_review"
        if has_failures:
            return "keep_sse_commonquery_as_sse_default_until_alternate_profile_failures_are_resolved"
        return "keep_sse_commonquery_as_sse_default"
    if has_mismatch:
        return "do_not_promote_faster_profile_until_core_fact_semantics_are_resolved"
    return f"candidate_fastest_source:{fastest}"


def _values_close(
    left: Any,
    right: Any,
    *,
    relative_tolerance: float,
    absolute_tolerance: float,
) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError):
        return left == right
    diff = abs(left_value - right_value)
    if diff <= absolute_tolerance:
        return True
    scale = max(abs(left_value), abs(right_value), 1.0)
    return diff / scale <= relative_tolerance


def _absolute_diff(left: Any, right: Any) -> Optional[float]:
    try:
        return round(abs(float(left) - float(right)), 6)
    except (TypeError, ValueError):
        return None


def _relative_diff(left: Any, right: Any) -> Optional[float]:
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError):
        return None
    scale = max(abs(left_value), abs(right_value), 1.0)
    return round(abs(left_value - right_value) / scale, 9)


def _core_fact_coverage(
    core_facts: Dict[str, Dict[str, Any]],
    *,
    core_fields: List[str],
) -> Dict[str, Any]:
    total = len(core_facts) * len(core_fields)
    present = 0
    missing: List[Dict[str, Any]] = []
    for key, row in sorted(core_facts.items()):
        fields = row.get("fields", {})
        for field in core_fields:
            if fields.get(field) is not None:
                present += 1
            else:
                missing.append({"instrument_period": key, "field": field})
    return {
        "instrument_period_count": len(core_facts),
        "required_field_count": total,
        "present_field_count": present,
        "coverage_ratio": round(present / total, 6) if total else None,
        "missing": missing[:100],
        "truncated_missing_count": max(0, len(missing) - 100),
    }


def _loads_json(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _count_values(values: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _first_present(items: List[Dict[str, Any]], key: str) -> Optional[Any]:
    for item in items:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def _avg(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def _round_or_none(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(value, 3)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare official financial source profiles using isolated live dry-runs.",
    )
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument(
        "--sources",
        default="sse,cninfo",
        help="Comma-separated official sources to compare. First source is baseline.",
    )
    parser.add_argument("--instrument-ids")
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Instrument limit when --instrument-ids is omitted.",
    )
    parser.add_argument("--report-period", default="2023Q4")
    parser.add_argument(
        "--report-periods",
        help="Comma-separated report periods. Overrides --report-period.",
    )
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--batch-timeout-seconds", type=float, default=90.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=12.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.1)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--core-fields")
    parser.add_argument(
        "--required-canonical-facts",
        help=(
            "Comma-separated canonical long-form facts required by the "
            "numeric coverage audit."
        ),
    )
    parser.add_argument(
        "--relative-tolerance",
        type=float,
        default=1e-6,
        help="Relative tolerance used for core fact consistency checks.",
    )
    parser.add_argument(
        "--absolute-tolerance",
        type=float,
        default=1e-3,
        help="Absolute tolerance used for core fact consistency checks.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir(),
        help="Directory for isolated SQLite DBs and checkpoints.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="Write the final JSON comparison to this path.",
    )
    parser.add_argument(
        "--include-core-fact-details",
        action="store_true",
        help="Include all loaded core fact rows in the final JSON output.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    instrument_ids = resolve_instrument_ids(
        instrument_ids=parse_instrument_ids(args.instrument_ids),
        exchange=args.exchange,
        limit=args.limit,
    )
    report_periods = (
        parse_report_periods(args.report_periods)
        if args.report_periods
        else [args.report_period]
    )
    result = asyncio.run(
        run_comparison(
            exchange=args.exchange,
            sources=parse_sources(args.sources),
            instrument_ids=instrument_ids,
            report_periods=report_periods,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
            batch_timeout_seconds=args.batch_timeout_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
            request_interval_seconds=args.request_interval_seconds,
            repeat=args.repeat,
            core_fields=parse_core_fields(args.core_fields),
            required_canonical_facts=parse_required_canonical_facts(
                args.required_canonical_facts
            ),
            relative_tolerance=args.relative_tolerance,
            absolute_tolerance=args.absolute_tolerance,
            include_core_fact_details=args.include_core_fact_details,
        )
    )
    payload = json_ready(result)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        with args.output_path.open("w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2, sort_keys=True)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
