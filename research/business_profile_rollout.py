"""Frozen benchmarks and fail-closed rollout gates for semantic production."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


BENCHMARK_SCHEMA_VERSION = "business_profile_atomic_benchmark.v1"
PROMOTION_MANIFEST_SCHEMA_VERSION = "business_profile_promotion_manifest.v1"
ROLLOUT_POLICY_VERSION = "business_profile_rollout.v1"
BENCHMARK_SPLITS = ("development", "holdout", "challenge", "production_sampling")

_REQUIRED_BENCHMARK_FIELDS = {
    "item_id",
    "instrument_id",
    "source_document_id",
    "field_family",
    "temporal_class",
    "exact_spans",
    "atomic_activities",
    "relationships",
    "exposure_facts",
    "negative_assertions",
}


def build_frozen_atomic_benchmark(
    items: Sequence[Mapping[str, Any]],
    *,
    runtime_identities: Mapping[str, str],
) -> dict[str, Any]:
    if not items:
        raise ValueError("atomic benchmark requires items")
    if not runtime_identities or any(
        not str(key).strip() or not str(value).strip()
        for key, value in runtime_identities.items()
    ):
        raise ValueError("atomic benchmark runtime identities are incomplete")
    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    issuer_split: dict[str, str] = {}
    for raw in items:
        item = dict(raw)
        missing = sorted(_REQUIRED_BENCHMARK_FIELDS - set(item))
        if missing:
            raise ValueError(f"atomic benchmark item missing fields: {missing}")
        item_id = _required_text(item, "item_id")
        if item_id in seen_ids:
            raise ValueError(f"duplicate atomic benchmark item_id: {item_id}")
        seen_ids.add(item_id)
        instrument_id = _required_text(item, "instrument_id")
        _required_text(item, "source_document_id")
        _required_text(item, "field_family")
        _required_text(item, "temporal_class")
        for key in (
            "exact_spans",
            "atomic_activities",
            "relationships",
            "exposure_facts",
            "negative_assertions",
        ):
            if not isinstance(item.get(key), list):
                raise ValueError(f"atomic benchmark {key} must be an array")
        split = issuer_split.setdefault(instrument_id, _benchmark_split(instrument_id))
        normalized.append({**item, "split": split})
    normalized.sort(key=lambda item: (item["split"], item["instrument_id"], item["item_id"]))
    core = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "runtime_identities": dict(sorted(runtime_identities.items())),
        "split_policy": "issuer_hash_no_report_leakage.v1",
        "items": normalized,
    }
    return {
        **core,
        "benchmark_hash": _stable_hash(core),
        "counts": {
            "items": len(normalized),
            "issuers": len(issuer_split),
            "by_split": {
                split: sum(item["split"] == split for item in normalized)
                for split in BENCHMARK_SPLITS
            },
        },
    }


def evaluate_atomic_benchmark(
    benchmark: Mapping[str, Any],
    evaluation_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if benchmark.get("schema_version") != BENCHMARK_SCHEMA_VERSION:
        raise ValueError("unsupported atomic benchmark schema")
    expected_ids = {str(item["item_id"]) for item in benchmark.get("items") or []}
    rows_by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[str] = set()
    for raw in evaluation_rows:
        row = dict(raw)
        item_id = _required_text(row, "item_id")
        if item_id not in expected_ids:
            raise ValueError(f"evaluation item is outside frozen benchmark: {item_id}")
        if item_id in seen:
            raise ValueError(f"duplicate evaluation item_id: {item_id}")
        seen.add(item_id)
        rows_by_family[_required_text(row, "field_family")].append(row)
    missing = sorted(expected_ids - seen)
    if missing:
        raise ValueError(f"benchmark evaluation is incomplete: {missing[:5]}")
    metrics = {
        family: _family_metrics(rows)
        for family, rows in sorted(rows_by_family.items())
    }
    payload = {
        "schema_version": "business_profile_atomic_evaluation.v1",
        "benchmark_hash": benchmark["benchmark_hash"],
        "runtime_identities": dict(benchmark["runtime_identities"]),
        "field_families": metrics,
    }
    payload["evaluation_hash"] = _stable_hash(payload)
    return payload


def build_field_family_promotion_manifests(
    evaluation: Mapping[str, Any],
    *,
    thresholds: Mapping[str, Mapping[str, float]],
) -> dict[str, dict[str, Any]]:
    manifests: dict[str, dict[str, Any]] = {}
    for family, metrics in sorted((evaluation.get("field_families") or {}).items()):
        required = dict(thresholds.get(family) or {})
        if not required:
            raise ValueError(f"promotion thresholds are missing: {family}")
        reasons = _threshold_failures(metrics, required)
        core = {
            "schema_version": PROMOTION_MANIFEST_SCHEMA_VERSION,
            "field_family": family,
            "enabled": not reasons,
            "benchmark_hash": evaluation["benchmark_hash"],
            "evaluation_hash": evaluation["evaluation_hash"],
            "runtime_identities": dict(evaluation["runtime_identities"]),
            "thresholds": required,
            "realized_metrics": dict(metrics),
            "automatic_disable_conditions": [
                "runtime_identity_changed",
                "unsupported_output_threshold_exceeded",
                "temporal_correctness_threshold_failed",
                "evidence_validity_threshold_failed",
                "drift_threshold_exceeded",
                "exception_rate_threshold_exceeded",
            ],
            "reason_codes": reasons,
            "policy_version": ROLLOUT_POLICY_VERSION,
        }
        manifests[family] = {**core, "manifest_hash": _stable_hash(core)}
    return manifests


def select_first_industry_cohort(
    industry_rows: Sequence[Mapping[str, Any]],
    *,
    minimum_instruments: int = 5,
) -> dict[str, Any]:
    eligible: list[dict[str, Any]] = []
    for raw in industry_rows:
        row = dict(raw)
        instruments = int(row.get("instrument_count") or 0)
        if instruments < minimum_instruments:
            continue
        explicit = _rate(row, "explicit_table_coverage")
        native = _rate(row, "native_text_quality")
        catalog = _rate(row, "catalog_coverage")
        difficulty = _rate(row, "semantic_difficulty")
        if min(explicit, native, catalog) <= 0:
            continue
        score = 0.35 * explicit + 0.30 * native + 0.25 * catalog + 0.10 * difficulty
        eligible.append({**row, "selection_score": score})
    if not eligible:
        raise ValueError("no industry cohort satisfies production prerequisites")
    selected = max(
        eligible,
        key=lambda row: (
            float(row["selection_score"]),
            int(row.get("instrument_count") or 0),
            str(row.get("industry_group") or ""),
        ),
    )
    return {
        "schema_version": "business_profile_industry_cohort_selection.v1",
        "selected_industry_group": selected["industry_group"],
        "selection_score": selected["selection_score"],
        "instrument_count": selected["instrument_count"],
        "selection_basis": {
            key: selected[key]
            for key in (
                "explicit_table_coverage",
                "native_text_quality",
                "catalog_coverage",
                "semantic_difficulty",
            )
        },
    }


def run_shadow_backfill_validation(
    source_db: str | Path,
    runner: Callable[[Path], Mapping[str, Any]],
) -> dict[str, Any]:
    source = Path(source_db)
    if not source.is_file():
        raise FileNotFoundError(source)
    source_hash_before = _file_hash(source)
    with tempfile.TemporaryDirectory(prefix="business-profile-shadow-") as temp_dir:
        shadow = Path(temp_dir) / source.name
        shutil.copy2(source, shadow)
        first = dict(runner(shadow))
        first_hash = _stable_hash(first.get("governed_output"))
        second = dict(runner(shadow))
        second_hash = _stable_hash(second.get("governed_output"))
    source_hash_after = _file_hash(source)
    required_flags = {
        "bulk_transaction_ok",
        "point_in_time_reads_ok",
        "machine_rework_recovery_ok",
        "zero_valuation_leakage",
    }
    failures = sorted(
        key for key in required_flags if first.get(key) is not True or second.get(key) is not True
    )
    if first_hash != second_hash:
        failures.append("replay_output_changed")
    if source_hash_before != source_hash_after:
        failures.append("source_database_changed")
    return {
        "schema_version": "business_profile_shadow_backfill_validation.v1",
        "passed": not failures,
        "reason_codes": failures,
        "replay_hash": first_hash,
        "source_database_hash": source_hash_before,
        "first_run": first,
        "second_run": second,
    }


def run_bounded_production_pilot(
    *,
    instrument_ids: Sequence[str],
    enabled_manifests: Mapping[str, Mapping[str, Any]],
    runner: Callable[[Sequence[str]], Mapping[str, Any]],
    rollback_drill: Callable[[], bool],
    kill_switch_drill: Callable[[], bool],
    max_instruments: int = 20,
) -> dict[str, Any]:
    instruments = tuple(dict.fromkeys(str(item).strip() for item in instrument_ids if str(item).strip()))
    if not instruments or len(instruments) > max_instruments:
        raise ValueError("production pilot instrument bound is invalid")
    enabled_families = sorted(
        family for family, manifest in enabled_manifests.items() if manifest.get("enabled") is True
    )
    if not enabled_families:
        raise ValueError("production pilot requires an enabled field-family manifest")
    result = dict(runner(instruments))
    rollback_ok = rollback_drill() is True
    kill_switch_ok = kill_switch_drill() is True
    required = {
        "zero_candidate_valuation_leakage": True,
        "audited_system_promotion": True,
        "bounded_scope": True,
    }
    failures = [key for key, value in required.items() if result.get(key) is not value]
    if not rollback_ok:
        failures.append("rollback_drill_failed")
    if not kill_switch_ok:
        failures.append("kill_switch_drill_failed")
    return {
        "schema_version": "business_profile_production_pilot.v1",
        "passed": not failures,
        "reason_codes": failures,
        "instrument_count": len(instruments),
        "enabled_field_families": enabled_families,
        "result": result,
        "rollback_drill_ok": rollback_ok,
        "kill_switch_drill_ok": kill_switch_ok,
    }


def evaluate_rollout_expansion(
    realized: Mapping[str, float],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    failures = _threshold_failures(realized, thresholds)
    return {
        "expand": not failures,
        "reason_codes": failures,
        "realized": dict(realized),
        "thresholds": dict(thresholds),
    }


def evaluate_scheduler_readiness(
    *,
    issuer_count: int,
    changed_issuer_rate: float,
    seconds_per_changed_issuer: float,
    available_window_seconds: float,
    exception_backlog: int,
    maximum_exception_backlog: int,
) -> dict[str, Any]:
    estimated_changed = max(0.0, issuer_count * changed_issuer_rate)
    estimated_seconds = estimated_changed * seconds_per_changed_issuer
    reasons: list[str] = []
    if estimated_seconds > available_window_seconds:
        reasons.append("full_market_capacity_exceeded")
    if exception_backlog > maximum_exception_backlog:
        reasons.append("exception_backlog_policy_failed")
    return {
        "scheduler_ready": not reasons,
        "reason_codes": reasons,
        "estimated_changed_issuers": estimated_changed,
        "estimated_runtime_seconds": estimated_seconds,
        "available_window_seconds": available_window_seconds,
        "exception_backlog": exception_backlog,
    }


def persist_rollout_artifact(payload: Mapping[str, Any], root: str | Path) -> Path:
    artifact_hash = _stable_hash(payload)
    path = Path(root) / f"{artifact_hash}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise RuntimeError(f"immutable rollout artifact mismatch: {path}")
    if not path.exists():
        path.write_text(encoded, encoding="utf-8")
    return path


def _family_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    count = len(rows)
    tp = sum(int(row.get("true_positive") or 0) for row in rows)
    fp = sum(int(row.get("false_positive") or 0) for row in rows)
    fn = sum(int(row.get("false_negative") or 0) for row in rows)
    predicted = tp + fp
    expected = tp + fn
    return {
        "item_count": count,
        "precision": tp / predicted if predicted else 1.0,
        "recall": tp / expected if expected else 1.0,
        "unsupported_output_rate": sum(bool(row.get("unsupported_output")) for row in rows) / count,
        "evidence_validity": sum(row.get("evidence_valid") is True for row in rows) / count,
        "temporal_correctness": sum(row.get("temporal_correct") is True for row in rows) / count,
        "stability": sum(row.get("stable_replay") is True for row in rows) / count,
        "deterministic_completion_rate": sum(row.get("deterministic_complete") is True for row in rows) / count,
        "llm_call_rate": sum(int(row.get("llm_calls") or 0) > 0 for row in rows) / count,
        "average_cost": sum(float(row.get("cost") or 0) for row in rows) / count,
        "average_latency_seconds": sum(float(row.get("latency_seconds") or 0) for row in rows) / count,
        "human_exception_rate": sum(str(row.get("exception_tier") or "") in {"quick_review", "deep_review"} for row in rows) / count,
    }


def _threshold_failures(metrics: Mapping[str, Any], thresholds: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for key, threshold in sorted(thresholds.items()):
        actual = float(metrics.get(key) or 0)
        if key.startswith("max_"):
            metric_key = key[4:]
            actual = float(metrics.get(metric_key) or 0)
            if actual > float(threshold):
                reasons.append(f"threshold_failed:{metric_key}:max")
        elif key.startswith("min_"):
            metric_key = key[4:]
            actual = float(metrics.get(metric_key) or 0)
            if actual < float(threshold):
                reasons.append(f"threshold_failed:{metric_key}:min")
        else:
            raise ValueError(f"threshold key must start with min_ or max_: {key}")
    return reasons


def _benchmark_split(instrument_id: str) -> str:
    bucket = int(hashlib.sha256(instrument_id.encode("utf-8")).hexdigest()[:8], 16) % 100
    if bucket < 55:
        return "development"
    if bucket < 80:
        return "holdout"
    if bucket < 92:
        return "challenge"
    return "production_sampling"


def _rate(row: Mapping[str, Any], key: str) -> float:
    value = float(row.get(key) or 0)
    if not 0 <= value <= 1:
        raise ValueError(f"industry cohort rate out of range: {key}")
    return value


def _required_text(row: Mapping[str, Any], key: str) -> str:
    value = str(row.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
