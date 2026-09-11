"""Generate the hash-bound post-isolation scale-readiness audit."""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
CHANGE_DIR = Path(__file__).resolve().parent
BATCH_DIR = (
    ROOT
    / "var/company_profile_shadow_batch/20260911/external-operating-ownership-replay-a"
    / "batch-manufacturing-materials-shadow-operating-ownership-external-gemini-20260911-a"
)
REVIEW_ARCHIVE = (
    ROOT
    / "openspec/changes/archive/2026-09-11-replay-company-profile-operating-ownership-external-path"
)
ISOLATION_ARCHIVE = (
    ROOT
    / "openspec/changes/archive/2026-09-11-isolate-company-profile-invalid-extract-items"
)
OUTPUT = CHANGE_DIR / "provider-free-scale-readiness-audit.v1.json"

EXPECTED_BATCH_ID = (
    "manufacturing-materials-shadow-operating-ownership-external-gemini-20260911-a"
)
EXPECTED_REPORT_COUNT = 20
EXPECTED_FAILED_TRACE_COUNT = 29


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(body).hexdigest()


def _nearest_rank(values: list[int], percentile: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _input_paths() -> dict[str, Path]:
    return {
        "batch_manifest": BATCH_DIR / "manifest.json",
        "batch_review_package": BATCH_DIR / "review-package.json",
        "source_review_package": REVIEW_ARCHIVE / "source-review-package.v1.json",
        "source_review_outcomes": REVIEW_ARCHIVE / "source-review-outcomes.v1.json",
        "empirical_readiness": REVIEW_ARCHIVE / "empirical-readiness-audit.v1.json",
        "invalid_item_audit": (
            ISOLATION_ARCHIVE / "provider-free-invalid-item-audit.v1.json"
        ),
    }


def _validate_and_load() -> tuple[dict[str, Any], ...]:
    paths = _input_paths()
    manifest = _load(paths["batch_manifest"])
    source_review = _load(paths["source_review_package"])
    outcomes = _load(paths["source_review_outcomes"])
    readiness = _load(paths["empirical_readiness"])
    isolation = _load(paths["invalid_item_audit"])

    if manifest["batch_id"] != EXPECTED_BATCH_ID:
        raise ValueError("unexpected authoritative batch identity")
    if len(manifest["reports"]) != EXPECTED_REPORT_COUNT:
        raise ValueError("authoritative batch must retain exactly twenty reports")
    if manifest["completed_report_count"] != EXPECTED_REPORT_COUNT:
        raise ValueError("authoritative batch execution is incomplete")
    if manifest["failed_report_count"] != 0:
        raise ValueError("authoritative batch contains report-level failures")
    if _sha256(paths["batch_review_package"]) != _sha256(
        paths["source_review_package"]
    ):
        raise ValueError("archived source review package differs from batch package")
    if source_review["batch_id"] != EXPECTED_BATCH_ID:
        raise ValueError("source review belongs to a different batch")
    if source_review["source_batch_result_hash"] != manifest["result_hash"]:
        raise ValueError("source review result hash mismatch")
    if readiness["source_batch_result_hash"] != manifest["result_hash"]:
        raise ValueError("readiness result hash mismatch")
    if readiness["review_package_hash"] != source_review["package_hash"]:
        raise ValueError("readiness review-package hash mismatch")
    review_ids = {row["review_row_id"] for row in source_review["rows"]}
    outcome_ids = {row["review_row_id"] for row in outcomes}
    if len(review_ids) != len(source_review["rows"]):
        raise ValueError("source review row identities are not unique")
    if len(outcome_ids) != len(outcomes):
        raise ValueError("source review outcome identities are not unique")
    if review_ids != outcome_ids:
        raise ValueError("source review outcomes are incomplete or contain unknown rows")

    source_batch = isolation["source_batch"]
    if source_batch["batch_id"] != EXPECTED_BATCH_ID:
        raise ValueError("invalid-item audit belongs to a different batch")
    if source_batch["manifest_sha256"] != _sha256(paths["batch_manifest"]):
        raise ValueError("invalid-item audit manifest hash mismatch")
    if source_batch["review_package_sha256"] != _sha256(
        paths["batch_review_package"]
    ):
        raise ValueError("invalid-item audit review-package hash mismatch")
    failures = isolation["failure_inventory"]["rows"]
    if len(failures) != EXPECTED_FAILED_TRACE_COUNT:
        raise ValueError("invalid-item audit must retain all 29 failed traces")
    if isolation["failure_inventory"]["unclassified_trace_count"] != 0:
        raise ValueError("invalid-item audit contains unclassified traces")
    if isolation["payload_boundary"]["raw_provider_payload_persisted"]:
        raise ValueError("historical raw payload boundary unexpectedly changed")

    report_refs = {row["sample_id"]: row for row in manifest["reports"]}
    if len(report_refs) != EXPECTED_REPORT_COUNT:
        raise ValueError("batch report identities are not unique")
    observed_failure_keys: set[tuple[str, str, str, str | None]] = set()
    for sample_id, reference in report_refs.items():
        report_path = BATCH_DIR / reference["relative_path"]
        if _sha256(report_path) != reference["output_sha256"]:
            raise ValueError(f"report hash mismatch: {sample_id}")
        report = _load(report_path)
        if report["sample_id"] != sample_id or report["batch_id"] != EXPECTED_BATCH_ID:
            raise ValueError(f"report identity mismatch: {sample_id}")
        for scope in report["scope_results"]:
            for trace in scope["provider_traces"]:
                if (
                    trace["call_type"] == "extract"
                    and trace["status"] == "failed"
                    and trace["error_code"] == "candidate_schema_invalid"
                ):
                    observed_failure_keys.add(
                        (
                            sample_id,
                            scope["scope_id"],
                            trace["semantic_request_id"],
                            trace["gateway_request_id"],
                        )
                    )
    inventoried_failure_keys = {
        (
            row["sample_id"],
            row["scope_id"],
            row["semantic_request_id"],
            row["gateway_request_id"],
        )
        for row in failures
    }
    if observed_failure_keys != inventoried_failure_keys:
        raise ValueError("invalid-item inventory does not match frozen failed traces")

    return manifest, source_review, outcomes, readiness, isolation


def build_audit() -> dict[str, Any]:
    manifest, review, outcomes, readiness, isolation = _validate_and_load()
    paths = _input_paths()
    failures = isolation["failure_inventory"]["rows"]

    failed_scopes: dict[str, set[str]] = defaultdict(set)
    failed_calls: Counter[str] = Counter()
    family_counts_by_report: dict[str, Counter[str]] = defaultdict(Counter)
    for row in failures:
        failed_calls[row["sample_id"]] += 1
        failed_scopes[row["sample_id"]].add(row["scope_id"])
        family_counts_by_report[row["sample_id"]][row["failure_family"]] += 1

    unresolved_by_report: Counter[str] = Counter()
    unresolved_in_failed_scopes: Counter[str] = Counter()
    for row in review["rows"]:
        if row["category"] != "unresolved":
            continue
        sample_id = row["sample_id"]
        unresolved_by_report[sample_id] += 1
        if row["scope_id"] in failed_scopes[sample_id]:
            unresolved_in_failed_scopes[sample_id] += 1

    report_rows: list[dict[str, Any]] = []
    unaffected_holds: list[str] = []
    optimistic_workloads: list[int] = []
    for metric in readiness["reports"]:
        sample_id = metric["sample_id"]
        failed = failed_calls[sample_id]
        unresolved = metric["unresolved_human_review_count"]
        if unresolved != unresolved_by_report[sample_id]:
            raise ValueError(f"unresolved review count mismatch: {sample_id}")
        removable = unresolved_in_failed_scopes[sample_id]
        optimistic_remaining = unresolved - removable
        optimistic_workloads.append(optimistic_remaining)
        hold_without_failure = metric["report_status"] == "hold" and failed == 0
        if hold_without_failure:
            unaffected_holds.append(sample_id)
        report_rows.append(
            {
                "sample_id": sample_id,
                "observed_report_status": metric["report_status"],
                "candidate_schema_invalid_extract_calls": failed,
                "affected_scope_count": len(failed_scopes[sample_id]),
                "failure_family_counts": dict(sorted(family_counts_by_report[sample_id].items())),
                "observed_unresolved_review_count": unresolved,
                "unresolved_rows_in_failed_scopes": removable,
                "optimistic_remaining_unresolved_count": optimistic_remaining,
                "hold_without_applicable_failed_call": hold_without_failure,
            }
        )

    reports_with_failures = sum(row["candidate_schema_invalid_extract_calls"] > 0 for row in report_rows)
    currently_usable = sum(
        row["observed_report_status"] in {"usable", "usable_with_caveats", "complete"}
        for row in report_rows
    )
    absolute_usable_upper_count = EXPECTED_REPORT_COUNT - len(unaffected_holds)
    absolute_usable_upper_rate = absolute_usable_upper_count / EXPECTED_REPORT_COUNT
    current_unresolved = [row["observed_unresolved_review_count"] for row in report_rows]

    noncritical = [row for row in outcomes if row["outcome"] == "noncritical_error"]
    critical = [row for row in outcomes if row["outcome"] == "critical_error"]
    if len(noncritical) != 4 or critical:
        raise ValueError("reviewed precision findings changed from the frozen result")

    optimistic_gates = {
        "usable_reports_at_least_90pct": absolute_usable_upper_rate >= 0.90,
        "sampled_precision_at_least_99pct": readiness["sampled_precision"] >= 0.99,
        "human_review_median_at_most_2": statistics.median(optimistic_workloads) <= 2,
        "human_review_p90_at_most_5": _nearest_rank(optimistic_workloads, 0.90) <= 5,
    }
    payload: dict[str, Any] = {
        "schema_version": "company_profile_shadow_post_isolation_scale_audit.v1",
        "audit_id": "quantify-company-profile-shadow-scale-readiness-20260911-a",
        "created_at": "2026-09-11",
        "input_hashes": {name: _sha256(path) for name, path in sorted(paths.items())},
        "authoritative_identity": {
            "batch_id": manifest["batch_id"],
            "batch_result_hash": manifest["result_hash"],
            "review_package_hash": review["package_hash"],
            "report_count": EXPECTED_REPORT_COUNT,
            "review_row_count": len(review["rows"]),
            "review_outcome_count": len(outcomes),
        },
        "observed": {
            "execution_completion_rate": readiness["execution_completion_rate"],
            "usable_report_count": currently_usable,
            "usable_report_rate": readiness["usable_report_rate"],
            "accepted_record_count": readiness["accepted_record_count"],
            "evidence_traceability_rate": readiness["evidence_traceability_rate"],
            "candidate_schema_invalid_extract_trace_count": len(failures),
            "reports_with_candidate_schema_invalid": reports_with_failures,
            "unresolved_review_total": sum(current_unresolved),
            "unresolved_review_median": readiness["unresolved_review_median"],
            "unresolved_review_p90": readiness["unresolved_review_p90"],
            "precision_reviewed_count": readiness["precision_reviewed_count"],
            "precision_correct_count": readiness["precision_correct_count"],
            "sampled_precision": readiness["sampled_precision"],
            "critical_semantic_error_count": readiness["critical_semantic_error_count"],
            "noncritical_semantic_error_count": len(noncritical),
            "readiness_decision": readiness["readiness_decision"],
            "failed_gates": sorted(
                name for name, passed in readiness["gate_results"].items() if not passed
            ),
        },
        "deterministic_bounds": {
            "evidence_level": "optimistic_provider_free_upper_or_lower_bound",
            "hold_reports_without_applicable_failed_call": sorted(unaffected_holds),
            "absolute_usable_report_upper_count": absolute_usable_upper_count,
            "absolute_usable_report_upper_rate": absolute_usable_upper_rate,
            "usable_gate_threshold": 0.90,
            "usable_gate_reachable_by_item_isolation_alone": absolute_usable_upper_rate >= 0.90,
            "unresolved_rows_in_failed_scopes_maximum_removable": sum(
                unresolved_in_failed_scopes.values()
            ),
            "optimistic_remaining_unresolved_total": sum(optimistic_workloads),
            "optimistic_unresolved_median_floor": float(
                statistics.median(optimistic_workloads)
            ),
            "optimistic_unresolved_p90_floor": float(
                _nearest_rank(optimistic_workloads, 0.90)
            ),
            "optimistic_gate_results_without_changing_precision": optimistic_gates,
            "optimistic_still_failed_gates": sorted(
                name for name, passed in optimistic_gates.items() if not passed
            ),
            "interpretation": (
                "Even if every failure-affected hold report became usable and every unresolved "
                "row in a failed scope disappeared, item isolation alone would still miss the "
                "usable-rate, sampled-precision, and p90 workload gates."
            ),
        },
        "source_review_findings": {
            "noncritical_errors": noncritical,
            "critical_errors": critical,
            "independent_of_historical_salvage": True,
        },
        "report_exposure": report_rows,
        "unavailable": {
            "raw_provider_payloads": "not_persisted",
            "historical_valid_sibling_count": "unavailable",
            "historical_recovered_accepted_record_count": "unavailable",
            "post_isolation_report_statuses": "unavailable_without_new_empirical_replay",
            "post_isolation_sampled_precision": "unavailable_without_new_source_review",
        },
        "boundaries": {
            "provider_calls": 0,
            "historical_bundle_mutated": False,
            "report_statuses_rewritten": False,
            "adaptive_token_budget_changed": False,
            "new_replay_authorized": False,
            "production_paths_opened": [],
            "production_authorization": "not_authorized",
        },
        "decision": "hold",
        "next_business_priority": (
            "Reduce report holds and reviewed precision errors on a future separately "
            "authorized empirical validation; token-budget dry-run remains a separate change."
        ),
    }
    payload["audit_hash"] = _payload_hash(payload)
    return payload


def main() -> None:
    payload = build_audit()
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if OUTPUT.exists() and OUTPUT.read_text(encoding="utf-8") != rendered:
        raise RuntimeError(f"immutable audit mismatch: {OUTPUT}")
    OUTPUT.write_text(rendered, encoding="utf-8")
    print(OUTPUT)


if __name__ == "__main__":
    main()
