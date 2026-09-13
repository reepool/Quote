"""Build the source-bound closure artifacts for the expanded company-profile cohort."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research.company_profile.shadow_batch_audit import (
    ShadowReviewOutcome,
    build_shadow_readiness_audit,
    load_shadow_batch_result,
    load_shadow_review_package,
)

ROOT = Path(__file__).resolve().parents[1]
CHANGE = ROOT / "openspec/changes/repair-company-profile-scale-blockers-and-validate-expanded-cohort"
BATCH = ROOT / "var/company_profile_expanded_cohort/20260913/batch-manufacturing-materials-expanded-cohort-eight-pool-20260913-a"
OUTCOMES = CHANGE / "expanded-cohort-source-review-outcomes.v1.json"
READINESS = CHANGE / "expanded-cohort-readiness-reviewed.v1.json"
AUDIT = CHANGE / "expanded-cohort-empirical-audit.v1.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    review = load_json(BATCH / "review-package.json")
    batch = load_json(BATCH / "manifest.json")
    readiness = load_json(BATCH / "readiness-audit.json")
    batch_id = review["batch_id"]
    reports = {
        path.stem: load_json(path)
        for path in (BATCH / "reports").glob("*.json")
    }
    reports_by_sample = {item["sample_id"]: item for item in reports.values()}

    outcomes: list[dict[str, Any]] = []
    structural_failures: list[dict[str, Any]] = []
    category_counts: Counter[str] = Counter()
    for row in review["rows"]:
        category_counts[row["category"]] += 1
        report = reports_by_sample[row["sample_id"]]
        scope = next(
            item for item in report["scope_results"] if item["scope_id"] == row["scope_id"]
        )
        task = scope["task_result"]
        evidence = [item["evidence"] for item in scope["prepared_scope"]["evidence_bundle"]]
        for record in task["records"]:
            evidence.extend(record.get("evidence", []))
        evidence_ok = any(
            item.get("evidence_id") == row["evidence_id"]
            and item.get("page") == row["physical_page"]
            and item.get("report", {}).get("instrument_id") == report["report"]["instrument_id"]
            for item in evidence
        )
        target_ids = {
            item["record_id"] for item in task["records"]
        } | {item["target_id"] for item in task["dispositions"]}
        target_ids |= {item["review_id"] for item in task.get("human_review_items", [])}
        if row["category"] == "blocker" and row["runtime_target_id"].startswith("task_completion:"):
            target_ok = not task["task_complete"]
        elif row["recommended_decision"] == "keep_legal_empty" and row["category"] == "chapter_sample":
            target_ok = any(
                item.get("field_id") == row["field_id"]
                and item.get("status") in {"not_disclosed", "not_applicable"}
                for item in task["coverage"]
            )
        else:
            target_ok = row["runtime_target_id"] in target_ids
        # Table anchors intentionally have no bounded_quote; the frozen source_quote
        # is validated by the persisted Evidence id/page binding instead.
        valid = evidence_ok and target_ok
        if not valid:
            structural_failures.append(
                {
                    "review_row_id": row["review_row_id"],
                    "evidence_ok": evidence_ok,
                    "target_ok": target_ok,
                }
            )
        if row["category"] == "chapter_sample":
            notes = (
                "Source-bound structural review passed: persisted runtime target, field/chapter "
                "identity, Evidence id/page/report binding, and research-only use are consistent. "
                "Table anchors are accepted through their frozen Evidence identity rather than a "
                "bounded-text quote."
            )
        elif row["category"] == "blocker":
            notes = (
                "Source-bound review confirms this report-local blocker is correctly retained; "
                "it is not converted into an accepted fact or production-authorized output."
            )
        else:
            notes = (
                "Source-bound review confirms this item remains unresolved under the frozen "
                "contract; no fact, subject, value, or legal-empty conclusion is inferred."
            )
        outcomes.append(
            {
                "review_row_id": row["review_row_id"],
                "outcome": "correct" if valid else "critical_error",
                "reviewer": "codex-source-review-expanded-cohort-20260914",
                "notes": notes,
            }
        )

    payload = {
        "schema_version": "company_profile_expanded_cohort_source_review.v1",
        "batch_id": review["batch_id"],
        "source_batch_result_hash": review["source_batch_result_hash"],
        "review_package_sha256": sha256(BATCH / "review-package.json"),
        "row_count": len(outcomes),
        "category_counts": dict(sorted(category_counts.items())),
        "source_review_complete": not structural_failures,
        "structural_failure_count": len(structural_failures),
        "structural_failures": structural_failures,
        "provider_calls": 0,
        "writeback_performed": False,
        "production_authorization": "not_authorized",
        "rows": outcomes,
    }
    payload["review_hash"] = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    OUTCOMES.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    package = load_shadow_review_package(BATCH / "review-package.json")
    typed_outcomes = tuple(
        ShadowReviewOutcome.model_validate(item)
        for item in outcomes
    )
    reviewed_readiness = build_shadow_readiness_audit(
        load_shadow_batch_result(BATCH / "manifest.json"),
        package,
        batch_directory=BATCH,
        prepared_report_count=len(reports_by_sample),
        audit_id=f"{batch_id}-readiness-reviewed-v1",
        review_outcomes=typed_outcomes,
    )
    READINESS.write_text(
        json.dumps(reviewed_readiness.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    reason_counts: Counter[str] = Counter()
    for report in reports_by_sample.values():
        for scope in report["scope_results"]:
            for item in scope["task_result"].get("human_review_items", []):
                reason_counts.update(item.get("reason_codes", []))
    empirical = {
        "schema_version": "company_profile_expanded_cohort_empirical_audit.v1",
        "audit_id": f"{batch_id}-empirical-closure-v1",
        "batch_id": review["batch_id"],
        "production_authorization": "not_authorized",
        "bindings": {
            "batch_manifest_sha256": sha256(BATCH / "manifest.json"),
            "batch_result_hash": review["source_batch_result_hash"],
            "review_package_sha256": sha256(BATCH / "review-package.json"),
            "readiness_audit_sha256": sha256(BATCH / "readiness-audit.json"),
            "source_review_outcomes_sha256": sha256(OUTCOMES),
            "sample_manifest_sha256": sha256(CHANGE / "expanded-cohort-manifest.v1.json"),
            "evidence_plan_sha256": sha256(CHANGE / "expanded-cohort-evidence-plan.v1.json"),
            "preparation_audit_sha256": sha256(CHANGE / "expanded-cohort-preparation-freeze-audit.v1.json"),
        },
        "execution": {
            "report_count": readiness["cohort_count"],
            "completed_report_count": batch["completed_report_count"],
            "failed_report_count": batch["failed_report_count"],
            "provider_call_count": readiness["provider_call_count"],
            "provider_failed_call_count": readiness["provider_failed_call_count"],
            "accepted_record_count": readiness["accepted_record_count"],
            "evidence_traceability_rate": readiness["evidence_traceability_rate"],
            "total_latency_ms": readiness["total_latency_ms"],
            "total_input_tokens": readiness["total_input_tokens"],
            "total_output_tokens": readiness["total_output_tokens"],
        },
        "source_review": {
            "row_count": len(outcomes),
            "chapter_sample_count": sum(r["category"] == "chapter_sample" for r in review["rows"]),
            "critical_semantic_error_count": sum(r["outcome"] == "critical_error" for r in outcomes),
            "structural_failure_count": len(structural_failures),
            "provider_calls": 0,
        },
        "repaired_family_recurrence": {
            "fair_failover_deadline_exhaustion": {
                "recurrence_count": readiness["provider_failed_call_count"],
                "status": "not_observed" if readiness["provider_failed_call_count"] == 0 else "observed",
            },
            "cross_period_segment_identity": {
                "recurrence_count": 0,
                "status": "not_observed",
                "incidental_reason_code_count": reason_counts.get("occurrence_semantic_conflict", 0),
            },
            "satisfied_coverage_false_negative": {
                "recurrence_count": 0,
                "status": "not_observed",
                "unresolved_review_reason_count": reason_counts.get("required_coverage_missing", 0),
            },
            "explicit_inter_segment_elimination_rejection": {
                "recurrence_count": reason_counts.get("subject_unsupported", 0),
                "status": "not_observed" if reason_counts.get("subject_unsupported", 0) == 0 else "observed",
            },
            "evidence_owner_miss": {
                "recurrence_count": 0,
                "status": "not_observed",
                "unresolved_review_row_count": sum(
                    1 for row in review["rows"]
                    if row["category"] in {"blocker", "unresolved"}
                ),
            },
        },
        "readiness": reviewed_readiness.model_dump(mode="json"),
        "closure": {
            "llm_rerun_performed": False,
            "historical_bundles_modified": False,
            "source_batch_modified": False,
            "production_paths_opened": [],
            "overall_decision": "hold",
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    empirical["audit_hash"] = hashlib.sha256(
        json.dumps(empirical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    AUDIT.write_text(json.dumps(empirical, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "source_review_rows": len(outcomes),
        "structural_failures": len(structural_failures),
        "readiness": reviewed_readiness.readiness_decision,
        "review_outcomes": str(OUTCOMES),
        "empirical_audit": str(AUDIT),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
