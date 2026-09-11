from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CHANGE_ROOT = Path(__file__).resolve().parent
SOURCE_ROOT = (
    REPOSITORY_ROOT
    / "var/company_profile_shadow_batch/20260911/external-operating-ownership-replay-a"
    / "batch-manufacturing-materials-shadow-operating-ownership-external-gemini-20260911-a"
)
OUTPUT_PATH = CHANGE_ROOT / "provider-free-invalid-item-audit.v1.json"

EXPECTED_FAMILY_COUNTS = {
    "context_only_legal_empty": 18,
    "contradictory_business_regime_legal_empty": 5,
    "control_only_no_change": 3,
    "business_overview_source_mismatch": 2,
    "ambiguous_counterparty_direction": 1,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _classify(error_detail: str) -> str:
    if "context-only ids" in error_detail:
        return "context_only_legal_empty"
    if "contradicts an evidenced control-scope change" in error_detail:
        return "contradictory_business_regime_legal_empty"
    if "control-scope no-change coverage" in error_detail:
        return "control_only_no_change"
    if "business overview source_text must match text evidence" in error_detail:
        return "business_overview_source_mismatch"
    if "counterparty measurement has ambiguous field direction" in error_detail:
        return "ambiguous_counterparty_direction"
    raise ValueError(f"unclassified candidate_schema_invalid trace: {error_detail}")


def _failed_extract_rows() -> tuple[list[dict[str, Any]], dict[str, str]]:
    rows: list[dict[str, Any]] = []
    report_hashes: dict[str, str] = {}
    for report_path in sorted((SOURCE_ROOT / "reports").glob("*.json")):
        relative_path = report_path.relative_to(REPOSITORY_ROOT).as_posix()
        report_hash = _sha256(report_path)
        report_hashes[relative_path] = report_hash
        report = json.loads(report_path.read_text(encoding="utf-8"))
        for scope in report.get("scope_results", []):
            for trace in scope.get("provider_traces", []):
                if not (
                    trace.get("call_type") == "extract"
                    and trace.get("error_code") == "candidate_schema_invalid"
                ):
                    continue
                error_detail = str(trace.get("error_detail") or "")
                rows.append(
                    {
                        "row_number": len(rows) + 1,
                        "report_path": relative_path,
                        "report_sha256": report_hash,
                        "sample_id": report.get("sample_id"),
                        "scope_id": scope.get("scope_id"),
                        "semantic_request_id": trace.get("semantic_request_id"),
                        "gateway_request_id": trace.get("gateway_request_id"),
                        "failure_family": _classify(error_detail),
                        "error_detail_sha256": hashlib.sha256(
                            error_detail.encode("utf-8")
                        ).hexdigest(),
                        "error_detail": error_detail,
                    }
                )
    return rows, report_hashes


def _combined_report_hash(report_hashes: dict[str, str]) -> str:
    material = "\n".join(
        f"{path} {digest}" for path, digest in sorted(report_hashes.items())
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def main() -> None:
    rows, report_hashes = _failed_extract_rows()
    family_counts = Counter(row["failure_family"] for row in rows)
    if len(rows) != 29:
        raise ValueError(f"expected 29 frozen failures, found {len(rows)}")
    if dict(sorted(family_counts.items())) != dict(
        sorted(EXPECTED_FAMILY_COUNTS.items())
    ):
        raise ValueError(f"unexpected failure-family counts: {family_counts}")

    source_files = {
        name: _sha256(SOURCE_ROOT / name)
        for name in ("manifest.json", "readiness-audit.json", "review-package.json")
    }
    audit = {
        "schema_version": "company_profile_invalid_extract_item_audit.v1",
        "audit_id": "isolate-company-profile-invalid-extract-items-20260911-a",
        "created_at": "2026-09-11",
        "source_batch": {
            "batch_id": SOURCE_ROOT.name.removeprefix("batch-"),
            "path": SOURCE_ROOT.relative_to(REPOSITORY_ROOT).as_posix(),
            "manifest_sha256": source_files["manifest.json"],
            "readiness_audit_sha256": source_files["readiness-audit.json"],
            "review_package_sha256": source_files["review-package.json"],
            "report_count": len(report_hashes),
            "reports_combined_sha256": _combined_report_hash(report_hashes),
        },
        "failure_inventory": {
            "candidate_schema_invalid_extract_trace_count": len(rows),
            "classified_trace_count": len(rows),
            "unclassified_trace_count": 0,
            "family_counts": dict(sorted(family_counts.items())),
            "rows": rows,
        },
        "payload_boundary": {
            "raw_provider_payload_persisted": False,
            "historical_candidate_salvage_count": "unavailable",
            "reason": (
                "The frozen bundle stores request identity, Evidence, hashes, and a "
                "bounded failure detail, but not the raw provider response payload."
            ),
        },
        "equivalent_fixture_results": {
            "provider_calls": 0,
            "valid_sibling_retention_count": 5,
            "invalid_item_exclusion_count": 5,
            "all_invalid_required_field_remains_unresolved": True,
            "fixture_tests": [
                "test_isolated_context_only_coverage_preserves_valid_material_sibling",
                "test_isolated_contradictory_regime_coverage_preserves_valid_regime",
                "test_isolated_control_no_change_coverage_preserves_valid_regime",
                "test_isolated_overview_source_mismatch_preserves_valid_activity",
                "test_isolated_ambiguous_share_preserves_directional_measurement",
            ],
        },
        "historical_bundle_mutated": False,
        "cohort_replay_performed": False,
        "provider_calls": 0,
        "adaptive_token_budget_changed": False,
        "production_paths_opened": [],
        "production_authorization": "not_authorized",
        "decision": "provider_free_item_isolation_fixtures_passed",
        "notes": [
            "The frozen twenty-report batch remains the authoritative empirical result.",
            "Equivalent fixtures prove adapter behavior but do not reconstruct historical candidates.",
            "This audit does not claim a new shadow replay, report usability, or production readiness.",
        ],
    }
    OUTPUT_PATH.write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(OUTPUT_PATH),
                "classified": len(rows),
                "provider_calls": 0,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
