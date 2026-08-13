"""Finalize a fully reviewed v2 traceability registry and fail closed."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    MigrationError,
    _load_json,
    _task_sort_key,
    parse_tasks,
    validate_v2_registry_data,
)


def _json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def finalize_registry(
    candidate: dict[str, Any],
    migration: dict[str, Any],
    pending_reviews: list[dict[str, Any]],
    multishall_review: dict[str, Any],
    *,
    v1_baseline_path: Path,
    previous_v2_path: Path,
    split_manifest_path: Path | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    candidate_hash = _json_sha256(candidate)
    if migration.get("candidate_registry_sha256") != candidate_hash:
        raise MigrationError("coverage migration is not bound to current candidate")
    if multishall_review.get("candidate_registry_sha256") != candidate_hash:
        raise MigrationError("multi-SHALL review is not bound to current candidate")

    pending_by_id = {
        str(row["pending_review_id"]): row for row in migration["pending_edges"]
    }
    reviewed_rows: dict[str, dict[str, Any]] = {}
    for review in pending_reviews:
        if review.get("candidate_registry_sha256") != candidate_hash:
            raise MigrationError("pending review is not bound to current candidate")
        reviewer = review.get("reviewer") or "record-owner"
        for row in review.get("rows", []):
            review_id = str(row.get("pending_review_id", ""))
            if not review_id or review_id in reviewed_rows:
                raise MigrationError(f"duplicate or missing pending review id: {review_id}")
            reviewed_rows[review_id] = {**row, "_reviewer": row.get("reviewer") or reviewer}
    if set(reviewed_rows) != set(pending_by_id):
        missing = sorted(set(pending_by_id) - set(reviewed_rows))
        extra = sorted(set(reviewed_rows) - set(pending_by_id))
        raise MigrationError(
            f"pending review set is incomplete or stale: "
            f"missing={len(missing)}, extra={len(extra)}"
        )

    tasks = parse_tasks()
    accepted = [dict(row) for row in migration["exact_edges"]]
    accepted_pending = 0
    corrected_pending = 0
    rejected_pending = 0
    for review_id, review in reviewed_rows.items():
        source = pending_by_id[review_id]
        status = review.get("review_status")
        if status == "rejected":
            rejected_pending += 1
            continue
        if status not in {"approved", "corrected"}:
            raise MigrationError(f"unresolved pending review: {review_id}")
        if not review.get("review_note"):
            raise MigrationError(f"pending review lacks note: {review_id}")
        if status == "corrected":
            corrected_pending += 1
            row = {
                "requirement_leaf_id": review.get(
                    "corrected_requirement_leaf_id", source["requirement_leaf_id"]
                ),
                "spec_clause_id": review.get(
                    "corrected_spec_clause_id", source["spec_clause_id"]
                ),
                "task_ids": review.get("corrected_task_ids", source["task_ids"]),
                "owner": review.get("corrected_owner", source["owner"]),
                "relationship": review.get(
                    "corrected_relationship", source["relationship"]
                ),
            }
        else:
            accepted_pending += 1
            row = {
                key: source[key]
                for key in (
                    "requirement_leaf_id",
                    "spec_clause_id",
                    "task_ids",
                    "owner",
                    "relationship",
                )
            }
        accepted.append(
            {
                **row,
                "reviewer": review["_reviewer"],
                "review_note": review["review_note"],
                "migration_status": f"reviewed_{status}",
            }
        )

    active_requirements = {
        str(node["requirement_leaf_id"])
        for node in candidate["requirement_leaves"]
        if node["status"] == "active"
    }
    active_specs = {
        str(node["spec_clause_id"])
        for node in candidate["spec_clauses"]
        if node["status"] == "active"
    }
    accepted = _merge_and_validate_edges(
        accepted,
        active_requirements=active_requirements,
        active_specs=active_specs,
        tasks=tasks,
    )
    registry = json.loads(json.dumps(candidate))
    registry["coverage_links"] = [
        {
            "coverage_link_id": f"AAM-V1-LNK-{index:04d}",
            "status": "active",
            "aliases": [],
            "requirement_leaf_id": row["requirement_leaf_id"],
            "spec_clause_id": row["spec_clause_id"],
            "task_ids": row["task_ids"],
            "owner": row["owner"],
            "relationship": row["relationship"],
            "rationale": (
                f"{row.get('migration_status', 'reviewed')}; "
                f"reviewer={row.get('reviewer')}; {row.get('review_note')}"
            ),
            "retired_reason": None,
        }
        for index, row in enumerate(accepted, start=1)
    ]

    spec_by_id = {
        str(node["spec_clause_id"]): node for node in registry["spec_clauses"]
    }
    multi_rows = list(multishall_review.get("rows", []))
    if len({str(row["spec_clause_id"]) for row in multi_rows}) != len(multi_rows):
        raise MigrationError("duplicate multi-SHALL review rows")
    pending_multi_ids = {
        spec_id
        for spec_id, node in spec_by_id.items()
        if node["status"] == "active"
        and node["multi_shall_disposition"] == "pending_review"
    }
    if {str(row["spec_clause_id"]) for row in multi_rows} != pending_multi_ids:
        raise MigrationError("multi-SHALL review does not exactly cover pending clauses")
    for row in multi_rows:
        spec_id = str(row["spec_clause_id"])
        node = spec_by_id[spec_id]
        if row.get("spec_text_sha256") != node["text_sha256"]:
            raise MigrationError(f"multi-SHALL text hash mismatch: {spec_id}")
        if row.get("disposition") != "compound_single_clause" or not row.get(
            "review_note"
        ):
            raise MigrationError(f"invalid multi-SHALL disposition: {spec_id}")
        node["multi_shall_disposition"] = "compound_single_clause"
        node["multi_shall_review_note"] = row["review_note"]

    validation = validate_v2_registry_data(
        registry,
        baseline_path=v1_baseline_path,
        previous_v2_path=previous_v2_path,
        spec_split_manifest_path=split_manifest_path,
        require_complete=True,
    )
    return registry, {
        **validation,
        "accepted_pending": accepted_pending,
        "corrected_pending": corrected_pending,
        "rejected_pending": rejected_pending,
        "exact_migrated_edges": len(migration["exact_edges"]),
        "final_registry_sha256": _json_sha256(registry),
    }


def _merge_and_validate_edges(
    rows: list[dict[str, Any]],
    *,
    active_requirements: set[str],
    active_specs: set[str],
    tasks: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        requirement_id = str(row["requirement_leaf_id"])
        spec_id = str(row["spec_clause_id"])
        if requirement_id not in active_requirements or spec_id not in active_specs:
            raise MigrationError(f"reviewed edge references inactive identity: {(requirement_id, spec_id)}")
        task_ids = list(row["task_ids"])
        if not task_ids or set(task_ids) - set(tasks):
            raise MigrationError(f"reviewed edge has invalid tasks: {(requirement_id, spec_id)}")
        if not row.get("owner") or row.get("relationship") not in {
            "implements",
            "verifies",
            "constrains",
        }:
            raise MigrationError(f"reviewed edge has invalid binding: {(requirement_id, spec_id)}")
        edge = (requirement_id, spec_id)
        current = result.get(edge)
        normalized = {
            **row,
            "requirement_leaf_id": requirement_id,
            "spec_clause_id": spec_id,
            "task_ids": sorted(set(task_ids), key=_task_sort_key),
        }
        if current is None:
            result[edge] = normalized
            continue
        if current["owner"] != normalized["owner"] or current["relationship"] != normalized["relationship"]:
            raise MigrationError(f"conflicting final coverage edge: {edge}")
        current["task_ids"] = sorted(
            set(current["task_ids"]) | set(normalized["task_ids"]), key=_task_sort_key
        )
        current["review_note"] = (
            f"{current.get('review_note')}; merged reviewer={normalized.get('reviewer')}"
        )
    return sorted(
        result.values(),
        key=lambda row: (
            int(row["requirement_leaf_id"].rsplit("-", 1)[1]),
            int(row["spec_clause_id"].rsplit("-", 1)[1]),
        ),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--coverage-migration", type=Path, required=True)
    parser.add_argument("--pending-review", type=Path, action="append", default=[])
    parser.add_argument("--multishall-review", type=Path, required=True)
    parser.add_argument("--v1-baseline", type=Path, required=True)
    parser.add_argument("--previous-v2", type=Path, required=True)
    parser.add_argument("--spec-split-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or args.report.exists():
        raise MigrationError("final registry output exists; refusing to overwrite")
    registry, report = finalize_registry(
        _load_json(args.candidate),
        _load_json(args.coverage_migration),
        [_load_json(path) for path in args.pending_review],
        _load_json(args.multishall_review),
        v1_baseline_path=args.v1_baseline,
        previous_v2_path=args.previous_v2,
        split_manifest_path=args.spec_split_manifest,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(registry, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    args.report.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
