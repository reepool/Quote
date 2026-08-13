"""Migrate exact reviewed coverage edges across requirement cleanup and spec splits."""

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
)


def _json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def migrate_reviews(
    review_baseline: dict[str, Any],
    current: dict[str, Any],
    split_manifest: dict[str, Any],
    main_reviews: list[dict[str, Any]],
    supplemental_reviews: list[dict[str, Any]],
    additions: dict[str, Any],
    suggestions: dict[str, Any],
) -> dict[str, Any]:
    current_sha256 = _json_sha256(current)
    if additions.get("candidate_registry_sha256") != current_sha256:
        raise MigrationError("coverage additions are not bound to current candidate")
    if suggestions.get("candidate_registry_sha256") != current_sha256:
        raise MigrationError("coverage suggestions are not bound to current candidate")

    old_requirements = {
        str(node["requirement_leaf_id"]): node
        for node in review_baseline["requirement_leaves"]
    }
    old_specs = {
        str(node["spec_clause_id"]): node for node in review_baseline["spec_clauses"]
    }
    current_requirements_by_key = {
        str(node["source_key"]): node
        for node in current["requirement_leaves"]
        if node["status"] == "active"
    }
    current_specs_by_key = {
        str(node["source_key"]): node
        for node in current["spec_clauses"]
        if node["status"] == "active"
    }
    current_specs_by_hash = {
        str(node["text_sha256"]): node
        for node in current["spec_clauses"]
        if node["status"] == "active"
    }
    split_by_old_id = {
        str(entry["old_spec_clause_id"]): entry
        for entry in split_manifest["entries"]
    }
    tasks = parse_tasks()

    old_edges: list[dict[str, Any]] = []
    for review in main_reviews:
        top_reviewer = review.get("reviewer") or "record-owner"
        for row in review.get("rows", []):
            status = row.get("review_status")
            if status == "rejected":
                continue
            if status not in {"approved", "corrected"}:
                raise MigrationError("main coverage review contains unresolved row")
            corrected = status == "corrected"
            old_edges.append(
                {
                    "requirement_leaf_id": str(row["requirement_leaf_id"]),
                    "spec_clause_id": str(
                        row.get("corrected_spec_clause_id")
                        if corrected
                        else row["spec_clause_id"]
                    ),
                    "task_ids": list(
                        row.get("corrected_task_ids")
                        if corrected
                        else row["task_ids"]
                    ),
                    "owner": str(
                        row.get("corrected_owner") if corrected else row["owner"]
                    ),
                    "relationship": str(
                        row.get("corrected_relationship")
                        if corrected
                        else row["relationship"]
                    ),
                    "reviewer": row.get("reviewer") or top_reviewer,
                    "review_note": row.get("review_note") or "reviewed coverage edge",
                }
            )
    for review in supplemental_reviews:
        top_reviewer = review.get("reviewer") or "record-owner"
        for row in review.get("rows", []):
            old_edges.append(
                {
                    "requirement_leaf_id": str(row["requirement_leaf_id"]),
                    "spec_clause_id": str(row["spec_clause_id"]),
                    "task_ids": list(row["task_ids"]),
                    "owner": str(row["owner"]),
                    "relationship": str(row["relationship"]),
                    "reviewer": row.get("reviewer") or top_reviewer,
                    "review_note": row.get("review_note") or "supplemental review",
                }
            )

    exact: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    dropped_retired_requirement_edges = 0
    for edge in old_edges:
        old_requirement = old_requirements.get(edge["requirement_leaf_id"])
        old_spec = old_specs.get(edge["spec_clause_id"])
        if old_requirement is None or old_spec is None:
            raise MigrationError("review edge references unknown review-baseline identity")
        current_requirement = current_requirements_by_key.get(
            str(old_requirement["source_key"])
        )
        if current_requirement is None:
            dropped_retired_requirement_edges += 1
            continue
        base = {
            "requirement_leaf_id": current_requirement["requirement_leaf_id"],
            "task_ids": _validated_tasks(edge["task_ids"], tasks),
            "owner": edge["owner"],
            "relationship": edge["relationship"],
            "reviewer": edge["reviewer"],
            "review_note": edge["review_note"],
        }
        split = split_by_old_id.get(edge["spec_clause_id"])
        if split is not None:
            for target in split["new_clauses"]:
                current_spec = current_specs_by_hash.get(str(target["text_sha256"]))
                if current_spec is None:
                    raise MigrationError("split target missing from current candidate")
                pending.append(
                    {
                        **base,
                        "spec_clause_id": current_spec["spec_clause_id"],
                        "migration_status": "pending_split_review",
                        "review_note": (
                            f"Inherited from split source {edge['spec_clause_id']}; "
                            f"{edge['review_note']}"
                        ),
                    }
                )
            continue
        current_spec = current_specs_by_key.get(str(old_spec["source_key"]))
        if current_spec is None:
            continue
        exact.append(
            {
                **base,
                "spec_clause_id": current_spec["spec_clause_id"],
                "migration_status": "approved_exact_source",
            }
        )

    for row in additions.get("rows", []):
        pending.append(
            {
                **row,
                "reviewer": "requirements-reverse-review",
                "migration_status": "pending_added_requirement_review",
            }
        )

    exact = _merge_edges(exact)
    pending = _merge_edges(pending)
    covered_specs = {row["spec_clause_id"] for row in (*exact, *pending)}
    covered_requirements = {
        row["requirement_leaf_id"] for row in (*exact, *pending)
    }
    active_spec_ids = {
        str(node["spec_clause_id"])
        for node in current["spec_clauses"]
        if node["status"] == "active"
    }
    active_requirement_ids = {
        str(node["requirement_leaf_id"])
        for node in current["requirement_leaves"]
        if node["status"] == "active"
    }

    suggestions_by_spec = _suggestions_by_target(suggestions["suggestions"], "spec_clause_id")
    suggestions_by_requirement = _suggestions_by_target(
        suggestions["suggestions"], "requirement_leaf_id"
    )
    for spec_id in sorted(active_spec_ids - covered_specs, key=_spec_sort_key):
        pending.append(
            _pending_from_suggestion(
                suggestions_by_spec[spec_id][0], "pending_uncovered_spec_review"
            )
        )
    covered_requirements = {
        row["requirement_leaf_id"] for row in (*exact, *pending)
    }
    for requirement_id in sorted(
        active_requirement_ids - covered_requirements, key=_requirement_sort_key
    ):
        pending.append(
            _pending_from_suggestion(
                suggestions_by_requirement[requirement_id][0],
                "pending_uncovered_requirement_review",
            )
        )
    pending = _merge_edges(pending)
    covered_specs = {row["spec_clause_id"] for row in (*exact, *pending)}
    covered_requirements = {
        row["requirement_leaf_id"] for row in (*exact, *pending)
    }
    if active_spec_ids - covered_specs or active_requirement_ids - covered_requirements:
        raise MigrationError("coverage migration queue is not bidirectionally complete")
    for index, row in enumerate(exact, start=1):
        row["coverage_evidence_id"] = f"AAM-V1-MIG-{index:04d}"
    for index, row in enumerate(pending, start=1):
        row["pending_review_id"] = f"AAM-V1-PND-{index:04d}"
    return {
        "schema_version": "announcement_asset_coverage_review_migration.v1",
        "candidate_registry_sha256": current_sha256,
        "exact_edges": exact,
        "pending_edges": pending,
        "summary": {
            "exact_edges": len(exact),
            "pending_edges": len(pending),
            "covered_specs": len(covered_specs),
            "covered_requirements": len(covered_requirements),
            "dropped_retired_requirement_edges": dropped_retired_requirement_edges,
        },
    }


def _validated_tasks(
    task_ids: list[str], tasks: dict[str, dict[str, Any]]
) -> list[str]:
    if not task_ids or set(task_ids) - set(tasks):
        raise MigrationError(f"coverage edge has invalid tasks: {task_ids}")
    return sorted(set(task_ids), key=_task_sort_key)


def _merge_edges(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        edge = (str(row["requirement_leaf_id"]), str(row["spec_clause_id"]))
        current = result.get(edge)
        if current is None:
            result[edge] = row
            continue
        if current["owner"] != row["owner"] or current["relationship"] != row["relationship"]:
            raise MigrationError(f"conflicting migrated coverage edge: {edge}")
        current["task_ids"] = sorted(
            set(current["task_ids"]) | set(row["task_ids"]), key=_task_sort_key
        )
        current["review_note"] += f"; merged reviewer={row.get('reviewer')}"
    return sorted(
        result.values(),
        key=lambda row: (
            _requirement_sort_key(row["requirement_leaf_id"]),
            _spec_sort_key(row["spec_clause_id"]),
        ),
    )


def _suggestions_by_target(
    suggestions: list[dict[str, Any]], field: str
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in suggestions:
        result.setdefault(str(row[field]), []).append(row)
    return result


def _pending_from_suggestion(row: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "requirement_leaf_id": row["requirement_leaf_id"],
        "spec_clause_id": row["spec_clause_id"],
        "task_ids": row["task_ids"],
        "owner": row["owner"],
        "relationship": row["relationship"],
        "reviewer": None,
        "review_note": status,
        "migration_status": status,
    }


def _spec_sort_key(value: str) -> int:
    return int(value.rsplit("-", 1)[1])


def _requirement_sort_key(value: str) -> int:
    return int(value.rsplit("-", 1)[1])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--review-baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--spec-split-manifest", type=Path, required=True)
    parser.add_argument("--main-review", type=Path, action="append", required=True)
    parser.add_argument(
        "--supplemental-review", type=Path, action="append", required=True
    )
    parser.add_argument("--additions", type=Path, required=True)
    parser.add_argument("--suggestions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise MigrationError("coverage migration output exists; refusing to overwrite")
    result = migrate_reviews(
        _load_json(args.review_baseline),
        _load_json(args.candidate),
        _load_json(args.spec_split_manifest),
        [_load_json(path) for path in args.main_review],
        [_load_json(path) for path in args.supplemental_review],
        _load_json(args.additions),
        _load_json(args.suggestions),
    )
    result["source_evidence_sha256"] = {
        "review_baseline": _file_sha256(args.review_baseline),
        **{
            f"main_review_{index}": _file_sha256(path)
            for index, path in enumerate(args.main_review, start=1)
        },
        **{
            f"supplemental_review_{index}": _file_sha256(path)
            for index, path in enumerate(args.supplemental_review, start=1)
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
