"""Carry reviewed traceability evidence across task-state-only regenerations."""

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
    validate_v2_registry_data,
)


def _json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _catalog_by_path(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row["path"]): row for row in registry["source_catalog"]}


def _nodes_by_id(
    registry: dict[str, Any], collection: str, identity_field: str
) -> dict[str, dict[str, Any]]:
    rows = registry[collection]
    result = {str(row[identity_field]): row for row in rows}
    if len(result) != len(rows):
        raise MigrationError(f"duplicate {identity_field} in {collection}")
    return result


def _assert_semantic_sources_unchanged(
    candidate: dict[str, Any], promoted: dict[str, Any]
) -> None:
    candidate_catalog = _catalog_by_path(candidate)
    promoted_catalog = _catalog_by_path(promoted)
    if set(candidate_catalog) != set(promoted_catalog):
        raise MigrationError("source catalog paths changed; reviewed evidence cannot migrate")
    for path, current in candidate_catalog.items():
        previous = promoted_catalog[path]
        if current["kind"] != previous["kind"]:
            raise MigrationError(f"source catalog kind changed: {path}")
        # Current-source hashes may change only when node-level aliases below
        # prove the exact prior text and locator for every reused identity.
        if current["kind"] not in {"tasks", "requirements", "spec"}:
            raise MigrationError(f"unsupported source catalog kind: {path}")


def _assert_nodes_unchanged(
    candidate: dict[str, Any], promoted: dict[str, Any]
) -> None:
    collections = (
        ("requirement_leaves", "requirement_leaf_id"),
        ("spec_clauses", "spec_clause_id"),
    )
    for collection, identity_field in collections:
        current = _nodes_by_id(candidate, collection, identity_field)
        previous = _nodes_by_id(promoted, collection, identity_field)
        if not set(previous) <= set(current):
            raise MigrationError(
                f"{collection} previous identity set changed; independent review is required"
            )
        for node_id, current_node in current.items():
            if node_id not in previous:
                continue
            previous_node = previous[node_id]
            stable_fields = (
                "status",
                "source_key",
                "text_sha256",
                "normalized_text",
                "aliases",
                "source_aliases",
                "retired_reason",
            )
            if collection == "requirement_leaves":
                stable_fields += ("superseded_by",)
            changed = any(
                current_node.get(field) != previous_node.get(field)
                for field in stable_fields
            )
            current_locator = {
                key: value
                for key, value in current_node["source_locator"].items()
                if key != "line_range"
            }
            previous_locator = {
                key: value
                for key, value in previous_node["source_locator"].items()
                if key != "line_range"
            }
            changed = changed or current_locator != previous_locator
            if not changed:
                continue
            aliases = {
                str(alias.get("source_key")): alias
                for alias in current_node.get("source_aliases", [])
            }
            alias = aliases.get(str(previous_node["source_key"]))
            if (
                alias is None
                or alias.get("text_sha256") != previous_node.get("text_sha256")
                or alias.get("locator") != previous_node.get("source_locator")
            ):
                raise MigrationError(
                    f"{collection} semantic identity changed without prior alias for {node_id}"
                )


def migrate_promoted_registry(
    candidate: dict[str, Any],
    promoted: dict[str, Any],
    *,
    promoted_path: Path,
    task_binding_review: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Migrate exact reviewed links when only the task catalog state changed."""

    if candidate.get("coverage_links"):
        raise MigrationError("candidate already contains coverage links")
    expected_previous_hash = candidate.get("previous_requirement_baseline", {}).get(
        "registry_sha256"
    )
    promoted_file_hash = _file_sha256(promoted_path)
    if expected_previous_hash != promoted_file_hash:
        raise MigrationError("candidate is not pinned to the promoted registry file")

    _assert_semantic_sources_unchanged(candidate, promoted)
    _assert_nodes_unchanged(candidate, promoted)

    current = json.loads(json.dumps(candidate))
    current["coverage_links"] = json.loads(
        json.dumps(promoted.get("coverage_links", []))
    )
    reviewed_task_bindings = _apply_task_binding_review(
        current, task_binding_review
    )
    current_specs = _nodes_by_id(current, "spec_clauses", "spec_clause_id")
    promoted_specs = _nodes_by_id(promoted, "spec_clauses", "spec_clause_id")
    migrated_multi_shall = 0
    for spec_id, current_node in current_specs.items():
        previous_node = promoted_specs.get(spec_id)
        if previous_node is None:
            continue
        if previous_node.get("multi_shall_disposition") != "compound_single_clause":
            continue
        note = previous_node.get("multi_shall_review_note")
        if not note:
            raise MigrationError(f"promoted multi-SHALL review lacks note: {spec_id}")
        current_node["multi_shall_disposition"] = "compound_single_clause"
        current_node["multi_shall_review_note"] = note
        migrated_multi_shall += 1

    validation = validate_v2_registry_data(
        current,
        previous_v2_path=promoted_path,
        require_complete=True,
    )
    return current, {
        **validation,
        "migration_kind": "task_state_only_review_carry_forward",
        "candidate_registry_sha256": _json_sha256(candidate),
        "promoted_registry_file_sha256": promoted_file_hash,
        "migrated_coverage_links": len(current["coverage_links"]),
        "migrated_multi_shall_reviews": migrated_multi_shall,
        "reviewed_task_bindings": reviewed_task_bindings,
        "final_registry_sha256": _json_sha256(current),
    }


def _apply_task_binding_review(
    registry: dict[str, Any], review: dict[str, Any] | None
) -> int:
    """Apply independently reviewed task responsibilities to stable exact edges."""

    if review is None:
        return 0
    expected_candidate_hash = review.get("candidate_registry_sha256")
    candidate_without_links = json.loads(json.dumps(registry))
    candidate_without_links["coverage_links"] = []
    if expected_candidate_hash != _json_sha256(candidate_without_links):
        raise MigrationError("task-binding review is not bound to current candidate")
    reviewer = review.get("reviewer")
    if not reviewer:
        raise MigrationError("task-binding review lacks reviewer")
    links = {
        (
            str(link["requirement_leaf_id"]),
            str(link["spec_clause_id"]),
        ): link
        for link in registry["coverage_links"]
    }
    seen: set[tuple[str, str, str]] = set()
    requirement_ids = {
        str(node["requirement_leaf_id"])
        for node in registry["requirement_leaves"]
        if node["status"] == "active"
    }
    spec_ids = {
        str(node["spec_clause_id"])
        for node in registry["spec_clauses"]
        if node["status"] == "active"
    }
    maximum_link_id = max(
        int(str(link["coverage_link_id"]).rsplit("-", 1)[1])
        for link in registry["coverage_links"]
    )
    for row in review.get("rows", []):
        requirement_id = str(row.get("requirement_leaf_id", ""))
        spec_id = str(row.get("spec_clause_id", ""))
        task_id = str(row.get("task_id", ""))
        key = (requirement_id, spec_id, task_id)
        if not all(key) or key in seen:
            raise MigrationError("task-binding review has duplicate or incomplete row")
        seen.add(key)
        if not row.get("review_note"):
            raise MigrationError("task-binding review lacks review note")
        link = links.get((requirement_id, spec_id))
        if row.get("add_coverage_link") is True:
            if link is not None:
                raise MigrationError("task-binding review duplicates an existing edge")
            if requirement_id not in requirement_ids or spec_id not in spec_ids:
                raise MigrationError("task-binding review references an inactive identity")
            owner = str(row.get("owner") or "")
            relationship = str(row.get("relationship") or "")
            if not owner or relationship not in {"implements", "verifies", "constrains"}:
                raise MigrationError("task-binding review has invalid new edge metadata")
            maximum_link_id += 1
            registry["coverage_links"].append(
                {
                    "coverage_link_id": f"AAM-V1-LNK-{maximum_link_id:04d}",
                    "status": "active",
                    "aliases": [],
                    "requirement_leaf_id": requirement_id,
                    "spec_clause_id": spec_id,
                    "task_ids": [task_id],
                    "owner": owner,
                    "relationship": relationship,
                    "rationale": (
                        f"reviewed_incremental_binding; reviewer={reviewer}; "
                        f"{row['review_note']}"
                    ),
                    "retired_reason": None,
                }
            )
            links[(requirement_id, spec_id)] = registry["coverage_links"][-1]
            continue
        if link is None:
            raise MigrationError("task-binding review does not reference exact promoted edge")
        link["task_ids"] = sorted(set(link["task_ids"]) | {task_id}, key=_task_key)
        link["rationale"] += (
            f"; reviewed_task_binding={task_id}; reviewer={reviewer}; "
            f"{row['review_note']}"
        )
    return len(seen)


def _task_key(task_id: str) -> tuple[int, int]:
    major, minor = task_id.split(".", maxsplit=1)
    return int(major), int(minor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--previous-v2", type=Path, required=True)
    parser.add_argument("--task-binding-review", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or args.report.exists():
        raise MigrationError("migration output exists; refusing to overwrite")
    registry, report = migrate_promoted_registry(
        _load_json(args.candidate),
        _load_json(args.previous_v2),
        promoted_path=args.previous_v2,
        task_binding_review=(
            _load_json(args.task_binding_review)
            if args.task_binding_review is not None
            else None
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(registry, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    args.report.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
