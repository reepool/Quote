"""Merge independently reviewed v2 traceability suggestions without overwriting evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Iterable, Sequence
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


def merge_reviews(
    candidate: dict[str, Any],
    suggestions: dict[str, Any],
    reviews: Sequence[dict[str, Any]],
    multi_shall_reviews: Sequence[dict[str, Any]],
    supplemental_reviews: Sequence[dict[str, Any]] = (),
    previous_v2_path: Path | None = None,
    spec_split_manifest_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a fully reviewed candidate or fail closed on incomplete review."""

    candidate_sha256 = _json_sha256(candidate)
    if suggestions.get("candidate_registry_sha256") != candidate_sha256:
        raise MigrationError("suggestions do not match the candidate registry")
    _validate_review_candidate_bindings(
        (*reviews, *multi_shall_reviews, *supplemental_reviews), candidate_sha256
    )
    suggestion_rows = list(suggestions.get("suggestions", []))
    suggestions_by_id = _unique_rows(
        suggestion_rows, "coverage_link_id", "suggestion"
    )
    reviewed_rows = [
        {
            **row,
            "_reviewer": row.get("reviewer")
            or review.get("reviewer")
            or "record-owner",
        }
        for review in reviews
        for row in review.get("rows", [])
    ]
    reviewed_by_id = _align_review_rows(reviewed_rows, suggestion_rows)
    if set(reviewed_by_id) != set(suggestions_by_id):
        missing = sorted(set(suggestions_by_id) - set(reviewed_by_id))
        raise MigrationError(
            f"coverage review set is incomplete: missing={len(missing)}"
        )

    active_requirements = {
        row["requirement_leaf_id"]
        for row in candidate["requirement_leaves"]
        if row["status"] == "active"
    }
    active_specs = {
        row["spec_clause_id"]
        for row in candidate["spec_clauses"]
        if row["status"] == "active"
    }
    tasks = parse_tasks()
    accepted: list[dict[str, Any]] = []
    rejected = 0
    corrected = 0
    for link_id, review_row in reviewed_by_id.items():
        suggestion = suggestions_by_id[link_id]
        _validate_review_source(review_row, suggestion)
        status = review_row.get("review_status")
        if status == "rejected":
            rejected += 1
            continue
        if status not in {"approved", "corrected"}:
            raise MigrationError(f"unresolved review status for {link_id}: {status}")
        if not review_row.get("review_note"):
            raise MigrationError(f"review note is required for {link_id}")

        if status == "corrected":
            corrected += 1
            spec_id = _required_correction(review_row, "corrected_spec_clause_id")
            task_ids = _required_correction(review_row, "corrected_task_ids")
            owner = _required_correction(review_row, "corrected_owner")
            relationship = _required_correction(review_row, "corrected_relationship")
            rationale = review_row.get(
                "corrected_rationale", "independent_manual_semantic_review"
            )
        else:
            spec_id = suggestion["spec_clause_id"]
            task_ids = suggestion["task_ids"]
            owner = suggestion["owner"]
            relationship = suggestion["relationship"]
            rationale = suggestion["rationale"]

        requirement_id = suggestion["requirement_leaf_id"]
        if requirement_id not in active_requirements:
            raise MigrationError(f"review references inactive requirement: {requirement_id}")
        if spec_id not in active_specs:
            raise MigrationError(f"review references inactive spec: {spec_id}")
        unknown_tasks = sorted(set(task_ids) - set(tasks), key=_task_sort_key)
        if unknown_tasks:
            raise MigrationError(f"review references unknown tasks: {unknown_tasks}")
        if not owner or relationship not in {"implements", "verifies", "constrains"}:
            raise MigrationError(f"invalid reviewed binding for {link_id}")
        accepted.append(
            {
                "coverage_link_id": link_id,
                "status": "active",
                "aliases": [],
                "requirement_leaf_id": requirement_id,
                "spec_clause_id": spec_id,
                "task_ids": sorted(set(task_ids), key=_task_sort_key),
                "owner": owner,
                "relationship": relationship,
                "rationale": f"{rationale}; reviewer={review_row['_reviewer']}",
                "retired_reason": None,
            }
        )

    accepted.extend(
        _supplemental_links(
            supplemental_reviews,
            accepted=accepted,
            suggestions=suggestion_rows,
            active_requirements=active_requirements,
            active_specs=active_specs,
            tasks=tasks,
        )
    )

    candidate = json.loads(json.dumps(candidate))
    candidate["coverage_links"] = _merge_duplicate_edges(accepted)
    multi_review_by_id = _multi_shall_review_index(multi_shall_reviews)
    pending_ids = {
        row["spec_clause_id"]
        for row in candidate["spec_clauses"]
        if row["status"] == "active"
        and row["multi_shall_disposition"] == "pending_review"
    }
    if set(multi_review_by_id) != pending_ids:
        missing = sorted(pending_ids - set(multi_review_by_id))
        orphan = sorted(set(multi_review_by_id) - pending_ids)
        raise MigrationError(
            "multi-SHALL review set is incomplete or stale: "
            f"missing={len(missing)}, orphan={len(orphan)}"
        )
    must_split: list[str] = []
    spec_by_id = {row["spec_clause_id"]: row for row in candidate["spec_clauses"]}
    for spec_id, review in multi_review_by_id.items():
        disposition = review.get("disposition")
        note = review.get("review_note")
        if not note:
            raise MigrationError(f"multi-SHALL review note is required for {spec_id}")
        if disposition == "must_split":
            must_split.append(spec_id)
            continue
        if disposition != "compound_single_clause":
            raise MigrationError(
                f"invalid multi-SHALL disposition for {spec_id}: {disposition}"
            )
        spec_by_id[spec_id]["multi_shall_disposition"] = "compound_single_clause"
        spec_by_id[spec_id]["multi_shall_review_note"] = note
    if must_split:
        raise MigrationError(
            f"independent SHALL obligations must be split in source: {len(must_split)}; "
            f"first={min(must_split)}"
        )

    validation = validate_v2_registry_data(
        candidate,
        previous_v2_path=previous_v2_path,
        spec_split_manifest_path=spec_split_manifest_path,
        require_complete=True,
    )
    report = {
        **validation,
        "suggestions": len(suggestion_rows),
        "approved_review_rows": len(reviewed_rows) - rejected - corrected,
        "corrected_review_rows": corrected,
        "rejected_review_rows": rejected,
        "multi_shall_review_rows": len(multi_review_by_id),
        "supplemental_review_rows": sum(
            len(review.get("rows", [])) for review in supplemental_reviews
        ),
        "reviewers": sorted(
            {
                str(review.get("reviewer"))
                for review in reviews
                if review.get("reviewer")
            }
        ),
    }
    return candidate, report


def _unique_rows(
    rows: Iterable[dict[str, Any]], field: str, label: str
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = str(row.get(field, ""))
        if not row_id:
            raise MigrationError(f"{label} lacks {field}")
        if row_id in result:
            raise MigrationError(f"duplicate {label} id: {row_id}")
        result[row_id] = row
    return result


def _validate_review_candidate_bindings(
    reviews: Sequence[dict[str, Any]], candidate_sha256: str
) -> None:
    for review in reviews:
        if review.get("candidate_registry_sha256") != candidate_sha256:
            raise MigrationError("review is not bound to the exact candidate registry")


def _validate_review_source(
    review: dict[str, Any], suggestion: dict[str, Any]
) -> None:
    immutable_fields = (
        "requirement_leaf_id",
        "spec_clause_id",
        "task_ids",
        "owner",
        "relationship",
    )
    for field in immutable_fields:
        if review.get(field) != suggestion.get(field):
            raise MigrationError(
                f"review mutated suggestion field {field}: "
                f"{suggestion['coverage_link_id']}"
            )


def _align_review_rows(
    reviews: Sequence[dict[str, Any]], suggestions: Sequence[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Align reviews across candidate regenerations before link ids are published."""

    _unique_rows(reviews, "coverage_link_id", "review row")
    suggestions_by_id = _unique_rows(
        suggestions, "coverage_link_id", "suggestion"
    )
    by_fingerprint: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for suggestion in suggestions:
        by_fingerprint.setdefault(_binding_fingerprint(suggestion), []).append(suggestion)
    aligned: dict[str, dict[str, Any]] = {}
    for review in reviews:
        review_id = str(review["coverage_link_id"])
        direct = suggestions_by_id.get(review_id)
        if direct is not None and _binding_fingerprint(direct) == _binding_fingerprint(
            review
        ):
            matched = direct
        else:
            matches = by_fingerprint.get(_binding_fingerprint(review), [])
            if len(matches) != 1:
                raise MigrationError(
                    "review cannot be aligned to one current suggestion: "
                    f"{review_id}; matches={len(matches)}"
                )
            matched = matches[0]
        matched_id = str(matched["coverage_link_id"])
        if matched_id in aligned:
            raise MigrationError(f"duplicate aligned review for suggestion: {matched_id}")
        aligned[matched_id] = review
    return aligned


def _binding_fingerprint(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("requirement_leaf_id"),
        row.get("spec_clause_id"),
        tuple(row.get("task_ids", [])),
        row.get("owner"),
        row.get("relationship"),
    )


def _required_correction(row: dict[str, Any], field: str) -> Any:
    value = row.get(field)
    if value is None or value == "" or value == []:
        raise MigrationError(
            f"corrected review {row['coverage_link_id']} lacks {field}"
        )
    return value


def _merge_duplicate_edges(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    by_edge: dict[tuple[str, str], dict[str, Any]] = {}
    for row in sorted(rows, key=lambda value: value["coverage_link_id"]):
        edge = (row["requirement_leaf_id"], row["spec_clause_id"])
        existing = by_edge.get(edge)
        if existing is None:
            by_edge[edge] = row
            continue
        if (
            existing["owner"] != row["owner"]
            or existing["relationship"] != row["relationship"]
        ):
            raise MigrationError(f"conflicting reviewed duplicate edge: {edge}")
        existing["task_ids"] = sorted(
            set(existing["task_ids"]) | set(row["task_ids"]), key=_task_sort_key
        )
        existing["rationale"] += f"; merged_review={row['coverage_link_id']}"
    return sorted(by_edge.values(), key=lambda row: row["coverage_link_id"])


def _multi_shall_review_index(
    reviews: Sequence[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows = [row for review in reviews for row in review.get("rows", [])]
    return _unique_rows(rows, "spec_clause_id", "multi-SHALL review")


def _supplemental_links(
    reviews: Sequence[dict[str, Any]],
    *,
    accepted: Sequence[dict[str, Any]],
    suggestions: Sequence[dict[str, Any]],
    active_requirements: set[str],
    active_specs: set[str],
    tasks: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for review in reviews:
        reviewer = review.get("reviewer") or "record-owner"
        for row in review.get("rows", []):
            rows.append({**row, "_reviewer": row.get("reviewer") or reviewer})
    supplemental_by_edge: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        edge = (str(row.get("requirement_leaf_id", "")), str(row.get("spec_clause_id", "")))
        if not all(edge):
            raise MigrationError("supplemental review lacks requirement/spec identity")
        if edge in supplemental_by_edge:
            raise MigrationError(f"duplicate supplemental review edge: {edge}")
        supplemental_by_edge[edge] = row
    covered_specs = {row["spec_clause_id"] for row in accepted}
    expected_orphans = active_specs - covered_specs
    supplemental_specs = {spec_id for _, spec_id in supplemental_by_edge}
    inactive_specs = supplemental_specs - active_specs
    if inactive_specs:
        raise MigrationError(
            f"supplemental review references inactive specs: first={min(inactive_specs)}"
        )
    if not expected_orphans <= supplemental_specs:
        missing = sorted(expected_orphans - supplemental_specs)
        raise MigrationError(
            f"supplemental review does not cover all orphan specs: missing={len(missing)}"
        )
    accepted_edges = {
        (row["requirement_leaf_id"], row["spec_clause_id"]) for row in accepted
    }
    duplicated_edges = accepted_edges & set(supplemental_by_edge)
    if duplicated_edges:
        raise MigrationError(
            f"supplemental review duplicates an accepted edge: {min(duplicated_edges)}"
        )
    maximum_link_number = max(
        int(row["coverage_link_id"].rsplit("-", 1)[1]) for row in suggestions
    )
    links: list[dict[str, Any]] = []
    for offset, (requirement_id, spec_id) in enumerate(
        sorted(
            supplemental_by_edge,
            key=lambda edge: (
                int(edge[1].rsplit("-", 1)[1]),
                int(edge[0].rsplit("-", 1)[1]),
            ),
        ),
        start=1,
    ):
        row = supplemental_by_edge[(requirement_id, spec_id)]
        task_ids = row.get("task_ids")
        owner = row.get("owner")
        relationship = row.get("relationship")
        note = row.get("review_note")
        if requirement_id not in active_requirements:
            raise MigrationError(
                f"supplemental review references inactive requirement: {requirement_id}"
            )
        if not isinstance(task_ids, list) or not task_ids:
            raise MigrationError(f"supplemental review lacks exact tasks: {spec_id}")
        unknown_tasks = sorted(set(task_ids) - set(tasks), key=_task_sort_key)
        if unknown_tasks:
            raise MigrationError(
                f"supplemental review references unknown tasks: {unknown_tasks}"
            )
        if not owner or relationship not in {"implements", "verifies", "constrains"}:
            raise MigrationError(f"invalid supplemental binding for {spec_id}")
        if not note:
            raise MigrationError(f"supplemental review note is required for {spec_id}")
        links.append(
            {
                "coverage_link_id": (
                    f"AAM-V1-LNK-{maximum_link_number + offset:04d}"
                ),
                "status": "active",
                "aliases": [],
                "requirement_leaf_id": requirement_id,
                "spec_clause_id": spec_id,
                "task_ids": sorted(set(task_ids), key=_task_sort_key),
                "owner": owner,
                "relationship": relationship,
                "rationale": (
                    f"supplemental_orphan_review; reviewer={row['_reviewer']}; {note}"
                ),
                "retired_reason": None,
            }
        )
    return links


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--suggestions", type=Path, required=True)
    parser.add_argument("--review", type=Path, action="append", required=True)
    parser.add_argument(
        "--multi-shall-review", type=Path, action="append", required=True
    )
    parser.add_argument("--supplemental-review", type=Path, action="append")
    parser.add_argument("--previous-v2", type=Path)
    parser.add_argument("--initial-bootstrap", action="store_true")
    parser.add_argument("--spec-split-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or args.report.exists():
        raise MigrationError("merge output already exists; refusing to overwrite")
    if bool(args.previous_v2) == bool(args.initial_bootstrap):
        raise MigrationError(
            "select exactly one of --initial-bootstrap or --previous-v2"
        )
    if args.spec_split_manifest and not args.previous_v2:
        raise MigrationError("--spec-split-manifest requires --previous-v2")
    merged, report = merge_reviews(
        _load_json(args.candidate),
        _load_json(args.suggestions),
        [_load_json(path) for path in args.review],
        [_load_json(path) for path in args.multi_shall_review],
        [_load_json(path) for path in (args.supplemental_review or [])],
        args.previous_v2,
        args.spec_split_manifest,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(merged, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    args.report.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
