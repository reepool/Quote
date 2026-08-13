"""Build a hash-bound manifest for reviewed atomic OpenSpec clause splits."""

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
    parse_spec_clauses,
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def build_manifest(
    previous: dict[str, Any],
    *,
    previous_bytes: bytes,
    reviews: list[dict[str, Any]],
    split_maps: list[list[dict[str, Any]]],
) -> dict[str, Any]:
    previous_by_id = {
        str(node["spec_clause_id"]): node for node in previous["spec_clauses"]
    }
    required_ids: set[str] = set()
    for review in reviews:
        for row in review.get("rows", []):
            if row.get("disposition") == "must_split":
                required_ids.add(str(row["spec_clause_id"]))
    entries = [entry for rows in split_maps for entry in rows]
    by_old_id: dict[str, dict[str, Any]] = {}
    for entry in entries:
        old_id = str(entry.get("old_spec_clause_id", ""))
        if not old_id or old_id in by_old_id:
            raise MigrationError(f"duplicate or missing split-map id: {old_id}")
        by_old_id[old_id] = entry
    if set(by_old_id) != required_ids:
        missing = sorted(required_ids - set(by_old_id))
        extra = sorted(set(by_old_id) - required_ids)
        raise MigrationError(
            f"split maps do not exactly cover reviewed must-split ids: "
            f"missing={len(missing)}, extra={len(extra)}"
        )

    current_by_identity = {
        (clause.spec_path, clause.text_sha256): clause
        for clause in parse_spec_clauses()
    }
    seen_targets: set[tuple[str, str]] = set()
    normalized_entries: list[dict[str, Any]] = []
    for old_id in sorted(by_old_id, key=lambda value: int(value.rsplit("-", 1)[1])):
        entry = by_old_id[old_id]
        previous_node = previous_by_id.get(old_id)
        if previous_node is None or previous_node["status"] != "active":
            raise MigrationError(f"split source is not active in previous v2: {old_id}")
        if entry.get("old_text_sha256") != previous_node["text_sha256"]:
            raise MigrationError(f"split source text hash mismatch: {old_id}")
        targets = [
            target
            for target in entry.get("new_clauses", [])
            if "SHALL" in str(target.get("normalized_text", ""))
        ]
        if len(targets) < 2:
            raise MigrationError(f"split must create at least two clauses: {old_id}")
        normalized_targets: list[dict[str, str]] = []
        for target in targets:
            identity = (str(target.get("path", "")), str(target.get("text_sha256", "")))
            if not all(identity) or identity in seen_targets:
                raise MigrationError(f"duplicate or incomplete split target: {identity}")
            clause = current_by_identity.get(identity)
            if clause is None or clause.normalized_text != target.get("normalized_text"):
                raise MigrationError(f"split target is not current source: {old_id}:{identity}")
            if clause.shall_occurrences != 1:
                raise MigrationError(
                    f"split target is not one independently addressable SHALL: {identity}"
                )
            seen_targets.add(identity)
            normalized_targets.append(
                {
                    "path": identity[0],
                    "normalized_text": clause.normalized_text,
                    "text_sha256": clause.text_sha256,
                }
            )
        normalized_entries.append(
            {
                "old_spec_clause_id": old_id,
                "old_text_sha256": previous_node["text_sha256"],
                "new_clauses": normalized_targets,
            }
        )
    return {
        "schema_version": "announcement_asset_spec_split_manifest.v1",
        "previous_registry_sha256": _sha256_bytes(previous_bytes),
        "entries": normalized_entries,
    }


def _split_map_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        rows = value
    elif isinstance(value, dict) and isinstance(value.get("rows"), list):
        rows = value["rows"]
    else:
        raise MigrationError("split map must be an array or an object with rows")
    if not all(isinstance(row, dict) for row in rows):
        raise MigrationError("split map rows must be JSON objects")
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-v2", type=Path, required=True)
    parser.add_argument("--review", type=Path, action="append", required=True)
    parser.add_argument("--split-map", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise MigrationError("split manifest output already exists; refusing to overwrite")
    previous_bytes = args.previous_v2.read_bytes()
    manifest = build_manifest(
        json.loads(previous_bytes),
        previous_bytes=previous_bytes,
        reviews=[_load_json(path) for path in args.review],
        split_maps=[
            _split_map_rows(json.loads(path.read_text(encoding="utf-8")))
            for path in args.split_map
        ],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "entries": len(manifest["entries"]),
                "new_clauses": sum(
                    len(entry["new_clauses"]) for entry in manifest["entries"]
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
