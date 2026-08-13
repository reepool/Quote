"""Bind reviewed coupled multi-SHALL dispositions to an exact v2 candidate."""

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
)


def _json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_review(
    candidate: dict[str, Any], reviews: list[dict[str, Any]]
) -> dict[str, Any]:
    pending = {
        str(node["spec_clause_id"]): node
        for node in candidate["spec_clauses"]
        if node["status"] == "active"
        and node["multi_shall_disposition"] == "pending_review"
    }
    source_rows = [row for review in reviews for row in review.get("rows", [])]
    by_id: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        if row.get("disposition") != "compound_single_clause":
            continue
        spec_id = str(row.get("spec_clause_id", ""))
        if spec_id in by_id:
            raise MigrationError(f"duplicate coupled review id: {spec_id}")
        by_id[spec_id] = row
    if set(by_id) != set(pending):
        missing = sorted(set(pending) - set(by_id))
        extra = sorted(set(by_id) - set(pending))
        raise MigrationError(
            f"coupled review does not match current pending clauses: "
            f"missing={len(missing)}, extra={len(extra)}"
        )
    rows: list[dict[str, str]] = []
    for spec_id in sorted(by_id, key=lambda value: int(value.rsplit("-", 1)[1])):
        source = by_id[spec_id]
        node = pending[spec_id]
        note = source.get("review_note")
        if not note:
            raise MigrationError(f"coupled review lacks note: {spec_id}")
        rows.append(
            {
                "spec_clause_id": spec_id,
                "spec_text_sha256": node["text_sha256"],
                "disposition": "compound_single_clause",
                "review_note": str(note),
            }
        )
    return {
        "schema_version": "announcement_asset_multishall_review.v1",
        "candidate_registry_sha256": _json_sha256(candidate),
        "reviewer": "independent-multishall-review-panel",
        "rows": rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--review", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise MigrationError("multi-SHALL review output exists; refusing to overwrite")
    result = build_review(
        _load_json(args.candidate),
        [_load_json(path) for path in args.review],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"rows": len(result["rows"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
