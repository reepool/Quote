#!/usr/bin/env python3
"""Audit product labels or create a controlled exact-alias catalog promotion."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_catalog_governance import (
    audit_product_label_resolutions,
    write_product_alias_promotion,
)
from research.business_profile_product_catalog import DEFAULT_PRODUCT_CATALOG_PATH


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit = subparsers.add_parser(
        "audit",
        help="summarize latest unmatched and ambiguous candidate product labels",
    )
    source = audit.add_mutually_exclusive_group()
    source.add_argument("--segments-json", type=Path)
    source.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    audit.add_argument("--record-limit", type=int, default=100000)
    audit.add_argument("--sample-limit", type=int, default=5)
    audit.add_argument(
        "--minimum-material-revenue-share",
        type=float,
        default=0.01,
    )
    audit.add_argument("--output", type=Path)

    promote = subparsers.add_parser(
        "promote-alias",
        help="write a new catalog version containing one reviewed exact alias",
    )
    promote.add_argument(
        "--source-catalog",
        type=Path,
        default=DEFAULT_PRODUCT_CATALOG_PATH,
    )
    promote.add_argument("--output-catalog", type=Path, required=True)
    promote.add_argument("--manifest-output", type=Path, required=True)
    promote.add_argument("--expected-version", required=True)
    promote.add_argument("--new-version", required=True)
    promote.add_argument("--released-on", required=True)
    promote.add_argument("--alias", required=True)
    promote.add_argument("--alias-id")
    promote.add_argument("--product-id", action="append", required=True)
    promote.add_argument("--industry-group", action="append", required=True)
    promote.add_argument("--review-policy")
    promote.add_argument("--operator", required=True)
    promote.add_argument("--reason", required=True)
    promote.add_argument(
        "--announcement-assets-db",
        type=Path,
        default=ROOT_DIR / "data" / "research.db",
    )
    promote.add_argument(
        "--archive-path-base",
        type=Path,
        default=ROOT_DIR,
        help="base directory used to resolve relative archive_path manifest values",
    )
    promote.add_argument("--official-evidence", type=Path, required=True)

    args = parser.parse_args(argv)
    if args.command == "audit":
        payload = _run_audit(args)
    else:
        payload = write_product_alias_promotion(
            source_path=args.source_catalog,
            output_path=args.output_catalog,
            manifest_path=args.manifest_output,
            financials_db=args.announcement_assets_db,
            official_evidence_path=args.official_evidence,
            archive_path_base=args.archive_path_base,
            expected_catalog_version=args.expected_version,
            new_catalog_version=args.new_version,
            released_on=args.released_on,
            alias=args.alias,
            alias_id=args.alias_id,
            product_ids=args.product_id,
            industry_groups=args.industry_group,
            review_policy=args.review_policy,
            operator=args.operator,
            reason=args.reason,
        )
        payload = {
            "status": "written",
            "output_catalog": str(args.output_catalog),
            "manifest_output": str(args.manifest_output),
            "promotion": payload,
        }
    _write_payload(payload, args.output if args.command == "audit" else None)
    return 3 if payload.get("status") == "incomplete" else 0


def _run_audit(args: argparse.Namespace) -> dict[str, Any]:
    if args.record_limit < 1:
        raise ValueError("record-limit must be positive")
    if args.segments_json is not None:
        segments = _load_segment_json(args.segments_json)
        load_diagnostics = {
            "input_total_candidate_product_rows": len(segments),
            "input_loaded_rows": len(segments),
            "input_truncated": False,
        }
        source = str(args.segments_json)
    else:
        segments, load_diagnostics = _load_candidate_segments(
            args.research_db,
            args.record_limit,
        )
        source = str(args.research_db)
    audit = audit_product_label_resolutions(
        segments,
        sample_limit=args.sample_limit,
        minimum_material_revenue_share=args.minimum_material_revenue_share,
    )
    if load_diagnostics["input_truncated"]:
        audit["status"] = "incomplete"
    return {
        "source": source,
        **load_diagnostics,
        **audit,
    }


def _load_candidate_segments(
    path: Path,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        ranked_cte = """
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY
                               CASE
                                   WHEN COALESCE(
                                       json_extract(
                                           metadata_json,
                                           '$.source_name'
                                       ),
                                       ''
                                   ) <> ''
                                    AND COALESCE(
                                       json_extract(
                                           metadata_json,
                                           '$.source_row_key'
                                       ),
                                       ''
                                   ) <> ''
                                   THEN json_extract(
                                       metadata_json,
                                       '$.source_name'
                                   ) || '|' || json_extract(
                                       metadata_json,
                                       '$.source_row_key'
                                   )
                                   ELSE 'unkeyed|' || record_id
                               END
                           ORDER BY version DESC, updated_at DESC, record_id DESC
                       ) AS source_row_rank
                FROM company_business_segments
                WHERE segment_type = 'product'
            )
        """
        total = int(
            conn.execute(
                f"""
                {ranked_cte}
                SELECT COUNT(*)
                FROM ranked
                WHERE source_row_rank = 1
                  AND review_status = 'candidate'
                """
            ).fetchone()[0]
        )
        rows = conn.execute(
            f"""
            {ranked_cte}
            SELECT *
            FROM ranked
            WHERE source_row_rank = 1
              AND review_status = 'candidate'
            ORDER BY updated_at DESC, record_id DESC
            LIMIT ?
            """,
            (limit + 1,),
        ).fetchall()
    loaded = [dict(row) for row in rows[:limit]]
    return loaded, {
        "input_total_candidate_product_rows": total,
        "input_loaded_rows": len(loaded),
        "input_truncated": total > limit,
    }


def _load_segment_json(path: Path) -> list[Mapping[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, Mapping):
        payload = payload.get("segments")
    if not isinstance(payload, list) or not all(
        isinstance(item, Mapping) for item in payload
    ):
        raise ValueError("segments JSON must be an array or a segments object")
    return payload


def _write_payload(payload: Mapping[str, Any], output: Optional[Path]) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if output is None:
        print(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(f"{rendered}\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
