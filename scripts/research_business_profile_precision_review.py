#!/usr/bin/env python3
"""Export or evaluate official-report product-label review packages."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_precision_review import (
    audit_product_label_review_readiness,
    build_product_alias_official_evidence_from_review,
    build_product_catalog_issue_review_package,
    build_product_label_review_package,
    evaluate_product_label_review,
)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    subparsers = parser.add_subparsers(dest="command", required=True)

    export = subparsers.add_parser("export")
    export.add_argument("--research-db", type=Path, required=True)
    export.add_argument("--financials-db", type=Path, required=True)
    export.add_argument("--instrument-id", action="append")
    export.add_argument("--report-period")
    export.add_argument("--minimum-revenue-share", type=float, default=0.01)

    catalog_issues = subparsers.add_parser("export-catalog-issues")
    catalog_issues.add_argument("--research-db", type=Path, required=True)
    catalog_issues.add_argument("--financials-db", type=Path, required=True)
    catalog_issues.add_argument("--instrument-id", action="append")
    catalog_issues.add_argument("--report-period")
    catalog_issues.add_argument("--minimum-revenue-share", type=float, default=0.01)
    catalog_issues.add_argument(
        "--archive-path-base",
        type=Path,
        default=ROOT_DIR,
    )

    promotion_evidence = subparsers.add_parser("prepare-promotion-evidence")
    promotion_evidence.add_argument("--review-package", type=Path, required=True)
    promotion_evidence.add_argument("--review-id", required=True)

    audit = subparsers.add_parser("audit")
    audit.add_argument("--research-db", type=Path, required=True)
    audit.add_argument("--financials-db", type=Path, required=True)
    audit.add_argument("--report-period")
    audit.add_argument("--minimum-revenue-share", type=float, default=0.01)
    audit.add_argument(
        "--minimum-precision-lower-bound",
        type=float,
        default=0.99,
    )
    audit.add_argument(
        "--expected-industry-group",
        action="append",
        help="Required industry group; defaults to all first-wave industries",
    )

    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("--review-package", type=Path, required=True)
    evaluate.add_argument(
        "--minimum-precision-lower-bound",
        type=float,
        default=0.99,
    )
    evaluate.add_argument("--maximum-exclusion-rate", type=float, default=0.05)

    args = parser.parse_args(argv)
    if args.command == "export":
        payload = build_product_label_review_package(
            research_db=args.research_db,
            financials_db=args.financials_db,
            instrument_ids=args.instrument_id,
            report_period=args.report_period,
            minimum_revenue_share=args.minimum_revenue_share,
        )
    elif args.command == "export-catalog-issues":
        payload = build_product_catalog_issue_review_package(
            research_db=args.research_db,
            financials_db=args.financials_db,
            instrument_ids=args.instrument_id,
            report_period=args.report_period,
            minimum_revenue_share=args.minimum_revenue_share,
            archive_path_base=args.archive_path_base,
        )
    elif args.command == "audit":
        payload = audit_product_label_review_readiness(
            research_db=args.research_db,
            financials_db=args.financials_db,
            report_period=args.report_period,
            minimum_revenue_share=args.minimum_revenue_share,
            minimum_precision_lower_bound=args.minimum_precision_lower_bound,
            expected_industry_groups=args.expected_industry_group,
        )
    elif args.command == "evaluate":
        package = json.loads(args.review_package.read_text(encoding="utf-8"))
        payload = evaluate_product_label_review(
            package,
            minimum_precision_lower_bound=args.minimum_precision_lower_bound,
            maximum_exclusion_rate=args.maximum_exclusion_rate,
        )
    else:
        package = json.loads(args.review_package.read_text(encoding="utf-8"))
        payload = build_product_alias_official_evidence_from_review(
            package,
            review_id=args.review_id,
        )
    _write_payload(payload, args.output)
    if args.command == "prepare-promotion-evidence":
        return 0
    return 0 if payload["status"] in {"pass", "ready_for_human_review"} else 3


def _write_payload(payload: Mapping[str, Any], output: Optional[Path]) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if output is None:
        print(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(f"{rendered}\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
