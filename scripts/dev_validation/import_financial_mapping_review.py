#!/usr/bin/env python
"""Validate manually reviewed financial mapping CSV rows.

This script intentionally produces a draft artifact only. It does not update
the in-code mapping catalog or production storage, because ambiguous financial
statement fields must be reviewed and versioned explicitly.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_source_field_mapping import (  # noqa: E402
    APPROVED_RELATIONSHIPS,
    FinancialSourceFieldMapping,
    RELATIONSHIP_EXACT_EQUIVALENT,
)
from scripts.research_cli_support import json_ready  # noqa: E402


DECISION_APPROVE_CORE = "approve_core"
DECISION_REJECT = "reject"
DECISION_NEEDS_MORE_EVIDENCE = "needs_more_evidence"
KNOWN_DECISIONS = {
    DECISION_APPROVE_CORE,
    DECISION_REJECT,
    DECISION_NEEDS_MORE_EVIDENCE,
    "",
}

STATEMENT_FAMILY_BY_STATEMENT_TYPE = {
    "balance_sheet": "balance_sheet",
    "profit_sheet": "income_statement",
    "income_statement": "income_statement",
    "cash_flow_sheet": "cash_flow",
    "cash_flow": "cash_flow",
}

DEFAULT_VALUE_TYPE_BY_STATEMENT_FAMILY = {
    "balance_sheet": "point_in_time",
    "income_statement": "period_reported_value",
    "cash_flow": "period_reported_value",
}


def import_financial_mapping_review(
    rows: Iterable[Dict[str, Any]],
    *,
    mapping_version: str,
) -> Dict[str, Any]:
    """Validate reviewed CSV rows and return approved draft mappings."""
    approved_mappings: List[Dict[str, Any]] = []
    review_outcomes: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    counters = {
        "approved_count": 0,
        "rejected_count": 0,
        "needs_more_evidence_count": 0,
        "ignored_count": 0,
    }

    for row_number, row in enumerate(rows, start=2):
        normalized = {str(key): _clean(value) for key, value in row.items()}
        decision = normalized.get("review_decision", "")
        if decision not in KNOWN_DECISIONS:
            row_errors = [f"unknown review_decision: {decision}"]
        else:
            row_errors = []

        outcome = {
            "row_number": row_number,
            "decision": decision or "ignored",
            "profile": normalized.get("profile"),
            "statement_type": normalized.get("statement_type"),
            "candidate": normalized.get("standard_field_key_candidate"),
            "status": "ignored",
            "errors": row_errors,
        }

        if decision == "":
            counters["ignored_count"] += 1
        elif decision == DECISION_REJECT:
            counters["rejected_count"] += 1
            outcome["status"] = "rejected"
        elif decision == DECISION_NEEDS_MORE_EVIDENCE:
            counters["needs_more_evidence_count"] += 1
            outcome["status"] = "needs_more_evidence"
        elif decision == DECISION_APPROVE_CORE:
            mapping, approval_errors = _build_approved_mapping(
                normalized,
                mapping_version=mapping_version,
            )
            row_errors.extend(approval_errors)
            if mapping is not None and not row_errors:
                approved_mappings.append(mapping.to_dict())
                counters["approved_count"] += 1
                outcome["status"] = "approved"
                outcome["mapping_key"] = {
                    "mapping_version": mapping.mapping_version,
                    "profile": mapping.profile,
                    "canonical_fact": mapping.canonical_fact,
                    "sina_field": mapping.sina_field,
                    "ths_metric": mapping.ths_metric,
                }
            else:
                outcome["status"] = "invalid"

        if row_errors:
            error_entry = {
                "row_number": row_number,
                "candidate": normalized.get("standard_field_key_candidate"),
                "errors": row_errors,
            }
            errors.append(error_entry)
            outcome["errors"] = row_errors
        review_outcomes.append(outcome)

    return {
        "summary": {
            "mapping_version": mapping_version,
            "row_count": len(review_outcomes),
            **counters,
            "error_count": len(errors),
        },
        "approved_mappings": approved_mappings,
        "review_outcomes": review_outcomes,
        "errors": errors,
    }


def _build_approved_mapping(
    row: Dict[str, str],
    *,
    mapping_version: str,
) -> tuple[Optional[FinancialSourceFieldMapping], List[str]]:
    errors: List[str] = []
    statement_family = STATEMENT_FAMILY_BY_STATEMENT_TYPE.get(row.get("statement_type", ""))
    if not statement_family:
        errors.append("statement_type is not supported")

    relationship = row.get("relationship") or RELATIONSHIP_EXACT_EQUIVALENT
    if relationship not in APPROVED_RELATIONSHIPS:
        errors.append("relationship must be exact_equivalent or equivalent_after_unit")

    canonical_fact = row.get("approved_local_field") or row.get("standard_field_key_candidate")
    semantic = row.get("approved_semantic")
    canonical_unit = row.get("approved_canonical_unit")
    source_unit = row.get("approved_source_unit") or canonical_unit
    sina_field = _approved_or_single(row, approved_key="approved_sina_field", source_key="sina_fields")
    ths_metric = _approved_or_single(row, approved_key="approved_ths_metric", source_key="ths_fields")
    unit_multiplier = _parse_float(row.get("unit_multiplier") or "1")

    required = {
        "profile": row.get("profile"),
        "approved_local_field": canonical_fact,
        "approved_semantic": semantic,
        "approved_canonical_unit": canonical_unit,
        "approved_source_unit": source_unit,
        "approved_sina_field": sina_field,
        "approved_ths_metric": ths_metric,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        errors.append("missing required approval fields: " + ",".join(missing))
    if unit_multiplier is None or unit_multiplier <= 0:
        errors.append("unit_multiplier must be a positive number")
    if row.get("unit_review_status") != "known_units_match" and not canonical_unit:
        errors.append("unit review is unresolved; approved_canonical_unit is required")
    if relationship == RELATIONSHIP_EXACT_EQUIVALENT and unit_multiplier not in {None, 1.0}:
        errors.append("exact_equivalent requires unit_multiplier=1")

    if errors or not statement_family or unit_multiplier is None:
        return None, errors

    return (
        FinancialSourceFieldMapping(
            canonical_fact=str(canonical_fact),
            statement_family=statement_family,
            profile=str(row["profile"]).strip().lower(),
            sina_field=str(sina_field),
            ths_metric=str(ths_metric),
            relationship=relationship,
            source_unit=str(source_unit),
            canonical_unit=str(canonical_unit),
            unit_multiplier=unit_multiplier,
            value_type=DEFAULT_VALUE_TYPE_BY_STATEMENT_FAMILY[statement_family],
            approved_for_core=True,
            mapping_version=mapping_version,
            semantic=str(semantic),
            evidence=(str(row.get("review_notes") or ""),),
        ),
        [],
    )


def _approved_or_single(row: Dict[str, str], *, approved_key: str, source_key: str) -> str:
    approved = row.get(approved_key)
    if approved:
        return approved
    values = [part.strip() for part in row.get(source_key, "").split("|") if part.strip()]
    return values[0] if len(values) == 1 else ""


def _parse_float(raw: str) -> Optional[float]:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def read_csv_rows(input_path: Path) -> List[Dict[str, Any]]:
    with input_path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate reviewed financial mapping CSV and export approved draft mappings."
    )
    parser.add_argument("--review-csv", required=True, help="Reviewed CSV path")
    parser.add_argument(
        "--mapping-version",
        required=True,
        help="Draft mapping version to assign to approved rows",
    )
    parser.add_argument("--output-json", help="Optional validation/draft JSON output path")
    parser.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit non-zero if any reviewed row is invalid.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    result = import_financial_mapping_review(
        read_csv_rows(Path(args.review_csv)),
        mapping_version=args.mapping_version,
    )
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    else:
        print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    if args.fail_on_errors and result["summary"]["error_count"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
