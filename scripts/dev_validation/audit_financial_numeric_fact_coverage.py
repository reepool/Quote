#!/usr/bin/env python
"""Audit financial numeric fact field coverage in a bounded local database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_fact_aliases import (  # noqa: E402
    get_standard_financial_fact_names,
)
from scripts.research_cli_support import json_ready  # noqa: E402
from scripts.research_financial_statements_rollout_validation import (  # noqa: E402
    normalize_report_periods,
)


DEFAULT_REQUIRED_CANONICAL_FACTS = [
    "revenue",
    "net_income_parent",
    "total_assets",
    "total_liabilities",
    "equity_parent",
    "operating_cf",
]


SEMANTIC_SUBSTITUTES_BY_REQUIRED = {
    "equity_parent": ["equity_total"],
    "net_income_parent": ["net_income_total"],
}


DERIVATION_COMPONENTS_BY_REQUIRED = {
    "equity_parent": [
        {
            "method": "equity_total_minus_minority_equity",
            "components": ["equity_total", "minority_equity"],
        }
    ],
    "net_income_parent": [
        {
            "method": "net_income_total_minus_minority_interest_income",
            "components": ["net_income_total", "minority_interest_income"],
        }
    ],
}


ALIAS_HINTS_BY_REQUIRED = {
    "revenue": [
        ("营业", "收入"),
        ("operating", "revenue"),
        ("total", "operating", "revenue"),
    ],
    "net_income_parent": [
        ("母公司", "净利润"),
        ("归母", "净利润"),
        ("parent", "net", "profit"),
        ("parent", "net", "income"),
    ],
    "total_assets": [("资产", "总"), ("total", "assets")],
    "total_liabilities": [("负债", "总"), ("total", "liabilities")],
    "equity_parent": [
        ("母公司", "权益"),
        ("归母", "权益"),
        ("parent", "equity"),
    ],
    "operating_cf": [
        ("经营", "现金流", "净额"),
        ("operating", "cash"),
    ],
}


def parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def audit_financial_numeric_fact_coverage(
    *,
    db_path: Path,
    instrument_ids: List[str],
    report_periods: List[str],
    required_canonical_facts: Optional[List[str]] = None,
    include_history: bool = False,
) -> Dict[str, Any]:
    """Return field-coverage diagnostics for selected instrument-periods."""
    if not db_path.exists():
        raise FileNotFoundError(f"financial DB does not exist: {db_path}")
    if not instrument_ids:
        raise ValueError("At least one instrument id is required")
    if not report_periods:
        raise ValueError("At least one report period is required")

    normalized_periods = normalize_report_periods(report_periods)
    required = required_canonical_facts or list(DEFAULT_REQUIRED_CANONICAL_FACTS)
    rows = _load_numeric_rows(
        db_path,
        instrument_ids=instrument_ids,
        report_periods=normalized_periods,
        include_history=include_history,
    )
    warnings_by_key = _load_core_fact_warnings(
        db_path,
        instrument_ids=instrument_ids,
        report_periods=normalized_periods,
        include_history=include_history,
    )

    expected_keys = [
        f"{instrument_id}|{report_period}"
        for instrument_id in instrument_ids
        for report_period in normalized_periods
    ]
    rows_by_key: Dict[str, List[sqlite3.Row]] = {key: [] for key in expected_keys}
    for row in rows:
        key = f"{row['instrument_id']}|{row['report_period']}"
        rows_by_key.setdefault(key, []).append(row)

    instrument_periods = {
        key: _audit_instrument_period(
            rows_by_key.get(key, []),
            required_canonical_facts=required,
            semantic_warnings=warnings_by_key.get(key, []),
        )
        for key in expected_keys
    }
    missing_rows = [
        key for key, item in instrument_periods.items() if item["numeric_fact_count"] == 0
    ]
    total_rows = sum(item["numeric_fact_count"] for item in instrument_periods.values())
    total_unmapped = sum(
        item["unmapped_field_count"] for item in instrument_periods.values()
    )
    total_missing_required = sum(
        len(item["missing_required_canonical_facts"])
        for item in instrument_periods.values()
    )
    total_unit_conflicts = sum(
        len(item["canonical_unit_conflicts"]) for item in instrument_periods.values()
    )
    total_semantic_warnings = sum(
        len(item["semantic_warnings"]) for item in instrument_periods.values()
    )
    gap_reason_counts = _count_gap_reasons(instrument_periods.values())

    return {
        "status": "passed"
        if not missing_rows and not total_missing_required and not total_unit_conflicts
        else "needs_review",
        "db_path": str(db_path),
        "instrument_ids": instrument_ids,
        "report_periods": normalized_periods,
        "required_canonical_facts": required,
        "catalog_version": "standard_financial_numeric_facts.v1",
        "known_canonical_fact_count": len(get_standard_financial_fact_names()),
        "include_history": include_history,
        "summary": {
            "instrument_period_count": len(expected_keys),
            "numeric_fact_count": total_rows,
            "missing_numeric_fact_rows": missing_rows,
            "unmapped_field_count": total_unmapped,
            "missing_required_canonical_fact_count": total_missing_required,
            "canonical_unit_conflict_count": total_unit_conflicts,
            "semantic_warning_count": total_semantic_warnings,
            "gap_reason_counts": gap_reason_counts,
        },
        "instrument_periods": instrument_periods,
    }


def _load_numeric_rows(
    db_path: Path,
    *,
    instrument_ids: List[str],
    report_periods: List[str],
    include_history: bool,
) -> List[sqlite3.Row]:
    tables = ["financial_numeric_facts_hot"]
    if include_history:
        tables.append("financial_numeric_facts_history")
    select_sql = " UNION ALL ".join(
        f"SELECT *, '{table}' AS physical_table FROM {table}" for table in tables
    )
    instrument_placeholders = ",".join("?" for _ in instrument_ids)
    period_placeholders = ",".join("?" for _ in report_periods)
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            f"""
            SELECT *
            FROM ({select_sql})
            WHERE instrument_id IN ({instrument_placeholders})
              AND report_period IN ({period_placeholders})
            ORDER BY instrument_id, report_period, statement_family, fact_name
            """,
            [*instrument_ids, *report_periods],
        ).fetchall()


def _load_core_fact_warnings(
    db_path: Path,
    *,
    instrument_ids: List[str],
    report_periods: List[str],
    include_history: bool,
) -> Dict[str, List[Dict[str, Any]]]:
    tables = ["financial_core_facts_hot"]
    if include_history:
        tables.append("financial_core_facts_history")
    select_sql = " UNION ALL ".join(
        f"SELECT instrument_id, report_period, lineage_json FROM {table}"
        for table in tables
    )
    instrument_placeholders = ",".join("?" for _ in instrument_ids)
    period_placeholders = ",".join("?" for _ in report_periods)
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT *
                FROM ({select_sql})
                WHERE instrument_id IN ({instrument_placeholders})
                  AND report_period IN ({period_placeholders})
                """,
                [*instrument_ids, *report_periods],
            ).fetchall()
    except sqlite3.Error:
        return {}
    warnings: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = f"{row['instrument_id']}|{row['report_period']}"
        lineage = _loads_json(row["lineage_json"])
        warnings.setdefault(key, []).extend(lineage.get("core_fact_warnings") or [])
    return warnings


def _audit_instrument_period(
    rows: List[sqlite3.Row],
    *,
    required_canonical_facts: List[str],
    semantic_warnings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    canonical_fields = sorted(
        {
            str(row["canonical_fact_name"])
            for row in rows
            if row["canonical_fact_name"]
        }
    )
    source_fields = sorted({str(row["fact_name"]) for row in rows if row["fact_name"]})
    source_distribution = _count_values(str(row["source"]) for row in rows)
    source_mode_distribution = _count_values(str(row["source_mode"]) for row in rows)
    statement_family_distribution = _count_values(
        str(row["statement_family"] or "unknown") for row in rows
    )
    unmapped_fields = sorted(
        {
            str(row["fact_name"])
            for row in rows
            if row["fact_name"] and not row["canonical_fact_name"]
        }
    )
    missing_required = [
        fact for fact in required_canonical_facts if fact not in set(canonical_fields)
    ]
    unit_conflicts = _canonical_unit_conflicts(rows)
    gap_reasons, required_fact_gaps = _classify_gap_reasons(
        numeric_fact_count=len(rows),
        canonical_fields=canonical_fields,
        unmapped_fields=unmapped_fields,
        missing_required=missing_required,
        unit_conflicts=unit_conflicts,
        semantic_warnings=semantic_warnings,
    )
    return {
        "numeric_fact_count": len(rows),
        "source_field_count": len(source_fields),
        "canonical_field_count": len(canonical_fields),
        "source_distribution": source_distribution,
        "source_mode_distribution": source_mode_distribution,
        "statement_family_distribution": statement_family_distribution,
        "canonical_fields": canonical_fields,
        "missing_required_canonical_facts": missing_required,
        "required_fact_gaps": required_fact_gaps,
        "unmapped_field_count": len(unmapped_fields),
        "unmapped_fields": unmapped_fields[:100],
        "truncated_unmapped_field_count": max(0, len(unmapped_fields) - 100),
        "canonical_unit_conflicts": unit_conflicts,
        "semantic_warnings": semantic_warnings[:100],
        "truncated_semantic_warning_count": max(0, len(semantic_warnings) - 100),
        "gap_reasons": gap_reasons,
        "sample_source_fields": source_fields[:30],
    }


def _classify_gap_reasons(
    *,
    numeric_fact_count: int,
    canonical_fields: List[str],
    unmapped_fields: List[str],
    missing_required: List[str],
    unit_conflicts: List[Dict[str, Any]],
    semantic_warnings: List[Dict[str, Any]],
) -> tuple[List[str], List[Dict[str, Any]]]:
    canonical_set = set(canonical_fields)
    reasons: set[str] = set()
    required_fact_gaps: List[Dict[str, Any]] = []
    if numeric_fact_count == 0:
        reasons.add("missing_numeric_rows")
    if unit_conflicts:
        reasons.add("canonical_unit_conflict")

    for fact in missing_required:
        fact_reasons = {"missing_required_canonical_fact"}
        gap: Dict[str, Any] = {
            "canonical_fact_name": fact,
            "reasons": [],
        }
        substitutes = [
            item
            for item in SEMANTIC_SUBSTITUTES_BY_REQUIRED.get(fact, [])
            if item in canonical_set
        ]
        if substitutes or _semantic_warning_matches_required(
            fact,
            semantic_warnings,
        ):
            fact_reasons.add("semantic_gap")
            gap["present_semantic_substitutes"] = substitutes

        derivation_gaps = _derivation_component_gaps(
            fact,
            canonical_set=canonical_set,
        )
        if derivation_gaps:
            fact_reasons.add("derivation_component_gap")
            gap["derivation_component_gaps"] = derivation_gaps

        alias_candidates = _alias_gap_candidates(fact, unmapped_fields)
        if alias_candidates:
            fact_reasons.add("alias_gap_candidate")
            gap["alias_candidates"] = alias_candidates

        gap["reasons"] = sorted(fact_reasons)
        reasons.update(fact_reasons)
        required_fact_gaps.append(gap)

    if unmapped_fields and "alias_gap_candidate" not in reasons:
        reasons.add("unmapped_nonrequired_fields")
    return sorted(reasons), required_fact_gaps


def _semantic_warning_matches_required(
    fact: str,
    semantic_warnings: List[Dict[str, Any]],
) -> bool:
    expected = {
        "equity_parent": {"equity_total_vs_parent_ambiguous"},
        "net_income_parent": {"net_income_total_vs_parent_ambiguous"},
    }.get(fact, set())
    if not expected:
        return False
    return any(str(item.get("warning") or "") in expected for item in semantic_warnings)


def _derivation_component_gaps(
    fact: str,
    *,
    canonical_set: set[str],
) -> List[Dict[str, Any]]:
    gaps: List[Dict[str, Any]] = []
    for rule in DERIVATION_COMPONENTS_BY_REQUIRED.get(fact, []):
        components = [str(item) for item in rule.get("components", [])]
        present = [item for item in components if item in canonical_set]
        missing = [item for item in components if item not in canonical_set]
        if present and missing:
            gaps.append(
                {
                    "method": str(rule.get("method") or ""),
                    "present_components": present,
                    "missing_components": missing,
                }
            )
    return gaps


def _alias_gap_candidates(
    fact: str,
    unmapped_fields: List[str],
) -> List[str]:
    hints = ALIAS_HINTS_BY_REQUIRED.get(fact, [])
    candidates: List[str] = []
    for field in unmapped_fields:
        normalized = _normalize_alias_hint(field)
        if any(all(part in normalized for part in pattern) for pattern in hints):
            candidates.append(field)
    return candidates[:20]


def _normalize_alias_hint(value: str) -> str:
    return (
        str(value)
        .strip()
        .lower()
        .replace("（", "(")
        .replace("）", ")")
        .replace("_", "")
        .replace("-", "")
        .replace(" ", "")
    )


def _count_gap_reasons(items: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        for reason in item.get("gap_reasons", []):
            counts[reason] = counts.get(reason, 0) + 1
    return counts


def _canonical_unit_conflicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    units_by_fact: Dict[str, set[str]] = {}
    for row in rows:
        canonical_name = row["canonical_fact_name"]
        if not canonical_name:
            continue
        unit = str(row["canonical_unit"] or row["unit"] or "")
        units_by_fact.setdefault(str(canonical_name), set()).add(unit)
    return [
        {"canonical_fact_name": name, "units": sorted(units)}
        for name, units in sorted(units_by_fact.items())
        if len(units) > 1
    ]


def _loads_json(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _count_values(values: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit stored financial numeric fact field coverage.",
    )
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--instrument-ids", required=True)
    parser.add_argument("--report-periods", required=True)
    parser.add_argument(
        "--required-canonical-facts",
        help="Comma-separated canonical facts required for the audit.",
    )
    parser.add_argument("--include-history", action="store_true")
    parser.add_argument("--output-path", type=Path)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result = audit_financial_numeric_fact_coverage(
        db_path=args.db_path,
        instrument_ids=parse_csv(args.instrument_ids),
        report_periods=parse_csv(args.report_periods),
        required_canonical_facts=parse_csv(args.required_canonical_facts) or None,
        include_history=args.include_history,
    )
    payload = json_ready(result)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        with args.output_path.open("w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2, sort_keys=True)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
