#!/usr/bin/env python
"""Audit bounded Sina/THS financial field mappings against sample payloads."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_fact_aliases import describe_financial_numeric_fact_name  # noqa: E402
from research.financial_source_field_mapping import (  # noqa: E402
    MAPPING_VERSION,
    get_financial_source_field_mappings,
)
from research.providers.akshare_financial_statements import (  # noqa: E402
    AkshareFinancialStatementsProvider,
)
from scripts.research_cli_support import json_ready  # noqa: E402


STATEMENT_TYPE_BY_FAMILY = {
    "balance_sheet": "balance_sheet",
    "income_statement": "profit_sheet",
    "cash_flow": "cash_flow_sheet",
}

REPORT_PERIOD_ALIASES = ("report_period", "report_date", "REPORT_DATE", "报告日", "报告期")

IDENTITY_SPECS = {
    "assets_equals_liabilities_plus_equity_total": {
        "total_assets": [
            "assets_total",
            "TOTAL_ASSETS",
            "资产总计",
            "总资产",
        ],
        "total_liabilities": [
            "total_debt",
            "TOTAL_LIABILITIES",
            "负债合计",
            "总负债",
        ],
        "equity_total": [
            "holder_equity_total",
            "TOTAL_EQUITY",
            "股东权益合计",
            "所有者权益合计",
            "所有者权益",
            "净资产",
        ],
    }
}

CANONICAL_COMPARISON_TOLERANCE_OVERRIDES = {
    # CNInfo data20 publishes official summary rows in 10k-CNY units. After
    # conversion to CNY, small sub-10k rounding differences are expected and
    # must not be confused with a Sina/THS local-core mapping failure.
    "cninfo_data20": {
        "absolute_tolerance": 100.0,
        "relative_tolerance": 1e-5,
    },
}

CANONICAL_COMPARISON_BLOCKING_SOURCES = {"ths_report", "eastmoney_report"}

FINANCIAL_STATEMENT_AMOUNT_TYPES = {
    "balance_sheet",
    "profit_sheet",
    "cash_flow_sheet",
}

PER_SHARE_FIELD_HINTS = (
    "eps",
    "每股收益",
)

MACHINE_REVIEW_APPROVED = "machine_approved_candidate"
MACHINE_REVIEW_REJECTED = "machine_rejected_candidate"
MACHINE_REVIEW_HUMAN = "human_review_required"
MACHINE_REVIEW_MORE_EVIDENCE = "needs_more_evidence"

MANUAL_RESOLUTIONS = {
    ("balance_sheet", "应付账款", "accounts_payable"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.accounts_payable",
        "semantic": "accounts_payable",
        "eastmoney_field": "ACCOUNTS_PAYABLE",
    },
    ("balance_sheet", "应付票据及应付账款", "payable_notes_and_accounts"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.payable_notes_and_accounts",
        "semantic": "notes_and_accounts_payable",
        "eastmoney_field": "NOTE_ACCOUNTS_PAYABLE",
    },
    ("balance_sheet", "资产总计", "assets_total"): {
        "decision": "approve_core",
        "local_field": "total_assets",
        "semantic": "total_assets",
        "eastmoney_field": "TOTAL_ASSETS",
    },
    ("balance_sheet", "负债和所有者权益(或股东权益)总计", "debt_and_equity_total"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.liabilities_and_equity_total",
        "semantic": "liabilities_and_equity_total",
        "eastmoney_field": "TOTAL_LIAB_EQUITY",
    },
    ("balance_sheet", "在建工程", "construction_in_process"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.construction_in_process",
        "semantic": "construction_in_process",
        "eastmoney_field": "CIP",
    },
    ("balance_sheet", "在建工程合计", "construction_process_total"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.construction_in_process_total",
        "semantic": "construction_in_process_total",
        "eastmoney_field": "",
        "note": "Eastmoney only exposed CIP with the same value in this sample.",
    },
    ("balance_sheet", "固定资产净额", "fixed_assets_total"): {
        "decision": "approve_core",
        "local_field": "fixed_assets",
        "semantic": "fixed_assets_net",
        "eastmoney_field": "FIXED_ASSET",
    },
    ("balance_sheet", "其他应付款", "other_accounts_payable"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.other_accounts_payable",
        "semantic": "other_accounts_payable",
        "eastmoney_field": "",
        "note": "Eastmoney only exposed TOTAL_OTHER_PAYABLE with the same value in this sample.",
    },
    ("balance_sheet", "其他应付款合计", "other_payable_total"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.other_payable_total",
        "semantic": "other_payable_total",
        "eastmoney_field": "TOTAL_OTHER_PAYABLE",
    },
    ("balance_sheet", "其他应收款", "other_receivable"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.other_receivable",
        "semantic": "other_receivable",
        "eastmoney_field": "",
        "note": "Eastmoney only exposed TOTAL_OTHER_RECE with the same value in this sample.",
    },
    ("balance_sheet", "其他应收款(合计)", "other_receivable_total"): {
        "decision": "approve_core",
        "local_field": "balance_sheet.other_receivable_total",
        "semantic": "other_receivable_total",
        "eastmoney_field": "TOTAL_OTHER_RECE",
    },
    ("cash_flow_sheet", "经营活动产生的现金流量净额", "indirect_act_cash_flow_net"): {
        "decision": "approve_core",
        "local_field": "operating_cf",
        "semantic": "net_cash_flow_from_operating_activities",
        "eastmoney_field": "NETCASH_OPERATE",
    },
    ("cash_flow_sheet", "现金及现金等价物净增加额", "indirect_cash_net_addition"): {
        "decision": "approve_core",
        "local_field": "total_cf",
        "semantic": "net_increase_in_cash_and_cash_equivalents",
        "eastmoney_field": "CCE_ADD",
    },
    ("profit_sheet", "少数股东损益", "minority_holder_income_loss"): {
        "decision": "approve_core",
        "local_field": "profit_sheet.minority_interest_income",
        "semantic": "minority_interest_income",
        "eastmoney_field": "MINORITY_INTEREST",
    },
    ("profit_sheet", "净利润", "net_profit"): {
        "decision": "approve_core",
        "local_field": "net_income_total",
        "semantic": "total_net_profit",
        "eastmoney_field": "NETPROFIT",
    },
    ("profit_sheet", "持续经营净利润", "continuing_net_profit"): {
        "decision": "approve_core",
        "local_field": "profit_sheet.continuing_net_profit",
        "semantic": "continuing_net_profit",
        "eastmoney_field": "CONTINUED_NETPROFIT",
    },
    ("profit_sheet", "其他综合收益", "other_common_profit"): {
        "decision": "approve_core",
        "local_field": "profit_sheet.other_comprehensive_income",
        "semantic": "other_comprehensive_income",
        "eastmoney_field": "OTHER_COMPRE_INCOME",
    },
    ("profit_sheet", "归属于母公司所有者的其他综合收益", "parent_other_comprehensive_income"): {
        "decision": "approve_core",
        "local_field": "profit_sheet.parent_other_comprehensive_income",
        "semantic": "parent_other_comprehensive_income",
        "eastmoney_field": "PARENT_OCI",
    },
    ("profit_sheet", "基本每股收益", "basic_eps"): {
        "decision": "approve_core",
        "local_field": "profit_sheet.basic_eps",
        "semantic": "basic_eps",
        "eastmoney_field": "BASIC_EPS",
        "unit": "CNY_per_share",
    },
    ("profit_sheet", "稀释每股收益", "diluted_eps"): {
        "decision": "approve_core",
        "local_field": "profit_sheet.diluted_eps",
        "semantic": "diluted_eps",
        "eastmoney_field": "DILUTED_EPS",
        "unit": "CNY_per_share",
    },
}

MANUAL_REJECTION_KEYS = {
    ("balance_sheet", "应付票据及应付账款", "accounts_payable"),
    ("balance_sheet", "应付账款", "payable_notes_and_accounts"),
    ("balance_sheet", "负债和所有者权益(或股东权益)总计", "assets_total"),
    ("balance_sheet", "资产总计", "debt_and_equity_total"),
    ("balance_sheet", "在建工程合计", "construction_in_process"),
    ("balance_sheet", "在建工程", "construction_process_total"),
    ("balance_sheet", "固定资产及清理合计", "fixed_assets_total"),
    ("balance_sheet", "固定资产净额", "fixed_assets"),
    ("balance_sheet", "固定资产及清理合计", "fixed_assets"),
    ("balance_sheet", "其他应付款合计", "other_accounts_payable"),
    ("balance_sheet", "其他应付款", "other_payable_total"),
    ("balance_sheet", "其他应收款(合计)", "other_receivable"),
    ("balance_sheet", "其他应收款", "other_receivable_total"),
    ("profit_sheet", "少数股东损益", "minority_common_profit_total"),
    ("profit_sheet", "归属于少数股东的综合收益总额", "minority_common_profit_total"),
    ("profit_sheet", "归属于少数股东的综合收益总额", "minority_holder_income_loss"),
    ("profit_sheet", "净利润", "continuing_net_profit"),
    ("profit_sheet", "持续经营净利润", "net_profit"),
    ("profit_sheet", "归属于母公司所有者的其他综合收益", "other_common_profit"),
    ("profit_sheet", "外币财务报表折算差额", "other_common_profit"),
    ("profit_sheet", "（二）以后将重分类进损益的其他综合收益", "other_common_profit"),
    ("profit_sheet", "其他综合收益", "parent_other_comprehensive_income"),
    ("profit_sheet", "外币财务报表折算差额", "parent_other_comprehensive_income"),
    ("profit_sheet", "（二）以后将重分类进损益的其他综合收益", "parent_other_comprehensive_income"),
    ("profit_sheet", "稀释每股收益", "basic_eps"),
    ("profit_sheet", "基本每股收益", "diluted_eps"),
}

MANUAL_RESOLUTIONS.update(
    {
        key: {
            "decision": "reject",
            "note": "Rejected by user-confirmed exact field-pair rules.",
        }
        for key in MANUAL_REJECTION_KEYS
    }
)


def parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def audit_sina_ths_financial_mapping_sample(
    sample: Dict[str, Any],
    *,
    profile: str,
    report_period: Optional[str] = None,
    absolute_tolerance: float = 1.0,
    relative_tolerance: float = 1e-6,
    mapping_version: str = MAPPING_VERSION,
) -> Dict[str, Any]:
    """Audit one bounded sample across Sina, THS, CNInfo, and Eastmoney sources."""
    if not isinstance(sample, dict):
        raise ValueError("sample must be a JSON object")

    period = _normalize_report_period(
        report_period or sample.get("report_period") or sample.get("report_date")
    )
    if period is None:
        raise ValueError("report_period is required")

    normalized_sources = {
        source_name: _normalize_source_payload(source_payload, report_period=period)
        for source_name, source_payload in sample.get("sources", {}).items()
        if isinstance(source_payload, dict)
    }
    mappings = get_financial_source_field_mappings(
        profile=profile,
        mapping_version=mapping_version,
    )
    mapping_rows = [
        _audit_mapping(
            mapping,
            normalized_sources=normalized_sources,
            absolute_tolerance=absolute_tolerance,
            relative_tolerance=relative_tolerance,
        )
        for mapping in mappings
    ]
    identity_checks = {
        source_name: _audit_identity_checks(
            source_payload,
            absolute_tolerance=absolute_tolerance,
            relative_tolerance=relative_tolerance,
        )
        for source_name, source_payload in normalized_sources.items()
    }
    source_field_values = _source_field_values_by_source(normalized_sources)
    generated_mapping_candidates = _generate_source_mapping_candidates(
        source_field_values,
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
    )
    local_standard_field_candidates = _build_local_standard_field_candidates(
        profile=profile,
        source_field_values=source_field_values,
        generated_mapping_candidates=generated_mapping_candidates,
    )
    canonical_values = _canonical_values_by_source(normalized_sources)
    canonical_field_matches = _canonical_field_matches_by_source(normalized_sources)
    canonical_comparisons = _compare_canonical_values(
        canonical_values,
        baseline_source="sina_report",
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
    )
    summary = _build_summary(
        mapping_rows=mapping_rows,
        normalized_sources=normalized_sources,
        identity_checks=identity_checks,
        canonical_comparisons=canonical_comparisons,
    )

    return {
        "status": "passed" if not summary["blocking_issue_count"] else "needs_review",
        "instrument_id": sample.get("instrument_id"),
        "report_period": period,
        "profile": profile,
        "mapping_version": mapping_version,
        "tolerance": {
            "absolute_tolerance": absolute_tolerance,
            "relative_tolerance": relative_tolerance,
        },
        "field_counts": {
            source_name: source_payload["field_count"]
            for source_name, source_payload in normalized_sources.items()
        },
        "source_field_values": source_field_values,
        "generated_mapping_candidates": generated_mapping_candidates,
        "local_standard_field_candidates": local_standard_field_candidates,
        "canonical_values": canonical_values,
        "canonical_field_matches": canonical_field_matches,
        "canonical_comparisons": canonical_comparisons,
        "mapping_audit": mapping_rows,
        "identity_checks": identity_checks,
        "summary": summary,
    }


def _normalize_source_payload(
    payload: Dict[str, Any],
    *,
    report_period: str,
) -> Dict[str, Any]:
    statements = {
        statement_type: _normalize_statement_rows(
            payload.get(statement_type),
            report_period=report_period,
        )
        for statement_type in ("balance_sheet", "profit_sheet", "cash_flow_sheet")
    }
    numeric_facts = [
        fact for fact in payload.get("numeric_facts", []) if isinstance(fact, dict)
    ]
    source_fields = set()
    for statement in statements.values():
        source_fields.update(
            field
            for field in statement
            if field not in {"ths_metrics", *REPORT_PERIOD_ALIASES}
            and not field.endswith("__single")
            and not field.endswith("__yoy")
            and not field.endswith("__mom")
            and not field.endswith("__single_yoy")
        )
    for fact in numeric_facts:
        source_fields.add(str(fact.get("fact_name") or fact.get("canonical_fact_name") or ""))
    source_fields.discard("")
    return {
        "statements": statements,
        "numeric_facts": numeric_facts,
        "source_fields": sorted(source_fields),
        "field_count": len(source_fields),
    }


def _normalize_statement_rows(
    raw_rows: Any,
    *,
    report_period: str,
) -> Dict[str, Any]:
    if raw_rows is None:
        return {}
    rows = raw_rows if isinstance(raw_rows, list) else [raw_rows]
    if not rows:
        return {}
    dataframe = pd.DataFrame([row for row in rows if isinstance(row, dict)])
    if dataframe.empty:
        return {}
    provider = AkshareFinancialStatementsProvider()
    indexed = provider._index_statement_rows(dataframe)
    return indexed.get(report_period, {})


def _audit_mapping(
    mapping: Any,
    *,
    normalized_sources: Dict[str, Dict[str, Any]],
    absolute_tolerance: float,
    relative_tolerance: float,
) -> Dict[str, Any]:
    statement_type = STATEMENT_TYPE_BY_FAMILY.get(mapping.statement_family)
    sina_value = _value_from_statement(
        normalized_sources.get("sina_report"),
        statement_type=statement_type,
        field_name=mapping.sina_field,
    )
    ths_value = _value_from_statement(
        normalized_sources.get("ths_report"),
        statement_type=statement_type,
        field_name=mapping.ths_metric,
    )
    comparison = _compare_values(
        sina_value,
        ths_value,
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
    )
    status = comparison["status"]
    if not mapping.approved_for_core:
        status = "not_approved_for_core"
    elif sina_value is None or ths_value is None:
        status = "missing_source_value"

    return {
        "canonical_fact": mapping.canonical_fact,
        "statement_family": mapping.statement_family,
        "profile": mapping.profile,
        "sina_field": mapping.sina_field,
        "ths_metric": mapping.ths_metric,
        "relationship": mapping.relationship,
        "source_unit": mapping.source_unit,
        "canonical_unit": mapping.canonical_unit,
        "unit_multiplier": mapping.unit_multiplier,
        "value_type": mapping.value_type,
        "approved_for_core": mapping.approved_for_core,
        "rejection_reason": mapping.rejection_reason,
        "sina_value": sina_value,
        "ths_value": ths_value,
        "comparison": comparison,
        "status": status,
    }


def _value_from_statement(
    source_payload: Optional[Dict[str, Any]],
    *,
    statement_type: Optional[str],
    field_name: str,
) -> Optional[float]:
    if not source_payload or statement_type is None:
        return None
    statement = source_payload.get("statements", {}).get(statement_type, {})
    return _to_float(statement.get(field_name))


def _source_field_values_by_source(
    normalized_sources: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    return {
        source_name: _source_field_values_for_source(source_payload)
        for source_name, source_payload in normalized_sources.items()
    }


def _source_field_values_for_source(source_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for statement_type, statement in source_payload.get("statements", {}).items():
        for field_name, raw_value in statement.items():
            if field_name == "ths_metrics" or "__" in field_name:
                continue
            numeric = _to_float(raw_value)
            if numeric is None:
                continue
            metadata = describe_financial_numeric_fact_name(field_name)
            rows.append(
                {
                    "statement_type": statement_type,
                    "field_name": field_name,
                    "value": numeric,
                    "canonical_fact_name": metadata.get("canonical_fact_name"),
                    "canonical_semantic": metadata.get("canonical_semantic"),
                    "canonical_unit": metadata.get("canonical_unit"),
                    "unit_review_status": (
                        "known_unit"
                        if metadata.get("canonical_unit")
                        else "requires_unit_review"
                    ),
                }
            )
    return sorted(
        rows,
        key=lambda item: (
            str(item.get("statement_type") or ""),
            str(item.get("field_name") or ""),
        ),
    )


def _generate_source_mapping_candidates(
    source_field_values: Dict[str, List[Dict[str, Any]]],
    *,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> Dict[str, Any]:
    source_pairs = (
        ("sina_report", "ths_report"),
        ("sina_report", "eastmoney_report"),
        ("ths_report", "eastmoney_report"),
    )
    candidates_by_pair: Dict[str, List[Dict[str, Any]]] = {}
    for left_source, right_source in source_pairs:
        pair_key = f"{left_source}__{right_source}"
        candidates_by_pair[pair_key] = _generate_pair_candidates(
            source_field_values.get(left_source, []),
            source_field_values.get(right_source, []),
            left_source=left_source,
            right_source=right_source,
            absolute_tolerance=absolute_tolerance,
            relative_tolerance=relative_tolerance,
        )
    return {
        "candidate_pairs": candidates_by_pair,
        "summary": {
            pair_key: len(rows)
            for pair_key, rows in candidates_by_pair.items()
        },
    }


def _generate_pair_candidates(
    left_rows: List[Dict[str, Any]],
    right_rows: List[Dict[str, Any]],
    *,
    left_source: str,
    right_source: str,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    right_by_statement: Dict[str, List[Dict[str, Any]]] = {}
    for row in right_rows:
        right_by_statement.setdefault(str(row.get("statement_type")), []).append(row)

    for left in left_rows:
        statement_type = str(left.get("statement_type"))
        for right in right_by_statement.get(statement_type, []):
            comparison = _compare_values(
                _to_float(left.get("value")),
                _to_float(right.get("value")),
                absolute_tolerance=absolute_tolerance,
                relative_tolerance=relative_tolerance,
            )
            if comparison["status"] != "passed":
                continue
            left_canonical = left.get("canonical_fact_name")
            right_canonical = right.get("canonical_fact_name")
            semantic_status = (
                "same_known_canonical"
                if left_canonical and left_canonical == right_canonical
                else "candidate_requires_review"
            )
            candidates.append(
                {
                    "left_source": left_source,
                    "left_statement_type": statement_type,
                    "left_field_name": left.get("field_name"),
                    "left_value": left.get("value"),
                    "left_canonical_fact_name": left_canonical,
                    "right_source": right_source,
                    "right_statement_type": statement_type,
                    "right_field_name": right.get("field_name"),
                    "right_value": right.get("value"),
                    "right_canonical_fact_name": right_canonical,
                    "relationship_candidate": (
                        "exact_equivalent"
                        if semantic_status == "same_known_canonical"
                        else "unknown_candidate"
                    ),
                    "semantic_status": semantic_status,
                    "approved_for_core": False,
                    **comparison,
                }
            )
    return sorted(
        candidates,
        key=lambda item: (
            str(item.get("left_statement_type") or ""),
            str(item.get("left_field_name") or ""),
            str(item.get("right_field_name") or ""),
        ),
    )


def _build_local_standard_field_candidates(
    *,
    profile: str,
    source_field_values: Dict[str, List[Dict[str, Any]]],
    generated_mapping_candidates: Dict[str, Any],
) -> Dict[str, Any]:
    """Build local standard-field candidates anchored on the Sina/THS intersection."""
    pair_candidates = generated_mapping_candidates.get("candidate_pairs", {})
    sina_ths = pair_candidates.get("sina_report__ths_report", [])
    eastmoney_by_sina = _right_matches_by_left(
        pair_candidates.get("sina_report__eastmoney_report", [])
    )
    eastmoney_by_ths = _right_matches_by_left(
        pair_candidates.get("ths_report__eastmoney_report", [])
    )
    source_lookup = _source_field_lookup(source_field_values)
    clusters = []
    for item in sina_ths:
        statement_type = item.get("left_statement_type")
        sina_key = _field_key(
            "sina_report",
            statement_type,
            item.get("left_field_name"),
        )
        ths_key = _field_key(
            "ths_report",
            statement_type,
            item.get("right_field_name"),
        )
        eastmoney_keys = sorted(
            {
                *eastmoney_by_sina.get(sina_key, set()),
                *eastmoney_by_ths.get(ths_key, set()),
            }
        )
        sources = {
            "sina_report": [_field_entry_from_lookup(source_lookup, sina_key)],
            "ths_report": [_field_entry_from_lookup(source_lookup, ths_key)],
        }
        eastmoney_entries = [
            _field_entry_from_lookup(source_lookup, key)
            for key in eastmoney_keys
            if key in source_lookup
        ]
        if eastmoney_entries:
            sources["eastmoney_report"] = eastmoney_entries

        unit_review = _build_unit_review(sources)
        canonical_names = sorted(
            {
                str(entry.get("canonical_fact_name"))
                for rows in sources.values()
                for entry in rows
                if entry.get("canonical_fact_name")
            }
        )
        review_status = (
            "known_canonical_candidate"
            if item.get("semantic_status") == "same_known_canonical"
            else "requires_semantic_review"
        )
        proposed_key = _propose_standard_field_key(
            canonical_names=canonical_names,
            statement_type=statement_type,
            ths_field=item.get("right_field_name"),
            eastmoney_entries=eastmoney_entries,
        )
        clusters.append(
            {
                "standard_field_key_candidate": proposed_key,
                "statement_type": statement_type,
                "profile": profile,
                "review_status": review_status,
                "approved_for_local_standard": False,
                "relationship_candidate": item.get("relationship_candidate"),
                "canonical_fact_candidates": canonical_names,
                "unit_review": unit_review,
                "sources": sources,
                "evidence": {
                    "sina_ths": item,
                    "eastmoney_match_count": len(eastmoney_entries),
                },
            }
        )
    clusters = _apply_machine_review_statuses(clusters)
    clusters.sort(
        key=lambda item: (
            str(item.get("statement_type") or ""),
            str(item.get("standard_field_key_candidate") or ""),
        )
    )
    return {
        "basis": "sina_ths_intersection_with_eastmoney_enrichment",
        "summary": {
            "candidate_count": len(clusters),
            "known_canonical_candidate_count": sum(
                1 for item in clusters if item["review_status"] == "known_canonical_candidate"
            ),
            "requires_semantic_review_count": sum(
                1 for item in clusters if item["review_status"] == "requires_semantic_review"
            ),
            "with_eastmoney_match_count": sum(
                1 for item in clusters if "eastmoney_report" in item["sources"]
            ),
            "known_unit_match_count": sum(
                1 for item in clusters if item["unit_review"]["status"] == "known_units_match"
            ),
            "requires_unit_review_count": sum(
                1 for item in clusters if item["unit_review"]["status"] != "known_units_match"
            ),
            "machine_approved_candidate_count": sum(
                1
                for item in clusters
                if item["machine_review"]["status"] == MACHINE_REVIEW_APPROVED
            ),
            "human_review_required_count": sum(
                1
                for item in clusters
                if item["machine_review"]["status"] == MACHINE_REVIEW_HUMAN
            ),
            "needs_more_evidence_count": sum(
                1
                for item in clusters
                if item["machine_review"]["status"] == MACHINE_REVIEW_MORE_EVIDENCE
            ),
        },
        "candidates": clusters,
    }


def _apply_machine_review_statuses(
    clusters: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    standard_counts = _cluster_counts(
        clusters,
        lambda item: (
            item.get("statement_type"),
            item.get("standard_field_key_candidate"),
        ),
    )
    ths_counts = _cluster_counts(
        clusters,
        lambda item: (
            item.get("statement_type"),
            _first_source_field(item, "ths_report"),
        ),
    )
    reviewed = []
    for item in clusters:
        updated = dict(item)
        updated["unit_review"] = _build_unit_review(updated.get("sources", {}))
        updated["machine_review"] = _machine_review_for_candidate(
            updated,
            standard_counts=standard_counts,
            ths_counts=ths_counts,
        )
        reviewed.append(updated)
    return reviewed


def _cluster_counts(
    clusters: List[Dict[str, Any]],
    key_builder: Any,
) -> Dict[Any, int]:
    counts: Dict[Any, int] = {}
    for item in clusters:
        key = key_builder(item)
        if key[-1] in {None, ""}:
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts


def _machine_review_for_candidate(
    candidate: Dict[str, Any],
    *,
    standard_counts: Dict[Any, int],
    ths_counts: Dict[Any, int],
) -> Dict[str, Any]:
    manual_resolution = _manual_resolution_for_candidate(candidate)
    if manual_resolution:
        return _machine_review_from_manual_resolution(candidate, manual_resolution)

    blockers = _machine_review_blockers(
        candidate,
        standard_counts=standard_counts,
        ths_counts=ths_counts,
    )
    reasons = []
    if "eastmoney_report" in candidate.get("sources", {}):
        reasons.append("three_source_numeric_match")
    if candidate.get("unit_review", {}).get("status") == "known_units_match":
        reasons.append("known_or_inferred_units_match")
    if candidate.get("review_status") == "known_canonical_candidate":
        reasons.append("known_canonical_alias")
    elif not blockers:
        reasons.append("unique_three_source_field_cluster")

    if blockers:
        status = (
            MACHINE_REVIEW_MORE_EVIDENCE
            if any(blocker in blockers for blocker in {"missing_eastmoney_match", "unit_conflict"})
            else MACHINE_REVIEW_HUMAN
        )
        confidence = "low"
        suggested_decision = "needs_more_evidence" if status == MACHINE_REVIEW_MORE_EVIDENCE else ""
    else:
        status = MACHINE_REVIEW_APPROVED
        confidence = "high" if candidate.get("review_status") == "known_canonical_candidate" else "medium_high"
        suggested_decision = "approve_core"

    return {
        "status": status,
        "confidence": confidence,
        "reasons": reasons,
        "blockers": blockers,
        "suggested_decision": suggested_decision,
        "suggested_relationship": "exact_equivalent" if not blockers else "",
        "suggested_canonical_unit": _single_known_unit(candidate.get("unit_review", {})),
        "suggested_source_unit": _single_known_unit(candidate.get("unit_review", {})),
        "suggested_unit_multiplier": 1.0 if not blockers else None,
        "suggested_local_field": candidate.get("standard_field_key_candidate") if not blockers else "",
        "suggested_semantic": candidate.get("standard_field_key_candidate") if not blockers else "",
        "suggested_sina_field": _first_source_field(candidate, "sina_report") if not blockers else "",
        "suggested_ths_metric": _first_source_field(candidate, "ths_report") if not blockers else "",
        "suggested_eastmoney_field": (
            _first_source_field(candidate, "eastmoney_report") if not blockers else ""
        ),
    }


def _manual_resolution_for_candidate(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    key = (
        str(candidate.get("statement_type") or ""),
        str(_first_source_field(candidate, "sina_report") or ""),
        str(_first_source_field(candidate, "ths_report") or ""),
    )
    resolution = MANUAL_RESOLUTIONS.get(key)
    if resolution is not None:
        return resolution
    return None


def _machine_review_from_manual_resolution(
    candidate: Dict[str, Any],
    resolution: Dict[str, Any],
) -> Dict[str, Any]:
    decision = resolution.get("decision")
    unit = str(resolution.get("unit") or _single_known_unit(candidate.get("unit_review", {})) or "CNY")
    if decision == "approve_core":
        return {
            "status": MACHINE_REVIEW_APPROVED,
            "confidence": "manual_confirmed",
            "reasons": ["user_confirmed_semantic_mapping"],
            "blockers": [],
            "suggested_decision": "approve_core",
            "suggested_relationship": "exact_equivalent",
            "suggested_canonical_unit": unit,
            "suggested_source_unit": unit,
            "suggested_unit_multiplier": 1.0,
            "suggested_local_field": resolution.get("local_field") or candidate.get("standard_field_key_candidate"),
            "suggested_semantic": resolution.get("semantic") or resolution.get("local_field") or "",
            "suggested_sina_field": _first_source_field(candidate, "sina_report") or "",
            "suggested_ths_metric": _first_source_field(candidate, "ths_report") or "",
            "suggested_eastmoney_field": resolution.get("eastmoney_field") or "",
            "review_note": resolution.get("note") or "",
        }
    return {
        "status": MACHINE_REVIEW_REJECTED,
        "confidence": "manual_confirmed",
        "reasons": ["user_rejected_or_non_exact_mapping"],
        "blockers": [],
        "suggested_decision": "reject",
        "suggested_relationship": "rejected",
        "suggested_canonical_unit": "",
        "suggested_source_unit": "",
        "suggested_unit_multiplier": None,
        "suggested_local_field": "",
        "suggested_semantic": "",
        "suggested_sina_field": _first_source_field(candidate, "sina_report") or "",
        "suggested_ths_metric": _first_source_field(candidate, "ths_report") or "",
        "suggested_eastmoney_field": "",
        "review_note": resolution.get("note") or "",
    }


def _machine_review_blockers(
    candidate: Dict[str, Any],
    *,
    standard_counts: Dict[Any, int],
    ths_counts: Dict[Any, int],
) -> List[str]:
    blockers = []
    sources = candidate.get("sources", {})
    if "eastmoney_report" not in sources:
        blockers.append("missing_eastmoney_match")
    for source_name in ("sina_report", "ths_report"):
        if len(_source_fields(candidate, source_name)) != 1:
            blockers.append(f"ambiguous_{source_name}_field")
    if len(_source_fields(candidate, "eastmoney_report")) != 1:
        blockers.append("ambiguous_eastmoney_field")

    statement_type = candidate.get("statement_type")
    standard_key = (statement_type, candidate.get("standard_field_key_candidate"))
    ths_key = (statement_type, _first_source_field(candidate, "ths_report"))
    if standard_counts.get(standard_key, 0) > 1:
        blockers.append("duplicate_local_field_candidate")
    if ths_counts.get(ths_key, 0) > 1:
        blockers.append("duplicate_ths_field_candidate")

    unit_status = candidate.get("unit_review", {}).get("status")
    if unit_status == "unit_conflict":
        blockers.append("unit_conflict")
    elif unit_status != "known_units_match":
        blockers.append("unresolved_unit")
    if _looks_like_per_share_candidate(candidate):
        blockers.append("per_share_same_value_ambiguity")
    return sorted(set(blockers))


def _source_fields(candidate: Dict[str, Any], source_name: str) -> List[str]:
    return [
        str(entry.get("field_name"))
        for entry in candidate.get("sources", {}).get(source_name, [])
        if entry.get("field_name")
    ]


def _first_source_field(candidate: Dict[str, Any], source_name: str) -> Optional[str]:
    fields = _source_fields(candidate, source_name)
    return fields[0] if fields else None


def _single_known_unit(unit_review: Dict[str, Any]) -> str:
    units = unit_review.get("known_units", []) or []
    return str(units[0]) if len(units) == 1 else ""


def _looks_like_per_share_candidate(candidate: Dict[str, Any]) -> bool:
    text = " ".join(
        field
        for source_name in ("sina_report", "ths_report", "eastmoney_report")
        for field in _source_fields(candidate, source_name)
    ).lower()
    return any(hint.lower() in text for hint in PER_SHARE_FIELD_HINTS)


def _build_unit_review(
    sources: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    units_by_source: Dict[str, List[Optional[str]]] = {
        source_name: [_entry_unit(entry) for entry in entries]
        for source_name, entries in sources.items()
    }
    known_units = {
        str(unit)
        for units in units_by_source.values()
        for unit in units
        if unit
    }
    unknown_sources = sorted(
        source_name
        for source_name, units in units_by_source.items()
        if any(not unit for unit in units)
    )
    if len(known_units) == 1 and not unknown_sources:
        status = "known_units_match"
    elif len(known_units) > 1:
        status = "unit_conflict"
    else:
        status = "requires_unit_review"
    return {
        "status": status,
        "known_units": sorted(known_units),
        "unknown_unit_sources": unknown_sources,
        "unit_basis": "canonical_alias_or_statement_amount_inference",
    }


def _entry_unit(entry: Dict[str, Any]) -> Optional[str]:
    if entry.get("canonical_unit"):
        return entry.get("canonical_unit")
    field_text = str(entry.get("field_name") or "").lower()
    if any(hint.lower() in field_text for hint in PER_SHARE_FIELD_HINTS):
        return "CNY_per_share"
    if entry.get("statement_type") in FINANCIAL_STATEMENT_AMOUNT_TYPES:
        return "CNY"
    return None


def _right_matches_by_left(rows: List[Dict[str, Any]]) -> Dict[str, set[str]]:
    matches: Dict[str, set[str]] = {}
    for row in rows:
        left_key = _field_key(
            row.get("left_source"),
            row.get("left_statement_type"),
            row.get("left_field_name"),
        )
        right_key = _field_key(
            row.get("right_source"),
            row.get("right_statement_type"),
            row.get("right_field_name"),
        )
        matches.setdefault(left_key, set()).add(right_key)
    return matches


def _source_field_lookup(
    source_field_values: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for source_name, rows in source_field_values.items():
        for row in rows:
            key = _field_key(
                source_name,
                row.get("statement_type"),
                row.get("field_name"),
            )
            lookup[key] = {"source": source_name, **row}
    return lookup


def _field_entry_from_lookup(
    source_lookup: Dict[str, Dict[str, Any]],
    key: str,
) -> Dict[str, Any]:
    return dict(source_lookup.get(key, {}))


def _field_key(source: Any, statement_type: Any, field_name: Any) -> str:
    return f"{source}|{statement_type}|{field_name}"


def _propose_standard_field_key(
    *,
    canonical_names: List[str],
    statement_type: Any,
    ths_field: Any,
    eastmoney_entries: List[Dict[str, Any]],
) -> str:
    if canonical_names:
        return canonical_names[0]
    if ths_field:
        return f"{statement_type}.{ths_field}"
    if eastmoney_entries:
        return f"{statement_type}.{eastmoney_entries[0].get('field_name')}"
    return f"{statement_type}.unmapped_candidate"


def _canonical_values_by_source(
    normalized_sources: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, float]]:
    return {
        source_name: _canonical_values_for_source(source_payload)
        for source_name, source_payload in normalized_sources.items()
    }


def _canonical_values_for_source(source_payload: Dict[str, Any]) -> Dict[str, float]:
    values: Dict[str, float] = {}
    for statement in source_payload.get("statements", {}).values():
        for field_name, raw_value in statement.items():
            if field_name == "ths_metrics" or "__" in field_name:
                continue
            numeric = _to_float(raw_value)
            if numeric is None:
                continue
            metadata = describe_financial_numeric_fact_name(field_name)
            canonical_name = metadata.get("canonical_fact_name")
            if canonical_name:
                values.setdefault(canonical_name, numeric)
    for fact in source_payload.get("numeric_facts", []):
        canonical_name = str(fact.get("canonical_fact_name") or "").strip()
        numeric = _to_float(fact.get("fact_value"))
        if canonical_name and numeric is not None:
            values.setdefault(canonical_name, numeric)
    return dict(sorted(values.items()))


def _canonical_field_matches_by_source(
    normalized_sources: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    return {
        source_name: _canonical_field_matches_for_source(source_payload)
        for source_name, source_payload in normalized_sources.items()
    }


def _canonical_field_matches_for_source(
    source_payload: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    matches: Dict[str, List[Dict[str, Any]]] = {}
    for statement_type, statement in source_payload.get("statements", {}).items():
        for field_name, raw_value in statement.items():
            if field_name == "ths_metrics" or "__" in field_name:
                continue
            numeric = _to_float(raw_value)
            if numeric is None:
                continue
            metadata = describe_financial_numeric_fact_name(field_name)
            canonical_name = metadata.get("canonical_fact_name")
            if not canonical_name:
                continue
            matches.setdefault(canonical_name, []).append(
                {
                    "statement_type": statement_type,
                    "field_name": field_name,
                    "value": numeric,
                    "canonical_semantic": metadata.get("canonical_semantic"),
                    "canonical_unit": metadata.get("canonical_unit"),
                }
            )
    for fact in source_payload.get("numeric_facts", []):
        canonical_name = str(fact.get("canonical_fact_name") or "").strip()
        numeric = _to_float(fact.get("fact_value"))
        if not canonical_name or numeric is None:
            continue
        matches.setdefault(canonical_name, []).append(
            {
                "statement_type": fact.get("statement_family"),
                "field_name": fact.get("fact_name"),
                "value": numeric,
                "canonical_semantic": None,
                "canonical_unit": fact.get("canonical_unit") or fact.get("unit"),
            }
        )
    return {
        canonical_name: rows
        for canonical_name, rows in sorted(matches.items())
    }


def _compare_canonical_values(
    canonical_values: Dict[str, Dict[str, float]],
    *,
    baseline_source: str,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> List[Dict[str, Any]]:
    baseline = canonical_values.get(baseline_source, {})
    comparisons: List[Dict[str, Any]] = []
    for source_name, values in canonical_values.items():
        if source_name == baseline_source:
            continue
        for canonical_fact in sorted(set(baseline) & set(values)):
            source_tolerance = _canonical_comparison_tolerance(
                source_name,
                absolute_tolerance=absolute_tolerance,
                relative_tolerance=relative_tolerance,
            )
            comparison = _compare_values(
                baseline[canonical_fact],
                values[canonical_fact],
                absolute_tolerance=source_tolerance["absolute_tolerance"],
                relative_tolerance=source_tolerance["relative_tolerance"],
            )
            comparisons.append(
                {
                    "baseline_source": baseline_source,
                    "other_source": source_name,
                    "canonical_fact": canonical_fact,
                    "blocking_for_local_core": (
                        source_name in CANONICAL_COMPARISON_BLOCKING_SOURCES
                    ),
                    "baseline_value": baseline[canonical_fact],
                    "other_value": values[canonical_fact],
                    "absolute_tolerance": source_tolerance["absolute_tolerance"],
                    "relative_tolerance": source_tolerance["relative_tolerance"],
                    **comparison,
                }
            )
    return comparisons


def _canonical_comparison_tolerance(
    source_name: str,
    *,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> Dict[str, float]:
    override = CANONICAL_COMPARISON_TOLERANCE_OVERRIDES.get(source_name, {})
    return {
        "absolute_tolerance": max(
            absolute_tolerance,
            float(override.get("absolute_tolerance", absolute_tolerance)),
        ),
        "relative_tolerance": max(
            relative_tolerance,
            float(override.get("relative_tolerance", relative_tolerance)),
        ),
    }


def _audit_identity_checks(
    source_payload: Dict[str, Any],
    *,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> Dict[str, Any]:
    combined_fields: Dict[str, Any] = {}
    for statement in source_payload.get("statements", {}).values():
        combined_fields.update(statement)

    checks = {}
    for check_name, spec in IDENTITY_SPECS.items():
        left = _first_float(combined_fields, spec["total_assets"])
        liabilities = _first_float(combined_fields, spec["total_liabilities"])
        equity_total = _first_float(combined_fields, spec["equity_total"])
        if left is None or liabilities is None or equity_total is None:
            checks[check_name] = {"status": "insufficient_components"}
            continue
        right = liabilities + equity_total
        checks[check_name] = {
            **_compare_values(
                left,
                right,
                absolute_tolerance=absolute_tolerance,
                relative_tolerance=relative_tolerance,
            ),
            "left_value": left,
            "right_value": right,
            "components": {
                "total_liabilities": liabilities,
                "equity_total": equity_total,
            },
        }
    return checks


def _build_summary(
    *,
    mapping_rows: List[Dict[str, Any]],
    normalized_sources: Dict[str, Dict[str, Any]],
    identity_checks: Dict[str, Any],
    canonical_comparisons: List[Dict[str, Any]],
) -> Dict[str, Any]:
    approved_rows = [row for row in mapping_rows if row["approved_for_core"]]
    approved_passed = [
        row for row in approved_rows if row["status"] == "passed"
    ]
    rejected_rows = [
        row for row in mapping_rows if not row["approved_for_core"]
    ]
    value_mismatches = [
        row for row in approved_rows if row["status"] == "value_mismatch"
    ]
    missing_values = [
        row for row in approved_rows if row["status"] == "missing_source_value"
    ]
    identity_failures = [
        {"source": source, "check": check_name, **check}
        for source, checks in identity_checks.items()
        for check_name, check in checks.items()
        if check.get("status") == "value_mismatch"
    ]
    canonical_mismatches = [
        item for item in canonical_comparisons if item.get("status") == "value_mismatch"
    ]
    blocking_canonical_mismatches = [
        item for item in canonical_mismatches if item.get("blocking_for_local_core") is True
    ]
    return {
        "source_count": len(normalized_sources),
        "mapping_count": len(mapping_rows),
        "approved_mapping_count": len(approved_rows),
        "approved_mapping_passed_count": len(approved_passed),
        "rejected_or_unapproved_mapping_count": len(rejected_rows),
        "missing_source_value_count": len(missing_values),
        "value_mismatch_count": len(value_mismatches),
        "identity_failure_count": len(identity_failures),
        "canonical_mismatch_count": len(canonical_mismatches),
        "blocking_canonical_mismatch_count": len(blocking_canonical_mismatches),
        "blocking_issue_count": (
            len(value_mismatches)
            + len(identity_failures)
            + len(blocking_canonical_mismatches)
        ),
    }


def _first_float(row: Dict[str, Any], aliases: Iterable[str]) -> Optional[float]:
    for alias in aliases:
        numeric = _to_float(row.get(alias))
        if numeric is not None:
            return numeric
    return None


def _compare_values(
    left: Optional[float],
    right: Optional[float],
    *,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> Dict[str, Any]:
    if left is None or right is None:
        return {
            "status": "missing_value",
            "absolute_diff": None,
            "relative_diff": None,
        }
    absolute_diff = abs(left - right)
    denominator = max(abs(left), abs(right), 1.0)
    relative_diff = absolute_diff / denominator
    return {
        "status": "passed"
        if absolute_diff <= absolute_tolerance
        or relative_diff <= relative_tolerance
        else "value_mismatch",
        "absolute_diff": absolute_diff,
        "relative_diff": relative_diff,
    }


def _to_float(value: Any) -> Optional[float]:
    if value in {None, "", "--"}:
        return None
    if isinstance(value, str):
        value = value.replace(",", "").replace("%", "").strip()
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


def _normalize_report_period(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) == 6 and text[:4].isdigit() and text[-1].upper() in {"1", "2", "3", "4"}:
        quarter = int(text[-1])
        month_day = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[quarter]
        return f"{text[:4]}-{month_day}"
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return text


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit a bounded Sina/THS financial field mapping sample."
    )
    parser.add_argument("--sample-path", required=True, help="Path to sample JSON")
    parser.add_argument("--output-path", help="Optional JSON output path")
    parser.add_argument("--profile", default="nonbank", choices=["nonbank", "bank"])
    parser.add_argument("--report-period", help="Override report period")
    parser.add_argument("--absolute-tolerance", type=float, default=1.0)
    parser.add_argument("--relative-tolerance", type=float, default=1e-6)
    parser.add_argument("--mapping-version", default=MAPPING_VERSION)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    sample_path = Path(args.sample_path)
    with sample_path.open("r", encoding="utf-8") as handle:
        sample = json.load(handle)
    result = audit_sina_ths_financial_mapping_sample(
        sample,
        profile=args.profile,
        report_period=args.report_period,
        absolute_tolerance=args.absolute_tolerance,
        relative_tolerance=args.relative_tolerance,
        mapping_version=args.mapping_version,
    )
    payload = json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        Path(args.output_path).write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)
    return 0 if result["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
