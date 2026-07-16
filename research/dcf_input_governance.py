"""Point-in-time local input governance for professional DCF."""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from datetime import date
from typing import Any, Dict, Iterable, Optional


A_SHARE_EXCHANGES = frozenset({"SSE", "SZSE", "BSE"})
CAPEX_SOURCE_FIELD = "cash_flow_sheet.pay_fixed_assets_etc_cash"
INTEREST_BEARING_DEBT_FIELDS = (
    "short_term_loans",
    "payable_short_term_bonds",
    "year_non_current_debt",
    "long_term_loan",
    "bonds_payable",
)


def resolve_financial_availability(
    bundle: Dict[str, Any],
    *,
    exchange: Optional[str],
) -> Dict[str, Any]:
    """Resolve actual or conservative estimated filing availability."""
    actual = _normalize_iso_date(
        bundle.get("data_available_date") or bundle.get("publish_date")
    )
    if actual:
        if bool(bundle.get("data_available_date_estimated")):
            return {
                "data_available_date": actual,
                "source": bundle.get("data_available_date_source")
                or "estimated_statutory_deadline",
                "quality_flag": bundle.get("data_available_date_quality_flag")
                or "estimated_conservative",
                "estimated": True,
            }
        return {
            "data_available_date": actual,
            "source": (
                "actual_data_available_date"
                if bundle.get("data_available_date")
                else "actual_publish_date"
            ),
            "quality_flag": "actual_filing_date",
            "estimated": False,
        }

    report_period = _normalize_iso_date(bundle.get("report_period"))
    normalized_exchange = str(exchange or bundle.get("exchange") or "").upper()
    if report_period and normalized_exchange in A_SHARE_EXCHANGES:
        deadline = estimate_a_share_filing_deadline(report_period)
        if deadline:
            return {
                "data_available_date": deadline,
                "source": "estimated_statutory_deadline",
                "quality_flag": "estimated_conservative",
                "estimated": True,
            }

    return {
        "data_available_date": None,
        "source": "missing",
        "quality_flag": "missing",
        "estimated": False,
    }


def estimate_a_share_filing_deadline(report_period: str) -> Optional[str]:
    """Return the conservative statutory latest filing date for a periodic report."""
    normalized = _normalize_iso_date(report_period)
    if not normalized:
        return None
    period = date.fromisoformat(normalized)
    if period.month == 3:
        deadline = date(period.year, 4, 30)
    elif period.month == 6:
        deadline = date(period.year, 8, 31)
    elif period.month == 9:
        deadline = date(period.year, 10, 31)
    elif period.month == 12:
        deadline = date(period.year + 1, 4, 30)
    else:
        return None
    return deadline.isoformat()


def select_financial_bundle_as_of(
    bundles: Iterable[Dict[str, Any]],
    *,
    valuation_date: str,
    exchange: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Select the latest report available on or before the valuation date."""
    cutoff = _normalize_iso_date(valuation_date)
    if not cutoff:
        raise ValueError("valuation_date must be an ISO date")

    candidates = []
    for raw_bundle in bundles:
        if not isinstance(raw_bundle, dict):
            continue
        availability = resolve_financial_availability(raw_bundle, exchange=exchange)
        available_date = availability["data_available_date"]
        report_period = _normalize_iso_date(raw_bundle.get("report_period"))
        if not available_date or not report_period or available_date > cutoff:
            continue
        candidates.append((report_period, available_date, raw_bundle, availability))

    if not candidates:
        return None
    _, _, selected_raw, availability = max(
        candidates,
        key=lambda item: (item[0], item[1]),
    )
    selected = deepcopy(selected_raw)
    selected["data_available_date"] = availability["data_available_date"]
    selected["data_available_date_source"] = availability["source"]
    selected["data_available_date_quality_flag"] = availability["quality_flag"]
    selected["data_available_date_estimated"] = availability["estimated"]
    lineage = selected.get("lineage")
    if not isinstance(lineage, dict):
        lineage = {}
        selected["lineage"] = lineage
    lineage["financial_availability"] = dict(availability)
    return selected


def derive_capital_expenditure(
    bundles: Iterable[Dict[str, Any]],
    *,
    selected_report_period: str,
) -> Dict[str, Any]:
    """Derive annual or TTM capex from cumulative cash-flow statement facts."""
    normalized_period = _normalize_iso_date(selected_report_period)
    if not normalized_period:
        return _capex_gap("invalid_selected_report_period")

    by_period = {
        str(bundle.get("report_period"))[:10]: bundle
        for bundle in bundles
        if isinstance(bundle, dict) and bundle.get("report_period")
    }
    selected = by_period.get(normalized_period)
    if selected is None:
        return _capex_gap("selected_report_period_missing")
    current_value = _extract_capex_value(selected)
    if current_value is None:
        return _capex_gap("current_capex_fact_missing")

    period = date.fromisoformat(normalized_period)
    if period.month == 12:
        return _capex_success(
            value=abs(current_value),
            method="annual_reported_cash_outflow",
            source_periods={"annual": normalized_period},
            quality_flag="reported_annual_proxy",
        )
    if period.month not in {3, 6, 9}:
        return _capex_gap("unsupported_interim_report_period")

    prior_annual_period = f"{period.year - 1:04d}-12-31"
    prior_same_period = f"{period.year - 1:04d}-{period.month:02d}-{period.day:02d}"
    prior_annual = _extract_capex_value(by_period.get(prior_annual_period) or {})
    prior_comparable = _extract_capex_value(by_period.get(prior_same_period) or {})
    if prior_annual is None or prior_comparable is None:
        return _capex_gap(
            "ttm_capex_bridge_incomplete",
            source_periods={
                "current_cumulative": normalized_period,
                "prior_annual": prior_annual_period,
                "prior_same_period_cumulative": prior_same_period,
            },
        )

    ttm_value = abs(current_value) + abs(prior_annual) - abs(prior_comparable)
    if not math.isfinite(ttm_value) or ttm_value < 0:
        return _capex_gap("ttm_capex_bridge_invalid")
    return _capex_success(
        value=ttm_value,
        method="ttm_cumulative_cash_flow_bridge",
        source_periods={
            "current_cumulative": normalized_period,
            "prior_annual": prior_annual_period,
            "prior_same_period_cumulative": prior_same_period,
        },
        quality_flag="derived_ttm_proxy",
    )


def derive_cash_and_debt(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Derive DCF cash and debt without treating total liabilities as debt."""
    facts = bundle.get("facts") or bundle.get("facts_json") or {}
    balance_sheet = facts.get("balance_sheet") if isinstance(facts, dict) else {}
    if not isinstance(balance_sheet, dict):
        balance_sheet = {}

    cash_source = next(
        (
            field_name
            for field_name in ("total_cash", "cash")
            if _safe_float(balance_sheet.get(field_name)) is not None
        ),
        None,
    )
    cash_value = (
        abs(_safe_float(balance_sheet.get(cash_source)) or 0.0)
        if cash_source
        else None
    )
    debt_components = {
        field_name: abs(value)
        for field_name in INTEREST_BEARING_DEBT_FIELDS
        if (value := _safe_float(balance_sheet.get(field_name))) is not None
    }
    debt_value = sum(debt_components.values()) if debt_components else None
    lease_value = _safe_float(balance_sheet.get("lease_debt"))
    if lease_value is not None:
        lease_value = abs(lease_value)

    payload = {
        "status": "success" if cash_value is not None and debt_value is not None else "partial",
        "cash_and_equivalents": cash_value,
        "cash_source_field": (
            f"balance_sheet.{cash_source}" if cash_source else None
        ),
        "total_debt": debt_value,
        "debt_definition": "sum_of_available_interest_bearing_debt_components",
        "debt_components": debt_components,
        "lease_liabilities": lease_value,
        "lease_source_field": (
            "balance_sheet.lease_debt" if lease_value is not None else None
        ),
        "excluded_fields": [
            "balance_sheet.total_debt",
            "balance_sheet.current_total_debt",
            "balance_sheet.non_current_debt_total",
        ],
        "quality_flag": (
            "derived_from_balance_sheet_components"
            if debt_components
            else "interest_bearing_debt_components_missing"
        ),
        "warnings": [],
    }
    if cash_value is None:
        payload["warnings"].append("cash_and_equivalents_missing")
    if debt_value is None:
        payload["warnings"].append("interest_bearing_debt_missing")
    payload["lineage_hash"] = _stable_hash(payload)
    return payload


def enrich_instrument_with_industry(
    instrument: Dict[str, Any],
    membership: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Copy authoritative industry identity into the DCF instrument view."""
    enriched = deepcopy(instrument)
    if not isinstance(membership, dict):
        enriched["dcf_industry_mapping_status"] = "missing"
        return enriched
    for field_name in (
        "taxonomy_system",
        "taxonomy_version",
        "industry_code",
        "industry_name",
        "industry_level",
        "mapping_status",
        "effective_date",
        "sw_l1_code",
        "sw_l1_name",
        "sw_l2_code",
        "sw_l2_name",
        "sw_l3_code",
        "sw_l3_name",
        "source",
        "source_mode",
        "data_as_of",
    ):
        if membership.get(field_name) is not None:
            target = field_name if field_name.startswith("sw_") else f"dcf_industry_{field_name}"
            enriched[target] = membership[field_name]
    enriched["dcf_industry_membership"] = deepcopy(membership)
    enriched["dcf_industry_mapping_status"] = str(
        membership.get("mapping_status") or "unknown"
    )
    return enriched


def _extract_capex_value(bundle: Dict[str, Any]) -> Optional[float]:
    facts = bundle.get("facts") or bundle.get("facts_json") or {}
    if not isinstance(facts, dict):
        return None
    cash_flow = facts.get("cash_flow_sheet") or {}
    if not isinstance(cash_flow, dict):
        return None
    return _safe_float(cash_flow.get("pay_fixed_assets_etc_cash"))


def _capex_success(
    *,
    value: float,
    method: str,
    source_periods: Dict[str, str],
    quality_flag: str,
) -> Dict[str, Any]:
    payload = {
        "status": "success",
        "value": value,
        "method": method,
        "source_field": CAPEX_SOURCE_FIELD,
        "source_periods": source_periods,
        "quality_flag": quality_flag,
        "proxy_scope": "fixed_intangible_and_other_long_term_asset_cash_purchases",
        "warnings": ["capital_expenditure_is_cash_purchase_proxy"],
    }
    payload["lineage_hash"] = _stable_hash(payload)
    return payload


def _capex_gap(
    reason: str,
    *,
    source_periods: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    payload = {
        "status": "unavailable",
        "value": None,
        "method": None,
        "source_field": CAPEX_SOURCE_FIELD,
        "source_periods": source_periods or {},
        "quality_flag": "missing_or_incomplete",
        "missing_reason": reason,
        "warnings": [reason],
    }
    payload["lineage_hash"] = _stable_hash(payload)
    return payload


def _safe_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _normalize_iso_date(value: Any) -> Optional[str]:
    text = str(value or "").strip()[:10]
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _stable_hash(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "A_SHARE_EXCHANGES",
    "CAPEX_SOURCE_FIELD",
    "derive_capital_expenditure",
    "derive_cash_and_debt",
    "enrich_instrument_with_industry",
    "estimate_a_share_filing_deadline",
    "resolve_financial_availability",
    "select_financial_bundle_as_of",
]
