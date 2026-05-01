"""
Fallback merge helpers for financial core facts.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Dict, Optional

from research.providers.base import FinancialFactsSnapshot


CORE_FACT_FIELDS = (
    "revenue",
    "gross_profit",
    "operating_profit",
    "pre_tax_profit",
    "net_income",
    "operating_cf",
    "total_cf",
    "total_assets",
    "total_liabilities",
    "equity",
    "current_assets",
    "current_liabilities",
    "inventory",
    "receivables",
    "fixed_assets",
    "intangible_assets",
    "shares_outstanding",
)


def merge_financial_core_facts_with_fallback(
    primary: Optional[FinancialFactsSnapshot],
    fallback: FinancialFactsSnapshot,
    *,
    fallback_policy: Optional[Dict[str, object]] = None,
) -> FinancialFactsSnapshot:
    """Fill missing primary core facts from fallback without overwriting official facts."""
    policy = fallback_policy or {}
    if not bool(policy.get("allow_third_party_fallback", True)):
        if primary is None:
            raise ValueError("fallback is disabled and no primary facts were supplied")
        return primary
    if primary is None:
        return replace(
            fallback,
            lineage_json={
                **fallback.lineage_json,
                "fallback_used": True,
                "fallback_reason": "missing_primary",
            },
        )

    updates = {}
    filled_fields = []
    for field_name in CORE_FACT_FIELDS:
        primary_value = getattr(primary, field_name)
        fallback_value = getattr(fallback, field_name)
        if primary_value is None and fallback_value is not None:
            updates[field_name] = fallback_value
            filled_fields.append(field_name)

    if not updates:
        return primary

    lineage_json = {
        **primary.lineage_json,
        "fallback_used": True,
        "fallback_source": fallback.source,
        "fallback_source_mode": fallback.source_mode,
        "fallback_filled_fields": filled_fields,
    }
    facts_json = {
        **primary.facts_json,
        "fallback_merge": {
            "source": fallback.source,
            "source_mode": fallback.source_mode,
            "filled_fields": filled_fields,
            "policy": policy,
        },
    }
    return replace(primary, facts_json=facts_json, lineage_json=lineage_json, **updates)
