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
    warning_fields = _semantic_warning_fields(fallback)
    if primary is None:
        sanitized_updates = {
            field_name: None
            for field_name in warning_fields
            if getattr(fallback, field_name, None) is not None
        }
        return replace(
            fallback,
            lineage_json={
                **fallback.lineage_json,
                "fallback_used": True,
                "fallback_reason": "missing_primary",
                "fallback_skipped_semantic_warning_fields": sorted(warning_fields),
            },
            **sanitized_updates,
        )

    updates = {}
    filled_fields = []
    skipped_semantic_warning_fields = []
    for field_name in CORE_FACT_FIELDS:
        primary_value = getattr(primary, field_name)
        fallback_value = getattr(fallback, field_name)
        if primary_value is None and fallback_value is not None:
            if field_name in warning_fields:
                skipped_semantic_warning_fields.append(field_name)
                continue
            updates[field_name] = fallback_value
            filled_fields.append(field_name)

    if not updates:
        if skipped_semantic_warning_fields:
            return replace(
                primary,
                lineage_json={
                    **primary.lineage_json,
                    "fallback_used": False,
                    "fallback_source": fallback.source,
                    "fallback_source_mode": fallback.source_mode,
                    "fallback_filled_fields": [],
                    "fallback_skipped_semantic_warning_fields": (
                        skipped_semantic_warning_fields
                    ),
                },
                facts_json={
                    **primary.facts_json,
                    "fallback_merge": {
                        "source": fallback.source,
                        "source_mode": fallback.source_mode,
                        "filled_fields": [],
                        "skipped_semantic_warning_fields": (
                            skipped_semantic_warning_fields
                        ),
                        "policy": policy,
                    },
                },
            )
        return primary

    lineage_json = {
        **primary.lineage_json,
        "fallback_used": True,
        "fallback_source": fallback.source,
        "fallback_source_mode": fallback.source_mode,
        "fallback_filled_fields": filled_fields,
        "fallback_skipped_semantic_warning_fields": skipped_semantic_warning_fields,
    }
    facts_json = {
        **primary.facts_json,
        "fallback_merge": {
            "source": fallback.source,
            "source_mode": fallback.source_mode,
            "filled_fields": filled_fields,
            "skipped_semantic_warning_fields": skipped_semantic_warning_fields,
            "policy": policy,
        },
    }
    return replace(primary, facts_json=facts_json, lineage_json=lineage_json, **updates)


def _semantic_warning_fields(snapshot: FinancialFactsSnapshot) -> set[str]:
    warnings = []
    warnings.extend(snapshot.lineage_json.get("core_fact_warnings") or [])
    warnings.extend(snapshot.facts_json.get("core_fact_warnings") or [])
    return {
        str(item.get("core_field"))
        for item in warnings
        if isinstance(item, dict) and item.get("core_field")
    }
