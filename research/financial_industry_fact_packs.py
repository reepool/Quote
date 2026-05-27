"""
Profile-scoped L1.5 financial fact packs.

The L1 local-core catalog stays focused on cross-industry facts. These packs
expose optional industry facts that are useful for bank/securities/insurance
analysis without turning their absence into a core-readiness blocker.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from research.financial_source_field_mapping import (
    MAPPING_VERSION,
    FinancialSourceFieldMapping,
    get_financial_source_field_mappings,
)


INDUSTRY_FACT_PACK_VERSION = "sina_ths_industry_financial_facts.v1"
INDUSTRY_FACT_PACK_PROFILES = ("bank", "securities", "insurance")

PACK_STATUS_APPROVED = "approved"
PACK_STATUS_NOT_YET_APPROVED = "not_yet_approved"
PACK_STATUS_UNSUPPORTED_PROFILE = "unsupported_profile"


@dataclass(frozen=True)
class IndustryFinancialFactPackEntry:
    """Approved optional industry fact exposed above the common L1 layer."""

    canonical_fact: str
    statement_family: str
    profile: str
    source_mappings: Dict[str, str]
    source_unit: str
    canonical_unit: str
    value_type: str
    approval_status: str
    pack_version: str = INDUSTRY_FACT_PACK_VERSION
    mapping_version: str = MAPPING_VERSION
    semantic: str = ""
    required_for_core: bool = False
    evidence: tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _entry_from_local_core_mapping(canonical_fact: str) -> IndustryFinancialFactPackEntry:
    mapping: Optional[FinancialSourceFieldMapping] = next(
        (
            item
            for item in get_financial_source_field_mappings(
                profile="bank",
                approved_for_core=True,
                mapping_version=MAPPING_VERSION,
            )
            if item.canonical_fact == canonical_fact
        ),
        None,
    )
    if mapping is None:
        raise ValueError(f"Bank industry fact is not backed by an approved mapping: {canonical_fact}")
    return IndustryFinancialFactPackEntry(
        canonical_fact=mapping.canonical_fact,
        statement_family=mapping.statement_family,
        profile="bank",
        source_mappings={
            "sina_report": mapping.sina_field,
            "ths_report": mapping.ths_metric,
        },
        source_unit=mapping.source_unit,
        canonical_unit=mapping.canonical_unit,
        value_type=mapping.value_type,
        approval_status=PACK_STATUS_APPROVED,
        mapping_version=mapping.mapping_version,
        semantic=mapping.semantic,
        evidence=mapping.evidence,
    )


_BANK_FACTS_V1 = (
    "balance_sheet.deposits_and_deposits",
    "balance_sheet.loans_payments_behalf",
    "profit_sheet.interest_income",
    "profit_sheet.interest_expenses",
    "profit_sheet.charges_commissions_expenses",
    "profit_sheet.benefit_credit_impairment_loss",
    "cash_flow_sheet.customer_and_interbank_deposits_addition",
    "cash_flow_sheet.customer_loan_advance_net_addition",
    "cash_flow_sheet.deposits_and_funds_net_addition",
    "cash_flow_sheet.receive_interest_fee_and_commission_cash",
    "cash_flow_sheet.pay_interest_fee_and_commission_cash",
)

_PACKS_BY_VERSION: Dict[str, Dict[str, tuple[IndustryFinancialFactPackEntry, ...]]] = {
    INDUSTRY_FACT_PACK_VERSION: {
        "bank": tuple(_entry_from_local_core_mapping(fact) for fact in _BANK_FACTS_V1),
        "securities": tuple(),
        "insurance": tuple(),
    }
}

_PROFILE_STATUS_BY_VERSION: Dict[str, Dict[str, str]] = {
    INDUSTRY_FACT_PACK_VERSION: {
        "bank": PACK_STATUS_APPROVED,
        "securities": PACK_STATUS_NOT_YET_APPROVED,
        "insurance": PACK_STATUS_NOT_YET_APPROVED,
    }
}


def get_financial_industry_fact_pack(
    *,
    profile: Optional[str],
    pack_version: str = INDUSTRY_FACT_PACK_VERSION,
    approved_only: bool = True,
) -> List[IndustryFinancialFactPackEntry]:
    """Return optional industry fact-pack entries for a statement profile."""
    if pack_version not in _PACKS_BY_VERSION:
        raise ValueError(f"Unsupported financial industry fact pack version: {pack_version}")
    profile_text = str(profile or "").strip().lower()
    entries = list(_PACKS_BY_VERSION[pack_version].get(profile_text, tuple()))
    if approved_only:
        entries = [
            entry
            for entry in entries
            if entry.approval_status == PACK_STATUS_APPROVED
        ]
    return entries


def get_financial_industry_fact_pack_status(
    *,
    profile: Optional[str],
    pack_version: str = INDUSTRY_FACT_PACK_VERSION,
) -> Dict[str, Any]:
    """Return profile pack status without implying a data-source failure."""
    if pack_version not in _PROFILE_STATUS_BY_VERSION:
        raise ValueError(f"Unsupported financial industry fact pack version: {pack_version}")
    profile_text = str(profile or "").strip().lower()
    status = _PROFILE_STATUS_BY_VERSION[pack_version].get(
        profile_text,
        PACK_STATUS_UNSUPPORTED_PROFILE,
    )
    return {
        "profile": profile_text or None,
        "pack_version": pack_version,
        "status": status,
        "approved_fact_count": len(
            get_financial_industry_fact_pack(
                profile=profile_text,
                pack_version=pack_version,
            )
        )
        if status == PACK_STATUS_APPROVED
        else 0,
    }


def get_approved_industry_canonical_facts(
    *,
    profile: Optional[str],
    pack_version: str = INDUSTRY_FACT_PACK_VERSION,
) -> List[str]:
    """Return approved industry canonical facts for read-service requests."""
    return [
        entry.canonical_fact
        for entry in get_financial_industry_fact_pack(
            profile=profile,
            pack_version=pack_version,
        )
    ]


def build_industry_pack_payload(
    *,
    instrument_id: str,
    report_period: Optional[str],
    profile: Optional[str],
    local_fact_result: Optional[Dict[str, Any]],
    pack_version: str = INDUSTRY_FACT_PACK_VERSION,
) -> Dict[str, Any]:
    """Build a non-blocking L1.5 service-layer payload from local canonical facts."""
    pack_status = get_financial_industry_fact_pack_status(
        profile=profile,
        pack_version=pack_version,
    )
    approved_facts = get_approved_industry_canonical_facts(
        profile=profile,
        pack_version=pack_version,
    )
    source_payload = local_fact_result or {}
    facts = {
        fact: row
        for fact, row in (source_payload.get("facts") or {}).items()
        if fact in approved_facts
    }
    missing_fields: List[Dict[str, Any]] = []
    if pack_status["status"] != PACK_STATUS_APPROVED:
        missing_fields.append(
            {
                "canonical_fact": None,
                "reason": "industry_pack_not_yet_approved"
                if pack_status["status"] == PACK_STATUS_NOT_YET_APPROVED
                else "industry_pack_unsupported_profile",
                "profile": pack_status["profile"],
                "pack_version": pack_version,
                "report_period": report_period,
            }
        )
    else:
        for fact in approved_facts:
            if fact not in facts:
                missing_fields.append(
                    {
                        "canonical_fact": fact,
                        "reason": "industry_pack_missing",
                        "profile": pack_status["profile"],
                        "pack_version": pack_version,
                        "report_period": source_payload.get("report_period") or report_period,
                    }
                )

    return {
        "status": "passed" if not missing_fields else "partial",
        "ready": not missing_fields,
        "is_optional": True,
        "instrument_id": instrument_id,
        "report_period": source_payload.get("report_period") or report_period,
        "profile": pack_status["profile"],
        "pack_version": pack_version,
        "mapping_version": source_payload.get("mapping_version") or MAPPING_VERSION,
        "approved_canonical_facts": approved_facts,
        "facts": facts,
        "missing_fields": missing_fields,
        "profile_pack_status": pack_status,
    }
