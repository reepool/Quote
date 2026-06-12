"""
Profile-scoped L1.5 financial fact packs.

The L1 local-core catalog stays focused on cross-industry facts. These packs
expose optional industry facts that are useful for bank/securities/insurance
analysis without turning their absence into a core-readiness blocker.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any, Dict, List, Optional

from research.financial_source_field_mapping import (
    MAPPING_VERSION,
    RELATIONSHIP_DERIVED_EQUIVALENT,
    RELATIONSHIP_EXACT_EQUIVALENT,
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
    relationship: str = RELATIONSHIP_EXACT_EQUIVALENT
    pack_version: str = INDUSTRY_FACT_PACK_VERSION
    mapping_version: str = MAPPING_VERSION
    semantic: str = ""
    required_for_core: bool = False
    raw_fact_names: tuple[str, ...] = ()
    derived_components: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _entry_from_local_core_mapping(
    canonical_fact: str,
    *,
    profile: str = "bank",
) -> IndustryFinancialFactPackEntry:
    mapping: Optional[FinancialSourceFieldMapping] = next(
        (
            item
            for item in get_financial_source_field_mappings(
                profile=profile,
                approved_for_core=True,
                mapping_version=MAPPING_VERSION,
            )
            if item.canonical_fact == canonical_fact
        ),
        None,
    )
    if mapping is None:
        raise ValueError(
            f"{profile} industry fact is not backed by an approved mapping: {canonical_fact}"
        )
    return IndustryFinancialFactPackEntry(
        canonical_fact=mapping.canonical_fact,
        statement_family=mapping.statement_family,
        profile=profile,
        source_mappings={
            "sina_report": mapping.sina_field,
            "ths_report": mapping.ths_metric,
        },
        source_unit=mapping.source_unit,
        canonical_unit=mapping.canonical_unit,
        value_type=mapping.value_type,
        approval_status=PACK_STATUS_APPROVED,
        relationship=mapping.relationship,
        mapping_version=mapping.mapping_version,
        semantic=mapping.semantic,
        evidence=mapping.evidence,
    )


def _raw_entry(
    *,
    canonical_fact: str,
    statement_family: str,
    profile: str,
    source_mappings: Dict[str, str],
    raw_fact_names: tuple[str, ...],
    semantic: str,
    value_type: str = "point_in_time",
    source_unit: str = "CNY",
    canonical_unit: str = "CNY",
) -> IndustryFinancialFactPackEntry:
    return IndustryFinancialFactPackEntry(
        canonical_fact=canonical_fact,
        statement_family=statement_family,
        profile=profile,
        source_mappings=source_mappings,
        source_unit=source_unit,
        canonical_unit=canonical_unit,
        value_type=value_type,
        approval_status=PACK_STATUS_APPROVED,
        relationship=RELATIONSHIP_EXACT_EQUIVALENT,
        semantic=semantic,
        raw_fact_names=raw_fact_names,
        evidence=("financial_industry_fact_pack_raw_exact_fields.v1",),
    )


def _bank_deposit_total_entry() -> IndustryFinancialFactPackEntry:
    return IndustryFinancialFactPackEntry(
        canonical_fact="balance_sheet.deposits_and_deposits",
        statement_family="balance_sheet",
        profile="bank",
        source_mappings={
            "derived": "balance_sheet.customer_deposits + balance_sheet.interbank_deposits",
            "sina_report": "吸收存款及同业存放",
            "ths_report": "deposits_and_deposits",
        },
        source_unit="CNY",
        canonical_unit="CNY",
        value_type="point_in_time",
        approval_status=PACK_STATUS_APPROVED,
        relationship=RELATIONSHIP_DERIVED_EQUIVALENT,
        semantic="customer_deposits_plus_interbank_deposits",
        derived_components=(
            "balance_sheet.customer_deposits",
            "balance_sheet.interbank_deposits",
        ),
        evidence=("cninfo_data20_balance_sheet_raw_bank_deposit_fields",),
    )


_BANK_FACTS_V1 = (
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

_BANK_RAW_AND_DERIVED_FACTS_V1 = (
    _raw_entry(
        canonical_fact="balance_sheet.customer_deposits",
        statement_family="balance_sheet",
        profile="bank",
        source_mappings={"cninfo_data20": "吸收存款"},
        raw_fact_names=("吸收存款",),
        semantic="bank_customer_deposits",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.interbank_deposits",
        statement_family="balance_sheet",
        profile="bank",
        source_mappings={"cninfo_data20": "同业存放及其他金融机构存放款项"},
        raw_fact_names=("同业存放及其他金融机构存放款项",),
        semantic="bank_interbank_and_other_financial_institution_deposits",
    ),
    _bank_deposit_total_entry(),
)

_SECURITIES_LOCAL_CORE_FACTS_V1 = (
    "balance_sheet.general_risk_preparation",
    "balance_sheet.repurchase_financial_assets",
    "balance_sheet.trade_financial_assets",
    "cash_flow_sheet.pay_interest_fee_and_commission_cash",
    "cash_flow_sheet.receive_interest_fee_and_commission_cash",
    "profit_sheet.benefit_credit_impairment_loss",
    "profit_sheet.charges_commissions_expenses",
    "profit_sheet.fair_changes_income",
    "profit_sheet.interest_expenses",
    "profit_sheet.interest_income",
    "profit_sheet.invest_income",
)

_SECURITIES_RAW_FACTS_V1 = (
    _raw_entry(
        canonical_fact="balance_sheet.agent_trading_security",
        statement_family="balance_sheet",
        profile="securities",
        source_mappings={
            "ths_report": "agent_trading_security",
            "cninfo_data20": "代理买卖证券款",
        },
        raw_fact_names=("agent_trading_security", "代理买卖证券款"),
        semantic="securities_customer_brokerage_funds",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.agent_underwriting_security",
        statement_family="balance_sheet",
        profile="securities",
        source_mappings={"ths_report": "agent_underwriting_security"},
        raw_fact_names=("agent_underwriting_security",),
        semantic="securities_underwriting_funds",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.sale_repurchase_financial_assets",
        statement_family="balance_sheet",
        profile="securities",
        source_mappings={
            "ths_report": "sale_repurchase_financial_assets",
            "cninfo_data20": "卖出回购金融资产款",
        },
        raw_fact_names=("sale_repurchase_financial_assets", "卖出回购金融资产款"),
        semantic="securities_sale_repurchase_financial_assets",
    ),
    _raw_entry(
        canonical_fact="profit_sheet.net_fee_commission_income",
        statement_family="income_statement",
        profile="securities",
        source_mappings={"cninfo_data20": "手续费及佣金净收入"},
        raw_fact_names=("手续费及佣金净收入",),
        semantic="securities_net_fee_and_commission_income",
        value_type="period_reported_value",
    ),
    _raw_entry(
        canonical_fact="cash_flow_sheet.repurchase_funds_net_addition",
        statement_family="cash_flow",
        profile="securities",
        source_mappings={"ths_report": "repurchase_funds_net_addition"},
        raw_fact_names=("repurchase_funds_net_addition",),
        semantic="securities_net_cash_increase_from_repurchase_business",
        value_type="period_reported_value",
    ),
)

_SECURITIES_BROKER_RISK_CONTROL_FACTS_V1 = (
    _raw_entry(
        canonical_fact="net_capital",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "净资本"},
        raw_fact_names=("净资本", "net_capital"),
        semantic="regulatory_net_capital",
    ),
    _raw_entry(
        canonical_fact="core_net_capital",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "核心净资本"},
        raw_fact_names=("核心净资本", "core_net_capital"),
        semantic="regulatory_core_net_capital",
    ),
    _raw_entry(
        canonical_fact="subordinated_net_capital",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "附属净资本"},
        raw_fact_names=("附属净资本", "subordinated_net_capital"),
        semantic="regulatory_subordinated_net_capital",
    ),
    _raw_entry(
        canonical_fact="regulatory_net_assets",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "净资产"},
        raw_fact_names=("净资产", "regulatory_net_assets"),
        semantic="regulatory_net_assets",
    ),
    _raw_entry(
        canonical_fact="risk_coverage_ratio",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "风险覆盖率"},
        raw_fact_names=("风险覆盖率", "risk_coverage_ratio"),
        semantic="risk_coverage_ratio",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="capital_leverage_ratio",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "资本杠杆率"},
        raw_fact_names=("资本杠杆率", "capital_leverage_ratio"),
        semantic="capital_leverage_ratio",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="liquidity_coverage_ratio",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "流动性覆盖率"},
        raw_fact_names=("流动性覆盖率", "liquidity_coverage_ratio"),
        semantic="liquidity_coverage_ratio",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="net_stable_funding_ratio",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "净稳定资金率"},
        raw_fact_names=("净稳定资金率", "net_stable_funding_ratio"),
        semantic="net_stable_funding_ratio",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="net_capital_to_net_assets",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "净资本/净资产"},
        raw_fact_names=("净资本/净资产", "净资本与净资产的比例", "net_capital_to_net_assets"),
        semantic="net_capital_to_net_assets",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="net_capital_to_liabilities",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "净资本/负债"},
        raw_fact_names=("净资本/负债", "净资本与负债的比例", "net_capital_to_liabilities"),
        semantic="net_capital_to_liabilities",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="proprietary_equity_securities_to_net_capital",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "自营权益类证券及证券衍生品/净资本"},
        raw_fact_names=(
            "自营权益类证券及证券衍生品/净资本",
            "自营权益类证券及证券衍生品与净资本的比例",
            "proprietary_equity_securities_to_net_capital",
        ),
        semantic="proprietary_equity_securities_and_derivatives_to_net_capital",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="proprietary_non_equity_securities_to_net_capital",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "自营非权益类证券及其衍生品/净资本"},
        raw_fact_names=(
            "自营非权益类证券及其衍生品/净资本",
            "自营非权益类证券及其衍生品与净资本的比例",
            "proprietary_non_equity_securities_to_net_capital",
        ),
        semantic="proprietary_non_equity_securities_and_derivatives_to_net_capital",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
    _raw_entry(
        canonical_fact="margin_financing_to_net_capital",
        statement_family="regulatory_risk_control",
        profile="securities",
        source_mappings={"broker_risk_control": "融资含融券金额/净资本"},
        raw_fact_names=("融资含融券金额/净资本", "margin_financing_to_net_capital"),
        semantic="margin_financing_including_securities_lending_to_net_capital",
        source_unit="ratio",
        canonical_unit="ratio",
    ),
)

_INSURANCE_LOCAL_CORE_FACTS_V1 = (
    "balance_sheet.debt_investment",
    "balance_sheet.general_risk_preparation",
    "balance_sheet.repurchase_financial_assets",
    "balance_sheet.trade_financial_assets",
    "cash_flow_sheet.pay_interest_fee_and_commission_cash",
    "cash_flow_sheet.receive_interest_fee_and_commission_cash",
    "profit_sheet.benefit_credit_impairment_loss",
    "profit_sheet.charges_commissions_expenses",
    "profit_sheet.fair_changes_income",
    "profit_sheet.interest_expenses",
    "profit_sheet.interest_income",
    "profit_sheet.invest_income",
)

_INSURANCE_RAW_FACTS_V1 = (
    _raw_entry(
        canonical_fact="balance_sheet.advance_premiums",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"cninfo_data20": "预收保费"},
        raw_fact_names=("预收保费",),
        semantic="insurance_advance_premiums",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.time_deposits",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"cninfo_data20": "定期存款"},
        raw_fact_names=("定期存款",),
        semantic="insurance_time_deposits",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.sale_repurchase_financial_assets",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={
            "ths_report": "sale_repurchase_financial_assets",
            "cninfo_data20": "卖出回购金融资产款",
        },
        raw_fact_names=("sale_repurchase_financial_assets", "卖出回购金融资产款"),
        semantic="insurance_sale_repurchase_financial_assets",
    ),
    _raw_entry(
        canonical_fact="profit_sheet.withdrawal_insurance_money",
        statement_family="income_statement",
        profile="insurance",
        source_mappings={"ths_report": "withdrawal_insurance_money"},
        raw_fact_names=("withdrawal_insurance_money",),
        semantic="insurance_surrender_benefit_or_withdrawal_payment",
        value_type="period_reported_value",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.bonds_payable",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"ths_report": "bonds_payable"},
        raw_fact_names=("bonds_payable",),
        semantic="insurance_bonds_payable",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.other_debt_investment",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"ths_report": "other_debt_investment"},
        raw_fact_names=("other_debt_investment",),
        semantic="insurance_other_debt_investment",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.other_equity_tools_investment",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"ths_report": "other_equity_tools_investment"},
        raw_fact_names=("other_equity_tools_investment",),
        semantic="insurance_other_equity_instrument_investment",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.derivative_financial_assets",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"ths_report": "derivative_financial_assets"},
        raw_fact_names=("derivative_financial_assets",),
        semantic="insurance_derivative_financial_assets",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.derivative_financial_liabilities",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"ths_report": "derivative_financial_debt"},
        raw_fact_names=("derivative_financial_debt",),
        semantic="insurance_derivative_financial_liabilities",
    ),
    _raw_entry(
        canonical_fact="balance_sheet.trading_financial_liabilities",
        statement_family="balance_sheet",
        profile="insurance",
        source_mappings={"ths_report": "trade_finance_debt"},
        raw_fact_names=("trade_finance_debt",),
        semantic="insurance_trading_financial_liabilities",
    ),
    _raw_entry(
        canonical_fact="profit_sheet.exchange_income",
        statement_family="income_statement",
        profile="insurance",
        source_mappings={"ths_report": "exchange_income"},
        raw_fact_names=("exchange_income",),
        semantic="insurance_exchange_gain_or_loss",
        value_type="period_reported_value",
    ),
    _raw_entry(
        canonical_fact="profit_sheet.associate_invest_income",
        statement_family="income_statement",
        profile="insurance",
        source_mappings={"ths_report": "associate_invest_income"},
        raw_fact_names=("associate_invest_income",),
        semantic="insurance_associate_and_joint_venture_investment_income",
        value_type="period_reported_value",
    ),
    _raw_entry(
        canonical_fact="profit_sheet.assets_impairment_loss",
        statement_family="income_statement",
        profile="insurance",
        source_mappings={"ths_report": "assets_impairment_loss"},
        raw_fact_names=("assets_impairment_loss",),
        semantic="insurance_asset_impairment_loss",
        value_type="period_reported_value",
    ),
)

_PACKS_BY_VERSION: Dict[str, Dict[str, tuple[IndustryFinancialFactPackEntry, ...]]] = {
    INDUSTRY_FACT_PACK_VERSION: {
        "bank": (
            *_BANK_RAW_AND_DERIVED_FACTS_V1,
            *(tuple(_entry_from_local_core_mapping(fact, profile="bank") for fact in _BANK_FACTS_V1)),
        ),
        "securities": (
            *_SECURITIES_RAW_FACTS_V1,
            *_SECURITIES_BROKER_RISK_CONTROL_FACTS_V1,
            *(tuple(
                _entry_from_local_core_mapping(fact, profile="securities")
                for fact in _SECURITIES_LOCAL_CORE_FACTS_V1
            )),
        ),
        "insurance": (
            *_INSURANCE_RAW_FACTS_V1,
            *(tuple(
                _entry_from_local_core_mapping(fact, profile="insurance")
                for fact in _INSURANCE_LOCAL_CORE_FACTS_V1
            )),
        ),
    }
}

_PROFILE_STATUS_BY_VERSION: Dict[str, Dict[str, str]] = {
    INDUSTRY_FACT_PACK_VERSION: {
        "bank": PACK_STATUS_APPROVED,
        "securities": PACK_STATUS_APPROVED,
        "insurance": PACK_STATUS_APPROVED,
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


def get_local_core_industry_canonical_facts(
    *,
    profile: Optional[str],
    pack_version: str = INDUSTRY_FACT_PACK_VERSION,
) -> List[str]:
    """Return pack facts that should be read through local-core lineage."""
    return [
        entry.canonical_fact
        for entry in get_financial_industry_fact_pack(
            profile=profile,
            pack_version=pack_version,
        )
        if not entry.raw_fact_names and not entry.derived_components
    ]


def _row_source_priority(row: Dict[str, Any]) -> tuple[int, str]:
    source = str(row.get("source") or "")
    source_mode = str(row.get("source_mode") or "")
    priority = {
        ("cninfo", "direct"): 0,
        ("akshare", "direct"): 1,
    }.get((source, source_mode), 5)
    return priority, str(row.get("updated_at") or "")


def _best_raw_row(
    rows: List[Dict[str, Any]],
    *,
    fact_names: tuple[str, ...],
) -> Optional[Dict[str, Any]]:
    candidates = [
        row
        for row in rows
        if str(row.get("fact_name") or "") in fact_names
        and _has_valid_numeric_value(row.get("fact_value"))
    ]
    if not candidates:
        return None
    return sorted(candidates, key=_row_source_priority)[0]


def _has_valid_numeric_value(value: Any) -> bool:
    if value is None:
        return False
    try:
        return not math.isnan(float(value))
    except (TypeError, ValueError):
        return True


def _fact_row_from_raw_source(
    *,
    entry: IndustryFinancialFactPackEntry,
    row: Dict[str, Any],
) -> Dict[str, Any]:
    payload = dict(row)
    payload["canonical_fact_name"] = entry.canonical_fact
    payload["canonical_unit"] = entry.canonical_unit
    payload["statement_family"] = entry.statement_family
    raw_fact = dict(payload.get("raw_fact") or {})
    raw_fact["industry_pack_mapping"] = {
        "pack_version": entry.pack_version,
        "relationship": entry.relationship,
        "source_mappings": entry.source_mappings,
        "semantic": entry.semantic,
    }
    payload["raw_fact"] = raw_fact
    return payload


def _derived_fact_row(
    *,
    entry: IndustryFinancialFactPackEntry,
    component_rows: Dict[str, Dict[str, Any]],
    report_period: Optional[str],
) -> Optional[Dict[str, Any]]:
    values = []
    for component in entry.derived_components:
        row = component_rows.get(component)
        if row is None or row.get("fact_value") is None:
            return None
        values.append(float(row["fact_value"]))
    first = component_rows[entry.derived_components[0]]
    return {
        "instrument_id": first.get("instrument_id"),
        "symbol": first.get("symbol"),
        "exchange": first.get("exchange"),
        "report_period": first.get("report_period") or report_period,
        "fact_name": entry.canonical_fact,
        "canonical_fact_name": entry.canonical_fact,
        "statement_family": entry.statement_family,
        "fact_value": sum(values),
        "unit": entry.canonical_unit,
        "canonical_unit": entry.canonical_unit,
        "source": "derived",
        "source_mode": "industry_pack",
        "raw_fact": {
            "industry_pack_mapping": {
                "pack_version": entry.pack_version,
                "relationship": entry.relationship,
                "source_mappings": entry.source_mappings,
                "semantic": entry.semantic,
                "derived_components": list(entry.derived_components),
                "component_values": {
                    component: component_rows[component].get("fact_value")
                    for component in entry.derived_components
                },
                "component_sources": {
                    component: {
                        "source": component_rows[component].get("source"),
                        "source_mode": component_rows[component].get("source_mode"),
                        "fact_name": component_rows[component].get("fact_name"),
                    }
                    for component in entry.derived_components
                },
            }
        },
    }


def build_industry_pack_payload(
    *,
    instrument_id: str,
    report_period: Optional[str],
    profile: Optional[str],
    local_fact_result: Optional[Dict[str, Any]],
    numeric_fact_rows: Optional[List[Dict[str, Any]]] = None,
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
    rows = numeric_fact_rows or []
    if pack_status["status"] == PACK_STATUS_APPROVED:
        entries = get_financial_industry_fact_pack(
            profile=profile,
            pack_version=pack_version,
        )
        for entry in entries:
            if not entry.raw_fact_names or entry.canonical_fact in facts:
                continue
            raw_row = _best_raw_row(rows, fact_names=entry.raw_fact_names)
            if raw_row is not None:
                facts[entry.canonical_fact] = _fact_row_from_raw_source(
                    entry=entry,
                    row=raw_row,
                )
        for entry in entries:
            if not entry.derived_components or entry.canonical_fact in facts:
                continue
            component_rows = {
                component: facts[component]
                for component in entry.derived_components
                if component in facts
            }
            derived = _derived_fact_row(
                entry=entry,
                component_rows=component_rows,
                report_period=source_payload.get("report_period") or report_period,
            )
            if derived is not None:
                facts[entry.canonical_fact] = derived
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
