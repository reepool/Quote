"""
Versioned source-field mappings for layered financial statements.

The catalog is intentionally conservative: a mapping is usable by the local
core layer only when it has been approved as an exact semantic equivalent, or
as exact after a documented unit conversion.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from typing import Any, Iterable, List, Optional


MAPPING_VERSION_V1 = "sina_ths_core_financial_facts.v1"
MAPPING_VERSION_V2 = "sina_ths_core_financial_facts.v2"
MAPPING_VERSION_V3 = "sina_ths_core_financial_facts.v3"
MAPPING_VERSION_V4 = "sina_ths_core_financial_facts.v4"
MAPPING_VERSION = "sina_ths_core_financial_facts.v5"

FINANCIAL_STATEMENT_PROFILES = ("nonbank", "bank", "securities", "insurance")
PROFILE_BASE_MAPPINGS = {
    "securities": "nonbank",
    "insurance": "nonbank",
}

RELATIONSHIP_EXACT_EQUIVALENT = "exact_equivalent"
RELATIONSHIP_EQUIVALENT_AFTER_UNIT = "equivalent_after_unit"
RELATIONSHIP_DERIVED_EQUIVALENT = "derived_equivalent"
RELATIONSHIP_BROADER_THAN = "broader_than"
RELATIONSHIP_NARROWER_THAN = "narrower_than"
RELATIONSHIP_RELATED_ONLY = "related_only"
RELATIONSHIP_REJECTED = "rejected"
RELATIONSHIP_UNKNOWN_CANDIDATE = "unknown_candidate"

APPROVED_RELATIONSHIPS = {
    RELATIONSHIP_EXACT_EQUIVALENT,
    RELATIONSHIP_EQUIVALENT_AFTER_UNIT,
}


@dataclass(frozen=True)
class FinancialSourceFieldMapping:
    """Auditable relationship between a Sina field and a THS metric."""

    canonical_fact: str
    statement_family: str
    profile: str
    sina_field: str
    ths_metric: str
    relationship: str
    source_unit: str
    canonical_unit: str
    value_type: str
    approved_for_core: bool
    mapping_version: str = MAPPING_VERSION
    unit_multiplier: float = 1.0
    semantic: str = ""
    rejection_reason: Optional[str] = None
    evidence: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _core_mapping(
    *,
    canonical_fact: str,
    statement_family: str,
    sina_field: str,
    ths_metric: str,
    semantic: str,
    profile: str,
    value_type: str = "period_reported_value",
    mapping_version: str = MAPPING_VERSION,
    source_unit: str = "CNY",
    canonical_unit: str = "CNY",
    evidence: tuple[str, ...] = (),
) -> FinancialSourceFieldMapping:
    return FinancialSourceFieldMapping(
        canonical_fact=canonical_fact,
        statement_family=statement_family,
        profile=profile,
        sina_field=sina_field,
        ths_metric=ths_metric,
        relationship=RELATIONSHIP_EXACT_EQUIVALENT,
        source_unit=source_unit,
        canonical_unit=canonical_unit,
        value_type=value_type,
        approved_for_core=True,
        mapping_version=mapping_version,
        semantic=semantic,
        evidence=evidence,
    )


_NONBANK_CORE_MAPPINGS_V1: tuple[FinancialSourceFieldMapping, ...] = (
    _core_mapping(
        canonical_fact="revenue",
        statement_family="income_statement",
        sina_field="营业收入",
        ths_metric="operating_income",
        semantic="operating_revenue",
        profile="nonbank",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="revenue",
        statement_family="income_statement",
        sina_field="营业总收入",
        ths_metric="operating_income_total",
        semantic="total_operating_revenue",
        profile="nonbank",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="operating_profit",
        statement_family="income_statement",
        sina_field="营业利润",
        ths_metric="operating_profit",
        semantic="operating_profit",
        profile="nonbank",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="pre_tax_profit",
        statement_family="income_statement",
        sina_field="利润总额",
        ths_metric="profit_total",
        semantic="pre_tax_profit",
        profile="nonbank",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="net_income_parent",
        statement_family="income_statement",
        sina_field="归属于母公司所有者的净利润",
        ths_metric="parent_holder_net_profit",
        semantic="parent_attributable_net_profit",
        profile="nonbank",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="total_assets",
        statement_family="balance_sheet",
        sina_field="资产总计",
        ths_metric="assets_total",
        semantic="total_assets",
        profile="nonbank",
        value_type="point_in_time",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="total_liabilities",
        statement_family="balance_sheet",
        sina_field="负债合计",
        ths_metric="total_debt",
        semantic="total_liabilities",
        profile="nonbank",
        value_type="point_in_time",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="equity_parent",
        statement_family="balance_sheet",
        sina_field="归属于母公司所有者权益合计",
        ths_metric="parent_holder_equity_total",
        semantic="parent_attributable_equity",
        profile="nonbank",
        value_type="point_in_time",
        mapping_version=MAPPING_VERSION_V1,
    ),
    _core_mapping(
        canonical_fact="operating_cf",
        statement_family="cash_flow",
        sina_field="经营活动产生的现金流量净额",
        ths_metric="act_cash_flow_net",
        semantic="net_cash_flow_from_operating_activities",
        profile="nonbank",
        mapping_version=MAPPING_VERSION_V1,
    ),
)

_BANK_CORE_MAPPINGS_V1: tuple[FinancialSourceFieldMapping, ...] = tuple(
    replace(mapping, profile="bank")
    for mapping in _NONBANK_CORE_MAPPINGS_V1
    if mapping.canonical_fact
    in {
        "revenue",
        "operating_profit",
        "pre_tax_profit",
        "net_income_parent",
        "total_assets",
        "total_liabilities",
        "equity_parent",
        "operating_cf",
    }
)

_REJECTED_MAPPINGS_V1: tuple[FinancialSourceFieldMapping, ...] = (
    FinancialSourceFieldMapping(
        canonical_fact="cash_like_bank_asset",
        statement_family="balance_sheet",
        profile="bank",
        sina_field="现金及存放中央银行款项",
        ths_metric="total_cash",
        relationship=RELATIONSHIP_REJECTED,
        source_unit="CNY",
        canonical_unit="CNY",
        value_type="point_in_time",
        approved_for_core=False,
        mapping_version=MAPPING_VERSION_V1,
        semantic="bank_cash_and_deposits_with_central_bank_vs_generic_cash",
        rejection_reason=(
            "Bank central-bank deposits are not equivalent to a generic cash "
            "or cash-equivalent metric."
        ),
    ),
)

_CATALOG_V1: tuple[FinancialSourceFieldMapping, ...] = (
    *_NONBANK_CORE_MAPPINGS_V1,
    *_BANK_CORE_MAPPINGS_V1,
    *_REJECTED_MAPPINGS_V1,
)

_LIVE_EVIDENCE_20260519 = (
    "/tmp/quote_sina_ths_local_core_live_audit_candidates_20260519.json",
)

_CATALOG_V2_ADDITIONS: tuple[FinancialSourceFieldMapping, ...] = (
    _core_mapping(
        canonical_fact="equity_parent",
        statement_family="balance_sheet",
        sina_field="归属于母公司股东权益合计",
        ths_metric="parent_holder_equity_total",
        semantic="parent_attributable_equity",
        profile="nonbank",
        value_type="point_in_time",
        evidence=_LIVE_EVIDENCE_20260519,
    ),
    _core_mapping(
        canonical_fact="net_income_parent",
        statement_family="income_statement",
        sina_field="归属于母公司的净利润",
        ths_metric="parent_holder_net_profit",
        semantic="parent_attributable_net_profit",
        profile="bank",
        evidence=_LIVE_EVIDENCE_20260519,
    ),
    _core_mapping(
        canonical_fact="equity_parent",
        statement_family="balance_sheet",
        sina_field="归属于母公司股东的权益",
        ths_metric="parent_holder_equity_total",
        semantic="parent_attributable_equity",
        profile="bank",
        value_type="point_in_time",
        evidence=_LIVE_EVIDENCE_20260519,
    ),
)

_LIVE_EVIDENCE_600519_2024 = (
    "/tmp/quote_financial_full_field_clusters_600519_2024_unit_review.json",
)

_CATALOG_V3_REVIEW_ROWS: tuple[tuple[str, str, str, str, str, str, str, str], ...] = (
    ("balance_sheet.accounts_payable", "balance_sheet", "应付账款", "accounts_payable", "accounts_payable", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.accrued_wages", "balance_sheet", "应付职工薪酬", "accrued_wages", "balance_sheet.accrued_wages", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.capital_reserve", "balance_sheet", "资本公积", "capital_reserve", "balance_sheet.capital_reserve", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.cash", "balance_sheet", "货币资金", "cash", "balance_sheet.cash", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.construction_in_process", "balance_sheet", "在建工程", "construction_in_process", "construction_in_process", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.construction_in_process_total", "balance_sheet", "在建工程合计", "construction_process_total", "construction_in_process_total", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.contract_debt", "balance_sheet", "合同负债", "contract_debt", "balance_sheet.contract_debt", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.debt_investment", "balance_sheet", "债权投资", "debt_investment", "balance_sheet.debt_investment", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.deferred_tax_assets", "balance_sheet", "递延所得税资产", "deferred_tax_assets", "balance_sheet.deferred_tax_assets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.deferred_tax_debt", "balance_sheet", "递延所得税负债", "deferred_tax_debt", "balance_sheet.deferred_tax_debt", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.deposits_and_deposits", "balance_sheet", "吸收存款及同业存放", "deposits_and_deposits", "balance_sheet.deposits_and_deposits", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.development_expenditure", "balance_sheet", "开发支出", "development_expenditure", "balance_sheet.development_expenditure", "nonbank", "point_in_time", "CNY"),
    ("fixed_assets", "balance_sheet", "固定资产净额", "fixed_assets_total", "fixed_assets_net", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.general_risk_preparation", "balance_sheet", "一般风险准备", "general_risk_preparation", "balance_sheet.general_risk_preparation", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.investment_property", "balance_sheet", "投资性房地产", "investment_property", "balance_sheet.investment_property", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.leading_funds", "balance_sheet", "拆出资金", "leading_funds", "balance_sheet.leading_funds", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.lease_debt", "balance_sheet", "租赁负债", "lease_debt", "balance_sheet.lease_debt", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.loans_payments_behalf", "balance_sheet", "发放贷款及垫款", "loans_payments_behalf", "balance_sheet.loans_payments_behalf", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.long_term_deferred_expenses", "balance_sheet", "长期待摊费用", "long_term_deferred_expenses", "balance_sheet.long_term_deferred_expenses", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.non_current_asset_due_year", "balance_sheet", "一年内到期的非流动资产", "non_current_asset_due_year", "balance_sheet.non_current_asset_due_year", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.non_current_debt_total", "balance_sheet", "非流动负债合计", "non_current_debt_total", "balance_sheet.non_current_debt_total", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.non_current_nets_total", "balance_sheet", "非流动资产合计", "non_current_nets_total", "balance_sheet.non_current_nets_total", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.notes_receivable", "balance_sheet", "应收票据", "notes_receivable", "balance_sheet.notes_receivable", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_accounts_payable", "balance_sheet", "其他应付款", "other_accounts_payable", "other_accounts_payable", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_comprehensive_income", "balance_sheet", "其他综合收益", "other_comprehensive_income", "balance_sheet.other_comprehensive_income", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_current_assets", "balance_sheet", "其他流动资产", "other_current_assets", "balance_sheet.other_current_assets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_current_debt", "balance_sheet", "其他流动负债", "other_current_debt", "balance_sheet.other_current_debt", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_non_current_assets", "balance_sheet", "其他非流动金融资产", "other_non_current_assets", "balance_sheet.other_non_current_assets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_non_current_nets", "balance_sheet", "其他非流动资产", "other_non_current_nets", "balance_sheet.other_non_current_nets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_payable_total", "balance_sheet", "其他应付款合计", "other_payable_total", "other_payable_total", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_receivable", "balance_sheet", "其他应收款", "other_receivable", "other_receivable", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.other_receivable_total", "balance_sheet", "其他应收款(合计)", "other_receivable_total", "other_receivable_total", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.payable_notes_and_accounts", "balance_sheet", "应付票据及应付账款", "payable_notes_and_accounts", "notes_and_accounts_payable", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.payment_money", "balance_sheet", "预付款项", "payment_money", "balance_sheet.payment_money", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.receivable_notes_and_accounts", "balance_sheet", "应收票据及应收账款", "receivable_notes_and_accounts", "balance_sheet.receivable_notes_and_accounts", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.repurchase_financial_assets", "balance_sheet", "买入返售金融资产", "repurchase_financial_assets", "balance_sheet.repurchase_financial_assets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.right_use_assets", "balance_sheet", "使用权资产", "right_use_assets", "balance_sheet.right_use_assets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.surplus_reserve", "balance_sheet", "盈余公积", "surplus_reserve", "balance_sheet.surplus_reserve", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.taxes_dues", "balance_sheet", "应交税费", "taxes_dues", "balance_sheet.taxes_dues", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.trade_financial_assets", "balance_sheet", "交易性金融资产", "trade_financial_assets", "balance_sheet.trade_financial_assets", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.year_non_current_debt", "balance_sheet", "一年内到期的非流动负债", "year_non_current_debt", "balance_sheet.year_non_current_debt", "nonbank", "point_in_time", "CNY"),
    ("current_assets", "balance_sheet", "流动资产合计", "total_current_assets", "current_assets", "nonbank", "point_in_time", "CNY"),
    ("current_liabilities", "balance_sheet", "流动负债合计", "current_total_debt", "current_liabilities", "nonbank", "point_in_time", "CNY"),
    ("equity_parent", "balance_sheet", "归属于母公司股东权益合计", "parent_holder_equity_total", "equity_parent", "nonbank", "point_in_time", "CNY"),
    ("equity_parent", "balance_sheet", "归属于母公司的股东权益合计", "parent_holder_equity_total", "equity_parent", "nonbank", "point_in_time", "CNY"),
    ("equity_total", "balance_sheet", "所有者权益(或股东权益)合计", "holder_equity_total", "equity_total", "nonbank", "point_in_time", "CNY"),
    ("equity_total", "balance_sheet", "所有者权益合计", "holder_equity_total", "equity_total", "nonbank", "point_in_time", "CNY"),
    ("intangible_assets", "balance_sheet", "无形资产", "intangible_assets", "intangible_assets", "nonbank", "point_in_time", "CNY"),
    ("inventory", "balance_sheet", "存货", "inventory", "inventory", "nonbank", "point_in_time", "CNY"),
    ("minority_equity", "balance_sheet", "少数股东权益", "minority_equity", "minority_equity", "nonbank", "point_in_time", "CNY"),
    ("receivables", "balance_sheet", "应收账款", "accounts_receivable", "receivables", "nonbank", "point_in_time", "CNY"),
    ("share_capital_amount", "balance_sheet", "实收资本(或股本)", "equity", "share_capital_amount", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.liabilities_and_equity_total", "balance_sheet", "负债和所有者权益(或股东权益)总计", "debt_and_equity_total", "liabilities_and_equity_total", "nonbank", "point_in_time", "CNY"),
    ("balance_sheet.liabilities_and_equity_total", "balance_sheet", "负债及股东权益总计", "debt_and_equity_total", "liabilities_and_equity_total", "nonbank", "point_in_time", "CNY"),
    ("total_assets", "balance_sheet", "资产总计", "assets_total", "total_assets", "nonbank", "point_in_time", "CNY"),
    ("total_liabilities", "balance_sheet", "负债合计", "total_debt", "total_liabilities", "nonbank", "point_in_time", "CNY"),
    ("undistributed_profit", "balance_sheet", "未分配利润", "undistributed_profits", "undistributed_profit", "nonbank", "point_in_time", "CNY"),
    ("cash_flow_sheet.act_cash_inflow_total", "cash_flow", "经营活动现金流入小计", "act_cash_inflow_total", "cash_flow_sheet.act_cash_inflow_total", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.act_cash_outflow_total", "cash_flow", "经营活动现金流出小计", "act_cash_outflow_total", "cash_flow_sheet.act_cash_outflow_total", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.add_cash_equivalents_ending_overage", "cash_flow", "现金等价物的期末余额", "add_cash_equivalents_ending_overage", "cash_flow_sheet.add_cash_equivalents_ending_overage", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.add_period_cash_and_equivalents_overage", "cash_flow", "期初现金及现金等价物余额", "add_period_cash_and_equivalents_overage", "cash_flow_sheet.add_period_cash_and_equivalents_overage", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.cash_ending_overage", "cash_flow", "现金的期末余额", "cash_ending_overage", "cash_flow_sheet.cash_ending_overage", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.customer_and_interbank_deposits_addition", "cash_flow", "客户存款和同业存放款项净增加额", "customer_and_interbank_deposits_addition", "cash_flow_sheet.customer_and_interbank_deposits_addition", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.customer_loan_advance_net_addition", "cash_flow", "客户贷款及垫款净增加额", "customer_loan_advance_net_addition", "cash_flow_sheet.customer_loan_advance_net_addition", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.deposits_and_funds_net_addition", "cash_flow", "存放中央银行和同业款项净增加额", "deposits_and_funds_net_addition", "cash_flow_sheet.deposits_and_funds_net_addition", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.employees_cash_payments", "cash_flow", "支付给职工以及为职工支付的现金", "employees_cash_payments", "cash_flow_sheet.employees_cash_payments", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.exchange_rate_cash_influence", "cash_flow", "汇率变动对现金及现金等价物的影响", "exchange_rate_cash_influence", "cash_flow_sheet.exchange_rate_cash_influence", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.financing_cash_flow_net", "cash_flow", "筹资活动产生的现金流量净额", "financing_cash_flow_net", "cash_flow_sheet.financing_cash_flow_net", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.financing_cash_outflow_total", "cash_flow", "筹资活动现金流出小计", "financing_cash_outflow_total", "cash_flow_sheet.financing_cash_outflow_total", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.fixed_assets_net_cash", "cash_flow", "处置固定资产、无形资产和其他长期资产所收回的现金净额", "fixed_assets_net_cash", "cash_flow_sheet.fixed_assets_net_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.invest_cash_flow_net", "cash_flow", "投资活动产生的现金流量净额", "invest_cash_flow_net", "cash_flow_sheet.invest_cash_flow_net", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.invest_cash_inflow_total", "cash_flow", "投资活动现金流入小计", "invest_cash_inflow_total", "cash_flow_sheet.invest_cash_inflow_total", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.invest_cash_outflow_total", "cash_flow", "投资活动现金流出小计", "invest_cash_outflow_total", "cash_flow_sheet.invest_cash_outflow_total", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.less_cash_ending_overage", "cash_flow", "现金的期初余额", "less_cash_ending_overage", "cash_flow_sheet.less_cash_ending_overage", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.less_cash_equivalents_ending_overage", "cash_flow", "现金等价物的期初余额", "less_cash_equivalents_ending_overage", "cash_flow_sheet.less_cash_equivalents_ending_overage", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.paid_purchasing_cash", "cash_flow", "购买商品、接受劳务支付的现金", "paid_purchasing_cash", "cash_flow_sheet.paid_purchasing_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_dividends_profits_interest_cash", "cash_flow", "分配股利、利润或偿付利息所支付的现金", "pay_dividends_profits_interest_cash", "cash_flow_sheet.pay_dividends_profits_interest_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_fixed_assets_etc_cash", "cash_flow", "购建固定资产、无形资产和其他长期资产所支付的现金", "pay_fixed_assets_etc_cash", "cash_flow_sheet.pay_fixed_assets_etc_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_interest_fee_and_commission_cash", "cash_flow", "支付利息、手续费及佣金的现金", "pay_interest_fee_and_commission_cash", "cash_flow_sheet.pay_interest_fee_and_commission_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_invest_cash", "cash_flow", "投资所支付的现金", "pay_invest_cash", "cash_flow_sheet.pay_invest_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_other_invest_cash", "cash_flow", "支付的其他与投资活动有关的现金", "pay_other_invest_cash", "cash_flow_sheet.pay_other_invest_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_other_operating_activity_cash", "cash_flow", "支付的其他与经营活动有关的现金", "pay_other_operating_activity_cash", "cash_flow_sheet.pay_other_operating_activity_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.pay_other_related_financing_cash", "cash_flow", "支付其他与筹资活动有关的现金", "pay_other_related_financing_cash", "cash_flow_sheet.pay_other_related_financing_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.period_cash_and_equivalents_overage", "cash_flow", "期末现金及现金等价物余额", "period_cash_and_equivalents_overage", "cash_flow_sheet.period_cash_and_equivalents_overage", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.receive_interest_fee_and_commission_cash", "cash_flow", "收取利息、手续费及佣金的现金", "receive_interest_fee_and_commission_cash", "cash_flow_sheet.receive_interest_fee_and_commission_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.received_invest_cash", "cash_flow", "收回投资所收到的现金", "received_invest_cash", "cash_flow_sheet.received_invest_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.received_invest_income_cash", "cash_flow", "取得投资收益收到的现金", "received_invest_income_cash", "cash_flow_sheet.received_invest_income_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.received_other_invest_cash", "cash_flow", "收到的其他与投资活动有关的现金", "received_other_invest_cash", "cash_flow_sheet.received_other_invest_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.related_operating_activity_receive_cash", "cash_flow", "收到的其他与经营活动有关的现金", "related_operating_activity_receive_cash", "cash_flow_sheet.related_operating_activity_receive_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.sale_received_cash", "cash_flow", "销售商品、提供劳务收到的现金", "sale_received_cash", "cash_flow_sheet.sale_received_cash", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.subsidiary_pay_holder_dividends", "cash_flow", "子公司支付给少数股东的股利、利润", "subsidiary_pay_holder_dividends", "cash_flow_sheet.subsidiary_pay_holder_dividends", "nonbank", "period_reported_value", "CNY"),
    ("cash_flow_sheet.tax_payments", "cash_flow", "支付的各项税费", "tax_payments", "cash_flow_sheet.tax_payments", "nonbank", "period_reported_value", "CNY"),
    ("operating_cf", "cash_flow", "经营活动产生的现金流量净额", "indirect_act_cash_flow_net", "net_cash_flow_from_operating_activities", "nonbank", "period_reported_value", "CNY"),
    ("total_cf", "cash_flow", "现金及现金等价物净增加额", "indirect_cash_net_addition", "net_increase_in_cash_and_cash_equivalents", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.minority_interest_income", "income_statement", "少数股东损益", "minority_holder_income_loss", "minority_interest_income", "nonbank", "period_reported_value", "CNY"),
    ("net_income_parent", "income_statement", "归属于母公司的净利润", "parent_holder_net_profit", "net_income_parent", "nonbank", "period_reported_value", "CNY"),
    ("net_income_parent", "income_statement", "归属于母公司所有者的净利润", "parent_holder_net_profit", "net_income_parent", "nonbank", "period_reported_value", "CNY"),
    ("net_income_total", "income_statement", "净利润", "net_profit", "total_net_profit", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.continuing_net_profit", "income_statement", "持续经营净利润", "continuing_net_profit", "continuing_net_profit", "nonbank", "period_reported_value", "CNY"),
    ("operating_profit", "income_statement", "营业利润", "operating_profit", "operating_profit", "nonbank", "period_reported_value", "CNY"),
    ("pre_tax_profit", "income_statement", "利润总额", "profit_total", "pre_tax_profit", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.asset_disposal_income", "income_statement", "资产处置收益", "asset_disposal_income", "profit_sheet.asset_disposal_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.basic_eps", "income_statement", "基本每股收益", "basic_eps", "basic_eps", "nonbank", "period_reported_value", "CNY_per_share"),
    ("profit_sheet.benefit_credit_impairment_loss", "income_statement", "信用减值损失", "benefit_credit_impairment_loss", "profit_sheet.benefit_credit_impairment_loss", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.benefit_finance_fee", "income_statement", "财务费用", "benefit_finance_fee", "profit_sheet.benefit_finance_fee", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.charges_commissions_expenses", "income_statement", "手续费及佣金支出", "charges_commissions_expenses", "profit_sheet.charges_commissions_expenses", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.common_profit_total", "income_statement", "综合收益总额", "common_profit_total", "profit_sheet.common_profit_total", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.diluted_eps", "income_statement", "稀释每股收益", "diluted_eps", "diluted_eps", "nonbank", "period_reported_value", "CNY_per_share"),
    ("profit_sheet.fair_changes_income", "income_statement", "公允价值变动收益", "fair_changes_income", "profit_sheet.fair_changes_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.financial_interest_expenses", "income_statement", "利息费用", "financial_interest_expenses", "profit_sheet.financial_interest_expenses", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.income_tax_expense", "income_statement", "所得税费用", "income_tax_expense", "profit_sheet.income_tax_expense", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.interest_expenses", "income_statement", "利息支出", "interest_expenses", "profit_sheet.interest_expenses", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.interest_income", "income_statement", "利息收入", "interest_income", "profit_sheet.interest_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.invest_income", "income_statement", "投资收益", "invest_income", "profit_sheet.invest_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.manage_fee", "income_statement", "管理费用", "manage_fee", "profit_sheet.manage_fee", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.non_operating_expenses", "income_statement", "营业外支出", "non_operating_expenses", "profit_sheet.non_operating_expenses", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.non_operating_income", "income_statement", "营业外收入", "non_operating_income", "profit_sheet.non_operating_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.operating_costs", "income_statement", "营业成本", "operating_costs", "profit_sheet.operating_costs", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.operating_costs_total", "income_statement", "营业总成本", "operating_costs_total", "profit_sheet.operating_costs_total", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.other_comprehensive_income", "income_statement", "其他综合收益", "other_common_profit", "other_comprehensive_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.other_income", "income_statement", "其他收益", "other_income", "profit_sheet.other_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.parent_common_profit_total", "income_statement", "归属于母公司所有者的综合收益总额", "parent_common_profit_total", "profit_sheet.parent_common_profit_total", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.parent_other_comprehensive_income", "income_statement", "归属于母公司所有者的其他综合收益", "parent_other_comprehensive_income", "parent_other_comprehensive_income", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.research_and_development_expenses", "income_statement", "研发费用", "research_and_development_expenses", "profit_sheet.research_and_development_expenses", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.sales_fee", "income_statement", "销售费用", "sales_fee", "profit_sheet.sales_fee", "nonbank", "period_reported_value", "CNY"),
    ("profit_sheet.taxes_and_surcharges", "income_statement", "营业税金及附加", "taxes_and_surcharges", "profit_sheet.taxes_and_surcharges", "nonbank", "period_reported_value", "CNY"),
)


def _review_rows_to_mappings(
    rows: tuple[tuple[str, str, str, str, str, str, str, str], ...],
    *,
    mapping_version: str = MAPPING_VERSION_V3,
) -> tuple[FinancialSourceFieldMapping, ...]:
    mappings: list[FinancialSourceFieldMapping] = []
    for (
        canonical_fact,
        statement_family,
        sina_field,
        ths_metric,
        semantic,
        profile,
        value_type,
        unit,
    ) in rows:
        mappings.append(
            _core_mapping(
                canonical_fact=canonical_fact,
                statement_family=statement_family,
                sina_field=sina_field,
                ths_metric=ths_metric,
                semantic=semantic,
                profile=profile,
                value_type=value_type,
                source_unit=unit,
                canonical_unit=unit,
                mapping_version=mapping_version,
                evidence=_LIVE_EVIDENCE_600519_2024,
            )
        )
    return tuple(mappings)

_CATALOG_V3_REVIEW_MAPPINGS = _review_rows_to_mappings(_CATALOG_V3_REVIEW_ROWS)


def _dedupe_mappings(
    mappings: Iterable[FinancialSourceFieldMapping],
) -> tuple[FinancialSourceFieldMapping, ...]:
    by_key: dict[tuple[str, str, str, str, str], FinancialSourceFieldMapping] = {}
    for mapping in mappings:
        key = (
            mapping.mapping_version,
            mapping.profile,
            mapping.canonical_fact,
            mapping.sina_field,
            mapping.ths_metric,
        )
        by_key[key] = mapping
    return tuple(by_key.values())


_CATALOG_V3 = _dedupe_mappings(
    (
        *(replace(mapping, mapping_version=MAPPING_VERSION_V3) for mapping in _CATALOG_V1),
        *(replace(mapping, mapping_version=MAPPING_VERSION_V3) for mapping in _CATALOG_V2_ADDITIONS),
        *_CATALOG_V3_REVIEW_MAPPINGS,
    )
)


def _clone_nonbank_catalog_to_profiles(
    mappings: Iterable[FinancialSourceFieldMapping],
    *,
    profiles: tuple[str, ...],
    mapping_version: str,
) -> tuple[FinancialSourceFieldMapping, ...]:
    """Materialize reviewed common mappings into each statement-profile catalog."""
    cloned: list[FinancialSourceFieldMapping] = []
    for mapping in mappings:
        if mapping.profile != "nonbank" or not mapping.approved_for_core:
            continue
        for profile in profiles:
            cloned.append(
                replace(
                    mapping,
                    profile=profile,
                    mapping_version=mapping_version,
                    evidence=(
                        *mapping.evidence,
                        "/tmp/quote_sina_ths_local_core_live_audit_v3_coverage_20260520.json",
                    ),
                )
            )
    return tuple(cloned)


_CATALOG_V4_PROFILE_MAPPINGS = _clone_nonbank_catalog_to_profiles(
    _CATALOG_V3,
    profiles=("bank", "securities", "insurance"),
    mapping_version=MAPPING_VERSION_V4,
)


_CATALOG_V4 = _dedupe_mappings(
    (
        *(replace(mapping, mapping_version=MAPPING_VERSION_V4) for mapping in _CATALOG_V3),
        *_CATALOG_V4_PROFILE_MAPPINGS,
    )
)


_LIVE_EVIDENCE_V4_PROFILE_PERIOD_20260520 = (
    "/tmp/quote_sina_ths_local_core_live_audit_v4_profile_period_20260520.json",
)


_CATALOG_V5_BANK_CORRECTIONS: tuple[FinancialSourceFieldMapping, ...] = (
    FinancialSourceFieldMapping(
        canonical_fact="balance_sheet.loans_payments_behalf",
        statement_family="balance_sheet",
        profile="bank",
        sina_field="发放贷款及垫款",
        ths_metric="loans_payments_behalf",
        relationship=RELATIONSHIP_REJECTED,
        source_unit="CNY",
        canonical_unit="CNY",
        value_type="point_in_time",
        approved_for_core=False,
        mapping_version=MAPPING_VERSION,
        semantic="bank_loans_and_advances_gross_vs_net",
        rejection_reason=(
            "Sina gross loans and advances include loan loss allowance; THS "
            "loans_payments_behalf matches Sina net loans and advances."
        ),
        evidence=_LIVE_EVIDENCE_V4_PROFILE_PERIOD_20260520,
    ),
    _core_mapping(
        canonical_fact="balance_sheet.loans_payments_behalf",
        statement_family="balance_sheet",
        sina_field="发放贷款及垫款净额",
        ths_metric="loans_payments_behalf",
        semantic="bank_loans_and_advances_net",
        profile="bank",
        value_type="point_in_time",
        mapping_version=MAPPING_VERSION,
        evidence=_LIVE_EVIDENCE_V4_PROFILE_PERIOD_20260520,
    ),
)


_CATALOG_BY_VERSION: dict[str, tuple[FinancialSourceFieldMapping, ...]] = {
    MAPPING_VERSION_V1: _CATALOG_V1,
    MAPPING_VERSION_V2: (
        *(replace(mapping, mapping_version=MAPPING_VERSION_V2) for mapping in _CATALOG_V1),
        *_CATALOG_V2_ADDITIONS,
    ),
    MAPPING_VERSION_V3: _CATALOG_V3,
    MAPPING_VERSION_V4: _CATALOG_V4,
    MAPPING_VERSION: _dedupe_mappings(
        (
            *(replace(mapping, mapping_version=MAPPING_VERSION) for mapping in _CATALOG_V4),
            *_CATALOG_V5_BANK_CORRECTIONS,
        )
    ),
}


def get_financial_source_field_mappings(
    *,
    profile: Optional[str] = None,
    approved_for_core: Optional[bool] = None,
    mapping_version: str = MAPPING_VERSION,
) -> List[FinancialSourceFieldMapping]:
    """Return source-field mappings filtered by profile and approval status."""
    if mapping_version not in _CATALOG_BY_VERSION:
        raise ValueError(f"Unsupported financial source-field mapping version: {mapping_version}")

    mappings: List[FinancialSourceFieldMapping] = list(_CATALOG_BY_VERSION[mapping_version])
    if profile is not None:
        profile_text = str(profile).strip().lower()
        direct_mappings = [mapping for mapping in mappings if mapping.profile == profile_text]
        if direct_mappings:
            mappings = direct_mappings
        else:
            base_profile = PROFILE_BASE_MAPPINGS.get(profile_text, profile_text)
            mappings = [
                replace(mapping, profile=profile_text)
                if base_profile != profile_text
                else mapping
                for mapping in mappings
                if mapping.profile == base_profile
            ]
    if approved_for_core is not None:
        mappings = [
            mapping
            for mapping in mappings
            if mapping.approved_for_core is approved_for_core
        ]
    return list(mappings)


def find_financial_source_field_mapping(
    *,
    profile: str,
    sina_field: Optional[str] = None,
    ths_metric: Optional[str] = None,
    canonical_fact: Optional[str] = None,
    mapping_version: str = MAPPING_VERSION,
) -> Optional[FinancialSourceFieldMapping]:
    """Find one exact catalog entry by profile and at least one field key."""
    for mapping in get_financial_source_field_mappings(
        profile=profile,
        mapping_version=mapping_version,
    ):
        if sina_field is not None and mapping.sina_field != sina_field:
            continue
        if ths_metric is not None and mapping.ths_metric != ths_metric:
            continue
        if canonical_fact is not None and mapping.canonical_fact != canonical_fact:
            continue
        return mapping
    return None
