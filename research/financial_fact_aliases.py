"""
Versioned aliases for deriving core financial facts from parsed numeric facts.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


CORE_FINANCIAL_FACT_ALIASES_V1: Dict[str, List[str]] = {
    "revenue": [
        "Revenue",
        "OperatingRevenue",
        "TotalOperatingRevenue",
        "TOTAL_OPERATE_INCOME",
        "营业收入",
        "营业总收入",
    ],
    "net_income": [
        "NetProfitAttributableToOwnersOfParent",
        "PARENT_NETPROFIT",
        "归属于母公司股东的净利润",
        "归属于母公司的净利润",
        "归属于母公司所有者的净利润",
        "归属母公司净利润",
        "NetProfit",
        "NETPROFIT",
        "净利润",
    ],
    "equity": [
        "EquityAttributableToOwnersOfParent",
        "归属于母公司股东权益合计",
        "归属于母公司股东的权益",
        "归属于母公司所有者权益合计",
        "归属于母公司所有者权益",
        "PARENT_EQUITY",
        "TOTAL_PARENT_EQUITY",
        "TotalEquity",
        "TOTAL_EQUITY",
        "股东权益合计",
        "所有者权益合计",
        "所有者权益",
        "净资产",
    ],
    "total_assets": [
        "Assets",
        "TotalAssets",
        "TOTAL_ASSETS",
        "资产总计",
        "总资产",
    ],
    "total_liabilities": [
        "Liabilities",
        "TotalLiabilities",
        "TOTAL_LIABILITIES",
        "负债合计",
        "总负债",
    ],
    "operating_cf": [
        "NetCashFlowsFromOperatingActivities",
        "NETCASH_OPERATE",
        "经营活动产生的现金流量净额",
    ],
    "shares_outstanding": [
        "SharesOutstanding",
        "TotalSharesOutstanding",
    ],
}


_CANONICAL_SEMANTICS_V1: Dict[str, str] = {
    "net_income": "parent_attributable_net_profit",
    "equity": "parent_attributable_equity",
}


_ALIAS_SEMANTICS_V1: Dict[str, Dict[str, str]] = {
    "net_income": {
        "NetProfitAttributableToOwnersOfParent": "parent_attributable_net_profit",
        "PARENT_NETPROFIT": "parent_attributable_net_profit",
        "parent_holder_net_profit": "parent_attributable_net_profit",
        "S2020_0310": "parent_attributable_net_profit",
        "归属于母公司股东的净利润": "parent_attributable_net_profit",
        "归属于母公司的净利润": "parent_attributable_net_profit",
        "归属于母公司所有者的净利润": "parent_attributable_net_profit",
        "归属母公司净利润": "parent_attributable_net_profit",
        "NetProfit": "total_net_profit",
        "NETPROFIT": "total_net_profit",
        "net_profit": "total_net_profit",
        "S2020_0300": "total_net_profit",
        "净利润": "total_net_profit",
    },
    "equity": {
        "EquityAttributableToOwnersOfParent": "parent_attributable_equity",
        "PARENT_EQUITY": "parent_attributable_equity",
        "TOTAL_PARENT_EQUITY": "parent_attributable_equity",
        "parent_holder_equity_total": "parent_attributable_equity",
        "S2010_0770": "parent_attributable_equity",
        "归属于母公司股东权益合计": "parent_attributable_equity",
        "归属于母公司股东的权益": "parent_attributable_equity",
        "归属于母公司所有者权益合计": "parent_attributable_equity",
        "归属于母公司所有者权益": "parent_attributable_equity",
        "TotalEquity": "total_owners_equity",
        "TOTAL_EQUITY": "total_owners_equity",
        "S2010_0790": "total_owners_equity",
        "股东权益合计": "total_owners_equity",
        "所有者权益合计": "total_owners_equity",
        "所有者权益": "total_owners_equity",
        "净资产": "total_owners_equity",
    },
}


_SEMANTIC_WARNING_CODES_V1: Dict[tuple[str, str], str] = {
    ("net_income", "total_net_profit"): "net_income_total_vs_parent_ambiguous",
    ("equity", "total_owners_equity"): "equity_total_vs_parent_ambiguous",
}


CORE_FINANCIAL_FACT_DERIVATION_RULES_V1: Dict[str, List[Dict[str, Any]]] = {
    "net_income": [
        {
            "method": "total_net_profit_minus_minority_interest_income",
            "components": {
                "total": [
                    "NetProfit",
                    "NETPROFIT",
                    "S2020_0300",
                    "净利润",
                ],
                "minority": [
                    "MinorityInterestIncome",
                    "MINORITY_INTEREST_INCOME",
                    "S2020_0320",
                    "少数股东损益",
                    "少数股东利润",
                    "少数股东收益",
                ],
            },
        }
    ],
    "equity": [
        {
            "method": "total_owners_equity_minus_minority_interest",
            "components": {
                "total": [
                    "TotalEquity",
                    "TOTAL_EQUITY",
                    "S2010_0790",
                    "股东权益合计",
                    "所有者权益合计",
                    "所有者权益",
                    "净资产",
                ],
                "minority": [
                    "MinorityInterest",
                    "MINORITY_EQUITY",
                    "MINORITY_INTEREST",
                    "S2010_0780",
                    "少数股东权益",
                    "少数股东权益合计",
                ],
            },
        }
    ],
}


STANDARD_FINANCIAL_FACT_CATALOG_VERSION = "standard_financial_numeric_facts.v1"


_STANDARD_FINANCIAL_FACT_CATALOG_V1: Dict[str, Dict[str, Any]] = {
    "revenue": {
        "statement_family": "income_statement",
        "semantic": "operating_revenue",
        "unit": "CNY",
        "aliases": [
            "Revenue",
            "OperatingRevenue",
            "TotalOperatingRevenue",
            "TOTAL_OPERATE_INCOME",
            "operating_income_total",
            "operating_income",
            "S2020_0010",
            "S2020_0020",
            "营业收入",
            "营业总收入",
        ],
    },
    "operating_profit": {
        "statement_family": "income_statement",
        "semantic": "operating_profit",
        "unit": "CNY",
        "aliases": ["OperatingProfit", "OPERATE_PROFIT", "operating_profit", "S2020_0240", "营业利润"],
    },
    "pre_tax_profit": {
        "statement_family": "income_statement",
        "semantic": "pre_tax_profit",
        "unit": "CNY",
        "aliases": ["ProfitBeforeTax", "TOTAL_PROFIT", "profit_total", "S2020_0280", "利润总额"],
    },
    "net_income_parent": {
        "statement_family": "income_statement",
        "semantic": "parent_attributable_net_profit",
        "unit": "CNY",
        "aliases": [
            "NetProfitAttributableToOwnersOfParent",
            "PARENT_NETPROFIT",
            "parent_holder_net_profit",
            "S2020_0310",
            "归属于母公司股东的净利润",
            "归属于母公司的净利润",
            "归属于母公司所有者的净利润",
            "归属母公司净利润",
        ],
    },
    "net_income_total": {
        "statement_family": "income_statement",
        "semantic": "total_net_profit",
        "unit": "CNY",
        "aliases": ["NetProfit", "NETPROFIT", "net_profit", "S2020_0300", "净利润"],
    },
    "minority_interest_income": {
        "statement_family": "income_statement",
        "semantic": "minority_interest_income",
        "unit": "CNY",
        "aliases": [
            "MinorityInterestIncome",
            "MINORITY_INTEREST_INCOME",
            "minority_holder_income_loss",
            "S2020_0320",
            "少数股东损益",
            "少数股东利润",
            "少数股东收益",
        ],
    },
    "total_assets": {
        "statement_family": "balance_sheet",
        "semantic": "total_assets",
        "unit": "CNY",
        "aliases": ["Assets", "TotalAssets", "TOTAL_ASSETS", "assets_total", "S2010_0380", "资产总计", "总资产"],
    },
    "total_liabilities": {
        "statement_family": "balance_sheet",
        "semantic": "total_liabilities",
        "unit": "CNY",
        "aliases": ["Liabilities", "TotalLiabilities", "TOTAL_LIABILITIES", "total_debt", "S2010_0690", "负债合计", "总负债"],
    },
    "equity_parent": {
        "statement_family": "balance_sheet",
        "semantic": "parent_attributable_equity",
        "unit": "CNY",
        "aliases": [
            "EquityAttributableToOwnersOfParent",
            "PARENT_EQUITY",
            "TOTAL_PARENT_EQUITY",
            "parent_holder_equity_total",
            "S2010_0770",
            "归属于母公司股东权益合计",
            "归属于母公司股东的权益",
            "归属于母公司所有者权益合计",
            "归属于母公司所有者权益",
        ],
    },
    "equity_total": {
        "statement_family": "balance_sheet",
        "semantic": "total_owners_equity",
        "unit": "CNY",
        "aliases": [
            "TotalEquity",
            "TOTAL_EQUITY",
            "S2010_0790",
            "股东权益合计",
            "所有者权益合计",
            "所有者权益",
            "净资产",
        ],
    },
    "minority_equity": {
        "statement_family": "balance_sheet",
        "semantic": "minority_interest",
        "unit": "CNY",
        "aliases": [
            "MinorityInterest",
            "MINORITY_EQUITY",
            "MINORITY_INTEREST",
            "minority_equity",
            "S2010_0780",
            "少数股东权益",
            "少数股东权益合计",
        ],
    },
    "current_assets": {
        "statement_family": "balance_sheet",
        "semantic": "current_assets",
        "unit": "CNY",
        "aliases": ["CurrentAssets", "TOTAL_CURRENT_ASSETS", "total_current_assets", "流动资产", "流动资产合计"],
    },
    "current_liabilities": {
        "statement_family": "balance_sheet",
        "semantic": "current_liabilities",
        "unit": "CNY",
        "aliases": ["CurrentLiabilities", "TOTAL_CURRENT_LIABILITIES", "current_total_debt", "流动负债", "流动负债合计"],
    },
    "inventory": {
        "statement_family": "balance_sheet",
        "semantic": "inventory",
        "unit": "CNY",
        "aliases": ["Inventory", "INVENTORY", "inventories", "存货"],
    },
    "receivables": {
        "statement_family": "balance_sheet",
        "semantic": "receivables",
        "unit": "CNY",
        "aliases": ["AccountsReceivable", "ACCOUNTS_RECEIVABLE", "account_receivable", "应收账款"],
    },
    "fixed_assets": {
        "statement_family": "balance_sheet",
        "semantic": "fixed_assets",
        "unit": "CNY",
        "aliases": ["FixedAssets", "FIXED_ASSETS", "fixed_assets", "固定资产"],
    },
    "intangible_assets": {
        "statement_family": "balance_sheet",
        "semantic": "intangible_assets",
        "unit": "CNY",
        "aliases": ["IntangibleAssets", "INTANGIBLE_ASSETS", "intangible_assets", "无形资产"],
    },
    "share_capital_amount": {
        "statement_family": "balance_sheet",
        "semantic": "paid_in_capital_amount",
        "unit": "CNY",
        "aliases": ["ShareCapital", "TOTAL_SHARE", "SHARE_CAPITAL", "S2010_0700", "总股本", "实收资本（或股本）"],
    },
    "shares_outstanding": {
        "statement_family": "capital_structure",
        "semantic": "shares_outstanding",
        "unit": "shares",
        "aliases": ["SharesOutstanding", "TotalSharesOutstanding"],
    },
    "undistributed_profit": {
        "statement_family": "balance_sheet",
        "semantic": "undistributed_profit",
        "unit": "CNY",
        "aliases": ["UndistributedProfit", "RETAINED_EARNINGS", "未分配利润"],
    },
    "operating_cf": {
        "statement_family": "cash_flow",
        "semantic": "net_cash_flow_from_operating_activities",
        "unit": "CNY",
        "aliases": [
            "NetCashFlowsFromOperatingActivities",
            "NETCASH_OPERATE",
            "act_cash_flow_net",
            "S2030_0250",
            "经营活动产生的现金流量净额",
        ],
    },
    "total_cf": {
        "statement_family": "cash_flow",
        "semantic": "net_increase_in_cash_and_cash_equivalents",
        "unit": "CNY",
        "aliases": ["NetIncreaseInCashAndCashEquivalents", "cash_net_addition", "S2030_0520", "现金及现金等价物净增加额"],
    },
    "net_capital": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_net_capital",
        "unit": "CNY",
        "aliases": ["net_capital", "净资本"],
    },
    "core_net_capital": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_core_net_capital",
        "unit": "CNY",
        "aliases": ["core_net_capital", "核心净资本"],
    },
    "subordinated_net_capital": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_subordinated_net_capital",
        "unit": "CNY",
        "aliases": ["subordinated_net_capital", "附属净资本"],
    },
    "regulatory_net_assets": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_net_assets",
        "unit": "CNY",
        "aliases": ["regulatory_net_assets"],
    },
    "risk_capital_reserve_total": {
        "statement_family": "regulatory_risk_control",
        "semantic": "total_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["risk_capital_reserve_total", "各项风险资本准备之和", "风险资本准备之和"],
    },
    "market_risk_capital_reserve": {
        "statement_family": "regulatory_risk_control",
        "semantic": "market_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["market_risk_capital_reserve", "市场风险资本准备"],
    },
    "credit_risk_capital_reserve": {
        "statement_family": "regulatory_risk_control",
        "semantic": "credit_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["credit_risk_capital_reserve", "信用风险资本准备"],
    },
    "operational_risk_capital_reserve": {
        "statement_family": "regulatory_risk_control",
        "semantic": "operational_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["operational_risk_capital_reserve", "操作风险资本准备"],
    },
    "balance_sheet_assets_total": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_balance_sheet_assets_total",
        "unit": "CNY",
        "aliases": ["balance_sheet_assets_total", "表内外资产总额", "表内资产总额"],
    },
    "off_balance_sheet_assets_total": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_off_balance_sheet_assets_total",
        "unit": "CNY",
        "aliases": ["off_balance_sheet_assets_total", "表外资产总额"],
    },
    "risk_coverage_ratio": {
        "statement_family": "regulatory_risk_control",
        "semantic": "risk_coverage_ratio",
        "unit": "ratio",
        "aliases": ["risk_coverage_ratio", "风险覆盖率"],
    },
    "capital_leverage_ratio": {
        "statement_family": "regulatory_risk_control",
        "semantic": "capital_leverage_ratio",
        "unit": "ratio",
        "aliases": ["capital_leverage_ratio", "资本杠杆率"],
    },
    "liquidity_coverage_ratio": {
        "statement_family": "regulatory_risk_control",
        "semantic": "liquidity_coverage_ratio",
        "unit": "ratio",
        "aliases": ["liquidity_coverage_ratio", "流动性覆盖率"],
    },
    "net_stable_funding_ratio": {
        "statement_family": "regulatory_risk_control",
        "semantic": "net_stable_funding_ratio",
        "unit": "ratio",
        "aliases": ["net_stable_funding_ratio", "净稳定资金率"],
    },
    "net_capital_to_net_assets": {
        "statement_family": "regulatory_risk_control",
        "semantic": "net_capital_to_net_assets",
        "unit": "ratio",
        "aliases": ["net_capital_to_net_assets", "净资本/净资产", "净资本与净资产的比例"],
    },
    "net_capital_to_liabilities": {
        "statement_family": "regulatory_risk_control",
        "semantic": "net_capital_to_liabilities",
        "unit": "ratio",
        "aliases": ["net_capital_to_liabilities", "净资本/负债", "净资本与负债的比例"],
    },
    "net_assets_to_liabilities": {
        "statement_family": "regulatory_risk_control",
        "semantic": "net_assets_to_liabilities",
        "unit": "ratio",
        "aliases": ["net_assets_to_liabilities", "净资产/负债", "净资产与负债的比例"],
    },
    "proprietary_equity_securities_to_net_capital": {
        "statement_family": "regulatory_risk_control",
        "semantic": "proprietary_equity_securities_and_derivatives_to_net_capital",
        "unit": "ratio",
        "aliases": ["proprietary_equity_securities_to_net_capital", "自营权益类证券及其衍生品/净资本", "自营权益类证券及证券衍生品/净资本"],
    },
    "proprietary_non_equity_securities_to_net_capital": {
        "statement_family": "regulatory_risk_control",
        "semantic": "proprietary_non_equity_securities_and_derivatives_to_net_capital",
        "unit": "ratio",
        "aliases": ["proprietary_non_equity_securities_to_net_capital", "自营非权益类证券及其衍生品/净资本", "自营非权益类证券及证券衍生品/净资本"],
    },
    "margin_financing_to_net_capital": {
        "statement_family": "regulatory_risk_control",
        "semantic": "margin_financing_including_securities_lending_to_net_capital",
        "unit": "ratio",
        "aliases": ["margin_financing_to_net_capital", "融资（含融券）的金额/净资本", "融资含融券的金额/净资本", "融资融券金额/净资本"],
    },
    "high_quality_liquid_assets": {
        "statement_family": "regulatory_risk_control",
        "semantic": "high_quality_liquid_assets",
        "unit": "CNY",
        "aliases": ["high_quality_liquid_assets", "优质流动性资产"],
    },
    "net_cash_outflow": {
        "statement_family": "regulatory_risk_control",
        "semantic": "net_cash_outflow",
        "unit": "CNY",
        "aliases": ["net_cash_outflow", "未来30日现金净流出量", "现金净流出量"],
    },
    "available_stable_funding": {
        "statement_family": "regulatory_risk_control",
        "semantic": "available_stable_funding",
        "unit": "CNY",
        "aliases": ["available_stable_funding", "可用稳定资金"],
    },
    "required_stable_funding": {
        "statement_family": "regulatory_risk_control",
        "semantic": "required_stable_funding",
        "unit": "CNY",
        "aliases": ["required_stable_funding", "所需稳定资金"],
    },
    "single_client_concentration_ratio": {
        "statement_family": "regulatory_risk_control",
        "semantic": "single_client_concentration_ratio",
        "unit": "ratio",
        "aliases": ["single_client_concentration_ratio", "单一客户相关风险占净资本比例"],
    },
    "single_security_concentration_ratio": {
        "statement_family": "regulatory_risk_control",
        "semantic": "single_security_concentration_ratio",
        "unit": "ratio",
        "aliases": ["single_security_concentration_ratio", "单一证券相关风险占净资本比例"],
    },
    "broker_operational_risk_brokerage_net_revenue": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_operational_risk_brokerage_net_revenue",
        "unit": "CNY",
        "aliases": ["broker_operational_risk_brokerage_net_revenue", "经纪业务净收入"],
    },
    "broker_operational_risk_investment_banking_net_revenue": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_operational_risk_investment_banking_net_revenue",
        "unit": "CNY",
        "aliases": ["broker_operational_risk_investment_banking_net_revenue", "投资银行业务净收入"],
    },
    "broker_operational_risk_asset_management_net_revenue": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_operational_risk_asset_management_net_revenue",
        "unit": "CNY",
        "aliases": ["broker_operational_risk_asset_management_net_revenue", "资产管理业务净收入"],
    },
    "broker_operational_risk_proprietary_net_revenue": {
        "statement_family": "regulatory_risk_control",
        "semantic": "regulatory_operational_risk_proprietary_net_revenue",
        "unit": "CNY",
        "aliases": ["broker_operational_risk_proprietary_net_revenue", "证券自营业务净收入", "自营业务净收入"],
    },
}


def _standard_fact_alias_index() -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for canonical_name, metadata in _STANDARD_FINANCIAL_FACT_CATALOG_V1.items():
        for alias in metadata.get("aliases", []):
            index[str(alias)] = {
                "canonical_fact_name": canonical_name,
                "canonical_statement_family": metadata["statement_family"],
                "canonical_semantic": metadata["semantic"],
                "canonical_unit": metadata["unit"],
                "canonical_version": STANDARD_FINANCIAL_FACT_CATALOG_VERSION,
            }
    return index


_STANDARD_FACT_ALIAS_INDEX_V1 = _standard_fact_alias_index()


def describe_financial_numeric_fact_name(
    fact_name: str,
    *,
    version: str = STANDARD_FINANCIAL_FACT_CATALOG_VERSION,
) -> Dict[str, Any]:
    """Return standardized metadata for a parsed long-form financial fact name."""
    if version != STANDARD_FINANCIAL_FACT_CATALOG_VERSION:
        raise ValueError(f"Unsupported financial numeric fact catalog version: {version}")
    return dict(_STANDARD_FACT_ALIAS_INDEX_V1.get(str(fact_name), {}))


def get_standard_financial_fact_catalog(
    version: str = STANDARD_FINANCIAL_FACT_CATALOG_VERSION,
) -> Dict[str, Dict[str, Any]]:
    """Return a copy of the canonical long-form financial fact catalog."""
    if version != STANDARD_FINANCIAL_FACT_CATALOG_VERSION:
        raise ValueError(f"Unsupported financial numeric fact catalog version: {version}")
    return {
        canonical_name: {
            **metadata,
            "aliases": list(metadata.get("aliases", [])),
        }
        for canonical_name, metadata in _STANDARD_FINANCIAL_FACT_CATALOG_V1.items()
    }


def get_standard_financial_fact_aliases(
    canonical_fact_name: Optional[str] = None,
    *,
    version: str = STANDARD_FINANCIAL_FACT_CATALOG_VERSION,
) -> Dict[str, List[str]] | List[str]:
    """Return source-native aliases from the centralized standard catalog."""
    catalog = get_standard_financial_fact_catalog(version)
    if canonical_fact_name is not None:
        metadata = catalog.get(str(canonical_fact_name))
        return [] if metadata is None else list(metadata.get("aliases", []))
    return {
        name: list(metadata.get("aliases", []))
        for name, metadata in catalog.items()
    }


def get_standard_financial_fact_names(
    *,
    statement_family: Optional[str] = None,
    version: str = STANDARD_FINANCIAL_FACT_CATALOG_VERSION,
) -> List[str]:
    """Return stable canonical fact names, optionally scoped by statement family."""
    catalog = get_standard_financial_fact_catalog(version)
    names = []
    for canonical_name, metadata in catalog.items():
        if statement_family and metadata.get("statement_family") != statement_family:
            continue
        names.append(canonical_name)
    return sorted(names)


def get_core_financial_fact_aliases(
    version: str = "core_financial_facts.v1",
) -> Dict[str, List[str]]:
    """Return a copy of the configured core financial fact alias mapping."""
    if version != "core_financial_facts.v1":
        raise ValueError(f"Unsupported core financial fact alias version: {version}")
    return {key: list(value) for key, value in CORE_FINANCIAL_FACT_ALIASES_V1.items()}


def get_core_financial_fact_derivation_rules(
    version: str = "core_financial_facts.v1",
) -> Dict[str, List[Dict[str, Any]]]:
    """Return conversion rules for deriving canonical fields from components."""
    if version != "core_financial_facts.v1":
        raise ValueError(f"Unsupported core financial fact alias version: {version}")
    return {
        field: [
            {
                **rule,
                "components": {
                    component: list(aliases)
                    for component, aliases in rule.get("components", {}).items()
                },
            }
            for rule in rules
        ]
        for field, rules in CORE_FINANCIAL_FACT_DERIVATION_RULES_V1.items()
    }


def describe_core_financial_fact_alias(
    core_field: str,
    alias: str,
    *,
    version: str = "core_financial_facts.v1",
) -> Dict[str, Any]:
    """Describe whether an upstream alias is compatible with a canonical field.

    The normalized `net_income` and `equity` fields are intentionally parent-
    attributable metrics because valuation ratios such as PE/PB/ROE use that
    shareholder-level denominator/numerator. Total company-level aliases are
    retained as raw numeric facts, but they are not considered clean canonical
    matches unless a later configuration explicitly introduces that metric.
    """
    if version != "core_financial_facts.v1":
        raise ValueError(f"Unsupported core financial fact alias version: {version}")
    field = str(core_field)
    alias_text = str(alias)
    canonical_semantic = _CANONICAL_SEMANTICS_V1.get(field, field)
    alias_semantic = _ALIAS_SEMANTICS_V1.get(field, {}).get(
        alias_text,
        canonical_semantic,
    )
    warning_code = _SEMANTIC_WARNING_CODES_V1.get((field, alias_semantic))
    return {
        "core_field": field,
        "alias": alias_text,
        "canonical_semantic": canonical_semantic,
        "alias_semantic": alias_semantic,
        "canonical_compatible": alias_semantic == canonical_semantic,
        "warnings": [] if warning_code is None else [warning_code],
    }
