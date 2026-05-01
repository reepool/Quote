"""
Versioned aliases for deriving core financial facts from parsed numeric facts.
"""

from __future__ import annotations

from typing import Dict, List


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
        "NetProfit",
        "NetProfitAttributableToOwnersOfParent",
        "PARENT_NETPROFIT",
        "归属于母公司股东的净利润",
        "净利润",
    ],
    "equity": [
        "EquityAttributableToOwnersOfParent",
        "TotalEquity",
        "TOTAL_EQUITY",
        "所有者权益合计",
        "归属于母公司股东权益合计",
    ],
    "total_assets": [
        "Assets",
        "TotalAssets",
        "TOTAL_ASSETS",
        "资产总计",
    ],
    "total_liabilities": [
        "Liabilities",
        "TotalLiabilities",
        "TOTAL_LIABILITIES",
        "负债合计",
    ],
    "operating_cf": [
        "NetCashFlowsFromOperatingActivities",
        "NETCASH_OPERATE",
        "经营活动产生的现金流量净额",
    ],
    "shares_outstanding": [
        "ShareCapital",
        "TOTAL_SHARE",
        "SHARE_CAPITAL",
        "总股本",
    ],
}


def get_core_financial_fact_aliases(
    version: str = "core_financial_facts.v1",
) -> Dict[str, List[str]]:
    """Return a copy of the configured core financial fact alias mapping."""
    if version != "core_financial_facts.v1":
        raise ValueError(f"Unsupported core financial fact alias version: {version}")
    return {key: list(value) for key, value in CORE_FINANCIAL_FACT_ALIASES_V1.items()}
