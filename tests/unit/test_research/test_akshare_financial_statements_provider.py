from types import SimpleNamespace

import pandas as pd

from research.providers.akshare_financial_statements import AkshareFinancialStatementsProvider


def test_akshare_financial_statements_provider_builds_bundle(monkeypatch):
    provider = AkshareFinancialStatementsProvider(
        provider_config={"statement_interface_order": ["eastmoney_report"]}
    )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_balance_sheet_by_report_em=lambda symbol="SH600519": pd.DataFrame(
                [
                    {
                        "REPORT_DATE": "2025-12-31",
                        "NOTICE_DATE": "2026-03-30",
                        "TOTAL_ASSETS": 1200.0,
                        "TOTAL_LIABILITIES": 420.0,
                        "TOTAL_CURRENT_ASSETS": 320.0,
                        "TOTAL_CURRENT_LIAB": 180.0,
                        "INVENTORY": 40.0,
                        "ACCOUNTS_RECE": 55.0,
                        "FIXED_ASSET": 260.0,
                        "INTANGIBLE_ASSET": 25.0,
                        "TOTAL_SHARE": 100.0,
                    }
                ]
            ),
            stock_profit_sheet_by_report_em=lambda symbol="SH600519": pd.DataFrame(
                [
                    {
                        "REPORT_DATE": "2025-12-31",
                        "NOTICE_DATE": "2026-03-30",
                        "TOTAL_OPERATE_INCOME": 1000.0,
                        "OPERATE_COST": 600.0,
                        "OPERATE_PROFIT": 230.0,
                        "TOTAL_PROFIT": 220.0,
                        "NETPROFIT": 180.0,
                    }
                ]
            ),
            stock_cash_flow_sheet_by_report_em=lambda symbol="SH600519": pd.DataFrame(
                [
                    {
                        "REPORT_DATE": "2025-12-31",
                        "NOTICE_DATE": "2026-03-30",
                        "NETCASH_OPERATE": 210.0,
                        "NETCASH_INCREASE_CASH": 35.0,
                    }
                ]
            ),
        ),
    )

    bundles = provider._fetch_financial_statement_bundles_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
    )

    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.instrument_id == "600519.SH"
    assert bundle.report_period == "2025-12-31"
    assert len(bundle.raw_statements) == 3
    assert bundle.facts is not None
    assert bundle.indicators is not None
    assert bundle.facts.revenue == 1000.0
    assert bundle.facts.gross_profit == 400.0
    assert bundle.facts.total_assets == 1200.0
    assert bundle.indicators.gross_margin == 0.4
    assert round(bundle.indicators.current_ratio or 0.0, 4) == round(320.0 / 180.0, 4)


def test_akshare_financial_statements_provider_falls_back_to_sina_report(monkeypatch):
    provider = AkshareFinancialStatementsProvider(
        provider_config={
            "statement_interface_order": ["eastmoney_report", "sina_report"],
        }
    )

    def _eastmoney_failure(symbol="SH600000"):
        raise TypeError("'NoneType' object is not subscriptable")

    def _sina_report(stock="sh600000", symbol="资产负债表"):
        if symbol == "资产负债表":
            return pd.DataFrame(
                [
                    {
                        "报告日": "20240331",
                        "公告日期": "20240430",
                        "资产总计": 1200.0,
                        "负债合计": 420.0,
                        "归属于母公司股东的权益": 780.0,
                        "股本": 100.0,
                    }
                ]
            )
        if symbol == "利润表":
            return pd.DataFrame(
                [
                    {
                        "报告日": "20240331",
                        "公告日期": "20240430",
                        "营业收入": 1000.0,
                        "营业成本": 600.0,
                        "归属于母公司的净利润": 180.0,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "报告日": "20240331",
                    "公告日期": "20240430",
                    "经营活动产生的现金流量净额": 210.0,
                    "现金及现金等价物净增加额": 35.0,
                }
            ]
        )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_balance_sheet_by_report_em=_eastmoney_failure,
            stock_profit_sheet_by_report_em=_eastmoney_failure,
            stock_cash_flow_sheet_by_report_em=_eastmoney_failure,
            stock_financial_report_sina=_sina_report,
        ),
    )

    bundles = provider._fetch_financial_statement_bundles_sync(
        [
            {
                "instrument_id": "600000.SH",
                "symbol": "600000",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
        report_periods=["2024-03-31"],
    )

    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.report_period == "2024-03-31"
    assert bundle.publish_date == "2024-04-30"
    assert bundle.raw_payload["akshare_statement_interface"] == "sina_report"
    assert bundle.facts is not None
    assert bundle.facts.revenue == 1000.0
    assert bundle.facts.net_income == 180.0
    assert bundle.facts.equity == 780.0
