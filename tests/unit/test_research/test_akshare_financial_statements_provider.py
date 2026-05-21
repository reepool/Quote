from types import SimpleNamespace

import pandas as pd

from research.providers.akshare_financial_statements import AkshareFinancialStatementsProvider


def test_akshare_financial_statements_provider_uses_local_core_order_when_enabled():
    provider = AkshareFinancialStatementsProvider(
        provider_config={
            "statement_interface_order": ["sina_report", "eastmoney_report"],
            "service_layers": {
                "local_core": {
                    "enabled": True,
                    "source_order": ["ths_report", "sina_report"],
                }
            },
        }
    )

    assert provider.statement_interface_order == ["ths_report", "sina_report"]


def test_akshare_financial_statements_provider_keeps_fallback_order_when_local_core_disabled():
    provider = AkshareFinancialStatementsProvider(
        provider_config={
            "statement_interface_order": ["sina_report", "eastmoney_report"],
            "service_layers": {
                "local_core": {
                    "enabled": False,
                    "source_order": ["ths_report", "sina_report"],
                }
            },
        }
    )

    assert provider.statement_interface_order == ["sina_report", "eastmoney_report"]


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
    assert bundle.facts.net_income is None
    assert bundle.facts.total_assets == 1200.0
    assert bundle.facts.equity is None
    assert bundle.facts.shares_outstanding is None
    warning_codes = {
        item["warning"] for item in bundle.facts.lineage_json["core_fact_warnings"]
    }
    assert warning_codes == {
        "net_income_total_vs_parent_ambiguous",
        "equity_total_vs_parent_ambiguous",
    }
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
    assert bundle.facts.shares_outstanding is None
    assert bundle.facts.lineage_json["core_fact_warnings"] == []


def test_akshare_financial_statements_provider_merges_target_period_statement_gaps(
    monkeypatch,
):
    provider = AkshareFinancialStatementsProvider(
        provider_config={
            "statement_interface_order": ["ths_report", "sina_report"],
        }
    )
    calls = []

    def _ths_balance(symbol="920005", indicator="按报告期"):
        calls.append(("ths_balance", symbol, indicator))
        return pd.DataFrame()

    def _ths_profit(symbol="920005", indicator="按报告期"):
        calls.append(("ths_profit", symbol, indicator))
        return pd.DataFrame(
            [
                {
                    "report_date": "2024-09-30",
                    "metric_name": "operating_income",
                    "value": 1000.0,
                },
                {
                    "report_date": "2024-09-30",
                    "metric_name": "parent_holder_net_profit",
                    "value": 180.0,
                },
            ]
        )

    def _ths_cash(symbol="920005", indicator="按报告期"):
        calls.append(("ths_cash", symbol, indicator))
        return pd.DataFrame(
            [
                {
                    "report_date": "2024-09-30",
                    "metric_name": "act_cash_flow_net",
                    "value": 210.0,
                }
            ]
        )

    def _sina_report(stock="bj920005", symbol="资产负债表"):
        calls.append(("sina", stock, symbol))
        if symbol == "资产负债表":
            return pd.DataFrame(
                [
                    {
                        "报告日": "20240930",
                        "公告日期": "20241030",
                        "资产总计": 1200.0,
                        "负债合计": 420.0,
                        "归属于母公司股东权益合计": 780.0,
                    }
                ]
            )
        if symbol == "利润表":
            return pd.DataFrame(
                [
                    {
                        "报告日": "20240930",
                        "公告日期": "20241030",
                        "营业收入": 9999.0,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "报告日": "20240930",
                    "公告日期": "20241030",
                    "经营活动产生的现金流量净额": 9999.0,
                }
            ]
        )

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_financial_debt_new_ths=_ths_balance,
            stock_financial_benefit_new_ths=_ths_profit,
            stock_financial_cash_new_ths=_ths_cash,
            stock_financial_report_sina=_sina_report,
        ),
    )

    bundles = provider._fetch_financial_statement_bundles_sync(
        [
            {
                "instrument_id": "920005.BJ",
                "symbol": "920005",
                "exchange": "BSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
        report_periods=["2024-09-30"],
    )

    assert calls == [
        ("ths_balance", "920005", "按报告期"),
        ("ths_profit", "920005", "按报告期"),
        ("ths_cash", "920005", "按报告期"),
        ("sina", "bj920005", "资产负债表"),
        ("sina", "bj920005", "利润表"),
        ("sina", "bj920005", "现金流量表"),
    ]
    assert len(bundles) == 1
    bundle = bundles[0]
    assert len(bundle.raw_statements) == 3
    assert bundle.raw_payload["akshare_statement_interface"] == "mixed"
    assert bundle.raw_payload["akshare_statement_interfaces"] == {
        "balance_sheet": "sina_report",
        "profit_sheet": "ths_report",
        "cash_flow_sheet": "ths_report",
    }
    assert bundle.facts is not None
    assert bundle.facts.total_assets == 1200.0
    assert bundle.facts.total_liabilities == 420.0
    assert bundle.facts.equity == 780.0
    assert bundle.facts.revenue == 1000.0
    assert bundle.facts.net_income == 180.0
    assert bundle.facts.operating_cf == 210.0


def test_akshare_financial_statements_provider_builds_bundle_from_ths_report(monkeypatch):
    provider = AkshareFinancialStatementsProvider(
        provider_config={"statement_interface_order": ["ths_report"]}
    )

    calls = []

    def _ths_frame(statement_name):
        def _fetch(symbol="600519", indicator="按报告期"):
            calls.append((statement_name, symbol, indicator))
            if statement_name == "balance":
                return pd.DataFrame(
                    [
                        {
                            "report_date": "2026-03-31",
                            "report_name": "2026一季报",
                            "quarter_name": "一季度",
                            "metric_name": "assets_total",
                            "value": 1200.0,
                            "single": 1200.0,
                            "yoy": 1.5,
                            "mom": 0.2,
                            "single_yoy": 1.5,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "total_debt",
                            "value": 420.0,
                            "single": 420.0,
                            "yoy": 0.5,
                            "mom": 0.1,
                            "single_yoy": 0.5,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "parent_holder_equity_total",
                            "value": 760.0,
                            "single": 760.0,
                            "yoy": 2.0,
                            "mom": 0.4,
                            "single_yoy": 2.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "total_current_assets",
                            "value": 320.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "current_total_debt",
                            "value": 180.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "inventories",
                            "value": 40.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "account_receivable",
                            "value": 55.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "fixed_assets",
                            "value": 260.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "intangible_assets",
                            "value": 25.0,
                        },
                    ]
                )
            if statement_name == "profit":
                return pd.DataFrame(
                    [
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "operating_income",
                            "value": 1000.0,
                            "single": 250.0,
                            "yoy": 5.0,
                            "mom": 1.0,
                            "single_yoy": 4.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "operating_costs",
                            "value": 600.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "operating_profit",
                            "value": 230.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "profit_total",
                            "value": 220.0,
                        },
                        {
                            "report_date": "2026-03-31",
                            "metric_name": "parent_holder_net_profit",
                            "value": 180.0,
                        },
                    ]
                )
            return pd.DataFrame(
                [
                    {
                        "report_date": "2026-03-31",
                        "metric_name": "act_cash_flow_net",
                        "value": 210.0,
                        "single": 60.0,
                        "yoy": 2.0,
                        "mom": 0.8,
                        "single_yoy": 2.1,
                    },
                    {
                        "report_date": "2026-03-31",
                        "metric_name": "cash_net_addition",
                        "value": 35.0,
                    },
                ]
            )

        return _fetch

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_financial_debt_new_ths=_ths_frame("balance"),
            stock_financial_benefit_new_ths=_ths_frame("profit"),
            stock_financial_cash_new_ths=_ths_frame("cash"),
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
        report_periods=["2026-03-31"],
    )

    assert calls == [
        ("balance", "600519", "按报告期"),
        ("profit", "600519", "按报告期"),
        ("cash", "600519", "按报告期"),
    ]
    assert provider._to_ths_stock_symbol("920833.BJ") == "920833"
    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.raw_payload["akshare_statement_interface"] == "ths_report"
    assert len(bundle.raw_statements) == 3
    assert bundle.facts is not None
    assert bundle.facts.revenue == 1000.0
    assert bundle.facts.gross_profit == 400.0
    assert bundle.facts.net_income == 180.0
    assert bundle.facts.equity == 760.0
    assert bundle.facts.operating_cf == 210.0
    assert bundle.facts.lineage_json["core_fact_warnings"] == []

    profit_payload = bundle.raw_payload["profit_sheet"]
    assert profit_payload["ths_metrics"]["operating_income"]["single"] == 250.0
    assert profit_payload["operating_income__single_yoy"] == 4.0
