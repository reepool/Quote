from types import SimpleNamespace

import pandas as pd

from research.providers.akshare_financial_statements import AkshareFinancialStatementsProvider


def test_akshare_financial_statements_provider_builds_bundle(monkeypatch):
    provider = AkshareFinancialStatementsProvider()

    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda: SimpleNamespace(
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
