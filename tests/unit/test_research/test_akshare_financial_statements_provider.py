from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from research.providers.akshare_financial_statements import AkshareFinancialStatementsProvider


class _SinaResponse:
    def __init__(self, *, body: bytes, status_code: int = 200, content_type: str = "application/json"):
        self.content = body
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}
        self.encoding = "utf-8"


class _SinaSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _sina_payload() -> bytes:
    return (
        '{"result":{"status":{"code":0},"data":{"report_date":'
        '[{"date_value":"20260630"}],"report_list":{"20260630":{'
        '"data_source":"定期报告","is_audit":"未审计",'
        '"publish_date":"20260820","rCurrency":"CNY",'
        '"rType":"合并期末","update_time":1787127123,"data":['
        '{"item_title":"总资产","item_value":"123.45"}]}}}}}'
    ).encode("utf-8")


def test_sina_report_adapter_parses_valid_json_and_applies_timeout():
    provider = AkshareFinancialStatementsProvider(
        {"request_timeout_seconds": 7.0, "retry_attempts": 2}
    )
    session = _SinaSession([_SinaResponse(body=_sina_payload())])
    provider._sina_session = session

    frame = provider._fetch_sina_financial_report(
        stock="sz300540", statement="资产负债表"
    )

    assert frame.loc[0, "报告日"] == "20260630"
    assert frame.loc[0, "总资产"] == 123.45
    assert session.calls[0][1]["timeout"] == 7.0


@pytest.mark.parametrize(
    "first_response",
    [
        _SinaResponse(body=b""),
        _SinaResponse(body=b"<html>busy</html>", content_type="text/html"),
        _SinaResponse(body=b"not-json", content_type="application/json"),
        _SinaResponse(body=b"busy", status_code=503, content_type="text/plain"),
    ],
)
def test_sina_report_adapter_retries_transient_responses(first_response):
    provider = AkshareFinancialStatementsProvider(
        {"retry_attempts": 2, "retry_backoff_seconds": 0.0}
    )
    session = _SinaSession(
        [first_response, _SinaResponse(body=_sina_payload())]
    )
    provider._sina_session = session

    result = provider._request_sina_financial_json(stock="sz300540", source="fzb")

    assert result["result"]["status"]["code"] == 0
    assert len(session.calls) == 2


def test_sina_report_adapter_reports_compact_final_diagnostic():
    provider = AkshareFinancialStatementsProvider(
        {"retry_attempts": 2, "retry_backoff_seconds": 0.0}
    )
    body = (b"<html>" + b"x" * 400 + b"</html>")
    provider._sina_session = _SinaSession(
        [
            _SinaResponse(body=body, content_type="text/html"),
            _SinaResponse(body=body, content_type="text/html"),
        ]
    )

    with pytest.raises(ValueError) as exc_info:
        provider._request_sina_financial_json(stock="sz300540", source="fzb")

    message = str(exc_info.value)
    assert "attempts=2" in message
    assert "content_type=text/html" in message
    assert len(message) < 400


def test_sina_report_adapter_retries_timeout():
    provider = AkshareFinancialStatementsProvider(
        {"retry_attempts": 2, "retry_backoff_seconds": 0.0}
    )
    session = _SinaSession(
        [requests.Timeout("timed out"), _SinaResponse(body=_sina_payload())]
    )
    provider._sina_session = session

    result = provider._request_sina_financial_json(stock="sz300540", source="fzb")

    assert result["result"]["status"]["code"] == 0
    assert len(session.calls) == 2


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
        "_fetch_sina_financial_report",
        lambda *, stock, statement: _sina_report(stock=stock, symbol=statement),
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
        "_fetch_sina_financial_report",
        lambda *, stock, statement: _sina_report(stock=stock, symbol=statement),
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


def test_akshare_financial_statements_provider_merges_missing_fields_from_sina(
    monkeypatch,
):
    provider = AkshareFinancialStatementsProvider(
        provider_config={
            "statement_interface_order": ["ths_report", "sina_report"],
        }
    )
    calls = []

    def _ths_balance(symbol="603019", indicator="按报告期"):
        calls.append(("ths_balance", symbol, indicator))
        return pd.DataFrame(
            [
                {
                    "report_date": "2024-06-30",
                    "metric_name": "assets_total",
                    "value": 1200.0,
                },
                {
                    "report_date": "2024-06-30",
                    "metric_name": "total_debt",
                    "value": 420.0,
                },
                {
                    "report_date": "2024-06-30",
                    "metric_name": "parent_holder_equity_total",
                    "value": 780.0,
                },
            ]
        )

    def _ths_profit(symbol="603019", indicator="按报告期"):
        calls.append(("ths_profit", symbol, indicator))
        return pd.DataFrame(
            [
                {
                    "report_date": "2024-06-30",
                    "metric_name": "index_deduct_holder_net_profit",
                    "value": 150.0,
                }
            ]
        )

    def _ths_cash(symbol="603019", indicator="按报告期"):
        calls.append(("ths_cash", symbol, indicator))
        return pd.DataFrame(
            [
                {
                    "report_date": "2024-06-30",
                    "metric_name": "act_cash_flow_net",
                    "value": 210.0,
                }
            ]
        )

    def _sina_report(stock="sh603019", symbol="资产负债表"):
        calls.append(("sina", stock, symbol))
        if symbol == "利润表":
            return pd.DataFrame(
                [
                    {
                        "报告日": "20240630",
                        "公告日期": "20240830",
                        "营业收入": 1000.0,
                        "归属于母公司股东的净利润": 180.0,
                    }
                ]
            )
        return pd.DataFrame()

    monkeypatch.setattr(
        provider,
        "_fetch_sina_financial_report",
        lambda *, stock, statement: _sina_report(stock=stock, symbol=statement),
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
                "instrument_id": "603019.SH",
                "symbol": "603019",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            }
        ],
        "direct",
        report_periods=["2024-06-30"],
    )

    assert ("sina", "sh603019", "利润表") in calls
    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.raw_payload["akshare_statement_interface"] == "mixed"
    assert bundle.raw_payload["akshare_statement_interfaces"]["profit_sheet"] == "mixed"
    assert bundle.raw_payload["akshare_statement_field_interfaces"]["profit_sheet"][
        "营业收入"
    ] == "sina_report"
    assert bundle.raw_payload["akshare_statement_field_interfaces"]["profit_sheet"][
        "归属于母公司股东的净利润"
    ] == "sina_report"
    assert bundle.facts is not None
    assert bundle.facts.revenue == 1000.0
    assert bundle.facts.net_income == 180.0
    assert bundle.facts.total_assets == 1200.0
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
