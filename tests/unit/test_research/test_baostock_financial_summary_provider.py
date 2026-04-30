import asyncio

from research.providers.baostock_financial_summary import BaostockFinancialSummaryProvider


class _FakeResultSet:
    def __init__(self, fields, rows):
        self.fields = fields
        self._rows = rows
        self._index = -1
        self.error_code = "0"
        self.error_msg = ""

    def next(self):
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


class _FakeLoginResult:
    error_code = "0"
    error_msg = ""


def test_baostock_financial_summary_provider_merges_multiple_quarter_feeds(monkeypatch):
    provider = BaostockFinancialSummaryProvider(lookback_quarters=1)

    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.login",
        lambda: _FakeLoginResult(),
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.logout",
        lambda: None,
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.query_profit_data",
        lambda *args, **kwargs: _FakeResultSet(
            ["code", "pubDate", "statDate", "roeAvg", "npMargin", "gpMargin", "epsTTM"],
            [["sh.600000", "2026-03-30", "2025-12-31", "12.5", "18.8", "42.0", "3.2"]],
        ),
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.query_operation_data",
        lambda *args, **kwargs: _FakeResultSet(
            ["code", "pubDate", "statDate", "AssetTurnRatio"],
            [["sh.600000", "2026-03-30", "2025-12-31", "0.91"]],
        ),
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.query_growth_data",
        lambda *args, **kwargs: _FakeResultSet(
            ["code", "pubDate", "statDate", "YOYAsset", "YOYEquity", "YOYNI"],
            [["sh.600000", "2026-03-30", "2025-12-31", "8.1", "7.5", "10.2"]],
        ),
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.query_balance_data",
        lambda *args, **kwargs: _FakeResultSet(
            ["code", "pubDate", "statDate", "currentRatio", "quickRatio", "liabilityToAsset"],
            [["sh.600000", "2026-03-30", "2025-12-31", "1.7", "1.1", "0.55"]],
        ),
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.query_cash_flow_data",
        lambda *args, **kwargs: _FakeResultSet(
            ["code", "pubDate", "statDate", "CFOToOR", "CFOToNP"],
            [["sh.600000", "2026-03-30", "2025-12-31", "0.25", "1.4"]],
        ),
    )
    monkeypatch.setattr(
        "research.providers.baostock_financial_summary.bs.query_dupont_data",
        lambda *args, **kwargs: _FakeResultSet(
            ["code", "pubDate", "statDate", "dupontROE"],
            [["sh.600000", "2026-03-30", "2025-12-31", "12.5"]],
        ),
    )

    loop = asyncio.new_event_loop()
    try:
        snapshots = loop.run_until_complete(
            provider.fetch_financial_summaries(
                instruments=[
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "name": "浦发银行",
                        "exchange": "SSE",
                        "type": "stock",
                    }
                ],
                exchange="SSE",
            )
        )
    finally:
        loop.close()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "600000.SH"
    assert snapshot.report_date == "2025-12-31"
    assert snapshot.pub_date == "2026-03-30"
    assert snapshot.roe == 12.5
    assert snapshot.current_ratio == 1.7
    assert snapshot.cfo_to_net_profit == 1.4
    assert snapshot.asset_turnover == 0.91
    assert snapshot.source == "baostock"
