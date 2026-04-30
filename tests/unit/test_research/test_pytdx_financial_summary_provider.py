import asyncio
from collections import OrderedDict

from research.providers.pytdx_financial_summary import PytdxFinancialSummaryProvider


class _FakeApi:
    def get_finance_info(self, market, code):
        assert market == 1
        assert code == "600000"
        return OrderedDict(
            [
                ("updated_date", 20260330),
                ("liudongzichan", 500.0),
                ("liudongfuzhai", 250.0),
                ("changqifuzhai", 150.0),
                ("zongzichan", 1000.0),
                ("zhuyingshouru", 800.0),
                ("jingyingxianjinliu", 120.0),
                ("jinglirun", 100.0),
            ]
        )


class _FakePool:
    def __init__(self):
        self.api = _FakeApi()

    def get_connection(self):
        return self.api


def test_pytdx_financial_summary_provider_computes_basic_ratios():
    provider = PytdxFinancialSummaryProvider(connection_pool=_FakePool())
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
    assert snapshot.pub_date == "2026-03-30"
    assert snapshot.current_ratio == 2.0
    assert snapshot.liability_to_asset == 0.4
    assert snapshot.cfo_to_revenue == 0.15
    assert snapshot.cfo_to_net_profit == 1.2
    assert snapshot.source == "pytdx"
