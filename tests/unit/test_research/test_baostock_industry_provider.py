import asyncio

from research.providers.baostock_industry import BaostockIndustryProvider


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


def test_baostock_industry_provider_builds_membership_snapshot(monkeypatch):
    provider = BaostockIndustryProvider()

    industry_rows = _FakeResultSet(
        ["date", "code", "code_name", "industry", "industryClassification"],
        [
            ["2026-04-18", "sh.600000", "浦发银行", "银行", "申万一级"],
        ],
    )

    monkeypatch.setattr(
        "research.providers.baostock_industry.bs.login",
        lambda: _FakeLoginResult(),
    )
    monkeypatch.setattr(
        "research.providers.baostock_industry.bs.logout",
        lambda: None,
    )
    monkeypatch.setattr(
        "research.providers.baostock_industry.bs.query_stock_industry",
        lambda: industry_rows,
    )

    loop = asyncio.new_event_loop()
    try:
        snapshots = loop.run_until_complete(
            provider.fetch_industries(
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
    assert snapshot.taxonomy_system == "sw_l1"
    assert snapshot.taxonomy_version is None
    assert snapshot.industry_code == "银行"
    assert snapshot.industry_name == "银行"
    assert snapshot.mapping_status == "reference_only"
    assert snapshot.source_classification == "申万一级"
    assert snapshot.source == "baostock"
