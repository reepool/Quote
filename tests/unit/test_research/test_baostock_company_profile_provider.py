import pytest

from research.providers.baostock_company_profile import BaostockCompanyProfileProvider


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


@pytest.mark.asyncio
async def test_baostock_provider_merges_basic_and_industry(monkeypatch):
    provider = BaostockCompanyProfileProvider()

    basic_rows = _FakeResultSet(
        ["code", "code_name", "ipoDate", "outDate", "type"],
        [
            ["sh.600000", "浦发银行", "1999-11-10", "", "1"],
        ],
    )
    industry_rows = _FakeResultSet(
        ["date", "code", "code_name", "industry", "industryClassification"],
        [
            ["2026-04-17", "sh.600000", "浦发银行", "银行", "申万一级"],
        ],
    )

    monkeypatch.setattr(
        "research.providers.baostock_company_profile.bs.login",
        lambda: _FakeLoginResult(),
    )
    monkeypatch.setattr(
        "research.providers.baostock_company_profile.bs.logout",
        lambda: None,
    )
    monkeypatch.setattr(
        "research.providers.baostock_company_profile.bs.query_stock_basic",
        lambda: basic_rows,
    )
    monkeypatch.setattr(
        "research.providers.baostock_company_profile.bs.query_stock_industry",
        lambda: industry_rows,
    )

    snapshots = await provider.fetch_company_profiles(
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

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "600000.SH"
    assert snapshot.company_name == "浦发银行"
    assert snapshot.industry_raw == "银行"
    assert snapshot.sector_raw == "申万一级"
    assert snapshot.status == "active"
    assert snapshot.source == "baostock"
