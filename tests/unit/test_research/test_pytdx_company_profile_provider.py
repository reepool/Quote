import asyncio
from collections import OrderedDict

import pytest

from research.providers.pytdx_company_profile import PytdxCompanyProfileProvider


class _FakeApi:
    def get_finance_info(self, market, code):
        assert market == 1
        assert code == "600000"
        return OrderedDict(
            [
                ("name", "浦发银行"),
                ("上市日期", "19991110"),
            ]
        )

    def get_company_info_category(self, market, code):
        return [
            {
                "name": "公司概况",
                "filename": "600000.txt",
                "start": 0,
                "length": 128,
            },
            {
                "name": "股本结构",
                "filename": "600000.txt",
                "start": 128,
                "length": 64,
            },
        ]

    def get_company_info_content(self, market, code, filename, start, length):
        return f"{filename}:{start}:{length}"


class _FakePool:
    def __init__(self):
        self.api = _FakeApi()

    def get_connection(self):
        return self.api


@pytest.fixture(autouse=True)
def cleanup_cache():
    yield


def test_pytdx_provider_builds_company_profile_snapshot():
    provider = PytdxCompanyProfileProvider(
        connection_pool=_FakePool(),
        max_content_categories=2,
        max_content_length=256,
    )

    loop = asyncio.new_event_loop()
    try:
        snapshots = loop.run_until_complete(
            provider.fetch_company_profiles(
                instruments=[
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "name": "浦发银行",
                        "exchange": "SSE",
                        "type": "stock",
                        "is_active": True,
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
    assert snapshot.company_name == "浦发银行"
    assert snapshot.listed_date == "1999-11-10"
    assert snapshot.market == "SSE"
    assert snapshot.status == "active"
    assert snapshot.source == "pytdx"
    assert snapshot.raw_payload["finance_info"]["name"] == "浦发银行"
    assert "公司概况" in snapshot.raw_payload["company_info_content"]
