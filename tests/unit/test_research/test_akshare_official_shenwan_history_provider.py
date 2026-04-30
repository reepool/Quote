from types import SimpleNamespace

import pandas as pd

from research.providers.akshare_official_shenwan_history import (
    AkshareOfficialShenwanHistoryProvider,
)


def test_official_shenwan_history_provider_selects_latest_record_per_symbol(monkeypatch):
    provider = AkshareOfficialShenwanHistoryProvider()
    monkeypatch.setattr(
        "research.providers.akshare_official_shenwan_history.load_akshare",
        lambda mode="direct": None,
    )
    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_industry_clf_hist_sw=lambda: pd.DataFrame(
                [
                    {
                        "symbol": "600519",
                        "start_date": "2001-07-31",
                        "industry_code": "340301",
                        "update_time": "2015-10-27",
                    },
                    {
                        "symbol": "600519",
                        "start_date": "2021-07-30",
                        "industry_code": "340501",
                        "update_time": "2022-05-09",
                    },
                    {
                        "symbol": "000001",
                        "start_date": "2014-02-21",
                        "industry_code": "480101",
                        "update_time": "2024-09-27",
                    },
                    {
                        "symbol": "000001",
                        "start_date": "2021-07-30",
                        "industry_code": "480301",
                        "update_time": "2025-12-15",
                    },
                    {
                        "symbol": "300750",
                        "start_date": "2018-06-11",
                        "industry_code": "640201",
                        "update_time": "2025-12-15",
                    },
                ]
            )
        ),
    )

    snapshots = provider._fetch_latest_classifications_sync(
        [
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
                "is_active": True,
            },
            {
                "instrument_id": "000001.SZ",
                "symbol": "000001",
                "exchange": "SZSE",
                "type": "stock",
                "is_active": True,
            },
        ],
        "direct",
    )

    assert [item.instrument_id for item in snapshots] == ["000001.SZ", "600519.SH"]
    assert snapshots[0].official_industry_code == "480301"
    assert snapshots[0].update_time == "2025-12-15"
    assert snapshots[1].official_industry_code == "340501"
    assert snapshots[1].start_date == "2021-07-30"


def test_official_shenwan_history_provider_respects_mode_support():
    provider = AkshareOfficialShenwanHistoryProvider()

    assert provider.supports_mode("direct") is True
    assert provider.supports_mode("proxy_patch") is True


def test_official_shenwan_history_provider_can_load_all_latest_rows(monkeypatch):
    provider = AkshareOfficialShenwanHistoryProvider()
    monkeypatch.setattr(
        provider,
        "_akshare",
        lambda mode="direct": SimpleNamespace(
            stock_industry_clf_hist_sw=lambda: pd.DataFrame(
                [
                    {
                        "symbol": "600519",
                        "start_date": "2021-07-30",
                        "industry_code": "340501",
                        "update_time": "2022-05-09",
                    },
                    {
                        "symbol": "000001",
                        "start_date": "2021-07-30",
                        "industry_code": "480301",
                        "update_time": "2025-12-15",
                    },
                ]
            )
        ),
    )

    snapshots = provider._fetch_all_latest_classifications_sync("direct")

    assert [item.instrument_id for item in snapshots] == ["000001.SZ", "600519.SH"]
    assert snapshots[0].official_industry_code == "480301"
    assert snapshots[1].exchange == "SSE"
