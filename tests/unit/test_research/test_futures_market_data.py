from __future__ import annotations

import asyncio
from datetime import date, timedelta
import pandas as pd
import pytest

from research.futures_market_data import (
    FuturesBar,
    FuturesDiagnosticsService,
    FuturesExposureMapping,
    FuturesMarketDataSyncService,
    FuturesSeries,
    FuturesStorageManager,
    default_futures_registry,
)
from research.providers.akshare_futures import AkshareFuturesMarketDataProvider
from research.providers.official_futures import (
    OfficialFuturesMarketDataProvider,
    OfficialFuturesSourceUnavailable,
)
from utils.config_manager import ResearchConfig, ResearchStorageConfig


def _research_config(tmp_path):
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(db_path=str(tmp_path / "research.db")),
        modules={
            "commodity_market_data": {
                "enabled": True,
                "storage": {"database": str(tmp_path / "futures.db")},
                "diagnostics": {
                    "lookback_years": [3, 5, 10],
                    "trading_days_per_year": 10,
                    "min_observation_ratio": 0.5,
                },
                "coverage": {
                    "target_history_years": 10,
                    "max_stale_trading_days": 9999,
                },
            }
        },
        sources={"akshare": {"futures_market_data": {"daily_interface": "futures_zh_daily_sina"}}},
    )


def test_futures_storage_initializes_futures_db_and_upserts_bars(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    registry = default_futures_registry(config.modules["commodity_market_data"])
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])
    storage.upsert_source_manifests(registry["source_manifests"])

    series = registry["series"][0]
    bars = [
        FuturesBar(
            series_id=series.series_id,
            trade_date="2020-01-02",
            open=1.0,
            high=1.2,
            low=0.9,
            close=1.1,
            raw_payload_hash="h1",
            source="akshare",
            source_mode="direct",
            source_profile="akshare_futures",
        )
    ]

    first = storage.upsert_price_bars(bars)
    second = storage.upsert_price_bars(bars)

    assert (tmp_path / "futures.db").exists()
    assert first == {"inserted": 1, "changed": 0, "unchanged": 0}
    assert second == {"inserted": 0, "changed": 0, "unchanged": 1}
    assert storage.get_price_bars(series.series_id)[0]["close"] == 1.1
    manifests = storage.list_source_manifests(enabled_only=False)
    assert {item["source_profile"] for item in manifests} == {
        "exchange_official",
        "akshare_futures",
    }
    assert next(item for item in manifests if item["source_profile"] == "akshare_futures")[
        "coverage_target_years"
    ] == 10


def test_default_futures_registry_includes_domestic_p0_universe(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["registry"] = {"include_default_p0_universe": True}

    registry = default_futures_registry(config.modules["commodity_market_data"])
    instrument_ids = {item.instrument_id for item in registry["instruments"]}
    series_ids = {item.series_id for item in registry["series"]}

    assert len(instrument_ids) >= 40
    assert {
        "CNF.ZC.CZCE",
        "CNF.RB.SHFE",
        "CNF.CU.SHFE",
        "CNF.SC.INE",
        "CNF.TA.CZCE",
        "CNF.LC.GFEX",
        "CNF.FG.CZCE",
    }.issubset(instrument_ids)
    assert "CNF.CU.SHFE.main" in series_ids


def test_futures_cycle_diagnostics_marks_ten_year_window_ready(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    series = FuturesSeries(
        series_id="CNF.CU.SHFE.main",
        instrument_id="CNF.CU.SHFE",
        symbol="CU0",
        series_type="main_continuous",
        source_profile="akshare_futures",
        source="akshare",
        unit="CNY/ton",
    )
    registry = default_futures_registry(config.modules["commodity_market_data"])
    storage.upsert_instruments_and_series(registry["instruments"], [series])
    start = date(2015, 1, 1)
    bars = [
        FuturesBar(
            series_id=series.series_id,
            trade_date=(start + timedelta(days=36 * index)).isoformat(),
            open=100 + index,
            high=101 + index,
            low=99 + index,
            close=100 + index,
            raw_payload_hash=f"h{index}",
        )
        for index in range(110)
    ]
    storage.upsert_price_bars(bars)

    result = FuturesDiagnosticsService(
        storage,
        config.modules["commodity_market_data"],
    ).refresh_series(series.series_id)
    diagnostics = storage.get_cycle_diagnostics(series.series_id)

    assert result["diagnostics_written"] == 3
    ten_year = next(item for item in diagnostics if item["lookback_years"] == 10)
    assert ten_year["cycle_state"] != "insufficient_history"
    assert ten_year["latest_price"] == 209


def test_futures_exposure_mapping_returns_structured_rows(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_exposure_mappings(
        [
            FuturesExposureMapping(
                mapping_id="600000.SH-copper",
                scope_type="instrument",
                scope_id="600000.SH",
                product_name="copper",
                revenue_series_id="CNF.CU.SHFE.main",
                cost_series_ids=["CNF.AL.SHFE.main"],
                spread_ids=["alumina_spread"],
                direction="positive",
            )
        ]
    )

    rows = storage.get_exposure_mappings(scope_type="instrument", scope_id="600000.SH")

    assert rows[0]["revenue_series_id"] == "CNF.CU.SHFE.main"
    assert rows[0]["cost_series_ids"] == ["CNF.AL.SHFE.main"]


def test_akshare_futures_provider_normalizes_fixture(monkeypatch, tmp_path):
    config = _research_config(tmp_path)

    class FakeAkshare:
        @staticmethod
        def futures_zh_daily_sina(symbol):
            assert symbol == "CU0"
            return pd.DataFrame(
                [
                    {
                        "date": "2020-01-02",
                        "open": 10,
                        "high": 12,
                        "low": 9,
                        "close": 11,
                        "volume": 100,
                    }
                ]
            )

    monkeypatch.setattr(
        "research.providers.akshare_futures.load_akshare",
        lambda mode: FakeAkshare(),
    )
    provider = AkshareFuturesMarketDataProvider(config)
    series = FuturesSeries(
        series_id="CNF.CU.SHFE.main",
        instrument_id="CNF.CU.SHFE",
        symbol="CU0",
        series_type="main_continuous",
        source_profile="akshare_futures",
        source="akshare",
        unit="CNY/ton",
    )

    bars = provider._fetch_daily_bars_sync(series, None, None, "direct")

    assert len(bars) == 1
    assert bars[0].trade_date == "2020-01-02"
    assert bars[0].quality_flag == "ok"
    assert bars[0].source_interface == "futures_zh_daily_sina"


def test_official_futures_provider_parses_shfe_and_selects_main_contract(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}
    }
    provider = OfficialFuturesMarketDataProvider(config)
    series = FuturesSeries(
        series_id="CNF.CU.SHFE.main",
        instrument_id="CNF.CU.SHFE",
        symbol="CU0",
        series_type="main_continuous",
        source_profile="exchange_official",
        source="exchange_official",
        unit="CNY/ton",
    )

    rows = provider._parse_shfe_payload(
        {
            "o_curinstrument": [
                {
                    "PRODUCTGROUPID": "cu",
                    "DELIVERYMONTH": "2407",
                    "OPENPRICE": "10",
                    "HIGHESTPRICE": "12",
                    "LOWESTPRICE": "9",
                    "CLOSEPRICE": "11",
                    "SETTLEMENTPRICE": "10.5",
                    "VOLUME": "100",
                    "OPENINTEREST": "200",
                    "TURNOVER": "1234",
                },
                {
                    "PRODUCTGROUPID": "cu",
                    "DELIVERYMONTH": "2408",
                    "OPENPRICE": "20",
                    "HIGHESTPRICE": "22",
                    "LOWESTPRICE": "19",
                    "CLOSEPRICE": "21",
                    "SETTLEMENTPRICE": "20.5",
                    "VOLUME": "150",
                    "OPENINTEREST": "300",
                    "TURNOVER": "5678",
                },
            ]
        },
        trade_date="2024-06-03",
        exchange="SHFE",
    )

    bars = provider._construct_main_series_bars(series, rows, mode="direct")

    assert len(bars) == 1
    assert bars[0].source_profile == "exchange_official"
    assert bars[0].source_interface == "official_shfe_daily_kx_dat"
    assert bars[0].close == 21
    assert bars[0].metadata["underlying_contract"] == "CU2408"
    assert bars[0].metadata["construction_method"] == "official_open_interest_main"


def test_official_futures_provider_parses_domestic_exchange_fixtures(tmp_path):
    provider = OfficialFuturesMarketDataProvider(_research_config(tmp_path))

    dce = provider._parse_dce_payload(
        {
            "data": [
                {
                    "variety": "铁矿石",
                    "contractId": "I2409",
                    "open": "800",
                    "high": "810",
                    "low": "790",
                    "close": "805",
                    "clearPrice": "803",
                    "volumn": "1000",
                    "openInterest": "2000",
                    "turnover": "123456",
                }
            ]
        },
        trade_date="2024-06-03",
    )
    gfex = provider._parse_gfex_payload(
        {
            "data": [
                {
                    "variety": "工业硅",
                    "varietyOrder": "si",
                    "delivMonth": "2409",
                    "open": "12000",
                    "high": "12100",
                    "low": "11900",
                    "close": "12050",
                    "clearPrice": "12040",
                    "volumn": "100",
                    "openInterest": "200",
                    "turnover": "1000",
                }
            ]
        },
        trade_date="2024-06-03",
    )
    czce = provider._parse_czce_text(
        "合约|昨结算|今开盘|最高价|最低价|今收盘|今结算|涨跌1|涨跌2|成交量|空盘量|增减量|成交额\n"
        "TA409|5000|5010|5020|4990|5015|5008|1|2|123|456|7|890",
        trade_date="2024-06-03",
    )

    assert dce[0].contract == "I2409"
    assert dce[0].source_interface == "official_dce_day_quotes"
    assert gfex[0].contract == "SI2409"
    assert gfex[0].source_interface == "official_gfex_ti_day_quotes"
    assert czce[0].contract == "TA409"
    assert czce[0].volume == 123
    assert czce[0].source_interface == "official_czce_future_data_daily_txt"


@pytest.mark.asyncio
async def test_futures_market_data_sync_writes_fixture_bars(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()

    async def fake_fetch_daily_bars(self, series, *, start_date=None, end_date=None, mode="direct"):
        return [
            FuturesBar(
                series_id=series.series_id,
                trade_date="2020-01-02",
                open=10,
                high=12,
                low=9,
                close=11,
                raw_payload_hash=f"{series.series_id}:2020-01-02",
                source="akshare",
                source_mode=mode,
                source_profile=series.source_profile,
                source_interface=series.source_interface,
            )
        ]

    monkeypatch.setattr(
        "research.providers.akshare_futures.AkshareFuturesMarketDataProvider.fetch_daily_bars",
        fake_fetch_daily_bars,
    )

    series_id = "CNF.CU.SHFE.main"
    result = await FuturesMarketDataSyncService(storage, config).sync(series_ids=[series_id])

    assert result["status"] == "success"
    assert result["totals"]["inserted"] == 1
    assert storage.get_price_bars(series_id)[0]["close"] == 11
    assert storage.get_cycle_diagnostics(series_id)


@pytest.mark.asyncio
async def test_futures_market_data_sync_prefers_official_source(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "preferred_order": ["exchange_official", "akshare_futures"],
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"], "timeout_seconds": 1},
        "akshare_futures": {"enabled": True, "timeout_seconds": 1},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    async def fake_official_fetch(self, series, *, start_date=None, end_date=None, mode="direct"):
        return [
            FuturesBar(
                series_id=series.series_id,
                trade_date="2024-06-03",
                open=10,
                high=12,
                low=9,
                close=11,
                raw_payload_hash=f"official:{series.series_id}:2024-06-03",
                source="exchange_official",
                source_mode=mode,
                source_profile="exchange_official",
                source_interface="official_shfe_daily_kx_dat",
            )
        ]

    async def unexpected_fallback(self, series, *, start_date=None, end_date=None, mode="direct"):
        raise AssertionError("fallback provider should not be called when official source succeeds")

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_daily_bars",
        fake_official_fetch,
    )
    monkeypatch.setattr(
        "research.providers.akshare_futures.AkshareFuturesMarketDataProvider.fetch_daily_bars",
        unexpected_fallback,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(series_ids=["CNF.CU.SHFE.main"])
    rows = storage.get_price_bars("CNF.CU.SHFE.main")

    assert result["status"] == "success"
    assert result["source_selection"]["official_success"] == 1
    assert result["source_selection"]["fallback_success"] == 0
    assert rows[0]["source_profile"] == "exchange_official"


@pytest.mark.asyncio
async def test_futures_market_data_sync_falls_back_after_official_unavailable(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "preferred_order": ["exchange_official", "akshare_futures"],
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"], "timeout_seconds": 1},
        "akshare_futures": {"enabled": True, "timeout_seconds": 1},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    async def failed_official_fetch(self, series, *, start_date=None, end_date=None, mode="direct"):
        raise OfficialFuturesSourceUnavailable("official fixture unavailable")

    async def fake_fallback_fetch(self, series, *, start_date=None, end_date=None, mode="direct"):
        return [
            FuturesBar(
                series_id=series.series_id,
                trade_date="2024-06-03",
                open=20,
                high=22,
                low=19,
                close=21,
                raw_payload_hash=f"akshare:{series.series_id}:2024-06-03",
                source="akshare",
                source_mode=mode,
                source_profile="akshare_futures",
                source_interface="futures_zh_daily_sina",
            )
        ]

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_daily_bars",
        failed_official_fetch,
    )
    monkeypatch.setattr(
        "research.providers.akshare_futures.AkshareFuturesMarketDataProvider.fetch_daily_bars",
        fake_fallback_fetch,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(series_ids=["CNF.CU.SHFE.main"])
    rows = storage.get_price_bars("CNF.CU.SHFE.main")

    assert result["status"] == "success"
    assert result["source_selection"]["official_failed"] == 1
    assert result["source_selection"]["fallback_success"] == 1
    assert result["series"][0]["official_status"] == "unavailable"
    assert rows[0]["source_profile"] == "akshare_futures"


@pytest.mark.asyncio
async def test_futures_market_data_sync_times_out_stuck_provider(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "akshare_futures": {"timeout_seconds": 1}
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    async def stuck_fetch_daily_bars(self, series, *, start_date=None, end_date=None, mode="direct"):
        await asyncio.sleep(5)
        return []

    monkeypatch.setattr(
        "research.providers.akshare_futures.AkshareFuturesMarketDataProvider.fetch_daily_bars",
        stuck_fetch_daily_bars,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=["CNF.CU.SHFE.main"]
    )

    assert result["status"] == "partial"
    assert result["totals"]["failed"] == 1
    assert "CNF.CU.SHFE.main" in result["series"][0]["series_id"]
