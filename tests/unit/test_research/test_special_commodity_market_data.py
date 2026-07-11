from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd

from utils import config_manager
from research.special_commodity_market_data import (
    AkshareCommoditySpotProvider,
    AkshareForeignFuturesProvider,
    CommodityAdapterRegistry,
    CommodityObservation,
    CommoditySeries,
    CommodityUniverseSelector,
    ConfiguredSourceChainProvider,
    EiaCommodityProvider,
    FredCommodityProvider,
    SpecialCommodityMasterDataService,
    SpecialCommodityCalendarGovernanceService,
    SpecialCommodityPolicyEventService,
    SpecialCommodityPriceSyncService,
    SpecialCommodityStorageManager,
    WorldBankCommodityProvider,
    _source_unit_matches,
)


def _research_config(tmp_path: Path):
    cfg = deepcopy(config_manager.get_research_config())
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    special_cfg["enabled"] = True
    special_cfg["storage"]["database"] = str(tmp_path / "futures.db")
    return cfg


def _fake_fred_governance_get(url, *args, **kwargs):
    if str(url).endswith("/fred/series"):
        return SimpleNamespace(
            url="https://api.stlouisfed.org/fred/series?series_id=DCOILWTICO&api_key=unit-test-key",
            raise_for_status=lambda: None,
            json=lambda: {
                "seriess": [
                    {
                        "id": "DCOILWTICO",
                        "title": "Crude Oil Prices: West Texas Intermediate",
                        "frequency": "Daily",
                        "units": "Dollars per Barrel",
                        "observation_start": "1986-01-02",
                        "last_updated": "2026-01-03",
                    }
                ]
            },
        )
    return SimpleNamespace(
        url="https://api.stlouisfed.org/fred/series/observations?series_id=DCOILWTICO&api_key=unit-test-key",
        raise_for_status=lambda: None,
        json=lambda: {"observations": [{"date": "2026-01-01", "value": "72.5"}]},
    )


def test_special_commodity_master_schema_and_seed(tmp_path):
    cfg = _research_config(tmp_path)
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()

    result = SpecialCommodityMasterDataService(
        storage,
        cfg.modules["commodity_market_data"]["special_commodity_market_data"],
    ).sync()

    assert result["status"] == "success"
    assert result["instruments"] == 11
    assert result["series"] >= 15

    dictionary = storage.read_dictionary()
    assert {item["commodity_id"] for item in dictionary["instruments"]} >= {
        "OIL.WTI.SPOT",
        "OIL.BRENT.SPOT",
        "METAL.COPPER.IMF",
        "METAL.ALUMINUM.IMF",
        "METAL.COPPER.LME_3M",
        "METAL.ALUMINIUM.LME_3M",
        "METAL.ZINC.LME_3M",
        "METAL.LEAD.LME_3M",
        "METAL.NICKEL.LME_3M",
        "METAL.TIN.LME_3M",
        "CN.CHEMICAL.PTA.SPOT",
    }
    assert {item["series_id"] for item in dictionary["series"]} >= {
        "CMD.OIL.WTI.SPOT.FRED.DAILY",
        "CMD.OIL.WTI.SPOT.EIA.DAILY",
        "CMD.METAL.COPPER.IMF.FRED.MONTHLY",
        "CMD.METAL.COPPER.WORLDBANK.MONTHLY",
        "CMD.METAL.COPPER.LME3M.DAILY",
        "CMD.METAL.ALUMINIUM.LME3M.DAILY",
        "CMD.CN.CHEMICAL.PTA.SPOT.100PPI.DAILY",
    }


def test_special_commodity_scope_resolution():
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    selector = CommodityUniverseSelector(cfg)

    energy = selector.resolve(scope_id="fred_energy_oil")
    assert {item.series_id for item in energy} == {
        "CMD.OIL.WTI.SPOT.FRED.DAILY",
        "CMD.OIL.BRENT.SPOT.FRED.DAILY",
    }

    explicit = selector.resolve(
        scope_id="fred_energy_oil",
        series_ids=["CMD.METAL.COPPER.IMF.FRED.MONTHLY"],
    )
    assert [item.series_id for item in explicit] == ["CMD.METAL.COPPER.IMF.FRED.MONTHLY"]

    assert {item.series_id for item in selector.resolve(scope_id="eia_energy_oil")} == {
        "CMD.OIL.WTI.SPOT.EIA.DAILY",
        "CMD.OIL.BRENT.SPOT.EIA.DAILY",
    }
    assert {
        item.source_profile for item in selector.resolve(scope_id="eia_energy_oil")
    } == {"eia_fred_oil_chain"}
    assert {item.series_id for item in selector.resolve(scope_id="world_bank_metals")} == {
        "CMD.METAL.COPPER.WORLDBANK.MONTHLY",
        "CMD.METAL.ALUMINUM.WORLDBANK.MONTHLY",
    }
    assert {item.series_id for item in selector.resolve(scope_id="lme_nonferrous")} == {
        "CMD.METAL.COPPER.LME3M.DAILY",
        "CMD.METAL.ALUMINIUM.LME3M.DAILY",
        "CMD.METAL.ZINC.LME3M.DAILY",
        "CMD.METAL.LEAD.LME3M.DAILY",
        "CMD.METAL.NICKEL.LME3M.DAILY",
        "CMD.METAL.TIN.LME3M.DAILY",
    }


def test_all_active_special_commodity_series_have_concrete_adapters():
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    registry = CommodityAdapterRegistry(cfg)
    selector = CommodityUniverseSelector(cfg)

    for source_profile in {item.source_profile for item in selector.resolve()}:
        provider, governance, blockers = registry.resolve(source_profile)
        assert blockers == []
        assert provider is not None
        assert governance is not None


def test_configured_source_chain_prefers_primary_and_fills_missing_dates(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(
        series_ids=["CMD.OIL.WTI.SPOT.EIA.DAILY"]
    )[0]
    provider = ConfiguredSourceChainProvider(
        item.source_profile,
        cfg["source_profiles"][item.source_profile],
        cfg,
    )

    def observation(series: CommoditySeries, observed_date: str, value: float):
        return CommodityObservation(
            series_id=series.series_id,
            observation_date=observed_date,
            value=value,
            currency="USD",
            unit="USD/barrel",
            raw_value=value,
            raw_currency="USD",
            raw_unit="USD/barrel",
            source_profile=series.source_profile,
            source_url=f"https://example.test/{series.source_profile}",
            quality_flag="official_public_api",
            source_symbol=series.source_symbol,
            parser_version="unit-test",
            raw_payload_hash=f"{series.source_profile}-{observed_date}-{value}",
        )

    class FakeProvider:
        def __init__(self, profile):
            self.profile = profile

        def fetch(self, series, *, start_date, end_date):
            rows = [observation(series[0], "2026-01-02", 70.0)]
            if self.profile == "fred_official_api":
                rows = [
                    observation(series[0], "2026-01-02", 70.5),
                    observation(series[0], "2026-01-03", 71.0),
                ]
            from research.special_commodity_market_data import CommodityProviderResult

            return CommodityProviderResult(observations=rows)

    monkeypatch.setattr(
        provider,
        "_resolve_provider",
        lambda profile: FakeProvider(profile),
    )
    result = provider.fetch([item], start_date="2026-01-01", end_date="2026-01-03")

    assert [(row.observation_date, row.value) for row in result.observations] == [
        ("2026-01-02", 70.0),
        ("2026-01-03", 71.0),
    ]
    assert result.observations[0].metadata["source_role"] == "primary"
    assert result.observations[1].metadata["source_role"] == "fallback"
    diagnostics = result.metadata["cross_source"]
    assert diagnostics["fallback_filled_dates"] == 1
    assert diagnostics["conflict_count"] == 1
    assert diagnostics["max_absolute_difference"] == 0.5


def test_source_unit_governance_normalizes_equivalent_official_labels():
    assert _source_unit_matches("USD/barrel", "Dollars per Barrel")
    assert _source_unit_matches("USD/barrel", "$/BBL")
    assert _source_unit_matches("USD/metric_ton", "U.S. Dollars per Metric Ton")
    assert _source_unit_matches("USD/metric_ton", "($/mt)")


def test_missing_governance_adapter_blocks_before_observation_write(tmp_path):
    cfg = _research_config(tmp_path)
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    special_cfg["source_profiles"]["fred_official_api"]["governance_adapter"] = ""
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()

    result = SpecialCommodityPriceSyncService(storage, cfg).sync(
        series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"],
        start_date="2026-01-01",
        end_date="2026-01-02",
        dry_run=False,
    )

    assert result["status"] == "blocked"
    assert result["master_data_governance"] == "blocked"
    assert result["inserted"] == 0
    assert result["blockers"][0]["reason"] == "missing_commodity_governance_adapter"
    assert storage.read_observations(series_id="CMD.OIL.WTI.SPOT.FRED.DAILY") == []
    governance_rows = storage.read_dictionary()["master_governance"]
    assert governance_rows[0]["governance_status"] == "blocked"


def test_fred_provider_missing_key_blocks(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["fred_official_api"]
    series = CommodityUniverseSelector(cfg).resolve(series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"])

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    result = FredCommodityProvider("fred_official_api", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert result.observations == []
    assert result.blockers[0]["reason"] == "missing_api_key"


def test_fred_provider_parses_observations(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["fred_official_api"]
    series = CommodityUniverseSelector(cfg).resolve(series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"])

    class _Response:
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=DCOILWTICO&api_key=unit-test-key"

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "observations": [
                    {"date": "2026-01-01", "value": "72.5"},
                    {"date": "2026-01-02", "value": "."},
                ]
            }

    def _fake_get(*args, **kwargs):
        return _Response()

    monkeypatch.setenv("FRED_API_KEY", "unit-test-key")
    monkeypatch.setattr("research.special_commodity_market_data.request_get", _fake_get)

    result = FredCommodityProvider("fred_official_api", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert len(result.observations) == 1
    obs = result.observations[0]
    assert obs.series_id == "CMD.OIL.WTI.SPOT.FRED.DAILY"
    assert obs.value == 72.5
    assert obs.currency == "USD"
    assert obs.unit == "USD/barrel"
    assert obs.source_profile == "fred_official_api"
    assert "unit-test-key" not in obs.source_url
    assert "api_key=%2A%2A%2A" in obs.source_url or "api_key=***" in obs.source_url


def test_eia_provider_parses_v2_observations(monkeypatch):
    cfg = deepcopy(
        config_manager.get_research_config().modules["commodity_market_data"][
            "special_commodity_market_data"
        ]
    )
    for item in cfg["series"]:
        if item["series_id"] == "CMD.OIL.WTI.SPOT.EIA.DAILY":
            item["active"] = True
    source_cfg = cfg["source_profiles"]["eia_official_api"]
    series = CommodityUniverseSelector(cfg).resolve(series_ids=["CMD.OIL.WTI.SPOT.EIA.DAILY"])

    class _Response:
        url = "https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key=unit-test-key"

        def raise_for_status(self):
            return None

        def json(self):
            return {
                "response": {
                    "total": 2,
                    "data": [
                        {"period": "2026-01-01", "value": "71.2"},
                        {"period": "2026-01-02", "value": None},
                    ]
                }
            }

    captured = {}

    def _fake_get(*args, **kwargs):
        captured.update(kwargs.get("params") or {})
        return _Response()

    monkeypatch.setenv("EIA_API_KEY", "unit-test-key")
    monkeypatch.setattr("research.special_commodity_market_data.request_get", _fake_get)

    result = EiaCommodityProvider("eia_official_api", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert len(result.observations) == 1
    obs = result.observations[0]
    assert obs.series_id == "CMD.OIL.WTI.SPOT.EIA.DAILY"
    assert obs.value == 71.2
    assert obs.source_profile == "eia_official_api"
    assert captured["start"] == "2026-01-01"
    assert captured["end"] == "2026-01-02"
    assert captured["facets[series][]"] == ["RWTC"]
    assert "unit-test-key" not in obs.source_url


def test_world_bank_provider_parses_pink_sheet_workbook(monkeypatch):
    cfg = deepcopy(
        config_manager.get_research_config().modules["commodity_market_data"][
            "special_commodity_market_data"
        ]
    )
    for item in cfg["series"]:
        if item["series_id"] == "CMD.METAL.COPPER.WORLDBANK.MONTHLY":
            item["active"] = True
    source_cfg = cfg["source_profiles"]["world_bank_public_dataset"]
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=["CMD.METAL.COPPER.WORLDBANK.MONTHLY"]
    )

    class _Response:
        url = "https://thedocs.worldbank.org/CMO-Historical-Data-Monthly.xlsx"
        content = b"unit-test-workbook"

        def raise_for_status(self):
            return None

    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: _Response(),
    )
    frame = pd.DataFrame(
        [
            ["World Bank Commodity Price Data", None],
            [None, None],
            [None, None],
            [None, None],
            [None, "Copper"],
            [None, "($/mt)"],
            ["2026M01", 8500.25],
            ["2026M02", None],
        ]
    )
    monkeypatch.setattr("pandas.read_excel", lambda *args, **kwargs: frame)

    result = WorldBankCommodityProvider("world_bank_public_dataset", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-02-28",
    )

    assert len(result.observations) == 1
    obs = result.observations[0]
    assert obs.series_id == "CMD.METAL.COPPER.WORLDBANK.MONTHLY"
    assert obs.observation_date == "2026-01-01"
    assert obs.value == 8500.25
    assert obs.unit == "USD/metric_ton"


def test_100ppi_provider_requires_series_mapping():
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["100ppi_public_web"]
    series = CommodityUniverseSelector(cfg).resolve(series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"])

    # Force a 100ppi provider call with an unmapped series to validate the gate.
    result = AkshareCommoditySpotProvider("100ppi_public_web", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert result.observations == []
    assert result.blockers[0]["reason"] == "missing_100ppi_series_mapping"


def test_100ppi_provider_fetches_mapped_akshare_rows(monkeypatch):
    cfg = deepcopy(
        config_manager.get_research_config().modules["commodity_market_data"][
            "special_commodity_market_data"
        ]
    )
    cfg["commodities"].append(
        {
            "commodity_id": "CN.CHEM.TEST.SPOT",
            "symbol": "TEST",
            "name": "Unit Test Chemical Spot",
            "category": "chemical",
            "commodity_type": "spot",
            "default_currency": "CNY",
            "default_unit": "CNY/ton",
            "active": True,
        }
    )
    cfg["series"].append(
        {
            "series_id": "CMD.CN.CHEM.TEST.100PPI.DAILY",
            "commodity_id": "CN.CHEM.TEST.SPOT",
            "venue": "100PPI",
            "source_profile": "100ppi_public_web",
            "source_symbol": "test",
            "frequency": "daily",
            "quote_type": "spot",
            "currency": "CNY",
            "unit": "CNY/ton",
            "active": True,
            "metadata": {
                "akshare_function": "unit_test_100ppi",
                "date_column": "日期",
                "value_column": "价格",
                "raw_unit": "CNY/ton",
                "region_or_spec": "unit-test",
                "source_url": "https://www.100ppi.com/test?token=secret",
            },
        }
    )
    captured = {}

    def _fake_100ppi(**kwargs):
        captured.update(kwargs)
        return [
            {"日期": "2026-01-01", "价格": "5500"},
            {"日期": "2026-01-03", "价格": "5600"},
        ]

    for item in cfg["series"]:
        if item["series_id"] == "CMD.CN.CHEM.TEST.100PPI.DAILY":
            item["metadata"].update(
                {
                    "akshare_start_argument": "start_day",
                    "akshare_end_argument": "end_day",
                    "akshare_date_format": "compact",
                }
            )
    fake_akshare = SimpleNamespace(unit_test_100ppi=_fake_100ppi)
    monkeypatch.setitem(sys.modules, "akshare", fake_akshare)
    source_cfg = cfg["source_profiles"]["100ppi_public_web"]
    series = CommodityUniverseSelector(cfg).resolve(series_ids=["CMD.CN.CHEM.TEST.100PPI.DAILY"])

    result = AkshareCommoditySpotProvider("100ppi_public_web", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert result.blockers == []
    assert len(result.observations) == 1
    obs = result.observations[0]
    assert obs.value == 5500
    assert obs.source_profile == "100ppi_public_web"
    assert obs.quality_flag == "aggregated_public_web"
    assert "secret" not in obs.source_url
    assert captured["start_day"] == "20260101"
    assert captured["end_day"] == "20260102"


def test_lme_akshare_provider_uses_sina_primary_without_fallback(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["lme_akshare_foreign_futures"]
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=["CMD.METAL.COPPER.LME3M.DAILY"]
    )
    calls = {"sina": 0, "eastmoney": 0}

    def _sina(symbol):
        calls["sina"] += 1
        assert symbol == "CAD"
        return pd.DataFrame(
            [
                {"date": "2016-07-11", "open": 4700, "high": 4800, "low": 4650, "close": 4750, "volume": 100, "position": 0},
                {"date": pd.Timestamp("2026-01-02"), "open": 9000, "high": 9100, "low": 8950, "close": 9050, "volume": 200, "position": 0},
            ]
        )

    def _eastmoney(symbol):
        calls["eastmoney"] += 1
        raise AssertionError("fallback must not run when Sina succeeds")

    fake_akshare = SimpleNamespace(
        futures_foreign_hist=_sina,
        futures_global_hist_em=_eastmoney,
        futures_hq_subscribe_exchange_symbol=lambda: pd.DataFrame(
            [{"symbol": "LME铜3个月", "code": "CAD"}]
        ),
    )
    monkeypatch.setattr(
        AkshareForeignFuturesProvider,
        "_load_akshare",
        staticmethod(lambda mode: fake_akshare),
    )

    result = AkshareForeignFuturesProvider(
        "lme_akshare_foreign_futures", source_cfg
    ).fetch(series, start_date="2026-01-01", end_date="2026-01-03")

    assert result.blockers == []
    assert calls == {"sina": 1, "eastmoney": 0}
    assert len(result.observations) == 1
    observation = result.observations[0]
    assert observation.value == 9050
    assert observation.source_symbol == "CAD"
    assert observation.metadata["actual_source_profile"] == "sina_foreign_futures"
    assert observation.metadata["open"] == 9000
    assert observation.metadata["volume"] == 200
    source_meta = result.metadata["series_metadata"][series[0].series_id]
    assert source_meta["lifecycle_start"] == "2016-07-11"
    assert source_meta["lifecycle_end"] == "2026-01-02"


def test_lme_akshare_provider_falls_back_to_eastmoney(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["lme_akshare_foreign_futures"]
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=["CMD.METAL.ALUMINIUM.LME3M.DAILY"]
    )

    def _sina(symbol):
        raise ConnectionError("unit-test primary failure")

    def _eastmoney(symbol):
        assert symbol == "LALT"
        return pd.DataFrame(
            [
                {"日期": "2013-06-21", "名称": "综合铝03", "开盘": 1780, "最高": 1810, "最低": 1770, "最新价": 1795, "总量": 0, "持仓": 0},
                {"日期": "2026-01-02", "名称": "综合铝03", "开盘": 2600, "最高": 2650, "最低": 2590, "最新价": 2630, "总量": 0, "持仓": 0},
            ]
        )

    fake_akshare = SimpleNamespace(
        futures_foreign_hist=_sina,
        futures_global_hist_em=_eastmoney,
        futures_hq_subscribe_exchange_symbol=lambda: pd.DataFrame(
            [{"symbol": "LME铝3个月", "code": "AHD"}]
        ),
    )
    monkeypatch.setattr(
        AkshareForeignFuturesProvider,
        "_load_akshare",
        staticmethod(lambda mode: fake_akshare),
    )

    result = AkshareForeignFuturesProvider(
        "lme_akshare_foreign_futures", source_cfg
    ).fetch(series, start_date="2026-01-02", end_date="2026-01-02")

    assert result.blockers == []
    assert len(result.observations) == 1
    assert result.observations[0].value == 2630
    assert result.observations[0].source_symbol == "LALT"
    assert result.observations[0].metadata["actual_source_profile"] == "eastmoney_global_futures"
    assert result.metadata["series_metadata"][series[0].series_id]["lifecycle_start"] == "2013-06-21"
    assert any(
        item["reason"] == "primary_provider_failed_fallback_used"
        for item in result.warnings
    )


def test_lme_provider_fills_isolated_primary_date_gap_from_eastmoney(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["lme_akshare_foreign_futures"]
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=[
            "CMD.METAL.COPPER.LME3M.DAILY",
            "CMD.METAL.ALUMINIUM.LME3M.DAILY",
            "CMD.METAL.TIN.LME3M.DAILY",
        ]
    )
    fallback_calls = []

    def _sina(symbol):
        rows = [
            {"date": "2021-02-24", "open": 100, "high": 110, "low": 90, "close": 105},
        ]
        if symbol in {"CAD", "AHD"}:
            rows.append(
                {"date": "2021-02-25", "open": 101, "high": 111, "low": 91, "close": 106}
            )
        return pd.DataFrame(rows)

    def _eastmoney(symbol):
        fallback_calls.append(symbol)
        assert symbol == "LTNT"
        return pd.DataFrame(
            [
                {"日期": "2021-02-25", "名称": "综合锡03", "开盘": 26910, "最高": 27500, "最低": 26385, "最新价": 26400},
            ]
        )

    fake_akshare = SimpleNamespace(
        futures_foreign_hist=_sina,
        futures_global_hist_em=_eastmoney,
        futures_hq_subscribe_exchange_symbol=lambda: pd.DataFrame(),
    )
    monkeypatch.setattr(
        AkshareForeignFuturesProvider,
        "_load_akshare",
        staticmethod(lambda mode: fake_akshare),
    )

    result = AkshareForeignFuturesProvider(
        "lme_akshare_foreign_futures", source_cfg
    ).fetch(series, start_date="2021-02-24", end_date="2021-02-25")

    assert result.blockers == []
    assert result.warnings == []
    assert fallback_calls == ["LTNT"]
    tin_rows = [
        item
        for item in result.observations
        if item.series_id == "CMD.METAL.TIN.LME3M.DAILY"
    ]
    assert len(tin_rows) == 2
    repaired = [item for item in tin_rows if item.observation_date == "2021-02-25"][0]
    assert repaired.value == 26400
    assert repaired.source_symbol == "LTNT"
    assert repaired.metadata["actual_source_profile"] == "eastmoney_global_futures"
    assert repaired.metadata["fallback_reason"] == "primary_date_missing"
    diagnostics = result.metadata["date_gap_fill"]
    assert diagnostics["fallback_requests"] == 1
    assert diagnostics["fallback_filled_dates"] == 1
    assert diagnostics["unresolved_dates"] == 0


def test_lme_provider_excludes_governed_nickel_suspension_from_gap_fill(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["lme_akshare_foreign_futures"]
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=[
            "CMD.METAL.COPPER.LME3M.DAILY",
            "CMD.METAL.ALUMINIUM.LME3M.DAILY",
            "CMD.METAL.NICKEL.LME3M.DAILY",
        ]
    )

    def _sina(symbol):
        rows = [
            {"date": "2022-03-08", "open": 100, "high": 110, "low": 90, "close": 105},
        ]
        if symbol in {"CAD", "AHD"}:
            rows.append(
                {"date": "2022-03-09", "open": 101, "high": 111, "low": 91, "close": 106}
            )
        return pd.DataFrame(rows)

    fake_akshare = SimpleNamespace(
        futures_foreign_hist=_sina,
        futures_global_hist_em=lambda symbol: (_ for _ in ()).throw(
            AssertionError("governed suspension must not request fallback")
        ),
        futures_hq_subscribe_exchange_symbol=lambda: pd.DataFrame(),
    )
    monkeypatch.setattr(
        AkshareForeignFuturesProvider,
        "_load_akshare",
        staticmethod(lambda mode: fake_akshare),
    )

    result = AkshareForeignFuturesProvider(
        "lme_akshare_foreign_futures", source_cfg
    ).fetch(series, start_date="2022-03-08", end_date="2022-03-09")

    assert result.warnings == []
    diagnostics = result.metadata["date_gap_fill"]
    assert diagnostics["governed_exception_dates"] == 1
    assert diagnostics["fallback_requests"] == 0
    assert diagnostics["unresolved_dates"] == 0


def test_lme_provider_reports_close_outside_intraday_range_without_rewrite(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["lme_akshare_foreign_futures"]
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=["CMD.METAL.COPPER.LME3M.DAILY"]
    )
    fake_akshare = SimpleNamespace(
        futures_foreign_hist=lambda symbol: pd.DataFrame(
            [
                {"date": "2021-03-23", "open": 9134, "high": 9145, "low": 8871, "close": 8870},
            ]
        ),
        futures_global_hist_em=lambda symbol: (_ for _ in ()).throw(
            AssertionError("single covered date must not request fallback")
        ),
        futures_hq_subscribe_exchange_symbol=lambda: pd.DataFrame(),
    )
    monkeypatch.setattr(
        AkshareForeignFuturesProvider,
        "_load_akshare",
        staticmethod(lambda mode: fake_akshare),
    )

    result = AkshareForeignFuturesProvider(
        "lme_akshare_foreign_futures", source_cfg
    ).fetch(series, start_date="2021-03-23", end_date="2021-03-23")

    assert result.observations[0].value == 8870
    assert result.observations[0].metadata["low"] == 8871
    assert result.observations[0].metadata["ohlc_consistency"] == "close_outside_intraday_range"
    assert result.metadata["quality_diagnostics"]["ohlc"]["close_outside_range"] == 1


def test_lme_governance_precedes_write_and_uses_only_observed_dates(monkeypatch, tmp_path):
    cfg = _research_config(tmp_path)
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    series_id = "CMD.METAL.COPPER.LME3M.DAILY"

    fake_akshare = SimpleNamespace(
        futures_foreign_hist=lambda symbol: pd.DataFrame(
            [
                {"date": "2016-07-11", "open": 4700, "high": 4800, "low": 4650, "close": 4750, "volume": 100, "position": 0},
                {"date": "2026-01-02", "open": 9000, "high": 9100, "low": 8950, "close": 9050, "volume": 200, "position": 0},
            ]
        ),
        futures_global_hist_em=lambda symbol: (_ for _ in ()).throw(
            AssertionError("fallback must not run")
        ),
        futures_foreign_detail=lambda symbol: pd.DataFrame(
            [
                ["交易品种", "伦敦铜(CFD差价合约并非期货)", "交易单位", "每手25吨", "报价单位", "美元/吨"],
                ["最小变动价位", "电话交易：0.5美元/吨 电子盘：0.25美元/吨", "合约交割月份", "LME三个月期货合约是连续合约，每日都有交割", "交易代码", "CAD"],
                ["上市交易所", "伦敦金属交易所", "交易时间", "LME Select北京时间（夏令时）08:00-02:00", "附加信息", None],
            ]
        ),
        futures_hq_subscribe_exchange_symbol=lambda: pd.DataFrame(
            [
                {"symbol": "LME铜3个月", "code": "CAD"},
                {"symbol": "LME铝3个月", "code": "AHD"},
                {"symbol": "LME锌3个月", "code": "ZSD"},
                {"symbol": "LME铅3个月", "code": "PBD"},
                {"symbol": "LME镍3个月", "code": "NID"},
                {"symbol": "LME锡3个月", "code": "SND"},
            ]
        ),
    )
    monkeypatch.setattr(
        AkshareForeignFuturesProvider,
        "_load_akshare",
        staticmethod(lambda mode: fake_akshare),
    )
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()

    result = SpecialCommodityPriceSyncService(storage, cfg).sync(
        series_ids=[series_id],
        start_date="2026-01-01",
        end_date="2026-01-04",
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["master_data_governance"] == "success"
    assert result["date_governance"] == "success"
    assert result["inserted"] == 1
    governance = {
        row["series_id"]: row for row in storage.read_dictionary()["master_governance"]
    }[series_id]
    assert governance["governance_status"] == "success"
    assert governance["lifecycle_start"] == "2016-07-11"
    governance_metadata = json.loads(governance["metadata_json"])
    assert governance_metadata["market_data_type"] == "cfd_proxy_to_lme_3m"
    assert governance_metadata["contract_multiplier"] == 25
    assert governance_metadata["tick_size"] == 0.25
    calendar = storage.read_publication_calendar(series_id=series_id)
    assert [row["observation_date"] for row in calendar] == ["2026-01-02"]
    assert calendar[0]["quality_flag"] == "aggregated_market_observed"


def test_special_commodity_sync_dry_run_uses_provider(monkeypatch, tmp_path):
    cfg = _research_config(tmp_path)

    monkeypatch.setenv("FRED_API_KEY", "unit-test-key")
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        _fake_fred_governance_get,
    )

    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    result = SpecialCommodityPriceSyncService(storage, cfg).sync(
        series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"],
        start_date="2026-01-01",
        end_date="2026-01-01",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["fetched_rows"] == 1
    assert result["would_write"] == 1
    assert result["master_data_governance"] == "success"
    assert result["date_governance"] == "success"
    assert result["source_date_count"] == 1
    assert storage.read_observations(series_id="CMD.OIL.WTI.SPOT.FRED.DAILY") == []


def test_special_commodity_storage_upsert_is_idempotent(monkeypatch, tmp_path):
    cfg = _research_config(tmp_path)

    monkeypatch.setenv("FRED_API_KEY", "unit-test-key")
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        _fake_fred_governance_get,
    )
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    service = SpecialCommodityPriceSyncService(storage, cfg)

    first = service.sync(
        series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"],
        start_date="2026-01-01",
        end_date="2026-01-01",
        dry_run=False,
    )
    second = service.sync(
        series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"],
        start_date="2026-01-01",
        end_date="2026-01-01",
        dry_run=False,
    )

    assert first["inserted"] == 1
    assert second["unchanged"] == 1
    rows = storage.read_observations(series_id="CMD.OIL.WTI.SPOT.FRED.DAILY")
    assert len(rows) == 1
    assert "unit-test-key" not in rows[0]["source_url"]
    dictionary = storage.read_dictionary()
    assert dictionary["master_governance"][0]["governance_status"] == "success"
    calendar = storage.read_publication_calendar(
        series_id="CMD.OIL.WTI.SPOT.FRED.DAILY"
    )
    assert [row["observation_date"] for row in calendar] == ["2026-01-01"]


def test_special_commodity_diagnostics_reads_latest_observations(monkeypatch, tmp_path):
    cfg = _research_config(tmp_path)

    monkeypatch.setenv("FRED_API_KEY", "unit-test-key")
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        _fake_fred_governance_get,
    )

    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityPriceSyncService(storage, cfg).sync(
        series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"],
        start_date="2026-01-01",
        end_date="2026-01-01",
        dry_run=False,
    )

    from research.special_commodity_market_data import SpecialCommodityReadService

    diagnostics = SpecialCommodityReadService(storage).diagnostics()
    assert diagnostics["currencies"] == ["USD"]
    assert diagnostics["units"] == ["USD/barrel"]
    assert diagnostics["latest_observations"][0]["series_id"] == "CMD.OIL.WTI.SPOT.FRED.DAILY"
    assert diagnostics["missing_master_governance"] == [
        item["series_id"]
        for item in storage.read_dictionary()["series"]
        if item["active"] and item["series_id"] != "CMD.OIL.WTI.SPOT.FRED.DAILY"
    ]


def test_manual_policy_event_sync_writes_policy_table(tmp_path):
    cfg = _research_config(tmp_path)
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    special_cfg["policy_events"] = [
        {
            "event_id": "thermal-coal-policy-2026-01",
            "commodity_id": "OIL.WTI.SPOT",
            "policy_type": "long_term_contract_reference",
            "effective_start": "2026-01-01",
            "effective_end": "2026-12-31",
            "currency": "CNY",
            "unit": "CNY/ton",
            "value_mid": 700,
            "source_profile": "manual_policy_event",
            "source_url": "manual://unit-test",
            "quality_flag": "manual_verified",
        }
    ]

    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    result = SpecialCommodityPolicyEventService(storage, special_cfg).sync(dry_run=False)

    assert result["status"] == "success"
    assert result["policy_events"] == 1
    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT commodity_id, policy_type, value_mid FROM commodity_policy_events WHERE event_id = ?",
            ("thermal-coal-policy-2026-01",),
        ).fetchone()
    assert row["commodity_id"] == "OIL.WTI.SPOT"
    assert row["policy_type"] == "long_term_contract_reference"
    assert row["value_mid"] == 700

    from research.special_commodity_market_data import SpecialCommodityReadService

    events = SpecialCommodityReadService(storage).policy_events(commodity_id="OIL.WTI.SPOT")
    assert events["count"] == 1
    assert events["events"][0]["event_id"] == "thermal-coal-policy-2026-01"


def test_calendar_governance_uses_only_source_observed_dates(monkeypatch, tmp_path):
    cfg = _research_config(tmp_path)
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    monkeypatch.setenv("FRED_API_KEY", "unit-test-key")
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        _fake_fred_governance_get,
    )

    result = SpecialCommodityCalendarGovernanceService(storage, special_cfg).run(
        series_ids=["CMD.OIL.WTI.SPOT.FRED.DAILY"],
        start_date="2026-01-01",
        end_date="2026-01-04",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["calendar_rows"] == 1
    assert result["missing_observations"] == 0
    assert result["would_write"] == 1
    assert result["per_source"]["fred_official_api"]["weekday_inference_used"] is False


def test_special_commodity_scheduler_report_compacts_normal_success():
    from scheduler.tasks import _format_special_commodity_scheduler_report

    report = _format_special_commodity_scheduler_report(
        {
            "status": "success",
            "run_id": 1,
            "dry_run": False,
            "target_series": 2,
            "venues": ["FRED"],
            "master_data_governance": "success",
            "date_governance": "success",
            "master_governance_records": 2,
            "source_date_count": 2,
            "fetched_rows": 2,
            "inserted": 2,
            "changed": 0,
            "unchanged": 0,
            "would_write": 0,
            "per_source": {
                "fred_official_api": {
                    "status": "success",
                    "series": 2,
                    "master_records": 2,
                    "calendar_rows": 2,
                    "fetched": 2,
                    "date_gap_fill": {
                        "fallback_filled_dates": 7,
                        "unresolved_dates": 0,
                    },
                    "quality_diagnostics": {
                        "ohlc": {"close_outside_range": 141},
                        "cross_source": {"conflict_count": 3},
                    },
                    "warnings": 0,
                    "blockers": 0,
                }
            },
            "warnings": [],
            "blockers": [],
        }
    )

    assert "特殊商品数据维护" in report
    assert "阻断:" not in report
    assert "告警:" not in report
    assert "fred_official_api" in report
    assert "主数据 `success`" in report
    assert "日期 `success`" in report
    assert "fallback_filled=7" in report
    assert "unresolved_gaps=0" in report
    assert "ohlc_outside=141" in report
    assert "source_conflicts=3" in report


def test_special_commodity_scheduled_window_is_bounded_and_explicit_dates_win():
    from scheduler.tasks import _resolve_special_commodity_sync_window

    assert _resolve_special_commodity_sync_window(
        None,
        None,
        lookback_days=10,
        as_of_date=date(2026, 7, 11),
    ) == ("2026-07-02", "2026-07-11")
    assert _resolve_special_commodity_sync_window(
        "2026-01-01",
        "2026-01-31",
        lookback_days=10,
        as_of_date=date(2026, 7, 11),
    ) == ("2026-01-01", "2026-01-31")


def test_special_commodity_schedule_is_isolated_from_domestic_futures_and_cache_warmup():
    scheduler_cfg = json.loads(
        (Path(__file__).parents[3] / "config" / "05_scheduler.json").read_text()
    )["scheduler_config"]
    jobs = scheduler_cfg["jobs"]
    special = jobs["special_commodity_price_sync"]
    assert special["enabled"] is True
    assert special["manual_only"] is False
    assert special["trigger"] == {
        "type": "cron",
        "day_of_week": "tue-sat",
        "hour": 8,
        "minute": 0,
        "second": 0,
    }
    assert special["parameters"]["scope_ids"] == [
        "lme_nonferrous",
        "eia_energy_oil",
    ]
    assert special["parameters"]["lookback_days"] == 10
    assert special["parameters"]["dry_run"] is False
    assert jobs["cache_warm_up"]["trigger"]["minute"] == 20
    assert "LME" not in jobs["futures_market_data_sync"]["parameters"]["exchanges"]

    enabled_at_0800 = [
        job_id
        for job_id, payload in jobs.items()
        if payload.get("enabled")
        and not payload.get("manual_only")
        and (payload.get("trigger") or {}).get("hour") == 8
        and (payload.get("trigger") or {}).get("minute", 0) == 0
    ]
    assert enabled_at_0800 == ["special_commodity_price_sync"]
