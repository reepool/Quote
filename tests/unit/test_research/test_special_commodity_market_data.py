from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from utils import config_manager
from research.special_commodity_market_data import (
    CommodityUniverseSelector,
    FredCommodityProvider,
    SpecialCommodityMasterDataService,
    SpecialCommodityPriceSyncService,
    SpecialCommodityStorageManager,
)


def _research_config(tmp_path: Path):
    cfg = deepcopy(config_manager.get_research_config())
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    special_cfg["enabled"] = True
    special_cfg["storage"]["database"] = str(tmp_path / "futures.db")
    return cfg


def test_special_commodity_master_schema_and_seed(tmp_path):
    cfg = _research_config(tmp_path)
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()

    result = SpecialCommodityMasterDataService(
        storage,
        cfg.modules["commodity_market_data"]["special_commodity_market_data"],
    ).sync()

    assert result["status"] == "success"
    assert result["instruments"] == 4
    assert result["series"] == 4

    dictionary = storage.read_dictionary()
    assert {item["commodity_id"] for item in dictionary["instruments"]} >= {
        "OIL.WTI.SPOT",
        "OIL.BRENT.SPOT",
        "METAL.COPPER.IMF",
        "METAL.ALUMINUM.IMF",
    }
    assert {item["series_id"] for item in dictionary["series"]} >= {
        "CMD.OIL.WTI.SPOT.FRED.DAILY",
        "CMD.METAL.COPPER.IMF.FRED.MONTHLY",
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
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=DCOILWTICO"

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


def test_special_commodity_sync_dry_run_uses_provider(monkeypatch, tmp_path):
    cfg = _research_config(tmp_path)

    class _Response:
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=DCOILWTICO"

        def raise_for_status(self):
            return None

        def json(self):
            return {"observations": [{"date": "2026-01-01", "value": "72.5"}]}

    monkeypatch.setenv("FRED_API_KEY", "unit-test-key")
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: _Response(),
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
    assert storage.read_observations(series_id="CMD.OIL.WTI.SPOT.FRED.DAILY") == []
