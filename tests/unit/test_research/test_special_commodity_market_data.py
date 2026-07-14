from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from utils import config_manager
from research.special_commodity_market_data import (
    AkShare100PpiSeriesCandidateAdapter,
    AkshareCommoditySpotProvider,
    AkshareForeignFuturesProvider,
    CommodityAdapterRegistry,
    CommodityObservation,
    CommodityDateGovernanceResult,
    CommodityMasterGovernanceResult,
    CommodityProviderResult,
    CommoditySeries,
    CommodityUniverseSelector,
    ConfiguredSourceChainProvider,
    EiaCommodityProvider,
    FredCommodityProvider,
    NbsMonthlyIndustrialOutputProvider,
    NbsOfficialAccessChallengeError,
    NbsOfficialSearchBlockedError,
    NbsProductionMaterialsGovernanceAdapter,
    NbsProductionMaterialsProvider,
    OfficialPublicIndicatorGovernanceAdapter,
    NdrcPolicyDiscoveryAdapter,
    SpecialCommodityPolicyDiscoveryService,
    SpecialCommodityMasterDataService,
    SpecialCommodityCalendarGovernanceService,
    SpecialCommodityPolicyEventService,
    SpecialCommodityReadService,
    SpecialCommoditySeriesCatalogService,
    SpecialCommodityPriceSyncService,
    SpecialCommodityGovernancePipeline,
    SpecialCommodityStorageManager,
    ShanghaiShippingExchangeCbcfiProvider,
    WorldBankCommodityProvider,
    _call_with_progress_logging,
    _observation_quality_diagnostics,
    _query_nbs_official_search,
    _actual_contract_series_blockers,
    _request_json_with_retry,
    _source_unit_matches,
)


def _research_config(tmp_path: Path):
    cfg = deepcopy(config_manager.get_research_config())
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    special_cfg["enabled"] = True
    special_cfg["storage"]["database"] = str(tmp_path / "futures.db")
    return cfg


def _configure_eb_candidate_fixture(module_cfg):
    catalog_cfg = module_cfg["series_catalog"]
    catalog_cfg["live_discovery"]["100ppi"]["enabled"] = False
    module_cfg["series"] = [
        item
        for item in module_cfg["series"]
        if item.get("source_symbol") != "EB"
        or item.get("source_profile") != "100ppi_public_web"
    ]
    catalog_cfg["candidates"] = [
        {
            "candidate_id": "100PPI.CHEMICAL.STYRENE",
            "provider_id": "100ppi_akshare",
            "source_profile": "100ppi_public_web",
            "source_symbol": "EB",
            "proposed_commodity_id": "CN.CHEMICAL.STYRENE.SPOT",
            "proposed_series_id": "CMD.CN.CHEMICAL.STYRENE.SPOT.100PPI.DAILY",
            "name": "China Styrene Spot Reference",
            "category": "chemical",
            "specification": "100ppi public-web product reference",
            "region": "China",
            "frequency": "daily",
            "currency": "CNY",
            "unit": "CNY/ton",
            "rollout_state": "discovered",
            "scheduler_eligible": False,
        }
    ]


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
    assert result["instruments"] == 42
    assert result["series"] >= 43

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
        "CN.CHEMICAL.METHANOL.SPOT",
        "CN.CHEMICAL.ETHYLENE_GLYCOL.SPOT",
        "CN.CHEMICAL.PVC.SPOT",
        "CN.CHEMICAL.POLYPROPYLENE.SPOT",
        "CN.CHEMICAL.STYRENE.SPOT",
        "CN.CHEMICAL.UREA.SPOT",
        "CN.CHEMICAL.CAUSTIC_SODA.SPOT",
        "CN.CHEMICAL.SODA_ASH.SPOT",
        "CN.BUILDING.GLASS.SPOT",
        "CN.ENERGY.ASPHALT.SPOT",
        "CN.ENERGY.LPG.SPOT",
        "CN.CHEMICAL.NATURAL_RUBBER.SPOT",
        "CN.FORESTRY.SOFTWOOD_PULP.SPOT",
        "CN.COAL.THERMAL.SHANXI_BLEND_5500.NBS",
        "CN.COAL.RAW_COAL.OUTPUT.NBS",
        "CN.COAL.FREIGHT.CBCFI.COMPOSITE",
        "CN.COAL.THERMAL.QHD_5500.LONG_TERM_POLICY",
    }
    assert {item["series_id"] for item in dictionary["series"]} >= {
        "CMD.OIL.WTI.SPOT.FRED.DAILY",
        "CMD.OIL.WTI.SPOT.EIA.DAILY",
        "CMD.METAL.COPPER.IMF.FRED.MONTHLY",
        "CMD.METAL.COPPER.WORLDBANK.MONTHLY",
        "CMD.METAL.COPPER.LME3M.DAILY",
        "CMD.METAL.ALUMINIUM.LME3M.DAILY",
        "CMD.CN.CHEMICAL.PTA.SPOT.100PPI.DAILY",
        "CMD.CN.CHEMICAL.METHANOL.SPOT.100PPI.DAILY",
        "CMD.CN.CHEMICAL.ETHYLENE_GLYCOL.SPOT.100PPI.DAILY",
        "CMD.CN.CHEMICAL.PVC.SPOT.100PPI.DAILY",
        "CMD.CN.CHEMICAL.POLYPROPYLENE.SPOT.100PPI.DAILY",
        "CMD.CN.COAL.THERMAL.SHANXI_BLEND_5500.NBS.TEN_DAY",
        "CMD.CN.COAL.RAW_COAL.OUTPUT.NBS.YTD.MONTHLY",
        "CMD.CN.COAL.FREIGHT.CBCFI.SSE.DAILY",
    }


def test_special_commodity_observation_watermarks_and_dry_run(tmp_path):
    cfg = _research_config(tmp_path)
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityMasterDataService(storage, special_cfg).sync()
    series = CommodityUniverseSelector(special_cfg).resolve()[0]
    observation = CommodityObservation(
        series_id=series.series_id,
        observation_date="2026-01-02",
        value=100.0,
        currency=series.currency,
        unit=series.unit,
        raw_value=100.0,
        raw_currency=series.currency,
        raw_unit=series.unit,
        source_profile=series.source_profile,
        source_url="https://example.test/commodity",
        quality_flag="unit_test",
        source_symbol=series.source_symbol,
        parser_version="unit-test",
        raw_payload_hash="commodity-h1",
    )

    dry_run = storage.upsert_observations(
        [observation], ingestion_run_id=None, dry_run=True
    )
    inserted = storage.upsert_observations(
        [observation], ingestion_run_id=None, dry_run=False
    )
    unchanged = storage.upsert_observations(
        [observation], ingestion_run_id=None, dry_run=False
    )
    changed_observation = CommodityObservation(
        **{
            **observation.__dict__,
            "value": 101.0,
            "raw_value": 101.0,
            "raw_payload_hash": "commodity-h2",
        }
    )
    changed = storage.upsert_observations(
        [changed_observation], ingestion_run_id=None, dry_run=False
    )

    assert dry_run["would_write"] == 1
    assert dry_run["changelog_written"] == 0
    assert inserted["changelog_written"] == 1
    assert unchanged["unchanged"] == 1
    assert unchanged["changelog_written"] == 0
    assert changed["changed"] == 1
    assert changed["changelog_written"] == 1
    assert "row_version" not in storage.read_observations(series_id=series.series_id)[0]
    with storage.get_connection() as conn:
        changes = conn.execute(
            """
            SELECT domain, dataset, series_id, row_version
            FROM data_change_log ORDER BY sequence_id
            """
        ).fetchall()
    assert [dict(row) for row in changes] == [
        {
            "domain": "commodity",
            "dataset": "commodity_price_observations",
            "series_id": series.series_id,
            "row_version": 1,
        },
        {
            "domain": "commodity",
            "dataset": "commodity_price_observations",
            "series_id": series.series_id,
            "row_version": 2,
        },
    ]
    storage.change_watermark_config = {"enabled": False}
    disabled_observation = CommodityObservation(
        **{
            **observation.__dict__,
            "observation_date": "2026-01-03",
            "raw_payload_hash": "commodity-disabled",
        }
    )
    disabled_write = storage.upsert_observations(
        [disabled_observation], ingestion_run_id=None, dry_run=False
    )
    assert disabled_write["inserted"] == 1
    assert disabled_write["changelog_written"] == 0
    assert len(storage.read_observations(series_id=series.series_id)) == 2
    with storage.get_connection() as conn:
        assert conn.execute("SELECT COUNT(*) FROM data_change_log").fetchone()[0] == 2


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

    methanol = selector.resolve(scope_id="cn_100ppi_methanol")
    assert [item.series_id for item in methanol] == [
        "CMD.CN.CHEMICAL.METHANOL.SPOT.100PPI.DAILY"
    ]

    ethylene_glycol = selector.resolve(scope_id="cn_100ppi_ethylene_glycol")
    assert [item.series_id for item in ethylene_glycol] == [
        "CMD.CN.CHEMICAL.ETHYLENE_GLYCOL.SPOT.100PPI.DAILY"
    ]

    pvc = selector.resolve(scope_id="cn_100ppi_pvc")
    assert [item.series_id for item in pvc] == [
        "CMD.CN.CHEMICAL.PVC.SPOT.100PPI.DAILY"
    ]

    polypropylene = selector.resolve(scope_id="cn_100ppi_polypropylene")
    assert [item.series_id for item in polypropylene] == [
        "CMD.CN.CHEMICAL.POLYPROPYLENE.SPOT.100PPI.DAILY"
    ]

    thermal_coal = selector.resolve(scope_id="cn_nbs_thermal_coal")
    assert [item.series_id for item in thermal_coal] == [
        "CMD.CN.COAL.THERMAL.SHANXI_BLEND_5500.NBS.TEN_DAY"
    ]

    raw_coal_output = selector.resolve(scope_id="cn_nbs_raw_coal_output")
    assert [item.series_id for item in raw_coal_output] == [
        "CMD.CN.COAL.RAW_COAL.OUTPUT.NBS.YTD.MONTHLY"
    ]

    cbcfi = selector.resolve(scope_id="cn_coal_cbcfi")
    assert [item.series_id for item in cbcfi] == [
        "CMD.CN.COAL.FREIGHT.CBCFI.SSE.DAILY"
    ]

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
    world_bank_fertilizers = selector.resolve(scope_id="world_bank_fertilizers")
    assert {item.series_id for item in world_bank_fertilizers} == {
        "CMD.FERTILIZER.PHOSPHATE_ROCK.WORLDBANK.MONTHLY",
        "CMD.FERTILIZER.DAP.WORLDBANK.MONTHLY",
        "CMD.FERTILIZER.TSP.WORLDBANK.MONTHLY",
        "CMD.FERTILIZER.UREA.WORLDBANK.MONTHLY",
        "CMD.FERTILIZER.POTASSIUM_CHLORIDE.WORLDBANK.MONTHLY",
    }
    assert {
        item.series_id: item.metadata["source_available_from"]
        for item in world_bank_fertilizers
    } == {
        "CMD.FERTILIZER.PHOSPHATE_ROCK.WORLDBANK.MONTHLY": "1960-01-01",
        "CMD.FERTILIZER.DAP.WORLDBANK.MONTHLY": "1967-01-01",
        "CMD.FERTILIZER.TSP.WORLDBANK.MONTHLY": "1960-01-01",
        "CMD.FERTILIZER.UREA.WORLDBANK.MONTHLY": "1960-01-01",
        "CMD.FERTILIZER.POTASSIUM_CHLORIDE.WORLDBANK.MONTHLY": "1960-01-01",
    }
    world_bank_agriculture = selector.resolve(scope_id="world_bank_agriculture")
    assert {item.series_id for item in world_bank_agriculture} == {
        "CMD.AGRI.PALM_OIL.WORLDBANK.MONTHLY",
        "CMD.AGRI.SOYBEANS.WORLDBANK.MONTHLY",
        "CMD.AGRI.SOYBEAN_OIL.WORLDBANK.MONTHLY",
        "CMD.AGRI.SOYBEAN_MEAL.WORLDBANK.MONTHLY",
        "CMD.AGRI.MAIZE.WORLDBANK.MONTHLY",
        "CMD.AGRI.WHEAT_US_HRW.WORLDBANK.MONTHLY",
        "CMD.AGRI.SUGAR_WORLD.WORLDBANK.MONTHLY",
        "CMD.AGRI.COTTON_A_INDEX.WORLDBANK.MONTHLY",
        "CMD.AGRI.RUBBER_TSR20.WORLDBANK.MONTHLY",
    }
    assert {item.unit for item in world_bank_agriculture} == {
        "USD/metric_ton",
        "USD/kg",
    }
    assert {
        item.metadata["source_available_from"] for item in world_bank_agriculture
    } == {"1960-01-01", "1999-01-01"}
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
    assert _source_unit_matches("USD/kg", "($/kg)")


def test_nbs_ten_day_title_period_parsing():
    assert NbsProductionMaterialsProvider.parse_period(
        "2026年6月下旬流通领域重要生产资料市场价格变动情况"
    ) == {
        "observation_date": "2026-06-30",
        "period_start": "2026-06-21",
        "period_end": "2026-06-30",
    }
    assert NbsProductionMaterialsProvider.parse_period(
        "流通领域重要生产资料市场价格变动情况（2014年1月1-10日）"
    ) == {
        "observation_date": "2014-01-10",
        "period_start": "2014-01-01",
        "period_end": "2014-01-10",
    }
    assert NbsProductionMaterialsProvider.parse_period(
        "2026年1月下旬流通领域重要生产资料市场价格变动情况"
    ) == {
        "observation_date": "2026-01-30",
        "period_start": "2026-01-21",
        "period_end": "2026-01-30",
    }
    assert NbsProductionMaterialsProvider.parse_period(
        "流通领域重要生产资料市场价格变动情况（2017年12月21日-30日）"
    ) == {
        "observation_date": "2017-12-30",
        "period_start": "2017-12-21",
        "period_end": "2017-12-30",
    }


def test_nbs_monthly_output_title_period_parsing():
    assert NbsMonthlyIndustrialOutputProvider.parse_period(
        "2026年1—2月份规模以上工业增加值增长6.3%"
    ) == {
        "observation_date": "2026-02-28",
        "period_start": "2026-01-01",
        "period_end": "2026-02-28",
    }
    assert NbsMonthlyIndustrialOutputProvider.parse_period(
        "2026年1—4月份规模以上工业增加值增长5.6%"
    ) == {
        "observation_date": "2026-04-30",
        "period_start": "2026-01-01",
        "period_end": "2026-04-30",
    }
    assert NbsMonthlyIndustrialOutputProvider.parse_period(
        "2026年3月份规模以上工业增加值增长5.7%"
    ) == {
        "observation_date": "2026-03-31",
        "period_start": "2026-01-01",
        "period_end": "2026-03-31",
    }
    assert NbsMonthlyIndustrialOutputProvider.parse_period(
        "2026年1月份规模以上工业增加值"
    ) is None
    assert NbsMonthlyIndustrialOutputProvider.parse_period(
        "2023年上半年规模以上工业增加值增长3.8%"
    ) == {
        "observation_date": "2023-06-30",
        "period_start": "2023-01-01",
        "period_end": "2023-06-30",
    }
    assert NbsMonthlyIndustrialOutputProvider.parse_period(
        "2025年前三季度规模以上工业增加值增长"
    ) == {
        "observation_date": "2025-09-30",
        "period_start": "2025-01-01",
        "period_end": "2025-09-30",
    }


@pytest.mark.parametrize(
    ("header", "row", "expected"),
    [
        (
            "<tr><td>指标</td><td>1—2月</td><td>1—2月</td></tr>"
            "<tr><td>指标</td><td>绝对量</td><td>同比增长（%）</td></tr>",
            "<tr><td>原煤（万吨）</td><td>76289</td><td>-0.3</td></tr>",
            76289.0,
        ),
        (
            "<tr><td>指标</td><td>5月</td><td>5月</td><td>1—5月</td><td>1—5月</td></tr>"
            "<tr><td>指标</td><td>绝对量</td><td>同比增长（%）</td><td>绝对量</td><td>同比增长（%）</td></tr>",
            "<tr><td>原煤（万吨）</td><td>39722</td><td>-1.7</td><td>198043</td><td>-0.3</td></tr>",
            198043.0,
        ),
    ],
)
def test_nbs_monthly_output_parses_only_published_ytd_value(
    monkeypatch, header, row, expected
):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(
        scope_id="cn_nbs_raw_coal_output"
    )[0]
    provider = NbsMonthlyIndustrialOutputProvider(
        item.source_profile, cfg["source_profiles"][item.source_profile]
    )
    html = f"<html><body><table>{header}{row}</table></body></html>"
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: SimpleNamespace(
            text=html,
            apparent_encoding="utf-8",
            encoding="utf-8",
            raise_for_status=lambda: None,
        ),
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_with_akshare_proxy",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("all proxy exits challenged")
        ),
    )
    observation = provider._parse_article(
        {
            "observation_date": "2026-05-31",
            "period_start": "2026-01-01",
            "period_end": "2026-05-31",
            "publication_date": "2026-06-16",
            "source_url": "https://www.stats.gov.cn/example.html",
        },
        item,
    )

    assert observation.value == expected
    assert observation.metadata["metric_type"] == "cumulative_ytd_output"
    assert observation.metadata["not_derived_monthly_value"] is True


def test_nbs_monthly_output_listing_ignores_unrelated_old_links(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = cfg["source_profiles"]["nbs_monthly_raw_coal_output"]
    provider = NbsMonthlyIndustrialOutputProvider(
        "nbs_monthly_raw_coal_output", source_cfg
    )
    html = """
    <html><body>
      <a href="/sj/zxfb/202302/t20230217_1896711.html">其他历史统计信息</a>
      <a href="/sj/zxfb/202606/t20260616_1963953.html">
        2026年5月份规模以上工业增加值增长5.8%
      </a>
      <a href="/sj/zxfbhjd/202605/t20260518_1963731.html">
        2026年1—4月份规模以上工业增加值增长5.6%
      </a>
    </body></html>
    """
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: SimpleNamespace(
            text=html,
            apparent_encoding="utf-8",
            encoding="utf-8",
            raise_for_status=lambda: None,
        ),
    )

    rows, oldest_publication = provider._listing_page(1)

    assert sorted(row["observation_date"] for row in rows) == [
        "2026-04-30",
        "2026-05-31",
    ]
    assert oldest_publication == date(2026, 5, 18)


def test_nbs_monthly_output_listing_rejects_access_challenge(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_monthly_raw_coal_output"])
    provider = NbsMonthlyIndustrialOutputProvider(
        "nbs_monthly_raw_coal_output", source_cfg
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: SimpleNamespace(
            text=(
                "<noscript>Please enable JavaScript and refresh the page.</noscript>"
                "<script>var c='/WZWSREL3NqL3p4ZmJoamQv/';</script>"
            ),
            apparent_encoding="utf-8",
            encoding="utf-8",
            raise_for_status=lambda: None,
        ),
    )

    with pytest.raises(NbsOfficialAccessChallengeError):
        provider._listing_page(1)


def test_nbs_monthly_output_listing_preserves_not_found_without_proxy(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_monthly_raw_coal_output"])
    provider = NbsMonthlyIndustrialOutputProvider(
        "nbs_monthly_raw_coal_output", source_cfg
    )

    class NotFoundError(RuntimeError):
        response = SimpleNamespace(status_code=404)

    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: (_ for _ in ()).throw(NotFoundError("404")),
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_with_akshare_proxy",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("404 must not use proxy fallback")
        ),
    )

    with pytest.raises(NotFoundError):
        provider._listing_page(67)


def test_nbs_monthly_output_complete_listing_skips_auxiliary_search(
    monkeypatch,
):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_monthly_raw_coal_output"])
    source_cfg["exact_only_max_periods"] = 1
    provider = NbsMonthlyIndustrialOutputProvider(
        "nbs_monthly_raw_coal_output", source_cfg
    )
    expected = provider._expected_periods(date(2025, 2, 28), date(2026, 5, 31))
    listing_rows = [
        {
            **item,
            "title": f"official-{item['observation_date']}",
            "source_url": f"https://www.stats.gov.cn/sj/zxfb/{item['observation_date']}.html",
            "publication_date": item["observation_date"],
        }
        for item in expected
    ]
    monkeypatch.setattr(
        provider,
        "_search_page",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("complete official listing must not use auxiliary search")
        ),
    )
    monkeypatch.setattr(
        provider,
        "_listing_page",
        lambda page: (listing_rows, date(2025, 1, 1)),
    )

    articles, warnings, diagnostics = provider._discover_articles(
        date(2025, 2, 28), date(2026, 5, 31)
    )

    assert len(articles) == len(expected)
    assert warnings == []
    assert diagnostics["unresolved_dates"] == 0
    assert diagnostics["auxiliary_search_warnings"] == 0


def test_nbs_monthly_output_hard_search_block_stops_remaining_gap_requests(
    monkeypatch,
):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_monthly_raw_coal_output"])
    source_cfg["exact_only_max_periods"] = 1
    provider = NbsMonthlyIndustrialOutputProvider(
        "nbs_monthly_raw_coal_output", source_cfg
    )
    expected = provider._expected_periods(date(2025, 2, 28), date(2026, 5, 31))
    listing_rows = [
        {
            **item,
            "title": f"official-{item['observation_date']}",
            "source_url": f"https://www.stats.gov.cn/sj/zxfb/{item['observation_date']}.html",
            "publication_date": item["observation_date"],
        }
        for item in expected[:-1]
    ]
    search_calls = []

    def blocked_search(**kwargs):
        search_calls.append(kwargs)
        raise NbsOfficialSearchBlockedError("current network address disabled")

    monkeypatch.setattr(provider, "_search_page", blocked_search)
    monkeypatch.setattr(
        provider,
        "_listing_page",
        lambda page: (listing_rows, date(2025, 1, 1)),
    )

    articles, warnings, diagnostics = provider._discover_articles(
        date(2025, 2, 28), date(2026, 5, 31)
    )

    assert len(search_calls) == 1
    assert search_calls[0]["sort"] == "relevance"
    assert len(articles) == len(expected) - 1
    assert diagnostics["unresolved_dates"] == 1
    assert diagnostics["auxiliary_search_warnings"] == 1
    assert any(
        item.get("reason") == "nbs_monthly_output_search_access_blocked"
        for item in warnings
    )


def test_nbs_official_search_hard_ip_block_opens_circuit_without_retry(
    monkeypatch,
):
    calls = []

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "ok": False,
                "code": -101,
                "msg": "当前网络地址已被禁用",
            },
        )

    monkeypatch.setattr(
        "research.special_commodity_market_data.request_post", fake_post
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.time.sleep",
        lambda seconds: (_ for _ in ()).throw(
            AssertionError("hard IP block must not sleep and retry")
        ),
    )

    with pytest.raises(NbsOfficialSearchBlockedError):
        _query_nbs_official_search(
            endpoint="https://api.so-gov.cn/query/s",
            site_code="bm36000002",
            query="原煤",
            page=1,
            page_size=20,
            sort="dateDesc",
            headers={},
            timeout=30.0,
            tls_config={},
            retry_cfg={"max_attempts": 3, "backoff_seconds": 5.0},
        )

    assert len(calls) == 1


def test_nbs_official_search_rotates_proxy_after_hard_ip_block(monkeypatch):
    direct_calls = []
    proxy_calls = []

    def fake_post(*args, **kwargs):
        direct_calls.append((args, kwargs))
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {
                "ok": False,
                "code": -101,
                "msg": "当前网络地址已被禁用",
            },
        )

    def fake_proxy(*args, **kwargs):
        proxy_calls.append((args, kwargs))
        return SimpleNamespace(
            json=lambda: {"ok": True, "resultDocs": [{"data": {"url": "x"}}]},
        )

    monkeypatch.setattr(
        "research.special_commodity_market_data.request_post", fake_post
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_with_akshare_proxy",
        fake_proxy,
    )

    documents = _query_nbs_official_search(
        endpoint="https://api.so-gov.cn/query/s",
        site_code="bm36000002",
        query="原煤",
        page=1,
        page_size=20,
        sort="dateDesc",
        headers={},
        timeout=30.0,
        tls_config={},
        retry_cfg={
            "max_attempts": 3,
            "backoff_seconds": 5.0,
            "blocked_proxy_rotation_attempts": 3,
        },
    )

    assert documents == [{"data": {"url": "x"}}]
    assert len(direct_calls) == 1
    assert len(proxy_calls) == 1
    assert proxy_calls[0][1]["attempts"] == 3


def test_nbs_official_search_recovers_transport_failure_through_proxy(monkeypatch):
    proxy_calls = []
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_post",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            ConnectionError("direct DNS failure")
        ),
    )

    def fake_proxy(*args, **kwargs):
        proxy_calls.append((args, kwargs))
        return SimpleNamespace(
            json=lambda: {"ok": True, "resultDocs": [{"data": {"url": "x"}}]},
        )

    monkeypatch.setattr(
        "research.special_commodity_market_data.request_with_akshare_proxy",
        fake_proxy,
    )

    documents = _query_nbs_official_search(
        endpoint="https://api.so-gov.cn/query/s",
        site_code="bm36000002",
        query="原煤",
        page=1,
        page_size=20,
        sort="dateDesc",
        headers={},
        timeout=30.0,
        tls_config={},
        retry_cfg={"blocked_proxy_rotation_attempts": 2},
    )

    assert documents == [{"data": {"url": "x"}}]
    assert len(proxy_calls) == 1
    assert proxy_calls[0][1]["attempts"] == 2


def test_nbs_official_search_transient_rate_limit_uses_bounded_backoff(
    monkeypatch,
):
    payloads = iter(
        [
            {"ok": False, "code": 429, "msg": "访问过于频繁，请稍后"},
            {"ok": True, "resultDocs": []},
        ]
    )
    calls = []
    sleeps = []

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        payload = next(payloads)
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: payload,
        )

    monkeypatch.setattr(
        "research.special_commodity_market_data.request_post", fake_post
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.time.sleep", sleeps.append
    )

    documents = _query_nbs_official_search(
        endpoint="https://api.so-gov.cn/query/s",
        site_code="bm36000002",
        query="原煤",
        page=1,
        page_size=20,
        sort="dateDesc",
        headers={},
        timeout=30.0,
        tls_config={},
        retry_cfg={"max_attempts": 3, "backoff_seconds": 5.0},
    )

    assert documents == []
    assert len(calls) == 2
    assert sleeps == [5.0]


def test_nbs_exact_discovery_checks_later_pages(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_production_material_market_price"])
    source_cfg["exact_search_max_pages"] = 3
    provider = NbsProductionMaterialsProvider(
        "nbs_production_material_market_price", source_cfg
    )
    calls = []

    def fake_search_page(*, page, sort, query):
        calls.append((page, sort, query))
        if page != 2:
            return []
        return [
            {
                "observation_date": "2017-01-10",
                "period_start": "2017-01-01",
                "period_end": "2017-01-10",
                "publication_date": "2017-01-14",
                "title": "流通领域重要生产资料市场价格变动情况（2017年1月1-10日）",
                "source_url": "https://www.stats.gov.cn/example.html",
            }
        ]

    monkeypatch.setattr(provider, "_search_page", fake_search_page)
    articles, warnings, diagnostics = provider._discover_articles(
        date(2017, 1, 1), date(2017, 1, 10)
    )

    assert [item[0] for item in calls] == [1, 2]
    assert [item["observation_date"] for item in articles] == ["2017-01-10"]
    assert not any(
        item.get("reason") == "nbs_unresolved_observation_periods"
        for item in warnings
    )
    assert diagnostics["unresolved_dates"] == 0


def test_nbs_search_business_failure_is_not_treated_as_empty(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    provider = NbsProductionMaterialsProvider(
        "nbs_production_material_market_price",
        cfg["source_profiles"]["nbs_production_material_market_price"],
    )
    response = SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {"ok": False, "code": -101, "msg": "network address disabled"},
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_post",
        lambda *args, **kwargs: response,
    )

    with pytest.raises(RuntimeError, match="business failure code=-101"):
        provider._search_page(page=1, sort="dateAsc", query="test")


def test_nbs_long_range_empty_broad_search_aborts_exact_scan(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_production_material_market_price"])
    source_cfg["search_max_pages_per_sort"] = 1
    source_cfg["exact_only_max_periods"] = 1
    provider = NbsProductionMaterialsProvider(
        "nbs_production_material_market_price", source_cfg
    )
    calls = []

    def empty_search(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(provider, "_search_page", empty_search)
    articles, warnings, diagnostics = provider._discover_articles(
        date(2017, 1, 1), date(2017, 1, 20)
    )

    assert articles == []
    assert len(calls) == 2
    assert {item["sort"] for item in calls} == {"dateAsc", "dateDesc"}
    assert any(
        item.get("reason") == "nbs_broad_discovery_empty_anomaly"
        for item in warnings
    )
    assert diagnostics["unresolved_dates"] == 2


def test_nbs_unresolved_period_is_reported_not_silently_omitted(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_production_material_market_price"])
    source_cfg["exact_only_max_periods"] = 100
    provider = NbsProductionMaterialsProvider(
        "nbs_production_material_market_price", source_cfg
    )
    monkeypatch.setattr(provider, "_search_page", lambda **kwargs: [])

    articles, warnings, diagnostics = provider._discover_articles(
        date(2017, 1, 1), date(2017, 1, 10)
    )

    assert articles == []
    unresolved = next(
        item
        for item in warnings
        if item.get("reason") == "nbs_unresolved_observation_periods"
    )
    assert unresolved["expected_periods"] == 1
    assert unresolved["missing_periods"] == 1
    assert unresolved["missing_samples"] == ["2017-01-10"]
    assert diagnostics["unresolved_dates"] == 1


def _cbcfi_html() -> str:
    return """
    <html><body><table>
      <thead>
        <tr><th>航线</th><th>单位</th><th>上期 2026-07-10</th>
        <th>本期 2026-07-13</th><th>与上期比涨跌</th></tr>
      </thead>
      <tbody>
        <tr><td>综合指数</td><td>点</td><td>877.88</td><td>920.45</td><td>42.57</td></tr>
      </tbody>
    </table></body></html>
    """


def test_sse_cbcfi_provider_parses_latest_official_period(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(scope_id="cn_coal_cbcfi")[0]
    response = SimpleNamespace(text=_cbcfi_html(), raise_for_status=lambda: None)
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: response,
    )
    provider = ShanghaiShippingExchangeCbcfiProvider(
        item.source_profile,
        cfg["source_profiles"][item.source_profile],
    )

    result = provider.fetch(
        [item], start_date="2026-07-10", end_date="2026-07-13"
    )

    assert result.blockers == []
    assert len(result.observations) == 2
    assert [item.observation_date for item in result.observations] == [
        "2026-07-10",
        "2026-07-13",
    ]
    assert [item.value for item in result.observations] == [877.88, 920.45]
    observation = result.observations[-1]
    assert observation.unit == "index_point"
    assert observation.currency == ""
    assert observation.metadata["data_kind"] == "industrial_indicator"
    assert observation.metadata["publication_date"] == "2026-07-13"
    assert observation.metadata["previous_value"] == 877.88
    assert result.warnings == []


def test_sse_cbcfi_provider_can_return_previous_public_period(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(scope_id="cn_coal_cbcfi")[0]
    response = SimpleNamespace(text=_cbcfi_html(), raise_for_status=lambda: None)
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: response,
    )
    provider = ShanghaiShippingExchangeCbcfiProvider(
        item.source_profile,
        cfg["source_profiles"][item.source_profile],
    )

    result = provider.fetch(
        [item], start_date="2026-07-10", end_date="2026-07-10"
    )

    assert result.blockers == []
    assert len(result.observations) == 1
    assert result.observations[0].observation_date == "2026-07-10"
    assert result.observations[0].value == 877.88


def test_sse_cbcfi_provider_blocks_unentitled_historical_backfill(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(scope_id="cn_coal_cbcfi")[0]
    response = SimpleNamespace(text=_cbcfi_html(), raise_for_status=lambda: None)
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: response,
    )
    provider = ShanghaiShippingExchangeCbcfiProvider(
        item.source_profile,
        cfg["source_profiles"][item.source_profile],
    )

    result = provider.fetch(
        [item], start_date="2011-12-07", end_date="2026-07-10"
    )

    assert len(result.observations) == 1
    assert result.observations[0].observation_date == "2026-07-10"
    assert result.blockers[0]["reason"] == "sse_cbcfi_public_history_requires_entitlement"


def test_official_public_indicator_governance_uses_explicit_series_semantics(
    monkeypatch,
):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(scope_id="cn_coal_cbcfi")[0]
    provider_result = CommodityProviderResult()

    class FakeProvider:
        def fetch(self, series, *, start_date, end_date):
            return provider_result

    adapter = OfficialPublicIndicatorGovernanceAdapter(
        cfg["source_profiles"][item.source_profile]
    )
    result = adapter.govern_master(
        [item], FakeProvider(), start_date="2026-07-13", end_date="2026-07-13"
    )

    assert result.blockers == []
    assert result.records[0]["governance_status"] == "success"
    assert result.records[0]["source_unit"] == "index_point"
    assert result.records[0]["lifecycle_start"] == "2011-12-07"
    assert result.records[0]["metadata"]["data_kind"] == "industrial_indicator"
    assert result.prefetched_result is provider_result


def test_nbs_official_observation_exception_is_governed_not_warned(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    source_cfg = deepcopy(cfg["source_profiles"]["nbs_production_material_market_price"])
    source_cfg["exact_only_max_periods"] = 100
    source_cfg["observation_exceptions"] = [
        {
            "observation_date": "2017-01-30",
            "reason": "spring_festival_release_cancelled",
            "evidence_url": "https://www.stats.gov.cn/example-schedule.html",
        }
    ]
    provider = NbsProductionMaterialsProvider(
        "nbs_production_material_market_price", source_cfg
    )
    monkeypatch.setattr(provider, "_search_page", lambda **kwargs: [])

    articles, warnings, diagnostics = provider._discover_articles(
        date(2017, 1, 21), date(2017, 1, 30)
    )

    assert articles == []
    assert warnings == []
    assert diagnostics["expected_periods"] == 1
    assert diagnostics["search_expected_periods"] == 0
    assert diagnostics["governed_exception_dates"] == 1
    assert diagnostics["unresolved_dates"] == 0


def test_nbs_provider_parses_official_coal_row_and_preserves_publication_date(monkeypatch):
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(scope_id="cn_nbs_thermal_coal")[0]
    provider = NbsProductionMaterialsProvider(
        item.source_profile,
        cfg["source_profiles"][item.source_profile],
    )
    monkeypatch.setattr(
        provider,
        "_discover_articles",
        lambda start, end: (
            [
                {
                    "observation_date": "2026-06-30",
                    "period_start": "2026-06-21",
                    "period_end": "2026-06-30",
                    "publication_date": "2026-07-04",
                    "title": "2026年6月下旬流通领域重要生产资料市场价格变动情况",
                    "source_url": "https://www.stats.gov.cn/example.html",
                }
                ],
                [],
                {
                    "enabled": True,
                    "expected_periods": 1,
                    "search_expected_periods": 1,
                    "discovered_periods": 1,
                    "governed_exception_dates": 0,
                    "governed_exception_samples": [],
                    "unresolved_dates": 0,
                    "unresolved_samples": [],
                    "publication_eligible_end": "2026-06-30",
                },
            ),
        )
    response = SimpleNamespace(
        text="""
        <table><tr><th>产品名称</th><th>单位</th><th>本期价格（元）</th></tr>
        <tr><td>山西优混（5500 大卡）</td><td>吨</td><td>854.6</td></tr></table>
        """,
        apparent_encoding="utf-8",
        encoding="utf-8",
        raise_for_status=lambda: None,
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.request_get",
        lambda *args, **kwargs: response,
    )

    result = provider.fetch([item], start_date="2026-06-21", end_date="2026-06-30")

    assert result.blockers == []
    assert len(result.observations) == 1
    observation = result.observations[0]
    assert observation.observation_date == "2026-06-30"
    assert observation.value == 854.6
    assert observation.currency == "CNY"
    assert observation.unit == "CNY/ton"
    assert observation.metadata["publication_date"] == "2026-07-04"
    assert observation.metadata["observation_period_start"] == "2026-06-21"
    assert observation.metadata["price_semantics"] == "wholesale_and_sales_market_price"


def test_nbs_master_governance_uses_source_row_evidence():
    cfg = config_manager.get_research_config().modules["commodity_market_data"][
        "special_commodity_market_data"
    ]
    item = CommodityUniverseSelector(cfg).resolve(scope_id="cn_nbs_thermal_coal")[0]
    observation = CommodityObservation(
        series_id=item.series_id,
        observation_date="2026-06-30",
        value=854.6,
        currency="CNY",
        unit="CNY/ton",
        raw_value=854.6,
        raw_currency="CNY",
        raw_unit="CNY/ton",
        source_profile=item.source_profile,
        source_url="https://www.stats.gov.cn/example.html",
        quality_flag="official_public_web",
        source_symbol=item.source_symbol,
        parser_version="unit-test",
        raw_payload_hash="nbs-test",
        metadata={
            "source_product_name": "山西优混（5500大卡）",
            "source_unit": "吨",
            "price_semantics": "wholesale_and_sales_market_price",
        },
    )

    class FakeProvider:
        def fetch(self, series, *, start_date, end_date):
            from research.special_commodity_market_data import CommodityProviderResult

            return CommodityProviderResult(observations=[observation])

    adapter = NbsProductionMaterialsGovernanceAdapter(
        cfg["source_profiles"][item.source_profile]
    )
    result = adapter.govern_master(
        [item],
        FakeProvider(),
        start_date="2026-06-21",
        end_date="2026-06-30",
    )

    assert result.blockers == []
    assert result.records[0]["governance_status"] == "success"
    assert result.records[0]["quality_flag"] == "official_master_verified"
    assert result.records[0]["lifecycle_start"] == "2014-01-10"


def test_nbs_legal_empty_window_reuses_verified_master_and_stays_success(
    tmp_path, monkeypatch
):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityMasterDataService(storage, module_cfg).sync()
    item = CommodityUniverseSelector(module_cfg).resolve(
        scope_id="cn_nbs_thermal_coal"
    )[0]
    verified = {
        "series_id": item.series_id,
        "commodity_id": item.commodity_id,
        "venue": item.venue,
        "source_profile": item.source_profile,
        "governance_status": "success",
        "quality_flag": "official_master_verified",
        "source_name": item.source_symbol,
        "source_frequency": item.frequency,
        "source_currency": item.currency,
        "source_unit": item.unit,
        "lifecycle_start": "2014-01-10",
        "lifecycle_end": None,
        "evidence_url": "https://www.stats.gov.cn/example.html",
        "evidence_hash": "verified-evidence",
        "governed_at": "2026-07-04T12:00:00+08:00",
        "metadata": {"latest_evidence_date": "2026-06-30"},
    }
    storage.upsert_master_governance([verified], dry_run=False)

    monkeypatch.setattr(
        NbsProductionMaterialsProvider,
        "fetch",
        lambda self, series, *, start_date, end_date: CommodityProviderResult(
            metadata={
                "date_gap_fill": {
                    "expected_periods": 0,
                    "unresolved_dates": 0,
                    "publication_eligible_end": "2026-07-09",
                }
            }
        ),
    )

    result = SpecialCommodityPriceSyncService(storage, cfg).sync(
        scope_id="cn_nbs_thermal_coal",
        start_date="2026-07-04",
        end_date="2026-07-13",
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["master_data_governance"] == "success"
    assert result["date_governance"] == "success"
    assert result["fetched_rows"] == 0
    assert result["warnings"] == []
    assert result["blockers"] == []
    stored = storage.read_master_governance([item.series_id])[item.series_id]
    assert stored["governance_status"] == "success"
    assert stored["evidence_hash"] == "verified-evidence"


def test_master_governance_refresh_failure_does_not_downgrade_verified_record(
    tmp_path,
):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityMasterDataService(storage, module_cfg).sync()
    item = CommodityUniverseSelector(module_cfg).resolve(
        scope_id="cn_nbs_thermal_coal"
    )[0]
    verified = {
        "series_id": item.series_id,
        "commodity_id": item.commodity_id,
        "venue": item.venue,
        "source_profile": item.source_profile,
        "governance_status": "success",
        "quality_flag": "official_master_verified",
        "source_name": item.source_symbol,
        "source_frequency": item.frequency,
        "source_currency": item.currency,
        "source_unit": item.unit,
        "lifecycle_start": "2014-01-10",
        "lifecycle_end": None,
        "evidence_url": "https://www.stats.gov.cn/example.html",
        "evidence_hash": "verified-evidence",
        "governed_at": "2026-07-04T12:00:00+08:00",
        "metadata": {},
    }
    storage.upsert_master_governance([verified], dry_run=False)
    blocked = dict(verified)
    blocked.update(
        governance_status="blocked",
        quality_flag="unverified",
        evidence_hash="failed-refresh",
    )

    counts = storage.upsert_master_governance([blocked], dry_run=False)

    assert counts == {"written": 0, "would_write": 0, "preserved_verified": 1}
    stored = storage.read_master_governance([item.series_id])[item.series_id]
    assert stored["governance_status"] == "success"
    assert stored["evidence_hash"] == "verified-evidence"


def test_official_api_json_request_retries_transient_failure(monkeypatch):
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok"}

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise ConnectionError("transient tls eof")
        return Response()

    monkeypatch.setattr("research.special_commodity_market_data.request_get", fake_get)
    response, payload = _request_json_with_retry(
        "https://example.test/data",
        params={},
        headers={},
        timeout=1,
        tls_config=None,
        retry_cfg={"max_attempts": 3, "backoff_seconds": 0},
        log_context="unit-test",
    )

    assert isinstance(response, Response)
    assert payload == {"status": "ok"}
    assert len(calls) == 2


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
    unresolved = next(
        item
        for item in result.warnings
        if item["reason"] == "world_bank_unresolved_monthly_values"
    )
    assert unresolved["missing_samples"] == ["2026-02-01"]
    assert result.metadata["date_gap_fill"]["unresolved_dates"] == 1

    governed_source_cfg = deepcopy(source_cfg)
    governed_source_cfg["observation_exceptions"] = [
        {
            "series_id": "CMD.METAL.COPPER.WORLDBANK.MONTHLY",
            "observation_date": "2026-02-01",
            "reason": "official_workbook_missing_value_marker",
            "evidence_url": "https://thedocs.worldbank.org/CMO-Historical-Data-Monthly.xlsx",
        }
    ]
    governed = WorldBankCommodityProvider(
        "world_bank_public_dataset", governed_source_cfg
    ).fetch(series, start_date="2026-01-01", end_date="2026-02-28")
    assert not any(
        item["reason"] == "world_bank_unresolved_monthly_values"
        for item in governed.warnings
    )
    assert governed.metadata["date_gap_fill"]["governed_exception_dates"] == 1


def test_world_bank_provider_retries_transient_workbook_failure(monkeypatch):
    cfg = deepcopy(
        config_manager.get_research_config().modules["commodity_market_data"][
            "special_commodity_market_data"
        ]
    )
    source_cfg = cfg["source_profiles"]["world_bank_public_dataset"]
    source_cfg["request_retry"] = {"max_attempts": 2, "backoff_seconds": 0}
    series = CommodityUniverseSelector(cfg).resolve(
        series_ids=["CMD.METAL.COPPER.WORLDBANK.MONTHLY"]
    )

    class _Response:
        url = "https://thedocs.worldbank.org/CMO-Historical-Data-Monthly.xlsx"
        content = b"unit-test-workbook"

        def raise_for_status(self):
            return None

    attempts = {"count": 0}

    def _fake_get(*args, **kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("transient workbook failure")
        return _Response()

    monkeypatch.setattr("research.special_commodity_market_data.request_get", _fake_get)
    monkeypatch.setattr(
        "pandas.read_excel",
        lambda *args, **kwargs: pd.DataFrame(
            [[None, "Copper"], [None, "($/mt)"], ["2026M01", 8500.25]]
        ),
    )

    result = WorldBankCommodityProvider("world_bank_public_dataset", source_cfg).fetch(
        series,
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert attempts["count"] == 2
    assert len(result.observations) == 1


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
                "raw_unit": "CNY/kg",
                "provider_value_multiplier_from_raw": 1000,
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
    assert obs.raw_value == 5.5
    assert obs.raw_unit == "CNY/kg"
    assert obs.source_profile == "100ppi_public_web"
    assert obs.quality_flag == "aggregated_public_web"
    assert "secret" not in obs.source_url
    assert captured["start_day"] == "20260101"
    assert captured["end_day"] == "20260102"
    diagnostics = result.metadata["quality_diagnostics"]["observations"]["series"]
    assert diagnostics[obs.series_id]["first_date"] == "2026-01-01"
    assert diagnostics[obs.series_id]["min_value"] == 5500
    assert diagnostics[obs.series_id]["unit"] == ["CNY/ton"]
    assert diagnostics[obs.series_id]["raw_unit"] == ["CNY/kg"]


def test_special_commodity_progress_wrapper_logs_completion(monkeypatch):
    messages = []
    monkeypatch.setattr(
        "research.special_commodity_market_data.logger.info",
        lambda message, *args: messages.append(message % args),
    )
    result = _call_with_progress_logging(
        lambda value: value + 1,
        kwargs={"value": 2},
        log_context="source=test series=TEST",
        interval_seconds=60,
    )
    assert result == 3
    assert any("call done context=source=test series=TEST" in item for item in messages)


def test_calendar_coverage_uses_persisted_exchange_evidence(tmp_path):
    cfg = _research_config(tmp_path)
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    with storage.get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE futures_trading_calendar (
                exchange TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                is_trading_day INTEGER NOT NULL,
                quality_flag TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO futures_trading_calendar VALUES (?, ?, ?, ?)",
            [
                ("CZCE", "2026-01-02", 1, "backfilled_verified"),
                ("CZCE", "2026-01-05", 1, "backfilled_verified"),
                ("CZCE", "2026-01-06", 1, "backfilled_verified"),
            ],
        )
    series = CommoditySeries(
        series_id="CMD.TEST.100PPI.DAILY",
        commodity_id="TEST",
        venue="100PPI",
        source_profile="100ppi_public_web",
        source_symbol="TEST",
        frequency="daily",
        quote_type="spot",
        currency="CNY",
        unit="CNY/ton",
        metadata={"expected_calendar_exchange": "CZCE"},
    )
    observations = [
        CommodityObservation(
            series_id=series.series_id,
            observation_date=value,
            value=100.0,
            currency="CNY",
            unit="CNY/ton",
            raw_value=100.0,
            raw_currency="CNY",
            raw_unit="CNY/ton",
            source_profile="100ppi_public_web",
            source_url="https://example.test",
            quality_flag="aggregated_public_web",
            source_symbol="TEST",
            parser_version="test",
            raw_payload_hash="hash",
        )
        for value in ("2026-01-02", "2026-01-06")
    ]
    diagnostics = SpecialCommodityGovernancePipeline(
        storage,
        cfg.modules["commodity_market_data"]["special_commodity_market_data"],
    )._calendar_coverage_diagnostics(
        [series],
        observations,
        start_date="2026-01-01",
        end_date="2026-01-31",
    )[series.series_id]
    assert diagnostics["expected_dates"] == 3
    assert diagnostics["missing_dates"] == 1
    assert diagnostics["missing_samples"] == ["2026-01-05"]
    assert diagnostics["coverage_ratio"] == 2 / 3
    assert diagnostics["longest_missing_trading_day_run"] == 1


def test_observation_quality_diagnostics_reports_jumps_and_units():
    rows = [
        CommodityObservation(
            series_id="CMD.TEST",
            observation_date=day,
            value=value,
            currency="CNY",
            unit="CNY/ton",
            raw_value=value,
            raw_currency="CNY",
            raw_unit="CNY/ton",
            source_profile="test",
            source_url="test",
            quality_flag="test",
            source_symbol="TEST",
            parser_version="test",
            raw_payload_hash=day,
        )
        for day, value in (("2025-12-31", 100.0), ("2026-01-02", 120.0))
    ]
    result = _observation_quality_diagnostics(rows)["series"]["CMD.TEST"]
    assert result["annual_counts"] == {"2025": 1, "2026": 1}
    assert result["min_value"] == 100.0
    assert round(result["largest_absolute_changes"][0]["pct_change"], 6) == 0.2


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


def test_observation_change_count_ignores_volatile_source_metadata(tmp_path):
    cfg = _research_config(tmp_path)
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityMasterDataService(
        storage,
        cfg.modules["commodity_market_data"]["special_commodity_market_data"],
    ).sync()

    base = CommodityObservation(
        series_id="CMD.METAL.COPPER.IMF.FRED.MONTHLY",
        observation_date="2026-01-01",
        value=12_345.0,
        currency="USD",
        unit="USD/metric_ton",
        raw_value=12_345.0,
        raw_currency="USD",
        raw_unit="USD/metric_ton",
        source_profile="fred_api",
        source_url="https://api.stlouisfed.org/fred/series/observations",
        quality_flag="official_verified",
        source_symbol="PCOPPUSDM",
        parser_version="fred_commodity_provider.v1",
        raw_payload_hash="vintage-a",
        metadata={"realtime_start": "2026-07-12"},
    )
    first = storage.upsert_observations([base], ingestion_run_id=None, dry_run=False)
    refreshed = CommodityObservation(
        **{
            **base.__dict__,
            "raw_payload_hash": "vintage-b",
            "metadata": {"realtime_start": "2026-07-13"},
        }
    )
    second = storage.upsert_observations(
        [refreshed], ingestion_run_id=None, dry_run=False
    )
    revised = CommodityObservation(
        **{
            **refreshed.__dict__,
            "value": 12_346.0,
            "raw_value": 12_346.0,
        }
    )
    third = storage.upsert_observations([revised], ingestion_run_id=None, dry_run=False)

    assert first == {
        "inserted": 1,
        "changed": 0,
        "unchanged": 0,
        "would_write": 0,
        "changelog_written": 1,
    }
    assert second == {
        "inserted": 0,
        "changed": 0,
        "unchanged": 1,
        "would_write": 0,
        "changelog_written": 0,
    }
    assert third == {
        "inserted": 0,
        "changed": 1,
        "unchanged": 0,
        "would_write": 0,
        "changelog_written": 1,
    }
    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT value, raw_payload_hash, metadata_json
            FROM commodity_price_observations
            WHERE series_id = ? AND observation_date = ? AND source_profile = ?
            """,
            (base.series_id, base.observation_date, base.source_profile),
        ).fetchone()
        change_count = conn.execute(
            "SELECT COUNT(*) FROM data_change_log WHERE dataset = ?",
            ("commodity_price_observations",),
        ).fetchone()[0]
    assert row["value"] == 12_346.0
    assert row["raw_payload_hash"] == "vintage-b"
    assert json.loads(row["metadata_json"])["realtime_start"] == "2026-07-13"
    assert change_count == 2


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
    assert result["changelog_written"] == 1
    assert result["event_summaries"][0]["effective_start"] == "2026-01-01"
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT commodity_id, policy_type, value_mid FROM commodity_policy_events WHERE event_id = ?",
            ("thermal-coal-policy-2026-01",),
        ).fetchone()
        changes = conn.execute(
            """
            SELECT domain, dataset, instrument_id
            FROM data_change_log ORDER BY sequence_id
            """
        ).fetchall()
    assert row["commodity_id"] == "OIL.WTI.SPOT"
    assert row["policy_type"] == "long_term_contract_reference"
    assert row["value_mid"] == 700
    assert [dict(item) for item in changes] == [
        {
            "domain": "policy",
            "dataset": "commodity_policy_events",
            "instrument_id": "OIL.WTI.SPOT",
        }
    ]
    assert not any(item["domain"] == "commodity" for item in changes)

    from research.special_commodity_market_data import SpecialCommodityReadService

    events = SpecialCommodityReadService(storage).policy_events(commodity_id="OIL.WTI.SPOT")
    assert events["count"] == 1
    assert events["events"][0]["event_id"] == "thermal-coal-policy-2026-01"


def test_configured_policy_event_rejects_invalid_range(tmp_path):
    cfg = _research_config(tmp_path)
    special_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    special_cfg["policy_events"] = [
        {
            "event_id": "invalid-policy-range",
            "commodity_id": "OIL.WTI.SPOT",
            "policy_type": "reasonable_range",
            "effective_start": "2026-01-01",
            "currency": "CNY",
            "unit": "CNY/ton",
            "value_low": 800,
            "value_high": 700,
            "source_profile": "manual_policy_event",
            "source_url": "manual://unit-test",
        }
    ]

    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    result = SpecialCommodityPolicyEventService(storage, special_cfg).sync(dry_run=True)

    assert result["status"] == "blocked"
    assert result["policy_events"] == 0
    assert result["blockers"][0]["validation_errors"] == ["policy_range_inverted"]


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
    assert "fallback_unresolved=0" in report
    assert "ohlc_outside=141" in report
    assert "source_conflicts=3" in report


def test_special_commodity_industrial_scope_windows_and_aggregation():
    from scheduler.tasks import (
        _aggregate_special_commodity_scope_results,
        _format_special_commodity_scheduler_report,
        _resolve_special_commodity_scope_run_window,
        _special_commodity_scope_run_due,
    )

    assert _special_commodity_scope_run_due(
        {"enabled": True, "run_days_of_month": [16, 17]},
        as_of_date=date(2026, 7, 16),
    )
    assert not _special_commodity_scope_run_due(
        {"enabled": True, "run_days_of_month": [16, 17]},
        as_of_date=date(2026, 7, 14),
    )
    assert not _special_commodity_scope_run_due(
        {"enabled": False}, as_of_date=date(2026, 7, 16)
    )
    assert _resolve_special_commodity_scope_run_window(
        {"window_mode": "provider_latest"}, as_of_date=date(2026, 7, 16)
    ) == (None, None)
    assert _resolve_special_commodity_scope_run_window(
        {"window_mode": "monthly", "lookback_months": 4},
        as_of_date=date(2026, 7, 16),
    ) == ("2026-04-01", "2026-07-16")

    result = _aggregate_special_commodity_scope_results(
        [
            {
                "scope_id": "cn_coal_cbcfi",
                "status": "success",
                "run_id": 1,
                "venues": ["SSE"],
                "target_series": 1,
                "fetched_rows": 2,
                "inserted": 0,
                "changed": 0,
                "unchanged": 2,
                "master_data_governance": "success",
                "date_governance": "success",
                "per_source": {},
                "warnings": [],
                "blockers": [],
            }
        ],
        [{"scope_id": "cn_nbs_raw_coal_output", "reason": "not_due"}],
    )
    assert result["status"] == "success"
    assert result["fetched_rows"] == 2
    assert result["run_ids"] == [1]
    report = _format_special_commodity_scheduler_report(
        result, title="大宗商品产业指标聚合同步"
    )
    assert "大宗商品产业指标聚合同步" in report
    assert "run_ids: `1`" in report
    assert "cn_coal_cbcfi" in report
    assert "cn_nbs_raw_coal_output" in report

    failed = _aggregate_special_commodity_scope_results(
        [
            {
                "scope_id": "cn_nbs_raw_coal_output",
                "status": "error",
                "blockers": [{"reason": "source_failed"}],
            },
            {
                "scope_id": "cn_coal_cbcfi",
                "status": "success",
                "fetched_rows": 2,
                "per_source": {
                    "sse_cbcfi_public_latest": {"status": "success"}
                },
            },
        ],
        [],
    )
    assert failed["status"] == "error"
    assert failed["fetched_rows"] == 2
    assert failed["blockers"][0]["scope_id"] == "cn_nbs_raw_coal_output"
    assert "cn_coal_cbcfi:sse_cbcfi_public_latest" in failed["per_source"]


@pytest.mark.asyncio
async def test_special_commodity_industrial_scope_failure_is_isolated(monkeypatch):
    import scheduler.tasks as task_module

    calls = []
    reports = []

    async def run_special_commodity_price_sync(**kwargs):
        calls.append(kwargs["scope_id"])
        if kwargs["scope_id"] == "broken_scope":
            raise RuntimeError("source unavailable")
        return {
            "status": "success",
            "dry_run": kwargs["dry_run"],
            "run_id": 7,
            "venues": ["SSE"],
            "target_series": 1,
            "fetched_rows": 2,
            "inserted": 0,
            "changed": 0,
            "unchanged": 2,
            "would_write": 0,
            "master_data_governance": "success",
            "date_governance": "success",
            "per_source": {},
            "warnings": [],
            "blockers": [],
        }

    async def send_task_report(**kwargs):
        reports.append(kwargs)
        return True

    monkeypatch.setattr(
        task_module,
        "data_manager",
        SimpleNamespace(
            run_special_commodity_price_sync=run_special_commodity_price_sync
        ),
    )
    task = task_module.ScheduledTasks.__new__(task_module.ScheduledTasks)
    task._active_tasks = set()
    task._send_task_report = send_task_report

    success = await task.special_commodity_industrial_indicator_sync(
        scope_runs=[
            {"scope_id": "broken_scope", "enabled": True},
            {
                "scope_id": "cn_coal_cbcfi",
                "enabled": True,
                "window_mode": "provider_latest",
            },
        ],
        dry_run=True,
    )

    assert success is False
    assert calls == ["broken_scope", "cn_coal_cbcfi"]
    assert task._active_tasks == set()
    assert len(reports) == 1
    content = reports[0]["report_data"]["content"]
    assert "大宗商品非价格产业指标聚合同步" in content
    assert "broken_scope" in content
    assert "cn_coal_cbcfi" in content


def test_policy_discovery_report_contains_copyable_review_commands():
    from scheduler.tasks import _format_special_commodity_scheduler_report

    report = _format_special_commodity_scheduler_report(
        {
            "status": "success",
            "dry_run": False,
            "documents": 1,
            "candidates": 1,
            "ready_for_promotion": 1,
            "pending_review": 0,
            "terminal_reviewed": 0,
            "document_write": {"inserted": 1},
            "candidate_write": {"inserted": 1},
            "review_actions": [
                {
                    "review_code": "93acac0c",
                    "document_number": "发改价格〔2022〕303号",
                    "policy_type": "long_term_transaction_reasonable_range",
                    "effective_start": "2022-05-01",
                    "value": "570.0-770.0 CNY/ton",
                }
            ],
        }
    )
    assert "待审核政策" in report
    assert "candidate_ref=93acac0c decision=approved" in report
    assert "candidate_ref=93acac0c decision=rejected" in report


def test_special_commodity_scheduled_window_is_bounded_and_explicit_dates_win():
    from scheduler.tasks import (
        _resolve_special_commodity_monthly_sync_window,
        _resolve_special_commodity_sync_window,
        _resolve_special_commodity_task_window,
    )

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
    assert _resolve_special_commodity_monthly_sync_window(
        None,
        None,
        lookback_months=6,
        as_of_date=date(2026, 7, 11),
    ) == ("2026-02-01", "2026-07-11")
    assert _resolve_special_commodity_monthly_sync_window(
        "2025-01-01",
        "2026-05-31",
        lookback_months=6,
        as_of_date=date(2026, 7, 11),
    ) == ("2025-01-01", "2026-05-31")
    assert _resolve_special_commodity_task_window(
        None,
        None,
        lookback_days=10,
        window_mode="provider_latest",
        as_of_date=date(2026, 7, 11),
    ) == (None, None)
    assert _resolve_special_commodity_task_window(
        "2026-07-10",
        "2026-07-13",
        lookback_days=10,
        window_mode="provider_latest",
        as_of_date=date(2026, 7, 11),
    ) == ("2026-07-10", "2026-07-13")


def test_special_commodity_schedules_split_overseas_and_domestic_spot_scopes():
    scheduler_cfg = json.loads(
        (Path(__file__).parents[3] / "config" / "05_scheduler.json").read_text()
    )["scheduler_config"]
    jobs = scheduler_cfg["jobs"]
    special = jobs["special_commodity_overseas_daily_price_sync"]
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
    domestic_spot = jobs["special_commodity_domestic_spot_price_sync"]
    assert domestic_spot["enabled"] is True
    assert domestic_spot["manual_only"] is False
    assert domestic_spot["trigger"] == {
        "type": "cron",
        "day_of_week": "mon-fri",
        "hour": 22,
        "minute": 30,
        "second": 0,
    }
    assert domestic_spot["parameters"]["scope_ids"] == [
        "cn_100ppi_chemical",
        "cn_100ppi_methanol",
        "cn_100ppi_ethylene_glycol",
        "cn_100ppi_pvc",
        "cn_100ppi_polypropylene",
        "cn_100ppi_styrene",
        "cn_100ppi_urea",
        "cn_100ppi_caustic_soda",
        "cn_100ppi_soda_ash",
        "cn_100ppi_glass",
        "cn_100ppi_asphalt",
        "cn_100ppi_lpg",
        "cn_100ppi_natural_rubber",
        "cn_100ppi_softwood_pulp",
        "cn_nbs_thermal_coal",
    ]
    assert domestic_spot["parameters"]["lookback_days"] == 10
    industrial = jobs["special_commodity_industrial_indicator_sync"]
    assert industrial["enabled"] is True
    assert industrial["manual_only"] is False
    assert industrial["trigger"] == {
        "type": "cron",
        "day_of_week": "mon-fri",
        "hour": 16,
        "minute": 30,
        "second": 0,
    }
    assert industrial["parameters"]["scope_runs"] == [
        {
            "scope_id": "cn_coal_cbcfi",
            "enabled": True,
            "window_mode": "provider_latest",
        },
        {
            "scope_id": "cn_nbs_raw_coal_output",
            "enabled": True,
            "window_mode": "monthly",
            "lookback_months": 4,
            "run_days_of_month": [15, 16, 17, 18, 19, 20, 21, 22],
        },
    ]
    assert industrial["parameters"]["dry_run"] is False
    assert special["parameters"]["lookback_days"] == 10
    assert special["parameters"]["dry_run"] is False
    monthly = jobs["special_commodity_international_monthly_price_sync"]
    assert monthly["enabled"] is True
    assert monthly["manual_only"] is False
    assert monthly["trigger"] == {
        "type": "cron",
        "day": "10,20",
        "hour": 8,
        "minute": 40,
        "second": 0,
    }
    assert monthly["parameters"]["scope_ids"] == [
        "fred_imf_metals",
        "world_bank_metals",
        "world_bank_fertilizers",
        "world_bank_agriculture",
    ]
    assert monthly["parameters"]["frequencies"] == ["monthly"]
    assert monthly["parameters"]["lookback_months"] == 6
    assert monthly["parameters"]["dry_run"] is False
    assert jobs["special_commodity_observation_backfill"]["manual_only"] is True
    assert jobs["special_commodity_policy_governance_sync"]["manual_only"] is False
    assert not {
        "special_commodity_price_sync",
        "special_commodity_cn_spot_sync",
        "special_commodity_price_monthly_sync",
        "special_commodity_price_backfill",
        "special_commodity_policy_discovery",
    }.intersection(jobs)
    assert jobs["cache_warm_up"]["trigger"]["minute"] == 20
    assert "LME" not in jobs["futures_market_data_sync"]["parameters"]["exchanges"]
    assert all(
        not scope_id.startswith("cn_100ppi_")
        for scope_id in special["parameters"]["scope_ids"]
    )

    enabled_at_0800 = [
        job_id
        for job_id, payload in jobs.items()
        if payload.get("enabled")
        and not payload.get("manual_only")
        and (payload.get("trigger") or {}).get("hour") == 8
        and (payload.get("trigger") or {}).get("minute", 0) == 0
    ]
    assert enabled_at_0800 == ["special_commodity_overseas_daily_price_sync"]


@pytest.mark.asyncio
async def test_semantic_special_commodity_tasks_reuse_one_private_runner():
    from scheduler.tasks import ScheduledTasks

    calls = []

    async def run_observation(**kwargs):
        calls.append(kwargs)
        return True

    task = ScheduledTasks.__new__(ScheduledTasks)
    task._run_special_commodity_observation_sync = run_observation

    assert await task.special_commodity_overseas_daily_price_sync(
        start_date="2026-07-01", end_date="2026-07-02", dry_run=True
    )
    assert await task.special_commodity_domestic_spot_price_sync(
        start_date="2026-07-01", end_date="2026-07-02", dry_run=True
    )
    assert await task.special_commodity_international_monthly_price_sync(
        start_date="2026-01-01", end_date="2026-06-30", dry_run=True
    )
    assert await task.special_commodity_observation_backfill(
        start_date="2026-07-01", end_date="2026-07-02", dry_run=True
    )

    assert [call["_task_id"] for call in calls] == [
        "special_commodity_overseas_daily_price_sync",
        "special_commodity_domestic_spot_price_sync",
        "special_commodity_international_monthly_price_sync",
        "special_commodity_observation_backfill",
    ]
    assert calls[0]["frequencies"] == ["daily"]
    assert calls[2]["frequencies"] == ["monthly"]
    for old_name in (
        "special_commodity_price_sync",
        "special_commodity_cn_spot_sync",
        "special_commodity_price_monthly_sync",
        "special_commodity_price_backfill",
        "special_commodity_policy_discovery",
    ):
        assert not hasattr(ScheduledTasks, old_name)


def test_special_commodity_evidence_storage_is_additive_and_idempotent(tmp_path):
    cfg = _research_config(tmp_path)
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityMasterDataService(
        storage,
        cfg.modules["commodity_market_data"]["special_commodity_market_data"],
    ).sync()

    document = {
        "document_id": "NDRC.2022.303.v1",
        "source_profile": "ndrc_official_policy",
        "source_url": "https://zfxxgk.ndrc.gov.cn/example",
        "document_number": "发改价格〔2022〕303号",
        "title": "关于进一步完善煤炭市场价格形成机制的通知",
        "published_date": "2022-02-24",
        "retrieved_at": "2026-07-12T12:00:00+08:00",
        "content_hash": "hash-v1",
        "content_text": "official policy evidence",
        "parser_version": "test.v1",
    }
    document_insert = storage.upsert_source_documents([document], dry_run=False)
    document_duplicate = storage.upsert_source_documents([document], dry_run=False)
    assert document_insert["inserted"] == 1
    assert document_insert["changelog_written"] == 1
    assert document_duplicate["unchanged"] == 1
    assert document_duplicate["changelog_written"] == 0

    candidate = {
        "candidate_id": "NDRC.2022.303.QHD5500",
        "document_id": document["document_id"],
        "commodity_id": "CN.COAL.THERMAL.QHD_5500.LONG_TERM_POLICY",
        "policy_type": "price_range",
        "review_status": "pending_review",
        "confidence": 0.95,
        "effective_start": "2022-05-01",
        "currency": "CNY",
        "unit": "CNY/ton",
        "value_low": 570.0,
        "value_high": 770.0,
        "field_lineage": {"value_low": "document:price-range"},
    }
    candidate_insert = storage.upsert_policy_candidates([candidate], dry_run=False)
    candidate_duplicate = storage.upsert_policy_candidates([candidate], dry_run=False)
    assert candidate_insert["inserted"] == 1
    assert candidate_insert["changelog_written"] == 1
    assert candidate_duplicate["unchanged"] == 1
    assert candidate_duplicate["changelog_written"] == 0
    assert storage.read_source_documents()[0]["document_number"] == "发改价格〔2022〕303号"
    assert storage.read_policy_candidates()[0]["review_status"] == "pending_review"
    with storage.get_connection() as conn:
        changes = conn.execute(
            """
            SELECT domain, dataset FROM data_change_log ORDER BY sequence_id
            """
        ).fetchall()
    assert [dict(row) for row in changes] == [
        {"domain": "policy", "dataset": "commodity_source_documents"},
        {"domain": "policy", "dataset": "commodity_policy_candidates"},
    ]

    # Existing price and policy repositories remain available after additive migration.
    assert storage.read_dictionary()["series"]
    assert storage.read_policy_events() == []


def test_policy_document_metadata_correction_advances_only_policy_watermark(tmp_path):
    cfg = _research_config(tmp_path)
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    document = {
        "document_id": "NDRC.METADATA.CORRECTION",
        "source_profile": "ndrc_official_policy",
        "source_url": "https://www.ndrc.gov.cn/policy.html",
        "document_number": "draft-1",
        "title": "Draft policy title",
        "published_date": "2026-07-01",
        "retrieved_at": "2026-07-12T12:00:00+08:00",
        "content_hash": "stable-content-hash",
        "content_text": "unchanged policy body",
        "parser_version": "test.v1",
    }
    corrected = {
        **document,
        "document_number": "final-1",
        "title": "Final policy title",
        "published_date": "2026-07-02",
        "retrieved_at": "2026-07-13T12:00:00+08:00",
    }

    assert storage.upsert_source_documents([document], dry_run=False)["inserted"] == 1
    corrected_write = storage.upsert_source_documents([corrected], dry_run=False)
    assert corrected_write["changed"] == 1
    assert corrected_write["changelog_written"] == 1
    assert storage.upsert_source_documents([corrected], dry_run=False)["unchanged"] == 1

    persisted = storage.read_source_documents()[0]
    assert persisted["document_number"] == "final-1"
    assert persisted["title"] == "Final policy title"
    assert persisted["published_date"] == "2026-07-02"
    assert "row_version" not in persisted
    with storage.get_connection() as conn:
        changes = conn.execute(
            """
            SELECT domain, dataset, change_type, old_hash, new_hash, row_version
            FROM data_change_log ORDER BY sequence_id
            """
        ).fetchall()
    assert [row["domain"] for row in changes] == ["policy", "policy"]
    assert [row["dataset"] for row in changes] == [
        "commodity_source_documents",
        "commodity_source_documents",
    ]
    assert [row["change_type"] for row in changes] == ["insert", "update"]
    assert changes[1]["old_hash"] != changes[1]["new_hash"]
    assert changes[1]["row_version"] == 2


def test_ndrc_policy_discovery_versions_evidence_and_keeps_policy_semantics(monkeypatch, tmp_path):
    catalog_url = "https://www.ndrc.gov.cn/xxgk/zcfb/"
    document_url = "https://www.ndrc.gov.cn/xxgk/zcfb/tz/202202/policy.html"
    catalog = SimpleNamespace(
        text=f'<a href="{document_url}">煤炭政策</a>',
        url=catalog_url,
        headers={"Content-Type": "text/html"},
        encoding="utf-8",
    )
    document = SimpleNamespace(
        text="""
        <html><title>关于进一步完善煤炭市场价格形成机制的通知</title>
        <body>发改价格〔2022〕303号 2022年2月25日
        秦皇岛港下水煤（5500千卡）中长期交易价格合理区间570-770元/吨，
        自2022年5月1日起执行。</body></html>
        """,
        url=document_url,
        headers={"Content-Type": "text/html"},
        encoding="utf-8",
    )

    def fake_request(url, **kwargs):
        return catalog if url == catalog_url else document

    monkeypatch.setattr(
        "research.special_commodity_market_data._request_with_retry",
        fake_request,
    )
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    module_cfg["policy_discovery"]["ndrc"]["catalog_urls"] = [catalog_url]
    module_cfg["policy_discovery"]["ndrc"]["document_urls"] = []
    storage = SpecialCommodityStorageManager(cfg)
    result = SpecialCommodityPolicyDiscoveryService(storage, module_cfg).run(
        start_date="2022-01-01",
        end_date="2022-12-31",
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["documents"] == 1
    assert result["candidates"] == 1
    assert result["ready_for_promotion"] == 1
    assert result["event_reconciliation"]["inserted"] == 1
    candidate = storage.read_policy_candidates()[0]
    review_code = candidate["candidate_id"].rsplit(".", 1)[-1][:8]
    assert storage.resolve_policy_candidate(review_code)["candidate_id"] == candidate["candidate_id"]
    assert storage.resolve_policy_candidate("发改价格〔2022〕303号")["candidate_id"] == candidate["candidate_id"]
    assert candidate["value_low"] == 570.0
    assert candidate["value_high"] == 770.0
    assert candidate["value_mid"] is None
    assert candidate["effective_start"] == "2022-05-01"
    assert json.loads(candidate["metadata_json"])["not_observed_transaction_price"] is True
    assert storage.set_policy_candidate_review_status(
        candidate_id=candidate["candidate_id"],
        review_status="approved",
        reviewer="unit-test",
        notes="official fields verified",
    ) is True
    promotion = SpecialCommodityPolicyEventService(storage, module_cfg).promote_approved_candidates(
        dry_run=False
    )
    assert promotion["status"] == "success"
    assert promotion["inserted"] == 0
    assert promotion["already_represented"] == 1
    assert promotion["unchanged"] == 1
    promoted = storage.read_policy_events()[0]
    assert promoted["value_low"] == 570.0
    assert promoted["value_high"] == 770.0
    assert promoted["value_mid"] is None
    rerun = SpecialCommodityPolicyDiscoveryService(storage, module_cfg).run(
        start_date="2022-01-01",
        end_date="2022-12-31",
        dry_run=False,
    )
    assert rerun["status"] == "success"
    assert rerun["terminal_reviewed"] == 1
    assert rerun["review_actions"] == []
    assert storage.read_policy_candidates()[0]["review_status"] == "approved"


def test_ndrc_policy_parser_fails_closed_for_ambiguous_contract_text():
    adapter = NdrcPolicyDiscoveryAdapter({"catalog_urls": ["https://www.ndrc.gov.cn/"]})
    document = {
        "document_id": "NDRC.ambiguous.v1",
        "title": "煤炭中长期合同工作通知",
        "source_url": "https://www.ndrc.gov.cn/ambiguous.html",
        "document_number": "",
    }
    candidate = adapter._parse_candidate(document, "煤炭中长期合同价格机制应保持合理稳定。")
    assert candidate is not None
    assert candidate["review_status"] == "pending_review"
    assert candidate["commodity_id"] is None
    assert candidate["value_low"] is None
    assert candidate["value_mid"] is None


def test_approved_policy_candidate_does_not_duplicate_existing_semantic_event(tmp_path):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    service = SpecialCommodityPolicyEventService(storage, module_cfg)
    assert service.sync(dry_run=False)["inserted"] == 1
    document = {
        "document_id": "NDRC.2022.303.DEDUP",
        "source_profile": "ndrc_official_policy_event",
        "source_url": "https://www.ndrc.gov.cn/303.html",
        "document_number": "发改价格〔2022〕303号",
        "title": "关于进一步完善煤炭市场价格形成机制的通知",
        "published_date": "2022-02-25",
        "retrieved_at": "2026-07-12T00:00:00+08:00",
        "content_hash": "dedup-test",
        "content_type": "text/html",
        "parser_version": "test.v1",
    }
    storage.upsert_source_documents([document], dry_run=False)
    candidate = {
        "candidate_id": "NDRC.CANDIDATE.DEDUP",
        "document_id": document["document_id"],
        "commodity_id": "CN.COAL.THERMAL.QHD_5500.LONG_TERM_POLICY",
        "policy_type": "long_term_transaction_reasonable_range",
        "review_status": "approved",
        "confidence": 1.0,
        "effective_start": "2022-05-01",
        "currency": "CNY",
        "unit": "CNY/ton",
        "value_low": 570.0,
        "value_high": 770.0,
        "value_mid": None,
    }
    storage.upsert_policy_candidates([candidate], dry_run=False)
    promotion = service.promote_approved_candidates(dry_run=False)
    assert promotion["already_represented"] == 1
    assert promotion["unchanged"] == 1
    assert len(storage.read_policy_events()) == 1


def test_special_commodity_indicator_api_contract_returns_only_indicator_series(tmp_path):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    module_cfg["commodities"].append(
        {
            "commodity_id": "CN.COAL.PORT.INVENTORY",
            "symbol": "COAL_PORT_INVENTORY",
            "name": "Coal Port Inventory",
            "category": "coal",
            "commodity_type": "industrial_indicator",
            "default_currency": "",
            "default_unit": "10k_ton",
            "active": True,
        }
    )
    module_cfg["series"].append(
        {
            "series_id": "CMD.CN.COAL.PORT.INVENTORY.TEST.DAILY",
            "commodity_id": "CN.COAL.PORT.INVENTORY",
            "venue": "TEST",
            "source_profile": "test_indicator",
            "source_symbol": "inventory",
            "frequency": "daily",
            "quote_type": "industrial_indicator",
            "currency": "",
            "unit": "10k_ton",
            "active": True,
            "metadata": {"data_kind": "industrial_indicator"},
        }
    )
    storage = SpecialCommodityStorageManager(cfg)
    storage.initialize()
    SpecialCommodityMasterDataService(storage, module_cfg).sync()
    result = SpecialCommodityReadService(storage).indicators(category="coal")
    assert result["status"] == "success"
    assert result["series_count"] == 3
    expected = {
        "CMD.CN.COAL.PORT.INVENTORY.TEST.DAILY",
        "CMD.CN.COAL.FREIGHT.CBCFI.SSE.DAILY",
        "CMD.CN.COAL.RAW_COAL.OUTPUT.NBS.YTD.MONTHLY",
    }
    assert {item["series_id"] for item in result["series"]} == expected
    assert result["observations"] == {series_id: [] for series_id in expected}


def test_actual_contract_price_requires_complete_contract_semantics():
    incomplete = CommoditySeries(
        series_id="CMD.CN.COAL.CONTRACT.TEST.MONTHLY",
        commodity_id="CN.COAL.CONTRACT.TEST",
        venue="TEST",
        source_profile="test",
        source_symbol="contract",
        frequency="monthly",
        quote_type="actual_contract_price",
        currency="CNY",
        unit="CNY/ton",
        metadata={"data_kind": "actual_contract_price"},
    )
    blockers = _actual_contract_series_blockers([incomplete])
    assert blockers[0]["reason"] == "actual_contract_series_semantics_incomplete"
    assert "stable_source_verified" in blockers[0]["missing_fields"]

    complete = CommoditySeries(
        **{
            **incomplete.__dict__,
            "metadata": {
                "data_kind": "actual_contract_price",
                "contract_scope": "annual_long_term_contract_monthly_settlement",
                "specification": "5500 kcal",
                "region": "Qinhuangdao",
                "tax_basis": "tax_inclusive",
                "freight_basis": "FOB",
                "stable_source_verified": True,
            },
        }
    )
    assert _actual_contract_series_blockers([complete]) == []


def test_special_commodity_series_catalog_is_idempotent_and_not_scheduled(tmp_path):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    _configure_eb_candidate_fixture(module_cfg)
    storage = SpecialCommodityStorageManager(cfg)
    service = SpecialCommoditySeriesCatalogService(storage, module_cfg)
    first = service.sync(dry_run=False)
    second = service.sync(dry_run=False)
    assert first["status"] == "success"
    assert first["candidates"] == 1
    assert first["inserted"] == 1
    assert second["unchanged"] == 1
    assert second["scheduler_eligible"] == 0
    state_counts = {}
    for row in storage.read_series_candidates():
        state_counts[row["rollout_state"]] = state_counts.get(row["rollout_state"], 0) + 1
    assert state_counts == {"discovered": 1}
    selector = CommodityUniverseSelector(module_cfg)
    assert all("BENZENE" not in item.series_id for item in selector.resolve(categories=["all"]))


def test_100ppi_live_discovery_only_returns_symbols_missing_from_production(
    tmp_path, monkeypatch
):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    fake_akshare = SimpleNamespace(
        futures_spot_price=lambda **kwargs: pd.DataFrame(
            {"symbol": ["EB", "NEW1"], "spot_price": [8000.0, 1000.0]}
        )
    )
    monkeypatch.setattr(
        "research.special_commodity_market_data.importlib.import_module",
        lambda name: fake_akshare,
    )

    rows = AkShare100PpiSeriesCandidateAdapter(
        module_cfg,
        {"lookback_days": 1, "progress_log_interval_seconds": 1},
    ).discover_candidates()

    assert [item["source_symbol"] for item in rows] == ["NEW1"]
    assert rows[0]["rollout_state"] == "discovered"
    assert rows[0]["scheduler_eligible"] is False
    assert rows[0]["diagnostics"]["reason"] == "new_source_symbol_requires_semantic_review"


def test_known_100ppi_expansion_scopes_use_the_existing_production_selector(tmp_path):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    selector = CommodityUniverseSelector(module_cfg)
    expected = {
        "cn_100ppi_styrene": "CMD.CN.CHEMICAL.STYRENE.SPOT.100PPI.DAILY",
        "cn_100ppi_urea": "CMD.CN.CHEMICAL.UREA.SPOT.100PPI.DAILY",
        "cn_100ppi_caustic_soda": "CMD.CN.CHEMICAL.CAUSTIC_SODA.SPOT.100PPI.DAILY",
        "cn_100ppi_soda_ash": "CMD.CN.CHEMICAL.SODA_ASH.SPOT.100PPI.DAILY",
        "cn_100ppi_glass": "CMD.CN.BUILDING.GLASS.SPOT.100PPI.DAILY",
        "cn_100ppi_asphalt": "CMD.CN.ENERGY.ASPHALT.SPOT.100PPI.DAILY",
        "cn_100ppi_lpg": "CMD.CN.ENERGY.LPG.SPOT.100PPI.DAILY",
        "cn_100ppi_natural_rubber": "CMD.CN.CHEMICAL.NATURAL_RUBBER.SPOT.100PPI.DAILY",
        "cn_100ppi_softwood_pulp": "CMD.CN.FORESTRY.SOFTWOOD_PULP.SPOT.100PPI.DAILY",
    }

    for scope_id, series_id in expected.items():
        assert [item.series_id for item in selector.resolve(scope_id=scope_id)] == [series_id]

    caustic_soda = selector.resolve(scope_id="cn_100ppi_caustic_soda")[0]
    assert caustic_soda.metadata["expected_calendar_exchange"] == "CZCE"
    assert caustic_soda.metadata["source_available_from"] == "2023-09-15"
    soda_ash = selector.resolve(scope_id="cn_100ppi_soda_ash")[0]
    assert soda_ash.metadata["source_available_from"] == "2020-06-01"
    glass = selector.resolve(scope_id="cn_100ppi_glass")[0]
    assert glass.metadata["source_available_from"] == "2013-01-04"
    assert glass.metadata["raw_unit"] == "CNY/m2"
    assert glass.metadata["provider_value_multiplier_from_raw"] == 80
    asphalt = selector.resolve(scope_id="cn_100ppi_asphalt")[0]
    assert asphalt.metadata["source_available_from"] == "2013-10-09"
    lpg = selector.resolve(scope_id="cn_100ppi_lpg")[0]
    assert lpg.metadata["source_available_from"] == "2020-03-30"
    natural_rubber = selector.resolve(scope_id="cn_100ppi_natural_rubber")[0]
    assert natural_rubber.metadata["source_available_from"] == "2013-01-04"
    softwood_pulp = selector.resolve(scope_id="cn_100ppi_softwood_pulp")[0]
    assert softwood_pulp.metadata["source_available_from"] == "2018-11-27"


def test_catalog_retires_candidate_once_source_symbol_is_formal_series(tmp_path):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    _configure_eb_candidate_fixture(module_cfg)
    storage = SpecialCommodityStorageManager(cfg)
    service = SpecialCommoditySeriesCatalogService(storage, module_cfg)
    service.sync(dry_run=False)
    assert storage.resolve_series_candidate("EB") is not None

    module_cfg["series"].append(
        {
            "series_id": "CMD.CN.CHEMICAL.STYRENE.SPOT.100PPI.DAILY",
            "commodity_id": "CN.CHEMICAL.STYRENE.SPOT",
            "venue": "100PPI",
            "source_profile": "100ppi_public_web",
            "source_symbol": "EB",
            "frequency": "daily",
            "quote_type": "spot_reference",
            "currency": "CNY",
            "unit": "CNY/ton",
            "active": True,
        }
    )
    module_cfg["series_catalog"]["candidates"] = []
    result = service.sync(dry_run=False)

    assert result["retired_production_candidates"] == 1
    assert storage.resolve_series_candidate("EB") is None


def test_special_commodity_series_catalog_reports_duplicate_source_identity(tmp_path):
    cfg = _research_config(tmp_path)
    module_cfg = cfg.modules["commodity_market_data"]["special_commodity_market_data"]
    _configure_eb_candidate_fixture(module_cfg)
    duplicate = dict(module_cfg["series_catalog"]["candidates"][0])
    duplicate["candidate_id"] = "100PPI.CHEMICAL.BENZENE.DUPLICATE"
    module_cfg["series_catalog"]["candidates"].append(duplicate)
    result = SpecialCommoditySeriesCatalogService(
        SpecialCommodityStorageManager(cfg), module_cfg
    ).sync(dry_run=True)
    assert result["status"] == "warning"
    assert result["blockers"][0]["reason"] == "commodity_candidate_source_identity_conflict"
