from __future__ import annotations

import json

import pandas as pd

from utils.config_manager import ResearchConfig, ResearchStorageConfig

from research.fx_market_data import (
    AggregatedPublicFxProvider,
    CfetsRmbFixingProvider,
    FredFxProvider,
    FxCalendarGovernanceService,
    FxDerivationService,
    FxInstrument,
    FxMasterDataService,
    FxObservation,
    FxQualityService,
    FxReadService,
    FxSeries,
    FxStorageManager,
    FxUniverseSelector,
    ManualFxProvider,
    FxRateSyncService,
    build_dcf_fx_context_from_local_service,
    build_configured_fx_provider,
)


class _FakeResponse:
    def __init__(self, *, json_payload=None, text="", status_code=200):
        self._json_payload = json_payload
        self.text = text
        self.status_code = status_code

    def json(self):
        return self._json_payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _module_cfg() -> dict:
    return {
        "enabled": True,
        "storage": {"database": "unused.db"},
        "universe": {"default_scope_id": "rmb_core"},
        "download_scopes": [
            {
                "scope_id": "rmb_core",
                "series_ids": [
                    "FX.USD_CNY.CFETS.MID.DAILY",
                    "FX.EUR_CNY.CFETS.MID.DAILY",
                    "FX.JPY_CNY.CFETS.MID.DAILY",
                    "FX.USD_CNH.MARKET.SPOT.DAILY",
                    "FX.EUR_CNH.MARKET.SPOT.DAILY",
                    "FX.JPY_CNH.MARKET.SPOT.DAILY",
                    "FX.EUR_CNH.DERIVED.DAILY",
                    "FX.JPY_CNH.DERIVED.DAILY",
                    "FXI.USD_TRADE_WEIGHTED.FRED.DAILY",
                ],
            }
        ],
        "sources": {
            "preferred_order": ["cfets_rmb_fixing", "cnh_market_aggregated_public", "fred_trade_weighted_dollar"],
            "cfets_rmb_fixing": {
                "enabled": True,
                "source": "cfets",
                "source_type": "official",
                "source_mode": "direct",
                "source_interface": "fixture",
                "role": "rmb_fixing_primary",
                "quality_flag": "official",
                "parser_version": "test.v1",
            },
            "cnh_market_aggregated_public": {
                "enabled": True,
                "source": "fixture",
                "source_type": "aggregated_public",
                "source_mode": "direct",
                "source_interface": "akshare.forex_hist_em",
                "disable_live_fetch": True,
                "role": "cnh_spot_fallback",
                "quality_flag": "aggregated_public",
                "parser_version": "test.v1",
            },
            "fred_trade_weighted_dollar": {
                "enabled": True,
                "source": "fred",
                "source_type": "official_public_dataset",
                "source_mode": "direct",
                "source_interface": "fixture",
                "role": "dollar_index_alternative",
                "quality_flag": "official",
                "parser_version": "test.v1",
            },
        },
        "derivations": [
            {
                "derived_series_id": "FX.EUR_CNH.DERIVED.DAILY",
                "source_series_ids": [
                    "FX.EUR_CNY.CFETS.MID.DAILY",
                    "FX.USD_CNY.CFETS.MID.DAILY",
                    "FX.USD_CNH.MARKET.SPOT.DAILY",
                ],
                "formula": "EUR_CNH = EUR_CNY / USD_CNY * USD_CNH",
                "date_policy": "same_date",
                "max_source_lag_days": 0,
                "quality_policy": "derived_from_governed_sources",
                "enabled": True,
            },
            {
                "derived_series_id": "FX.JPY_CNH.DERIVED.DAILY",
                "source_series_ids": [
                    "FX.JPY_CNY.CFETS.MID.DAILY",
                    "FX.USD_CNY.CFETS.MID.DAILY",
                    "FX.USD_CNH.MARKET.SPOT.DAILY",
                ],
                "formula": "JPY_CNH = JPY_CNY / USD_CNY * USD_CNH",
                "date_policy": "same_date",
                "max_source_lag_days": 0,
                "quality_policy": "derived_from_governed_sources",
                "enabled": True,
            },
        ],
        "quality": {
            "max_stale_observation_days": 5,
            "abnormal_jump_pct": 0.05,
            "source_conflict_tolerance_pct": 0.01,
            "required_first_phase_series": [
                "FX.USD_CNY.CFETS.MID.DAILY",
                "FX.EUR_CNY.CFETS.MID.DAILY",
                "FX.JPY_CNY.CFETS.MID.DAILY",
                "FX.USD_CNH.MARKET.SPOT.DAILY",
                "FX.EUR_CNH.DERIVED.DAILY",
                "FX.JPY_CNH.DERIVED.DAILY",
            ],
        },
    }


def _research_config(tmp_path):
    module_cfg = _module_cfg()
    module_cfg["storage"] = {"database": str(tmp_path / "fx.db")}
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(db_path=str(tmp_path / "research.db")),
        modules={"fx_market_data": module_cfg},
    )


def _seed_storage(tmp_path):
    config = _research_config(tmp_path)
    storage = FxStorageManager(config)
    storage.initialize()
    FxMasterDataService(storage, config.modules["fx_market_data"]).sync()
    return config, storage


def test_fx_storage_initializes_and_seeds_master_data(tmp_path):
    config, storage = _seed_storage(tmp_path)

    assert storage.db_path == str(tmp_path / "fx.db")
    dictionary = FxReadService(storage, config.modules["fx_market_data"]).dictionary()

    assert dictionary["source_policy"] == "local_fx_db_only"
    assert {item["currency_code"] for item in dictionary["currencies"]} >= {"CNY", "CNH", "USD", "EUR", "JPY"}
    jpy = next(item for item in dictionary["instruments"] if item["instrument_id"] == "FX.JPY_CNY")
    assert jpy["quote_multiplier"] == 100
    assert any(item["source_profile"] == "cfets_rmb_fixing" for item in dictionary["source_profiles"])


def test_fx_scope_resolution_defaults_and_fails_closed(tmp_path):
    config, storage = _seed_storage(tmp_path)
    selector = FxUniverseSelector(config.modules["fx_market_data"], storage)

    default = selector.resolve()
    explicit = selector.resolve(series_ids=["FX.USD_CNY.CFETS.MID.DAILY"])
    missing = selector.resolve(scope_id="missing_scope")
    empty = selector.resolve(instrument_ids=["FX.DOES_NOT_EXIST"])

    assert default.status == "success"
    assert "FX.USD_CNY.CFETS.MID.DAILY" in default.series_ids
    assert explicit.series_ids == ["FX.USD_CNY.CFETS.MID.DAILY"]
    assert missing.status == "blocked"
    assert "unknown_fx_scope:missing_scope" in missing.blockers
    assert empty.status == "blocked"
    assert empty.blockers == ["empty_fx_download_scope"]


def test_fx_observations_are_idempotent_and_jpy_multiplier_affects_conversion(tmp_path):
    config, storage = _seed_storage(tmp_path)

    obs = FxObservation(
        series_id="FX.JPY_CNY.CFETS.MID.DAILY",
        observation_date="2026-06-26",
        value=4.65,
        base_currency="JPY",
        quote_currency="CNY",
        quote_multiplier=100,
        source_profile="cfets_rmb_fixing",
        quality_flag="official",
    )
    assert storage.upsert_observation(obs) == "inserted"
    assert storage.upsert_observation(obs) == "unchanged"

    converted = FxReadService(storage, config.modules["fx_market_data"]).convert(
        from_currency="JPY",
        to_currency="CNY",
        amount=100,
        observation_date="2026-06-26",
    )

    assert converted["status"] == "success"
    assert converted["converted_amount"] == 4.65
    assert converted["conversion_policy"] == "direct"


def test_fx_manual_provider_sync_and_derivation_lineage(tmp_path):
    config, storage = _seed_storage(tmp_path)
    observations = [
        {
            "series_id": "FX.USD_CNY.CFETS.MID.DAILY",
            "observation_date": "2026-06-26",
            "value": 7.2,
            "base_currency": "USD",
            "quote_currency": "CNY",
            "quote_multiplier": 1,
            "source_profile": "cfets_rmb_fixing",
            "quality_flag": "official",
        },
        {
            "series_id": "FX.EUR_CNY.CFETS.MID.DAILY",
            "observation_date": "2026-06-26",
            "value": 7.92,
            "base_currency": "EUR",
            "quote_currency": "CNY",
            "quote_multiplier": 1,
            "source_profile": "cfets_rmb_fixing",
            "quality_flag": "official",
        },
        {
            "series_id": "FX.JPY_CNY.CFETS.MID.DAILY",
            "observation_date": "2026-06-26",
            "value": 4.65,
            "base_currency": "JPY",
            "quote_currency": "CNY",
            "quote_multiplier": 100,
            "source_profile": "cfets_rmb_fixing",
            "quality_flag": "official",
        },
        {
            "series_id": "FX.USD_CNH.MARKET.SPOT.DAILY",
            "observation_date": "2026-06-26",
            "value": 7.25,
            "base_currency": "USD",
            "quote_currency": "CNH",
            "quote_multiplier": 1,
            "source_profile": "cnh_market_aggregated_public",
            "quality_flag": "aggregated_public",
        },
    ]

    result = FxRateSyncService(
        storage,
        config,
        providers={
            "cfets_rmb_fixing": ManualFxProvider(observations),
            "cnh_market_aggregated_public": ManualFxProvider(observations),
        },
    ).sync(series_ids=[item["series_id"] for item in observations], start_date="2026-06-26", end_date="2026-06-26")
    derivation = FxDerivationService(storage, config.modules["fx_market_data"]).run(
        start_date="2026-06-26",
        end_date="2026-06-26",
    )
    eur_cnh = storage.get_observations(series_id="FX.EUR_CNH.DERIVED.DAILY")[0]
    jpy_cnh = storage.get_observations(series_id="FX.JPY_CNH.DERIVED.DAILY")[0]

    assert result["status"] == "success"
    assert result["totals"]["inserted"] == 4
    assert derivation["status"] == "success"
    assert round(eur_cnh["value"], 6) == round(7.92 / 7.2 * 7.25, 6)
    assert round(jpy_cnh["value"], 6) == round(4.65 / 7.2 * 7.25, 6)
    assert eur_cnh["quality_flag"] == "derived"
    assert "source_observations" in eur_cnh["metadata"]


def test_fx_readiness_and_quality_are_local_only(tmp_path):
    config, storage = _seed_storage(tmp_path)

    readiness = FxReadService(storage, config.modules["fx_market_data"]).readiness(as_of_date="2026-06-26")
    quality = FxQualityService(storage, config.modules["fx_market_data"]).run(as_of_date="2026-06-26")
    missing = FxReadService(storage, config.modules["fx_market_data"]).convert(
        from_currency="USD",
        to_currency="CNY",
        observation_date="2026-06-26",
    )

    assert readiness["status"] == "blocked"
    assert any(item.startswith("missing_or_stale_fx_series:") for item in readiness["blockers"])
    assert quality["status"] == "blocked"
    assert missing["status"] == "missing"
    assert missing["source_policy"] == "local_fx_db_only"


def test_fx_configured_provider_contract_writes_reviewed_payloads(tmp_path):
    config, storage = _seed_storage(tmp_path)
    module_cfg = config.modules["fx_market_data"]
    module_cfg["sources"]["cfets_rmb_fixing"]["fixture_observations"] = [
        {
            "series_id": "FX.USD_CNY.CFETS.MID.DAILY",
            "observation_date": "2026-06-26",
            "value": 7.2,
            "source_url": "fixture://cfets/usd_cny",
        }
    ]

    provider = build_configured_fx_provider("cfets_rmb_fixing", module_cfg["sources"]["cfets_rmb_fixing"])
    result = FxRateSyncService(storage, config).sync(
        series_ids=["FX.USD_CNY.CFETS.MID.DAILY"],
        start_date="2026-06-26",
        end_date="2026-06-26",
    )
    rows = storage.get_observations(series_id="FX.USD_CNY.CFETS.MID.DAILY")

    assert isinstance(provider, CfetsRmbFixingProvider)
    assert result["status"] == "success"
    assert result["totals"]["inserted"] == 1
    assert rows[0]["base_currency"] == "USD"
    assert rows[0]["quote_currency"] == "CNY"
    assert rows[0]["quality_flag"] == "official"
    assert rows[0]["metadata"]["adapter_type"] == "cfets_rmb_fixing"


def test_fx_provider_gates_disabled_missing_dependency_and_unverified_live(tmp_path):
    config, storage = _seed_storage(tmp_path)
    module_cfg = config.modules["fx_market_data"]

    dry_run = FxRateSyncService(storage, config).sync(
        series_ids=["FX.USD_CNH.MARKET.SPOT.DAILY"],
        start_date="2026-06-26",
        end_date="2026-06-26",
        dry_run=True,
    )
    blocked = FxRateSyncService(storage, config).sync(
        series_ids=["FX.USD_CNH.MARKET.SPOT.DAILY"],
        start_date="2026-06-26",
        end_date="2026-06-26",
        dry_run=False,
    )
    module_cfg["sources"]["cnh_market_aggregated_public"]["missing_dependency"] = "akshare"
    dependency = FxRateSyncService(storage, config).sync(
        series_ids=["FX.USD_CNH.MARKET.SPOT.DAILY"],
        start_date="2026-06-26",
        end_date="2026-06-26",
    )
    del module_cfg["sources"]["cnh_market_aggregated_public"]["missing_dependency"]
    module_cfg["sources"]["cnh_market_aggregated_public"]["enabled"] = False
    disabled = FxRateSyncService(storage, config).sync(
        series_ids=["FX.USD_CNH.MARKET.SPOT.DAILY"],
        start_date="2026-06-26",
        end_date="2026-06-26",
    )
    provider = build_configured_fx_provider(
        "cnh_market_aggregated_public",
        module_cfg["sources"]["cnh_market_aggregated_public"],
    )

    assert dry_run["status"] == "success"
    assert dry_run["source_results"][0]["status"] == "dry_run"
    assert "live_fx_source_adapter_not_verified" in dry_run["warnings"]
    assert blocked["status"] == "blocked"
    assert blocked["blockers"] == ["live_fx_source_adapter_not_verified"]
    assert dependency["status"] == "blocked"
    assert dependency["blockers"] == ["fx_provider_missing_dependency:akshare"]
    assert disabled["status"] == "success"
    assert disabled["source_results"][0]["status"] == "skipped"
    assert isinstance(provider, AggregatedPublicFxProvider)


def test_cfets_live_provider_parses_current_rmb_fixings(tmp_path, monkeypatch):
    config, storage = _seed_storage(tmp_path)
    provider = CfetsRmbFixingProvider(
        "cfets_rmb_fixing",
        config.modules["fx_market_data"]["sources"]["cfets_rmb_fixing"],
    )
    payload = {
        "data": {"lastDate": "2026-06-26 9:15"},
        "records": [
            {"vrtEName": "USD/CNY", "price": "6.8166"},
            {"vrtEName": "EUR/CNY", "price": "7.7405"},
            {"vrtEName": "100JPY/CNY", "price": "4.2107"},
        ],
    }
    monkeypatch.setattr(provider, "_http_get", lambda *args, **kwargs: _FakeResponse(json_payload=payload))

    result = provider.fetch(
        series=[FxSeries.from_dict(row) for row in storage.list_series(active_only=True)],
        start_date="2026-06-26",
        end_date="2026-06-26",
        dry_run=False,
    )

    assert result.status == "success"
    assert len(result.observations) == 3
    jpy = next(item for item in result.observations if item.series_id == "FX.JPY_CNY.CFETS.MID.DAILY")
    assert jpy.value == 4.2107
    assert jpy.quote_multiplier == 100
    assert jpy.quality_flag == "official"
    assert result.metadata["parser_diagnostics"]["live_requests_performed"] == 1


def test_cfets_provider_uses_safe_history_for_prior_rmb_fixings(tmp_path, monkeypatch):
    import akshare as ak
    import pandas as pd

    config, storage = _seed_storage(tmp_path)
    source_cfg = dict(config.modules["fx_market_data"]["sources"]["cfets_rmb_fixing"])
    source_cfg["safe_historical_enabled"] = True
    provider = CfetsRmbFixingProvider("cfets_rmb_fixing", source_cfg)
    payload = {
        "data": {"lastDate": "2026-06-26 9:15"},
        "records": [
            {"vrtEName": "USD/CNY", "price": "6.8166"},
            {"vrtEName": "EUR/CNY", "price": "7.7405"},
            {"vrtEName": "100JPY/CNY", "price": "4.2107"},
        ],
    }
    safe_frame = pd.DataFrame(
        [
            {"日期": "2026-06-25", "美元": 682.09, "欧元": 773.49, "日元": 4.2124},
            {"日期": "2026-06-26", "美元": 681.66, "欧元": 774.05, "日元": 4.2107},
        ]
    )
    monkeypatch.setattr(provider, "_http_get", lambda *args, **kwargs: _FakeResponse(json_payload=payload))
    monkeypatch.setattr(ak, "currency_boc_safe", lambda: safe_frame)

    result = provider.fetch(
        series=[FxSeries.from_dict(row) for row in storage.list_series(active_only=True)],
        start_date="2026-06-25",
        end_date="2026-06-26",
        dry_run=False,
    )
    by_key = {(item.series_id, item.observation_date): item for item in result.observations}

    assert result.status == "success"
    assert len(result.observations) == 6
    assert by_key[("FX.USD_CNY.CFETS.MID.DAILY", "2026-06-25")].value == 6.8209
    assert by_key[("FX.EUR_CNY.CFETS.MID.DAILY", "2026-06-25")].value == 7.7349
    assert by_key[("FX.JPY_CNY.CFETS.MID.DAILY", "2026-06-25")].value == 4.2124
    assert by_key[("FX.USD_CNY.CFETS.MID.DAILY", "2026-06-26")].value == 6.8166
    assert by_key[("FX.JPY_CNY.CFETS.MID.DAILY", "2026-06-25")].quote_multiplier == 100
    assert result.metadata["parser_diagnostics"]["safe_historical"]["observations_parsed"] == 6


def test_cfets_provider_falls_back_to_safe_history_when_current_endpoint_fails(tmp_path, monkeypatch):
    import akshare as ak
    import pandas as pd

    config, storage = _seed_storage(tmp_path)
    source_cfg = dict(config.modules["fx_market_data"]["sources"]["cfets_rmb_fixing"])
    source_cfg["safe_historical_enabled"] = True
    provider = CfetsRmbFixingProvider("cfets_rmb_fixing", source_cfg)
    safe_frame = pd.DataFrame([{"日期": "2026-06-25", "美元": 682.09, "欧元": 773.49, "日元": 4.2124}])
    monkeypatch.setattr(provider, "_http_get", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("timeout")))
    monkeypatch.setattr(ak, "currency_boc_safe", lambda: safe_frame)

    result = provider.fetch(
        series=[FxSeries.from_dict(row) for row in storage.list_series(active_only=True)],
        start_date="2026-06-25",
        end_date="2026-06-25",
        dry_run=False,
    )

    assert result.status == "success"
    assert len(result.observations) == 3
    assert result.blockers == []
    assert any(item.startswith("cfets_current_request_failed:") for item in result.warnings)
    assert result.metadata["parser_diagnostics"]["current_endpoint_failed"] is True


def test_fred_live_provider_parses_trade_weighted_dollar(tmp_path, monkeypatch):
    config, storage = _seed_storage(tmp_path)
    provider = FredFxProvider(
        "fred_trade_weighted_dollar",
        config.modules["fx_market_data"]["sources"]["fred_trade_weighted_dollar"],
    )
    csv_text = "observation_date,DTWEXBGS\n2026-06-25,120.3336\n2026-06-26,119.6451\n"
    monkeypatch.setattr(provider, "_http_get", lambda *args, **kwargs: _FakeResponse(text=csv_text))

    result = provider.fetch(
        series=[FxSeries.from_dict(row) for row in storage.list_series(active_only=True)],
        start_date="2026-06-26",
        end_date="2026-06-26",
        dry_run=False,
    )

    assert result.status == "success"
    assert len(result.observations) == 1
    assert result.observations[0].series_id == "FXI.USD_TRADE_WEIGHTED.FRED.DAILY"
    assert result.observations[0].value == 119.6451
    assert result.observations[0].quality_flag == "official"


def test_akshare_aggregated_provider_parses_offshore_cnh_history(tmp_path, monkeypatch):
    config, storage = _seed_storage(tmp_path)
    source_cfg = dict(config.modules["fx_market_data"]["sources"]["cnh_market_aggregated_public"])
    source_cfg.pop("disable_live_fetch", None)
    source_cfg["symbols"] = {
        "FX.USD_CNH.MARKET.SPOT.DAILY": "USDCNH",
        "FX.EUR_CNH.MARKET.SPOT.DAILY": "EURCNH",
        "FX.JPY_CNH.MARKET.SPOT.DAILY": "JPYCNH",
    }
    provider = AggregatedPublicFxProvider("cnh_market_aggregated_public", source_cfg)
    frames = {
        "USDCNH": pd.DataFrame([
            {"日期": "2026-06-25", "代码": "USDCNH", "最新价": 7.25},
            {"日期": "2026-06-26", "代码": "USDCNH", "最新价": 7.26},
        ]),
        "EURCNH": pd.DataFrame([
            {"日期": "2026-06-26", "代码": "EURCNH", "最新价": 8.01},
        ]),
        "JPYCNH": pd.DataFrame([
            {"日期": "2026-06-26", "代码": "JPYCNH", "最新价": 4.62},
        ]),
    }

    monkeypatch.setattr(provider, "_akshare_forex_hist_em", lambda _ak, symbol: frames[symbol])

    result = provider.fetch(
        series=[
            FxSeries.from_dict(row)
            for row in storage.list_series(active_only=True)
            if row["series_id"] in source_cfg["symbols"]
        ],
        start_date="2026-06-26",
        end_date="2026-06-26",
        dry_run=False,
    )

    assert result.status == "success"
    assert {item.series_id for item in result.observations} == set(source_cfg["symbols"])
    assert len(result.observations) == 3
    by_series = {item.series_id: item for item in result.observations}
    assert by_series["FX.USD_CNH.MARKET.SPOT.DAILY"].value == 7.26
    assert by_series["FX.EUR_CNH.MARKET.SPOT.DAILY"].value == 8.01
    assert by_series["FX.JPY_CNH.MARKET.SPOT.DAILY"].quote_multiplier == 100
    assert by_series["FX.JPY_CNH.MARKET.SPOT.DAILY"].value == 4.62
    assert by_series["FX.JPY_CNH.MARKET.SPOT.DAILY"].metadata["provider"] == "akshare_forex_hist_em"
    assert result.metadata["parser_diagnostics"]["akshare_requests_performed"] == 3
    assert result.metadata["parser_diagnostics"]["akshare_rows_seen"] == 4


def test_yahoo_aggregated_provider_parses_offshore_cnh_spot(tmp_path, monkeypatch):
    config, storage = _seed_storage(tmp_path)
    source_cfg = dict(config.modules["fx_market_data"]["sources"]["cnh_market_aggregated_public"])
    source_cfg.pop("disable_live_fetch", None)
    source_cfg["source_interface"] = "yahoo_chart_fx"
    source_cfg["symbols"] = {
        "FX.USD_CNH.MARKET.SPOT.DAILY": "USDCNH=X",
        "FX.EUR_CNH.MARKET.SPOT.DAILY": "EURCNH=X",
        "FX.JPY_CNH.MARKET.SPOT.DAILY": "JPYCNH=X",
    }
    provider = AggregatedPublicFxProvider("cnh_market_aggregated_public", source_cfg)
    close_by_symbol = {"USDCNH=X": 7.25, "EURCNH=X": 8.01, "JPYCNH=X": 4.62}

    def _fake_get(url, **kwargs):
        symbol = url.rsplit("/", 1)[-1]
        return _FakeResponse(
            json_payload={
                "chart": {
                    "result": [
                        {
                            "timestamp": [1782432000],
                            "indicators": {"quote": [{"close": [close_by_symbol[symbol]]}]},
                        }
                    ],
                    "error": None,
                }
            }
        )

    monkeypatch.setattr(provider, "_http_get", _fake_get)

    result = provider.fetch(
        series=[
            FxSeries.from_dict(row)
            for row in storage.list_series(active_only=True)
            if row["series_id"] in source_cfg["symbols"]
        ],
        start_date="2026-06-26",
        end_date="2026-06-26",
        dry_run=False,
    )

    assert result.status == "success"
    assert {item.series_id for item in result.observations} == set(source_cfg["symbols"])
    assert {item.quality_flag for item in result.observations} == {"aggregated_public"}
    by_series = {item.series_id: item for item in result.observations}
    assert by_series["FX.USD_CNH.MARKET.SPOT.DAILY"].value == 7.25
    assert by_series["FX.EUR_CNH.MARKET.SPOT.DAILY"].value == 8.01
    assert by_series["FX.JPY_CNH.MARKET.SPOT.DAILY"].quote_multiplier == 100
    assert by_series["FX.JPY_CNH.MARKET.SPOT.DAILY"].value == 462.0
    assert by_series["FX.JPY_CNH.MARKET.SPOT.DAILY"].metadata["source_close_per_1_base"] == 4.62
    assert result.metadata["parser_diagnostics"]["live_requests_performed"] == 3
    assert result.metadata["parser_diagnostics"]["rows_seen"] == 3


def test_fx_calendar_governance_uses_source_policy_and_observations(tmp_path):
    config, storage = _seed_storage(tmp_path)
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.CFETS.MID.DAILY",
            observation_date="2026-06-26",
            value=7.2,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cfets_rmb_fixing",
            quality_flag="official",
        )
    )

    result = FxCalendarGovernanceService(storage, config.modules["fx_market_data"]).run(
        source_profiles=["cfets_rmb_fixing", "cnh_market_aggregated_public"],
        start_date="2026-06-26",
        end_date="2026-06-27",
    )

    with storage.get_connection() as conn:
        rows = {
            (row["source_profile"], row["calendar_date"]): dict(row)
            for row in conn.execute(
                """
                SELECT source_profile, calendar_date, is_publication_day,
                       publication_status, quality_flag, metadata_json
                FROM fx_calendars
                """
            ).fetchall()
        }
    cfets_publish = rows[("cfets_rmb_fixing", "2026-06-26")]
    cnh_missing = rows[("cnh_market_aggregated_public", "2026-06-26")]
    cnh_weekend = rows[("cnh_market_aggregated_public", "2026-06-27")]
    cfets_metadata = json.loads(cfets_publish["metadata_json"])

    assert result["status"] == "success"
    assert result["status_counts"] == {
        "observed": 1,
        "missing_expected_observation": 1,
        "expected_non_publication": 2,
    }
    assert cfets_publish["publication_status"] == "observed"
    assert cfets_metadata["source_observation_count"] == 1
    assert cfets_metadata["source_observation_series_ids"] == ["FX.USD_CNY.CFETS.MID.DAILY"]
    assert cnh_missing["publication_status"] == "missing_expected_observation"
    assert cnh_missing["quality_flag"] == "missing_expected_observation"
    assert cnh_weekend["publication_status"] == "expected_non_publication"
    assert cnh_weekend["is_publication_day"] == 0


def test_fx_calendar_governance_respects_configured_holidays(tmp_path):
    config, storage = _seed_storage(tmp_path)
    config.modules["fx_market_data"]["sources"]["cfets_rmb_fixing"]["calendar"] = {
        "policy": "china_business_day_configured_holidays",
        "holiday_dates": ["2026-06-26"],
    }

    result = FxCalendarGovernanceService(storage, config.modules["fx_market_data"]).run(
        source_profiles=["cfets_rmb_fixing"],
        start_date="2026-06-26",
        end_date="2026-06-26",
    )

    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT is_publication_day, publication_status, metadata_json
            FROM fx_calendars
            WHERE source_profile = ? AND calendar_date = ?
            """,
            ("cfets_rmb_fixing", "2026-06-26"),
        ).fetchone()
    metadata = json.loads(row["metadata_json"])

    assert result["status_counts"] == {"expected_non_publication": 1}
    assert row["is_publication_day"] == 0
    assert row["publication_status"] == "expected_non_publication"
    assert metadata["calendar_reason"] == "configured_holiday"


def test_fx_derivation_inverse_missing_source_gap_and_lag_policy(tmp_path):
    config, storage = _seed_storage(tmp_path)
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.CFETS.MID.DAILY",
            observation_date="2026-06-26",
            value=7.2,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cfets_rmb_fixing",
            quality_flag="official",
        )
    )
    inverse = FxReadService(storage, config.modules["fx_market_data"]).convert(
        from_currency="CNY",
        to_currency="USD",
        amount=7.2,
        observation_date="2026-06-26",
    )
    derivation = FxDerivationService(storage, config.modules["fx_market_data"]).run(
        start_date="2026-06-26",
        end_date="2026-06-26",
    )
    quality = FxQualityService(storage, config.modules["fx_market_data"]).run(as_of_date="2026-06-26")

    assert inverse["status"] == "success"
    assert inverse["conversion_policy"] == "inverse"
    assert round(inverse["converted_amount"], 6) == 1.0
    assert derivation["status"] == "partial"
    assert derivation["totals"]["gaps"] == 2
    assert quality["issue_counts"]["derivation_gap"] == 2


def test_fx_quality_checks_abnormal_jumps_source_conflicts_and_multiplier(tmp_path):
    config, storage = _seed_storage(tmp_path)
    storage.upsert_instrument(
        FxInstrument(
            instrument_id="FX.BAD_MULTIPLIER",
            instrument_type="currency_pair",
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=0,
            market_scope="test",
            category="test",
        )
    )
    storage.upsert_series(
        FxSeries(
            series_id="FX.USD_CNY.FALLBACK.SPOT.DAILY",
            instrument_id="FX.USD_CNY",
            source_profile="cnh_market_aggregated_public",
            rate_type="spot",
            frequency="daily",
            timezone="Asia/Shanghai",
            publication_lag="same_day",
            quality_policy="test",
        )
    )
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.CFETS.MID.DAILY",
            observation_date="2026-06-25",
            value=7.2,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cfets_rmb_fixing",
            quality_flag="official",
        )
    )
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.CFETS.MID.DAILY",
            observation_date="2026-06-26",
            value=7.8,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cfets_rmb_fixing",
            quality_flag="official",
        )
    )
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.FALLBACK.SPOT.DAILY",
            observation_date="2026-06-26",
            value=7.4,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cnh_market_aggregated_public",
            quality_flag="aggregated_public",
        )
    )

    quality = FxQualityService(storage, config.modules["fx_market_data"]).run(as_of_date="2026-06-26")

    assert quality["status"] == "blocked"
    assert quality["issue_counts"]["invalid_quote_multiplier"] == 1
    assert quality["issue_counts"]["abnormal_jump"] >= 1
    assert quality["issue_counts"]["source_conflict"] == 1


def test_dcf_fx_context_helper_uses_valuation_date_cutoff(tmp_path):
    config, storage = _seed_storage(tmp_path)
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.CFETS.MID.DAILY",
            observation_date="2026-06-20",
            value=7.1,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cfets_rmb_fixing",
            quality_flag="official",
        )
    )
    storage.upsert_observation(
        FxObservation(
            series_id="FX.USD_CNY.CFETS.MID.DAILY",
            observation_date="2026-06-26",
            value=7.2,
            base_currency="USD",
            quote_currency="CNY",
            quote_multiplier=1,
            source_profile="cfets_rmb_fixing",
            quality_flag="official",
        )
    )

    context = build_dcf_fx_context_from_local_service(
        FxReadService(storage, config.modules["fx_market_data"]),
        config.modules["fx_market_data"],
        valuation_date="2026-06-25",
        research_mode=False,
    )

    usd_cny = context["assumptions"]["fx_usd_cny"]
    assert usd_cny["value"] == 7.1
    assert usd_cny["as_of_date"] == "2026-06-20"
    assert usd_cny["metadata"]["fx_series_id"] == "FX.USD_CNY.CFETS.MID.DAILY"
    assert usd_cny["metadata"]["conversion_policy"] == "direct"
    assert context["source_policy"] == "local_fx_db_only"
    assert "fx_hkd_cny_local_rate_missing" in context["blockers"]


def test_dcf_fx_context_helper_marks_research_fallback_when_missing(tmp_path):
    config, storage = _seed_storage(tmp_path)

    context = build_dcf_fx_context_from_local_service(
        FxReadService(storage, config.modules["fx_market_data"]),
        config.modules["fx_market_data"],
        valuation_date="2026-06-26",
        research_mode=True,
    )

    assert context["status"] == "research_fallback_required"
    assert context["source_policy"] == "local_fx_db_only"
    assert "fx_usd_cny_local_rate_missing" in context["blockers"]
    assert context["assumptions"]["fx_usd_cny"]["value"] is None
    assert context["assumptions"]["fx_usd_cny"]["fallback_used"] is False
