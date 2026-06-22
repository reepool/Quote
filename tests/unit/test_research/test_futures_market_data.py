from __future__ import annotations

import asyncio
import json
import sys
import types
from datetime import date, timedelta
import pandas as pd
import pytest
import requests

from research.futures_market_data import (
    FuturesBar,
    FuturesCalendarService,
    FuturesCalendarNotice,
    FuturesContinuousMapping,
    FuturesContract,
    FuturesContractBar,
    FuturesDiagnosticsService,
    FuturesExposureMapping,
    FuturesInstrument,
    FuturesInstrumentCalendarOverride,
    FuturesManualCalendarReview,
    FuturesMasterDiscoveryCandidate,
    FuturesMasterDiscoveryGovernanceService,
    FuturesMasterGovernanceService,
    FuturesMarketDataSyncService,
    FuturesOfficialCalendarBackfillService,
    FuturesProductSpec,
    FuturesReadinessService,
    FuturesSeries,
    FuturesStorageManager,
    FuturesTradingCalendarDay,
    FuturesTradingDayGovernanceService,
    FuturesUniverseSelector,
    default_futures_registry,
    infer_contract_month,
    make_futures_contract_id,
    make_futures_instrument_id,
    make_futures_series_id,
)
from research.providers.akshare_futures import AkshareFuturesMarketDataProvider
from research.providers.official_futures_calendar import OfficialFuturesCalendarProvider
from research.providers.official_futures import (
    DceOfficialBrowserClient,
    OfficialFuturesContractBar,
    OfficialFuturesDailyProbeResult,
    OfficialFuturesMarketDataProvider,
    OfficialFuturesSourceUnavailable,
    classify_official_futures_failure,
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


def _scope_module_cfg():
    return {
        "trading_day_governance": {
            "enabled_exchanges": ["SHFE", "INE", "DCE", "CZCE", "GFEX"],
        },
        "universe": {
            "default_series_types": ["main_continuous"],
        },
        "registry": {
            "include_default_p0_universe": True,
            "instruments": [],
        },
        "download_scopes": [
            {
                "scope_id": "domestic_all",
                "exchanges": ["all"],
                "categories": ["all"],
                "series_types": ["main_continuous"],
            },
            {
                "scope_id": "gfex_all",
                "exchanges": ["GFEX"],
                "categories": ["all"],
                "series_types": ["main_continuous"],
            },
            {
                "scope_id": "shfe_nonferrous_precious",
                "exchanges": ["SHFE"],
                "categories": ["nonferrous", "precious_metal"],
                "series_types": ["main_continuous"],
            },
        ],
    }


def test_futures_universe_selector_resolves_all_and_named_scope():
    selector = FuturesUniverseSelector(_scope_module_cfg())

    domestic = selector.resolve(scope_id="domestic_all")
    gfex = selector.resolve(scope_id="gfex_all")

    assert domestic.status == "success"
    assert set(domestic.exchanges) == {"SHFE", "INE", "DCE", "CZCE", "GFEX"}
    assert "CNF.CU.SHFE.main" in domestic.series_ids
    assert gfex.exchanges == ["GFEX"]
    assert set(gfex.categories) == {"new_energy_material"}
    assert set(gfex.instrument_ids) == {
        "CNF.LC.GFEX",
        "CNF.PS.GFEX",
        "CNF.SI.GFEX",
    }


def test_futures_universe_selector_resolves_exchange_category_instrument_and_series_filters():
    selector = FuturesUniverseSelector(_scope_module_cfg())

    exchange_only = selector.resolve(exchanges=["INE"])
    category_only = selector.resolve(categories=["rubber"])
    instrument_only = selector.resolve(instrument_ids=["CNF.CU.SHFE"])
    series_only = selector.resolve(series_ids=["CNF.CU.SHFE.main"])
    filtered_scope = selector.resolve(scope_id="shfe_nonferrous_precious", categories=["precious_metal"])

    assert exchange_only.exchanges == ["INE"]
    assert set(exchange_only.instrument_ids) == {"CNF.LU.INE", "CNF.NR.INE", "CNF.SC.INE"}
    assert set(category_only.exchanges) == {"SHFE", "INE"}
    assert instrument_only.instrument_ids == ["CNF.CU.SHFE"]
    assert instrument_only.series_ids == ["CNF.CU.SHFE.main"]
    assert series_only.instrument_ids == ["CNF.CU.SHFE"]
    assert set(filtered_scope.categories) == {"precious_metal"}
    assert set(filtered_scope.instrument_ids) == {"CNF.AG.SHFE", "CNF.AU.SHFE"}


def test_futures_universe_selector_reports_invalid_and_empty_scopes():
    selector = FuturesUniverseSelector(_scope_module_cfg())

    invalid = selector.resolve(scope_id="missing_scope", exchanges=["BAD"], categories=["bad_category"])
    empty = selector.resolve(exchanges=["GFEX"], categories=["coal"])

    assert invalid.status == "blocked"
    assert "invalid_futures_scope:missing_scope" in invalid.blockers
    assert empty.status == "blocked"
    assert empty.blockers == ["empty_futures_download_scope"]


def test_futures_calendar_and_price_services_report_scope_blockers_before_requests(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()

    calendar = FuturesOfficialCalendarBackfillService(storage, config, config.modules["commodity_market_data"]).run(
        scope_id="missing_scope",
        start_date="2024-06-03",
        end_date="2024-06-03",
        dry_run=True,
    )
    readiness = FuturesReadinessService(storage, config.modules["commodity_market_data"]).build(
        scope_id="missing_scope",
    )
    diagnostics = FuturesDiagnosticsService(storage, config.modules["commodity_market_data"]).refresh_all(
        scope_id="missing_scope",
    )
    price = asyncio.run(
        FuturesMarketDataSyncService(storage, config).sync(
            scope_id="missing_scope",
            start_date="2024-06-03",
            end_date="2024-06-03",
            dry_run=True,
        )
    )

    assert calendar["status"] == "blocked"
    assert readiness["status"] == "blocked"
    assert diagnostics["status"] == "blocked"
    assert price["status"] == "blocked"
    assert calendar["scope_selection"]["blockers"] == ["invalid_futures_scope:missing_scope"]
    assert readiness["scope_selection"]["blockers"] == ["invalid_futures_scope:missing_scope"]
    assert diagnostics["scope_selection"]["blockers"] == ["invalid_futures_scope:missing_scope"]
    assert price["scope_selection"]["blockers"] == ["invalid_futures_scope:missing_scope"]


def test_futures_market_data_production_auto_backfills_estimated_calendar_before_provider(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["GFEX"]},
        "akshare_futures": {"enabled": False},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("provider should not be called when production calendar quality is estimated")

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars",
        fail_if_called,
    )

    def fake_calendar_backfill(self, **kwargs):
        assert kwargs["exchanges"] == ["GFEX"]
        assert kwargs["start_date"] == "2026-06-20"
        assert kwargs["end_date"] == "2026-06-20"
        assert kwargs["dry_run"] is False
        self.storage.upsert_trading_calendar([
            FuturesTradingCalendarDay(
                exchange="GFEX",
                trade_date="2026-06-20",
                is_trading_day=False,
                source_profile="exchange_official_daily_probe",
                quality_flag="backfilled_verified",
            )
        ])
        return {
            "status": "success",
            "totals": {"rows_written": 1, "trading_days": 0, "closed_days": 1, "unresolved_dates": 0},
            "blockers": [],
            "warnings": [],
        }

    monkeypatch.setattr(FuturesOfficialCalendarBackfillService, "run", fake_calendar_backfill)

    result = asyncio.run(
        FuturesMarketDataSyncService(storage, config).sync(
            scope_id="gfex_all",
            start_date="2026-06-20",
            end_date="2026-06-20",
            dry_run=False,
        )
    )

    assert result["status"] == "success"
    assert result["totals"]["inserted"] == 0
    assert result["trading_day_governance"]["minimum_quality"] == "backfilled_verified"
    assert result["trading_day_governance"]["auto_official_calendar_backfill"]["attempted"] is True
    assert result["trading_day_governance"]["auto_official_calendar_backfill"]["ranges"] == [
        {"exchange": "GFEX", "start_date": "2026-06-20", "end_date": "2026-06-20"}
    ]
    assert result["trading_day_governance"]["skipped_dates_by_exchange"]["GFEX"] == ["2026-06-20"]
    assert storage.get_price_bars("CNF.SI.GFEX.main") == []


def test_futures_market_data_production_blocks_when_auto_calendar_backfill_fails(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["GFEX"]},
        "akshare_futures": {"enabled": False},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("provider should not be called when official calendar repair fails")

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars",
        fail_if_called,
    )

    def failed_calendar_backfill(self, **kwargs):
        return {
            "status": "blocked",
            "totals": {"rows_written": 0, "trading_days": 0, "closed_days": 0, "unresolved_dates": 1},
            "blockers": ["unresolved_official_calendar_dates"],
            "warnings": [],
        }

    monkeypatch.setattr(FuturesOfficialCalendarBackfillService, "run", failed_calendar_backfill)

    result = asyncio.run(
        FuturesMarketDataSyncService(storage, config).sync(
            scope_id="gfex_all",
            start_date="2026-06-20",
            end_date="2026-06-20",
            dry_run=False,
        )
    )

    assert result["status"] == "blocked"
    assert result["totals"]["inserted"] == 0
    assert "calendar_quality_below_threshold:GFEX:estimated<required:backfilled_verified" in result["reason"]
    assert result["trading_day_governance"]["auto_official_calendar_backfill"]["status"] == "blocked"


def test_gfex_master_governance_blocks_without_verified_calendar(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        scope_id="gfex_all",
        start_date="2022-12-22",
        end_date="2022-12-22",
        dry_run=True,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "missing_verified_gfex_trading_calendar_coverage"


def test_gfex_master_governance_dry_run_discovers_without_writing(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2022-12-22",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        assert exchange == "GFEX"
        assert trade_date == "2022-12-22"
        self._increment_metric("GFEX", "challenge_count", 1)
        self._increment_metric("GFEX", "challenge_backoff_seconds", 10)
        self._increment_metric("GFEX", "batch_pause_count", 1)
        self._increment_metric("GFEX", "batch_pause_seconds", 10)
        return [
            _official_contract_row(
                exchange="GFEX",
                trade_date="2022-12-22",
                variety="LC",
                contract="LC2301",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        scope_id="gfex_all",
        start_date="2022-12-22",
        end_date="2022-12-22",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["counts"]["contracts_discovered"] == 1
    assert result["counts"]["would_write_contracts"] == 1
    assert result["counts"]["contracts_written"] == 0
    assert result["counts"]["challenge_count"] == 1
    assert result["counts"]["challenge_backoff_seconds"] == 10
    assert result["counts"]["batch_pause_count"] == 1
    assert result["counts"]["batch_pause_seconds"] == 10
    assert storage.list_contracts(exchange="GFEX") == []


def test_dce_master_governance_dry_run_discovers_contracts(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["DCE"]},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        assert exchange == "DCE"
        assert trade_date == "2026-01-02"
        return [
            _official_contract_row(
                exchange="DCE",
                trade_date="2026-01-02",
                variety="I",
                contract="I2601",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["exchange"] == "DCE"
    assert result["calendar"]["verified_trading_days"] == 1
    assert result["counts"]["instruments"] == 9
    assert result["counts"]["contracts_discovered"] == 1
    assert result["counts"]["would_write_contracts"] == 1
    assert result["contracts"][0]["contract_id"] == "CNF.I.DCE.I2601"
    assert storage.list_contracts(exchange="DCE") == []


def test_dce_master_governance_reprocesses_auto_promoted_unknown_varieties(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["DCE"]},
        "enabled_exchanges": ["DCE"],
        "adapters": {
            "DCE": {
                "enabled": True,
                "known_products": {
                    "BZ": {
                        "category": "chemical",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["DCE"]},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="BZ",
                contract="BZ2601",
                raw_payload={"variety": "纯苯", "contractId": "bz2601"},
            )
        ]

    def fake_fetch_exchange_product_specs_sync(self, exchange, target_symbols=None):
        return {
            "BZ": FuturesProductSpec(
                exchange="DCE",
                symbol="BZ",
                name="纯苯",
                contract_multiplier=30,
                tick_size=1,
                source_profile="exchange_official_product_spec",
                source_interface="official_dce_contract_info",
            )
        }

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_product_specs_sync",
        fake_fetch_exchange_product_specs_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["warnings"] == []
    assert result["counts"]["master_discovery_auto_promoted"] == 1
    assert result["counts"]["auto_promoted_reprocessed_varieties"] == 1
    assert result["counts"]["contracts_discovered"] == 1
    assert result["counts"]["contracts_written"] == 1
    assert storage.get_instrument("CNF.BZ.DCE")["unit"] == "CNY/ton"
    assert storage.list_contracts(exchange="DCE")[0]["contract_id"] == "CNF.BZ.DCE.BZ2601"


def test_dce_master_governance_retries_failed_trade_dates(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data"] = {
        "contract_discovery_retry": {
            "retry_passes": 1,
            "retry_pause_seconds": 0,
        }
    }
    config.modules["commodity_market_data"].update(module_cfg)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["DCE"]},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        ),
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-03",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        ),
    ])
    calls = {}

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        calls[trade_date] = calls.get(trade_date, 0) + 1
        if trade_date == "2026-01-02" and calls[trade_date] == 1:
            raise OfficialFuturesSourceUnavailable("temporary DCE browser startup failure")
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="I",
                contract="I2601",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-03",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["counts"]["official_request_count"] == 3
    assert result["counts"]["task_retry_passes"] == 1
    assert result["counts"]["task_retry_resolved"] == 1
    assert result["counts"]["failed_trade_dates"] == 0
    assert result["warnings"] == []
    assert calls == {"2026-01-02": 2, "2026-01-03": 1}


def test_dce_master_governance_reports_product_spec_enrichment_failure(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["DCE"]},
        "enabled_exchanges": ["DCE"],
        "adapters": {"DCE": {"enabled": True, "known_products": {}}},
    }
    config.modules["commodity_market_data"].update(module_cfg)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["DCE"]},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="I",
                contract="I2601",
            )
        ]

    def fake_fetch_exchange_product_specs_sync(self, exchange, target_symbols=None):
        raise OfficialFuturesSourceUnavailable("DCE browser session failed")

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_product_specs_sync",
        fake_fetch_exchange_product_specs_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=True,
    )

    assert result["status"] == "warning"
    assert result["counts"]["contracts_discovered"] == 1
    assert any(
        warning.get("reason") == "official_product_spec_enrichment_unavailable"
        and warning.get("exchange") == "DCE"
        and "DCE browser session failed" in warning.get("error", "")
        for warning in result["warnings"]
    )


def test_dce_master_governance_refreshes_existing_master_data(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["DCE"]},
        "enabled_exchanges": ["DCE"],
        "adapters": {"DCE": {"enabled": True, "known_products": {}}},
    }
    config.modules["commodity_market_data"].update(module_cfg)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["DCE"]},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    _seed_dce_iron_ore_master(storage)
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])
    before = storage.get_instrument("CNF.I.DCE")
    assert before["name"] == "DCE Iron Ore"
    assert before["metadata"] == {}

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="I",
                contract="I2601",
                raw_payload={"variety": "铁矿石", "contractId": "I2601"},
            )
        ]

    def fake_fetch_exchange_product_specs_sync(self, exchange, target_symbols=None):
        assert exchange == "DCE"
        return {
            "I": FuturesProductSpec(
                exchange="DCE",
                symbol="I",
                name="铁矿石",
                currency="CNY",
                contract_multiplier=100,
                tick_size=0.5,
                source_profile="exchange_official_product_spec",
                source_interface="official_dce_product_rule_page",
                source_url="https://example.test/dce/i",
                quality_flag="official_product_spec_verified",
                field_sources={
                    "name": {
                        "source_type": "official_product_rule_page",
                        "source_interface": "official_dce_product_rule_page",
                        "source_ref": "https://example.test/dce/i",
                        "quality_flag": "official_product_spec_verified",
                    },
                    "currency": {
                        "source_type": "governed_rule_metadata",
                        "source_interface": "config_master_data_discovery",
                        "quality_flag": "governed_rule_metadata",
                    },
                },
            )
        }

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_product_specs_sync",
        fake_fetch_exchange_product_specs_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    instrument = storage.get_instrument("CNF.I.DCE")
    series = storage.get_series("CNF.I.DCE.main")

    assert result["status"] == "success"
    assert result["counts"]["initial_instruments"] == 9
    assert result["counts"]["refreshed_instruments"] == 1
    assert result["counts"]["final_instruments"] == 9
    assert result["counts"]["contracts_written"] == 1
    assert instrument["name"] == "铁矿石"
    assert instrument["unit"] == "CNY/ton"
    assert instrument["metadata"]["master_governance_evidence"]["field_sources"]["name"]["source_type"] == (
        "official_product_rule_page"
    )
    assert series["unit"] == "CNY/ton"
    assert series["metadata"]["master_governance_refreshed_from_instrument"] == "CNF.I.DCE"


def test_dce_browser_warmup_failure_does_not_block_requested_api(monkeypatch):
    class FakePage:
        async def sleep(self, seconds):
            return None

        async def evaluate(self, script, await_promise=False, return_by_value=False):
            if "/dcereport/publicweb/maxTradeDate" in script:
                return json.dumps({
                    "status": -1,
                    "ok": False,
                    "text": "TypeError: Failed to fetch",
                })
            assert "/dcereport/publicweb/tradepara/contractInfo" in script
            return json.dumps({
                "status": 200,
                "ok": True,
                "text": json.dumps({"success": True, "data": []}),
            })

    class FakeBrowser:
        async def get(self, url):
            return FakePage()

        def stop(self):
            return None

    async def fake_start(**kwargs):
        return FakeBrowser()

    monkeypatch.setitem(sys.modules, "nodriver", types.SimpleNamespace(start=fake_start))
    monkeypatch.setattr(DceOfficialBrowserClient, "_start_virtual_display_if_needed", lambda self: None)

    client = DceOfficialBrowserClient({
        "settle_seconds": 0,
        "retry_attempts": 1,
        "browser_executable_path": "/tmp/fake-chrome",
    })
    try:
        payload = client.fetch_contract_info_payload()
    finally:
        client.close()

    assert payload == {"success": True, "data": []}


def test_dce_browser_restarts_session_after_in_page_fetch_failure(monkeypatch):
    state = {"starts": 0, "requested_api_calls": 0}

    class FakePage:
        async def sleep(self, seconds):
            return None

        async def evaluate(self, script, await_promise=False, return_by_value=False):
            if "/dcereport/publicweb/maxTradeDate" in script:
                return json.dumps({
                    "status": 200,
                    "ok": True,
                    "text": json.dumps({"success": True}),
                })
            assert "/dcereport/publicweb/tradepara/contractInfo" in script
            state["requested_api_calls"] += 1
            if state["requested_api_calls"] == 1:
                return json.dumps({
                    "status": -1,
                    "ok": False,
                    "text": "TypeError: Failed to fetch",
                })
            return json.dumps({
                "status": 200,
                "ok": True,
                "text": json.dumps({"success": True, "data": [{"varietyOrder": "I"}]}),
            })

    class FakeBrowser:
        def __init__(self):
            self.stopped = False

        async def get(self, url):
            return FakePage()

        def stop(self):
            self.stopped = True

    async def fake_start(**kwargs):
        state["starts"] += 1
        return FakeBrowser()

    monkeypatch.setitem(sys.modules, "nodriver", types.SimpleNamespace(start=fake_start))
    monkeypatch.setattr(DceOfficialBrowserClient, "_start_virtual_display_if_needed", lambda self: None)

    client = DceOfficialBrowserClient({
        "settle_seconds": 0,
        "retry_attempts": 2,
        "retry_backoff_seconds": 0,
        "browser_executable_path": "/tmp/fake-chrome",
    })
    try:
        payload = client.fetch_contract_info_payload()
    finally:
        client.close()

    assert payload == {"success": True, "data": [{"varietyOrder": "I"}]}
    assert state["starts"] == 2
    assert state["requested_api_calls"] == 2


def test_master_governance_uses_provider_supported_exchanges(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="SHFE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        assert exchange == "SHFE"
        assert trade_date == "2026-01-02"
        return [
            _official_contract_row(
                exchange="SHFE",
                trade_date="2026-01-02",
                variety="CU",
                contract="CU2601",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["SHFE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["exchange"] == "SHFE"
    assert result["counts"]["contracts_discovered"] == 1
    assert result["contracts"][0]["contract_id"] == "CNF.CU.SHFE.CU2601"


def test_gfex_master_governance_write_upserts_contracts(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2022-12-22",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="LC",
                contract="LC2301",
            ),
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="SI",
                contract="SI2301",
            ),
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        scope_id="gfex_all",
        start_date="2022-12-22",
        end_date="2022-12-22",
        dry_run=False,
    )
    contracts = storage.list_contracts(exchange="GFEX")

    assert result["status"] == "success"
    assert result["counts"]["contracts_discovered"] == 2
    assert result["counts"]["contracts_written"] == 2
    assert {item["instrument_id"] for item in contracts} == {"CNF.LC.GFEX", "CNF.SI.GFEX"}
    assert {item["contract_month"] for item in contracts} == {"2023-01"}
    assert all(item["quality_flag"] == "official_daily_discovered_partial" for item in contracts)


def test_gfex_master_governance_discovers_platinum_and_palladium_candidates(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2025-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="LC",
                contract="LC2506",
            ),
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="PT",
                contract="PT2506",
            ),
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="PD",
                contract="PD2506",
            ),
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        scope_id="gfex_all",
        start_date="2025-01-02",
        end_date="2025-01-02",
        dry_run=True,
    )

    assert result["status"] == "warning"
    assert result["counts"]["contracts_discovered"] == 1
    assert result["counts"]["master_discovery_candidates"] == 2
    assert result["counts"]["master_discovery_auto_promoted"] == 2
    assert {item["instrument_id"] for item in result["contracts"]} == {"CNF.LC.GFEX"}
    warning = result["warnings"][0]
    assert warning["reason"] == "unmapped_gfex_varieties"
    assert dict(warning["samples"]) == {"PD": 1, "PT": 1}
    assert {
        item["candidate_instrument_id"]
        for item in warning["discovery_candidates"]
    } == {"CNF.PD.GFEX", "CNF.PT.GFEX"}


def test_futures_master_discovery_storage_idempotency_and_conflict(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()

    first = FuturesMasterDiscoveryCandidate(
        discovery_id="GFEX:XY",
        exchange="GFEX",
        variety_symbol="XY",
        candidate_instrument_id="CNF.XY.GFEX",
        candidate_series_id="CNF.XY.GFEX.main",
        candidate_name="GFEX Test Product",
        candidate_category="new_energy_material",
        candidate_unit="CNY/ton",
        first_seen_trade_date="2026-01-02",
        last_seen_trade_date="2026-01-02",
        observed_contracts=["XY2601"],
        evidence={"source_profile": "fixture"},
        confidence_score=0.95,
        quality_flag="discovered_verified",
        review_status="none",
    )
    second = FuturesMasterDiscoveryCandidate(
        **{
            **first.__dict__,
            "last_seen_trade_date": "2026-01-03",
            "observed_contracts": ["XY2602"],
        }
    )
    conflicting = FuturesMasterDiscoveryCandidate(
        **{
            **first.__dict__,
            "candidate_unit": "CNY/kg",
            "last_seen_trade_date": "2026-01-04",
            "observed_contracts": ["XY2603"],
        }
    )

    assert storage.upsert_master_discoveries([first]) == 1
    assert storage.upsert_master_discoveries([second]) == 1
    row = storage.list_master_discoveries(exchange="GFEX", variety_symbol="XY")[0]
    assert row["first_seen_trade_date"] == "2026-01-02"
    assert row["last_seen_trade_date"] == "2026-01-03"
    assert row["observed_contracts"] == ["XY2601", "XY2602"]

    storage.upsert_master_discoveries([conflicting])
    conflict = storage.list_master_discoveries(exchange="GFEX", variety_symbol="XY")[0]
    assert conflict["quality_flag"] == "conflict"
    assert conflict["review_status"] == "pending"
    assert "candidate_unit" in conflict["evidence"]["conflict_fields"]


def test_futures_master_discovery_governance_auto_promotes_dce_high_confidence(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "enabled_exchanges": ["DCE"],
        "adapters": {
            "DCE": {
                "enabled": True,
                "known_products": {
                    "XY": {
                        "name": "DCE Test Product",
                        "category": "chemical",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="XY",
                contract="XY2601",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["counts"]["candidates_discovered"] == 1
    assert result["counts"]["candidates_written"] == 1
    assert result["counts"]["auto_promoted"] == 1
    assert storage.get_instrument("CNF.XY.DCE")["symbol"] == "XY"
    assert storage.get_series("CNF.XY.DCE.main")["instrument_id"] == "CNF.XY.DCE"


def test_futures_master_discovery_uses_official_daily_name_without_promoting(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "enabled_exchanges": ["DCE"],
        "adapters": {"DCE": {"enabled": True, "known_products": {}}},
    }
    config.modules["commodity_market_data"].update(module_cfg)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="BZ",
                contract="BZ2601",
                raw_payload={"variety": "纯苯", "contractId": "bz2601"},
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    row = storage.list_master_discoveries(exchange="DCE", variety_symbol="BZ")[0]

    assert result["status"] == "warning"
    assert result["counts"]["candidates_discovered"] == 1
    assert result["counts"]["pending_review"] == 1
    assert result["counts"]["auto_promoted"] == 0
    assert row["candidate_name"] == "纯苯"
    assert row["candidate_unit"] == ""
    assert row["quality_flag"] == "discovered_unverified"
    assert row["review_status"] == "pending"
    assert storage.get_instrument("CNF.BZ.DCE") is None


def test_futures_master_discovery_enriches_dce_official_product_specs_without_guessing(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["DCE"]},
        "enabled_exchanges": ["DCE"],
        "adapters": {"DCE": {"enabled": True, "known_products": {}}},
    }
    config.modules["commodity_market_data"].update(module_cfg)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="BZ",
                contract="BZ2601",
                raw_payload={"variety": "纯苯", "contractId": "bz2601"},
            )
        ]

    def fake_fetch_exchange_product_specs_sync(self, exchange, target_symbols=None):
        return {
            "BZ": FuturesProductSpec(
                exchange="DCE",
                symbol="BZ",
                name="纯苯",
                contract_multiplier=5,
                tick_size=1,
                source_profile="exchange_official_product_spec",
                source_interface="official_dce_contract_info",
                evidence={"source_limitations": ["quote_unit_not_available"]},
            )
        }

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_product_specs_sync",
        fake_fetch_exchange_product_specs_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    row = storage.list_master_discoveries(exchange="DCE", variety_symbol="BZ")[0]

    assert result["status"] == "warning"
    assert result["counts"]["candidates_discovered"] == 1
    assert result["counts"]["pending_review"] == 1
    assert result["counts"]["auto_promoted"] == 0
    assert row["candidate_name"] == "纯苯"
    assert row["contract_multiplier"] == 5
    assert row["tick_size"] == 1
    assert row["candidate_unit"] == ""
    assert row["evidence"]["enrichment_status"] == "official_product_spec_partial"
    assert storage.get_instrument("CNF.BZ.DCE") is None


def test_futures_master_discovery_uses_daily_name_when_known_product_lacks_name(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": False},
        "enabled_exchanges": ["DCE"],
        "adapters": {
            "DCE": {
                "enabled": True,
                "known_products": {
                    "BZ": {
                        "category": "chemical",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="BZ",
                contract="BZ2601",
                raw_payload={"variety": "纯苯", "contractId": "BZ2601"},
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    row = storage.list_master_discoveries(exchange="DCE", variety_symbol="BZ")[0]
    instrument = storage.get_instrument("CNF.BZ.DCE")

    assert result["status"] == "success"
    assert result["counts"]["pending_review"] == 0
    assert result["counts"]["auto_promoted"] == 1
    assert row["candidate_name"] == "纯苯"
    assert row["evidence"]["name_source"] == "official_daily_rows"
    assert row["evidence"]["missing_required_fields"] == []
    assert row["evidence"]["field_sources"]["name"]["source_type"] == "official_daily_rows"
    assert row["evidence"]["field_sources"]["category"]["source_type"] == "governed_rule_metadata"
    assert row["evidence"]["field_sources"]["unit"]["source_type"] == "governed_rule_metadata"
    assert instrument["name"] == "纯苯"


def test_futures_master_discovery_promotes_dce_when_product_spec_and_rule_metadata_complete(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["DCE"]},
        "enabled_exchanges": ["DCE"],
        "adapters": {
            "DCE": {
                "enabled": True,
                "known_products": {
                    "BZ": {
                        "category": "chemical",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="BZ",
                contract="BZ2601",
                raw_payload={"variety": "纯苯", "contractId": "bz2601"},
            )
        ]

    def fake_fetch_exchange_product_specs_sync(self, exchange, target_symbols=None):
        return {
            "BZ": FuturesProductSpec(
                exchange="DCE",
                symbol="BZ",
                name="纯苯",
                contract_multiplier=30,
                tick_size=1,
                source_profile="exchange_official_product_spec",
                source_interface="official_dce_contract_info",
            )
        }

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_product_specs_sync",
        fake_fetch_exchange_product_specs_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    row = storage.list_master_discoveries(exchange="DCE", variety_symbol="BZ")[0]
    instrument = storage.get_instrument("CNF.BZ.DCE")

    assert result["status"] == "success"
    assert result["counts"]["pending_review"] == 0
    assert result["counts"]["auto_promoted"] == 1
    assert row["candidate_name"] == "纯苯"
    assert row["candidate_category"] == "chemical"
    assert row["candidate_unit"] == "CNY/ton"
    assert row["evidence"]["missing_required_fields"] == []
    assert row["evidence"]["field_sources"]["name"]["source_type"] == "official_product_spec"
    assert row["evidence"]["field_sources"]["category"]["source_type"] == "governed_rule_metadata"
    assert row["evidence"]["field_sources"]["unit"]["source_type"] == "governed_rule_metadata"
    assert row["contract_multiplier"] == 30
    assert row["tick_size"] == 1
    assert row["quality_flag"] == "promoted"
    assert row["review_status"] == "auto_promoted"
    assert instrument["symbol"] == "BZ"
    assert storage.get_series("CNF.BZ.DCE.main")["instrument_id"] == "CNF.BZ.DCE"


def test_gfex_governed_product_specs_use_common_enrichment_contract(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["GFEX"]},
        "enabled_exchanges": ["GFEX"],
        "adapters": {
            "GFEX": {
                "enabled": True,
                "known_products": {
                    "ZZ": {
                        "name": "GFEX Test Product",
                        "category": "test_category",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                        "source_url": "GFEX governed test rule",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "_fetch_gfex_product_page_specs",
        lambda self, target_symbols=None: {},
    )

    specs = OfficialFuturesMarketDataProvider(config).fetch_exchange_product_specs_sync("GFEX")

    assert set(specs) == {"ZZ"}
    assert specs["ZZ"].name == "GFEX Test Product"
    assert specs["ZZ"].category == "test_category"
    assert specs["ZZ"].unit == "CNY/ton"
    assert specs["ZZ"].source_profile == "governed_product_rule_metadata"
    assert specs["ZZ"].field_sources["name"]["source_type"] == "governed_rule_metadata"
    assert specs["ZZ"].field_sources["unit"]["source_ref"] == "GFEX governed test rule"


def test_gfex_official_product_page_specs_merge_with_governed_category(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["GFEX"]},
        "enabled_exchanges": ["GFEX"],
        "adapters": {
            "GFEX": {
                "enabled": True,
                "product_rule_pages": {"PT": "http://www.gfex.com.cn/gfex/sspzb/sspz.shtml"},
                "known_products": {
                    "PT": {
                        "name": "GFEX Platinum",
                        "category": "precious_metal",
                        "currency": "CNY",
                        "unit": "CNY/gram",
                        "source_url": "GFEX governed platinum rule",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    html = """
    <html>
      <head><meta name="ColumnName" content="铂" /></head>
      <body>
        <table>
          <tr><td>交易品种</td><td>铂</td></tr>
          <tr><td>交易单位</td><td>1000克/手</td></tr>
          <tr><td>报价单位</td><td>元（人民币）/克</td></tr>
          <tr><td>最小变动价位</td><td>0.2元/克</td></tr>
          <tr><td>交易代码</td><td>PT</td></tr>
        </table>
      </body>
    </html>
    """

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "_request_gfex_product_rule_page",
        lambda self, session, url, symbol: html if symbol == "PT" else (_ for _ in ()).throw(
            OfficialFuturesSourceUnavailable("not part of test")
        ),
    )

    specs = OfficialFuturesMarketDataProvider(config).fetch_exchange_product_specs_sync("GFEX")
    spec = specs["PT"]

    assert spec.name == "铂"
    assert spec.category == "precious_metal"
    assert spec.currency == "CNY"
    assert spec.unit == "CNY/gram"
    assert spec.contract_multiplier == 1000
    assert spec.tick_size == 0.2
    assert spec.field_sources["name"]["source_type"] == "official_product_rule_page"
    assert spec.field_sources["unit"]["source_type"] == "official_product_rule_page"
    assert spec.field_sources["category"]["source_type"] == "governed_rule_metadata"


def test_gfex_official_product_page_specs_auto_discovers_listed_product_page(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["GFEX"]},
        "enabled_exchanges": ["GFEX"],
        "adapters": {
            "GFEX": {
                "enabled": True,
                "listed_products_page": "http://www.gfex.com.cn/gfex/sspzb/sspz.shtml",
                "product_rule_pages": {},
                "known_products": {
                    "ZZ": {
                        "name": "GFEX Test Product",
                        "category": "test_category",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                        "source_url": "GFEX governed test rule",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    listing_html = """
    <html>
      <body>
        <dl class="sspz">
          <dd><a href="/gfex/zz/sspz.shtml" target="_self" title="测试品种">测试品种</a></dd>
        </dl>
      </body>
    </html>
    """
    product_html = """
    <html>
      <body>
        <table>
          <tr><td>交易品种</td><td>测试品种</td></tr>
          <tr><td>交易单位</td><td>10吨/手</td></tr>
          <tr><td>报价单位</td><td>元/吨</td></tr>
          <tr><td>最小变动价位</td><td>1元/吨</td></tr>
          <tr><td>交易代码</td><td>ZZ</td></tr>
        </table>
      </body>
    </html>
    """

    def fake_request(self, session, url, symbol):
        if symbol == "listed_products":
            return listing_html
        if str(url).endswith("/gfex/zz/sspz.shtml"):
            return product_html
        raise OfficialFuturesSourceUnavailable(f"unexpected url={url} symbol={symbol}")

    monkeypatch.setattr(OfficialFuturesMarketDataProvider, "_request_gfex_product_rule_page", fake_request)

    specs = OfficialFuturesMarketDataProvider(config).fetch_exchange_product_specs_sync(
        "GFEX",
        target_symbols=["ZZ"],
    )
    spec = specs["ZZ"]

    assert spec.name == "测试品种"
    assert spec.category == "test_category"
    assert spec.currency == "CNY"
    assert spec.unit == "CNY/ton"
    assert spec.contract_multiplier == 10
    assert spec.tick_size == 1
    assert spec.source_url == "http://www.gfex.com.cn/gfex/zz/sspz.shtml"
    assert spec.field_sources["name"]["source_type"] == "official_product_rule_page"
    assert spec.field_sources["category"]["source_type"] == "governed_rule_metadata"


def test_dce_official_product_page_specs_merge_with_governed_category(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["DCE"]},
        "enabled_exchanges": ["DCE"],
        "adapters": {
            "DCE": {
                "enabled": True,
                "product_rule_pages": {},
                "known_products": {
                    "BZ": {
                        "name": "DCE Benzene",
                        "category": "chemical",
                        "currency": "CNY",
                        "unit": "CNY/barrel",
                        "source_url": "DCE governed benzene rule",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    home_html = """
    <html>
      <body>
        <a href="http://www.dce.com.cn/dce/channel/list/7000020.html">纯苯期货/期权</a>
      </body>
    </html>
    """
    product_html = """
    <html>
      <body>
        <table>
          <tr><td>交易品种</td><td>纯苯</td></tr>
          <tr><td>交易单位</td><td>30吨/手</td></tr>
          <tr><td>报价单位</td><td>元/吨</td></tr>
          <tr><td>最小变动价位</td><td>1元/吨</td></tr>
          <tr><td>交易代码</td><td>BZ</td></tr>
        </table>
      </body>
    </html>
    """

    class FakeDceBrowserClient:
        def fetch_contract_info_payload(self):
            return {
                "success": True,
                "data": [
                    {
                        "contractId": "BZ2601",
                        "varietyOrder": "BZ",
                        "variety": "纯苯",
                        "unit": "30",
                        "tick": "1",
                    }
                ],
            }

        def fetch_page_html(self, url):
            if str(url).rstrip("/") == "http://www.dce.com.cn":
                return home_html
            return product_html

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "_get_dce_browser_client",
        lambda self: FakeDceBrowserClient(),
    )

    specs = OfficialFuturesMarketDataProvider(config).fetch_exchange_product_specs_sync("DCE", target_symbols=["BZ"])
    spec = specs["BZ"]

    assert spec.name == "纯苯"
    assert spec.category == "chemical"
    assert spec.currency == "CNY"
    assert spec.unit == "CNY/ton"
    assert spec.contract_multiplier == 30
    assert spec.tick_size == 1
    assert spec.field_sources["name"]["source_type"] == "official_product_rule_page"
    assert spec.field_sources["unit"]["source_type"] == "official_product_rule_page"
    assert spec.field_sources["category"]["source_type"] == "governed_rule_metadata"


def test_gfex_master_discovery_auto_promotes_from_common_product_spec(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": True, "enabled_exchanges": ["GFEX"]},
        "enabled_exchanges": ["GFEX"],
        "adapters": {
            "GFEX": {
                "enabled": True,
                "known_products": {
                    "ZZ": {
                        "name": "GFEX Test Product",
                        "category": "test_category",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                        "source_url": "GFEX governed test rule",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "_fetch_gfex_product_page_specs",
        lambda self, target_symbols=None: {},
    )
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="ZZ",
                contract="ZZ2601",
                raw_payload={"variety": "测试品种", "varietyOrder": "ZZ", "delivMonth": "2601"},
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["GFEX"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    row = storage.list_master_discoveries(exchange="GFEX", variety_symbol="ZZ")[0]
    instrument = storage.get_instrument("CNF.ZZ.GFEX")

    assert result["status"] == "success"
    assert result["counts"]["auto_promoted"] == 1
    assert row["evidence"]["missing_required_fields"] == []
    assert row["evidence"]["field_sources"]["name"]["source_type"] == "governed_rule_metadata"
    assert instrument["name"] == "GFEX Test Product"


def test_futures_master_discovery_preserves_legacy_product_lineage(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    module_cfg = _scope_module_cfg()
    module_cfg["master_data_discovery"] = {
        "enabled": True,
        "auto_promote_high_confidence": True,
        "official_product_spec_enrichment": {"enabled": False},
        "enabled_exchanges": ["DCE"],
        "adapters": {
            "DCE": {
                "enabled": True,
                "known_products": {
                    "S": {
                        "name": "大豆",
                        "category": "agriculture",
                        "currency": "CNY",
                        "unit": "CNY/ton",
                        "legacy_product": True,
                        "legacy_product_name": "old yellow soybean",
                        "successor_family": ["CNF.A.DCE", "CNF.B.DCE"],
                        "primary_chronological_successor": "CNF.A.DCE",
                        "oilseed_import_soybean_successor": "CNF.B.DCE",
                        "lineage_note": "legacy soybean split into A and B successor families",
                    }
                },
            }
        },
    }
    config.modules["commodity_market_data"].update(module_cfg)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="DCE",
            trade_date="2002-07-01",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="S",
                contract="S0209",
                raw_payload={"variety": "大豆", "contractId": "S0209"},
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["DCE"],
        start_date="2002-07-01",
        end_date="2002-07-01",
        dry_run=False,
    )
    instrument = storage.get_instrument("CNF.S.DCE")
    metadata = instrument["metadata"]
    lineage = metadata["master_discovery_evidence"]["product_lineage"]

    assert result["status"] == "success"
    assert result["counts"]["auto_promoted"] == 1
    assert instrument["name"] == "大豆"
    assert lineage["legacy_product"] is True
    assert lineage["successor_family"] == ["CNF.A.DCE", "CNF.B.DCE"]
    assert lineage["primary_chronological_successor"] == "CNF.A.DCE"
    assert lineage["oilseed_import_soybean_successor"] == "CNF.B.DCE"


def test_futures_master_discovery_pending_review_blocks_readiness_warning(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="ZZ",
                contract="ZZ2601",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterDiscoveryGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        scope_id="gfex_all",
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )
    readiness = FuturesReadinessService(storage, config.modules["commodity_market_data"]).build(scope_id="gfex_all")

    assert result["status"] == "warning"
    assert result["counts"]["pending_review"] == 1
    assert storage.list_master_discoveries(exchange="GFEX", variety_symbol="ZZ")[0]["quality_flag"] == "discovered_unverified"
    assert readiness["master_discovery"]["needs_master_review"] is True
    assert "needs_master_review:GFEX:ZZ" in readiness["warnings"]


def test_gfex_master_governance_persists_unknown_variety_discovery(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="LC",
                contract="LC2601",
            ),
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="ZZ",
                contract="ZZ2601",
            ),
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        scope_id="gfex_all",
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )

    assert result["status"] == "warning"
    assert result["counts"]["contracts_written"] == 1
    assert result["counts"]["master_discovery_candidates"] == 1
    assert storage.list_master_discoveries(exchange="GFEX", variety_symbol="ZZ")


def test_gfex_master_governance_uses_promoted_discovery_instruments(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_master_discoveries([
        FuturesMasterDiscoveryCandidate(
            discovery_id="GFEX:PT",
            exchange="GFEX",
            variety_symbol="PT",
            candidate_instrument_id="CNF.PT.GFEX",
            candidate_series_id="CNF.PT.GFEX.main",
            candidate_name="GFEX Platinum",
            candidate_category="precious_metal",
            candidate_currency="CNY",
            candidate_unit="CNY/gram",
            first_seen_trade_date="2026-01-02",
            last_seen_trade_date="2026-01-02",
            observed_contracts=["PT2606"],
            confidence_score=0.95,
            quality_flag="discovered_verified",
            review_status="none",
        )
    ])
    promotion = storage.promote_master_discovery("GFEX:PT")
    assert promotion["status"] == "success"
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    def fake_fetch_exchange_contract_bars_sync(self, exchange, trade_date):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="PT",
                contract="PT2606",
            )
        ]

    monkeypatch.setattr(
        OfficialFuturesMarketDataProvider,
        "fetch_exchange_contract_bars_sync",
        fake_fetch_exchange_contract_bars_sync,
    )

    result = FuturesMasterGovernanceService(
        storage,
        config,
        config.modules["commodity_market_data"],
    ).run(
        exchanges=["GFEX"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=False,
    )

    assert result["status"] == "success"
    assert result["counts"]["instruments"] == 4
    assert result["counts"]["contracts_written"] == 1
    assert storage.list_contracts(exchange="GFEX")[0]["instrument_id"] == "CNF.PT.GFEX"


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


def test_futures_storage_persists_contract_bars_mapping_and_calendar(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    registry = default_futures_registry(config.modules["commodity_market_data"])
    storage.upsert_categories(registry["categories"])
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])

    contract = FuturesContract(
        contract_id="CNF.CU.SHFE.CU2407",
        instrument_id="CNF.CU.SHFE",
        exchange="SHFE",
        exchange_contract_code="CU2407",
        contract_month="2024-07",
        currency="CNY",
        unit="CNY/ton",
        source="fixture",
    )
    contract_bar = FuturesContractBar(
        contract_id=contract.contract_id,
        instrument_id=contract.instrument_id,
        trade_date="2024-06-03",
        open=10,
        high=12,
        low=9,
        close=11,
        raw_payload_hash="contract-hash",
        source="exchange_official",
        source_mode="direct",
        source_profile="exchange_official",
        source_interface="fixture",
    )
    mapping = FuturesContinuousMapping(
        series_id="CNF.CU.SHFE.main",
        trade_date="2024-06-03",
        contract_id=contract.contract_id,
        exchange_contract_code="CU2407",
        instrument_id=contract.instrument_id,
        construction_method="official_open_interest_main",
        selection_open_interest=200,
        selection_volume=100,
    )

    storage.upsert_contracts([contract])
    write_result = storage.upsert_contract_price_bars([contract_bar])
    storage.upsert_continuous_mappings([mapping])
    calendar_result = FuturesCalendarService(storage, config.modules["commodity_market_data"]).seed_default_calendar(
        exchanges=["SHFE"],
        start_date="2024-06-01",
        end_date="2024-06-04",
    )

    assert write_result["inserted"] == 1
    assert storage.get_contract(contract.contract_id)["exchange_contract_code"] == "CU2407"
    assert storage.get_contract_price_bars(contract.contract_id)[0]["close"] == 11
    assert storage.list_continuous_mappings("CNF.CU.SHFE.main")[0]["contract_id"] == contract.contract_id
    assert calendar_result["calendar_rows"] == 4
    assert storage.get_latest_expected_trade_date("SHFE", as_of_date="2024-06-04")["trade_date"] == "2024-06-04"


def test_futures_calendar_seed_does_not_downgrade_verified_days(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2026-06-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    result = FuturesCalendarService(
        storage,
        config.modules["commodity_market_data"],
    ).seed_default_calendar(
        exchanges=["GFEX"],
        start_date="2026-06-02",
        end_date="2026-06-03",
    )

    rows = storage.list_calendar_days(exchange="GFEX", start_date="2026-06-02", end_date="2026-06-03")
    row_by_date = {row["trade_date"]: row for row in rows}
    assert result["calendar_rows"] == 1
    assert row_by_date["2026-06-02"]["quality_flag"] == "backfilled_verified"
    assert row_by_date["2026-06-02"]["source_profile"] == "exchange_official_daily_probe"
    assert row_by_date["2026-06-03"]["quality_flag"] == "estimated"


def test_futures_storage_persists_trading_day_governance_tables(tmp_path):
    config = _research_config(tmp_path)
    storage = FuturesStorageManager(config)
    storage.initialize()
    registry = default_futures_registry(config.modules["commodity_market_data"])
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])

    notice = FuturesCalendarNotice(
        notice_id="SHFE:notice:2024",
        exchange="SHFE",
        source_profile="exchange_official_calendar",
        notice_type="holiday_notice",
        title="2024 holiday notice",
        url="https://www.shfe.com.cn/notice",
        raw_content_hash="hash",
        raw_payload={"calendar": [{"trade_date": "2024-06-03", "is_trading_day": True}]},
        parse_status="parsed",
        confidence=0.9,
        derived_changes=[{"trade_date": "2024-06-03"}],
    )
    review = FuturesManualCalendarReview(
        review_id="review-1",
        status="review_required",
        decision="pending",
        reviewer="",
        reason="fixture ambiguous",
        evidence_ref=notice.notice_id,
        scope_type="exchange",
        exchange="SHFE",
        trade_dates=["2024-06-03"],
    )
    override = FuturesInstrumentCalendarOverride(
        override_id="override-1",
        instrument_id="CNF.CU.SHFE",
        exchange="SHFE",
        start_date="2024-06-03",
        end_date="2024-06-03",
        is_trading_day=True,
        manual_review_id=review.review_id,
    )

    assert storage.upsert_calendar_notices([notice]) == 1
    assert storage.upsert_manual_calendar_reviews([review]) == 1
    assert storage.upsert_instrument_calendar_overrides([override]) == 1

    assert storage.list_calendar_notices(exchange="SHFE")[0]["raw_payload"]["calendar"][0]["trade_date"] == "2024-06-03"
    assert storage.list_manual_calendar_reviews(status="review_required")[0]["evidence_ref"] == notice.notice_id
    assert storage.list_instrument_calendar_overrides(instrument_id="CNF.CU.SHFE")[0]["manual_review_id"] == review.review_id


def test_trading_day_governance_expands_dates_and_quality_gates(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["trading_day_governance"] = {
        "enabled_exchanges": ["SHFE"],
        "quality_gates": {
            "dry_run_min_quality": "estimated",
            "production_min_quality": "official_parsed",
        },
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    service = FuturesTradingDayGovernanceService(storage, config.modules["commodity_market_data"])
    service.bootstrap_estimated_calendar(
        exchanges=["SHFE"],
        start_date="2024-06-01",
        end_date="2024-06-04",
    )

    dry_run = service.validate_quality_gate(
        service.expand_target_dates(
            exchanges=["SHFE"],
            start_date="2024-06-01",
            end_date="2024-06-04",
            dry_run=True,
        ),
        dry_run=True,
    )
    production = service.validate_quality_gate(
        service.expand_target_dates(
            exchanges=["SHFE"],
            start_date="2024-06-01",
            end_date="2024-06-04",
            dry_run=False,
        ),
        dry_run=False,
    )

    assert dry_run["target_dates_by_exchange"]["SHFE"] == ["2024-06-03", "2024-06-04"]
    assert dry_run["skipped_dates_by_exchange"]["SHFE"] == ["2024-06-01", "2024-06-02"]
    assert dry_run["production_write_eligible"] is True
    assert production["status"] == "blocked"
    assert "calendar_quality_below_threshold:SHFE:estimated<required:official_parsed" in production["blockers"]


def test_official_futures_calendar_provider_parses_structured_notice(tmp_path):
    provider = OfficialFuturesCalendarProvider(_research_config(tmp_path))
    notice = FuturesCalendarNotice(
        notice_id="SHFE:calendar:fixture",
        exchange="SHFE",
        source_profile="exchange_official_calendar",
        notice_type="calendar",
        raw_content_hash="hash",
        raw_payload={
            "calendar": [
                {"trade_date": "2024-06-03", "is_trading_day": True},
                {"trade_date": "2024-06-08", "is_trading_day": False},
            ]
        },
    )

    parsed = provider.parse_notice(notice)

    assert parsed.review_required is False
    assert parsed.notice.parse_status == "parsed"
    assert [item.trade_date for item in parsed.calendar_days] == ["2024-06-03", "2024-06-08"]
    assert parsed.calendar_days[0].quality_flag == "official_parsed"


def test_official_futures_provider_probe_classifies_trading_and_closed(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}
    }
    provider = OfficialFuturesMarketDataProvider(config)

    monkeypatch.setattr(
        provider,
        "_request_exchange_payload",
        lambda session, exchange, trade_date: {
            "o_curinstrument": [
                {
                    "PRODUCTGROUPID": "cu",
                    "DELIVERYMONTH": "2407",
                    "OPENPRICE": "10",
                    "HIGHESTPRICE": "12",
                    "LOWESTPRICE": "9",
                    "CLOSEPRICE": "11",
                }
            ]
        },
    )

    trading = provider.probe_exchange_trading_day("SHFE", "2024-06-03")
    assert trading.status == "trading"
    assert trading.is_trading_day is True
    assert trading.row_count == 1

    monkeypatch.setattr(provider, "_request_exchange_payload", lambda session, exchange, trade_date: {"o_curinstrument": []})
    closed = provider.probe_exchange_trading_day("SHFE", "2024-06-08")
    assert closed.status == "closed"
    assert closed.is_trading_day is False
    assert closed.row_count == 0


def test_official_futures_provider_probe_does_not_close_old_empty_payload(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}
    }
    config.modules["commodity_market_data"]["trading_day_governance"] = {
        "official_calendar_backfill": {
            "empty_payload_closed_start_dates": {"SHFE": "2010-01-01"}
        }
    }
    provider = OfficialFuturesMarketDataProvider(config)

    monkeypatch.setattr(provider, "_request_exchange_payload", lambda session, exchange, trade_date: {"o_curinstrument": []})

    result = provider.probe_exchange_trading_day("SHFE", "2000-01-04")

    assert result.status == "unresolved"
    assert result.is_trading_day is None
    assert "before reliable empty-closed start date" in result.failure_reason


def test_official_futures_provider_probe_does_not_close_old_no_report(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}
    }
    config.modules["commodity_market_data"]["trading_day_governance"] = {
        "official_calendar_backfill": {
            "empty_payload_closed_start_dates": {"SHFE": "2010-01-01"}
        }
    }
    provider = OfficialFuturesMarketDataProvider(config)

    def _raise(*args, **kwargs):
        raise OfficialFuturesSourceUnavailable("404 Client Error: Not Found")

    monkeypatch.setattr(provider, "_request_exchange_payload", _raise)

    result = provider.probe_exchange_trading_day("SHFE", "2000-01-04")

    assert result.status == "unresolved"
    assert result.is_trading_day is None
    assert result.metadata["classification_rule"] == "official_no_report_before_reliable_history_start"


def test_official_futures_provider_probe_keeps_failures_unresolved(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}
    }
    provider = OfficialFuturesMarketDataProvider(config)

    def _raise(*args, **kwargs):
        raise OfficialFuturesSourceUnavailable("timeout")

    monkeypatch.setattr(provider, "_request_exchange_payload", _raise)

    result = provider.probe_exchange_trading_day("SHFE", "2024-06-03")

    assert result.status == "unresolved"
    assert result.is_trading_day is None
    assert "timeout" in result.failure_reason


def test_official_calendar_backfill_writes_verified_rows_and_no_weekday_guess(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["trading_day_governance"] = {
        "enabled_exchanges": ["SHFE"],
        "official_calendar_backfill": {
            "start_date": "2010-01-01",
            "retry_unresolved_pause_seconds": 0,
        },
    }
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"]}
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    def _probe(self, exchange, trade_date):
        if trade_date == "2024-06-03":
            return OfficialFuturesDailyProbeResult(
                exchange=exchange,
                trade_date=trade_date,
                status="trading",
                is_trading_day=True,
                row_count=2,
                source_interface="fixture",
                evidence_url="https://official.example/20240603",
                parser_version="fixture.v1",
                payload_hash="hash-trading",
            )
        if trade_date == "2024-06-04":
            return OfficialFuturesDailyProbeResult(
                exchange=exchange,
                trade_date=trade_date,
                status="closed",
                is_trading_day=False,
                row_count=0,
                source_interface="fixture",
                evidence_url="https://official.example/20240604",
                parser_version="fixture.v1",
                payload_hash="hash-closed",
            )
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="unresolved",
            is_trading_day=None,
            row_count=0,
            source_interface="fixture",
            evidence_url="https://official.example/20240605",
            parser_version="fixture.v1",
            failure_reason="fixture failure",
        )

    monkeypatch.setattr("research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day", _probe)

    result = FuturesOfficialCalendarBackfillService(storage, config, config.modules["commodity_market_data"]).run(
        exchanges=["SHFE"],
        start_date="2024-06-03",
        end_date="2024-06-05",
    )
    rows = storage.list_calendar_days(exchange="SHFE", start_date="2024-06-03", end_date="2024-06-05")

    assert result["status"] == "blocked"
    assert result["totals"]["rows_written"] == 2
    assert result["totals"]["unresolved_dates"] == 1
    assert [row["trade_date"] for row in rows] == ["2024-06-03", "2024-06-04"]
    assert {row["quality_flag"] for row in rows} == {"backfilled_verified"}
    assert storage.list_manual_calendar_reviews(status="review_required")[0]["metadata"]["unresolved_count"] == 1


def test_official_calendar_backfill_retries_unresolved_dates_at_task_end(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["trading_day_governance"] = {
        "enabled_exchanges": ["GFEX"],
        "official_calendar_backfill": {
            "start_date": "2022-12-22",
            "retry_unresolved_passes": 1,
            "retry_unresolved_pause_seconds": 0,
        },
    }
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["GFEX"]}
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    attempts = {}

    def _probe(self, exchange, trade_date):
        attempts[trade_date] = attempts.get(trade_date, 0) + 1
        if trade_date == "2024-06-04" and attempts[trade_date] == 1:
            return OfficialFuturesDailyProbeResult(
                exchange=exchange,
                trade_date=trade_date,
                status="unresolved",
                is_trading_day=None,
                row_count=0,
                source_interface="fixture",
                evidence_url="https://official.example/20240604",
                parser_version="fixture.v1",
                failure_reason="gfex_html_challenge http_status=567",
                metadata={"failure_category": "possible_anti_bot_or_ip_risk_control", "is_retryable": True},
            )
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="trading",
            is_trading_day=True,
            row_count=2,
            source_interface="fixture",
            evidence_url=f"https://official.example/{trade_date.replace('-', '')}",
            parser_version="fixture.v1",
            payload_hash=f"hash-{trade_date}",
        )

    monkeypatch.setattr("research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day", _probe)

    result = FuturesOfficialCalendarBackfillService(storage, config, config.modules["commodity_market_data"]).run(
        exchanges=["GFEX"],
        start_date="2024-06-03",
        end_date="2024-06-05",
    )
    rows = storage.list_calendar_days(exchange="GFEX", start_date="2024-06-03", end_date="2024-06-05")

    assert result["status"] == "success"
    assert result["totals"]["rows_written"] == 3
    assert result["totals"]["unresolved_dates"] == 0
    assert result["exchanges"][0]["retry_passes_attempted"] == 1
    assert result["exchanges"][0]["retry_dates_resolved"] == 1
    assert attempts["2024-06-04"] == 2
    assert [row["trade_date"] for row in rows] == ["2024-06-03", "2024-06-04", "2024-06-05"]
    assert storage.list_manual_calendar_reviews(status="review_required") == []


def test_official_calendar_backfill_max_days_reports_partial_not_unresolved(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["trading_day_governance"] = {
        "enabled_exchanges": ["DCE"],
        "official_calendar_backfill": {
            "start_date": "2000-06-01",
            "retry_unresolved_passes": 1,
            "retry_unresolved_pause_seconds": 0,
        },
    }
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": True, "enabled_exchanges": ["DCE"]}
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    def _probe(self, exchange, trade_date):
        return OfficialFuturesDailyProbeResult(
            exchange=exchange,
            trade_date=trade_date,
            status="trading",
            is_trading_day=True,
            row_count=2,
            source_interface="fixture",
            evidence_url=f"https://official.example/{trade_date.replace('-', '')}",
            parser_version="fixture.v1",
            payload_hash=f"hash-{trade_date}",
        )

    monkeypatch.setattr("research.providers.official_futures.OfficialFuturesMarketDataProvider.probe_exchange_trading_day", _probe)

    result = FuturesOfficialCalendarBackfillService(storage, config, config.modules["commodity_market_data"]).run(
        exchanges=["DCE"],
        start_date="2000-06-01",
        end_date="2000-06-05",
        dry_run=True,
        max_days=2,
    )

    assert result["status"] == "partial"
    assert result["totals"]["request_count"] == 2
    assert result["totals"]["unresolved_dates"] == 0
    assert result["totals"]["truncated_dates"] == 3
    assert result["exchanges"][0]["status"] == "partial"
    assert result["exchanges"][0]["truncated_from_date"] == "2000-06-03"
    assert result["exchanges"][0]["failure_samples"] == []
    assert storage.list_manual_calendar_reviews(status="review_required") == []


def test_default_futures_registry_includes_domestic_p0_universe(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["registry"] = {"include_default_p0_universe": True}

    registry = default_futures_registry(config.modules["commodity_market_data"])
    instrument_ids = {item.instrument_id for item in registry["instruments"]}
    series_ids = {item.series_id for item in registry["series"]}
    categories = {item.category for item in registry["categories"]}

    assert len(instrument_ids) >= 40
    assert {"ferrous", "nonferrous", "precious_metal", "energy", "chemical", "agriculture"}.issubset(
        categories
    )
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


def test_futures_id_helpers_are_deterministic():
    assert make_futures_instrument_id("cu", "shfe") == "CNF.CU.SHFE"
    assert make_futures_instrument_id("cl", "cme", namespace="glf") == "GLF.CL.CME"
    assert make_futures_contract_id("CNF.CU.SHFE", "cu2407") == "CNF.CU.SHFE.CU2407"
    assert make_futures_series_id("CNF.CU.SHFE") == "CNF.CU.SHFE.main"
    assert make_futures_series_id("CNF.CU.SHFE", "index_continuous") == "CNF.CU.SHFE.index"
    assert infer_contract_month("CU2407") == "2024-07"
    assert infer_contract_month("TA409") == "2024-09"


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


def test_futures_readiness_uses_trading_calendar_expected_date(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["coverage"]["max_stale_trading_days"] = 0
    storage = FuturesStorageManager(config)
    storage.initialize()
    registry = default_futures_registry(config.modules["commodity_market_data"])
    storage.upsert_categories(registry["categories"])
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])
    FuturesCalendarService(storage, config.modules["commodity_market_data"]).seed_default_calendar(
        exchanges=["SHFE"],
        start_date="2024-06-01",
        end_date="2024-06-03",
    )
    storage.upsert_price_bars(
        [
            FuturesBar(
                series_id="CNF.CU.SHFE.main",
                trade_date="2024-05-31",
                open=10,
                high=12,
                low=9,
                close=11,
                raw_payload_hash="calendar-stale",
                source="exchange_official",
                source_profile="exchange_official",
                source_mode="direct",
            )
        ]
    )

    payload = FuturesReadinessService(storage, config.modules["commodity_market_data"]).build()
    cu_payload = next(item for item in payload["series"] if item["series_id"] == "CNF.CU.SHFE.main")

    assert cu_payload["latest_expected_trade_date"] == "2024-06-03"
    assert "stale_latest_bar" in cu_payload["warnings"]
    assert cu_payload["calendar_source_profile"] == "estimated_weekday_calendar"


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

    artifacts = provider._build_storage_artifacts(series, rows, mode="direct")
    bars = artifacts["series_bars"]

    assert len(bars) == 1
    assert len(artifacts["contracts"]) == 2
    assert len(artifacts["contract_bars"]) == 2
    assert len(artifacts["mappings"]) == 1
    assert bars[0].source_profile == "exchange_official"
    assert bars[0].source_interface == "official_shfe_daily_kx_dat"
    assert bars[0].close == 21
    assert bars[0].metadata["underlying_contract"] == "CU2408"
    assert bars[0].metadata["underlying_contract_id"] == "CNF.CU.SHFE.CU2408"
    assert bars[0].metadata["construction_method"] == "official_open_interest_main"
    assert artifacts["mappings"][0].contract_id == "CNF.CU.SHFE.CU2408"


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
    dce_closed = provider._parse_dce_payload(
        {
            "data": [
                {
                    "variety": "总计",
                    "contractId": None,
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "clearPrice": None,
                    "volumn": 0,
                    "openInterest": 0,
                    "turnover": None,
                }
            ]
        },
        trade_date="2024-06-08",
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
    assert dce_closed == []
    assert gfex[0].contract == "SI2409"
    assert gfex[0].source_interface == "official_gfex_ti_day_quotes"
    assert czce[0].contract == "TA409"
    assert czce[0].volume == 123
    assert czce[0].source_interface == "official_czce_future_data_daily_txt"


def test_official_futures_provider_dce_direct_uses_futures_trade_type(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {
            "enabled": True,
            "enabled_exchanges": ["DCE"],
            "dce_browser": {"enabled": False},
        }
    }
    provider = OfficialFuturesMarketDataProvider(config)
    captured = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"success": True, "data": []}

    def fake_post(url, *, session, tls_config, json, timeout):
        captured["url"] = url
        captured["json"] = json
        return Response()

    monkeypatch.setattr("research.providers.official_futures.request_post", fake_post)

    payload = provider._request_exchange_payload(None, "DCE", "2024-06-03")

    assert payload["data"] == []
    assert captured["json"]["tradeType"] == "0"
    assert captured["json"]["statisticsType"] == 0
    assert captured["json"]["lang"] is None


def test_official_futures_provider_parses_dce_contract_info_specs(tmp_path):
    provider = OfficialFuturesMarketDataProvider(_research_config(tmp_path))

    specs = provider._parse_dce_contract_info_payload(
        {
            "data": [
                {
                    "contractId": "BZ2601",
                    "variety": "纯苯",
                    "varietyOrder": "BZ",
                    "unit": "5",
                    "tick": "1",
                    "startTradeDate": "20250630",
                    "endTradeDate": "20260114",
                    "endDeliveryDate": "20260120",
                },
                {
                    "contractId": "BZ2602",
                    "variety": "纯苯",
                    "varietyOrder": "BZ",
                    "unit": "5",
                    "tick": "1",
                },
            ]
        }
    )

    assert set(specs) == {"BZ"}
    assert specs["BZ"].name == "纯苯"
    assert specs["BZ"].contract_multiplier == 5
    assert specs["BZ"].tick_size == 1
    assert specs["BZ"].unit == ""
    assert specs["BZ"].source_interface == "official_dce_contract_info"
    assert "dce_contract_info_unit_is_contract_trading_unit_not_quote_unit" in specs["BZ"].evidence["source_limitations"]


def test_official_futures_provider_gfex_uses_ajax_headers(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {
            "enabled": True,
            "enabled_exchanges": ["GFEX"],
        }
    }
    provider = OfficialFuturesMarketDataProvider(config)
    captured = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"code": "0", "data": []}

    def fake_post(url, *, session, tls_config, data, headers, timeout):
        captured["url"] = url
        captured["data"] = data
        captured["headers"] = headers
        captured["session_headers"] = dict(session.headers)
        return Response()

    monkeypatch.setattr("research.providers.official_futures.request_post", fake_post)

    payload = provider._request_exchange_payload(None, "GFEX", "2024-06-12")

    assert payload["data"] == []
    assert captured["url"].endswith("/u/interfacesWebTiDayQuotes/loadList")
    assert captured["data"] == {"trade_date": "20240612", "trade_type": "0", "variety": ""}
    assert captured["headers"]["X-Requested-With"] == "XMLHttpRequest"
    assert captured["headers"]["Origin"] == "http://www.gfex.com.cn"
    assert captured["session_headers"]["Referer"] == "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml"


def test_official_futures_provider_gfex_challenge_retry_survives_prior_generic_error(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {
            "enabled": True,
            "enabled_exchanges": ["GFEX"],
            "retry_attempts": 2,
            "retry_backoff_seconds": 0,
            "request_interval_seconds_by_exchange": {"GFEX": 0},
            "challenge_retry_attempts_by_exchange": {"GFEX": 3},
            "challenge_backoff_seconds_by_exchange": {"GFEX": 0},
        }
    }
    provider = OfficialFuturesMarketDataProvider(config)
    calls = {"count": 0}

    class Response:
        def __init__(self, *, status_code=200, headers=None, payload=None, text=""):
            self.status_code = status_code
            self.headers = headers or {}
            self._payload = payload or {}
            self.text = text

        def raise_for_status(self):
            if self.status_code >= 400 and self.status_code != 567:
                raise requests.HTTPError(f"{self.status_code} error")

        def json(self):
            return self._payload

    def fake_post(url, *, session, tls_config, data, headers, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.ConnectionError("connection reset")
        if calls["count"] == 2:
            return Response(
                status_code=567,
                headers={"content-type": "text/html; charset=UTF-8"},
                text="<html>challenge</html>",
            )
        return Response(status_code=200, payload={"code": "0", "data": [{"variety": "SI"}]})

    monkeypatch.setattr("research.providers.official_futures.request_post", fake_post)

    payload = provider._request_exchange_payload(None, "GFEX", "2023-08-11")

    assert calls["count"] == 3
    assert payload["data"] == [{"variety": "SI"}]


def test_official_futures_provider_uses_exchange_specific_request_interval(tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {
            "enabled": True,
            "enabled_exchanges": ["SHFE", "GFEX"],
            "request_interval_seconds": 0.05,
            "request_interval_seconds_by_exchange": {"DCE": 1.0, "GFEX": 0.9},
            "challenge_retry_attempts_by_exchange": {"GFEX": 3},
            "challenge_backoff_seconds_by_exchange": {"GFEX": 10},
            "rate_limit_retry_attempts_by_exchange": {"DCE": 3},
            "rate_limit_backoff_seconds_by_exchange": {"DCE": 60},
            "batch_pause_every_requests_by_exchange": {"DCE": 100, "GFEX": 180},
            "batch_pause_seconds_by_exchange": {"DCE": 60, "GFEX": 10},
        }
    }

    provider = OfficialFuturesMarketDataProvider(config)

    assert provider._request_interval_for_exchange("DCE") == 1.0
    assert provider._request_interval_for_exchange("GFEX") == 0.9
    assert provider._request_interval_for_exchange("SHFE") == 0.05
    assert provider._challenge_retry_attempts_for_exchange("GFEX") == 3
    assert provider._challenge_retry_attempts_for_exchange("SHFE") == 0
    assert provider._challenge_backoff_for_exchange("GFEX") == 10
    assert provider._rate_limit_retry_attempts_for_exchange("DCE") == 3
    assert provider._rate_limit_backoff_for_exchange("DCE") == 60
    assert provider.batch_pause_every_requests_by_exchange["DCE"] == 100
    assert provider.batch_pause_seconds_by_exchange["DCE"] == 60
    assert provider.batch_pause_every_requests_by_exchange["GFEX"] == 180
    assert provider.batch_pause_seconds_by_exchange["GFEX"] == 10


def test_official_futures_provider_dce_rate_limit_uses_exchange_backoff(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {
            "enabled": True,
            "enabled_exchanges": ["DCE"],
            "retry_attempts": 2,
            "retry_backoff_seconds": 0,
            "request_interval_seconds_by_exchange": {"DCE": 0},
            "rate_limit_retry_attempts_by_exchange": {"DCE": 2},
            "rate_limit_backoff_seconds_by_exchange": {"DCE": 60},
            "dce_browser": {"enabled": False},
        }
    }
    provider = OfficialFuturesMarketDataProvider(config)
    calls = {"count": 0}
    sleeps = []

    class Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    def fake_post(url, *, session, tls_config, json, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            return Response({"success": False, "msg": "访问过于频繁，请稍后访问！"})
        return Response({"success": True, "data": []})

    monkeypatch.setattr("research.providers.official_futures.request_post", fake_post)
    monkeypatch.setattr("research.providers.official_futures.time.sleep", lambda seconds: sleeps.append(seconds))

    payload = provider._request_exchange_payload(None, "DCE", "2024-06-03")

    assert payload["success"] is True
    assert calls["count"] == 2
    assert sleeps == [60]
    metrics = provider.snapshot_metrics()["DCE"]
    assert metrics["rate_limit_count"] == 1
    assert metrics["rate_limit_backoff_seconds"] == 60


def test_official_futures_provider_metrics_snapshot(tmp_path):
    config = _research_config(tmp_path)
    provider = OfficialFuturesMarketDataProvider(config)

    provider._increment_metric("GFEX", "challenge_count", 1)
    provider._increment_metric("GFEX", "batch_pause_seconds", 20)

    snapshot = provider.snapshot_metrics()
    assert snapshot["GFEX"]["challenge_count"] == 1
    assert snapshot["GFEX"]["batch_pause_seconds"] == 20


def test_official_futures_provider_detects_gfex_html_challenge(tmp_path):
    config = _research_config(tmp_path)
    provider = OfficialFuturesMarketDataProvider(config)
    response = requests.Response()
    response.status_code = 567
    response.headers["content-type"] = "text/html; charset=UTF-8"
    response._content = b"<!doctype html><html lang=zh-CN></html>"

    assert provider._is_challenge_response(response) is True
    assert provider._is_retryable_challenge(
        OfficialFuturesSourceUnavailable("gfex_html_challenge http_status=567")
    ) is True


def test_official_futures_provider_dce_uses_browser_client(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {
            "enabled": True,
            "enabled_exchanges": ["DCE"],
            "dce_browser": {"enabled": True, "settle_seconds": 0},
        }
    }
    calls = []

    class FakeDceBrowserClient:
        def __init__(self, cfg):
            calls.append(("init", cfg))
            self.closed = False

        def fetch_day_quotes_payload(self, trade_date):
            calls.append(("fetch", trade_date))
            return {
                "success": True,
                "data": [
                    {
                        "variety": "铁矿石",
                        "contractId": "i2409",
                        "open": "800",
                        "high": "810",
                        "low": "790",
                        "close": "805",
                        "clearPrice": "803",
                        "volumn": "1000",
                        "openInterest": "2000",
                        "turnover": "123456",
                    }
                ],
            }

        def close(self):
            calls.append(("close", None))
            self.closed = True

    monkeypatch.setattr("research.providers.official_futures.DceOfficialBrowserClient", FakeDceBrowserClient)
    provider = OfficialFuturesMarketDataProvider(config)

    result = provider.probe_exchange_trading_day("DCE", "2024-06-03")
    provider.close()

    assert result.status == "trading"
    assert result.is_trading_day is True
    assert result.row_count == 1
    assert calls == [
        ("init", {"enabled": True, "settle_seconds": 0}),
        ("fetch", "20240603"),
        ("close", None),
    ]


def test_dce_browser_client_resolves_chrome_path_precedence(monkeypatch):
    monkeypatch.setattr("research.providers.official_futures._default_dce_chrome_path", lambda: "/opt/google/chrome/chrome")
    monkeypatch.delenv("QUOTE_DCE_CHROME_PATH", raising=False)

    assert DceOfficialBrowserClient({}).browser_executable_path == "/opt/google/chrome/chrome"

    monkeypatch.setenv("QUOTE_DCE_CHROME_PATH", "/env/chrome")
    assert DceOfficialBrowserClient({}).browser_executable_path == "/env/chrome"
    assert DceOfficialBrowserClient({"browser_executable_path": "/cfg/chrome"}).browser_executable_path == "/cfg/chrome"


def test_official_futures_failure_classification_marks_network_and_antibot():
    network = classify_official_futures_failure("[Errno 101] Network is unreachable")
    assert network.category == "network_unreachable"
    assert network.suspected_local_ip_risk_control is True

    antibot = classify_official_futures_failure("HTTP 403 Forbidden access denied by WAF")
    assert antibot.category == "possible_anti_bot_or_ip_risk_control"
    assert antibot.suspected_local_ip_risk_control is True

    challenge = classify_official_futures_failure("567 Server Error: Unknown Status for url")
    assert challenge.category == "possible_anti_bot_or_ip_risk_control"
    assert challenge.suspected_local_ip_risk_control is True

    no_report = classify_official_futures_failure(
        "404 Client Error: Not Found for url: https://example.test/kx20140315.dat"
    )
    assert no_report.category == "official_not_found_or_no_report"
    assert no_report.suspected_local_ip_risk_control is False


def test_futures_smoke_writes_failure_report_on_exception(tmp_path):
    from scripts.dev_validation.validate_futures_market_data_smoke import build_parser, write_failure_report

    output_path = tmp_path / "smoke_failure.json"
    args = build_parser().parse_args(
        [
            "--series-ids",
            "CNF.CU.SHFE.main",
            "--start-date",
            "bad-date",
            "--end-date",
            "2024-06-03",
            "--db-path",
            str(tmp_path / "smoke.db"),
            "--output-path",
            str(output_path),
        ]
    )
    write_failure_report(args, ValueError("bad date fixture"))

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["exception_type"] == "ValueError"
    assert payload["start_date"] == "bad-date"


def _official_contract_row(
    *,
    exchange: str = "SHFE",
    trade_date: str = "2024-06-03",
    variety: str = "CU",
    contract: str = "CU2407",
    close: float = 11,
    open_interest: float = 200,
    raw_payload: Optional[dict] = None,
) -> OfficialFuturesContractBar:
    return OfficialFuturesContractBar(
        exchange=exchange,
        trade_date=trade_date,
        variety=variety,
        contract=contract,
        open=10,
        high=12,
        low=9,
        close=close,
        settlement=10.5,
        volume=100,
        open_interest=open_interest,
        amount=1234,
        source_interface="official_shfe_daily_kx_dat",
        raw_payload=raw_payload or {"contract": contract},
    )


def _seed_dce_iron_ore_master(storage: FuturesStorageManager) -> None:
    storage.upsert_instruments_and_series(
        [
            FuturesInstrument(
                instrument_id="CNF.I.DCE",
                symbol="I",
                name="DCE Iron Ore",
                exchange="DCE",
                category="ferrous",
                currency="CNY",
                unit="CNY/ton",
                priority="P0",
                active=True,
                source_profiles=["configured_seed"],
                metadata={},
            )
        ],
        [
            FuturesSeries(
                series_id="CNF.I.DCE.main",
                instrument_id="CNF.I.DCE",
                symbol="I.main",
                series_type="main_continuous",
                source_profile="exchange_official",
                source="exchange_official",
                source_mode="direct",
                source_interface="official_dce_daily_contract_fanout",
                construction_method="exchange_main_continuous",
                currency="CNY",
                unit="CNY/ton",
                priority="P0",
                active=True,
                metadata={},
            )
        ],
    )


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
async def test_futures_market_data_sync_uses_governed_trading_dates(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "exchange_official": {"enabled": False},
        "akshare_futures": {"enabled": True, "timeout_seconds": 1},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    requested_dates = []

    async def fake_fetch_daily_bars(self, series, *, start_date=None, end_date=None, mode="direct"):
        requested_dates.append((start_date, end_date))
        return [
            FuturesBar(
                series_id=series.series_id,
                trade_date=start_date,
                open=10,
                high=12,
                low=9,
                close=11,
                raw_payload_hash=f"{series.series_id}:{start_date}",
                source="akshare",
                source_mode=mode,
                source_profile="akshare_futures",
                source_interface="fixture",
            )
        ]

    monkeypatch.setattr(
        "research.providers.akshare_futures.AkshareFuturesMarketDataProvider.fetch_daily_bars",
        fake_fetch_daily_bars,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=["CNF.CU.SHFE.main"],
        start_date="2024-06-01",
        end_date="2024-06-04",
    )

    assert result["status"] == "success"
    assert requested_dates == [("2024-06-03", "2024-06-03"), ("2024-06-04", "2024-06-04")]
    assert result["totals"]["calendar_skipped"] == 2
    assert result["trading_day_governance"]["skipped_dates_by_exchange"]["SHFE"] == [
        "2024-06-01",
        "2024-06-02",
    ]


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

    async def fake_official_fetch(self, exchange, trade_date, *, mode="direct"):
        return [_official_contract_row(exchange=exchange, trade_date=trade_date)]

    async def unexpected_fallback(self, series, *, start_date=None, end_date=None, mode="direct"):
        raise AssertionError("fallback provider should not be called when official source succeeds")

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_exchange_contract_bars",
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
    assert result["official_fanout"]["exchange_payload_requests"] == 1
    assert result["official_fanout"]["series_artifacts_built"] == 1
    assert rows[0]["source_profile"] == "exchange_official"
    assert storage.get_contract("CNF.CU.SHFE.CU2407")
    assert storage.get_contract_price_bars("CNF.CU.SHFE.CU2407")[0]["close"] == 11
    assert storage.list_continuous_mappings("CNF.CU.SHFE.main")[0]["contract_id"] == "CNF.CU.SHFE.CU2407"


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

    async def failed_official_fetch(self, exchange, trade_date, *, mode="direct"):
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
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_exchange_contract_bars",
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
    assert result["official_fanout"]["exchange_payload_requests"] == 1
    assert result["official_fanout"]["fallback_attempts"] == 1
    assert result["series"][0]["official_status"] == "unavailable"
    assert rows[0]["source_profile"] == "akshare_futures"


@pytest.mark.asyncio
async def test_futures_market_data_sync_fans_out_one_exchange_payload(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "preferred_order": ["exchange_official", "akshare_futures"],
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"], "timeout_seconds": 1},
        "akshare_futures": {"enabled": False},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    calls = []

    async def fake_official_fetch(self, exchange, trade_date, *, mode="direct"):
        calls.append((exchange, trade_date))
        return [
            _official_contract_row(exchange=exchange, trade_date=trade_date, variety="CU", contract="CU2407", close=11),
            _official_contract_row(exchange=exchange, trade_date=trade_date, variety="AL", contract="AL2407", close=21),
        ]

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_exchange_contract_bars",
        fake_official_fetch,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=["CNF.CU.SHFE.main", "CNF.AL.SHFE.main"],
        start_date="2024-06-03",
        end_date="2024-06-03",
    )

    assert result["status"] == "success"
    assert calls == [("SHFE", "2024-06-03")]
    assert result["official_fanout"]["exchange_payload_requests"] == 1
    assert result["official_fanout"]["exchange_payload_cache_hits"] == 1
    assert result["official_fanout"]["series_artifacts_built"] == 2
    assert storage.get_price_bars("CNF.CU.SHFE.main")[0]["close"] == 11
    assert storage.get_price_bars("CNF.AL.SHFE.main")[0]["close"] == 21


@pytest.mark.asyncio
async def test_futures_market_data_sync_uses_promoted_discovery_series(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"].update(_scope_module_cfg())
    config.modules["commodity_market_data"]["sources"] = {
        "preferred_order": ["exchange_official", "akshare_futures"],
        "exchange_official": {"enabled": True, "enabled_exchanges": ["GFEX"], "timeout_seconds": 1},
        "akshare_futures": {"enabled": False},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()
    storage.upsert_master_discoveries([
        FuturesMasterDiscoveryCandidate(
            discovery_id="GFEX:PT",
            exchange="GFEX",
            variety_symbol="PT",
            candidate_instrument_id="CNF.PT.GFEX",
            candidate_series_id="CNF.PT.GFEX.main",
            candidate_name="GFEX Platinum",
            candidate_category="precious_metal",
            candidate_currency="CNY",
            candidate_unit="CNY/gram",
            first_seen_trade_date="2026-01-02",
            last_seen_trade_date="2026-01-02",
            observed_contracts=["PT2606"],
            confidence_score=0.95,
            quality_flag="discovered_verified",
            review_status="none",
        )
    ])
    assert storage.promote_master_discovery("GFEX:PT")["status"] == "success"
    storage.upsert_trading_calendar([
        FuturesTradingCalendarDay(
            exchange="GFEX",
            trade_date="2026-01-02",
            is_trading_day=True,
            source_profile="exchange_official_daily_probe",
            quality_flag="backfilled_verified",
        )
    ])

    async def fake_official_fetch(self, exchange, trade_date, *, mode="direct"):
        return [
            _official_contract_row(
                exchange=exchange,
                trade_date=trade_date,
                variety="PT",
                contract="PT2606",
                close=280.0,
            )
        ]

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_exchange_contract_bars",
        fake_official_fetch,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=["CNF.PT.GFEX.main"],
        start_date="2026-01-02",
        end_date="2026-01-02",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["scope_selection"]["series_ids"] == ["CNF.PT.GFEX.main"]
    assert result["series"][0]["series_id"] == "CNF.PT.GFEX.main"
    assert result["totals"]["would_write_price_bars"] == 1


@pytest.mark.asyncio
async def test_futures_market_data_sync_falls_back_after_official_empty(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "preferred_order": ["exchange_official", "akshare_futures"],
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"], "timeout_seconds": 1},
        "akshare_futures": {"enabled": True, "timeout_seconds": 1},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    async def fake_official_fetch(self, exchange, trade_date, *, mode="direct"):
        return [_official_contract_row(exchange=exchange, trade_date=trade_date, variety="CU", contract="CU2407")]

    async def fake_fallback_fetch(self, series, *, start_date=None, end_date=None, mode="direct"):
        return [
            FuturesBar(
                series_id=series.series_id,
                trade_date=start_date,
                open=20,
                high=22,
                low=19,
                close=21,
                raw_payload_hash=f"akshare:{series.series_id}:{start_date}",
                source="akshare",
                source_mode=mode,
                source_profile="akshare_futures",
                source_interface="futures_zh_daily_sina",
            )
        ]

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_exchange_contract_bars",
        fake_official_fetch,
    )
    monkeypatch.setattr(
        "research.providers.akshare_futures.AkshareFuturesMarketDataProvider.fetch_daily_bars",
        fake_fallback_fetch,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=["CNF.AL.SHFE.main"],
        start_date="2024-06-03",
        end_date="2024-06-03",
    )

    assert result["status"] == "success"
    assert result["series"][0]["official_status"] == "failed"
    assert result["series"][0]["date_results"][0]["official_status"] == "empty"
    assert result["source_selection"]["fallback_success"] == 1
    assert result["official_fanout"]["official_empty"] == 1
    assert result["official_fanout"]["fallback_attempts"] == 1
    assert storage.get_price_bars("CNF.AL.SHFE.main")[0]["source_profile"] == "akshare_futures"


@pytest.mark.asyncio
async def test_futures_market_data_sync_dry_run_reports_would_write_rows(monkeypatch, tmp_path):
    config = _research_config(tmp_path)
    config.modules["commodity_market_data"]["sources"] = {
        "preferred_order": ["exchange_official", "akshare_futures"],
        "exchange_official": {"enabled": True, "enabled_exchanges": ["SHFE"], "timeout_seconds": 1},
        "akshare_futures": {"enabled": False},
    }
    storage = FuturesStorageManager(config)
    storage.initialize()

    async def fake_official_fetch(self, exchange, trade_date, *, mode="direct"):
        return [_official_contract_row(exchange=exchange, trade_date=trade_date)]

    monkeypatch.setattr(
        "research.providers.official_futures.OfficialFuturesMarketDataProvider.fetch_exchange_contract_bars",
        fake_official_fetch,
    )

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=["CNF.CU.SHFE.main"],
        start_date="2024-06-03",
        end_date="2024-06-03",
        dry_run=True,
    )

    assert result["status"] == "success"
    assert result["totals"]["inserted"] == 0
    assert result["totals"]["changed"] == 0
    assert result["totals"]["unchanged"] == 0
    assert result["totals"]["would_write_price_bars"] == 1
    assert result["series"][0]["write_result"] == {
        "inserted": 0,
        "changed": 0,
        "unchanged": 0,
        "would_write_rows": 1,
    }
    assert storage.get_price_bars("CNF.CU.SHFE.main") == []


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
