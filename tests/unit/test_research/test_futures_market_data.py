from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta
import pandas as pd
import pytest

from research.futures_market_data import (
    FuturesBar,
    FuturesCalendarService,
    FuturesCalendarNotice,
    FuturesContinuousMapping,
    FuturesContract,
    FuturesContractBar,
    FuturesDiagnosticsService,
    FuturesExposureMapping,
    FuturesInstrumentCalendarOverride,
    FuturesManualCalendarReview,
    FuturesMarketDataSyncService,
    FuturesOfficialCalendarBackfillService,
    FuturesReadinessService,
    FuturesSeries,
    FuturesStorageManager,
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
    assert gfex.categories == ["new_energy_material"]
    assert set(gfex.instrument_ids) == {"CNF.LC.GFEX", "CNF.PS.GFEX", "CNF.SI.GFEX"}


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
        "official_calendar_backfill": {"start_date": "2010-01-01"},
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
        raw_payload={"contract": contract},
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
