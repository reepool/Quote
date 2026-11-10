"""Business arithmetic, SQL projections and page contracts over small fixtures."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from data_manager import DataManager
from research.financial_read_projection import (
    CORE_FACTS,
    INDICATORS,
    METADATA,
)
from research.risk_free_rate_sync import RiskFreeRateSyncService
from research.storage import ResearchStorageManager
from research.valuation_service import ResearchValuationService
from tests.unit.test_api.test_data_consumption_contract import get
from tests.unit.test_research.test_risk_free_rate import _bind, _make_conn, _StorageStub


class MemoryFinancialStorage:
    _active_db_path = ":memory:"
    _deserialize_json = staticmethod(json.loads)
    get_financial_statement_bundle = (
        ResearchStorageManager.get_financial_statement_bundle
    )
    get_financial_statement_bundles = (
        ResearchStorageManager.get_financial_statement_bundles
    )

    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.queries = []
        self.conn.set_trace_callback(self.queries.append)
        facts = list(
            dict.fromkeys(
                (
                    *METADATA,
                    *CORE_FACTS,
                    "report_type",
                    "statement_family",
                    "source_file_id",
                    "filing_id",
                    "equity_total",
                    "minority_equity",
                    "facts_json",
                    "lineage_json",
                )
            )
        )
        indicators = list(
            dict.fromkeys(
                (
                    "instrument_id",
                    "report_period",
                    *INDICATORS,
                    "indicators_json",
                    "source",
                    "source_mode",
                    "data_as_of",
                    "ingestion_run_id",
                    "created_at",
                    "updated_at",
                )
            )
        )
        statements = list(
            dict.fromkeys((*METADATA, "statement_type", "statement_json"))
        )
        for table, fields in (
            ("financial_facts", facts),
            ("financial_indicator_snapshots", indicators),
            ("financial_statements_raw", statements),
        ):
            self.conn.execute(f"CREATE TABLE {table} ({','.join(fields)})")
        metadata = {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
            "report_period": "2026-09-30",
            "currency": "CNY",
            "schema_version": "test.v1",
            "source": "fixture",
            "source_mode": "local",
            "data_as_of": "2026-10-31T12:00:00+08:00",
            "created_at": "2026-10-31T12:00:00+08:00",
            "updated_at": "2026-10-31T12:00:00+08:00",
        }
        large = json.dumps({"raw": "x" * 90000})
        self.insert(
            "financial_facts",
            {
                **metadata,
                "revenue": 1000.0,
                "net_income": 200.0,
                "facts_json": large,
                "lineage_json": "{}",
            },
        )
        self.insert(
            "financial_indicator_snapshots",
            {
                **{key: value for key, value in metadata.items() if key in indicators},
                "gross_margin": 0.8,
                "roe": 0.2,
                "current_ratio": 2.0,
                "indicators_json": large,
            },
        )
        self.insert(
            "financial_statements_raw",
            {**metadata, "statement_type": "income", "statement_json": large},
        )
        self.conn.commit()

    def insert(self, table, row):
        self.conn.execute(
            f"INSERT INTO {table} ({','.join(row)}) VALUES ({','.join('?' for _ in row)})",
            list(row.values()),
        )

    @contextmanager
    def get_connection(self):
        yield self.conn

    def _apply_pragmas(self, connection):
        pass


def financial_manager(storage):
    manager = SimpleNamespace(
        _require_research_storage=lambda: storage,
        research_config=SimpleNamespace(
            modules={"financial_statements": {"enabled": True}}
        ),
        _get_research_financial_statement_service_layers=AsyncMock(return_value={}),
        _get_research_instrument_info=AsyncMock(
            return_value={
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
            }
        ),
        _run_financial_storage_call=DataManager._run_financial_storage_call,
    )
    manager.get_research_financial_statements = (
        DataManager.get_research_financial_statements.__get__(manager)
    )
    manager.get_research_financial_statements_history = (
        DataManager.get_research_financial_statements_history.__get__(manager)
    )
    return manager


@pytest.mark.asyncio
async def test_financial_http_summary_fields_full_history_and_flags(monkeypatch):
    from fastapi import FastAPI

    from api import routes

    storage = MemoryFinancialStorage()
    manager = financial_manager(storage)
    monkeypatch.setattr(routes, "data_manager", manager)
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    path = "/research/company/600519.SH/financial-statements"
    storage.queries.clear()
    response = await get(app, path)
    assert response.status_code == 200, response.text
    assert len(response.content) < 8192
    assert response.json()["view"] == "summary"
    assert response.json()["ratios"]["gross_margin"] == 0.8
    assert response.json()["units"]["indicators.current_ratio"] == "multiple"
    assert all(
        "facts_json" not in query
        and "indicators_json" not in query
        and "statement_json" not in query
        for query in storage.queries
    )
    assert (
        "facts" not in response.json()
        and "details" not in response.json()["indicators"]
    )
    projected = await get(app, path, fields="revenue,net_income", view="full")
    assert projected.status_code == 200, projected.text
    assert projected.json()["revenue"] == 1000 and projected.json()["net_income"] == 200
    assert (
        "gross_profit" not in projected.json() and "indicators" not in projected.json()
    )
    full = await get(app, path, view="full")
    assert full.status_code == 200, full.text
    assert full.json()["facts"]["raw"].startswith("xx")
    assert len(full.json()["statements"]) == 1
    history = await get(app, path + "/history", view="summary", fields="revenue")
    assert history.status_code == 200, history.text
    assert history.json()["items"][0]["revenue"] == 1000
    assert "net_income" not in history.json()["items"][0]
    for params in (
        {"fields": "nonsense"},
        {"include_snapshot": "false"},
        {"include_statements": "true"},
        {"view": "full", "fields": "revenue", "include_statements": "true"},
    ):
        rejected = await get(app, path, **params)
        assert rejected.status_code == 422, rejected.text
        assert rejected.json()["error_code"] in {
            "invalid_query_parameter",
            "unknown_query_parameter",
        }
    known = await get(app, path, include_local_core=False)
    assert known.status_code == 200
    schema = app.openapi()
    assert (
        "anyOf"
        in schema["paths"][
            "/api/v1/research/company/{instrument_id}/financial-statements"
        ]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    )
    storage.conn.close()


def make_rate_storage():
    conn = _make_conn()
    conn.execute("DROP TABLE risk_free_rate_observations")
    conn.execute("""CREATE TABLE risk_free_rate_observations (
        series_id TEXT REFERENCES risk_free_rate_series(series_id), observation_date TEXT,
        source_profile TEXT DEFAULT 'default', value REAL, revision_id TEXT DEFAULT 'latest',
        source TEXT, source_mode TEXT, data_as_of TEXT, row_hash TEXT, row_version INTEGER DEFAULT 1,
        ingestion_run_id INTEGER, created_at TEXT, updated_at TEXT,
        PRIMARY KEY(series_id, observation_date, source_profile, revision_id))""")
    conn.execute("PRAGMA foreign_keys=ON")
    stub = _bind(_StorageStub(conn))
    stub.get_risk_free_rate_page = (
        ResearchStorageManager.get_risk_free_rate_page.__get__(stub)
    )
    return stub


def test_rate_first_sync_page_identity_unknown_history_and_failed_fetch():
    storage = make_rate_storage()
    rows = [
        {"observation_date": "2026-11-06", "value": 1.6, "source_profile": "a"},
        {"observation_date": "2026-11-06", "value": 1.7, "source_profile": "b"},
        {"observation_date": "2002-01-04", "value": 3.2},
    ]
    service = RiskFreeRateSyncService(storage, fetcher=lambda: rows)
    assert (
        service.sync()["written"] == 3
    )  # foreign key is enforced on an initially empty DB
    page = storage.get_risk_free_rate_page("china_treasury_10y", limit=1)
    assert page["observations"][0]["source_profile"] == "a"
    assert page["total_available"] == 3 and page["next_offset"] == 1
    assert (
        storage.get_risk_free_rate_page("china_treasury_10y", limit=1, offset=1)[
            "observations"
        ][0]["source_profile"]
        == "b"
    )
    assert (
        storage.get_risk_free_rate_page("china_treasury_10y", order="asc")[
            "observations"
        ][0]["observation_date"]
        == "2002-01-04"
    )
    before = storage.list_risk_free_rate_series()[0]["data_as_of"]
    assert datetime.fromisoformat(before).tzinfo is not None
    assert RiskFreeRateSyncService(storage, fetcher=list).sync()["status"] == "empty"
    assert storage.list_risk_free_rate_series()[0]["data_as_of"] == before
    storage.upsert_risk_free_rate_observations(
        "china_treasury_10y", [{"observation_date": "2000-01-01", "value": 2.0}]
    )
    old = storage.get_risk_free_rate_page("china_treasury_10y", end_date="2000-12-31")
    assert old["observations"][0]["data_as_of"] is None
    assert old["observations"][0]["data_as_of_quality"] == "unknown_historical"
    assert old["series"]["last_observation_date"] == "2026-11-06"
    assert len(storage.get_risk_free_rate_observations("china_treasury_10y")) == 4
    final = storage.get_risk_free_rate_page("china_treasury_10y", limit=2, offset=2)
    assert final["truncated"] is False and final["next_offset"] is None
    bounded = storage.get_risk_free_rate_page(
        "china_treasury_10y", start_date="2026-11-06", end_date="2026-11-06"
    )
    assert bounded["total_available"] == 2
    assert bounded["first_date"] == bounded["last_date"] == "2026-11-06"
    empty = storage.get_risk_free_rate_page(
        "china_treasury_10y", start_date="2030-01-01"
    )
    assert empty["observations"] == [] and empty["total_available"] == 0
    assert empty["first_date"] is empty["last_date"] is empty["next_offset"] is None
    storage._conn.close()


def test_rate_failed_write_does_not_advance_series_time(monkeypatch):
    storage = make_rate_storage()
    storage.upsert_risk_free_rate_series(
        {"series_id": "china_treasury_10y", "data_as_of": "2026-11-01T12:00:00+08:00"}
    )

    def fail(*args):
        raise sqlite3.OperationalError("fixture write failure")

    monkeypatch.setattr(storage, "upsert_risk_free_rate_observations", fail)
    with pytest.raises(sqlite3.OperationalError):
        RiskFreeRateSyncService(
            storage, fetcher=lambda: [{"observation_date": "2026-11-06", "value": 1.6}]
        ).sync()
    assert (
        storage.list_risk_free_rate_series()[0]["data_as_of"]
        == "2026-11-01T12:00:00+08:00"
    )
    storage._conn.close()


def test_rate_partial_write_and_fetch_failure_preserve_success_time(monkeypatch):
    from research import risk_free_rate_sync

    storage = make_rate_storage()
    series_id = "china_treasury_10y"
    previous = "2026-11-01T12:00:00+08:00"
    fetched = "2026-11-09T16:00:00+08:00"
    storage.upsert_risk_free_rate_series(
        {"series_id": series_id, "data_as_of": previous}
    )
    monkeypatch.setattr(risk_free_rate_sync, "_WRITE_CHUNK_SIZE", 1)
    writer = storage.upsert_risk_free_rate_observations
    calls = 0

    def partial_write(series, rows):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise sqlite3.OperationalError("fixture second-chunk failure")
        return writer(series, rows)

    monkeypatch.setattr(storage, "upsert_risk_free_rate_observations", partial_write)
    with pytest.raises(sqlite3.OperationalError):
        RiskFreeRateSyncService(
            storage,
            fetcher=lambda: [
                {"observation_date": "2026-11-05", "value": 1.6},
                {"observation_date": "2026-11-06", "value": 1.7},
            ],
        ).sync(data_as_of=fetched)
    assert storage.list_risk_free_rate_series()[0]["data_as_of"] == previous
    rows = storage.get_risk_free_rate_observations(series_id)
    assert len(rows) == 1 and rows[0]["data_as_of"] == fetched

    def failed_fetch():
        raise ConnectionError("fixture fetch failure")

    with pytest.raises(ConnectionError):
        RiskFreeRateSyncService(storage, fetcher=failed_fetch).sync()
    assert storage.list_risk_free_rate_series()[0]["data_as_of"] == previous
    storage._conn.close()


def test_relative_clean_multiples_and_optional_policy():
    instrument = {"instrument_id": "600519.SH", "symbol": "600519", "exchange": "SSE"}
    membership = {
        "mapping_status": "authoritative",
        "sw_l2_code": "industry",
        "sw_l2_name": "industry",
    }
    peers = [
        {"instrument_id": str(i), "pe_ttm": value}
        for i, value in enumerate([-601.89, 20.0, 22.0, 921.98, float("nan")])
    ]

    def run(maximum=None):
        config = {"min_peer_count": 1}
        if maximum is not None:
            config["max_pe"] = maximum
        return ResearchValuationService({"relative": config}).build_relative_valuation(
            instrument=instrument,
            subject_row={**instrument, "pe_ttm": 21.0},
            industry_membership=membership,
            peer_rows=peers,
        )

    raw, filtered = run(), run(100.0)
    assert raw["benchmark_summary"]["pe_ttm"]["excluded_peer_count"] == 2
    stats = filtered["benchmark_summary"]["pe_ttm"]
    assert stats["peer_mean"] == stats["peer_median"] == 21.0
    assert stats["raw_peer_count"] == 4 and stats["valid_peer_count"] == 2
    assert raw["parameter_hash"] != filtered["parameter_hash"]
    assert any(
        item["reason"] == "above_maximum"
        for item in filtered["diagnostics"]["metric_exclusions"]["pe_ttm"]
    )


def test_relative_checks_available_denominators_and_subject_validity():
    service = ResearchValuationService()
    peers = [
        {"instrument_id": "unknown", "pe_ttm": 20.0},
        {
            "instrument_id": "valid",
            "pe_ttm": 22.0,
            "details": {"metrics": {"pe_ttm": {"denominator": 10.0}}},
        },
        *[
            {
                "instrument_id": str(value),
                "pe_ttm": 21.0,
                "details": {"metrics": {"pe_ttm": {"denominator": value}}},
            }
            for value in (-10.0, 0.0)
        ],
    ]
    result, exclusions = service._build_metric_benchmark(
        subject_row=peers[-1], peer_rows=peers, field_name="pe_ttm"
    )
    assert result["valid_peer_count"] == 2
    assert result["peer_mean"] == result["peer_median"] == 21.0
    assert result["raw_peer_count"] == 4
    assert [item["reason"] for item in exclusions] == ["invalid_denominator"] * 2
    assert result["percentile_rank"] is result["premium_to_median"] is None
    assert result["subject_missing_reason"] == "invalid_denominator"
    assert result["policy"]["denominator_check"] == "when_metadata_available"
    capped, _ = ResearchValuationService(
        {"relative": {"max_pe": 10}}
    )._build_metric_benchmark(
        subject_row=peers[0], peer_rows=peers, field_name="pe_ttm"
    )
    assert capped["percentile_rank"] is capped["premium_to_median"] is None
    assert capped["subject_missing_reason"] == "above_maximum"


@pytest.mark.asyncio
async def test_rate_page_metadata_survives_http_models(monkeypatch):
    from fastapi import FastAPI

    from api import routes

    storage = make_rate_storage()
    storage.upsert_risk_free_rate_series({"series_id": "china_treasury_10y"})
    storage.upsert_risk_free_rate_observations(
        "china_treasury_10y",
        [{"observation_date": "2026-11-06", "value": 1.6, "source_profile": "fixture"}],
    )
    page = storage.get_risk_free_rate_page("china_treasury_10y", limit=2)
    manager = SimpleNamespace(get_research_risk_free_rate=AsyncMock(return_value=page))
    monkeypatch.setattr(routes, "data_manager", manager)
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    response = await get(
        app, "/research/risk-free-rate", series_id="china_treasury_10y", limit=2
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["total_available"] == 1 and result["truncated"] is False
    assert result["first_date"] == result["last_date"] == "2026-11-06"
    assert result["series"]["last_observation_date"] == "2026-11-06"
    assert result["observations"][0]["source_profile"] == "fixture"
    assert result["observations"][0]["data_as_of_quality"] == "unknown_historical"
    assert result["observations"][0]["revision_kind"] == "overwrite_slot"
    manager.get_research_risk_free_rate.assert_awaited_once_with(
        "china_treasury_10y",
        start_date=None,
        end_date=None,
        order="desc",
        limit=2,
        offset=0,
    )
    storage._conn.close()
