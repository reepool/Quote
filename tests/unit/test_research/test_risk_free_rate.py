"""Tests for the risk-free rate series (REQ-13).

Change: add-quote-api-data-capability-improvements.
- Storage round-trip via bound methods on an in-memory sqlite stub.
- Sync service with an injected fetcher (no network).
- Route with a mocked data_manager.
"""

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from research.storage import ResearchStorageManager
from research.risk_free_rate_sync import RiskFreeRateSyncService


class _StorageStub:
    def __init__(self, conn):
        self._conn = conn
        # non-None so the interests-db routing wrapper calls through to the raw
        # method and uses this stub's get_connection instead of opening a file.
        self._active_db_path = ":memory:"

    @contextmanager
    def get_connection(self):
        yield self._conn

    def _apply_pragmas(self, conn):
        pass


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE risk_free_rate_series (
            series_id TEXT PRIMARY KEY, name TEXT, rate_type TEXT, tenor TEXT,
            currency TEXT, unit TEXT, frequency TEXT, timezone TEXT,
            source_profile TEXT, source TEXT, source_mode TEXT, data_as_of TEXT,
            ingestion_run_id INTEGER, created_at TEXT, updated_at TEXT
        );
        CREATE TABLE risk_free_rate_observations (
            series_id TEXT, observation_date TEXT, source_profile TEXT DEFAULT 'default',
            value REAL, revision_id TEXT DEFAULT 'latest', source TEXT, source_mode TEXT,
            data_as_of TEXT, ingestion_run_id INTEGER, created_at TEXT, updated_at TEXT,
            PRIMARY KEY (series_id, observation_date, source_profile, revision_id)
        );
        """
    )
    conn.commit()
    return conn


def _bind(stub):
    stub.upsert_risk_free_rate_series = ResearchStorageManager.upsert_risk_free_rate_series.__get__(stub)
    stub.upsert_risk_free_rate_observations = ResearchStorageManager.upsert_risk_free_rate_observations.__get__(stub)
    stub.get_risk_free_rate_observations = ResearchStorageManager.get_risk_free_rate_observations.__get__(stub)
    stub.list_risk_free_rate_series = ResearchStorageManager.list_risk_free_rate_series.__get__(stub)
    return stub


def test_storage_roundtrip_and_date_filter():
    stub = _bind(_StorageStub(_make_conn()))
    stub.upsert_risk_free_rate_series({
        "series_id": "china_treasury_10y", "name": "CN10Y", "rate_type": "china_treasury_yield",
    })
    n = stub.upsert_risk_free_rate_observations("china_treasury_10y", [
        {"observation_date": "2024-01-02", "value": 2.55},
        {"observation_date": "2024-02-01", "value": 2.44},
        {"observation_date": "2024-03-01", "value": 2.31},
    ])
    assert n == 3

    series = stub.list_risk_free_rate_series()
    assert series[0]["series_id"] == "china_treasury_10y"

    all_obs = stub.get_risk_free_rate_observations("china_treasury_10y")
    assert [o["observation_date"] for o in all_obs] == ["2024-01-02", "2024-02-01", "2024-03-01"]

    windowed = stub.get_risk_free_rate_observations(
        "china_treasury_10y", start_date="2024-01-15", end_date="2024-02-15"
    )
    assert [o["value"] for o in windowed] == [2.44]


def test_storage_empty_when_no_data():
    stub = _bind(_StorageStub(_make_conn()))
    assert stub.get_risk_free_rate_observations("missing") == []
    assert stub.list_risk_free_rate_series() == []


def test_storage_upsert_is_idempotent():
    stub = _bind(_StorageStub(_make_conn()))
    stub.upsert_risk_free_rate_series({"series_id": "s1"})
    stub.upsert_risk_free_rate_observations("s1", [{"observation_date": "2024-01-02", "value": 2.5}])
    stub.upsert_risk_free_rate_observations("s1", [{"observation_date": "2024-01-02", "value": 2.7}])
    obs = stub.get_risk_free_rate_observations("s1")
    assert len(obs) == 1 and obs[0]["value"] == 2.7


def test_sync_service_writes_fetched_observations():
    stub = _bind(_StorageStub(_make_conn()))
    fetcher = lambda: [
        {"observation_date": "2024-01-02", "value": 2.55},
        {"observation_date": "2024-01-03", "value": 2.56},
    ]
    service = RiskFreeRateSyncService(stub, fetcher=fetcher)
    result = service.sync(data_as_of="2024-01-03")
    assert result["written"] == 2 and result["status"] == "ok"
    assert len(stub.get_risk_free_rate_observations("china_treasury_10y")) == 2


def test_sync_service_empty_source_degrades():
    stub = _bind(_StorageStub(_make_conn()))
    service = RiskFreeRateSyncService(stub, fetcher=lambda: [])
    result = service.sync()
    assert result["status"] == "empty" and result["written"] == 0


@pytest.mark.asyncio
async def test_route_returns_series(monkeypatch):
    import api.routes as routes
    from datetime import date as _date

    payload = {
        "series_id": "china_treasury_10y",
        "series": {"series_id": "china_treasury_10y", "rate_type": "china_treasury_yield"},
        "observations": [{"observation_date": "2024-01-02", "value": 2.55, "revision_id": "latest"}],
        "total": 1,
    }
    mgr = SimpleNamespace(get_research_risk_free_rate=AsyncMock(return_value=payload))
    monkeypatch.setattr(routes, "data_manager", mgr)

    resp = await routes.get_research_risk_free_rate(
        series_id="china_treasury_10y", start_date=_date(2024, 1, 1), end_date=_date(2024, 12, 31)
    )
    assert resp.total == 1
    assert resp.observations[0].value == 2.55


@pytest.mark.asyncio
async def test_route_empty_series_not_error(monkeypatch):
    import api.routes as routes

    payload = {"series_id": "x", "series": None, "observations": [], "total": 0}
    mgr = SimpleNamespace(get_research_risk_free_rate=AsyncMock(return_value=payload))
    monkeypatch.setattr(routes, "data_manager", mgr)

    resp = await routes.get_research_risk_free_rate(series_id="x", start_date=None, end_date=None)
    assert resp.total == 0 and resp.observations == []
