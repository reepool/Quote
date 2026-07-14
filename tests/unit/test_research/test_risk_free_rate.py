"""Tests for the risk-free rate series (REQ-13).

Change: add-quote-api-data-capability-improvements.
- Storage round-trip via bound methods on an in-memory sqlite stub.
- Sync service with an injected fetcher (no network).
- Route with a mocked data_manager.
"""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from research.storage import ResearchStorageManager
from research.risk_free_rate_sync import RiskFreeRateSyncService
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


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
            row_hash TEXT, row_version INTEGER NOT NULL DEFAULT 1,
            ingestion_run_id INTEGER, created_at TEXT, updated_at TEXT
        );
        CREATE TABLE risk_free_rate_observations (
            series_id TEXT, observation_date TEXT, source_profile TEXT DEFAULT 'default',
            value REAL, revision_id TEXT DEFAULT 'latest', source TEXT, source_mode TEXT,
            data_as_of TEXT, row_hash TEXT, row_version INTEGER NOT NULL DEFAULT 1,
            ingestion_run_id INTEGER, created_at TEXT, updated_at TEXT,
            PRIMARY KEY (series_id, observation_date, source_profile, revision_id)
        );
        CREATE TABLE data_change_log (
            sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL, dataset TEXT NOT NULL, change_type TEXT NOT NULL,
            business_key_json TEXT NOT NULL, instrument_id TEXT, series_id TEXT,
            observation_date TEXT, period TEXT, old_hash TEXT, new_hash TEXT,
            row_version INTEGER, source TEXT, source_mode TEXT, source_profile TEXT,
            ingestion_run_id TEXT, batch_id TEXT, changed_at TEXT NOT NULL
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


def _build_routed_storage(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()

    sentinels = (
        (storage.financials_db_path, "financial", 9.91),
        (storage.valuation_db_path, "valuation", 8.81),
        (storage.interests_db_path, "interests", 1.74),
    )
    for db_path, label, value in sentinels:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO risk_free_rate_series (
                    series_id, name, rate_type, source, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "china_treasury_10y",
                    label,
                    "china_treasury_yield",
                    "unit",
                    "2026-07-14T00:00:00+08:00",
                    "2026-07-14T00:00:00+08:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO risk_free_rate_observations (
                    series_id, observation_date, value, source, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "china_treasury_10y",
                    "2026-07-13",
                    value,
                    "unit",
                    "2026-07-14T00:00:00+08:00",
                    "2026-07-14T00:00:00+08:00",
                ),
            )
            conn.commit()
    return storage


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
    assert "row_hash" not in series[0]
    assert "row_version" not in series[0]

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
    stub.upsert_risk_free_rate_observations("s1", [{"observation_date": "2024-01-02", "value": 2.5}])
    stub.upsert_risk_free_rate_observations("s1", [{"observation_date": "2024-01-02", "value": 2.7}])
    obs = stub.get_risk_free_rate_observations("s1")
    assert len(obs) == 1 and obs[0]["value"] == 2.7
    changes = stub._conn.execute(
        """
        SELECT dataset, series_id, observation_date, row_version
        FROM data_change_log
        WHERE dataset = 'risk_free_rate_observations'
        ORDER BY sequence_id
        """
    ).fetchall()
    assert [tuple(row) for row in changes] == [
        ("risk_free_rate_observations", "s1", "2024-01-02", 1),
        ("risk_free_rate_observations", "s1", "2024-01-02", 2),
    ]


def test_database_route_state_is_isolated_between_overlapping_threads(tmp_path):
    storage = _build_routed_storage(tmp_path)
    first_entered = Event()
    second_entered = Event()
    first_exited = Event()

    def first_scope():
        with storage.financial_database_scope():
            first_entered.set()
            assert second_entered.wait(timeout=5)
        first_exited.set()
        return storage.active_storage_db_path()

    def second_scope():
        assert first_entered.wait(timeout=5)
        with storage.financial_database_scope():
            second_entered.set()
            assert first_exited.wait(timeout=5)
        return storage.active_storage_db_path()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_scope)
        second_future = executor.submit(second_scope)
        assert first_future.result(timeout=10) == storage.db_path
        assert second_future.result(timeout=10) == storage.db_path

    assert storage.active_storage_db_path() == storage.db_path


@pytest.mark.parametrize(
    ("scope_name", "scoped_db_attr"),
    (
        ("financial_database_scope", "financials_db_path"),
        ("valuation_database_scope", "valuation_db_path"),
    ),
)
def test_interest_reads_ignore_other_thread_database_scope(
    tmp_path,
    scope_name,
    scoped_db_attr,
):
    storage = _build_routed_storage(tmp_path)
    entered = Event()
    release = Event()

    def hold_other_domain_scope():
        with getattr(storage, scope_name)():
            assert storage.active_storage_db_path() == getattr(storage, scoped_db_attr)
            entered.set()
            assert release.wait(timeout=5)
        return storage.active_storage_db_path()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(hold_other_domain_scope)
        assert entered.wait(timeout=5)
        try:
            series = storage.list_risk_free_rate_series()
            observations = storage.get_risk_free_rate_observations(
                "china_treasury_10y"
            )
        finally:
            release.set()
        assert future.result(timeout=10) == storage.db_path

    assert series[0]["name"] == "interests"
    assert observations[0]["value"] == pytest.approx(1.74)
    assert storage.active_storage_db_path() == storage.db_path


def test_nested_and_failing_database_scopes_restore_previous_route(tmp_path):
    storage = _build_routed_storage(tmp_path)

    assert storage.active_storage_db_path() == storage.db_path
    with storage.financial_database_scope():
        assert storage.active_storage_db_path() == storage.financials_db_path
        with storage.valuation_database_scope():
            assert storage.active_storage_db_path() == storage.valuation_db_path
            with storage.interests_database_scope():
                assert storage.active_storage_db_path() == storage.interests_db_path
            assert storage.active_storage_db_path() == storage.valuation_db_path
        assert storage.active_storage_db_path() == storage.financials_db_path
    assert storage.active_storage_db_path() == storage.db_path

    with pytest.raises(RuntimeError, match="route failure"):
        with storage.financial_database_scope():
            raise RuntimeError("route failure")
    assert storage.active_storage_db_path() == storage.db_path


def test_missing_series_does_not_hide_configured_interests_data(tmp_path):
    storage = _build_routed_storage(tmp_path)

    assert storage.get_risk_free_rate_observations("missing") == []
    assert [item["name"] for item in storage.list_risk_free_rate_series()] == [
        "interests"
    ]


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
