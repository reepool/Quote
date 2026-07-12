"""Tests for time-point industry membership (REQ-07.2).

Change: add-quote-api-data-capability-improvements.
- Unit-tests the SQL/interval selection in
  ResearchStorageManager.get_industry_membership_as_of against an in-memory sqlite,
  by binding the unbound method onto a lightweight stub (avoids heavy storage init).
- Route-level test uses a mocked data_manager (test_research_routes pattern).
"""

import json
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from research.storage import ResearchStorageManager


class _StorageStub:
    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def get_connection(self):
        yield self._conn

    def _apply_pragmas(self, conn):  # noqa: D401 - test stub
        pass

    def _deserialize_json(self, value):
        if not value:
            return None
        return json.loads(value)


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE industry_classification_history (
            row_hash TEXT PRIMARY KEY,
            instrument_id TEXT, symbol TEXT, exchange TEXT,
            taxonomy_system TEXT, taxonomy_version TEXT,
            official_industry_code TEXT, official_start_date TEXT,
            official_update_time TEXT, classification_json TEXT,
            source TEXT, source_mode TEXT, created_at TEXT, updated_at TEXT
        )
        """
    )
    rows = [
        ("h1", "600000.SH", "600000", "SSE", "sw", "2021", "A", "2020-01-01",
         "2020-01-05", json.dumps({"industry_name": "银行"}), "sw", "official",
         "2020-01-05", "2020-01-05"),
        ("h2", "600000.SH", "600000", "SSE", "sw", "2021", "B", "2022-06-01",
         "2022-06-03", json.dumps({"industry_name": "非银金融"}), "sw", "official",
         "2022-06-03", "2022-06-03"),
    ]
    conn.executemany(
        "INSERT INTO industry_classification_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn


def _call_as_of(as_of):
    stub = _StorageStub(_make_conn())
    method = ResearchStorageManager.get_industry_membership_as_of
    return method(stub, "600000.SH", as_of)


def test_as_of_picks_earlier_interval_with_expiry():
    res = _call_as_of("2021-03-15")
    assert res is not None
    assert res["official_industry_code"] == "A"
    assert res["effective_date"] == "2020-01-01"
    assert res["expiry_date"] == "2022-06-01"
    assert res["classification"] == {"industry_name": "银行"}
    assert res["as_of_date"] == "2021-03-15"


def test_as_of_picks_latest_interval_open_ended():
    res = _call_as_of("2023-01-01")
    assert res["official_industry_code"] == "B"
    assert res["effective_date"] == "2022-06-01"
    assert res["expiry_date"] is None


def test_as_of_before_any_classification_returns_none():
    assert _call_as_of("2019-01-01") is None


@pytest.mark.asyncio
async def test_route_returns_as_of_response(monkeypatch):
    import api.routes as routes
    from datetime import date as _date

    payload = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
        "taxonomy_system": "sw",
        "taxonomy_version": "2021",
        "official_industry_code": "A",
        "as_of_date": "2021-03-15",
        "effective_date": "2020-01-01",
        "expiry_date": "2022-06-01",
        "source": "sw",
        "source_mode": "official",
        "official_update_time": "2020-01-05",
        "created_at": "2020-01-05",
        "updated_at": "2020-01-05",
        "classification": {"industry_name": "银行"},
    }
    mgr = SimpleNamespace(get_research_industry_as_of=AsyncMock(return_value=payload))
    monkeypatch.setattr(routes, "data_manager", mgr)

    resp = await routes.get_research_company_industry_as_of(
        instrument_id="600000.SH", as_of_date=_date(2021, 3, 15),
        taxonomy_system=None, include_snapshot=True,
    )
    assert resp.official_industry_code == "A"
    assert resp.expiry_date == "2022-06-01"


@pytest.mark.asyncio
async def test_route_404_when_none(monkeypatch):
    import api.routes as routes
    from datetime import date as _date
    from fastapi import HTTPException

    mgr = SimpleNamespace(get_research_industry_as_of=AsyncMock(return_value=None))
    monkeypatch.setattr(routes, "data_manager", mgr)

    with pytest.raises(HTTPException) as exc:
        await routes.get_research_company_industry_as_of(
            instrument_id="600000.SH", as_of_date=_date(2010, 1, 1),
            taxonomy_system=None, include_snapshot=True,
        )
    assert exc.value.status_code == 404
