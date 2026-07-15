from datetime import date, datetime
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_sources.tdx_factor_engine import TdxFactorEngine
from database.operations import DatabaseOperations


class _FakeTdxApi:
    def __init__(self, bars):
        self.bars = list(bars)
        self.bar_calls = 0

    def get_xdxr_info(self, _market, _code):
        return [{
            "category": 1,
            "year": 2007,
            "month": 6,
            "day": 18,
            "fenhong": 0.0,
            "songzhuangu": 1.0,
            "peigu": 0.0,
            "peigujia": 0.0,
        }]

    def get_security_bars(self, _category, _market, _code, offset, _count):
        self.bar_calls += 1
        return self.bars if offset == 0 else []


def _compute_000001(api, overrides=None):
    return TdxFactorEngine().compute_factors(
        api,
        market=0,
        code="000001",
        instrument_id="000001.SZ",
        start_date=datetime(2007, 1, 1),
        end_date=datetime(2007, 12, 31),
        pre_close_overrides=overrides,
    )


def test_local_suspension_override_wins_over_tdx_bar_close():
    api = _FakeTdxApi([
        {"datetime": "2007-05-31 00:00:00", "close": 99.0},
        {"datetime": "2007-05-01 00:00:00", "close": 90.0},
    ])

    factors = _compute_000001(api, {date(2007, 6, 18): 28.69})

    assert len(factors) == 1
    assert factors[0]["pre_close"] == 28.69
    assert factors[0]["factor"] == pytest.approx(1.1)
    assert api.bar_calls == 0


def test_tdx_fallback_maps_event_date_missing_from_bars():
    api = _FakeTdxApi([
        {"datetime": "2007-05-31 00:00:00", "close": 28.69},
        {"datetime": "2007-05-01 00:00:00", "close": 27.0},
    ])

    factors = _compute_000001(api)

    assert len(factors) == 1
    assert factors[0]["pre_close"] == 28.69
    assert factors[0]["factor"] == pytest.approx(1.1)


def test_local_pre_close_resolution_prefers_suspension_placeholder():
    resolve = DatabaseOperations._resolve_xdxr_pre_close_candidate

    assert resolve(28.69, 27.0) == 28.69
    assert resolve(None, 11.31) == 11.31
    assert resolve(0.0, 0.0) == 0.0


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _Scalars:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class _RowsResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return _Scalars(self.rows)


class _AuditSession:
    def __init__(self, rows):
        self.results = [_ScalarResult(len(rows)), _RowsResult(rows)]
        self.execute = AsyncMock(side_effect=self.results)


@pytest.mark.asyncio
async def test_tdx_audit_query_filters_counts_and_pages_stably():
    rows = [SimpleNamespace(
        instrument_id="000001.SZ",
        ex_date=datetime(2007, 6, 18),
        factor=1.1,
        cumulative_factor=1.1,
        pre_close=28.69,
        fenhong=0.0,
        songzhuangu=1.0,
        peigu=0.0,
        peigujia=0.0,
        validation_result="computed_unvalidated",
        ref_factor=None,
        ref_source=None,
        ratio_diff_pct=None,
        conflict_reason=None,
        source="tdx_xdxr",
        created_at=None,
        updated_at=None,
    ), SimpleNamespace(
        instrument_id="000001.SZ",
        ex_date=datetime(2008, 10, 31),
        factor=1.303862,
        cumulative_factor=1.434248,
        pre_close=11.31,
        fenhong=0.335,
        songzhuangu=3.0,
        peigu=0.0,
        peigujia=0.0,
        validation_result="computed_unvalidated",
        ref_factor=None,
        ref_source=None,
        ratio_diff_pct=None,
        conflict_reason=None,
        source="tdx_xdxr",
        created_at=None,
        updated_at=None,
    )]
    session = _AuditSession(rows[:1])
    operations = DatabaseOperations.__new__(DatabaseOperations)
    operations.db_logger = Mock()

    @asynccontextmanager
    async def get_session():
        yield session

    operations.get_async_session = get_session
    session.results[0].value = 2

    page = await operations.get_tdx_audit_factors(
        instrument_id="000001.SZ",
        start_date=date(2007, 1, 1),
        end_date=date(2008, 12, 31),
        validation_result="computed_unvalidated",
        limit=1,
        offset=0,
    )

    assert page["total"] == 2
    assert page["returned"] == 1
    assert page["has_more"] is True
    assert page["items"][0]["ex_date"] == datetime(2007, 6, 18)
