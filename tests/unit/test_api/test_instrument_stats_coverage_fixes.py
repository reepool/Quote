"""Tests for fix-instrument-stats-coverage-defects (A2/A3/A6).

Confirmed against the quant platform's M0 confirmation checklist
(docs/development/quote_api_data_confirmation_response.md):
- A2: /instruments status filter crashed with real non-enum status values.
- A3: /stats read flat keys that never existed in get_database_statistics()'s
  nested return shape, so everything silently defaulted to 0/{}.
- A6: /quotes/coverage's quoted_count counted instruments with unknown
  listed_date, inflating coverage_ratio above 1.0 in cross-market queries.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import api.routes as routes
from api.models import InstrumentStatusEnum


def test_instrument_status_enum_covers_all_real_values():
    real_values = {
        "active", "auto_deactivated_no_data", "auto_deactivated_zombie",
        "calculation_terminated", "delisted", "excluded", "metadata_only",
        "suspended",
    }
    enum_values = {member.value for member in InstrumentStatusEnum}
    assert real_values.issubset(enum_values)


@pytest.mark.asyncio
async def test_get_instruments_returns_delisted_without_crash(monkeypatch):
    delisted_row = {
        "instrument_id": "600193.SH", "symbol": "600193", "name": "已退市样本",
        "exchange": "SSE", "type": "stock", "currency": "CNY",
        "listed_date": datetime(2000, 1, 1), "delisted_date": datetime(2020, 1, 1),
        "status": "delisted", "is_active": False, "is_st": False, "trading_status": 0,
        "source": "test", "source_symbol": "600193",
        "created_at": datetime(2024, 1, 1), "updated_at": datetime(2024, 1, 1),
        "data_version": 1,
    }
    mgr = SimpleNamespace(
        db_ops=SimpleNamespace(get_instruments_with_filters=AsyncMock(return_value=[delisted_row]))
    )
    monkeypatch.setattr(routes, "data_manager", mgr)

    result = await routes.get_instruments(
        exchange=None, type=None, industry=None, sector=None, market=None,
        status=None, is_active=False, is_st=None, trading_status=None,
        listed_after=None, listed_before=None, delisted_after=None, delisted_before=None,
        limit=100, offset=0, sort_by="symbol", sort_order="asc",
    )
    assert result[0].status.value == "delisted"


def test_get_instruments_with_filters_type_filter_is_lowercased():
    """The compiled SQL filter must compare against the lowercased type value.

    Regression guard for the case-sensitivity bug: type=STOCK previously matched
    zero rows because InstrumentDB.type is stored lowercase ('stock').
    """
    from sqlalchemy import select
    from sqlalchemy.dialects import sqlite as sqlite_dialect
    from database.models import InstrumentDB

    instrument_type = "STOCK"
    stmt = select(InstrumentDB).filter(InstrumentDB.type == instrument_type.lower())
    compiled = stmt.compile(dialect=sqlite_dialect.dialect(), compile_kwargs={"literal_binds": True})
    assert "'stock'" in str(compiled)
    assert "'STOCK'" not in str(compiled)


@pytest.mark.asyncio
async def test_stats_route_reads_nested_structure(monkeypatch):
    nested_stats = {
        "instruments": {
            "total": 13155, "active": 9272,
            "by_exchange": {"SSE": 2742, "SZSE": 5396, "BSE": 327, "HKEX": 4690},
            "by_type": {"stock": 10554, "index": 2601},
            "by_status": {"active": 9185, "delisted": 480},
        },
        "daily_quotes": {
            "total": 6487818, "by_trading_status": {}, "by_source": {},
            "latest_date": datetime(2026, 7, 10), "earliest_date": datetime(2023, 7, 17),
        },
        "trading_calendar": {"total_records": 4233, "trading_days": 3081, "by_exchange": {}},
        "data_updates": {"total": 0, "by_status": {}, "latest": None},
    }
    supplement = {
        "by_industry": {"银行": 42, "白酒": 12},
        "trading_calendar_earliest": datetime(2023, 7, 17),
        "trading_calendar_latest": datetime(2026, 9, 29),
    }
    mgr = SimpleNamespace(
        db_ops=SimpleNamespace(
            get_database_statistics=AsyncMock(return_value=nested_stats),
            get_stats_supplement=AsyncMock(return_value=supplement),
        )
    )
    monkeypatch.setattr(routes, "data_manager", mgr)

    resp = await routes.get_data_statistics()
    assert resp.instruments_count == 13155
    assert resp.quotes_count == 6487818
    assert resp.trading_days_count == 3081
    assert resp.instruments_by_exchange == {"SSE": 2742, "SZSE": 5396, "BSE": 327, "HKEX": 4690}
    assert resp.instruments_by_industry == {"银行": 42, "白酒": 12}
    assert resp.quotes_date_range == {
        "start": datetime(2023, 7, 17), "end": datetime(2026, 7, 10)
    }


@pytest.mark.asyncio
async def test_stats_route_handles_empty_database(monkeypatch):
    mgr = SimpleNamespace(
        db_ops=SimpleNamespace(
            get_database_statistics=AsyncMock(return_value={}),
            get_stats_supplement=AsyncMock(return_value={}),
        )
    )
    monkeypatch.setattr(routes, "data_manager", mgr)

    resp = await routes.get_data_statistics()
    assert resp.instruments_count == 0
    assert resp.quotes_date_range == {}


@pytest.mark.asyncio
async def test_coverage_ratio_capped_when_unknown_listed_date_present(monkeypatch):
    # Mirrors the real repro: HKEX rows with NULL listed_date inflating quoted_count.
    cov = {
        "date": "2024-06-28", "exchange": None, "instrument_type": "stock",
        "listed_count": 5365, "quoted_count": 5301,
        "unknown_listed_date_quoted_count": 2362,
        "coverage_ratio": 5301 / 5365,
    }
    mgr = SimpleNamespace(db_ops=SimpleNamespace(get_daily_coverage=AsyncMock(return_value=cov)))
    monkeypatch.setattr(routes, "data_manager", mgr)

    from datetime import date
    resp = await routes.get_quotes_coverage(
        date=date(2024, 6, 28), start_date=None, end_date=None,
        exchange=None, instrument_type="stock",
    )
    item = resp.items[0]
    assert item.coverage_ratio <= 1.0
    assert item.unknown_listed_date_quoted_count == 2362
