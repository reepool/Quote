"""Tests for quote API data-capability improvements (REQ-04/11.1/01.1/01.3/12).

Change: add-quote-api-data-capability-improvements.
Follows the direct-route-call + mocked data_manager pattern used in
test_quote_query_semantics.py.
"""

import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

import api.routes as routes
from api.models import QuoteQueryRequest


def _quote_row(instrument_id: str, trade_time: datetime, close: float) -> dict:
    return {
        "time": trade_time,
        "instrument_id": instrument_id,
        "symbol": instrument_id.split(".")[0],
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1000,
        "amount": 10000.0,
        "turnover": 0.0,
        "pre_close": close,
        "change": 0.0,
        "pct_change": 0.0,
        "tradestatus": 1,
        "factor": 1.0,
        "adjustment_type": "none",
        "is_complete": True,
        "quality_score": 1.0,
        "source": "unit-test",
        "batch_id": None,
    }


def _instrument(instrument_id, symbol, name, exchange, instrument_type, **extra):
    base = {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "name": name,
        "exchange": exchange,
        "type": instrument_type,
    }
    base.update(extra)
    return base


def _json(response):
    return json.loads(response.body.decode("utf-8"))


def _mgr_with_rows(instrument, rows):
    return SimpleNamespace(
        db_ops=SimpleNamespace(
            get_instrument_by_symbol=AsyncMock(),
            get_instrument_by_id=AsyncMock(return_value=instrument),
        ),
        get_quotes=AsyncMock(return_value=rows),
        _apply_quote_filters=AsyncMock(side_effect=lambda data, _filters: data),
        _generate_quote_statistics=AsyncMock(return_value={}),
        get_cached_adjustment_factors=AsyncMock(return_value=[]),
    )


def _five_day_frame():
    return pd.DataFrame(
        [_quote_row("000001.SZ", datetime(2026, 4, 20 + i), 10.0 + i) for i in range(5)]
    )


@pytest.mark.asyncio
async def test_pagination_limit_offset_slices(monkeypatch):
    inst = _instrument("000001.SZ", "000001", "平安银行", "SZSE", "stock")
    monkeypatch.setattr(routes, "data_manager", _mgr_with_rows(inst, _five_day_frame()))

    request = QuoteQueryRequest(
        instrument_id="000001.SZ",
        start_date=datetime(2026, 4, 20),
        end_date=datetime(2026, 4, 24),
        return_format="json",
        limit=2,
        offset=1,
    )
    payload = _json(await routes.get_daily_quotes(request=request, adjust="none", include_delisted=False))

    assert payload["total_records"] == 2
    assert payload["pagination"]["total_available"] == 5
    assert payload["pagination"]["limit"] == 2
    assert payload["pagination"]["offset"] == 1
    assert payload["pagination"]["returned_records"] == 2
    # stable time-ascending order => offset 1 is the 2nd day
    times = [row["time"] for row in payload["data"]]
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_pagination_omitted_limit_returns_all(monkeypatch):
    inst = _instrument("000001.SZ", "000001", "平安银行", "SZSE", "stock")
    monkeypatch.setattr(routes, "data_manager", _mgr_with_rows(inst, _five_day_frame()))

    request = QuoteQueryRequest(
        instrument_id="000001.SZ",
        start_date=datetime(2026, 4, 20),
        end_date=datetime(2026, 4, 24),
        return_format="json",
    )
    payload = _json(await routes.get_daily_quotes(request=request, adjust="none", include_delisted=False))

    assert payload["total_records"] == 5
    assert payload["pagination"]["limit"] is None
    assert payload["pagination"]["total_available"] == 5
    assert payload["pagination"]["returned_records"] == 5


@pytest.mark.asyncio
async def test_include_delisted_echoed_and_default_false(monkeypatch):
    inst = _instrument(
        "000002.SZ", "000002", "已退市样本", "SZSE", "stock",
        delisted_date=datetime(2020, 5, 1),
    )
    monkeypatch.setattr(
        routes, "data_manager",
        _mgr_with_rows(inst, pd.DataFrame([_quote_row("000002.SZ", datetime(2019, 4, 24), 5.0)])),
    )

    request = QuoteQueryRequest(
        instrument_id="000002.SZ",
        start_date=datetime(2019, 4, 22),
        end_date=datetime(2019, 4, 24),
        return_format="json",
    )
    payload_default = _json(await routes.get_daily_quotes(request=request, adjust="none", include_delisted=False))
    assert payload_default["include_delisted"] is False
    assert payload_default["instrument_delisted"] is True
    assert payload_default["total_records"] == 1

    payload_opt = _json(
        await routes.get_daily_quotes(request=request, adjust="none", include_delisted=True)
    )
    assert payload_opt["include_delisted"] is True
    # opting in must not change the returned rows for an id query
    assert payload_opt["total_records"] == payload_default["total_records"]


@pytest.mark.asyncio
async def test_quotes_coverage_single_date(monkeypatch):
    cov = {
        "date": "2024-06-03",
        "exchange": "SSE",
        "instrument_type": "stock",
        "listed_count": 2240,
        "quoted_count": 2200,
        "coverage_ratio": 2200 / 2240,
    }
    mgr = SimpleNamespace(db_ops=SimpleNamespace(get_daily_coverage=AsyncMock(return_value=cov)))
    monkeypatch.setattr(routes, "data_manager", mgr)

    resp = await routes.get_quotes_coverage(
        date=date(2024, 6, 3), start_date=None, end_date=None,
        exchange="SSE", instrument_type="stock",
    )
    assert resp.total == 1
    assert resp.items[0].listed_count == 2240
    assert resp.items[0].quoted_count == 2200
    mgr.db_ops.get_daily_coverage.assert_awaited_once()


@pytest.mark.asyncio
async def test_quotes_coverage_requires_a_date(monkeypatch):
    mgr = SimpleNamespace(db_ops=SimpleNamespace(get_daily_coverage=AsyncMock()))
    monkeypatch.setattr(routes, "data_manager", mgr)
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        await routes.get_quotes_coverage(
            date=None, start_date=None, end_date=None, exchange=None, instrument_type="stock",
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_instruments_expose_lot_and_tick(monkeypatch):
    inst = _instrument(
        "00001.HK", "00001", "长和", "HKEX", "stock",
        currency="HKD", lot_size=500, tick_size=0.05,
        listed_date=datetime(2015, 3, 18), delisted_date=None,
        status="active", is_active=True, is_st=False, trading_status=1,
        created_at=datetime(2024, 1, 1), updated_at=datetime(2024, 1, 1),
        data_version=1,
    )
    mgr = SimpleNamespace(
        db_ops=SimpleNamespace(get_instruments_with_filters=AsyncMock(return_value=[inst]))
    )
    monkeypatch.setattr(routes, "data_manager", mgr)

    result = await routes.get_instruments(
        exchange="HKEX", type=None, industry=None, sector=None, market=None,
        status=None, is_active=None, is_st=None, trading_status=None,
        listed_after=None, listed_before=None, delisted_after=None, delisted_before=None,
        limit=100, offset=0, sort_by="symbol", sort_order="asc",
    )
    assert result[0].lot_size == 500
    assert result[0].tick_size == 0.05
    # delisted filters forwarded to db layer
    kwargs = mgr.db_ops.get_instruments_with_filters.await_args.kwargs
    assert "delisted_after" in kwargs and "delisted_before" in kwargs
