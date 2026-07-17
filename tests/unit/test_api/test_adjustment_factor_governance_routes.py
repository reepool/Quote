import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest
from fastapi import HTTPException

import api.routes as routes
from api.models import QuoteQueryRequest


def _json(response):
    return json.loads(response.body.decode("utf-8"))


def _quote_manager(factor_bundle):
    instrument = {
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "name": "Ping An Bank",
        "exchange": "SZSE",
        "type": "stock",
    }
    rows = pd.DataFrame([{
        "time": datetime(2026, 7, 15),
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "volume": 100,
        "amount": 1000.0,
        "pre_close": 10.0,
        "source": "unit",
    }])
    return SimpleNamespace(
        db_ops=SimpleNamespace(
            get_instrument_by_id=AsyncMock(return_value=instrument),
            get_instrument_by_symbol=AsyncMock(),
        ),
        get_quotes=AsyncMock(return_value=rows),
        _apply_quote_filters=AsyncMock(side_effect=lambda data, _filters: data),
        _generate_quote_statistics=AsyncMock(return_value={}),
        get_cached_adjustment_factor_bundle=AsyncMock(return_value=factor_bundle),
    )


@pytest.mark.asyncio
async def test_observation_and_canonical_routes_forward_filters(monkeypatch):
    db_ops = SimpleNamespace(
        get_adjustment_factor_observations=AsyncMock(return_value={
            "total": 1,
            "limit": 10,
            "offset": 0,
            "returned": 1,
            "has_more": False,
            "items": [{
                "instrument_id": "000001.SZ",
                "ex_date": datetime(2020, 5, 28),
                "source": "akshare",
            }],
        }),
        get_canonical_adjustment_factor_page=AsyncMock(return_value={
            "total": 0,
            "limit": 10,
            "offset": 0,
            "returned": 0,
            "has_more": False,
            "items": [],
        }),
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))

    observations = await routes.get_adjustment_factor_observations(
        instrument_id="000001.SZ",
        source="akshare",
        source_profile="sina_hfq_factor",
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        limit=10,
        offset=0,
    )
    canonical = await routes.get_canonical_adjustment_factors(
        instrument_id="000001.SZ",
        series_version="v1",
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        limit=10,
        offset=0,
    )

    assert observations.dataset == "adjustment_factor_observations"
    assert canonical.dataset == "adjustment_factors_canonical"
    assert db_ops.get_adjustment_factor_observations.await_args.kwargs[
        "source_profile"
    ] == "sina_hfq_factor"
    assert db_ops.get_canonical_adjustment_factor_page.await_args.kwargs["start_date"] == date(2020, 1, 1)
    assert db_ops.get_canonical_adjustment_factor_page.await_args.kwargs["end_date"] == date(2020, 12, 31)


@pytest.mark.asyncio
async def test_adjusted_quote_discloses_canonical_factor_metadata(monkeypatch):
    manager = _quote_manager({
        "factors": [],
        "requested_dataset": "canonical",
        "actual_dataset": "canonical",
        "series_version": "v1",
        "fallback_used": False,
        "availability_error": None,
        "instrument_status": {"coverage_status": "complete_no_events"},
    })
    monkeypatch.setattr(routes, "data_manager", manager)

    response = await routes.get_daily_quotes(
        request=QuoteQueryRequest(
            instrument_id="000001.SZ",
            start_date=datetime(2026, 7, 15),
            end_date=datetime(2026, 7, 15),
            return_format="json",
        ),
        adjust="qfq",
        include_delisted=False,
    )
    payload = _json(response)

    assert payload["factor_metadata"] == {
        "requested_dataset": "canonical",
        "actual_dataset": "canonical",
        "series_version": "v1",
        "fallback_used": False,
        "availability_error": None,
        "coverage_status": "complete_no_events",
    }


@pytest.mark.asyncio
async def test_adjusted_quote_rejects_unavailable_canonical_without_fallback(monkeypatch):
    manager = _quote_manager({
        "factors": [],
        "requested_dataset": "canonical",
        "actual_dataset": "canonical",
        "series_version": "v1",
        "fallback_used": False,
        "availability_error": "canonical factor series v1 is not promotion eligible",
        "instrument_status": None,
    })
    monkeypatch.setattr(routes, "data_manager", manager)

    with pytest.raises(HTTPException) as exc_info:
        await routes.get_daily_quotes(
            request=QuoteQueryRequest(
                instrument_id="000001.SZ",
                start_date=datetime(2026, 7, 15),
                end_date=datetime(2026, 7, 15),
                return_format="json",
            ),
            adjust="qfq",
            include_delisted=False,
        )

    assert exc_info.value.status_code == 409
