from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

import api.routes as routes


def _page(items=None, total=0, limit=100, offset=0):
    items = list(items or [])
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "returned": len(items),
        "has_more": offset + len(items) < total,
    }


def _event():
    return {
        "instrument_id": "000001.SZ",
        "ex_date": datetime(2007, 6, 18),
        "factor": 1.1,
        "cumulative_factor": 1.1,
        "pre_close": 28.69,
        "fenhong": 0.0,
        "songzhuangu": 1.0,
        "peigu": 0.0,
        "peigujia": 0.0,
        "validation_result": "computed_unvalidated",
        "ref_factor": None,
        "ref_source": None,
        "ratio_diff_pct": None,
        "conflict_reason": None,
        "source": "tdx_xdxr",
        "created_at": datetime(2026, 7, 15, 10, 0),
        "updated_at": datetime(2026, 7, 15, 10, 0),
    }


@pytest.mark.asyncio
async def test_xdxr_audit_route_normalizes_filters_and_marks_audit_only(monkeypatch):
    query = AsyncMock(return_value=_page([_event()], total=2, limit=1))
    monkeypatch.setattr(
        routes,
        "data_manager",
        SimpleNamespace(db_ops=SimpleNamespace(get_tdx_audit_factors=query)),
    )

    response = await routes.get_xdxr_audit_events(
        instrument_id="000001.SZSE",
        start_date=date(2007, 1, 1),
        end_date=date(2008, 12, 31),
        validation_result="computed_unvalidated",
        limit=1,
        offset=0,
    )

    assert response.audit_only is True
    assert response.dataset == "adjustment_factors_tdx"
    assert response.total == 2
    assert response.has_more is True
    assert response.items[0].songzhuangu == 1.0
    assert query.await_args.kwargs["instrument_id"] == "000001.SZ"
    assert query.await_args.kwargs["validation_result"] == "computed_unvalidated"


@pytest.mark.asyncio
async def test_xdxr_audit_route_returns_empty_page(monkeypatch):
    query = AsyncMock(return_value=_page())
    monkeypatch.setattr(
        routes,
        "data_manager",
        SimpleNamespace(db_ops=SimpleNamespace(get_tdx_audit_factors=query)),
    )

    response = await routes.get_xdxr_audit_events(
        instrument_id=None,
        start_date=None,
        end_date=None,
        validation_result=None,
        limit=100,
        offset=0,
    )

    assert response.total == 0
    assert response.items == []
    assert response.audit_only is True


@pytest.mark.asyncio
async def test_xdxr_audit_route_rejects_reversed_date_range(monkeypatch):
    query = AsyncMock()
    monkeypatch.setattr(
        routes,
        "data_manager",
        SimpleNamespace(db_ops=SimpleNamespace(get_tdx_audit_factors=query)),
    )

    with pytest.raises(HTTPException) as exc:
        await routes.get_xdxr_audit_events(
            instrument_id="000001.SZ",
            start_date=date(2008, 1, 1),
            end_date=date(2007, 1, 1),
            validation_result=None,
            limit=100,
            offset=0,
        )

    assert exc.value.status_code == 400
    query.assert_not_awaited()
