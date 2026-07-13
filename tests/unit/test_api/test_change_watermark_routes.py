from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

import api.routes as routes


def _change(sequence_id, domain="quotes", dataset="daily_quotes"):
    return {
        "sequence_id": sequence_id,
        "domain": domain,
        "dataset": dataset,
        "change_type": "insert",
        "business_key": {
            "instrument_id": "000001.SZ",
            "trade_date": "2026-07-10",
        },
        "instrument_id": "000001.SZ",
        "series_id": None,
        "observation_date": datetime(2026, 7, 10),
        "period": None,
        "old_hash": None,
        "new_hash": "abc",
        "row_version": 1,
        "source": "unit",
        "source_mode": None,
        "source_profile": None,
        "ingestion_run_id": None,
        "batch_id": None,
        "changed_at": datetime(2026, 7, 13, 9, 30),
    }


@pytest.mark.asyncio
async def test_latest_change_watermark_empty(monkeypatch):
    db_ops = SimpleNamespace(
        get_change_watermark=AsyncMock(
            return_value={
                "domain": "quotes",
                "dataset": None,
                "latest_sequence": 0,
                "is_empty": True,
            }
        )
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))

    response = await routes.get_latest_change_watermark(domain="quotes", dataset=None)

    assert response.latest_sequence == 0
    assert response.is_empty is True
    db_ops.get_change_watermark.assert_awaited_once_with(domain="quotes", dataset=None)


@pytest.mark.asyncio
async def test_change_query_returns_paginated_records(monkeypatch):
    db_ops = SimpleNamespace(
        get_data_changes=AsyncMock(
            return_value={
                "since_sequence": 1,
                "latest_sequence": 3,
                "latest_returned_sequence": 2,
                "next_sequence": 2,
                "has_more": True,
                "limit": 1,
                "count": 1,
                "changes": [_change(2)],
            }
        )
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))

    response = await routes.get_data_changes(
        since_sequence=1,
        domain="quotes",
        dataset="daily_quotes",
        instrument_id=None,
        series_id=None,
        start_date=None,
        end_date=None,
        limit=1,
    )

    assert response.count == 1
    assert response.has_more is True
    assert response.changes[0].sequence_id == 2
    kwargs = db_ops.get_data_changes.await_args.kwargs
    assert kwargs["domain"] == "quotes"
    assert kwargs["dataset"] == "daily_quotes"


@pytest.mark.asyncio
async def test_daily_quote_changes_filters_to_quote_dataset(monkeypatch):
    db_ops = SimpleNamespace(
        get_data_changes=AsyncMock(
            return_value={
                "since_sequence": 0,
                "latest_sequence": 1,
                "latest_returned_sequence": 1,
                "next_sequence": 1,
                "has_more": False,
                "limit": 1000,
                "count": 1,
                "changes": [_change(1)],
            }
        )
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))

    response = await routes.get_daily_quote_changes(
        since_sequence=0,
        instrument_id="000001.SZ",
        start_date=None,
        end_date=None,
        limit=1000,
    )

    assert response.changes[0].domain == "quotes"
    kwargs = db_ops.get_data_changes.await_args.kwargs
    assert kwargs["domain"] == "quotes"
    assert kwargs["dataset"] == "daily_quotes"
    assert kwargs["instrument_id"] == "000001.SZ"


@pytest.mark.asyncio
async def test_change_query_invalid_sequence_returns_400(monkeypatch):
    db_ops = SimpleNamespace(
        get_data_changes=AsyncMock(side_effect=ValueError("since_sequence must be >= 0"))
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))

    with pytest.raises(HTTPException) as exc:
        await routes.get_data_changes(
            since_sequence=-1,
            domain=None,
            dataset=None,
            instrument_id=None,
            series_id=None,
            start_date=None,
            end_date=None,
            limit=1000,
        )

    assert exc.value.status_code == 400
