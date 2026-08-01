from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

import api.routes as routes


@pytest.mark.asyncio
async def test_canonical_quality_and_decisions_resolve_active_version(monkeypatch):
    db_ops = SimpleNamespace(
        get_adjustment_factor_series_quality=AsyncMock(return_value={
            "status": "promoted",
            "promotion_eligible": True,
            "coverage_ratio": 1.0,
        }),
        get_adjustment_factor_decision_page=AsyncMock(return_value={
            "total": 1,
            "limit": 10,
            "offset": 0,
            "returned": 1,
            "has_more": False,
            "items": [{"instrument_id": "000001.SZ"}],
        }),
    )
    manager = SimpleNamespace(
        db_ops=db_ops,
        _effective_adjustment_factor_governance=Mock(return_value=(
            {"canonical_series_version": "active-v1"},
            {"source": "configured_default", "error": None},
        )),
    )
    monkeypatch.setattr(routes, "data_manager", manager)

    quality = await routes.get_adjustment_factor_quality(
        series_version=None
    )
    decisions = await routes.get_adjustment_factor_decisions(
        series_version=None,
        instrument_id="000001.SZ",
        confidence="high",
        limit=10,
        offset=0,
    )

    assert quality.series_version == "active-v1"
    assert decisions.filters["series_version"] == "active-v1"
    assert decisions.total == 1
    db_ops.get_adjustment_factor_series_quality.assert_awaited_once_with(
        "active-v1"
    )
    assert db_ops.get_adjustment_factor_decision_page.await_args.kwargs == {
        "series_version": "active-v1",
        "instrument_id": "000001.SZ",
        "confidence": "high",
        "limit": 10,
        "offset": 0,
    }


def test_active_default_fails_closed_for_invalid_activation(monkeypatch):
    monkeypatch.setattr(
        routes,
        "data_manager",
        SimpleNamespace(
            _effective_adjustment_factor_governance=Mock(return_value=(
                {"canonical_series_version": "configured-v1"},
                {"error": "invalid JSON"},
            ))
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        routes._resolve_adjustment_factor_series_version(None)

    assert exc_info.value.status_code == 503
    assert "invalid JSON" in str(exc_info.value.detail)
    assert routes._resolve_adjustment_factor_series_version("audit-v1") == (
        "audit-v1"
    )
