from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import api.routes as routes


@pytest.mark.asyncio
async def test_official_observation_and_coverage_routes_forward_filters(monkeypatch):
    db_ops = SimpleNamespace(
        get_corporate_action_observations=AsyncMock(
            return_value={
                "total": 1,
                "limit": 10,
                "offset": 0,
                "returned": 1,
                "has_more": False,
                "items": [
                    {
                        "instrument_id": "000001.SZ",
                        "source": "cninfo",
                        "source_profile": "cninfo_dividend",
                        "ex_date": datetime(2026, 6, 12),
                    }
                ],
            }
        ),
        get_corporate_action_instrument_status_page=AsyncMock(
            return_value={
                "total": 1,
                "limit": 10,
                "offset": 0,
                "returned": 1,
                "has_more": False,
                "items": [
                    {
                        "instrument_id": "000001.SZ",
                        "source_profile": "cninfo_dividend",
                        "coverage_status": "complete_with_events",
                    }
                ],
            }
        ),
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))

    observations = await routes.get_official_corporate_action_observations(
        instrument_id="000001.SZ",
        source="cninfo",
        source_profile="cninfo_dividend",
        action_type="dividend",
        quality_status="structured_complete",
        include_inactive=True,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 12, 31),
        limit=10,
        offset=0,
    )
    coverage = await routes.get_official_corporate_action_coverage(
        instrument_id="000001.SZ",
        source_profile="cninfo_dividend",
        coverage_status="complete_with_events",
        limit=10,
        offset=0,
    )

    assert observations.dataset == "corporate_action_observations"
    assert coverage.dataset == "corporate_action_instrument_status"
    assert db_ops.get_corporate_action_observations.await_args.kwargs[
        "start_date"
    ] == date(2026, 1, 1)
    assert (
        db_ops.get_corporate_action_observations.await_args.kwargs["include_inactive"]
        is True
    )
    assert (
        db_ops.get_corporate_action_instrument_status_page.await_args.kwargs[
            "coverage_status"
        ]
        == "complete_with_events"
    )
