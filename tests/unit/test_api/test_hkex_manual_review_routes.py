from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from api.models import HKEXManualReviewRequest
from api.routes import (
    append_hkex_manual_review_evidence,
    get_hkex_master_review_required,
    list_hkex_manual_review_evidence,
)


@pytest.mark.asyncio
async def test_hkex_review_required_route_runs_audit_only():
    with patch("api.routes.data_manager") as dm:
        dm.sync_hkex_instrument_master = AsyncMock(return_value={
            "status": "success",
            "mode": "audit_only",
            "summary": {"review_required": 1},
            "exchanges": {
                "HKEX": {
                    "review_required_samples": [
                        {"instrument_id": "02934.HK", "reason": "missing"}
                    ]
                }
            },
            "warnings": [],
            "errors": [],
        })

        response = await get_hkex_master_review_required(limit=5)

    dm.sync_hkex_instrument_master.assert_awaited_once_with(mode="audit_only")
    assert response.review_required == 1
    assert response.samples[0]["instrument_id"] == "02934.HK"


@pytest.mark.asyncio
async def test_hkex_manual_review_routes_append_and_list():
    with patch("api.routes.data_manager") as dm:
        dm.append_hkex_manual_review_evidence = AsyncMock(return_value={
            "status": "success",
            "path": "data/hkex_manual_review.json",
            "entry": {"instrument_id": "02934.HK", "action": "delisted"},
            "total": 1,
        })
        dm.get_hkex_manual_review_evidence = AsyncMock(return_value={
            "status": "success",
            "path": "data/hkex_manual_review.json",
            "total": 1,
            "entries": [{"instrument_id": "02934.HK", "action": "delisted"}],
        })
        request = HKEXManualReviewRequest(
            instrument_id="02934.HK",
            action="delisted",
            effective_date=date(2026, 5, 30),
            reason="confirmed",
        )

        append_response = await append_hkex_manual_review_evidence(request)
        list_response = await list_hkex_manual_review_evidence(limit=10)

    assert append_response.entry["instrument_id"] == "02934.HK"
    assert list_response.total == 1
    dm.append_hkex_manual_review_evidence.assert_awaited_once()
    dm.get_hkex_manual_review_evidence.assert_awaited_once_with(limit=10)
