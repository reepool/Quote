from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import api.routes as routes


@pytest.mark.asyncio
async def test_cninfo_llm_lineage_routes_are_read_only_and_forward_filters(monkeypatch):
    page = {
        "total": 1, "limit": 10, "offset": 0, "returned": 1,
        "has_more": False, "items": [{"source_event_key": "event-1"}],
    }
    db_ops = SimpleNamespace(
        get_corporate_action_document_bundle=AsyncMock(return_value=page.copy()),
        get_corporate_action_llm_analyses=AsyncMock(return_value=page.copy()),
        get_corporate_action_resolution_reviews=AsyncMock(return_value=page.copy()),
        get_corporate_action_resolved_terms_page=AsyncMock(return_value=page.copy()),
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))
    artifacts = await routes.get_corporate_action_document_artifacts(
        announcement_id="ann-1", source_event_key="event-1", limit=10, offset=0,
    )
    analyses = await routes.get_corporate_action_llm_analyses(
        instrument_id="000001.SZ", source_event_key="event-1",
        validation_status="manual_required", limit=10, offset=0,
    )
    reviews = await routes.get_corporate_action_resolution_reviews(
        source_event_key="event-1", decision="resolved", limit=10, offset=0,
    )
    terms = await routes.get_corporate_action_resolved_terms(
        instrument_id="000001.SZ", source_event_key="event-1",
        active_only=True, limit=10, offset=0,
    )
    assert artifacts.dataset == "corporate_action_document_artifacts"
    assert analyses.dataset == "corporate_action_llm_analyses"
    assert reviews.dataset == "corporate_action_resolution_reviews"
    assert terms.dataset == "corporate_action_resolved_terms"
    assert db_ops.get_corporate_action_llm_analyses.await_args.kwargs["instrument_id"] == "000001.SZ"


@pytest.mark.asyncio
async def test_resolution_review_route_maps_business_validation_error(monkeypatch):
    db_ops = SimpleNamespace()
    manager = SimpleNamespace(
        db_ops=db_ops,
        review_cninfo_corporate_action_resolution=AsyncMock(
            side_effect=ValueError("resolved review requires evidence")
        ),
    )
    monkeypatch.setattr(routes, "data_manager", manager)
    with pytest.raises(Exception) as exc_info:
        await routes.review_corporate_action_resolution({"decision": "resolved"})
    assert exc_info.value.status_code == 400
