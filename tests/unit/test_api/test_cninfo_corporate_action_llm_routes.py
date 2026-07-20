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
        get_corporate_action_review_queue=AsyncMock(return_value=page.copy()),
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
    queue = await routes.get_corporate_action_resolution_review_queue(
        instrument_id="000001.SZ", validation_status="manual_required",
        review_tier="quick_review", failed_gate="no_conflict",
        gate_signature="no_conflict", source_profile="cninfo_dividend",
        action_type="mixed_distribution", event_type="share_reform",
        reviewed_state="unreviewed", include_machine_rework=False,
        limit=10, offset=0,
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
    assert queue.dataset == "corporate_action_resolution_review_queue"
    assert reviews.dataset == "corporate_action_resolution_reviews"
    assert terms.dataset == "corporate_action_resolved_terms"
    assert db_ops.get_corporate_action_llm_analyses.await_args.kwargs["instrument_id"] == "000001.SZ"
    queue_kwargs = db_ops.get_corporate_action_review_queue.await_args.kwargs
    assert queue_kwargs["review_tier"] == "quick_review"
    assert queue_kwargs["include_machine_rework"] is False


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


@pytest.mark.asyncio
async def test_batch_resolution_review_route_forwards_bounded_payload(monkeypatch):
    manager = SimpleNamespace(
        review_cninfo_corporate_action_resolutions_batch=AsyncMock(return_value={
            "status": "partial", "total": 2, "succeeded": 1, "failed": 1,
            "items": [],
        })
    )
    monkeypatch.setattr(routes, "data_manager", manager)
    payload = {"reviewer": "reviewer", "items": [{"analysis_id": 1}]}
    result = await routes.review_corporate_action_resolutions_batch(payload)
    assert result["status"] == "partial"
    assert manager.review_cninfo_corporate_action_resolutions_batch.await_args.args[0] == payload


@pytest.mark.asyncio
async def test_review_queue_route_maps_invalid_filter_to_bad_request(monkeypatch):
    db_ops = SimpleNamespace(
        get_corporate_action_review_queue=AsyncMock(
            side_effect=ValueError("failed_gate contains unsupported characters")
        )
    )
    monkeypatch.setattr(routes, "data_manager", SimpleNamespace(db_ops=db_ops))
    with pytest.raises(Exception) as exc_info:
        await routes.get_corporate_action_resolution_review_queue(
            failed_gate="bad.path", limit=10, offset=0,
        )
    assert exc_info.value.status_code == 400
