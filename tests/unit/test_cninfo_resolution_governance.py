import asyncio
from datetime import date, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from data_sources.cninfo_resolution_governance import (
    APPLICABILITY_POLICY_VERSION,
    classify_date_applicability,
    derive_resolution_state,
)


def _row(**overrides):
    row = {
        "instrument_id": "600108.SH",
        "source_profile": "cninfo_dividend",
        "source_event_key": "event-1",
        "action_type": "dividend",
        "event_status": "announced_incomplete",
        "ex_date": None,
        "record_date": None,
        "pay_date": None,
        "share_arrival_date": None,
        "cash_dividend_per_share": 0.1,
        "bonus_shares_per_share": 0.0,
        "capitalization_shares_per_share": 0.0,
        "rights_shares_per_share": 0.0,
    }
    row.update(overrides)
    return row


def _state(event_key, state, *, candidate_count=0, terminal=False, next_action=None):
    return {
        "instrument_id": "600108.SH",
        "source_event_key": event_key,
        "source_profile": "cninfo_dividend",
        "action_type": "dividend",
        "exchange": "SSE",
        "policy_version": APPLICABILITY_POLICY_VERSION,
        "state_version": "cninfo_resolution_state_v2",
        "resolution_state": state,
        "is_terminal": terminal,
        "factor_blocking": not terminal,
        "state_reason": state,
        "next_action": next_action or (
            "semantic_resolution" if candidate_count else "discover_official_announcements"
        ),
        "candidate_count": candidate_count,
        "latest_analysis_id": None,
        "latest_review_id": None,
        "resolved_effective_date": None,
        "diagnostics": {},
    }


def test_cash_dividend_optional_dates_do_not_create_extra_blockers():
    result = classify_date_applicability(_row())

    assert result["required_date_roles"] == ["effective_date"]
    assert result["missing_required_date_roles"] == ["effective_date"]
    assert "pay_date" in result["supporting_date_roles"]
    assert "share_arrival_date" not in result["supporting_date_roles"]


def test_explicit_no_distribution_is_not_factor_blocking():
    result = derive_resolution_state(
        _row(
            action_type="distribution",
            cash_dividend_per_share=0.0,
            description="不派发股利",
        ),
        scan_status="success",
    )

    assert result["applicability"]["explicit_non_effective"] is True
    assert result["resolution_state"] == "non_effective"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_compound_distribution_description_is_not_terminalized_from_substring():
    result = derive_resolution_state(
        _row(
            action_type="distribution",
            cash_dividend_per_share=0.0,
            capitalization_shares_per_share=0.0,
            description="不进行利润分配，但以资本公积每10股转增10股",
        ),
        scan_status="success",
    )

    assert result["applicability"]["explicit_non_effective"] is False
    assert result["resolution_state"] == "evidence_unavailable"
    assert result["factor_blocking"] is True


def test_bse_is_source_unsupported_and_not_factor_blocking():
    result = derive_resolution_state(_row(instrument_id="920000.BJ"))

    assert result["resolution_state"] == "source_not_supported"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_raw_effective_date_supersedes_prior_operational_gap_state():
    result = derive_resolution_state(_row(ex_date="2026-06-12"))

    assert result["resolution_state"] == "resolved_source"
    assert result["is_terminal"] is True
    assert result["factor_blocking"] is False


def test_model_only_cancellation_remains_manual_required():
    result = derive_resolution_state(
        _row(),
        candidate_count=1,
        latest_analysis={
            "validation_status": "manual_required",
            "result": {"event_stage": "cancelled"},
        },
    )

    assert result["resolution_state"] == "manual_required"
    assert result["is_terminal"] is False
    assert result["factor_blocking"] is True


def test_reviewed_non_effective_is_terminal_but_empty_scan_is_retryable():
    reviewed = derive_resolution_state(
        _row(),
        latest_review={
            "decision": "rejected",
            "review_payload": {"terminal_reason": "non_effective"},
        },
    )
    unavailable = derive_resolution_state(_row(), scan_status="success")

    assert reviewed["resolution_state"] == "non_effective"
    assert reviewed["is_terminal"] is True
    assert reviewed["factor_blocking"] is False
    assert unavailable["resolution_state"] == "evidence_unavailable"
    assert unavailable["is_terminal"] is False
    assert unavailable["factor_blocking"] is True


def test_multiple_resolved_dates_remain_a_blocking_conflict():
    result = derive_resolution_state(
        _row(),
        resolved_evidence={"effective_date": "2026-06-12"},
        resolved_evidence_conflict=True,
    )

    assert result["resolution_state"] == "conflict"
    assert result["is_terminal"] is False
    assert result["factor_blocking"] is True


def test_unbounded_search_requires_manual_anchor():
    result = derive_resolution_state(_row(), scan_status="unbounded_anchor")

    assert result["resolution_state"] == "manual_required"
    assert result["next_action"] == "manual_anchor_or_external_evidence"


def test_rejected_review_does_not_confirm_model_only_cancellation():
    result = derive_resolution_state(
        _row(),
        latest_analysis={
            "validation_status": "manual_required",
            "result": {"event_stage": "cancelled"},
        },
        latest_review={
            "decision": "rejected",
            "notes": "The model cancellation conclusion is not confirmed.",
            "review_payload": {
                "original_result": {"event_stage": "cancelled"},
            },
        },
    )

    assert result["resolution_state"] == "manual_required"
    assert result["is_terminal"] is False


@pytest.mark.asyncio
async def test_governance_routes_exact_event_keys_across_write_stages():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [
        _state("event-discovery", "discovery_pending"),
        _state(
            "event-candidate",
            "candidate_pending_analysis",
            candidate_count=1,
        ),
    ]
    after_discovery = [
        _state(
            "event-discovery",
            "candidate_pending_analysis",
            candidate_count=1,
        ),
        initial[1],
    ]
    final = [
        _state("event-discovery", "resolved_evidence", terminal=True),
        _state("event-candidate", "resolved_evidence", terminal=True),
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, after_discovery, final]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "success",
            "target_samples": [{
                "source_event_key": "event-discovery",
                "candidate_count": 1,
            }],
            "errors": [],
        }
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={"status": "success", "errors": []}
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 2, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[
        {
            "instrument_id": "600108.SH",
            "source_event_key": "event-discovery",
        },
        {
            "instrument_id": "600108.SH",
            "source_event_key": "event-candidate",
        },
    ])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery", "resolution"],
        max_events=2,
        title_max_concurrency=99,
        dry_run=False,
    )

    assert result["status"] == "success"
    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-discovery"]
    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "title_max_concurrency"
    ] == 50
    assert result["parameters"]["title_max_concurrency"] == 50
    assert manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-discovery", "event-candidate"]
    manager.db_ops.upsert_corporate_action_resolution_states.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_governance_starts_resolution_before_discovery_returns():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [_state("event-discovery", "discovery_pending")]
    terminal = [
        _state("event-discovery", "resolved_evidence", terminal=True)
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, terminal, terminal]
    )
    analysis_started = asyncio.Event()
    ordering = []

    async def discover(**kwargs):
        await kwargs["on_event_ready"]({
            "instrument_id": "600108.SH",
            "source_event_key": "event-discovery",
            "candidate_count": 1,
        })
        await analysis_started.wait()
        ordering.append("discovery_returned")
        return {
            "status": "success",
            "target_samples": [{
                "source_event_key": "event-discovery",
                "candidate_count": 1,
            }],
            "errors": [],
        }

    async def analyze(**kwargs):
        ordering.append("analysis_started")
        analysis_started.set()
        return {
            "status": "success",
            "counts": {"processed": 1, "analyzed": 1},
            "targets": {"batch_events": 1},
            "errors": [],
        }

    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        side_effect=discover
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        side_effect=analyze
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 1, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[{
        "instrument_id": "600108.SH",
        "source_event_key": "event-discovery",
    }])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery", "resolution"],
        max_events=1,
        title_max_concurrency=2,
        pipeline={"mode": "async", "llm_concurrency": 2},
        dry_run=False,
    )

    assert result["status"] == "success"
    assert ordering == ["analysis_started", "discovery_returned"]
    assert result["stages"]["resolution"]["title_overlap"] == {
        "enabled": True,
        "event_count": 1,
        "run_count": 1,
    }
    assert manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-discovery"]


@pytest.mark.asyncio
async def test_async_governance_merges_overlapped_resolution_targets():
    manager = DataManager()
    manager.db_ops = Mock()
    initial = [
        _state(f"event-{index}", "discovery_pending")
        for index in range(1, 4)
    ]
    terminal = [
        _state(f"event-{index}", "resolved_evidence", terminal=True)
        for index in range(1, 4)
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[initial, terminal, terminal]
    )

    async def discover(**kwargs):
        for index in range(1, 4):
            await kwargs["on_event_ready"]({
                "instrument_id": "600108.SH",
                "source_event_key": f"event-{index}",
                "candidate_count": 1,
            })
        return {
            "status": "success",
            "target_samples": [
                {
                    "source_event_key": f"event-{index}",
                    "candidate_count": 1,
                }
                for index in range(1, 4)
            ],
            "errors": [],
        }

    async def analyze(**kwargs):
        event_keys = kwargs["source_event_keys"]
        return {
            "status": "success",
            "counts": {"processed": len(event_keys)},
            "targets": {"batch_events": len(event_keys)},
            "errors": [],
        }

    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        side_effect=discover
    )
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        side_effect=analyze
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 3, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery", "resolution"],
        max_events=3,
        title_max_concurrency=2,
        pipeline={"mode": "async", "llm_concurrency": 2},
        dry_run=False,
    )

    resolution = result["stages"]["resolution"]
    assert result["status"] == "success"
    assert resolution["targets"]["batch_events"] == 3
    assert resolution["counts"]["processed"] == 3
    assert resolution["title_overlap"] == {
        "enabled": True,
        "event_count": 3,
        "run_count": 2,
    }
    assert [
        run["batch_events"] for run in resolution["pipeline_runs"]
    ] == [2, 1]


@pytest.mark.asyncio
async def test_governance_dry_run_does_not_persist_state():
    manager = DataManager()
    manager.db_ops = Mock()
    inventory = [_state("event-1", "discovery_pending")]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[inventory, inventory]
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock()

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory"],
        max_events=1,
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    manager.db_ops.upsert_corporate_action_resolution_states.assert_not_awaited()


@pytest.mark.asyncio
async def test_discovery_only_scope_does_not_consume_resolution_candidates():
    manager = DataManager()
    manager.db_ops = Mock()
    inventory = [
        _state("event-discovery", "discovery_pending"),
        _state(
            "event-candidate",
            "candidate_pending_analysis",
            candidate_count=1,
        ),
    ]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[inventory, inventory]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={"status": "dry_run", "target_samples": [], "errors": []}
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert result["targets"]["processable_events"] == 1
    assert result["targets"]["batch_event_keys"] == ["event-discovery"]


@pytest.mark.asyncio
async def test_evidence_unavailable_is_skipped_until_retry_is_requested():
    manager = DataManager()
    manager.db_ops = Mock()
    unavailable = [_state(
        "event-empty",
        "evidence_unavailable",
        next_action="retry_discovery",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[unavailable, unavailable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock()

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert result["targets"]["processable_events"] == 0
    manager.discover_cninfo_special_action_effective_dates.assert_not_awaited()


@pytest.mark.asyncio
async def test_evidence_unavailable_can_be_explicitly_retried():
    manager = DataManager()
    manager.db_ops = Mock()
    unavailable = [_state(
        "event-empty",
        "evidence_unavailable",
        next_action="retry_discovery",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[unavailable, unavailable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "dry_run",
            "target_samples": [],
            "skipped_samples": [],
            "errors": [],
        }
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        retry_evidence_unavailable=True,
        dry_run=True,
    )

    assert result["targets"]["batch_event_keys"] == ["event-empty"]
    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-empty"]


@pytest.mark.asyncio
async def test_retryable_discovery_error_reenters_discovery_stage():
    manager = DataManager()
    manager.db_ops = Mock()
    retryable = [_state(
        "event-error",
        "retryable_error",
        next_action="retry_failed_stage",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[retryable, retryable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "dry_run",
            "target_samples": [],
            "skipped_samples": [],
            "errors": [],
        }
    )

    await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        max_events=1,
        dry_run=True,
    )

    assert manager.discover_cninfo_special_action_effective_dates.await_args.kwargs[
        "source_event_keys"
    ] == ["event-error"]


@pytest.mark.asyncio
async def test_inventory_preserves_prior_evidence_unavailable_state():
    manager = DataManager()
    manager.db_ops = Mock()
    prior_attempt = "2026-07-20T08:30:00"
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [_row(source_event_key="event-empty")],
        [],
        [],
        [],
        [],
        [{
            "source_event_key": "event-empty",
            "resolution_state": "evidence_unavailable",
            "state_reason": "completed_scan_selected_no_matching_announcement",
            "next_action": "retry_discovery",
            "last_attempt_at": prior_attempt,
        }],
    ])

    inventory = await manager._load_cninfo_resolution_governance_inventory(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        exchanges=["SSE"],
    )

    assert inventory[0]["resolution_state"] == "evidence_unavailable"
    assert inventory[0]["next_action"] == "retry_discovery"
    assert inventory[0]["last_attempt_at"] == datetime(2026, 7, 20, 8, 30)
    assert inventory[0]["diagnostics"]["prior_resolution_state"] == (
        "evidence_unavailable"
    )
    inventory_query = manager.db_ops.execute_read_query.await_args_list[0].args[0]
    assert "fiscal_period IS NULL" in inventory_query
    assert "date(created_at) BETWEEN" in inventory_query
    assert "date(updated_at) BETWEEN" in inventory_query
    resolved_query, resolved_params = (
        manager.db_ops.execute_read_query.await_args_list[2].args
    )
    assert "evidence_source IN" in resolved_query
    assert {
        value for key, value in resolved_params.items()
        if key.startswith("governed_evidence_source_")
    } == {
        "cninfo_reviewed_official_document",
        "cninfo_announcement_review",
        "cninfo_announcement",
    }


@pytest.mark.asyncio
async def test_conflicting_resolved_dates_do_not_publish_a_resolved_date():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [_row(source_event_key="event-conflict")],
        [],
        [
            {
                "id": 2,
                "source_event_key": "event-conflict",
                "effective_date": "2026-06-13",
                "date_basis": "ex_date",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "announcement-2",
            },
            {
                "id": 1,
                "source_event_key": "event-conflict",
                "effective_date": "2026-06-12",
                "date_basis": "ex_date",
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": "announcement-1",
            },
        ],
        [],
        [],
        [],
    ])

    inventory = await manager._load_cninfo_resolution_governance_inventory(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        exchanges=["SSE"],
    )

    assert inventory[0]["resolution_state"] == "conflict"
    assert inventory[0]["resolved_effective_date"] is None
    assert inventory[0]["diagnostics"][
        "conflicting_resolved_effective_dates"
    ] == ["2026-06-12", "2026-06-13"]


@pytest.mark.asyncio
async def test_successful_retry_clears_prior_retryable_error():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(side_effect=[
        [_row(source_event_key="event-retried")],
        [],
        [],
        [],
        [],
        [{
            "source_event_key": "event-retried",
            "resolution_state": "retryable_error",
            "state_reason": "announcement_timeout",
            "next_action": "retry_failed_stage",
            "last_attempt_at": "2026-07-20T08:30:00",
        }],
    ])

    inventory = await manager._load_cninfo_resolution_governance_inventory(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 21),
        exchanges=["SSE"],
        scan_status_by_event={"event-retried": "success"},
    )

    assert inventory[0]["resolution_state"] == "evidence_unavailable"
    assert inventory[0]["state_reason"] == (
        "completed_scan_selected_no_matching_announcement"
    )


@pytest.mark.asyncio
async def test_llm_candidate_loader_filters_exact_source_event_keys():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock(return_value=[])

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        source_event_keys=["event-1"],
        max_events=1,
        dry_run=True,
        llm_client=Mock(),
    )

    query, params = manager.db_ops.execute_read_query.await_args_list[0].args
    assert result["targets"]["candidate_events"] == 0
    assert "o.source_event_key IN" in query
    assert params["source_event_key_0"] == "event-1"


@pytest.mark.asyncio
async def test_factor_scope_can_rebuild_already_resolved_requested_instrument():
    manager = DataManager()
    manager.db_ops = Mock()
    resolved = [_state("event-1", "resolved_evidence", terminal=True)]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[resolved, resolved]
    )
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        return_value={"status": "dry_run"}
    )

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        instrument_ids=["600108.SH"],
        scopes=["inventory", "factors"],
        dry_run=True,
    )

    assert result["factor_rebuild"]["status"] == "dry_run"
    assert manager.rebuild_cninfo_primary_adjustment_factors.await_args.kwargs[
        "instrument_ids"
    ] == ["600108.SH"]


@pytest.mark.asyncio
async def test_governance_preserves_partial_stage_failure_as_retryable_state():
    manager = DataManager()
    manager.db_ops = Mock()
    pending = [_state("event-1", "discovery_pending")]
    retryable = [_state(
        "event-1",
        "retryable_error",
        next_action="retry_failed_stage",
    )]
    manager._load_cninfo_resolution_governance_inventory = AsyncMock(
        side_effect=[pending, retryable, retryable]
    )
    manager.discover_cninfo_special_action_effective_dates = AsyncMock(
        return_value={
            "status": "partial",
            "target_samples": [],
            "errors": [{
                "source_event_key": "event-1",
                "error": "announcement_timeout",
            }],
        }
    )
    manager.db_ops.upsert_corporate_action_resolution_states = AsyncMock(
        return_value={"inserted": 1, "changed": 0, "unchanged": 0, "failed": 0}
    )
    manager.db_ops.execute_read_query = AsyncMock(return_value=[
        {
            "instrument_id": "600108.SH",
            "source_event_key": "event-1",
        }
    ])

    result = await manager.govern_cninfo_corporate_action_resolutions(
        start_date="1990-12-19",
        end_date="2026-07-21",
        exchanges=["SSE"],
        scopes=["inventory", "discovery"],
        dry_run=False,
    )

    assert result["status"] == "partial"
    assert result["stage_failures"] == ["discovery"]
    assert result["inventory"]["state_counts"] == {"retryable_error": 1}
