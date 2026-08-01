from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager


def _anomaly_kwargs(**overrides):
    values = {
        "start_date": date(2026, 7, 28),
        "end_date": date(2026, 7, 29),
        "exchanges": ["SSE", "SZSE"],
        "cninfo_result": {"observed_event_keys": []},
        "announcement_governance_context": None,
        "rebuild_result": {"reconciliation": {"conflicts": []}},
        "enabled": True,
        "max_events": 50,
        "profile": "semantic_extraction",
        "download_documents": True,
        "run_ocr": False,
        "auto_promote_validated": True,
        "title_max_concurrency": 50,
        "pipeline_mode": "async",
        "pipeline_llm_concurrency": 50,
        "pipeline_download_concurrency": 8,
        "pipeline_document_parse_concurrency": 8,
        "pipeline_progress_interval_seconds": 30,
    }
    values.update(overrides)
    return values


@pytest.mark.asyncio
async def test_daily_anomaly_governance_bypasses_ordinary_complete_event():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "ordinary",
        "quality_status": "structured_complete",
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["ordinary"]},
    ))

    assert result["execution_status"] == "skipped"
    assert result["candidate_event_count"] == 0
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_anomaly_governance_ignores_uncommitted_observed_event():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock()
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={
            "observed_event_keys": ["event-not-committed"],
            "persisted_event_keys": [],
        },
    ))

    assert result["execution_status"] == "skipped"
    manager._load_cninfo_daily_semantic_events.assert_not_awaited()
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_anomaly_governance_can_be_disabled():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "partial",
        "quality_status": "partial_missing_ex_date",
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["partial"]},
        enabled=False,
    ))

    assert result["execution_status"] == "disabled"
    assert result["readiness_status"] == "partial"
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_anomaly_manual_review_does_not_fail_execution():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "partial",
        "quality_status": "partial_missing_ex_date",
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={
            "status": "partial",
            "counts": {
                "processed": 1,
                "document_failures": 0,
            },
            "auto_promotion": {"promoted": 0, "failed": 0},
            "review_workload": {"remaining_manual_review": 1},
            "targets": {"candidate_events": 1, "has_more": False},
            "errors": [],
        }
    )

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["partial"]},
    ))

    assert result["execution_status"] == "success"
    assert result["readiness_status"] == "partial"
    assert result["status"] == "success"


@pytest.mark.asyncio
async def test_daily_anomaly_failure_is_deferred_and_operationally_partial():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "partial",
        "quality_status": "partial_missing_ex_date",
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        side_effect=RuntimeError("llm unavailable")
    )

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["partial"]},
    ))

    assert result["execution_status"] == "partial"
    assert result["deferred_instrument_ids"] == ["600000.SH"]


@pytest.mark.asyncio
async def test_daily_semantic_event_load_failure_preserves_retry_lineage():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(
        side_effect=RuntimeError("database unavailable")
    )
    record = SimpleNamespace(
        title="重整计划资本公积金转增股本实施公告",
        announcement_key="announcement-1",
        published_at="2026-07-29T01:00:00+00:00",
    )
    context = {
        "announcement_scan": {
            "matched_records_by_exchange": {"SSE": [record]},
            "matched_instruments_by_record": {
                "announcement-1": {"600000.SH"}
            },
        }
    }

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={
            "persisted_event_keys": ["event-1"],
            "affected_instrument_ids": ["600000.SH"],
        },
        announcement_governance_context=context,
    ))

    assert result["execution_status"] == "partial"
    assert result["deferred_source_event_keys"] == ["event-1"]
    assert result["deferred_instrument_ids"] == ["600000.SH"]
    assert result["deferred_semantic_event_keys_by_instrument"] == {
        "600000.SH": ["event-1"]
    }
    assert result["deferred_special_announcements_by_instrument"] == {
        "600000.SH": [{
            "announcement_key": "announcement-1",
            "announcement_date": "2026-07-29",
            "title": "重整计划资本公积金转增股本实施公告",
            "exceptional_markers": ["重整"],
        }]
    }
    assert result["llm"]["stage"] == "load_semantic_events"


@pytest.mark.asyncio
async def test_daily_anomaly_promotion_returns_rebuild_instruments():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "partial",
        "quality_status": "partial_missing_ex_date",
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={
            "status": "success",
            "counts": {"document_failures": 0},
            "auto_promotion": {
                "promoted": 1,
                "failed": 0,
                "promoted_source_event_keys": ["partial"],
            },
            "review_workload": {"remaining_manual_review": 0},
            "targets": {"candidate_events": 1, "has_more": False},
            "errors": [],
        }
    )

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["partial"]},
    ))

    assert result["promoted_instrument_ids"] == ["600000.SH"]


@pytest.mark.asyncio
async def test_daily_anomaly_reports_only_successfully_promoted_instrument():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[
        {
            "instrument_id": "600000.SH",
            "source_event_key": "manual-event",
            "quality_status": "partial_missing_ex_date",
        },
        {
            "instrument_id": "600001.SH",
            "source_event_key": "promoted-event",
            "quality_status": "partial_missing_ex_date",
        },
    ])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={
            "status": "partial",
            "counts": {"document_failures": 0},
            "auto_promotion": {
                "promoted": 1,
                "failed": 0,
                "promoted_source_event_keys": ["promoted-event"],
            },
            "review_workload": {"remaining_manual_review": 1},
            "targets": {"candidate_events": 2, "has_more": False},
            "errors": [],
        }
    )

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={
            "observed_event_keys": ["manual-event", "promoted-event"]
        },
    ))

    assert result["promoted_instrument_ids"] == ["600001.SH"]
    assert result["execution_status"] == "success"
    assert result["readiness_status"] == "partial"


@pytest.mark.asyncio
async def test_zero_anomaly_cap_preserves_event_backlog_as_partial():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "partial",
        "quality_status": "partial_missing_ex_date",
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["partial"]},
        max_events=0,
    ))

    assert result["readiness_status"] == "partial"
    assert result["deferred_semantic_event_keys_by_instrument"] == {
        "600000.SH": ["partial"]
    }
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmatched_special_announcement_is_deferred_without_event():
    manager = DataManager()
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock()
    record = SimpleNamespace(
        title="重整计划资本公积金转增股本实施公告",
        announcement_key="announcement-1",
        published_at="2026-07-29T01:00:00+00:00",
    )
    context = {
        "announcement_scan": {
            "matched_records_by_exchange": {"SSE": [record]},
            "matched_instruments_by_record": {
                "announcement-1": {"600000.SH"}
            },
        }
    }

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        announcement_governance_context=context,
    ))

    assert result["unmatched_instrument_ids"] == ["600000.SH"]
    assert result["readiness_status"] == "partial"
    assert result["deferred_special_announcements_by_instrument"] == {
        "600000.SH": [{
            "announcement_key": "announcement-1",
            "announcement_date": "2026-07-29",
            "title": "重整计划资本公积金转增股本实施公告",
            "exceptional_markers": ["重整"],
        }]
    }
    manager.analyze_cninfo_corporate_action_candidates.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmatched_special_workload_counts_announcements_not_instruments():
    manager = DataManager()
    records = [
        SimpleNamespace(
            title="重整计划资本公积金转增股本实施公告",
            announcement_key=f"announcement-{index}",
            published_at="2026-07-29T01:00:00+00:00",
        )
        for index in (1, 2)
    ]
    context = {
        "announcement_scan": {
            "matched_records_by_exchange": {"SSE": records},
            "matched_instruments_by_record": {
                record.announcement_key: {"600000.SH"}
                for record in records
            },
        }
    }

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        announcement_governance_context=context,
    ))

    assert result["unmatched_special_announcement_count"] == 2
    assert result["unmatched_instrument_ids"] == ["600000.SH"]


@pytest.mark.asyncio
async def test_deferred_special_lineage_selects_later_structured_event():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "special-event",
        "quality_status": "structured_complete",
        "announcement_date": date(2026, 7, 29),
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={
            "status": "success",
            "counts": {"processed": 1, "document_failures": 0},
            "auto_promotion": {"promoted": 0, "failed": 0},
            "review_workload": {"remaining_manual_review": 0},
            "targets": {"candidate_events": 1, "has_more": False},
            "errors": [],
        }
    )
    context = {
        "announcement_scan": {
            "matched_records_by_exchange": {},
            "matched_instruments_by_record": {},
            "deferred_special_announcements_by_instrument": {
                "600000.SH": [{
                    "announcement_key": "announcement-1",
                    "announcement_date": "2026-07-29",
                    "title": "重整计划资本公积金转增股本实施公告",
                    "exceptional_markers": ["重整"],
                }]
            },
        }
    }

    result = await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["special-event"]},
        announcement_governance_context=context,
    ))

    assert result["source_event_keys"] == ["special-event"]
    assert result["candidates"][0]["exceptional_markers"] == ["重整"]
    manager.analyze_cninfo_corporate_action_candidates.assert_awaited_once()


@pytest.mark.asyncio
async def test_daily_anomaly_semantic_range_includes_historical_event_anchors():
    manager = DataManager()
    manager._load_cninfo_daily_semantic_events = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "source_event_key": "historical-partial",
        "quality_status": "partial_missing_ex_date",
        "announcement_date": date(2026, 7, 29),
        "record_date": date(2012, 7, 30),
    }])
    manager.analyze_cninfo_corporate_action_candidates = AsyncMock(
        return_value={
            "status": "success",
            "counts": {"processed": 1, "document_failures": 0},
            "auto_promotion": {"promoted": 0, "failed": 0},
            "review_workload": {"remaining_manual_review": 1},
            "targets": {"candidate_events": 1, "has_more": False},
            "errors": [],
        }
    )

    await manager._govern_cninfo_daily_anomalies(**_anomaly_kwargs(
        cninfo_result={"observed_event_keys": ["historical-partial"]},
    ))

    call = manager.analyze_cninfo_corporate_action_candidates.await_args.kwargs
    assert call["start_date"] == date(2012, 7, 30)
    assert call["end_date"] == date(2026, 7, 29)


@pytest.mark.asyncio
async def test_daily_maintenance_rebuilds_after_anomaly_promotion():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
        },
        {
            "instrument_id": "600001.SH",
            "symbol": "600001",
        },
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH", "600001.SH"],
        "candidate_count": 2,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600001.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    rebuild_result = {
        "status": "success",
        "cninfo_path": {"pending_count": 0},
        "tdx_path": {"pending_count": 0},
        "reconciliation": {"status": "success", "totals": {}},
        "source_completeness": {
            "cninfo": {"status": "success"},
            "tdx_reference": {"status": "success"},
            "reconciliation": {"status": "success"},
        },
    }
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        side_effect=[rebuild_result, rebuild_result]
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager._govern_cninfo_daily_anomalies = AsyncMock(return_value={
        "status": "success",
        "execution_status": "success",
        "readiness_status": "success",
        "deferred_instrument_ids": [],
        "promoted_instrument_ids": ["600000.SH"],
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert (
        manager.rebuild_cninfo_primary_adjustment_factors.await_count == 2
    )
    assert (
        manager.rebuild_cninfo_primary_adjustment_factors.await_args_list[
            0
        ].kwargs["instrument_ids"]
        == ["600001.SH"]
    )
    assert (
        manager.rebuild_cninfo_primary_adjustment_factors.await_args_list[
            1
        ].kwargs["instrument_ids"]
        == ["600000.SH"]
    )
    assert (
        manager._load_daily_factor_cutoff_deferred_instrument_ids.await_args.args[
            0
        ]
        == ["600000.SH", "600001.SH"]
    )
    assert result["status"] == "success"


@pytest.mark.asyncio
async def test_daily_maintenance_merges_active_canonical_targeted_candidate():
    manager = DataManager()
    manager.data_config = {
        "adjustment_factor_governance": {},
    }
    manager._effective_adjustment_factor_governance = Mock(return_value=(
        {
            "read_dataset": "canonical",
            "canonical_series_version": "v1",
        },
        {"source": "runtime_manifest"},
    ))
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {"instrument_id": "600000.SH", "symbol": "600000"},
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_adjustment_factor_series_status = AsyncMock(
        return_value={
            "status": "promoted",
            "promotion_eligible": True,
        }
    )
    manager.db_ops.list_adjustment_factor_instrument_statuses = AsyncMock(
        return_value=[]
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock(return_value={})
    )
    manager.db_ops.merge_canonical_adjustment_factor_subset = AsyncMock(
        return_value={
            "canonical_rows": 2,
            "instrument_statuses": 1,
            "target_row_count": 100,
            "target_instrument_count": 1,
        }
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(
        return_value={
            "status": "success",
            "candidate_ids": ["600000.SH"],
            "candidate_count": 1,
        }
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(
        return_value={
            "status": "success",
            "affected_instrument_ids": ["600000.SH"],
        }
    )
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    base_rebuild = {
        "status": "success",
        "cninfo_path": {"pending_count": 0},
        "tdx_path": {"pending_count": 0},
        "reconciliation": {"status": "success", "totals": {}},
        "source_completeness": {
            "cninfo": {
                "status": "success",
                "incomplete_instruments": 0,
                "all_instrument_ids": [],
            },
            "tdx_reference": {
                "status": "success",
                "incomplete_instruments": 0,
                "all_instrument_ids": [],
            },
        },
        "overall_completeness": {
            "status": "success",
            "overall_incomplete_instruments": 0,
        },
    }
    targeted_rebuild = {
        **base_rebuild,
        "candidate": {
            "staging_series_version": "v1__staging__daily",
            "incremental_merge_eligible": True,
            "row_count": 2,
            "blocked_segment_count": 0,
        },
    }
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        side_effect=[base_rebuild, targeted_rebuild]
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager._govern_cninfo_daily_anomalies = AsyncMock(return_value={
        "status": "success",
        "execution_status": "success",
        "readiness_status": "success",
        "deferred_instrument_ids": [],
        "promoted_instrument_ids": [],
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert manager.rebuild_cninfo_primary_adjustment_factors.await_count == 2
    targeted_call = (
        manager.rebuild_cninfo_primary_adjustment_factors.await_args_list[
            1
        ].kwargs
    )
    assert targeted_call["source_selection_mode"] == "three_source"
    assert targeted_call["build_canonical"] is True
    manager.db_ops.merge_canonical_adjustment_factor_subset.assert_awaited_once_with(
        staging_series_version="v1__staging__daily",
        target_series_version="v1",
        expected_instrument_ids=["600000.SH"],
    )
    cninfo_args = (
        manager.backfill_a_share_cninfo_corporate_actions.await_args.kwargs
    )
    assert cninfo_args["endpoint_targets"] == [
        {
            "instrument_id": "600000.SH",
            "source_profile": "cninfo_dividend",
        },
        {
            "instrument_id": "600000.SH",
            "source_profile": "cninfo_allotment",
        },
    ]
    assert result["canonical_maintenance"]["status"] == "success"
    assert result["production_isolation"] is False


@pytest.mark.asyncio
async def test_post_promotion_rebuild_does_not_mask_initial_rebuild_failure():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {"instrument_id": "600000.SH", "symbol": "600000"},
        {"instrument_id": "600001.SH", "symbol": "600001"},
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock(return_value={})
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600001.SH"],
        "candidate_count": 1,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH", "600001.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    initial_failure = {
        "status": "failed",
        "cninfo_path": {},
        "tdx_path": {},
        "reconciliation": {},
        "source_completeness": {},
    }
    promotion_success = {
        "status": "success",
        "cninfo_path": {"pending_count": 0},
        "tdx_path": {"pending_count": 0},
        "reconciliation": {"status": "success", "totals": {}},
        "source_completeness": {},
    }
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        side_effect=[initial_failure, promotion_success]
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager._govern_cninfo_daily_anomalies = AsyncMock(return_value={
        "status": "success",
        "execution_status": "success",
        "readiness_status": "success",
        "deferred_instrument_ids": [],
        "promoted_instrument_ids": ["600000.SH"],
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert result["status"] == "partial"
    assert result["factor_rebuild"]["status"] == "success"
    assert result["factor_rebuild_phases"]["initial_status"] == "failed"
    assert (
        result["factor_rebuild_phases"]["post_promotion_status"] == "success"
    )
    assert result["factor_rebuild_phases"][
        "post_promotion_instrument_ids"
    ] == ["600000.SH"]
    assert result["factor_rebuild_phases"]["initial_readiness"]["status"] == (
        "failed"
    )
    retry_call = (
        manager.db_ops
        .replace_corporate_action_daily_factor_retry_instruments
        .await_args
    )
    assert retry_call.args[0] == ["600001.SH"]


@pytest.mark.asyncio
async def test_post_promotion_rebuild_supersedes_initial_pending_retry():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
        },
        {
            "instrument_id": "600001.SH",
            "symbol": "600001",
        },
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock(return_value={})
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH", "600001.SH"],
        "candidate_count": 2,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH", "600001.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    initial_partial = {
        "status": "partial",
        "cninfo_path": {"pending_instrument_ids": ["600000.SH"]},
        "tdx_path": {"pending_instrument_ids": []},
        "reconciliation": {},
        "source_completeness": {
            "cninfo": {
                "status": "partial",
                "incomplete_instruments": 1,
                "all_instrument_ids": ["600000.SH"],
                "instrument_ids": ["600000.SH"],
            },
            "tdx_reference": {
                "status": "success",
                "incomplete_instruments": 0,
                "all_instrument_ids": [],
                "instrument_ids": [],
            },
        },
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 1,
            "all_instrument_ids": ["600000.SH"],
            "instrument_ids": ["600000.SH"],
        },
    }
    promotion_success = {
        "status": "success",
        "cninfo_path": {"pending_instrument_ids": []},
        "tdx_path": {"pending_instrument_ids": []},
        "reconciliation": {"status": "success", "totals": {}},
        "source_completeness": {
            "cninfo": {
                "status": "success",
                "incomplete_instruments": 0,
                "all_instrument_ids": [],
                "instrument_ids": [],
            },
            "tdx_reference": {
                "status": "success",
                "incomplete_instruments": 0,
                "all_instrument_ids": [],
                "instrument_ids": [],
            },
        },
        "overall_completeness": {
            "status": "success",
            "overall_incomplete_instruments": 0,
            "all_instrument_ids": [],
            "instrument_ids": [],
        },
    }
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        side_effect=[initial_partial, promotion_success]
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager._govern_cninfo_daily_anomalies = AsyncMock(return_value={
        "status": "success",
        "execution_status": "success",
        "readiness_status": "success",
        "deferred_instrument_ids": [],
        "promoted_instrument_ids": ["600000.SH"],
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    retry_call = (
        manager.db_ops
        .replace_corporate_action_daily_factor_retry_instruments
        .await_args
    )
    assert retry_call.args[0] == []
    assert result["data_readiness"]["status"] == "success"


@pytest.mark.asyncio
async def test_post_promotion_readiness_preserves_other_initial_blockers():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {"instrument_id": "600000.SH", "symbol": "600000"},
        {"instrument_id": "600001.SH", "symbol": "600001"},
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock(return_value={})
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600001.SH"],
        "candidate_count": 1,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH", "600001.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    initial_partial = {
        "status": "partial",
        "cninfo_path": {
            "pending_count": 1,
            "pending_instrument_ids": ["600001.SH"],
            "pending": [{"instrument_id": "600001.SH"}],
        },
        "tdx_path": {
            "pending_count": 0,
            "pending_instrument_ids": [],
            "pending": [],
        },
        "reconciliation": {
            "status": "partial",
            "totals": {
                "conflicts": 1,
                "cninfo_only": 0,
                "tdx_only": 0,
            },
            "conflicts": [{"instrument_id": "600001.SH"}],
            "cninfo_only": [],
            "tdx_only": [],
        },
        "source_completeness": {
            "cninfo": {
                "status": "partial",
                "incomplete_instruments": 2,
                "all_instrument_ids": ["600000.SH", "600001.SH"],
                "instrument_ids": ["600000.SH"],
            },
            "tdx_reference": {
                "status": "success",
                "incomplete_instruments": 0,
                "instrument_ids": [],
            },
            "reconciliation": {
                "status": "partial",
                "incomplete_instruments": 1,
                "instrument_ids": ["600001.SH"],
            },
        },
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 2,
            "all_instrument_ids": ["600000.SH", "600001.SH"],
            "instrument_ids": ["600000.SH"],
        },
    }
    promotion_success = {
        "status": "success",
        "cninfo_path": {
            "pending_count": 0,
            "pending_instrument_ids": [],
            "pending": [],
        },
        "tdx_path": {
            "pending_count": 0,
            "pending_instrument_ids": [],
            "pending": [],
        },
        "reconciliation": {
            "status": "success",
            "totals": {
                "conflicts": 0,
                "cninfo_only": 0,
                "tdx_only": 0,
            },
            "conflicts": [],
            "cninfo_only": [],
            "tdx_only": [],
        },
        "source_completeness": {
            "cninfo": {
                "status": "success",
                "incomplete_instruments": 0,
                "instrument_ids": [],
            },
            "tdx_reference": {
                "status": "success",
                "incomplete_instruments": 0,
                "instrument_ids": [],
            },
            "reconciliation": {
                "status": "success",
                "incomplete_instruments": 0,
                "instrument_ids": [],
            },
        },
        "overall_completeness": {
            "status": "success",
            "overall_incomplete_instruments": 0,
            "instrument_ids": [],
        },
    }
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        side_effect=[initial_partial, promotion_success]
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager._govern_cninfo_daily_anomalies = AsyncMock(return_value={
        "status": "success",
        "execution_status": "success",
        "readiness_status": "success",
        "deferred_instrument_ids": [],
        "promoted_instrument_ids": ["600000.SH"],
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert result["factor_rebuild"]["status"] == "success"
    assert result["data_readiness"]["status"] == "partial"
    assert result["data_readiness"]["pending_factor_events"] == 1
    assert result["data_readiness"]["reconciliation_status"] == "partial"
    assert result["data_readiness"]["reconciliation_totals"]["conflicts"] == 1
    assert (
        result["data_readiness"]["overall_completeness_status"] == "partial"
    )
    assert result["data_readiness"]["overall_incomplete_instruments"] == 1
    assert result["factor_rebuild_phases"]["initial_readiness"][
        "cninfo_pending_factor_events"
    ] == 1


@pytest.mark.asyncio
async def test_cutoff_deferred_loader_includes_governed_effective_date_evidence():
    manager = DataManager()
    manager.db_ops = Mock()

    async def query_rows(query, params):
        if "corporate_action_effective_date_evidence" in query:
            assert "observation.is_current = 1" in query
            assert "evidence.resolution_status = 'resolved'" in query
            assert {
                value
                for key, value in params.items()
                if key.startswith("cutoff_evidence_source_")
            } >= {"cninfo_operator_attestation"}
            return [{"instrument_id": "600000.SH"}]
        return []

    manager.db_ops.execute_read_query = AsyncMock(side_effect=query_rows)

    result = await manager._load_daily_factor_cutoff_deferred_instrument_ids(
        ["600000.SH"],
        cutoff_date=date(2026, 7, 28),
        end_date=date(2026, 7, 29),
    )

    assert result == ["600000.SH"]


@pytest.mark.asyncio
async def test_daily_maintenance_excludes_bse_only_from_cninfo():
    manager = DataManager()
    manager.db_ops = Mock()

    async def active_instruments(exchange, **_kwargs):
        return [{
            "instrument_id": {
                "SSE": "600000.SH",
                "SZSE": "000001.SZ",
                "BSE": "920000.BJ",
            }[exchange]
        }]

    manager.db_ops.get_active_instruments = AsyncMock(side_effect=active_instruments)
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 17)
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["000001.SZ", "600000.SH"],
        "candidate_count": 2,
        "endpoint_targets": [
            {
                "instrument_id": "000001.SZ",
                "source_profile": "cninfo_dividend",
            },
            {
                "instrument_id": "600000.SH",
                "source_profile": "cninfo_dividend",
            },
        ],
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(
        return_value={
            "status": "success",
            "affected_instrument_ids": ["600000.SH"],
        }
    )
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": ["920000.BJ"],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        return_value={
            "status": "partial",
            "cninfo_path": {"pending_count": 1},
            "tdx_path": {"pending_count": 0},
            "reconciliation": {"status": "partial", "totals": {}},
            "overall_completeness": {
                "status": "partial",
                "overall_incomplete_instruments": 1,
            },
        }
    )

    result = await manager.maintain_a_share_cninfo_primary_factors(
        start_date="2026-07-01",
        end_date="2026-07-18",
        exchanges=["SSE", "SZSE", "BSE"],
        request_interval_seconds=0,
    )

    cninfo_args = manager.backfill_a_share_cninfo_corporate_actions.await_args.kwargs
    tdx_args = manager.backfill_tdx_xdxr_history.await_args.kwargs
    rebuild_args = manager.rebuild_cninfo_primary_adjustment_factors.await_args.kwargs
    assert cninfo_args["exchanges"] == ["SSE", "SZSE"]
    assert cninfo_args["instrument_ids"] == ["000001.SZ", "600000.SH"]
    assert cninfo_args["endpoint_targets"] == [
        {
            "instrument_id": "000001.SZ",
            "source_profile": "cninfo_dividend",
        },
        {
            "instrument_id": "600000.SH",
            "source_profile": "cninfo_dividend",
        },
    ]
    assert tdx_args["exchanges"] == ["SSE", "SZSE", "BSE"]
    assert tdx_args["instrument_ids"] == ["000001.SZ", "600000.SH", "920000.BJ"]
    assert rebuild_args["exchanges"] == ["SSE", "SZSE", "BSE"]
    assert rebuild_args["instrument_ids"] == ["600000.SH", "920000.BJ"]
    assert result["status"] == "success"
    assert result["data_readiness"]["status"] == "partial"
    assert result["affected_instruments"]["count"] == 2
    assert result["parameters"]["cninfo_excluded_exchanges"] == ["BSE"]
    assert result["parameters"]["tdx_effective_refresh_mode"] == "targeted"
    assert result["tdx_refresh"]["refresh_mode"] == "targeted"
    assert result["execution_status"]["primary"] == "success"
    assert result["cninfo_refresh"]["source_coverage"]["excluded_reason"] == (
        "source_not_supported"
    )


@pytest.mark.asyncio
async def test_bse_only_daily_maintenance_skips_cninfo_but_runs_tdx():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(
        return_value=[{"instrument_id": "920000.BJ"}]
    )
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 17)
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock()
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(
        return_value={"status": "success"}
    )

    result = await manager.maintain_a_share_cninfo_primary_factors(
        start_date="2026-07-01",
        end_date="2026-07-18",
        exchanges=["BSE"],
    )

    manager.backfill_a_share_cninfo_corporate_actions.assert_not_awaited()
    manager.backfill_tdx_xdxr_history.assert_awaited_once()
    manager.rebuild_cninfo_primary_adjustment_factors.assert_not_awaited()
    assert result["status"] == "success"
    assert result["cninfo_refresh"]["status"] == "skipped"
    assert result["factor_rebuild"]["status"] == "skipped"
    assert result["parameters"]["tdx_exchanges"] == ["BSE"]


@pytest.mark.asyncio
async def test_daily_maintenance_propagates_discovery_partial_operational_status():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "symbol": "600000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 21)
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "partial",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
        "candidates": [{
            "instrument_id": "600000.SH",
            "reasons": ["announcement_activity"],
        }],
        "_announcement_governance_context": {
            "announcement_scan": {},
            "pending_candidate_ids": [],
            "active_instruments": {
                "600000.SH": {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                }
            },
        },
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "partial",
        "affected_instrument_ids": [],
        "errors": [{"instrument_id": "600000.SH", "reason": "timeout"}],
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={
        "scan_states_persisted": 1,
        "audits_persisted": 0,
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock()

    result = await manager.maintain_a_share_cninfo_primary_factors(
        start_date="2026-07-15",
        end_date="2026-07-22",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert result["status"] == "partial"
    assert result["factor_rebuild"]["status"] == "skipped"
    assert (
        manager._persist_cninfo_daily_announcement_activity.call_args.kwargs[
            "pending_candidate_ids"
        ]
        == ["600000.SH"]
    )


@pytest.mark.asyncio
async def test_daily_maintenance_caps_factor_end_at_latest_common_quote_date():
    manager = DataManager()
    manager.db_ops = Mock()

    async def active_instruments(exchange, **_kwargs):
        return [{
            "instrument_id": {
                "SSE": "600000.SH",
                "SZSE": "000001.SZ",
                "BSE": "920000.BJ",
            }[exchange]
        }]

    manager.db_ops.get_active_instruments = AsyncMock(
        side_effect=active_instruments
    )
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={
            "SSE": date(2026, 7, 28),
            "SZSE": date(2026, 7, 28),
            "BSE": date(2026, 7, 28),
        }
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
        "candidates": [],
        "_announcement_governance_context": {
            "announcement_scan": {
                "deferred_factor_instrument_ids": ["000001.SZ"],
            },
            "pending_candidate_ids": [],
            "active_instruments": {
                "600000.SH": {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                },
                "000001.SZ": {
                    "instrument_id": "000001.SZ",
                    "exchange": "SZSE",
                },
            },
        },
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={
        "scan_states_persisted": 2,
        "audits_persisted": 0,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(return_value={
        "status": "partial",
        "cninfo_path": {"pending_count": 0},
        "tdx_path": {"pending_count": 3},
        "reconciliation": {"status": "partial", "totals": {}},
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 1,
        },
        "source_completeness": {
            "cninfo": {
                "status": "success",
                "incomplete_instruments": 0,
            },
            "tdx_reference": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
            "reconciliation": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
        },
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE", "SZSE", "BSE"],
        request_interval_seconds=0,
    )

    rebuild_args = manager.rebuild_cninfo_primary_adjustment_factors.await_args.kwargs
    discovery_args = (
        manager.discover_a_share_cninfo_daily_candidates.await_args.kwargs
    )
    assert rebuild_args["end_date"] == date(2026, 7, 28)
    assert rebuild_args["instrument_ids"] == ["000001.SZ", "600000.SH"]
    assert discovery_args["announcement_start_date"] == date(2026, 7, 28)
    assert (
        manager.db_ops.get_latest_stock_quote_dates_by_exchange.await_args.kwargs[
            "listed_on_or_before"
        ]
        == date(2026, 7, 28)
    )
    assert (
        manager.db_ops.get_latest_stock_quote_dates_by_exchange.await_args.kwargs[
            "completed_on_or_before"
        ]
        == date(2026, 7, 28)
    )
    assert result["status"] == "success"
    assert result["data_readiness"]["status"] == "success"
    assert result["data_readiness"]["tdx_reference"]["status"] == "partial"
    assert result["factor_cutoff"]["resolved_end_date"] == "2026-07-28"
    assert (
        manager._persist_cninfo_daily_announcement_activity.call_args.kwargs[
            "pending_factor_instrument_ids"
        ]
        == []
    )


@pytest.mark.asyncio
async def test_daily_maintenance_defers_rebuild_when_quote_cutoff_is_unavailable():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "600000.SH",
        "symbol": "600000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={}
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
    })
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock()

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    manager.rebuild_cninfo_primary_adjustment_factors.assert_not_awaited()
    assert result["status"] == "partial"
    assert result["factor_cutoff"]["resolved_end_date"] is None
    assert result["factor_rebuild"]["reason"] == "factor_cutoff_unavailable"


@pytest.mark.asyncio
async def test_bse_only_daily_maintenance_persists_factor_retry_without_scan():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "920000.BJ",
        "symbol": "920000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={"BSE": date(2026, 7, 28)}
    )
    manager.db_ops.get_corporate_action_daily_factor_retry_instrument_ids = (
        AsyncMock(return_value=["920000.BJ"])
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock(return_value={"inserted": 0, "cleared": 0, "pending": 1})
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock()
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(return_value={
        "status": "partial",
        "cninfo_path": {
            "pending_count": 0,
            "pending_instrument_ids": [],
        },
        "tdx_path": {
            "pending_count": 1,
            "pending_instrument_ids": ["920000.BJ"],
        },
        "source_completeness": {
            "cninfo": {"status": "success", "incomplete_instruments": 0},
            "tdx_reference": {"status": "partial", "incomplete_instruments": 1},
            "reconciliation": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
        },
        "reconciliation": {"status": "partial", "totals": {}},
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 1,
        },
    })

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["BSE"],
    )

    manager.rebuild_cninfo_primary_adjustment_factors.assert_awaited_once()
    retry_write = (
        manager.db_ops
        .replace_corporate_action_daily_factor_retry_instruments
        .await_args
    )
    assert retry_write.args == (["920000.BJ"],)
    assert retry_write.kwargs["scope_instrument_ids"] == ["920000.BJ"]
    assert result["factor_retry_state"]["status"] == "success"


@pytest.mark.asyncio
async def test_daily_maintenance_preserves_retry_queue_when_load_fails():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[{
        "instrument_id": "920000.BJ",
        "symbol": "920000",
    }])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={"BSE": date(2026, 7, 28)}
    )
    manager.db_ops.get_corporate_action_daily_factor_retry_instrument_ids = (
        AsyncMock(side_effect=RuntimeError("retry read failed"))
    )
    manager.db_ops.replace_corporate_action_daily_factor_retry_instruments = (
        AsyncMock()
    )
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock()
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock()

    result = await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["BSE"],
    )

    (
        manager.db_ops
        .replace_corporate_action_daily_factor_retry_instruments
        .assert_not_awaited()
    )
    assert result["status"] == "partial"
    assert result["factor_retry_state"] == {
        "status": "failed",
        "reason": "factor_retry_load_failed_queue_preserved",
        "error": "retry read failed",
    }


@pytest.mark.asyncio
async def test_daily_maintenance_persists_complete_new_pending_factor_queue():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "trading_status": 1,
        },
        {
            "instrument_id": "600001.SH",
            "symbol": "600001",
            "trading_status": 1,
        },
    ])
    manager.db_ops.get_previous_trading_day = AsyncMock(
        return_value=date(2026, 7, 28)
    )
    manager.db_ops.get_latest_stock_quote_dates_by_exchange = AsyncMock(
        return_value={"SSE": date(2026, 7, 28)}
    )
    manager._load_daily_factor_cutoff_deferred_instrument_ids = AsyncMock(
        return_value=[]
    )
    manager.discover_a_share_cninfo_daily_candidates = AsyncMock(return_value={
        "status": "success",
        "candidate_ids": ["600000.SH"],
        "candidate_count": 1,
        "candidates": [],
        "_announcement_governance_context": {
            "announcement_scan": {
                "deferred_factor_instrument_ids": ["600001.SH"],
            },
            "pending_candidate_ids": [],
            "active_instruments": {
                "600000.SH": {
                    "instrument_id": "600000.SH",
                    "exchange": "SSE",
                },
                "600001.SH": {
                    "instrument_id": "600001.SH",
                    "exchange": "SSE",
                },
            },
        },
    })
    manager._persist_cninfo_daily_announcement_activity = Mock(return_value={})
    manager.backfill_a_share_cninfo_corporate_actions = AsyncMock(return_value={
        "status": "success",
        "affected_instrument_ids": ["600000.SH"],
    })
    manager.backfill_tdx_xdxr_history = AsyncMock(return_value={
        "status": "success",
        "event_instrument_ids": [],
        "totals": {},
    })
    manager.rebuild_cninfo_primary_adjustment_factors = AsyncMock(return_value={
        "status": "partial",
        "cninfo_path": {
            "pending_count": 2,
            "pending": [{"instrument_id": "600000.SH"}],
            "pending_instrument_ids": ["600000.SH", "600001.SH"],
        },
        "tdx_path": {
            "pending_count": 0,
            "pending": [],
            "pending_instrument_ids": [],
        },
        "source_completeness": {
            "cninfo": {"status": "partial", "incomplete_instruments": 2},
            "tdx_reference": {"status": "success", "incomplete_instruments": 0},
            "reconciliation": {
                "status": "success",
                "incomplete_instruments": 0,
            },
        },
        "reconciliation": {"status": "success", "totals": {}},
        "overall_completeness": {
            "status": "partial",
            "overall_incomplete_instruments": 2,
        },
    })

    await manager.maintain_a_share_cninfo_primary_factors(
        end_date="2026-07-29",
        exchanges=["SSE"],
        request_interval_seconds=0,
    )

    assert (
        manager._persist_cninfo_daily_announcement_activity.call_args.kwargs[
            "pending_factor_instrument_ids"
        ]
        == ["600000.SH", "600001.SH"]
    )
