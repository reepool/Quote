import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
    _format_a_share_canonical_factor_promotion_report,
    _format_a_share_canonical_factor_selection_report,
    _format_a_share_canonical_storage_report,
    _format_a_share_factor_rebuild_report,
    _format_cninfo_primary_factor_report,
    data_manager,
)


def test_factor_rebuild_report_contains_quality_evidence():
    content = _format_a_share_factor_rebuild_report({
        "status": "partial",
        "dry_run": False,
        "checkpoint_id": "unit",
        "parameters": {
            "start_date": "1990-12-19",
            "end_date": "2026-07-16",
            "exchanges": ["SSE", "SZSE", "BSE"],
            "source": "akshare",
        },
        "observations": {"completed_instruments": 1},
        "canonical": {
            "row_count": 1,
            "event_reconciliation": {
                "exact_matches": 1,
                "shifted_matches": 2,
                "factor_conflicts": 3,
                "candidate_only": 4,
                "tdx_only": 5,
            },
            "tdx_adjusted_price_comparison": {
                "max_adjusted_price_error_pct": 0.2,
            },
            "legacy_adjusted_price_comparison": {
                "max_adjusted_price_error_pct": 20.1,
            },
            "quality_gates": {"coverage": True, "tdx_event_reconciliation": False},
            "promotion_eligible": False,
        },
    })

    assert "shifted_matches=2" in content
    assert "legacy_max=20.1" in content
    assert "tdx_event_reconciliation=False" in content
    assert "可切换生产: `False`" in content


def test_factor_rebuild_dry_run_report_shows_planned_universe_without_fake_work():
    content = _format_a_share_factor_rebuild_report({
        "status": "dry_run",
        "dry_run": True,
        "checkpoint_id": "unit",
        "parameters": {
            "start_date": "1990-12-19",
            "end_date": "2026-07-16",
            "exchanges": ["SSE", "SZSE"],
            "source": "akshare",
        },
        "universe": {
            "instrument_count": 2,
            "completed_count": 0,
            "pending_count": 2,
        },
        "observations": {"existing_rows": 0, "existing_instruments": 0},
    })

    assert "结论: *预演完成*" in content
    assert "instrument_count=2" in content
    assert "pending_count=2" in content
    assert "外部请求: `0`" in content
    assert "标准序列" not in content


def test_three_source_selection_job_is_manual_dry_run_first():
    config = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )
    job = config["scheduler_config"]["jobs"][
        "a_share_canonical_adjustment_factor_selection"
    ]

    assert job["manual_only"] is True
    assert job["parameters"]["dry_run"] is True
    assert "backfill_sina" not in job["parameters"]
    assert "resume" not in job["parameters"]
    assert "checkpoint_id" not in job["parameters"]
    assert job["parameters"]["build_canonical"] is True
    assert job["parameters"]["exchanges"] == ["SSE", "SZSE"]
    assert job["max_instances"] == 1


def test_canonical_promotion_job_is_manual_and_dry_run_first():
    config = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )
    job = config["scheduler_config"]["jobs"][
        "a_share_canonical_adjustment_factor_promotion"
    ]

    assert job["manual_only"] is True
    assert job["parameters"]["dry_run"] is True
    assert job["parameters"]["confirm"] is False
    assert job["parameters"]["activate_reads"] is True


def test_canonical_storage_job_is_manual_preview_first():
    config = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )
    jobs = config["scheduler_config"]["jobs"]

    assert jobs["a_share_adjustment_factor_rebuild"]["enabled"] is False
    maintenance = jobs[
        "a_share_canonical_adjustment_factor_storage_maintenance"
    ]
    assert maintenance["manual_only"] is True
    assert maintenance["parameters"]["dry_run"] is True
    assert maintenance["parameters"]["confirm"] is False


def test_canonical_storage_report_is_bounded():
    content = _format_a_share_canonical_storage_report({
        "status": "dry_run",
        "maintenance_operation": "retention",
        "dry_run": True,
        "confirmed": False,
        "active_series_version": "v1",
        "candidate_counts": {"series_statuses": 30, "report_bytes": 1000},
        "candidate_versions": [f"v1__staging__{index}" for index in range(30)],
    })

    assert "series_statuses=30" in content
    assert "v1__staging__19" in content
    assert "v1__staging__20" not in content


def test_canonical_promotion_report_separates_preflight_and_activation():
    content = _format_a_share_canonical_factor_promotion_report({
        "status": "partial",
        "action": "promote",
        "dry_run": False,
        "confirmed": True,
        "parameters": {
            "staging_series_version": "v1__staging__unit",
            "target_series_version": "v1",
        },
        "preflight": {
            "eligible": True,
            "canonical_row_count": 10,
            "instrument_status_count": 2,
            "complete_with_events": 2,
            "complete_no_events": 0,
            "freshness": {
                "eligible": True,
                "candidate_end_date": "2026-07-31",
                "latest_completed_sessions": {
                    "SSE": "2026-07-31",
                    "SZSE": "2026-07-31",
                },
            },
        },
        "promotion": {"canonical_rows": 10, "instrument_statuses": 2},
        "errors": ["activation failed"],
    })

    assert "预检通过: `True`" in content
    assert "原子晋级: `rows=10, instruments=2`" in content
    assert "阻塞或错误:" in content
    assert "activation failed" in content


@pytest.mark.asyncio
async def test_scheduler_canonical_promotion_delegates_confirmation(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    promote = AsyncMock(return_value={
        "status": "dry_run",
        "action": "promote",
        "dry_run": True,
        "confirmed": False,
        "parameters": {},
        "preflight": {"eligible": True},
    })
    monkeypatch.setattr(
        data_manager,
        "promote_a_share_canonical_adjustment_factor_candidate",
        promote,
    )

    result = await task.a_share_canonical_adjustment_factor_promotion(
        staging_series_version="v1__staging__unit",
        target_series_version="v1",
        dry_run=True,
        confirm=False,
    )

    assert result["status"] == "dry_run"
    assert promote.await_args.kwargs["staging_series_version"] == (
        "v1__staging__unit"
    )
    assert promote.await_args.kwargs["confirm"] is False
    assert (
        "a_share_canonical_adjustment_factor_promotion"
        not in task._active_tasks
    )


@pytest.mark.asyncio
async def test_scheduler_canonical_storage_delegates_confirmation(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    maintain = AsyncMock(return_value={
        "status": "dry_run",
        "maintenance_operation": "retention",
        "dry_run": True,
    })
    monkeypatch.setattr(
        data_manager,
        "maintain_a_share_canonical_adjustment_factor_storage",
        maintain,
    )

    result = await task.a_share_canonical_adjustment_factor_storage_maintenance(
        operation="retention",
        dry_run=False,
        confirm=True,
    )

    assert result["maintenance_operation"] == "retention"
    assert maintain.await_args.kwargs["dry_run"] is False
    assert maintain.await_args.kwargs["confirm"] is True


@pytest.mark.asyncio
async def test_scheduler_canonical_storage_reports_failure(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = True
    task._send_task_report = AsyncMock(return_value=True)
    monkeypatch.setattr(
        data_manager,
        "maintain_a_share_canonical_adjustment_factor_storage",
        AsyncMock(side_effect=RuntimeError("storage unavailable")),
    )

    result = await task.a_share_canonical_adjustment_factor_storage_maintenance(
        operation="retention",
        dry_run=False,
        confirm=True,
    )

    assert result["status"] == "failed"
    assert result["errors"] == ["storage unavailable"]
    report = task._send_task_report.await_args.kwargs
    assert report["report_data"]["status"] == "failed"
    assert report["report_data"]["result"] == result
    assert report["task_name"] == (
        "a_share_canonical_adjustment_factor_storage_maintenance"
    )


def test_three_source_selection_report_is_bounded_and_auditable():
    samples = [
        {
            "instrument_id": f"{index:06d}.SZ",
            "start_date": "1990-12-19",
            "end_date": "2026-07-29",
            "reason": "no_eligible_consensus_cninfo_fallback",
        }
        for index in range(25)
    ]
    content = _format_a_share_canonical_factor_selection_report({
        "status": "partial",
        "parameters": {
            "start_date": "1990-12-19",
            "end_date": "2026-07-29",
            "exchanges": ["SSE", "SZSE"],
            "instrument_ids": ["000001.SZ"],
        },
        "selection": {
            "source_events": {
                "cninfo_rows": 10,
                "tdx_rows": 12,
                "baostock_sina_factor_rows": 9,
                "baostock_sina_instruments": 1,
            },
            "source_selection": {
                "selection_counts": {"cninfo": 1},
                "confidence_counts": {"low": 1},
                "agreement_counts": {"cninfo__tdx": 1},
            },
            "candidate": {
                "staging_series_version": "candidate",
                "row_count": 10,
                "blocked_segment_count": 0,
                "low_confidence_segment_count": 1,
                "promotion_eligible": True,
                "pairwise_reconciliation": {
                    "cninfo__baostock_sina_composite": {
                        "exact_matches": 8,
                        "shifted_matches": 1,
                        "conflicts": 1,
                        "left_only": 2,
                        "right_only": 3,
                        "factor_difference_buckets": {
                            "le_0_01_pct": 4,
                            "0_01_to_0_1_pct": 3,
                            "0_1_to_0_5_pct": 2,
                            "0_5_to_1_pct": 1,
                            "gt_1_pct": 1,
                        },
                    },
                },
                "blocked_decisions": [{
                    "instrument_id": "600455.SH",
                    "start_date": "1990-12-19",
                    "end_date": "2026-07-29",
                    "reason": "reviewed_source_override_ineligible",
                    "confidence": "blocked",
                }],
                "reviewed_source_override_samples": [{
                    "instrument_id": "000004.SZ",
                    "start_date": "1990-12-19",
                    "end_date": "2026-07-29",
                    "selected_source": "tdx",
                    "reason": "reviewed_source_override",
                    "reviewed_source_override": {
                        "reason": (
                            "operator_reviewed_whole_lifecycle_tdx_path"
                        ),
                        "catalog_version": "unit-v1",
                    },
                }],
                "conflict_samples": samples,
            },
        },
    })

    assert "生产表影响: `无" in content
    assert "数据获取: `local_only`" in content
    assert "BaoStock_Sina composite events=9" in content
    assert "cninfo=1" in content
    assert "low=1" in content
    assert "cninfo__tdx=1" in content
    assert (
        "cninfo__BaoStock_Sina composite: exact=8, shifted=1"
        in content
    )
    assert "gt_1_pct=1" in content
    assert "自动晋级生产: `False`" in content
    assert "硬阻塞明细:" in content
    assert "600455.SH" in content
    assert "人工全生命周期来源覆盖:" in content
    assert "000004.SZ" in content
    assert "catalog=unit-v1" in content
    assert content.index("硬阻塞明细:") < content.index(
        "低置信与历史单源样本:"
    )
    assert "000019.SZ" in content
    assert "000020.SZ" not in content


@pytest.mark.asyncio
async def test_scheduler_factor_rebuild_delegates_manual_parameters(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    rebuild = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(
        data_manager,
        "rebuild_a_share_adjustment_factor_governance",
        rebuild,
    )

    with pytest.raises(RuntimeError, match="deprecated"):
        await task.a_share_adjustment_factor_rebuild(
            start_date="1990-12-19",
            end_date="2026-07-16",
            exchanges=["SZSE"],
            instrument_ids=["000001.SZ"],
            dry_run=True,
            request_interval_seconds=1.5,
        )

    rebuild.assert_not_awaited()
    assert "a_share_adjustment_factor_rebuild" not in task._active_tasks


@pytest.mark.asyncio
async def test_scheduler_three_source_selection_coordinates_without_promotion(
    monkeypatch,
):
    task = ScheduledTasks()
    task.telegram_enabled = False
    backfill = AsyncMock()
    selection = AsyncMock(return_value={
        "status": "success",
        "candidate": {"candidate_built": True, "promotion_eligible": True},
    })
    monkeypatch.setattr(
        data_manager,
        "rebuild_a_share_adjustment_factor_governance",
        backfill,
    )
    monkeypatch.setattr(
        data_manager,
        "rebuild_cninfo_primary_adjustment_factors",
        selection,
    )

    result = await task.a_share_canonical_adjustment_factor_selection(
        start_date="1990-12-19",
        end_date="2026-07-29",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=False,
        build_canonical=True,
        factor_relative_tolerance=0.0002,
    )

    assert result["status"] == "success"
    assert result["promoted"] is False
    backfill.assert_not_awaited()
    assert selection.await_args.kwargs["source_selection_mode"] == "three_source"
    assert selection.await_args.kwargs["build_canonical"] is True
    assert selection.await_args.kwargs["factor_relative_tolerance"] == 0.0002
    assert (
        "a_share_canonical_adjustment_factor_selection"
        not in task._active_tasks
    )


@pytest.mark.asyncio
async def test_scheduler_reports_partial_when_local_selection_is_partial(
    monkeypatch,
):
    task = ScheduledTasks()
    task.telegram_enabled = False
    selection = AsyncMock(return_value={
        "status": "partial",
        "candidate": {"candidate_built": True},
    })
    monkeypatch.setattr(
        data_manager,
        "rebuild_cninfo_primary_adjustment_factors",
        selection,
    )

    result = await task.a_share_canonical_adjustment_factor_selection(
        start_date="1990-12-19",
        end_date="2026-07-29",
        dry_run=False,
    )

    assert result["status"] == "partial"


@pytest.mark.asyncio
async def test_scheduler_three_source_selection_is_local_only_dry_run(
    monkeypatch,
):
    task = ScheduledTasks()
    task.telegram_enabled = False
    backfill = AsyncMock()
    selection = AsyncMock(return_value={
        "status": "dry_run",
        "candidate": {"candidate_built": True},
    })
    monkeypatch.setattr(
        data_manager,
        "rebuild_a_share_adjustment_factor_governance",
        backfill,
    )
    monkeypatch.setattr(
        data_manager,
        "rebuild_cninfo_primary_adjustment_factors",
        selection,
    )

    result = await task.a_share_canonical_adjustment_factor_selection(
        start_date="1990-12-19",
        end_date="2026-07-29",
    )

    assert result["status"] == "dry_run"
    backfill.assert_not_awaited()
    assert selection.await_args.kwargs["dry_run"] is True
    assert (
        selection.await_args.kwargs["factor_relative_tolerance"]
        == 0.001
    )


def test_cninfo_primary_factor_report_keeps_production_isolation_visible():
    report = _format_cninfo_primary_factor_report({
        "status": "partial",
        "operation": "a_share_cninfo_adjustment_factor_rebuild",
        "parameters": {
            "start_date": "1990-12-19",
            "end_date": "2026-07-17",
            "exchanges": ["SZSE"],
        },
        "source_events": {"cninfo_rows": 10, "tdx_rows": 12},
        "cninfo_path": {"derived_events": 8},
        "tdx_path": {"derived_events": 9},
        "reconciliation": {
            "totals": {"rounded_matches": 3, "conflicts": 1, "tdx_only": 2},
            "matching_policy": {
                "factor_relative_tolerance": 0.0001,
                "rounded_precision_policy": {
                    "version": "tdx_xdxr_observed_precision_v2"
                },
            },
        },
        "benchmark": {
            "benchmark_series_version": "benchmark",
            "source_selection_status": "deferred",
            "reference_sources": {
                "tdx_event_derived_v1": {"available_instruments": 1},
            },
        },
        "candidate": {
            "candidate_built": False,
            "promotion_eligible": False,
        },
    })

    assert "生产表影响: `无`" in report
    assert "conflicts=1" in report
    assert "rounded_matches=3" in report
    assert "policy=tdx_xdxr_observed_precision_v2" in report
    assert "factor_relative_tolerance=0.010000%" in report
    assert "主源选择: `deferred`" in report
    assert "候选构造: `未执行" in report


def test_cninfo_daily_report_reads_nested_incremental_results():
    report = _format_cninfo_primary_factor_report({
        "status": "success",
        "operation": "a_share_cninfo_primary_daily_maintenance",
        "parameters": {
            "start_date": "2026-07-15",
            "end_date": "2026-07-22",
            "exchanges": ["SSE", "SZSE", "BSE"],
            "cninfo_exchanges": ["SSE", "SZSE"],
            "cninfo_excluded_exchanges": ["BSE"],
        },
        "candidate_discovery": {
            "status": "success",
            "candidate_count": 12,
            "deferred_count": 0,
            "announcement_scan": {
                "announcements_seen": 88,
                "carryover_revalidation": {
                    "policy_version": (
                        "cninfo_corporate_action_daily_title_trigger_v3"
                    ),
                    "evaluated": 5,
                    "excluded": 5,
                    "cleared_candidate_instruments": 5,
                },
            },
        },
        "cninfo_refresh": {
            "counters": {
                "requested_instruments": 12,
                "observations_inserted": 2,
                "observations_changed": 3,
                "observations_unchanged": 7,
                "observations_retired": 1,
            },
            "endpoint_metrics": {
                "target_counts": {
                    "cninfo_dividend": 10,
                    "cninfo_allotment": 2,
                },
                "request_counts": {
                    "cninfo_dividend": 11,
                    "cninfo_allotment": 2,
                },
                "final_retry_targets": 1,
                "final_retry_recovered": 1,
            },
            "adaptive_throttle": {
                "http_403_count": 3,
                "http_429_count": 0,
                "adaptive_wait_seconds": 12.5,
                "short_cooldown_count": 2,
                "circuit_trip_count": 1,
                "circuit_wait_seconds": 60,
            },
            "errors": [],
        },
        "bse_official_refresh": {
            "status": "success",
            "coverage_scope": "recent_window_only",
            "full_history_complete": False,
            "requested_start_date": "2026-07-21",
            "requested_end_date": "2026-07-22",
            "matched_announcement_count": 2,
            "parsed_event_count": 1,
            "parse_partial_count": 0,
            "scan": {"pages_scanned": 2},
        },
        "tdx_refresh": {
            "refresh_mode": "targeted",
            "target_scope": {
                "instrument_count": 149,
                "rotating_sample_count": 100,
            },
            "totals": {
                "processed_instruments": 5533,
                "raw_events": 4,
                "errors": 0,
                "timeouts": 0,
            }
        },
        "affected_instruments": {
            "count": 5,
            "cninfo_count": 4,
            "tdx_count": 2,
        },
        "factor_rebuild": {
            "status": "partial",
            "source_events": {"cninfo_rows": 40, "tdx_rows": 45},
            "cninfo_path": {"derived_events": 38},
            "tdx_path": {"derived_events": 44},
            "reconciliation": {"totals": {"exact_matches": 35}},
            "candidate": {"candidate_built": False},
        },
        "data_readiness": {
            "status": "partial",
            "pending_factor_events": 2,
            "cninfo": {
                "status": "success",
                "pending_factor_events": 0,
                "incomplete_instruments": 0,
            },
            "tdx_reference": {
                "status": "partial",
                "pending_factor_events": 2,
                "incomplete_instruments": 1,
            },
            "reconciliation": {
                "status": "partial",
                "incomplete_instruments": 1,
            },
            "overall_incomplete_instruments": 1,
        },
        "anomaly_governance": {
            "execution_status": "success",
            "readiness_status": "partial",
            "candidate_event_count": 3,
            "selected_event_count": 2,
            "deferred_event_count": 1,
            "unmatched_special_announcement_count": 1,
            "reason_counts": {
                "exceptional_implementation_title": 2,
                "incomplete_structured_event": 1,
            },
            "deferred_special_announcements_by_instrument": {
                "600000.SH": [{
                    "title": "重整计划资本公积金转增股本实施公告",
                }],
            },
            "llm": {
                "counts": {
                    "processed": 2,
                    "analyzed": 2,
                    "errors": 0,
                    "document_failures": 0,
                },
                "auto_promotion": {"promoted": 1},
                "review_workload": {"remaining_manual_review": 1},
            },
            "announcement_only_triage": {
                "mode": "shadow",
                "execution_status": "success",
                "case_count": 2,
                "processed_case_count": 1,
                "announcement_count": 3,
                "routing_counts": {
                    "active_probable_xdxr": 1,
                    "active_uncertain": 1,
                    "active_pending": 0,
                    "inactive_watch": 0,
                },
                "reactivated_case_count": 1,
                "primary_evidence_change_count": 1,
                "error_count": 0,
            },
        },
        "execution_status": {
            "primary": "success",
            "bse_official": "success",
            "tdx_reference": "partial",
            "reconciliation": "partial",
        },
        "canonical_maintenance": {
            "status": "partial",
            "active_series_version": "v1",
            "scope_instrument_count": 5,
            "blocker_reason": "predecessor_watermark_stale",
            "workflow_deferred": True,
            "actionable_retry_count": 0,
            "predecessor": {
                "reason": "predecessor_watermark_stale",
                "required_through": "2026-07-22",
                "successful_through_by_exchange": {
                    "SSE": "2026-07-21",
                },
            },
        },
        "factor_retry_state": {
            "status": "success",
            "actionable_retry_count": 2,
        },
        "stage_durations": {
            "candidate_discovery_seconds": 2,
            "cninfo_refresh_seconds": 10,
            "bse_official_refresh_seconds": 1.5,
            "tdx_refresh_seconds": 3,
            "factor_rebuild_seconds": 1,
            "anomaly_llm_seconds": 4,
            "total_seconds": 20,
        },
    })

    assert "A 股公司行动增量日更" in report
    assert "selected=12" in report
    assert "requested=12" in report
    assert "processed=5533" in report
    assert "dividend targets=10 requests=11" in report
    assert "403=3" in report
    assert "circuits=1" in report
    assert "BSE官方近期证据: `status=success" in report
    assert "events=1, partial=0, full_history=False" in report
    assert "mode=targeted, targets=149, rotation=100" in report
    assert "cninfo_primary=success, bse_official=success" in report
    assert "discovery=2.0s, cninfo=10.0s, bse=1.5s" in report
    assert "total=5" in report
    assert "CNInfo事件: `40`" in report
    assert "TDX因子: `44`" in report
    assert "CNInfo就绪度: `status=success, pending_factors=0" in report
    assert "TDX参考路径: `status=partial, pending_factors=2" in report
    assert "跨源对账: `status=partial, incomplete_instruments=1" in report
    assert "execution=success, readiness=partial, candidates=3" in report
    assert (
        "reasons=exceptional_implementation_title:2,"
        "incomplete_structured_event:1"
    ) in report
    assert "processed=2, analyzed=2, promoted=1, manual=1" in report
    assert "公告语义分流: `mode=shadow, execution=success, cases=2" in report
    assert "processed=1, announcements=3, probable=1, uncertain=1" in report
    assert "pending=0, inactive=0, reactivated=1, primary_changes=1" in report
    assert "reason=predecessor_watermark_stale" in report
    assert "workflow_deferred=True, actionable_retry=0" in report
    assert "因子重试队列: `status=success, actionable=2`" in report
    assert "600000.SH:重整计划资本公积金转增股本实施公告" in report
    assert "公告待办重验:" in report
    assert "evaluated=5, excluded=5, rerouted_structured=0" in report


@pytest.mark.asyncio
async def test_scheduler_cninfo_primary_rebuild_delegates_manual_parameters(monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    rebuild = AsyncMock(return_value={"status": "dry_run", "dry_run": True})
    monkeypatch.setattr(
        data_manager,
        "rebuild_cninfo_primary_adjustment_factors",
        rebuild,
    )

    result = await task.a_share_cninfo_adjustment_factor_rebuild(
        start_date="1990-12-19",
        end_date="2026-07-17",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
        field_tolerance=0.001,
        factor_relative_tolerance=0.0002,
    )

    assert result["status"] == "dry_run"
    assert rebuild.await_args.kwargs["instrument_ids"] == ["000001.SZ"]
    assert rebuild.await_args.kwargs["field_tolerance"] == 0.001
    assert rebuild.await_args.kwargs["factor_relative_tolerance"] == 0.0002
    assert rebuild.await_args.kwargs["build_canonical"] is False
    assert "a_share_cninfo_adjustment_factor_rebuild" not in task._active_tasks
