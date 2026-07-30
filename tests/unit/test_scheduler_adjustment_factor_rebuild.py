from unittest.mock import AsyncMock

import pytest

from scheduler.tasks import (
    ScheduledTasks,
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

    result = await task.a_share_adjustment_factor_rebuild(
        start_date="1990-12-19",
        end_date="2026-07-16",
        exchanges=["SZSE"],
        instrument_ids=["000001.SZ"],
        dry_run=True,
        request_interval_seconds=1.5,
    )

    assert result["status"] == "dry_run"
    assert rebuild.await_args.kwargs["instrument_ids"] == ["000001.SZ"]
    assert rebuild.await_args.kwargs["request_interval_seconds"] == 1.5
    assert "a_share_adjustment_factor_rebuild" not in task._active_tasks


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
            "announcement_scan": {"announcements_seen": 88},
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
        },
        "execution_status": {
            "primary": "success",
            "tdx_reference": "partial",
            "reconciliation": "partial",
        },
        "stage_durations": {
            "candidate_discovery_seconds": 2,
            "cninfo_refresh_seconds": 10,
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
    assert "mode=targeted, targets=149, rotation=100" in report
    assert "cninfo_primary=success, tdx_reference=partial" in report
    assert "discovery=2.0s, cninfo=10.0s" in report
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
