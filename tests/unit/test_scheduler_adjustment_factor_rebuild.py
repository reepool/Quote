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
        "reconciliation": {"totals": {"conflicts": 1, "tdx_only": 2}},
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
    assert "主源选择: `deferred`" in report
    assert "候选构造: `未执行" in report


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
    )

    assert result["status"] == "dry_run"
    assert rebuild.await_args.kwargs["instrument_ids"] == ["000001.SZ"]
    assert rebuild.await_args.kwargs["field_tolerance"] == 0.001
    assert rebuild.await_args.kwargs["build_canonical"] is False
    assert "a_share_cninfo_adjustment_factor_rebuild" not in task._active_tasks
