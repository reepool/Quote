import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

from scheduler.tasks import ScheduledTasks, _format_futures_market_data_scheduler_report


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_futures_trading_day_governance_task_reports_warning_and_clears_active_flag():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_trading_day_governance = AsyncMock(
        return_value={
            "status": "warning",
            "target_date_expansion": {
                "status": "warning",
                "target_date_count": 1,
                "skipped_date_count": 1,
                "minimum_quality": "estimated",
            },
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(task.futures_trading_day_governance(dry_run=True))

    assert result is True
    data_manager.run_futures_trading_day_governance.assert_awaited_once_with(
        scope_id=None,
        scope_ids=None,
        exchanges=None,
        categories=None,
        instrument_ids=None,
        series_ids=None,
        series_types=None,
        start_date=None,
        end_date=None,
        dry_run=True,
    )
    assert "futures_trading_day_governance" not in task._active_tasks


def test_futures_official_calendar_backfill_task_reports_and_clears_active_flag():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_official_calendar_backfill = AsyncMock(
        return_value={
            "status": "success",
            "domain": "futures_official_trading_calendar_backfill",
            "source_profile": "exchange_official_daily_probe",
            "quality_flag": "backfilled_verified",
            "start_date": "2024-06-01",
            "end_date": "2024-06-03",
            "totals": {"rows_written": 3, "trading_days": 1, "closed_days": 2, "unresolved_dates": 0},
            "exchanges": [{"exchange": "SHFE", "rows_written": 3, "trading_days": 1, "closed_days": 2}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.futures_official_calendar_backfill(
            exchanges=["SHFE"],
            start_date="2024-06-01",
            end_date="2024-06-03",
            dry_run=False,
            max_days=3,
        )
    )

    assert result is True
    data_manager.run_futures_official_calendar_backfill.assert_awaited_once_with(
        scope_id=None,
        scope_ids=None,
        exchanges=["SHFE"],
        categories=None,
        instrument_ids=None,
        series_ids=None,
        series_types=None,
        start_date="2024-06-01",
        end_date="2024-06-03",
        dry_run=False,
        max_days=3,
    )
    assert "futures_official_calendar_backfill" not in task._active_tasks


def test_futures_official_calendar_backfill_config_has_no_note_runtime_parameter():
    config = json.loads(Path("config/05_scheduler.json").read_text(encoding="utf-8"))
    parameters = config["scheduler_config"]["jobs"]["futures_official_calendar_backfill"]["parameters"]

    assert "note" not in parameters


def test_futures_official_calendar_report_includes_failure_samples():
    report = _format_futures_market_data_scheduler_report(
        {
            "status": "blocked",
            "domain": "futures_official_trading_calendar_backfill",
            "source_profile": "exchange_official_daily_probe",
            "quality_flag": "backfilled_verified",
            "start_date": "2024-01-01",
            "end_date": "2024-01-12",
            "probe_end_date": "2024-01-12",
            "dry_run": True,
            "totals": {
                "rows_written": 0,
                "trading_days": 9,
                "closed_days": 1,
                "unresolved_dates": 1,
                "request_count": 11,
                "challenge_count": 1,
                "challenge_backoff_seconds": 20,
                "batch_pause_count": 0,
                "batch_pause_seconds": 0,
            },
            "exchanges": [
                {
                    "exchange": "GFEX",
                    "rows_written": 0,
                    "trading_days": 9,
                    "closed_days": 1,
                    "unresolved_dates": 1,
                    "future_dates_unresolved": 0,
                    "challenge_count": 1,
                    "challenge_backoff_seconds": 20,
                    "batch_pause_count": 0,
                    "batch_pause_seconds": 0,
                    "latest_verified_date": "2024-01-10",
                    "failure_samples": [
                        {
                            "trade_date": "2024-01-11",
                            "reason": "gfex_html_challenge http_status=567",
                        }
                    ],
                }
            ],
        }
    )

    assert "失败样本" in report
    assert "challenge_count: `1`" in report
    assert "challenges=1" in report
    assert "GFEX 2024-01-11: gfex_html_challenge http_status=567" in report


def test_futures_master_governance_report_includes_source_pressure_metrics():
    report = _format_futures_market_data_scheduler_report(
        {
            "status": "success",
            "domain": "futures_master_governance",
            "exchange": "GFEX",
            "source_profile": "exchange_official_daily_contract_discovery",
            "start_date": "2025-01-01",
            "end_date": "2025-01-10",
            "dry_run": True,
            "calendar": {
                "verified_trading_days": 5,
                "first_trade_date": "2025-01-02",
                "last_trade_date": "2025-01-08",
            },
            "counts": {
                "instruments": 3,
                "series": 3,
                "contracts_discovered": 6,
                "contracts_written": 0,
                "would_write_contracts": 6,
                "official_request_count": 5,
                "challenge_count": 2,
                "challenge_backoff_seconds": 20,
                "batch_pause_count": 1,
                "batch_pause_seconds": 10,
                "retry_backoff_count": 1,
                "retry_backoff_seconds": 0.5,
            },
            "contracts": [],
            "warnings": [],
            "blockers": [],
        }
    )

    assert "challenge_count: `2`" in report
    assert "challenge_backoff_seconds: `20`" in report
    assert "batch_pause_count: `1`" in report
    assert "retry_backoff_seconds: `0.5`" in report


def test_futures_market_data_sync_stops_when_governance_blocks_production():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_trading_day_governance = AsyncMock(
        return_value={
            "status": "blocked",
            "target_date_expansion": {
                "status": "blocked",
                "blockers": ["calendar_quality_below_threshold:SHFE"],
                "target_date_count": 1,
                "skipped_date_count": 0,
            },
        }
    )
    data_manager.run_futures_market_data_sync = AsyncMock(return_value={"status": "success"})
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(task.futures_market_data_sync(dry_run=False))

    assert result is False
    data_manager.run_futures_market_data_sync.assert_not_awaited()
    assert "futures_market_data_sync" not in task._active_tasks


def test_futures_market_data_sync_allows_dry_run_with_governance_warning():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_trading_day_governance = AsyncMock(
        return_value={
            "status": "blocked",
            "target_date_expansion": {
                "status": "blocked",
                "blockers": ["estimated_calendar:SHFE"],
                "target_date_count": 1,
                "skipped_date_count": 0,
            },
        }
    )
    data_manager.run_futures_market_data_sync = AsyncMock(
        return_value={
            "status": "success",
            "totals": {"inserted": 0, "changed": 0, "unchanged": 1, "failed": 0},
            "trading_day_governance": {"status": "warning", "target_date_count": 1},
            "series": [{"series_id": "CNF.CU.SHFE.main", "status": "success", "fetched_rows": 1}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(task.futures_market_data_sync(dry_run=True))

    assert result is True
    data_manager.run_futures_market_data_sync.assert_awaited_once()
