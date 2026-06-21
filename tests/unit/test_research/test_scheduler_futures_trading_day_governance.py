import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock

from scheduler.tasks import (
    ScheduledTasks,
    _format_futures_market_data_scheduler_report,
    _format_futures_market_data_scheduler_reports,
)


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_futures_market_data_report_distinguishes_actual_calendar_quality_from_threshold():
    report = _format_futures_market_data_scheduler_report(
        {
            "status": "success",
            "dry_run": True,
            "scope_selection": {"exchanges": ["GFEX"]},
            "totals": {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "failed": 0,
                "calendar_skipped": 433,
                "provider_empty_on_trading_day": 2044,
            },
            "trading_day_governance": {
                "status": "success",
                "target_date_count": 843,
                "skipped_date_count": 433,
                "minimum_quality": "estimated",
                "expansions": [
                    {
                        "exchange": "GFEX",
                        "quality_summary": {
                            "lowest_quality": "backfilled_verified",
                            "quality_distribution": {"backfilled_verified": 1276},
                        },
                    }
                ],
            },
            "series": [
                {
                    "series_id": "CNF.SI.GFEX.main",
                    "fetched_rows": 843,
                    "write_result": {"would_write_rows": 843},
                    "status": "success",
                }
            ],
        }
    )

    assert "calendar_quality: `backfilled_verified`" in report
    assert "calendar_min_required: `estimated`" in report
    assert "exchange/scope: `GFEX`" in report
    assert "dry_run: `True`" in report
    assert "would_write=843" in report


def test_futures_market_data_report_splits_series_details_by_exchange():
    result = {
        "status": "success",
        "dry_run": False,
        "scope_selection": {"exchanges": ["GFEX", "SHFE"]},
        "totals": {
            "inserted": 2,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
            "calendar_skipped": 0,
            "provider_empty_on_trading_day": 0,
        },
        "trading_day_governance": {
            "status": "success",
            "target_date_count": 1,
            "minimum_quality": "estimated",
            "expansions": [
                {"exchange": "GFEX", "quality_summary": {"lowest_quality": "backfilled_verified"}},
                {"exchange": "SHFE", "quality_summary": {"lowest_quality": "backfilled_verified"}},
            ],
        },
        "series": [
            {
                "series_id": "CNF.SI.GFEX.main",
                "fetched_rows": 1,
                "write_result": {"inserted": 1, "would_write_rows": 0},
                "status": "success",
            },
            {
                "series_id": "CNF.CU.SHFE.main",
                "fetched_rows": 1,
                "write_result": {"inserted": 1, "would_write_rows": 0},
                "status": "success",
            },
        ],
    }

    reports = _format_futures_market_data_scheduler_reports(result)

    assert len(reports) == 3
    assert "exchange/scope: `GFEX,SHFE`" in reports[0]
    assert "序列明细已按交易所拆分发送" in reports[0]
    assert "exchange/scope: `GFEX`" in reports[1]
    assert "CNF.SI.GFEX.main" in reports[1]
    assert "CNF.CU.SHFE.main" not in reports[1]
    assert "exchange/scope: `SHFE`" in reports[2]
    assert "CNF.CU.SHFE.main" in reports[2]
    assert "CNF.SI.GFEX.main" not in reports[2]


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


def test_futures_official_calendar_backfill_task_treats_truncated_partial_as_completed():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_official_calendar_backfill = AsyncMock(
        return_value={
            "status": "partial",
            "domain": "futures_official_trading_calendar_backfill",
            "source_profile": "exchange_official_daily_probe",
            "quality_flag": "backfilled_verified",
            "start_date": "2000-06-01",
            "end_date": "2026-06-20",
            "totals": {
                "rows_written": 0,
                "trading_days": 72,
                "closed_days": 28,
                "unresolved_dates": 0,
                "truncated_dates": 9416,
            },
            "exchanges": [{"exchange": "DCE", "truncated_dates": 9416}],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.futures_official_calendar_backfill(
            exchanges=["DCE"],
            start_date="2000-06-01",
            end_date="2026-06-20",
            dry_run=True,
            max_days=100,
        )
    )

    assert result is True
    assert "futures_official_calendar_backfill" not in task._active_tasks
    report_call = task._send_task_report.await_args.kwargs["report_data"]
    assert report_call["status"] == "success"
    assert "truncated_dates" in report_call["content"]


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


def test_futures_official_calendar_report_separates_truncated_dates_from_failures():
    report = _format_futures_market_data_scheduler_report(
        {
            "status": "partial",
            "domain": "futures_official_trading_calendar_backfill",
            "source_profile": "exchange_official_daily_probe",
            "quality_flag": "backfilled_verified",
            "start_date": "2000-06-01",
            "end_date": "2026-06-20",
            "probe_end_date": "2026-06-20",
            "dry_run": True,
            "totals": {
                "rows_written": 0,
                "trading_days": 72,
                "closed_days": 28,
                "unresolved_dates": 0,
                "truncated_dates": 9416,
                "request_count": 100,
                "rate_limit_count": 2,
                "rate_limit_backoff_seconds": 180,
            },
            "exchanges": [
                {
                    "exchange": "DCE",
                    "rows_written": 0,
                    "trading_days": 72,
                    "closed_days": 28,
                    "unresolved_dates": 0,
                    "truncated_dates": 9416,
                    "rate_limit_count": 2,
                    "rate_limit_backoff_seconds": 180,
                    "future_dates_unresolved": 0,
                    "latest_verified_date": "2000-09-08",
                    "failure_samples": [],
                }
            ],
        }
    )

    assert "状态: `partial`" in report
    assert "truncated_dates: `9416`" in report
    assert "truncated=9416" in report
    assert "rate_limit_count: `2`" in report
    assert "rate_limits=2" in report
    assert "失败样本" not in report


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


def test_futures_master_governance_report_compacts_discovery_warnings():
    warning = {
        "reason": "unmapped_gfex_varieties",
        "samples": [("PD", 646), ("PT", 646)],
        "discovery_candidates": [
            {
                "discovery_id": "GFEX:PD",
                "candidate_instrument_id": "CNF.PD.GFEX",
                "candidate_name": "GFEX Palladium",
                "candidate_category": "precious_metal",
                "candidate_unit": "CNY/gram",
                "evidence": {"large": "payload" * 500},
            },
            {
                "discovery_id": "GFEX:PT",
                "candidate_instrument_id": "CNF.PT.GFEX",
                "candidate_name": "GFEX Platinum",
                "candidate_category": "precious_metal",
                "candidate_unit": "CNY/gram",
                "evidence": {"large": "payload" * 500},
            },
        ],
    }
    report = _format_futures_market_data_scheduler_report(
        {
            "status": "warning",
            "domain": "futures_master_governance",
            "exchange": "GFEX",
            "source_profile": "exchange_official_daily_contract_discovery",
            "start_date": "2022-12-22",
            "end_date": "2026-06-19",
            "dry_run": True,
            "calendar": {
                "verified_trading_days": 843,
                "first_trade_date": "2022-12-22",
                "last_trade_date": "2026-06-18",
            },
            "counts": {
                "instruments": 3,
                "series": 3,
                "contracts_discovered": 114,
                "contracts_written": 0,
                "would_write_contracts": 114,
                "official_request_count": 843,
                "challenge_count": 7,
                "challenge_backoff_seconds": 70,
                "batch_pause_count": 4,
                "batch_pause_seconds": 40,
            },
            "contracts": [],
            "warnings": [warning],
            "blockers": [],
        }
    )

    assert len(report) < 4096
    assert "unmapped_gfex_varieties" in report
    assert "samples=[PD:646, PT:646]" in report
    assert "CNF.PD.GFEX:GFEX Palladium/precious_metal/CNY/gram" in report
    assert "payloadpayload" not in report


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


def test_futures_market_data_sync_runs_master_governance_per_exchange():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_trading_day_governance = AsyncMock(
        return_value={
            "status": "success",
            "target_date_expansion": {
                "status": "success",
                "target_dates_by_exchange": {
                    "DCE": ["2026-06-19"],
                    "GFEX": ["2026-06-19"],
                },
                "target_date_count": 2,
                "skipped_date_count": 0,
            },
        }
    )
    data_manager.run_futures_master_governance = AsyncMock(
        side_effect=[
            {"status": "success", "exchange": "DCE", "blockers": []},
            {"status": "success", "exchange": "GFEX", "blockers": []},
        ]
    )
    data_manager.run_futures_market_data_sync = AsyncMock(
        return_value={
            "status": "success",
            "totals": {"inserted": 0, "changed": 0, "unchanged": 2, "failed": 0},
            "trading_day_governance": {"status": "success", "target_date_count": 2},
            "scope_selection": {"exchanges": ["DCE", "GFEX"]},
            "series": [
                {"series_id": "CNF.I.DCE.main", "status": "success", "fetched_rows": 1},
                {"series_id": "CNF.SI.GFEX.main", "status": "success", "fetched_rows": 1},
            ],
        }
    )
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.futures_market_data_sync(
            exchanges=["DCE", "GFEX"],
            dry_run=False,
            requires_master_data_governance=True,
        )
    )

    assert result is True
    assert data_manager.run_futures_master_governance.await_count == 2
    calls = data_manager.run_futures_master_governance.await_args_list
    assert calls[0].kwargs["exchanges"] == ["DCE"]
    assert calls[0].kwargs["start_date"] == "2026-06-19"
    assert calls[0].kwargs["end_date"] == "2026-06-19"
    assert calls[1].kwargs["exchanges"] == ["GFEX"]
    data_manager.run_futures_market_data_sync.assert_awaited_once()


def test_futures_market_data_sync_blocks_when_one_exchange_master_governance_blocks():
    task = ScheduledTasks()

    from scheduler import tasks as scheduler_tasks_module

    data_manager = scheduler_tasks_module.data_manager
    data_manager.run_futures_trading_day_governance = AsyncMock(
        return_value={
            "status": "success",
            "target_date_expansion": {
                "status": "success",
                "target_dates_by_exchange": {
                    "DCE": ["2026-06-19"],
                    "GFEX": ["2026-06-19"],
                },
                "target_date_count": 2,
                "skipped_date_count": 0,
            },
        }
    )
    data_manager.run_futures_master_governance = AsyncMock(
        side_effect=[
            {"status": "blocked", "exchange": "DCE", "blockers": ["no_dce_contracts_discovered"]},
            {"status": "success", "exchange": "GFEX", "blockers": []},
        ]
    )
    data_manager.run_futures_market_data_sync = AsyncMock(return_value={"status": "success"})
    task._send_task_report = AsyncMock(return_value=True)

    result = _run(
        task.futures_market_data_sync(
            exchanges=["DCE", "GFEX"],
            dry_run=False,
            requires_master_data_governance=True,
        )
    )

    assert result is False
    assert data_manager.run_futures_master_governance.await_count == 2
    data_manager.run_futures_market_data_sync.assert_not_awaited()
    assert "futures_market_data_sync" not in task._active_tasks
