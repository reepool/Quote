from datetime import date
from unittest.mock import AsyncMock, Mock, patch

import pytest

from scheduler.job_config import JobConfigManager
from scheduler.tasks import ScheduledTasks, _format_instrument_master_governance_summary


def test_format_instrument_master_governance_summary_success():
    summary = _format_instrument_master_governance_summary({
        "status": "fresh",
        "action": "reused_fresh_master",
        "summary": {
            "added_instruments": 0,
            "deactivated_instruments": 0,
            "active_count": 5519,
        },
        "warnings": [],
        "errors": [],
    })

    assert "状态: fresh (reused_fresh_master)" in summary
    assert "新增: 0，停用: 0，停牌: 0，复活: 0，待复核: 0，活跃合计: 5519" in summary


def test_format_instrument_master_governance_summary_skipped_warning_and_error():
    skipped = _format_instrument_master_governance_summary({
        "status": "skipped",
        "reason": "historical_current_master_governance_skipped",
        "summary": {},
        "warnings": ["HKEX: unsupported market for instrument master governance"],
        "errors": [],
    })
    failed = _format_instrument_master_governance_summary({
        "status": "error",
        "action": "synced",
        "summary": {"added_instruments": 0, "deactivated_instruments": 0, "active_count": 0},
        "warnings": [],
        "errors": ["SSE: instrument master sync timed out after 180s"],
    })

    assert "状态: skipped (historical_current_master_governance_skipped)" in skipped
    assert "警告: HKEX: unsupported market" in skipped
    assert "状态: error (synced)" in failed
    assert "错误: SSE: instrument master sync timed out after 180s" in failed


def test_format_instrument_master_governance_summary_hkex_details():
    summary = _format_instrument_master_governance_summary({
        "status": "warning",
        "action": "synced",
        "summary": {
            "added_instruments": 0,
            "deactivated_instruments": 0,
            "suspended_instruments": 0,
            "reactivated_instruments": 1,
            "review_required": 3015,
            "active_count": 3020,
        },
        "exchanges": {
            "HKEX": {
                "mode": "audit_only",
                "official_active_count": 8,
                "official_delisted_count": 2,
                "supplemental_count": 5,
                "safe_write_preview_count": 4,
                "allowed_reactivation_count": 1,
                "allowed_suspension_count": 0,
                "source_usage": {
                    "hkex_securities_list": 8,
                    "hkexnews_active_list": 4,
                },
                "quote_availability": {
                    "no_local_quote_count": 1314,
                    "stale_local_quote_count": 1611,
                },
            }
        },
        "warnings": ["HKEX official securities-list source not configured"],
        "errors": [],
    })

    assert "HKEX: mode=audit_only，official_active=8，official_delisted=2，supplemental=5，safe_write候选=4" in summary
    assert "HKEX生命周期候选: 可复活=1，可停牌=0" in summary
    assert "HKEX源: hkex_securities_list:8，hkexnews_active_list:4" in summary
    assert "HKEX行情诊断: 无本地行情=1314，过旧=1611" in summary


def test_manual_only_job_can_omit_trigger():
    config_manager = Mock()
    scheduler_config = Mock()
    scheduler_config.jobs = {
        "hkex_instrument_master_sync": {
            "enabled": True,
            "manual_only": True,
            "description": "港股主数据同步/审计",
            "parameters": {"mode": "audit_only"},
        },
        "a_share_stock_master_sync": {
            "enabled": True,
            "manual_only": True,
            "description": "A股股票主数据同步",
            "parameters": {"exchanges": ["SSE", "SZSE", "BSE"]},
        },
        "index_master_governance_sync": {
            "enabled": True,
            "manual_only": True,
            "description": "A股指数主数据治理",
            "parameters": {"exchanges": ["SSE", "SZSE"]},
        }
    }
    scheduler_config.max_instances = 10
    scheduler_config.misfire_grace_time = 300
    scheduler_config.coalesce = True
    config_manager.get_scheduler_config.return_value = scheduler_config

    jobs = JobConfigManager(config_manager).load_job_configs()

    assert "hkex_instrument_master_sync" in jobs
    assert jobs["hkex_instrument_master_sync"].manual_only is True
    assert jobs["hkex_instrument_master_sync"].trigger is None
    assert "a_share_stock_master_sync" in jobs
    assert jobs["a_share_stock_master_sync"].manual_only is True
    assert jobs["a_share_stock_master_sync"].trigger is None
    assert "index_master_governance_sync" in jobs
    assert jobs["index_master_governance_sync"].manual_only is True
    assert jobs["index_master_governance_sync"].trigger is None


@pytest.mark.asyncio
async def test_hk_daily_data_update_passes_hkex_master_governance_job_name():
    task = ScheduledTasks.__new__(ScheduledTasks)
    task.daily_data_update = AsyncMock(return_value=True)

    success = await task.hk_daily_data_update(exchanges=["HKEX"])

    assert success is True
    task.daily_data_update.assert_awaited_once_with(
        exchanges=["HKEX"],
        wait_for_market_close=False,
        enable_trading_day_check=True,
        per_instrument_timeout_sec=None,
        progress_log_every=200,
        progress_log_interval_sec=300,
        master_governance_job_name="hk_daily_data_update",
        job_config=None,
    )


@pytest.mark.asyncio
async def test_hkex_instrument_master_sync_manual_task_runs_audit_and_reports():
    task = ScheduledTasks()
    task.telegram_enabled = False
    task._send_task_report = AsyncMock(return_value=False)

    with patch("scheduler.tasks.data_manager") as dm:
        dm.run_master_governance = AsyncMock(return_value={
            "status": "success",
            "mode": "audit_only",
            "summary": {"active_count": 3020, "review_required": 1},
            "exchanges": {
                "HKEX": {
                    "safe_write_preview_count": 2792,
                    "allowed_reactivation_count": 50,
                    "allowed_suspension_count": 130,
                    "official_suspension_count": 160,
                    "source_usage": {"hkex_securities_list": 17671},
                    "review_required_samples": [
                        {
                            "instrument_id": "08888.HK",
                            "reason": "missing",
                            "local": {"name": "REVIEW SAMPLE"},
                        }
                    ],
                }
            },
            "warnings": [],
            "errors": [],
        })

        success = await task.hkex_instrument_master_sync(mode="audit_only", timeout_sec=60)

    assert success is True
    requirement = dm.run_master_governance.await_args.args[0][0]
    assert requirement.scope == "hkex_instrument"
    assert requirement.exchanges == ["HKEX"]
    assert requirement.mode == "audit_only"
    assert requirement.options == {}
    assert requirement.timeout_sec == 60
    task._send_task_report.assert_awaited_once()


@pytest.mark.asyncio
async def test_hkex_instrument_master_sync_manual_task_passes_lifecycle_write_mode():
    task = ScheduledTasks()
    task.telegram_enabled = False
    task._send_task_report = AsyncMock(return_value=False)

    with patch("scheduler.tasks.data_manager") as dm:
        dm.run_master_governance = AsyncMock(return_value={
            "status": "success",
            "mode": "lifecycle_write",
            "summary": {"active_count": 3038, "review_required": 0},
            "exchanges": {"HKEX": {"status": "success"}},
            "warnings": [],
            "errors": [],
        })

        success = await task.hkex_instrument_master_sync(mode="lifecycle_write", timeout_sec=60)

    assert success is True
    requirement = dm.run_master_governance.await_args.args[0][0]
    assert requirement.scope == "hkex_instrument"
    assert requirement.exchanges == ["HKEX"]
    assert requirement.mode == "lifecycle_write"
    assert requirement.options == {}
    assert requirement.timeout_sec == 60
    task._send_task_report.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_share_stock_master_sync_manual_task_runs_without_daily_quotes():
    task = ScheduledTasks()
    task.telegram_enabled = False
    task._send_task_report = AsyncMock(return_value=False)

    with patch("scheduler.tasks.data_manager") as dm:
        dm.run_master_governance = AsyncMock(return_value={
            "status": "success",
            "summary": {
                "added_instruments": 1,
                "deactivated_instruments": 0,
                "active_count": 5528,
                "source_authority": {"official": 3},
            },
            "exchanges": {
                "SSE": {
                    "status": "success",
                    "after": {"active_count": 2314},
                    "added_count": 1,
                    "deactivated_count": 0,
                    "source_authority": "official",
                    "source_usage": {"sse_official": 2314},
                },
            },
            "warnings": [],
            "errors": [],
        })

        success = await task.a_share_stock_master_sync(
            exchanges=["SSE"],
            timeout_sec=180,
        )

    assert success is True
    requirement = dm.run_master_governance.await_args.args[0][0]
    assert requirement.scope == "a_share_stock"
    assert requirement.exchanges == ["SSE"]
    assert requirement.instrument_types == ["stock"]
    assert requirement.mode == "force_refresh"
    assert requirement.job_name == "a_share_stock_master_sync"
    assert requirement.job_type == "manual"
    assert requirement.timeout_sec == 180
    task._send_task_report.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "A 股股票主数据同步" in report_data["content"]
    assert "sse_official=2314" in report_data["content"]


@pytest.mark.asyncio
async def test_index_master_governance_sync_manual_task_runs_without_daily_quotes():
    task = ScheduledTasks()
    task.telegram_enabled = False
    task._send_task_report = AsyncMock(return_value=False)

    with patch("scheduler.tasks.data_manager") as dm:
        dm.run_master_governance = AsyncMock(return_value={
            "status": "warning",
            "summary": {
                "master_rows_saved": 3,
                "evidence_rows_saved": 2,
                "active_count": 128,
                "lifecycle_skip_count": 2,
                "direct_terminated_count": 1,
                "inferred_terminated_count": 1,
                "stale_no_quote_count": 0,
                "samples": [
                    {
                        "instrument_id": "480055.SZ",
                        "state": "calculation_terminated",
                        "confidence": "series_inferred",
                    }
                ],
            },
            "warnings": ["CSIndex full-list endpoint is not enabled"],
            "errors": [],
        })

        success = await task.index_master_governance_sync(
            exchanges=["SZSE"],
            timeout_sec=120,
            target_date=date(2026, 6, 13),
    )

    assert success is True
    requirement = dm.run_master_governance.await_args.args[0][0]
    assert requirement.scope == "a_share_index"
    assert requirement.exchanges == ["SZSE"]
    assert requirement.mode == "force_refresh"
    assert requirement.target_date == date(2026, 6, 13)
    assert requirement.timeout_sec == 120
    task._send_task_report.assert_awaited_once()
    report_data = task._send_task_report.await_args.kwargs["report_data"]
    assert "A 股指数主数据治理" in report_data["content"]
    assert "480055.SZ" in report_data["content"]
