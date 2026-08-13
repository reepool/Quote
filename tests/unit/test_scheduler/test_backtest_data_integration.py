import asyncio
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scheduler.dependencies import validate_integrated_backtest_stages
from research.backtest_data.rollout import BacktestRolloutPolicy
from scheduler.tasks import (
    ScheduledTasks,
    _financial_vintage_stage_report,
    _run_backtest_stage,
    _run_index_constituent_history_stage,
)
from utils.exceptions import DataSourceError, ErrorCodes
from utils.a_share_historical_backfill import (
    A_SHARE_BACKFILL_DEFAULT_SCOPES,
    normalize_a_share_backfill_parameters,
)


def test_rollout_is_disabled_by_default_and_has_bounded_controls():
    rollout = BacktestRolloutPolicy.load()
    for name in (
        "index_composition_forward",
        "security_state_forward",
        "daily_price_limits",
        "financial_filing_vintages",
        "canonical_corporate_actions",
    ):
        stage = rollout.stage(name)
        assert stage.enabled is False
        assert stage.timeout_seconds > 0
        assert stage.max_rows > 0


def test_default_historical_backfill_scopes_remain_backward_compatible():
    parameters = normalize_a_share_backfill_parameters(
        start_date="2026-01-01", end_date="2026-01-31"
    )
    assert parameters["scopes"] == list(A_SHARE_BACKFILL_DEFAULT_SCOPES)


def test_optional_backtest_scopes_are_accepted_only_when_explicit():
    parameters = normalize_a_share_backfill_parameters(
        start_date="2026-01-01",
        end_date="2026-01-31",
        scopes=["index_composition", "security_state", "price_limits", "corporate_actions"],
    )
    assert parameters["scopes"] == [
        "index_composition", "security_state", "price_limits", "corporate_actions"
    ]


def test_scheduler_validation_rejects_umbrella_backtest_cron():
    assert validate_integrated_backtest_stages({"backtest_data_full_market": object()}) == [
        "umbrella backtest-data cron is forbidden"
    ]


def test_scheduler_validation_accepts_disabled_stages_without_parent_jobs():
    assert validate_integrated_backtest_stages({}) == []


def test_backtest_stage_retries_and_returns_degraded_failure(monkeypatch):
    attempts = []
    policy = SimpleNamespace(
        enabled=True,
        timeout_seconds=1,
        retry_count=1,
        continue_on_error=True,
        freshness_hours=24,
        max_rows=50,
    )
    monkeypatch.setattr(
        "scheduler.tasks.BacktestRolloutPolicy.load",
        lambda: SimpleNamespace(stage=lambda name: policy),
    )

    def fail():
        attempts.append(1)
        raise RuntimeError("local projection failed")

    result = asyncio.run(_run_backtest_stage("canonical_corporate_actions", fail))
    assert len(attempts) == 2
    assert result["status"] == "failed"
    assert result["controls"]["attempt"] == 2
    assert result["controls"]["continue_on_error"] is True
    assert result["blockers"] == ["local projection failed"]


def test_backtest_stage_raises_when_continuation_is_disabled(monkeypatch):
    policy = SimpleNamespace(
        enabled=True,
        timeout_seconds=1,
        retry_count=0,
        continue_on_error=False,
        freshness_hours=24,
        max_rows=50,
    )
    monkeypatch.setattr(
        "scheduler.tasks.BacktestRolloutPolicy.load",
        lambda: SimpleNamespace(stage=lambda name: policy),
    )
    with pytest.raises(RuntimeError, match="backtest stage financial_filing_vintages failed"):
        asyncio.run(
            _run_backtest_stage(
                "financial_filing_vintages",
                lambda: (_ for _ in ()).throw(ValueError("readiness failed")),
            )
        )


def test_backtest_stage_timeout_is_not_retried_while_thread_is_in_flight(
    monkeypatch
):
    policy = SimpleNamespace(
        enabled=True,
        timeout_seconds=1,
        retry_count=3,
        continue_on_error=True,
        freshness_hours=24,
        max_rows=50,
    )
    monkeypatch.setattr(
        "scheduler.tasks.BacktestRolloutPolicy.load",
        lambda: SimpleNamespace(stage=lambda name: policy),
    )

    async def force_timeout(awaitable, timeout):
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr("scheduler.tasks.asyncio.wait_for", force_timeout)
    result = asyncio.run(
        _run_backtest_stage("canonical_corporate_actions", lambda: {})
    )
    assert result["status"] == "failed"
    assert result["controls"]["attempt"] == 1
    assert result["blockers"] == ["stage_timeout_in_flight_not_retried"]


def test_financial_vintage_report_preserves_explicit_parent_scope(tmp_path):
    scope = {
        "exchanges": ["SZSE"],
        "instrument_ids": ["000001.SZ"],
        "symbols": [],
        "report_periods": ["2025-12-31"],
    }
    result = _financial_vintage_stage_report(
        str(tmp_path / "missing.db"), inherited_scope=scope, dry_run=True
    )
    assert result["status"] == "dry_run"
    assert result["inherited_scope"] == scope
    assert result["network_requests"] == 0


def test_index_only_historical_scope_runs_before_stock_universe_calls(
    tmp_path, monkeypatch
):
    task = ScheduledTasks()
    task.telegram_enabled = False
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.data_config", {"data_dir": str(tmp_path)}
    )

    async def forbidden(*args, **kwargs):
        raise AssertionError("optional unavailable scope must not resolve universe/provider")

    monkeypatch.setattr(
        "scheduler.tasks.data_manager.run_master_governance", forbidden
    )
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.filter_repair_universe", forbidden
    )
    index_stage = AsyncMock(return_value={
        "stage": "index_composition",
        "status": "dry_run",
        "network_requests": 0,
        "provider_usage": [],
        "estimated_total_requests": 3,
        "blockers": [],
        "membership_readiness": "planned",
        "weight_readiness": "deferred",
    })
    monkeypatch.setattr(
        "scheduler.tasks._run_index_constituent_history_stage", index_stage
    )
    result = asyncio.run(
        task.a_share_daily_data_historical_backfill(
            start_date="2026-01-01",
            end_date="2026-01-31",
            exchanges=["SSE"],
            scopes=["index_composition"],
            instrument_ids=["000001.SH"],
            dry_run=True,
        )
    )
    assert result["status"] == "dry_run"
    assert result["stages"]["index_composition"]["network_requests"] == 0
    assert result["stages"]["index_composition"]["provider_usage"] == []
    index_stage.assert_awaited_once()
    assert index_stage.await_args.kwargs["sampling"] == "daily"
    assert index_stage.await_args.kwargs["max_queries_per_run"] == 4000
    assert index_stage.await_args.kwargs["daily_request_reserve"] == 5000


def test_index_history_parameters_are_normalized_and_forwarded(tmp_path, monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.data_config", {"data_dir": str(tmp_path)}
    )
    stage = AsyncMock(return_value={
        "status": "dry_run", "network_requests": 0, "provider_usage": [], "blockers": []
    })
    monkeypatch.setattr("scheduler.tasks._run_index_constituent_history_stage", stage)

    result = asyncio.run(task.a_share_daily_data_historical_backfill(
        start_date="2020-01-01",
        end_date="2020-01-31",
        scopes=["index_composition"],
        dry_run=True,
        index_instrument_ids="000016.sh,000300.sh",
        index_sampling="monthly",
        index_max_queries_per_run="12",
        index_daily_request_reserve="6000",
        index_checkpoint_path=str(tmp_path / "index.json"),
    ))

    assert result["status"] == "dry_run"
    kwargs = stage.await_args.kwargs
    assert kwargs["index_instrument_ids"] == ["000016.SH", "000300.SH"]
    assert kwargs["sampling"] == "monthly"
    assert kwargs["max_queries_per_run"] == 12
    assert kwargs["daily_request_reserve"] == 6000
    assert kwargs["checkpoint_path"] == str(tmp_path / "index.json")


def test_index_history_session_conflict_returns_retryable_block(tmp_path, monkeypatch):
    class FakeExecutor:
        def shutdown(self, wait=False):
            return None

    class LockedSource:
        def __init__(self, *args, **kwargs):
            self._bs_executor = FakeExecutor()

        async def initialize(self):
            raise DataSourceError(
                "Another local process already owns the BaoStock session",
                ErrorCodes.DATASOURCE_RATE_LIMIT,
            )

        async def close(self):
            return None

        async def get_historical_index_constituents(self, index_id, observation_date):
            raise AssertionError("query must not run after session conflict")

    async def calendar(*args, **kwargs):
        return [{"date": date(2026, 1, 5), "is_trading_day": True}]

    monkeypatch.setattr(
        "scheduler.tasks.data_manager.db_ops.get_trading_calendar_records", calendar
    )
    monkeypatch.setattr("scheduler.tasks.data_manager.source_factory", None)
    monkeypatch.setattr(
        "scheduler.tasks.config_manager.get",
        lambda key, default=None: {
            "baostock": {
                "enabled": True,
                "usage_state_path": str(tmp_path / "usage.json"),
                "session_lock_path": str(tmp_path / "session.lock"),
            }
        } if key == "data_sources_config" else default,
    )
    monkeypatch.setattr("scheduler.tasks._quote_database_path", lambda: str(tmp_path / "quotes.db"))
    monkeypatch.setattr("data_sources.baostock_source.BaostockSource", LockedSource)

    result = asyncio.run(_run_index_constituent_history_stage(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        index_instrument_ids=["000016.SH"],
        daily_request_reserve=5000,
        sampling="daily",
        max_queries_per_run=1,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        dry_run=False,
        resume=True,
    ))

    assert result["status"] == "blocked"
    assert result["retryable"] is True
    assert result["network_requests"] == 0
    assert "Another local process already owns" in result["blockers"][0]


def test_index_history_honors_disabled_baostock_source(tmp_path, monkeypatch):
    async def calendar(*args, **kwargs):
        return [{"date": date(2026, 1, 5), "is_trading_day": True}]

    monkeypatch.setattr(
        "scheduler.tasks.data_manager.db_ops.get_trading_calendar_records", calendar
    )
    monkeypatch.setattr(
        "scheduler.tasks.config_manager.get",
        lambda key, default=None: {
            "baostock": {
                "enabled": False,
                "usage_state_path": str(tmp_path / "usage.json"),
                "session_lock_path": str(tmp_path / "session.lock"),
            }
        } if key == "data_sources_config" else default,
    )

    result = asyncio.run(_run_index_constituent_history_stage(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        index_instrument_ids=["000016.SH"],
        daily_request_reserve=5000,
        sampling="daily",
        max_queries_per_run=1,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        dry_run=False,
        resume=True,
    ))

    assert result["status"] == "unavailable"
    assert result["blockers"] == ["baostock_source_disabled"]
    assert result["network_requests"] == 0
    assert not (tmp_path / "usage.json").exists()


def test_optional_only_scope_preserves_blocked_index_status(tmp_path, monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.data_config", {"data_dir": str(tmp_path)}
    )
    monkeypatch.setattr(
        "scheduler.tasks._run_index_constituent_history_stage",
        AsyncMock(return_value={
            "stage": "index_composition",
            "status": "blocked",
            "network_requests": 0,
            "provider_usage": [],
            "blockers": ["session_conflict"],
        }),
    )

    result = asyncio.run(task.a_share_daily_data_historical_backfill(
        start_date="2026-01-05",
        end_date="2026-01-05",
        scopes=["index_composition", "security_state"],
        dry_run=False,
    ))

    assert result["status"] == "blocked"
    assert "index_composition:session_conflict" in result["blockers"]


def test_optional_only_scope_reports_partial_index_failure(tmp_path, monkeypatch):
    task = ScheduledTasks()
    task.telegram_enabled = False
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.data_config", {"data_dir": str(tmp_path)}
    )
    monkeypatch.setattr(
        "scheduler.tasks._run_index_constituent_history_stage",
        AsyncMock(return_value={
            "stage": "index_composition",
            "status": "partial",
            "network_requests": 1,
            "provider_usage": ["baostock"],
            "blockers": ["000300.SH member count outside guardrail: 0"],
            "failures": [{
                "unit_id": "000300.SH:2005-04-08",
                "reason": "000300.SH member count outside guardrail: 0",
            }],
        }),
    )

    result = asyncio.run(task.a_share_daily_data_historical_backfill(
        start_date="2005-04-08",
        end_date="2005-04-08",
        scopes=["index_composition"],
        dry_run=False,
    ))

    assert result["status"] == "partial"
    assert result["blockers"] == [
        "index_composition:000300.SH member count outside guardrail: 0"
    ]
    assert result["failure_samples"] == [{
        "instrument_id": "000300.SH:2005-04-08",
        "reason": "000300.SH member count outside guardrail: 0",
    }]


def test_index_history_initializes_reused_baostock_source(tmp_path, monkeypatch):
    class ReusedSource:
        def __init__(self):
            self.initialize = AsyncMock()
            self.get_historical_index_constituents = AsyncMock()

    reused = ReusedSource()

    async def calendar(*args, **kwargs):
        return [{"date": date(2026, 1, 5), "is_trading_day": True}]

    monkeypatch.setattr(
        "scheduler.tasks.data_manager.db_ops.get_trading_calendar_records", calendar
    )
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.source_factory",
        SimpleNamespace(get_source_instance=lambda *args, **kwargs: reused),
    )
    monkeypatch.setattr(
        "scheduler.tasks.config_manager.get",
        lambda key, default=None: {
            "baostock": {
                "enabled": True,
                "usage_state_path": str(tmp_path / "usage.json"),
                "session_lock_path": str(tmp_path / "session.lock"),
            }
        } if key == "data_sources_config" else default,
    )
    monkeypatch.setattr("scheduler.tasks._quote_database_path", lambda: str(tmp_path / "quotes.db"))
    run = AsyncMock(return_value={
        "stage": "index_composition",
        "status": "success",
        "network_requests": 1,
        "provider_usage": ["baostock"],
        "blockers": [],
        "totals": {},
    })
    monkeypatch.setattr(
        "scheduler.tasks.CoreIndexConstituentHistoryBackfill.run", run
    )

    result = asyncio.run(_run_index_constituent_history_stage(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 5),
        index_instrument_ids=["000016.SH"],
        daily_request_reserve=5000,
        sampling="daily",
        max_queries_per_run=1,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        dry_run=False,
        resume=True,
    ))

    reused.initialize.assert_awaited_once()
    assert result["status"] == "success"


def test_index_history_blocks_on_incomplete_local_calendar(tmp_path, monkeypatch):
    async def incomplete_calendar(*args, **kwargs):
        return [
            {"date": date(2026, 1, 5), "is_trading_day": True},
            {"date": date(2026, 1, 7), "is_trading_day": True},
        ]

    monkeypatch.setattr(
        "scheduler.tasks.data_manager.db_ops.get_trading_calendar_records",
        incomplete_calendar,
    )
    monkeypatch.setattr(
        "scheduler.tasks.config_manager.get",
        lambda key, default=None: {
            "baostock": {
                "enabled": True,
                "usage_state_path": str(tmp_path / "usage.json"),
                "session_lock_path": str(tmp_path / "session.lock"),
            }
        } if key == "data_sources_config" else default,
    )

    result = asyncio.run(_run_index_constituent_history_stage(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 7),
        index_instrument_ids=["000016.SH"],
        daily_request_reserve=5000,
        sampling="daily",
        max_queries_per_run=3,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        dry_run=True,
        resume=True,
    ))

    assert result["status"] == "blocked"
    assert result["network_requests"] == 0
    assert result["calendar_coverage"]["missing_days"] == 1


def test_index_history_write_uses_partial_quota_headroom(tmp_path, monkeypatch):
    class ReusedSource:
        def __init__(self):
            self.initialize = AsyncMock()
            self.get_historical_index_constituents = AsyncMock()

    reused = ReusedSource()

    async def complete_calendar(*args, **kwargs):
        return [
            {"date": date(2026, 1, 5), "is_trading_day": True},
            {"date": date(2026, 1, 6), "is_trading_day": True},
            {"date": date(2026, 1, 7), "is_trading_day": True},
        ]

    monkeypatch.setattr(
        "scheduler.tasks.data_manager.db_ops.get_trading_calendar_records",
        complete_calendar,
    )
    monkeypatch.setattr(
        "scheduler.tasks.data_manager.source_factory",
        SimpleNamespace(get_source_instance=lambda *args, **kwargs: reused),
    )
    monkeypatch.setattr(
        "scheduler.tasks.config_manager.get",
        lambda key, default=None: {
            "baostock": {
                "enabled": True,
                "daily_request_safety_limit": 6000,
                "usage_state_path": str(tmp_path / "usage.json"),
                "session_lock_path": str(tmp_path / "session.lock"),
            }
        } if key == "data_sources_config" else default,
    )
    monkeypatch.setattr("scheduler.tasks._quote_database_path", lambda: str(tmp_path / "quotes.db"))
    run = AsyncMock(return_value={
        "stage": "index_composition",
        "status": "partial",
        "network_requests": 1,
        "provider_usage": ["baostock"],
        "blockers": ["insufficient_baostock_quota_headroom"],
        "totals": {},
    })
    monkeypatch.setattr(
        "scheduler.tasks.CoreIndexConstituentHistoryBackfill.run", run
    )

    result = asyncio.run(_run_index_constituent_history_stage(
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 7),
        index_instrument_ids=["000016.SH"],
        daily_request_reserve=5000,
        sampling="daily",
        max_queries_per_run=4000,
        checkpoint_path=str(tmp_path / "checkpoint.json"),
        dry_run=False,
        resume=True,
    ))

    reused.initialize.assert_awaited_once()
    run.assert_awaited_once()
    assert result["status"] == "partial"
