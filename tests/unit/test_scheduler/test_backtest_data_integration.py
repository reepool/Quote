import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scheduler.dependencies import validate_integrated_backtest_stages
from research.backtest_data.rollout import BacktestRolloutPolicy
from scheduler.tasks import (
    ScheduledTasks,
    _financial_vintage_stage_report,
    _run_backtest_stage,
)
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


def test_optional_only_historical_scope_returns_before_universe_or_provider_calls(
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
    assert result["status"] == "unavailable"
    assert result["stages"]["index_composition"]["network_requests"] == 0
    assert result["stages"]["index_composition"]["provider_usage"] == []
