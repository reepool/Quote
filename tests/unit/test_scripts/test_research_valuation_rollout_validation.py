import pytest

from scripts.research_valuation_rollout_validation import (
    exit_code_for_result,
    run_rollout_validation,
    run_rollout_validation_with_lifecycle,
)


class FakeValidationManager:
    def __init__(self, *, ready: bool = True):
        self.calls = []
        self.ready = ready
        self.initialized = False
        self.closed = False

    async def initialize(self, **kwargs):
        self.calls.append(("initialize", kwargs))
        self.initialized = True

    async def close(self):
        self.calls.append(("close", {}))
        self.closed = True

    async def run_valuation_history_rebuild(self, **kwargs):
        self.calls.append(("sync", kwargs))
        return {"status": "success", "rows_written": 24}

    async def run_valuation_input_sync(self, **kwargs):
        self.calls.append(("input_sync", kwargs))
        return {"status": "success", "snapshots_written": 2}

    async def get_research_valuation_readiness(self):
        self.calls.append(("readiness", {}))
        return {
            "ready_for_rollout": self.ready,
            "module_enabled": True,
            "valuation_history_total": 24 if self.ready else 2,
            "missing_valuation_history_count": 0 if self.ready else 22,
            "blockers": [] if self.ready else ["valuation_history_coverage_incomplete"],
            "relative_valuation": {
                "ready": self.ready,
                "blockers": [] if self.ready else ["valuation_history_coverage_incomplete"],
            },
        }


@pytest.mark.asyncio
async def test_rollout_validation_runs_sync_and_readiness_by_default():
    manager = FakeValidationManager(ready=True)

    result = await run_rollout_validation(
        manager,
        exchanges=["SSE", "SZSE"],
        limit_per_exchange=2,
        target_instrument_ids=["600000.SH"],
    )

    assert result["status"] == "ready"
    assert result["summary"]["ready_for_rollout"] is True
    assert result["input_sync"]["status"] == "skipped"
    assert [call[0] for call in manager.calls] == ["sync", "readiness"]
    assert manager.calls[0][1] == {
        "exchanges": ["SSE", "SZSE"],
        "limit_per_exchange": 2,
        "target_instrument_ids": ["600000.SH"],
        "allow_disabled_module": False,
        "quote_limit_days": None,
        "window_mode": "trading_days",
        "write_policy": "missing_only",
    }


@pytest.mark.asyncio
async def test_rollout_validation_can_sync_inputs_before_history():
    manager = FakeValidationManager(ready=True)

    result = await run_rollout_validation(
        manager,
        exchanges=["SSE"],
        limit_per_exchange=1,
        target_instrument_ids=["600000.SH"],
        sync_inputs=True,
        input_sync_mode="incremental",
    )

    assert result["input_sync"]["status"] == "success"
    assert [call[0] for call in manager.calls] == [
        "input_sync",
        "sync",
        "readiness",
    ]
    assert manager.calls[0][1] == {
        "exchanges": ["SSE"],
        "limit_per_exchange": 1,
        "target_instrument_ids": ["600000.SH"],
        "sync_mode": "incremental",
    }
    assert manager.calls[1][1] == {
        "exchanges": ["SSE"],
        "limit_per_exchange": 1,
        "target_instrument_ids": ["600000.SH"],
        "allow_disabled_module": False,
        "quote_limit_days": None,
        "window_mode": "trading_days",
        "write_policy": "missing_only",
    }


@pytest.mark.asyncio
async def test_rollout_validation_skip_sync_still_reads_readiness():
    manager = FakeValidationManager(ready=False)

    result = await run_rollout_validation(
        manager,
        skip_sync=True,
    )

    assert result["status"] == "not_ready"
    assert result["sync"]["status"] == "skipped"
    assert [call[0] for call in manager.calls] == ["readiness"]


def test_rollout_validation_exit_code_for_not_ready():
    result = {"summary": {"ready_for_rollout": False}}

    assert exit_code_for_result(result, fail_on_not_ready=False) == 0
    assert exit_code_for_result(result, fail_on_not_ready=True) == 2
    assert (
        exit_code_for_result(
            {"summary": {"ready_for_rollout": True}},
            fail_on_not_ready=True,
        )
        == 0
    )


@pytest.mark.asyncio
async def test_rollout_validation_lifecycle_closes_on_success():
    manager = FakeValidationManager(ready=True)

    result = await run_rollout_validation_with_lifecycle(
        manager,
        exchanges=["SSE"],
        skip_sync=True,
    )

    assert result["status"] == "ready"
    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == ["initialize", "readiness", "close"]
    assert manager.calls[0][1] == {
        "include_data_sources": False,
        "load_progress": False,
    }


@pytest.mark.asyncio
async def test_rollout_validation_lifecycle_closes_on_failure():
    class FailingManager(FakeValidationManager):
        async def get_research_valuation_readiness(self):
            self.calls.append(("readiness", {}))
            raise RuntimeError("readiness failed")

    manager = FailingManager()

    with pytest.raises(RuntimeError, match="readiness failed"):
        await run_rollout_validation_with_lifecycle(
            manager,
            skip_sync=True,
        )

    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == ["initialize", "readiness", "close"]
    assert manager.calls[0][1] == {
        "include_data_sources": False,
        "load_progress": False,
    }
