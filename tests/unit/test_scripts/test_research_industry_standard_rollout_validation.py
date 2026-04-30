import pytest

from scripts.research_industry_standard_rollout_validation import (
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

    async def run_industry_official_mapping_refresh(self, **kwargs):
        self.calls.append(("refresh", kwargs))
        return {"status": "success", "mapping_cache_rows_written": 433}

    async def run_industry_standard_sync(self, **kwargs):
        self.calls.append(("sync", kwargs))
        return {"status": "success", "total_memberships_written": 2}

    async def get_research_industry_standard_readiness(self):
        self.calls.append(("readiness", {}))
        return {
            "industry_standard_ready": self.ready,
            "blockers": [] if self.ready else ["authoritative_membership_coverage_incomplete"],
            "relative_valuation": {
                "ready": self.ready,
                "blockers": [] if self.ready else ["authoritative_membership_coverage_incomplete"],
            },
        }


@pytest.mark.asyncio
async def test_rollout_validation_runs_sync_and_readiness_by_default():
    manager = FakeValidationManager(ready=True)

    result = await run_rollout_validation(
        manager,
        exchanges=["SSE", "SZSE"],
        limit_per_exchange=2,
        budget_mode="availability_first",
        allow_paid_proxy=True,
    )

    assert result["status"] == "ready"
    assert result["summary"]["industry_standard_ready"] is True
    assert result["refresh"]["status"] == "skipped"
    assert [call[0] for call in manager.calls] == ["sync", "readiness"]
    assert manager.calls[0][1] == {
        "exchanges": ["SSE", "SZSE"],
        "limit_per_exchange": 2,
        "budget_mode": "availability_first",
        "allow_paid_proxy": True,
    }


@pytest.mark.asyncio
async def test_rollout_validation_can_include_audit_only_official_refresh():
    manager = FakeValidationManager(ready=True)

    result = await run_rollout_validation(
        manager,
        exchanges=["SSE", "SZSE"],
        budget_mode="availability_first",
        allow_paid_proxy=True,
        skip_refresh=False,
    )

    assert result["status"] == "ready"
    assert [call[0] for call in manager.calls] == ["refresh", "sync", "readiness"]
    assert manager.calls[0][1] == {
        "exchanges": ["SSE", "SZSE"],
        "budget_mode": "availability_first",
        "allow_paid_proxy": True,
    }


@pytest.mark.asyncio
async def test_rollout_validation_skip_flags_still_read_readiness():
    manager = FakeValidationManager(ready=False)

    result = await run_rollout_validation(
        manager,
        skip_refresh=True,
        skip_sync=True,
    )

    assert result["status"] == "not_ready"
    assert result["refresh"]["status"] == "skipped"
    assert result["sync"]["status"] == "skipped"
    assert [call[0] for call in manager.calls] == ["readiness"]


def test_rollout_validation_exit_code_for_not_ready():
    result = {"summary": {"industry_standard_ready": False}}

    assert exit_code_for_result(result, fail_on_not_ready=False) == 0
    assert exit_code_for_result(result, fail_on_not_ready=True) == 2
    assert (
        exit_code_for_result(
            {"summary": {"industry_standard_ready": True}},
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
        skip_refresh=True,
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
        async def get_research_industry_standard_readiness(self):
            self.calls.append(("readiness", {}))
            raise RuntimeError("readiness failed")

    manager = FailingManager()

    with pytest.raises(RuntimeError, match="readiness failed"):
        await run_rollout_validation_with_lifecycle(
            manager,
            skip_refresh=True,
            skip_sync=True,
        )

    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == ["initialize", "readiness", "close"]
    assert manager.calls[0][1] == {
        "include_data_sources": False,
        "load_progress": False,
    }
