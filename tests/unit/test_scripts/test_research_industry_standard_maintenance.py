import pytest

from scripts.research_industry_standard_maintenance import (
    exit_code_for_result,
    run_maintenance,
    run_maintenance_with_lifecycle,
)


class FakeMaintenanceManager:
    def __init__(self, *, ready: bool = True):
        self.calls = []
        self.ready = ready
        self.initialized = False
        self.closed = False
        self._coverage_call_count = 0

    async def initialize(self, **kwargs):
        self.calls.append(("initialize", kwargs))
        self.initialized = True

    async def close(self):
        self.calls.append(("close", {}))
        self.closed = True

    async def get_research_industry_standard_coverage_gaps(self, **kwargs):
        self.calls.append(("coverage_gaps", kwargs))
        self._coverage_call_count += 1
        if self._coverage_call_count == 1:
            missing = 3
        else:
            missing = 1
        return {
            "missing_authoritative_membership_count": missing,
            "target_instrument_count": 5,
        }

    async def run_industry_official_mapping_refresh(self, **kwargs):
        self.calls.append(("refresh", kwargs))
        return {"status": "success", "mapping_cache_rows_written": 433}

    async def run_industry_standard_sync(self, **kwargs):
        self.calls.append(("sync", kwargs))
        return {"status": "success", "total_memberships_written": 4}

    async def run_industry_standard_gap_fill_sync(self, **kwargs):
        self.calls.append(("gap_fill_sync", kwargs))
        return {
            "status": "success",
            "coverage_before": {
                "missing_authoritative_membership_count": 1,
                "target_instrument_count": 5,
            },
            "coverage_after": {
                "missing_authoritative_membership_count": 0,
                "target_instrument_count": 5,
            },
            "repaired_instrument_count": 1,
        }

    async def get_research_industry_standard_readiness(self, **kwargs):
        self.calls.append(("readiness", kwargs))
        return {
            "industry_standard_ready": self.ready,
            "blockers": [] if self.ready else ["authoritative_membership_coverage_incomplete"],
        }


@pytest.mark.asyncio
async def test_maintenance_runs_full_sync_gap_fill_and_readiness():
    manager = FakeMaintenanceManager(ready=True)

    result = await run_maintenance(
        manager,
        exchanges=["SSE", "SZSE"],
        taxonomy_system="sw",
        taxonomy_version="sw_2021",
        missing_limit_per_exchange=20,
        budget_mode="availability_first",
        allow_paid_proxy=True,
    )

    assert result["status"] == "ready"
    assert result["summary"]["missing_before_sync"] == 3
    assert result["summary"]["missing_after_sync"] == 1
    assert result["summary"]["missing_after_gap_fill"] == 0
    assert result["summary"]["full_sync_memberships_written"] == 4
    assert result["summary"]["gap_fill_repaired_instrument_count"] == 1
    assert [call[0] for call in manager.calls] == [
        "coverage_gaps",
        "sync",
        "coverage_gaps",
        "gap_fill_sync",
        "readiness",
    ]
    assert manager.calls[1][1] == {
        "exchanges": ["SSE", "SZSE"],
        "limit_per_exchange": None,
        "budget_mode": "availability_first",
        "allow_paid_proxy": True,
        "force_component_refresh": False,
    }


@pytest.mark.asyncio
async def test_maintenance_can_include_official_refresh_and_skip_gap_fill():
    manager = FakeMaintenanceManager(ready=False)

    result = await run_maintenance(
        manager,
        exchanges=["SSE"],
        budget_mode="availability_first",
        allow_paid_proxy=True,
        include_official_refresh=True,
        skip_gap_fill=True,
    )

    assert result["status"] == "not_ready"
    assert result["refresh"]["status"] == "success"
    assert result["gap_fill"]["status"] == "skipped"
    assert [call[0] for call in manager.calls] == [
        "coverage_gaps",
        "refresh",
        "sync",
        "coverage_gaps",
        "readiness",
    ]


@pytest.mark.asyncio
async def test_maintenance_can_force_component_refresh():
    manager = FakeMaintenanceManager(ready=True)

    result = await run_maintenance(
        manager,
        exchanges=["SSE"],
        force_component_refresh=True,
    )

    assert result["status"] == "ready"
    assert manager.calls[1][0] == "sync"
    assert manager.calls[1][1]["force_component_refresh"] is True


def test_maintenance_exit_code_for_not_ready():
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
async def test_maintenance_lifecycle_closes_on_success():
    manager = FakeMaintenanceManager(ready=True)

    result = await run_maintenance_with_lifecycle(
        manager,
        exchanges=["SSE"],
        skip_sync=True,
        skip_gap_fill=True,
    )

    assert result["status"] == "ready"
    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == [
        "initialize",
        "coverage_gaps",
        "readiness",
        "close",
    ]
    assert manager.calls[0][1] == {
        "include_data_sources": False,
        "load_progress": False,
    }


@pytest.mark.asyncio
async def test_maintenance_lifecycle_closes_on_failure():
    class FailingManager(FakeMaintenanceManager):
        async def get_research_industry_standard_readiness(self, **kwargs):
            self.calls.append(("readiness", kwargs))
            raise RuntimeError("readiness failed")

    manager = FailingManager()

    with pytest.raises(RuntimeError, match="readiness failed"):
        await run_maintenance_with_lifecycle(
            manager,
            skip_sync=True,
            skip_gap_fill=True,
        )

    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == [
        "initialize",
        "coverage_gaps",
        "readiness",
        "close",
    ]
