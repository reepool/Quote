import asyncio

import pytest

from scripts.research_industry_standard_gap_fill import (
    exit_code_for_result,
    run_gap_fill,
    run_gap_fill_with_lifecycle,
)


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class FakeGapFillManager:
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

    async def get_research_industry_standard_coverage_gaps(self, **kwargs):
        self.calls.append(("coverage_gaps", kwargs))
        return {
            "missing_authoritative_membership_count": 2,
            "target_instrument_count": 3,
        }

    async def run_industry_standard_gap_fill_sync(self, **kwargs):
        self.calls.append(("gap_fill_sync", kwargs))
        return {
            "status": "success",
            "coverage_before": {
                "missing_authoritative_membership_count": 2,
                "target_instrument_count": 3,
            },
            "coverage_after": {
                "missing_authoritative_membership_count": 0,
                "target_instrument_count": 3,
            },
            "repaired_instrument_count": 2,
        }

    async def get_research_industry_standard_readiness(self, **kwargs):
        self.calls.append(("readiness", kwargs))
        return {
            "industry_standard_ready": self.ready,
            "blockers": [] if self.ready else ["authoritative_membership_coverage_incomplete"],
        }


def test_gap_fill_runs_targeted_sync_and_readiness():
    manager = FakeGapFillManager(ready=True)

    result = _run(
        run_gap_fill(
            manager,
            exchanges=["SSE", "SZSE"],
            taxonomy_system="sw",
            taxonomy_version="sw_2021",
            missing_limit_per_exchange=20,
            budget_mode="availability_first",
            allow_paid_proxy=True,
        )
    )

    assert result["status"] == "ready"
    assert result["summary"]["missing_before"] == 2
    assert result["summary"]["missing_after"] == 0
    assert result["summary"]["repaired_instrument_count"] == 2
    assert [call[0] for call in manager.calls] == ["gap_fill_sync", "readiness"]
    assert manager.calls[0][1] == {
        "exchanges": ["SSE", "SZSE"],
        "taxonomy_system": "sw",
        "taxonomy_version": "sw_2021",
        "missing_limit_per_exchange": 20,
        "budget_mode": "availability_first",
        "allow_paid_proxy": True,
    }


def test_gap_fill_skip_sync_only_reads_current_gaps_and_readiness():
    manager = FakeGapFillManager(ready=False)

    result = _run(
        run_gap_fill(
            manager,
            exchanges=["SSE"],
            skip_sync=True,
        )
    )

    assert result["status"] == "not_ready"
    assert result["gap_fill"]["status"] == "skipped"
    assert [call[0] for call in manager.calls] == ["coverage_gaps", "readiness"]


def test_gap_fill_exit_code_for_not_ready():
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


def test_gap_fill_lifecycle_closes_on_success():
    manager = FakeGapFillManager(ready=True)

    result = _run(
        run_gap_fill_with_lifecycle(
            manager,
            exchanges=["SSE"],
            skip_sync=True,
        )
    )

    assert result["status"] == "ready"
    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == ["initialize", "coverage_gaps", "readiness", "close"]
    assert manager.calls[0][1] == {
        "include_data_sources": False,
        "load_progress": False,
    }


def test_gap_fill_lifecycle_closes_on_failure():
    class FailingManager(FakeGapFillManager):
        async def get_research_industry_standard_readiness(self, **kwargs):
            self.calls.append(("readiness", kwargs))
            raise RuntimeError("readiness failed")

    manager = FailingManager()

    with pytest.raises(RuntimeError, match="readiness failed"):
        _run(
            run_gap_fill_with_lifecycle(
                manager,
                skip_sync=True,
            )
        )

    assert manager.initialized is True
    assert manager.closed is True
    assert [call[0] for call in manager.calls] == ["initialize", "coverage_gaps", "readiness", "close"]
    assert manager.calls[0][1] == {
        "include_data_sources": False,
        "load_progress": False,
    }
