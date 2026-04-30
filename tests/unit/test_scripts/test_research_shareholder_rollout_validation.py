from types import SimpleNamespace

import pytest

from scripts.research_shareholder_rollout_validation import (
    exit_code_for_result,
    run_rollout_validation,
)


class _FakeManager:
    def __init__(self, readiness):
        self.research_config = SimpleNamespace(
            markets=["SSE", "SZSE", "BSE"],
            modules={
                "shareholders": {
                    "enabled": False,
                    "delivery_mode": "free_best_effort",
                    "snapshot_api_requires_mode": "paid_high_availability",
                }
            }
        )
        self.readiness = readiness
        self.sync_calls = []

    async def run_shareholder_shadow_sync(self, **kwargs):
        self.sync_calls.append(kwargs)
        return {"status": "success", "total_snapshots_written": 2}

    async def get_research_shareholder_readiness(self):
        return self.readiness


@pytest.mark.asyncio
async def test_shareholder_rollout_validation_applies_runtime_gates_and_syncs():
    manager = _FakeManager(
        {
            "ready_for_paid_high_availability_rollout": True,
            "module_enabled": True,
            "snapshot_api_enabled": True,
            "delivery_mode": "paid_high_availability",
            "target_instrument_count": 2,
            "snapshot_total": 2,
            "missing_snapshot_count": 0,
            "scope_counts": {
                "holder_count": 2,
                "top10_holders": 2,
                "reference_only_ownership_clues": 2,
            },
            "blockers": [],
        }
    )

    result = await run_rollout_validation(
        manager,
        exchanges=["SSE"],
        limit_per_exchange=2,
        budget_mode="availability_first",
        allow_paid_proxy=True,
        enable_module=True,
        delivery_mode="paid_high_availability",
    )

    assert result["status"] == "ready"
    assert result["runtime_overrides"]["before"]["enabled"] is False
    assert result["runtime_overrides"]["after"]["enabled"] is True
    assert result["runtime_overrides"]["after"]["markets"] == ["SSE"]
    assert result["summary"]["ready_for_paid_high_availability_rollout"] is True
    assert manager.sync_calls == [
        {
            "exchanges": ["SSE"],
            "limit_per_exchange": 2,
            "budget_mode": "availability_first",
            "allow_paid_proxy": True,
        }
    ]


@pytest.mark.asyncio
async def test_shareholder_rollout_validation_can_skip_sync():
    manager = _FakeManager(
        {
            "ready_for_paid_high_availability_rollout": False,
            "module_enabled": False,
            "snapshot_api_enabled": False,
            "delivery_mode": "free_best_effort",
            "target_instrument_count": 10,
            "snapshot_total": 0,
            "missing_snapshot_count": 10,
            "scope_counts": {},
            "blockers": ["shareholders_module_disabled", "no_shareholder_snapshots"],
        }
    )

    result = await run_rollout_validation(manager, skip_sync=True)

    assert result["status"] == "not_ready"
    assert result["sync"] == {"status": "skipped", "reason": "skip_sync=true"}
    assert manager.sync_calls == []


def test_shareholder_rollout_validation_exit_code_can_fail_on_not_ready():
    assert exit_code_for_result(
        {"summary": {"ready_for_paid_high_availability_rollout": False}},
        fail_on_not_ready=True,
    ) == 2
    assert exit_code_for_result(
        {"summary": {"ready_for_paid_high_availability_rollout": False}},
        fail_on_not_ready=False,
    ) == 0
