import asyncio
import time

import pytest

from utils.llm import (
    LlmConfig,
    LlmDeadlineExceededError,
    LlmPoolCoordinatorRegistry,
    LlmRateLimitError,
)


class _FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += float(seconds)


def _profile(source, resource, *, max_concurrency=4):
    model = "grok" if "grok" in source else "luna"
    return {
        "enabled": True,
        "provider": "openai_compatible",
        "provider_resource": resource,
        "source_label": source,
        "base_url": "https://provider.example/v1",
        "endpoint": "/v1/chat/completions",
        "api_key_env": f"TEST_{model.upper()}_KEY",
        "model": model,
        "structured_output_mode": "json_object",
        "supported_structured_output_modes": ["json_object"],
        "timeout_seconds": 10,
        "queue_timeout_seconds": 1,
        "attempt_timeout_seconds": 5,
        "max_retries": 0,
        "max_schema_repair_attempts": 0,
        "max_concurrency": max_concurrency,
        "requests_per_minute": 0,
        "max_output_tokens_field": "max_tokens",
        "stream": False,
        "stream_include_usage": True,
    }


def _config(
    *,
    total=2,
    queue_size=4,
    borrow=True,
    member_limit=0,
    failure_threshold=2,
    open_seconds=5,
):
    return LlmConfig.from_mapping({
        "enabled": True,
        "provider_resources": {
            "grok-resource": {
                "hard_max_concurrency": 4,
                "default_bulk_concurrency": 4,
                "reserved_concurrency": 0,
                "http_max_connections": 4,
                "http_max_keepalive_connections": 4,
                "requests_per_minute": 0,
            },
            "luna-resource": {
                "hard_max_concurrency": 4,
                "default_bulk_concurrency": 4,
                "reserved_concurrency": 0,
                "http_max_connections": 4,
                "http_max_keepalive_connections": 4,
                "requests_per_minute": 0,
            },
        },
        "profiles": {
            "semantic__grok": _profile("pipio:grok", "grok-resource"),
            "semantic__luna": _profile("pipio:luna", "luna-resource"),
            "title__grok": _profile("pipio:grok", "grok-resource"),
            "title__luna": _profile("pipio:luna", "luna-resource"),
        },
        "pools": {
            "shared": {
                "enabled": True,
                "total_concurrency": total,
                "queue_size": queue_size,
                "strategy": "weighted_fair",
                "borrow_idle_capacity": borrow,
                "members": [
                    {
                        "source_label": "pipio:grok",
                        "weight": 3,
                        "max_concurrency": member_limit,
                        "profiles": {
                            "semantic": "semantic__grok",
                            "title": "title__grok",
                        },
                    },
                    {
                        "source_label": "pipio:luna",
                        "weight": 1,
                        "max_concurrency": member_limit,
                        "profiles": {
                            "semantic": "semantic__luna",
                            "title": "title__luna",
                        },
                    },
                ],
                "failover": {
                    "failure_threshold": failure_threshold,
                    "open_seconds": open_seconds,
                    "half_open_max_probes": 1,
                    "min_attempt_seconds": 0.1,
                },
            }
        },
        "routes": {
            "semantic": {
                "pool": "shared",
                "required_structured_output_modes": ["json_object"],
            },
            "title": {
                "pool": "shared",
                "required_structured_output_modes": ["json_object"],
            },
        },
    })


@pytest.mark.asyncio
async def test_registry_shares_pool_cap_across_clients_and_logical_profiles():
    config = _config(total=2, queue_size=8)
    registry = LlmPoolCoordinatorRegistry()
    first = registry.get(config, "shared")
    second = registry.get(config, "shared")
    assert first is second

    active = 0
    peak = 0

    async def work(index):
        nonlocal active, peak
        lease = await registry.get(config, "shared").acquire(
            deadline=time.monotonic() + 1,
            correlation={"logical_profile": "semantic" if index % 2 else "title"},
        )
        logical = "semantic" if index % 2 else "title"
        await lease.select_member(logical_profile=logical, deadline=time.monotonic() + 1)
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.005)
        active -= 1
        await lease.finish_member(success=True)
        await lease.close()

    await asyncio.gather(*(work(index) for index in range(8)))
    snapshot = first.snapshot()
    assert peak == 2
    assert snapshot.active == 0
    assert snapshot.admitted == snapshot.completed == 8
    assert snapshot.waiting == 0


@pytest.mark.asyncio
async def test_deterministic_three_to_one_fairness_and_low_weight_non_starvation():
    config = _config(total=1)
    coordinator = LlmPoolCoordinatorRegistry().get(config, "shared")
    selected = []
    for _ in range(40):
        lease = await coordinator.acquire(deadline=time.monotonic() + 1)
        member = await lease.select_member(
            logical_profile="semantic", deadline=time.monotonic() + 1
        )
        selected.append(member.source_label)
        await lease.finish_member(success=True)
        await lease.close()
    assert selected.count("pipio:grok") == 30
    assert selected.count("pipio:luna") == 10
    assert "pipio:luna" in selected[:4]


@pytest.mark.asyncio
async def test_idle_capacity_borrowing_preserves_member_and_pool_limits():
    config = _config(total=2, member_limit=1, borrow=True)
    coordinator = LlmPoolCoordinatorRegistry().get(config, "shared")
    first = await coordinator.acquire(deadline=time.monotonic() + 1)
    first_member = await first.select_member(
        logical_profile="semantic", deadline=time.monotonic() + 1
    )
    second = await coordinator.acquire(deadline=time.monotonic() + 1)
    second_member = await second.select_member(
        logical_profile="semantic", deadline=time.monotonic() + 1
    )
    assert first_member.source_label == "pipio:grok"
    assert second_member.source_label == "pipio:luna"
    assert second_member.borrowed is True
    snapshot = coordinator.snapshot()
    assert snapshot.active == 2
    assert max(member.active for member in snapshot.members) == 1
    await first.finish_member(success=True)
    await second.finish_member(success=True)
    await first.close()
    await second.close()


@pytest.mark.asyncio
async def test_disabled_borrowing_waits_for_weighted_member_and_honors_deadline():
    config = _config(total=2, member_limit=1, borrow=False)
    coordinator = LlmPoolCoordinatorRegistry().get(config, "shared")
    first = await coordinator.acquire(deadline=time.monotonic() + 1)
    await first.select_member(logical_profile="semantic", deadline=time.monotonic() + 1)
    second = await coordinator.acquire(deadline=time.monotonic() + 1)
    with pytest.raises(LlmDeadlineExceededError):
        await second.select_member(
            logical_profile="semantic", deadline=time.monotonic() + 0.01
        )
    await first.finish_member(success=True)
    await first.close()
    await second.close()
    assert coordinator.snapshot().active == 0


@pytest.mark.asyncio
async def test_admission_queue_limit_cancellation_and_cleanup():
    config = _config(total=1, queue_size=1)
    coordinator = LlmPoolCoordinatorRegistry().get(config, "shared")
    first = await coordinator.acquire(deadline=time.monotonic() + 1)
    queued = asyncio.create_task(
        coordinator.acquire(deadline=time.monotonic() + 1)
    )
    await asyncio.sleep(0)
    with pytest.raises(LlmRateLimitError):
        await coordinator.acquire(deadline=time.monotonic() + 1)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    await first.close()
    snapshot = coordinator.snapshot()
    assert snapshot.active == 0
    assert snapshot.waiting == 0
    assert snapshot.queue_full == 1
    assert snapshot.cancelled == 1


@pytest.mark.asyncio
async def test_circuit_opens_half_opens_and_recovers_with_fake_clock():
    clock = _FakeClock()
    config = _config(total=1, failure_threshold=1, open_seconds=5)
    coordinator = LlmPoolCoordinatorRegistry(clock=clock).get(config, "shared")
    first = await coordinator.acquire(deadline=clock() + 1)
    failed = await first.select_member(
        logical_profile="semantic", deadline=clock() + 1
    )
    assert failed.source_label == "pipio:grok"
    await first.finish_member(success=False, error_code="rate_limit_error", status_code=429)
    await first.close()
    opened = coordinator.snapshot().members[0]
    assert opened.circuit_state == "open"
    assert opened.cooldown_remaining_seconds == 5

    fallback = await coordinator.acquire(deadline=clock() + 1)
    selected = await fallback.select_member(
        logical_profile="semantic", deadline=clock() + 1
    )
    assert selected.source_label == "pipio:luna"
    await fallback.finish_member(success=True)
    await fallback.close()

    clock.advance(5)
    probe = await coordinator.acquire(deadline=clock() + 1)
    probe_selection = await probe.select_member(
        logical_profile="semantic",
        deadline=clock() + 1,
        excluded_sources=("pipio:luna",),
    )
    assert probe_selection.source_label == "pipio:grok"
    assert probe_selection.half_open_probe is True
    await probe.finish_member(success=True)
    await probe.close()
    assert coordinator.snapshot().members[0].circuit_state == "closed"


@pytest.mark.asyncio
async def test_failover_metrics_snapshots_and_registry_lifecycle_are_non_secret():
    config = _config(total=1)
    registry = LlmPoolCoordinatorRegistry()
    coordinator = registry.get(config, "shared")
    lease = await coordinator.acquire(
        deadline=time.monotonic() + 1,
        correlation={
            "logical_profile": "semantic",
            "run_id": "run-1",
            "document_text": "must-not-appear",
        },
    )
    await lease.record_failover(error_code="rate_limit_error", succeeded=True)
    await lease.close()
    snapshot = coordinator.snapshot()
    assert snapshot.failover_requested == snapshot.failover_succeeded == 1
    assert snapshot.failover_by_error == {"rate_limit_error": 1}
    assert "document_text" not in str(snapshot.correlations)
    await registry.close_all()
    assert coordinator.snapshot().closed is True
    assert registry.snapshots() == {}
