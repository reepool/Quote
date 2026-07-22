import asyncio
import json
import time

import pytest

from utils.llm import (
    BoundedResourcePool,
    BoundedStageQueue,
    CallableTransport,
    LlmClient,
    LlmConfig,
    LlmDeadlineExceededError,
    LlmMessage,
    LlmRateLimitError,
    LlmRequest,
    LlmTransientTransportError,
    OutcomeLedger,
    OutcomeStatus,
    OrchestrationError,
    PipelineController,
    ProviderCoordinator,
    ProviderCoordinatorRegistry,
    ProviderResourceConfig,
    ResourceLeaseError,
    StageOutcome,
    StageQueueClosedError,
    StageRunner,
    WorkItem,
)


def _provider_response(label="ok"):
    return {
        "choices": [{
            "message": {"content": json.dumps({"label": label})},
            "finish_reason": "stop",
        }],
        "id": f"provider-{label}",
    }


def _multi_profile_config(*, hard=2, bulk=1, orchestration=True):
    return LlmConfig.from_mapping({
        "enabled": True,
        "provider_resources": {
            "shared": {
                "provider": "openai_compatible",
                "hard_max_concurrency": hard,
                "default_bulk_concurrency": bulk,
                "reserved_concurrency": hard - bulk,
                "http_max_connections": max(hard, 4),
                "http_max_keepalive_connections": max(hard, 2),
            }
        },
        "orchestration": {"enabled": orchestration},
        "profiles": {
            name: {
                "enabled": True,
                "provider": "openai_compatible",
                "provider_resource": "shared",
                "base_url": "https://provider.example/v1",
                "endpoint": "/v1/chat/completions",
                "api_key_env": "TEST_LLM_KEY",
                "model": "test-model",
                "max_concurrency": hard,
                "requests_per_minute": 0,
                "max_retries": 0,
            }
            for name in ("one", "two")
        },
    })


def _request(profile, item, *, bulk=False):
    return LlmRequest(
        profile=profile,
        messages=[LlmMessage(role="user", content=f"item {item}")],
        response_schema={
            "type": "object",
            "required": ["label"],
            "properties": {"label": {"type": "string"}},
        },
        metadata={
            "workload": f"workload-{profile}",
            "run_id": "run-1",
            "stage": "classify",
            "stage_sequence": 1,
            "business_item_key": item,
            "input_hash": f"hash-{item}",
            "bulk": bulk,
            "secret_not_forwarded": "hidden",
        },
    )


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += float(seconds)


def test_orchestration_configuration_validates_global_and_local_limits():
    with pytest.raises(ValueError, match="hard_max_concurrency"):
        ProviderResourceConfig.from_mapping("bad", {
            "hard_max_concurrency": 61,
        })
    with pytest.raises(ValueError, match="document_parse"):
        LlmConfig.from_mapping({
            "orchestration": {"resource_limits": {"document_parse": 9}}
        })
    with pytest.raises(ValueError, match="exceeds provider resource"):
        LlmConfig.from_mapping({
            "provider_resources": {
                "small": {"hard_max_concurrency": 2}
            },
            "profiles": {
                "large": {
                    "enabled": True,
                    "provider_resource": "small",
                    "max_concurrency": 3,
                }
            },
        })
    with pytest.raises(ValueError, match="soft_failure_rate_threshold"):
        ProviderResourceConfig.from_mapping("bad-adaptive", {
            "adaptive_soft_failure_rate_threshold": 0,
        })


@pytest.mark.asyncio
async def test_two_clients_and_profiles_share_one_provider_ceiling():
    active = 0
    peak = 0

    async def transport(url, headers, payload, timeout):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return _provider_response(payload["messages"][-1]["content"])

    registry = ProviderCoordinatorRegistry()
    config = _multi_profile_config(hard=2, bulk=2)
    clients = [
        LlmClient(
            config,
            transport=CallableTransport(transport),
            environment={"TEST_LLM_KEY": "secret"},
            provider_coordinator_registry=registry,
        )
        for _ in range(2)
    ]

    responses = await asyncio.gather(*(
        clients[index % 2].complete(
            _request("one" if index % 2 == 0 else "two", str(index), bulk=True)
        )
        for index in range(8)
    ))

    assert peak == 2
    assert len({response.request_id for response in responses}) == 8
    assert all(response.lineage["run_id"] == "run-1" for response in responses)


@pytest.mark.asyncio
async def test_bulk_limit_preserves_headroom_for_non_bulk_work():
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="shared",
        hard_max_concurrency=3,
        default_bulk_concurrency=2,
        reserved_concurrency=1,
        http_max_connections=4,
        http_max_keepalive_connections=3,
    ))
    deadline = time.monotonic() + 1
    await coordinator.acquire(workload="bulk-a", deadline=deadline, bulk=True)
    await coordinator.acquire(workload="bulk-b", deadline=deadline, bulk=True)
    third_bulk = asyncio.create_task(coordinator.acquire(
        workload="bulk-c", deadline=deadline, bulk=True
    ))
    await asyncio.sleep(0)
    assert not third_bulk.done()

    await coordinator.acquire(
        workload="interactive", deadline=deadline, bulk=False
    )
    assert coordinator.snapshot().active == 3
    await coordinator.release(workload="interactive", bulk=False)
    await coordinator.release(workload="bulk-a", bulk=True)
    await asyncio.wait_for(third_bulk, timeout=0.2)
    await coordinator.release(workload="bulk-b", bulk=True)
    await coordinator.release(workload="bulk-c", bulk=True)


@pytest.mark.asyncio
async def test_provider_admission_is_fair_and_cancellation_safe():
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="fair",
        hard_max_concurrency=1,
        default_bulk_concurrency=1,
        reserved_concurrency=0,
        http_max_connections=2,
        http_max_keepalive_connections=1,
    ))
    deadline = time.monotonic() + 1
    await coordinator.acquire(workload="blocker", deadline=deadline, bulk=True)
    order = []

    async def acquire_and_record(workload):
        await coordinator.acquire(
            workload=workload, deadline=deadline, bulk=True
        )
        order.append(workload)
        await coordinator.release(workload=workload, bulk=True)

    first_a = asyncio.create_task(acquire_and_record("a"))
    second_a = asyncio.create_task(acquire_and_record("a"))
    first_b = asyncio.create_task(acquire_and_record("b"))
    cancelled = asyncio.create_task(acquire_and_record("cancelled"))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    await coordinator.release(workload="blocker", bulk=True)
    await asyncio.gather(first_a, second_a, first_b)

    assert order[:2] == ["a", "b"]
    snapshot = coordinator.snapshot()
    assert snapshot.active == 0
    assert snapshot.cancelled == 1


@pytest.mark.asyncio
async def test_provider_cooldown_and_deadline_are_enforced():
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="cooldown",
        hard_max_concurrency=1,
        default_bulk_concurrency=1,
        reserved_concurrency=0,
        http_max_connections=2,
        http_max_keepalive_connections=1,
    ))
    await coordinator.set_cooldown(0.03)
    started = time.monotonic()
    await coordinator.acquire(
        workload="waiter", deadline=time.monotonic() + 0.2, bulk=True
    )
    assert time.monotonic() - started >= 0.02
    await coordinator.release(workload="waiter", bulk=True)

    await coordinator.set_cooldown(0.05)
    with pytest.raises(LlmDeadlineExceededError):
        await coordinator.acquire(
            workload="expired",
            deadline=time.monotonic() + 0.005,
            bulk=True,
        )


@pytest.mark.asyncio
async def test_provider_adaptive_concurrency_coalesces_correlated_soft_failures():
    clock = _FakeClock()
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="adaptive",
        hard_max_concurrency=20,
        default_bulk_concurrency=15,
        reserved_concurrency=5,
        http_max_connections=20,
        http_max_keepalive_connections=20,
        adaptive_min_bulk_concurrency=5,
        adaptive_failure_coalescing_seconds=10,
        adaptive_outcome_window_size=30,
        adaptive_soft_failure_min_count=2,
        adaptive_soft_failure_rate_threshold=0.05,
        adaptive_soft_decrease_ratio=0.8,
        transient_cooldown_seconds=0,
    ), clock=clock)

    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=503
    )
    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=503
    )
    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=502
    )

    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 12
    assert snapshot.adaptive_retryable_failures == 3
    assert snapshot.adaptive_congestion_events == 1
    assert snapshot.adaptive_coalesced_failures == 2
    assert snapshot.adaptive_last_failure_class == "soft_transient"
    assert snapshot.adaptive_state == "congestion_episode"

    clock.advance(11)
    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=503
    )
    assert coordinator.snapshot().effective_bulk_concurrency == 12
    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=503
    )
    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 9
    assert snapshot.adaptive_congestion_events == 2


@pytest.mark.asyncio
async def test_provider_adaptive_hard_throttle_is_immediate_and_coalesced():
    clock = _FakeClock()
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="hard-throttle",
        hard_max_concurrency=20,
        default_bulk_concurrency=16,
        reserved_concurrency=4,
        http_max_connections=20,
        http_max_keepalive_connections=20,
        adaptive_min_bulk_concurrency=4,
        adaptive_failure_coalescing_seconds=10,
        adaptive_hard_decrease_ratio=0.5,
        rate_limit_cooldown_seconds=5,
    ), clock=clock)

    await coordinator.report_retryable_failure(
        error_code="rate_limit_error",
        status_code=429,
        retry_after_seconds=7,
    )
    await coordinator.report_retryable_failure(
        error_code="rate_limit_error",
        status_code=429,
        retry_after_seconds=3,
    )

    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 8
    assert snapshot.adaptive_retryable_failures == 2
    assert snapshot.adaptive_congestion_events == 1
    assert snapshot.adaptive_coalesced_failures == 1
    assert snapshot.cooldown_remaining_seconds == pytest.approx(7)
    assert snapshot.adaptive_state == "cooldown"
    await coordinator.close()


@pytest.mark.asyncio
async def test_provider_adaptive_hard_throttle_upgrades_soft_episode_once():
    clock = _FakeClock()
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="hard-upgrade",
        hard_max_concurrency=20,
        default_bulk_concurrency=20,
        reserved_concurrency=0,
        http_max_connections=20,
        http_max_keepalive_connections=20,
        adaptive_min_bulk_concurrency=4,
        adaptive_failure_coalescing_seconds=10,
        adaptive_soft_failure_min_count=2,
        adaptive_soft_decrease_ratio=0.8,
        adaptive_hard_decrease_ratio=0.5,
        rate_limit_cooldown_seconds=0,
        transient_cooldown_seconds=0,
    ), clock=clock)

    await coordinator.report_retryable_failure(status_code=503)
    await coordinator.report_retryable_failure(status_code=503)
    assert coordinator.snapshot().effective_bulk_concurrency == 16

    await coordinator.report_retryable_failure(
        error_code="rate_limit_error", status_code=429
    )
    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 10
    assert snapshot.adaptive_congestion_events == 1
    assert snapshot.adaptive_coalesced_failures == 1
    assert snapshot.adaptive_last_failure_class == "hard_rate_limit"

    await coordinator.report_retryable_failure(
        error_code="rate_limit_error", status_code=429
    )
    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 10
    assert snapshot.adaptive_congestion_events == 1
    assert snapshot.adaptive_coalesced_failures == 2


@pytest.mark.asyncio
async def test_provider_adaptive_recovers_proportionally_from_six_to_fifteen():
    clock = _FakeClock()
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="recovery",
        hard_max_concurrency=20,
        default_bulk_concurrency=15,
        reserved_concurrency=5,
        http_max_connections=20,
        http_max_keepalive_connections=20,
        adaptive_min_bulk_concurrency=5,
        adaptive_failure_coalescing_seconds=1,
        adaptive_hard_decrease_ratio=0.4,
        adaptive_recovery_successes=2,
        adaptive_recovery_quiet_seconds=10,
        adaptive_recovery_probe_interval_seconds=5,
        adaptive_recovery_growth_factor=4 / 3,
        rate_limit_cooldown_seconds=0,
    ), clock=clock)

    await coordinator.report_retryable_failure(
        error_code="rate_limit_error", status_code=429
    )
    assert coordinator.snapshot().effective_bulk_concurrency == 6

    clock.advance(10)
    await coordinator.report_success()
    await coordinator.report_success()
    assert coordinator.snapshot().effective_bulk_concurrency == 8

    await coordinator.report_success()
    await coordinator.report_success()
    assert coordinator.snapshot().effective_bulk_concurrency == 8
    clock.advance(5)
    await coordinator.report_success()
    assert coordinator.snapshot().effective_bulk_concurrency == 11

    clock.advance(5)
    await coordinator.report_success()
    await coordinator.report_success()
    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 15
    assert snapshot.adaptive_recovery_probes == 3
    assert snapshot.adaptive_state == "steady"


@pytest.mark.asyncio
async def test_provider_adaptive_failure_interrupts_recovery_probe():
    clock = _FakeClock()
    coordinator = ProviderCoordinator(ProviderResourceConfig(
        name="recovery-interruption",
        hard_max_concurrency=20,
        default_bulk_concurrency=15,
        reserved_concurrency=5,
        http_max_connections=20,
        http_max_keepalive_connections=20,
        adaptive_min_bulk_concurrency=5,
        adaptive_failure_coalescing_seconds=1,
        adaptive_soft_failure_min_count=2,
        adaptive_soft_failure_rate_threshold=0.05,
        adaptive_soft_decrease_ratio=0.8,
        adaptive_hard_decrease_ratio=0.4,
        adaptive_recovery_successes=2,
        adaptive_recovery_quiet_seconds=5,
        adaptive_recovery_probe_interval_seconds=5,
        adaptive_recovery_growth_factor=4 / 3,
        rate_limit_cooldown_seconds=0,
        transient_cooldown_seconds=0,
    ), clock=clock)

    await coordinator.report_retryable_failure(
        error_code="rate_limit_error", status_code=429
    )
    clock.advance(5)
    await coordinator.report_success()
    await coordinator.report_success()
    assert coordinator.snapshot().effective_bulk_concurrency == 8

    clock.advance(2)
    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=503
    )
    await coordinator.report_retryable_failure(
        error_code="transient_transport_error", status_code=503
    )
    snapshot = coordinator.snapshot()
    assert snapshot.effective_bulk_concurrency == 6
    assert snapshot.adaptive_congestion_events == 2
    assert snapshot.adaptive_success_streak == 0
    assert snapshot.adaptive_state == "congestion_episode"

    clock.advance(2)
    await coordinator.report_success()
    await coordinator.report_success()
    assert coordinator.snapshot().effective_bulk_concurrency == 6



@pytest.mark.asyncio
async def test_retryable_response_sets_cooldown_before_next_admission():
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    call_times = []
    throttled_at = None

    async def transport(url, headers, payload, timeout):
        nonlocal throttled_at
        call_times.append(time.monotonic())
        if len(call_times) == 1:
            first_started.set()
            await release_first.wait()
            throttled_at = time.monotonic()
            return {
                "status_code": 429,
                "headers": {"retry-after": "0.05"},
                "data": {},
            }
        return _provider_response("second")

    client = LlmClient(
        _multi_profile_config(hard=1, bulk=1),
        transport=CallableTransport(transport),
        environment={"TEST_LLM_KEY": "secret"},
        provider_coordinator_registry=ProviderCoordinatorRegistry(),
    )
    first = asyncio.create_task(client.complete(_request("one", "first", bulk=True)))
    await first_started.wait()
    second = asyncio.create_task(
        client.complete(_request("one", "second", bulk=True))
    )
    await asyncio.sleep(0)
    release_first.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert isinstance(results[0], Exception)
    assert results[1].data == {"label": "second"}
    assert throttled_at is not None
    assert call_times[1] - throttled_at >= 0.04
    snapshot = client.provider_coordinator_registry.snapshots()[0]
    assert snapshot.effective_bulk_concurrency == 1


@pytest.mark.asyncio
async def test_provider_cooldown_uses_uncapped_retry_after():
    async def transport(url, headers, payload, timeout):
        return {
            "status_code": 429,
            "headers": {"retry-after": "120"},
            "data": {},
        }

    config = _multi_profile_config(hard=1, bulk=1)
    assert config.profiles["one"].max_retry_after_seconds == 30
    registry = ProviderCoordinatorRegistry()
    client = LlmClient(
        config,
        transport=CallableTransport(transport),
        environment={"TEST_LLM_KEY": "secret"},
        provider_coordinator_registry=registry,
    )

    with pytest.raises(LlmRateLimitError):
        await client.complete(_request("one", "long-retry-after", bulk=True))

    snapshot = registry.snapshots()[0]
    assert snapshot.cooldown_remaining_seconds >= 119
    coordinator = registry.get(config.provider_resources["shared"])
    await coordinator.close()


@pytest.mark.asyncio
async def test_transport_failure_sets_cooldown_before_releasing_provider_slot():
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    call_times = []
    failed_at = None

    async def transport(url, headers, payload, timeout):
        nonlocal failed_at
        call_times.append(time.monotonic())
        if len(call_times) == 1:
            first_started.set()
            await release_first.wait()
            failed_at = time.monotonic()
            raise LlmTransientTransportError("temporary transport failure")
        return _provider_response("second")

    config = _multi_profile_config(hard=1, bulk=1)
    object.__setattr__(
        config.provider_resources["shared"],
        "transient_cooldown_seconds",
        0.05,
    )
    client = LlmClient(
        config,
        transport=CallableTransport(transport),
        environment={"TEST_LLM_KEY": "secret"},
        provider_coordinator_registry=ProviderCoordinatorRegistry(),
    )
    first = asyncio.create_task(client.complete(_request("one", "first", bulk=True)))
    await first_started.wait()
    second = asyncio.create_task(client.complete(_request("one", "second", bulk=True)))
    await asyncio.sleep(0)
    release_first.set()
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert isinstance(results[0], LlmTransientTransportError)
    assert results[1].data == {"label": "second"}
    assert failed_at is not None
    assert call_times[1] - failed_at >= 0.04


@pytest.mark.asyncio
async def test_attempt_timeout_reports_one_provider_failure():
    async def slow_transport(url, headers, payload, timeout):
        await asyncio.sleep(0.05)

    config = _multi_profile_config(hard=10, bulk=8)
    profile = config.profiles["one"]
    object.__setattr__(profile, "max_retries", 0)
    object.__setattr__(profile, "timeout_seconds", 1.0)
    object.__setattr__(profile, "attempt_timeout_seconds", 0.01)
    object.__setattr__(
        config.provider_resources["shared"],
        "transient_cooldown_seconds",
        0.0,
    )
    registry = ProviderCoordinatorRegistry()
    client = LlmClient(
        config,
        transport=CallableTransport(slow_transport),
        environment={"TEST_LLM_KEY": "secret"},
        provider_coordinator_registry=registry,
    )

    with pytest.raises(LlmTransientTransportError):
        await client.complete(_request("one", "timeout", bulk=True))

    snapshot = registry.snapshots()[0]
    assert snapshot.adaptive_retryable_failures == 1
    assert snapshot.adaptive_congestion_events == 0
    assert snapshot.effective_bulk_concurrency == 8


@pytest.mark.asyncio
async def test_resource_pools_are_independent_and_reject_nested_resources():
    parse_pool = BoundedResourcePool("document_parse", 2)
    writer_pool = BoundedResourcePool("sqlite_writer", 1)
    async with parse_pool.slot():
        with pytest.raises(ResourceLeaseError):
            async with writer_pool.slot():
                pass
    assert parse_pool.snapshot().active == 0
    assert parse_pool.snapshot().acquired == parse_pool.snapshot().released == 1


@pytest.mark.asyncio
async def test_resource_pool_rejects_same_resource_reentry():
    writer_pool = BoundedResourcePool("sqlite_writer", 1)
    async with writer_pool.slot():
        with pytest.raises(ResourceLeaseError):
            async with writer_pool.slot():
                pass


@pytest.mark.asyncio
async def test_resource_pool_direct_acquire_rejects_nested_lease():
    writer_pool = BoundedResourcePool("sqlite_writer", 1)
    await writer_pool.acquire()
    try:
        with pytest.raises(ResourceLeaseError):
            await writer_pool.acquire()
    finally:
        writer_pool.release()


@pytest.mark.asyncio
async def test_stage_queue_applies_backpressure_and_closes_cleanly():
    queue = BoundedStageQueue(maxsize=1)
    first = WorkItem("1", "test", "run", "one", "stage")
    second = WorkItem("2", "test", "run", "two", "stage")
    await queue.put(first)
    blocked_put = asyncio.create_task(queue.put(second))
    await asyncio.sleep(0)
    assert not blocked_put.done()
    item, _ = await queue.get()
    assert item == first
    await queue.task_done()
    await blocked_put
    await queue.close()
    item, _ = await queue.get()
    assert item == second
    await queue.task_done()
    with pytest.raises(StageQueueClosedError):
        await queue.get()
    await queue.join()


@pytest.mark.asyncio
async def test_stage_runner_preserves_identity_for_out_of_order_results():
    queue = BoundedStageQueue(maxsize=4)
    outcomes = []

    async def callback(item):
        await asyncio.sleep(0.02 if item.business_item_key == "slow" else 0)
        return {"item": item.business_item_key}

    runner = StageRunner(
        name="extract",
        queue=queue,
        callback=callback,
        workers=2,
        on_outcome=outcomes.append,
    )
    await runner.start()
    await queue.put(WorkItem("1", "test", "run", "slow", "extract"))
    await queue.put(WorkItem("2", "test", "run", "fast", "extract"))
    await queue.join()
    await runner.close()

    assert [item.item.business_item_key for item in outcomes] == ["fast", "slow"]
    assert {
        item.item.business_item_key: item.output["item"] for item in outcomes
    } == {"fast": "fast", "slow": "slow"}
    assert runner.snapshot().succeeded == 2


@pytest.mark.asyncio
async def test_stage_runner_publish_failure_closes_queue_and_wakes_producer():
    queue = BoundedStageQueue(maxsize=1)
    callback_entered = asyncio.Event()
    release_callback = asyncio.Event()

    async def publish(_outcome):
        callback_entered.set()
        await release_callback.wait()
        raise RuntimeError("downstream unavailable")

    runner = StageRunner(
        name="publish_failure",
        queue=queue,
        callback=lambda item: item.business_item_key,
        workers=1,
        on_outcome=publish,
    )
    await runner.start()
    await queue.put(WorkItem("1", "test", "run", "one", "stage"))
    await callback_entered.wait()
    await queue.put(WorkItem("2", "test", "run", "two", "stage"))
    blocked_put = asyncio.create_task(
        queue.put(WorkItem("3", "test", "run", "three", "stage"))
    )
    await asyncio.sleep(0)
    assert not blocked_put.done()
    release_callback.set()
    with pytest.raises(StageQueueClosedError):
        await blocked_put
    with pytest.raises(OrchestrationError, match="outcome publication failed"):
        await runner.close()
    assert queue.depth == 0


def test_outcome_ledger_keeps_only_unacknowledged_work():
    item = WorkItem("1", "test", "run", "event", "persist", 3)
    outcome = StageOutcome(item=item, status=OutcomeStatus.SUCCESS)
    ledger = OutcomeLedger()
    ledger.add(outcome)
    assert ledger.pending() == (outcome,)
    ledger.acknowledge(outcome)
    assert ledger.pending() == ()


@pytest.mark.asyncio
async def test_pipeline_graceful_close_drains_upstream_before_downstream():
    upstream_queue = BoundedStageQueue(maxsize=2)
    downstream_queue = BoundedStageQueue(maxsize=2)
    outputs = []

    async def route(outcome):
        await downstream_queue.put(outcome.item.next_stage(
            "downstream", payload=outcome.output
        ))

    upstream = StageRunner(
        name="upstream",
        queue=upstream_queue,
        callback=lambda item: item.business_item_key,
        workers=1,
        on_outcome=route,
    )
    downstream = StageRunner(
        name="downstream",
        queue=downstream_queue,
        callback=lambda item: item.payload,
        workers=1,
        on_outcome=lambda outcome: outputs.append(outcome.output),
    )
    pipeline = PipelineController()
    pipeline.add_stage(upstream)
    pipeline.add_stage(downstream)
    await pipeline.start()
    await upstream_queue.put(WorkItem(
        "1", "test", "run", "event-1", "upstream"
    ))
    await pipeline.close()

    assert outputs == ["event-1"]


@pytest.mark.asyncio
async def test_client_close_is_idempotent_and_blocks_new_requests():
    class ClosingTransport(CallableTransport):
        def __init__(self):
            super().__init__(lambda *args: _provider_response())
            self.close_count = 0

        async def close(self):
            self.close_count += 1

    transport = ClosingTransport()
    client = LlmClient(
        _multi_profile_config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "secret"},
        owns_transport=True,
    )
    await client.close()
    await client.close()
    assert transport.close_count == 1
    with pytest.raises(Exception, match="closed"):
        await client.complete(_request("one", "after-close"))


@pytest.mark.asyncio
async def test_injected_transport_remains_caller_owned_by_default():
    class ClosingTransport(CallableTransport):
        def __init__(self):
            super().__init__(lambda *args: _provider_response())
            self.close_count = 0

        async def close(self):
            self.close_count += 1

    transport = ClosingTransport()
    client = LlmClient(
        _multi_profile_config(),
        transport=transport,
        environment={"TEST_LLM_KEY": "secret"},
    )

    await client.close()

    assert transport.close_count == 0


@pytest.mark.asyncio
async def test_client_created_transport_is_always_client_owned(monkeypatch):
    class ClosingTransport(CallableTransport):
        def __init__(self):
            super().__init__(lambda *args: _provider_response())
            self.close_count = 0

        async def close(self):
            self.close_count += 1

    transport = ClosingTransport()
    monkeypatch.setattr(
        "utils.llm.client.HttpxOpenAICompatibleTransport",
        lambda **_kwargs: transport,
    )
    client = LlmClient(
        _multi_profile_config(),
        environment={"TEST_LLM_KEY": "secret"},
        owns_transport=False,
    )

    await client.close()

    assert transport.close_count == 1


@pytest.mark.asyncio
async def test_failure_preserves_redacted_orchestration_lineage():
    async def failing(*args):
        raise RuntimeError("temporary failure")

    client = LlmClient(
        _multi_profile_config(),
        transport=CallableTransport(failing),
        environment={"TEST_LLM_KEY": "secret"},
    )
    with pytest.raises(Exception) as captured:
        await client.complete(_request("one", "event-1"))
    error = captured.value
    assert error.request_id
    assert error.request_hash
    assert error.lineage == {
        "workload": "workload-one",
        "run_id": "run-1",
        "stage": "classify",
        "stage_sequence": 1,
        "business_item_key": "event-1",
        "input_hash": "hash-event-1",
    }
    assert "secret_not_forwarded" not in error.lineage


@pytest.mark.asyncio
async def test_retry_backoff_applies_bounded_deterministic_jitter(monkeypatch):
    profile = _multi_profile_config().profiles["one"]
    object.__setattr__(profile, "retry_backoff_seconds", 1.0)
    object.__setattr__(profile, "retry_jitter_ratio", 0.2)
    delays = []

    async def record_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr("utils.llm.client.asyncio.sleep", record_sleep)
    await LlmClient._backoff(
        profile,
        1,
        response=None,
        deadline=time.monotonic() + 10,
        random_source=lambda: 1.0,
    )

    assert delays == [pytest.approx(1.2)]


@pytest.mark.asyncio
async def test_orchestration_disable_keeps_profile_only_rollback_path():
    active = 0
    peak = 0

    async def transport(url, headers, payload, timeout):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1
        return _provider_response()

    config = _multi_profile_config(
        hard=2, bulk=1, orchestration=False
    )
    client = LlmClient(
        config,
        transport=CallableTransport(transport),
        environment={"TEST_LLM_KEY": "secret"},
    )
    await asyncio.gather(
        client.complete(_request("one", "one")),
        client.complete(_request("one", "two")),
        client.complete(_request("two", "three")),
        client.complete(_request("two", "four")),
    )

    assert peak > config.provider_resources["shared"].default_bulk_concurrency
