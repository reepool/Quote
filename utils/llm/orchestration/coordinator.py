"""Shared provider and local resource coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import logging
import math
import time
from typing import AsyncIterator, Callable, Deque, Optional

from ..errors import LlmDeadlineExceededError
from ..models import ProviderResourceConfig
from .models import ProviderSnapshot, ResourceLeaseError, ResourceSnapshot


LOGGER = logging.getLogger(__name__)


@dataclass
class _Waiter:
    workload: str
    bulk: bool
    enqueued_at: float
    future: asyncio.Future[None]
    admitted: bool = False
    rpm_wait_started_at: Optional[float] = None


@dataclass(frozen=True)
class _ProviderOutcome:
    timestamp: float
    outcome: str


class ProviderCoordinator:
    """Fair in-process admission for one provider account resource."""

    def __init__(
        self,
        config: ProviderResourceConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
        rpm_window_seconds: float = 60.0,
    ) -> None:
        self.config = config
        self._clock = clock
        self._lock = asyncio.Lock()
        self._queues: dict[str, Deque[_Waiter]] = {}
        self._schedule: tuple[str, ...] = ()
        self._schedule_cursor = 0
        self._workload_weights = dict(config.workload_weights)
        self._active = 0
        self._active_bulk = 0
        self._active_by_workload: dict[str, int] = {}
        self._cooldown_until = 0.0
        self._cooldown_handle: Optional[asyncio.TimerHandle] = None
        self._rpm_window_seconds = max(0.01, float(rpm_window_seconds))
        self._rpm_timestamps: Deque[float] = deque()
        self._rpm_wake_handle: Optional[asyncio.TimerHandle] = None
        self._total_rpm_wait_ms = 0
        self._effective_bulk_concurrency = config.default_bulk_concurrency
        self._adaptive_min_bulk_concurrency = max(
            1,
            min(
                config.adaptive_min_bulk_concurrency,
                config.default_bulk_concurrency,
            ),
        )
        self._adaptive_retryable_failures = 0
        self._adaptive_success_streak = 0
        self._adaptive_congestion_events = 0
        self._adaptive_coalesced_failures = 0
        self._adaptive_recovery_probes = 0
        self._adaptive_last_failure_class: Optional[str] = None
        self._adaptive_last_failure_at: Optional[float] = None
        self._adaptive_last_recovery_probe_at: Optional[float] = None
        self._adaptive_episode_until = 0.0
        self._adaptive_episode_failure_class: Optional[str] = None
        self._adaptive_episode_base_limit: Optional[int] = None
        self._adaptive_soft_evidence_after = 0.0
        self._adaptive_outcomes: Deque[_ProviderOutcome] = deque(
            maxlen=config.adaptive_outcome_window_size
        )
        self._admitted = 0
        self._admitted_by_workload: dict[str, int] = {}
        self._completed = 0
        self._completed_by_workload: dict[str, int] = {}
        self._cancelled = 0
        self._deadline_exceeded = 0
        self._total_admission_wait_ms = 0

    async def acquire(
        self,
        *,
        workload: str,
        deadline: float,
        bulk: bool,
    ) -> None:
        loop = asyncio.get_running_loop()
        waiter = _Waiter(
            workload=str(workload or "direct").strip() or "direct",
            bulk=bool(bulk),
            enqueued_at=self._clock(),
            future=loop.create_future(),
        )
        async with self._lock:
            self._queues.setdefault(waiter.workload, deque()).append(waiter)
            self._rebuild_schedule_locked()
            self._dispatch_locked()
        try:
            remaining = deadline - self._clock()
            if remaining <= 0:
                raise asyncio.TimeoutError
            await asyncio.wait_for(asyncio.shield(waiter.future), timeout=remaining)
        except asyncio.TimeoutError as exc:
            await self._withdraw(waiter, deadline_exceeded=True)
            raise LlmDeadlineExceededError() from exc
        except BaseException:
            await self._withdraw(waiter, deadline_exceeded=False)
            raise

    async def _withdraw(self, waiter: _Waiter, *, deadline_exceeded: bool) -> None:
        async with self._lock:
            if waiter.admitted:
                self._release_locked(
                    waiter.workload, waiter.bulk, completed=False
                )
            else:
                queue = self._queues.get(waiter.workload)
                if queue is not None:
                    try:
                        queue.remove(waiter)
                    except ValueError:
                        pass
                    if not queue:
                        self._queues.pop(waiter.workload, None)
                        self._rebuild_schedule_locked()
                if not waiter.future.done():
                    waiter.future.cancel()
            self._record_rpm_wait_locked(waiter)
            if deadline_exceeded:
                self._deadline_exceeded += 1
            else:
                self._cancelled += 1
            self._dispatch_locked()

    async def release(self, *, workload: str, bulk: bool) -> None:
        async with self._lock:
            self._release_locked(workload, bulk, completed=True)
            self._dispatch_locked()

    def _release_locked(
        self, workload: str, bulk: bool, *, completed: bool
    ) -> None:
        if self._active <= 0:
            raise RuntimeError("provider coordinator release without active lease")
        self._active -= 1
        if bulk:
            if self._active_bulk <= 0:
                raise RuntimeError("provider coordinator bulk release without active lease")
            self._active_bulk -= 1
        active_for_workload = self._active_by_workload.get(workload, 0)
        if active_for_workload <= 0:
            raise RuntimeError(
                "provider coordinator workload release without active lease"
            )
        if active_for_workload == 1:
            self._active_by_workload.pop(workload, None)
        else:
            self._active_by_workload[workload] = active_for_workload - 1
        if completed:
            self._completed += 1
            self._completed_by_workload[workload] = (
                self._completed_by_workload.get(workload, 0) + 1
            )

    @asynccontextmanager
    async def slot(
        self,
        *,
        workload: str,
        deadline: float,
        bulk: bool = False,
    ) -> AsyncIterator[None]:
        await self.acquire(workload=workload, deadline=deadline, bulk=bulk)
        try:
            yield
        finally:
            await self.release(workload=workload, bulk=bulk)

    async def set_cooldown(self, seconds: float) -> None:
        delay = max(0.0, float(seconds))
        if delay <= 0:
            return
        async with self._lock:
            self._cooldown_until = max(
                self._cooldown_until, self._clock() + delay
            )
            self._schedule_cooldown_wake_locked()

    async def report_retryable_failure(
        self,
        *,
        error_code: str = "",
        status_code: Optional[int] = None,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        """Record one retryable outcome and adapt provider-wide admission."""
        normalized_code = str(error_code or "").strip().lower()
        is_rate_limit = status_code == 429 or normalized_code == "rate_limit_error"
        failure_class = "hard_rate_limit" if is_rate_limit else "soft_transient"
        configured_cooldown = (
            self.config.rate_limit_cooldown_seconds
            if is_rate_limit
            else self.config.transient_cooldown_seconds
        )
        cooldown = max(
            configured_cooldown,
            max(0.0, float(retry_after_seconds or 0.0)),
        )
        async with self._lock:
            now = self._clock()
            prior_limit = self._effective_bulk_concurrency
            self._adaptive_retryable_failures += 1
            self._adaptive_success_streak = 0
            self._adaptive_last_failure_class = failure_class
            self._adaptive_last_failure_at = now
            self._adaptive_last_recovery_probe_at = None
            self._adaptive_outcomes.append(_ProviderOutcome(
                timestamp=now,
                outcome=failure_class,
            ))
            if cooldown > 0:
                self._cooldown_until = max(
                    self._cooldown_until, now + cooldown
                )
                self._schedule_cooldown_wake_locked()
            if not self.config.adaptive_concurrency_enabled:
                return

            if now < self._adaptive_episode_until:
                if (
                    is_rate_limit
                    and self._adaptive_episode_failure_class
                    != "hard_rate_limit"
                ):
                    episode_base_limit = (
                        self._adaptive_episode_base_limit or prior_limit
                    )
                    reduced = math.floor(
                        episode_base_limit
                        * self.config.adaptive_hard_decrease_ratio
                    )
                    if episode_base_limit > self._adaptive_min_bulk_concurrency:
                        reduced = min(reduced, episode_base_limit - 1)
                    hard_limit = max(
                        self._adaptive_min_bulk_concurrency,
                        reduced,
                    )
                    self._effective_bulk_concurrency = min(
                        prior_limit,
                        hard_limit,
                    )
                    self._adaptive_episode_failure_class = "hard_rate_limit"
                    episode_seconds = (
                        self.config.adaptive_failure_coalescing_seconds
                    )
                    self._adaptive_episode_until = max(
                        self._adaptive_episode_until,
                        now + episode_seconds,
                    )
                    self._adaptive_soft_evidence_after = max(
                        self._adaptive_soft_evidence_after,
                        self._adaptive_episode_until,
                    )
                    LOGGER.warning(
                        "event=llm.provider.congestion_escalated resource=%s "
                        "class=%s code=%s status=%s base_limit=%s "
                        "bulk_limit=%s->%s ratio=%.3f cooldown_seconds=%.1f "
                        "episode_seconds=%.1f events=%s raw_failures=%s",
                        self.config.name,
                        failure_class,
                        normalized_code or "rate_limit_error",
                        status_code,
                        episode_base_limit,
                        prior_limit,
                        self._effective_bulk_concurrency,
                        self.config.adaptive_hard_decrease_ratio,
                        cooldown,
                        episode_seconds,
                        self._adaptive_congestion_events,
                        self._adaptive_retryable_failures,
                    )
                    return
                self._adaptive_coalesced_failures += 1
                LOGGER.debug(
                    "event=llm.provider.failure_coalesced resource=%s "
                    "class=%s code=%s status=%s episode_remaining_seconds=%.1f "
                    "raw_failures=%s coalesced=%s",
                    self.config.name,
                    failure_class,
                    normalized_code or "retryable_provider_error",
                    status_code,
                    self._adaptive_episode_until - now,
                    self._adaptive_retryable_failures,
                    self._adaptive_coalesced_failures,
                )
                return

            window_requests, soft_failures, soft_failure_rate = (
                self._adaptive_window_stats_locked()
            )
            should_decrease = is_rate_limit or (
                soft_failures >= self.config.adaptive_soft_failure_min_count
                and soft_failure_rate
                >= self.config.adaptive_soft_failure_rate_threshold
            )
            if not should_decrease:
                LOGGER.warning(
                    "event=llm.provider.transient_failure resource=%s code=%s "
                    "status=%s window_requests=%s soft_failures=%s "
                    "failure_rate=%.3f threshold_count=%s threshold_rate=%.3f",
                    self.config.name,
                    normalized_code or "retryable_provider_error",
                    status_code,
                    window_requests,
                    soft_failures,
                    soft_failure_rate,
                    self.config.adaptive_soft_failure_min_count,
                    self.config.adaptive_soft_failure_rate_threshold,
                )
                return

            ratio = (
                self.config.adaptive_hard_decrease_ratio
                if is_rate_limit
                else self.config.adaptive_soft_decrease_ratio
            )
            reduced = math.floor(prior_limit * ratio)
            if prior_limit > self._adaptive_min_bulk_concurrency:
                reduced = min(reduced, prior_limit - 1)
            self._effective_bulk_concurrency = max(
                self._adaptive_min_bulk_concurrency,
                reduced,
            )
            episode_seconds = self.config.adaptive_failure_coalescing_seconds
            self._adaptive_episode_until = now + episode_seconds
            self._adaptive_episode_failure_class = failure_class
            self._adaptive_episode_base_limit = prior_limit
            self._adaptive_soft_evidence_after = self._adaptive_episode_until
            self._adaptive_congestion_events += 1
            if not is_rate_limit:
                episode_start = now - episode_seconds
                episode_failures = sum(
                    1
                    for outcome in self._adaptive_outcomes
                    if outcome.outcome == "soft_transient"
                    and outcome.timestamp >= episode_start
                )
                self._adaptive_coalesced_failures += max(
                    0, episode_failures - 1
                )
            LOGGER.warning(
                "event=llm.provider.congestion_started resource=%s class=%s "
                "code=%s status=%s bulk_limit=%s->%s ratio=%.3f "
                "cooldown_seconds=%.1f episode_seconds=%.1f "
                "window_requests=%s soft_failures=%s failure_rate=%.3f "
                "events=%s raw_failures=%s coalesced=%s",
                self.config.name,
                failure_class,
                normalized_code or "retryable_provider_error",
                status_code,
                prior_limit,
                self._effective_bulk_concurrency,
                ratio,
                cooldown,
                episode_seconds,
                window_requests,
                soft_failures,
                soft_failure_rate,
                self._adaptive_congestion_events,
                self._adaptive_retryable_failures,
                self._adaptive_coalesced_failures,
            )

    async def report_success(self) -> None:
        """Probe higher concurrency after quiet, sustained provider success."""
        if not self.config.adaptive_concurrency_enabled:
            return
        async with self._lock:
            now = self._clock()
            self._adaptive_outcomes.append(_ProviderOutcome(
                timestamp=now,
                outcome="success",
            ))
            if self._effective_bulk_concurrency >= self.config.default_bulk_concurrency:
                self._adaptive_success_streak = 0
                return
            self._adaptive_success_streak += 1
            if (
                self._adaptive_success_streak
                < self.config.adaptive_recovery_successes
            ):
                return
            recovery_ready_at = self._adaptive_recovery_ready_at_locked()
            if now < recovery_ready_at:
                return
            prior_limit = self._effective_bulk_concurrency
            self._effective_bulk_concurrency = min(
                self.config.default_bulk_concurrency,
                max(
                    prior_limit + 1,
                    math.ceil(
                        prior_limit
                        * self.config.adaptive_recovery_growth_factor
                    ),
                ),
            )
            self._adaptive_success_streak = 0
            self._adaptive_recovery_probes += 1
            self._adaptive_last_recovery_probe_at = now
            LOGGER.info(
                "event=llm.provider.recovery_probe resource=%s bulk_limit=%s->%s "
                "growth_factor=%.3f probes=%s",
                self.config.name,
                prior_limit,
                self._effective_bulk_concurrency,
                self.config.adaptive_recovery_growth_factor,
                self._adaptive_recovery_probes,
            )
            self._dispatch_locked()

    def _adaptive_window_stats_locked(self) -> tuple[int, int, float]:
        eligible = [
            outcome
            for outcome in self._adaptive_outcomes
            if outcome.timestamp >= self._adaptive_soft_evidence_after
        ]
        request_count = len(eligible)
        soft_failures = sum(
            1 for outcome in eligible if outcome.outcome == "soft_transient"
        )
        failure_rate = (
            soft_failures / request_count if request_count else 0.0
        )
        return request_count, soft_failures, failure_rate

    def _adaptive_recovery_ready_at_locked(self) -> float:
        ready_at = max(self._cooldown_until, self._adaptive_episode_until)
        if self._adaptive_last_failure_at is not None:
            ready_at = max(
                ready_at,
                self._adaptive_last_failure_at
                + self.config.adaptive_recovery_quiet_seconds,
            )
        if self._adaptive_last_recovery_probe_at is not None:
            ready_at = max(
                ready_at,
                self._adaptive_last_recovery_probe_at
                + self.config.adaptive_recovery_probe_interval_seconds,
            )
        return ready_at

    def _schedule_cooldown_wake_locked(self) -> None:
        remaining = self._cooldown_until - self._clock()
        if remaining <= 0:
            self._dispatch_locked()
            return
        if self._cooldown_handle is not None:
            self._cooldown_handle.cancel()
        loop = asyncio.get_running_loop()
        self._cooldown_handle = loop.call_later(
            remaining, lambda: asyncio.create_task(self._cooldown_elapsed())
        )

    def _prune_rpm_locked(self, now: float) -> None:
        while (
            self._rpm_timestamps
            and now - self._rpm_timestamps[0] >= self._rpm_window_seconds
        ):
            self._rpm_timestamps.popleft()

    def _rpm_is_exhausted_locked(self, now: float) -> bool:
        self._prune_rpm_locked(now)
        return (
            self.config.requests_per_minute > 0
            and len(self._rpm_timestamps) >= self.config.requests_per_minute
        )

    def _mark_rpm_waiters_locked(self, now: float) -> None:
        for queue in self._queues.values():
            for waiter in queue:
                if waiter.rpm_wait_started_at is None:
                    waiter.rpm_wait_started_at = now

    def _record_rpm_wait_locked(self, waiter: _Waiter) -> None:
        if waiter.rpm_wait_started_at is None:
            return
        self._total_rpm_wait_ms += max(
            0,
            round((self._clock() - waiter.rpm_wait_started_at) * 1000),
        )
        waiter.rpm_wait_started_at = None

    def _schedule_rpm_wake_locked(self, now: float) -> None:
        if not self._rpm_timestamps:
            return
        remaining = (
            self._rpm_timestamps[0] + self._rpm_window_seconds - now
        )
        if remaining <= 0:
            self._dispatch_locked()
            return
        if self._rpm_wake_handle is not None:
            self._rpm_wake_handle.cancel()
        loop = asyncio.get_running_loop()
        self._rpm_wake_handle = loop.call_later(
            remaining,
            lambda: asyncio.create_task(self._rpm_elapsed()),
        )

    async def _rpm_elapsed(self) -> None:
        async with self._lock:
            self._rpm_wake_handle = None
            self._dispatch_locked()

    async def _cooldown_elapsed(self) -> None:
        async with self._lock:
            self._cooldown_handle = None
            self._dispatch_locked()

    def _dispatch_locked(self) -> None:
        now = self._clock()
        if now < self._cooldown_until:
            self._schedule_cooldown_wake_locked()
            return
        while self._active < self.config.hard_max_concurrency:
            if not self._schedule:
                break
            now = self._clock()
            if self._rpm_is_exhausted_locked(now):
                self._mark_rpm_waiters_locked(now)
                self._schedule_rpm_wake_locked(now)
                return
            waiter = self._next_eligible_waiter_locked()
            if waiter is None:
                break
            self._record_rpm_wait_locked(waiter)
            waiter.admitted = True
            if self.config.requests_per_minute > 0:
                self._rpm_timestamps.append(now)
            self._active += 1
            if waiter.bulk:
                self._active_bulk += 1
            self._active_by_workload[waiter.workload] = (
                self._active_by_workload.get(waiter.workload, 0) + 1
            )
            self._admitted += 1
            self._admitted_by_workload[waiter.workload] = (
                self._admitted_by_workload.get(waiter.workload, 0) + 1
            )
            self._total_admission_wait_ms += max(
                0, round((self._clock() - waiter.enqueued_at) * 1000)
            )
            if not waiter.future.done():
                waiter.future.set_result(None)

    def _next_eligible_waiter_locked(self) -> Optional[_Waiter]:
        if not self._schedule:
            return None
        attempts = len(self._schedule)
        for _ in range(attempts):
            workload = self._schedule[self._schedule_cursor % len(self._schedule)]
            self._schedule_cursor = (self._schedule_cursor + 1) % len(
                self._schedule
            )
            queue = self._queues.get(workload)
            if not queue:
                continue
            waiter = queue[0]
            if (
                waiter.bulk
                and self._active_bulk >= self._effective_bulk_concurrency
            ):
                continue
            queue.popleft()
            if not queue:
                self._queues.pop(workload, None)
                self._rebuild_schedule_locked()
            return waiter
        return None

    def _rebuild_schedule_locked(self) -> None:
        schedule: list[str] = []
        for workload in sorted(self._queues):
            weight = max(1, int(self._workload_weights.get(workload, 1)))
            schedule.extend([workload] * weight)
        self._schedule = tuple(schedule)
        if not self._schedule:
            self._schedule_cursor = 0
        else:
            self._schedule_cursor %= len(self._schedule)

    def snapshot(self) -> ProviderSnapshot:
        now = self._clock()
        self._prune_rpm_locked(now)
        waiting_by_workload = {
            workload: len(queue)
            for workload, queue in self._queues.items()
            if queue
        }
        window_requests, soft_failures, soft_failure_rate = (
            self._adaptive_window_stats_locked()
        )
        recovery_ready_at = self._adaptive_recovery_ready_at_locked()
        rpm_exhausted = self._rpm_is_exhausted_locked(now)
        rpm_next_admission_seconds = (
            max(
                0.0,
                self._rpm_timestamps[0] + self._rpm_window_seconds - now,
            )
            if rpm_exhausted and self._rpm_timestamps
            else 0.0
        )
        rpm_waiting = (
            sum(waiting_by_workload.values()) if rpm_exhausted else 0
        )
        if now < self._cooldown_until:
            adaptive_state = "cooldown"
        elif now < self._adaptive_episode_until:
            adaptive_state = "congestion_episode"
        elif self._effective_bulk_concurrency < self.config.default_bulk_concurrency:
            adaptive_state = (
                "recovery_hold" if now < recovery_ready_at else "recovery_probe"
            )
        else:
            adaptive_state = "steady"
        return ProviderSnapshot(
            resource_name=self.config.name,
            active=self._active,
            active_bulk=self._active_bulk,
            active_by_workload=dict(self._active_by_workload),
            waiting=sum(waiting_by_workload.values()),
            waiting_by_workload=waiting_by_workload,
            admitted=self._admitted,
            admitted_by_workload=dict(self._admitted_by_workload),
            completed=self._completed,
            completed_by_workload=dict(self._completed_by_workload),
            cancelled=self._cancelled,
            deadline_exceeded=self._deadline_exceeded,
            cooldown_remaining_seconds=max(
                0.0, self._cooldown_until - self._clock()
            ),
            configured_bulk_concurrency=self.config.default_bulk_concurrency,
            effective_bulk_concurrency=self._effective_bulk_concurrency,
            configured_requests_per_minute=self.config.requests_per_minute,
            rpm_window_requests=len(self._rpm_timestamps),
            rpm_waiting=rpm_waiting,
            rpm_next_admission_seconds=rpm_next_admission_seconds,
            total_rpm_wait_ms=self._total_rpm_wait_ms,
            adaptive_retryable_failures=self._adaptive_retryable_failures,
            adaptive_success_streak=self._adaptive_success_streak,
            adaptive_congestion_events=self._adaptive_congestion_events,
            adaptive_coalesced_failures=self._adaptive_coalesced_failures,
            adaptive_recovery_probes=self._adaptive_recovery_probes,
            adaptive_window_requests=window_requests,
            adaptive_window_soft_failures=soft_failures,
            adaptive_window_failure_rate=soft_failure_rate,
            adaptive_last_failure_class=self._adaptive_last_failure_class,
            adaptive_state=adaptive_state,
            adaptive_episode_remaining_seconds=max(
                0.0, self._adaptive_episode_until - now
            ),
            adaptive_recovery_quiet_remaining_seconds=max(
                0.0, recovery_ready_at - now
            ),
            total_admission_wait_ms=self._total_admission_wait_ms,
        )

    def add_workload_weights(self, weights: dict[str, int]) -> None:
        for workload, weight in weights.items():
            self._workload_weights[str(workload)] = max(1, int(weight))

    async def close(self) -> None:
        async with self._lock:
            if self._cooldown_handle is not None:
                self._cooldown_handle.cancel()
                self._cooldown_handle = None
            if self._rpm_wake_handle is not None:
                self._rpm_wake_handle.cancel()
                self._rpm_wake_handle = None
            for queue in self._queues.values():
                for waiter in queue:
                    if not waiter.future.done():
                        waiter.future.cancel()
            self._queues.clear()
            self._schedule = ()


class ProviderCoordinatorRegistry:
    """Event-loop-local coordinators shared by all clients in the process."""

    def __init__(self) -> None:
        self._coordinators: dict[str, ProviderCoordinator] = {}
        self._loop_id: Optional[int] = None

    def get(self, config: ProviderResourceConfig) -> ProviderCoordinator:
        loop_id = id(asyncio.get_running_loop())
        if self._loop_id != loop_id:
            self._coordinators.clear()
            self._loop_id = loop_id
        coordinator = self._coordinators.get(config.name)
        if coordinator is None:
            coordinator = ProviderCoordinator(config)
            self._coordinators[config.name] = coordinator
        elif self._coordination_limits(coordinator.config) != (
            self._coordination_limits(config)
        ):
            raise ValueError(
                f"conflicting provider resource configuration: {config.name}"
            )
        else:
            coordinator.add_workload_weights(dict(config.workload_weights))
        return coordinator

    @staticmethod
    def _coordination_limits(
        config: ProviderResourceConfig,
    ) -> tuple[object, ...]:
        return (
            config.provider,
            config.hard_max_concurrency,
            config.default_bulk_concurrency,
            config.reserved_concurrency,
            config.requests_per_minute,
            config.adaptive_concurrency_enabled,
            config.adaptive_min_bulk_concurrency,
            config.adaptive_recovery_successes,
            config.adaptive_failure_coalescing_seconds,
            config.adaptive_outcome_window_size,
            config.adaptive_soft_failure_min_count,
            config.adaptive_soft_failure_rate_threshold,
            config.adaptive_soft_decrease_ratio,
            config.adaptive_hard_decrease_ratio,
            config.adaptive_recovery_quiet_seconds,
            config.adaptive_recovery_probe_interval_seconds,
            config.adaptive_recovery_growth_factor,
            config.rate_limit_cooldown_seconds,
            config.transient_cooldown_seconds,
        )

    def snapshots(self) -> tuple[ProviderSnapshot, ...]:
        return tuple(
            coordinator.snapshot()
            for coordinator in self._coordinators.values()
        )

    def clear(self) -> None:
        self._coordinators.clear()
        self._loop_id = None

    async def close_all(self) -> None:
        """Cancel provider waiters and release event-loop-owned state."""
        coordinators = tuple(self._coordinators.values())
        self._coordinators.clear()
        self._loop_id = None
        for coordinator in coordinators:
            await coordinator.close()


_HELD_RESOURCES: ContextVar[tuple[str, ...]] = ContextVar(
    "llm_orchestration_held_resources", default=()
)


class BoundedResourcePool:
    """A cancellation-safe local resource pool with nested-lease protection."""

    def __init__(self, name: str, limit: int) -> None:
        self.name = str(name).strip()
        self.limit = max(1, int(limit))
        self._semaphore = asyncio.Semaphore(self.limit)
        self._active = 0
        self._waiting = 0
        self._acquired = 0
        self._released = 0

    async def acquire(self, deadline: Optional[float] = None) -> None:
        held = _HELD_RESOURCES.get()
        if held:
            raise ResourceLeaseError(
                f"cannot acquire {self.name} while holding resource "
                f"{held[-1]}"
            )
        self._waiting += 1
        try:
            if deadline is None:
                await self._semaphore.acquire()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LlmDeadlineExceededError()
                try:
                    await asyncio.wait_for(
                        self._semaphore.acquire(), timeout=remaining
                    )
                except asyncio.TimeoutError as exc:
                    raise LlmDeadlineExceededError() from exc
        finally:
            self._waiting -= 1
        self._active += 1
        self._acquired += 1
        _HELD_RESOURCES.set(held + (self.name,))

    def release(self) -> None:
        if self._active <= 0:
            raise RuntimeError(f"resource pool {self.name} released without lease")
        held = _HELD_RESOURCES.get()
        if not held or held[-1] != self.name:
            raise ResourceLeaseError(
                f"resource pool {self.name} must be released by its lease owner"
            )
        _HELD_RESOURCES.set(held[:-1])
        self._active -= 1
        self._released += 1
        self._semaphore.release()

    @asynccontextmanager
    async def slot(self, deadline: Optional[float] = None) -> AsyncIterator[None]:
        await self.acquire(deadline)
        try:
            yield
        finally:
            self.release()

    def snapshot(self) -> ResourceSnapshot:
        return ResourceSnapshot(
            resource_name=self.name,
            limit=self.limit,
            active=self._active,
            waiting=self._waiting,
            acquired=self._acquired,
            released=self._released,
        )


class ResourcePoolRegistry:
    def __init__(self, limits: dict[str, int]) -> None:
        self._pools = {
            name: BoundedResourcePool(name, limit)
            for name, limit in limits.items()
        }

    def get(self, name: str) -> BoundedResourcePool:
        try:
            return self._pools[name]
        except KeyError as exc:
            raise KeyError(f"unknown orchestration resource pool: {name}") from exc

    def snapshots(self) -> tuple[ResourceSnapshot, ...]:
        return tuple(pool.snapshot() for pool in self._pools.values())
