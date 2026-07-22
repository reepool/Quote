"""Shared provider and local resource coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import time
from typing import AsyncIterator, Callable, Deque, Optional

from ..errors import LlmDeadlineExceededError
from ..models import ProviderResourceConfig
from .models import ProviderSnapshot, ResourceLeaseError, ResourceSnapshot


@dataclass
class _Waiter:
    workload: str
    bulk: bool
    enqueued_at: float
    future: asyncio.Future[None]
    admitted: bool = False


class ProviderCoordinator:
    """Fair in-process admission for one provider account resource."""

    def __init__(
        self,
        config: ProviderResourceConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
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

    async def _cooldown_elapsed(self) -> None:
        async with self._lock:
            self._cooldown_handle = None
            self._dispatch_locked()

    def _dispatch_locked(self) -> None:
        if self._clock() < self._cooldown_until:
            self._schedule_cooldown_wake_locked()
            return
        while self._active < self.config.hard_max_concurrency:
            waiter = self._next_eligible_waiter_locked()
            if waiter is None:
                break
            waiter.admitted = True
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
                and self._active_bulk >= self.config.default_bulk_concurrency
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
        waiting_by_workload = {
            workload: len(queue)
            for workload, queue in self._queues.items()
            if queue
        }
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
    ) -> tuple[str, int, int, int]:
        return (
            config.provider,
            config.hard_max_concurrency,
            config.default_bulk_concurrency,
            config.reserved_concurrency,
        )

    def snapshots(self) -> tuple[ProviderSnapshot, ...]:
        return tuple(
            coordinator.snapshot()
            for coordinator in self._coordinators.values()
        )

    def clear(self) -> None:
        self._coordinators.clear()
        self._loop_id = None


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
