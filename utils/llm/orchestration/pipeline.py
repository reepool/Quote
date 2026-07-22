"""Bounded queues and stage runners for business-owned async pipelines."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
import inspect
import logging
import time
from typing import Any, Awaitable, Callable, Deque, Optional

from ..errors import LlmDeadlineExceededError
from .coordinator import BoundedResourcePool
from .models import (
    OrchestrationError,
    OutcomeStatus,
    StageOutcome,
    StageQueueClosedError,
    StageSnapshot,
    WorkItem,
)


StageCallback = Callable[[WorkItem], Any | Awaitable[Any]]
OutcomeCallback = Callable[[StageOutcome], Any | Awaitable[Any]]
ErrorClassifier = Callable[[Exception], tuple[OutcomeStatus, str, bool]]


class BoundedStageQueue:
    """A closeable bounded queue with deterministic drain semantics."""

    def __init__(self, maxsize: int) -> None:
        if int(maxsize) < 1:
            raise ValueError("stage queue maxsize must be positive")
        self.maxsize = int(maxsize)
        self._items: Deque[tuple[WorkItem, float]] = deque()
        self._condition = asyncio.Condition()
        self._closed = False
        self._unfinished = 0

    async def put(self, item: WorkItem) -> None:
        async with self._condition:
            while len(self._items) >= self.maxsize and not self._closed:
                await self._condition.wait()
            if self._closed:
                raise StageQueueClosedError("stage queue is closed")
            self._items.append((item, time.monotonic()))
            self._unfinished += 1
            self._condition.notify_all()

    async def get(self) -> tuple[WorkItem, int]:
        async with self._condition:
            while not self._items and not self._closed:
                await self._condition.wait()
            if not self._items:
                raise StageQueueClosedError("stage queue is closed and drained")
            item, enqueued_at = self._items.popleft()
            self._condition.notify_all()
            return item, max(0, round((time.monotonic() - enqueued_at) * 1000))

    async def task_done(self) -> None:
        async with self._condition:
            if self._unfinished <= 0:
                raise ValueError("stage queue task_done called too many times")
            self._unfinished -= 1
            self._condition.notify_all()

    async def join(self) -> None:
        async with self._condition:
            while self._unfinished:
                await self._condition.wait()

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    async def drain(self) -> list[WorkItem]:
        async with self._condition:
            items = [item for item, _ in self._items]
            self._unfinished -= len(items)
            self._items.clear()
            self._condition.notify_all()
            return items

    @property
    def depth(self) -> int:
        return len(self._items)

    @property
    def closed(self) -> bool:
        return self._closed


@dataclass
class _StageMetrics:
    active: int = 0
    succeeded: int = 0
    skipped: int = 0
    retryable_failed: int = 0
    terminal_failed: int = 0
    cancelled: int = 0
    deadline_exceeded: int = 0
    total_queue_wait_ms: int = 0
    total_execution_ms: int = 0


class StageRunner:
    """Execute a caller-owned stage callback with bounded workers."""

    def __init__(
        self,
        *,
        name: str,
        queue: BoundedStageQueue,
        callback: StageCallback,
        workers: int,
        on_outcome: Optional[OutcomeCallback] = None,
        resource_pool: Optional[BoundedResourcePool] = None,
        error_classifier: Optional[ErrorClassifier] = None,
    ) -> None:
        self.name = str(name).strip()
        self.queue = queue
        self.callback = callback
        self.workers = max(1, int(workers))
        self.on_outcome = on_outcome
        self.resource_pool = resource_pool
        self.error_classifier = error_classifier or self._default_error_classifier
        self._tasks: list[asyncio.Task[None]] = []
        self._metrics = _StageMetrics()
        self._fatal_error: Optional[BaseException] = None

    async def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(
                self._worker(), name=f"llm-stage-{self.name}-{index}"
            )
            for index in range(1, self.workers + 1)
        ]

    async def close(self, *, cancel: bool = False) -> list[WorkItem]:
        await self.queue.close()
        pending: list[WorkItem] = []
        if cancel:
            pending = await self.queue.drain()
            for task in self._tasks:
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        fatal_error = self._fatal_error
        self._fatal_error = None
        if fatal_error is not None:
            raise OrchestrationError(
                f"stage {self.name} outcome publication failed"
            ) from fatal_error
        return pending

    async def _worker(self) -> None:
        while True:
            try:
                item, queue_wait_ms = await self.queue.get()
            except StageQueueClosedError:
                return
            started = time.monotonic()
            self._metrics.active += 1
            cancelled = False
            try:
                if self.resource_pool is None:
                    output = await self._invoke(item)
                else:
                    async with self.resource_pool.slot():
                        output = await self._invoke(item)
                outcome = StageOutcome(
                    item=item,
                    status=OutcomeStatus.SUCCESS,
                    output=output,
                    queue_wait_ms=queue_wait_ms,
                    execution_ms=max(
                        0, round((time.monotonic() - started) * 1000)
                    ),
                )
            except asyncio.CancelledError:
                cancelled = True
                outcome = StageOutcome(
                    item=item,
                    status=OutcomeStatus.CANCELLED,
                    error_code="cancelled",
                    error_message="stage worker cancelled",
                    queue_wait_ms=queue_wait_ms,
                    execution_ms=max(
                        0, round((time.monotonic() - started) * 1000)
                    ),
                )
            except Exception as exc:
                status, error_code, retryable = self.error_classifier(exc)
                outcome = StageOutcome(
                    item=item,
                    status=status,
                    error_code=error_code,
                    error_message=str(exc),
                    retryable=retryable,
                    queue_wait_ms=queue_wait_ms,
                    execution_ms=max(
                        0, round((time.monotonic() - started) * 1000)
                    ),
                )
            finally:
                self._metrics.active -= 1
            try:
                # Publish downstream work before acknowledging the upstream queue.
                # This makes queue.join() a reliable stage barrier for routed pipelines.
                await self._record_and_publish(outcome)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._fatal_error = self._fatal_error or exc
                self._metrics.terminal_failed += 1
                await self.queue.close()
                await self.queue.drain()
                return
            finally:
                await self.queue.task_done()
            if cancelled:
                raise asyncio.CancelledError

    async def _invoke(self, item: WorkItem) -> Any:
        value = self.callback(item)
        return await value if inspect.isawaitable(value) else value

    async def _record_and_publish(self, outcome: StageOutcome) -> None:
        if self.on_outcome is not None:
            value = self.on_outcome(outcome)
            if inspect.isawaitable(value):
                await value
        self._metrics.total_queue_wait_ms += outcome.queue_wait_ms
        self._metrics.total_execution_ms += outcome.execution_ms
        if outcome.status == OutcomeStatus.SUCCESS:
            self._metrics.succeeded += 1
        elif outcome.status == OutcomeStatus.SKIPPED_IDEMPOTENT:
            self._metrics.skipped += 1
        elif outcome.status == OutcomeStatus.RETRYABLE_FAILURE:
            self._metrics.retryable_failed += 1
        elif outcome.status == OutcomeStatus.TERMINAL_FAILURE:
            self._metrics.terminal_failed += 1
        elif outcome.status == OutcomeStatus.CANCELLED:
            self._metrics.cancelled += 1
        elif outcome.status == OutcomeStatus.DEADLINE_EXCEEDED:
            self._metrics.deadline_exceeded += 1

    @staticmethod
    def _default_error_classifier(
        exc: Exception,
    ) -> tuple[OutcomeStatus, str, bool]:
        if isinstance(exc, LlmDeadlineExceededError):
            return OutcomeStatus.DEADLINE_EXCEEDED, "deadline_exceeded", False
        return OutcomeStatus.TERMINAL_FAILURE, exc.__class__.__name__, False

    def snapshot(self) -> StageSnapshot:
        return StageSnapshot(
            stage=self.name,
            queue_depth=self.queue.depth,
            active=self._metrics.active,
            succeeded=self._metrics.succeeded,
            skipped=self._metrics.skipped,
            retryable_failed=self._metrics.retryable_failed,
            terminal_failed=self._metrics.terminal_failed,
            cancelled=self._metrics.cancelled,
            deadline_exceeded=self._metrics.deadline_exceeded,
            total_queue_wait_ms=self._metrics.total_queue_wait_ms,
            total_execution_ms=self._metrics.total_execution_ms,
        )


class OutcomeLedger:
    """Track outcomes until the business writer acknowledges durable commit."""

    def __init__(self) -> None:
        self._pending: dict[tuple[str, str, int], StageOutcome] = {}

    def add(self, outcome: StageOutcome) -> None:
        key = (
            outcome.item.work_id,
            outcome.item.stage,
            outcome.item.stage_sequence,
        )
        self._pending[key] = outcome

    def acknowledge(self, outcome: StageOutcome) -> None:
        key = (
            outcome.item.work_id,
            outcome.item.stage,
            outcome.item.stage_sequence,
        )
        self._pending.pop(key, None)

    def pending(self) -> tuple[StageOutcome, ...]:
        return tuple(self._pending.values())


class PipelineController:
    def __init__(self) -> None:
        self._stages: list[StageRunner] = []
        self.ledger = OutcomeLedger()

    def add_stage(self, stage: StageRunner) -> None:
        self._stages.append(stage)

    async def start(self) -> None:
        for stage in self._stages:
            await stage.start()

    async def close(self, *, cancel: bool = False) -> tuple[WorkItem, ...]:
        pending: list[WorkItem] = []
        stages = reversed(self._stages) if cancel else self._stages
        first_error: Optional[BaseException] = None
        for stage in stages:
            try:
                pending.extend(await stage.close(cancel=cancel))
            except BaseException as exc:
                first_error = first_error or exc
        if first_error is not None:
            raise first_error
        return tuple(pending)

    def snapshots(self) -> tuple[StageSnapshot, ...]:
        return tuple(stage.snapshot() for stage in self._stages)


class AggregateProgressLogger:
    """Periodically log aggregate snapshots without item-level message floods."""

    def __init__(
        self,
        *,
        logger: logging.Logger,
        interval_seconds: float,
        snapshot: Callable[[], Any],
        label: str,
    ) -> None:
        self.logger = logger
        self.interval_seconds = max(0.1, float(interval_seconds))
        self.snapshot = snapshot
        self.label = label
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> "AggregateProgressLogger":
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.interval_seconds
                )
                return
            except asyncio.TimeoutError:
                self.logger.info(
                    "LLM orchestration progress label=%s snapshot=%s",
                    self.label,
                    self.snapshot(),
                )
