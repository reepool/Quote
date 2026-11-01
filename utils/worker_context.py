"""Worker-loop execution context shared between the scheduler and research layers.

Neutral home for the main-loop registry, per-run worker state and progress
reporting so scheduler tasks and business services can cooperate on worker-loop
isolation (openspec: isolate-long-tasks-from-api-event-loop) without the
research layer importing scheduler internals.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
import time
from typing import Any, Callable, Coroutine, Dict, Optional

_main_loop_lock = threading.Lock()
_main_loop: Optional[asyncio.AbstractEventLoop] = None

_current_run: contextvars.ContextVar[Optional["WorkerRunState"]] = contextvars.ContextVar(
    "worker_current_run", default=None
)


def set_main_loop(loop: Optional[asyncio.AbstractEventLoop]) -> None:
    """Register the process main event loop (called once at startup)."""
    global _main_loop
    with _main_loop_lock:
        _main_loop = loop


def get_main_loop() -> Optional[asyncio.AbstractEventLoop]:
    """Return the registered main loop, or None when unknown (tests, CLI)."""
    with _main_loop_lock:
        return _main_loop


def is_worker_loop() -> bool:
    """Whether the current coroutine runs on a worker loop instead of the main loop."""
    try:
        current: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
    except RuntimeError:
        return False
    registered = get_main_loop()
    return registered is not None and current is not registered


async def run_on_main_loop(factory: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    """Run a coroutine factory on the main loop from any loop.

    On the main loop (or when no main loop is registered, e.g. tests/CLI) the
    factory is awaited directly. From a worker loop the call is marshalled via
    ``run_coroutine_threadsafe`` and awaited non-blockingly with
    ``asyncio.wrap_future`` — never ``.result()``, which would freeze the worker
    loop and inflate cancellation latency past the report-send timeout.
    """
    current = asyncio.get_running_loop()
    target = get_main_loop()
    if target is None or target is current:
        return await factory()
    return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(factory(), target))


def _cancel_handle(handle: asyncio.Task) -> None:
    if not handle.done():
        handle.cancel()


class WorkerRunState:
    """Per-run worker execution state: cancellation handle plus progress snapshot.

    Written from the worker thread (progress) and the main loop (cancel
    requests); read from the main loop (heartbeat, /status, shutdown). All
    cross-thread access goes through the internal lock.
    """

    def __init__(
        self,
        *,
        job_id: str,
        run_id: str,
        max_runtime_seconds: Optional[float] = None,
    ) -> None:
        self.job_id = job_id
        self.run_id = run_id
        self.max_runtime_seconds = max_runtime_seconds
        self.started_at = time.monotonic()
        self.finished = False
        self._lock = threading.Lock()
        self._phase = "queued"
        self._progress: Dict[str, Any] = {}
        self._last_progress_at = self.started_at
        self._handle: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._pending_cancel = False
        self._stop_requested = False

    def attach_handle(self, handle: asyncio.Task, loop: asyncio.AbstractEventLoop) -> None:
        with self._lock:
            self._handle = handle
            self._loop = loop
            if self._phase == "queued":
                self._phase = "running"

    def consume_pending_cancel(self) -> bool:
        """Whether a cancel arrived before the worker task handle existed."""
        with self._lock:
            pending = self._pending_cancel
            self._pending_cancel = False
            return pending

    def request_cancel(self, reason: str = "") -> bool:
        """Thread-safe cancellation injection at the task's next await checkpoint."""
        with self._lock:
            self._stop_requested = True
            self._pending_cancel = True
            handle = self._handle
            loop = self._loop
        if handle is None or loop is None or handle.done():
            # No handle yet: the starter coroutine honours _pending_cancel as
            # soon as it attaches; a finished run needs no cancellation.
            return handle is not None and not handle.done()
        try:
            loop.call_soon_threadsafe(_cancel_handle, handle)
        except RuntimeError:
            # Worker loop already closed; the thread is on its way out.
            return False
        del reason  # logged by callers
        return True

    @property
    def stop_requested(self) -> bool:
        with self._lock:
            return self._stop_requested

    def update_progress(self, phase: Optional[str] = None, **counters: Any) -> None:
        now = time.monotonic()
        with self._lock:
            if phase:
                self._phase = phase
            self._progress.update(counters)
            self._last_progress_at = now

    def mark_finished(self) -> None:
        with self._lock:
            self.finished = True
            self._phase = "finished"

    def snapshot(self) -> Dict[str, Any]:
        """Thread-safe copy for heartbeat logging and status queries."""
        now = time.monotonic()
        with self._lock:
            return {
                "job_id": self.job_id,
                "run_id": self.run_id,
                "phase": self._phase,
                "progress": dict(self._progress),
                "elapsed_seconds": round(now - self.started_at, 1),
                "seconds_since_progress": round(now - self._last_progress_at, 1),
                "max_runtime_seconds": self.max_runtime_seconds,
                "stop_requested": self._stop_requested,
                "finished": self.finished,
            }


def bind_current_run(state: WorkerRunState) -> None:
    """Bind the worker run state to the current context (worker starter only)."""
    _current_run.set(state)


def report_progress(phase: Optional[str] = None, **counters: Any) -> None:
    """Checkpoint helper: record progress of the run bound to this context.

    No-op outside a worker run (main-loop escape-hatch jobs, tests, CLI), so
    business services can call it unconditionally at checkpoint sites.
    """
    state = _current_run.get()
    if state is not None:
        state.update_progress(phase=phase, **counters)


def get_current_run_state() -> Optional[WorkerRunState]:
    return _current_run.get()
