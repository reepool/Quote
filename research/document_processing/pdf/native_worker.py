"""Supervised native PDFium/pypdf worker pool.

The parent process never calls the production PDFium or pypdf adapters when a
pool is configured. Workers are deliberately small protocol endpoints: they
receive immutable PDF bytes, emit serializable progress/results, and never
write business state.
"""

from __future__ import annotations

import multiprocessing
import queue
import threading
import time
import atexit
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

NATIVE_WORKER_PROTOCOL = "quote-pdf-native-worker.v1"


class NativeWorkerFailure(RuntimeError):
    """A bounded failure crossing the supervised native-worker boundary."""

    def __init__(self, code: str, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


def _native_worker_main(worker_id: int, input_queue: Any, output_queue: Any) -> None:
    """Run one clean native worker until its parent closes the pool."""
    from .adapters import _render_pdfium_batch
    from .core import PdfiumNativeAdapter, PypdfNativeAdapter

    adapters = {
        "pypdfium2": PdfiumNativeAdapter(),
        "pypdf": PypdfNativeAdapter(),
    }
    while True:
        task = input_queue.get()
        if task is None:
            return
        request_id = str(task.get("request_id", ""))
        try:
            if task.get("protocol") != NATIVE_WORKER_PROTOCOL:
                raise ValueError("native worker protocol mismatch")
            operation = str(task.get("operation"))
            content = bytes(task.get("content", b""))
            pages = tuple(int(page) for page in task.get("pages", ()))
            if operation == "extract":
                engine = str(task.get("engine"))
                adapter = adapters.get(engine)
                if adapter is None:
                    raise ValueError(f"unsupported native engine: {engine}")

                def progress(page: Any) -> None:
                    output_queue.put({
                        "protocol": NATIVE_WORKER_PROTOCOL,
                        "kind": "page",
                        "worker_id": worker_id,
                        "request_id": request_id,
                        "page": page,
                    })

                def page_count(count: int) -> None:
                    output_queue.put({
                        "protocol": NATIVE_WORKER_PROTOCOL,
                        "kind": "page_count",
                        "worker_id": worker_id,
                        "request_id": request_id,
                        "page_count": int(count),
                    })

                result = adapter.extract(
                    content,
                    target_pages=pages,
                    progress_callback=progress,
                    page_count_callback=page_count,
                )
                output_queue.put({
                    "protocol": NATIVE_WORKER_PROTOCOL,
                    "kind": "result",
                    "worker_id": worker_id,
                    "request_id": request_id,
                    "result": result,
                })
            elif operation == "render":
                images = _render_pdfium_batch(content, pages, int(task["dpi"]))
                output_queue.put({
                    "protocol": NATIVE_WORKER_PROTOCOL,
                    "kind": "render_result",
                    "worker_id": worker_id,
                    "request_id": request_id,
                    "images": images,
                })
            else:
                raise ValueError(f"unsupported native worker operation: {operation}")
        except BaseException as exc:
            # BaseException is intentional at the worker boundary: the parent
            # still receives a typed failure for KeyboardInterrupt/SystemExit.
            output_queue.put({
                "protocol": NATIVE_WORKER_PROTOCOL,
                "kind": "error",
                "worker_id": worker_id,
                "request_id": request_id,
                "error_type": type(exc).__name__,
                "error": str(exc),
            })


@dataclass
class _WorkerSlot:
    worker_id: int
    input_queue: Any
    output_queue: Any
    process: Any
    busy: bool = False
    disabled: bool = False


class NativeWorkerPool:
    """Persistent, bounded, signal-aware native worker pool."""

    def __init__(
        self,
        *,
        max_workers: int = 1,
        queue_size: int = 32,
        queue_wait_seconds: float = 30.0,
        task_timeout_seconds: float = 900.0,
        start_method: str = "spawn",
        max_restarts: int = 3,
        worker_target: Any = None,
    ) -> None:
        if max_workers < 1 or queue_size < 1 or max_restarts < 0:
            raise ValueError("native worker pool limits must be positive")
        if start_method not in multiprocessing.get_all_start_methods():
            raise ValueError(f"unsupported multiprocessing start method: {start_method}")
        self.max_workers = int(max_workers)
        self.queue_size = int(queue_size)
        self.queue_wait_seconds = float(queue_wait_seconds)
        self.task_timeout_seconds = float(task_timeout_seconds)
        self.start_method = start_method
        self.max_restarts = int(max_restarts)
        self.worker_target = worker_target or _native_worker_main
        self._context = multiprocessing.get_context(start_method)
        self._condition = threading.Condition()
        self._slots: list[_WorkerSlot] = []
        self._restart_count = 0
        self._queue_wait_total = 0.0
        self._closed = False

    @property
    def restart_count(self) -> int:
        return self._restart_count

    @property
    def queue_wait_total_seconds(self) -> float:
        return self._queue_wait_total

    def _start_slot(self, worker_id: int) -> _WorkerSlot:
        input_queue = self._context.Queue(maxsize=1)
        output_queue = self._context.Queue(maxsize=self.queue_size)
        process = self._context.Process(
            target=self.worker_target,
            args=(worker_id, input_queue, output_queue),
            name=f"quote-pdf-native-{worker_id}",
            daemon=True,
        )
        process.start()
        return _WorkerSlot(worker_id, input_queue, output_queue, process)

    def _ensure_started(self) -> None:
        if self._slots:
            return
        for worker_id in range(self.max_workers):
            self._slots.append(self._start_slot(worker_id))

    def _acquire(self) -> _WorkerSlot:
        wait_started = time.monotonic()
        deadline = time.monotonic() + self.queue_wait_seconds
        with self._condition:
            self._ensure_started()
            while True:
                if self._closed:
                    raise NativeWorkerFailure("native_worker_pool_closed", "native worker pool is closed")
                if self._slots and all(slot.disabled for slot in self._slots):
                    raise NativeWorkerFailure("native_worker_restart_exhausted", "native worker restart limit exhausted")
                for slot in self._slots:
                    if not slot.busy and not slot.disabled:
                        slot.busy = True
                        self._queue_wait_total += time.monotonic() - wait_started
                        return slot
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise NativeWorkerFailure("native_worker_queue_timeout", "native worker queue wait exceeded")
                self._condition.wait(timeout=remaining)

    def _release(self, slot: _WorkerSlot) -> None:
        with self._condition:
            slot.busy = False
            self._condition.notify()

    def _replace(self, slot: _WorkerSlot) -> None:
        try:
            if slot.process.is_alive():
                slot.process.terminate()
            slot.process.join(timeout=2.0)
        finally:
            self._restart_count += 1
            if self._restart_count > self.max_restarts:
                slot.disabled = True
                return
            replacement = self._start_slot(slot.worker_id)
            index = self._slots.index(slot)
            self._slots[index] = replacement

    def _run(self, task: Mapping[str, Any], *, timeout_seconds: float) -> tuple[dict[str, Any], ...]:
        slot = self._acquire()
        request_id = str(task["request_id"])
        messages: list[dict[str, Any]] = []
        started = time.monotonic()
        try:
            try:
                slot.input_queue.put(dict(task), timeout=self.queue_wait_seconds)
            except queue.Full as exc:
                raise NativeWorkerFailure("native_worker_queue_timeout", "native worker input queue is full") from exc
            while True:
                remaining = timeout_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    self._replace(slot)
                    messages.append({
                        "kind": "error",
                        "request_id": request_id,
                        "error_type": "TimeoutError",
                        "error": "native worker task exceeded its hard timeout",
                        "timeout": True,
                    })
                    return tuple(messages)
                try:
                    message = slot.output_queue.get(timeout=min(remaining, 0.25))
                except queue.Empty:
                    if not slot.process.is_alive():
                        exitcode = slot.process.exitcode
                        self._replace(slot)
                        messages.append({
                            "kind": "error",
                            "request_id": request_id,
                            "error_type": "WorkerExit",
                            "error": f"native worker exited with code {exitcode}",
                            "exitcode": exitcode,
                        })
                        return tuple(messages)
                    continue
                if message.get("protocol") != NATIVE_WORKER_PROTOCOL or message.get("request_id") != request_id:
                    self._replace(slot)
                    messages.append({
                        "kind": "error",
                        "request_id": request_id,
                        "error_type": "ProtocolError",
                        "error": "native worker returned an invalid message",
                    })
                    return tuple(messages)
                messages.append(message)
                if message.get("kind") in {"result", "render_result", "error"}:
                    return tuple(messages)
        finally:
            self._release(slot)

    def extract(
        self,
        content: bytes,
        engine: str,
        *,
        target_pages: Sequence[int] = (),
        timeout_seconds: float | None = None,
    ) -> Any:
        from .core import NativePage, NativeResult, PdfDiagnostic

        request_id = f"{time.time_ns()}-{threading.get_ident()}"
        try:
            messages = self._run(
            {
                "protocol": NATIVE_WORKER_PROTOCOL,
                "request_id": request_id,
                "operation": "extract",
                "engine": engine,
                "content": bytes(content),
                "pages": tuple(int(page) for page in target_pages),
            },
            timeout_seconds=float(timeout_seconds or self.task_timeout_seconds),
            )
        except NativeWorkerFailure as exc:
            return NativeResult(0, (), (PdfDiagnostic(exc.code, str(exc), severity="error", details=exc.details),), f"isolated-{engine}")
        pages: list[NativePage] = []
        diagnostics: list[PdfDiagnostic] = []
        page_count = 0
        for message in messages:
            if message.get("kind") == "page_count":
                page_count = int(message.get("page_count", 0))
            elif message.get("kind") == "page":
                page = message.get("page")
                if isinstance(page, NativePage):
                    pages.append(page)
            elif message.get("kind") == "result":
                result = message.get("result")
                if isinstance(result, NativeResult):
                    return result
            elif message.get("kind") == "error":
                exitcode = message.get("exitcode")
                code = "native_worker_timeout" if message.get("timeout") else "native_worker_crashed" if exitcode is not None else "native_worker_protocol_error" if message.get("error_type") == "ProtocolError" else "native_extraction_error"
                details = {"worker_exitcode": exitcode} if exitcode is not None else {}
                diagnostics.append(PdfDiagnostic(code, str(message.get("error", "native worker failed")), severity="error", details=details))
        if page_count == 0 and target_pages:
            page_count = max(int(page) for page in target_pages)
        expected_pages = tuple(sorted(set(int(page) for page in target_pages))) if target_pages else tuple(range(1, page_count + 1))
        if diagnostics and expected_pages:
            returned = {page.page_number for page in pages}
            for number in expected_pages:
                if number not in returned:
                    pages.append(NativePage(number, "", 0.0, tuple(diagnostics)))
            pages.sort(key=lambda page: page.page_number)
        return NativeResult(page_count, tuple(pages), tuple(diagnostics), f"isolated-{engine}")

    def render(self, content: bytes, pages: Sequence[int], *, dpi: int, timeout_seconds: float | None = None) -> Mapping[int, bytes]:
        request_id = f"{time.time_ns()}-{threading.get_ident()}"
        try:
            messages = self._run(
                {
                    "protocol": NATIVE_WORKER_PROTOCOL,
                    "request_id": request_id,
                    "operation": "render",
                    "content": bytes(content),
                    "pages": tuple(int(page) for page in pages),
                    "dpi": int(dpi),
                },
                timeout_seconds=float(timeout_seconds or self.task_timeout_seconds),
            )
        except NativeWorkerFailure:
            raise
        for message in messages:
            if message.get("kind") == "render_result":
                return {int(number): bytes(value) for number, value in dict(message.get("images", {})).items()}
        error = next((item for item in messages if item.get("kind") == "error"), None)
        if error:
            exitcode = error.get("exitcode")
            if error.get("timeout"):
                code = "native_worker_timeout"
            elif exitcode is not None:
                code = "native_worker_crashed"
            elif error.get("error_type") == "ProtocolError":
                code = "native_worker_protocol_error"
            else:
                code = "native_render_error"
            raise NativeWorkerFailure(code, str(error.get("error", "native worker render failed")), details={"worker_exitcode": exitcode} if exitcode is not None else {})
        raise NativeWorkerFailure("native_worker_protocol_error", "native worker returned no render result")

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            slots = tuple(self._slots)
            self._slots.clear()
            self._condition.notify_all()
        for slot in slots:
            try:
                if slot.process.is_alive():
                    slot.input_queue.put_nowait(None)
                slot.process.join(timeout=2.0)
                if slot.process.is_alive():
                    slot.process.terminate()
                    slot.process.join(timeout=2.0)
            finally:
                slot.input_queue.close()
                slot.output_queue.close()


_POOL_LOCK = threading.Lock()
_POOLS: dict[tuple[Any, ...], NativeWorkerPool] = {}


def get_shared_native_worker_pool(
    *,
    max_workers: int,
    queue_size: int,
    queue_wait_seconds: float,
    task_timeout_seconds: float,
    start_method: str,
    max_restarts: int,
) -> NativeWorkerPool:
    """Return one process-local pool for an equivalent native configuration."""
    key = (
        int(max_workers),
        int(queue_size),
        float(queue_wait_seconds),
        float(task_timeout_seconds),
        str(start_method),
        int(max_restarts),
    )
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        if pool is None:
            pool = NativeWorkerPool(
                max_workers=key[0],
                queue_size=key[1],
                queue_wait_seconds=key[2],
                task_timeout_seconds=key[3],
                start_method=key[4],
                max_restarts=key[5],
            )
            _POOLS[key] = pool
        return pool


@atexit.register
def _close_shared_native_worker_pools() -> None:
    for pool in tuple(_POOLS.values()):
        pool.close()
