"""Persistent control and progress telemetry for long business-profile backfills."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping

from utils.date_utils import get_shanghai_time


PROGRESS_SCHEMA_VERSION = "business_profile_backfill_progress.v1"
STOP_SCHEMA_VERSION = "business_profile_backfill_stop_request.v1"
ACTIVE_STATES = frozenset({"running", "stop_requested"})
TERMINAL_STATES = frozenset({"completed", "stopped", "blocked", "failed", "interrupted"})
_CONTROL_LOCKS_GUARD = threading.Lock()
_CONTROL_LOCKS: dict[str, threading.RLock] = {}


@dataclass(frozen=True)
class ContinuousBackfillOptions:
    poll_interval_seconds: float = 30.0
    max_idle_cycles: int = 3
    max_cycles: int | None = None
    heartbeat_interval_seconds: float = 30.0
    progress_report_interval_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.poll_interval_seconds < 0:
            raise ValueError("continuous poll_interval_seconds must not be negative")
        if self.max_idle_cycles <= 0:
            raise ValueError("continuous max_idle_cycles must be positive")
        if self.max_cycles is not None and self.max_cycles <= 0:
            raise ValueError("continuous max_cycles must be positive when provided")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("continuous heartbeat_interval_seconds must be positive")
        if self.progress_report_interval_seconds < 0:
            raise ValueError(
                "continuous progress_report_interval_seconds must not be negative"
            )


class BusinessProfileBackfillControlStore:
    """Own atomic progress snapshots and run-targeted cooperative stop requests."""

    def __init__(self, checkpoint_root: str | Path) -> None:
        self.control_root = Path(checkpoint_root) / "control"
        self.progress_path = self.control_root / "backfill_progress.json"
        self.stop_path = self.control_root / "backfill_stop_request.json"
        lock_key = str(self.control_root.resolve())
        with _CONTROL_LOCKS_GUARD:
            self._lock = _CONTROL_LOCKS.setdefault(lock_key, threading.RLock())

    def read_progress(self) -> dict[str, Any]:
        with self._lock:
            payload = self._read_json(self.progress_path)
        if not payload:
            return {
                "schema_version": PROGRESS_SCHEMA_VERSION,
                "state": "not_started",
                "run_id": None,
            }
        if payload.get("schema_version") != PROGRESS_SCHEMA_VERSION:
            raise ValueError("unsupported business-profile backfill progress schema")
        return payload

    def begin(
        self,
        *,
        run_id: str,
        mode: str,
        phase: str | None,
        parameters: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not str(run_id).strip():
            raise ValueError("business-profile backfill run_id is required")
        now = _now_text()
        with self._lock:
            previous = self.read_progress()
            superseded = (
                previous.get("run_id")
                if previous.get("state") in ACTIVE_STATES
                and previous.get("run_id") != run_id
                else None
            )
            payload = {
                "schema_version": PROGRESS_SCHEMA_VERSION,
                "run_id": str(run_id),
                "mode": str(mode),
                "phase": phase,
                "state": "running",
                "started_at": now,
                "heartbeat_at": now,
                "finished_at": None,
                "cycle": 0,
                "idle_cycles": 0,
                "cumulative_workers": {},
                "latest_result": None,
                "queue_health": {},
                "rollout_readiness": {},
                "reason_codes": [],
                "stop_requested_at": None,
                "superseded_run_id": superseded,
                "parameters": dict(parameters),
            }
            self._write_json(self.progress_path, payload)
        return payload

    def update(self, run_id: str, **changes: Any) -> dict[str, Any]:
        """Update only the matching run so an old task cannot overwrite a restart."""

        with self._lock:
            current = self.read_progress()
            if current.get("run_id") != run_id:
                return current
            requested = self.should_stop(run_id)
            next_state = str(changes.get("state") or current.get("state") or "running")
            if requested and next_state == "running":
                changes["state"] = "stop_requested"
                changes.setdefault("stop_requested_at", requested.get("requested_at"))
            current.update(changes)
            current["heartbeat_at"] = _now_text()
            self._write_json(self.progress_path, current)
            return current

    def finish(
        self,
        run_id: str,
        *,
        state: str,
        reason_codes: list[str] | tuple[str, ...] = (),
        latest_result: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if state not in TERMINAL_STATES:
            raise ValueError(f"unsupported business-profile terminal state: {state}")
        changes: dict[str, Any] = {
            "state": state,
            "finished_at": _now_text(),
            "reason_codes": list(
                dict.fromkeys(str(item) for item in reason_codes if item)
            ),
        }
        if latest_result is not None:
            changes["latest_result"] = dict(latest_result)
        return self.update(run_id, **changes)

    def request_stop(self, *, reason: str = "operator_request") -> dict[str, Any]:
        with self._lock:
            progress = self.read_progress()
            run_id = str(progress.get("run_id") or "").strip()
            if not run_id or progress.get("state") not in ACTIVE_STATES:
                return {
                    "status": "not_running",
                    "target_run_id": None,
                    "progress": progress,
                }
            requested_at = _now_text()
            request = {
                "schema_version": STOP_SCHEMA_VERSION,
                "target_run_id": run_id,
                "requested_at": requested_at,
                "reason": str(reason or "operator_request"),
            }
            self._write_json(self.stop_path, request)
            progress.update(
                {
                    "state": "stop_requested",
                    "stop_requested_at": requested_at,
                    "heartbeat_at": requested_at,
                }
            )
            self._write_json(self.progress_path, progress)
            return {
                "status": "stop_requested",
                "target_run_id": run_id,
                "request": request,
                "progress": progress,
            }

    def should_stop(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            request = self._read_json(self.stop_path)
        if not request:
            return None
        if request.get("schema_version") != STOP_SCHEMA_VERSION:
            raise ValueError("unsupported business-profile stop-request schema")
        return request if request.get("target_run_id") == run_id else None

    def status(self) -> dict[str, Any]:
        progress = self.read_progress()
        heartbeat = _parse_datetime(progress.get("heartbeat_at"))
        heartbeat_age = None
        if heartbeat is not None:
            heartbeat_age = max(
                0.0,
                (get_shanghai_time() - heartbeat).total_seconds(),
            )
        return {
            **progress,
            "heartbeat_age_seconds": (
                round(heartbeat_age, 3) if heartbeat_age is not None else None
            ),
            "stop_request": self.should_stop(str(progress.get("run_id") or "")),
        }

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError(f"business-profile control file must contain an object: {path}")
        return dict(payload)

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp"
        )
        try:
            temporary.write_text(
                json.dumps(
                    dict(payload),
                    ensure_ascii=False,
                    sort_keys=True,
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


class ContinuousBusinessProfileBackfillRunner:
    """Repeat bounded durable passes until a governed terminal condition is met."""

    def __init__(
        self,
        store: BusinessProfileBackfillControlStore,
        *,
        options: ContinuousBackfillOptions,
    ) -> None:
        self.store = store
        self.options = options

    async def run(
        self,
        *,
        run_id: str,
        phase: str | None,
        parameters: Mapping[str, Any],
        run_cycle: Callable[[Callable[[], bool]], Awaitable[Mapping[str, Any]]],
        on_progress: Callable[[Mapping[str, Any]], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        progress = self.store.begin(
            run_id=run_id,
            mode="continuous",
            phase=phase,
            parameters=parameters,
        )
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(run_id, heartbeat_stop)
        )
        cumulative: dict[str, dict[str, int]] = {}
        previous_result: dict[str, Any] = {}
        idle_cycles = 0
        cycle = 0
        last_report_at = 0.0

        def should_stop() -> bool:
            return self.store.should_stop(run_id) is not None

        try:
            while True:
                if should_stop():
                    return self.store.finish(
                        run_id,
                        state="stopped",
                        reason_codes=["operator_stop_requested"],
                        latest_result=previous_result,
                    )

                result = dict(await run_cycle(should_stop))
                cycle += 1
                compact = _compact_cycle_result(result)
                cumulative = _merge_worker_counters(cumulative, result.get("workers"))
                material_progress = _material_progress_units(result, previous_result)
                queue_health = dict(result.get("queue_health") or {})
                readiness = dict(result.get("rollout_readiness") or {})
                claimable = int(queue_health.get("claimable") or 0)
                running = int(queue_health.get("running") or 0)
                terminal = int(queue_health.get("terminal") or 0)
                if material_progress == 0 and claimable == 0 and running == 0:
                    idle_cycles += 1
                else:
                    idle_cycles = 0

                requested = should_stop()
                progress = self.store.update(
                    run_id,
                    state="stop_requested" if requested else "running",
                    cycle=cycle,
                    idle_cycles=idle_cycles,
                    cumulative_workers=cumulative,
                    latest_result=compact,
                    queue_health=queue_health,
                    rollout_readiness=readiness,
                    reason_codes=list(readiness.get("phase_reason_codes") or []),
                )
                previous_result = compact
                now = time.monotonic()
                if (
                    on_progress is not None
                    and self.options.progress_report_interval_seconds > 0
                    and (
                        last_report_at == 0.0
                        or now - last_report_at
                        >= self.options.progress_report_interval_seconds
                    )
                ):
                    await on_progress(progress)
                    last_report_at = now

                if requested or str(result.get("status") or "") == "stopped":
                    return self.store.finish(
                        run_id,
                        state="stopped",
                        reason_codes=["operator_stop_requested"],
                        latest_result=compact,
                    )
                result_status = str(result.get("status") or "failed").lower()
                if result_status in {"not_ready", "disabled"}:
                    return self.store.finish(
                        run_id,
                        state="blocked",
                        reason_codes=[
                            str(result.get("reason") or result_status),
                        ],
                        latest_result=compact,
                    )
                if result_status in {"failed", "error"}:
                    return self.store.finish(
                        run_id,
                        state="failed",
                        reason_codes=[
                            str(result.get("reason") or result_status),
                        ],
                        latest_result=compact,
                    )
                if readiness.get("phase_ready") is True:
                    return self.store.finish(
                        run_id,
                        state="completed",
                        reason_codes=["active_phase_ready"],
                        latest_result=compact,
                    )
                if terminal > 0:
                    return self.store.finish(
                        run_id,
                        state="blocked",
                        reason_codes=[
                            "terminal_failures_present",
                            *list(readiness.get("phase_reason_codes") or []),
                        ],
                        latest_result=compact,
                    )
                if idle_cycles >= self.options.max_idle_cycles:
                    return self.store.finish(
                        run_id,
                        state="blocked",
                        reason_codes=[
                            "no_progress_limit_reached",
                            *list(readiness.get("phase_reason_codes") or []),
                        ],
                        latest_result=compact,
                    )
                if (
                    self.options.max_cycles is not None
                    and cycle >= self.options.max_cycles
                ):
                    return self.store.finish(
                        run_id,
                        state="stopped",
                        reason_codes=["cycle_limit_reached"],
                        latest_result=compact,
                    )
                if await self._wait_for_stop(run_id):
                    return self.store.finish(
                        run_id,
                        state="stopped",
                        reason_codes=["operator_stop_requested"],
                        latest_result=compact,
                    )
        except asyncio.CancelledError:
            self.store.finish(
                run_id,
                state="interrupted",
                reason_codes=["task_cancelled"],
                latest_result=previous_result,
            )
            raise
        except Exception as exc:
            self.store.finish(
                run_id,
                state="failed",
                reason_codes=[f"{type(exc).__name__}: {exc}"],
                latest_result=previous_result,
            )
            raise
        finally:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

    async def _heartbeat_loop(self, run_id: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    stop.wait(),
                    timeout=self.options.heartbeat_interval_seconds,
                )
            except asyncio.TimeoutError:
                self.store.update(run_id)

    async def _wait_for_stop(self, run_id: str) -> bool:
        deadline = time.monotonic() + self.options.poll_interval_seconds
        while time.monotonic() < deadline:
            if self.store.should_stop(run_id) is not None:
                return True
            await asyncio.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        return self.store.should_stop(run_id) is not None


def _merge_worker_counters(
    cumulative: Mapping[str, Mapping[str, int]],
    workers: Any,
) -> dict[str, dict[str, int]]:
    output = {stage: dict(values) for stage, values in cumulative.items()}
    for stage, raw in dict(workers or {}).items():
        target = output.setdefault(str(stage), {})
        for key in (
            "claimed",
            "completed",
            "retried",
            "terminal_failures",
            "lease_conflicts",
        ):
            target[key] = int(target.get(key) or 0) + int(dict(raw or {}).get(key) or 0)
    return output


def _material_progress_units(
    current: Mapping[str, Any],
    previous: Mapping[str, Any],
) -> int:
    discovery = dict(current.get("discovery") or {})
    enqueue = dict(current.get("enqueue") or {})
    workers = dict(current.get("workers") or {})
    units = sum(
        int(discovery.get(key) or 0)
        for key in ("frontier_inserted", "frontier_changed")
    )
    units += sum(
        int(enqueue.get(key) or 0)
        for key in ("inserted", "reset", "superseded")
    )
    units += sum(
        int(dict(stage or {}).get(key) or 0)
        for stage in workers.values()
        for key in ("completed", "retried", "terminal_failures")
    )
    previous_discovery = dict(previous.get("discovery") or {})
    previous_backlog = previous_discovery.get("discovery_window_backlog")
    current_backlog = discovery.get("discovery_window_backlog")
    if previous_backlog is not None and current_backlog is not None:
        units += max(0, int(previous_backlog) - int(current_backlog))
    return units


def _compact_cycle_result(result: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "schema_version",
        "status",
        "operation",
        "selection_policy",
        "knowledge_cutoff",
        "discovery",
        "enqueue",
        "workers",
        "throughput",
        "queue_health",
        "reconciliation",
        "rollout",
        "rollout_readiness",
        "writer",
    )
    return {key: result.get(key) for key in keys if key in result}


def _now_text() -> str:
    return get_shanghai_time().isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=get_shanghai_time().tzinfo)
    return parsed
