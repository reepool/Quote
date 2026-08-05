"""Durable asynchronous orchestration for business-profile production."""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

from utils.date_utils import get_shanghai_time


WORK_SCHEMA_VERSION = "business_profile_work_item.v1"
ASYNC_REPORT_SCHEMA_VERSION = "business_profile_async_production_report.v1"
WORK_STAGES = ("acquire", "parse", "semantic", "publish")
CLAIMABLE_STATUSES = ("pending", "retry_due")
TERMINAL_STATUSES = ("completed", "superseded", "terminal_failure")
AUTOMATIC_DOCUMENT_TYPES = ("annual_report", "annual_report_correction")
_WRITE_COORDINATOR_CREATION_LOCK = threading.Lock()


@dataclass(frozen=True)
class StageBudget:
    max_items: int = 10
    max_concurrency: int = 2
    max_elapsed_seconds: float = 300.0
    high_water_mark: int = 1000

    def __post_init__(self) -> None:
        if (
            min(
                self.max_items,
                self.max_concurrency,
                self.max_elapsed_seconds,
                self.high_water_mark,
            )
            <= 0
        ):
            raise ValueError("business-profile stage budgets must be positive")


class BusinessProfileWriteCoordinator:
    """Serialize short SQLite transactions without serializing worker computation."""

    def __init__(self, *, inter_write_seconds: float = 0.01) -> None:
        if inter_write_seconds < 0:
            raise ValueError(
                "business-profile inter_write_seconds must not be negative"
            )
        self.inter_write_seconds = float(inter_write_seconds)
        self._writer_lock = threading.RLock()
        self._metrics_lock = threading.Lock()
        self._local = threading.local()
        self._pending_writers = 0
        self._active_writers = 0
        self._max_pending_writers = 0
        self._max_active_writers = 0
        self._write_transactions = 0
        self._wait_seconds = 0.0
        self._write_seconds = 0.0
        self._last_release_monotonic = 0.0

    @contextmanager
    def write_scope(self):
        """Hold the single-writer gate for one transaction, with reentrant safety."""

        depth = int(getattr(self._local, "depth", 0))
        if depth:
            self._local.depth = depth + 1
            try:
                yield
            finally:
                self._local.depth -= 1
            return

        wait_started = time.monotonic()
        with self._metrics_lock:
            self._pending_writers += 1
            self._max_pending_writers = max(
                self._max_pending_writers,
                self._pending_writers,
            )
        self._writer_lock.acquire()
        acquired_at = time.monotonic()
        with self._metrics_lock:
            self._pending_writers -= 1
            self._active_writers += 1
            self._max_active_writers = max(
                self._max_active_writers,
                self._active_writers,
            )
            last_release = self._last_release_monotonic
        remaining_yield = self.inter_write_seconds - max(
            0.0, acquired_at - last_release
        )
        if last_release and remaining_yield > 0:
            time.sleep(remaining_yield)
        write_started = time.monotonic()
        self._local.depth = 1
        try:
            yield
        finally:
            finished_at = time.monotonic()
            self._local.depth = 0
            with self._metrics_lock:
                self._active_writers -= 1
                self._write_transactions += 1
                self._wait_seconds += max(0.0, acquired_at - wait_started)
                self._write_seconds += max(0.0, finished_at - write_started)
                self._last_release_monotonic = finished_at
            self._writer_lock.release()

    async def run(self, func: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Run one short synchronous write unit outside the event-loop thread."""

        def invoke() -> Any:
            with self.write_scope():
                return func(*args, **kwargs)

        return await asyncio.to_thread(invoke)

    def snapshot(self) -> dict[str, Any]:
        with self._metrics_lock:
            return {
                "pending_writers": self._pending_writers,
                "active_writers": self._active_writers,
                "max_pending_writers": self._max_pending_writers,
                "max_active_writers": self._max_active_writers,
                "write_transactions": self._write_transactions,
                "wait_seconds": round(self._wait_seconds, 6),
                "write_seconds": round(self._write_seconds, 6),
                "inter_write_seconds": self.inter_write_seconds,
            }


def get_business_profile_write_coordinator(
    storage: Any,
    *,
    inter_write_seconds: float = 0.01,
) -> BusinessProfileWriteCoordinator:
    """Return the process-local single writer shared by one storage manager."""

    coordinator = getattr(storage, "_business_profile_write_coordinator", None)
    if coordinator is not None:
        return coordinator
    with _WRITE_COORDINATOR_CREATION_LOCK:
        coordinator = getattr(storage, "_business_profile_write_coordinator", None)
        if coordinator is None:
            coordinator = BusinessProfileWriteCoordinator(
                inter_write_seconds=inter_write_seconds
            )
            setattr(storage, "_business_profile_write_coordinator", coordinator)
    return coordinator


class BusinessProfileWorkRepository:
    """Own idempotent work identities, leases, retries, and queue diagnostics."""

    def __init__(self, storage: Any, *, checkpoint_root: str | Path):
        self.storage = storage
        self.checkpoint_root = Path(checkpoint_root)

    def enqueue_latest_annual(
        self,
        *,
        knowledge_cutoff: str,
        processing_identity: Mapping[str, Any],
        instrument_ids: Sequence[str] = (),
        start_date: str | None = None,
        end_date: str | None = None,
        max_attempts: int = 3,
        force: bool = False,
    ) -> dict[str, int]:
        cutoff = _date_text(knowledge_cutoff, "knowledge_cutoff")
        normalized_start = _date_text(start_date, "start_date") if start_date else None
        normalized_end = _date_text(end_date, "end_date") if end_date else None
        if normalized_start and normalized_end and normalized_start > normalized_end:
            raise ValueError(
                "business-profile backfill start_date must not exceed end_date"
            )
        rows = self._frontier_rows(
            knowledge_cutoff=cutoff,
            start_date=normalized_start,
            end_date=normalized_end,
            instrument_ids=instrument_ids,
            document_types=AUTOMATIC_DOCUMENT_TYPES,
        )
        latest: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            instrument_id = str(row["instrument_id"])
            if instrument_id not in latest or _frontier_sort_key(
                row
            ) > _frontier_sort_key(latest[instrument_id]):
                latest[instrument_id] = row
        return self._enqueue_rows(
            tuple(latest.values()),
            policy="latest_annual_only",
            processing_identity=processing_identity,
            max_attempts=max_attempts,
            force=force,
            supersede_older=True,
        )

    def enqueue_scoped(
        self,
        *,
        knowledge_cutoff: str,
        processing_identity: Mapping[str, Any],
        instrument_ids: Sequence[str] = (),
        start_date: str | None = None,
        end_date: str | None = None,
        document_types: Sequence[str] = (),
        max_attempts: int = 3,
        force: bool = False,
    ) -> dict[str, int]:
        cutoff = _date_text(knowledge_cutoff, "knowledge_cutoff")
        normalized_instruments = tuple(
            sorted({str(item).strip() for item in instrument_ids if str(item).strip()})
        )
        normalized_types = tuple(
            sorted({str(item).strip() for item in document_types if str(item).strip()})
        )
        if not normalized_instruments and not start_date:
            raise ValueError(
                "business-profile backfill requires instruments or a bounded start date"
            )
        normalized_start = _date_text(start_date, "start_date") if start_date else None
        normalized_end = _date_text(end_date, "end_date") if end_date else None
        if normalized_start and normalized_end and normalized_start > normalized_end:
            raise ValueError(
                "business-profile backfill start_date must not exceed end_date"
            )
        rows = self._frontier_rows(
            knowledge_cutoff=cutoff,
            start_date=normalized_start,
            end_date=normalized_end,
            instrument_ids=normalized_instruments,
            document_types=normalized_types,
        )
        return self._enqueue_rows(
            rows,
            policy="expanded",
            processing_identity=processing_identity,
            max_attempts=max_attempts,
            force=force,
            supersede_older=False,
        )

    def claim(
        self,
        stage: str,
        *,
        limit: int,
        lease_owner: str,
        lease_seconds: int,
    ) -> tuple[dict[str, Any], ...]:
        normalized_stage = _stage(stage)
        now = get_shanghai_time()
        now_text = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=max(1, lease_seconds))).isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM business_profile_work_items
                WHERE stage = ?
                  AND (
                    (status IN ('pending', 'retry_due')
                     AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                    OR (status = 'running' AND lease_expires_at <= ?)
                  )
                ORDER BY report_period DESC, created_at, work_id
                LIMIT ?
                """,
                (normalized_stage, now_text, now_text, max(1, int(limit))),
            ).fetchall()
            work_ids = [str(row["work_id"]) for row in rows]
            if work_ids:
                placeholders = ",".join("?" for _ in work_ids)
                conn.execute(
                    "UPDATE business_profile_work_items "
                    "SET status = 'running', attempt_count = attempt_count + 1, "
                    "lease_owner = ?, lease_expires_at = ?, updated_at = ? "
                    f"WHERE work_id IN ({placeholders})",
                    (lease_owner, lease_expires_at, now_text, *work_ids),
                )
            conn.commit()
        return tuple(self.get(item) for item in work_ids)

    def acknowledge(
        self,
        work_id: str,
        *,
        lease_owner: str,
        result: Mapping[str, Any],
    ) -> None:
        item = self.get(work_id)
        stage = _stage(item["stage"])
        now = get_shanghai_time().isoformat()
        stage_index = WORK_STAGES.index(stage)
        completed = stage_index == len(WORK_STAGES) - 1
        next_stage = stage if completed else WORK_STAGES[stage_index + 1]
        metadata = {
            **dict(item.get("metadata") or {}),
            "stage_results": {
                **dict((item.get("metadata") or {}).get("stage_results") or {}),
                stage: dict(result),
            },
        }
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                """
                UPDATE business_profile_work_items
                SET stage = ?, status = ?, next_attempt_at = NULL,
                    attempt_count = 0,
                    lease_owner = NULL, lease_expires_at = NULL, last_error = NULL,
                    metadata_json = ?, completed_at = ?, updated_at = ?
                WHERE work_id = ? AND status = 'running' AND lease_owner = ?
                """,
                (
                    next_stage,
                    "completed" if completed else "pending",
                    _canonical_json(metadata),
                    now if completed else None,
                    now,
                    work_id,
                    str(lease_owner),
                ),
            )
            conn.commit()
        if int(cursor.rowcount or 0) != 1:
            raise RuntimeError(
                f"business-profile work acknowledgement conflict: {work_id}"
            )

    def fail(
        self,
        work_id: str,
        *,
        lease_owner: str,
        error: str,
        retryable: bool = True,
        initial_backoff_seconds: int = 300,
    ) -> str:
        item = self.get(work_id)
        attempts = int(item["attempt_count"])
        terminal = not retryable or attempts >= int(item["max_attempts"])
        now = get_shanghai_time()
        next_attempt = (
            None
            if terminal
            else (
                now
                + timedelta(
                    seconds=max(1, initial_backoff_seconds)
                    * (2 ** max(attempts - 1, 0))
                )
            ).isoformat()
        )
        status = "terminal_failure" if terminal else "retry_due"
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                """
                UPDATE business_profile_work_items
                SET status = ?, next_attempt_at = ?, lease_owner = NULL,
                    lease_expires_at = NULL, last_error = ?, updated_at = ?
                WHERE work_id = ? AND status = 'running' AND lease_owner = ?
                """,
                (
                    status,
                    next_attempt,
                    str(error)[:4000],
                    now.isoformat(),
                    work_id,
                    str(lease_owner),
                ),
            )
            conn.commit()
        if int(cursor.rowcount or 0) != 1:
            return "lease_lost"
        return status

    def get(self, work_id: str) -> dict[str, Any]:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT * FROM business_profile_work_items WHERE work_id = ?",
                (str(work_id),),
            ).fetchone()
        if row is None:
            raise KeyError(f"business-profile work item not found: {work_id}")
        return _decode_work_row(row)

    def health(self) -> dict[str, Any]:
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                """
                SELECT stage, status, COUNT(*) AS row_count,
                       MIN(created_at) AS oldest_created_at
                FROM business_profile_work_items
                GROUP BY stage, status ORDER BY stage, status
                """
            ).fetchall()
        now_value = get_shanghai_time()
        groups = []
        for row in rows:
            item = dict(row)
            oldest = str(item.get("oldest_created_at") or "")
            try:
                item["oldest_age_seconds"] = max(
                    0.0,
                    (now_value - datetime.fromisoformat(oldest)).total_seconds(),
                )
            except (TypeError, ValueError):
                item["oldest_age_seconds"] = None
            groups.append(item)
        return {
            "as_of": now,
            "total": sum(int(item["row_count"]) for item in groups),
            "running": sum(
                int(item["row_count"])
                for item in groups
                if item["status"] == "running"
            ),
            "claimable": sum(
                int(item["row_count"])
                for item in groups
                if item["status"] in CLAIMABLE_STATUSES
            ),
            "terminal": sum(
                int(item["row_count"])
                for item in groups
                if item["status"] == "terminal_failure"
            ),
            "completed": sum(
                int(item["row_count"])
                for item in groups
                if item["status"] == "completed"
            ),
            "finalized": sum(
                int(item["row_count"])
                for item in groups
                if item["status"] in TERMINAL_STATUSES
            ),
            "groups": groups,
        }

    def claimable_count(self, stage: str) -> int:
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM business_profile_work_items "
                    "WHERE stage = ? AND status IN ('pending', 'retry_due')",
                    (_stage(stage),),
                ).fetchone()[0]
            )

    def _frontier_rows(
        self,
        *,
        knowledge_cutoff: str,
        start_date: str | None = None,
        end_date: str | None = None,
        instrument_ids: Sequence[str] = (),
        document_types: Sequence[str] = (),
    ) -> tuple[Mapping[str, Any], ...]:
        clauses = [
            "status <> 'superseded'",
            "published_at IS NOT NULL",
            "report_period IS NOT NULL",
            "substr(published_at, 1, 10) <= ?",
        ]
        params: list[Any] = [knowledge_cutoff]
        if start_date:
            clauses.append("substr(published_at, 1, 10) >= ?")
            params.append(_date_text(start_date, "start_date"))
        if end_date:
            clauses.append("substr(published_at, 1, 10) <= ?")
            params.append(_date_text(end_date, "end_date"))
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            clauses.append(f"instrument_id IN ({placeholders})")
            params.extend(instrument_ids)
        if document_types:
            placeholders = ",".join("?" for _ in document_types)
            clauses.append(f"document_type IN ({placeholders})")
            params.extend(document_types)
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_announcement_frontier WHERE "
                + " AND ".join(clauses),
                tuple(params),
            ).fetchall()
        return tuple(dict(row) for row in rows)

    def _enqueue_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        policy: str,
        processing_identity: Mapping[str, Any],
        max_attempts: int,
        force: bool = False,
        supersede_older: bool,
    ) -> dict[str, int]:
        identity_hash = _stable_hash(processing_identity)
        now = get_shanghai_time().isoformat()
        inserted = reused = superseded = reset = 0
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            for row in rows:
                work_id = (
                    "bp-work-"
                    + _stable_hash(
                        {
                            "frontier_id": row["frontier_id"],
                            "policy": policy,
                            "processing_identity_hash": identity_hash,
                        }
                    )[:24]
                )
                checkpoint = self.checkpoint_root / f"{work_id}.json"
                existing = conn.execute(
                    "SELECT status FROM business_profile_work_items WHERE work_id = ?",
                    (work_id,),
                ).fetchone()
                if existing is not None:
                    existing_status = str(existing["status"])
                    if force and existing_status in TERMINAL_STATUSES:
                        conn.execute(
                            "UPDATE business_profile_work_items SET stage = 'acquire', "
                            "status = 'pending', attempt_count = 0, next_attempt_at = NULL, "
                            "lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, "
                            "completed_at = NULL, updated_at = ? WHERE work_id = ?",
                            (now, work_id),
                        )
                        reset += 1
                    else:
                        reused += 1
                else:
                    metadata = {
                        "schema_version": WORK_SCHEMA_VERSION,
                        "title": row.get("title"),
                        "published_at": row.get("published_at"),
                        "processing_identity": dict(processing_identity),
                    }
                    conn.execute(
                        """
                        INSERT INTO business_profile_work_items (
                            work_id, frontier_id, instrument_id, source,
                            announcement_id, report_period, document_type, policy,
                            processing_identity_hash, stage, status, attempt_count,
                            max_attempts, next_attempt_at, lease_owner, lease_expires_at,
                            checkpoint_path, last_error, metadata_json, completed_at,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'acquire', 'pending', 0,
                                  ?, NULL, NULL, NULL, ?, NULL, ?, NULL, ?, ?)
                        """,
                        (
                            work_id,
                            row["frontier_id"],
                            row["instrument_id"],
                            row["source"],
                            row["announcement_id"],
                            row["report_period"],
                            row["document_type"],
                            policy,
                            identity_hash,
                            max(1, int(max_attempts)),
                            str(checkpoint),
                            _canonical_json(metadata),
                            now,
                            now,
                        ),
                    )
                    inserted += 1
                if supersede_older:
                    candidates = conn.execute(
                        """
                        SELECT work.work_id, frontier.report_period,
                               frontier.published_at, frontier.document_type,
                               frontier.frontier_id
                        FROM business_profile_work_items AS work
                        JOIN business_profile_announcement_frontier AS frontier
                          ON frontier.frontier_id = work.frontier_id
                        WHERE work.instrument_id = ? AND work.policy = ?
                          AND work.work_id <> ?
                          AND work.status IN ('pending', 'retry_due')
                        """,
                        (row["instrument_id"], policy, work_id),
                    ).fetchall()
                    obsolete_ids = [
                        str(item["work_id"])
                        for item in candidates
                        if _frontier_sort_key(dict(item)) < _frontier_sort_key(row)
                    ]
                    if obsolete_ids:
                        placeholders = ",".join("?" for _ in obsolete_ids)
                        cursor = conn.execute(
                            "UPDATE business_profile_work_items "
                            "SET status = 'superseded', updated_at = ? "
                            f"WHERE work_id IN ({placeholders})",
                            (now, *obsolete_ids),
                        )
                        superseded += int(cursor.rowcount or 0)
            conn.commit()
        return {
            "eligible": len(rows),
            "inserted": inserted,
            "reused": reused,
            "reset": reset,
            "superseded": superseded,
        }


class BusinessProfileAsyncProductionService:
    """Run discovery first, then independently bounded asynchronous workers."""

    def __init__(
        self,
        *,
        repository: BusinessProfileWorkRepository,
        discovery_runner: Callable[..., Awaitable[Mapping[str, Any]]],
        stage_runner: Callable[[str, Mapping[str, Any]], Awaitable[Mapping[str, Any]]],
        lease_seconds: int = 900,
        retry_backoff_seconds: int = 300,
        write_coordinator: BusinessProfileWriteCoordinator | None = None,
    ) -> None:
        self.repository = repository
        self.discovery_runner = discovery_runner
        self.stage_runner = stage_runner
        self.lease_seconds = max(1, int(lease_seconds))
        self.retry_backoff_seconds = max(1, int(retry_backoff_seconds))
        self.write_coordinator = (
            write_coordinator
            or get_business_profile_write_coordinator(repository.storage)
        )

    async def run_daily(
        self,
        *,
        knowledge_cutoff: str,
        processing_identity: Mapping[str, Any],
        discovery_kwargs: Mapping[str, Any],
        stage_budgets: Mapping[str, StageBudget],
        max_attempts: int = 3,
    ) -> dict[str, Any]:
        started = time.monotonic()
        try:
            discovery = dict(await self.discovery_runner(**dict(discovery_kwargs)))
        except Exception as exc:
            discovery = {
                "status": "failed",
                "errors": [f"{type(exc).__name__}: {exc}"],
            }
        enqueue = await self.write_coordinator.run(
            self.repository.enqueue_latest_annual,
            knowledge_cutoff=knowledge_cutoff,
            processing_identity=processing_identity,
            max_attempts=max_attempts,
        )
        workers = await self._run_workers(stage_budgets)
        health = await asyncio.to_thread(self.repository.health)
        discovery_status = str(discovery.get("status") or "failed").lower()
        return {
            "schema_version": ASYNC_REPORT_SCHEMA_VERSION,
            "status": (
                "success"
                if discovery_status in {"success", "unchanged"}
                else "degraded"
            ),
            "operation": "business_profile_daily_incremental",
            "knowledge_cutoff": knowledge_cutoff,
            "discovery": discovery,
            "enqueue": enqueue,
            "workers": workers,
            "queue_health": health,
            "writer": self.write_coordinator.snapshot(),
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }

    async def run_backfill(
        self,
        *,
        knowledge_cutoff: str,
        processing_identity: Mapping[str, Any],
        instrument_ids: Sequence[str] = (),
        start_date: str | None = None,
        end_date: str | None = None,
        document_types: Sequence[str] = (),
        discovery_kwargs: Mapping[str, Any] | None = None,
        stage_budgets: Mapping[str, StageBudget] | None = None,
        max_attempts: int = 3,
        force: bool = False,
        selection_policy: str = "expanded",
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        if not instrument_ids and not start_date:
            raise ValueError(
                "business-profile backfill requires instruments or a bounded start date"
            )
        policy = str(selection_policy or "expanded").strip()
        if policy not in {"latest_annual_only", "expanded"}:
            raise ValueError(f"unsupported business-profile backfill policy: {policy}")
        if policy == "latest_annual_only":
            unsupported_types = sorted(
                set(str(item) for item in document_types) - set(AUTOMATIC_DOCUMENT_TYPES)
            )
            if unsupported_types:
                raise ValueError(
                    "latest-annual backfill does not accept specialist document types: "
                    + ",".join(unsupported_types)
                )
        discovery = None
        if discovery_kwargs is not None:
            try:
                discovery = dict(await self.discovery_runner(**dict(discovery_kwargs)))
            except Exception as exc:
                discovery = {
                    "status": "failed",
                    "errors": [f"{type(exc).__name__}: {exc}"],
                }
        if policy == "latest_annual_only":
            enqueue = await self.write_coordinator.run(
                self.repository.enqueue_latest_annual,
                knowledge_cutoff=knowledge_cutoff,
                processing_identity=processing_identity,
                instrument_ids=instrument_ids,
                start_date=start_date,
                end_date=end_date,
                max_attempts=max_attempts,
                force=force,
            )
        elif policy == "expanded":
            enqueue = await self.write_coordinator.run(
                self.repository.enqueue_scoped,
                knowledge_cutoff=knowledge_cutoff,
                processing_identity=processing_identity,
                instrument_ids=instrument_ids,
                start_date=start_date,
                end_date=end_date,
                document_types=document_types,
                max_attempts=max_attempts,
                force=force,
            )
        workers = await self._run_workers(
            stage_budgets or {},
            should_stop=should_stop,
        )
        stopped = any(
            str(item.get("status") or "").lower() == "stopped"
            for item in workers.values()
        )
        return {
            "schema_version": ASYNC_REPORT_SCHEMA_VERSION,
            "status": (
                "stopped"
                if stopped
                else "degraded"
                if discovery is not None
                and str(discovery.get("status") or "failed").lower()
                not in {"success", "unchanged"}
                else "success"
            ),
            "operation": "business_profile_backfill",
            "selection_policy": policy,
            "knowledge_cutoff": knowledge_cutoff,
            "discovery": discovery,
            "enqueue": enqueue,
            "workers": workers,
            "queue_health": await asyncio.to_thread(self.repository.health),
            "writer": self.write_coordinator.snapshot(),
        }

    async def _run_workers(
        self,
        stage_budgets: Mapping[str, StageBudget],
        *,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        semantic_depth = await asyncio.to_thread(
            self.repository.claimable_count,
            "semantic",
        )
        semantic_limit = stage_budgets.get("semantic")
        acquire_backpressured = bool(
            semantic_limit and semantic_depth >= semantic_limit.high_water_mark
        )
        stage_done = {stage: asyncio.Event() for stage in WORK_STAGES}

        async def run_stage(stage: str) -> tuple[str, dict[str, Any]]:
            budget = stage_budgets.get(stage)
            try:
                if budget is None:
                    return stage, {
                        "status": "deferred",
                        "reason": "no_stage_budget",
                    }
                if stage == "acquire" and acquire_backpressured:
                    return stage, {
                        "status": "backpressured",
                        "reason": "semantic_high_water_mark",
                        "semantic_depth": semantic_depth,
                    }
                stage_index = WORK_STAGES.index(stage)
                upstream_done = (
                    None
                    if stage_index == 0
                    else stage_done[WORK_STAGES[stage_index - 1]]
                )
                return stage, await self._drain_stage(
                    stage,
                    budget,
                    upstream_done=upstream_done,
                    should_stop=should_stop,
                )
            finally:
                stage_done[stage].set()

        results = await asyncio.gather(*(run_stage(stage) for stage in WORK_STAGES))
        return dict(results)

    async def _drain_stage(
        self,
        stage: str,
        budget: StageBudget,
        *,
        upstream_done: asyncio.Event | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        claimed = completed = retried = failed = lease_conflicts = 0
        errors: list[dict[str, str]] = []
        stopped = False
        lease_owner = f"async-{stage}-{get_shanghai_time().strftime('%Y%m%d%H%M%S%f')}"
        while claimed < budget.max_items:
            if should_stop is not None and should_stop():
                stopped = True
                break
            if time.monotonic() - started >= budget.max_elapsed_seconds:
                break
            batch_limit = min(
                budget.max_concurrency,
                budget.max_items - claimed,
            )
            items = await self.write_coordinator.run(
                self.repository.claim,
                stage,
                limit=batch_limit,
                lease_owner=lease_owner,
                lease_seconds=self.lease_seconds,
            )
            if not items:
                if upstream_done is None or upstream_done.is_set():
                    break
                await asyncio.sleep(0.05)
                continue
            claimed += len(items)

            async def run_one(item: Mapping[str, Any]) -> None:
                nonlocal completed, retried, failed, lease_conflicts
                try:
                    result = dict(await self.stage_runner(stage, item))
                    status = str(result.get("status") or "").lower()
                    if status not in {"success", "completed", "unchanged"}:
                        raise RuntimeError(
                            str(result.get("reason") or status or "stage_failed")
                        )
                    await self.write_coordinator.run(
                        self.repository.acknowledge,
                        str(item["work_id"]),
                        lease_owner=lease_owner,
                        result=result,
                    )
                    completed += 1
                except Exception as exc:
                    terminal_status = await self.write_coordinator.run(
                        self.repository.fail,
                        str(item["work_id"]),
                        lease_owner=lease_owner,
                        error=f"{type(exc).__name__}: {exc}",
                        retryable=_retryable(exc),
                        initial_backoff_seconds=self.retry_backoff_seconds,
                    )
                    if terminal_status == "retry_due":
                        retried += 1
                    elif terminal_status == "lease_lost":
                        lease_conflicts += 1
                    else:
                        failed += 1
                    errors.append(
                        {
                            "work_id": str(item["work_id"]),
                            "error": f"{type(exc).__name__}: {exc}"[:1000],
                        }
                    )

            await asyncio.gather(*(run_one(item) for item in items))
            if should_stop is not None and should_stop():
                stopped = True
                break
        return {
            "status": "stopped" if stopped else "success",
            "stop_requested": stopped,
            "claimed": claimed,
            "completed": completed,
            "retried": retried,
            "terminal_failures": failed,
            "lease_conflicts": lease_conflicts,
            "errors": errors[:20],
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "writer": self.write_coordinator.snapshot(),
        }


def parse_stage_budgets(value: Mapping[str, Any] | None) -> dict[str, StageBudget]:
    output = {}
    for stage, raw in dict(value or {}).items():
        normalized = _stage(stage)
        payload = dict(raw or {})
        output[normalized] = StageBudget(
            max_items=int(payload.get("max_items", 10)),
            max_concurrency=int(payload.get("max_concurrency", 2)),
            max_elapsed_seconds=float(payload.get("max_elapsed_seconds", 300.0)),
            high_water_mark=int(payload.get("high_water_mark", 1000)),
        )
    return output


def _decode_work_row(row: Mapping[str, Any]) -> dict[str, Any]:
    item = dict(row)
    try:
        metadata = json.loads(item.pop("metadata_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        metadata = {}
    item["metadata"] = metadata if isinstance(metadata, Mapping) else {}
    return item


def _frontier_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str, str]:
    document_type = str(row.get("document_type") or "")
    return (
        str(row.get("report_period") or ""),
        int(document_type.endswith("_correction")),
        str(row.get("published_at") or ""),
        str(row.get("frontier_id") or ""),
    )


def _stage(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in WORK_STAGES:
        raise ValueError(f"unsupported business-profile work stage: {value}")
    return normalized


def _date_text(value: Any, field_name: str) -> str:
    text = str(value or "")[:10]
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {value}") from exc


def _retryable(exc: Exception) -> bool:
    return isinstance(exc, (OSError, RuntimeError, TimeoutError, asyncio.TimeoutError))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
