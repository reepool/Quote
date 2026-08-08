"""Durable asynchronous orchestration for business-profile production."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
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
logger = logging.getLogger(__name__)


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
            knowledge_cutoff=cutoff,
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
            knowledge_cutoff=cutoff,
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
        processing_identity_hash: str | None = None,
        exclude_work_ids: Sequence[str] = (),
    ) -> tuple[dict[str, Any], ...]:
        normalized_stage = _stage(stage)
        now = get_shanghai_time()
        now_text = now.isoformat()
        lease_expires_at = (now + timedelta(seconds=max(1, lease_seconds))).isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            identity_clause = (
                "AND processing_identity_hash = ?"
                if processing_identity_hash
                else ""
            )
            excluded = tuple(
                sorted({str(value) for value in exclude_work_ids if str(value)})
            )
            exclusion_clause = (
                "AND work_id NOT IN ("
                + ",".join("?" for _ in excluded)
                + ")"
                if excluded
                else ""
            )
            params: list[Any] = [normalized_stage, now_text, now_text]
            if processing_identity_hash:
                params.append(str(processing_identity_hash))
            params.extend(excluded)
            params.append(max(1, int(limit)))
            rows = conn.execute(
                f"""
                SELECT * FROM business_profile_work_items
                WHERE stage = ?
                  AND (
                    (status IN ('pending', 'retry_due')
                     AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                    OR (status = 'running' AND lease_expires_at <= ?)
                  )
                  {identity_clause}
                  {exclusion_clause}
                ORDER BY report_period DESC, updated_at, created_at, work_id
                LIMIT ?
                """,
                tuple(params),
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

    def defer_configuration(
        self,
        work_id: str,
        *,
        lease_owner: str,
        reason: str,
        retry_after_seconds: int = 300,
    ) -> str:
        """Release a configuration-blocked lease without consuming an attempt."""

        now = get_shanghai_time()
        next_attempt_at = (
            now + timedelta(seconds=max(1, int(retry_after_seconds)))
        ).isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                """
                UPDATE business_profile_work_items
                SET status = 'retry_due',
                    attempt_count = CASE
                        WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0
                    END,
                    next_attempt_at = ?, lease_owner = NULL,
                    lease_expires_at = NULL, last_error = ?, updated_at = ?
                WHERE work_id = ? AND status = 'running' AND lease_owner = ?
                """,
                (
                    next_attempt_at,
                    str(reason)[:4000],
                    now.isoformat(),
                    work_id,
                    str(lease_owner),
                ),
            )
            conn.commit()
        return "configuration_blocked" if int(cursor.rowcount or 0) == 1 else "lease_lost"

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

    def get_bound_frontier(
        self,
        work: str | Mapping[str, Any],
    ) -> dict[str, Any]:
        """Load the active frontier row whose identity was frozen into the work item."""

        item = self.get(work) if isinstance(work, str) else dict(work)
        frontier_id = str(item.get("frontier_id") or "").strip()
        if not frontier_id:
            raise ValueError("business-profile work item has no frontier_id")
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            row = conn.execute(
                "SELECT * FROM business_profile_announcement_frontier "
                "WHERE frontier_id = ?",
                (frontier_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError(
                f"business-profile bound frontier is missing: {frontier_id}"
            )
        frontier = dict(row)
        if str(frontier.get("status") or "") == "superseded":
            raise RuntimeError(
                f"business-profile bound frontier is superseded: {frontier_id}"
            )
        identity_fields = (
            "frontier_id",
            "instrument_id",
            "source",
            "announcement_id",
            "report_period",
            "document_type",
        )
        mismatches = [
            field
            for field in identity_fields
            if str(item.get(field) or "") != str(frontier.get(field) or "")
        ]
        if mismatches:
            raise RuntimeError(
                "business-profile work/frontier identity mismatch: "
                + ",".join(mismatches)
            )
        if not str(frontier.get("source_url") or "").strip():
            raise RuntimeError(
                f"business-profile bound frontier has no source URL: {frontier_id}"
            )
        try:
            metadata = json.loads(frontier.pop("metadata_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        frontier["metadata"] = metadata if isinstance(metadata, Mapping) else {}
        return frontier

    def get_usable_bound_manifest(
        self,
        work: str | Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Return an integrity-verified annual-report asset matching the work identity."""

        from research.annual_report_assets import AnnualReportAssetCatalog

        item = self.get(work) if isinstance(work, str) else dict(work)
        asset = AnnualReportAssetCatalog(self.storage).find_reusable_filing(
            instrument_id=str(item.get("instrument_id") or ""),
            report_period=str(item.get("report_period") or ""),
            source=str(item.get("source") or "").strip().lower(),
            filing_id=str(item.get("announcement_id") or ""),
        )
        if asset is None:
            return None
        if str(asset.get("report_type") or "") != str(
            item.get("document_type") or ""
        ):
            return None
        return asset

    def recover_completed_without_bound_manifest(
        self,
        *,
        work_ids: Sequence[str] = (),
    ) -> dict[str, Any]:
        """Requeue positively identified empty completions without touching valid work."""

        clauses = [
            "work.status = 'completed'",
            "work.policy = 'latest_annual_only'",
            "frontier.status <> 'superseded'",
        ]
        params: list[Any] = []
        normalized_ids = tuple(
            sorted({str(item).strip() for item in work_ids if str(item).strip()})
        )
        if normalized_ids:
            placeholders = ",".join("?" for _ in normalized_ids)
            clauses.append(f"work.work_id IN ({placeholders})")
            params.extend(normalized_ids)
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT work.* FROM business_profile_work_items AS work "
                "JOIN business_profile_announcement_frontier AS frontier "
                "ON frontier.frontier_id = work.frontier_id WHERE "
                + " AND ".join(clauses)
                + " ORDER BY work.work_id",
                tuple(params),
            ).fetchall()
        candidates = [_decode_work_row(row) for row in rows]
        defective = [
            item
            for item in candidates
            if self.get_usable_bound_manifest(item) is None
        ]
        now = get_shanghai_time().isoformat()
        recovery_token = hashlib.sha256(now.encode("utf-8")).hexdigest()[:12]
        recovered_ids: list[str] = []
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            for item in defective:
                metadata = dict(item.get("metadata") or {})
                previous_stage_results = metadata.pop("stage_results", {})
                history = list(metadata.get("recovery_history") or [])
                history.append(
                    {
                        "reason": "completed_without_usable_bound_manifest",
                        "recovered_at": now,
                        "from_stage": item.get("stage"),
                        "from_status": item.get("status"),
                        "from_completed_at": item.get("completed_at"),
                        "from_attempt_count": item.get("attempt_count"),
                        "from_checkpoint_path": item.get("checkpoint_path"),
                        "invalidated_stage_results": previous_stage_results,
                    }
                )
                metadata["recovery_history"] = history[-10:]
                checkpoint = Path(str(item["checkpoint_path"]))
                recovered_checkpoint = checkpoint.with_name(
                    f"{checkpoint.stem}.recovery-{recovery_token}{checkpoint.suffix}"
                )
                cursor = conn.execute(
                    """
                    UPDATE business_profile_work_items
                    SET stage = 'acquire', status = 'pending', attempt_count = 0,
                        next_attempt_at = NULL, lease_owner = NULL,
                        lease_expires_at = NULL, checkpoint_path = ?,
                        last_error = NULL, metadata_json = ?, completed_at = NULL,
                        updated_at = ?
                    WHERE work_id = ? AND status = 'completed'
                      AND policy = 'latest_annual_only'
                    """,
                    (
                        str(recovered_checkpoint),
                        _canonical_json(metadata),
                        now,
                        item["work_id"],
                    ),
                )
                if int(cursor.rowcount or 0) == 1:
                    recovered_ids.append(str(item["work_id"]))
            conn.commit()
        return {
            "eligible_completed": len(candidates),
            "valid_manifest_preserved": len(candidates) - len(defective),
            "requeued": len(recovered_ids),
            "work_ids": recovered_ids,
        }

    def recover_completed_without_evidence(self) -> dict[str, Any]:
        """Requeue completed work whose stage history proves it produced no evidence."""

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_work_items "
                "WHERE status = 'completed' AND policy = 'latest_annual_only' "
                "ORDER BY work_id"
            ).fetchall()
        candidates: list[tuple[dict[str, Any], str]] = []
        for row in rows:
            item = _decode_work_row(row)
            metadata = dict(item.get("metadata") or {})
            stage_results = dict(metadata.get("stage_results") or {})
            affected_stage = _evidence_free_stage(stage_results)
            if affected_stage is None:
                continue
            if self.get_usable_bound_manifest(item) is not None:
                candidates.append((item, affected_stage))
        now = get_shanghai_time().isoformat()
        recovery_token = hashlib.sha256(
            f"evidence:{now}".encode("utf-8")
        ).hexdigest()[:12]
        recovered_ids: list[str] = []
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            for item, affected_stage in candidates:
                metadata = dict(item.get("metadata") or {})
                previous_stage_results = dict(metadata.get("stage_results") or {})
                history = list(metadata.get("recovery_history") or [])
                history.append(
                    {
                        "reason": "completed_without_evidence_backed_output",
                        "recovered_at": now,
                        "from_stage": item.get("stage"),
                        "from_status": item.get("status"),
                        "from_completed_at": item.get("completed_at"),
                        "from_attempt_count": item.get("attempt_count"),
                        "from_checkpoint_path": item.get("checkpoint_path"),
                        "affected_stage": affected_stage,
                        "invalidated_stage_results": previous_stage_results,
                    }
                )
                metadata["recovery_history"] = history[-10:]
                checkpoint = Path(str(item["checkpoint_path"]))
                recovered_checkpoint = checkpoint.with_name(
                    f"{checkpoint.stem}.evidence-recovery-{recovery_token}{checkpoint.suffix}"
                )
                if not _write_recovery_checkpoint(
                    checkpoint,
                    recovered_checkpoint,
                    affected_stage=affected_stage,
                ):
                    affected_stage = "acquire"
                affected_index = WORK_STAGES.index(affected_stage)
                preserved_results = {
                    stage: previous_stage_results[stage]
                    for stage in WORK_STAGES[:affected_index]
                    if stage in previous_stage_results
                }
                if preserved_results:
                    metadata["stage_results"] = preserved_results
                else:
                    metadata.pop("stage_results", None)
                cursor = conn.execute(
                    """
                    UPDATE business_profile_work_items
                    SET stage = ?, status = 'pending', attempt_count = 0,
                        next_attempt_at = NULL, lease_owner = NULL,
                        lease_expires_at = NULL, checkpoint_path = ?,
                        last_error = NULL, metadata_json = ?, completed_at = NULL,
                        updated_at = ?
                    WHERE work_id = ? AND status = 'completed'
                      AND policy = 'latest_annual_only'
                    """,
                    (
                        affected_stage,
                        str(recovered_checkpoint),
                        _canonical_json(metadata),
                        now,
                        item["work_id"],
                    ),
                )
                if int(cursor.rowcount or 0) == 1:
                    recovered_ids.append(str(item["work_id"]))
            conn.commit()
        return {
            "eligible_completed": len(rows),
            "valid_manifest_candidates": len(candidates),
            "requeued": len(recovered_ids),
            "work_ids": recovered_ids,
        }

    def recover_structured_semantic_retries(self) -> dict[str, Any]:
        """Resume only structured semantic retries proven to lack usable evidence."""

        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT * FROM business_profile_work_items "
                "WHERE stage = 'semantic' "
                "AND status IN ('retry_due', 'terminal_failure') "
                "ORDER BY work_id"
            ).fetchall()
        candidates: list[tuple[dict[str, Any], tuple[str, ...]]] = []
        for row in rows:
            item = _decode_work_row(row)
            if not _is_structured_semantic_quality_failure(item.get("last_error")):
                continue
            reasons = _structured_semantic_checkpoint_reasons(
                Path(str(item.get("checkpoint_path") or ""))
            )
            if not reasons or self.get_usable_bound_manifest(item) is None:
                continue
            candidates.append((item, reasons))
        now = get_shanghai_time().isoformat()
        recovered_ids: list[str] = []
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            for item, reasons in candidates:
                metadata = dict(item.get("metadata") or {})
                history = list(metadata.get("recovery_history") or [])
                history.append(
                    {
                        "reason": "structured_semantic_fallback_activated",
                        "recovered_at": now,
                        "from_stage": item.get("stage"),
                        "from_status": item.get("status"),
                        "from_attempt_count": item.get("attempt_count"),
                        "checkpoint_path": item.get("checkpoint_path"),
                        "zero_output_reasons": list(reasons),
                    }
                )
                metadata["recovery_history"] = history[-10:]
                cursor = conn.execute(
                    """
                    UPDATE business_profile_work_items
                    SET status = 'pending', next_attempt_at = NULL,
                        lease_owner = NULL, lease_expires_at = NULL,
                        last_error = NULL, metadata_json = ?, updated_at = ?
                    WHERE work_id = ? AND stage = 'semantic'
                      AND status IN ('retry_due', 'terminal_failure')
                    """,
                    (
                        _canonical_json(metadata),
                        now,
                        item["work_id"],
                    ),
                )
                if int(cursor.rowcount or 0) == 1:
                    recovered_ids.append(str(item["work_id"]))
            conn.commit()
        return {
            "eligible_semantic_retries": len(rows),
            "valid_manifest_candidates": len(candidates),
            "requeued": len(recovered_ids),
            "work_ids": recovered_ids,
        }

    def recover_stale_scope_items(
        self,
        *,
        processing_identity: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Repair stale semantic checkpoints without consuming content attempts."""

        marker = "stale semantic production checkpoint scope"
        identity_hash = (
            _stable_hash(processing_identity) if processing_identity is not None else None
        )
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT work_id, instrument_id, stage, status, attempt_count, "
                "processing_identity_hash, metadata_json, checkpoint_path "
                "FROM business_profile_work_items "
                "WHERE status IN ('retry_due', 'terminal_failure') "
                "AND last_error LIKE ? "
                + ("AND processing_identity_hash = ? " if identity_hash else "")
                + "ORDER BY work_id",
                (
                    (f"%{marker}%", identity_hash)
                    if identity_hash
                    else (f"%{marker}%",)
                ),
            ).fetchall()
        now = get_shanghai_time().isoformat()
        recovery_token = hashlib.sha256(
            f"stale-scope:{now}".encode("utf-8")
        ).hexdigest()[:12]
        recovered_ids: list[str] = []
        preserved = rotated = 0
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute("BEGIN IMMEDIATE")
            for row in rows:
                try:
                    metadata = json.loads(row["metadata_json"] or "{}")
                except (TypeError, json.JSONDecodeError):
                    metadata = {}
                metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
                checkpoint = Path(str(row["checkpoint_path"] or ""))
                checkpoint_cutoff = _checkpoint_knowledge_cutoff(checkpoint)
                previous_stage_results = dict(metadata.get("stage_results") or {})
                bound_identity = (
                    dict(processing_identity)
                    if processing_identity is not None
                    else dict(metadata.get("processing_identity") or {})
                )
                repeated_preserved_recovery = _was_preserved_scope_recovery(
                    metadata,
                    checkpoint_path=checkpoint,
                )
                checkpoint_compatible = bool(
                    checkpoint_cutoff
                    and not repeated_preserved_recovery
                    and _checkpoint_matches_work_scope(
                        checkpoint,
                        instrument_id=str(row["instrument_id"]),
                        knowledge_cutoff=checkpoint_cutoff,
                        processing_identity=bound_identity,
                    )
                )
                if checkpoint_compatible:
                    metadata["knowledge_cutoff"] = checkpoint_cutoff
                    recovered_stage = str(row["stage"])
                    recovered_checkpoint = checkpoint
                    recovery_reason = "stale_scope_cutoff_restored"
                else:
                    metadata.pop("stage_results", None)
                    recovered_stage = "acquire"
                    recovered_checkpoint = _rotated_checkpoint_path(
                        checkpoint,
                        checkpoint_root=self.checkpoint_root,
                        work_id=str(row["work_id"]),
                        token=f"scope-recovery-{recovery_token}",
                    )
                    recovery_reason = "stale_scope_checkpoint_rotated"
                history = list(metadata.get("recovery_history") or [])
                history.append(
                    {
                        "reason": recovery_reason,
                        "recovered_at": now,
                        "from_stage": row["stage"],
                        "from_status": row["status"],
                        "from_attempt_count": row["attempt_count"],
                        "from_checkpoint_path": row["checkpoint_path"],
                        "knowledge_cutoff": checkpoint_cutoff,
                        "invalidated_stage_results": (
                            previous_stage_results if checkpoint_cutoff is None else {}
                        ),
                    }
                )
                metadata["recovery_history"] = history[-10:]
                cursor = conn.execute(
                    "UPDATE business_profile_work_items SET stage = ?, status = 'pending', "
                    "attempt_count = 0, next_attempt_at = NULL, "
                    "lease_owner = NULL, lease_expires_at = NULL, last_error = NULL, "
                    "completed_at = NULL, checkpoint_path = ?, metadata_json = ?, "
                    "updated_at = ? "
                    "WHERE work_id = ? AND status IN ('retry_due', 'terminal_failure')",
                    (
                        recovered_stage,
                        str(recovered_checkpoint),
                        _canonical_json(metadata),
                        now,
                        row["work_id"],
                    ),
                )
                if int(cursor.rowcount or 0) == 1:
                    recovered_ids.append(str(row["work_id"]))
                    if checkpoint_compatible:
                        preserved += 1
                    else:
                        rotated += 1
            conn.commit()
        return {
            "eligible_stale_scope_items": len(rows),
            "requeued": len(recovered_ids),
            "checkpoint_preserved": preserved,
            "checkpoint_rotated": rotated,
            "processing_identity_filtered": processing_identity is not None,
            "work_ids": recovered_ids,
        }

    def ensure_work_knowledge_cutoff(
        self,
        work: str | Mapping[str, Any],
        *,
        fallback: str,
    ) -> str:
        """Return and durably bind the cutoff used by one work item."""

        if isinstance(work, str):
            item = self.get(work)
        else:
            work_id = str(work.get("work_id") or "")
            item = self.get(work_id) if work_id else dict(work)
        metadata = dict(item.get("metadata") or {})
        bound = _normalized_optional_date(metadata.get("knowledge_cutoff"))
        if bound is None:
            bound = _checkpoint_knowledge_cutoff(
                Path(str(item.get("checkpoint_path") or ""))
            )
        if bound is None:
            bound = _date_text(fallback, "knowledge_cutoff")
        if metadata.get("knowledge_cutoff") == bound:
            return bound
        metadata["knowledge_cutoff"] = bound
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            conn.execute(
                "UPDATE business_profile_work_items SET metadata_json = ?, "
                "updated_at = ? WHERE work_id = ?",
                (_canonical_json(metadata), now, str(item["work_id"])),
            )
            conn.commit()
        return bound

    def resolve_missing_document_exceptions(
        self,
        work: str | Mapping[str, Any],
    ) -> int:
        """Resolve only runtime exceptions caused by the now-repaired missing document."""

        item = self.get(work) if isinstance(work, str) else dict(work)
        if self.get_usable_bound_manifest(item) is None:
            return 0
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            rows = conn.execute(
                "SELECT exception_id, reason_codes_json, metadata_json "
                "FROM business_profile_exceptions "
                "WHERE instrument_id = ? AND status = 'open' "
                "AND tier = 'machine_rework' "
                "AND target_type = 'document_field_family'",
                (str(item["instrument_id"]),),
            ).fetchall()
        exception_ids: list[str] = []
        for row in rows:
            try:
                reasons = set(json.loads(row["reason_codes_json"] or "[]"))
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            source_document_id = str(metadata.get("source_document_id") or "")
            if (
                "planned_document_missing_or_invalid_locally" in reasons
                and bool(metadata.get("runtime_exception"))
                and source_document_id.startswith("unresolved-plan:")
            ):
                exception_ids.append(str(row["exception_id"]))
        if not exception_ids:
            return 0
        placeholders = ",".join("?" for _ in exception_ids)
        now = get_shanghai_time().isoformat()
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            cursor = conn.execute(
                "UPDATE business_profile_exceptions "
                "SET status = 'resolved', resolved_at = ?, updated_at = ? "
                f"WHERE exception_id IN ({placeholders}) AND status = 'open'",
                (now, now, *exception_ids),
            )
            conn.commit()
        return max(0, int(cursor.rowcount or 0))

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

    def claimable_count(
        self,
        stage: str,
        *,
        processing_identity_hash: str | None = None,
    ) -> int:
        identity_clause = (
            " AND processing_identity_hash = ?"
            if processing_identity_hash
            else ""
        )
        params: tuple[Any, ...] = (
            (_stage(stage), str(processing_identity_hash))
            if processing_identity_hash
            else (_stage(stage),)
        )
        with self.storage.get_connection() as conn:
            self.storage._apply_pragmas(conn)
            return int(
                conn.execute(
                    "SELECT COUNT(*) FROM business_profile_work_items "
                    "WHERE stage = ? AND status IN ('pending', 'retry_due')"
                    + identity_clause,
                    params,
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
        knowledge_cutoff: str,
        processing_identity: Mapping[str, Any],
        max_attempts: int,
        force: bool = False,
        supersede_older: bool,
    ) -> dict[str, int]:
        identity_hash = _stable_hash(processing_identity)
        now = get_shanghai_time().isoformat()
        inserted = reused = superseded = identity_superseded = reset = 0
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
                    "SELECT status, metadata_json, checkpoint_path "
                    "FROM business_profile_work_items WHERE work_id = ?",
                    (work_id,),
                ).fetchone()
                if existing is not None:
                    existing_status = str(existing["status"])
                    try:
                        existing_metadata = json.loads(
                            existing["metadata_json"] or "{}"
                        )
                    except (TypeError, json.JSONDecodeError):
                        existing_metadata = {}
                    existing_metadata = (
                        dict(existing_metadata)
                        if isinstance(existing_metadata, Mapping)
                        else {}
                    )
                    if (
                        _normalized_optional_date(
                            existing_metadata.get("knowledge_cutoff")
                        )
                        is None
                    ):
                        existing_metadata["knowledge_cutoff"] = (
                            _checkpoint_knowledge_cutoff(
                                Path(str(existing["checkpoint_path"] or ""))
                            )
                            or knowledge_cutoff
                        )
                        conn.execute(
                            "UPDATE business_profile_work_items SET metadata_json = ?, "
                            "updated_at = ? WHERE work_id = ?",
                            (_canonical_json(existing_metadata), now, work_id),
                        )
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
                        "knowledge_cutoff": knowledge_cutoff,
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
                cursor = conn.execute(
                    "UPDATE business_profile_work_items "
                    "SET status = 'superseded', next_attempt_at = NULL, "
                    "lease_owner = NULL, lease_expires_at = NULL, updated_at = ? "
                    "WHERE frontier_id = ? AND policy = ? AND work_id <> ? "
                    "AND processing_identity_hash <> ? "
                    "AND (status IN ('pending', 'retry_due', 'terminal_failure') "
                    "OR (status = 'running' AND lease_expires_at <= ?))",
                    (
                        now,
                        row["frontier_id"],
                        policy,
                        work_id,
                        identity_hash,
                        now,
                    ),
                )
                retired = int(cursor.rowcount or 0)
                identity_superseded += retired
                superseded += retired
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
                          AND work.status IN (
                              'pending', 'retry_due', 'terminal_failure'
                          )
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
            "identity_superseded": identity_superseded,
        }


class BusinessProfileFrontierBoundAcquirer:
    """Archive the exact official document selected into durable work."""

    def __init__(self, *, repository: BusinessProfileWorkRepository, archive_service: Any):
        self.repository = repository
        self.archive_service = archive_service

    def acquire(self, work: Mapping[str, Any]) -> dict[str, Any]:
        from research.business_profile_discovery import BusinessProfileDocumentCandidate
        from research.business_profile_documents import classify_business_profile_document

        frontier = self.repository.get_bound_frontier(work)
        existing = self.repository.get_usable_bound_manifest(work)
        if existing is not None:
            resolved = self.repository.resolve_missing_document_exceptions(work)
            return {
                "status": "unchanged",
                "frontier_id": frontier["frontier_id"],
                "source_file_id": existing["source_file_id"],
                "archive_path": existing["archive_path"],
                "resolved_missing_document_exceptions": resolved,
            }
        title = str(frontier["title"])
        classification = classify_business_profile_document(title, adjunct_type="PDF")
        if (
            not classification.selected
            or classification.document_type != str(work.get("document_type") or "")
        ):
            raise RuntimeError(
                "business-profile bound frontier classification mismatch: "
                f"{frontier['frontier_id']}"
            )
        metadata = dict(frontier.get("metadata") or {})
        candidate = BusinessProfileDocumentCandidate(
            announcement_id=str(frontier["announcement_id"]),
            title=title,
            announcement_time=frontier.get("published_at"),
            symbols=[str(frontier["symbol"])],
            adjunct_url=str(frontier["source_url"]),
            adjunct_type="PDF",
            classification=classification,
            selection_reasons=list(metadata.get("selection_reasons") or []),
            source=str(frontier["source"]).strip().lower(),
            source_tier=(
                "official_primary"
                if str(frontier["source"]).strip().lower() == "cninfo"
                else "official_backup"
            ),
            raw_payload={"frontier_id": str(frontier["frontier_id"])},
        )
        instrument = {
            "instrument_id": str(frontier["instrument_id"]),
            "symbol": str(frontier["symbol"]),
            "exchange": str(frontier["exchange"]),
        }
        archive = self.archive_service.archive_candidates(
            instrument,
            [candidate],
            max_documents=1,
            checkpoint_path=Path(str(work["checkpoint_path"])).with_suffix(
                ".acquire.json"
            ),
        )
        manifest = self.repository.get_usable_bound_manifest(work)
        if manifest is None:
            errors = "; ".join(
                str(item.get("error") or "") for item in archive.errors
            )
            raise RuntimeError(
                "business-profile bound acquisition produced no usable manifest: "
                f"frontier_id={frontier['frontier_id']} errors={errors or 'unknown'}"
            )
        resolved = self.repository.resolve_missing_document_exceptions(work)
        return {
            "status": "success",
            "frontier_id": frontier["frontier_id"],
            "source_file_id": manifest["source_file_id"],
            "archive_path": manifest["archive_path"],
            "archive": archive.to_dict(),
            "resolved_missing_document_exceptions": resolved,
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
        progress_log_interval_seconds: float = 30.0,
        write_coordinator: BusinessProfileWriteCoordinator | None = None,
    ) -> None:
        self.repository = repository
        self.discovery_runner = discovery_runner
        self.stage_runner = stage_runner
        self.lease_seconds = max(1, int(lease_seconds))
        self.retry_backoff_seconds = max(1, int(retry_backoff_seconds))
        self.progress_log_interval_seconds = max(
            0.1, float(progress_log_interval_seconds)
        )
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
        logger.info(
            "business-profile daily start cutoff=%s stages=%s",
            knowledge_cutoff,
            _stage_budget_log_value(stage_budgets),
        )
        recovery = await self.write_coordinator.run(
            self.repository.recover_completed_without_evidence
        )
        stale_scope_recovery = await self.write_coordinator.run(
            self.repository.recover_stale_scope_items,
            processing_identity=processing_identity,
        )
        structured_recovery = await self.write_coordinator.run(
            self.repository.recover_structured_semantic_retries
        )
        recovery["stale_scope"] = stale_scope_recovery
        recovery["structured_semantic"] = structured_recovery
        recovery["requeued"] = int(recovery.get("requeued") or 0) + int(
            structured_recovery.get("requeued") or 0
        ) + int(stale_scope_recovery.get("requeued") or 0)
        recovery["work_ids"] = sorted(
            {
                *list(recovery.get("work_ids") or []),
                *list(structured_recovery.get("work_ids") or []),
                *list(stale_scope_recovery.get("work_ids") or []),
            }
        )
        self._log_recovery(recovery)
        logger.info(
            "business-profile discovery start operation=daily scope=%s",
            _safe_discovery_scope(discovery_kwargs),
        )
        try:
            discovery = await self._run_discovery_with_heartbeat(
                operation="daily",
                discovery_kwargs=discovery_kwargs,
            )
        except Exception as exc:
            logger.warning(
                "business-profile discovery failed operation=daily category=%s",
                type(exc).__name__,
            )
            discovery = {
                "status": "failed",
                "errors": [f"{type(exc).__name__}: {exc}"],
            }
        self._log_discovery(discovery)
        enqueue = await self.write_coordinator.run(
            self.repository.enqueue_latest_annual,
            knowledge_cutoff=knowledge_cutoff,
            processing_identity=processing_identity,
            max_attempts=max_attempts,
        )
        logger.info(
            "business-profile enqueue eligible=%s inserted=%s reused=%s reset=%s "
            "superseded=%s",
            int(enqueue.get("eligible") or 0),
            int(enqueue.get("inserted") or 0),
            int(enqueue.get("reused") or 0),
            int(enqueue.get("reset") or 0),
            int(enqueue.get("superseded") or 0),
        )
        workers = await self._run_workers(
            stage_budgets,
            processing_identity=processing_identity,
        )
        health = await asyncio.to_thread(self.repository.health)
        throughput = _business_profile_throughput(enqueue, workers)
        status, reason_codes = _business_profile_operation_status(
            discovery=discovery,
            workers=workers,
        )
        report = {
            "schema_version": ASYNC_REPORT_SCHEMA_VERSION,
            "status": status,
            "reason_codes": reason_codes,
            "operation": "business_profile_daily_incremental",
            "knowledge_cutoff": knowledge_cutoff,
            "discovery": discovery,
            "recovery": recovery,
            "enqueue": enqueue,
            "workers": workers,
            "throughput": throughput,
            "queue_health": health,
            "writer": self.write_coordinator.snapshot(),
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        self._log_completion(report)
        return report

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
        started = time.monotonic()
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
        logger.info(
            "business-profile backfill start cutoff=%s policy=%s instruments=%s "
            "start_date=%s end_date=%s document_types=%s stages=%s",
            knowledge_cutoff,
            policy,
            len(instrument_ids),
            start_date,
            end_date,
            tuple(document_types),
            _stage_budget_log_value(stage_budgets or {}),
        )
        recovery = await self.write_coordinator.run(
            self.repository.recover_completed_without_evidence
        )
        stale_scope_recovery = await self.write_coordinator.run(
            self.repository.recover_stale_scope_items,
            processing_identity=processing_identity,
        )
        structured_recovery = await self.write_coordinator.run(
            self.repository.recover_structured_semantic_retries
        )
        recovery["stale_scope"] = stale_scope_recovery
        recovery["structured_semantic"] = structured_recovery
        recovery["requeued"] = int(recovery.get("requeued") or 0) + int(
            structured_recovery.get("requeued") or 0
        ) + int(stale_scope_recovery.get("requeued") or 0)
        recovery["work_ids"] = sorted(
            {
                *list(recovery.get("work_ids") or []),
                *list(structured_recovery.get("work_ids") or []),
                *list(stale_scope_recovery.get("work_ids") or []),
            }
        )
        self._log_recovery(recovery)
        discovery = None
        if discovery_kwargs is not None:
            logger.info(
                "business-profile discovery start operation=backfill scope=%s",
                _safe_discovery_scope(discovery_kwargs),
            )
            try:
                discovery = await self._run_discovery_with_heartbeat(
                    operation="backfill",
                    discovery_kwargs=discovery_kwargs,
                )
            except Exception as exc:
                logger.warning(
                    "business-profile discovery failed operation=backfill category=%s",
                    type(exc).__name__,
                )
                discovery = {
                    "status": "failed",
                    "errors": [f"{type(exc).__name__}: {exc}"],
                }
            self._log_discovery(discovery)
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
        logger.info(
            "business-profile enqueue eligible=%s inserted=%s reused=%s reset=%s "
            "superseded=%s",
            int(enqueue.get("eligible") or 0),
            int(enqueue.get("inserted") or 0),
            int(enqueue.get("reused") or 0),
            int(enqueue.get("reset") or 0),
            int(enqueue.get("superseded") or 0),
        )
        workers = await self._run_workers(
            stage_budgets or {},
            processing_identity=processing_identity,
            should_stop=should_stop,
        )
        throughput = _business_profile_throughput(enqueue, workers)
        status, reason_codes = _business_profile_operation_status(
            discovery=discovery,
            workers=workers,
        )
        report = {
            "schema_version": ASYNC_REPORT_SCHEMA_VERSION,
            "status": status,
            "reason_codes": reason_codes,
            "operation": "business_profile_backfill",
            "selection_policy": policy,
            "knowledge_cutoff": knowledge_cutoff,
            "discovery": discovery,
            "recovery": recovery,
            "enqueue": enqueue,
            "workers": workers,
            "throughput": throughput,
            "queue_health": await asyncio.to_thread(self.repository.health),
            "writer": self.write_coordinator.snapshot(),
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
        self._log_completion(report)
        return report

    async def _run_discovery_with_heartbeat(
        self,
        *,
        operation: str,
        discovery_kwargs: Mapping[str, Any],
    ) -> dict[str, Any]:
        started = time.monotonic()
        task = asyncio.ensure_future(self.discovery_runner(**dict(discovery_kwargs)))
        try:
            while not task.done():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(task),
                        timeout=self.progress_log_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    logger.info(
                        "business-profile discovery heartbeat operation=%s "
                        "elapsed_seconds=%.3f scope=%s",
                        operation,
                        time.monotonic() - started,
                        _safe_discovery_scope(discovery_kwargs),
                    )
            return dict(await task)
        except asyncio.CancelledError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            raise

    @staticmethod
    def _log_recovery(recovery: Mapping[str, Any]) -> None:
        stale = dict(recovery.get("stale_scope") or {})
        structured = dict(recovery.get("structured_semantic") or {})
        logger.info(
            "business-profile recovery requeued=%s stale_requeued=%s "
            "checkpoint_preserved=%s checkpoint_rotated=%s structured_requeued=%s",
            int(recovery.get("requeued") or 0),
            int(stale.get("requeued") or 0),
            int(stale.get("checkpoint_preserved") or 0),
            int(stale.get("checkpoint_rotated") or 0),
            int(structured.get("requeued") or 0),
        )

    @staticmethod
    def _log_discovery(discovery: Mapping[str, Any]) -> None:
        backlog = int(discovery.get("discovery_window_backlog") or 0)
        log = (
            logger.warning
            if backlog or str(discovery.get("status")) == "failed"
            else logger.info
        )
        log(
            "business-profile discovery end status=%s pages=%s announcements=%s "
            "selected=%s frontier_inserted=%s frontier_changed=%s backlog=%s "
            "incomplete_windows=%s windows=%s errors=%s",
            discovery.get("status"),
            int(discovery.get("pages_scanned") or 0),
            int(discovery.get("announcements_seen") or 0),
            int(discovery.get("selected_announcements") or 0),
            int(discovery.get("frontier_inserted") or 0),
            int(discovery.get("frontier_changed") or 0),
            backlog,
            len(discovery.get("incomplete_windows") or ()),
            [
                {
                    key: window.get(key)
                    for key in (
                        "exchange",
                        "start_date",
                        "end_date",
                        "stop_reason",
                        "splittable",
                    )
                }
                for window in discovery.get("incomplete_windows") or ()
                if isinstance(window, Mapping)
            ][:10],
            len(discovery.get("errors") or ()),
        )

    def _log_completion(self, report: Mapping[str, Any]) -> None:
        health = dict(report.get("queue_health") or {})
        writer = dict(report.get("writer") or {})
        log = (
            logger.warning
            if str(report.get("status")) == "degraded"
            else logger.info
        )
        log(
            "business-profile run end operation=%s status=%s elapsed_seconds=%s "
            "claimable=%s running=%s terminal=%s completed=%s "
            "writer_pending=%s writer_max_pending=%s writer_transactions=%s",
            report.get("operation"),
            report.get("status"),
            report.get("elapsed_seconds"),
            int(health.get("claimable") or 0),
            int(health.get("running") or 0),
            int(health.get("terminal") or 0),
            int(health.get("completed") or 0),
            int(writer.get("pending_writers") or 0),
            int(writer.get("max_pending_writers") or 0),
            int(writer.get("write_transactions") or 0),
        )

    async def _run_workers(
        self,
        stage_budgets: Mapping[str, StageBudget],
        *,
        processing_identity: Mapping[str, Any] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        processing_identity_hash = (
            _stable_hash(processing_identity) if processing_identity is not None else None
        )
        semantic_depth = await asyncio.to_thread(
            self.repository.claimable_count,
            "semantic",
            processing_identity_hash=processing_identity_hash,
        )
        semantic_limit = stage_budgets.get("semantic")
        acquire_backpressured = bool(
            semantic_limit and semantic_depth >= semantic_limit.high_water_mark
        )
        stage_done = {stage: asyncio.Event() for stage in WORK_STAGES}
        logger.info(
            "business-profile workers start semantic_depth=%s acquire_backpressured=%s",
            semantic_depth,
            acquire_backpressured,
        )

        async def run_stage(stage: str) -> tuple[str, dict[str, Any]]:
            budget = stage_budgets.get(stage)
            try:
                if budget is None:
                    logger.info(
                        "business-profile stage deferred stage=%s reason=no_stage_budget",
                        stage,
                    )
                    return stage, {
                        "status": "deferred",
                        "reason": "no_stage_budget",
                    }
                if stage == "acquire" and acquire_backpressured:
                    logger.warning(
                        "business-profile stage backpressured stage=acquire "
                        "semantic_depth=%s high_water_mark=%s",
                        semantic_depth,
                        semantic_limit.high_water_mark if semantic_limit else None,
                    )
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
                    processing_identity_hash=processing_identity_hash,
                    upstream_done=upstream_done,
                    should_stop=should_stop,
                )
            finally:
                stage_done[stage].set()

        results = await asyncio.gather(*(run_stage(stage) for stage in WORK_STAGES))
        workers = dict(results)
        logger.info(
            "business-profile workers end stages=%s writer=%s",
            {
                stage: {
                    key: result.get(key)
                    for key in (
                        "status",
                        "claimed",
                        "completed",
                        "retried",
                        "terminal_failures",
                    )
                }
                for stage, result in workers.items()
            },
            self.write_coordinator.snapshot(),
        )
        return workers

    async def _drain_stage(
        self,
        stage: str,
        budget: StageBudget,
        *,
        processing_identity_hash: str | None = None,
        upstream_done: asyncio.Event | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        claimed = completed = retried = failed = lease_conflicts = 0
        configuration_blocked = 0
        claimed_work_ids: set[str] = set()
        quality_totals: dict[str, Any] = {}
        errors: list[dict[str, str]] = []
        stopped = False
        lease_owner = f"async-{stage}-{get_shanghai_time().strftime('%Y%m%d%H%M%S%f')}"
        logger.info(
            "business-profile stage start stage=%s max_items=%s concurrency=%s "
            "max_elapsed_seconds=%s high_water_mark=%s",
            stage,
            budget.max_items,
            budget.max_concurrency,
            budget.max_elapsed_seconds,
            budget.high_water_mark,
        )
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
                processing_identity_hash=processing_identity_hash,
                exclude_work_ids=tuple(claimed_work_ids),
            )
            if not items:
                if upstream_done is None or upstream_done.is_set():
                    break
                await asyncio.sleep(0.05)
                continue
            claimed += len(items)
            claimed_work_ids.update(str(item["work_id"]) for item in items)

            async def run_one(item: Mapping[str, Any]) -> None:
                nonlocal completed, retried, failed, lease_conflicts
                nonlocal configuration_blocked
                try:
                    result = dict(await self.stage_runner(stage, item))
                    quality = dict(result.get("quality") or {})
                    for key in (
                        "blocking_machine_rework",
                        "selected_documents",
                        "selected_pages",
                        "outline_pages_scoped",
                        "evidence_records",
                        "record_count",
                        "verified_records",
                        "empty_output_documents",
                        "expected_non_disclosure_documents",
                        "structured_fallback_required",
                        "structured_fallback_calls",
                        "structured_fallback_accepted_records",
                        "structured_fallback_rejected",
                    ):
                        quality_totals[key] = quality_totals.get(key, 0) + int(
                            quality.get(key) or 0
                        )
                    for counter_name in (
                        "outline_sources",
                        "outline_confidences",
                        "empty_output_reasons",
                        "blocked_configuration_reasons",
                    ):
                        target = quality_totals.setdefault(counter_name, {})
                        for label, count in dict(
                            quality.get(counter_name) or {}
                        ).items():
                            target[str(label)] = target.get(str(label), 0) + int(
                                count or 0
                            )
                    if quality.get("blocked_configuration") is True:
                        reasons = ",".join(
                            sorted(
                                dict(
                                    quality.get("blocked_configuration_reasons") or {}
                                )
                            )
                        )
                        deferred_status = await self.write_coordinator.run(
                            self.repository.defer_configuration,
                            str(item["work_id"]),
                            lease_owner=lease_owner,
                            reason=(
                                "blocked_configuration:"
                                + (reasons or "semantic_gateway_unavailable")
                            ),
                            retry_after_seconds=self.retry_backoff_seconds,
                        )
                        if deferred_status == "configuration_blocked":
                            configuration_blocked += 1
                        else:
                            lease_conflicts += 1
                        return
                    status = str(result.get("status") or "").lower()
                    if status not in {"success", "completed", "unchanged"}:
                        raise RuntimeError(
                            str(result.get("reason") or status or "stage_failed")
                        )
                    if quality and not bool(quality.get("stage_ready", True)):
                        raise RuntimeError(
                            "business-profile stage quality gate failed: "
                            f"stage={stage} "
                            f"blocking_machine_rework={int(quality.get('blocking_machine_rework') or 0)} "
                            f"selected_documents={int(quality.get('selected_documents') or 0)}"
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
                    logger.warning(
                        "business-profile work failed work_id=%s instrument_id=%s "
                        "stage=%s disposition=%s category=%s error=%s",
                        item.get("work_id"),
                        item.get("instrument_id"),
                        stage,
                        terminal_status,
                        type(exc).__name__,
                        str(exc).replace("\n", " ")[:1000],
                    )
                    logger.debug(
                        "business-profile work failure traceback work_id=%s "
                        "instrument_id=%s stage=%s",
                        item.get("work_id"),
                        item.get("instrument_id"),
                        stage,
                        exc_info=True,
                    )

            batch_task = asyncio.create_task(self._run_stage_batch(items, run_one))
            batch_started = time.monotonic()
            try:
                while not batch_task.done():
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(batch_task),
                            timeout=self.progress_log_interval_seconds,
                        )
                    except asyncio.TimeoutError:
                        logger.info(
                            "business-profile stage heartbeat stage=%s "
                            "elapsed_seconds=%.3f batch_elapsed_seconds=%.3f "
                            "in_flight=%s claimed=%s completed=%s retried=%s "
                            "terminal_failures=%s configuration_blocked=%s "
                            "claim_exclusions=%s writer=%s",
                            stage,
                            time.monotonic() - started,
                            time.monotonic() - batch_started,
                            max(
                                0,
                                claimed
                                - completed
                                - retried
                                - failed
                                - configuration_blocked
                                - lease_conflicts,
                            ),
                            claimed,
                            completed,
                            retried,
                            failed,
                            configuration_blocked,
                            len(claimed_work_ids),
                            self.write_coordinator.snapshot(),
                        )
                await batch_task
            except asyncio.CancelledError:
                batch_task.cancel()
                try:
                    await batch_task
                except asyncio.CancelledError:
                    pass
                raise
            logger.info(
                "business-profile stage batch stage=%s batch_size=%s claimed=%s "
                "completed=%s retried=%s terminal_failures=%s "
                "configuration_blocked=%s lease_conflicts=%s elapsed_seconds=%.3f",
                stage,
                len(items),
                claimed,
                completed,
                retried,
                failed,
                configuration_blocked,
                lease_conflicts,
                time.monotonic() - started,
            )
            if should_stop is not None and should_stop():
                stopped = True
                break
        result = {
            "status": "stopped" if stopped else "success",
            "stop_requested": stopped,
            "claimed": claimed,
            "completed": completed,
            "retried": retried,
            "terminal_failures": failed,
            "lease_conflicts": lease_conflicts,
            "configuration_blocked": configuration_blocked,
            "claim_exclusions": len(claimed_work_ids),
            "errors": errors[:20],
            "quality": quality_totals,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "writer": self.write_coordinator.snapshot(),
        }
        log = (
            logger.warning
            if failed or retried or configuration_blocked
            else logger.info
        )
        log(
            "business-profile stage end stage=%s status=%s claimed=%s completed=%s "
            "retried=%s terminal_failures=%s configuration_blocked=%s "
            "lease_conflicts=%s claim_exclusions=%s elapsed_seconds=%s writer=%s",
            stage,
            result["status"],
            claimed,
            completed,
            retried,
            failed,
            configuration_blocked,
            lease_conflicts,
            result["claim_exclusions"],
            result["elapsed_seconds"],
            result["writer"],
        )
        return result

    @staticmethod
    async def _run_stage_batch(
        items: Sequence[Mapping[str, Any]],
        run_one: Callable[[Mapping[str, Any]], Awaitable[None]],
    ) -> None:
        await asyncio.gather(*(run_one(item) for item in items))


def _business_profile_operation_status(
    *,
    discovery: Mapping[str, Any] | None,
    workers: Mapping[str, Mapping[str, Any]],
) -> tuple[str, list[str]]:
    reason_codes: list[str] = []
    stopped = any(
        str(result.get("status") or "").lower() == "stopped"
        for result in workers.values()
    )
    if discovery is not None:
        discovery_status = str(discovery.get("status") or "failed").lower()
        resumable_backlog = bool(
            discovery_status == "degraded"
            and int(discovery.get("discovery_window_backlog") or 0) > 0
            and not discovery.get("errors")
        )
        if discovery_status not in {"success", "unchanged"} and not resumable_backlog:
            reason_codes.append("discovery_failed")
    counter_reasons = (
        ("retried", "worker_retries"),
        ("terminal_failures", "worker_terminal_failures"),
        ("configuration_blocked", "worker_configuration_blocked"),
        ("lease_conflicts", "worker_lease_conflicts"),
    )
    for counter, reason in counter_reasons:
        if any(int(result.get(counter) or 0) > 0 for result in workers.values()):
            reason_codes.append(reason)
    if stopped:
        return "stopped", ["stop_requested", *reason_codes]
    return ("degraded" if reason_codes else "success"), reason_codes


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


def _normalized_optional_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return _date_text(value, "knowledge_cutoff")
    except ValueError:
        return None


def _checkpoint_matches_work_scope(
    path: Path,
    *,
    instrument_id: str,
    knowledge_cutoff: str,
    processing_identity: Mapping[str, Any],
) -> bool:
    if not path.is_file() or not processing_identity:
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return False
    scope = payload.get("scope") if isinstance(payload, Mapping) else None
    if not isinstance(scope, Mapping):
        return False
    required_scope_fields = {
        "instruments",
        "field_families",
        "knowledge_cutoff",
        "identities",
        "promotion_manifest_hashes",
    }
    if not required_scope_fields.issubset(scope):
        return False
    field_families = tuple(processing_identity.get("field_families") or ())
    identities = dict(processing_identity.get("runtime_identities") or {})
    if not field_families or not identities:
        return False
    expected = {
        "instruments": (instrument_id,),
        "field_families": field_families,
        "knowledge_cutoff": knowledge_cutoff,
        "identities": identities,
        "promotion_manifest_hashes": dict(
            processing_identity.get("promotion_manifest_hashes") or {}
        ),
    }
    from research.business_profile_semantic_pipeline import (
        semantic_production_logical_scope_payload,
    )

    return semantic_production_logical_scope_payload(
        scope
    ) == semantic_production_logical_scope_payload(expected)


def _was_preserved_scope_recovery(
    metadata: Mapping[str, Any],
    *,
    checkpoint_path: Path,
) -> bool:
    history = metadata.get("recovery_history")
    if not isinstance(history, Sequence) or isinstance(history, (str, bytes)):
        return False
    for raw in reversed(history):
        if not isinstance(raw, Mapping):
            continue
        if str(raw.get("reason") or "") != "stale_scope_cutoff_restored":
            return False
        recovered_path = str(
            raw.get("from_checkpoint_path") or raw.get("checkpoint_path") or ""
        )
        return recovered_path == str(checkpoint_path)
    return False


def _checkpoint_knowledge_cutoff(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    scope = payload.get("scope")
    if not isinstance(scope, Mapping):
        return None
    return _normalized_optional_date(scope.get("knowledge_cutoff"))


def _rotated_checkpoint_path(
    checkpoint: Path,
    *,
    checkpoint_root: Path,
    work_id: str,
    token: str,
) -> Path:
    base = checkpoint if checkpoint.name else checkpoint_root / f"{work_id}.json"
    suffix = base.suffix or ".json"
    return base.with_name(f"{base.stem}.{token}{suffix}")


def _stage_budget_log_value(
    budgets: Mapping[str, StageBudget],
) -> dict[str, dict[str, Any]]:
    return {
        stage: {
            "max_items": budget.max_items,
            "max_concurrency": budget.max_concurrency,
            "max_elapsed_seconds": budget.max_elapsed_seconds,
            "high_water_mark": budget.high_water_mark,
        }
        for stage, budget in budgets.items()
    }


def _safe_discovery_scope(value: Mapping[str, Any]) -> dict[str, Any]:
    safe_keys = (
        "exchanges",
        "start_date",
        "end_date",
        "lookback_days",
        "overlap_days",
        "page_size",
        "max_pages_per_market",
        "max_windows_per_market",
        "category",
    )
    return {key: value.get(key) for key in safe_keys if key in value}


def _retryable(exc: Exception) -> bool:
    return isinstance(exc, (OSError, RuntimeError, TimeoutError, asyncio.TimeoutError))


def _evidence_free_stage(stage_results: Mapping[str, Any]) -> str | None:
    """Return the earliest stage proven to have blocked evidence production."""

    for stage in ("parse", "semantic", "publish"):
        raw_result = stage_results.get(stage)
        if not isinstance(raw_result, Mapping) or not raw_result:
            continue
        result = dict(raw_result)
        quality = dict(result.get("quality") or {})
        if quality and not bool(quality.get("stage_ready", True)):
            return stage
        metrics = dict(result.get("metrics") or {})
        if int(metrics.get("errors") or 0) > 0:
            return stage
        if stage == "parse" and "pages" in metrics:
            if int(metrics.get("pages") or 0) == 0:
                return stage
    return None


def _write_recovery_checkpoint(
    source: Path,
    target: Path,
    *,
    affected_stage: str,
) -> bool:
    """Create a retry checkpoint that retains only valid upstream artifacts."""

    pipeline_stage = {
        "parse": "select",
        "semantic": "extract",
        "publish": "verify",
    }.get(affected_stage)
    if pipeline_stage is None or not source.is_file():
        return False
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, Mapping):
        return False
    pipeline_stages = ("plan", "select", "extract", "verify", "promote")
    affected_index = pipeline_stages.index(pipeline_stage)
    completed = set(payload.get("completed_stages") or [])
    required = set(pipeline_stages[:affected_index])
    if not required.issubset(completed):
        return False
    recovered = dict(payload)
    recovered["completed_stages"] = list(pipeline_stages[:affected_index])
    recovered["artifacts"] = {
        stage: artifact
        for stage, artifact in dict(payload.get("artifacts") or {}).items()
        if stage in required
    }
    recovered["metrics"] = {}
    recovered["status"] = "partial" if required else "pending"
    recovered["stopped_reason"] = None
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".part")
    try:
        temporary.write_text(_canonical_json(recovered), encoding="utf-8")
        os.replace(temporary, target)
    except OSError:
        temporary.unlink(missing_ok=True)
        return False
    return True


def _structured_semantic_checkpoint_reasons(path: Path) -> tuple[str, ...]:
    if not path.is_file():
        return ()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(payload, Mapping):
        return ()
    metrics = dict(payload.get("metrics") or {})
    if (
        int(metrics.get("selected_pages") or metrics.get("pages") or 0) <= 0
        or int(metrics.get("evidence_records") or 0) != 0
        or int(metrics.get("record_count") or 0) != 0
    ):
        return ()
    reasons = set(dict(metrics.get("empty_output_reasons") or {}))
    for raw in dict(metrics.get("by_field_family") or {}).values():
        family = dict(raw or {})
        reasons.update(dict(family.get("reason_code_counts") or {}))
    affected = reasons.intersection(
        {"ambiguous_table_layout", "deterministic_parser_failure"}
    )
    return tuple(sorted(affected))


def _is_structured_semantic_quality_failure(error: Any) -> bool:
    normalized = str(error or "").strip().lower()
    return "quality_gate:extract" in normalized or (
        "stage quality gate failed" in normalized and "stage=semantic" in normalized
    )


def _business_profile_throughput(
    enqueue: Mapping[str, Any],
    workers: Mapping[str, Any],
) -> dict[str, Any]:
    stage_completed = {
        stage: int(dict(result or {}).get("completed") or 0)
        for stage, result in workers.items()
    }
    return {
        "enqueued": int(enqueue.get("inserted") or 0),
        "requeued": int(enqueue.get("reset") or 0),
        "superseded": int(enqueue.get("superseded") or 0),
        "worker_completed": int(stage_completed.get("publish") or 0),
        "stage_completed": stage_completed,
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
