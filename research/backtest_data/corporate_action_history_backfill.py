"""Resumable historical projection over local corporate-action evidence."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from research.backtest_data.corporate_action_projection import (
    CanonicalCorporateActionProjector,
)
from utils.date_utils import get_shanghai_time


CHECKPOINT_SCHEMA_VERSION = "canonical-corporate-action-history.v1"
DEFAULT_CHECKPOINT_ROOT = Path("data/runtime/canonical_corporate_action_backfill")
_CHECKPOINT_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def _stable_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _normalized_strings(values: Optional[Sequence[str]]) -> list[str]:
    return sorted({str(item).strip() for item in values or () if str(item).strip()})


def normalize_history_backfill_parameters(
    *,
    db_path: str | Path,
    batch_size: int = 500,
    instrument_ids: Optional[Sequence[str]] = None,
    source_event_keys: Optional[Sequence[str]] = None,
) -> dict[str, Any]:
    """Normalize parameters that define checkpoint and batch identity."""
    normalized_batch_size = int(batch_size)
    if normalized_batch_size < 1 or normalized_batch_size > 5000:
        raise ValueError("batch_size must be between 1 and 5000")
    return {
        "database_path": str(Path(db_path).expanduser().resolve()),
        "batch_size": normalized_batch_size,
        "instrument_ids": _normalized_strings(instrument_ids),
        "source_event_keys": _normalized_strings(source_event_keys),
    }


def build_source_batches(
    universe: Sequence[Mapping[str, Any]], batch_size: int
) -> list[dict[str, Any]]:
    """Split an ordered frozen universe into content-addressed batches."""
    batches: list[dict[str, Any]] = []
    for offset in range(0, len(universe), batch_size):
        items = [dict(item) for item in universe[offset : offset + batch_size]]
        batch_hash = _stable_hash({"items": items})
        batches.append(
            {
                "batch_id": f"batch_{offset // batch_size:06d}_{batch_hash[:16]}",
                "offset": offset,
                "count": len(items),
                "observation_ids": [int(item["observation_id"]) for item in items],
                "identity_hash": batch_hash,
            }
        )
    return batches


class CorporateActionHistoryCheckpointStore:
    """Atomic JSON checkpoints bound to parameters and a frozen source universe."""

    def __init__(self, root: str | Path = DEFAULT_CHECKPOINT_ROOT):
        self.root = Path(root)

    def path_for(self, checkpoint_id: str) -> Path:
        if not _CHECKPOINT_ID_PATTERN.fullmatch(str(checkpoint_id)):
            raise ValueError("checkpoint_id may contain only letters, digits, '-' and '_'")
        return self.root / f"{checkpoint_id}.json"

    def resolve_id(
        self,
        parameters: Mapping[str, Any],
        source_universe_hash: str,
        explicit_checkpoint_id: Optional[str] = None,
    ) -> str:
        if explicit_checkpoint_id:
            self.path_for(explicit_checkpoint_id)
            return str(explicit_checkpoint_id)
        identity_hash = _stable_hash(
            {
                "parameters": dict(parameters),
                "source_universe_hash": source_universe_hash,
            }
        )
        return f"canonical_ca_{identity_hash[:20]}"

    def initialize(
        self,
        *,
        checkpoint_id: str,
        parameters: Mapping[str, Any],
        source_universe_hash: str,
        batches: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        now = get_shanghai_time().isoformat()
        return {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "checkpoint_id": checkpoint_id,
            "parameter_hash": _stable_hash(dict(parameters)),
            "parameters": dict(parameters),
            "source_universe_hash": source_universe_hash,
            "batch_identities": [
                {
                    "batch_id": str(batch["batch_id"]),
                    "identity_hash": str(batch["identity_hash"]),
                    "count": int(batch["count"]),
                }
                for batch in batches
            ],
            "completed_batch_ids": [],
            "counters": {
                "considered": 0,
                "ready": 0,
                "blocked": 0,
                "inserted": 0,
                "unchanged": 0,
            },
            "blocker_reasons": {},
            "latest_watermark": None,
            "last_report": None,
            "created_at": now,
            "updated_at": now,
        }

    def load(
        self,
        *,
        checkpoint_id: str,
        parameters: Mapping[str, Any],
        source_universe_hash: str,
        batches: Sequence[Mapping[str, Any]],
    ) -> Optional[dict[str, Any]]:
        path = self.path_for(checkpoint_id)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported corporate-action checkpoint schema")
        if payload.get("parameter_hash") != _stable_hash(dict(parameters)):
            raise ValueError("checkpoint parameters do not match the requested run")
        if payload.get("source_universe_hash") != source_universe_hash:
            raise ValueError(
                "checkpoint source universe changed; use a new checkpoint id or restart"
            )
        expected_batches = [
            {
                "batch_id": str(batch["batch_id"]),
                "identity_hash": str(batch["identity_hash"]),
                "count": int(batch["count"]),
            }
            for batch in batches
        ]
        if payload.get("batch_identities") != expected_batches:
            raise ValueError("checkpoint batch identities do not match the requested run")
        return payload

    def save(self, payload: Mapping[str, Any]) -> Path:
        path = self.path_for(str(payload["checkpoint_id"]))
        path.parent.mkdir(parents=True, exist_ok=True)
        persisted = dict(payload)
        persisted["updated_at"] = get_shanghai_time().isoformat()
        encoded = _stable_json(persisted)
        temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(path)
        return path


class CanonicalCorporateActionHistoryBackfill:
    """Bounded operator workflow; it never invokes provider acquisition."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        checkpoint_root: str | Path = DEFAULT_CHECKPOINT_ROOT,
        logger: Optional[logging.Logger] = None,
    ):
        self.db_path = Path(db_path)
        self.projector = CanonicalCorporateActionProjector(self.db_path)
        self.checkpoints = CorporateActionHistoryCheckpointStore(checkpoint_root)
        self.logger = logger or logging.getLogger(__name__)

    def run(
        self,
        *,
        dry_run: bool = True,
        batch_size: int = 500,
        resume: bool = True,
        checkpoint_id: Optional[str] = None,
        instrument_ids: Optional[Sequence[str]] = None,
        source_event_keys: Optional[Sequence[str]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> dict[str, Any]:
        parameters = normalize_history_backfill_parameters(
            db_path=self.db_path,
            batch_size=batch_size,
            instrument_ids=instrument_ids,
            source_event_keys=source_event_keys,
        )
        universe_result = self.projector.select_source_universe(
            instrument_ids=parameters["instrument_ids"],
            source_event_keys=parameters["source_event_keys"],
            should_stop=should_stop,
        )
        universe = universe_result["items"]
        source_universe_hash = str(universe_result["source_universe_hash"])
        batches = build_source_batches(universe, parameters["batch_size"])
        resolved_checkpoint_id = self.checkpoints.resolve_id(
            parameters,
            source_universe_hash,
            explicit_checkpoint_id=checkpoint_id,
        )
        watermark_before = self.projector.read_latest_watermark()
        base_report: dict[str, Any] = {
            "stage": "canonical_corporate_action_history_backfill",
            "operation": "backfill_canonical_corporate_actions_history",
            "dry_run": bool(dry_run),
            "resume": bool(resume),
            "provider_usage": [],
            "network_requests": 0,
            "database_id": self.projector.store.database_id,
            "database_path": parameters["database_path"],
            "checkpoint_id": resolved_checkpoint_id,
            "checkpoint_path": str(self.checkpoints.path_for(resolved_checkpoint_id)),
            "source_universe_hash": source_universe_hash,
            "requested_scope": {
                "instrument_ids": parameters["instrument_ids"],
                "source_event_keys": parameters["source_event_keys"],
            },
            "selected": len(universe),
            "total_batches": len(batches),
            "watermark_before": watermark_before,
        }
        if universe_result.get("stopped"):
            return {
                **base_report,
                "status": "stopped",
                "blockers": ["stop_requested"],
                "failed_batches": [],
                "completed_batches": 0,
                "considered": 0,
                "ready": 0,
                "blocked": 0,
                "blocker_reasons": {},
                "inserted": 0,
                "unchanged": 0,
                "would_change": 0,
                "watermark": watermark_before,
                "watermark_changed": False,
            }
        if not universe_result.get("available", True):
            return {
                **base_report,
                "status": "unavailable",
                "blockers": ["corporate_action_observations_missing"],
                "failed_batches": [],
                "completed_batches": 0,
                "considered": 0,
                "ready": 0,
                "blocked": 0,
                "blocker_reasons": {},
                "inserted": 0,
                "unchanged": 0,
                "would_change": 0,
                "watermark": watermark_before,
            }

        checkpoint: Optional[dict[str, Any]] = None
        if not dry_run and resume:
            checkpoint = self.checkpoints.load(
                checkpoint_id=resolved_checkpoint_id,
                parameters=parameters,
                source_universe_hash=source_universe_hash,
                batches=batches,
            )
        if checkpoint is None:
            checkpoint = self.checkpoints.initialize(
                checkpoint_id=resolved_checkpoint_id,
                parameters=parameters,
                source_universe_hash=source_universe_hash,
                batches=batches,
            )
            if not dry_run:
                self.checkpoints.save(checkpoint)

        counters = (
            Counter()
            if dry_run
            else Counter({key: int(value) for key, value in checkpoint["counters"].items()})
        )
        blocker_reasons = (
            Counter()
            if dry_run
            else Counter(
                {key: int(value) for key, value in checkpoint["blocker_reasons"].items()}
            )
        )
        completed = set(checkpoint["completed_batch_ids"] if not dry_run else [])
        successful_batches = 0
        stopped = False
        failed_batches: list[dict[str, Any]] = []
        self.logger.info(
            "Canonical corporate-action history projection started: selected=%s batches=%s dry_run=%s checkpoint=%s",
            len(universe),
            len(batches),
            dry_run,
            resolved_checkpoint_id,
        )
        for batch_number, batch in enumerate(batches, start=1):
            if should_stop is not None and should_stop():
                stopped = True
                break
            batch_id = str(batch["batch_id"])
            if batch_id in completed:
                continue
            self.logger.info(
                "Canonical corporate-action projection batch %s/%s: id=%s rows=%s",
                batch_number,
                len(batches),
                batch_id,
                batch["count"],
            )
            try:
                expected_source_items = [
                    dict(item)
                    for item in universe[
                        int(batch["offset"]) : int(batch["offset"]) + int(batch["count"])
                    ]
                ]
                batch_report = self.projector.project(
                    observation_ids=batch["observation_ids"],
                    expected_source_items=expected_source_items,
                    batch_commit=(
                        {
                            "checkpoint_id": resolved_checkpoint_id,
                            "batch_id": batch_id,
                            "source_universe_hash": source_universe_hash,
                            "batch_identity_hash": str(batch["identity_hash"]),
                        }
                        if not dry_run and resume
                        else None
                    ),
                    dry_run=bool(dry_run),
                )
            except Exception as exc:
                failure = {
                    "batch_id": batch_id,
                    "batch_number": batch_number,
                    "count": int(batch["count"]),
                    "error": str(exc),
                }
                failed_batches.append(failure)
                self.logger.exception(
                    "Canonical corporate-action projection batch failed: %s", batch_id
                )
                if not dry_run:
                    checkpoint["last_report"] = failure
                    self.checkpoints.save(checkpoint)
                break
            for key in ("considered", "ready", "blocked", "inserted", "unchanged"):
                counters[key] += int(batch_report.get(key, 0))
            counters["would_change"] += int(batch_report.get("would_change", 0))
            blocker_reasons.update(batch_report.get("blocker_reasons") or {})
            successful_batches += 1
            if not dry_run:
                completed.add(batch_id)
                checkpoint["completed_batch_ids"] = sorted(completed)
                checkpoint["counters"] = {
                    key: int(counters[key])
                    for key in ("considered", "ready", "blocked", "inserted", "unchanged")
                }
                checkpoint["blocker_reasons"] = dict(sorted(blocker_reasons.items()))
                checkpoint["latest_watermark"] = self.projector.read_latest_watermark()
                checkpoint["last_report"] = batch_report
                self.checkpoints.save(checkpoint)

        watermark_after = self.projector.read_latest_watermark()
        status = (
            "stopped"
            if stopped
            else "dry_run"
            if dry_run and not failed_batches
            else "failed"
            if failed_batches
            else "success"
        )
        result = {
            **base_report,
            "status": status,
            "blockers": ["stop_requested"] if stopped else [],
            "completed_batches": len(completed) if not dry_run else successful_batches,
            "failed_batches": failed_batches,
            "considered": int(counters["considered"]),
            "ready": int(counters["ready"]),
            "blocked": int(counters["blocked"]),
            "blocker_reasons": dict(sorted(blocker_reasons.items())),
            "inserted": int(counters["inserted"]),
            "unchanged": int(counters["unchanged"]),
            "would_change": int(counters["would_change"]),
            "watermark": watermark_after,
            "watermark_changed": watermark_before != watermark_after,
        }
        self.logger.info(
            "Canonical corporate-action history projection finished: status=%s considered=%s inserted=%s unchanged=%s blocked=%s",
            status,
            result["considered"],
            result["inserted"],
            result["unchanged"],
            result["blocked"],
        )
        return result
