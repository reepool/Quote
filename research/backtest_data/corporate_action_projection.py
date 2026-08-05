"""Deterministic canonical corporate-action projection over existing evidence."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from research.backtest_data.quote_store import BacktestQuoteStore, semantic_hash
from utils.date_utils import get_shanghai_time


FACTOR_ACTION_TYPES = {
    "cash_dividend",
    "dividend",
    "bonus_shares",
    "capitalization",
    "rights_issue",
    "split",
    "mixed",
}


def _date(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)[:10]


def _aware(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed.isoformat()


def _latest_time(*values: Any) -> str:
    normalized = [item for item in (_aware(value) for value in values) if item]
    return max(normalized) if normalized else get_shanghai_time().isoformat()


class CanonicalCorporateActionProjector:
    """Project governed local evidence; this class never performs acquisition."""

    projection_version = "canonical-corporate-action.v1"

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.store = BacktestQuoteStore(self.db_path)

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        """Open an existing database without allowing SQLite to create it."""
        uri = f"{self.db_path.expanduser().resolve().as_uri()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def read_latest_watermark(self) -> Optional[int]:
        """Read the quote-domain watermark without initializing or migrating storage."""
        try:
            with self._read_connection() as connection:
                if not self._table_exists(connection, "data_change_log"):
                    return 0
                row = connection.execute(
                    "SELECT MAX(sequence_id) AS sequence FROM data_change_log "
                    "WHERE domain = 'backtest'"
                ).fetchone()
                return int(row["sequence"] or 0)
        except sqlite3.OperationalError:
            return None

    def _table_exists(self, connection: sqlite3.Connection, table: str) -> bool:
        return connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone() is not None

    def _fetch_optional(
        self,
        connection: sqlite3.Connection,
        table: str,
        *,
        instrument_id: str,
        source_event_key: str,
        order_by: str = "updated_at DESC, id DESC",
        extra_where: str = "",
    ) -> Optional[dict[str, Any]]:
        if not self._table_exists(connection, table):
            return None
        row = connection.execute(
            f"SELECT * FROM {table} WHERE instrument_id = ? AND source_event_key = ? "
            f"{extra_where} ORDER BY {order_by} LIMIT 1",
            (instrument_id, source_event_key),
        ).fetchone()
        return dict(row) if row else None

    def select_source_universe(
        self,
        *,
        instrument_ids: Optional[Sequence[str]] = None,
        source_event_keys: Optional[Sequence[str]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> dict[str, Any]:
        """Return ordered observation identities plus their governed evidence hash."""
        clauses = ["1=1"]
        params: list[Any] = []
        normalized_instruments = sorted(
            {str(item).strip() for item in instrument_ids or () if str(item).strip()}
        )
        normalized_events = sorted(
            {str(item).strip() for item in source_event_keys or () if str(item).strip()}
        )
        if normalized_instruments:
            placeholders = ",".join("?" for _ in normalized_instruments)
            clauses.append(f"instrument_id IN ({placeholders})")
            params.extend(normalized_instruments)
        if normalized_events:
            placeholders = ",".join("?" for _ in normalized_events)
            clauses.append(f"source_event_key IN ({placeholders})")
            params.extend(normalized_events)
        try:
            connection_context = self._read_connection()
            connection = connection_context.__enter__()
        except sqlite3.OperationalError:
            return {
                "available": False,
                "stopped": False,
                "items": [],
                "source_universe_hash": semantic_hash({"items": []}),
            }
        try:
            if not self._table_exists(connection, "corporate_action_observations"):
                return {
                    "available": False,
                    "stopped": False,
                    "items": [],
                    "source_universe_hash": semantic_hash({"items": []}),
                }
            observation_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(corporate_action_observations)"
                ).fetchall()
            }
            if "is_current" in observation_columns:
                clauses.append("is_current = 1")
            row_hash_expression = "row_hash" if "row_hash" in observation_columns else "NULL"
            rows = connection.execute(
                "SELECT *, "
                f"{row_hash_expression} AS selected_row_hash "
                "FROM corporate_action_observations WHERE "
                + " AND ".join(clauses)
                + " ORDER BY instrument_id, source_event_key, id",
                params,
            ).fetchall()
            items = []
            for row in rows:
                if should_stop is not None and should_stop():
                    return {
                        "available": True,
                        "stopped": True,
                        "items": [],
                        "source_universe_hash": semantic_hash({"items": []}),
                    }
                observation = dict(row)
                projection = self._build_projection(connection, observation)
                items.append(self._source_item(observation, projection))
        finally:
            connection_context.__exit__(None, None, None)
        return {
            "available": True,
            "stopped": False,
            "items": items,
            "source_universe_hash": semantic_hash({"items": items}),
        }

    def project(
        self,
        *,
        instrument_ids: Optional[Sequence[str]] = None,
        source_event_keys: Optional[Sequence[str]] = None,
        observation_ids: Optional[Sequence[int]] = None,
        expected_source_items: Optional[Sequence[Mapping[str, Any]]] = None,
        batch_commit: Optional[Mapping[str, Any]] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Rebuild changed events from local evidence under an explicit scope."""
        if not dry_run:
            self.store.initialize()
        clauses = ["1=1"]
        params: list[Any] = []
        if instrument_ids:
            placeholders = ",".join("?" for _ in instrument_ids)
            clauses.append(f"instrument_id IN ({placeholders})")
            params.extend(str(item) for item in instrument_ids)
        if source_event_keys:
            placeholders = ",".join("?" for _ in source_event_keys)
            clauses.append(f"source_event_key IN ({placeholders})")
            params.extend(str(item) for item in source_event_keys)
        if observation_ids is not None:
            normalized_observation_ids = sorted({int(item) for item in observation_ids})
            if not normalized_observation_ids:
                return self._report(
                    considered=0,
                    inserted=0,
                    unchanged=0,
                    ready=0,
                    blocked=0,
                    blocker_reasons={},
                    blockers=[],
                    dry_run=dry_run,
                    would_change=0,
                )
            placeholders = ",".join("?" for _ in normalized_observation_ids)
            clauses.append(f"id IN ({placeholders})")
            params.extend(normalized_observation_ids)
        try:
            connection_context = (
                self._read_connection() if dry_run else self.store.connection()
            )
            connection = connection_context.__enter__()
        except sqlite3.OperationalError:
            return self._report(
                considered=0,
                inserted=0,
                unchanged=0,
                ready=0,
                blocked=0,
                blocker_reasons={},
                blockers=["corporate_action_observations_missing"],
                dry_run=dry_run,
                would_change=0,
            )
        try:
            if not self._table_exists(connection, "corporate_action_observations"):
                return self._report(
                    considered=0,
                    inserted=0,
                    unchanged=0,
                    ready=0,
                    blocked=0,
                    blocker_reasons={},
                    blockers=["corporate_action_observations_missing"],
                    dry_run=dry_run,
                    would_change=0,
                )
            observation_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(corporate_action_observations)"
                ).fetchall()
            }
            if "is_current" in observation_columns:
                clauses.append("is_current = 1")
            observations = connection.execute(
                "SELECT * FROM corporate_action_observations WHERE " + " AND ".join(clauses) +
                " ORDER BY instrument_id, source_event_key, id",
                params,
            ).fetchall()
            projected = [self._build_projection(connection, dict(row)) for row in observations]
            if expected_source_items is not None:
                actual_source_items = [
                    self._source_item(dict(observation), projection)
                    for observation, projection in zip(observations, projected)
                ]
                expected_items = [dict(item) for item in expected_source_items]
                if actual_source_items != expected_items:
                    raise ValueError(
                        "source universe changed during batch projection; restart with a new checkpoint"
                    )
            existing_revisions: dict[tuple[str, str], str] = {}
            current_hashes: dict[str, str] = {}
            if dry_run and self._table_exists(
                connection, "canonical_corporate_action_revisions"
            ):
                event_ids = sorted(
                    {str(row["canonical_event_id"]) for row in projected}
                )
                for offset in range(0, len(event_ids), 800):
                    event_chunk = event_ids[offset : offset + 800]
                    placeholders = ",".join("?" for _ in event_chunk)
                    existing_revisions.update(
                        {
                            (
                                str(row["canonical_event_id"]),
                                str(row["projection_revision_id"]),
                            ): str(row["input_hash"])
                            for row in connection.execute(
                                "SELECT canonical_event_id, projection_revision_id, input_hash "
                                "FROM canonical_corporate_action_revisions "
                                f"WHERE canonical_event_id IN ({placeholders})",
                                event_chunk,
                            ).fetchall()
                        }
                    )
                    if self._table_exists(
                        connection, "canonical_corporate_action_current"
                    ):
                        current_hashes.update(
                            {
                                str(row["canonical_event_id"]): str(row["input_hash"])
                                for row in connection.execute(
                                    "SELECT c.canonical_event_id, r.input_hash "
                                    "FROM canonical_corporate_action_current c "
                                    "JOIN canonical_corporate_action_revisions r "
                                    "ON r.canonical_event_id = c.canonical_event_id "
                                    "AND r.projection_revision_id = c.projection_revision_id "
                                    f"WHERE c.canonical_event_id IN ({placeholders})",
                                    event_chunk,
                                ).fetchall()
                            }
                        )
        finally:
            connection_context.__exit__(None, None, None)
        inserted = unchanged = 0
        blocked_rows = [row for row in projected if not row["backtest_ready"]]
        blocker_counts = Counter(
            reason
            for row in blocked_rows
            for reason in row.get("blocking_reasons", [])
        )
        would_change = 0
        if dry_run:
            for row in projected:
                key = (row["canonical_event_id"], row["projection_revision_id"])
                existing_hash = existing_revisions.get(key)
                if existing_hash is not None and existing_hash != row["input_hash"]:
                    raise ValueError(
                        "immutable canonical projection revision has different content"
                    )
                if existing_hash == row["input_hash"] or current_hashes.get(
                    row["canonical_event_id"]
                ) == row["input_hash"]:
                    unchanged += 1
                else:
                    would_change += 1
        elif projected:
            batch_result = self.store.append_canonical_actions(
                projected, batch_commit=batch_commit
            )
            inserted = int(batch_result["inserted"])
            unchanged = int(batch_result["unchanged"])
        return self._report(
            considered=len(projected),
            inserted=inserted,
            unchanged=unchanged,
            ready=len(projected) - len(blocked_rows),
            blocked=len(blocked_rows),
            blocker_reasons=dict(sorted(blocker_counts.items())),
            blockers=[],
            dry_run=dry_run,
            would_change=would_change,
        )

    @staticmethod
    def _source_item(
        observation: Mapping[str, Any], projection: Mapping[str, Any]
    ) -> dict[str, Any]:
        return {
            "observation_id": int(observation["id"]),
            "instrument_id": str(observation["instrument_id"]),
            "source_event_key": str(observation["source_event_key"]),
            "row_hash": observation.get("selected_row_hash", observation.get("row_hash")),
            "projection_input_hash": projection["input_hash"],
        }

    def _report(
        self,
        considered: int,
        inserted: int,
        unchanged: int,
        ready: int,
        blocked: int,
        blocker_reasons: Mapping[str, int],
        blockers: list[str],
        dry_run: bool,
        would_change: int,
    ) -> dict[str, Any]:
        status = (
            "dry_run"
            if dry_run
            else "unavailable"
            if blockers
            else "degraded"
            if blocked
            else "success"
        )
        return {
            "stage": "canonical_corporate_action_projection",
            "status": status,
            "reuse_decision": "reuse",
            "provider_usage": [],
            "network_requests": 0,
            "dry_run": dry_run,
            "considered": considered,
            "ready": ready,
            "inserted": inserted,
            "unchanged": unchanged,
            "would_change": would_change if dry_run else 0,
            "blocked": blocked,
            "blocker_reasons": dict(blocker_reasons),
            "blockers": blockers,
            "database_id": "quotes",
            "watermark": self.store.readiness().get("latest_watermark") if not dry_run else None,
        }

    def _build_projection(
        self, connection: sqlite3.Connection, observation: Mapping[str, Any]
    ) -> dict[str, Any]:
        instrument_id = str(observation["instrument_id"])
        source_event_key = str(observation["source_event_key"])
        state = self._fetch_optional(
            connection,
            "corporate_action_resolution_states",
            instrument_id=instrument_id,
            source_event_key=source_event_key,
        )
        terms = self._fetch_optional(
            connection,
            "corporate_action_resolved_terms",
            instrument_id=instrument_id,
            source_event_key=source_event_key,
            extra_where="AND is_active = 1",
        )
        evidence = self._fetch_optional(
            connection,
            "corporate_action_effective_date_evidence",
            instrument_id=instrument_id,
            source_event_key=source_event_key,
        )
        coverage = self._coverage(connection, instrument_id, str(observation.get("source") or ""))
        action_type = str(observation.get("action_type") or "unknown")
        factor_effect = action_type in FACTOR_ACTION_TYPES
        accepted_terms = terms or observation
        effective_date = _date(
            (state or {}).get("resolved_effective_date")
            or (evidence or {}).get("effective_date")
            or observation.get("ex_date")
        )
        blockers: list[str] = []
        event_status = str(observation.get("event_status") or "unvalidated").lower()
        quality_status = str(observation.get("quality_status") or "unvalidated").lower()
        if event_status in {"cancelled", "rejected", "retired"} or not bool(observation.get("is_current", 1)):
            blockers.append("lifecycle_not_applicable")
        if event_status not in {"accepted", "confirmed", "effective", "implemented", "completed"}:
            blockers.append("event_not_accepted")
        if quality_status in {"conflict", "unresolved", "unvalidated", "rejected"}:
            blockers.append("quality_not_accepted")
        if state and bool(state.get("factor_blocking")):
            blockers.append(str(state.get("state_reason") or "factor_governance_blocked"))
        if factor_effect and not effective_date:
            blockers.append("effective_date_missing")
        if factor_effect and not self._has_required_terms(action_type, accepted_terms):
            blockers.append("economic_terms_missing")
        if evidence and str(evidence.get("resolution_status") or "").lower() in {"conflict", "unresolved", "rejected"}:
            blockers.append("effective_date_conflict")
        coverage_state = str((coverage or {}).get("coverage_status") or "unknown")
        if coverage_state in {"partial", "failed", "unknown"}:
            blockers.append("acquisition_coverage_incomplete")
        blockers = sorted(set(blockers))
        source_lineage = {
            "observation": {
                "id": observation.get("id"),
                "row_hash": observation.get("row_hash"),
                "source": observation.get("source"),
                "source_profile": observation.get("source_profile"),
            },
            "resolved_terms_id": (terms or {}).get("id"),
            "resolution_state_id": (state or {}).get("id"),
            "effective_date_evidence_id": (evidence or {}).get("id"),
            "coverage_status_id": (coverage or {}).get("id"),
        }
        decision_available_at = _latest_time(
            observation.get("updated_at"),
            (terms or {}).get("updated_at"),
            (state or {}).get("updated_at"),
            (evidence or {}).get("updated_at"),
            (coverage or {}).get("updated_at"),
        )
        stable_payload = {
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "action_type": action_type,
            "announcement_date": _date(observation.get("announcement_date")),
            "record_date": _date(observation.get("record_date")),
            "effective_date": effective_date,
            "payment_date": _date(observation.get("pay_date")),
            "share_arrival_date": _date(observation.get("share_arrival_date")),
            "cash_dividend_per_share": accepted_terms.get("cash_dividend_per_share"),
            "bonus_shares_per_share": accepted_terms.get("bonus_shares_per_share"),
            "capitalization_shares_per_share": accepted_terms.get("capitalization_shares_per_share"),
            "rights_shares_per_share": accepted_terms.get("rights_shares_per_share"),
            "rights_price": accepted_terms.get("rights_price"),
            "currency": accepted_terms.get("currency") or observation.get("currency"),
            "factor_effect": factor_effect,
            "backtest_ready": not blockers,
            "lifecycle_applicability": "applicable" if "lifecycle_not_applicable" not in blockers else "not_applicable",
            "coverage_state": coverage_state,
            "quality_state": "accepted" if not blockers else "blocked",
            "blocking_reasons": blockers,
            "source_lineage": source_lineage,
            "projection_version": self.projection_version,
            "decision_available_at": decision_available_at,
        }
        input_hash = semantic_hash(stable_payload)
        event_material = f"{instrument_id}|{source_event_key}".encode("utf-8")
        canonical_event_id = "ca_" + hashlib.sha256(event_material).hexdigest()[:24]
        return {
            "canonical_event_id": canonical_event_id,
            "projection_revision_id": "capr_" + input_hash[:24],
            **stable_payload,
            "input_hash": input_hash,
        }

    def _coverage(
        self, connection: sqlite3.Connection, instrument_id: str, source: str
    ) -> Optional[dict[str, Any]]:
        if not self._table_exists(connection, "corporate_action_instrument_status"):
            return None
        row = connection.execute(
            "SELECT * FROM corporate_action_instrument_status WHERE instrument_id = ? "
            "AND source = ? ORDER BY updated_at DESC, id DESC LIMIT 1",
            (instrument_id, source),
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _has_required_terms(action_type: str, row: Mapping[str, Any]) -> bool:
        if action_type in {"cash_dividend", "dividend"}:
            return row.get("cash_dividend_per_share") is not None
        if action_type == "bonus_shares":
            return row.get("bonus_shares_per_share") is not None
        if action_type == "capitalization":
            return row.get("capitalization_shares_per_share") is not None
        if action_type == "rights_issue":
            return row.get("rights_shares_per_share") is not None and row.get("rights_price") is not None
        if action_type == "mixed":
            return any(
                row.get(name) is not None
                for name in (
                    "cash_dividend_per_share",
                    "bonus_shares_per_share",
                    "capitalization_shares_per_share",
                    "rights_shares_per_share",
                )
            )
        return True
