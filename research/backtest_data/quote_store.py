"""SQLite storage and point-in-time reads for quote-domain backtest data."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import base64
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from research.change_watermarks import append_change_record, ensure_change_log_schema
from research.temporal_data_availability import require_aware
from utils.date_utils import get_shanghai_time


QUOTE_BACKTEST_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS backtest_resource_probe_runs (
    probe_id TEXT PRIMARY KEY,
    catalog_version TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    no_write INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_composition_snapshots (
    snapshot_id TEXT NOT NULL PRIMARY KEY,
    revision_id TEXT NOT NULL,
    index_instrument_id TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    reference_date TEXT,
    published_at TEXT,
    available_at TEXT,
    availability_quality TEXT,
    source TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    artifact_hash TEXT,
    weight_unit TEXT,
    completeness_state TEXT NOT NULL,
    validity_basis TEXT,
    semantic_hash TEXT NOT NULL,
    ingestion_run_id TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(index_instrument_id, revision_id),
    UNIQUE(index_instrument_id, effective_date, source, artifact_hash)
);

CREATE INDEX IF NOT EXISTS idx_index_snapshot_lookup
ON index_composition_snapshots(index_instrument_id, effective_date, available_at);

CREATE TABLE IF NOT EXISTS index_composition_members (
    snapshot_id TEXT NOT NULL,
    member_row_id TEXT NOT NULL,
    constituent_instrument_id TEXT,
    source_symbol TEXT NOT NULL,
    weight REAL,
    inclusion_metadata_json TEXT NOT NULL DEFAULT '{}',
    quality_json TEXT NOT NULL DEFAULT '{}',
    row_hash TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, member_row_id),
    FOREIGN KEY(snapshot_id) REFERENCES index_composition_snapshots(snapshot_id)
);

CREATE TABLE IF NOT EXISTS index_composition_validity_revisions (
    validity_revision_id TEXT PRIMARY KEY,
    index_instrument_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to_exclusive TEXT,
    decision_available_at TEXT,
    availability_quality TEXT,
    basis TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    input_snapshot_ids_json TEXT NOT NULL DEFAULT '[]',
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(snapshot_id) REFERENCES index_composition_snapshots(snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_index_validity_lookup
ON index_composition_validity_revisions(index_instrument_id, valid_from, decision_available_at);

CREATE TABLE IF NOT EXISTS security_state_current_observations (
    instrument_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    state TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    source TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    semantic_hash TEXT NOT NULL,
    quality TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS security_state_events (
    event_id TEXT PRIMARY KEY,
    instrument_id TEXT NOT NULL,
    symbol TEXT,
    exchange TEXT,
    event_type TEXT NOT NULL,
    prior_state TEXT,
    new_state TEXT,
    effective_date TEXT,
    published_at TEXT,
    available_at TEXT,
    availability_quality TEXT,
    source TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    artifact_hash TEXT,
    quality TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_event_lookup
ON security_state_events(instrument_id, effective_date, available_at);

CREATE TABLE IF NOT EXISTS security_state_interval_revisions (
    interval_revision_id TEXT PRIMARY KEY,
    interval_key TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    state TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to_exclusive TEXT,
    decision_available_at TEXT,
    availability_quality TEXT,
    confidence TEXT NOT NULL,
    status TEXT NOT NULL,
    input_event_ids_json TEXT NOT NULL DEFAULT '[]',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_interval_lookup
ON security_state_interval_revisions(instrument_id, valid_from, decision_available_at);

CREATE TABLE IF NOT EXISTS daily_price_limit_revisions (
    revision_id TEXT PRIMARY KEY,
    instrument_id TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    limit_up REAL,
    limit_down REAL,
    reference_price REAL,
    source_mode TEXT NOT NULL,
    source TEXT NOT NULL,
    source_profile TEXT NOT NULL,
    rule_version TEXT,
    decision_available_at TEXT,
    availability_quality TEXT,
    inputs_json TEXT NOT NULL DEFAULT '{}',
    quality TEXT NOT NULL,
    semantic_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(instrument_id, trade_date, semantic_hash)
);

CREATE INDEX IF NOT EXISTS idx_price_limit_lookup
ON daily_price_limit_revisions(instrument_id, trade_date, decision_available_at);

CREATE TABLE IF NOT EXISTS canonical_corporate_action_revisions (
    canonical_event_id TEXT NOT NULL,
    projection_revision_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    source_event_key TEXT,
    action_type TEXT NOT NULL,
    announcement_date TEXT,
    record_date TEXT,
    effective_date TEXT,
    payment_date TEXT,
    share_arrival_date TEXT,
    cash_dividend_per_share REAL,
    bonus_shares_per_share REAL,
    capitalization_shares_per_share REAL,
    rights_shares_per_share REAL,
    rights_price REAL,
    currency TEXT,
    factor_effect INTEGER NOT NULL DEFAULT 0,
    backtest_ready INTEGER NOT NULL DEFAULT 0,
    lifecycle_applicability TEXT NOT NULL,
    coverage_state TEXT NOT NULL,
    quality_state TEXT NOT NULL,
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    source_lineage_json TEXT NOT NULL DEFAULT '{}',
    input_hash TEXT NOT NULL,
    projection_version TEXT NOT NULL,
    decision_available_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(canonical_event_id, projection_revision_id)
);

CREATE INDEX IF NOT EXISTS idx_canonical_action_lookup
ON canonical_corporate_action_revisions(instrument_id, effective_date, decision_available_at);

CREATE TABLE IF NOT EXISTS canonical_corporate_action_current (
    canonical_event_id TEXT PRIMARY KEY,
    projection_revision_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def semantic_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(dict(value)).encode("utf-8")).hexdigest()


def _encode_cursor(*, database_id: str, domain: str, sequence: int) -> str:
    payload = _json(
        {"database_id": database_id, "domain": domain, "sequence": int(sequence)}
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(value: str, *, database_id: str, domain: str) -> int:
    try:
        padded = value + "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
        if payload.get("database_id") != database_id or payload.get("domain") != domain:
            raise ValueError("cursor scope does not match this database and domain")
        sequence = int(payload["sequence"])
        if sequence < 0:
            raise ValueError
        return sequence
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("invalid change cursor") from exc


def _aware_iso(value: Any, *, field_name: str, required: bool = False) -> Optional[str]:
    if value is None or value == "":
        if required:
            raise ValueError(f"{field_name} is required")
        return None
    if isinstance(value, datetime):
        require_aware(value, field_name=field_name)
        return value.isoformat()
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    require_aware(parsed, field_name=field_name)
    return parsed.isoformat()


def _date_text(value: Any, *, field_name: str, required: bool = False) -> Optional[str]:
    if value is None or value == "":
        if required:
            raise ValueError(f"{field_name} is required")
        return None
    text = str(value)[:10]
    if len(text) != 10:
        raise ValueError(f"{field_name} must use YYYY-MM-DD")
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD") from exc
    return text


class BacktestQuoteStore:
    """Own additive quote-domain backtest tables and fail-closed PIT reads."""

    database_id = "quotes"

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connection() as connection:
            connection.executescript(QUOTE_BACKTEST_SCHEMA_SQL)
            interval_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(security_state_interval_revisions)"
                ).fetchall()
            }
            if "interval_key" not in interval_columns:
                connection.execute(
                    "ALTER TABLE security_state_interval_revisions "
                    "ADD COLUMN interval_key TEXT NOT NULL DEFAULT ''"
                )
                connection.execute(
                    "UPDATE security_state_interval_revisions SET interval_key = "
                    "instrument_id || ':' || valid_from WHERE interval_key = ''"
                )
            ensure_change_log_schema(connection)
            connection.commit()

    def record_probe(self, *, probe_id: str, catalog_version: str, scope: Mapping[str, Any], result: Mapping[str, Any]) -> None:
        if not result.get("no_write", False):
            raise ValueError("only no-write probe results can be recorded")
        now = get_shanghai_time().isoformat()
        with self.connection() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO backtest_resource_probe_runs "
                "(probe_id, catalog_version, scope_json, result_json, no_write, created_at) VALUES (?, ?, ?, ?, 1, ?)",
                (probe_id, catalog_version, _json(scope), _json(result), now),
            )
            connection.commit()

    def _append_change(
        self,
        connection: sqlite3.Connection,
        *,
        dataset: str,
        change_type: str,
        business_key: Mapping[str, Any],
        new_hash: Optional[str],
        instrument_id: Optional[str] = None,
        observation_date: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        source_profile: Optional[str] = None,
    ) -> None:
        append_change_record(
            connection,
            config=None,
            domain="backtest",
            dataset=dataset,
            change_type=change_type,
            business_key=business_key,
            changed_at=get_shanghai_time().isoformat(),
            instrument_id=instrument_id,
            observation_date=observation_date,
            new_hash=new_hash,
            source=source,
            source_mode=source_mode,
            source_profile=source_profile,
        )

    def upsert_index_snapshot(
        self,
        *,
        snapshot: Mapping[str, Any],
        members: Sequence[Mapping[str, Any]],
        validity: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        snapshot_id = str(snapshot.get("snapshot_id") or "").strip()
        if not snapshot_id:
            raise ValueError("snapshot_id is required")
        effective = _date_text(snapshot.get("effective_date"), field_name="effective_date", required=True)
        available = _aware_iso(snapshot.get("available_at"), field_name="available_at")
        if snapshot.get("published_at") is not None:
            _aware_iso(snapshot.get("published_at"), field_name="published_at")
        members_payload = []
        for index, item in enumerate(members):
            source_symbol = str(item.get("source_symbol") or item.get("symbol") or "").strip()
            if not source_symbol:
                raise ValueError("index constituent source_symbol is required")
            row = {
                "member_row_id": str(item.get("member_row_id") or f"{source_symbol}:{index}"),
                "constituent_instrument_id": item.get("constituent_instrument_id"),
                "source_symbol": source_symbol,
                "weight": item.get("weight"),
                "inclusion_metadata": item.get("inclusion_metadata") or {},
                "quality": item.get("quality") or {},
            }
            row["row_hash"] = semantic_hash(row)
            members_payload.append(row)
        now = get_shanghai_time().isoformat()
        normalized = {
            "snapshot_id": snapshot_id,
            "revision_id": str(snapshot.get("revision_id") or snapshot_id),
            "index_instrument_id": str(snapshot.get("index_instrument_id") or ""),
            "effective_date": effective,
            "reference_date": _date_text(snapshot.get("reference_date"), field_name="reference_date"),
            "published_at": _aware_iso(snapshot.get("published_at"), field_name="published_at"),
            "available_at": available,
            "availability_quality": snapshot.get("availability_quality"),
            "source": str(snapshot.get("source") or "unknown"),
            "source_profile": str(snapshot.get("source_profile") or "unknown"),
            "artifact_hash": snapshot.get("artifact_hash"),
            "weight_unit": snapshot.get("weight_unit"),
            "completeness_state": str(snapshot.get("completeness_state") or "partial"),
            "validity_basis": snapshot.get("validity_basis"),
            "ingestion_run_id": snapshot.get("ingestion_run_id"),
        }
        if not normalized["index_instrument_id"]:
            raise ValueError("index_instrument_id is required")
        normalized["semantic_hash"] = semantic_hash({**normalized, "members": members_payload})
        with self.connection() as connection:
            existing = connection.execute(
                "SELECT semantic_hash FROM index_composition_snapshots WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if existing and existing["semantic_hash"] == normalized["semantic_hash"]:
                return {"status": "unchanged", "snapshot_id": snapshot_id, "member_count": len(members_payload)}
            if existing:
                raise ValueError("immutable index snapshot identity already has different content")
            connection.execute(
                "INSERT INTO index_composition_snapshots "
                "(snapshot_id, revision_id, index_instrument_id, effective_date, reference_date, published_at, available_at, availability_quality, source, source_profile, artifact_hash, weight_unit, completeness_state, validity_basis, semantic_hash, ingestion_run_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    normalized["snapshot_id"], normalized["revision_id"], normalized["index_instrument_id"],
                    normalized["effective_date"], normalized["reference_date"], normalized["published_at"],
                    normalized["available_at"], normalized["availability_quality"], normalized["source"],
                    normalized["source_profile"], normalized["artifact_hash"], normalized["weight_unit"],
                    normalized["completeness_state"], normalized["validity_basis"], normalized["semantic_hash"],
                    normalized["ingestion_run_id"], now,
                ),
            )
            for row in members_payload:
                connection.execute(
                    "INSERT INTO index_composition_members (snapshot_id, member_row_id, constituent_instrument_id, source_symbol, weight, inclusion_metadata_json, quality_json, row_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        snapshot_id, row["member_row_id"], row["constituent_instrument_id"], row["source_symbol"],
                        row["weight"], _json(row["inclusion_metadata"]), _json(row["quality"]), row["row_hash"],
                    ),
                )
            if validity is not None:
                self._insert_index_validity(connection, normalized, validity)
            self._append_change(
                connection,
                dataset="index_composition",
                change_type="insert",
                business_key={"snapshot_id": snapshot_id, "revision_id": normalized["revision_id"]},
                new_hash=normalized["semantic_hash"],
                source=normalized["source"],
                source_mode="snapshot",
                source_profile=normalized["source_profile"],
            )
            connection.commit()
        return {"status": "inserted", "snapshot_id": snapshot_id, "member_count": len(members_payload)}

    def _insert_index_validity(
        self,
        connection: sqlite3.Connection,
        snapshot: Mapping[str, Any],
        validity: Mapping[str, Any],
    ) -> str:
        revision_id = str(validity.get("validity_revision_id") or "").strip()
        if not revision_id:
            raise ValueError("validity_revision_id is required")
        payload = {
            "validity_revision_id": revision_id,
            "index_instrument_id": snapshot["index_instrument_id"],
            "snapshot_id": snapshot["snapshot_id"],
            "valid_from": _date_text(validity.get("valid_from"), field_name="valid_from", required=True),
            "valid_to_exclusive": _date_text(validity.get("valid_to_exclusive"), field_name="valid_to_exclusive"),
            "decision_available_at": _aware_iso(validity.get("decision_available_at"), field_name="decision_available_at"),
            "availability_quality": validity.get("availability_quality"),
            "basis": str(validity.get("basis") or "unknown"),
            "evidence": validity.get("evidence") or {},
            "input_snapshot_ids": validity.get("input_snapshot_ids") or [snapshot["snapshot_id"]],
        }
        payload["semantic_hash"] = semantic_hash(payload)
        existing = connection.execute(
            "SELECT semantic_hash FROM index_composition_validity_revisions "
            "WHERE validity_revision_id = ?",
            (revision_id,),
        ).fetchone()
        if existing:
            if existing["semantic_hash"] != payload["semantic_hash"]:
                raise ValueError("immutable index validity revision has different content")
            return revision_id
        connection.execute(
            "INSERT INTO index_composition_validity_revisions "
            "(validity_revision_id, index_instrument_id, snapshot_id, valid_from, valid_to_exclusive, decision_available_at, availability_quality, basis, evidence_json, input_snapshot_ids_json, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                revision_id, payload["index_instrument_id"], payload["snapshot_id"], payload["valid_from"],
                payload["valid_to_exclusive"], payload["decision_available_at"], payload["availability_quality"],
                payload["basis"], _json(payload["evidence"]), _json(payload["input_snapshot_ids"]),
                payload["semantic_hash"], get_shanghai_time().isoformat(),
            ),
        )
        return revision_id

    def append_index_validity(
        self, *, snapshot_id: str, validity: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Append later-learned validity evidence without mutating a snapshot."""
        with self.connection() as connection:
            snapshot = connection.execute(
                "SELECT * FROM index_composition_snapshots WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if snapshot is None:
                raise ValueError("unknown index snapshot_id")
            revision_id = str(validity.get("validity_revision_id") or "").strip()
            existing = connection.execute(
                "SELECT semantic_hash FROM index_composition_validity_revisions "
                "WHERE validity_revision_id = ?",
                (revision_id,),
            ).fetchone()
            self._insert_index_validity(connection, dict(snapshot), validity)
            if existing:
                return {"status": "unchanged", "validity_revision_id": revision_id}
            row = connection.execute(
                "SELECT semantic_hash FROM index_composition_validity_revisions "
                "WHERE validity_revision_id = ?",
                (revision_id,),
            ).fetchone()
            self._append_change(
                connection,
                dataset="index_composition_validity",
                change_type="insert",
                business_key={"validity_revision_id": revision_id, "snapshot_id": snapshot_id},
                new_hash=row["semantic_hash"],
                observation_date=str(validity.get("valid_from") or "")[:10] or None,
            )
            connection.commit()
        return {"status": "inserted", "validity_revision_id": revision_id}

    def list_index_constituents(
        self,
        index_instrument_id: str,
        *,
        as_of_date: str,
        known_at: Optional[str] = None,
        strict: bool = True,
        limit: int = 1000,
        offset: int = 0,
    ) -> dict[str, Any]:
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        as_of = _date_text(as_of_date, field_name="as_of_date", required=True)
        cutoff = _aware_iso(known_at, field_name="known_at") if known_at else None
        if strict and cutoff is None:
            return {
                "status": "unavailable",
                "reason": "known_at_required_for_strict_pit",
                "items": [],
                "total": 0,
                "as_of_date": as_of,
                "strict": strict,
            }
        with self.connection() as connection:
            where = ["index_instrument_id = ?", "effective_date <= ?", "completeness_state = 'complete'"]
            params: list[Any] = [index_instrument_id, as_of]
            if cutoff:
                where.append("available_at IS NOT NULL AND available_at <= ?")
                params.append(cutoff)
            snapshot_rows = connection.execute(
                "SELECT * FROM index_composition_snapshots WHERE " + " AND ".join(where) +
                " ORDER BY effective_date DESC, available_at DESC, snapshot_id DESC",
                params,
            ).fetchall()
            if not snapshot_rows:
                return {"status": "unavailable", "reason": "no_complete_snapshot_available", "items": [], "total": 0}
            snapshot = None
            validity = None
            for candidate in snapshot_rows:
                validity_where = ["snapshot_id = ?", "valid_from <= ?", "(valid_to_exclusive IS NULL OR valid_to_exclusive > ?)"]
                validity_params: list[Any] = [candidate["snapshot_id"], as_of, as_of]
                if cutoff:
                    validity_where.append("decision_available_at IS NOT NULL AND decision_available_at <= ?")
                    validity_params.append(cutoff)
                candidate_validity = connection.execute(
                    "SELECT * FROM index_composition_validity_revisions WHERE " + " AND ".join(validity_where) +
                    " ORDER BY decision_available_at DESC, validity_revision_id DESC LIMIT 1",
                    validity_params,
                ).fetchone()
                if candidate_validity is not None or not strict:
                    snapshot = dict(candidate)
                    validity = candidate_validity
                    break
            if snapshot is None:
                return {"status": "unavailable", "reason": "validity_evidence_missing", "items": [], "total": 0}
            count = connection.execute(
                "SELECT COUNT(*) AS count FROM index_composition_members WHERE snapshot_id = ?",
                (snapshot["snapshot_id"],),
            ).fetchone()["count"]
            members = connection.execute(
                "SELECT * FROM index_composition_members WHERE snapshot_id = ? ORDER BY source_symbol, member_row_id LIMIT ? OFFSET ?",
                (snapshot["snapshot_id"], int(limit), max(int(offset), 0)),
            ).fetchall()
        return {
            "status": "success",
            "as_of_date": as_of,
            "known_at": cutoff,
            "snapshot": snapshot,
            "validity": dict(validity) if validity else None,
            "items": [dict(row) for row in members],
            "total": int(count),
            "limit": int(limit),
            "offset": max(int(offset), 0),
            "strict": strict,
        }

    def append_security_event(self, event: Mapping[str, Any]) -> dict[str, Any]:
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            raise ValueError("event_id is required")
        payload = {
            "event_id": event_id,
            "instrument_id": str(event.get("instrument_id") or ""),
            "symbol": event.get("symbol"),
            "exchange": event.get("exchange"),
            "event_type": str(event.get("event_type") or ""),
            "prior_state": event.get("prior_state"),
            "new_state": event.get("new_state"),
            "effective_date": _date_text(event.get("effective_date"), field_name="effective_date"),
            "published_at": _aware_iso(event.get("published_at"), field_name="published_at"),
            "available_at": _aware_iso(event.get("available_at"), field_name="available_at"),
            "availability_quality": event.get("availability_quality"),
            "source": str(event.get("source") or "unknown"),
            "source_profile": str(event.get("source_profile") or "unknown"),
            "artifact_hash": event.get("artifact_hash"),
            "quality": str(event.get("quality") or "unresolved"),
            "evidence": event.get("evidence") or {},
        }
        if not payload["instrument_id"] or not payload["event_type"]:
            raise ValueError("instrument_id and event_type are required")
        payload["semantic_hash"] = semantic_hash(payload)
        with self.connection() as connection:
            existing = connection.execute("SELECT semantic_hash FROM security_state_events WHERE event_id = ?", (event_id,)).fetchone()
            if existing and existing["semantic_hash"] == payload["semantic_hash"]:
                return {"status": "unchanged", "event_id": event_id}
            if existing:
                raise ValueError("security event identity is immutable")
            connection.execute(
                "INSERT INTO security_state_events (event_id, instrument_id, symbol, exchange, event_type, prior_state, new_state, effective_date, published_at, available_at, availability_quality, source, source_profile, artifact_hash, quality, evidence_json, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    payload["event_id"], payload["instrument_id"], payload["symbol"], payload["exchange"], payload["event_type"],
                    payload["prior_state"], payload["new_state"], payload["effective_date"], payload["published_at"], payload["available_at"],
                    payload["availability_quality"], payload["source"], payload["source_profile"], payload["artifact_hash"], payload["quality"],
                    _json(payload["evidence"]), payload["semantic_hash"], get_shanghai_time().isoformat(),
                ),
            )
            self._append_change(
                connection,
                dataset="security_state_events",
                change_type="insert",
                business_key={"event_id": event_id},
                new_hash=payload["semantic_hash"],
                instrument_id=payload["instrument_id"],
                observation_date=payload["effective_date"],
                source=payload["source"],
                source_mode="event",
                source_profile=payload["source_profile"],
            )
            connection.commit()
        return {"status": "inserted", "event_id": event_id}

    def append_security_interval(self, interval: Mapping[str, Any]) -> dict[str, Any]:
        revision_id = str(interval.get("interval_revision_id") or "").strip()
        if not revision_id:
            raise ValueError("interval_revision_id is required")
        payload = {
            "interval_revision_id": revision_id,
            "interval_key": str(
                interval.get("interval_key")
                or ":".join(
                    str(item)
                    for item in (interval.get("input_event_ids") or [])
                )
                or f"{interval.get('instrument_id')}:{str(interval.get('valid_from'))[:10]}"
            ),
            "instrument_id": str(interval.get("instrument_id") or ""),
            "state": str(interval.get("state") or "unknown"),
            "valid_from": _date_text(interval.get("valid_from"), field_name="valid_from", required=True),
            "valid_to_exclusive": _date_text(interval.get("valid_to_exclusive"), field_name="valid_to_exclusive"),
            "decision_available_at": _aware_iso(interval.get("decision_available_at"), field_name="decision_available_at"),
            "availability_quality": interval.get("availability_quality"),
            "confidence": str(interval.get("confidence") or "unknown"),
            "status": str(interval.get("status") or "unresolved"),
            "input_event_ids": interval.get("input_event_ids") or [],
            "evidence": interval.get("evidence") or {},
        }
        if not payload["instrument_id"]:
            raise ValueError("instrument_id is required")
        payload["semantic_hash"] = semantic_hash(payload)
        with self.connection() as connection:
            existing = connection.execute(
                "SELECT semantic_hash FROM security_state_interval_revisions "
                "WHERE interval_revision_id = ?",
                (revision_id,),
            ).fetchone()
            if existing:
                if existing["semantic_hash"] != payload["semantic_hash"]:
                    raise ValueError("immutable security interval revision has different content")
                return {"status": "unchanged", "interval_revision_id": revision_id}
            connection.execute(
                "INSERT INTO security_state_interval_revisions (interval_revision_id, interval_key, instrument_id, state, valid_from, valid_to_exclusive, decision_available_at, availability_quality, confidence, status, input_event_ids_json, evidence_json, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    revision_id, payload["interval_key"], payload["instrument_id"], payload["state"], payload["valid_from"], payload["valid_to_exclusive"],
                    payload["decision_available_at"], payload["availability_quality"], payload["confidence"], payload["status"],
                    _json(payload["input_event_ids"]), _json(payload["evidence"]), payload["semantic_hash"], get_shanghai_time().isoformat(),
                ),
            )
            self._append_change(
                connection,
                dataset="security_state_intervals",
                change_type="insert",
                business_key={"interval_revision_id": revision_id},
                new_hash=payload["semantic_hash"],
                instrument_id=payload["instrument_id"],
                observation_date=payload["valid_from"],
            )
            connection.commit()
        return {"status": "inserted", "interval_revision_id": revision_id}

    def upsert_current_security_observation(self, observation: Mapping[str, Any]) -> dict[str, Any]:
        instrument_id = str(observation.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError("instrument_id is required")
        observed_at = _aware_iso(observation.get("observed_at"), field_name="observed_at", required=True)
        available_at = _aware_iso(observation.get("available_at") or observed_at, field_name="available_at", required=True)
        payload = {
            "instrument_id": instrument_id,
            "symbol": str(observation.get("symbol") or ""),
            "exchange": str(observation.get("exchange") or ""),
            "state": str(observation.get("state") or "unknown"),
            "observed_at": observed_at,
            "available_at": available_at,
            "source": str(observation.get("source") or "instrument_master"),
            "source_profile": str(observation.get("source_profile") or "unknown"),
            "quality": str(observation.get("quality") or "observed_transition"),
        }
        payload["semantic_hash"] = semantic_hash(
            {
                key: value
                for key, value in payload.items()
                if key not in {"observed_at", "available_at"}
            }
        )
        with self.connection() as connection:
            old = connection.execute("SELECT * FROM security_state_current_observations WHERE instrument_id = ?", (instrument_id,)).fetchone()
            if old and old["semantic_hash"] == payload["semantic_hash"]:
                return {"status": "unchanged", "instrument_id": instrument_id, "transition": None}
            connection.execute(
                "INSERT OR REPLACE INTO security_state_current_observations (instrument_id, symbol, exchange, state, observed_at, available_at, source, source_profile, semantic_hash, quality) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    instrument_id, payload["symbol"], payload["exchange"], payload["state"], payload["observed_at"], payload["available_at"],
                    payload["source"], payload["source_profile"], payload["semantic_hash"], payload["quality"],
                ),
            )
            transition = None
            if old and old["state"] != payload["state"]:
                transition = {"prior_state": old["state"], "new_state": payload["state"]}
            connection.commit()
        return {"status": "updated" if old else "inserted", "instrument_id": instrument_id, "transition": transition}

    def append_price_limit(self, row: Mapping[str, Any]) -> dict[str, Any]:
        revision_id = str(row.get("revision_id") or "").strip()
        if not revision_id:
            raise ValueError("revision_id is required")
        payload = {
            "revision_id": revision_id,
            "instrument_id": str(row.get("instrument_id") or ""),
            "trade_date": _date_text(row.get("trade_date"), field_name="trade_date", required=True),
            "limit_up": row.get("limit_up"),
            "limit_down": row.get("limit_down"),
            "reference_price": row.get("reference_price"),
            "source_mode": str(row.get("source_mode") or "unknown"),
            "source": str(row.get("source") or "unknown"),
            "source_profile": str(row.get("source_profile") or "unknown"),
            "rule_version": row.get("rule_version"),
            "decision_available_at": _aware_iso(row.get("decision_available_at"), field_name="decision_available_at"),
            "availability_quality": row.get("availability_quality"),
            "inputs": row.get("inputs") or {},
            "quality": str(row.get("quality") or "unresolved"),
        }
        if not payload["instrument_id"]:
            raise ValueError("instrument_id is required")
        if payload["source_mode"] == "derived_rule" and payload["reference_price"] is None:
            raise ValueError("derived price limits require governed reference_price")
        if payload["source_mode"] == "derived_rule":
            reference_basis = str(payload["inputs"].get("reference_price_basis") or "")
            if reference_basis in {"", "raw_prior_close", "unadjusted_prior_close"}:
                raise ValueError(
                    "derived price limits require governed reference_price_basis; "
                    "raw prior close is not accepted"
                )
        payload["semantic_hash"] = semantic_hash(payload)
        with self.connection() as connection:
            existing = connection.execute("SELECT semantic_hash FROM daily_price_limit_revisions WHERE revision_id = ?", (revision_id,)).fetchone()
            if existing:
                if existing["semantic_hash"] != payload["semantic_hash"]:
                    raise ValueError("immutable price-limit revision has different content")
                return {"status": "unchanged", "revision_id": revision_id}
            connection.execute(
                "INSERT INTO daily_price_limit_revisions (revision_id, instrument_id, trade_date, limit_up, limit_down, reference_price, source_mode, source, source_profile, rule_version, decision_available_at, availability_quality, inputs_json, quality, semantic_hash, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    revision_id, payload["instrument_id"], payload["trade_date"], payload["limit_up"], payload["limit_down"], payload["reference_price"],
                    payload["source_mode"], payload["source"], payload["source_profile"], payload["rule_version"], payload["decision_available_at"],
                    payload["availability_quality"], _json(payload["inputs"]), payload["quality"], payload["semantic_hash"], get_shanghai_time().isoformat(),
                ),
            )
            self._append_change(
                connection,
                dataset="daily_price_limits",
                change_type="insert",
                business_key={"revision_id": revision_id, "instrument_id": payload["instrument_id"], "trade_date": payload["trade_date"]},
                new_hash=payload["semantic_hash"],
                instrument_id=payload["instrument_id"],
                observation_date=payload["trade_date"],
                source=payload["source"],
                source_mode=payload["source_mode"],
                source_profile=payload["source_profile"],
            )
            connection.commit()
        return {"status": "inserted", "revision_id": revision_id}

    def resolve_security_state(self, instrument_id: str, *, effective_date: str, known_at: str, strict: bool = True) -> dict[str, Any]:
        effective = _date_text(effective_date, field_name="effective_date", required=True)
        cutoff = _aware_iso(known_at, field_name="known_at", required=True)
        with self.connection() as connection:
            intervals = connection.execute(
                "WITH ranked AS ("
                " SELECT *, ROW_NUMBER() OVER ("
                "  PARTITION BY interval_key"
                "  ORDER BY decision_available_at DESC, interval_revision_id DESC"
                " ) AS revision_rank"
                " FROM security_state_interval_revisions"
                " WHERE instrument_id = ? AND valid_from <= ?"
                " AND (valid_to_exclusive IS NULL OR valid_to_exclusive > ?)"
                " AND decision_available_at IS NOT NULL AND decision_available_at <= ?"
                ") SELECT * FROM ranked WHERE revision_rank = 1"
                " ORDER BY valid_from DESC, decision_available_at DESC",
                (instrument_id, effective, effective, cutoff),
            ).fetchall()
            event = connection.execute(
                "SELECT * FROM security_state_events WHERE instrument_id = ? AND effective_date <= ? AND available_at IS NOT NULL AND available_at <= ? ORDER BY effective_date DESC, available_at DESC LIMIT 1",
                (instrument_id, effective, cutoff),
            ).fetchone()
            current = connection.execute(
                "SELECT * FROM security_state_current_observations "
                "WHERE instrument_id = ? AND substr(observed_at, 1, 10) <= ? "
                "AND available_at <= ? ORDER BY available_at DESC LIMIT 1",
                (instrument_id, effective, cutoff),
            ).fetchone()
        interval_rows = [dict(row) for row in intervals]
        blocking_intervals = [
            row for row in interval_rows if row["status"] in {"conflict", "unresolved"}
        ]
        distinct_states = {row["state"] for row in interval_rows if row["status"] == "confirmed"}
        if strict and (blocking_intervals or len(distinct_states) > 1):
            return {"status": "unavailable", "reason": "state_interval_conflict", "instrument_id": instrument_id, "effective_date": effective, "known_at": cutoff, "evidence": interval_rows}
        interval = interval_rows[0] if interval_rows else None
        event_row = dict(event) if event else None
        if strict and event_row and str(event_row.get("quality") or "").lower() in {
            "unresolved", "ambiguous", "conflict", "pending", "unvalidated"
        }:
            return {"status": "unavailable", "reason": "state_event_quality_blocked", "instrument_id": instrument_id, "effective_date": effective, "known_at": cutoff, "evidence": event_row}
        chosen = interval or event_row or (dict(current) if current else None)
        if chosen is None:
            return {"status": "unavailable", "reason": "state_interval_or_event_missing", "instrument_id": instrument_id, "effective_date": effective, "known_at": cutoff}
        return {"status": "success", "instrument_id": instrument_id, "effective_date": effective, "known_at": cutoff, "state": chosen.get("state") or chosen.get("new_state"), "evidence": chosen}

    def resolve_price_limit(self, instrument_id: str, *, trade_date: str, known_at: str, strict: bool = True) -> dict[str, Any]:
        trade = _date_text(trade_date, field_name="trade_date", required=True)
        cutoff = _aware_iso(known_at, field_name="known_at", required=True)
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM daily_price_limit_revisions WHERE instrument_id = ? AND trade_date = ? AND decision_available_at IS NOT NULL AND decision_available_at <= ? ORDER BY decision_available_at DESC, created_at DESC LIMIT 1",
                (instrument_id, trade, cutoff),
            ).fetchone()
        if row is None:
            return {"status": "unavailable", "reason": "price_limit_revision_missing", "instrument_id": instrument_id, "trade_date": trade, "known_at": cutoff}
        result = dict(row)
        if strict and result["quality"] in {"unresolved", "ambiguous", "partial"}:
            return {"status": "unavailable", "reason": "price_limit_quality_blocked", "evidence": result}
        return {"status": "success", "instrument_id": instrument_id, "trade_date": trade, "known_at": cutoff, "evidence": result}

    def append_canonical_action(self, row: Mapping[str, Any]) -> dict[str, Any]:
        event_id = str(row.get("canonical_event_id") or "").strip()
        revision_id = str(row.get("projection_revision_id") or "").strip()
        if not event_id or not revision_id:
            raise ValueError("canonical_event_id and projection_revision_id are required")
        payload = dict(row)
        payload["canonical_event_id"] = event_id
        payload["projection_revision_id"] = revision_id
        payload["instrument_id"] = str(row.get("instrument_id") or "")
        payload["action_type"] = str(row.get("action_type") or "unknown")
        payload["decision_available_at"] = _aware_iso(row.get("decision_available_at"), field_name="decision_available_at")
        payload["effective_date"] = _date_text(row.get("effective_date"), field_name="effective_date")
        payload["announcement_date"] = _date_text(row.get("announcement_date"), field_name="announcement_date")
        payload["record_date"] = _date_text(row.get("record_date"), field_name="record_date")
        payload["payment_date"] = _date_text(row.get("payment_date"), field_name="payment_date")
        payload["share_arrival_date"] = _date_text(row.get("share_arrival_date"), field_name="share_arrival_date")
        payload["factor_effect"] = bool(row.get("factor_effect", False))
        payload["backtest_ready"] = bool(row.get("backtest_ready", False))
        payload["lifecycle_applicability"] = str(row.get("lifecycle_applicability") or "unknown")
        payload["coverage_state"] = str(row.get("coverage_state") or "unknown")
        payload["quality_state"] = str(row.get("quality_state") or "unresolved")
        payload["blocking_reasons"] = list(row.get("blocking_reasons") or [])
        payload["source_lineage"] = row.get("source_lineage") or {}
        payload["input_hash"] = str(row.get("input_hash") or semantic_hash({key: value for key, value in payload.items() if key not in {"projection_revision_id", "created_at"}}))
        payload["projection_version"] = str(row.get("projection_version") or "canonical-corporate-action.v1")
        with self.connection() as connection:
            existing = connection.execute("SELECT input_hash FROM canonical_corporate_action_revisions WHERE canonical_event_id = ? AND projection_revision_id = ?", (event_id, revision_id)).fetchone()
            if existing:
                if existing["input_hash"] != payload["input_hash"]:
                    raise ValueError("immutable canonical projection revision has different content")
                return {"status": "unchanged", "canonical_event_id": event_id, "projection_revision_id": revision_id}
            current = connection.execute(
                "SELECT r.input_hash FROM canonical_corporate_action_current c "
                "JOIN canonical_corporate_action_revisions r "
                "ON r.canonical_event_id = c.canonical_event_id "
                "AND r.projection_revision_id = c.projection_revision_id "
                "WHERE c.canonical_event_id = ?",
                (event_id,),
            ).fetchone()
            if current and current["input_hash"] == payload["input_hash"]:
                return {"status": "unchanged", "canonical_event_id": event_id, "projection_revision_id": revision_id}
            connection.execute(
                "INSERT INTO canonical_corporate_action_revisions (canonical_event_id, projection_revision_id, instrument_id, source_event_key, action_type, announcement_date, record_date, effective_date, payment_date, share_arrival_date, cash_dividend_per_share, bonus_shares_per_share, capitalization_shares_per_share, rights_shares_per_share, rights_price, currency, factor_effect, backtest_ready, lifecycle_applicability, coverage_state, quality_state, blocking_reasons_json, source_lineage_json, input_hash, projection_version, decision_available_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event_id, revision_id, payload["instrument_id"], payload.get("source_event_key"), payload["action_type"], payload["announcement_date"], payload["record_date"], payload["effective_date"], payload["payment_date"], payload["share_arrival_date"],
                    payload.get("cash_dividend_per_share"), payload.get("bonus_shares_per_share"), payload.get("capitalization_shares_per_share"), payload.get("rights_shares_per_share"), payload.get("rights_price"), payload.get("currency"), int(payload["factor_effect"]), int(payload["backtest_ready"]), payload["lifecycle_applicability"], payload["coverage_state"], payload["quality_state"], _json(payload["blocking_reasons"]), _json(payload["source_lineage"]), payload["input_hash"], payload["projection_version"], payload["decision_available_at"], get_shanghai_time().isoformat(),
                ),
            )
            connection.execute(
                "INSERT OR REPLACE INTO canonical_corporate_action_current (canonical_event_id, projection_revision_id, payload_json, updated_at) VALUES (?, ?, ?, ?)",
                (event_id, revision_id, _json(payload), get_shanghai_time().isoformat()),
            )
            self._append_change(
                connection,
                dataset="canonical_corporate_actions",
                change_type="insert",
                business_key={"canonical_event_id": event_id, "projection_revision_id": revision_id},
                new_hash=payload["input_hash"],
                instrument_id=payload["instrument_id"],
                observation_date=payload["effective_date"],
            )
            connection.commit()
        return {"status": "inserted", "canonical_event_id": event_id, "projection_revision_id": revision_id}

    def list_canonical_actions(self, *, instrument_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, action_type: Optional[str] = None, ready_only: bool = False, known_at: Optional[str] = None, change_cursor: Optional[str] = None, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        if limit < 1 or limit > 5000:
            raise ValueError("limit must be between 1 and 5000")
        cutoff = _aware_iso(known_at, field_name="known_at") if known_at else None
        clauses = []
        params: list[Any] = []
        if instrument_id:
            clauses.append("instrument_id = ?")
            params.append(instrument_id)
        if start_date:
            clauses.append("effective_date >= ?")
            params.append(_date_text(start_date, field_name="start_date", required=True))
        if end_date:
            clauses.append("effective_date <= ?")
            params.append(_date_text(end_date, field_name="end_date", required=True))
        if action_type:
            clauses.append("action_type = ?")
            params.append(action_type)
        if ready_only:
            clauses.append("backtest_ready = 1")
        if cutoff:
            clauses.append("decision_available_at IS NOT NULL AND decision_available_at <= ?")
            params.append(cutoff)
        changed_event_ids: Optional[list[str]] = None
        if change_cursor:
            after_sequence = _decode_cursor(
                change_cursor, database_id=self.database_id, domain="backtest"
            )
            with self.connection() as connection:
                change_rows = connection.execute(
                    "SELECT business_key_json FROM data_change_log "
                    "WHERE domain = 'backtest' AND dataset = 'canonical_corporate_actions' "
                    "AND sequence_id > ? ORDER BY sequence_id",
                    (after_sequence,),
                ).fetchall()
            changed_event_ids = sorted(
                {
                    str(json.loads(row["business_key_json"])["canonical_event_id"])
                    for row in change_rows
                }
            )
            if not changed_event_ids:
                return {"status": "success", "known_at": cutoff, "items": [], "total": 0, "limit": limit, "offset": max(int(offset), 0), "database_id": self.database_id, "change_cursor": change_cursor}
            clauses.append(
                "canonical_event_id IN (" + ",".join("?" for _ in changed_event_ids) + ")"
            )
            params.extend(changed_event_ids)
        where = " AND ".join(clauses) if clauses else "1=1"
        with self.connection() as connection:
            base = (
                "FROM (SELECT *, ROW_NUMBER() OVER ("
                "PARTITION BY canonical_event_id ORDER BY decision_available_at DESC, "
                "created_at DESC, projection_revision_id DESC) AS revision_rank "
                f"FROM canonical_corporate_action_revisions WHERE {where}) "
                "WHERE revision_rank = 1"
            )
            count = int(connection.execute("SELECT COUNT(*) AS count " + base, params).fetchone()["count"])
            rows = connection.execute(
                "SELECT * " + base + " ORDER BY instrument_id, effective_date, canonical_event_id, decision_available_at DESC LIMIT ? OFFSET ?",
                [*params, limit, max(int(offset), 0)],
            ).fetchall()
        return {
            "status": "success",
            "known_at": cutoff,
            "items": [dict(row) for row in rows],
            "total": count,
            "limit": limit,
            "offset": max(int(offset), 0),
            "database_id": self.database_id,
            "change_cursor": change_cursor,
        }

    def read_changes(
        self,
        *,
        cursor: Optional[str] = None,
        dataset: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Read resumable changes within this physical database only."""
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        after_sequence = (
            _decode_cursor(cursor, database_id=self.database_id, domain="backtest")
            if cursor
            else 0
        )
        clauses = ["domain = 'backtest'", "sequence_id > ?"]
        params: list[Any] = [after_sequence]
        if dataset:
            clauses.append("dataset = ?")
            params.append(dataset)
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM data_change_log WHERE " + " AND ".join(clauses) +
                " ORDER BY sequence_id LIMIT ?",
                [*params, int(limit)],
            ).fetchall()
        items = [dict(row) for row in rows]
        sequence = int(items[-1]["sequence_id"]) if items else after_sequence
        return {
            "database_id": self.database_id,
            "domain": "backtest",
            "items": items,
            "next_cursor": _encode_cursor(
                database_id=self.database_id,
                domain="backtest",
                sequence=sequence,
            ),
        }

    def readiness(self) -> dict[str, Any]:
        tables = (
            "index_composition_snapshots",
            "security_state_events",
            "daily_price_limit_revisions",
            "canonical_corporate_action_revisions",
        )
        with self.connection() as connection:
            counts = {
                table: int(connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"])
                for table in tables
            }
            latest = connection.execute("SELECT MAX(sequence_id) AS sequence FROM data_change_log WHERE domain = 'backtest'").fetchone()["sequence"]
        return {"database_id": self.database_id, "counts": counts, "latest_watermark": int(latest or 0)}
