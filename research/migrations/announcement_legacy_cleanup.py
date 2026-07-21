"""One-way migration from legacy CNInfo announcement storage."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


LEGACY_STATE_TABLE = "cninfo_announcement_scan_state"
LEGACY_AUDIT_TABLE = "cninfo_announcement_audit"


def create_verified_backup_if_needed(db_path: str | Path) -> Optional[Dict[str, Any]]:
    """Create and verify a consistent pre-cleanup backup when legacy tables exist."""
    path = Path(db_path)
    if not path.exists() or not _legacy_tables_in_database(path):
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = path.with_name(
        f"{path.name}.pre_announcement_legacy_cleanup.{stamp}.bak"
    )
    suffix = 1
    while backup_path.exists():
        backup_path = path.with_name(
            f"{path.name}.pre_announcement_legacy_cleanup.{stamp}.{suffix}.bak"
        )
        suffix += 1
    with sqlite3.connect(path) as source, sqlite3.connect(backup_path) as target:
        source_counts = _legacy_row_counts(source)
        source.backup(target)
        integrity = str(target.execute("PRAGMA integrity_check").fetchone()[0])
        backup_counts = _legacy_row_counts(target)
    if integrity.lower() != "ok":
        backup_path.unlink(missing_ok=True)
        raise RuntimeError(f"announcement cleanup backup integrity failed: {integrity}")
    if backup_counts != source_counts:
        backup_path.unlink(missing_ok=True)
        raise RuntimeError(
            "announcement cleanup backup row-count verification failed: "
            f"source={source_counts} backup={backup_counts}"
        )
    digest = _file_sha256(backup_path)
    if not digest or backup_path.stat().st_size <= 0:
        backup_path.unlink(missing_ok=True)
        raise RuntimeError("announcement cleanup backup verification failed")
    return {
        "path": str(backup_path),
        "sha256": digest,
        "size_bytes": backup_path.stat().st_size,
        "integrity_check": integrity,
        "legacy_table_rows": backup_counts,
    }


def migrate_reconcile_and_drop(
    conn: sqlite3.Connection,
    *,
    backup: Optional[Mapping[str, Any]],
    now: str,
) -> Optional[Dict[str, Any]]:
    """Backfill, reconcile, and remove legacy tables in one transaction."""
    state_exists = _object_type(conn, LEGACY_STATE_TABLE) == "table"
    audit_exists = _object_type(conn, LEGACY_AUDIT_TABLE) == "table"
    if not state_exists and not audit_exists:
        return None
    if backup is None:
        raise RuntimeError("announcement legacy cleanup requires a verified backup")

    state_rows = (
        [dict(row) for row in conn.execute(f"SELECT * FROM {LEGACY_STATE_TABLE}")]
        if state_exists
        else []
    )
    audit_rows = (
        [dict(row) for row in conn.execute(f"SELECT * FROM {LEGACY_AUDIT_TABLE}")]
        if audit_exists
        else []
    )
    for item in state_rows:
        _backfill_state(conn, item, now=now)
    for item in audit_rows:
        _backfill_audit(conn, item, now=now)

    reconciliation = _reconcile(conn, state_rows=state_rows, audit_rows=audit_rows)
    if reconciliation["errors"]:
        raise RuntimeError(
            "announcement legacy reconciliation failed: "
            + "; ".join(reconciliation["errors"][:10])
        )
    if state_exists:
        conn.execute(f"DROP TABLE {LEGACY_STATE_TABLE}")
    if audit_exists:
        conn.execute(f"DROP TABLE {LEGACY_AUDIT_TABLE}")
    return {
        **reconciliation,
        "backup": dict(backup),
        "legacy_tables_dropped": [
            name
            for name, exists in (
                (LEGACY_STATE_TABLE, state_exists),
                (LEGACY_AUDIT_TABLE, audit_exists),
            )
            if exists
        ],
    }


def _backfill_state(conn: sqlite3.Connection, item: Mapping[str, Any], *, now: str) -> None:
    scope, scope_key = _legacy_scope(item)
    metadata = _required_json_value(
        item.get("metadata_json"),
        default={},
        field_name="metadata_json",
        row_identity=f"state:{item.get('purpose_key')}:{item.get('market')}",
    )
    if not isinstance(metadata, dict):
        raise RuntimeError("legacy announcement state metadata_json must be an object")
    metadata = {
        **metadata,
        "migrated_from": LEGACY_STATE_TABLE,
        "legacy_market": item.get("market"),
        "legacy_column": item.get("column_name"),
        "legacy_cursor": item.get("last_watermark"),
    }
    conn.execute(
        """
        INSERT INTO announcement_scan_state (
            purpose_key, source, scope_key, scope_json,
            committed_cursor_kind, committed_cursor_value,
            max_published_at, last_scan_started_at,
            last_scan_completed_at, pages_scanned, requests_made,
            announcements_seen, selected_announcements, status,
            is_complete, stop_reason, attempts_json, metadata_json,
            created_at, updated_at
        ) VALUES (?, 'cninfo', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?)
        ON CONFLICT(purpose_key, source, scope_key) DO NOTHING
        """,
        (
            item.get("purpose_key"),
            scope_key,
            _stable_json(scope),
            "published_at" if item.get("last_watermark") else None,
            item.get("last_watermark"),
            item.get("last_watermark"),
            item.get("last_scan_started_at"),
            item.get("last_scan_completed_at"),
            int(item.get("pages_scanned") or 0),
            int(item.get("pages_scanned") or 0),
            int(item.get("announcements_seen") or 0),
            int(item.get("selected_announcements") or 0),
            item.get("status") or "success",
            int((item.get("status") or "success") == "success"),
            "legacy_migration",
            _stable_json(metadata),
            item.get("created_at") or now,
            item.get("updated_at") or now,
        ),
    )


def _backfill_audit(conn: sqlite3.Connection, item: Mapping[str, Any], *, now: str) -> None:
    adjunct_url = item.get("adjunct_url")
    attachments = (
        [{"source_url": adjunct_url, "resolved_url": None}]
        if adjunct_url
        else []
    )
    row_identity = f"audit:{item.get('announcement_id')}"
    selection_reasons = _required_json_value(
        item.get("selection_reasons_json"),
        default=[],
        field_name="selection_reasons_json",
        row_identity=row_identity,
    )
    raw_payload = _required_json_value(
        item.get("raw_payload_json"),
        default={},
        field_name="raw_payload_json",
        row_identity=row_identity,
    )
    if not isinstance(selection_reasons, list):
        raise RuntimeError(
            f"legacy announcement selection_reasons_json must be a list: {row_identity}"
        )
    if not isinstance(raw_payload, dict):
        raise RuntimeError(
            f"legacy announcement raw_payload_json must be an object: {row_identity}"
        )
    conn.execute(
        """
        INSERT INTO announcement_audit (
            purpose_key, source, announcement_key,
            source_announcement_id, instrument_id, symbol,
            exchange, market, published_at, published_at_raw,
            title, attachments_json, selection_reasons_json,
            diagnostics_json, raw_payload_json, ingestion_run_id,
            created_at, updated_at
        ) VALUES (?, 'cninfo', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(purpose_key, announcement_key, instrument_id) DO NOTHING
        """,
        (
            item.get("purpose_key"),
            f"cninfo:{item.get('announcement_id')}",
            item.get("announcement_id"),
            item.get("instrument_id") or "",
            item.get("symbol"),
            item.get("market"),
            item.get("market"),
            item.get("announcement_time"),
            item.get("announcement_time"),
            item.get("title"),
            _stable_json(attachments),
            _stable_json(selection_reasons),
            _stable_json(["migrated_from_legacy_cninfo_audit"]),
            _stable_json(raw_payload),
            item.get("ingestion_run_id"),
            item.get("created_at") or now,
            item.get("updated_at") or now,
        ),
    )


def _reconcile(
    conn: sqlite3.Connection,
    *,
    state_rows: list[Dict[str, Any]],
    audit_rows: list[Dict[str, Any]],
) -> Dict[str, Any]:
    errors: list[str] = []
    for item in state_rows:
        _, scope_key = _legacy_scope(item)
        row = conn.execute(
            """
            SELECT * FROM announcement_scan_state
            WHERE purpose_key = ? AND source = 'cninfo' AND scope_key = ?
            """,
            (item.get("purpose_key"), scope_key),
        ).fetchone()
        if row is None:
            errors.append(f"state_missing:{item.get('purpose_key')}:{scope_key}")
            continue
        generic = dict(row)
        legacy_cursor = item.get("last_watermark")
        if generic.get("committed_cursor_value") != legacy_cursor:
            metadata = _json_value(generic.get("metadata_json"), {})
            advanced = (
                metadata.get("legacy_cursor") == legacy_cursor
                and str(generic.get("updated_at") or "")
                >= str(item.get("updated_at") or "")
            )
            if not advanced:
                errors.append(
                    f"state_cursor_mismatch:{item.get('purpose_key')}:{scope_key}"
                )
    for item in audit_rows:
        announcement_key = f"cninfo:{item.get('announcement_id')}"
        row = conn.execute(
            """
            SELECT * FROM announcement_audit
            WHERE purpose_key = ? AND announcement_key = ? AND instrument_id = ?
            """,
            (
                item.get("purpose_key"),
                announcement_key,
                item.get("instrument_id") or "",
            ),
        ).fetchone()
        if row is None:
            errors.append(f"audit_missing:{item.get('purpose_key')}:{announcement_key}")
            continue
        generic = dict(row)
        if _json_hash_strict(
            generic.get("selection_reasons_json"),
            [],
            field_name="generic.selection_reasons_json",
            row_identity=announcement_key,
        ) != _json_hash_strict(
            item.get("selection_reasons_json"),
            [],
            field_name="legacy.selection_reasons_json",
            row_identity=announcement_key,
        ):
            errors.append(f"audit_reason_mismatch:{announcement_key}")
        if _json_hash_strict(
            generic.get("raw_payload_json"),
            {},
            field_name="generic.raw_payload_json",
            row_identity=announcement_key,
        ) != _json_hash_strict(
            item.get("raw_payload_json"),
            {},
            field_name="legacy.raw_payload_json",
            row_identity=announcement_key,
        ):
            errors.append(f"audit_payload_mismatch:{announcement_key}")
        if generic.get("source_announcement_id") != item.get("announcement_id"):
            errors.append(f"audit_identity_mismatch:{announcement_key}")
        if generic.get("ingestion_run_id") != item.get("ingestion_run_id"):
            errors.append(f"audit_lineage_mismatch:{announcement_key}")
    return {
        "legacy_state_rows": len(state_rows),
        "legacy_audit_rows": len(audit_rows),
        "reconciled_state_rows": len(state_rows) - sum(
            error.startswith("state_") for error in errors
        ),
        "reconciled_audit_rows": len(audit_rows) - sum(
            error.startswith("audit_") for error in errors
        ),
        "errors": errors,
    }


def _legacy_scope(item: Mapping[str, Any]) -> tuple[Dict[str, Any], str]:
    scope = {
        "exchange": item.get("market"),
        "market": item.get("market"),
        "instrument_id": None,
        "symbol": None,
        "keyword": None,
        "category": None,
        "source_options": {"column": item.get("column_name")},
    }
    return scope, hashlib.sha256(_stable_json(scope).encode("utf-8")).hexdigest()


def _legacy_tables_in_database(path: Path) -> bool:
    with sqlite3.connect(path) as conn:
        names = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    return bool({LEGACY_STATE_TABLE, LEGACY_AUDIT_TABLE} & names)


def _legacy_row_counts(conn: sqlite3.Connection) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for table_name in (LEGACY_STATE_TABLE, LEGACY_AUDIT_TABLE):
        if _object_type(conn, table_name) == "table":
            counts[table_name] = int(
                conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            )
    return counts


def _object_type(conn: sqlite3.Connection, name: str) -> Optional[str]:
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = ? LIMIT 1",
        (name,),
    ).fetchone()
    return None if row is None else str(row[0])


def _json_value(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError):
        return default


def _required_json_value(
    value: Any,
    default: Any,
    *,
    field_name: str,
    row_identity: str,
) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "legacy announcement cleanup encountered invalid JSON: "
            f"row={row_identity} field={field_name}"
        ) from exc


def _json_hash_strict(
    value: Any,
    default: Any,
    *,
    field_name: str,
    row_identity: str,
) -> str:
    return hashlib.sha256(
        _stable_json(
            _required_json_value(
                value,
                default,
                field_name=field_name,
                row_identity=row_identity,
            )
        ).encode("utf-8")
    ).hexdigest()


def _stable_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
