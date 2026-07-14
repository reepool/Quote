"""Shared SQLite change-watermark helpers for research data domains."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from typing import Any, Mapping, Optional


CHANGE_LOG_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS data_change_log (
    sequence_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    domain TEXT NOT NULL,
    dataset TEXT NOT NULL,
    change_type TEXT NOT NULL,
    business_key_json TEXT NOT NULL,
    instrument_id TEXT,
    series_id TEXT,
    observation_date TEXT,
    period TEXT,
    old_hash TEXT,
    new_hash TEXT,
    row_version INTEGER,
    source TEXT,
    source_mode TEXT,
    source_profile TEXT,
    ingestion_run_id TEXT,
    batch_id TEXT,
    changed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_change_log_domain_sequence
ON data_change_log(domain, sequence_id);

CREATE INDEX IF NOT EXISTS idx_change_log_dataset_sequence
ON data_change_log(dataset, sequence_id);

CREATE INDEX IF NOT EXISTS idx_change_log_domain_dataset_sequence
ON data_change_log(domain, dataset, sequence_id);

CREATE INDEX IF NOT EXISTS idx_change_log_instrument_date
ON data_change_log(instrument_id, observation_date);

CREATE INDEX IF NOT EXISTS idx_change_log_series_date
ON data_change_log(series_id, observation_date);
"""


def ensure_change_log_schema(conn: sqlite3.Connection) -> None:
    """Create the additive shared changelog schema in the current database."""
    conn.executescript(CHANGE_LOG_SCHEMA_SQL)


def ensure_row_version_column(conn: sqlite3.Connection, table_name: str) -> None:
    """Add a row-version column without rewriting existing business rows."""
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"invalid SQLite table name: {table_name}")
    columns = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if "row_version" not in columns:
        conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1"
        )


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", "disabled"}
    return bool(value)


def is_change_log_enabled(
    config: Optional[Mapping[str, Any]],
    *,
    domain: str,
    dataset: str,
) -> bool:
    """Resolve rollout switches with enabled-by-default additive semantics."""
    cfg = config if isinstance(config, Mapping) else {}
    if not _coerce_bool(cfg.get("enabled"), True):
        return False
    domains = cfg.get("domains") if isinstance(cfg.get("domains"), Mapping) else {}
    datasets = cfg.get("datasets") if isinstance(cfg.get("datasets"), Mapping) else {}
    if domain in domains and not _coerce_bool(domains.get(domain), True):
        return False
    if dataset in datasets and not _coerce_bool(datasets.get(dataset), True):
        return False
    return True


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def append_change_record(
    conn: sqlite3.Connection,
    *,
    config: Optional[Mapping[str, Any]],
    domain: str,
    dataset: str,
    change_type: str,
    business_key: Mapping[str, Any],
    changed_at: str,
    instrument_id: Optional[str] = None,
    series_id: Optional[str] = None,
    observation_date: Optional[str] = None,
    period: Optional[str] = None,
    old_hash: Optional[str] = None,
    new_hash: Optional[str] = None,
    row_version: Optional[int] = None,
    source: Optional[str] = None,
    source_mode: Optional[str] = None,
    source_profile: Optional[str] = None,
    ingestion_run_id: Optional[int | str] = None,
    batch_id: Optional[str] = None,
) -> bool:
    """Append one local-observed change record in the caller's transaction."""
    if not is_change_log_enabled(config, domain=domain, dataset=dataset):
        return False
    conn.execute(
        """
        INSERT INTO data_change_log (
            domain, dataset, change_type, business_key_json, instrument_id,
            series_id, observation_date, period, old_hash, new_hash,
            row_version, source, source_mode, source_profile, ingestion_run_id,
            batch_id, changed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            domain,
            dataset,
            change_type,
            json.dumps(
                dict(business_key),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=_json_default,
            ),
            instrument_id,
            series_id,
            observation_date,
            period,
            old_hash,
            new_hash,
            row_version,
            source,
            source_mode,
            source_profile,
            str(ingestion_run_id) if ingestion_run_id is not None else None,
            batch_id,
            changed_at,
        ),
    )
    return True
