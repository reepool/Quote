#!/usr/bin/env python3
"""Dry-run or execute bounded semantic row-hash backfills."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from typing import Any, Callable


def _semantic_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _canonical_market_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        try:
            return format(Decimal(str(value)).normalize(), "f")
        except InvalidOperation:
            return str(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, dict):
        return {
            str(key): _canonical_market_value(item)
            for key, item in sorted(value.items())
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_market_value(item) for item in value]
    return str(value)


def _market_semantic_hash(payload: dict[str, Any]) -> str:
    normalized = {
        key: _canonical_market_value(payload.get(key))
        for key in sorted(payload)
    }
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_value(value: Any) -> Any:
    if value in (None, ""):
        return {}
    if isinstance(value, (dict, list)):
        return value
    return json.loads(str(value))


def _normalized_payload(
    row: sqlite3.Row,
    *,
    json_columns: tuple[str, ...] = (),
    excluded_columns: tuple[str, ...] = (),
) -> dict[str, Any]:
    payload = dict(row)
    for column in json_columns:
        payload[column] = _json_value(payload.get(column))
    for column in (
        "rowid",
        "row_hash",
        "row_version",
        "ingestion_run_id",
        "created_at",
        "updated_at",
        *excluded_columns,
    ):
        payload.pop(column, None)
    return payload


def _quote_payload(row: sqlite3.Row) -> dict[str, Any]:
    fields = (
        "instrument_id",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover",
        "pre_close",
        "change",
        "pct_change",
        "tradestatus",
        "factor",
        "adjustment_type",
        "is_complete",
        "quality_score",
        "source",
    )
    payload = {field: row[field] for field in fields}
    payload["time"] = datetime.fromisoformat(str(payload["time"]))
    payload["is_complete"] = bool(payload["is_complete"])
    return payload


def _factor_payload(row: sqlite3.Row) -> dict[str, Any]:
    fields = (
        "instrument_id",
        "ex_date",
        "factor",
        "cumulative_factor",
        "dividend",
        "bonus_shares",
        "rights_shares",
        "rights_price",
        "event_type",
        "source",
    )
    payload = {field: row[field] for field in fields}
    payload["ex_date"] = datetime.fromisoformat(str(payload["ex_date"]))
    return payload


def _normalizer(**kwargs: Any) -> Callable[[sqlite3.Row], dict[str, Any]]:
    return lambda row: _normalized_payload(row, **kwargs)


TABLES: dict[str, dict[str, Any]] = {
    "daily_quotes": {
        "date_column": "time",
        "normalizer": _quote_payload,
        "hasher": _market_semantic_hash,
    },
    "adjustment_factors": {
        "date_column": "ex_date",
        "normalizer": _factor_payload,
        "hasher": _market_semantic_hash,
    },
    "shareholder_snapshots": {
        "normalizer": _normalizer(
            json_columns=("snapshot_json",),
            excluded_columns=("data_as_of",),
        ),
    },
    "financial_facts": {
        "date_column": "report_period",
        "normalizer": _normalizer(
            json_columns=("facts_json", "lineage_json"),
            excluded_columns=("data_as_of",),
        ),
    },
    "financial_numeric_facts": {
        "date_column": "report_period",
        "normalizer": _normalizer(
            json_columns=("dimensions_json", "raw_fact_json"),
            excluded_columns=("dimensions_hash",),
        ),
    },
    "industry_taxonomy": {
        "normalizer": _normalizer(json_columns=("aliases_json",)),
    },
    "industry_memberships": {
        "date_column": "effective_date",
        "normalizer": _normalizer(
            json_columns=("membership_json",),
            excluded_columns=("data_as_of",),
        ),
    },
    "valuation_inputs": {
        "date_column": "as_of_date",
        "normalizer": _normalizer(json_columns=("diagnostics_json",)),
    },
    "valuation_history": {
        "date_column": "as_of_date",
        "normalizer": _normalizer(
            json_columns=("details_json",),
            excluded_columns=("data_as_of",),
        ),
    },
    "risk_snapshots": {
        "date_column": "as_of_date",
        "normalizer": _normalizer(
            json_columns=("details_json",),
            excluded_columns=("data_as_of",),
        ),
    },
    "technical_indicator_latest": {
        "date_column": "as_of_date",
        "normalizer": _normalizer(
            json_columns=("summary_json",),
            excluded_columns=("data_as_of",),
        ),
    },
    "risk_free_rate_series": {
        "normalizer": _normalizer(),
    },
    "risk_free_rate_observations": {
        "date_column": "observation_date",
        "normalizer": _normalizer(),
    },
}

for alias, canonical in {
    "financial_core_facts_hot": "financial_facts",
    "financial_core_facts_history": "financial_facts",
    "financial_numeric_facts_hot": "financial_numeric_facts",
    "financial_numeric_facts_history": "financial_numeric_facts",
}.items():
    TABLES[alias] = TABLES[canonical]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing semantic row hashes without emitting changelog rows."
    )
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--table", required=True, choices=sorted(TABLES))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--execute", action="store_true")
    return parser.parse_args()


def backfill_hashes(
    db_path: str | Path,
    table: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
    execute: bool = False,
) -> dict[str, Any]:
    if table not in TABLES:
        raise ValueError(f"unsupported table: {table}")
    if limit <= 0 or limit > 10000:
        raise ValueError("limit must be between 1 and 10000")
    resolved_db_path = Path(db_path)
    if not resolved_db_path.is_file():
        raise FileNotFoundError(f"database file does not exist: {resolved_db_path}")
    spec = TABLES[table]
    date_column = spec.get("date_column")
    if (start_date or end_date) and not date_column:
        raise ValueError(f"table {table} does not support date bounds")

    clauses = ["row_hash IS NULL"]
    params: list[Any] = []
    if start_date:
        clauses.append(f"{date_column} >= ?")
        params.append(start_date)
    if end_date:
        clauses.append(f"{date_column} <= ?")
        params.append(end_date)
    where_sql = " AND ".join(clauses)

    conn = sqlite3.connect(str(resolved_db_path))
    conn.row_factory = sqlite3.Row
    try:
        columns = {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if not columns:
            raise ValueError(f"table does not exist: {table}")
        if "row_hash" not in columns:
            raise ValueError(f"table has no row_hash column: {table}")
        rows = conn.execute(
            f"SELECT rowid, * FROM {table} WHERE {where_sql} ORDER BY rowid LIMIT ?",
            [*params, limit],
        ).fetchall()
        updates = [
            (
                spec.get("hasher", _semantic_hash)(spec["normalizer"](row)),
                int(row["rowid"]),
            )
            for row in rows
        ]
        if execute and updates:
            conn.executemany(
                f"UPDATE {table} SET row_hash = ? WHERE rowid = ? AND row_hash IS NULL",
                updates,
            )
            conn.commit()
        remaining = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {where_sql}",
            params,
        ).fetchone()[0]
    finally:
        conn.close()

    return {
        "status": "executed" if execute else "dry_run",
        "db_path": str(resolved_db_path),
        "table": table,
        "selected": len(updates),
        "written": len(updates) if execute else 0,
        "remaining_missing_hashes": int(remaining),
        "limit": limit,
        "start_date": start_date,
        "end_date": end_date,
    }


def main() -> int:
    args = parse_args()
    result = backfill_hashes(
        args.db_path,
        args.table,
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        execute=args.execute,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
