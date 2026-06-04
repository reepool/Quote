#!/usr/bin/env python3
"""Validate HKEX lifecycle_write against a copied quotes database.

This script intentionally redirects the Quote database connection before
importing DataManager, then runs the production HKEX master sync logic against
the temporary database only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


LIFECYCLE_FIELDS = (
    "status",
    "is_active",
    "trading_status",
    "delisted_date",
)

EXCLUDED_METADATA_TYPES = {
    "cbbc",
    "debt",
    "hdr",
    "inline_warrant",
    "leveraged_and_inverse_product",
    "leveraged_inverse_product",
    "professional_preference_share",
    "restricted_security",
    "rmb_counter",
    "spac_share",
    "spac_warrant",
    "stock_connect_special_counter",
    "temporary_counter",
    "trading_only",
    "reserved_transition_counter",
    "warrant",
}


def _resolve(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _backup_sqlite(source_db: Path, work_db: Path) -> None:
    work_db.parent.mkdir(parents=True, exist_ok=True)
    if work_db.exists():
        raise FileExistsError(f"work database already exists: {work_db}")
    source_uri = f"file:{source_db}?mode=ro"
    with sqlite3.connect(source_uri, uri=True) as source, sqlite3.connect(str(work_db)) as dest:
        source.backup(dest)


def _fetch_hkex_lifecycle_rows(db_path: Path) -> Dict[str, Dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT instrument_id, symbol, name, status, is_active, trading_status,
                   delisted_date, source, updated_at
            FROM instruments
            WHERE exchange = 'HKEX'
            """
        ).fetchall()
    return {row["instrument_id"]: dict(row) for row in rows}


def _count_rows(rows: Iterable[sqlite3.Row]) -> Dict[str, int]:
    return {str(row[0]): int(row[1]) for row in rows}


def _snapshot(db_path: Path) -> Dict[str, Any]:
    with _connect(db_path) as conn:
        status_counts = _count_rows(
            conn.execute(
                """
                SELECT COALESCE(status, '<null>'), COUNT(*)
                FROM instruments
                WHERE exchange = 'HKEX'
                GROUP BY COALESCE(status, '<null>')
                ORDER BY COALESCE(status, '<null>')
                """
            ).fetchall()
        )
        total = conn.execute(
            "SELECT COUNT(*) FROM instruments WHERE exchange = 'HKEX'"
        ).fetchone()[0]
        active = conn.execute(
            """
            SELECT COUNT(*)
            FROM instruments
            WHERE exchange = 'HKEX' AND is_active = 1
            """
        ).fetchone()[0]
        tradable_stock = conn.execute(
            """
            SELECT COUNT(*)
            FROM instruments
            WHERE exchange = 'HKEX'
              AND type = 'stock'
              AND is_active = 1
              AND trading_status = 1
            """
        ).fetchone()[0]
        excluded_active = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM instruments i
            LEFT JOIN instrument_master_metadata m
              ON m.instrument_id = i.instrument_id
            WHERE i.exchange = 'HKEX'
              AND (i.is_active = 1 OR i.trading_status = 1)
              AND (
                m.research_scope = 'exclude'
                OR m.product_type IN ({','.join('?' for _ in EXCLUDED_METADATA_TYPES)})
              )
            """,
            tuple(sorted(EXCLUDED_METADATA_TYPES)),
        ).fetchone()[0]
        metadata_exclude = conn.execute(
            """
            SELECT COUNT(*)
            FROM instrument_master_metadata
            WHERE exchange = 'HKEX' AND research_scope = 'exclude'
            """
        ).fetchone()[0]
        recent_discrepancies = conn.execute(
            """
            SELECT COUNT(*)
            FROM instrument_master_discrepancies
            WHERE exchange = 'HKEX'
              AND created_at >= datetime('now', '-1 day')
            """
        ).fetchone()[0]
    return {
        "total": int(total),
        "active": int(active),
        "tradable_stock": int(tradable_stock),
        "status_counts": status_counts,
        "excluded_active_or_tradable": int(excluded_active),
        "metadata_exclude": int(metadata_exclude),
        "recent_discrepancies": int(recent_discrepancies),
    }


def _normalize_bool(value: Any) -> int:
    return 1 if value in (True, 1, "1") else 0


def _field_tuple(row: Optional[Dict[str, Any]]) -> Tuple[Any, ...]:
    if row is None:
        return tuple(None for _ in LIFECYCLE_FIELDS)
    return tuple(row.get(field) for field in LIFECYCLE_FIELDS)


def _classify_change(before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> str:
    if before is None and after is not None:
        return "inserted"
    if before is not None and after is None:
        return "deleted"
    if before is None or after is None:
        return "unknown"

    before_active = _normalize_bool(before.get("is_active"))
    after_active = _normalize_bool(after.get("is_active"))
    before_trading = _normalize_bool(before.get("trading_status"))
    after_trading = _normalize_bool(after.get("trading_status"))
    after_status = str(after.get("status") or "").lower()

    if after_status == "excluded" and after_active == 0 and after_trading == 0:
        return "excluded"
    if after_status == "delisted" and after_active == 0:
        return "delisted"
    if after_status == "suspended" and after_active == 1 and after_trading == 0:
        return "suspended"
    if before_active == 0 and after_active == 1 and after_trading == 1:
        return "reactivated"
    if before_trading == 1 and after_trading == 0:
        return "trading_disabled"
    return "other_lifecycle_change"


def _diff_lifecycle(
    before: Dict[str, Dict[str, Any]],
    after: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    changed: List[Dict[str, Any]] = []
    for instrument_id in sorted(set(before) | set(after)):
        before_row = before.get(instrument_id)
        after_row = after.get(instrument_id)
        if _field_tuple(before_row) == _field_tuple(after_row):
            continue
        changed.append(
            {
                "instrument_id": instrument_id,
                "symbol": (after_row or before_row or {}).get("symbol"),
                "name": (after_row or before_row or {}).get("name"),
                "change_type": _classify_change(before_row, after_row),
                "before": {field: (before_row or {}).get(field) for field in LIFECYCLE_FIELDS},
                "after": {field: (after_row or {}).get(field) for field in LIFECYCLE_FIELDS},
                "after_source": (after_row or {}).get("source"),
            }
        )
    counts: Dict[str, int] = {}
    for item in changed:
        counts[item["change_type"]] = counts.get(item["change_type"], 0) + 1
    return {
        "total": len(changed),
        "counts": counts,
        "samples": changed[:50],
    }


def _build_blockers(result: Dict[str, Any], before_after: Dict[str, Any]) -> List[str]:
    blockers: List[str] = []
    hkex = (result.get("exchanges") or {}).get("HKEX") or {}
    summary = result.get("summary") or {}
    policy = hkex.get("source_evidence_policy") or {}
    lifecycle_diff = before_after["lifecycle_diff"]
    lifecycle_counts = lifecycle_diff.get("counts") or {}

    if result.get("status") == "error":
        blockers.append("sync_result_status_error")
    if int(summary.get("review_required", 0) or 0) > 0:
        blockers.append("review_required_not_zero")
    for key in (
        "safe_write_allowed",
        "reactivation_write_allowed",
        "delisting_write_allowed",
        "suspension_write_allowed",
    ):
        if not policy.get(key):
            blockers.append(f"source_policy_{key}_false")
    if int(before_after["after"].get("excluded_active_or_tradable", 0) or 0) > 0:
        blockers.append("excluded_hkex_products_still_active_or_tradable")

    expected_counts = {
        "delisted": int(hkex.get("delisted_count", 0) or 0),
        "suspended": int(hkex.get("suspended_count", 0) or 0),
        "reactivated": int(hkex.get("reactivated_count", 0) or 0),
        "excluded": max(
            0,
            int((before_after["after"].get("status_counts") or {}).get("excluded", 0) or 0)
            - int((before_after["before"].get("status_counts") or {}).get("excluded", 0) or 0),
        ),
    }
    for change_type, expected in expected_counts.items():
        actual = int(lifecycle_counts.get(change_type, 0) or 0)
        if change_type == "excluded":
            actual += int(lifecycle_counts.get("trading_disabled", 0) or 0)
        if actual != expected:
            blockers.append(
                f"{change_type}_diff_count_mismatch_expected_{expected}_actual_{actual}"
            )

    unexpected_types = {
        change_type: count
        for change_type, count in lifecycle_counts.items()
        if change_type not in expected_counts
        and not (change_type == "trading_disabled" and expected_counts.get("excluded", 0) == count)
    }
    if unexpected_types:
        blockers.append(f"unexpected_lifecycle_change_types_{unexpected_types}")
    return blockers


async def _run_sync(work_db: Path, timeout_sec: int) -> Dict[str, Any]:
    from utils.config_manager import config_manager

    config_manager.set_nested("database_config.db_path", str(work_db))
    config_manager.set_nested("data_config.hkex_instrument_master_sync.enabled", True)
    config_manager.set_nested("data_config.hkex_instrument_master_sync.mode", "lifecycle_write")

    from data_manager import DataManager

    manager = DataManager()
    return await manager.sync_hkex_instrument_master(
        mode="lifecycle_write",
        timeout_sec=timeout_sec,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run HKEX lifecycle_write on a copied quotes DB and validate lifecycle changes."
    )
    parser.add_argument("--source-db", default="data/quotes.db")
    parser.add_argument(
        "--work-db",
        default=f"/tmp/hkex_lifecycle_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
    )
    parser.add_argument("--timeout-sec", type=int, default=120)
    parser.add_argument(
        "--runtime-timeout-sec",
        type=int,
        default=420,
        help="Hard timeout for the whole lifecycle validation run.",
    )
    parser.add_argument("--json-output", default="")
    parser.add_argument(
        "--allow-blockers",
        action="store_true",
        help="Return exit code 0 even when validation blockers are found.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    source_db = _resolve(args.source_db)
    work_db = _resolve(args.work_db)
    output_path = _resolve(args.json_output) if args.json_output else None

    if not source_db.exists():
        raise FileNotFoundError(f"source database not found: {source_db}")

    _backup_sqlite(source_db, work_db)
    before_rows = _fetch_hkex_lifecycle_rows(work_db)
    before_snapshot = _snapshot(work_db)
    try:
        result = asyncio.run(
            asyncio.wait_for(
                _run_sync(work_db, timeout_sec=args.timeout_sec),
                timeout=args.runtime_timeout_sec,
            )
        )
    except asyncio.TimeoutError:
        result = {
            "status": "error",
            "mode": "lifecycle_write",
            "summary": {"review_required": -1},
            "exchanges": {
                "HKEX": {
                    "status": "error",
                    "errors": [
                        f"hkex lifecycle validation exceeded {args.runtime_timeout_sec}s"
                    ],
                    "source_evidence_policy": {},
                    "delisted_count": 0,
                    "suspended_count": 0,
                    "reactivated_count": 0,
                }
            },
            "warnings": [],
            "errors": [f"hkex lifecycle validation exceeded {args.runtime_timeout_sec}s"],
        }
    after_rows = _fetch_hkex_lifecycle_rows(work_db)
    after_snapshot = _snapshot(work_db)

    validation = {
        "source_db": str(source_db),
        "work_db": str(work_db),
        "before": before_snapshot,
        "after": after_snapshot,
        "sync_result": result,
        "lifecycle_diff": _diff_lifecycle(before_rows, after_rows),
    }
    blockers = _build_blockers(result, validation)
    validation["validation"] = {
        "status": "pass" if not blockers else "blocked",
        "blockers": blockers,
    }

    text = json.dumps(validation, ensure_ascii=False, indent=2, default=str)
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
    print(text)
    if blockers and not args.allow_blockers:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
