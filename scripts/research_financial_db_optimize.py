#!/usr/bin/env python3
"""Optimize financials.db storage while preserving the production DB name."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_BACKUP_DIR = Path("/home/python/Quote/data/PVE-Bak/QuoteBak")


def _object_type(conn: sqlite3.Connection, name: str) -> Optional[str]:
    row = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = ? ORDER BY type LIMIT 1",
        (name,),
    ).fetchone()
    return None if row is None else str(row[0])


def _count(conn: sqlite3.Connection, table_or_view: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table_or_view}").fetchone()[0])


def _backup_sqlite_db(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileExistsError(f"backup already exists: {destination}")
    with sqlite3.connect(source) as src, sqlite3.connect(destination) as dst:
        src.backup(dst)


def _optimize_work_db(work_db: Path) -> Dict[str, Any]:
    with sqlite3.connect(work_db) as conn:
        conn.row_factory = sqlite3.Row
        before_type = _object_type(conn, "financial_numeric_facts")
        before_count = _count(conn, "financial_numeric_facts") if before_type else None
        hot_count = _count(conn, "financial_numeric_facts_hot")
        history_count = _count(conn, "financial_numeric_facts_history")

        if before_type == "table":
            conn.execute("DROP TABLE financial_numeric_facts")
            conn.execute(
                """
                CREATE VIEW financial_numeric_facts AS
                SELECT * FROM financial_numeric_facts_hot
                UNION ALL
                SELECT * FROM financial_numeric_facts_history
                """
            )
            conn.commit()
        elif before_type == "view":
            pass
        else:
            raise RuntimeError("financial_numeric_facts object is missing")

        after_type = _object_type(conn, "financial_numeric_facts")
        after_count = _count(conn, "financial_numeric_facts")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    return {
        "before_type": before_type,
        "after_type": after_type,
        "before_count": before_count,
        "after_count": after_count,
        "hot_count": hot_count,
        "history_count": history_count,
        "integrity_check": integrity,
    }


def build_plan(db_path: Path, backup_dir: Path, *, timestamp: Optional[str] = None) -> Dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"financial database not found: {db_path}")
    stamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "db_path": str(db_path),
        "backup_dir": str(backup_dir),
        "backup_path": str(backup_dir / f"{db_path.name}.{stamp}.bak"),
        "work_path": str(db_path.with_name(f"{db_path.name}.optimize_work_{stamp}")),
        "compact_path": str(db_path.with_name(f"{db_path.name}.optimize_compact_{stamp}")),
        "final_path": str(db_path),
        "keeps_database_name": True,
        "drops_duplicate_physical_table": "financial_numeric_facts",
        "creates_compatibility_view": "financial_numeric_facts",
    }


def execute_optimization(plan: Dict[str, Any]) -> Dict[str, Any]:
    db_path = Path(plan["db_path"])
    backup_path = Path(plan["backup_path"])
    work_path = Path(plan["work_path"])
    compact_path = Path(plan["compact_path"])

    if work_path.exists() or compact_path.exists():
        raise FileExistsError("temporary optimization path already exists")

    _backup_sqlite_db(db_path, backup_path)
    _backup_sqlite_db(db_path, work_path)
    validation = _optimize_work_db(work_path)
    if validation["integrity_check"] != "ok":
        raise RuntimeError(f"work DB integrity check failed: {validation['integrity_check']}")
    if validation["before_count"] is not None and validation["after_count"] != validation["before_count"]:
        raise RuntimeError(
            "compatibility view row count differs from original financial_numeric_facts"
        )

    with sqlite3.connect(work_path) as conn:
        conn.execute(f"VACUUM INTO '{compact_path}'")

    with sqlite3.connect(compact_path) as conn:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"compact DB integrity check failed: {integrity}")

    os.replace(compact_path, db_path)
    try:
        work_path.unlink()
    except FileNotFoundError:
        pass

    return {
        **plan,
        "status": "success",
        "validation": validation,
        "backup_size_bytes": backup_path.stat().st_size,
        "final_size_bytes": db_path.stat().st_size,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/financials.db")
    parser.add_argument("--backup-dir", default=str(DEFAULT_BACKUP_DIR))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Print migration plan only")
    mode.add_argument("--execute", action="store_true", help="Execute backup, rebuild, validation, and cutover")
    parser.add_argument("--output-path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_plan(Path(args.db_path), Path(args.backup_dir))
    result: Dict[str, Any]
    if args.execute:
        result = execute_optimization(plan)
    else:
        result = {
            **plan,
            "status": "dry_run",
            "note": "No files modified. Re-run with --execute to optimize.",
        }
    content = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_path).write_text(content + "\n", encoding="utf-8")
    else:
        print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
