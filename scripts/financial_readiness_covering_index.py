"""Operator script: covering index for financial readiness queries
(openspec: accelerate-financial-readiness-queries, tasks 3.2/3.3).

Run ONLY inside the verified maintenance window documented in
docs/development/financial_readiness_covering_index_runbook.md.

Subcommands:
  check     Preflight: current index state, free disk space, running financial
            tasks checklist (must all be clear before --apply).
  apply     Create the covering index (long transaction; WAL readers unaffected,
            writers block for the duration). Interruptible: SQLite rolls back
            an interrupted CREATE INDEX; rerun resumes from scratch.
  verify    EXPLAIN QUERY PLAN for the narrow readiness query - must show the
            covering index without table row lookups.
  rollback  Drop the index (no data impact).
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
import time
from pathlib import Path

INDEX_NAME = "idx_financial_numeric_facts_hot_readiness_covering"
INDEX_DDL = (
    f"CREATE INDEX {INDEX_NAME} ON financial_numeric_facts_hot "
    "(instrument_id, report_period, canonical_fact_name, updated_at, fact_value, unit)"
)
NARROW_QUERY = (
    "SELECT instrument_id, report_period, fact_name, canonical_fact_name, "
    "fact_value, unit, updated_at, raw_fact_json "
    "FROM ("
    "  SELECT instrument_id, report_period, fact_name, canonical_fact_name,"
    "         fact_value, unit, updated_at, raw_fact_json FROM financial_numeric_facts_hot"
    "  UNION ALL"
    "  SELECT instrument_id, report_period, fact_name, canonical_fact_name,"
    "         fact_value, unit, updated_at, raw_fact_json FROM financial_numeric_facts_history"
    ") WHERE instrument_id = ? AND report_period = ? "
    "AND canonical_fact_name IN ('revenue','net_income_parent')"
)


def _db_path(args) -> Path:
    return Path(args.db or "data/financials.db").resolve()


def _progress(stage: str, done: int, total: int) -> None:
    print(f"\r[{stage}] {done}/{total} pages", end="", flush=True)


def cmd_check(args) -> int:
    db = _db_path(args)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    indexes = [
        r[0]
        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='financial_numeric_facts_hot'")
    ]
    print(f"db: {db} ({db.stat().st_size / 1e9:.1f} GB)")
    print(f"existing hot-table indexes: {indexes}")
    print(f"covering index present: {INDEX_NAME in indexes}")
    _total, free = shutil.disk_usage(db)
    print(f"disk free: {free / 1e9:.1f} GB (index estimate ~1-2 GB)")
    conn.close()
    print("\nWindow checklist (all must be clear, see runbook):")
    for line in (
        "  [ ] financial_disclosure_incremental_sync NOT running (21:45 nightly, 4h cap)",
        "  [ ] financial_disclosure_reconciliation_sync NOT running (Sun 09:30, 8h cap)",
        "  [ ] financial_l1_full_import NOT running (manual_only)",
        "  [ ] broker_risk_control_incremental_sync NOT running (manual_only)",
        "  [ ] no /run of a financial writer task in flight",
        "  [ ] disk free >= 3 GB",
    ):
        print(line)
    return 0


def cmd_apply(args) -> int:
    db = _db_path(args)
    conn = sqlite3.connect(db, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    total = conn.execute("SELECT COUNT(*) FROM financial_numeric_facts_hot").fetchone()[0]
    print(f"creating {INDEX_NAME} over {total} rows ...")
    started = time.monotonic()
    conn.executescript(
        f"""
        CREATE INDEX IF NOT EXISTS {INDEX_NAME} ON financial_numeric_facts_hot
        (instrument_id, report_period, canonical_fact_name, updated_at, fact_value, unit);
        """
    )
    elapsed = time.monotonic() - started
    size = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", (INDEX_NAME,)
    ).fetchone()[0]
    print(f"done in {elapsed:.0f}s (present={bool(size)})")
    print("run verify + the equivalent on financial_numeric_facts_history if ever needed:")
    print(f"  python {Path(__file__).name} verify --db {db}")
    conn.close()
    return 0


def cmd_verify(args) -> int:
    db = _db_path(args)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    print("== EXPLAIN QUERY PLAN (narrow readiness query, hot branch) ==")
    for r in conn.execute("EXPLAIN QUERY PLAN " + NARROW_QUERY, ("000001.SZ", "2026-06-30")):
        print(" ", r[3])
    conn.close()
    return 0


def cmd_rollback(args) -> int:
    db = _db_path(args)
    conn = sqlite3.connect(db, isolation_level=None)
    conn.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
    print(f"dropped {INDEX_NAME}")
    conn.close()
    return 0


def main() -> int:
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument("--db", default="data/financials.db")
    parser = argparse.ArgumentParser(description=__doc__, parents=[shared])
    sub = parser.add_subparsers(dest="command", required=True)
    for name, fn in (("check", cmd_check), ("apply", cmd_apply), ("verify", cmd_verify), ("rollback", cmd_rollback)):
        p = sub.add_parser(name, parents=[shared], help=fn.__doc__)
        p.set_defaults(fn=fn)
    args = parser.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
