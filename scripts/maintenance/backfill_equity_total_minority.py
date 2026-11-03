"""Backfill equity_total/minority_equity on wide financial fact tables.

Strategy: one bounded pass over the numeric facts layer (hot + history)
builds a temp (instrument_id, report_period) -> (equity_total, minority)
table, then each wide table is updated via a join. Safe to re-run; only
fills NULL targets. Reports coverage and balance identity pass rate.

Usage:
    python scripts/maintenance/backfill_equity_total_minority.py \
        [--db data/financials.db] [--dry-run]
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

TARGET_TABLES = (
    "financial_facts",
    "financial_core_facts_hot",
    "financial_core_facts_history",
)

NUMERIC_SOURCES = ("financial_numeric_facts_hot", "financial_numeric_facts_history")


def _build_temp(conn: sqlite3.Connection) -> int:
    matches = " OR ".join(
        "(n.canonical_fact_name = '{k}' OR n.fact_name = '{k}')".format(k=k)
        for k in ("equity_total", "TotalEquity", "股东权益合计", "所有者权益合计")
    )
    minority = " OR ".join(
        "(n.canonical_fact_name = '{k}' OR n.fact_name = '{k}')".format(k=k)
        for k in ("minority_equity", "少数股东权益", "少数股东权益合计")
    )
    conn.execute("DROP TABLE IF EXISTS temp.eq_fill")
    conn.execute(
        """
        CREATE TEMP TABLE eq_fill AS
        SELECT instrument_id, report_period,
               MAX(CASE WHEN ({total_match}) THEN fact_value END) AS equity_total,
               MAX(CASE WHEN ({minority_match}) THEN fact_value END) AS minority_equity
        FROM (
            SELECT instrument_id, report_period, canonical_fact_name, fact_name, fact_value
            FROM financial_numeric_facts_hot
            UNION ALL
            SELECT instrument_id, report_period, canonical_fact_name, fact_name, fact_value
            FROM financial_numeric_facts_history
        ) n
        WHERE ({total_match}) OR ({minority_match})
        GROUP BY instrument_id, report_period
        """.format(total_match=matches, minority_match=minority)
    )
    conn.execute(
        "CREATE INDEX temp.idx_eq_fill ON eq_fill(instrument_id, report_period)"
    )
    count = conn.execute("SELECT COUNT(*) FROM temp.eq_fill").fetchone()[0]
    return int(count)


def _update_target(conn: sqlite3.Connection, target: str, column: str) -> int:
    cur = conn.execute(
        f"""
        UPDATE {target} AS t
        SET {column} = (
            SELECT f.{column} FROM temp.eq_fill f
            WHERE f.instrument_id = t.instrument_id
              AND f.report_period = t.report_period
        )
        WHERE t.{column} IS NULL
          AND EXISTS (
            SELECT 1 FROM temp.eq_fill f
            WHERE f.instrument_id = t.instrument_id
              AND f.report_period = t.report_period
              AND f.{column} IS NOT NULL
          )
        """
    )
    conn.commit()
    return max(cur.rowcount, 0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/financials.db")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"database not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        for target in TARGET_TABLES:
            columns = {
                row[1] for row in conn.execute(f"PRAGMA table_info({target})")
            }
            if not {"equity_total", "minority_equity"} <= columns:
                print(f"skip {target}: additive columns not present")
                return 1

        started = time.time()
        fill_rows = _build_temp(conn)
        print(f"temp eq_fill rows={fill_rows} built in {time.time()-started:.1f}s")

        total_updated = 0
        for target in TARGET_TABLES:
            for column in ("equity_total", "minority_equity"):
                if args.dry_run:
                    pending = conn.execute(
                        f"""
                        SELECT COUNT(*) FROM {target} t
                        JOIN temp.eq_fill f
                          ON f.instrument_id = t.instrument_id
                         AND f.report_period = t.report_period
                        WHERE t.{column} IS NULL AND f.{column} IS NOT NULL
                        """
                    ).fetchone()[0]
                    print(f"[dry-run] {target}.{column}: fillable={pending}")
                    continue
                updated = _update_target(conn, target, column)
                total_updated += updated
                print(f"{target}.{column}: updated={updated}")

        for target in TARGET_TABLES:
            row = conn.execute(
                f"""
                SELECT COUNT(*),
                       SUM(equity_total IS NOT NULL),
                       SUM(minority_equity IS NOT NULL)
                FROM {target}
                """
            ).fetchone()
            if row and row[0]:
                print(
                    f"{target}: rows={row[0]} equity_total={row[1]} "
                    f"minority_equity={row[2]}"
                )

        identity = conn.execute(
            """
            SELECT COUNT(*),
                   SUM(CASE WHEN equity_total IS NOT NULL
                        AND ABS(total_assets - total_liabilities - equity_total)
                            <= 0.01 * ABS(total_assets)
                       THEN 1 ELSE 0 END)
            FROM financial_facts
            WHERE total_assets > 0
            """
        ).fetchone()
        if identity and identity[0]:
            print(
                f"identity(1%): total={identity[0]} ok={identity[1]} "
                f"rate={identity[1] / identity[0]:.4f}"
            )
        if not args.dry_run:
            print(f"total rows updated: {total_updated}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
