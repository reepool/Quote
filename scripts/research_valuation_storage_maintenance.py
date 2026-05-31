#!/usr/bin/env python3
"""Inspect and optionally clean valuation history storage.

The default mode is read-only. Destructive cleanup requires
`--delete-parameter-hash` and `--confirm-delete`.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _print_hash_summary(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT
            calc_method,
            calc_version,
            parameter_hash,
            COUNT(*) AS rows,
            COUNT(DISTINCT instrument_id) AS instruments,
            MIN(as_of_date) AS start_date,
            MAX(as_of_date) AS end_date
        FROM valuation_history
        GROUP BY calc_method, calc_version, parameter_hash
        ORDER BY rows DESC
        """
    ).fetchall()
    print("valuation_history parameter hashes:")
    for row in rows:
        print(
            "  "
            f"{row['calc_method']} / {row['calc_version']} / {row['parameter_hash']}: "
            f"rows={row['rows']}, instruments={row['instruments']}, "
            f"range={row['start_date']}~{row['end_date']}"
        )


def _print_size_summary(conn: sqlite3.Connection) -> None:
    page = conn.execute("PRAGMA page_count").fetchone()[0]
    free = conn.execute("PRAGMA freelist_count").fetchone()[0]
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    print(
        "sqlite pages: "
        f"page_count={page}, freelist_count={free}, "
        f"page_size={page_size}, file_bytes={page * page_size}, "
        f"free_bytes={free * page_size}"
    )
    try:
        rows = conn.execute(
            """
            SELECT name, SUM(pgsize) AS bytes, COUNT(*) AS pages
            FROM dbstat
            GROUP BY name
            ORDER BY bytes DESC
            LIMIT 12
            """
        ).fetchall()
    except sqlite3.DatabaseError as exc:
        print(f"dbstat unavailable: {exc}")
        return
    print("largest sqlite objects:")
    for row in rows:
        print(f"  {row['name']}: bytes={row['bytes']}, pages={row['pages']}")


def _delete_parameter_hash(conn: sqlite3.Connection, parameter_hash: str) -> int:
    cur = conn.execute(
        "DELETE FROM valuation_history WHERE parameter_hash = ?",
        (parameter_hash,),
    )
    return int(cur.rowcount or 0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/valuation.db")
    parser.add_argument("--delete-parameter-hash")
    parser.add_argument("--confirm-delete", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"valuation database not found: {db_path}")

    with _connect(db_path) as conn:
        _print_hash_summary(conn)
        _print_size_summary(conn)

        if args.delete_parameter_hash:
            if not args.confirm_delete:
                raise SystemExit(
                    "refusing to delete without --confirm-delete; "
                    "rerun after backup if the hash is obsolete"
                )
            deleted = _delete_parameter_hash(conn, args.delete_parameter_hash)
            conn.commit()
            print(f"deleted valuation_history rows: {deleted}")

        if args.vacuum:
            print("running VACUUM; this can take a while on large valuation.db")
            conn.execute("VACUUM")
            print("VACUUM finished")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
