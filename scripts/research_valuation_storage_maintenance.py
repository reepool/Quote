#!/usr/bin/env python3
"""Inspect and optionally clean valuation history storage.

The default mode is read-only. Destructive cleanup requires
`--delete-parameter-hash` and `--confirm-delete`. Hash rewrites require
`--rewrite-from-parameter-hash`, `--rewrite-to-parameter-hash`, and
`--confirm-rewrite`.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.storage import ResearchStorageManager


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


def _rewrite_parameter_hash(
    conn: sqlite3.Connection,
    *,
    source_hash: str,
    target_hash: str,
) -> int:
    conflict = conn.execute(
        """
        SELECT COUNT(*)
        FROM valuation_history src
        JOIN valuation_history dst
          ON dst.instrument_id = src.instrument_id
         AND dst.as_of_date = src.as_of_date
         AND dst.calc_method = src.calc_method
         AND dst.calc_version = src.calc_version
         AND dst.parameter_hash = ?
        WHERE src.parameter_hash = ?
        """,
        (target_hash, source_hash),
    ).fetchone()[0]
    if int(conflict or 0) > 0:
        raise SystemExit(
            "refusing to rewrite because target hash already has conflicting "
            f"rows: {conflict}; delete or inspect duplicates first"
        )

    cur = conn.execute(
        """
        UPDATE valuation_history
        SET parameter_hash = ?
        WHERE parameter_hash = ?
        """,
        (target_hash, source_hash),
    )
    return int(cur.rowcount or 0)


def _compact_valuation_history_details(
    conn: sqlite3.Connection,
    *,
    batch_size: int,
    limit: int,
) -> dict[str, int]:
    total_seen = 0
    total_updated = 0
    total_old_bytes = 0
    total_new_bytes = 0
    started = time.monotonic()

    while True:
        remaining = max(0, limit - total_seen) if limit > 0 else batch_size
        if limit > 0 and remaining == 0:
            break
        current_batch_size = min(batch_size, remaining) if limit > 0 else batch_size
        rows = conn.execute(
            """
            SELECT rowid, details_json
            FROM valuation_history
            WHERE instr(details_json, '"latest_financial_report_period"') > 0
               OR instr(details_json, '"valuation_input"') > 0
               OR instr(details_json, '"availability_dates"') > 0
            LIMIT ?
            """,
            (current_batch_size,),
        ).fetchall()
        if not rows:
            break

        updates: list[tuple[str, int]] = []
        for row in rows:
            total_seen += 1
            rowid = int(row["rowid"])
            old_text = str(row["details_json"] or "{}")
            try:
                payload = json.loads(old_text)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            compact_payload = ResearchStorageManager.compact_valuation_history_details_payload(
                payload
            )
            new_text = json.dumps(
                compact_payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            if new_text == old_text:
                continue
            updates.append((new_text, rowid))
            total_updated += 1
            total_old_bytes += len(old_text.encode("utf-8"))
            total_new_bytes += len(new_text.encode("utf-8"))

        if updates:
            conn.executemany(
                """
                UPDATE valuation_history
                SET details_json = ?
                WHERE rowid = ?
                """,
                updates,
            )
            conn.commit()

        elapsed = time.monotonic() - started
        saved = total_old_bytes - total_new_bytes
        print(
            "compacted valuation_history details: "
            f"seen={total_seen}, updated={total_updated}, "
            f"saved_bytes={saved}, elapsed={elapsed:.1f}s",
            flush=True,
        )

    return {
        "seen": total_seen,
        "updated": total_updated,
        "old_bytes": total_old_bytes,
        "new_bytes": total_new_bytes,
        "saved_bytes": total_old_bytes - total_new_bytes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/valuation.db")
    parser.add_argument("--delete-parameter-hash")
    parser.add_argument("--confirm-delete", action="store_true")
    parser.add_argument("--rewrite-from-parameter-hash")
    parser.add_argument("--rewrite-to-parameter-hash")
    parser.add_argument("--confirm-rewrite", action="store_true")
    parser.add_argument("--compact-valuation-history-details", action="store_true")
    parser.add_argument("--confirm-compact", action="store_true")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--limit", type=int, default=0)
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

        if args.rewrite_from_parameter_hash or args.rewrite_to_parameter_hash:
            if not (args.rewrite_from_parameter_hash and args.rewrite_to_parameter_hash):
                raise SystemExit(
                    "hash rewrite requires both --rewrite-from-parameter-hash "
                    "and --rewrite-to-parameter-hash"
                )
            if not args.confirm_rewrite:
                raise SystemExit(
                    "refusing to rewrite without --confirm-rewrite; "
                    "rerun after backup if the hash migration is intended"
                )
            rewritten = _rewrite_parameter_hash(
                conn,
                source_hash=args.rewrite_from_parameter_hash,
                target_hash=args.rewrite_to_parameter_hash,
            )
            conn.commit()
            print(f"rewritten valuation_history rows: {rewritten}")

        if args.compact_valuation_history_details:
            if not args.confirm_compact:
                raise SystemExit(
                    "refusing to compact without --confirm-compact; "
                    "rerun after backup if details migration is intended"
                )
            summary = _compact_valuation_history_details(
                conn,
                batch_size=max(1, int(args.batch_size)),
                limit=max(0, int(args.limit)),
            )
            print(f"valuation_history details compact summary: {summary}")

        if args.vacuum:
            print("running VACUUM; this can take a while on large valuation.db")
            conn.execute("VACUUM")
            print("VACUUM finished")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
