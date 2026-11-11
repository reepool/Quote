"""Operator script: backfill local_core_mapping lineage on legacy fact rows
(openspec: backfill-financial-fact-lineage).

Legacy akshare imports (2026-05 era) stamped lineage per the then-catalog,
leaving a large unstamped population. Readiness admission drops unstamped
rows, so reconciliation re-targets those instruments forever while the
fetch layer's unchanged-file skip prevents re-stamping. This script stamps
unstamped hot/history rows whose source field matches the CURRENT approved
catalog, using the import path's own resolver and the storage layer's own
serialization/hash semantics. Keyset-paginated batches, per-source backup
tables, idempotent reruns, full rollback.

Subcommands: check / apply (--dry-run) / verify / rollback.
Run inside a verified no-writer window for data/financials.db.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.financial_source_field_mapping import MAPPING_VERSION  # noqa: E402
from research.financial_statements_sync import _local_core_mapping_lineage  # noqa: E402
from research.storage import ResearchStorageManager  # noqa: E402

BACKUP_HOT = "financial_numeric_facts_lineage_backfill_202611_hot"
BACKUP_HISTORY = "financial_numeric_facts_lineage_backfill_202611_history"
HASH_EXCLUDED_KEYS = (
    "dimensions_hash",
    "ingestion_run_id",
    "created_at",
    "updated_at",
    "row_hash",
    "row_version",
)
INTERFACES = ("sina_report", "ths_report")
SOURCES = (
    ("hot", "financial_numeric_facts_hot", BACKUP_HOT),
    ("history", "financial_numeric_facts_history", BACKUP_HISTORY),
)


def _connect(db: Path, readonly: bool = False) -> sqlite3.Connection:
    uri = f"file:{db}?mode=ro" if readonly else f"file:{db}"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _storage(db: Path) -> ResearchStorageManager:
    """Real storage manager over the financial db, for parity lookups
    (industry membership / company profile via the exact service getters)."""
    manager = ResearchStorageManager.__new__(ResearchStorageManager)
    manager.db_path = str(db)
    manager.financials_db_path = str(db)
    manager._active_db_path = None
    manager._db_route_state = threading.local()
    manager.change_watermark_config = {}
    manager.financial_readiness_narrow_rows = False
    return manager


def _profile_resolver(storage: ResearchStorageManager):
    """Per-instrument statement profile, resolved exactly like the sync
    service loaders do (same storage getters + same resolver)."""
    from research.financial_statement_profile import resolve_financial_statement_profile

    cache: dict[str, str | None] = {}

    def resolve(instrument_id: str) -> str | None:
        if instrument_id in cache:
            return cache[instrument_id]
        industry = None
        company = None
        try:
            industry = storage.get_industry_membership(instrument_id, include_snapshot=False)
        except Exception:  # noqa: BLE001 - profile lookup is best-effort parity
            industry = None
        try:
            company = storage.get_company_profile(instrument_id, include_snapshot=False)
        except Exception:  # noqa: BLE001
            company = None
        try:
            resolution = resolve_financial_statement_profile(
                industry_membership=industry,
                company_profile=company,
                instrument={"instrument_id": instrument_id},
            )
            cache[instrument_id] = getattr(resolution, "profile", None)
        except Exception:  # noqa: BLE001
            cache[instrument_id] = None
        return cache[instrument_id]

    return resolve


def _new_row_hash(manager: ResearchStorageManager, row: sqlite3.Row, new_raw_fact: dict) -> str:
    """row_hash exactly like _financial_numeric_fact_row_hash: over parsed
    dicts (dimensions_json / raw_fact_json deserialized), minus excluded keys."""
    payload = dict(row)
    payload.pop("rowid", None)
    payload["dimensions_json"] = manager._deserialize_json(payload.get("dimensions_json")) or {}
    payload["raw_fact_json"] = new_raw_fact
    for key in HASH_EXCLUDED_KEYS:
        payload.pop(key, None)
    return manager._semantic_hash(payload)


def _stamp_row(manager, row, resolve_profile) -> tuple[str, str] | None | str:
    """Classify and optionally re-stamp one row against the current catalog.

    Returns:
      tuple(new_raw_json, new_hash) - stamp/refresh needed
      None - leave unchanged (already current / unmapped / other interface)
      'stale_unmapped' - had a superseded stamp the current catalog withdrew
    """
    raw = row["raw_fact_json"]
    if not raw:
        return None
    rf = json.loads(raw)
    if not isinstance(rf, dict):
        return None
    lcm = rf.get("local_core_mapping")
    if isinstance(lcm, dict) and lcm.get("mapping_version") == MAPPING_VERSION:
        return None  # already current
    interface = rf.get("akshare_statement_interface")
    if interface not in INTERFACES:
        return None
    lineage = _local_core_mapping_lineage(
        statement_interface=interface,
        fact_name=row["fact_name"],
        profile=resolve_profile(row["instrument_id"]),
        mapping_version=MAPPING_VERSION,
    )
    if lineage is None:
        return "stale_unmapped" if isinstance(lcm, dict) else None
    rf["local_core_mapping"] = lineage
    return manager._json_text(rf), _new_row_hash(manager, row, rf)


def cmd_check(args: argparse.Namespace) -> int:
    db = Path(args.db).resolve()
    conn = _connect(db, readonly=True)
    print(f"db: {db} ({db.stat().st_size / 1e9:.1f} GB)")
    for _, table, _backup in SOURCES:
        started = time.monotonic()
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        unstamped = conn.execute(
            f"SELECT COUNT(*) FROM {table} "
            "WHERE raw_fact_json IS NULL OR raw_fact_json NOT LIKE '%local_core_mapping%'"
        ).fetchone()[0]
        print(f"{table}: total={total:,} unstamped={unstamped:,} ({time.monotonic() - started:.0f}s)")
    conn.close()
    print("\nWriter checklist: no financial job in /api/v1/system/worker-runs;")
    print("incremental starts 21:45; reconciliation Sunday; l1/broker manual.")
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    db = Path(args.db).resolve()
    manager = _storage(db)
    storage = _storage(db)
    resolve_profile = _profile_resolver(storage)
    conn = _connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    for _src, table, backup in SOURCES:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {backup} (_rowid INTEGER PRIMARY KEY, "
            "raw_fact_json TEXT, row_hash TEXT)"
        )

    totals = {
        "scanned": 0,
        "stamped": 0,
        "stale_unmapped": 0,
        "unmapped": 0,
        "other_interface": 0,
        "null_raw": 0,
    }
    batches_done = 0
    started = time.monotonic()
    for src, table, backup in SOURCES:
        if args.table not in ("both", src):
            continue
        last_rowid = 0
        while True:
            # Superset prefilter: unstamped rows OR rows not carrying the
            # current version string (stale-version stamps). Python decides
            # precisely per row.
            rows = conn.execute(
                f"SELECT rowid, * FROM {table} WHERE rowid > ? "
                "AND (raw_fact_json IS NULL OR raw_fact_json NOT LIKE ?) "
                "ORDER BY rowid LIMIT ?",
                (last_rowid, f"%{MAPPING_VERSION}%", args.batch_rows),
            ).fetchall()
            if not rows:
                break
            last_rowid = rows[-1]["rowid"]
            updates: list[tuple[str, str, int]] = []
            for row in rows:
                totals["scanned"] += 1
                if not row["raw_fact_json"]:
                    totals["null_raw"] += 1
                    continue
                result = _stamp_row(manager, row, resolve_profile)
                if result is None:
                    try:
                        interface_val = json.loads(row["raw_fact_json"]).get(
                            "akshare_statement_interface"
                        )
                    except Exception:  # noqa: BLE001
                        interface_val = None
                    totals["other_interface" if interface_val not in INTERFACES else "unmapped"] += 1
                    continue
                if result == "stale_unmapped":
                    totals["stale_unmapped"] += 1
                    continue
                updates.append((result[0], result[1], row["rowid"]))
                totals["stamped"] += 1
            if updates and not args.dry_run:
                conn.execute("BEGIN")
                conn.executemany(
                    f"INSERT OR IGNORE INTO {backup} (_rowid, raw_fact_json, row_hash) "
                    f"SELECT rowid, raw_fact_json, row_hash FROM {table} WHERE rowid=?",
                    [(u[2],) for u in updates],
                )
                conn.executemany(
                    f"UPDATE {table} SET raw_fact_json=?, row_hash=? WHERE rowid=?",
                    updates,
                )
                conn.commit()
            batches_done += 1
            print(
                f"\r[{src}] scanned={totals['scanned']:,} stamped={totals['stamped']:,} "
                f"unmapped={totals['unmapped']:,} other={totals['other_interface']:,} "
                f"elapsed={time.monotonic() - started:.0f}s   ",
                end="",
                flush=True,
            )
            if args.limit_batches and batches_done >= args.limit_batches:
                break
        if args.limit_batches and batches_done >= args.limit_batches:
            break
    print()
    print(f"TOTALS: {totals} elapsed={time.monotonic() - started:.0f}s dry_run={args.dry_run}")
    conn.close()
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    db = Path(args.db).resolve()
    storage = _storage(db)
    resolve_profile = _profile_resolver(storage)
    conn = _connect(db, readonly=True)
    manager = _storage(db)
    bad = 0
    checked = 0
    stale_seen = 0
    for _src, table, _backup in SOURCES:
        rows = conn.execute(
            f"SELECT rowid, * FROM {table} WHERE raw_fact_json LIKE '%local_core_mapping%' "
            f"AND rowid % 997 = 0 LIMIT {args.sample}"
        ).fetchall()
        for row in rows:
            checked += 1
            rf = json.loads(row["raw_fact_json"])
            stamp = rf.get("local_core_mapping")
            if not isinstance(stamp, dict) or "akshare_statement_interface" not in rf:
                continue
            if stamp.get("mapping_version") != MAPPING_VERSION:
                stale_seen += 1  # pre-existing superseded stamp - expected before backfill
                continue
            lineage = _local_core_mapping_lineage(
                statement_interface=rf["akshare_statement_interface"],
                fact_name=row["fact_name"],
                profile=resolve_profile(row["instrument_id"]),
                mapping_version=MAPPING_VERSION,
            )
            if lineage is not None and lineage != stamp:
                bad += 1
                if bad <= 5:
                    print(f"MISMATCH {table} rowid={row['rowid']} {row['instrument_id']} {row['fact_name']}")
    print(f"verify: checked={checked} mismatches={bad} stale_version_seen={stale_seen}")
    try:
        from utils.config_manager import config_manager

        mgr = ResearchStorageManager(config_manager.get_research_config())
        cfg = config_manager.get_research_config()
        req = (
            cfg.modules.get("financial_statements", {}).get("readiness", {}).get("required_core_facts", [])
        )
        with mgr.financial_database_scope():
            r = mgr.get_financial_local_core_facts(
                "000001.SZ",
                report_period="2024-03-31",
                requested_canonical_facts=req,
                mapping_version=MAPPING_VERSION,
                include_history=True,
            )
        print(
            f"probe 000001.SZ 2024-03-31: ready={r.get('ready')} facts={len(r.get('facts') or {})} "
            f"missing={len(r.get('missing_fields') or [])}"
        )
    except Exception as exc:  # noqa: BLE001 - probe is advisory output
        print(f"probe failed: {exc}")
    conn.close()
    return 1 if bad else 0


def cmd_rollback(args: argparse.Namespace) -> int:
    db = Path(args.db).resolve()
    conn = _connect(db)
    conn.execute("PRAGMA busy_timeout=30000")
    restored = 0
    for _src, table, backup in SOURCES:
        count = conn.execute(f"SELECT COUNT(*) FROM {backup}").fetchone()[0]
        if not count:
            continue
        conn.execute("BEGIN")
        conn.execute(
            f"UPDATE {table} SET raw_fact_json=(SELECT b.raw_fact_json FROM {backup} b WHERE b._rowid={table}.rowid), "
            f"row_hash=(SELECT b.row_hash FROM {backup} b WHERE b._rowid={table}.rowid) "
            f"WHERE rowid IN (SELECT _rowid FROM {backup})"
        )
        conn.execute(f"DELETE FROM {backup}")
        conn.commit()
        restored += count
        print(f"restored {count} rows into {table} (backup rows cleared)")
    print(f"rollback complete: {restored} rows restored")
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
    sub.choices["apply"].add_argument("--batch-rows", type=int, default=20000)
    sub.choices["apply"].add_argument("--dry-run", action="store_true")
    sub.choices["apply"].add_argument("--limit-batches", type=int, default=0)
    sub.choices["apply"].add_argument("--table", choices=("hot", "history", "both"), default="both")
    sub.choices["verify"].add_argument("--sample", type=int, default=500)
    args = parser.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
