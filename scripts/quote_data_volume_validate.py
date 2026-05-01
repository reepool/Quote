#!/usr/bin/env python3
"""Post-migration validation for the Quote data volume."""

from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path
from typing import Iterable


def command_text(args: Iterable[str]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in args)


def run(args: list[str], timeout: int = 60) -> subprocess.CompletedProcess[str] | None:
    print(f"$ {command_text(args)}")
    try:
        result = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        print(f"SKIP: command not found: {args[0]}")
        return None
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT after {timeout}s")
        return None
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip())
    if result.returncode != 0:
        print(f"[exit {result.returncode}]")
    return result


def section(title: str) -> None:
    print()
    print(f"## {title}")


def sqlite_query(db_path: Path, sql: str, timeout: int) -> None:
    if not db_path.exists():
        print(f"missing: {db_path}")
        return
    run(["sqlite3", str(db_path), sql], timeout=timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--integrity-timeout", type=int, default=300)
    parser.add_argument("--nas-write-test", action="store_true")
    args = parser.parse_args()

    root = args.project_root.resolve()
    data_dir = root / "data"
    quotes_db = data_dir / "quotes.db"
    research_db = data_dir / "research.db"
    pve_bak = data_dir / "PVE-Bak"
    quote_bak = data_dir / "QuoteBak"
    backup_dir = pve_bak / "QuoteBak"

    section("Mount State")
    for path in (data_dir, pve_bak, quote_bak):
        run(["findmnt", "-R", "-T", str(path)])
    run(["df", "-h", str(data_dir), str(pve_bak), str(quote_bak)])

    section("Application Paths")
    for path in (
        quotes_db,
        research_db,
        data_dir / "download_progress.json",
        data_dir / "reports",
        data_dir / "backups",
        data_dir / "filings",
        pve_bak,
        quote_bak,
    ):
        status = "OK" if path.exists() else "MISSING"
        print(f"{status}\t{path}")

    section("SQLite Integrity And Smoke Counts")
    sqlite_query(
        quotes_db,
        (
            "PRAGMA integrity_check; "
            "SELECT 'daily_quotes', COUNT(*) FROM daily_quotes; "
            "SELECT 'instruments', COUNT(*) FROM instruments;"
        ),
        args.integrity_timeout,
    )
    sqlite_query(
        research_db,
        (
            "PRAGMA integrity_check; "
            "SELECT 'shareholder_snapshots', COUNT(*) FROM shareholder_snapshots; "
            "SELECT 'industry_memberships', COUNT(*) FROM industry_memberships; "
            "SELECT 'industry_taxonomy', COUNT(*) FROM industry_taxonomy;"
        ),
        args.integrity_timeout,
    )

    section("Backup Path")
    print(f"expected backup directory: {backup_dir}")
    if args.nas_write_test:
        backup_dir.mkdir(parents=True, exist_ok=True)
        probe = backup_dir / ".quote_data_volume_write_test"
        probe.write_text("ok\n", encoding="utf-8")
        print(f"write-test created: {probe}")
        probe.unlink()
        print("write-test removed")
    else:
        print("SKIP: pass --nas-write-test to create and remove a small probe file")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
