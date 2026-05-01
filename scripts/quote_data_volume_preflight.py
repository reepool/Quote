#!/usr/bin/env python3
"""Non-destructive preflight for the Quote data volume migration."""

from __future__ import annotations

import argparse
import shlex
import shutil
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


def print_file_sizes(path: Path) -> None:
    if not path.exists():
        print(f"missing: {path}")
        return
    for child in sorted(path.iterdir()):
        if child.is_file():
            print(f"{child.name}\t{child.stat().st_size}")


def sqlite_check(db_path: Path, timeout: int) -> None:
    if not db_path.exists():
        print(f"missing: {db_path}")
        return
    run(["sqlite3", str(db_path), "PRAGMA integrity_check;"], timeout=timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--source-dir", type=Path, default=Path("data_bak"))
    parser.add_argument("--device", default="/dev/sda3")
    parser.add_argument("--temp-mount", type=Path, default=Path("/home/python/sd"))
    parser.add_argument("--skip-integrity", action="store_true")
    parser.add_argument("--integrity-timeout", type=int, default=300)
    parser.add_argument("--run-rsync-dry-run", action="store_true")
    args = parser.parse_args()

    root = args.project_root.resolve()
    source_dir = args.source_dir if args.source_dir.is_absolute() else root / args.source_dir
    data_dir = root / "data"

    section("Mount State")
    for path in (data_dir, source_dir, args.temp_mount):
        run(["findmnt", "-R", "-T", str(path)])
    run(["lsblk", "-f", args.device])
    run(["df", "-h", str(data_dir), str(source_dir), str(args.temp_mount)])

    section("Source Size And Files")
    run(["du", "-xh", "-d", "1", str(source_dir)])
    print_file_sizes(source_dir)

    section("Open Database Handles")
    db_files = [
        source_dir / "quotes.db",
        source_dir / "quotes.db-wal",
        source_dir / "research.db",
        source_dir / "research.db-wal",
    ]
    if shutil.which("lsof"):
        run(["lsof", *[str(path) for path in db_files if path.exists()]])
    else:
        print("SKIP: lsof not found")

    section("WAL And SHM Files")
    for pattern in ("*.db-wal", "*.db-shm"):
        for path in sorted(source_dir.glob(pattern)):
            print(f"{path.name}\t{path.stat().st_size}")

    section("SQLite Integrity")
    if args.skip_integrity:
        print("SKIP: --skip-integrity set")
    else:
        sqlite_check(source_dir / "quotes.db", args.integrity_timeout)
        sqlite_check(source_dir / "research.db", args.integrity_timeout)

    section("Dry-Run Copy Command")
    rsync_cmd = [
        "rsync",
        "-aHAXn",
        "--numeric-ids",
        "--one-file-system",
        f"{source_dir}/",
        f"{args.temp_mount}/",
    ]
    print(command_text(rsync_cmd))
    if not shutil.which("rsync"):
        print("WARN: rsync not found; fallback copy command for execution:")
        print(command_text(["cp", "-a", "-x", f"{source_dir}/.", f"{args.temp_mount}/"]))
    if args.run_rsync_dry_run:
        run(rsync_cmd, timeout=3600)

    section("Fstab")
    fstab = Path("/etc/fstab")
    if fstab.exists():
        print(fstab.read_text(encoding="utf-8", errors="replace").rstrip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
