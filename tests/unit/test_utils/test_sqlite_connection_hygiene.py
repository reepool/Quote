"""SQLite connection hygiene guards (change: enforce-sqlite-connection-closing).

Two guards:
1. a pattern scan failing on any new bare `with sqlite3.connect(` in
   production code (exemptions only via the justified allowlist below);
2. an fd regression test exercising a converted tier-A long-running path
   (business profile semantic runtime resolver builder) repeatedly.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator

import pytest

ROOT = Path(__file__).resolve().parents[3]

# Production code directories and root modules the pattern guard scans.
SCAN_DIRS = ("research", "scheduler", "api", "database", "utils", "data_sources")
SCAN_ROOT_FILES = (
    "data_manager.py",
    "main.py",
    "instrument_master_governance.py",
    "proxy_patch_bootstrap.py",
)

# Exemptions: {repo-relative path: {line numbers}} plus a justification per
# entry. Entries must carry a reason and the list is expected only to shrink.
ALLOWLIST: dict[str, dict[int, str]] = {
    # (none - all production sites were converted in this change)
}

_BARE_PATTERN = "with sqlite3.connect("


def _iter_production_files() -> Iterator[Path]:
    for directory in SCAN_DIRS:
        for path in (ROOT / directory).rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            yield path
    for name in SCAN_ROOT_FILES:
        path = ROOT / name
        if path.is_file():
            yield path


def test_no_bare_sqlite_with_connect_in_production_code() -> None:
    offenders: list[str] = []
    for path in _iter_production_files():
        relative = str(path.relative_to(ROOT))
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _BARE_PATTERN not in line:
                continue
            reason = ALLOWLIST.get(relative, {}).get(lineno)
            if reason:
                continue
            offenders.append(f"{relative}:{lineno}")
    assert not offenders, (
        "bare 'with sqlite3.connect(' never closes the connection "
        "(incident 2026-11-01); wrap in contextlib.closing or route through "
        "a finally-close factory. Offenders:\n" + "\n".join(offenders)
    )


def test_allowlist_entries_exist_with_reasons() -> None:
    for relative, entries in ALLOWLIST.items():
        path = ROOT / relative
        assert path.is_file(), f"allowlist entry points at missing file: {relative}"
        for lineno, reason in entries.items():
            assert isinstance(reason, str) and reason.strip(), (
                f"{relative}:{lineno} lacks a justification comment"
            )


def test_semantic_runtime_resolver_leaks_no_file_handles(tmp_path: Path) -> None:
    """Tier-A fd regression: the converted resolver builder must not
    accumulate handles across repeated builds (incident-fix pattern)."""
    if sys.platform != "linux" or not os.path.isdir("/proc/self/fd"):
        pytest.skip("fd accounting requires Linux /proc")

    from research.business_profile_semantic_runtime import (
        build_business_profile_counterparty_resolver,
    )

    db_path = tmp_path / "hygiene.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE instruments (
                instrument_id TEXT PRIMARY KEY,
                listed_date TEXT,
                delisted_date TEXT,
                type TEXT,
                exchange TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE company_profiles (
                instrument_id TEXT PRIMARY KEY,
                data_as_of TEXT,
                profile_json TEXT
            )
            """
        )
        conn.commit()

    @contextmanager
    def get_connection() -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    storage = SimpleNamespace(
        quotes_db_path=str(db_path),
        get_connection=get_connection,
        _apply_pragmas=lambda conn: None,
    )

    build_business_profile_counterparty_resolver(storage)  # warm-up
    baseline = len(os.listdir("/proc/self/fd"))
    for _ in range(25):
        build_business_profile_counterparty_resolver(storage)
    after = len(os.listdir("/proc/self/fd"))
    assert after <= baseline + 2, (
        f"file handles leaked across resolver builds: {baseline} -> {after}"
    )
