#!/usr/bin/env python3
"""Audit HKEX lifecycle states against current official source snapshots."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_sources.hkex_instrument_master import (  # noqa: E402
    HKEXNewsStockListProvider,
    HKEXSuspensionReportProvider,
)


def _local_ids(conn: sqlite3.Connection, status: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT instrument_id
        FROM instruments
        WHERE exchange = 'HKEX' AND type = 'stock' AND status = ?
        """,
        (status,),
    ).fetchall()
    return {str(row[0]) for row in rows}


def main() -> int:
    conn = sqlite3.connect(str(REPO_ROOT / "data" / "quotes.db"))
    local_delisted = _local_ids(conn, "delisted")
    local_suspended = _local_ids(conn, "suspended")

    active = HKEXNewsStockListProvider(
        "https://www.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json"
    ).fetch_html(lifecycle_status="active", timeout_sec=60)
    delisted = HKEXNewsStockListProvider(
        "https://www.hkexnews.hk/ncms/script/eds/inactivestock_sehk_e.json"
    ).fetch_html(lifecycle_status="delisted", timeout_sec=60)
    main_board = HKEXSuspensionReportProvider(
        "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/Exchange-Reports/Prolonged-Suspension-Status-Report/psuspenrep_mb.pdf",
        market="Main Board",
    ).fetch_pdf(timeout_sec=60)
    gem = HKEXSuspensionReportProvider(
        "https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/Exchange-Reports/Prolonged-Suspension-Status-Report/psuspenrep_gem.pdf",
        market="GEM",
    ).fetch_pdf(timeout_sec=60)

    active_ids = {row["instrument_id"] for row in active.rows}
    delisted_ids = {row["instrument_id"] for row in delisted.rows}
    suspended_ids = {row["instrument_id"] for row in main_board.rows + gem.rows}

    result = {
        "local_counts": {
            "delisted": len(local_delisted),
            "suspended": len(local_suspended),
        },
        "official_counts": {
            "active": len(active.rows),
            "delisted": len(delisted.rows),
            "suspended": len(suspended_ids),
        },
        "delisted_missing_from_official_delisted": sorted(local_delisted - delisted_ids),
        "delisted_also_in_active_or_suspended": sorted(local_delisted & (active_ids | suspended_ids)),
        "suspended_missing_from_official_suspended": sorted(local_suspended - suspended_ids),
        "suspended_missing_from_active": sorted(local_suspended - active_ids),
        "suspended_also_in_delisted": sorted(local_suspended & delisted_ids),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
