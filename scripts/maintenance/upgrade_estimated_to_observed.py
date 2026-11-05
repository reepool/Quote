"""Upgrade statutory-fallback availability rows to observed dates.

After the truncation-aware survey rerun (wider page caps, delisted stocks
included), rows previously filled with the statutory estimate are
re-resolved against the evidence maps; any row that now has an observed
disclosure date is upgraded (observed beats the conservative bound).
Late-filer observed dates beyond the deadline are legitimate and kept.

Idempotent: only touches rows whose lineage still says
estimated_statutory_deadline.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from research.financial_available_date_backfill import (  # noqa: E402
    OBSERVED_EVIDENCE_SOURCES,
    _load_available_date_evidence,
    _json_object,
)

CST = ZoneInfo("Asia/Shanghai")


def main() -> int:
    db_path = Path("data/financials.db")
    if not db_path.exists():
        print(f"database not found: {db_path}")
        return 1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=60000")
        table_names = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        evidence = _load_available_date_evidence(conn, table_names)

        rows = conn.execute(
            """
            SELECT instrument_id, report_period, data_available_date,
                   publish_date, lineage_json
            FROM financial_facts
            WHERE lineage_json LIKE '%estimated_statutory_deadline%'
            """
        ).fetchall()

        upgraded = 0
        kept = 0
        no_evidence = 0
        for row in rows:
            lineage_full = _json_object(row["lineage_json"])
            entry = lineage_full.get("data_available_date_backfill", {})
            if entry.get("source") != "estimated_statutory_deadline":
                kept += 1
                continue
            key = (row["instrument_id"], row["report_period"])
            observed = None
            observed_source = None
            period_end = row["report_period"][:10]
            for source in (
                "source_file",
                "statements_raw",
                "survey_audit",
                "disclosure_event",
            ):
                value = evidence.get(source, {}).get(key)
                # impossible evidence (before the period end, issuer title
                # typos) never upgrades a statutory fallback row
                if value and value >= period_end:
                    observed, observed_source = value, source
                    break
            if not observed:
                no_evidence += 1
                continue

            entry["statutory_to_observed_upgrade"] = {
                "previous_value": row["data_available_date"],
                "value": observed,
                "source": observed_source,
                "updated_at": datetime.now(CST).isoformat(),
            }
            entry["value"] = observed
            entry["source"] = observed_source
            entry["quality"] = "observed"
            lineage_full["data_available_date_backfill"] = entry

            new_publish = row["publish_date"] or observed

            conn.execute(
                """
                UPDATE financial_facts
                SET data_available_date = ?, publish_date = ?, lineage_json = ?
                WHERE instrument_id = ? AND report_period = ?
                """,
                (
                    observed,
                    new_publish,
                    json.dumps(lineage_full, ensure_ascii=False, sort_keys=True),
                    row["instrument_id"],
                    row["report_period"],
                ),
            )
            upgraded += 1
        conn.commit()
        print(
            f"scanned={len(rows)} upgraded={upgraded} kept_non_estimated={kept} "
            f"no_evidence={no_evidence}"
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
