"""One-off TZ correction for availability dates filled before the fix.

cninfo stores a display date D as epoch D-1 16:00Z (CST midnight of D),
verified against the 2026Q3 pipeline anchor (Maotai: epoch 10-30T16:00Z,
display/publish 10-31). The original backfill took the UTC date part, so
observed evidence rows are one day early - the future-function direction.

This script rebuilds the evidence maps with the tz-aware normalization
(_normalize_date now converts to Asia/Shanghai), looks each backfilled
row up directly in the evidence maps (bypassing the publish_date
short-circuit in _resolve_available_date), and updates rows whose stored
date differs. publish_date filled from evidence is corrected the same
way. Statutory rows are untouched. Idempotent.
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
    _load_available_date_evidence,
    _json_object,
)

OBSERVED_SOURCES = ("source_file", "statements_raw", "survey_audit", "disclosure_event")
CST = ZoneInfo("Asia/Shanghai")


def _cst_date(value) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return text[:10]
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(CST)
    return parsed.date().isoformat()


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
            WHERE lineage_json LIKE '%data_available_date_backfill%'
            """
        ).fetchall()

        fixed = 0
        unchanged = 0
        non_observed = 0
        missing_evidence = 0
        for row in rows:
            lineage = _json_object(row["lineage_json"]).get(
                "data_available_date_backfill", {}
            )
            source = lineage.get("source", "")
            if source not in OBSERVED_SOURCES:
                non_observed += 1
                continue
            key = (row["instrument_id"], row["report_period"])
            evidence_date = evidence.get(source, {}).get(key)
            if not evidence_date:
                missing_evidence += 1
                continue
            if evidence_date == row["data_available_date"]:
                unchanged += 1
                continue

            lineage_full = _json_object(row["lineage_json"])
            entry = lineage_full.get("data_available_date_backfill", {})
            entry["tz_correction"] = {
                "previous_value": row["data_available_date"],
                "value": evidence_date,
                "updated_at": datetime.now(CST).isoformat(),
                "reason": "utc_to_cst_date_normalization",
            }
            entry["value"] = evidence_date
            lineage_full["data_available_date_backfill"] = entry

            new_publish = row["publish_date"]
            if new_publish and _cst_date(new_publish) == new_publish[:10]:
                pass  # plain date or already CST - keep
            elif new_publish:
                # publish_date filled from the same evidence string; shift by
                # the same correction delta
                try:
                    from datetime import date as _date, timedelta

                    delta = (
                        date.fromisoformat(evidence_date)
                        - date.fromisoformat(row["data_available_date"])
                    )
                    if delta:
                        new_publish = (
                            _date.fromisoformat(new_publish[:10]) + delta
                        ).isoformat()
                except ValueError:
                    pass

            conn.execute(
                """
                UPDATE financial_facts
                SET data_available_date = ?, publish_date = ?, lineage_json = ?
                WHERE instrument_id = ? AND report_period = ?
                """,
                (
                    evidence_date,
                    new_publish,
                    json.dumps(lineage_full, ensure_ascii=False, sort_keys=True),
                    row["instrument_id"],
                    row["report_period"],
                ),
            )
            fixed += 1
        conn.commit()
        print(
            f"scanned={len(rows)} tz_fixed={fixed} unchanged={unchanged} "
            f"non_observed={non_observed} evidence_missing={missing_evidence}"
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
