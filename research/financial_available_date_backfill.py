"""Backfill financial fact availability dates from local evidence."""

from __future__ import annotations

import argparse
import calendar
import json
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo


CORE_FACT_TABLES = (
    "financial_core_facts_hot",
    "financial_core_facts_history",
)


@dataclass
class AvailableDateBackfillReport:
    """Summary for a financial available-date backfill run."""

    status: str
    dry_run: bool
    scanned_rows: int = 0
    eligible_rows: int = 0
    updated_rows: int = 0
    skipped_rows: int = 0
    source_counts: Dict[str, int] = field(default_factory=dict)
    table_counts: Dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "dry_run": self.dry_run,
            "scanned_rows": self.scanned_rows,
            "eligible_rows": self.eligible_rows,
            "updated_rows": self.updated_rows,
            "skipped_rows": self.skipped_rows,
            "source_counts": dict(sorted(self.source_counts.items())),
            "table_counts": dict(sorted(self.table_counts.items())),
            "errors": self.errors,
        }


def backfill_financial_available_dates(
    db_path: Path | str,
    *,
    write_enabled: bool = False,
    limit: Optional[int] = None,
) -> AvailableDateBackfillReport:
    """Fill missing core-fact data_available_date using local evidence.

    Estimated statutory deadlines are deliberately conservative and are marked
    in lineage so downstream research can distinguish them from observed
    announcement dates.
    """

    path = Path(db_path)
    report = AvailableDateBackfillReport(status="success", dry_run=not write_enabled)
    now = datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()

    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        table_names = _table_names(conn)
        tables = [table for table in CORE_FACT_TABLES if table in table_names]
        evidence = _load_available_date_evidence(conn, table_names)

        remaining = limit
        for table_name in tables:
            rows = _missing_available_date_rows(conn, table_name, remaining)
            if remaining is not None:
                remaining = max(0, remaining - len(rows))
            for row in rows:
                report.scanned_rows += 1
                candidate = _resolve_available_date(row, evidence)
                if candidate is None:
                    report.skipped_rows += 1
                    continue

                report.eligible_rows += 1
                report.updated_rows += 1
                report.source_counts[candidate["source"]] = (
                    report.source_counts.get(candidate["source"], 0) + 1
                )
                report.table_counts[table_name] = report.table_counts.get(table_name, 0) + 1

                if write_enabled:
                    lineage = _json_object(row["lineage_json"])
                    lineage["data_available_date_backfill"] = {
                        "source": candidate["source"],
                        "quality": candidate["quality"],
                        "value": candidate["date"],
                        "updated_at": now,
                    }
                    publish_date = row["publish_date"]
                    if candidate["quality"] == "observed" and not publish_date:
                        publish_date = candidate["date"]
                    conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET data_available_date = ?,
                            publish_date = ?,
                            lineage_json = ?,
                            updated_at = ?
                        WHERE instrument_id = ? AND report_period = ?
                        """,
                        (
                            candidate["date"],
                            publish_date,
                            json.dumps(lineage, ensure_ascii=False, sort_keys=True),
                            now,
                            row["instrument_id"],
                            row["report_period"],
                        ),
                    )

            if remaining == 0:
                break

        if write_enabled:
            conn.commit()

    if report.errors:
        report.status = "degraded" if report.updated_rows else "failed"
    return report


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _missing_available_date_rows(
    conn: sqlite3.Connection,
    table_name: str,
    limit: Optional[int],
) -> list[sqlite3.Row]:
    query = f"""
        SELECT instrument_id, report_period, fiscal_year, fiscal_quarter,
               publish_date, source_file_id, lineage_json
        FROM {table_name}
        WHERE data_available_date IS NULL OR data_available_date = ''
        ORDER BY exchange, instrument_id, report_period DESC
    """
    if limit is not None:
        query += " LIMIT ?"
        return conn.execute(query, (limit,)).fetchall()
    return conn.execute(query).fetchall()


def _load_available_date_evidence(
    conn: sqlite3.Connection,
    table_names: set[str],
) -> Dict[str, Dict[tuple[str, str], str]]:
    evidence: Dict[str, Dict[tuple[str, str], str]] = {
        "source_file": {},
        "disclosure_event": {},
    }
    if "financial_source_files" in table_names:
        for row in conn.execute(
            """
            SELECT instrument_id, report_period, published_at
            FROM financial_source_files
            WHERE published_at IS NOT NULL AND published_at <> ''
            """
        ).fetchall():
            _put_min_date(
                evidence["source_file"],
                row["instrument_id"],
                row["report_period"],
                row["published_at"],
            )
    if "financial_disclosure_event_state" in table_names:
        for row in conn.execute(
            """
            SELECT instrument_id, report_period, announcement_time
            FROM financial_disclosure_event_state
            WHERE announcement_time IS NOT NULL
              AND announcement_time <> ''
              AND classification IN ('periodic_report_available', 'local_core_gap')
            """
        ).fetchall():
            _put_min_date(
                evidence["disclosure_event"],
                row["instrument_id"],
                row["report_period"],
                row["announcement_time"],
            )
    return evidence


def _put_min_date(
    target: Dict[tuple[str, str], str],
    instrument_id: str,
    report_period: str,
    raw_date: Any,
) -> None:
    normalized = _normalize_date(raw_date)
    if not normalized:
        return
    key = (str(instrument_id), str(report_period))
    current = target.get(key)
    if current is None or normalized < current:
        target[key] = normalized


def _resolve_available_date(
    row: sqlite3.Row,
    evidence: Dict[str, Dict[tuple[str, str], str]],
) -> Optional[Dict[str, str]]:
    key = (str(row["instrument_id"]), str(row["report_period"]))
    publish_date = _normalize_date(row["publish_date"])
    if publish_date:
        return {"date": publish_date, "source": "core_publish_date", "quality": "observed"}
    for source_name in ("source_file", "disclosure_event"):
        value = evidence[source_name].get(key)
        if value:
            return {"date": value, "source": source_name, "quality": "observed"}
    estimated = _estimated_statutory_deadline(
        row["report_period"],
        row["fiscal_year"],
        row["fiscal_quarter"],
    )
    if estimated:
        return {
            "date": estimated,
            "source": "estimated_statutory_deadline",
            "quality": "estimated",
        }
    return None


def _estimated_statutory_deadline(
    report_period: Any,
    fiscal_year: Any,
    fiscal_quarter: Any,
) -> Optional[str]:
    year: Optional[int] = None
    quarter: Optional[int] = None
    try:
        if fiscal_year is not None and fiscal_quarter is not None:
            year = int(fiscal_year)
            quarter = int(fiscal_quarter)
    except (TypeError, ValueError):
        year = None
        quarter = None

    if year is None or quarter is None:
        parsed = _parse_report_period(report_period)
        if parsed is None:
            return None
        year, quarter = parsed

    if quarter == 1:
        deadline = date(year, 4, 30)
    elif quarter == 2:
        deadline = date(year, 8, 31)
    elif quarter == 3:
        deadline = date(year, 10, 31)
    elif quarter == 4:
        deadline = date(year + 1, 4, 30)
    else:
        return None
    return deadline.isoformat()


def _parse_report_period(value: Any) -> Optional[tuple[int, int]]:
    text = str(value or "").strip()
    if len(text) == 6 and text[4].upper() == "Q":
        try:
            return int(text[:4]), int(text[5])
        except ValueError:
            return None
    normalized = _normalize_date(text)
    if not normalized:
        return None
    parsed = date.fromisoformat(normalized)
    quarter = (parsed.month - 1) // 3 + 1
    return parsed.year, quarter


def _normalize_date(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.date().isoformat()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[:10], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _json_object(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill financial core fact data_available_date from local evidence.",
    )
    parser.add_argument(
        "--db-path",
        default="data/financials.db",
        help="Path to financials.db.",
    )
    parser.add_argument(
        "--write-enabled",
        action="store_true",
        help="Persist updates. Defaults to dry-run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum rows to scan across core fact tables.",
    )
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    report = backfill_financial_available_dates(
        Path(args.db_path),
        write_enabled=bool(args.write_enabled),
        limit=args.limit,
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report.status in {"success", "degraded"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
