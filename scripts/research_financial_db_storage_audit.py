#!/usr/bin/env python3
"""Audit financial SQLite database storage without expensive full-page scans."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


CRITICAL_TABLES = [
    "financial_numeric_facts",
    "financial_numeric_facts_hot",
    "financial_numeric_facts_history",
    "financial_statements_raw",
    "financial_source_files",
    "financial_core_facts_hot",
    "financial_core_facts_history",
    "raw_payload_audit",
    "ingestion_runs",
]

JSON_SAMPLE_COLUMNS = [
    ("financial_numeric_facts_hot", "raw_fact_json"),
    ("financial_numeric_facts_hot", "dimensions_json"),
    ("financial_numeric_facts_history", "raw_fact_json"),
    ("financial_statements_raw", "statement_json"),
    ("raw_payload_audit", "payload_json"),
    ("financial_source_files", "metadata_json"),
    ("financial_source_files", "parser_diagnostics_json"),
]


def _fetch_one(conn: sqlite3.Connection, sql: str, params: Iterable[Any] = ()) -> Any:
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None:
        return None
    return row[0]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return (
        _fetch_one(
            conn,
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE name = ? AND type IN ('table', 'view')
            """,
            (table_name,),
        )
        > 0
    )


def _object_type(conn: sqlite3.Connection, name: str) -> Optional[str]:
    return _fetch_one(
        conn,
        "SELECT type FROM sqlite_master WHERE name = ? ORDER BY type LIMIT 1",
        (name,),
    )


def _count_rows(conn: sqlite3.Connection, table_name: str) -> Optional[int]:
    if not _table_exists(conn, table_name):
        return None
    return int(_fetch_one(conn, f"SELECT COUNT(*) FROM {table_name}") or 0)


def _sample_column_length(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    *,
    sample_size: int,
) -> Optional[Dict[str, Any]]:
    if not _table_exists(conn, table_name):
        return None
    columns = {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in columns:
        return None
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS sampled_rows,
               AVG(length({column_name})) AS avg_bytes,
               MAX(length({column_name})) AS max_bytes
        FROM (
            SELECT {column_name}
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT ?
        )
        """,
        (int(sample_size),),
    ).fetchone()
    sampled_rows = int(row["sampled_rows"] or 0)
    if sampled_rows == 0:
        return {
            "table": table_name,
            "column": column_name,
            "sampled_rows": 0,
            "avg_bytes": 0.0,
            "max_bytes": 0,
        }
    return {
        "table": table_name,
        "column": column_name,
        "sampled_rows": sampled_rows,
        "avg_bytes": round(float(row["avg_bytes"] or 0.0), 2),
        "max_bytes": int(row["max_bytes"] or 0),
    }


def audit_financial_db(db_path: Path, *, sample_size: int = 10_000) -> Dict[str, Any]:
    """Return lightweight storage diagnostics for a financial SQLite database."""
    if not db_path.exists():
        raise FileNotFoundError(f"financial database not found: {db_path}")
    file_size = db_path.stat().st_size
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        page_size = int(_fetch_one(conn, "PRAGMA page_size") or 0)
        page_count = int(_fetch_one(conn, "PRAGMA page_count") or 0)
        freelist_count = int(_fetch_one(conn, "PRAGMA freelist_count") or 0)
        object_counts = {
            str(row["type"]): int(row["count"])
            for row in conn.execute(
                "SELECT type, COUNT(*) AS count FROM sqlite_master GROUP BY type"
            ).fetchall()
        }
        table_rows = {
            table_name: _count_rows(conn, table_name)
            for table_name in CRITICAL_TABLES
        }
        object_types = {
            table_name: _object_type(conn, table_name)
            for table_name in CRITICAL_TABLES
            if _object_type(conn, table_name)
        }
        duplicate_evidence = {
            "numeric_equals_hot": (
                table_rows.get("financial_numeric_facts")
                == table_rows.get("financial_numeric_facts_hot")
                and table_rows.get("financial_numeric_facts") is not None
            ),
            "history_rows": table_rows.get("financial_numeric_facts_history"),
        }
        json_samples = [
            sample
            for table_name, column_name in JSON_SAMPLE_COLUMNS
            for sample in [
                _sample_column_length(
                    conn,
                    table_name,
                    column_name,
                    sample_size=sample_size,
                )
            ]
            if sample is not None
        ]
        indexes = [
            {"name": str(row["name"]), "table": str(row["tbl_name"])}
            for row in conn.execute(
                """
                SELECT name, tbl_name
                FROM sqlite_master
                WHERE type = 'index'
                ORDER BY tbl_name, name
                """
            ).fetchall()
        ]
    return {
        "db_path": str(db_path),
        "file_size_bytes": file_size,
        "file_size_gib": round(file_size / 1024 / 1024 / 1024, 3),
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "page_bytes_gib": round(page_size * page_count / 1024 / 1024 / 1024, 3),
        "object_counts": object_counts,
        "critical_table_rows": table_rows,
        "critical_object_types": object_types,
        "duplicate_numeric_fact_evidence": duplicate_evidence,
        "json_length_samples": json_samples,
        "index_count": len(indexes),
        "indexes": indexes,
        "sample_size": int(sample_size),
        "estimated": True,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    """Render audit report as operator-friendly Markdown."""
    lines = [
        "# Financial DB Storage Audit",
        "",
        f"- DB: `{report['db_path']}`",
        f"- File size: `{report['file_size_gib']} GiB`",
        f"- Page bytes: `{report['page_bytes_gib']} GiB`",
        f"- Page size/count: `{report['page_size']}` / `{report['page_count']}`",
        f"- Free pages: `{report['freelist_count']}`",
        f"- Objects: `{report['object_counts']}`",
        f"- Index count: `{report['index_count']}`",
        "",
        "## Critical Row Counts",
        "",
        "| Object | Type | Rows |",
        "|---|---:|---:|",
    ]
    object_types = report.get("critical_object_types") or {}
    for table_name, rows in (report.get("critical_table_rows") or {}).items():
        lines.append(f"| `{table_name}` | `{object_types.get(table_name, '-')}` | `{rows}` |")
    lines.extend(
        [
            "",
            "## Duplicate Numeric Fact Evidence",
            "",
            f"- `numeric_equals_hot`: `{report['duplicate_numeric_fact_evidence']['numeric_equals_hot']}`",
            f"- `history_rows`: `{report['duplicate_numeric_fact_evidence']['history_rows']}`",
            "",
            "## JSON Length Samples",
            "",
            "| Table | Column | Sampled Rows | Avg Bytes | Max Bytes |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for sample in report.get("json_length_samples") or []:
        lines.append(
            "| `{table}` | `{column}` | `{sampled_rows}` | `{avg_bytes}` | `{max_bytes}` |".format(
                **sample
            )
        )
    lines.extend(
        [
            "",
            "> Metrics are lightweight estimates unless explicitly marked otherwise.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/financials.db")
    parser.add_argument("--sample-size", type=int, default=10_000)
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    parser.add_argument("--output-path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = audit_financial_db(Path(args.db_path), sample_size=args.sample_size)
    if args.format == "json":
        content = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    else:
        content = render_markdown(report)
    if args.output_path:
        Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_path).write_text(content + "\n", encoding="utf-8")
    else:
        print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
