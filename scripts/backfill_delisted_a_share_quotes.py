#!/usr/bin/env python3
"""Dry-run or execute targeted delisted A-share historical quote backfill."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = str(Path(__file__).resolve().parent)
sys.path = [entry for entry in sys.path if entry != SCRIPT_DIR]
if str(PROJECT_ROOT) in sys.path:
    sys.path.remove(str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))


def _split_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill historical quotes for locally known delisted A-share stocks."
    )
    parser.add_argument("--execute", action="store_true", help="Run live source fetches and write daily_quotes. Default is dry-run.")
    parser.add_argument("--exchanges", default="SSE,SZSE,BSE", help="Comma-separated exchanges. Default: SSE,SZSE,BSE")
    parser.add_argument("--delisted-year-start", type=int, default=None)
    parser.add_argument("--delisted-year-end", type=int, default=None)
    parser.add_argument("--delisted-start-date", default=None)
    parser.add_argument("--delisted-end-date", default=None)
    parser.add_argument("--instrument-ids", default=None, help="Comma-separated instrument ids, e.g. 000508.SZ,600625.SH")
    parser.add_argument("--limit", type=int, default=None, help="Maximum selected instruments to process.")
    parser.add_argument("--timeout-sec", type=int, default=None, help="Per-instrument timeout for live execution.")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--db-path", default=str(PROJECT_ROOT / "data" / "quotes.db"), help="SQLite quotes database path for dry-run.")
    return parser.parse_args()


def _date_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _bounded_samples(items: list[dict], limit: int) -> list[dict]:
    samples: list[dict] = []
    for item in items[: max(0, int(limit or 0))]:
        samples.append({
            "instrument_id": item.get("instrument_id"),
            "symbol": item.get("symbol"),
            "name": item.get("name"),
            "exchange": item.get("exchange"),
            "listed_date": _date_text(item.get("listed_date")),
            "delisted_date": _date_text(item.get("delisted_date")),
            "quote_rows": int(item.get("quote_rows") or 0),
            "first_quote_date": _date_text(item.get("first_quote_date")),
            "last_quote_date": _date_text(item.get("last_quote_date")),
            "coverage_status": item.get("coverage_status"),
        })
    return samples


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def _instrument_where(args: argparse.Namespace) -> tuple[str, list[Any]]:
    exchanges = _split_csv(args.exchanges)
    instrument_ids = _split_csv(args.instrument_ids)
    clauses = [
        "type = 'stock'",
        "listed_date IS NOT NULL",
        "delisted_date IS NOT NULL",
    ]
    params: list[Any] = []
    if exchanges:
        clauses.append(f"exchange IN ({','.join('?' for _ in exchanges)})")
        params.extend(exchanges)
    else:
        clauses.append("exchange IN ('SSE','SZSE','BSE')")
    if args.delisted_year_start is not None:
        clauses.append("strftime('%Y', delisted_date) >= ?")
        params.append(str(int(args.delisted_year_start)))
    if args.delisted_year_end is not None:
        clauses.append("strftime('%Y', delisted_date) <= ?")
        params.append(str(int(args.delisted_year_end)))
    if args.delisted_start_date:
        clauses.append("delisted_date >= ?")
        params.append(args.delisted_start_date)
    if args.delisted_end_date:
        clauses.append("delisted_date <= ?")
        params.append(args.delisted_end_date)
    if instrument_ids:
        clauses.append(f"instrument_id IN ({','.join('?' for _ in instrument_ids)})")
        params.extend(instrument_ids)
    return " AND ".join(clauses), params


def _load_instruments(conn: sqlite3.Connection, args: argparse.Namespace, *, apply_limit: bool) -> list[dict]:
    where_sql, params = _instrument_where(args)
    sql = f"""
        SELECT instrument_id, symbol, name, exchange, listed_date, delisted_date,
               status, is_active, trading_status, source_symbol
        FROM instruments
        WHERE {where_sql}
        ORDER BY delisted_date, exchange, symbol
    """
    if apply_limit and args.limit and int(args.limit) > 0:
        sql += " LIMIT ?"
        params = [*params, int(args.limit)]
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _load_quote_coverage(conn: sqlite3.Connection, instrument_ids: list[str]) -> dict[str, dict]:
    coverage: dict[str, dict] = {}
    for idx in range(0, len(instrument_ids), 800):
        chunk = instrument_ids[idx: idx + 800]
        if not chunk:
            continue
        sql = f"""
            SELECT instrument_id, COUNT(*) AS quote_rows, MIN(time) AS first_quote_date, MAX(time) AS last_quote_date
            FROM daily_quotes
            WHERE instrument_id IN ({','.join('?' for _ in chunk)})
            GROUP BY instrument_id
        """
        for row in conn.execute(sql, chunk).fetchall():
            coverage[row["instrument_id"]] = dict(row)
    return coverage


def _attach_coverage(instruments: list[dict], coverage_by_id: dict[str, dict], *, include_covered: bool) -> list[dict]:
    rows: list[dict] = []
    for item in instruments:
        coverage = coverage_by_id.get(item.get("instrument_id"), {})
        quote_rows = int(coverage.get("quote_rows") or 0)
        listed = _parse_date(item.get("listed_date"))
        delisted = _parse_date(item.get("delisted_date"))
        first_quote = _parse_date(coverage.get("first_quote_date"))
        last_quote = _parse_date(coverage.get("last_quote_date"))
        if quote_rows <= 0:
            status = "missing"
        elif listed and delisted and first_quote and last_quote and first_quote <= listed and last_quote >= delisted:
            status = "covered"
        else:
            status = "partial"
        if status == "covered" and not include_covered:
            continue
        item = dict(item)
        item.update({
            "quote_rows": quote_rows,
            "first_quote_date": coverage.get("first_quote_date"),
            "last_quote_date": coverage.get("last_quote_date"),
            "coverage_status": status,
        })
        rows.append(item)
    return rows


def _coverage_by_year(instruments: list[dict], coverage_by_id: dict[str, dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for item in instruments:
        delisted = _parse_date(item.get("delisted_date"))
        if not delisted:
            continue
        year = str(delisted.year)
        bucket = grouped.setdefault(year, {
            "delisted_year": year,
            "instrument_count": 0,
            "with_quotes_count": 0,
            "no_quotes_count": 0,
            "covered_count": 0,
            "first_quote_date": None,
            "last_quote_date": None,
        })
        bucket["instrument_count"] += 1
        coverage = coverage_by_id.get(item.get("instrument_id"), {})
        quote_rows = int(coverage.get("quote_rows") or 0)
        first_quote = _parse_date(coverage.get("first_quote_date"))
        last_quote = _parse_date(coverage.get("last_quote_date"))
        listed = _parse_date(item.get("listed_date"))
        if quote_rows > 0:
            bucket["with_quotes_count"] += 1
            raw_first = coverage.get("first_quote_date")
            raw_last = coverage.get("last_quote_date")
            if raw_first is not None and (bucket["first_quote_date"] is None or raw_first < bucket["first_quote_date"]):
                bucket["first_quote_date"] = raw_first
            if raw_last is not None and (bucket["last_quote_date"] is None or raw_last > bucket["last_quote_date"]):
                bucket["last_quote_date"] = raw_last
        else:
            bucket["no_quotes_count"] += 1
        if quote_rows > 0 and listed and delisted and first_quote and last_quote and first_quote <= listed and last_quote >= delisted:
            bucket["covered_count"] += 1
    rows = []
    for year in sorted(grouped):
        item = grouped[year]
        item["uncovered_count"] = item["instrument_count"] - item["covered_count"]
        rows.append(item)
    return rows


async def _run_dry_run(args: argparse.Namespace) -> dict:
    print("[delisted-backfill] dry-run opening sqlite database", file=sys.stderr)
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    exchanges = _split_csv(args.exchanges)
    instrument_ids = _split_csv(args.instrument_ids)
    try:
        print("[delisted-backfill] dry-run listing candidates", file=sys.stderr)
        all_instruments = _load_instruments(conn, args, apply_limit=False)
        coverage_by_id = _load_quote_coverage(
            conn,
            [item["instrument_id"] for item in all_instruments if item.get("instrument_id")],
        )
        all_uncovered = _attach_coverage(all_instruments, coverage_by_id, include_covered=False)
        candidates = all_uncovered
        if args.limit and int(args.limit) > 0:
            candidates = candidates[: int(args.limit)]
        print("[delisted-backfill] dry-run summarizing coverage", file=sys.stderr)
        coverage = _coverage_by_year(all_instruments, coverage_by_id)
    finally:
        conn.close()
    print("[delisted-backfill] dry-run complete", file=sys.stderr)
    return {
        "operation": "delisted_a_share_quote_backfill",
        "dry_run": True,
        "status": "dry_run",
        "filters": {
            "exchanges": exchanges or ["SSE", "SZSE", "BSE"],
            "delisted_year_start": args.delisted_year_start,
            "delisted_year_end": args.delisted_year_end,
            "delisted_start_date": args.delisted_start_date,
            "delisted_end_date": args.delisted_end_date,
            "instrument_ids": instrument_ids or [],
            "include_already_covered": False,
            "limit": args.limit,
        },
        "target_count": len(all_uncovered),
        "limited_target_count": len(candidates),
        "coverage_status_counts": dict(Counter(str(item.get("coverage_status") or "unknown") for item in all_uncovered)),
        "coverage_before": coverage,
        "samples": {
            "targets": _bounded_samples(candidates, args.sample_limit),
        },
    }


async def main() -> int:
    args = parse_args()
    if not args.execute:
        result = await _run_dry_run(args)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0

    from data_manager import data_manager

    await data_manager.initialize(include_data_sources=True, load_progress=False)
    result = await data_manager.run_delisted_a_share_quote_backfill(
        exchanges=_split_csv(args.exchanges),
        delisted_year_start=args.delisted_year_start,
        delisted_year_end=args.delisted_year_end,
        delisted_start_date=args.delisted_start_date,
        delisted_end_date=args.delisted_end_date,
        instrument_ids=_split_csv(args.instrument_ids),
        limit=args.limit,
        dry_run=False,
        per_instrument_timeout_sec=args.timeout_sec,
        fail_fast=args.fail_fast,
        sample_limit=args.sample_limit,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") in {"success", "dry_run", "warning"} else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
