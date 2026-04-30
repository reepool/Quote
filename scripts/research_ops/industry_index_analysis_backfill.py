#!/usr/bin/env python
"""Backfill Shenwan industry index-analysis history into research storage."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import (
    initialize_manager_for_research_cli,
    json_ready,
)


DEFAULT_INDEX_TYPES = ["市场表征", "一级行业", "二级行业", "三级行业", "风格指数"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill Shenwan index-analysis daily history via AkShare.",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date, YYYY-MM-DD or YYYYMMDD.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date, YYYY-MM-DD or YYYYMMDD.",
    )
    parser.add_argument(
        "--index-types",
        help="Comma-separated index types. Defaults to configured supported types.",
    )
    parser.add_argument(
        "--limit-per-type",
        type=int,
        help="Optional per-index-type row limit for smoke validation.",
    )
    parser.add_argument(
        "--source",
        default="akshare",
        help="Provider source name. Defaults to akshare.",
    )
    parser.add_argument(
        "--mode",
        default="direct",
        choices=["direct", "proxy_patch"],
        help="AkShare runtime mode.",
    )
    parser.add_argument(
        "--chunk-frequency",
        default="month",
        choices=["day", "month", "quarter", "year", "none"],
        help="Date chunk size. Defaults to month to keep SWS paginated requests bounded.",
    )
    parser.add_argument(
        "--combine-index-types",
        action="store_true",
        help="Fetch all index types in one chunk call. Defaults to one call per index type.",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop at the first failed chunk. Defaults to continue and report failures.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    await initialize_manager_for_research_cli(data_manager)
    try:
        result = await run_backfill(
            data_manager,
            start_date=args.start_date,
            end_date=args.end_date,
            index_types=_parse_index_types(args.index_types),
            limit_per_type=args.limit_per_type,
            source=args.source,
            mode=args.mode,
            chunk_frequency=args.chunk_frequency,
            split_index_types=not args.combine_index_types,
            stop_on_error=bool(args.stop_on_error),
        )
        print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result.get("status") == "success" else 2
    finally:
        await data_manager.close()


async def run_backfill(
    manager: Any,
    *,
    start_date: str,
    end_date: str,
    index_types: Optional[List[str]] = None,
    limit_per_type: Optional[int] = None,
    source: str = "akshare",
    mode: str = "direct",
    chunk_frequency: str = "month",
    split_index_types: bool = True,
    stop_on_error: bool = False,
) -> Dict[str, Any]:
    """Run historical backfill in bounded chunks so successful chunks persist."""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if start > end:
        raise ValueError("start_date must be earlier than or equal to end_date")

    normalized_types = index_types or list(DEFAULT_INDEX_TYPES)
    chunk_results: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    rows_written = 0

    for chunk_start, chunk_end in iter_date_chunks(
        start,
        end,
        frequency=chunk_frequency,
    ):
        type_groups: Iterable[Optional[List[str]]]
        if split_index_types:
            type_groups = [[index_type] for index_type in normalized_types]
        else:
            type_groups = [normalized_types]

        for type_group in type_groups:
            result = await manager.run_industry_index_analysis_backfill(
                start_date=chunk_start.isoformat(),
                end_date=chunk_end.isoformat(),
                index_types=type_group,
                limit_per_type=limit_per_type,
                source=source,
                mode=mode,
            )
            chunk_result = {
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "index_types": type_group,
                "status": result.get("status"),
                "rows_written": int(result.get("rows_written") or 0),
                "reason": result.get("reason"),
                "coverage": result.get("coverage"),
            }
            chunk_results.append(chunk_result)
            rows_written += chunk_result["rows_written"]
            if result.get("status") != "success":
                failures.append(chunk_result)
                if stop_on_error:
                    return _build_backfill_result(
                        start=start,
                        end=end,
                        chunk_frequency=chunk_frequency,
                        split_index_types=split_index_types,
                        source=source,
                        mode=mode,
                        rows_written=rows_written,
                        chunk_results=chunk_results,
                        failures=failures,
                    )

    return _build_backfill_result(
        start=start,
        end=end,
        chunk_frequency=chunk_frequency,
        split_index_types=split_index_types,
        source=source,
        mode=mode,
        rows_written=rows_written,
        chunk_results=chunk_results,
        failures=failures,
    )


def _build_backfill_result(
    *,
    start: date,
    end: date,
    chunk_frequency: str,
    split_index_types: bool,
    source: str,
    mode: str,
    rows_written: int,
    chunk_results: List[Dict[str, Any]],
    failures: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "status": "success" if not failures else "partial_success",
        "operation": "history_backfill_chunked",
        "source": source,
        "mode": mode,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "chunk_frequency": chunk_frequency,
        "split_index_types": split_index_types,
        "chunks_total": len(chunk_results),
        "chunks_failed": len(failures),
        "rows_written": rows_written,
        "failures": failures,
        "chunks": chunk_results,
    }


def _parse_index_types(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None:
        return None
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def iter_date_chunks(
    start: date,
    end: date,
    *,
    frequency: str,
) -> Iterable[Tuple[date, date]]:
    if frequency == "none":
        yield start, end
        return

    current = start
    while current <= end:
        if frequency == "month":
            last_day = monthrange(current.year, current.month)[1]
            chunk_end = date(current.year, current.month, last_day)
        elif frequency == "day":
            chunk_end = current
        elif frequency == "quarter":
            quarter_end_month = ((current.month - 1) // 3 + 1) * 3
            chunk_end = date(
                current.year,
                quarter_end_month,
                monthrange(current.year, quarter_end_month)[1],
            )
        elif frequency == "year":
            chunk_end = date(current.year, 12, 31)
        else:
            raise ValueError(f"Unsupported chunk frequency: {frequency}")

        if chunk_end > end:
            chunk_end = end
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _parse_date(value: str) -> date:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return datetime.fromisoformat(text).date()


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
