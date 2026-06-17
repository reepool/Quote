#!/usr/bin/env python
"""Backfill official futures exchange trading calendars in bounded chunks."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.futures_market_data import (  # noqa: E402
    FuturesOfficialCalendarBackfillService,
    FuturesStorageManager,
    FuturesUniverseSelector,
    default_futures_registry,
)
from scripts.research_cli_support import json_ready, parse_exchanges  # noqa: E402
from utils.config_manager import UnifiedConfigManager  # noqa: E402
from utils.date_utils import get_shanghai_time  # noqa: E402


DEFAULT_EXCHANGES = ["SHFE", "INE", "DCE", "CZCE", "GFEX"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill futures official trading calendars by exchange/year chunks. "
            "Defaults to dry-run; pass --write to persist into futures.db."
        )
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help=(
            "Override all exchange starts. If omitted, uses "
            "trading_day_governance.official_calendar_backfill.exchange_start_dates "
            "or empty_payload_closed_start_dates per exchange."
        ),
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Defaults to current Asia/Shanghai date. Future dates are not weekday-filled.",
    )
    parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_EXCHANGES),
        help="Comma-separated exchanges, e.g. SHFE,INE,DCE,CZCE,GFEX.",
    )
    parser.add_argument("--scope-id", default=None, help="Configured futures download scope id.")
    parser.add_argument(
        "--categories",
        default=None,
        help="Comma-separated futures categories; supports all.",
    )
    parser.add_argument(
        "--instrument-ids",
        default=None,
        help="Comma-separated futures instrument ids.",
    )
    parser.add_argument(
        "--series-ids",
        default=None,
        help="Comma-separated futures series ids.",
    )
    parser.add_argument(
        "--series-types",
        default=None,
        help="Comma-separated futures series types; defaults to configured main_continuous.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Override futures.db path. Defaults to commodity_market_data.storage.database.",
    )
    parser.add_argument("--chunk-years", type=int, default=1)
    parser.add_argument("--max-chunks", type=int, default=None)
    parser.add_argument("--official-timeout-seconds", type=float, default=None)
    parser.add_argument("--official-retry-attempts", type=int, default=None)
    parser.add_argument("--official-retry-backoff-seconds", type=float, default=None)
    parser.add_argument(
        "--replace-exchange-calendar",
        action="store_true",
        help="Before writing, delete existing calendar rows for each selected exchange/date range. A db backup is created first.",
    )
    parser.add_argument("--write", action="store_true", help="Persist calendar rows.")
    parser.add_argument(
        "--output-path",
        type=Path,
        default=None,
        help="JSON summary output path. A .jsonl progress file is written beside it.",
    )
    return parser


def _date_key(raw: str) -> str:
    return date.fromisoformat(str(raw)[:10]).isoformat()


def _csv(raw: Optional[str]) -> Optional[List[str]]:
    if not raw:
        return None
    values = [item.strip() for item in str(raw).split(",") if item.strip()]
    return values or None


def _add_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return value.replace(month=2, day=28, year=value.year + years)


def _chunk_ranges(start_date: str, end_date: str, chunk_years: int) -> Iterable[Tuple[str, str]]:
    if chunk_years <= 0:
        raise ValueError("chunk_years must be positive")
    start = date.fromisoformat(_date_key(start_date))
    end = date.fromisoformat(_date_key(end_date))
    current = start
    while current <= end:
        next_start = _add_years(current, chunk_years)
        chunk_end = min(end, next_start - timedelta(days=1))
        yield current.isoformat(), chunk_end.isoformat()
        current = next_start


def _default_output_path(start_date: str, end_date: str, dry_run: bool) -> Path:
    suffix = "dryrun" if dry_run else "write"
    stamp = get_shanghai_time().strftime("%Y%m%d_%H%M%S")
    return Path("/tmp") / f"quote_futures_official_calendar_{start_date}_{end_date}_{suffix}_{stamp}.json"


def _configured_exchange_start_dates(
    module_cfg: Dict[str, Any],
    exchanges: Sequence[str],
    fallback_start_date: str,
) -> Dict[str, str]:
    governance_cfg = module_cfg.get("trading_day_governance") if isinstance(module_cfg.get("trading_day_governance"), dict) else {}
    backfill_cfg = (
        governance_cfg.get("official_calendar_backfill")
        if isinstance(governance_cfg.get("official_calendar_backfill"), dict)
        else {}
    )
    configured: Dict[str, Any] = {}
    for key in ("empty_payload_closed_start_dates", "exchange_start_dates"):
        values = backfill_cfg.get(key)
        if isinstance(values, dict):
            configured.update(values)
    return {
        exchange: _date_key(configured.get(exchange) or fallback_start_date)
        for exchange in exchanges
    }


def _calendar_summary(db_path: str) -> List[Dict[str, Any]]:
    path = Path(db_path)
    if not path.exists():
        return []
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT exchange, quality_flag, COUNT(*) AS row_count,
                   MIN(trade_date) AS min_date, MAX(trade_date) AS max_date,
                   SUM(CASE WHEN is_trading_day = 1 THEN 1 ELSE 0 END) AS trading_days,
                   SUM(CASE WHEN is_trading_day = 0 THEN 1 ELSE 0 END) AS closed_days
            FROM futures_trading_calendar
            GROUP BY exchange, quality_flag
            ORDER BY exchange, quality_flag
            """
        ).fetchall()
    return [dict(row) for row in rows]


def _backup_db(db_path: str) -> Optional[str]:
    path = Path(db_path)
    if not path.exists():
        return None
    backup = path.with_name(f"{path.name}.bak_futures_calendar_{get_shanghai_time().strftime('%Y%m%d_%H%M%S')}")
    shutil.copy2(path, backup)
    return str(backup)


def _delete_exchange_calendar(db_path: str, exchange: str, start_date: str, end_date: str) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            DELETE FROM futures_trading_calendar
            WHERE exchange = ? AND trade_date >= ? AND trade_date <= ?
            """,
            (exchange, start_date, end_date),
        )
        return int(cursor.rowcount if cursor.rowcount is not None else 0)


def _rollup_chunks(chunks: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    totals = {
        "chunks": len(chunks),
        "rows_written": 0,
        "trading_days": 0,
        "closed_days": 0,
        "unresolved_dates": 0,
        "request_count": 0,
    }
    statuses: Dict[str, int] = {}
    for chunk in chunks:
        status = str(chunk.get("status") or "unknown")
        statuses[status] = statuses.get(status, 0) + 1
        chunk_totals = chunk.get("totals") or {}
        for key in totals:
            if key == "chunks":
                continue
            totals[key] += int(chunk_totals.get(key) or 0)
    totals["status_counts"] = statuses
    return totals


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    end_date = _date_key(args.end_date or get_shanghai_time().date().isoformat())
    explicit_scope_requested = any(
        [
            args.scope_id,
            args.categories,
            args.instrument_ids,
            args.series_ids,
            args.series_types,
        ]
    )
    exchanges = parse_exchanges(args.exchanges) or DEFAULT_EXCHANGES
    dry_run = not args.write
    start_label = _date_key(args.start_date) if args.start_date else "configured_exchange_starts"
    output_path = args.output_path or _default_output_path(start_label, end_date, dry_run)
    progress_path = output_path.with_suffix(output_path.suffix + ".jsonl")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    config = UnifiedConfigManager().get_research_config()
    module_cfg = config.modules.setdefault("commodity_market_data", {})
    if args.db_path:
        module_cfg.setdefault("storage", {})["database"] = args.db_path
    official_cfg = module_cfg.setdefault("sources", {}).setdefault("exchange_official", {})
    if args.official_timeout_seconds is not None:
        official_cfg["timeout_seconds"] = args.official_timeout_seconds
    if args.official_retry_attempts is not None:
        official_cfg["retry_attempts"] = args.official_retry_attempts
        dce_browser_cfg = official_cfg.setdefault("dce_browser", {})
        if isinstance(dce_browser_cfg, dict):
            dce_browser_cfg["retry_attempts"] = args.official_retry_attempts
    if args.official_retry_backoff_seconds is not None:
        official_cfg["retry_backoff_seconds"] = args.official_retry_backoff_seconds
    db_path = str(module_cfg.get("storage", {}).get("database") or "data/futures.db")
    fallback_start_date = _date_key(
        args.start_date
        or module_cfg.get("trading_day_governance", {})
        .get("official_calendar_backfill", {})
        .get("start_date")
        or "2000-01-01"
    )
    exchange_start_dates = (
        {exchange: _date_key(args.start_date) for exchange in exchanges}
        if args.start_date
        else _configured_exchange_start_dates(module_cfg, exchanges, fallback_start_date)
    )
    invalid_ranges = {
        exchange: start
        for exchange, start in exchange_start_dates.items()
        if start > end_date
    }
    if invalid_ranges:
        raise ValueError(f"exchange start_date must be <= end_date: {invalid_ranges}")

    storage = FuturesStorageManager(config, db_path=db_path)
    storage.initialize()
    registry = default_futures_registry(module_cfg)
    storage.upsert_categories(registry.get("categories", []))
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])
    storage.upsert_source_manifests(registry.get("source_manifests", []))
    scope_selection = FuturesUniverseSelector(module_cfg, storage).resolve(
        scope_id=args.scope_id,
        exchanges=exchanges if args.exchanges or not explicit_scope_requested else None,
        categories=_csv(args.categories),
        instrument_ids=_csv(args.instrument_ids),
        series_ids=_csv(args.series_ids),
        series_types=_csv(args.series_types),
    )
    if scope_selection.blockers:
        payload = {
            "status": "blocked",
            "domain": "futures_official_trading_calendar_backfill",
            "dry_run": dry_run,
            "start_date": start_label,
            "end_date": end_date,
            "scope_selection": scope_selection.as_dict(),
            "blockers": scope_selection.blockers,
            "warnings": scope_selection.warnings,
        }
        output_path.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(json_ready({"summary_path": str(output_path), "status": "blocked"}), ensure_ascii=False), flush=True)
        return 1
    exchanges = scope_selection.exchanges

    service = FuturesOfficialCalendarBackfillService(storage, config, module_cfg)
    chunk_results: List[Dict[str, Any]] = []
    chunk_count = 0
    started_at = get_shanghai_time().isoformat()
    backup_path = None
    deleted_rows_by_exchange: Dict[str, int] = {}
    if args.write and args.replace_exchange_calendar:
        backup_path = _backup_db(db_path)
        for exchange in exchanges:
            deleted_rows_by_exchange[exchange] = _delete_exchange_calendar(
                db_path,
                exchange,
                exchange_start_dates[exchange],
                end_date,
            )
    with progress_path.open("a", encoding="utf-8") as progress:
        for exchange in exchanges:
            exchange_start = exchange_start_dates[exchange]
            for chunk_start, chunk_end in _chunk_ranges(exchange_start, end_date, args.chunk_years):
                chunk_count += 1
                if args.max_chunks is not None and chunk_count > args.max_chunks:
                    break
                chunk_started = get_shanghai_time()
                try:
                    result = service.run(
                        scope_id=args.scope_id,
                        exchanges=[exchange],
                        categories=_csv(args.categories),
                        instrument_ids=_csv(args.instrument_ids),
                        series_ids=_csv(args.series_ids),
                        series_types=_csv(args.series_types),
                        start_date=chunk_start,
                        end_date=chunk_end,
                        dry_run=dry_run,
                    )
                    status = str(result.get("status") or "unknown")
                    error = ""
                except Exception as exc:
                    result = {
                        "status": "failed",
                        "domain": "futures_official_trading_calendar_backfill",
                        "exchange": exchange,
                        "start_date": chunk_start,
                        "end_date": chunk_end,
                        "dry_run": dry_run,
                        "error": str(exc),
                        "totals": {},
                    }
                    status = "failed"
                    error = str(exc)
                chunk_payload = {
                    "exchange": exchange,
                    "start_date": chunk_start,
                    "end_date": chunk_end,
                    "dry_run": dry_run,
                    "status": status,
                    "elapsed_seconds": round((get_shanghai_time() - chunk_started).total_seconds(), 3),
                    "totals": result.get("totals") or {},
                    "blockers": result.get("blockers") or [],
                    "warnings": result.get("warnings") or [],
                    "error": error,
                    "result": result,
                }
                chunk_results.append(chunk_payload)
                progress.write(json.dumps(json_ready(chunk_payload), ensure_ascii=False) + "\n")
                progress.flush()
                print(
                    json.dumps(json_ready({k: v for k, v in chunk_payload.items() if k != "result"}), ensure_ascii=False),
                    flush=True,
                )
            if args.max_chunks is not None and chunk_count >= args.max_chunks:
                break

    payload = {
        "status": "success"
        if all(chunk.get("status") == "success" for chunk in chunk_results)
        else "partial",
        "started_at": started_at,
        "completed_at": get_shanghai_time().isoformat(),
        "db_path": db_path,
        "dry_run": dry_run,
        "start_date": start_label,
        "end_date": end_date,
        "exchanges": exchanges,
        "scope_selection": scope_selection.as_dict(),
        "exchange_start_dates": exchange_start_dates,
        "chunk_years": args.chunk_years,
        "progress_path": str(progress_path),
        "backup_path": backup_path,
        "deleted_rows_by_exchange": deleted_rows_by_exchange,
        "totals": _rollup_chunks(chunk_results),
        "calendar_summary_after": _calendar_summary(db_path),
        "chunks": chunk_results,
    }
    output_path.write_text(json.dumps(json_ready(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(json_ready({"summary_path": str(output_path), "totals": payload["totals"]}), ensure_ascii=False),
        flush=True,
    )
    return 0 if payload["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
