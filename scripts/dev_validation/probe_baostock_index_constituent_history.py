#!/usr/bin/env python3
"""Bounded live probe for governed BaoStock historical index membership."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_sources.baostock_source import BaostockSource
from data_sources.base_source import RateLimitConfig
from research.backtest_data.index_constituent_history_backfill import (
    CoreIndexConstituentHistoryBackfill,
)
from utils import config_manager


async def run(args: argparse.Namespace) -> dict:
    source_config = config_manager.get("data_sources_config", {}).get("baostock", {})
    rate_limit = RateLimitConfig(
        max_requests_per_minute=int(source_config.get("max_requests_per_minute", 300)),
        max_requests_per_hour=int(source_config.get("max_requests_per_hour", 5000)),
        max_requests_per_day=int(source_config.get("max_requests_per_day", 40000)),
        retry_times=int(source_config.get("retry_times", 5)),
        retry_interval=float(source_config.get("retry_interval", 5.0)),
        min_interval_seconds=float(source_config.get("min_interval_seconds", 0.2)),
    )
    source = BaostockSource(
        "baostock_index_history_probe",
        rate_limit,
        connection_timeout_seconds=float(source_config.get("connection_timeout", 30.0)),
        login_timeout_seconds=float(source_config.get("login_timeout", 30.0)),
        daily_request_safety_limit=int(
            source_config.get("daily_request_safety_limit", 40000)
        ),
        usage_state_path=source_config.get(
            "usage_state_path", "data/runtime/baostock/api_usage.json"
        ),
        session_lock_path=source_config.get(
            "session_lock_path", "data/runtime/baostock/session.lock"
        ),
    )
    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=args.database,
        checkpoint_path=args.checkpoint,
        fetcher=source.get_historical_index_constituents,
        quota_reader=source.get_quota_snapshot,
    )
    probe_date = date.fromisoformat(args.date)
    plan = service.build_plan(
        start_date=probe_date,
        end_date=probe_date,
        trading_dates=[probe_date],
        indexes=args.indexes,
        daily_request_reserve=args.daily_request_reserve,
        sampling="monthly",
        max_queries_per_run=len(args.indexes),
    )
    before = source.get_quota_snapshot()
    try:
        await source.initialize()
        result = await service.run(plan, resume=False)
    finally:
        await source.close()
        source._bs_executor.shutdown(wait=False)
    after = source.get_quota_snapshot()
    with sqlite3.connect(args.database) as connection:
        snapshots = connection.execute(
            "SELECT index_instrument_id, effective_date, available_at, "
            "completeness_state, weight_unit, artifact_hash "
            "FROM index_composition_snapshots ORDER BY index_instrument_id"
        ).fetchall()
        counts = connection.execute(
            "SELECT s.index_instrument_id, COUNT(*) "
            "FROM index_composition_members m "
            "JOIN index_composition_snapshots s ON s.snapshot_id=m.snapshot_id "
            "GROUP BY s.index_instrument_id ORDER BY s.index_instrument_id"
        ).fetchall()
        integrity = connection.execute("PRAGMA quick_check").fetchone()[0]
    return {
        "result": result,
        "quota_before": before,
        "quota_after": after,
        "quota_delta": int(after["count"]) - int(before["count"]),
        "snapshots": [list(item) for item in snapshots],
        "member_counts": {item[0]: item[1] for item in counts},
        "quick_check": integrity,
        "database": str(args.database),
        "checkpoint": str(args.checkpoint),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2020-06-30")
    parser.add_argument(
        "--indexes", nargs="+", default=["000300.SH", "000905.SH", "000016.SH"]
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--daily-request-reserve", type=int, default=5000)
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
