#!/usr/bin/env python3
"""Validate multi-year index-history behavior from a live-probe seed.

The seed memberships are real BaoStock observations. Annual changes in this
offline replay are deterministic fixtures and do not assert historical facts.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.backtest_data.index_constituent_history_backfill import (
    CoreIndexConstituentHistoryBackfill,
)


INDEXES = ("000300.SH", "000905.SH", "000016.SH")


def _source_code(instrument_id: str) -> str:
    symbol, exchange = instrument_id.split(".", 1)
    if exchange not in {"SH", "SZ"}:
        raise ValueError(f"unsupported constituent exchange: {instrument_id}")
    return f"{exchange.lower()}.{symbol}"


def _load_seed_members(database: Path) -> Dict[str, List[str]]:
    with sqlite3.connect(database) as connection:
        rows = connection.execute(
            "SELECT s.index_instrument_id, m.constituent_instrument_id "
            "FROM index_composition_members m "
            "JOIN index_composition_snapshots s ON s.snapshot_id=m.snapshot_id "
            "ORDER BY s.index_instrument_id, m.constituent_instrument_id"
        ).fetchall()
    seeds: Dict[str, List[str]] = {index_id: [] for index_id in INDEXES}
    for index_id, member_id in rows:
        if index_id in seeds:
            seeds[index_id].append(member_id)
    expected = {"000300.SH": 300, "000905.SH": 500, "000016.SH": 50}
    actual = {index_id: len(members) for index_id, members in seeds.items()}
    if actual != expected:
        raise ValueError(f"live-probe seed counts do not match: {actual}")
    return seeds


def _annual_variants(seeds: Dict[str, List[str]]) -> Dict[str, List[List[str]]]:
    universe = sorted({member for members in seeds.values() for member in members})
    variants: Dict[str, List[List[str]]] = {}
    for index_id, members in seeds.items():
        alternatives = [member for member in universe if member not in members]
        if len(alternatives) < 4:
            raise ValueError(f"not enough seed alternatives for {index_id}")
        annual: List[List[str]] = []
        for position in range(5):
            variant = list(members)
            if position:
                variant[position - 1] = alternatives[position - 1]
            annual.append(sorted(variant))
        variants[index_id] = annual
    return variants


def _observation_dates() -> List[date]:
    return [
        date(year, month, 15)
        for year in range(2018, 2023)
        for month in range(1, 13)
    ]


def _table_counts(database: Path) -> Dict[str, int]:
    tables = (
        "index_composition_snapshots",
        "index_composition_members",
        "index_composition_validity_revisions",
        "data_change_log",
    )
    with sqlite3.connect(database) as connection:
        return {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    if args.database.exists() or args.checkpoint.exists():
        raise FileExistsError("temporary database and checkpoint paths must be new")
    seeds = _load_seed_members(args.probe_database)
    variants = _annual_variants(seeds)
    observation_dates = _observation_dates()
    fetch_calls: List[str] = []

    async def fetch(index_id: str, observation_date: date) -> List[Dict[str, str]]:
        fetch_calls.append(f"{index_id}:{observation_date.isoformat()}")
        members = variants[index_id][observation_date.year - 2018]
        return [
            {"code": _source_code(member), "code_name": f"Fixture {member}"}
            for member in members
        ]

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=args.database,
        checkpoint_path=args.checkpoint,
        fetcher=fetch,
        quota_reader=lambda: {
            "date": "offline",
            "count": 0,
            "limit": 40000,
            "remaining": 40000,
        },
    )
    plan = service.build_plan(
        start_date=observation_dates[0],
        end_date=observation_dates[-1],
        trading_dates=observation_dates,
        indexes=INDEXES,
        daily_request_reserve=5000,
        sampling="monthly",
        max_queries_per_run=args.chunk_queries,
    )
    runs: List[Dict[str, Any]] = []
    while True:
        result = await service.run(plan, resume=True)
        runs.append({
            "status": result["status"],
            "network_requests": result["network_requests"],
            "inserted": result["inserted"],
            "collapsed_observations": result["collapsed_observations"],
            "blockers": result["blockers"],
        })
        if result["status"] == "success":
            break
        if result["blockers"] != ["batch_query_limit_reached"]:
            raise RuntimeError(f"unexpected partial result: {result}")
        if len(runs) > plan.query_count:
            raise RuntimeError("resume did not converge")

    counts_before_replay = _table_counts(args.database)
    replay = await service.run(plan, resume=True)
    counts_after_replay = _table_counts(args.database)
    if replay["network_requests"] != 0 or counts_after_replay != counts_before_replay:
        raise AssertionError("idempotent replay changed persisted state")

    strict_future = service.store.list_index_constituents(
        "000300.SH",
        as_of_date="2020-06-15",
        known_at="2099-01-01T00:00:00+08:00",
        limit=100,
    )
    strict_before_acquisition = service.store.list_index_constituents(
        "000300.SH",
        as_of_date="2020-06-15",
        known_at="2017-01-01T00:00:00+08:00",
    )
    first_page = service.store.list_index_constituents(
        "000300.SH",
        as_of_date="2020-06-15",
        known_at="2099-01-01T00:00:00+08:00",
        limit=100,
        offset=0,
    )
    second_page = service.store.list_index_constituents(
        "000300.SH",
        as_of_date="2020-06-15",
        known_at="2099-01-01T00:00:00+08:00",
        limit=100,
        offset=100,
    )
    first_ids = {item["constituent_instrument_id"] for item in first_page["items"]}
    second_ids = {item["constituent_instrument_id"] for item in second_page["items"]}

    with sqlite3.connect(args.database) as connection:
        integrity = connection.execute("PRAGMA quick_check").fetchone()[0]
        duplicate_members = connection.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT snapshot_id, constituent_instrument_id, COUNT(*) AS n "
            "FROM index_composition_members GROUP BY snapshot_id, constituent_instrument_id "
            "HAVING n > 1)"
        ).fetchone()[0]
        null_weights = connection.execute(
            "SELECT COUNT(*) FROM index_composition_members WHERE weight IS NOT NULL"
        ).fetchone()[0]
    checkpoint = json.loads(args.checkpoint.read_text(encoding="utf-8"))

    assertions = {
        "quick_check_ok": integrity == "ok",
        "all_units_checkpointed": len(checkpoint["completed_units"]) == plan.query_count,
        "bounded_resume_used": len(runs) > 1,
        "strict_pit_ready": strict_future["status"] == "success",
        "pre_acquisition_fails_closed": strict_before_acquisition["status"] == "unavailable",
        "membership_ready_weights_deferred": strict_future.get("readiness") == {
            "membership": "ready",
            "weights": "deferred",
        },
        "pagination_total_stable": first_page["total"] == second_page["total"] == 300,
        "pagination_non_overlapping": not (first_ids & second_ids),
        "no_duplicate_members": duplicate_members == 0,
        "all_weights_null": null_weights == 0,
        "idempotent_counts": counts_before_replay == counts_after_replay,
    }
    if not all(assertions.values()):
        raise AssertionError(f"temporary validation failed: {assertions}")
    return {
        "mode": "offline_fixture_replay_from_live_probe_seed",
        "source_accuracy_claimed": False,
        "probe_database": str(args.probe_database),
        "database": str(args.database),
        "checkpoint": str(args.checkpoint),
        "plan_hash": plan.identity,
        "indexes": list(INDEXES),
        "range": [plan.start_date.isoformat(), plan.end_date.isoformat()],
        "observation_dates": len(plan.observation_dates),
        "planned_queries": plan.query_count,
        "fetch_calls": len(fetch_calls),
        "runs": runs,
        "counts": counts_after_replay,
        "replay": replay,
        "assertions": assertions,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--probe-database", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--chunk-queries", type=int, default=17)
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
