"""Operator tool: baseline, sample freeze and equivalence gate for the
narrow financial readiness path (openspec: accelerate-financial-readiness-
queries).

Subcommands:
  baseline     Time wide vs narrow readiness per candidate on production data.
  dump-sample  Freeze an equivalence sample set (JSON) for the dual-run gate.
  equivalence  Run the wide/narrow dual-run over a frozen sample; exit 1 on
               any mismatch.

Read-only against the configured financial database; safe to run while
nightly tasks are active. Run via the repo dev environment:
  bash scripts/quote_dev_env.sh python scripts/research_financial_readiness_perf.py <cmd> ...
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

MAPPING_VERSION = "sina_ths_core_financial_facts.v6"

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from research.storage import ResearchStorageManager
from utils.config_manager import config_manager

DEFAULT_SAMPLE_OUT = "tests/unit/test_research/fixtures/financial_readiness_equivalence_sample.json"
RECONCILIATION_PERIODS = [
    "2024-12-31",
    "2025-03-31",
    "2025-06-30",
    "2025-09-30",
    "2025-12-31",
    "2026-03-31",
    "2026-06-30",
]


def _build_manager() -> ResearchStorageManager:
    manager = ResearchStorageManager.__new__(ResearchStorageManager)
    financials = config_manager.get_nested("database_config.financial_readiness_db_path", None)
    db_path = Path(financials or "data/financials.db").resolve()
    manager.db_path = str(db_path)
    manager.financials_db_path = str(db_path)
    manager._active_db_path = None
    manager._db_route_state = threading.local()
    manager.change_watermark_config = {}
    manager.financial_readiness_narrow_rows = False
    return manager


def _pick_sample(manager: ResearchStorageManager, count: int, periods: list[str]) -> list[dict[str, Any]]:
    """Mix hot-table instruments, zero-data (local-gap) instruments and BSE."""
    with (
        manager.pinned_financial_read_connection(),
        manager.financial_database_scope(),
        manager.get_connection() as conn,
    ):
        hot = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT instrument_id FROM financial_numeric_facts_hot "
                "WHERE report_period = ? LIMIT ?",
                (periods[-1], count),
            )
        ]
        zero_data = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT instrument_id FROM financial_disclosure_event_state "
                "WHERE announcement_id LIKE 'local-gap:%' LIMIT ?",
                (max(4, count // 10),),
            )
        ]
        bse = [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT instrument_id FROM financial_numeric_facts_hot "
                "WHERE instrument_id LIKE '%.BJ' LIMIT ?",
                (max(4, count // 20),),
            )
        ]
    instruments: list[str] = []
    for pool in (zero_data, bse, hot):
        for item in pool:
            if item not in instruments:
                instruments.append(item)
        if len(instruments) >= count:
            break
    return [
        {"instrument_id": instrument_id, "report_periods": periods}
        for instrument_id in instruments[:count]
    ]


def _resolve_profiles(manager: ResearchStorageManager, sample: list[dict[str, Any]]) -> dict[str, str]:
    """Resolve each instrument's statement profile exactly like the service."""
    from research.financial_statement_profile import resolve_financial_statement_profile

    profiles: dict[str, str] = {}
    with (
        manager.pinned_financial_read_connection(),
        manager.financial_database_scope(),
        manager.get_connection(),
    ):
        for entry in sample:
            resolution = resolve_financial_statement_profile(instrument={"instrument_id": entry["instrument_id"]})
            profiles[entry["instrument_id"]] = resolution.profile if hasattr(resolution, "profile") else str(resolution)
    return profiles


def _run_readiness(
    manager: ResearchStorageManager,
    sample: list[dict[str, Any]],
    narrow: bool,
    profiles: dict[str, str] | None = None,
    mapping_version: str | None = None,
) -> list[dict[str, Any]]:
    manager.financial_readiness_narrow_rows = narrow
    out: list[dict[str, Any]] = []
    for entry in sample:
        instrument_id = entry["instrument_id"]
        profile = (profiles or {}).get(instrument_id)
        for period in entry["report_periods"]:
            result = manager.get_financial_local_core_facts(
                instrument_id,
                report_period=period,
                requested_canonical_facts=["revenue", "net_income_parent", "equity_parent",
                                           "total_assets", "total_liabilities"],
                profile=profile,
                mapping_version=mapping_version,
                include_history=True,
            )
            out.append(
                {
                    "instrument_id": instrument_id,
                    "report_period": period,
                    "ready": result.get("ready"),
                    "missing_fields": result.get("missing_fields"),
                    "facts": {
                        name: {
                            "report_period": fact.get("report_period"),
                            "fact_name": fact.get("fact_name"),
                            "fact_value": fact.get("fact_value"),
                            "unit": fact.get("unit"),
                            "updated_at": fact.get("updated_at"),
                        }
                        for name, fact in (result.get("facts") or {}).items()
                    },
                }
            )
    return out


def cmd_baseline(args: argparse.Namespace) -> int:
    manager = _build_manager()
    sample = _pick_sample(manager, args.count, RECONCILIATION_PERIODS)
    profiles = _resolve_profiles(manager, sample)
    print(f"sample: {len(sample)} instruments x {len(RECONCILIATION_PERIODS)} periods")
    if args.decompose:
        counts = {"connections": 0, "queries": 0}
        real_get_conn = manager.get_connection

        class _CountingConn:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, *a, **k):
                counts["queries"] += 1
                return self._inner.execute(sql, *a, **k)

            def __getattr__(self, name):
                return getattr(self._inner, name)

        @contextmanager
        def counting_conn():
            counts["connections"] += 1
            with real_get_conn() as c:
                yield _CountingConn(c)

        manager.get_connection = counting_conn
        try:
            n_evals = len(sample) * len(RECONCILIATION_PERIODS)
            started = time.monotonic()
            _run_readiness(manager, sample, narrow=False, profiles=profiles, mapping_version=MAPPING_VERSION)
            wide_elapsed = time.monotonic() - started
            print(
                f"decompose(wide): {counts['connections']} connections, {counts['queries']} queries "
                f"for {n_evals} evaluations -> {counts['connections'] / n_evals:.2f} conn, "
                f"{counts['queries'] / n_evals:.2f} queries per candidate, {wide_elapsed / n_evals * 1000:.1f} ms/candidate"
            )
        finally:
            del manager.get_connection
    timings: dict[str, float] = {}
    for label, narrow in (("wide", False), ("narrow", True)):
        started = time.monotonic()
        results = _run_readiness(manager, sample, narrow, profiles=profiles, mapping_version=MAPPING_VERSION)
        elapsed = time.monotonic() - started
        per_candidate = elapsed / max(1, len(results))
        timings[label] = per_candidate
        print(
            f"{label:6s}: {elapsed:.2f}s total | {per_candidate * 1000:.1f} ms/candidate "
            f"| {len(results)} evaluations"
        )
    if timings.get("wide"):
        speedup = timings["wide"] / max(timings.get("narrow") or 1e-9, 1e-9)
        print(f"speedup: {speedup:.1f}x")
        print(
            "projection: full scan of ~27.8k candidates -> "
            f"wide {27800 * timings['wide'] / 3600:.1f}h vs narrow {27800 * timings['narrow'] / 3600:.1f}h"
        )
    return 0


def cmd_dump_sample(args: argparse.Namespace) -> int:
    manager = _build_manager()
    sample = _pick_sample(manager, args.count, RECONCILIATION_PERIODS)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(sample, ensure_ascii=False, indent=1))
    print(f"frozen sample written: {out_path} ({len(sample)} instruments)")
    return 0


def cmd_equivalence(args: argparse.Namespace) -> int:
    manager = _build_manager()
    sample = json.loads(Path(args.sample_file).read_text())
    profiles = _resolve_profiles(manager, sample)
    print(
        f"equivalence gate: {len(sample)} instruments (dual run, wide vs narrow, "
        f"production shape: profile resolution + mapping_version={MAPPING_VERSION})"
    )
    wide = _run_readiness(manager, sample, narrow=False, profiles=profiles, mapping_version=MAPPING_VERSION)
    narrow = _run_readiness(manager, sample, narrow=True, profiles=profiles, mapping_version=MAPPING_VERSION)
    mismatches = 0
    for w, n in zip(wide, narrow):
        if w != n:
            mismatches += 1
            if mismatches <= 5:
                print(f"MISMATCH {w['instrument_id']} {w['report_period']}:")
                print(f"  wide  : ready={w['ready']} facts={list(w['facts'])}")
                print(f"  narrow: ready={n['ready']} facts={list(n['facts'])}")
    print(f"result: {len(wide)} evaluations, {mismatches} mismatches")
    return 1 if mismatches else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p_base = sub.add_parser("baseline", help="time wide vs narrow readiness")
    p_base.add_argument("--count", type=int, default=30)
    p_base.add_argument("--decompose", action="store_true", help="count connections/queries per candidate (wide)")
    p_dump = sub.add_parser("dump-sample", help="freeze an equivalence sample set")
    p_dump.add_argument("--count", type=int, default=220)
    p_dump.add_argument("--out", default=DEFAULT_SAMPLE_OUT)
    p_eq = sub.add_parser("equivalence", help="dual-run equivalence gate")
    p_eq.add_argument("--sample-file", default=DEFAULT_SAMPLE_OUT)
    args = parser.parse_args()
    return {
        "baseline": cmd_baseline,
        "dump-sample": cmd_dump_sample,
        "equivalence": cmd_equivalence,
    }[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
