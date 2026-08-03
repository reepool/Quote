#!/usr/bin/env python3
"""Validate business-profile shadow, pilot, expansion, and scheduler gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.business_profile_rollout import (
    evaluate_rollout_expansion,
    evaluate_scheduler_readiness,
    run_bounded_production_pilot,
    run_shadow_backfill_validation,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="business-profile-rollout-validation-") as temp_dir:
        source = Path(temp_dir) / "production-shaped.db"
        with sqlite3.connect(source) as conn:
            conn.execute(
                "CREATE TABLE governed_facts (fact_id TEXT PRIMARY KEY, status TEXT NOT NULL)"
            )

        def shadow_runner(path: Path):
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "INSERT OR IGNORE INTO governed_facts VALUES ('fact-1', 'approved')"
                )
                rows = conn.execute(
                    "SELECT fact_id, status FROM governed_facts ORDER BY fact_id"
                ).fetchall()
            return {
                "governed_output": rows,
                "bulk_transaction_ok": True,
                "point_in_time_reads_ok": True,
                "machine_rework_recovery_ok": True,
                "zero_valuation_leakage": True,
            }

        shadow = run_shadow_backfill_validation(source, shadow_runner)

    pilot = run_bounded_production_pilot(
        instrument_ids=["601088.SH", "600362.SH"],
        enabled_manifests={
            "atomic_activities": {"enabled": True, "manifest_hash": "isolated-test"}
        },
        runner=lambda instruments: {
            "zero_candidate_valuation_leakage": True,
            "audited_system_promotion": True,
            "bounded_scope": len(instruments) == 2,
        },
        rollback_drill=lambda: True,
        kill_switch_drill=lambda: True,
    )
    expansion = evaluate_rollout_expansion(
        {
            "precision": 0.995,
            "drift_rate": 0.01,
            "average_cost": 0.2,
            "human_exception_rate": 0.02,
        },
        {
            "min_precision": 0.99,
            "max_drift_rate": 0.02,
            "max_average_cost": 0.5,
            "max_human_exception_rate": 0.05,
        },
    )
    scheduler = evaluate_scheduler_readiness(
        issuer_count=5500,
        changed_issuer_rate=0.05,
        seconds_per_changed_issuer=10,
        available_window_seconds=3600,
        exception_backlog=20,
        maximum_exception_backlog=100,
    )
    report = {
        "schema_version": "business_profile_rollout_gate_validation.v1",
        "scope": "isolated_temporary_database",
        "production_writes_performed": False,
        "scheduler_enabled": False,
        "shadow": shadow,
        "pilot": pilot,
        "expansion": expansion,
        "scheduler_readiness": scheduler,
    }
    report["report_hash"] = hashlib.sha256(
        json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"output": str(args.output), "report_hash": report["report_hash"]}))
    return 0 if shadow["passed"] and pilot["passed"] and expansion["expand"] and scheduler["scheduler_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
