#!/usr/bin/env python3
"""Validate three-stage business-profile candidate backfill in an isolated /tmp DB."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_structured_sync import (
    WRITE_OPERATOR_SWITCH,
    StructuredBusinessProfileSyncService,
)
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchConfig, UnifiedConfigManager


def load_selection(path: Path) -> tuple[str, list[dict[str, Any]]]:
    """Load the frozen benchmark selector output as a sync universe."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    as_of_date = str(payload.get("as_of_date") or "")[:10]
    if not as_of_date:
        raise ValueError("selection is missing as_of_date")
    industries = payload.get("industries")
    if not isinstance(industries, Mapping):
        raise ValueError("selection is missing industries")
    universe = []
    for industry_group, result in industries.items():
        selected = (
            result.get("selected_issuers") if isinstance(result, Mapping) else None
        )
        if not isinstance(selected, list):
            raise ValueError(
                f"selection industry is missing selected_issuers: {industry_group}"
            )
        for issuer in selected:
            if not isinstance(issuer, Mapping):
                raise ValueError("selection selected_issuers must contain objects")
            universe.append(
                {
                    **dict(issuer),
                    "instrument_id": str(issuer.get("instrument_id") or "").strip(),
                    "industry_group": str(industry_group),
                    "exchange": str(issuer.get("exchange") or "").strip(),
                }
            )
    if not universe:
        raise ValueError("selection contains no issuers")
    instrument_ids = [str(item["instrument_id"]) for item in universe]
    if any(not instrument_id for instrument_id in instrument_ids):
        raise ValueError("selection contains an empty instrument_id")
    if len(set(instrument_ids)) != len(instrument_ids):
        raise ValueError("selection contains duplicate instruments")
    return as_of_date, universe


def build_isolated_config(
    base: ResearchConfig,
    *,
    temp_root: Path,
    max_instruments: int,
) -> ResearchConfig:
    """Clone production configuration while routing every writable path to /tmp."""
    root = _require_fresh_tmp_root(temp_root)
    config = deepcopy(base)
    config.enabled = True
    config.storage.db_path = str(root / "research.db")
    config.storage.attach_quotes_db = False
    config.storage.financials_db_path = str(root / "financials.db")
    config.storage.valuation_db_path = str(root / "valuation.db")
    config.storage.interests_db_path = str(root / "interests.db")
    config.storage.shadow_mode = True
    module = config.modules.setdefault("business_profile_evidence", {})
    sources = module.setdefault("free_structured_sources", {})
    sources["enabled"] = True
    sources["candidate_only"] = True
    runtime = sources.setdefault("runtime", {})
    runtime["max_instruments_per_run"] = max_instruments
    runtime["max_elapsed_seconds"] = max(
        300.0,
        float(runtime.get("max_elapsed_seconds") or 0),
    )
    runtime["raw_cache_root"] = str(root / "raw")
    runtime["checkpoint_root"] = str(root / "checkpoints")
    return config


async def validate_backfill(
    *,
    selection_path: Path,
    temp_root: Path,
    max_instruments: int = 30,
    max_elapsed_seconds: float = 300.0,
) -> dict[str, Any]:
    """Run initial, recovery, and replay stages against an isolated candidate DB."""
    as_of_date, universe = load_selection(selection_path)
    if len(universe) > max_instruments:
        raise ValueError(
            f"selection has {len(universe)} issuers, above max_instruments="
            f"{max_instruments}"
        )
    resolved_temp_root = _require_fresh_tmp_root(temp_root)
    base = UnifiedConfigManager("config").get_research_config()
    config = build_isolated_config(
        base,
        temp_root=resolved_temp_root,
        max_instruments=max_instruments,
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
    )
    common = {
        "as_of_date": as_of_date,
        "universe": universe,
        "max_instruments": len(universe),
        "max_elapsed_seconds": max_elapsed_seconds,
        "dry_run": False,
        "candidate_write": True,
        "operator_switch": WRITE_OPERATOR_SWITCH,
    }
    first_checkpoint = resolved_temp_root / "checkpoints" / "first.json"
    first = await service.sync(
        **common,
        checkpoint_path=first_checkpoint,
    )
    recovery = await service.sync(
        **common,
        checkpoint_path=first_checkpoint,
        resume=True,
    )
    replay = await service.sync(
        **common,
        checkpoint_path=resolved_temp_root / "checkpoints" / "replay.json",
    )
    database_audit = _audit_database(Path(config.storage.db_path))
    idempotent = (
        replay["candidate_evidence_written"] == 0
        and replay["candidate_segments_written"] == 0
    )
    zero_leakage = (
        database_audit["approved_record_count"] == 0
        and database_audit["value_chain_role_count"] == 0
        and database_audit["company_exposure_count"] == 0
    )
    passed = (
        first["status"] in {"success", "degraded"}
        and recovery["status"] == "success"
        and recovery["completed"] is True
        and replay["status"] == "success"
        and idempotent
        and zero_leakage
    )
    report = {
        "schema_version": "business_profile_temp_backfill_validation.v1",
        "status": "pass" if passed else "fail",
        "selection_path": str(selection_path),
        "selection_hash": _file_hash(selection_path),
        "temp_root": str(resolved_temp_root),
        "production_database_written": False,
        "instrument_count": len(universe),
        "first_run": first,
        "recovery_run": recovery,
        "replay_run": replay,
        "database_audit": database_audit,
        "idempotent_replay_run": idempotent,
        "zero_candidate_to_dcf_leakage": zero_leakage,
    }
    output = resolved_temp_root / "validation_report.json"
    output.write_text(
        f"{json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)}\n",
        encoding="utf-8",
    )
    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--temp-root", type=Path, required=True)
    parser.add_argument("--max-instruments", type=int, default=30)
    parser.add_argument("--max-elapsed-seconds", type=float, default=300.0)
    args = parser.parse_args(argv)
    report = asyncio.run(
        validate_backfill(
            selection_path=args.selection,
            temp_root=args.temp_root,
            max_instruments=args.max_instruments,
            max_elapsed_seconds=args.max_elapsed_seconds,
        )
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "instrument_count": report["instrument_count"],
                "temp_root": report["temp_root"],
                "first_candidates": {
                    "evidence": report["first_run"]["candidate_evidence_written"],
                    "segments": report["first_run"]["candidate_segments_written"],
                },
                "recovery_candidates": {
                    "evidence": report["recovery_run"]["candidate_evidence_written"],
                    "segments": report["recovery_run"]["candidate_segments_written"],
                },
                "replay_candidates": {
                    "evidence": report["replay_run"]["candidate_evidence_written"],
                    "segments": report["replay_run"]["candidate_segments_written"],
                },
                "database_audit": report["database_audit"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if report["status"] == "pass" else 3


def _require_fresh_tmp_root(path: Path) -> Path:
    resolved = Path(path).resolve()
    tmp = Path("/tmp").resolve()
    if resolved == tmp or tmp not in resolved.parents:
        raise ValueError("temp_root must be a child of /tmp")
    if resolved.exists() and any(resolved.iterdir()):
        raise FileExistsError(f"temp_root must be empty: {resolved}")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _audit_database(path: Path) -> dict[str, int]:
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True) as conn:
        candidate_evidence = _count(
            conn,
            "business_profile_evidence",
            "review_status = 'candidate'",
        )
        candidate_segments = _count(
            conn,
            "company_business_segments",
            "review_status = 'candidate'",
        )
        approved = _count(
            conn,
            "business_profile_evidence",
            "review_status = 'approved'",
        ) + _count(
            conn,
            "company_business_segments",
            "review_status = 'approved'",
        )
        return {
            "candidate_evidence_count": candidate_evidence,
            "candidate_segment_count": candidate_segments,
            "approved_record_count": approved,
            "value_chain_role_count": _count(conn, "company_value_chain_roles"),
            "company_exposure_count": _count(
                conn,
                "company_commodity_exposures",
            ),
            "ingestion_run_count": _count(
                conn,
                "ingestion_runs",
                "domain = 'business_profile_structured'",
            ),
        }


def _count(
    conn: sqlite3.Connection,
    table: str,
    where: Optional[str] = None,
) -> int:
    clause = f" WHERE {where}" if where else ""
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}{clause}").fetchone()[0])


def _file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
