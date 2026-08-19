#!/usr/bin/env python3
"""Select a read-only first-wave business-profile parser benchmark."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_benchmark import select_parser_benchmark
from research.business_profile_corpus import (
    apply_instrument_lifecycle,
    list_first_wave_universe,
    load_business_profile_source_manifests,
    load_instrument_lifecycle,
)


def build_parser_benchmark(
    *,
    research_db: Path,
    announcement_assets_db: Path,
    quotes_db: Path,
    as_of_date: str,
    evidence_path: Optional[Path] = None,
    issuers_per_industry: int = 5,
) -> Dict[str, Any]:
    """Build a benchmark payload without production database writes."""
    with _read_only_connection(research_db) as research_conn:
        universe = list_first_wave_universe(research_conn, as_of_date=as_of_date)
    with _read_only_connection(quotes_db) as quotes_conn:
        lifecycle = load_instrument_lifecycle(
            quotes_conn,
            [str(item["instrument_id"]) for item in universe],
        )
    universe = apply_instrument_lifecycle(
        universe,
        lifecycle,
        as_of_date=as_of_date,
    )
    with _read_only_connection(announcement_assets_db) as asset_conn:
        source_manifests = load_business_profile_source_manifests(
            asset_conn,
            [str(item["instrument_id"]) for item in universe],
        )
    evidence = _load_evidence(evidence_path)
    result = select_parser_benchmark(
        universe,
        evidence_profiles=evidence,
        source_manifests=source_manifests,
        issuers_per_industry=issuers_per_industry,
    )
    return {
        "as_of_date": as_of_date,
        "research_db": str(research_db),
        "announcement_assets_db": str(announcement_assets_db),
        "quotes_db": str(quotes_db),
        "evidence_path": str(evidence_path) if evidence_path else None,
        **result,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    parser.add_argument(
        "--announcement-assets-db", type=Path, default=Path("data/research.db")
    )
    parser.add_argument("--quotes-db", type=Path, default=Path("data/quotes.db"))
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--issuers-per-industry", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    payload = build_parser_benchmark(
        research_db=args.research_db,
        announcement_assets_db=args.announcement_assets_db,
        quotes_db=args.quotes_db,
        as_of_date=args.as_of_date,
        evidence_path=args.evidence,
        issuers_per_industry=args.issuers_per_industry,
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    else:
        print(rendered)
    return 0 if payload["status"] == "ready" else 3


def _read_only_connection(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)


def _load_evidence(path: Optional[Path]) -> Sequence[Mapping[str, Any]]:
    if path is None:
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("evidence_profiles")
    if not isinstance(payload, list):
        raise ValueError(
            "benchmark evidence must be a list or evidence_profiles object"
        )
    return [item for item in payload if isinstance(item, dict)]


if __name__ == "__main__":
    raise SystemExit(main())
