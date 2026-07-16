#!/usr/bin/env python3
"""Audit the first-wave business-profile corpus without production writes."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_corpus import (
    apply_instrument_lifecycle,
    discover_annotation_files,
    list_first_wave_universe,
    load_business_profile_source_manifests,
    load_instrument_lifecycle,
    summarize_corpus_readiness,
)


def build_corpus_audit(
    *,
    research_db: Path,
    financials_db: Path,
    quotes_db: Path,
    as_of_date: str,
    annotation_root: Optional[Path] = None,
    include_delisted: bool = False,
    expected_report_periods: Sequence[str] = (),
) -> Dict[str, Any]:
    """Build the audit payload using read-only SQLite connections."""
    with _read_only_connection(research_db) as research_conn:
        universe = list_first_wave_universe(research_conn, as_of_date=as_of_date)
    if quotes_db.exists():
        with _read_only_connection(quotes_db) as quotes_conn:
            lifecycle = load_instrument_lifecycle(
                quotes_conn,
                [str(item["instrument_id"]) for item in universe],
            )
        universe = apply_instrument_lifecycle(
            universe,
            lifecycle,
            as_of_date=as_of_date,
            include_delisted=include_delisted,
        )
    manifests = []
    if financials_db.exists():
        with _read_only_connection(financials_db) as financials_conn:
            manifests = load_business_profile_source_manifests(
                financials_conn,
                [str(item["instrument_id"]) for item in universe],
            )
    annotations = discover_annotation_files(annotation_root)
    return {
        "as_of_date": as_of_date,
        "research_db": str(research_db),
        "financials_db": str(financials_db),
        "quotes_db": str(quotes_db),
        "annotation_root": str(annotation_root) if annotation_root else None,
        "readiness": summarize_corpus_readiness(
            universe,
            source_manifests=manifests,
            annotation_files=annotations,
            expected_report_periods=expected_report_periods,
        ),
        "universe": universe,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--research-db", type=Path, default=Path("data/research.db"))
    parser.add_argument(
        "--financials-db", type=Path, default=Path("data/financials.db")
    )
    parser.add_argument("--quotes-db", type=Path, default=Path("data/quotes.db"))
    parser.add_argument("--as-of-date", default=date.today().isoformat())
    parser.add_argument("--annotation-root", type=Path)
    parser.add_argument(
        "--report-periods",
        help="Comma-separated expected report periods; defaults to two annuals and one semiannual",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include-universe", action="store_true")
    parser.add_argument("--include-delisted", action="store_true")
    args = parser.parse_args(argv)
    expected_report_periods = _parse_report_periods(
        args.report_periods,
        as_of_date=args.as_of_date,
    )
    payload = build_corpus_audit(
        research_db=args.research_db,
        financials_db=args.financials_db,
        quotes_db=args.quotes_db,
        as_of_date=args.as_of_date,
        annotation_root=args.annotation_root,
        include_delisted=args.include_delisted,
        expected_report_periods=expected_report_periods,
    )
    if not args.include_universe:
        payload.pop("universe", None)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


def _read_only_connection(path: Path) -> sqlite3.Connection:
    if not path.exists():
        raise FileNotFoundError(path)
    return sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)


def _parse_report_periods(raw: Optional[str], *, as_of_date: str) -> Sequence[str]:
    if raw:
        return [item.strip() for item in raw.split(",") if item.strip()]
    year = int(str(as_of_date)[:4])
    return [
        f"{year - 2}-12-31",
        f"{year - 1}-06-30",
        f"{year - 1}-12-31",
    ]


if __name__ == "__main__":
    raise SystemExit(main())
