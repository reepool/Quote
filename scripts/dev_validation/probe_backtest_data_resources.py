#!/usr/bin/env python3
"""Audit existing backtest-data capabilities without network or database writes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from research.backtest_data.catalog import load_default_catalog
from research.backtest_data.probes import BoundedProbeScope, ExistingResourceProbeSuite
from utils import config_manager


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identifiers", nargs="+", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--markets", nargs="*", default=[])
    parser.add_argument("--quotes-db")
    parser.add_argument("--financials-db")
    parser.add_argument("--research-db")
    parser.add_argument("--output", help="Optional non-production JSON audit artifact")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    research_config = config_manager.get_research_config()
    scope = BoundedProbeScope.build(
        identifiers=args.identifiers,
        start_date=args.start_date,
        end_date=args.end_date,
        markets=args.markets,
    )
    suite = ExistingResourceProbeSuite(
        catalog=load_default_catalog(),
        quotes_db_path=args.quotes_db or research_config.storage.quotes_db_path,
        financials_db_path=(
            args.financials_db or research_config.storage.financials_db_path
        ),
        research_db_path=args.research_db or research_config.storage.db_path,
    )
    payload = suite.run_all(scope)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
