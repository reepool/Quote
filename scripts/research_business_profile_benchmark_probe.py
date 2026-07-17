#!/usr/bin/env python3
"""Probe official reports for a selected business-profile parser benchmark."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_benchmark_probe import probe_benchmark_documents


def run_benchmark_probe(
    *,
    benchmark_path: Path,
    industry_groups: Sequence[str] = (),
    instrument_ids: Sequence[str] = (),
    max_issuers: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    search_key: Optional[str] = "年度报告",
    category: Optional[str] = None,
    page_size: int = 30,
    max_pages: int = 5,
    download_root: Optional[Path] = None,
    max_documents_per_issuer: int = 1,
) -> Dict[str, Any]:
    """Load one benchmark selection and run a bounded read-only probe."""
    benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
    if not isinstance(benchmark, dict):
        raise ValueError("benchmark payload must be an object")
    return probe_benchmark_documents(
        benchmark,
        industry_groups=industry_groups,
        instrument_ids=instrument_ids,
        max_issuers=max_issuers,
        start_date=start_date,
        end_date=end_date,
        search_key=search_key,
        category=category,
        page_size=page_size,
        max_pages=max_pages,
        download_root=download_root,
        max_documents_per_issuer=max_documents_per_issuer,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--industry", action="append", default=[])
    parser.add_argument("--instrument", action="append", default=[])
    parser.add_argument("--max-issuers", type=int)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--search-key", default="年度报告")
    parser.add_argument("--category")
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--download-root", type=Path)
    parser.add_argument("--max-documents-per-issuer", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    payload = run_benchmark_probe(
        benchmark_path=args.benchmark,
        industry_groups=args.industry,
        instrument_ids=args.instrument,
        max_issuers=args.max_issuers,
        start_date=args.start_date,
        end_date=args.end_date,
        search_key=args.search_key,
        category=args.category,
        page_size=args.page_size,
        max_pages=args.max_pages,
        download_root=args.download_root,
        max_documents_per_issuer=args.max_documents_per_issuer,
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    else:
        print(rendered)
    return 0 if payload["status"] == "success" else 3


if __name__ == "__main__":
    raise SystemExit(main())
