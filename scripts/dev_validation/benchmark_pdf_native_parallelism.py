#!/usr/bin/env python3
"""Read-only benchmark for isolated native PDF worker parallelism.

Usage requires explicit local PDF paths. The default matrix is 1, 2, 4, 6, 8,
and 10 workers; it never changes production configuration or source assets.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.document_processing.pdf import (
    PdfEvaluationCase,
    benchmark_native_parallelism,
    compute_content_hash,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", action="append", required=True, help="explicit local PDF path; repeat for multiple documents")
    parser.add_argument("--pages", nargs="*", type=int, default=(), help="physical pages to parse for every PDF")
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--widths", nargs="+", type=int, default=(1, 2, 4, 6, 8, 10))
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    cases = []
    for raw_path in args.pdf:
        path = Path(raw_path)
        content = path.read_bytes()
        if not content.lstrip().startswith(b"%PDF-"):
            raise ValueError(f"invalid PDF signature: {path}")
        cases.append(PdfEvaluationCase(
            case_id=path.stem,
            pdf_path=str(path),
            content_hash=compute_content_hash(content),
            page_count=None,
            target_pages=tuple(args.pages),
        ))
    report = benchmark_native_parallelism(
        cases,
        widths=tuple(args.widths),
        rounds=args.rounds,
        timeout_seconds=args.timeout_seconds,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
