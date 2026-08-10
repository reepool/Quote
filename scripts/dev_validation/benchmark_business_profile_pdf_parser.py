"""Run a bounded read-only benchmark over explicit cached annual-report PDFs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from research.business_profile_pdf_benchmark import (
    DEFAULT_CONCURRENCY_MATRIX,
    run_pdf_parser_benchmark,
    write_benchmark_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdf", action="append", default=[], help="Explicit local PDF")
    parser.add_argument(
        "--manifest",
        action="append",
        default=[],
        help="JSON manifest whose rows include path and sha256/content_hash",
    )
    parser.add_argument(
        "--concurrency",
        default=",".join(str(value) for value in DEFAULT_CONCURRENCY_MATRIX),
        help="Comma-separated pypdf worker matrix (default: 4,6,8)",
    )
    parser.add_argument("--max-documents", type=int, default=8)
    parser.add_argument("--max-total-mib", type=int, default=512)
    parser.add_argument("--max-elapsed-seconds", type=float, default=600.0)
    parser.add_argument(
        "--report",
        help="Optional explicit JSON report path; otherwise the report is stdout only",
    )
    args = parser.parse_args()
    matrix = tuple(
        int(value.strip()) for value in args.concurrency.split(",") if value.strip()
    )
    report = run_pdf_parser_benchmark(
        pdf_paths=args.pdf,
        manifest_paths=args.manifest,
        concurrency_matrix=matrix,
        max_documents=args.max_documents,
        max_total_bytes=args.max_total_mib * 1024 * 1024,
        max_elapsed_seconds=args.max_elapsed_seconds,
    )
    if args.report:
        output_path = write_benchmark_report(report, Path(args.report))
        print(f"report={output_path}")
        console_payload = {
            "schema_version": report["schema_version"],
            "corpus_hash": report["corpus_hash"],
            "recommendation": report["recommendation"],
            "trials": [
                {
                    "concurrency": trial["concurrency"],
                    "wall_seconds": trial["wall_seconds"],
                    "throughput_documents_per_second": trial[
                        "throughput_documents_per_second"
                    ],
                    "peak_concurrency": trial["peak_concurrency"],
                    "warning_count": trial["warning_count"],
                    "failed_documents": trial["failed_documents"],
                    "timed_out": trial["timed_out"],
                    "fidelity": trial["fidelity"],
                }
                for trial in report["trials"]
            ],
        }
    else:
        console_payload = report
    print(json.dumps(console_payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
