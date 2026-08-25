#!/usr/bin/env python3
"""Run a bounded, hash-verified PDF page-recovery evaluation."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace

from research.document_processing.pdf import (
    DEFAULT_PROFILES,
    assess_gpu_canary,
    evaluate_cases,
    load_manifest,
    write_report,
)


def _exit_code(approval: dict | None) -> int:
    return 2 if approval is not None and not approval["gpu_canary_approved"] else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--profile", choices=sorted(DEFAULT_PROFILES), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--gpu-approval-output")
    parser.add_argument("--cache-dir", default=os.environ.get("QUOTE_PDF_OCR_CACHE_DIR"))
    parser.add_argument("--max-cases", type=int)
    parser.add_argument("--max-pages", type=int)
    parser.add_argument("--max-elapsed-seconds", type=float)
    args = parser.parse_args()

    profile = DEFAULT_PROFILES[args.profile]
    if args.cache_dir:
        profile = replace(profile, ocr_model_cache_dir=args.cache_dir)
    cases = load_manifest(args.manifest)
    report = evaluate_cases(
        cases,
        [profile],
        max_cases=args.max_cases,
        max_pages=args.max_pages,
        max_elapsed_seconds=args.max_elapsed_seconds,
    )
    write_report(report, args.output)
    approval = None
    if profile.ocr_device.startswith("gpu"):
        approval = assess_gpu_canary(report)
        if not args.gpu_approval_output:
            raise ValueError("--gpu-approval-output is required for a GPU profile")
        write_report(approval, args.gpu_approval_output)
    summary = {
        "schema_version": report["schema_version"],
        "read_only": report["read_only"],
        "corpus_hash": report["corpus_hash"],
        "profiles": [
            {
                "profile": item["profile"],
                "success_rate": item["success_rate"],
                "p95_seconds": item["p95_seconds"],
                "ocr_page_p95_seconds": item["ocr_page_p95_seconds"],
                "ocr_pages": item["ocr_pages"],
            }
            for item in report["profiles"]
        ],
        "gpu_canary_approved": approval["gpu_canary_approved"] if approval else None,
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return _exit_code(approval)


if __name__ == "__main__":
    raise SystemExit(main())
