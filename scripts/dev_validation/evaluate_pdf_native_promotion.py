#!/usr/bin/env python3
"""Compare PDFium-first and pypdf rollback native profiles on frozen assets."""

from __future__ import annotations

import argparse
from dataclasses import replace

from research.document_processing.pdf import (
    DEFAULT_PROFILES,
    PdfAcceptanceGates,
    PdfProfile,
    assess_report,
    evaluate_cases,
    load_manifest,
    write_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    pdfium = DEFAULT_PROFILES["pdfium_native"]
    pypdf = replace(pdfium, name="pypdf_native_promotion_baseline", native_engines=("pypdf",))
    cases = tuple(case for case in load_manifest(args.manifest) if case.ocr_mode == "none")
    report = evaluate_cases(cases, (pdfium, pypdf))
    # Fidelity is decisive: a profile must preserve labelled evidence, native
    # selections and negative-OCR pages before its latency can be considered.
    report["native_promotion_assessment"] = assess_report(
        report,
        PdfAcceptanceGates(min_chinese_exact_match=1.0, min_numeric_exact_match=1.0),
    )
    write_report(report, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
