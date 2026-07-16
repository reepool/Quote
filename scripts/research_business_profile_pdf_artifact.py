#!/usr/bin/env python3
"""Build a compressed page artifact for one archived business-profile PDF."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_pdf_artifacts import (
    BusinessProfilePdfArtifactExtractor,
    BusinessProfilePdfArtifactStore,
)


def build_pdf_artifact(
    *,
    source_pdf: Path,
    source_file_id: Optional[str] = None,
    target_page_numbers: Sequence[int] = (),
    low_text_character_threshold: int = 40,
    diagnostics_only: bool = False,
) -> Dict[str, Any]:
    """Extract one local PDF and optionally persist its immutable artifact."""
    artifact = BusinessProfilePdfArtifactExtractor(
        low_text_character_threshold=low_text_character_threshold,
    ).extract_file(
        source_pdf,
        source_file_id=source_file_id,
        target_page_numbers=target_page_numbers,
    )
    write_result = None
    if not diagnostics_only:
        write_result = BusinessProfilePdfArtifactStore().write(artifact)
    return {
        "status": artifact.status,
        "source_file_id": source_file_id,
        "source_pdf": str(source_pdf),
        "source_content_hash": artifact.source_content_hash,
        "parameter_hash": artifact.parameter_hash,
        "artifact_hash": artifact.artifact_hash,
        "artifact_path": None if write_result is None else write_result.artifact_path,
        "write_status": None if write_result is None else write_result.status,
        "page_count": artifact.page_count,
        "heading_match_count": len(artifact.heading_index),
        "low_text_pages": artifact.low_text_pages,
        "ocr_required_pages": artifact.ocr_required_pages,
        "diagnostics": artifact.diagnostics,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_pdf", type=Path)
    parser.add_argument("--source-file-id")
    parser.add_argument(
        "--target-pages",
        help="Comma-separated one-based pages known to contain target sections",
    )
    parser.add_argument("--low-text-character-threshold", type=int, default=40)
    parser.add_argument("--diagnostics-only", action="store_true")
    args = parser.parse_args(argv)
    payload = build_pdf_artifact(
        source_pdf=args.source_pdf,
        source_file_id=args.source_file_id,
        target_page_numbers=_parse_pages(args.target_pages),
        low_text_character_threshold=args.low_text_character_threshold,
        diagnostics_only=args.diagnostics_only,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 2 if payload["status"] == "parse_failed" else 0


def _parse_pages(value: Optional[str]) -> Sequence[int]:
    if not value:
        return []
    pages = sorted({int(item.strip()) for item in value.split(",") if item.strip()})
    if any(page < 1 for page in pages):
        raise ValueError("target pages must be positive one-based page numbers")
    return pages


if __name__ == "__main__":
    raise SystemExit(main())
