#!/usr/bin/env python3
"""Apply source-bound Stage 5 Activity decisions to one committed report offline."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.company_profile.stage5_bundle import Stage5OfflineActivityReviewRequest
from research.company_profile.stage5_service import (
    ManufacturingMaterialsProfileSliceService,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-directory", required=True, type=Path)
    parser.add_argument("--review-request", required=True, type=Path)
    parser.add_argument("--output-path", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        request = Stage5OfflineActivityReviewRequest.model_validate_json(
            args.review_request.read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ValueError("offline Activity review request is unreadable") from exc

    run_directory = args.run_directory.resolve()
    destination = args.output_path.resolve()
    if destination.is_relative_to(run_directory):
        raise ValueError("offline Activity review output must be outside the input run")
    if destination.exists():
        raise FileExistsError(f"offline Activity review output exists: {destination}")

    result = ManufacturingMaterialsProfileSliceService().apply_committed_activity_reviews(
        run_directory=run_directory,
        review_request=request,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}-{uuid.uuid4().hex}.tmp"
    encoded = json.dumps(
        result.model_dump(mode="json"),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
