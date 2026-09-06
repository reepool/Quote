#!/usr/bin/env python3
"""Evaluate one committed stage-five run against approved Gold and negative cases."""

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

from research.company_profile.stage5_benchmark import evaluate_committed_stage5_run

DEFAULT_GOLD_PATH = (
    ROOT_DIR
    / "docs/development/company_profile_manufacturing_materials_gold_annotations.v1.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-directory", required=True, type=Path)
    parser.add_argument("--gold-path", type=Path, default=DEFAULT_GOLD_PATH)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    benchmark = evaluate_committed_stage5_run(
        args.run_directory,
        gold_path=args.gold_path,
    )
    destination = args.run_directory / "post-run-benchmark.json"
    if destination.exists():
        raise FileExistsError(f"post-run benchmark already exists: {destination}")
    temporary = args.run_directory / f".post-run-benchmark-{uuid.uuid4().hex}.tmp"
    encoded = json.dumps(
        benchmark.model_dump(mode="json"),
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
