#!/usr/bin/env python3
"""Evaluate one committed stage-five run against approved Gold and negative cases."""

from __future__ import annotations

import argparse
import hashlib
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
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--evaluation-id")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    benchmark = evaluate_committed_stage5_run(
        args.run_directory,
        gold_path=args.gold_path,
    )
    if args.output_path is not None and not args.evaluation_id:
        raise ValueError("--evaluation-id is required with --output-path")
    destination = args.output_path or (
        args.run_directory / "post-run-benchmark.json"
    )
    run_directory = args.run_directory.resolve()
    if args.output_path is not None and destination.resolve().is_relative_to(
        run_directory
    ):
        raise ValueError("offline evaluation output must be outside the input bundle")
    if destination.exists():
        raise FileExistsError(f"post-run benchmark already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object]
    if args.output_path is None:
        payload = benchmark.model_dump(mode="json")
    else:
        source_benchmark = args.run_directory / "post-run-benchmark.json"
        payload = {
            "schema_version": "company_profile_stage5_offline_evaluation.v1",
            "evaluation_id": args.evaluation_id,
            "runtime_source": {
                "run_id": benchmark.run_id,
                "run_directory": str(args.run_directory),
                "manifest_sha256": _sha256_file(
                    args.run_directory / "manifest.json"
                ),
                "source_benchmark_sha256": (
                    _sha256_file(source_benchmark)
                    if source_benchmark.is_file()
                    else None
                ),
            },
            "benchmark": benchmark.model_dump(mode="json"),
        }
    temporary = destination.parent / f".{destination.name}-{uuid.uuid4().hex}.tmp"
    encoded = json.dumps(
        payload,
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
