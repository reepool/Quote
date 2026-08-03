#!/usr/bin/env python3
"""Run one bounded business-profile semantic production stage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.business_profile_semantic_pipeline import (
    PIPELINE_MODES,
    BusinessProfileSemanticPipeline,
    SemanticProductionCheckpointStore,
    SemanticProductionScope,
    parse_semantic_production_config,
)


def _artifact_handler(stage: str):
    def run(**kwargs: Any) -> Mapping[str, Any]:
        payload = dict(kwargs["payload"])
        stage_payload = payload.get(stage)
        if not isinstance(stage_payload, Mapping):
            raise ValueError(
                f"input artifact must contain an object for stage: {stage}"
            )
        return {
            "status": str(stage_payload.get("status") or "success"),
            "reason": stage_payload.get("reason"),
            "artifact": stage_payload.get("artifact"),
            "metrics": dict(stage_payload.get("metrics") or {}),
        }

    return run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=PIPELINE_MODES)
    parser.add_argument("--instrument", action="append", required=True)
    parser.add_argument("--field-family", action="append", required=True)
    parser.add_argument("--knowledge-cutoff", required=True)
    parser.add_argument("--identities", required=True, type=Path)
    parser.add_argument("--promotion-manifests", type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--checkpoint", required=True, type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = parse_semantic_production_config(_read_json(args.config))
    scope = SemanticProductionScope(
        instruments=tuple(args.instrument),
        field_families=tuple(args.field_family),
        knowledge_cutoff=args.knowledge_cutoff,
        identities=_read_json(args.identities),
        promotion_manifest_hashes=(
            _read_json(args.promotion_manifests) if args.promotion_manifests else {}
        ),
    )
    pipeline = BusinessProfileSemanticPipeline(
        config=config,
        checkpoint_store=SemanticProductionCheckpointStore(args.checkpoint),
        handlers={stage: _artifact_handler(stage) for stage in ("plan", "select", "extract", "verify", "promote")},
    )
    result = pipeline.run(
        args.mode,
        scope=scope,
        payload=_read_json(args.input) if args.input else {},
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("status") not in {"stopped"} else 2


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return payload


if __name__ == "__main__":
    sys.exit(main())
