#!/usr/bin/env python3
"""Persist the current production required-set measurement without mutation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import AnnouncementAssetConfig
from research.announcement_assets.capacity_artifact import measure_required_set_evidence
from scripts.dev_validation.inventory_announcement_asset_capacity import (
    _validate_new_output_path,
    _write_new_json,
)
from utils.config_manager import config_manager


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(), project_root=PROJECT_ROOT
    )
    payload = measure_required_set_evidence(config)
    _write_new_json(output, payload)
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
