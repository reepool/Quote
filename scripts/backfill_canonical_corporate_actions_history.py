#!/usr/bin/env python3
"""Run the local-only canonical corporate-action historical projection."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.backtest_data.corporate_action_history_backfill import (
    CanonicalCorporateActionHistoryBackfill,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", default="data/quotes.db")
    parser.add_argument(
        "--checkpoint-root",
        default="data/runtime/canonical_corporate_action_backfill",
    )
    parser.add_argument("--checkpoint-id")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--instrument-id", action="append", dest="instrument_ids")
    parser.add_argument("--source-event-key", action="append", dest="source_event_keys")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write canonical revisions; without this flag the command is a dry-run.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Reprocess all batches instead of resuming completed checkpoint batches.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    result = CanonicalCorporateActionHistoryBackfill(
        Path(args.db_path),
        checkpoint_root=Path(args.checkpoint_root),
    ).run(
        dry_run=not args.write,
        batch_size=args.batch_size,
        resume=not args.restart,
        checkpoint_id=args.checkpoint_id,
        instrument_ids=args.instrument_ids,
        source_event_keys=args.source_event_keys,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result.get("status") in {"success", "dry_run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
