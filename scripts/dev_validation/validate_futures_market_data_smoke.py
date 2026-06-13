#!/usr/bin/env python
"""Run a bounded futures market-data smoke against a disposable futures.db."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.futures_market_data import (  # noqa: E402
    FuturesMarketDataSyncService,
    FuturesReadinessService,
    FuturesStorageManager,
    default_futures_registry,
)
from scripts.research_cli_support import json_ready  # noqa: E402
from utils.config_manager import UnifiedConfigManager  # noqa: E402


def _csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [item.strip() for item in str(raw).split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate futures market-data sync/readiness on a bounded series set."
    )
    parser.add_argument(
        "--series-ids",
        default="CNF.CU.SHFE.main",
        help="Comma-separated futures series ids to sync.",
    )
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--mode", default="direct", choices=["direct", "proxy_patch"])
    parser.add_argument("--timeout-seconds", type=float, default=None)
    parser.add_argument(
        "--disable-official",
        action="store_true",
        help="Skip official exchange providers and exercise fallback routing only.",
    )
    parser.add_argument(
        "--disable-fallback",
        action="store_true",
        help="Disable AkShare fallback to validate official-only behavior.",
    )
    parser.add_argument(
        "--db-path",
        default="/tmp/quote_futures_market_data_smoke.db",
        help="Smoke database path. Defaults to /tmp to avoid writing production data.",
    )
    parser.add_argument(
        "--write-enabled",
        action="store_true",
        help="Persist fetched bars into the smoke db. Without this, only metadata/run rows are written.",
    )
    parser.add_argument("--output-path", type=Path, default=None)
    return parser


async def async_main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    series_ids = _csv(args.series_ids)
    config = UnifiedConfigManager().get_research_config()
    module_cfg = config.modules.setdefault("commodity_market_data", {})
    module_cfg.setdefault("storage", {})["database"] = args.db_path
    sources_cfg = module_cfg.setdefault("sources", {})
    sources_cfg.setdefault("exchange_official", {})["enabled"] = not args.disable_official
    sources_cfg.setdefault("akshare_futures", {})["enabled"] = not args.disable_fallback
    if args.timeout_seconds is not None:
        sources_cfg.setdefault("exchange_official", {})["timeout_seconds"] = args.timeout_seconds
        sources_cfg.setdefault("akshare_futures", {})["timeout_seconds"] = args.timeout_seconds

    storage = FuturesStorageManager(config, db_path=args.db_path)
    storage.initialize()
    registry = default_futures_registry(module_cfg)
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])
    storage.upsert_source_manifests(registry.get("source_manifests", []))

    result = await FuturesMarketDataSyncService(storage, config).sync(
        series_ids=series_ids,
        start_date=args.start_date,
        end_date=args.end_date,
        mode=args.mode,
        dry_run=not args.write_enabled,
    )
    readiness = FuturesReadinessService(storage, module_cfg).build()
    payload: Dict[str, Any] = {
        "status": result.get("status"),
        "write_enabled": bool(args.write_enabled),
        "db_path": args.db_path,
        "series_ids": series_ids,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "mode": args.mode,
        "official_enabled": not args.disable_official,
        "fallback_enabled": not args.disable_fallback,
        "timeout_seconds": args.timeout_seconds,
        "sync": result,
        "readiness": readiness,
    }
    output = json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0 if payload["status"] == "success" else 2


def main() -> int:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        code = loop.run_until_complete(async_main())
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(locals().get("code", 1))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
