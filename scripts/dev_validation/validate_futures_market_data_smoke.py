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
    FuturesBar,
    FuturesCalendarService,
    FuturesContinuousMapping,
    FuturesContract,
    FuturesContractBar,
    FuturesMarketDataSyncService,
    FuturesReadinessService,
    FuturesStorageManager,
    default_futures_registry,
    make_futures_contract_id,
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
    parser.add_argument(
        "--offline-fixture",
        action="store_true",
        help="Seed one local official-style contract/default-price fixture and skip remote provider calls.",
    )
    parser.add_argument("--output-path", type=Path, default=None)
    return parser


def _seed_offline_fixture(storage: FuturesStorageManager, series_id: str, trade_date: str) -> Dict[str, Any]:
    series = storage.get_series(series_id)
    if not series:
        return {"status": "skipped", "reason": "series_not_found", "series_id": series_id}
    instrument_id = str(series["instrument_id"])
    symbol = str(series.get("symbol") or instrument_id.split(".")[1])
    contract_code = f"{symbol.replace('0', '')}2407".upper()
    contract_id = make_futures_contract_id(instrument_id, contract_code)
    contract = FuturesContract(
        contract_id=contract_id,
        instrument_id=instrument_id,
        exchange=str(series.get("exchange") or instrument_id.split(".")[-1]),
        exchange_contract_code=contract_code,
        contract_month="2024-07",
        delivery_month="2024-07",
        currency=str(series.get("currency") or "CNY"),
        unit=str(series.get("unit") or ""),
        source="exchange_official",
        quality_flag="fixture",
        metadata={
            "source_profile": "exchange_official",
            "source_interface": "offline_fixture",
            "parser_version": "offline_fixture.v1",
        },
    )
    contract_bar = FuturesContractBar(
        contract_id=contract_id,
        instrument_id=instrument_id,
        trade_date=trade_date,
        open=10.0,
        high=12.0,
        low=9.0,
        close=11.0,
        settlement=10.5,
        volume=100.0,
        open_interest=200.0,
        amount=1234.0,
        currency=contract.currency,
        unit=contract.unit,
        source="exchange_official",
        source_mode="offline_fixture",
        source_profile="exchange_official",
        source_interface="offline_fixture",
        parser_version="offline_fixture.v1",
        quality_flag="fixture",
        raw_payload_hash=f"offline-contract:{contract_id}:{trade_date}",
    )
    mapping = FuturesContinuousMapping(
        series_id=series_id,
        trade_date=trade_date,
        contract_id=contract_id,
        exchange_contract_code=contract_code,
        instrument_id=instrument_id,
        construction_method="official_open_interest_main",
        construction_version="offline_fixture.v1",
        selection_open_interest=200.0,
        selection_volume=100.0,
        source_profile="exchange_official",
        quality_flag="fixture",
    )
    bar = FuturesBar(
        series_id=series_id,
        trade_date=trade_date,
        open=10.0,
        high=12.0,
        low=9.0,
        close=11.0,
        settlement=10.5,
        volume=100.0,
        open_interest=200.0,
        amount=1234.0,
        currency=contract.currency,
        unit=contract.unit,
        source="exchange_official",
        source_mode="offline_fixture",
        source_profile="exchange_official",
        source_interface="offline_fixture",
        parser_version="offline_fixture.v1",
        quality_flag="fixture",
        raw_payload_hash=f"offline-series:{series_id}:{trade_date}",
        metadata={
            "underlying_contract": contract_code,
            "underlying_contract_id": contract_id,
            "construction_method": "official_open_interest_main",
            "construction_version": "offline_fixture.v1",
        },
    )
    return {
        "status": "success",
        "contract": storage.upsert_contracts([contract]),
        "contract_bars": storage.upsert_contract_price_bars([contract_bar]),
        "mapping": storage.upsert_continuous_mappings([mapping]),
        "series_bars": storage.upsert_price_bars([bar]),
        "series_id": series_id,
        "instrument_id": instrument_id,
        "contract_id": contract_id,
        "trade_date": trade_date,
    }


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
    storage.upsert_categories(registry.get("categories", []))
    storage.upsert_instruments_and_series(registry["instruments"], registry["series"])
    storage.upsert_source_manifests(registry.get("source_manifests", []))
    calendar_result = FuturesCalendarService(storage, module_cfg).seed_default_calendar()

    fixture_result: Dict[str, Any] = {}
    if args.offline_fixture:
        fixture_date = args.end_date or args.start_date or "2024-06-03"
        fixture_result = _seed_offline_fixture(storage, series_ids[0], fixture_date)
        result = {
            "status": "success" if fixture_result.get("status") == "success" else "partial",
            "mode": "offline_fixture",
            "series_ids": series_ids,
            "totals": {"inserted": 1 if fixture_result.get("status") == "success" else 0},
            "source_selection": {"official_success": 1, "fallback_success": 0},
        }
    else:
        result = await FuturesMarketDataSyncService(storage, config).sync(
            series_ids=series_ids,
            start_date=args.start_date,
            end_date=args.end_date,
            mode=args.mode,
            dry_run=not args.write_enabled,
        )
    readiness = FuturesReadinessService(storage, module_cfg).build()
    first_series = storage.get_series(series_ids[0]) if series_ids else None
    dictionary = {
        "status": "success",
        "categories": storage.list_categories(active_only=True),
        "instruments": storage.list_instruments(active_only=True),
        "series": storage.list_series(active_only=True),
        "source_policy": "local_futures_db_only",
    }
    default_prices: Dict[str, Any] = {"status": "skipped", "reason": "series_not_found"}
    if first_series:
        resolved_series = storage.resolve_default_series(
            str(first_series["instrument_id"]),
            series_type=module_cfg.get("master_data", {}).get("default_research_series_type")
            or "main_continuous",
        )
        if resolved_series:
            price_rows = storage.get_price_bars(resolved_series["series_id"])
            mapping_rows = storage.list_continuous_mappings(resolved_series["series_id"])
            default_prices = {
                "status": "success",
                "series": resolved_series,
                "row_count": len(price_rows),
                "mapping": mapping_rows,
                "source_policy": "local_futures_db_only",
            }
        else:
            default_prices = {
                "status": "not_found",
                "instrument_id": str(first_series["instrument_id"]),
                "row_count": 0,
                "mapping": [],
            }
    payload: Dict[str, Any] = {
        "status": result.get("status"),
        "write_enabled": bool(args.write_enabled),
        "offline_fixture": bool(args.offline_fixture),
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
        "calendar": calendar_result,
        "fixture": fixture_result,
        "api_checks": {
            "dictionary": {
                "status": dictionary.get("status"),
                "category_count": len(dictionary.get("categories", [])),
                "instrument_count": len(dictionary.get("instruments", [])),
                "series_count": len(dictionary.get("series", [])),
            },
            "default_prices": {
                "status": default_prices.get("status"),
                "row_count": default_prices.get("row_count", 0),
                "mapping_count": len(default_prices.get("mapping", [])),
                "source_policy": default_prices.get("source_policy"),
            },
        },
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
