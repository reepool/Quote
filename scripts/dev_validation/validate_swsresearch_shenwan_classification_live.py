#!/usr/bin/env python
"""Validate the live official SWS Shenwan classification XLS source."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from scripts.research_cli_support import (
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


async def validate_live_source(
    manager: Any,
    *,
    exchanges: Optional[List[str]],
    limit_per_exchange: Optional[int],
    force_refresh: bool,
    use_db_manifest: bool,
) -> Dict[str, Any]:
    """Download/parse the official XLS files and optionally measure DB target coverage."""
    from research.providers.registry import IndustryStandardProviderRegistry

    await initialize_manager_for_research_cli(manager)
    try:
        registry = IndustryStandardProviderRegistry(research_config=manager.research_config)
        provider = registry.get("swsresearch")
        if provider is None:
            raise RuntimeError("swsresearch industry standard provider is unavailable")

        previous_source_files: Dict[str, Dict[str, Any]] = {}
        storage = getattr(manager, "research_storage", None)
        if use_db_manifest and storage is not None:
            previous_source_files = storage.get_latest_industry_source_files(
                source=getattr(provider, "source_name", "swsresearch"),
                source_mode="direct",
                artifact_kinds=[
                    getattr(
                        provider,
                        "STOCK_HISTORY_ARTIFACT",
                        "shenwan_stock_classification_history",
                    ),
                    getattr(
                        provider,
                        "CODE_TABLE_ARTIFACT",
                        "shenwan_classification_code_table",
                    ),
                ],
            )

        bundle_fetcher = getattr(provider, "fetch_official_classification_bundle", None)
        if bundle_fetcher is None:
            raise RuntimeError("swsresearch provider does not expose official bundle fetcher")

        bundle = await bundle_fetcher(
            mode="direct",
            previous_source_files=previous_source_files,
            force_refresh=force_refresh,
        )

        taxonomy_level_counts = Counter(
            int(node.industry_level) for node in bundle.taxonomy_nodes
        )
        latest_exchange_counts = Counter(
            item.exchange for item in bundle.latest_classifications if item.exchange
        )
        result: Dict[str, Any] = {
            "status": "ok",
            "changed": bundle.changed,
            "unchanged_artifacts": bundle.unchanged_artifacts,
            "source_files": [
                {
                    "artifact_kind": item.artifact_kind,
                    "status": item.status,
                    "sha256": item.sha256,
                    "row_count": item.row_count,
                    "max_source_update_time": item.max_source_update_time,
                    "etag": item.etag,
                    "last_modified": item.last_modified,
                    "content_length": item.content_length,
                }
                for item in bundle.source_files
            ],
            "taxonomy": {
                "nodes": len(bundle.taxonomy_nodes),
                "level_counts": dict(sorted(taxonomy_level_counts.items())),
            },
            "classification_history_rows": len(bundle.history_rows),
            "latest_classifications": {
                "rows": len(bundle.latest_classifications),
                "by_exchange": dict(sorted(latest_exchange_counts.items())),
            },
            "diagnostics": bundle.diagnostics,
        }

        target_exchanges = exchanges or manager.research_config.markets
        if bundle.changed and target_exchanges:
            coverage = []
            total_targets = 0
            total_memberships = 0
            for exchange in target_exchanges:
                instruments = await manager.db_ops.get_instruments_by_exchange(exchange)
                stock_instruments = [
                    item
                    for item in instruments
                    if item.get("type") == "stock" and item.get("is_active", True)
                ]
                if limit_per_exchange is not None:
                    stock_instruments = stock_instruments[:limit_per_exchange]
                memberships = provider.build_latest_memberships(
                    instruments=stock_instruments,
                    taxonomy_nodes=bundle.taxonomy_nodes,
                    latest_classifications=bundle.latest_classifications,
                )
                target_count = len(stock_instruments)
                membership_count = len(memberships)
                membership_ids = {item.instrument_id for item in memberships}
                missing_instrument_ids = [
                    str(item.get("instrument_id") or "")
                    for item in stock_instruments
                    if str(item.get("instrument_id") or "") not in membership_ids
                ]
                total_targets += target_count
                total_memberships += membership_count
                coverage.append(
                    {
                        "exchange": exchange,
                        "targets": target_count,
                        "memberships": membership_count,
                        "coverage_ratio": None
                        if target_count == 0
                        else membership_count / target_count,
                        "missing_instrument_sample": missing_instrument_ids[:20],
                    }
                )
            result["target_coverage"] = {
                "exchanges": coverage,
                "targets": total_targets,
                "memberships": total_memberships,
                "coverage_ratio": None
                if total_targets == 0
                else total_memberships / total_targets,
            }

        return result
    finally:
        close = getattr(manager, "close", None)
        if close is not None:
            await close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate live official SWS Shenwan classification XLS files.",
    )
    parser.add_argument(
        "--exchanges",
        help="Comma-separated exchanges for target coverage, for example SSE,SZSE,BSE.",
    )
    parser.add_argument(
        "--limit-per-exchange",
        type=int,
        help="Optional per-exchange coverage limit.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Bypass conditional ETag/Last-Modified/sha256 short-circuit.",
    )
    parser.add_argument(
        "--use-db-manifest",
        action="store_true",
        help="Use the latest stored source-file manifests for conditional requests.",
    )
    parser.add_argument(
        "--fail-on-incomplete-coverage",
        action="store_true",
        help="Exit with code 2 when measured target coverage is below 100%%.",
    )
    return parser


async def _async_main(args: argparse.Namespace) -> int:
    from data_manager import data_manager

    result = await validate_live_source(
        data_manager,
        exchanges=parse_exchanges(args.exchanges),
        limit_per_exchange=args.limit_per_exchange,
        force_refresh=bool(args.force_refresh),
        use_db_manifest=bool(args.use_db_manifest),
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    if args.fail_on_incomplete_coverage:
        coverage = result.get("target_coverage") or {}
        ratio = coverage.get("coverage_ratio")
        if ratio is not None and ratio < 1.0:
            return 2
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
