#!/usr/bin/env python3
"""Run bounded read-only live checks for business-profile discovery sources."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.business_profile_exchange_discovery import (  # noqa: E402
    BusinessProfileDiscoveryCoordinator,
)
from utils.config_manager import UnifiedConfigManager  # noqa: E402


INSTRUMENT_SUFFIX_EXCHANGES = {
    "SH": "SSE",
    "SZ": "SZSE",
    "BJ": "BSE",
}


def parse_instrument_id(value: str) -> Dict[str, str]:
    """Parse one canonical A-share instrument id for a live probe."""
    instrument_id = str(value or "").strip().upper()
    if "." not in instrument_id:
        raise ValueError(f"instrument id must use canonical code.suffix form: {value}")
    symbol, suffix = instrument_id.rsplit(".", 1)
    exchange = INSTRUMENT_SUFFIX_EXCHANGES.get(suffix)
    if exchange is None or not symbol.isdigit() or len(symbol) != 6:
        raise ValueError(f"unsupported A-share instrument id: {value}")
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
    }


def run_live_discovery_probe(
    instrument_ids: Sequence[str],
    *,
    start_date: str,
    end_date: str,
    mode: str = "chain",
    page_size: int = 10,
    max_pages: int = 1,
    search_key: Optional[str] = "年度报告",
    max_instruments: int = 10,
    coordinator: Optional[BusinessProfileDiscoveryCoordinator] = None,
) -> Dict[str, Any]:
    """Probe configured discovery sources without downloads or database writes."""
    if mode not in {"chain", "backup"}:
        raise ValueError("mode must be chain or backup")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if page_size < 1 or page_size > 100:
        raise ValueError("page_size must be between 1 and 100")
    if max_pages < 1 or max_pages > 5:
        raise ValueError("max_pages must be between 1 and 5")
    if max_instruments < 1 or max_instruments > 20:
        raise ValueError("max_instruments must be between 1 and 20")
    if not instrument_ids:
        raise ValueError("at least one instrument is required")
    if len(instrument_ids) > max_instruments:
        raise ValueError(f"instrument count exceeds max_instruments={max_instruments}")

    instruments = [parse_instrument_id(value) for value in instrument_ids]
    active_coordinator = coordinator or _configured_coordinator()
    results = []
    for instrument in instruments:
        if mode == "chain":
            resolution = active_coordinator.discover_instrument(
                instrument,
                start_date=start_date,
                end_date=end_date,
                search_key=search_key,
                page_size=page_size,
                max_pages=max_pages,
                dry_run=True,
            )
            results.append(
                {
                    **instrument,
                    "status": resolution.status,
                    "selected_source": resolution.selected_source,
                    "selected_source_tier": resolution.selected_source_tier,
                    "fallback_used": resolution.fallback_used,
                    "fallback_reason": resolution.fallback_reason,
                    "candidate_count": len(resolution.candidates),
                    "candidate_titles": [item.title for item in resolution.candidates],
                    "attempts": [
                        {
                            "source": item.source,
                            "source_tier": item.source_tier,
                            "status": item.status,
                            "candidate_count": item.candidate_count,
                            "pages_scanned": item.pages_scanned,
                            "announcements_seen": item.announcements_seen,
                            "errors": item.errors,
                        }
                        for item in resolution.attempts
                    ],
                }
            )
            continue

        adapter = active_coordinator.backup_adapters.get(instrument["exchange"])
        if adapter is None:
            results.append(
                {
                    **instrument,
                    "status": "blocked",
                    "selected_source": None,
                    "selected_source_tier": None,
                    "fallback_used": False,
                    "fallback_reason": "exchange_backup_disabled_or_unconfigured",
                    "candidate_count": 0,
                    "candidate_titles": [],
                    "attempts": [],
                }
            )
            continue
        result = adapter.discover_instrument(
            instrument,
            start_date=start_date,
            end_date=end_date,
            search_key=search_key,
            page_size=page_size,
            max_pages=max_pages,
            dry_run=True,
        )
        results.append(
            {
                **instrument,
                "status": result.status,
                "selected_source": result.source,
                "selected_source_tier": result.source_tier,
                "fallback_used": True,
                "fallback_reason": "direct_backup_probe",
                "candidate_count": len(result.candidates),
                "candidate_titles": [item.title for item in result.candidates],
                "attempts": [
                    {
                        "source": result.source,
                        "source_tier": result.source_tier,
                        "status": result.status,
                        "candidate_count": len(result.candidates),
                        "pages_scanned": result.pages_scanned,
                        "announcements_seen": result.announcements_seen,
                        "errors": result.errors,
                    }
                ],
            }
        )

    status = (
        "success"
        if results
        and all(
            item["status"] == "success" and item["candidate_count"] > 0
            for item in results
        )
        else "degraded"
    )
    return {
        "schema_version": "business_profile_exchange_discovery_probe.v1",
        "status": status,
        "mode": mode,
        "bounds": {
            "start_date": start_date,
            "end_date": end_date,
            "page_size": page_size,
            "max_pages": max_pages,
            "max_instruments": max_instruments,
            "download_documents": False,
            "write_production_state": False,
        },
        "instrument_count": len(instruments),
        "success_count": sum(
            item["status"] == "success" and item["candidate_count"] > 0
            for item in results
        ),
        "results": results,
    }


def _configured_coordinator() -> BusinessProfileDiscoveryCoordinator:
    research_config = UnifiedConfigManager("config").get_research_config()
    return BusinessProfileDiscoveryCoordinator.from_research_config(research_config)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instrument", action="append", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--mode", choices=("chain", "backup"), default="chain")
    parser.add_argument("--search-key", default="年度报告")
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--max-instruments", type=int, default=10)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    payload = run_live_discovery_probe(
        args.instrument,
        start_date=args.start_date,
        end_date=args.end_date,
        mode=args.mode,
        search_key=args.search_key,
        page_size=args.page_size,
        max_pages=args.max_pages,
        max_instruments=args.max_instruments,
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    else:
        print(rendered)
    return 0 if payload["status"] == "success" else 3


if __name__ == "__main__":
    raise SystemExit(main())
