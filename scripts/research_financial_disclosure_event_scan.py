#!/usr/bin/env python
"""Scan CNInfo announcements for financial disclosure delay/suspension events."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.financial_disclosure_events import (  # noqa: E402
    build_financial_disclosure_events,
    build_financial_symbol_index,
    financial_disclosure_event_filter,
)
from research.providers.cninfo_announcements import (  # noqa: E402
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanner,
)
from scripts.dev_validation.prepare_sina_ths_local_core_import_manifest import (  # noqa: E402
    DEFAULT_EXCHANGES,
    collect_target_instruments,
)
from scripts.research_cli_support import (  # noqa: E402
    initialize_manager_for_research_cli,
    json_ready,
    parse_exchanges,
)


CNINFO_MARKET_CONFIG = {
    "SSE": {"market": "SSE", "column": "sse", "plate": "sh"},
    "SZSE": {"market": "SZSE", "column": "szse", "plate": "sz"},
    "BSE": {"market": "BSE", "column": "neeq", "plate": "bj"},
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan CNInfo announcements and output financial disclosure events."
    )
    parser.add_argument("--exchanges", default=",".join(DEFAULT_EXCHANGES))
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--search-key", default="")
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument("--request-timeout-seconds", type=float, default=20.0)
    return parser


async def async_main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    exchanges = parse_exchanges(args.exchanges) or list(DEFAULT_EXCHANGES)

    from data_manager import data_manager

    await initialize_manager_for_research_cli(data_manager)
    try:
        instruments_by_exchange = await collect_target_instruments(
            data_manager.db_ops,
            exchanges=exchanges,
        )
    finally:
        close = getattr(data_manager, "close", None)
        if close is not None:
            await close()

    symbol_index = build_financial_symbol_index(
        instrument
        for instruments in instruments_by_exchange.values()
        for instrument in instruments
    )
    scanner = CninfoAnnouncementScanner(
        request_timeout_seconds=args.request_timeout_seconds,
        request_interval_seconds=args.request_interval_seconds,
    )
    scan_results: List[Dict[str, Any]] = []
    all_selected = []
    for exchange in exchanges:
        config_values = CNINFO_MARKET_CONFIG.get(exchange)
        if not config_values:
            continue
        config = CninfoAnnouncementScanConfig(
            purpose_key="financial_disclosure_events",
            market=config_values["market"],
            column=config_values["column"],
            plate=config_values["plate"],
            start_date=args.start_date,
            end_date=args.end_date,
            page_size=args.page_size,
            max_pages=args.max_pages,
            search_key=args.search_key or None,
        )
        result = scanner.scan(config, filters=[financial_disclosure_event_filter])
        all_selected.extend(result.selected_records)
        scan_results.append(
            {
                "exchange": exchange,
                "pages_scanned": result.pages_scanned,
                "announcements_seen": result.announcements_seen,
                "selected_count": len(result.selected_records),
                "max_announcement_time": result.max_announcement_time,
                "errors": result.errors,
            }
        )

    events = build_financial_disclosure_events(all_selected, symbol_index)
    payload = {
        "status": "success",
        "purpose": "financial_disclosure_events",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "exchanges": exchanges,
        "scan_results": scan_results,
        "selected_announcements": [
            {
                "announcement_id": record.announcement_id,
                "announcement_time": record.announcement_time,
                "market": record.market,
                "column": record.column,
                "symbols": list(record.symbols),
                "title": record.title,
                "selection_reasons": list(record.selection_reasons),
                "mapped_event": any(
                    event.announcement_id == record.announcement_id
                    for event in events
                ),
            }
            for record in all_selected
        ],
        "event_count": len(events),
        "events": [event.to_manifest_item() for event in events],
    }
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(
        json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())
