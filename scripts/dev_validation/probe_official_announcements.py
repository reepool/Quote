#!/usr/bin/env python3
"""Bounded read-only live probe for official announcement providers."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcements import AnnouncementQuery, AnnouncementScope
from research.providers.registry import OfficialAnnouncementProviderRegistry
from utils.config_manager import config_manager


LOGGER = logging.getLogger(__name__)
ALLOWED_SOURCES = frozenset({"cninfo", "sse", "szse", "bse"})
EXCHANGE_SUFFIXES = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
MAX_TARGETS = 8
MAX_PAGES = 5
MAX_PAGE_SIZE = 30


def parse_target(value: str) -> dict[str, str]:
    """Parse `source:instrument_id` and reject implicit market inference."""
    source, separator, instrument_id = str(value or "").strip().partition(":")
    source = source.strip().lower()
    instrument_id = instrument_id.strip().upper()
    if not separator or source not in ALLOWED_SOURCES:
        raise ValueError(
            "target must be source:instrument_id with source in cninfo,sse,szse,bse"
        )
    symbol, dot, suffix = instrument_id.partition(".")
    exchange = EXCHANGE_SUFFIXES.get(suffix)
    if not dot or exchange is None or not symbol.isdigit() or len(symbol) != 6:
        raise ValueError("instrument_id must use 6-digit SYMBOL.SH/SZ/BJ format")
    if source in {"sse", "szse", "bse"} and source.upper() != exchange:
        raise ValueError(
            f"target source {source} does not match instrument exchange {exchange}"
        )
    return {
        "source": source,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "exchange": exchange,
    }


def run_probe(
    *,
    targets: Iterable[Mapping[str, str]],
    start_date: str,
    end_date: str,
    page_size: int,
    max_pages: int,
    request_timeout_seconds: float,
    request_interval_seconds: float,
    registry: Optional[OfficialAnnouncementProviderRegistry] = None,
) -> dict[str, Any]:
    """Probe configured providers without opening storage or retrieving attachments."""
    normalized_targets = [dict(item) for item in targets]
    if not normalized_targets or len(normalized_targets) > MAX_TARGETS:
        raise ValueError(f"target count must be between 1 and {MAX_TARGETS}")
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError("end_date must not be earlier than start_date")
    effective_page_size = min(MAX_PAGE_SIZE, max(1, int(page_size)))
    effective_max_pages = min(MAX_PAGES, max(1, int(max_pages)))
    effective_timeout = min(20.0, max(1.0, float(request_timeout_seconds)))
    effective_interval = max(0.1, float(request_interval_seconds))

    if registry is None:
        research_config = config_manager.get_research_config()
        overrides = {
            item["source"]: {
                "enabled": True,
                "request_timeout_seconds": effective_timeout,
                "request_interval_seconds": effective_interval,
                "retry_attempts": 0,
            }
            for item in normalized_targets
        }
        registry = OfficialAnnouncementProviderRegistry(
            research_config=research_config,
            provider_config_overrides=overrides,
        )

    results: list[dict[str, Any]] = []
    started = time.monotonic()
    for index, target in enumerate(normalized_targets, start=1):
        source = target["source"]
        target_started = time.monotonic()
        LOGGER.info(
            "official announcement live probe started: source=%s instrument_id=%s range=%s..%s pages=%s page_size=%s",
            source,
            target["instrument_id"],
            start.isoformat(),
            end.isoformat(),
            effective_max_pages,
            effective_page_size,
        )
        provider = registry.get(source)
        if provider is None:
            results.append(
                {
                    **target,
                    "status": "unavailable",
                    "error": "provider_not_registered",
                    "elapsed_seconds": round(time.monotonic() - target_started, 3),
                }
            )
            continue
        query = AnnouncementQuery(
            purpose_key="official_announcement_live_probe",
            source=source,
            scope=AnnouncementScope(
                exchange=target["exchange"],
                market=target["exchange"],
                instrument_id=target["instrument_id"],
                symbol=target["symbol"],
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                page_size=effective_page_size,
                max_pages=effective_max_pages,
            ),
        )
        try:
            provider.capabilities.validate(query)
            scan = provider.discover(query)
            first_record = scan.records[0] if scan.records else None
            capabilities = asdict(provider.capabilities)
            capabilities["exchanges"] = sorted(provider.capabilities.exchanges)
            result = {
                **target,
                "status": scan.status,
                "is_complete": scan.is_complete,
                "pages_scanned": scan.pages_scanned,
                "requests_made": scan.requests_made,
                "announcements_seen": scan.announcements_seen,
                "stop_reason": scan.stop_reason,
                "cursor_commit_allowed": scan.cursor_commit_allowed,
                "effective_bounds": {
                    "page_size": effective_page_size,
                    "max_pages": effective_max_pages,
                    "request_timeout_seconds": effective_timeout,
                    "request_interval_seconds": effective_interval,
                },
                "capabilities": capabilities,
                "response_shape": {
                    "first_record_raw_keys": sorted(
                        first_record.raw_payload.keys()
                    )
                    if first_record
                    else [],
                    "first_record_attachment_count": len(first_record.attachments)
                    if first_record
                    else 0,
                },
                "record_samples": [
                    {
                        "announcement_key": item.announcement_key,
                        "title": item.title,
                        "published_at": item.published_at,
                        "attachment_count": len(item.attachments),
                        "diagnostics": list(item.diagnostics),
                    }
                    for item in scan.records[:3]
                ],
                "diagnostics": dict(scan.diagnostics),
                "errors": list(scan.errors),
                "elapsed_seconds": round(time.monotonic() - target_started, 3),
            }
        except Exception as exc:
            result = {
                **target,
                "status": "failed",
                "error": f"{type(exc).__name__}:{exc}",
                "elapsed_seconds": round(time.monotonic() - target_started, 3),
            }
        results.append(result)
        LOGGER.info(
            "official announcement live probe completed: source=%s instrument_id=%s status=%s elapsed=%s",
            source,
            target["instrument_id"],
            result["status"],
            result["elapsed_seconds"],
        )
        if index < len(normalized_targets):
            time.sleep(effective_interval)

    failed_statuses = {"failed", "unavailable", "indeterminate", "degraded"}
    return {
        "status": (
            "degraded"
            if any(item.get("status") in failed_statuses for item in results)
            else "success"
        ),
        "read_only": True,
        "database_writes": False,
        "attachment_downloads": False,
        "target_count": len(normalized_targets),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        action="append",
        required=True,
        help="Explicit source and instrument, e.g. cninfo:600000.SH",
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.5)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument(
        "--allow-live-network",
        action="store_true",
        help="Required acknowledgement that the bounded probe contacts live sites.",
    )
    args = parser.parse_args()
    if not args.allow_live_network:
        parser.error("--allow-live-network is required")
    targets = [parse_target(value) for value in args.target]
    report = run_probe(
        targets=targets,
        start_date=args.start_date,
        end_date=args.end_date,
        page_size=args.page_size,
        max_pages=args.max_pages,
        request_timeout_seconds=args.request_timeout_seconds,
        request_interval_seconds=args.request_interval_seconds,
    )
    payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
