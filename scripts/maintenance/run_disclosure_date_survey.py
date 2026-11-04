"""Production runner: historical disclosure-date survey (M2.4).

Scheduler/worker-side entry for the survey. Wires:
- the project announcement acquisition service (full access stack),
- announcement_audit as the evidence sink (event state untouched),
- a per-window checkpoint under data/QuoteBak.

Probe gate: a one-page acquire must succeed before any window scans;
on refusal the runner exits non-zero with scheduler-entry guidance
(2026-11-03 data20 403 incident contract, change M2/D3).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from research.financial_disclosure_date_survey import (  # noqa: E402
    SURVEY_PURPOSE_KEY,
    survey_historical_disclosure_dates,
)

DEFAULT_PERIODS = (
    "2023-12-31",
    "2024-03-31",
    "2024-06-30",
    "2024-09-30",
    "2024-12-31",
    "2025-03-31",
    "2025-06-30",
    "2025-09-30",
    "2025-12-31",
    "2026-03-31",
)
DEFAULT_EXCHANGES = ("SSE", "SZSE", "BSE")


class _RouteShim:
    """Attribute shim unifying AnnouncementRouteResult/ScanResult for the
    survey core (status/records/errors)."""

    def __init__(self, *, status: str, records: list, errors: list) -> None:
        self.status = status
        self.records = records
        self.errors = errors


def _build_instrument_map(exchanges) -> dict:
    quotes_db = REPO / "data" / "quotes.db"
    conn = sqlite3.connect(f"file:{quotes_db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT exchange, symbol, instrument_id FROM instruments
            WHERE type='stock' AND status='active'
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        (exchange, symbol): instrument_id
        for exchange, symbol, instrument_id in rows
        if exchange in exchanges
    }


async def run(args) -> dict:
    from data_manager import data_manager

    await data_manager.initialize(include_data_sources=False, load_progress=False)
    acquisition_service = data_manager._build_official_announcement_acquisition_service(
        request_interval_seconds=args.interval,
    )
    if acquisition_service is None:
        raise RuntimeError("announcement acquisition service is not configured")
    storage = data_manager.research_storage
    if storage is None:
        raise RuntimeError("research storage is not initialized")

    def _sync_acquire(query):
        """RouteResult is returned synchronously by the acquisition service."""
        route = acquisition_service.acquire(query)
        scan = getattr(route, "scan_result", None)
        if scan is None:
            errors = [
                str(getattr(attempt, "error", "") or "")
                for attempt in (getattr(route, "attempts", None) or ())
            ]
            return _RouteShim(
                status=getattr(route, "status", "failed"),
                records=[],
                errors=[e for e in errors if e] or [str(route)],
            )
        return _RouteShim(
            status=getattr(scan, "status", getattr(route, "status", "failed")),
            records=list(getattr(scan, "records", None) or []),
            errors=list(getattr(scan, "errors", None) or []),
        )

    async def acquire(query):
        return _sync_acquire(query)

    def audit_writer(purpose_key, record, instrument_id):
        storage.store_announcement_audit(
            purpose_key=purpose_key,
            record=record,
            instrument_id=instrument_id,
        )

    instrument_map = _build_instrument_map(args.exchanges)
    checkpoint_path = Path(args.checkpoint)
    # announcement_audit for the financial disclosure domain lives in
    # financials.db: route storage writes through financial_database_scope
    # (same convention as the disclosure incremental sync).
    with storage.financial_database_scope():
        report = await survey_historical_disclosure_dates(
            exchanges=args.exchanges,
            report_periods=args.periods,
            acquire=acquire,
            audit_writer=audit_writer,
            instrument_map=instrument_map,
            checkpoint_path=checkpoint_path,
            request_interval_seconds=args.interval,
            page_size=args.page_size,
            max_pages=args.max_pages,
            probe_enabled=not args.skip_probe,
            write_enabled=True,
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exchanges", nargs="*", default=list(DEFAULT_EXCHANGES))
    parser.add_argument("--periods", nargs="*", default=list(DEFAULT_PERIODS))
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--page-size", type=int, default=30)
    parser.add_argument("--max-pages", type=int, default=40)
    parser.add_argument(
        "--checkpoint",
        default=str(REPO / "data" / "QuoteBak" / "financial_survey" / "checkpoint.json"),
    )
    parser.add_argument(
        "--skip-probe",
        action="store_true",
        help="Skip the one-page access probe (not recommended).",
    )
    args = parser.parse_args()

    report = asyncio.run(run(args))
    print(json.dumps(report, ensure_ascii=False, indent=1, default=str))
    if report.get("aborted"):
        print(
            "ABORTED: announcement access refused; rerun this command in a "
            "later quiet window (completed windows are checkpointed).",
            file=sys.stderr,
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
