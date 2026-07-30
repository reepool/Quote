#!/usr/bin/env python3
"""Audit and repair reviewed A-share security-code lineage quote history."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Install before importing AkShare or other HTTP-heavy project modules.
from proxy_patch_bootstrap import (  # noqa: E402
    get_akshare_proxy_patch_state,
    install_akshare_proxy_patch,
)

install_akshare_proxy_patch(required=False)

import akshare as ak  # noqa: E402

from data_sources.a_share_code_lineage import (  # noqa: E402
    CATALOG_PATH,
    LineageReconciliationError,
    build_lineage_audit,
    build_lineage_metadata_row,
    build_missing_only_quotes,
    load_lineage_catalog,
    normalize_quote_rows,
    reconcile_reviewed_history,
)
from data_sources.base_source import RateLimitConfig  # noqa: E402
from data_sources.tdx_source import TdxSource  # noqa: E402
from database import db_ops  # noqa: E402
from utils import config_manager  # noqa: E402


logger = logging.getLogger("a_share_code_lineage_backfill")


def _parse_existing_dates(rows: list[dict[str, Any]]) -> set[date]:
    result: set[date] = set()
    for row in rows:
        value = row.get("trade_date")
        if isinstance(value, datetime):
            result.add(value.date())
        elif isinstance(value, date):
            result.add(value)
        elif value:
            result.add(date.fromisoformat(str(value)[:10]))
    return result


async def _load_local_state(instrument_id: str) -> tuple[set[date], dict[str, Any]]:
    rows = await db_ops.execute_read_query(
        """
        SELECT date(time) AS trade_date
        FROM daily_quotes
        WHERE instrument_id = :instrument_id
        ORDER BY time
        """,
        {"instrument_id": instrument_id},
    )
    metadata_rows = await db_ops.execute_read_query(
        """
        SELECT metadata_json
        FROM instrument_master_metadata
        WHERE instrument_id = :instrument_id
        """,
        {"instrument_id": instrument_id},
    )
    metadata: dict[str, Any] = {}
    if metadata_rows:
        try:
            parsed = json.loads(metadata_rows[0].get("metadata_json") or "{}")
            if isinstance(parsed, dict):
                metadata = parsed
        except json.JSONDecodeError:
            logger.warning("Existing metadata_json is invalid; a fresh governed payload will be used")
    return _parse_existing_dates(rows), metadata


async def _load_first_current_quotes(entry) -> dict[date, dict[str, Any]]:
    quotes: dict[date, dict[str, Any]] = {}
    for transition in entry.transitions:
        rows = await db_ops.execute_read_query(
            """
            SELECT date(time) AS trade_date, open, high, low, close,
                   volume, amount, turnover, source
            FROM daily_quotes
            WHERE instrument_id = :instrument_id
              AND time >= :effective_date
            ORDER BY time
            LIMIT 1
            """,
            {
                "instrument_id": entry.instrument_id,
                "effective_date": transition.effective_date.isoformat(),
            },
        )
        if rows:
            quotes[transition.effective_date] = dict(rows[0])
    return quotes


def _tdx_source() -> TdxSource:
    raw = config_manager.get_nested("data_sources_config.pytdx", {}) or {}
    rate = RateLimitConfig(
        max_requests_per_minute=int(raw.get("max_requests_per_minute", 6000)),
        max_requests_per_hour=int(raw.get("max_requests_per_hour", 300000)),
        max_requests_per_day=int(raw.get("max_requests_per_day", 5000000)),
        retry_times=int(raw.get("retry_times", 3)),
        retry_interval=float(raw.get("retry_interval", 1.0)),
    )
    return TdxSource(
        "pytdx_lineage_repair",
        rate,
        pool_size=int(raw.get("connection_pool_size", 3)),
        connection_timeout=float(raw.get("connection_timeout_sec", 10)),
        ip_refresh_hours=float(raw.get("ip_refresh_interval_hours", 24)),
        batch_size=int(raw.get("batch_size", 800)),
    )


async def _fetch_live_rows(entry) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    start_dt = datetime.combine(entry.security_code_history_start, datetime.min.time())
    end_dt = datetime.combine(entry.repair_history_end, datetime.max.time())
    source = _tdx_source()
    try:
        logger.info("Fetching pytdx history for %s", entry.instrument_id)
        await source.initialize()
        tdx_rows = await source.get_daily_data(
            entry.instrument_id,
            entry.symbol,
            start_dt,
            end_dt,
        )
    finally:
        await source.close()

    logger.info("Fetching AkShare/Tencent history for %s", entry.instrument_id)
    tx_frame = await asyncio.to_thread(
        ak.stock_zh_a_hist_tx,
        symbol=entry.symbol,
        start_date=entry.security_code_history_start.strftime("%Y%m%d"),
        end_date=entry.repair_history_end.strftime("%Y%m%d"),
        adjust="",
        timeout=30,
    )
    tx_rows = tx_frame.to_dict(orient="records")
    return tdx_rows, tx_rows


async def run(args: argparse.Namespace) -> dict[str, Any]:
    catalog = load_lineage_catalog(args.catalog)
    instrument_id = args.instrument.strip().upper()
    if instrument_id not in catalog:
        raise ValueError(f"{instrument_id} is not in the reviewed lineage catalog")
    entry = catalog[instrument_id]

    await db_ops.initialize()
    existing_dates, existing_metadata = await _load_local_state(instrument_id)
    first_current_quotes = await _load_first_current_quotes(entry)
    tdx_raw, tx_raw = await _fetch_live_rows(entry)
    tdx_rows = normalize_quote_rows(tdx_raw, source="pytdx")
    tx_rows = normalize_quote_rows(tx_raw, source="akshare_tx")
    reconciliation = reconcile_reviewed_history(entry, tdx_rows, tx_rows)
    audit_before = build_lineage_audit(
        entry,
        existing_dates=existing_dates,
        reconciliation=reconciliation,
        first_current_quotes=first_current_quotes,
    )
    batch_id = f"lineage_{entry.symbol}_{entry.reviewed_at:%Y%m%d}"
    plan = build_missing_only_quotes(
        entry,
        reconciliation.selected_rows,
        existing_dates,
        batch_id=batch_id,
    )
    report: dict[str, Any] = {
        "status": "dry_run",
        "instrument_id": instrument_id,
        "catalog_version": entry.catalog_version,
        "proxy_patch": get_akshare_proxy_patch_state(),
        "source_counts": {
            "pytdx": len(tdx_rows),
            "akshare_tx": len(tx_rows),
            "reconciled": len(reconciliation.selected_rows),
        },
        "audit_before": audit_before,
        "reviewed_rows": {
            item.trade_date.isoformat(): item.values()
            for item in reconciliation.selected_rows
            if item.trade_date in entry.decisions_by_date
        },
        "planned_insert_count": len(plan),
        "planned_start": plan[0]["time"].date().isoformat() if plan else None,
        "planned_end": plan[-1]["time"].date().isoformat() if plan else None,
        "write_stats": None,
        "metadata_saved": 0,
    }
    if not args.apply:
        return report

    existing_lineage = (
        (existing_metadata.get("metadata") or {}).get("a_share_code_lineage")
        if isinstance(existing_metadata.get("metadata"), dict)
        else None
    )
    metadata_is_current = (
        isinstance(existing_lineage, dict)
        and existing_lineage.get("catalog_version") == entry.catalog_version
    )
    if not plan and metadata_is_current:
        report["status"] = "already_complete"
        report["audit_after"] = audit_before
        return report

    logger.info("Applying %d missing-only rows for %s", len(plan), instrument_id)
    write_stats = await db_ops.save_daily_quotes(
        plan,
        return_stats=True,
        insert_only=True,
    )
    report["write_stats"] = write_stats
    if not isinstance(write_stats, dict):
        raise RuntimeError("daily quote writer did not return write statistics")
    if int(write_stats.get("changed", 0)) != 0:
        raise RuntimeError("missing-only repair unexpectedly changed an existing row")
    if int(write_stats.get("inserted", 0)) != len(plan):
        raise RuntimeError(
            f"expected {len(plan)} inserts, got {write_stats.get('inserted')}"
        )

    persisted_dates, _ = await _load_local_state(instrument_id)
    planned_dates = {item["time"].date() for item in plan}
    absent_after_write = sorted(planned_dates - persisted_dates)
    if absent_after_write:
        raise RuntimeError(
            f"{len(absent_after_write)} planned dates are absent after write"
        )

    metadata_row = build_lineage_metadata_row(
        entry,
        reconciliation=reconciliation,
        inserted_rows=plan,
        existing_payload=existing_metadata,
    )
    metadata_saved = await db_ops.save_instrument_master_metadata_batch([metadata_row])
    if metadata_saved != 1:
        raise RuntimeError("quote rows were saved but lineage metadata was not persisted")

    report["metadata_saved"] = metadata_saved
    report["audit_after"] = build_lineage_audit(
        entry,
        existing_dates=persisted_dates,
        reconciliation=reconciliation,
        first_current_quotes=first_current_quotes,
    )
    report["status"] = "applied" if plan else "metadata_recovered"
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit and repair reviewed A-share code-lineage daily quotes",
    )
    parser.add_argument("--instrument", default="600018.SH")
    parser.add_argument("--catalog", type=Path, default=CATALOG_PATH)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the missing-only plan; default is dry-run",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = _build_parser().parse_args()
    try:
        result = asyncio.run(run(args))
    except LineageReconciliationError as exc:
        logger.error("%s", exc)
        print(json.dumps({"status": "blocked", "diagnostics": exc.diagnostics}, ensure_ascii=False, default=str))
        return 2
    except Exception as exc:
        logger.exception("Lineage repair failed: %s", exc)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
