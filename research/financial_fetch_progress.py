"""Shared n/m progress logs for financial statement fetches."""

from __future__ import annotations

from typing import Any, Optional

DEFAULT_FINANCIAL_FETCH_PROGRESS_INTERVAL = 200


def should_emit_financial_fetch_progress(
    processed: int,
    total: int,
    *,
    interval: int = DEFAULT_FINANCIAL_FETCH_PROGRESS_INTERVAL,
) -> bool:
    """Emit the first item, every N items, and the last item."""
    if processed <= 0 or total <= 0:
        return False
    if processed == 1 or processed == total:
        return True
    return processed % max(1, int(interval)) == 0


def format_financial_fetch_progress(
    *,
    channel: str,
    processed: int,
    total: int,
    elapsed_seconds: Optional[float] = None,
    **details: Any,
) -> str:
    parts = [f"[FinancialFetch] {channel} progress {processed}/{total}"]
    for key, value in details.items():
        if value is None or value == "":
            continue
        parts.append(f"{key}={value}")
    if elapsed_seconds is not None:
        parts.append(f"elapsed={elapsed_seconds:.1f}s")
    return " ".join(parts)


def log_financial_fetch_progress(
    logger: Any,
    *,
    channel: str,
    processed: int,
    total: int,
    elapsed_seconds: Optional[float] = None,
    **details: Any,
) -> None:
    """Log fetch progress when the current index crosses the reporting cadence."""
    if not should_emit_financial_fetch_progress(processed, total):
        return
    logger.info(
        format_financial_fetch_progress(
            channel=channel,
            processed=processed,
            total=total,
            elapsed_seconds=elapsed_seconds,
            **details,
        )
    )
