"""Run a read-only full-market audit of A-share XDXR reconciliation."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import date
import json
from pathlib import Path
from typing import Any, Dict

from scheduler.tasks import data_manager


def _difference_bucket(item: Dict[str, Any]) -> str:
    difference = float(item.get("factor_diff_pct") or 0.0)
    if difference < 10:
        return "5_to_10_pct"
    if difference < 20:
        return "10_to_20_pct"
    if difference < 50:
        return "20_to_50_pct"
    return "50_pct_or_more"


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    rows = await data_manager.db_ops.execute_read_query(
        """
        SELECT instrument_id
        FROM instruments
        WHERE exchange IN ('SSE', 'SZSE', 'BSE')
          AND type = 'stock'
        ORDER BY instrument_id
        """
    )
    instrument_ids = [row["instrument_id"] for row in rows]
    result = await data_manager.reconcile_tdx_xdxr_history(
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        instrument_ids=instrument_ids,
        sample_limit=args.sample_limit,
    )
    conflicts = result.get("factor_conflict_samples") or []
    shifted = result.get("shifted_match_samples") or []
    reference_only = result.get("reference_factor_change_only_samples") or []
    tdx_only = result.get("tdx_event_only_samples") or []
    distributions = result.get("distributions") or {}
    display_limit = max(0, args.display_samples)

    return {
        "instrument_count": len(instrument_ids),
        "status": result.get("status"),
        "totals": result.get("totals"),
        "reference_source_distribution": result.get(
            "reference_source_distribution"
        ),
        "factor_conflicts": {
            "same_date": sum(
                item.get("tdx_ex_date") == item.get("reference_ex_date")
                for item in conflicts
            ),
            "shifted_date": sum(
                item.get("tdx_ex_date") != item.get("reference_ex_date")
                for item in conflicts
            ),
            "by_action": distributions.get("factor_conflicts_by_action") or {},
            "by_difference": dict(Counter(
                _difference_bucket(item) for item in conflicts
            )),
            "by_source": dict(Counter(
                str(item.get("source") or "unknown") for item in conflicts
            )),
            "by_validation": dict(Counter(
                str(item.get("validation_result") or "unknown")
                for item in conflicts
            )),
            "by_decade": distributions.get("factor_conflicts_by_decade") or {},
            "samples": conflicts[:display_limit],
            "recent_samples": [
                item
                for item in conflicts
                if str(item.get("tdx_ex_date") or "") >= "2020-01-01"
            ][:display_limit],
        },
        "shifted_matches": {
            "reference_after_tdx": sum(
                int(item.get("calendar_day_distance") or 0) > 0 for item in shifted
            ),
            "reference_before_tdx": sum(
                int(item.get("calendar_day_distance") or 0) < 0 for item in shifted
            ),
            "same_calendar_date": sum(
                int(item.get("calendar_day_distance") or 0) == 0 for item in shifted
            ),
            "samples": shifted[:display_limit],
        },
        "reference_factor_change_only": {
            "by_source": distributions.get(
                "reference_factor_change_only_by_source"
            ) or {},
            "by_decade": distributions.get(
                "reference_factor_change_only_by_decade"
            ) or {},
            "samples": reference_only[:display_limit],
        },
        "tdx_event_only": {
            "by_action": distributions.get("tdx_event_only_by_action") or {},
            "by_decade": distributions.get("tdx_event_only_by_decade") or {},
            "samples": tdx_only[:display_limit],
        },
        "matching_policy": result.get("matching_policy"),
        "warnings": result.get("warnings"),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="1990-12-19")
    parser.add_argument("--end-date", default="2026-07-15")
    parser.add_argument("--sample-limit", type=int, default=1000)
    parser.add_argument("--display-samples", type=int, default=3)
    parser.add_argument("--output")
    args = parser.parse_args()
    payload = json.dumps(asyncio.run(_run(args)), ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)


if __name__ == "__main__":
    main()
