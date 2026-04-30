#!/usr/bin/env python3
"""
Live validation for SWS Research index-analysis latest daily rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


async def _run() -> int:
    parser = argparse.ArgumentParser(
        description="Validate SWS Research index-analysis latest daily endpoint."
    )
    parser.add_argument(
        "--index-types",
        default="市场表征,一级行业,二级行业,三级行业,风格指数",
        help="Comma-separated index dimensions to probe.",
    )
    parser.add_argument("--limit-per-type", type=int, default=3)
    args = parser.parse_args()

    config_path = ROOT_DIR / "config" / "10_research.json"
    research_config = json.loads(config_path.read_text(encoding="utf-8"))["research_config"]
    standard_cfg = research_config.get("modules", {}).get("industry", {}).get("standard", {})
    index_cfg = research_config.get("sources", {}).get("swsresearch", {}).get(
        "index_analysis",
        {},
    )
    endpoint = index_cfg.get(
        "endpoint",
        (
            "https://www.swsresearch.com/institute-sw/api/index_analysis/"
            "day_week_month_report/"
        ),
    )
    timeout = float(index_cfg.get("request_timeout_seconds", 20.0))
    index_types = [
        item.strip()
        for item in str(args.index_types).split(",")
        if item.strip()
    ]
    snapshots = _fetch_latest_rows(
        endpoint=endpoint,
        index_types=index_types,
        limit_per_type=args.limit_per_type,
        timeout=timeout,
    )
    counts: dict[str, int] = {}
    latest_dates: dict[str, str] = {}
    for snapshot in snapshots:
        index_type = snapshot.get("index_type") or "unknown"
        counts[index_type] = counts.get(index_type, 0) + 1
        latest_dates[index_type] = max(
            latest_dates.get(index_type, ""),
            snapshot.get("trade_date") or "",
        )

    print(
        json.dumps(
            {
                "status": "success" if snapshots else "empty",
                "rows": len(snapshots),
                "counts": counts,
                "latest_dates": latest_dates,
                "sample": [
                    {
                        "sw_index_code": item["sw_index_code"],
                        "sw_index_name": item["sw_index_name"],
                        "index_type": item["index_type"],
                        "trade_date": item["trade_date"],
                        "close_index": item["close_index"],
                        "pe": item["pe"],
                        "pb": item["pb"],
                        "dividend_yield": item["dividend_yield"],
                    }
                    for item in snapshots[:10]
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if snapshots else 2


def _fetch_latest_rows(
    *,
    endpoint: str,
    index_types: list[str],
    limit_per_type: int,
    timeout: float,
) -> list[dict[str, Any]]:
    session = requests.Session()
    rows: list[dict[str, Any]] = []
    headers = {
        "Accept": "application/json",
        "Referer": "https://www.swsresearch.com/institute_sw/home",
        "User-Agent": "Mozilla/5.0",
    }
    for index_type in index_types:
        response = session.get(
            endpoint,
            params={
                "type": "DAY",
                "index_type": index_type,
                "page": 1,
                "page_size": max(1, int(limit_per_type)),
            },
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if str(payload.get("code")) != "200":
            raise ValueError(f"SWS index-analysis returned non-ok payload: {payload}")
        for raw in (payload.get("data") or {}).get("results") or []:
            rows.append(
                {
                    "sw_index_code": str(raw.get("swindexcode") or "").strip(),
                    "sw_index_name": str(raw.get("swindexname") or "").strip(),
                    "index_type": index_type,
                    "trade_date": _to_trade_date(raw.get("bargaindate")),
                    "close_index": _to_float(raw.get("closeindex")),
                    "pe": _to_float(raw.get("pe")),
                    "pb": _to_float(raw.get("pb")),
                    "dividend_yield": _to_float(raw.get("dp")),
                }
            )
    return rows


def _to_float(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--"}:
        return None
    return float(text)


def _to_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
