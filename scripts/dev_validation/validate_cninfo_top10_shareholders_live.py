#!/usr/bin/env python3
"""
Validate CNInfo data20 structured top-10 shareholder endpoints.

This is a live diagnostic script. It does not write project databases.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RESEARCH_DB = ROOT / "data" / "research.db"
DEFAULT_QUOTES_DB = ROOT / "data" / "quotes.db"

CNINFO_BASE = "https://www.cninfo.com.cn/data20/stockholderCapital"
DEFAULT_SYMBOLS = [
    "000002",
    "600000",
    "600050",
    "688981",
    "920833",
    "920489",
    "430489",
]


def _request_json(
    session: requests.Session,
    endpoint: str,
    symbol: str,
    timeout: float,
) -> Dict[str, Any]:
    url = f"{CNINFO_BASE}/{endpoint}"
    response = session.get(
        url,
        params={"scode": symbol},
        headers={
            "Accept": "application/json, text/plain, */*",
            "Referer": (
                "https://www.cninfo.com.cn/new/disclosure/stock?"
                f"stockCode={symbol}"
            ),
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        },
        timeout=timeout,
    )
    body_preview = response.text[:300]
    try:
        payload = response.json()
    except Exception as exc:
        return {
            "http_status": response.status_code,
            "ok": False,
            "error": f"json_decode_failed: {exc}",
            "body_preview": body_preview,
            "records": [],
        }

    data = payload.get("data") if isinstance(payload, dict) else None
    records = data.get("records") if isinstance(data, dict) else None
    return {
        "http_status": response.status_code,
        "ok": response.status_code == 200
        and isinstance(data, dict)
        and data.get("resultMsg") == "success",
        "code": payload.get("code") if isinstance(payload, dict) else None,
        "result_code": data.get("resultCode") if isinstance(data, dict) else None,
        "result_msg": data.get("resultMsg") if isinstance(data, dict) else None,
        "total": data.get("total") if isinstance(data, dict) else None,
        "count": data.get("count") if isinstance(data, dict) else None,
        "records": records if isinstance(records, list) else [],
        "body_preview": body_preview if response.status_code != 200 else None,
    }


def _latest_group(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    dates = sorted(
        {str(row.get("F001D")) for row in records if row.get("F001D")},
        reverse=True,
    )
    latest_date = dates[0] if dates else None
    latest_rows = [
        _normalize_top_holder_row(row)
        for row in records
        if latest_date is not None and str(row.get("F001D")) == latest_date
    ]
    latest_rows.sort(key=lambda row: row.get("rank") or 999)
    return {
        "latest_report_date": latest_date,
        "latest_rows": latest_rows,
        "latest_count": len(latest_rows),
        "available_report_dates": dates[:8],
    }


def _normalize_top_holder_row(row: Dict[str, Any]) -> Dict[str, Any]:
    shares_10k = _to_float(row.get("F003N"))
    return {
        "report_date": row.get("F001D"),
        "rank": _to_int(row.get("F005N")),
        "holder_name": row.get("F002V"),
        "holding_shares": shares_10k * 10000 if shares_10k is not None else None,
        "holding_shares_10k": shares_10k,
        "holding_ratio": _to_float(row.get("F004N")),
        "share_type": row.get("F006V"),
        "change": row.get("F007V"),
    }


def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _symbols_from_db_missing_top10(
    research_db: Path,
    quotes_db: Path,
    exchanges: Iterable[str],
    limit: int,
) -> List[str]:
    if limit <= 0:
        return []
    exchange_values = [str(exchange).strip().upper() for exchange in exchanges if exchange]
    if not exchange_values:
        return []
    placeholders = ",".join("?" for _ in exchange_values)
    query = f"""
        SELECT q.instruments.symbol
        FROM q.instruments
        LEFT JOIN shareholder_snapshots s
          ON s.instrument_id = q.instruments.instrument_id
        WHERE q.instruments.type = 'stock'
          AND q.instruments.is_active = 1
          AND q.instruments.exchange IN ({placeholders})
          AND (s.instrument_id IS NULL OR COALESCE(s.top_holders_count, 0) = 0)
        ORDER BY q.instruments.exchange, q.instruments.instrument_id
        LIMIT ?
    """
    with sqlite3.connect(str(research_db)) as conn:
        conn.execute("ATTACH ? AS q", (str(quotes_db),))
        rows = conn.execute(
            query,
            [*exchange_values, limit],
        ).fetchall()
    return [str(row[0]).strip() for row in rows if row and row[0]]


def validate_symbol(
    session: requests.Session,
    symbol: str,
    timeout: float,
    endpoint_interval: float,
    compact: bool = False,
) -> Dict[str, Any]:
    endpoints = {
        "top10_shareholders": "getTopTenStockholders",
        "top10_float_shareholders": "getTopTenCirculatingStockholders",
        "holder_count": "getStockholderNum",
    }
    result: Dict[str, Any] = {"symbol": symbol, "endpoints": {}}
    for index, (label, endpoint) in enumerate(endpoints.items(), start=1):
        if index > 1 and endpoint_interval > 0:
            time.sleep(endpoint_interval)
        payload = _request_json(session, endpoint, symbol, timeout)
        records = payload.pop("records", [])
        endpoint_result = {
            **payload,
            "record_count": len(records),
        }
        if label.startswith("top10"):
            endpoint_result.update(_latest_group(records))
            if compact:
                latest_rows = endpoint_result.get("latest_rows") or []
                endpoint_result["latest_rows"] = latest_rows[:1]
        else:
            dates = [
                str(row.get("ENDDATE"))
                for row in records
                if isinstance(row, dict) and row.get("ENDDATE")
            ]
            endpoint_result["latest_report_date"] = dates[0] if dates else None
            endpoint_result["latest_holder_count"] = (
                _to_int(records[0].get("F001N"))
                if records and isinstance(records[0], dict)
                else None
            )
            endpoint_result["sample_rows"] = records[:3]
            if compact:
                endpoint_result["sample_rows"] = records[:1]
        result["endpoints"][label] = endpoint_result
    top10 = result["endpoints"]["top10_shareholders"]
    result["cninfo_top10_ready"] = (
        bool(top10.get("ok")) and int(top10.get("latest_count") or 0) >= 10
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate CNInfo data20 structured top-10 shareholder endpoints."
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=DEFAULT_SYMBOLS,
        help="Stock symbols to validate. Defaults cover SSE/SZSE/BSE examples.",
    )
    parser.add_argument(
        "--sample-db-missing",
        type=int,
        default=0,
        help="Append N active A-share symbols currently missing top10_holders in research.db.",
    )
    parser.add_argument(
        "--exchanges",
        default="SSE,SZSE",
        help="Comma-separated exchanges used with --sample-db-missing.",
    )
    parser.add_argument("--research-db", default=str(DEFAULT_RESEARCH_DB))
    parser.add_argument("--quotes-db", default=str(DEFAULT_QUOTES_DB))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--interval", type=float, default=0.5)
    parser.add_argument("--endpoint-interval", type=float, default=0.2)
    parser.add_argument("--fail-on-missing", action="store_true")
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Only print compact per-endpoint samples instead of all latest rows.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = list(dict.fromkeys(str(symbol).strip() for symbol in args.symbols if symbol))
    if args.sample_db_missing:
        symbols.extend(
            _symbols_from_db_missing_top10(
                Path(args.research_db),
                Path(args.quotes_db),
                args.exchanges.split(","),
                args.sample_db_missing,
            )
        )
        symbols = list(dict.fromkeys(symbols))

    session = requests.Session()
    results = []
    for index, symbol in enumerate(symbols, start=1):
        if index > 1 and args.interval > 0:
            time.sleep(args.interval)
        results.append(
            validate_symbol(
                session,
                symbol,
                args.timeout,
                args.endpoint_interval,
                args.compact,
            )
        )

    ready_count = sum(1 for item in results if item["cninfo_top10_ready"])
    output = {
        "source": "cninfo_data20_stockholderCapital",
        "official_page_script": (
            "https://static.cninfo.com.cn/new/js/app/data/person-stock-news.js"
        ),
        "symbols_requested": len(symbols),
        "cninfo_top10_ready_count": ready_count,
        "cninfo_top10_ready_ratio": ready_count / len(results) if results else None,
        "results": results,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    if args.fail_on_missing and ready_count < len(results):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
