"""
Validate the production yfinance source path with proxy-patch runtime enabled.

This script exercises ``data_sources.yfinance_source.YFinanceSource`` rather
than calling ``yf.download`` directly, so it can catch regressions in the
project's session/proxy wiring.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_sources.base_source import RateLimitConfig
from data_sources.yfinance_source import YFinanceSource
from utils.proxy_patch_runtime import get_yfinance_proxy_patch_state


async def _run(args: argparse.Namespace) -> int:
    source = YFinanceSource(args.source_name, RateLimitConfig())

    try:
        import yfinance as yf

        yf.set_tz_cache_location("/tmp/py-yfinance")
    except Exception:
        pass

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")
    yf_symbol = source._build_yf_symbol(args.symbol, args.exchange)
    if not yf_symbol:
        print(
            json.dumps(
                {
                    "ok": False,
                    "symbol": args.symbol,
                    "exchange": args.exchange,
                    "error": "failed to build yfinance symbol",
                    "patch": get_yfinance_proxy_patch_state(),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    data = await source._fetch_yahoo_data_library(
        yf_symbol,
        start_date=start_date,
        end_date=end_date,
        timeout_sec=args.timeout,
    )
    result = {
        "ok": bool(data is not None and not data.empty),
        "source_name": args.source_name,
        "symbol": args.symbol,
        "exchange": args.exchange,
        "yf_symbol": yf_symbol,
        "start": args.start,
        "end": args.end,
        "rows": 0 if data is None else int(len(data)),
        "columns": [] if data is None else [str(column) for column in data.columns],
        "first_index": None if data is None or data.empty else str(data.index.min()),
        "last_index": None if data is None or data.empty else str(data.index.max()),
        "proxy_patch_ready": bool(source.proxy_patch_ready),
        "patch": get_yfinance_proxy_patch_state(),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["ok"] else 2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-name", default="yfinance_us_stock")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--exchange", default="NASDAQ")
    parser.add_argument("--start", default="2017-01-01")
    parser.add_argument("--end", default="2017-04-30")
    parser.add_argument("--timeout", type=int, default=15)
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
