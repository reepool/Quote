#!/usr/bin/env python3
"""Smoke-test the browser-assisted DCE official daily-data route.

This script does not write databases or cache files. It verifies that the local
runtime can start a real headed Chrome session, pass DCE's site challenge, and
classify one trading day plus one closed day through the official dayQuotes API.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.providers.official_futures import OfficialFuturesMarketDataProvider
from utils.config_manager import ResearchConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate DCE official browser-assisted daily-data probe")
    parser.add_argument("--chrome", help="Path to a real Chrome binary. Also accepted via QUOTE_DCE_CHROME_PATH.")
    parser.add_argument("--trading-date", default="2026-06-12", help="Known DCE trading date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--closed-date", default="2026-06-13", help="Known DCE closed date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--settle-seconds", type=float, default=9)
    args = parser.parse_args()

    if args.chrome:
        os.environ["QUOTE_DCE_CHROME_PATH"] = str(Path(args.chrome).expanduser())

    config = ResearchConfig(
        enabled=True,
        modules={
            "commodity_market_data": {
                "enabled": True,
                "sources": {
                    "exchange_official": {
                        "enabled": True,
                        "enabled_exchanges": ["DCE"],
                        "dce_browser": {
                            "enabled": True,
                            "settle_seconds": args.settle_seconds,
                            "virtual_display": "auto",
                        },
                    }
                },
            }
        },
    )
    provider = OfficialFuturesMarketDataProvider(config)
    try:
        trading = provider.probe_exchange_trading_day("DCE", args.trading_date)
        closed = provider.probe_exchange_trading_day("DCE", args.closed_date)
    finally:
        provider.close()

    payload = {
        "trading": trading.__dict__,
        "closed": closed.__dict__,
        "ok": trading.status == "trading" and closed.status == "closed",
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
