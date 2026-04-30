"""
Validate yfinance availability through akshare_proxy_patch.

The patch must be installed before importing yfinance.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from utils.proxy_patch_runtime import (
    get_yfinance_proxy_patch_state,
    install_yfinance_proxy_patch,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--start", default="2017-01-01")
    parser.add_argument("--end", default="2017-04-30")
    args = parser.parse_args()

    install_yfinance_proxy_patch(required=True)

    import yfinance as yf

    try:
        yf.set_tz_cache_location("/tmp/py-yfinance")
    except Exception:
        pass

    data = yf.download(
        args.symbol,
        start=args.start,
        end=args.end,
        progress=False,
        auto_adjust=False,
        threads=False,
    )
    result = {
        "ok": bool(data is not None and not data.empty),
        "symbol": args.symbol,
        "start": args.start,
        "end": args.end,
        "rows": 0 if data is None else int(len(data)),
        "columns": [] if data is None else [str(column) for column in data.columns],
        "first_index": None if data is None or data.empty else str(data.index.min()),
        "last_index": None if data is None or data.empty else str(data.index.max()),
        "patch": get_yfinance_proxy_patch_state(),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
