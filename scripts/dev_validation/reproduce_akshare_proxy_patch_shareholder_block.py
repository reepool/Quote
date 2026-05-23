#!/usr/bin/env python3
"""
Reproduce shareholder endpoint blocking with and without akshare_proxy_patch.

The proxy-patch child process imports akshare_proxy_patch before importing
requests or akshare, matching the package's required bootstrap order.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_SYMBOLS = ["600000", "600050", "000001", "920833"]
CHILD_ENV = "AKSHARE_PROXY_REPRO_CHILD_MODE"


def _run_child_mode(mode: str, symbols: List[str], timeout: float) -> int:
    if mode == "proxy_patch":
        import akshare_proxy_patch  # noqa: F401

    from io import StringIO
    import re

    import akshare as ak
    import pandas as pd
    import requests

    session = requests.Session()
    results = []
    for symbol in symbols:
        sina_url = (
            "https://vip.stock.finance.sina.com.cn/corp/go.php/"
            f"vCI_StockHolder/stockid/{symbol}.phtml"
        )
        raw_result: Dict[str, Any] = {"url": sina_url}
        try:
            response = session.get(sina_url, timeout=timeout)
            text = response.text or ""
            title_match = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
            raw_result.update(
                {
                    "http_status": response.status_code,
                    "body_length": len(text),
                    "title": title_match.group(1).strip() if title_match else None,
                    "head": text[:120].replace("\n", " "),
                }
            )
            try:
                raw_result["pandas_table_count"] = len(pd.read_html(StringIO(text)))
                raw_result["pandas_error"] = None
            except Exception as exc:
                raw_result["pandas_table_count"] = 0
                raw_result["pandas_error"] = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            raw_result["request_error"] = f"{type(exc).__name__}: {exc}"

        akshare_result: Dict[str, Any] = {}
        try:
            top_holders = ak.stock_main_stock_holder(stock=symbol)
            akshare_result["stock_main_stock_holder_rows"] = len(top_holders)
            akshare_result["stock_main_stock_holder_error"] = None
        except Exception as exc:
            akshare_result["stock_main_stock_holder_rows"] = 0
            akshare_result["stock_main_stock_holder_error"] = (
                f"{type(exc).__name__}: {exc}"
            )

        try:
            holder_count = ak.stock_zh_a_gdhs_detail_em(symbol=symbol)
            akshare_result["stock_zh_a_gdhs_detail_em_rows"] = len(holder_count)
            akshare_result["stock_zh_a_gdhs_detail_em_error"] = None
        except Exception as exc:
            akshare_result["stock_zh_a_gdhs_detail_em_rows"] = 0
            akshare_result["stock_zh_a_gdhs_detail_em_error"] = (
                f"{type(exc).__name__}: {exc}"
            )

        results.append(
            {
                "symbol": symbol,
                "sina_raw": raw_result,
                "akshare": akshare_result,
            }
        )

    output = {
        "mode": mode,
        "python": sys.executable,
        "akshare_version": getattr(ak, "__version__", None),
        "results": results,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


def _spawn_mode(mode: str, symbols: List[str], timeout: float) -> Dict[str, Any]:
    env = os.environ.copy()
    env[CHILD_ENV] = mode
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--symbols",
        *symbols,
        "--timeout",
        str(timeout),
    ]
    completed = subprocess.run(
        cmd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return {
        "mode": mode,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reproduce Sina/Eastmoney shareholder access with direct requests "
            "and akshare_proxy_patch."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["both", "direct", "proxy_patch"],
        default="both",
        help="Run one mode or spawn both modes. Default: both.",
    )
    parser.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeout", type=float, default=20.0)
    return parser.parse_args()


def main() -> int:
    child_mode = os.environ.get(CHILD_ENV)
    args = parse_args()
    symbols = [str(symbol).strip() for symbol in args.symbols if str(symbol).strip()]
    if child_mode:
        return _run_child_mode(child_mode, symbols, args.timeout)

    modes = ["direct", "proxy_patch"] if args.mode == "both" else [args.mode]
    results = [_spawn_mode(mode, symbols, args.timeout) for mode in modes]
    print(json.dumps({"runs": results}, ensure_ascii=False, indent=2))
    return 0 if all(item["returncode"] == 0 for item in results) else 1


if __name__ == "__main__":
    sys.exit(main())
