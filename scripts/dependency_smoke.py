#!/usr/bin/env python
"""行情依赖的生产调用冒烟：最小真实调用 + 结构断言。

覆盖 pytdx、baostock、yfinance、curl_cffi、akshare-proxy-patch。
akshare 本体仍由 scripts/akshare_smoke.py 负责。

输出：最后一行打印 JSON 汇总；exit 0 = 全部通过，exit 1 = 存在失败。
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

QUOTE_BAK = PROJECT_ROOT / "data" / "QuoteBak"


def _pytdx_daily_bars() -> Any:
    from data_sources.tdx_source import (
        TdxIPManager,
        close_tdx_socket,
        first_parseable_daily_bars,
    )
    from pytdx.hq import TdxHq_API

    manager = TdxIPManager()
    manager.refresh(force=True)
    ip, port = manager.get_ip()
    api = TdxHq_API(heartbeat=False, auto_retry=False)
    try:
        if not api.connect(ip, port, time_out=3):
            raise ConnectionError(f"pytdx connect failed for {ip}:{port}")
        bars = first_parseable_daily_bars(api)
        if not bars:
            raise AssertionError(f"pytdx daily probe returned no parseable bar from {ip}:{port}")
        return bars
    finally:
        close_tdx_socket(api)


def _baostock_history_k() -> Any:
    import baostock as bs

    end = date.today()
    start = end - timedelta(days=30)
    login = bs.login()
    try:
        if getattr(login, "error_code", None) != "0":
            raise RuntimeError(
                f"baostock login failed: {getattr(login, 'error_code', None)} "
                f"{getattr(login, 'error_msg', '')}"
            )
        result = bs.query_history_k_data_plus(
            "sh.600519",
            "date,code,open,high,low,close,volume",
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            frequency="d",
            adjustflag="3",
        )
        if getattr(result, "error_code", None) != "0":
            raise RuntimeError(
                f"baostock query failed: {getattr(result, 'error_code', None)} "
                f"{getattr(result, 'error_msg', '')}"
            )
        rows = []
        while result.next():
            rows.append(result.get_row_data())
        fields = list(getattr(result, "fields", []) or [])
        if "date" not in fields or "close" not in fields:
            raise AssertionError(f"baostock fields unexpected: {fields}")
        if not rows:
            raise AssertionError("baostock history returned no rows")
        return rows
    finally:
        try:
            bs.logout()
        except Exception:
            pass


def _yfinance_download_aapl() -> Any:
    import yfinance as yf

    cache_root = QUOTE_BAK / "dependency-smoke" / "yfinance"
    if QUOTE_BAK.is_dir():
        cache_root.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(cache_root))
    end = date.today()
    start = end - timedelta(days=14)
    frame = yf.download(
        "AAPL",
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        auto_adjust=False,
        progress=False,
        threads=False,
        multi_level_index=False,
    )
    if frame is None or getattr(frame, "empty", True):
        raise AssertionError("yfinance download returned no rows")
    columns = {str(column) for column in frame.columns}
    if "Close" not in columns:
        raise AssertionError(f"yfinance missing Close in {sorted(columns)[:12]}")
    return frame


def _curl_cffi_chrome_session() -> Any:
    from curl_cffi.requests.session import Session as ChromeTlsSession

    session = ChromeTlsSession(impersonate="chrome")
    try:
        response = session.request("GET", "https://www.cninfo.com.cn/", timeout=20)
    finally:
        close = getattr(session, "close", None)
        if callable(close):
            close()
    status = getattr(response, "status_code", None)
    if not isinstance(status, int):
        raise AssertionError(f"curl_cffi response has no status_code: {type(response)}")
    return response


def _install_akshare_patch() -> Any:
    from proxy_patch_bootstrap import install_akshare_proxy_patch

    state = install_akshare_proxy_patch(required=True)
    if not state.ready:
        raise AssertionError(f"akshare proxy patch not ready: {state.error}")
    return state


def _install_yfinance_patch() -> Any:
    from utils.proxy_patch_runtime import install_yfinance_proxy_patch

    state = install_yfinance_proxy_patch(required=True)
    if not state.ready:
        raise AssertionError(f"yfinance proxy patch not ready: {state.error}")
    return state


CASES: Dict[str, List[Tuple[str, Callable[[], Any]]]] = {
    "pytdx": [("daily_bars", _pytdx_daily_bars)],
    "baostock": [("history_k", _baostock_history_k)],
    "yfinance": [("download_aapl", _yfinance_download_aapl)],
    "curl_cffi": [("chrome_tls_session", _curl_cffi_chrome_session)],
    "akshare_proxy_patch": [
        ("install_akshare_patch", _install_akshare_patch),
        ("install_yfinance_patch", _install_yfinance_patch),
    ],
}


def run_cases(cases: List[Tuple[str, Callable[[], Any]]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for name, call in cases:
        case: Dict[str, Any] = {"name": name, "ok": False, "rows": 0, "error": None}
        try:
            payload = call()
            case["rows"] = len(payload) if payload is not None and hasattr(payload, "__len__") else 1
            case["ok"] = True
        except Exception as exc:
            case["error"] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc(file=sys.stderr)
        results.append(case)
    return results


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", choices=sorted(CASES), help="只跑一个依赖的冒烟")
    parser.add_argument("--list-cases", action="store_true", help="仅列出用例名")
    args = parser.parse_args(argv)
    selected = [args.package] if args.package else list(CASES)
    if args.list_cases:
        print(
            json.dumps(
                {name: [case for case, _ in CASES[name]] for name in selected},
                ensure_ascii=False,
            )
        )
        return 0
    results: List[Dict[str, Any]] = []
    for name in selected:
        for case in run_cases(CASES[name]):
            case["package"] = name
            results.append(case)
    failed = [case for case in results if not case["ok"]]
    summary = {
        "status": "success" if not failed else "failed",
        "passed": len(results) - len(failed),
        "failed": len(failed),
        "cases": results,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
