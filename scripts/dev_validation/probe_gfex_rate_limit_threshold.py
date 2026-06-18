#!/usr/bin/env python3
"""Probe GFEX official endpoint rate-limit behavior with bounded request counts."""

from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import requests


ROOT_DIR = Path(__file__).resolve().parents[2]
GFEX_DAY_URL = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"
GFEX_CALENDAR_URL = "http://www.gfex.com.cn/u/interfacesWebTpTradingCalendar/loadList"
GFEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Referer": "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml",
    "Origin": "http://www.gfex.com.cn",
    "X-Requested-With": "XMLHttpRequest",
}
SENSITIVE_KEYS = {"auth_token", "cookie", "nid18", "nid18_create_time", "proxy", "token"}


def _load_proxy_config() -> Dict[str, Any]:
    with (ROOT_DIR / "config" / "03_data.json").open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return dict(((data.get("data_sources_config") or {}).get("akshare") or {}).get("proxy_patch") or {})


def _gateway_host(gateway: str) -> str:
    parsed = urllib.parse.urlparse(gateway if "://" in gateway else f"http://{gateway}")
    return parsed.hostname or gateway


def _find_proxy_url(payload: Any) -> Optional[str]:
    if isinstance(payload, str):
        text = payload.strip()
        return text if text.startswith(("http://", "https://", "socks5://")) else None
    if isinstance(payload, Mapping):
        for key in ("proxy", "proxy_url", "http", "https"):
            candidate = _find_proxy_url(payload.get(key))
            if candidate:
                return candidate
        for value in payload.values():
            candidate = _find_proxy_url(value)
            if candidate:
                return candidate
    if isinstance(payload, list):
        for item in payload:
            candidate = _find_proxy_url(item)
            if candidate:
                return candidate
    return None


def _manual_proxy_url(timeout: float) -> Optional[str]:
    cfg = _load_proxy_config()
    gateway = str(cfg.get("gateway") or "").strip()
    token = str(cfg.get("auth_token") or "").strip()
    if not gateway or not token:
        return None
    params = urllib.parse.urlencode({"token": token, "version": "0.5.0"})
    url = f"http://{_gateway_host(gateway)}:47001/api/akshare-auth?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": GFEX_HEADERS["User-Agent"]})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    return _find_proxy_url(payload)


def _mask(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): ("***" if str(k).lower() in SENSITIVE_KEYS else _mask(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [_mask(item) for item in value]
    text = str(value)
    for scheme in ("http://", "https://", "socks5://"):
        start = text.find(scheme)
        while start >= 0:
            at_pos = text.find("@", start + len(scheme))
            if at_pos < 0:
                break
            end_candidates = [
                pos
                for pos in (
                    text.find(",", at_pos),
                    text.find(" ", at_pos),
                    text.find('"', at_pos),
                    text.find("'", at_pos),
                )
                if pos >= 0
            ]
            end = min(end_candidates) if end_candidates else len(text)
            text = text[: start + len(scheme)] + "***:***" + text[at_pos:end] + text[end:]
            start = text.find(scheme, start + len(scheme) + 7)
    return text


def _date_range(start: str, count: int) -> list[str]:
    current = date.fromisoformat(start)
    return [(current + timedelta(days=i)).strftime("%Y%m%d") for i in range(count)]


def _request_day(session: requests.Session, trade_date: str, timeout: float, proxies: Optional[Dict[str, str]]) -> Dict[str, Any]:
    started = time.monotonic()
    try:
        response = session.post(
            GFEX_DAY_URL,
            data={"trade_date": trade_date, "trade_type": "0", "variety": ""},
            headers=GFEX_HEADERS,
            timeout=timeout,
            proxies=proxies,
        )
        elapsed = time.monotonic() - started
        summary: Dict[str, Any] = {
            "trade_date": trade_date,
            "http_status": response.status_code,
            "elapsed_sec": round(elapsed, 3),
            "content_type": response.headers.get("content-type", ""),
        }
        text = response.text or ""
        if text.lstrip().startswith("{"):
            try:
                payload = response.json()
                summary.update(
                    {
                        "code": payload.get("code"),
                        "msg": payload.get("msg"),
                        "data_len": len(payload.get("data") or []),
                        "ok": response.status_code == 200 and payload.get("code") == "0",
                    }
                )
            except Exception as exc:
                summary.update({"ok": False, "error": f"json_error:{exc}"})
        else:
            summary.update({"ok": False, "body_prefix": text[:120].replace("\n", " ")})
        return summary
    except Exception as exc:
        return {"trade_date": trade_date, "ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_probe(args: argparse.Namespace) -> Dict[str, Any]:
    proxy_url = _manual_proxy_url(args.timeout) if args.mode == "manual_proxy" else None
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    session = requests.Session()
    session.headers.update(GFEX_HEADERS)
    results = []
    consecutive_failures = 0
    for idx, trade_date in enumerate(_date_range(args.start, args.count), start=1):
        item = _request_day(session, trade_date, args.timeout, proxies)
        item["seq"] = idx
        results.append(item)
        if item.get("ok"):
            consecutive_failures = 0
        else:
            consecutive_failures += 1
        if consecutive_failures >= args.stop_after_failures:
            break
        if idx < args.count and args.interval > 0:
            time.sleep(args.interval)
    ok_count = sum(1 for item in results if item.get("ok"))
    first_failure = next((item for item in results if not item.get("ok")), None)
    return {
        "mode": args.mode,
        "start": args.start,
        "count_requested": args.count,
        "count_executed": len(results),
        "interval_sec": args.interval,
        "timeout_sec": args.timeout,
        "ok_count": ok_count,
        "failure_count": len(results) - ok_count,
        "first_failure": _mask(first_failure) if first_failure else None,
        "proxy_used": bool(proxy_url),
        "results": _mask(results),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["direct", "manual_proxy"], default="direct")
    parser.add_argument("--start", default="2023-01-03")
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--stop-after-failures", type=int, default=5)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    print(json.dumps(run_probe(args), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
