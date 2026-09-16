#!/usr/bin/env python3
"""Write-free probe of production CNInfo access via attach_cninfo_access.

Exercises one data20 GET, one announcement POST, and one static URL. It does
not upsert shareholder snapshots or finish an ingestion run. HTTPS static
prefers headed Chrome through ``attach_cninfo_access``; Chrome TLS and proxy
remain backups. Headed Chrome unit-hop probes belong in tests, not this
production-factory path.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.providers.cninfo_http import (  # noqa: E402
    attach_cninfo_access,
    reset_cninfo_access_runtime,
)


DATA20_URL = "https://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders"
ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
STATIC_URL = "https://static.cninfo.com.cn/finalpage/2026-09-16/1225566315.PDF"
ANNOUNCEMENT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.cninfo.com.cn",
    "Referer": "https://www.cninfo.com.cn/new/disclosure/stock",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}


def _is_json(response: Any) -> bool:
    try:
        response.json()
    except (TypeError, ValueError):
        return False
    return True


def _report(name: str, response: Any) -> dict[str, Any]:
    content = bytes(getattr(response, "content", b"") or b"")
    payload = {
        "name": name,
        "status": getattr(response, "status_code", None),
        "url": getattr(response, "url", ""),
        "access_mode": getattr(response, "access_mode", None),
        "json": _is_json(response),
        "body_chars": len(str(getattr(response, "text", "") or "")),
        "body_bytes": len(content),
        "pdf": content.startswith(b"%PDF-"),
    }
    print(json.dumps(payload, ensure_ascii=False))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scode", default="000001", help="data20 stock code")
    args = parser.parse_args()

    reset_cninfo_access_runtime()
    session = attach_cninfo_access()
    reports = []
    try:
        reports.append(
            _report(
                "data20",
                session.get(DATA20_URL, params={"scode": args.scode}, timeout=30),
            )
        )
        reports.append(
            _report(
                "announcement",
                session.post(
                    ANNOUNCEMENT_URL,
                    data={
                        "pageNum": "1",
                        "pageSize": "1",
                        "column": "szse",
                        "tabName": "fulltext",
                        "plate": "sz",
                    },
                    headers=ANNOUNCEMENT_HEADERS,
                    timeout=30,
                ),
            )
        )
        reports.append(_report("static", session.get(STATIC_URL, timeout=20)))
    finally:
        reset_cninfo_access_runtime()

    by_name = {item["name"]: item for item in reports}
    www_ok = all(
        int(by_name[name].get("status") or 0) < 400 for name in ("data20", "announcement")
    )
    static = by_name.get("static", {})
    static_mode = static.get("access_mode")
    static_status = int(static.get("status") or 0)
    static_ok = static_mode in {"headed_chrome", "chrome_tls", "proxy_patch"} and (
        static.get("pdf") is True or static_status in {404, 410}
    )
    print(
        json.dumps(
            {
                "ok": www_ok and static_ok,
                "access_modes": [
                    item.get("access_mode") for item in reports if item.get("access_mode")
                ],
            },
            ensure_ascii=False,
        )
    )
    return 0 if www_ok and static_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
