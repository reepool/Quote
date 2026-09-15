#!/usr/bin/env python3
"""Write-free probe of CNInfo headed-Chrome access.

Constructs the headed-Chrome access provider directly and exercises homepage
bootstrap, one data20 endpoint, and one announcement query. It does not upsert
shareholder snapshots or finish an ingestion run.
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

from research.providers.cninfo_headed_chrome import (  # noqa: E402
    create_cninfo_headed_chrome_access,
)


HOMEPAGE = "https://www.cninfo.com.cn/"
DATA20_URL = "https://www.cninfo.com.cn/data20/stockholderCapital/getTopTenStockholders"
ANNOUNCEMENT_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
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
    payload = {
        "name": name,
        "status": getattr(response, "status_code", None),
        "url": getattr(response, "url", ""),
        "access_mode": getattr(response, "access_mode", None),
        "json": _is_json(response),
        "body_chars": len(str(getattr(response, "text", "") or "")),
    }
    print(json.dumps(payload, ensure_ascii=False))
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scode", default="000001", help="data20 stock code")
    args = parser.parse_args()

    access = create_cninfo_headed_chrome_access()
    reports = []
    try:
        reports.append(_report("homepage", access.get(HOMEPAGE, timeout=30)))
        reports.append(
            _report(
                "data20",
                access.get(DATA20_URL, params={"scode": args.scode}, timeout=30),
            )
        )
        reports.append(
            _report(
                "announcement",
                access.post(
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
    finally:
        access.close()

    modes = {item.get("access_mode") for item in reports}
    print(
        json.dumps(
            {
                "ok": all(int(item.get("status") or 0) < 400 for item in reports),
                "access_modes": sorted(mode for mode in modes if mode),
            },
            ensure_ascii=False,
        )
    )
    return 0 if all(int(item.get("status") or 0) < 400 for item in reports) else 1


if __name__ == "__main__":
    raise SystemExit(main())
