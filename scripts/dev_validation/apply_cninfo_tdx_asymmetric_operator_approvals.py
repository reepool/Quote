#!/usr/bin/env python3
"""Apply the 15 operator-approved CNInfo/TDX asymmetric date decisions."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from data_manager import DataManager


REVIEWER = "operator_cninfo_tdx_asymmetric_20260727"
COMMON_NOTES = (
    "用户确认该事项属于股改或重整引发的非对称分派送转。"
    "CNInfo保留公司全股东口径经济数字，TDX保留流通股东口径数字；"
    "CNInfo因子仅采用CNInfo经济数字，生效日采用指定TDX除权交易日。"
)

DECISIONS: tuple[dict[str, Any], ...] = (
    {
        "instrument_id": "000623.SZ",
        "instrument_name": "吉林敖东",
        "source_event_key": (
            "84a7d330b9e64e9fcd7ae5359215f559"
            "d6fd79ac87bbcdeffb76c063219cfe37"
        ),
        "tdx_record_id": 31451,
        "expected_tdx_ex_date": "2005-08-04",
        "source_event_category": "股改分红",
        "notes": (
            COMMON_NOTES
            + "既有审核中的非流通股定向缩股说明继续通过审核链保留。"
        ),
    },
    {
        "instrument_id": "000897.SZ",
        "instrument_name": "津滨发展",
        "source_event_key": (
            "b1487f9417d5401bb2036faaff18d4b9"
            "3c497b39e878ba65dd10bdc4e82080bc"
        ),
        "tdx_record_id": 34700,
        "expected_tdx_ex_date": "2005-11-11",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000762.SZ",
        "instrument_name": "西藏矿业",
        "source_event_key": (
            "24b09d2acdeb4e157b5424fdaf22fddb"
            "4c34951738799db1c7b2c5daebb51102"
        ),
        "tdx_record_id": 33156,
        "expected_tdx_ex_date": "2006-02-10",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000912.SZ",
        "instrument_name": "泸天化",
        "source_event_key": (
            "f71c815ca1fa08e65f788e5cbc9fb688"
            "a9a259c0127a3b661d6f0c436059b967"
        ),
        "tdx_record_id": 34944,
        "expected_tdx_ex_date": "2006-02-13",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000915.SZ",
        "instrument_name": "华特达因",
        "source_event_key": (
            "a7f0668794ba957df7200a69cf453c5c"
            "cb78a846835a96dad266346d81a9db3c"
        ),
        "tdx_record_id": 34969,
        "expected_tdx_ex_date": "2006-07-20",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000952.SZ",
        "instrument_name": "广济药业",
        "source_event_key": (
            "605b83b9515a46190a9922eb0aa5d821"
            "c385dd3a55bb126977c5d6fd889c5902"
        ),
        "tdx_record_id": 35450,
        "expected_tdx_ex_date": "2006-07-24",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000893.SZ",
        "instrument_name": "亚钾国际",
        "source_event_key": (
            "0111d2a0df431f5784016cda4cb721596"
            "70b40a018bedccb2f1a6800b4819e0a"
        ),
        "tdx_record_id": 34654,
        "expected_tdx_ex_date": "2006-09-25",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "600825.SH",
        "instrument_name": "新华传媒",
        "source_event_key": (
            "40a1499e72a0b8c1624fcac935fb072af"
            "c50564ce4c8c7cbc4dc4bac87f71414"
        ),
        "tdx_record_id": 15410,
        "expected_tdx_ex_date": "2006-10-17",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000665.SZ",
        "instrument_name": "湖北广电",
        "source_event_key": (
            "d32ffd2c50e7c416cf3db266208ec14c9"
            "4a577cde2e7d25e98c16444b7987d7a"
        ),
        "tdx_record_id": 31916,
        "expected_tdx_ex_date": "2006-12-12",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "600645.SH",
        "instrument_name": "中源协和",
        "source_event_key": (
            "15325fbad023c3c3e8b2febac6f092cdd"
            "97da8cdffaf61cca049d477d7517704"
        ),
        "tdx_record_id": 12221,
        "expected_tdx_ex_date": "2007-01-25",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000609.SZ",
        "instrument_name": "*ST中迪",
        "source_event_key": (
            "2ffa715e41a4e596feaef2936807656b3"
            "d3f52aae033f1dc8d93f9c37ff51f6a"
        ),
        "tdx_record_id": 31290,
        "expected_tdx_ex_date": "2007-01-29",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "600234.SH",
        "instrument_name": "科新发展",
        "source_event_key": (
            "3d90a669096792d26134a50aa685614b1"
            "5fd8f2faea18c0a1ddc9ef64459e0bf"
        ),
        "tdx_record_id": 5886,
        "expected_tdx_ex_date": "2007-02-09",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000779.SZ",
        "instrument_name": "甘咨询",
        "source_event_key": (
            "5b105f596f50675b1c5f8116699df2ab"
            "3f95bba95f885f1571fe3edda458f070"
        ),
        "tdx_record_id": 33302,
        "expected_tdx_ex_date": "2007-02-12",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "600094.SH",
        "instrument_name": "大名城",
        "source_event_key": (
            "59aafe8dc83f660226b6692dd3321ee2f"
            "3d500b3f8eb24a6ee63eadfd828f0c7"
        ),
        "tdx_record_id": 3686,
        "expected_tdx_ex_date": "2007-03-19",
        "source_event_category": "股改分红",
    },
    {
        "instrument_id": "000615.SZ",
        "instrument_name": "*ST美谷",
        "source_event_key": (
            "2ba8b2e4b5103f39508378c434c2cb54"
            "56c0f69774015f88fef59da58d7b7bf0"
        ),
        "tdx_record_id": 31352,
        "expected_tdx_ex_date": "2025-12-29",
        "source_event_category": "重整转增",
    },
)


async def _run(
    decisions: tuple[dict[str, Any], ...],
    *,
    write: bool,
) -> list[dict[str, Any]]:
    manager = DataManager()
    results = []
    for decision in decisions:
        payload = {
            **decision,
            "reviewer": REVIEWER,
            "notes": decision.get("notes") or COMMON_NOTES,
            "dry_run": not write,
        }
        result = (
            await manager.review_cninfo_tdx_asymmetric_operator_approval(
                payload
            )
        )
        results.append({
            "instrument_name": decision["instrument_name"],
            **result,
        })
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist the 15 exact operator-approved review bundles.",
    )
    parser.add_argument(
        "--instrument-id",
        action="append",
        help="Limit preview/write to one instrument; may be repeated.",
    )
    args = parser.parse_args()
    requested = {
        str(value or "").strip().upper()
        for value in (args.instrument_id or [])
        if str(value or "").strip()
    }
    decisions = tuple(
        item for item in DECISIONS
        if not requested or item["instrument_id"] in requested
    )
    missing = sorted(
        requested - {item["instrument_id"] for item in decisions}
    )
    if missing:
        parser.error(
            "no operator decision for instrument(s): " + ", ".join(missing)
        )
    results = asyncio.run(_run(decisions, write=args.write))
    print(json.dumps(
        {
            "status": "success" if args.write else "dry_run",
            "count": len(results),
            "results": results,
        },
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
