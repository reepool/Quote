#!/usr/bin/env python3
"""Apply the operator-approved CNInfo asymmetric review decisions."""

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


DECISIONS: tuple[dict[str, Any], ...] = (
    {
        "instrument_id": "000519.SZ",
        "source_event_key": (
            "d57b066ddc7b70a9341bd330287b4b464"
            "2740cc6453f7fc6b2b46ae19c238f11"
        ),
        "analysis_id": 668,
        "announcement_id": "1202315394",
        "reviewer": "operator_cninfo_asymmetric_20260726",
        "effective_date": "2016-05-11",
        "date_basis": "赠与股份上市流通日",
        "factor_effect": "normal",
        "beneficiary_scope": "业绩承诺补偿股份受赠股东",
        "beneficiary_terms": {
            "bonus_shares_per_10_eligible_shares": 0.8189,
        },
        "total_share_capital_terms": {
            "bonus_shares_per_share": 0.08189,
        },
        "notes": "通过；业绩承诺引起的不对称股份赠送，沿用CNInfo原记录。",
    },
    {
        "instrument_id": "600449.SH",
        "source_event_key": (
            "768f211ea9c7d3cb371b8e63a966f376"
            "70a158c4737bf52a14c853b988eab39e"
        ),
        "analysis_id": 843,
        "announcement_id": "17947379",
        "reviewer": "operator_cninfo_asymmetric_20260726",
        "effective_date": "2006-08-15",
        "date_basis": "股份到账日",
        "factor_effect": "normal",
        "beneficiary_scope": "股权分置改革流通股股东",
        "beneficiary_terms": {
            "capitalization_shares_per_10_circulating_shares": 4.42,
        },
        "total_share_capital_terms": {
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.172488,
        },
        "notes": (
            "修改数字后通过；流通股东每10股获转增4.42股，"
            "折合总股本每10股转增1.72488股。"
        ),
    },
    {
        "instrument_id": "000031.SZ",
        "source_event_key": (
            "9e28f3a809e8ab45ef9f6cde9c58a3a"
            "e7ea49f46d14a404bf2c32fafa55f2c63"
        ),
        "analysis_id": 540,
        "announcement_id": "16425108",
        "reviewer": "operator_cninfo_asymmetric_20260726",
        "effective_date": "2006-02-14",
        "date_basis": "股份到账及复牌日",
        "factor_effect": "normal",
        "beneficiary_scope": "股权分置改革流通股股东",
        "beneficiary_terms": {
            "cash_dividend_per_10_circulating_shares": 2.7,
        },
        "total_share_capital_terms": {
            "cash_dividend_per_share": 0.1,
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.0,
        },
        "notes": (
            "修改数字后通过；总股本口径每10股派1元，不记录原错误的"
            "每10股送1.7股。流通股东实际每10股获2.7元。"
        ),
    },
    {
        "instrument_id": "000035.SZ",
        "source_event_key": (
            "a44ae5120ed104b4ee9db3ac7122095ba"
            "55c22e7f91bc997404644691e23adc2"
        ),
        "analysis_id": 539,
        "announcement_id": "61843804",
        "reviewer": "operator_cninfo_asymmetric_20260726",
        "effective_date": "2012-11-30",
        "date_basis": "股权登记日；公告明确无需除权",
        "factor_effect": "none",
        "beneficiary_scope": "重整债务清偿受让方，流通股东不获配",
        "beneficiary_terms": {
            "circulating_shareholder_factor_effect": 0,
        },
        "total_share_capital_terms": {
            "bonus_shares_per_share": 0.0,
            "capitalization_shares_per_share": 0.2596,
        },
        "notes": (
            "通过；事件表保留每10股转增2.596股，但新增股本全部用于"
            "偿债，公告明确不需除权，复权因子影响为0。"
        ),
    },
    {
        "instrument_id": "000623.SZ",
        "source_event_key": (
            "84a7d330b9e64e9fcd7ae5359215f559"
            "d6fd79ac87bbcdeffb76c063219cfe37"
        ),
        "announcement_id": "15718433",
        "reviewer": "operator_cninfo_asymmetric_20260727",
        "effective_date": "2005-08-04",
        "date_basis": "股权分置改革实施完成并恢复交易日",
        "factor_effect": "normal",
        "beneficiary_scope": (
            "全体股东法定分红；非流通股东现金转赠流通股东；"
            "非流通股定向缩股"
        ),
        "beneficiary_terms": {
            "circulating_cash_per_10_shares_approx": 4.0,
            "nontradable_shares_before_10k": 16255.20,
            "nontradable_shrink_ratio": 0.6074,
            "nontradable_shares_after_10k": 9873.4085,
            "total_shares_before_10k": 35049.69,
            "total_shares_after_10k": 28667.8985,
            "nontradable_shrink_price_factor_effect": "not_applied",
        },
        "total_share_capital_terms": {
            "cash_dividend_per_share": 0.214,
        },
        "notes": (
            "按CNInfo口径通过：全体股东每10股派2.14元并于2005-08-04"
            "完成股改、恢复交易。流通股东约每10股获4元属于股东间补偿"
            "口径，不写入CNInfo因子；非流通股按1:0.6074缩股仅记录为"
            "非对称资本结构变化，不构造送转或负向价格复权。"
        ),
    },
)


async def _apply(
    decisions: tuple[dict[str, Any], ...],
) -> list[dict[str, Any]]:
    manager = DataManager()
    results = []
    for payload in decisions:
        result = await manager.review_cninfo_asymmetric_manual_override(
            dict(payload)
        )
        results.append({
            "instrument_id": payload["instrument_id"],
            "source_event_key": payload["source_event_key"],
            **result,
        })
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--write",
        action="store_true",
        help="Persist the operator-approved review bundles.",
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
    if not args.write:
        print(json.dumps(
            {"status": "preview", "decisions": decisions},
            ensure_ascii=False,
            indent=2,
        ))
        return 0
    print(json.dumps(
        {"status": "success", "results": asyncio.run(_apply(decisions))},
        ensure_ascii=False,
        indent=2,
        default=str,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
