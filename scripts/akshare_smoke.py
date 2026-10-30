#!/usr/bin/env python
"""akshare 生产接口冒烟检查：最小真实拉数 + 结构断言。

用途：
  1. akshare 升级前后的验证护栏（scripts/upgrade_akshare.py 调用）；
  2. 人工排查"某源突然失效"时快速定位。

覆盖生产在用的关键行情接口：LME 新浪主源、东财备源、100ppi 现货、
央行 SAFE 汇率、中美国债收益率。走项目统一的 proxy_patch 加载路径，
冒烟结果即代表服务运行环境的可用性。

输出：最后一行打印 JSON 汇总；exit 0 = 全部通过，exit 1 = 存在失败。
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _lme_sina_factory(symbol: str) -> Tuple[str, Callable, Dict[str, Any]]:
    def _call(akshare):
        return akshare.futures_foreign_hist(symbol=symbol)

    return (
        f"lme_sina_{symbol.lower()}",
        _call,
        {"min_rows": 1, "required_columns": ["date", "open", "high", "low", "close"]},
    )


def _lme_eastmoney_factory(symbol: str) -> Tuple[str, Callable, Dict[str, Any]]:
    def _call(akshare):
        return akshare.futures_global_hist_em(symbol=symbol)

    return (
        f"lme_em_{symbol.lower()}",
        _call,
        {"min_rows": 1, "required_columns": ["日期", "开盘", "最高", "最低", "最新价"]},
    )


def build_cases(today: Optional[date] = None) -> List[Tuple[str, Callable, Dict[str, Any]]]:
    """返回冒烟用例表：名称、akshare 调用、结构断言参数。"""
    today = today or date.today()
    window_start = (today - timedelta(days=14)).strftime("%Y%m%d")
    window_end = today.strftime("%Y%m%d")

    def _spot_100ppi_ta(akshare):
        return akshare.futures_spot_price_daily(
            vars_list=["TA"], start_day=window_start, end_day=window_end
        )

    def _fx_boc_safe(akshare):
        return akshare.currency_boc_safe()

    def _bond_zh_us_rate(akshare):
        return akshare.bond_zh_us_rate()

    return [
        _lme_sina_factory("CAD"),
        _lme_sina_factory("SND"),
        _lme_eastmoney_factory("LCPT"),
        (
            "spot_100ppi_ta",
            _spot_100ppi_ta,
            {"min_rows": 1, "required_columns": ["date"]},
        ),
        (
            "fx_boc_safe",
            _fx_boc_safe,
            {"min_rows": 1, "required_columns": ["日期"]},
        ),
        (
            "bond_zh_us_rate",
            _bond_zh_us_rate,
            {
                "min_rows": 1,
                "required_columns_any": [
                    ["日期"],
                    ["中国国债收益率10年", "中国国债收益率10年%"],
                ],
            },
        ),
    ]


def run_cases(cases) -> List[Dict[str, Any]]:
    """逐个执行用例并断言结构与行数，返回逐例结果。"""
    results: List[Dict[str, Any]] = []
    for name, call, assertion in cases:
        case: Dict[str, Any] = {"name": name, "ok": False, "rows": 0, "error": None}
        try:
            frame = call(_load_akshare())
            rows = len(frame)
            case["rows"] = rows
            columns = set(map(str, frame.columns))
            required_any_groups = assertion.get("required_columns_any") or []
            for group in required_any_groups:
                if not any(column in columns for column in group):
                    raise AssertionError(
                        f"none of {group} in {sorted(columns)[:12]}"
                    )
            required = assertion.get("required_columns") or []
            missing = [column for column in required if column not in columns]
            if missing:
                raise AssertionError(f"missing columns {missing} in {sorted(columns)[:12]}")
            min_rows = int(assertion.get("min_rows", 1))
            if rows < min_rows:
                raise AssertionError(f"rows {rows} < min_rows {min_rows}")
            case["ok"] = True
        except Exception as exc:
            case["error"] = f"{type(exc).__name__}: {exc}"
            traceback.print_exc(file=sys.stderr)
        results.append(case)
    return results


_AKSHARE_LOADED: Dict[str, Any] = {}


def _load_akshare():
    """通过项目统一入口加载 akshare（与调度服务一致的 proxy_patch 模式）。"""
    if "akshare" not in _AKSHARE_LOADED:
        from research.providers.akshare_support import load_akshare

        _AKSHARE_LOADED["akshare"] = load_akshare("proxy_patch")
    return _AKSHARE_LOADED["akshare"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list-cases", action="store_true", help="仅列出用例名")
    args = parser.parse_args()
    cases = build_cases()
    if args.list_cases:
        print(json.dumps({"cases": [name for name, _, _ in cases]}, ensure_ascii=False))
        return 0
    results = run_cases(cases)
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
