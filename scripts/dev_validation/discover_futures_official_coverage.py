#!/usr/bin/env python
"""Discover reliable official futures daily-data coverage by exchange/year samples."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.providers.official_futures import (  # noqa: E402
    OfficialFuturesMarketDataProvider,
    _official_daily_url,
    classify_official_futures_failure,
)
from scripts.research_cli_support import json_ready, parse_exchanges  # noqa: E402
from utils.config_manager import UnifiedConfigManager  # noqa: E402
from utils.date_utils import get_shanghai_time  # noqa: E402


DEFAULT_EXCHANGES = ["SHFE", "INE", "DCE", "CZCE", "GFEX"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe representative official futures daily endpoints to estimate reliable coverage starts."
    )
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=get_shanghai_time().year)
    parser.add_argument("--years", default=None, help="Comma-separated explicit years. Overrides start/end.")
    parser.add_argument("--exchanges", default=",".join(DEFAULT_EXCHANGES))
    parser.add_argument(
        "--sample-dates",
        default="01-04,06-15,12-15",
        help="Comma-separated MM-DD samples per year. Invalid calendar dates are skipped.",
    )
    parser.add_argument("--max-probes", type=int, default=None)
    parser.add_argument("--official-timeout-seconds", type=float, default=None)
    parser.add_argument("--official-retry-attempts", type=int, default=None)
    parser.add_argument("--disable-dce-browser", action="store_true")
    parser.add_argument("--output-path", type=Path, default=None)
    return parser


def _parse_years(args: argparse.Namespace) -> List[int]:
    if args.years:
        return sorted({int(item.strip()) for item in args.years.split(",") if item.strip()})
    if args.start_year > args.end_year:
        raise ValueError("start-year must be <= end-year")
    return list(range(args.start_year, args.end_year + 1))


def _sample_trade_dates(year: int, sample_dates: Sequence[str]) -> List[str]:
    values: List[str] = []
    for item in sample_dates:
        try:
            values.append(date.fromisoformat(f"{year}-{item.strip()}").isoformat())
        except ValueError:
            continue
    return values


def _summarize_exchange(probes: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    success_years = sorted({int(item["trade_date"][:4]) for item in probes if item.get("status") == "success"})
    empty_years = sorted({int(item["trade_date"][:4]) for item in probes if item.get("status") == "empty"})
    unresolved_years = sorted({int(item["trade_date"][:4]) for item in probes if item.get("status") == "unresolved"})
    category_counts: Dict[str, int] = {}
    suspected_ip_risk_control = False
    for item in probes:
        category = str(item.get("failure_category") or item.get("status") or "unknown")
        category_counts[category] = category_counts.get(category, 0) + 1
        suspected_ip_risk_control = suspected_ip_risk_control or bool(item.get("suspected_local_ip_risk_control"))
    return {
        "earliest_parseable_year": min(success_years) if success_years else None,
        "latest_parseable_year": max(success_years) if success_years else None,
        "success_years": success_years,
        "empty_years": empty_years,
        "unresolved_years": unresolved_years,
        "failure_category_counts": category_counts,
        "suspected_local_ip_risk_control": suspected_ip_risk_control,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    exchanges = parse_exchanges(args.exchanges) or DEFAULT_EXCHANGES
    years = _parse_years(args)
    sample_dates = [item.strip() for item in args.sample_dates.split(",") if item.strip()]

    config = UnifiedConfigManager().get_research_config()
    module_cfg = config.modules.setdefault("commodity_market_data", {})
    official_cfg = module_cfg.setdefault("sources", {}).setdefault("exchange_official", {})
    if args.official_timeout_seconds is not None:
        official_cfg["timeout_seconds"] = args.official_timeout_seconds
    if args.official_retry_attempts is not None:
        official_cfg["retry_attempts"] = args.official_retry_attempts
        dce_cfg = official_cfg.setdefault("dce_browser", {})
        if isinstance(dce_cfg, dict):
            dce_cfg["retry_attempts"] = args.official_retry_attempts
    if args.disable_dce_browser:
        official_cfg.setdefault("dce_browser", {})["enabled"] = False

    provider = OfficialFuturesMarketDataProvider(config)
    probes: List[Dict[str, Any]] = []
    probe_count = 0
    try:
        for exchange in exchanges:
            for year in years:
                for trade_date in _sample_trade_dates(year, sample_dates):
                    if args.max_probes is not None and probe_count >= args.max_probes:
                        break
                    probe_count += 1
                    endpoint_url = _official_daily_url(exchange, trade_date)
                    try:
                        rows = provider.fetch_exchange_contract_bars_sync(exchange, trade_date)
                        status = "success" if rows else "empty"
                        payload = {
                            "exchange": exchange,
                            "trade_date": trade_date,
                            "endpoint_url": endpoint_url,
                            "status": status,
                            "row_count": len(rows),
                            "failure_category": "",
                            "suspected_local_ip_risk_control": False,
                            "summary": "",
                        }
                    except Exception as exc:
                        classification = classify_official_futures_failure(exc)
                        payload = {
                            "exchange": exchange,
                            "trade_date": trade_date,
                            "endpoint_url": endpoint_url,
                            "status": "unresolved",
                            "row_count": 0,
                            "failure_category": classification.category,
                            "suspected_local_ip_risk_control": classification.suspected_local_ip_risk_control,
                            "summary": classification.summary,
                            "error": str(exc)[:1000],
                        }
                    probes.append(payload)
                    print(json.dumps(json_ready(payload), ensure_ascii=False), flush=True)
                if args.max_probes is not None and probe_count >= args.max_probes:
                    break
            if args.max_probes is not None and probe_count >= args.max_probes:
                break
    finally:
        provider.close()

    by_exchange: Dict[str, Dict[str, Any]] = {}
    for exchange in sorted({item["exchange"] for item in probes}):
        by_exchange[exchange] = _summarize_exchange([item for item in probes if item["exchange"] == exchange])
    result = {
        "status": "success",
        "generated_at": get_shanghai_time().isoformat(),
        "exchanges": exchanges,
        "years": years,
        "sample_dates": sample_dates,
        "probe_count": len(probes),
        "by_exchange": by_exchange,
        "probes": probes,
    }
    output = json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
