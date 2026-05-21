#!/usr/bin/env python
"""Probe SSE commonQuery structured financial period availability.

The command is read-only and bounded. It inspects configured SSE
`commonQuery.do` endpoint candidates and records row/numeric-field counts per
candidate so a missing report period can be separated from endpoint failure.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.providers.official_financial_filings import (  # noqa: E402
    classify_official_filing_response,
)
from scripts.research_cli_support import json_ready  # noqa: E402
from scripts.research_financial_statements_rollout_validation import (  # noqa: E402
    normalize_report_periods,
)
from utils.config_manager import config_manager  # noqa: E402


DEFAULT_USER_AGENT = "QuoteResearch/sse-financial-period-probe"


class _SafeFormatDict(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def parse_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).split(",") if part.strip()]


def build_context(instrument_id: str, report_period: str) -> Dict[str, str]:
    symbol = str(instrument_id).split(".", 1)[0]
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "stockid": symbol,
        "exchange": "SSE",
        "report_period": report_period,
        "report_year": report_period[:4],
        "report_type_id": sse_report_type_id(report_period),
    }


def sse_report_type_id(report_period: str) -> str:
    value = str(report_period or "").upper()
    if value.endswith("Q1") or value.endswith("-03-31"):
        return "4000"
    if value.endswith("Q2") or value.endswith("-06-30"):
        return "1000"
    if value.endswith("Q3") or value.endswith("-09-30"):
        return "4400"
    if value.endswith("Q4") or value.endswith("FY") or value.endswith("-12-31"):
        return "5000"
    return "5000"


def load_sse_endpoint_candidates(*, include_disabled: bool = True) -> List[Dict[str, Any]]:
    research_config = config_manager.get_research_config()
    source_cfg = research_config.sources.get("sse", {})
    financial_cfg = source_cfg.get("financial_statements", {})
    candidates = [
        candidate
        for candidate in financial_cfg.get("endpoint_candidates", [])
        if isinstance(candidate, dict)
    ]
    if include_disabled:
        return candidates
    return [candidate for candidate in candidates if bool(candidate.get("enabled"))]


def probe_sse_period_availability(
    *,
    instrument_id: str,
    report_periods: List[str],
    timeout_seconds: float,
    request_interval_seconds: float,
    include_disabled_candidates: bool = True,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    normalized_periods = normalize_report_periods(report_periods)
    candidates = load_sse_endpoint_candidates(
        include_disabled=include_disabled_candidates,
    )
    http_session = session or requests.Session()
    started_at = time.perf_counter()
    results: List[Dict[str, Any]] = []
    max_year_results = [
        probe_max_year(
            http_session,
            instrument_id=instrument_id,
            report_type_id=report_type_id,
            timeout_seconds=timeout_seconds,
        )
        for report_type_id in sorted(
            {sse_report_type_id(period) for period in normalized_periods}
        )
    ]
    for report_period in normalized_periods:
        context = build_context(instrument_id, report_period)
        for candidate in candidates:
            if not candidate.get("url"):
                continue
            result = probe_candidate(
                http_session,
                candidate=candidate,
                context=context,
                timeout_seconds=timeout_seconds,
            )
            result["report_period"] = report_period
            result["instrument_id"] = instrument_id
            results.append(result)
            if request_interval_seconds > 0:
                time.sleep(request_interval_seconds)
    summary = summarize_period_probe_results(
        results,
        report_periods=normalized_periods,
        max_year_results=max_year_results,
    )
    return {
        "status": "passed" if summary["periods_with_numeric_rows"] else "needs_review",
        "source": "sse",
        "source_profile": "sse_commonquery",
        "instrument_id": instrument_id,
        "report_periods": normalized_periods,
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        "candidate_count": len(candidates),
        "request_policy": {
            "timeout_seconds": timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
            "include_disabled_candidates": include_disabled_candidates,
        },
        "summary": summary,
        "max_year_results": max_year_results,
        "candidate_results": results,
    }


def probe_max_year(
    session: requests.Session,
    *,
    instrument_id: str,
    report_type_id: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    symbol = str(instrument_id).split(".", 1)[0]
    params = {
        "isPagination": "false",
        "sqlId": "COMMON_MAP_MAXYEAR_L",
        "STOCK_ID": symbol,
        "REPORT_PERIOD_ID": report_type_id,
    }
    started_at = time.perf_counter()
    url = "https://query.sse.com.cn/commonQuery.do"
    try:
        response = session.get(
            url,
            headers={
                "User-Agent": DEFAULT_USER_AGENT,
                "Referer": (
                    "https://english.sse.com.cn/markets/dataservice/xbrl/"
                    f"companyinfo/?stock_id={symbol}&report_period_id={report_type_id}"
                ),
            },
            params=params,
            timeout=timeout_seconds,
        )
        payload = parse_json_or_jsonp(response.text)
        max_year = extract_max_report_year(payload)
        return {
            "report_type_id": report_type_id,
            "request_params": params,
            "http_status": int(getattr(response, "status_code", 0) or 0),
            "max_report_year": max_year,
            "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
            "error": None,
        }
    except Exception as exc:  # pragma: no cover - transport-specific
        return {
            "report_type_id": report_type_id,
            "request_params": params,
            "http_status": None,
            "max_report_year": None,
            "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
            "error": str(exc),
        }


def probe_candidate(
    session: requests.Session,
    *,
    candidate: Dict[str, Any],
    context: Dict[str, str],
    timeout_seconds: float,
) -> Dict[str, Any]:
    request_config = candidate.get("request") or candidate.get("request_config") or {}
    method = str(request_config.get("method") or "GET").upper()
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        **format_mapping(request_config.get("headers") or {}, context),
    }
    params = format_mapping(
        request_config.get("query_params") or request_config.get("params") or {},
        context,
    )
    body = format_mapping(
        request_config.get("body_params") or request_config.get("body") or {},
        context,
    )
    url = str(candidate.get("url") or "").format_map(_SafeFormatDict(context))
    started_at = time.perf_counter()
    try:
        response = (
            session.get(url, headers=headers, params=params, timeout=timeout_seconds)
            if method == "GET"
            else session.request(
                method,
                url,
                headers=headers,
                params=params if method == "GET" else None,
                data=body if method != "GET" else None,
                timeout=timeout_seconds,
            )
        )
        content = bytes(response.content or b"")
        content_type = response.headers.get("Content-Type")
        classification = classify_official_filing_response(
            content,
            content_type=content_type,
            http_status=getattr(response, "status_code", None),
            url=getattr(response, "url", url),
        )
        parsed = parse_json_or_jsonp(response.text)
        row_count, numeric_field_count, source_field_count = count_sse_rows(parsed)
        return {
            "candidate_key": candidate.get("key"),
            "sql_id": params.get("sqlId") or (candidate.get("evidence") or {}).get("sql_id"),
            "url": getattr(response, "url", url),
            "request_method": method,
            "request_params": params,
            "http_status": int(getattr(response, "status_code", 0) or 0),
            "content_type": content_type,
            "sample_bytes": len(content),
            "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
            "row_count": row_count,
            "source_field_count": source_field_count,
            "numeric_field_count": numeric_field_count,
            "period_availability": classify_period_availability(
                http_status=int(getattr(response, "status_code", 0) or 0),
                response_class=classification.response_class,
                row_count=row_count,
                numeric_field_count=numeric_field_count,
            ),
            **classification.as_probe_fields(),
            "error": None,
        }
    except Exception as exc:  # pragma: no cover - transport-specific
        return {
            "candidate_key": candidate.get("key"),
            "sql_id": params.get("sqlId") or (candidate.get("evidence") or {}).get("sql_id"),
            "url": url,
            "request_method": method,
            "request_params": params,
            "http_status": None,
            "content_type": None,
            "sample_bytes": 0,
            "elapsed_ms": int((time.perf_counter() - started_at) * 1000),
            "row_count": 0,
            "source_field_count": 0,
            "numeric_field_count": 0,
            "period_availability": "request_error",
            "response_class": "request_error",
            "artifact_kind": None,
            "parser_candidate": None,
            "structured_payload": False,
            "readiness_blocker": "request_error",
            "classification_diagnostics": {},
            "error": str(exc),
        }


def count_sse_rows(payload: Any) -> tuple[int, int, int]:
    if not isinstance(payload, dict):
        return 0, 0, 0
    rows = payload.get("result")
    if not isinstance(rows, list):
        return 0, 0, 0
    source_fields: set[str] = set()
    numeric_field_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if not re.match(r"^S\d{4}_\d{4}$", str(key)):
                continue
            source_fields.add(str(key))
            if is_numeric_value(value):
                numeric_field_count += 1
    return len(rows), numeric_field_count, len(source_fields)


def is_numeric_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return True
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "--"}:
        return False
    try:
        float(text)
    except ValueError:
        return False
    return True


def classify_period_availability(
    *,
    http_status: int,
    response_class: str,
    row_count: int,
    numeric_field_count: int,
) -> str:
    if not 200 <= int(http_status or 0) < 400:
        return "endpoint_unavailable"
    if row_count > 0 and numeric_field_count > 0:
        return "structured_numeric_rows"
    if row_count > 0:
        return "structured_rows_without_numeric_fields"
    if response_class in {"json_manifest", "empty"}:
        return "empty_structured_payload"
    return response_class or "unsupported_response"


def summarize_period_probe_results(
    results: List[Dict[str, Any]],
    *,
    report_periods: List[str],
    max_year_results: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    periods_with_numeric = sorted(
        {
            str(item.get("report_period"))
            for item in results
            if int(item.get("numeric_field_count") or 0) > 0
        }
    )
    periods_without_numeric = [
        period for period in report_periods if period not in set(periods_with_numeric)
    ]
    reachable_periods = sorted(
        {
            str(item.get("report_period"))
            for item in results
            if int(item.get("http_status") or 0) in range(200, 400)
        }
    )
    if periods_without_numeric and periods_with_numeric:
        assessment = "period_unavailable_or_query_adapter_gap"
    elif periods_without_numeric and reachable_periods:
        assessment = "endpoint_reachable_but_no_numeric_rows"
    elif periods_without_numeric:
        assessment = "endpoint_unreachable_or_blocked"
    else:
        assessment = "all_periods_have_numeric_rows"
    max_year_by_report_type_id = {
        str(item.get("report_type_id")): item.get("max_report_year")
        for item in max_year_results or []
    }
    periods_beyond_report_type_max_year = [
        period
        for period in periods_without_numeric
        if _period_exceeds_report_type_max_year(
            period,
            max_year_by_report_type_id=max_year_by_report_type_id,
        )
    ]
    if periods_beyond_report_type_max_year:
        assessment = "period_beyond_sse_report_type_max_year"
    return {
        "assessment": assessment,
        "periods_with_numeric_rows": periods_with_numeric,
        "periods_without_numeric_rows": periods_without_numeric,
        "periods_beyond_report_type_max_year": periods_beyond_report_type_max_year,
        "max_year_by_report_type_id": max_year_by_report_type_id,
        "reachable_periods": reachable_periods,
        "total_row_count": sum(int(item.get("row_count") or 0) for item in results),
        "total_numeric_field_count": sum(
            int(item.get("numeric_field_count") or 0) for item in results
        ),
        "availability_counts": count_values(
            str(item.get("period_availability") or "unknown") for item in results
        ),
    }


def extract_max_report_year(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("result")
    if not isinstance(rows, list) or not rows:
        return None
    value = rows[0].get("REPORT_YEAR") if isinstance(rows[0], dict) else None
    return str(value) if value not in (None, "") else None


def _period_exceeds_report_type_max_year(
    report_period: str,
    *,
    max_year_by_report_type_id: Dict[str, Any],
) -> bool:
    report_type_id = sse_report_type_id(report_period)
    max_year = max_year_by_report_type_id.get(report_type_id)
    if max_year in (None, ""):
        return False
    period_year = str(report_period)[:4]
    if not period_year.isdigit() or not str(max_year).isdigit():
        return False
    return int(period_year) > int(str(max_year))


def parse_json_or_jsonp(text: str) -> Any:
    raw = str(text or "").lstrip("\ufeff\r\n\t ")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    match = re.match(r"^[A-Za-z_$][\w$.]*\s*\((.*)\)\s*;?\s*$", raw, flags=re.S)
    if not match:
        match = re.match(r"^null\s*\((.*)\)\s*;?\s*$", raw, flags=re.S)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def format_mapping(raw: Dict[str, Any], context: Dict[str, str]) -> Dict[str, Any]:
    return {str(key): format_value(value, context) for key, value in raw.items()}


def format_value(value: Any, context: Dict[str, str]) -> Any:
    if isinstance(value, str):
        return value.format_map(_SafeFormatDict(context))
    if isinstance(value, list):
        return [format_value(item, context) for item in value]
    if isinstance(value, dict):
        return format_mapping(value, context)
    return value


def count_values(values: Any) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe SSE commonQuery report-period availability.",
    )
    parser.add_argument("--instrument-id", default="600000.SH")
    parser.add_argument("--report-periods", default="2024Q4,2025Q4")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument("--enabled-only", action="store_true")
    parser.add_argument("--output-path", type=Path)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result = probe_sse_period_availability(
        instrument_id=args.instrument_id,
        report_periods=parse_csv(args.report_periods),
        timeout_seconds=args.timeout_seconds,
        request_interval_seconds=args.request_interval_seconds,
        include_disabled_candidates=not args.enabled_only,
    )
    payload = json_ready(result)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        with args.output_path.open("w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj, ensure_ascii=False, indent=2, sort_keys=True)
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
