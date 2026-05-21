#!/usr/bin/env python
"""Run live official financial JSON validation in bounded batches.

The command defaults to an isolated /tmp SQLite database. It is intended for
full-download preparation and does not mutate production research storage.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from research.official_financial_source_profiles import (  # noqa: E402
    default_official_source_for_exchange,
    parser_profile_for,
    source_profile_for,
    source_profile_metadata,
)
from scripts.dev_validation.validate_sse_official_financial_json_live import (  # noqa: E402
    run_validation,
)
from scripts.research_cli_support import json_ready  # noqa: E402
from scripts.research_financial_statements_rollout_validation import (  # noqa: E402
    normalize_report_periods,
)
from utils.config_manager import config_manager  # noqa: E402


DEFAULT_SAMPLE_IDS_BY_EXCHANGE = {
    "SSE": ["600000.SH", "600004.SH", "600009.SH", "600010.SH", "600011.SH"],
    "SZSE": ["000001.SZ", "000002.SZ", "000004.SZ", "000006.SZ", "000007.SZ"],
    "BSE": ["920000.BJ", "920001.BJ", "920002.BJ", "920003.BJ", "920005.BJ"],
}


def _default_db_path() -> Path:
    return Path("/tmp") / f"quote_official_financial_json_batches_{os.getpid()}.db"


def _default_checkpoint_path() -> Path:
    return Path("/tmp") / f"quote_official_financial_json_batches_{os.getpid()}.checkpoint.json"


def parse_instrument_ids(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def parse_report_periods(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def chunked(items: List[str], batch_size: int) -> Iterable[List[str]]:
    size = max(1, int(batch_size or 1))
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _normalize_report_periods(report_periods: List[str]) -> List[str]:
    return normalize_report_periods(report_periods)


def _checkpoint_key(instrument_id: str, report_period: str) -> str:
    return f"{instrument_id}|{report_period}"


def _completed_checkpoint_keys(
    checkpoint: Dict[str, Any],
    *,
    exchange: Optional[str] = None,
    report_periods: Optional[List[str]] = None,
    source: Optional[str] = None,
    source_profile: Optional[str] = None,
    parser_profile: Optional[str] = None,
) -> set[str]:
    if not _checkpoint_metadata_matches(
        checkpoint,
        exchange=exchange,
        report_periods=report_periods,
        source=source,
        source_profile=source_profile,
        parser_profile=parser_profile,
    ):
        return set()
    keys = set(str(item) for item in checkpoint.get("completed_instrument_periods", []))
    legacy_period = checkpoint.get("report_period")
    if legacy_period:
        legacy_period = _normalize_report_periods([str(legacy_period)])[0]
        for instrument_id in checkpoint.get("completed_instruments", []):
            keys.add(_checkpoint_key(str(instrument_id), str(legacy_period)))
    return keys


def _checkpoint_metadata_matches(
    checkpoint: Dict[str, Any],
    *,
    exchange: Optional[str] = None,
    report_periods: Optional[List[str]] = None,
    source: Optional[str] = None,
    source_profile: Optional[str] = None,
    parser_profile: Optional[str] = None,
) -> bool:
    checks = {
        "exchange": str(exchange).upper() if exchange else None,
        "source": str(source).lower() if source else None,
        "source_profile": source_profile,
        "parser_profile": parser_profile,
        "report_periods": _normalize_report_periods(report_periods or [])
        if report_periods
        else None,
    }
    for key, expected in checks.items():
        if expected is None:
            continue
        actual = checkpoint.get(key)
        if actual in (None, ""):
            continue
        if key == "exchange":
            actual = str(actual).upper()
        elif key == "source":
            actual = str(actual).lower()
        elif key == "parser_profile":
            actual = str(actual)
        elif key == "report_periods":
            actual = _normalize_report_periods(
                [actual] if isinstance(actual, str) else [str(item) for item in actual]
            )
        if actual != expected:
            return False
    return True


def resolve_instrument_ids(
    *,
    instrument_ids: Optional[List[str]],
    exchange: str,
    limit: Optional[int],
) -> List[str]:
    explicit_ids = instrument_ids or []
    if explicit_ids:
        return explicit_ids

    from_quotes = _load_instrument_ids_from_quotes_db(exchange=exchange, limit=limit)
    if from_quotes:
        return from_quotes

    samples = DEFAULT_SAMPLE_IDS_BY_EXCHANGE.get(str(exchange).upper(), [])
    return samples[:limit] if limit is not None else list(samples)


def _load_instrument_ids_from_quotes_db(
    *,
    exchange: str,
    limit: Optional[int],
) -> List[str]:
    research_config = config_manager.get_research_config()
    quotes_db_path = Path(str(research_config.storage.quotes_db_path or ""))
    if not quotes_db_path.is_absolute():
        quotes_db_path = ROOT_DIR / quotes_db_path
    if not quotes_db_path.exists():
        return []

    sql = (
        "SELECT instrument_id FROM instruments "
        "WHERE exchange = ? AND type = 'stock' AND COALESCE(is_active, 1) = 1 "
        "ORDER BY symbol"
    )
    params: List[Any] = [exchange]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))

    try:
        with sqlite3.connect(f"file:{quotes_db_path}?mode=ro", uri=True) as conn:
            rows = conn.execute(sql, params).fetchall()
    except sqlite3.Error:
        return []
    return [str(row[0]) for row in rows if row and row[0]]


async def run_batches(
    *,
    instrument_ids: List[str],
    exchange: str,
    official_source: Optional[str] = None,
    report_periods: List[str],
    db_path: Path,
    batch_size: int,
    batch_timeout_seconds: float,
    request_timeout_seconds: Optional[float],
    request_interval_seconds: Optional[float],
    checkpoint_path: Optional[Path] = None,
    include_batch_details: bool = False,
) -> Dict[str, Any]:
    started_at = time.perf_counter()
    if not report_periods:
        raise ValueError("At least one report period is required")
    official_source = official_source or default_official_source_for_exchange(exchange)
    source_profile = source_profile_for(exchange, official_source, strict=True)
    parser_profile = parser_profile_for(exchange, official_source)
    checkpoint = _load_checkpoint(checkpoint_path)
    normalized_report_periods = _normalize_report_periods(report_periods)
    required_keys = {
        _checkpoint_key(instrument_id, report_period)
        for instrument_id in instrument_ids
        for report_period in normalized_report_periods
    }
    completed_from_checkpoint = _completed_checkpoint_keys(
        checkpoint,
        exchange=exchange,
        report_periods=normalized_report_periods,
        source=official_source,
        source_profile=source_profile,
        parser_profile=parser_profile,
    )
    skipped_from_checkpoint = [
        instrument_id
        for instrument_id in instrument_ids
        if all(
            _checkpoint_key(instrument_id, report_period) in completed_from_checkpoint
            for report_period in normalized_report_periods
        )
    ]
    pending_ids = [
        instrument_id
        for instrument_id in instrument_ids
        if instrument_id not in skipped_from_checkpoint
    ]
    batches = list(chunked(pending_ids, batch_size))
    batch_results: List[Dict[str, Any]] = []

    for index, batch_ids in enumerate(batches, start=1):
        batch_started_at = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                run_validation(
                    instrument_ids=batch_ids,
                    exchange=exchange,
                    official_source=official_source,
                    report_periods=report_periods,
                    db_path=db_path,
                    request_timeout_seconds=request_timeout_seconds,
                    request_interval_seconds=request_interval_seconds,
                ),
                timeout=float(batch_timeout_seconds),
            )
            batch_status = result.get("status", "unknown")
            failed_instruments = [
                item["instrument_id"]
                for item in result.get("instrument_results", [])
                if item.get("status") != "passed"
            ]
            failed_instrument_periods = result.get("failed_instrument_periods", [])
            batch_result = {
                "batch_index": index,
                "status": batch_status,
                "source": result.get("source") or result.get("official_source"),
                "source_profile": result.get("source_profile") or source_profile,
                "parser_profile": result.get("parser_profile") or parser_profile,
                "source_mode": result.get("source_mode"),
                "parser_version": result.get("parser_version"),
                "numeric_fact_parser": result.get("numeric_fact_parser"),
                "structured_json_fact_parser": result.get(
                    "structured_json_fact_parser"
                ),
                "alias_mapping_version": result.get("alias_mapping_version"),
                "instrument_ids": batch_ids,
                "instrument_count": len(batch_ids),
                "report_periods": result.get("report_periods", normalized_report_periods),
                "instrument_period_count": len(batch_ids)
                * len(normalized_report_periods),
                "elapsed_seconds": round(time.perf_counter() - batch_started_at, 3),
                "failed_instruments": sorted(set(failed_instruments)),
                "failed_instrument_periods": failed_instrument_periods,
                "sync_summary": _sync_summary(result.get("sync", {})),
                "period_summaries": _period_summaries(result.get("period_results", [])),
            }
            if include_batch_details:
                batch_result["result"] = result
            batch_results.append(batch_result)
            if batch_status == "passed":
                _mark_checkpoint_success(
                    checkpoint,
                    batch_ids=batch_ids,
                    report_periods=normalized_report_periods,
                    batch_result=batch_result,
                )
            else:
                _mark_checkpoint_failure(
                    checkpoint,
                    batch_ids=batch_ids,
                    report_periods=normalized_report_periods,
                    batch_result=batch_result,
                )
        except asyncio.TimeoutError:
            batch_result = {
                "batch_index": index,
                "status": "timeout",
                "source": official_source,
                "source_profile": source_profile,
                "parser_profile": parser_profile,
                "source_mode": "direct",
                "instrument_ids": batch_ids,
                "instrument_count": len(batch_ids),
                "report_periods": normalized_report_periods,
                "instrument_period_count": len(batch_ids)
                * len(normalized_report_periods),
                "elapsed_seconds": round(time.perf_counter() - batch_started_at, 3),
                "failed_instruments": batch_ids,
                "failed_instrument_periods": [
                    {"instrument_id": instrument_id, "report_period": report_period}
                    for instrument_id in batch_ids
                    for report_period in normalized_report_periods
                ],
                "error": f"batch_timeout_seconds={batch_timeout_seconds}",
            }
            batch_results.append(batch_result)
            _mark_checkpoint_failure(
                checkpoint,
                batch_ids=batch_ids,
                report_periods=normalized_report_periods,
                batch_result=batch_result,
            )
        except Exception as exc:
            batch_result = {
                "batch_index": index,
                "status": "failed",
                "source": official_source,
                "source_profile": source_profile,
                "parser_profile": parser_profile,
                "source_mode": "direct",
                "instrument_ids": batch_ids,
                "instrument_count": len(batch_ids),
                "report_periods": normalized_report_periods,
                "instrument_period_count": len(batch_ids)
                * len(normalized_report_periods),
                "elapsed_seconds": round(time.perf_counter() - batch_started_at, 3),
                "failed_instruments": batch_ids,
                "failed_instrument_periods": [
                    {"instrument_id": instrument_id, "report_period": report_period}
                    for instrument_id in batch_ids
                    for report_period in normalized_report_periods
                ],
                "error": str(exc),
            }
            batch_results.append(batch_result)
            _mark_checkpoint_failure(
                checkpoint,
                batch_ids=batch_ids,
                report_periods=normalized_report_periods,
                batch_result=batch_result,
            )
        _save_checkpoint(
            checkpoint_path,
            checkpoint,
            exchange=exchange,
            report_periods=normalized_report_periods,
            db_path=db_path,
            source=official_source,
            source_profile=source_profile,
            parser_profile=parser_profile,
        )

    failed_instruments = [
        instrument_id
        for batch in batch_results
        for instrument_id in batch.get("failed_instruments", [])
    ]
    failed_instrument_periods = [
        item
        for batch in batch_results
        for item in batch.get("failed_instrument_periods", [])
    ]
    passed_batches = [
        batch for batch in batch_results if batch.get("status") == "passed"
    ]
    elapsed_seconds = time.perf_counter() - started_at
    summary = {
        "status": "passed" if not failed_instruments else "degraded",
        "write_enabled": False,
        "db_path": str(db_path),
        "storage_target": {
            "kind": "temp_sqlite",
            "db_path": str(db_path),
        },
        "checkpoint_path": str(checkpoint_path) if checkpoint_path else None,
        "exchange": exchange,
        "source": official_source,
        "official_source": official_source,
        "source_profile": source_profile,
        "source_profile_metadata": source_profile_metadata(
            exchange,
            official_source,
            strict=False,
        ),
        "source_mode": "direct",
        "parser_version": _first_present(batch_results, "parser_version"),
        "parser_profile": _first_present(batch_results, "parser_profile")
        or parser_profile,
        "numeric_fact_parser": _first_present(batch_results, "numeric_fact_parser"),
        "structured_json_fact_parser": _first_present(
            batch_results,
            "structured_json_fact_parser",
        ),
        "alias_mapping_version": _first_present(batch_results, "alias_mapping_version"),
        "report_period": (
            normalized_report_periods[0] if len(normalized_report_periods) == 1 else None
        ),
        "report_periods": normalized_report_periods,
        "request_policy": {
            "request_timeout_seconds": request_timeout_seconds,
            "request_interval_seconds": request_interval_seconds,
            "batch_timeout_seconds": batch_timeout_seconds,
            "source_profile": source_profile,
            "parser_profile": parser_profile,
            "batch_size": batch_size,
            "retry_attempts": None,
            "retry_backoff_seconds": None,
            "max_concurrency": 1,
            "concurrency_assumption": "single_process_sequential",
        },
        "instrument_count": len(instrument_ids),
        "instrument_period_count": len(instrument_ids) * len(normalized_report_periods),
        "pending_instrument_count": len(pending_ids),
        "pending_instrument_period_count": len(required_keys - completed_from_checkpoint),
        "skipped_from_checkpoint_count": len(skipped_from_checkpoint),
        "batch_size": batch_size,
        "batch_count": len(batches),
        "passed_batch_count": len(passed_batches),
        "failed_batch_count": len(batches) - len(passed_batches),
        "failed_instrument_count": len(failed_instruments),
        "failed_instruments": failed_instruments,
        "failed_instrument_period_count": len(failed_instrument_periods),
        "failed_instrument_periods": failed_instrument_periods,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "throughput_instruments_per_minute": round(
            (len(pending_ids) / elapsed_seconds * 60.0)
            if elapsed_seconds > 0
            else 0.0,
            3,
        ),
        "throughput_instrument_periods_per_minute": round(
            (
                len(pending_ids)
                * len(normalized_report_periods)
                / elapsed_seconds
                * 60.0
            )
            if elapsed_seconds > 0
            else 0.0,
            3,
        ),
        "period_summaries": _merge_period_summaries(batch_results),
        "total_source_manifests_written": sum(
            int(batch.get("sync_summary", {}).get("source_manifests_written") or 0)
            for batch in batch_results
        ),
        "total_numeric_facts_written": sum(
            int(batch.get("sync_summary", {}).get("numeric_facts_written") or 0)
            for batch in batch_results
        ),
        "total_core_facts_written": sum(
            int(batch.get("sync_summary", {}).get("core_facts_written") or 0)
            for batch in batch_results
        ),
    }
    if include_batch_details:
        summary["skipped_from_checkpoint"] = sorted(skipped_from_checkpoint)
    return {**summary, "batches": batch_results}


def _first_present(items: List[Dict[str, Any]], key: str) -> Optional[Any]:
    for item in items:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def _load_checkpoint(checkpoint_path: Optional[Path]) -> Dict[str, Any]:
    if checkpoint_path is None or not checkpoint_path.exists():
        return {
            "completed_instruments": [],
            "completed_instrument_periods": [],
            "failed_batches": [],
            "completed_batches": [],
        }
    try:
        with checkpoint_path.open("r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except (OSError, json.JSONDecodeError):
        payload = {}
    payload.setdefault("completed_instruments", [])
    payload.setdefault("completed_instrument_periods", [])
    payload.setdefault("failed_batches", [])
    payload.setdefault("completed_batches", [])
    return payload


def _save_checkpoint(
    checkpoint_path: Optional[Path],
    checkpoint: Dict[str, Any],
    *,
    exchange: str,
    report_periods: List[str],
    db_path: Path,
    source: Optional[str] = None,
    source_profile: Optional[str] = None,
    parser_profile: Optional[str] = None,
) -> None:
    if checkpoint_path is None:
        return
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint["exchange"] = exchange
    if source:
        checkpoint["source"] = source
    if source_profile:
        checkpoint["source_profile"] = source_profile
    if parser_profile:
        checkpoint["parser_profile"] = parser_profile
    checkpoint["report_period"] = report_periods[0] if len(report_periods) == 1 else None
    checkpoint["report_periods"] = report_periods
    checkpoint["db_path"] = str(db_path)
    checkpoint["updated_at_epoch"] = time.time()
    tmp_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as file_obj:
        json.dump(checkpoint, file_obj, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(checkpoint_path)


def _mark_checkpoint_success(
    checkpoint: Dict[str, Any],
    *,
    batch_ids: List[str],
    report_periods: List[str],
    batch_result: Dict[str, Any],
) -> None:
    completed = set(checkpoint.get("completed_instruments", []))
    completed.update(batch_ids)
    checkpoint["completed_instruments"] = sorted(completed)
    completed_periods = set(
        str(item) for item in checkpoint.get("completed_instrument_periods", [])
    )
    completed_periods.update(
        _checkpoint_key(instrument_id, report_period)
        for instrument_id in batch_ids
        for report_period in report_periods
    )
    checkpoint["completed_instrument_periods"] = sorted(completed_periods)
    checkpoint.setdefault("completed_batches", []).append(
        _checkpoint_batch_record(batch_result)
    )


def _mark_checkpoint_failure(
    checkpoint: Dict[str, Any],
    *,
    batch_ids: List[str],
    report_periods: List[str],
    batch_result: Dict[str, Any],
) -> None:
    checkpoint.setdefault("failed_batches", []).append(
        {
            **_checkpoint_batch_record(batch_result),
            "instrument_ids": batch_ids,
            "report_periods": report_periods,
            "failed_instruments": batch_result.get("failed_instruments", batch_ids),
            "failed_instrument_periods": batch_result.get(
                "failed_instrument_periods",
                [
                    {"instrument_id": instrument_id, "report_period": report_period}
                    for instrument_id in batch_ids
                    for report_period in report_periods
                ],
            ),
            "error": batch_result.get("error"),
        }
    )


def _checkpoint_batch_record(batch_result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "batch_index": batch_result.get("batch_index"),
        "status": batch_result.get("status"),
        "source": batch_result.get("source"),
        "source_profile": batch_result.get("source_profile"),
        "parser_profile": batch_result.get("parser_profile"),
        "source_mode": batch_result.get("source_mode"),
        "instrument_count": batch_result.get("instrument_count"),
        "instrument_period_count": batch_result.get("instrument_period_count"),
        "report_periods": batch_result.get("report_periods", []),
        "elapsed_seconds": batch_result.get("elapsed_seconds"),
        "sync_summary": batch_result.get("sync_summary", {}),
        "period_summaries": batch_result.get("period_summaries", []),
    }


def _sync_summary(sync: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status": sync.get("status"),
        "source_manifests_written": sync.get("total_source_manifests_written", 0),
        "numeric_facts_written": sync.get("total_numeric_facts_written", 0),
        "core_facts_written": sync.get("total_core_facts_written", 0),
        "successful_exchanges": sync.get("successful_exchanges", 0),
        "attempted_exchanges": sync.get("attempted_exchanges", 0),
    }


def _period_summaries(period_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    for item in period_results:
        sync = _sync_summary(item.get("sync", {}))
        failed_pairs = item.get("failed_instrument_periods", [])
        summaries.append(
            {
                "input_report_period": item.get("input_report_period"),
                "report_period": item.get("report_period"),
                "elapsed_seconds": item.get("elapsed_seconds"),
                "failed_instrument_period_count": len(failed_pairs),
                "failed_instrument_periods": failed_pairs,
                **sync,
            }
        )
    return summaries


def _merge_period_summaries(
    batch_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for batch in batch_results:
        for item in batch.get("period_summaries", []):
            report_period = str(item.get("report_period") or "")
            if not report_period:
                continue
            target = merged.setdefault(
                report_period,
                {
                    "report_period": report_period,
                    "source_manifests_written": 0,
                    "numeric_facts_written": 0,
                    "core_facts_written": 0,
                    "failed_instrument_period_count": 0,
                    "failed_instrument_periods": [],
                    "elapsed_seconds": 0.0,
                },
            )
            target["source_manifests_written"] += int(
                item.get("source_manifests_written") or 0
            )
            target["numeric_facts_written"] += int(
                item.get("numeric_facts_written") or 0
            )
            target["core_facts_written"] += int(item.get("core_facts_written") or 0)
            failed_pairs = item.get("failed_instrument_periods", [])
            target["failed_instrument_period_count"] += len(failed_pairs)
            target["failed_instrument_periods"].extend(failed_pairs)
            target["elapsed_seconds"] = round(
                float(target.get("elapsed_seconds") or 0.0)
                + float(item.get("elapsed_seconds") or 0.0),
                3,
            )
    return [merged[key] for key in sorted(merged)]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate official financial structured JSON in batches.",
    )
    parser.add_argument("--exchange", default="SSE")
    parser.add_argument(
        "--official-source",
        help="Official source. Defaults to sse for SSE and cninfo for SZSE/BSE.",
    )
    parser.add_argument("--report-period", default="2023Q4")
    parser.add_argument(
        "--report-periods",
        help="Comma-separated report periods. Overrides --report-period.",
    )
    parser.add_argument("--instrument-ids")
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Limit instruments loaded from quotes DB. Defaults to 5.",
    )
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--batch-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--request-timeout-seconds", type=float, default=8.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.2)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=_default_db_path(),
        help="Isolated SQLite DB path. Defaults to /tmp with the current pid.",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=Path,
        default=_default_checkpoint_path(),
        help="JSON checkpoint path. Existing completed instruments are skipped.",
    )
    parser.add_argument(
        "--include-batch-details",
        action="store_true",
        help="Include each nested validator result. Disabled by default for larger samples.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    instrument_ids = resolve_instrument_ids(
        instrument_ids=parse_instrument_ids(args.instrument_ids),
        exchange=args.exchange,
        limit=args.limit,
    )
    result = asyncio.run(
        run_batches(
            instrument_ids=instrument_ids,
            exchange=args.exchange,
            official_source=args.official_source
            or default_official_source_for_exchange(args.exchange),
            report_periods=(
                parse_report_periods(args.report_periods)
                if args.report_periods
                else [args.report_period]
            ),
            db_path=args.db_path,
            batch_size=args.batch_size,
            batch_timeout_seconds=args.batch_timeout_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
            request_interval_seconds=args.request_interval_seconds,
            checkpoint_path=args.checkpoint_path,
            include_batch_details=args.include_batch_details,
        )
    )
    print(json.dumps(json_ready(result), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
