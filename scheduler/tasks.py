"""
Scheduled tasks for the quote system.
Defines all automated data update and maintenance tasks.
"""

import asyncio
import hashlib
import json
import os
import threading
import time as time_module
from datetime import datetime, date, timedelta, time
from pathlib import Path
from typing import List, Dict, Any, Optional, Union

# Some tests and operator scripts import scheduler.tasks directly. Install the
# proxy patch before project utility imports can pull in HTTP client stacks.
from proxy_patch_bootstrap import install_akshare_proxy_patch as _install_akshare_proxy_patch

_install_akshare_proxy_patch(required=False)

from utils import scheduler_logger, config_manager, TelegramBot
from .job_config import JobConfig
from .database_backup import (
    BACKUP_STATUS_FAILED,
    BACKUP_STATUS_SKIPPED,
    BACKUP_STATUS_SUCCESS,
    DatabaseBackupResult,
    DatabaseBackupRunResult,
    DatabaseBackupService,
)
from data_manager import data_manager
from instrument_master_governance import MasterGovernanceRequirement
from utils.date_utils import DateUtils, get_shanghai_time
from utils.cache import cache_manager
from utils.a_share_historical_backfill import (
    A_SHARE_BACKFILL_DEFAULT_SCOPES,
    A_SHARE_BACKFILL_OPTIONAL_SCOPES,
    A_SHARE_EXCHANGE_INCEPTION,
    AShareBackfillCheckpointStore,
    coerce_date,
    evaluate_calendar_coverage,
    normalize_a_share_backfill_parameters,
    normalize_string_list,
    serialize_checkpoint_parameters,
)
from research.backtest_data.corporate_action_projection import (
    CanonicalCorporateActionProjector,
)
from research.backtest_data.corporate_action_history_backfill import (
    CanonicalCorporateActionHistoryBackfill,
)
from research.backtest_data.financial_store import FinancialVintageStore
from research.backtest_data.maintenance import BacktestDataMaintenance
from research.backtest_data.index_constituent_history_backfill import (
    CoreIndexConstituentHistoryBackfill,
    SUPPORTED_INDEXES,
)
from research.backtest_data.rollout import BacktestRolloutPolicy


def _quote_database_path() -> str:
    db = getattr(getattr(data_manager, "db_ops", None), "db", None)
    return str(
        getattr(db, "db_path", None)
        or config_manager.get_nested("database_config.db_path", "data/quotes.db")
    )


def _disabled_backtest_stage(name: str) -> Dict[str, Any]:
    return {
        "stage": name,
        "status": "disabled",
        "reuse_decision": "extend_existing",
        "provider_usage": [],
        "network_requests": 0,
        "blockers": ["rollout_disabled"],
    }


async def _run_index_constituent_history_stage(
    *,
    start_date: date,
    end_date: date,
    index_instrument_ids: List[str],
    daily_request_reserve: int,
    sampling: str,
    max_queries_per_run: int,
    checkpoint_path: str,
    dry_run: bool,
    resume: bool,
) -> Dict[str, Any]:
    from data_sources.baostock_source import BaostockAccessGovernor, BaostockSource
    from data_sources.base_source import RateLimitConfig
    from utils.exceptions import DataSourceError, NetworkError

    source_config = config_manager.get("data_sources_config", {}).get("baostock", {})
    if not source_config.get("enabled", False):
        return {
            "stage": "index_composition",
            "status": "unavailable",
            "network_requests": 0,
            "provider_usage": [],
            "blockers": ["baostock_source_disabled"],
            "membership_readiness": "unavailable",
            "weight_readiness": "deferred",
        }
    daily_limit = int(source_config.get("daily_request_safety_limit", 40000))
    usage_state_path = source_config.get(
        "usage_state_path", "data/runtime/baostock/api_usage.json"
    )
    session_lock_path = source_config.get(
        "session_lock_path", "data/runtime/baostock/session.lock"
    )
    default_usage_path = "data/runtime/baostock/api_usage.json"
    default_lock_path = "data/runtime/baostock/session.lock"
    governor = BaostockAccessGovernor(
        daily_request_limit=daily_limit,
        state_path=usage_state_path,
        session_lock_path=session_lock_path,
        legacy_state_path=(
            "~/.cache/quote/baostock_api_usage.json"
            if BaostockAccessGovernor._resolve_path(usage_state_path)
            == BaostockAccessGovernor._resolve_path(default_usage_path)
            else None
        ),
        legacy_session_lock_path=(
            "~/.cache/quote/baostock_session.lock"
            if BaostockAccessGovernor._resolve_path(session_lock_path)
            == BaostockAccessGovernor._resolve_path(default_lock_path)
            else None
        ),
    )
    records = await data_manager.db_ops.get_trading_calendar_records(
        "SSE", start_date, end_date
    )
    calendar_coverage = evaluate_calendar_coverage(
        "SSE", start_date, end_date, records
    )
    if calendar_coverage.get("status") == "blocked":
        return {
            "stage": "index_composition",
            "status": "blocked",
            "network_requests": 0,
            "provider_usage": [],
            "blockers": [
                "SSE:calendar_coverage_incomplete:"
                f"{calendar_coverage.get('missing_days', 0)}"
            ],
            "calendar_coverage": calendar_coverage,
            "membership_readiness": "unavailable",
            "weight_readiness": "deferred",
        }
    trading_dates = []
    for record in records:
        if not bool(record.get("is_trading_day")):
            continue
        value = record.get("date")
        if isinstance(value, datetime):
            value = value.date()
        elif not isinstance(value, date):
            try:
                value = datetime.fromisoformat(str(value)[:10]).date()
            except (TypeError, ValueError):
                continue
        trading_dates.append(value)

    service = CoreIndexConstituentHistoryBackfill(
        quotes_db_path=_quote_database_path(),
        checkpoint_path=checkpoint_path,
        quota_reader=governor.usage_snapshot,
    )
    try:
        plan = service.build_plan(
            start_date=start_date,
            end_date=end_date,
            trading_dates=trading_dates,
            indexes=index_instrument_ids,
            daily_request_reserve=daily_request_reserve,
            sampling=sampling,
            max_queries_per_run=max_queries_per_run,
        )
    except ValueError as exc:
        return {
            "stage": "index_composition",
            "status": "unavailable",
            "network_requests": 0,
            "provider_usage": [],
            "blockers": [str(exc)],
            "supported_indexes": sorted(SUPPORTED_INDEXES),
            "membership_readiness": "unavailable",
            "weight_readiness": "deferred",
        }
    dry_run_result = service.dry_run(plan)
    if dry_run:
        return dry_run_result
    non_quota_blockers = [
        item for item in (dry_run_result.get("blockers") or [])
        if item != "insufficient_baostock_quota_headroom"
    ]
    usable_requests = int(
        (dry_run_result.get("quota") or {}).get("usable", 0) or 0
    )
    if non_quota_blockers or usable_requests < 3:
        return dry_run_result

    source = None
    owns_source = False
    source_factory = getattr(data_manager, "source_factory", None)
    if source_factory is not None:
        source = source_factory.get_source_instance("baostock", region="a_stock")
    if source is None:
        rate_limit = RateLimitConfig(
            max_requests_per_minute=int(source_config.get("max_requests_per_minute", 300)),
            max_requests_per_hour=int(source_config.get("max_requests_per_hour", 5000)),
            max_requests_per_day=int(source_config.get("max_requests_per_day", 40000)),
            retry_times=int(source_config.get("retry_times", 5)),
            retry_interval=float(source_config.get("retry_interval", 5.0)),
            min_interval_seconds=float(source_config.get("min_interval_seconds", 0.2)),
        )
        source = BaostockSource(
            "baostock_index_history",
            rate_limit,
            connection_timeout_seconds=float(source_config.get("connection_timeout", 30.0)),
            login_timeout_seconds=float(source_config.get("login_timeout", 30.0)),
            daily_request_safety_limit=daily_limit,
            usage_state_path=source_config.get(
                "usage_state_path", "data/runtime/baostock/api_usage.json"
            ),
            session_lock_path=source_config.get(
                "session_lock_path", "data/runtime/baostock/session.lock"
            ),
        )
        owns_source = True
    if not hasattr(source, "get_historical_index_constituents"):
        return {
            "stage": "index_composition",
            "status": "unavailable",
            "network_requests": 0,
            "provider_usage": [],
            "blockers": ["configured_baostock_history_source_unavailable"],
            "membership_readiness": "unavailable",
            "weight_readiness": "deferred",
        }
    quota_before = governor.usage_snapshot()
    try:
        await source.initialize()
        service.fetcher = source.get_historical_index_constituents
        stage_result = await service.run(plan, resume=resume)
    except (DataSourceError, NetworkError) as exc:
        stage_result = {
            "stage": "index_composition",
            "status": "blocked",
            "network_requests": 0,
            "provider_usage": [],
            "blockers": [f"baostock_source_unavailable:{exc}"],
            "membership_readiness": "unavailable",
            "weight_readiness": "deferred",
            "retryable": True,
        }
    finally:
        if owns_source:
            await source.close()
            source._bs_executor.shutdown(wait=False)
    quota_after = governor.usage_snapshot()
    stage_result["quota_before"] = quota_before
    stage_result["quota_after"] = quota_after
    stage_result["calendar_coverage"] = calendar_coverage
    stage_result["quota_request_delta"] = max(
        int(quota_after.get("count", 0)) - int(quota_before.get("count", 0)), 0
    )
    stage_result.setdefault("totals", {})["quota_request_delta"] = stage_result[
        "quota_request_delta"
    ]
    return stage_result


async def _run_backtest_stage(
    name: str,
    function: Any,
    *args: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    policy = BacktestRolloutPolicy.load().stage(name)
    if not policy.enabled:
        return _disabled_backtest_stage(name)
    last_error: Optional[Exception] = None
    timed_out = False
    for attempt in range(1, policy.retry_count + 2):
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(function, *args, **kwargs),
                timeout=policy.timeout_seconds,
            )
            result.setdefault("controls", {})
            result["controls"].update(
                {
                    "attempt": attempt,
                    "timeout_seconds": policy.timeout_seconds,
                    "retry_count": policy.retry_count,
                    "continue_on_error": policy.continue_on_error,
                    "freshness_hours": policy.freshness_hours,
                    "max_rows": policy.max_rows,
                }
            )
            return result
        except asyncio.TimeoutError as exc:
            last_error = exc
            timed_out = True
            break
        except Exception as exc:
            last_error = exc
            if attempt <= policy.retry_count:
                continue
    blocker = (
        "stage_timeout_in_flight_not_retried"
        if timed_out
        else str(last_error or "stage_failed")
    )
    failure = {
        "stage": name,
        "status": "failed",
        "reuse_decision": "extend_existing",
        "provider_usage": [],
        "network_requests": 0,
        "blockers": [blocker],
        "controls": {
            "attempt": attempt,
            "timeout_seconds": policy.timeout_seconds,
            "retry_count": policy.retry_count,
            "continue_on_error": policy.continue_on_error,
            "freshness_hours": policy.freshness_hours,
            "max_rows": policy.max_rows,
        },
    }
    if not policy.continue_on_error:
        raise RuntimeError(f"backtest stage {name} failed: {blocker}")
    return failure


def _financial_vintage_stage_report(
    db_path: str,
    *,
    inherited_scope: Dict[str, Any],
    dry_run: bool,
) -> Dict[str, Any]:
    """Report local filing-vintage state without starting acquisition."""
    base = {
        "stage": "financial_filing_vintages",
        "reuse_decision": "extend_existing",
        "inherited_scope": inherited_scope,
        "provider_usage": [],
        "network_requests": 0,
        "inserted": 0,
        "changed": 0,
        "unchanged": 0,
        "database_id": "financials",
    }
    if dry_run:
        return {
            **base,
            "status": "dry_run",
            "readiness": None,
            "watermark": None,
            "blockers": ["dry_run_no_write"],
        }
    try:
        store = FinancialVintageStore(db_path)
        store.initialize()
        readiness = store.readiness()
    except Exception as exc:
        return {
            **base,
            "status": "unavailable",
            "readiness": None,
            "watermark": None,
            "blockers": [f"financial_vintage_readiness_unavailable:{exc}"],
        }
    filings = readiness.get("filings") or {}
    facts = readiness.get("facts") or {}
    blockers = []
    if int(filings.get("missing_availability") or 0):
        blockers.append("filing_availability_missing")
    if int(filings.get("missing_artifact") or 0):
        blockers.append("filing_artifact_missing")
    if int(facts.get("unknown_semantic") or 0):
        blockers.append("financial_period_semantic_unknown")
    if int(readiness.get("unresolved_relationships") or 0):
        blockers.append("filing_relationship_unresolved")
    return {
        **base,
        "status": "degraded" if blockers else "success",
        "readiness": readiness,
        "watermark": readiness.get("latest_watermark"),
        "blockers": blockers,
    }


def _business_profile_backfill_control_store():
    from research.business_profile_backfill_control import (
        BusinessProfileBackfillControlStore,
    )

    checkpoint_root = config_manager.get_nested(
        "research_config.modules.business_profile_evidence."
        "production_operations.checkpoint_root",
        "data/checkpoints/business_profile_async",
    )
    return BusinessProfileBackfillControlStore(str(checkpoint_root))


def _format_business_profile_backfill_progress(progress: Dict[str, Any]) -> str:
    latest = dict(progress.get("latest_result") or {})
    queue = dict(progress.get("queue_health") or latest.get("queue_health") or {})
    readiness = dict(
        progress.get("rollout_readiness")
        or latest.get("rollout_readiness")
        or {}
    )
    cumulative = dict(
        progress.get("cumulative_workers") or latest.get("workers") or {}
    )
    throughput = dict(latest.get("throughput") or {})
    enqueue = dict(latest.get("enqueue") or {})
    worker_text = "，".join(
        f"{stage}:完成{int(dict(values or {}).get('completed') or 0)}"
        f"/重试{int(dict(values or {}).get('retried') or 0)}"
        for stage, values in cumulative.items()
    ) or "暂无"
    reasons = list(progress.get("reason_codes") or [])
    return (
        f"状态: {progress.get('state', 'unknown')}\n"
        f"run_id: {progress.get('run_id') or 'N/A'}\n"
        f"阶段: {progress.get('phase') or 'N/A'}\n"
        f"循环: {int(progress.get('cycle') or 0)}，空转: "
        f"{int(progress.get('idle_cycles') or 0)}\n"
        f"heartbeat_age_seconds: {progress.get('heartbeat_age_seconds')}\n"
        f"队列: claimable={int(queue.get('claimable') or 0)}，"
        f"running={int(queue.get('running') or 0)}，"
        f"terminal={int(queue.get('terminal') or 0)}\n"
        f"当前年报覆盖率: "
        f"{float(readiness.get('current_annual_coverage_ratio') or 0):.2%}\n"
        f"phase_ready: {bool(readiness.get('phase_ready'))}\n"
        f"本批: 入队{int(throughput.get('enqueued') or enqueue.get('inserted') or 0)}，"
        f"完整完成{int(throughput.get('worker_completed') or 0)}\n"
        f"累计: {worker_text}\n"
        f"原因: {','.join(str(item) for item in reasons) or '无'}"
    )


def _business_profile_completed_items(result: Dict[str, Any]) -> int:
    progress = dict(result.get("continuous_progress") or {})
    cumulative = dict(progress.get("cumulative_workers") or {})
    if cumulative:
        return int(dict(cumulative.get("publish") or {}).get("completed") or 0)
    return int((result.get("throughput") or {}).get("worker_completed") or 0)


def _apply_hkex_gap_guard(
    gaps: List[Any],
    max_segments_per_instrument: Optional[int],
    max_missing_days_per_instrument: Optional[int],
) -> tuple[List[Any], List[Dict[str, Any]], int]:
    """为 HKEX 自动修复添加保护，避免单个标的海量缺口拖垮整个任务。"""
    if not gaps:
        return gaps, [], 0

    grouped: Dict[str, List[Any]] = {}
    for gap in gaps:
        grouped.setdefault(gap.instrument_id, []).append(gap)

    filtered: List[Any] = []
    skipped_details: List[Dict[str, Any]] = []
    skipped_count = 0

    for instrument_id, instrument_gaps in grouped.items():
        first_gap = instrument_gaps[0]
        if first_gap.exchange != 'HKEX':
            filtered.extend(instrument_gaps)
            continue

        segments = len(instrument_gaps)
        missing_days = sum(gap.gap_days for gap in instrument_gaps)

        skip_reasons = []
        if max_segments_per_instrument and segments > max_segments_per_instrument:
            skip_reasons.append(f"segments>{max_segments_per_instrument}")
        if max_missing_days_per_instrument and missing_days > max_missing_days_per_instrument:
            skip_reasons.append(f"missing_days>{max_missing_days_per_instrument}")

        if skip_reasons:
            skipped_count += segments
            skipped_details.append({
                'instrument_id': instrument_id,
                'symbol': first_gap.symbol,
                'exchange': first_gap.exchange,
                'gap_segments': segments,
                'missing_days': missing_days,
                'reason': ','.join(skip_reasons),
            })
            continue

        filtered.extend(instrument_gaps)

    return filtered, skipped_details, skipped_count


def _format_scheduler_status(status: Any) -> tuple[str, str]:
    """Return a Telegram icon and concise Chinese label for task status."""
    status_text = str(status or "unknown")
    normalized = status_text.lower()
    if normalized == "success":
        return "✅", "成功"
    if normalized in {"degraded", "warning", "partial"}:
        return "⚠️", "部分完成"
    if normalized in {"skipped", "disabled", "unavailable", "dry_run"}:
        return "ℹ️", "未执行"
    if normalized in {"failed", "error", "blocked"}:
        return "❌", "失败"
    return "ℹ️", status_text


def _format_a_share_historical_backfill_report(result: Dict[str, Any]) -> str:
    """Build a bounded operator report for the unified A-share history task."""
    normalized_status = str(result.get('status') or '').lower()
    if normalized_status == 'dry_run':
        icon, label = 'ℹ️', '预演完成'
    elif normalized_status == 'scan_only':
        icon, label = 'ℹ️', '源扫描完成'
    else:
        icon, label = _format_scheduler_status(result.get('status'))
    parameters = result.get('parameters') or {}
    stages = result.get('stages') or {}
    lines = []
    stage_counter_keys = {
        'dividends': (
            'raw_events', 'saved_events', 'pending_factors',
            'empty_instruments', 'errors', 'timeouts',
        ),
        'factors': (
            'derived_factors', 'pending_factors_detected', 'pending_factors',
            'raw_events',
            'errors', 'timeouts',
        ),
        'pending_quote_repair': (
            'target_instruments', 'quote_rows_saved', 'quote_failures',
            'redriven_factors', 'remaining_pending_factors',
        ),
        'completeness': (
            'persisted_tdx_events', 'pending_factors', 'pending_instruments',
            'unresolved_lifecycle_instruments', 'degraded_lifecycle_fallbacks',
            'exact_factor_matches', 'shifted_factor_matches',
            'factor_conflicts', 'reference_factor_change_only',
            'tdx_event_only',
        ),
    }
    for stage_name in (
        'master', 'calendar', 'quotes', 'dividends', 'factors',
        'index_composition', 'security_state', 'price_limits', 'corporate_actions',
        'pending_quote_repair', 'completeness',
    ):
        stage = stages.get(stage_name) or {}
        counters = stage.get('totals') or stage.get('counters') or {}
        preferred_keys = stage_counter_keys.get(stage_name)
        counter_items = (
            ((key, counters.get(key)) for key in preferred_keys)
            if preferred_keys
            else list(counters.items())[:6]
        )
        detail = ', '.join(
            f"{key}={value}"
            for key, value in counter_items
            if key in counters
            if isinstance(value, (int, float, str))
        )
        lines.append(
            f"{stage_name}: {stage.get('status', 'skipped')}"
            + (f" ({detail})" if detail else "")
        )
    blocker_lines = [str(item) for item in (result.get('blockers') or [])[:10]]
    failure_samples = result.get('failure_samples') or []
    sample_lines = [
        f"{item.get('instrument_id', item.get('exchange', 'unknown'))}: {item.get('reason', 'failed')}"
        for item in failure_samples[:10]
        if isinstance(item, dict)
    ]
    completeness_samples = (stages.get('completeness') or {}).get('samples') or []
    remediation_lines = []
    for item in completeness_samples[:10]:
        if not isinstance(item, dict):
            continue
        date_text = item.get('ex_date', '')
        if not date_text and (item.get('tdx_ex_date') or item.get('reference_ex_date')):
            date_text = (
                f"tdx={item.get('tdx_ex_date', '?')} "
                f"reference={item.get('reference_ex_date', '?')}"
            )
        remediation_lines.append((
            f"{item.get('instrument_id', item.get('exchange', 'unknown'))}"
            f" {date_text}: {item.get('reason', 'review_required')}"
        ).strip())
    extras = ""
    if blocker_lines:
        extras += "\n\n阻断项:\n```text\n" + "\n".join(blocker_lines) + "\n```"
    if sample_lines:
        extras += "\n\n失败样本:\n```text\n" + "\n".join(sample_lines) + "\n```"
    if remediation_lines:
        extras += "\n\n完整性样本:\n```text\n" + "\n".join(remediation_lines) + "\n```"
    return (
        f"{icon} *A 股历史全量回补*\n\n"
        f"结论: *{label}*\n"
        f"状态: `{result.get('status')}`\n"
        f"dry_run: `{result.get('dry_run')}`\n"
        f"scan_sources: `{result.get('scan_sources', False)}`\n"
        f"repair_pending_factor_quotes: `{parameters.get('repair_pending_factor_quotes', False)}`\n"
        f"checkpoint: `{result.get('checkpoint_id')}`\n"
        f"范围: `{parameters.get('start_date')}` 至 `{parameters.get('end_date')}`\n"
        f"市场: `{','.join(parameters.get('exchanges') or [])}`\n"
        f"scopes: `{','.join(parameters.get('scopes') or [])}`\n\n"
        "阶段:\n```text\n"
        + "\n".join(lines)
        + "\n```"
        + extras
    )


def _format_a_share_corporate_action_validation_report(
    result: Dict[str, Any],
) -> str:
    """Build a bounded report for layered corporate-action validation."""
    icon, label = _format_scheduler_status(result.get('status'))
    parameters = result.get('parameters') or {}
    universe = result.get('universe') or {}
    coverage = result.get('source_coverage') or {}
    event_totals = (result.get('event_validation') or {}).get('totals') or {}
    official_totals = (result.get('official_validation') or {}).get('totals') or {}
    cumulative_totals = (result.get('cumulative_validation') or {}).get('totals') or {}
    lines = [
        f"{icon} *A 股公司行动多源验证*",
        "",
        f"结论: *{label}*",
        f"状态: `{result.get('status', 'unknown')}`",
        f"只读: `{result.get('read_only', True)}`",
        f"范围: `{parameters.get('start_date', 'N/A')}` 至 "
        f"`{parameters.get('end_date', 'N/A')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        f"股票: `{universe.get('instrument_count', 0)}`",
        "",
        "事件字段:",
        "`"
        + ", ".join(
            f"{key}={event_totals.get(key, 0)}"
            for key in (
                'tdx_comparable_events', 'eastmoney_implemented_events',
                'exact_event_field_matches', 'shifted_event_field_matches',
                'event_field_conflicts', 'tdx_event_only',
                'eastmoney_event_only', 'unsupported_rights_only_tdx_events',
            )
        )
        + "`",
        "",
        "累计因子:",
        "`"
        + ", ".join(
            f"{key}={cumulative_totals.get(key, 0)}"
            for key in (
                'instrument_source_paths_compared', 'latest_acceptable',
                'latest_warning', 'latest_conflict',
                'historical_conflict_anchors', 'reference_paths_unavailable',
                'latest_error_p95_pct',
                'latest_error_max_pct',
            )
        )
        + "`",
        "",
        "官方公告证据:",
        "`"
        + ", ".join(
            f"{key}={official_totals.get(key, 0)}"
            for key in (
                'events_checked', 'official_announcement_evidence_found',
                'official_announcement_evidence_not_found',
            )
        )
        + "`",
        "",
        "源覆盖:",
        f"`periods_requested={len(coverage.get('periods_requested') or [])}, "
        f"periods_succeeded={coverage.get('periods_succeeded', 0)}, "
        f"failed_periods={len(coverage.get('failed_periods') or [])}`",
    ]
    reasons = [str(item) for item in (result.get('reasons') or [])[:10]]
    if reasons:
        lines.extend(["", "待跟踪:", *[f"- `{item}`" for item in reasons]])
    samples = []
    for key in ('field_conflict_samples', 'tdx_only_samples', 'eastmoney_only_samples'):
        samples.extend((result.get('event_validation') or {}).get(key) or [])
    samples.extend(
        (result.get('cumulative_validation') or {}).get('anchor_conflict_samples') or []
    )
    samples.extend(
        (result.get('cumulative_validation') or {}).get('unavailable_samples') or []
    )
    if samples:
        lines.extend(["", "样本:"])
        for item in samples[:10]:
            event_date = (
                item.get('tdx_ex_date')
                or item.get('ex_date')
                or item.get('anchor_date')
            )
            lines.append(
                f"`{item.get('instrument_id', 'unknown')} {event_date}: "
                f"{item.get('reason') or item.get('classification') or 'conflict'}`"
            )
    return "\n".join(lines)


def _format_a_share_cninfo_corporate_action_report(
    result: Dict[str, Any],
) -> str:
    """Build a concise report for official CNInfo corporate-action backfill."""
    normalized_status = str(result.get("status") or "unknown").lower()
    if normalized_status == "dry_run":
        icon, label = "ℹ️", "预演完成"
    else:
        icon, label = _format_scheduler_status(normalized_status)
    parameters = result.get("parameters") or {}
    universe = result.get("universe") or {}
    counters = result.get("counters") or {}
    lines = [
        f"{icon} *A 股巨潮官方公司行动回补*",
        "",
        f"结论: *{label}*",
        f"状态: `{result.get('status')}`",
        f"dry_run: `{result.get('dry_run')}`",
        f"checkpoint: `{result.get('checkpoint_id')}`",
        f"范围: `{parameters.get('start_date')}` 至 `{parameters.get('end_date')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        f"scopes: `{','.join(parameters.get('scopes') or [])}`",
        "",
        "规划:",
        "`"
        f"instrument_count={universe.get('instrument_count', 0)}, "
        f"completed_count={universe.get('completed_count', 0)}, "
        f"pending_count={universe.get('pending_count', 0)}"
        "`",
    ]
    if normalized_status == "dry_run":
        lines.extend([
            "外部请求: `0`",
            "数据库写入: `0`",
            "生产因子影响: `无`",
        ])
        return "\n".join(lines)
    lines.extend([
        "",
        "处理:",
        "`"
        + ", ".join(
            f"{key}={counters.get(key, 0)}"
            for key in (
                "requested_instruments",
                "requested_endpoints",
                "observations_inserted",
                "observations_changed",
                "observations_unchanged",
                "observations_reactivated",
                "observations_retired",
            )
        )
        + "`",
        "覆盖:",
        "`"
        + ", ".join(
            f"{key}={counters.get(key, 0)}"
            for key in (
                "complete_with_events",
                "complete_no_events",
                "partial_missing_fields",
                "indeterminate",
                "missing_ex_date_events",
                "ignored_placeholders",
            )
        )
        + "`",
        f"需公告补证: `{result.get('announcement_recovery_required', 0)}`",
        f"生产因子影响: `{'无' if result.get('production_isolation', True) else '有'}`",
    ])
    error_lines = [
        f"{item.get('instrument_id')} {item.get('source_profile')}: "
        f"{item.get('reason')}"
        for item in (result.get("errors") or [])[:10]
        if isinstance(item, dict)
    ]
    if error_lines:
        lines.extend(["", "异常样本:", "```text", *error_lines, "```"])
    return "\n".join(lines)


def _format_canonical_corporate_action_history_report(
    result: Dict[str, Any],
) -> str:
    """Keep the Telegram summary bounded while the full result stays authoritative."""
    blocker_reasons = result.get("blocker_reasons") or {}
    blocker_summary = ", ".join(
        f"{name}={count}"
        for name, count in sorted(
            blocker_reasons.items(), key=lambda item: (-int(item[1]), str(item[0]))
        )[:8]
    ) or "none"
    return "\n".join(
        [
            "*Canonical corporate-action history projection*",
            f"status: `{result.get('status')}`; dry_run: `{result.get('dry_run')}`",
            f"selected/considered: `{result.get('selected', 0)}/{result.get('considered', 0)}`",
            f"ready/blocked: `{result.get('ready', 0)}/{result.get('blocked', 0)}`",
            f"inserted/unchanged/would_change: `{result.get('inserted', 0)}/{result.get('unchanged', 0)}/{result.get('would_change', 0)}`",
            f"batches: `{result.get('completed_batches', 0)}/{result.get('total_batches', 0)}`; failed: `{len(result.get('failed_batches') or [])}`",
            f"blocker reasons: `{blocker_summary}`",
            f"checkpoint: `{result.get('checkpoint_id')}`",
            f"database/watermark: `{result.get('database_id')}` / `{result.get('watermark')}`",
            "provider/network requests: `0`",
        ]
    )


def _format_cninfo_special_action_discovery_report(
    result: Dict[str, Any],
) -> str:
    """Build a bounded report for candidate-only effective-date discovery."""
    status = str(result.get("status") or "unknown").lower()
    if status == "dry_run":
        icon, label = "ℹ️", "预演完成"
    else:
        icon, label = _format_scheduler_status(status)
    parameters = result.get("parameters") or {}
    targets = result.get("targets") or {}
    evidence = result.get("evidence") or {}
    title_classification = result.get("title_classification") or {}
    governance = result.get("announcement_governance") or {}
    lines = [
        f"{icon} *A 股巨潮特殊公司行动公告发现*",
        "",
        f"结论: *{label}*",
        f"状态: `{result.get('status')}`",
        f"dry_run: `{result.get('dry_run')}`",
        f"范围: `{parameters.get('start_date')}` 至 `{parameters.get('end_date')}`",
        f"扫描市场: `{','.join(parameters.get('scanned_exchanges') or [])}`",
        f"排除市场: `{','.join(parameters.get('excluded_exchanges') or [])}`",
        "",
        "目标:",
        "`"
        f"loaded={targets.get('candidate_rows_loaded', 0)}, "
        f"searchable={targets.get('searchable_events', 0)}, "
        f"batch={targets.get('batch_events', 0)}, "
        f"offset={targets.get('target_offset', 0)}, "
        f"with_candidates={targets.get('events_with_candidates', 0)}, "
        f"without_candidates={targets.get('events_without_candidates', 0)}, "
        f"unbounded_skipped={targets.get('skipped_without_bounded_anchor', 0)}"
        "`",
        "标题语义分类: `"
        f"enabled={title_classification.get('enabled', False)}, "
        f"status={title_classification.get('status')}, "
        f"titles={title_classification.get('input_title_count', 0)}, "
        f"requests={title_classification.get('request_count', 0)}, "
        f"concurrency={title_classification.get('peak_concurrency', 0)}/"
        f"{title_classification.get('max_concurrency', 0)}, "
        f"event_errors={title_classification.get('event_errors', 0)}`",
        "标题失败批次拆分重试: "
        f"requests={title_classification.get('isolated_retry_request_count', 0)}, "
        f"events={title_classification.get('isolated_retry_event_count', 0)}",
        "公告证据: `"
        f"candidate={evidence.get('candidate_count', 0)}, "
        f"rejected={evidence.get('rejected_count', 0)}`",
        "公告治理: `"
        f"run_id={governance.get('ingestion_run_id')}, "
        f"scans={governance.get('scan_states_persisted', 0)}, "
        f"audits={governance.get('audits_persisted', 0)}, "
        f"errors={governance.get('errors', 0)}`",
        f"下一批 offset: `{targets.get('next_target_offset')}`",
        "已确认有效日期: `0（本任务不从标题推断日期）`",
        f"生产因子影响: `{'无' if result.get('production_isolation', True) else '有'}`",
    ]
    errors = result.get("errors") or []
    if errors:
        lines.append(f"异常明细: `{len(errors)} 条，另行分消息发送`")
    return "\n".join(lines)


def _format_cninfo_problem_detail_messages(
    result: Dict[str, Any],
    *,
    title: str,
    items_per_message: int = 12,
) -> List[str]:
    """Build bounded Telegram messages only for actionable task problems."""
    problem_lines: List[str] = []
    seen = set()

    def add_problem(stage: str, item: Any) -> None:
        if isinstance(item, dict):
            instrument_id = str(item.get("instrument_id") or "-")
            event_key = str(item.get("source_event_key") or "-")[:12]
            code = str(item.get("code") or "")
            error = str(
                item.get("error") or item.get("reason") or "unknown_error"
            )
        else:
            instrument_id = "-"
            event_key = "-"
            code = ""
            error = str(item)
        identity = (stage, instrument_id, event_key, code, error)
        if identity in seen:
            return
        seen.add(identity)
        prefix = f"{stage} {instrument_id} {event_key}"
        if code:
            prefix += f" {code}"
        problem_lines.append(f"{prefix}: {error}"[:260])

    for item in result.get("errors") or []:
        add_problem("task", item)
    for stage_name, stage_result in (result.get("stages") or {}).items():
        if not isinstance(stage_result, dict):
            continue
        for item in stage_result.get("errors") or []:
            add_problem(str(stage_name), item)
        for sample in stage_result.get("target_samples") or []:
            if not isinstance(sample, dict):
                continue
            if sample.get("title_classification_status") == "failed":
                add_problem(
                    str(stage_name),
                    {
                        **sample,
                        "error": "title_classification_failed",
                    },
                )
            for error in sample.get("errors") or []:
                add_problem(
                    str(stage_name),
                    {**sample, "error": error},
                )
    for sample in result.get("target_samples") or []:
        if not isinstance(sample, dict):
            continue
        if sample.get("title_classification_status") == "failed":
            add_problem(
                "discovery",
                {**sample, "error": "title_classification_failed"},
            )
        for error in sample.get("errors") or []:
            add_problem("discovery", {**sample, "error": error})

    chunk_size = max(1, int(items_per_message))
    chunks = [
        problem_lines[offset: offset + chunk_size]
        for offset in range(0, len(problem_lines), chunk_size)
    ]
    return [
        "\n".join([
            f"*{title} ({index}/{len(chunks)})*",
            "```text",
            *chunk,
            "```",
        ])
        for index, chunk in enumerate(chunks, start=1)
    ]


def _format_llm_rpm(pipeline: Dict[str, Any]) -> str:
    configured = pipeline.get("llm_requests_per_minute")
    effective = pipeline.get("effective_llm_requests_per_minute")
    if effective is not None:
        if effective == 0:
            return "unlimited"
        if configured in (None, 0, "0"):
            return f"{effective} (inherited)"
        return str(effective)
    if configured in (None, 0, "0"):
        return "inherit(provider)"
    return str(configured)


def _format_cninfo_corporate_action_llm_report(result: Dict[str, Any]) -> str:
    """Build a bounded governed-resolution report for公告正文解析."""
    counts = result.get("counts") or {}
    targets = result.get("targets") or {}
    review_workload = result.get("review_workload") or {}
    tiers = review_workload.get("tiers") or {}
    signatures = review_workload.get("gate_signatures") or {}
    reason_codes = review_workload.get("reason_codes") or {}
    metrics = result.get("llm_metrics") or {}
    auto_promotion = result.get("auto_promotion") or {}
    pipeline = (result.get("parameters") or {}).get("pipeline") or {}
    latency = metrics.get("latency_ms") or {}
    top_signatures = sorted(
        signatures.items(), key=lambda item: (-int(item[1]), str(item[0]))
    )[:5]
    top_reason_codes = sorted(
        reason_codes.items(), key=lambda item: (-int(item[1]), str(item[0]))
    )[:8]
    lines = [
        "ℹ️ *A 股巨潮公司行动公告正文解析*",
        "",
        f"结论: *{'部分完成' if result.get('status') == 'partial' else '预演完成' if result.get('dry_run') else '完成'}*",
        f"状态: `{result.get('status')}`",
        f"dry_run: `{result.get('dry_run')}`",
        "pipeline: `"
        f"mode={pipeline.get('mode', 'serial')}, "
        f"llm={pipeline.get('llm_concurrency', 0)}, "
        f"llm_rpm={_format_llm_rpm(pipeline)}, "
        f"download={pipeline.get('download_concurrency', 0)}, "
        f"parse={pipeline.get('document_parse_concurrency', 0)}, "
        f"writer={pipeline.get('writer_concurrency', 0)}`",
        f"候选事件: `{targets.get('candidate_events', 0)}`，本批: `{targets.get('batch_events', 0)}`",
        f"处理/分析: `{counts.get('processed', 0)}/{counts.get('analyzed', 0)}`",
        f"通过证据门禁: `{counts.get('validated_candidates', 0)}`",
        "自动晋级: `"
        f"enabled={auto_promotion.get('enabled', False)}, "
        f"eligible={auto_promotion.get('eligible', 0)}, "
        f"promoted={auto_promotion.get('promoted', 0)}, "
        f"dry_run_eligible={auto_promotion.get('dry_run_eligible', 0)}, "
        f"skipped={auto_promotion.get('skipped', 0)}, "
        f"failed={auto_promotion.get('failed', 0)}`",
        f"机器返工: `{tiers.get('machine_rework', 0)}`",
        f"快速审核: `{tiers.get('quick_review', 0)}`，深度审核: `{tiers.get('deep_review', 0)}`",
        f"剩余人工审核: `{review_workload.get('remaining_manual_review', 0)}`",
        f"旧口径 manual_required: `{counts.get('manual_required', 0)}`",
        "审核原因码: `"
        + ", ".join(f"{name}={count}" for name, count in top_reason_codes)
        + "`",
        f"Token: `input={metrics.get('input_tokens', 0)}, output={metrics.get('output_tokens', 0)}, total={metrics.get('total_tokens', 0)}`",
        f"输出预算超限: `{metrics.get('provider_output_budget_overruns', 0)}`",
        f"延迟 ms: `p50={latency.get('p50')}, p95={latency.get('p95')}, max={latency.get('max')}`",
        f"LLM 未启用: `{counts.get('llm_disabled', 0)}`",
        f"文档失败: `{counts.get('document_failures', 0)}`，分析失败: `{counts.get('errors', 0)}`",
        f"下一批 offset: `{targets.get('next_target_offset')}`",
        "",
        "说明: 高置信结果可写入受治理的 resolved 层；原始 CNInfo 记录和生产复权因子保持隔离。",
    ]
    reason_counts = auto_promotion.get("reason_counts") or {}
    top_auto_reasons = sorted(
        reason_counts.items(), key=lambda item: (-int(item[1]), str(item[0]))
    )[:5]
    if top_auto_reasons:
        lines.extend([
            "",
            "自动晋级结果:",
            "```text",
            *(f"{reason}: {count}" for reason, count in top_auto_reasons),
            "```",
        ])
    if top_signatures:
        lines.extend([
            "",
            "主要 gate 签名:",
            "```text",
            *(f"{signature}: {count}" for signature, count in top_signatures),
            "```",
        ])
    errors = result.get("errors") or []
    if errors:
        lines.extend(["", "异常样本:", "```text"])
        lines.extend(
            f"{item.get('source_event_key', 'unknown')}: "
            f"{item.get('code', 'unknown')} attempts={item.get('attempt_count')} "
            f"{item.get('error', '')}"
            for item in errors[:10] if isinstance(item, dict)
        )
        lines.append("```")
    return "\n".join(lines)


def _format_cninfo_resolution_governance_report(result: Dict[str, Any]) -> str:
    """Build a bounded full-market unresolved-event governance report."""
    inventory = result.get("inventory") or {}
    targets = result.get("targets") or {}
    parameters = result.get("parameters") or {}
    pipeline = parameters.get("pipeline") or {}
    stages = result.get("stages") or {}
    discovery = stages.get("discovery") or {}
    asymmetric_review = stages.get("asymmetric_review") or {}
    tdx_asymmetric_review = stages.get("tdx_asymmetric_review") or {}
    title_classification = discovery.get("title_classification") or {}
    state_counts = inventory.get("state_counts") or {}
    next_actions = inventory.get("next_action_counts") or {}
    lines = [
        "ℹ️ *A 股 CNInfo 公司行动日期闭环治理*",
        "",
        f"结论: *{'预演完成' if result.get('dry_run') else '部分完成' if result.get('status') == 'partial' else '完成'}*",
        f"状态: `{result.get('status')}`",
        f"dry_run: `{result.get('dry_run')}`",
        f"范围: `{parameters.get('start_date')}` 至 `{parameters.get('end_date')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        f"scopes: `{','.join(parameters.get('scopes') or [])}`",
        "pipeline: `"
        f"mode={pipeline.get('mode', 'serial')}, "
        f"llm={pipeline.get('llm_concurrency', 0)}, "
        f"llm_rpm={_format_llm_rpm(pipeline)}, "
        f"parse={pipeline.get('document_parse_concurrency', 0)}, "
        f"writer={pipeline.get('writer_concurrency', 0)}`",
        "库存: `"
        f"total={inventory.get('total_events', 0)}, "
        f"actionable={inventory.get('actionable_events', 0)}, "
        f"terminal={inventory.get('terminal_events', 0)}, "
        f"factor_blocking={inventory.get('factor_blocking_events', 0)}, "
        f"source_unsupported={inventory.get('source_unsupported_events', 0)}`",
        "本批: `"
        f"eligible={targets.get('eligible_events', 0)}, "
        f"processable={targets.get('processable_events', 0)}, "
        f"batch={targets.get('batch_events', 0)}, "
        f"has_more={targets.get('has_more', False)}, "
        f"next_offset={targets.get('next_target_offset')}`",
        f"状态写入: `{result.get('state_write') or {}}`",
        f"阶段异常: `{','.join(result.get('stage_failures') or []) or '无'}`",
        "标题语义分类: `"
        f"enabled={parameters.get('classify_titles_with_llm', False)}, "
        f"status={title_classification.get('status', '未运行')}, "
        f"titles={title_classification.get('input_title_count', 0)}, "
        f"requests={title_classification.get('request_count', 0)}, "
        f"concurrency={title_classification.get('peak_concurrency', 0)}/"
        f"{title_classification.get('max_concurrency', 0)}, "
        f"event_errors={title_classification.get('event_errors', 0)}`",
        "标题失败批次拆分重试: "
        f"requests={title_classification.get('isolated_retry_request_count', 0)}, "
        f"events={title_classification.get('isolated_retry_event_count', 0)}",
        "说明: 原始 CNInfo 事件不修改；北交所不进入 CNInfo 公告解析。",
    ]
    if asymmetric_review:
        lines.extend([
            "非对称旁路: `"
            f"scanned={asymmetric_review.get('scanned', 0)}, "
            f"eligible={asymmetric_review.get('eligible', 0)}, "
            f"promoted={asymmetric_review.get('promoted', 0)}, "
            f"updated={asymmetric_review.get('updated', 0)}, "
            f"unchanged={asymmetric_review.get('unchanged', 0)}, "
            f"skipped={asymmetric_review.get('skipped', 0)}, "
            f"blocked={asymmetric_review.get('blocked', 0)}, "
            f"failed={asymmetric_review.get('failed', 0)}`",
            "非对称隔离: `"
            f"network_access={asymmetric_review.get('network_access')}, "
            f"llm_invocations={asymmetric_review.get('llm_invocations', 0)}`",
            "非对称分页: `"
            f"batch={targets.get('asymmetric_batch_events', 0)}, "
            f"has_more={targets.get('asymmetric_has_more', False)}, "
            "next_offset="
            f"{targets.get('asymmetric_next_target_offset')}`",
        ])
        blocked_reasons = (
            asymmetric_review.get("blocked_reason_counts") or {}
        )
        if blocked_reasons:
            lines.extend([
                "非对称阻塞原因:",
                "```text",
                *(
                    f"{key}: {value}"
                    for key, value in sorted(blocked_reasons.items())
                ),
                "```",
            ])
    if tdx_asymmetric_review:
        lines.extend([
            "TDX非对称对账: `"
            f"scanned={tdx_asymmetric_review.get('scanned', 0)}, "
            f"special={tdx_asymmetric_review.get('special_events', 0)}, "
            f"matched={tdx_asymmetric_review.get('eligible', 0)}, "
            f"promoted={tdx_asymmetric_review.get('promoted', 0)}, "
            f"skipped={tdx_asymmetric_review.get('skipped', 0)}, "
            f"blocked={tdx_asymmetric_review.get('blocked', 0)}, "
            f"failed={tdx_asymmetric_review.get('failed', 0)}`",
            "TDX非对称隔离: `"
            f"network_access={tdx_asymmetric_review.get('network_access')}, "
            f"llm_invocations={tdx_asymmetric_review.get('llm_invocations', 0)}`",
        ])
        mismatch_reasons = (
            tdx_asymmetric_review.get("mismatch_reason_counts") or {}
        )
        if mismatch_reasons:
            lines.extend([
                "TDX非对称不一致原因:",
                "```text",
                *(
                    f"{key}: {value}"
                    for key, value in sorted(mismatch_reasons.items())
                ),
                "```",
            ])
    if state_counts:
        lines.extend([
            "",
            "状态分布:",
            "```text",
            *(f"{key}: {value}" for key, value in sorted(state_counts.items())),
            "```",
        ])
    if next_actions:
        lines.extend([
            "下一步分布:",
            "```text",
            *(f"{key}: {value}" for key, value in sorted(next_actions.items())),
            "```",
        ])
    problem_count = len(_format_cninfo_problem_detail_messages(
        result,
        title="CNInfo 公司行动异常明细",
        items_per_message=1,
    ))
    if problem_count:
        lines.append(f"异常明细: `{problem_count} 条，另行分消息发送`")
    return "\n".join(lines)


def _format_cninfo_resolution_reset_report(result: Dict[str, Any]) -> str:
    """Build a compact report for the destructive-development reset task."""
    parameters = result.get("parameters") or {}
    events = result.get("events") or {}
    deleted = result.get("deleted") or {}
    return "\n".join([
        "ℹ️ *A 股 CNInfo 公司行动治理数据重置*",
        "",
        f"结论: *{'预演完成' if result.get('dry_run') else '完成' if result.get('status') == 'success' else '部分完成'}*",
        f"状态: `{result.get('status')}`",
        f"dry_run: `{result.get('dry_run')}`",
        f"confirm_reset: `{result.get('confirmed')}`",
        f"include_unanchored: `{parameters.get('include_unanchored', False)}`",
        f"范围: `{parameters.get('start_date')}` 至 `{parameters.get('end_date')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        "事件: `"
        f"selected={events.get('selected', 0)}, "
        f"protected_resolved={events.get('protected_resolved', 0)}, "
        f"reset={events.get('reset', 0)}`",
        f"派生数据: `{deleted}`",
        "说明: 原始 CNInfo 事件、TDX 数据及已 resolved 事件血缘不修改。",
    ])


def _format_a_share_factor_rebuild_report(result: Dict[str, Any]) -> str:
    """Build a bounded adjustment-factor governance report."""
    icon, label = _format_scheduler_status(result.get("status"))
    is_dry_run = bool(result.get("dry_run"))
    if result.get("status") == "dry_run":
        icon, label = "ℹ️", "预演完成"
    parameters = result.get("parameters") or {}
    universe = result.get("universe") or {}
    observations = result.get("observations") or {}
    canonical = result.get("canonical") or {}
    reconciliation = canonical.get("event_reconciliation") or {}
    tdx_price = canonical.get("tdx_adjusted_price_comparison") or {}
    legacy_price = canonical.get("legacy_adjusted_price_comparison") or {}
    gates = canonical.get("quality_gates") or {}
    lines = [
        f"{icon} *A 股复权因子重建与治理*",
        "",
        f"结论: *{label}*",
        f"状态: `{result.get('status', 'unknown')}`",
        f"dry_run: `{result.get('dry_run', True)}`",
        f"checkpoint: `{result.get('checkpoint_id', 'N/A')}`",
        f"范围: `{parameters.get('start_date', 'N/A')}` 至 `{parameters.get('end_date', 'N/A')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        f"来源: `{parameters.get('source', 'akshare')}`",
        f"staging版本: `{result.get('staging_series_version', 'N/A')}`",
        f"生产版本: `{result.get('target_series_version', 'N/A')}`",
        "",
        "规划:",
        "`"
        + ", ".join(
            f"{key}={universe.get(key, 0)}"
            for key in (
                "instrument_count", "completed_count", "pending_count",
            )
        )
        + "`",
    ]
    if is_dry_run:
        lines.extend([
            "已有证据: `"
            f"rows={observations.get('existing_rows', 0)}, "
            f"instruments={observations.get('existing_instruments', 0)}`",
            "外部请求: `0`",
            "数据库写入: `0`",
        ])
    else:
        lines.extend([
            "处理: `"
            + ", ".join(
                f"{key}={observations.get(key, 0)}"
                for key in (
                    "requested_instruments", "completed_instruments",
                    "empty_instruments", "observation_inserted",
                    "observation_changed", "errors",
                )
            )
            + "`",
            "",
            "标准序列: `"
            + ", ".join(
                f"{key}={canonical.get(key, 0)}"
                for key in (
                    "row_count", "built_instruments", "coverage_ratio",
                    "conflict_count", "conflict_ratio", "saved_rows",
                )
            )
            + "`",
            "事件核对: `"
            + ", ".join(
                f"{key}={reconciliation.get(key, 0)}"
                for key in (
                    "exact_matches", "shifted_matches", "factor_conflicts",
                    "candidate_only", "tdx_only",
                )
            )
            + "`",
            "路径误差: `"
            f"tdx_max={tdx_price.get('max_adjusted_price_error_pct')}, "
            f"legacy_max={legacy_price.get('max_adjusted_price_error_pct')}`",
            "质量门禁: `"
            + ", ".join(f"{key}={value}" for key, value in gates.items())
            + "`",
            f"可切换生产: `{canonical.get('promotion_eligible', False)}`",
            f"已晋级生产: `{canonical.get('promoted', False)}`",
        ])
    samples = canonical.get("samples") or []
    if samples:
        lines.extend([
            "",
            "异常样本:",
            "```text",
            *[
                f"{item.get('instrument_id')} "
                f"{item.get('ex_date') or item.get('date') or item.get('candidate_ex_date')}: "
                f"{item.get('reason') or 'adjusted_price_path_difference'}"
                for item in samples[:10]
            ],
            "```",
        ])
    return "\n".join(lines)


def _format_cninfo_primary_factor_report(result: Dict[str, Any]) -> str:
    """Build a bounded report for the isolated multi-source factor workflow."""
    status = result.get("status", "unknown")
    label = {"success": "完成", "partial": "部分完成", "dry_run": "预演完成"}.get(
        status, status
    )
    is_daily = result.get("operation") == "a_share_cninfo_primary_daily_maintenance"
    parameters = result.get("parameters") or {}
    factor_result = (result.get("factor_rebuild") or {}) if is_daily else result
    reconciliation = factor_result.get("reconciliation") or {}
    totals = reconciliation.get("totals") or {}
    matching_policy = reconciliation.get("matching_policy") or {}
    rounded_policy = matching_policy.get("rounded_precision_policy") or {}
    candidate = factor_result.get("candidate") or {}
    benchmark = factor_result.get("benchmark") or {}
    reference_sources = benchmark.get("reference_sources") or {}
    discovery = result.get("candidate_discovery") or {}
    cninfo_refresh = result.get("cninfo_refresh") or {}
    bse_official = result.get("bse_official_refresh") or {}
    bse_scan = bse_official.get("scan") or {}
    cninfo_counters = cninfo_refresh.get("counters") or {}
    endpoint_metrics = cninfo_refresh.get("endpoint_metrics") or {}
    endpoint_targets = endpoint_metrics.get("target_counts") or {}
    endpoint_requests = endpoint_metrics.get("request_counts") or {}
    throttle_metrics = cninfo_refresh.get("adaptive_throttle") or {}
    tdx_refresh = result.get("tdx_refresh") or {}
    tdx_totals = tdx_refresh.get("totals") or {}
    tdx_scope = tdx_refresh.get("target_scope") or {}
    affected = result.get("affected_instruments") or {}
    readiness = result.get("data_readiness") or {}
    execution_status = result.get("execution_status") or {}
    stage_durations = result.get("stage_durations") or {}
    anomaly = result.get("anomaly_governance") or {}
    anomaly_llm = anomaly.get("llm") or {}
    anomaly_counts = anomaly_llm.get("counts") or {}
    anomaly_promotion = anomaly_llm.get("auto_promotion") or {}
    anomaly_review = anomaly_llm.get("review_workload") or {}
    announcement_scan = discovery.get("announcement_scan") or {}
    carryover_revalidation = (
        announcement_scan.get("carryover_revalidation") or {}
    )
    canonical_maintenance = result.get("canonical_maintenance") or {}
    canonical_predecessor = canonical_maintenance.get("predecessor") or {}
    factor_retry_state = result.get("factor_retry_state") or {}
    anomaly_reason_counts = anomaly.get("reason_counts") or {}
    anomaly_reason_summary = ",".join(
        f"{reason}:{count}"
        for reason, count in sorted(anomaly_reason_counts.items())
    ) or "none"
    lines = [
        "ℹ️ *A 股公司行动与复权因子多源基准*",
        "",
        f"结论: *{label}*",
        f"状态: `{status}`",
        "生产表影响: `"
        + (
            "promoted canonical 定向更新"
            if canonical_maintenance.get("merge")
            else "无"
        )
        + "`",
        f"范围: `{parameters.get('start_date', 'N/A')}` 至 `{parameters.get('end_date', 'N/A')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        f"CNInfo事件: `{(factor_result.get('source_events') or {}).get('cninfo_rows', 0)}`",
        f"TDX事件: `{(factor_result.get('source_events') or {}).get('tdx_rows', 0)}`",
        f"CNInfo因子: `{(factor_result.get('cninfo_path') or {}).get('derived_events', 0)}`",
        f"TDX因子: `{(factor_result.get('tdx_path') or {}).get('derived_events', 0)}`",
        "事件对账: `"
        + ", ".join(
            f"{key}={totals.get(key, 0)}"
            for key in (
                "exact_matches", "rounded_matches", "shifted_matches", "conflicts",
                "cninfo_only", "tdx_only",
            )
        )
        + "`",
        "舍入匹配: `"
        f"policy={rounded_policy.get('version', 'N/A')}, "
        "factor_relative_tolerance="
        f"{float(matching_policy.get('factor_relative_tolerance', 0.0)) * 100:.6f}%"
        "`",
        f"基准版本: `{benchmark.get('benchmark_series_version', 'N/A')}`",
        f"主源选择: `{benchmark.get('source_selection_status', 'deferred')}`",
        "来源覆盖: `"
        + ", ".join(
            f"{source}={details.get('available_instruments', 0)}"
            for source, details in sorted(reference_sources.items())
        )
        + "`",
    ]
    if candidate.get("candidate_built"):
        lines.extend([
            f"候选版本: `{candidate.get('staging_series_version', 'N/A')}`",
            f"候选行数: `{candidate.get('row_count', 0)}`",
            f"可晋级生产: `{candidate.get('promotion_eligible', False)}`",
        ])
    else:
        lines.append(
            "生产因子候选: `未构造（build_canonical=false）`"
            if is_daily
            else "候选构造: `未执行（需 build_canonical=true）`"
        )
    if is_daily:
        lines[0] = "ℹ️ *A 股公司行动增量日更*"
        lines.insert(1, "模式: `公告/事件候选刷新 + 受影响标的因子重建`")
        lines.insert(
            7,
            "CNInfo市场: `"
            + ",".join(parameters.get("cninfo_exchanges") or [])
            + "`；排除: `"
            + ",".join(parameters.get("cninfo_excluded_exchanges") or [])
            + "` (`source_not_supported`)",
        )
        incremental_lines = [
            "候选发现: `"
            f"status={discovery.get('status', 'N/A')}, "
            f"selected={discovery.get('candidate_count', 0)}, "
            f"deferred={discovery.get('deferred_count', 0)}, "
            f"announcements={(discovery.get('announcement_scan') or {}).get('announcements_seen', 0)}, "
            "relevant="
            f"{((discovery.get('announcement_scan') or {}).get('title_filter') or {}).get('selected_records', 0)}"
            "`",
            "CNInfo刷新: `"
            f"requested={cninfo_counters.get('requested_instruments', 0)}, "
            f"inserted={cninfo_counters.get('observations_inserted', 0)}, "
            f"changed={cninfo_counters.get('observations_changed', 0)}, "
            f"unchanged={cninfo_counters.get('observations_unchanged', 0)}, "
            f"retired={cninfo_counters.get('observations_retired', 0)}, "
            f"errors={len(cninfo_refresh.get('errors') or [])}"
            "`",
            "CNInfo端点: `"
            f"dividend targets={endpoint_targets.get('cninfo_dividend', 0)} "
            f"requests={endpoint_requests.get('cninfo_dividend', 0)}; "
            f"allotment targets={endpoint_targets.get('cninfo_allotment', 0)} "
            f"requests={endpoint_requests.get('cninfo_allotment', 0)}; "
            f"final_retry={endpoint_metrics.get('final_retry_targets', 0)}/"
            f"recovered={endpoint_metrics.get('final_retry_recovered', 0)}"
            "`",
            "CNInfo限流: `"
            f"403={throttle_metrics.get('http_403_count', 0)}, "
            f"429={throttle_metrics.get('http_429_count', 0)}, "
            f"wait={float(throttle_metrics.get('adaptive_wait_seconds', 0) or 0):.1f}s, "
            f"cooldowns={throttle_metrics.get('short_cooldown_count', 0)}, "
            f"circuits={throttle_metrics.get('circuit_trip_count', 0)}, "
            f"circuit_wait={float(throttle_metrics.get('circuit_wait_seconds', 0) or 0):.1f}s"
            "`",
            "BSE官方近期证据: `"
            f"status={bse_official.get('status', 'N/A')}, "
            f"scope={bse_official.get('coverage_scope', 'recent_window_only')}, "
            f"window={bse_official.get('requested_start_date', 'N/A')}.."
            f"{bse_official.get('requested_end_date', 'N/A')}, "
            f"pages={bse_scan.get('pages_scanned', 0)}, "
            f"announcements={bse_official.get('matched_announcement_count', 0)}, "
            f"events={bse_official.get('parsed_event_count', 0)}, "
            f"partial={bse_official.get('parse_partial_count', 0)}, "
            f"full_history={bse_official.get('full_history_complete', False)}"
            "`",
            "TDX刷新: `"
            f"mode={tdx_refresh.get('refresh_mode', 'N/A')}, "
            f"targets={tdx_scope.get('instrument_count', 0)}, "
            f"rotation={tdx_scope.get('rotating_sample_count', 0)}, "
            f"processed={tdx_totals.get('processed_instruments', 0)}, "
            f"events={tdx_totals.get('raw_events', 0)}, "
            f"errors={tdx_totals.get('errors', 0)}, "
            f"timeouts={tdx_totals.get('timeouts', 0)}"
            "`",
            "执行状态: `"
            f"cninfo_primary={execution_status.get('primary', status)}, "
            f"bse_official={execution_status.get('bse_official', 'N/A')}, "
            f"tdx_reference={execution_status.get('tdx_reference', 'N/A')}, "
            f"reconciliation={execution_status.get('reconciliation', 'N/A')}, "
            f"canonical={execution_status.get('canonical', 'inactive')}"
            "`",
            "Canonical日更: `"
            f"status={canonical_maintenance.get('status', 'inactive')}, "
            "series="
            f"{canonical_maintenance.get('active_series_version') or 'N/A'}, "
            f"scope={canonical_maintenance.get('scope_instrument_count', 0)}, "
            "missing_coverage="
            f"{canonical_maintenance.get('missing_coverage_count', 0)}, "
            "merge_eligible="
            f"{canonical_maintenance.get('incremental_merge_eligible', False)}, "
            "merged_rows="
            f"{(canonical_maintenance.get('merge') or {}).get('canonical_rows', 0)}"
            "`",
            "Canonical阻塞: `"
            f"reason={canonical_maintenance.get('blocker_reason') or 'none'}, "
            "workflow_deferred="
            f"{canonical_maintenance.get('workflow_deferred', False)}, "
            "actionable_retry="
            f"{canonical_maintenance.get('actionable_retry_count', 0)}"
            "`",
            "因子重试队列: `"
            f"status={factor_retry_state.get('status', 'N/A')}, "
            "actionable="
            f"{factor_retry_state.get('actionable_retry_count', 0)}"
            "`",
            "Canonical前序: `"
            f"reason={canonical_predecessor.get('reason', 'N/A')}, "
            f"required={canonical_predecessor.get('required_through', 'N/A')}, "
            "cutoffs="
            f"{canonical_predecessor.get('successful_through_by_exchange', {})}"
            "`",
            "受影响标的: `"
            f"total={affected.get('count', 0)}, "
            f"cninfo={affected.get('cninfo_count', 0)}, "
            f"tdx={affected.get('tdx_count', 0)}"
            "`",
            "CNInfo就绪度: `"
            f"status={(readiness.get('cninfo') or {}).get('status', readiness.get('status', 'not_evaluated'))}, "
            f"pending_factors={(readiness.get('cninfo') or {}).get('pending_factor_events', 0)}, "
            f"incomplete_instruments={(readiness.get('cninfo') or {}).get('incomplete_instruments', 0)}"
            "`",
            "TDX参考路径: `"
            f"status={(readiness.get('tdx_reference') or {}).get('status', 'not_evaluated')}, "
            f"pending_factors={(readiness.get('tdx_reference') or {}).get('pending_factor_events', 0)}, "
            f"incomplete_instruments={(readiness.get('tdx_reference') or {}).get('incomplete_instruments', 0)}"
            "`",
            "跨源对账: `"
            f"status={(readiness.get('reconciliation') or {}).get('status', 'not_evaluated')}, "
            f"incomplete_instruments={(readiness.get('reconciliation') or {}).get('incomplete_instruments', 0)}"
            "`",
            "窗口: `"
            f"announcements={parameters.get('announcement_start_date', 'N/A')}.."
            f"{parameters.get('announcement_run_at', 'N/A')}, "
            f"factor_end={(result.get('factor_cutoff') or {}).get('resolved_end_date', parameters.get('end_date', 'N/A'))}"
            "`",
            "异常语义治理: `"
            f"execution={anomaly.get('execution_status', 'N/A')}, "
            f"readiness={anomaly.get('readiness_status', 'N/A')}, "
            f"candidates={anomaly.get('candidate_event_count', 0)}, "
            f"selected={anomaly.get('selected_event_count', 0)}, "
            f"deferred={anomaly.get('deferred_event_count', 0)}, "
            "unmatched="
            f"{anomaly.get('unmatched_special_announcement_count', 0)}, "
            f"reasons={anomaly_reason_summary}"
            "`",
            "异常 LLM: `"
            f"processed={anomaly_counts.get('processed', 0)}, "
            f"analyzed={anomaly_counts.get('analyzed', 0)}, "
            f"promoted={anomaly_promotion.get('promoted', 0)}, "
            f"manual={anomaly_review.get('remaining_manual_review', 0)}, "
            f"errors={anomaly_counts.get('errors', 0)}, "
            f"document_failures={anomaly_counts.get('document_failures', 0)}"
            "`",
            "阶段耗时: `"
            f"discovery={float(stage_durations.get('candidate_discovery_seconds', 0) or 0):.1f}s, "
            f"cninfo={float(stage_durations.get('cninfo_refresh_seconds', 0) or 0):.1f}s, "
            f"bse={float(stage_durations.get('bse_official_refresh_seconds', 0) or 0):.1f}s, "
            f"tdx={float(stage_durations.get('tdx_refresh_seconds', 0) or 0):.1f}s, "
            f"factors={float(stage_durations.get('factor_rebuild_seconds', 0) or 0):.1f}s, "
            "canonical="
            f"{float(stage_durations.get('canonical_maintenance_seconds', 0) or 0):.1f}s, "
            f"llm={float(stage_durations.get('anomaly_llm_seconds', 0) or 0):.1f}s, "
            f"total={float(stage_durations.get('total_seconds', 0) or 0):.1f}s"
            "`",
        ]
        if factor_result.get("status") == "skipped":
            incremental_lines.append("因子重建: `无需执行（本轮无受影响标的）`")
        if int(carryover_revalidation.get("evaluated", 0) or 0):
            incremental_lines.append(
                "公告待办重验: `"
                f"policy={carryover_revalidation.get('policy_version', 'N/A')}, "
                f"evaluated={carryover_revalidation.get('evaluated', 0)}, "
                f"excluded={carryover_revalidation.get('excluded', 0)}, "
                "rerouted_structured="
                f"{carryover_revalidation.get('rerouted_structured', 0)}, "
                "retained_exceptional="
                f"{carryover_revalidation.get('retained_exceptional', 0)}, "
                "retained_missing_title="
                f"{carryover_revalidation.get('retained_missing_title', 0)}"
                "`"
            )
        unmatched_samples = []
        for instrument_id, items in sorted(
            (
                anomaly.get(
                    "deferred_special_announcements_by_instrument"
                ) or {}
            ).items()
        ):
            for item in items or ():
                title = str(item.get("title") or "").strip()
                if title:
                    unmatched_samples.append(
                        f"{instrument_id}:{title[:80]}"
                    )
                if len(unmatched_samples) >= 5:
                    break
            if len(unmatched_samples) >= 5:
                break
        if unmatched_samples:
            incremental_lines.append(
                "待处理公告样本: `" + "；".join(unmatched_samples) + "`"
            )
        lines[8:8] = incremental_lines
    return "\n".join(lines)


def _format_a_share_canonical_factor_selection_report(
    result: Dict[str, Any],
) -> str:
    """Build a bounded report for the manual three-source selection workflow."""
    def _source_label(value: Any) -> str:
        normalized = str(value or "")
        return normalized.replace(
            "baostock_sina_composite",
            "BaoStock_Sina composite",
        )

    status = str(result.get("status") or "unknown").lower()
    label = {
        "success": "完成",
        "partial": "部分完成",
        "dry_run": "预演完成",
        "failed": "失败",
    }.get(status, status)
    parameters = result.get("parameters") or {}
    selection = result.get("selection") or {}
    source_events = selection.get("source_events") or {}
    source_selection = selection.get("source_selection") or {}
    candidate = selection.get("candidate") or {}
    pairwise = candidate.get("pairwise_reconciliation") or {}
    lines = [
        "ℹ️ *A 股三源主复权因子候选选择*",
        "",
        f"结论: *{label}*",
        f"状态: `{status}`",
        "生产表影响: `无（仅独立观察层与 versioned staging）`",
        f"范围: `{parameters.get('start_date', 'N/A')}` 至 "
        f"`{parameters.get('end_date', 'N/A')}`",
        f"市场: `{','.join(parameters.get('exchanges') or [])}`",
        f"定向标的: `{len(parameters.get('instrument_ids') or [])}`",
        "数据获取: `local_only`",
        "来源覆盖: `"
        f"cninfo_events={source_events.get('cninfo_rows', 0)}, "
        f"tdx_events={source_events.get('tdx_rows', 0)}, "
        "BaoStock_Sina composite events="
        f"{source_events.get('baostock_sina_factor_rows', 0)}, "
        "instruments="
        f"{source_events.get('baostock_sina_instruments', 0)}`",
        "路径选择: `"
        + ", ".join(
            f"{_source_label(source)}={count}"
            for source, count in sorted(
                (source_selection.get("selection_counts") or {}).items()
            )
        )
        + "`",
        "置信度: `"
        + ", ".join(
            f"{confidence}={count}"
            for confidence, count in sorted(
                (source_selection.get("confidence_counts") or {}).items()
            )
        )
        + "`",
        "一致模式: `"
        + ", ".join(
            f"{pattern}={count}"
            for pattern, count in sorted(
                (source_selection.get("agreement_counts") or {}).items()
            )
        )
        + "`",
        f"候选版本: `{candidate.get('staging_series_version', 'N/A')}`",
        f"候选行数: `{candidate.get('row_count', 0)}`",
        f"阻塞区间: `{candidate.get('blocked_segment_count', 0)}`",
        f"低置信区间: `{candidate.get('low_confidence_segment_count', 0)}`",
        "历史单源兜底: `"
        f"{candidate.get('historical_single_source_segment_count', 0)}`",
        f"可供独立晋级审核: `{candidate.get('promotion_eligible', False)}`",
        "自动晋级生产: `False`",
    ]
    if pairwise:
        lines.append(
            "事件对账: `"
            + "; ".join(
                (
                    f"{_source_label(name)}: "
                    f"exact={values.get('exact_matches', 0)}, "
                    f"shifted={values.get('shifted_matches', 0)}, "
                    f"conflicts={values.get('conflicts', 0)}, "
                    f"left_only={values.get('left_only', 0)}, "
                    f"right_only={values.get('right_only', 0)}"
                )
                for name, values in sorted(pairwise.items())
            )
            + "`"
        )
        factor_buckets = {
            label: sum(
                int(
                    values.get("factor_difference_buckets", {}).get(
                        label, 0
                    )
                )
                for values in pairwise.values()
            )
            for label in (
                "le_0_01_pct",
                "0_01_to_0_1_pct",
                "0_1_to_0_5_pct",
                "0_5_to_1_pct",
                "gt_1_pct",
            )
        }
        lines.append(
            "因子差异分桶: `"
            + ", ".join(
                f"{label}={count}"
                for label, count in factor_buckets.items()
            )
            + "`"
        )
    blocked_decisions = candidate.get("blocked_decisions") or []
    if blocked_decisions:
        lines.extend(["", "硬阻塞明细:", "```text"])
        for item in blocked_decisions[:20]:
            lines.append(
                f"{item.get('instrument_id', 'unknown')} "
                f"{item.get('start_date', '?')}..{item.get('end_date', '?')}: "
                f"{item.get('reason', 'blocked')}"
            )
        lines.append("```")
    override_samples = (
        candidate.get("reviewed_source_override_samples") or []
    )
    if override_samples:
        lines.extend(["", "人工全生命周期来源覆盖:", "```text"])
        for item in override_samples[:20]:
            evidence = item.get("reviewed_source_override") or {}
            lines.append(
                f"{item.get('instrument_id', 'unknown')} "
                f"{item.get('start_date', '?')}..{item.get('end_date', '?')}: "
                f"{_source_label(item.get('selected_source'))}; "
                f"{evidence.get('reason', item.get('reason', 'reviewed'))}; "
                f"catalog={evidence.get('catalog_version', 'unknown')}"
            )
        lines.append("```")
    samples = [
        item
        for item in (candidate.get("conflict_samples") or [])
        if item.get("confidence") != "blocked"
    ]
    if samples:
        lines.extend(["", "低置信与历史单源样本:", "```text"])
        for item in samples[:20]:
            lines.append(
                f"{item.get('instrument_id', 'unknown')} "
                f"{item.get('start_date', '?')}..{item.get('end_date', '?')}: "
                f"{item.get('reason', 'conflict')}"
            )
        lines.append("```")
    return "\n".join(lines)


def _format_a_share_canonical_factor_promotion_report(
    result: Dict[str, Any],
) -> str:
    """Build a bounded promotion, activation, or rollback report."""

    status = str(result.get("status") or "unknown").lower()
    label = {
        "success": "完成",
        "partial": "部分完成",
        "dry_run": "预演完成",
        "blocked": "阻塞",
        "failed": "失败",
    }.get(status, status)
    parameters = result.get("parameters") or {}
    preflight = result.get("preflight") or {}
    freshness = preflight.get("freshness") or {}
    promotion = result.get("promotion") or {}
    activation = result.get("activation") or {}
    lines = [
        "ℹ️ *A 股主复权因子发布与回滚*",
        "",
        f"结论: *{label}*",
        f"状态: `{status}`",
        f"操作: `{result.get('action', 'promote')}`",
        f"预演: `{bool(result.get('dry_run'))}`",
        f"人工确认: `{bool(result.get('confirmed'))}`",
        "staging: `"
        f"{parameters.get('staging_series_version') or 'N/A'}`",
        "稳定版本: `"
        f"{parameters.get('target_series_version') or 'N/A'}`",
        f"预检通过: `{bool(preflight.get('eligible'))}`",
        "持久化计数: `"
        f"rows={preflight.get('canonical_row_count', 0)}, "
        f"instruments={preflight.get('instrument_status_count', 0)}, "
        f"with_events={preflight.get('complete_with_events', 0)}, "
        f"no_events={preflight.get('complete_no_events', 0)}`",
    ]
    if freshness:
        lines.append(
            "最新交易日门禁: `"
            f"eligible={freshness.get('eligible', False)}, "
            f"candidate_end={freshness.get('candidate_end_date')}, "
            f"sessions={freshness.get('latest_completed_sessions')}`"
        )
    if promotion:
        lines.append(
            "原子晋级: `"
            f"rows={promotion.get('canonical_rows', 0)}, "
            f"instruments={promotion.get('instrument_statuses', 0)}`"
        )
    if activation:
        lines.append(
            "生产读取: `"
            f"dataset={activation.get('read_dataset')}, "
            "series="
            f"{activation.get('canonical_series_version') or 'N/A'}`"
        )
    errors = [
        *(preflight.get("errors") or []),
        *(result.get("errors") or []),
    ]
    if errors:
        lines.extend(["", "阻塞或错误:", "```text"])
        lines.extend(str(error) for error in errors[:20])
        lines.append("```")
    return "\n".join(lines)


def _format_a_share_canonical_storage_report(
    result: Dict[str, Any],
) -> str:
    """Build a bounded report for decision migration and retention."""

    icon, label = _format_scheduler_status(result.get("status"))
    counts = result.get("candidate_counts") or {}
    versions = result.get("versions") or []
    lines = [
        f"{icon} *A 股主复权因子存储维护*",
        "",
        f"结论: *{label}*",
        f"状态: `{result.get('status', 'unknown')}`",
        f"操作: `{result.get('maintenance_operation')}`",
        f"预演: `{bool(result.get('dry_run'))}`",
        f"人工确认: `{bool(result.get('confirmed'))}`",
        "活动版本: `"
        f"{result.get('active_series_version') or 'N/A'}`",
    ]
    if versions:
        lines.append(
            "决策迁移: `"
            f"versions={len(versions)}, "
            f"decisions={result.get('migrated_decisions', 0)}, "
            f"reports={result.get('compacted_reports', 0)}`"
        )
        lines.extend([
            "版本样本:",
            "```text",
            *[
                f"{item.get('series_version')}: "
                f"{item.get('status')} decisions="
                f"{item.get('report_decisions', 0)}"
                for item in versions[:20]
            ],
            "```",
        ])
    if counts:
        lines.append(
            "保留候选: `"
            + ", ".join(
                f"{key}={value}" for key, value in counts.items()
            )
            + "`"
        )
        candidates = result.get("candidate_versions") or []
        if candidates:
            lines.extend([
                "候选版本:",
                "```text",
                *[str(value) for value in candidates[:20]],
                "```",
            ])
    return "\n".join(lines)


def _format_instrument_master_governance_summary(governance: Optional[Dict[str, Any]]) -> str:
    """Return a compact operator summary for shared instrument master governance."""
    if not isinstance(governance, dict) or not governance:
        return ""

    status = governance.get("status", "unknown")
    action = governance.get("action") or governance.get("reason") or "unknown"
    lines = [f"状态: {status} ({action})"]

    summary = governance.get("summary")
    if isinstance(summary, dict):
        lines.append(
            "新增: "
            f"{summary.get('added_instruments', 0)}，"
            f"停用: {summary.get('deactivated_instruments', 0)}，"
            f"停牌: {summary.get('suspended_instruments', 0)}，"
            f"复活: {summary.get('reactivated_instruments', 0)}，"
            f"待复核: {summary.get('review_required', 0)}，"
            f"活跃合计: {summary.get('active_count', 0)}"
        )
        source_authority = summary.get("source_authority")
        if isinstance(source_authority, dict) and source_authority:
            lines.append(
                "权威来源: "
                + "；".join(
                    f"{authority}={count}"
                    for authority, count in list(source_authority.items())[:4]
                )
            )

    children = governance.get("children")
    if isinstance(children, list) and children:
        child_lines = []
        for child in children[:4]:
            if not isinstance(child, dict):
                continue
            child_summary = child.get("summary") or {}
            child_lines.append(
                f"{child.get('scope', 'unknown')}={child.get('status', 'unknown')}"
                f"/{child.get('action') or child.get('reason') or 'unknown'}"
                f"(active={child_summary.get('active_count', 0)})"
            )
        if child_lines:
            lines.append("策略: " + "；".join(child_lines))

    exchanges = governance.get("exchanges")
    if isinstance(exchanges, dict):
        a_share_lines = []
        for exchange in ("SSE", "SZSE", "BSE"):
            item = exchanges.get(exchange)
            if not isinstance(item, dict):
                continue
            source_diag = item.get("source_diagnostics") if isinstance(item.get("source_diagnostics"), dict) else {}
            source_authority = item.get("source_authority") or source_diag.get("selected_source_authority")
            if source_authority:
                a_share_lines.append(f"{exchange}:{source_authority}")
        if a_share_lines:
            lines.append("A股股票源: " + "；".join(a_share_lines))
    if isinstance(exchanges, dict) and "HKEX" in exchanges:
        hkex = exchanges.get("HKEX") or {}
        lines.append(
            "HKEX: "
            f"mode={hkex.get('mode', 'unknown')}，"
            f"official_active={hkex.get('official_active_count', 0)}，"
            f"official_delisted={hkex.get('official_delisted_count', 0)}，"
            f"supplemental={hkex.get('supplemental_count', 0)}，"
            f"safe_write候选={hkex.get('safe_write_preview_count', 0)}"
        )
        if hkex.get("allowed_reactivation_count") or hkex.get("allowed_suspension_count"):
            lines.append(
                "HKEX生命周期候选: "
                f"可复活={hkex.get('allowed_reactivation_count', 0)}，"
                f"可停牌={hkex.get('allowed_suspension_count', 0)}"
            )
        source_usage = hkex.get("source_usage")
        if isinstance(source_usage, dict) and source_usage:
            source_text = "，".join(
                f"{source}:{count}" for source, count in list(source_usage.items())[:4]
            )
            lines.append(f"HKEX源: {source_text}")
        quote_availability = hkex.get("quote_availability")
        if isinstance(quote_availability, dict):
            lines.append(
                "HKEX行情诊断: "
                f"无本地行情={quote_availability.get('no_local_quote_count', 0)}，"
                f"过旧={quote_availability.get('stale_local_quote_count', 0)}"
            )

    warnings = governance.get("warnings") or []
    errors = governance.get("errors") or []
    if warnings:
        lines.append("警告: " + "；".join(str(item) for item in warnings[:3]))
    if errors:
        lines.append("错误: " + "；".join(str(item) for item in errors[:3]))
    return "\n".join(lines)


def _attach_instrument_master_governance_report(
    report_data: Dict[str, Any],
    result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Attach shared master-governance diagnostics to scheduler reports."""
    if not isinstance(result, dict):
        return report_data
    governance = result.get("instrument_master_governance")
    if not isinstance(governance, dict):
        return report_data
    report_data["instrument_master_governance"] = governance
    report_data["instrument_master_governance_summary"] = (
        _format_instrument_master_governance_summary(governance)
    )
    return report_data


def _format_repair_universe_summary(diagnostics: Optional[Dict[str, Any]]) -> str:
    """Return a compact report summary for repair-universe lifecycle filtering."""
    if not isinstance(diagnostics, dict) or not diagnostics:
        return ""

    reasons = diagnostics.get("reason_distribution") or {}
    reason_text = "，".join(
        f"{reason}={count}" for reason, count in list(reasons.items())[:5]
    ) or "无"
    clip_reasons = diagnostics.get("clip_reason_distribution") or {}
    clip_reason_text = "，".join(
        f"{reason}={count}" for reason, count in list(clip_reasons.items())[:5]
    )
    samples = diagnostics.get("samples") or []
    sample_text = "；".join(
        str(sample.get("instrument_id") or sample.get("symbol") or "unknown")
        + f"({sample.get('reason', 'unknown')})"
        for sample in samples[:5]
        if isinstance(sample, dict)
    )
    lines = [
        f"模式: {diagnostics.get('mode', 'unknown')}",
        (
            "标的: "
            f"输入={diagnostics.get('input_instrument_count', 0)}，"
            f"可修复={diagnostics.get('eligible_instrument_count', 0)}，"
            f"裁剪={diagnostics.get('clipped_instrument_count', 0)}，"
            f"生命周期跳过={diagnostics.get('skipped_instrument_count', 0)}"
        ),
        (
            "缺口跳过: "
            f"segments={diagnostics.get('skipped_gap_segment_count', 0)}，"
            f"missing_days={diagnostics.get('skipped_missing_days', 0)}"
        ),
        f"原因: {reason_text}",
    ]
    if clip_reason_text:
        lines.append(f"裁剪原因: {clip_reason_text}")
    if diagnostics.get("degraded_fallback_count"):
        lines.append(f"降级兜底: {diagnostics.get('degraded_fallback_count')}")
    current_refresh = diagnostics.get("current_master_refresh")
    if isinstance(current_refresh, dict) and current_refresh.get("requested"):
        lines.append(
            "显式主数据刷新: "
            f"{current_refresh.get('status', 'unknown')} "
            f"{current_refresh.get('scopes', [])}"
        )
    if sample_text:
        lines.append(f"样例: {sample_text}")
    warnings = diagnostics.get("warnings") or []
    if warnings:
        lines.append("警告: " + "；".join(str(item) for item in warnings[:3]))
    return "\n".join(lines)


def _format_seconds_for_report(value: Any) -> str:
    """Format elapsed seconds for compact operator reports."""
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{sec:02d}s"


def _format_valuation_input_scheduler_report(
    result: Dict[str, Any],
    *,
    title: str,
) -> str:
    """Build a detailed Telegram report for valuation input sync/backfill."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    exchanges = [
        item for item in (result.get("exchanges") or [])
        if isinstance(item, dict)
    ]
    attempted = int(result.get("attempted_exchanges", len(exchanges)) or 0)
    successful = int(result.get("successful_exchanges", 0) or 0)
    total_rows = int(result.get("total_snapshots_written", 0) or 0)
    total_requested = int(
        result.get("total_requested_instruments")
        or sum(int(item.get("requested_instruments", 0) or 0) for item in exchanges)
    )
    total_covered = int(
        result.get("total_covered_instruments")
        or sum(int(item.get("covered_instruments", 0) or 0) for item in exchanges)
    )
    total_missing = int(
        result.get("total_missing_instruments")
        or sum(int(item.get("missing_instruments", 0) or 0) for item in exchanges)
    )
    elapsed_seconds = result.get("elapsed_seconds")
    if elapsed_seconds is None:
        elapsed_seconds = sum(
            float(item.get("elapsed_seconds", 0) or 0) for item in exchanges
        ) or None

    start_date = result.get("start_date") or "未指定"
    end_date = result.get("end_date") or "latest"
    lines = [
        f"🔧 *{title}*",
        f"{icon} 状态: {label} ({status})",
        "",
        "*执行摘要*",
        f"• source: {result.get('source', 'unknown')} / {result.get('source_mode', 'unknown')}",
        f"• sync_mode: {result.get('sync_mode', 'unknown')}",
        f"• 日期范围: {start_date} ~ {end_date}",
        f"• 交易所: {successful}/{attempted}",
        f"• 请求标的: {total_requested}",
        f"• 覆盖标的: {total_covered}",
        f"• 缺失标的: {total_missing}",
        f"• 写入/更新: {total_rows}",
        f"• 耗时: {_format_seconds_for_report(elapsed_seconds)}",
    ]
    existing_covered_total = sum(
        int(item.get("existing_covered_instruments", 0) or 0)
        for item in exchanges
    )
    if existing_covered_total:
        lines.append(f"• 沿用既有输入: {existing_covered_total}")

    if exchanges:
        lines.extend(["", "*分交易所*"])
        for item in exchanges:
            ex_status = item.get("status", "unknown")
            ex_icon, ex_label = _format_scheduler_status(ex_status)
            existing_covered = int(item.get("existing_covered_instruments", 0) or 0)
            reuse_text = f", reused={existing_covered}" if existing_covered else ""
            lines.append(
                f"• {item.get('exchange', 'unknown')}: {ex_icon} {ex_label}, "
                f"rows={item.get('snapshots_written', 0)}, "
                f"requested={item.get('requested_instruments', 0)}, "
                f"covered={item.get('covered_instruments', 0)}, "
                f"missing={item.get('missing_instruments', 0)}{reuse_text}, "
                f"elapsed={_format_seconds_for_report(item.get('elapsed_seconds'))}"
            )
            missing_ids = item.get("missing_instrument_ids") or []
            if missing_ids:
                lines.append(
                    "  缺失样例: " + ", ".join(str(x) for x in missing_ids[:10])
                )

    governance_summary = _format_instrument_master_governance_summary(
        result.get("instrument_master_governance")
    )
    if governance_summary:
        lines.extend(["", "*证券主数据治理*", governance_summary])

    return "\n".join(lines)


def _format_valuation_history_scheduler_report(
    result: Dict[str, Any],
    *,
    title: str,
) -> str:
    """Build a detailed Telegram report for valuation history rebuild jobs."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    exchanges = [
        item for item in (result.get("exchanges") or [])
        if isinstance(item, dict)
    ]
    attempted = int(result.get("attempted_exchanges", len(exchanges)) or 0)
    successful = int(result.get("successful_exchanges", 0) or 0)
    total_rows = int(
        result.get("total_rows_written")
        or sum(int(item.get("rows_written", 0) or 0) for item in exchanges)
    )
    total_existing = int(
        result.get("total_existing_rows_skipped")
        or sum(int(item.get("existing_rows_skipped", 0) or 0) for item in exchanges)
    )
    total_processed = int(
        result.get("total_instruments_processed")
        or sum(int(item.get("instruments_processed", 0) or 0) for item in exchanges)
    )
    total_skipped = sum(int(item.get("skipped_instruments", 0) or 0) for item in exchanges)
    missing_financials = sum(len(item.get("missing_financials") or []) for item in exchanges)
    missing_inputs = sum(len(item.get("missing_valuation_inputs") or []) for item in exchanges)

    window_mode = result.get("window_mode") or "trading_days"
    if window_mode == "last_12_quarters":
        window_label = "过去12个季度"
    else:
        quote_limit_days = result.get("quote_limit_days")
        window_label = f"最近{quote_limit_days}个交易日" if quote_limit_days else "配置交易日窗口"

    lines = [
        f"🔧 *{title}*",
        f"{icon} 状态: {label} ({status})",
        "",
        "*执行摘要*",
        f"• 窗口: {window_label} ({window_mode})",
        f"• 写入策略: {result.get('write_policy', 'unknown')}",
        f"• 交易所: {successful}/{attempted}",
        f"• 处理标的: {total_processed}",
        f"• 跳过标的: {total_skipped}",
        f"• 写入/更新: {total_rows}",
        f"• 已存在跳过: {total_existing}",
        f"• 缺财务数据: {missing_financials}",
        f"• 缺估值输入: {missing_inputs}",
        f"• 耗时: {_format_seconds_for_report(result.get('elapsed_seconds'))}",
    ]

    if exchanges:
        lines.extend(["", "*分交易所*"])
        for item in exchanges:
            ex_status = item.get("status", "unknown")
            ex_icon, ex_label = _format_scheduler_status(ex_status)
            lines.append(
                f"• {item.get('exchange', 'unknown')}: {ex_icon} {ex_label}, "
                f"processed={item.get('instruments_processed', 0)}, "
                f"skipped={item.get('skipped_instruments', 0)}, "
                f"rows={item.get('rows_written', 0)}, "
                f"existing={item.get('existing_rows_skipped', 0)}, "
                f"missing_fin={len(item.get('missing_financials') or [])}, "
                f"missing_input={len(item.get('missing_valuation_inputs') or [])}"
            )
            missing_samples = (item.get("missing_financials") or [])[:5]
            if missing_samples:
                lines.append("  缺财务样例: " + ", ".join(str(x) for x in missing_samples))

    governance_summary = _format_instrument_master_governance_summary(
        result.get("instrument_master_governance")
    )
    if governance_summary:
        lines.extend(["", "*证券主数据治理*", governance_summary])

    return "\n".join(lines)


def _format_shareholder_shadow_scheduler_report(
    result: Dict[str, Any],
    readiness: Optional[Dict[str, Any]] = None,
) -> str:
    """Build a clear operator-facing report for shareholder shadow sync."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    exchanges = result.get("exchanges") or []
    successful_exchanges = int(result.get("successful_exchanges", 0) or 0)
    attempted_exchanges = int(result.get("attempted_exchanges", len(exchanges)) or 0)
    total_written = int(result.get("total_snapshots_written", 0) or 0)
    write_policy = str(result.get("write_policy") or "refresh_all")
    unchanged_total = sum(
        int(exchange.get("unchanged_instruments", 0) or 0)
        for exchange in exchanges
        if isinstance(exchange, dict)
    )
    missing_total = sum(
        int(exchange.get("missing_instruments", 0) or 0)
        for exchange in exchanges
        if isinstance(exchange, dict)
    )

    if status == "success" and missing_total == 0:
        conclusion = "成功 - 本次同步已覆盖全部目标标的，股东户数、前十大股东、实控线索均满足要求。"
    elif status in {"success", "degraded"}:
        conclusion = f"{label} - 本次仍有 {missing_total} 个标的未满足 required scope。"
    else:
        conclusion = f"{label} - 股东摘要同步失败或未完成。"

    lines = [
        f"{icon} *股东摘要影子同步*",
        "",
        "任务: `shareholder_shadow_sync`",
        f"结论: *{conclusion}*",
        "",
        f"状态: `{status}`",
        f"写入策略: `{write_policy}`",
        f"交易所: {successful_exchanges}/{attempted_exchanges} 成功",
        f"本次写入/刷新快照: {total_written}",
        f"本次无需改写快照: {unchanged_total}",
        f"本次未补齐标的: {missing_total}",
    ]

    if isinstance(readiness, dict):
        ready = bool(readiness.get("ready_for_paid_high_availability_rollout"))
        blockers = readiness.get("blockers") or []
        target_count = int(readiness.get("target_instrument_count", 0) or 0)
        snapshot_total = int(readiness.get("snapshot_total", 0) or 0)
        missing_snapshot_count = int(readiness.get("missing_snapshot_count", 0) or 0)
        scope_counts = readiness.get("scope_counts") or {}
        lines.extend(
            [
                "",
                "当前覆盖:",
                f"readiness: {'ready' if ready else 'not_ready'}",
                f"目标标的: {target_count}",
                f"当前快照: {snapshot_total}",
                f"缺失快照: {missing_snapshot_count}",
                (
                    "required_scope: "
                    f"holder_count={scope_counts.get('holder_count', 0)}, "
                    f"top10={scope_counts.get('top10_holders', 0)}, "
                    f"ownership_clues={scope_counts.get('reference_only_ownership_clues', 0)}"
                ),
            ]
        )
        if blockers:
            lines.append("blockers: " + "；".join(str(item) for item in blockers[:5]))

    governance_summary = _format_instrument_master_governance_summary(
        result.get("instrument_master_governance")
    )
    if governance_summary:
        lines.extend(["", "证券主数据:", governance_summary])

    lines.extend(["", "交易所明细:"])
    for exchange_result in exchanges:
        if not isinstance(exchange_result, dict):
            continue
        exchange = exchange_result.get("exchange", "unknown")
        exchange_status = exchange_result.get("status", "unknown")
        requested = int(exchange_result.get("requested_instruments", 0) or 0)
        resolved = int(exchange_result.get("resolved_instruments", 0) or 0)
        written = int(exchange_result.get("snapshots_written", 0) or 0)
        unchanged = int(exchange_result.get("unchanged_instruments", 0) or 0)
        missing = int(exchange_result.get("missing_instruments", 0) or 0)
        attempted_sources = exchange_result.get("attempted_sources") or []
        successful_sources = exchange_result.get("successful_sources") or []
        source_text = (
            ", ".join(str(item) for item in successful_sources)
            or exchange_result.get("source")
            or "N/A"
        )
        lines.append(
            f"• {exchange}: {exchange_status}，覆盖 {resolved}/{requested}，写入 {written}，未变 {unchanged}，缺口 {missing}"
        )
        lines.append(f"  来源: {source_text}")
        if attempted_sources:
            lines.append(
                "  尝试: " + ", ".join(str(item) for item in attempted_sources)
            )
        missing_ids = exchange_result.get("missing_instrument_ids") or []
        if missing_ids:
            lines.append(
                "  未补齐样例: " + ", ".join(str(item) for item in missing_ids[:5])
            )

    return "\n".join(lines)


def _format_financial_l1_import_scheduler_report(result: Dict[str, Any]) -> str:
    """Build compact Telegram content for Financial L1 full import."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    review_batches = result.get("review_batches") or []
    failed_batches = result.get("failed_batches") or []
    lines = [
        f"结论: {icon} *{label}*",
        f"status: `{status}`",
        f"数据库: `{result.get('db_path', 'unknown')}`",
        f"日志目录: `{result.get('log_dir', 'unknown')}`",
        f"manifest: `{result.get('manifest_path', 'unknown')}`",
        f"progress: `{result.get('progress_path', 'unknown')}`",
        f"报告期: `{', '.join(result.get('report_periods') or [])}`",
        (
            "批次: "
            f"{result.get('completed_batch_count', 0)}/"
            f"{result.get('selected_batch_count', result.get('batch_count', 0))}"
        ),
        f"目标标的: `{result.get('target_count', 0)}`",
        f"需复核批次: `{len(review_batches)}`，失败批次: `{len(failed_batches)}`",
        f"耗时: `{result.get('elapsed_seconds', 0)}s`",
    ]
    lifecycle = result.get("report_period_lifecycle_summary")
    if isinstance(lifecycle, dict):
        lines.append(
            "生命周期排除: "
            f"上市前 {lifecycle.get('pre_listing', 0)}，"
            f"退市后 {lifecycle.get('post_delisting', 0)}，"
            f"公告解释 {lifecycle.get('disclosure_events', 0)}"
        )
    if review_batches:
        lines.append("后续动作: 查看 review batch 明细，属于公告/生命周期解释的继续记录，字段缺失 blocker 单独补处理。")
    return "\n".join(lines)


def _format_financial_disclosure_scheduler_report(result: Dict[str, Any]) -> str:
    """Build compact Telegram content for financial disclosure maintenance."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    lines = [
        f"结论: {icon} *{label}*",
        f"status: `{status}`",
        f"数据库: `{result.get('db_path', 'unknown')}`",
        f"模式: `{'reconciliation' if result.get('reconciliation') else 'incremental'}`",
        f"报告期: `{', '.join(result.get('report_periods') or [])}`",
        f"公告扫描: {result.get('announcements_scanned', 0)}，命中 {result.get('selected_announcements', 0)}，页数 {result.get('pages_scanned', 0)}",
        (
            "公告过滤: "
            f"财报相关 {result.get('financial_like_announcements', 0)}，"
            f"过滤噪声 {result.get('filtered_financial_like_announcements', 0)}，"
            f"命中未成事件 {result.get('selected_without_event_count', 0)}"
        ),
        f"候选: `{result.get('candidate_count', 0)}`",
        (
            "处理: "
            f"写入/修复 {result.get('changed_count', 0)}，"
            f"跳过未变 {result.get('unchanged_count', 0)}，"
            f"pending recheck {result.get('pending_recheck_count', 0)}，"
            f"待退市风险 {result.get('pending_delisting_risk_count', 0)}，"
            f"过期pending {result.get('expired_pending_count', 0)}"
        ),
        (
            "质量状态: "
            f"accepted gaps {result.get('accepted_gap_count', 0)}，"
            f"mapping policy gaps {result.get('mapping_policy_gap_count', 0)}，"
            f"source missing {result.get('source_missing_gap_count', 0)}，"
            f"blockers {result.get('blocking_gap_count', 0)}，"
            f"failed {result.get('failed_count', 0)}"
        ),
        f"耗时: `{result.get('elapsed_seconds', 0)}s`",
    ]
    unlimited_candidates = int(result.get("candidate_unlimited_count", 0) or 0)
    candidate_limit = int(result.get("candidate_limit", 0) or 0)
    if candidate_limit > 0 and unlimited_candidates > result.get("candidate_count", 0):
        lines.append(
            "候选限制: "
            f"本轮选择 {result.get('candidate_count', 0)}/"
            f"{unlimited_candidates}，按交易所/profile/报告期均衡抽样"
        )
    source_routing = result.get("source_routing") or {}
    candidate_sources = result.get("candidate_sources") or {}
    if candidate_sources:
        lines.append(
            "候选来源: "
            f"新公告 {candidate_sources.get('new_event', 0)}，"
            f"历史pending {candidate_sources.get('pending_state', 0)}，"
            f"历史accepted {candidate_sources.get('accepted_state', 0)}，"
            f"本地缺口 {candidate_sources.get('local_gap', 0)}，"
            f"旧噪声过滤 {candidate_sources.get('filtered_stale_pending', 0)}，"
            f"过期pending {candidate_sources.get('expired_pending', 0)}"
        )
    if source_routing:
        lines.append(
            "补数源: "
            f"CNInfo尝试 {source_routing.get('cninfo_attempts', 0)}，"
            f"ready {source_routing.get('cninfo_successes', 0)}，"
            f"批处理通过 {source_routing.get('cninfo_batch_successes', 0)}，"
            f"缺失/歧义 {source_routing.get('cninfo_missing_or_ambiguous', 0)}；"
            f"Sina/THS fallback尝试 {source_routing.get('fallback_attempts', 0)}，"
            f"成功 {source_routing.get('fallback_successes', 0)}"
        )
        final_source = str(source_routing.get("final_source") or "").lower()
        if not final_source:
            cninfo_successes = int(source_routing.get("cninfo_successes", 0) or 0)
            fallback_successes = int(source_routing.get("fallback_successes", 0) or 0)
            if cninfo_successes and fallback_successes:
                final_source = "mixed"
            elif cninfo_successes:
                final_source = "cninfo"
            elif fallback_successes:
                final_source = "fallback"
            else:
                final_source = "none"
        source_labels = {
            "cninfo": "CNInfo",
            "fallback": "fallback（Sina/THS，非 CNInfo）",
            "mixed": "CNInfo + fallback（Sina/THS）",
            "none": "未发生补数",
        }
        lines.append(f"数据来源: {source_labels.get(final_source, final_source)}")
        routing_errors = source_routing.get("errors") or []
        if routing_errors:
            lines.append(
                "补数源警告: " + "；".join(str(item) for item in routing_errors[:3])
            )
            if status == "success":
                lines.append(
                    "说明: 官方结构化源未完全就绪，但最终数据采集已完成；"
                    "本次结果以 fallback 数据为准，后续可对账官方数据。"
                )
            else:
                lines.append(
                    "说明: 官方结构化源降级，已保留 fallback 结果，后续对账需复核官方数据。"
                )
    scan_errors = result.get("scan_errors") or []
    if scan_errors:
        lines.append("扫描警告: " + "；".join(str(item) for item in scan_errors[:3]))
    if result.get("pending_delisting_risk_count", 0):
        lines.append("说明: 待退市风险只是披露异常待补状态，不会改写股票主数据退市状态。")
    if result.get("mapping_policy_gap_count", 0):
        lines.append("说明: mapping policy gap 是字段标准或映射准入问题，不会反复调用 CNInfo/THS/Sina 补数。")
    if result.get("blocking_gap_count", 0):
        blocker_samples = result.get("blocker_samples") or []
        if blocker_samples:
            rendered_samples = []
            for item in blocker_samples[:5]:
                missing = item.get("missing_fields") or []
                missing_text = ",".join(str(field) for field in missing[:3])
                suffix = f":{missing_text}" if missing_text else ""
                rendered_samples.append(
                    f"{item.get('instrument_id')}@{item.get('report_period')}{suffix}"
                )
            lines.append("blocker样本: " + "；".join(rendered_samples))
        lines.append("后续动作: blocker 按 source missing 或其他数据质量问题补处理，不能并入 accepted gaps。")
    vintage_stage = (result.get("backtest_stages") or {}).get(
        "financial_filing_vintages"
    )
    if isinstance(vintage_stage, dict):
        lines.append(
            "Filing vintage: "
            f"{vintage_stage.get('status', 'unknown')}，"
            f"watermark {vintage_stage.get('watermark')}，"
            f"blockers {len(vintage_stage.get('blockers') or [])}"
        )
    broker_post = result.get("broker_risk_control_post_task")
    if isinstance(broker_post, dict):
        broker_status = broker_post.get("status", "unknown")
        broker_icon, broker_label = _format_scheduler_status(broker_status)
        backfill = broker_post.get("backfill") or {}
        announcement_scan = broker_post.get("announcement_scan") or {}
        window = broker_post.get("date_window") or {}
        lines.extend(
            [
                "",
                "*券商风控后置任务*",
                f"{broker_icon} 状态: {broker_label} ({broker_status})",
                f"窗口: `{window.get('start_date', 'unknown')}` ~ `{window.get('end_date', 'unknown')}`",
                f"tier: `{broker_post.get('tier', 'unknown')}`，dry_run: `{broker_post.get('dry_run')}`",
                (
                    "处理: "
                    f"目标券商 {len(broker_post.get('target_instruments') or [])}，"
                    f"公告 {announcement_scan.get('selected_announcements', 0)}，"
                    f"解析报告 {backfill.get('reports_parsed', 0)}，"
                    f"facts {backfill.get('facts_parsed', 0)}，"
                    f"写入 {backfill.get('facts_written', 0)}"
                ),
            ]
        )
    return "\n".join(lines)


def _format_broker_risk_control_scheduler_report(result: Dict[str, Any]) -> str:
    """Build compact Telegram content for broker regulatory fact maintenance."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    backfill = result.get("backfill") or {}
    announcement_scan = result.get("announcement_scan") or {}
    window = result.get("date_window") or {}
    per_instrument = announcement_scan.get("per_instrument_scan") or {}
    lines = [
        f"结论: {icon} *{label}*",
        f"status: `{status}`",
        f"模式: `{result.get('mode', 'incremental_update')}`",
        f"窗口: `{window.get('start_date', 'unknown')}` ~ `{window.get('end_date', 'unknown')}`",
        f"tier: `{result.get('tier', 'unknown')}`，dry_run: `{result.get('dry_run')}`",
        f"交易所: `{', '.join(result.get('exchanges') or [])}`",
        f"目标券商: `{len(result.get('target_instruments') or [])}`",
        (
            "公告扫描: "
            f"选中 {announcement_scan.get('selected_announcements', 0)}，"
            f"逐公司尝试 {per_instrument.get('attempted_instruments', 0)}，"
            f"命中公司 {per_instrument.get('instruments_with_matches', 0)}"
        ),
        (
            "解析写入: "
            f"reports {backfill.get('reports_parsed', 0)}/"
            f"{backfill.get('reports_discovered', 0)}，"
            f"facts parsed {backfill.get('facts_parsed', 0)}，"
            f"facts written {backfill.get('facts_written', 0)}"
        ),
        (
            "异常: "
            f"parse_failures {backfill.get('parse_failures', 0)}，"
            f"retryable_pending {backfill.get('retryable_pending_reports', 0)}"
        ),
        f"耗时: `{_format_seconds_for_report(result.get('elapsed_seconds'))}`",
    ]
    governance_summary = _format_instrument_master_governance_summary(
        result.get("instrument_master_governance")
    )
    if governance_summary:
        lines.extend(["", "*证券主数据治理*", governance_summary])
    return "\n".join(lines)


def _format_shareholder_incremental_scheduler_report(
    result: Dict[str, Any],
    readiness: Optional[Dict[str, Any]] = None,
) -> str:
    """Build a clear operator-facing report for shareholder incremental sync."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    changed = int(result.get("changed_instruments", 0) or 0)
    unchanged = int(result.get("unchanged_instruments", 0) or 0)
    pending = int(result.get("pending_rechecks", 0) or 0)
    failed = int(result.get("failed_instruments", 0) or 0)
    candidates = int(result.get("candidate_instruments", 0) or 0)
    written = int(result.get("snapshots_written", 0) or 0)
    would_write = int(result.get("would_write_snapshots", 0) or 0)
    dry_run = bool(result.get("dry_run", False))

    if status == "success" and changed == 0 and failed == 0:
        conclusion = "成功 - 已完成公告驱动增量检查，本次没有发现需要写入的股东快照变化。"
    elif status in {"success", "degraded"}:
        write_text = f"预计写入 {would_write}" if dry_run else f"写入 {written}"
        conclusion = f"{label} - 本次检查候选 {candidates} 个，{write_text} 个，失败 {failed} 个。"
    else:
        conclusion = f"{label} - 股东增量检查失败或未完成。"

    lines = [
        f"{icon} *股东摘要每日增量检查*",
        "",
        "任务: `shareholder_incremental_sync`",
        f"结论: *{conclusion}*",
        "",
        f"状态: `{status}`",
        f"运行模式: {'dry_run' if dry_run else 'write'}",
        f"公告扫描: pages={result.get('pages_scanned', 0)}，records={result.get('announcements_scanned', 0)}，selected={result.get('selected_announcements', 0)}",
        f"候选标的: {candidates}",
        f"变化写入: {written}",
        f"未变化: {unchanged}",
        f"待复查: {pending}",
        f"失败: {failed}",
    ]
    if dry_run:
        lines.append(f"dry_run 预计写入: {would_write}")

    attempted_sources = result.get("attempted_sources") or []
    successful_sources = result.get("successful_sources") or []
    if attempted_sources or successful_sources:
        lines.extend(
            [
                "",
                "数据源:",
                "尝试: " + (", ".join(str(item) for item in attempted_sources) or "N/A"),
                "成功: " + (", ".join(str(item) for item in successful_sources) or "N/A"),
            ]
        )

    if isinstance(readiness, dict):
        ready = bool(readiness.get("ready_for_paid_high_availability_rollout"))
        blockers = readiness.get("blockers") or []
        lines.extend(
            [
                "",
                f"readiness: {'ready' if ready else 'not_ready'}",
                f"缺失快照: {int(readiness.get('missing_snapshot_count', 0) or 0)}",
            ]
        )
        if blockers:
            lines.append("blockers: " + "；".join(str(item) for item in blockers[:5]))

    governance_summary = _format_instrument_master_governance_summary(
        result.get("instrument_master_governance")
    )
    if governance_summary:
        lines.extend(["", "证券主数据:", governance_summary])

    failed_ids = result.get("failed_instrument_ids") or []
    if failed_ids:
        lines.extend(["", "失败样例: " + ", ".join(str(item) for item in failed_ids[:10])])

    scan_errors = result.get("scan_errors") or []
    if scan_errors:
        lines.extend(["", "公告扫描异常: " + "；".join(str(item) for item in scan_errors[:3])])

    return "\n".join(lines)


def _format_source_mode(result: Dict[str, Any]) -> str:
    source = result.get("source") or "N/A"
    mode = result.get("mode") or "N/A"
    return f"`{source}` / `{mode}`"


def _format_industry_standard_scheduler_report(result: Dict[str, Any]) -> str:
    """Build an operator-facing report for the scheduled Shenwan standard sync."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    memberships_written = int(result.get("total_memberships_written", 0) or 0)
    history_rows = int(result.get("classification_history_rows_written", 0) or 0)
    taxonomy_nodes = int(result.get("taxonomy_nodes_written", 0) or 0)
    official_rows = int(result.get("total_official_classifications_written", 0) or 0)
    successful_exchanges = int(result.get("successful_exchanges", 0) or 0)
    attempted_exchanges = int(result.get("attempted_exchanges", 0) or 0)
    source_files_unchanged = bool(result.get("source_files_unchanged"))
    reason = result.get("reason")

    if status == "success" and source_files_unchanged:
        conclusion = "官方分类文件未变化，本地 authoritative coverage 已满足，本次无需重写。"
    elif status == "success":
        conclusion = (
            f"同步成功，本次写入 {memberships_written} 条股票行业归属、"
            f"{history_rows} 条分类历史。"
        )
    elif status == "degraded":
        conclusion = reason or "同步部分完成，至少存在交易所覆盖不足或上游/本地校验异常。"
    elif status in {"skipped", "disabled", "unavailable"}:
        conclusion = reason or "任务未执行，请查看原因和交易所明细。"
    else:
        conclusion = reason or "任务失败，请查看日志和交易所明细。"

    exchange_lines = []
    for item in result.get("exchanges", []) or []:
        diagnostics = item.get("diagnostics") or {}
        existing = diagnostics.get("existing_authoritative_memberships")
        target = diagnostics.get("target_instruments")
        coverage = f", existing={existing}/{target}" if existing is not None and target is not None else ""
        unchanged = ", source_files=unchanged" if diagnostics.get("source_files_unchanged") else ""
        error = f", reason={item.get('error_message')}" if item.get("error_message") else ""
        exchange_lines.append(
            f"{item.get('exchange', 'UNKNOWN')}: {item.get('status', 'unknown')}, "
            f"memberships={item.get('memberships_written', 0)}, "
            f"official={item.get('official_classifications_written', 0)}"
            f"{coverage}{unchanged}{error}"
        )
    if not exchange_lines:
        exchange_lines.append(reason or "无交易所明细")

    return (
        f"{icon} *研究域申万官方分类文件每日同步*\n\n"
        f"任务: `industry_standard_sync`\n"
        f"结论: *{label}* - {conclusion}\n\n"
        f"状态: `{status}`\n"
        f"数据源: {_format_source_mode(result)}\n"
        f"source_files: `{'unchanged' if source_files_unchanged else 'updated/checked'}`\n"
        f"交易所: `{successful_exchanges}/{attempted_exchanges}` 成功\n"
        f"taxonomy_nodes: `{taxonomy_nodes}`\n"
        f"history_rows: `{history_rows}`\n"
        f"memberships_written: `{memberships_written}`\n"
        f"official_classifications: `{official_rows}`\n\n"
        "交易所明细:\n"
        "```text\n"
        + "\n".join(exchange_lines[:10])
        + "\n```"
    )


def _format_industry_index_analysis_scheduler_report(result: Dict[str, Any]) -> str:
    """Build an operator-facing report for the scheduled Shenwan index-analysis sync."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    rows_written = int(result.get("rows_written", 0) or 0)
    summary = result.get("summary") or {}
    coverage = result.get("coverage") or {}
    type_counts = summary.get("index_type_counts") or {}
    coverage_counts = coverage.get("index_type_counts") or {}
    latest_trade_date = summary.get("latest_trade_date") or coverage.get("end_date") or "N/A"
    distinct_codes = int(summary.get("distinct_index_codes", 0) or 0)
    reason = result.get("reason")

    if status == "success" and rows_written > 0:
        conclusion = (
            f"同步成功，本次写入 {rows_written} 行指数分析指标，"
            f"最新交易日 {latest_trade_date}。"
        )
    elif status == "success":
        conclusion = "任务成功结束，但本次没有写入指数分析指标；需结合上游返回和日志判断是否为合法空跑。"
    elif status in {"disabled", "unavailable", "skipped"}:
        conclusion = reason or "任务未执行，请查看配置或研究存储初始化状态。"
    else:
        conclusion = reason or "任务失败，请查看日志。"

    detail_lines = []
    detail_source = type_counts or coverage_counts
    for index_type, counts in detail_source.items():
        missing_metrics = counts.get("missing_metrics") or {}
        missing_text = f", missing={missing_metrics}" if missing_metrics else ""
        detail_lines.append(
            f"{index_type}: rows={counts.get('rows', 0)}, "
            f"codes={counts.get('codes', 0)}, "
            f"dates={counts.get('trade_dates', 0)}"
            f"{missing_text}"
        )
    if not detail_lines:
        detail_lines.append(reason or "无指数类型明细")

    return (
        f"{icon} *研究域申万行业指数分析日频指标同步*\n\n"
        f"任务: `industry_index_analysis_sync`\n"
        f"结论: *{label}* - {conclusion}\n\n"
        f"状态: `{status}`\n"
        f"operation: `{result.get('operation', 'latest')}`\n"
        f"数据源: {_format_source_mode(result)}\n"
        f"rows_written: `{rows_written}`\n"
        f"latest_trade_date: `{latest_trade_date}`\n"
        f"distinct_index_codes: `{distinct_codes}`\n"
        f"index_types: `{len(detail_source)}`\n"
        f"说明: 该任务只写 `industry_index_analysis_daily`，不改股票行业归属。\n\n"
        "指数类型明细:\n"
        "```text\n"
        + "\n".join(detail_lines[:10])
        + "\n```"
    )


def _futures_series_exchange(item: Dict[str, Any]) -> str:
    series_id = str(item.get("series_id") or "")
    parts = [part for part in series_id.split(".") if part]
    if len(parts) >= 4:
        return parts[-2].upper()
    instrument_id = str(item.get("instrument_id") or "")
    parts = [part for part in instrument_id.split(".") if part]
    if len(parts) >= 3:
        return parts[-1].upper()
    return "UNKNOWN"


def _futures_result_exchange_label(result: Dict[str, Any]) -> str:
    scope = result.get("scope_selection") or {}
    exchanges = scope.get("exchanges") or []
    if not exchanges:
        governance = result.get("trading_day_governance") or result.get("target_date_expansion") or {}
        exchanges = governance.get("exchanges") or []
    if not exchanges:
        governance = result.get("trading_day_governance") or result.get("target_date_expansion") or {}
        exchanges = [
            item.get("exchange")
            for item in governance.get("expansions") or []
            if item.get("exchange")
        ]
    if not exchanges:
        exchanges = sorted(
            {
                _futures_series_exchange(item)
                for item in result.get("series") or []
                if _futures_series_exchange(item) != "UNKNOWN"
            }
        )
    return ",".join(str(item).upper() for item in exchanges if item) or "configured"


def _summarize_futures_master_governance_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {
        "candidates": 0,
        "pending": 0,
        "auto_promoted": 0,
        "contracts_discovered": 0,
        "contracts_written": 0,
    }
    summaries = []
    statuses = []
    for item in results:
        status = str(item.get("status") or "unknown")
        statuses.append(status)
        item_counts = item.get("counts") or {}
        counts["candidates"] += int(item_counts.get("master_discovery_candidates", 0) or 0)
        counts["pending"] += int(item_counts.get("master_discovery_pending_review", 0) or 0)
        counts["auto_promoted"] += int(item_counts.get("master_discovery_auto_promoted", 0) or 0)
        counts["contracts_discovered"] += int(item_counts.get("contracts_discovered", 0) or 0)
        counts["contracts_written"] += int(item_counts.get("contracts_written", 0) or 0)
        summaries.append({
            "exchange": item.get("exchange"),
            "status": status,
            "counts": {
                "master_discovery_candidates": item_counts.get("master_discovery_candidates", 0),
                "master_discovery_pending_review": item_counts.get("master_discovery_pending_review", 0),
                "master_discovery_auto_promoted": item_counts.get("master_discovery_auto_promoted", 0),
                "contracts_discovered": item_counts.get("contracts_discovered", 0),
                "contracts_written": item_counts.get("contracts_written", 0),
            },
            "blockers": item.get("blockers") or [],
            "warnings": item.get("warnings") or [],
        })
    if any(status == "blocked" for status in statuses):
        aggregate_status = "blocked"
    elif any(status in {"warning", "partial"} for status in statuses):
        aggregate_status = "warning"
    else:
        aggregate_status = "success" if statuses else "skipped"
    return {
        "status": aggregate_status,
        "counts": counts,
        "results": summaries,
    }


def _format_futures_market_data_scheduler_report(
    result: Dict[str, Any],
    *,
    series_override: Optional[List[Dict[str, Any]]] = None,
    exchange_label: Optional[str] = None,
    include_series_details: bool = True,
) -> str:
    """Build an operator-facing report for futures market-data maintenance."""
    def _aggregate_series_totals(items: List[Dict[str, Any]]) -> Dict[str, int]:
        aggregate = {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
            "calendar_skipped": 0,
            "provider_empty_on_trading_day": 0,
            "fetched_rows": 0,
            "would_write_price_bars": 0,
        }
        for item in items:
            status_text = str(item.get("status") or "")
            if status_text == "failed":
                aggregate["failed"] += 1
            if status_text in {"calendar_skip", "lifecycle_skip"}:
                aggregate["calendar_skipped"] += 1
            aggregate["fetched_rows"] += int(item.get("fetched_rows") or 0)
            write_result = item.get("write_result") or {}
            aggregate["inserted"] += int(write_result.get("inserted") or 0)
            aggregate["changed"] += int(write_result.get("changed") or 0)
            aggregate["unchanged"] += int(write_result.get("unchanged") or 0)
            aggregate["would_write_price_bars"] += int(write_result.get("would_write_rows") or 0)
            for date_result in item.get("date_results") or []:
                if int(date_result.get("fetched_rows") or 0) == 0:
                    aggregate["provider_empty_on_trading_day"] += 1
        return aggregate

    series = series_override if series_override is not None else (result.get("series") or [])
    totals = (
        _aggregate_series_totals(series)
        if series_override is not None
        else (result.get("totals") or {})
    )
    status = result.get("status", "unknown")
    if series_override is not None:
        status = "partial" if int(totals.get("failed", 0) or 0) > 0 else "success"
    icon, label = _format_scheduler_status(status)

    def _freshness_lines() -> List[str]:
        exchange_completeness = result.get("exchange_completeness") or {}
        lines: List[str] = []
        for exchange, item in sorted(exchange_completeness.items()):
            if exchange_label and str(exchange).upper() != exchange_label.upper():
                continue
            target_dates = item.get("required_target_dates") or item.get("governed_target_dates") or []
            missing_dates = item.get("remaining_missing_dates") or []
            repaired_dates = item.get("repaired_dates") or []
            blockers = item.get("blockers") or []
            lines.append(
                f"{exchange}: range={item.get('requested_start_date') or 'N/A'}.."
                f"{item.get('requested_end_date') or 'N/A'}, "
                f"cutoff={item.get('publication_cutoff') or 'N/A'}, "
                f"targets={','.join(map(str, target_dates)) or 'none'}, "
                f"expected={item.get('expected_latest_trading_date') or 'N/A'}, "
                f"actual={item.get('actual_latest_price_date') or 'N/A'}, "
                f"repaired={','.join(map(str, repaired_dates)) or 'none'}, "
                f"missing={','.join(map(str, missing_dates)) or 'none'}, "
                f"blockers={';'.join(map(str, blockers)) or 'none'}"
            )
        return lines

    def _format_warning_items(warnings: List[Any], *, limit: int = 10) -> List[str]:
        lines: List[str] = []
        for item in warnings[:limit]:
            if not isinstance(item, dict):
                lines.append(str(item))
                continue
            reason = item.get("reason") or "warning"
            if str(reason).startswith("unmapped_") and str(reason).endswith("_varieties"):
                samples = item.get("samples") or []
                sample_text = ", ".join(f"{symbol}:{count}" for symbol, count in samples[:20])
                candidates = item.get("discovery_candidates") or []
                candidate_parts = []
                for candidate in candidates[:20]:
                    if not isinstance(candidate, dict):
                        candidate_parts.append(str(candidate))
                        continue
                    candidate_parts.append(
                        f"{candidate.get('candidate_instrument_id') or candidate.get('discovery_id')}:"
                        f"{candidate.get('candidate_name') or 'N/A'}/"
                        f"{candidate.get('candidate_category') or 'N/A'}/"
                        f"{candidate.get('candidate_unit') or 'N/A'}"
                    )
                candidate_text = "; ".join(candidate_parts) if candidate_parts else "none"
                lines.append(
                    f"{reason}: samples=[{sample_text}], discovery_candidates=[{candidate_text}]"
                )
                continue
            compact = {key: value for key, value in item.items() if key != "discovery_candidates"}
            lines.append(str(compact))
        return lines

    if result.get("domain") == "futures_master_discovery_governance":
        counts = result.get("counts") or {}
        blockers = result.get("blockers") or []
        warnings = result.get("warnings") or []
        detail_lines = []
        config_lines = []
        for item in (result.get("candidates") or [])[:12]:
            missing_fields = item.get("missing_required_fields") or []
            detail_lines.append(
                f"{item.get('discovery_id', 'unknown')}: "
                f"name={item.get('candidate_name') or 'N/A'}, "
                f"category={item.get('candidate_category') or 'N/A'}, "
                f"unit={item.get('candidate_unit') or 'N/A'}, "
                f"quality={item.get('quality_flag') or 'N/A'}, "
                f"review={item.get('review_status') or 'N/A'}, "
                f"missing={','.join(missing_fields) if missing_fields else 'none'}, "
                f"first={item.get('first_seen_trade_date') or 'N/A'}, "
                f"last={item.get('last_seen_trade_date') or 'N/A'}"
            )
            config_update = item.get("config_update") or {}
            suggested_entry = config_update.get("suggested_entry") or {}
            if config_update:
                config_lines.append(
                    f"{item.get('discovery_id', 'unknown')}: update "
                    f"{config_update.get('file', 'config/11_futures.json')} -> "
                    f"{config_update.get('json_path', 'N/A')} = "
                    f"{json.dumps(suggested_entry, ensure_ascii=False, sort_keys=True)}"
                )
        if not detail_lines:
            detail_lines.append(result.get("reason") or "无候选样本")
        promotion_lines = []
        for item in (result.get("promotion_results") or [])[:12]:
            promotion_lines.append(
                f"{item.get('discovery_id', 'unknown')}: "
                f"status={item.get('status')}, "
                f"instrument={item.get('instrument_id', 'N/A')}, "
                f"series={item.get('series_id', 'N/A')}"
            )
        blocker_text = ""
        if blockers:
            blocker_text = "\n\n阻塞项:\n```text\n" + "\n".join(map(str, blockers[:10])) + "\n```"
        warning_text = ""
        if warnings:
            warning_text = "\n\n警告:\n```text\n" + "\n".join(_format_warning_items(warnings)) + "\n```"
        promotion_text = ""
        if promotion_lines:
            promotion_text = "\n\nPromotion:\n```text\n" + "\n".join(promotion_lines) + "\n```"
        config_update_text = ""
        if config_lines:
            config_update_text = (
                "\n\n配置维护提示:\n"
                "当前任务只写入 futures.db，不自动回写配置文件。若确认新增/变更品种应长期纳入治理，"
                "请按以下建议更新配置：\n"
                "```text\n"
                + "\n".join(config_lines[:8])
                + "\n```"
            )
        return (
            f"{icon} *商品期货主数据发现治理*\n\n"
            f"结论: *{label}*\n"
            f"状态: `{status}`\n"
            f"source_profile: `{result.get('source_profile', 'N/A')}`\n"
            f"range: `{result.get('start_date', 'N/A')}` 至 `{result.get('end_date', 'N/A')}`\n"
            f"dry_run: `{result.get('dry_run', True)}`\n\n"
            f"candidates_discovered: `{counts.get('candidates_discovered', 0)}`\n"
            f"candidates_written: `{counts.get('candidates_written', 0)}`\n"
            f"would_write_candidates: `{counts.get('would_write_candidates', 0)}`\n"
            f"pending_review: `{counts.get('pending_review', 0)}`\n"
            f"auto_promoted: `{counts.get('auto_promoted', 0)}`\n"
            f"official_request_count: `{counts.get('official_request_count', 0)}`\n\n"
            "候选样本:\n"
            "```text\n"
            + "\n".join(detail_lines)
            + "\n```"
            + promotion_text
            + config_update_text
            + blocker_text
            + warning_text
        )
    if result.get("domain") == "futures_master_governance":
        counts = result.get("counts") or {}
        calendar = result.get("calendar") or {}
        blockers = result.get("blockers") or []
        warnings = result.get("warnings") or []
        detail_lines = []
        for item in (result.get("contracts") or [])[:10]:
            detail_lines.append(
                f"{item.get('contract_id', 'unknown')}: "
                f"instrument={item.get('instrument_id', 'N/A')}, "
                f"code={item.get('exchange_contract_code', 'N/A')}, "
                f"month={item.get('contract_month', 'N/A')}, "
                f"first={item.get('first_observed_trade_date', 'N/A')}, "
                f"last={item.get('last_observed_trade_date', 'N/A')}"
            )
        if not detail_lines:
            detail_lines.append(result.get("reason") or "无合约样本")
        blocker_text = ""
        if blockers:
            blocker_text = "\n\n阻塞项:\n```text\n" + "\n".join(map(str, blockers[:10])) + "\n```"
        warning_text = ""
        if warnings:
            warning_text = "\n\n警告:\n```text\n" + "\n".join(_format_warning_items(warnings)) + "\n```"
        return (
            f"{icon} *商品期货主数据治理*\n\n"
            f"结论: *{label}*\n"
            f"状态: `{status}`\n"
            f"exchange: `{result.get('exchange', 'N/A')}`\n"
            f"source_profile: `{result.get('source_profile', 'N/A')}`\n"
            f"range: `{result.get('start_date', 'N/A')}` 至 `{result.get('end_date', 'N/A')}`\n"
            f"dry_run: `{result.get('dry_run', True)}`\n\n"
            f"verified_trading_days: `{calendar.get('verified_trading_days', 0)}`\n"
            f"calendar_first: `{calendar.get('first_trade_date', 'N/A')}`\n"
            f"calendar_last: `{calendar.get('last_trade_date', 'N/A')}`\n\n"
            f"instruments: `{counts.get('instruments', 0)}`\n"
            f"series: `{counts.get('series', 0)}`\n"
            f"initial_instruments: `{counts.get('initial_instruments', counts.get('instruments', 0))}`\n"
            f"final_instruments: `{counts.get('final_instruments', counts.get('instruments', 0))}`\n"
            f"refreshed_instruments: `{counts.get('refreshed_instruments', 0)}`\n"
            f"initial_series: `{counts.get('initial_series', counts.get('series', 0))}`\n"
            f"final_series: `{counts.get('final_series', counts.get('series', 0))}`\n"
            f"refreshed_series: `{counts.get('refreshed_series', 0)}`\n"
            f"contracts_discovered: `{counts.get('contracts_discovered', 0)}`\n"
            f"contracts_written: `{counts.get('contracts_written', 0)}`\n"
            f"would_write_contracts: `{counts.get('would_write_contracts', 0)}`\n"
            f"official_request_count: `{counts.get('official_request_count', 0)}`\n"
            f"master_discovery_candidates: `{counts.get('master_discovery_candidates', 0)}`\n"
            f"master_discovery_pending_review: `{counts.get('master_discovery_pending_review', 0)}`\n"
            f"master_discovery_auto_promoted: `{counts.get('master_discovery_auto_promoted', 0)}`\n\n"
            f"challenge_count: `{counts.get('challenge_count', 0)}`\n"
            f"challenge_backoff_seconds: `{counts.get('challenge_backoff_seconds', 0)}`\n"
            f"batch_pause_count: `{counts.get('batch_pause_count', 0)}`\n"
            f"batch_pause_seconds: `{counts.get('batch_pause_seconds', 0)}`\n"
            f"retry_backoff_count: `{counts.get('retry_backoff_count', 0)}`\n"
            f"retry_backoff_seconds: `{counts.get('retry_backoff_seconds', 0)}`\n"
            f"task_retry_passes: `{counts.get('task_retry_passes', 0)}`\n"
            f"task_retry_resolved: `{counts.get('task_retry_resolved', 0)}`\n"
            f"failed_trade_dates: `{counts.get('failed_trade_dates', 0)}`\n\n"
            "合约样本:\n"
            "```text\n"
            + "\n".join(detail_lines)
            + "\n```"
            + blocker_text
            + warning_text
        )
    if result.get("domain") == "futures_official_trading_calendar_backfill":
        totals = result.get("totals") or {}
        detail_lines = []
        for item in (result.get("exchanges") or [])[:10]:
            detail_lines.append(
                f"{item.get('exchange', 'unknown')}: written={item.get('rows_written', 0)}, "
                f"trading={item.get('trading_days', 0)}, "
                f"closed={item.get('closed_days', 0)}, "
                f"unresolved={item.get('unresolved_dates', 0)}, "
                f"truncated={item.get('truncated_dates', item.get('truncated_remaining_dates', 0))}, "
                f"future_unresolved={item.get('future_dates_unresolved', 0)}, "
                f"retry_passes={item.get('retry_passes_attempted', 0)}, "
                f"retry_resolved={item.get('retry_dates_resolved', 0)}, "
                f"challenges={item.get('challenge_count', 0)}, "
                f"challenge_sleep={item.get('challenge_backoff_seconds', 0)}, "
                f"rate_limits={item.get('rate_limit_count', 0)}, "
                f"rate_limit_sleep={item.get('rate_limit_backoff_seconds', 0)}, "
                f"batch_pauses={item.get('batch_pause_count', 0)}, "
                f"batch_sleep={item.get('batch_pause_seconds', 0)}, "
                f"latest={item.get('latest_verified_date') or 'N/A'}"
            )
        if not detail_lines:
            detail_lines.append(result.get("reason") or "无交易所明细")
        failure_lines = []
        for item in result.get("exchanges") or []:
            exchange = item.get("exchange", "unknown")
            for sample in (item.get("failure_samples") or [])[:5]:
                failure_lines.append(
                    f"{exchange} {sample.get('trade_date', 'N/A')}: "
                    f"{sample.get('reason', sample.get('status', 'unresolved'))}"
                )
                if len(failure_lines) >= 10:
                    break
            if len(failure_lines) >= 10:
                break
        failure_text = ""
        if failure_lines:
            failure_text = (
                "\n\n失败样本:\n"
                "```text\n"
                + "\n".join(failure_lines)
                + "\n```"
            )
        return (
            f"{icon} *商品期货官方交易日历回填*\n\n"
            f"结论: *{label}*\n"
            f"状态: `{status}`\n"
            f"source_profile: `{result.get('source_profile', 'N/A')}`\n"
            f"quality_flag: `{result.get('quality_flag', 'N/A')}`\n"
            f"range: `{result.get('start_date', 'N/A')}` 至 `{result.get('end_date', 'N/A')}`\n"
            f"probe_end: `{result.get('probe_end_date', 'N/A')}`\n"
            f"dry_run: `{result.get('dry_run', False)}`\n\n"
            f"rows_written: `{totals.get('rows_written', 0)}`\n"
            f"trading_days: `{totals.get('trading_days', 0)}`\n"
            f"closed_days: `{totals.get('closed_days', 0)}`\n"
            f"unresolved_dates: `{totals.get('unresolved_dates', 0)}`\n"
            f"truncated_dates: `{totals.get('truncated_dates', 0)}`\n"
            f"request_count: `{totals.get('request_count', 0)}`\n\n"
            f"challenge_count: `{totals.get('challenge_count', 0)}`\n"
            f"challenge_backoff_seconds: `{totals.get('challenge_backoff_seconds', 0)}`\n"
            f"rate_limit_count: `{totals.get('rate_limit_count', 0)}`\n"
            f"rate_limit_backoff_seconds: `{totals.get('rate_limit_backoff_seconds', 0)}`\n"
            f"batch_pause_count: `{totals.get('batch_pause_count', 0)}`\n"
            f"batch_pause_seconds: `{totals.get('batch_pause_seconds', 0)}`\n\n"
            "交易所明细:\n"
            "```text\n"
            + "\n".join(detail_lines)
            + "\n```"
            + failure_text
        )
    governance = dict(result.get("trading_day_governance") or result.get("target_date_expansion") or {})
    if series_override is not None:
        target_dates_for_series = set()
        for item in series:
            for target_date in item.get("target_trade_dates") or []:
                target_dates_for_series.add(str(target_date))
            for date_result in item.get("date_results") or []:
                if date_result.get("trade_date"):
                    target_dates_for_series.add(str(date_result.get("trade_date")))
            lifecycle = item.get("lifecycle") or {}
            if lifecycle.get("target_start") and lifecycle.get("target_end"):
                if lifecycle.get("target_start") == lifecycle.get("target_end"):
                    target_dates_for_series.add(str(lifecycle.get("target_start")))
        if target_dates_for_series:
            governance["target_date_count"] = len(target_dates_for_series)
        if exchange_label:
            matching_expansions = [
                expansion for expansion in governance.get("expansions") or []
                if str(expansion.get("exchange") or "").upper() == exchange_label.upper()
            ]
            if matching_expansions:
                governance["expansions"] = matching_expansions
    actual_calendar_quality = governance.get("lowest_quality")
    if not actual_calendar_quality:
        expansion_qualities = []
        for expansion in governance.get("expansions") or []:
            summary = expansion.get("quality_summary") or {}
            quality = summary.get("lowest_quality")
            if quality:
                expansion_qualities.append(str(quality))
        if expansion_qualities:
            # Show the actual weakest calendar evidence used for the run.  The
            # configured minimum threshold is reported separately below.
            actual_calendar_quality = min(
                expansion_qualities,
                key=lambda item: {
                    "missing": 0,
                    "estimated": 1,
                    "estimated_unverified": 1,
                    "manual_override": 2,
                    "backfilled_verified": 3,
                    "official": 4,
                    "official_parsed": 4,
                }.get(item, 0),
            )
    actual_calendar_quality = actual_calendar_quality or "N/A"
    master_governance = result.get("master_data_governance")
    master_governance_status = "not_requested"
    if isinstance(master_governance, dict):
        master_governance_status = str(master_governance.get("status") or "unknown")
    else:
        master_governance = {}
    master_counts = master_governance.get("counts") or {}
    lifecycle_skipped_series = 0
    lifecycle_clipped_series = 0
    lifecycle_filtered_dates = 0
    for item in series:
        lifecycle = item.get("lifecycle") or {}
        lifecycle_status = str(lifecycle.get("status") or "")
        if lifecycle_status == "lifecycle_clipped":
            lifecycle_clipped_series += 1
        if lifecycle_status == "lifecycle_skip" or item.get("status") == "lifecycle_skip":
            lifecycle_skipped_series += 1
        try:
            original_dates = int(lifecycle.get("original_target_dates") or 0)
            filtered_dates = int(lifecycle.get("filtered_target_dates") or 0)
        except (TypeError, ValueError):
            original_dates = 0
            filtered_dates = 0
        if original_dates > filtered_dates:
            lifecycle_filtered_dates += original_dates - filtered_dates
    detail_lines = []
    if include_series_details:
        for item in series[:10]:
            write_result = item.get("write_result") or {}
            lifecycle = item.get("lifecycle") or {}
            lifecycle_text = ""
            if lifecycle:
                lifecycle_status = lifecycle.get("status") or "unknown"
                original_dates = lifecycle.get("original_target_dates")
                filtered_dates = lifecycle.get("filtered_target_dates")
                if original_dates is not None and filtered_dates is not None:
                    lifecycle_text = f", lifecycle={lifecycle_status}({original_dates}->{filtered_dates})"
                else:
                    lifecycle_text = f", lifecycle={lifecycle_status}"
            detail_lines.append(
                f"{item.get('series_id', 'unknown')}: fetched={item.get('fetched_rows', 0)}, "
                f"would_write={write_result.get('would_write_rows', 0)}, "
                f"inserted={write_result.get('inserted', 0)}, "
                f"changed={write_result.get('changed', 0)}, "
                f"unchanged={write_result.get('unchanged', 0)}, "
                f"status={item.get('status', 'ok')}"
                f"{lifecycle_text}"
            )
        if len(series) > 10:
            detail_lines.append(f"... {len(series) - 10} more series omitted")
        if not detail_lines:
            detail_lines.append(result.get("reason") or "无序列明细")
    else:
        detail_lines.append("序列明细已按交易所拆分发送。")
    exchange_scope = exchange_label or _futures_result_exchange_label(result)
    freshness_lines = _freshness_lines()
    freshness_text = ""
    if freshness_lines:
        freshness_text = (
            "\n交易所目标与完整性:\n```text\n"
            + "\n".join(freshness_lines)
            + "\n```\n"
        )
    return (
        f"{icon} *商品期货行情数据维护*\n\n"
        f"结论: *{label}*\n"
        f"状态: `{status}`\n"
        f"run_id: `{result.get('run_id', 'N/A')}`\n"
        f"exchange/scope: `{exchange_scope}`\n"
        f"inserted: `{totals.get('inserted', 0)}`\n"
        f"changed: `{totals.get('changed', 0)}`\n"
        f"unchanged: `{totals.get('unchanged', 0)}`\n"
        f"failed: `{totals.get('failed', 0)}`\n"
        f"dry_run: `{result.get('dry_run', 'N/A')}`\n\n"
        f"calendar_skipped: `{totals.get('calendar_skipped', governance.get('skipped_date_count', 0))}`\n"
        f"provider_empty_on_trading_day: `{totals.get('provider_empty_on_trading_day', 0)}`\n"
        f"lifecycle_skipped_series: `{lifecycle_skipped_series}`\n"
        f"lifecycle_clipped_series: `{lifecycle_clipped_series}`\n"
        f"lifecycle_filtered_dates: `{lifecycle_filtered_dates}`\n"
        f"trading_day_governance: `{governance.get('status', 'N/A')}`\n"
        f"master_data_governance: `{master_governance_status}`\n"
        f"master_discovery_candidates: `{master_counts.get('candidates', 0)}`\n"
        f"master_discovery_pending: `{master_counts.get('pending', 0)}`\n"
        f"master_discovery_auto_promoted: `{master_counts.get('auto_promoted', 0)}`\n"
        f"target_trade_dates: `{governance.get('target_date_count', 0)}`\n"
        f"calendar_quality: `{actual_calendar_quality}`\n"
        f"calendar_min_required: `{governance.get('minimum_quality', 'N/A')}`\n\n"
        + freshness_text
        + "序列明细:\n"
        + "```text\n"
        + "\n".join(detail_lines)
        + "\n```"
    )


def _format_futures_market_data_scheduler_reports(result: Dict[str, Any]) -> List[str]:
    """Build Telegram-sized futures market-data reports, splitting details by exchange."""
    if result.get("domain") or not result.get("series"):
        return [_format_futures_market_data_scheduler_report(result)]
    series = result.get("series") or []
    totals = result.get("totals") or {}
    master_governance = result.get("master_data_governance") or {}
    master_counts = master_governance.get("counts") if isinstance(master_governance, dict) else {}
    master_counts = master_counts or {}
    master_status = str(master_governance.get("status") or "success")
    master_results = master_governance.get("results") if isinstance(master_governance, dict) else []
    master_has_blockers = any(
        bool((item or {}).get("blockers"))
        or str((item or {}).get("status") or "") in {"blocked", "failed"}
        for item in (master_results or [])
    )
    exchange_completeness = result.get("exchange_completeness") or {}
    normal_success = (
        str(result.get("status") or "") == "success"
        and not bool(result.get("dry_run"))
        and int(totals.get("failed") or 0) == 0
        and int(totals.get("provider_empty_on_trading_day") or 0) == 0
        and str((result.get("trading_day_governance") or {}).get("status") or "") == "success"
        and master_status in {"success", "warning", "skipped"}
        and not master_has_blockers
        and int(master_counts.get("pending") or 0) == 0
        and all(
            str(item.get("status") or "") == "success"
            for item in exchange_completeness.values()
        )
    )
    if normal_success:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for item in series:
            groups.setdefault(_futures_series_exchange(item), []).append(item)

        lines = []
        lifecycle_skipped_total = 0
        for exchange in sorted(groups):
            exchange_items = groups[exchange]
            exchange_totals = {
                "fetched": 0,
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "failed": 0,
                "lifecycle_skipped": 0,
            }
            for item in exchange_items:
                exchange_totals["fetched"] += int(item.get("fetched_rows") or 0)
                if str(item.get("status") or "") == "failed":
                    exchange_totals["failed"] += 1
                if str(item.get("status") or "") == "lifecycle_skip":
                    exchange_totals["lifecycle_skipped"] += 1
                write_result = item.get("write_result") or {}
                exchange_totals["inserted"] += int(write_result.get("inserted") or 0)
                exchange_totals["changed"] += int(write_result.get("changed") or 0)
                exchange_totals["unchanged"] += int(write_result.get("unchanged") or 0)
            lifecycle_skipped_total += exchange_totals["lifecycle_skipped"]
            completeness = exchange_completeness.get(exchange) or {}
            lines.append(
                f"{exchange}: 写入 {exchange_totals['inserted']}，"
                f"更新 {exchange_totals['changed']}，"
                f"不变 {exchange_totals['unchanged']}，"
                f"获取 {exchange_totals['fetched']}，"
                f"跳过 {exchange_totals['lifecycle_skipped']}，"
                f"目标 {','.join(completeness.get('required_target_dates') or []) or 'none'}，"
                f"预期 {completeness.get('expected_latest_trading_date') or 'N/A'}，"
                f"实际 {completeness.get('actual_latest_price_date') or 'N/A'}"
            )
        if not lines:
            lines.append("无交易所明细")
        governance = result.get("trading_day_governance") or {}
        return [
            (
                "✅ *商品期货行情日更*\n\n"
                f"状态: `success`\n"
                f"run_id: `{result.get('run_id', 'N/A')}`\n"
                f"交易所: `{_futures_result_exchange_label(result)}`\n"
                f"交易日: `{governance.get('target_date_count', 0)}`\n"
                f"写入: `{totals.get('inserted', 0)}`｜"
                f"更新: `{totals.get('changed', 0)}`｜"
                f"不变: `{totals.get('unchanged', 0)}`｜"
                f"失败: `{totals.get('failed', 0)}`\n"
                f"新增品种: `{master_counts.get('auto_promoted', 0)}`｜"
                f"待确认品种: `{master_counts.get('pending', 0)}`｜"
                f"生命周期跳过: `{lifecycle_skipped_total}`\n\n"
                "交易所明细:\n"
                "```text\n"
                + "\n".join(lines)
                + "\n```"
            )
        ]
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for item in series:
        groups.setdefault(_futures_series_exchange(item), []).append(item)
    if len(groups) <= 1 and len(series) <= 10:
        return [_format_futures_market_data_scheduler_report(result)]
    reports = [
        _format_futures_market_data_scheduler_report(
            result,
            include_series_details=False,
        )
    ]
    for exchange in sorted(groups):
        reports.append(
            _format_futures_market_data_scheduler_report(
                result,
                series_override=groups[exchange],
                exchange_label=exchange,
                include_series_details=True,
            )
        )
    return reports


def _format_fx_market_data_scheduler_report(result: Dict[str, Any]) -> str:
    """Return a compact operator report for FX-domain tasks."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    scope = result.get("scope_selection") or {}
    totals = result.get("totals") or {}
    counts = result.get("counts") or {}
    source_results = result.get("source_results") or []
    blockers = result.get("blockers") or []
    warnings = result.get("warnings") or []
    lines = [
        f"{icon} *外汇数据维护*",
        "",
        f"状态: `{status}` ({label})",
    ]
    if result.get("domain"):
        lines.append(f"域: `{result.get('domain')}`")
    if result.get("run_id") is not None:
        lines.append(f"run_id: `{result.get('run_id')}`")
    if scope:
        resolved = scope.get("series_ids") or []
        lines.append(f"序列数: `{len(resolved)}`")
    if totals:
        lines.append(
            "写入统计: "
            f"新增 `{totals.get('inserted', 0)}`｜"
            f"更新 `{totals.get('updated', 0)}`｜"
            f"不变 `{totals.get('unchanged', 0)}`｜"
            f"缺口 `{totals.get('gaps', 0)}`｜"
            f"失败 `{totals.get('failed', 0)}`"
        )
    if counts:
        lines.append(
            "主数据: "
            + "｜".join(f"{key} `{value}`" for key, value in sorted(counts.items()))
        )
    if result.get("rows") is not None:
        lines.append(f"日历行: `{result.get('rows')}`")
    if source_results:
        source_lines = [
            f"{item.get('source_profile')}: {item.get('status')}"
            for item in source_results[:8]
        ]
        lines.extend(["", "来源:", "```text", "\n".join(source_lines), "```"])
    if blockers:
        lines.extend(["", "阻断:", "```text", "\n".join(str(item) for item in blockers[:12]), "```"])
    if warnings:
        lines.extend(["", "告警:", "```text", "\n".join(str(item) for item in warnings[:12]), "```"])
    return "\n".join(lines)


def _format_risk_free_rate_sync_report(result: Dict[str, Any]) -> str:
    """Return a compact operator report for the risk-free-rate series sync task."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status("success" if status == "ok" else status)
    lines = [
        f"{icon} *无风险利率序列同步*",
        "",
        f"状态: `{status}` ({label})",
    ]
    if result.get("series_id"):
        lines.append(f"序列: `{result.get('series_id')}`")
    lines.append(
        "写入统计: "
        f"拉取 `{result.get('fetched', 0)}`｜写入 `{result.get('written', 0)}`"
    )
    if result.get("earliest_date") or result.get("latest_date"):
        lines.append(
            f"覆盖区间: `{result.get('earliest_date') or 'N/A'}` 至 `{result.get('latest_date') or 'N/A'}`"
        )
    if result.get("latest_value") is not None:
        lines.append(f"最新值: `{result.get('latest_value')}`")
    if result.get("elapsed_seconds") is not None:
        lines.append(f"耗时: `{result.get('elapsed_seconds')}s`")
    if result.get("reason"):
        lines.extend(["", f"原因: `{result.get('reason')}`"])
    return "\n".join(lines)


def _format_special_commodity_scheduler_report(
    result: Dict[str, Any], *, title: str = "特殊商品数据维护"
) -> str:
    """Return a compact operator report for special commodity data tasks."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)
    warnings = result.get("warnings") or []
    blockers = result.get("blockers") or []
    per_source = result.get("per_source") or {}
    detailed = status not in {"success", "skipped", "disabled"} or bool(warnings or blockers)

    lines = [
        f"{icon} *{title}*",
        "",
        f"状态: `{status}` ({label})",
    ]
    if result.get("run_id") is not None:
        lines.append(f"run_id: `{result.get('run_id')}`")
    elif result.get("run_ids"):
        lines.append(
            "run_ids: `"
            + ",".join(str(value) for value in result.get("run_ids", []))
            + "`"
        )
    if result.get("dry_run") is not None:
        lines.append(f"dry_run: `{bool(result.get('dry_run'))}`")
    if result.get("start_date") or result.get("end_date"):
        lines.append(f"range: `{result.get('start_date') or 'N/A'}` 至 `{result.get('end_date') or 'N/A'}`")
    if result.get("target_series") is not None:
        lines.append(f"序列数: `{result.get('target_series')}`")
    if result.get("venues"):
        lines.append(f"venues: `{','.join(result.get('venues') or [])}`")
    if result.get("master_data_governance") is not None:
        lines.append(
            "治理: "
            f"主数据 `{result.get('master_data_governance')}`｜"
            f"日期 `{result.get('date_governance')}`｜"
            f"主数据证据 `{result.get('master_governance_records', 0)}`｜"
            f"来源日期 `{result.get('source_date_count', 0)}`"
        )

    if "fetched_rows" in result:
        lines.append(
            "观测值: "
            f"获取 `{result.get('fetched_rows', 0)}`｜"
            f"新增 `{result.get('inserted', 0)}`｜"
            f"更新 `{result.get('changed', 0)}`｜"
            f"不变 `{result.get('unchanged', 0)}`｜"
            f"dry_run预计 `{result.get('would_write', 0)}`"
        )
        for scope in result.get("scope_results") or []:
            lines.append(
                "scope: "
                f"`{scope.get('scope_id')}`｜状态 `{scope.get('status')}`｜"
                f"获取 `{scope.get('fetched_rows', 0)}`｜新增 `{scope.get('inserted', 0)}`｜"
                f"更新 `{scope.get('changed', 0)}`｜不变 `{scope.get('unchanged', 0)}`"
            )
        for scope in result.get("skipped_scopes") or []:
            lines.append(
                f"scope: `{scope.get('scope_id')}`｜跳过 `{scope.get('reason', 'not_due')}`"
            )
    elif "calendar_rows" in result:
        lines.append(
            "发布日历: "
            f"行数 `{result.get('calendar_rows', 0)}`｜"
            f"缺失观测 `{result.get('missing_observations', 0)}`｜"
            f"写入 `{result.get('written', 0)}`｜"
            f"dry_run预计 `{result.get('would_write', 0)}`"
        )
    elif "policy_events" in result:
        lines.append(
            "政策事件: "
            f"事件 `{result.get('policy_events', 0)}`｜"
            f"新增 `{result.get('inserted', 0)}`｜"
            f"更新 `{result.get('changed', 0)}`｜"
            f"不变 `{result.get('unchanged', 0)}`｜"
            f"dry_run预计 `{result.get('would_write', 0)}`"
        )
        for event in (result.get("event_summaries") or [])[:5]:
            value_range = (
                f"{event.get('value_low')}-{event.get('value_high')}"
                if event.get("value_low") is not None and event.get("value_high") is not None
                else str(event.get("value_mid") if event.get("value_mid") is not None else "N/A")
            )
            lines.append(
                "事件明细: "
                f"`{event.get('commodity_id')}`｜"
                f"生效 `{event.get('effective_start')}` 至 `{event.get('effective_end') or '持续有效'}`｜"
                f"区间 `{value_range} {event.get('unit')}`｜非行情"
            )
    elif "documents" in result:
        lines.append(
            "政策发现: "
            f"文档 `{result.get('documents', 0)}`｜"
            f"候选 `{result.get('candidates', 0)}`｜"
            f"可提升 `{result.get('ready_for_promotion', 0)}`｜"
            f"待复核 `{result.get('pending_review', 0)}`"
        )
        for action in (result.get("review_actions") or [])[:5]:
            lines.extend(
                [
                    "",
                    f"待审核政策: `{action.get('document_number') or action.get('title') or action.get('review_code')}`",
                    f"类型: `{action.get('policy_type')}`｜生效: `{action.get('effective_start') or 'N/A'}`｜值: `{action.get('value')}`",
                    "批准并落库:",
                    f"`/run special_commodity_policy_candidate_review candidate_ref={action.get('review_code')} decision=approved notes=verified`",
                    "拒绝并永久忽略当前版本:",
                    f"`/run special_commodity_policy_candidate_review candidate_ref={action.get('review_code')} decision=rejected notes=not_applicable`",
                ]
            )
        if result.get("terminal_reviewed"):
            lines.append(f"已审核且不再提示: `{result.get('terminal_reviewed')}`")
        document_write = result.get("document_write") or {}
        candidate_write = result.get("candidate_write") or {}
        lines.append(
            "写入: "
            f"文档新增 `{document_write.get('inserted', 0)}`/预计 `{document_write.get('would_write', 0)}`｜"
            f"候选新增 `{candidate_write.get('inserted', 0)}`/预计 `{candidate_write.get('would_write', 0)}`"
        )
        event_reconciliation = result.get("event_reconciliation") or {}
        if event_reconciliation:
            lines.append(
                "正式事件对账: "
                f"新增 `{event_reconciliation.get('inserted', 0)}`｜"
                f"更新 `{event_reconciliation.get('changed', 0)}`｜"
                f"不变 `{event_reconciliation.get('unchanged', 0)}`｜"
                f"状态 `{event_reconciliation.get('status', 'unknown')}`"
            )
            already_represented = int(
                event_reconciliation.get("candidate_already_represented", 0) or 0
            )
            if already_represented:
                lines.append(f"已批准候选已由正式事件覆盖: `{already_represented}`")
    elif result.get("candidate_id") and result.get("decision"):
        lines.append(
            "政策候选审核: "
            f"候选 `{result.get('candidate_id')}`｜"
            f"决定 `{result.get('decision')}`｜"
            f"正式提升 `{'已完成' if result.get('promoted') else '不适用'}`"
        )
    elif "rollout_state_counts" in result:
        states = result.get("rollout_state_counts") or {}
        lines.append(
            "扩品候选: "
            f"总数 `{result.get('candidates', 0)}`｜"
            f"已发现 `{states.get('discovered', 0)}`｜"
            f"阻断 `{states.get('blocked', 0)}`｜"
            f"可调度 `{result.get('scheduler_eligible', 0)}`"
        )
    elif result.get("reason"):
        lines.append(f"原因: `{result.get('reason')}`")

    if per_source:
        source_lines = []
        for source_profile, item in sorted(per_source.items()):
            if isinstance(item, dict):
                cross_source = (
                    (item.get("quality_diagnostics") or {}).get("cross_source") or {}
                )
                diagnostics = item.get("quality_diagnostics") or {}
                observation_series = (
                    (diagnostics.get("observations") or {}).get("series") or {}
                )
                coverage_series = diagnostics.get("calendar_coverage") or {}
                first_dates = [
                    value.get("first_date")
                    for value in observation_series.values()
                    if isinstance(value, dict) and value.get("first_date")
                ]
                latest_dates = [
                    value.get("latest_date")
                    for value in observation_series.values()
                    if isinstance(value, dict) and value.get("latest_date")
                ]
                missing_dates = sum(
                    int(value.get("missing_dates", 0) or 0)
                    for value in coverage_series.values()
                    if isinstance(value, dict)
                )
                source_coverage = item.get("source_coverage") or {}
                coverage_suffix = ""
                if source_coverage:
                    reports_discovered = source_coverage.get(
                        "reports_discovered"
                    )
                    if reports_discovered is None:
                        reports_discovered = source_coverage.get(
                            "articles_discovered", 0
                        )
                    coverage_suffix = (
                        f", reports={reports_discovered}, "
                        f"metric_absent={source_coverage.get('reports_without_metric', 0)}, "
                        f"parse_failed={source_coverage.get('parse_failures', 0)}, "
                        f"field_reconciled={source_coverage.get('field_reconciliations', 0)}"
                    )
                    if "title_value_recoveries" in source_coverage:
                        coverage_suffix += (
                            ", title_recovered="
                            f"{source_coverage.get('title_value_recoveries', 0)}"
                        )
                source_lines.append(
                    f"{source_profile}: status={item.get('status', 'unknown')}, "
                    f"series={item.get('series', 0)}, master={item.get('master_records', 0)}, "
                    f"dates={item.get('calendar_rows', 0)}, fetched={item.get('fetched', 0)}, "
                    f"fallback_filled={(item.get('date_gap_fill') or {}).get('fallback_filled_dates', 0)}, "
                    f"fallback_unresolved={(item.get('date_gap_fill') or {}).get('unresolved_dates', 0)}, "
                    f"ohlc_outside={((item.get('quality_diagnostics') or {}).get('ohlc') or {}).get('close_outside_range', 0)}, "
                    f"source_conflicts={cross_source.get('conflict_count', 0)}, "
                    f"first={min(first_dates) if first_dates else 'N/A'}, "
                    f"latest={max(latest_dates) if latest_dates else 'N/A'}, "
                    f"calendar_missing={missing_dates}, "
                    f"warnings={item.get('warnings', 0)}, "
                    f"blockers={item.get('blockers', 0)}"
                    f"{coverage_suffix}"
                )
            else:
                source_lines.append(f"{source_profile}: {item}")
        lines.extend(["", "来源:", "```text", "\n".join(source_lines[:12]), "```"])

    if detailed and blockers:
        lines.extend(["", "阻断:", "```text", "\n".join(str(item) for item in blockers[:12]), "```"])
    if detailed and warnings:
        lines.extend(["", "告警:", "```text", "\n".join(str(item) for item in warnings[:12]), "```"])
    return "\n".join(lines)


def _resolve_special_commodity_sync_window(
    start_date: Optional[str],
    end_date: Optional[str],
    *,
    lookback_days: int,
    as_of_date: Optional[date] = None,
) -> tuple[str, str]:
    """Resolve an explicit or rolling calendar-day window for scheduled sync."""
    if bool(start_date) != bool(end_date):
        raise ValueError("special commodity sync requires both start_date and end_date")
    if start_date and end_date:
        return str(start_date)[:10], str(end_date)[:10]
    days = max(1, int(lookback_days))
    resolved_end = as_of_date or get_shanghai_time().date()
    resolved_start = resolved_end - timedelta(days=days - 1)
    return resolved_start.isoformat(), resolved_end.isoformat()


def _resolve_special_commodity_task_window(
    start_date: Optional[str],
    end_date: Optional[str],
    *,
    lookback_days: int,
    window_mode: str,
    as_of_date: Optional[date] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve rolling windows or leave latest-snapshot sources unbounded."""
    normalized_mode = str(window_mode or "rolling").strip().lower()
    if normalized_mode == "rolling":
        return _resolve_special_commodity_sync_window(
            start_date,
            end_date,
            lookback_days=lookback_days,
            as_of_date=as_of_date,
        )
    if normalized_mode == "provider_latest":
        if bool(start_date) != bool(end_date):
            raise ValueError(
                "special commodity provider_latest sync requires both start_date and end_date"
            )
        if start_date and end_date:
            return str(start_date)[:10], str(end_date)[:10]
        return None, None
    raise ValueError(f"unsupported special commodity window_mode: {window_mode}")


def _resolve_special_commodity_monthly_sync_window(
    start_date: Optional[str],
    end_date: Optional[str],
    *,
    lookback_months: int,
    as_of_date: Optional[date] = None,
) -> tuple[str, str]:
    """Resolve a bounded month-aligned window for scheduled monthly sources."""
    if bool(start_date) != bool(end_date):
        raise ValueError("special commodity monthly sync requires both start_date and end_date")
    if start_date and end_date:
        return str(start_date)[:10], str(end_date)[:10]

    months = max(1, int(lookback_months))
    resolved_end = as_of_date or get_shanghai_time().date()
    month_index = resolved_end.year * 12 + resolved_end.month - months
    start_year, start_month_zero_based = divmod(month_index, 12)
    resolved_start = date(start_year, start_month_zero_based + 1, 1)
    return resolved_start.isoformat(), resolved_end.isoformat()


def _special_commodity_scope_run_due(
    scope_run: Dict[str, Any], *, as_of_date: Optional[date] = None
) -> bool:
    """Return whether a configured scope profile is due on this scheduler run."""
    if scope_run.get("enabled") is False:
        return False
    resolved = as_of_date or get_shanghai_time().date()
    days = [int(value) for value in scope_run.get("run_days_of_month", [])]
    return not days or resolved.day in days


def _resolve_special_commodity_scope_run_window(
    scope_run: Dict[str, Any], *, as_of_date: Optional[date] = None
) -> tuple[Optional[str], Optional[str]]:
    """Resolve one industrial scope's provider-specific observation window."""
    mode = str(scope_run.get("window_mode") or "rolling").strip().lower()
    if mode == "monthly":
        return _resolve_special_commodity_monthly_sync_window(
            None,
            None,
            lookback_months=max(1, int(scope_run.get("lookback_months") or 4)),
            as_of_date=as_of_date,
        )
    return _resolve_special_commodity_task_window(
        None,
        None,
        lookback_days=max(1, int(scope_run.get("lookback_days") or 10)),
        window_mode=mode,
        as_of_date=as_of_date,
    )


def _aggregate_special_commodity_scope_results(
    results: List[Dict[str, Any]], skipped_scopes: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Aggregate independently governed scopes into one operator-facing task result."""
    severity = {
        "success": 0,
        "skipped": 0,
        "disabled": 0,
        "warning": 1,
        "partial": 1,
        "blocked": 2,
        "error": 3,
        "failed": 3,
    }
    statuses = [str(item.get("status") or "error") for item in results]
    status = max(statuses, key=lambda value: severity.get(value, 3)) if statuses else "skipped"
    aggregate: Dict[str, Any] = {
        "status": status,
        "dry_run": any(bool(item.get("dry_run")) for item in results),
        "run_ids": [item.get("run_id") for item in results if item.get("run_id") is not None],
        "venues": sorted({venue for item in results for venue in item.get("venues", [])}),
        "target_series": sum(int(item.get("target_series", 0) or 0) for item in results),
        "fetched_rows": sum(int(item.get("fetched_rows", 0) or 0) for item in results),
        "inserted": sum(int(item.get("inserted", 0) or 0) for item in results),
        "changed": sum(int(item.get("changed", 0) or 0) for item in results),
        "unchanged": sum(int(item.get("unchanged", 0) or 0) for item in results),
        "would_write": sum(int(item.get("would_write", 0) or 0) for item in results),
        "master_governance_records": sum(
            int(item.get("master_governance_records", 0) or 0) for item in results
        ),
        "source_date_count": sum(
            int(item.get("source_date_count", 0) or 0) for item in results
        ),
        "master_data_governance": (
            "success" if results and all(item.get("master_data_governance") == "success" for item in results)
            else status
        ),
        "date_governance": (
            "success" if results and all(item.get("date_governance") == "success" for item in results)
            else status
        ),
        "per_source": {},
        "warnings": [],
        "blockers": [],
        "scope_results": [],
        "skipped_scopes": skipped_scopes,
    }
    for item in results:
        scope_id = item.get("scope_id")
        for source_id, source_result in (item.get("per_source") or {}).items():
            aggregate["per_source"][f"{scope_id}:{source_id}"] = source_result
        aggregate["warnings"].extend(
            [{**warning, "scope_id": scope_id} for warning in item.get("warnings", [])]
        )
        aggregate["blockers"].extend(
            [{**blocker, "scope_id": scope_id} for blocker in item.get("blockers", [])]
        )
        aggregate["scope_results"].append(
            {
                "scope_id": scope_id,
                "status": item.get("status"),
                "run_id": item.get("run_id"),
                "fetched_rows": item.get("fetched_rows", 0),
                "inserted": item.get("inserted", 0),
                "changed": item.get("changed", 0),
                "unchanged": item.get("unchanged", 0),
            }
        )
    return aggregate


class ScheduledTasks:
    """定时任务管理类"""

    def __init__(self):
        self.config = config_manager

        # Telegram调用
        self.bot_config = self.config.get_telegram_config()
        self.telegram_enabled = self.bot_config.enabled
        self.bot = TelegramBot() if self.telegram_enabled else None

        # 任务自追踪集合：无论通过调度器还是 Telegram /run 直接调用，都会被记录
        self._active_tasks: set = set()

    async def initialize(self, debug=False):
        """初始化定时任务"""
        scheduler_logger.info("[Scheduler] Initializing scheduled tasks...")
        if debug:
            # 使用统一的通知接口
            if self.telegram_enabled:
                try:
                    await self.bot.send_scheduler_notification("定时任务系统已启动，开始加载任务...", "info")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

    async def _run_annual_report_asset_job(
        self,
        *,
        job_name: str,
        runner,
        job_config: Optional[JobConfig],
    ) -> bool:
        """Execute one durable shared-asset command and publish its result."""
        scheduler_logger.info(
            "[Scheduler] Starting shared annual-report asset job: %s",
            job_name,
        )
        try:
            result = await runner()
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Shared annual-report asset job failed: %s",
                job_name,
            )
            result = {
                "schema_version": "annual_report_asset_job_execution.v1",
                "job_name": job_name,
                "status": "failed",
                "outcome": "failed",
                "diagnostics": {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            }
        await self._send_task_report(
            report_data={
                "name": job_name,
                "status": result.get("status"),
                "result": result,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
            report_type="maintenance_report",
            task_name=job_name,
            job_config=job_config,
        )
        return bool(
            result.get("status") == "completed"
            and result.get("outcome") in {"success", "partial"}
        )

    async def annual_report_asset_latest_backfill(
        self,
        as_of: Optional[str] = None,
        bounds: Optional[Dict[str, int]] = None,
        trigger_kind: Optional[str] = None,
        operator_principal: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Run the manual-only latest-effective annual-report bootstrap."""
        return await self._run_annual_report_asset_job(
            job_name="annual_report_asset_latest_backfill",
            runner=lambda: data_manager.run_annual_report_asset_latest_backfill(
                as_of=as_of,
                bounds=bounds,
                trigger_kind=trigger_kind or "manual",
                **({"principal_id": operator_principal}
                   if operator_principal is not None else {}),
            ),
            job_config=job_config,
        )

    async def annual_report_asset_daily_update(
        self,
        timezone: Optional[str] = None,
        overlap_days: Optional[int] = None,
        catch_up_max_days: Optional[int] = None,
        minimum_runs_per_calendar_day: Optional[int] = None,
        universe_refresh_cadence: Optional[str] = None,
        run_cutoff: Optional[str] = None,
        bounds: Optional[Dict[str, int]] = None,
        trigger_kind: Optional[str] = None,
        operator_principal: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Run bounded daily discovery, reconciliation, and attachment upkeep."""
        expected_schedule = {
            "timezone": timezone,
            "overlap_days": overlap_days,
            "catch_up_max_days": catch_up_max_days,
            "minimum_runs_per_calendar_day": minimum_runs_per_calendar_day,
            "universe_refresh_cadence": universe_refresh_cadence,
        }
        effective_trigger = (
            trigger_kind
            or (
                "manual"
                if job_config is not None and job_config.manual_only
                else "cron"
            )
        )
        principal_id = (
            operator_principal
            if effective_trigger != "cron"
            else "service:annual-report-asset-scheduler"
        )
        return await self._run_annual_report_asset_job(
            job_name="annual_report_asset_daily_update",
            runner=lambda: data_manager.run_annual_report_asset_daily_update(
                run_cutoff=run_cutoff,
                bounds=bounds,
                trigger_kind=effective_trigger,
                expected_schedule=expected_schedule,
                **({"principal_id": principal_id} if principal_id is not None else {}),
            ),
            job_config=job_config,
        )

    async def annual_report_asset_integrity_audit(
        self,
        read_only: bool = True,
        content_hashes: Optional[List[str]] = None,
        deletion_ids: Optional[List[str]] = None,
        action_flags: Optional[Dict[str, bool]] = None,
        bounds: Optional[Dict[str, int]] = None,
        trigger_kind: Optional[str] = None,
        operator_principal: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Run a read-only audit unless an exact bounded repair is authorized."""
        requested_actions = {
            name for name, enabled in (action_flags or {}).items() if enabled
        }
        if read_only and requested_actions:
            raise ValueError("read-only integrity audit cannot request repair actions")
        if not read_only and not requested_actions:
            raise ValueError("non-read-only integrity audit requires an action flag")
        effective_trigger = (
            trigger_kind
            or (
                "cron"
                if job_config is not None and not job_config.manual_only
                else "manual"
            )
        )
        principal_id = (
            operator_principal
            if effective_trigger != "cron"
            else "service:annual-report-asset-scheduler"
        )
        return await self._run_annual_report_asset_job(
            job_name="annual_report_asset_integrity_audit",
            runner=lambda: data_manager.run_annual_report_asset_integrity_audit(
                content_hashes=tuple(content_hashes or ()),
                deletion_ids=tuple(deletion_ids or ()),
                action_flags=action_flags,
                bounds=bounds,
                trigger_kind=effective_trigger,
                **({"principal_id": principal_id} if principal_id is not None else {}),
            ),
            job_config=job_config,
        )

    async def annual_report_asset_backup(
        self,
        recovery_journal_retention_policy: Optional[str] = None,
        recovery_journal_integrity_policy: Optional[str] = None,
        bounds: Optional[Dict[str, int]] = None,
        trigger_kind: Optional[str] = None,
        operator_principal: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Run the independent-failure-domain incremental archive backup."""
        expected_policy = {
            "recovery_journal_retention_policy": (
                recovery_journal_retention_policy
            ),
            "recovery_journal_integrity_policy": (
                recovery_journal_integrity_policy
            ),
        }
        effective_trigger = (
            trigger_kind
            or (
                "manual"
                if job_config is not None and job_config.manual_only
                else "cron"
            )
        )
        principal_id = (
            operator_principal
            if effective_trigger != "cron"
            else "service:annual-report-asset-scheduler"
        )
        return await self._run_annual_report_asset_job(
            job_name="annual_report_asset_backup",
            runner=lambda: data_manager.run_annual_report_asset_backup(
                bounds=bounds,
                trigger_kind=effective_trigger,
                expected_recovery_policy=expected_policy,
                **({"principal_id": principal_id} if principal_id is not None else {}),
            ),
            job_config=job_config,
        )

    async def hkex_instrument_master_sync(
        self,
        mode: str = "audit_only",
        timeout_sec: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """手工触发 HKEX 主数据同步/审计任务。"""
        try:
            scheduler_logger.info(
                "[Scheduler] Starting HKEX instrument master sync: mode=%s timeout=%s",
                mode,
                timeout_sec,
            )
            normalized_mode = str(mode or "audit_only").strip().lower()
            if normalized_mode not in {"audit_only", "safe_write", "lifecycle_write"}:
                raise ValueError(f"unsupported HKEX instrument master sync mode: {normalized_mode}")
            result = await data_manager.run_master_governance([
                MasterGovernanceRequirement(
                    scope="hkex_instrument",
                    exchanges=["HKEX"],
                    instrument_types=["stock"],
                    mode=normalized_mode,
                    job_name="hkex_instrument_master_sync",
                    job_type="manual",
                    timeout_sec=timeout_sec,
                )
            ])
            hkex = (result.get("exchanges") or {}).get("HKEX", {})
            summary = result.get("summary") or {}
            source_usage = hkex.get("source_usage") or {}
            source_usage_text = ", ".join(
                f"{key}={value}" for key, value in source_usage.items()
            ) or "无"
            samples = hkex.get("review_required_samples") or []
            sample_lines = []
            for item in samples[:5]:
                local = item.get("local") or {}
                sample_lines.append(
                    f"- {item.get('instrument_id')}: {local.get('name', '')} "
                    f"({item.get('reason')})"
                )
            sample_text = "\n".join(sample_lines) if sample_lines else "无"

            content = (
                "*HKEX 主数据同步*\n\n"
                f"状态: `{result.get('status')}`\n"
                f"mode: `{result.get('mode')}`\n"
                f"active_count: `{summary.get('active_count', 0)}`\n"
                f"待复核: `{summary.get('review_required', 0)}`\n"
                f"safe_write候选: `{hkex.get('safe_write_preview_count', 0)}`\n"
                f"可复活候选: `{hkex.get('allowed_reactivation_count', 0)}`\n"
                f"可停牌候选: `{hkex.get('allowed_suspension_count', 0)}`\n"
                f"官方停牌证据: `{hkex.get('official_suspension_count', 0)}`\n"
                f"source_usage: `{source_usage_text}`\n"
                f"warnings: `{len(result.get('warnings') or [])}`，"
                f"errors: `{len(result.get('errors') or [])}`\n\n"
                "待复核样本:\n"
                f"{sample_text}"
            )
            await self._send_task_report(
                report_data={
                    "name": "HKEX 主数据同步",
                    "status": result.get("status"),
                    "content": content,
                    "result": result,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                report_type="maintenance_report",
                task_name="hkex_instrument_master_sync",
                job_config=job_config,
            )
            return result.get("status") in {"success", "warning"}
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] HKEX instrument master sync failed: {e}")
            if self.telegram_enabled:
                try:
                    await self.bot.send_task_notification(
                        f"HKEX 主数据同步失败: {e}",
                        "hkex_instrument_master_sync",
                        "error",
                    )
                except Exception as notify_error:
                    scheduler_logger.error(
                        f"[Scheduler] Failed to send HKEX master sync failure notification: {notify_error}"
            )
            return False

    async def a_share_stock_master_sync(
        self,
        exchanges: Optional[List[str]] = None,
        timeout_sec: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """手工触发 A 股股票主数据同步，不请求日更行情。"""
        if exchanges is None:
            exchanges = ["SSE", "SZSE", "BSE"]

        normalized_exchanges = [
            str(exchange).strip().upper()
            for exchange in exchanges
            if str(exchange).strip()
        ]
        if not normalized_exchanges:
            raise ValueError("A-share stock master sync requires at least one exchange")
        unsupported = sorted(set(normalized_exchanges) - {"SSE", "SZSE", "BSE"})
        if unsupported:
            raise ValueError(f"unsupported A-share stock master exchanges: {unsupported}")

        try:
            scheduler_logger.info(
                "[Scheduler] Starting A-share stock master sync: exchanges=%s timeout=%s",
                normalized_exchanges,
                timeout_sec,
            )
            result = await data_manager.run_master_governance([
                MasterGovernanceRequirement(
                    scope="a_share_stock",
                    exchanges=normalized_exchanges,
                    instrument_types=["stock"],
                    mode="force_refresh",
                    job_name="a_share_stock_master_sync",
                    job_type="manual",
                    timeout_sec=timeout_sec,
                )
            ])
            stage_policy = BacktestRolloutPolicy.load().stage("security_state_forward")
            maintenance = BacktestDataMaintenance(_quote_database_path())
            result.setdefault("backtest_stages", {})[
                "security_state_forward"
            ] = await _run_backtest_stage(
                "security_state_forward",
                maintenance.sync_security_state_from_instruments,
                exchanges=normalized_exchanges,
                max_rows=stage_policy.max_rows,
            )
            result["backtest_stages"][
                "security_state_announcements"
            ] = await _run_backtest_stage(
                "security_state_forward",
                maintenance.sync_security_events_from_announcements,
                config_manager.get_research_config().storage.db_path,
                max_rows=stage_policy.max_rows,
            )
            summary = result.get("summary") or {}
            source_authority = summary.get("source_authority") or {}
            source_authority_text = ", ".join(
                f"{key}={value}" for key, value in source_authority.items()
            ) or "无"

            exchange_lines = []
            exchange_results = result.get("exchanges") or {}
            for exchange in normalized_exchanges:
                item = exchange_results.get(exchange) or {}
                after = item.get("after") if isinstance(item.get("after"), dict) else {}
                source_usage = item.get("source_usage") or {}
                source_usage_text = ", ".join(
                    f"{key}={value}" for key, value in source_usage.items()
                ) or "无"
                exchange_lines.append(
                    f"- {exchange}: {item.get('status', 'unknown')} "
                    f"active={after.get('active_count', 0)} "
                    f"+{item.get('added_count', 0)}/-{item.get('deactivated_count', 0)} "
                    f"authority={item.get('source_authority') or 'unknown'} "
                    f"sources={source_usage_text}"
                )
            exchange_text = "\n".join(exchange_lines) if exchange_lines else "无"

            content = (
                "*A 股股票主数据同步*\n\n"
                f"状态: `{result.get('status')}`\n"
                f"市场: `{','.join(normalized_exchanges)}`\n"
                "mode: `force_refresh`\n"
                f"新增: `{summary.get('added_instruments', 0)}`\n"
                f"停用: `{summary.get('deactivated_instruments', 0)}`\n"
                f"活跃合计: `{summary.get('active_count', 0)}`\n"
                f"source_authority: `{source_authority_text}`\n"
                f"warnings: `{len(result.get('warnings') or [])}`，"
                f"errors: `{len(result.get('errors') or [])}`\n\n"
                "市场明细:\n"
                f"{exchange_text}"
            )
            await self._send_task_report(
                report_data={
                    "name": "A 股股票主数据同步",
                    "status": result.get("status"),
                    "content": content,
                    "result": result,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                report_type="maintenance_report",
                task_name="a_share_stock_master_sync",
                job_config=job_config,
            )
            return result.get("status") in {"success", "warning", "fresh"}
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] A-share stock master sync failed: {e}")
            if self.telegram_enabled:
                try:
                    await self.bot.send_task_notification(
                        f"A 股股票主数据同步失败: {e}",
                        "a_share_stock_master_sync",
                        "error",
                    )
                except Exception as notify_error:
                    scheduler_logger.error(
                        "[Scheduler] Failed to send A-share stock master sync failure notification: %s",
                        notify_error,
                    )
            return False

    async def index_master_governance_sync(
        self,
        exchanges: Optional[List[str]] = None,
        timeout_sec: Optional[int] = None,
        target_date: Optional[date] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """手工触发 A 股指数主数据治理，不请求日更行情。"""
        if exchanges is None:
            exchanges = ["SSE", "SZSE"]
        if target_date is None:
            target_date = date.today()

        try:
            scheduler_logger.info(
                "[Scheduler] Starting A-share index master governance sync: exchanges=%s target_date=%s timeout=%s",
                exchanges,
                target_date,
                timeout_sec,
            )
            result = await data_manager.run_master_governance([
                MasterGovernanceRequirement(
                    scope="a_share_index",
                    exchanges=exchanges,
                    instrument_types=["index"],
                    mode="force_refresh",
                    target_date=target_date,
                    job_name="index_master_governance_sync",
                    job_type="manual",
                    timeout_sec=timeout_sec,
                )
            ])
            stage_policy = BacktestRolloutPolicy.load().stage(
                "index_composition_forward"
            )
            parent_snapshots = list(
                result.get("index_composition_snapshots") or []
            )
            if parent_snapshots:
                index_stage = await _run_backtest_stage(
                    "index_composition_forward",
                    BacktestDataMaintenance(_quote_database_path()).ingest_index_snapshots,
                    parent_snapshots,
                    max_rows=stage_policy.max_rows,
                )
            elif stage_policy.enabled:
                index_stage = {
                    **_disabled_backtest_stage("index_composition_forward"),
                    "status": "unavailable",
                    "blockers": ["parent_output_has_no_composition_snapshots"],
                }
            else:
                index_stage = _disabled_backtest_stage(
                    "index_composition_forward"
                )
            result.setdefault("backtest_stages", {})[
                "index_composition_forward"
            ] = index_stage
            summary = result.get("summary") or {}
            samples = summary.get("samples") or []
            sample_lines = []
            for item in samples[:5]:
                sample_lines.append(
                    f"- {item.get('instrument_id')}: "
                    f"{item.get('state')} {item.get('confidence', '')}".strip()
                )
            sample_text = "\n".join(sample_lines) if sample_lines else "无"

            content = (
                "*A 股指数主数据治理*\n\n"
                f"状态: `{result.get('status')}`\n"
                f"市场: `{','.join(exchanges)}`\n"
                f"target_date: `{target_date.isoformat()}`\n"
                f"主数据写入: `{summary.get('master_rows_saved', 0)}`\n"
                f"证据写入: `{summary.get('evidence_rows_saved', 0)}`\n"
                f"活跃指数: `{summary.get('active_count', 0)}`\n"
                f"停编跳过: `{summary.get('lifecycle_skip_count', 0)}`\n"
                f"直接: `{summary.get('direct_terminated_count', 0)}`，"
                f"推断: `{summary.get('inferred_terminated_count', 0)}`，"
                f"invalid-quote: `{summary.get('invalid_quote_code_deactivated_count', 0)}`，"
                f"stale: `{summary.get('stale_no_quote_count', 0)}`\n"
                f"warnings: `{len(result.get('warnings') or [])}`，"
                f"errors: `{len(result.get('errors') or [])}`\n\n"
                "样例:\n"
                f"{sample_text}"
            )
            await self._send_task_report(
                report_data={
                    "name": "A 股指数主数据治理",
                    "status": result.get("status"),
                    "content": content,
                    "result": result,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                report_type="maintenance_report",
                task_name="index_master_governance_sync",
                job_config=job_config,
            )
            return result.get("status") in {"success", "warning", "fresh"}
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] A-share index master governance sync failed: {e}")
            if self.telegram_enabled:
                try:
                    await self.bot.send_task_notification(
                        f"A 股指数主数据治理失败: {e}",
                        "index_master_governance_sync",
                        "error",
                    )
                except Exception as notify_error:
                    scheduler_logger.error(
                        "[Scheduler] Failed to send index governance failure notification: %s",
                        notify_error,
                    )
            return False

    async def _send_task_report(self, report_data: dict, report_type: str,
                               task_name: str, job_config: Optional[JobConfig] = None) -> bool:
        """
        统一的任务报告发送方法

        Args:
            report_data: 报告数据
            report_type: 报告类型
            task_name: 任务名称
            job_config: JobConfig对象(任务配置对象)，取其report属性用于判断是否发送报告

        Returns:
            bool: 是否发送成功
        """
        try:
            if (
                isinstance(report_data.get('instrument_master_governance'), dict)
                and not report_data.get('instrument_master_governance_summary')
            ):
                report_data['instrument_master_governance_summary'] = (
                    _format_instrument_master_governance_summary(
                        report_data.get('instrument_master_governance')
                    )
                )

            # 检查是否应该发送报告
            should_send = False
            if job_config and hasattr(job_config, 'report'):
                should_send = job_config.report

            if not should_send or not self.telegram_enabled:
                scheduler_logger.debug(f"[Scheduler] Task {task_name} report disabled or Telegram not enabled")
                return False

            report_timeout_seconds = int(
                config_manager.get_nested(
                    'api_config.report_send_timeout_seconds',
                    45,
                )
                or 45
            )

            # Notification delivery must not block task lifecycle completion.
            summary_sent = await asyncio.wait_for(
                self.bot.send_report_notification({
                    'report_type': report_type,
                    'task_name': task_name,
                    **report_data  # 将所有报告数据传递下去
                }, report_type),
                timeout=report_timeout_seconds,
            )

            detail_failures = 0
            detail_messages = [
                str(item)
                for item in (report_data.get("detail_messages") or [])[:20]
                if str(item).strip()
            ]
            for index, detail_message in enumerate(detail_messages, start=1):
                try:
                    await asyncio.wait_for(
                        self.bot.send_data_notification(
                            detail_message,
                            level="warning",
                        ),
                        timeout=report_timeout_seconds,
                    )
                except Exception as exc:
                    detail_failures += 1
                    scheduler_logger.warning(
                        "[Scheduler] Task %s detail report %s/%s failed: %s",
                        task_name,
                        index,
                        len(detail_messages),
                        exc,
                    )

            scheduler_logger.info(
                "[Scheduler] Task %s report sent: summary=%s details=%s "
                "detail_failures=%s",
                task_name,
                bool(summary_sent),
                len(detail_messages),
                detail_failures,
            )
            return bool(summary_sent)

        except asyncio.TimeoutError:
            scheduler_logger.warning(
                "[Scheduler] Task %s report send timed out; task result is preserved",
                task_name,
            )
            return False

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send task report for {task_name}: {e}")
            return False

    async def hk_daily_data_update(self,
                                   exchanges: Optional[List[str]] = None,
                                   wait_for_market_close: bool = False,
                                   enable_trading_day_check: bool = True,
                                   per_instrument_timeout_sec: Optional[int] = None,
                                   progress_log_every: int = 200,
                                   progress_log_interval_sec: int = 300,
                                   job_config: Optional[JobConfig] = None) -> bool:
        """港股每日数据更新任务（委托至 daily_data_update）"""
        if exchanges is None:
            exchanges = ['HKEX']
        return await self.daily_data_update(
            exchanges=exchanges,
            wait_for_market_close=wait_for_market_close,
            enable_trading_day_check=enable_trading_day_check,
            per_instrument_timeout_sec=per_instrument_timeout_sec,
            progress_log_every=progress_log_every,
            progress_log_interval_sec=progress_log_interval_sec,
            master_governance_job_name='hk_daily_data_update',
            job_config=job_config
        )

    async def us_daily_data_update(self,
                                   exchanges: Optional[List[str]] = None,
                                   wait_for_market_close: bool = False,
                                   enable_trading_day_check: bool = True,
                                   per_instrument_timeout_sec: Optional[int] = None,
                                   progress_log_every: int = 500,
                                   progress_log_interval_sec: int = 300,
                                   job_config: Optional[JobConfig] = None) -> bool:
        """美股每日数据更新任务（委托至 daily_data_update）"""
        if exchanges is None:
            exchanges = ['NASDAQ', 'NYSE']
        return await self.daily_data_update(
            exchanges=exchanges,
            wait_for_market_close=wait_for_market_close,
            enable_trading_day_check=enable_trading_day_check,
            per_instrument_timeout_sec=per_instrument_timeout_sec,
            progress_log_every=progress_log_every,
            progress_log_interval_sec=progress_log_interval_sec,
            job_config=job_config
        )

    async def daily_data_update(self,
                            exchanges: Optional[List[str]] = None,
                            wait_for_market_close: bool = True,
                            market_close_delay_minutes: int = 15,
                            enable_trading_day_check: bool = True,
                            per_instrument_timeout_sec: Optional[int] = None,
                            progress_log_every: int = 200,
                            progress_log_interval_sec: int = 300,
                            instrument_types: Optional[List[str]] = None,
                            target_date: Optional[date] = None,
                            run_factor_audit: bool = True,
                            master_governance_job_name: str = 'daily_data_update',
                            job_config: Optional[JobConfig] = None) -> bool:
        """每日数据更新任务

        Args:
            target_date: 指定补数据的目标日期，为 None 时默认 date.today()
        """
        self._active_tasks.add('daily_data_update')
        try:
            # 使用配置参数或默认值
            if exchanges is None:
                exchanges = self.config.get_nested(
                    'data_config.market_presets.a_shares',
                    default=['SSE', 'SZSE', 'BSE']
                )

            today = target_date if target_date else date.today()
            is_backfill = target_date is not None and target_date != date.today()

            if is_backfill:
                scheduler_logger.info(f"[Scheduler] Starting BACKFILL data update for {today}...")
                # 补数据模式：跳过等待收盘和交易日检查
                wait_for_market_close = False
                enable_trading_day_check = False
            else:
                scheduler_logger.info("[Scheduler] Starting daily data update task...")

            trading_calendar_updates = {}

            # 步骤1: 更新每个交易所的交易日历
            scheduler_logger.info("[Scheduler] Step 1: Updating trading calendars...")
            for exchange in exchanges:
                try:
                    # 更新当日和未来一周的交易日历
                    start_date = today
                    end_date = today + timedelta(days=7)

                    scheduler_logger.info(f"[Scheduler] Updating trading calendar for {exchange} ({start_date} to {end_date})")
                    updated_count = await data_manager._update_trading_calendar(exchange, start_date, end_date)
                    trading_calendar_updates[exchange] = updated_count
                    scheduler_logger.info(f"[Scheduler] Updated {updated_count} trading days for {exchange}")

                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to update trading calendar for {exchange}: {e}")
                    trading_calendar_updates[exchange] = 0

            # 步骤2: 交易日检查
            trading_exchanges = []
            if enable_trading_day_check:
                scheduler_logger.info("[Scheduler] Step 2: Checking trading days...")

                for exchange in exchanges:
                    try:
                        is_trading = await data_manager.db_ops.is_trading_day(exchange, today)
                        if is_trading:
                            trading_exchanges.append(exchange)
                            scheduler_logger.info(f"[Scheduler] {exchange} is trading today")
                        else:
                            scheduler_logger.info(f"[Scheduler] {exchange} is not trading today")
                    except Exception as e:
                        scheduler_logger.warning(f"[Scheduler] Failed to check trading day for {exchange}: {e}")
                        # fallback to DateUtils
                        if DateUtils.is_trading_day(exchange, today):
                            trading_exchanges.append(exchange)
                            scheduler_logger.info(f"[Scheduler] {exchange} is trading today (fallback check)")

                if not trading_exchanges:
                    # 非交易日，使用报告系统发送通知
                    report_data = {
                        'name': '每日数据更新报告',
                        'status': 'info',
                        'non_trading_day': True,
                        'date': today.strftime('%Y-%m-%d'),
                        'trading_calendar_updates': trading_calendar_updates
                    }
                    await self._send_task_report(report_data, 'daily_update_report', '每日数据更新', job_config)
                    scheduler_logger.info("[Scheduler] Non-trading day, task finished.")
                    return False

            else:
                trading_exchanges = exchanges

            # 步骤3: 等待市场收盘
            if wait_for_market_close:
                scheduler_logger.info("[Scheduler] Step 3: Waiting for market close...")
                await self._wait_for_markets_close(trading_exchanges, market_close_delay_minutes)

            # 步骤4: 执行数据更新
            scheduler_logger.info("[Scheduler] Step 4: Executing data update...")
            update_results = await data_manager.update_daily_data(
                exchanges=trading_exchanges,
                target_date=today,
                per_instrument_timeout_sec=per_instrument_timeout_sec,
                progress_log_every=progress_log_every,
                progress_log_interval_sec=progress_log_interval_sec,
                instrument_types=instrument_types,
                run_factor_audit=run_factor_audit,
                master_governance_job_name=master_governance_job_name,
            )
            stage_policy = BacktestRolloutPolicy.load().stage(
                "daily_price_limits"
            )
            update_results.setdefault("backtest_stages", {})[
                "daily_price_limits"
            ] = await _run_backtest_stage(
                "daily_price_limits",
                BacktestDataMaintenance(_quote_database_path()).sync_source_reported_price_limits,
                start_date=today.isoformat(),
                end_date=today.isoformat(),
                dry_run=False,
                max_rows=stage_policy.max_rows,
            )

            # 步骤5: 发送报告
            # 判断更新状态
            success_count = update_results.get('success_count', 0)
            failure_count = update_results.get('failure_count', 0)
            total_quotes_added = update_results.get('total_quotes_added', 0)
            exchange_stats = update_results.get('exchange_stats') or {}
            total_instruments = sum(
                int(stats.get('total_instruments', stats.get('total_count', 0)) or 0)
                for stats in exchange_stats.values()
                if isinstance(stats, dict) and 'error' not in stats
            )
            no_op = (
                failure_count == 0
                and success_count == 0
                and total_quotes_added == 0
                and total_instruments > 0
            )
            if no_op:
                update_results['no_op'] = True
                update_results['no_op_reason'] = 'target_date_already_covered'
                update_results['summary_note'] = '目标日期已覆盖，无新增行情'
                update_results['success_rate'] = 100.0
            is_successful = failure_count == 0 and (success_count > 0 or no_op)

            report_data = {
                'name': '每日数据更新报告',
                'status': 'success' if is_successful else 'warning',  # 明确的成功/失败状态
                'date': today.strftime('%Y-%m-%d'),
                'trading_exchanges': trading_exchanges,
                'update_results': update_results,
                'trading_calendar_updates': trading_calendar_updates,
                'start_time': datetime.now().strftime('%H:%M:%S')
            }

            await self._send_task_report(
                report_data=report_data,
                report_type='daily_update_report',
                task_name='每日数据更新',
                job_config=job_config
            )

            scheduler_logger.info("[Scheduler] Daily data update completed successfully")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Daily data update failed: {e}")
            # 统一使用报告系统发送失败通知
            failure_report_data = {
                'name': '每日数据更新报告',
                'status': 'error',
                'error_message': str(e)
            }
            await self._send_task_report(
                report_data=failure_report_data,
                report_type='daily_update_report',
                task_name='每日数据更新',
                job_config=job_config
            )
            return False
        finally:
            self._active_tasks.discard('daily_data_update')

    async def a_share_daily_data_historical_backfill(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        scopes: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        scan_sources: bool = False,
        resume: bool = True,
        checkpoint_id: Optional[str] = None,
        chunk_size: int = 100,
        repair_universe_mode: str = 'historical_backfill',
        override_lifecycle_filter: bool = False,
        repair_universe_limit: Optional[int] = None,
        force_current_master_refresh: bool = True,
        repair_pending_factor_quotes: bool = False,
        index_instrument_ids: Optional[List[str]] = None,
        index_daily_request_reserve: int = 5000,
        index_sampling: str = 'daily',
        index_max_queries_per_run: int = 4000,
        index_checkpoint_path: Optional[str] = None,
        per_instrument_timeout_sec: Optional[int] = 30,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Run the dry-run-first governed A-share historical backfill pipeline."""
        task_id = 'a_share_daily_data_historical_backfill'
        self._active_tasks.add(task_id)
        result: Dict[str, Any] = {}
        try:
            parameters = normalize_a_share_backfill_parameters(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                scopes=scopes,
                instrument_ids=instrument_ids,
                dry_run=dry_run,
                scan_sources=scan_sources,
                resume=resume,
                chunk_size=chunk_size,
                repair_universe_mode=repair_universe_mode,
                override_lifecycle_filter=override_lifecycle_filter,
                force_current_master_refresh=force_current_master_refresh,
                repair_pending_factor_quotes=repair_pending_factor_quotes,
            )
            if repair_universe_limit is not None:
                repair_universe_limit = int(repair_universe_limit)
                if repair_universe_limit < 1:
                    raise ValueError('repair_universe_limit must be positive')
            if per_instrument_timeout_sec is not None:
                per_instrument_timeout_sec = int(per_instrument_timeout_sec)
                if per_instrument_timeout_sec < 1:
                    raise ValueError('per_instrument_timeout_sec must be positive')
            normalized_index_ids = [
                item.upper() for item in normalize_string_list(index_instrument_ids)
            ] or list(SUPPORTED_INDEXES)
            index_daily_request_reserve = int(index_daily_request_reserve)
            if index_daily_request_reserve < 0:
                raise ValueError('index_daily_request_reserve must not be negative')
            index_sampling = str(index_sampling or 'daily').strip().lower()
            if index_sampling not in {'daily', 'monthly'}:
                raise ValueError('index_sampling must be daily or monthly')
            index_max_queries_per_run = int(index_max_queries_per_run)
            if index_max_queries_per_run < 1 or index_max_queries_per_run > 4500:
                raise ValueError('index_max_queries_per_run must be between 1 and 4500')

            checkpoint_parameters = dict(parameters)
            checkpoint_parameters.update({
                'repair_universe_limit': repair_universe_limit,
                'per_instrument_timeout_sec': per_instrument_timeout_sec,
            })
            if 'index_composition' in parameters['scopes']:
                checkpoint_parameters.update({
                    'index_instrument_ids': normalized_index_ids,
                    'index_daily_request_reserve': index_daily_request_reserve,
                    'index_sampling': index_sampling,
                    'index_max_queries_per_run': index_max_queries_per_run,
                })
            data_dir = data_manager.data_config.get('data_dir', 'data')
            checkpoint_store = AShareBackfillCheckpointStore(data_dir)
            resolved_checkpoint_id = checkpoint_store.resolve_id(
                checkpoint_parameters,
                checkpoint_id,
                prefer_existing=parameters['resume'] and not parameters['dry_run'],
            )
            checkpoint = None
            if parameters['resume'] and not parameters['dry_run']:
                checkpoint = checkpoint_store.load(
                    resolved_checkpoint_id,
                    checkpoint_parameters,
                )

            result = {
                'status': (
                    'scan_only'
                    if parameters['scan_sources']
                    else ('dry_run' if parameters['dry_run'] else 'success')
                ),
                'operation': task_id,
                'dry_run': parameters['dry_run'],
                'scan_sources': parameters['scan_sources'],
                'checkpoint_id': resolved_checkpoint_id,
                'checkpoint_path': str(checkpoint_store.path_for(resolved_checkpoint_id)),
                'resumed': bool(checkpoint),
                'parameters': serialize_checkpoint_parameters(checkpoint_parameters),
                'stages': {},
                'blockers': [],
                'warnings': [],
                'errors': [],
                'failure_samples': [],
            }
            selected_optional_scopes = sorted(
                set(parameters['scopes']) & set(A_SHARE_BACKFILL_OPTIONAL_SCOPES)
            )
            for optional_scope in selected_optional_scopes:
                if optional_scope == 'index_composition':
                    continue
                result['stages'][optional_scope] = {
                    'status': 'unavailable',
                    'reuse_decision': 'extend_existing',
                    'inherited_scope': {
                        'start_date': parameters['start_date'].isoformat(),
                        'end_date': parameters['end_date'].isoformat(),
                        'exchanges': list(parameters['exchanges']),
                        'instrument_ids': list(parameters['instrument_ids']),
                    },
                    'provider_usage': [],
                    'network_requests': 0,
                    'inserted': 0,
                    'changed': 0,
                    'unchanged': 0,
                    'watermark': None,
                    'blockers': ['historical_source_not_proven_by_bounded_probe'],
                }
            if 'index_composition' in selected_optional_scopes:
                resolved_index_checkpoint_path = index_checkpoint_path or str(
                    Path(data_dir)
                    / 'backfill_checkpoints'
                    / f"{resolved_checkpoint_id}_index_composition.json"
                )
                index_stage = await _run_index_constituent_history_stage(
                    start_date=parameters['start_date'],
                    end_date=parameters['end_date'],
                    index_instrument_ids=normalized_index_ids,
                    daily_request_reserve=index_daily_request_reserve,
                    sampling=index_sampling,
                    max_queries_per_run=index_max_queries_per_run,
                    checkpoint_path=resolved_index_checkpoint_path,
                    dry_run=parameters['dry_run'],
                    resume=parameters['resume'],
                )
                result['stages']['index_composition'] = index_stage
                if index_stage.get('status') in {
                    'partial', 'blocked', 'unavailable', 'failed'
                }:
                    result['blockers'].extend(
                        f"index_composition:{item}"
                        for item in (index_stage.get('blockers') or [])
                    )
                result['failure_samples'].extend(
                    {
                        'instrument_id': item.get('unit_id', 'index_composition'),
                        'reason': item.get('reason', 'failed'),
                    }
                    for item in (index_stage.get('failures') or [])[:10]
                    if isinstance(item, dict)
                )
            selected_default_scopes = (
                set(parameters['scopes']) & set(A_SHARE_BACKFILL_DEFAULT_SCOPES)
            )
            if selected_optional_scopes and not selected_default_scopes:
                stage_statuses = [
                    str((result['stages'].get(scope) or {}).get('status') or 'unavailable')
                    for scope in selected_optional_scopes
                ]
                if len(stage_statuses) == 1:
                    result['status'] = stage_statuses[0]
                elif any(status in {'blocked', 'failed', 'error'} for status in stage_statuses):
                    result['status'] = 'blocked'
                elif any(status in {'success', 'dry_run'} for status in stage_statuses):
                    result['status'] = 'partial'
                else:
                    result['status'] = 'unavailable'
                if self.telegram_enabled:
                    await self._send_task_report(
                        report_data={
                            'name': 'A 股历史全量回补',
                            'status': result['status'],
                            'content': _format_a_share_historical_backfill_report(result),
                            'result': result,
                        },
                        report_type='maintenance_report',
                        task_name=task_id,
                        job_config=job_config,
                    )
                return result

            # Stage 1: optional current-master refresh. A resumed checkpoint
            # already owns a frozen universe and must not silently replace it.
            if checkpoint and checkpoint.get('universe'):
                master_stage = {
                    'status': 'skipped',
                    'reason': 'resume_uses_frozen_universe',
                }
            elif 'master' not in parameters['scopes']:
                master_stage = {'status': 'skipped', 'reason': 'scope_not_selected'}
            elif parameters['dry_run']:
                master_stage = {
                    'status': 'dry_run',
                    'reason': 'current_master_refresh_planned',
                }
            elif parameters['force_current_master_refresh']:
                master_stage = await data_manager._run_repair_current_master_refresh(
                    job_name=task_id,
                    exchanges=parameters['exchanges'],
                    instrument_types=['stock'],
                    target_date=parameters['end_date'],
                    scopes=['a_share_stock'],
                )
                if str(master_stage.get('status') or '').lower() in {
                    'failed', 'error', 'blocked'
                }:
                    result['blockers'].append('master_governance_failed')
            else:
                master_stage = {
                    'status': 'skipped',
                    'reason': 'current_master_refresh_disabled',
                }
            result['stages']['master'] = master_stage
            master_blocked = 'master_governance_failed' in result['blockers']

            if checkpoint and checkpoint.get('universe'):
                frozen_universe = list(checkpoint.get('universe') or [])
                universe_diagnostics = dict(
                    checkpoint.get('universe_diagnostics') or {}
                )
                candidate_universe_ids = list(
                    checkpoint.get('candidate_universe_ids')
                    or [item.get('instrument_id') for item in frozen_universe]
                )
            else:
                frozen_universe = []
                universe_diagnostics = {}
                candidate_universe_ids = []
                requested_ids = set(parameters['instrument_ids'])
                for exchange in parameters['exchanges']:
                    raw_instruments = await data_manager.db_ops.get_repair_universe_instruments(
                        exchange,
                        instrument_types=['stock'],
                    )
                    selected_candidates = [
                        item for item in raw_instruments
                        if not requested_ids
                        or item.get('instrument_id') in requested_ids
                    ]
                    if repair_universe_limit is not None:
                        selected_candidates = selected_candidates[:repair_universe_limit]
                    candidate_universe_ids.extend(
                        item.get('instrument_id')
                        for item in selected_candidates
                        if item.get('instrument_id')
                    )
                    governed, diagnostics = await data_manager.filter_repair_universe(
                        raw_instruments,
                        start_date=parameters['start_date'],
                        end_date=parameters['end_date'],
                        mode=parameters['repair_universe_mode'],
                        instrument_ids=parameters['instrument_ids'],
                        override_lifecycle_filter=parameters['override_lifecycle_filter'],
                        limit=repair_universe_limit,
                    )
                    universe_diagnostics[exchange] = diagnostics
                    for instrument in governed:
                        frozen_universe.append({
                            'instrument_id': instrument['instrument_id'],
                            'symbol': instrument['symbol'],
                            'exchange': exchange,
                            'start_date': (
                                data_manager._date_from_any(
                                    instrument.get('_repair_start_date')
                                ) or parameters['start_date']
                            ).isoformat(),
                            'end_date': (
                                data_manager._date_from_any(
                                    instrument.get('_repair_end_date')
                                ) or parameters['end_date']
                            ).isoformat(),
                        })
                result['universe_diagnostics'] = universe_diagnostics

            result['universe_diagnostics'] = universe_diagnostics

            lifecycle_reason_distribution: Dict[str, int] = {}
            lifecycle_samples: List[Dict[str, Any]] = []
            lifecycle_skipped = 0
            degraded_fallbacks = 0
            for diagnostics in universe_diagnostics.values():
                lifecycle_skipped += int(
                    diagnostics.get('skipped_instrument_count', 0) or 0
                )
                degraded_fallbacks += int(
                    diagnostics.get('degraded_fallback_count', 0) or 0
                )
                for reason, count in (
                    diagnostics.get('reason_distribution') or {}
                ).items():
                    lifecycle_reason_distribution[reason] = (
                        lifecycle_reason_distribution.get(reason, 0)
                        + int(count or 0)
                    )
                lifecycle_samples.extend(
                    item
                    for item in (diagnostics.get('samples') or [])
                    if item.get('reason') == 'inactive_without_lifecycle_boundary'
                )
                lifecycle_samples.extend(
                    diagnostics.get('degraded_fallback_samples') or []
                )
            unresolved_lifecycle = int(
                lifecycle_reason_distribution.get(
                    'inactive_without_lifecycle_boundary', 0
                ) or 0
            )
            result['lifecycle_completeness'] = {
                'status': (
                    'partial'
                    if unresolved_lifecycle or degraded_fallbacks
                    else 'success'
                ),
                'totals': {
                    'candidate_instruments': len(set(candidate_universe_ids)),
                    'eligible_instruments': len(frozen_universe),
                    'skipped_instruments': lifecycle_skipped,
                    'unresolved_lifecycle_instruments': unresolved_lifecycle,
                    'degraded_lifecycle_fallbacks': degraded_fallbacks,
                },
                'reason_distribution': lifecycle_reason_distribution,
                'samples': lifecycle_samples[:20],
            }

            result['universe'] = {
                'instrument_count': len(frozen_universe),
                'by_exchange': {
                    exchange: sum(
                        1 for item in frozen_universe if item.get('exchange') == exchange
                    )
                    for exchange in parameters['exchanges']
                },
            }
            if not frozen_universe:
                result['blockers'].append('historical_repair_universe_empty')
            universe_blocked = not frozen_universe

            if checkpoint is None:
                checkpoint = checkpoint_store.initialize(
                    resolved_checkpoint_id,
                    checkpoint_parameters,
                    frozen_universe,
                )
            checkpoint['universe_diagnostics'] = universe_diagnostics
            checkpoint['candidate_universe_ids'] = sorted(set(candidate_universe_ids))
            checkpoint.setdefault('stages', {})['master'] = master_stage
            if not parameters['dry_run']:
                checkpoint_store.save(checkpoint)

            def completed_chunks(stage_name: str) -> set[str]:
                stage_state = checkpoint.setdefault('stages', {}).setdefault(stage_name, {})
                return set(stage_state.get('completed_chunks') or [])

            def mark_chunk_complete(stage_name: str, chunk_id_value: str) -> None:
                stage_state = checkpoint.setdefault('stages', {}).setdefault(stage_name, {})
                completed = set(stage_state.get('completed_chunks') or [])
                completed.add(chunk_id_value)
                stage_state['completed_chunks'] = sorted(completed)
                if not parameters['dry_run']:
                    checkpoint_store.save(checkpoint)

            def iter_chunks() -> List[tuple[str, str, List[Dict[str, Any]]]]:
                chunks = []
                for exchange in parameters['exchanges']:
                    exchange_items = [
                        item for item in frozen_universe
                        if item.get('exchange') == exchange
                    ]
                    for offset in range(0, len(exchange_items), parameters['chunk_size']):
                        chunk = exchange_items[offset: offset + parameters['chunk_size']]
                        identity = hashlib.sha1(
                            ','.join(item['instrument_id'] for item in chunk).encode('utf-8')
                        ).hexdigest()[:12]
                        chunks.append((exchange, f"{exchange}:{offset // parameters['chunk_size']}:{identity}", chunk))
                return chunks

            chunks = iter_chunks()

            # Stage 2: refresh when selected, always validate when quotes need it.
            blocked_exchanges = set()
            calendar_items = {}
            calendar_required = 'calendar' in parameters['scopes'] or 'quotes' in parameters['scopes']
            if calendar_required:
                for exchange in parameters['exchanges']:
                    effective_start = max(
                        parameters['start_date'],
                        A_SHARE_EXCHANGE_INCEPTION[exchange],
                    )
                    update_error = None
                    updated_rows = 0
                    if 'calendar' in parameters['scopes'] and not parameters['dry_run']:
                        try:
                            updated_rows = await data_manager._update_trading_calendar(
                                exchange,
                                effective_start,
                                parameters['end_date'],
                            )
                        except Exception as exc:
                            update_error = str(exc)
                    records = await data_manager.db_ops.get_trading_calendar_records(
                        exchange,
                        effective_start,
                        parameters['end_date'],
                    )
                    coverage = evaluate_calendar_coverage(
                        exchange,
                        parameters['start_date'],
                        parameters['end_date'],
                        records,
                    )
                    coverage['updated_rows'] = int(updated_rows or 0)
                    if update_error:
                        coverage['update_error'] = update_error
                        result['warnings'].append(
                            f"{exchange} calendar refresh failed: {update_error}"
                        )
                    if coverage['status'] == 'blocked':
                        blocked_exchanges.add(exchange)
                        result['blockers'].append(
                            f"{exchange}:calendar_coverage_incomplete:{coverage['missing_days']}"
                        )
                    calendar_items[exchange] = coverage
                calendar_status = 'blocked' if blocked_exchanges else (
                    'dry_run' if parameters['dry_run'] else 'success'
                )
                calendar_stage = {
                    'status': calendar_status,
                    'exchanges': calendar_items,
                    'counters': {
                        'exchanges': len(calendar_items),
                        'blocked_exchanges': len(blocked_exchanges),
                        'missing_days': sum(
                            int(item.get('missing_days', 0) or 0)
                            for item in calendar_items.values()
                        ),
                    },
                }
            else:
                calendar_stage = {'status': 'skipped', 'reason': 'scope_not_selected'}
            result['stages']['calendar'] = calendar_stage
            checkpoint.setdefault('stages', {})['calendar'] = calendar_stage
            if not parameters['dry_run']:
                checkpoint_store.save(checkpoint)

            # Stage 3: bounded quote chunks. Existing range-backfill defaults are
            # untouched; the new optional filters are supplied only here.
            quote_totals = {
                'chunks_total': sum(1 for exchange, _, _ in chunks if exchange not in blocked_exchanges),
                'chunks_completed': 0,
                'chunks_resumed': 0,
                'success_count': 0,
                'failure_count': 0,
                'quotes_added': 0,
            }
            if 'quotes' not in parameters['scopes']:
                quote_stage = {'status': 'skipped', 'reason': 'scope_not_selected', 'totals': quote_totals}
            elif parameters['dry_run']:
                quote_stage = {'status': 'dry_run', 'totals': quote_totals}
            elif master_blocked or universe_blocked:
                quote_stage = {
                    'status': 'blocked',
                    'reason': (
                        'master_governance_failed'
                        if master_blocked
                        else 'historical_repair_universe_empty'
                    ),
                    'totals': quote_totals,
                }
            elif blocked_exchanges:
                runnable = [item for item in chunks if item[0] not in blocked_exchanges]
                if not runnable:
                    quote_stage = {
                        'status': 'blocked',
                        'reason': 'calendar_coverage_incomplete',
                        'totals': quote_totals,
                    }
                else:
                    quote_stage = {'status': 'partial', 'totals': quote_totals}
            else:
                quote_stage = {'status': 'success', 'totals': quote_totals}

            if (
                'quotes' in parameters['scopes']
                and not parameters['dry_run']
                and not master_blocked
                and not universe_blocked
            ):
                completed = completed_chunks('quotes')
                for exchange, chunk_identity, chunk in chunks:
                    if exchange in blocked_exchanges:
                        continue
                    if chunk_identity in completed:
                        quote_totals['chunks_resumed'] += 1
                        quote_totals['chunks_completed'] += 1
                        continue
                    chunk_result = await data_manager.update_daily_data_range(
                        exchanges=[exchange],
                        start_date=parameters['start_date'],
                        end_date=parameters['end_date'],
                        per_instrument_timeout_sec=per_instrument_timeout_sec,
                        instrument_types=['stock'],
                        run_factor_audit=False,
                        repair_universe_mode=parameters['repair_universe_mode'],
                        override_lifecycle_filter=parameters['override_lifecycle_filter'],
                        instrument_ids=[item['instrument_id'] for item in chunk],
                        instrument_date_ranges={
                            item['instrument_id']: {
                                'start_date': item['start_date'],
                                'end_date': item['end_date'],
                            }
                            for item in chunk
                        },
                        sync_adjustment_factors='factors' in parameters['scopes'],
                        factor_sync_reason='historical_backfill',
                        force_current_master_refresh=False,
                    )
                    quote_totals['success_count'] += int(chunk_result.get('success_count', 0) or 0)
                    quote_totals['failure_count'] += int(chunk_result.get('failure_count', 0) or 0)
                    quote_totals['quotes_added'] += int(chunk_result.get('total_quotes_added', 0) or 0)
                    exchange_stats = chunk_result.get('exchange_stats') or {}
                    chunk_failed = (
                        int(chunk_result.get('failure_count', 0) or 0) > 0
                        or bool(chunk_result.get('error'))
                        or any(
                            isinstance(item, dict) and bool(item.get('error'))
                            for item in exchange_stats.values()
                        )
                    )
                    if not chunk_failed:
                        mark_chunk_complete('quotes', chunk_identity)
                        quote_totals['chunks_completed'] += 1
                    else:
                        quote_stage['status'] = 'partial'
                        result['failure_samples'].append({
                            'exchange': exchange,
                            'reason': 'quote_chunk_failed',
                            'chunk_id': chunk_identity,
                        })
            result['stages']['quotes'] = quote_stage

            # Stages 4/5 share one TDX request pass. Raw events are saved first;
            # factor derivation updates only events with prior-close evidence.
            corporate_selected = bool({'dividends', 'factors'} & set(parameters['scopes']))
            corporate_totals = {
                'chunks_total': len(chunks),
                'chunks_completed': 0,
                'chunks_resumed': 0,
                'raw_events': 0,
                'saved_events': 0,
                'existing_events_refreshed': 0,
                'derived_factors': 0,
                'pending_factors': 0,
                'empty_instruments': 0,
                'timeouts': 0,
                'errors': 0,
            }
            corporate_status = 'skipped'
            if corporate_selected:
                corporate_status = (
                    'scan_only'
                    if parameters['scan_sources']
                    else ('dry_run' if parameters['dry_run'] else 'success')
                )
                if (
                    (not parameters['dry_run'] or parameters['scan_sources'])
                    and (master_blocked or universe_blocked)
                ):
                    corporate_status = 'blocked'
            if (
                corporate_selected
                and (not parameters['dry_run'] or parameters['scan_sources'])
                and not master_blocked
                and not universe_blocked
            ):
                completed = (
                    set()
                    if parameters['scan_sources']
                    else completed_chunks('corporate_actions')
                )
                for exchange, chunk_identity, chunk in chunks:
                    if chunk_identity in completed:
                        corporate_totals['chunks_resumed'] += 1
                        corporate_totals['chunks_completed'] += 1
                        continue
                    chunk_result = await data_manager.backfill_tdx_xdxr_history(
                        exchanges=[exchange],
                        start_date=parameters['start_date'],
                        end_date=parameters['end_date'],
                        instrument_ids=[item['instrument_id'] for item in chunk],
                        instrument_date_ranges={
                            item['instrument_id']: {
                                'start_date': item['start_date'],
                                'end_date': item['end_date'],
                            }
                            for item in chunk
                        },
                        derive_factors='factors' in parameters['scopes'],
                        repair_universe_mode=parameters['repair_universe_mode'],
                        override_lifecycle_filter=parameters['override_lifecycle_filter'],
                        per_instrument_timeout_sec=per_instrument_timeout_sec,
                        dry_run=parameters['dry_run'],
                    )
                    totals = chunk_result.get('totals') or {}
                    for key in (
                        'raw_events', 'saved_events', 'existing_events_refreshed',
                        'derived_factors', 'pending_factors', 'empty_instruments',
                        'timeouts', 'errors',
                    ):
                        corporate_totals[key] += int(totals.get(key, 0) or 0)
                    successful_chunk_statuses = (
                        {'success', 'dry_run'}
                        if parameters['scan_sources']
                        else {'success'}
                    )
                    if chunk_result.get('status') in successful_chunk_statuses:
                        if not parameters['scan_sources']:
                            mark_chunk_complete('corporate_actions', chunk_identity)
                        corporate_totals['chunks_completed'] += 1
                    else:
                        corporate_status = 'partial'
                        result['failure_samples'].extend(
                            (chunk_result.get('samples') or [])[:5]
                        )

            result['stages']['dividends'] = (
                {'status': corporate_status, 'totals': corporate_totals}
                if 'dividends' in parameters['scopes']
                else {'status': 'skipped', 'reason': 'scope_not_selected'}
            )
            result['stages']['factors'] = (
                {'status': corporate_status, 'totals': corporate_totals}
                if 'factors' in parameters['scopes']
                else {'status': 'skipped', 'reason': 'scope_not_selected'}
            )

            pending_summary: Dict[str, Any] = {
                'status': 'skipped',
                'totals': {
                    'pending_factors': 0,
                    'pending_instruments': 0,
                    'pending_cash_events': 0,
                },
                'instrument_ids': [],
                'samples': [],
            }
            pending_repair_stage: Dict[str, Any] = {
                'status': 'skipped',
                'reason': (
                    'repair_not_requested'
                    if not parameters['repair_pending_factor_quotes']
                    else 'not_applicable'
                ),
            }
            reconciliation: Dict[str, Any] = {
                'status': 'skipped',
                'totals': {},
            }
            completeness_stage: Dict[str, Any] = {
                'status': 'skipped',
                'reason': 'corporate_action_scope_not_selected',
                'totals': {},
                'reasons': [],
                'samples': [],
            }

            if corporate_selected and parameters['dry_run']:
                completeness_stage = {
                    'status': (
                        'scan_only' if parameters['scan_sources'] else 'dry_run'
                    ),
                    'totals': {
                        'pending_factors': corporate_totals['pending_factors'],
                        **result['lifecycle_completeness']['totals'],
                    },
                    'reasons': [],
                    'samples': result['lifecycle_completeness']['samples'][:20],
                }
            elif corporate_selected and (master_blocked or universe_blocked):
                completeness_stage = {
                    'status': 'blocked',
                    'reason': (
                        'master_governance_failed'
                        if master_blocked
                        else 'historical_repair_universe_empty'
                    ),
                    'totals': result['lifecycle_completeness']['totals'],
                    'reasons': ['corporate_action_completeness_not_evaluated'],
                    'samples': result['lifecycle_completeness']['samples'][:20],
                }
            elif corporate_selected:
                completeness_ids = sorted(set(candidate_universe_ids))
                pending_summary = (
                    await data_manager.get_tdx_xdxr_pending_factor_summary(
                        start_date=parameters['start_date'],
                        end_date=parameters['end_date'],
                        instrument_ids=completeness_ids,
                    )
                )

                if parameters['repair_pending_factor_quotes']:
                    pending_ids = list(pending_summary.get('instrument_ids') or [])
                    if not pending_ids:
                        pending_repair_stage = {
                            'status': 'success',
                            'reason': 'no_pending_factors',
                            'totals': {
                                'target_instruments': 0,
                                'remaining_pending_factors': 0,
                            },
                        }
                    else:
                        quote_repair = await data_manager.run_delisted_a_share_quote_backfill(
                            exchanges=parameters['exchanges'],
                            instrument_ids=pending_ids,
                            dry_run=False,
                            per_instrument_timeout_sec=per_instrument_timeout_sec,
                        )
                        frozen_by_id = {
                            item['instrument_id']: item for item in frozen_universe
                        }
                        repair_ranges = {
                            instrument_id: {
                                'start_date': frozen_by_id[instrument_id]['start_date'],
                                'end_date': frozen_by_id[instrument_id]['end_date'],
                            }
                            for instrument_id in pending_ids
                            if instrument_id in frozen_by_id
                        }
                        repair_exchanges = sorted({
                            frozen_by_id[instrument_id]['exchange']
                            for instrument_id in pending_ids
                            if instrument_id in frozen_by_id
                        }) or parameters['exchanges']
                        redrive_result = await data_manager.backfill_tdx_xdxr_history(
                            exchanges=repair_exchanges,
                            start_date=parameters['start_date'],
                            end_date=parameters['end_date'],
                            instrument_ids=pending_ids,
                            instrument_date_ranges=repair_ranges,
                            derive_factors=True,
                            repair_universe_mode=parameters['repair_universe_mode'],
                            override_lifecycle_filter=parameters['override_lifecycle_filter'],
                            per_instrument_timeout_sec=per_instrument_timeout_sec,
                            dry_run=False,
                        )
                        pending_summary = (
                            await data_manager.get_tdx_xdxr_pending_factor_summary(
                                start_date=parameters['start_date'],
                                end_date=parameters['end_date'],
                                instrument_ids=completeness_ids,
                            )
                        )
                        quote_failures = int(
                            quote_repair.get('failure_count', 0) or 0
                        )
                        redrive_totals = redrive_result.get('totals') or {}
                        repair_failed = (
                            quote_failures > 0
                            or int(redrive_totals.get('errors', 0) or 0) > 0
                            or int(redrive_totals.get('timeouts', 0) or 0) > 0
                            or int(
                                pending_summary.get('totals', {}).get(
                                    'pending_factors', 0
                                ) or 0
                            ) > 0
                        )
                        pending_repair_stage = {
                            'status': 'partial' if repair_failed else 'success',
                            'totals': {
                                'target_instruments': len(pending_ids),
                                'quote_target_instruments': int(
                                    quote_repair.get('target_count', 0) or 0
                                ),
                                'quote_rows_saved': int(
                                    quote_repair.get('saved_rows', 0) or 0
                                ),
                                'quote_failures': quote_failures,
                                'redriven_factors': int(
                                    redrive_totals.get('derived_factors', 0) or 0
                                ),
                                'remaining_pending_factors': int(
                                    pending_summary.get('totals', {}).get(
                                        'pending_factors', 0
                                    ) or 0
                                ),
                            },
                            'quote_result': quote_repair,
                            'redrive_result': redrive_result,
                        }

                reconciliation = await data_manager.reconcile_tdx_xdxr_history(
                    start_date=parameters['start_date'],
                    end_date=parameters['end_date'],
                    instrument_ids=completeness_ids,
                )
                result['warnings'].extend(reconciliation.get('warnings') or [])
                pending_totals = pending_summary.get('totals') or {}
                corporate_totals['pending_factors_detected'] = int(
                    corporate_totals.get('pending_factors', 0) or 0
                )
                corporate_totals['pending_factors'] = int(
                    pending_totals.get('pending_factors', 0) or 0
                )
                reconciliation_totals = reconciliation.get('totals') or {}
                lifecycle_totals = result['lifecycle_completeness']['totals']
                completeness_reasons = []
                if int(pending_totals.get('pending_factors', 0) or 0) > 0:
                    completeness_reasons.append('pending_factors')
                if int(
                    lifecycle_totals.get('unresolved_lifecycle_instruments', 0)
                    or 0
                ) > 0:
                    completeness_reasons.append('unresolved_lifecycle_boundaries')
                if int(
                    lifecycle_totals.get('degraded_lifecycle_fallbacks', 0)
                    or 0
                ) > 0:
                    completeness_reasons.append('degraded_lifecycle_fallbacks')
                if corporate_totals['errors'] or corporate_totals['timeouts']:
                    completeness_reasons.append('provider_failures')
                if int(
                    reconciliation_totals.get('reference_factor_change_only', 0)
                    or reconciliation_totals.get('reference_only_events', 0)
                    or 0
                ) > 0:
                    completeness_reasons.append(
                        'reference_factor_changes_unmatched'
                    )
                if int(
                    reconciliation_totals.get('factor_conflicts', 0) or 0
                ) > 0:
                    completeness_reasons.append('factor_conflicts')
                if reconciliation.get('status') == 'unavailable':
                    completeness_reasons.append('reference_evidence_unavailable')

                completeness_samples = []
                completeness_samples.extend(pending_summary.get('samples') or [])
                completeness_samples.extend(
                    reconciliation.get('reference_factor_change_only_samples')
                    or reconciliation.get('reference_only_samples')
                    or []
                )
                completeness_samples.extend(
                    reconciliation.get('factor_conflict_samples') or []
                )
                completeness_samples.extend(
                    result['lifecycle_completeness'].get('samples') or []
                )
                completeness_stage = {
                    'status': 'partial' if completeness_reasons else 'success',
                    'totals': {
                        'persisted_tdx_events': int(
                            reconciliation_totals.get('tdx_events', 0) or 0
                        ),
                        **pending_totals,
                        **lifecycle_totals,
                        'reference_factor_changes': int(
                            reconciliation_totals.get(
                                'reference_factor_changes',
                                reconciliation_totals.get('reference_events', 0),
                            ) or 0
                        ),
                        'exact_factor_matches': int(
                            reconciliation_totals.get('exact_factor_matches', 0)
                            or 0
                        ),
                        'shifted_factor_matches': int(
                            reconciliation_totals.get('shifted_factor_matches', 0)
                            or 0
                        ),
                        'factor_conflicts': int(
                            reconciliation_totals.get('factor_conflicts', 0) or 0
                        ),
                        'reference_factor_change_only': int(
                            reconciliation_totals.get(
                                'reference_factor_change_only',
                                reconciliation_totals.get('reference_only_events', 0),
                            ) or 0
                        ),
                        'tdx_event_only': int(
                            reconciliation_totals.get(
                                'tdx_event_only',
                                reconciliation_totals.get('tdx_only_events', 0),
                            ) or 0
                        ),
                        'calendar_unavailable_instruments': int(
                            reconciliation_totals.get(
                                'calendar_unavailable_instruments', 0
                            ) or 0
                        ),
                    },
                    'reasons': completeness_reasons,
                    'pending': pending_summary,
                    'reconciliation': reconciliation,
                    'lifecycle': result['lifecycle_completeness'],
                    'samples': completeness_samples[:20],
                }

            result['stages']['pending_quote_repair'] = pending_repair_stage
            result['stages']['completeness'] = completeness_stage

            checkpoint.setdefault('stages', {})['quotes_summary'] = quote_stage
            checkpoint.setdefault('stages', {})['corporate_actions_summary'] = {
                'status': corporate_status,
                'totals': corporate_totals,
            }
            checkpoint.setdefault('stages', {})['pending_quote_repair_summary'] = {
                'status': pending_repair_stage.get('status'),
                'reason': pending_repair_stage.get('reason'),
                'totals': pending_repair_stage.get('totals') or {},
            }
            checkpoint.setdefault('stages', {})['completeness_summary'] = {
                'status': completeness_stage.get('status'),
                'totals': completeness_stage.get('totals') or {},
                'reasons': completeness_stage.get('reasons') or [],
            }
            if not parameters['dry_run']:
                checkpoint_store.save(checkpoint)

            if parameters['scan_sources']:
                if result['blockers'] or corporate_status == 'blocked':
                    result['status'] = 'blocked'
                elif corporate_status == 'partial':
                    result['status'] = 'partial'
                else:
                    result['status'] = 'scan_only'
            elif parameters['dry_run']:
                result['status'] = 'dry_run'
            elif result['blockers']:
                result['status'] = 'blocked'
            elif any(
                str((result['stages'].get(name) or {}).get('status')).lower()
                in {'partial', 'failed', 'error'}
                for name in (
                    'master', 'calendar', 'quotes', 'dividends', 'factors',
                    'index_composition', 'security_state', 'price_limits',
                    'corporate_actions', 'pending_quote_repair', 'completeness',
                )
            ):
                result['status'] = 'partial'
            elif any(
                str((result['stages'].get(name) or {}).get('status')).lower()
                == 'unavailable'
                for name in A_SHARE_BACKFILL_OPTIONAL_SCOPES
                if name in parameters['scopes']
            ):
                result['status'] = 'partial'
            else:
                result['status'] = 'success'
            result['failure_samples'] = result['failure_samples'][:20]
            result['warnings'] = result['warnings'][:50]
            result['errors'] = result['errors'][:50]

            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        'name': 'A 股历史全量回补',
                        'status': result['status'],
                        'content': _format_a_share_historical_backfill_report(result),
                        'result': result,
                    },
                    report_type='maintenance_report',
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] A-share historical backfill failed: %s",
                exc,
            )
            failure = {
                'status': 'failed',
                'operation': task_id,
                'dry_run': bool(dry_run),
                'scan_sources': bool(scan_sources),
                'checkpoint_id': checkpoint_id,
                'error': str(exc),
                'stages': result.get('stages', {}) if isinstance(result, dict) else {},
                'blockers': [],
                'warnings': [],
                'errors': [str(exc)],
                'failure_samples': [],
                'parameters': result.get('parameters', {}) if isinstance(result, dict) else {},
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        'name': 'A 股历史全量回补',
                        'status': 'failed',
                        'content': _format_a_share_historical_backfill_report(failure),
                        'result': failure,
                    },
                    report_type='maintenance_report',
                    task_name=task_id,
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_adjustment_factor_rebuild(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        source: str = 'akshare',
        dry_run: bool = True,
        resume: bool = True,
        chunk_size: int = 100,
        request_interval_seconds: float = 1.0,
        checkpoint_id: Optional[str] = None,
        build_canonical: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Reject the obsolete AkShare-era rebuild entry point."""

        raise RuntimeError(
            "a_share_adjustment_factor_rebuild is deprecated; use "
            "a_share_cninfo_adjustment_factor_rebuild, "
            "a_share_canonical_adjustment_factor_selection, and "
            "a_share_canonical_adjustment_factor_promotion"
        )

    async def a_share_corporate_action_validation(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        reference_sources: Optional[List[str]] = None,
        scan_official_announcements: bool = True,
        official_sample_limit: int = 50,
        official_lookback_years: int = 3,
        field_tolerance: float = 0.0001,
        acceptable_cumulative_error_pct: float = 0.1,
        warning_cumulative_error_pct: float = 0.5,
        per_source_timeout_sec: int = 60,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Run the manual read-only A-share corporate-action validation."""
        task_id = 'a_share_corporate_action_validation'
        self._active_tasks.add(task_id)
        try:
            normalized_start = coerce_date(start_date, field_name='start_date')
            normalized_end = coerce_date(end_date, field_name='end_date')
            normalized_exchanges = [
                item.upper() for item in normalize_string_list(exchanges)
            ] or ['SSE', 'SZSE', 'BSE']
            normalized_ids = [
                item.upper() for item in normalize_string_list(instrument_ids)
            ]
            normalized_sources = [
                item.lower() for item in normalize_string_list(reference_sources)
            ] or ['baostock', 'akshare']
            scan_official = (
                scan_official_announcements
                if isinstance(scan_official_announcements, bool)
                else str(scan_official_announcements).strip().lower()
                in {'1', 'true', 'yes', 'y', 'on'}
            )
            result = await data_manager.validate_a_share_corporate_actions(
                start_date=normalized_start,
                end_date=normalized_end,
                exchanges=normalized_exchanges,
                instrument_ids=normalized_ids,
                reference_sources=normalized_sources,
                scan_official_announcements=scan_official,
                official_sample_limit=int(official_sample_limit),
                official_lookback_years=int(official_lookback_years),
                field_tolerance=float(field_tolerance),
                acceptable_cumulative_error_pct=float(
                    acceptable_cumulative_error_pct
                ),
                warning_cumulative_error_pct=float(
                    warning_cumulative_error_pct
                ),
                per_source_timeout_sec=int(per_source_timeout_sec),
                sample_limit=int(sample_limit),
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        'name': 'A 股公司行动多源验证',
                        'status': result.get('status'),
                        'content': _format_a_share_corporate_action_validation_report(
                            result
                        ),
                        'result': result,
                    },
                    report_type='maintenance_report',
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] A-share corporate-action validation failed: %s",
                exc,
            )
            failure = {
                'status': 'failed',
                'operation': task_id,
                'read_only': True,
                'error': str(exc),
                'errors': [str(exc)],
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        'name': 'A 股公司行动多源验证',
                        'status': 'failed',
                        'content': _format_a_share_corporate_action_validation_report(
                            failure
                        ),
                        'result': failure,
                    },
                    report_type='maintenance_report',
                    task_name=task_id,
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_corporate_action_backfill(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        scopes: Optional[List[str]] = None,
        dry_run: bool = True,
        resume: bool = True,
        chunk_size: int = 50,
        request_interval_seconds: float = 1.0,
        per_instrument_timeout_sec: int = 60,
        checkpoint_id: Optional[str] = None,
        active_only: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Run the manual official CNInfo corporate-action backfill."""
        task_id = "a_share_cninfo_corporate_action_backfill"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.backfill_a_share_cninfo_corporate_actions(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                scopes=scopes,
                dry_run=bool(dry_run),
                resume=bool(resume),
                chunk_size=int(chunk_size),
                request_interval_seconds=float(request_interval_seconds),
                per_instrument_timeout_sec=int(per_instrument_timeout_sec),
                checkpoint_id=checkpoint_id,
                active_only=bool(active_only),
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股巨潮官方公司行动回补",
                        "status": result.get("status"),
                        "content": _format_a_share_cninfo_corporate_action_report(
                            result
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] A-share CNInfo corporate-action backfill failed: %s",
                exc,
            )
            failure = {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "error": str(exc),
                "errors": [str(exc)],
                "production_isolation": True,
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股巨潮官方公司行动回补",
                        "status": "failed",
                        "content": _format_a_share_cninfo_corporate_action_report(
                            failure
                        ),
                        "result": failure,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_canonical_corporate_action_history_backfill(
        self,
        dry_run: bool = True,
        batch_size: int = 500,
        resume: bool = True,
        checkpoint_id: Optional[str] = None,
        instrument_ids: Optional[List[str]] = None,
        source_event_keys: Optional[List[str]] = None,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Project existing historical evidence without provider acquisition."""
        task_id = "a_share_canonical_corporate_action_history_backfill"
        self._active_tasks.add(task_id)
        try:
            executor = CanonicalCorporateActionHistoryBackfill(
                _quote_database_path(), logger=scheduler_logger
            )
            stop_requested = threading.Event()
            worker = asyncio.create_task(
                asyncio.to_thread(
                    executor.run,
                    dry_run=bool(dry_run),
                    batch_size=int(batch_size),
                    resume=bool(resume),
                    checkpoint_id=checkpoint_id,
                    instrument_ids=instrument_ids,
                    source_event_keys=source_event_keys,
                    should_stop=stop_requested.is_set,
                )
            )
            try:
                result = await asyncio.shield(worker)
            except asyncio.CancelledError:
                stop_requested.set()
                try:
                    await worker
                except Exception:
                    scheduler_logger.exception(
                        "[Scheduler] Canonical history worker failed while stopping"
                    )
                raise
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "Canonical corporate-action history projection",
                        "status": result.get("status"),
                        "content": _format_canonical_corporate_action_history_report(
                            result
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Canonical corporate-action history backfill failed: %s",
                exc,
            )
            failure = {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "provider_usage": [],
                "network_requests": 0,
                "error": str(exc),
                "errors": [str(exc)],
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "Canonical corporate-action history projection",
                        "status": "failed",
                        "content": _format_canonical_corporate_action_history_report(
                            failure
                        ),
                        "result": failure,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_special_action_discovery(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        max_events: int = 500,
        target_offset: int = 0,
        window_before_days: int = 30,
        window_after_days: int = 30,
        max_window_days: int = 180,
        max_anchor_gap_days: int = 60,
        page_size: int = 30,
        max_pages: int = 5,
        request_interval_seconds: float = 0.5,
        per_event_timeout_sec: int = 60,
        classify_titles_with_llm: bool = True,
        title_classification_profile: str = "corporate_action_title_classification",
        title_max_titles_per_request: int = 80,
        title_max_concurrency: int = 50,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Discover official announcement candidates for missing-date events."""
        task_id = "a_share_cninfo_special_action_discovery"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.discover_cninfo_special_action_effective_dates(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                dry_run=bool(dry_run),
                max_events=int(max_events),
                target_offset=int(target_offset),
                window_before_days=int(window_before_days),
                window_after_days=int(window_after_days),
                max_window_days=int(max_window_days),
                max_anchor_gap_days=int(max_anchor_gap_days),
                page_size=int(page_size),
                max_pages=int(max_pages),
                request_interval_seconds=float(request_interval_seconds),
                per_event_timeout_sec=int(per_event_timeout_sec),
                classify_titles_with_llm=bool(classify_titles_with_llm),
                title_classification_profile=title_classification_profile,
                title_max_titles_per_request=int(
                    title_max_titles_per_request
                ),
                title_max_concurrency=int(title_max_concurrency),
                sample_limit=int(sample_limit),
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股巨潮特殊公司行动公告发现",
                        "status": result.get("status"),
                        "content": _format_cninfo_special_action_discovery_report(
                            result
                        ),
                        "detail_messages": _format_cninfo_problem_detail_messages(
                            result,
                            title="CNInfo 特殊公司行动异常明细",
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] CNInfo special-action discovery failed: %s", exc
            )
            return {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "production_isolation": True,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_corporate_action_llm_resolution(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        source_event_keys: Optional[List[str]] = None,
        max_events: int = 100,
        target_offset: int = 0,
        profile: str = "semantic_extraction",
        resume: bool = True,
        dry_run: bool = True,
        download_documents: bool = True,
        run_ocr: bool = False,
        refresh_documents: bool = False,
        discover_candidates: bool = False,
        auto_promote_validated: bool = True,
        exclude_reviewed_events: bool = False,
        pipeline: Optional[Dict[str, Any]] = None,
        pipeline_mode: Optional[str] = None,
        pipeline_download_concurrency: Optional[int] = None,
        pipeline_document_parse_concurrency: Optional[int] = None,
        pipeline_llm_concurrency: Optional[int] = None,
        pipeline_llm_requests_per_minute: Optional[int] = None,
        pipeline_progress_interval_seconds: Optional[float] = None,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Analyze CNInfo documents and auto-promote only governed results."""
        task_id = "a_share_cninfo_corporate_action_llm_resolution"
        self._active_tasks.add(task_id)
        try:
            scheduler_logger.info(
                "[Scheduler] Starting CNInfo corporate-action LLM resolution: "
                "range=%s..%s exchanges=%s instruments=%s events=%s max_events=%s "
                "offset=%s profile=%s resume=%s dry_run=%s download_documents=%s "
                "run_ocr=%s refresh_documents=%s discover_candidates=%s "
                "auto_promote_validated=%s exclude_reviewed_events=%s "
                "pipeline_mode=%s pipeline_llm_concurrency=%s "
                "pipeline_llm_requests_per_minute=%s",
                start_date,
                end_date,
                exchanges,
                instrument_ids,
                source_event_keys,
                max_events,
                target_offset,
                profile,
                resume,
                dry_run,
                download_documents,
                run_ocr,
                refresh_documents,
                discover_candidates,
                auto_promote_validated,
                exclude_reviewed_events,
                pipeline_mode,
                pipeline_llm_concurrency,
                pipeline_llm_requests_per_minute,
            )
            effective_pipeline = dict(pipeline or {})
            pipeline_overrides = {
                "mode": pipeline_mode,
                "download_concurrency": pipeline_download_concurrency,
                "document_parse_concurrency": (
                    pipeline_document_parse_concurrency
                ),
                "llm_concurrency": pipeline_llm_concurrency,
                "llm_requests_per_minute": pipeline_llm_requests_per_minute,
                "progress_interval_seconds": (
                    pipeline_progress_interval_seconds
                ),
            }
            effective_pipeline.update({
                key: value for key, value in pipeline_overrides.items()
                if value is not None
            })
            result = await data_manager.analyze_cninfo_corporate_action_candidates(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                source_event_keys=source_event_keys,
                max_events=int(max_events),
                target_offset=int(target_offset),
                profile=profile,
                resume=bool(resume),
                dry_run=bool(dry_run),
                download_documents=bool(download_documents),
                run_ocr=bool(run_ocr),
                refresh_documents=bool(refresh_documents),
                discover_candidates=bool(discover_candidates),
                auto_promote_validated=bool(auto_promote_validated),
                exclude_reviewed_events=bool(exclude_reviewed_events),
                pipeline=effective_pipeline,
                sample_limit=int(sample_limit),
            )
            if not dry_run:
                result.setdefault("backtest_stages", {})[
                    "canonical_corporate_actions"
                ] = await _run_backtest_stage(
                    "canonical_corporate_actions",
                    CanonicalCorporateActionProjector(
                        _quote_database_path()
                    ).project,
                    instrument_ids=instrument_ids,
                    source_event_keys=source_event_keys,
                )
            scheduler_logger.info(
                "[Scheduler] CNInfo corporate-action LLM resolution completed: "
                "status=%s targets=%s processed=%s analyzed=%s validated=%s "
                "auto_eligible=%s auto_promoted=%s auto_skipped=%s auto_failed=%s "
                "machine_rework=%s quick_review=%s deep_review=%s remaining_review=%s "
                "budget_overruns=%s document_failures=%s errors=%s next_offset=%s",
                result.get("status"),
                (result.get("targets") or {}).get("batch_events", 0),
                (result.get("counts") or {}).get("processed", 0),
                (result.get("counts") or {}).get("analyzed", 0),
                (result.get("counts") or {}).get("validated_candidates", 0),
                (result.get("auto_promotion") or {}).get("eligible", 0),
                (result.get("auto_promotion") or {}).get("promoted", 0),
                (result.get("auto_promotion") or {}).get("skipped", 0),
                (result.get("auto_promotion") or {}).get("failed", 0),
                ((result.get("review_workload") or {}).get("tiers") or {}).get(
                    "machine_rework", 0
                ),
                ((result.get("review_workload") or {}).get("tiers") or {}).get(
                    "quick_review", 0
                ),
                ((result.get("review_workload") or {}).get("tiers") or {}).get(
                    "deep_review", 0
                ),
                (result.get("review_workload") or {}).get(
                    "remaining_manual_review", 0
                ),
                (result.get("llm_metrics") or {}).get(
                    "provider_output_budget_overruns", 0
                ),
                (result.get("counts") or {}).get("document_failures", 0),
                (result.get("counts") or {}).get("errors", 0),
                (result.get("targets") or {}).get("next_target_offset"),
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股巨潮公司行动公告正文解析",
                        "status": result.get("status"),
                        "content": _format_cninfo_corporate_action_llm_report(result),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception("[Scheduler] CNInfo corporate-action LLM resolution failed: %s", exc)
            return {
                "status": "failed", "operation": task_id, "dry_run": bool(dry_run),
                "production_isolation": True,
                "raw_observation_modified": False,
                "production_factor_modified": False,
                "error": str(exc), "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_corporate_action_llm_incremental(
        self,
        lookback_days: int = 14,
        exchanges: Optional[List[str]] = None,
        max_events: int = 100,
        profile: str = "semantic_extraction",
        resume: bool = True,
        dry_run: bool = True,
        download_documents: bool = True,
        run_ocr: bool = False,
        refresh_documents: bool = False,
        auto_promote_validated: bool = True,
        exclude_reviewed_events: bool = True,
        window_before_days: int = 30,
        window_after_days: int = 30,
        max_window_days: int = 180,
        max_anchor_gap_days: int = 60,
        page_size: int = 30,
        max_pages: int = 5,
        request_interval_seconds: float = 0.5,
        per_event_timeout_sec: int = 60,
        classify_titles_with_llm: bool = True,
        title_classification_profile: str = "corporate_action_title_classification",
        title_max_titles_per_request: int = 80,
        title_max_concurrency: int = 50,
        pipeline: Optional[Dict[str, Any]] = None,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Default-disabled daily unresolved-event governance entry point."""
        end = get_shanghai_time().date()
        start = end - timedelta(days=max(1, int(lookback_days)))
        return await self.a_share_cninfo_corporate_action_resolution_governance(
            start_date=start,
            end_date=end,
            exchanges=exchanges or ["SSE", "SZSE"],
            scopes=["inventory", "discovery", "resolution"],
            max_events=max_events,
            target_offset=0,
            profile=profile,
            resume=resume,
            dry_run=dry_run,
            download_documents=download_documents,
            run_ocr=run_ocr,
            refresh_documents=refresh_documents,
            auto_promote_validated=auto_promote_validated,
            exclude_reviewed_events=exclude_reviewed_events,
            window_before_days=window_before_days,
            window_after_days=window_after_days,
            max_window_days=max_window_days,
            max_anchor_gap_days=max_anchor_gap_days,
            page_size=page_size,
            max_pages=max_pages,
            request_interval_seconds=request_interval_seconds,
            per_event_timeout_sec=per_event_timeout_sec,
            classify_titles_with_llm=classify_titles_with_llm,
            title_classification_profile=title_classification_profile,
            title_max_titles_per_request=title_max_titles_per_request,
            title_max_concurrency=title_max_concurrency,
            pipeline=pipeline,
            sample_limit=sample_limit,
            job_config=job_config,
        )

    async def a_share_cninfo_corporate_action_resolution_governance(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        source_event_keys: Optional[List[str]] = None,
        scopes: Optional[List[str]] = None,
        max_events: int = 100,
        target_offset: int = 0,
        profile: str = "semantic_extraction",
        resume: bool = True,
        dry_run: bool = True,
        download_documents: bool = True,
        run_ocr: bool = False,
        refresh_documents: bool = False,
        auto_promote_validated: bool = True,
        exclude_reviewed_events: bool = True,
        retry_evidence_unavailable: bool = False,
        window_before_days: int = 30,
        window_after_days: int = 30,
        max_window_days: int = 180,
        max_anchor_gap_days: int = 60,
        page_size: int = 30,
        max_pages: int = 5,
        request_interval_seconds: float = 0.5,
        per_event_timeout_sec: int = 60,
        classify_titles_with_llm: bool = True,
        title_classification_profile: str = "corporate_action_title_classification",
        title_max_titles_per_request: int = 80,
        title_max_concurrency: int = 50,
        pipeline: Optional[Dict[str, Any]] = None,
        pipeline_mode: Optional[str] = None,
        pipeline_download_concurrency: Optional[int] = None,
        pipeline_document_parse_concurrency: Optional[int] = None,
        pipeline_llm_concurrency: Optional[int] = None,
        pipeline_llm_requests_per_minute: Optional[int] = None,
        pipeline_progress_interval_seconds: Optional[float] = None,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Run bounded full-market CNInfo unresolved-date governance."""
        task_id = "a_share_cninfo_corporate_action_resolution_governance"
        self._active_tasks.add(task_id)
        try:
            effective_pipeline = dict(pipeline or {})
            pipeline_overrides = {
                "mode": pipeline_mode,
                "download_concurrency": pipeline_download_concurrency,
                "document_parse_concurrency": (
                    pipeline_document_parse_concurrency
                ),
                "llm_concurrency": pipeline_llm_concurrency,
                "llm_requests_per_minute": pipeline_llm_requests_per_minute,
                "progress_interval_seconds": (
                    pipeline_progress_interval_seconds
                ),
            }
            effective_pipeline.update({
                key: value for key, value in pipeline_overrides.items()
                if value is not None
            })
            result = await data_manager.govern_cninfo_corporate_action_resolutions(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                source_event_keys=source_event_keys,
                scopes=scopes,
                max_events=int(max_events),
                target_offset=int(target_offset),
                profile=profile,
                resume=bool(resume),
                dry_run=bool(dry_run),
                download_documents=bool(download_documents),
                run_ocr=bool(run_ocr),
                refresh_documents=bool(refresh_documents),
                auto_promote_validated=bool(auto_promote_validated),
                exclude_reviewed_events=bool(exclude_reviewed_events),
                retry_evidence_unavailable=bool(retry_evidence_unavailable),
                window_before_days=int(window_before_days),
                window_after_days=int(window_after_days),
                max_window_days=int(max_window_days),
                max_anchor_gap_days=int(max_anchor_gap_days),
                page_size=int(page_size),
                max_pages=int(max_pages),
                request_interval_seconds=float(request_interval_seconds),
                per_event_timeout_sec=int(per_event_timeout_sec),
                classify_titles_with_llm=bool(classify_titles_with_llm),
                title_classification_profile=title_classification_profile,
                title_max_titles_per_request=int(
                    title_max_titles_per_request
                ),
                title_max_concurrency=int(title_max_concurrency),
                pipeline=effective_pipeline,
                sample_limit=int(sample_limit),
            )
            if not dry_run:
                result.setdefault("backtest_stages", {})[
                    "canonical_corporate_actions"
                ] = await _run_backtest_stage(
                    "canonical_corporate_actions",
                    CanonicalCorporateActionProjector(
                        _quote_database_path()
                    ).project,
                    instrument_ids=instrument_ids,
                    source_event_keys=source_event_keys,
                )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股 CNInfo 公司行动日期闭环治理",
                        "status": result.get("status"),
                        "content": _format_cninfo_resolution_governance_report(result),
                        "detail_messages": _format_cninfo_problem_detail_messages(
                            result,
                            title="CNInfo 公司行动治理异常明细",
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] CNInfo resolution governance failed: %s", exc
            )
            return {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "production_isolation": True,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_corporate_action_resolution_reset(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        source_event_keys: Optional[List[str]] = None,
        include_unanchored: bool = False,
        dry_run: bool = True,
        confirm_reset: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Preview or execute the bounded non-resolved governance reset."""
        task_id = "a_share_cninfo_corporate_action_resolution_reset"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.reset_cninfo_corporate_action_resolution_governance(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                source_event_keys=source_event_keys,
                include_unanchored=bool(include_unanchored),
                dry_run=bool(dry_run),
                confirm_reset=bool(confirm_reset),
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股 CNInfo 公司行动治理数据重置",
                        "status": result.get("status"),
                        "content": _format_cninfo_resolution_reset_report(result),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] CNInfo resolution reset failed: %s", exc
            )
            return {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "confirmed": bool(confirm_reset),
                "production_isolation": True,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_adjustment_factor_rebuild(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        build_canonical: bool = False,
        series_version: str = "a_share_cninfo_primary_v1",
        field_tolerance: float = 0.0001,
        factor_relative_tolerance: float = 0.001,
        max_session_shift: int = 3,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Run the manual CNInfo-primary factor rebuild and reconciliation."""
        task_id = "a_share_cninfo_adjustment_factor_rebuild"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.rebuild_cninfo_primary_adjustment_factors(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                dry_run=bool(dry_run),
                build_canonical=bool(build_canonical),
                series_version=series_version,
                field_tolerance=float(field_tolerance),
                factor_relative_tolerance=float(factor_relative_tolerance),
                max_session_shift=int(max_session_shift),
                sample_limit=int(sample_limit),
            )
            if not dry_run:
                result.setdefault("backtest_stages", {})[
                    "canonical_corporate_actions"
                ] = await _run_backtest_stage(
                    "canonical_corporate_actions",
                    CanonicalCorporateActionProjector(
                        _quote_database_path()
                    ).project,
                    instrument_ids=instrument_ids,
                )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股 CNInfo 主线复权因子重建",
                        "status": result.get("status"),
                        "content": _format_cninfo_primary_factor_report(result),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] CNInfo-primary factor rebuild failed: %s", exc
            )
            return {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "production_isolation": True,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_canonical_adjustment_factor_selection(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        build_canonical: bool = True,
        series_version: str = "a_share_cninfo_primary_v1",
        field_tolerance: float = 0.0001,
        factor_relative_tolerance: float = 0.001,
        max_session_shift: int = 3,
        sample_limit: int = 20,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Build a local CNInfo/TDX/BaoStock-Sina staging candidate."""
        task_id = "a_share_canonical_adjustment_factor_selection"
        self._active_tasks.add(task_id)
        parameters = {
            "start_date": start_date,
            "end_date": end_date,
            "exchanges": exchanges or ["SSE", "SZSE"],
            "instrument_ids": instrument_ids or [],
            "dry_run": bool(dry_run),
            "build_canonical": bool(build_canonical),
            "series_version": series_version,
            "field_tolerance": float(field_tolerance),
            "factor_relative_tolerance": float(factor_relative_tolerance),
            "max_session_shift": int(max_session_shift),
            "sample_limit": int(sample_limit),
        }
        try:
            selection_result = (
                await data_manager.rebuild_cninfo_primary_adjustment_factors(
                    start_date=start_date,
                    end_date=end_date,
                    exchanges=exchanges,
                    instrument_ids=instrument_ids,
                    dry_run=bool(dry_run),
                    build_canonical=bool(build_canonical),
                    series_version=series_version,
                    source_selection_mode="three_source",
                    field_tolerance=float(field_tolerance),
                    factor_relative_tolerance=float(
                        factor_relative_tolerance
                    ),
                    max_session_shift=int(max_session_shift),
                    sample_limit=int(sample_limit),
                )
            )
            selection_status = str(
                selection_result.get("status") or "failed"
            ).lower()
            if dry_run:
                status = (
                    "partial"
                    if selection_status in {"failed", "partial"}
                    else "dry_run"
                )
            else:
                status = (
                    "failed"
                    if selection_status == "failed"
                    else "partial"
                    if selection_status == "partial"
                    else "success"
                )
            result = {
                "status": status,
                "operation": task_id,
                "dry_run": bool(dry_run),
                "production_isolation": True,
                "parameters": parameters,
                "selection": selection_result,
                "promoted": False,
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股三源主复权因子候选选择",
                        "status": status,
                        "content": (
                            _format_a_share_canonical_factor_selection_report(
                                result
                            )
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] A-share canonical factor selection failed: %s",
                exc,
            )
            return {
                "status": "failed",
                "operation": task_id,
                "dry_run": bool(dry_run),
                "production_isolation": True,
                "parameters": parameters,
                "promoted": False,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_canonical_adjustment_factor_promotion(
        self,
        staging_series_version: Optional[str] = None,
        target_series_version: str = "a_share_cninfo_primary_v1",
        action: str = "promote",
        activate_reads: bool = True,
        dry_run: bool = True,
        confirm: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Explicitly validate/promote/activate or roll back canonical reads."""

        task_id = "a_share_canonical_adjustment_factor_promotion"
        self._active_tasks.add(task_id)
        try:
            result = (
                await data_manager
                .promote_a_share_canonical_adjustment_factor_candidate(
                    staging_series_version=staging_series_version,
                    target_series_version=target_series_version,
                    action=action,
                    activate_reads=bool(activate_reads),
                    dry_run=bool(dry_run),
                    confirm=bool(confirm),
                )
            )
            if not dry_run:
                result.setdefault("backtest_stages", {})[
                    "canonical_corporate_actions"
                ] = await _run_backtest_stage(
                    "canonical_corporate_actions",
                    CanonicalCorporateActionProjector(
                        _quote_database_path()
                    ).project,
                )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股主复权因子发布与回滚",
                        "status": result.get("status"),
                        "content": (
                            _format_a_share_canonical_factor_promotion_report(
                                result
                            )
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] A-share canonical factor promotion failed: %s",
                exc,
            )
            failure = {
                "status": "failed",
                "operation": task_id,
                "action": str(action or "promote"),
                "dry_run": bool(dry_run),
                "confirmed": bool(confirm),
                "parameters": {
                    "staging_series_version": staging_series_version,
                    "target_series_version": target_series_version,
                    "activate_reads": bool(activate_reads),
                },
                "errors": [str(exc)],
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股主复权因子发布与回滚",
                        "status": "failed",
                        "content": (
                            _format_a_share_canonical_factor_promotion_report(
                                failure
                            )
                        ),
                        "result": failure,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_canonical_adjustment_factor_storage_maintenance(
        self,
        operation: str = "migrate_decisions",
        series_versions: Optional[List[str]] = None,
        keep_recent_staging: int = 2,
        keep_recent_benchmarks: int = 5,
        endpoint_status_retention_days: int = 90,
        dry_run: bool = True,
        confirm: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Preview or apply canonical metadata migration and retention."""

        task_id = "a_share_canonical_adjustment_factor_storage_maintenance"
        self._active_tasks.add(task_id)
        try:
            result = (
                await data_manager
                .maintain_a_share_canonical_adjustment_factor_storage(
                    operation=operation,
                    series_versions=series_versions,
                    keep_recent_staging=int(keep_recent_staging),
                    keep_recent_benchmarks=int(keep_recent_benchmarks),
                    endpoint_status_retention_days=int(
                        endpoint_status_retention_days
                    ),
                    dry_run=bool(dry_run),
                    confirm=bool(confirm),
                )
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股主复权因子存储维护",
                        "status": result.get("status"),
                        "content": _format_a_share_canonical_storage_report(
                            result
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Canonical factor storage maintenance failed: %s",
                exc,
            )
            failure = {
                "status": "failed",
                "operation": task_id,
                "maintenance_operation": operation,
                "dry_run": bool(dry_run),
                "confirmed": bool(confirm),
                "errors": [str(exc)],
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股主复权因子存储维护",
                        "status": "failed",
                        "content": _format_a_share_canonical_storage_report(
                            failure
                        ),
                        "result": failure,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_cninfo_corporate_action_daily_sync(
        self,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        rolling_days: int = 7,
        announcement_schedule_mode: str = "trading_day",
        announcement_overlap_days: int = 2,
        announcement_page_size: int = 30,
        announcement_max_pages: int = 240,
        event_lookahead_days: int = 14,
        candidate_limit: int = 1000,
        safety_sweep_size: int = 100,
        tdx_refresh_mode: str = "targeted",
        tdx_rotating_sample_size: int = 100,
        request_interval_seconds: float = 0.5,
        per_instrument_timeout_sec: int = 60,
        build_canonical: bool = False,
        maintain_promoted_canonical: bool = True,
        series_version: str = "a_share_cninfo_primary_v1",
        anomaly_llm_enabled: bool = True,
        anomaly_llm_max_events: int = 50,
        anomaly_llm_profile: str = "semantic_extraction",
        anomaly_llm_download_documents: bool = True,
        anomaly_llm_run_ocr: bool = False,
        anomaly_llm_auto_promote_validated: bool = True,
        anomaly_llm_title_max_concurrency: int = 50,
        anomaly_llm_pipeline_mode: str = "async",
        anomaly_llm_pipeline_llm_concurrency: int = 50,
        anomaly_llm_pipeline_download_concurrency: int = 8,
        anomaly_llm_pipeline_document_parse_concurrency: int = 8,
        anomaly_llm_pipeline_progress_interval_seconds: float = 30.0,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Refresh incremental CNInfo candidates and affected factor paths."""
        task_id = "a_share_cninfo_corporate_action_daily_sync"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.maintain_a_share_cninfo_primary_factors(
                start_date=start_date,
                end_date=end_date,
                exchanges=exchanges,
                instrument_ids=instrument_ids,
                rolling_days=int(rolling_days),
                announcement_schedule_mode=announcement_schedule_mode,
                announcement_overlap_days=int(announcement_overlap_days),
                announcement_page_size=int(announcement_page_size),
                announcement_max_pages=int(announcement_max_pages),
                event_lookahead_days=int(event_lookahead_days),
                candidate_limit=int(candidate_limit),
                safety_sweep_size=int(safety_sweep_size),
                tdx_refresh_mode=tdx_refresh_mode,
                tdx_rotating_sample_size=int(tdx_rotating_sample_size),
                request_interval_seconds=float(request_interval_seconds),
                per_instrument_timeout_sec=int(per_instrument_timeout_sec),
                build_canonical=bool(build_canonical),
                maintain_promoted_canonical=bool(
                    maintain_promoted_canonical
                ),
                series_version=series_version,
                anomaly_llm_enabled=bool(anomaly_llm_enabled),
                anomaly_llm_max_events=int(anomaly_llm_max_events),
                anomaly_llm_profile=anomaly_llm_profile,
                anomaly_llm_download_documents=bool(
                    anomaly_llm_download_documents
                ),
                anomaly_llm_run_ocr=bool(anomaly_llm_run_ocr),
                anomaly_llm_auto_promote_validated=bool(
                    anomaly_llm_auto_promote_validated
                ),
                anomaly_llm_title_max_concurrency=int(
                    anomaly_llm_title_max_concurrency
                ),
                anomaly_llm_pipeline_mode=anomaly_llm_pipeline_mode,
                anomaly_llm_pipeline_llm_concurrency=int(
                    anomaly_llm_pipeline_llm_concurrency
                ),
                anomaly_llm_pipeline_download_concurrency=int(
                    anomaly_llm_pipeline_download_concurrency
                ),
                anomaly_llm_pipeline_document_parse_concurrency=int(
                    anomaly_llm_pipeline_document_parse_concurrency
                ),
                anomaly_llm_pipeline_progress_interval_seconds=float(
                    anomaly_llm_pipeline_progress_interval_seconds
                ),
            )
            result.setdefault("backtest_stages", {})[
                "canonical_corporate_actions"
            ] = await _run_backtest_stage(
                "canonical_corporate_actions",
                CanonicalCorporateActionProjector(
                    _quote_database_path()
                ).project,
                instrument_ids=instrument_ids,
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data={
                        "name": "A 股 CNInfo/TDX 公司行动日更",
                        "status": result.get("status"),
                        "content": _format_cninfo_primary_factor_report(result),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] CNInfo corporate-action daily sync failed: %s", exc
            )
            return {
                "status": "failed",
                "operation": task_id,
                "production_isolation": True,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def a_share_tdx_corporate_action_weekly_full_refresh(
        self,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        exchanges: Optional[List[str]] = None,
        per_instrument_timeout_sec: int = 60,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Refresh the full TDX XDXR reference universe on a periodic schedule."""
        from utils.a_share_historical_backfill import coerce_date

        task_id = "a_share_tdx_corporate_action_weekly_full_refresh"
        self._active_tasks.add(task_id)
        try:
            normalized_end = coerce_date(
                end_date or get_shanghai_time().date(),
                field_name="end_date",
            )
            normalized_start = coerce_date(
                start_date or date(1990, 12, 19),
                field_name="start_date",
            )
            started_at = time_module.monotonic()
            result = await data_manager.backfill_tdx_xdxr_history(
                exchanges=exchanges,
                start_date=normalized_start,
                end_date=normalized_end,
                instrument_ids=None,
                derive_factors=True,
                repair_universe_mode="current_repair",
                per_instrument_timeout_sec=int(per_instrument_timeout_sec),
                dry_run=False,
            )
            result.setdefault("backtest_stages", {})[
                "canonical_corporate_actions"
            ] = await _run_backtest_stage(
                "canonical_corporate_actions",
                CanonicalCorporateActionProjector(
                    _quote_database_path()
                ).project,
            )
            result["refresh_mode"] = "full"
            result["duration_seconds"] = round(
                time_module.monotonic() - started_at,
                3,
            )
            if self.telegram_enabled:
                totals = result.get("totals") or {}
                await self._send_task_report(
                    report_data={
                        "name": "A 股 TDX 公司行动周度全市场参考刷新",
                        "status": result.get("status"),
                        "content": (
                            "ℹ️ *A 股 TDX 公司行动周度参考刷新*\n\n"
                            f"状态: `{result.get('status')}`\n"
                            "模式: `full`\n"
                            f"处理标的: `{totals.get('processed_instruments', 0)}`\n"
                            f"事件: `{totals.get('raw_events', 0)}`\n"
                            f"错误: `{totals.get('errors', 0)}`；"
                            f"超时: `{totals.get('timeouts', 0)}`\n"
                            f"耗时: `{result.get('duration_seconds', 0):.1f}s`\n"
                            "说明: `TDX 仅为参考源，不改变 CNInfo 主源就绪度`"
                        ),
                        "result": result,
                    },
                    report_type="maintenance_report",
                    task_name=task_id,
                    job_config=job_config,
                )
            return result
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Weekly full TDX corporate-action refresh failed: %s",
                exc,
            )
            return {
                "status": "failed",
                "operation": task_id,
                "refresh_mode": "full",
                "production_isolation": True,
                "error": str(exc),
                "errors": [str(exc)],
            }
        finally:
            self._active_tasks.discard(task_id)

    async def daily_data_backfill_range(
        self,
        start_date: date,
        end_date: date,
        exchanges: Optional[List[str]] = None,
        per_instrument_timeout_sec: Optional[int] = None,
        progress_log_every: int = 200,
        progress_log_interval_sec: int = 300,
        instrument_types: Optional[List[str]] = None,
        run_factor_audit: bool = False,
        repair_universe_mode: str = 'historical_backfill',
        override_lifecycle_filter: bool = False,
        repair_universe_limit: Optional[int] = None,
        force_current_master_refresh: bool = False,
        current_master_refresh_scopes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """区间补充日线数据，避免按交易日重复执行完整日更。"""
        self._active_tasks.add('daily_data_backfill_range')
        try:
            if exchanges is None:
                exchanges = self.config.get_nested(
                    'data_config.market_presets.a_shares',
                    default=['SSE', 'SZSE', 'BSE']
                )

            scheduler_logger.info(
                "[Scheduler] Starting RANGE BACKFILL data update for %s~%s exchanges=%s",
                start_date, end_date, exchanges
            )

            trading_calendar_updates = {}
            scheduler_logger.info("[Scheduler] Range backfill: updating trading calendars once...")
            for exchange in exchanges:
                try:
                    updated_count = await data_manager._update_trading_calendar(
                        exchange,
                        start_date,
                        end_date + timedelta(days=7),
                    )
                    trading_calendar_updates[exchange] = updated_count
                    scheduler_logger.info(
                        "[Scheduler] Range backfill updated %d trading days for %s",
                        updated_count,
                        exchange,
                    )
                except Exception as e:
                    scheduler_logger.error(
                        "[Scheduler] Range backfill failed to update calendar for %s: %s",
                        exchange,
                        e,
                    )
                    trading_calendar_updates[exchange] = 0

            update_results = await data_manager.update_daily_data_range(
                exchanges=exchanges,
                start_date=start_date,
                end_date=end_date,
                per_instrument_timeout_sec=per_instrument_timeout_sec,
                progress_log_every=progress_log_every,
                progress_log_interval_sec=progress_log_interval_sec,
                instrument_types=instrument_types,
                run_factor_audit=run_factor_audit,
                repair_universe_mode=repair_universe_mode,
                override_lifecycle_filter=override_lifecycle_filter,
                repair_universe_limit=repair_universe_limit,
                force_current_master_refresh=force_current_master_refresh,
                current_master_refresh_scopes=current_master_refresh_scopes,
            )
            update_results['trading_calendar_updates'] = trading_calendar_updates
            scheduler_logger.info("[Scheduler] Range backfill completed successfully")
            return update_results

        except Exception as e:
            scheduler_logger.error("[Scheduler] Range backfill failed: %s", e)
            return {
                'operation': 'range_backfill',
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'success_count': 0,
                'failure_count': 1,
                'total_quotes_added': 0,
                'exchange_stats': {},
                'error': str(e),
            }
        finally:
            self._active_tasks.discard('daily_data_backfill_range')

    async def delisted_a_share_quote_backfill(
        self,
        exchanges: Optional[List[str]] = None,
        delisted_year_start: Optional[int] = None,
        delisted_year_end: Optional[int] = None,
        delisted_start_date: Optional[Union[str, date, datetime]] = None,
        delisted_end_date: Optional[Union[str, date, datetime]] = None,
        instrument_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        dry_run: bool = True,
        per_instrument_timeout_sec: Optional[int] = None,
        fail_fast: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> Dict[str, Any]:
        """Operator-triggered historical quote backfill for delisted A-share stocks."""
        self._active_tasks.add('delisted_a_share_quote_backfill')
        try:
            scheduler_logger.info(
                "[Scheduler] Starting delisted A-share quote backfill dry_run=%s exchanges=%s years=%s-%s limit=%s",
                dry_run,
                exchanges,
                delisted_year_start,
                delisted_year_end,
                limit,
            )
            result = await data_manager.run_delisted_a_share_quote_backfill(
                exchanges=exchanges,
                delisted_year_start=delisted_year_start,
                delisted_year_end=delisted_year_end,
                delisted_start_date=delisted_start_date,
                delisted_end_date=delisted_end_date,
                instrument_ids=instrument_ids,
                limit=limit,
                dry_run=dry_run,
                per_instrument_timeout_sec=per_instrument_timeout_sec,
                fail_fast=fail_fast,
            )
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data=result,
                    report_type='maintenance_report',
                    task_name='退市A股历史行情回补',
                    job_config=job_config,
                )
            return result
        except Exception as e:
            scheduler_logger.error("[Scheduler] Delisted A-share quote backfill failed: %s", e)
            failure = {
                'operation': 'delisted_a_share_quote_backfill',
                'status': 'error',
                'error_message': str(e),
            }
            if self.telegram_enabled:
                await self._send_task_report(
                    report_data=failure,
                    report_type='maintenance_report',
                    task_name='退市A股历史行情回补',
                    job_config=job_config,
                )
            return failure
        finally:
            self._active_tasks.discard('delisted_a_share_quote_backfill')

    async def weekly_data_maintenance(self,
                                  backup_database: bool = True,
                                  cleanup_old_logs: bool = True,
                                  log_retention_days: int = 30,
                                  optimize_database: bool = True,
                                  validate_data_integrity: bool = True,
                                  cleanup_ghost_stocks: bool = False,
                                  ghost_stock_grace_days: int = 14,
                                  zombie_stock_grace_days: int = 30,
                                  sync_adjustment_factors: bool = True,
                                  factor_sync_exchanges: Optional[List[str]] = None,
                                  factor_sync_days_back: int = 7,
                                  job_config: Optional[JobConfig] = None) -> bool:
        """每周数据维护任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting weekly data maintenance...")

            # 清理过期缓存
            await cache_manager.quote_cache.clear_expired_data()
            await cache_manager.general_cache._cleanup_expired()

            # 数据库统计
            stats = await data_manager.db_ops.get_database_statistics()
            scheduler_logger.info(f"[Scheduler] Database stats: {stats}")

            if backup_database:
                scheduler_logger.info(
                    "[Scheduler] Weekly maintenance no longer performs database backup; "
                    "production backups are handled only by database_backup."
                )

            # 清理旧日志
            if cleanup_old_logs:
                await self._cleanup_old_logs(log_retention_days)

            if cleanup_ghost_stocks:
                scheduler_logger.warning(
                    "[Scheduler] Ghost/zombie instruments cleanup is deprecated and skipped. "
                    "Lifecycle changes must be handled by exchange-specific master governance."
                )

            # 周度复权因子同步（兜底校验，防止每日精准筛选遗漏）
            factor_sync_status = '成功'
            factor_sync_result = {}
            if sync_adjustment_factors:
                try:
                    factor_sync_result = await data_manager.sync_all_adjustment_factors(
                        exchanges=factor_sync_exchanges,
                        days_back=factor_sync_days_back,
                    )
                    scheduler_logger.info(f"[Scheduler] Weekly factor sync result: {factor_sync_result}")
                except Exception as e:
                    factor_sync_status = f'失败: {e}'
                    scheduler_logger.error(f"[Scheduler] Weekly factor sync failed: {e}")
            else:
                factor_sync_status = '跳过'
                scheduler_logger.info("[Scheduler] Weekly factor sync skipped by config")

            # 数据完整性验证应覆盖本轮清理与因子同步后的最终状态
            if validate_data_integrity:
                await self._validate_data_integrity()

            # 数据库优化放在维护写入之后，避免先优化再大量写入导致收益被抵消
            if optimize_database:
                await self._optimize_database()

            scheduler_logger.info("[Scheduler] Weekly maintenance completed")

            # 生成维护报告数据
            factor_summary = ''
            for ex, res in factor_sync_result.items():
                if isinstance(res, dict) and 'synced' in res:
                    factor_summary += f"{ex}: synced={res['synced']}, skipped={res['skipped']}, failed={res['failed']}; "
            if not factor_summary:
                factor_summary = factor_sync_status

            maintenance_report_data = {
                'name': '每周数据维护报告',
                'status': 'success',  # 明确的成功状态
                'tasks_completed': 6,
                'duration': 'N/A', # 可以在任务开始和结束时记录时间来计算
                'maintenance_tasks': [
                    {'task_name': '数据库备份', 'status': '独立任务执行' if backup_database else '跳过'},
                    {'task_name': '日志清理', 'status': '成功' if cleanup_old_logs else '跳过'},
                    {'task_name': '幽灵/僵尸标的清理', 'status': '已废弃，跳过'},
                    {'task_name': '复权因子周度同步', 'status': factor_summary},
                    {'task_name': '数据完整性验证', 'status': '成功' if validate_data_integrity else '跳过'},
                    {'task_name': '数据库优化', 'status': '成功' if optimize_database else '跳过'},
                ],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            }

            # 发送维护报告
            await self._send_task_report(
                report_data=maintenance_report_data,
                report_type='maintenance_report',
                task_name='每周数据维护',
                job_config=job_config
            )

            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Weekly maintenance failed: {e}")
            if self.telegram_enabled:
                try:
                    # 生成失败报告数据
                    failure_report_data = {
                        'name': '每周数据维护报告',
                        'status': 'error',  # 明确的失败状态
                        'tasks_completed': '维护任务执行失败',
                        'error_message': str(e),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    # 发送失败报告
                    await self._send_task_report(
                        report_data=failure_report_data,
                        report_type='maintenance_report',
                        task_name='每周数据维护',
                        job_config=job_config
                    )
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send failure notification: {e}")
            return False

    async def monthly_data_integrity_check(self,
                                        exchanges: Optional[List[str]] = None,
                                        severity_filter: Optional[List[str]] = None,
                                        days_to_check: int = 45,
                                        repair_universe_mode: str = 'historical_backfill',
                                        override_lifecycle_filter: bool = False,
                                        repair_universe_limit: Optional[int] = None,
                                        force_current_master_refresh: bool = False,
                                        current_master_refresh_scopes: Optional[List[str]] = None,
                                        job_config: Optional[JobConfig] = None) -> bool:
        """月度数据完整性检查和缺口修复任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting monthly data integrity check...")

            # 使用配置参数
            if exchanges is None:
                exchanges = self.config.get_nested(
                    'data_config.market_presets.a_shares',
                    default=['SSE', 'SZSE', 'BSE']
                )

            # 计算检查范围：上个月
            today = date.today()
            # 获取上个月的最后一天
            if today.month == 1:
                end_date = date(today.year - 1, 12, 31)
            else:
                end_date = date(today.year, today.month, 1) - timedelta(days=1)

            # 检查开始日期：从结束日期向前推算指定天数
            start_date = end_date - timedelta(days=days_to_check)

            scheduler_logger.info(f"[Scheduler] Checking data integrity for exchanges: {exchanges}")
            scheduler_logger.info(f"[Scheduler] Date range: {start_date} to {end_date}")

            # 检查每个交易所的数据缺口
            for exchange in exchanges:
                try:
                    scheduler_logger.info(f"[Scheduler] Checking gaps for {exchange}...")

                    # 使用生命周期感知的 GAP 检测系统
                    gap_result = await data_manager.detect_data_gaps(
                        [exchange],
                        start_date,
                        end_date,
                        repair_universe_mode=repair_universe_mode,
                        override_lifecycle_filter=override_lifecycle_filter,
                        repair_universe_limit=repair_universe_limit,
                        force_current_master_refresh=force_current_master_refresh,
                        current_master_refresh_scopes=current_master_refresh_scopes,
                        include_diagnostics=True,
                    )
                    gaps = gap_result['gaps']
                    scheduler_logger.info(f"[Scheduler] Found {len(gaps)} total gaps for {exchange}")

                    # 过滤严重程度（如未配置则不过滤）
                    if severity_filter:
                        filtered_gaps = [g for g in gaps if g.severity in severity_filter]
                        scheduler_logger.info(f"[Scheduler] Found {len(filtered_gaps)} gaps matching severity filter for {exchange}")
                    else:
                        filtered_gaps = gaps
                        scheduler_logger.info(f"[Scheduler] No severity filter applied for {exchange}")

                    if filtered_gaps:
                        # 使用现有的缺口填补系统
                        for gap in filtered_gaps:
                            try:
                                await data_manager._fill_single_gap(gap)
                                # API限流控制
                                await asyncio.sleep(0.5)
                            except Exception as gap_e:
                                scheduler_logger.warning(f"[Scheduler] Failed to fill gap for {gap.instrument_id}: {gap_e}")

                except Exception as exchange_e:
                    scheduler_logger.error(f"[Scheduler] Failed to check {exchange}: {exchange_e}")

            # 任务完成后，重新检测以生成报告
            scheduler_logger.info("[Scheduler] Re-detecting gaps to generate final report...")
            final_gap_result = await data_manager.detect_data_gaps(
                exchanges,
                start_date,
                end_date,
                repair_universe_mode=repair_universe_mode,
                override_lifecycle_filter=override_lifecycle_filter,
                repair_universe_limit=repair_universe_limit,
                force_current_master_refresh=force_current_master_refresh,
                current_master_refresh_scopes=current_master_refresh_scopes,
                include_diagnostics=True,
            )
            final_gaps = final_gap_result['gaps']
            repair_universe = final_gap_result.get('repair_universe', {})

            # 在发送报告前，先对gaps数据进行统计
            from collections import Counter, defaultdict

            total_gaps = len(final_gaps)
            affected_stocks_set = {gap.instrument_id for gap in final_gaps}
            affected_stocks_count = len(affected_stocks_set)
            severity_distribution = dict(Counter(gap.severity for gap in final_gaps))

            # 获取受影响最严重的股票
            top_affected_stocks = data_manager.get_top_affected_stocks(final_gaps, limit=10)

            report_data = {
                'name': '数据缺口报告',
                'status': 'success',
                'summary': {
                    'total_gaps': total_gaps,
                    'affected_stocks': affected_stocks_count,
                    'severity_distribution': severity_distribution,
                    'lifecycle_skipped_instruments': repair_universe.get('skipped_instrument_count', 0),
                    'lifecycle_skipped_gap_segments': repair_universe.get('skipped_gap_segment_count', 0),
                },
                'top_affected_stocks': top_affected_stocks,
                'repair_universe': repair_universe,
                'repair_universe_summary': _format_repair_universe_summary(repair_universe),
            }

            # 发送详细的完成通知
            await self._send_task_report(
                report_data=report_data,
                report_type='gap_report',
                task_name='月度数据完整性检查',
                job_config=job_config
            )

            scheduler_logger.info(f"[Scheduler] Monthly data integrity check completed")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Monthly data integrity check failed: {e}")
            # 统一使用报告系统发送失败通知
            failure_report_data = {
                'name': '数据缺口报告',
                'status': 'error',
                'error_message': str(e)
            }
            await self._send_task_report(
                report_data=failure_report_data,
                report_type='gap_report',
                task_name='月度数据完整性检查',
                job_config=job_config
            )
            return False

    async def find_gap_and_repair(self,
                                  exchanges: Optional[List[str]] = None,
                                  start_date: Optional[date] = None,
                                  end_date: Optional[date] = None,
                                  severity_filter: Optional[List[str]] = None,
                                  skip_failed_segments: bool = True,
                                  skip_ttl_days: int = 30,
                                  hkex_max_gap_segments_per_instrument: Optional[int] = 20,
                                  hkex_max_missing_days_per_instrument: Optional[int] = 60,
                                  repair_universe_mode: str = 'historical_backfill',
                                  override_lifecycle_filter: bool = False,
                                  repair_universe_limit: Optional[int] = None,
                                  force_current_master_refresh: bool = False,
                                  current_master_refresh_scopes: Optional[List[str]] = None,
                                  job_config: Optional[JobConfig] = None) -> bool:
        """检测数据缺口并修复（复合任务）"""
        self._active_tasks.add('find_gap_and_repair')
        try:
            scheduler_logger.info("[Scheduler] Starting gap detect and repair task...")

            if exchanges is None:
                exchanges = self.config.get_nested(
                    'data_config.market_presets.a_shares',
                    default=['SSE', 'SZSE', 'BSE']
                )

            if isinstance(start_date, str):
                start_date = date.fromisoformat(start_date)
            elif isinstance(start_date, datetime):
                start_date = start_date.date()
            if start_date is None:
                start_date = date(2024, 1, 1)

            if isinstance(end_date, str):
                end_date = date.fromisoformat(end_date)
            elif isinstance(end_date, datetime):
                end_date = end_date.date()
            if end_date is None:
                end_date = date.today()

            scheduler_logger.info(f"[Scheduler] Exchanges: {exchanges}")
            scheduler_logger.info(f"[Scheduler] Date range: {start_date} to {end_date}")

            gap_result = await data_manager.detect_data_gaps(
                exchanges,
                start_date,
                end_date,
                repair_universe_mode=repair_universe_mode,
                override_lifecycle_filter=override_lifecycle_filter,
                repair_universe_limit=repair_universe_limit,
                force_current_master_refresh=force_current_master_refresh,
                current_master_refresh_scopes=current_master_refresh_scopes,
                include_diagnostics=True,
            )
            all_gaps = gap_result['gaps']
            repair_universe = gap_result.get('repair_universe', {})
            scheduler_logger.info(f"[Scheduler] Detected {len(all_gaps)} gaps")
            if repair_universe.get('skipped_instrument_count') or repair_universe.get('skipped_gap_segment_count'):
                scheduler_logger.info(
                    "[Scheduler] Repair universe lifecycle-skipped instruments=%s gap_segments=%s reasons=%s",
                    repair_universe.get('skipped_instrument_count', 0),
                    repair_universe.get('skipped_gap_segment_count', 0),
                    repair_universe.get('reason_distribution', {}),
                )

            if severity_filter:
                gaps_to_repair = [gap for gap in all_gaps if gap.severity in severity_filter]
                scheduler_logger.info(f"[Scheduler] Severity filter applied: {severity_filter} -> {len(gaps_to_repair)} gaps")
            else:
                gaps_to_repair = all_gaps

            gaps_to_repair, hkex_guard_details, hkex_guard_skipped = _apply_hkex_gap_guard(
                gaps_to_repair,
                hkex_max_gap_segments_per_instrument,
                hkex_max_missing_days_per_instrument,
            )
            if hkex_guard_skipped:
                scheduler_logger.warning(
                    "[Scheduler] HKEX guard skipped %s gap segments across %s instruments",
                    hkex_guard_skipped,
                    len(hkex_guard_details)
                )

            skip_set = set()
            if skip_failed_segments:
                skip_set = await data_manager.load_gap_skip_set(ttl_days=skip_ttl_days)
                scheduler_logger.info(
                    "[Scheduler] Loaded %s recent failed gap segments into skip set",
                    len(skip_set)
                )

            repaired = 0
            failed = 0
            skipped_known_failures = 0
            skipped_after_no_data_failures = 0
            no_data_failures_by_instrument: Dict[str, int] = {}
            reportable_gaps = []
            max_no_data_failures_per_instrument = int(
                self.config.get_nested(
                    'data_config.repair_universe_governance.max_no_data_failures_per_instrument',
                    default=3,
                ) or 0
            )
            max_index_no_data_failures_per_instrument = int(
                self.config.get_nested(
                    'data_config.repair_universe_governance.max_index_no_data_failures_per_instrument',
                    default=1,
                ) or 0
            )
            failure_details = []
            for gap in gaps_to_repair:
                if skip_failed_segments and data_manager.is_gap_skipped(
                    skip_set, gap.instrument_id, gap.gap_start, gap.gap_end
                ):
                    skipped_known_failures += 1
                    continue

                no_data_failure_limit = max_no_data_failures_per_instrument
                if str(getattr(gap, 'instrument_type', '') or '').lower() == 'index':
                    no_data_failure_limit = max_index_no_data_failures_per_instrument
                if (
                    no_data_failure_limit > 0
                    and no_data_failures_by_instrument.get(gap.instrument_id, 0) >= no_data_failure_limit
                ):
                    skipped_after_no_data_failures += 1
                    if skip_failed_segments:
                        await data_manager.record_gap_skip(
                            gap.instrument_id,
                            gap.gap_start,
                            gap.gap_end,
                            reason='instrument_no_data_failure_limit',
                        )
                        skip_set.add(data_manager.build_gap_skip_key(
                            gap.instrument_id, gap.gap_start, gap.gap_end
                        ))
                    continue

                reportable_gaps.append(gap)
                try:
                    success = await data_manager._fill_single_gap(gap)
                    if success:
                        repaired += 1
                        no_data_failures_by_instrument[gap.instrument_id] = 0
                    else:
                        no_data_failures_by_instrument[gap.instrument_id] = (
                            no_data_failures_by_instrument.get(gap.instrument_id, 0) + 1
                        )
                        if skip_failed_segments:
                            await data_manager.record_gap_skip(
                                gap.instrument_id, gap.gap_start, gap.gap_end, reason='no_data'
                            )
                            skip_set.add(data_manager.build_gap_skip_key(
                                gap.instrument_id, gap.gap_start, gap.gap_end
                            ))
                        failure_details.append({
                            'instrument_id': gap.instrument_id,
                            'exchange': gap.exchange,
                            'gap_start': gap.gap_start,
                            'gap_end': gap.gap_end,
                            'reason': 'fill_returned_false'
                        })
                        scheduler_logger.warning(
                            "[Scheduler] Gap repair returned false for %s (%s) %s to %s",
                            gap.instrument_id,
                            gap.exchange,
                            gap.gap_start,
                            gap.gap_end
                        )
                        failed += 1
                except Exception as gap_e:
                    if skip_failed_segments:
                        await data_manager.record_gap_skip(
                            gap.instrument_id, gap.gap_start, gap.gap_end, reason='source_error'
                        )
                        skip_set.add(data_manager.build_gap_skip_key(
                            gap.instrument_id, gap.gap_start, gap.gap_end
                        ))
                    scheduler_logger.warning(f"[Scheduler] Failed to fill gap for {gap.instrument_id}: {gap_e}")
                    failure_details.append({
                        'instrument_id': gap.instrument_id,
                        'exchange': gap.exchange,
                        'gap_start': gap.gap_start,
                        'gap_end': gap.gap_end,
                        'reason': str(gap_e)
                    })
                    failed += 1
                await asyncio.sleep(0.5)

            from collections import Counter
            severity_distribution = dict(Counter(gap.severity for gap in reportable_gaps))
            affected_stocks = len({gap.instrument_id for gap in reportable_gaps})
            top_affected_stocks = data_manager.get_top_affected_stocks(reportable_gaps, limit=10)
            repair_universe_degraded = bool(
                repair_universe.get('warnings') or repair_universe.get('errors')
            )

            report_data = {
                'name': '数据缺口检测与修复报告',
                'status': 'success' if failed == 0 and not repair_universe_degraded else 'warning',
                'total_gaps': len(reportable_gaps),
                'affected_stocks': affected_stocks,
                'severity_distribution': severity_distribution,
                'top_affected_stocks': top_affected_stocks,
                'summary': {
                    'total_gaps': len(reportable_gaps),
                    'affected_stocks': affected_stocks,
                    'severity_distribution': severity_distribution,
                    'detected_gaps': len(all_gaps),
                    'candidate_gaps': len(gaps_to_repair),
                    'reportable_gaps': len(reportable_gaps),
                    'repaired_gaps': repaired,
                    'failed_repairs': failed,
                    'skipped_known_failures': skipped_known_failures,
                    'skipped_after_no_data_failures': skipped_after_no_data_failures,
                    'skipped_by_hkex_guard': hkex_guard_skipped,
                    'lifecycle_skipped_instruments': repair_universe.get('skipped_instrument_count', 0),
                    'lifecycle_skipped_gap_segments': repair_universe.get('skipped_gap_segment_count', 0),
                    'lifecycle_skipped_missing_days': repair_universe.get('skipped_missing_days', 0),
                },
                'failure_details': failure_details[:50],
                'skipped_instruments': hkex_guard_details[:50],
                'repair_universe': repair_universe,
                'repair_universe_summary': _format_repair_universe_summary(repair_universe),
                'instrument_master_governance': gap_result.get('instrument_master_governance'),
                'filters': {
                    'exchanges': exchanges,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'severity_filter': severity_filter,
                    'skip_failed_segments': skip_failed_segments,
                    'skip_ttl_days': skip_ttl_days,
                    'hkex_max_gap_segments_per_instrument': hkex_max_gap_segments_per_instrument,
                    'hkex_max_missing_days_per_instrument': hkex_max_missing_days_per_instrument,
                    'repair_universe_mode': repair_universe_mode,
                    'override_lifecycle_filter': override_lifecycle_filter,
                    'repair_universe_limit': repair_universe_limit,
                    'force_current_master_refresh': force_current_master_refresh,
                    'current_master_refresh_scopes': current_master_refresh_scopes,
                }
            }

            await self._send_task_report(
                report_data=report_data,
                report_type='gap_report',
                task_name='数据缺口检测与修复',
                job_config=job_config
            )

            scheduler_logger.info("[Scheduler] Gap detect and repair task completed")
            return failed == 0

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Gap detect and repair task failed: {e}")
            failure_report_data = {
                'name': '数据缺口检测与修复报告',
                'status': 'error',
                'error_message': str(e)
            }
            await self._send_task_report(
                report_data=failure_report_data,
                report_type='gap_report',
                task_name='数据缺口检测与修复',
                job_config=job_config
            )
            return False
        finally:
            self._active_tasks.discard('find_gap_and_repair')

    async def company_profile_shadow_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 company profile 影子同步任务。"""
        self._active_tasks.add('company_profile_shadow_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting company profile shadow sync...")

            result = await data_manager.run_company_profile_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '公司档案影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('source') or 'no-source'})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='公司档案影子同步',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Company profile shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '公司档案影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'company_profile_shadow_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='公司档案影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('company_profile_shadow_sync')

    async def business_profile_structured_sync(
        self,
        as_of_date: Optional[str] = None,
        sources: Optional[List[str]] = None,
        industry_groups: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        max_instruments: Optional[int] = None,
        max_elapsed_seconds: Optional[float] = None,
        candidate_write: bool = True,
        operator_switch: str = "",
        checkpoint_path: Optional[str] = None,
        resume: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Run one bounded candidate-only business-profile maintenance batch."""
        task_id = "business_profile_structured_sync"
        self._active_tasks.add(task_id)
        try:
            scheduler_logger.info(
                "[Scheduler] Starting structured business-profile sync..."
            )
            result = await data_manager.run_business_profile_structured_sync(
                as_of_date=as_of_date,
                sources=sources,
                industry_groups=industry_groups,
                instrument_ids=instrument_ids,
                max_instruments=max_instruments,
                max_elapsed_seconds=max_elapsed_seconds,
                candidate_write=candidate_write,
                operator_switch=operator_switch,
                checkpoint_path=checkpoint_path,
                resume=resume,
            )
            status = str(result.get("status") or "failed")
            success = status in {"success", "degraded"}
            source_tasks = []
            for source, source_result in sorted((result.get("sources") or {}).items()):
                source_tasks.append(
                    {
                        "task_name": source,
                        "status": (
                            f"success={source_result.get('success_count', 0)}, "
                            f"empty={source_result.get('empty_count', 0)}, "
                            f"failed={source_result.get('failed_count', 0)}"
                        ),
                    }
                )
            if not source_tasks:
                source_tasks.append(
                    {
                        "task_name": task_id,
                        "status": result.get("reason") or status,
                    }
                )
            await self._send_task_report(
                report_data={
                    "name": "结构化业务画像同步报告",
                    "status": "success" if success else "error",
                    "tasks_completed": result.get("attempted_instruments", 0),
                    "duration": f"{result.get('elapsed_seconds', 0):.2f}s",
                    "maintenance_tasks": source_tasks,
                    "business_profile_sync": result,
                },
                report_type="maintenance_report",
                task_name="结构化业务画像同步",
                job_config=job_config,
            )
            return success
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Structured business-profile sync failed: %s",
                exc,
            )
            await self._send_task_report(
                report_data={
                    "name": "结构化业务画像同步报告",
                    "status": "error",
                    "tasks_completed": 0,
                    "duration": "N/A",
                    "maintenance_tasks": [
                        {"task_name": task_id, "status": str(exc)}
                    ],
                },
                report_type="maintenance_report",
                task_name="结构化业务画像同步",
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_id)

    async def business_profile_daily_incremental(
        self,
        knowledge_cutoff: Optional[str] = None,
        exchanges: Optional[List[str]] = None,
        field_families: Optional[List[str]] = None,
        runtime_identities: Optional[Dict[str, str]] = None,
        max_attempts: int = 3,
        discovery_kwargs: Optional[Dict[str, Any]] = None,
        stage_budgets: Optional[Dict[str, Dict[str, Any]]] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Discover first, then advance bounded durable business-profile queues."""
        task_id = "business_profile_daily_incremental"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.run_business_profile_daily_incremental(
                knowledge_cutoff=knowledge_cutoff,
                exchanges=exchanges,
                field_families=field_families,
                runtime_identities=runtime_identities,
                max_attempts=max_attempts,
                discovery_kwargs=discovery_kwargs,
                stage_budgets=stage_budgets,
            )
            status = str(result.get("status") or "failed")
            success = status in {"success", "degraded", "disabled"}
            await self._send_task_report(
                report_data={
                    "name": "业务画像异步日更报告",
                    "status": "success" if success else "error",
                    "tasks_completed": _business_profile_completed_items(result),
                    "duration": f"{float(result.get('elapsed_seconds') or 0):.2f}s",
                    "maintenance_tasks": [
                        {"task_name": task_id, "status": result.get("reason") or status}
                    ],
                    "business_profile_async_production": result,
                },
                report_type="maintenance_report",
                task_name="业务画像异步日更",
                job_config=job_config,
            )
            return success
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Business-profile daily incremental failed: %s", exc
            )
            return False
        finally:
            self._active_tasks.discard(task_id)

    async def business_profile_backfill(
        self,
        knowledge_cutoff: Optional[str] = None,
        rollout_phase: Optional[str] = None,
        selection_policy: Optional[str] = None,
        instrument_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        document_types: Optional[List[str]] = None,
        field_families: Optional[List[str]] = None,
        runtime_identities: Optional[Dict[str, str]] = None,
        force: bool = False,
        max_attempts: int = 3,
        stage_budgets: Optional[Dict[str, Dict[str, Any]]] = None,
        continuous: bool = False,
        continuous_poll_seconds: float = 30.0,
        continuous_max_idle_cycles: int = 3,
        continuous_max_cycles: Optional[int] = None,
        heartbeat_interval_seconds: float = 30.0,
        progress_report_interval_seconds: float = 0.0,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Run one bounded pass or continuously drain durable profile queues."""
        task_id = "business_profile_backfill"
        if continuous and force:
            scheduler_logger.error(
                "[Scheduler] Continuous business-profile backfill rejects force=true "
                "because every cycle would reset finalized work"
            )
            return False
        if continuous and str(selection_policy or "").strip() == "expanded":
            scheduler_logger.error(
                "[Scheduler] Continuous business-profile backfill only supports "
                "the governed latest-annual rollout"
            )
            return False

        store = _business_profile_backfill_control_store()
        run_id = (
            "business-profile-"
            + get_shanghai_time().strftime("%Y%m%d%H%M%S%f")
        )
        active_phase = str(
            rollout_phase
            or config_manager.get_nested(
                "business_profile_rollout.active_phase",
                "",
            )
            or ""
        )
        parameters = {
            "knowledge_cutoff": knowledge_cutoff,
            "rollout_phase": rollout_phase,
            "selection_policy": selection_policy,
            "instrument_ids": list(instrument_ids or []),
            "start_date": start_date,
            "end_date": end_date,
            "document_types": list(document_types or []),
            "field_families": list(field_families or []),
            "force": bool(force),
            "max_attempts": int(max_attempts),
            "continuous": bool(continuous),
        }

        async def run_cycle(should_stop) -> Dict[str, Any]:
            return await data_manager.run_business_profile_backfill(
                knowledge_cutoff=knowledge_cutoff,
                rollout_phase=rollout_phase,
                selection_policy=selection_policy,
                instrument_ids=instrument_ids,
                start_date=start_date,
                end_date=end_date,
                document_types=document_types,
                field_families=field_families,
                runtime_identities=runtime_identities,
                force=force,
                max_attempts=max_attempts,
                stage_budgets=stage_budgets,
                should_stop=should_stop,
            )

        started_monotonic = time_module.monotonic()
        outcome = "running"
        scheduler_logger.info(
            "[Scheduler] Business-profile backfill start run_id=%s mode=%s "
            "phase=%s cutoff=%s policy=%s instruments=%s start_date=%s end_date=%s",
            run_id,
            "continuous" if continuous else "single_batch",
            active_phase or None,
            knowledge_cutoff,
            selection_policy,
            len(instrument_ids or ()),
            start_date,
            end_date,
        )
        self._active_tasks.add(task_id)
        try:
            if continuous:
                from research.business_profile_backfill_control import (
                    ContinuousBackfillOptions,
                    ContinuousBusinessProfileBackfillRunner,
                )

                options = ContinuousBackfillOptions(
                    poll_interval_seconds=float(continuous_poll_seconds),
                    max_idle_cycles=int(continuous_max_idle_cycles),
                    max_cycles=(
                        int(continuous_max_cycles)
                        if continuous_max_cycles is not None
                        else None
                    ),
                    heartbeat_interval_seconds=float(heartbeat_interval_seconds),
                    progress_report_interval_seconds=float(
                        progress_report_interval_seconds
                    ),
                )
                runner = ContinuousBusinessProfileBackfillRunner(
                    store,
                    options=options,
                )

                async def send_progress(progress: Dict[str, Any]) -> None:
                    snapshot = store.status()
                    scheduler_logger.info(
                        "[Scheduler] Business-profile continuous progress: %s",
                        _format_business_profile_backfill_progress(snapshot),
                    )
                    await self._send_task_report(
                        report_data={
                            "name": "业务画像持续回补进度",
                            "status": "running",
                            "maintenance_tasks": [
                                {
                                    "task_name": task_id,
                                    "status": snapshot.get("state"),
                                }
                            ],
                            "business_profile_backfill_progress": snapshot,
                            "detail_messages": [
                                _format_business_profile_backfill_progress(snapshot)
                            ],
                        },
                        report_type="maintenance_report",
                        task_name="业务画像持续回补进度",
                        job_config=job_config,
                    )

                progress = await runner.run(
                    run_id=run_id,
                    phase=active_phase or None,
                    parameters=parameters,
                    run_cycle=run_cycle,
                    on_progress=send_progress,
                )
                result = dict(progress.get("latest_result") or {})
                result["continuous_progress"] = progress
                success = progress.get("state") in {"completed", "stopped"}
            else:
                store.begin(
                    run_id=run_id,
                    mode="single_batch",
                    phase=active_phase or None,
                    parameters=parameters,
                )
                result = await run_cycle(
                    lambda: store.should_stop(run_id) is not None
                )
                result_status = str(result.get("status") or "failed").lower()
                success = result_status in {
                    "success",
                    "degraded",
                    "disabled",
                    "stopped",
                }
                progress_state = "stopped" if result_status == "stopped" else (
                    "completed" if success else "failed"
                )
                progress = store.finish(
                    run_id,
                    state=progress_state,
                    reason_codes=[
                        "operator_stop_requested"
                        if result_status == "stopped"
                        else "single_batch_complete"
                        if success
                        else str(result.get("reason") or result_status)
                    ],
                    latest_result=result,
                )
                result["continuous_progress"] = progress
            outcome = str(result.get("status") or progress.get("state") or "unknown")
            await self._send_task_report(
                report_data={
                    "name": (
                        "业务画像持续回补报告"
                        if continuous
                        else "业务画像手工回补报告"
                    ),
                    "status": "success" if success else "error",
                    "tasks_completed": _business_profile_completed_items(result),
                    "duration": (
                        f"{time_module.monotonic() - started_monotonic:.3f}s"
                    ),
                    "maintenance_tasks": [
                        {
                            "task_name": task_id,
                            "status": result.get("reason") or result.get("status"),
                        }
                    ],
                    "business_profile_async_production": result,
                    "detail_messages": [
                        _format_business_profile_backfill_progress(store.status())
                    ],
                },
                report_type="maintenance_report",
                task_name=(
                    "业务画像持续回补" if continuous else "业务画像手工回补"
                ),
                job_config=job_config,
            )
            return success
        except asyncio.CancelledError:
            outcome = "interrupted"
            store.finish(
                run_id,
                state="interrupted",
                reason_codes=["task_cancelled"],
            )
            raise
        except Exception as exc:
            outcome = "failed"
            store.finish(
                run_id,
                state="failed",
                reason_codes=[f"{type(exc).__name__}: {exc}"],
            )
            scheduler_logger.exception(
                "[Scheduler] Business-profile backfill failed: %s", exc
            )
            return False
        finally:
            scheduler_logger.info(
                "[Scheduler] Business-profile backfill end run_id=%s mode=%s "
                "outcome=%s elapsed_seconds=%.3f",
                run_id,
                "continuous" if continuous else "single_batch",
                outcome,
                time_module.monotonic() - started_monotonic,
            )
            self._active_tasks.discard(task_id)

    async def business_profile_backfill_control(
        self,
        action: str = "status",
        reason: str = "operator_request",
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Read progress or request a cooperative stop for the active backfill."""

        task_id = "business_profile_backfill_control"
        action_name = str(action or "status").strip().lower()
        if action_name not in {"status", "stop"}:
            scheduler_logger.error(
                "[Scheduler] Unsupported business-profile control action: %s",
                action_name,
            )
            return False
        try:
            store = _business_profile_backfill_control_store()
            control = (
                store.request_stop(reason=str(reason or "operator_request"))
                if action_name == "stop"
                else {"status": "success", "progress": store.status()}
            )
            progress = store.status()
            await self._send_task_report(
                report_data={
                    "name": "业务画像回补控制",
                    "status": control.get("status"),
                    "maintenance_tasks": [
                        {
                            "task_name": task_id,
                            "status": control.get("status"),
                        }
                    ],
                    "business_profile_backfill_control": control,
                    "business_profile_backfill_progress": progress,
                    "detail_messages": [
                        _format_business_profile_backfill_progress(progress)
                    ],
                },
                report_type="maintenance_report",
                task_name="业务画像回补控制",
                job_config=job_config,
            )
            return True
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Business-profile backfill control failed: %s",
                exc,
            )
            return False

    async def business_profile_unit_rule_control(
        self,
        action: str = "show",
        rule_id: Optional[str] = None,
        dimension: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        multiplier: Optional[str] = None,
        reason: str = "operator_correction",
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Inspect or append a governed correction for a business-profile unit rule."""

        task_id = "business_profile_unit_rule_control"
        action_name = str(action or "show").strip().lower()
        try:
            if action_name not in {"show", "correct"}:
                raise ValueError(
                    "business-profile unit-rule action must be show or correct"
                )
            normalized_rule_id = str(rule_id or "").strip()
            if not normalized_rule_id:
                raise ValueError("business-profile unit-rule control requires rule_id")
            storage = getattr(data_manager, "research_storage", None)
            if storage is None:
                raise ValueError("business-profile research storage is unavailable")

            from research.business_profile_async_production import (
                ensure_business_profile_storage_ready,
            )
            from research.business_profile_unit_registry import (
                BusinessProfileUnitRuleRegistry,
            )

            ensure_business_profile_storage_ready(storage)
            registry = BusinessProfileUnitRuleRegistry(storage)
            notification = None
            if action_name == "show":
                current = registry.get_rule(normalized_rule_id)
                history = registry.get_rule_history(normalized_rule_id)
                payload = {
                    "action": "show",
                    "rule": current,
                    "history": history,
                    "history_event_count": len(history),
                }
                detail = (
                    f"规则={normalized_rule_id} 状态={current.get('status')} "
                    f"单位={current.get('source_unit')} "
                    f"维度={current.get('dimension')} "
                    f"规范单位={current.get('canonical_unit')} "
                    f"倍率={current.get('multiplier')} "
                    f"历史事件={len(history)}"
                )
            else:
                if dimension is None or canonical_unit is None or multiplier is None:
                    raise ValueError(
                        "correct requires dimension, canonical_unit and multiplier"
                    )
                correction = registry.correct_rule(
                    normalized_rule_id,
                    dimension=str(dimension),
                    canonical_unit=str(canonical_unit),
                    multiplier=multiplier,
                    reason=str(reason or "operator_correction"),
                )
                dispatcher = getattr(
                    data_manager,
                    "_dispatch_business_profile_unit_rule_notifications",
                    None,
                )
                if callable(dispatcher):
                    notification = await dispatcher()
                replacement = correction["replacement_rule"]
                payload = {
                    "action": "correct",
                    **correction,
                    "notification": notification,
                }
                detail = (
                    f"旧规则={normalized_rule_id} 已追加替代规则="
                    f"{replacement.get('rule_id')} 状态={replacement.get('status')} "
                    f"维度={replacement.get('dimension')} "
                    f"规范单位={replacement.get('canonical_unit')} "
                    f"倍率={replacement.get('multiplier')} "
                    f"重放语义产物={correction.get('replayed_artifacts', 0)}"
                )

            await self._send_task_report(
                report_data={
                    "name": "公司画像单位规则控制",
                    "status": "success",
                    "tasks_completed": 1,
                    "maintenance_tasks": [
                        {"task_name": task_id, "status": "success"}
                    ],
                    "business_profile_unit_rule_control": payload,
                    "detail_messages": [detail],
                },
                report_type="maintenance_report",
                task_name="公司画像单位规则控制",
                job_config=job_config,
            )
            return True
        except Exception as exc:
            scheduler_logger.exception(
                "[Scheduler] Business-profile unit-rule control failed: %s", exc
            )
            await self._send_task_report(
                report_data={
                    "name": "公司画像单位规则控制",
                    "status": "error",
                    "tasks_completed": 0,
                    "maintenance_tasks": [
                        {"task_name": task_id, "status": "failed"}
                    ],
                    "detail_messages": [f"{type(exc).__name__}: {exc}"],
                },
                report_type="maintenance_report",
                task_name="公司画像单位规则控制",
                job_config=job_config,
            )
            return False

    async def industry_shadow_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 industry 影子同步任务。"""
        self._active_tasks.add('industry_shadow_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting industry shadow sync...")

            result = await data_manager.run_industry_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '行业归属影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('source') or 'no-source'})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='行业归属影子同步',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Industry shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '行业归属影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'industry_shadow_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='行业归属影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_shadow_sync')

    async def industry_standard_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        force_component_refresh: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 strict Shenwan 行业标准层日更同步任务。"""
        self._active_tasks.add('industry_standard_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting industry standard sync...")

            result = await data_manager.run_industry_standard_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                force_component_refresh=force_component_refresh,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '申万标准行业同步报告',
                'content': _format_industry_standard_scheduler_report(result),
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('memberships_written', 0)} memberships)"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='申万标准行业同步',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Industry standard sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '申万标准行业同步报告',
                    'content': _format_industry_standard_scheduler_report({
                        'status': 'error',
                        'reason': str(e),
                        'attempted_exchanges': 0,
                        'successful_exchanges': 0,
                        'exchanges': [
                            {'exchange': 'ALL', 'status': 'error', 'error_message': str(e)}
                        ],
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'industry_standard_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='申万标准行业同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_standard_sync')

    async def industry_standard_rebuild_official(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        drop_existing: bool = True,
        drop_source_files: bool = False,
        force_refresh: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 strict Shenwan 官方分类全量重建任务。"""
        self._active_tasks.add('industry_standard_rebuild_official')
        try:
            scheduler_logger.info("[Scheduler] Starting official Shenwan industry rebuild...")

            result = await data_manager.rebuild_official_industry_standard(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                drop_existing=drop_existing,
                drop_source_files=drop_source_files,
                force_refresh=force_refresh,
            )

            status = result.get('status', 'failed')
            success = status == 'success'
            sync_result = result.get('sync') or {}
            readiness = result.get('readiness') or {}
            table_counts = result.get('table_counts') or {}
            after_counts = table_counts.get('after') or {}

            maintenance_tasks = [
                {
                    'task_name': 'official_rebuild',
                    'status': (
                        f"{status} "
                        f"(memberships={sync_result.get('total_memberships_written', 0)}, "
                        f"history={sync_result.get('classification_history_rows_written', 0)})"
                    ),
                },
                {
                    'task_name': 'readiness',
                    'status': (
                        f"ready={readiness.get('industry_standard_ready')} "
                        f"target={readiness.get('target_instrument_count', 0)}"
                    ),
                },
                {
                    'task_name': 'table_counts',
                    'status': (
                        f"taxonomy={after_counts.get('industry_taxonomy', 0)}, "
                        f"memberships={after_counts.get('industry_memberships', 0)}, "
                        f"source_files={after_counts.get('industry_source_files', 0)}"
                    ),
                },
            ]

            report_data = {
                'name': '申万官方分类全量重建报告',
                'status': 'success' if success else 'error',
                'tasks_completed': sync_result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': maintenance_tasks,
                'summary': {
                    'status': status,
                    'industry_standard_ready': readiness.get('industry_standard_ready'),
                    'target_instrument_count': readiness.get('target_instrument_count', 0),
                    'table_counts_after': after_counts,
                },
            }

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='申万官方分类全量重建',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Official Shenwan industry rebuild failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '申万官方分类全量重建报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'industry_standard_rebuild_official', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='申万官方分类全量重建',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_standard_rebuild_official')

    async def industry_index_analysis_sync(
        self,
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域申万行业指数分析日频指标同步任务。"""
        self._active_tasks.add('industry_index_analysis_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting industry index-analysis sync...")

            result = await data_manager.run_industry_index_analysis_sync(
                index_types=index_types,
                limit_per_type=limit_per_type,
            )

            status = result.get('status', 'failed')
            success = status == 'success'
            summary = result.get('summary') or {}
            type_counts = summary.get('index_type_counts') or {}
            report_data = {
                'name': '申万行业指数分析同步报告',
                'content': _format_industry_index_analysis_scheduler_report(result),
                'status': 'success' if success else status,
                'tasks_completed': len(type_counts),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': index_type,
                        'status': (
                            f"rows={counts.get('rows', 0)}, "
                            f"codes={counts.get('codes', 0)}"
                        ),
                    }
                    for index_type, counts in type_counts.items()
                ],
                'summary': {
                    'rows_written': result.get('rows_written', 0),
                    'latest_trade_date': summary.get('latest_trade_date'),
                    'distinct_index_codes': summary.get('distinct_index_codes', 0),
                },
            }

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='申万行业指数分析同步',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Industry index-analysis sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '申万行业指数分析同步报告',
                    'content': _format_industry_index_analysis_scheduler_report({
                        'status': 'error',
                        'reason': str(e),
                        'rows_written': 0,
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'industry_index_analysis_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='申万行业指数分析同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_index_analysis_sync')

    async def industry_index_analysis_backfill(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        source: str = "akshare",
        mode: str = "direct",
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域申万行业指数分析历史回补任务，默认禁用并要求显式日期。"""
        self._active_tasks.add('industry_index_analysis_backfill')
        try:
            if not start_date or not end_date:
                raise ValueError("industry_index_analysis_backfill requires start_date and end_date")

            scheduler_logger.info(
                "[Scheduler] Starting industry index-analysis backfill %s-%s...",
                start_date,
                end_date,
            )
            result = await data_manager.run_industry_index_analysis_backfill(
                start_date=start_date,
                end_date=end_date,
                index_types=index_types,
                limit_per_type=limit_per_type,
                source=source,
                mode=mode,
            )
            status = result.get('status', 'failed')
            success = status == 'success'
            coverage = result.get('coverage') or {}
            type_counts = coverage.get('index_type_counts') or {}
            report_data = {
                'name': '申万行业指数分析历史回补报告',
                'status': 'success' if success else status,
                'tasks_completed': len(type_counts),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': index_type,
                        'status': (
                            f"fetched_rows={counts.get('rows', 0)}, "
                            f"dates={counts.get('trade_dates', 0)}"
                        ),
                    }
                    for index_type, counts in type_counts.items()
                ],
                'summary': {
                    'rows_written': result.get('rows_written', 0),
                    'start_date': coverage.get('start_date'),
                    'end_date': coverage.get('end_date'),
                    'trade_dates': coverage.get('trade_dates', 0),
                },
            }
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='申万行业指数分析历史回补',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Industry index-analysis backfill failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '申万行业指数分析历史回补报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'industry_index_analysis_backfill', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='申万行业指数分析历史回补',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_index_analysis_backfill')

    async def futures_market_data_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
        dry_run: bool = False,
        requires_trading_calendar_backfill: bool = True,
        requires_trading_day_governance: bool = True,
        requires_master_data_governance: bool = True,
        master_governance_max_days: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货行情日更任务。"""
        self._active_tasks.add('futures_market_data_sync')
        try:
            master_results: List[Dict[str, Any]] = []
            blocked_calendar_exchanges: List[str] = []
            effective_scope_id = scope_id
            effective_scope_ids = scope_ids
            effective_exchanges = exchanges
            if requires_trading_day_governance:
                calendar_start_date = start_date or end_date
                calendar_end_date = end_date or start_date
                if requires_trading_calendar_backfill:
                    scheduler_logger.info(
                        "[Scheduler] Futures official calendar preflight start exchanges=%s scope_id=%s scope_ids=%s start=%s end=%s dry_run=%s",
                        exchanges,
                        scope_id,
                        scope_ids,
                        calendar_start_date,
                        calendar_end_date,
                        dry_run,
                    )
                    calendar_backfill_result = await data_manager.run_futures_official_calendar_backfill(
                        scope_id=scope_id,
                        scope_ids=scope_ids,
                        exchanges=exchanges,
                        categories=categories,
                        instrument_ids=instrument_ids,
                        series_ids=series_ids,
                        series_types=series_types,
                        start_date=calendar_start_date,
                        end_date=calendar_end_date,
                        dry_run=dry_run,
                    )
                    scheduler_logger.info(
                        "[Scheduler] Futures official calendar preflight done status=%s exchanges=%s start=%s end=%s totals=%s",
                        calendar_backfill_result.get("status"),
                        calendar_backfill_result.get("exchanges") or exchanges,
                        calendar_start_date,
                        calendar_end_date,
                        calendar_backfill_result.get("totals") or {},
                    )
                    if calendar_backfill_result.get("status") == "blocked" and not dry_run:
                        exchange_results = calendar_backfill_result.get("exchanges") or []
                        blocked_calendar_exchanges = sorted(
                            {
                                str(item.get("exchange") or "").upper()
                                for item in exchange_results
                                if str(item.get("status") or "") == "blocked"
                                and str(item.get("exchange") or "").strip()
                            }
                        )
                        runnable_calendar_exchanges = sorted(
                            {
                                str(item.get("exchange") or "").upper()
                                for item in exchange_results
                                if str(item.get("status") or "") != "blocked"
                                and str(item.get("exchange") or "").strip()
                            }
                        )
                        if not runnable_calendar_exchanges:
                            await self._send_task_report(
                                report_data={
                                    'name': '商品期货官方交易日历前置回填报告',
                                    'content': _format_futures_market_data_scheduler_report(calendar_backfill_result),
                                    'status': 'error',
                                    'tasks_completed': 0,
                                    'duration': 'N/A',
                                    'maintenance_tasks': [
                                        {
                                            'task_name': 'futures_official_calendar_backfill',
                                            'status': "; ".join(
                                                calendar_backfill_result.get("blockers") or ["blocked"]
                                            ),
                                        }
                                    ],
                                },
                                report_type='maintenance_report',
                                task_name='商品期货官方交易日历前置回填',
                                job_config=job_config,
                            )
                            return False
                        scheduler_logger.warning(
                            "[Scheduler] Futures official calendar preflight partially blocked; "
                            "continuing with runnable exchanges blocked=%s runnable=%s",
                            blocked_calendar_exchanges,
                            runnable_calendar_exchanges,
                        )
                        await self._send_task_report(
                            report_data={
                                'name': '商品期货官方交易日历前置回填报告',
                                'content': _format_futures_market_data_scheduler_report(calendar_backfill_result),
                                'status': 'warning',
                                'tasks_completed': len(runnable_calendar_exchanges),
                                'duration': 'N/A',
                                'maintenance_tasks': [
                                    {
                                        'task_name': 'futures_official_calendar_backfill',
                                        'status': (
                                            "blocked="
                                            + ",".join(blocked_calendar_exchanges or ["unknown"])
                                            + "; continued="
                                            + ",".join(runnable_calendar_exchanges)
                                        ),
                                    }
                                ],
                            },
                            report_type='maintenance_report',
                            task_name='商品期货官方交易日历前置回填',
                            job_config=job_config,
                        )
                        effective_scope_id = None
                        effective_scope_ids = None
                        effective_exchanges = runnable_calendar_exchanges
                else:
                    scheduler_logger.info(
                        "[Scheduler] Futures official calendar preflight skipped exchanges=%s scope_id=%s "
                        "scope_ids=%s start=%s end=%s",
                        effective_exchanges,
                        effective_scope_id,
                        effective_scope_ids,
                        calendar_start_date,
                        calendar_end_date,
                    )
                governance_result = await data_manager.run_futures_trading_day_governance(
                    scope_id=effective_scope_id,
                    scope_ids=effective_scope_ids,
                    exchanges=effective_exchanges,
                    categories=categories,
                    instrument_ids=instrument_ids,
                    series_ids=series_ids,
                    series_types=series_types,
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=dry_run,
                )
                governance_status = governance_result.get("status")
                if governance_status == "blocked" and not dry_run:
                    await self._send_task_report(
                        report_data={
                            'name': '商品期货交易日治理前置检查报告',
                            'content': _format_futures_market_data_scheduler_report(governance_result),
                            'status': 'error',
                            'tasks_completed': 0,
                            'duration': 'N/A',
                            'maintenance_tasks': [
                                {
                                    'task_name': 'futures_trading_day_governance',
                                    'status': "; ".join(
                                        governance_result.get("target_date_expansion", {}).get("blockers") or
                                        governance_result.get("readiness", {}).get("blockers") or
                                        ["blocked"]
                                    ),
                                }
                            ],
                        },
                        report_type='maintenance_report',
                        task_name='商品期货交易日治理前置检查',
                        job_config=job_config,
                    )
                    return False
            if requires_master_data_governance:
                master_start_date = start_date
                master_end_date = end_date
                target_dates_by_exchange = (
                    (governance_result.get("target_date_expansion") or {}).get("target_dates_by_exchange") or {}
                    if requires_trading_day_governance
                    else {}
                )
                target_dates = sorted(
                    {
                        str(trade_date)
                        for dates in target_dates_by_exchange.values()
                        for trade_date in (dates or [])
                        if trade_date
                    }
                )
                if target_dates and not (master_start_date or master_end_date):
                    master_start_date = target_dates[0]
                    master_end_date = target_dates[-1]
                if not target_dates and requires_trading_day_governance:
                    scheduler_logger.info(
                        "[Scheduler] Futures master governance skipped because trading-day governance returned no target dates"
                    )
                else:
                    exchange_dates = {
                        str(exchange).upper(): sorted(str(item) for item in dates or [] if item)
                        for exchange, dates in target_dates_by_exchange.items()
                        if str(exchange).strip()
                    }
                    if exchange_dates:
                        for exchange, dates in sorted(exchange_dates.items()):
                            if not dates:
                                continue
                            master_results.append(
                                await data_manager.run_futures_master_governance(
                                    exchanges=[exchange],
                                    categories=categories,
                                    instrument_ids=instrument_ids,
                                    series_ids=series_ids,
                                    series_types=series_types,
                                    start_date=dates[0],
                                    end_date=dates[-1],
                                    dry_run=dry_run,
                                    max_days=master_governance_max_days,
                                )
                            )
                    else:
                        master_results.append(
                            await data_manager.run_futures_master_governance(
                                scope_id=effective_scope_id,
                                scope_ids=effective_scope_ids,
                                exchanges=effective_exchanges,
                                categories=categories,
                                instrument_ids=instrument_ids,
                                series_ids=series_ids,
                                series_types=series_types,
                                start_date=master_start_date,
                                end_date=master_end_date,
                                dry_run=dry_run,
                                max_days=master_governance_max_days,
                            )
                        )
                    blocked_master_results = [
                        item for item in master_results
                        if item.get("status") == "blocked"
                    ]
                    if blocked_master_results and not dry_run:
                        blocker_lines = [
                            f"{item.get('exchange', 'N/A')}: "
                            + "; ".join(item.get("blockers") or [item.get("reason") or "blocked"])
                            for item in blocked_master_results
                        ]
                        master_result = {
                            "status": "blocked",
                            "domain": "futures_master_governance",
                            "exchange": ",".join(
                                str(item.get("exchange") or "N/A") for item in blocked_master_results
                            ),
                            "source_profile": "exchange_official_daily_contract_discovery",
                            "start_date": master_start_date,
                            "end_date": master_end_date,
                            "dry_run": dry_run,
                            "counts": {},
                            "contracts": [],
                            "blockers": blocker_lines,
                            "warnings": [],
                        }
                        await self._send_task_report(
                            report_data={
                                'name': '商品期货主数据治理前置检查报告',
                                'content': _format_futures_market_data_scheduler_report(master_result),
                                'status': 'error',
                                'tasks_completed': 0,
                                'duration': 'N/A',
                                'maintenance_tasks': [
                                    {
                                        'task_name': 'futures_master_governance',
                                        'status': "; ".join(master_result.get("blockers") or ["blocked"]),
                                    }
                                ],
                            },
                            report_type='maintenance_report',
                            task_name='商品期货主数据治理前置检查',
                            job_config=job_config,
                        )
                        return False
            result = await data_manager.run_futures_market_data_sync(
                scope_id=effective_scope_id,
                scope_ids=effective_scope_ids,
                exchanges=effective_exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                mode=mode,
                dry_run=dry_run,
            )
            if master_results:
                master_governance_summary = _summarize_futures_master_governance_results(master_results)
                result["master_data_governance"] = master_governance_summary
                try:
                    run_id = result.get("run_id")
                    if run_id is not None:
                        data_manager._require_futures_storage().heartbeat_ingestion_run(
                            int(run_id),
                            metadata={"master_data_governance": master_governance_summary},
                        )
                except Exception as metadata_error:
                    scheduler_logger.warning(
                        "[Scheduler] Failed to persist futures master governance report context "
                        "run_id=%s error=%s",
                        result.get("run_id"),
                        metadata_error,
                    )
            status = result.get('status', 'failed')
            if (
                dry_run
                and requires_trading_day_governance
                and str(governance_result.get("status") or "") != "success"
                and status == "success"
            ):
                status = "partial"
                result["status"] = status
                result["governance_preflight"] = {
                    "status": governance_result.get("status"),
                    "blockers": (
                        (governance_result.get("target_date_expansion") or {}).get("blockers")
                        or []
                    ),
                    "warnings": (
                        (governance_result.get("target_date_expansion") or {}).get("warnings")
                        or []
                    ),
                }
            if blocked_calendar_exchanges:
                result["calendar_preflight"] = {
                    "status": "blocked",
                    "blocked_exchanges": blocked_calendar_exchanges,
                    "continued_exchanges": effective_exchanges or [],
                }
                if status == "success":
                    status = "partial"
                result["status"] = status
                try:
                    run_id = result.get("run_id")
                    if run_id is not None:
                        data_manager._require_futures_storage().finish_ingestion_run(
                            int(run_id),
                            status=status,
                            metadata=result,
                        )
                except Exception as metadata_error:
                    scheduler_logger.warning(
                        "[Scheduler] Failed to persist calendar preflight status context "
                        "run_id=%s error=%s",
                        result.get("run_id"),
                        metadata_error,
                    )
            success = status == 'success'
            reports = _format_futures_market_data_scheduler_reports(result)
            report_mode = (
                "compact"
                if len(reports) == 1 and "*商品期货行情日更*" in reports[0]
                else "detailed"
            )
            scheduler_logger.info(
                "[Scheduler] Futures market-data report prepared run_id=%s status=%s "
                "report_mode=%s report_count=%s inserted=%s changed=%s unchanged=%s failed=%s",
                result.get("run_id"),
                status,
                report_mode,
                len(reports),
                (result.get("totals") or {}).get("inserted", 0),
                (result.get("totals") or {}).get("changed", 0),
                (result.get("totals") or {}).get("unchanged", 0),
                (result.get("totals") or {}).get("failed", 0),
            )
            for index, content in enumerate(reports, start=1):
                await self._send_task_report(
                    report_data={
                        'name': '商品期货行情数据维护报告',
                        'content': content,
                        'status': 'success' if success else status,
                        'tasks_completed': len(result.get('series') or []),
                        'duration': 'N/A',
                        'maintenance_tasks': [
                            {
                                'task_name': item.get('series_id', 'unknown'),
                                'status': str((item.get('write_result') or {}).get('inserted', 0)),
                            }
                            for item in (result.get('series') or [])
                        ],
                    },
                    report_type='maintenance_report',
                    task_name=(
                        '商品期货行情数据维护'
                        if index == 1
                        else f'商品期货行情数据维护明细{index - 1}'
                    ),
                    job_config=job_config,
                )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures market-data sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '商品期货行情数据维护报告',
                    'content': _format_futures_market_data_scheduler_report({
                        'status': 'error',
                        'reason': str(e),
                        'totals': {},
                        'series': [],
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'futures_market_data_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货行情数据维护',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('futures_market_data_sync')

    async def fx_master_sync(self, job_config: Optional[JobConfig] = None) -> bool:
        """外汇主数据治理任务。"""
        self._active_tasks.add('fx_master_sync')
        try:
            result = await data_manager.run_fx_master_sync()
            success = result.get("status") == "success"
            await self._send_task_report(
                report_data={
                    'name': '外汇主数据治理报告',
                    'content': _format_fx_market_data_scheduler_report(result),
                    'status': 'success' if success else result.get("status", "error"),
                    'tasks_completed': sum((result.get("counts") or {}).values()),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_master_sync', 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='外汇主数据治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] FX master sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '外汇主数据治理报告',
                    'content': _format_fx_market_data_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_master_sync', 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='外汇主数据治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('fx_master_sync')

    async def fx_calendar_governance(
        self,
        source_profiles: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """外汇发布日历治理任务。"""
        self._active_tasks.add('fx_calendar_governance')
        try:
            result = await data_manager.run_fx_calendar_governance(
                source_profiles=source_profiles,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            success = result.get("status") == "success"
            await self._send_task_report(
                report_data={
                    'name': '外汇发布日历治理报告',
                    'content': _format_fx_market_data_scheduler_report(result),
                    'status': 'success' if success else result.get("status", "error"),
                    'tasks_completed': int(result.get("rows") or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_calendar_governance', 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='外汇发布日历治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] FX calendar governance failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '外汇发布日历治理报告',
                    'content': _format_fx_market_data_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_calendar_governance', 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='外汇发布日历治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('fx_calendar_governance')

    async def fx_rate_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """外汇汇率日更任务。"""
        self._active_tasks.add('fx_rate_sync')
        try:
            result = await data_manager.run_fx_rate_sync(
                scope_id=scope_id,
                scope_ids=scope_ids,
                series_ids=series_ids,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            success = result.get("status") == "success"
            await self._send_task_report(
                report_data={
                    'name': '外汇汇率数据维护报告',
                    'content': _format_fx_market_data_scheduler_report(result),
                    'status': 'success' if success else result.get("status", "error"),
                    'tasks_completed': len((result.get("scope_selection") or {}).get("series_ids") or []),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_rate_sync', 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='外汇汇率数据维护',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] FX rate sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '外汇汇率数据维护报告',
                    'content': _format_fx_market_data_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_rate_sync', 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='外汇汇率数据维护',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('fx_rate_sync')

    async def risk_free_rate_sync(
        self,
        data_as_of: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """无风险利率序列日更任务 (REQ-13)。

        采集失败降级为空写入, 不抛出; 只写 risk_free_rate_* 表, 消费端只读。
        注: 需在 config/05_scheduler.json 注册调度条目后才会周期运行。
        """
        self._active_tasks.add('risk_free_rate_sync')
        try:
            result = await data_manager.sync_risk_free_rate(data_as_of=data_as_of)
            success = result.get("status") in ("ok", "empty")
            scheduler_logger.info(f"[Scheduler] risk_free_rate_sync: {result}")
            await self._send_task_report(
                report_data={
                    'name': '无风险利率序列同步报告',
                    'status': 'success' if success else 'error',
                    'content': _format_risk_free_rate_sync_report(result),
                },
                report_type='maintenance_report',
                task_name='无风险利率序列同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] risk_free_rate_sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '无风险利率序列同步报告',
                    'status': 'error',
                    'content': _format_risk_free_rate_sync_report(
                        {'status': 'error', 'reason': str(e)}
                    ),
                },
                report_type='maintenance_report',
                task_name='无风险利率序列同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('risk_free_rate_sync')

    async def fx_rate_backfill(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """外汇汇率历史回补任务，要求显式日期范围。"""
        if not start_date or not end_date:
            raise ValueError("fx_rate_backfill requires start_date and end_date")
        return await self.fx_rate_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            series_ids=series_ids,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            job_config=job_config,
        )

    async def _run_special_commodity_observation_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_days: int = 10,
        window_mode: str = 'rolling',
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
        _task_id: Optional[str] = None,
        _task_name: Optional[str] = None,
    ) -> bool:
        """Run one governed special-commodity observation task."""
        if not _task_id or not _task_name:
            raise ValueError('special commodity task identity is required')
        self._active_tasks.add(_task_id)
        try:
            start_date, end_date = _resolve_special_commodity_task_window(
                start_date,
                end_date,
                lookback_days=lookback_days,
                window_mode=window_mode,
            )
            scheduler_logger.info(
                "[Scheduler] Starting special commodity task: task_id=%s task_name=%s scope_id=%s scope_ids=%s start=%s end=%s lookback_days=%s window_mode=%s dry_run=%s",
                _task_id,
                _task_name,
                scope_id,
                scope_ids,
                start_date,
                end_date,
                lookback_days,
                window_mode,
                dry_run,
            )
            result = await data_manager.run_special_commodity_price_sync(
                scope_id=scope_id,
                scope_ids=scope_ids,
                venues=venues,
                categories=categories,
                commodity_ids=commodity_ids,
                series_ids=series_ids,
                frequencies=frequencies,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            success = result.get("status") == "success"
            await self._send_task_report(
                report_data={
                    'name': f'{_task_name}报告',
                    'content': _format_special_commodity_scheduler_report(
                        result, title=_task_name
                    ),
                    'status': 'success' if success else result.get("status", "error"),
                    'tasks_completed': int(result.get("target_series", 0) or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': _task_id, 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name=_task_name,
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.exception(
                "[Scheduler] Special commodity task failed: task_id=%s task_name=%s",
                _task_id,
                _task_name,
            )
            await self._send_task_report(
                report_data={
                    'name': f'{_task_name}报告',
                    'content': _format_special_commodity_scheduler_report(
                        {'status': 'error', 'reason': str(e)}, title=_task_name
                    ),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': _task_id, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name=_task_name,
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(_task_id)

    async def special_commodity_overseas_daily_price_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_days: int = 10,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """同步海外特殊商品日频价格。"""
        return await self._run_special_commodity_observation_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            venues=venues,
            categories=categories,
            commodity_ids=commodity_ids,
            series_ids=series_ids,
            frequencies=frequencies or ['daily'],
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            dry_run=dry_run,
            job_config=job_config,
            _task_id='special_commodity_overseas_daily_price_sync',
            _task_name='海外特殊商品日频价格同步',
        )

    async def special_commodity_domestic_spot_price_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_days: int = 10,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """国内特殊商品现货与价格基准同步，共用治理与持久化链路。"""
        return await self._run_special_commodity_observation_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            venues=venues,
            categories=categories,
            commodity_ids=commodity_ids,
            series_ids=series_ids,
            frequencies=frequencies,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            dry_run=dry_run,
            job_config=job_config,
            _task_id='special_commodity_domestic_spot_price_sync',
            _task_name='国内特殊商品现货价格与基准同步',
        )

    async def special_commodity_industrial_indicator_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_days: int = 10,
        window_mode: str = 'provider_latest',
        scope_runs: Optional[List[Dict[str, Any]]] = None,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Aggregate independently scheduled industrial-indicator scopes."""
        task_id = 'special_commodity_industrial_indicator_sync'
        task_name = '大宗商品非价格产业指标聚合同步'
        self._active_tasks.add(task_id)
        try:
            explicit_selection = any(
                (
                    scope_id,
                    scope_ids,
                    venues,
                    categories,
                    commodity_ids,
                    series_ids,
                    frequencies,
                    start_date,
                    end_date,
                )
            )
            if scope_runs and not explicit_selection:
                as_of_date = get_shanghai_time().date()
                results: List[Dict[str, Any]] = []
                skipped_scopes: List[Dict[str, Any]] = []
                for scope_run in scope_runs:
                    configured_scope = str(scope_run.get('scope_id') or '').strip()
                    if not configured_scope:
                        raise ValueError('industrial indicator scope_run requires scope_id')
                    if not _special_commodity_scope_run_due(
                        scope_run, as_of_date=as_of_date
                    ):
                        skip_reason = (
                            'disabled'
                            if scope_run.get('enabled') is False
                            else 'not_due'
                        )
                        skipped_scopes.append(
                            {
                                'scope_id': configured_scope,
                                'reason': skip_reason,
                                'run_days_of_month': scope_run.get('run_days_of_month', []),
                            }
                        )
                        scheduler_logger.info(
                            "[Scheduler] Industrial indicator scope skipped: scope_id=%s reason=%s as_of=%s",
                            configured_scope,
                            skip_reason,
                            as_of_date,
                        )
                        continue
                    scope_start, scope_end = _resolve_special_commodity_scope_run_window(
                        scope_run, as_of_date=as_of_date
                    )
                    scheduler_logger.info(
                        "[Scheduler] Industrial indicator scope start: scope_id=%s window_mode=%s start=%s end=%s dry_run=%s",
                        configured_scope,
                        scope_run.get('window_mode'),
                        scope_start,
                        scope_end,
                        dry_run,
                    )
                    try:
                        result = await data_manager.run_special_commodity_price_sync(
                            scope_id=configured_scope,
                            start_date=scope_start,
                            end_date=scope_end,
                            dry_run=dry_run,
                        )
                    except Exception as exc:
                        scheduler_logger.exception(
                            "[Scheduler] Industrial indicator scope failed: scope_id=%s",
                            configured_scope,
                        )
                        result = {
                            'status': 'error',
                            'dry_run': dry_run,
                            'target_series': 0,
                            'fetched_rows': 0,
                            'inserted': 0,
                            'changed': 0,
                            'unchanged': 0,
                            'would_write': 0,
                            'master_data_governance': 'error',
                            'date_governance': 'error',
                            'warnings': [],
                            'blockers': [
                                {
                                    'reason': 'industrial_scope_sync_failed',
                                    'error': str(exc),
                                }
                            ],
                            'per_source': {},
                        }
                    result['scope_id'] = configured_scope
                    results.append(result)
                combined = _aggregate_special_commodity_scope_results(
                    results, skipped_scopes
                )
            else:
                resolved_start, resolved_end = _resolve_special_commodity_task_window(
                    start_date,
                    end_date,
                    lookback_days=lookback_days,
                    window_mode=window_mode,
                )
                combined = await data_manager.run_special_commodity_price_sync(
                    scope_id=scope_id,
                    scope_ids=scope_ids,
                    venues=venues,
                    categories=categories,
                    commodity_ids=commodity_ids,
                    series_ids=series_ids,
                    frequencies=frequencies,
                    start_date=resolved_start,
                    end_date=resolved_end,
                    dry_run=dry_run,
                )
                combined['scope_id'] = scope_id
            success = combined.get('status') in {'success', 'skipped'}
            await self._send_task_report(
                report_data={
                    'name': '大宗商品非价格产业指标聚合同步报告',
                    'content': _format_special_commodity_scheduler_report(
                        combined, title=task_name
                    ),
                    'status': 'success' if success else combined.get('status', 'error'),
                    'tasks_completed': int(combined.get('target_series', 0) or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': task_id, 'status': combined.get('status')}
                    ],
                },
                report_type='maintenance_report',
                task_name=task_name,
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(
                "[Scheduler] Industrial indicator sync failed: %s", e
            )
            await self._send_task_report(
                report_data={
                    'name': '大宗商品非价格产业指标聚合同步报告',
                    'content': _format_special_commodity_scheduler_report(
                        {'status': 'error', 'reason': str(e)}, title=task_name
                    ),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': task_id, 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name=task_name,
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_id)

    async def special_commodity_observation_backfill(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """特殊商品价格和产业指标历史观测回补，要求显式日期范围。"""
        if not start_date or not end_date:
            raise ValueError(
                "special_commodity_observation_backfill requires start_date and end_date"
            )
        return await self._run_special_commodity_observation_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            venues=venues,
            categories=categories,
            commodity_ids=commodity_ids,
            series_ids=series_ids,
            frequencies=frequencies,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            job_config=job_config,
            _task_id='special_commodity_observation_backfill',
            _task_name='特殊商品价格与产业指标历史回补',
        )

    async def special_commodity_international_monthly_price_sync(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        lookback_months: int = 6,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Monthly special-commodity sync using the shared governance-first path."""
        start_date, end_date = _resolve_special_commodity_monthly_sync_window(
            start_date,
            end_date,
            lookback_months=lookback_months,
        )
        scheduler_logger.info(
            "[Scheduler] Starting monthly special commodity sync: scope_id=%s scope_ids=%s "
            "start=%s end=%s lookback_months=%s dry_run=%s",
            scope_id,
            scope_ids,
            start_date,
            end_date,
            lookback_months,
            dry_run,
        )
        return await self._run_special_commodity_observation_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            venues=venues,
            categories=categories,
            commodity_ids=commodity_ids,
            series_ids=series_ids,
            frequencies=frequencies or ["monthly"],
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
            job_config=job_config,
            _task_id='special_commodity_international_monthly_price_sync',
            _task_name='国际特殊商品月度价格基准同步',
        )

    async def special_commodity_calendar_governance(
        self,
        scope_id: Optional[str] = None,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """特殊商品观测/发布日历治理任务。"""
        self._active_tasks.add('special_commodity_calendar_governance')
        try:
            result = await data_manager.run_special_commodity_calendar_governance(
                scope_id=scope_id,
                series_ids=series_ids,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            success = result.get("status") == "success"
            await self._send_task_report(
                report_data={
                    'name': '特殊商品发布日历治理报告',
                    'content': _format_special_commodity_scheduler_report(result),
                    'status': 'success' if success else result.get("status", "error"),
                    'tasks_completed': int(result.get("target_series", 0) or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'special_commodity_calendar_governance', 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='特殊商品发布日历治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Special commodity calendar governance failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '特殊商品发布日历治理报告',
                    'content': _format_special_commodity_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'special_commodity_calendar_governance', 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='特殊商品发布日历治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('special_commodity_calendar_governance')

    async def special_commodity_policy_governance_sync(
        self,
        adapter_id: str = "ndrc",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """特殊商品官方政策目录发现与候选治理任务。"""
        task_id = 'special_commodity_policy_governance_sync'
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.run_special_commodity_policy_discovery(
                adapter_id=adapter_id,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            success = result.get("status") in {"success", "warning"}
            await self._send_task_report(
                report_data={
                    'name': '特殊商品政策目录发现与事件治理报告',
                    'content': _format_special_commodity_scheduler_report(result),
                    'status': result.get("status", "error"),
                    'tasks_completed': int(result.get("documents", 0) or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_id, 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='特殊商品政策目录发现与事件治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error("[Scheduler] Special commodity policy discovery failed: %s", e)
            await self._send_task_report(
                report_data={
                    'name': '特殊商品政策目录发现与事件治理报告',
                    'content': _format_special_commodity_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_id, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='特殊商品政策目录发现与事件治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_id)

    async def special_commodity_policy_candidate_review(
        self,
        candidate_ref: str,
        decision: str,
        reviewer: str = "telegram_operator",
        notes: str = "",
        promote: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """人工审核政策候选；正式事件仍由独立治理任务提升。"""
        task_id = "special_commodity_policy_candidate_review"
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.review_special_commodity_policy_candidate(
                candidate_ref=candidate_ref,
                decision=decision,
                reviewer=reviewer,
                notes=notes,
                promote=promote,
            )
            success = result.get("status") == "success"
            await self._send_task_report(
                report_data={
                    "name": "特殊商品政策候选审核报告",
                    "content": _format_special_commodity_scheduler_report(result),
                    "status": result.get("status", "error"),
                    "tasks_completed": 1 if success else 0,
                    "duration": "N/A",
                    "maintenance_tasks": [{"task_name": task_id, "status": result.get("status")}],
                },
                report_type="maintenance_report",
                task_name="特殊商品政策候选审核",
                job_config=job_config,
            )
            return success
        except Exception as exc:
            scheduler_logger.error("[Scheduler] Special commodity policy candidate review failed: %s", exc)
            return False
        finally:
            self._active_tasks.discard(task_id)

    async def special_commodity_series_catalog_sync(
        self,
        dry_run: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """特殊商品扩品候选目录同步任务。"""
        task_id = 'special_commodity_series_catalog_sync'
        self._active_tasks.add(task_id)
        try:
            result = await data_manager.run_special_commodity_series_catalog_sync(dry_run=dry_run)
            success = result.get("status") in {"success", "warning"}
            await self._send_task_report(
                report_data={
                    'name': '特殊商品扩品候选目录报告',
                    'content': _format_special_commodity_scheduler_report(result),
                    'status': result.get("status", "error"),
                    'tasks_completed': int(result.get("candidates", 0) or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_id, 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='特殊商品扩品候选目录',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error("[Scheduler] Special commodity series catalog sync failed: %s", e)
            return False
        finally:
            self._active_tasks.discard(task_id)

    async def fx_derivation_sync(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """外汇派生汇率同步任务。"""
        self._active_tasks.add('fx_derivation_sync')
        try:
            result = await data_manager.run_fx_derivation_sync(
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            success = result.get("status") in {"success", "partial"}
            await self._send_task_report(
                report_data={
                    'name': '外汇派生汇率维护报告',
                    'content': _format_fx_market_data_scheduler_report(result),
                    'status': 'success' if success else result.get("status", "error"),
                    'tasks_completed': int((result.get("totals") or {}).get("inserted", 0)),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_derivation_sync', 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='外汇派生汇率维护',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] FX derivation sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '外汇派生汇率维护报告',
                    'content': _format_fx_market_data_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_derivation_sync', 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='外汇派生汇率维护',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('fx_derivation_sync')

    async def fx_quality_check(
        self,
        as_of_date: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """外汇数据质量检查任务。"""
        self._active_tasks.add('fx_quality_check')
        try:
            result = await data_manager.run_fx_quality_check(as_of_date=as_of_date)
            success = result.get("status") in {"success", "warning"}
            await self._send_task_report(
                report_data={
                    'name': '外汇数据质量检查报告',
                    'content': _format_fx_market_data_scheduler_report(result.get("readiness") or result),
                    'status': result.get("status", "success") if success else result.get("status", "error"),
                    'tasks_completed': int(result.get("issues_recorded") or 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_quality_check', 'status': result.get("status")}],
                },
                report_type='maintenance_report',
                task_name='外汇数据质量检查',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] FX quality check failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '外汇数据质量检查报告',
                    'content': _format_fx_market_data_scheduler_report({'status': 'error', 'reason': str(e)}),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': 'fx_quality_check', 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='外汇数据质量检查',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('fx_quality_check')

    async def futures_market_data_backfill(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
        dry_run: bool = False,
        requires_trading_calendar_backfill: bool = True,
        requires_trading_day_governance: bool = True,
        requires_master_data_governance: bool = True,
        master_governance_max_days: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货行情历史回补任务，要求显式日期范围。"""
        if not start_date or not end_date:
            raise ValueError("futures_market_data_backfill requires start_date and end_date")
        return await self.futures_market_data_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            dry_run=dry_run,
            requires_trading_calendar_backfill=requires_trading_calendar_backfill,
            requires_trading_day_governance=requires_trading_day_governance,
            requires_master_data_governance=requires_master_data_governance,
            master_governance_max_days=master_governance_max_days,
            job_config=job_config,
        )

    async def futures_trading_day_governance(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货交易日治理前置任务。"""
        self._active_tasks.add('futures_trading_day_governance')
        try:
            result = await data_manager.run_futures_trading_day_governance(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'warning', 'partial'} or (dry_run and status == 'blocked')
            await self._send_task_report(
                report_data={
                    'name': '商品期货交易日治理报告',
                    'content': _format_futures_market_data_scheduler_report(result),
                    'status': 'success' if success else 'error',
                    'tasks_completed': 1 if success else 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {
                            'task_name': 'futures_trading_day_governance',
                            'status': status,
                        }
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货交易日治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures trading-day governance failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '商品期货交易日治理报告',
                    'content': _format_futures_market_data_scheduler_report({
                        'status': 'error',
                        'reason': str(e),
                        'target_date_expansion': {},
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'futures_trading_day_governance', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货交易日治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('futures_trading_day_governance')

    async def futures_official_calendar_backfill(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        max_days: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货官方交易日历回填任务；不下载行情价格。"""
        self._active_tasks.add('futures_official_calendar_backfill')
        try:
            result = await data_manager.run_futures_official_calendar_backfill(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                max_days=max_days,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'warning', 'partial'} or (dry_run and status == 'blocked')
            await self._send_task_report(
                report_data={
                    'name': '商品期货官方交易日历回填报告',
                    'content': _format_futures_market_data_scheduler_report(result),
                    'status': 'success' if success else 'error',
                    'tasks_completed': 1 if success else 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {
                            'task_name': 'futures_official_calendar_backfill',
                            'status': status,
                        }
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货官方交易日历回填',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures official calendar backfill failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '商品期货官方交易日历回填报告',
                    'content': _format_futures_market_data_scheduler_report({
                        'status': 'error',
                        'domain': 'futures_official_trading_calendar_backfill',
                        'reason': str(e),
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'futures_official_calendar_backfill', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货官方交易日历回填',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('futures_official_calendar_backfill')

    async def futures_master_governance(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        max_days: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货主数据治理任务；按交易所执行官方日行情合约发现。"""
        self._active_tasks.add('futures_master_governance')
        try:
            result = await data_manager.run_futures_master_governance(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                max_days=max_days,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'warning'} or (dry_run and status == 'blocked')
            await self._send_task_report(
                report_data={
                    'name': '商品期货主数据治理报告',
                    'content': _format_futures_market_data_scheduler_report(result),
                    'status': 'success' if success else 'error',
                    'tasks_completed': 1 if success else 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {
                            'task_name': 'futures_master_governance',
                            'status': status,
                        }
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货主数据治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures master governance failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '商品期货主数据治理报告',
                    'content': _format_futures_market_data_scheduler_report({
                        'status': 'error',
                        'domain': 'futures_master_governance',
                        'reason': str(e),
                        'counts': {},
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'futures_master_governance', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货主数据治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('futures_master_governance')

    async def futures_master_discovery_governance(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        max_days: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货主数据发现治理任务；按交易所 adapter 发现未知品种。"""
        self._active_tasks.add('futures_master_discovery_governance')
        try:
            result = await data_manager.run_futures_master_discovery_governance(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                max_days=max_days,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'warning'} or (dry_run and status == 'blocked')
            await self._send_task_report(
                report_data={
                    'name': '商品期货主数据发现治理报告',
                    'content': _format_futures_market_data_scheduler_report(result),
                    'status': 'success' if success else 'error',
                    'tasks_completed': 1 if success else 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {
                            'task_name': 'futures_master_discovery_governance',
                            'status': status,
                        }
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货主数据发现治理',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures master discovery governance failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '商品期货主数据发现治理报告',
                    'content': _format_futures_market_data_scheduler_report({
                        'status': 'error',
                        'domain': 'futures_master_discovery_governance',
                        'reason': str(e),
                        'counts': {},
                    }),
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'futures_master_discovery_governance', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货主数据发现治理',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('futures_master_discovery_governance')

    async def futures_cycle_diagnostics_refresh(
        self,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货周期诊断刷新任务。"""
        self._active_tasks.add('futures_cycle_diagnostics_refresh')
        try:
            result = await data_manager.refresh_futures_cycle_diagnostics(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
            )
            success = result.get('status') == 'success'
            await self._send_task_report(
                report_data={
                    'name': '商品期货周期诊断刷新报告',
                    'status': 'success' if success else result.get('status', 'failed'),
                    'tasks_completed': result.get('series_count', 0),
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'diagnostics_written', 'status': str(result.get('diagnostics_written', 0))}
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货周期诊断刷新',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures diagnostics refresh failed: {e}")
            return False
        finally:
            self._active_tasks.discard('futures_cycle_diagnostics_refresh')

    async def futures_spread_recompute(
        self,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货价差重算任务。"""
        self._active_tasks.add('futures_spread_recompute')
        try:
            result = await data_manager.recompute_futures_spreads()
            success = result.get('status') == 'success'
            await self._send_task_report(
                report_data={
                    'name': '商品期货价差重算报告',
                    'status': 'success' if success else result.get('status', 'failed'),
                    'tasks_completed': len(result.get('spreads') or []),
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {
                            'task_name': item.get('spread_id', 'unknown'),
                            'status': f"values={item.get('values_written', 0)}",
                        }
                        for item in (result.get('spreads') or [])
                    ],
                },
                report_type='maintenance_report',
                task_name='商品期货价差重算',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Futures spread recompute failed: {e}")
            return False
        finally:
            self._active_tasks.discard('futures_spread_recompute')

    async def industry_standard_gap_fill(
        self,
        exchanges: Optional[List[str]] = None,
        missing_limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 strict Shenwan authoritative membership 缺口检测与定向修复任务。"""
        self._active_tasks.add('industry_standard_gap_fill')
        try:
            scheduler_logger.info("[Scheduler] Starting industry standard gap fill...")

            result = await data_manager.run_industry_standard_gap_fill_sync(
                exchanges=exchanges,
                missing_limit_per_exchange=missing_limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded', 'skipped'}
            coverage_before = result.get('coverage_before') or {}
            coverage_after = result.get('coverage_after') or {}
            missing_before = int(
                coverage_before.get('missing_authoritative_membership_count', 0)
            )
            missing_after = int(
                coverage_after.get('missing_authoritative_membership_count', 0)
            )
            repair_targets = int(result.get('targeted_instrument_count', 0))
            repaired = int(result.get('repaired_instrument_count', 0))

            scheduler_logger.info(
                "[Scheduler] Industry standard gap fill summary: status=%s, targets=%s, repaired=%s, missing_before=%s, missing_after=%s",
                status,
                repair_targets,
                repaired,
                missing_before,
                missing_after,
            )

            sync_result = result.get('sync') or {}
            maintenance_tasks = [
                {
                    'task_name': 'coverage_before',
                    'status': (
                        f"missing={missing_before} "
                        f"target={coverage_before.get('target_instrument_count', 0)}"
                    ),
                },
                {
                    'task_name': 'gap_fill',
                    'status': (
                        f"{status} "
                        f"(targets={repair_targets}, repaired={repaired})"
                    ),
                },
            ]
            for exchange_result in sync_result.get('exchanges', []):
                maintenance_tasks.append(
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('memberships_written', 0)} memberships)"
                        ),
                    }
                )

            report_data = {
                'name': '申万标准行业缺口修复报告',
                'status': (
                    'success'
                    if status in {'success', 'skipped'}
                    else 'warning' if status == 'degraded' else 'error'
                ),
                'tasks_completed': repaired,
                'duration': 'N/A',
                'maintenance_tasks': maintenance_tasks,
                'summary': {
                    'missing_before': missing_before,
                    'missing_after': missing_after,
                    'repair_targets': repair_targets,
                    'repaired_instrument_count': repaired,
                },
            }

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='申万标准行业缺口修复',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Industry standard gap fill failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '申万标准行业缺口修复报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'industry_standard_gap_fill', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='申万标准行业缺口修复',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_standard_gap_fill')

    async def industry_official_mapping_refresh(
        self,
        exchanges: Optional[List[str]] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域申万官方行业码映射缓存刷新任务。"""
        self._active_tasks.add('industry_official_mapping_refresh')
        try:
            scheduler_logger.info("[Scheduler] Starting industry official mapping refresh...")

            result = await data_manager.run_industry_official_mapping_refresh(
                exchanges=exchanges,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            source = result.get('source') or 'industry_official_mapping_refresh'
            mode = result.get('mode')
            task_name = source if mode is None else f"{source}:{mode}"
            status_parts = [
                status,
                f"rows={result.get('mapping_cache_rows_written', 0)}",
                f"mapped={result.get('mapped_code_count', 0)}/{result.get('total_code_count', 0)}",
                f"components={result.get('component_taxonomy_count', 0)}",
            ]

            report_data = {
                'name': '申万官方映射刷新报告',
                'status': 'success' if success else 'error',
                'tasks_completed': 1 if success else 0,
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': task_name,
                        'status': " ".join(status_parts),
                    }
                ],
            }

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='申万官方映射刷新',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(
                f"[Scheduler] Industry official mapping refresh failed: {e}"
            )
            await self._send_task_report(
                report_data={
                    'name': '申万官方映射刷新报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {
                            'task_name': 'industry_official_mapping_refresh',
                            'status': str(e),
                        }
                    ],
                },
                report_type='maintenance_report',
                task_name='申万官方映射刷新',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('industry_official_mapping_refresh')

    async def financial_summary_shadow_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 financial summary 影子同步任务。"""
        self._active_tasks.add('financial_summary_shadow_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting financial summary shadow sync...")

            result = await data_manager.run_financial_summary_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '财务摘要影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('source') or 'no-source'})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务摘要影子同步',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Financial summary shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '财务摘要影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'financial_summary_shadow_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='财务摘要影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('financial_summary_shadow_sync')

    async def shareholder_shadow_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        write_policy: str = "refresh_all",
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 shareholders 影子同步任务。"""
        self._active_tasks.add('shareholder_shadow_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting shareholder shadow sync...")

            result = await data_manager.run_shareholder_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                write_policy=write_policy,
            )
            try:
                readiness = await data_manager.get_research_shareholder_readiness()
            except Exception as readiness_error:
                scheduler_logger.warning(
                    "[Scheduler] Failed to load shareholder readiness for report: %s",
                    readiness_error,
                )
                readiness = None

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '股东摘要影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'content': _format_shareholder_shadow_scheduler_report(
                    result,
                    readiness,
                ),
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('source') or 'no-source'})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='股东摘要影子同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Shareholder shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '股东摘要影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'shareholder_shadow_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='股东摘要影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('shareholder_shadow_sync')

    async def shareholder_reconciliation_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        write_policy: str = "changed_only",
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 shareholders 周期复核与补足任务。"""
        self._active_tasks.add('shareholder_reconciliation_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting shareholder reconciliation sync...")

            result = await data_manager.run_shareholder_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                write_policy=write_policy,
            )
            try:
                readiness = await data_manager.get_research_shareholder_readiness()
            except Exception as readiness_error:
                scheduler_logger.warning(
                    "[Scheduler] Failed to load shareholder readiness for reconciliation report: %s",
                    readiness_error,
                )
                readiness = None

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '股东摘要周期复核与补足报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'content': _format_shareholder_shadow_scheduler_report(
                    result,
                    readiness,
                ),
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('source') or 'no-source'}, "
                            f"write_policy={result.get('write_policy', write_policy)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='股东摘要周期复核与补足',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Shareholder reconciliation sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '股东摘要周期复核与补足报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'shareholder_reconciliation_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='股东摘要周期复核与补足',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('shareholder_reconciliation_sync')

    async def shareholder_incremental_sync(
        self,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages_per_market: Optional[int] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 shareholders 每日增量/变更检查任务。"""
        self._active_tasks.add('shareholder_incremental_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting shareholder incremental sync...")

            result = await data_manager.run_shareholder_incremental_sync(
                exchanges=exchanges,
                lookback_days=lookback_days,
                overlap_days=overlap_days,
                page_size=page_size,
                max_pages_per_market=max_pages_per_market,
                max_candidates=max_candidates,
                pending_recheck_days=pending_recheck_days,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                dry_run=dry_run,
            )
            try:
                readiness = await data_manager.get_research_shareholder_readiness()
            except Exception as readiness_error:
                scheduler_logger.warning(
                    "[Scheduler] Failed to load shareholder readiness for incremental report: %s",
                    readiness_error,
                )
                readiness = None

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '股东摘要每日增量检查报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('changed_instruments', 0),
                'duration': 'N/A',
                'content': _format_shareholder_incremental_scheduler_report(
                    result,
                    readiness,
                ),
                'maintenance_tasks': [
                    {
                        'task_name': 'shareholder_incremental_sync',
                        'status': (
                            f"{status} "
                            f"(candidates={result.get('candidate_instruments', 0)}, "
                            f"written={result.get('snapshots_written', 0)})"
                        ),
                    }
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='股东摘要每日增量检查',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Shareholder incremental sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '股东摘要每日增量检查报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'shareholder_incremental_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='股东摘要每日增量检查',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('shareholder_incremental_sync')

    async def financial_statements_shadow_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 financial statements 影子同步任务。"""
        self._active_tasks.add('financial_statements_shadow_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting financial statements shadow sync...")

            result = await data_manager.run_financial_statements_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}

            report_data = {
                'name': '财务报表影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"({exchange_result.get('source') or 'no-source'})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务报表影子同步',
                job_config=job_config,
            )

            return success

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Financial statements shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '财务报表影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'financial_statements_shadow_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='财务报表影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('financial_statements_shadow_sync')

    async def financial_l1_full_import(
        self,
        exchanges: Optional[List[str]] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        db_path: str = "data/financials.db",
        log_dir: Optional[str] = None,
        limit_per_exchange: Optional[int] = None,
        batch_size: int = 20,
        resume: bool = False,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        financial_disclosure_events_path: Optional[str] = None,
        manifest_only: bool = False,
        start_batch: Optional[int] = None,
        end_batch: Optional[int] = None,
        max_batches: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """财务 L1 本地核心层全量/补处理手工任务。"""
        task_name = 'financial_l1_full_import'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info("[Scheduler] Starting financial L1 full import...")
            result = await data_manager.run_financial_l1_full_import(
                exchanges=exchanges,
                report_periods=report_periods,
                period_window=period_window,
                rolling_quarters=rolling_quarters,
                baseline_report_period=baseline_report_period,
                latest_report_period=latest_report_period,
                db_path=db_path,
                log_dir=log_dir,
                limit_per_exchange=limit_per_exchange,
                batch_size=batch_size,
                resume=resume,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
                financial_disclosure_events_path=financial_disclosure_events_path,
                manifest_only=manifest_only,
                start_batch=start_batch,
                end_batch=end_batch,
                max_batches=max_batches,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'success_with_review', 'manifest_ready'}
            report_data = {
                'name': '财务 L1 本地核心层全量导入报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('completed_batch_count', 0),
                'duration': f"{result.get('elapsed_seconds', 0)}s",
                'content': _format_financial_l1_import_scheduler_report(result),
                'maintenance_tasks': [
                    {
                        'task_name': task_name,
                        'status': (
                            f"{status} "
                            f"(batches={result.get('completed_batch_count', 0)}/"
                            f"{result.get('selected_batch_count', result.get('batch_count', 0))})"
                        ),
                    }
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务 L1 本地核心层全量导入',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Financial L1 full import failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '财务 L1 本地核心层全量导入报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_name, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='财务 L1 本地核心层全量导入',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def _run_broker_risk_control_incremental_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        per_instrument_page_size: Optional[int] = None,
        per_instrument_max_pages: Optional[int] = None,
        limit_instruments: Optional[int] = None,
        instrument_ids: Optional[List[str]] = None,
        report_period_types: Optional[List[str]] = None,
        source_profile: Optional[str] = None,
        include_standalone_supplement: Optional[bool] = None,
        archive_root: Optional[str] = None,
        dry_run: bool = False,
        scan_only: bool = False,
    ) -> Dict[str, Any]:
        """Execute broker regulatory incremental sync and return the raw result."""
        scheduler_logger.info("[Scheduler] Starting broker risk-control incremental sync...")
        return await data_manager.run_broker_risk_control_incremental_sync(
            exchanges=exchanges,
            lookback_days=lookback_days,
            overlap_days=overlap_days,
            page_size=page_size,
            max_pages=max_pages,
            per_instrument_page_size=per_instrument_page_size,
            per_instrument_max_pages=per_instrument_max_pages,
            limit_instruments=limit_instruments,
            instrument_ids=instrument_ids,
            report_period_types=report_period_types,
            source_profile=source_profile,
            include_standalone_supplement=include_standalone_supplement,
            archive_root=archive_root,
            dry_run=dry_run,
            scan_only=scan_only,
        )

    async def broker_risk_control_incremental_sync(
        self,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        per_instrument_page_size: Optional[int] = None,
        per_instrument_max_pages: Optional[int] = None,
        limit_instruments: Optional[int] = None,
        instrument_ids: Optional[List[str]] = None,
        report_period_types: Optional[List[str]] = None,
        source_profile: Optional[str] = None,
        include_standalone_supplement: Optional[bool] = None,
        archive_root: Optional[str] = None,
        dry_run: bool = False,
        scan_only: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """券商风控指标公告增量维护任务。"""
        task_name = 'broker_risk_control_incremental_sync'
        self._active_tasks.add(task_name)
        try:
            result = await self._run_broker_risk_control_incremental_sync(
                exchanges=exchanges,
                lookback_days=lookback_days,
                overlap_days=overlap_days,
                page_size=page_size,
                max_pages=max_pages,
                per_instrument_page_size=per_instrument_page_size,
                per_instrument_max_pages=per_instrument_max_pages,
                limit_instruments=limit_instruments,
                instrument_ids=instrument_ids,
                report_period_types=report_period_types,
                source_profile=source_profile,
                include_standalone_supplement=include_standalone_supplement,
                archive_root=archive_root,
                dry_run=dry_run,
                scan_only=scan_only,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'partial', 'scan_only', 'disabled', 'unavailable'}
            backfill = result.get("backfill") or {}
            report_data = {
                'name': '券商风控指标增量维护报告',
                'status': 'success' if success else 'error',
                'tasks_completed': backfill.get('facts_written', 0),
                'duration': _format_seconds_for_report(result.get('elapsed_seconds')),
                'content': _format_broker_risk_control_scheduler_report(result),
                'maintenance_tasks': [
                    {
                        'task_name': task_name,
                        'status': (
                            f"{status} "
                            f"(reports={backfill.get('reports_parsed', 0)}, "
                            f"facts={backfill.get('facts_parsed', 0)}, "
                            f"written={backfill.get('facts_written', 0)})"
                        ),
                    }
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='券商风控指标增量维护',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Broker risk-control incremental sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '券商风控指标增量维护报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_name, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='券商风控指标增量维护',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def financial_disclosure_incremental_sync(
        self,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages_per_market: Optional[int] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
        target_symbols: Optional[List[str]] = None,
        announcement_search_key: Optional[str] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        db_path: Optional[str] = None,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """财务公告驱动增量检查任务。"""
        task_name = 'financial_disclosure_incremental_sync'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info("[Scheduler] Starting financial disclosure incremental sync...")
            result = await data_manager.run_financial_disclosure_incremental_sync(
                exchanges=exchanges,
                lookback_days=lookback_days,
                overlap_days=overlap_days,
                page_size=page_size,
                max_pages_per_market=max_pages_per_market,
                max_candidates=max_candidates,
                pending_recheck_days=pending_recheck_days,
                target_instrument_ids=target_instrument_ids,
                target_symbols=target_symbols,
                announcement_search_key=announcement_search_key,
                report_periods=report_periods,
                period_window=period_window,
                rolling_quarters=rolling_quarters,
                baseline_report_period=baseline_report_period,
                latest_report_period=latest_report_period,
                db_path=db_path,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
                dry_run=dry_run,
            )
            result.setdefault('backtest_stages', {})[
                'financial_filing_vintages'
            ] = await _run_backtest_stage(
                'financial_filing_vintages',
                _financial_vintage_stage_report,
                str(result.get('db_path') or db_path or 'data/financials.db'),
                inherited_scope={
                    'exchanges': list(exchanges or result.get('exchanges') or []),
                    'instrument_ids': list(target_instrument_ids or []),
                    'symbols': list(target_symbols or []),
                    'report_periods': list(report_periods or result.get('report_periods') or []),
                },
                dry_run=bool(dry_run),
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '财务公告驱动增量检查报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('changed_count', 0),
                'duration': f"{result.get('elapsed_seconds', 0)}s",
                'content': _format_financial_disclosure_scheduler_report(result),
                'maintenance_tasks': [
                    {
                        'task_name': task_name,
                        'status': (
                            f"{status} "
                            f"(candidates={result.get('candidate_count', 0)}, "
                            f"changed={result.get('changed_count', 0)})"
                        ),
                    }
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务公告驱动增量检查',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Financial disclosure incremental sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '财务公告驱动增量检查报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_name, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='财务公告驱动增量检查',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def financial_disclosure_reconciliation_sync(
        self,
        exchanges: Optional[List[str]] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
        target_symbols: Optional[List[str]] = None,
        db_path: Optional[str] = None,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        dry_run: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """财务本地核心层周度有界对账任务。"""
        task_name = 'financial_disclosure_reconciliation_sync'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info("[Scheduler] Starting financial disclosure reconciliation sync...")
            result = await data_manager.run_financial_disclosure_reconciliation_sync(
                exchanges=exchanges,
                report_periods=report_periods,
                period_window=period_window,
                rolling_quarters=rolling_quarters,
                baseline_report_period=baseline_report_period,
                latest_report_period=latest_report_period,
                max_candidates=max_candidates,
                pending_recheck_days=pending_recheck_days,
                target_instrument_ids=target_instrument_ids,
                target_symbols=target_symbols,
                db_path=db_path,
                request_interval_seconds=request_interval_seconds,
                request_timeout_seconds=request_timeout_seconds,
                dry_run=dry_run,
            )
            result.setdefault('backtest_stages', {})[
                'financial_filing_vintages'
            ] = await _run_backtest_stage(
                'financial_filing_vintages',
                _financial_vintage_stage_report,
                str(result.get('db_path') or db_path or 'data/financials.db'),
                inherited_scope={
                    'exchanges': list(exchanges or result.get('exchanges') or []),
                    'instrument_ids': list(target_instrument_ids or []),
                    'symbols': list(target_symbols or []),
                    'report_periods': list(report_periods or result.get('report_periods') or []),
                },
                dry_run=bool(dry_run),
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '财务本地核心层周度对账报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('changed_count', 0),
                'duration': f"{result.get('elapsed_seconds', 0)}s",
                'content': _format_financial_disclosure_scheduler_report(result),
                'maintenance_tasks': [
                    {
                        'task_name': task_name,
                        'status': (
                            f"{status} "
                            f"(candidates={result.get('candidate_count', 0)}, "
                            f"blockers={result.get('blocking_gap_count', 0)})"
                        ),
                    }
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务本地核心层周度对账',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Financial disclosure reconciliation sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '财务本地核心层周度对账报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_name, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='财务本地核心层周度对账',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def financial_statements_catchup_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        sync_mode: str = "catchup",
        force_full: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 financial statements 日度增量 catch-up 任务。"""
        task_name = 'financial_statements_catchup_sync'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info("[Scheduler] Starting financial statements catch-up sync...")
            result = await data_manager.run_financial_statements_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                sync_mode=sync_mode,
                force_full=force_full,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '财务报表日度增量同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('rows_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务报表日度增量同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Financial statements catch-up failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '财务报表日度增量同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_name, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='财务报表日度增量同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def financial_statements_reconciliation_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        sync_mode: str = "catchup",
        force_full: bool = True,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 financial statements 周度对账与修复任务。"""
        task_name = 'financial_statements_reconciliation_sync'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info("[Scheduler] Starting financial statements reconciliation sync...")
            result = await data_manager.run_financial_statements_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
                sync_mode=sync_mode,
                force_full=force_full,
            )
            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '财务报表周度对账修复报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('rows_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)
            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='财务报表周度对账修复',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(
                f"[Scheduler] Financial statements reconciliation failed: {e}"
            )
            await self._send_task_report(
                report_data={
                    'name': '财务报表周度对账修复报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [{'task_name': task_name, 'status': str(e)}],
                },
                report_type='maintenance_report',
                task_name='财务报表周度对账修复',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def valuation_history_rebuild(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        quote_limit_days: Optional[int] = None,
        window_mode: str = "trading_days",
        write_policy: str = "missing_only",
        progress_log_every: int = 200,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 valuation history 重建任务。"""
        self._active_tasks.add('valuation_history_rebuild')
        try:
            scheduler_logger.info("[Scheduler] Starting valuation history rebuild...")

            result = await data_manager.run_valuation_history_rebuild(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                allow_disabled_module=True,
                quote_limit_days=quote_limit_days,
                window_mode=window_mode,
                write_policy=write_policy,
                progress_log_every=progress_log_every,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '估值历史重建报告',
                'status': 'success' if success else 'error',
                'content': _format_valuation_history_scheduler_report(
                    result,
                    title='估值历史重建报告',
                ),
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('rows_written', 0)}, "
                            f"existing={exchange_result.get('existing_rows_skipped', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='估值历史重建',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Valuation history rebuild failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '估值历史重建报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'valuation_history_rebuild', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='估值历史重建',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('valuation_history_rebuild')

    async def valuation_history_weekly_reconcile(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        quote_limit_days: Optional[int] = 60,
        write_policy: str = "missing_only",
        progress_log_every: int = 200,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 valuation history 周度回补校验任务。"""
        return await self.valuation_history_rebuild(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            quote_limit_days=quote_limit_days,
            window_mode="trading_days",
            write_policy=write_policy,
            progress_log_every=progress_log_every,
            job_config=job_config,
        )

    async def valuation_history_12q_rebuild(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        quote_limit_days: Optional[int] = None,
        window_mode: str = "last_12_quarters",
        write_policy: str = "missing_only",
        progress_log_every: int = 200,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 valuation history 过去 12 个季度窗口重建任务。"""
        return await self.valuation_history_rebuild(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            quote_limit_days=quote_limit_days,
            window_mode="last_12_quarters",
            write_policy=write_policy,
            progress_log_every=progress_log_every,
            job_config=job_config,
        )

    async def valuation_history_full_rebuild(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        quote_limit_days: Optional[int] = None,
        window_mode: str = "last_12_quarters",
        write_policy: str = "missing_only",
        progress_log_every: int = 200,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """Backward-compatible alias for the 12-quarter valuation rebuild."""
        return await self.valuation_history_12q_rebuild(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            quote_limit_days=quote_limit_days,
            window_mode=window_mode,
            write_policy=write_policy,
            progress_log_every=progress_log_every,
            job_config=job_config,
        )

    async def valuation_input_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        sync_mode: str = "incremental",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 valuation inputs 同步任务。"""
        task_name = 'valuation_input_sync'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info(
                "[Scheduler] Starting valuation input sync: exchanges=%s "
                "limit_per_exchange=%s source=%s source_mode=%s sync_mode=%s "
                "start_date=%s end_date=%s",
                exchanges,
                limit_per_exchange,
                source,
                source_mode,
                sync_mode,
                start_date,
                end_date,
            )

            result = await data_manager.run_valuation_input_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                source=source,
                source_mode=source_mode,
                sync_mode=sync_mode,
                start_date=start_date,
                end_date=end_date,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            duration = _format_seconds_for_report(result.get('elapsed_seconds'))
            report_data = {
                'name': '估值输入同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': duration,
                'total_requested_instruments': result.get('total_requested_instruments', 0),
                'total_covered_instruments': result.get('total_covered_instruments', 0),
                'total_missing_instruments': result.get('total_missing_instruments', 0),
                'total_snapshots_written': result.get('total_snapshots_written', 0),
                'content': _format_valuation_input_scheduler_report(
                    result,
                    title='估值输入同步报告',
                ),
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('snapshots_written', 0)}, "
                            f"missing={exchange_result.get('missing_instruments', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='估值输入同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Valuation input sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '估值输入同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': task_name, 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='估值输入同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def valuation_input_full_backfill(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 valuation inputs 全量历史回填任务。"""
        task_name = 'valuation_input_full_backfill'
        self._active_tasks.add(task_name)
        try:
            scheduler_logger.info(
                "[Scheduler] Starting valuation input full backfill: exchanges=%s "
                "limit_per_exchange=%s source=%s source_mode=%s start_date=%s end_date=%s",
                exchanges,
                limit_per_exchange,
                source,
                source_mode,
                start_date,
                end_date,
            )

            result = await data_manager.run_valuation_input_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                source=source,
                source_mode=source_mode,
                sync_mode="full",
                start_date=start_date,
                end_date=end_date,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            duration = _format_seconds_for_report(result.get('elapsed_seconds'))
            report_data = {
                'name': '估值输入全量回填报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': duration,
                'total_requested_instruments': result.get('total_requested_instruments', 0),
                'total_covered_instruments': result.get('total_covered_instruments', 0),
                'total_missing_instruments': result.get('total_missing_instruments', 0),
                'total_snapshots_written': result.get('total_snapshots_written', 0),
                'content': _format_valuation_input_scheduler_report(
                    result,
                    title='估值输入全量回填报告',
                ),
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('snapshots_written', 0)}, "
                            f"missing={exchange_result.get('missing_instruments', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='估值输入全量回填',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(
                f"[Scheduler] Valuation input full backfill failed: {e}"
            )
            await self._send_task_report(
                report_data={
                    'name': '估值输入全量回填报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': task_name, 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='估值输入全量回填',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard(task_name)

    async def technical_snapshot_refresh(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        adjustment: Optional[str] = None,
        period: Optional[str] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 technical latest snapshot 刷新任务。"""
        self._active_tasks.add('technical_snapshot_refresh')
        try:
            scheduler_logger.info("[Scheduler] Starting technical snapshot refresh...")

            result = await data_manager.run_technical_snapshot_refresh(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                adjustment=adjustment,
                period=period,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '技术指标最新快照刷新报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('rows_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='技术指标最新快照刷新',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Technical snapshot refresh failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '技术指标最新快照刷新报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'technical_snapshot_refresh', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='技术指标最新快照刷新',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('technical_snapshot_refresh')

    async def analyst_forecast_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 analyst forecast 影子同步任务。"""
        self._active_tasks.add('analyst_forecast_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting analyst forecast shadow sync...")

            result = await data_manager.run_analyst_forecast_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '分析师预期影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('forecasts_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='分析师预期影子同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Analyst forecast shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '分析师预期影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'analyst_forecast_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='分析师预期影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('analyst_forecast_sync')

    async def research_report_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 research report 影子同步任务。"""
        self._active_tasks.add('research_report_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting research report shadow sync...")

            result = await data_manager.run_research_report_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '研报元数据影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('reports_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='研报元数据影子同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Research report shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '研报元数据影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'research_report_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='研报元数据影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('research_report_sync')

    async def sentiment_event_sync(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 sentiment event 影子同步任务。"""
        self._active_tasks.add('sentiment_event_sync')
        try:
            scheduler_logger.info("[Scheduler] Starting sentiment event shadow sync...")

            result = await data_manager.run_sentiment_event_shadow_sync(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
                budget_mode=budget_mode,
                allow_paid_proxy=allow_paid_proxy,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '事件舆情影子同步报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('events_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='事件舆情影子同步',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Sentiment event shadow sync failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '事件舆情影子同步报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'sentiment_event_sync', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='事件舆情影子同步',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('sentiment_event_sync')

    async def risk_snapshot_rebuild(
        self,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """研究域 risk snapshot 重建任务。"""
        self._active_tasks.add('risk_snapshot_rebuild')
        try:
            scheduler_logger.info("[Scheduler] Starting risk snapshot rebuild...")

            result = await data_manager.run_risk_snapshot_rebuild(
                exchanges=exchanges,
                limit_per_exchange=limit_per_exchange,
            )

            status = result.get('status', 'failed')
            success = status in {'success', 'degraded'}
            report_data = {
                'name': '风险快照重建报告',
                'status': 'success' if success else 'error',
                'tasks_completed': result.get('successful_exchanges', 0),
                'duration': 'N/A',
                'maintenance_tasks': [
                    {
                        'task_name': exchange_result.get('exchange', 'unknown'),
                        'status': (
                            f"{exchange_result.get('status')} "
                            f"(rows={exchange_result.get('rows_written', 0)})"
                        ),
                    }
                    for exchange_result in result.get('exchanges', [])
                ],
            }
            _attach_instrument_master_governance_report(report_data, result)

            await self._send_task_report(
                report_data=report_data,
                report_type='maintenance_report',
                task_name='风险快照重建',
                job_config=job_config,
            )
            return success
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Risk snapshot rebuild failed: {e}")
            await self._send_task_report(
                report_data={
                    'name': '风险快照重建报告',
                    'status': 'error',
                    'tasks_completed': 0,
                    'duration': 'N/A',
                    'maintenance_tasks': [
                        {'task_name': 'risk_snapshot_rebuild', 'status': str(e)}
                    ],
                },
                report_type='maintenance_report',
                task_name='风险快照重建',
                job_config=job_config,
            )
            return False
        finally:
            self._active_tasks.discard('risk_snapshot_rebuild')

    async def system_health_check(self,
                                check_data_sources: bool = True,
                                check_database: bool = True,
                                check_disk_space: bool = True,
                                check_memory_usage: bool = True,
                                check_telegram: bool = True,
                                disk_space_threshold_mb: int = 1000,
                                memory_threshold_percent: int = 85,
                                health_check_timeout_sec: int = 30,
                                job_config: Optional[JobConfig] = None) -> bool:
        """系统健康检查任务"""
        try:
            # ★ 运行管控：当有其他任务正在运行时，跳过本次健康检查
            # 检查 _active_tasks（覆盖调度器调用 + Telegram /run 直接调用）
            other_tasks = {t for t in self._active_tasks if t != 'system_health_check'}
            if other_tasks:
                task_names = ', '.join(other_tasks)
                skip_msg = f"当前 {task_names} 任务正在运行，健康检查延迟进行"
                scheduler_logger.info(f"[Scheduler] {skip_msg}")
                if self.telegram_enabled and self.bot:
                    try:
                        await self.bot.send_scheduler_notification(skip_msg, level='info')
                    except Exception as notify_err:
                        scheduler_logger.warning(f"[Scheduler] 发送跳过通知失败: {notify_err}")
                return True  # 返回 True 表示非异常跳过

            scheduler_logger.info("[Scheduler] Starting system health check...")
            start_time = datetime.now()
            try:
                status = await asyncio.wait_for(
                    data_manager.get_system_status(),
                    timeout=health_check_timeout_sec
                )
            except asyncio.TimeoutError:
                error_msg = f"System status check timed out after {health_check_timeout_sec}s"
                scheduler_logger.error(f"[Scheduler] {error_msg}")
                failure_report_data = {
                    'name': '系统健康检查报告',
                    'status': 'error',
                    'error_message': error_msg
                }
                await self._send_task_report(
                    report_data=failure_report_data,
                    report_type='health_check_report',
                    task_name='系统健康检查',
                    job_config=job_config
                )
                return False

            # 检查数据源健康状态
            if check_data_sources:
                unhealthy_sources = [
                    source for source, is_healthy in status.get('data_sources', {}).items()
                    if not is_healthy
                ]
            else:
                unhealthy_sources = []
            auto_repair_results = {}
            if check_data_sources and unhealthy_sources:
                scheduler_logger.warning(f"[Scheduler] 侦测到异常数据源 {unhealthy_sources}, 尝试自动修复")
                new_unhealthy_sources = []
                for src_name in unhealthy_sources:
                    repair_res = "失败"
                    try:
                        source = data_manager.source_factory.sources.get(src_name)
                        if source:
                            if hasattr(source, '_relogin'):
                                await source._relogin()
                            elif hasattr(source, '_reconnect'):
                                await source._reconnect()
                            is_healthy = await source.health_check()
                            if is_healthy:
                                repair_res = "成功"
                            else:
                                repair_res = "失败（健康检查未通过）"
                                new_unhealthy_sources.append(src_name)
                        else:
                            repair_res = "失败（数据源未初始化）"
                            new_unhealthy_sources.append(src_name)
                    except Exception as e:
                        repair_res = f"失败（{e}）"
                        new_unhealthy_sources.append(src_name)
                    
                    auto_repair_results[src_name] = repair_res
                    scheduler_logger.info(f"[Scheduler] {src_name} 自动修复结果: {repair_res}")
                
                unhealthy_sources = new_unhealthy_sources

                if self.telegram_enabled and self.bot:
                    msg = "数据源自动修复结果:\n" + "\n".join(f"- {k}: {v}" for k, v in auto_repair_results.items())
                    level = "warning" if len(unhealthy_sources) > 0 else "success"
                    await self.bot.send_scheduler_notification(msg, level=level)

            # 检查数据库连接
            database_unhealthy = False
            if check_database:
                if not status.get('database'):
                    error_msg = "错误: 数据库连接异常"
                    scheduler_logger.error(error_msg)
                    database_unhealthy = True

            # 检查磁盘空间
            if check_disk_space:
                await self._check_disk_space(disk_space_threshold_mb)

            # 检查内存使用
            if check_memory_usage:
                await self._check_memory_usage(memory_threshold_percent)

            # 检查Telegram连接状态
            if check_telegram:
                telegram_result = await self._check_telegram_connection()
            else:
                telegram_result = "⏭️ Telegram检查已跳过"

            # 生成健康检查报告数据
            check_results = []
            # 数据源检查
            if check_data_sources: # 检查数据源
                is_ds_healthy = not unhealthy_sources
                check_results.append({
                    "check_name": "数据源连接",
                    "result": "正常" if is_ds_healthy else f"异常: {', '.join(unhealthy_sources)}",
                    "status_icon": "✅" if is_ds_healthy else "❌"
                })
                if auto_repair_results:
                    repair_ok = all(v == "成功" for v in auto_repair_results.values())
                    repair_details = ", ".join(f"{k}: {v}" for k, v in auto_repair_results.items())
                    check_results.append({
                        "check_name": "数据源自动修复",
                        "result": repair_details,
                        "status_icon": "✅" if repair_ok else "❌"
                    })
            # 数据库检查
            if check_database:
                is_db_healthy = not database_unhealthy
                check_results.append({
                    "check_name": "数据库状态",
                    "result": "正常" if is_db_healthy else "连接异常",
                    "status_icon": "✅" if is_db_healthy else "❌"
                })
            # 磁盘空间检查
            if check_disk_space:
                # _check_disk_space 内部会记录警告，这里假设它成功则为充足
                check_results.append({"check_name": "磁盘空间", "result": "充足", "status_icon": "✅"}) # TODO: 实际应根据_check_disk_space的返回值判断
            # 内存使用检查
            if check_memory_usage:
                # _check_memory_usage 内部会记录警告，这里假设它成功则为正常
                check_results.append({"check_name": "内存使用", "result": "正常", "status_icon": "✅"}) # TODO: 实际应根据_check_memory_usage的返回值判断
            # Telegram连接检查
            if check_telegram:
                is_tg_healthy = "✅" in telegram_result
                check_results.append({
                    "check_name": "Telegram连接",
                    "result": telegram_result.replace("✅ ", "").replace("❌ ", ""),
                    "status_icon": "✅" if is_tg_healthy else "❌"
                })

            # 计算健康状态
            healthy_issues = [
                r for r in check_results 
                if r.get('status_icon') == '❌' or '异常' in r.get('result', '') or '错误' in r.get('result', '')
            ]
            is_healthy = len(healthy_issues) == 0

            report_data = {
                'name': '系统健康检查报告',
                'status': 'success' if is_healthy else 'warning',  # 明确的成功/失败状态
                'overall_status': 'HEALTHY' if is_healthy else 'WARNING',
                'checks_performed': len(check_results),
                'issues_found': len(healthy_issues),
                'check_results': check_results,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'duration': f"{(datetime.now() - start_time).total_seconds():.1f}s"
            }
            scheduler_logger.debug(f"[Scheduler] Generated health check report: {report_data}")

            # 发送报告
            await self._send_task_report(
                report_data=report_data,
                report_type='health_check_report',
                task_name='系统健康检查',
                job_config=job_config
            )

            scheduler_logger.info("[Scheduler] Health check completed")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Health check failed: {e}")
            # 统一使用报告系统发送失败通知
            failure_report_data = {
                'name': '系统健康检查报告',
                'status': 'error',
                'error_message': str(e)
            }
            await self._send_task_report(
                report_data=failure_report_data,
                report_type='health_check_report',
                task_name='系统健康检查',
                job_config=job_config
            )
            return False

    async def market_dependency_version_check(
        self,
        packages: Optional[List[Dict[str, str]]] = None,
        timeout_sec: float = 10.0,
        notify_when_latest: bool = False,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """检查行情数据相关 Python 包是否有可升级版本，并通过 Telegram 通知。"""
        self._active_tasks.add('market_dependency_version_check')
        try:
            from utils.market_dependency_versions import (
                check_market_dependency_versions,
                format_market_dependency_version_message,
            )

            scheduler_logger.info("[Scheduler] Starting market dependency version check...")
            result = await asyncio.to_thread(
                check_market_dependency_versions,
                packages,
                timeout_sec=timeout_sec,
            )

            updates = result.get('updates') or []
            errors = result.get('errors') or []
            scheduler_logger.info(
                "[Scheduler] Market dependency version check completed: updates=%d, errors=%d",
                len(updates),
                len(errors),
            )
            for item in result.get('statuses') or []:
                scheduler_logger.info(
                    "[Scheduler] dependency_version %s installed=%s latest=%s update=%s error=%s",
                    item.get('name'),
                    item.get('installed_version'),
                    item.get('latest_version'),
                    item.get('update_available'),
                    item.get('error'),
                )

            if (updates or errors or notify_when_latest) and self.telegram_enabled and self.bot:
                message = format_market_dependency_version_message(result)
                level = 'warning' if updates or errors else 'success'
                await self.bot.send_task_notification(
                    message,
                    task_name='market_dependency_version_check',
                    level=level,
                )

            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Market dependency version check failed: {e}")
            if self.telegram_enabled and self.bot:
                try:
                    await self.bot.send_task_notification(
                        f"行情依赖版本检查失败: {e}",
                        task_name='market_dependency_version_check',
                        level='error',
                    )
                except Exception as notify_err:
                    scheduler_logger.warning(
                        f"[Scheduler] 发送行情依赖版本检查失败通知失败: {notify_err}"
                    )
            return False
        finally:
            self._active_tasks.discard('market_dependency_version_check')

    async def cache_warm_up(self,
                           warm_popular_stocks: bool = True,
                           popular_stocks_count: int = 50,
                           warm_market_indices: bool = True,
                           preload_recent_data: bool = True,
                           recent_data_days: int = 7,
                           job_config: Optional[JobConfig] = None) -> bool:
        """缓存预热任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting cache warm up...")

            if not cache_manager.enabled:
                scheduler_logger.info("[Scheduler] Cache disabled, skipping warm up")
                return False

            # 预热热门股票缓存
            if warm_popular_stocks:
                popular_instruments = await data_manager.db_ops.get_instruments_list(
                    limit=popular_stocks_count, is_active=True
                )

                warmed_count = 0
                for instrument in popular_instruments[:popular_stocks_count]:
                    instrument_id = instrument['instrument_id']

                    # 预加载数据
                    if preload_recent_data:
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=recent_data_days)

                        data = await data_manager.get_quotes(
                            instrument_id=instrument_id,
                            start_date=start_date,
                            end_date=end_date,
                            return_format='pandas'
                        )

                        if not data.empty:
                            warmed_count += 1
                            scheduler_logger.debug(f"[Scheduler] Warmed up cache for {instrument_id}")

                scheduler_logger.info(f"[Scheduler] Warmed up cache for {warmed_count} popular stocks")

            # 预热市场指数
            if warm_market_indices:
                await self._warm_up_market_indices(recent_data_days)

            # 生成缓存预热报告数据
            report_data = {
                'name': '缓存预热报告',
                'status': 'success',  # 缓存预热通常成功，除非有异常
                'stocks_warmed': warmed_count,
                'cache_hit_rate': 'N/A',
                'duration': 'N/A',
                'popular_stocks': f"预热了{warmed_count}支热门股票",
                'market_indices': '完成市场指数预热' if warm_market_indices else '跳过市场指数预热',
                'recent_data': f"预加载了{recent_data_days}天的历史数据",
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # 发送报告
            await self._send_task_report(
                report_data=report_data,
                report_type='cache_warm_up_report',
                task_name='缓存预热',
                job_config=job_config
            )

            scheduler_logger.info("[Scheduler] Cache warm up completed")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Cache warm up failed: {e}")
            return False

    async def trading_calendar_update(self,
                                    exchanges: Optional[List[str]] = None,
                                    update_future_months: int = 6,
                                    force_update: bool = False,
                                    validate_holidays: bool = True,
                                    job_config: Optional[JobConfig] = None) -> bool:
        """交易日历更新任务"""
        try:
            scheduler_logger.info("[Scheduler] Updating trading calendars...")

            # 使用配置参数或默认值
            if exchanges is None:
                exchanges = self.config.get_nested(
                    'data_config.market_presets.a_shares',
                    default=['SSE', 'SZSE', 'BSE']
                )

            current_year = datetime.now().year
            future_year = current_year + 1 if update_future_months >= 12 else current_year

            updated_exchanges = []

            for exchange in exchanges:
                try:
                    # 根据配置的update_future_months参数更新交易日历
                    from datetime import timedelta
                    today = date.today()
                    start_date = today
                    end_date = today + timedelta(days=update_future_months * 30)  # 粗略估算，每个月30天

                    scheduler_logger.info(f"[Scheduler] Updating {exchange} trading calendar from {start_date} to {end_date}")

                    # 缓存交易日历（使用DateUtils获取）
                    current_year = today.year
                    trading_days = DateUtils.get_trading_days_in_range(exchange, start_date, end_date)

                    await cache_manager.quote_cache.set_trading_calendar(
                        exchange, current_year, trading_days, ttl=86400 * 30  # 30天缓存
                    )

                    # 同时更新数据库
                    try:
                        updated_count = await data_manager._update_trading_calendar(exchange, start_date, end_date)
                        scheduler_logger.info(f"[Scheduler] Database calendar updated for {exchange}: {updated_count} days")
                    except Exception as db_e:
                        scheduler_logger.warning(f"[Scheduler] Failed to update database calendar for {exchange}: {db_e}")

                    # 验证节假日
                    if validate_holidays:
                        await self._validate_holidays(exchange, current_year)

                    updated_exchanges.append(exchange)

                except Exception as e:
                    scheduler_logger.warning(f"[Scheduler] Failed to update calendar for {exchange}: {e}")

            scheduler_logger.info(f"[Scheduler] Trading calendars updated for: {', '.join(updated_exchanges)}")
            
            # 生成交易日历更新报告数据
            report_data = {
                'name': '交易日历更新报告',
                'status': 'success' if len(updated_exchanges) > 0 else 'warning',
                'exchanges_updated': len(updated_exchanges),
                'trading_days_added': 'N/A', # 可以在_update_trading_calendar中返回
                'holidays_added': 'N/A', # 可以在_validate_holidays中返回
                'exchange_details': {
                    ex: {'status': '成功'} for ex in updated_exchanges
                },
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            # 发送报告
            await self._send_task_report(
                report_data=report_data,
                report_type='trading_calendar_report',
                task_name='交易日历更新',
                job_config=job_config
            )
            return len(updated_exchanges) > 0
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Trading calendar update failed: {e}")
            return False

    async def _wait_for_markets_close(self, exchanges: List[str], delay_minutes: int = 15):
        """等待交易所收盘（支持配置化延迟时间）"""
        scheduler_logger.info(f"[Scheduler] Waiting for markets to close: {exchanges}")

        # 不同交易所的收盘时间（北京时间）
        # 注意: 美股收盘时间随夏令时变化 (EDT=04:00 CST, EST=05:00 CST)
        market_close_times = {
            'SSE': time(15, 0),    # A股 15:00收盘
            'SZSE': time(15, 0),   # 深圳同样15:00
            'HKEX': time(16, 0),   # 港股 16:00收盘
        }

        # 动态计算美股收盘的北京时间（自动处理夏令时）
        if any(ex in ('NASDAQ', 'NYSE') for ex in exchanges):
            try:
                from zoneinfo import ZoneInfo
                us_et = datetime.now(ZoneInfo("America/New_York"))
                close_et = us_et.replace(hour=16, minute=0, second=0, microsecond=0)
                close_cst = close_et.astimezone(ZoneInfo("Asia/Shanghai"))
                us_close_time = close_cst.time()
                scheduler_logger.debug(f"[Scheduler] US market close in CST: {us_close_time}")
            except ImportError:
                # zoneinfo 不可用时的降级处理
                us_close_time = time(5, 0)  # 保守使用非夏令时 05:00 CST
                scheduler_logger.warning("[Scheduler] zoneinfo unavailable, using default US close time 05:00 CST")
            market_close_times['NASDAQ'] = us_close_time
            market_close_times['NYSE'] = us_close_time

        # 找出最晚的收盘时间
        latest_close = max(market_close_times[ex] for ex in exchanges if ex in market_close_times)

        # 等待到收盘时间后指定分钟
        now = datetime.now()
        close_time = datetime.combine(now.date(), latest_close)
        update_time = close_time + timedelta(minutes=delay_minutes)

        if now < update_time:
            wait_seconds = (update_time - now).total_seconds()
            scheduler_logger.info(f"[Scheduler] Waiting {wait_seconds/60:.1f} minutes until market close + {delay_minutes}min delay")
            await asyncio.sleep(wait_seconds)

    async def _cleanup_old_logs(self, retention_days: int):
        """清理旧日志文件"""
        try:
            import os
            import glob
            from datetime import datetime, timedelta

            log_dir = "log"
            if not os.path.exists(log_dir):
                return

            cutoff_date = datetime.now() - timedelta(days=retention_days)
            cleaned_files = 0

            # 清理日志文件
            for log_file in glob.glob(os.path.join(log_dir, "*.log*")):
                try:
                    file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
                    if file_time < cutoff_date:
                        os.remove(log_file)
                        cleaned_files += 1
                except Exception as e:
                    scheduler_logger.warning(f"[Scheduler] Failed to remove log file {log_file}: {e}")

            if cleaned_files > 0:
                scheduler_logger.info(f"[Scheduler] Cleaned up {cleaned_files} old log files (retention: {retention_days} days)")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to cleanup old logs: {e}")

    async def _optimize_database(self):
        """优化数据库"""
        try:
            scheduler_logger.info("[Scheduler] Optimizing database...")

            # 执行数据库优化操作
            vacuum_success = await data_manager.db_ops.execute_query("VACUUM")
            analyze_success = await data_manager.db_ops.execute_query("ANALYZE")

            if vacuum_success and analyze_success:
                scheduler_logger.info("[Scheduler] Database optimization completed successfully")
            else:
                scheduler_logger.warning("[Scheduler] Database optimization partially failed")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Database optimization failed: {e}")

    async def _validate_data_integrity(self):
        """验证数据完整性"""
        try:
            scheduler_logger.info("[Scheduler] Validating data integrity...")

            # 检查数据库中的数据一致性
            validation_results = await data_manager.db_ops.validate_data_integrity()

            if validation_results.get('total_issues', 0) > 0:
                issues_count = validation_results['total_issues']
                scheduler_logger.warning(f"[Scheduler] Found {issues_count} data integrity issues")

                # 记录具体问题类型
                for issue in validation_results.get('issues', []):
                    scheduler_logger.warning(f"[Scheduler] Issue: {issue.get('description', 'Unknown')}")

                # 记录警告
                for warning in validation_results.get('warnings', []):
                    scheduler_logger.info(f"[Scheduler] Warning: {warning.get('description', 'Unknown')}")
            else:
                scheduler_logger.info("[Scheduler] Data integrity validation passed")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Data integrity validation failed: {e}")

    async def _check_disk_space(self, threshold_mb: int):
        """检查磁盘空间"""
        try:
            import shutil

            _, _, free = shutil.disk_usage(".")
            free_mb = free // (1024 * 1024)

            if free_mb < threshold_mb:
                warning_msg = f"磁盘空间不足: 剩余 {free_mb}MB, 阈值 {threshold_mb}MB"
                scheduler_logger.warning(warning_msg)
                if self.telegram_enabled:
                    try:
                        await self.bot.send_task_notification(warning_msg, "system_health_check", "warning")
                    except Exception as notify_error:
                        scheduler_logger.error(f"[Scheduler] Failed to send notification: {notify_error}")
            else:
                scheduler_logger.debug(f"[Scheduler] Disk space OK: {free_mb}MB available")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to check disk space: {e}")

    async def _check_memory_usage(self, threshold_percent: int):
        """检查内存使用情况"""
        try:
            import psutil

            memory = psutil.virtual_memory()
            used_percent = memory.percent

            if used_percent > threshold_percent:
                warning_msg = f"内存使用率过高: {used_percent:.1f}%, 阈值 {threshold_percent}%"
                scheduler_logger.warning(warning_msg)
                if self.telegram_enabled:
                    try:
                        await self.bot.send_task_notification(warning_msg, "system_health_check", "warning")
                    except Exception as notify_error:
                        scheduler_logger.error(f"[Scheduler] Failed to send notification: {notify_error}")
            else:
                scheduler_logger.debug(f"[Scheduler] Memory usage OK: {used_percent:.1f}%")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to check memory usage: {e}")

    async def _check_telegram_connection(self):
        """检查Telegram连接状态并尝试修复"""
        try:
            scheduler_logger.info("[Scheduler] Checking Telegram connection status...")
            bot = self.bot
            timeout_seconds = self.config.get_nested(
                'telegram_config.health_check_timeout_sec', 10
            )
            # 检查连接健康状态
            is_healthy = await asyncio.wait_for(
                bot.check_connection_health(),
                timeout=timeout_seconds
            )

            if is_healthy:
                scheduler_logger.info("[Scheduler] Telegram connection is healthy")
                return "✅ Telegram连接正常"

            scheduler_logger.warning("[Scheduler] Telegram connection is unhealthy, attempting to fix...")

            # 尝试修复连接
            repair_success = await asyncio.wait_for(
                bot.ensure_connection(),
                timeout=timeout_seconds
            )

            if repair_success:
                success_msg = "✅ Telegram连接修复成功"
                scheduler_logger.info(f"[Scheduler] {success_msg}")

                # 发送修复成功通知
                if self.telegram_enabled:
                    try:
                        await self.bot.send_scheduler_notification(success_msg, "success")
                    except Exception as notify_error:
                        scheduler_logger.error(f"[Scheduler] Failed to send Telegram repair success notification: {notify_error}")

                return "✅ Telegram连接修复成功"
            else:
                error_msg = "❌ Telegram连接修复失败，需要人工干预"
                scheduler_logger.error(f"[Scheduler] {error_msg}")

                # 发送修复失败通知
                if self.telegram_enabled:
                    try:
                        # 如果连接修复失败，这个通知可能也发送失败，但我们还是尝试
                        await self.bot.send_scheduler_notification(error_msg, "error")
                    except Exception as notify_error:
                        scheduler_logger.error(f"[Scheduler] Failed to send Telegram repair failure notification: {notify_error}")

                return "❌ Telegram连接修复失败"

        except asyncio.TimeoutError:
            error_msg = "❌ Telegram连接检查超时"
            scheduler_logger.error(f"[Scheduler] {error_msg}")
            if self.telegram_enabled:
                try:
                    await self.bot.send_scheduler_notification(error_msg, "error")
                except Exception as notify_error:
                    scheduler_logger.error(
                        f"[Scheduler] Failed to send Telegram timeout notification: {notify_error}"
                    )
            return error_msg
        except Exception as e:
            error_msg = f"❌ Telegram连接检查异常: {str(e)}"
            scheduler_logger.error(f"[Scheduler] {error_msg}")

            # 发送异常通知
            if self.telegram_enabled:
                try:
                    await self.bot.send_scheduler_notification(error_msg, "error")
                except Exception as notify_error:
                    scheduler_logger.error(f"[Scheduler] Failed to send Telegram check exception notification: {notify_error}")

            return error_msg


    async def _warm_up_market_indices(self, recent_data_days: int):
        """预热市场指数缓存"""
        try:
            # 获取主要市场指数
            market_indices = [
                '000001.SH',  # 上证指数
                '399001.SZ',  # 深证成指
                '399006.SZ',  # 创业板指
            ]

            end_date = datetime.now()
            start_date = end_date - timedelta(days=recent_data_days)

            warmed_count = 0
            for index_id in market_indices:
                try:
                    data = await data_manager.get_quotes(
                        instrument_id=index_id,
                        start_date=start_date,
                        end_date=end_date,
                        return_format='pandas'
                    )

                    if not data.empty:
                        warmed_count += 1
                        scheduler_logger.debug(f"[Scheduler] Warmed up cache for index {index_id}")

                except Exception as e:
                    scheduler_logger.debug(f"[Scheduler] Failed to warm up index {index_id}: {e}")

            if warmed_count > 0:
                scheduler_logger.info(f"[Scheduler] Warmed up cache for {warmed_count} market indices")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to warm up market indices: {e}")

    async def _validate_holidays(self, exchange: str, year: int):
        """验证节假日"""
        try:
            # 这里可以添加节假日验证逻辑
            # 比如检查交易日历是否包含合理的假期日期
            scheduler_logger.debug(f"[Scheduler] Validated holidays for {exchange} {year}")
        except Exception as e:
            scheduler_logger.warning(f"[Scheduler] Failed to validate holidays for {exchange} {year}: {e}")

    
    async def database_backup(self,
                            use_backup_config: bool = True,
                            source_db_path: Optional[str] = None,
                            backup_directory: Optional[str] = None,
                            retention_days: Optional[int] = None,
                            notification_enabled: Optional[bool] = None,
                            filename_pattern: Optional[str] = None,
                            max_backup_files: Optional[int] = None,
                            job_config: Optional[JobConfig] = None) -> bool:
        """数据库备份任务"""
        scheduler_logger.info("[Scheduler] Starting unified database backup task...")
        if not use_backup_config:
            scheduler_logger.warning(
                "[Scheduler] use_backup_config=false is deprecated; unified database_backup_config is authoritative"
            )
        if any(
            value is not None
            for value in (
                source_db_path,
                backup_directory,
                retention_days,
                notification_enabled,
                filename_pattern,
                max_backup_files,
            )
        ):
            scheduler_logger.warning(
                "[Scheduler] legacy database_backup runtime override parameters are deprecated; "
                "configure database_backup_config instead"
            )

        service = DatabaseBackupService.from_config_manager(self.config, scheduler_logger)

        async def notify_database_result(result: DatabaseBackupResult) -> None:
            if not service.config.notification_enabled or not service.config.per_database_notification:
                return
            if not self.telegram_enabled or not self.bot:
                return
            level = "info" if result.status in {BACKUP_STATUS_SUCCESS, BACKUP_STATUS_SKIPPED} else "error"
            try:
                await self.bot.send_task_notification(
                    self._format_database_backup_notification(result),
                    task_name="database_backup",
                    level=level,
                )
            except Exception as notify_err:
                scheduler_logger.warning(
                    "[Scheduler] Failed to send database backup per-db notification: %s",
                    notify_err,
                )

        run_result: DatabaseBackupRunResult = await service.run(
            on_database_result=notify_database_result
        )
        report_data = run_result.to_report_data()
        if run_result.preflight_error:
            report_data["error_message"] = run_result.preflight_error
        elif run_result.failure_count:
            report_data["error_message"] = "; ".join(
                result.error or result.skipped_reason or result.name
                for result in run_result.results
                if result.status == BACKUP_STATUS_FAILED
            )

        await self._send_task_report(report_data, 'backup_result', '数据库备份', job_config)
        if run_result.success:
            scheduler_logger.info(
                "[Scheduler] Unified database backup completed successfully: success=%s skipped=%s deleted=%s",
                run_result.success_count,
                run_result.skipped_count,
                run_result.cleanup_deleted_count,
            )
        else:
            scheduler_logger.error(
                "[Scheduler] Unified database backup failed: success=%s failed=%s skipped=%s error=%s",
                run_result.success_count,
                run_result.failure_count,
                run_result.skipped_count,
                run_result.preflight_error,
            )
        return run_result.success

    def _format_database_backup_notification(self, result: DatabaseBackupResult) -> str:
        """Format one database backup result for Telegram task notification."""
        if result.status == BACKUP_STATUS_SUCCESS:
            return (
                "数据库备份完成\n"
                f"数据库: `{result.name}`\n"
                f"源: `{result.source}`\n"
                f"备份: `{result.backup_path}`\n"
                f"大小: `{result.backup_size}` bytes\n"
                f"耗时: `{result.duration:.1f}s`\n"
                f"校验: `{result.validation_status}`\n"
                f"清理: 删除 `{len(result.cleanup.deleted_files)}` 个旧备份"
            )
        if result.status == BACKUP_STATUS_SKIPPED:
            return (
                "数据库备份跳过\n"
                f"数据库: `{result.name}`\n"
                f"源: `{result.source}`\n"
                f"原因: `{result.skipped_reason}`"
            )
        return (
            "数据库备份失败\n"
            f"数据库: `{result.name}`\n"
            f"源: `{result.source}`\n"
            f"错误: `{result.error}`\n"
            f"继续后续: `{result.continued_after_failure}`"
        )

# 全局定时任务实例
scheduled_tasks = ScheduledTasks()
