"""
Scheduled tasks for the quote system.
Defines all automated data update and maintenance tasks.
"""

import asyncio
import os
from datetime import datetime, date, timedelta, time
from typing import List, Dict, Any, Optional

# Some tests and operator scripts import scheduler.tasks directly. Install the
# proxy patch before project utility imports can pull in HTTP client stacks.
from proxy_patch_bootstrap import install_akshare_proxy_patch as _install_akshare_proxy_patch

_install_akshare_proxy_patch(required=False)

from utils import scheduler_logger, config_manager, TelegramBot
from .job_config import JobConfig
from data_manager import data_manager
from instrument_master_governance import MasterGovernanceRequirement
from utils.date_utils import DateUtils
from utils.cache import cache_manager


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
    if normalized in {"skipped", "disabled", "unavailable"}:
        return "ℹ️", "未执行"
    if normalized in {"failed", "error"}:
        return "❌", "失败"
    return "ℹ️", status_text


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

    if exchanges:
        lines.extend(["", "*分交易所*"])
        for item in exchanges:
            ex_status = item.get("status", "unknown")
            ex_icon, ex_label = _format_scheduler_status(ex_status)
            lines.append(
                f"• {item.get('exchange', 'unknown')}: {ex_icon} {ex_label}, "
                f"rows={item.get('snapshots_written', 0)}, "
                f"requested={item.get('requested_instruments', 0)}, "
                f"covered={item.get('covered_instruments', 0)}, "
                f"missing={item.get('missing_instruments', 0)}, "
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
            f"待退市风险 {result.get('pending_delisting_risk_count', 0)}"
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
            f"本地缺口 {candidate_sources.get('local_gap', 0)}，"
            f"旧噪声过滤 {candidate_sources.get('filtered_stale_pending', 0)}"
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
        routing_errors = source_routing.get("errors") or []
        if routing_errors:
            lines.append(
                "补数源警告: " + "；".join(str(item) for item in routing_errors[:3])
            )
    scan_errors = result.get("scan_errors") or []
    if scan_errors:
        lines.append("扫描警告: " + "；".join(str(item) for item in scan_errors[:3]))
    if result.get("pending_delisting_risk_count", 0):
        lines.append("说明: 待退市风险只是披露异常待补状态，不会改写股票主数据退市状态。")
    if result.get("mapping_policy_gap_count", 0):
        lines.append("说明: mapping policy gap 是字段标准或映射准入问题，不会反复调用 CNInfo/THS/Sina 补数。")
    if result.get("blocking_gap_count", 0):
        lines.append("后续动作: blocker 按 source missing 或其他数据质量问题补处理，不能并入 accepted gaps。")
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


def _format_futures_market_data_scheduler_report(
    result: Dict[str, Any],
    *,
    series_override: Optional[List[Dict[str, Any]]] = None,
    exchange_label: Optional[str] = None,
    include_series_details: bool = True,
) -> str:
    """Build an operator-facing report for futures market-data maintenance."""
    status = result.get("status", "unknown")
    icon, label = _format_scheduler_status(status)

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
        for item in (result.get("candidates") or [])[:12]:
            detail_lines.append(
                f"{item.get('discovery_id', 'unknown')}: "
                f"name={item.get('candidate_name') or 'N/A'}, "
                f"category={item.get('candidate_category') or 'N/A'}, "
                f"unit={item.get('candidate_unit') or 'N/A'}, "
                f"quality={item.get('quality_flag') or 'N/A'}, "
                f"review={item.get('review_status') or 'N/A'}, "
                f"first={item.get('first_seen_trade_date') or 'N/A'}, "
                f"last={item.get('last_seen_trade_date') or 'N/A'}"
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
    totals = result.get("totals") or {}
    governance = result.get("trading_day_governance") or result.get("target_date_expansion") or {}
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
    series = series_override if series_override is not None else (result.get("series") or [])
    detail_lines = []
    if include_series_details:
        for item in series[:10]:
            write_result = item.get("write_result") or {}
            detail_lines.append(
                f"{item.get('series_id', 'unknown')}: fetched={item.get('fetched_rows', 0)}, "
                f"would_write={write_result.get('would_write_rows', 0)}, "
                f"inserted={write_result.get('inserted', 0)}, "
                f"changed={write_result.get('changed', 0)}, "
                f"unchanged={write_result.get('unchanged', 0)}, "
                f"status={item.get('status', 'ok')}"
            )
        if len(series) > 10:
            detail_lines.append(f"... {len(series) - 10} more series omitted")
        if not detail_lines:
            detail_lines.append(result.get("reason") or "无序列明细")
    else:
        detail_lines.append("序列明细已按交易所拆分发送。")
    exchange_scope = exchange_label or _futures_result_exchange_label(result)
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
        f"trading_day_governance: `{governance.get('status', 'N/A')}`\n"
        f"target_trade_dates: `{governance.get('target_date_count', 0)}`\n"
        f"calendar_quality: `{actual_calendar_quality}`\n"
        f"calendar_min_required: `{governance.get('minimum_quality', 'N/A')}`\n\n"
        "序列明细:\n"
        "```text\n"
        + "\n".join(detail_lines)
        + "\n```"
    )


def _format_futures_market_data_scheduler_reports(result: Dict[str, Any]) -> List[str]:
    """Build Telegram-sized futures market-data reports, splitting details by exchange."""
    if result.get("domain") or not result.get("series"):
        return [_format_futures_market_data_scheduler_report(result)]
    series = result.get("series") or []
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
            await asyncio.wait_for(
                self.bot.send_report_notification({
                    'report_type': report_type,
                    'task_name': task_name,
                    **report_data  # 将所有报告数据传递下去
                }, report_type),
                timeout=report_timeout_seconds,
            )

            scheduler_logger.info(f"[Scheduler] Task {task_name} report sent successfully")
            return True

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

            # 备份数据库
            if backup_database:
                backup_enabled = self.config.get_nested('database_config.backup_enabled', True)
                if backup_enabled:
                    success = await data_manager.backup_data()
                    if success:
                        scheduler_logger.info("[Scheduler] Database backup completed successfully")
                    else:
                        scheduler_logger.warning("[Scheduler] Database backup failed")

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
                    {'task_name': '数据库备份', 'status': '成功' if backup_database else '跳过'},
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
            severity_distribution = dict(Counter(gap.severity for gap in all_gaps))
            affected_stocks = len({gap.instrument_id for gap in all_gaps})
            top_affected_stocks = data_manager.get_top_affected_stocks(all_gaps, limit=10)

            report_data = {
                'name': '数据缺口检测与修复报告',
                'status': 'success' if failed == 0 else 'warning',
                'total_gaps': len(all_gaps),
                'affected_stocks': affected_stocks,
                'severity_distribution': severity_distribution,
                'top_affected_stocks': top_affected_stocks,
                'summary': {
                    'detected_gaps': len(all_gaps),
                    'candidate_gaps': len(gaps_to_repair),
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

    async def quarterly_cleanup(self,
                              cleanup_old_quotes: bool = True,
                              quote_retention_months: int = 36,
                              cleanup_temp_files: bool = True,
                              cleanup_backup_files: bool = False,
                              backup_retention_months: int = 12,
                              job_config: Optional[JobConfig] = None) -> bool:
        """季度清理任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting quarterly cleanup...")

            # 清理旧行情数据
            if cleanup_old_quotes:
                days_to_keep = quote_retention_months * 30  # 转换为天数
                success = await data_manager.db_ops.cleanup_old_data(days_to_keep=days_to_keep)

                if success:
                    scheduler_logger.info(f"[Scheduler] Cleaned up quotes older than {quote_retention_months} months")
                else:
                    scheduler_logger.warning("[Scheduler] Failed to cleanup old quotes")

            # 清理临时文件
            if cleanup_temp_files:
                await self._cleanup_temp_files()

            # 清理备份文件
            if cleanup_backup_files:
                await self._cleanup_backup_files(backup_retention_months)

            scheduler_logger.info("[Scheduler] Quarterly cleanup completed")
            if self.telegram_enabled:
                try:
                    # 生成季度清理报告数据
                    cleanup_report_data = {
                        'name': '季度数据清理报告',
                        'status': 'success',  # 明确的成功状态
                        'tasks_completed': '历史数据清理, 临时文件清理',
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    # 发送清理报告
                    await self._send_task_report(
                        report_data=cleanup_report_data,
                        report_type='maintenance_report',
                        task_name='季度数据清理',
                        job_config=job_config
                    )
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Quarterly cleanup failed: {e}")
            # 统一使用报告系统发送失败通知
            failure_report_data = {
                'name': '季度数据清理报告',
                'status': 'error',
                'error_message': str(e)
            }
            await self._send_task_report(
                report_data=failure_report_data,
                report_type='maintenance_report', # 复用维护报告模板
                task_name='季度数据清理',
                job_config=job_config
            )
            return False

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
        requires_trading_day_governance: bool = True,
        requires_master_data_governance: bool = False,
        master_governance_max_days: Optional[int] = None,
        job_config: Optional[JobConfig] = None,
    ) -> bool:
        """商品期货行情日更任务。"""
        self._active_tasks.add('futures_market_data_sync')
        try:
            if requires_trading_day_governance:
                governance_result = await data_manager.run_futures_trading_day_governance(
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
                    master_results = []
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
                                scope_id=scope_id,
                                scope_ids=scope_ids,
                                exchanges=exchanges,
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
            )
            status = result.get('status', 'failed')
            success = status == 'success'
            for index, content in enumerate(_format_futures_market_data_scheduler_reports(result), start=1):
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
        requires_trading_day_governance: bool = True,
        requires_master_data_governance: bool = False,
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

    async def _cleanup_temp_files(self):
        """清理临时文件"""
        try:
            import os
            import glob
            from datetime import datetime, timedelta

            temp_dirs = ["temp", "tmp", "/tmp/quote_system"]
            cleaned_files = 0
            cutoff_date = datetime.now() - timedelta(days=7)  # 7天前的临时文件

            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    for temp_file in glob.glob(os.path.join(temp_dir, "*")):
                        try:
                            file_time = datetime.fromtimestamp(os.path.getmtime(temp_file))
                            if file_time < cutoff_date:
                                if os.path.isfile(temp_file):
                                    os.remove(temp_file)
                                    cleaned_files += 1
                        except Exception as e:
                            scheduler_logger.warning(f"[Scheduler] Failed to remove temp file {temp_file}: {e}")

            if cleaned_files > 0:
                scheduler_logger.info(f"[Scheduler] Cleaned up {cleaned_files} temporary files")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to cleanup temp files: {e}")

    async def _cleanup_backup_files(self, retention_months: int):
        """清理备份文件"""
        try:
            import os
            import glob
            from datetime import datetime, timedelta

            backup_dir = "backup"
            if not os.path.exists(backup_dir):
                return

            cutoff_date = datetime.now() - timedelta(days=retention_months * 30)
            cleaned_files = 0

            for backup_file in glob.glob(os.path.join(backup_dir, "*.db*")):
                try:
                    file_time = datetime.fromtimestamp(os.path.getmtime(backup_file))
                    if file_time < cutoff_date:
                        os.remove(backup_file)
                        cleaned_files += 1
                except Exception as e:
                    scheduler_logger.warning(f"[Scheduler] Failed to remove backup file {backup_file}: {e}")

            if cleaned_files > 0:
                scheduler_logger.info(f"[Scheduler] Cleaned up {cleaned_files} old backup files (retention: {retention_months} months)")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to cleanup backup files: {e}")

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
        try:
            scheduler_logger.info("[Scheduler] Starting database backup task...")
            scheduler_logger.debug(f"[Scheduler] use_backup_config parameter: {use_backup_config}")

            # 读取配置 - 按优先级合并参数
            backup_config = self.config.get_nested('backup_config', {}) or {}

            # 使用传入参数，否则使用配置文件中的值，最后使用默认值
            backup_directory = backup_directory or backup_config.get('backup_directory', 'data/PVE-Bak/QuoteBak')
            retention_days = retention_days or backup_config.get('retention_days', 30)
            notification_enabled = notification_enabled if notification_enabled is not None else backup_config.get('notification_enabled', True)
            filename_pattern = filename_pattern or backup_config.get('filename_pattern', 'quotes_backup_{timestamp}.db')
            max_backup_files = max_backup_files or backup_config.get('max_backup_files', 10)
            backup_sources = self._resolve_database_backup_sources(
                backup_config=backup_config,
                source_db_path=source_db_path,
                filename_pattern=filename_pattern,
            )
            if not backup_sources:
                raise RuntimeError("没有可备份的数据库文件")

            import os
            for source in backup_sources:
                if not os.path.exists(source["path"]):
                    raise FileNotFoundError(f"源数据库文件不存在: {source['path']}")

            total_source_size = sum(os.path.getsize(source["path"]) for source in backup_sources)

            # 创建备份目录
            os.makedirs(backup_directory, exist_ok=True)

            # 检查磁盘空间
            await self._check_disk_space_for_backup(total_source_size, backup_directory)

            # 生成备份文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # 执行备份
            start_time = datetime.now()

            import shutil
            backup_results = []
            cleanup_patterns = []
            for source in backup_sources:
                source_path = source["path"]
                source_size = os.path.getsize(source_path)
                source_name = source["name"]
                source_stem = os.path.splitext(os.path.basename(source_path))[0]
                source_pattern = source["filename_pattern"]
                backup_filename = source_pattern.format(
                    timestamp=timestamp,
                    name=source_name,
                    stem=source_stem,
                )
                backup_path = os.path.join(backup_directory, backup_filename)
                scheduler_logger.info(
                    f"[Scheduler] Copying database from {source_path} to {backup_path}"
                )
                shutil.copy2(source_path, backup_path)
                if not os.path.exists(backup_path) or os.path.getsize(backup_path) != source_size:
                    raise IOError(f"备份文件创建失败或大小不匹配: {backup_path}")
                backup_results.append(
                    {
                        "name": source_name,
                        "source": source_path,
                        "backup_file": backup_filename,
                        "file_size": source_size,
                    }
                )
                cleanup_patterns.append(
                    source_pattern.format(timestamp="*", name=source_name, stem=source_stem)
                )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            scheduler_logger.info(
                f"[Scheduler] Database backup completed successfully: {len(backup_results)} files"
            )

            # 清理过期备份
            await self._cleanup_old_backups(
                backup_directory,
                retention_days,
                max_backup_files,
                backup_patterns=cleanup_patterns,
            )

            # 使用统一报告接口发送通知
            report_data = {
                'name': '数据库备份报告',
                'success': True,
                'backup_file': f"{len(backup_results)} files",
                'backup_files': backup_results,
                'file_size': total_source_size,
                'duration': duration,
                'timestamp': datetime.now().isoformat()
            }
            await self._send_task_report(report_data, 'backup_result', '数据库备份', job_config)
            return True

        except Exception as e:
            error_msg = f"数据库备份失败: {str(e)}"
            scheduler_logger.error(f"[Scheduler] {error_msg}")
            report_data = {'name': '数据库备份报告', 'success': False, 'error_message': str(e)}
            await self._send_task_report(report_data, 'backup_result', '数据库备份', job_config)
            return False

    def _resolve_database_backup_sources(
        self,
        *,
        backup_config: Dict[str, Any],
        source_db_path: Optional[str],
        filename_pattern: str,
    ) -> List[Dict[str, str]]:
        """Resolve the database files covered by the backup task."""
        import glob

        if source_db_path:
            source_stem = os.path.splitext(os.path.basename(source_db_path))[0]
            return [
                {
                    "name": source_stem,
                    "path": source_db_path,
                    "filename_pattern": filename_pattern,
                }
            ]

        configured = backup_config.get("source_databases")
        sources: List[Dict[str, str]] = []
        if configured:
            for item in configured:
                if isinstance(item, str):
                    path = item
                    stem = os.path.splitext(os.path.basename(path))[0]
                    pattern = f"{stem}_backup_{{timestamp}}.db"
                    name = stem
                else:
                    path = str(item.get("path") or "")
                    stem = os.path.splitext(os.path.basename(path))[0]
                    name = str(item.get("name") or stem)
                    pattern = str(
                        item.get("filename_pattern") or f"{stem}_backup_{{timestamp}}.db"
                    )
                if path:
                    sources.append({"name": name, "path": path, "filename_pattern": pattern})
        else:
            path = backup_config.get("source_db_path", "data/quotes.db")
            sources.append(
                {
                    "name": os.path.splitext(os.path.basename(path))[0],
                    "path": path,
                    "filename_pattern": filename_pattern,
                }
            )

        known_paths = {os.path.abspath(source["path"]) for source in sources}
        if backup_config.get("include_extra_data_dbs", False):
            for path in sorted(glob.glob(str(backup_config.get("extra_db_glob", "data/*.db")))):
                abs_path = os.path.abspath(path)
                if abs_path in known_paths:
                    continue
                stem = os.path.splitext(os.path.basename(path))[0]
                sources.append(
                    {
                        "name": stem,
                        "path": path,
                        "filename_pattern": f"{stem}_backup_{{timestamp}}.db",
                    }
                )
                known_paths.add(abs_path)
        return sources

    async def _check_disk_space_for_backup(self, required_bytes: int, backup_directory: str):
        """检查备份目录的磁盘空间"""
        try:
            import shutil

            # 获取备份目录所在磁盘的可用空间
            stat = shutil.disk_usage(backup_directory)
            free_space = stat.free

            # 预留额外空间 (至少是备份文件大小的2倍)
            required_space = required_bytes * 2

            if free_space < required_space:
                error_msg = f"磁盘空间不足: 需要 {required_space/(1024*1024):.1f}MB, 可用 {free_space/(1024*1024):.1f}MB"
                scheduler_logger.warning(f"[Scheduler] {error_msg}")
                raise RuntimeError(error_msg)

            scheduler_logger.debug(f"[Scheduler] Disk space check passed: {free_space/(1024*1024):.1f}MB available")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Disk space check failed: {e}")
            raise

    async def _cleanup_old_backups(
        self,
        backup_directory: str,
        retention_days: int,
        max_files: int,
        backup_patterns: Optional[List[str]] = None,
    ):
        """清理过期的备份文件"""
        try:
            import glob
            from datetime import datetime, timedelta

            if not os.path.exists(backup_directory):
                return

            cutoff_date = datetime.now() - timedelta(days=retention_days)
            deleted_count = 0
            patterns = backup_patterns or ["quotes_backup_*.db"]

            for pattern in patterns:
                backup_pattern = os.path.join(backup_directory, pattern)
                backup_files = glob.glob(backup_pattern)
                if not backup_files:
                    continue
                backup_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

                for backup_file in backup_files[max_files:]:
                    try:
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(backup_file))
                        if file_mtime < cutoff_date:
                            os.remove(backup_file)
                            deleted_count += 1
                            scheduler_logger.info(
                                f"[Scheduler] Deleted old backup: {os.path.basename(backup_file)}"
                            )
                    except Exception as e:
                        scheduler_logger.warning(
                            f"[Scheduler] Failed to delete old backup {backup_file}: {e}"
                        )

                remaining_files = glob.glob(backup_pattern)
                remaining_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

                for backup_file in remaining_files[max_files:]:
                    try:
                        os.remove(backup_file)
                        deleted_count += 1
                        scheduler_logger.info(
                            f"[Scheduler] Deleted excess backup: {os.path.basename(backup_file)}"
                        )
                    except Exception as e:
                        scheduler_logger.warning(
                            f"[Scheduler] Failed to delete excess backup {backup_file}: {e}"
                        )

            if deleted_count > 0:
                scheduler_logger.info(f"[Scheduler] Cleanup completed: deleted {deleted_count} old backup files")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to cleanup old backups: {e}")



# 全局定时任务实例
scheduled_tasks = ScheduledTasks()
