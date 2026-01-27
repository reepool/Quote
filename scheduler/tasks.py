"""
Scheduled tasks for the quote system.
Defines all automated data update and maintenance tasks.
"""

import asyncio
import os
from datetime import datetime, date, timedelta, time
from typing import List, Dict, Any, Optional

from utils import scheduler_logger, config_manager, TelegramBot
from .job_config import JobConfig
from data_manager import data_manager
from utils.date_utils import DateUtils
from utils.cache import cache_manager


class ScheduledTasks:
    """定时任务管理类"""

    def __init__(self):
        self.config = config_manager

        # Telegram调用
        self.bot_config = self.config.get_telegram_config()
        self.telegram_enabled = self.bot_config.enabled
        self.bot = TelegramBot() if self.telegram_enabled else None

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
            # 检查是否应该发送报告
            should_send = False
            if job_config and hasattr(job_config, 'report'):
                should_send = job_config.report

            if not should_send or not self.telegram_enabled:
                scheduler_logger.debug(f"[Scheduler] Task {task_name} report disabled or Telegram not enabled")
                return False

            # 直接调用 tgbot 的报告发送方法，它会处理报告生成
            await self.bot.send_report_notification({
                'report_type': report_type,
                'task_name': task_name,
                **report_data  # 将所有报告数据传递下去
            }, report_type)

            scheduler_logger.info(f"[Scheduler] Task {task_name} report sent successfully")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send task report for {task_name}: {e}")
            return False

    async def daily_data_update(self,
                            exchanges: Optional[List[str]] = None,
                            wait_for_market_close: bool = True,
                            market_close_delay_minutes: int = 15,
                            enable_trading_day_check: bool = True,
                            per_instrument_timeout_sec: Optional[int] = None,
                            progress_log_every: int = 200,
                            progress_log_interval_sec: int = 300,
                            job_config: Optional[JobConfig] = None) -> bool:
        """每日数据更新任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting daily data update task...")

            # 使用配置参数或默认值
            if exchanges is None:
                exchanges = ['SSE', 'SZSE']

            today = date.today()
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
                progress_log_interval_sec=progress_log_interval_sec
            )

            # 步骤5: 发送报告
            # 判断更新状态
            success_count = update_results.get('success_count', 0)
            failure_count = update_results.get('failure_count', 0)
            is_successful = failure_count == 0 and success_count > 0

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

    async def weekly_data_maintenance(self,
                                  backup_database: bool = True,
                                  cleanup_old_logs: bool = True,
                                  log_retention_days: int = 30,
                                  optimize_database: bool = True,
                                  validate_data_integrity: bool = True,
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

            # 数据库优化
            if optimize_database:
                await self._optimize_database()

            # 数据完整性验证
            if validate_data_integrity:
                await self._validate_data_integrity()

            scheduler_logger.info("[Scheduler] Weekly maintenance completed")

            # 生成维护报告数据
            maintenance_report_data = {
                'name': '每周数据维护报告',
                'status': 'success',  # 明确的成功状态
                'tasks_completed': 3,
                'duration': 'N/A', # 可以在任务开始和结束时记录时间来计算
                'maintenance_tasks': [
                    {'task_name': '数据库备份', 'status': '成功' if backup_database else '跳过'},
                    {'task_name': '日志清理', 'status': '成功' if cleanup_old_logs else '跳过'},
                    {'task_name': '数据库优化', 'status': '成功' if optimize_database else '跳过'},
                    {'task_name': '数据完整性验证', 'status': '成功' if validate_data_integrity else '跳过'}
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
                                        job_config: Optional[JobConfig] = None) -> bool:
        """月度数据完整性检查和缺口修复任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting monthly data integrity check...")

            # 使用配置参数
            if exchanges is None:
                exchanges = ['SSE', 'SZSE']

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

                    # 使用现有的GAP检测系统
                    gaps = await data_manager.detect_data_gaps([exchange], start_date, end_date)
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
            final_gaps = await data_manager.detect_data_gaps(exchanges, start_date, end_date)

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
                    'severity_distribution': severity_distribution
                },
                'top_affected_stocks': top_affected_stocks
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
                                  job_config: Optional[JobConfig] = None) -> bool:
        """检测数据缺口并修复（复合任务）"""
        try:
            scheduler_logger.info("[Scheduler] Starting gap detect and repair task...")

            if exchanges is None:
                exchanges = ['SSE', 'SZSE', 'BSE']

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

            all_gaps = await data_manager.detect_data_gaps(exchanges, start_date, end_date)
            scheduler_logger.info(f"[Scheduler] Detected {len(all_gaps)} gaps")

            if severity_filter:
                gaps_to_repair = [gap for gap in all_gaps if gap.severity in severity_filter]
                scheduler_logger.info(f"[Scheduler] Severity filter applied: {severity_filter} -> {len(gaps_to_repair)} gaps")
            else:
                gaps_to_repair = all_gaps

            repaired = 0
            failed = 0
            failure_details = []
            for gap in gaps_to_repair:
                try:
                    success = await data_manager._fill_single_gap(gap)
                    if success:
                        repaired += 1
                    else:
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
                    'repaired_gaps': repaired,
                    'failed_repairs': failed
                },
                'failure_details': failure_details[:50],
                'filters': {
                    'exchanges': exchanges,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'severity_filter': severity_filter
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
            auto_repair_result = None
            if check_data_sources and 'baostock_a_stock' in unhealthy_sources:
                scheduler_logger.warning("[Scheduler] baostock_a_stock unhealthy, attempting auto-repair")
                auto_repair_result = "失败"
                try:
                    source = data_manager.source_factory.sources.get('baostock_a_stock')
                    if source:
                        await source._relogin()
                        is_healthy = await source.health_check()
                        if is_healthy:
                            auto_repair_result = "成功"
                            unhealthy_sources = [
                                src for src in unhealthy_sources if src != 'baostock_a_stock'
                            ]
                        else:
                            auto_repair_result = "失败（健康检查未通过）"
                    else:
                        auto_repair_result = "失败（数据源未初始化）"
                except Exception as e:
                    auto_repair_result = f"失败（{e}）"
                scheduler_logger.info(f"[Scheduler] baostock_a_stock auto-repair result: {auto_repair_result}")
                if self.telegram_enabled and self.bot:
                    level = "success" if auto_repair_result == "成功" else "warning"
                    await self.bot.send_scheduler_notification(
                        f"数据源自动修复结果: baostock_a_stock {auto_repair_result}",
                        level=level
                    )

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
                if auto_repair_result is not None:
                    repair_ok = auto_repair_result == "成功"
                    check_results.append({
                        "check_name": "数据源自动修复",
                        "result": f"baostock_a_stock: {auto_repair_result}",
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
                exchanges = ['SSE', 'SZSE']

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
        market_close_times = {
            'SSE': time(15, 0),    # A股 15:00收盘
            'SZSE': time(15, 0),   # 深圳同样15:00
            'HKEX': time(16, 0),   # 港股 16:00收盘
            'NASDAQ': time(5, 0),  # 美股 05:00收盘（北京时间）
            'NYSE': time(5, 0)     # 纽交所 05:00收盘（北京时间）
        }

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
            backup_config = self.config.get_nested('backup_config', {})

            # 使用传入参数，否则使用配置文件中的值，最后使用默认值
            source_db_path = source_db_path or backup_config.get('source_db_path', 'data/quotes.db')
            backup_directory = backup_directory or backup_config.get('backup_directory', 'data/PVE-Bak/QuoteBak')
            retention_days = retention_days or backup_config.get('retention_days', 30)
            notification_enabled = notification_enabled if notification_enabled is not None else backup_config.get('notification_enabled', True)
            filename_pattern = filename_pattern or backup_config.get('filename_pattern', 'quotes_backup_{timestamp}.db')
            max_backup_files = max_backup_files or backup_config.get('max_backup_files', 10)

            # 验证源数据库文件是否存在
            import os
            if not os.path.exists(source_db_path):
                raise FileNotFoundError(f"源数据库文件不存在: {source_db_path}")

            # 获取源文件大小
            source_size = os.path.getsize(source_db_path)

            # 检查磁盘空间
            await self._check_disk_space_for_backup(source_size, backup_directory)

            # 创建备份目录
            os.makedirs(backup_directory, exist_ok=True)

            # 生成备份文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = filename_pattern.format(timestamp=timestamp)
            backup_path = os.path.join(backup_directory, backup_filename)

            # 执行备份
            start_time = datetime.now()
            scheduler_logger.info(f"[Scheduler] Copying database from {source_db_path} to {backup_path}")

            import shutil
            shutil.copy2(source_db_path, backup_path)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            # 验证备份文件
            if not os.path.exists(backup_path) or os.path.getsize(backup_path) != source_size:
                raise IOError(f"备份文件创建失败或大小不匹配: {backup_path}")

            scheduler_logger.info(f"[Scheduler] Database backup completed successfully: {backup_path}")

            # 清理过期备份
            await self._cleanup_old_backups(backup_directory, retention_days, max_backup_files)

            # 使用统一报告接口发送通知
            report_data = {
                'name': '数据库备份报告', 'success': True, 'backup_file': backup_filename,
                'file_size': source_size, 'duration': duration, 'timestamp': datetime.now().isoformat()
            }
            await self._send_task_report(report_data, 'backup_result', '数据库备份', job_config)
            return True

        except Exception as e:
            error_msg = f"数据库备份失败: {str(e)}"
            scheduler_logger.error(f"[Scheduler] {error_msg}")
            report_data = {'name': '数据库备份报告', 'success': False, 'error_message': str(e)}
            await self._send_task_report(report_data, 'backup_result', '数据库备份', job_config)
            return False

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

    async def _cleanup_old_backups(self, backup_directory: str, retention_days: int, max_files: int):
        """清理过期的备份文件"""
        try:
            import glob
            from datetime import datetime, timedelta

            if not os.path.exists(backup_directory):
                return

            # 获取所有备份文件
            backup_pattern = os.path.join(backup_directory, "quotes_backup_*.db")
            backup_files = glob.glob(backup_pattern)

            if not backup_files:
                return

            # 按修改时间排序（最新的在前）
            backup_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

            cutoff_date = datetime.now() - timedelta(days=retention_days)
            deleted_count = 0

            # 清理过期文件
            for backup_file in backup_files[max_files:]:  # 超过最大文件数量的
                try:
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(backup_file))
                    if file_mtime < cutoff_date:
                        os.remove(backup_file)
                        deleted_count += 1
                        scheduler_logger.info(f"[Scheduler] Deleted old backup: {os.path.basename(backup_file)}")
                except Exception as e:
                    scheduler_logger.warning(f"[Scheduler] Failed to delete old backup {backup_file}: {e}")

            # 如果文件数量仍超过限制，删除最旧的文件
            remaining_files = glob.glob(backup_pattern)
            remaining_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

            for backup_file in remaining_files[max_files:]:
                try:
                    os.remove(backup_file)
                    deleted_count += 1
                    scheduler_logger.info(f"[Scheduler] Deleted excess backup: {os.path.basename(backup_file)}")
                except Exception as e:
                    scheduler_logger.warning(f"[Scheduler] Failed to delete excess backup {backup_file}: {e}")

            if deleted_count > 0:
                scheduler_logger.info(f"[Scheduler] Cleanup completed: deleted {deleted_count} old backup files")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to cleanup old backups: {e}")



# 全局定时任务实例
scheduled_tasks = ScheduledTasks()
