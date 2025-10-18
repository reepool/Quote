"""
Scheduled tasks for the quote system.
Defines all automated data update and maintenance tasks.
"""

import asyncio
import os
from datetime import datetime, date, timedelta, time
from typing import List

from utils import scheduler_logger, config_manager, TelegramBot

from data_manager import data_manager
from utils.date_utils import DateUtils
from utils.cache import cache_manager
from utils.report import (
    format_daily_update_report,
    format_health_check_report,
    format_maintenance_report,
    format_cache_warm_up_report,
    format_trading_calendar_report,
    format_backup_result,
    format_gap_report
)


class ScheduledTasks:
    """定时任务管理类"""

    def __init__(self):
        self.config = config_manager
        self.telegram_enabled = self.config.get_nested('telegram_config.enabled', False)

    async def _send_task_report(self, report_data: dict, report_type: str,
                               task_name: str, job_config=None) -> bool:
        """
        统一的任务报告发送方法

        Args:
            report_data: 报告数据
            report_type: 报告类型
            task_name: 任务名称
            job_config: 任务配置对象，用于判断是否发送报告

        Returns:
            bool: 是否发送成功
        """
        try:
            # 检查是否应该发送报告
            should_send = False
            if job_config and hasattr(job_config, 'report'):
                should_send = job_config.report
            else:
                # 如果没有配置，默认不发送
                should_send = False

            if not should_send or not self.telegram_enabled:
                scheduler_logger.debug(f"[Scheduler] Task {task_name} report disabled or Telegram not enabled")
                return False

            # 选择对应的格式化方法
            formatter_map = {
                'daily_update_report': format_daily_update_report,
                'health_check_report': format_health_check_report,
                'maintenance_report': format_maintenance_report,
                'cache_warm_up_report': format_cache_warm_up_report,
                'trading_calendar_report': format_trading_calendar_report,
                'backup_result': format_backup_result,
                'gap_report': format_gap_report
            }

            formatter = formatter_map.get(report_type)
            if not formatter:
                scheduler_logger.warning(f"[Scheduler] Unknown report type: {report_type}")
                return False

            # 生成报告
            formatted_report = formatter(report_data, 'telegram')

            # 发送报告
            async with TelegramBot() as bot:
                await bot.send_report_notification({
                    'report_type': report_type,
                    'task_name': task_name,
                    'content': formatted_report,
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }, report_type)

            scheduler_logger.info(f"[Scheduler] Task {task_name} report sent successfully")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send task report for {task_name}: {e}")
            return False

    async def initialize(self):
        """初始化定时任务"""
        scheduler_logger.info("[Scheduler] Initializing scheduled tasks...")
        # 使用统一的通知接口
        if self.telegram_enabled:
            try:
                async with TelegramBot() as bot:
                    await bot.send_task_notification("定时任务系统已启动，开始加载任务...")
            except Exception as e:
                scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

    async def daily_data_update(self,
                            exchanges: List[str] = None,
                            wait_for_market_close: bool = True,
                            market_close_delay_minutes: int = 15,
                            enable_trading_day_check: bool = True,
                            job_config=None):
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
                    # 没有交易日，发送通知并退出
                    await self._send_non_trading_day_notification(today, exchanges, trading_calendar_updates)
                    return False

            else:
                trading_exchanges = exchanges

            # 步骤3: 等待市场收盘
            if wait_for_market_close:
                scheduler_logger.info("[Scheduler] Step 3: Waiting for market close...")
                await self._wait_for_markets_close(trading_exchanges, market_close_delay_minutes)

            # 步骤4: 执行数据更新
            scheduler_logger.info("[Scheduler] Step 4: Executing data update...")
            update_results = await data_manager.update_daily_data(exchanges=trading_exchanges, target_date=today)

            # 步骤5: 发送报告
            report_data = {
                'name': '每日数据更新报告',
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
            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(f"❌ 每日数据更新失败: {str(e)}")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return False

    async def weekly_data_maintenance(self,
                                  backup_database: bool = True,
                                  cleanup_old_logs: bool = True,
                                  log_retention_days: int = 30,
                                  optimize_database: bool = True,
                                  validate_data_integrity: bool = True):
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
            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification("每周数据维护已完成")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Weekly maintenance failed: {e}")
            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(f"每周数据维护失败: {str(e)}")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return False

    async def monthly_data_integrity_check(self,
                                        exchanges: List[str] = None,
                                        severity_filter: List[str] = None,
                                        days_to_check: int = 45):
        """月度数据完整性检查和缺口修复任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting monthly data integrity check...")

            # 使用配置参数
            if exchanges is None:
                exchanges = ['SSE', 'SZSE']
            if severity_filter is None:
                severity_filter = ['high', 'critical']

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

            total_gaps_found = 0
            total_gaps_filled = 0
            exchange_results = {}

            # 检查每个交易所的数据缺口
            for exchange in exchanges:
                try:
                    scheduler_logger.info(f"[Scheduler] Checking gaps for {exchange}...")

                    # 使用现有的GAP检测系统
                    gaps = await data_manager.detect_data_gaps([exchange], start_date, end_date)

                    # 过滤严重程度
                    filtered_gaps = [g for g in gaps if g.severity in severity_filter]

                    total_gaps_found += len(filtered_gaps)
                    exchange_results[exchange] = {
                        'total_gaps': len(gaps),
                        'filtered_gaps': len(filtered_gaps),
                        'gaps_filled': 0
                    }

                    if filtered_gaps:
                        scheduler_logger.info(f"[Scheduler] Found {len(filtered_gaps)} gaps ({len(gaps)} total) for {exchange}")

                        # 使用现有的缺口填补系统
                        filled_count = 0
                        for gap in filtered_gaps:
                            try:
                                success = await data_manager._fill_single_gap(gap)
                                if success:
                                    filled_count += 1
                                    total_gaps_filled += 1
                                    exchange_results[exchange]['gaps_filled'] = filled_count

                                # API限流控制
                                await asyncio.sleep(0.5)

                            except Exception as gap_e:
                                scheduler_logger.warning(f"[Scheduler] Failed to fill gap for {gap.instrument_id}: {gap_e}")

                        scheduler_logger.info(f"[Scheduler] Filled {filled_count}/{len(filtered_gaps)} gaps for {exchange}")
                    else:
                        scheduler_logger.info(f"[Scheduler] No significant gaps found for {exchange}")

                except Exception as exchange_e:
                    scheduler_logger.error(f"[Scheduler] Failed to check {exchange}: {exchange_e}")
                    exchange_results[exchange] = {'error': str(exchange_e)}

            # 发送详细的完成通知
            await self._send_integrity_check_notification(
                start_date, end_date, exchange_results, total_gaps_found, total_gaps_filled
            )

            scheduler_logger.info(f"[Scheduler] Monthly data integrity check completed")
            scheduler_logger.info(f"[Scheduler] Summary: {total_gaps_filled}/{total_gaps_found} gaps filled")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Monthly data integrity check failed: {e}")
            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(f"❌ 月度数据完整性检查失败: {str(e)}")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return False

    async def quarterly_cleanup(self,
                              cleanup_old_quotes: bool = True,
                              quote_retention_months: int = 36,
                              cleanup_temp_files: bool = True,
                              cleanup_backup_files: bool = False,
                              backup_retention_months: int = 12):
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
                    async with TelegramBot() as bot:
                        await bot.send_task_notification("季度数据清理已完成")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Quarterly cleanup failed: {e}")
            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(f"季度清理失败: {str(e)}")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return False

    async def system_health_check(self,
                                check_data_sources: bool = True,
                                check_database: bool = True,
                                check_disk_space: bool = True,
                                check_memory_usage: bool = True,
                                disk_space_threshold_mb: int = 1000,
                                memory_threshold_percent: int = 85,
                                job_config=None):
        """系统健康检查任务"""
        try:
            scheduler_logger.info("[Scheduler] Starting system health check...")

            status = await data_manager.get_system_status()

            # 检查数据源健康状态
            if check_data_sources:
                unhealthy_sources = [
                    source for source, is_healthy in status.get('data_sources', {}).items()
                    if not is_healthy
                ]

                if unhealthy_sources:
                    warning_msg = f"警告: 以下数据源状态异常: {', '.join(unhealthy_sources)}"
                    scheduler_logger.warning(warning_msg)
                    if self.telegram_enabled:
                        try:
                            async with TelegramBot() as bot:
                                await bot.send_task_notification(warning_msg)
                        except Exception as e:
                            scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

            # 检查数据库连接
            if check_database:
                if not status.get('database'):
                    error_msg = "错误: 数据库连接异常"
                    scheduler_logger.error(error_msg)
                    if self.telegram_enabled:
                        try:
                            async with TelegramBot() as bot:
                                await bot.send_task_notification(error_msg)
                        except Exception as e:
                            scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

            # 检查磁盘空间
            if check_disk_space:
                await self._check_disk_space(disk_space_threshold_mb)

            # 检查内存使用
            if check_memory_usage:
                await self._check_memory_usage(memory_threshold_percent)

            # 检查缓存状态
            cache_stats = status.get('cache', {})
            if cache_stats.get('cache_enabled'):
                scheduler_logger.info(f"[Scheduler] Cache stats: {cache_stats}")

            # 生成健康检查报告数据
            check_results = []
            if check_data_sources:
                check_results.append("✅ 数据源连接正常")
            if check_database:
                check_results.append("✅ 数据库状态正常")
            if check_disk_space:
                check_results.append("✅ 磁盘空间充足")
            if check_memory_usage:
                check_results.append("✅ 内存使用正常")

            report_data = {
                'name': '系统健康检查报告',
                'overall_status': 'HEALTHY' if len([r for r in check_results if '❌' in r]) == 0 else 'WARNING',
                'checks_performed': len(check_results),
                'issues_found': len([r for r in check_results if '❌' in r]),
                'check_results': check_results,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

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
            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(f"系统健康检查失败: {str(e)}")
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            return False

    async def cache_warm_up(self,
                           warm_popular_stocks: bool = True,
                           popular_stocks_count: int = 50,
                           warm_market_indices: bool = True,
                           preload_recent_data: bool = True,
                           recent_data_days: int = 7,
                           job_config=None):
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
                                    exchanges: List[str] = None,
                                    update_future_months: int = 6,
                                    force_update: bool = False,
                                    validate_holidays: bool = True,
                                    job_config=None):
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

            # 返回成功状态
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
                        async with TelegramBot() as bot:
                            await bot.send_task_notification(warning_msg)
                    except Exception as e:
                        scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
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
                        async with TelegramBot() as bot:
                            await bot.send_task_notification(warning_msg)
                    except Exception as e:
                        scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")
            else:
                scheduler_logger.debug(f"[Scheduler] Memory usage OK: {used_percent:.1f}%")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to check memory usage: {e}")

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

    async def _send_non_trading_day_notification(self, today: date, exchanges: List[str], trading_calendar_updates: dict):
        """发送非交易日通知"""
        try:
            # 格式化交易日历更新信息
            calendar_info = []
            for exchange, count in trading_calendar_updates.items():
                calendar_info.append(f"{exchange}: {count}天")
            calendar_str = ", ".join(calendar_info)

            # 使用新的统一报告系统
            from utils.report import generate_report

            # 构建报告数据
            report_data = {
                'target_date': today.strftime('%Y-%m-%d'),
                'calendar_updates': trading_calendar_updates,
                'non_trading_day': True
            }

            # 使用统一报告生成器
            formatted_message = generate_report('daily_update_report', report_data, 'telegram')

            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(formatted_message)
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send non-trading day notification: {e}")

    async def _send_daily_update_completion_notification(self, today: date, trading_exchanges: List[str],
                                                        update_results: dict, trading_calendar_updates: dict):
        """发送每日更新完成通知"""
        try:
            # 构建统一的每日更新报告数据格式
            update_report = {
                'summary': {
                    'target_date': today.strftime('%Y-%m-%d'),
                    'total_instruments_checked': update_results.get('success_count', 0) + update_results.get('failure_count', 0),
                    'updated_instruments': update_results.get('success_count', 0),
                    'new_quotes_added': update_results.get('total_quotes_added', 0),
                    'success_rate': (update_results.get('success_count', 0) /
                                  (update_results.get('success_count', 0) + update_results.get('failure_count', 0)) * 100
                                  if (update_results.get('success_count', 0) + update_results.get('failure_count', 0)) > 0 else 0)
                },
                'exchange_stats': {}
            }

            # 构建交易所统计信息
            for exchange in trading_exchanges:
                if exchange in update_results.get('exchange_results', {}):
                    result = update_results['exchange_results'][exchange]
                    if 'error' not in result:
                        update_report['exchange_stats'][exchange] = {
                            'updated_count': result.get('success_count', 0),
                            'checked_count': result.get('success_count', 0),
                            'new_quotes': result.get('quotes_added', 0)
                        }

            # 使用新的统一报告系统
            from utils.report import generate_report
            formatted_message = generate_report('daily_update_report', update_report, 'telegram')

            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(formatted_message)
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send daily update completion notification: {e}")

    
    async def database_backup(self,
                            use_backup_config: bool = True,
                            source_db_path: str = None,
                            backup_directory: str = None,
                            retention_days: int = None,
                            notification_enabled: bool = None,
                            filename_pattern: str = None,
                            max_backup_files: int = None):
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
            if not os.path.exists(source_db_path):
                error_msg = f"源数据库文件不存在: {source_db_path}"
                scheduler_logger.error(f"[Scheduler] {error_msg}")
                await self._send_backup_notification(False, error_msg, notification_enabled)
                return False

            # 获取源文件大小
            source_size = os.path.getsize(source_db_path)
            source_size_mb = source_size / (1024 * 1024)

            # 检查磁盘空间
            await self._check_disk_space_for_backup(source_size, backup_directory)

            # 创建备份目录
            os.makedirs(backup_directory, exist_ok=True)

            # 生成备份文件名
            from datetime import datetime
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
            if not os.path.exists(backup_path):
                error_msg = f"备份文件创建失败: {backup_path}"
                scheduler_logger.error(f"[Scheduler] {error_msg}")
                await self._send_backup_notification(False, error_msg, notification_enabled)
                return False

            backup_size = os.path.getsize(backup_path)
            if backup_size != source_size:
                error_msg = f"备份文件大小不匹配: 源文件 {source_size} bytes, 备份文件 {backup_size} bytes"
                scheduler_logger.error(f"[Scheduler] {error_msg}")
                await self._send_backup_notification(False, error_msg, notification_enabled)
                return False

            scheduler_logger.info(f"[Scheduler] Database backup completed successfully")
            scheduler_logger.info(f"[Scheduler] Backup details: {backup_path}, Size: {backup_size/(1024*1024):.2f}MB, Duration: {duration:.2f}s")

            # 清理过期备份
            await self._cleanup_old_backups(backup_directory, retention_days, max_backup_files)

            # 发送成功通知
            success_msg = f"数据库备份成功: {backup_filename} ({backup_size/(1024*1024):.2f}MB, 耗时 {duration:.2f}s)"
            await self._send_backup_notification(True, success_msg, notification_enabled, {
                'backup_file': backup_filename,
                'backup_size_mb': backup_size/(1024*1024),
                'duration_seconds': duration,
                'backup_path': backup_path
            })

            return True

        except Exception as e:
            error_msg = f"数据库备份失败: {str(e)}"
            scheduler_logger.error(f"[Scheduler] {error_msg}")
            await self._send_backup_notification(False, error_msg, notification_enabled)
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

    async def _send_backup_notification(self, success: bool, message: str, notification_enabled: bool = True, details: dict = None):
        """发送备份任务通知"""
        if not notification_enabled:
            scheduler_logger.info(f"[Scheduler] Backup notification disabled: {message}")
            return

        try:
            if success:
                # 使用新的统一报告系统
                from utils.report import generate_report

                # 构建统一的备份数据格式
                backup_result = {
                    'success': True,
                    'backup_file': details.get('backup_file', 'N/A') if details else 'N/A',
                    'file_size': details.get('backup_size_mb', 0) * 1024 * 1024 if details else 0,  # 转换为字节
                    'duration': details.get('duration_seconds', 0) if details else 0,
                    'timestamp': details.get('timestamp', datetime.now().isoformat()) if details else datetime.now().isoformat()
                }

                formatted_message = generate_report('backup_result', backup_result, 'telegram')
            else:
                # 失败通知使用新系统
                from utils.report import generate_report

                backup_result = {
                    'success': False,
                    'error_message': message
                }

                formatted_message = generate_report('backup_result', backup_result, 'telegram')

            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(formatted_message)
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send backup notification: {e}")

    async def _send_integrity_check_notification(self, start_date: date, end_date: date,
                                               exchange_results: dict, total_gaps_found: int, total_gaps_filled: int):
        """发送月度数据完整性检查通知"""
        try:
            # 计算成功率
            success_rate = (total_gaps_filled / total_gaps_found * 100) if total_gaps_found > 0 else 100.0

            # 构建详细消息
            date_range = f"{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}"

            # 构建交易所详情
            exchange_details = []
            for exchange, result in exchange_results.items():
                if 'error' not in result:
                    total = result.get('total_gaps', 0)
                    filtered = result.get('filtered_gaps', 0)
                    filled = result.get('gaps_filled', 0)

                    if filtered > 0:
                        exchange_details.append(f"• {exchange}: 发现{filtered}个缺口，修复{filled}个")
                    else:
                        exchange_details.append(f"• {exchange}: 无重要缺口")
                else:
                    exchange_details.append(f"• {exchange}: 检查失败")

            # 构建报告数据
            integrity_report = {
                'start_date': start_date,
                'end_date': end_date,
                'exchange_results': exchange_results,
                'total_gaps_found': total_gaps_found,
                'total_gaps_filled': total_gaps_filled,
                'success_rate': success_rate,
                'exchange_details': exchange_details
            }

            # 使用新的统一报告系统
            from utils.report import generate_report
            formatted_message = generate_report('system_status', integrity_report, 'telegram')

            if self.telegram_enabled:
                try:
                    async with TelegramBot() as bot:
                        await bot.send_task_notification(formatted_message)
                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to send notification: {e}")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to send integrity check notification: {e}")


# 全局定时任务实例
scheduled_tasks = ScheduledTasks()