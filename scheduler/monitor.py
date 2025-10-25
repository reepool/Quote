"""
Scheduler monitoring and management tools.
Provides utilities for monitoring scheduled tasks and managing scheduler state.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from utils import monitor_logger, TelegramBot
from utils.config_manager import UnifiedConfigManager, config_manager

from .scheduler import task_scheduler
from .tasks import scheduled_tasks
from utils.date_utils import get_shanghai_time


@dataclass
class JobExecutionRecord:
    """任务执行记录"""
    job_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, completed, failed
    duration: Optional[float] = None  # 执行时长（秒）
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class SchedulerMonitor:
    """调度器监控器"""

    def __init__(self, config_manager: UnifiedConfigManager):
        self.execution_history: List[JobExecutionRecord] = []
        self.startup_time = None
        self.config_manager = config_manager
        self.bot_config = self.config_manager.get_telegram_config()
        self.telegram_enabled = self.bot_config.enabled
        self.bot = TelegramBot() if self.telegram_enabled else None

        # 从配置管理器加载监控配置
        monitor_config = self.config_manager.get_monitor_config()
        self.max_history_size = monitor_config.max_history_size
        self.alert_thresholds = monitor_config.alert_thresholds
        self.startup_delay = monitor_config.startup_delay
        self.min_wait_time = monitor_config.min_wait_time

    async def initialize(self):
        """初始化监控器"""
        monitor_logger.info("[SchedulerMonitor] Initializing scheduler monitor...")

        self.startup_time = get_shanghai_time()

        # 智能等待系统完全启动
        await self._wait_for_scheduler_ready()

        # 检查初始状态并发送通知
        await self._send_initial_status()

        await self._start_monitoring()

    async def _wait_for_scheduler_ready(self):
        """智能等待调度器准备就绪"""
        monitor_logger.info(f"[SchedulerMonitor] Waiting at least {self.min_wait_time}s (max {self.startup_delay}s) for scheduler to be ready...")

        # 先确保最小等待时间
        await asyncio.sleep(self.min_wait_time)
        waited_time = self.min_wait_time

        # 然后智能检查调度器状态
        max_wait_time = self.startup_delay
        check_interval = 0.5  # 每0.5秒检查一次，更及时

        while waited_time < max_wait_time:
            # 检查调度器是否正在运行
            if task_scheduler.scheduler.running:
                monitor_logger.info(f"[SchedulerMonitor] Scheduler is ready after {waited_time:.1f} seconds")
                return

            # 等待一小段时间再检查
            await asyncio.sleep(check_interval)
            waited_time += check_interval

        # 如果超时了，记录警告但继续执行
        monitor_logger.warning(f"[SchedulerMonitor] Scheduler not ready after {max_wait_time} seconds, proceeding anyway")

    async def _start_monitoring(self):
        """启动监控任务"""
        asyncio.create_task(self._monitor_loop())

    async def _send_initial_status(self):
        """发送初始状态通知"""
        try:
            # 检查调度器状态
            if not task_scheduler.scheduler.running:
                await self._send_alert("调度器启动失败！请检查系统配置")
                return

            # 获取任务状态
            jobs_status = task_scheduler.get_all_jobs_status()
            if 'error' in jobs_status:
                await self._send_alert(f"获取任务状态失败: {jobs_status['error']}")
                return

            # 发送成功通知
            total_jobs = jobs_status.get('total_jobs', 0)
            jobs_list = jobs_status.get('jobs', {})

            # 过滤掉None值并计算统计信息
            valid_jobs = [j for j in jobs_list.values() if j is not None]
            running_jobs = len([j for j in valid_jobs if j.get('status') == 'running'])
            disabled_jobs = len([j for j in valid_jobs if not j.get('enabled', True)])

            await self._send_success_notification(
                f"调度器监控已启动\n"
                f"总任务数: {total_jobs}\n"
                f"运行中: {running_jobs}\n"
                f"已禁用: {disabled_jobs}"
            )

        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to send initial status: {e}")

    async def _send_success_notification(self, message: str):
        """发送成功通知"""
        monitor_logger.info(f"[SchedulerMonitor] Success: {message}")

        if self.telegram_enabled:
            try:
                await self.bot.send_scheduler_notification(message, "success")
            except Exception as e:
                monitor_logger.error(f"[SchedulerMonitor] Failed to send success notification via Telegram: {e}")

    async def _monitor_loop(self):
        """监控循环"""
        while True:
            try:
                await self._check_scheduler_health()
                await self._check_long_running_jobs()
                await self._cleanup_old_records()
                await asyncio.sleep(60)  # 每分钟检查一次
            except Exception as e:
                monitor_logger.error(f"[SchedulerMonitor] Monitor loop error: {e}")
                await asyncio.sleep(300)  # 错误时等待5分钟

    async def record_job_start(self, job_id: str) -> JobExecutionRecord:
        """记录任务开始"""
        record = JobExecutionRecord(
            job_id=job_id,
            start_time=get_shanghai_time()
        )

        self.execution_history.append(record)

        # 限制历史记录大小
        if len(self.execution_history) > self.max_history_size:
            self.execution_history = self.execution_history[-self.max_history_size:]

        monitor_logger.debug(f"[SchedulerMonitor] Job {job_id} started at {record.start_time}")
        return record

    async def record_job_completion(self, record: JobExecutionRecord, result: Dict[str, Any] = None):
        """记录任务完成"""
        record.end_time = get_shanghai_time()
        record.status = "completed"
        record.duration = (record.end_time - record.start_time).total_seconds()
        record.result = result

        monitor_logger.info(f"[SchedulerMonitor] Job {record.job_id} completed in {record.duration:.2f}s")

        # 检查执行时间
        if record.duration > self.alert_thresholds['max_execution_time']:
            await self._send_alert(f"任务 {record.job_id} 执行时间过长: {record.duration:.2f}s")

    async def record_job_failure(self, record: JobExecutionRecord, error_message: str):
        """记录任务失败"""
        record.end_time = get_shanghai_time()
        record.status = "failed"
        record.duration = (record.end_time - record.start_time).total_seconds()
        record.error_message = error_message

        monitor_logger.error(f"[SchedulerMonitor] Job {record.job_id} failed after {record.duration:.2f}s: {error_message}")

        # 检查连续失败
        await self._check_consecutive_failures(record.job_id)

    async def _check_scheduler_health(self):
        """检查调度器健康状态"""
        try:
            # 检查调度器是否运行
            if not task_scheduler.scheduler.running:
                # 如果在启动后的5分钟内，只记录日志不发送警告
                if self.startup_time and (get_shanghai_time() - self.startup_time).total_seconds() < 300:
                    monitor_logger.warning("[SchedulerMonitor] Scheduler not running (startup grace period)")
                    return
                else:
                    await self._send_alert("调度器未运行！")
                    return

            # 检查任务状态
            jobs_status = task_scheduler.get_all_jobs_status()
            if 'error' in jobs_status:
                await self._send_alert(f"获取任务状态失败: {jobs_status['error']}")
                return

            # 检查任务执行情况
            await self._analyze_job_execution_stats()

        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Health check failed: {e}")

    async def _analyze_job_execution_stats(self):
        """分析任务执行统计"""
        try:
            # 分析最近24小时的执行情况
            since = get_shanghai_time() - timedelta(hours=24)
            recent_executions = [
                record for record in self.execution_history
                if record.start_time >= since
            ]

            if not recent_executions:
                return

            # 计算失败率
            failed_count = sum(1 for r in recent_executions if r.status == "failed")
            total_count = len(recent_executions)
            failure_rate = failed_count / total_count if total_count > 0 else 0

            if failure_rate > self.alert_thresholds['max_failure_rate']:
                await self._send_alert(f"任务失败率过高: {failure_rate:.2%} ({failed_count}/{total_count})")

            # 检查长时间运行的任务
            long_running = [
                r for r in recent_executions
                if r.duration and r.duration > self.alert_thresholds['max_execution_time']
            ]

            if long_running:
                job_names = [r.job_id for r in long_running]
                await self._send_alert(f"检测到长时间运行的任务: {', '.join(job_names)}")

        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to analyze execution stats: {e}")

    async def _check_long_running_jobs(self):
        """检查并告警长时间运行的任务"""
        try:
            now = get_shanghai_time()
            # 筛选出仍在运行中的任务记录
            running_records = [
                r for r in self.execution_history if r.status == "running"
            ]

            for record in running_records:
                duration = (now - record.start_time).total_seconds()
                # 使用配置中的超时阈值
                if duration > self.alert_thresholds.get('max_execution_time', 300):
                    # 为避免重复告警，可以添加一个标记，这里简化处理
                    await self._send_alert(
                        f"任务 {record.job_id} 已运行超过 {duration/60:.1f} 分钟，可能已卡住！"
                    )
        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to check long running jobs: {e}")

    async def _check_consecutive_failures(self, job_id: str):
        """检查连续失败"""
        try:
            # 获取最近该任务的执行记录
            recent_records = [
                r for r in self.execution_history[-20:]  # 最近20条记录
                if r.job_id == job_id
            ]

            if len(recent_records) < self.alert_thresholds['max_consecutive_failures']:
                return

            # 检查是否连续失败
            consecutive_failures = 0
            for record in reversed(recent_records):
                if record.status == "failed":
                    consecutive_failures += 1
                else:
                    break

            if consecutive_failures >= self.alert_thresholds['max_consecutive_failures']:
                await self._send_alert(f"任务 {job_id} 连续失败 {consecutive_failures} 次！")

        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to check consecutive failures: {e}")

    async def _cleanup_old_records(self):
        """清理旧记录"""
        try:
            # 保留最近7天的记录
            cutoff_time = get_shanghai_time() - timedelta(days=7)
            self.execution_history = [
                record for record in self.execution_history
                if record.start_time >= cutoff_time
            ]

        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to cleanup old records: {e}")

    async def _send_alert(self, message: str):
        """发送警报"""
        monitor_logger.warning(f"[SchedulerMonitor] Alert: {message}")

        if self.telegram_enabled:
            try:
                await self.bot.send_scheduler_notification(message, "error")
            except Exception as e:
                monitor_logger.error(f"[SchedulerMonitor] Failed to send alert via Telegram: {e}")

    def get_execution_stats(self) -> Dict[str, Any]:
        """获取执行统计信息"""
        try:
            # 计算不同时间段的统计
            now = get_shanghai_time()
            periods = {
                '1h': timedelta(hours=1),
                '24h': timedelta(hours=24),
                '7d': timedelta(days=7),
                '30d': timedelta(days=30)
            }

            stats = {}
            for period_name, period_delta in periods.items():
                since = now - period_delta
                period_records = [
                    r for r in self.execution_history
                    if r.start_time >= since
                ]

                if period_records:
                    total_count = len(period_records)
                    failed_count = sum(1 for r in period_records if r.status == "failed")
                    completed_count = sum(1 for r in period_records if r.status == "completed")
                    avg_duration = sum(r.duration for r in period_records if r.duration) / max(completed_count, 1)

                    stats[period_name] = {
                        'total_executions': total_count,
                        'completed_executions': completed_count,
                        'failed_executions': failed_count,
                        'success_rate': (completed_count / total_count * 100) if total_count > 0 else 0,
                        'average_duration': avg_duration
                    }
                else:
                    stats[period_name] = {
                        'total_executions': 0,
                        'completed_executions': 0,
                        'failed_executions': 0,
                        'success_rate': 0,
                        'average_duration': 0
                    }

            return {
                'current_time': now,
                'monitoring': {
                    'total_records': len(self.execution_history),
                    'max_history_size': self.max_history_size,
                    'alert_thresholds': self.alert_thresholds
                },
                'execution_stats': stats
            }

        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to get execution stats: {e}")
            return {'error': str(e)}

    def get_recent_executions(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取最近的执行记录"""
        try:
            recent_records = self.execution_history[-limit:]
            return [
                {
                    'job_id': record.job_id,
                    'start_time': record.start_time,
                    'end_time': record.end_time,
                    'status': record.status,
                    'duration': record.duration,
                    'error_message': record.error_message
                }
                for record in recent_records
            ]
        except Exception as e:
            monitor_logger.error(f"[SchedulerMonitor] Failed to get recent executions: {e}")
            return []

    async def execute_job_with_monitoring(self, job_id: str, func, *args, **kwargs):
        """带监控的任务执行包装器"""
        record = await self.record_job_start(job_id)

        try:
            result = await func(*args, **kwargs)
            await self.record_job_completion(record, {'result': 'success'})
            return result

        except Exception as e:
            await self.record_job_failure(record, str(e))
            raise