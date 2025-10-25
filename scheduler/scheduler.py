"""
Task scheduler for the quote system.
Uses APScheduler to manage and execute scheduled tasks.
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED

from utils import scheduler_logger, config_manager, TelegramBot
from utils.singleton import singleton

from .tasks import scheduled_tasks
from .job_config import JobConfigManager, job_config_manager


@singleton
class TaskScheduler:
    """任务调度器"""

    def __init__(self):
        self.config = config_manager
        self.scheduler = AsyncIOScheduler()
        self.jobs: Dict[str, Any] = {}
        self.enabled = self.config.get_nested('scheduler_config.enabled', True)
        # 创建 JobConfigManager 实例，由 TaskScheduler 统一管理
        self.job_config_manager = JobConfigManager(self.config)
        
        # 将此实例注入到 job_config 模块，作为全局单例使用，以解决循环依赖问题
        from . import job_config
        job_config.job_config_manager = self.job_config_manager

    async def initialize(self):
        """初始化调度器"""
        if not self.enabled:
            scheduler_logger.info("[Scheduler] Scheduler disabled, skipping initialization")
            return

        try:
            scheduler_logger.info("[Scheduler] Initializing task scheduler...")

            # 初始化任务配置管理器
            self.job_configs = self.job_config_manager.load_job_configs()

            # 初始化任务
            await scheduled_tasks.initialize()

            # 设置事件监听
            self.scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
            self.scheduler.add_listener(self._job_missed_listener, EVENT_JOB_MISSED)

            # 配置任务
            await self._setup_jobs_from_config()

            # 启动调度器
            self.scheduler.start()
            scheduler_logger.info("[Scheduler] Task scheduler started successfully")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to initialize scheduler: {e}")
            raise

    async def _setup_jobs_from_config(self):
        """从配置文件设置定时任务"""
        try:
            scheduled_jobs = 0

            for job_id, job_config in self.job_configs.items():
                try:
                    # 只调度已启用的任务
                    if not job_config.enabled:
                        scheduler_logger.info(f"[Scheduler] Job '{job_id}' is disabled, skipping scheduling.")
                        continue

                    # 获取任务函数
                    task_func = getattr(scheduled_tasks, job_id, None)
                    if not task_func:
                        scheduler_logger.error(f"[Scheduler] Task function not found: {job_id}")
                        continue

                    # 创建带参数的任务函数
                    # 总是创建参数化任务，以便将 job_config 注入
                    task_func = self._create_parameterized_task(task_func, job_config.parameters, job_config)

                    # 添加任务
                    success = await self._add_job_from_config(job_config, task_func)
                    if success:
                        scheduled_jobs += 1

                except Exception as e:
                    scheduler_logger.error(f"[Scheduler] Failed to setup job {job_id}: {e}")
                    continue

            scheduler_logger.info(f"[Scheduler] Configured {scheduled_jobs} scheduled jobs from config")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to setup jobs from config: {e}")
            raise

    def _create_parameterized_task(self, func, parameters: Dict[str, Any], job_config=None):
        """创建带参数的任务函数
            func: 任务函数（从 scheduled_tasks 类实例中获取的以 job_id 为名称的函数，比如：daily_data_update）
            parameters: 任务参数（在 config.json 中配置的任务参数，在每个任务的 parameters 字段中）
            job_config: 任务配置（可选，在 config.json 中任务的所有配置信息，传递给函数后可以获取除 parameters 以为的的其他配置，比如是否启用 report 等）
            返回：带参数的任务函数，传递给 scheduler.add_job() 的 func 参数，这个函数封装了所有参数
            这个函数是一个函数工厂，生成并返回一个新的函数
        """
        async def parameterized_task():
            try:
                # 将 job_config 添加到参数中，如果它存在的话
                all_parameters = {**parameters, 'job_config': job_config} if job_config else parameters.copy()

                # 调用原函数并传入参数
                # aiojobs scheduler 会自动处理协程函数的调用
                return await func(**all_parameters)
            except (asyncio.CancelledError, KeyboardInterrupt):
                # 对于取消或手动中断，直接重新抛出，不记录为错误
                raise
            except BaseException as e:
                # 捕获所有其他异常，包括 Exception 和其他系统级错误
                scheduler_logger.error(f"[Scheduler] Task execution failed: {e}")
                raise  # 重新抛出，以便APScheduler的错误监听器可以捕获

        return parameterized_task

    async def _add_job_from_config(self, job_config, func) -> bool:
        """从配置添加任务"""
        try:
            loop = asyncio.get_running_loop()
            job = await loop.run_in_executor(
                None,
                lambda: self.scheduler.add_job(
                    func,
                    trigger=job_config.trigger,
                    id=job_config.job_id,
                    replace_existing=True,
                    max_instances=job_config.max_instances,
                    misfire_grace_time=job_config.misfire_grace_time,
                    coalesce=job_config.coalesce
                )
            )

            self.jobs[job_config.job_id] = {
                'job': job,
                'description': job_config.description,
                'config': job_config,
                # job.next_run_time might not be immediately available
                'next_run': getattr(job, 'next_run_time', None)
            }

            scheduler_logger.info(f"[Scheduler] Added job: {job_config.job_id} - {job_config.description}")

            # 记录下次运行时间
            next_run = self.job_config_manager.get_next_run_time(job_config.job_id)
            if next_run:
                scheduler_logger.debug(f"[Scheduler] Job {job_config.job_id} next run: {next_run}")

            return True

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to add job {job_config.job_id}: {e}")
            return False

    def _job_error_listener(self, event):
        """任务错误监听器"""
        job_id = event.job_id if hasattr(event, 'job_id') else 'unknown'
        exception = event.exception if hasattr(event, 'exception') else 'Unknown error'
        scheduled_time = event.scheduled_run_time if hasattr(event, 'scheduled_run_time') else None

        scheduler_logger.error(f"[Scheduler] Job {job_id} failed at {scheduled_time}: {exception}")

        try:
            bot = TelegramBot()
            asyncio.create_task(
                bot.send_scheduler_notification(f"定时任务 {job_id} 执行失败: {str(exception)}", level='error')
            )
        except Exception:
            scheduler_logger.error("[Scheduler] Failed to send error notification")

    def _job_missed_listener(self, event):
        """任务错过监听器"""
        job_id = event.job_id if hasattr(event, 'job_id') else 'unknown'
        scheduled_time = event.scheduled_run_time if hasattr(event, 'scheduled_run_time') else None

        scheduler_logger.warning(f"[Scheduler] Job {job_id} missed at {scheduled_time}")
        
        try:
            bot = TelegramBot()
            asyncio.create_task(
                bot.send_scheduler_notification(f"定时任务 {job_id} 错过执行", level='warning')
            )
        except Exception:
            scheduler_logger.error("[Scheduler] Failed to send missed notification")


    async def run_job_now(self, job_id: str):
        """立即运行指定任务"""
        try:
            if job_id in self.jobs:
                # 直接修改任务的下次运行时间为“现在”，使其立即触发
                # 这是一个线程安全的操作，但为了遵循异步最佳实践，仍使用 run_in_executor
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self.scheduler.modify_job(job_id, next_run_time=datetime.now())
                )
                scheduler_logger.info(f"[Scheduler] Job {job_id} scheduled for immediate execution")
                return True
            else:
                scheduler_logger.warning(f"[Scheduler] Job {job_id} not found")
                return False

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to run job {job_id}: {e}")
            return False

    async def pause_job(self, job_id: str) -> bool:
        """暂停任务"""
        try:
            if job_id in self.jobs:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.scheduler.pause_job, job_id)
                scheduler_logger.info(f"[Scheduler] Job {job_id} paused")
                return True
            else:
                scheduler_logger.warning(f"[Scheduler] Job {job_id} not found")
                return False
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to pause job {job_id}: {e}")
            return False

    async def resume_job(self, job_id: str) -> bool:
        """恢复任务"""
        try:
            if job_id in self.jobs:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.scheduler.resume_job, job_id)
                scheduler_logger.info(f"[Scheduler] Job {job_id} resumed")
                return True
            else:
                scheduler_logger.warning(f"[Scheduler] Job {job_id} not found")
                return False
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to resume job {job_id}: {e}")
            return False

    async def remove_job(self, job_id: str) -> bool:
        """移除任务"""
        try:
            if job_id in self.jobs:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.scheduler.remove_job, job_id)
                del self.jobs[job_id]
                scheduler_logger.info(f"[Scheduler] Job {job_id} removed")
                return True
            else:
                scheduler_logger.warning(f"[Scheduler] Job {job_id} not found")
                return False
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to remove job {job_id}: {e}")
            return False

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        try:
            if job_id not in self.jobs:
                return None

            job_info = self.jobs[job_id]
            job = job_info['job']
            job_config = job_info.get('config')

            # 获取next_run_time并确保时区信息正确
            next_run_time = getattr(job, 'next_run_time', None)
            if next_run_time is not None:
                # 确保时间对象有时区信息，如果没有则假设为UTC
                from datetime import timezone
                if next_run_time.tzinfo is None:
                    next_run_time = next_run_time.replace(tzinfo=timezone.utc)

            status = {
                'id': job_id,
                'name': getattr(job, 'name', job_id),
                'description': job_info['description'],
                'enabled': job_config.enabled if job_config else True,
                'next_run_time': next_run_time,  # 使用更明确的字段名
                'trigger': str(getattr(job, 'trigger', 'unknown')),
                'pending': getattr(job, 'pending', False),
                'running': getattr(job, 'running', False),
                'executions': getattr(job, 'executions', 0),
                'max_instances': getattr(job, 'max_instances', 1),
                'misfire_grace_time': getattr(job, 'misfire_grace_time', 300),
                'coalesce': getattr(job, 'coalesce', True)
            }

            # 添加配置参数
            if job_config:
                status['parameters'] = job_config.parameters

            return status

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to get job status for {job_id}: {e}")
            return None

    def get_all_jobs_status(self) -> Dict[str, Any]:
        """获取所有任务状态"""
        try:
            jobs_status = {}
            for job_id in self.jobs:
                jobs_status[job_id] = self.get_job_status(job_id)

            return {
                'scheduler_running': self.scheduler.running,
                'total_jobs': len(self.jobs),
                'jobs': jobs_status
            }

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to get all jobs status: {e}")
            return {'error': str(e)}

    async def load_jobs_from_config(self) -> None:
        """重新从配置文件加载任务配置"""
        try:
            scheduler_logger.info("[Scheduler] 重新加载任务配置...")

            # 1. 移除所有现有的任务
            scheduler_logger.info(f"[Scheduler] 移除现有任务，当前任务数: {len(self.jobs)}")
            jobs_to_remove = list(self.jobs.keys())
            for job_id in jobs_to_remove:
                try:
                    await self.remove_job(job_id)
                    scheduler_logger.debug(f"[Scheduler] 已移除任务: {job_id}")
                except Exception as e:
                    scheduler_logger.warning(f"[Scheduler] 移除任务失败 {job_id}: {e}")

            # 2. 重新加载任务配置
            scheduler_logger.info("[Scheduler] 重新加载任务配置管理器...")
            self.job_configs = self.job_config_manager.load_job_configs()

            # 3. 重新设置任务
            scheduler_logger.info("[Scheduler] 重新设置任务...")
            await self._setup_jobs_from_config()

            scheduler_logger.info(f"[Scheduler] 任务配置重载完成，新任务数: {len(self.jobs)}")

        except Exception as e:
            scheduler_logger.error(f"[Scheduler] 重载任务配置失败: {e}")
            import traceback
            scheduler_logger.error(f"[Scheduler] 错误堆栈: {traceback.format_exc()}")
            raise

    async def shutdown(self):
        """关闭调度器"""
        try:
            if self.scheduler.running:
                scheduler_logger.info("[Scheduler] Shutting down task scheduler (will wait for jobs to complete)...")
                loop = asyncio.get_running_loop()
                # shutdown(wait=True) 是一个阻塞操作，需要放入执行器
                await loop.run_in_executor(None, self.scheduler.shutdown, True)
                scheduler_logger.info("[Scheduler] Task scheduler shutdown completed")
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Error during shutdown: {e}")


# 全局调度器实例
task_scheduler = TaskScheduler()