"""
Task scheduler for the quote system.
Uses APScheduler to manage and execute scheduled tasks.
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from builtins import TimeoutError as BuiltinTimeoutError

# Scheduler can be imported by operational scripts without going through
# main.py; install the patch before importing project utilities.
from proxy_patch_bootstrap import install_akshare_proxy_patch as _install_akshare_proxy_patch

_install_akshare_proxy_patch(required=False)

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_MAX_INSTANCES

from utils import scheduler_logger, config_manager, TelegramBot
from utils.singleton import singleton

from .tasks import scheduled_tasks
from .job_config import JobConfigManager, job_config_manager
from .dependencies import SchedulerDependencyExecutor


_SCHEDULER_ALREADY_TRACKED_PARAM = "_scheduler_already_tracked"


@singleton
class TaskScheduler:
    """任务调度器"""

    def __init__(self):
        self.config = config_manager
        self.scheduler = AsyncIOScheduler()
        self.jobs: Dict[str, Any] = {}
        self.running_tasks: Dict[str, Dict[str, datetime]] = {}
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
            dependency_errors = self.job_config_manager.validate_dependency_configs()
            if dependency_errors:
                message = "; ".join(dependency_errors)
                scheduler_logger.error("[Scheduler] Invalid dependency configuration: %s", message)
                raise ValueError(f"Invalid scheduler dependency configuration: {message}")
            self.dependency_executor = SchedulerDependencyExecutor(
                job_configs=self.job_configs,
                raw_job_runner=self._run_configured_task_raw,
                logger=scheduler_logger,
            )

            # 初始化任务
            await scheduled_tasks.initialize()

            # 设置事件监听
            self.scheduler.add_listener(self._job_error_listener, EVENT_JOB_ERROR)
            self.scheduler.add_listener(self._job_missed_listener, EVENT_JOB_MISSED)
            self.scheduler.add_listener(self._job_max_instances_listener, EVENT_JOB_MAX_INSTANCES)

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
                    if getattr(job_config, 'manual_only', False):
                        scheduler_logger.info(
                            f"[Scheduler] Job '{job_id}' is manual-only, skipping automatic scheduling."
                        )
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
            job_id = job_config.job_id if job_config else getattr(func, '__name__', 'unknown')
            run_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            start_time = datetime.now()
            if job_id not in self.running_tasks:
                self.running_tasks[job_id] = {}
            self.running_tasks[job_id][run_id] = start_time
            scheduler_logger.info(
                f"[Scheduler] Task {job_id} started (run_id={run_id})"
            )
            try:
                # ★ 任务启动前 Telegram 通知
                if job_config and getattr(job_config, 'pre_run_notify', False):
                    try:
                        bot = TelegramBot()
                        await bot.send_task_notification(
                            f"开始执行...\n\n📋 任务: {job_config.description}",
                            task_name=job_id,
                            level="info"
                        )
                    except Exception as notify_err:
                        scheduler_logger.warning(
                            f"[Scheduler] 发送任务启动通知失败: {job_id}, {notify_err}"
                        )

                executor = getattr(self, "dependency_executor", None)
                if executor is None:
                    self.dependency_executor = SchedulerDependencyExecutor(
                        job_configs=self.job_configs,
                        raw_job_runner=self._run_configured_task_raw,
                        logger=scheduler_logger,
                    )
                    executor = self.dependency_executor
                dependency_result = await executor.run_job(
                    job_id,
                    {
                        **dict(parameters or {}),
                        _SCHEDULER_ALREADY_TRACKED_PARAM: True,
                    },
                    include_dependencies=True,
                )
                self._log_dependency_results(job_id, dependency_result.get("dependency_results"))
                await self._send_dependency_report(
                    job_id,
                    dependency_result.get("dependency_results"),
                    job_config,
                )
                result = bool(dependency_result.get("success"))
                duration = (datetime.now() - start_time).total_seconds()
                scheduler_logger.info(
                    f"[Scheduler] Task {job_id} completed in {duration:.1f}s (run_id={run_id})"
                )
                return result
            except asyncio.TimeoutError:
                duration = (datetime.now() - start_time).total_seconds()
                timeout_msg = (
                    f"[Scheduler] Task {job_id} timed out after {duration:.1f}s "
                    f"(run_id={run_id})"
                )
                scheduler_logger.error(timeout_msg)
                raise
            except (asyncio.CancelledError, KeyboardInterrupt):
                # 对于取消或手动中断，直接重新抛出，不记录为错误
                raise
            except BaseException as e:
                # 捕获所有其他异常，包括 Exception 和其他系统级错误
                duration = (datetime.now() - start_time).total_seconds()
                scheduler_logger.error(
                    f"[Scheduler] Task {job_id} failed after {duration:.1f}s "
                    f"(run_id={run_id}): {e}"
                )
                raise  # 重新抛出，以便APScheduler的错误监听器可以捕获
            finally:
                if job_id in self.running_tasks:
                    self.running_tasks[job_id].pop(run_id, None)
                    if not self.running_tasks[job_id]:
                        self.running_tasks.pop(job_id, None)

        return parameterized_task

    async def _run_configured_task_raw(
        self,
        job_id: str,
        parameters: Dict[str, Any],
        include_dependencies: bool = False,
    ) -> Any:
        """Run a single configured task without expanding dependencies."""
        if include_dependencies:
            executor = getattr(self, "dependency_executor", None)
            if executor is None:
                self.dependency_executor = SchedulerDependencyExecutor(
                    job_configs=self.job_configs,
                    raw_job_runner=self._run_configured_task_raw,
                    logger=scheduler_logger,
                )
                executor = self.dependency_executor
            return await executor.run_job(
                job_id,
                parameters,
                include_dependencies=True,
            )
        job_config = self.job_configs.get(job_id)
        task_func = getattr(scheduled_tasks, job_id, None)
        if task_func is None:
            raise ValueError(f"Task function not found: {job_id}")
        all_parameters = dict(parameters or {})
        already_tracked = bool(all_parameters.pop(_SCHEDULER_ALREADY_TRACKED_PARAM, False))
        all_parameters["job_config"] = job_config
        max_runtime_seconds = all_parameters.pop("max_runtime_seconds", None)
        for metadata_key in ("note", "operator_note", "comment", "comments"):
            all_parameters.pop(metadata_key, None)
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        tracked_here = False
        if not already_tracked:
            max_instances = int(getattr(job_config, "max_instances", 1) or 1)
            active_runs = self.running_tasks.get(job_id, {})
            if len(active_runs) >= max_instances:
                scheduler_logger.warning(
                    "[Scheduler] Raw task %s skipped because max_instances=%s is already active",
                    job_id,
                    max_instances,
                )
                return False
            if job_id not in self.running_tasks:
                self.running_tasks[job_id] = {}
            self.running_tasks[job_id][run_id] = datetime.now()
            tracked_here = True
        try:
            if max_runtime_seconds:
                return await asyncio.wait_for(
                    task_func(**all_parameters),
                    timeout=max_runtime_seconds,
                )
            return await task_func(**all_parameters)
        finally:
            if tracked_here and job_id in self.running_tasks:
                self.running_tasks[job_id].pop(run_id, None)
                if not self.running_tasks[job_id]:
                    self.running_tasks.pop(job_id, None)

    def _log_dependency_results(
        self,
        job_id: str,
        dependency_results: Optional[Dict[str, Any]],
    ) -> None:
        if not isinstance(dependency_results, dict):
            return
        for phase, groups in dependency_results.items():
            for group in groups or []:
                scheduler_logger.info(
                    "[Scheduler] Dependency group result: parent=%s phase=%s group=%s mode=%s status=%s",
                    job_id,
                    phase,
                    group.get("group_id"),
                    group.get("mode"),
                    group.get("status"),
                )
                for node in group.get("nodes") or []:
                    scheduler_logger.info(
                        "[Scheduler] Dependency node result: parent=%s phase=%s group=%s job=%s status=%s elapsed=%.1fs error=%s",
                        job_id,
                        phase,
                        group.get("group_id"),
                        node.get("job_id"),
                        node.get("status"),
                        float(node.get("elapsed_seconds") or 0),
                        node.get("error"),
                    )

    async def _send_dependency_report(
        self,
        job_id: str,
        dependency_results: Optional[Dict[str, Any]],
        job_config=None,
    ) -> None:
        """Send a concise dependency execution summary for configured job reports."""
        if not getattr(job_config, "report", False):
            return
        if not isinstance(dependency_results, dict):
            return
        lines = []
        for phase, groups in dependency_results.items():
            for group in groups or []:
                lines.append(
                    f"- {phase}/{group.get('group_id')} [{group.get('mode')}]: {group.get('status')}"
                )
                for node in group.get("nodes") or []:
                    summary = node.get("summary") or {}
                    counters = ", ".join(f"{key}={value}" for key, value in summary.items())
                    inherited = node.get("inherited_parameters") or {}
                    inherited_text = ", ".join(f"{key}={value}" for key, value in inherited.items()) or "none"
                    error = node.get("error")
                    detail = (
                        f"  - {node.get('job_id')}: {node.get('status')} "
                        f"({float(node.get('elapsed_seconds') or 0):.1f}s, inherit={inherited_text}"
                    )
                    if counters:
                        detail += f", {counters}"
                    if error:
                        detail += f", error={error}"
                    detail += ")"
                    lines.append(detail)
        if not lines:
            return
        try:
            bot = TelegramBot()
            await bot.send_task_notification(
                "配置化前后置任务执行结果:\n\n" + "\n".join(lines),
                task_name=job_id,
                level="info",
            )
        except Exception as notify_err:
            scheduler_logger.warning(
                "[Scheduler] Failed to send dependency report for %s: %s",
                job_id,
                notify_err,
            )

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
                # 使用与 get_job_status 一致的字段名
                'next_run_time': getattr(job, 'next_run_time', None)
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
        exception_message = self._format_job_exception(exception)

        scheduler_logger.error(f"[Scheduler] Job {job_id} failed at {scheduled_time}: {exception_message}")

        try:
            bot = TelegramBot()
            asyncio.create_task(
                bot.send_scheduler_notification(
                    f"定时任务 {job_id} 执行失败: {exception_message}",
                    level='error',
                )
            )
        except Exception:
            scheduler_logger.error("[Scheduler] Failed to send error notification")

    @staticmethod
    def _format_job_exception(exception) -> str:
        if isinstance(exception, (asyncio.TimeoutError, BuiltinTimeoutError)):
            return "TimeoutError: task exceeded max_runtime_seconds"
        message = str(exception).strip()
        if message:
            return message
        return type(exception).__name__

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

    def _job_max_instances_listener(self, event):
        """任务并发实例达到上限监听器"""
        job_id = event.job_id if hasattr(event, 'job_id') else 'unknown'
        scheduled_time = event.scheduled_run_time if hasattr(event, 'scheduled_run_time') else None
        running_info = self.running_tasks.get(job_id, {})
        durations = []
        now = datetime.now()
        for started_at in running_info.values():
            durations.append((now - started_at).total_seconds())
        longest = f"{max(durations):.1f}s" if durations else "unknown"
        scheduler_logger.warning(
            f"[Scheduler] Job {job_id} skipped due to max instances "
            f"at {scheduled_time}, running_for={longest}"
        )


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

    async def execute_job_direct(
        self,
        job_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        include_dependencies: bool = True,
    ) -> bool:
        """Execute a configured job immediately, including manual-only jobs."""
        try:
            job_config = self.job_config_manager.get_job_config(job_id)
            if job_config is None:
                scheduler_logger.warning("[Scheduler] Job config not found for direct execution: %s", job_id)
                return False
            params = dict(job_config.parameters or {})
            params.update(dict(parameters or {}))
            executor = getattr(self, "dependency_executor", None)
            if executor is None:
                self.job_configs = self.job_config_manager.get_all_job_configs()
                self.dependency_executor = SchedulerDependencyExecutor(
                    job_configs=self.job_configs,
                    raw_job_runner=self._run_configured_task_raw,
                    logger=scheduler_logger,
                )
                executor = self.dependency_executor
            dependency_result = await executor.run_job(
                job_id,
                params,
                include_dependencies=include_dependencies,
            )
            self._log_dependency_results(job_id, dependency_result.get("dependency_results"))
            await self._send_dependency_report(
                job_id,
                dependency_result.get("dependency_results"),
                job_config,
            )
            return bool(dependency_result.get("success"))
        except Exception as e:
            scheduler_logger.error(f"[Scheduler] Failed to execute job directly {job_id}: {e}")
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
