"""
Telegram任务管理机器人核心逻辑
整合所有组件，提供统一的任务管理接口
"""

import asyncio
import os
import shlex
from typing import Optional, List, Dict, Any

from .handlers import TaskManagerHandlers
from .formatters import TaskManagerFormatters
from .keyboards import TaskManagerKeyboards
from utils import task_manager_logger




class TaskManagerBot:
    """Telegram任务管理机器人主类"""

    def __init__(self, telegram_bot, task_scheduler, job_config_manager,
                 scheduler_monitor, config_manager, logger):
        """
        初始化任务管理机器人

        Args:
            telegram_bot: TelegramBot实例
            task_scheduler: TaskScheduler实例
            job_config_manager: JobConfigManager实例
            scheduler_monitor: SchedulerMonitor实例
            config_manager: ConfigManager实例
            logger: 日志记录器
        """
        self.telegram_bot = telegram_bot
        self.task_scheduler = task_scheduler
        self.job_config_manager = job_config_manager
        self.scheduler_monitor = scheduler_monitor
        self.config_manager = config_manager
        self.logger = logger

        self.handlers = TaskManagerHandlers(self)
        self._initialized = False

    async def initialize(self) -> None:
        """初始化任务管理机器人"""
        try:
            self.logger.info("[TaskManagerBot] Initializing task manager bot...")

            # 注册事件处理器
            await self._register_handlers()

            self._initialized = True
            self.logger.info("[TaskManagerBot] Task manager bot initialized successfully")

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Failed to initialize task manager bot: {e}")
            raise

    async def _register_handlers(self) -> None:
        """注册事件处理器"""
        try:
            from telethon import events

            # 注册命令处理器
            self.telegram_bot.register_command_handler('/start', self.handlers.handle_start_command)
            self.telegram_bot.register_command_handler('/status', self.handlers.handle_status_command)
            self.telegram_bot.register_command_handler('/help', self.handlers.handle_help_command)
            self.telegram_bot.register_command_handler('/detail', self.handlers.handle_detail_command)
            self.telegram_bot.register_command_handler('/run', self.handlers.handle_run_command)
            self.telegram_bot.register_command_handler('/backfill', self.handlers.handle_backfill_command)
            self.telegram_bot.register_command_handler('/backfill_factors', self.handlers.handle_backfill_factors_command)
            self.telegram_bot.register_command_handler('/industry_standard_sync', self.handlers.handle_industry_standard_sync_command)
            self.telegram_bot.register_command_handler('/industry_standard_rebuild', self.handlers.handle_industry_standard_rebuild_command)
            self.telegram_bot.register_command_handler('/industry_index_analysis_sync', self.handlers.handle_industry_index_analysis_sync_command)
            self.telegram_bot.register_command_handler('/industry_index_analysis_backfill', self.handlers.handle_industry_index_analysis_backfill_command)
            self.telegram_bot.register_command_handler('/futures_calendar_backfill', self.handlers.handle_futures_calendar_backfill_command)
            self.telegram_bot.register_command_handler('/audit_factors', self.handlers.handle_audit_factors_command)
            self.telegram_bot.register_command_handler('/hkex_review', self.handlers.handle_hkex_review_command)
            self.telegram_bot.register_command_handler('/smart_fill_gaps', self.handlers.handle_smart_fill_gaps_command)
            self.telegram_bot.register_command_handler('/find_gap_and_repair', self.handlers.handle_find_gap_and_repair_command)
            self.telegram_bot.register_command_handler('/reload_config', self.handle_reload_config_command)
            self.telegram_bot.register_command_handler('/restart_system', self.handle_restart_system_command)

            # 注册回调查询处理器
            self.telegram_bot.register_callback_handler(self.handlers.handle_callback_query)

            self.logger.info("[TaskManagerBot] Event handlers registered successfully")

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Failed to register event handlers: {e}")
            # 不抛出异常，允许系统继续运行
            self.logger.warning("[TaskManagerBot] Task manager bot will not respond to commands")

    async def send_message(self, chat_id: int, text: str, keyboard: List = None,
                          parse_mode: str = None) -> Any:
        """发送消息的封装方法"""
        try:
            if keyboard:
                # 如果有键盘，直接使用
                return await self.telegram_bot.bot_thon.send_message(
                    entity=chat_id,
                    message=text,
                    parse_mode=parse_mode,
                    buttons=keyboard
                )
            else:
                return await self.telegram_bot.bot_thon.send_message(
                    entity=chat_id,
                    message=text,
                    parse_mode=parse_mode
                )

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Failed to send message to {chat_id}: {e}")
            raise

    async def edit_message(self, chat_id: int, message_id: int, text: str,
                          keyboard: List = None, parse_mode: str = None) -> Any:
        """编辑消息的封装方法"""
        try:
            if keyboard:
                # 如果有键盘，直接使用
                return await self.telegram_bot.bot_thon.edit_message(
                    entity=chat_id,
                    id=message_id,
                    text=text,
                    parse_mode=parse_mode,
                    buttons=keyboard
                )
            else:
                return await self.telegram_bot.bot_thon.edit_message(
                    entity=chat_id,
                    id=message_id,
                    text=text,
                    parse_mode=parse_mode
                )

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Failed to edit message {message_id} in {chat_id}: {e}")
            # 如果编辑失败，尝试发送新消息
            try:
                await self.send_message(chat_id, text, keyboard, parse_mode)
            except Exception as e2:
                self.logger.error(f"[TaskManagerBot] Failed to send new message after edit failure: {e2}")
                raise

    async def send_task_management_help(self, chat_id: int) -> None:
        """发送任务管理帮助信息"""
        help_message = (
            "🤖 *任务管理帮助*\n\n"
            "*可用命令:*\n"
            "• `/start` - 显示主菜单\n"
            "• `/status` - 查看任务状态\n"
            "• `/run <任务ID>` - 立即执行任务\n"
            "• `/run shareholder_shadow_sync` - 手工触发股东摘要全量刷新\n"
            "• `/run shareholder_reconciliation_sync` - 手工触发股东摘要周期复核与补足\n"
            "• `/run shareholder_incremental_sync` - 手工触发股东摘要每日增量检查\n"
            "• `/backfill <日期|开始日期 结束日期> [交易所...]` - 补充指定日期或日期范围的缺失数据\n"
            "• `/backfill_factors [交易所...] [missing|full]` - 回填复权因子\n"
            "• `/industry_standard_sync [force]` - 申万官方分类日更同步\n"
            "• `/industry_standard_rebuild [force] [drop_source_files]` - 申万官方分类全量重建\n"
            "• `/industry_index_analysis_sync [limit=N]` - 申万行业指数分析日频同步\n"
            "• `/industry_index_analysis_backfill start=YYYY-MM-DD end=YYYY-MM-DD [limit=N] [chunk=month|day|quarter|year|none]` - 申万行业指数分析历史回补\n"
            "• `/futures_calendar_backfill exchange=SHFE start=YYYY-MM-DD end=YYYY-MM-DD [dry_run|write] [max_days=N]` - 手工回填期货交易日历\n"
            "• `/smart_fill_gaps` - 智能补足大段数据缺口\n"
            "• `/find_gap_and_repair` - 精确逐日检测并修复缺口\n"
            "• `/reload_config` - 重载配置文件\n"
            "• `/restart_system confirm` - 重启 quote system 服务\n"
            "• `/help` - 查看帮助信息\n\n"
            "*功能说明:*\n"
            "• 📋 查看任务状态 - 查看所有任务的运行状态\n"
            "• 📝 任务详情 - 查看任务的详细信息和执行历史\n"
            "• 🚀 立即执行 - 手动触发任务执行\n"
            "• 📥 数据补充 - 补充缺失数据或因子\n"
            "• ✅ 启用任务 - 将任务加入调度器\n"
            "• 🔴 禁用任务 - 将任务从调度器移除\n"
            "• 🔄 重载配置 - 热重载配置文件无需重启\n\n"
            "*注意事项:*\n"
            "• 只有配置的管理员才能使用此功能\n"
            "• 启用/禁用任务会修改配置文件\n"
            "• 重载配置会立即生效，无需重启进程\n"
            "• 操作结果会通过消息反馈"
        )

        keyboard = TaskManagerKeyboards.back_menu()

        await self.send_message(
            chat_id,
            help_message,
            keyboard=keyboard,
            parse_mode='markdown'
        )

    async def send_system_status(self, chat_id: int) -> None:
        """发送系统状态信息"""
        try:
            # 获取调度器状态
            scheduler_status = self.task_scheduler.get_all_jobs_status()

            # 获取监控统计
            monitoring_stats = self.scheduler_monitor.get_execution_stats()

            # 格式化状态消息
            message = (
                "📊 *系统状态*\n\n"
                f"🔄 调度器状态: {'运行中' if scheduler_status.get('scheduler_running') else '已停止'}\n"
                f"📋 总任务数: {scheduler_status.get('total_jobs', 0)}\n"
                f"📈 监控记录数: {monitoring_stats.get('monitoring', {}).get('total_records', 0)}\n\n"
            )

            # 添加24小时执行统计
            stats_24h = monitoring_stats.get('execution_stats', {}).get('24h', {})
            if stats_24h:
                success_rate = stats_24h.get('success_rate', 0)
                avg_duration = stats_24h.get('average_duration', 0)
                message += (
                    f"📊 *24小时执行统计*\n"
                    f"• 总执行: {stats_24h.get('total_executions', 0)}次\n"
                    f"• 成功率: {success_rate:.1f}%\n"
                    f"• 平均耗时: {avg_duration:.1f}秒\n"
                )

            keyboard = TaskManagerKeyboards.back_menu()

            await self.send_message(
                chat_id,
                message,
                keyboard=keyboard,
                parse_mode='markdown'
            )

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Failed to send system status: {e}")
            error_message = TaskManagerFormatters.format_error_message("scheduler_error", str(e))
            keyboard = TaskManagerKeyboards.back_menu()

            await self.send_message(
                chat_id,
                error_message,
                keyboard=keyboard,
                parse_mode='markdown'
            )

    async def handle_help_command(self, event) -> None:
        """处理帮助命令"""
        await self.send_task_management_help(event.chat_id)

    async def handle_system_status_command(self, event) -> None:
        """处理系统状态命令"""
        await self.send_system_status(event.chat_id)

    async def handle_reload_config_command(self, event) -> None:
        """处理配置重载命令"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        message_text = event.text if hasattr(event, 'text') else '/reload_config'

        # 详细日志记录
        self.logger.info(f"[TaskManagerBot] 收到命令: '{message_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")
        self.logger.debug(f"[TaskManagerBot] 处理配置重载请求，chat_id: {chat_id}")

        try:
            if not self.is_authorized(chat_id):
                await self.send_message(chat_id, "⛔ *未授权操作*\n\n当前 chat_id 不在管理员白名单中。", parse_mode='markdown')
                self.logger.warning(f"[TaskManagerBot] 拒绝未授权配置重载请求，chat_id: {chat_id}")
                return

            # 发送开始重载的消息
            start_message = "🔄 *正在重载配置...*\n\n正在重新读取配置文件并更新调度器设置..."
            await self.send_message(chat_id, start_message, parse_mode='markdown')

            # 执行配置重载
            success = await self.reload_scheduler_config()

            if success:
                success_message = (
                    "✅ *配置重载成功*\n\n"
                    "📋 *已更新的配置:*\n"
                    "• 任务启用/禁用状态\n"
                    "• 任务触发时间设置\n"
                    "• 任务参数配置\n"
                    "• 报告发送设置\n"
                    "• 报告模板和格式配置\n\n"
                    "💡 *提示: 所有配置修改已立即生效，无需重启进程*"
                )
                await self.send_message(chat_id, success_message, parse_mode='markdown')
                self.logger.info(f"[TaskManagerBot] 配置重载成功，chat_id: {chat_id}")
            else:
                error_message = (
                    "❌ *配置重载失败*\n\n"
                    "请检查配置文件格式是否正确，或稍后重试。\n\n"
                    "📋 *可能的原因:*\n"
                    "• 配置文件格式错误\n"
                    "• 任务配置无效\n"
                    "• 调度器内部错误"
                )
                await self.send_message(chat_id, error_message, parse_mode='markdown')
                self.logger.error(f"[TaskManagerBot] 配置重载失败，chat_id: {chat_id}")

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] 处理配置重载命令失败: {e}")
            error_message = (
                f"❌ *配置重载异常*\n\n"
                f"错误信息: `{str(e)}`\n\n"
                f"请稍后重试或联系管理员。"
            )
            await self.send_message(chat_id, error_message, parse_mode='markdown')

    async def handle_restart_system_command(self, event) -> None:
        """处理系统服务重启命令。"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        message_text = event.text if hasattr(event, 'text') else '/restart_system'

        self.logger.info(f"[TaskManagerBot] 收到命令: '{message_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")

        try:
            if not self.is_authorized(chat_id):
                await self.send_message(chat_id, "⛔ *未授权操作*\n\n当前 chat_id 不在管理员白名单中。", parse_mode='markdown')
                self.logger.warning(f"[TaskManagerBot] 拒绝未授权服务重启请求，chat_id: {chat_id}")
                return

            restart_cfg = self._get_service_restart_config()
            if not restart_cfg["enabled"]:
                await self.send_message(
                    chat_id,
                    "⚠️ *系统重启命令未启用*\n\n请先在 `telegram_config.ops.service_restart.enabled` 中显式启用。",
                    parse_mode='markdown',
                )
                return

            tokens = str(message_text or "").strip().split()
            if len(tokens) < 2 or tokens[1].lower() != "confirm":
                service_name = restart_cfg["service_name"]
                await self.send_message(
                    chat_id,
                    (
                        "⚠️ *确认重启系统服务*\n\n"
                        f"服务: `{service_name}`\n"
                        "该操作会短暂中断 API、调度器和 Telegram 交互。\n\n"
                        "如确认执行，请发送:\n"
                        "`/restart_system confirm`"
                    ),
                    parse_mode='markdown',
                )
                return

            running_tasks = self._get_running_task_summary()
            if running_tasks:
                sample_text = "\n".join(
                    f"• `{item['job_id']}`，运行中实例: `{item['run_count']}`"
                    for item in running_tasks[:10]
                )
                more_count = max(0, len(running_tasks) - 10)
                more_text = f"\n• ... 另有 `{more_count}` 个任务" if more_count else ""
                await self.send_message(
                    chat_id,
                    (
                        "⏳ *暂不重启系统服务*\n\n"
                        "当前仍有任务正在运行。为避免中断数据更新或写库，请等待任务完成后再发送 "
                        "`/restart_system confirm`。\n\n"
                        f"*运行中的任务:*\n{sample_text}{more_text}"
                    ),
                    parse_mode='markdown',
                )
                self.logger.warning(
                    "[TaskManagerBot] 拒绝系统重启请求，仍有运行中任务: %s",
                    [item["job_id"] for item in running_tasks],
                )
                return

            delay_seconds = restart_cfg["delay_seconds"]
            if restart_cfg["mode"] == "self_exit":
                command_display = f"self_exit(exit_code={restart_cfg['exit_code']})"
            else:
                command = self._build_service_restart_command(restart_cfg)
                command_display = " ".join(shlex.quote(part) for part in command)
            await self.send_message(
                chat_id,
                (
                    "🔄 *已提交系统服务重启请求*\n\n"
                    f"服务: `{restart_cfg['service_name']}`\n"
                    f"模式: `{restart_cfg['mode']}`\n"
                    f"延迟: `{delay_seconds}` 秒\n"
                    f"命令: `{command_display}`\n\n"
                    "后续 10-30 秒内 Telegram 交互可能短暂离线。"
                ),
                parse_mode='markdown',
            )
            if restart_cfg["mode"] == "self_exit":
                asyncio.create_task(
                    self._restart_service_by_self_exit(
                        delay_seconds=delay_seconds,
                        exit_code=restart_cfg["exit_code"],
                    )
                )
            else:
                asyncio.create_task(
                    self._restart_service_after_delay(
                        chat_id=chat_id,
                        command=command,
                        delay_seconds=delay_seconds,
                        timeout_seconds=restart_cfg["timeout_seconds"],
                    )
                )
        except Exception as e:
            self.logger.error(f"[TaskManagerBot] 处理系统重启命令失败: {e}")
            await self.send_message(
                chat_id,
                f"❌ *系统重启命令异常*\n\n错误信息: `{str(e)}`",
                parse_mode='markdown',
            )

    def _get_running_task_summary(self) -> List[Dict[str, Any]]:
        """Return currently running scheduler tasks tracked by TaskScheduler."""
        running_tasks = getattr(self.task_scheduler, "running_tasks", {}) or {}
        summary: List[Dict[str, Any]] = []
        for job_id, runs in sorted(running_tasks.items()):
            if not runs:
                continue
            try:
                run_count = len(runs)
            except TypeError:
                run_count = 1
            summary.append({
                "job_id": str(job_id),
                "run_count": run_count,
            })
        return summary

    def _get_service_restart_config(self) -> Dict[str, Any]:
        """读取并规范化 Telegram 服务重启配置。"""
        cfg = self.config_manager.get_nested('telegram_config.ops.service_restart', {}) or {}
        mode = str(cfg.get("mode") or "self_exit").strip().lower()
        if mode not in {"self_exit", "systemctl"}:
            raise ValueError("invalid service_restart.mode")
        service_name = str(cfg.get("service_name") or "quote-system.service").strip()
        if not service_name or "/" in service_name or service_name.startswith("-"):
            raise ValueError("invalid service_restart.service_name")
        systemctl_path = str(cfg.get("systemctl_path") or "systemctl").strip()
        if not systemctl_path or any(ch in systemctl_path for ch in [";", "&", "|", "\n", "\r"]):
            raise ValueError("invalid service_restart.systemctl_path")
        return {
            "enabled": bool(cfg.get("enabled", False)),
            "mode": mode,
            "service_name": service_name,
            "systemctl_path": systemctl_path,
            "use_sudo": bool(cfg.get("use_sudo", False)),
            "delay_seconds": max(0.0, float(cfg.get("delay_seconds", 2))),
            "timeout_seconds": max(1.0, float(cfg.get("timeout_seconds", 15))),
            "exit_code": max(1, int(cfg.get("exit_code", 1))),
        }

    @staticmethod
    def _build_service_restart_command(restart_cfg: Dict[str, Any]) -> List[str]:
        command = [
            str(restart_cfg["systemctl_path"]),
            "restart",
            str(restart_cfg["service_name"]),
        ]
        if restart_cfg.get("use_sudo"):
            command.insert(0, "sudo")
        return command

    async def _restart_service_after_delay(
        self,
        *,
        chat_id: int,
        command: List[str],
        delay_seconds: float,
        timeout_seconds: float,
    ) -> None:
        """延迟执行固定 systemd restart 命令。成功时当前进程通常会被 systemd 重启。"""
        await asyncio.sleep(delay_seconds)
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
            if process.returncode == 0:
                self.logger.info("[TaskManagerBot] 服务重启命令已成功返回")
                return
            output = (stderr or stdout or b"").decode("utf-8", errors="replace").strip()
            await self.send_message(
                chat_id,
                f"❌ *系统服务重启失败*\n\n退出码: `{process.returncode}`\n输出: `{output[:1000]}`",
                parse_mode='markdown',
            )
            self.logger.error(f"[TaskManagerBot] 服务重启命令失败: rc={process.returncode}, output={output}")
        except asyncio.TimeoutError:
            await self.send_message(
                chat_id,
                f"❌ *系统服务重启超时*\n\n命令在 `{timeout_seconds}` 秒内未返回。",
                parse_mode='markdown',
            )
            self.logger.error("[TaskManagerBot] 服务重启命令超时")
        except Exception as e:
            await self.send_message(
                chat_id,
                f"❌ *系统服务重启异常*\n\n错误信息: `{str(e)}`",
                parse_mode='markdown',
            )
            self.logger.error(f"[TaskManagerBot] 服务重启命令异常: {e}")

    async def _restart_service_by_self_exit(
        self,
        *,
        delay_seconds: float,
        exit_code: int,
    ) -> None:
        """Exit the current process so systemd Restart=on-failure can restart it."""
        await asyncio.sleep(delay_seconds)
        self.logger.warning(
            "[TaskManagerBot] 服务重启进入 self_exit 模式，当前进程即将退出，exit_code=%s",
            exit_code,
        )
        os._exit(exit_code)

    async def reload_scheduler_config(self) -> bool:
        """重载调度器配置"""
        try:
            self.logger.info("[TaskManagerBot] 开始重载调度器配置...")

            # 1. 重载配置管理器中的配置
            self.logger.info("[TaskManagerBot] 步骤1: 重载配置文件...")
            self.config_manager.reload_config()

            # 2. 刷新长生命周期服务中缓存的配置引用
            self.logger.info("[TaskManagerBot] 步骤2: 刷新运行时配置引用...")
            from data_manager import data_manager
            data_manager.refresh_runtime_config()

            # 3. 重载任务配置管理器
            self.logger.info("[TaskManagerBot] 步骤3: 重载任务配置...")
            self.job_config_manager.load_job_configs()

            # 4. 重新加载调度器中的任务
            self.logger.info("[TaskManagerBot] 步骤4: 重新加载调度器任务...")
            await self.task_scheduler.load_jobs_from_config()

            # 5. 重载报告配置
            self.logger.info("[TaskManagerBot] 步骤5: 重载报告配置...")
            from utils import report
            report.reload_report_config()

            self.logger.info("[TaskManagerBot] 调度器配置重载完成")
            return True

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] 重载调度器配置失败: {e}")
            import traceback
            self.logger.error(f"[TaskManagerBot] 错误堆栈: {traceback.format_exc()}")
            return False

    def is_authorized(self, chat_id: int) -> bool:
        """检查用户是否有权限使用任务管理功能"""
        try:
            # 从配置获取授权的chat_id列表
            authorized_chats = self.config_manager.get_nested('telegram_config.chat_id', [])
            authorized_normalized = {str(item).strip() for item in authorized_chats if str(item).strip()}

            # 支持不同格式的chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    return chat_id in authorized_normalized
                else:
                    return str(int(chat_id)) in authorized_normalized
            else:
                return str(int(chat_id)) in authorized_normalized

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Error checking authorization for {chat_id}: {e}")
            return False

    async def cleanup(self) -> None:
        """清理资源"""
        try:
            if self._initialized:
                # 清理处理器状态
                if hasattr(self.handlers, 'cleanup_user_state'):
                    # 清理所有用户状态
                    user_states = getattr(self.handlers, 'user_states', {})
                    for chat_id in list(user_states.keys()):
                        self.handlers.cleanup_user_state(chat_id)

                self._initialized = False
                self.logger.info("[TaskManagerBot] Task manager bot cleanup completed")

        except Exception as e:
            self.logger.error(f"[TaskManagerBot] Error during cleanup: {e}")

    @property
    def is_initialized(self) -> bool:
        """检查是否已初始化"""
        return self._initialized
