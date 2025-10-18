"""
Telegram任务管理机器人核心逻辑
整合所有组件，提供统一的任务管理接口
"""

import asyncio
from typing import Optional, List, Dict, Any

from .handlers import TaskManagerHandlers
from .formatters import TaskManagerFormatters
from .keyboards import TaskManagerKeyboards


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
            from telethon.tl import types

            # 注册命令处理器
            @self.telegram_bot.bot_thon.on(events.NewMessage(func=lambda e: e.text and e.text.startswith('/start')))
            async def start_handler(event):
                await self.handlers.handle_start_command(event)

            @self.telegram_bot.bot_thon.on(events.NewMessage(func=lambda e: e.text and e.text.startswith('/status')))
            async def status_handler(event):
                await self.handlers.handle_status_command(event)

            @self.telegram_bot.bot_thon.on(events.NewMessage(func=lambda e: e.text and e.text.startswith('/help')))
            async def help_handler(event):
                await self.handlers.handle_help_command(event)

            @self.telegram_bot.bot_thon.on(events.NewMessage(func=lambda e: e.text and e.text.startswith('/detail')))
            async def detail_handler(event):
                await self.handlers.handle_detail_command(event)

            @self.telegram_bot.bot_thon.on(events.NewMessage(func=lambda e: e.text and e.text.startswith('/reload_config')))
            async def reload_config_handler(event):
                await self.handle_reload_config_command(event)

            # 注册回调查询处理器
            @self.telegram_bot.bot_thon.on(events.CallbackQuery)
            async def callback_handler(event):
                await self.handlers.handle_callback_query(event)

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
            "• `/reload_config` - 重载配置文件\n"
            "• `/help` - 查看帮助信息\n\n"
            "*功能说明:*\n"
            "• 📋 查看任务状态 - 查看所有任务的运行状态\n"
            "• 📝 任务详情 - 查看任务的详细信息和执行历史\n"
            "• 🚀 立即执行 - 手动触发任务执行\n"
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
        self.logger.info(f"[TaskManagerBot] 收到配置重载请求，chat_id: {chat_id}")

        try:
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

    async def reload_scheduler_config(self) -> bool:
        """重载调度器配置"""
        try:
            self.logger.info("[TaskManagerBot] 开始重载调度器配置...")

            # 1. 重载配置管理器中的配置
            self.logger.info("[TaskManagerBot] 步骤1: 重载配置文件...")
            self.config_manager.reload_config()

            # 2. 重载任务配置管理器
            self.logger.info("[TaskManagerBot] 步骤2: 重载任务配置...")
            self.job_config_manager.load_job_configs()

            # 3. 重新加载调度器中的任务
            self.logger.info("[TaskManagerBot] 步骤3: 重新加载调度器任务...")
            await self.task_scheduler.load_jobs_from_config()

            # 4. 重载报告配置
            self.logger.info("[TaskManagerBot] 步骤4: 重载报告配置...")
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

            # 支持不同格式的chat_id
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    return chat_id in authorized_chats
                else:
                    chat_id = int(chat_id)
            else:
                chat_id = int(chat_id)

            return chat_id in authorized_chats

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