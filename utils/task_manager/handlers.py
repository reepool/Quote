"""
Telegram任务管理机器人消息处理器
处理用户的各种交互操作
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

from .models import TaskStatusInfo, TaskStatus, TaskManagerState
from .formatters import TaskManagerFormatters
from .keyboards import TaskManagerKeyboards
from utils import task_manager_logger


class TaskManagerHandlers:
    """任务管理器消息处理器"""

    def __init__(self, task_manager):
        self.task_manager = task_manager
        self.user_states: Dict[int, TaskManagerState] = {}

    async def handle_start_command(self, event) -> None:
        """处理 /start 命令"""
        chat_id = event.chat_id
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 收到/start命令，chat_id: {chat_id}")

        user_state = self._get_user_state(chat_id)
        user_state.current_view = "main"
        user_state.selected_job_id = None

        message = TaskManagerFormatters.format_main_message()

        # 简化处理：暂时不使用键盘，发送带有命令提示的文本消息
        message += "\n\n*可用命令：*\n"
        message += "• `/status` - 查看任务状态\n"
        message += "• `/detail <任务ID>` - 查看任务详情\n"
        message += "• `/reload_config` - 重载配置文件\n"
        message += "• `/help` - 查看帮助信息\n\n"
        message += "*示例：* `/detail trading_calendar_update` 或 `/reload_config`"

        self.task_manager.logger.debug(f"[TaskManagerHandlers] 发送/start响应消息到 {chat_id}")

        await self.task_manager.send_message(
            chat_id,
            message,
            parse_mode='markdown'
        )

    async def handle_status_command(self, event) -> None:
        """处理 /status 命令"""
        chat_id = event.chat_id
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 收到/status命令，chat_id: {chat_id}")
        await self._show_task_status_simple(chat_id)

    async def handle_help_command(self, event) -> None:
        """处理 /help 命令"""
        chat_id = event.chat_id
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 收到/help命令，chat_id: {chat_id}")

        help_message = (
            "🤖 *Quote 任务管理器帮助*\n\n"
            "*可用命令：*\n"
            "• `/status` - 查看所有任务状态\n"
            "• `/detail <任务ID>` - 查看任务详情\n"
            "• `/reload_config` - 重载配置文件\n"
            "• `/help` - 显示此帮助信息\n\n"
            "*可用的任务ID：*\n"
            "• `trading_calendar_update` - 交易日历更新\n"
            "• `daily_data_update` - 每日数据更新任务\n"
            "• `system_health_check` - 系统健康检查\n"
            "• `weekly_maintenance` - 每周数据维护\n"
            "• `monthly_sync` - 每月全量数据同步\n"
            "• `quarterly_cleanup` - 季度数据清理\n"
            "• `cache_warm_up` - 缓存预热\n\n"
            "*使用示例：*\n"
            "• `/detail trading_calendar_update`\n"
            "• `/detail daily_data_update`\n"
            "• `/reload_config` - 重载所有任务配置\n\n"
            "💡 *提示：*\n"
            "• 使用 `/status` 可以看到所有任务的当前状态和下次执行时间\n"
            "• 使用 `/reload_config` 可以在修改配置文件后热重载，无需重启进程"
        )

        await self.task_manager.send_message(
            chat_id,
            help_message,
            parse_mode='markdown'
        )

    async def handle_detail_command(self, event) -> None:
        """处理 /detail 命令"""
        chat_id = event.chat_id
        command_text = event.text
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 收到/detail命令: {command_text}, chat_id: {chat_id}")

        # 解析命令参数
        parts = command_text.split()
        if len(parts) < 2:
            error_message = (
                "❌ *参数错误*\n\n"
                "请指定要查看的任务ID。\n\n"
                "*格式：* `/detail <任务ID>`\n\n"
                "*示例：* `/detail trading_calendar_update`\n\n"
                "使用 `/help` 查看所有可用的任务ID。"
            )
            await self.task_manager.send_message(
                chat_id,
                error_message,
                parse_mode='markdown'
            )
            return

        job_id = parts[1]
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 查看任务详情: {job_id}")

        try:
            await self._handle_task_detail(chat_id, job_id)
        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 处理/detail命令失败: {e}")
            error_message = (
                f"❌ *获取任务详情失败*\n\n"
                f"任务ID: `{job_id}`\n"
                f"错误: {str(e)}\n\n"
                f"请检查任务ID是否正确，使用 `/help` 查看可用任务。"
            )
            await self.task_manager.send_message(
                chat_id,
                error_message,
                parse_mode='markdown'
            )

    async def handle_callback_query(self, event) -> None:
        """处理回调查询"""
        # Telethon中chat_id属性访问
        chat_id = event.chat_id
        user_state = self._get_user_state(chat_id)

        try:
            # Telethon中data属性访问
            action, target, job_id = TaskManagerKeyboards.parse_callback_data(event.data)

            # 处理不同的操作
            if action == "task_detail":
                await self._handle_task_detail(chat_id, target)
            elif action == "task_action":
                await self._handle_task_action(chat_id, target, job_id)
            elif action == "confirm":
                await self._handle_confirmation(chat_id, target, job_id)
            elif action == "cancel":
                await self._handle_cancellation(chat_id, target, job_id)
            elif action == "refresh":
                await self._handle_refresh(chat_id, target, job_id)
            elif action == "back":
                await self._handle_navigation(chat_id, target, job_id)
            elif action == "retry":
                await self._handle_retry(chat_id, job_id)
            elif action == "loading":
                # 忽略加载状态的点击
                pass
            elif TaskManagerKeyboards.is_separator_button(event.data):
                # 忽略分隔符按钮的点击
                pass
            else:
                await self._handle_unknown_action(chat_id, event.data)

        except Exception as e:
            await self._handle_error(chat_id, "handler_error", str(e))

    async def _handle_task_detail(self, chat_id: int, job_id: str) -> None:
        """处理任务详情查看"""
        user_state = self._get_user_state(chat_id)
        user_state.current_view = "detail"
        user_state.selected_job_id = job_id

        try:
            # 直接获取任务详情并显示，避免复杂对象处理
            await self._show_task_detail_safe(chat_id, job_id)

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 处理任务详情失败: {e}")
            import traceback
            self.task_manager.logger.debug(f"[TaskManagerHandlers] 错误堆栈: {traceback.format_exc()}")

            # 发送基本错误信息
            error_message = (
                f"❌ *获取任务详情失败*\n\n"
                f"任务ID: `{job_id}`\n"
                f"错误: {str(e)}\n\n"
                f"请检查任务ID是否正确，或稍后重试。"
            )

            await self.task_manager.send_message(
                chat_id,
                error_message,
                parse_mode='markdown'
            )

    async def _show_task_detail_safe(self, chat_id: int, job_id: str) -> None:
        """安全显示任务详情，避免复杂对象处理"""
        try:
            self.task_manager.logger.debug(f"[TaskManagerHandlers] 安全获取任务详情: {job_id}")

            # 从配置文件直接获取信息
            from utils import config_manager
            job_cfg = config_manager.get_nested(f'scheduler_config.jobs.{job_id}', {})

            if not job_cfg:
                error_message = f"❌ *任务不存在*\n\n任务ID: `{job_id}`\n\n请使用 `/help` 查看可用任务。"
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
                return

            # 构建基本任务信息
            description = job_cfg.get('description', job_id)
            enabled = job_cfg.get('enabled', True)
            trigger_cfg = job_cfg.get('trigger', {})
            parameters = job_cfg.get('parameters', {})

            # 状态判断
            status_emoji = "🟢" if enabled else "🔴"
            status_text = "运行中" if enabled else "已禁用"

            # 触发器描述
            trigger_type = trigger_cfg.get('type', 'unknown')
            if trigger_type == 'cron':
                hour = trigger_cfg.get('hour', '*')
                minute = trigger_cfg.get('minute', '*')
                day_of_week = trigger_cfg.get('day_of_week', '*')
                trigger_desc = f"定时执行 - {minute}分 {hour}时 (周{day_of_week})"
            elif trigger_type == 'interval':
                hours = trigger_cfg.get('hours', 0)
                minutes = trigger_cfg.get('minutes', 0)
                if hours > 0:
                    trigger_desc = f"间隔执行 - 每{hours}小时"
                elif minutes > 0:
                    trigger_desc = f"间隔执行 - 每{minutes}分钟"
                else:
                    trigger_desc = "间隔执行"
            else:
                trigger_desc = f"未知类型: {trigger_type}"

            # 构建消息
            message = (
                f"📝 *任务详情*\n\n"
                f"🏷️ *任务名称:* {description}\n"
                f"🆔 *任务ID:* `{job_id}`\n"
                f"{status_emoji} *状态:* {status_text}\n\n"
                f"⏰ *触发器信息*\n"
                f"类型: {trigger_desc}\n\n"
            )

            # 添加参数信息
            if parameters:
                message += f"🔧 *配置参数*\n"
                for key, value in parameters.items():
                    if isinstance(value, (list, dict)):
                        value_str = str(len(value)) + " 项"
                    else:
                        value_str = str(value)
                    message += f"   {key}: {value_str}\n"
                message += "\n"

            message += f"💡 *提示*\n使用 `/status` 查看所有任务状态"

            # 发送消息
            await self.task_manager.send_message(
                chat_id,
                message,
                parse_mode='markdown'
            )

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务详情显示成功: {job_id}")

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 安全显示任务详情失败: {e}")
            raise

    async def _handle_task_action(self, chat_id: int, action: str, job_id: str) -> None:
        """处理任务操作"""
        user_state = self._get_user_state(chat_id)

        if action in ["disable", "enable"]:
            # 需要确认的操作
            task_detail = await self._get_task_detail(job_id)
            if not task_detail:
                await self._handle_error(chat_id, "task_not_found", job_id)
                return

            message = TaskManagerFormatters.format_confirmation(action, task_detail.description)
            keyboard = TaskManagerKeyboards.confirmation_menu(action, job_id)

            await self._edit_message(
                chat_id,
                user_state.message_id,
                message,
                keyboard,
                parse_mode='markdown'
            )

        elif action == "run":
            # 立即执行任务
            await self._execute_task_action(chat_id, action, job_id)

        else:
            await self._handle_unknown_action(chat_id, f"task_action:{action}")

    async def _handle_confirmation(self, chat_id: int, action: str, job_id: str) -> None:
        """处理确认操作"""
        await self._execute_task_action(chat_id, action, job_id)

    async def _handle_cancellation(self, chat_id: int, action: str, job_id: str) -> None:
        """处理取消操作"""
        user_state = self._get_user_state(chat_id)

        # 返回任务详情
        await self._handle_task_detail(chat_id, job_id)

    async def _handle_refresh(self, chat_id: int, target: str, job_id: str = None) -> None:
        """处理刷新操作"""
        if target == "status":
            await self._show_task_status(chat_id, refresh=True)
        elif target == "detail" and job_id:
            await self._handle_task_detail(chat_id, job_id)
        else:
            await self._handle_unknown_action(chat_id, f"refresh:{target}")

    async def _handle_navigation(self, chat_id: int, target: str, job_id: str = None) -> None:
        """处理导航操作"""
        user_state = self._get_user_state(chat_id)

        if target == "main":
            user_state.current_view = "main"
            user_state.selected_job_id = None
            await self.handle_start_command(type('Event', (), {'chat_id': chat_id})())
        elif target == "status":
            await self._show_task_status(chat_id)
        elif target == "detail" and job_id:
            await self._handle_task_detail(chat_id, job_id)
        else:
            await self._handle_unknown_action(chat_id, f"back:{target}")

    async def _handle_retry(self, chat_id: int, job_id: str) -> None:
        """处理重试操作"""
        await self._handle_task_detail(chat_id, job_id)

    async def _handle_unknown_action(self, chat_id: int, action: str) -> None:
        """处理未知操作"""
        message = f"❓ *未知操作*\n\n操作: `{action}`\n\n请返回主页重新开始"
        keyboard = TaskManagerKeyboards.back_menu()

        await self.task_manager.send_message(
            chat_id,
            message,
            keyboard=keyboard,
            parse_mode='markdown'
        )

    async def _handle_error(self, chat_id: int, error_type: str, details: str = None) -> None:
        """处理错误情况"""
        user_state = self._get_user_state(chat_id)
        job_id = user_state.selected_job_id

        message = TaskManagerFormatters.format_error_message(error_type, details)
        keyboard = TaskManagerKeyboards.error_menu(error_type, job_id)

        if user_state.message_id:
            await self._edit_message(chat_id, user_state.message_id, message, keyboard, parse_mode='markdown')
        else:
            await self.task_manager.send_message(chat_id, message, keyboard=keyboard, parse_mode='markdown')

    async def _show_task_status_simple(self, chat_id: int, refresh: bool = False) -> None:
        """显示任务状态（简化版，不使用键盘）"""
        try:
            self.task_manager.logger.debug(f"[TaskManagerHandlers] 开始显示任务状态到 {chat_id}")

            # 获取任务状态
            running_tasks, disabled_tasks, total_tasks = await self._get_all_tasks_status()

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 获取到任务状态，准备格式化消息")

            # 格式化状态消息
            message = TaskManagerFormatters.format_task_status_summary(
                running_tasks, disabled_tasks, total_tasks
            )

            # 添加命令提示
            message += "\n\n*可用的任务控制命令：*\n"
            message += "• `/run <task_id>` - 立即运行任务\n"
            message += "• `/enable <task_id>` - 启用任务\n"
            message += "• `/disable <task_id>` - 禁用任务\n"
            message += "• `/detail <task_id>` - 查看任务详情"

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 发送状态消息到 {chat_id}")

            # 发送消息（不使用键盘）
            await self.task_manager.send_message(
                chat_id,
                message,
                parse_mode='markdown'
            )

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 状态消息发送完成")

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 显示任务状态失败: {str(e)}")
            import traceback
            self.task_manager.logger.error(f"[TaskManagerHandlers] 异常堆栈: {traceback.format_exc()}")
            await self._handle_error_simple(chat_id, "scheduler_error", str(e))

    async def _show_task_status(self, chat_id: int, refresh: bool = False) -> None:
        """显示任务状态"""
        user_state = self._get_user_state(chat_id)
        user_state.current_view = "status"

        # 发送加载消息
        if refresh:
            loading_message = TaskManagerFormatters.format_loading_message("刷新任务状态")
        else:
            loading_message = TaskManagerFormatters.format_loading_message("获取任务状态")

        loading_keyboard = TaskManagerKeyboards.loading_menu("加载状态")
        sent_message = await self.task_manager.send_message(
            chat_id,
            loading_message,
            keyboard=loading_keyboard,
            parse_mode='markdown'
        )
        user_state.message_id = sent_message.id if hasattr(sent_message, 'id') else None

        try:
            # 获取任务状态
            running_tasks, disabled_tasks, total_tasks = await self._get_all_tasks_status()

            # 格式化状态消息
            message = TaskManagerFormatters.format_task_status_summary(
                running_tasks, disabled_tasks, total_tasks
            )
            keyboard = TaskManagerKeyboards.task_status_menu(running_tasks, disabled_tasks)

            # 编辑消息
            await self._edit_message(chat_id, user_state.message_id, message, keyboard, parse_mode='markdown')

        except Exception as e:
            await self._handle_error(chat_id, "scheduler_error", str(e))

    async def _handle_error_simple(self, chat_id: int, error_type: str, details: str = None) -> None:
        """处理错误情况（简化版）"""
        message = TaskManagerFormatters.format_error_message(error_type, details)

        # 发送错误消息（不使用键盘）
        await self.task_manager.send_message(
            chat_id,
            message,
            parse_mode='markdown'
        )

    async def _execute_task_action(self, chat_id: int, action: str, job_id: str) -> None:
        """执行任务操作"""
        user_state = self._get_user_state(chat_id)

        # 发送加载消息
        action_text = {"run": "执行任务", "enable": "启用任务", "disable": "禁用任务"}.get(action, "处理任务")
        loading_message = TaskManagerFormatters.format_loading_message(action_text)
        loading_keyboard = TaskManagerKeyboards.loading_menu(action_text)

        await self._edit_message(
            chat_id,
            user_state.message_id,
            loading_message,
            loading_keyboard,
            parse_mode='markdown'
        )

        try:
            # 执行操作
            success = await self._perform_task_action(action, job_id)

            # 显示结果
            message = TaskManagerFormatters.format_action_result(action, job_id, success)
            keyboard = TaskManagerKeyboards.action_result_menu(job_id, success)

            await self._edit_message(
                chat_id,
                user_state.message_id,
                message,
                keyboard,
                parse_mode='markdown'
            )

            # 如果操作成功，清除选中的任务ID
            if success:
                user_state.selected_job_id = None

        except Exception as e:
            await self._handle_error(chat_id, "handler_error", str(e))

    async def _get_all_tasks_status(self) -> Tuple[List[TaskStatusInfo], List[TaskStatusInfo], int]:
        """获取所有任务状态"""
        try:
            task_manager_logger.info("[TaskManagerHandlers] 开始获取所有任务状态")

            # 获取调度器状态
            scheduler_status = self.task_manager.task_scheduler.get_all_jobs_status()
            task_manager_logger.debug(f"[TaskManagerHandlers] 调度器状态获取成功，任务数量: {len(scheduler_status.get('jobs', {}))}")

            # 获取配置信息
            job_configs = self.task_manager.job_config_manager.job_configs
            task_manager_logger.debug(f"[TaskManagerHandlers] 任务配置数量: {len(job_configs)}")

            total_jobs = len(job_configs)
            task_manager_logger.info(f"[TaskManagerHandlers] 总任务数: {total_jobs}")

            if total_jobs == 0:
                self.task_manager.logger.warning("[TaskManagerHandlers] 没有找到任何任务配置")
                return [], [], 0

            # 处理任务状态
            running_tasks = []
            disabled_tasks = []
            total_tasks = 0

            for job_id, job_config in job_configs.items():
                total_tasks += 1
                self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理任务: {job_id}")
                # 处理JobConfig对象或字典数据
                if hasattr(job_config, '__dict__'):
                    # 处理trigger对象，将其转换为字典格式
                    trigger_dict = None
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: 处理JobConfig对象")

                    # 使用APScheduler的类型检查而不是检查__dict__属性
                    from apscheduler.triggers.cron import CronTrigger
                    from apscheduler.triggers.interval import IntervalTrigger
                    from apscheduler.triggers.date import DateTrigger

                    trigger_obj = job_config.trigger
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: Trigger对象类型: {type(trigger_obj).__name__}")

                    if isinstance(trigger_obj, CronTrigger):
                        # CronTrigger对象
                        self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: 处理CronTrigger")
                        trigger_dict = {
                            'type': 'cron',
                            'second': getattr(trigger_obj, 'second', 0),
                            'minute': getattr(trigger_obj, 'minute', '*'),
                            'hour': getattr(trigger_obj, 'hour', '*'),
                            'day': getattr(trigger_obj, 'day', '*'),
                            'month': getattr(trigger_obj, 'month', '*'),
                            'day_of_week': getattr(trigger_obj, 'day_of_week', '*'),
                            'timezone': str(getattr(trigger_obj, 'timezone', 'Asia/Shanghai'))
                        }
                    elif isinstance(trigger_obj, IntervalTrigger):
                        # IntervalTrigger对象
                        self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: 处理IntervalTrigger")
                        interval = trigger_obj.interval
                        hours = int(interval.total_seconds() // 3600)
                        minutes = int((interval.total_seconds() % 3600) // 60)
                        seconds = int(interval.total_seconds() % 60)

                        trigger_dict = {
                            'type': 'interval',
                            'hours': hours,
                            'minutes': minutes,
                            'seconds': seconds,
                            'timezone': str(getattr(trigger_obj, 'timezone', 'Asia/Shanghai'))
                        }
                    elif isinstance(trigger_obj, DateTrigger):
                        # DateTrigger对象
                        self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: 处理DateTrigger")
                        trigger_dict = {
                            'type': 'date',
                            'run_date': str(getattr(trigger_obj, 'run_date', None)),
                            'timezone': str(getattr(trigger_obj, 'timezone', 'Asia/Shanghai'))
                        }
                    else:
                        self.task_manager.logger.warning(f"[TaskManagerHandlers] 任务 {job_id}: 未知的Trigger类型: {type(trigger_obj).__name__}")
                        trigger_dict = {
                            'type': 'unknown',
                            'classname': type(trigger_obj).__name__,
                            'repr': repr(trigger_obj)
                        }

                    config_dict = {
                        'enabled': job_config.enabled,
                        'description': job_config.description,
                        'trigger': trigger_dict,
                        'parameters': job_config.parameters,
                        'max_instances': job_config.max_instances,
                        'misfire_grace_time': job_config.misfire_grace_time,
                        'coalesce': job_config.coalesce
                    }
                else:
                    config_dict = job_config

                task_info = TaskStatusInfo.from_scheduler_data(
                    job_id,
                    scheduler_status,
                    config_dict
                )

                self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id} 状态: {task_info.status.value}")

                if task_info.status == TaskStatus.DISABLED:
                    disabled_tasks.append(task_info)
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id} 已添加到禁用列表")
                else:
                    running_tasks.append(task_info)
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id} 已添加到运行列表")

            # 按描述排序
            running_tasks.sort(key=lambda x: x.description)
            disabled_tasks.sort(key=lambda x: x.description)

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务统计 - 运行中: {len(running_tasks)}, 禁用: {len(disabled_tasks)}, 总计: {total_tasks}")

            return running_tasks, disabled_tasks, total_tasks

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 获取任务状态失败: {str(e)}")
            import traceback
            self.task_manager.logger.error(f"[TaskManagerHandlers] 异常堆栈: {traceback.format_exc()}")
            raise Exception(f"获取任务状态失败: {str(e)}")

    async def _get_task_detail(self, job_id: str) -> Optional[TaskStatusInfo]:
        """获取任务详情"""
        try:
            self.task_manager.logger.debug(f"[TaskManagerHandlers] 开始获取任务详情: {job_id}")

            # 获取调度器状态
            scheduler_status = self.task_manager.task_scheduler.get_all_jobs_status()
            self.task_manager.logger.debug(f"[TaskManagerHandlers] 调度器状态获取成功")

            # 获取配置信息
            job_config = self.task_manager.job_config_manager.job_configs.get(job_id)
            if not job_config:
                self.task_manager.logger.warning(f"[TaskManagerHandlers] 任务配置不存在: {job_id}")
                return None

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务配置类型: {type(job_config)}")

            # 直接从配置文件获取trigger信息，避免处理APScheduler对象
            try:
                config_dict = {}

                if hasattr(job_config, '__dict__'):
                    # JobConfig对象
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理JobConfig对象")
                    config_dict = {
                        'enabled': getattr(job_config, 'enabled', True),
                        'description': getattr(job_config, 'description', job_id),
                        'parameters': getattr(job_config, 'parameters', {}),
                        'max_instances': getattr(job_config, 'max_instances', 1),
                        'misfire_grace_time': getattr(job_config, 'misfire_grace_time', 300),
                        'coalesce': getattr(job_config, 'coalesce', True)
                    }
                else:
                    # 字典数据
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理字典配置")
                    config_dict = job_config.copy()

                # 从配置文件获取trigger信息，避免APScheduler对象问题
                from utils import config_manager
                job_cfg = config_manager.get_nested(f'scheduler_config.jobs.{job_id}', {})
                trigger_cfg = job_cfg.get('trigger', {})

                # 安全地创建trigger字典
                trigger_dict = {
                    'type': trigger_cfg.get('type', 'unknown'),
                    'second': trigger_cfg.get('second', 0),
                    'minute': trigger_cfg.get('minute', '*'),
                    'hour': trigger_cfg.get('hour', '*'),
                    'day': trigger_cfg.get('day', '*'),
                    'month': trigger_cfg.get('month', '*'),
                    'day_of_week': trigger_cfg.get('day_of_week', '*'),
                    'hours': trigger_cfg.get('hours', 0),
                    'minutes': trigger_cfg.get('minutes', 0),
                    'seconds': trigger_cfg.get('seconds', 0),
                    'timezone': trigger_cfg.get('timezone', 'Asia/Shanghai')
                }

                config_dict['trigger'] = trigger_dict
                self.task_manager.logger.debug(f"[TaskManagerHandlers] 配置字典准备完成")

            except Exception as config_error:
                self.task_manager.logger.error(f"[TaskManagerHandlers] 处理配置失败: {config_error}")
                # 使用基本配置
                config_dict = {
                    'enabled': True,
                    'description': job_id,
                    'trigger': {'type': 'unknown', 'error': 'Config processing failed'},
                    'parameters': {}
                }

            # 创建TaskStatusInfo对象
            try:
                task_info = TaskStatusInfo.from_scheduler_data(
                    job_id,
                    scheduler_status,
                    config_dict
                )
                self.task_manager.logger.debug(f"[TaskManagerHandlers] TaskStatusInfo创建成功")
                return task_info

            except Exception as create_error:
                self.task_manager.logger.error(f"[TaskManagerHandlers] 创建TaskStatusInfo失败: {create_error}")
                import traceback
                self.task_manager.logger.debug(f"[TaskManagerHandlers] 堆栈: {traceback.format_exc()}")

                # 手动创建基本的TaskStatusInfo
                from .models import TaskTriggerInfo, TaskStatus
                trigger_info = TaskTriggerInfo(
                    trigger_type="unknown",
                    description="处理失败时显示的基本信息"
                )

                return TaskStatusInfo(
                    job_id=job_id,
                    description=config_dict.get('description', job_id),
                    enabled=config_dict.get('enabled', True),
                    in_scheduler=job_id in scheduler_status.get('jobs', {}),
                    status=TaskStatus.ERROR,
                    trigger_info=trigger_info,
                    parameters=config_dict.get('parameters', {})
                )

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 获取任务详情失败: {e}")
            import traceback
            self.task_manager.logger.debug(f"[TaskManagerHandlers] 完整错误堆栈: {traceback.format_exc()}")
            raise Exception(f"获取任务详情失败: {str(e)}")

    async def _get_task_executions(self, job_id: str, limit: int = 5) -> List:
        """获取任务执行历史"""
        try:
            # 从监控器获取执行历史
            recent_executions = self.task_manager.scheduler_monitor.get_recent_executions(limit * 2)  # 获取更多记录用于过滤

            # 过滤指定任务的执行记录
            task_executions = []
            for execution in recent_executions:
                if execution.get('job_id') == job_id:
                    task_executions.append(execution)
                    if len(task_executions) >= limit:
                        break

            return task_executions

        except Exception as e:
            # 如果获取执行历史失败，返回空列表而不是抛出异常
            return []

    async def _perform_task_action(self, action: str, job_id: str) -> bool:
        """执行任务操作"""
        try:
            if action == "run":
                return await self.task_manager.task_scheduler.run_job_now(job_id)
            elif action == "enable":
                success = await self._enable_task(job_id)
                if success:
                    # 重新加载任务配置
                    await self.task_manager.job_config_manager.load_job_configs()
                    await self.task_manager.task_scheduler.load_jobs_from_config()
                return success
            elif action == "disable":
                success = await self._disable_task(job_id)
                if success:
                    # 从调度器移除任务
                    await self.task_manager.task_scheduler.remove_job(job_id)
                return success
            else:
                return False

        except Exception as e:
            raise Exception(f"执行任务操作失败: {str(e)}")

    async def _enable_task(self, job_id: str) -> bool:
        """启用任务"""
        try:
            # 更新配置
            config_path = f"scheduler_config.jobs.{job_id}.enabled"
            return await self.task_manager.config_manager.update_nested(config_path, True)

        except Exception as e:
            raise Exception(f"启用任务失败: {str(e)}")

    async def _disable_task(self, job_id: str) -> bool:
        """禁用任务"""
        try:
            # 更新配置
            config_path = f"scheduler_config.jobs.{job_id}.enabled"
            return await self.task_manager.config_manager.update_nested(config_path, False)

        except Exception as e:
            raise Exception(f"禁用任务失败: {str(e)}")

    async def _edit_message(self, chat_id: int, message_id: int, text: str,
                           keyboard: List = None, parse_mode: str = None) -> None:
        """编辑消息"""
        try:
            await self.task_manager.edit_message(
                chat_id,
                message_id,
                text,
                keyboard=keyboard,
                parse_mode=parse_mode
            )
        except Exception as e:
            # 如果编辑失败，发送新消息
            await self.task_manager.send_message(
                chat_id,
                text,
                keyboard=keyboard,
                parse_mode=parse_mode
            )

    def _get_user_state(self, chat_id: int) -> TaskManagerState:
        """获取用户状态"""
        if chat_id not in self.user_states:
            self.user_states[chat_id] = TaskManagerState(chat_id=chat_id)
        return self.user_states[chat_id]

    def cleanup_user_state(self, chat_id: int) -> None:
        """清理用户状态"""
        if chat_id in self.user_states:
            del self.user_states[chat_id]