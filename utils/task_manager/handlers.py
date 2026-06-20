"""
Telegram任务管理机器人消息处理器
处理用户的各种交互操作
"""

import asyncio
import shlex
from datetime import datetime
from types import SimpleNamespace
from typing import Dict, Any, Optional, List, Tuple

from .models import TaskStatusInfo, TaskStatus, TaskManagerState
from .formatters import TaskManagerFormatters
from .keyboards import TaskManagerKeyboards
from utils import task_manager_logger
from utils.date_utils import DateUtils


class TaskManagerHandlers:
    """任务管理器消息处理器"""

    def __init__(self, task_manager):
        self.task_manager = task_manager
        self.user_states: Dict[int, TaskManagerState] = {}

    async def handle_start_command(self, event) -> None:
        """处理 /start 命令"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        message_text = event.text if hasattr(event, 'text') else '/start'

        # 详细日志记录
        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{message_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理/start命令，chat_id: {chat_id}")

        user_state = self._get_user_state(chat_id)
        user_state.current_view = "main"
        user_state.selected_job_id = None

        message = TaskManagerFormatters.format_main_message()

        # 简化处理：暂时不使用键盘，发送带有命令提示的文本消息
        message += "\n\n*可用命令：*\n"
        message += "• `/status` - 查看任务状态\n"
        message += "• `/detail <任务ID>` - 查看任务详情\n"
        message += "• `/run <任务ID>` - 立即执行任务\n"
        message += "• `/run shareholder_shadow_sync` - 手工触发股东摘要全量刷新\n"
        message += "• `/run shareholder_reconciliation_sync` - 手工触发股东摘要周期复核与补足\n"
        message += "• `/run shareholder_incremental_sync` - 手工触发股东摘要每日增量检查\n"
        message += "• `/run hkex_instrument_master_sync` - 手工触发港股主数据 safe-write 同步\n"
        message += "• `/run index_master_governance_sync` - 手工触发A股指数主数据治理\n"
        message += "• `/backfill_factors [交易所...] [missing|full]` - 回填复权因子\n"
        message += "• `/industry_standard_sync [force]` - 申万官方分类日更同步\n"
        message += "• `/industry_standard_rebuild [force] [drop_source_files]` - 申万官方分类全量重建\n"
        message += "• `/industry_index_analysis_sync [limit=N]` - 申万行业指数分析日频同步\n"
        message += "• `/industry_index_analysis_backfill start=YYYY-MM-DD end=YYYY-MM-DD [limit=N] [chunk=month|day|quarter|year|none]` - 申万行业指数分析历史回补\n"
        message += "• `/futures_calendar_backfill exchange=SHFE start=YYYY-MM-DD end=YYYY-MM-DD [dry_run|write] [max_days=N]` - 手工回填期货交易日历\n"
        message += "• `/futures_master_governance exchange=GFEX [start=YYYY-MM-DD] [end=YYYY-MM-DD] [dry_run|write] [max_days=N]` - 期货主数据治理\n"
        message += "• `/futures_master_discovery_governance exchange=GFEX [start=YYYY-MM-DD] [end=YYYY-MM-DD] [dry_run|write] [max_days=N]` - 期货主数据发现治理\n"
        message += "• `/audit_factors` - 审计自研复权因子 (TDX)\n"
        message += "• `/hkex_review pending|list|<代码> <active|suspended|delisted> [日期] [原因]` - 港股主数据人工复核\n"
        message += "• `/smart_fill_gaps` - 智能补足大段缺口\n"
        message += "• `/find_gap_and_repair` - 精确逐日修复缺口\n"
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
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        message_text = event.text if hasattr(event, 'text') else '/status'

        # 详细日志记录
        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{message_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理/status命令，chat_id: {chat_id}")

        await self._show_task_status_simple(chat_id)

    async def handle_help_command(self, event) -> None:
        """处理 /help 命令"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        message_text = event.text if hasattr(event, 'text') else '/help'

        # 详细日志记录
        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{message_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理/help命令，chat_id: {chat_id}")

        help_message = (
            "🤖 *Quote 任务管理器帮助*\n\n"
            "*可用命令：*\n"
            "• `/status` - 查看所有任务状态\n"
            "• `/detail <任务ID>` - 查看任务详情\n"
            "• `/run <任务ID>` - 立即执行任务\n"
            "• `/run shareholder_shadow_sync` - 手工触发股东摘要全量刷新\n"
            "• `/run shareholder_reconciliation_sync` - 手工触发股东摘要周期复核与补足\n"
            "• `/run shareholder_incremental_sync` - 手工触发股东摘要每日增量检查\n"
            "• `/run hkex_instrument_master_sync` - 手工触发港股主数据 safe-write 同步\n"
            "• `/run index_master_governance_sync` - 手工触发A股指数主数据治理\n"
            "• `/backfill <日期|开始日期 结束日期> [交易所...]` - 补齐指定日期或日期范围的缺失数据\n"
            "• `/backfill_factors [交易所...] [missing|full]` - 回填复权因子\n"
            "• `/industry_standard_sync [force]` - 申万官方分类日更同步\n"
            "• `/industry_standard_rebuild [force] [drop_source_files]` - 申万官方分类全量重建\n"
            "• `/industry_index_analysis_sync [limit=N]` - 申万行业指数分析日频同步\n"
            "• `/industry_index_analysis_backfill start=YYYY-MM-DD end=YYYY-MM-DD [limit=N] [chunk=month|day|quarter|year|none]` - 申万行业指数分析历史回补\n"
            "• `/futures_calendar_backfill exchange=SHFE start=YYYY-MM-DD end=YYYY-MM-DD [dry_run|write] [max_days=N]` - 手工回填期货交易日历\n"
            "• `/futures_master_governance exchange=GFEX [start=YYYY-MM-DD] [end=YYYY-MM-DD] [dry_run|write] [max_days=N]` - 期货主数据治理\n"
            "• `/futures_master_discovery_governance exchange=GFEX [start=YYYY-MM-DD] [end=YYYY-MM-DD] [dry_run|write] [max_days=N]` - 期货主数据发现治理\n"
            "• `/hkex_review pending|list|<代码> <active|suspended|delisted> [日期] [原因]` - 港股主数据人工复核\n"
            "• `/smart_fill_gaps` - 智能补足大段数据缺口 (Phase 1)\n"
            "• `/find_gap_and_repair` - 精确逐日修复所有缺口 (Phase 2)\n"
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
            "*研究域任务ID：*\n"
            "• `shareholder_shadow_sync` - 股东摘要手工全量刷新\n"
            "• `shareholder_reconciliation_sync` - 股东摘要周期复核与补足\n"
            "• `shareholder_incremental_sync` - 股东摘要每日增量检查\n"
            "• `industry_standard_sync` - 申万官方分类日更同步\n"
            "• `industry_standard_rebuild` - 申万官方分类全量重建\n"
            "• `industry_index_analysis_sync` - 申万指数分析日频同步\n"
            "• `industry_index_analysis_backfill` - 申万指数分析历史回补\n\n"
            "*申万行业示例：*\n"
            "• `/industry_standard_sync` - 使用 source manifest 做日更，未变化则短路\n"
            "• `/industry_standard_sync force` - 强制重新拉取官方文件并同步\n"
            "• `/industry_standard_rebuild force` - 清理 strict Shenwan slice 后全量重建\n\n"
            "• `/industry_index_analysis_sync limit=20` - 小样本同步申万指数分析指标\n\n"
            "• `/industry_index_analysis_backfill start=2024-10-25 end=2024-10-25 limit=20` - 小样本回补历史申万指数分析指标\n"
            "• `/industry_index_analysis_backfill start=2023-12-01 end=2023-12-29 chunk=day` - 按日补缺申万指数分析历史缺口\n\n"
            "*期货交易日历示例：*\n"
            "• `/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
            "• `/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 write max_days=10`\n\n"
            "*期货主数据治理示例：*\n"
            "• `/futures_master_governance exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
            "• `/futures_master_discovery_governance exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n\n"
            "*使用示例：*\n"
            "• `/detail trading_calendar_update`\n"
            "• `/run system_health_check`\n"
            "• `/run daily_data_update 2026-03-27`\n"
            "• `/run shareholder_shadow_sync`\n"
            "• `/run shareholder_reconciliation_sync`\n"
            "• `/run shareholder_incremental_sync`\n"
            "• `/backfill 2026-03-27`\n"
            "• `/backfill 2026-04-09 2026-05-21 SSE SZSE BSE`\n"
            "• `/backfill_factors HKEX full`\n"
            "• `/industry_standard_sync`\n"
            "• `/industry_standard_rebuild force`\n"
            "• `/industry_index_analysis_sync limit=20`\n"
            "• `/industry_index_analysis_backfill start=2024-10-25 end=2024-10-25 limit=20`\n"
            "• `/industry_index_analysis_backfill start=2023-12-01 end=2023-12-29 chunk=day`\n"
            "• `/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
            "• `/futures_master_discovery_governance exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
            "• `/reload_config` - 重载所有任务配置\n\n"
            "💡 *提示：*\n"
            "• 使用 `/status` 可以看到所有任务的当前状态和下次执行时间\n"
            "• 使用 `/run` 可以立即执行任何任务\n"
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
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        command_text = event.text if hasattr(event, 'text') else '/detail'

        # 详细日志记录
        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{command_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")
        self.task_manager.logger.debug(f"[TaskManagerHandlers] 处理/detail命令: {command_text}, chat_id: {chat_id}")

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

            command_hint = "\n\n**💡 可用命令:**\n"
            command_hint += "• `/run <任务ID>` - 立即执行\n"
            command_hint += "• `/detail <任务ID>` - 查看详情\n"
            command_hint += "• `/help` - 获取更多帮助"

            # 格式化状态消息
            messages = TaskManagerFormatters.format_task_status_messages(
                running_tasks,
                disabled_tasks,
                total_tasks,
            )

            self.task_manager.logger.debug(f"[TaskManagerHandlers] 发送状态消息到 {chat_id}")

            # 发送消息（不使用键盘）
            try:
                for index, message in enumerate(messages):
                    if index == len(messages) - 1:
                        message += command_hint
                    await self.task_manager.send_message(
                        chat_id,
                        message,
                        parse_mode='markdown'
                    )
            except Exception as send_error:
                if "Message was too long" not in str(send_error):
                    raise

                self.task_manager.logger.warning(
                    "[TaskManagerHandlers] 状态消息过长，使用紧凑摘要重试"
                )
                compact_messages = TaskManagerFormatters.format_task_status_messages(
                    running_tasks,
                    disabled_tasks,
                    total_tasks,
                    max_chars=2400,
                )
                for index, compact_message in enumerate(compact_messages):
                    if index == len(compact_messages) - 1:
                        compact_message += command_hint
                    await self.task_manager.send_message(
                        chat_id,
                        compact_message,
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

                # 优先检查任务是否在配置中被禁用
                if not job_config.enabled:
                    task_info = TaskStatusInfo(
                        job_id=job_id,
                        description=job_config.description,
                        status=TaskStatus.DISABLED,
                        enabled=False,
                        in_scheduler=False,
                        next_run_time=None,
                        trigger_info=None, # 触发器信息对于禁用任务不重要
                        parameters=job_config.parameters
                    )
                    disabled_tasks.append(task_info)
                    self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id} 已禁用，直接添加到禁用列表")
                    continue # 处理下一个任务

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
                        # CronTrigger对象，提取其参数
                        self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: 处理CronTrigger")
                        # 安全地获取每个字段，如果不存在则使用默认值 '*'
                        trigger_dict = {
                            'type': 'cron',
                            'year': str(getattr(trigger_obj, 'year', '*')),
                            'month': str(getattr(trigger_obj, 'month', '*')),
                            'day': str(getattr(trigger_obj, 'day', '*')),
                            'week': str(getattr(trigger_obj, 'week', '*')),
                            'day_of_week': str(getattr(trigger_obj, 'day_of_week', '*')),
                            'hour': str(getattr(trigger_obj, 'hour', '*')),
                            'minute': str(getattr(trigger_obj, 'minute', '*')),
                            'second': str(getattr(trigger_obj, 'second', '*')),
                            'timezone': str(getattr(trigger_obj, 'timezone', 'Asia/Shanghai'))
                        }
                    elif isinstance(trigger_obj, IntervalTrigger):
                        # IntervalTrigger对象，提取其参数
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
                        # DateTrigger对象，提取其参数
                        self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: 处理DateTrigger")
                        trigger_dict = {
                            'type': 'date',
                            'run_date': str(getattr(trigger_obj, 'run_date', None)),
                            'timezone': str(getattr(trigger_obj, 'timezone', 'Asia/Shanghai'))
                        }
                    elif trigger_obj is None and getattr(job_config, 'manual_only', False):
                        self.task_manager.logger.debug(f"[TaskManagerHandlers] 任务 {job_id}: manual_only任务无Trigger")
                        trigger_dict = {
                            'type': 'manual_only',
                            'classname': 'NoneType',
                            'repr': 'manual_only'
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
                        'manual_only': getattr(job_config, 'manual_only', False),
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
            recent_executions = self.task_manager.task_scheduler.scheduler_monitor.get_recent_executions(limit * 2)  # 获取更多记录用于过滤

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
                    await self.task_manager.task_scheduler.remove_job(job_id)  # 确认异步调用
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

    async def handle_hkex_review_command(self, event) -> None:
        """处理 /hkex_review 命令，用于港股主数据人工复核。"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        command_text = event.text if hasattr(event, 'text') else '/hkex_review'
        parts = command_text.strip().split()

        usage = (
            "港股主数据人工复核用法:\n"
            "`/hkex_review pending [limit]`\n"
            "`/hkex_review list [limit]`\n"
            "`/hkex_review 02934.HK delisted 2026-05-30 已确认退市 evidence=https://...`\n"
            "`/hkex_review 00005.HK suspended 停牌复核`"
        )
        if len(parts) < 2:
            await self.task_manager.send_message(chat_id, usage, parse_mode='markdown')
            return

        subcommand = parts[1].strip().lower()
        try:
            from data_manager import data_manager

            if subcommand == 'pending':
                limit = 5
                if len(parts) >= 3:
                    try:
                        limit = max(1, min(int(parts[2]), 20))
                    except ValueError:
                        limit = 5
                result = await data_manager.sync_hkex_instrument_master(mode='audit_only')
                hkex = (result.get('exchanges') or {}).get('HKEX', {})
                samples = hkex.get('review_required_samples', [])[:limit]
                lines = [
                    "港股主数据待复核",
                    f"状态: `{result.get('status')}`",
                    f"待复核: `{(result.get('summary') or {}).get('review_required', 0)}`",
                ]
                if samples:
                    lines.append("样本:")
                    for item in samples:
                        local = item.get('local') or {}
                        lines.append(
                            f"- `{item.get('instrument_id')}` {local.get('name', '')} "
                            f"`{item.get('reason')}`"
                        )
                else:
                    lines.append("当前无待复核样本。")
                await self.task_manager.send_message(chat_id, "\n".join(lines), parse_mode='markdown')
                return

            if subcommand == 'list':
                limit = 10
                if len(parts) >= 3:
                    try:
                        limit = max(1, min(int(parts[2]), 50))
                    except ValueError:
                        limit = 10
                payload = await data_manager.get_hkex_manual_review_evidence(limit=limit)
                lines = [
                    "港股主数据人工复核记录",
                    f"文件: `{payload.get('path')}`",
                    f"总数: `{payload.get('total')}`",
                ]
                for item in payload.get('entries', [])[-limit:]:
                    lines.append(
                        f"- `{item.get('instrument_id')}` `{item.get('action')}` "
                        f"`{item.get('effective_date') or '-'}` {item.get('reason') or ''}"
                    )
                await self.task_manager.send_message(chat_id, "\n".join(lines), parse_mode='markdown')
                return

            if len(parts) < 3:
                await self.task_manager.send_message(chat_id, usage, parse_mode='markdown')
                return

            instrument_id = parts[1]
            action = parts[2]
            rest = parts[3:]
            effective_date = None
            if rest:
                try:
                    datetime.fromisoformat(rest[0][:10])
                    effective_date = rest.pop(0)[:10]
                except ValueError:
                    effective_date = None

            evidence_url = ''
            reason_parts = []
            for token in rest:
                if token.startswith('evidence=') or token.startswith('url='):
                    evidence_url = token.split('=', 1)[1].strip()
                else:
                    reason_parts.append(token)
            reason = ' '.join(reason_parts).strip()

            payload = await data_manager.append_hkex_manual_review_evidence(
                instrument_id=instrument_id,
                action=action,
                effective_date=effective_date,
                reason=reason,
                evidence_url=evidence_url,
                reviewed_by=str(user_id),
            )
            entry = payload.get('entry') or {}
            message = (
                "已追加港股主数据人工复核\n"
                f"代码: `{entry.get('instrument_id')}`\n"
                f"结论: `{entry.get('action')}`\n"
                f"日期: `{entry.get('effective_date') or '-'}`\n"
                f"文件: `{payload.get('path')}`\n"
                f"总数: `{payload.get('total')}`\n\n"
                "下一步可执行 `/hkex_review pending` 复核 audit_only 样本变化。"
            )
            await self.task_manager.send_message(chat_id, message, parse_mode='markdown')
        except ValueError as exc:
            await self.task_manager.send_message(
                chat_id,
                f"参数错误: `{exc}`\n\n{usage}",
                parse_mode='markdown',
            )
        except Exception as exc:
            self.task_manager.logger.error(f"[TaskManagerHandlers] HKEX review command failed: {exc}")
            await self.task_manager.send_message(
                chat_id,
                f"港股主数据人工复核失败: `{exc}`",
                parse_mode='markdown',
            )

    async def handle_run_command(self, event) -> None:
        """处理 /run 命令，支持 /run <task_id> [日期]"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        command_text = event.text if hasattr(event, 'text') else '/run'

        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{command_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")

        # 解析命令参数
        parts = command_text.strip().split()
        if len(parts) < 2:
            error_message = (
                "❌ *缺少任务ID*\n\n"
                "请使用: `/run <task_id> [日期]`\n\n"
                "例如:\n"
                "• `/run system_health_check`\n"
                "• `/run daily_data_update 2026-03-27`\n\n"
                "使用 `/help` 查看可用任务列表。"
            )
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        job_id = parts[1]
        if job_id == 'futures_official_calendar_backfill' and len(parts) > 2:
            await self.handle_futures_calendar_backfill_command(
                SimpleNamespace(
                    chat_id=chat_id,
                    sender_id=user_id,
                    text='/futures_calendar_backfill ' + ' '.join(parts[2:]),
                )
            )
            return
        if job_id == 'futures_master_governance' and len(parts) > 2:
            await self.handle_futures_master_governance_command(
                SimpleNamespace(
                    chat_id=chat_id,
                    sender_id=user_id,
                    text='/futures_master_governance ' + ' '.join(parts[2:]),
                )
            )
            return
        if job_id == 'futures_master_discovery_governance' and len(parts) > 2:
            await self.handle_futures_master_discovery_governance_command(
                SimpleNamespace(
                    chat_id=chat_id,
                    sender_id=user_id,
                    text='/futures_master_discovery_governance ' + ' '.join(parts[2:]),
                )
            )
            return
        if job_id in {'futures_market_data_sync', 'futures_market_data_backfill'} and len(parts) > 2:
            await self.handle_futures_market_data_command(
                SimpleNamespace(
                    chat_id=chat_id,
                    sender_id=user_id,
                    text=f'/{job_id} ' + ' '.join(parts[2:]),
                ),
                job_id=job_id,
            )
            return

        # 解析可选的日期参数（第三个参数）
        target_date = None
        if len(parts) >= 3 and job_id == 'daily_data_update':
            target_date = self._parse_date_arg(parts[2])
            if target_date is None:
                error_message = f"❌ *日期格式错误*\n\n`{parts[2]}` 不是有效日期。\n\n请使用 `YYYY-MM-DD` 格式，例如: `/run daily_data_update 2026-03-27`"
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
                return

        self.task_manager.logger.info(f"[TaskManagerHandlers] 尝试立即执行任务: {job_id}, target_date={target_date}")

        try:
            # 验证任务是否存在
            from utils import config_manager
            job_cfg = config_manager.get_nested(f'scheduler_config.jobs.{job_id}', {})

            if not job_cfg:
                error_message = f"❌ *任务不存在*\n\n任务ID: `{job_id}`\n\n请使用 `/help` 查看可用任务。"
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
                return

            if not job_cfg.get('enabled', True):
                error_message = f"❌ *任务已禁用*\n\n任务ID: `{job_id}`\n\n请先启用任务后再执行。"
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
                return

            # 发送开始执行通知
            date_info = f" (补数据日期: {target_date})" if target_date else ""
            start_message = f"⏳ *正在执行任务...*\n\n任务ID: `{job_id}`{date_info}\n\n请稍候，任务执行中..."
            await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')

            # 执行任务
            self.task_manager.logger.info(f"[TaskManagerHandlers] 开始执行任务: {job_id}")
            success = await self._execute_task_direct(chat_id, job_id, target_date=target_date)

            if success:
                success_message = f"✅ *任务执行成功*\n\n任务ID: `{job_id}`{date_info}"
                await self.task_manager.send_message(chat_id, success_message, parse_mode='markdown')
            else:
                error_message = f"❌ *任务执行失败*\n\n任务ID: `{job_id}`{date_info}\n\n请检查日志。"
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')

        except Exception as e:
            error_message = (
                f"❌ *执行任务时发生异常*\n\n"
                f"任务ID: `{job_id}`\n"
                f"错误: {str(e)}\n\n"
                f"请检查日志或稍后重试。"
            )
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            self.task_manager.logger.error(f"[TaskManagerHandlers] 执行任务异常: {job_id}, 错误: {e}")

    async def handle_backfill_command(self, event) -> None:
        """处理 /backfill 命令，用于补充指定日期或日期范围的缺失数据

        用法: /backfill <日期> [交易所...]
              /backfill <开始日期> <结束日期> [交易所...]
        示例: /backfill 2026-03-27
              /backfill 2026-03-27 SSE SZSE
              /backfill 2026-04-09 2026-05-21 SSE SZSE BSE
        """
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        command_text = event.text if hasattr(event, 'text') else '/backfill'

        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{command_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")

        parts = command_text.strip().split()
        if len(parts) < 2:
            error_message = (
                "❌ *缺少日期参数*\n\n"
                "用法:\n"
                "• `/backfill <日期> [交易所...]`\n"
                "• `/backfill <开始日期> <结束日期> [交易所...]`\n\n"
                "示例:\n"
                "• `/backfill 2026-03-27` - 补充 3/27 所有交易所数据\n"
                "• `/backfill 2026-03-27 SSE` - 仅补充上交所数据\n"
                "• `/backfill 2026-04-09 2026-05-21 SSE SZSE BSE` - 补充日期范围内交易日"
            )
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        # 解析日期：第二个日期参数存在时进入时间段模式。
        start_date = self._parse_date_arg(parts[1])
        if start_date is None:
            error_message = f"❌ *日期格式错误*\n\n`{parts[1]}` 不是有效日期。\n\n请使用 `YYYY-MM-DD` 格式。"
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        end_date = start_date
        exchange_arg_start = 2
        if len(parts) >= 3:
            parsed_end_date = self._parse_date_arg(parts[2])
            if parsed_end_date is not None:
                end_date = parsed_end_date
                exchange_arg_start = 3

        if end_date < start_date:
            error_message = f"❌ *日期范围错误*\n\n结束日期 `{end_date}` 早于开始日期 `{start_date}`。"
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        # 解析可选的交易所参数
        exchanges = None
        if len(parts) > exchange_arg_start:
            valid_exchanges = {'SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE'}
            requested_exchanges = [ex.upper() for ex in parts[exchange_arg_start:]]
            invalid_exchanges = [ex for ex in requested_exchanges if ex not in valid_exchanges]
            if invalid_exchanges:
                error_message = f"❌ *无效的交易所代码*\n\n无效参数: `{', '.join(invalid_exchanges)}`\n支持: SSE, SZSE, BSE, HKEX, NASDAQ, NYSE"
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
                return
            exchanges = requested_exchanges

        exchanges_info = f" (交易所: {', '.join(exchanges)})" if exchanges else " (所有交易所)"
        effective_exchanges = exchanges or self._get_default_backfill_exchanges()
        backfill_dates = self._build_backfill_dates(start_date, end_date, effective_exchanges)

        if not backfill_dates:
            empty_message = (
                "⚠️ *没有可补充的交易日*\n\n"
                f"日期范围: `{start_date}` 至 `{end_date}`{exchanges_info}"
            )
            await self.task_manager.send_message(chat_id, empty_message, parse_mode='markdown')
            return

        if start_date == end_date:
            date_info = f"目标日期: `{start_date}`"
        else:
            date_info = (
                f"日期范围: `{start_date}` 至 `{end_date}`\n"
                f"交易日数量: `{len(backfill_dates)}`"
            )
        start_message = f"⏳ *正在补充数据...*\n\n{date_info}{exchanges_info}\n\n请稍候，任务执行中..."
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')

        try:
            from scheduler.tasks import scheduled_tasks

            succeeded = []
            failed = []
            if start_date != end_date:
                try:
                    result = await scheduled_tasks.daily_data_backfill_range(
                        start_date=start_date,
                        end_date=end_date,
                        exchanges=exchanges,
                        run_factor_audit=False,
                    )
                    if result and result.get('failure_count', 0) == 0:
                        succeeded = backfill_dates
                    else:
                        failed.append((f"{start_date}~{end_date}", result.get('error', '任务返回未成功') if isinstance(result, dict) else '任务返回未成功'))
                except Exception as range_e:
                    failed.append((f"{start_date}~{end_date}", str(range_e)))
                    self.task_manager.logger.error(
                        f"[TaskManagerHandlers] 区间数据补充失败: {start_date}~{end_date}, 错误: {range_e}"
                    )
            else:
                for target_date in backfill_dates:
                    try:
                        result = await scheduled_tasks.daily_data_update(
                            exchanges=exchanges,
                            target_date=target_date,
                            wait_for_market_close=False,
                            enable_trading_day_check=False,
                            run_factor_audit=False,
                        )
                        if result:
                            succeeded.append(target_date)
                        else:
                            failed.append((target_date, '任务返回未成功'))
                    except Exception as single_e:
                        failed.append((target_date, str(single_e)))
                        self.task_manager.logger.error(
                            f"[TaskManagerHandlers] 单日数据补充失败: {target_date}, 错误: {single_e}"
                        )

            if not failed:
                success_message = (
                    "✅ *数据补充完成*\n\n"
                    f"{date_info}{exchanges_info}\n"
                    f"成功交易日: `{len(succeeded)}`"
                )
                await self.task_manager.send_message(chat_id, success_message, parse_mode='markdown')
            else:
                failed_preview = "\n".join(
                    f"• `{day}`: {reason}" for day, reason in failed[:10]
                )
                more = f"\n... 另有 {len(failed) - 10} 个失败日期" if len(failed) > 10 else ""
                error_message = (
                    "⚠️ *数据补充未完全成功*\n\n"
                    f"{date_info}{exchanges_info}\n"
                    f"成功交易日: `{len(succeeded)}`\n"
                    f"失败交易日: `{len(failed)}`\n\n"
                    f"{failed_preview}{more}\n\n"
                    "请检查日志。"
                )
                await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')

        except Exception as e:
            error_message = f"❌ *数据补充失败*\n\n错误: {str(e)}\n\n请检查日志。"
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            self.task_manager.logger.error(f"[TaskManagerHandlers] 数据补充失败: {e}")

    def _get_default_backfill_exchanges(self) -> List[str]:
        """Return exchanges used by scheduler daily_data_update when /backfill omits exchanges."""
        try:
            from utils import config_manager
            exchanges = config_manager.get_nested(
                'data_config.market_presets.a_shares',
                default=['SSE', 'SZSE', 'BSE'],
            )
            if isinstance(exchanges, list) and exchanges:
                return [str(exchange).upper() for exchange in exchanges]
        except Exception:
            pass
        return ['SSE', 'SZSE', 'BSE']

    def _build_backfill_dates(self, start_date, end_date, exchanges: List[str]) -> List:
        """Build target dates for /backfill; ranges run only on trading days."""
        if start_date == end_date:
            return [start_date]

        dates = set()
        for exchange in exchanges:
            dates.update(DateUtils.get_trading_days_in_range(exchange, start_date, end_date))
        return sorted(day for day in dates if start_date <= day <= end_date)

    async def handle_backfill_factors_command(self, event) -> None:
        """处理 /backfill_factors 命令，走 DataManager 正式回补逻辑。"""
        chat_id = event.chat_id
        user_id = event.sender_id if hasattr(event, 'sender_id') else 'Unknown'
        command_text = event.text if hasattr(event, 'text') else '/backfill_factors'

        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{command_text}' | 用户ID: {user_id} | 聊天ID: {chat_id}")

        parts = command_text.strip().split()
        exchanges: List[str] = []
        mode = 'missing'
        valid_exchanges = {'SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE'}
        valid_modes = {'missing', 'full', 'resume'}

        for token in parts[1:]:
            normalized = token.upper()
            if normalized in valid_exchanges:
                exchanges.append(normalized)
                continue

            lowered = token.lower()
            if lowered in valid_modes:
                mode = 'missing' if lowered == 'resume' else lowered
                continue

            error_message = (
                "❌ *参数错误*\n\n"
                "用法: `/backfill_factors [交易所...] [missing|full]`\n\n"
                "*示例：*\n"
                "• `/backfill_factors`\n"
                "• `/backfill_factors SSE SZSE`\n"
                "• `/backfill_factors HKEX full`\n"
                "• `/backfill_factors HKEX missing`\n"
            )
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        exchanges = list(dict.fromkeys(exchanges))
        exchange_info = ', '.join(exchanges) if exchanges else '当前启用市场'
        start_message = (
            "⏳ *开始回填复权因子...*\n\n"
            f"交易所: `{exchange_info}`\n"
            f"模式: `{mode}`\n\n"
            "任务已在后台启动。执行完成后将在此发送报告。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')

        asyncio.create_task(
            self._run_backfill_factors_task(
                chat_id=chat_id,
                exchanges=exchanges or None,
                mode=mode,
            )
        )

    async def _run_backfill_factors_task(
        self,
        chat_id: int,
        exchanges: Optional[List[str]],
        mode: str,
    ) -> None:
        """后台执行正式复权因子回补任务。"""
        try:
            from data_manager import data_manager

            await data_manager.initialize()
            result = await data_manager.backfill_adjustment_factors(
                exchanges=exchanges,
                mode=mode,
            )
            totals = result.get('totals', {})
            by_exchange = result.get('by_exchange', {})
            exchange_lines = []
            for exchange, stats in by_exchange.items():
                exchange_lines.append(
                    f"{exchange}: scanned={stats.get('stocks_total', 0)}, "
                    f"synced={stats.get('synced_instruments', 0)}, "
                    f"skip_existing={stats.get('skipped_existing', 0)}, "
                    f"no_factors={stats.get('no_factors', 0)}, "
                    f"errors={stats.get('errors', 0)}"
                )

            summary = "\n".join(exchange_lines) if exchange_lines else "无可报告结果"
            msg = (
                "✅ *复权因子回填完成*\n\n"
                f"模式: `{result.get('mode', mode)}`\n"
                f"范围: `{result.get('start_date')}` ~ `{result.get('end_date')}`\n"
                f"交易所: `{', '.join(result.get('exchanges', exchanges or []))}`\n\n"
                "```text\n"
                f"total_stocks={totals.get('stocks_total', 0)}\n"
                f"synced_instruments={totals.get('synced_instruments', 0)}\n"
                f"saved_records={totals.get('saved_records', 0)}\n"
                f"skipped_existing={totals.get('skipped_existing', 0)}\n"
                f"no_factors={totals.get('no_factors', 0)}\n"
                f"errors={totals.get('errors', 0)}\n"
                f"{summary}\n"
                "```"
            )
            await self.task_manager.send_message(chat_id, msg, parse_mode='markdown')

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 执行复权因子回填异常: {e}")
            await self.task_manager.send_message(chat_id, f"❌ *复权因子回填异常*\n\n错误: {str(e)}", parse_mode='markdown')
        finally:
            try:
                from data_manager import data_manager
                await data_manager.close()
            except Exception as close_error:
                self.task_manager.logger.warning(
                f"[TaskManagerHandlers] 关闭 DataManager 失败: {close_error}"
                )

    async def handle_industry_standard_sync_command(self, event) -> None:
        """处理 /industry_standard_sync 命令，执行申万官方分类日更同步。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/industry_standard_sync'
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        parts = command_text.strip().split()
        allowed = {'force'}
        unknown = [token for token in parts[1:] if token.lower() not in allowed]
        if unknown:
            error_message = (
                "❌ *参数错误*\n\n"
                "用法: `/industry_standard_sync [force]`\n\n"
                "`force` 表示绕过 source manifest 短路，强制重新拉取官方文件。"
            )
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        force_refresh = any(token.lower() == 'force' for token in parts[1:])
        start_message = (
            "⏳ *申万官方分类日更同步已启动...*\n\n"
            f"force_refresh: `{force_refresh}`\n"
            "任务在后台执行，完成后会发送结果摘要。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        asyncio.create_task(
            self._run_industry_standard_sync_task(
                chat_id=chat_id,
                force_refresh=force_refresh,
            )
        )

    async def handle_industry_standard_rebuild_command(self, event) -> None:
        """处理 /industry_standard_rebuild 命令，执行申万官方分类全量重建。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/industry_standard_rebuild'
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        parts = command_text.strip().split()
        allowed = {'force', 'drop_source_files'}
        unknown = [token for token in parts[1:] if token.lower() not in allowed]
        if unknown:
            error_message = (
                "❌ *参数错误*\n\n"
                "用法: `/industry_standard_rebuild [force] [drop_source_files]`\n\n"
                "说明：该命令会清理并重建 strict Shenwan 行业标准层，不影响行情库或其他研究域。"
            )
            await self.task_manager.send_message(chat_id, error_message, parse_mode='markdown')
            return

        lowered = {token.lower() for token in parts[1:]}
        force_refresh = 'force' in lowered or not parts[1:]
        drop_source_files = 'drop_source_files' in lowered
        start_message = (
            "⏳ *申万官方分类全量重建已启动...*\n\n"
            "范围: `SSE,SZSE,BSE`\n"
            "drop_existing: `true`\n"
            f"drop_source_files: `{drop_source_files}`\n"
            f"force_refresh: `{force_refresh}`\n\n"
            "该任务会清理 strict Shenwan slice 后重建，完成后会发送结果摘要。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        asyncio.create_task(
            self._run_industry_standard_rebuild_task(
                chat_id=chat_id,
                force_refresh=force_refresh,
                drop_source_files=drop_source_files,
            )
        )

    async def handle_industry_index_analysis_sync_command(self, event) -> None:
        """处理 /industry_index_analysis_sync 命令，执行申万行业指数分析同步。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/industry_index_analysis_sync'
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        parts = command_text.strip().split()
        limit_per_type = None
        for token in parts[1:]:
            lowered = token.lower()
            if not lowered.startswith('limit='):
                await self.task_manager.send_message(
                    chat_id,
                    (
                        "❌ *参数错误*\n\n"
                        "用法: `/industry_index_analysis_sync [limit=N]`\n\n"
                        "`limit=N` 用于每个指数维度的小样本验证；不传则同步完整配置维度。"
                    ),
                    parse_mode='markdown',
                )
                return
            try:
                limit_per_type = max(1, int(lowered.split('=', 1)[1]))
            except ValueError:
                await self.task_manager.send_message(
                    chat_id,
                    "❌ *参数错误*\n\n`limit=N` 中 N 必须是正整数。",
                    parse_mode='markdown',
                )
                return

        start_message = (
            "⏳ *申万行业指数分析同步已启动...*\n\n"
            f"limit_per_type: `{limit_per_type}`\n"
            "该任务只写 `industry_index_analysis_daily`，不会改股票行业归属。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        asyncio.create_task(
            self._run_industry_index_analysis_sync_task(
                chat_id=chat_id,
                limit_per_type=limit_per_type,
            )
        )

    async def handle_industry_index_analysis_backfill_command(self, event) -> None:
        """处理 /industry_index_analysis_backfill 命令，执行申万行业指数分析历史回补。"""
        chat_id = event.chat_id
        command_text = (
            event.text if hasattr(event, 'text') else '/industry_index_analysis_backfill'
        )
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        args: Dict[str, str] = {}
        for token in command_text.strip().split()[1:]:
            if '=' not in token:
                await self.task_manager.send_message(
                    chat_id,
                    (
                        "❌ *参数错误*\n\n"
                        "用法: `/industry_index_analysis_backfill start=YYYY-MM-DD "
                        "end=YYYY-MM-DD [limit=N] [chunk=month|day|quarter|year|none]`"
                    ),
                    parse_mode='markdown',
                )
                return
            key, value = token.split('=', 1)
            args[key.strip().lower()] = value.strip()

        start_date = args.get('start')
        end_date = args.get('end')
        if not start_date or not end_date:
            await self.task_manager.send_message(
                chat_id,
                (
                    "❌ *参数错误*\n\n"
                    "必须提供 `start=YYYY-MM-DD` 和 `end=YYYY-MM-DD`。"
                ),
                parse_mode='markdown',
            )
            return

        limit_per_type = None
        if args.get('limit'):
            try:
                limit_per_type = max(1, int(args['limit']))
            except ValueError:
                await self.task_manager.send_message(
                    chat_id,
                    "❌ *参数错误*\n\n`limit=N` 中 N 必须是正整数。",
                    parse_mode='markdown',
                )
                return

        chunk_frequency = args.get('chunk', 'month').lower()
        if chunk_frequency not in {'day', 'month', 'quarter', 'year', 'none'}:
            await self.task_manager.send_message(
                chat_id,
                (
                    "❌ *参数错误*\n\n"
                    "`chunk` 必须是 `day|month|quarter|year|none` 之一。"
                ),
                parse_mode='markdown',
            )
            return

        start_message = (
            "⏳ *申万行业指数分析历史回补已启动...*\n\n"
            f"start: `{start_date}`\n"
            f"end: `{end_date}`\n"
            f"limit_per_type: `{limit_per_type}`\n"
            f"chunk_frequency: `{chunk_frequency}`\n"
            "该任务只写 `industry_index_analysis_daily`，不会改股票行业归属。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        asyncio.create_task(
            self._run_industry_index_analysis_backfill_task(
                chat_id=chat_id,
                start_date=start_date,
                end_date=end_date,
                limit_per_type=limit_per_type,
                chunk_frequency=chunk_frequency,
            )
        )

    async def handle_futures_calendar_backfill_command(self, event) -> None:
        """处理 /futures_calendar_backfill 命令，执行期货官方交易日历手工回填。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/futures_calendar_backfill'
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        try:
            tokens = shlex.split(command_text.strip())
        except ValueError as exc:
            await self.task_manager.send_message(
                chat_id,
                f"❌ *参数错误*\n\n命令解析失败: `{exc}`",
                parse_mode='markdown',
            )
            return
        args: Dict[str, str] = {}
        flags = set()
        for token in tokens[1:]:
            if '=' in token:
                key, value = token.split('=', 1)
                args[key.strip().lower()] = value.strip()
            else:
                flags.add(token.strip().lower())

        allowed_keys = {
            'exchange',
            'exchanges',
            'scope',
            'scope_id',
            'categories',
            'instrument_ids',
            'series_ids',
            'series_types',
            'start',
            'start_date',
            'end',
            'end_date',
            'max_days',
        }
        allowed_flags = {'dry_run', 'write'}
        unknown_keys = sorted(set(args) - allowed_keys)
        unknown_flags = sorted(flags - allowed_flags)
        if unknown_keys or unknown_flags:
            await self._send_futures_calendar_backfill_usage(chat_id)
            return

        start_date = args.get('start') or args.get('start_date')
        end_date = args.get('end') or args.get('end_date')
        if not start_date or not end_date:
            await self._send_futures_calendar_backfill_usage(chat_id)
            return
        if self._parse_date_arg(start_date) is None or self._parse_date_arg(end_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start/end` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if start_date > end_date:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 不能晚于 `end`。",
                parse_mode='markdown',
            )
            return
        if 'dry_run' in flags and 'write' in flags:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`dry_run` 和 `write` 只能二选一。",
                parse_mode='markdown',
            )
            return

        max_days = None
        if args.get('max_days'):
            try:
                max_days = max(1, int(args['max_days']))
            except ValueError:
                await self.task_manager.send_message(
                    chat_id,
                    "❌ *参数错误*\n\n`max_days=N` 中 N 必须是正整数。",
                    parse_mode='markdown',
                )
                return

        exchanges = self._split_csv_arg(args.get('exchanges') or args.get('exchange'))
        categories = self._split_csv_arg(args.get('categories'))
        instrument_ids = self._split_csv_arg(args.get('instrument_ids'))
        series_ids = self._split_csv_arg(args.get('series_ids'))
        series_types = self._split_csv_arg(args.get('series_types'))
        scope_id = args.get('scope_id') or args.get('scope')
        dry_run = 'write' not in flags

        start_message = (
            "⏳ *期货官方交易日历手工回填已启动...*\n\n"
            f"scope_id: `{scope_id}`\n"
            f"exchanges: `{','.join(exchanges or []) or None}`\n"
            f"categories: `{','.join(categories or []) or None}`\n"
            f"start: `{start_date}`\n"
            f"end: `{end_date}`\n"
            f"dry_run: `{dry_run}`\n"
            f"max_days: `{max_days}`\n\n"
            "说明：该任务只维护 `data/futures.db` 的交易日历，不下载行情价格。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        await self._run_futures_calendar_backfill_task(
            chat_id=chat_id,
            scope_id=scope_id,
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

    async def _send_futures_calendar_backfill_usage(self, chat_id: int) -> None:
        await self.task_manager.send_message(
            chat_id,
            (
                "❌ *参数错误*\n\n"
                "用法: `/futures_calendar_backfill exchange=SHFE start=YYYY-MM-DD "
                "end=YYYY-MM-DD [dry_run|write] [max_days=N]`\n\n"
                "可选参数: `scope=gfex_all`、`categories=all`、"
                "`instrument_ids=CNF.LC.GFEX`、`series_ids=CNF.LC.GFEX.main`。\n\n"
                "示例:\n"
                "• `/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
                "• `/futures_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 write max_days=10`"
            ),
            parse_mode='markdown',
        )

    async def handle_futures_master_governance_command(self, event) -> None:
        """处理 /futures_master_governance 命令，执行期货主数据治理。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/futures_master_governance'
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        try:
            tokens = shlex.split(command_text.strip())
        except ValueError as exc:
            await self.task_manager.send_message(
                chat_id,
                f"❌ *参数错误*\n\n命令解析失败: `{exc}`",
                parse_mode='markdown',
            )
            return
        args: Dict[str, str] = {}
        flags = set()
        for token in tokens[1:]:
            if '=' in token:
                key, value = token.split('=', 1)
                args[key.strip().lower()] = value.strip()
            else:
                flags.add(token.strip().lower())

        allowed_keys = {
            'exchange',
            'exchanges',
            'scope',
            'scope_id',
            'categories',
            'instrument_ids',
            'series_ids',
            'series_types',
            'start',
            'start_date',
            'end',
            'end_date',
            'max_days',
        }
        allowed_flags = {'dry_run', 'write'}
        unknown_keys = sorted(set(args) - allowed_keys)
        unknown_flags = sorted(flags - allowed_flags)
        if unknown_keys or unknown_flags:
            await self._send_futures_master_governance_usage(chat_id)
            return
        if 'dry_run' in flags and 'write' in flags:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`dry_run` 和 `write` 只能二选一。",
                parse_mode='markdown',
            )
            return

        start_date = args.get('start') or args.get('start_date')
        end_date = args.get('end') or args.get('end_date')
        if start_date and self._parse_date_arg(start_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if end_date and self._parse_date_arg(end_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`end` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if start_date and end_date and start_date > end_date:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 不能晚于 `end`。",
                parse_mode='markdown',
            )
            return

        max_days = None
        if args.get('max_days'):
            try:
                max_days = max(1, int(args['max_days']))
            except ValueError:
                await self.task_manager.send_message(
                    chat_id,
                    "❌ *参数错误*\n\n`max_days=N` 中 N 必须是正整数。",
                    parse_mode='markdown',
                )
                return

        exchanges = self._split_csv_arg(args.get('exchanges') or args.get('exchange'))
        categories = self._split_csv_arg(args.get('categories'))
        instrument_ids = self._split_csv_arg(args.get('instrument_ids'))
        series_ids = self._split_csv_arg(args.get('series_ids'))
        series_types = self._split_csv_arg(args.get('series_types'))
        scope_id = args.get('scope_id') or args.get('scope')
        dry_run = 'write' not in flags

        start_message = (
            "⏳ *期货主数据治理已启动...*\n\n"
            f"scope_id: `{scope_id}`\n"
            f"exchanges: `{','.join(exchanges or []) or None}`\n"
            f"categories: `{','.join(categories or []) or None}`\n"
            f"start: `{start_date}`\n"
            f"end: `{end_date}`\n"
            f"dry_run: `{dry_run}`\n"
            f"max_days: `{max_days}`\n\n"
            "说明：当前仅支持 GFEX 官方日行情合约发现，写入 `data/futures.db` 主数据表。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        await self._run_futures_master_governance_task(
            chat_id=chat_id,
            scope_id=scope_id,
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

    async def _send_futures_master_governance_usage(self, chat_id: int) -> None:
        await self.task_manager.send_message(
            chat_id,
            (
                "❌ *参数错误*\n\n"
                "用法: `/futures_master_governance exchange=GFEX [start=YYYY-MM-DD] "
                "[end=YYYY-MM-DD] [dry_run|write] [max_days=N]`\n\n"
                "可选参数: `scope=gfex_all`、`categories=all`、"
                "`instrument_ids=CNF.LC.GFEX`、`series_ids=CNF.LC.GFEX.main`。\n\n"
                "示例:\n"
                "• `/futures_master_governance exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
                "• `/run futures_master_governance exchange=GFEX start=2022-12-22 end=2022-12-31 write max_days=10`"
            ),
            parse_mode='markdown',
        )

    async def handle_futures_master_discovery_governance_command(self, event) -> None:
        """处理 /futures_master_discovery_governance 命令，执行期货主数据发现治理。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/futures_master_discovery_governance'
        self.task_manager.logger.info(
            f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}"
        )

        try:
            tokens = shlex.split(command_text.strip())
        except ValueError as exc:
            await self.task_manager.send_message(
                chat_id,
                f"❌ *参数错误*\n\n命令解析失败: `{exc}`",
                parse_mode='markdown',
            )
            return
        args: Dict[str, str] = {}
        flags = set()
        for token in tokens[1:]:
            if '=' in token:
                key, value = token.split('=', 1)
                args[key.strip().lower()] = value.strip()
            else:
                flags.add(token.strip().lower())

        allowed_keys = {
            'exchange',
            'exchanges',
            'scope',
            'scope_id',
            'categories',
            'instrument_ids',
            'series_ids',
            'series_types',
            'start',
            'start_date',
            'end',
            'end_date',
            'max_days',
        }
        allowed_flags = {'dry_run', 'write'}
        unknown_keys = sorted(set(args) - allowed_keys)
        unknown_flags = sorted(flags - allowed_flags)
        if unknown_keys or unknown_flags:
            await self._send_futures_master_discovery_governance_usage(chat_id)
            return
        if 'dry_run' in flags and 'write' in flags:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`dry_run` 和 `write` 只能二选一。",
                parse_mode='markdown',
            )
            return

        start_date = args.get('start') or args.get('start_date')
        end_date = args.get('end') or args.get('end_date')
        if start_date and self._parse_date_arg(start_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if end_date and self._parse_date_arg(end_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`end` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if start_date and end_date and start_date > end_date:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 不能晚于 `end`。",
                parse_mode='markdown',
            )
            return

        max_days = None
        if args.get('max_days'):
            try:
                max_days = max(1, int(args['max_days']))
            except ValueError:
                await self.task_manager.send_message(
                    chat_id,
                    "❌ *参数错误*\n\n`max_days=N` 中 N 必须是正整数。",
                    parse_mode='markdown',
                )
                return

        exchanges = self._split_csv_arg(args.get('exchanges') or args.get('exchange'))
        categories = self._split_csv_arg(args.get('categories'))
        instrument_ids = self._split_csv_arg(args.get('instrument_ids'))
        series_ids = self._split_csv_arg(args.get('series_ids'))
        series_types = self._split_csv_arg(args.get('series_types'))
        scope_id = args.get('scope_id') or args.get('scope')
        dry_run = 'write' not in flags

        start_message = (
            "⏳ *期货主数据发现治理已启动...*\n\n"
            f"scope_id: `{scope_id}`\n"
            f"exchanges: `{','.join(exchanges or []) or None}`\n"
            f"categories: `{','.join(categories or []) or None}`\n"
            f"start: `{start_date}`\n"
            f"end: `{end_date}`\n"
            f"dry_run: `{dry_run}`\n"
            f"max_days: `{max_days}`\n\n"
            "说明：该任务发现未知交易品种，写入候选主数据表；高置信候选按配置 promotion。"
        )
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        await self._run_futures_master_discovery_governance_task(
            chat_id=chat_id,
            scope_id=scope_id,
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

    async def _send_futures_master_discovery_governance_usage(self, chat_id: int) -> None:
        await self.task_manager.send_message(
            chat_id,
            (
                "❌ *参数错误*\n\n"
                "用法: `/futures_master_discovery_governance exchange=GFEX [start=YYYY-MM-DD] "
                "[end=YYYY-MM-DD] [dry_run|write] [max_days=N]`\n\n"
                "可选参数: `scope=gfex_all`、`categories=all`、"
                "`instrument_ids=CNF.LC.GFEX`、`series_ids=CNF.LC.GFEX.main`。\n\n"
                "示例:\n"
                "• `/futures_master_discovery_governance exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10`\n"
                "• `/run futures_master_discovery_governance exchange=GFEX start=2022-12-22 end=2022-12-31 write max_days=10`"
            ),
            parse_mode='markdown',
        )

    async def handle_futures_market_data_command(self, event, *, job_id: str) -> None:
        """处理期货行情同步/回补命令。"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else f'/{job_id}'
        self.task_manager.logger.info(
            "[TaskManagerHandlers] 收到期货行情命令: '%s' | 聊天ID: %s",
            command_text,
            chat_id,
        )
        try:
            tokens = shlex.split(command_text.strip())
        except ValueError as exc:
            await self.task_manager.send_message(
                chat_id,
                f"❌ *参数错误*\n\n命令解析失败: `{exc}`",
                parse_mode='markdown',
            )
            return

        args: Dict[str, str] = {}
        flags = set()
        for token in tokens[1:]:
            if '=' in token:
                key, value = token.split('=', 1)
                args[key.strip().lower()] = value.strip()
            else:
                flags.add(token.strip().lower())

        allowed_keys = {
            'exchange',
            'exchanges',
            'scope',
            'scope_id',
            'categories',
            'instrument_ids',
            'series_ids',
            'series_types',
            'start',
            'start_date',
            'end',
            'end_date',
            'mode',
            'requires_trading_day_governance',
            'requires_master_data_governance',
            'master_governance_max_days',
        }
        allowed_flags = {
            'dry_run',
            'write',
            'skip_trading_day_governance',
            'requires_master_data_governance',
            'skip_master_data_governance',
        }
        if sorted(set(args) - allowed_keys) or sorted(flags - allowed_flags):
            await self._send_futures_market_data_usage(chat_id, job_id=job_id)
            return
        if 'dry_run' in flags and 'write' in flags:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`dry_run` 和 `write` 只能二选一。",
                parse_mode='markdown',
            )
            return

        start_date = args.get('start') or args.get('start_date')
        end_date = args.get('end') or args.get('end_date')
        if job_id == 'futures_market_data_backfill' and (not start_date or not end_date):
            await self._send_futures_market_data_usage(chat_id, job_id=job_id)
            return
        if start_date and self._parse_date_arg(start_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if end_date and self._parse_date_arg(end_date) is None:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`end` 必须使用 `YYYY-MM-DD` 格式。",
                parse_mode='markdown',
            )
            return
        if start_date and end_date and start_date > end_date:
            await self.task_manager.send_message(
                chat_id,
                "❌ *参数错误*\n\n`start` 不能晚于 `end`。",
                parse_mode='markdown',
            )
            return

        master_governance_max_days = None
        if args.get('master_governance_max_days'):
            try:
                master_governance_max_days = max(1, int(args['master_governance_max_days']))
            except ValueError:
                await self.task_manager.send_message(
                    chat_id,
                    "❌ *参数错误*\n\n`master_governance_max_days=N` 中 N 必须是正整数。",
                    parse_mode='markdown',
                )
                return

        mode = args.get('mode') or 'direct'
        dry_run = 'write' not in flags
        requires_trading_day_governance = (
            str(args.get('requires_trading_day_governance', 'true')).lower() not in {'0', 'false', 'no'}
            and 'skip_trading_day_governance' not in flags
        )
        requires_master_data_governance = (
            str(args.get('requires_master_data_governance', 'false')).lower() in {'1', 'true', 'yes'}
            or 'requires_master_data_governance' in flags
        ) and 'skip_master_data_governance' not in flags

        await self._run_futures_market_data_task(
            chat_id=chat_id,
            job_id=job_id,
            scope_id=args.get('scope_id') or args.get('scope'),
            exchanges=self._split_csv_arg(args.get('exchanges') or args.get('exchange')),
            categories=self._split_csv_arg(args.get('categories')),
            instrument_ids=self._split_csv_arg(args.get('instrument_ids')),
            series_ids=self._split_csv_arg(args.get('series_ids')),
            series_types=self._split_csv_arg(args.get('series_types')),
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            dry_run=dry_run,
            requires_trading_day_governance=requires_trading_day_governance,
            requires_master_data_governance=requires_master_data_governance,
            master_governance_max_days=master_governance_max_days,
        )

    async def _send_futures_market_data_usage(self, chat_id: int, *, job_id: str) -> None:
        await self.task_manager.send_message(
            chat_id,
            (
                "❌ *参数错误*\n\n"
                f"用法: `/{job_id} exchange=GFEX start=YYYY-MM-DD end=YYYY-MM-DD [dry_run|write]`\n\n"
                "可选参数: `scope=gfex_all`、`categories=all`、"
                "`instrument_ids=CNF.LC.GFEX`、`series_ids=CNF.LC.GFEX.main`、"
                "`mode=direct`、`requires_master_data_governance`。\n"
                "未显式指定 `write` 时默认按 dry-run 执行；正式落库必须带 `write`。\n\n"
                "示例:\n"
                f"• `/run {job_id} exchange=GFEX start=2026-06-01 end=2026-06-10 dry_run`\n"
                f"• `/run {job_id} scope=gfex_all start=2026-06-01 end=2026-06-10 write`"
            ),
            parse_mode='markdown',
        )

    @staticmethod
    def _split_csv_arg(value: Optional[str]) -> Optional[List[str]]:
        if not value:
            return None
        items = [item.strip() for item in str(value).replace(';', ',').split(',') if item.strip()]
        return items or None

    async def _run_futures_market_data_task(
        self,
        *,
        chat_id: int,
        job_id: str,
        scope_id: Optional[str],
        exchanges: Optional[List[str]],
        categories: Optional[List[str]],
        instrument_ids: Optional[List[str]],
        series_ids: Optional[List[str]],
        series_types: Optional[List[str]],
        start_date: Optional[str],
        end_date: Optional[str],
        mode: str,
        dry_run: bool,
        requires_trading_day_governance: bool,
        requires_master_data_governance: bool,
        master_governance_max_days: Optional[int],
    ) -> None:
        try:
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 执行期货行情任务: job_id=%s scope_id=%s exchanges=%s "
                "categories=%s start=%s end=%s mode=%s dry_run=%s",
                job_id,
                scope_id,
                exchanges,
                categories,
                start_date,
                end_date,
                mode,
                dry_run,
            )
            if requires_master_data_governance:
                from data_manager import data_manager

                master_result = await data_manager.run_futures_master_governance(
                    scope_id=scope_id,
                    scope_ids=None,
                    exchanges=exchanges,
                    categories=categories,
                    instrument_ids=instrument_ids,
                    series_ids=series_ids,
                    series_types=series_types,
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=dry_run,
                    max_days=master_governance_max_days,
                )
                master_status = str(master_result.get('status') or '').lower()
                if master_status not in {'success', 'warning'}:
                    self.task_manager.logger.warning(
                        "[TaskManagerHandlers] 期货行情前置主数据治理未通过: status=%s result=%s",
                        master_status,
                        master_result,
                    )
                    await self.task_manager.send_message(
                        chat_id,
                        (
                            "❌ *期货行情任务失败*\n\n"
                            f"task: `{job_id}`\n"
                            "reason: `master_governance_failed`\n"
                            f"master_status: `{master_status or 'unknown'}`"
                        ),
                        parse_mode='markdown',
                    )
                    return

            from data_manager import data_manager

            result_payload = await data_manager.run_futures_market_data_sync(
                scope_id=scope_id,
                scope_ids=None,
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
            result_status = str(result_payload.get('status') or '').lower()
            result = result_status in {'success', 'warning', 'degraded', 'partial'}
            totals = result_payload.get('totals') or {}
            source_selection = result_payload.get('source_selection') or {}
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 期货行情任务完成: job_id=%s success=%s status=%s "
                "fetched_rows=%s would_write=%s failed=%s official_success=%s official_failed=%s",
                job_id,
                result,
                result_status,
                totals.get('fetched_rows'),
                totals.get('would_write_price_bars'),
                totals.get('failed'),
                source_selection.get('official_success'),
                source_selection.get('official_failed'),
            )
            status_text = '成功' if result else '失败'
            await self.task_manager.send_message(
                chat_id,
                (
                    f"{'✅' if result else '❌'} *期货行情任务{status_text}*\n\n"
                    f"task: `{job_id}`\n"
                    f"status: `{result_status or 'unknown'}`\n"
                    f"exchange/scope: `{','.join(exchanges or []) or scope_id or 'configured'}`\n"
                    f"start: `{start_date}`\n"
                    f"end: `{end_date}`\n"
                    f"dry_run: `{dry_run}`\n"
                    f"fetched_rows: `{totals.get('fetched_rows', 0)}`\n"
                    f"would_write_price_bars: `{totals.get('would_write_price_bars', 0)}`\n"
                    f"failed: `{totals.get('failed', 0)}`\n"
                    f"official_success: `{source_selection.get('official_success', 0)}`\n"
                    f"official_failed: `{source_selection.get('official_failed', 0)}`\n\n"
                    "详细结果以任务日志为准。"
                ),
                parse_mode='markdown',
            )
            try:
                from scheduler.tasks import _format_futures_market_data_scheduler_reports

                for report_message in _format_futures_market_data_scheduler_reports(result_payload):
                    await self.task_manager.send_message(
                        chat_id,
                        report_message,
                        parse_mode='markdown',
                    )
            except Exception as report_exc:
                self.task_manager.logger.warning(
                    "[TaskManagerHandlers] 期货行情详细报告发送失败: %s",
                    report_exc,
                )
        except Exception as exc:
            self.task_manager.logger.error(
                "[TaskManagerHandlers] 期货行情任务异常: %s",
                exc,
            )
            await self.task_manager.send_message(
                chat_id,
                f"❌ *期货行情任务异常*\n\n错误: `{exc}`",
                parse_mode='markdown',
            )

    async def _run_futures_calendar_backfill_task(
        self,
        *,
        chat_id: int,
        scope_id: Optional[str],
        exchanges: Optional[List[str]],
        categories: Optional[List[str]],
        instrument_ids: Optional[List[str]],
        series_ids: Optional[List[str]],
        series_types: Optional[List[str]],
        start_date: str,
        end_date: str,
        dry_run: bool,
        max_days: Optional[int],
    ) -> None:
        try:
            scheduler = self.task_manager.task_scheduler
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 执行期货交易日历手工回填: "
                "scope_id=%s exchanges=%s categories=%s start=%s end=%s dry_run=%s max_days=%s",
                scope_id,
                exchanges,
                categories,
                start_date,
                end_date,
                dry_run,
                max_days,
            )
            result = await scheduler.execute_job_direct(
                'futures_official_calendar_backfill',
                parameters={
                    'scope_id': scope_id,
                    'scope_ids': None,
                    'exchanges': exchanges,
                    'categories': categories,
                    'instrument_ids': instrument_ids,
                    'series_ids': series_ids,
                    'series_types': series_types,
                    'start_date': start_date,
                    'end_date': end_date,
                    'dry_run': dry_run,
                    'max_days': max_days,
                },
                include_dependencies=True,
            )
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 期货交易日历手工回填完成: success=%s",
                result,
            )
            status_text = '成功' if result else '失败'
            await self.task_manager.send_message(
                chat_id,
                (
                    f"{'✅' if result else '❌'} *期货交易日历手工回填{status_text}*\n\n"
                    f"exchange/scope: `{','.join(exchanges or []) or scope_id or 'configured'}`\n"
                    f"start: `{start_date}`\n"
                    f"end: `{end_date}`\n"
                    f"dry_run: `{dry_run}`\n"
                    f"max_days: `{max_days}`\n\n"
                    "详细结果以任务报告和日志为准。"
                ),
                parse_mode='markdown',
            )
        except Exception as exc:
            self.task_manager.logger.error(
                f"[TaskManagerHandlers] 期货交易日历手工回填异常: {exc}"
            )
            await self.task_manager.send_message(
                chat_id,
                f"❌ *期货交易日历手工回填异常*\n\n错误: `{exc}`",
                parse_mode='markdown',
            )

    async def _run_futures_master_governance_task(
        self,
        *,
        chat_id: int,
        scope_id: Optional[str],
        exchanges: Optional[List[str]],
        categories: Optional[List[str]],
        instrument_ids: Optional[List[str]],
        series_ids: Optional[List[str]],
        series_types: Optional[List[str]],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
        max_days: Optional[int],
    ) -> None:
        try:
            scheduler = self.task_manager.task_scheduler
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 执行期货主数据治理: "
                "scope_id=%s exchanges=%s categories=%s start=%s end=%s dry_run=%s max_days=%s",
                scope_id,
                exchanges,
                categories,
                start_date,
                end_date,
                dry_run,
                max_days,
            )
            result = await scheduler.execute_job_direct(
                'futures_master_governance',
                parameters={
                    'scope_id': scope_id,
                    'scope_ids': None,
                    'exchanges': exchanges,
                    'categories': categories,
                    'instrument_ids': instrument_ids,
                    'series_ids': series_ids,
                    'series_types': series_types,
                    'start_date': start_date,
                    'end_date': end_date,
                    'dry_run': dry_run,
                    'max_days': max_days,
                },
                include_dependencies=True,
            )
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 期货主数据治理完成: success=%s",
                result,
            )
            status_text = '成功' if result else '失败'
            await self.task_manager.send_message(
                chat_id,
                (
                    f"{'✅' if result else '❌'} *期货主数据治理{status_text}*\n\n"
                    f"exchange/scope: `{','.join(exchanges or []) or scope_id or 'configured'}`\n"
                    f"start: `{start_date}`\n"
                    f"end: `{end_date}`\n"
                    f"dry_run: `{dry_run}`\n"
                    f"max_days: `{max_days}`\n\n"
                    "详细结果以任务报告和日志为准。"
                ),
                parse_mode='markdown',
            )
        except Exception as exc:
            self.task_manager.logger.error(
                f"[TaskManagerHandlers] 期货主数据治理异常: {exc}"
            )
            await self.task_manager.send_message(
                chat_id,
                f"❌ *期货主数据治理异常*\n\n错误: `{exc}`",
                parse_mode='markdown',
            )

    async def _run_futures_master_discovery_governance_task(
        self,
        *,
        chat_id: int,
        scope_id: Optional[str],
        exchanges: Optional[List[str]],
        categories: Optional[List[str]],
        instrument_ids: Optional[List[str]],
        series_ids: Optional[List[str]],
        series_types: Optional[List[str]],
        start_date: Optional[str],
        end_date: Optional[str],
        dry_run: bool,
        max_days: Optional[int],
    ) -> None:
        try:
            scheduler = self.task_manager.task_scheduler
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 执行期货主数据发现治理: "
                "scope_id=%s exchanges=%s categories=%s start=%s end=%s dry_run=%s max_days=%s",
                scope_id,
                exchanges,
                categories,
                start_date,
                end_date,
                dry_run,
                max_days,
            )
            result = await scheduler.execute_job_direct(
                'futures_master_discovery_governance',
                parameters={
                    'scope_id': scope_id,
                    'scope_ids': None,
                    'exchanges': exchanges,
                    'categories': categories,
                    'instrument_ids': instrument_ids,
                    'series_ids': series_ids,
                    'series_types': series_types,
                    'start_date': start_date,
                    'end_date': end_date,
                    'dry_run': dry_run,
                    'max_days': max_days,
                },
                include_dependencies=True,
            )
            self.task_manager.logger.info(
                "[TaskManagerHandlers] 期货主数据发现治理完成: success=%s",
                result,
            )
            status_text = '成功' if result else '失败'
            await self.task_manager.send_message(
                chat_id,
                (
                    f"{'✅' if result else '❌'} *期货主数据发现治理{status_text}*\n\n"
                    f"exchange/scope: `{','.join(exchanges or []) or scope_id or 'configured'}`\n"
                    f"start: `{start_date}`\n"
                    f"end: `{end_date}`\n"
                    f"dry_run: `{dry_run}`\n"
                    f"max_days: `{max_days}`\n\n"
                    "详细结果以任务报告和日志为准。"
                ),
                parse_mode='markdown',
            )
        except Exception as exc:
            self.task_manager.logger.error(
                f"[TaskManagerHandlers] 期货主数据发现治理异常: {exc}"
            )
            await self.task_manager.send_message(
                chat_id,
                f"❌ *期货主数据发现治理异常*\n\n错误: `{exc}`",
                parse_mode='markdown',
            )

    async def _run_industry_standard_sync_task(
        self,
        chat_id: int,
        force_refresh: bool,
    ) -> None:
        """后台执行申万官方分类日更同步。"""
        try:
            from data_manager import data_manager

            if data_manager.research_storage is None:
                await data_manager.initialize(include_data_sources=False, load_progress=False)

            result = await data_manager.run_industry_standard_sync(
                exchanges=['SSE', 'SZSE', 'BSE'],
                budget_mode='availability_first',
                allow_paid_proxy=True,
                force_component_refresh=force_refresh,
            )
            readiness = await data_manager.get_research_industry_standard_readiness()
            sync_status = result.get('status')
            msg = self._format_industry_standard_result(
                title='申万官方分类日更同步完成',
                result=result,
                readiness=readiness,
            )
            await self.task_manager.send_message(
                chat_id,
                msg,
                parse_mode='markdown',
            )
            self.task_manager.logger.info(
                f"[TaskManagerHandlers] 申万官方分类日更同步完成: {sync_status}"
            )

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 申万官方分类日更同步异常: {e}")
            await self.task_manager.send_message(
                chat_id,
                f"❌ *申万官方分类日更同步异常*\n\n错误: `{str(e)}`",
                parse_mode='markdown',
            )

    async def _run_industry_standard_rebuild_task(
        self,
        chat_id: int,
        force_refresh: bool,
        drop_source_files: bool,
    ) -> None:
        """后台执行申万官方分类全量重建。"""
        try:
            from data_manager import data_manager

            if data_manager.research_storage is None:
                await data_manager.initialize(include_data_sources=False, load_progress=False)

            result = await data_manager.rebuild_official_industry_standard(
                exchanges=['SSE', 'SZSE', 'BSE'],
                budget_mode='availability_first',
                allow_paid_proxy=True,
                drop_existing=True,
                drop_source_files=drop_source_files,
                force_refresh=force_refresh,
            )
            msg = self._format_industry_standard_result(
                title='申万官方分类全量重建完成',
                result=result.get('sync') or result,
                readiness=result.get('readiness') or {},
                table_counts=(result.get('table_counts') or {}).get('after') or {},
            )
            await self.task_manager.send_message(
                chat_id,
                msg,
                parse_mode='markdown',
            )
            self.task_manager.logger.info(
                f"[TaskManagerHandlers] 申万官方分类全量重建完成: {result.get('status')}"
            )

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 申万官方分类全量重建异常: {e}")
            await self.task_manager.send_message(
                chat_id,
                f"❌ *申万官方分类全量重建异常*\n\n错误: `{str(e)}`",
                parse_mode='markdown',
            )

    async def _run_industry_index_analysis_sync_task(
        self,
        chat_id: int,
        limit_per_type: Optional[int],
    ) -> None:
        """后台执行申万行业指数分析日频指标同步。"""
        try:
            from data_manager import data_manager

            if data_manager.research_storage is None:
                await data_manager.initialize(include_data_sources=False, load_progress=False)

            result = await data_manager.run_industry_index_analysis_sync(
                limit_per_type=limit_per_type,
            )
            msg = self._format_industry_index_analysis_result(result)
            await self.task_manager.send_message(
                chat_id,
                msg,
                parse_mode='markdown',
            )
            self.task_manager.logger.info(
                f"[TaskManagerHandlers] 申万行业指数分析同步完成: {result.get('status')}"
            )

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 申万行业指数分析同步异常: {e}")
            await self.task_manager.send_message(
                chat_id,
                f"❌ *申万行业指数分析同步异常*\n\n错误: `{str(e)}`",
                parse_mode='markdown',
            )

    async def _run_industry_index_analysis_backfill_task(
        self,
        chat_id: int,
        start_date: str,
        end_date: str,
        limit_per_type: Optional[int],
        chunk_frequency: str,
    ) -> None:
        """后台执行申万行业指数分析历史回补。"""
        try:
            from data_manager import data_manager

            if data_manager.research_storage is None:
                await data_manager.initialize(include_data_sources=False, load_progress=False)

            result = await data_manager.run_industry_index_analysis_backfill(
                start_date=start_date,
                end_date=end_date,
                limit_per_type=limit_per_type,
                chunk_frequency=chunk_frequency,
                split_index_types=True,
            )
            msg = self._format_industry_index_analysis_result(result)
            await self.task_manager.send_message(
                chat_id,
                msg,
                parse_mode='markdown',
            )
            self.task_manager.logger.info(
                f"[TaskManagerHandlers] 申万行业指数分析历史回补完成: {result.get('status')}"
            )

        except Exception as e:
            self.task_manager.logger.error(
                f"[TaskManagerHandlers] 申万行业指数分析历史回补异常: {e}"
            )
            await self.task_manager.send_message(
                chat_id,
                f"❌ *申万行业指数分析历史回补异常*\n\n错误: `{str(e)}`",
                parse_mode='markdown',
            )

    @staticmethod
    def _format_industry_index_analysis_result(result: Dict[str, Any]) -> str:
        """格式化申万行业指数分析同步结果。"""
        if result.get('operation') == 'history_backfill_chunked':
            failures = result.get('failures') or []
            failure_lines = [
                (
                    f"{item.get('start_date')}~{item.get('end_date')} "
                    f"{','.join(item.get('index_types') or [])}: {item.get('status')}"
                )
                for item in failures[:10]
            ]
            failure_text = "\n".join(failure_lines) if failure_lines else "无失败分块"
            icon = "✅" if result.get('status') == 'success' else "⚠️"
            return (
                f"{icon} *申万行业指数分析历史回补完成*\n\n"
                f"status: `{result.get('status')}`\n"
                f"operation: `{result.get('operation')}`\n"
                f"date_range: `{result.get('start_date')}` ~ `{result.get('end_date')}`\n"
                f"chunk_frequency: `{result.get('chunk_frequency')}`\n"
                f"chunks_total: `{result.get('chunks_total', 0)}`\n"
                f"chunks_failed: `{result.get('chunks_failed', 0)}`\n"
                f"rows_written: `{result.get('rows_written', 0)}`\n\n"
                "```text\n"
                f"{failure_text}\n"
                "```"
            )

        summary = result.get('summary') or {}
        coverage = result.get('coverage') or {}
        type_counts = summary.get('index_type_counts') or {}
        lines = [
            f"{index_type}: rows={counts.get('rows', 0)}, codes={counts.get('codes', 0)}"
            for index_type, counts in type_counts.items()
        ]
        coverage_counts = coverage.get('index_type_counts') or {}
        coverage_lines = [
            (
                f"{index_type}: fetched_rows={counts.get('rows', 0)}, "
                f"dates={counts.get('trade_dates', 0)}, "
                f"missing={counts.get('missing_metrics', {})}"
            )
            for index_type, counts in coverage_counts.items()
        ]
        details = "\n".join(lines) if lines else result.get('reason') or "无维度明细"
        coverage_details = "\n".join(coverage_lines) if coverage_lines else "无本次覆盖率明细"
        icon = "✅" if result.get('status') == 'success' else "⚠️"
        return (
            f"{icon} *申万行业指数分析同步完成*\n\n"
            f"status: `{result.get('status')}`\n"
            f"operation: `{result.get('operation')}`\n"
            f"rows_written: `{result.get('rows_written', 0)}`\n"
            f"latest_trade_date: `{summary.get('latest_trade_date')}`\n"
            f"distinct_index_codes: `{summary.get('distinct_index_codes', 0)}`\n\n"
            "```text\n"
            f"{details}\n"
            "\n本次覆盖:\n"
            f"{coverage_details}\n"
            "```"
        )

    @staticmethod
    def _format_industry_standard_result(
        *,
        title: str,
        result: Dict[str, Any],
        readiness: Dict[str, Any],
        table_counts: Optional[Dict[str, Any]] = None,
    ) -> str:
        """格式化申万官方分类同步/重建结果。"""
        exchange_lines = []
        for item in result.get('exchanges', []) or []:
            exchange_lines.append(
                f"{item.get('exchange')}: status={item.get('status')}, "
                f"memberships={item.get('memberships_written', 0)}, "
                f"official={item.get('official_classifications_written', 0)}"
            )
        exchanges_text = "\n".join(exchange_lines) if exchange_lines else "无交易所明细"
        coverage_lines = []
        for item in readiness.get('exchange_coverage', []) or []:
            coverage_lines.append(
                f"{item.get('exchange')}: "
                f"{item.get('authoritative_memberships', 0)}/"
                f"{item.get('target_instruments', 0)} "
                f"({float(item.get('coverage_ratio', 0.0)):.2%})"
            )
        coverage_text = "\n".join(coverage_lines) if coverage_lines else "无 readiness 明细"
        table_text = ""
        if table_counts:
            table_text = (
                "\n\n表计数:\n"
                f"taxonomy={table_counts.get('industry_taxonomy', 0)}\n"
                f"history={table_counts.get('industry_classification_history', 0)}\n"
                f"memberships={table_counts.get('industry_memberships', 0)}\n"
                f"source_files={table_counts.get('industry_source_files', 0)}"
            )

        return (
            f"✅ *{title}*\n\n"
            f"status: `{result.get('status')}`\n"
            f"source: `{result.get('source')}` / `{result.get('mode')}`\n"
            f"taxonomy_nodes: `{result.get('taxonomy_nodes_written', 0)}`\n"
            f"history_rows: `{result.get('classification_history_rows_written', 0)}`\n"
            f"memberships: `{result.get('total_memberships_written', 0)}`\n"
            f"official_classifications: `{result.get('total_official_classifications_written', 0)}`\n"
            f"readiness: `{readiness.get('industry_standard_ready')}`\n\n"
            "```text\n"
            f"{exchanges_text}\n\n"
            f"{coverage_text}"
            f"{table_text}\n"
            "```"
        )

    def _parse_date_arg(self, date_str: str):
        """解析日期字符串，返回 date 对象或 None"""
        from datetime import datetime as _dt
        try:
            return _dt.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return None

    async def _execute_task_direct(self, chat_id: int, job_id: str, target_date=None) -> bool:
        """直接执行任务，不通过UI交互

        Args:
            chat_id: 聊天ID
            job_id: 任务ID
            target_date: 可选，指定补数据日期（仅 daily_data_update 有效）
        """
        try:
            self.task_manager.logger.info(f"[TaskManagerHandlers] 直接执行任务: {job_id}, target_date={target_date}")

            # 如果有 target_date 且是 daily_data_update，直接调用 scheduled_tasks
            if target_date and job_id == 'daily_data_update':
                from scheduler.tasks import scheduled_tasks
                from utils import config_manager
                job_cfg = config_manager.get_nested(f'scheduler_config.jobs.{job_id}', {})
                params = job_cfg.get('parameters', {})
                job_config = None
                job_config_manager = getattr(self.task_manager, 'job_config_manager', None)
                if job_config_manager and hasattr(job_config_manager, 'get_job_config'):
                    job_config = job_config_manager.get_job_config(job_id)

                success = await scheduled_tasks.daily_data_update(
                    exchanges=params.get('exchanges'),
                    target_date=target_date,
                    wait_for_market_close=False,
                    enable_trading_day_check=False,
                    instrument_types=params.get('instrument_types'),
                    job_config=job_config,
                )
                self.task_manager.logger.info(f"[TaskManagerHandlers] 任务执行结果: {job_id}, 成功: {success}")
                return success

            # 常规执行路径：通过调度器
            scheduler = self.task_manager.task_scheduler
            available_jobs = list(scheduler.jobs.keys())
            self.task_manager.logger.info(f"[TaskManagerHandlers] 调度器中的可用任务: {available_jobs}")

            if job_id not in scheduler.jobs:
                from scheduler.tasks import scheduled_tasks
                from utils import config_manager

                job_cfg = config_manager.get_nested(f'scheduler_config.jobs.{job_id}', {})
                if not job_cfg.get('manual_only', False):
                    self.task_manager.logger.error(f"[TaskManagerHandlers] 任务 {job_id} 不在调度器中！可用任务: {available_jobs}")
                    return False

                if getattr(scheduled_tasks, job_id, None) is None:
                    self.task_manager.logger.error(f"[TaskManagerHandlers] 手工任务函数不存在: {job_id}")
                    return False

                self.task_manager.logger.info(
                    f"[TaskManagerHandlers] 通过依赖执行器执行 manual_only 任务: {job_id}"
                )
                result = await scheduler.execute_job_direct(
                    job_id,
                    include_dependencies=True,
                )
                self.task_manager.logger.info(f"[TaskManagerHandlers] manual_only 任务执行结果: {job_id}, 成功: {result}")
                return bool(result)

            success = await scheduler.run_job_now(job_id)
            self.task_manager.logger.info(f"[TaskManagerHandlers] 任务调度结果: {job_id}, 成功: {success}")
            return success

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 直接执行任务失败: {job_id}, 错误: {e}")
            import traceback
            self.task_manager.logger.error(f"[TaskManagerHandlers] 错误详情: {traceback.format_exc()}")
            return False

    async def handle_smart_fill_gaps_command(self, event) -> None:
        """处理 /smart_fill_gaps 命令，补足大段数据缺口"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/smart_fill_gaps'
        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}")

        # 解析可选参数
        parts = command_text.strip().split()
        extra_args = parts[1:]  # 透传给脚本的额外参数

        start_message = "⏳ *智能缺口补足已启动 (Phase 1)...*\n\n后台正在扫描并补足大段数据缺口。执行完成后将发送报告。"
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        asyncio.create_task(self._run_script(chat_id, 'smart_fill_gaps.py', extra_args))

    async def handle_audit_factors_command(self, event) -> None:
        """处理 /audit_factors 命令，调用自研复权因子结构化审计"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/audit_factors'
        
        # 默认使用 resume 模式
        extra_args = ["--exchange", "SSE", "--mode", "resume"]
        
        parts = command_text.split()
        if len(parts) > 1:
            exchange = parts[1].upper()
            if exchange in ["SSE", "SZSE", "BSE"]:
                extra_args[1] = exchange
            
            if len(parts) > 2:
                mode = parts[2].lower()
                if mode in ["full", "resume"]:
                    extra_args[3] = mode

        start_message = f"⏳ *开始执行自研复权因子审计*\n\n交易所: `{extra_args[1]}`\n模式: `{extra_args[3]}`\n\n后台处理中，因为需要密集比对，请耐心等待几分钟..."
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')

        asyncio.create_task(self._run_script(chat_id, 'audit_tdx_factors.py', extra_args))

    async def handle_find_gap_and_repair_command(self, event) -> None:
        """处理 /find_gap_and_repair 命令，精确逐日检测并修复缺口"""
        chat_id = event.chat_id
        command_text = event.text if hasattr(event, 'text') else '/find_gap_and_repair'
        self.task_manager.logger.info(f"[TaskManagerHandlers] 收到命令: '{command_text}' | 聊天ID: {chat_id}")

        parts = command_text.strip().split()
        extra_args = parts[1:]

        start_message = "⏳ *精确缺口修复已启动 (Phase 2)...*\n\n后台正在逐品种对比交易日历并修复缺口。此过程可能需要较长时间，完成后将发送报告。"
        await self.task_manager.send_message(chat_id, start_message, parse_mode='markdown')
        asyncio.create_task(self._run_script(chat_id, 'find_gap_and_repair.py', extra_args))

    async def _run_script(self, chat_id: int, script_name: str, extra_args: list = None):
        """通用的后台脚本运行器，捕获输出并发送报告"""
        try:
            import sys
            import os
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            script_path = os.path.join(project_root, 'scripts', script_name)

            cmd = [sys.executable, script_path]
            if extra_args:
                cmd.extend(extra_args)

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=project_root
            )
            stdout, stderr = await process.communicate()
            output = stdout.decode('utf-8', errors='replace')
            error_output = stderr.decode('utf-8', errors='replace')

            if process.returncode == 0:
                report_text = self._extract_report(output)
                msg = f"✅ *{script_name} 执行完成*\n\n```text\n{report_text}\n```"
                await self.task_manager.send_message(chat_id, msg, parse_mode='markdown')
            else:
                # 错误输出也过滤日志噪声
                clean_err = self._filter_log_lines(error_output[-800:])
                msg = f"❌ *{script_name} 执行失败 (Exit: {process.returncode})*\n\n```text\n{clean_err}\n```"
                await self.task_manager.send_message(chat_id, msg, parse_mode='markdown')

        except Exception as e:
            self.task_manager.logger.error(f"[TaskManagerHandlers] 运行 {script_name} 异常: {e}")
            await self.task_manager.send_message(
                chat_id, f"❌ *{script_name} 执行异常*\n\n错误: {str(e)}", parse_mode='markdown'
            )

    @staticmethod
    def _extract_report(output: str) -> str:
        """从脚本输出中提取报告区块

        策略：
        1. 优先查找以 '====...' 开头的报告分隔符，提取整段报告
        2. 回退：过滤掉日志行，仅保留纯报告内容
        3. 最终截取不超过 2000 字符
        """
        lines = output.strip().split('\n')

        # 策略1: 查找报告分隔符（连续的 '=' 行）
        report_start = -1
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('=' * 10) and ('报告' in stripped or '报告' in lines[i + 1].strip() if i + 1 < len(lines) else False):
                report_start = i
                break
            # 也匹配 "📊" 等 emoji 开头的报告标题行
            if '📊' in stripped and report_start == -1:
                # 向前找分隔符行
                if i > 0 and lines[i - 1].strip().startswith('=' * 10):
                    report_start = i - 1
                else:
                    report_start = i
                break

        if report_start >= 0:
            report_lines = lines[report_start:]
            # 过滤掉混入报告末尾的日志行
            report_lines = [l for l in report_lines if not TaskManagerHandlers._is_log_line(l)]
            return '\n'.join(report_lines)[-2000:]

        # 策略2: 回退——取最后 30 行并过滤日志
        tail_lines = lines[-30:]
        clean_lines = [l for l in tail_lines if not TaskManagerHandlers._is_log_line(l)]
        return '\n'.join(clean_lines)[-2000:] if clean_lines else output[-1500:]

    @staticmethod
    def _is_log_line(line: str) -> bool:
        """判断一行是否为日志行（应被过滤）"""
        stripped = line.strip()
        # 匹配 [INFO], [WARNING], [ERROR], [DEBUG] 等日志前缀
        if any(stripped.startswith(f'[{level}]') for level in ('INFO', 'WARNING', 'ERROR', 'DEBUG')):
            return True
        # 匹配带时间戳的日志格式: [LEVEL][2026-...
        if len(stripped) > 7 and stripped[0] == '[' and ']' in stripped[:10]:
            for level in ('INFO', 'WARNING', 'ERROR', 'DEBUG'):
                if f'[{level}]' in stripped[:30]:
                    return True
        # 匹配 Unclosed client session 等 asyncio 噪声
        if 'Unclosed client session' in stripped or 'client_session:' in stripped:
            return True
        return False

    @staticmethod
    def _filter_log_lines(text: str) -> str:
        """过滤文本中的日志行"""
        lines = text.split('\n')
        clean = [l for l in lines if not TaskManagerHandlers._is_log_line(l)]
        return '\n'.join(clean)

    def cleanup_user_state(self, chat_id: int) -> None:
        """清理用户状态"""
        if chat_id in self.user_states:
            del self.user_states[chat_id]
