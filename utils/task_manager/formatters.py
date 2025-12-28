"""
Task Manager Formatters
任务管理器格式化工具
"""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from utils import task_manager_logger
from utils.date_utils import DateUtils
from utils.task_manager.models import TaskStatusInfo


class TaskManagerFormatters:
    """任务管理器格式化工具类"""

    @staticmethod
    def format_main_message() -> str:
        """格式化主菜单消息"""
        message = "🔧 **任务管理器**\n\n"
        message += "欢迎使用任务管理器！\n"
        message += "您可以在这里查看和控制各种系统任务。"
        return message

    @staticmethod
    def format_confirmation(action: str, task_description: str) -> str:
        """格式化确认消息"""
        action_text = {
            'enable': '启用',
            'disable': '禁用',
            'start': '启动',
            'stop': '停止',
            'restart': '重启'
        }.get(action, action)

        return f"⚠️ 确认{action_text}任务：\n\n*{task_description}*"

    @staticmethod
    def format_error_message(error_type: str, details: str = "") -> str:
        """格式化错误消息"""
        error_messages = {
            'task_not_found': "❌ 任务未找到",
            'scheduler_error': "❌ 调度器错误",
            'action_failed': "❌ 操作失败",
            'permission_denied': "❌ 权限不足",
            'invalid_state': "❌ 无效状态",
            'timeout': "❌ 操作超时",
            'network_error': "❌ 网络错误"
        }

        base_message = error_messages.get(error_type, "❌ 未知错误")
        if details:
            return f"{base_message}\n\n详情：{details}"
        return base_message

    @staticmethod
    def format_task_status_summary(running_tasks: List[Union[TaskStatusInfo, Dict]],
                                 disabled_tasks: List[Union[TaskStatusInfo, Dict]],
                                 total_tasks: int) -> str:
        """格式化任务状态摘要"""
        task_manager_logger.debug(f"[TaskManagerFormatters] Formatting task status summary: "
                                  f"total={total_tasks}, running={len(running_tasks)}, disabled={len(disabled_tasks)}")

        message = "📊 **任务状态概览**\n\n"

        # 统计信息
        enabled_count = len(running_tasks)
        disabled_count = len(disabled_tasks)

        message += f"• 总任务数：{total_tasks}\n"
        message += f"• 运行中：{enabled_count}\n"
        message += f"• 已禁用：{disabled_count}\n\n"

        # 运行中的任务
        if running_tasks:
            message += "**🟢 运行中的任务：**\n"
            for task in running_tasks:  # 显示所有任务
                if hasattr(task, 'job_id'):  # TaskStatusInfo对象
                    job_id = task.job_id
                    description = task.description
                    # 使用新的时间格式化函数
                    next_run = DateUtils.get_task_status_display(
                        task.next_run_time,
                        str(task.status) if hasattr(task, 'status') and task.status else 'unknown'
                    )
                else:  # 字典对象
                    job_id = task.get('job_id', 'N/A')
                    description = task.get('description', '未知任务')
                    next_run = task.get('next_run', '未安排')
                message += f"• {description} ({job_id}) - {next_run}\n"

        # 禁用的任务
        if disabled_tasks:
            message += f"\n**🔴 已禁用的任务：**\n"
            for task in disabled_tasks:  # 显示所有禁用任务
                if hasattr(task, 'job_id'):  # TaskStatusInfo对象
                    job_id = task.job_id
                    description = task.description
                else:  # 字典对象
                    job_id = task.get('job_id', 'N/A')
                    description = task.get('description', '未知任务')
                message += f"• {description} ({job_id})\n"

        if not running_tasks and not disabled_tasks:
            message += "暂无任务"

        return message

    @staticmethod
    def format_loading_message(action: str) -> str:
        """格式化加载消息"""
        return f"⏳ {action}中..."

    @staticmethod
    def format_action_result(action: str, job_id: str, success: bool) -> str:
        """格式化操作结果消息"""
        action_text = {
            'enable': '启用',
            'disable': '禁用',
            'start': '启动',
            'stop': '停止',
            'restart': '重启',
            'refresh': '刷新'
        }.get(action, action)

        if success:
            return f"✅ 任务 {job_id} {action_text}成功"
        else:
            return f"❌ 任务 {job_id} {action_text}失败"