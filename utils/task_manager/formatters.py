"""
Task Manager Formatters
任务管理器格式化工具
"""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from utils import task_manager_logger
from utils.date_utils import DateUtils
from utils.task_manager.models import TaskStatus, TaskStatusInfo


class TaskManagerFormatters:
    """任务管理器格式化工具类"""

    STATUS_MESSAGE_MAX_CHARS = 3400
    TASK_DESCRIPTION_MAX_CHARS = 32

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
                                 total_tasks: int,
                                 max_chars: int = STATUS_MESSAGE_MAX_CHARS) -> str:
        """格式化任务状态摘要"""
        task_manager_logger.debug(f"[TaskManagerFormatters] Formatting task status summary: "
                                  f"total={total_tasks}, running={len(running_tasks)}, disabled={len(disabled_tasks)}")

        status_buckets = TaskManagerFormatters._bucket_status_tasks(running_tasks, disabled_tasks)

        lines = [
            "📊 **任务状态概览**",
            "",
            (
                f"总计 `{total_tasks}` 个｜"
                f"已调度 `{len(status_buckets['running'])}`｜"
                f"手工/暂停 `{len(status_buckets['paused'])}`｜"
                f"异常 `{len(status_buckets['error'])}`｜"
                f"禁用 `{len(status_buckets['disabled'])}`"
            ),
            "复制反引号内命令可直接运行；用 `/detail <任务ID>` 查看完整配置。",
            "",
        ]

        sections = [
            ("🟢 已调度", status_buckets["running"], True),
            ("🟡 手工/暂停", status_buckets["paused"], True),
            ("❌ 异常", status_buckets["error"], True),
            ("🔴 已禁用", status_buckets["disabled"], False),
        ]
        rendered_any = False
        for title, tasks, include_schedule in sections:
            if not tasks:
                continue
            rendered_any = True
            lines.extend(
                TaskManagerFormatters._format_task_group_section_lines(
                    title,
                    tasks,
                    include_schedule=include_schedule,
                )
            )

        if not rendered_any:
            lines.append("暂无任务")

        return TaskManagerFormatters._fit_status_message(lines, max_chars=max_chars)

    @staticmethod
    def format_task_status_messages(
        running_tasks: List[Union[TaskStatusInfo, Dict]],
        disabled_tasks: List[Union[TaskStatusInfo, Dict]],
        total_tasks: int,
        max_chars: int = STATUS_MESSAGE_MAX_CHARS,
    ) -> List[str]:
        """格式化任务状态消息列表，用于 Telegram 分段发送。"""
        task_manager_logger.debug(
            "[TaskManagerFormatters] Formatting paged task status: "
            f"total={total_tasks}, running={len(running_tasks)}, disabled={len(disabled_tasks)}"
        )
        status_buckets = TaskManagerFormatters._bucket_status_tasks(running_tasks, disabled_tasks)
        overview_lines = [
            "📊 **任务状态概览**",
            "",
            (
                f"总计 `{total_tasks}` 个｜"
                f"已调度 `{len(status_buckets['running'])}`｜"
                f"手工/暂停 `{len(status_buckets['paused'])}`｜"
                f"异常 `{len(status_buckets['error'])}`｜"
                f"禁用 `{len(status_buckets['disabled'])}`"
            ),
            "复制反引号内命令可直接运行；用 `/detail <任务ID>` 查看完整配置。",
            "",
        ]

        messages = [
            TaskManagerFormatters._format_status_message_page(
                overview_lines,
                [
                    ("🟢 已调度", status_buckets["running"], True),
                    ("❌ 异常", status_buckets["error"], True),
                ],
                max_chars=max_chars,
            ),
            TaskManagerFormatters._format_status_message_page(
                [],
                [("🟡 手工运行", status_buckets["paused"], True)],
                max_chars=max_chars,
                empty_text="暂无手工运行任务",
            ),
            TaskManagerFormatters._format_status_message_page(
                [],
                [("🔴 已禁用", status_buckets["disabled"], False)],
                max_chars=max_chars,
                empty_text="暂无禁用任务",
            ),
        ]
        return [message for message in messages if message]

    @staticmethod
    def _bucket_status_tasks(
        running_tasks: List[Union[TaskStatusInfo, Dict]],
        disabled_tasks: List[Union[TaskStatusInfo, Dict]],
    ) -> Dict[str, List[Union[TaskStatusInfo, Dict]]]:
        status_buckets = {
            "running": [],
            "paused": [],
            "error": [],
            "disabled": list(disabled_tasks or []),
        }
        for task in list(running_tasks or []):
            status = TaskManagerFormatters._task_status_value(task, default="running")
            if status in status_buckets:
                status_buckets[status].append(task)
            else:
                status_buckets["running"].append(task)
        return status_buckets

    @staticmethod
    def _format_status_message_page(
        prefix_lines: List[str],
        sections: List[tuple],
        *,
        max_chars: int,
        empty_text: str = "暂无任务",
    ) -> str:
        lines = list(prefix_lines)
        rendered_any = False
        for title, tasks, include_schedule in sections:
            if not tasks:
                continue
            rendered_any = True
            lines.extend(
                TaskManagerFormatters._format_task_group_section_lines(
                    title,
                    tasks,
                    include_schedule=include_schedule,
                )
            )
        if not rendered_any:
            lines.append(empty_text)
        return TaskManagerFormatters._fit_status_message(lines, max_chars=max_chars)

    @staticmethod
    def _format_task_group_section(
        title: str,
        tasks: List[Union[TaskStatusInfo, Dict]],
        *,
        include_schedule: bool,
    ) -> str:
        return "\n".join(
            TaskManagerFormatters._format_task_group_section_lines(
                title,
                tasks,
                include_schedule=include_schedule,
            )
        ) + "\n"

    @staticmethod
    def _format_task_group_section_lines(
        title: str,
        tasks: List[Union[TaskStatusInfo, Dict]],
        *,
        include_schedule: bool,
    ) -> List[str]:
        grouped: Dict[str, List[Union[TaskStatusInfo, Dict]]] = {}
        for task in tasks:
            group = TaskManagerFormatters._task_domain(
                TaskManagerFormatters._task_job_id(task)
            )
            grouped.setdefault(group, []).append(task)

        lines = [f"**{title}**"]
        for group in TaskManagerFormatters._ordered_task_groups(grouped):
            lines.append(f"**{group}**")
            for task in sorted(grouped[group], key=TaskManagerFormatters._task_sort_key):
                lines.append(TaskManagerFormatters._format_task_line(task, include_schedule))
            lines.append("")
        return lines

    @staticmethod
    def _format_task_line(task: Union[TaskStatusInfo, Dict], include_schedule: bool) -> str:
        job_id = TaskManagerFormatters._task_job_id(task)
        description = TaskManagerFormatters._shorten_text(
            TaskManagerFormatters._task_description(task),
            TaskManagerFormatters.TASK_DESCRIPTION_MAX_CHARS,
        )
        schedule = TaskManagerFormatters._task_schedule_text(task) if include_schedule else None
        if schedule:
            return f"• {description}：`/run {job_id}`｜{schedule}"
        return f"• {description}：`/run {job_id}`"

    @staticmethod
    def _fit_status_message(lines: List[str], *, max_chars: int) -> str:
        if max_chars <= 0:
            return "\n".join(lines)

        message = "\n".join(lines).strip()
        if len(message) <= max_chars:
            return message

        fitted: List[str] = []
        omitted_tasks = 0
        footer_template = "\n\n... 已省略 `{}` 个任务；请用 `/detail <任务ID>` 查看详情，或按任务ID执行 `/run <任务ID>`。"
        for index, line in enumerate(lines):
            remaining_task_lines = sum(1 for item in lines[index:] if item.startswith("• "))
            footer = footer_template.format(remaining_task_lines)
            candidate = "\n".join(fitted + [line]).strip()
            if len(candidate) + len(footer) > max_chars:
                omitted_tasks = remaining_task_lines
                break
            fitted.append(line)

        if omitted_tasks:
            fitted_message = "\n".join(fitted).rstrip()
            return f"{fitted_message}{footer_template.format(omitted_tasks)}"

        return message[: max_chars - 1].rstrip() + "…"

    @staticmethod
    def _shorten_text(text: str, max_chars: int) -> str:
        if max_chars <= 0 or len(text) <= max_chars:
            return text
        return text[: max_chars - 1].rstrip() + "…"

    @staticmethod
    def _task_job_id(task: Union[TaskStatusInfo, Dict]) -> str:
        if hasattr(task, "job_id"):
            return str(getattr(task, "job_id") or "N/A")
        return str(task.get("job_id", "N/A"))

    @staticmethod
    def _task_description(task: Union[TaskStatusInfo, Dict]) -> str:
        if hasattr(task, "description"):
            return str(getattr(task, "description") or "未知任务")
        return str(task.get("description", "未知任务"))

    @staticmethod
    def _task_status_value(task: Union[TaskStatusInfo, Dict], default: str = "unknown") -> str:
        status = getattr(task, "status", None) if hasattr(task, "status") else task.get("status")
        if isinstance(status, TaskStatus):
            return status.value
        if status:
            return str(status).replace("TaskStatus.", "").lower()
        return default

    @staticmethod
    def _task_schedule_text(task: Union[TaskStatusInfo, Dict]) -> str:
        if hasattr(task, "job_id"):
            trigger_info = getattr(task, "trigger_info", None)
            if trigger_info and getattr(trigger_info, "trigger_type", "") == "manual":
                return "触发: 手工"
            status = TaskManagerFormatters._task_status_value(task, default="unknown")
            next_run = DateUtils.get_task_status_display(
                getattr(task, "next_run_time", None),
                status,
            )
            return f"下次: {next_run}"
        return f"下次: {task.get('next_run', '未安排')}"

    @staticmethod
    def _task_sort_key(task: Union[TaskStatusInfo, Dict]) -> tuple:
        return (
            TaskManagerFormatters._task_description(task),
            TaskManagerFormatters._task_job_id(task),
        )

    @staticmethod
    def _ordered_task_groups(grouped: Dict[str, List[Union[TaskStatusInfo, Dict]]]) -> List[str]:
        preferred = [
            "行情与主数据",
            "港美市场",
            "行业与指数",
            "股东与披露",
            "财务与估值",
            "研究与风控",
            "数据质量与维护",
            "系统运维",
            "其他",
        ]
        return [group for group in preferred if group in grouped] + sorted(
            group for group in grouped if group not in preferred
        )

    @staticmethod
    def _task_domain(job_id: str) -> str:
        job_id = job_id.lower()
        if any(key in job_id for key in ("industry", "index")):
            return "行业与指数"
        if any(key in job_id for key in ("daily_data", "master_governance", "instrument_master", "calendar")):
            if job_id.startswith(("hk_", "us_")) or "hkex" in job_id:
                return "港美市场"
            return "行情与主数据"
        if any(key in job_id for key in ("shareholder", "disclosure", "broker_risk")):
            return "股东与披露"
        if any(key in job_id for key in ("financial", "valuation")):
            return "财务与估值"
        if any(key in job_id for key in ("company_profile", "analyst", "research", "sentiment", "technical", "risk")):
            return "研究与风控"
        if any(key in job_id for key in ("gap", "maintenance", "cleanup", "backup", "cache", "integrity")):
            return "数据质量与维护"
        if any(key in job_id for key in ("health", "dependency", "system")):
            return "系统运维"
        return "其他"

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
