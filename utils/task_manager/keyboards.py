"""
Telegram任务管理机器人键盘布局定义
提供统一的键盘布局，确保用户体验一致
"""

from typing import List, Optional, Union

from .models import TaskStatusInfo, TaskAction


class TaskManagerKeyboards:
    """任务管理器键盘布局工具"""

    @staticmethod
    def main_menu() -> List[List[dict]]:
        """主菜单键盘"""
        return [
            [
                {"text": "📋 查看任务状态", "callback": "status"},
                {"text": "🔄 刷新状态", "callback": "refresh"}
            ],
            [
                {"text": "❌ 关闭", "callback": "close"}
            ]
        ]

    @staticmethod
    def task_status_menu(running_tasks: List[TaskStatusInfo],
                         disabled_tasks: List[TaskStatusInfo]) -> List[List[dict]]:
        """任务状态菜单键盘"""
        keyboard = []

        # 运行中的任务
        if running_tasks:
            for task in running_tasks:
                status_emoji = "🟢" if task.status.value == "running" else "🟡"
                display_text = f"{status_emoji} {task.description}"
                if len(display_text) > 40:  # 限制按钮文本长度
                    display_text = display_text[:37] + "..."

                keyboard.append([
                    {"text": display_text, "callback": f"task_detail:{task.job_id}"}
                ])

        # 已禁用的任务
        if disabled_tasks:
            if running_tasks:  # 添加分隔
                keyboard.append([{"text": "--- 已禁用的任务 ---", "callback": "separator"}])

            for task in disabled_tasks:
                display_text = f"🔴 {task.description}"
                if len(display_text) > 40:
                    display_text = display_text[:37] + "..."

                keyboard.append([
                    {"text": display_text, "callback": f"task_detail:{task.job_id}"}
                ])

        # 底部操作按钮
        keyboard.extend([
            [
                {"text": "🔄 刷新状态", "callback": "refresh:status"},
                {"text": "🏠 返回主页", "callback": "back:main"}
            ]
        ])

        return keyboard

    @staticmethod
    def task_detail_menu(task: TaskStatusInfo) -> List[List[dict]]:
        """任务详情菜单键盘"""
        from research.company_profile.operator_closure import is_retired_operator_entry

        keyboard = []

        # 根据任务状态显示不同的操作按钮
        if not is_retired_operator_entry(task.job_id):
            if task.enabled and task.in_scheduler:
                # 任务正在运行
                keyboard.append([
                    {"text": "🚀 立即执行", "callback": f"task_action:run:{task.job_id}"},
                    {"text": "🔴 禁用任务", "callback": f"task_action:disable:{task.job_id}"}
                ])
            elif not task.enabled:
                # 任务已禁用
                keyboard.append([
                    {"text": "✅ 启用任务", "callback": f"task_action:enable:{task.job_id}"}
                ])

        # 通用操作按钮
        keyboard.append([
            {"text": "🔄 刷新详情", "callback": f"refresh:detail:{task.job_id}"},
            {"text": "📊 查看状态", "callback": "back:status"}
        ])

        keyboard.append([
            {"text": "🏠 返回主页", "callback": "back:main"}
        ])

        return keyboard

    @staticmethod
    def confirmation_menu(action: str, job_id: str) -> List[List[dict]]:
        """确认操作菜单键盘"""
        action_text = {
            "disable": "确认禁用",
            "enable": "确认启用"
        }.get(action, "确认")

        action_emoji = "🔴" if action == "disable" else "✅"

        keyboard = [
            [
                {"text": f"{action_emoji} {action_text}", "callback": f"confirm:{action}:{job_id}"},
                {"text": "❌ 取消", "callback": f"cancel:{action}:{job_id}"}
            ],
            [
                {"text": "🔙 返回详情", "callback": f"back:detail:{job_id}"}
            ]
        ]

        return keyboard

    @staticmethod
    def action_result_menu(job_id: str, success: bool) -> List[List[dict]]:
        """操作结果菜单键盘"""
        if success:
            keyboard = [
                [
                    {"text": "🔄 刷新状态", "callback": "refresh:status"},
                    {"text": "📊 查看详情", "callback": f"task_detail:{job_id}"}
                ],
                [
                    {"text": "🏠 返回主页", "callback": "back:main"}
                ]
            ]
        else:
            keyboard = [
                [
                    {"text": "🔄 重试", "callback": f"retry:{job_id}"},
                    {"text": "🔙 返回", "callback": f"back:detail:{job_id}"}
                ],
                [
                    {"text": "🏠 返回主页", "callback": "back:main"}
                ]
            ]

        return keyboard

    @staticmethod
    def loading_menu(action: str = "处理中") -> List[List[dict]]:
        """加载状态菜单键盘"""
        return [
            [
                {"text": "⏳ " + action, "callback": "loading"}
            ]
        ]

    @staticmethod
    def error_menu(error_type: str, job_id: str = None) -> List[List[dict]]:
        """错误状态菜单键盘"""
        keyboard = []

        if job_id:
            keyboard.append([
                {"text": "🔄 重试", "callback": f"retry:{job_id}"},
                {"text": "🔙 返回", "callback": f"back:detail:{job_id}"}
            ])

        keyboard.append([
            {"text": "📊 查看状态", "callback": "back:status"},
            {"text": "🏠 返回主页", "callback": "back:main"}
        ])

        return keyboard

    @staticmethod
    def back_menu(target: str = "main", job_id: str = None) -> List[List[dict]]:
        """返回菜单键盘"""
        if target == "detail" and job_id:
            keyboard = [
                [
                    {"text": "🔙 返回详情", "callback": f"back:detail:{job_id}"}
                ],
                [
                    {"text": "📊 查看状态", "callback": "back:status"},
                    {"text": "🏠 返回主页", "callback": "back:main"}
                ]
            ]
        else:
            keyboard = [
                [
                    {"text": "🔙 返回上级", "callback": f"back:{target}"},
                    {"text": "🏠 返回主页", "callback": "back:main"}
                ]
            ]

        return keyboard

    @staticmethod
    def parse_callback_data(data: str) -> tuple:
        """解析回调查询数据"""
        parts = data.split(':', 2)  # 最多分成3部分
        if len(parts) == 1:
            return parts[0], None, None
        elif len(parts) == 2:
            return parts[0], parts[1], None
        else:
            return parts[0], parts[1], parts[2]

    @staticmethod
    def create_callback_data(action: str, target: str = None, job_id: str = None) -> str:
        """创建回调查询数据"""
        parts = [action]
        if target:
            parts.append(target)
        if job_id:
            parts.append(job_id)
        return ':'.join(parts)

    @staticmethod
    def is_separator_button(button_data: str) -> bool:
        """检查是否为分隔符按钮"""
        return button_data == "separator"

    @staticmethod
    def get_button_emoji(status: str) -> str:
        """获取状态对应的emoji"""
        emoji_map = {
            "running": "🟢",
            "paused": "🟡",
            "disabled": "🔴",
            "error": "❌",
            "enabled": "✅",
            "loading": "⏳"
        }
        return emoji_map.get(status, "❓")