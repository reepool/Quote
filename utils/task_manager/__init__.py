"""
Telegram任务管理机器人模块

提供通过Telegram Bot管理调度器任务的功能，包括：
- 任务状态查询
- 任务详情查看
- 任务执行控制
- 任务启用/禁用管理
"""

from .task_manager import TaskManagerBot

__all__ = ['TaskManagerBot']