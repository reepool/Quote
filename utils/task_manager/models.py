"""
Telegram任务管理机器人数据模型
定义任务状态、配置和执行历史的数据结构
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum


class TaskStatus(Enum):
    """任务状态枚举"""
    RUNNING = "running"       # 运行中
    PAUSED = "paused"         # 已暂停
    DISABLED = "disabled"     # 已禁用
    ERROR = "error"          # 错误状态


class TaskAction(Enum):
    """任务操作枚举"""
    RUN_NOW = "run_now"         # 立即运行
    ENABLE = "enable"           # 启用任务
    DISABLE = "disable"         # 禁用任务
    BACK = "back"              # 返回上级
    REFRESH = "refresh"         # 刷新状态


@dataclass
class TaskExecutionRecord:
    """任务执行记录"""
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "unknown"  # running, completed, failed
    duration: Optional[float] = None  # 执行时长（秒）
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskExecutionRecord':
        """从字典创建执行记录对象"""
        return cls(
            start_time=data.get('start_time'),
            end_time=data.get('end_time'),
            status=data.get('status', 'unknown'),
            duration=data.get('duration'),
            error_message=data.get('error_message'),
            result=data.get('result')
        )


@dataclass
class TaskTriggerInfo:
    """任务触发器信息"""
    trigger_type: str  # cron, interval, date
    description: str
    next_run_time: Optional[datetime] = None
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None

    @classmethod
    def from_apscheduler_trigger(cls, trigger) -> 'TaskTriggerInfo':
        """从APScheduler触发器创建触发器信息"""
        if hasattr(trigger, 'fields'):
            # CronTrigger
            cron_parts = []
            if hasattr(trigger, 'minute') and trigger.minute:
                cron_parts.append(str(trigger.minute))
            if hasattr(trigger, 'hour') and trigger.hour:
                cron_parts.append(str(trigger.hour))
            if hasattr(trigger, 'day') and trigger.day:
                cron_parts.append(str(trigger.day))
            if hasattr(trigger, 'day_of_week') and trigger.day_of_week:
                cron_parts.append(str(trigger.day_of_week))
            if hasattr(trigger, 'month') and trigger.month:
                cron_parts.append(str(trigger.month))

            cron_expr = ' '.join(cron_parts) if cron_parts else None
            return cls(
                trigger_type="cron",
                description=f"定时执行: {cron_expr}" if cron_expr else "定时执行",
                cron_expression=cron_expr
            )
        elif hasattr(trigger, 'interval'):
            # IntervalTrigger
            interval_seconds = trigger.interval.total_seconds()
            hours = int(interval_seconds // 3600)
            minutes = int((interval_seconds % 3600) // 60)

            if hours > 0:
                desc = f"间隔执行: 每{hours}小时"
                if minutes > 0:
                    desc += f"{minutes}分钟"
            elif minutes > 0:
                desc = f"间隔执行: 每{minutes}分钟"
            else:
                desc = "间隔执行"

            return cls(
                trigger_type="interval",
                description=desc,
                interval_seconds=int(interval_seconds)
            )
        else:
            return cls(
                trigger_type="unknown",
                description="未知触发器类型"
            )


@dataclass
class TaskStatusInfo:
    """任务状态信息"""
    job_id: str
    description: str
    enabled: bool
    in_scheduler: bool  # 是否在调度器中
    status: TaskStatus
    trigger_info: TaskTriggerInfo
    next_run_time: Optional[datetime] = None
    last_execution: Optional[TaskExecutionRecord] = None
    recent_executions: List[TaskExecutionRecord] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    max_instances: int = 1
    misfire_grace_time: int = 300
    coalesce: bool = True

    @classmethod
    def from_scheduler_data(cls, job_id: str, scheduler_data: Dict[str, Any],
                          config_data: Optional[Dict[str, Any]] = None) -> 'TaskStatusInfo':
        """从调度器数据创建任务状态信息"""

        # 确定任务状态
        enabled = config_data.get('enabled', False) if config_data else False
        in_scheduler = job_id in scheduler_data.get('jobs', {})

        if not enabled:
            status = TaskStatus.DISABLED
        elif not in_scheduler:
            status = TaskStatus.ERROR
        elif scheduler_data.get('jobs', {}).get(job_id, {}).get('status') == 'paused':
            status = TaskStatus.PAUSED
        else:
            status = TaskStatus.RUNNING

        # 处理触发器信息
        trigger_info = None
        if in_scheduler:
            job_data = scheduler_data['jobs'][job_id]
            # 这里需要实际的APScheduler Job对象来获取触发器信息
            # 暂时使用配置信息
            if config_data and 'trigger' in config_data:
                trigger_config = config_data['trigger']
                trigger_type = trigger_config.get('type', 'unknown')

                if trigger_type == 'cron':
                    cron_parts = [
                        str(trigger_config.get('second', 0)),
                        str(trigger_config.get('minute', '*')),
                        str(trigger_config.get('hour', '*')),
                        str(trigger_config.get('day', '*')),
                        str(trigger_config.get('month', '*')),
                        str(trigger_config.get('day_of_week', '*'))
                    ]
                    trigger_info = TaskTriggerInfo(
                        trigger_type="cron",
                        description=f"定时执行: {' '.join(cron_parts)}",
                        cron_expression=' '.join(cron_parts)
                    )
                elif trigger_type == 'interval':
                    hours = trigger_config.get('hours', 0)
                    minutes = trigger_config.get('minutes', 0)
                    seconds = trigger_config.get('seconds', 0)
                    total_seconds = hours * 3600 + minutes * 60 + seconds

                    if total_seconds > 0:
                        desc_parts = []
                        if hours > 0:
                            desc_parts.append(f"{hours}小时")
                        if minutes > 0:
                            desc_parts.append(f"{minutes}分钟")
                        if seconds > 0:
                            desc_parts.append(f"{seconds}秒")

                        trigger_info = TaskTriggerInfo(
                            trigger_type="interval",
                            description=f"间隔执行: 每{' '.join(desc_parts)}",
                            interval_seconds=total_seconds
                        )

        if not trigger_info:
            trigger_info = TaskTriggerInfo(
                trigger_type="unknown",
                description="未知触发器类型"
            )

        # 获取下次运行时间
        next_run_time = None
        if in_scheduler:
            job_data = scheduler_data['jobs'][job_id]
            # 优先使用新的字段名，如果不存在则尝试旧字段名
            next_run_time = job_data.get('next_run_time') or job_data.get('next_run')

            # 导入时间格式化工具
            try:
                from utils.date_utils import DateUtils
                # 这里先保持原始的datetime对象，格式化交给formatters处理
                # 确保时间对象可以被正确传递给格式化函数
                if next_run_time and hasattr(next_run_time, 'astimezone'):
                    # 时间对象已经有时区信息，保持原样
                    pass
                elif next_run_time:
                    # 为没有时区信息的时间对象添加默认时区
                    from datetime import timezone
                    if next_run_time.tzinfo is None:
                        next_run_time = next_run_time.replace(tzinfo=timezone.utc)
            except ImportError:
                # 如果导入失败，保持原有逻辑
                pass

        # 处理参数
        parameters = config_data.get('parameters', {}) if config_data else {}

        return cls(
            job_id=job_id,
            description=config_data.get('description', job_id) if config_data else job_id,
            enabled=enabled,
            in_scheduler=in_scheduler,
            status=status,
            trigger_info=trigger_info,
            next_run_time=next_run_time,
            parameters=parameters,
            max_instances=config_data.get('max_instances', 1) if config_data else 1,
            misfire_grace_time=config_data.get('misfire_grace_time', 300) if config_data else 300,
            coalesce=config_data.get('coalesce', True) if config_data else True
        )


@dataclass
class TaskManagerState:
    """任务管理器状态"""
    chat_id: int
    current_view: str = "main"  # main, status, detail
    selected_job_id: Optional[str] = None
    last_update: Optional[datetime] = None
    message_id: Optional[int] = None  # 当前显示的消息ID，用于编辑