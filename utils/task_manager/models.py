"""
Telegram任务管理机器人数据模型
定义任务状态、配置和执行历史的数据结构
"""
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from enum import Enum
from zoneinfo import ZoneInfo

from utils import task_manager_logger


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
            # --- CronTrigger ---
            cron_parts = []
            # 遵循标准的 cron 格式: 分钟、小时、日、月、星期
            cron_parts.append(str(getattr(trigger, 'minute', '*')))
            cron_parts.append(str(getattr(trigger, 'hour', '*')))
            cron_parts.append(str(getattr(trigger, 'day', '*')))
            cron_parts.append(str(getattr(trigger, 'month', '*')))
            cron_parts.append(str(getattr(trigger, 'day_of_week', '*')))

            cron_expr = ' '.join(cron_parts) if cron_parts else None
            return cls(
                trigger_type="cron",
                description=f"定时执行: {cron_expr}" if cron_expr else "定时执行",
                cron_expression=cron_expr
            )
        elif hasattr(trigger, 'interval'):
            # --- IntervalTrigger ---
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
        elif hasattr(trigger, 'run_date'):
            # --- DateTrigger ---
            run_date = getattr(trigger, 'run_date')
            desc = "单次执行"
            if isinstance(run_date, datetime):
                # 确保datetime对象有时区信息
                if run_date.tzinfo is None:
                    warnings.warn("Received a naive datetime from APScheduler. Assuming scheduler's timezone.", UserWarning)
                    from utils import config_manager
                    scheduler_tz_str = config_manager.get_scheduler_config().timezone
                    run_date = run_date.replace(tzinfo=ZoneInfo(scheduler_tz_str)) # run_date 是 datetime.datetime 对象，datetime 对象不可修改，,replace() 方法返回一个新的 datetime 对象，因此原始 APScheduler 触发器对象不会被修改。

                # 使用工具类进行格式化，以获得更友好的显示
                from utils.date_utils import DateUtils
                formatted_date = DateUtils.format_datetime(run_date, show_timezone=True)
                desc = f"单次执行: {formatted_date}"

            return cls(
                trigger_type="date",
                description=desc,
                next_run_time=run_date
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
        task_manager_logger.debug(f"[TaskManager] Determining task status for job {job_id}: {enabled}")
        in_scheduler = job_id in scheduler_data.get('jobs', {})
        task_manager_logger.debug(f"[TaskManager] Determining task status for job {job_id}: {in_scheduler}")

        if not enabled:
            status = TaskStatus.DISABLED
        elif not in_scheduler:
            status = TaskStatus.ERROR
        # 如果任务在调度器中，但没有下一次运行时间，则认为是暂停状态
        elif in_scheduler and scheduler_data.get('jobs', {}).get(job_id, {}).get('next_run_time') is None:
            status = TaskStatus.PAUSED
        else:
            status = TaskStatus.RUNNING
        task_manager_logger.debug(f"[TaskManager] Creating task status info for job {job_id}: {status}")

        # 处理触发器信息
        trigger_info = None
        if in_scheduler:
            job_data = scheduler_data['jobs'][job_id]
            task_manager_logger.debug(f"[TaskManager] Getting trigger info for job {job_id}: {job_data}")
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
                    task_manager_logger.debug(f"Cron trigger for job {job_id}: {trigger_info}")
                
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
                    task_manager_logger.debug(f"Interval trigger for job {job_id}: {trigger_info}")

                elif trigger_type == 'run_date':
                    run_date_str = trigger_config.get('run_date')
                    if run_date_str:
                        try:
                            run_date = datetime.fromisoformat(run_date_str)
                            from utils.date_utils import DateUtils
                            formatted_date = DateUtils.format_datetime(run_date, show_timezone=True)
                            trigger_info = TaskTriggerInfo(
                                trigger_type="date",
                                description=f"单次执行: {formatted_date}",
                                next_run_time=run_date
                            )
                        except (ValueError, TypeError):
                            trigger_info = TaskTriggerInfo(
                                trigger_type="run_date",
                                description=f"单次执行: (无效日期 {run_date_str})"
                            )
                    task_manager_logger.debug(f"Run_date trigger for job {job_id}: {trigger_info}")

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

            # 增加对 next_run_time 类型的健壮性处理
            if isinstance(next_run_time, str):
                try:
                    next_run_time = datetime.fromisoformat(next_run_time.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    task_manager_logger.warning(f"Invalid next_run_time format for job {job_id}: {next_run_time}")
                    next_run_time = None
            elif not isinstance(next_run_time, datetime):
                next_run_time = None

            # 确保 datetime 对象有时区信息
            if next_run_time and next_run_time.tzinfo is None:
                from datetime import timezone
                # 假设无时区的时间是UTC时间
                next_run_time = next_run_time.replace(tzinfo=timezone.utc)

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