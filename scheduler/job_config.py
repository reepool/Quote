"""
Scheduler job configuration parser and manager.
Handles parsing and validation of job configurations from config.json.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import time

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from utils import scheduler_logger


@dataclass
class JobConfig:
    """任务配置数据类"""
    job_id: str
    enabled: bool
    description: str
    trigger: Any  # CronTrigger or IntervalTrigger
    max_instances: int
    misfire_grace_time: int
    coalesce: bool
    parameters: Dict[str, Any]
    report: bool = False  # 是否发送报告通知


class JobConfigManager:
    """任务配置管理器"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.job_configs: Dict[str, JobConfig] = {}

    def load_job_configs(self) -> Dict[str, JobConfig]:
        """从配置文件加载任务配置"""
        try:
            scheduler_config = self.config_manager.get_nested('scheduler_config', {})
            jobs_config = scheduler_config.get('jobs', {})

            self.job_configs = {}

            for job_id, job_data in jobs_config.items():
                try:
                    job_config = self._parse_job_config(job_id, job_data)
                    if job_config:
                        self.job_configs[job_id] = job_config
                        scheduler_logger.info(f"[JobConfigManager] Loaded config for job: {job_id}")
                    else:
                        scheduler_logger.warning(f"[JobConfigManager] Skipped disabled job: {job_id}")

                except Exception as e:
                    scheduler_logger.error(f"[JobConfigManager] Failed to parse config for job {job_id}: {e}")
                    continue

            scheduler_logger.info(f"[JobConfigManager] Loaded {len(self.job_configs)} job configurations")
            return self.job_configs

        except Exception as e:
            scheduler_logger.error(f"[JobConfigManager] Failed to load job configurations: {e}")
            return {}

    def _parse_job_config(self, job_id: str, job_data: Dict[str, Any]) -> Optional[JobConfig]:
        """解析单个任务配置"""
        try:
            # 检查任务是否启用
            if not job_data.get('enabled', True):
                return None

            # 解析触发器
            trigger = self._parse_trigger(job_data.get('trigger', {}))
            if trigger is None:
                scheduler_logger.error(f"[JobConfigManager] Invalid trigger for job {job_id}")
                return None

            # 构建任务配置
            job_config = JobConfig(
                job_id=job_id,
                enabled=job_data.get('enabled', True),
                description=job_data.get('description', ''),
                trigger=trigger,
                max_instances=job_data.get('max_instances', 1),
                misfire_grace_time=job_data.get('misfire_grace_time', 300),
                coalesce=job_data.get('coalesce', True),
                parameters=job_data.get('parameters', {}),
                report=job_data.get('report', False)  # 默认不发送报告
            )

            return job_config

        except Exception as e:
            scheduler_logger.error(f"[JobConfigManager] Error parsing job config {job_id}: {e}")
            return None

    def _parse_trigger(self, trigger_config: Dict[str, Any]) -> Optional[Any]:
        """解析触发器配置"""
        try:
            trigger_type = trigger_config.get('type', '').lower()

            if trigger_type == 'cron':
                return self._parse_cron_trigger(trigger_config)
            elif trigger_type == 'interval':
                return self._parse_interval_trigger(trigger_config)
            elif trigger_type == 'date':
                # 日期触发器（立即执行）暂时不支持配置化
                return None
            else:
                scheduler_logger.error(f"[JobConfigManager] Unsupported trigger type: {trigger_type}")
                return None

        except Exception as e:
            scheduler_logger.error(f"[JobConfigManager] Error parsing trigger: {e}")
            return None

    def _parse_cron_trigger(self, trigger_config: Dict[str, Any]) -> CronTrigger:
        """解析 Cron 触发器"""
        # 提取 cron 参数
        cron_kwargs = {}

        # 基本时间字段
        if 'second' in trigger_config:
            cron_kwargs['second'] = trigger_config['second']
        if 'minute' in trigger_config:
            cron_kwargs['minute'] = trigger_config['minute']
        if 'hour' in trigger_config:
            cron_kwargs['hour'] = trigger_config['hour']
        if 'month' in trigger_config:
            cron_kwargs['month'] = trigger_config['month']
        if 'day_of_week' in trigger_config:
            cron_kwargs['day_of_week'] = trigger_config['day_of_week']
        if 'week' in trigger_config:
            cron_kwargs['week'] = trigger_config['week']

        # 处理day字段（支持特殊值'last'）
        if 'day_of_month' in trigger_config:
            cron_kwargs['day'] = trigger_config['day_of_month']
        elif 'day' in trigger_config:
            day_value = trigger_config['day']
            if day_value == 'last':
                cron_kwargs['day'] = 'last'  # 支持quarterly_cleanup的"day": "last"
            else:
                cron_kwargs['day'] = day_value

        # 其他 cron 参数
        if 'start_date' in trigger_config:
            cron_kwargs['start_date'] = trigger_config['start_date']
        if 'end_date' in trigger_config:
            cron_kwargs['end_date'] = trigger_config['end_date']
        if 'timezone' in trigger_config:
            cron_kwargs['timezone'] = trigger_config['timezone']
        if 'jitter' in trigger_config:
            cron_kwargs['jitter'] = trigger_config['jitter']

        return CronTrigger(**cron_kwargs)

    def _parse_interval_trigger(self, trigger_config: Dict[str, Any]) -> IntervalTrigger:
        """解析间隔触发器"""
        interval_kwargs = {}

        # 时间间隔字段
        if 'weeks' in trigger_config:
            interval_kwargs['weeks'] = trigger_config['weeks']
        if 'days' in trigger_config:
            interval_kwargs['days'] = trigger_config['days']
        if 'hours' in trigger_config:
            interval_kwargs['hours'] = trigger_config['hours']
        if 'minutes' in trigger_config:
            interval_kwargs['minutes'] = trigger_config['minutes']
        if 'seconds' in trigger_config:
            interval_kwargs['seconds'] = trigger_config['seconds']

        # 其他间隔参数
        if 'start_date' in trigger_config:
            interval_kwargs['start_date'] = trigger_config['start_date']
        if 'end_date' in trigger_config:
            interval_kwargs['end_date'] = trigger_config['end_date']
        if 'timezone' in trigger_config:
            interval_kwargs['timezone'] = trigger_config['timezone']
        if 'jitter' in trigger_config:
            interval_kwargs['jitter'] = trigger_config['jitter']

        # 确保至少有一个时间间隔参数
        if not any(k in interval_kwargs for k in ['weeks', 'days', 'hours', 'minutes', 'seconds']):
            raise ValueError("Interval trigger must have at least one time interval parameter")

        return IntervalTrigger(**interval_kwargs)

    def get_job_config(self, job_id: str) -> Optional[JobConfig]:
        """获取指定任务的配置"""
        return self.job_configs.get(job_id)

    def get_all_job_configs(self) -> Dict[str, JobConfig]:
        """获取所有任务配置"""
        return self.job_configs.copy()

    def is_job_enabled(self, job_id: str) -> bool:
        """检查任务是否启用"""
        job_config = self.get_job_config(job_id)
        return job_config.enabled if job_config else False

    def get_job_parameters(self, job_id: str) -> Dict[str, Any]:
        """获取任务参数"""
        job_config = self.get_job_config(job_id)
        return job_config.parameters if job_config else {}

    def get_next_run_time(self, job_id: str) -> Optional[str]:
        """获取任务下次运行时间（用于日志显示）"""
        job_config = self.get_job_config(job_id)
        if not job_config:
            return None

        try:
            # 创建一个临时触发器来获取下次运行时间
            from utils.date_utils import get_shanghai_time
            now = get_shanghai_time()
            next_run = job_config.trigger.get_next_fire_time(None, now)
            return next_run.isoformat() if next_run else None
        except Exception as e:
            scheduler_logger.error(f"[JobConfigManager] Error getting next run time for {job_id}: {e}")
            return None

    def validate_job_config(self, job_config: JobConfig) -> bool:
        """验证任务配置的有效性"""
        try:
            # 基本验证
            if not job_config.job_id:
                return False

            if not job_config.description:
                return False

            if not job_config.trigger:
                return False

            if job_config.max_instances < 1:
                return False

            if job_config.misfire_grace_time < 0:
                return False

            # 验证参数
            if not isinstance(job_config.parameters, dict):
                return False

            return True

        except Exception as e:
            scheduler_logger.error(f"[JobConfigManager] Error validating job config: {e}")
            return False


# 全局任务配置管理器实例
job_config_manager = None