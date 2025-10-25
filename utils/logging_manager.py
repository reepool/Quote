"""
统一的日志管理模块
整合基础日志配置和高级日志功能
"""

import logging
import sys
import os
import time
import functools
import traceback
import threading
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass
from collections import defaultdict, deque

from utils.exceptions import QuoteSystemError, ErrorCodes
from .config_manager import config_manager

# 获取 logging_manager 模块的专用日志器
logger = logging.getLogger("LoggingManager")


@dataclass
class LogConfig:
    """日志配置"""
    level: str = "INFO"
    format: str = "[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    file_max_bytes: int = 10 * 1024 * 1024  # 10MB
    file_backup_count: int = 5
    enable_console: bool = True
    enable_file: bool = True
    log_directory: Optional[str] = None
    log_filename: str = "sys.log"
    enable_rotation: bool = True
    rotation_type: str = "size"  # "size" or "time"


class LoggingManager:
    """统一的日志管理器"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return

        self._initialized = True
        self._loggers: Dict[str, logging.Logger] = {}
        self._config = LogConfig()
        self._metrics = defaultdict(int)

    def configure(self, config: LogConfig = None):
        """配置日志系统"""
        if config:
            self._config = config

        # 处理日志目录 - 如果未设置，使用默认值
        if self._config.log_directory is None:
            # 使用默认路径：相对于项目根目录的 log 文件夹
            project_root = Path(__file__).resolve().parents[1]
            self._config.log_directory = str(project_root / "log")

        # 确保日志目录存在
        Path(self._config.log_directory).mkdir(parents=True, exist_ok=True)

        # 设置根日志级别
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self._config.level.upper()))

        # 清除现有处理器
        self._clear_handlers(root_logger)

        # 添加控制台处理器
        if self._config.enable_console:
            self._add_console_handler(root_logger)

        # 添加文件处理器
        if self._config.enable_file:
            self._add_file_handler(root_logger)

    def configure_from_config_file(self):
        """从配置文件加载日志配置"""
        try:
            logging_config = config_manager.get_logging_config()

            # 处理相对路径 - 转换为绝对路径
            log_directory = logging_config.file_config.directory
            if not os.path.isabs(log_directory):
                # 如果是相对路径，相对于项目根目录
                project_root = Path(__file__).resolve().parents[1]
                log_directory = str(project_root / log_directory)

            # 确保日志目录存在
            Path(log_directory).mkdir(parents=True, exist_ok=True)

            # 转换配置格式 - 安全地访问配置属性
            rotation_config = getattr(logging_config.file_config, 'rotation', None) or {}

            config = LogConfig(
                level=getattr(logging_config, 'level', 'INFO'),
                format=getattr(logging_config, 'format', '[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s'),
                date_format=getattr(logging_config, 'date_format', '%Y-%m-%d %H:%M:%S'),
                file_max_bytes=rotation_config.get('max_bytes_mb', 10) * 1024 * 1024 if isinstance(rotation_config, dict) else 10 * 1024 * 1024,
                file_backup_count=rotation_config.get('backup_count', 5) if isinstance(rotation_config, dict) else 5,
                enable_console=getattr(logging_config.console_config, 'enabled', True) if hasattr(logging_config.console_config, 'enabled') else logging_config.console_config.get('enabled', True) if isinstance(logging_config.console_config, dict) else True,
                enable_file=getattr(logging_config.file_config, 'enabled', True) if hasattr(logging_config.file_config, 'enabled') else logging_config.file_config.get('enabled', True) if isinstance(logging_config.file_config, dict) else True,
                log_directory=log_directory,
                log_filename=getattr(logging_config.file_config, 'filename', 'sys.log') if hasattr(logging_config.file_config, 'filename') else logging_config.file_config.get('filename', 'sys.log') if isinstance(logging_config.file_config, dict) else 'sys.log',
                enable_rotation=rotation_config.get('enabled', True) if isinstance(rotation_config, dict) else True,
                rotation_type=rotation_config.get('type', 'size') if isinstance(rotation_config, dict) else 'size'
            )

            # 应用配置
            self.configure(config)

            # 配置模块特定的日志级别
            self._configure_module_loggers(logging_config.modules)

            # 配置性能监控
            self._configure_performance_monitoring(logging_config.performance_monitoring)

            return logging_config

        except Exception as e:
            raise QuoteSystemError(
                f"Failed to configure logging from config file: {str(e)}",
                ErrorCodes.CONFIG_INVALID_FORMAT
            ) from e

    def _safe_get_config(self, config_obj: Any, key: str, default: Any) -> Any:
        """安全地从配置对象（dataclass或dict）获取值"""
        if hasattr(config_obj, key):
            return getattr(config_obj, key, default)
        elif isinstance(config_obj, dict) and key in config_obj:
            return config_obj.get(key, default)
        return default

    def _configure_module_loggers(self, modules_config: Dict[str, Any]):
        """配置模块特定的日志器"""
        for module_name, module_config in modules_config.items():
            enabled = self._safe_get_config(module_config, 'enabled', True)
            if enabled:
                logger = self.get_logger(module_name)
                level = self._safe_get_config(module_config, 'level', 'INFO')
                logger.setLevel(getattr(logging, level.upper()))

    def _configure_performance_monitoring(self, perf_config: Any):
        """配置性能监控"""
        enabled = self._safe_get_config(perf_config, 'enabled', True)

        # 可以在这里根据配置启用/禁用性能监控功能
        if not enabled:
            # 禁用性能监控的相关功能
            pass

        # 设置慢操作阈值 - 字段名在dataclass中是 slow_operation_threshold
        threshold = self._safe_get_config(perf_config, 'slow_operation_threshold', 1.0)
        # 可以将这个值存储起来供装饰器使用
        self._slow_operation_threshold = threshold

    def _clear_handlers(self, logger: logging.Logger):
        """清除现有处理器"""
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    def _add_console_handler(self, logger: logging.Logger):
        """添加控制台处理器"""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(
            self._config.format,
            datefmt=self._config.date_format
        ))
        logger.addHandler(console_handler)

    def _add_file_handler(self, logger: logging.Logger):
        """添加文件处理器"""
        log_file_path = Path(self._config.log_directory) / self._config.log_filename

        if self._config.rotation_type == "size":
            file_handler = RotatingFileHandler(
                filename=log_file_path,
                maxBytes=self._config.file_max_bytes,
                backupCount=self._config.file_backup_count,
                encoding="utf-8"
            )
        else:  # time rotation
            file_handler = TimedRotatingFileHandler(
                filename=log_file_path,
                when="midnight",
                interval=1,
                backupCount=self._config.file_backup_count,
                encoding="utf-8"
            )

        file_handler.setFormatter(logging.Formatter(
            self._config.format,
            datefmt=self._config.date_format
        ))
        logger.addHandler(file_handler)

    def get_logger(self, name: str = None) -> logging.Logger:
        """获取日志记录器"""
        if name is None:
            name = "quotesystem"

        if name not in self._loggers:
            logger = logging.getLogger(name)
            self._loggers[name] = logger

        return self._loggers[name]

    def set_level(self, level: str, logger_name: str = None):
        """设置日志级别"""
        log_level = getattr(logging, level.upper(), logging.INFO)

        if logger_name:
            logger = self.get_logger(logger_name)
            logger.setLevel(log_level)
        else:
            # 设置根日志级别
            logging.getLogger().setLevel(log_level)

    def add_custom_handler(self, handler: logging.Handler, logger_name: str = None):
        """添加自定义处理器"""
        logger = self.get_logger(logger_name) if logger_name else logging.getLogger()
        logger.addHandler(handler)

    def get_metrics(self) -> Dict[str, int]:
        """获取日志统计指标"""
        return dict(self._metrics)

    def reset_metrics(self):
        """重置统计指标"""
        self._metrics.clear()


class LogContext:
    """日志上下文管理器"""

    def __init__(self, module: str, operation: str = None,
                 instrument_id: str = None, exchange: str = None,
                 extra_context: Dict[str, Any] = None, **kwargs):
        self.module = module
        self.operation = operation
        self.instrument_id = instrument_id
        self.exchange = exchange
        # 兼容旧版本，接受但不使用的额外参数
        self.extra_context = {k: v for k, v in (extra_context or {}).items()
                            if k not in ['source'] and not k.startswith('_')}
        # 添加kwargs中的有效参数到extra_context
        valid_kwargs = {k: v for k, v in kwargs.items()
                       if k in ['symbol', 'user_id', 'request_id'] and not k.startswith('_')}
        self.extra_context.update(valid_kwargs)
        self.start_time = None
        self.logger = logging_manager.get_logger(module)

    def __enter__(self):
        self.start_time = time.time()
        self._log_start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        if exc_type is not None:
            self._log_error(exc_val, duration, exc_tb)
        else:
            self._log_success(duration)

    def _get_context_str(self) -> str:
        """获取上下文字符串"""
        parts = [self.module]

        if self.operation:
            parts.append(self.operation)

        if self.exchange:
            parts.append(f"Exchange:{self.exchange}")

        if self.instrument_id:
            parts.append(f"ID:{self.instrument_id}")

        # 添加额外上下文
        for key, value in self.extra_context.items():
            parts.append(f"{key}:{value}")

        return ".".join(parts)

    def _log_start(self):
        """记录开始日志"""
        context = self._get_context_str()
        self.logger.info(f"[{context}] Starting operation")
        logging_manager._metrics[f"{context}_started"] += 1

    def _log_success(self, duration: float):
        """记录成功日志"""
        context = self._get_context_str()
        self.logger.info(f"[{context}] Operation completed in {duration:.2f}s")
        logging_manager._metrics[f"{context}_completed"] += 1
        logging_manager._metrics[f"{context}_duration"] += duration

    def _log_error(self, error: Exception, duration: float, tb):
        """记录错误日志"""
        context = self._get_context_str()
        error_msg = f"[{context}] Operation failed in {duration:.2f}s: {str(error)}"

        if isinstance(error, (ConnectionError, TimeoutError)):
            self.logger.error(error_msg)
        else:
            self.logger.error(error_msg)
            self.logger.debug(f"[{context}] Traceback: {''.join(traceback.format_tb(tb))}")

        logging_manager._metrics[f"{context}_failed"] += 1


def log_execution(module: str, operation: str = None, extra_context: Dict[str, Any] = None):
    """日志装饰器"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # 尝试从参数中提取上下文信息
            context = extra_context or {}
            context.update(_extract_context_from_args(args, kwargs))

            with LogContext(module, operation or func.__name__, **context):
                return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # 尝试从参数中提取上下文信息
            context = extra_context or {}
            context.update(_extract_context_from_args(args, kwargs))

            with LogContext(module, operation or func.__name__, **context):
                return await func(*args, **kwargs)

        # 判断函数是否为协程函数
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def log_performance(module: str, threshold: float = 1.0):
    """性能监控日志装饰器"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                logger = logging_manager.get_logger(module)

                if duration > threshold:
                    logger.warning(f"[{module}] Slow operation: {func.__name__} took {duration:.2f}s")
                else:
                    logger.debug(f"[{module}] {func.__name__} completed in {duration:.2f}s")

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                logger = logging_manager.get_logger(module)

                if duration > threshold:
                    logger.warning(f"[{module}] Slow operation: {func.__name__} took {duration:.2f}s")
                else:
                    logger.debug(f"[{module}] {func.__name__} completed in {duration:.2f}s")

        # 判断函数是否为协程函数
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def log_data_operation(operation_type: str, count: Optional[int] = None):
    """数据操作日志"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging_manager.get_logger("DataOperation")
            logger.info(f"[DataOperation] Starting {operation_type}")
            if count:
                logger.info(f"[DataOperation] Processing {count} items")

            try:
                result = func(*args, **kwargs)
                logger.info(f"[DataOperation] {operation_type} completed successfully")
                logging_manager._metrics[f"{operation_type}_success"] += 1
                return result
            except Exception as e:
                logger.error(f"[DataOperation] {operation_type} failed: {str(e)}")
                logging_manager._metrics[f"{operation_type}_failed"] += 1
                raise

        return wrapper

    return decorator


def _extract_context_from_args(args: tuple, kwargs: dict) -> Dict[str, Any]:
    """从函数参数中提取上下文信息"""
    context = {}

    # 提取常见的上下文参数
    if 'instrument_id' in kwargs:
        context['instrument_id'] = kwargs['instrument_id']
    elif args and isinstance(args[0], str):
        context['instrument_id'] = args[0]

    if 'exchange' in kwargs:
        context['exchange'] = kwargs['exchange']
    elif len(args) > 1 and isinstance(args[1], str):
        context['exchange'] = args[1]

    if 'symbol' in kwargs:
        context['symbol'] = kwargs['symbol']

    if 'user_id' in kwargs:
        context['user_id'] = kwargs['user_id']

    if 'request_id' in kwargs:
        context['request_id'] = kwargs['request_id']

    return context


class MetricsLogger:
    """指标记录器"""

    def __init__(self, module: str):
        self.module = module
        self.metrics = defaultdict(lambda: deque(maxlen=1000))

    def increment(self, metric_name: str, value: int = 1):
        """增加计数器"""
        key = f"{self.module}.{metric_name}"
        self.metrics[key].append(value)
        logging_manager._metrics[key] = logging_manager._metrics.get(key, 0) + value

        logger = logging_manager.get_logger(self.module)
        logger.debug(f"[Metrics] {key}: {logging_manager._metrics[key]}")

    def timing(self, metric_name: str, duration: float):
        """记录时间"""
        key = f"{self.module}.{metric_name}_duration"
        self.metrics[key].append(duration)

        logger = logging_manager.get_logger(self.module)
        logger.debug(f"[Metrics] {key}: {duration:.2f}s")

    def gauge(self, metric_name: str, value: Any):
        """记录瞬时值"""
        key = f"{self.module}.{metric_name}"
        self.metrics[key].append(value)

        logger = logging_manager.get_logger(self.module)
        logger.debug(f"[Metrics] {key}: {value}")

    def get_metrics(self) -> dict:
        """获取所有指标"""
        result = {}
        for key, values in self.metrics.items():
            if values:
                result[key] = {
                    'count': len(values),
                    'latest': values[-1],
                    'sum': sum(values) if isinstance(values[-1], (int, float)) else None
                }
        return result

    def reset(self):
        """重置指标"""
        self.metrics.clear()


# 全局日志管理器实例
logging_manager = LoggingManager()

# 兼容性：保持原有的 logger 接口
logger = logging_manager.get_logger()

# 预定义的指标记录器实例
data_manager_metrics = MetricsLogger("DataManager")
data_source_metrics = MetricsLogger("DataSource")
database_metrics = MetricsLogger("Database")
cache_metrics = MetricsLogger("Cache")
api_metrics = MetricsLogger("API")


class ModuleLoggers:
    """模块专用日志器集合"""

    # 核心模块日志器
    DataManager = logging_manager.get_logger("DataManager")
    Database = logging_manager.get_logger("Database")
    DataSource = logging_manager.get_logger("DataSource")
    API = logging_manager.get_logger("API")
    Scheduler = logging_manager.get_logger("Scheduler")

    # 服务模块日志器
    Config = logging_manager.get_logger("Config")
    TelegramBot = logging_manager.get_logger("TelegramBot")
    TaskManager = logging_manager.get_logger("TaskManager")
    Monitor = logging_manager.get_logger("Monitor")
    Report = logging_manager.get_logger("Report")
    tg_task_manager = logging_manager.get_logger("tg_task_manager")


    # 数据源专用日志器
    AkShareSource = logging_manager.get_logger("AkShareSource")
    YFinanceSource = logging_manager.get_logger("YFinanceSource")
    Tushare = logging_manager.get_logger("Tushare")
    Baostock = logging_manager.get_logger("Baostock")

    # 工具模块日志器
    Cache = logging_manager.get_logger("Cache")
    Validation = logging_manager.get_logger("Validation")
    DateUtils = logging_manager.get_logger("DateUtils")

    @classmethod
    def get_logger(cls, module_name: str):
        """获取指定模块的日志器"""
        return logging_manager.get_logger(module_name)

    @classmethod
    def configure_from_config(cls):
        """根据配置文件配置各个模块的日志器"""
        try:
            logging_config = logging_manager.configure_from_config_file()

            print("Configuring module-specific loggers:")
            for module_name, module_config in logging_config.modules.items():
                # 安全地访问 enabled 属性
                enabled = getattr(module_config, 'enabled', True) if hasattr(module_config, 'enabled') else module_config.get('enabled', True) if isinstance(module_config, dict) else True
                # 安全地访问 level 属性
                level = getattr(module_config, 'level', 'INFO') if hasattr(module_config, 'level') else module_config.get('level', 'INFO') if isinstance(module_config, dict) else 'INFO'

                if enabled:
                    logger = cls.get_logger(module_name)
                    logger.setLevel(getattr(logging, level.upper()))
                    print(f"  ✓ {module_name}: {level}")
                else:
                    # 如果模块被禁用，设置为 CRITICAL 级别
                    logger = cls.get_logger(module_name)
                    logger.setLevel(getattr(logging, 'CRITICAL'))
                    print(f"  ✗ {module_name}: DISABLED")

            return logging_config

        except Exception as e:
            print(f"Failed to configure module loggers: {e}")
            raise


# 便捷的模块日志器别名
dm_logger = ModuleLoggers.DataManager
db_logger = ModuleLoggers.Database
ds_logger = ModuleLoggers.DataSource
api_logger = ModuleLoggers.API
scheduler_logger = ModuleLoggers.Scheduler
monitor_logger = ModuleLoggers.Monitor
config_logger = ModuleLoggers.Config
tgbot_logger = ModuleLoggers.TelegramBot
task_manager_logger = ModuleLoggers.TaskManager
report_logger = ModuleLoggers.Report
tg_task_logger = ModuleLoggers.tg_task_manager
cache_logger = ModuleLoggers.Cache
validation_logger = ModuleLoggers.Validation
date_utils_logger = ModuleLoggers.DateUtils
akshare_logger = ModuleLoggers.AkShareSource
yfinance_logger = ModuleLoggers.YFinanceSource
tushare_logger = ModuleLoggers.Tushare
baostock_logger = ModuleLoggers.Baostock

# 初始化默认配置
def initialize_logging(use_config_file: bool = True):
    """初始化日志系统"""
    try:
        if use_config_file:
            # 从配置文件初始化
            logging_config = logging_manager.configure_from_config_file()
            print(f"Logging system initialized from config file")

            # 显示配置信息 - 安全地访问配置属性
            print(f"  - Log level: {getattr(logging_config, 'level', 'INFO')}")
            print(f"  - File logging: {getattr(logging_config.file_config, 'enabled', True) if hasattr(logging_config.file_config, 'enabled') else logging_config.file_config.get('enabled', True) if isinstance(logging_config.file_config, dict) else True}")
            print(f"  - Console logging: {getattr(logging_config.console_config, 'enabled', True) if hasattr(logging_config.console_config, 'enabled') else logging_config.console_config.get('enabled', True) if isinstance(logging_config.console_config, dict) else True}")
            print(f"  - Performance monitoring: {getattr(logging_config.performance_monitoring, 'enabled', True) if hasattr(logging_config.performance_monitoring, 'enabled') else logging_config.performance_monitoring.get('enabled', True) if isinstance(logging_config.performance_monitoring, dict) else True}")
        else:
            # 使用默认配置初始化
            logging_manager.configure()
            print("Logging system initialized with default config")

        logger.info("Logging system initialized successfully")
        return True

    except Exception as e:
        print(f"Failed to initialize logging: {e}")
        # 如果配置文件初始化失败，尝试使用默认配置
        if use_config_file:
            print("Falling back to default configuration...")
            try:
                logging_manager.configure()
                logger.info("Logging system initialized with fallback config")
                return True
            except Exception as fallback_e:
                print(f"Fallback initialization also failed: {fallback_e}")

        raise QuoteSystemError(
            f"Failed to initialize logging: {str(e)}",
            ErrorCodes.CONFIG_INVALID_FORMAT
        ) from e


# 自动初始化（使用配置文件）
initialize_logging(use_config_file=True)