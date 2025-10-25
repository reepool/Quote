"""
工具模块包
提供项目所需的通用工具和功能
"""

# 导出核心工具
from .config_manager import config_manager, LoggingConfig, LoggingModuleConfig
from .exceptions import (
    QuoteSystemError,
    ConfigurationError,
    DataSourceError,
    DatabaseError,
    ValidationError,
    NetworkError,
    ExchangeMappingError,
    CacheError,
    TaskExecutionError,
    APIError,
    ErrorCodes,
    create_error_response,
    handle_exception
)
from .logging_manager import (
    LogContext,
    log_execution,
    log_performance,
    log_data_operation,
    MetricsLogger,
    logging_manager,
    logger,
    data_manager_metrics,
    data_source_metrics,
    database_metrics,
    cache_metrics,
    api_metrics,
    LogConfig,
    initialize_logging,
    ModuleLoggers,
    dm_logger,
    db_logger,
    ds_logger,
    api_logger,
    scheduler_logger,
    monitor_logger,
    config_logger,
    tgbot_logger,
    task_manager_logger,
    report_logger,
    akshare_logger,
    yfinance_logger,
    tushare_logger,
    baostock_logger,
    cache_logger,
    validation_logger,
    date_utils_logger
)
from .process_manager import ProcessManager, get_process_manager
from .security_utils import (
    SecurityValidator,
    InputValidator,
    SecurityHeaders,
    RateLimiter,
    api_rate_limiter,
    data_download_limiter
)
# 性能监控模块（存在循环依赖问题，暂时注释掉）
# from .performance_utils import (
#     PerformanceMonitor,
#     ResourceMonitor,
#     performance_monitor,
#     resource_monitor,
#     MemoryProfiler,
#     PerformanceOptimizer
# )
from .exchange_utils import exchange_mapper, get_region_from_exchange, get_exchanges_from_region
from .date_utils import DateUtils, get_shanghai_time
from .cache import cache_manager
from .path_utils import BASE_DIR, CONFIG_DIR, LOG_DIR, DATA_DIR, SERVICE_DIR
from .singleton import singleton
from .code_utils import (
    code_converter,
    convert_to_database_format,
    convert_to_standard_format,
    is_valid_standard_format,
    is_valid_database_format
)
# 延迟导入validation以避免pydantic依赖问题
try:
    from .validation import DataValidator, QueryValidator
except ImportError:
    DataValidator = None
    QueryValidator = None
# Telegram Bot
from .tgbot import TelegramBot
# 统一报告系统 (只导入 generate_report)
from .report import generate_report
# 交易日历工具（延迟导入以避免循环依赖）

# 版本信息
__version__ = "1.0.0"
__author__ = "Reepool"

__all__ = [
    # 配置管理
    "config_manager",

    # 异常处理
    "QuoteSystemError",
    "ConfigurationError",
    "DataSourceError",
    "DatabaseError",
    "ValidationError",
    "NetworkError",
    "ExchangeMappingError",
    "CacheError",
    "TaskExecutionError",
    "APIError",
    "ErrorCodes",
    "create_error_response",
    "handle_exception",

    # 日志工具
    "LogContext",
    "log_execution",
    "log_performance",
    "log_data_operation",
    "MetricsLogger",
    "logging_manager",
    "logger",
    "data_manager_metrics",
    "data_source_metrics",
    "database_metrics",
    "cache_metrics",
    "api_metrics",
    "LogConfig",
    "initialize_logging",
    "ModuleLoggers",
    "dm_logger",
    "db_logger",
    "ds_logger",
    "api_logger",
    "scheduler_logger",
    "config_logger",
    "tgbot_logger",
    "task_manager_logger",
    "report_logger",
    "akshare_logger",
    "yfinance_logger",
    "cache_logger",
    "validation_logger",
    "date_utils_logger",

    # 安全工具
    "SecurityValidator",
    "InputValidator",
    "SecurityHeaders",
    "RateLimiter",
    "api_rate_limiter",
    "data_download_limiter",

    # 性能工具（暂时注释掉）
    # "PerformanceMonitor",
    # "ResourceMonitor",
    # "performance_monitor",
    # "resource_monitor",
    # "MemoryProfiler",
    # "PerformanceOptimizer",

    # 业务工具
    "exchange_mapper",
    "get_region_from_exchange",
    "get_exchanges_from_region",
    "DateUtils",
    "get_shanghai_time",
    "cache_manager",

    # 路径工具
    "BASE_DIR",
    "CONFIG_DIR",
    "LOG_DIR",
    "DATA_DIR",
    "SERVICE_DIR",

    # 单例工具
    "singleton",

    # Telegram工具
    "TelegramBot",

    # 进程管理
    "ProcessManager",
    "get_process_manager",

    # 统一报告系统
    "generate_report", # 仅导出 generate_report

    # 代码转换工具
    "code_converter",
    "convert_to_database_format",
    "convert_to_standard_format",
    "is_valid_standard_format",
    "is_valid_database_format",

    ]

# 条件导出验证器（如果可用）
if DataValidator is not None:
    __all__.extend(["DataValidator", "QueryValidator"])