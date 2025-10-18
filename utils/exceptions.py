"""
统一异常定义模块
提供项目特定的异常类和错误处理机制
"""

from typing import Optional, Dict, Any


class QuoteSystemError(Exception):
    """行情系统基础异常类"""

    def __init__(self, message: str, error_code: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.context = context or {}

    def __str__(self) -> str:
        return f"[{self.error_code}] {self.message}"


class ConfigurationError(QuoteSystemError):
    """配置相关错误"""
    pass


class DataSourceError(QuoteSystemError):
    """数据源相关错误"""
    pass


class DatabaseError(QuoteSystemError):
    """数据库相关错误"""
    pass


class ValidationError(QuoteSystemError):
    """数据验证错误"""
    pass


class NetworkError(QuoteSystemError):
    """网络连接错误"""
    pass


class ExchangeMappingError(QuoteSystemError):
    """交易所映射错误"""
    pass


class CacheError(QuoteSystemError):
    """缓存相关错误"""
    pass


class TaskExecutionError(QuoteSystemError):
    """任务执行错误"""
    pass


class APIError(QuoteSystemError):
    """API相关错误"""
    pass


# 错误代码常量
class ErrorCodes:
    """错误代码常量"""

    # 配置错误
    CONFIG_NOT_FOUND = "CONFIG_001"
    CONFIG_INVALID_FORMAT = "CONFIG_002"
    CONFIG_MISSING_KEY = "CONFIG_003"

    # 数据源错误
    DATASOURCE_CONNECTION_FAILED = "DS_001"
    DATASOURCE_RATE_LIMIT = "DS_002"
    DATASOURCE_INVALID_RESPONSE = "DS_003"
    DATASOURCE_NOT_SUPPORTED = "DS_004"

    # 数据库错误
    DB_CONNECTION_FAILED = "DB_001"
    DB_QUERY_FAILED = "DB_002"
    DB_TRANSACTION_FAILED = "DB_003"
    DB_INTEGRITY_ERROR = "DB_004"

    # 验证错误
    VALIDATION_INVALID_SYMBOL = "VAL_001"
    VALIDATION_INVALID_DATE = "VAL_002"
    VALIDATION_INVALID_EXCHANGE = "VAL_003"
    VALIDATION_MISSING_REQUIRED_FIELD = "VAL_004"

    # 网络错误
    NETWORK_TIMEOUT = "NET_001"
    NETWORK_CONNECTION_ERROR = "NET_002"
    NETWORK_RATE_LIMIT = "NET_003"

    # 缓存错误
    CACHE_CONNECTION_FAILED = "CACHE_001"
    CACHE_SERIALIZATION_ERROR = "CACHE_002"
    CACHE_EXPIRED = "CACHE_003"

    # 任务错误
    TASK_NOT_FOUND = "TASK_001"
    TASK_EXECUTION_FAILED = "TASK_002"
    TASK_TIMEOUT = "TASK_003"


def create_error_response(error: QuoteSystemError,
                         include_traceback: bool = False) -> Dict[str, Any]:
    """创建标准化的错误响应"""
    response = {
        "error": True,
        "error_code": error.error_code,
        "message": error.message,
        "context": error.context
    }

    if include_traceback:
        import traceback
        response["traceback"] = traceback.format_exc()

    return response


def handle_exception(func):
    """统一异常处理装饰器"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except QuoteSystemError as e:
            # 已经是系统异常，直接重新抛出
            raise
        except Exception as e:
            # 转换为系统异常
            raise QuoteSystemError(
                f"Unexpected error in {func.__name__}: {str(e)}",
                error_code="UNEXPECTED_ERROR"
            ) from e

    return wrapper