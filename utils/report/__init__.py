"""
统一报告系统
提供可配置的模板化报告生成功能
"""

from .engine import ReportEngine
from .formatters import ReportFormatter
from .templates import TemplateManager
from .adapters import OutputAdapter

# 导入标准日志器
from utils import report_logger

# 全局报告引擎实例
_report_engine = None

def _get_report_engine() -> ReportEngine:
    """
    获取报告引擎实例，支持配置热读取

    Returns:
        ReportEngine: 报告引擎实例
    """
    global _report_engine
    if _report_engine is None:
        _report_engine = ReportEngine()
        report_logger.debug("[Report] Report engine initialized")
    return _report_engine

def generate_report(report_type: str, data: dict,
                   output_format: str = 'telegram') -> str:
    """
    统一的报告生成接口

    Args:
        report_type: 报告类型 ('download_report', 'daily_update_report', 'gap_report', 'system_status', 'backup_result')
        data: 报告数据
        output_format: 输出格式 ('telegram', 'console', 'api')

    Returns:
        str: 格式化后的报告内容
    """
    report_logger.info(f"[Report] Generating report: type={report_type}, format={output_format}, data_keys={list(data.keys())}")

    try:
        engine = _get_report_engine()
        result = engine.generate(report_type, data, output_format)
        report_logger.debug(f"[Report] Report generated successfully: type={report_type}, length={len(result)}")
        return result
    except Exception as e:
        report_logger.error(f"[Report] Failed to generate report: type={report_type}, error={str(e)}")
        raise

def reload_report_config() -> bool:
    """
    重载报告配置

    Returns:
        bool: 重载是否成功
    """
    global _report_engine
    try:
        report_logger.info("[Report] Reloading report configuration...")

        # 重新创建报告引擎实例
        _report_engine = ReportEngine()

        report_logger.info("[Report] Report configuration reloaded successfully")
        return True

    except Exception as e:
        report_logger.error(f"[Report] Failed to reload report configuration: {e}")
        return False

# 快捷方法
def format_download_report(data: dict, output_format: str = 'telegram') -> str:
    """格式化数据下载报告"""
    return generate_report('download_report', data, output_format)

def format_daily_update_report(data: dict, output_format: str = 'telegram') -> str:
    """格式化每日数据更新报告"""
    return generate_report('daily_update_report', data, output_format)

def format_gap_report(data: dict, output_format: str = 'console') -> str:
    """格式化数据缺口报告"""
    return generate_report('gap_report', data, output_format)

def format_system_status(data: dict, output_format: str = 'telegram') -> str:
    """格式化系统状态报告"""
    return generate_report('system_status', data, output_format)

def format_backup_result(data: dict, output_format: str = 'telegram') -> str:
    """格式化备份结果报告"""
    return generate_report('backup_result', data, output_format)

def format_health_check_report(data: dict, output_format: str = 'telegram') -> str:
    """格式化系统健康检查报告"""
    return generate_report('health_check_report', data, output_format)

def format_maintenance_report(data: dict, output_format: str = 'telegram') -> str:
    """格式化数据维护报告"""
    return generate_report('maintenance_report', data, output_format)

def format_cache_warm_up_report(data: dict, output_format: str = 'telegram') -> str:
    """格式化缓存预热报告"""
    return generate_report('cache_warm_up_report', data, output_format)

def format_trading_calendar_report(data: dict, output_format: str = 'telegram') -> str:
    """格式化交易日历更新报告"""
    return generate_report('trading_calendar_report', data, output_format)

# 导出主要类
__all__ = [
    'ReportEngine',
    'ReportFormatter',
    'TemplateManager',
    'OutputAdapter',
    'generate_report',
    'reload_report_config',
    'format_download_report',
    'format_daily_update_report',
    'format_gap_report',
    'format_system_status',
    'format_backup_result',
    'format_health_check_report',
    'format_maintenance_report',
    'format_cache_warm_up_report',
    'format_trading_calendar_report',
    'report_logger'
]