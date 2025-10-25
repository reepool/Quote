"""
统一报告系统
提供可配置的模板化报告生成功能
"""

from .engine import ReportEngine
from .formatters import ReportFormatter
from .templates import TemplateManager
from .adapters import OutputAdapter

# 导入标准日志器
from utils.logging_manager import report_logger
from utils.config_manager import config_manager

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
                   output_format: str = None) -> str:
    """
    统一的报告生成接口

    Args:
        report_type: 报告类型 ('download_report', 'daily_update_report', 'gap_report', 'system_status', 'backup_result')
        data: 报告数据
        output_format: 输出格式 ('telegram', 'console', 'api')

    Returns:
        str: 格式化后的报告内容
    """
    # 如果未提供output_format，则从配置中读取默认值
    if output_format is None:
        report_config = config_manager.get_report_config()
        template_config = report_config.templates.get(report_type, {})
        output_format = template_config.get('output_format', 'telegram')

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

# 导出主要类
__all__ = [
    'ReportEngine',
    'ReportFormatter',
    'TemplateManager',
    'OutputAdapter',
    'generate_report',
    'reload_report_config'
]