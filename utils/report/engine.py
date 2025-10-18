"""
报告生成引擎
核心的报告生成逻辑
"""

from typing import Dict, Any, Optional
from utils import config_manager, report_logger
from .templates import TemplateManager
from .formatters import ReportFormatter
from .adapters import OutputAdapter


class ReportEngine:
    """报告生成引擎核心类"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # 使用标准项目日志器
        self.logger = report_logger
        """
        初始化报告引擎

        Args:
            config: 报告配置，如果为None则从config_manager获取
        """
        if config is None:
            from utils import config_manager
            full_config = config_manager.to_dict()
            config = full_config.get('report_config', {})

        self.config = config
        self.template_manager = TemplateManager(config)
        self.formatter = ReportFormatter(config)
        self.adapter = OutputAdapter(config)

    def generate(self, report_type: str, data: Dict[str, Any],
                output_format: str = 'telegram') -> str:
        """
        生成报告的统一接口

        Args:
            report_type: 报告类型
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 生成的报告内容

        Raises:
            ValueError: 当报告类型不支持时
            ValueError: 当输出格式不支持时
        """
        report_logger.debug(f"[ReportEngine] Starting report generation: type={report_type}, format={output_format}")

        # 验证输入参数
        if not self.template_manager.validate_template(report_type):
            report_logger.error(f"[ReportEngine] Invalid report type: {report_type}")
            raise ValueError(f"Unsupported or invalid report type: {report_type}")

        if not self.adapter.validate_format(output_format):
            report_logger.error(f"[ReportEngine] Invalid output format: {output_format}")
            raise ValueError(f"Unsupported output format: {output_format}")

        # 获取模板配置
        template = self.template_manager.get_template(report_type)
        if not template:
            report_logger.error(f"[ReportEngine] Template not found for report type: {report_type}")
            raise ValueError(f"Template not found for report type: {report_type}")

        report_logger.debug(f"[ReportEngine] Template found: {template.get('name', report_type)}")

        # 准备数据
        prepared_data = self._prepare_data(report_type, data, template)
        report_logger.debug(f"[ReportEngine] Data prepared with {len(prepared_data)} fields")

        # 格式化报告
        formatted_content = self.formatter.format(template, prepared_data, output_format)

        # 输出适配
        final_content = self.adapter.adapt(formatted_content, output_format)

        return final_content

    def _prepare_data(self, report_type: str, data: Dict[str, Any],
                     template: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备报告数据

        Args:
            report_type: 报告类型
            data: 原始数据
            template: 模板配置

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        # 复制数据以避免修改原始数据
        prepared_data = data.copy()

        # 添加模板相关信息
        prepared_data.update({
            'report_type': report_type,
            'name': template.get('name', ''),
            'emoji': template.get('emoji', ''),
            'generated_at': self._get_current_time(),
            'source': 'Quote System'
        })

        # 根据报告类型进行特殊处理
        if report_type == 'download_report':
            prepared_data = self._prepare_download_report_data(prepared_data)
        elif report_type == 'daily_update_report':
            prepared_data = self._prepare_daily_update_report_data(prepared_data)
        elif report_type == 'gap_report':
            prepared_data = self._prepare_gap_report_data(prepared_data)
        elif report_type == 'system_status':
            prepared_data = self._prepare_system_status_data(prepared_data)
        elif report_type == 'backup_result':
            prepared_data = self._prepare_backup_result_data(prepared_data)

        return prepared_data

    def _prepare_download_report_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备下载报告数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        summary = data.get('summary', {})
        performance = data.get('performance_metrics', {})

        # 提取关键指标
        data.update({
            'total_instruments': summary.get('processed_instruments', 0),
            'success_count': summary.get('success_count', 0),
            'failure_count': summary.get('failure_count', 0),
            'total_quotes': summary.get('total_quotes', 0),
            'success_rate': summary.get('success_rate', 0),
            'elapsed_time': performance.get('elapsed_time', ''),
            'exchange_stats': data.get('exchange_stats', {})
        })

        return data

    def _prepare_daily_update_report_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备每日更新报告数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        summary = data.get('summary', {})

        # 提取关键指标
        data.update({
            'updated_instruments': summary.get('updated_instruments', 0),
            'new_quotes': summary.get('new_quotes_added', 0),
            'success_rate': summary.get('success_rate', 0),
            'target_date': summary.get('target_date', ''),
            'exchange_stats': data.get('exchange_stats', {})
        })

        return data

    def _prepare_gap_report_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备缺口报告数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        summary = data.get('summary', {})

        # 提取关键指标
        data.update({
            'total_gaps': summary.get('total_gaps', 0),
            'affected_stocks': summary.get('affected_stocks', 0),
            'severity_distribution': summary.get('severity_distribution', {}),
            'exchange_distribution': summary.get('exchange_distribution', {}),
            'top_affected_stocks': data.get('top_affected_stocks', [])
        })

        return data

    def _prepare_system_status_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备系统状态数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        # 保持原始数据结构，添加系统信息
        data.update({
            'system_status': data,
            'timestamp': self._get_current_time()
        })

        return data

    def _prepare_backup_result_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备备份结果数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        # 提取关键信息
        backup_info = {
            'success': data.get('success', False),
            'backup_file': data.get('backup_file', ''),
            'file_size': data.get('file_size', 0),
            'duration': data.get('duration', 0),
            'error_message': data.get('error_message', '')
        }

        data.update(backup_info)

        return data

    def _get_current_time(self) -> str:
        """
        获取当前时间字符串

        Returns:
            str: 当前时间
        """
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_available_report_types(self) -> list:
        """
        获取可用的报告类型

        Returns:
            list: 报告类型列表
        """
        return self.template_manager.list_available_templates()

    def get_available_formats(self) -> list:
        """
        获取可用的输出格式

        Returns:
            list: 输出格式列表
        """
        return self.adapter.get_supported_formats()

    def validate_report_request(self, report_type: str, output_format: str) -> bool:
        """
        验证报告请求是否有效

        Args:
            report_type: 报告类型
            output_format: 输出格式

        Returns:
            bool: 是否有效
        """
        return (self.template_manager.validate_template(report_type) and
                self.adapter.validate_format(output_format))

    def get_template_info(self, report_type: str) -> Dict[str, Any]:
        """
        获取模板信息

        Args:
            report_type: 报告类型

        Returns:
            Dict[str, Any]: 模板信息
        """
        return self.template_manager.get_template_info(report_type)