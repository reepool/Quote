"""
报告生成引擎
核心的报告生成逻辑
"""

from typing import Dict, Any, Optional
from utils.logging_manager import report_logger, config_logger
from utils.config_manager import config_manager
from .templates import TemplateManager
from .formatters import ReportFormatter
from .adapters import OutputAdapter
from utils.singleton import singleton


@singleton
class ReportEngine:
    """报告生成引擎核心类"""

    def __init__(self):
        # 使用标准项目日志器
        self.logger = report_logger
        """
        初始化报告引擎
        """
        try:
            # 直接从配置管理器获取类型安全的报告配置对象
            report_config = config_manager.get_report_config()
            self.config = report_config

            # 将配置对象传递给子管理器
            self.template_manager = TemplateManager(report_config)
            self.formatter = ReportFormatter(report_config)
            self.adapter = OutputAdapter(report_config)

            config_logger.debug(f"[ReportEngine] Report engine initialized with config: {report_config}")

        except Exception as e:
            config_logger.error(f"[ReportEngine] Failed to initialize with report config: {e}")
            # 在配置失败时提供一个备用方案，以避免完全崩溃
            from utils.config_manager import ReportConfig
            self.config = ReportConfig()
            self.template_manager = TemplateManager(self.config)
            self.formatter = ReportFormatter(self.config)
            self.adapter = OutputAdapter(self.config)

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
        elif report_type == 'health_check_report':
            prepared_data = self._prepare_health_check_report_data(prepared_data)

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
        # 如果是错误报告，直接返回错误信息
        if data.get('status') == 'error':
            return data

        if data.get('non_trading_day'): # 非交易日报告
            data['date'] = data.get('date', '')
            # 格式化 calendar_updates 为字符串列表
            formatted_calendar_updates = []
            for ex, count in data.get('trading_calendar_updates', {}).items():
                formatted_calendar_updates.append(f"• {ex}: 更新 {count} 天")
            data['calendar_updates'] = '\n'.join(formatted_calendar_updates) if formatted_calendar_updates else '无更新'
        else: # 正常交易日更新报告
            summary = data.get('update_results', {})
            success_rate = summary.get('success_rate')
            if success_rate is None:
                total_checked = summary.get('total_instruments_checked', 0)
                if not total_checked:
                    total_checked = summary.get('success_count', 0) + summary.get('failure_count', 0)
                success_rate = (
                    summary.get('success_count', 0) / total_checked * 100
                    if total_checked > 0 else 0
                )

            data.update({
                'date': data.get('date', ''),
                'updated_instruments': summary.get('success_count', 0),
                'new_quotes': summary.get('total_quotes_added', 0),
                'success_rate': success_rate,
                'summary_note': summary.get('summary_note'),
                'exchange_stats': self._format_daily_exchange_stats_for_table(
                    summary.get('exchange_stats', {})
                ),
                'instrument_master_sync': summary.get('instrument_master_sync', data.get('instrument_master_sync')),
                'index_master_governance': summary.get(
                    'index_master_governance',
                    data.get('index_master_governance'),
                ),
                'catchup_stats': summary.get('catchup_stats', data.get('catchup_stats')),
            })

        data['instrument_master_sync_summary'] = self._format_instrument_master_sync_summary(
            data.get('instrument_master_sync')
        )
        data['index_master_governance_summary'] = self._format_index_master_governance_summary(
            data.get('index_master_governance')
        )
        data['daily_catchup_summary'] = self._format_daily_catchup_summary(
            data.get('catchup_stats')
        )

        # 确保所有报告都有一个明确的名称
        data['name'] = data.get('name', '每日数据更新报告')

        return data

    def _format_daily_exchange_stats_for_table(
        self,
        exchange_stats: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """Keep the daily Telegram exchange table compact and scalar-only."""
        if not isinstance(exchange_stats, dict):
            return {}

        display_stats: Dict[str, Dict[str, Any]] = {}
        for exchange, stats in exchange_stats.items():
            if not isinstance(stats, dict):
                continue
            display_stats[exchange] = {
                'success_count': stats.get('success_count', 0),
                'failure_count': stats.get('failure_count', 0),
                'quotes_added': stats.get('quotes_added', stats.get('quotes_count', 0)),
                'total_instruments': stats.get('total_instruments', stats.get('total_count', 0)),
            }
        return display_stats

    def _format_daily_catchup_summary(self, catchup_stats: Dict[str, Any]) -> str:
        """Format bounded daily catch-up diagnostics for operator reports."""
        if not isinstance(catchup_stats, dict) or not catchup_stats:
            return ''

        new_count = int(catchup_stats.get('new_instrument_count', 0) or 0)
        short_gap_count = int(catchup_stats.get('short_gap_count', 0) or 0)
        capped_count = int(catchup_stats.get('capped_count', 0) or 0)
        missing_listed = int(catchup_stats.get('skipped_missing_listed_date', 0) or 0)
        quotes_added = int(catchup_stats.get('catchup_quotes_added', 0) or 0)
        if not any((new_count, short_gap_count, capped_count, missing_listed, quotes_added)):
            return ''

        lines = [
            f"新股追补: {new_count}，短缺口追补: {short_gap_count}，追补行情: {quotes_added}",
        ]
        if capped_count or missing_listed:
            lines.append(f"窗口截断: {capped_count}，缺少上市日: {missing_listed}")

        samples = catchup_stats.get('samples') or []
        sample_parts = []
        for sample in samples[:5]:
            if not isinstance(sample, dict):
                continue
            sample_parts.append(
                f"{sample.get('instrument_id')} {sample.get('reason')} "
                f"{sample.get('fetch_start_date')}~{sample.get('end_date')} "
                f"rows={sample.get('quotes_added', 0)}"
            )
        if sample_parts:
            lines.append('样例: ' + '；'.join(sample_parts))
        return '\n'.join(lines)

    def _format_instrument_master_sync_summary(self, sync_result: Dict[str, Any]) -> str:
        """Format master-sync diagnostics for operator reports."""
        if not isinstance(sync_result, dict) or not sync_result:
            return ''

        status = sync_result.get('status', 'unknown')
        reason = sync_result.get('reason')
        if status == 'skipped':
            return f"状态: skipped\n原因: {reason or 'not provided'}"

        summary = sync_result.get('summary', {}) if isinstance(sync_result.get('summary'), dict) else {}
        lines = [
            f"状态: {status}",
            f"新增: {summary.get('added_instruments', 0)}，停用: {summary.get('deactivated_instruments', 0)}，活跃合计: {summary.get('active_count', 0)}",
        ]

        exchange_parts = []
        for exchange, item in (sync_result.get('exchanges') or {}).items():
            if not isinstance(item, dict):
                continue
            after = item.get('after') if isinstance(item.get('after'), dict) else {}
            exchange_parts.append(
                f"{exchange} 状态={item.get('status', 'unknown')} "
                f"活跃={after.get('active_count', 0)} "
                f"+{item.get('added_count', 0)}/-{item.get('deactivated_count', 0)}"
            )
        if exchange_parts:
            lines.append('市场: ' + '；'.join(exchange_parts))

        warnings = sync_result.get('warnings') or []
        errors = sync_result.get('errors') or []
        if warnings:
            lines.append('警告: ' + '；'.join(str(w) for w in warnings[:3]))
        if errors:
            lines.append('错误: ' + '；'.join(str(e) for e in errors[:3]))
        return '\n'.join(lines)

    def _format_index_master_governance_summary(self, governance: Dict[str, Any]) -> str:
        """Format concise A-share index governance diagnostics for Telegram reports."""
        if not isinstance(governance, dict) or not governance:
            return ''

        status = governance.get('status', 'unknown')
        reason = governance.get('reason')
        if status == 'skipped':
            return f"状态: skipped\n原因: {reason or 'not provided'}"

        summary = governance.get('summary', {}) if isinstance(governance.get('summary'), dict) else {}
        lines = [
            f"状态: {status}",
            (
                f"主数据写入: {summary.get('master_rows_saved', 0)}，"
                f"证据写入: {summary.get('evidence_rows_saved', 0)}，"
                f"活跃指数: {summary.get('active_count', 0)}"
            ),
            (
                f"停编跳过: {summary.get('lifecycle_skip_count', 0)}，"
                f"直接: {summary.get('direct_terminated_count', 0)}，"
                f"推断: {summary.get('inferred_terminated_count', 0)}，"
                f"metadata-only: {summary.get('metadata_only_legacy_deactivated_count', 0)}，"
                f"stale: {summary.get('stale_no_quote_count', 0)}"
            ),
        ]

        source_usage = summary.get('source_usage') or {}
        if source_usage:
            lines.append(
                '来源: ' + '；'.join(
                    f"{source}={count}" for source, count in list(source_usage.items())[:4]
                )
            )

        warnings = governance.get('warnings') or []
        errors = governance.get('errors') or []
        if warnings:
            lines.append('警告: ' + '；'.join(str(w) for w in warnings[:2]))
        if errors:
            lines.append('错误: ' + '；'.join(str(e) for e in errors[:2]))

        samples = summary.get('samples') or []
        sample_parts = []
        for sample in samples[:5]:
            if not isinstance(sample, dict):
                continue
            sample_parts.append(
                f"{sample.get('instrument_id')} {sample.get('state')} "
                f"{sample.get('confidence', '')}".strip()
            )
        if sample_parts:
            lines.append('样例: ' + '；'.join(sample_parts))
        return '\n'.join(lines)

    def _prepare_gap_report_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备缺口报告数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """
        summary = data.get('summary', {})
        repair_universe = data.get('repair_universe') or {}

        # 提取关键指标
        data.update({
            'total_gaps': summary.get('total_gaps', 0),
            'affected_stocks': summary.get('affected_stocks', 0),
            'severity_distribution': summary.get('severity_distribution', {}),
            'exchange_distribution': summary.get('exchange_distribution', {}),
            'top_affected_stocks': data.get('top_affected_stocks', []),
            'lifecycle_skipped_instruments': (
                summary.get('lifecycle_skipped_instruments')
                or repair_universe.get('skipped_instrument_count', 0)
            ),
            'lifecycle_skipped_gap_segments': (
                summary.get('lifecycle_skipped_gap_segments')
                or repair_universe.get('skipped_gap_segment_count', 0)
            ),
            'repair_universe_reason_distribution': repair_universe.get('reason_distribution', {}),
            'repair_universe_summary': data.get('repair_universe_summary', ''),
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

    def _prepare_health_check_report_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        准备健康检查报告数据

        Args:
            data: 原始数据

        Returns:
            Dict[str, Any]: 准备好的数据
        """

        # 这个方法保留为空，以备将来可能的扩展。
        
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
