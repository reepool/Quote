"""
统一报告格式化器
负责将数据格式化为各种报告格式
"""

from datetime import datetime
from typing import Dict, List, Any
from .templates import TemplateManager
from utils.config_manager import ReportConfig
from utils.logging_manager import report_logger




class ReportFormatter:
    """统一报告格式化器"""

    def __init__(self, config: ReportConfig):
        """
        初始化格式化器

        Args:
            config: 报告配置对象
        """
        self.config = config
        self.template_manager = TemplateManager(config)

    def format(self, template: Dict[str, Any], data: Dict[str, Any],
               output_format: str) -> str:
        """
        格式化报告

        Args:
            template: 模板配置
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 格式化后的报告内容
        """
        sections = []

        # 格式化各个段落
        for section_config in template.get('sections', []):
            section_content = self._format_section(
                section_config, data, output_format
            )
            if section_content:
                sections.append(section_content)

        # 组合最终报告
        return self._combine_sections(sections, template, output_format)

    def _format_section(self, section: Dict[str, Any], data: Dict[str, Any],
                       output_format: str) -> str:
        """
        格式化单个段落

        Args:
            section: 段落配置
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 格式化后的段落内容
        """
        # 检查条件是否满足
        condition = section.get('condition')
        if condition:
            try:
                # 将 data 字典解包到 eval 的局部命名空间中
                if not eval(condition, {}, data):
                    return ""  # 条件不满足，不渲染此段落
            except Exception as e:
                report_logger.warning(f"Error evaluating condition '{condition}': {e}")
                return "" # 条件评估出错，不渲染

        section_type = section.get('type')

        if section_type == 'static':
            return self._format_static_section(section, data)
        elif section_type == 'metrics':
            return self._format_metrics_section(section, data, output_format)
        elif section_type == 'table':
            return self._format_table_section(section, data, output_format)
        elif section_type == 'list':
            return self._format_list_section(section, data, output_format)
        elif section_type == 'status':
            return self._format_status_section(section, data, output_format)
        else:
            report_logger.error(f"Invalid section type: {section_type}")

        return ""

    def _format_static_section(self, section: Dict[str, Any], data: Dict[str, Any]) -> str:
        """
        格式化静态段落

        Args:
            section: 段落配置
            data: 报告数据

        Returns:
            str: 格式化后的段落内容
        """
        content = section.get('content', '')
        template_data = {
            'name': data.get('name', ''),
            'time': datetime.now().strftime('%H:%M'),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'source': 'Quote System',
            **data
        }

        # 使用 format_map 和一个带有默认值的字典，以避免KeyError
        class SafeDict(dict):
            def __missing__(self, key):
                # 如果模板中的变量在数据中找不到，用占位符本身替换，并加上标记
                return f"{{{key}}}"

        safe_template_data = SafeDict(template_data)
        report_logger.debug(f"Formatting static section with template data: {template_data}")
        return content.format_map(safe_template_data)

    def _format_metrics_section(self, section: Dict[str, Any], data: Dict[str, Any],
                              output_format: str) -> str:
        """
        格式化指标段落

        Args:
            section: 段落配置
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 格式化后的段落内容
        """
        fields = section.get('fields', [])
        lines = []
        title = section.get('title')

        if title:
            lines.append(f"*{title}*")

        for field in fields:
            value = self._get_nested_value(data, field)
            if value is not None:
                formatted_value = self._format_value(value, field, output_format)
                label = self._get_field_label(field)
                # 如果有标题，增加缩进
                prefix = "  •" if title else "•"
                lines.append(f"{prefix} {label}: {formatted_value}")

        return '\n'.join(lines)

    def _format_table_section(self, section: Dict[str, Any], data: Dict[str, Any],
                             output_format: str) -> str:
        """
        格式化表格段落

        Args:
            section: 段落配置
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 格式化后的段落内容
        """
        data_source = section.get('data_source')
        table_data = self._get_nested_value(data, data_source)

        if not table_data or not isinstance(table_data, dict):
            return ""

        # 尝试使用prettytable生成更美观的表格
        try:
            from prettytable import PrettyTable
            
            # 动态确定表头
            headers = ["Exchange"]
            first_item = next(iter(table_data.values()), {}) # 获取table_data字典的第一个值（exchange stats，不包含键"SSE"和"SZSE"）变成迭代器(iterator)，并取出第一个元素，目的是用这个元素来构造列头
            if isinstance(first_item, dict):
                for key in first_item.keys():
                    headers.append(self._get_field_label(key))
            
            table = PrettyTable(headers)
            table.align = "l"

            for exchange, stats in table_data.items():
                row = [exchange]
                if isinstance(stats, dict):
                    for key in first_item.keys():
                        value = stats.get(key)
                        row.append(self._format_value(value, key, output_format))
                table.add_row(row)
            
            if output_format == 'console':
                return str(table)
            elif output_format == 'telegram':
                # 对于Telegram，使用无边框表格并包裹在等宽字体块中以保证对齐
                table_string = table.get_string(header=True, border=False)
                return f"```{table_string}```"
            else: # for other formats like api
                return table.get_string(header=True, border=False)

        except ImportError:
            # 如果没有prettytable，使用简单的手动格式化
            lines = []
            for key, value in table_data.items():
                if isinstance(value, dict):
                    # 处理嵌套字典
                    lines.append(f"*{key}*:")
                    for sub_key, sub_value in value.items():
                        formatted_value = self._format_value(sub_value, sub_key, output_format)
                        label = self._get_field_label(sub_key)
                        lines.append(f"  - {label}: {formatted_value}")
                else:
                    formatted_value = self._format_value(value, key, output_format)
                    lines.append(f"• {key}: {formatted_value}")
            return '\n'.join(lines)

    def _format_list_section(self, section: Dict[str, Any], data: Dict[str, Any],
                           output_format: str) -> str:
        """
        格式化列表段落

        Args:
            section: 段落配置
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 格式化后的段落内容
        """
        data_source = section.get('data_source')
        list_data = self._get_nested_value(data, data_source)

        if not list_data or not isinstance(list_data, (list, dict)):
            return ""

        lines = []
        title = section.get('title')
        if title:
            lines.append(f"*{title}*")

        if isinstance(list_data, list):
            for i, item in enumerate(list_data[:10]):  # 限制显示数量
                if isinstance(item, dict):
                    item_str = self._format_dict_item(item, section, output_format)
                    # 如果有标题，增加缩进
                    prefix = f"  {i+1}." if title else f"{i+1}."
                    lines.append(f"{prefix} {item_str}")
                else:
                    prefix = "  •" if title else "•"
                    lines.append(f"{prefix} {item}")
        elif isinstance(list_data, dict):
            for key, value in list_data.items():
                formatted_value = self._format_value(value, key, output_format)
                lines.append(f"• {key}: {formatted_value}")

        return '\n'.join(lines)

    def _format_status_section(self, section: Dict[str, Any], data: Dict[str, Any],
                             output_format: str) -> str:
        """
        格式化状态段落

        Args:
            section: 段落配置
            data: 报告数据
            output_format: 输出格式

        Returns:
            str: 格式化后的段落内容
        """
        data_source = section.get('data_source')
        status_value = self._get_nested_value(data, data_source)

        if status_value is None:
            return ""

        lines = []
        if isinstance(status_value, dict): # 如果状态数据是字典，则遍历显示
            for key, value in status_value.items():
                status_icon = "✅" if value else "❌"
                lines.append(f"{status_icon} {key}: {'正常' if value else '异常'}")
            return '\n'.join(lines)
        else:
            # 如果状态数据是字符串或数字，则直接显示
            status_text = str(status_value)
            if status_text.lower() in ('healthy', 'success'):
                return f"任务执行状态: ✅ {status_text}"
            elif status_text.lower() in ('warning', 'error'):
                return f"任务执行状态: ❌ {status_text}"
            else:
                return f"任务执行状态: ℹ️ {status_text}"

    def _combine_sections(self, sections: List[str], template: Dict[str, Any],
                         output_format: str) -> str:
        """
        组合段落为最终报告

        Args:
            sections: 格式化后的段落列表
            template: 模板配置
            output_format: 输出格式

        Returns:
            str: 最终报告内容
        """
        if not sections:
            return ""

        separator = self._get_separator(output_format)
        report = separator.join(sections)

        report_logger.debug(f"Formatted report: {report}")

        return report

    def _get_nested_value(self, data: Dict[str, Any], key_path: str) -> Any:
        """
        获取嵌套字典中的值

        Args:
            data: 数据字典
            key_path: 键路径，如 'summary.total_instruments'

        Returns:
            Any: 找到的值，如果不存在返回None
        """
        if not key_path:
            return None

        keys = key_path.split('.')
        value = data

        for key in keys:
            if not isinstance(value, dict) or key not in value:
                return None
            value = value[key]
        return value

    def _format_value(self, value: Any, field_name: str, output_format: str) -> str:
        """
        格式化值

        Args:
            value: 原始值
            field_name: 字段名
            output_format: 输出格式

        Returns:
            str: 格式化后的值
        """
        if value is None:
            return "N/A"

        # 处理不同类型的值
        if isinstance(value, (int, float)):
            if 'rate' in field_name.lower() or 'percentage' in field_name.lower():
                return f"{value:.1f}%"
            elif 'count' in field_name.lower() or 'total' in field_name.lower():
                return f"{value:,}"  # 千分位分隔
            elif 'size' in field_name.lower() or 'bytes' in field_name.lower():
                if value >= 1024*1024:
                    return f"{value/1024/1024:.2f}MB"
                elif value >= 1024:
                    return f"{value/1024:.2f}KB"
                else:
                    return f"{value}B"
            else:
                return str(value)
        elif isinstance(value, bool):
            return "是" if value else "否"
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return str(value)

    def _get_field_label(self, field_name: str) -> str:
        """
        获取字段显示标签

        Args:
            field_name: 字段名

        Returns:
            str: 显示标签
        """
        labels = self.config.field_labels

        return labels.get(field_name, field_name.replace('_', ' ').title())

    def _format_dict_item(self, item: Dict[str, Any], section_config: Dict[str, Any], output_format: str) -> str:
        """
        格式化字典项

        Args:
            item: 字典项
            section_config: 列表段落的配置
            output_format: 输出格式

        Returns:
            str: 格式化后的字符串
        """
        display_format = section_config.get('display_format')

        if display_format:
            # 使用配置的格式字符串
            class SafeDict(dict):
                def __missing__(self, key):
                    return f"{{{key}}}"
            
            # 增加对Telegram交互式链接的支持
            if output_format == 'telegram' and 'symbol' in item and 'exchange' in item:
                # 创建一个副本以避免修改原始数据
                interactive_item = item.copy()
                symbol = interactive_item['symbol']
                exchange = interactive_item['exchange']
                instrument_id = f"{symbol}.{exchange}"
                # 将 symbol 格式化为可点击的命令
                interactive_item['symbol'] = f"[{symbol}](command:/detail_{instrument_id.replace('.', '_')})"
                return display_format.format_map(SafeDict(interactive_item))
            else:
                return display_format.format_map(SafeDict(item))
        else:
            # 回退到旧的硬编码逻辑
            if 'symbol' in item:
                return f"{item.get('symbol', '')} ({item.get('exchange', '')})"
            elif 'name' in item:
                return item['name']
            else:
                return str(item)

    def _get_separator(self, output_format: str) -> str:
        """
        获取段落分隔符

        Args:
            output_format: 输出格式

        Returns:
            str: 分隔符
        """
        separators = {
            'telegram': '\n\n',
            'console': '\n\n',
            'api': ', '
        }

        return separators.get(output_format, '\n\n')