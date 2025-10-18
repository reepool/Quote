"""
统一报告格式化器
负责将数据格式化为各种报告格式
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
from .templates import TemplateManager


class ReportFormatter:
    """统一报告格式化器"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化格式化器

        Args:
            config: 报告配置
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

        try:
            return content.format(**template_data)
        except KeyError as e:
            return content  # 如果模板变量不存在，返回原内容

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

        for field in fields:
            value = self._get_nested_value(data, field)
            if value is not None:
                formatted_value = self._format_value(value, field, output_format)
                label = self._get_field_label(field)
                lines.append(f"• {label}: {formatted_value}")

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

        lines = []
        for key, value in table_data.items():
            if isinstance(value, dict):
                # 处理嵌套字典
                for sub_key, sub_value in value.items():
                    formatted_value = self._format_value(sub_value, sub_key, output_format)
                    lines.append(f"• {key}.{sub_key}: {formatted_value}")
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
        if isinstance(list_data, list):
            for i, item in enumerate(list_data[:10]):  # 限制显示数量
                if isinstance(item, dict):
                    item_str = self._format_dict_item(item, output_format)
                    lines.append(f"{i+1}. {item_str}")
                else:
                    lines.append(f"• {item}")
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
        status_data = self._get_nested_value(data, data_source)

        if not status_data:
            return ""

        if isinstance(status_data, dict):
            lines = []
            for key, value in status_data.items():
                status_icon = "✅" if value else "❌"
                lines.append(f"{status_icon} {key}: {'正常' if value else '异常'}")
            return '\n'.join(lines)
        else:
            return f"状态: {status_data}"

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

        # 添加模板前缀
        emoji = template.get('emoji', '')
        name = template.get('name', '')

        if emoji and name:
            prefix = f"{emoji} *{name}*"
            report = f"{prefix}\n\n{report}"

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
        keys = key_path.split('.')
        value = data

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None

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
        labels = {
            'total_instruments': '处理股票',
            'success_count': '成功数量',
            'failure_count': '失败数量',
            'total_quotes': '行情数据',
            'success_rate': '成功率',
            'updated_instruments': '更新股票',
            'new_quotes': '新增行情',
            'total_gaps': '总缺口数',
            'affected_stocks': '受影响股票',
            'backup_file': '备份文件',
            'file_size': '文件大小',
            'duration': '耗时'
        }

        return labels.get(field_name, field_name.replace('_', ' ').title())

    def _format_dict_item(self, item: Dict[str, Any], output_format: str) -> str:
        """
        格式化字典项

        Args:
            item: 字典项
            output_format: 输出格式

        Returns:
            str: 格式化后的字符串
        """
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