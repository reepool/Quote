"""
报告输出适配器
负责适配不同输出格式的特殊需求
"""

from datetime import datetime
from typing import Dict, Any
from utils.config_manager import ReportConfig
from utils.logging_manager import report_logger




class OutputAdapter:
    """报告输出格式适配器"""

    def __init__(self, config: ReportConfig):
        """
        初始化适配器

        Args:
            config: 报告配置对象
        """
        self.config = config
        self.format_configs = config.formats

    def adapt(self, content: str, output_format: str) -> str:
        """
        适配输出格式

        Args:
            content: 原始内容
            output_format: 输出格式

        Returns:
            str: 适配后的内容
        """
        if output_format == 'telegram':
            report_logger.debug("[Report] Adapted for Telegram")
            return self._adapt_telegram(content)
        elif output_format == 'console':
            report_logger.debug("[Report] Adapted for Console")
            return self._adapt_console(content)
        elif output_format == 'api':
            report_logger.debug("[Report] Adapted for API")
            return self._adapt_api(content)
        else:
            report_logger.warning(f"[Report] Unsupported output format: {output_format}")
        
        return content

    def _adapt_telegram(self, content: str) -> str:
        """
        Telegram格式适配

        Args:
            content: 原始内容

        Returns:
            str: Telegram适配后的内容
        """
        return content

    def _adapt_console(self, content: str) -> str:
        """
        控制台格式适配

        Args:
            content: 原始内容

        Returns:
            str: 控制台适配后的内容
        """
        config = self.format_configs.get('console', {})
        max_width = config.get('max_width', 100)

        # 移除Markdown格式
        content = self._remove_markdown_formatting(content)

        # 处理长行
        lines = content.split('\n')
        adapted_lines = []

        for line in lines:
            if len(line) > max_width:
                # 分割长行
                words = line.split(' ')
                current_line = ''
                for word in words:
                    if len(current_line + ' ' + word) <= max_width:
                        current_line += (' ' if current_line else '') + word
                    else:
                        if current_line:
                            adapted_lines.append(current_line)
                        current_line = word
                if current_line:
                    adapted_lines.append(current_line)
            else:
                adapted_lines.append(line)

        report_logger.debug(f"[Report] Adapted for Console: {adapted_lines}")
        return '\n'.join(adapted_lines)

    def _adapt_api(self, content: str) -> str:
        """
        API格式适配

        Args:
            content: 原始内容

        Returns:
            str: API适配后的内容（JSON格式）
        """
        config = self.format_configs.get('api', {})

        if config.get('include_raw_data', False):
            # 返回结构化数据
            return self._convert_to_api_format(content)
        else:
            # 返回简单文本
            return content

    def _remove_markdown_formatting(self, text: str) -> str:
        """
        移除Markdown格式

        Args:
            text: 原始文本

        Returns:
            str: 移除格式后的文本
        """
        import re

        # 移除粗体格式
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        # 移除斜体格式
        text = re.sub(r'_([^_]+)_', r'\1', text)
        # 移除代码格式
        text = re.sub(r'`([^`]+)`', r'\1', text)
        # 移除链接格式
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)

        return text

    def _convert_to_api_format(self, content: str) -> str:
        """
        转换为API格式

        Args:
            content: 原始内容

        Returns:
            str: JSON格式的API响应
        """
        import json

        # 尝试解析内容并转换为结构化数据
        try:
            # 简单的内容解析逻辑
            lines = content.split('\n')
            data = {
                'content': content,
                'formatted_at': datetime.now().isoformat(),
                'metadata': {
                    'line_count': len(lines),
                    'char_count': len(content)
                }
            }

            report_logger.debug(f"[Report] Converted to API format: {data}")
            return json.dumps(data, ensure_ascii=False, indent=2)
        except Exception:
            # 如果解析失败，返回简单格式
            report_logger.warning(f"[Report] Failed to convert to API format: {content}")
            return json.dumps({
                'content': content,
                'formatted_at': datetime.now().isoformat()
            }, ensure_ascii=False)

    def get_supported_formats(self) -> list:
        """
        获取支持的输出格式列表

        Returns:
            list: 支持的格式列表
        """
        return list(self.format_configs.keys())

    def validate_format(self, output_format: str) -> bool:
        """
        验证输出格式是否支持

        Args:
            output_format: 输出格式

        Returns:
            bool: 是否支持
        """
        return output_format in self.get_supported_formats()