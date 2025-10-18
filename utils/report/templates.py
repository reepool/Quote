"""
报告模板管理器
负责加载和管理报告模板配置
"""

from typing import Dict, List, Any, Optional
from utils import config_manager


class TemplateManager:
    """报告模板管理器"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化模板管理器

        Args:
            config: 报告配置字典，如果为None则从config_manager获取
        """
        if config is None:
            from utils import config_manager
            full_config = config_manager.to_dict()
            config = full_config.get('report_config', {})

        self.config = config
        self.templates = config.get('templates', {})
        self.formats = config.get('formats', {})

    def get_template(self, report_type: str) -> Optional[Dict[str, Any]]:
        """
        获取指定类型的报告模板配置

        Args:
            report_type: 报告类型

        Returns:
            Dict[str, Any]: 模板配置，如果不存在返回None
        """
        return self.templates.get(report_type)

    def get_sections(self, report_type: str) -> List[Dict[str, Any]]:
        """
        获取指定报告类型的段落配置

        Args:
            report_type: 报告类型

        Returns:
            List[Dict[str, Any]]: 段落配置列表
        """
        template = self.get_template(report_type)
        if template:
            return template.get('sections', [])
        return []

    def get_format_config(self, output_format: str) -> Dict[str, Any]:
        """
        获取输出格式配置

        Args:
            output_format: 输出格式 ('telegram', 'console', 'api')

        Returns:
            Dict[str, Any]: 格式配置
        """
        return self.formats.get(output_format, {})

    def get_template_info(self, report_type: str) -> Dict[str, Any]:
        """
        获取模板基本信息

        Args:
            report_type: 报告类型

        Returns:
            Dict[str, Any]: 模板信息
        """
        template = self.get_template(report_type)
        if not template:
            return {}

        return {
            'name': template.get('name', ''),
            'emoji': template.get('emoji', ''),
            'sections_count': len(template.get('sections', [])),
            'sections': [section.get('name', '') for section in template.get('sections', [])]
        }

    def list_available_templates(self) -> List[str]:
        """
        获取所有可用的模板类型

        Returns:
            List[str]: 模板类型列表
        """
        return list(self.templates.keys())

    def validate_template(self, report_type: str) -> bool:
        """
        验证模板配置是否完整

        Args:
            report_type: 报告类型

        Returns:
            bool: 是否有效
        """
        template = self.get_template(report_type)
        if not template:
            return False

        # 检查必要字段
        required_fields = ['name', 'sections']
        for field in required_fields:
            if field not in template:
                return False

        # 检查段落配置
        sections = template.get('sections', [])
        if not sections:
            return False

        for section in sections:
            if 'name' not in section or 'type' not in section:
                return False

        return True