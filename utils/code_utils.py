"""
代码转换工具函数
提供各种代码格式的转换功能
"""

from typing import Dict


class CodeConverter:
    """代码转换器"""

    def __init__(self):
        # 交易所代码映射：标准格式 -> 数据库格式
        self.exchange_to_db: Dict[str, str] = {
            'SZSE': 'SZ',
            'SSE': 'SH',
            'BSE': 'BSE',
            'HKEX': 'HKEX',
            'NASDAQ': 'NASDAQ',
            'NYSE': 'NYSE'
        }

        # 反向映射：数据库格式 -> 标准格式
        self.db_to_exchange: Dict[str, str] = {v: k for k, v in self.exchange_to_db.items()}

    def convert_to_database_format(self, instrument_id: str) -> str:
        """
        将标准交易所代码转换为数据库存储格式

        Args:
            instrument_id: 股票代码，格式如 "001872.SZSE"

        Returns:
            转换后的股票代码，格式如 "001872.SZ"
        """
        parts = instrument_id.split('.')
        if len(parts) != 2:
            return instrument_id

        symbol, exchange = parts
        if exchange.upper() in self.exchange_to_db:
            return f"{symbol}.{self.exchange_to_db[exchange.upper()]}"

        return instrument_id

    def convert_to_standard_format(self, instrument_id: str) -> str:
        """
        将数据库格式转换为标准交易所代码格式

        Args:
            instrument_id: 数据库中的股票代码，格式如 "001872.SZ"

        Returns:
            转换后的股票代码，格式如 "001872.SZSE"
        """
        parts = instrument_id.split('.')
        if len(parts) != 2:
            return instrument_id

        symbol, exchange = parts
        if exchange.upper() in self.db_to_exchange:
            return f"{symbol}.{self.db_to_exchange[exchange.upper()]}"

        return instrument_id

    def is_valid_instrument_format(self, instrument_id: str, format_type: str = 'standard') -> bool:
        """
        验证股票代码格式

        Args:
            instrument_id: 股票代码
            format_type: 格式类型 ('standard' 或 'database')

        Returns:
            是否为有效格式
        """
        import re

        if format_type == 'standard':
            # 标准格式：数字 + . + 交易所代码 (SSE, SZSE, HKEX, NASDAQ, NYSE)
            pattern = r'^\d+\.(SSE|SZSE|HKEX|NASDAQ|NYSE)$'
        else:
            # 数据库格式：数字 + . + 交易所代码 (SH, SZ, BSE, HKEX, NASDAQ, NYSE)
            pattern = r'^\d+\.(SH|SZ|BSE|HKEX|NASDAQ|NYSE)$'

        return bool(re.match(pattern, instrument_id, re.IGNORECASE))

    def extract_exchange(self, instrument_id: str) -> str:
        """
        从股票代码中提取交易所代码

        Args:
            instrument_id: 股票代码

        Returns:
            交易所代码
        """
        parts = instrument_id.split('.')
        if len(parts) == 2:
            return parts[1].upper()
        return ""

    def extract_symbol(self, instrument_id: str) -> str:
        """
        从股票代码中提取股票代码部分

        Args:
            instrument_id: 完整的股票代码

        Returns:
            股票代码部分
        """
        parts = instrument_id.split('.')
        if len(parts) >= 1:
            return parts[0]
        return instrument_id


# 全局实例
code_converter = CodeConverter()


# 便捷函数
def convert_to_database_format(instrument_id: str) -> str:
    """转换到数据库格式的便捷函数"""
    return code_converter.convert_to_database_format(instrument_id)


def convert_to_standard_format(instrument_id: str) -> str:
    """转换到标准格式的便捷函数"""
    return code_converter.convert_to_standard_format(instrument_id)


def is_valid_standard_format(instrument_id: str) -> bool:
    """验证标准格式的便捷函数"""
    return code_converter.is_valid_instrument_format(instrument_id, 'standard')


def is_valid_database_format(instrument_id: str) -> bool:
    """验证数据库格式的便捷函数"""
    return code_converter.is_valid_instrument_format(instrument_id, 'database')