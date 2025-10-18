"""
数据源复权配置管理
统一管理所有数据源的复权类型设置
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any

class AdjustmentType(Enum):
    """复权类型枚举 - Baostock专用"""
    BACK_ADJUSTED = "1"   # 后复权
    FRONT_ADJUSTED = "2"  # 前复权
    NON_ADJUSTED = "3"    # 不复权

class DataSourceAdjustment:
    """各数据源的复权配置"""

    # BaoStock配置
    BAOSTOCK = {
        'adjustflag': AdjustmentType.FRONT_ADJUSTED.value,
        'description': '前复权数据，适合历史数据分析',
        'factor': 1.0
    }

    # AkShare配置
    AKSHARE = {
        'adjust': 'qfq',  # 前复权
        'description': '前复权数据，适合历史数据分析',
        'factor': 1.0
    }

    # yFinance配置
    YFINANCE = {
        'auto_adjust': True,  # 默认前复权
        'description': '前复权数据，适合历史数据分析',
        'factor': 1.0
    }

    # Tushare配置
    TUSHARE = {
        'description': '前复权数据，适合历史数据分析',
        'factor': 1.0
    }

@dataclass
class AdjustmentConfig:
    """复权配置管理类"""

    @staticmethod
    def get_adjustment_config(data_source: str) -> Dict[str, Any]:
        """获取指定数据源的复权配置"""
        configs = {
            'baostock': DataSourceAdjustment.BAOSTOCK,
            'akshare': DataSourceAdjustment.AKSHARE,
            'yfinance': DataSourceAdjustment.YFINANCE,
            'tushare': DataSourceAdjustment.TUSHARE
        }
        return configs.get(data_source.lower(), {})

    @staticmethod
    def get_default_factor() -> float:
        """获取默认复权因子"""
        return 1.0

    @staticmethod
    def is_front_adjusted(data_source: str) -> bool:
        """检查数据源是否使用前复权"""
        config = AdjustmentConfig.get_adjustment_config(data_source)
        # 检查是否为前复权配置
        if data_source.lower() == 'baostock':
            return config.get('adjustflag') == AdjustmentType.FRONT_ADJUSTED.value
        elif data_source.lower() == 'akshare':
            return config.get('adjust') == 'qfq'
        elif data_source.lower() == 'yfinance':
            return config.get('auto_adjust', True)
        elif data_source.lower() == 'tushare':
            return True  # Tushare默认为前复权
        return True  # 默认为前复权

# 全局复权配置常量
ADJUSTMENT_TYPE_FRONT = "前复权"
ADJUSTMENT_TYPE_BACK = "后复权"
ADJUSTMENT_TYPE_NONE = "不复权"
DEFAULT_ADJUSTMENT_TYPE = ADJUSTMENT_TYPE_FRONT