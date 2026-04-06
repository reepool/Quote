"""
数据源复权配置管理
统一管理所有数据源的复权类型设置

改造说明:
  所有数据源统一下载非复权(原始)数据, 复权由 utils.adjustment.AdjustmentEngine 动态计算.
  BaoStock 同时通过 query_adjust_factor 接口获取复权因子.
  AkShare 同时通过 stock_zh_a_daily(adjust="hfq-factor") 获取复权因子.
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
    """各数据源的复权配置 - 统一使用非复权原始数据"""

    # BaoStock配置
    BAOSTOCK = {
        'adjustflag': AdjustmentType.NON_ADJUSTED.value,  # "3" 不复权
        'description': '非复权原始数据（由本地引擎动态复权）',
        'type': 'none'
    }

    # AkShare配置
    AKSHARE = {
        'adjust': '',  # 空字符串 = 不复权
        'description': '非复权原始数据（由本地引擎动态复权）',
        'type': 'none'
    }

    # yFinance配置
    YFINANCE = {
        'auto_adjust': False,  # 不自动复权, 保留原始 OHLC + Adj Close
        'description': '非复权原始数据（由本地引擎动态复权）',
        'type': 'none'
    }

    # Tushare配置
    TUSHARE = {
        'description': '非复权原始数据（由本地引擎动态复权）',
        'type': 'none'
    }

    # pytdx配置
    PYTDX = {
        'description': '非复权原始数据（由本地引擎动态复权, vol 单位已转换为股）',
        'type': 'none'
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
            'tushare': DataSourceAdjustment.TUSHARE,
            'pytdx': DataSourceAdjustment.PYTDX,
        }
        return configs.get(data_source.lower(), {})

    @staticmethod
    def get_default_factor() -> float:
        """获取默认复权因子（非复权数据, 因子=1.0）"""
        return 1.0

    @staticmethod
    def is_front_adjusted(data_source: str) -> bool:
        """检查数据源是否使用前复权

        改造后所有数据源统一使用非复权, 此方法始终返回 False.
        """
        return False


# 全局复权配置常量
ADJUSTMENT_TYPE_FRONT = "前复权"
ADJUSTMENT_TYPE_BACK = "后复权"
ADJUSTMENT_TYPE_NONE = "不复权"
DEFAULT_ADJUSTMENT_TYPE = ADJUSTMENT_TYPE_NONE  # 改为不复权