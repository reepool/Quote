"""
交易所映射工具函数
提供统一的交易所代码、地区映射和转换功能
"""

from typing import Dict, List, Optional
from utils.exceptions import ExchangeMappingError, ErrorCodes
from utils import log_execution
from .config_manager import config_manager


class ExchangeMapper:
    """交易所映射器"""

    def __init__(self):
        self._exchange_mapping = None
        self._region_to_exchanges = None
        self._load_mappings()

    @log_execution("ExchangeMapper", "load_mappings")
    def _load_mappings(self):
        """从配置加载映射关系"""
        try:
            exchange_rules = config_manager.get_exchange_rules()
            self._exchange_mapping = exchange_rules.exchange_mapping

            # 构建地区到交易所的反向映射
            self._region_to_exchanges = {}
            for exchange, region in self._exchange_mapping.items():
                if region not in self._region_to_exchanges:
                    self._region_to_exchanges[region] = []
                self._region_to_exchanges[region].append(exchange)

            return len(self._exchange_mapping)

        except Exception as e:
            raise ExchangeMappingError(
                f"Failed to load exchange mappings: {str(e)}",
                ErrorCodes.CONFIG_INVALID_FORMAT
            ) from e

    def get_region_from_exchange(self, exchange: str) -> Optional[str]:
        """根据交易所代码获取地区"""
        if not exchange:
            return None
        return self._exchange_mapping.get(exchange.upper())

    def get_exchanges_from_region(self, region: str) -> List[str]:
        """根据地区获取交易所代码列表"""
        if not region:
            return []
        return self._region_to_exchanges.get(region, [])

    def is_exchange_supported(self, exchange: str, supported_regions: List[str]) -> bool:
        """检查交易所是否在支持的地区列表中"""
        if not exchange or not supported_regions:
            return False

        region = self.get_region_from_exchange(exchange)
        if not region:
            return False

        # 支持直接使用交易所代码或地区名称
        return exchange in supported_regions or region in supported_regions

    def normalize_supported_exchanges(self, supported_list: List[str]) -> List[str]:
        """
        标准化支持的交易所列表
        将地区名称转换为实际的交易所代码列表
        """
        if not supported_list:
            return []

        normalized = set()

        for item in supported_list:
            if item is None:
                # None 表示支持所有交易所
                return list(self._exchange_mapping.keys())

            item_str = str(item).upper()

            # 如果是已知的交易所代码，直接添加
            if item_str in self._exchange_mapping:
                normalized.add(item_str)
            # 如果是地区名称，转换为该地区的所有交易所
            elif item_str in self._region_to_exchanges:
                normalized.update(self._region_to_exchanges[item_str])
                # 使用独立日志器避免循环依赖
                import logging
                mapper_logger = logging.getLogger("ExchangeMapper")
                mapper_logger.debug(f"[ExchangeMapper] Region {item_str} mapped to exchanges: {self._region_to_exchanges[item_str]}")
            else:
                import logging
                mapper_logger = logging.getLogger("ExchangeMapper")
                mapper_logger.warning(f"[ExchangeMapper] Unknown exchange or region: {item}")

        return list(normalized)

    def get_exchange_by_symbol(self, symbol: str) -> Optional[str]:
        """根据股票代码推断交易所"""
        if not symbol:
            return None

        try:
            import logging
            mapper_logger = logging.getLogger("ExchangeMapper")

            symbol_rules = config_manager.get_nested("exchange_rules.symbol_rules")
            start_rules = config_manager.get_nested("exchange_rules.symbol_start_with")

            for exchange, prefixes in start_rules.items():
                allowed_len = symbol_rules.get(exchange)

                # 检查长度规则
                if allowed_len is not None:
                    if isinstance(allowed_len, int):
                        length_ok = len(symbol) == allowed_len
                    elif isinstance(allowed_len, list):
                        length_ok = len(symbol) in allowed_len
                    else:
                        length_ok = False
                else:
                    length_ok = True

                # 检查前缀规则
                if length_ok and symbol.startswith(tuple(prefixes)):
                    return exchange

            mapper_logger.warning(f"[ExchangeMapper] Cannot determine exchange for symbol: {symbol}")
            return None

        except Exception as e:
            mapper_logger.error(f"[ExchangeMapper] Error determining exchange for {symbol}: {e}")
            return None

    def filter_exchanges_by_config(self, exchanges: List[str], data_config: Dict) -> List[str]:
        """根据数据配置过滤启用的交易所"""
        if not exchanges or not data_config:
            return exchanges

        enabled_exchanges = []
        for exchange in exchanges:
            import logging
            mapper_logger = logging.getLogger("ExchangeMapper")

            region = self.get_region_from_exchange(exchange)
            if region and data_config.get(region, {}).get('enabled', False):
                enabled_exchanges.append(exchange)
            else:
                mapper_logger.debug(f"[ExchangeMapper] Exchange {exchange} (region: {region}) is disabled")

        return enabled_exchanges

    def validate_exchange_config(self, exchange: str, supported_regions: List[str],
                               data_config: Dict) -> bool:
        """验证交易所配置是否完整和正确"""
        import logging
        mapper_logger = logging.getLogger("ExchangeMapper")

        if not exchange:
            return False

        # 检查交易所是否在支持列表中
        if not self.is_exchange_supported(exchange, supported_regions):
            mapper_logger.warning(f"[ExchangeMapper] Exchange {exchange} not in supported regions: {supported_regions}")
            return False

        # 检查对应地区是否启用
        region = self.get_region_from_exchange(exchange)
        if not region or not data_config.get(region, {}).get('enabled', False):
            mapper_logger.warning(f"[ExchangeMapper] Region {region} for exchange {exchange} is not enabled")
            return False

        return True


# 全局映射器实例
exchange_mapper = ExchangeMapper()


def get_region_from_exchange(exchange: str) -> Optional[str]:
    """便捷函数：根据交易所代码获取地区"""
    return exchange_mapper.get_region_from_exchange(exchange)


def get_exchanges_from_region(region: str) -> List[str]:
    """便捷函数：根据地区获取交易所代码列表"""
    return exchange_mapper.get_exchanges_from_region(region)


def normalize_supported_exchanges(supported_list: List[str]) -> List[str]:
    """便捷函数：标准化支持的交易所列表"""
    return exchange_mapper.normalize_supported_exchanges(supported_list)


def is_exchange_supported(exchange: str, supported_regions: List[str]) -> bool:
    """便捷函数：检查交易所是否支持"""
    return exchange_mapper.is_exchange_supported(exchange, supported_regions)


def get_exchange_by_symbol(symbol: str) -> Optional[str]:
    """便捷函数：根据股票代码推断交易所"""
    return exchange_mapper.get_exchange_by_symbol(symbol)