"""
data source factory for the quote system.
Manages different data sources and provides unified interface with support for comprehensive features.
"""

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, date

from utils import ds_logger, config_manager
from utils.logging_manager import LogContext
from database.operations import DatabaseOperations
from database.models import Instrument, DailyQuote, TradingCalendar

from .base_source import BaseDataSource, RateLimitConfig
from .yfinance_source import YFinanceSource
from .akshare_source import AkShareSource
from .tushare_source import TushareSource
from .baostock_source import BaostockSource
from utils.exchange_utils import exchange_mapper


class DataSourceFactory:
    """data source factory class with comprehensive features"""

    def __init__(self, db_operations: DatabaseOperations):
        self.config = config_manager
        self.db_ops = db_operations
        self.sources: Dict[str, BaseDataSource] = {}

        # 使用工具函数的映射器
        self.exchange_mapper = exchange_mapper

        # 预构建地区到数据源实例的映射关系（用于快速查找）
        self.region_to_sources: Dict[str, Dict[str, List[BaseDataSource]]] = {
            'primary': {},
            'backup': {}
        }

        # 交易日历缓存
        self.trading_calendar_cache: Dict[str, Dict[date, bool]] = {}

    async def initialize(self):
        """初始化所有数据源"""
        try:
            ds_logger.info("[DataSourceFactory] Initializing data sources...")

            # 获取配置
            data_config = self.config.get('data_sources', {})
            sources_config = self.config.get('data_sources_config', {})

            # 遍历所有数据源配置
            for source_name, source_config in sources_config.items():
                if not source_config.get('enabled', False):
                    ds_logger.info(f"[DataSourceFactory] Skipping disabled source: {source_name}")
                    continue

                # 获取数据源限流配置
                rate_limit_config = self._get_rate_limit_config_for_source(source_name)

                # 获取数据源支持的地区和主力地区
                supported_regions = source_config.get('exchanges_supported', [])
                primary_regions = source_config.get('primary_source_of', [])

                # 为每个启用的地区创建数据源实例
                for region in supported_regions:
                    if region is None:
                        continue

                    # 检查该地区是否启用
                    if not data_config.get(region, {}).get('enabled', False):
                        ds_logger.info(f"[DataSourceFactory] Skipping {region} for {source_name} (region disabled)")
                        continue

                    # 为数据源命名（直接使用地区名称）
                    source_instance_name = f"{source_name}_{region}"

                    # 创建数据源实例
                    try:
                        if source_name == 'yfinance':
                            source_instance = YFinanceSource(source_instance_name, rate_limit_config)
                        elif source_name == 'akshare':
                            source_instance = AkShareSource(source_instance_name, rate_limit_config)
                        elif source_name == 'tushare':
                            source_instance = TushareSource(source_instance_name, rate_limit_config)
                        elif source_name == 'baostock':
                            source_instance = BaostockSource(source_instance_name, rate_limit_config)
                        else:
                            ds_logger.warning(f"[DataSourceFactory] Unknown source type: {source_name}")
                            continue

                        await source_instance.initialize()
                        self.sources[source_instance_name] = source_instance

                        # 构建地区到数据源的映射关系
                        if region in primary_regions:
                            source_type = 'primary'
                            ds_logger.info(f"[DataSourceFactory] Initialized {source_instance_name} as PRIMARY source for {region}")
                        else:
                            source_type = 'backup'
                            ds_logger.info(f"[DataSourceFactory] Initialized {source_instance_name} as BACKUP source for {region}")

                        if region not in self.region_to_sources[source_type]:
                            self.region_to_sources[source_type][region] = []
                        self.region_to_sources[source_type][region].append(source_instance)

                    except Exception as e:
                        ds_logger.error(f"[DataSourceFactory] Failed to initialize {source_instance_name}: {e}")
                        continue

            ds_logger.info(f"[DataSourceFactory] Initialized {len(self.sources)} data sources")

        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to initialize data sources: {e}")
            raise

    def _get_rate_limit_config_for_source(self, source_name: str) -> RateLimitConfig:
        """获取数据源特定的限流配置"""
        sources_config = self.config.get('data_sources_config', {})
        source_config = sources_config.get(source_name, {})

        return RateLimitConfig(
            max_requests_per_minute=source_config.get('max_requests_per_minute', 30),
            max_requests_per_hour=source_config.get('max_requests_per_hour', 500),
            max_requests_per_day=source_config.get('max_requests_per_day', 5000),
            retry_times=source_config.get('retry_times', 3),
            retry_interval=source_config.get('retry_interval', 2.0)
        )

    def get_available_sources(self) -> List[str]:
        """获取所有可用的数据源名称"""
        return list(self.sources.keys())

    def get_source(self, exchange: str) -> BaseDataSource:
        """根据交易所获取数据源"""
        exchange = exchange.upper()
        region = self.exchange_mapper.get_region_from_exchange(exchange)
        if not region:
            raise ValueError(f"Unsupported exchange: {exchange}")

        # 优先返回主数据源，然后是备用数据源
        primary_source = self.get_primary_source(exchange)
        if primary_source:
            return primary_source

        backup_source = self.get_backup_source(exchange)
        if backup_source:
            return backup_source

        raise ValueError(f"No data source available for exchange: {exchange}")

    def get_primary_source(self, exchange: str) -> Optional[BaseDataSource]:
        """获取主要数据源"""
        exchange = exchange.upper()
        region = self.exchange_mapper.get_region_from_exchange(exchange)
        if not region:
            return None

        # 使用预构建的映射关系快速查找
        primary_sources = self.region_to_sources['primary'].get(region, [])
        return primary_sources[0] if primary_sources else None

    def get_backup_source(self, exchange: str) -> Optional[BaseDataSource]:
        """获取备用数据源"""
        exchange = exchange.upper()
        region = self.exchange_mapper.get_region_from_exchange(exchange)
        if not region:
            return None

        # 使用预构建的映射关系快速查找
        backup_sources = self.region_to_sources['backup'].get(region, [])
        return backup_sources[0] if backup_sources else None

    async def get_instrument_list(self, exchange: str, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """获取交易品种列表 - 智能降级策略"""
        exchange = exchange.upper()

        # 第一步：检查是否需要强制刷新
        if not force_refresh:
            try:
                with LogContext("DataSourceFactory", "get_cached_instruments", exchange=exchange):
                    cached_instruments = await self._get_cached_instruments(exchange)
                    # 缓存检查：如果记录数过少（少于100条），强制刷新
                    if cached_instruments and len(cached_instruments) >= 100:
                        ds_logger.info(f"[DataSourceFactory] Using cached instruments for {exchange}: {len(cached_instruments)} items")
                        return cached_instruments
                    elif cached_instruments:
                        ds_logger.warning(f"[DataSourceFactory] Cache has insufficient instruments for {exchange}: {len(cached_instruments)} items, forcing refresh")
            except Exception as e:
                ds_logger.warning(f"[DataSourceFactory] Failed to get cached instruments: {e}")
        else:
            ds_logger.info(f"[DataSourceFactory] Force refresh requested for {exchange}")

        # 第二步：尝试从主数据源获取
        primary_source = self.get_primary_source(exchange)
        if not primary_source:
            ds_logger.error(f"[DataSourceFactory] No data source configured for exchange: {exchange}")
            return []

        try:
            with LogContext("DataSourceFactory", "get_primary_instruments", exchange=exchange):
                instruments = await primary_source.get_instrument_list(exchange)
                if instruments and len(instruments) > 50:  # 认为是完整的列表
                    # 验证数据质量
                    if self._validate_instrument_list(instruments, exchange):
                        # 保存到缓存
                        await self._save_instruments_cache(exchange, instruments)
                        ds_logger.info(f"[DataSourceFactory] Got fresh instruments from {primary_source.name}: {len(instruments)} items")
                        return instruments
                    else:
                        ds_logger.warning(f"[DataSourceFactory] Invalid instrument list from {primary_source.name}")
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to get instrument list from {primary_source.name}: {e}")

        # 第三步：尝试从备用数据源获取（但只用于历史数据，不用于instruments列表）
        backup_source = self.get_backup_source(exchange)
        if backup_source:
            ds_logger.warning(f"[DataSourceFactory] Primary source failed for {exchange}, checking backup...")
            # 注意：yfinance等备用数据源不提供完整instruments列表，所以这里不获取

        # 第四步：如果所有方法都失败，返回空列表
        ds_logger.error(f"[DataSourceFactory] No valid instrument list available for {exchange}")
        return []

    async def _get_cached_instruments(self, exchange: str) -> List[Dict[str, Any]]:
        """从缓存获取交易品种列表"""
        try:
            instruments = await self.db_ops.get_instruments_by_exchange(exchange)
            # 检查缓存是否过期（超过24小时）
            if instruments:
                from datetime import datetime, date, timedelta
                from utils.date_utils import get_shanghai_time

                latest_update = max(inst.get('updated_at', datetime.min) for inst in instruments)
                if isinstance(latest_update, str):
                    latest_update = datetime.fromisoformat(latest_update.replace('Z', '+00:00'))

                cache_age = get_shanghai_time() - latest_update
                if cache_age > timedelta(hours=24):
                    ds_logger.warning(f"[DataSourceFactory] Cache expired for {exchange} (age: {cache_age}), will refresh")
                    return []  # 返回空列表触发刷新

            return instruments
        except Exception as e:
            ds_logger.warning(f"[DataSourceFactory] Failed to get cached instruments: {e}")
            return []

    async def _save_instruments_cache(self, exchange: str, instruments: List[Dict[str, Any]]):
        """保存交易品种列表到缓存"""
        try:
            await self.db_ops.save_instrument_list(instruments)
            ds_logger.debug(f"[DataSourceFactory] Saved {len(instruments)} instruments to cache")
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to save instruments cache: {e}")

    def _validate_instrument_list(self, instruments: List[Dict[str, Any]], exchange: str) -> bool:
        """验证交易品种列表的质量"""
        if not instruments:
            return False

        # 检查必需字段
        required_fields = ['instrument_id', 'symbol', 'name', 'exchange', 'type']
        for instrument in instruments[:10]:  # 只检查前10个，提高效率
            for field in required_fields:
                if field not in instrument:
                    ds_logger.warning(f"[DataSourceFactory] Instrument missing required field '{field}': {instrument}")
                    return False

        # 检查数据一致性
        for instrument in instruments[:10]:
            if instrument.get('exchange') != exchange:
                ds_logger.warning(f"[DataSourceFactory] Instrument exchange mismatch: expected {exchange}, got {instrument.get('exchange')}")
                return False

        ds_logger.debug(f"[DataSourceFactory] Instrument list validation passed for {exchange}")
        return True

    def _validate_daily_data(self, data: List[Dict[str, Any]], instrument_id: str, symbol: str) -> bool:
        """验证日线数据的质量"""
        if not data:
            return False

        # 检查必需字段
        required_fields = ['time', 'instrument_id', 'open', 'high', 'low', 'close']
        for quote in data[:5]:  # 只检查前5条数据，提高效率
            for field in required_fields:
                if field not in quote:
                    ds_logger.warning(f"[DataSourceFactory] Quote missing required field '{field}': {quote}")
                    return False

        # 检查数据一致性
        for quote in data[:5]:
            if quote.get('instrument_id') != instrument_id:
                ds_logger.warning(f"[DataSourceFactory] Quote instrument_id mismatch: expected {instrument_id}, got {quote.get('instrument_id')}")
                return False

            # 检查价格合理性
            try:
                open_price = float(quote.get('open', 0))
                close_price = float(quote.get('close', 0))
                high_price = float(quote.get('high', 0))
                low_price = float(quote.get('low', 0))

                if open_price <= 0 or close_price <= 0 or high_price <= 0 or low_price <= 0:
                    ds_logger.warning(f"[DataSourceFactory] Invalid price data: open={open_price}, close={close_price}, high={high_price}, low={low_price}")
                    return False

                if high_price < low_price:
                    ds_logger.warning(f"[DataSourceFactory] Invalid price relationship: high < low for {symbol}")
                    return False

                if close_price > high_price or close_price < low_price:
                    # 这是正常的，股价可能跳空
                    pass

            except (ValueError, TypeError):
                ds_logger.warning(f"[DataSourceFactory] Invalid price data type for {symbol}")
                return False

        ds_logger.debug(f"[DataSourceFactory] Daily data validation passed for {symbol}")
        return True

    async def get_daily_data(self, exchange: str, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """获取日线数据 - 智能降级策略"""
        exchange = exchange.upper()

        # 第一步：尝试从主数据源获取
        primary_source = self.get_primary_source(exchange)
        if not primary_source:
            ds_logger.error(f"[DataSourceFactory] No data source configured for exchange: {exchange}")
            return []

        try:
            with LogContext("DataSourceFactory", "get_primary_data",
                           exchange=exchange, source=primary_source.name,
                           instrument_id=instrument_id, symbol=symbol):
                data = await primary_source.get_daily_data(instrument_id, symbol, start_date, end_date)
                if data and self._validate_daily_data(data, instrument_id, symbol):
                    ds_logger.debug(f"[DataSourceFactory] Got data from {primary_source.name}: {len(data)} quotes")
                    return data
                else:
                    ds_logger.warning(f"[DataSourceFactory] Invalid data from {primary_source.name}")
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to get daily data from {primary_source.name}: {e}")

        # 第二步：尝试从备用数据源获取（yfinance等）
        backup_source = self.get_backup_source(exchange)
        if backup_source:
            ds_logger.info(f"[DataSourceFactory] Trying backup source: {backup_source.name} for {symbol}")
            try:
                with LogContext("DataSourceFactory", "get_backup_data",
                               exchange=exchange, source=backup_source.name,
                               instrument_id=instrument_id, symbol=symbol):
                    data = await backup_source.get_daily_data(instrument_id, symbol, start_date, end_date)
                    if data and self._validate_daily_data(data, instrument_id, symbol):
                        ds_logger.info(f"[DataSourceFactory] Got data from backup {backup_source.name}: {len(data)} quotes")
                        return data
                    else:
                        ds_logger.warning(f"[DataSourceFactory] Invalid data from backup {backup_source.name}")
            except Exception as backup_e:
                ds_logger.error(f"[DataSourceFactory] Backup source also failed: {backup_e}")

        ds_logger.warning(f"[DataSourceFactory] No data available for {exchange} {symbol}")
        return []

    async def get_latest_daily_data(self, exchange: str, instrument_id: str, symbol: str) -> Dict[str, Any]:
        """获取最新日线数据"""
        source = self.get_primary_source(exchange)
        if not source:
            ds_logger.error(f"[DataSourceFactory] No data source for exchange: {exchange}")
            return {}

        try:
            return await source.get_latest_daily_data(instrument_id, symbol)
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to get latest data from {source.name}: {e}")

            # 尝试备用数据源
            backup_source = self.get_backup_source(exchange)
            if backup_source:
                ds_logger.info(f"[DataSourceFactory] Trying backup source: {backup_source.name}")
                try:
                    return await backup_source.get_latest_daily_data(instrument_id, symbol)
                except Exception as backup_e:
                    ds_logger.error(f"[DataSourceFactory] Backup source also failed: {backup_e}")

            return {}

    async def get_daily_data_batch(self, exchange: str, instruments: List[Dict[str, Any]],
                                 start_date: datetime, end_date: datetime,
                                 batch_size: int = 50) -> List[Dict[str, Any]]:
        """批量获取日线数据"""
        with LogContext("DataSourceFactory", "get_daily_data_batch",
                       exchange=exchange,
                       extra_context={
                           "instrument_count": len(instruments),
                           "date_range": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                           "batch_size": batch_size
                       }):

            source = self.get_primary_source(exchange)
            if not source:
                ds_logger.error(f"No data source for exchange: {exchange}")
                return []

            try:
                ds_logger.info(f"Fetching batch data from {source.name} for {len(instruments)} instruments")
                data = await source.get_daily_data_batch(instruments, start_date, end_date, batch_size)

                if data:
                    ds_logger.info(f"Successfully retrieved {len(data)} quotes from {source.name}")
                else:
                    ds_logger.warning(f"No data returned from {source.name}")

                return data

            except Exception as e:
                ds_logger.error(f"Failed to get daily data batch from {source.name}: {e}")

                # 尝试备用数据源
                backup_source = self.get_backup_source(exchange)
                if backup_source:
                    ds_logger.info(f"Trying backup source: {backup_source.name}")
                    try:
                        with LogContext("DataSourceFactory", "backup_source_retry", exchange=exchange):
                            backup_data = await backup_source.get_daily_data_batch(instruments, start_date, end_date, batch_size)
                            if backup_data:
                                ds_logger.info(f"Backup source {backup_source.name} succeeded: {len(backup_data)} quotes")
                            else:
                                ds_logger.warning(f"Backup source {backup_source.name} returned no data")
                            return backup_data
                    except Exception as backup_e:
                        ds_logger.error(f"Backup source {backup_source.name} also failed: {backup_e}")

                return []

    async def get_latest_data_batch(self, exchange: str, instruments: List[Dict[str, Any]],
                                 batch_size: int = 50) -> List[Dict[str, Any]]:
        """批量获取最新数据"""
        source = self.get_primary_source(exchange)
        if not source:
            ds_logger.error(f"[DataSourceFactory] No data source for exchange: {exchange}")
            return []

        try:
            return await source.get_latest_data_batch(instruments, batch_size)
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to get latest data batch from {source.name}: {e}")
            return []

    # 交易日历相关方法
    async def is_trading_day(self, exchange: str, check_date: date) -> bool:
        """检查指定日期是否为交易日"""
        if exchange not in self.trading_calendar_cache:
            self.trading_calendar_cache[exchange] = {}

        # 检查缓存
        if check_date in self.trading_calendar_cache[exchange]:
            return self.trading_calendar_cache[exchange][check_date]

        # 从数据库查询
        is_trading = await self.db_ops.is_trading_day(exchange, check_date)
        self.trading_calendar_cache[exchange][check_date] = is_trading

        return is_trading

    async def get_trading_days(self, exchange: str, start_date: date, end_date: date) -> List[date]:
        """获取指定日期范围内的交易日列表"""
        trading_days = await self.db_ops.get_trading_days(exchange, start_date, end_date, is_trading_day=True)

        # 更新缓存
        if exchange not in self.trading_calendar_cache:
            self.trading_calendar_cache[exchange] = {}

        for trading_day in trading_days:
            self.trading_calendar_cache[exchange][trading_day] = True

        return trading_days

    async def get_next_trading_day(self, exchange: str, check_date: date) -> Optional[date]:
        """获取下一个交易日"""
        return await self.db_ops.get_next_trading_day(exchange, check_date)

    async def get_previous_trading_day(self, exchange: str, check_date: date) -> Optional[date]:
        """获取上一个交易日"""
        return await self.db_ops.get_previous_trading_day(exchange, check_date)

    async def update_trading_calendar(self, exchange: str, start_date: date, end_date: date) -> int:
        """更新交易日历"""
        source = self.get_primary_source(exchange)
        if not source or not hasattr(source, 'get_trading_calendar'):
            ds_logger.warning(f"[DataSourceFactory] Source for {exchange} doesn't support trading calendar")
            return 0

        try:
            calendar_data = await source.get_trading_calendar(exchange, start_date, end_date)
            if calendar_data:
                return await self.db_ops.save_trading_calendar(calendar_data)
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to update trading calendar: {e}")

        return 0

    # 数据质量评估方法
    async def assess_data_quality(self, exchange: str, instrument_id: str,
                                 start_date: date, end_date: date) -> Dict[str, Any]:
        """评估数据质量"""
        return await self.db_ops.assess_data_quality(instrument_id, start_date, end_date)

    async def health_check_all(self) -> Dict[str, bool]:
        """检查所有数据源健康状态"""
        health_status = {}

        for name, source in self.sources.items():
            try:
                is_healthy = await source.health_check()
                health_status[name] = is_healthy
                ds_logger.info(f"[DataSourceFactory] Health check {name}: {'OK' if is_healthy else 'FAILED'}")
            except Exception as e:
                health_status[name] = False
                ds_logger.error(f"[DataSourceFactory] Health check failed for {name}: {e}")

        return health_status

    async def close_all(self):
        """关闭所有数据源连接"""
        ds_logger.info("[DataSourceFactory] Closing all data sources...")
        for name, source in self.sources.items():
            try:
                await source.close()
                ds_logger.info(f"[DataSourceFactory] Closed {name}")
            except Exception as e:
                ds_logger.error(f"[DataSourceFactory] Error closing {name}: {e}")

        self.sources.clear()
        self.trading_calendar_cache.clear()


# 全局数据源工厂实例
data_source_factory = None

async def get_data_source_factory(db_operations: DatabaseOperations) -> "DataSourceFactory":
    """获取数据源工厂实例（单例模式）"""
    global data_source_factory
    if data_source_factory is None:
        data_source_factory = DataSourceFactory(db_operations)
        await data_source_factory.initialize()
    return data_source_factory