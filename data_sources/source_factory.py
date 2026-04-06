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
from .tdx_source import TdxSource
from utils.exchange_utils import exchange_mapper


class DataSourceFactory:
    """data source factory class with comprehensive features"""

    def __init__(self, db_operations: DatabaseOperations):
        self.config = config_manager
        self.db_ops = db_operations
        self.sources: Dict[str, BaseDataSource] = {}

        # 使用工具函数的映射器
        self.exchange_mapper = exchange_mapper

        # 日线数据主/备路由 (按 region)
        self.region_to_sources: Dict[str, Dict[str, List[BaseDataSource]]] = {
            'primary': {},
            'backup': {}
        }

        # 品种列表路由 (按 region) — 独立于日线数据主源
        self.instrument_sources: Dict[str, List[BaseDataSource]] = {}

        # 交易日历路由 (按 region) — 独立于日线数据主源
        self.calendar_sources: Dict[str, List[BaseDataSource]] = {}

        # 复权因子路由 (按 exchange) — 不用 region, 因为 BSE 和 SSE/SZSE 共享 region=a_stock
        self.factor_routes: Dict[str, Dict[str, Any]] = {}

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
                # 跳过非数据源区块的全局配置选项（如 health_check_timeout_sec 等数字或布尔值）
                if not isinstance(source_config, dict):
                    continue
                    
                if not source_config.get('enabled', False):
                    ds_logger.info(f"[DataSourceFactory] Skipping disabled source: {source_name}")
                    continue

                # 获取数据源限流配置
                rate_limit_config = self._get_rate_limit_config_for_source(source_name)

                # 获取数据源支持的地区和各种主力地区
                supported_regions = source_config.get('exchanges_supported', [])
                primary_regions = source_config.get('primary_source_of', [])
                # 品种列表/日历路由: 未配置时 fallback 到 primary_source_of (向后兼容)
                instrument_regions = source_config.get(
                    'instrument_list_source_of',
                    source_config.get('primary_source_of', [])
                )
                calendar_regions = source_config.get(
                    'calendar_source_of',
                    source_config.get('primary_source_of', [])
                )

                # 为每个启用的地区创建数据源实例
                for region in supported_regions:
                    if region is None:
                        continue

                    # 检查该地区是否启用
                    if not data_config.get(region, {}).get('enabled', False):
                        ds_logger.info(f"[DataSourceFactory] Skipping {region} for {source_name} (region disabled)")
                        continue

                    # 为数据源命名
                    source_instance_name = f"{source_name}_{region}"

                    # 创建数据源实例
                    try:
                        source_instance = self._create_source_instance(
                            source_name, source_instance_name, source_config,
                            rate_limit_config
                        )
                        if source_instance is None:
                            continue

                        await source_instance.initialize()
                        self.sources[source_instance_name] = source_instance

                        # 1. 日线数据路由 (主/备)
                        if region in primary_regions:
                            source_type = 'primary'
                            ds_logger.info(f"[DataSourceFactory] Initialized {source_instance_name} as PRIMARY source for {region}")
                        else:
                            source_type = 'backup'
                            ds_logger.info(f"[DataSourceFactory] Initialized {source_instance_name} as BACKUP source for {region}")

                        if region not in self.region_to_sources[source_type]:
                            self.region_to_sources[source_type][region] = []
                        self.region_to_sources[source_type][region].append(source_instance)

                        # 2. 品种列表路由 (独立于日线主源)
                        if region in instrument_regions:
                            if region not in self.instrument_sources:
                                self.instrument_sources[region] = []
                            self.instrument_sources[region].append(source_instance)
                            ds_logger.info(f"[DataSourceFactory] {source_instance_name} → instrument_source for {region}")

                        # 3. 交易日历路由 (独立于日线主源)
                        if region in calendar_regions:
                            if region not in self.calendar_sources:
                                self.calendar_sources[region] = []
                            self.calendar_sources[region].append(source_instance)
                            ds_logger.info(f"[DataSourceFactory] {source_instance_name} → calendar_source for {region}")

                    except Exception as e:
                        ds_logger.error(f"[DataSourceFactory] Failed to initialize {source_instance_name}: {e}")
                        continue

            # 4. 初始化因子路由 (按 exchange, 不按 region)
            self._init_factor_routes()

            ds_logger.info(f"[DataSourceFactory] Initialized {len(self.sources)} data sources")
            ds_logger.info(
                f"[DataSourceFactory] 路由摘要: "
                f"日线主源={list(self.region_to_sources['primary'].keys())}, "
                f"品种源={list(self.instrument_sources.keys())}, "
                f"日历源={list(self.calendar_sources.keys())}, "
                f"因子路由={list(self.factor_routes.keys())}"
            )

        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to initialize data sources: {e}")
            raise

    def _create_source_instance(
        self, source_name: str, instance_name: str,
        source_config: dict, rate_limit_config: RateLimitConfig
    ) -> Optional[BaseDataSource]:
        """根据数据源名称创建实例"""
        if source_name == 'pytdx':
            return TdxSource(
                instance_name, rate_limit_config,
                pool_size=source_config.get('connection_pool_size', 3),
                connection_timeout=source_config.get('connection_timeout_sec', 10),
                ip_refresh_hours=source_config.get('ip_refresh_interval_hours', 24),
                batch_size=source_config.get('batch_size', 800),
            )
        elif source_name == 'baostock':
            return BaostockSource(instance_name, rate_limit_config)
        elif source_name == 'akshare':
            return AkShareSource(instance_name, rate_limit_config)
        elif source_name == 'yfinance':
            return YFinanceSource(instance_name, rate_limit_config)
        elif source_name == 'tushare':
            return TushareSource(instance_name, rate_limit_config)
        else:
            ds_logger.warning(f"[DataSourceFactory] Unknown source type: {source_name}")
            return None

    def _init_factor_routes(self):
        """初始化因子路由 — 按 exchange 而非 region"""
        factor_config = self.config.get('factor_sources', {})
        if not factor_config:
            ds_logger.warning("[DataSourceFactory] No factor_sources config found")
            return

        for exchange, cfg in factor_config.items():
            route: Dict[str, Any] = {
                'primary_instance': None,
                'validator_instance': None,
                'fallback_instance': None,
            }

            # 查找 primary/fallback 实例 (按基名称匹配)
            primary_name = cfg.get('primary')
            fallback_name = cfg.get('fallback')
            validator_name = cfg.get('validator')

            if primary_name:
                route['primary_instance'] = self._find_source_by_base_name(primary_name)
            if fallback_name:
                route['fallback_instance'] = self._find_source_by_base_name(fallback_name)

            # validator: tdx_xdxr 是特殊的 — 从 pytdx source 内部获取 factor_engine
            if validator_name == 'tdx_xdxr':
                pytdx_source = self._find_source_by_base_name('pytdx')
                if pytdx_source and hasattr(pytdx_source, 'factor_engine'):
                    route['validator_instance'] = pytdx_source.factor_engine
                else:
                    ds_logger.warning(
                        f"[DataSourceFactory] tdx_xdxr validator requested for {exchange} "
                        f"but pytdx source not available"
                    )
            elif validator_name:
                route['validator_instance'] = self._find_source_by_base_name(validator_name)

            self.factor_routes[exchange] = route
            ds_logger.info(
                f"[DataSourceFactory] factor_route[{exchange}]: "
                f"primary={primary_name}, validator={validator_name}, fallback={fallback_name}"
            )

    def _find_source_by_base_name(self, base_name: str) -> Optional[BaseDataSource]:
        """按数据源基名称查找已注册的实例 (如 'baostock' 匹配 'baostock_a_stock')"""
        for name, source in self.sources.items():
            if name.startswith(base_name):
                return source
        return None

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
        """获取第一个备用数据源"""
        sources = self.get_backup_sources(exchange)
        return sources[0] if sources else None

    def get_backup_sources(self, exchange: str) -> List[BaseDataSource]:
        """获取所有备用数据源 (用于降级链遍历)"""
        exchange = exchange.upper()
        region = self.exchange_mapper.get_region_from_exchange(exchange)
        if not region:
            return []
        return self.region_to_sources['backup'].get(region, [])

    async def get_instrument_list(self, exchange: str, force_refresh: bool = False,
                                  instrument_types: List[str] = None) -> List[Dict[str, Any]]:
        """获取交易品种列表 - 智能降级策略

        Args:
            exchange: 交易所代码
            force_refresh: 是否强制刷新
            instrument_types: 要获取的品种类型列表, 如 ['stock', 'index', 'etf']
        """
        exchange = exchange.upper()

        # 第一步：检查是否需要强制刷新
        if not force_refresh:
            try:
                with LogContext("DataSourceFactory", "get_cached_instruments", exchange=exchange):
                    cached_instruments = await self._get_cached_instruments(exchange)
                    # 如果指定了 instrument_types，按类型过滤缓存
                    if cached_instruments and instrument_types:
                        cached_instruments = [
                            inst for inst in cached_instruments
                            if inst.get('type') in instrument_types
                        ]
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

        # 第二步：尝试从品种列表专用源获取 (独立于日线主源)
        region = self.exchange_mapper.get_region_from_exchange(exchange)
        instrument_source_list = self.instrument_sources.get(region, []) if region else []
        if not instrument_source_list:
            # fallback: 日线主源
            primary_source = self.get_primary_source(exchange)
            if primary_source:
                instrument_source_list = [primary_source]

        if not instrument_source_list:
            ds_logger.error(f"[DataSourceFactory] No instrument source configured for exchange: {exchange}")
            return []

        primary_source = instrument_source_list[0]
        backup_sources = instrument_source_list[1:]

        try:
            with LogContext("DataSourceFactory", "get_primary_instruments", exchange=exchange):
                # 透传 instrument_types（支持该参数的数据源会按类型过滤）
                try:
                    instruments = await primary_source.get_instrument_list(
                        exchange, instrument_types=instrument_types
                    )
                except TypeError:
                    # 数据源不支持 instrument_types 参数，回退到基本调用
                    instruments = await primary_source.get_instrument_list(exchange)
                    if instrument_types:
                        instruments = [
                            inst for inst in instruments
                            if inst.get('type') in instrument_types
                        ]

                # 第三步：尝试从其他品种源进行并集合并
                for backup_source in backup_sources:
                    try:
                        ds_logger.info(f"[DataSourceFactory] Fetching instruments from backup_source {backup_source.name} for {exchange}...")
                        try:
                            backup_instruments = await backup_source.get_instrument_list(exchange, instrument_types=instrument_types)
                        except TypeError:
                            backup_instruments = await backup_source.get_instrument_list(exchange)
                            if instrument_types:
                                backup_instruments = [inst for inst in backup_instruments if inst.get('type') in instrument_types]
                        
                        if backup_instruments:
                            # 按照 instrument_id 对仪器进行合并，保留 primary 的绝对优先级
                            merged_dict = {inst['instrument_id']: inst for inst in instruments}
                            added_count = 0
                            for b_inst in backup_instruments:
                                if b_inst['instrument_id'] not in merged_dict:
                                    merged_dict[b_inst['instrument_id']] = b_inst
                                    added_count += 1
                            instruments = list(merged_dict.values())
                            ds_logger.info(f"[DataSourceFactory] Successfully merged {added_count} instruments from {backup_source.name}. Total now: {len(instruments)}")
                    except Exception as e:
                        ds_logger.warning(f"[DataSourceFactory] Failed to get instruments from backup source {backup_source.name}: {e}")

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
                if open_price < 0 or close_price < 0 or high_price < 0 or low_price < 0:
                    ds_logger.warning(f"[DataSourceFactory] Negative price data: open={open_price}, close={close_price}, high={high_price}, low={low_price}")
                    return False
                    
                if open_price == 0 or close_price == 0:
                    ds_logger.warning(f"[DataSourceFactory] Zero price data observed: open={open_price}, close={close_price}, high={high_price}, low={low_price}. Will be filtered by DataManager.")

                if high_price < low_price:
                    ds_logger.warning(f"[DataSourceFactory] Invalid price relationship: high < low for {symbol}. Will be filtered by DataManager.")

                if close_price > high_price or close_price < low_price:
                    # 这是正常的，股价可能跳空 / 不调整
                    pass

            except (ValueError, TypeError):
                ds_logger.warning(f"[DataSourceFactory] Invalid price data type for {symbol}")
                return False

        ds_logger.debug(f"[DataSourceFactory] Daily data validation passed for {symbol}")
        return True

    async def get_daily_data(self, exchange: str, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime,
                           instrument_type: str = 'stock',
                           source_symbol: str = '') -> List[Dict[str, Any]]:
        """获取日线数据 - 智能降级策略

        Args:
            exchange: 交易所代码
            instrument_id: 品种 ID
            symbol: 交易代码
            start_date: 开始日期
            end_date: 结束日期
            instrument_type: 品种类型
            source_symbol: 数据源原始代码（如东财 105.AAPL），可选
        """
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
                data = await primary_source.get_daily_data(
                    instrument_id, symbol, start_date, end_date,
                    instrument_type, source_symbol=source_symbol
                )
                if data and self._validate_daily_data(data, instrument_id, symbol):
                    ds_logger.debug(f"[DataSourceFactory] Got data from {primary_source.name}: {len(data)} quotes")
                    return data
                else:
                    ds_logger.warning(f"[DataSourceFactory] Invalid data from {primary_source.name}")
        except Exception as e:
            ds_logger.error(f"[DataSourceFactory] Failed to get daily data from {primary_source.name}: {e}")

        # 第二步：遍历所有备用数据源（降级链）
        for backup_source in self.get_backup_sources(exchange):
            ds_logger.info(f"[DataSourceFactory] Trying backup source: {backup_source.name} for {symbol} (instrument_type={instrument_type})")
            try:
                with LogContext("DataSourceFactory", "get_backup_data",
                               exchange=exchange, source=backup_source.name,
                               instrument_id=instrument_id, symbol=symbol):
                    data = await backup_source.get_daily_data(
                        instrument_id, symbol, start_date, end_date,
                        instrument_type, source_symbol=source_symbol
                    )
                    if data and self._validate_daily_data(data, instrument_id, symbol):
                        ds_logger.info(f"[DataSourceFactory] Got data from backup {backup_source.name}: {len(data)} quotes")
                        return data
                    else:
                        ds_logger.warning(f"[DataSourceFactory] Invalid data from backup {backup_source.name}")
            except Exception as backup_e:
                ds_logger.error(f"[DataSourceFactory] Backup source {backup_source.name} failed: {backup_e}")

        ds_logger.warning(f"[DataSourceFactory] No data available for {exchange} {symbol}")
        return []

    async def get_adjustment_factors(self, exchange: str, instrument_id: str,
                                     symbol: str, start_date: datetime,
                                     end_date: datetime) -> List[Dict[str, Any]]:
        """获取复权因子 - 使用因子独立路由 (按 exchange 而非 region)"""
        exchange = exchange.upper()

        # ★ 直接用 exchange 查因子路由, 不经过 region 映射
        factor_cfg = self.factor_routes.get(exchange, {})

        # 尝试因子主源
        primary = factor_cfg.get('primary_instance')
        if primary and hasattr(primary, 'get_adjustment_factors'):
            try:
                factors = await primary.get_adjustment_factors(
                    instrument_id, symbol, start_date, end_date
                )
                if factors:
                    ds_logger.debug(
                        f"[DataSourceFactory] Got {len(factors)} factors from "
                        f"{getattr(primary, 'name', type(primary).__name__)} for {exchange}"
                    )
                    return factors
            except Exception as e:
                ds_logger.warning(
                    f"[DataSourceFactory] Factor primary failed for {exchange}: {e}"
                )

        # 尝试因子备用源
        fallback = factor_cfg.get('fallback_instance')
        if fallback and hasattr(fallback, 'get_adjustment_factors'):
            try:
                factors = await fallback.get_adjustment_factors(
                    instrument_id, symbol, start_date, end_date
                )
                if factors:
                    ds_logger.info(
                        f"[DataSourceFactory] Got {len(factors)} factors from fallback "
                        f"{getattr(fallback, 'name', type(fallback).__name__)} for {exchange}"
                    )
                    return factors
            except Exception as e:
                ds_logger.warning(
                    f"[DataSourceFactory] Factor fallback also failed for {exchange}: {e}"
                )

        # 最后兜底: 遍历日线备用源（跳过已尝试的因子路由源和不支持该交易所的源）
        tried_sources = {id(s) for s in [primary, fallback] if s is not None}
        for backup_source in self.get_backup_sources(exchange):
            if id(backup_source) in tried_sources:
                continue
            if not hasattr(backup_source, 'get_adjustment_factors'):
                continue
            supported = getattr(backup_source, 'supported_exchanges', None)
            if supported and exchange not in supported:
                continue
            try:
                factors = await backup_source.get_adjustment_factors(
                    instrument_id, symbol, start_date, end_date
                )
                if factors:
                    ds_logger.info(
                        f"[DataSourceFactory] Got {len(factors)} factors from "
                        f"daily-backup {backup_source.name}"
                    )
                    return factors
            except Exception as e:
                ds_logger.warning(
                    f"[DataSourceFactory] Daily-backup factor fetch failed: {e}"
                )

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
        """更新交易日历 - 使用日历专用路由 (独立于日线主源)"""
        region = self.exchange_mapper.get_region_from_exchange(exchange)
        calendar_source_list = self.calendar_sources.get(region, []) if region else []
        if not calendar_source_list:
            # fallback: 日线主源
            primary_source = self.get_primary_source(exchange)
            if primary_source and hasattr(primary_source, 'get_trading_calendar'):
                calendar_source_list = [primary_source]

        for source in calendar_source_list:
            if not hasattr(source, 'get_trading_calendar'):
                continue
            try:
                calendar_data = await source.get_trading_calendar(exchange, start_date, end_date)
                if calendar_data:
                    saved = await self.db_ops.save_trading_calendar(calendar_data)
                    ds_logger.info(
                        f"[DataSourceFactory] Calendar updated from {source.name}: {saved} records"
                    )
                    return saved
            except Exception as e:
                ds_logger.warning(f"[DataSourceFactory] Calendar source {source.name} failed: {e}")

        ds_logger.warning(f"[DataSourceFactory] No calendar source available for {exchange}")
        return 0

    # 数据质量评估方法
    async def assess_data_quality(self, exchange: str, instrument_id: str,
                                 start_date: date, end_date: date) -> Dict[str, Any]:
        """评估数据质量"""
        return await self.db_ops.assess_data_quality(instrument_id, start_date, end_date)

    async def health_check_all(self) -> Dict[str, bool]:
        """检查所有数据源健康状态"""
        health_status = {}
        timeout_seconds = self.config.get_nested(
            'data_sources_config.health_check_timeout_sec', 10
        )

        async def check_source(name: str, source: BaseDataSource) -> tuple[str, bool]:
            try:
                is_healthy = await asyncio.wait_for(
                    source.health_check(), timeout=timeout_seconds
                )
                ds_logger.info(f"[DataSourceFactory] Health check {name}: {'OK' if is_healthy else 'FAILED'}")
                return name, is_healthy
            except asyncio.TimeoutError:
                ds_logger.error(
                    f"[DataSourceFactory] Health check timed out for {name} "
                    f"after {timeout_seconds}s"
                )
                return name, False
            except Exception as e:
                ds_logger.error(f"[DataSourceFactory] Health check failed for {name}: {e}")
                return name, False

        if not self.sources:
            return {}

        tasks = [check_source(name, source) for name, source in self.sources.items()]
        results = await asyncio.gather(*tasks)
        
        for name, is_healthy in results:
            health_status[name] = is_healthy

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
        self.region_to_sources = {'primary': {}, 'backup': {}}
        self.instrument_sources.clear()
        self.calendar_sources.clear()
        self.factor_routes.clear()
        self.trading_calendar_cache.clear()

        # 重置全局单例引用，允许下次使用时重新初始化
        global data_source_factory
        data_source_factory = None
        ds_logger.info("[DataSourceFactory] All sources closed, factory reset for re-initialization")


# 全局数据源工厂实例
data_source_factory = None

async def get_data_source_factory(db_operations: DatabaseOperations) -> "DataSourceFactory":
    """获取数据源工厂实例（单例模式）"""
    global data_source_factory
    if data_source_factory is None:
        data_source_factory = DataSourceFactory(db_operations)
        await data_source_factory.initialize()
    return data_source_factory
