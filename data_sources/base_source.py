"""
base data source class for the quote system.
Provides common functionality for all data sources with rate limiting and  features.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from dataclasses import dataclass

from utils import ds_logger, config_manager
from database.models import Instrument, DailyQuote, TradingCalendar


@dataclass
class RateLimitConfig:
    """限流配置"""
    max_requests_per_minute: int = 60
    max_requests_per_hour: int = 1000
    max_requests_per_day: int = 10000
    retry_times: int = 3
    retry_interval: float = 1.0


@dataclass
class DataQualityConfig:
    """数据质量配置"""
    min_price: float = 0.01  # 最小价格
    max_price: float = 100000  # 最大价格
    max_price_change_pct: float = 50  # 最大涨跌幅百分比
    min_volume: int = 0  # 最小成交量
    max_volume: int = 10**12  # 最大成交量
    require_tradestatus: bool = True  # 是否要求交易状态
    quality_threshold: float = 0.5  # 数据质量阈值


class RateLimiter:
    """限流器"""

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.requests = []  # 存储请求时间戳
        self.lock = asyncio.Lock()
        self.last_reset_time = time.monotonic()
        self.daily_request_count = 0

    async def acquire(self):
        """获取请求许可"""
        async with self.lock:
            now = time.monotonic()

            # 每天重置计数器
            if now - self.last_reset_time > 86400:
                self.requests = []
                self.daily_request_count = 0
                self.last_reset_time = now

            # 清理过期的请求记录
            self.requests = [req_time for req_time in self.requests
                           if now - req_time < 86400]

            # 检查各个时间窗口的限流
            minute_ago = now - 60
            hour_ago = now - 3600
            day_ago = now - 86400

            minute_requests = [t for t in self.requests if t > minute_ago]
            hour_requests = [t for t in self.requests if t > hour_ago]
            day_requests = [t for t in self.requests if t > day_ago]

            requests_minute = len(minute_requests)
            requests_hour = len(hour_requests)
            requests_day = len(day_requests)

            wait_time = 0
            limit_reached = False

            if requests_minute >= self.config.max_requests_per_minute:
                limit_reached = True
                earliest_min = minute_requests[0]
                wait_time = max(wait_time, (earliest_min + 60) - now)

            if requests_hour >= self.config.max_requests_per_hour:
                limit_reached = True
                earliest_hour = hour_requests[0]
                wait_time = max(wait_time, (earliest_hour + 3600) - now)

            if requests_day >= self.config.max_requests_per_day:
                limit_reached = True
                earliest_day = day_requests[0]
                wait_time = max(wait_time, (earliest_day + 86400) - now)

            if limit_reached and wait_time > 0:
                # 限制最大等待时间为30秒，避免长时间阻塞
                max_wait = 30.0
                actual_wait = min(wait_time, max_wait)
                ds_logger.warning(f"[RateLimiter] Rate limit reached, waiting {actual_wait:.1f} seconds")
                await asyncio.sleep(actual_wait)

            # 记录当前请求
            self.requests.append(now)
            self.daily_request_count += 1
            ds_logger.debug(f"[RateLimiter] Request acquired. Recent: {requests_minute}/min, {requests_hour}/hour, {requests_day}/day")


class BaseDataSource(ABC):
    """数据源基类"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None,
                 quality_config: DataQualityConfig = None):
        self.name = name
        self.rate_limiter = RateLimiter(rate_limit_config or RateLimitConfig())
        self.quality_config = quality_config or DataQualityConfig()
        self.session = None
        self.is_initialized = False
        self.supported_exchanges = []
        self.trading_calendar_cache = {}

    async def initialize(self):
        """初始化数据源"""
        if not self.is_initialized:
            ds_logger.info(f"[{self.name}] Initializing  data source...")
            await self._initialize_impl()
            self.is_initialized = True
            ds_logger.info(f"[{self.name}] data source initialized successfully")

    @abstractmethod
    async def _initialize_impl(self):
        """初始化实现"""
        pass

    async def close(self):
        """关闭数据源连接"""
        if self.session:
            await self.session.close()
            self.session = None
        ds_logger.info(f"[{self.name}] data source closed")

    @abstractmethod
    async def get_instrument_list(self, exchange: str = None) -> List[Dict[str, Any]]:
        """获取交易品种列表"""
        pass

    @abstractmethod
    async def get_daily_data(self, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """获取历史日线数据"""
        pass

    @abstractmethod
    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        """获取最新日线数据"""
        pass

    # 可选的方法，子类可以选择性实现
    async def get_trading_calendar(self, exchange: str, start_date: date,
                                  end_date: date) -> List[Dict[str, Any]]:
        """获取交易日历（可选实现）"""
        return []

    async def get_instruments_batch(self, exchange: str, batch_size: int = 100) -> List[List[Dict[str, Any]]]:
        """分批获取交易品种列表"""
        try:
            await self.rate_limiter.acquire()

            all_instruments = await self.get_instrument_list(exchange)

            # 分批处理
            batches = []
            for i in range(0, len(all_instruments), batch_size):
                batch = all_instruments[i:i + batch_size]
                batches.append(batch)

            ds_logger.info(f"[{self.name}] Split {len(all_instruments)} instruments into {len(batches)} batches")
            return batches

        except Exception as e:
            ds_logger.error(f"[{self.name}] Failed to get instruments batch: {e}")
            raise

    async def get_daily_data_batch(self, instruments: List[Dict[str, Any]],
                                 start_date: datetime, end_date: datetime,
                                 batch_size: int = 50) -> List[Dict[str, Any]]:
        """批量获取日线数据"""
        all_data = []
        total_instruments = len(instruments)

        for i in range(0, len(instruments), batch_size):
            batch = instruments[i:i + batch_size]
            ds_logger.info(f"[{self.name}] Processing batch {i//batch_size + 1}/{(total_instruments-1)//batch_size + 1}")

            # 并行处理批次内的每个品种
            tasks = []
            for instrument in batch:
                task = self._get_single_instrument_data(
                    instrument['instrument_id'],
                    instrument['symbol'],
                    start_date,
                    end_date
                )
                tasks.append(task)

            # 等待批次完成
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理结果
            for result in batch_results:
                if isinstance(result, Exception):
                    ds_logger.error(f"[{self.name}] Error in batch processing: {result}")
                elif result:
                    all_data.extend(result)

            # 批次间延迟，避免限流
            if i + batch_size < len(instruments):
                await asyncio.sleep(1.0)

        ds_logger.info(f"[{self.name}] Retrieved {len(all_data)} daily quotes for {total_instruments} instruments")
        return all_data

    async def _get_single_instrument_data(self, instrument_id: str, symbol: str,
                                        start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """获取单个品种的日线数据（带重试机制和质量检查）"""
        for attempt in range(self.rate_limiter.config.retry_times):
            try:
                await self.rate_limiter.acquire()
                data = await self.get_daily_data(instrument_id, symbol, start_date, end_date)

                # 数据质量检查
                if data:
                    quality_score = self._assess_data_quality(data, symbol)
                    if quality_score > self.quality_config.quality_threshold:  # 使用配置化的质量阈值
                        # 为每条数据添加质量评分
                        for quote in data:
                            quote['quality_score'] = quality_score
                        return data
                    else:
                        ds_logger.warning(f"[{self.name}] Data quality too low for {symbol}: {quality_score}")

            except Exception as e:
                ds_logger.warning(f"[{self.name}] Attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt < self.rate_limiter.config.retry_times - 1:
                    await asyncio.sleep(self.rate_limiter.config.retry_interval * (2 ** attempt))
                else:
                    ds_logger.error(f"[{self.name}] All attempts failed for {symbol}: {e}")

        return []

    async def get_latest_data_batch(self, instruments: List[Dict[str, Any]],
                                   batch_size: int = 50) -> List[Dict[str, Any]]:
        """批量获取最新数据"""
        all_data = []

        for i in range(0, len(instruments), batch_size):
            batch = instruments[i:i + batch_size]
            ds_logger.info(f"[{self.name}] Processing latest data batch {i//batch_size + 1}")

            # 并行处理
            tasks = []
            for instrument in batch:
                task = self._get_single_latest_data(
                    instrument['instrument_id'],
                    instrument['symbol']
                )
                tasks.append(task)

            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理结果
            for result in batch_results:
                if isinstance(result, Exception):
                    ds_logger.error(f"[{self.name}] Error in latest batch processing: {result}")
                elif result:
                    all_data.append(result)

            # 批次间延迟
            if i + batch_size < len(instruments):
                await asyncio.sleep(0.5)

        ds_logger.info(f"[{self.name}] Retrieved latest data for {len(all_data)} instruments")
        return all_data

    async def _get_single_latest_data(self, instrument_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """获取单个品种的最新数据（带重试机制和质量检查）"""
        for attempt in range(self.rate_limiter.config.retry_times):
            try:
                await self.rate_limiter.acquire()
                data = await self.get_latest_daily_data(instrument_id, symbol)

                if data:
                    quality_score = self._assess_single_data_quality(data, symbol)
                    if quality_score > self.quality_config.quality_threshold:
                        data['quality_score'] = quality_score
                        return data
                    else:
                        ds_logger.warning(f"[{self.name}] Latest data quality too low for {symbol}: {quality_score}")

            except Exception as e:
                ds_logger.warning(f"[{self.name}] Latest data attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt < self.rate_limiter.config.retry_times - 1:
                    await asyncio.sleep(self.rate_limiter.config.retry_interval)
                else:
                    ds_logger.error(f"[{self.name}] All latest data attempts failed for {symbol}: {e}")
                    return None

        return None

    def _assess_data_quality(self, data: List[Dict[str, Any]], symbol: str) -> float:
        """评估数据质量"""
        if not data:
            return 0.0

        quality_scores = []
        for quote in data:
            score = self._assess_single_data_quality(quote, symbol)
            quality_scores.append(score)

        return sum(quality_scores) / len(quality_scores)

    def _assess_single_data_quality(self, quote: Dict[str, Any], symbol: str) -> Optional[float]:
        """评估单条数据质量"""
        score = 1.0

        try:
            # 检查必需字段
            required_fields = ['open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in quote or quote[field] is None:
                    score -= 0.2

            # 检查价格合理性
            open_price = float(quote.get('open', 0))
            high_price = float(quote.get('high', 0))
            low_price = float(quote.get('low', 0))
            close_price = float(quote.get('close', 0))
            volume = int(quote.get('volume', 0))

            # 价格范围检查
            if open_price < self.quality_config.min_price or open_price > self.quality_config.max_price:
                score -= 0.3
            if close_price < self.quality_config.min_price or close_price > self.quality_config.max_price:
                score -= 0.3

            # 价格逻辑检查
            if high_price < low_price:
                score -= 0.4
            if high_price < open_price or high_price < close_price:
                score -= 0.2
            if low_price > open_price or low_price > close_price:
                score -= 0.2

            # 成交量检查
            if volume < self.quality_config.min_volume or volume > self.quality_config.max_volume:
                score -= 0.1

            # 涨跌幅检查（如果有前收盘价）
            if 'pre_close' in quote and quote['pre_close']:
                pre_close = float(quote['pre_close'])
                if pre_close > 0:
                    change_pct = abs((close_price - pre_close) / pre_close * 100)
                    if change_pct > self.quality_config.max_price_change_pct:
                        score -= 0.3

            # 交易状态检查
            if self.quality_config.require_tradestatus and 'tradestatus' in quote:
                if quote.get('tradestatus', 1) != 1:  # 1表示正常交易
                    score -= 0.1

        except (ValueError, TypeError) as e:
            ds_logger.warning(f"[{self.name}] Data quality assessment error for {symbol}: {e}")
            return 0.0

        return max(0.0, score)

    def _generate_instrument_id(self, symbol: str, exchange: str) -> str:
        """生成交易品种ID"""
        return f"{symbol}.{exchange}"

    def _normalize_symbol(self, symbol: str, exchange: str) -> str:
        """标准化交易代码"""
        # 读取配置文件中的交易代码规则
        rules = config_manager.get_nested("exchange_rules.symbol_rules")
        width = rules.get(exchange.upper())

        if symbol.isdigit() and width:
            if isinstance(width, int):
                return symbol.zfill(width)
            elif isinstance(width, list):
                for w in sorted(width):
                    if len(symbol) <= w:
                        return symbol.zfill(w)
                return symbol
        return symbol

    def _parse_instrument_data(self, raw_data: Dict[str, Any], exchange: str) -> Dict[str, Any]:
        """解析交易品种数据为标准格式"""
        return {
            'instrument_id': self._generate_instrument_id(raw_data.get('symbol', ''), exchange),
            'symbol': self._normalize_symbol(raw_data.get('symbol', ''), exchange),
            'name': raw_data.get('name', ''),
            'exchange': exchange,
            'type': raw_data.get('type', 'stock'),
            'currency': raw_data.get('currency', 'CNY'),
            'listed_date': raw_data.get('listed_date'),
            'delisted_date': raw_data.get('delisted_date'),
            'industry': raw_data.get('industry', ''),
            'sector': raw_data.get('sector', ''),
            'market': raw_data.get('market', ''),
            'status': raw_data.get('status', 'active'),
            'is_active': raw_data.get('is_active', True),
            'is_st': raw_data.get('is_st', False),
            'trading_status': raw_data.get('trading_status', 1),
            'source': self.name,
            'source_symbol': raw_data.get('symbol', ''),
        }

    def _parse_quote_data(self, raw_data: Dict[str, Any], instrument_id: str,
                         symbol: str) -> Dict[str, Any]:
        """解析行情数据为标准格式"""
        return {
            'time': raw_data.get('time'),
            'instrument_id': instrument_id,
            'symbol': symbol,
            'open': float(raw_data.get('open', 0)),
            'high': float(raw_data.get('high', 0)),
            'low': float(raw_data.get('low', 0)),
            'close': float(raw_data.get('close', 0)),
            'volume': int(raw_data.get('volume', 0)),
            'amount': float(raw_data.get('amount', 0)),
            'turnover': raw_data.get('turnover'),
            'pre_close': raw_data.get('pre_close'),
            'change': raw_data.get('change'),
            'pct_change': raw_data.get('pct_change'),
            'tradestatus': raw_data.get('tradestatus', 1),
            'factor': raw_data.get('factor', 1.0),
            'adjustment_type': raw_data.get('adjustment_type', 'none'),
            'is_complete': raw_data.get('is_complete', True),
            'quality_score': raw_data.get('quality_score', 1.0),
            'source': self.name,
        }

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 简单的健康检查，获取一个已知品种的数据
            test_symbol = self._get_test_symbol()
            if test_symbol:
                await self.get_daily_data(
                    self._generate_instrument_id(test_symbol, "TEST"),
                    test_symbol,
                    datetime.now() - timedelta(days=7),
                    datetime.now()
                )
            return True
        except Exception as e:
            ds_logger.error(f"[{self.name}] Health check failed: {e}")
            return False

    def _get_test_symbol(self) -> Optional[str]:
        """获取测试用的交易代码"""
        return None  # 子类可以重写此方法

    def get_source_info(self) -> Dict[str, Any]:
        """获取数据源信息"""
        return {
            'name': self.name,
            'is_initialized': self.is_initialized,
            'supported_exchanges': self.supported_exchanges,
            'rate_limit': {
                'max_requests_per_minute': self.rate_limiter.config.max_requests_per_minute,
                'max_requests_per_hour': self.rate_limiter.config.max_requests_per_hour,
                'max_requests_per_day': self.rate_limiter.config.max_requests_per_day,
                'retry_times': self.rate_limiter.config.retry_times,
            },
            'daily_request_count': self.rate_limiter.daily_request_count,
        }