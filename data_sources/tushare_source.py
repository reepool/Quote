"""
Tushare data source implementation.
Specialized for Chinese stock market data.
"""

import asyncio
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd

try:
    import tushare as ts
    TUSHARE_AVAILABLE = True
except ImportError:
    TUSHARE_AVAILABLE = False
    ts = None

from .base_source import BaseDataSource, RateLimitConfig
from .adjustment_config import AdjustmentConfig
from utils.exceptions import DataSourceError, NetworkError, ErrorCodes
from utils import log_execution, data_source_metrics, config_manager, get_shanghai_time
from utils import tushare_logger


class TushareSource(BaseDataSource):
    """Tushare数据源 - 专注中国股市数据"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.pro = None
        self.token = None
        self.last_request_time = 0
        self.min_interval = 0.2  # Tushare免费版每分钟200次，即每秒3.3次

    @log_execution("Tushare", "initialize")
    async def _initialize_impl(self):
        """初始化Tushare数据源"""
        try:
            if not TUSHARE_AVAILABLE:
                tushare_logger.error("Tushare library not available. Please install with: pip install tushare")
                raise DataSourceError(
                    "Tushare library not available. Please install with: pip install tushare",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                )

            tushare_logger.info("Initializing Tushare data source")

            # 从配置获取token
            self.token = config_manager.get_nested('data_sources_config.tushare.token', None)
            if not self.token:
                # 尝试从环境变量获取
                import os
                self.token = os.getenv('TUSHARE_TOKEN')

            if not self.token:
                tushare_logger.warning("No Tushare token found, using free interface with limited functionality")
                # 使用免费的接口，不需要token
                ts.set_token('')  # 空token使用免费接口
                self.pro = ts.pro_api()
            else:
                ts.set_token(self.token)
                self.pro = ts.pro_api()

            # 测试连接
            test_result = await self._test_connection()
            if test_result:
                data_source_metrics.increment("connection_success")
                tushare_logger.info("Tushare connection test successful")
            else:
                data_source_metrics.increment("connection_failure")
                tushare_logger.warning("Tushare connection test failed")
                raise DataSourceError(
                    "Tushare connection test failed",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                )

        except Exception as e:
            data_source_metrics.increment("initialization_failure")
            if isinstance(e, (ConnectionError, TimeoutError)):
                tushare_logger.error(f"Network error during Tushare initialization: {str(e)}")
                raise NetworkError(
                    f"Failed to initialize Tushare connection: {str(e)}",
                    ErrorCodes.NETWORK_CONNECTION_ERROR
                ) from e
            else:
                tushare_logger.error(f"Error during Tushare initialization: {str(e)}")
                raise DataSourceError(
                    f"Failed to initialize Tushare: {str(e)}",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                ) from e

    async def _test_connection(self) -> bool:
        """测试Tushare连接"""
        try:
            # 使用trade_cal接口测试连接，这个接口比较轻量
            await asyncio.to_thread(
                self.pro.trade_cal,
                exchange='SSE',
                start_date='20240101',
                end_date='20240101'
            )
            return True
        except Exception as e:
            tushare_logger.error(f"Tushare connection test failed: {e}")
            return False

    async def get_instrument_list(self, exchange: str = None) -> List[Dict[str, Any]]:
        """获取交易品种列表"""
        try:
            await self._respect_rate_limit()

            if exchange == 'SSE':
                exchange_code = 'SSE'
            elif exchange == 'SZSE':
                exchange_code = 'SZSE'
            else:
                exchange_code = 'SSE'  # 默认上交所

            # 获取股票基本信息
            df = await asyncio.to_thread(
                self.pro.stock_basic,
                exchange=exchange_code,
                list_status='L',
                fields='ts_code,symbol,name,area,industry,list_date'
            )

            if df.empty:
                tushare_logger.warning(f"No instruments found for {exchange}")
                return []

            instruments = []
            for _, row in df.iterrows():
                instrument = {
                    'instrument_id': row['ts_code'],
                    'symbol': row['symbol'],
                    'name': row['name'],
                    'exchange': exchange,
                    'type': 'stock',
                    'currency': 'CNY',
                    'is_active': True,
                    'created_at': get_shanghai_time(),
                    'updated_at': get_shanghai_time()
                }
                instruments.append(instrument)

            tushare_logger.info(f"Retrieved {len(instruments)} instruments from Tushare for {exchange}")
            return instruments

        except Exception as e:
            tushare_logger.error(f"Failed to get instrument list from Tushare: {e}")
            return []

    async def get_daily_data(self, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """获取历史日线数据"""
        try:
            await self._respect_rate_limit()

            # 转换日期格式
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')

            # 获取日线数据
            df = await asyncio.to_thread(
                self.pro.daily,
                ts_code=instrument_id,
                start_date=start_str,
                end_date=end_str
            )

            if df.empty:
                tushare_logger.debug(f"No data found for {instrument_id} from {start_str} to {end_str}")
                return []

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('tushare')

            # 转换数据格式
            quotes = []
            for _, row in df.iterrows():
                quote = {
                    'time': pd.to_datetime(row['trade_date']),
                    'instrument_id': instrument_id,
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row['vol']) if pd.notna(row['vol']) else 0,
                    'amount': float(row['amount']) if pd.notna(row['amount']) else 0.0,
                    'factor': adjustment_config['factor']  # Tushare前复权数据
                }
                quotes.append(quote)

            tushare_logger.debug(f"Retrieved {len(quotes)} daily quotes for {instrument_id}")
            return quotes

        except Exception as e:
            tushare_logger.error(f"Failed to get daily data from Tushare for {instrument_id}: {e}")
            return []

    async def _respect_rate_limit(self):
        """遵守频率限制"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            await asyncio.sleep(sleep_time)

        self.last_request_time = time.time()

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            await self._respect_rate_limit()

            # 测试获取交易日历
            df = await asyncio.to_thread(
                self.pro.trade_cal,
                exchange='SSE',
                start_date='20240101',
                end_date='20240101'
            )

            return not df.empty

        except Exception as e:
            tushare_logger.error(f"Tushare health check failed: {e}")
            return False

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        """获取最新日线数据"""
        try:
            await self._respect_rate_limit()

            # 获取最近2个交易日的数据
            from datetime import datetime, timedelta
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

            df = await asyncio.to_thread(
                self.pro.daily,
                ts_code=instrument_id,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                return {}

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('tushare')

            # 获取最新一条记录
            latest = df.iloc[0]

            return {
                'time': pd.to_datetime(latest['trade_date']),
                'instrument_id': instrument_id,
                'open': float(latest['open']),
                'high': float(latest['high']),
                'low': float(latest['low']),
                'close': float(latest['close']),
                'volume': int(latest['vol']) if pd.notna(latest['vol']) else 0,
                'amount': float(latest['amount']) if pd.notna(latest['amount']) else 0.0,
                'factor': adjustment_config['factor']
            }

        except Exception as e:
            tushare_logger.error(f"Failed to get latest daily data from Tushare for {instrument_id}: {e}")
            return {}

    async def close(self):
        """关闭数据源连接"""
        # Tushare不需要显式关闭连接
        pass