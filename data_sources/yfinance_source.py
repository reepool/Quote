"""
Yahoo Finance data source implementation with rate limiting.
Supports A-shares, HK stocks, and US stocks.
"""

import requests
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import io

from .base_source import BaseDataSource, RateLimitConfig
from .adjustment_config import AdjustmentConfig
from utils import yfinance_logger

class YFinanceConstants:
    """Yahoo Finance 数据源的常量"""
    # 测试连接和健康检查使用的股票代码
    TEST_SYMBOL_US = "AAPL"
    TEST_SYMBOL_HK = "00700.HK"
    TEST_SYMBOL_CN = "000001.SS"
    # 默认的用户代理
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

class YFinanceSource(BaseDataSource):
    """Yahoo Finance数据源"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.aio_session = None
        self.user_agent = YFinanceConstants.DEFAULT_USER_AGENT

    async def _initialize_impl(self):
        """初始化Yahoo Finance数据源"""
        try:
            # 创建异步HTTP会话
            timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=30)
            connector = aiohttp.TCPConnector(
                limit=8,
                limit_per_host=8,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )
            self.aio_session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'User-Agent': self.user_agent}
            )

            # 测试连接 - 使用美股作为测试，因为更稳定
            test_symbol = YFinanceConstants.TEST_SYMBOL_US
            test_data = await self._fetch_yahoo_data(test_symbol, period="5d", max_retries=2)
            if test_data is not None and not test_data.empty:
                yfinance_logger.info(f"[{self.name}] Successfully connected to Yahoo Finance")
            else:
                yfinance_logger.warning(f"[{self.name}] Yahoo Finance connection test failed - will use as backup source")

        except Exception as e:
            yfinance_logger.warning(f"[{self.name}] Yahoo Finance initialization failed: {e}")
            yfinance_logger.info(f"[{self.name}] Will continue as backup data source")
            # 不抛出异常，允许作为备用数据源

    async def get_instrument_list(self, exchange: str = None) -> List[Dict[str, Any]]:
        """获取交易品种列表 - yfinance不支持此功能"""
        _ = exchange  # 参数保留以保持接口一致性，但实际不使用
        yfinance_logger.warning(f"[{self.name}] yfinance does not support getting complete instrument lists")
        yfinance_logger.info(f"[{self.name}] yfinance should only be used for historical data fetching")
        return []

    
    async def get_daily_data(self, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """获取历史日线数据"""
        try:
            await self.rate_limiter.acquire()
            
            # 从 instrument_id 中解析交易所信息
            exchange = instrument_id.split('.')[-1] if '.' in instrument_id else None
            
            # 构建Yahoo Finance代码
            yf_symbol = self._build_yf_symbol(symbol, exchange)
            if not yf_symbol:
                yfinance_logger.error(f"[{self.name}] Cannot build Yahoo Finance symbol for: {symbol}")
                return []

            # 获取数据
            data = await self._fetch_yahoo_data(yf_symbol, start_date, end_date)
            if data is None or data.empty:
                yfinance_logger.warning(f"[{self.name}] No data found for {yf_symbol}")
                return []

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('yfinance')

            # 转换数据格式
            quotes = []
            for date, row in data.iterrows():
                # 计算成交额（如果需要的话）
                amount = None
                if pd.notna(row['Volume']) and pd.notna(row['Close']) and row['Volume'] > 0:
                    # 对于A股，可以估算成交额 = 成交量 * 收盘价
                    # 对于美股和港股，Yahoo Finance通常不提供准确的成交额数据
                    name_lower = self.name.lower()
                    if any(keyword in name_lower for keyword in ['a_stock', 'cn_stock', 'china']):
                        amount = float(row['Volume']) * float(row['Close'])
                    else:
                        amount = None  # 美股和港股保持None

                quote = {
                    'time': date.to_pydatetime(),
                    'instrument_id': instrument_id,
                    'open': float(row['Open']) if pd.notna(row['Open']) else 0.0,
                    'high': float(row['High']) if pd.notna(row['High']) else 0.0,
                    'low': float(row['Low']) if pd.notna(row['Low']) else 0.0,
                    'close': float(row['Close']) if pd.notna(row['Close']) else 0.0,
                    'volume': int(row['Volume']) if pd.notna(row['Volume']) and row['Volume'] > 0 else None,
                    'amount': amount,
                    'factor': adjustment_config['factor']  # yfinance前复权数据
                }
                quotes.append(quote)

            yfinance_logger.debug(f"[{self.name}] Retrieved {len(quotes)} daily quotes for {yf_symbol}")
            return quotes

        except Exception as e:
            yfinance_logger.error(f"[{self.name}] Failed to get daily data for {symbol}: {e}")
            return []

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        """获取最新日线数据"""
        try:
            await self.rate_limiter.acquire()
            
            # 从 instrument_id 中解析交易所信息
            exchange = instrument_id.split('.')[-1] if '.' in instrument_id else None

            yf_symbol = self._build_yf_symbol(symbol, exchange)
            if not yf_symbol:
                return {}

            # 获取最近5天的数据
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)

            data = await self._fetch_yahoo_data(yf_symbol, start_date, end_date)
            if data is None or data.empty:
                return {}

            # 获取最新一条记录
            latest_row = data.iloc[-1]
            latest_date = data.index[-1]

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('yfinance')

            # 计算成交额（与get_daily_data保持一致）
            amount = None
            if pd.notna(latest_row['Volume']) and pd.notna(latest_row['Close']) and latest_row['Volume'] > 0:
                name_lower = self.name.lower()
                if any(keyword in name_lower for keyword in ['a_stock', 'cn_stock', 'china']):
                    amount = float(latest_row['Volume']) * float(latest_row['Close'])
                else:
                    amount = None

            return {
                'time': latest_date.to_pydatetime(),
                'instrument_id': instrument_id,
                'open': float(latest_row['Open']) if pd.notna(latest_row['Open']) else 0.0,
                'high': float(latest_row['High']) if pd.notna(latest_row['High']) else 0.0,
                'low': float(latest_row['Low']) if pd.notna(latest_row['Low']) else 0.0,
                'close': float(latest_row['Close']) if pd.notna(latest_row['Close']) else 0.0,
                'volume': int(latest_row['Volume']) if pd.notna(latest_row['Volume']) and latest_row['Volume'] > 0 else None,
                'amount': amount,
                'factor': adjustment_config['factor']  # yfinance前复权数据
            }

        except Exception as e:
            yfinance_logger.error(f"[{self.name}] Failed to get latest data for {symbol}: {e}")
            return {}

    def _build_yf_symbol(self, symbol: str, exchange: Optional[str]) -> Optional[str]:
        """构建Yahoo Finance代码"""
        if not isinstance(symbol, str) or not symbol:
            yfinance_logger.warning(f"[{self.name}] Invalid symbol provided: {symbol}")
            return None

        exchange_upper = exchange.upper() if exchange else None

        if exchange_upper in ['SSE', 'SZSE', 'BSE']:
            # A股
            if len(symbol) == 6:
                if exchange_upper == 'SSE' or symbol.startswith(('6', '9')): # 兼容没有交易所信息的情况
                    return f"{symbol}.SS"  # 上交所
                else:
                    return f"{symbol}.SZ"  # 深交所
        elif exchange_upper == 'HKEX':
            # 港股
            if len(symbol) == 5 and symbol.isdigit():
                return f"{symbol}.HK"
        elif exchange_upper in ['NASDAQ', 'NYSE']:
            # 美股
            return symbol
        
        # 如果已经符合Yahoo格式，直接返回
        if '.' in symbol:
            return symbol

        yfinance_logger.warning(f"[{self.name}] Cannot determine Yahoo Finance suffix for symbol '{symbol}' with exchange '{exchange}'")
        return None

    async def _fetch_yahoo_data(self, symbol: str, start_date: datetime = None,
                              end_date: datetime = None, period: str = None, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """从Yahoo Finance获取数据"""
        for attempt in range(max_retries):
            try:
                yfinance_logger.debug(f"[{self.name}] Fetching data for {symbol}, attempt {attempt + 1}/{max_retries}")

                # 首先尝试使用yfinance库（更稳定）
                data = await self._fetch_yahoo_data_library(symbol, start_date, end_date, period)
                if data is not None and not data.empty:
                    yfinance_logger.debug(f"[{self.name}] Successfully fetched {len(data)} records for {symbol}")
                    return data

                # 如果yfinance库失败，尝试使用异步HTTP方法
                if start_date and end_date and attempt < max_retries - 1:
                    yfinance_logger.debug(f"[{self.name}] Trying async HTTP method for {symbol}")
                    data = await self._fetch_yahoo_data_async(symbol, start_date, end_date)
                    if data is not None and not data.empty:
                        return data

            except Exception as e:
                yfinance_logger.warning(f"[{self.name}] Attempt {attempt + 1} failed for {symbol}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # 指数退避

        yfinance_logger.warning(f"[{self.name}] All attempts failed for {symbol}")
        return None

    async def _fetch_yahoo_data_library(self, symbol: str, start_date: datetime = None,
                                      end_date: datetime = None, period: str = None) -> Optional[pd.DataFrame]:
        """使用yfinance库获取数据"""
        try:
            session = requests.Session()
            session.headers['User-Agent'] = self.user_agent
            session.headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })

            ticker = yf.Ticker(symbol, session=session)

            if start_date and end_date:
                # 获取复权配置
                adjustment_config = AdjustmentConfig.get_adjustment_config('yfinance')

                # 指定日期范围
                data = await asyncio.to_thread(
                    ticker.history,
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d'),
                    interval='1d',
                    auto_adjust=adjustment_config['auto_adjust'],
                    repair=True
                )
            elif period:
                # 获取复权配置
                adjustment_config = AdjustmentConfig.get_adjustment_config('yfinance')

                # 指定时间段
                data = await asyncio.to_thread(
                    ticker.history,
                    period=period,
                    interval='1d',
                    auto_adjust=adjustment_config['auto_adjust'],
                    repair=True
                )
            else:
                yfinance_logger.error(f"[{self.name}] Must specify either date range or period")
                return None

            if data.empty:
                yfinance_logger.debug(f"[{self.name}] No data returned for {symbol}")
                return None

            # 清理数据
            data = data.dropna()
            if data.empty:
                return None

            return data

        except Exception as e:
            yfinance_logger.debug(f"[{self.name}] yfinance library failed for {symbol}: {e}")
            return None

    async def _fetch_yahoo_data_async(self, symbol: str, start_date: datetime = None,
                                    end_date: datetime = None) -> Optional[pd.DataFrame]:
        """使用异步HTTP请求获取Yahoo Finance数据（备用方法）"""
        try:
            # 构建URL
            if start_date and end_date:
                period1 = int(start_date.timestamp())
                period2 = int(end_date.timestamp())
                url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
                params = {
                    'period1': period1,
                    'period2': period2,
                    'interval': '1d',
                    'events': 'history',
                    'includeAdjustedClose': 'true'
                }
            else:
                yfinance_logger.error(f"[{self.name}] Must specify date range for async fetch")
                return None

            async with self.aio_session.get(url, params=params) as response:
                if response.status == 200:
                    # 读取CSV数据
                    csv_data = await response.text()
                    df = pd.read_csv(io.StringIO(csv_data))

                    if df.empty:
                        return None

                    # 设置日期索引
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.set_index('Date', inplace=True)

                    # 重命名列
                    df.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

                    return df
                else:
                    yfinance_logger.error(f"[{self.name}] HTTP error {response.status} for {symbol}")
                    return None

        except Exception as e:
            yfinance_logger.error(f"[{self.name}] Async fetch failed for {symbol}: {e}")
            return None

    def _get_test_symbol(self) -> Optional[str]:
        """获取测试用的交易代码"""
        name_lower = self.name.lower()

        # 美股测试代码
        if any(keyword in name_lower for keyword in ['us_stock', 'nasdaq', 'nyse']):
            return YFinanceConstants.TEST_SYMBOL_US
        # 港股测试代码
        elif any(keyword in name_lower for keyword in ['hk_stock', 'hongkong']):
            return YFinanceConstants.TEST_SYMBOL_HK
        # A股测试代码
        elif any(keyword in name_lower for keyword in ['a_stock', 'cn_stock', 'china']):
            return YFinanceConstants.TEST_SYMBOL_CN
        return None

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 使用美股作为健康检查，更稳定
            test_symbol = YFinanceConstants.TEST_SYMBOL_US

            # 测试获取最近数据
            data = await self._fetch_yahoo_data(test_symbol, period="5d", max_retries=2)
            if data is not None and not data.empty:
                yfinance_logger.debug(f"[{self.name}] Health check passed with {len(data)} records")
                return True
            else:
                yfinance_logger.warning(f"[{self.name}] Health check failed - no data returned")
                return False

        except Exception as e:
            yfinance_logger.warning(f"[{self.name}] Health check failed: {e}")
            return False

    async def close(self):
        """关闭数据源连接"""
        if self.aio_session:
            await self.aio_session.close()
            self.aio_session = None
        await super().close()