"""
Yahoo Finance data source implementation with rate limiting.
Supports A-shares, HK stocks, and US stocks.
"""

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


class YFinanceSource(BaseDataSource):
    """Yahoo Finance数据源"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.aio_session = None
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

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
            test_symbol = "AAPL"  # 使用美股测试，比A股更稳定
            test_data = await self._fetch_yahoo_data(test_symbol, period="5d")
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

            # 构建Yahoo Finance代码
            yf_symbol = self._build_yf_symbol(symbol)
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

            yf_symbol = self._build_yf_symbol(symbol)
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

    def _build_yf_symbol(self, symbol: str) -> Optional[str]:
        """构建Yahoo Finance代码"""
        name_lower = self.name.lower()

        # A股数据源判断 - 更精确的匹配
        if any(keyword in name_lower for keyword in ['a_stock', 'cn_stock', 'china']):
            # A股
            if len(symbol) == 6:
                if symbol.startswith(('6', '9')):
                    return f"{symbol}.SS"  # 上交所
                else:
                    return f"{symbol}.SZ"  # 深交所
            elif len(symbol) == 9 and '.' in symbol:
                # 已经是Yahoo格式
                return symbol
        # 港股数据源判断
        elif any(keyword in name_lower for keyword in ['hk_stock', 'hongkong']):
            # 港股
            if len(symbol) == 5 and symbol.isdigit():
                return f"{symbol}.HK"
            elif len(symbol) == 9 and '.' in symbol:
                # 已经是Yahoo格式
                return symbol
        # 美股数据源判断
        elif any(keyword in name_lower for keyword in ['us_stock', 'nasdaq', 'nyse']):
            # 美股
            return symbol

        yfinance_logger.warning(f"[{self.name}] Unknown symbol format: {symbol}")
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
            # 设置yfinance的用户代理和会话
            import requests
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
            return "AAPL"
        # 港股测试代码
        elif any(keyword in name_lower for keyword in ['hk_stock', 'hongkong']):
            return "00700.HK"
        # A股测试代码
        elif any(keyword in name_lower for keyword in ['a_stock', 'cn_stock', 'china']):
            return "000001.SS"
        return None

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 使用美股作为健康检查，更稳定
            test_symbol = "AAPL"

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