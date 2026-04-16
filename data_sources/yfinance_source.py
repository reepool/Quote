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
import os
from contextlib import contextmanager

from .base_source import BaseDataSource, RateLimitConfig
from .adjustment_config import AdjustmentConfig
from utils import yfinance_logger, config_manager

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

    _FACTOR_ANCHOR_LOOKBACK_DAYS = 90
    _FACTOR_THRESHOLD = 0.0001

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.supported_exchanges = ['HKEX', 'NASDAQ', 'NYSE']  # yfinance 仅用于港股和美股
        self.aio_session = None
        self.user_agent = YFinanceConstants.DEFAULT_USER_AGENT
        
        # 加载代理配置
        proxy_config = config_manager.get_nested('data_sources_config.yfinance.proxy', {})
        self.proxy_dict = {}
        if isinstance(proxy_config, dict):
            http_proxy = proxy_config.get('http', '')
            https_proxy = proxy_config.get('https', '')
            if http_proxy:
                self.proxy_dict['http'] = http_proxy
            if https_proxy:
                self.proxy_dict['https'] = https_proxy
        
        self.aio_proxy = self.proxy_dict.get('http') or self.proxy_dict.get('https')

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

            # 尝试通过环境变量重定向缓存目录到非保护区，避免权限报错
            try:
                import yfinance as yf
                yf.set_tz_cache_location("/tmp/py-yfinance")
            except Exception:
                pass

            # 测试连接 - 使用异步原生请求代替脆弱的 yfinance 内库
            test_symbol = YFinanceConstants.TEST_SYMBOL_US
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{test_symbol}"
            
            try:
                async with self.aio_session.get(url, proxy=self.aio_proxy, timeout=5) as response:
                    if response.status == 200:
                        yfinance_logger.info(f"[{self.name}] Successfully connected to Yahoo Finance")
                    else:
                        yfinance_logger.warning(f"[{self.name}] Yahoo Finance connection test returned {response.status} - will use as backup source")
            except Exception as e:
                yfinance_logger.warning(f"[{self.name}] Yahoo Finance connection test failed: {e} - will use as backup source")

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
                           start_date: datetime, end_date: datetime,
                           instrument_type: str = 'stock',
                           source_symbol: str = '') -> List[Dict[str, Any]]:
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
                    'factor': 1.0,           # 非复权原始数据，复权由本地引擎计算
                    'adjustment_type': 'none',
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
                'factor': 1.0,           # 非复权原始数据
                'adjustment_type': 'none',
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

        if exchange_upper in ['SSE', 'SZSE']:
            # A股（不含北交所，Yahoo Finance 不支持 BSE）
            if len(symbol) == 6:
                if exchange_upper == 'SSE' or symbol.startswith(('6', '9')):
                    return f"{symbol}.SS"  # 上交所
                else:
                    return f"{symbol}.SZ"  # 深交所
        elif exchange_upper in ('HKEX', 'HK'):
            # 港股（exchange 来自 instrument_id.split('.') 时为 'HK'，来自配置时为 'HKEX'）
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
                              end_date: datetime = None, period: str = None) -> Optional[pd.DataFrame]:
        """从Yahoo Finance获取数据"""
        try:
            yfinance_logger.debug(f"[{self.name}] Fetching data for {symbol}")

            # 首先尝试使用yfinance库（更稳定）
            data = await self._fetch_yahoo_data_library(symbol, start_date, end_date, period)
            if data is not None and not data.empty:
                yfinance_logger.debug(f"[{self.name}] Successfully fetched {len(data)} records for {symbol}")
                return data

            # 如果yfinance库失败，尝试使用异步HTTP方法
            if start_date and end_date:
                yfinance_logger.debug(f"[{self.name}] Trying async HTTP method for {symbol}")
                data = await self._fetch_yahoo_data_async(symbol, start_date, end_date)
                if data is not None and not data.empty:
                    return data

            yfinance_logger.warning(f"[{self.name}] All fetch attempts failed for {symbol}")
            return None

        except Exception as e:
            yfinance_logger.warning(f"[{self.name}] Fetch operation failed for {symbol}: {e}")
            return None

    @contextmanager
    def _temporary_proxy_env(self):
        """仅在执行时挂载代理，避免污染全局（针对 YF crumb 不听话的问题）"""
        if not getattr(self, 'aio_proxy', None):
            yield
            return
            
        old_http = os.environ.get('HTTP_PROXY')
        old_https = os.environ.get('HTTPS_PROXY')
        
        os.environ['HTTP_PROXY'] = self.aio_proxy
        os.environ['HTTPS_PROXY'] = self.aio_proxy
        try:
            yield
        finally:
            if old_http is not None:
                os.environ['HTTP_PROXY'] = old_http
            else:
                os.environ.pop('HTTP_PROXY', None)
                
            if old_https is not None:
                os.environ['HTTPS_PROXY'] = old_https
            else:
                os.environ.pop('HTTPS_PROXY', None)

    async def _fetch_yahoo_data_library(self, symbol: str, start_date: datetime = None,
                                      end_date: datetime = None, period: str = None,
                                      timeout_sec: int = 8) -> Optional[pd.DataFrame]:
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
            
            # 使用代理池
            if self.proxy_dict:
                session.proxies.update(self.proxy_dict)

            ticker = yf.Ticker(symbol, session=session)

            with self._temporary_proxy_env():
                if start_date and end_date:
                    # 指定日期范围 — 关闭 auto_adjust，获取原始未复权价格
                    data = await asyncio.wait_for(
                        asyncio.to_thread(
                            ticker.history,
                            start=start_date.strftime('%Y-%m-%d'),
                            end=end_date.strftime('%Y-%m-%d'),
                            interval='1d',
                            auto_adjust=False,   # 保留 Adj Close 列，用于因子计算
                            repair=True,
                            proxy=self.aio_proxy  # 必须显式传入，否则 crumb 获取会死锁
                        ),
                        timeout=timeout_sec
                    )
                elif period:
                    # 指定时间段
                    data = await asyncio.wait_for(
                        asyncio.to_thread(
                            ticker.history,
                            period=period,
                            interval='1d',
                            auto_adjust=False,
                            repair=True,
                            proxy=self.aio_proxy  # 必须显式传入
                        ),
                        timeout=timeout_sec
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

        except asyncio.TimeoutError:
            yfinance_logger.debug(f"[{self.name}] yfinance library timed out for {symbol} after {timeout_sec}s")
            return None
        except Exception as e:
            yfinance_logger.debug(f"[{self.name}] yfinance library failed for {symbol}: {e}")
            return None

    async def _fetch_yahoo_data_async(self, symbol: str, start_date: datetime = None,
                                    end_date: datetime = None) -> Optional[pd.DataFrame]:
        """使用底层原生 requests 通过线程池异步化发起获取数据"""
        if not start_date or not end_date:
            yfinance_logger.error(f"[{self.name}] Must specify date range for async fetch")
            return None

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            'period1': int(start_date.timestamp()),
            'period2': int(end_date.timestamp()),
            'interval': '1d',
            'events': 'history'
        }

        try:
            def _do_fetch():
                headers = {'User-Agent': self.user_agent, 'Accept': '*/*'}
                proxies = {'http': self.aio_proxy, 'https': self.aio_proxy} if self.aio_proxy else None
                return requests.get(url, params=params, headers=headers, proxies=proxies, timeout=15)

            response = await asyncio.to_thread(_do_fetch)
            
            if response.status_code == 200:
                data = response.json()
                chart = data.get('chart', {})
                result = chart.get('result', [])
                if not result:
                    yfinance_logger.warning(f"[{self.name}] No result found in chart API for {symbol}")
                    return None
                    
                res_data = result[0]
                timestamps = res_data.get('timestamp')
                if not timestamps:
                    yfinance_logger.debug(f"[{self.name}] No timestamps found for {symbol}")
                    return pd.DataFrame()

                quote = res_data.get('indicators', {}).get('quote', [{}])[0]
                adjclose = res_data.get('indicators', {}).get('adjclose', [{}])[0]
                
                df = pd.DataFrame({
                    'Date': pd.to_datetime(timestamps, unit='s'),
                    'Open': quote.get('open', [None]*len(timestamps)),
                    'High': quote.get('high', [None]*len(timestamps)),
                    'Low': quote.get('low', [None]*len(timestamps)),
                    'Close': quote.get('close', [None]*len(timestamps)),
                    'Adj Close': adjclose.get('adjclose', [None]*len(timestamps)) if adjclose else quote.get('close', [None]*len(timestamps)),
                    'Volume': quote.get('volume', [None]*len(timestamps)),
                })
                
                df.set_index('Date', inplace=True)
                df.index = df.index.tz_localize(None)
                return df
            else:
                text = response.text
                yfinance_logger.warning(f"[{self.name}] V8 Chart API returned status {response.status_code} for {symbol}: {text[:100]}")
                return None

        except Exception as e:
            yfinance_logger.error(f"[{self.name}] Requests fetch via V8 Chart failed for {symbol}: {e}")
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

    def _build_sparse_factor_events(
        self,
        instrument_id: str,
        cum_factor: pd.Series,
        requested_start: datetime,
        requested_end: datetime,
    ) -> List[Dict[str, Any]]:
        """将累积因子序列压缩为稀疏事件列表。

        关键点：
        1. 带窗口前锚点时，只记录窗口内真正发生跳变的日期
        2. 无锚点时，允许首点非 1.0 作为初始锚记录
        """
        if cum_factor is None or len(cum_factor) == 0:
            return []

        requested_start_date = requested_start.date()
        requested_end_date = requested_end.date()

        normalized = pd.to_numeric(cum_factor.copy(), errors='coerce')
        normalized.index = pd.to_datetime(normalized.index)
        normalized = normalized.replace([float('inf'), float('-inf')], float('nan')).dropna()
        normalized = normalized[normalized > 0]
        normalized = normalized[normalized.index >= pd.Timestamp('1990-01-01')].sort_index()
        if normalized.empty:
            return []

        has_anchor_before_range = any(ts.date() < requested_start_date for ts in normalized.index)
        shifts = normalized.diff().abs()

        factors: List[Dict[str, Any]] = []
        prev_cum: Optional[float] = None

        for ts, cum in normalized.items():
            current_date = ts.date()
            cum_val = float(cum)
            shift_raw = shifts.get(ts)
            shift_val = float(shift_raw) if pd.notna(shift_raw) else None

            is_first_point = prev_cum is None
            is_event_day = False
            if is_first_point:
                is_event_day = not has_anchor_before_range and abs(cum_val - 1.0) > self._FACTOR_THRESHOLD
            elif shift_val is not None and shift_val > self._FACTOR_THRESHOLD:
                is_event_day = True

            if requested_start_date <= current_date <= requested_end_date and is_event_day:
                day_factor = round(cum_val / prev_cum, 6) if prev_cum not in (None, 0) else round(cum_val, 6)
                factors.append({
                    'instrument_id': instrument_id,
                    'ex_date': ts.to_pydatetime(),
                    'factor': day_factor,
                    'cumulative_factor': round(cum_val, 6),
                    'source': 'yfinance',
                })

            prev_cum = cum_val

        return factors

    async def get_adjustment_factors(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """从 yfinance 的 Adj Close 反推复权因子

        原理：yfinance 在 auto_adjust=False 时同时返回 Close（原始价）和 Adj Close（复权价）。
        两者之比即当日累积复权因子。在发生突变的日期为除权除息事件。
        仅返回因子发生显著变化的事件日（阈值 > 0.0001），避免冗余记录。
        """
        try:
            await self.rate_limiter.acquire()
            exchange = instrument_id.split('.')[-1] if '.' in instrument_id else None
            yf_symbol = self._build_yf_symbol(symbol, exchange)
            if not yf_symbol:
                return []

            anchor_start = start_date - timedelta(days=self._FACTOR_ANCHOR_LOOKBACK_DAYS)
            data = await self._fetch_yahoo_data(yf_symbol, anchor_start, end_date)
            if data is None or data.empty:
                return []

            # 必须含有 Adj Close 列（auto_adjust=False 时提供）
            if 'Adj Close' not in data.columns:
                yfinance_logger.warning(
                    f"[{self.name}] 'Adj Close' column missing for {yf_symbol}, cannot compute factors"
                )
                return []

            # 计算每日累积因子 = Adj Close / Close
            close_col = 'Close'
            adj_col = 'Adj Close'
            data = data.copy()
            data[close_col] = pd.to_numeric(data[close_col], errors='coerce')
            data[adj_col] = pd.to_numeric(data[adj_col], errors='coerce')
            data = data.dropna(subset=[close_col, adj_col])
            data = data[(data[close_col] > 0) & (data[adj_col] > 0)].sort_index()
            if data.empty:
                return []

            data['_cum_factor'] = data[adj_col] / data[close_col]
            factors = self._build_sparse_factor_events(
                instrument_id=instrument_id,
                cum_factor=data['_cum_factor'],
                requested_start=start_date,
                requested_end=end_date,
            )

            yfinance_logger.info(
                f"[{self.name}] Found {len(factors)} adjustment factor events for {yf_symbol}"
            )
            return factors

        except Exception as e:
            yfinance_logger.warning(
                f"[{self.name}] Failed to get adjustment factors for {symbol}: {e}"
            )
            return []

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 使用简单的底层 HTTP 请求去验证代理连通性，避开 yfinance.Ticker 内核脆弱的 crumb 生成器
            test_symbol = YFinanceConstants.TEST_SYMBOL_US
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{test_symbol}"
            
            # 使用预设好的 AIO Session（携带了 User-Agent）和 代理
            async with self.aio_session.get(url, proxy=self.aio_proxy, timeout=10) as response:
                if response.status == 200:
                    yfinance_logger.debug(f"[{self.name}] Health check passed directly to endpoints")
                    return True
                else:
                    yfinance_logger.warning(f"[{self.name}] Health check HTTP failed with status {response.status}")
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
