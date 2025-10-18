"""
AkShare data source implementation with rate limiting.
Specialized for Chinese A-shares market data.
"""

import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date
from collections import Counter
import pandas as pd
import akshare as ak

from .base_source import BaseDataSource, RateLimitConfig
from .adjustment_config import AdjustmentConfig
from utils.exceptions import DataSourceError, NetworkError, ErrorCodes
from utils import log_execution, data_source_metrics, config_manager, get_shanghai_time, exchange_mapper
from utils import akshare_logger


class AkShareSource(BaseDataSource):
    """AkShare数据源 - 专注A股数据"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.aio_session = None
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

    @log_execution("AkShare", "initialize")
    async def _initialize_impl(self):
        """初始化AkShare数据源"""
        try:
            akshare_logger.info("Initializing AkShare data source")

            # 创建异步HTTP会话，增加超时时间
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

            # 测试连接
            test_data = await self._test_akshare_connection()
            if test_data:
                data_source_metrics.increment("connection_success")
                akshare_logger.info("AkShare connection test successful")
            else:
                data_source_metrics.increment("connection_failure")
                akshare_logger.error("AkShare connection test failed")
                raise DataSourceError(
                    "AkShare connection test failed",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                )

        except Exception as e:
            data_source_metrics.increment("initialization_failure")
            if isinstance(e, (ConnectionError, TimeoutError)):
                akshare_logger.error(f"Network error during AkShare initialization: {str(e)}")
                raise NetworkError(
                    f"Failed to initialize AkShare connection: {str(e)}",
                    ErrorCodes.NETWORK_CONNECTION_ERROR
                ) from e
            else:
                akshare_logger.error(f"Error during AkShare initialization: {str(e)}")
                raise DataSourceError(
                    f"Failed to initialize AkShare: {str(e)}",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                ) from e

    async def _test_akshare_connection(self) -> bool:
        """测试AkShare连接（带重试机制）"""
        max_retries = 3
        retry_delay = 2.0

        for attempt in range(max_retries):
            try:
                akshare_logger.debug(f"[{self.name}] Connection test attempt {attempt + 1}/{max_retries}")

                # 检查AkShare函数是否可用
                if ak.stock_zh_a_spot_em is None:
                    akshare_logger.error(f"[{self.name}] ak.stock_zh_a_spot_em is None, trying to re-import...")
                    # 尝试重新导入akshare
                    import importlib
                    importlib.reload(ak)
                    if ak.stock_zh_a_spot_em is None:
                        akshare_logger.error(f"[{self.name}] ak.stock_zh_a_spot_em is still None after reload")
                        return False

                # 使用 stock_info_a_code_name 测试连接
                test_data = await asyncio.to_thread(ak.stock_info_a_code_name)

                if test_data is not None and not test_data.empty:
                    akshare_logger.debug(f"[{self.name}] Connection test successful on attempt {attempt + 1}")
                    return True
                else:
                    akshare_logger.warning(f"[{self.name}] Connection test returned empty data on attempt {attempt + 1}")

            except Exception as e:
                akshare_logger.warning(f"[{self.name}] Connection test attempt {attempt + 1} failed: {e}")

                if attempt < max_retries - 1:
                    akshare_logger.info(f"[{self.name}] Waiting {retry_delay}s before retry...")
                    await asyncio.sleep(retry_delay)

        akshare_logger.error(f"[{self.name}] All {max_retries} connection test attempts failed")
        return False


    async def get_instrument_list(self, exchange: str = None) -> List[Dict[str, Any]]:
        """获取沪深京A股交易所股票列表（根据配置规则解析）"""
        try:
            await self.rate_limiter.acquire()

            # 安全加载配置
            try:
                start_rules = config_manager.get_nested("exchange_rules.symbol_start_with")
                supported_regions = config_manager.get_nested("data_sources_config.akshare.exchanges_supported")
            except Exception as config_error:
                akshare_logger.error(f"[{self.name}] Failed to load config: {config_error}")
                return []

            # 使用工具函数验证交易所配置
            if exchange and not exchange_mapper.is_exchange_supported(exchange, supported_regions):
                akshare_logger.warning(f"[{self.name}] Unsupported exchange: {exchange}")
                return []

            # 根据交易所选择对应的 AkShare API
            ak_exchange_api_map = {
                "SSE": ak.stock_info_sh_name_code,
                "SZSE": ak.stock_info_sz_name_code,
                "BSE": ak.stock_info_bj_name_code
            }

            stock_list = None

            if exchange and exchange in ak_exchange_api_map:
                # 使用特定交易所的 API
                try:
                    if exchange == "SSE":
                        # 分步获取沪市股票：主板A股和科创板
                        all_sse_stocks = []

                        # 获取主板A股
                        try:
                            main_board_stocks = await asyncio.to_thread(
                                ak.stock_info_sh_name_code,
                                symbol="主板A股"
                            )
                            if not main_board_stocks.empty:
                                all_sse_stocks.append(main_board_stocks)
                                akshare_logger.info(f"[{self.name}] Got {len(main_board_stocks)} SSE main board stocks")
                        except Exception as e:
                            akshare_logger.warning(f"[{self.name}] Failed to get SSE main board stocks: {e}")

                        # 获取科创板
                        try:
                            star_market_stocks = await asyncio.to_thread(
                                ak.stock_info_sh_name_code,
                                symbol="科创板"
                            )
                            if not star_market_stocks.empty:
                                all_sse_stocks.append(star_market_stocks)
                                akshare_logger.info(f"[{self.name}] Got {len(star_market_stocks)} SSE STAR market stocks")
                        except Exception as e:
                            akshare_logger.warning(f"[{self.name}] Failed to get SSE STAR market stocks: {e}")

                        # 合并所有沪市股票
                        if all_sse_stocks:
                            stock_list = pd.concat(all_sse_stocks, ignore_index=True)
                            akshare_logger.info(f"[{self.name}] Got total {len(stock_list)} SSE stocks (main board + STAR market)")
                        else:
                            stock_list = pd.DataFrame()
                    else:
                        # 其他交易所使用原有逻辑
                        stock_list = await asyncio.to_thread(ak_exchange_api_map[exchange])
                        akshare_logger.info(f"[{self.name}] Got {len(stock_list)} {exchange} stocks from specific API")

                except Exception as e:
                    akshare_logger.warning(f"[{self.name}] Failed to get {exchange} stocks from specific API: {e}")
                    stock_list = None

            # 如果特定交易所API失败，或者没有指定交易所，尝试使用全市场API
            if stock_list is None or stock_list.empty:
                try:
                    akshare_logger.info(f"[{self.name}] Using stock_info_a_code_name as fallback")
                    all_stocks = await asyncio.to_thread(ak.stock_info_a_code_name)
                    if all_stocks.empty:
                        akshare_logger.warning(f"[{self.name}] Empty stock list returned from AkShare")
                        return []

                    # 如果指定了交易所，根据股票代码前缀过滤
                    if exchange:
                        prefixes = start_rules.get(exchange, [])
                        if prefixes:
                            mask = all_stocks['code'].str.startswith(tuple(prefixes))
                            stock_list = all_stocks[mask]
                            akshare_logger.info(f"[{self.name}] Filtered {len(stock_list)} {exchange} stocks from {len(all_stocks)} total stocks")
                        else:
                            stock_list = all_stocks  # 如果没有配置前缀规则，返回全部
                            akshare_logger.warning(f"[{self.name}] No prefix rules for {exchange}, returning all stocks")
                    else:
                        stock_list = all_stocks  # 如果没有指定交易所，返回全部

                except Exception as e:
                    akshare_logger.error(f"[{self.name}] Failed to get stock list from stock_info_a_code_name: {e}")
                    return []

            if stock_list.empty:
                akshare_logger.warning(f"[{self.name}] Empty stock list returned from AkShare")
                return []

            def _get_exchange(symbol: str) -> str:
                """使用工具函数判断交易所（仅用于全市场）"""
                exchange = exchange_mapper.get_exchange_by_symbol(symbol)
                return exchange if exchange else "UNKNOWN"

            instruments = []
            for row in stock_list.itertuples(index=False):
                try:
                    # 根据数据来源确定列名
                    if exchange in ["SSE"]:  # stock_info_sh_name_code
                        symbol = str(row.证券代码)
                        name = str(row.证券简称)
                        exchange_code = "SSE"
                    elif exchange in ["SZSE"]:  # stock_info_sz_name_code
                        symbol = str(row.A股代码)
                        name = str(row.A股简称)
                        exchange_code = "SZSE"
                    elif exchange in ["BSE"]:  # stock_info_bj_name_code
                        symbol = str(row.证券代码)
                        name = str(row.证券简称)
                        exchange_code = "BSE"
                    else:  # stock_info_a_code_name (fallback)
                        symbol = str(row.code)
                        name = str(row.name)
                        exchange_code = exchange or _get_exchange(symbol)
                        if exchange_code == "UNKNOWN":
                            continue

                    # 所有API都没有价格信息，默认设为活跃状态
                    is_active = True

                    instrument_id = self._generate_instrument_id(symbol, exchange_code)

                    # 判断是否为 ST 股
                    is_st = 'ST' in name.upper() or '*ST' in name.upper()

                    # 统一数据格式，与 Baostock 保持一致
                    instrument_data = {
                        "instrument_id": instrument_id,
                        "symbol": symbol,
                        "name": name,
                        "exchange": exchange_code,
                        "type": "stock",  # 统一为小写
                        "currency": "CNY",
                        "listed_date": None,  # AkShare API 暂时没有提供
                        "delisted_date": None,  # AkShare API 暂时没有提供
                        "industry": "",  # AkShare API 暂时没有提供
                        "sector": "",   # AkShare API 暂时没有提供
                        "market": "",   # AkShare API 暂时没有提供
                        "status": "active",  # 默认为活跃
                        "is_active": is_active,
                        "is_st": is_st,
                        "trading_status": 1,  # 默认为正常交易状态
                        "source": "akshare",
                        "source_symbol": symbol,  # AkShare 的原始代码
                        "created_at": datetime.now(),
                        "updated_at": datetime.now(),
                        "data_version": 1
                    }

                    instruments.append(instrument_data)

                except Exception as e:
                    akshare_logger.debug(f"[{self.name}] Error processing stock {row}: {e}")
                    continue

            exchange_counts = Counter(inst["exchange"] for inst in instruments)

            akshare_logger.info(f"[{self.name}] Retrieved {len(instruments)} instruments ({dict(exchange_counts)})")
            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get instrument list: {e}")
            return []


    async def get_daily_data(self, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """获取A股历史日线数据"""
        try:
            await self.rate_limiter.acquire()

            # 构建AkShare代码格式
            ak_symbol = self._build_akshare_symbol(symbol)
            if not ak_symbol:
                akshare_logger.error(f"[{self.name}] Cannot build AkShare symbol for: {symbol}")
                return []

            # 获取历史数据
            data = await self._fetch_akshare_data(ak_symbol, start_date, end_date)
            if data is None or data.empty:
                akshare_logger.warning(f"[{self.name}] No data found for {ak_symbol}")
                return []

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('akshare')

            # 转换数据格式，按日期排序以计算前收盘价
            data_sorted = data.sort_index()
            quotes = []
            previous_close = None

            for i, (date, row) in enumerate(data_sorted.iterrows()):
                # 基础报价数据
                quote = {
                    'time': date.to_pydatetime(),
                    'instrument_id': instrument_id,
                    'open': float(row['open']) if pd.notna(row['open']) and row['open'] > 0 else 0.0,
                    'high': float(row['high']) if pd.notna(row['high']) and row['high'] > 0 else 0.0,
                    'low': float(row['low']) if pd.notna(row['low']) and row['low'] > 0 else 0.0,
                    'close': float(row['close']) if pd.notna(row['close']) and row['close'] > 0 else 0.0,
                    'volume': int(row['volume']) if pd.notna(row['volume']) and row['volume'] > 0 else 0,
                    'amount': float(row['amount']) if pd.notna(row['amount']) and row['amount'] > 0 else 0.0,
                    'created_at': get_shanghai_time()
                }

                # 使用数据增强函数添加缺失字段
                enhanced_quote = self._enhance_quote_data(quote, row, previous_close)
                quotes.append(enhanced_quote)

                # 更新前收盘价为当前收盘价，供下一个交易日使用
                previous_close = enhanced_quote['close']

            akshare_logger.debug(f"[{self.name}] Retrieved {len(quotes)} daily quotes for {ak_symbol}")
            return quotes

        except Exception as e:
            # 网络错误处理
            if any(keyword in str(e).lower() for keyword in ["connection", "timeout", "remote", "network", "decode", "decompressing"]):
                akshare_logger.warning(f"[{self.name}] Network error for {symbol}: {e}")
                # 可以在这里添加重试逻辑，类似 baostock 的实现
            else:
                akshare_logger.error(f"[{self.name}] Failed to get daily data for {symbol}: {e}")
            return []

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        """获取最新A股日线数据"""
        try:
            await self.rate_limiter.acquire()

            ak_symbol = self._build_akshare_symbol(symbol)
            if not ak_symbol:
                return {}

            # 获取最近几天的数据
            end_date = datetime.now()
            start_date = end_date - timedelta(days=10)

            data = await self._fetch_akshare_data(ak_symbol, start_date, end_date)
            if data is None or data.empty:
                return {}

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('akshare')

            # 按日期排序，获取前一条记录以计算前收盘价
            data_sorted = data.sort_index()

            # 获取最新一条记录
            latest_row = data_sorted.iloc[-1]
            latest_date = data_sorted.index[-1]

            # 获取前一个交易日的收盘价（如果存在）
            previous_close = None
            if len(data_sorted) > 1:
                prev_row = data_sorted.iloc[-2]
                previous_close = float(prev_row['close']) if pd.notna(prev_row['close']) and prev_row['close'] > 0 else None

            # 基础报价数据
            quote = {
                'time': latest_date.to_pydatetime(),
                'instrument_id': instrument_id,
                'open': float(latest_row['open']) if pd.notna(latest_row['open']) and latest_row['open'] > 0 else 0.0,
                'high': float(latest_row['high']) if pd.notna(latest_row['high']) and latest_row['high'] > 0 else 0.0,
                'low': float(latest_row['low']) if pd.notna(latest_row['low']) and latest_row['low'] > 0 else 0.0,
                'close': float(latest_row['close']) if pd.notna(latest_row['close']) and latest_row['close'] > 0 else 0.0,
                'volume': int(latest_row['volume']) if pd.notna(latest_row['volume']) and latest_row['volume'] > 0 else 0,
                'amount': float(latest_row['amount']) if pd.notna(latest_row['amount']) and latest_row['amount'] > 0 else 0.0,
                'created_at': get_shanghai_time()
            }

            # 使用数据增强函数添加缺失字段
            enhanced_quote = self._enhance_quote_data(quote, latest_row, previous_close)
            return enhanced_quote

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get latest data for {symbol}: {e}")
            return {}

    def _build_akshare_symbol(self, symbol: str) -> Optional[str]:
        """构建AkShare代码格式"""
        if len(symbol) == 6:
            return symbol
        akshare_logger.warning(f"[{self.name}] Invalid A-share symbol format: {symbol}")
        return None

    def _enhance_quote_data(self, quote_data: Dict, row_data: pd.Series,
                           previous_close: float = None) -> Dict:
        """增强报价数据，添加缺失字段以匹配Baostock格式"""
        enhanced = quote_data.copy()

        # 基础交易状态字段
        enhanced['tradestatus'] = 1  # 默认正常交易
        enhanced['source'] = 'akshare'

        # 前收盘价处理
        if previous_close and previous_close > 0:
            enhanced['pre_close'] = previous_close
        else:
            enhanced['pre_close'] = 0.0

        # 涨跌额计算
        if 'change' not in enhanced and enhanced['pre_close'] > 0:
            enhanced['change'] = enhanced['close'] - enhanced['pre_close']
        else:
            enhanced['change'] = 0.0

        # 涨跌幅处理
        if 'pct_change' not in enhanced:
            if 'change_pct' in row_data and pd.notna(row_data['change_pct']):
                # AkShare可能直接提供涨跌幅
                enhanced['pct_change'] = float(row_data['change_pct'])
            elif enhanced['pre_close'] > 0:
                # 计算涨跌幅
                enhanced['pct_change'] = (enhanced['close'] - enhanced['pre_close']) / enhanced['pre_close'] * 100
            else:
                enhanced['pct_change'] = 0.0
        else:
            enhanced['pct_change'] = float(enhanced['pct_change'])

        # 换手率处理
        if 'turnover' not in enhanced:
            if 'turnover_rate' in row_data and pd.notna(row_data['turnover_rate']):
                enhanced['turnover'] = float(row_data['turnover_rate'])
            else:
                enhanced['turnover'] = 0.0
        else:
            enhanced['turnover'] = float(enhanced['turnover'])

        # 确保数值类型的正确性
        for field in ['open', 'high', 'low', 'close', 'volume', 'amount']:
            if field in enhanced and enhanced[field] is not None:
                enhanced[field] = float(enhanced[field]) if field != 'volume' else int(float(enhanced[field]))
            elif field in enhanced:
                enhanced[field] = 0.0 if field != 'volume' else 0

        # 添加复权信息
        adjustment_config = AdjustmentConfig.get_adjustment_config('akshare')
        enhanced['factor'] = adjustment_config['factor']

        return enhanced

    async def _fetch_akshare_data(self, symbol: str, start_date: datetime,
                                end_date: datetime) -> Optional[pd.DataFrame]:
        """从AkShare获取历史数据"""
        max_retries = 3
        base_delay = 2.0

        # 获取复权配置
        adjustment_config = AdjustmentConfig.get_adjustment_config('akshare')

        for attempt in range(max_retries):
            try:
                # 调用AkShare API获取历史数据
                data = await asyncio.to_thread(
                    ak.stock_zh_a_hist,
                    symbol=symbol,
                    period="daily",
                    start_date=start_date.strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d'),
                    adjust=adjustment_config['adjust']  # 使用统一的前复权配置
                )

                if data.empty:
                    akshare_logger.warning(f"[{self.name}] No data returned from AkShare for {symbol}")
                    return None

                # 成功获取数据，跳出重试循环
                break

            except Exception as e:
                error_msg = str(e)
                if "Connection aborted" in error_msg or "RemoteDisconnected" in error_msg:
                    akshare_logger.warning(f"[{self.name}] Connection error for {symbol} (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        # 指数退避延迟
                        delay = base_delay * (2 ** attempt) + (attempt * 0.5)
                        akshare_logger.info(f"[{self.name}] Retrying {symbol} in {delay:.1f} seconds...")
                        await asyncio.sleep(delay)
                        continue
                else:
                    akshare_logger.error(f"[{self.name}] Error fetching AkShare data for {symbol}: {e}")
                    return None

        else:
            # 所有重试都失败了
            akshare_logger.error(f"[{self.name}] All retries failed for {symbol}")
            return None

        # 检查是否有有效数据
        if data.empty:
            akshare_logger.warning(f"[{self.name}] Data became empty after retries for {symbol}")
            return None

        # 检测列名格式并设置日期索引
        columns = list(data.columns)
        akshare_logger.debug(f"[{self.name}] Raw columns from AkShare: {columns}")

        # 检测日期列名（中文或英文）
        date_column = None
        for col in ['日期', 'date', 'Date']:
            if col in columns:
                date_column = col
                break

        if date_column is None:
            akshare_logger.error(f"[{self.name}] No date column found in data: {columns}")
            return None

        # 设置日期索引并转换格式
        data[date_column] = pd.to_datetime(data[date_column])
        data.set_index(date_column, inplace=True)

        # 重命名列以匹配标准格式（支持中英文列名）
        column_mapping = {
            # 中文列名
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amount',
            '换手率': 'turnover_rate',
            # 英文列名
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume',
            'amount': 'amount',
            'amplitude': 'amplitude',
            'change_pct': 'change_pct',
            'change_amount': 'change_amount',
            'turnover_rate': 'turnover_rate'
        }

        # 只映射实际存在的列
        available_mapping = {col: new_col for col, new_col in column_mapping.items() if col in data.columns}
        data = data.rename(columns=available_mapping)

        akshare_logger.debug(f"[{self.name}] Columns after mapping: {list(data.columns)}")

        # 检查必要列是否存在
        required_columns = ['open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            akshare_logger.error(f"[{self.name}] Missing required columns: {missing_columns}")
            akshare_logger.error(f"[{self.name}] Available columns: {list(data.columns)}")
            return None

        # 清理数据 - 只删除关键列都为空的行
        key_columns = ['open', 'high', 'low', 'close']
        data = data.dropna(subset=key_columns, how='all')

        if data.empty:
            akshare_logger.warning(f"[{self.name}] Data became empty after cleaning for {symbol}")
            return None

        akshare_logger.debug(f"[{self.name}] Successfully processed {len(data)} rows for {symbol}")
        return data

    async def get_a_stock_indices(self) -> List[Dict[str, Any]]:
        """获取A股指数列表"""
        try:
            await self.rate_limiter.acquire()

            indices = await asyncio.to_thread(
                ak.stock_zh_index_spot_em,
                symbol="沪深重要指数"  # 可获取多种指数：{"沪深重要指数", "上证系列指数", "深证系列指数", "指数成份", "中证系列指数"}
            )

            if indices.empty:
                return []

            instruments = []
            for _, row in indices.iterrows():
                try:
                    symbol = str(row['代码'])
                    name = str(row['名称'])
                    exchange = 'NONE'      # 暂时未开发按照指数代码区分交易所的逻辑
                    instrument_id = self._generate_instrument_id(symbol, exchange)

                    instrument = {
                        'instrument_id': instrument_id,
                        'symbol': symbol,
                        'name': name,
                        'exchange': exchange,
                        'type': 'INDEX',
                        'currency': 'CNY',
                        'is_active': True
                    }
                    instruments.append(instrument)

                except Exception as e:
                    akshare_logger.warning(f"[{self.name}] Error processing index {row}: {e}")
                    continue

            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get A-share indices: {e}")
            return []

    async def get_a_stock_etfs(self) -> List[Dict[str, Any]]:
        """获取A股ETF列表"""
        try:
            await self.rate_limiter.acquire()

            etfs = await asyncio.to_thread(
                ak.fund_etf_spot_em
            )

            if etfs.empty:
                return []

            instruments = []
            for _, row in etfs.iterrows():
                try:
                    symbol = str(row['代码'])
                    name = str(row['名称'])
                    exchange = 'SSE' if symbol.startswith('5') else 'SZSE'
                    instrument_id = self._generate_instrument_id(symbol, exchange)

                    instrument = {
                        'instrument_id': instrument_id,
                        'symbol': symbol,
                        'name': name,
                        'exchange': exchange,
                        'type': 'ETF',
                        'currency': 'CNY',
                        'is_active': True
                    }
                    instruments.append(instrument)

                except Exception as e:
                    akshare_logger.warning(f"[{self.name}] Error processing ETF {row}: {e}")
                    continue

            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get A-share ETFs: {e}")
            return []

    async def get_trading_calendar(self, exchange: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """获取指定交易所的交易日历"""
        try:
            await self.rate_limiter.acquire()

            # 获取所有交易日数据
            trade_days = await asyncio.to_thread(ak.tool_trade_date_hist_sina)

            if trade_days.empty:
                return []

            # 转换为日期格式
            trade_days['trade_date'] = pd.to_datetime(trade_days['trade_date'])

            # 筛选日期范围
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())

            filtered_days = trade_days[
                (trade_days['trade_date'] >= start_datetime) &
                (trade_days['trade_date'] <= end_datetime)
            ]

            # 转换为统一格式
            calendar_data = []
            for _, row in filtered_days.iterrows():
                trade_date = row['trade_date'].date()
                calendar_data.append({
                    'date': trade_date,
                    'exchange': exchange.upper(),
                    'is_trading_day': True
                })

            akshare_logger.debug(f"[{self.name}] Retrieved {len(calendar_data)} trading days for {exchange}")
            return calendar_data

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get trading calendar for {exchange}: {e}")
            return []

    def _get_test_symbol(self) -> Optional[str]:
        """获取测试用的交易代码"""
        return "300708"  # 聚灿光电

    async def health_check(self) -> bool:
        """数据源健康检查，带重试机制"""
        try:
            # 使用连接测试函数
            return await self._test_akshare_connection()

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Health check failed: {e}")
            return False

    async def close(self):
        """关闭AkShare数据源连接和资源"""
        if self.aio_session:
            await self.aio_session.close()
            self.aio_session = None
        await super().close()