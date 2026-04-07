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

# AkShare 代理补丁 - 必须在 import akshare 之前调用
# 此处不能使用 config_manager（会触发循环导入），直接读取 JSON 配置
import json
import logging
from pathlib import Path

_patch_logger = logging.getLogger("akshare_proxy_patch_init")

def _install_akshare_proxy_patch() -> None:
    """从配置文件读取 proxy_patch 参数并安装补丁"""
    config_path = Path(__file__).resolve().parent.parent / "config" / "03_data.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data_config = json.load(f)
        patch_cfg = data_config.get("data_sources_config", {}).get("akshare", {}).get("proxy_patch", {})
        if not patch_cfg.get("enabled", False):
            _patch_logger.info("akshare proxy patch 未启用，跳过")
            return

        import akshare_proxy_patch
        akshare_proxy_patch.install_patch(
            patch_cfg.get("gateway", "101.201.173.125"),
            auth_token=patch_cfg.get("auth_token", ""),
            retry=patch_cfg.get("retry", 30),
            hook_domains=patch_cfg.get("hook_domains", [
                "fund.eastmoney.com",
                "push2.eastmoney.com",
                "push2his.eastmoney.com",
                "emweb.securities.eastmoney.com",
            ]),
        )
        _patch_logger.info("akshare proxy patch 已安装 (token=%s***)", patch_cfg.get("auth_token", "")[:6])
    except ImportError:
        _patch_logger.warning("akshare-proxy-patch 未安装，跳过代理补丁")
    except Exception as e:
        _patch_logger.warning("安装 akshare proxy patch 失败: %s", e)

_install_akshare_proxy_patch()

import akshare as ak

from .base_source import BaseDataSource, RateLimitConfig
from .adjustment_config import AdjustmentConfig
from utils.exceptions import DataSourceError, NetworkError, ErrorCodes
from utils import log_execution, data_source_metrics, config_manager, get_shanghai_time, exchange_mapper
from utils import akshare_logger


class AkShareSource(BaseDataSource):
    """AkShare数据源 - 支持 A 股 (SSE/SZSE/BSE) + 港股 (HKEX) + 美股 (NASDAQ/NYSE)"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.supported_exchanges = ['SSE', 'SZSE', 'BSE']  # AkShare 支持全部 A 股交易所
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


    async def get_instrument_list(self, exchange: str = None, instrument_types: List[str] = None) -> List[Dict[str, Any]]:
        """获取交易所股票列表，支持 A 股 (SSE/SZSE/BSE)、港股 (HKEX)、美股 (NASDAQ/NYSE)"""
        try:
            await self.rate_limiter.acquire()

            # 港股分支
            if exchange == 'HKEX':
                return await self._get_hk_instrument_list()

            # 美股分支
            if exchange in ('NASDAQ', 'NYSE'):
                return await self._get_us_instrument_list(exchange)

            # 安全加载配置（A 股路径）
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

            # 判断是否需要拉取底层股票列表
            needs_stock = not instrument_types or any(t in instrument_types for t in ["stock", "etf"])

            if needs_stock and exchange and exchange in ak_exchange_api_map:
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
            if needs_stock and (stock_list is None or stock_list.empty):
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
            if stock_list is None:
                stock_list = pd.DataFrame()

            def _get_exchange(symbol: str) -> str:
                """使用工具函数判断交易所（仅用于全市场）"""
                exchange = exchange_mapper.get_exchange_by_symbol(symbol)
                return exchange if exchange else "UNKNOWN"

            instruments = []
            if not stock_list.empty:
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

        # == 新增指数获取逻辑 ==
            if instrument_types and "index" in instrument_types:
                try:
                    index_dfs = []
                    if not exchange or exchange == "SSE":
                        df_sh = await asyncio.to_thread(ak.stock_zh_index_spot_em, symbol="上证系列指数")
                        df_sh["exchange_code"] = "SSE"
                        index_dfs.append(df_sh)
                    if not exchange or exchange == "SZSE":
                        df_sz = await asyncio.to_thread(ak.stock_zh_index_spot_em, symbol="深证系列指数")
                        df_sz["exchange_code"] = "SZSE"
                        index_dfs.append(df_sz)

                    if index_dfs:
                        all_indices = pd.concat(index_dfs, ignore_index=True)
                        for row in all_indices.itertuples(index=False):
                            try:
                                symbol = str(row.代码).zfill(6)
                                name = str(row.名称)
                                exchange_code = str(row.exchange_code)
                                instrument_id = self._generate_instrument_id(symbol, exchange_code)
                                
                                instrument_data = {
                                    "instrument_id": instrument_id,
                                    "symbol": symbol,
                                    "name": name,
                                    "exchange": exchange_code,
                                    "type": "index",
                                    "currency": "CNY",
                                    "listed_date": None,
                                    "delisted_date": None,
                                    "industry": "",
                                    "sector": "",
                                    "market": "",
                                    "status": "active",
                                    "is_active": True,
                                    "is_st": False,
                                    "trading_status": 1,
                                    "source": "akshare",
                                    "source_symbol": symbol,
                                    "created_at": datetime.now(),
                                    "updated_at": datetime.now(),
                                    "data_version": 1
                                }
                                instruments.append(instrument_data)
                            except Exception as parse_e:
                                akshare_logger.warning(f"[{self.name}] Failed to parses index row: {parse_e}")
                        akshare_logger.info(f"[{self.name}] Successfully appended {len(all_indices)} indices")
                except Exception as e:
                    akshare_logger.error(f"[{self.name}] Failed to fetch indices from AkShare: {e}")

            from collections import Counter
            exchange_counts = Counter(inst["exchange"] for inst in instruments)

            akshare_logger.info(f"[{self.name}] Retrieved {len(instruments)} instruments ({dict(exchange_counts)})")
            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get instrument list: {e}")
            return []


    async def get_daily_data(self, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime,
                           instrument_type: str = 'stock',
                           source_symbol: str = '') -> List[Dict[str, Any]]:
        """获取历史日线数据，支持 A 股 / 港股 / 美股"""
        try:
            await self.rate_limiter.acquire()

            # 从 instrument_id 后缀推断交易所
            suffix = instrument_id.split('.')[-1].upper() if '.' in instrument_id else ''

            # 港股 / 美股路径
            if suffix == 'HK':
                data = await self._fetch_akshare_data(
                    symbol, start_date, end_date, instrument_type,
                    instrument_id=instrument_id
                )
                if data is None or data.empty:
                    akshare_logger.warning(f"[{self.name}] No data found for {symbol} (HK)")
                    return []
            elif suffix == 'US':
                # 优先使用 source_symbol（已在 _get_us_instrument_list 中保存的东财前缀代码）
                # 如 "105.AAPL"（NASDAQ）/ "106.BAC"（NYSE），避免盲猜浪费 API 调用
                if source_symbol and '.' in source_symbol:
                    us_code = source_symbol
                    akshare_logger.debug(f"[{self.name}] Using source_symbol directly: {us_code}")
                else:
                    # 降级：盲猜前缀，先尝试 NASDAQ(105)，再尝试 NYSE(106)
                    us_code = f"105.{symbol}"
                    akshare_logger.debug(f"[{self.name}] source_symbol unavailable, guessing: {us_code}")

                data = await self._fetch_akshare_data(
                    us_code, start_date, end_date, instrument_type,
                    instrument_id=instrument_id
                )
                if (data is None or data.empty) and not (source_symbol and '.' in source_symbol):
                    # 仅在盲猜模式下才尝试第二个前缀
                    akshare_logger.debug(f"[{self.name}] 105.{symbol} empty, trying 106.{symbol} (NYSE)")
                    us_code = f"106.{symbol}"
                    data = await self._fetch_akshare_data(
                        us_code, start_date, end_date, instrument_type,
                        instrument_id=instrument_id
                    )
                if data is None or data.empty:
                    akshare_logger.warning(f"[{self.name}] No data found for {symbol} (US)")
                    return []
            else:
                # A 股路径（原有逻辑）
                ak_symbol = self._build_akshare_symbol(symbol)
                if not ak_symbol and instrument_type != 'index':
                    akshare_logger.error(f"[{self.name}] Cannot build AkShare symbol for: {symbol}")
                    return []
                data = await self._fetch_akshare_data(
                    symbol if instrument_type == 'index' else ak_symbol,
                    start_date, end_date, instrument_type,
                    instrument_id=instrument_id
                )

            # 共用校验
            if data is None or data.empty:
                akshare_logger.warning(f"[{self.name}] No data found for {symbol}")
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

            akshare_logger.debug(f"[{self.name}] Retrieved {len(quotes)} daily quotes for {symbol}")
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

        # 复权信息 (非复权原始数据, 复权由本地引擎计算)
        enhanced['factor'] = 1.0
        enhanced['adjustment_type'] = 'none'

        return enhanced

    async def _fetch_akshare_data(self, symbol: str, start_date: datetime,
                                end_date: datetime, instrument_type: str = 'stock',
                                instrument_id: str = '') -> Optional[pd.DataFrame]:
        """从 AkShare 获取历史数据，按市场分发不同 API"""
        max_retries = 3
        base_delay = 2.0

        # 从 instrument_id 后缀判断市场
        suffix = instrument_id.split('.')[-1].upper() if instrument_id and '.' in instrument_id else ''

        # 复权配置: 统一使用非复权原始数据
        adjustment_config = AdjustmentConfig.get_adjustment_config('akshare')
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')

        for attempt in range(max_retries):
            try:
                if instrument_type == 'index':
                    # A 股指数
                    data = await asyncio.to_thread(
                        ak.index_zh_a_hist,
                        symbol=symbol,
                        period="daily",
                        start_date=start_str,
                        end_date=end_str
                    )
                elif suffix == 'HK':
                    # 港股: ak.stock_hk_hist — symbol 格式为 5 位数字，如 "00700"
                    data = await asyncio.to_thread(
                        ak.stock_hk_hist,
                        symbol=symbol,
                        period="daily",
                        start_date=start_str,
                        end_date=end_str,
                        adjust=""  # 不复权
                    )
                elif suffix == 'US':
                    # 美股: ak.stock_us_hist — symbol 格式为东财前缀代码，如 "105.AAPL"
                    # source_symbol 已在 _get_us_instrument_list 阶段存为东财格式
                    data = await asyncio.to_thread(
                        ak.stock_us_hist,
                        symbol=symbol,
                        period="daily",
                        start_date=start_str,
                        end_date=end_str,
                        adjust=""  # 不复权
                    )
                else:
                    # A 股（含北交所）
                    data = await asyncio.to_thread(
                        ak.stock_zh_a_hist,
                        symbol=symbol,
                        period="daily",
                        start_date=start_str,
                        end_date=end_str,
                        adjust=adjustment_config['adjust']  # 空字符串 = 不复权
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
                        delay = base_delay * (2 ** attempt) + (attempt * 0.5)
                        akshare_logger.info(f"[{self.name}] Retrying {symbol} in {delay:.1f} seconds...")
                        await asyncio.sleep(delay)
                        continue
            except TypeError as e:
                # 东财返回的数据结构缺失导致'NoneType' object is not subscriptable，说明该指数无此区间数据
                akshare_logger.warning(f"[{self.name}] No data structure found for {symbol} (TypeError)")
                return None
            except Exception as e:
                akshare_logger.error(f"[{self.name}] Error fetching AkShare data for {symbol}: {e}")
                return None

        else:
            akshare_logger.error(f"[{self.name}] All retries failed for {symbol}")
            return None

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

        # 重命名列（港股/美股列名与 A股完全一致，均为中文）
        column_mapping = {
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
            # 英文兜底
            'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close',
            'volume': 'volume', 'amount': 'amount', 'amplitude': 'amplitude',
            'change_pct': 'change_pct', 'change_amount': 'change_amount',
            'turnover_rate': 'turnover_rate'
        }

        available_mapping = {col: new_col for col, new_col in column_mapping.items() if col in data.columns}
        data = data.rename(columns=available_mapping)

        akshare_logger.debug(f"[{self.name}] Columns after mapping: {list(data.columns)}")

        required_columns = ['open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            akshare_logger.error(f"[{self.name}] Missing required columns: {missing_columns}")
            akshare_logger.error(f"[{self.name}] Available columns: {list(data.columns)}")
            return None

        data = data.dropna(subset=['open', 'high', 'low', 'close'], how='all')

        if data.empty:
            akshare_logger.warning(f"[{self.name}] Data became empty after cleaning for {symbol}")
            return None

        akshare_logger.debug(f"[{self.name}] Successfully processed {len(data)} rows for {symbol}")
        return data

    # ------------------------------------------------------------------ #
    #  港股 / 美股品种列表                                                   #
    # ------------------------------------------------------------------ #

    def _build_instrument_record(self, symbol: str, name: str, exchange: str,
                                  currency: str, source_symbol: str) -> Dict[str, Any]:
        """生成统一格式的品种记录"""
        instrument_id = self._generate_instrument_id(symbol, exchange)
        return {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "name": name,
            "exchange": exchange,
            "type": "stock",
            "currency": currency,
            "listed_date": None,
            "delisted_date": None,
            "industry": "",
            "sector": "",
            "market": "",
            "status": "active",
            "is_active": True,
            "is_st": False,
            "trading_status": 1,
            "source": "akshare",
            "source_symbol": source_symbol,  # 东财原始代码，用于后续 API 调用
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "data_version": 1,
        }

    async def _get_hk_instrument_list(self) -> List[Dict[str, Any]]:
        """获取港股全量品种列表（约 2800 只）"""
        try:
            akshare_logger.info(f"[{self.name}] Fetching HK stock list from AkShare...")
            df = await asyncio.to_thread(ak.stock_hk_spot_em)
            if df is None or df.empty:
                akshare_logger.warning(f"[{self.name}] Empty HK stock list")
                return []

            akshare_logger.debug(f"[{self.name}] HK spot columns: {list(df.columns)}")

            instruments = []
            for row in df.itertuples(index=False):
                try:
                    # 东财港股代码格式: "00700" 或不足5位时补零
                    raw_code = str(getattr(row, '代码', '')).strip()
                    symbol = raw_code.zfill(5)  # 港股代码固定5位
                    name = str(getattr(row, '名称', '')).strip()
                    if not symbol or not name:
                        continue
                    instruments.append(
                        self._build_instrument_record(symbol, name, 'HKEX', 'HKD', raw_code)
                    )
                except Exception as row_e:
                    akshare_logger.debug(f"[{self.name}] HK row parse error: {row_e}")

            akshare_logger.info(f"[{self.name}] Got {len(instruments)} HK instruments")
            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get HK instrument list: {e}")
            return []

    async def _get_us_instrument_list(self, exchange: str) -> List[Dict[str, Any]]:
        """获取美股品种列表，按东财前缀区分 NASDAQ(105) / NYSE(106)"""
        # 东财美股前缀映射: 105 → NASDAQ, 106 → NYSE, 107 → AMEX
        exchange_prefix_map = {'NASDAQ': '105', 'NYSE': '106'}
        target_prefix = exchange_prefix_map.get(exchange)
        if not target_prefix:
            akshare_logger.warning(f"[{self.name}] Unknown US exchange: {exchange}")
            return []

        try:
            akshare_logger.info(f"[{self.name}] Fetching US stock list ({exchange}) from AkShare...")
            df = await asyncio.to_thread(ak.stock_us_spot_em)
            if df is None or df.empty:
                akshare_logger.warning(f"[{self.name}] Empty US stock list")
                return []

            akshare_logger.debug(f"[{self.name}] US spot columns: {list(df.columns)}")

            instruments = []
            for row in df.itertuples(index=False):
                try:
                    # 东财美股代码格式: "105.AAPL" / "106.BAC"
                    raw_code = str(getattr(row, '代码', '')).strip()
                    if not raw_code or '.' not in raw_code:
                        continue
                    prefix, ticker = raw_code.split('.', 1)
                    if prefix != target_prefix:
                        continue  # 跳过其他交易所
                    name = str(getattr(row, '名称', '')).strip()
                    if not ticker or not name:
                        continue
                    # instrument_id = "AAPL.US", source_symbol = "105.AAPL" (用于 API 调用)
                    instruments.append(
                        self._build_instrument_record(ticker, name, exchange, 'USD', raw_code)
                    )
                except Exception as row_e:
                    akshare_logger.debug(f"[{self.name}] US row parse error: {row_e}")

            akshare_logger.info(f"[{self.name}] Got {len(instruments)} {exchange} instruments")
            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get US instrument list ({exchange}): {e}")
            return []

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
                    akshare_logger.warning(f"[{self.name}] Failed to process row: {e}")
                    continue

            return instruments

        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to get A-share ETFs: {e}")
            return []

    async def get_trading_calendar(self, exchange: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """获取指定交易所的交易日历
        - A 股 (SSE/SZSE/BSE): 调用新浪交易日历接口
        - 港股 (HKEX) / 美股 (NASDAQ/NYSE): 用 holidays 库本地生成
        """
        try:
            await self.rate_limiter.acquire()

            # ---- 港股 / 美股: 本地生成 ----
            if exchange in ('HKEX', 'NASDAQ', 'NYSE'):
                return self._generate_local_calendar(exchange, start_date, end_date)

            # ---- A 股: 新浪接口 ----
            trade_days = await asyncio.to_thread(ak.tool_trade_date_hist_sina)

            if trade_days.empty:
                return []

            trade_days['trade_date'] = pd.to_datetime(trade_days['trade_date'])

            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())

            filtered_days = trade_days[
                (trade_days['trade_date'] >= start_datetime) &
                (trade_days['trade_date'] <= end_datetime)
            ]

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

    def _generate_local_calendar(self, exchange: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """利用 holidays 库为港股/美股本地生成交易日历（排除周末与法定假日）"""
        try:
            import holidays as hol
            from datetime import timedelta

            if exchange == 'HKEX':
                holiday_set = hol.HK(years=range(start_date.year, end_date.year + 1))
            else:  # NASDAQ / NYSE
                holiday_set = hol.US(years=range(start_date.year, end_date.year + 1))

            calendar_data = []
            current = start_date
            while current <= end_date:
                # 排除周末 (5=Saturday, 6=Sunday) 和法定假日
                if current.weekday() < 5 and current not in holiday_set:
                    calendar_data.append({
                        'date': current,
                        'exchange': exchange.upper(),
                        'is_trading_day': True
                    })
                current += timedelta(days=1)

            akshare_logger.debug(
                f"[{self.name}] Generated {len(calendar_data)} trading days for {exchange} "
                f"({start_date} ~ {end_date})"
            )
            return calendar_data

        except ImportError:
            akshare_logger.warning(f"[{self.name}] 'holidays' library not installed; returning empty calendar for {exchange}")
            return []
        except Exception as e:
            akshare_logger.error(f"[{self.name}] Failed to generate local calendar for {exchange}: {e}")
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

    async def get_adjustment_factors(self, instrument_id: str, symbol: str,
                                     start_date: datetime, end_date: datetime
                                     ) -> List[Dict[str, Any]]:
        """获取复权因子数据

        调用 ak.stock_zh_a_daily(adjust="hfq-factor") 获取后复权因子.
        """
        try:
            await self.rate_limiter.acquire()

            # HKEX 分支: 通过不复权/后复权价格反推因子
            suffix = instrument_id.split('.')[-1].upper() if '.' in instrument_id else ''
            if suffix == 'HK':
                return await self._get_hk_adjustment_factors(
                    instrument_id, symbol, start_date, end_date
                )

            # AkShare 需要带市场前缀的代码
            if 'SH' in instrument_id or 'SSE' in instrument_id:
                ak_symbol = f"sh{symbol}"
            elif 'SZ' in instrument_id:
                ak_symbol = f"sz{symbol}"
            elif 'BJ' in instrument_id or 'BSE' in instrument_id:
                ak_symbol = f"bj{symbol}"
            else:
                akshare_logger.debug(
                    f"Unsupported instrument_id for AkShare factor: {instrument_id}"
                )
                return []

            try:
                factor_df = await asyncio.to_thread(
                    ak.stock_zh_a_daily,
                    symbol=ak_symbol,
                    adjust="hfq-factor"
                )
            except Exception as fetch_e:
                akshare_logger.warning(
                    f"AkShare factor fetch failed for {ak_symbol}: {fetch_e}"
                )
                return []

            if factor_df is None or factor_df.empty:
                return []

            # ---------- 解析: 仅保留因子发生显著变化的除权事件日 ----------
            # ak.stock_zh_a_daily(adjust="hfq-factor") 返回每日一条累积因子记录,
            # 非除权日前后值相同。只记录突变日, 与 BaoStock 保持稀疏存储一致。

            # 识别日期列
            if 'date' in factor_df.columns:
                factor_df = factor_df.set_index('date')
            factor_df.index = pd.to_datetime(factor_df.index)

            # 过滤脏日期 (AkShare 部分 BSE 品种含 1900-01-01 等无效记录)
            factor_df = factor_df[factor_df.index >= pd.Timestamp('1990-01-01')]

            factor_df = factor_df.sort_index()  # ★ 必须按时间正序排列，否则累积复权算法错乱

            # 识别因子列
            factor_col = None
            for col_name in ('hfq_factor', 'factor', factor_df.columns[0]):
                if col_name in factor_df.columns:
                    factor_col = col_name
                    break

            if factor_col is None:
                akshare_logger.warning(
                    f"AkShare factor DataFrame has no recognized factor column for {instrument_id}"
                )
                return []

            # 仅保留日期范围内的记录
            factor_df = factor_df[
                (factor_df.index.date >= start_date.date()) &
                (factor_df.index.date <= end_date.date())
            ]

            if factor_df.empty:
                return []

            # ★ AkShare 返回的因子列可能是 object/str 类型，必须先转 float
            factor_df = factor_df.copy()
            factor_df[factor_col] = pd.to_numeric(factor_df[factor_col], errors='coerce')
            factor_df = factor_df.dropna(subset=[factor_col])

            if factor_df.empty:
                return []

            # 计算相邻日期的因子变化量 (绝对值)
            factor_df['_shift'] = factor_df[factor_col].diff().abs()

            factors = []
            prev_cum = None

            for dt, row in factor_df.iterrows():
                cum = float(row[factor_col])
                shift = float(row['_shift']) if pd.notna(row['_shift']) else None

                # 首行: 如果起始就有累积因子 (非1.0) 记录; 或因子突变
                is_event_day = (
                    (shift is None and abs(cum - 1.0) > 0.0001) or   # 首行非1.0
                    (shift is not None and shift > 0.0001)              # 因子突变
                )

                if is_event_day:
                    # 单日因子 = 当日累积 / 前日累积
                    if prev_cum is not None and prev_cum != 0:
                        day_factor = round(cum / prev_cum, 6)
                    else:
                        day_factor = round(cum, 6)

                    try:
                        factor_record = {
                            'instrument_id': instrument_id,
                            'ex_date': dt.to_pydatetime(),
                            'factor': day_factor,
                            'cumulative_factor': round(cum, 6),
                            'source': 'akshare',
                        }
                        factors.append(factor_record)
                    except Exception as parse_e:
                        akshare_logger.warning(
                            f"Failed to parse AkShare factor row for {instrument_id}: {parse_e}"
                        )

                prev_cum = cum

            akshare_logger.info(
                f"Retrieved {len(factors)} adjustment factors from AkShare for {instrument_id}"
            )
            return factors

        except Exception as e:
            akshare_logger.error(
                f"Failed to get adjustment factors from AkShare for {instrument_id}: {e}"
            )
            return []

    async def _get_hk_adjustment_factors(
        self, instrument_id: str, symbol: str,
        start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """港股复权因子: 对比不复权/后复权收盘价反推累积因子

        原理: cumulative_factor = hfq_close / raw_close
        仅在因子发生显著变化时记录 (除权事件日)，与 A 股稀疏存储一致。
        """
        try:
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')

            # 1. 获取不复权数据
            await self.rate_limiter.acquire()
            raw_df = await asyncio.to_thread(
                ak.stock_hk_hist, symbol=symbol, period="daily",
                start_date=start_str, end_date=end_str, adjust=""
            )

            # 2. 获取后复权数据
            await self.rate_limiter.acquire()
            hfq_df = await asyncio.to_thread(
                ak.stock_hk_hist, symbol=symbol, period="daily",
                start_date=start_str, end_date=end_str, adjust="hfq"
            )

            if raw_df is None or hfq_df is None or raw_df.empty or hfq_df.empty:
                akshare_logger.debug(
                    f"[{self.name}] HK factor: no data for {symbol}"
                )
                return []

            # 3. 标准化日期索引并对齐
            raw_df['日期'] = pd.to_datetime(raw_df['日期'])
            hfq_df['日期'] = pd.to_datetime(hfq_df['日期'])
            raw_df = raw_df.set_index('日期').sort_index()
            hfq_df = hfq_df.set_index('日期').sort_index()

            # 取交集日期
            common_dates = raw_df.index.intersection(hfq_df.index)
            if common_dates.empty:
                return []

            raw_close = raw_df.loc[common_dates, '收盘'].astype(float)
            hfq_close = hfq_df.loc[common_dates, '收盘'].astype(float)

            # 4. 计算每日累积因子 = hfq_close / raw_close
            cum_factor = (hfq_close / raw_close).round(6)

            # 过滤掉无效值 (raw_close == 0 会产生 inf)
            cum_factor = cum_factor.replace([float('inf'), float('-inf')], float('nan')).dropna()
            if cum_factor.empty:
                return []

            # 5. 仅保留因子突变日
            # ★ 港股阈值 0.03: 通过价格反推因子的浮点精度噪声远大于 A 股，
            #   实测 00700 全量数据中 0.0001 产生 3642 个假阳性，
            #   0.03 可精确识别拆股/特别股息等真正除权事件
            _HK_FACTOR_THRESHOLD = 0.03
            factor_shift = cum_factor.diff().abs()
            factors: List[Dict[str, Any]] = []
            prev_cum = None

            for dt, cum in cum_factor.items():
                cum_val = float(cum)
                shift_val = float(factor_shift.get(dt, 0)) if pd.notna(factor_shift.get(dt)) else None

                is_event_day = (
                    (shift_val is None and abs(cum_val - 1.0) > _HK_FACTOR_THRESHOLD) or   # 首行非 1.0
                    (shift_val is not None and shift_val > _HK_FACTOR_THRESHOLD)             # 因子突变
                )

                if is_event_day:
                    if prev_cum is not None and prev_cum != 0:
                        day_factor = round(cum_val / prev_cum, 6)
                    else:
                        day_factor = round(cum_val, 6)

                    factors.append({
                        'instrument_id': instrument_id,
                        'ex_date': dt.to_pydatetime(),
                        'factor': day_factor,
                        'cumulative_factor': round(cum_val, 6),
                        'source': 'akshare',
                    })

                prev_cum = cum_val

            akshare_logger.info(
                f"[{self.name}] HK factors: {len(factors)} events for {symbol} "
                f"({start_str}-{end_str})"
            )
            return factors

        except Exception as e:
            akshare_logger.error(
                f"[{self.name}] Failed to get HK adjustment factors for {symbol}: {e}"
            )
            return []

    async def close(self):
        """关闭AkShare数据源连接和资源"""
        if self.aio_session:
            await self.aio_session.close()
            self.aio_session = None
        await super().close()