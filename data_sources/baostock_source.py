"""
Baostock data source implementation.
Specialized for Chinese stock market historical data.
"""

import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date
import pandas as pd
import baostock as bs

from .base_source import BaseDataSource, RateLimitConfig
from .adjustment_config import AdjustmentConfig
from utils.exceptions import DataSourceError, NetworkError, ErrorCodes
from utils import log_execution, data_source_metrics
from utils import baostock_logger


class BaostockSource(BaseDataSource):
    """Baostock数据源 - 专注中国股市历史数据"""

    def __init__(self, name: str, rate_limit_config: RateLimitConfig = None):
        super().__init__(name, rate_limit_config)
        self.supported_exchanges = ['SSE', 'SZSE']  # 上海证券交易所、深圳证券交易所
        self.is_logged_in = False
        self.network_error_count = 0
        self.max_network_errors = 10  # 最大网络错误次数

    @log_execution("Baostock", "initialize")
    async def _initialize_impl(self):
        """初始化Baostock数据源"""
        try:
            baostock_logger.info("Initializing Baostock data source")

            # 登录Baostock
            lg = await asyncio.to_thread(bs.login)
            if lg.error_code != '0':
                baostock_logger.error(f"Baostock login failed: {lg.error_msg}")
                raise DataSourceError(
                    f"Baostock login failed: {lg.error_msg}",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                )

            self.is_logged_in = True
            data_source_metrics.increment("connection_success")
            baostock_logger.info("Baostock connection successful")

        except Exception as e:
            data_source_metrics.increment("initialization_failure")
            if isinstance(e, (ConnectionError, TimeoutError)):
                baostock_logger.error(f"Network error during Baostock initialization: {str(e)}")
                raise NetworkError(
                    f"Failed to initialize Baostock connection: {str(e)}",
                    ErrorCodes.NETWORK_CONNECTION_ERROR
                ) from e
            else:
                baostock_logger.error(f"Error during Baostock initialization: {str(e)}")
                raise DataSourceError(
                    f"Failed to initialize Baostock: {str(e)}",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                ) from e

    # BaoStock type 字段映射: 1=股票, 2=指数, 4=可转债, 5=ETF/基金
    # 注意: BaoStock query_history_k_data_plus 不支持 ETF 日线数据查询
    BAOSTOCK_TYPE_MAP: Dict[str, str] = {'1': 'stock', '2': 'index', '5': 'etf'}

    async def get_instrument_list(self, exchange: str = None,
                                  instrument_types: List[str] = None) -> List[Dict[str, Any]]:
        """获取交易品种列表

        Args:
            exchange: 交易所代码 (SSE/SZSE)
            instrument_types: 要获取的品种类型列表, 如 ['stock', 'index', 'etf']
                              为 None 时仅获取 stock（保持向后兼容）
        """
        # 默认仅获取股票，保持向后兼容
        if instrument_types is None:
            instrument_types = ['stock']

        try:
            await self.rate_limiter.acquire()

            # 获取品种列表（包含上市日期）
            rs = await asyncio.to_thread(
                bs.query_stock_basic
            )

            if rs.error_code != '0':
                baostock_logger.error(f"Failed to query stock list: {rs.error_msg}")
                return []

            # 转换为DataFrame
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            df = pd.DataFrame(data_list, columns=rs.fields)

            if df.empty:
                baostock_logger.warning(f"No instruments found for {exchange}")
                return []

            instruments = []
            for _, row in df.iterrows():
                # BaoStock type 映射
                bs_type = row['type']
                mapped_type = self.BAOSTOCK_TYPE_MAP.get(bs_type)

                # 跳过不支持的类型或未启用的类型
                if not mapped_type or mapped_type not in instrument_types:
                    continue

                # 股票需要检查活跃状态；指数和 ETF 不适用此过滤
                if mapped_type == 'stock' and row['status'] != '1':
                    continue

                # 根据交易所过滤数据
                code = row['code']
                if exchange == 'SSE' and not code.startswith('sh.'):
                    continue
                elif exchange == 'SZSE' and not code.startswith('sz.'):
                    continue
                elif exchange == 'BSE' and not code.startswith('bj.'):
                    continue

                # Baostock的代码格式转换
                if exchange == 'SSE':
                    symbol = code.replace('sh.', '')
                    ts_code = f"{symbol}.SH"
                else:
                    symbol = code.replace('sz.', '')
                    ts_code = f"{symbol}.SZ"

                # 处理上市日期
                listed_date = None
                if pd.notna(row['ipoDate']) and row['ipoDate']:
                    try:
                        listed_date = pd.to_datetime(row['ipoDate']).to_pydatetime()
                    except Exception as e:
                        baostock_logger.warning(f"Failed to parse ipoDate {row['ipoDate']} for {code}: {e}")

                # 处理退市日期
                delisted_date = None
                if pd.notna(row['outDate']) and row['outDate']:
                    try:
                        delisted_date = pd.to_datetime(row['outDate']).to_pydatetime()
                    except Exception as e:
                        baostock_logger.warning(f"Failed to parse outDate {row['outDate']} for {code}: {e}")

                # 判断是否为 ST 股（仅对股票有意义）
                name = row['code_name']
                is_st = mapped_type == 'stock' and ('ST' in name.upper() or '*ST' in name.upper())

                instrument = {
                    'instrument_id': ts_code,
                    'symbol': symbol,
                    'name': name,
                    'exchange': exchange,
                    'type': mapped_type,
                    'currency': 'CNY',
                    'listed_date': listed_date,
                    'delisted_date': delisted_date,
                    'industry': row.get('industry', ''),
                    'sector': row.get('area', ''),  # BaoStock 的 area 映射为 sector
                    'market': row.get('market', ''),
                    'status': 'active',
                    'is_active': True,
                    'is_st': is_st,
                    'trading_status': 1,  # 默认为正常交易状态
                    'source': 'baostock',
                    'source_symbol': code,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now(),
                    'data_version': 1
                }
                instruments.append(instrument)

            # 按类型统计日志
            type_counts = {}
            for inst in instruments:
                t = inst['type']
                type_counts[t] = type_counts.get(t, 0) + 1
            baostock_logger.info(f"Retrieved {len(instruments)} instruments from Baostock for {exchange}: {type_counts}")
            return instruments

        except Exception as e:
            baostock_logger.error(f"Failed to get instrument list from Baostock: {e}")
            return []

    async def get_daily_data(self, instrument_id: str, symbol: str,
                           start_date: datetime, end_date: datetime,
                           instrument_type: str = 'stock') -> List[Dict[str, Any]]:
        """获取历史日线数据"""
        try:
            await self.rate_limiter.acquire()

            # 确保登录状态
            await self._ensure_login()

            # 重置网络错误计数器（成功请求）
            self.network_error_count = 0

            # 转换为Baostock代码格式
            if 'SH' in instrument_id or 'SSE' in instrument_id:
                baostock_code = f"sh.{symbol}"
            elif 'SZ' in instrument_id:
                baostock_code = f"sz.{symbol}"
            else:
                baostock_logger.error(f"Cannot parse instrument_id: {instrument_id}")
                return []

            # 转换日期格式
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')

            baostock_logger.debug(f"[BaoStock] Querying {baostock_code} from {start_str} to {end_str}")
            
            # 为阻塞调用添加超时和重试逻辑
            max_retries = self.rate_limiter.config.retry_times
            base_delay = self.rate_limiter.config.retry_interval
            rs = None

            relogin_attempted = False
            for attempt in range(max_retries):
                try:
                    # 获取日线数据 - 使用统一的前复权配置
                    adjustment_config = AdjustmentConfig.get_adjustment_config('baostock')
                    
                    rs = await asyncio.wait_for(
                        asyncio.to_thread(
                            bs.query_history_k_data_plus,
                            baostock_code,
                            start_date=start_str,
                            end_date=end_str,
                            frequency="d",
                            adjustflag=adjustment_config['adjustflag'],
                            fields="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
                        ),
                        timeout=120.0  # 设置120秒（2分钟）超时
                    )
                    # 如果成功，跳出循环
                    break
                except asyncio.TimeoutError:
                    baostock_logger.warning(f"Timeout fetching data for {baostock_code} (attempt {attempt + 1}/{max_retries})")
                    self.network_error_count += 1
                    if not relogin_attempted:
                        relogin_attempted = True
                        try:
                            baostock_logger.warning("Timeout detected, attempting BaoStock re-login")
                            await self._relogin()
                            self.network_error_count = 0
                        except Exception as relogin_error:
                            baostock_logger.error(f"BaoStock re-login after timeout failed: {relogin_error}")
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # 指数退避
                        baostock_logger.info(f"Retrying in {delay:.1f} seconds...")
                        await asyncio.sleep(delay)
                    else:
                        baostock_logger.error(f"All retry attempts failed for {baostock_code} due to timeout.")
                        raise  # 最终抛出超时异常
                except Exception as inner_e:
                    # 捕获类似 Broken pipe 等 Socket 异常
                    err_msg = str(inner_e).lower()
                    if any(kw in err_msg for kw in ["broken pipe", "pipe", "reset", "abort", "网络", "connection", "timeout", "eof"]):
                        baostock_logger.warning(f"Network error during fetch {baostock_code} (attempt {attempt + 1}/{max_retries}): {inner_e}")
                        self.network_error_count += 1
                        if not relogin_attempted:
                            relogin_attempted = True
                            try:
                                baostock_logger.warning("Network drop detected, attempting BaoStock re-login")
                                await self._relogin()
                                self.network_error_count = 0
                            except Exception as relogin_error:
                                baostock_logger.error(f"BaoStock re-login failed: {relogin_error}")
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)
                            baostock_logger.info(f"Retrying inner exception in {delay:.1f} seconds...")
                            await asyncio.sleep(delay)
                            continue
                    # 对于非网络异常直接向外抛
                    raise

            if rs is None or rs.error_code != '0':
                error_msg = rs.error_msg if rs else "rs is None"
                baostock_logger.error(f"Failed to query daily data: {error_msg}")

                # 特殊处理"用户未登录"或"网络"错误
                if rs and ("用户未登录" in rs.error_msg or "网络" in rs.error_msg):
                    baostock_logger.warning("BaoStock session expired or network error returned, marking as logged out")
                    self.is_logged_in = False

                    # 尝试重新登录并重试一次
                    try:
                        await self._relogin()
                        baostock_logger.info(f"Retrying {baostock_code} after re-login")

                        # 重新查询
                        rs = await asyncio.to_thread(
                            bs.query_history_k_data_plus,
                            baostock_code,
                            start_date=start_str,
                            end_date=end_str,
                            frequency="d",
                            adjustflag=adjustment_config['adjustflag'],
                            fields="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST"
                        )

                        if rs.error_code != '0':
                            baostock_logger.error(f"Retry failed: {rs.error_msg}")
                            return []
                        else:
                            baostock_logger.info(f"Retry successful for {baostock_code}")
                    except Exception as retry_error:
                        baostock_logger.error(f"Retry attempt failed: {retry_error}")
                        return []

                return []

            # 转换为DataFrame
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            df = pd.DataFrame(data_list, columns=rs.fields)

            if df.empty:
                baostock_logger.info(f"No data found for {instrument_id} from {start_str} to {end_str}")
                return []

            # 转换数据格式
            adjustment_config = AdjustmentConfig.get_adjustment_config('baostock')
            quotes = []
            for _, row in df.iterrows():
                quote = {
                    # 基础交易数据
                    'time': pd.to_datetime(row['date']),
                    'instrument_id': instrument_id,
                    'open': float(row['open']) if row['open'] else 0.0,
                    'high': float(row['high']) if row['high'] else 0.0,
                    'low': float(row['low']) if row['low'] else 0.0,
                    'close': float(row['close']) if row['close'] else 0.0,
                    'volume': int(float(row['volume'])) if row['volume'] and float(row['volume']) > 0 else 0,
                    'amount': float(row['amount']) if row['amount'] and float(row['amount']) > 0 else 0.0,

                    # BaoStock 直接提供的扩展字段
                    'pre_close': float(row['preclose']) if row['preclose'] else 0.0,
                    'turnover': float(row['turn']) if row['turn'] else 0.0,
                    'tradestatus': int(row['tradestatus']) if row['tradestatus'] else 1,
                    'pct_change': float(row['pctChg']) if row['pctChg'] else 0.0,

                    # 复权信息
                    'factor': adjustment_config['factor'],
                    'adjustment_type': adjustment_config.get('type', 'forward'),
                    'source': 'baostock'
                }
                quotes.append(quote)

            baostock_logger.info(f"Retrieved {len(quotes)} daily quotes for {instrument_id} ({start_str} to {end_str})")
            return quotes

        except Exception as e:
            # 网络错误处理
            if any(keyword in str(e).lower() for keyword in ["网络", "decode", "decompressing", "connection", "timeout", "urlopen", "broken pipe", "pipe", "reset", "abort", "eof"]):
                self.network_error_count += 1
                baostock_logger.warning(f"Network error #{self.network_error_count} for {instrument_id}: {e}")

                # 如果网络错误次数过多，增加等待时间
                if self.network_error_count >= 3:
                    wait_time = min(self.network_error_count * 5, 30)  # 最多等待30秒
                    baostock_logger.info(f"Waiting {wait_time}s due to consecutive network errors...")
                    await asyncio.sleep(wait_time)

                # 如果网络错误次数过多，强制重新连接
                if self.network_error_count >= self.max_network_errors:
                    baostock_logger.warning("Too many network errors, forcing reconnection...")
                    self.is_logged_in = False
                    try:
                        await self._relogin()
                        self.network_error_count = 0
                    except Exception as reconnect_error:
                        baostock_logger.error(f"Failed to reconnect: {reconnect_error}")
                        return []
            else:
                baostock_logger.error(f"Failed to get daily data from Baostock for {instrument_id}: {e}")
            return []

    
    async def _ensure_login(self):
        """确保登录状态，如果未登录则重新登录"""
        # 如果已经标记为已登录，且最近有成功操作，则跳过检查
        if self.is_logged_in:
            return

        # 如果标记为未登录，执行重新登录
        if not self.is_logged_in:
            baostock_logger.warning("BaoStock session expired, re-logging in...")
            await self._relogin()

    async def _relogin(self):
        """重新登录BaoStock"""
        try:
            # 先尝试登出（如果还有连接）
            if self.is_logged_in:
                try:
                    await asyncio.to_thread(bs.logout)
                except:
                    pass  # 忽略登出错误
                self.is_logged_in = False

            # 重新登录
            lg = await asyncio.to_thread(bs.login)
            if lg.error_code != '0':
                baostock_logger.error(f"BaoStock re-login failed: {lg.error_msg}")
                raise DataSourceError(
                    f"BaoStock re-login failed: {lg.error_msg}",
                    ErrorCodes.DATASOURCE_CONNECTION_FAILED
                )

            self.is_logged_in = True
            baostock_logger.info("BaoStock re-login successful")

        except Exception as e:
            baostock_logger.error(f"Error during BaoStock re-login: {e}")
            raise DataSourceError(
                f"Failed to re-login BaoStock: {e}",
                ErrorCodes.DATASOURCE_CONNECTION_FAILED
            ) from e

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            if not self.is_logged_in:
                return False

            await self.rate_limiter.acquire()

            # 测试获取一天的数据
            rs = await asyncio.to_thread(
                bs.query_history_k_data_plus,
                "sh.600000",
                start_date='2024-01-01',
                end_date='2024-01-01',
                frequency="d",
                fields="date,code,open,high,low,close,preclose,volume,amount"
            )

            return rs.error_code == '0'

        except Exception as e:
            baostock_logger.error(f"Baostock health check failed: {e}")
            return False

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Optional[Dict[str, Any]]:
        """获取最新日线数据"""
        try:
            await self.rate_limiter.acquire()

            # 转换为Baostock代码格式
            if 'SH' in instrument_id or 'SSE' in instrument_id:
                baostock_code = f"sh.{symbol}"
            elif 'SZ' in instrument_id:
                baostock_code = f"sz.{symbol}"
            else:
                baostock_logger.error(f"Cannot parse instrument_id: {instrument_id}")
                return None

            # 获取最近5个交易日的数据
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

            rs = await asyncio.to_thread(
                bs.query_history_k_data_plus,
                baostock_code,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                fields="date,code,open,high,low,close,preclose,volume,amount"
            )

            if rs.error_code != '0':
                baostock_logger.error(f"Failed to query latest data: {rs.error_msg}")
                return None

            # 获取复权配置
            adjustment_config = AdjustmentConfig.get_adjustment_config('baostock')

            # 转换为DataFrame并获取最新记录
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return None

            # 获取最新一条记录
            latest_record = data_list[-1]

            return {
                'time': pd.to_datetime(latest_record[0]),
                'instrument_id': instrument_id,
                'open': float(latest_record[2]) if latest_record[2] else 0.0,
                'high': float(latest_record[3]) if latest_record[3] else 0.0,
                'low': float(latest_record[4]) if latest_record[4] else 0.0,
                'close': float(latest_record[5]) if latest_record[5] else 0.0,
                'volume': int(float(latest_record[7])) if latest_record[7] and float(latest_record[7]) > 0 else 0,
                'amount': float(latest_record[8]) if latest_record[8] and float(latest_record[8]) > 0 else 0.0,
                'factor': adjustment_config['factor'],
                'adjustment_type': adjustment_config.get('type', 'forward')
            }

        except Exception as e:
            baostock_logger.error(f"Failed to get latest daily data from Baostock for {instrument_id}: {e}")
            return None

    async def get_trading_calendar(self, exchange: str, start_date: date,
                                  end_date: date) -> List[Dict[str, Any]]:
        """获取交易日历（BaoStock交易字典）"""
        try:
            await self.rate_limiter.acquire()
            await self._ensure_login()

            baostock_logger.info(f"Getting trading calendar for {exchange} from {start_date} to {end_date}")

            # BaoStock使用query_trade_dates接口获取交易日历
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')

            # 获取交易日历数据
            rs = await asyncio.to_thread(
                bs.query_trade_dates,
                start_date=start_str,
                end_date=end_str
            )

            if rs.error_code != '0':
                baostock_logger.error(f"Failed to query trading calendar: {rs.error_msg}")
                # 如果错误是登录相关，更新登录状态
                if '用户未登录' in rs.error_msg or 'not login' in rs.error_msg.lower():
                    baostock_logger.warning("BaoStock login status lost, updating session state")
                    self.is_logged_in = False
                return []

            # 转换为DataFrame
            calendar_data = []
            while (rs.error_code == '0') & rs.next():
                calendar_data.append(rs.get_row_data())

            if not calendar_data:
                baostock_logger.warning(f"No trading calendar data found for {exchange}")
                return []

            df = pd.DataFrame(calendar_data, columns=rs.fields)

            # 转换为标准格式
            trading_calendar = []
            for _, row in df.iterrows():
                trade_date = pd.to_datetime(row['calendar_date'])  # 保持datetime类型，不转换为date
                is_trading = row['is_trading_day'] == '1'

                calendar_entry = {
                    'exchange': exchange,
                    'date': trade_date,
                    'is_trading_day': is_trading,
                    'reason': None if is_trading else 'Non-trading day',
                    'session_type': 'full_day' if is_trading else None,
                    'source': 'baostock',
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                trading_calendar.append(calendar_entry)

            baostock_logger.info(f"Retrieved {len(trading_calendar)} trading calendar entries from Baostock for {exchange}")
            return trading_calendar

        except Exception as e:
            baostock_logger.error(f"Failed to get trading calendar from Baostock for {exchange}: {e}")
            return []

    async def close(self):
        """关闭数据源连接"""
        if self.is_logged_in:
            try:
                await asyncio.to_thread(bs.logout)
                self.is_logged_in = False
                baostock_logger.info("Baostock logged out successfully")
            except Exception as e:
                baostock_logger.error(f"Error during Baostock logout: {e}")
