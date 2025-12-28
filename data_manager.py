"""
Data Manager for the quote system.
Provides high-level data management operations with comprehensive features including
trading calendar integration, data quality assessment, and gap detection.
"""

from __future__ import annotations
import asyncio
import json
import os
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field

import pandas as pd

from utils import dm_logger, config_manager, log_execution, LogContext, TelegramBot
# 直接导入代码转换工具，避免依赖问题
from utils.code_utils import convert_to_database_format
from database.operations import DatabaseOperations
from database.models import Instrument, DailyQuote, TradingCalendar, DataUpdateInfo
# Note: get_data_source_factory will be imported dynamically to avoid circular import
from utils.date_utils import DateUtils, get_shanghai_time
from utils.validation import DataValidator
from utils.cache import cache_manager


@dataclass
class DownloadProgress:
    """标准下载进度跟踪"""
    total_instruments: int = 0
    processed_instruments: int = 0
    successful_downloads: int = 0
    failed_downloads: int = 0
    total_quotes: int = 0
    start_time: datetime = field(default_factory=get_shanghai_time)
    current_exchange: str = ""
    current_batch: int = 0
    total_batches: int = 0
    errors: List[str] = field(default_factory=list)
    trading_days_processed: int = 0
    total_trading_days: int = 0
    data_gaps_detected: int = 0
    quality_issues: int = 0
    batch_id: str = field(default_factory=lambda: datetime.now().strftime('%Y%m%d_%H%M%S'))

    def get_progress_percentage(self) -> float:
        """获取进度百分比"""
        if self.total_instruments == 0:
            return 0.0
        return (self.processed_instruments / self.total_instruments) * 100

    def get_elapsed_time(self) -> timedelta:
        """获取已用时间"""
        return get_shanghai_time() - self.start_time

    def get_success_rate(self) -> float:
        """获取成功率"""
        if self.processed_instruments == 0:
            return 0.0
        return (self.successful_downloads / self.processed_instruments) * 100

    def get_data_quality_score(self) -> float:
        """获取数据质量评分"""
        if self.total_quotes == 0:
            return 0.0
        quality_score = max(0, 100 - (self.quality_issues / self.total_quotes * 100))
        return min(100, quality_score)

    def add_error(self, error: str):
        """添加错误信息"""
        self.errors.append(f"[{get_shanghai_time().strftime('%H:%M:%S')}] {error}")
        if len(self.errors) > 100:  # 限制错误记录数量
            self.errors.pop(0)


@dataclass
class DataGapInfo:
    """数据缺口信息"""
    instrument_id: str
    symbol: str
    exchange: str
    gap_start: date
    gap_end: date
    gap_days: int
    gap_type: str  # 'missing_data', 'trading_suspension', 'quality_issue'
    severity: str  # 'low', 'medium', 'high', 'critical'
    recommendation: str
    missing_dates: List['date'] = field(default_factory=list)  # 具体的缺失日期列表


class DataManager:
    """标准数据管理器"""

    def __init__(self):
        self.config = config_manager
        self.telegram_enabled = self.config.get_nested('telegram_config.enabled', False)
        self.data_config = self.config.get_nested('data_config', {})
        self.download_chunk_days = self.data_config.get('download_chunk_days', 7)
        self.progress_file = os.path.join(self.data_config.get('data_dir', 'data'), 'download_progress.json')
        self.progress: DownloadProgress = DownloadProgress()
        self.is_running = False

        # 使用统一的数据库操作实例（不重复初始化）
        from database import db_ops
        self.db_ops = db_ops
        self.source_factory = None

    @log_execution("DataManager", "initialize")
    async def initialize(self) -> None:
        """初始化数据管理器"""
        try:
            dm_logger.info("Initializing DataManager components...")

            # 确保数据目录存在
            data_dir = self.data_config.get('data_dir', 'data')
            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(os.path.join(data_dir, 'backups'), exist_ok=True)
            os.makedirs(os.path.join(data_dir, 'reports'), exist_ok=True)

            # db_ops 已经在模块级别初始化，无需重复初始化
            if not self.db_ops.SessionLocal:
                dm_logger.warning("db_ops not initialized, this should not happen!")
                await self.db_ops.initialize()

            # 初始化数据源工厂
            from data_sources.source_factory import get_data_source_factory
            self.source_factory = await get_data_source_factory(self.db_ops)

            # 加载之前的进度
            await self._load_progress()

            dm_logger.info("DataManager initialized successfully")

        except Exception as e:
            dm_logger.error(f"Failed to initialize DataManager: {e}")
            raise

    @log_execution("DataManager", "download_all_historical_data")
    async def download_all_historical_data(self, exchanges: Optional[List[str]] = None,
                                         start_date: Optional[date] = None, end_date: Optional[date] = None,
                                         resume: bool = True,
                                         quality_threshold: float = 0.7,
                                         force_update_calendar: bool = True) -> None:
        """下载所有历史数据

        Args:
            exchanges: 交易所列表
            start_date: 开始日期（如果为None，使用每个股票的上市日期）
            end_date: 结束日期（如果为None，使用昨天）
            resume: 是否续传
            quality_threshold: 数据质量阈值
        """
        if self.is_running:
            dm_logger.warning("download already in progress")
            return

        self.is_running = True

        # 处理续传逻辑
        has_existing_progress = (self.progress.processed_instruments > 0 and
                               self.progress.total_instruments > 0)

        if resume and has_existing_progress:
            dm_logger.info(f"Resume mode: {self.progress.processed_instruments}/{self.progress.total_instruments} instruments already processed")
        else:
            if has_existing_progress:
                dm_logger.warning(f"Reset mode: Found existing progress ({self.progress.processed_instruments}/{self.progress.total_instruments} instruments)")
            self.progress = DownloadProgress()

        try:
            dm_logger.info("Starting historical data download process")

            # 默认下载支持的交易所
            if exchanges is None:
                exchanges = ['SSE', 'SZSE']
                dm_logger.info(f"Using default exchanges: {exchanges}")

            # 获取所有交易品种并计算总数
            all_instruments = {}
            if not (resume and has_existing_progress):
                self.progress.total_instruments = 0
                for exchange in exchanges:
                    instruments = await self.db_ops.get_instruments_by_exchange(exchange)
                    all_instruments[exchange] = instruments
                    self.progress.total_instruments += len(instruments)
            await self._save_progress()

            # 设置结束日期
            if end_date is None:
                end_date = date.today() - timedelta(days=1)

            # 预处理：更新交易日历（仅在需要时）
            if force_update_calendar:
                dm_logger.info("Updating trading calendar from data source...")
                for exchange in exchanges:
                    await self._update_trading_calendar(exchange, start_date, end_date)
            else:
                dm_logger.info("Using cached trading calendar (no force update)")

            # 下载数据
            for exchange in exchanges:
                with LogContext("DataManager", "download_exchange", exchange=exchange):
                    # 如果在续传模式，需要重新获取instruments
                    if exchange not in all_instruments:
                        all_instruments[exchange] = await self.db_ops.get_instruments_by_exchange(exchange)

                    # 统一使用精确下载模式
                    await self._download_exchange_precise(
                        exchange, all_instruments[exchange], start_date, end_date, quality_threshold, resume
                    )

            # 后处理：检测数据缺口和质量问题
            await self._post_download_analysis(exchanges, start_date, end_date)

            # 生成详细的下载报告
            dm_logger.info("[DataManager] Generating detailed download completion report...")
            download_report = await self._generate_download_report(exchanges)

            # 保存报告到文件（可选）
            try:
                import os
                reports_dir = "data/reports"
                os.makedirs(reports_dir, exist_ok=True)
                report_file = os.path.join(reports_dir, f"download_report_{self.progress.batch_id}.json")

                import json
                # 转换datetime对象为字符串以便JSON序列化
                def json_serializer(obj):
                    if hasattr(obj, 'isoformat'):
                        return obj.isoformat()
                    elif hasattr(obj, 'total_seconds'):
                        return {'total_seconds': obj.total_seconds(), 'hours': obj.total_seconds() // 3600, 'minutes': (obj.total_seconds() % 3600) // 60}
                    return str(obj)

                with open(report_file, 'w', encoding='utf-8') as f:
                    json.dump(download_report, f, ensure_ascii=False, indent=2, default=json_serializer)

                dm_logger.info(f"[DataManager] Download report saved to: {report_file}")
            except Exception as e:
                dm_logger.warning(f"[DataManager] Failed to save download report: {e}")

        except Exception as e:
            dm_logger.error(f"historical download failed: {e}")
        finally:
            self.is_running = False
            await self._save_progress()  # 确保在任务结束时保存进度

    async def _update_trading_calendar(self, exchange: str, start_date: date, end_date: date):
        """更新交易日历"""
        try:
            dm_logger.info(f"Updating trading calendar for {exchange}")

            # 计算需要更新的日期范围
            calendar_start = start_date or date(1990, 12, 19)
            calendar_end = end_date or date.today()

            # 从数据源获取交易日历
            updated_count = await self.source_factory.update_trading_calendar(
                exchange, calendar_start, calendar_end
            )

            dm_logger.info(f"Updated {updated_count} trading days for {exchange}")
            return updated_count

        except Exception as e:
            dm_logger.warning(f"Failed to update trading calendar for {exchange}: {e}")
            return 0

    async def _get_exchange_processed_count(self, exchange: str) -> int:
        """获取指定交易所已处理的股票数量"""
        try:
            # 查询该交易所有数据的股票数量
            query = """
            SELECT COUNT(DISTINCT dq.instrument_id) as processed_count
            FROM daily_quotes dq
            JOIN instruments i ON dq.instrument_id = i.instrument_id
            WHERE i.exchange = ? AND i.is_active = 1
            """
            result = await self.db_ops.execute_query(query, (exchange,))
            return result[0]['processed_count'] if result else 0
        except Exception as e:
            dm_logger.warning(f"Failed to get processed count for {exchange}: {e}")
            return 0

    async def _download_exchange_precise(self, exchange: str, instruments: List[Dict],
                                                start_date: date, end_date: date,
                                                quality_threshold: float, resume: bool = False):
        """标准精确下载（基于上市日期和交易日历）"""
        try:
            self.progress.current_exchange = exchange
            dm_logger.info(f"Starting precise download for exchange: {exchange}")

            if not instruments:
                dm_logger.warning(f"No instruments found for {exchange}")
                return

            dm_logger.info(f"Found {len(instruments)} instruments for {exchange}")

            # 分批处理
            batch_size = self.data_config.get('batch_size', 50)
            batches = [instruments[i:i + batch_size] for i in range(0, len(instruments), batch_size)]

            # 修复的续传逻辑 - 按交易所独立跟踪进度
            if resume:
                # 检查当前交易所是否已经有处理记录
                exchange_processed = await self._get_exchange_processed_count(exchange)
                dm_logger.info(f"Resume mode: {exchange} - {exchange_processed} instruments already processed")
            else:
                exchange_processed = 0
                dm_logger.info(f"Fresh download mode: {exchange}")

            processed_batches = (exchange_processed // batch_size)
            start_batch = processed_batches + 1

            self.progress.total_batches = len(batches)
            dm_logger.info(f"Processing {len(batches)} batches for {exchange}, resuming from batch {start_batch}")

            for batch_idx, batch in enumerate(batches, 1):
                if batch_idx < start_batch:
                    continue

                with LogContext("DataManager", "process_precise_batch",
                               exchange=exchange, batch_idx=batch_idx):

                    self.progress.current_batch = batch_idx
                    dm_logger.info(f"Processing precise batch {batch_idx}/{len(batches)}")

                    await self._download_batch_precise(
                        batch, exchange, start_date, end_date, quality_threshold
                    )

                    # 批次间延迟
                    if batch_idx < len(batches):
                        await asyncio.sleep(2.0)

        except Exception as e:
            dm_logger.error(f"Failed to download precise data: {e}")
            self.progress.add_error(f"Exchange {exchange}: {str(e)}")

    async def _download_batch_precise(self, instruments: List[Dict], exchange: str,
                                            start_date: date, end_date: date,
                                            quality_threshold: float):
        """标准精确批次下载"""
        batch_data = []
        batch_quality_scores = []

        for instrument in instruments:
            try:
                # 获取股票的上市日期
                listed_date = instrument.get('listed_date')
                instrument_start = listed_date if listed_date else start_date

                instrument_start_date = instrument_start
                if isinstance(instrument_start_date, datetime):
                    instrument_start_date = instrument_start_date.date()

                if instrument_start_date and instrument_start_date > end_date:
                    continue

                # 获取交易日历 (统一转换为date类型)
                from utils.date_utils import normalize_date_range
                instrument_start_for_query, end_date_for_query = normalize_date_range(instrument_start, end_date)

                trading_days = await self.source_factory.get_trading_days(
                    exchange, instrument_start_for_query, end_date_for_query
                )

                if not trading_days:
                    dm_logger.warning(f"No trading days found for {instrument['instrument_id']}")
                    continue

                # 按交易日下载
                instrument_data = await self._download_instrument_by_trading_days(
                    instrument, exchange, trading_days, quality_threshold,
                    start_date, end_date
                )

                if instrument_data:
                    batch_data.extend(instrument_data)
                    batch_quality_scores.extend([d.get('quality_score', 1.0) for d in instrument_data])

            except Exception as e:
                dm_logger.error(f"Failed to download {instrument.get('instrument_id', 'unknown')}: {e}")

        # 保存数据
        if batch_data:
            success = await self.db_ops.save_daily_quotes(batch_data)
            if success:
                self.progress.successful_downloads += len(instruments)
                self.progress.total_quotes += len(batch_data)

                # 计算批次质量
                batch_quality = sum(batch_quality_scores) / len(batch_quality_scores) if batch_quality_scores else 0
                if batch_quality < quality_threshold:
                    self.progress.quality_issues += len(batch_data)

                dm_logger.info(f"Saved precise batch: {len(batch_data)} quotes, quality: {batch_quality:.2f}")
            else:
                self.progress.failed_downloads += len(instruments)
                self.progress.add_error(f"Failed to save precise batch for {exchange}")

        self.progress.processed_instruments += len(instruments)
        await self._save_progress()

    async def _download_instrument_by_trading_days(self, instrument: Dict, exchange: str,
                                                 trading_days: List[date],
                                                 quality_threshold: float, # 质量阈值
                                                 start_date: Optional[date] = None,
                                                 end_date: Optional[date] = None) -> List[Dict]:
        """按交易日下载单个股票的数据"""
        all_data = []

        # 检查是否为一次性下载模式
        if self.download_chunk_days == 0:
            # 一次性下载所有数据
            try:
                if not trading_days:
                    dm_logger.warning(f"No trading days available for {instrument['instrument_id']}")
                    return []

                # 使用传入的日期范围，而不是交易日历的边界
                # 这样确保只下载用户指定日期范围内的数据
                if trading_days:
                    # 过滤交易日，只保留在用户指定范围内的
                    filtered_trading_days = []
                    for day in trading_days:
                        # 检查 start_date 和 end_date 是否为 None
                        if (start_date is None or start_date <= day) and \
                           (end_date is None or day <= end_date):
                            filtered_trading_days.append(day)

                    if not filtered_trading_days:
                        dm_logger.info(f"No trading days in specified date range for {instrument['instrument_id']}")
                        return []

                    # 使用实际交易日的范围进行下载
                    actual_start_date = filtered_trading_days[0]
                    actual_end_date = filtered_trading_days[-1]

                    start_date_for_download = datetime.combine(actual_start_date, datetime.min.time())
                    end_date_for_download = datetime.combine(actual_end_date, datetime.max.time())

                    dm_logger.info(f"一次性下载 {instrument['instrument_id']} 数据 "
                                  f"从 {start_date_for_download.date()} 到 {end_date_for_download.date()} "
                                  f"(共 {len(filtered_trading_days)} 个交易日)")

                    # 更新trading_days为过滤后的列表，用于后续处理
                    trading_days = filtered_trading_days
                else:
                    dm_logger.warning(f"No trading days available for {instrument['instrument_id']}")
                    return []

                
                # 获取所有数据
                all_data_response = await self.source_factory.get_daily_data(
                    exchange,
                    instrument['instrument_id'],
                    instrument['symbol'],
                    start_date_for_download,
                    end_date_for_download
                )

                if all_data_response:
                    # 数据质量检查和标准化
                    improved_data = await self._improve_data_quality(
                        all_data_response, instrument, trading_days
                    )
                    all_data.extend(improved_data)
                    dm_logger.info(f"成功下载 {instrument['instrument_id']} {len(improved_data)} 条记录")
                else:
                    dm_logger.warning(f"未获取到 {instrument['instrument_id']} 的数据")

                return all_data

            except Exception as e:
                dm_logger.error(f"一次性下载 {instrument['instrument_id']} 数据失败: {e}")
                return []

        # 分段下载模式，将交易日按配置的chunk_days分组
        chunk_groups = []
        current_chunk = []

        for trading_day in trading_days:
            if not current_chunk or (trading_day - current_chunk[0]).days < self.download_chunk_days:
                current_chunk.append(trading_day)
            else:
                chunk_groups.append(current_chunk)
                current_chunk = [trading_day]

        if current_chunk:
            chunk_groups.append(current_chunk)

        dm_logger.info(f"分段下载 {instrument['instrument_id']} 数据，"
                      f"每段 {self.download_chunk_days} 天，共 {len(chunk_groups)} 段")

        for i, trunk_days in enumerate(chunk_groups, 1):
            try:
                start_date = datetime.combine(trunk_days[0], datetime.min.time())
                end_date = datetime.combine(trunk_days[-1], datetime.max.time())

                dm_logger.debug(f"下载第 {i}/{len(chunk_groups)} 段 "
                              f"{instrument['instrument_id']} 数据 "
                              f"从 {start_date.date()} 到 {end_date.date()}")

                # 获取这一段的数据
                trunk_data = await self.source_factory.get_daily_data(
                    exchange,
                    instrument['instrument_id'],
                    instrument['symbol'],
                    start_date,
                    end_date
                )

                if trunk_data:
                    # 数据质量检查和标准化
                    improved_data = await self._improve_data_quality(
                        trunk_data, instrument, trading_days
                    )
                    all_data.extend(improved_data)
                    dm_logger.debug(f"第 {i} 段获取到 {len(improved_data)} 条记录")
                else:
                    dm_logger.warning(f"第 {i} 段未获取到数据")

                # API限流延迟
                await asyncio.sleep(0.5)

            except Exception as e:
                dm_logger.error(f"下载第 {i} 段 {instrument['instrument_id']} 数据失败: {e}")

        return all_data

    async def _improve_data_quality(self, data: List[Dict], instrument: Dict,
                                  trading_days: List[date]) -> List[Dict]:
        """标准数据质量处理和衍生字段计算"""
        improved_data = []

        # 按时间排序以便正确计算衍生字段
        sorted_data = sorted(data, key=lambda x: x['time'])

        for i, quote in enumerate(sorted_data):
            try:
                # 基本数据验证
                if self._validate_quote_data(quote):
                    # 确保基本字段
                    quote['instrument_id'] = instrument['instrument_id']
                    quote['batch_id'] = self.progress.batch_id

                    # 计算衍生字段
                    self._calculate_derived_fields(quote, i, sorted_data, instrument)

                    # 数据质量评分和完整性检查
                    quote['is_complete'] = self._check_data_completeness(quote)
                    quote['quality_score'] = self._calculate_quality_score(quote, instrument)

                    # 检查是否为交易日（仅用于质量控制）
                    time_val = quote.get('time')
                    if isinstance(time_val, datetime):
                        quote_date = time_val.date()
                    elif isinstance(time_val, date):
                        quote_date = time_val
                    else:
                        dm_logger.warning(f"Invalid time type for {instrument.get('instrument_id')}: {type(time_val)}")
                        continue # 跳过此条记录

                    is_trading_day = quote_date in trading_days
                    if not is_trading_day and quote.get('tradestatus', 1) == 1:
                        # 如果不是交易日但交易状态显示正常，标记为异常
                        quote['quality_score'] = max(0.0, quote['quality_score'] - 0.3)

                    improved_data.append(quote)
                else:
                    self.progress.quality_issues += 1

            except Exception as e:
                dm_logger.warning(f"Failed to improve quote data: {e}")
                self.progress.quality_issues += 1

        return improved_data

    def _calculate_derived_fields(self, quote: Dict, index: int, sorted_quotes: List[Dict], instrument: Dict):
        """计算衍生字段"""
        try:
            # 1. 计算涨跌额（如果没有前收盘价，使用前一天数据）
            if 'pre_close' not in quote or quote['pre_close'] is None or quote['pre_close'] <= 0:
                if index > 0:
                    quote['pre_close'] = sorted_quotes[index-1]['close']
                else:
                    quote['pre_close'] = quote['close']  # 第一天默认无变化

            # 计算涨跌额
            if quote['pre_close'] and quote['pre_close'] > 0:
                quote['change'] = round(quote['close'] - quote['pre_close'], 4)
            else:
                quote['change'] = 0.0

            # 2. 计算涨跌幅（如果BaoStock没有提供或不合理）
            if 'pct_change' not in quote or quote['pct_change'] is None:
                if quote['pre_close'] and quote['pre_close'] > 0:
                    quote['pct_change'] = round((quote['change'] / quote['pre_close']) * 100, 2)
                else:
                    quote['pct_change'] = 0.0

            # 3. 判断复权类型
            if 'factor' in quote:
                if quote['factor'] == 1.0:
                    quote['adjustment_type'] = 'none'
                elif quote['factor'] > 1.0:
                    quote['adjustment_type'] = 'forward'  # 前复权
                else:
                    quote['adjustment_type'] = 'backward'  # 后复权

        except Exception as e:
            dm_logger.warning(f"Error calculating derived fields: {e}")
            # 设置默认值
            quote.setdefault('change', 0.0)
            quote.setdefault('pct_change', 0.0)

    def _check_data_completeness(self, quote: Dict) -> bool:
        """检查数据完整性"""
        try:
            # 检查必需字段
            required_fields = ['open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in quote or quote[field] is None or quote[field] <= 0:
                    return False

            # 检查价格逻辑
            if not (quote['high'] >= quote['low'] >= 0):
                return False
            if not (quote['high'] >= quote['open'] >= quote['low']):
                return False
            if not (quote['high'] >= quote['close'] >= quote['low']):
                return False

            # 检查成交量合理性
            if quote['volume'] < 0:
                return False

            # 检查成交额
            if 'amount' in quote and quote['amount'] < 0:
                return False

            return True

        except Exception as e:
            dm_logger.warning(f"Error checking data completeness: {e}")
            return False

  
    def _validate_quote_data(self, quote: Dict) -> bool:
        """验证行情数据"""
        try:
            # 检查必需字段
            required_fields = ['time', 'open', 'high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in quote or quote[field] is None:
                    return False

            # 检查价格合理性
            prices = [float(quote.get('open', 0)), float(quote.get('high', 0)),
                     float(quote.get('low', 0)), float(quote.get('close', 0))]

            if any(p <= 0 for p in prices):
                return False

            # 检查价格逻辑
            high, low = float(quote['high']), float(quote['low'])
            if high < low:
                return False

            return True

        except (ValueError, TypeError):
            return False

    def _calculate_quality_score(self, quote: Dict, instrument: Dict) -> float:
        """计算数据质量评分"""
        score = 1.0

        try:
            # 价格一致性检查
            prices = {
                'open': float(quote['open']),
                'high': float(quote['high']),
                'low': float(quote['low']),
                'close': float(quote['close'])
            }

            # 检查高低价关系
            if prices['high'] < max(prices['open'], prices['close']):
                score -= 0.1
            if prices['low'] > min(prices['open'], prices['close']):
                score -= 0.1

            # 检查成交量
            volume = int(quote.get('volume', 0))
            if volume <= 0:
                score -= 0.2

            # 检查交易状态
            tradestatus = quote.get('tradestatus', 1)
            if tradestatus != 1:  # 非正常交易
                score -= 0.3

            # 检查数据完整性
            if not quote.get('is_complete', True):
                score -= 0.1

        except (ValueError, TypeError, KeyError):
            score = 0.0

        return max(0.0, score)

    async def _post_download_analysis(self, exchanges: List[str],
                                    start_date: date, end_date: date):
        """下载后分析"""
        try:
            dm_logger.info("Starting post-download analysis...")

            # 检测数据缺口
            gaps = await self.detect_data_gaps(exchanges, start_date, end_date)
            self.progress.data_gaps_detected = len(gaps)

            # 生成分析报告
            await self._generate_analysis_report(gaps)

            dm_logger.info(f"Post-download analysis completed. Found {len(gaps)} data gaps")

        except Exception as e:
            dm_logger.error(f"Post-download analysis failed: {e}")

    async def detect_data_gaps(self, exchanges: List[str],
                             start_date: date, end_date: date) -> List[DataGapInfo]:
        """检测数据缺口"""
        gaps = []

        for exchange in exchanges:
            try:
                # 获取交易所的所有活跃股票
                instruments = await self.db_ops.get_active_instruments(exchange)

                for instrument in instruments:
                    instrument_gaps = await self._detect_instrument_gaps(
                        instrument, start_date, end_date
                    )
                    gaps.extend(instrument_gaps)

            except Exception as e:
                dm_logger.error(f"Failed to detect gaps for {exchange}: {e}")

        return gaps

    async def _detect_instrument_gaps(self, instrument: Dict,
                                    start_date: date, end_date: date) -> List[DataGapInfo]:
        """检测单个股票的数据缺口"""
        gaps = []

        try:
            # 获取股票的上市日期
            listed_date = instrument.get('listed_date')
            if start_date is None:
                start_date = listed_date.date() if isinstance(listed_date, datetime) else listed_date
                dm_logger.debug(f"Start date not specified, using listed date {listed_date} to be gap statistic start date")
            elif listed_date:
                start_date = max(start_date, listed_date.date() if isinstance(listed_date, datetime) else listed_date)

            if start_date is None:
                # Cannot determine start date, so skip
                return []

            # 获取交易日历 (统一转换为date类型)
            from utils.date_utils import normalize_date_range
            start_date_for_query, end_date_for_query = normalize_date_range(start_date, end_date)

            trading_days = await self.source_factory.get_trading_days(
                instrument['exchange'], start_date_for_query, end_date_for_query
            )

            # 获取已有数据的日期
            existing_dates = await self.db_ops.get_existing_data_dates(
                instrument['instrument_id'], start_date, end_date
            )

            # 找出缺失的交易日
            missing_dates = set(trading_days) - set(existing_dates)

            if missing_dates:
                # 将连续的缺失日期合并为缺口，并返回对应的日期列表
                gap_ranges_with_dates = self._merge_consecutive_dates_with_details(sorted(missing_dates))

                for gap_start, gap_end, gap_missing_dates in gap_ranges_with_dates:
                    gap_days = (gap_end - gap_start).days + 1

                    gaps.append(DataGapInfo(
                        instrument_id=instrument['instrument_id'],
                        symbol=instrument['symbol'],
                        exchange=instrument['exchange'],
                        gap_start=gap_start,
                        gap_end=gap_end,
                        gap_days=gap_days,
                        gap_type='missing_data',
                        severity=self._assess_gap_severity(gap_days),
                        recommendation=self._get_gap_recommendation(gap_days),
                        missing_dates=gap_missing_dates
                    ))

        except Exception as e:
            dm_logger.error(f"Failed to detect gaps for {instrument['instrument_id']}: {e}")

        return gaps

    def _merge_consecutive_dates_with_details(self, dates: List[date]) -> List[Tuple[date, date, List[date]]]:
        """合并连续日期为范围，并返回对应的日期列表"""
        if not dates:
            return []

        ranges = []
        start = dates[0]
        end = dates[0]
        current_range_dates = [dates[0]]

        for current in dates[1:]:
            if (current - end).days == 1:  # 连续日期
                end = current
                current_range_dates.append(current)
            else:  # 不连续，开始新范围
                ranges.append((start, end, current_range_dates.copy()))
                start = end = current
                current_range_dates = [current]

        ranges.append((start, end, current_range_dates))
        return ranges

    def _assess_gap_severity(self, gap_days: int) -> str:
        """评估缺口严重程度"""
        if gap_days <= 1:
            return 'low'
        elif gap_days <= 5:
            return 'medium'
        elif gap_days <= 20:
            return 'high'
        else:
            return 'critical'

    def _get_gap_recommendation(self, gap_days: int) -> str:
        """获取缺口处理建议"""
        if gap_days <= 1:
            return 'Monitor in next update'
        elif gap_days <= 5:
            return 'Schedule immediate fill'
        elif gap_days <= 20:
            return 'Prioritize for data completion'
        else:
            return 'Investigate cause - possible delisting or suspension'

    async def _generate_analysis_report(self, gaps: List[DataGapInfo]):
        """生成分析报告"""
        try:
            report_dir = os.path.join(self.data_config.get('data_dir', 'data'), 'reports')
            report_file = os.path.join(report_dir, f"data_analysis_{self.progress.batch_id}.json")

            report_data = {
                'batch_id': self.progress.batch_id,
                'generated_at': get_shanghai_time().isoformat(),
                'download_progress': {
                    'total_instruments': self.progress.total_instruments,
                    'processed_instruments': self.progress.processed_instruments,
                    'successful_downloads': self.progress.successful_downloads,
                    'failed_downloads': self.progress.failed_downloads,
                    'total_quotes': self.progress.total_quotes,
                    'success_rate': self.progress.get_success_rate(),
                    'quality_score': self.progress.get_data_quality_score(),
                    'data_gaps_detected': len(gaps)
                },
                'data_gaps': [
                    {
                        'instrument_id': gap.instrument_id,
                        'symbol': gap.symbol,
                        'exchange': gap.exchange,
                        'gap_start': gap.gap_start.isoformat(),
                        'gap_end': gap.gap_end.isoformat(),
                        'gap_days': gap.gap_days,
                        'gap_type': gap.gap_type,
                        'severity': gap.severity,
                        'recommendation': gap.recommendation
                    }
                    for gap in gaps
                ],
                'gap_summary': {
                    'total_gaps': len(gaps),
                    'by_severity': {
                        severity: len([g for g in gaps if g.severity == severity])
                        for severity in ['low', 'medium', 'high', 'critical']
                    },
                    'by_exchange': {
                        exchange: len([g for g in gaps if g.exchange == exchange])
                        for exchange in set(gap.exchange for gap in gaps)
                    }
                }
            }

            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, ensure_ascii=False, indent=2)

            dm_logger.info(f"Analysis report saved to {report_file}")

        except Exception as e:
            dm_logger.error(f"Failed to generate analysis report: {e}")

    async def fill_data_gaps(
        self,
        exchange: str = None,
        severity_filter: List[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        instrument_ids: Optional[List[str]] = None,
        gap_type_filter: Optional[List[str]] = None,
        max_gap_days: Optional[int] = None,
        dry_run: bool = False
    ):
        """填补数据缺口"""
        try:
            dm_logger.info("Starting data gap filling process...")

            if end_date is None:
                end_date = date.today()

            exchanges = [exchange] if exchange else ['SSE', 'SZSE', 'BSE']

            # 获取需要填补的缺口
            gaps = await self.detect_data_gaps(exchanges, start_date, end_date)

            # 过滤严重程度
            if severity_filter:
                gaps = [g for g in gaps if g.severity in severity_filter]
            if instrument_ids:
                gaps = [g for g in gaps if g.instrument_id in instrument_ids]
            if gap_type_filter:
                gaps = [g for g in gaps if g.gap_type in gap_type_filter]
            if max_gap_days is not None:
                gaps = [g for g in gaps if g.gap_days <= max_gap_days]

            dm_logger.info(f"Found {len(gaps)} gaps to fill")

            filled_count = 0
            for gap in gaps:
                try:
                    if not dry_run:
                        success = await self._fill_single_gap(gap)
                        if success:
                            filled_count += 1

                    # API限流
                    await asyncio.sleep(1.0)

                except Exception as e:
                    dm_logger.error(f"Failed to fill gap for {gap.instrument_id}: {e}")

        except Exception as e:
            dm_logger.error(f"Data gap filling failed: {e}")

    async def _fill_single_gap(self, gap: DataGapInfo) -> bool:
        """填补单个数据缺口"""
        try:
            # 获取股票信息
            instrument = await self.db_ops.get_instrument_info(instrument_id=gap.instrument_id)
            converted_id = None
            if not instrument:
                try:
                    converted_id = convert_to_database_format(gap.instrument_id)
                except Exception:
                    converted_id = None

                if converted_id and converted_id != gap.instrument_id:
                    instrument = await self.db_ops.get_instrument_info(instrument_id=converted_id)

            if not instrument and gap.symbol:
                instrument = await self.db_ops.get_instrument_info(symbol=gap.symbol)
            if not instrument:
                dm_logger.warning(
                    f"[DataManager] Gap fill skipped: instrument not found {gap.instrument_id}"
                )
                return False

            start_date = datetime.combine(gap.gap_start, datetime.min.time())
            end_date = datetime.combine(gap.gap_end, datetime.max.time())

            # 从数据源获取缺失数据
            source_instrument_id = gap.instrument_id
            if converted_id and converted_id != gap.instrument_id:
                source_instrument_id = converted_id
            source_symbol = gap.symbol or instrument.get('symbol')

            data = await self.source_factory.get_daily_data(
                gap.exchange,
                source_instrument_id,
                source_symbol,
                start_date,
                end_date
            )

            if data:
                dm_logger.info(
                    f"[DataManager] Gap fill data fetched: {gap.instrument_id} "
                    f"{gap.gap_start} to {gap.gap_end}, rows={len(data)}"
                )
                # 保存数据
                for quote in data:
                    quote['instrument_id'] = instrument.get('instrument_id')
                success = await self.db_ops.save_daily_quotes(data)
                if success:
                    dm_logger.info(f"Filled gap for {gap.symbol}: {gap.gap_start} to {gap.gap_end}")
                    return True
                dm_logger.warning(
                    f"[DataManager] Gap fill save failed: {gap.instrument_id} "
                    f"{gap.gap_start} to {gap.gap_end}"
                )
            else:
                dm_logger.warning(
                    f"[DataManager] Gap fill returned no data: {gap.instrument_id} "
                    f"{gap.gap_start} to {gap.gap_end}"
                )

            return False

        except Exception as e:
            dm_logger.error(f"Failed to fill gap for {gap.instrument_id}: {e}")
            return False

    async def get_quotes(self, instrument_id: Optional[str] = None, symbol: Optional[str] = None,
                               start_date: Optional[datetime] = None, end_date: Optional[datetime] = None,
                               include_quality: bool = True,
                               return_format: str = 'pandas') -> Union[pd.DataFrame, List[Dict[str, Any]], str]:
        """获取行情数据"""
        try:
            # 参数验证
            if instrument_id:
                instrument_id = DataValidator.validate_instrument_id(instrument_id)
                instrument_id = convert_to_database_format(instrument_id)
            if symbol:
                symbol = DataValidator.validate_symbol(symbol)

            start_date, end_date = DataValidator.validate_date_range(start_date, end_date)

            if not (instrument_id or symbol):
                raise ValueError("Either instrument_id or symbol must be provided")

            # 从数据库获取
            data = await self.db_ops.get_daily_quotes(
                instrument_id=instrument_id,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                include_quality=include_quality
            )

            return self._format_response(data, return_format)

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to get quotes: {e}")
            raise

    async def download_single_instrument_data(self, instrument_id: str, instrument_info: dict,
                                             start_date: date, end_date: date, resume: bool = True) -> bool:
        """下载指定股票的历史数据"""
        try:
            dm_logger.info(f"[DataManager] Starting single instrument download: {instrument_id}")

            # 转换为数据库格式
            db_instrument_id = convert_to_database_format(instrument_id)
            dm_logger.info(f"[DataManager] Converting instrument ID: {instrument_id} -> {db_instrument_id}")

            # 初始化数据源工厂
            if not hasattr(self, 'source_factory') or not self.source_factory:
                from data_sources.source_factory import DataSourceFactory
                self.source_factory = DataSourceFactory(self.db_ops)
                await self.source_factory.initialize()

            # 获取该股票的主要数据源
            exchange = instrument_info.get('exchange')
            primary_source = self.source_factory.get_primary_source(exchange)

            if not primary_source:
                dm_logger.error(f"[DataManager] No data source available for exchange: {exchange}")
                return False

            dm_logger.info(f"[DataManager] Using data source: {primary_source.name}")

            # 获取交易日历 (统一转换为date类型)
            from utils.date_utils import normalize_date_range
            start_date_for_query, end_date_for_query = normalize_date_range(start_date, end_date)

            trading_days = await self.source_factory.get_trading_days(exchange, start_date_for_query, end_date_for_query)
            dm_logger.info(f"[DataManager] Found {len(trading_days)} trading days")

            # 获取已有数据的日期
            existing_dates = await self.db_ops.get_existing_data_dates(db_instrument_id, start_date, end_date)
            dm_logger.info(f"[DataManager] Found {len(existing_dates)} existing data records")

            # 计算需要下载的交易日
            missing_dates = set(trading_days) - set(existing_dates)

            if not missing_dates:
                dm_logger.info(f"[DataManager] No missing data for {instrument_id}")
                return True

            dm_logger.info(f"[DataManager] Need to download {len(missing_dates)} trading days")

            # 按时间顺序排列缺失日期
            sorted_missing_dates = sorted(missing_dates)

            # 获取下载范围并转换为datetime
            download_start = datetime.combine(sorted_missing_dates[0], datetime.min.time())
            download_end = datetime.combine(sorted_missing_dates[-1], datetime.max.time())

            dm_logger.info(f"[DataManager] Downloading data from {download_start.date()} to {download_end.date()}")

            try:
                # 一次性下载所有数据
                data = await primary_source.get_daily_data(
                    instrument_id=instrument_id,
                    symbol=instrument_info.get('symbol'),
                    start_date=download_start,
                    end_date=download_end
                )

                if data and len(data) > 0:
                    # 确保数据中包含正确的instrument_id
                    for quote in data:
                        quote['instrument_id'] = db_instrument_id

                    # 保存数据到数据库
                    success = await self.db_ops.save_daily_quotes(data)

                    if success:
                        saved_count = len(data)
                        dm_logger.info(f"[DataManager] Saved {saved_count} records for {instrument_id}")

                        # 统计结果
                        total_required = len(missing_dates)
                        success_rate = (saved_count / total_required * 100) if total_required > 0 else 0

                        dm_logger.info(f"[DataManager] Single instrument download completed: {instrument_id}")
                        dm_logger.info(f"[DataManager] Success: {saved_count}/{total_required} ({success_rate:.1f}%)")

                        return success_rate >= 80.0  # 80%以上成功率认为成功
                    else:
                        dm_logger.error(f"[DataManager] Failed to save data for {instrument_id}")
                        return False
                else:
                    dm_logger.warning(f"[DataManager] No data returned for {instrument_id}")
                    return False

            except Exception as e:
                dm_logger.error(f"[DataManager] Failed to download {instrument_id}: {e}")
                return False

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to download single instrument {instrument_id}: {e}")
            return False

    async def get_system_status(self) -> Dict[str, Any]:
        """获取标准系统状态"""
        try:
            db_stats = await self.db_ops.get_database_statistics()
            source_health = await self.source_factory.health_check_all()

            return {
                'data_manager': {
                    'is_running': self.is_running,
                    'download_progress': {
                        'batch_id': self.progress.batch_id,
                        'total_instruments': self.progress.total_instruments,
                        'processed_instruments': self.progress.processed_instruments,
                        'success_rate': self.progress.get_success_rate(),
                        'quality_score': self.progress.get_data_quality_score(),
                        'data_gaps_detected': self.progress.data_gaps_detected,
                        'elapsed_time': str(self.progress.get_elapsed_time())
                    }
                },
                'database': db_stats,
                'data_sources': source_health,
                'timestamp': get_shanghai_time()
            }

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to get system status: {e}")
            return {'error': str(e)}

    async def _generate_download_report(self, exchanges: List[str]) -> dict:
        """生成详细的下载报告"""
        try:
            dm_logger.info("[DataManager] Generating detailed download report...")

            # 基础统计信息
            report = {
                'summary': {
                    'batch_id': self.progress.batch_id,
                    'total_instruments': self.progress.total_instruments,
                    'processed_instruments': self.progress.processed_instruments,
                    'successful_downloads': self.progress.successful_downloads,
                    'failed_downloads': self.progress.failed_downloads,
                    'total_quotes': self.progress.total_quotes,
                    'data_gaps_detected': self.progress.data_gaps_detected,
                    'quality_score': self.progress.get_data_quality_score(),
                    'start_time': self.progress.start_time,
                    'duration': datetime.now() - self.progress.start_time
                },
                'exchange_stats': {},
                'database_stats': {},
                'performance_metrics': {},
                'errors': self.progress.errors[-10:] if self.progress.errors else []  # 最近10个错误
            }

            # 获取各交易所的详细统计
            for exchange in exchanges:
                try:
                    # 统计该交易所的数据
                    exchange_query = """
                    SELECT
                        COUNT(DISTINCT i.instrument_id) as total_instruments,
                        COUNT(dq.instrument_id) as total_quotes,
                        MIN(dq.time) as earliest_date,
                        MAX(dq.time) as latest_date
                    FROM instruments i
                    LEFT JOIN daily_quotes dq ON i.instrument_id = dq.instrument_id
                    WHERE i.exchange = ? AND i.is_active = 1
                    """

                    result = await self.db_ops.execute_query(exchange_query, (exchange,))
                    if result:
                        stats = result[0]
                        report['exchange_stats'][exchange] = {
                            'total_instruments': stats['total_instruments'],
                            'total_quotes': stats['total_quotes'],
                            'earliest_date': stats['earliest_date'],
                            'latest_date': stats['latest_date'],
                            'coverage_ratio': (stats['total_quotes'] / (stats['total_instruments'] * 1000)) if stats['total_instruments'] > 0 else 0  # 估算覆盖率
                        }
                except Exception as e:
                    dm_logger.warning(f"[DataManager] Failed to generate stats for {exchange}: {e}")
                    report['exchange_stats'][exchange] = {'error': str(e)}

            # 获取数据库统计信息
            try:
                db_stats = await self.db_ops.get_database_statistics()
                report['database_stats'] = db_stats
            except Exception as e:
                dm_logger.warning(f"[DataManager] Failed to get database statistics: {e}")
                report['database_stats'] = {'error': str(e)}

            # 性能指标
            if report['summary']['duration'].total_seconds() > 0:
                duration_seconds = report['summary']['duration'].total_seconds()
                report['performance_metrics'] = {
                    'quotes_per_second': self.progress.total_quotes / duration_seconds if duration_seconds > 0 else 0,
                    'instruments_per_minute': self.progress.processed_instruments / (duration_seconds / 60) if duration_seconds > 0 else 0,
                    'average_quotes_per_instrument': self.progress.total_quotes / self.progress.processed_instruments if self.progress.processed_instruments > 0 else 0
                }

            dm_logger.info("[DataManager] Download report generated successfully")
            return report

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to generate download report: {e}")
            return {
                'summary': {'error': str(e)},
                'exchange_stats': {},
                'database_stats': {},
                'performance_metrics': {},
                'errors': [str(e)]
            }

    async def _generate_daily_update_report(self, exchanges: List[str], target_date: date, update_results: dict) -> dict:
        """生成每日更新报告"""
        try:
            dm_logger.info("[DataManager] Generating daily update report...")

            report = {
                'summary': {
                    'target_date': target_date.isoformat(),
                    'exchanges': exchanges,
                    'total_instruments_checked': 0,
                    'updated_instruments': 0,
                    'new_quotes_added': 0,
                    'success_rate': 0,
                    'update_time': datetime.now().strftime('%H:%M:%S')
                },
                'exchange_stats': {},
                'update_results': update_results.get('exchange_stats', {}),
                'errors': []
            }

            # 统计更新结果
            for exchange in exchanges:
                try:
                    stats = update_results.get(exchange, {})
                    if stats and 'error' not in stats:
                        # 检查是否是更新任务的结果还是每日数据更新的结果
                        if 'updated_count' in stats:  # 每日数据更新结果
                            updated_count = stats.get('updated_count', 0)
                            total_count = stats.get('total_active', 0)
                            new_quotes = stats.get('new_quotes', 0)
                            rate = (updated_count / total_count * 100) if total_count > 0 else 0
                            report['exchange_stats'][exchange] = {
                                'updated_instruments': updated_count,
                                'total_active_instruments': total_count,
                                'new_quotes_added': new_quotes,
                                'update_rate': rate
                            }
                            report['summary']['total_instruments_checked'] += total_count
                            report['summary']['updated_instruments'] += updated_count
                            report['summary']['new_quotes_added'] += new_quotes
                        elif 'success_count' in stats:  # 标准下载任务结果
                            success_count = stats.get('success_count', 0)
                            total_count = stats.get('total_count', 0)
                            quotes_count = stats.get('quotes_count', 0)
                            report['exchange_stats'][exchange] = {
                                'success_count': success_count,
                                'total_count': total_count,
                                'quotes_count': quotes_count
                            }
                            report['summary']['total_instruments_checked'] += total_count
                            report['summary']['updated_instruments'] += success_count
                            report['summary']['new_quotes_added'] += quotes_count
                    else:
                        report['exchange_stats'][exchange] = {'error': str(stats.get('error', 'Unknown error'))}
                        report['errors'].append(f"Exchange {exchange}: {stats.get('error', 'Unknown error')}")

                except Exception as e:
                    dm_logger.error(f"[DataManager] Error processing {exchange} update results: {e}")
                    report['exchange_stats'][exchange] = {'error': str(e)}
                    report['errors'].append(f"Exchange {exchange}: {str(e)}")

            # 计算总体成功率
            total_checked = report['summary']['total_instruments_checked']
            total_updated = report['summary']['updated_instruments']
            report['summary']['success_rate'] = (total_updated / total_checked * 100) if total_checked > 0 else 100.0
 
            return report

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to generate daily update report: {e}")
            return {
                'summary': {
                    'target_date': target_date.isoformat(),
                    'exchanges': exchanges,
                    'total_instruments_checked': 0,
                    'updated_instruments': 0,
                    'new_quotes_added': 0,
                    'success_rate': 0,
                    'update_time': datetime.now().strftime('%H:%M:%S')
                },
                'exchange_stats': {},
                'update_results': update_results.get('exchange_stats', {}),
                'errors': [str(e)]
            }

    async def _save_progress(self):
        """保存标准进度到文件"""
        try:
            progress_data = {
                'batch_id': self.progress.batch_id,
                'total_instruments': self.progress.total_instruments,
                'processed_instruments': self.progress.processed_instruments,
                'successful_downloads': self.progress.successful_downloads,
                'failed_downloads': self.progress.failed_downloads,
                'total_quotes': self.progress.total_quotes,
                'trading_days_processed': self.progress.trading_days_processed,
                'total_trading_days': self.progress.total_trading_days,
                'data_gaps_detected': self.progress.data_gaps_detected,
                'quality_issues': self.progress.quality_issues,
                'start_time': self.progress.start_time.isoformat(),
                'current_exchange': self.progress.current_exchange,
                'errors': self.progress.errors[-50:]
            }

            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to save progress: {e}")

    async def _load_progress(self):
        """加载标准进度文件"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress_data = json.load(f)

                self.progress = DownloadProgress(
                    batch_id=progress_data.get('batch_id', datetime.now().strftime('%Y%m%d_%H%M%S')),
                    total_instruments=progress_data.get('total_instruments', 0),
                    processed_instruments=progress_data.get('processed_instruments', 0),
                    successful_downloads=progress_data.get('successful_downloads', 0),
                    failed_downloads=progress_data.get('failed_downloads', 0),
                    total_quotes=progress_data.get('total_quotes', 0),
                    trading_days_processed=progress_data.get('trading_days_processed', 0),
                    total_trading_days=progress_data.get('total_trading_days', 0),
                    data_gaps_detected=progress_data.get('data_gaps_detected', 0),
                    quality_issues=progress_data.get('quality_issues', 0),
                    start_time=datetime.fromisoformat(progress_data['start_time']),
                    current_exchange=progress_data.get('current_exchange', ''),
                    errors=progress_data.get('errors', [])
                )

                dm_logger.info(f"[DataManager] progress loaded: {self.progress.processed_instruments}/{self.progress.total_instruments}")

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to load progress: {e}")

    def _format_response(self, data: pd.DataFrame, format_type: str) -> Union[pd.DataFrame, List[Dict[str, Any]], str]:
        """格式化响应数据"""
        if data is None:
            return None
        if format_type == 'pandas':
            return data
        elif format_type == 'json':
            return data if isinstance(data, list) else data.to_dict('records')
        elif format_type == 'csv':
            return data.to_csv(index=False)
        else:
            return data

    async def _apply_quote_filters(self, data: Any, filters: Dict[str, Any]) -> Any:
        """应用行情过滤器"""
        try:
            if data is None:
                return data

            # 转换为DataFrame如果需要
            if not isinstance(data, pd.DataFrame):
                data = pd.DataFrame(data)

            # 应用过滤器
            if filters.get('tradestatus') is not None:
                data = data[data['tradestatus'] == filters['tradestatus']]

            if filters.get('min_volume') is not None:
                data = data[data['volume'] >= filters['min_volume']]

            if filters.get('is_complete') is not None:
                data = data[data['is_complete'] == filters['is_complete']]

            return data

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to apply quote filters: {e}")
            return data

    async def _generate_quote_statistics(self, data: Any) -> Dict[str, Any]:
        """生成行情统计信息"""
        try:
            if data is None or (hasattr(data, 'empty') and data.empty):
                return {}

            # 转换为DataFrame
            if not isinstance(data, pd.DataFrame):
                data = pd.DataFrame(data)

            stats = {
                'total_records': len(data),
                'date_range': {
                    'start': data['time'].min() if not data.empty else None,
                    'end': data['time'].max() if not data.empty else None
                },
                'price_stats': {
                    'avg_close': data['close'].mean() if 'close' in data.columns else None,
                    'min_close': data['close'].min() if 'close' in data.columns else None,
                    'max_close': data['close'].max() if 'close' in data.columns else None,
                    'avg_volume': data['volume'].mean() if 'volume' in data.columns else None,
                    'total_volume': data['volume'].sum() if 'volume' in data.columns else 0,
                    'total_amount': data['amount'].sum() if 'amount' in data.columns else 0.0
                },
                'trading_days': len(data[data['tradestatus'] == 1]) if 'tradestatus' in data.columns else len(data)
            }

            return stats

        except Exception as e:
            dm_logger.error(f"[DataManager] Failed to generate quote statistics: {e}")
            return {}
 
    async def update_daily_data(self, exchanges: Optional[List[str]] = None, target_date: Optional[date] = None) -> Optional[dict]:
        """每日数据更新"""
        try:
            dm_logger.info(f"[DataManager] Starting daily data update for exchanges: {exchanges}")

            if exchanges is None:
                exchanges = ['SSE', 'SZSE']

            if target_date is None:
                target_date = date.today()

            # 统计更新结果
            update_results = {
                'success_count': 0,
                'failure_count': 0,
                'total_quotes_added': 0,
                'exchange_stats': {}
            }

            for exchange in exchanges:
                try:
                    dm_logger.info(f"[DataManager] Updating data for {exchange}")

                    # 获取该交易所的活跃股票
                    instruments = await self.db_ops.get_active_instruments(exchange)
                    exchange_result = {
                        'success_count': 0,
                        'failure_count': 0,
                        'quotes_added': 0,
                        'total_instruments': len(instruments)
                    }

                    for instrument in instruments:
                        try:
                            # 获取最新日期
                            latest_date = await self.db_ops.get_latest_quote_date(
                                instrument['instrument_id']
                            )

                            # 如果没有数据或数据不是最新的，则更新
                            should_update = (
                                latest_date is None or
                                latest_date < datetime.combine(target_date, datetime.min.time())
                            )

                            if should_update:
                                # 从数据源获取数据
                                start_date = target_date - timedelta(days=1)
                                end_date = target_date

                                data = await self.source_factory.get_daily_data(
                                    exchange,
                                    instrument['instrument_id'],
                                    instrument['symbol'],
                                    datetime.combine(start_date, datetime.min.time()),
                                    datetime.combine(end_date, datetime.max.time())
                                )

                                if data:
                                    await self.db_ops.save_daily_quotes(data)
                                    exchange_result['quotes_added'] += len(data)
                                    update_results['total_quotes_added'] += len(data)
                                    dm_logger.debug(f"[DataManager] Updated {len(data)} records for {instrument['symbol']}")

                                exchange_result['success_count'] += 1
                                update_results['success_count'] += 1

                        except Exception as e:
                            dm_logger.error(f"[DataManager] Failed to update {instrument['symbol']}: {e}")
                            exchange_result['failure_count'] += 1
                            update_results['failure_count'] += 1
                            continue

                    update_results['exchange_stats'][exchange] = exchange_result
                    dm_logger.info(f"[DataManager] {exchange} update completed: {exchange_result['success_count']} success, {exchange_result['failure_count']} failed, {exchange_result['quotes_added']} quotes added")

                except Exception as e:
                    dm_logger.error(f"[DataManager] Failed to update {exchange}: {e}")
                    update_results['failure_count'] += 1
                    update_results['exchange_results'][exchange] = {'error': str(e)}
                    continue

            # 生成并发送详细的更新报告
            dm_logger.info("[DataManager] Generating daily update completion report...")
            update_report = await self._generate_daily_update_report(exchanges, target_date, update_results)

            # 保存报告到文件（可选）
            try:
                import os
                reports_dir = "data/reports"
                os.makedirs(reports_dir, exist_ok=True)
                report_file = os.path.join(reports_dir, f"daily_update_report_{target_date.isoformat()}.json")

                import json
                with open(report_file, 'w', encoding='utf-8') as f:
                    json.dump(update_report, f, ensure_ascii=False, indent=2, default=str)

                dm_logger.info(f"[DataManager] Daily update report saved to: {report_file}")
            except Exception as e:
                dm_logger.warning(f"[DataManager] Failed to save daily update report: {e}")

            dm_logger.info("[DataManager] Daily data update completed")
            return update_results
        
        except Exception as e:
            dm_logger.error(f"[DataManager] Daily data update failed: {e}")
            return {
                'success_count': 0,
                'failure_count': 0,
                'total_quotes_added': 0,
                'exchange_results': {},
                'error': str(e)
            }

    @log_execution("DataManager", "backup_data") 
    async def backup_data(self, backup_path: str = None, include_compression: bool = True) -> bool:
        """备份数据库数据

        Args:
            backup_path: 备份文件路径，如果为None则自动生成
            include_compression: 是否包含压缩备份

        Returns:
            bool: 备份是否成功
        """
        try:
            dm_logger.info("[DataManager] Starting database backup...")

            # 生成备份文件路径
            if backup_path is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_dir = os.path.join(self.data_config.get('data_dir', 'data'), 'backups')
                os.makedirs(backup_dir, exist_ok=True)
                backup_path = os.path.join(backup_dir, f"quotes_backup_{timestamp}.db")

            # 执行备份
            success = await self.db_ops.backup_database(backup_path)

            if success:
                dm_logger.info(f"[DataManager] Database backup completed: {backup_path}")

                # 可选：创建压缩备份
                if include_compression:
                    try:
                        import gzip
                        import shutil

                        compressed_path = backup_path + '.gz'
                        with open(backup_path, 'rb') as f_in:
                            with gzip.open(compressed_path, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)

                        dm_logger.info(f"[DataManager] Compressed backup created: {compressed_path}")
                    except Exception as e:
                        dm_logger.warning(f"[DataManager] Failed to create compressed backup: {e}")

                return True
            else:
                dm_logger.error("[DataManager] Database backup failed")
                return False

        except Exception as e:
            dm_logger.error(f"[DataManager] Backup failed: {e}")
            return False

    def get_top_affected_stocks(self, gaps: List[DataGapInfo], limit: int = 10) -> List[Dict[str, Any]]:
        """
        从缺口列表中计算并返回受影响最严重的股票。

        Args:
            gaps: DataGapInfo 对象的列表。
            limit: 返回的股票数量。

        Returns:
            一个包含受影响最严重股票信息的字典列表。
        """
        from collections import defaultdict

        gaps_by_stock = defaultdict(list)
        for gap in gaps:
            gaps_by_stock[gap.instrument_id].append(gap)

        stock_scores = []
        for stock_id, stock_gaps in gaps_by_stock.items():
            total_missing_days = sum(g.gap_days for g in stock_gaps)
            critical_gaps = sum(1 for g in stock_gaps if g.severity == 'critical')
            high_gaps = sum(1 for g in stock_gaps if g.severity == 'high')
            score = total_missing_days + (critical_gaps * 10) + (high_gaps * 5)
            stock_scores.append({
                'symbol': stock_gaps[0].symbol,
                'severity_score': score,
                'total_missing_days': total_missing_days
            })
        stock_scores.sort(key=lambda x: x['severity_score'], reverse=True)
        return stock_scores[:limit]


# 全局标准数据管理器实例
data_manager = DataManager()
