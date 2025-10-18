"""
database operations for the quote system.
Supports comprehensive data management with new schema.
"""

import asyncio
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple, Union
import pandas as pd
from sqlalchemy import text, func, desc, asc
from sqlalchemy.orm import sessionmaker
from utils.date_utils import get_shanghai_time
from utils import db_logger, config_manager



from .connection import db_manager
from .models import (
    InstrumentDB, DailyQuoteDB, TradingCalendarDB, TradingSessionDB,
    DataUpdateDB, DataSourceStatusDB
)


class DatabaseOperations:
    """database operations with new schema support"""

    def __init__(self, auto_initialize=True):
        self.db = db_manager
        self.engine = None
        self.async_engine = None
        self.SessionLocal = None
        self.AsyncSessionLocal = None
        self.db_logger = db_logger
        self.config_manager = config_manager

        # 自动初始化
        if auto_initialize:
            try:
                self.db.initialize()
                self.engine = self.db.sync_engine
                self.async_engine = self.db.async_engine
                self.SessionLocal = self.db.SessionLocal
                self.AsyncSessionLocal = self.db.AsyncSessionLocal
                self.db_logger.info("DatabaseOperations initialized successfully")
            except Exception as e:
                self.db_logger.error(f"DatabaseOperations initialization failed: {e}")
                raise

    async def initialize(self):
        """初始化数据库操作"""
        try:
            self.db_logger.info("Initializing DatabaseOperations...")

            # 确保数据库连接正常
            self.db.initialize()

            # 更新本地引用
            self.engine = self.db.sync_engine
            self.async_engine = self.db.async_engine
            self.SessionLocal = self.db.SessionLocal
            self.AsyncSessionLocal = self.db.AsyncSessionLocal

            self.db_logger.info("DatabaseOperations initialized successfully")
        except Exception as e:
            self.db_logger.error(f"Failed to initialize DatabaseOperations: {e}")
            raise

    def get_async_session(self):
        """Get async database session"""
        return self.db.AsyncSessionLocal()

    def get_session(self):
        """Get synchronous database session"""
        return self.SessionLocal()

    # === Instrument Operations ===

    async def get_instruments_by_exchange(self, exchange: str, is_active: bool = True) -> List[Dict[str, Any]]:
        """根据交易所获取交易品种列表"""
        try:
            with self.get_session() as session:
                query = session.query(InstrumentDB).filter(InstrumentDB.exchange == exchange)

                if is_active is not None:
                    query = query.filter(InstrumentDB.is_active == is_active)

                query = query.order_by(InstrumentDB.symbol)

                instruments = []
                for instrument in query:
                    instruments.append({
                        'instrument_id': instrument.instrument_id,
                        'symbol': instrument.symbol,
                        'name': instrument.name,
                        'exchange': instrument.exchange,
                        'type': instrument.type,
                        'currency': instrument.currency,
                        'listed_date': instrument.listed_date,
                        'delisted_date': instrument.delisted_date,
                        'industry': instrument.industry,
                        'sector': instrument.sector,
                        'market': instrument.market,
                        'status': instrument.status,
                        'is_active': instrument.is_active,
                        'is_st': instrument.is_st,
                        'trading_status': instrument.trading_status,
                                                'source': instrument.source,
                        'source_symbol': instrument.source_symbol,
                        'created_at': instrument.created_at,
                        'updated_at': instrument.updated_at,
                        'data_version': instrument.data_version
                    })

                return instruments

        except Exception as e:
            self.db_logger.error(f"Failed to get instruments by exchange {exchange}: {e}")
            return []

    async def get_active_instruments(self, exchange: str = None) -> List[Dict[str, Any]]:
        """获取活跃交易品种列表"""
        try:
            with self.get_session() as session:
                query = session.query(InstrumentDB).filter(InstrumentDB.is_active == True)

                if exchange:
                    query = query.filter(InstrumentDB.exchange == exchange)

                query = query.order_by(InstrumentDB.exchange, InstrumentDB.symbol)

                instruments = []
                for instrument in query:
                    instruments.append({
                        'instrument_id': instrument.instrument_id,
                        'symbol': instrument.symbol,
                        'name': instrument.name,
                        'exchange': instrument.exchange,
                        'type': instrument.type,
                        'currency': instrument.currency,
                        'listed_date': instrument.listed_date,
                        'delisted_date': instrument.delisted_date,
                        'industry': instrument.industry,
                        'sector': instrument.sector,
                        'market': instrument.market,
                        'status': instrument.status,
                        'is_active': instrument.is_active,
                        'is_st': instrument.is_st,
                        'trading_status': instrument.trading_status,
                                                'source': instrument.source,
                        'source_symbol': instrument.source_symbol,
                        'created_at': instrument.created_at,
                        'updated_at': instrument.updated_at,
                        'data_version': instrument.data_version
                    })

                return instruments

        except Exception as e:
            self.db_logger.error(f"Failed to get active instruments: {e}")
            return []

    async def get_existing_data_dates(self, instrument_id: str, start_date: date, end_date: date) -> List[date]:
        """获取指定品种的已有数据日期"""
        try:
            with self.get_session() as session:
                query = session.query(DailyQuoteDB.time).filter(
                    DailyQuoteDB.instrument_id == instrument_id,
                    DailyQuoteDB.time >= start_date,
                    DailyQuoteDB.time <= end_date
                ).order_by(DailyQuoteDB.time)

                dates = []
                for result in query:
                    if isinstance(result[0], datetime):
                        dates.append(result[0].date())
                    else:
                        dates.append(result[0])

                return dates

        except Exception as e:
            self.db_logger.error(f"Failed to get existing data dates for {instrument_id}: {e}")
            return []

    async def get_instruments_with_filters(
        self,
        exchange: str = None,
        instrument_type: str = None,
        industry: str = None,
        sector: str = None,
        market: str = None,
        status: str = None,
        is_active: bool = None,
        is_st: bool = None,
        trading_status: int = None,
        listed_after: date = None,
        listed_before: date = None,
        limit: int = 100,
        offset: int = 0,
        sort_by: str = "symbol",
        sort_order: str = "asc"
    ) -> List[Dict[str, Any]]:
        """根据过滤条件获取交易品种列表"""
        try:
            with self.get_session() as session:
                query = session.query(InstrumentDB)

                # 应用过滤器
                if exchange:
                    query = query.filter(InstrumentDB.exchange == exchange)
                if instrument_type:
                    query = query.filter(InstrumentDB.type == instrument_type)
                if industry:
                    query = query.filter(InstrumentDB.industry == industry)
                if sector:
                    query = query.filter(InstrumentDB.sector == sector)
                if market:
                    query = query.filter(InstrumentDB.market == market)
                if status:
                    query = query.filter(InstrumentDB.status == status)
                if is_active is not None:
                    query = query.filter(InstrumentDB.is_active == is_active)
                if is_st is not None:
                    query = query.filter(InstrumentDB.is_st == is_st)
                if trading_status is not None:
                    query = query.filter(InstrumentDB.trading_status == trading_status)
                if listed_after:
                    query = query.filter(InstrumentDB.listed_date >= listed_after)
                if listed_before:
                    query = query.filter(InstrumentDB.listed_date <= listed_before)

                # 排序
                if hasattr(InstrumentDB, sort_by):
                    sort_column = getattr(InstrumentDB, sort_by)
                    if sort_order.lower() == 'desc':
                        query = query.order_by(desc(sort_column))
                    else:
                        query = query.order_by(asc(sort_column))

                # 分页
                query = query.limit(limit).offset(offset)

                instruments = []
                for instrument in query:
                    instruments.append({
                        'instrument_id': instrument.instrument_id,
                        'symbol': instrument.symbol,
                        'name': instrument.name,
                        'exchange': instrument.exchange,
                        'type': instrument.type,
                        'currency': instrument.currency,
                        'listed_date': instrument.listed_date,
                        'delisted_date': instrument.delisted_date,
                        'issue_date': instrument.issue_date,
                        'industry': instrument.industry,
                        'sector': instrument.sector,
                        'market': instrument.market,
                        'status': instrument.status,
                        'is_active': instrument.is_active,
                        'is_st': instrument.is_st,
                        'trading_status': instrument.trading_status,
                                                'source': instrument.source,
                        'source_symbol': instrument.source_symbol,
                        'created_at': instrument.created_at,
                        'updated_at': instrument.updated_at,
                        'data_version': instrument.data_version
                    })

                return instruments

        except Exception as e:
            self.db_logger.error(f"Failed to get instruments with filters: {e}")
            return []

    async def get_instrument_by_id(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """根据ID获取交易品种信息"""
        return await self.get_instrument_info(instrument_id=instrument_id)

    async def count_quotes_by_instrument(self, instrument_id: str, start_date: date = None,
                                         end_date: date = None) -> int:
        """统计指定股票的数据记录数"""
        try:
            with self.get_session() as session:
                query = session.query(DailyQuoteDB).filter(
                    DailyQuoteDB.instrument_id == instrument_id
                )

                if start_date:
                    query = query.filter(DailyQuoteDB.time >= start_date)
                if end_date:
                    query = query.filter(DailyQuoteDB.time <= end_date)

                return query.count()

        except Exception as e:
            self.db_logger.error(f"Failed to count quotes for {instrument_id}: {e}")
            return 0

    async def get_instrument_date_range(self, instrument_id: str, start_date: date = None,
                                        end_date: date = None) -> Dict[str, Any]:
        """获取指定股票的数据日期范围"""
        try:
            with self.get_session() as session:
                query = session.query(
                    func.min(DailyQuoteDB.time).label('min_date'),
                    func.max(DailyQuoteDB.time).label('max_date')
                ).filter(
                    DailyQuoteDB.instrument_id == instrument_id
                )

                if start_date:
                    query = query.filter(DailyQuoteDB.time >= start_date)
                if end_date:
                    query = query.filter(DailyQuoteDB.time <= end_date)

                result = query.first()

                if result and result.min_date and result.max_date:
                    return {
                        'start_date': result.min_date.date() if isinstance(result.min_date, datetime) else result.min_date,
                        'end_date': result.max_date.date() if isinstance(result.max_date, datetime) else result.max_date
                    }
                else:
                    return {}

        except Exception as e:
            self.db_logger.error(f"Failed to get date range for {instrument_id}: {e}")
            return {}

    async def get_instrument_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """根据交易代码获取交易品种信息"""
        return await self.get_instrument_info(symbol=symbol)

    async def save_daily_quotes(self, quotes: List[Dict[str, Any]]) -> bool:
        """批量保存日线数据（别名方法）"""
        return await self.save_daily_data(quotes)

    async def get_daily_quotes(
        self,
        instrument_id: str = None,
        symbol: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        tradestatus: int = None,
        is_complete: bool = None,
        min_volume: int = None,
        include_quality: bool = True,
        limit: int = None,
        return_format: str = 'pandas'
    ) -> Any:
        """获取日线数据（别名方法）"""
        return await self.get_daily_data(
            instrument_id=instrument_id,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            tradestatus=tradestatus,
            is_complete=is_complete,
            min_volume=min_volume,
            return_format=return_format
        )

    async def save_instrument_list(self, instruments: List[Dict[str, Any]]) -> bool:
        """保存交易品种列表（别名方法）"""
        return await self.save_instruments_batch(instruments)

    async def save_instruments_batch(self, instruments: List[Dict[str, Any]]) -> bool:
        """批量保存交易品种信息"""
        try:
            with self.get_session() as session:
                success_count = 0
                for instrument_data in instruments:
                    try:
                        # 处理日期字段
                        processed_data = {}
                        for key, value in instrument_data.items():
                            if key in ['listed_date', 'delisted_date', 'issue_date', 'created_at', 'updated_at']:
                                if value is None:
                                    processed_data[key] = None
                                elif isinstance(value, str):
                                    # 尝试解析字符串日期
                                    try:
                                        if value in ['', 'None', 'null']:
                                            processed_data[key] = None
                                        elif len(value) == 10:  # YYYY-MM-DD
                                            processed_data[key] = datetime.strptime(value, '%Y-%m-%d')
                                        elif len(value) > 10:  # YYYY-MM-DD HH:MM:SS or ISO format
                                            processed_data[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                                        else:
                                            processed_data[key] = None
                                    except (ValueError, TypeError):
                                        self.db_logger.warning(f"Invalid date format for {key}: {value}")
                                        processed_data[key] = None
                                else:
                                    processed_data[key] = value
                            else:
                                processed_data[key] = value

                        # 检查是否已存在
                        existing = session.query(InstrumentDB).filter(
                            InstrumentDB.instrument_id == processed_data['instrument_id']
                        ).first()

                        if existing:
                            # 更新现有记录
                            for key, value in processed_data.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                            existing.updated_at = get_shanghai_time()
                        else:
                            # 创建新记录
                            try:
                                # 确保必需字段存在
                                required_fields = ['instrument_id', 'symbol', 'name', 'exchange', 'type', 'currency']
                                for field in required_fields:
                                    if field not in processed_data:
                                        self.db_logger.warning(f"Missing required field '{field}' for instrument {instrument_data.get('instrument_id', 'unknown')}")

                                # 过滤掉无效字段
                                valid_data = {k: v for k, v in processed_data.items() if hasattr(InstrumentDB, k)}

                                # 调试：打印数据信息
                                self.db_logger.debug(f"Creating instrument {processed_data.get('instrument_id', 'unknown')} with {len(valid_data)} fields")

                                db_instrument = InstrumentDB(**valid_data)
                                session.add(db_instrument)
                            except Exception as create_error:
                                self.db_logger.error(f"Error creating instrument: {create_error}")
                                self.db_logger.error(f"Original data keys: {list(processed_data.keys())}")
                                raise
                        success_count += 1
                    except Exception as e:
                        self.db_logger.error(f"Error saving instrument {instrument_data.get('instrument_id', 'unknown')}: {e}")

                session.commit()
                self.db_logger.info(f"Successfully saved {success_count}/{len(instruments)} instruments")
                return success_count > 0

        except Exception as e:
            import traceback
            self.db_logger.error(f"Failed to save instruments batch: {e}")
            self.db_logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def get_instruments_list(
        self,
        exchange: str = None,
        type: str = None,
        is_active: bool = True,
        status: str = None,
        industry: str = None,
        limit: int = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """获取交易品种列表"""
        try:
            with self.get_session() as session:
                query = session.query(InstrumentDB)

                # Apply filters
                if exchange:
                    query = query.filter(InstrumentDB.exchange == exchange)
                if type:
                    query = query.filter(InstrumentDB.type == type)
                if is_active is not None:
                    query = query.filter(InstrumentDB.is_active == is_active)
                if status:
                    query = query.filter(InstrumentDB.status == status)
                if industry:
                    query = query.filter(InstrumentDB.industry == industry)

                # Apply ordering and pagination
                query = query.order_by(InstrumentDB.exchange, InstrumentDB.symbol)
                if limit:
                    query = query.limit(limit).offset(offset)

                instruments = []
                for instrument in query:
                    instruments.append({
                        'instrument_id': instrument.instrument_id,
                        'symbol': instrument.symbol,
                        'name': instrument.name,
                        'exchange': instrument.exchange,
                        'type': instrument.type,
                        'currency': instrument.currency,
                        'listed_date': instrument.listed_date,
                        'delisted_date': instrument.delisted_date,
                        'issue_date': instrument.issue_date,
                        'industry': instrument.industry,
                        'sector': instrument.sector,
                        'market': instrument.market,
                        'status': instrument.status,
                        'is_active': instrument.is_active,
                        'is_st': instrument.is_st,
                        'trading_status': instrument.trading_status,
                                                'source': instrument.source,
                        'source_symbol': instrument.source_symbol,
                        'created_at': instrument.created_at,
                        'updated_at': instrument.updated_at,
                        'data_version': instrument.data_version
                    })

                return instruments

        except Exception as e:
            self.db_logger.error(f"Failed to get instruments list: {e}")
            return []

    async def get_instrument_info(
        self,
        symbol: str = None,
        instrument_id: str = None
    ) -> Optional[Dict[str, Any]]:
        """获取单个交易品种详细信息"""
        try:
            with self.get_session() as session:
                query = session.query(InstrumentDB)

                if instrument_id:
                    query = query.filter(InstrumentDB.instrument_id == instrument_id)
                elif symbol:
                    query = query.filter(InstrumentDB.symbol == symbol)
                else:
                    return None

                instrument = query.first()
                if not instrument:
                    return None

                return {
                    'instrument_id': instrument.instrument_id,
                    'symbol': instrument.symbol,
                    'name': instrument.name,
                    'exchange': instrument.exchange,
                    'type': instrument.type,
                    'currency': instrument.currency,
                    'listed_date': instrument.listed_date,
                    'delisted_date': instrument.delisted_date,
                    'issue_date': instrument.issue_date,
                    'industry': instrument.industry,
                    'sector': instrument.sector,
                    'market': instrument.market,
                    'status': instrument.status,
                    'is_active': instrument.is_active,
                    'is_st': instrument.is_st,
                    'trading_status': instrument.trading_status,
                    'source': instrument.source,
                    'source_symbol': instrument.source_symbol,
                    'created_at': instrument.created_at,
                    'updated_at': instrument.updated_at,
                    'data_version': instrument.data_version
                }

        except Exception as e:
            self.db_logger.error(f"Failed to get instrument info: {e}")
            return None

    # === Daily Quote Operations ===

    async def save_daily_data(self, quotes: List[Dict[str, Any]]) -> bool:
        """批量保存日线数据"""
        try:
            with self.get_session() as session:
                success_count = 0
                update_count = 0

                for quote_data in quotes:
                    try:
                        # Check if record exists
                        existing = session.query(DailyQuoteDB).filter(
                            DailyQuoteDB.time == quote_data['time'],
                            DailyQuoteDB.instrument_id == quote_data['instrument_id']
                        ).first()

                        if existing:
                            # Update existing record
                            for key, value in quote_data.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                            existing.updated_at = get_shanghai_time()
                            update_count += 1
                        else:
                            # Create new record
                            db_quote = DailyQuoteDB(**quote_data)
                            session.add(db_quote)
                            success_count += 1

                    except Exception as e:
                        self.db_logger.error(f"Error saving quote {quote_data.get('instrument_id', 'unknown')} {quote_data.get('time', 'unknown')}: {e}")

                session.commit()
                self.db_logger.info(f"Successfully saved daily data: {success_count} new, {update_count} updated")
                return success_count > 0 or update_count > 0

        except Exception as e:
            self.db_logger.error(f"Failed to save daily data: {e}")
            return False

    async def get_daily_data(
        self,
        instrument_id: str = None,
        symbol: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        tradestatus: int = None,
        is_complete: bool = None,
        min_volume: int = None,
        limit: int = None,
        return_format: str = 'pandas'
    ) -> Union[pd.DataFrame, List[Dict]]:
        """获取日线数据"""
        try:
            with self.get_session() as session:
                query = session.query(DailyQuoteDB)

                # Apply instrument filter
                if instrument_id:
                    query = query.filter(DailyQuoteDB.instrument_id == instrument_id)
                elif symbol:
                    # Join with instruments table to get instrument_id from symbol
                    query = query.join(InstrumentDB).filter(InstrumentDB.symbol == symbol)

                # Apply date range filter
                if start_date:
                    query = query.filter(DailyQuoteDB.time >= start_date)
                if end_date:
                    query = query.filter(DailyQuoteDB.time <= end_date)

                # Apply other filters
                if tradestatus is not None:
                    query = query.filter(DailyQuoteDB.tradestatus == tradestatus)
                if is_complete is not None:
                    query = query.filter(DailyQuoteDB.is_complete == is_complete)
                if min_volume:
                    query = query.filter(DailyQuoteDB.volume >= min_volume)

                # Order and limit
                query = query.order_by(DailyQuoteDB.time.desc())
                if limit:
                    query = query.limit(limit)

                # Execute query
                results = []
                for quote in query:
                    results.append({
                        'time': quote.time,
                        'instrument_id': quote.instrument_id,
                        'open': quote.open,
                        'high': quote.high,
                        'low': quote.low,
                        'close': quote.close,
                        'volume': quote.volume,
                        'amount': quote.amount,
                        'turnover': quote.turnover,
                        'pre_close': quote.pre_close,
                        'change': quote.change,
                        'pct_change': quote.pct_change,
                        'tradestatus': quote.tradestatus,
                        'factor': quote.factor,
                        'adjustment_type': quote.adjustment_type,
                        'is_complete': quote.is_complete,
                        'quality_score': quote.quality_score,
                        'source': quote.source,
                        'batch_id': quote.batch_id,
                        'created_at': quote.created_at,
                        'updated_at': quote.updated_at
                    })

                # Convert to requested format
                if return_format == 'pandas':
                    return pd.DataFrame(results)
                else:
                    return results

        except Exception as e:
            self.db_logger.error(f"Failed to get daily data: {e}")
            return pd.DataFrame() if return_format == 'pandas' else []

    async def get_latest_quote_date(self, instrument_id: str) -> Optional[datetime]:
        """获取最新日期"""
        try:
            with self.get_session() as session:
                latest = session.query(DailyQuoteDB).filter(
                    DailyQuoteDB.instrument_id == instrument_id
                ).order_by(DailyQuoteDB.time.desc()).first()

                if latest:
                    return latest.time
                return None

        except Exception as e:
            self.db_logger.error(f"Failed to get latest quote date for {instrument_id}: {e}")
            return None

    # === Trading Calendar Operations ===

    async def save_trading_calendar(self, calendar_data: List[Dict[str, Any]]) -> bool:
        """保存交易日历数据"""
        try:
            with self.get_session() as session:
                success_count = 0
                for data in calendar_data:
                    try:
                        # Check if record exists
                        existing = session.query(TradingCalendarDB).filter(
                            TradingCalendarDB.exchange == data['exchange'],
                            TradingCalendarDB.date == data['date']
                        ).first()

                        if existing:
                            # Update existing record
                            for key, value in data.items():
                                if hasattr(existing, key):
                                    setattr(existing, key, value)
                            existing.updated_at = get_shanghai_time()
                        else:
                            # Create new record
                            db_calendar = TradingCalendarDB(**data)
                            session.add(db_calendar)
                        success_count += 1

                    except Exception as e:
                        self.db_logger.error(f"Error saving calendar record: {e}")

                session.commit()
                self.db_logger.info(f"Successfully saved {success_count} calendar records")
                return success_count

        except Exception as e:
            self.db_logger.error(f"Failed to save trading calendar: {e}")
            return False

    async def get_trading_days(
        self,
        exchange: str = None,
        start_date: Union[str, date] = None,
        end_date: Union[str, date] = None,
        is_trading_day: bool = None
    ) -> List[date]:
        """获取交易日列表"""
        try:
            with self.get_session() as session:
                query = session.query(TradingCalendarDB.date).distinct()

                # Apply filters
                if exchange:
                    query = query.filter(TradingCalendarDB.exchange == exchange)
                if start_date:
                    # Convert string to date if needed
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    # Convert date to datetime to match database storage format
                    start_datetime = datetime.combine(start_date, datetime.min.time())
                    query = query.filter(TradingCalendarDB.date >= start_datetime)
                if end_date:
                    # Convert string to date if needed
                    if isinstance(end_date, str):
                        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                    # Convert date to datetime to match database storage format
                    # Use end of day to ensure the end date is included
                    end_datetime = datetime.combine(end_date, datetime.max.time())
                    query = query.filter(TradingCalendarDB.date <= end_datetime)
                if is_trading_day is not None:
                    query = query.filter(TradingCalendarDB.is_trading_day == is_trading_day)

                query = query.order_by(TradingCalendarDB.date)

                trading_days = []
                for result in query:
                    if isinstance(result[0], datetime):
                        trading_days.append(result[0].date())
                    else:
                        trading_days.append(result[0])

                return trading_days

        except Exception as e:
            self.db_logger.error(f"Failed to get trading days: {e}")
            return []

    async def get_next_trading_day(self, exchange: str, check_date: Union[str, date]) -> Optional[date]:
        """获取下一个交易日"""
        try:
            # Convert string to date if needed
            if isinstance(check_date, str):
                check_date = datetime.strptime(check_date, '%Y-%m-%d').date()

            # Convert date to datetime to match database storage format
            check_datetime = datetime.combine(check_date, datetime.max.time())

            with self.get_session() as session:
                result = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date > check_datetime,
                    TradingCalendarDB.is_trading_day == True
                ).order_by(TradingCalendarDB.date).first()

                return result.date if result else None

        except Exception as e:
            self.db_logger.error(f"Failed to get next trading day for {exchange} {check_date}: {e}")
            return None

    async def get_previous_trading_day(self, exchange: str, check_date: Union[str, date]) -> Optional[date]:
        """获取上一个交易日"""
        try:
            # Convert string to date if needed
            if isinstance(check_date, str):
                check_date = datetime.strptime(check_date, '%Y-%m-%d').date()

            # Convert date to datetime to match database storage format
            check_datetime = datetime.combine(check_date, datetime.min.time())

            with self.get_session() as session:
                result = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date < check_datetime,
                    TradingCalendarDB.is_trading_day == True
                ).order_by(TradingCalendarDB.date.desc()).first()

                return result.date if result else None

        except Exception as e:
            self.db_logger.error(f"Failed to get previous trading day for {exchange} {check_date}: {e}")
            return None

    async def is_trading_day(self, exchange: str, check_date: Union[str, date]) -> bool:
        """检查指定日期是否为交易日"""
        try:
            # Convert string to date if needed
            if isinstance(check_date, str):
                check_date = datetime.strptime(check_date, '%Y-%m-%d').date()

            # For exact date matching, we need to check for any datetime on that day
            start_datetime = datetime.combine(check_date, datetime.min.time())
            end_datetime = datetime.combine(check_date, datetime.max.time())

            with self.get_session() as session:
                result = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date >= start_datetime,
                    TradingCalendarDB.date <= end_datetime
                ).first()

                return result.is_trading_day if result else False

        except Exception as e:
            self.db_logger.error(f"Failed to check trading day for {exchange} {check_date}: {e}")
            return False

    # === Data Update Operations ===

    async def create_data_update(self, update_info: Dict[str, Any]) -> str:
        """创建数据更新记录"""
        try:
            with self.get_session() as session:
                db_update = DataUpdateDB(**update_info)
                session.add(db_update)
                session.commit()
                session.refresh(db_update)
                return db_update.batch_id

        except Exception as e:
            self.db_logger.error(f"Failed to create data update record: {e}")
            return None

    async def update_data_update_progress(self, batch_id: str, progress: float, status: str = None) -> bool:
        """更新数据进度"""
        try:
            with self.get_session() as session:
                update = session.query(DataUpdateDB).filter(
                    DataUpdateDB.batch_id == batch_id
                ).first()

                if update:
                    update.progress = progress
                    if status:
                        update.status = status
                    if status == 'completed':
                        update.completed_at = get_shanghai_time()
                        update.duration_seconds = int((get_shanghai_time() - update.started_at).total_seconds())

                    session.commit()
                    return True
                return False

        except Exception as e:
            self.db_logger.error(f"Failed to update data update progress: {e}")
            return False

    async def get_data_updates(
        self,
        batch_id: str = None,
        status: str = None,
        limit: int = None
    ) -> List[Dict[str, Any]]:
        """获取数据更新记录"""
        try:
            with self.get_session() as session:
                query = session.query(DataUpdateDB).order_by(DataUpdateDB.created_at.desc())

                if batch_id:
                    query = query.filter(DataUpdateDB.batch_id == batch_id)
                if status:
                    query = query.filter(DataUpdateDB.status == status)
                if limit:
                    query = query.limit(limit)

                updates = []
                for update in query:
                    updates.append({
                        'update_id': update.update_id,
                        'batch_id': update.batch_id,
                        'update_type': update.update_type,
                        'target': update.target,
                        'exchange': update.exchange,
                        'start_date': update.start_date,
                        'end_date': update.end_date,
                        'total_instruments': update.total_instruments,
                        'processed_instruments': update.processed_instruments,
                        'new_records': update.new_records,
                        'updated_records': update.updated_records,
                        'error_records': update.error_records,
                        'status': update.status,
                        'progress': update.progress,
                        'error_message': update.error_message,
                        'started_at': update.started_at,
                        'completed_at': update.completed_at,
                        'duration_seconds': update.duration_seconds,
                        'created_at': update.created_at,
                        'updated_at': update.updated_at
                    })

                return updates

        except Exception as e:
            self.db_logger.error(f"Failed to get data updates: {e}")
            return []

    # === Statistics and Analysis ===

    async def get_database_statistics(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            stats = {}

            with self.get_session() as session:
                # Instruments statistics
                stats['instruments'] = {
                    'total': session.query(InstrumentDB).count(),
                    'active': session.query(InstrumentDB).filter(InstrumentDB.is_active == True).count(),
                    'by_exchange': {},
                    'by_type': {},
                    'by_status': {}
                }

                # Exchange distribution
                for exchange in ['SSE', 'SZSE', 'BSE']:
                    count = session.query(InstrumentDB).filter(InstrumentDB.exchange == exchange).count()
                    stats['instruments']['by_exchange'][exchange] = count

                # Type distribution
                for type_name in ['stock', 'etf', 'index', 'bond']:
                    count = session.query(InstrumentDB).filter(InstrumentDB.type == type_name).count()
                    stats['instruments']['by_type'][type_name] = count

                # Status distribution
                status_counts = session.query(
                    InstrumentDB.status, func.count(InstrumentDB.status)
                ).group_by(InstrumentDB.status).all()
                for status, count in status_counts:
                    stats['instruments']['by_status'][status] = count

                # Daily quotes statistics
                stats['daily_quotes'] = {
                    'total': session.query(DailyQuoteDB).count(),
                    'by_trading_status': {},
                    'by_source': {},
                    'latest_date': None,
                    'earliest_date': None
                }

                # Trading status distribution
                status_counts = session.query(
                    DailyQuoteDB.tradestatus, func.count(DailyQuoteDB.tradestatus)
                ).group_by(DailyQuoteDB.tradestatus).all()
                for status, count in status_counts:
                    stats['daily_quotes']['by_trading_status'][status] = count

                # Source distribution
                source_counts = session.query(
                    DailyQuoteDB.source, func.count(DailyQuoteDB.source)
                ).group_by(DailyQuoteDB.source).all()
                for source, count in source_counts:
                    stats['daily_quotes']['by_source'][source or 'unknown'] = count

                # Date range
                latest = session.query(func.max(DailyQuoteDB.time)).scalar()
                earliest = session.query(func.min(DailyQuoteDB.time)).scalar()
                stats['daily_quotes']['latest_date'] = latest
                stats['daily_quotes']['earliest_date'] = earliest

                # Trading calendar statistics
                stats['trading_calendar'] = {
                    'total_records': session.query(TradingCalendarDB).count(),
                    'trading_days': session.query(TradingCalendarDB).filter(TradingCalendarDB.is_trading_day == True).count(),
                    'by_exchange': {}
                }

                for exchange in ['SSE', 'SZSE', 'BSE']:
                    trading_days = session.query(TradingCalendarDB).filter(
                        TradingCalendarDB.exchange == exchange,
                        TradingCalendarDB.is_trading_day == True
                    ).count()
                    stats['trading_calendar']['by_exchange'][exchange] = trading_days

                # Data updates statistics
                stats['data_updates'] = {
                    'total': session.query(DataUpdateDB).count(),
                    'by_status': {},
                    'latest': None
                }

                # Status distribution
                status_counts = session.query(
                    DataUpdateDB.status, func.count(DataUpdateDB.status)
                ).group_by(DataUpdateDB.status).all()
                for status, count in status_counts:
                    stats['data_updates']['by_status'][status] = count

                # Latest update
                latest = session.query(DataUpdateDB).order_by(DataUpdateDB.created_at.desc()).first()
                if latest:
                    stats['data_updates']['latest'] = {
                        'batch_id': latest.batch_id,
                        'update_type': latest.update_type,
                        'status': latest.status,
                        'progress': latest.progress,
                        'created_at': latest.created_at
                    }

            return stats

        except Exception as e:
            self.db_logger.error(f"Failed to get database statistics: {e}")
            return {}

    # === Database Maintenance ===

    async def cleanup_old_data(self, days_to_keep: int = 365) -> bool:
        """清理旧数据"""
        try:
            cutoff_date = get_shanghai_time() - timedelta(days=days_to_keep)

            with self.get_session() as session:
                # Clean up old daily quotes
                quotes_deleted = session.query(DailyQuoteDB).filter(
                    DailyQuoteDB.time < cutoff_date
                ).delete()

                # Clean up old trading calendar data
                calendar_deleted = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.date < cutoff_date
                ).delete()

                # Clean up old data update records
                updates_deleted = session.query(DataUpdateDB).filter(
                    DataUpdateDB.created_at < cutoff_date
                ).delete()

                session.commit()
                self.db_logger.info(f"Cleaned up old data: {quotes_deleted} quotes, {calendar_deleted} calendar records, {updates_deleted} update records")
                return True

        except Exception as e:
            self.db_logger.error(f"Failed to cleanup old data: {e}")
            return False

    async def optimize_database(self) -> bool:
        """优化数据库"""
        try:
            with self.get_session() as session:
                session.execute(text("VACUUM"))
                session.execute(text("ANALYZE"))
                session.commit()
                self.db_logger.info("Database optimization completed")
                return True

        except Exception as e:
            self.db_logger.error(f"Failed to optimize database: {e}")
            return False

    async def assess_data_quality(self, instrument_id: str, start_date: date, end_date: date) -> float:
        """评估指定品种在指定日期范围内的数据质量"""
        try:
            with self.get_session() as session:
                query = session.query(DailyQuoteDB).filter(
                    DailyQuoteDB.instrument_id == instrument_id,
                    DailyQuoteDB.time >= start_date,
                    DailyQuoteDB.time <= end_date
                )

                total_records = query.count()
                if total_records == 0:
                    return 0.0

                # 计算质量评分
                quality_scores = []
                for quote in query:
                    score = 1.0

                    # 检查价格合理性
                    if quote.high < quote.low:
                        score -= 0.4
                    if quote.high < max(quote.open, quote.close):
                        score -= 0.2
                    if quote.low > min(quote.open, quote.close):
                        score -= 0.2

                    # 检查成交量
                    if quote.volume <= 0:
                        score -= 0.2

                    # 检查交易状态
                    if quote.tradestatus != 1:
                        score -= 0.3

                    # 使用已有的质量评分（如果有）
                    if quote.quality_score is not None:
                        score = quote.quality_score

                    quality_scores.append(max(0.0, score))

                # 返回平均质量评分
                return sum(quality_scores) / len(quality_scores)

        except Exception as e:
            self.db_logger.error(f"Failed to assess data quality for {instrument_id}: {e}")
            return 0.0

    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> bool:
        """执行SQL查询

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            bool: 执行是否成功
        """
        try:
            # 安全检查：只允许特定类型的SQL语句
            allowed_operations = [
                'VACUUM', 'ANALYZE', 'REINDEX', 'CHECK', 'PRAGMA',
                'CREATE INDEX', 'DROP INDEX', 'ALTER TABLE'
            ]

            query_upper = query.strip().upper()

            # 检查是否为允许的操作
            if not any(query_upper.startswith(op) for op in allowed_operations):
                self.db_logger.warning(f"Blocked potentially dangerous query: {query[:100]}...")
                return False

            with self.get_session() as session:
                if params:
                    session.execute(text(query), params)
                else:
                    session.execute(text(query))
                session.commit()

                self.db_logger.debug(f"Successfully executed query: {query[:100]}...")
                return True

        except Exception as e:
            self.db_logger.error(f"Failed to execute query '{query[:100]}...': {e}")
            return False

    async def validate_data_integrity(self) -> Dict[str, Any]:
        """验证数据完整性

        Returns:
            Dict: 验证结果，包含发现的问题数量和详细信息
        """
        try:
            self.db_logger.info("Starting data integrity validation...")

            validation_results = {
                'total_issues': 0,
                'issues': [],
                'warnings': [],
                'statistics': {},
                'validation_timestamp': get_shanghai_time()
            }

            with self.get_session() as session:
                # 1. 检查重复的行情记录
                duplicate_quotes = session.query(
                    DailyQuoteDB.instrument_id,
                    DailyQuoteDB.time,
                    func.count(DailyQuoteDB.time).label('count')
                ).group_by(
                    DailyQuoteDB.instrument_id,
                    DailyQuoteDB.time
                ).having(
                    func.count(DailyQuoteDB.time) > 1
                ).count()

                if duplicate_quotes > 0:
                    validation_results['issues'].append({
                        'type': 'duplicate_quotes',
                        'count': duplicate_quotes,
                        'severity': 'high',
                        'description': f'Found {duplicate_quotes} duplicate quote records'
                    })
                    validation_results['total_issues'] += duplicate_quotes

                # 2. 检查无效的价格数据
                invalid_prices = session.query(DailyQuoteDB).filter(
                    (DailyQuoteDB.high < DailyQuoteDB.low) |
                    (DailyQuoteDB.high < 0) |
                    (DailyQuoteDB.low < 0) |
                    (DailyQuoteDB.open < 0) |
                    (DailyQuoteDB.close < 0) |
                    (DailyQuoteDB.volume < 0) |
                    (DailyQuoteDB.amount < 0)
                ).count()

                if invalid_prices > 0:
                    validation_results['issues'].append({
                        'type': 'invalid_prices',
                        'count': invalid_prices,
                        'severity': 'high',
                        'description': f'Found {invalid_prices} records with invalid price/volume data'
                    })
                    validation_results['total_issues'] += invalid_prices

                # 3. 检查缺失的交易品种信息
                orphaned_quotes = session.query(DailyQuoteDB).outerjoin(
                    InstrumentDB, DailyQuoteDB.instrument_id == InstrumentDB.instrument_id
                ).filter(InstrumentDB.instrument_id.is_(None)).count()

                if orphaned_quotes > 0:
                    validation_results['issues'].append({
                        'type': 'orphaned_quotes',
                        'count': orphaned_quotes,
                        'severity': 'medium',
                        'description': f'Found {orphaned_quotes} quote records without corresponding instrument'
                    })
                    validation_results['total_issues'] += orphaned_quotes

                # 4. 检查数据质量评分低于阈值的记录
                low_quality_quotes = session.query(DailyQuoteDB).filter(
                    (DailyQuoteDB.quality_score < 0.5) |
                    (DailyQuoteDB.quality_score.is_(None))
                ).count()

                if low_quality_quotes > 0:
                    validation_results['warnings'].append({
                        'type': 'low_quality',
                        'count': low_quality_quotes,
                        'severity': 'low',
                        'description': f'Found {low_quality_quotes} records with low quality scores'
                    })

                # 5. 统计信息
                validation_results['statistics'] = {
                    'total_quotes': session.query(DailyQuoteDB).count(),
                    'total_instruments': session.query(InstrumentDB).count(),
                    'instruments_without_quotes': session.query(InstrumentDB).outerjoin(
                        DailyQuoteDB, InstrumentDB.instrument_id == DailyQuoteDB.instrument_id
                    ).filter(DailyQuoteDB.instrument_id.is_(None)).count(),
                    'trading_calendar_records': session.query(TradingCalendarDB).count(),
                    'data_update_records': session.query(DataUpdateDB).count()
                }

            # 记录验证结果
            if validation_results['total_issues'] > 0:
                self.db_logger.warning(f"Data integrity validation found {validation_results['total_issues']} issues")
            else:
                self.db_logger.info("Data integrity validation passed with no critical issues")

            return validation_results

        except Exception as e:
            self.db_logger.error(f"Failed to validate data integrity: {e}")
            return {
                'total_issues': 1,
                'issues': [{'type': 'validation_error', 'description': str(e)}],
                'warnings': [],
                'statistics': {},
                'validation_timestamp': get_shanghai_time()
            }

    async def backup_database(self, backup_path: str = None) -> bool:
        """备份数据库

        Args:
            backup_path: 备份文件路径

        Returns:
            bool: 备份是否成功
        """
        try:
            return await self.db.backup_database(backup_path)
        except Exception as e:
            self.db_logger.error(f"Failed to backup database: {e}")
            return False

    async def get_existing_data_dates_by_exchange(self, exchange: str, start_date: date, end_date: date) -> set:
        """获取指定交易所的已有数据日期集合

        Args:
            exchange: 交易所代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            set: 包含所有已有数据的日期集合
        """
        try:
            with self.get_session() as session:
                # 获取交易所的所有活跃交易品种
                instrument_ids = session.query(InstrumentDB.instrument_id).filter(
                    InstrumentDB.exchange == exchange,
                    InstrumentDB.is_active == True
                ).all()

                if not instrument_ids:
                    return set()

                instrument_ids = [item[0] for item in instrument_ids]

                # 查询这些品种在指定日期范围内的数据
                query = session.query(DailyQuoteDB.time).filter(
                    DailyQuoteDB.instrument_id.in_(instrument_ids),
                    DailyQuoteDB.time >= datetime.combine(start_date, datetime.min.time()),
                    DailyQuoteDB.time <= datetime.combine(end_date, datetime.max.time())
                ).distinct()

                data_dates = set()
                for result in query:
                    if isinstance(result[0], datetime):
                        data_dates.add(result[0].date())
                    else:
                        data_dates.add(result[0])

                return data_dates

        except Exception as e:
            self.db_logger.error(f"Failed to get existing data dates by exchange {exchange}: {e}")
            return set()

    async def get_trading_calendar_records(self, exchange: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """获取交易日历记录

        Args:
            exchange: 交易所代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[Dict]: 交易日历记录列表
        """
        try:
            with self.get_session() as session:
                query = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date >= datetime.combine(start_date, datetime.min.time()),
                    TradingCalendarDB.date <= datetime.combine(end_date, datetime.max.time())
                ).order_by(TradingCalendarDB.date)

                records = []
                for record in query:
                    records.append({
                        'exchange': record.exchange,
                        'date': record.date,
                        'is_trading_day': record.is_trading_day,
                        'reason': record.reason,
                        'session_type': record.session_type,
                        'source': record.source,
                        'created_at': record.created_at,
                        'updated_at': record.updated_at
                    })

                return records

        except Exception as e:
            self.db_logger.error(f"Failed to get trading calendar records for {exchange}: {e}")
            return []

    async def get_latest_calendar_record(self, exchange: str) -> Optional[Dict[str, Any]]:
        """获取最新的交易日历记录

        Args:
            exchange: 交易所代码

        Returns:
            Optional[Dict]: 最新记录或None
        """
        try:
            with self.get_session() as session:
                record = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange
                ).order_by(TradingCalendarDB.date.desc()).first()

                if not record:
                    return None

                return {
                    'exchange': record.exchange,
                    'date': record.date,
                    'is_trading_day': record.is_trading_day,
                    'reason': record.reason,
                    'session_type': record.session_type,
                    'source': record.source,
                    'created_at': record.created_at,
                    'updated_at': record.updated_at
                }

        except Exception as e:
            self.db_logger.error(f"Failed to get latest calendar record for {exchange}: {e}")
            return None

    async def get_calendar_statistics(self, exchange: str) -> Dict[str, Any]:
        """获取交易日历统计信息

        Args:
            exchange: 交易所代码

        Returns:
            Dict: 统计信息
        """
        try:
            with self.get_session() as session:
                # 总记录数
                total_days = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange
                ).count()

                # 交易日数
                trading_days = session.query(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.is_trading_day == True
                ).count()

                # 非交易日数
                non_trading_days = total_days - trading_days

                # 日期范围
                earliest = session.query(func.min(TradingCalendarDB.date)).filter(
                    TradingCalendarDB.exchange == exchange
                ).scalar()

                latest = session.query(func.max(TradingCalendarDB.date)).filter(
                    TradingCalendarDB.exchange == exchange
                ).scalar()

                # 最后更新时间
                last_updated = session.query(func.max(TradingCalendarDB.updated_at)).filter(
                    TradingCalendarDB.exchange == exchange
                ).scalar()

                return {
                    'total_days': total_days,
                    'trading_days': trading_days,
                    'non_trading_days': non_trading_days,
                    'date_range': {
                        'earliest': earliest,
                        'latest': latest
                    },
                    'last_updated': last_updated
                }

        except Exception as e:
            self.db_logger.error(f"Failed to get calendar statistics for {exchange}: {e}")
            return {}


# Global instance
database_operations = DatabaseOperations()