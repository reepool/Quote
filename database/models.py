"""
database models for the quote system.
Redesigned to support comprehensive stock information and trading status.
"""

from datetime import datetime, date
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, PrimaryKeyConstraint, Index, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
# 安全导入get_shanghai_time，避免循环引用问题
def safe_get_shanghai_time():
    """安全的get_shanghai_time调用"""
    try:
        from utils.date_utils import get_shanghai_time
        return get_shanghai_time()
    except (ImportError, ModuleNotFoundError):
        # 如果导入失败（例如循环导入），使用备用实现
        try:
            # 优先使用 zoneinfo (Python 3.9+)
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo("Asia/Shanghai"))
        except ImportError:
            # 兼容旧版本 Python
            from datetime import timezone, timedelta
            return datetime.now(timezone(timedelta(hours=8), 'Asia/Shanghai'))

Base = declarative_base()


class InstrumentDB(Base):
    """database model for trading instruments"""
    __tablename__ = 'instruments'

    # Primary fields
    instrument_id = Column(String(32), primary_key=True)
    symbol = Column(String(32), nullable=False, index=True)
    name = Column(String(128), nullable=False)
    exchange = Column(String(16), nullable=False, index=True)
    type = Column(String(16), nullable=False)  # stock, etf, index, etc.
    currency = Column(String(8), nullable=False)

    # Date information
    listed_date = Column(DateTime, nullable=True, index=True)  # 上市日期
    delisted_date = Column(DateTime, nullable=True, index=True)  # 退市日期
    issue_date = Column(DateTime, nullable=True)  # 发行日期

    # Stock specific information
    industry = Column(String(64), nullable=True, index=True)  # 行业
    sector = Column(String(64), nullable=True, index=True)  # 板块
    market = Column(String(16), nullable=True, index=True)  # 市场 (主板、创业板等)

    # Trading status
    status = Column(String(16), nullable=False, default='active', index=True)  # active, suspended, delisted
    is_active = Column(Boolean, default=True, index=True)  # 兼容性字段
    is_st = Column(Boolean, default=False, index=True)  # 是否ST股
    trading_status = Column(Integer, default=1, index=True)  # 1=正常交易, 0=停牌

    
    # Source information
    source = Column(String(32), nullable=True, index=True)  # 数据来源
    source_symbol = Column(String(32), nullable=True)  # 数据源中的代码

    # Metadata
    created_at = Column(DateTime, default=safe_get_shanghai_time)
    updated_at = Column(DateTime, default=safe_get_shanghai_time, onupdate=safe_get_shanghai_time)
    data_version = Column(Integer, default=1)  # 数据版本

    # Relationships
    market_data = relationship("DailyQuoteDB", back_populates="instrument")

    __table_args__ = (
        Index('idx_instruments_symbol_exchange', 'symbol', 'exchange'),
        Index('idx_instruments_industry', 'industry'),
        Index('idx_instruments_status', 'status', 'is_active'),
        Index('idx_instruments_trading_status', 'trading_status'),
        Index('idx_instruments_dates', 'listed_date', 'delisted_date'),
    )


class TradingCalendarDB(Base):
    """Trading calendar model for accurate trading days"""
    __tablename__ = 'trading_calendar'

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(16), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    is_trading_day = Column(Boolean, nullable=False, index=True)

    # Additional information
    reason = Column(String(128), nullable=True)  # 非交易日原因
    session_type = Column(String(32), nullable=True)  # 上午、下午、全天

    # Metadata
    source = Column(String(32), nullable=True)  # 数据来源
    created_at = Column(DateTime, default=safe_get_shanghai_time)
    updated_at = Column(DateTime, default=safe_get_shanghai_time, onupdate=safe_get_shanghai_time)

    __table_args__ = (
        Index('idx_trading_calendar_date', 'date'),
        Index('idx_trading_calendar_exchange', 'exchange', 'date'),
        Index('idx_trading_calendar_trading', 'is_trading_day', 'date'),
        PrimaryKeyConstraint('id'),
        # 添加交易所和日期的唯一约束
        UniqueConstraint('exchange', 'date', name='uq_trading_calendar_exchange_date')
    )


class DailyQuoteDB(Base):
    """daily quote model with comprehensive trading information"""
    __tablename__ = 'daily_quotes'

    # Primary keys
    time = Column(DateTime, nullable=False, index=True)
    instrument_id = Column(String(32), ForeignKey('instruments.instrument_id'), nullable=False, index=True)

    # Basic price data
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)

    # Volume and value
    volume = Column(Integer, nullable=False, index=True)
    amount = Column(Float, nullable=False, index=True)
    turnover = Column(Float, nullable=True, index=True)  # 换手率

    # Price changes
    pre_close = Column(Float, nullable=True)
    change = Column(Float, nullable=True)
    pct_change = Column(Float, nullable=True, index=True)  # 涨跌幅

    # Trading status
    tradestatus = Column(Integer, nullable=False, default=1, index=True)  # 1=正常交易, 0=停牌

    
    # Adjustment information
    factor = Column(Float, default=1.0, nullable=False)  # 复权因子
    adjustment_type = Column(String(16), default='none', nullable=True)  # forward, backward, none

    # Data quality flags
    is_complete = Column(Boolean, default=True, nullable=True)  # 数据是否完整
    quality_score = Column(Float, default=1.0, nullable=True)  # 数据质量评分

    # Source information
    source = Column(String(32), nullable=True, index=True)  # 数据来源

    # Metadata
    created_at = Column(DateTime, default=safe_get_shanghai_time)
    updated_at = Column(DateTime, default=safe_get_shanghai_time, onupdate=safe_get_shanghai_time)
    batch_id = Column(String(32), nullable=True, index=True)  # 批次ID

    # Relationships
    instrument = relationship("InstrumentDB", back_populates="market_data")

    __table_args__ = (
        PrimaryKeyConstraint('time', 'instrument_id'),
        Index('idx_daily_quotes_instrument_time', 'instrument_id', 'time'),
        Index('idx_daily_quotes_tradestatus', 'tradestatus'),
        Index('idx_daily_quotes_volume', 'volume'),
        Index('idx_daily_quotes_amount', 'amount'),
        Index('idx_daily_quotes_pct_change', 'pct_change'),
        Index('idx_daily_quotes_source', 'source', 'time'),
        Index('idx_daily_quotes_batch', 'batch_id'),
        Index('idx_daily_quotes_date', 'time'),
                Index('idx_daily_quotes_complete', 'is_complete'),
        Index('idx_daily_quotes_quality', 'quality_score'),
    )


class TradingSessionDB(Base):
    """Trading session model for intraday data"""
    __tablename__ = 'trading_sessions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    instrument_id = Column(String(32), ForeignKey('instruments.instrument_id'), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    session_type = Column(String(32), nullable=False, index=True)  # morning, afternoon, night

    # Session data
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)

    # Session status
    tradestatus = Column(Integer, nullable=False, default=1)
    is_suspended = Column(Boolean, default=False)

    # Metadata
    source = Column(String(32), nullable=True)
    created_at = Column(DateTime, default=safe_get_shanghai_time)
    updated_at = Column(DateTime, default=safe_get_shanghai_time, onupdate=safe_get_shanghai_time)

    __table_args__ = (
        Index('idx_trading_sessions_instrument_date', 'instrument_id', 'date'),
        Index('idx_trading_sessions_date', 'date'),
        PrimaryKeyConstraint('id'),
    )


class DataUpdateDB(Base):
    """data update record model"""
    __tablename__ = 'data_updates'

    update_id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(32), nullable=False, index=True)  # 批次ID

    # Update information
    update_type = Column(String(32), nullable=False, index=True)  # full, incremental, repair
    target = Column(String(32), nullable=False, index=True)  # instruments, quotes, sessions
    exchange = Column(String(16), nullable=False, index=True)

    # Date range
    start_date = Column(DateTime, nullable=False, index=True)
    end_date = Column(DateTime, nullable=False, index=True)

    # Statistics
    total_instruments = Column(Integer, nullable=False)
    processed_instruments = Column(Integer, nullable=False)
    new_records = Column(Integer, nullable=False)
    updated_records = Column(Integer, nullable=False)
    error_records = Column(Integer, nullable=False)

    # Status and metadata
    status = Column(String(16), nullable=False, index=True)  # pending, running, completed, failed
    error_message = Column(Text, nullable=True)
    progress = Column(Float, default=0.0, nullable=False)  # 进度百分比

    # Timing
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # Additional info
    config = Column(Text, nullable=True)  # 配置信息(JSON格式)
    source = Column(String(32), nullable=True)  # 数据源
    created_at = Column(DateTime, default=safe_get_shanghai_time)
    updated_at = Column(DateTime, default=safe_get_shanghai_time, onupdate=safe_get_shanghai_time)

    __table_args__ = (
        Index('idx_data_updates_batch_id', 'batch_id'),
        Index('idx_data_updates_status', 'status'),
        Index('idx_data_updates_dates', 'start_date', 'end_date'),
        PrimaryKeyConstraint('update_id'),
    )


class DataSourceStatusDB(Base):
    """Data source status tracking model"""
    __tablename__ = 'data_source_status'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(32), nullable=False, unique=True, index=True)

    # Status information
    is_active = Column(Boolean, default=True, nullable=False)
    is_available = Column(Boolean, default=True, nullable=False)
    last_check = Column(DateTime, nullable=True)

    # Statistics
    total_requests = Column(Integer, default=0, nullable=False)
    successful_requests = Column(Integer, default=0, nullable=False)
    failed_requests = Column(Integer, default=0, nullable=False)
    last_error = Column(Text, nullable=True)

    # Rate limiting
    requests_per_minute = Column(Integer, default=0, nullable=False)
    requests_per_hour = Column(Integer, default=0, nullable=False)
    requests_per_day = Column(Integer, default=0, nullable=False)

    # Configuration
    rate_limit_reset = Column(DateTime, nullable=True)
    config = Column(Text, nullable=True)  # 配置信息(JSON格式)

    # Metadata
    created_at = Column(DateTime, default=safe_get_shanghai_time)
    updated_at = Column(DateTime, default=safe_get_shanghai_time, onupdate=safe_get_shanghai_time)

    __table_args__ = (
        Index('idx_data_source_status_name', 'source_name'),
        Index('idx_data_source_status_active', 'is_active', 'is_available'),
        PrimaryKeyConstraint('id'),
    )


# Pydantic models for API
class Instrument(BaseModel):
    """trading instrument API model"""
    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    name: str = Field(..., description="品种名称")
    exchange: str = Field(..., description="交易所")
    type: str = Field(..., description="品种类型")
    currency: str = Field(..., description="交易货币")

    # Date information
    listed_date: Optional[datetime] = Field(None, description="上市日期")
    delisted_date: Optional[datetime] = Field(None, description="退市日期")
    issue_date: Optional[datetime] = Field(None, description="发行日期")

    # Stock specific information
    industry: Optional[str] = Field(None, description="行业")
    sector: Optional[str] = Field(None, description="板块")
    market: Optional[str] = Field(None, description="市场")

    # Trading status
    status: str = Field('active', description="交易状态")
    is_active: bool = Field(True, description="是否活跃")
    is_st: bool = Field(False, description="是否ST股")
    trading_status: int = Field(1, description="交易状态码 1=正常 0=停牌")

    
    # Source information
    source: Optional[str] = Field(None, description="数据来源")
    source_symbol: Optional[str] = Field(None, description="数据源代码")

    # Metadata
    created_at: datetime = Field(default_factory=safe_get_shanghai_time)
    updated_at: datetime = Field(default_factory=safe_get_shanghai_time)
    data_version: int = Field(1, description="数据版本")

    class Config:
        from_attributes = True


class DailyQuote(BaseModel):
    """daily quote API model"""
    time: datetime = Field(..., description="时间")
    instrument_id: str = Field(..., description="交易品种ID")

    # Basic price data
    open: float = Field(..., description="开盘价")
    high: float = Field(..., description="最高价")
    low: float = Field(..., description="最低价")
    close: float = Field(..., description="收盘价")

    # Volume and value
    volume: int = Field(..., description="成交量")
    amount: float = Field(..., description="成交额")
    turnover: Optional[float] = Field(None, description="换手率")

    # Price changes
    pre_close: Optional[float] = Field(None, description="前收盘价")
    change: Optional[float] = Field(None, description="涨跌额")
    pct_change: Optional[float] = Field(None, description="涨跌幅")

    # Trading status
    tradestatus: int = Field(1, description="交易状态 1=正常 0=停牌")

    
    # Data quality
    is_complete: bool = Field(True, description="数据是否完整")
    quality_score: float = Field(1.0, description="数据质量评分")

    # Adjustment information
    factor: float = Field(1.0, description="复权因子")
    adjustment_type: Optional[str] = Field(None, description="复权类型")

    # Source information
    source: Optional[str] = Field(None, description="数据来源")
    batch_id: Optional[str] = Field(None, description="批次ID")

    # Metadata
    created_at: datetime = Field(default_factory=safe_get_shanghai_time)
    updated_at: datetime = Field(default_factory=safe_get_shanghai_time)

    class Config:
        from_attributes = True


class TradingCalendar(BaseModel):
    """Trading calendar API model"""
    id: int
    exchange: str
    date: datetime
    is_trading_day: bool
    reason: Optional[str] = None
    session_type: Optional[str] = None
    source: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class QuoteQueryRequest(BaseModel):
    """quote query request model"""
    instrument_id: Optional[str] = Field(None, description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[str] = Field(None, description="交易所")

    # Date range
    start_date: datetime = Field(..., description="开始日期")
    end_date: datetime = Field(..., description="结束日期")

    # Filters
    tradestatus: Optional[int] = Field(None, description="交易状态过滤")
    is_complete: Optional[bool] = Field(None, description="数据完整性过滤")
    min_volume: Optional[int] = Field(None, description="最小成交量过滤")

    # Response format
    return_format: str = Field("pandas", description="返回格式: pandas, json, csv")
    include_metadata: bool = Field(False, description="是否包含元数据")

    # Pagination
    limit: Optional[int] = Field(None, description="限制返回记录数")
    offset: Optional[int] = Field(0, description="偏移量")


class QuoteQueryResponse(BaseModel):
    """quote query response model"""
    instrument_id: str
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None

    # Data
    data: list
    total_records: int

    # Query parameters
    start_date: datetime
    end_date: datetime
    filters: Dict[str, Any] = {}

    # Response info
    format: str
    query_time: float = 0.0

    # Statistics
    stats: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


class DataUpdateInfo(BaseModel):
    """Data update information model"""
    update_id: int
    batch_id: str
    update_type: str
    target: str
    exchange: str

    # Date range
    start_date: datetime
    end_date: datetime

    # Statistics
    total_instruments: int
    processed_instruments: int
    new_records: int
    updated_records: int
    error_records: int

    # Status
    status: str
    progress: float
    error_message: Optional[str] = None

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None

    # Metadata
    created_at: datetime
    updated_at: datetime