"""
API data models for the quote system.
Pydantic models for request/response validation with comprehensive features.
"""

from enum import Enum
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
from pydantic import BaseModel, Field, validator
from database.models import Instrument, DailyQuote, TradingCalendar


class ExchangeEnum(str, Enum):
    """交易所枚举"""
    SSE = "SSE"
    SZSE = "SZSE"
    BSE = "BSE"
    HKEX = "HKEX"
    NASDAQ = "NASDAQ"
    NYSE = "NYSE"


class InstrumentTypeEnum(str, Enum):
    """交易品种类型枚举"""
    STOCK = "STOCK"
    BOND = "BOND"
    ETF = "ETF"
    INDEX = "INDEX"
    FUND = "FUND"
    REIT = "REIT"
    WARRANT = "WARRANT"
    FUTURES = "FUTURES"


class InstrumentStatusEnum(str, Enum):
    """交易品种状态枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"


class AdjustmentTypeEnum(str, Enum):
    """复权类型枚举"""
    NONE = "none"
    FORWARD = "forward"
    BACKWARD = "backward"


class ReturnFormatEnum(str, Enum):
    """返回格式枚举"""
    PANDAS = "pandas"
    JSON = "json"
    CSV = "csv"


class InstrumentResponse(BaseModel):
    """增强交易品种信息响应模型"""
    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    name: str = Field(..., description="品种名称")
    exchange: ExchangeEnum = Field(..., description="交易所")
    type: InstrumentTypeEnum = Field(..., description="品种类型")
    currency: str = Field(..., description="交易货币")

    # 日期信息
    listed_date: Optional[datetime] = Field(None, description="上市日期")
    delisted_date: Optional[datetime] = Field(None, description="退市日期")
    issue_date: Optional[datetime] = Field(None, description="发行日期")

    # 股票特定信息
    industry: Optional[str] = Field(None, description="行业")
    sector: Optional[str] = Field(None, description="板块")
    market: Optional[str] = Field(None, description="市场")

    # 交易状态
    status: InstrumentStatusEnum = Field(InstrumentStatusEnum.ACTIVE, description="交易状态")
    is_active: bool = Field(True, description="是否活跃")
    is_st: bool = Field(False, description="是否ST股")
    trading_status: int = Field(1, description="交易状态码")

    
    # 源信息
    source: Optional[str] = Field(None, description="数据来源")
    source_symbol: Optional[str] = Field(None, description="数据源代码")

    # 元数据
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    data_version: int = Field(1, description="数据版本")

    class Config:
        from_attributes = True


class DailyQuoteResponse(BaseModel):
    """增强日线行情响应模型"""
    time: datetime = Field(..., description="时间")
    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")

    # 基本价格数据
    open: float = Field(..., description="开盘价", ge=0)
    high: float = Field(..., description="最高价", ge=0)
    low: float = Field(..., description="最低价", ge=0)
    close: float = Field(..., description="收盘价", ge=0)

    # 成交量和金额
    volume: int = Field(..., description="成交量", ge=0)
    amount: float = Field(..., description="成交额", ge=0)
    turnover: Optional[float] = Field(None, description="换手率", ge=0)

    # 价格变动
    pre_close: Optional[float] = Field(None, description="前收盘价")
    change: Optional[float] = Field(None, description="涨跌额")
    pct_change: Optional[float] = Field(None, description="涨跌幅")

    # 交易状态
    tradestatus: int = Field(1, description="交易状态 1=正常 0=停牌")

    # 复权信息
    factor: float = Field(1.0, description="复权因子", ge=0)
    adjustment_type: Optional[AdjustmentTypeEnum] = Field(None, description="复权类型")

    # 数据质量
    is_complete: bool = Field(True, description="数据是否完整")
    quality_score: float = Field(1.0, description="数据质量评分", ge=0, le=1)

    # 源信息
    source: Optional[str] = Field(None, description="数据来源")
    batch_id: Optional[str] = Field(None, description="批次ID")

    class Config:
        from_attributes = True


class TradingCalendarResponse(BaseModel):
    """交易日历响应模型"""
    id: int
    instrument_id: str
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


class DataGapResponse(BaseModel):
    """数据缺口响应模型"""
    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: ExchangeEnum = Field(..., description="交易所")
    gap_start: date = Field(..., description="缺口开始日期")
    gap_end: date = Field(..., description="缺口结束日期")
    gap_days: int = Field(..., description="缺口天数")
    gap_type: str = Field(..., description="缺口类型")
    severity: str = Field(..., description="严重程度")
    recommendation: str = Field(..., description="处理建议")


class DataQualityReportResponse(BaseModel):
    """数据质量报告响应模型"""
    batch_id: str = Field(..., description="批次ID")
    generated_at: datetime = Field(..., description="生成时间")
    total_instruments: int = Field(..., description="总品种数")
    total_quotes: int = Field(..., description="总行情数")
    quality_score: float = Field(..., description="整体质量评分")
    data_gaps_count: int = Field(..., description="数据缺口数量")
    quality_issues_count: int = Field(..., description="质量问题数量")

    # 按严重程度统计
    gaps_by_severity: Dict[str, int] = Field(..., description="按严重程度统计的缺口")

    # 按交易所统计
    gaps_by_exchange: Dict[str, int] = Field(..., description="按交易所统计的缺口")

    # 详细缺口列表
    gaps: List[DataGapResponse] = Field(..., description="详细缺口列表")


class QuoteQueryRequest(BaseModel):
    """增强行情查询请求模型"""
    instrument_id: Optional[str] = Field(None, description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[ExchangeEnum] = Field(None, description="交易所")

    # 日期范围
    start_date: datetime = Field(..., description="开始日期")
    end_date: datetime = Field(..., description="结束日期")

    # 过滤器
    tradestatus: Optional[int] = Field(None, description="交易状态过滤")
    is_complete: Optional[bool] = Field(None, description="数据完整性过滤")
    min_volume: Optional[int] = Field(None, description="最小成交量过滤")
    min_quality_score: Optional[float] = Field(None, description="最小质量评分", ge=0, le=1)
    include_suspended: bool = Field(False, description="是否包含停牌数据")

    # 响应格式
    return_format: ReturnFormatEnum = Field(ReturnFormatEnum.PANDAS, description="返回格式: pandas, json, csv")
    include_metadata: bool = Field(False, description="是否包含元数据")
    include_quality: bool = Field(True, description="是否包含质量信息")

    # 分页
    limit: Optional[int] = Field(None, description="限制返回记录数")
    offset: int = Field(0, description="偏移量")

    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError("End date must be after start date")
        return v

    @validator('min_quality_score')
    def validate_quality_score(cls, v):
        if v is not None and (v < 0 or v > 1):
            raise ValueError("Quality score must be between 0 and 1")
        return v


class InstrumentQueryRequest(BaseModel):
    """增强交易品种查询请求模型"""
    exchange: Optional[ExchangeEnum] = Field(None, description="交易所代码")
    type: Optional[InstrumentTypeEnum] = Field(None, description="品种类型")
    industry: Optional[str] = Field(None, description="行业")
    sector: Optional[str] = Field(None, description="板块")
    market: Optional[str] = Field(None, description="市场")
    status: Optional[InstrumentStatusEnum] = Field(None, description="状态")

    # 状态过滤
    is_active: Optional[bool] = Field(None, description="是否活跃")
    is_st: Optional[bool] = Field(None, description="是否ST股")
    trading_status: Optional[int] = Field(None, description="交易状态码")

    # 日期过滤
    listed_after: Optional[date] = Field(None, description="上市日期晚于")
    listed_before: Optional[date] = Field(None, description="上市日期早于")
    delisted_after: Optional[date] = Field(None, description="退市日期晚于")
    delisted_before: Optional[date] = Field(None, description="退市日期早于")

    # 分页和排序
    limit: int = Field(100, description="返回数量限制", ge=1, le=1000)
    offset: int = Field(0, description="偏移量")
    sort_by: str = Field("symbol", description="排序字段")
    sort_order: str = Field("asc", description="排序方向")

    @validator('sort_order')
    def validate_sort_order(cls, v):
        if v not in ['asc', 'desc']:
            raise ValueError("Sort order must be 'asc' or 'desc'")
        return v


class QuoteQueryResponse(BaseModel):
    """增强行情查询响应模型"""
    instrument_id: str
    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None

    # 数据
    data: List[Dict[str, Any]]
    total_records: int

    # 查询参数
    start_date: datetime
    end_date: datetime
    filters: Dict[str, Any] = {}

    # 响应信息
    format: str
    query_time: float = 0.0

    # 统计信息
    stats: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

    # 质量信息
    quality_summary: Optional[Dict[str, Any]] = None


class SystemStatusResponse(BaseModel):
    """增强系统状态响应模型"""
    data_manager: Dict[str, Any]
    database: Dict[str, Any]
    data_sources: Dict[str, bool]
    timestamp: datetime

    # 新增字段
    trading_calendar_status: Dict[str, Any] = Field(default_factory=dict)
    data_quality_metrics: Dict[str, Any] = Field(default_factory=dict)
    recent_gaps: List[Dict[str, Any]] = Field(default_factory=list)


class DownloadProgressResponse(BaseModel):
    """增强下载进度响应模型"""
    batch_id: str = Field(..., description="批次ID")
    total_instruments: int = Field(..., description="总品种数")
    processed_instruments: int = Field(..., description="已处理品种数")
    successful_downloads: int = Field(..., description="成功下载数")
    failed_downloads: int = Field(..., description="失败下载数")
    total_quotes: int = Field(..., description="总行情数")

    # 新增字段
    trading_days_processed: int = Field(..., description="已处理交易日数")
    total_trading_days: int = Field(..., description="总交易日数")
    data_gaps_detected: int = Field(..., description="检测到的数据缺口数")
    quality_issues: int = Field(..., description="质量问题数")

    # 进度指标
    progress_percentage: float = Field(..., description="进度百分比")
    success_rate: float = Field(..., description="成功率")
    quality_score: float = Field(..., description="数据质量评分")

    # 时间信息
    elapsed_time: str = Field(..., description="已用时间")
    estimated_remaining_time: Optional[str] = Field(None, description="预计剩余时间")

    # 当前状态
    current_exchange: str = Field(..., description="当前交易所")
    current_batch: int = Field(..., description="当前批次")
    total_batches: int = Field(..., description="总批次数")

    # 错误信息
    recent_errors: List[str] = Field(..., description="最近错误")


class BatchDownloadRequest(BaseModel):
    """增强批量下载请求模型"""
    exchanges: List[ExchangeEnum] = Field(..., description="交易所列表")

    # 日期范围选择
    start_date: Optional[date] = Field(None, description="开始日期")
    end_date: Optional[date] = Field(None, description="结束日期")
    years: Optional[List[int]] = Field(None, description="年份列表")

    # 下载选项
    precise_mode: bool = Field(True, description="精确模式（基于上市日期）")
    resume: bool = Field(True, description="续传模式")
    quality_threshold: float = Field(0.7, description="数据质量阈值", ge=0, le=1)

    # 批处理配置
    batch_size: int = Field(50, description="批次大小", ge=1, le=200)
    max_concurrent: int = Field(3, description="最大并发数", ge=1, le=10)

    # 交易状态过滤
    include_suspended: bool = Field(False, description="包含停牌股票")
    include_delisted: bool = Field(False, description="包含退市股票")

    @validator('years')
    def validate_years(cls, v):
        if v is not None:
            current_year = datetime.now().year
            for year in v:
                if year < 1990 or year > current_year + 1:
                    raise ValueError(f"Year {year} is out of valid range (1990-{current_year + 1})")
        return v

    @validator('end_date')
    def validate_date_range(cls, v, values):
        if v and 'start_date' in values and values['start_date'] and v < values['start_date']:
            raise ValueError("End date must be after start date")
        return v


class DataGapFillRequest(BaseModel):
    """数据缺口填补请求模型"""
    exchange: Optional[ExchangeEnum] = Field(None, description="交易所代码")
    instrument_ids: Optional[List[str]] = Field(None, description="指定股票ID列表")
    severity_filter: Optional[List[str]] = Field(None, description="严重程度过滤")
    gap_type_filter: Optional[List[str]] = Field(None, description="缺口类型过滤")
    max_gap_days: Optional[int] = Field(None, description="最大缺口天数")
    dry_run: bool = Field(False, description="试运行模式")

    @validator('severity_filter')
    def validate_severity_filter(cls, v):
        if v is not None:
            valid_severities = ['low', 'medium', 'high', 'critical']
            invalid_severities = [s for s in v if s not in valid_severities]
            if invalid_severities:
                raise ValueError(f"Invalid severities: {invalid_severities}. Valid: {valid_severities}")
        return v


class DataGapFillResponse(BaseModel):
    """数据缺口填补响应模型"""
    total_gaps_found: int = Field(..., description="发现的总缺口数")
    gaps_to_fill: int = Field(..., description="将要填补的缺口数")
    successfully_filled: int = Field(..., description="成功填补的缺口数")
    failed_to_fill: int = Field(..., description="填补失败的缺口数")
    dry_run: bool = Field(..., description="是否为试运行")
    processing_time: float = Field(..., description="处理时间（秒）")
    details: List[Dict[str, Any]] = Field(..., description="详细处理信息")


class TradingCalendarQueryRequest(BaseModel):
    """交易日历查询请求模型"""
    exchange: ExchangeEnum = Field(..., description="交易所代码")
    start_date: date = Field(..., description="开始日期")
    end_date: date = Field(..., description="结束日期")
    include_weekends: bool = Field(False, description="是否包含周末")
    session_type: Optional[str] = Field(None, description="交易时段类型")

    @validator('end_date')
    def validate_date_range(cls, v, values):
        if 'start_date' in values and v < values['start_date']:
            raise ValueError("End date must be after start date")
        return v


class DataStatsResponse(BaseModel):
    """增强数据统计响应模型"""
    instruments_count: int = Field(..., description="交易品种总数")
    quotes_count: int = Field(..., description="行情记录总数")
    trading_days_count: int = Field(..., description="交易日总数")

    # 日期范围
    quotes_date_range: Dict[str, datetime] = Field(..., description="行情日期范围")
    trading_calendar_range: Dict[str, datetime] = Field(..., description="交易日历范围")

    # 统计信息
    instruments_by_exchange: Dict[str, int] = Field(..., description="按交易所统计")
    instruments_by_type: Dict[str, int] = Field(..., description="按品种类型统计")
    instruments_by_industry: Dict[str, int] = Field(..., description="按行业统计")

    # 质量统计
    data_quality_summary: Dict[str, Any] = Field(..., description="数据质量摘要")
    gap_summary: Dict[str, Any] = Field(..., description="缺口摘要")

    # 更新信息
    recent_updates: List[Dict[str, Any]] = Field(..., description="最近更新记录")
    last_data_update: Optional[datetime] = Field(None, description="最后数据更新时间")


class DataValidationRequest(BaseModel):
    """数据验证请求模型"""
    exchange: Optional[ExchangeEnum] = Field(None, description="交易所代码")
    instrument_ids: Optional[List[str]] = Field(None, description="指定股票ID列表")
    start_date: date = Field(..., description="开始日期")
    end_date: date = Field(..., description="结束日期")
    validation_type: str = Field("completeness", description="验证类型")
    strict_mode: bool = Field(False, description="严格模式")

    @validator('validation_type')
    def validate_validation_type(cls, v):
        valid_types = ['completeness', 'quality', 'consistency', 'all']
        if v not in valid_types:
            raise ValueError(f"Validation type must be one of {valid_types}")
        return v


class DataValidationResponse(BaseModel):
    """数据验证响应模型"""
    validation_type: str = Field(..., description="验证类型")
    total_instruments_checked: int = Field(..., description="检查的总品种数")
    passed_validations: int = Field(..., description="通过验证的品种数")
    failed_validations: int = Field(..., description="未通过验证的品种数")

    # 详细结果
    validation_details: List[Dict[str, Any]] = Field(..., description="详细验证结果")
    quality_scores: Dict[str, float] = Field(..., description="质量评分")

    # 处理信息
    processing_time: float = Field(..., description="处理时间（秒）")
    validation_timestamp: datetime = Field(..., description="验证时间")