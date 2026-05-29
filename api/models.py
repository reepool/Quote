"""
API data models for the quote system.
Pydantic models for request/response validation with comprehensive features.
"""

from enum import Enum
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, date
from pydantic import BaseModel, Field, field_validator, model_validator
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

    @field_validator('type', mode='before')
    @classmethod
    def normalize_instrument_type(cls, v):
        if isinstance(v, str):
            return v.upper()
        return v

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

    @model_validator(mode='after')
    def validate_date_range(self) -> 'QuoteQueryRequest':
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("End date must be after start date")
        return self

    @field_validator('start_date', mode='before')
    @classmethod
    def parse_start_date(cls, v):
        if isinstance(v, str) and len(v) == 10:
            return datetime.fromisoformat(v)
        return v

    @field_validator('end_date', mode='before')
    @classmethod
    def parse_end_date(cls, v):
        if isinstance(v, str) and len(v) == 10:
            return datetime.fromisoformat(v)
        return v

    @field_validator('min_quality_score')
    @classmethod
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

    @field_validator('sort_order')
    @classmethod
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

    @field_validator('years')
    @classmethod
    def validate_years(cls, v):
        if v is not None:
            current_year = datetime.now().year
            for year in v:
                if year < 1990 or year > current_year + 1:
                    raise ValueError(f"Year {year} is out of valid range (1990-{current_year + 1})")
        return v

    @model_validator(mode='after')
    def validate_date_range(self) -> 'BatchDownloadRequest':
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("End date must be after start date")
        return self


class DataGapFillRequest(BaseModel):
    """数据缺口填补请求模型"""
    exchange: Optional[ExchangeEnum] = Field(None, description="交易所代码")
    instrument_ids: Optional[List[str]] = Field(None, description="指定股票ID列表")
    severity_filter: Optional[List[str]] = Field(None, description="严重程度过滤")
    gap_type_filter: Optional[List[str]] = Field(None, description="缺口类型过滤")
    max_gap_days: Optional[int] = Field(None, description="最大缺口天数")
    dry_run: bool = Field(False, description="试运行模式")

    @field_validator('severity_filter')
    @classmethod
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

    @model_validator(mode='after')
    def validate_date_range(self) -> 'TradingCalendarQueryRequest':
        if self.start_date and self.end_date and self.end_date < self.start_date:
            raise ValueError("End date must be after start date")
        return self


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

    @field_validator('validation_type')
    @classmethod
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


class ResearchCompanyProfileResponse(BaseModel):
    """研究域公司档案响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    company_name: str = Field(..., description="公司全称")
    short_name: str = Field(..., description="公司简称")
    exchange: str = Field(..., description="交易所")
    market: Optional[str] = Field(None, description="市场标识")
    listed_date: Optional[str] = Field(None, description="上市日期")
    industry_raw: Optional[str] = Field(None, description="原始行业字段")
    sector_raw: Optional[str] = Field(None, description="原始板块字段")
    status: Optional[str] = Field(None, description="上市状态")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    profile: Optional[Dict[str, Any]] = Field(None, description="标准化快照详情")


class ResearchFinancialSummaryResponse(BaseModel):
    """研究域财务摘要响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    report_date: Optional[str] = Field(None, description="报告期")
    pub_date: Optional[str] = Field(None, description="披露日期")
    fiscal_year: Optional[int] = Field(None, description="财年")
    fiscal_quarter: Optional[int] = Field(None, description="财季")
    currency: str = Field(..., description="货币")
    schema_version: str = Field(..., description="摘要schema版本")
    roe: Optional[float] = Field(None, description="净资产收益率")
    gross_margin: Optional[float] = Field(None, description="毛利率")
    net_margin: Optional[float] = Field(None, description="净利率")
    current_ratio: Optional[float] = Field(None, description="流动比率")
    quick_ratio: Optional[float] = Field(None, description="速动比率")
    liability_to_asset: Optional[float] = Field(None, description="资产负债率")
    yoy_asset: Optional[float] = Field(None, description="总资产同比")
    yoy_equity: Optional[float] = Field(None, description="净资产同比")
    yoy_net_profit: Optional[float] = Field(None, description="净利润同比")
    cfo_to_revenue: Optional[float] = Field(None, description="经营现金流/营业收入")
    cfo_to_net_profit: Optional[float] = Field(None, description="经营现金流/净利润")
    asset_turnover: Optional[float] = Field(None, description="总资产周转率")
    eps: Optional[float] = Field(None, description="每股收益")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    summary: Optional[Dict[str, Any]] = Field(None, description="标准化摘要详情")


class ResearchShareholderSnapshotResponse(BaseModel):
    """研究域股东摘要快照响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    coverage_status: str = Field(..., description="覆盖语义")
    holder_count: Optional[int] = Field(None, description="最新股东户数")
    holder_count_report_date: Optional[str] = Field(None, description="股东户数对应日期")
    top_holders_report_date: Optional[str] = Field(None, description="前十大股东对应日期")
    top_holders_count: Optional[int] = Field(None, description="已入库的前十大股东数量")
    top_holders_total_ratio: Optional[float] = Field(None, description="前十大合计持股比例")
    control_owner_name: Optional[str] = Field(None, description="控股线索名称")
    control_owner_ratio: Optional[float] = Field(None, description="控股线索持股比例")
    schema_version: str = Field(..., description="schema版本")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    snapshot: Optional[Dict[str, Any]] = Field(None, description="股东摘要详情")


class ResearchShareholderExchangeCoverageResponse(BaseModel):
    """股东域按交易所覆盖情况。"""

    exchange: str = Field(..., description="交易所")
    target_instruments: int = Field(..., description="目标股票数量")
    snapshot_count: int = Field(..., description="已落库股东快照数量")
    coverage_ratio: float = Field(..., description="覆盖率")
    ready: bool = Field(..., description="该交易所是否满足覆盖要求")


class ResearchShareholderScopeCoverageResponse(BaseModel):
    """股东域按 required scope 的覆盖情况。"""

    scope: str = Field(..., description="股东摘要字段范围")
    target_instruments: int = Field(..., description="目标股票数量")
    snapshot_count: int = Field(..., description="该 scope 已覆盖的股票数量")
    coverage_ratio: float = Field(..., description="scope 覆盖率")
    ready: bool = Field(..., description="该 scope 是否满足覆盖要求")


class ResearchShareholderReadinessResponse(BaseModel):
    """研究域股东域 rollout readiness 响应模型。"""

    generated_at: datetime = Field(..., description="状态生成时间")
    markets: List[str] = Field(default_factory=list, description="目标市场列表")
    module_enabled: bool = Field(..., description="股东模块是否启用")
    delivery_mode: str = Field(..., description="当前交付模式")
    snapshot_api_requires_mode: Optional[str] = Field(
        None,
        description="开放正式 snapshot API 所需模式",
    )
    snapshot_api_enabled: bool = Field(..., description="当前 snapshot API 门禁是否满足")
    target_instrument_count: int = Field(..., description="目标股票池总量")
    target_instruments_by_exchange: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的目标股票池数量",
    )
    snapshot_total: int = Field(..., description="已落库股东快照总量")
    missing_snapshot_count: int = Field(..., description="仍缺失的股东快照数量")
    required_scope: List[str] = Field(
        default_factory=list,
        description="rollout 期望覆盖的股东摘要字段范围",
    )
    coverage_status_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 coverage_status 统计",
    )
    source_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 source 统计",
    )
    source_mode_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 source_mode 统计",
    )
    scope_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 coverage_scope 统计",
    )
    latest_updated_at: Optional[datetime] = Field(None, description="最近更新时间")
    latest_data_as_of: Optional[datetime] = Field(None, description="最近数据时间")
    exchange_coverage: List[ResearchShareholderExchangeCoverageResponse] = Field(
        default_factory=list,
        description="按交易所统计的覆盖情况",
    )
    scope_coverage: List[ResearchShareholderScopeCoverageResponse] = Field(
        default_factory=list,
        description="按 required scope 统计的覆盖情况",
    )
    ready_for_paid_high_availability_rollout: bool = Field(
        ...,
        description="是否满足 paid_high_availability rollout 条件",
    )
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")


class ResearchFinancialStatementRawResponse(BaseModel):
    """研究域原始财务报表响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    statement_type: str = Field(..., description="报表类型")
    report_period: str = Field(..., description="报告期")
    publish_date: Optional[str] = Field(None, description="披露日期")
    fiscal_year: Optional[int] = Field(None, description="财年")
    fiscal_quarter: Optional[int] = Field(None, description="财季")
    currency: str = Field(..., description="货币")
    schema_version: str = Field(..., description="原始报表schema版本")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    statement: Optional[Dict[str, Any]] = Field(None, description="原始报表详情")


class ResearchFinancialIndicatorSnapshotResponse(BaseModel):
    """研究域财务指标快照响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    report_period: str = Field(..., description="报告期")
    gross_margin: Optional[float] = Field(None, description="毛利率")
    operating_margin: Optional[float] = Field(None, description="营业利润率")
    net_margin: Optional[float] = Field(None, description="净利率")
    roe: Optional[float] = Field(None, description="净资产收益率")
    roa: Optional[float] = Field(None, description="总资产收益率")
    current_ratio: Optional[float] = Field(None, description="流动比率")
    quick_ratio: Optional[float] = Field(None, description="速动比率")
    asset_liability_ratio: Optional[float] = Field(None, description="资产负债率")
    revenue_per_share: Optional[float] = Field(None, description="每股营业收入")
    operating_cf_to_revenue: Optional[float] = Field(None, description="经营现金流/营业收入")
    operating_cf_to_net_income: Optional[float] = Field(None, description="经营现金流/净利润")
    book_value_per_share: Optional[float] = Field(None, description="每股净资产")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    details: Optional[Dict[str, Any]] = Field(None, description="指标计算详情")


class ResearchFinancialStatementsResponse(BaseModel):
    """研究域完整财务报表组合读模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    report_period: str = Field(..., description="报告期")
    publish_date: Optional[str] = Field(None, description="披露日期")
    fiscal_year: Optional[int] = Field(None, description="财年")
    fiscal_quarter: Optional[int] = Field(None, description="财季")
    currency: str = Field(..., description="货币")
    schema_version: str = Field(..., description="规范化事实schema版本")
    revenue: Optional[float] = Field(None, description="营业收入")
    gross_profit: Optional[float] = Field(None, description="毛利润")
    operating_profit: Optional[float] = Field(None, description="营业利润")
    pre_tax_profit: Optional[float] = Field(None, description="利润总额")
    net_income: Optional[float] = Field(None, description="净利润")
    operating_cf: Optional[float] = Field(None, description="经营活动现金流净额")
    total_cf: Optional[float] = Field(None, description="现金及现金等价物净增加额")
    total_assets: Optional[float] = Field(None, description="总资产")
    total_liabilities: Optional[float] = Field(None, description="总负债")
    equity: Optional[float] = Field(None, description="净资产")
    current_assets: Optional[float] = Field(None, description="流动资产")
    current_liabilities: Optional[float] = Field(None, description="流动负债")
    inventory: Optional[float] = Field(None, description="存货")
    receivables: Optional[float] = Field(None, description="应收账款")
    fixed_assets: Optional[float] = Field(None, description="固定资产")
    intangible_assets: Optional[float] = Field(None, description="无形资产")
    shares_outstanding: Optional[float] = Field(None, description="总股本")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    facts: Optional[Dict[str, Any]] = Field(None, description="财务事实明细")
    indicators: Optional[ResearchFinancialIndicatorSnapshotResponse] = Field(
        None,
        description="派生财务指标快照",
    )
    statements: List[ResearchFinancialStatementRawResponse] = Field(
        default_factory=list,
        description="原始报表分项",
    )
    service_layers: Optional[Dict[str, Any]] = Field(
        None,
        description="可选财务服务分层结果，包括 L1 本地核心、L1.5 行业专项和显式 L3 远程扩展诊断",
    )


class ResearchFinancialStatementsHistoryResponse(BaseModel):
    """研究域多报告期财务报表读模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    period_window: str = Field(..., description="报告期窗口模式")
    rolling_quarters: int = Field(..., description="最近报告期数量上限")
    requested_report_periods: List[str] = Field(
        default_factory=list,
        description="显式请求的报告期",
    )
    report_periods: List[str] = Field(
        default_factory=list,
        description="实际返回的报告期",
    )
    period_count: int = Field(..., description="返回报告期数量")
    items: List[ResearchFinancialStatementsResponse] = Field(
        default_factory=list,
        description="逐报告期财务报表数据",
    )


class ResearchValuationHistoryItemResponse(BaseModel):
    """研究域估值历史单点响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    as_of_date: str = Field(..., description="估值日期")
    currency: str = Field(..., description="货币")
    close_price: Optional[float] = Field(None, description="收盘价")
    market_cap: Optional[float] = Field(None, description="估算总市值")
    float_market_cap: Optional[float] = Field(None, description="估算流通市值")
    pe_ratio: Optional[float] = Field(None, description="市盈率")
    pb_ratio: Optional[float] = Field(None, description="市净率")
    ps_ratio: Optional[float] = Field(None, description="市销率")
    pe_static: Optional[float] = Field(None, description="静态市盈率")
    pe_ttm: Optional[float] = Field(None, description="TTM 市盈率")
    pe_forward: Optional[float] = Field(None, description="前瞻市盈率")
    pb_mrq: Optional[float] = Field(None, description="最近报告期市净率")
    ps_static: Optional[float] = Field(None, description="静态市销率")
    ps_ttm: Optional[float] = Field(None, description="TTM 市销率")
    ps_forward: Optional[float] = Field(None, description="前瞻市销率")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    source: str = Field(..., description="来源标识")
    source_mode: str = Field(..., description="来源模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    details: Optional[Dict[str, Any]] = Field(None, description="估值计算详情")


class ResearchValuationHistoryResponse(BaseModel):
    """研究域估值历史响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    data_points: int = Field(..., description="时间序列点数")
    window_start: str = Field(..., description="窗口起点")
    window_end: str = Field(..., description="窗口终点")
    items: List[ResearchValuationHistoryItemResponse] = Field(
        default_factory=list,
        description="估值历史序列",
    )


class ResearchRelativeValuationPeerResponse(BaseModel):
    """研究域同行估值行响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    as_of_date: Optional[str] = Field(None, description="估值日期")
    close_price: Optional[float] = Field(None, description="收盘价")
    market_cap: Optional[float] = Field(None, description="估算总市值")
    float_market_cap: Optional[float] = Field(None, description="估算流通市值")
    pe_ratio: Optional[float] = Field(None, description="市盈率")
    pb_ratio: Optional[float] = Field(None, description="市净率")
    ps_ratio: Optional[float] = Field(None, description="市销率")
    pe_static: Optional[float] = Field(None, description="静态市盈率")
    pe_ttm: Optional[float] = Field(None, description="TTM 市盈率")
    pe_forward: Optional[float] = Field(None, description="前瞻市盈率")
    pb_mrq: Optional[float] = Field(None, description="最近报告期市净率")
    ps_static: Optional[float] = Field(None, description="静态市销率")
    ps_ttm: Optional[float] = Field(None, description="TTM 市销率")
    ps_forward: Optional[float] = Field(None, description="前瞻市销率")
    data_as_of: Optional[datetime] = Field(None, description="数据快照时间")


class ResearchRelativeValuationMetricResponse(BaseModel):
    """研究域相对估值单指标摘要。"""

    subject_value: Optional[float] = Field(None, description="标的当前值")
    peer_mean: Optional[float] = Field(None, description="同行均值")
    peer_median: Optional[float] = Field(None, description="同行中位数")
    peer_min: Optional[float] = Field(None, description="同行最小值")
    peer_max: Optional[float] = Field(None, description="同行最大值")
    peer_p25: Optional[float] = Field(None, description="同行 25 分位")
    peer_p75: Optional[float] = Field(None, description="同行 75 分位")
    valid_peer_count: Optional[int] = Field(None, description="有效同行样本数")
    excluded_peer_count: Optional[int] = Field(None, description="被排除同行样本数")
    percentile_rank: Optional[float] = Field(None, description="标的在同行中的分位排名")
    premium_to_median: Optional[float] = Field(None, description="相对中位数溢价率")


class ResearchRelativeValuationResponse(BaseModel):
    """研究域相对估值响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    status: str = Field(..., description="计算状态")
    missing_reason: Optional[str] = Field(None, description="缺失原因")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    benchmark_taxonomy_system: Optional[str] = Field(None, description="基准行业体系")
    benchmark_taxonomy_version: Optional[str] = Field(None, description="基准行业版本")
    benchmark_level: Optional[int] = Field(None, description="基准行业层级")
    benchmark_field: Optional[str] = Field(None, description="基准行业字段")
    benchmark_code: Optional[str] = Field(None, description="基准行业编码")
    benchmark_name: Optional[str] = Field(None, description="基准行业名称")
    benchmark_sw_l2_code: Optional[str] = Field(None, description="申万二级行业编码")
    benchmark_sw_l2_name: Optional[str] = Field(None, description="申万二级行业名称")
    peer_count: int = Field(..., description="同行数量")
    subject_valuation: Optional[ResearchRelativeValuationPeerResponse] = Field(
        None,
        description="标的当前估值",
    )
    benchmark_summary: Dict[str, Optional[ResearchRelativeValuationMetricResponse]] = Field(
        default_factory=dict,
        description="同行基准摘要",
    )
    metric_variants: List[str] = Field(
        default_factory=list,
        description="本次计算使用的估值指标口径",
    )
    diagnostics: Dict[str, Any] = Field(
        default_factory=dict,
        description="相对估值诊断信息",
    )
    industry_index_benchmark: Optional[Dict[str, Any]] = Field(
        None,
        description="申万官方指数分析 benchmark，不参与同行估值分布计算",
    )
    peers: List[ResearchRelativeValuationPeerResponse] = Field(
        default_factory=list,
        description="同行样本",
    )
    data_as_of: Optional[datetime] = Field(None, description="数据快照时间")


class ResearchValuationExchangeCoverageResponse(BaseModel):
    """估值历史按交易所覆盖情况。"""

    exchange: str = Field(..., description="交易所")
    target_instruments: int = Field(..., description="目标股票数量")
    valuation_history_count: int = Field(..., description="已有估值历史的股票数量")
    coverage_ratio: float = Field(..., description="覆盖率")
    ready: bool = Field(..., description="该交易所是否满足覆盖要求")


class ResearchValuationRelativeReadinessResponse(BaseModel):
    """估值域内相对估值 readiness 摘要。"""

    require_authoritative: bool = Field(..., description="是否要求 authoritative 行业归属")
    benchmark_level: int = Field(..., description="相对估值比较层级")
    benchmark_field: str = Field(..., description="相对估值比较字段")
    ready: bool = Field(..., description="是否满足相对估值 rollout 条件")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")
    industry_standard_ready: bool = Field(..., description="行业标准层是否满足要求")
    industry_standard_error: Optional[str] = Field(
        None,
        description="行业 readiness 聚合失败原因",
    )


class ResearchValuationReadinessResponse(BaseModel):
    """研究域估值 rollout readiness 响应模型。"""

    generated_at: datetime = Field(..., description="状态生成时间")
    markets: List[str] = Field(default_factory=list, description="目标市场列表")
    module_enabled: bool = Field(..., description="估值模块是否启用")
    target_instrument_count: int = Field(..., description="目标股票池总量")
    target_instruments_by_exchange: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的目标股票池数量",
    )
    valuation_history_total: int = Field(..., description="已有估值历史的股票总量")
    missing_valuation_history_count: int = Field(
        ...,
        description="仍缺失估值历史的股票数量",
    )
    valuation_input_total: int = Field(0, description="已有可用估值输入的股票数量")
    missing_valuation_input_count: int = Field(
        0,
        description="仍缺失估值输入的股票数量",
    )
    valuation_inputs: Dict[str, Any] = Field(
        default_factory=dict,
        description="估值输入覆盖与来源摘要",
    )
    valuation_storage: Dict[str, Any] = Field(
        default_factory=dict,
        description="估值域物理存储摘要",
    )
    source_counts: Dict[str, int] = Field(default_factory=dict, description="按 source 统计")
    source_mode_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 source_mode 统计",
    )
    calc_method_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按计算方法统计",
    )
    calc_version_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按计算版本统计",
    )
    metric_coverage: Dict[str, Any] = Field(
        default_factory=dict,
        description="估值指标口径覆盖情况",
    )
    latest_as_of_date: Optional[str] = Field(None, description="最近估值日期")
    latest_updated_at: Optional[datetime] = Field(None, description="最近更新时间")
    latest_data_as_of: Optional[datetime] = Field(None, description="最近数据时间")
    exchange_coverage: List[ResearchValuationExchangeCoverageResponse] = Field(
        default_factory=list,
        description="按交易所统计的估值历史覆盖情况",
    )
    relative_valuation: ResearchValuationRelativeReadinessResponse = Field(
        ...,
        description="相对估值 rollout readiness",
    )
    financial_statements: Optional[Dict[str, Any]] = Field(
        None,
        description="财务报表 readiness 摘要",
    )
    ready_for_rollout: bool = Field(..., description="估值模块是否满足 rollout 条件")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")


class ResearchFinancialStatementsReadinessResponse(BaseModel):
    """研究域财务报表 rollout readiness 响应模型。"""

    generated_at: datetime = Field(..., description="状态生成时间")
    markets: List[str] = Field(default_factory=list, description="目标市场列表")
    module_enabled: bool = Field(..., description="财务报表模块是否启用")
    target_instrument_count: int = Field(..., description="目标股票池总量")
    target_instruments_by_exchange: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的目标股票池数量",
    )
    expected_report_periods: List[str] = Field(
        default_factory=list,
        description="应覆盖的报告期",
    )
    readiness: Dict[str, Any] = Field(
        default_factory=dict,
        description="存储层 readiness 明细",
    )
    ready_for_rollout: bool = Field(..., description="是否满足 rollout 条件")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")


class ResearchMetadataExchangeCoverageResponse(BaseModel):
    """研究元数据按交易所覆盖情况。"""

    exchange: str = Field(..., description="交易所")
    target_instruments: int = Field(..., description="目标股票数量")
    instrument_count: int = Field(..., description="已覆盖股票数量")
    coverage_ratio: float = Field(..., description="覆盖率")
    ready: bool = Field(..., description="该交易所是否满足覆盖要求")


class ResearchMetadataDomainReadinessResponse(BaseModel):
    """单个研究元数据域 rollout readiness。"""

    domain: str = Field(..., description="研究元数据域")
    module_enabled: bool = Field(..., description="模块是否启用")
    target_instrument_count: int = Field(..., description="目标股票池总量")
    target_instruments_by_exchange: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的目标股票池数量",
    )
    instrument_total: int = Field(..., description="已覆盖股票数量")
    row_total: int = Field(..., description="已落库记录总数")
    missing_instrument_count: int = Field(..., description="仍缺失的股票数量")
    source_counts: Dict[str, int] = Field(default_factory=dict, description="按 source 统计")
    source_mode_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 source_mode 统计",
    )
    extra_counts: Dict[str, Dict[str, int]] = Field(
        default_factory=dict,
        description="域内额外分布统计",
    )
    latest_item_date: Optional[str] = Field(None, description="最近业务日期")
    latest_updated_at: Optional[datetime] = Field(None, description="最近更新时间")
    latest_data_as_of: Optional[datetime] = Field(None, description="最近数据时间")
    exchange_coverage: List[ResearchMetadataExchangeCoverageResponse] = Field(
        default_factory=list,
        description="按交易所统计的覆盖情况",
    )
    ready_for_rollout: bool = Field(..., description="该域是否满足 rollout 条件")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")


class ResearchMetadataReadinessResponse(BaseModel):
    """研究元数据 rollout readiness 响应模型。"""

    generated_at: datetime = Field(..., description="状态生成时间")
    markets: List[str] = Field(default_factory=list, description="目标市场列表")
    domain_count: int = Field(..., description="元数据域数量")
    ready_domain_count: int = Field(..., description="已 ready 的元数据域数量")
    ready_for_rollout: bool = Field(..., description="研究元数据是否整体 ready")
    blockers: List[str] = Field(default_factory=list, description="整体阻塞原因")
    domains: List[ResearchMetadataDomainReadinessResponse] = Field(
        default_factory=list,
        description="逐域 readiness 明细",
    )


class ResearchDcfScenarioResponse(BaseModel):
    """研究域 DCF 场景响应模型。"""

    scenario: str = Field(..., description="场景名称")
    growth_rate: float = Field(..., description="增长率")
    discount_rate: float = Field(..., description="折现率")
    terminal_growth: float = Field(..., description="永续增长率")
    equity_value: float = Field(..., description="股权价值")
    intrinsic_value_per_share: Optional[float] = Field(None, description="每股内在价值")
    upside_to_last_close: Optional[float] = Field(None, description="相对现价空间")
    projected_cash_flows: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="投影现金流",
    )


class ResearchDcfSensitivityPointResponse(BaseModel):
    """研究域 DCF 敏感性点响应模型。"""

    growth_rate: float = Field(..., description="增长率")
    discount_rate: float = Field(..., description="折现率")
    intrinsic_value_per_share: Optional[float] = Field(None, description="每股内在价值")


class ResearchDcfValuationResponse(BaseModel):
    """研究域 DCF 估值响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    status: str = Field(..., description="计算状态")
    missing_reason: Optional[str] = Field(None, description="缺失原因")
    base_cash_flow: Optional[float] = Field(None, description="基准现金流")
    base_cash_flow_source: str = Field(..., description="基准现金流来源")
    projection_years: int = Field(..., description="投影年数")
    shares_outstanding: Optional[float] = Field(None, description="总股本")
    latest_close: Optional[float] = Field(None, description="最新收盘价")
    scenarios: List[ResearchDcfScenarioResponse] = Field(
        default_factory=list,
        description="DCF 场景结果",
    )
    sensitivity: List[ResearchDcfSensitivityPointResponse] = Field(
        default_factory=list,
        description="DCF 敏感性矩阵点集",
    )


class ResearchAnalystCoverageResponse(BaseModel):
    """研究域分析师覆盖与一致预期响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    status: str = Field(..., description="状态")
    missing_reason: Optional[str] = Field(None, description="缺失原因")
    as_of_date: str = Field(..., description="快照日期")
    rating_summary: Optional[str] = Field(None, description="评级摘要")
    report_count: Optional[int] = Field(None, description="研报数")
    institution_count: Optional[int] = Field(None, description="覆盖机构数")
    buy_count: Optional[int] = Field(None, description="买入评级数")
    overweight_count: Optional[int] = Field(None, description="增持/推荐评级数")
    neutral_count: Optional[int] = Field(None, description="中性/持有评级数")
    underperform_count: Optional[int] = Field(None, description="减持/回避评级数")
    sell_count: Optional[int] = Field(None, description="卖出评级数")
    eps_fy1: Optional[float] = Field(None, description="FY1 每股收益预测")
    eps_fy2: Optional[float] = Field(None, description="FY2 每股收益预测")
    net_profit_fy1: Optional[float] = Field(None, description="FY1 净利润预测")
    net_profit_fy2: Optional[float] = Field(None, description="FY2 净利润预测")
    pe_fy1: Optional[float] = Field(None, description="FY1 市盈率预测")
    pe_fy2: Optional[float] = Field(None, description="FY2 市盈率预测")
    source: str = Field(..., description="来源标识")
    source_mode: str = Field(..., description="来源模式")
    data_as_of: datetime = Field(..., description="数据时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    forecast: Optional[Dict[str, Any]] = Field(None, description="预测明细")


class ResearchReportItemResponse(BaseModel):
    """研究域研报元数据单条响应模型。"""

    report_id: str = Field(..., description="研报唯一ID")
    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    publish_date: str = Field(..., description="发布日期")
    report_title: str = Field(..., description="研报标题")
    institution_name: Optional[str] = Field(None, description="机构名称")
    analyst_name: Optional[str] = Field(None, description="分析师")
    rating: Optional[str] = Field(None, description="评级")
    rating_change: Optional[str] = Field(None, description="评级变化")
    target_price: Optional[float] = Field(None, description="目标价")
    report_url: Optional[str] = Field(None, description="研报链接")
    source: str = Field(..., description="来源标识")
    source_mode: str = Field(..., description="来源模式")
    data_as_of: datetime = Field(..., description="数据时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    report: Optional[Dict[str, Any]] = Field(None, description="研报明细")


class ResearchReportsResponse(BaseModel):
    """研究域研报元数据列表响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[str] = Field(None, description="交易所")
    data_points: int = Field(..., description="数据条数")
    window_start: Optional[str] = Field(None, description="窗口开始日期")
    window_end: Optional[str] = Field(None, description="窗口结束日期")
    items: List[ResearchReportItemResponse] = Field(
        default_factory=list,
        description="研报元数据列表",
    )


class ResearchSentimentEventItemResponse(BaseModel):
    """研究域事件/情绪单条响应模型。"""

    event_id: str = Field(..., description="事件唯一ID")
    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    event_date: str = Field(..., description="事件日期")
    event_type: str = Field(..., description="事件类型")
    event_subtype: Optional[str] = Field(None, description="事件子类型")
    title: Optional[str] = Field(None, description="事件标题")
    sentiment_score: Optional[float] = Field(None, description="情绪分数")
    severity: Optional[str] = Field(None, description="严重程度")
    source: str = Field(..., description="来源标识")
    source_mode: str = Field(..., description="来源模式")
    data_as_of: datetime = Field(..., description="数据时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    details: Optional[Dict[str, Any]] = Field(None, description="事件详情")


class ResearchSentimentEventsResponse(BaseModel):
    """研究域事件/情绪列表响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[str] = Field(None, description="交易所")
    data_points: int = Field(..., description="数据条数")
    window_start: Optional[str] = Field(None, description="窗口开始日期")
    window_end: Optional[str] = Field(None, description="窗口结束日期")
    items: List[ResearchSentimentEventItemResponse] = Field(
        default_factory=list,
        description="事件/情绪列表",
    )


class ResearchRiskSnapshotResponse(BaseModel):
    """研究域风险快照响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    status: str = Field(..., description="状态")
    missing_reason: Optional[str] = Field(None, description="缺失原因")
    as_of_date: str = Field(..., description="风险日期")
    benchmark_instrument_id: Optional[str] = Field(None, description="基准标的ID")
    volatility_20d: Optional[float] = Field(None, description="20日年化波动率")
    volatility_60d: Optional[float] = Field(None, description="60日年化波动率")
    beta_60d: Optional[float] = Field(None, description="60日 Beta")
    max_drawdown_252d: Optional[float] = Field(None, description="252日最大回撤")
    average_turnover_20d: Optional[float] = Field(None, description="20日平均换手率")
    average_amount_20d: Optional[float] = Field(None, description="20日平均成交额")
    liability_to_asset: Optional[float] = Field(None, description="资产负债率")
    current_ratio: Optional[float] = Field(None, description="流动比率")
    operating_cf_to_net_income: Optional[float] = Field(None, description="经营现金流/净利润")
    negative_event_count_30d: Optional[int] = Field(None, description="30日负面事件数")
    risk_score: Optional[float] = Field(None, description="综合风险分数")
    risk_level: Optional[str] = Field(None, description="风险等级")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    source: str = Field(..., description="来源标识")
    source_mode: str = Field(..., description="来源模式")
    data_as_of: datetime = Field(..., description="数据时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    details: Optional[Dict[str, Any]] = Field(None, description="风险计算细节")


class ResearchIndustryMembershipResponse(BaseModel):
    """研究域行业归属响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: str = Field(..., description="交易代码")
    exchange: str = Field(..., description="交易所")
    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    industry_code: str = Field(..., description="行业编码")
    industry_name: str = Field(..., description="行业名称")
    industry_level: int = Field(..., description="行业层级")
    parent_code: Optional[str] = Field(None, description="父级行业编码")
    mapping_status: str = Field(..., description="行业映射状态")
    effective_date: Optional[str] = Field(None, description="行业纳入生效日期")
    source_classification: Optional[str] = Field(None, description="来源分类口径")
    source_industry_name: Optional[str] = Field(None, description="来源行业名称")
    sw_l1_code: Optional[str] = Field(None, description="申万一级行业编码")
    sw_l1_name: Optional[str] = Field(None, description="申万一级行业名称")
    sw_l2_code: Optional[str] = Field(None, description="申万二级行业编码")
    sw_l2_name: Optional[str] = Field(None, description="申万二级行业名称")
    sw_l3_code: Optional[str] = Field(None, description="申万三级行业编码")
    sw_l3_name: Optional[str] = Field(None, description="申万三级行业名称")
    sw_l1_index_code: Optional[str] = Field(None, description="申万一级指数代码")
    sw_l2_index_code: Optional[str] = Field(None, description="申万二级指数代码")
    sw_l3_index_code: Optional[str] = Field(None, description="申万三级指数代码")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    data_as_of: datetime = Field(..., description="数据快照时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    membership: Optional[Dict[str, Any]] = Field(None, description="标准化归属详情")


class ResearchIndustryTaxonomyNodeResponse(BaseModel):
    """研究域行业 taxonomy 节点响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    industry_code: str = Field(..., description="行业编码")
    industry_name: str = Field(..., description="行业名称")
    industry_level: int = Field(..., description="行业层级")
    parent_code: Optional[str] = Field(None, description="父级行业编码")
    sw_index_code: Optional[str] = Field(None, description="申万指数代码")
    aliases: Dict[str, Any] = Field(default_factory=dict, description="行业别名与源代码")
    source_classification: Optional[str] = Field(None, description="来源分类口径")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    is_active: bool = Field(..., description="节点是否有效")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")


class ResearchIndustryTaxonomyResponse(BaseModel):
    """研究域行业 taxonomy 列表响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    industry_level: Optional[int] = Field(None, description="行业层级过滤")
    parent_code: Optional[str] = Field(None, description="父级行业编码过滤")
    industry_code: Optional[str] = Field(None, description="行业编码过滤")
    sw_index_code: Optional[str] = Field(None, description="申万指数代码过滤")
    active_only: bool = Field(..., description="是否只返回有效节点")
    limit: int = Field(..., description="返回数量限制")
    offset: int = Field(..., description="偏移量")
    total: int = Field(..., description="符合条件的总记录数")
    items: List[ResearchIndustryTaxonomyNodeResponse] = Field(
        default_factory=list,
        description="行业 taxonomy 节点列表",
    )


class ResearchIndustryComponentSetResponse(BaseModel):
    """研究域行业成分集响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    industry_code: str = Field(..., description="行业编码")
    component_count: int = Field(..., description="成分数量")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    built_at: datetime = Field(..., description="缓存构建时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    symbols: Optional[List[str]] = Field(None, description="成分股票代码列表")


class ResearchIndustryComponentSetsResponse(BaseModel):
    """研究域行业成分集列表响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    industry_code: Optional[str] = Field(None, description="行业编码过滤")
    sw_index_code: Optional[str] = Field(None, description="申万指数代码过滤")
    resolved_industry_code: Optional[str] = Field(None, description="解析后的行业编码")
    missing_reason: Optional[str] = Field(None, description="空结果原因")
    max_age_days: Optional[int] = Field(None, description="最大缓存年龄（天）")
    include_symbols: bool = Field(..., description="是否包含成分股票列表")
    limit: int = Field(..., description="返回数量限制")
    offset: int = Field(..., description="偏移量")
    total: int = Field(..., description="符合条件的总记录数")
    items: List[ResearchIndustryComponentSetResponse] = Field(
        default_factory=list,
        description="行业成分集列表",
    )


class ResearchIndustryIndexAnalysisItemResponse(BaseModel):
    """申万行业指数分析日度记录响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    sw_index_code: str = Field(..., description="申万指数代码")
    trade_date: date = Field(..., description="交易日期")
    sw_index_name: str = Field(..., description="申万指数名称")
    index_type: Optional[str] = Field(None, description="指数分类")
    close_index: Optional[float] = Field(None, description="收盘指数")
    bargain_volume: Optional[float] = Field(None, description="成交量，单位：亿股")
    markup: Optional[float] = Field(None, description="涨跌幅，单位：百分数值")
    turnover_rate: Optional[float] = Field(None, description="换手率，单位：百分数值")
    pe: Optional[float] = Field(None, description="市盈率")
    pb: Optional[float] = Field(None, description="市净率")
    mean_price: Optional[float] = Field(None, description="均价")
    bargain_sum_rate: Optional[float] = Field(None, description="成交额占比，单位：百分数值")
    negotiable_share_sum: Optional[float] = Field(None, description="流通市值合计，单位：亿元")
    average_negotiable_share_sum: Optional[float] = Field(None, description="平均流通市值，单位：亿元")
    dividend_yield: Optional[float] = Field(None, description="股息率，单位：百分数值")
    source: str = Field(..., description="命中数据源")
    source_mode: str = Field(..., description="命中模式")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    raw_payload: Optional[Dict[str, Any]] = Field(None, description="原始载荷")


class ResearchIndustryIndexAnalysisResponse(BaseModel):
    """申万行业指数分析列表响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    sw_index_code: Optional[str] = Field(None, description="申万指数代码过滤")
    index_type: Optional[str] = Field(None, description="指数分类过滤")
    trade_date: Optional[date] = Field(None, description="交易日期过滤")
    start_date: Optional[date] = Field(None, description="开始日期过滤")
    end_date: Optional[date] = Field(None, description="结束日期过滤")
    include_payload: bool = Field(..., description="是否包含原始载荷")
    limit: int = Field(..., description="返回数量限制")
    offset: int = Field(..., description="偏移量")
    total: int = Field(..., description="符合条件的总记录数")
    summary: Dict[str, Any] = Field(default_factory=dict, description="全表摘要")
    field_units: Dict[str, Any] = Field(
        default_factory=dict,
        description="申万指数分析数值字段单位说明",
    )
    items: List[ResearchIndustryIndexAnalysisItemResponse] = Field(
        default_factory=list,
        description="申万行业指数分析记录",
    )


class ResearchIndustryIndexAnalysisBenchmarkResponse(BaseModel):
    """通过 taxonomy alias 查询的申万行业指数分析 benchmark 响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    industry_code: str = Field(..., description="行业编码")
    sw_index_code: Optional[str] = Field(None, description="申万指数代码")
    missing_reason: Optional[str] = Field(None, description="未返回 benchmark 的原因")
    taxonomy_node: ResearchIndustryTaxonomyNodeResponse = Field(
        ...,
        description="taxonomy 节点",
    )
    index_analysis: Optional[ResearchIndustryIndexAnalysisItemResponse] = Field(
        None,
        description="最新申万指数分析记录",
    )


class ResearchIndustryStandardMappingCacheReadinessResponse(BaseModel):
    """严格申万官方映射缓存 readiness 摘要。"""

    total: int = Field(..., description="缓存总行数")
    mapped: int = Field(..., description="已映射官方行业码数量")
    unmapped: int = Field(..., description="未映射官方行业码数量")
    latest_built_at: Optional[datetime] = Field(None, description="最近一次缓存构建时间")
    latest_updated_at: Optional[datetime] = Field(None, description="最近一次缓存更新时间")
    source: Optional[str] = Field(None, description="最近一次缓存构建来源")
    source_mode: Optional[str] = Field(None, description="最近一次缓存构建模式")
    cache_max_age_days: int = Field(..., description="允许的最大缓存年龄（天）")
    minimum_mapping_rows: int = Field(..., description="最小缓存行数阈值")
    minimum_mapped_rows: int = Field(..., description="最小已映射行数阈值")
    fresh: bool = Field(..., description="缓存是否在允许年龄范围内")
    meets_minimum_rows: bool = Field(..., description="是否满足最小缓存行数阈值")
    meets_minimum_mapped_rows: bool = Field(..., description="是否满足最小已映射行数阈值")


class ResearchIndustryStandardCoverageResponse(BaseModel):
    """严格申万层覆盖摘要。"""

    total: int = Field(..., description="总记录数")
    counts: Dict[str, int] = Field(default_factory=dict, description="按状态统计")
    latest_updated_at: Optional[datetime] = Field(None, description="最近更新时间")
    latest_data_as_of: Optional[datetime] = Field(None, description="最近业务时间")
    meets_target_universe: bool = Field(..., description="是否覆盖当前目标股票池")


class ResearchIndustryStandardExchangeCoverageResponse(BaseModel):
    """按交易所统计的 authoritative 覆盖率。"""

    exchange: str = Field(..., description="交易所")
    target_instruments: int = Field(..., description="目标股票数量")
    authoritative_memberships: int = Field(..., description="authoritative 归属数量")
    coverage_ratio: float = Field(..., description="authoritative 覆盖率")
    ready: bool = Field(..., description="该交易所是否已满足覆盖要求")


class ResearchRelativeValuationReadinessResponse(BaseModel):
    """相对估值 rollout readiness 摘要。"""

    require_authoritative: bool = Field(..., description="是否要求 authoritative 行业归属")
    benchmark_level: int = Field(..., description="相对估值比较层级")
    ready: bool = Field(..., description="是否可进入 rollout")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")


class ResearchIndustryIndexAnalysisReadinessResponse(BaseModel):
    """申万指数分析缓存 readiness 摘要。"""

    enabled: bool = Field(..., description="申万指数分析同步是否启用")
    total: int = Field(..., description="已落库指数分析记录数")
    distinct_index_codes: int = Field(..., description="已覆盖指数代码数量")
    latest_trade_date: Optional[date] = Field(None, description="最新交易日")
    latest_updated_at: Optional[datetime] = Field(None, description="最近更新时间")
    index_type_counts: Dict[str, Any] = Field(
        default_factory=dict,
        description="按指数分类统计的行数和代码数",
    )


class ResearchIndustryStandardUnmappedBacklogItemResponse(BaseModel):
    """严格申万 readiness 中的未映射 backlog 摘要项。"""

    official_industry_code: str = Field(..., description="官方行业六码")
    best_taxonomy_industry_code: Optional[str] = Field(
        None,
        description="当前最佳候选标准行业编码",
    )
    current_classification_count: int = Field(
        ...,
        description="当前 latest classifications 中受影响的股票数量",
    )
    impacted_exchange_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的当前受影响股票数量",
    )
    sample_instruments: List[str] = Field(
        default_factory=list,
        description="当前受影响标的样本",
    )


class ResearchIndustryStandardUnmappedBacklogSummaryResponse(BaseModel):
    """严格申万 readiness 中的未映射 backlog 汇总。"""

    official_code_total: int = Field(..., description="未映射 official code 总数")
    current_classification_total: int = Field(
        ...,
        description="当前 latest classifications 中受影响的股票总数",
    )
    top_items: List[ResearchIndustryStandardUnmappedBacklogItemResponse] = Field(
        default_factory=list,
        description="高 impact 未映射 official code 样本",
    )


class ResearchIndustryStandardOverrideReviewItemResponse(BaseModel):
    """严格申万 readiness 中的 override review 摘要项。"""

    official_industry_code: str = Field(..., description="官方行业六码")
    review_status: str = Field(..., description="override 审阅状态")
    status_reason: str = Field(..., description="override 审阅状态说明")


class ResearchIndustryStandardOverrideReviewSummaryResponse(BaseModel):
    """严格申万 readiness 中的 override review 汇总。"""

    requires_attention: bool = Field(..., description="override review 是否仍需人工关注")
    configured_override_total: int = Field(..., description="当前配置 override 数量")
    ready_candidate_total: int = Field(..., description="当前 ready candidate 数量")
    applied_override_total: int = Field(..., description="当前已生效 override 数量")
    pending_manual_override_total: int = Field(
        ...,
        description="当前 ready 但未配置的 override 数量",
    )
    status_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="override 审阅状态分布",
    )
    top_items: List[ResearchIndustryStandardOverrideReviewItemResponse] = Field(
        default_factory=list,
        description="需要关注的 override 审阅样本",
    )


class ResearchIndustryStandardReadinessResponse(BaseModel):
    """严格申万标准层与相对估值 rollout readiness 响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    generated_at: datetime = Field(..., description="状态生成时间")
    markets: List[str] = Field(default_factory=list, description="目标市场列表")
    target_instrument_count: int = Field(..., description="目标股票池总量")
    target_instruments_by_exchange: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的目标股票池数量",
    )
    official_mapping_cache: ResearchIndustryStandardMappingCacheReadinessResponse = Field(
        ...,
        description="官方映射缓存状态",
    )
    official_classifications: ResearchIndustryStandardCoverageResponse = Field(
        ...,
        description="official classification 覆盖情况",
    )
    memberships: ResearchIndustryStandardCoverageResponse = Field(
        ...,
        description="标准行业归属覆盖情况",
    )
    unmapped_backlog: ResearchIndustryStandardUnmappedBacklogSummaryResponse = Field(
        ...,
        description="未映射 official-code backlog 摘要",
    )
    override_review: ResearchIndustryStandardOverrideReviewSummaryResponse = Field(
        ...,
        description="official override review 摘要",
    )
    exchange_coverage: List[ResearchIndustryStandardExchangeCoverageResponse] = Field(
        default_factory=list,
        description="按交易所统计的 authoritative 覆盖率",
    )
    industry_standard_ready: bool = Field(..., description="严格申万标准层是否 ready")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")
    relative_valuation: ResearchRelativeValuationReadinessResponse = Field(
        ...,
        description="相对估值 rollout readiness",
    )
    index_analysis: Optional[ResearchIndustryIndexAnalysisReadinessResponse] = Field(
        None,
        description="申万指数分析缓存状态",
    )


class ResearchOfficialIndustryCodeMappingResponse(BaseModel):
    """官方行业码到研究 taxonomy 的映射缓存响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    official_industry_code: str = Field(..., description="官方行业六码")
    best_taxonomy_industry_code: Optional[str] = Field(
        None,
        description="自动推断阶段的最佳候选行业编码",
    )
    mapped_industry_code: Optional[str] = Field(
        None,
        description="最终采用的标准行业编码",
    )
    mapping_status: str = Field(..., description="映射状态")
    mapping_confidence: str = Field(..., description="映射置信度")
    overlap_count: Optional[int] = Field(None, description="成分重叠数量")
    official_symbol_count: Optional[int] = Field(
        None,
        description="官方行业样本股票数量",
    )
    taxonomy_symbol_count: Optional[int] = Field(
        None,
        description="taxonomy 节点样本股票数量",
    )
    precision: Optional[float] = Field(None, description="映射精确率")
    recall: Optional[float] = Field(None, description="映射召回率")
    source: str = Field(..., description="映射来源")
    source_mode: str = Field(..., description="映射来源模式")
    built_at: datetime = Field(..., description="映射构建时间")
    ingestion_run_id: Optional[int] = Field(None, description="采集运行ID")
    created_at: datetime = Field(..., description="创建时间")
    updated_at: datetime = Field(..., description="更新时间")
    mapping: Optional[Dict[str, Any]] = Field(None, description="映射详情与审计载荷")


class ResearchOfficialIndustryCodeMappingsResponse(BaseModel):
    """官方行业码映射缓存列表响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    mapping_status: Optional[str] = Field(None, description="映射状态过滤条件")
    source: Optional[str] = Field(None, description="来源过滤条件")
    source_mode: Optional[str] = Field(None, description="来源模式过滤条件")
    max_age_days: Optional[int] = Field(None, description="缓存年龄过滤条件")
    limit: int = Field(..., description="分页大小")
    offset: int = Field(..., description="分页偏移")
    total: int = Field(..., description="过滤后总记录数")
    mapping_status_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="当前 taxonomy/source 维度下的状态汇总",
    )
    items: List[ResearchOfficialIndustryCodeMappingResponse] = Field(
        default_factory=list,
        description="映射缓存行",
    )


class ResearchOfficialIndustryCodeBacklogItemResponse(
    ResearchOfficialIndustryCodeMappingResponse
):
    """官方行业码未映射 backlog 行响应模型。"""

    current_classification_count: int = Field(
        ...,
        description="当前 latest classifications 中受影响的股票数量",
    )
    impacted_exchange_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的当前受影响股票数量",
    )
    sample_instruments: List[str] = Field(
        default_factory=list,
        description="当前受影响标的样本",
    )
    review_priority: str = Field(..., description="人工回修优先级")
    override_candidate_ready: bool = Field(
        ...,
        description="是否值得优先进入 manual override 人工核验",
    )
    override_candidate_reason: str = Field(
        ...,
        description="override-review 信号说明",
    )
    candidate_count: int = Field(..., description="候选数量")
    top_candidate_overlap_gap: Optional[int] = Field(
        None,
        description="top candidate 与第二候选的 overlap 差值",
    )
    manual_override_suggestion: Optional[Dict[str, Any]] = Field(
        None,
        description="可直接转译为 manual_overrides 条目的建议载荷",
    )


class ResearchOfficialIndustryCodeBacklogResponse(BaseModel):
    """官方行业码未映射 backlog 列表响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    source: Optional[str] = Field(None, description="来源过滤条件")
    source_mode: Optional[str] = Field(None, description="来源模式过滤条件")
    max_age_days: Optional[int] = Field(None, description="缓存年龄过滤条件")
    limit: int = Field(..., description="分页大小")
    offset: int = Field(..., description="分页偏移")
    total: int = Field(..., description="过滤后 backlog 总行数")
    current_classification_total: int = Field(
        ...,
        description="过滤后 backlog 当前影响的股票总数",
    )
    override_candidate_total: int = Field(
        ...,
        description="过滤后值得优先人工核验的 backlog 行数量",
    )
    review_priority_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="过滤后 backlog 的优先级分布",
    )
    items: List[ResearchOfficialIndustryCodeBacklogItemResponse] = Field(
        default_factory=list,
        description="未映射 backlog 行",
    )


class ResearchOfficialMappingOverrideCandidatesResponse(BaseModel):
    """official manual-override 候选导出响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    source: Optional[str] = Field(None, description="来源过滤条件")
    source_mode: Optional[str] = Field(None, description="来源模式过滤条件")
    max_age_days: Optional[int] = Field(None, description="缓存年龄过滤条件")
    limit: int = Field(..., description="分页大小")
    offset: int = Field(..., description="分页偏移")
    total: int = Field(..., description="导出的 override-ready 候选总数")
    current_classification_total: int = Field(
        ...,
        description="导出的 override-ready 候选当前影响的股票总数",
    )
    override_candidate_total: int = Field(
        ...,
        description="导出的 ready candidate 数量",
    )
    review_priority_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="导出结果的优先级分布",
    )
    manual_overrides: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="可直接转译为 official_mapping.manual_overrides 的配置片段",
    )
    items: List[ResearchOfficialIndustryCodeBacklogItemResponse] = Field(
        default_factory=list,
        description="ready candidate 明细行",
    )


class ResearchOfficialMappingOverrideReviewItemResponse(BaseModel):
    """official override 审阅行响应模型。"""

    official_industry_code: str = Field(..., description="官方六位行业码")
    review_status: str = Field(..., description="审阅状态")
    status_reason: str = Field(..., description="审阅状态说明")
    configured_override: Optional[Dict[str, Any]] = Field(
        None,
        description="当前配置中的 manual_override 条目",
    )
    ready_candidate: Optional[Dict[str, Any]] = Field(
        None,
        description="当前 ready candidate 导出的 override 建议",
    )
    applied_override: Optional[Dict[str, Any]] = Field(
        None,
        description="当前 mapping cache 中实际生效的 manual_override 记录",
    )


class ResearchOfficialMappingOverrideReviewResponse(BaseModel):
    """official override 审阅视图响应模型。"""

    taxonomy_system: str = Field(..., description="内部行业体系标识")
    taxonomy_version: Optional[str] = Field(None, description="行业体系版本")
    source: Optional[str] = Field(None, description="来源过滤条件")
    source_mode: Optional[str] = Field(None, description="来源模式过滤条件")
    max_age_days: Optional[int] = Field(None, description="缓存年龄过滤条件")
    attention_only: bool = Field(False, description="是否仅返回 attention 状态")
    review_status: List[str] = Field(
        default_factory=list,
        description="当前应用的 review_status 过滤条件",
    )
    configured_override_total: int = Field(
        ...,
        description="当前配置中的 manual_override 条目数",
    )
    ready_candidate_total: int = Field(
        ...,
        description="当前 ready candidate 数量",
    )
    applied_override_total: int = Field(
        ...,
        description="当前 mapping cache 中已生效 manual_override 数量",
    )
    pending_manual_override_total: int = Field(
        ...,
        description="当前 ready 但尚未配置的 manual_override 数量",
    )
    status_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="审阅状态分布",
    )
    pending_manual_overrides: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description="ready but not configured 的 manual_overrides 配置片段",
    )
    items: List[ResearchOfficialMappingOverrideReviewItemResponse] = Field(
        default_factory=list,
        description="审阅行明细",
    )


class ResearchSourceSummaryItemResponse(BaseModel):
    """研究域分段来源摘要。"""

    available: bool = Field(..., description="该数据分段是否可用")
    source: Optional[str] = Field(None, description="命中的数据源")
    source_mode: Optional[str] = Field(None, description="命中的来源模式")
    data_as_of: Optional[datetime] = Field(None, description="该分段数据时间")
    missing_reason: Optional[str] = Field(None, description="缺失原因")


class ResearchCompanyOverviewResponse(BaseModel):
    """研究域公司概览响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[str] = Field(None, description="交易所")
    market: Optional[str] = Field(None, description="市场标识")
    company_name: Optional[str] = Field(None, description="公司全称")
    short_name: Optional[str] = Field(None, description="公司简称")
    listed_date: Optional[str] = Field(None, description="上市日期")
    industry_raw: Optional[str] = Field(None, description="原始行业字段")
    sector_raw: Optional[str] = Field(None, description="原始板块字段")
    industry_system: Optional[str] = Field(None, description="内部行业体系标识")
    industry_taxonomy_version: Optional[str] = Field(None, description="内部行业体系版本")
    industry_code: Optional[str] = Field(None, description="行业编码")
    industry_name: Optional[str] = Field(None, description="行业名称")
    industry_level: Optional[int] = Field(None, description="行业层级")
    industry_mapping_status: Optional[str] = Field(None, description="行业映射状态")
    sw_l1_code: Optional[str] = Field(None, description="申万一级行业编码")
    sw_l1_name: Optional[str] = Field(None, description="申万一级行业名称")
    sw_l2_code: Optional[str] = Field(None, description="申万二级行业编码")
    sw_l2_name: Optional[str] = Field(None, description="申万二级行业名称")
    sw_l3_code: Optional[str] = Field(None, description="申万三级行业编码")
    sw_l3_name: Optional[str] = Field(None, description="申万三级行业名称")
    status: Optional[str] = Field(None, description="上市状态")
    report_date: Optional[str] = Field(None, description="最新报告期")
    pub_date: Optional[str] = Field(None, description="最新披露日期")
    fiscal_year: Optional[int] = Field(None, description="最新财年")
    fiscal_quarter: Optional[int] = Field(None, description="最新财季")
    currency: Optional[str] = Field(None, description="货币")
    schema_version: Optional[str] = Field(None, description="财务摘要schema版本")
    roe: Optional[float] = Field(None, description="净资产收益率")
    gross_margin: Optional[float] = Field(None, description="毛利率")
    net_margin: Optional[float] = Field(None, description="净利率")
    current_ratio: Optional[float] = Field(None, description="流动比率")
    quick_ratio: Optional[float] = Field(None, description="速动比率")
    liability_to_asset: Optional[float] = Field(None, description="资产负债率")
    yoy_asset: Optional[float] = Field(None, description="总资产同比")
    yoy_equity: Optional[float] = Field(None, description="净资产同比")
    yoy_net_profit: Optional[float] = Field(None, description="净利润同比")
    cfo_to_revenue: Optional[float] = Field(None, description="经营现金流/营业收入")
    cfo_to_net_profit: Optional[float] = Field(None, description="经营现金流/净利润")
    asset_turnover: Optional[float] = Field(None, description="总资产周转率")
    eps: Optional[float] = Field(None, description="每股收益")
    data_as_of: datetime = Field(..., description="概览数据时间")
    source_summary: Dict[str, ResearchSourceSummaryItemResponse] = Field(
        ...,
        description="各分段来源摘要",
    )
    missing_sections: List[str] = Field(
        default_factory=list,
        description="缺失的数据分段",
    )
    company_profile: Optional[ResearchCompanyProfileResponse] = Field(
        None,
        description="公司档案分段详情",
    )
    industry: Optional[ResearchIndustryMembershipResponse] = Field(
        None,
        description="行业归属分段详情",
    )
    financial_summary: Optional[ResearchFinancialSummaryResponse] = Field(
        None,
        description="财务摘要分段详情",
    )


class ResearchTechnicalQuoteSummaryResponse(BaseModel):
    """技术分析所用行情窗口摘要。"""

    quote_source: str = Field(..., description="行情来源")
    data_points: int = Field(..., description="参与计算的K线数量")
    window_start: datetime = Field(..., description="计算窗口开始时间")
    window_end: datetime = Field(..., description="计算窗口结束时间")
    requested_adjustment: str = Field(..., description="请求的复权方式")
    applied_adjustment: str = Field(..., description="实际应用的复权方式")
    latest_quality_score: Optional[float] = Field(None, description="最新质量评分")


class ResearchTechnicalCacheExchangeCoverageResponse(BaseModel):
    """技术指标最新快照按交易所覆盖情况。"""

    exchange: str = Field(..., description="交易所")
    target_instruments: int = Field(..., description="目标股票数量")
    snapshot_count: int = Field(..., description="已覆盖快照股票数量")
    coverage_ratio: float = Field(..., description="覆盖率")
    ready: bool = Field(..., description="该交易所是否满足覆盖要求")


class ResearchTechnicalCacheReadinessResponse(BaseModel):
    """技术指标最新快照缓存 rollout readiness。"""

    generated_at: datetime = Field(..., description="状态生成时间")
    markets: List[str] = Field(default_factory=list, description="目标市场列表")
    module_enabled: bool = Field(..., description="技术分析模块是否启用")
    cache_enabled: bool = Field(..., description="最新快照缓存是否启用")
    period: str = Field(..., description="快照周期")
    adjustment: str = Field(..., description="快照复权口径")
    target_instrument_count: int = Field(..., description="目标股票池总量")
    target_instruments_by_exchange: Dict[str, int] = Field(
        default_factory=dict,
        description="按交易所统计的目标股票池数量",
    )
    snapshot_total: int = Field(..., description="已覆盖股票数量")
    row_total: int = Field(..., description="已落库快照行数")
    missing_snapshot_count: int = Field(..., description="仍缺失快照的股票数量")
    source_counts: Dict[str, int] = Field(default_factory=dict, description="按 source 统计")
    source_mode_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按 source_mode 统计",
    )
    calc_method_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按计算方法统计",
    )
    calc_version_counts: Dict[str, int] = Field(
        default_factory=dict,
        description="按计算版本统计",
    )
    status_counts: Dict[str, int] = Field(default_factory=dict, description="按计算状态统计")
    signal_counts: Dict[str, int] = Field(default_factory=dict, description="按信号统计")
    latest_as_of_date: Optional[str] = Field(None, description="最近快照日期")
    latest_updated_at: Optional[datetime] = Field(None, description="最近更新时间")
    latest_data_as_of: Optional[datetime] = Field(None, description="最近数据时间")
    exchange_coverage: List[ResearchTechnicalCacheExchangeCoverageResponse] = Field(
        default_factory=list,
        description="按交易所统计的覆盖情况",
    )
    ready_for_rollout: bool = Field(..., description="技术快照缓存是否满足 rollout 条件")
    blockers: List[str] = Field(default_factory=list, description="阻塞原因")


class ResearchTechnicalSummaryResponse(BaseModel):
    """研究域技术分析摘要响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[str] = Field(None, description="交易所")
    data_as_of: datetime = Field(..., description="技术摘要对应时间")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    status: str = Field(..., description="计算状态")
    missing_reason: Optional[str] = Field(None, description="缺失原因")
    signal: str = Field(..., description="技术信号")
    trend_score: Optional[float] = Field(None, description="趋势评分")
    close: Optional[float] = Field(None, description="最新收盘价")
    pct_change_1d: Optional[float] = Field(None, description="1日收益率")
    pct_change_20d: Optional[float] = Field(None, description="20日收益率")
    sma20: Optional[float] = Field(None, description="20日均线")
    sma60: Optional[float] = Field(None, description="60日均线")
    ema12: Optional[float] = Field(None, description="12日EMA")
    ema26: Optional[float] = Field(None, description="26日EMA")
    macd: Optional[float] = Field(None, description="MACD DIF")
    macd_signal: Optional[float] = Field(None, description="MACD DEA")
    macd_hist: Optional[float] = Field(None, description="MACD柱")
    rsi14: Optional[float] = Field(None, description="14日RSI")
    adx: Optional[float] = Field(None, description="ADX 趋势强度")
    plus_di: Optional[float] = Field(None, description="+DI")
    minus_di: Optional[float] = Field(None, description="-DI")
    stoch_k: Optional[float] = Field(None, description="随机指标 K")
    stoch_d: Optional[float] = Field(None, description="随机指标 D")
    cci: Optional[float] = Field(None, description="CCI")
    williams_r: Optional[float] = Field(None, description="Williams %R")
    boll_upper: Optional[float] = Field(None, description="布林上轨")
    boll_middle: Optional[float] = Field(None, description="布林中轨")
    boll_lower: Optional[float] = Field(None, description="布林下轨")
    atr14: Optional[float] = Field(None, description="14日ATR")
    volume_ratio: Optional[float] = Field(None, description="量比")
    distance_to_sma20: Optional[float] = Field(None, description="相对20日均线偏离")
    distance_to_sma60: Optional[float] = Field(None, description="相对60日均线偏离")
    quote_summary: ResearchTechnicalQuoteSummaryResponse = Field(
        ...,
        description="计算所用行情窗口摘要",
    )


class ResearchTechnicalIndicatorPointResponse(BaseModel):
    """技术指标时间序列点。"""

    time: datetime = Field(..., description="K线时间")
    close: Optional[float] = Field(None, description="收盘价")
    sma20: Optional[float] = Field(None, description="20日均线")
    sma60: Optional[float] = Field(None, description="60日均线")
    ema12: Optional[float] = Field(None, description="12日EMA")
    ema26: Optional[float] = Field(None, description="26日EMA")
    macd: Optional[float] = Field(None, description="MACD DIF")
    macd_signal: Optional[float] = Field(None, description="MACD DEA")
    macd_hist: Optional[float] = Field(None, description="MACD柱")
    rsi14: Optional[float] = Field(None, description="14日RSI")
    adx: Optional[float] = Field(None, description="ADX 趋势强度")
    plus_di: Optional[float] = Field(None, description="+DI")
    minus_di: Optional[float] = Field(None, description="-DI")
    stoch_k: Optional[float] = Field(None, description="随机指标 K")
    stoch_d: Optional[float] = Field(None, description="随机指标 D")
    cci: Optional[float] = Field(None, description="CCI")
    williams_r: Optional[float] = Field(None, description="Williams %R")
    boll_upper: Optional[float] = Field(None, description="布林上轨")
    boll_middle: Optional[float] = Field(None, description="布林中轨")
    boll_lower: Optional[float] = Field(None, description="布林下轨")
    atr14: Optional[float] = Field(None, description="14日ATR")
    volume_ratio: Optional[float] = Field(None, description="量比")
    trend_score: Optional[float] = Field(None, description="趋势评分")
    signal: str = Field(..., description="该时点技术信号")


class ResearchTechnicalIndicatorsResponse(BaseModel):
    """研究域技术指标时间序列响应模型。"""

    instrument_id: str = Field(..., description="交易品种ID")
    symbol: Optional[str] = Field(None, description="交易代码")
    exchange: Optional[str] = Field(None, description="交易所")
    calc_method: str = Field(..., description="计算方法")
    calc_version: str = Field(..., description="计算版本")
    parameter_hash: str = Field(..., description="参数哈希")
    requested_adjustment: str = Field(..., description="请求的复权方式")
    applied_adjustment: str = Field(..., description="实际应用的复权方式")
    data_points: int = Field(..., description="返回的序列点数量")
    window_start: datetime = Field(..., description="序列起始时间")
    window_end: datetime = Field(..., description="序列结束时间")
    items: List[ResearchTechnicalIndicatorPointResponse] = Field(
        ...,
        description="技术指标时间序列",
    )
