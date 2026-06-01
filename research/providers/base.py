"""
Base provider contracts for research ingestion.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class CompanyProfileSnapshot:
    """Normalized company profile snapshot from one provider."""

    instrument_id: str
    symbol: str
    company_name: str
    short_name: str
    exchange: str
    market: Optional[str] = None
    listed_date: Optional[str] = None
    industry_raw: Optional[str] = None
    sector_raw: Optional[str] = None
    status: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    raw_payload: Dict[str, Any] = field(default_factory=dict)


class BaseCompanyProfileProvider(ABC):
    """Base contract for company profile providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_company_profiles(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[CompanyProfileSnapshot]:
        """Fetch company profiles for a set of instruments."""


@dataclass(frozen=True)
class FinancialSummarySnapshot:
    """Normalized latest financial summary snapshot from one provider."""

    instrument_id: str
    symbol: str
    exchange: str
    report_date: Optional[str] = None
    pub_date: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    currency: str = "CNY"
    schema_version: str = "financial_summary.v1"
    roe: Optional[float] = None
    gross_margin: Optional[float] = None
    net_margin: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    liability_to_asset: Optional[float] = None
    yoy_asset: Optional[float] = None
    yoy_equity: Optional[float] = None
    yoy_net_profit: Optional[float] = None
    cfo_to_revenue: Optional[float] = None
    cfo_to_net_profit: Optional[float] = None
    asset_turnover: Optional[float] = None
    eps: Optional[float] = None
    source: str = ""
    source_mode: str = "direct"
    summary_json: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)


class BaseFinancialSummaryProvider(ABC):
    """Base contract for financial summary providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_financial_summaries(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[FinancialSummarySnapshot]:
        """Fetch latest financial summary snapshots for a set of instruments."""


@dataclass(frozen=True)
class ShareholderSnapshot:
    """Normalized latest shareholder summary snapshot from one provider."""

    instrument_id: str
    symbol: str
    exchange: str
    coverage_status: str = "reference_only"
    holder_count: Optional[int] = None
    holder_count_report_date: Optional[str] = None
    top_holders_report_date: Optional[str] = None
    top_holders_count: Optional[int] = None
    top_holders_total_ratio: Optional[float] = None
    control_owner_name: Optional[str] = None
    control_owner_ratio: Optional[float] = None
    schema_version: str = "shareholders.v1"
    source: str = ""
    source_mode: str = "direct"
    snapshot_json: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)


class BaseShareholderProvider(ABC):
    """Base contract for shareholder summary providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_shareholder_snapshots(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[ShareholderSnapshot]:
        """Fetch latest shareholder summary snapshots for a set of instruments."""


@dataclass(frozen=True)
class FinancialStatementRawSnapshot:
    """One raw statement payload for a report period."""

    instrument_id: str
    symbol: str
    exchange: str
    statement_type: str
    report_period: str
    publish_date: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    currency: str = "CNY"
    schema_version: str = "financial_statements_raw.v1"
    source: str = ""
    source_mode: str = "direct"
    statement_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinancialSourceFileManifest:
    """Manifest metadata for one official or fallback financial source file."""

    source: str
    exchange: str
    report_period: str
    parser_version: str
    source_mode: str = "direct"
    instrument_id: Optional[str] = None
    symbol: Optional[str] = None
    report_type: Optional[str] = None
    filing_id: Optional[str] = None
    source_url: Optional[str] = None
    archive_path: Optional[str] = None
    content_hash: Optional[str] = None
    content_length: Optional[int] = None
    published_at: Optional[str] = None
    downloaded_at: Optional[str] = None
    source_file_id: Optional[str] = None
    status: str = "discovered"
    schema_version: str = "financial_source_file_manifest.v1"
    parser_diagnostics: Dict[str, Any] = field(default_factory=dict)
    metadata_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinancialFilingPayload:
    """Downloaded structured financial filing payload with manifest metadata."""

    manifest: FinancialSourceFileManifest
    content: bytes
    text: Optional[str] = None
    content_type: Optional[str] = None


@dataclass(frozen=True)
class FinancialNumericFactSnapshot:
    """One long-form numeric fact parsed from a structured financial filing."""

    source_file_id: str
    instrument_id: str
    symbol: str
    exchange: str
    report_period: str
    fact_name: str
    fact_value: Optional[float]
    source: str
    parser_version: str
    source_mode: str = "direct"
    report_type: Optional[str] = None
    statement_family: Optional[str] = None
    canonical_fact_name: Optional[str] = None
    canonical_statement_family: Optional[str] = None
    canonical_semantic: Optional[str] = None
    canonical_unit: Optional[str] = None
    canonical_version: Optional[str] = None
    taxonomy_namespace: Optional[str] = None
    context_id: Optional[str] = None
    unit: Optional[str] = None
    decimals: Optional[str] = None
    precision: Optional[str] = None
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    instant: Optional[str] = None
    currency: str = "CNY"
    value_text: Optional[str] = None
    dimensions_json: Dict[str, Any] = field(default_factory=dict)
    raw_fact_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinancialFactsSnapshot:
    """Normalized financial facts for one latest report period."""

    instrument_id: str
    symbol: str
    exchange: str
    report_period: str
    publish_date: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    currency: str = "CNY"
    schema_version: str = "financial_facts.v1"
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_profit: Optional[float] = None
    pre_tax_profit: Optional[float] = None
    net_income: Optional[float] = None
    operating_cf: Optional[float] = None
    total_cf: Optional[float] = None
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    equity: Optional[float] = None
    current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    inventory: Optional[float] = None
    receivables: Optional[float] = None
    fixed_assets: Optional[float] = None
    intangible_assets: Optional[float] = None
    shares_outstanding: Optional[float] = None
    source: str = ""
    source_mode: str = "direct"
    facts_json: Dict[str, Any] = field(default_factory=dict)
    report_type: Optional[str] = None
    statement_family: Optional[str] = None
    data_available_date: Optional[str] = None
    source_file_id: Optional[str] = None
    filing_id: Optional[str] = None
    lineage_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinancialIndicatorSnapshot:
    """Derived financial indicators for one latest report period."""

    instrument_id: str
    symbol: str
    exchange: str
    report_period: str
    publish_date: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    currency: str = "CNY"
    schema_version: str = "financial_indicator_snapshots.v1"
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    asset_liability_ratio: Optional[float] = None
    revenue_per_share: Optional[float] = None
    operating_cf_to_revenue: Optional[float] = None
    operating_cf_to_net_income: Optional[float] = None
    book_value_per_share: Optional[float] = None
    source: str = ""
    source_mode: str = "direct"
    indicators_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FinancialStatementBundle:
    """One latest financial statement bundle for an instrument."""

    instrument_id: str
    symbol: str
    exchange: str
    report_period: str
    publish_date: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    currency: str = "CNY"
    source: str = ""
    source_mode: str = "direct"
    raw_statements: List[FinancialStatementRawSnapshot] = field(default_factory=list)
    facts: Optional[FinancialFactsSnapshot] = None
    indicators: Optional[FinancialIndicatorSnapshot] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValuationHistorySnapshot:
    """One derived valuation history row from local quotes and financial facts."""

    instrument_id: str
    symbol: str
    exchange: str
    as_of_date: str
    currency: str = "CNY"
    close_price: Optional[float] = None
    market_cap: Optional[float] = None
    float_market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    ps_ratio: Optional[float] = None
    pe_static: Optional[float] = None
    pe_ttm: Optional[float] = None
    pe_forward: Optional[float] = None
    pb_mrq: Optional[float] = None
    ps_static: Optional[float] = None
    ps_ttm: Optional[float] = None
    ps_forward: Optional[float] = None
    calc_method: str = "valuation_history_builtin"
    calc_version: str = "valuation_history.v1"
    parameter_hash: str = ""
    source: str = "local_quotes_financial_facts"
    source_mode: str = "derived"
    details_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValuationInputSnapshot:
    """One valuation input row with explicit source, unit, and date lineage."""

    instrument_id: str
    symbol: str
    exchange: str
    as_of_date: str
    currency: str = "CNY"
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    float_market_cap: Optional[float] = None
    float_shares: Optional[float] = None
    source: str = ""
    source_mode: str = "direct"
    input_kind: str = "market_cap_or_share_count"
    unit: Optional[str] = None
    data_as_of: Optional[str] = None
    diagnostics_json: Dict[str, Any] = field(default_factory=dict)


class BaseValuationInputProvider(ABC):
    """Base contract for market-cap and share-count valuation inputs."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_valuation_inputs(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        sync_mode: str = "incremental",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[ValuationInputSnapshot]:
        """Fetch normalized valuation input snapshots for a set of instruments."""


@dataclass(frozen=True)
class AnalystForecastSnapshot:
    """One normalized analyst coverage / consensus forecast snapshot."""

    instrument_id: str
    symbol: str
    exchange: str
    as_of_date: str
    rating_summary: Optional[str] = None
    report_count: Optional[int] = None
    institution_count: Optional[int] = None
    buy_count: Optional[int] = None
    overweight_count: Optional[int] = None
    neutral_count: Optional[int] = None
    underperform_count: Optional[int] = None
    sell_count: Optional[int] = None
    eps_fy1: Optional[float] = None
    eps_fy2: Optional[float] = None
    net_profit_fy1: Optional[float] = None
    net_profit_fy2: Optional[float] = None
    pe_fy1: Optional[float] = None
    pe_fy2: Optional[float] = None
    source: str = ""
    source_mode: str = "direct"
    forecast_json: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResearchReportSnapshot:
    """One normalized research report metadata row."""

    report_id: str
    instrument_id: str
    symbol: str
    exchange: str
    publish_date: str
    report_title: str
    institution_name: Optional[str] = None
    analyst_name: Optional[str] = None
    rating: Optional[str] = None
    rating_change: Optional[str] = None
    target_price: Optional[float] = None
    report_url: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    report_json: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SentimentEventSnapshot:
    """One normalized event / sentiment row."""

    event_id: str
    instrument_id: str
    symbol: str
    exchange: str
    event_date: str
    event_type: str
    event_subtype: Optional[str] = None
    title: Optional[str] = None
    sentiment_score: Optional[float] = None
    severity: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    details_json: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RiskSnapshot:
    """One derived risk snapshot row."""

    instrument_id: str
    symbol: str
    exchange: str
    as_of_date: str
    benchmark_instrument_id: Optional[str] = None
    volatility_20d: Optional[float] = None
    volatility_60d: Optional[float] = None
    beta_60d: Optional[float] = None
    max_drawdown_252d: Optional[float] = None
    average_turnover_20d: Optional[float] = None
    average_amount_20d: Optional[float] = None
    liability_to_asset: Optional[float] = None
    current_ratio: Optional[float] = None
    operating_cf_to_net_income: Optional[float] = None
    negative_event_count_30d: Optional[int] = None
    risk_score: Optional[float] = None
    risk_level: Optional[str] = None
    calc_method: str = "risk_snapshot_builtin"
    calc_version: str = "risk_snapshot.v1"
    parameter_hash: str = ""
    source: str = "local_quotes_financial_facts"
    source_mode: str = "derived"
    details_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BetaResult:
    """One on-demand benchmark-aware beta calculation result."""

    instrument_id: str
    symbol: str
    exchange: str
    as_of_date: str
    benchmark_family: str
    benchmark_instrument_id: str
    benchmark_name: Optional[str] = None
    window_days: int = 60
    status: str = "success"
    missing_reason: Optional[str] = None
    beta: Optional[float] = None
    alpha: Optional[float] = None
    correlation: Optional[float] = None
    r_squared: Optional[float] = None
    stock_volatility: Optional[float] = None
    benchmark_volatility: Optional[float] = None
    residual_volatility: Optional[float] = None
    tracking_error: Optional[float] = None
    standard_error_beta: Optional[float] = None
    t_stat_beta: Optional[float] = None
    p_value_beta: Optional[float] = None
    quality_flag: Optional[str] = None
    interpretation_flags: List[str] = field(default_factory=list)
    observation_count: int = 0
    min_observation_count: int = 0
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    stock_adjustment: str = "none"
    benchmark_adjustment: str = "none"
    calc_method: str = "beta_ols_daily_return"
    calc_version: str = "beta_on_demand.v1"
    parameter_hash: str = ""
    source: str = "local_quotes"
    source_mode: str = "derived"
    details_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TechnicalIndicatorLatestSnapshot:
    """One derived latest technical indicator snapshot."""

    instrument_id: str
    symbol: str
    exchange: str
    period: str
    as_of_date: str
    adjustment: str
    applied_adjustment: str
    calc_method: str = "ta_builtin"
    calc_version: str = "technical_summary.v1"
    parameter_hash: str = ""
    status: str = "complete"
    missing_reason: Optional[str] = None
    signal: str = "neutral"
    trend_score: Optional[float] = None
    close_price: Optional[float] = None
    pct_change_1d: Optional[float] = None
    pct_change_20d: Optional[float] = None
    sma20: Optional[float] = None
    sma60: Optional[float] = None
    ema12: Optional[float] = None
    ema26: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    rsi14: Optional[float] = None
    adx: Optional[float] = None
    plus_di: Optional[float] = None
    minus_di: Optional[float] = None
    stoch_k: Optional[float] = None
    stoch_d: Optional[float] = None
    cci: Optional[float] = None
    williams_r: Optional[float] = None
    boll_upper: Optional[float] = None
    boll_middle: Optional[float] = None
    boll_lower: Optional[float] = None
    atr14: Optional[float] = None
    volume_ratio: Optional[float] = None
    distance_to_sma20: Optional[float] = None
    distance_to_sma60: Optional[float] = None
    source: str = "local_quotes"
    source_mode: str = "derived"
    summary_json: Dict[str, Any] = field(default_factory=dict)


class BaseFinancialStatementsProvider(ABC):
    """Base contract for full financial statement providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_financial_statement_bundles(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
        report_periods: Optional[List[str]] = None,
    ) -> List[FinancialStatementBundle]:
        """Fetch financial statement bundles for configured report periods."""


class BaseOfficialFinancialFilingProvider(ABC):
    """Base contract for official structured financial filing providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_financial_filings(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        report_periods: List[str],
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[FinancialFilingPayload]:
        """Fetch structured filing payloads by instrument and report period."""


@dataclass(frozen=True)
class IndustryTaxonomySnapshot:
    """Normalized industry taxonomy node from one provider."""

    taxonomy_system: str
    industry_code: str
    industry_name: str
    taxonomy_version: Optional[str] = None
    industry_level: int = 1
    parent_code: Optional[str] = None
    source_classification: Optional[str] = None
    sw_index_code: Optional[str] = None
    aliases_json: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    source_mode: str = "direct"
    raw_payload: Dict[str, Any] = field(default_factory=dict)


def build_taxonomy_children_index(
    taxonomy_nodes: List["IndustryTaxonomySnapshot"],
) -> Dict[str, List["IndustryTaxonomySnapshot"]]:
    """Index taxonomy children by parent industry code."""

    children_by_parent: Dict[str, List[IndustryTaxonomySnapshot]] = {}
    for node in taxonomy_nodes:
        parent_code = str(node.parent_code or "").strip()
        if parent_code:
            children_by_parent.setdefault(parent_code, []).append(node)
    return children_by_parent


def get_leaf_taxonomy_nodes(
    taxonomy_nodes: List["IndustryTaxonomySnapshot"],
    *,
    minimum_level: int = 2,
) -> List["IndustryTaxonomySnapshot"]:
    """Return taxonomy nodes with no children at or above the requested level."""

    children_by_parent = build_taxonomy_children_index(taxonomy_nodes)
    leaves: List[IndustryTaxonomySnapshot] = []
    for node in taxonomy_nodes:
        if int(node.industry_level) < minimum_level:
            continue
        if children_by_parent.get(str(node.industry_code)):
            continue
        leaves.append(node)
    return leaves


@dataclass(frozen=True)
class IndustrySnapshot:
    """Normalized latest industry membership snapshot from one provider."""

    instrument_id: str
    symbol: str
    exchange: str
    taxonomy_system: str
    industry_code: str
    industry_name: str
    industry_level: int = 1
    parent_code: Optional[str] = None
    taxonomy_version: Optional[str] = None
    mapping_status: str = "reference_only"
    effective_date: Optional[str] = None
    source_classification: Optional[str] = None
    source_industry_name: Optional[str] = None
    sw_l1_code: Optional[str] = None
    sw_l1_name: Optional[str] = None
    sw_l2_code: Optional[str] = None
    sw_l2_name: Optional[str] = None
    sw_l3_code: Optional[str] = None
    sw_l3_name: Optional[str] = None
    sw_l1_index_code: Optional[str] = None
    sw_l2_index_code: Optional[str] = None
    sw_l3_index_code: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    membership_json: Dict[str, Any] = field(default_factory=dict)
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IndustrySourceFileSnapshot:
    """Metadata for one fetched official industry source artifact."""

    source: str
    source_mode: str
    artifact_kind: str
    url: str
    parser_version: str
    status: str = "downloaded"
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    content_length: Optional[int] = None
    sha256: Optional[str] = None
    row_count: int = 0
    max_source_update_time: Optional[str] = None
    raw_headers: Dict[str, Any] = field(default_factory=dict)
    metadata_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IndustryClassificationHistorySnapshot:
    """One official stock-to-Shenwan-classification history row."""

    instrument_id: str
    symbol: str
    exchange: str
    taxonomy_system: str
    taxonomy_version: Optional[str]
    official_industry_code: str
    official_start_date: Optional[str] = None
    official_update_time: Optional[str] = None
    source_file_id: Optional[int] = None
    row_hash: str = ""
    source: str = ""
    source_mode: str = "direct"
    classification_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OfficialIndustryHistorySnapshot:
    """One latest official Shenwan stock-classification history record."""

    instrument_id: str
    symbol: str
    exchange: str
    official_industry_code: str
    start_date: Optional[str] = None
    update_time: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OfficialIndustryClassificationSnapshot:
    """Normalized latest official Shenwan classification snapshot for storage."""

    instrument_id: str
    symbol: str
    exchange: str
    taxonomy_system: str
    taxonomy_version: Optional[str] = None
    official_industry_code: str = ""
    official_start_date: Optional[str] = None
    official_update_time: Optional[str] = None
    mapped_industry_code: Optional[str] = None
    mapped_industry_name: Optional[str] = None
    mapped_industry_level: Optional[int] = None
    mapped_parent_code: Optional[str] = None
    mapping_status: str = "unmapped"
    mapping_confidence: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    classification_json: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IndustryIndexAnalysisSnapshot:
    """One official Shenwan index-analysis daily row."""

    taxonomy_system: str
    sw_index_code: str
    trade_date: str
    taxonomy_version: Optional[str] = None
    sw_index_name: str = ""
    index_type: Optional[str] = None
    close_index: Optional[float] = None
    bargain_volume: Optional[float] = None
    markup: Optional[float] = None
    turnover_rate: Optional[float] = None
    pe: Optional[float] = None
    pb: Optional[float] = None
    mean_price: Optional[float] = None
    bargain_sum_rate: Optional[float] = None
    negotiable_share_sum: Optional[float] = None
    average_negotiable_share_sum: Optional[float] = None
    dividend_yield: Optional[float] = None
    source: str = ""
    source_mode: str = "direct"
    raw_payload: Dict[str, Any] = field(default_factory=dict)


INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS: Dict[str, Dict[str, str]] = {
    "close_index": {
        "unit": "index_points",
        "description": "收盘指数点位",
    },
    "bargain_volume": {
        "unit": "100_million_shares",
        "description": "成交量，单位为亿股",
    },
    "markup": {
        "unit": "percent",
        "description": "涨跌幅，百分数值而非小数比例",
    },
    "turnover_rate": {
        "unit": "percent",
        "description": "换手率，百分数值而非小数比例",
    },
    "pe": {
        "unit": "multiple",
        "description": "市盈率倍数",
    },
    "pb": {
        "unit": "multiple",
        "description": "市净率倍数",
    },
    "mean_price": {
        "unit": "CNY_per_share",
        "description": "均价，元/股",
    },
    "bargain_sum_rate": {
        "unit": "percent",
        "description": "成交额占比，百分数值而非小数比例",
    },
    "negotiable_share_sum": {
        "unit": "100_million_CNY",
        "description": "流通市值，亿元",
    },
    "average_negotiable_share_sum": {
        "unit": "100_million_CNY",
        "description": "平均流通市值，亿元",
    },
    "dividend_yield": {
        "unit": "percent",
        "description": "股息率，百分数值而非小数比例",
    },
}


class BaseIndustryIndexAnalysisProvider(ABC):
    """Base contract for official industry index-analysis providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_latest_index_analysis(
        self,
        *,
        mode: str = "direct",
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
    ) -> List[IndustryIndexAnalysisSnapshot]:
        """Fetch latest index-analysis rows for configured index dimensions."""


class BaseIndustryProvider(ABC):
    """Base contract for latest industry membership providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_industries(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[IndustrySnapshot]:
        """Fetch latest industry membership snapshots for a set of instruments."""


class BaseIndustryStandardProvider(BaseIndustryProvider):
    """Base contract for authoritative industry taxonomy providers."""

    def get_last_fetch_metadata(self) -> Dict[str, Any]:
        """Return optional provider diagnostics from the latest fetch."""
        return {}

    @abstractmethod
    async def fetch_taxonomy(
        self,
        *,
        mode: str = "direct",
    ) -> List[IndustryTaxonomySnapshot]:
        """Fetch taxonomy nodes for one authoritative industry system."""

    async def fetch_component_sets(
        self,
        *,
        taxonomy_nodes: Optional[List[IndustryTaxonomySnapshot]] = None,
        mode: str = "direct",
    ) -> Dict[str, set[str]]:
        """Fetch stock membership sets for current taxonomy nodes when available."""
        return {}


@dataclass(frozen=True)
class IndustryNameHintSnapshot:
    """One provider-specific industry-name hint for strict taxonomy fallback."""

    instrument_id: str
    symbol: str
    exchange: str
    industry_name: str
    taxonomy_system: str = "sw"
    taxonomy_version: Optional[str] = None
    source_classification: Optional[str] = None
    source: str = ""
    source_mode: str = "direct"
    raw_payload: Dict[str, Any] = field(default_factory=dict)


class BaseIndustryNameSupplementProvider(ABC):
    """Base contract for name-based strict industry supplement providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_industry_name_hints(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[IndustryNameHintSnapshot]:
        """Fetch stock-level industry-name hints for taxonomy-name matching."""


class BaseOfficialIndustryHistoryProvider(ABC):
    """Base contract for official stock-classification history providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_latest_classifications(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[OfficialIndustryHistorySnapshot]:
        """Fetch the latest official stock-classification record for each target."""

    @abstractmethod
    async def fetch_all_latest_classifications(
        self,
        *,
        mode: str = "direct",
    ) -> List[OfficialIndustryHistorySnapshot]:
        """Fetch latest official stock-classification records for the whole universe."""


class BaseAnalystForecastProvider(ABC):
    """Base contract for analyst forecast / coverage providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_analyst_forecasts(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[AnalystForecastSnapshot]:
        """Fetch normalized analyst forecast snapshots."""


class BaseResearchReportProvider(ABC):
    """Base contract for research report metadata providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_research_reports(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[ResearchReportSnapshot]:
        """Fetch normalized research report metadata rows."""


class BaseSentimentEventProvider(ABC):
    """Base contract for event / sentiment providers."""

    source_name: str = ""
    supported_modes: set[str] = {"direct"}

    def supports_mode(self, mode: str) -> bool:
        return mode in self.supported_modes

    @abstractmethod
    async def fetch_sentiment_events(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[SentimentEventSnapshot]:
        """Fetch normalized event / sentiment rows."""
