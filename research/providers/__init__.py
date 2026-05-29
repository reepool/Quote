"""
Research data providers.
"""

from .akshare_financial_statements import AkshareFinancialStatementsProvider
from .akshare_analyst_forecasts import AkshareAnalystForecastProvider
from .akshare_research_reports import AkshareResearchReportProvider
from .akshare_sentiment_events import AkshareSentimentEventProvider
from .akshare_official_shenwan_history import AkshareOfficialShenwanHistoryProvider
from .akshare_shareholders import AkshareShareholdersProvider
from .akshare_shenwan_industry import AkshareShenwanIndustryProvider
from .akshare_swsresearch_index_analysis import AkshareSWSResearchIndexAnalysisProvider
from .akshare_valuation_inputs import AkshareCninfoValuationInputProvider
from .eastmoney_industry_supplement import EastmoneyIndustryNameSupplementProvider
from .manual_industry_supplement import ManualIndustryNameSupplementProvider
from .official_financial_filings import ConfiguredOfficialFinancialFilingProvider
from .sina_industry_supplement import SinaIndustryNameSupplementProvider
from .swsresearch_shenwan_classification import (
    SWSResearchClassificationBundle,
    SWSResearchShenwanClassificationProvider,
)
from .base import (
    AnalystForecastSnapshot,
    BaseAnalystForecastProvider,
    BaseCompanyProfileProvider,
    BaseIndustryNameSupplementProvider,
    BaseIndustryIndexAnalysisProvider,
    BaseShareholderProvider,
    BaseOfficialIndustryHistoryProvider,
    OfficialIndustryClassificationSnapshot,
    IndustryNameHintSnapshot,
    BaseResearchReportProvider,
    BaseSentimentEventProvider,
    BaseValuationInputProvider,
    CompanyProfileSnapshot,
    IndustryClassificationHistorySnapshot,
    IndustrySourceFileSnapshot,
    IndustryIndexAnalysisSnapshot,
    INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS,
    OfficialIndustryHistorySnapshot,
    ResearchReportSnapshot,
    RiskSnapshot,
    SentimentEventSnapshot,
)
from .efinance_shareholders import EfinanceShareholdersProvider
from .cninfo_shareholders import CninfoShareholdersProvider
from .baostock_company_profile import BaostockCompanyProfileProvider
from .baostock_financial_summary import BaostockFinancialSummaryProvider
from .baostock_industry import BaostockIndustryProvider
from .base import (
    BaseFinancialStatementsProvider,
    BaseFinancialSummaryProvider,
    BaseOfficialFinancialFilingProvider,
    FinancialFilingPayload,
    FinancialFactsSnapshot,
    FinancialIndicatorSnapshot,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
    ValuationHistorySnapshot,
    ValuationInputSnapshot,
    BaseIndustryProvider,
    BaseIndustryStandardProvider,
    FinancialSummarySnapshot,
    IndustrySnapshot,
    IndustryTaxonomySnapshot,
    ShareholderSnapshot,
)
from .pytdx_company_profile import PytdxCompanyProfileProvider
from .pytdx_financial_summary import PytdxFinancialSummaryProvider
from .pytdx_industry import PytdxIndustryProvider
from .registry import (
    AnalystForecastProviderRegistry,
    CompanyProfileProviderRegistry,
    FinancialStatementsProviderRegistry,
    FinancialSummaryProviderRegistry,
    IndustryProviderRegistry,
    IndustryIndexAnalysisProviderRegistry,
    IndustryNameSupplementProviderRegistry,
    IndustryStandardProviderRegistry,
    OfficialIndustryHistoryProviderRegistry,
    OfficialFinancialFilingProviderRegistry,
    ResearchReportProviderRegistry,
    ShareholderProviderRegistry,
    SentimentEventProviderRegistry,
    ValuationInputProviderRegistry,
)

__all__ = [
    "AnalystForecastSnapshot",
    "BaseAnalystForecastProvider",
    "BaseCompanyProfileProvider",
    "BaseIndustryNameSupplementProvider",
    "BaseIndustryIndexAnalysisProvider",
    "BaseShareholderProvider",
    "BaseOfficialIndustryHistoryProvider",
    "OfficialIndustryClassificationSnapshot",
    "IndustryNameHintSnapshot",
    "BaseResearchReportProvider",
    "BaseSentimentEventProvider",
    "BaseValuationInputProvider",
    "CompanyProfileSnapshot",
    "IndustryClassificationHistorySnapshot",
    "IndustrySourceFileSnapshot",
    "IndustryIndexAnalysisSnapshot",
    "INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS",
    "OfficialIndustryHistorySnapshot",
    "ResearchReportSnapshot",
    "RiskSnapshot",
    "SentimentEventSnapshot",
    "BaseFinancialStatementsProvider",
    "BaseOfficialFinancialFilingProvider",
    "FinancialFilingPayload",
    "FinancialSourceFileManifest",
    "FinancialNumericFactSnapshot",
    "FinancialStatementRawSnapshot",
    "FinancialFactsSnapshot",
    "FinancialIndicatorSnapshot",
    "FinancialStatementBundle",
    "ValuationHistorySnapshot",
    "ValuationInputSnapshot",
    "BaseFinancialSummaryProvider",
    "FinancialSummarySnapshot",
    "BaseIndustryProvider",
    "BaseIndustryStandardProvider",
    "IndustrySnapshot",
    "IndustryTaxonomySnapshot",
    "ShareholderSnapshot",
    "AkshareAnalystForecastProvider",
    "AkshareFinancialStatementsProvider",
    "AkshareOfficialShenwanHistoryProvider",
    "AkshareResearchReportProvider",
    "AkshareSentimentEventProvider",
    "AkshareShenwanIndustryProvider",
    "AkshareSWSResearchIndexAnalysisProvider",
    "AkshareShareholdersProvider",
    "AkshareCninfoValuationInputProvider",
    "EastmoneyIndustryNameSupplementProvider",
    "ManualIndustryNameSupplementProvider",
    "ConfiguredOfficialFinancialFilingProvider",
    "SinaIndustryNameSupplementProvider",
    "SWSResearchClassificationBundle",
    "SWSResearchShenwanClassificationProvider",
    "BaostockCompanyProfileProvider",
    "BaostockFinancialSummaryProvider",
    "BaostockIndustryProvider",
    "CninfoShareholdersProvider",
    "EfinanceShareholdersProvider",
    "PytdxCompanyProfileProvider",
    "PytdxFinancialSummaryProvider",
    "PytdxIndustryProvider",
    "AnalystForecastProviderRegistry",
    "CompanyProfileProviderRegistry",
    "FinancialStatementsProviderRegistry",
    "FinancialSummaryProviderRegistry",
    "IndustryProviderRegistry",
    "IndustryIndexAnalysisProviderRegistry",
    "IndustryNameSupplementProviderRegistry",
    "IndustryStandardProviderRegistry",
    "OfficialIndustryHistoryProviderRegistry",
    "OfficialFinancialFilingProviderRegistry",
    "ResearchReportProviderRegistry",
    "ShareholderProviderRegistry",
    "SentimentEventProviderRegistry",
    "ValuationInputProviderRegistry",
]
