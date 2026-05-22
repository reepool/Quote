"""
Provider registry for research ingestion.
"""

from __future__ import annotations

from typing import Dict, Optional

from .akshare_financial_statements import AkshareFinancialStatementsProvider
from .akshare_analyst_forecasts import AkshareAnalystForecastProvider
from .akshare_research_reports import AkshareResearchReportProvider
from .akshare_sentiment_events import AkshareSentimentEventProvider
from .akshare_official_shenwan_history import AkshareOfficialShenwanHistoryProvider
from .akshare_shareholders import AkshareShareholdersProvider
from .akshare_shenwan_industry import AkshareShenwanIndustryProvider
from .akshare_swsresearch_index_analysis import AkshareSWSResearchIndexAnalysisProvider
from .baostock_company_profile import BaostockCompanyProfileProvider
from .baostock_financial_summary import BaostockFinancialSummaryProvider
from .baostock_industry import BaostockIndustryProvider
from .cninfo_shareholders import CninfoShareholdersProvider
from .efinance_shareholders import EfinanceShareholdersProvider
from .eastmoney_industry_supplement import EastmoneyIndustryNameSupplementProvider
from .manual_industry_supplement import ManualIndustryNameSupplementProvider
from .official_financial_filings import ConfiguredOfficialFinancialFilingProvider
from .sina_industry_supplement import SinaIndustryNameSupplementProvider
from .swsresearch_index_analysis import SWSResearchIndexAnalysisProvider
from .swsresearch_shenwan_classification import SWSResearchShenwanClassificationProvider
from .base import (
    BaseAnalystForecastProvider,
    BaseCompanyProfileProvider,
    BaseFinancialStatementsProvider,
    BaseOfficialFinancialFilingProvider,
    BaseFinancialSummaryProvider,
    BaseIndustryProvider,
    BaseIndustryIndexAnalysisProvider,
    BaseIndustryNameSupplementProvider,
    BaseOfficialIndustryHistoryProvider,
    BaseIndustryStandardProvider,
    BaseResearchReportProvider,
    BaseShareholderProvider,
    BaseSentimentEventProvider,
)
from .pytdx_company_profile import PytdxCompanyProfileProvider
from .pytdx_financial_summary import PytdxFinancialSummaryProvider
from .pytdx_industry import PytdxIndustryProvider
from utils.config_manager import ResearchConfig, config_manager


class CompanyProfileProviderRegistry:
    """Registry for company profile providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseCompanyProfileProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        pytdx_profile_cfg = research_config.sources.get("pytdx", {}).get("company_profile", {})
        self._providers = providers if providers is not None else {
            "baostock": BaostockCompanyProfileProvider(),
            "pytdx": PytdxCompanyProfileProvider(**pytdx_profile_cfg),
        }

    def get(self, source_name: str) -> Optional[BaseCompanyProfileProvider]:
        return self._providers.get(source_name)


class FinancialSummaryProviderRegistry:
    """Registry for financial summary providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseFinancialSummaryProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        pytdx_cfg = research_config.sources.get("pytdx", {}).get("financial_summary", {})
        baostock_cfg = research_config.sources.get("baostock", {}).get("financial_summary", {})
        self._providers = providers if providers is not None else {
            "baostock": BaostockFinancialSummaryProvider(**baostock_cfg),
            "pytdx": PytdxFinancialSummaryProvider(**pytdx_cfg),
        }

    def get(self, source_name: str) -> Optional[BaseFinancialSummaryProvider]:
        return self._providers.get(source_name)


class FinancialStatementsProviderRegistry:
    """Registry for full financial statement providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseFinancialStatementsProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        akshare_cfg = research_config.sources.get("akshare", {}).get(
            "financial_statements",
            {},
        )
        self._providers = providers if providers is not None else {
            "akshare": AkshareFinancialStatementsProvider(provider_config=akshare_cfg),
        }

    def get(self, source_name: str) -> Optional[BaseFinancialStatementsProvider]:
        return self._providers.get(source_name)


class OfficialFinancialFilingProviderRegistry:
    """Registry for official structured financial filing providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseOfficialFinancialFilingProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        if providers is not None:
            self._providers = providers
            return

        module_cfg = research_config.modules.get("financial_statements", {})
        official_cfg = module_cfg.get("official_structured_sources", {})
        candidates = official_cfg.get("candidates", [])
        self._providers = {}
        for candidate in candidates:
            source_name = str(candidate.get("source") or "").strip()
            if not source_name:
                continue
            source_cfg = research_config.sources.get(source_name, {})
            financial_cfg = {
                **candidate,
                **source_cfg.get("financial_statements", {}),
            }
            self._providers[source_name] = ConfiguredOfficialFinancialFilingProvider(
                source_name=source_name,
                source_config=financial_cfg,
            )

    def get(self, source_name: str) -> Optional[BaseOfficialFinancialFilingProvider]:
        return self._providers.get(source_name)


class ShareholderProviderRegistry:
    """Registry for shareholder summary providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseShareholderProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        akshare_cfg = research_config.sources.get("akshare", {}).get("shareholders", {})
        self._providers = providers if providers is not None else {
            "akshare": AkshareShareholdersProvider(**akshare_cfg),
            "efinance": EfinanceShareholdersProvider(),
            "cninfo": CninfoShareholdersProvider(),
        }

    def get(self, source_name: str) -> Optional[BaseShareholderProvider]:
        return self._providers.get(source_name)


class AnalystForecastProviderRegistry:
    """Registry for analyst forecast providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseAnalystForecastProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        self._providers = providers if providers is not None else {
            "akshare": AkshareAnalystForecastProvider(),
        }

    def get(self, source_name: str) -> Optional[BaseAnalystForecastProvider]:
        return self._providers.get(source_name)


class ResearchReportProviderRegistry:
    """Registry for research report metadata providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseResearchReportProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        report_cfg = research_config.modules.get("research_reports", {})
        self._providers = providers if providers is not None else {
            "akshare": AkshareResearchReportProvider(
                max_reports_per_instrument=int(
                    report_cfg.get("max_reports_per_instrument", 20)
                )
            ),
        }

    def get(self, source_name: str) -> Optional[BaseResearchReportProvider]:
        return self._providers.get(source_name)


class SentimentEventProviderRegistry:
    """Registry for event / sentiment providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseSentimentEventProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        sentiment_cfg = research_config.modules.get("sentiment_events", {})
        self._providers = providers if providers is not None else {
            "akshare": AkshareSentimentEventProvider(
                lookback_days=int(sentiment_cfg.get("lookback_days", 7)),
                event_families=list(
                    sentiment_cfg.get(
                        "event_families",
                        ["notice", "executive_share_change", "pledge_ratio"],
                    )
                ),
            ),
        }

    def get(self, source_name: str) -> Optional[BaseSentimentEventProvider]:
        return self._providers.get(source_name)


class IndustryProviderRegistry:
    """Registry for industry providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseIndustryProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        self._providers = providers if providers is not None else {
            "baostock": BaostockIndustryProvider(),
            "pytdx": PytdxIndustryProvider(),
        }

    def get(self, source_name: str) -> Optional[BaseIndustryProvider]:
        return self._providers.get(source_name)


class IndustryStandardProviderRegistry:
    """Registry for authoritative industry standard providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseIndustryStandardProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        industry_standard_cfg = research_config.modules.get("industry", {}).get("standard", {})
        akshare_standard_cfg = research_config.sources.get("akshare", {}).get(
            "industry_standard",
            {},
        )
        swsresearch_cfg = research_config.sources.get("swsresearch", {}).get(
            "industry_standard",
            {},
        )
        self._providers = providers if providers is not None else {
            "swsresearch": SWSResearchShenwanClassificationProvider(
                taxonomy_system=industry_standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=industry_standard_cfg.get("taxonomy_version", "sw_2021"),
                stock_history_url=swsresearch_cfg.get(
                    "stock_history_url",
                    (
                        "https://www.swsresearch.com/swindex/pdf/SwClass2021/"
                        "StockClassifyUse_stock.xls"
                    ),
                ),
                code_table_url=swsresearch_cfg.get(
                    "code_table_url",
                    (
                        "https://www.swsresearch.com/swindex/pdf/SwClass2021/"
                        "SwClassCode_2021.xls"
                    ),
                ),
                request_timeout_seconds=swsresearch_cfg.get(
                    "request_timeout_seconds",
                    30.0,
                ),
                parser_version=swsresearch_cfg.get(
                    "parser_version",
                    "swsresearch_shenwan_classification.v1",
                ),
                minimum_stock_history_rows=swsresearch_cfg.get(
                    "minimum_stock_history_rows",
                    5000,
                ),
                minimum_code_rows=swsresearch_cfg.get("minimum_code_rows", 400),
                symbol_aliases=swsresearch_cfg.get("symbol_aliases", []),
                extra_ca_cert_path=swsresearch_cfg.get("extra_ca_cert_path"),
            ),
            "akshare": AkshareShenwanIndustryProvider(
                taxonomy_system=industry_standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=industry_standard_cfg.get("taxonomy_version", "sw_2021"),
                constituent_base_url=akshare_standard_cfg.get(
                    "constituent_base_url",
                    "https://legulegu.com/stockdata/index-composition",
                ),
                constituent_request_timeout_seconds=akshare_standard_cfg.get(
                    "constituent_request_timeout_seconds",
                    8.0,
                ),
                max_constituent_fetch_seconds=akshare_standard_cfg.get(
                    "max_constituent_fetch_seconds",
                ),
                max_failed_constituent_pages=akshare_standard_cfg.get(
                    "max_failed_constituent_pages",
                    25,
                ),
                failed_code_sample_limit=akshare_standard_cfg.get(
                    "failed_code_sample_limit",
                    10,
                ),
            ),
        }

    def get(self, source_name: str) -> Optional[BaseIndustryStandardProvider]:
        return self._providers.get(source_name)


class IndustryNameSupplementProviderRegistry:
    """Registry for stock-level industry-name supplement providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseIndustryNameSupplementProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        standard_cfg = research_config.modules.get("industry", {}).get("standard", {})
        supplement_cfg = standard_cfg.get("name_supplement", {})
        eastmoney_cfg = research_config.sources.get("eastmoney", {}).get(
            "industry_standard_supplement",
            {},
        )
        sina_cfg = research_config.sources.get("sina", {}).get(
            "industry_standard_supplement",
            {},
        )
        self._providers = providers if providers is not None else {
            "manual": ManualIndustryNameSupplementProvider(
                entries=supplement_cfg.get("manual_entries", []),
                taxonomy_system=standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=standard_cfg.get("taxonomy_version", "sw_2021"),
            ),
            "sina": SinaIndustryNameSupplementProvider(
                endpoint_template=sina_cfg.get(
                    "endpoint_template",
                    (
                        "https://vip.stock.finance.sina.com.cn/corp/go.php/"
                        "vCI_CorpXiangGuan/stockid/{stockid}.phtml"
                    ),
                ),
                request_timeout_seconds=sina_cfg.get(
                    "request_timeout_seconds",
                    8.0,
                ),
                request_interval_seconds=sina_cfg.get(
                    "request_interval_seconds",
                    0.1,
                ),
                retry_attempts=sina_cfg.get("retry_attempts", 2),
                retry_backoff_seconds=sina_cfg.get(
                    "retry_backoff_seconds",
                    0.5,
                ),
                taxonomy_system=standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=standard_cfg.get("taxonomy_version", "sw_2021"),
            ),
            "eastmoney": EastmoneyIndustryNameSupplementProvider(
                endpoint=eastmoney_cfg.get(
                    "endpoint",
                    "https://push2.eastmoney.com/api/qt/stock/get",
                ),
                fields=eastmoney_cfg.get("fields", "f57,f58,f127"),
                request_timeout_seconds=eastmoney_cfg.get(
                    "request_timeout_seconds",
                    8.0,
                ),
                request_interval_seconds=eastmoney_cfg.get(
                    "request_interval_seconds",
                    0.05,
                ),
                retry_attempts=eastmoney_cfg.get("retry_attempts", 2),
                retry_backoff_seconds=eastmoney_cfg.get(
                    "retry_backoff_seconds",
                    0.5,
                ),
                taxonomy_system=standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=standard_cfg.get("taxonomy_version", "sw_2021"),
            ),
        }

    def get(self, source_name: str) -> Optional[BaseIndustryNameSupplementProvider]:
        return self._providers.get(source_name)


class OfficialIndustryHistoryProviderRegistry:
    """Registry for official stock-classification history providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseOfficialIndustryHistoryProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        self._providers = providers if providers is not None else {
            "swsresearch": SWSResearchShenwanClassificationProvider(),
            "akshare": AkshareOfficialShenwanHistoryProvider(),
        }

    def get(self, source_name: str) -> Optional[BaseOfficialIndustryHistoryProvider]:
        return self._providers.get(source_name)


class IndustryIndexAnalysisProviderRegistry:
    """Registry for industry index-analysis providers."""

    def __init__(
        self,
        providers: Optional[Dict[str, BaseIndustryIndexAnalysisProvider]] = None,
        research_config: Optional[ResearchConfig] = None,
    ):
        research_config = research_config or config_manager.get_research_config()
        standard_cfg = research_config.modules.get("industry", {}).get("standard", {})
        index_cfg = research_config.sources.get("swsresearch", {}).get("index_analysis", {})
        akshare_index_cfg = research_config.sources.get("akshare", {}).get(
            "index_analysis",
            {},
        )
        self._providers = providers if providers is not None else {
            "swsresearch": SWSResearchIndexAnalysisProvider(
                endpoint=index_cfg.get(
                    "endpoint",
                    (
                        "https://www.swsresearch.com/institute-sw/api/index_analysis/"
                        "day_week_month_report/"
                    ),
                ),
                taxonomy_system=standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=standard_cfg.get("taxonomy_version", "sw_2021"),
                index_types=index_cfg.get("supported_index_types"),
                request_timeout_seconds=index_cfg.get("request_timeout_seconds", 20.0),
                retry_attempts=index_cfg.get("retry_attempts", 2),
                retry_backoff_seconds=index_cfg.get("retry_backoff_seconds", 0.5),
                page_size=index_cfg.get("page_size", 200),
                max_pages_per_type=index_cfg.get("max_pages_per_type", 10),
                extra_ca_cert_path=index_cfg.get("extra_ca_cert_path"),
            ),
            "akshare": AkshareSWSResearchIndexAnalysisProvider(
                endpoint=akshare_index_cfg.get(
                    "endpoint",
                    (
                        "https://www.swsresearch.com/institute-sw/api/index_analysis/"
                        "index_analysis_report/"
                    ),
                ),
                taxonomy_system=standard_cfg.get("taxonomy_system", "sw"),
                taxonomy_version=standard_cfg.get("taxonomy_version", "sw_2021"),
                index_types=akshare_index_cfg.get(
                    "supported_index_types",
                    index_cfg.get("supported_index_types"),
                ),
                request_timeout_seconds=akshare_index_cfg.get(
                    "request_timeout_seconds",
                    20.0,
                ),
                retry_attempts=akshare_index_cfg.get("retry_attempts", 2),
                retry_backoff_seconds=akshare_index_cfg.get(
                    "retry_backoff_seconds",
                    0.5,
                ),
                request_interval_seconds=akshare_index_cfg.get(
                    "request_interval_seconds",
                    0.0,
                ),
                page_size=akshare_index_cfg.get("page_size", 50),
                max_pages_per_type=akshare_index_cfg.get("max_pages_per_type", 200),
            ),
        }

    def get(self, source_name: str) -> Optional[BaseIndustryIndexAnalysisProvider]:
        return self._providers.get(source_name)
