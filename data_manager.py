"""
Data Manager for the quote system.
Provides high-level data management operations with comprehensive features including
trading calendar integration, data quality assessment, and gap detection.
"""

from __future__ import annotations

# Some runtime entry points import DataManager directly instead of going through
# main.py. Keep the proxy patch bootstrap before imports that may pull in
# requests/akshare/efinance transitively.
from proxy_patch_bootstrap import install_akshare_proxy_patch as _install_akshare_proxy_patch

_install_akshare_proxy_patch(required=False)

import asyncio
from collections import Counter
from copy import deepcopy
import hashlib
import inspect
import json
import os
from calendar import monthrange
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union, Set
from datetime import datetime, date, timedelta, timezone
from dataclasses import asdict, dataclass, field, replace

import pandas as pd

from utils import dm_logger, config_manager, log_execution, LogContext, TelegramBot
# 直接导入代码转换工具，避免依赖问题
from utils.code_utils import convert_to_database_format
from database.operations import DatabaseOperations
from database.models import Instrument, DailyQuote, TradingCalendar, DataUpdateInfo, GapSkipDB
# Note: get_data_source_factory will be imported dynamically to avoid circular import
from utils.date_utils import DateUtils, get_shanghai_time
from utils.validation import DataValidator
from utils.cache import cache_manager
from research.empty_support import (
    EMPTY_PLACEHOLDER_MODE,
    EMPTY_PLACEHOLDER_REASON,
    EMPTY_PLACEHOLDER_SOURCE,
    allows_optional_empty_exchange,
    get_optional_empty_exchanges,
)
from research.financial_statement_profile import resolve_financial_statement_profile
from research.financial_source_field_mapping import MAPPING_VERSION as FINANCIAL_MAPPING_VERSION
from research.financial_industry_fact_packs import (
    INDUSTRY_FACT_PACK_VERSION,
    build_industry_pack_payload,
    get_local_core_industry_canonical_facts,
)
from instrument_master_governance import (
    AShareIndexPolicy,
    AShareStockPolicy,
    HKEXInstrumentPolicy,
    MasterGovernanceOrchestrator,
    MasterGovernanceRequirement,
    PolicyRegistry,
    SUPPORTED_MODES_BY_SCOPE,
)


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
    instrument_type: Optional[str] = None


class DataManager:
    """标准数据管理器"""

    def __init__(self):
        self.config = config_manager
        self.research_config = self.config.get_research_config()
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
        self.research_storage = None
        self.futures_storage = None

        # 复权因子内存缓存: {instrument_id: (timestamp, factors)}
        # TTL = 1 小时, 适用于 API 高频查询场景
        self._factor_cache: Dict[str, tuple] = {}
        self._FACTOR_CACHE_TTL: float = 3600.0  # 秒
        self._dcf_run_cache: Dict[str, Dict[str, Any]] = {}

    def refresh_runtime_config(self) -> None:
        """Refresh config references cached on the long-lived DataManager."""
        self.research_config = self.config.get_research_config()
        self.telegram_enabled = self.config.get_nested('telegram_config.enabled', False)
        self.data_config = self.config.get_nested('data_config', {})
        self.download_chunk_days = self.data_config.get('download_chunk_days', 7)
        self.progress_file = os.path.join(
            self.data_config.get('data_dir', 'data'),
            'download_progress.json',
        )

    async def get_cached_adjustment_factors(
        self, instrument_id: str
    ) -> List[Dict[str, Any]]:
        """获取复权因子（带内存缓存）

        缓存策略:
        - Key: instrument_id
        - TTL: 1 小时 (因子数据变化频率极低, 仅在除权日更新)
        - 失效时: 从 DB 重新加载并刷新缓存

        用于 API 层的高频查询, 避免每次请求都 hit DB.
        全量下载后若需立即生效, 可调用 invalidate_factor_cache() 清除缓存.

        Args:
            instrument_id: 品种 ID

        Returns:
            复权因子列表, 按 ex_date 升序
        """
        import time
        now = time.monotonic()
        cached = self._factor_cache.get(instrument_id)
        if cached is not None:
            ts, factors = cached
            if (now - ts) < self._FACTOR_CACHE_TTL:
                return factors

        # 缓存未命中或已过期 → 从 DB 加载
        factors = await self.db_ops.get_adjustment_factors(instrument_id)
        self._factor_cache[instrument_id] = (now, factors)
        return factors

    def invalidate_factor_cache(self, instrument_id: Optional[str] = None) -> None:
        """清除复权因子缓存

        Args:
            instrument_id: 若指定则清除单个品种缓存, 否则清除全部缓存
        """
        if instrument_id:
            self._factor_cache.pop(instrument_id, None)
        else:
            self._factor_cache.clear()
        dm_logger.debug(
            "[DataManager] Factor cache cleared for %s",
            instrument_id or "ALL"
        )



    @log_execution("DataManager", "initialize")
    async def initialize(
        self,
        *,
        include_data_sources: bool = True,
        load_progress: bool = True,
    ) -> None:
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

            self._initialize_research_storage()
            self._initialize_futures_storage()

            if include_data_sources:
                # 初始化数据源工厂
                from data_sources.source_factory import get_data_source_factory

                self.source_factory = await get_data_source_factory(self.db_ops)
            else:
                dm_logger.info(
                    "[DataManager] Skipping data source factory initialization "
                    "(include_data_sources=false)"
                )

            if load_progress:
                # 加载之前的进度
                await self._load_progress()
            else:
                dm_logger.info(
                    "[DataManager] Skipping download progress load "
                    "(load_progress=false)"
                )

            dm_logger.info("DataManager initialized successfully")

        except Exception as e:
            dm_logger.error(f"Failed to initialize DataManager: {e}")
            raise

    def _initialize_research_storage(self) -> None:
        """初始化研究域独立存储。

        该步骤是软依赖：
        - 仅在 research_config.enabled 时执行
        - 初始化失败只记录告警，不阻塞现有行情系统启动
        """
        if not self.research_config.enabled:
            return

        try:
            from research.storage import ResearchStorageManager

            self.research_storage = ResearchStorageManager(self.research_config)
            self.research_storage.initialize()
            dm_logger.info(
                "[DataManager] Research storage initialized: %s",
                self.research_config.storage.db_path,
            )
        except Exception as e:
            dm_logger.warning(
                "[DataManager] Research storage initialization failed, continuing "
                "without research storage: %s",
                e,
            )
            self.research_storage = None

    def _require_research_storage(self):
        """返回 research storage；若不可用则抛出运行时错误。"""
        if not self.research_config.enabled:
            raise RuntimeError("research_config.enabled is false")

        if self.research_storage is None:
            raise RuntimeError("research storage is not initialized")

        return self.research_storage

    def _initialize_futures_storage(self) -> None:
        """Initialize dedicated futures-domain storage when configured."""
        if not self.research_config.enabled:
            return
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        if not module_cfg.get("enabled", False):
            return
        try:
            from research.futures_market_data import (
                FuturesTradingDayGovernanceService,
                FuturesStorageManager,
                default_futures_registry,
            )

            self.futures_storage = FuturesStorageManager(self.research_config)
            self.futures_storage.initialize()
            registry = default_futures_registry(module_cfg)
            self.futures_storage.upsert_categories(registry.get("categories", []))
            self.futures_storage.upsert_instruments_and_series(
                registry["instruments"],
                registry["series"],
            )
            self.futures_storage.upsert_source_manifests(
                registry.get("source_manifests", []),
            )
            calendar_cfg = (module_cfg.get("master_data") or {}).get("calendar") or {}
            if calendar_cfg.get("seed_on_initialize", False):
                FuturesTradingDayGovernanceService(
                    self.futures_storage,
                    module_cfg,
                ).bootstrap_estimated_calendar()
            dm_logger.info(
                "[DataManager] Futures storage initialized: %s",
                self.futures_storage.db_path,
            )
        except Exception as e:
            dm_logger.warning(
                "[DataManager] Futures storage initialization failed, continuing "
                "without futures storage: %s",
                e,
            )
            self.futures_storage = None

    def _require_futures_storage(self):
        """Return futures storage or raise a structured runtime error."""
        if not self.research_config.enabled:
            raise RuntimeError("research_config.enabled is false")
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research commodity_market_data module is disabled")
        if self.futures_storage is None:
            self._initialize_futures_storage()
        if self.futures_storage is None:
            raise RuntimeError("futures storage is not initialized")
        return self.futures_storage

    @staticmethod
    def _load_research_storage_state(loader):
        """Run a lightweight research SQLite aggregate read consistently.

        Research storage methods open short-lived SQLite connections and are already
        synchronous. Keep readiness aggregate reads on this path instead of mixing
        ad-hoc thread offloading across domains.
        """
        return loader()

    def _require_research_industry_standard_config(self) -> Dict[str, Any]:
        """返回 research industry standard 配置。"""
        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            raise RuntimeError("research industry module is disabled")

        standard_cfg = industry_config.get("standard", {})
        if not standard_cfg.get("enabled", True):
            raise RuntimeError("research industry standard layer is disabled")

        return standard_cfg

    @staticmethod
    def _industry_index_analysis_field_units() -> Dict[str, Any]:
        """Return normalized field-unit metadata for Shenwan index-analysis rows."""
        from research.providers.base import INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS

        return INDUSTRY_INDEX_ANALYSIS_FIELD_UNITS

    @staticmethod
    def _is_research_target_instrument(instrument: Dict[str, Any]) -> bool:
        """Return whether one instrument belongs to the research stock universe."""
        instrument_type = instrument.get("type")
        if instrument_type is None:
            return True
        return str(instrument_type).upper() == "STOCK"

    async def _list_research_target_instrument_ids_by_exchange(
        self,
        exchange: str,
    ) -> List[str]:
        """Return current research target instrument ids for one exchange."""
        target_ids_reader = getattr(
            self.db_ops,
            "get_research_target_instrument_ids_by_exchange",
            None,
        )
        if target_ids_reader is not None:
            reader_result = target_ids_reader(
                exchange,
                is_active=True,
            )
            if inspect.isawaitable(reader_result):
                return await reader_result
            if isinstance(reader_result, (list, tuple, set)):
                return sorted(
                    {
                        str(instrument_id).strip()
                        for instrument_id in reader_result
                        if str(instrument_id).strip()
                    }
                )

        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
        return sorted(
            {
                str(instrument.get("instrument_id", "")).strip()
                for instrument in instruments
                if self._is_research_target_instrument(instrument)
                and str(instrument.get("instrument_id", "")).strip()
            }
        )

    async def _count_research_target_instruments_by_exchange(
        self,
        exchanges: List[str],
        *,
        excluded_exchanges: Optional[Set[str]] = None,
    ) -> Tuple[Dict[str, int], int]:
        """Count current target research instruments by exchange."""
        excluded = {
            str(exchange).strip().upper()
            for exchange in (excluded_exchanges or set())
            if str(exchange).strip()
        }
        counts: Dict[str, int] = {}
        total = 0
        for exchange in exchanges:
            if str(exchange).strip().upper() in excluded:
                counts[exchange] = 0
                continue
            target_ids = await self._list_research_target_instrument_ids_by_exchange(
                exchange
            )
            count = len(target_ids)
            counts[exchange] = count
            total += count
        return counts, total

    def _require_research_shareholders_config(
        self,
        *,
        require_snapshot_api: bool = False,
    ) -> Dict[str, Any]:
        """返回 research shareholders 配置，并按需检查 API 门禁。"""
        module_cfg = self.research_config.modules.get("shareholders", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research shareholders module is disabled")

        if require_snapshot_api:
            required_mode = module_cfg.get("snapshot_api_requires_mode")
            delivery_mode = module_cfg.get("delivery_mode")
            if required_mode and delivery_mode != required_mode:
                raise RuntimeError(
                    "research shareholder snapshot API requires "
                    f"{required_mode}, current delivery_mode is {delivery_mode}"
                )

        return module_cfg

    async def _get_research_instrument_info(
        self,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        return await self.db_ops.get_instrument_info(instrument_id=instrument_id)

    def _module_allows_optional_empty_exchange(
        self,
        module_name: str,
        exchange: Optional[str],
    ) -> bool:
        if not exchange:
            return False
        return allows_optional_empty_exchange(
            self.research_config,
            module_name,
            exchange,
        )

    @staticmethod
    def _empty_placeholder_timestamps() -> tuple[str, str, str]:
        now = get_shanghai_time().isoformat()
        return now, now, now

    def _build_empty_company_profile_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        name = instrument.get("name") or instrument.get("symbol") or instrument.get("instrument_id")
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "company_name": name,
            "short_name": name,
            "exchange": instrument.get("exchange"),
            "market": instrument.get("market"),
            "listed_date": instrument.get("listed_date"),
            "industry_raw": None,
            "sector_raw": None,
            "status": "active" if instrument.get("is_active", True) else "delisted",
            "source": EMPTY_PLACEHOLDER_SOURCE,
            "source_mode": EMPTY_PLACEHOLDER_MODE,
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if include_snapshot:
            payload["profile"] = {
                "status": "empty",
                "missing_reason": EMPTY_PLACEHOLDER_REASON,
                "optional_empty_exchange": instrument.get("exchange"),
            }
        return payload

    def _build_empty_industry_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        industry_cfg = self.research_config.modules.get("industry", {})
        standard_cfg = industry_cfg.get("standard", {})
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "taxonomy_system": standard_cfg.get("taxonomy_system", "sw"),
            "taxonomy_version": standard_cfg.get("taxonomy_version"),
            "industry_code": "unsupported_exchange",
            "industry_name": "未覆盖",
            "industry_level": 0,
            "parent_code": None,
            "mapping_status": "optional_empty_exchange",
            "effective_date": None,
            "source_classification": None,
            "source_industry_name": None,
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "sw_l1_index_code": None,
            "sw_l2_index_code": None,
            "sw_l3_index_code": None,
            "source": EMPTY_PLACEHOLDER_SOURCE,
            "source_mode": EMPTY_PLACEHOLDER_MODE,
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if include_snapshot:
            payload["membership"] = {
                "status": "empty",
                "missing_reason": EMPTY_PLACEHOLDER_REASON,
                "optional_empty_exchange": instrument.get("exchange"),
            }
        return payload

    def _build_empty_financial_summary_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "report_date": None,
            "pub_date": None,
            "fiscal_year": None,
            "fiscal_quarter": None,
            "currency": "CNY",
            "schema_version": "financial_summary.v1",
            "roe": None,
            "gross_margin": None,
            "net_margin": None,
            "current_ratio": None,
            "quick_ratio": None,
            "liability_to_asset": None,
            "yoy_asset": None,
            "yoy_equity": None,
            "yoy_net_profit": None,
            "cfo_to_revenue": None,
            "cfo_to_net_profit": None,
            "asset_turnover": None,
            "eps": None,
            "source": EMPTY_PLACEHOLDER_SOURCE,
            "source_mode": EMPTY_PLACEHOLDER_MODE,
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if include_snapshot:
            payload["summary"] = {
                "status": "empty",
                "missing_reason": EMPTY_PLACEHOLDER_REASON,
                "optional_empty_exchange": instrument.get("exchange"),
            }
        return payload

    def _build_empty_shareholder_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_snapshot: bool = True,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "coverage_status": "optional_empty_exchange",
            "holder_count": None,
            "holder_count_report_date": None,
            "top_holders_report_date": None,
            "top_holders_count": None,
            "top_holders_total_ratio": None,
            "control_owner_name": None,
            "control_owner_ratio": None,
            "schema_version": "shareholders.v1",
            "source": EMPTY_PLACEHOLDER_SOURCE,
            "source_mode": EMPTY_PLACEHOLDER_MODE,
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if include_snapshot:
            payload["snapshot"] = {
                "coverage_scope": [],
                "scope_sources": {},
                "missing_reason": EMPTY_PLACEHOLDER_REASON,
                "optional_empty_exchange": instrument.get("exchange"),
            }
        return payload

    def _build_empty_financial_statements_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_statements: bool = True,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "report_period": "unsupported_exchange",
            "publish_date": None,
            "fiscal_year": None,
            "fiscal_quarter": None,
            "currency": "CNY",
            "schema_version": "financial_facts.v1",
            "revenue": None,
            "gross_profit": None,
            "operating_profit": None,
            "pre_tax_profit": None,
            "net_income": None,
            "operating_cf": None,
            "total_cf": None,
            "total_assets": None,
            "total_liabilities": None,
            "equity": None,
            "current_assets": None,
            "current_liabilities": None,
            "inventory": None,
            "receivables": None,
            "fixed_assets": None,
            "intangible_assets": None,
            "shares_outstanding": None,
            "source": EMPTY_PLACEHOLDER_SOURCE,
            "source_mode": EMPTY_PLACEHOLDER_MODE,
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
            "facts": {
                "status": "empty",
                "missing_reason": EMPTY_PLACEHOLDER_REASON,
                "optional_empty_exchange": instrument.get("exchange"),
            },
            "indicators": None,
            "statements": [],
        }
        if not include_statements:
            payload["statements"] = []
        return payload

    def _build_empty_analyst_coverage_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_details: bool = True,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "status": "empty",
            "missing_reason": EMPTY_PLACEHOLDER_REASON,
            "as_of_date": get_shanghai_time().date().isoformat(),
            "rating_summary": None,
            "report_count": None,
            "institution_count": None,
            "buy_count": None,
            "overweight_count": None,
            "neutral_count": None,
            "underperform_count": None,
            "sell_count": None,
            "eps_fy1": None,
            "eps_fy2": None,
            "net_profit_fy1": None,
            "net_profit_fy2": None,
            "pe_fy1": None,
            "pe_fy2": None,
            "source": EMPTY_PLACEHOLDER_SOURCE,
            "source_mode": EMPTY_PLACEHOLDER_MODE,
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
        }
        if include_details:
            payload["forecast"] = {
                "status": "empty",
                "missing_reason": EMPTY_PLACEHOLDER_REASON,
                "optional_empty_exchange": instrument.get("exchange"),
            }
        return payload

    def _build_empty_reports_response(self, instrument: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "data_points": 0,
            "window_start": None,
            "window_end": None,
            "items": [],
        }

    def _build_empty_sentiment_events_response(
        self,
        instrument: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "data_points": 0,
            "window_start": None,
            "window_end": None,
            "items": [],
        }

    def _build_empty_company_overview_response(
        self,
        instrument: Dict[str, Any],
        *,
        include_profile_snapshot: bool,
        include_industry_snapshot: bool,
        include_financial_snapshot: bool,
    ) -> Dict[str, Any]:
        now = get_shanghai_time().isoformat()
        payload = {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "market": instrument.get("market"),
            "company_name": instrument.get("name"),
            "short_name": instrument.get("name"),
            "listed_date": instrument.get("listed_date"),
            "industry_raw": None,
            "sector_raw": None,
            "industry_system": None,
            "industry_taxonomy_version": None,
            "industry_code": None,
            "industry_name": None,
            "industry_level": None,
            "industry_mapping_status": None,
            "sw_l1_code": None,
            "sw_l1_name": None,
            "sw_l2_code": None,
            "sw_l2_name": None,
            "sw_l3_code": None,
            "sw_l3_name": None,
            "status": "active" if instrument.get("is_active", True) else "delisted",
            "report_date": None,
            "pub_date": None,
            "fiscal_year": None,
            "fiscal_quarter": None,
            "currency": None,
            "schema_version": None,
            "roe": None,
            "gross_margin": None,
            "net_margin": None,
            "current_ratio": None,
            "quick_ratio": None,
            "liability_to_asset": None,
            "yoy_asset": None,
            "yoy_equity": None,
            "yoy_net_profit": None,
            "cfo_to_revenue": None,
            "cfo_to_net_profit": None,
            "asset_turnover": None,
            "eps": None,
            "data_as_of": now,
            "source_summary": {
                "company_profile": {
                    "available": False,
                    "source": None,
                    "source_mode": None,
                    "data_as_of": None,
                    "missing_reason": EMPTY_PLACEHOLDER_REASON,
                },
                "industry": {
                    "available": False,
                    "source": None,
                    "source_mode": None,
                    "data_as_of": None,
                    "missing_reason": (
                        EMPTY_PLACEHOLDER_REASON
                        if self._module_allows_optional_empty_exchange(
                            "industry",
                            instrument.get("exchange"),
                        )
                        else "snapshot_not_available"
                    ),
                },
                "financial_summary": {
                    "available": False,
                    "source": None,
                    "source_mode": None,
                    "data_as_of": None,
                    "missing_reason": EMPTY_PLACEHOLDER_REASON,
                },
            },
            "missing_sections": ["company_profile", "industry", "financial_summary"],
        }
        if include_profile_snapshot:
            payload["company_profile"] = self._build_empty_company_profile_response(
                instrument,
                include_snapshot=True,
            )
        if include_industry_snapshot:
            payload["industry"] = self._build_empty_industry_response(
                instrument,
                include_snapshot=True,
            )
        if include_financial_snapshot:
            payload["financial_summary"] = self._build_empty_financial_summary_response(
                instrument,
                include_snapshot=True,
            )
        return payload

    async def run_company_profile_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """运行 company_profile 影子同步。

        该方法只写入 research.db，不影响现有行情库。
        """
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        from research.company_profile_sync import CompanyProfileShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='company_profile_shadow_sync',
        )
        service = CompanyProfileShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def get_research_company_profile(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 company profile。"""
        storage = self._require_research_storage()
        normalized_id = convert_to_database_format(instrument_id)
        profile = await asyncio.to_thread(
            storage.get_company_profile,
            normalized_id,
            include_snapshot=include_snapshot,
        )
        if profile is not None:
            return profile

        instrument = await self._get_research_instrument_info(normalized_id)
        if instrument and self._module_allows_optional_empty_exchange(
            "company_profile",
            instrument.get("exchange"),
        ):
            return self._build_empty_company_profile_response(
                instrument,
                include_snapshot=include_snapshot,
            )
        return None

    async def run_industry_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """运行 industry 影子同步。

        该方法只写入 research.db，不影响现有行情库。
        """
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research industry module is disabled",
            }

        from research.industry_sync import IndustryShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='industry_shadow_sync',
        )
        service = IndustryShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_industry_standard_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        instrument_ids_by_exchange: Optional[Dict[str, List[str]]] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        force_component_refresh: bool = False,
    ) -> Dict[str, Any]:
        """运行 strict Shenwan 行业标准层同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research industry module is disabled",
            }

        standard_cfg = industry_config.get("standard", {})
        if not standard_cfg.get("enabled", True):
            return {
                "status": "disabled",
                "reason": "research industry standard layer is disabled",
            }

        from research.industry_standard_sync import IndustryStandardSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='industry_standard_sync',
        )
        service = IndustryStandardSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            instrument_ids_by_exchange=instrument_ids_by_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            force_component_refresh=force_component_refresh,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def rebuild_official_industry_standard(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        drop_existing: bool = True,
        drop_source_files: bool = False,
        force_refresh: bool = True,
    ) -> Dict[str, Any]:
        """Run a controlled full rebuild of strict Shenwan official rows."""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research industry module is disabled",
            }

        standard_cfg = industry_config.get("standard", {})
        if not standard_cfg.get("enabled", True):
            return {
                "status": "disabled",
                "reason": "research industry standard layer is disabled",
            }

        from research.industry_standard_operations import rebuild_official_industry_standard

        return await rebuild_official_industry_standard(
            self,
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            drop_existing=drop_existing,
            drop_source_files=drop_source_files,
            force_refresh=force_refresh,
        )

    async def run_industry_index_analysis_sync(
        self,
        *,
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """运行申万行业指数分析指标同步，不写股票行业归属。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        index_cfg = self.research_config.sources.get("swsresearch", {}).get(
            "index_analysis",
            {},
        )
        if not index_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "swsresearch.index_analysis.enabled is false",
            }

        from research.industry_index_analysis_sync import IndustryIndexAnalysisSyncService

        service = IndustryIndexAnalysisSyncService(
            storage=self.research_storage,
            research_config=self.research_config,
        )
        return await service.sync_latest(
            index_types=index_types,
            limit_per_type=limit_per_type,
            start_date=start_date,
            end_date=end_date,
            latest_date=latest_date,
        )

    async def run_industry_index_analysis_backfill(
        self,
        *,
        start_date: str,
        end_date: str,
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        source: str = "akshare",
        mode: str = "direct",
        chunk_frequency: Optional[str] = None,
        split_index_types: bool = True,
        stop_on_error: bool = False,
    ) -> Dict[str, Any]:
        """运行申万行业指数分析历史回补，不写股票行业归属。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        index_cfg = self.research_config.sources.get(source, {}).get(
            "index_analysis",
            {},
        )
        if not index_cfg and source == "akshare":
            index_cfg = self.research_config.sources.get("swsresearch", {}).get(
                "index_analysis",
                {},
            )
        if not index_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": f"{source}.index_analysis.enabled is false",
            }

        if chunk_frequency and chunk_frequency != "none":
            return await self._run_industry_index_analysis_backfill_chunked(
                start_date=start_date,
                end_date=end_date,
                index_types=index_types,
                limit_per_type=limit_per_type,
                source=source,
                mode=mode,
                chunk_frequency=chunk_frequency,
                split_index_types=split_index_types,
                stop_on_error=stop_on_error,
                index_cfg=index_cfg,
            )

        from research.industry_index_analysis_sync import IndustryIndexAnalysisSyncService

        service = IndustryIndexAnalysisSyncService(
            storage=self.research_storage,
            research_config=self.research_config,
        )
        return await service.sync_history(
            index_types=index_types,
            limit_per_type=limit_per_type,
            start_date=start_date,
            end_date=end_date,
            source=source,
            mode=mode,
        )

    async def _run_industry_index_analysis_backfill_chunked(
        self,
        *,
        start_date: str,
        end_date: str,
        index_types: Optional[List[str]],
        limit_per_type: Optional[int],
        source: str,
        mode: str,
        chunk_frequency: str,
        split_index_types: bool,
        stop_on_error: bool,
        index_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Run Shenwan index-analysis backfill in bounded date/type chunks."""
        start = self._parse_backfill_date(start_date)
        end = self._parse_backfill_date(end_date)
        if start > end:
            raise ValueError("start_date must be earlier than or equal to end_date")

        target_types = list(
            index_types
            or index_cfg.get("supported_index_types")
            or ["市场表征", "一级行业", "二级行业", "三级行业", "风格指数"]
        )
        chunks: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        rows_written = 0

        for chunk_start, chunk_end in self._iter_backfill_date_chunks(
            start,
            end,
            chunk_frequency,
        ):
            type_groups = (
                [[index_type] for index_type in target_types]
                if split_index_types
                else [target_types]
            )
            for type_group in type_groups:
                result = await self.run_industry_index_analysis_backfill(
                    start_date=chunk_start.isoformat(),
                    end_date=chunk_end.isoformat(),
                    index_types=type_group,
                    limit_per_type=limit_per_type,
                    source=source,
                    mode=mode,
                    chunk_frequency=None,
                )
                chunk_result = {
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "index_types": type_group,
                    "status": result.get("status"),
                    "rows_written": int(result.get("rows_written") or 0),
                    "reason": result.get("reason"),
                    "coverage": result.get("coverage"),
                }
                chunks.append(chunk_result)
                rows_written += chunk_result["rows_written"]
                if result.get("status") != "success":
                    failures.append(chunk_result)
                    if stop_on_error:
                        return self._build_index_analysis_chunked_result(
                            start=start,
                            end=end,
                            chunk_frequency=chunk_frequency,
                            split_index_types=split_index_types,
                            source=source,
                            mode=mode,
                            rows_written=rows_written,
                            chunks=chunks,
                            failures=failures,
                        )

        return self._build_index_analysis_chunked_result(
            start=start,
            end=end,
            chunk_frequency=chunk_frequency,
            split_index_types=split_index_types,
            source=source,
            mode=mode,
            rows_written=rows_written,
            chunks=chunks,
            failures=failures,
        )

    @staticmethod
    def _parse_backfill_date(value: str) -> date:
        normalized = str(value).strip()
        if len(normalized) == 8 and normalized.isdigit():
            normalized = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}"
        return date.fromisoformat(normalized)

    @staticmethod
    def _iter_backfill_date_chunks(
        start: date,
        end: date,
        frequency: str,
    ):
        current = start
        while current <= end:
            if frequency == "day":
                chunk_end = current
            elif frequency == "month":
                chunk_end = date(
                    current.year,
                    current.month,
                    monthrange(current.year, current.month)[1],
                )
            elif frequency == "quarter":
                quarter_end_month = ((current.month - 1) // 3 + 1) * 3
                chunk_end = date(
                    current.year,
                    quarter_end_month,
                    monthrange(current.year, quarter_end_month)[1],
                )
            elif frequency == "year":
                chunk_end = date(current.year, 12, 31)
            else:
                raise ValueError(
                    "chunk_frequency must be one of day, month, quarter, year, none"
                )

            if chunk_end > end:
                chunk_end = end
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    @staticmethod
    def _build_index_analysis_chunked_result(
        *,
        start: date,
        end: date,
        chunk_frequency: str,
        split_index_types: bool,
        source: str,
        mode: str,
        rows_written: int,
        chunks: List[Dict[str, Any]],
        failures: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "status": "success" if not failures else "partial_success",
            "operation": "history_backfill_chunked",
            "source": source,
            "mode": mode,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "chunk_frequency": chunk_frequency,
            "split_index_types": split_index_types,
            "chunks_total": len(chunks),
            "chunks_failed": len(failures),
            "rows_written": rows_written,
            "failures": failures,
            "chunks": chunks,
        }

    async def run_industry_official_mapping_refresh(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """刷新 strict Shenwan 官方行业码映射缓存。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research industry module is disabled",
            }

        standard_cfg = industry_config.get("standard", {})
        if not standard_cfg.get("enabled", True):
            return {
                "status": "disabled",
                "reason": "research industry standard layer is disabled",
            }

        from research.industry_standard_sync import IndustryStandardSyncService

        service = IndustryStandardSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        return await service.refresh_official_mapping_cache(
            exchanges=exchanges,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )

    async def get_research_industry_standard_coverage_gaps(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        missing_limit_per_exchange: Optional[int] = None,
        include_missing_instrument_ids: bool = True,
    ) -> Dict[str, Any]:
        """Return authoritative-membership coverage gaps for strict Shenwan current membership."""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        markets = list(exchanges or self.research_config.markets)
        optional_empty_exchanges = get_optional_empty_exchanges(
            self.research_config,
            "industry",
        )
        missing_limit = (
            None
            if missing_limit_per_exchange is None
            else max(int(missing_limit_per_exchange), 0)
        )

        target_ids_by_exchange: Dict[str, List[str]] = {}
        target_counts_by_exchange: Dict[str, int] = {}
        target_total = 0
        skipped_optional_empty_exchanges: List[str] = []

        for exchange in markets:
            normalized_exchange = str(exchange).strip().upper()
            if normalized_exchange in optional_empty_exchanges:
                target_ids_by_exchange[exchange] = []
                target_counts_by_exchange[exchange] = 0
                skipped_optional_empty_exchanges.append(exchange)
                continue

            target_ids = await self._list_research_target_instrument_ids_by_exchange(
                exchange
            )
            target_ids_by_exchange[exchange] = target_ids
            target_counts_by_exchange[exchange] = len(target_ids)
            target_total += len(target_ids)

        def _load_storage_state() -> Dict[str, List[str]]:
            return {
                exchange: storage.list_industry_membership_instrument_ids(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                    mapping_status="authoritative",
                    exchange=exchange,
                )
                for exchange in markets
                if str(exchange).strip().upper() not in optional_empty_exchanges
            }

        authoritative_ids_by_exchange = _load_storage_state()

        authoritative_total = 0
        missing_total = 0
        exchange_gaps = []
        targeted_missing_ids_by_exchange: Dict[str, List[str]] = {}

        for exchange in markets:
            target_ids = target_ids_by_exchange.get(exchange, [])
            target_id_set = set(target_ids)
            optional_empty_exchange = (
                str(exchange).strip().upper() in optional_empty_exchanges
            )
            authoritative_ids = sorted(
                target_id_set.intersection(
                    authoritative_ids_by_exchange.get(exchange, [])
                )
            )
            missing_ids = sorted(target_id_set.difference(authoritative_ids))
            authoritative_count = len(authoritative_ids)
            missing_count = len(missing_ids)
            authoritative_total += authoritative_count
            missing_total += missing_count

            if missing_limit is None:
                sampled_missing_ids = missing_ids
            else:
                sampled_missing_ids = missing_ids[:missing_limit]

            if missing_ids:
                targeted_missing_ids_by_exchange[exchange] = sampled_missing_ids

            coverage_ratio = (
                authoritative_count / len(target_ids)
                if target_ids
                else 1.0
            )
            row: Dict[str, Any] = {
                "exchange": exchange,
                "target_instruments": len(target_ids),
                "authoritative_memberships": authoritative_count,
                "missing_instrument_count": missing_count,
                "coverage_ratio": coverage_ratio,
                "ready": missing_count == 0,
                "optional_empty_exchange": optional_empty_exchange,
            }
            if include_missing_instrument_ids:
                row["missing_instrument_ids"] = sampled_missing_ids
                row["missing_ids_truncated"] = (
                    missing_limit is not None and len(sampled_missing_ids) < missing_count
                )
            exchange_gaps.append(row)

        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "optional_empty_exchanges": skipped_optional_empty_exchanges,
            "target_instrument_count": target_total,
            "target_instruments_by_exchange": target_counts_by_exchange,
            "authoritative_membership_total": authoritative_total,
            "missing_authoritative_membership_count": missing_total,
            "ready": missing_total == 0,
            "missing_limit_per_exchange": missing_limit_per_exchange,
            "exchange_gaps": exchange_gaps,
            "targeted_missing_instrument_ids_by_exchange": (
                targeted_missing_ids_by_exchange
                if include_missing_instrument_ids
                else {}
            ),
        }

    async def run_industry_standard_gap_fill_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        missing_limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Detect authoritative-membership gaps and run a targeted strict Shenwan sync."""
        coverage_before = await self.get_research_industry_standard_coverage_gaps(
            exchanges=exchanges,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            missing_limit_per_exchange=missing_limit_per_exchange,
            include_missing_instrument_ids=True,
        )
        instrument_ids_by_exchange = {
            exchange: instrument_ids
            for exchange, instrument_ids in (
                coverage_before.get("targeted_missing_instrument_ids_by_exchange") or {}
            ).items()
            if instrument_ids
        }
        targeted_exchanges = list(instrument_ids_by_exchange)
        targeted_instrument_count = sum(
            len(instrument_ids) for instrument_ids in instrument_ids_by_exchange.values()
        )

        if targeted_instrument_count == 0:
            return {
                "status": "skipped",
                "reason": "no_missing_authoritative_memberships",
                "requested": {
                    "exchanges": exchanges,
                    "taxonomy_system": taxonomy_system,
                    "taxonomy_version": taxonomy_version,
                    "missing_limit_per_exchange": missing_limit_per_exchange,
                    "budget_mode": budget_mode,
                    "allow_paid_proxy": allow_paid_proxy,
                },
                "coverage_before": coverage_before,
                "coverage_after": coverage_before,
                "targeted_exchanges": [],
                "targeted_instrument_count": 0,
                "targeted_missing_instrument_ids_by_exchange": {},
                "sync": {"status": "skipped", "reason": "no_missing_authoritative_memberships"},
            }

        sync_result = await self.run_industry_standard_sync(
            exchanges=targeted_exchanges,
            instrument_ids_by_exchange=instrument_ids_by_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        coverage_after = await self.get_research_industry_standard_coverage_gaps(
            exchanges=exchanges,
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            missing_limit_per_exchange=missing_limit_per_exchange,
            include_missing_instrument_ids=True,
        )
        missing_before = int(
            coverage_before.get("missing_authoritative_membership_count", 0)
        )
        missing_after = int(
            coverage_after.get("missing_authoritative_membership_count", 0)
        )
        repaired_instrument_count = max(missing_before - missing_after, 0)

        sync_status = str(sync_result.get("status", "failed"))
        if sync_status == "failed":
            status = "failed"
        elif missing_after == 0:
            status = "success"
        elif repaired_instrument_count > 0 or sync_status in {"success", "degraded"}:
            status = "degraded"
        else:
            status = sync_status

        return {
            "status": status,
            "requested": {
                "exchanges": exchanges,
                "taxonomy_system": taxonomy_system,
                "taxonomy_version": taxonomy_version,
                "missing_limit_per_exchange": missing_limit_per_exchange,
                "budget_mode": budget_mode,
                "allow_paid_proxy": allow_paid_proxy,
            },
            "targeted_exchanges": targeted_exchanges,
            "targeted_instrument_count": targeted_instrument_count,
            "targeted_missing_instrument_ids_by_exchange": instrument_ids_by_exchange,
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
            "repaired_instrument_count": repaired_instrument_count,
            "remaining_missing_instrument_count": missing_after,
            "sync": sync_result,
        }

    async def get_research_industry(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 industry membership。"""
        storage = self._require_research_storage()
        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            raise RuntimeError("research industry module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        membership = await asyncio.to_thread(
            storage.get_industry_membership,
            normalized_id,
            include_snapshot=include_snapshot,
        )
        if membership is not None:
            return membership

        instrument = await self._get_research_instrument_info(normalized_id)
        if instrument and self._module_allows_optional_empty_exchange(
            "industry",
            instrument.get("exchange"),
        ):
            return self._build_empty_industry_response(
                instrument,
                include_snapshot=include_snapshot,
            )
        return None

    async def list_research_industry_taxonomy(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        industry_level: Optional[int] = None,
        parent_code: Optional[str] = None,
        industry_code: Optional[str] = None,
        sw_index_code: Optional[str] = None,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """读取研究域标准行业 taxonomy 节点。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )

        items = await asyncio.to_thread(
            storage.list_industry_taxonomy_records,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            industry_level=industry_level,
            parent_code=parent_code,
            industry_code=industry_code,
            sw_index_code=sw_index_code,
            active_only=active_only,
            limit=limit,
            offset=offset,
        )
        total = await asyncio.to_thread(
            storage.count_industry_taxonomy_records,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            industry_level=industry_level,
            parent_code=parent_code,
            industry_code=industry_code,
            sw_index_code=sw_index_code,
            active_only=active_only,
        )
        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "industry_level": industry_level,
            "parent_code": parent_code,
            "industry_code": industry_code,
            "sw_index_code": sw_index_code,
            "active_only": active_only,
            "limit": limit,
            "offset": offset,
            "total": total,
            "items": items,
        }

    async def list_research_industry_component_sets(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        industry_code: Optional[str] = None,
        sw_index_code: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        include_symbols: bool = True,
    ) -> Dict[str, Any]:
        """读取研究域标准行业成分缓存。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        resolved_industry_code = industry_code
        missing_reason = None

        if sw_index_code and not resolved_industry_code:
            taxonomy_nodes = await asyncio.to_thread(
                storage.list_industry_taxonomy_records,
                taxonomy_system=resolved_taxonomy_system,
                taxonomy_version=resolved_taxonomy_version,
                sw_index_code=sw_index_code,
                active_only=True,
                limit=1,
                offset=0,
            )
            if taxonomy_nodes:
                resolved_industry_code = str(taxonomy_nodes[0]["industry_code"])
            else:
                missing_reason = "taxonomy_alias_not_found"

        if missing_reason and not resolved_industry_code:
            items = []
            total = 0
        else:
            items = await asyncio.to_thread(
                storage.list_industry_component_set_records,
                taxonomy_system=resolved_taxonomy_system,
                taxonomy_version=resolved_taxonomy_version,
                industry_code=resolved_industry_code,
                max_age_days=max_age_days,
                limit=limit,
                offset=offset,
                include_symbols=include_symbols,
            )
            total = await asyncio.to_thread(
                storage.count_industry_component_sets,
                taxonomy_system=resolved_taxonomy_system,
                taxonomy_version=resolved_taxonomy_version,
                industry_code=resolved_industry_code,
                max_age_days=max_age_days,
            )
            if total == 0:
                items, total = await asyncio.to_thread(
                    storage.list_industry_component_set_records_from_memberships,
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                    industry_code=resolved_industry_code,
                    max_age_days=max_age_days,
                    limit=limit,
                    offset=offset,
                    include_symbols=include_symbols,
                )
        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "industry_code": industry_code,
            "sw_index_code": sw_index_code,
            "resolved_industry_code": resolved_industry_code,
            "missing_reason": missing_reason,
            "max_age_days": max_age_days,
            "include_symbols": include_symbols,
            "limit": limit,
            "offset": offset,
            "total": total,
            "items": items,
        }

    async def list_research_industry_index_analysis(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        sw_index_code: Optional[str] = None,
        index_type: Optional[str] = None,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        include_payload: bool = True,
    ) -> Dict[str, Any]:
        """读取申万行业指数分析日度数据。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )

        items = await asyncio.to_thread(
            storage.list_industry_index_analysis_daily,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            sw_index_code=sw_index_code,
            index_type=index_type,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
            include_payload=include_payload,
        )
        total = await asyncio.to_thread(
            storage.count_industry_index_analysis_daily,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            sw_index_code=sw_index_code,
            index_type=index_type,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
        )
        summary = await asyncio.to_thread(
            storage.summarize_industry_index_analysis_daily,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
        )
        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "sw_index_code": sw_index_code,
            "index_type": index_type,
            "trade_date": trade_date,
            "start_date": start_date,
            "end_date": end_date,
            "include_payload": include_payload,
            "limit": limit,
            "offset": offset,
            "total": total,
            "summary": summary,
            "field_units": self._industry_index_analysis_field_units(),
            "items": items,
        }

    async def get_research_industry_index_analysis_latest(
        self,
        sw_index_code: str,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        include_payload: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取单个申万指数代码的最新行业指数分析数据。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        return await asyncio.to_thread(
            storage.get_latest_industry_index_analysis,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            sw_index_code=sw_index_code,
            include_payload=include_payload,
        )

    async def get_research_industry_index_analysis_latest_by_taxonomy(
        self,
        industry_code: str,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        include_payload: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """通过 taxonomy 显式 index alias 读取最新申万指数分析 benchmark。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        nodes = await asyncio.to_thread(
            storage.list_industry_taxonomy_records,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            industry_code=industry_code,
            active_only=True,
            limit=1,
            offset=0,
        )
        if not nodes:
            return None

        node = nodes[0]
        sw_index_code = node.get("sw_index_code")
        if not sw_index_code:
            return {
                "taxonomy_system": resolved_taxonomy_system,
                "taxonomy_version": resolved_taxonomy_version,
                "industry_code": industry_code,
                "sw_index_code": None,
                "missing_reason": "taxonomy_node_has_no_sw_index_code",
                "taxonomy_node": node,
                "index_analysis": None,
            }

        latest = await asyncio.to_thread(
            storage.get_latest_industry_index_analysis,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            sw_index_code=str(sw_index_code),
            include_payload=include_payload,
        )
        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "industry_code": industry_code,
            "sw_index_code": str(sw_index_code),
            "missing_reason": None if latest else "index_analysis_not_found",
            "taxonomy_node": node,
            "index_analysis": latest,
        }

    async def _get_research_industry_index_benchmark_for_membership(
        self,
        storage: Any,
        industry_membership: Optional[Dict[str, Any]],
        benchmark_field: Optional[str],
        *,
        include_payload: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Resolve official SWS index-analysis benchmark from explicit membership aliases."""
        if not industry_membership or not benchmark_field:
            return None
        alias_field_map = {
            "sw_l1_code": "sw_l1_index_code",
            "sw_l2_code": "sw_l2_index_code",
            "sw_l3_code": "sw_l3_index_code",
        }
        alias_field = alias_field_map.get(str(benchmark_field))
        if not alias_field:
            return None

        sw_index_code = str(industry_membership.get(alias_field) or "").strip()
        payload = {
            "benchmark_field": benchmark_field,
            "alias_field": alias_field,
            "sw_index_code": sw_index_code or None,
            "missing_reason": None,
            "index_analysis": None,
        }
        if not sw_index_code:
            payload["missing_reason"] = "membership_has_no_sw_index_alias"
            return payload

        latest = await asyncio.to_thread(
            storage.get_latest_industry_index_analysis,
            taxonomy_system=industry_membership.get("taxonomy_system", "sw"),
            taxonomy_version=industry_membership.get("taxonomy_version"),
            sw_index_code=sw_index_code,
            include_payload=include_payload,
        )
        payload["index_analysis"] = latest
        if latest is None:
            payload["missing_reason"] = "index_analysis_not_found"
        return payload

    async def list_research_official_industry_code_mappings(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        mapping_status: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        include_mapping: bool = True,
    ) -> Dict[str, Any]:
        """读取 official Shenwan six-digit code 映射缓存列表。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )

        items = await asyncio.to_thread(
            storage.list_official_industry_code_mappings,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            mapping_status=mapping_status,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=limit,
            offset=offset,
            include_mapping=include_mapping,
        )
        total = await asyncio.to_thread(
            storage.count_official_industry_code_mappings,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            mapping_status=mapping_status,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
        )
        mapping_status_counts = await asyncio.to_thread(
            storage.summarize_official_industry_code_mappings,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
        )
        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "mapping_status": mapping_status,
            "source": source,
            "source_mode": source_mode,
            "max_age_days": max_age_days,
            "limit": limit,
            "offset": offset,
            "total": total,
            "mapping_status_counts": mapping_status_counts,
            "items": items,
        }

    async def get_research_official_industry_code_mapping(
        self,
        official_industry_code: str,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        include_mapping: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取单条 official Shenwan six-digit code 映射缓存。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        return await asyncio.to_thread(
            storage.get_official_industry_code_mapping,
            official_industry_code,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            include_mapping=include_mapping,
        )

    async def list_research_unmapped_official_industry_code_backlog(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        include_mapping: bool = True,
        override_candidate_ready_only: bool = False,
    ) -> Dict[str, Any]:
        """读取 unmapped official-code backlog 视图。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        backlog_review_cfg = self._official_mapping_backlog_review_config(standard_cfg)

        items = await asyncio.to_thread(
            storage.list_unmapped_official_industry_code_backlog,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=limit,
            offset=offset,
            include_mapping=include_mapping,
        )
        backlog_summary = await asyncio.to_thread(
            storage.summarize_unmapped_official_industry_code_backlog,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
        )
        enriched_items = [
            self._enrich_unmapped_backlog_item(item, backlog_review_cfg)
            for item in items
        ]
        if override_candidate_ready_only:
            enriched_items = [
                item
                for item in enriched_items
                if bool(item.get("override_candidate_ready"))
            ]

        review_priority_counts: Dict[str, int] = {}
        override_candidate_total = 0
        filtered_current_classification_total = 0
        for item in enriched_items:
            priority = str(item.get("review_priority") or "unknown")
            review_priority_counts[priority] = review_priority_counts.get(priority, 0) + 1
            if bool(item.get("override_candidate_ready")):
                override_candidate_total += 1
            filtered_current_classification_total += int(
                item.get("current_classification_count", 0) or 0
            )

        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "source": source,
            "source_mode": source_mode,
            "max_age_days": max_age_days,
            "limit": limit,
            "offset": offset,
            "total": len(enriched_items)
            if override_candidate_ready_only
            else int(backlog_summary.get("official_code_total", 0)),
            "current_classification_total": filtered_current_classification_total
            if override_candidate_ready_only
            else int(backlog_summary.get("current_classification_total", 0)),
            "override_candidate_total": override_candidate_total,
            "review_priority_counts": review_priority_counts,
            "items": enriched_items,
        }

    async def list_research_official_mapping_override_candidates(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
        include_mapping: bool = True,
    ) -> Dict[str, Any]:
        """导出可进入 manual_overrides 审核的 official-code 候选集合。"""
        backlog_payload = await self.list_research_unmapped_official_industry_code_backlog(
            taxonomy_system=taxonomy_system,
            taxonomy_version=taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=limit,
            offset=offset,
            include_mapping=include_mapping,
            override_candidate_ready_only=True,
        )
        manual_overrides: Dict[str, Dict[str, Any]] = {}
        for item in backlog_payload.get("items", []):
            suggestion = item.get("manual_override_suggestion") or {}
            official_industry_code = suggestion.get("official_industry_code")
            taxonomy_industry_code = suggestion.get("taxonomy_industry_code")
            if not official_industry_code or not taxonomy_industry_code:
                continue
            manual_overrides[str(official_industry_code)] = {
                "taxonomy_industry_code": str(taxonomy_industry_code),
                "confidence": suggestion.get("confidence"),
                "reason": suggestion.get("reason"),
            }

        return {
            "taxonomy_system": backlog_payload.get("taxonomy_system"),
            "taxonomy_version": backlog_payload.get("taxonomy_version"),
            "source": backlog_payload.get("source"),
            "source_mode": backlog_payload.get("source_mode"),
            "max_age_days": backlog_payload.get("max_age_days"),
            "limit": backlog_payload.get("limit"),
            "offset": backlog_payload.get("offset"),
            "total": backlog_payload.get("total", 0),
            "current_classification_total": backlog_payload.get(
                "current_classification_total", 0
            ),
            "override_candidate_total": backlog_payload.get(
                "override_candidate_total", 0
            ),
            "review_priority_counts": backlog_payload.get(
                "review_priority_counts", {}
            ),
            "manual_overrides": manual_overrides,
            "items": backlog_payload.get("items", []),
        }

    async def get_research_official_mapping_override_review(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        max_age_days: Optional[int] = None,
        include_mapping: bool = True,
        attention_only: bool = False,
        review_status: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """聚合 configured/ready/applied official override 的审阅视图。"""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        configured_overrides = self._configured_official_mapping_overrides(standard_cfg)
        ready_payload = await self.list_research_official_mapping_override_candidates(
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            source=source,
            source_mode=source_mode,
            max_age_days=max_age_days,
            limit=500,
            offset=0,
            include_mapping=include_mapping,
        )
        persisted_rows = await asyncio.to_thread(
            storage.get_official_industry_code_mappings,
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            max_age_days=max_age_days,
        )

        applied_overrides: Dict[str, Dict[str, Any]] = {}
        for row in persisted_rows:
            mapping = row.get("mapping") or {}
            if str(mapping.get("mapping_source") or "") != "manual_override":
                continue
            official_industry_code = str(row.get("official_industry_code") or "").strip()
            if not official_industry_code:
                continue
            applied_overrides[official_industry_code] = {
                "official_industry_code": official_industry_code,
                "taxonomy_industry_code": row.get("mapped_industry_code")
                or row.get("best_taxonomy_industry_code"),
                "mapping_source": mapping.get("mapping_source"),
                "override_reason": mapping.get("override_reason"),
                "built_at": row.get("built_at"),
                "source": row.get("source"),
                "source_mode": row.get("source_mode"),
            }

        ready_candidates = {
            str(code): dict(payload)
            for code, payload in (ready_payload.get("manual_overrides") or {}).items()
        }
        union_codes = sorted(
            set(configured_overrides)
            | set(ready_candidates)
            | set(applied_overrides)
        )
        requested_statuses = {
            str(status).strip()
            for status in (review_status or [])
            if str(status).strip()
        }

        items: List[Dict[str, Any]] = []
        for official_industry_code in union_codes:
            configured_override = configured_overrides.get(official_industry_code)
            ready_candidate = ready_candidates.get(official_industry_code)
            applied_override = applied_overrides.get(official_industry_code)
            review_status, status_reason = self._classify_official_override_review_status(
                configured_override=configured_override,
                ready_candidate=ready_candidate,
                applied_override=applied_override,
            )
            items.append(
                {
                    "official_industry_code": official_industry_code,
                    "review_status": review_status,
                    "status_reason": status_reason,
                    "configured_override": configured_override,
                    "ready_candidate": ready_candidate,
                    "applied_override": applied_override,
                }
            )

        if attention_only:
            items = [
                item
                for item in items
                if str(item.get("review_status") or "") != "configured_and_applied"
            ]
        if requested_statuses:
            items = [
                item
                for item in items
                if str(item.get("review_status") or "") in requested_statuses
            ]

        status_counts: Dict[str, int] = {}
        pending_manual_overrides: Dict[str, Dict[str, Any]] = {}
        for item in items:
            item_review_status = str(item.get("review_status") or "")
            status_counts[item_review_status] = status_counts.get(item_review_status, 0) + 1
            ready_candidate = item.get("ready_candidate")
            if item_review_status == "ready_candidate_pending_config" and ready_candidate:
                pending_manual_overrides[str(item.get("official_industry_code"))] = dict(
                    ready_candidate
                )

        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "source": source,
            "source_mode": source_mode,
            "max_age_days": max_age_days,
            "attention_only": attention_only,
            "review_status": sorted(requested_statuses),
            "configured_override_total": len(configured_overrides),
            "ready_candidate_total": int(ready_payload.get("override_candidate_total", 0)),
            "applied_override_total": len(applied_overrides),
            "pending_manual_override_total": len(pending_manual_overrides),
            "status_counts": status_counts,
            "pending_manual_overrides": pending_manual_overrides,
            "items": items,
        }

    async def get_research_industry_standard_readiness(
        self,
        *,
        taxonomy_system: Optional[str] = None,
        taxonomy_version: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return strict Shenwan readiness and relative-valuation rollout status."""
        storage = self._require_research_storage()
        standard_cfg = self._require_research_industry_standard_config()
        resolved_taxonomy_system = taxonomy_system or standard_cfg.get(
            "taxonomy_system", "sw"
        )
        resolved_taxonomy_version = taxonomy_version or standard_cfg.get(
            "taxonomy_version"
        )
        mapping_cfg = standard_cfg.get("official_mapping", {})
        readiness_backlog_limit = int(mapping_cfg.get("readiness_backlog_limit", 5))
        readiness_override_review_limit = int(
            mapping_cfg.get("readiness_override_review_limit", 5)
        )
        markets = list(self.research_config.markets)
        optional_empty_exchanges = set()
        if not bool(standard_cfg.get("classification_primary_enabled", False)):
            optional_empty_exchanges = get_optional_empty_exchanges(
                self.research_config,
                "industry",
            )

        target_by_exchange, target_total = await self._count_research_target_instruments_by_exchange(
            markets,
            excluded_exchanges=optional_empty_exchanges,
        )

        def _load_storage_state() -> Dict[str, Any]:
            return {
                "mapping_counts": storage.summarize_official_industry_code_mappings(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                ),
                "mapping_cache_info": storage.get_latest_official_industry_code_mapping_cache_info(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                ),
                "official_classifications": storage.summarize_official_industry_classifications(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                ),
                "memberships": storage.summarize_industry_memberships(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                ),
                "authoritative_by_exchange": storage.count_industry_memberships_by_exchange(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                    mapping_status="authoritative",
                ),
                "unmapped_backlog_summary": storage.summarize_unmapped_official_industry_code_backlog(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                ),
                "top_unmapped_backlog": storage.list_unmapped_official_industry_code_backlog(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                    limit=readiness_backlog_limit,
                    offset=0,
                    include_mapping=False,
                ),
                "index_analysis": storage.summarize_industry_index_analysis_daily(
                    taxonomy_system=resolved_taxonomy_system,
                    taxonomy_version=resolved_taxonomy_version,
                ),
            }

        storage_state = self._load_research_storage_state(_load_storage_state)
        override_review = await self.get_research_official_mapping_override_review(
            taxonomy_system=resolved_taxonomy_system,
            taxonomy_version=resolved_taxonomy_version,
            include_mapping=True,
        )
        mapping_counts = storage_state["mapping_counts"]
        mapping_cache_info = storage_state["mapping_cache_info"]
        official_classifications = storage_state["official_classifications"]
        memberships = storage_state["memberships"]
        authoritative_by_exchange = storage_state["authoritative_by_exchange"]
        unmapped_backlog_summary = storage_state["unmapped_backlog_summary"]
        top_unmapped_backlog = storage_state["top_unmapped_backlog"]
        index_analysis_summary = storage_state["index_analysis"]

        latest_built_at = None if not mapping_cache_info else mapping_cache_info.get("built_at")
        latest_updated_at = None if not mapping_cache_info else mapping_cache_info.get("updated_at")
        cache_max_age_days = int(mapping_cfg.get("cache_max_age_days", 7))
        minimum_mapping_rows = int(mapping_cfg.get("minimum_mapping_rows", 0))
        minimum_mapped_rows = int(mapping_cfg.get("minimum_mapped_rows", 0))
        mapping_total = int(mapping_counts.get("mapped", 0) + mapping_counts.get("unmapped", 0))
        mapped_count = int(mapping_counts.get("mapped", 0))
        unmapped_count = int(mapping_counts.get("unmapped", 0))

        cache_fresh = False
        if latest_built_at:
            try:
                built_at_dt = datetime.fromisoformat(str(latest_built_at))
                cache_fresh = get_shanghai_time() - built_at_dt <= timedelta(days=cache_max_age_days)
            except ValueError:
                cache_fresh = False

        meets_minimum_rows = mapping_total >= minimum_mapping_rows
        meets_minimum_mapped_rows = mapped_count >= minimum_mapped_rows

        classification_total = int(official_classifications.get("total", 0))
        membership_total = int(memberships.get("total", 0))
        authoritative_total = int(memberships.get("counts", {}).get("authoritative", 0))
        unmapped_backlog_current_total = int(
            unmapped_backlog_summary.get("current_classification_total", 0)
        )
        override_status_counts = dict(override_review.get("status_counts", {}) or {})
        attention_status_counts = {
            status: int(count)
            for status, count in override_status_counts.items()
            if status != "configured_and_applied" and int(count or 0) > 0
        }
        override_requires_attention = bool(attention_status_counts)
        override_top_items = [
            {
                "official_industry_code": item.get("official_industry_code"),
                "review_status": item.get("review_status"),
                "status_reason": item.get("status_reason"),
            }
            for item in (override_review.get("items", []) or [])
            if str(item.get("review_status") or "") != "configured_and_applied"
        ][:readiness_override_review_limit]

        exchange_coverage = []
        for exchange in markets:
            target_instruments = int(target_by_exchange.get(exchange, 0))
            authoritative_memberships = int(authoritative_by_exchange.get(exchange, 0))
            coverage_ratio = (
                authoritative_memberships / target_instruments
                if target_instruments > 0
                else 1.0
            )
            exchange_coverage.append(
                {
                    "exchange": exchange,
                    "target_instruments": target_instruments,
                    "authoritative_memberships": authoritative_memberships,
                    "coverage_ratio": coverage_ratio,
                    "ready": authoritative_memberships >= target_instruments,
                }
            )

        blockers: List[str] = []
        if target_total <= 0:
            blockers.append("no_target_instruments")
        if target_total > 0 and authoritative_total < target_total:
            blockers.append("authoritative_membership_coverage_incomplete")

        industry_standard_ready = len(blockers) == 0
        valuation_relative_cfg = self.research_config.modules.get("valuation", {}).get(
            "relative", {}
        )
        require_authoritative = bool(
            valuation_relative_cfg.get("require_authoritative", True)
        )
        if require_authoritative:
            relative_valuation_ready = industry_standard_ready
            relative_valuation_blockers = blockers.copy()
        else:
            membership_coverage_ready = target_total > 0 and membership_total >= target_total
            relative_valuation_ready = membership_coverage_ready
            relative_valuation_blockers = (
                [] if membership_coverage_ready else ["membership_coverage_incomplete"]
            )

        return {
            "taxonomy_system": resolved_taxonomy_system,
            "taxonomy_version": resolved_taxonomy_version,
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "target_instrument_count": target_total,
            "target_instruments_by_exchange": target_by_exchange,
            "official_mapping_cache": {
                "total": mapping_total,
                "mapped": mapped_count,
                "unmapped": unmapped_count,
                "latest_built_at": latest_built_at,
                "latest_updated_at": latest_updated_at,
                "source": None if not mapping_cache_info else mapping_cache_info.get("source"),
                "source_mode": None if not mapping_cache_info else mapping_cache_info.get("source_mode"),
                "cache_max_age_days": cache_max_age_days,
                "minimum_mapping_rows": minimum_mapping_rows,
                "minimum_mapped_rows": minimum_mapped_rows,
                "fresh": cache_fresh,
                "meets_minimum_rows": meets_minimum_rows,
                "meets_minimum_mapped_rows": meets_minimum_mapped_rows,
            },
            "official_classifications": {
                "total": classification_total,
                "counts": official_classifications.get("counts", {}),
                "latest_updated_at": official_classifications.get("latest_updated_at"),
                "latest_data_as_of": official_classifications.get("latest_official_update_time"),
                "meets_target_universe": classification_total >= target_total if target_total > 0 else False,
            },
            "memberships": {
                "total": membership_total,
                "counts": memberships.get("counts", {}),
                "latest_updated_at": memberships.get("latest_updated_at"),
                "latest_data_as_of": memberships.get("latest_data_as_of"),
                "meets_target_universe": authoritative_total >= target_total if target_total > 0 else False,
            },
            "unmapped_backlog": {
                "official_code_total": int(
                    unmapped_backlog_summary.get("official_code_total", 0)
                ),
                "current_classification_total": unmapped_backlog_current_total,
                "top_items": [
                    {
                        "official_industry_code": item.get("official_industry_code"),
                        "best_taxonomy_industry_code": item.get(
                            "best_taxonomy_industry_code"
                        ),
                        "current_classification_count": int(
                            item.get("current_classification_count", 0)
                        ),
                        "impacted_exchange_counts": item.get(
                            "impacted_exchange_counts", {}
                        ),
                        "sample_instruments": item.get("sample_instruments", []),
                    }
                    for item in top_unmapped_backlog
                ],
            },
            "override_review": {
                "requires_attention": override_requires_attention,
                "configured_override_total": int(
                    override_review.get("configured_override_total", 0)
                ),
                "ready_candidate_total": int(
                    override_review.get("ready_candidate_total", 0)
                ),
                "applied_override_total": int(
                    override_review.get("applied_override_total", 0)
                ),
                "pending_manual_override_total": int(
                    override_review.get("pending_manual_override_total", 0)
                ),
                "status_counts": override_status_counts,
                "top_items": override_top_items,
            },
            "exchange_coverage": exchange_coverage,
            "industry_standard_ready": industry_standard_ready,
            "blockers": blockers,
            "relative_valuation": {
                "require_authoritative": require_authoritative,
                "benchmark_level": int(valuation_relative_cfg.get("benchmark_level", 2)),
                "ready": relative_valuation_ready,
                "blockers": relative_valuation_blockers,
            },
            "index_analysis": {
                "enabled": bool(
                    self.research_config.sources.get("swsresearch", {})
                    .get("index_analysis", {})
                    .get("enabled", False)
                ),
                "total": int(index_analysis_summary.get("total", 0)),
                "distinct_index_codes": int(
                    index_analysis_summary.get("distinct_index_codes", 0)
                ),
                "latest_trade_date": index_analysis_summary.get("latest_trade_date"),
                "latest_updated_at": index_analysis_summary.get("latest_updated_at"),
                "index_type_counts": index_analysis_summary.get("index_type_counts", {}),
            },
        }

    @staticmethod
    def _official_mapping_backlog_review_config(
        standard_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        mapping_cfg = standard_cfg.get("official_mapping", {})
        review_cfg = mapping_cfg.get("backlog_review", {})
        return {
            "minimum_current_classification_count": max(
                1, int(review_cfg.get("minimum_current_classification_count", 1))
            ),
            "minimum_overlap_count": max(
                1, int(review_cfg.get("minimum_overlap_count", 1))
            ),
            "minimum_precision": float(review_cfg.get("minimum_precision", 0.0)),
            "minimum_recall": float(review_cfg.get("minimum_recall", 0.0)),
            "minimum_top_candidate_overlap_gap": max(
                0, int(review_cfg.get("minimum_top_candidate_overlap_gap", 0))
            ),
        }

    @staticmethod
    def _configured_official_mapping_overrides(
        standard_cfg: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        raw = (
            standard_cfg.get("official_mapping", {}).get("manual_overrides", {}) or {}
        )
        if not isinstance(raw, dict):
            return {}
        normalized: Dict[str, Dict[str, Any]] = {}
        for official_industry_code, payload in raw.items():
            code = str(official_industry_code or "").strip()
            if not code or not isinstance(payload, dict):
                continue
            taxonomy_industry_code = str(
                payload.get("taxonomy_industry_code") or ""
            ).strip()
            if not taxonomy_industry_code:
                continue
            normalized[code] = {
                "taxonomy_industry_code": taxonomy_industry_code,
                "confidence": payload.get("confidence"),
                "reason": payload.get("reason"),
            }
        return normalized

    @staticmethod
    def _classify_official_override_review_status(
        *,
        configured_override: Optional[Dict[str, Any]],
        ready_candidate: Optional[Dict[str, Any]],
        applied_override: Optional[Dict[str, Any]],
    ) -> Tuple[str, str]:
        configured_code = (
            str(configured_override.get("taxonomy_industry_code") or "").strip()
            if configured_override
            else ""
        )
        ready_code = (
            str(ready_candidate.get("taxonomy_industry_code") or "").strip()
            if ready_candidate
            else ""
        )
        applied_code = (
            str(applied_override.get("taxonomy_industry_code") or "").strip()
            if applied_override
            else ""
        )

        if configured_override and applied_override:
            if configured_code and configured_code == applied_code:
                if ready_candidate and ready_code and ready_code != configured_code:
                    return (
                        "configured_ready_candidate_mismatch",
                        "ready_candidate_differs_from_configured_override",
                    )
                return (
                    "configured_and_applied",
                    "configured_override_reflected_in_mapping_cache",
                )
            return (
                "configured_applied_mismatch",
                "applied_manual_override_differs_from_current_config",
            )

        if configured_override and not applied_override:
            if ready_candidate and ready_code and ready_code != configured_code:
                return (
                    "configured_ready_candidate_mismatch",
                    "configured_override_differs_from_ready_candidate",
                )
            return (
                "configured_not_applied",
                "configured_override_not_reflected_in_mapping_cache",
            )

        if applied_override and not configured_override:
            return (
                "applied_not_configured",
                "mapping_cache_contains_manual_override_without_config_entry",
            )

        if ready_candidate and not configured_override:
            return (
                "ready_candidate_pending_config",
                "ready_candidate_not_yet_configured",
            )

        return ("review_only", "review_state_could_not_be_classified")

    @staticmethod
    def _enrich_unmapped_backlog_item(
        item: Dict[str, Any],
        review_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        enriched = dict(item)
        mapping_payload = enriched.get("mapping") or {}
        candidate_rankings = list(mapping_payload.get("candidate_rankings") or [])
        current_count = int(enriched.get("current_classification_count", 0) or 0)
        best_candidate = candidate_rankings[0] if candidate_rankings else None
        second_candidate = (
            candidate_rankings[1] if len(candidate_rankings) > 1 else None
        )
        candidate_count = len(candidate_rankings)
        top_gap = None
        if best_candidate is not None and second_candidate is not None:
            top_gap = int(best_candidate.get("overlap_count", 0) or 0) - int(
                second_candidate.get("overlap_count", 0) or 0
            )

        has_current_impact = (
            current_count >= review_cfg["minimum_current_classification_count"]
        )
        has_best_candidate = bool(
            best_candidate
            and str(best_candidate.get("taxonomy_industry_code") or "").strip()
        )
        candidate_strength_ready = (
            has_best_candidate
            and int(best_candidate.get("overlap_count", 0) or 0)
            >= review_cfg["minimum_overlap_count"]
            and float(best_candidate.get("precision", 0.0) or 0.0)
            >= review_cfg["minimum_precision"]
            and float(best_candidate.get("recall", 0.0) or 0.0)
            >= review_cfg["minimum_recall"]
        )
        gap_ready = second_candidate is None or (
            top_gap is not None
            and top_gap >= review_cfg["minimum_top_candidate_overlap_gap"]
        )
        override_candidate_ready = (
            has_current_impact and has_best_candidate and candidate_strength_ready and gap_ready
        )

        if override_candidate_ready:
            reason = (
                "single_strong_candidate_with_current_impact"
                if second_candidate is None
                else "top_candidate_clearly_leads_with_current_impact"
            )
            priority = "high"
        elif not has_best_candidate:
            reason = "no_candidate_rankings"
            priority = "low" if current_count <= 0 else "medium"
        elif not has_current_impact:
            reason = "no_current_impact"
            priority = "low"
        elif not candidate_strength_ready:
            reason = "candidate_strength_below_threshold"
            priority = "medium"
        else:
            reason = "top_candidate_gap_too_small"
            priority = "medium"

        enriched["review_priority"] = priority
        enriched["override_candidate_ready"] = override_candidate_ready
        enriched["override_candidate_reason"] = reason
        enriched["candidate_count"] = candidate_count
        enriched["top_candidate_overlap_gap"] = top_gap
        enriched["manual_override_suggestion"] = DataManager._build_manual_override_suggestion(
            enriched,
            best_candidate=best_candidate,
            override_candidate_ready=override_candidate_ready,
        )
        return enriched

    @staticmethod
    def _build_manual_override_suggestion(
        item: Dict[str, Any],
        *,
        best_candidate: Optional[Dict[str, Any]],
        override_candidate_ready: bool,
    ) -> Optional[Dict[str, Any]]:
        if not override_candidate_ready or not best_candidate:
            return None
        taxonomy_code = str(best_candidate.get("taxonomy_industry_code") or "").strip()
        if not taxonomy_code:
            return None

        official_code = str(item.get("official_industry_code") or "").strip()
        reason = (
            "Suggested from official mapping backlog: "
            f"{item.get('override_candidate_reason')} "
            f"(current_classification_count={int(item.get('current_classification_count', 0) or 0)}, "
            f"overlap={int(best_candidate.get('overlap_count', 0) or 0)}, "
            f"precision={float(best_candidate.get('precision', 0.0) or 0.0):.4f}, "
            f"recall={float(best_candidate.get('recall', 0.0) or 0.0):.4f})"
        )
        return {
            "official_industry_code": official_code,
            "taxonomy_industry_code": taxonomy_code,
            "confidence": "review_candidate",
            "reason": reason,
        }

    async def run_financial_summary_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """运行 financial_summary 影子同步。

        该方法只写入 research.db，不影响现有行情库。
        """
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        from research.financial_summary_sync import FinancialSummaryShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='financial_summary_shadow_sync',
        )
        service = FinancialSummaryShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_shareholder_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        write_policy: str = "refresh_all",
    ) -> Dict[str, Any]:
        """运行 shareholders 影子同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("shareholders", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research shareholders module is disabled",
            }

        from research.shareholder_sync import ShareholderShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='shareholder_shadow_sync',
        )
        service = ShareholderShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            write_policy=write_policy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_shareholder_incremental_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages_per_market: Optional[int] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """运行 shareholders 每日增量/变更检查同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("shareholders", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research shareholders module is disabled",
            }

        from research.shareholder_incremental_sync import ShareholderIncrementalSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='shareholder_incremental_sync',
        )
        service = ShareholderIncrementalSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            lookback_days=lookback_days,
            overlap_days=overlap_days,
            page_size=page_size,
            max_pages_per_market=max_pages_per_market,
            max_candidates=max_candidates,
            pending_recheck_days=pending_recheck_days,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            dry_run=dry_run,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def get_research_shareholder_readiness(self) -> Dict[str, Any]:
        """Return shareholder-domain readiness and rollout blockers."""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("shareholders", {})
        markets = list(self.research_config.markets)
        optional_empty_exchanges = get_optional_empty_exchanges(
            self.research_config,
            "shareholders",
        )

        target_by_exchange, target_total = await self._count_research_target_instruments_by_exchange(
            markets,
            excluded_exchanges=optional_empty_exchanges,
        )

        def _load_storage_state() -> Dict[str, Any]:
            summary = storage.summarize_shareholder_snapshots()
            target_exchanges = [
                exchange
                for exchange in markets
                if exchange not in optional_empty_exchanges
            ]
            if target_exchanges != markets and hasattr(
                storage,
                "summarize_shareholder_snapshots_by_exchanges",
            ):
                target_summary = storage.summarize_shareholder_snapshots_by_exchanges(
                    target_exchanges
                )
            else:
                target_summary = summary
            return {
                "summary": summary,
                "target_summary": target_summary,
                "by_exchange": storage.count_shareholder_snapshots_by_exchange(),
            }

        storage_state = self._load_research_storage_state(_load_storage_state)
        summary = storage_state["summary"]
        target_summary = storage_state["target_summary"]
        snapshots_by_exchange = storage_state["by_exchange"]

        enabled = bool(module_cfg.get("enabled", False))
        delivery_mode = str(module_cfg.get("delivery_mode", "free_best_effort"))
        required_mode = module_cfg.get("snapshot_api_requires_mode")
        required_scope = [
            str(scope).strip()
            for scope in module_cfg.get("allowed_scope", [])
            if str(scope).strip()
        ]
        snapshot_api_enabled = enabled and (
            not required_mode or delivery_mode == required_mode
        )

        snapshot_total = int(target_summary.get("total", 0))
        missing_snapshot_count = max(target_total - snapshot_total, 0)
        scope_counts = {
            str(scope): int(count or 0)
            for scope, count in (target_summary.get("scope_counts") or {}).items()
            if str(scope).strip()
        }

        exchange_coverage = []
        for exchange in markets:
            target_instruments = int(target_by_exchange.get(exchange, 0))
            snapshot_count = int(snapshots_by_exchange.get(exchange, 0))
            coverage_ratio = (
                snapshot_count / target_instruments if target_instruments > 0 else 1.0
            )
            exchange_coverage.append(
                {
                    "exchange": exchange,
                    "target_instruments": target_instruments,
                    "snapshot_count": snapshot_count,
                    "coverage_ratio": coverage_ratio,
                    "ready": snapshot_count >= target_instruments,
                }
            )

        scope_coverage = []
        for scope in required_scope:
            scope_snapshot_count = int(scope_counts.get(scope, 0))
            coverage_ratio = (
                scope_snapshot_count / target_total if target_total > 0 else 1.0
            )
            scope_coverage.append(
                {
                    "scope": scope,
                    "target_instruments": target_total,
                    "snapshot_count": scope_snapshot_count,
                    "coverage_ratio": coverage_ratio,
                    "ready": scope_snapshot_count >= target_total,
                }
            )

        blockers: List[str] = []
        if not enabled:
            blockers.append("shareholders_module_disabled")
        if target_total <= 0:
            blockers.append("no_target_instruments")
        if snapshot_total <= 0:
            blockers.append("no_shareholder_snapshots")
        if target_total > 0 and snapshot_total < target_total:
            blockers.append("shareholder_snapshot_coverage_incomplete")
        if target_total > 0 and any(item["snapshot_count"] < target_total for item in scope_coverage):
            blockers.append("required_scope_coverage_incomplete")
        if required_mode and delivery_mode != required_mode:
            blockers.append("delivery_mode_gate_not_satisfied")

        ready = len(blockers) == 0 and snapshot_api_enabled

        return {
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "module_enabled": enabled,
            "delivery_mode": delivery_mode,
            "snapshot_api_requires_mode": required_mode,
            "snapshot_api_enabled": snapshot_api_enabled,
            "target_instrument_count": target_total,
            "target_instruments_by_exchange": target_by_exchange,
            "snapshot_total": snapshot_total,
            "missing_snapshot_count": missing_snapshot_count,
            "required_scope": required_scope,
            "coverage_status_counts": target_summary.get("coverage_status_counts", {}),
            "source_counts": target_summary.get("source_counts", {}),
            "source_mode_counts": target_summary.get("source_mode_counts", {}),
            "scope_counts": scope_counts,
            "latest_updated_at": target_summary.get("latest_updated_at"),
            "latest_data_as_of": target_summary.get("latest_data_as_of"),
            "exchange_coverage": exchange_coverage,
            "scope_coverage": scope_coverage,
            "ready_for_paid_high_availability_rollout": ready,
            "blockers": blockers,
        }

    async def get_research_financial_statements_readiness(self) -> Dict[str, Any]:
        """Return financial-statement warehouse readiness and rollout blockers."""
        from research.financial_statements_sync import build_financial_report_periods

        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("financial_statements", {})
        markets = list(self.research_config.markets)
        optional_empty_exchanges = get_optional_empty_exchanges(
            self.research_config,
            "financial_statements",
        )
        target_by_exchange, target_total = await self._count_research_target_instruments_by_exchange(
            markets,
            excluded_exchanges=optional_empty_exchanges,
        )
        target_instrument_ids: List[str] = []
        for exchange in markets:
            if str(exchange).strip().upper() in optional_empty_exchanges:
                continue
            target_instrument_ids.extend(
                await self._list_research_target_instrument_ids_by_exchange(exchange)
            )
        target_instrument_ids = sorted(set(target_instrument_ids))
        history_cfg = module_cfg.get("history", {})
        storage_cfg = module_cfg.get("storage", {})
        hot_anchor_policy = storage_cfg.get("hot_anchor_policy", {})
        expected_periods = build_financial_report_periods(
            baseline_report_period=str(
                history_cfg.get("baseline_report_period", "2024Q1")
            ),
            rolling_min_quarters=int(history_cfg.get("rolling_min_quarters", 8)),
            optional_anchor_period=history_cfg.get("optional_ttm_anchor_period"),
            include_optional_anchor=bool(
                hot_anchor_policy.get("include_ttm_anchor_period", True)
            ),
        )

        def _load_readiness() -> Dict[str, Any]:
            readiness_cfg = module_cfg.get("readiness", {})
            readiness = self._run_financial_storage_call(
                storage,
                "validate_financial_statement_readiness",
                expected_periods=expected_periods,
                instrument_ids=target_instrument_ids,
                required_core_facts=list(readiness_cfg.get("required_core_facts", [])),
                fallback_sources=list(
                    module_cfg.get("fallback_policy", {}).get(
                        "fallback_source_priority",
                        ["akshare"],
                    )
                ),
                readiness_config=readiness_cfg,
            )
            return readiness if isinstance(readiness, dict) else {}

        readiness = self._load_research_storage_state(_load_readiness)
        blockers = list(readiness.get("blockers", []))
        if not bool(module_cfg.get("enabled", False)):
            blockers.insert(0, "financial_statements_module_disabled")
        blockers = list(dict.fromkeys(str(item) for item in blockers if str(item)))
        ready = bool(readiness.get("ready_for_rollout", False)) and not blockers

        return {
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "module_enabled": bool(module_cfg.get("enabled", False)),
            "target_instrument_count": target_total,
            "target_instruments_by_exchange": target_by_exchange,
            "expected_report_periods": expected_periods,
            "readiness": readiness,
            "ready_for_rollout": ready,
            "blockers": blockers,
        }

    async def get_research_valuation_readiness(self) -> Dict[str, Any]:
        """Return valuation-domain readiness and rollout blockers."""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        markets = list(self.research_config.markets)
        optional_empty_exchanges = get_optional_empty_exchanges(
            self.research_config,
            "valuation",
        )

        target_by_exchange, target_total = await self._count_research_target_instruments_by_exchange(
            markets,
            excluded_exchanges=optional_empty_exchanges,
        )

        relative_cfg = module_cfg.get("relative", {})
        metric_fields = [
            str(item)
            for item in relative_cfg.get("metric_variants", ["pe_ttm", "pb_mrq", "ps_ttm"])
            if str(item).strip()
        ]
        from research.valuation_service import ResearchValuationService

        history_identity = ResearchValuationService(module_cfg).history_identity()

        def _load_storage_state() -> Dict[str, Any]:
            metric_coverage: Dict[str, Any] = {}
            input_coverage: Dict[str, Any] = {}
            try:
                candidate = storage.summarize_valuation_metric_coverage(
                    metric_fields=metric_fields,
                    calc_method=history_identity["calc_method"],
                    calc_version=history_identity["calc_version"],
                    parameter_hash=history_identity["parameter_hash"],
                    parameter_hashes=history_identity.get("compatible_parameter_hashes"),
                )
                if isinstance(candidate, dict):
                    metric_coverage = candidate
            except Exception:
                metric_coverage = {}
            try:
                candidate = storage.summarize_valuation_input_coverage()
                if isinstance(candidate, dict):
                    input_coverage = candidate
            except Exception:
                input_coverage = {}
            return {
                "summary": storage.summarize_valuation_history(
                    calc_method=history_identity["calc_method"],
                    calc_version=history_identity["calc_version"],
                    parameter_hash=history_identity["parameter_hash"],
                    parameter_hashes=history_identity.get("compatible_parameter_hashes"),
                ),
                "by_exchange": storage.count_valuation_history_by_exchange(
                    calc_method=history_identity["calc_method"],
                    calc_version=history_identity["calc_version"],
                    parameter_hash=history_identity["parameter_hash"],
                    parameter_hashes=history_identity.get("compatible_parameter_hashes"),
                ),
                "metric_coverage": metric_coverage,
                "input_coverage": input_coverage,
            }

        storage_state = self._load_research_storage_state(_load_storage_state)
        summary = storage_state["summary"]
        valuation_by_exchange = storage_state["by_exchange"]
        metric_coverage = storage_state.get("metric_coverage", {})
        input_coverage = storage_state.get("input_coverage", {})

        enabled = bool(module_cfg.get("enabled", False))
        require_authoritative = bool(relative_cfg.get("require_authoritative", True))
        benchmark_level = int(relative_cfg.get("benchmark_level", 2))
        benchmark_field = str(relative_cfg.get("benchmark_field", "sw_l2_code"))

        valuation_history_total = int(summary.get("total", 0))
        valuation_input_total = int(input_coverage.get("usable_input_count", 0) or 0)
        missing_valuation_history_count = max(
            target_total - valuation_history_total,
            0,
        )
        missing_valuation_input_count = max(target_total - valuation_input_total, 0)

        exchange_coverage = []
        for exchange in markets:
            target_instruments = int(target_by_exchange.get(exchange, 0))
            valuation_history_count = int(valuation_by_exchange.get(exchange, 0))
            coverage_ratio = (
                valuation_history_count / target_instruments
                if target_instruments > 0
                else 1.0
            )
            exchange_coverage.append(
                {
                    "exchange": exchange,
                    "target_instruments": target_instruments,
                    "valuation_history_count": valuation_history_count,
                    "coverage_ratio": coverage_ratio,
                    "ready": valuation_history_count >= target_instruments,
                }
            )

        industry_readiness_error = None
        industry_readiness: Optional[Dict[str, Any]] = None
        try:
            industry_readiness = await self.get_research_industry_standard_readiness()
            industry_relative = industry_readiness.get("relative_valuation", {})
            industry_relative_ready = bool(industry_relative.get("ready", False))
            industry_relative_blockers = [
                str(blocker)
                for blocker in industry_relative.get("blockers", [])
                if str(blocker)
            ]
        except Exception as exc:
            industry_readiness_error = str(exc)
            industry_relative_ready = False
            industry_relative_blockers = ["industry_standard_readiness_unavailable"]

        blockers: List[str] = []
        if not enabled:
            blockers.append("valuation_module_disabled")
        if target_total <= 0:
            blockers.append("no_target_instruments")
        if valuation_history_total <= 0:
            blockers.append("no_valuation_history")
        if target_total > 0 and valuation_history_total < target_total:
            blockers.append("valuation_history_coverage_incomplete")
        if valuation_input_total <= 0:
            blockers.append("no_valuation_inputs")
        if target_total > 0 and valuation_input_total < target_total:
            blockers.append("valuation_input_coverage_incomplete")
        blockers.extend(
            blocker
            for blocker in industry_relative_blockers
            if blocker not in blockers
        )

        history_ready = (
            target_total > 0
            and valuation_history_total >= target_total
            and valuation_history_total > 0
            and valuation_input_total >= target_total
        )
        financial_readiness_payload: Optional[Dict[str, Any]] = None
        financial_ready = True
        financial_cfg = self.research_config.modules.get("financial_statements", {})
        if bool(financial_cfg.get("enabled", False)):
            if not history_ready:
                financial_readiness_payload = {
                    "ready_for_rollout": None,
                    "status": "skipped",
                    "reason": "skipped_until_valuation_input_and_history_coverage_pass",
                    "blockers": [],
                }
            else:
                try:
                    financial_readiness_payload = await self.get_research_financial_statements_readiness()
                    financial_ready = bool(
                        financial_readiness_payload.get("ready_for_rollout", False)
                    )
                except Exception as exc:
                    financial_ready = False
                    financial_readiness_payload = {
                        "ready_for_rollout": False,
                        "blockers": ["financial_statement_readiness_unavailable"],
                        "error": str(exc),
                    }
        if not financial_ready and "financial_statement_readiness_incomplete" not in blockers:
            blockers.append("financial_statement_readiness_incomplete")

        relative_valuation_ready = (
            enabled
            and history_ready
            and industry_relative_ready
            and financial_ready
        )
        ready_for_rollout = len(blockers) == 0 and relative_valuation_ready

        return {
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "module_enabled": enabled,
            "target_instrument_count": target_total,
            "target_instruments_by_exchange": target_by_exchange,
            "valuation_history_total": valuation_history_total,
            "missing_valuation_history_count": missing_valuation_history_count,
            "valuation_input_total": valuation_input_total,
            "missing_valuation_input_count": missing_valuation_input_count,
            "valuation_inputs": input_coverage,
            "valuation_storage": {
                "db_path": getattr(storage, "valuation_db_path", None),
            },
            "source_counts": summary.get("source_counts", {}),
            "source_mode_counts": summary.get("source_mode_counts", {}),
            "calc_method_counts": summary.get("calc_method_counts", {}),
            "calc_version_counts": summary.get("calc_version_counts", {}),
            "metric_coverage": metric_coverage,
            "latest_as_of_date": summary.get("latest_as_of_date"),
            "latest_updated_at": summary.get("latest_updated_at"),
            "latest_data_as_of": summary.get("latest_data_as_of"),
            "exchange_coverage": exchange_coverage,
            "relative_valuation": {
                "require_authoritative": require_authoritative,
                "benchmark_level": benchmark_level,
                "benchmark_field": benchmark_field,
                "ready": relative_valuation_ready,
                "blockers": (
                    []
                    if relative_valuation_ready
                    else list(dict.fromkeys(industry_relative_blockers + [
                        blocker
                        for blocker in blockers
                        if blocker
                        in {
                            "valuation_module_disabled",
                            "no_target_instruments",
                            "no_valuation_history",
                            "valuation_history_coverage_incomplete",
                            "no_valuation_inputs",
                            "valuation_input_coverage_incomplete",
                        }
                    ]))
                ),
                "industry_standard_ready": industry_relative_ready,
                "industry_standard_error": industry_readiness_error,
            },
            "financial_statements": financial_readiness_payload,
            "ready_for_rollout": ready_for_rollout,
            "blockers": blockers,
        }

    async def get_research_metadata_readiness(self) -> Dict[str, Any]:
        """Return readiness for external research metadata domains."""
        storage = self._require_research_storage()
        markets = list(self.research_config.markets)
        domain_specs = [
            {
                "domain": "analyst_forecasts",
                "summary_loader": storage.summarize_analyst_forecasts,
                "exchange_loader": storage.count_analyst_forecasts_by_exchange,
                "empty_blocker": "no_analyst_forecasts",
                "coverage_blocker": "analyst_forecast_coverage_incomplete",
            },
            {
                "domain": "research_reports",
                "summary_loader": storage.summarize_research_reports,
                "exchange_loader": storage.count_research_reports_by_exchange,
                "empty_blocker": "no_research_reports",
                "coverage_blocker": "research_report_coverage_incomplete",
            },
            {
                "domain": "sentiment_events",
                "summary_loader": storage.summarize_sentiment_events,
                "exchange_loader": storage.count_sentiment_events_by_exchange,
                "empty_blocker": "no_sentiment_events",
                "coverage_blocker": "sentiment_event_coverage_incomplete",
            },
        ]

        def _load_storage_state() -> Dict[str, Dict[str, Any]]:
            state: Dict[str, Dict[str, Any]] = {}
            for spec in domain_specs:
                domain = spec["domain"]
                state[domain] = {
                    "summary": spec["summary_loader"](),
                    "by_exchange": spec["exchange_loader"](),
                }
            return state

        storage_state = self._load_research_storage_state(_load_storage_state)

        domains = []
        ready_domain_count = 0
        grouped_blockers: List[str] = []
        for spec in domain_specs:
            domain = spec["domain"]
            module_cfg = self.research_config.modules.get(domain, {})
            enabled = bool(module_cfg.get("enabled", False))
            optional_empty_exchanges = get_optional_empty_exchanges(
                self.research_config,
                domain,
            )
            target_by_exchange, target_total = await self._count_research_target_instruments_by_exchange(
                markets,
                excluded_exchanges=optional_empty_exchanges,
            )
            summary = storage_state[domain]["summary"]
            by_exchange = storage_state[domain]["by_exchange"]

            instrument_total = int(summary.get("instrument_total", 0))
            missing_instrument_count = max(target_total - instrument_total, 0)

            exchange_coverage = []
            for exchange in markets:
                target_instruments = int(target_by_exchange.get(exchange, 0))
                instrument_count = int(by_exchange.get(exchange, 0))
                coverage_ratio = (
                    instrument_count / target_instruments
                    if target_instruments > 0
                    else 1.0
                )
                exchange_coverage.append(
                    {
                        "exchange": exchange,
                        "target_instruments": target_instruments,
                        "instrument_count": instrument_count,
                        "coverage_ratio": coverage_ratio,
                        "ready": instrument_count >= target_instruments,
                    }
                )

            blockers: List[str] = []
            if not enabled:
                blockers.append(f"{domain}_module_disabled")
            if target_total <= 0:
                blockers.append("no_target_instruments")
            if instrument_total <= 0:
                blockers.append(spec["empty_blocker"])
            if target_total > 0 and instrument_total < target_total:
                blockers.append(spec["coverage_blocker"])

            ready = len(blockers) == 0
            if ready:
                ready_domain_count += 1
            grouped_blockers.extend(f"{domain}:{blocker}" for blocker in blockers)

            domains.append(
                {
                    "domain": domain,
                    "module_enabled": enabled,
                    "target_instrument_count": target_total,
                    "target_instruments_by_exchange": target_by_exchange,
                    "instrument_total": instrument_total,
                    "row_total": int(summary.get("row_total", 0)),
                    "missing_instrument_count": missing_instrument_count,
                    "source_counts": summary.get("source_counts", {}),
                    "source_mode_counts": summary.get("source_mode_counts", {}),
                    "extra_counts": {
                        key: value
                        for key, value in summary.items()
                        if key.endswith("_counts")
                        and key not in {"source_counts", "source_mode_counts"}
                    },
                    "latest_item_date": summary.get("latest_item_date"),
                    "latest_updated_at": summary.get("latest_updated_at"),
                    "latest_data_as_of": summary.get("latest_data_as_of"),
                    "exchange_coverage": exchange_coverage,
                    "ready_for_rollout": ready,
                    "blockers": blockers,
                }
            )

        return {
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "domain_count": len(domain_specs),
            "ready_domain_count": ready_domain_count,
            "ready_for_rollout": ready_domain_count == len(domain_specs),
            "blockers": grouped_blockers,
            "domains": domains,
        }

    async def get_research_technical_cache_readiness(self) -> Dict[str, Any]:
        """Return readiness for persisted latest technical indicator snapshots."""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("technical", {})
        markets = list(self.research_config.markets)
        latest_cache_cfg = module_cfg.get("latest_cache", {})
        cache_enabled = bool(latest_cache_cfg.get("enabled", True))
        period = str(latest_cache_cfg.get("period", "1d"))
        adjustment = self._normalize_research_adjustment(
            str(
                latest_cache_cfg.get("adjustment")
                or module_cfg.get("default_adjustment", "qfq")
            )
        )
        optional_empty_exchanges = get_optional_empty_exchanges(
            self.research_config,
            "technical",
        )

        target_by_exchange, target_total = await self._count_research_target_instruments_by_exchange(
            markets,
            excluded_exchanges=optional_empty_exchanges,
        )

        def _load_storage_state() -> Dict[str, Any]:
            return {
                "summary": storage.summarize_technical_indicator_latest(
                    period=period,
                    adjustment=adjustment,
                ),
                "by_exchange": storage.count_technical_indicator_latest_by_exchange(
                    period=period,
                    adjustment=adjustment,
                ),
            }

        storage_state = self._load_research_storage_state(_load_storage_state)
        summary = storage_state["summary"]
        by_exchange = storage_state["by_exchange"]

        enabled = bool(module_cfg.get("enabled", False))
        snapshot_total = int(summary.get("instrument_total", 0))
        missing_snapshot_count = max(target_total - snapshot_total, 0)

        exchange_coverage = []
        for exchange in markets:
            target_instruments = int(target_by_exchange.get(exchange, 0))
            snapshot_count = int(by_exchange.get(exchange, 0))
            coverage_ratio = (
                snapshot_count / target_instruments
                if target_instruments > 0
                else 1.0
            )
            exchange_coverage.append(
                {
                    "exchange": exchange,
                    "target_instruments": target_instruments,
                    "snapshot_count": snapshot_count,
                    "coverage_ratio": coverage_ratio,
                    "ready": snapshot_count >= target_instruments,
                }
            )

        blockers: List[str] = []
        if not enabled:
            blockers.append("technical_module_disabled")
        if not cache_enabled:
            blockers.append("technical_latest_cache_disabled")
        if target_total <= 0:
            blockers.append("no_target_instruments")
        if snapshot_total <= 0:
            blockers.append("no_technical_indicator_latest")
        if target_total > 0 and snapshot_total < target_total:
            blockers.append("technical_indicator_latest_coverage_incomplete")

        return {
            "generated_at": get_shanghai_time().isoformat(),
            "markets": markets,
            "module_enabled": enabled,
            "cache_enabled": cache_enabled,
            "period": period,
            "adjustment": adjustment,
            "target_instrument_count": target_total,
            "target_instruments_by_exchange": target_by_exchange,
            "snapshot_total": snapshot_total,
            "row_total": int(summary.get("row_total", 0)),
            "missing_snapshot_count": missing_snapshot_count,
            "source_counts": summary.get("source_counts", {}),
            "source_mode_counts": summary.get("source_mode_counts", {}),
            "calc_method_counts": summary.get("calc_method_counts", {}),
            "calc_version_counts": summary.get("calc_version_counts", {}),
            "status_counts": summary.get("status_counts", {}),
            "signal_counts": summary.get("signal_counts", {}),
            "latest_as_of_date": summary.get("latest_as_of_date"),
            "latest_updated_at": summary.get("latest_updated_at"),
            "latest_data_as_of": summary.get("latest_data_as_of"),
            "exchange_coverage": exchange_coverage,
            "ready_for_rollout": len(blockers) == 0,
            "blockers": blockers,
        }

    async def run_financial_statements_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
        report_periods: Optional[List[str]] = None,
        sync_mode: str = "backfill",
        force_full: bool = False,
    ) -> Dict[str, Any]:
        """运行 financial_statements 影子同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("financial_statements", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research financial_statements module is disabled",
            }

        from research import FinancialStatementsShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='financial_statements_shadow_sync',
        )
        service = FinancialStatementsShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
            report_periods=report_periods,
            sync_mode=sync_mode,
            force_full=force_full,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_financial_l1_full_import(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        db_path: str = "data/financials.db",
        log_dir: Optional[str] = None,
        limit_per_exchange: Optional[int] = None,
        batch_size: int = 20,
        resume: bool = False,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        financial_disclosure_events_path: Optional[str] = None,
        manifest_only: bool = False,
        start_batch: Optional[int] = None,
        end_batch: Optional[int] = None,
        max_batches: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Run Financial L1 full import through the Python orchestrator."""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }
        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        from scripts.research_financial_l1_full_import import (
            DEFAULT_ACCEPTED_SOURCE_GAPS,
            DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES,
            DEFAULT_EXCHANGES,
            DEFAULT_SOURCE_ORDER,
            resolve_report_periods,
            run_full_import,
        )
        from scripts.dev_validation.audit_financial_numeric_fact_coverage import (
            DEFAULT_REQUIRED_CANONICAL_FACTS,
        )
        from scripts.dev_validation.prepare_sina_ths_local_core_import_manifest import (
            load_financial_disclosure_events,
        )
        from research.financial_source_field_mapping import MAPPING_VERSION

        resolved_periods = resolve_report_periods(
            report_periods=",".join(report_periods or []),
            period_window=period_window,
            rolling_quarters=rolling_quarters,
            baseline_report_period=baseline_report_period,
            latest_report_period=latest_report_period,
            optional_anchor_period=None,
            include_optional_anchor=False,
        )
        target_log_dir = Path(log_dir) if log_dir else Path("log") / "financial_l1_full_import" / get_shanghai_time().strftime("%Y%m%d_%H%M%S")
        return await run_full_import(
            log_dir=target_log_dir,
            db_path=Path(db_path),
            report_periods=resolved_periods,
            exchanges=exchanges or list(DEFAULT_EXCHANGES),
            limit_per_exchange=limit_per_exchange,
            batch_size=batch_size,
            mapping_version=MAPPING_VERSION,
            source_order=list(DEFAULT_SOURCE_ORDER),
            required_canonical_facts=list(DEFAULT_REQUIRED_CANONICAL_FACTS),
            financial_disclosure_events=load_financial_disclosure_events(
                Path(financial_disclosure_events_path)
                if financial_disclosure_events_path
                else None
            ),
            accepted_source_gap_specs=list(DEFAULT_ACCEPTED_SOURCE_GAPS),
            accepted_source_gap_exchanges=list(DEFAULT_ACCEPTED_SOURCE_GAP_EXCHANGES),
            continue_on_needs_review=True,
            skip_ready_targets=True,
            request_interval_seconds=request_interval_seconds,
            request_timeout_seconds=request_timeout_seconds,
            instrument_master_governance_enabled=True,
            resume=resume,
            manifest_only=manifest_only,
            start_batch=start_batch,
            end_batch=end_batch,
            max_batches=max_batches,
        )

    async def run_financial_disclosure_incremental_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages_per_market: Optional[int] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
        target_symbols: Optional[List[str]] = None,
        announcement_search_key: Optional[str] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        db_path: Optional[str] = None,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Run CNInfo announcement-driven Financial L1 incremental maintenance."""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }
        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }
        module_cfg = self.research_config.modules.get("financial_statements", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research financial_statements module is disabled",
            }

        from research.financial_disclosure_incremental_sync import (
            FinancialDisclosureIncrementalSyncService,
        )

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='financial_disclosure_incremental_sync',
        )
        service = FinancialDisclosureIncrementalSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            lookback_days=lookback_days,
            overlap_days=overlap_days,
            page_size=page_size,
            max_pages_per_market=max_pages_per_market,
            max_candidates=max_candidates,
            pending_recheck_days=pending_recheck_days,
            target_instrument_ids=target_instrument_ids,
            target_symbols=target_symbols,
            announcement_search_key=announcement_search_key,
            report_periods=report_periods,
            period_window=period_window,
            rolling_quarters=rolling_quarters,
            baseline_report_period=baseline_report_period,
            latest_report_period=latest_report_period,
            db_path=db_path,
            request_interval_seconds=request_interval_seconds,
            request_timeout_seconds=request_timeout_seconds,
            dry_run=dry_run,
            reconciliation=False,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_broker_risk_control_incremental_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        overlap_days: Optional[int] = None,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
        per_instrument_page_size: Optional[int] = None,
        per_instrument_max_pages: Optional[int] = None,
        limit_instruments: Optional[int] = None,
        instrument_ids: Optional[List[str]] = None,
        report_period_types: Optional[List[str]] = None,
        source_profile: Optional[str] = None,
        include_standalone_supplement: Optional[bool] = None,
        archive_root: Optional[str] = None,
        dry_run: bool = False,
        scan_only: bool = False,
    ) -> Dict[str, Any]:
        """Run broker regulatory fact maintenance as a financial-data post task."""
        started_at = datetime.now()
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
                "mode": "incremental_update",
            }
        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
                "mode": "incremental_update",
            }
        module_cfg = self.research_config.modules.get("financial_statements", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research financial_statements module is disabled",
                "mode": "incremental_update",
            }
        broker_cfg = module_cfg.get("broker_risk_control_reports", {})
        if not broker_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "broker_risk_control_reports module is disabled",
                "mode": "incremental_update",
            }

        incremental_cfg = broker_cfg.get("incremental", {})
        target_exchanges = exchanges or list(
            broker_cfg.get("exchanges")
            or self.research_config.markets
            or ["SSE", "SZSE", "BSE"]
        )
        lookback = int(
            incremental_cfg.get("lookback_days", 14)
            if lookback_days is None
            else lookback_days
        )
        overlap = int(
            incremental_cfg.get("overlap_days", 3)
            if overlap_days is None
            else overlap_days
        )
        as_of = get_shanghai_time().date()
        start = as_of - timedelta(days=max(1, lookback + overlap))
        scan_page_size = int(
            incremental_cfg.get("page_size", 30) if page_size is None else page_size
        )
        scan_max_pages = int(
            incremental_cfg.get("max_pages", 10) if max_pages is None else max_pages
        )
        instrument_page_size = int(
            incremental_cfg.get("per_instrument_page_size", 30)
            if per_instrument_page_size is None
            else per_instrument_page_size
        )
        instrument_max_pages = int(
            incremental_cfg.get("per_instrument_max_pages", 2)
            if per_instrument_max_pages is None
            else per_instrument_max_pages
        )
        target_limit = int(
            incremental_cfg.get("limit_instruments", 0)
            if limit_instruments is None
            else limit_instruments
        )
        periods_window = int(incremental_cfg.get("quarters", 12) or 12)
        selected_source_profile = (
            source_profile
            or broker_cfg.get("source_profile")
            or "broker_annual_report_embedded_risk_control"
        )
        selected_report_period_types = (
            report_period_types
            or incremental_cfg.get("report_period_types")
            or ["annual", "semiannual"]
        )
        selected_archive_root = (
            archive_root
            or broker_cfg.get("storage", {}).get("archive_root")
            or "data/filings/financial_statements/broker_risk_control"
        )
        include_supplement = bool(
            incremental_cfg.get("include_standalone_supplement", False)
            if include_standalone_supplement is None
            else include_standalone_supplement
        )
        tier = str(broker_cfg.get("storage", {}).get("incremental_tier") or "hot")

        from scripts.dev_validation.backfill_broker_risk_control_reports import (
            run_broker_risk_control_backfill,
        )

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=target_exchanges,
            job_name='broker_risk_control_incremental_sync',
        )
        result = await asyncio.to_thread(
            run_broker_risk_control_backfill,
            db_ops=self.db_ops,
            storage=self.research_storage,
            exchanges=target_exchanges,
            as_of_date=as_of.isoformat(),
            quarters=periods_window,
            start_date=start.isoformat(),
            end_date=as_of.isoformat(),
            limit_instruments=target_limit,
            instrument_ids=instrument_ids,
            write=not dry_run,
            scan_only=scan_only,
            page_size=scan_page_size,
            max_pages=scan_max_pages,
            per_instrument_scan=True,
            per_instrument_page_size=instrument_page_size,
            per_instrument_max_pages=instrument_max_pages,
            report_period_types=selected_report_period_types,
            source_profile=selected_source_profile,
            include_standalone_supplement=include_supplement,
            archive_root=selected_archive_root,
            tier=tier,
        )
        result["mode"] = "incremental_update"
        result["elapsed_seconds"] = round((datetime.now() - started_at).total_seconds(), 3)
        result["lookback_days"] = lookback
        result["overlap_days"] = overlap
        return self._attach_instrument_master_governance(result, governance)

    async def run_financial_disclosure_reconciliation_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        report_periods: Optional[List[str]] = None,
        period_window: str = "latest",
        rolling_quarters: int = 10,
        baseline_report_period: str = "2024Q1",
        latest_report_period: Optional[str] = None,
        max_candidates: Optional[int] = None,
        pending_recheck_days: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
        target_symbols: Optional[List[str]] = None,
        db_path: Optional[str] = None,
        request_interval_seconds: float = 0.2,
        request_timeout_seconds: float = 20.0,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Run bounded Financial L1 reconciliation over configured report periods."""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }
        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }
        module_cfg = self.research_config.modules.get("financial_statements", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research financial_statements module is disabled",
            }

        from research.financial_disclosure_incremental_sync import (
            FinancialDisclosureIncrementalSyncService,
        )

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='financial_disclosure_reconciliation_sync',
        )
        service = FinancialDisclosureIncrementalSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            max_candidates=max_candidates,
            pending_recheck_days=pending_recheck_days,
            target_instrument_ids=target_instrument_ids,
            target_symbols=target_symbols,
            report_periods=report_periods,
            period_window=period_window,
            rolling_quarters=rolling_quarters,
            baseline_report_period=baseline_report_period,
            latest_report_period=latest_report_period,
            db_path=db_path,
            request_interval_seconds=request_interval_seconds,
            request_timeout_seconds=request_timeout_seconds,
            dry_run=dry_run,
            reconciliation=True,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_analyst_forecast_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """运行 analyst_forecasts 影子同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("analyst_forecasts", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research analyst_forecasts module is disabled",
            }

        from research.analyst_forecast_sync import AnalystForecastShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='analyst_forecast_shadow_sync',
        )
        service = AnalystForecastShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_research_report_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """运行 research_reports 影子同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("research_reports", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research research_reports module is disabled",
            }

        from research.research_report_sync import ResearchReportShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='research_report_shadow_sync',
        )
        service = ResearchReportShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_sentiment_event_shadow_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        budget_mode: Optional[str] = None,
        allow_paid_proxy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """运行 sentiment_events 影子同步。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("sentiment_events", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research sentiment_events module is disabled",
            }

        from research.sentiment_event_sync import SentimentEventShadowSyncService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='sentiment_event_shadow_sync',
        )
        service = SentimentEventShadowSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            budget_mode=budget_mode,
            allow_paid_proxy=allow_paid_proxy,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_valuation_history_rebuild(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        target_instrument_ids: Optional[List[str]] = None,
        allow_disabled_module: bool = False,
        quote_limit_days: Optional[int] = None,
        window_mode: str = "trading_days",
        write_policy: str = "missing_only",
        progress_log_every: int = 200,
    ) -> Dict[str, Any]:
        """运行 valuation_history 重建。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False) and not allow_disabled_module:
            return {
                "status": "disabled",
                "reason": "research valuation module is disabled",
            }

        from research.valuation_history_sync import ValuationHistoryRebuildService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='valuation_history_rebuild',
            job_type='historical',
        )
        service = ValuationHistoryRebuildService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            target_instrument_ids=target_instrument_ids,
            quote_limit_days=quote_limit_days,
            window_mode=window_mode,
            write_policy=write_policy,
            progress_log_every=progress_log_every,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_valuation_input_sync(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        sync_mode: str = "incremental",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        target_instrument_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """同步 valuation_inputs 所需股本/市值输入。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        from research.valuation_input_sync import ValuationInputSyncService

        normalized_mode = str(sync_mode or "incremental").strip().lower()
        job_type = (
            "historical"
            if normalized_mode in {"full", "backfill", "history", "historical"}
            else "incremental"
        )
        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='valuation_input_sync',
            job_type=job_type,
        )
        service = ValuationInputSyncService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            source=source,
            source_mode=source_mode,
            sync_mode=sync_mode,
            start_date=start_date,
            end_date=end_date,
            target_instrument_ids=target_instrument_ids,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_risk_snapshot_rebuild(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
    ) -> Dict[str, Any]:
        """运行 risk snapshot 重建。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("risk", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research risk module is disabled",
            }

        from research.risk_snapshot_sync import RiskSnapshotRebuildService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='risk_snapshot_rebuild',
        )
        service = RiskSnapshotRebuildService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_technical_snapshot_refresh(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        limit_per_exchange: Optional[int] = None,
        adjustment: Optional[str] = None,
        period: Optional[str] = None,
    ) -> Dict[str, Any]:
        """运行 technical_indicator_latest 最新快照刷新。"""
        if not self.research_config.enabled:
            return {
                "status": "disabled",
                "reason": "research_config.enabled is false",
            }

        if self.research_storage is None:
            return {
                "status": "unavailable",
                "reason": "research storage is not initialized",
            }

        module_cfg = self.research_config.modules.get("technical", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "reason": "research technical module is disabled",
            }
        latest_cache_cfg = module_cfg.get("latest_cache", {})
        if not latest_cache_cfg.get("enabled", True):
            return {
                "status": "disabled",
                "reason": "research technical latest cache is disabled",
            }

        normalized_adjustment = (
            self._normalize_research_adjustment(adjustment)
            if adjustment is not None
            else None
        )

        from research.technical_snapshot_sync import TechnicalIndicatorLatestRefreshService

        governance = await self._ensure_research_job_instrument_master_governance(
            exchanges=exchanges,
            job_name='technical_snapshot_refresh',
        )
        service = TechnicalIndicatorLatestRefreshService(
            db_ops=self.db_ops,
            storage=self.research_storage,
            research_config=self.research_config,
            adjust_quotes=self._apply_research_adjustment,
        )
        result = await service.sync(
            exchanges=exchanges,
            limit_per_exchange=limit_per_exchange,
            adjustment=normalized_adjustment,
            period=period,
        )
        return self._attach_instrument_master_governance(result, governance)

    async def run_futures_market_data_sync(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Run futures market-data sync into the dedicated futures database."""
        storage = self._require_futures_storage()
        from research.futures_market_data import FuturesMarketDataSyncService

        service = FuturesMarketDataSyncService(storage, self.research_config)
        return await service.sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
            start_date=start_date,
            end_date=end_date,
            mode=mode,
            dry_run=dry_run,
        )

    async def run_futures_trading_day_governance(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Refresh and validate local futures trading-day governance state."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        from research.futures_market_data import FuturesTradingDayGovernanceService
        from research.futures_market_data import FuturesUniverseSelector

        def _run() -> Dict[str, Any]:
            scope_selection = FuturesUniverseSelector(module_cfg, storage).resolve(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
            )
            if scope_selection.blockers:
                return {
                    "status": "blocked",
                    "domain": "futures_trading_day_governance",
                    "scope_selection": scope_selection.as_dict(),
                    "seed_result": {},
                    "target_date_expansion": {
                        "status": "blocked",
                        "blockers": scope_selection.blockers,
                        "warnings": scope_selection.warnings,
                    },
                    "readiness": {},
                    "dry_run": dry_run,
                }
            service = FuturesTradingDayGovernanceService(storage, module_cfg)
            calendar_cfg = (module_cfg.get("master_data") or {}).get("calendar") or {}
            governance_cfg = module_cfg.get("trading_day_governance") or {}
            allow_estimated_bootstrap = bool(
                calendar_cfg.get("seed_on_governance", False)
                or governance_cfg.get("allow_estimated_calendar_bootstrap", False)
            )
            if allow_estimated_bootstrap:
                seed_result = service.bootstrap_estimated_calendar(
                    exchanges=scope_selection.exchanges,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                seed_result = {
                    "status": "skipped",
                    "reason": "estimated_calendar_bootstrap_disabled",
                    "rows_written": 0,
                }
            expansion = service.expand_target_dates(
                exchanges=scope_selection.exchanges,
                start_date=start_date,
                end_date=end_date,
                purpose="trading_day_governance",
                dry_run=dry_run,
            )
            gate = service.validate_quality_gate(
                expansion,
                dry_run=dry_run,
                purpose="sync",
            )
            readiness = service.readiness(
                exchanges=scope_selection.exchanges,
                start_date=start_date,
                end_date=end_date,
            )
            return {
                "status": "blocked" if gate.get("status") == "blocked" else readiness.get("status", "success"),
                "domain": "futures_trading_day_governance",
                "scope_selection": scope_selection.as_dict(),
                "seed_result": seed_result,
                "target_date_expansion": gate,
                "readiness": readiness,
                "dry_run": dry_run,
            }

        return await asyncio.to_thread(_run)

    async def run_futures_official_calendar_backfill(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
        max_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Backfill futures exchange calendars from official daily evidence only."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        from research.futures_market_data import FuturesOfficialCalendarBackfillService

        def _run() -> Dict[str, Any]:
            return FuturesOfficialCalendarBackfillService(
                storage,
                self.research_config,
                module_cfg,
            ).run(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                max_days=max_days,
            )

        return await asyncio.to_thread(_run)

    async def run_futures_master_governance(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        max_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Govern futures instruments, series, and contracts before futures price sync."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        from research.futures_market_data import FuturesMasterGovernanceService

        def _run() -> Dict[str, Any]:
            return FuturesMasterGovernanceService(
                storage,
                self.research_config,
                module_cfg,
            ).run(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                max_days=max_days,
            )

        return await asyncio.to_thread(_run)

    async def run_futures_master_discovery_governance(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
        max_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Discover unknown futures varieties and govern candidate master data."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        from research.futures_market_data import FuturesMasterDiscoveryGovernanceService

        def _run() -> Dict[str, Any]:
            return FuturesMasterDiscoveryGovernanceService(
                storage,
                self.research_config,
                module_cfg,
            ).run(
                scope_id=scope_id,
                scope_ids=scope_ids,
                exchanges=exchanges,
                categories=categories,
                instrument_ids=instrument_ids,
                series_ids=series_ids,
                series_types=series_types,
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                max_days=max_days,
            )

        return await asyncio.to_thread(_run)

    async def refresh_futures_cycle_diagnostics(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Refresh persisted futures cycle diagnostics from local bars."""
        storage = self._require_futures_storage()
        from research.futures_market_data import FuturesDiagnosticsService

        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        return await asyncio.to_thread(
            FuturesDiagnosticsService(storage, module_cfg).refresh_all,
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )

    async def recompute_futures_spreads(self) -> Dict[str, Any]:
        """Recompute configured futures spreads from local bars."""
        storage = self._require_futures_storage()
        from research.futures_market_data import FuturesSpreadService

        return await asyncio.to_thread(FuturesSpreadService(storage).recompute_all)

    async def get_research_futures_readiness(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        exchanges: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        series_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Return futures market-data readiness."""
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        if not self.research_config.enabled:
            return {
                "domain": "futures_market_data",
                "status": "disabled",
                "reason": "research_config.enabled is false",
                "blockers": ["research_config_disabled"],
                "warnings": [],
            }
        if not module_cfg.get("enabled", False):
            return {
                "domain": "futures_market_data",
                "status": "disabled",
                "reason": "research commodity_market_data module is disabled",
                "blockers": ["commodity_market_data_disabled"],
                "warnings": [],
            }
        storage = self._require_futures_storage()
        from research.futures_market_data import FuturesReadinessService

        return await asyncio.to_thread(
            FuturesReadinessService(storage, module_cfg).build,
            scope_id=scope_id,
            scope_ids=scope_ids,
            exchanges=exchanges,
            categories=categories,
            instrument_ids=instrument_ids,
            series_ids=series_ids,
            series_types=series_types,
        )

    async def get_research_futures_instruments(
        self,
        *,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """List local futures instruments and series metadata."""
        storage = self._require_futures_storage()
        instruments, series = await asyncio.to_thread(
            lambda: (
                storage.list_instruments(active_only=active_only),
                storage.list_series(active_only=active_only),
            )
        )
        return {
            "status": "success",
            "instruments": instruments,
            "series": series,
        }

    async def get_research_futures_dictionary(self) -> Dict[str, Any]:
        """Return local futures data dictionary and available metadata."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            from research.futures_market_data import default_futures_calendar_source_profiles

            categories = storage.list_categories(active_only=True)
            instruments = storage.list_instruments(active_only=True)
            series = storage.list_series(active_only=True)
            manifests = storage.list_source_manifests(enabled_only=False)
            calendar_source_profiles = [
                asdict(item) if hasattr(item, "__dataclass_fields__") else item
                for item in default_futures_calendar_source_profiles(
                    self.research_config.modules.get("commodity_market_data", {})
                )
            ]
            exchanges = sorted({item.get("exchange") for item in instruments if item.get("exchange")})
            units = sorted({item.get("unit") for item in instruments if item.get("unit")})
            currencies = sorted({item.get("currency") for item in instruments if item.get("currency")})
            series_types = sorted({item.get("series_type") for item in series if item.get("series_type")})
            return {
                "status": "success",
                "source_policy": "local_futures_db_only",
                "categories": categories,
                "exchanges": exchanges,
                "units": units,
                "currencies": currencies,
                "series_types": series_types,
                "source_profiles": manifests,
                "calendar_source_profiles": calendar_source_profiles,
                "instruments": instruments,
                "series": series,
                "warnings": [],
            }

        return await asyncio.to_thread(_load)

    async def get_research_futures_instrument_detail(self, instrument_id: str) -> Dict[str, Any]:
        """Return one local futures root instrument with series/contracts."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            instrument = storage.get_instrument(instrument_id)
            if not instrument:
                return {"status": "not_found", "instrument_id": instrument_id}
            return {
                "status": "success",
                "instrument": instrument,
                "series": storage.find_series(instrument_id=instrument_id, active_only=False),
                "contracts": storage.list_contracts(instrument_id=instrument_id, active_only=False),
            }

        return await asyncio.to_thread(_load)

    async def get_research_futures_contracts(
        self,
        *,
        instrument_id: Optional[str] = None,
        exchange: Optional[str] = None,
        contract_month: Optional[str] = None,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """List local real futures contracts."""
        storage = self._require_futures_storage()
        rows = await asyncio.to_thread(
            storage.list_contracts,
            instrument_id=instrument_id,
            exchange=exchange,
            contract_month=contract_month,
            active_only=active_only,
        )
        return {"status": "success", "row_count": len(rows), "contracts": rows}

    async def get_research_futures_contract_prices(
        self,
        contract_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read local real-contract daily bars."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            contract = storage.get_contract(contract_id)
            if not contract:
                return {"status": "not_found", "contract_id": contract_id, "rows": []}
            rows = storage.get_contract_price_bars(
                contract_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            return {"status": "success", "contract": contract, "row_count": len(rows), "rows": rows}

        return await asyncio.to_thread(_load)

    async def get_research_futures_series(
        self,
        *,
        instrument_id: Optional[str] = None,
        series_type: Optional[str] = None,
        source_profile: Optional[str] = None,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """List local futures research series."""
        storage = self._require_futures_storage()
        rows = await asyncio.to_thread(
            storage.find_series,
            instrument_id=instrument_id,
            series_type=series_type,
            source_profile=source_profile,
            active_only=active_only,
        )
        return {"status": "success", "row_count": len(rows), "series": rows}

    async def get_research_futures_default_prices(
        self,
        *,
        instrument_id: str,
        series_type: Optional[str] = None,
        source_profile: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
        include_lineage: bool = False,
    ) -> Dict[str, Any]:
        """Resolve a root futures instrument to the default local research series."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        default_type = (
            series_type
            or module_cfg.get("master_data", {}).get("default_research_series_type")
            or "main_continuous"
        )

        def _load() -> Dict[str, Any]:
            series = storage.resolve_default_series(
                instrument_id,
                series_type=default_type,
                source_profile=source_profile,
            )
            if not series:
                return {
                    "status": "not_found",
                    "instrument_id": instrument_id,
                    "series_type": default_type,
                    "rows": [],
                    "input_gaps": [
                        {
                            "field": "futures_default_series",
                            "reason": "no_active_default_futures_series",
                        }
                    ],
                }
            rows = storage.get_price_bars(
                series["series_id"],
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            payload = {
                "status": "success",
                "instrument_id": instrument_id,
                "series": series,
                "row_count": len(rows),
                "rows": rows,
                "source_policy": "local_futures_db_only",
            }
            if include_lineage:
                payload["mapping"] = storage.list_continuous_mappings(
                    series["series_id"],
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                )
            return payload

        return await asyncio.to_thread(_load)

    async def get_research_futures_mapping(
        self,
        series_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read continuous-series contract mapping rows."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            series = storage.get_series(series_id)
            if not series:
                return {"status": "not_found", "series_id": series_id, "mapping": []}
            rows = storage.list_continuous_mappings(
                series_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            return {"status": "success", "series": series, "row_count": len(rows), "mapping": rows}

        return await asyncio.to_thread(_load)

    async def get_research_futures_calendar(
        self,
        *,
        exchange: Optional[str] = None,
        instrument_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        trading_only: bool = False,
    ) -> Dict[str, Any]:
        """Read local futures trading calendar rows."""
        storage = self._require_futures_storage()
        if instrument_id:
            instrument = await asyncio.to_thread(storage.get_instrument, instrument_id)
            if not instrument:
                return {
                    "status": "not_found",
                    "instrument_id": instrument_id,
                    "calendar": [],
                    "row_count": 0,
                }
            exchange = str(instrument.get("exchange") or exchange or "").upper() or exchange
        rows = await asyncio.to_thread(
            storage.list_calendar_days,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            trading_only=trading_only,
        )
        return {"status": "success", "row_count": len(rows), "calendar": rows}

    async def get_research_futures_trading_day_governance(
        self,
        *,
        exchange: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return futures trading-day governance readiness."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        from research.futures_market_data import FuturesTradingDayGovernanceService

        exchanges = [exchange] if exchange else None
        return await asyncio.to_thread(
            FuturesTradingDayGovernanceService(storage, module_cfg).readiness,
            exchanges=exchanges,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_research_futures_calendar_evidence(
        self,
        *,
        exchange: Optional[str] = None,
        parse_status: Optional[str] = None,
        review_status: Optional[str] = "review_required",
        limit: Optional[int] = 100,
    ) -> Dict[str, Any]:
        """Return futures calendar notices and manual-review records."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            notices = storage.list_calendar_notices(
                exchange=exchange,
                parse_status=parse_status,
                limit=limit,
            )
            reviews = storage.list_manual_calendar_reviews(
                status=review_status,
                exchange=exchange,
                limit=limit,
            )
            return {
                "status": "success",
                "source_policy": "local_futures_db_only",
                "notices": notices,
                "manual_reviews": reviews,
                "notice_count": len(notices),
                "manual_review_count": len(reviews),
            }

        return await asyncio.to_thread(_load)

    async def get_research_futures_target_dates(
        self,
        *,
        exchange: Optional[str] = None,
        instrument_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        purpose: str = "api",
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Resolve governed futures target trading dates without provider calls."""
        storage = self._require_futures_storage()
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        from research.futures_market_data import FuturesTradingDayGovernanceService

        def _load() -> Dict[str, Any]:
            service = FuturesTradingDayGovernanceService(storage, module_cfg)
            return service.validate_quality_gate(
                service.expand_target_dates(
                    exchanges=[exchange] if exchange else None,
                    instrument_ids=[instrument_id] if instrument_id else None,
                    start_date=start_date,
                    end_date=end_date,
                    purpose=purpose,
                    dry_run=dry_run,
                ),
                dry_run=dry_run,
                purpose=purpose,
            )

        return await asyncio.to_thread(_load)

    async def get_research_futures_source_manifests(
        self,
        *,
        enabled_only: bool = False,
    ) -> Dict[str, Any]:
        """List local futures source manifests."""
        storage = self._require_futures_storage()
        rows = await asyncio.to_thread(storage.list_source_manifests, enabled_only=enabled_only)
        return {"status": "success", "row_count": len(rows), "source_manifests": rows}

    async def get_research_futures_prices(
        self,
        series_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read local futures price bars for a series."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            series = storage.get_series(series_id)
            if not series:
                return {"status": "not_found", "series_id": series_id, "rows": []}
            rows = storage.get_price_bars(
                series_id,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            return {
                "status": "success",
                "series": series,
                "row_count": len(rows),
                "rows": rows,
            }

        return await asyncio.to_thread(_load)

    async def get_research_futures_cycle_diagnostics(
        self,
        series_id: str,
        *,
        as_of_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Read persisted futures cycle diagnostics for a series."""
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            series = storage.get_series(series_id)
            if not series:
                return {"status": "not_found", "series_id": series_id, "diagnostics": []}
            diagnostics = storage.get_cycle_diagnostics(series_id, as_of_date=as_of_date)
            return {
                "status": "success" if diagnostics else "empty",
                "series": series,
                "diagnostics": diagnostics,
            }

        return await asyncio.to_thread(_load)

    async def get_research_futures_spreads(
        self,
        *,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """List local futures spread definitions."""
        storage = self._require_futures_storage()
        definitions = await asyncio.to_thread(
            storage.list_spread_definitions,
            active_only=active_only,
        )
        return {
            "status": "success",
            "spreads": definitions,
        }

    async def get_research_futures_spread_values(
        self,
        spread_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read local futures spread values."""
        storage = self._require_futures_storage()
        rows = await asyncio.to_thread(
            storage.get_spread_values,
            spread_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        return {
            "status": "success" if rows else "empty",
            "spread_id": spread_id,
            "row_count": len(rows),
            "rows": rows,
        }

    async def get_research_company_futures_exposure(
        self,
        instrument_id: str,
    ) -> Dict[str, Any]:
        """Read local futures exposure mappings for a company/instrument."""
        storage = self._require_futures_storage()
        normalized_id = convert_to_database_format(instrument_id)

        def _load() -> Dict[str, Any]:
            mappings = storage.get_exposure_mappings(
                scope_type="instrument",
                scope_id=normalized_id,
            )
            if not mappings:
                return {
                    "status": "missing",
                    "instrument_id": normalized_id,
                    "mappings": [],
                    "input_gaps": [
                        {
                            "field": "futures_exposure_mapping",
                            "requiredness": "cyclical_dcf_optional_until_mapping_rollout",
                            "reason": "no_local_futures_exposure_mapping",
                        }
                    ],
                }
            return {
                "status": "success",
                "instrument_id": normalized_id,
                "mappings": mappings,
                "input_gaps": [],
            }

        return await asyncio.to_thread(_load)

    async def get_local_futures_cycle_inputs_for_dcf(
        self,
        instrument_id: str,
    ) -> Dict[str, Any]:
        """Return local futures exposure and diagnostics for cyclical DCF."""
        exposure = await self.get_research_company_futures_exposure(instrument_id)
        if exposure.get("status") != "success":
            return exposure
        storage = self._require_futures_storage()

        def _load() -> Dict[str, Any]:
            diagnostics_by_series: Dict[str, Any] = {}
            for mapping in exposure.get("mappings", []):
                series_ids = []
                if mapping.get("revenue_series_id"):
                    series_ids.append(mapping["revenue_series_id"])
                series_ids.extend(mapping.get("cost_series_ids") or [])
                for series_id in sorted(set(series_ids)):
                    diagnostics_by_series[series_id] = storage.get_cycle_diagnostics(series_id)
            return {
                **exposure,
                "diagnostics_by_series": diagnostics_by_series,
            }

        return await asyncio.to_thread(_load)

    async def get_research_financial_summary(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 financial summary。"""
        storage = self._require_research_storage()
        normalized_id = convert_to_database_format(instrument_id)
        summary = await asyncio.to_thread(
            self._run_financial_storage_call,
            storage,
            "get_financial_summary",
            normalized_id,
            include_snapshot=include_snapshot,
        )
        if summary is not None:
            return summary

        instrument = await self._get_research_instrument_info(normalized_id)
        if instrument and self._module_allows_optional_empty_exchange(
            "financial_summary",
            instrument.get("exchange"),
        ):
            return self._build_empty_financial_summary_response(
                instrument,
                include_snapshot=include_snapshot,
            )
        return None

    async def get_research_shareholders(
        self,
        instrument_id: str,
        *,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 shareholder snapshot。"""
        storage = self._require_research_storage()
        self._require_research_shareholders_config(require_snapshot_api=True)
        normalized_id = convert_to_database_format(instrument_id)
        snapshot = await asyncio.to_thread(
            storage.get_shareholder_snapshot,
            normalized_id,
            include_snapshot=include_snapshot,
        )
        if snapshot is not None:
            return snapshot

        instrument = await self._get_research_instrument_info(normalized_id)
        if instrument and self._module_allows_optional_empty_exchange(
            "shareholders",
            instrument.get("exchange"),
        ):
            return self._build_empty_shareholder_response(
                instrument,
                include_snapshot=include_snapshot,
            )
        return None

    async def get_research_financial_statements(
        self,
        instrument_id: str,
        *,
        include_statements: bool = True,
        report_period: Optional[str] = None,
        requested_canonical_facts: Optional[List[str]] = None,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        include_local_core: bool = False,
        include_industry_facts: bool = False,
        allow_remote_extension: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 financial statements bundle。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("financial_statements", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research financial_statements module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        bundle = await asyncio.to_thread(
            self._run_financial_storage_call,
            storage,
            "get_financial_statement_bundle",
            normalized_id,
            include_statements=include_statements,
            report_period=report_period,
        )
        if bundle is not None:
            service_layers = await self._get_research_financial_statement_service_layers(
                storage,
                normalized_id,
                bundle=bundle,
                report_period=report_period,
                requested_canonical_facts=requested_canonical_facts,
                profile=profile,
                mapping_version=mapping_version,
                include_local_core=include_local_core,
                include_industry_facts=include_industry_facts,
                allow_remote_extension=allow_remote_extension,
            )
            if service_layers:
                bundle["service_layers"] = service_layers
            return bundle

        instrument = await self._get_research_instrument_info(normalized_id)
        service_layers = await self._get_research_financial_statement_service_layers(
            storage,
            normalized_id,
            instrument=instrument,
            report_period=report_period,
            requested_canonical_facts=requested_canonical_facts,
            profile=profile,
            mapping_version=mapping_version,
            include_local_core=include_local_core,
            include_industry_facts=include_industry_facts,
            allow_remote_extension=allow_remote_extension,
        )
        if service_layers and self._financial_statement_service_layers_have_data(service_layers):
            return self._build_financial_statements_service_layer_response(
                normalized_id,
                instrument,
                service_layers,
                report_period=report_period,
            )
        if instrument and self._module_allows_optional_empty_exchange(
            "financial_statements",
            instrument.get("exchange"),
        ):
            placeholder = self._build_empty_financial_statements_response(
                instrument,
                include_statements=include_statements,
            )
            if service_layers:
                placeholder["service_layers"] = service_layers
            return placeholder
        return None

    async def get_research_financial_statements_history(
        self,
        instrument_id: str,
        *,
        include_statements: bool = False,
        period_window: str = "latest",
        rolling_quarters: int = 12,
        report_periods: Optional[List[str]] = None,
        requested_canonical_facts: Optional[List[str]] = None,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        include_local_core: bool = False,
        include_industry_facts: bool = False,
        allow_remote_extension: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域多报告期 financial statements history。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("financial_statements", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research financial_statements module is disabled")
        if period_window != "latest" and not report_periods:
            raise ValueError("Only period_window=latest is supported unless report_periods is supplied")

        normalized_id = convert_to_database_format(instrument_id)
        limit = max(1, min(int(rolling_quarters), 40))
        requested_periods = [
            str(item).strip()
            for item in (report_periods or [])
            if str(item).strip()
        ]
        bundles = await asyncio.to_thread(
            self._run_financial_storage_call,
            storage,
            "get_financial_statement_bundles",
            normalized_id,
            include_statements=include_statements,
            report_periods=requested_periods or None,
            limit=limit,
        )
        instrument = await self._get_research_instrument_info(normalized_id)
        items: List[Dict[str, Any]] = []
        for bundle in bundles:
            service_layers = await self._get_research_financial_statement_service_layers(
                storage,
                normalized_id,
                bundle=bundle,
                instrument=instrument,
                report_period=bundle.get("report_period"),
                requested_canonical_facts=requested_canonical_facts,
                profile=profile,
                mapping_version=mapping_version,
                include_local_core=include_local_core,
                include_industry_facts=include_industry_facts,
                allow_remote_extension=allow_remote_extension,
            )
            if service_layers:
                bundle["service_layers"] = service_layers
            items.append(bundle)

        if not items and instrument is None:
            return None
        symbol = (
            (items[0].get("symbol") if items else None)
            or (instrument or {}).get("symbol")
            or normalized_id.split(".")[0]
        )
        exchange = (
            (items[0].get("exchange") if items else None)
            or (instrument or {}).get("exchange")
            or ""
        )
        return {
            "instrument_id": normalized_id,
            "symbol": symbol,
            "exchange": exchange,
            "period_window": period_window,
            "rolling_quarters": limit,
            "requested_report_periods": requested_periods,
            "report_periods": [item.get("report_period") for item in items],
            "period_count": len(items),
            "items": items,
        }

    def _financial_statement_service_layer_config(self) -> Dict[str, Any]:
        module_cfg = self.research_config.modules.get("financial_statements", {})
        module_layers = module_cfg.get("service_layers")
        if isinstance(module_layers, dict):
            return module_layers
        akshare_cfg = self.research_config.sources.get("akshare", {}).get(
            "financial_statements",
            {},
        )
        layers = akshare_cfg.get("service_layers", {})
        return layers if isinstance(layers, dict) else {}

    async def _get_research_financial_statement_service_layers(
        self,
        storage: Any,
        instrument_id: str,
        *,
        bundle: Optional[Dict[str, Any]] = None,
        instrument: Optional[Dict[str, Any]] = None,
        report_period: Optional[str] = None,
        requested_canonical_facts: Optional[List[str]] = None,
        profile: Optional[str] = None,
        mapping_version: Optional[str] = None,
        include_local_core: bool = False,
        include_industry_facts: bool = False,
        allow_remote_extension: bool = False,
    ) -> Dict[str, Any]:
        requested = [str(item) for item in (requested_canonical_facts or []) if str(item)]
        if (
            not include_local_core
            and not include_industry_facts
            and not allow_remote_extension
            and not requested
        ):
            return {}

        layers_cfg = self._financial_statement_service_layer_config()
        result: Dict[str, Any] = {}
        local_core_payload: Optional[Dict[str, Any]] = None
        local_cfg = layers_cfg.get("local_core", {})
        resolved_mapping_version = (
            mapping_version
            or local_cfg.get("mapping_version")
            or FINANCIAL_MAPPING_VERSION
        )
        profile_resolution = None
        resolved_profile = profile
        if (include_local_core or include_industry_facts or requested) and resolved_profile is None:
            profile_resolution = await self._resolve_research_financial_statement_profile(
                storage,
                instrument_id,
                instrument=instrument or bundle,
            )
            resolved_profile = profile_resolution.get("profile") if profile_resolution else None
        if include_local_core or requested:
            if not local_cfg.get("enabled", False):
                local_core_payload = {
                    "status": "disabled_by_config",
                    "ready": False,
                    "instrument_id": instrument_id,
                    "report_period": report_period,
                    "profile": resolved_profile,
                    "profile_resolution": profile_resolution,
                    "mapping_version": resolved_mapping_version,
                    "requested_canonical_facts": requested,
                    "facts": {},
                    "missing_fields": [
                        {
                            "canonical_fact": item,
                            "reason": "local_core_disabled_by_config",
                            "mapping_version": resolved_mapping_version,
                            "profile": resolved_profile,
                        }
                        for item in requested
                    ],
                }
            else:
                local_core_payload = await asyncio.to_thread(
                    self._run_financial_storage_call,
                    storage,
                    "get_financial_local_core_facts",
                    instrument_id,
                    report_period=report_period,
                    requested_canonical_facts=requested or None,
                    profile=resolved_profile,
                    mapping_version=resolved_mapping_version,
                    include_history=True,
                )
                local_core_payload["profile_resolution"] = profile_resolution
                local_core_payload["status"] = (
                    "passed" if local_core_payload.get("ready") else "partial"
                )
            result["local_core"] = local_core_payload

        if include_industry_facts:
            industry_cfg = layers_cfg.get("industry_pack", {})
            industry_enabled = bool(industry_cfg.get("enabled", True))
            industry_pack_version = industry_cfg.get(
                "pack_version",
                INDUSTRY_FACT_PACK_VERSION,
            )
            if not industry_enabled:
                result["industry_pack"] = {
                    "status": "disabled_by_config",
                    "ready": False,
                    "is_optional": True,
                    "instrument_id": instrument_id,
                    "report_period": report_period,
                    "profile": resolved_profile,
                    "profile_resolution": profile_resolution,
                    "pack_version": industry_pack_version,
                    "facts": {},
                    "missing_fields": [
                        {
                            "canonical_fact": None,
                            "reason": "industry_pack_disabled_by_config",
                            "profile": resolved_profile,
                            "pack_version": industry_pack_version,
                            "report_period": report_period,
                        }
                    ],
                }
            else:
                industry_requested_facts = get_local_core_industry_canonical_facts(
                    profile=resolved_profile,
                    pack_version=industry_pack_version,
                )
                industry_local_result = None
                if industry_requested_facts:
                    industry_local_result = await asyncio.to_thread(
                        self._run_financial_storage_call,
                        storage,
                        "get_financial_local_core_facts",
                        instrument_id,
                        report_period=report_period,
                        requested_canonical_facts=industry_requested_facts,
                        profile=resolved_profile,
                        mapping_version=resolved_mapping_version,
                        include_history=True,
                    )
                industry_numeric_rows = await asyncio.to_thread(
                    self._run_financial_storage_call,
                    storage,
                    "get_financial_numeric_facts",
                    instrument_id,
                    report_period=report_period,
                    include_history=True,
                )
                industry_payload = build_industry_pack_payload(
                    instrument_id=instrument_id,
                    report_period=report_period,
                    profile=resolved_profile,
                    local_fact_result=industry_local_result,
                    numeric_fact_rows=industry_numeric_rows,
                    pack_version=industry_pack_version,
                )
                industry_payload["profile_resolution"] = profile_resolution
                result["industry_pack"] = industry_payload

        if allow_remote_extension:
            remote_cfg = layers_cfg.get("remote_extension", {})
            if not remote_cfg.get("enabled", False):
                result["remote_extension"] = {
                    "status": "disabled_by_config",
                    "is_remote": True,
                    "source": remote_cfg.get("source", "akshare"),
                    "statement_interface": remote_cfg.get(
                        "statement_interface",
                        "eastmoney_report",
                    ),
                    "instrument_id": instrument_id,
                    "requested_canonical_facts": requested,
                    "facts": [],
                    "missing_fields": [
                        {
                            "canonical_fact": item,
                            "reason": "remote_extension_disabled_by_config",
                        }
                        for item in requested
                    ],
                }
            elif requested:
                remote_instrument = instrument or {
                    "instrument_id": instrument_id,
                    "symbol": (bundle or {}).get("symbol"),
                    "exchange": (bundle or {}).get("exchange"),
                }
                exchange = (
                    remote_instrument.get("exchange")
                    or (bundle or {}).get("exchange")
                    or ""
                )
                from research.financial_remote_extension import (
                    FinancialRemoteExtensionService,
                )

                service = FinancialRemoteExtensionService(
                    provider_config={
                        "statement_interface_order": [
                            remote_cfg.get("statement_interface", "eastmoney_report")
                        ],
                        "request_timeout_seconds": remote_cfg.get(
                            "request_timeout_seconds",
                            30.0,
                        ),
                        "request_interval_seconds": remote_cfg.get(
                            "request_interval_seconds",
                            0.5,
                        ),
                        "retry_attempts": remote_cfg.get("retry_attempts", 2),
                        "retry_backoff_seconds": remote_cfg.get(
                            "retry_backoff_seconds",
                            1.0,
                        ),
                    }
                )
                result["remote_extension"] = await service.fetch_facts(
                    instrument=remote_instrument,
                    exchange=exchange,
                    requested_canonical_facts=requested,
                    report_periods=[report_period] if report_period else None,
                    allow_remote_extension=True,
                )
        return result

    @staticmethod
    def _run_financial_storage_call(
        storage: Any,
        method_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Run a financial-domain storage call against the split financial DB when available."""
        method = getattr(storage, method_name)
        try:
            inspect.getattr_static(storage, "financial_database_scope")
        except AttributeError:
            scope_factory = None
        else:
            scope_factory = getattr(storage, "financial_database_scope", None)
        if callable(scope_factory):
            try:
                scope = scope_factory()
                enter = getattr(scope, "__enter__")
                getattr(scope, "__exit__")
            except (AttributeError, TypeError):
                scope = None
            if scope is not None and callable(enter):
                with scope:
                    return method(*args, **kwargs)
        return method(*args, **kwargs)

    async def _resolve_research_financial_statement_profile(
        self,
        storage: Any,
        instrument_id: str,
        *,
        instrument: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        industry_membership = await asyncio.to_thread(
            storage.get_industry_membership,
            instrument_id,
            include_snapshot=False,
        )
        if isinstance(industry_membership, dict) and not industry_membership.get("instrument_id"):
            industry_membership = {**industry_membership, "instrument_id": instrument_id}
        company_profile = await asyncio.to_thread(
            storage.get_company_profile,
            instrument_id,
            include_snapshot=False,
        )
        return resolve_financial_statement_profile(
            industry_membership=industry_membership,
            company_profile=company_profile,
            instrument=instrument,
        ).to_dict()

    @staticmethod
    def _financial_statement_service_layers_have_data(
        service_layers: Dict[str, Any],
    ) -> bool:
        local_core = service_layers.get("local_core") or {}
        if local_core.get("facts"):
            return True
        industry_pack = service_layers.get("industry_pack") or {}
        if industry_pack.get("facts"):
            return True
        remote_extension = service_layers.get("remote_extension") or {}
        return bool(remote_extension.get("facts"))

    def _build_financial_statements_service_layer_response(
        self,
        instrument_id: str,
        instrument: Optional[Dict[str, Any]],
        service_layers: Dict[str, Any],
        *,
        report_period: Optional[str] = None,
    ) -> Dict[str, Any]:
        data_as_of, created_at, updated_at = self._empty_placeholder_timestamps()
        local_core = service_layers.get("local_core") or {}
        selected_period = (
            report_period
            or local_core.get("report_period")
            or (service_layers.get("industry_pack") or {}).get("report_period")
            or self._latest_remote_extension_report_period(service_layers)
            or ""
        )
        return {
            "instrument_id": instrument_id,
            "symbol": (instrument or {}).get("symbol") or instrument_id.split(".")[0],
            "exchange": (instrument or {}).get("exchange") or instrument_id.split(".")[-1],
            "report_period": selected_period,
            "publish_date": None,
            "fiscal_year": self._fiscal_year_from_report_period(selected_period),
            "fiscal_quarter": self._fiscal_quarter_from_report_period(selected_period),
            "currency": "CNY",
            "schema_version": "financial_service_layers.v1",
            "source": "service_layers",
            "source_mode": "local_or_explicit_remote",
            "data_as_of": data_as_of,
            "ingestion_run_id": None,
            "created_at": created_at,
            "updated_at": updated_at,
            "facts": self._flatten_financial_service_layer_facts(service_layers),
            "indicators": None,
            "statements": [],
            "service_layers": service_layers,
        }

    @staticmethod
    def _flatten_financial_service_layer_facts(
        service_layers: Dict[str, Any],
    ) -> Dict[str, Any]:
        facts: Dict[str, Any] = {}
        for canonical_fact, row in (service_layers.get("local_core") or {}).get(
            "facts",
            {},
        ).items():
            facts[canonical_fact] = row.get("fact_value")
        for row in (service_layers.get("remote_extension") or {}).get("facts", []):
            canonical_fact = row.get("canonical_fact_name")
            if canonical_fact and canonical_fact not in facts:
                facts[canonical_fact] = row.get("fact_value")
        return facts

    @staticmethod
    def _latest_remote_extension_report_period(service_layers: Dict[str, Any]) -> Optional[str]:
        periods = [
            str(row.get("report_period"))
            for row in (service_layers.get("remote_extension") or {}).get("facts", [])
            if row.get("report_period")
        ]
        return max(periods) if periods else None

    @staticmethod
    def _fiscal_year_from_report_period(report_period: Optional[str]) -> Optional[int]:
        if not report_period:
            return None
        try:
            return int(str(report_period)[:4])
        except ValueError:
            return None

    @staticmethod
    def _fiscal_quarter_from_report_period(report_period: Optional[str]) -> Optional[int]:
        if not report_period:
            return None
        text = str(report_period)
        if text.endswith("Q1") or text.endswith("-03-31"):
            return 1
        if text.endswith("Q2") or text.endswith("-06-30"):
            return 2
        if text.endswith("Q3") or text.endswith("-09-30"):
            return 3
        if text.endswith("Q4") or text.endswith("-12-31"):
            return 4
        return None

    async def get_research_valuation_history(
        self,
        instrument_id: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 120,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 valuation history。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        normalized_id = convert_to_database_format(instrument_id)

        from research.query_service import ResearchQueryService
        from research.valuation_service import ResearchValuationService

        query_service = ResearchQueryService(storage)
        valuation_service = ResearchValuationService(module_cfg)
        identity = valuation_service.history_identity()
        rows = await asyncio.to_thread(
            query_service.get_valuation_history_rows,
            normalized_id,
            start_date=None if start_date is None else start_date.isoformat(),
            end_date=None if end_date is None else end_date.isoformat(),
            limit=limit,
            include_details=include_details,
            calc_method=identity["calc_method"],
            calc_version=identity["calc_version"],
            parameter_hash=identity["parameter_hash"],
            parameter_hashes=identity.get("compatible_parameter_hashes"),
        )
        return valuation_service.build_history_response(rows)

    async def get_research_valuation_percentile(
        self,
        instrument_id: str,
        *,
        as_of_date: Optional[date] = None,
        quarters: int = 12,
        metrics: Optional[List[str]] = None,
        min_points: int = 60,
        negative_policy: str = "flag",
        include_series: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域单品种估值历史分位。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None

        from research.query_service import ResearchQueryService
        from research.valuation_service import ResearchValuationService

        query_service = ResearchQueryService(storage)
        valuation_service = ResearchValuationService(module_cfg)
        identity = valuation_service.history_identity()
        rows = await asyncio.to_thread(
            query_service.get_valuation_history_rows,
            normalized_id,
            start_date=None,
            end_date=None if as_of_date is None else as_of_date.isoformat(),
            limit=0,
            include_details=False,
            calc_method=identity["calc_method"],
            calc_version=identity["calc_version"],
            parameter_hash=identity["parameter_hash"],
            parameter_hashes=identity.get("compatible_parameter_hashes"),
        )
        return valuation_service.build_history_percentile_response(
            rows,
            instrument=instrument,
            as_of_date=None if as_of_date is None else as_of_date.isoformat(),
            quarters=quarters,
            metrics=metrics,
            min_points=min_points,
            negative_policy=negative_policy,
            include_series=include_series,
        )

    async def get_research_relative_valuation(
        self,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域相对估值。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None

        from research.query_service import ResearchQueryService
        from research.valuation_service import ResearchValuationService

        query_service = ResearchQueryService(storage)
        valuation_service = ResearchValuationService(module_cfg)
        identity = valuation_service.history_identity()
        subject_row = await asyncio.to_thread(
            query_service.get_latest_valuation_history_row,
            normalized_id,
            include_details=True,
            calc_method=identity["calc_method"],
            calc_version=identity["calc_version"],
            parameter_hash=identity["parameter_hash"],
            parameter_hashes=identity.get("compatible_parameter_hashes"),
        )
        industry_membership = await asyncio.to_thread(
            query_service.get_industry_membership,
            normalized_id,
            include_snapshot=True,
        )

        peer_rows: list[Dict[str, Any]] = []
        relative_cfg = module_cfg.get("relative", {})
        if industry_membership is not None:
            benchmark_context = valuation_service.resolve_relative_benchmark_context(
                industry_membership
            )
            require_authoritative = bool(relative_cfg.get("require_authoritative", True))
            membership_eligible = (
                not require_authoritative
                or industry_membership.get("mapping_status") == "authoritative"
            )
        else:
            benchmark_context = {}
            membership_eligible = False

        if (
            membership_eligible
            and benchmark_context.get("supported")
            and benchmark_context.get("benchmark_code")
        ):
            peer_rows = await asyncio.to_thread(
                storage.get_latest_peer_valuation_rows,
                benchmark_context["benchmark_code"],
                exclude_instrument_id=normalized_id,
                taxonomy_system=industry_membership.get("taxonomy_system", "sw"),
                taxonomy_version=industry_membership.get("taxonomy_version"),
                benchmark_field=benchmark_context["benchmark_field"],
                limit=int(relative_cfg.get("max_peer_rows", 20)),
                include_details=False,
                calc_method=identity["calc_method"],
                calc_version=identity["calc_version"],
                parameter_hash=identity["parameter_hash"],
                parameter_hashes=identity.get("compatible_parameter_hashes"),
            )

        result = valuation_service.build_relative_valuation(
            instrument=instrument,
            subject_row=subject_row,
            industry_membership=industry_membership,
            peer_rows=peer_rows,
        )
        result["industry_index_benchmark"] = await (
            self._get_research_industry_index_benchmark_for_membership(
                storage,
                industry_membership,
                benchmark_context.get("benchmark_field"),
                include_payload=False,
            )
        )
        return result

    async def get_research_dcf_valuation(
        self,
        instrument_id: str,
        *,
        growth_rate: Optional[float] = None,
        discount_rate: Optional[float] = None,
        terminal_growth: Optional[float] = None,
        projection_years: Optional[int] = None,
        model_profile: Optional[str] = None,
        model_strategy: Optional[str] = None,
        valuation_date: Optional[str] = None,
        scenario_set: Optional[str] = None,
        terminal_method: Optional[str] = None,
        cash_flow_model: Optional[str] = None,
        include_forecast_rows: bool = True,
        include_sensitivity: bool = True,
        include_lineage: bool = True,
        include_model_comparison: bool = False,
        include_workbook: bool = False,
        workbook_style: Optional[str] = None,
        force_model: bool = False,
        research_mode: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 DCF 估值。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None

        financial_bundle = await asyncio.to_thread(
            self._run_financial_storage_call,
            storage,
            "get_financial_statement_bundle",
            normalized_id,
            include_statements=False,
        )
        if financial_bundle is None:
            return None
        financial_bundle = await asyncio.to_thread(
            self._enrich_dcf_bundle_with_broker_risk_control_facts,
            storage,
            normalized_id,
            financial_bundle,
        )

        latest_quotes = await self.db_ops.get_daily_data(
            instrument_id=normalized_id,
            limit=1,
            return_format="pandas",
        )
        latest_close = None
        if latest_quotes is not None and not latest_quotes.empty:
            latest_close = float(latest_quotes.iloc[0]["close"])

        overrides = {
            key: value
            for key, value in {
                "growth_rate": growth_rate,
                "discount_rate": discount_rate,
                "terminal_growth": terminal_growth,
                "projection_years": projection_years,
                "model_profile": model_profile,
                "model_strategy": model_strategy,
                "valuation_date": valuation_date,
                "scenario_set": scenario_set,
                "terminal_method": terminal_method,
                "cash_flow_model": cash_flow_model,
                "include_forecast_rows": include_forecast_rows,
                "include_sensitivity": include_sensitivity,
                "include_lineage": include_lineage,
                "include_model_comparison": include_model_comparison,
                "include_workbook": include_workbook,
                "workbook_style": workbook_style,
                "force_model": force_model,
                "research_mode": research_mode,
            }.items()
            if value is not None
        }
        dcf_beta_cfg = module_cfg.get("dcf", {}).get("beta", {})
        if "beta" not in overrides and dcf_beta_cfg.get("enabled", True):
            try:
                beta_payload = await self.get_research_beta(
                    normalized_id,
                    benchmark_family=dcf_beta_cfg.get("benchmark_family", "market_default"),
                    benchmark_instrument_id=dcf_beta_cfg.get("benchmark_instrument_id"),
                    window_days=int(dcf_beta_cfg.get("window_days", 252)),
                    include_details=False,
                )
            except RuntimeError:
                beta_payload = None
            if beta_payload:
                beta_result = next(
                    (
                        item
                        for item in beta_payload.get("items", [])
                        if item.get("status") == "success" and item.get("beta") is not None
                    ),
                    None,
                )
                if beta_result:
                    overrides["beta"] = float(beta_result["beta"])
                    overrides["beta_source"] = "beta_on_demand"
                    overrides["beta_benchmark"] = {
                        "benchmark_family": beta_result.get("benchmark_family"),
                        "benchmark_instrument_id": beta_result.get("benchmark_instrument_id"),
                        "benchmark_name": beta_result.get("benchmark_name"),
                        "window_days": beta_result.get("window_days"),
                        "as_of_date": beta_result.get("as_of_date"),
                    }

        futures_cycle_context = await self._get_dcf_futures_cycle_context(normalized_id)
        if futures_cycle_context:
            overrides.setdefault(
                "commodity_price_assumption",
                futures_cycle_context.get("commodity_price_assumption"),
            )
            overrides.setdefault(
                "cycle_index_level",
                futures_cycle_context.get("cycle_index_level"),
            )
            overrides["futures_cycle_context"] = futures_cycle_context

        cache_key = self._build_dcf_run_cache_key(
            normalized_id,
            financial_bundle,
            latest_close,
            overrides,
        )
        cached_result = self._get_dcf_run_cache(cache_key, module_cfg)
        if cached_result is not None:
            return cached_result

        from research.valuation_service import ResearchValuationService

        valuation_service = ResearchValuationService(module_cfg)
        result = valuation_service.run_dcf(
            instrument=instrument,
            financial_bundle=financial_bundle,
            latest_close=latest_close,
            overrides=overrides,
        )
        if futures_cycle_context:
            result = deepcopy(result)
            cyclical_diagnostics = result.setdefault("cyclical_model_diagnostics", {})
            if isinstance(cyclical_diagnostics, dict):
                cyclical_diagnostics["futures_market_data"] = futures_cycle_context
        self._store_dcf_run_cache(cache_key, result, module_cfg)
        return result

    async def _get_dcf_futures_cycle_context(
        self,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return compact local futures diagnostics for DCF, if configured."""
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        if not module_cfg.get("enabled", False):
            return None
        try:
            payload = await self.get_local_futures_cycle_inputs_for_dcf(instrument_id)
        except RuntimeError:
            return None
        if payload.get("status") != "success":
            return None
        diagnostics_by_series = payload.get("diagnostics_by_series") or {}
        selected_series_id = None
        selected_diagnostic = None
        for series_id, diagnostics in diagnostics_by_series.items():
            if not isinstance(diagnostics, list) or not diagnostics:
                continue
            selected_series_id = series_id
            selected_diagnostic = next(
                (
                    item for item in diagnostics
                    if item.get("lookback_years") == 10 and item.get("percentile") is not None
                ),
                None,
            ) or next(
                (item for item in diagnostics if item.get("percentile") is not None),
                diagnostics[0],
            )
            break
        if not selected_diagnostic:
            return None
        return {
            "status": "success",
            "instrument_id": instrument_id,
            "selected_series_id": selected_series_id,
            "commodity_price_assumption": selected_diagnostic.get("latest_price"),
            "cycle_index_level": selected_diagnostic.get("percentile"),
            "diagnostic": selected_diagnostic,
            "exposure_mappings": payload.get("mappings") or [],
            "diagnostics_by_series": diagnostics_by_series,
            "source_policy": "local_futures_db_only",
        }

    def _enrich_dcf_bundle_with_broker_risk_control_facts(
        self,
        storage: Any,
        instrument_id: str,
        financial_bundle: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Merge local broker risk-control canonical facts into the DCF input bundle."""
        from research.broker_risk_control import (
            BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
            BROKER_RISK_CONTROL_CANONICAL_FACTS,
            BROKER_RISK_CONTROL_SOURCE_PROFILE,
        )

        if not hasattr(storage, "get_financial_numeric_facts"):
            return financial_bundle
        enriched = deepcopy(financial_bundle)
        latest_facts = enriched.setdefault("latest_facts", {})
        if not isinstance(latest_facts, dict):
            latest_facts = {}
            enriched["latest_facts"] = latest_facts
        lineage = enriched.setdefault("lineage", {})
        if not isinstance(lineage, dict):
            lineage = {}
            enriched["lineage"] = lineage
        selected_rows: Dict[str, Dict[str, Any]] = {}
        for canonical_name in BROKER_RISK_CONTROL_CANONICAL_FACTS:
            try:
                rows = self._run_financial_storage_call(
                    storage,
                    "get_financial_numeric_facts",
                    instrument_id,
                    include_history=True,
                    canonical_fact_name=canonical_name,
                    limit=8,
                )
            except Exception:
                continue
            if not isinstance(rows, list):
                continue
            row = self._select_latest_broker_risk_control_fact(rows)
            if row is None:
                continue
            selected_rows[canonical_name] = row
            latest_facts.setdefault(canonical_name, row.get("fact_value"))
            enriched.setdefault(canonical_name, row.get("fact_value"))

        if not selected_rows:
            return enriched

        facts_lineage = {}
        for canonical_name, row in selected_rows.items():
            raw_fact = row.get("raw_fact") or {}
            dimensions = row.get("dimensions") or {}
            source_file_id = row.get("source_file_id")
            manifest = self._find_financial_source_manifest(
                storage,
                instrument_id=instrument_id,
                report_period=row.get("report_period"),
                source=row.get("source"),
                source_file_id=source_file_id,
            )
            facts_lineage[canonical_name] = {
                "canonical_fact_name": canonical_name,
                "report_period": row.get("report_period"),
                "source_profile": raw_fact.get("source_profile"),
                "source": row.get("source"),
                "source_mode": row.get("source_mode"),
                "source_file_id": source_file_id,
                "unit": row.get("unit"),
                "canonical_unit": row.get("canonical_unit"),
                "data_available_date": (
                    row.get("data_available_date")
                    or (manifest or {}).get("published_at")
                    or (manifest or {}).get("downloaded_at")
                ),
                "parser_version": row.get("parser_version"),
                "report_scope": dimensions.get("report_scope") or raw_fact.get("report_scope"),
                "physical_table": row.get("physical_table"),
            }
        lineage["broker_risk_control"] = {
            "source_profile": "broker_regulatory_financial_facts",
            "primary_source_profile": BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
            "supplementary_source_profile": BROKER_RISK_CONTROL_SOURCE_PROFILE,
            "facts": facts_lineage,
        }
        net_capital_lineage = facts_lineage.get("net_capital") or {}
        if net_capital_lineage:
            latest_facts.setdefault(
                "net_capital_report_scope",
                net_capital_lineage.get("report_scope"),
            )
            enriched.setdefault("net_capital_report_scope", net_capital_lineage.get("report_scope"))
            latest_facts.setdefault(
                "net_capital_data_available_date",
                net_capital_lineage.get("data_available_date"),
            )
        return enriched

    @staticmethod
    def _select_latest_broker_risk_control_fact(
        rows: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        from research.broker_risk_control import BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE

        def _source_priority(row: Dict[str, Any]) -> int:
            raw_fact = row.get("raw_fact") or {}
            return 1 if raw_fact.get("source_profile") == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE else 0

        candidates = [
            row for row in rows if row.get("fact_value") is not None and row.get("report_period")
        ]
        if not candidates:
            return None
        return max(
            candidates,
            key=lambda row: (
                str(row.get("report_period") or ""),
                _source_priority(row),
                str(row.get("updated_at") or ""),
            ),
        )

    @staticmethod
    def _find_financial_source_manifest(
        storage: Any,
        *,
        instrument_id: str,
        report_period: Optional[str],
        source: Optional[str],
        source_file_id: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not hasattr(storage, "get_financial_source_file_manifests"):
            return None
        try:
            manifests = DataManager._run_financial_storage_call(
                storage,
                "get_financial_source_file_manifests",
                instrument_id=instrument_id,
                report_period=report_period,
                source=source,
            )
        except Exception:
            return None
        if not isinstance(manifests, list):
            return None
        for manifest in manifests:
            if manifest.get("source_file_id") == source_file_id:
                return manifest
        return manifests[0] if manifests else None

    async def get_research_dcf_assumptions(
        self,
        *,
        market: str = "SSE",
        currency: str = "CNY",
    ) -> Dict[str, Any]:
        """读取专业 DCF 本地假设参数。"""
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        from research.professional_dcf import ProfessionalDcfEngine

        engine = ProfessionalDcfEngine(
            module_cfg.get("dcf", {}).get("professional")
            or module_cfg.get("professional_dcf")
            or module_cfg.get("dcf")
        )
        assumptions = engine.get_assumptions(market=market, currency=currency)
        return {
            "market": market,
            "currency": currency,
            "assumptions": list(assumptions.values()),
            "source_registry": engine.list_assumption_sources(),
        }

    async def refresh_research_dcf_assumptions(
        self,
        *,
        source_profile: str = "manual_config",
        timeout_seconds: Optional[int] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """显式刷新专业 DCF 假设参数。

        当前阶段只暴露本地 source registry 和刷新诊断，不在 DCF 计算路径
        里隐式访问外部数据源。
        """
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        from research.professional_dcf import ProfessionalDcfEngine

        engine = ProfessionalDcfEngine(
            module_cfg.get("dcf", {}).get("professional")
            or module_cfg.get("professional_dcf")
            or module_cfg.get("dcf")
        )
        return engine.refresh_assumptions(
            source_profile=source_profile,
            timeout_seconds=timeout_seconds,
            dry_run=dry_run,
        )

    async def get_research_dcf_workbook_artifact(self, artifact_id: str) -> Optional[Path]:
        """Resolve a DCF workbook artifact inside the configured report directory."""
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")
        if not artifact_id.startswith("dcf_"):
            raise ValueError("invalid DCF workbook artifact id")
        allowed_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
        if any(ch not in allowed_chars for ch in artifact_id):
            raise ValueError("invalid DCF workbook artifact id")

        dcf_cfg = module_cfg.get("dcf", {}) if isinstance(module_cfg, dict) else {}
        professional = dcf_cfg.get("professional", {}) if isinstance(dcf_cfg, dict) else {}
        workbook_cfg = professional.get("workbook") or dcf_cfg.get("workbook") or {}
        artifact_dir = Path(workbook_cfg.get("artifact_dir", "data/reports/dcf_workbooks"))
        candidate = (artifact_dir / f"{artifact_id}.xlsx").resolve()
        root = artifact_dir.resolve()
        if root not in candidate.parents:
            raise ValueError("invalid DCF workbook artifact path")
        if not candidate.exists() or not candidate.is_file():
            return None
        return candidate

    def _build_dcf_run_cache_key(
        self,
        instrument_id: str,
        financial_bundle: Dict[str, Any],
        latest_close: Optional[float],
        overrides: Dict[str, Any],
    ) -> str:
        payload = {
            "instrument_id": instrument_id,
            "financial_bundle_hash": self._stable_hash(financial_bundle),
            "latest_close": latest_close,
            "overrides": overrides,
        }
        return self._stable_hash(payload)

    def _get_dcf_run_cache(
        self,
        cache_key: str,
        module_cfg: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        cache_cfg = self._dcf_cache_config(module_cfg)
        if not cache_cfg.get("enabled", False):
            return None
        entry = self._dcf_run_cache.get(cache_key)
        if not entry:
            return None
        now = datetime.now(timezone.utc)
        if entry["expires_at_dt"] <= now:
            self._dcf_run_cache.pop(cache_key, None)
            return None
        result = deepcopy(entry["result"])
        result["cache_info"] = {
            "enabled": True,
            "cache_hit": True,
            "cache_key": cache_key,
            "cached_at": entry["cached_at"],
            "created_at": entry["cached_at"],
            "expires_at": entry["expires_at"],
            "input_hash": entry.get("input_hash"),
            "parameter_hash": entry.get("parameter_hash"),
            "entry_count": len(self._dcf_run_cache),
            "invalidation_policy": entry["invalidation_policy"],
        }
        return result

    def _store_dcf_run_cache(
        self,
        cache_key: str,
        result: Dict[str, Any],
        module_cfg: Dict[str, Any],
    ) -> None:
        cache_cfg = self._dcf_cache_config(module_cfg)
        if not cache_cfg.get("enabled", False):
            if isinstance(result, dict):
                result["cache_info"] = {"enabled": False, "cache_hit": False}
            return
        now = datetime.now(timezone.utc).replace(microsecond=0)
        ttl_hours = int(cache_cfg.get("ttl_hours", 24) or 24)
        expires_at_dt = now + timedelta(hours=ttl_hours)
        cache_result = deepcopy(result)
        cache_result["cache_info"] = {
            "enabled": True,
            "cache_hit": False,
            "cache_key": cache_key,
            "cached_at": now.isoformat(),
            "created_at": now.isoformat(),
            "expires_at": expires_at_dt.isoformat(),
            "input_hash": result.get("input_hash"),
            "parameter_hash": result.get("parameter_hash"),
            "entry_count": len(self._dcf_run_cache) + (0 if cache_key in self._dcf_run_cache else 1),
            "invalidation_policy": "financial_bundle_hash/latest_close/overrides_identity_change",
        }
        result["cache_info"] = deepcopy(cache_result["cache_info"])
        self._dcf_run_cache[cache_key] = {
            "created_at": now.isoformat(),
            "cached_at": now.isoformat(),
            "expires_at": expires_at_dt.isoformat(),
            "expires_at_dt": expires_at_dt,
            "input_hash": result.get("input_hash"),
            "parameter_hash": result.get("parameter_hash"),
            "invalidation_policy": cache_result["cache_info"]["invalidation_policy"],
            "summary": self._dcf_run_cache_summary(cache_result),
            "result": cache_result,
        }
        self._trim_dcf_run_cache(cache_cfg)

    def _dcf_run_cache_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "instrument_id": result.get("instrument_id"),
            "model_profile": result.get("model_profile"),
            "status": result.get("status"),
            "valuation_date": result.get("valuation_date"),
            "input_hash": result.get("input_hash"),
            "parameter_hash": result.get("parameter_hash"),
            "assumption_snapshot": result.get("assumptions"),
            "forecast_rows": result.get("forecast_rows", []),
            "sensitivity": result.get("sensitivity", []),
            "workbook": result.get("workbook"),
        }

    def _trim_dcf_run_cache(self, cache_cfg: Dict[str, Any]) -> None:
        max_entries = int(cache_cfg.get("max_entries", 128) or 128)
        while len(self._dcf_run_cache) > max_entries:
            oldest_key = min(
                self._dcf_run_cache,
                key=lambda key: self._dcf_run_cache[key]["created_at"],
            )
            self._dcf_run_cache.pop(oldest_key, None)

    @staticmethod
    def _dcf_cache_config(module_cfg: Dict[str, Any]) -> Dict[str, Any]:
        dcf_cfg = module_cfg.get("dcf", {}) if isinstance(module_cfg, dict) else {}
        professional = dcf_cfg.get("professional", {}) if isinstance(dcf_cfg, dict) else {}
        cache_cfg = professional.get("bounded_cache") or dcf_cfg.get("bounded_cache") or {}
        return cache_cfg if isinstance(cache_cfg, dict) else {}

    @staticmethod
    def _stable_hash(payload: Any) -> str:
        return hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]

    async def get_research_dcf_model_profiles(self) -> Dict[str, Any]:
        """读取专业 DCF 模型 profile 注册表。"""
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        from research.professional_dcf import ProfessionalDcfEngine

        engine = ProfessionalDcfEngine(
            module_cfg.get("dcf", {}).get("professional")
            or module_cfg.get("professional_dcf")
            or module_cfg.get("dcf")
        )
        return {"model_profiles": engine.list_model_profiles()}

    async def get_research_dcf_input_gaps(
        self,
        instrument_id: str,
        *,
        model_profile: str = "nonfinancial_fcff.v1",
    ) -> Optional[Dict[str, Any]]:
        """读取专业 DCF 输入缺口。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None
        financial_bundle = await asyncio.to_thread(
            self._run_financial_storage_call,
            storage,
            "get_financial_statement_bundle",
            normalized_id,
            include_statements=False,
        )
        if financial_bundle is None:
            financial_bundle = {"instrument_id": normalized_id}

        from research.professional_dcf import ProfessionalDcfEngine

        engine = ProfessionalDcfEngine(
            module_cfg.get("dcf", {}).get("professional")
            or module_cfg.get("professional_dcf")
            or module_cfg.get("dcf")
        )
        return engine.build_input_gaps(
            instrument=instrument,
            financial_bundle=financial_bundle,
            model_profile=model_profile,
        )

    async def get_research_dcf_readiness(
        self,
        instrument_id: str,
    ) -> Optional[Dict[str, Any]]:
        """读取单公司专业 DCF profile readiness。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("valuation", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research valuation module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None
        financial_bundle = await asyncio.to_thread(
            self._run_financial_storage_call,
            storage,
            "get_financial_statement_bundle",
            normalized_id,
            include_statements=False,
        )
        if financial_bundle is None:
            financial_bundle = {"instrument_id": normalized_id}

        from research.professional_dcf import ProfessionalDcfEngine

        engine = ProfessionalDcfEngine(
            module_cfg.get("dcf", {}).get("professional")
            or module_cfg.get("professional_dcf")
            or module_cfg.get("dcf")
        )
        profile_rows = []
        for profile in engine.list_model_profiles():
            profile_name = profile["model_profile"]
            gaps = engine.build_input_gaps(
                instrument=instrument,
                financial_bundle=financial_bundle,
                model_profile=profile_name,
            )
            implementation_status = profile.get("implementation_status", "unknown")
            blockers = [f"missing_{item['field']}" for item in gaps["missing_fields"]]
            if implementation_status != "implemented":
                blockers.append("model_profile_not_implemented")
            profile_rows.append(
                {
                    "model_profile": profile_name,
                    "implementation_status": implementation_status,
                    "supported_company_types": profile.get("supported_company_types", []),
                    "ready": gaps["ready"] and implementation_status == "implemented",
                    "missing_fields": gaps["missing_fields"],
                    "blockers": blockers,
                    "warnings": [] if implementation_status == "implemented" else ["guardrail_only"],
                }
            )

        return {
            "instrument_id": instrument.get("instrument_id", normalized_id),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "ready": any(item["ready"] for item in profile_rows),
            "profiles": profile_rows,
            "coverage_diagnostics": {
                "profile_count": len(profile_rows),
                "implemented_profile_count": sum(
                    1 for item in profile_rows if item["implementation_status"] == "implemented"
                ),
                "ready_profile_count": sum(1 for item in profile_rows if item["ready"]),
            },
        }

    async def get_research_analyst_coverage(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 analyst coverage。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("analyst_forecasts", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research analyst_forecasts module is disabled")

        normalized_id = convert_to_database_format(instrument_id)

        from research.query_service import ResearchQueryService

        query_service = ResearchQueryService(storage)
        forecast = await asyncio.to_thread(
            query_service.get_latest_analyst_forecast,
            normalized_id,
            include_details=include_details,
        )
        if forecast is None:
            instrument = await self._get_research_instrument_info(normalized_id)
            if instrument and self._module_allows_optional_empty_exchange(
                "analyst_forecasts",
                instrument.get("exchange"),
            ):
                return self._build_empty_analyst_coverage_response(
                    instrument,
                    include_details=include_details,
                )
            return None

        forecast["status"] = "success"
        forecast["missing_reason"] = None
        return forecast

    async def get_research_reports(
        self,
        instrument_id: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 20,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 research reports。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("research_reports", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research research_reports module is disabled")

        normalized_id = convert_to_database_format(instrument_id)

        from research.query_service import ResearchQueryService

        query_service = ResearchQueryService(storage)
        rows = await asyncio.to_thread(
            query_service.list_research_reports,
            normalized_id,
            start_date=None if start_date is None else start_date.isoformat(),
            end_date=None if end_date is None else end_date.isoformat(),
            limit=limit,
            include_details=include_details,
        )
        if not rows:
            instrument = await self._get_research_instrument_info(normalized_id)
            if instrument and self._module_allows_optional_empty_exchange(
                "research_reports",
                instrument.get("exchange"),
            ):
                return self._build_empty_reports_response(instrument)
            return None

        return {
            "instrument_id": normalized_id,
            "symbol": rows[0].get("symbol"),
            "exchange": rows[0].get("exchange"),
            "data_points": len(rows),
            "window_start": rows[-1].get("publish_date"),
            "window_end": rows[0].get("publish_date"),
            "items": rows,
        }

    async def get_research_sentiment_events(
        self,
        instrument_id: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        event_types: Optional[List[str]] = None,
        limit: int = 50,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 sentiment events。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("sentiment_events", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research sentiment_events module is disabled")

        normalized_id = convert_to_database_format(instrument_id)

        from research.query_service import ResearchQueryService

        query_service = ResearchQueryService(storage)
        rows = await asyncio.to_thread(
            query_service.list_sentiment_events,
            normalized_id,
            start_date=None if start_date is None else start_date.isoformat(),
            end_date=None if end_date is None else end_date.isoformat(),
            event_types=event_types,
            limit=limit,
            include_details=include_details,
        )
        if not rows:
            instrument = await self._get_research_instrument_info(normalized_id)
            if instrument and self._module_allows_optional_empty_exchange(
                "sentiment_events",
                instrument.get("exchange"),
            ):
                return self._build_empty_sentiment_events_response(instrument)
            return None

        return {
            "instrument_id": normalized_id,
            "symbol": rows[0].get("symbol"),
            "exchange": rows[0].get("exchange"),
            "data_points": len(rows),
            "window_start": rows[-1].get("event_date"),
            "window_end": rows[0].get("event_date"),
            "items": rows,
        }

    async def get_research_risk(
        self,
        instrument_id: str,
        *,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 risk snapshot。"""
        storage = self._require_research_storage()
        module_cfg = self.research_config.modules.get("risk", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research risk module is disabled")

        normalized_id = convert_to_database_format(instrument_id)

        from research.query_service import ResearchQueryService
        from research.risk_service import ResearchRiskService

        query_service = ResearchQueryService(storage)
        snapshot = await asyncio.to_thread(
            query_service.get_latest_risk_snapshot,
            normalized_id,
            include_details=include_details,
        )
        if snapshot is None:
            return None

        risk_service = ResearchRiskService(module_cfg)
        return risk_service.build_response(snapshot)

    async def get_research_beta(
        self,
        instrument_id: str,
        *,
        benchmark_family: str = "market_default",
        benchmark_instrument_id: Optional[str] = None,
        window_days: Optional[int] = None,
        as_of_date: Optional[date] = None,
        include_details: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """按需实时计算 benchmark-aware beta。"""
        module_cfg = self.research_config.modules.get("beta", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research beta module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None

        windows = (
            [int(window_days)]
            if window_days is not None
            else [int(item) for item in module_cfg.get("windows", [60, 120, 252])]
        )
        if not windows or any(item < 2 for item in windows):
            raise ValueError("window_days must be at least 2")

        stock_adjustment = self._normalize_research_adjustment(
            str(module_cfg.get("stock_adjustment", "qfq"))
        )
        benchmark_adjustment = str(module_cfg.get("benchmark_adjustment", "none"))
        quote_limit_days = max(
            int(module_cfg.get("quote_limit_days", 0) or 0),
            max(windows) + 40,
        )
        end_datetime = None
        if as_of_date is not None:
            end_datetime = datetime(
                as_of_date.year,
                as_of_date.month,
                as_of_date.day,
                23,
                59,
                59,
            )

        raw_stock_quotes = await self.db_ops.get_daily_data(
            instrument_id=normalized_id,
            end_date=end_datetime,
            limit=quote_limit_days,
            return_format="pandas",
        )
        stock_quotes = raw_stock_quotes
        applied_stock_adjustment = "none"
        if raw_stock_quotes is not None and not raw_stock_quotes.empty:
            stock_quotes, applied_stock_adjustment = await self._apply_research_adjustment(
                raw_stock_quotes,
                normalized_id,
                instrument,
                stock_adjustment,
            )

        benchmarks = await self._resolve_research_beta_benchmarks(
            instrument,
            benchmark_family=benchmark_family,
            benchmark_instrument_id=benchmark_instrument_id,
        )
        if not benchmarks:
            benchmarks = [
                {
                    "benchmark_family": benchmark_family or "custom",
                    "benchmark_instrument_id": benchmark_instrument_id or "",
                    "benchmark_name": None,
                    "selection_rule": "benchmark_not_resolved",
                    "as_of_date": None if as_of_date is None else as_of_date.isoformat(),
                }
            ]

        from research.beta_service import ResearchBetaService

        service = ResearchBetaService(module_cfg)
        items: List[Dict[str, Any]] = []
        benchmark_cache: Dict[str, Optional[pd.DataFrame]] = {}
        for benchmark in benchmarks:
            benchmark_quotes = await self._get_research_beta_benchmark_quotes(
                benchmark,
                quote_limit_days=quote_limit_days,
                end_date=end_datetime,
                cache=benchmark_cache,
            )
            for result in service.build_results(
                stock_quotes=stock_quotes,
                benchmark_quotes=benchmark_quotes,
                instrument=instrument,
                benchmark=benchmark,
                windows=windows,
                stock_adjustment=applied_stock_adjustment,
                benchmark_adjustment=benchmark_adjustment,
            ):
                items.append(
                    self._research_beta_result_to_dict(
                        result,
                        include_details=include_details,
                    )
                )
        self._attach_research_beta_window_stability_flags(items)

        return {
            "instrument_id": normalized_id,
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "benchmark_family": benchmark_family,
            "benchmark_instrument_id": benchmark_instrument_id,
            "window_days": window_days,
            "windows": windows,
            "as_of_date": None if as_of_date is None else as_of_date.isoformat(),
            "data_points": len(items),
            "items": items,
        }

    async def _resolve_research_beta_benchmarks(
        self,
        instrument: Dict[str, Any],
        *,
        benchmark_family: str,
        benchmark_instrument_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        family = (benchmark_family or "market_default").strip()
        if benchmark_instrument_id:
            return [
                {
                    "benchmark_family": "custom" if family == "all" else family,
                    "benchmark_instrument_id": benchmark_instrument_id,
                    "benchmark_name": None,
                    "selection_rule": "explicit_benchmark_instrument_id",
                }
            ]

        beta_cfg = self.research_config.modules.get("beta", {})
        if family == "market_default":
            board = self._resolve_research_beta_board_benchmark(instrument)
            if board:
                return [
                    {
                        **board,
                        "benchmark_family": "market_default",
                        "selection_rule": f"market_default_from_{board.get('selection_rule')}",
                    }
                ]
            broad = beta_cfg.get("benchmarks", {}).get("market_broad", [])
            if broad:
                item = broad[0]
                return [
                    {
                        "benchmark_family": "market_default",
                        "benchmark_instrument_id": item.get("instrument_id"),
                        "benchmark_name": item.get("name"),
                        "selection_rule": "fallback_first_market_broad",
                    }
                ]
        if family == "market_broad":
            return [
                {
                    "benchmark_family": "market_broad",
                    "benchmark_instrument_id": item.get("instrument_id"),
                    "benchmark_name": item.get("name"),
                    "selection_rule": "configured_market_broad",
                }
                for item in beta_cfg.get("benchmarks", {}).get("market_broad", [])
                if item.get("instrument_id")
            ]
        if family == "board":
            board = self._resolve_research_beta_board_benchmark(instrument)
            return [] if board is None else [board]
        if family == "industry_sw_l2":
            return [await self._resolve_research_beta_industry_benchmark(instrument)]
        if family == "all":
            candidates: List[Dict[str, Any]] = []
            for target_family in ["market_default", "board", "market_broad", "industry_sw_l2"]:
                candidates.extend(
                    await self._resolve_research_beta_benchmarks(
                        instrument,
                        benchmark_family=target_family,
                        benchmark_instrument_id=None,
                    )
                )
            return self._dedupe_research_beta_benchmarks(candidates)
        if family == "custom":
            raise ValueError("benchmark_instrument_id is required when benchmark_family=custom")
        raise ValueError(
            "benchmark_family must be one of market_default, market_broad, board, industry_sw_l2, custom, all"
        )

    @staticmethod
    def _dedupe_research_beta_benchmarks(
        benchmarks: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for benchmark in benchmarks:
            benchmark_id = str(benchmark.get("benchmark_instrument_id") or "")
            key = benchmark_id or (
                f"{benchmark.get('benchmark_family')}:{benchmark.get('selection_rule')}"
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(benchmark)
        return deduped

    def _resolve_research_beta_board_benchmark(
        self,
        instrument: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        beta_cfg = self.research_config.modules.get("beta", {})
        symbol = str(instrument.get("symbol") or "")
        exchange = str(instrument.get("exchange") or "")
        for rule in beta_cfg.get("board_benchmark_rules", []):
            exchanges = rule.get("exchanges") or []
            prefixes = rule.get("symbol_prefixes") or []
            if exchanges and exchange not in exchanges:
                continue
            if prefixes and not any(symbol.startswith(str(prefix)) for prefix in prefixes):
                continue
            return {
                "benchmark_family": "board",
                "benchmark_instrument_id": rule.get("benchmark_instrument_id"),
                "benchmark_name": rule.get("benchmark_name"),
                "selection_rule": rule.get("name", "configured_board_rule"),
            }
        return None

    async def _resolve_research_beta_industry_benchmark(
        self,
        instrument: Dict[str, Any],
    ) -> Dict[str, Any]:
        if self.research_storage is None:
            return {
                "benchmark_family": "industry_sw_l2",
                "benchmark_instrument_id": "",
                "benchmark_name": None,
                "selection_rule": "research_storage_required_for_industry_beta",
            }
        membership = await asyncio.to_thread(
            self.research_storage.get_industry_membership,
            instrument["instrument_id"],
            include_snapshot=False,
        )
        if not membership or membership.get("mapping_status") != "authoritative":
            return {
                "benchmark_family": "industry_sw_l2",
                "benchmark_instrument_id": "",
                "benchmark_name": None,
                "selection_rule": "authoritative_sw_l2_membership_required",
            }
        sw_index_code = membership.get("sw_l2_index_code")
        sw_l2_name = membership.get("sw_l2_name")
        if not sw_index_code and sw_l2_name:
            sw_index_code = await asyncio.to_thread(
                self._lookup_research_beta_industry_index_code_by_name,
                sw_l2_name,
            )
        if not sw_index_code:
            return {
                "benchmark_family": "industry_sw_l2",
                "benchmark_instrument_id": "",
                "benchmark_name": sw_l2_name,
                "selection_rule": "industry_benchmark_index_code_not_available",
            }
        return {
            "benchmark_family": "industry_sw_l2",
            "benchmark_instrument_id": str(sw_index_code),
            "benchmark_name": sw_l2_name,
            "selection_rule": "authoritative_sw_l2_membership",
            "taxonomy_system": membership.get("taxonomy_system", "sw"),
            "taxonomy_version": membership.get("taxonomy_version", "sw_2021"),
        }

    def _lookup_research_beta_industry_index_code_by_name(
        self,
        sw_l2_name: str,
    ) -> Optional[str]:
        if self.research_storage is None:
            return None
        normalized = str(sw_l2_name or "").replace("Ⅱ", "").replace("II", "").strip()
        if not normalized:
            return None
        with self.research_storage.get_connection() as conn:
            self.research_storage._apply_pragmas(conn)
            row = conn.execute(
                """
                SELECT sw_index_code
                FROM industry_index_analysis_daily
                WHERE sw_index_name = ?
                   OR REPLACE(REPLACE(sw_index_name, 'Ⅱ', ''), 'II', '') = ?
                GROUP BY sw_index_code
                ORDER BY COUNT(*) DESC
                LIMIT 1
                """,
                (sw_l2_name, normalized),
            ).fetchone()
        return None if row is None else str(row["sw_index_code"])

    async def _get_research_beta_benchmark_quotes(
        self,
        benchmark: Dict[str, Any],
        *,
        quote_limit_days: int,
        end_date: Optional[datetime],
        cache: Dict[str, Optional[pd.DataFrame]],
    ) -> Optional[pd.DataFrame]:
        benchmark_id = benchmark.get("benchmark_instrument_id")
        if not benchmark_id:
            return None
        family = benchmark.get("benchmark_family")
        cache_key = f"{family}:{benchmark_id}:{quote_limit_days}:{end_date}"
        if cache_key in cache:
            return cache[cache_key]
        if family == "industry_sw_l2":
            if self.research_storage is None:
                cache[cache_key] = None
                return None
            end_date_text = None if end_date is None else end_date.date().isoformat()
            rows = await asyncio.to_thread(
                self.research_storage.list_industry_index_analysis_daily,
                taxonomy_system=str(benchmark.get("taxonomy_system", "sw")),
                taxonomy_version=str(benchmark.get("taxonomy_version", "sw_2021")),
                sw_index_code=str(benchmark_id),
                end_date=end_date_text,
                limit=quote_limit_days,
                include_payload=False,
            )
            if not rows:
                cache[cache_key] = None
                return None
            frame = pd.DataFrame(
                [
                    {
                        "time": item.get("trade_date"),
                        "close": item.get("close_index"),
                    }
                    for item in rows
                ]
            )
            cache[cache_key] = frame
            return frame

        frame = await self.db_ops.get_daily_data(
            instrument_id=str(benchmark_id),
            end_date=end_date,
            limit=quote_limit_days,
            return_format="pandas",
        )
        cache[cache_key] = frame
        return frame

    @staticmethod
    def _research_beta_result_to_dict(
        result: Any,
        *,
        include_details: bool,
    ) -> Dict[str, Any]:
        data = dict(result.__dict__)
        details = data.pop("details_json", {})
        if include_details:
            data["diagnostics"] = details
        return data

    @staticmethod
    def _attach_research_beta_window_stability_flags(
        items: List[Dict[str, Any]],
    ) -> None:
        grouped: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for item in items:
            if item.get("status") != "success" or item.get("beta") is None:
                continue
            key = (
                str(item.get("benchmark_family") or ""),
                str(item.get("benchmark_instrument_id") or ""),
            )
            grouped.setdefault(key, []).append(item)

        for group_items in grouped.values():
            if len(group_items) < 2:
                continue
            betas = [float(item["beta"]) for item in group_items]
            beta_range = max(betas) - min(betas)
            if beta_range < 0.4:
                continue
            for item in group_items:
                flags = list(item.get("interpretation_flags") or [])
                if "unstable_across_windows" not in flags:
                    flags.append("unstable_across_windows")
                item["interpretation_flags"] = flags
                diagnostics = item.get("diagnostics")
                if isinstance(diagnostics, dict):
                    diagnostics["beta_range_across_returned_windows"] = beta_range

    async def get_research_company_overview(
        self,
        instrument_id: str,
        *,
        include_profile_snapshot: bool = False,
        include_industry_snapshot: bool = False,
        include_financial_snapshot: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 company overview。"""
        storage = self._require_research_storage()
        normalized_id = convert_to_database_format(instrument_id)

        from research.query_service import ResearchQueryService

        service = ResearchQueryService(storage)
        overview = await asyncio.to_thread(
            service.get_company_overview,
            normalized_id,
            include_profile_snapshot=include_profile_snapshot,
            include_industry_snapshot=include_industry_snapshot,
            include_financial_snapshot=include_financial_snapshot,
        )
        if overview is not None:
            return overview

        instrument = await self._get_research_instrument_info(normalized_id)
        if instrument and (
            self._module_allows_optional_empty_exchange(
                "company_profile",
                instrument.get("exchange"),
            )
            or self._module_allows_optional_empty_exchange(
                "industry",
                instrument.get("exchange"),
            )
            or self._module_allows_optional_empty_exchange(
                "financial_summary",
                instrument.get("exchange"),
            )
        ):
            return self._build_empty_company_overview_response(
                instrument,
                include_profile_snapshot=include_profile_snapshot,
                include_industry_snapshot=include_industry_snapshot,
                include_financial_snapshot=include_financial_snapshot,
            )
        return None

    async def get_research_technical_summary(
        self,
        instrument_id: str,
        *,
        adjust: str = "qfq",
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 technical summary。"""
        if not self.research_config.enabled:
            raise RuntimeError("research_config.enabled is false")

        technical_config = self.research_config.modules.get("technical", {})
        if not technical_config.get("enabled", False):
            raise RuntimeError("research technical module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None

        requested_adjustment = self._normalize_research_adjustment(
            adjust or technical_config.get("default_adjustment", "qfq")
        )
        summary_config = technical_config.get("summary", {})
        lookback_bars = int(summary_config.get("lookback_bars", 180))

        quotes = await self.db_ops.get_daily_data(
            instrument_id=normalized_id,
            limit=lookback_bars,
            return_format="pandas",
        )
        if quotes is None or quotes.empty:
            return None

        processed_quotes, applied_adjustment = await self._apply_research_adjustment(
            quotes,
            normalized_id,
            instrument,
            requested_adjustment,
        )

        from research.technical_service import ResearchTechnicalAnalysisService

        service = ResearchTechnicalAnalysisService(summary_config)
        return service.build_summary(
            processed_quotes,
            instrument,
            requested_adjustment=requested_adjustment,
            applied_adjustment=applied_adjustment,
        )

    async def get_research_technical_indicators(
        self,
        instrument_id: str,
        *,
        adjust: str = "qfq",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 120,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域 technical indicators 时间序列。"""
        if not self.research_config.enabled:
            raise RuntimeError("research_config.enabled is false")

        technical_config = self.research_config.modules.get("technical", {})
        if not technical_config.get("enabled", False):
            raise RuntimeError("research technical module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        instrument = await self.db_ops.get_instrument_by_id(normalized_id)
        if not instrument:
            return None

        requested_adjustment = self._normalize_research_adjustment(
            adjust or technical_config.get("default_adjustment", "qfq")
        )
        summary_config = technical_config.get("summary", {})
        lookback_bars = int(summary_config.get("lookback_bars", 180))

        query_limit = limit if start_date or end_date else max(limit, lookback_bars)
        quotes = await self.db_ops.get_daily_data(
            instrument_id=normalized_id,
            start_date=start_date,
            end_date=end_date,
            limit=query_limit,
            return_format="pandas",
        )
        if quotes is None or quotes.empty:
            return None

        processed_quotes, applied_adjustment = await self._apply_research_adjustment(
            quotes,
            normalized_id,
            instrument,
            requested_adjustment,
        )

        from research.technical_service import ResearchTechnicalAnalysisService

        service = ResearchTechnicalAnalysisService(summary_config)
        return service.build_indicator_series(
            processed_quotes,
            instrument,
            requested_adjustment=requested_adjustment,
            applied_adjustment=applied_adjustment,
            limit=limit,
        )

    async def _apply_research_adjustment(
        self,
        quotes: pd.DataFrame,
        instrument_id: str,
        instrument: Dict[str, Any],
        adjustment: str,
    ) -> Tuple[pd.DataFrame, str]:
        """Apply the requested adjustment for research technical calculations."""
        from utils.adjustment import AdjustmentEngine

        records = quotes.to_dict("records")
        instrument_type = str(instrument.get("type", "")).lower()

        if adjustment in {"qfq", "hfq"} and instrument_type == "stock":
            factors = await self.get_cached_adjustment_factors(instrument_id)
            if factors:
                adjusted_records = AdjustmentEngine.apply_adjustment(
                    records,
                    factors,
                    adjustment,
                )
                return pd.DataFrame(adjusted_records), adjustment

        return pd.DataFrame(AdjustmentEngine.no_adjust(records)), "none"

    @staticmethod
    def _normalize_research_adjustment(adjustment: str) -> str:
        normalized = (adjustment or "qfq").lower().strip()
        if normalized in {"qfq", "forward"}:
            return "qfq"
        if normalized in {"hfq", "backward"}:
            return "hfq"
        if normalized == "none":
            return "none"
        raise ValueError("adjust must be one of qfq, hfq, none")

    async def close(self) -> None:
        """安全关闭所有数据源连接，防止协程退出时 ResourceWarning"""
        if hasattr(self, 'source_factory') and self.source_factory:
            try:
                await self.source_factory.close_all()
                dm_logger.info("[DataManager] All data sources closed safely.")
            except Exception as e:
                dm_logger.error(f"[DataManager] Error closing data sources: {e}")

        try:
            db = getattr(self.db_ops, "db", None)
            if db is not None and hasattr(db, "close_async"):
                await db.close_async()
        except Exception as e:
            dm_logger.error(f"[DataManager] Error closing database connections: {e}")

    @log_execution("DataManager", "download_all_historical_data")
    async def download_all_historical_data(self, exchanges: Optional[List[str]] = None,
                                         start_date: Optional[date] = None, end_date: Optional[date] = None,
                                         resume: bool = True,
                                         quality_threshold: float = 0.7,
                                         force_update_calendar: bool = True,
                                         instrument_types: Optional[List[str]] = None) -> None:
        """下载所有历史数据

        Args:
            exchanges: 交易所列表
            start_date: 开始日期（如果为None，使用每个股票的上市日期）
            end_date: 结束日期（如果为None，使用昨天）
            resume: 是否续传
            quality_threshold: 数据质量阈值
            instrument_types: 品种类型列表, 如 ['stock', 'index', 'etf']
                              为 None 时从配置 data_config.instrument_types 读取
        """
        if self.is_running:
            dm_logger.warning("download already in progress")
            return

        self.is_running = True

        # 从配置读取默认品种类型
        if instrument_types is None:
            instrument_types = self.data_config.get('instrument_types', ['stock'])
        dm_logger.info(f"Instrument types to download: {instrument_types}")

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
                exchanges = ['SSE', 'SZSE', 'BSE']
                dm_logger.info(f"Using default exchanges: {exchanges}")

            # 获取所有交易品种并计算总数
            all_instruments = {}
            if not (resume and has_existing_progress):
                self.progress.total_instruments = 0
                for exchange in exchanges:
                    instruments = await self.db_ops.get_instruments_by_exchange(exchange)
                    # 按品种类型过滤
                    if instrument_types:
                        instruments = [
                            inst for inst in instruments
                            if inst.get('type') in instrument_types
                        ]
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
                        instruments = await self.db_ops.get_instruments_by_exchange(exchange)
                        if instrument_types:
                            instruments = [
                                inst for inst in instruments
                                if inst.get('type') in instrument_types
                            ]
                        all_instruments[exchange] = instruments

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

            # 不再使用基于全市场总数的粗糙批次跳过，而是传入 resume 给批次处理，进行逐个品种的精确跳过
            start_batch = 1
            self.progress.total_batches = len(batches)
            
            if resume:
                dm_logger.info(f"Resume mode: {exchange} - will check each instrument individually")
            else:
                dm_logger.info(f"Fresh download mode: {exchange} - downloading all from scratch")

            dm_logger.info(f"Processing {len(batches)} batches for {exchange}")

            for batch_idx, batch in enumerate(batches, 1):

                with LogContext("DataManager", "process_precise_batch",
                               exchange=exchange, batch_idx=batch_idx):

                    self.progress.current_batch = batch_idx
                    dm_logger.info(f"Processing precise batch {batch_idx}/{len(batches)}")

                    await self._download_batch_precise(
                        batch, exchange, start_date, end_date, quality_threshold, resume
                    )

                    # 批次间延迟
                    if batch_idx < len(batches):
                        await asyncio.sleep(2.0)

        except Exception as e:
            dm_logger.error(f"Failed to download precise data: {e}")
            self.progress.add_error(f"Exchange {exchange}: {str(e)}")

    async def _download_batch_precise(self, instruments: List[Dict], exchange: str,
                                            start_date: date, end_date: date,
                                            quality_threshold: float, resume: bool = False):
        """标准精确批次下载"""
        batch_data = []
        batch_quality_scores = []
        skipped_in_loop = 0  # 在循环内已累加 processed_instruments 的数量

        for instrument in instruments:
            try:
                instrument_start = None
                
                # ====== 精确断点续传检测 ======
                if resume:
                    last_update = await self.db_ops.get_latest_quote_date(
                        instrument['instrument_id']
                    )
                    if last_update:
                        if isinstance(last_update, datetime):
                            last_update = last_update.date()
                            
                        if last_update >= end_date:
                            dm_logger.debug(f"Skipping {instrument['instrument_id']}, already updated to {last_update}")
                            self.progress.processed_instruments += 1
                            skipped_in_loop += 1
                            continue
                        else:
                            # 从最后更新日期的下一天开始下载
                            dm_logger.debug(f"Resuming {instrument['instrument_id']} from {last_update + timedelta(days=1)}")
                            instrument_start = last_update + timedelta(days=1)
                
                if instrument_start is None:
                    # 获取品种的上市日期（指数/ETF 可能无 listed_date）
                    listed_date = instrument.get('listed_date')
                    instrument_start = listed_date if listed_date else start_date
                
                # 兜底：如果仍然无起始日期，使用配置的默认起始年份
                if instrument_start is None:
                    default_year = self.data_config.get('default_start_years', {}).get(exchange, 1990)
                    instrument_start = date(default_year, 1, 1)

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

                # 批量同步复权因子（仅对股票类型品种）
                stocks_needing_factors = [
                    {
                        'instrument_id': inst['instrument_id'],
                        'symbol': inst['symbol'],
                        'start_date': start_date or date(1990, 1, 1),
                        'end_date': end_date,
                    }
                    for inst in instruments
                    if inst.get('type', 'stock') == 'stock'
                ]
                if stocks_needing_factors:
                    await self._batch_sync_adjustment_factors(
                        exchange,
                        stocks_needing_factors,
                        skip_filter=True,
                        sync_reason='daily',
                    )
            else:
                self.progress.failed_downloads += len(instruments)
                self.progress.add_error(f"Failed to save precise batch for {exchange}")

        # 只累加未在循环内已计数的品种（避免 resume 跳过的被重复计入）
        self.progress.processed_instruments += len(instruments) - skipped_in_loop
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

                    source_name = instrument.get('source', 'Unknown')
                    dm_logger.info(f"一次性下载 {instrument['instrument_id']} 数据 "
                                  f"从 {start_date_for_download.date()} 到 {end_date_for_download.date()} "
                                  f"通过源 [{source_name}] (共 {len(filtered_trading_days)} 个交易日)")

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
                    end_date_for_download,
                    instrument.get('type', 'stock'),
                    source_symbol=instrument.get('source_symbol', '')
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
                    end_date,
                    instrument.get('type', 'stock'),
                    source_symbol=instrument.get('source_symbol', '')
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

            # 3. 复权类型：改造后 DB 存储的是非复权原始数据
            # 复权在 API 查询时由 AdjustmentEngine 动态计算
            if 'adjustment_type' not in quote or quote.get('adjustment_type') is None:
                quote['adjustment_type'] = 'none'

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

    def _get_repair_universe_governance_config(self) -> Dict[str, Any]:
        """Return local lifecycle-filter policy for historical repair/backfill."""
        index_config = self._get_index_master_governance_config()
        defaults: Dict[str, Any] = {
            'enabled': True,
            'default_mode': 'historical_backfill',
            'sample_limit': 10,
            'allow_lifecycle_filter_override': True,
            'max_override_instruments': 50,
            'max_override_limit': 50,
            'current_repair_requires_tradable': True,
            'allow_inactive_pre_lifecycle_history': True,
            'skip_index_lifecycle_states': [
                'calculation_terminated',
                'inactive',
                'stale_no_quote',
            ],
            'enable_local_stale_no_quote': bool(index_config.get('skip_stale_no_quote', True)),
            'stale_no_quote_trading_days': int(
                index_config.get('stale_no_quote_trading_days', 10) or 10
            ),
            'stale_governance_continue_policy': 'warn',
        }
        raw_config = self.data_config.get('repair_universe_governance')
        if not isinstance(raw_config, dict):
            raw_config = self.config.get_nested('data_config.repair_universe_governance', {})
        if isinstance(raw_config, dict):
            defaults.update(raw_config)
        defaults['skip_index_lifecycle_states'] = {
            str(item).lower()
            for item in defaults.get('skip_index_lifecycle_states', [])
            if str(item).strip()
        }
        return defaults

    @staticmethod
    def _normalize_repair_universe_mode(mode: Optional[str], *, dry_run: bool = False) -> str:
        """Normalize repair-universe execution mode names."""
        if dry_run:
            return 'dry_run'
        normalized = str(mode or 'historical_backfill').strip().lower()
        aliases = {
            'current': 'current_repair',
            'repair': 'current_repair',
            'historical': 'historical_backfill',
            'backfill': 'historical_backfill',
            'forensic': 'override',
        }
        normalized = aliases.get(normalized, normalized)
        allowed = {'current_repair', 'historical_backfill', 'dry_run', 'override'}
        if normalized not in allowed:
            raise ValueError(f"Unsupported repair universe mode: {mode}")
        return normalized

    def _build_repair_universe_diagnostics(
        self,
        *,
        mode: str,
        start_date: Optional[date],
        end_date: Optional[date],
        override_lifecycle_filter: bool = False,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Create the structured diagnostics payload used by repair jobs."""
        return {
            'enabled': enabled,
            'mode': mode,
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None,
            'override_lifecycle_filter': bool(override_lifecycle_filter),
            'input_instrument_count': 0,
            'eligible_instrument_count': 0,
            'clipped_instrument_count': 0,
            'skipped_instrument_count': 0,
            'skipped_gap_segment_count': 0,
            'skipped_missing_days': 0,
            'reason_distribution': {},
            'samples': [],
            'warnings': [],
            'errors': [],
            'current_master_refresh': {
                'requested': False,
                'status': 'not_requested',
                'scopes': [],
                'operator_requested': False,
            },
        }

    def _record_repair_universe_skip(
        self,
        diagnostics: Dict[str, Any],
        *,
        reason: str,
        instrument: Optional[Dict[str, Any]] = None,
        gap: Optional[DataGapInfo] = None,
        gap_segments: int = 0,
        missing_days: int = 0,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        detail: Optional[str] = None,
    ) -> None:
        """Append one bounded lifecycle-skip diagnostic."""
        reason = str(reason or 'unknown')
        distribution = Counter(diagnostics.get('reason_distribution') or {})
        distribution[reason] += max(1, int(gap_segments or 1))
        diagnostics['reason_distribution'] = dict(distribution)
        if gap_segments:
            diagnostics['skipped_gap_segment_count'] += int(gap_segments)
        if missing_days:
            diagnostics['skipped_missing_days'] += int(missing_days)

        sample_limit = int(self._get_repair_universe_governance_config().get('sample_limit', 10) or 0)
        if sample_limit <= 0 or len(diagnostics.get('samples') or []) >= sample_limit:
            return

        source = instrument or {}
        sample = {
            'instrument_id': (
                getattr(gap, 'instrument_id', None)
                or source.get('instrument_id')
            ),
            'symbol': getattr(gap, 'symbol', None) or source.get('symbol'),
            'exchange': getattr(gap, 'exchange', None) or source.get('exchange'),
            'type': source.get('type'),
            'status': source.get('status'),
            'reason': reason,
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None,
        }
        if gap is not None:
            sample.update({
                'gap_start': gap.gap_start.isoformat(),
                'gap_end': gap.gap_end.isoformat(),
                'gap_days': gap.gap_days,
            })
        if detail:
            sample['detail'] = detail
        diagnostics.setdefault('samples', []).append(sample)

    def _validate_repair_universe_override(
        self,
        *,
        override_lifecycle_filter: bool,
        instrument_ids: Optional[List[str]] = None,
        limit: Optional[int] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Reject broad lifecycle-filter overrides before quote source calls."""
        if not override_lifecycle_filter:
            return

        config = config or self._get_repair_universe_governance_config()
        if not config.get('allow_lifecycle_filter_override', True):
            raise ValueError("repair universe lifecycle-filter override is disabled by config")

        max_instruments = int(config.get('max_override_instruments', 50) or 50)
        max_limit = int(config.get('max_override_limit', max_instruments) or max_instruments)
        bounded_ids = [item for item in (instrument_ids or []) if str(item).strip()]
        if bounded_ids:
            if len(set(bounded_ids)) > max_instruments:
                raise ValueError(
                    f"repair universe override target set is too broad: "
                    f"{len(set(bounded_ids))}>{max_instruments}"
                )
            return

        if limit is not None and 0 < int(limit) <= max_limit:
            return

        raise ValueError(
            "repair universe lifecycle-filter override requires explicit "
            "instrument_ids or a small repair_universe_limit"
        )

    @staticmethod
    def _instrument_types_for_governance_scopes(
        scopes: Optional[List[str]],
        fallback: List[str],
    ) -> List[str]:
        """Map explicit governance scopes to instrument types for refresh calls."""
        if not scopes:
            return fallback
        mapped: Set[str] = set()
        for scope in scopes:
            normalized = str(scope or '').strip().lower()
            if normalized == 'a_share_index':
                mapped.add('index')
            elif normalized in {'a_share_stock', 'hkex_instrument'}:
                mapped.add('stock')
        return sorted(mapped) or fallback

    async def _run_repair_current_master_refresh(
        self,
        *,
        job_name: str,
        exchanges: List[str],
        instrument_types: List[str],
        target_date: date,
        scopes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Run operator-requested current master refresh for repair workflows."""
        requirements = self._build_master_governance_requirements(
            job_name=job_name,
            exchanges=exchanges,
            instrument_types=instrument_types,
            job_type='historical',
            target_date=target_date,
            force_refresh=True,
        )
        requested_scopes = {
            str(scope).strip()
            for scope in (scopes or [])
            if str(scope).strip()
        }
        if requested_scopes:
            requirements = [
                requirement
                for requirement in requirements
                if requirement.scope in requested_scopes
            ]
        if not requirements:
            started_at = get_shanghai_time()
            return {
                'status': 'skipped',
                'action': 'skipped',
                'reason': 'no_requested_master_governance_scope',
                'job_name': job_name,
                'job_type': 'historical',
                'started_at': started_at.isoformat(),
                'finished_at': get_shanghai_time().isoformat(),
                'elapsed_sec': 0.0,
                'summary': {'exchanges': [], 'active_count': 0},
                'exchanges': {},
                'children': [],
                'warnings': [],
                'errors': [],
            }
        result = await self.run_master_governance(requirements)
        result['job_name'] = job_name
        result['job_type'] = 'historical'
        result['operator_requested_for_historical_repair'] = True
        return result

    async def _load_index_lifecycle_evidence_by_instrument(
        self,
        exchanges: List[str],
        states: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Read latest local index lifecycle evidence, when the DB supports it."""
        if not hasattr(self.db_ops, 'get_index_lifecycle_evidence'):
            return {}
        try:
            rows = await self.db_ops.get_index_lifecycle_evidence(
                exchanges=exchanges,
                states=states,
            )
        except Exception as exc:
            dm_logger.warning("[DataManager] Failed to load index lifecycle evidence: %s", exc)
            return {}

        evidence_by_id: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            instrument_id = row.get('instrument_id')
            if instrument_id and instrument_id not in evidence_by_id:
                evidence_by_id[instrument_id] = row
        return evidence_by_id

    def _resolve_repair_window_for_instrument(
        self,
        instrument: Dict[str, Any],
        *,
        start_date: Optional[date],
        end_date: date,
        mode: str,
        config: Dict[str, Any],
        lifecycle_evidence: Optional[Dict[str, Any]] = None,
        latest_quote_date: Optional[date] = None,
        override_lifecycle_filter: bool = False,
    ) -> Dict[str, Any]:
        """Return lifecycle-aware repair window for one local instrument row."""
        requested_start = start_date
        requested_end = end_date
        if override_lifecycle_filter:
            return {
                'eligible': True,
                'start_date': requested_start,
                'end_date': requested_end,
                'reason': 'override',
                'clipped': False,
            }

        start = requested_start
        end = requested_end
        status = str(instrument.get('status') or '').strip().lower()
        instrument_type = str(instrument.get('type') or '').strip().lower()
        exchange = str(instrument.get('exchange') or '').strip().upper()
        is_active = instrument.get('is_active')
        trading_status = instrument.get('trading_status')
        listed = self._date_from_any(instrument.get('listed_date'))
        delisted = self._date_from_any(instrument.get('delisted_date'))

        if start is None:
            start = listed
        elif listed and start < listed:
            start = listed

        if start is None:
            return {
                'eligible': False,
                'reason': 'missing_lifecycle_start',
                'start_date': None,
                'end_date': end,
                'clipped': False,
            }

        if listed and listed > requested_end:
            return {
                'eligible': False,
                'reason': 'before_listed_date',
                'start_date': start,
                'end_date': end,
                'clipped': True,
            }

        if delisted and end > delisted:
            end = delisted

        index_states = set(config.get('skip_index_lifecycle_states') or set())
        evidence = lifecycle_evidence or {}
        evidence_state = str(evidence.get('lifecycle_state') or '').strip().lower()
        lifecycle_state = evidence_state or status
        index_lifecycle_boundary = (
            self._date_from_any(evidence.get('effective_date'))
            or self._date_from_any(evidence.get('last_quote_date'))
            or delisted
        )

        if instrument_type == 'index' and lifecycle_state in index_states:
            if index_lifecycle_boundary:
                if end > index_lifecycle_boundary:
                    end = index_lifecycle_boundary
            else:
                return {
                    'eligible': False,
                    'reason': f'index_lifecycle_{lifecycle_state}',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                }

        if (
            instrument_type == 'index'
            and exchange in {'SSE', 'SZSE'}
            and lifecycle_state not in index_states
            and config.get('enable_local_stale_no_quote', True)
        ):
            stale_days = int(config.get('stale_no_quote_trading_days', 10) or 10)
            if latest_quote_date is not None and requested_end > latest_quote_date + timedelta(days=stale_days):
                if end > latest_quote_date:
                    end = latest_quote_date
                lifecycle_state = 'stale_no_quote'

        if mode == 'current_repair':
            if is_active in (False, 0, '0'):
                return {
                    'eligible': False,
                    'reason': 'inactive_master_status',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                }
            if (
                config.get('current_repair_requires_tradable', True)
                and trading_status not in (None, 1, True, '1')
            ):
                return {
                    'eligible': False,
                    'reason': 'non_tradable_master_status',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                }

        if mode in {'historical_backfill', 'dry_run'}:
            inactive_without_boundary = (
                is_active in (False, 0, '0')
                and not delisted
                and not index_lifecycle_boundary
                and status not in {'active', 'active_quote'}
            )
            if inactive_without_boundary:
                return {
                    'eligible': False,
                    'reason': 'inactive_without_lifecycle_boundary',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                }

        if start > end:
            if listed and listed > requested_end:
                reason = 'before_listed_date'
            elif delisted and requested_start and requested_start > delisted:
                reason = 'after_delisted_date'
            elif instrument_type == 'index' and lifecycle_state in index_states | {'stale_no_quote'}:
                reason = f'index_lifecycle_{lifecycle_state}'
            else:
                reason = 'outside_lifecycle_window'
            return {
                'eligible': False,
                'reason': reason,
                'start_date': start,
                'end_date': end,
                'clipped': True,
            }

        return {
            'eligible': True,
            'start_date': start,
            'end_date': end,
            'reason': lifecycle_state if lifecycle_state else 'eligible',
            'clipped': start != requested_start or end != requested_end,
        }

    async def filter_repair_universe(
        self,
        instruments: List[Dict[str, Any]],
        *,
        start_date: Optional[date],
        end_date: date,
        mode: Optional[str] = None,
        instrument_ids: Optional[List[str]] = None,
        override_lifecycle_filter: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Filter and clip local repair/backfill instruments by lifecycle state."""
        config = self._get_repair_universe_governance_config()
        normalized_mode = self._normalize_repair_universe_mode(
            mode or config.get('default_mode'),
            dry_run=dry_run,
        )
        override_lifecycle_filter = (
            bool(override_lifecycle_filter) or normalized_mode == 'override'
        )
        self._validate_repair_universe_override(
            override_lifecycle_filter=override_lifecycle_filter,
            instrument_ids=instrument_ids,
            limit=limit,
            config=config,
        )

        diagnostics = self._build_repair_universe_diagnostics(
            mode=normalized_mode,
            start_date=start_date,
            end_date=end_date,
            override_lifecycle_filter=override_lifecycle_filter,
            enabled=bool(config.get('enabled', True)),
        )
        diagnostics['input_instrument_count'] = len(instruments or [])

        if not config.get('enabled', True):
            eligible = [dict(item) for item in (instruments or [])]
            diagnostics['eligible_instrument_count'] = len(eligible)
            return eligible, diagnostics

        if instrument_ids:
            requested_ids = {str(item).strip() for item in instrument_ids if str(item).strip()}
            instruments = [
                item for item in (instruments or [])
                if item.get('instrument_id') in requested_ids
            ]

        if limit is not None and int(limit) > 0:
            instruments = list(instruments or [])[:int(limit)]

        states = list(config.get('skip_index_lifecycle_states') or [])
        exchanges = sorted({
            str(item.get('exchange')).upper()
            for item in (instruments or [])
            if item.get('exchange')
        })
        evidence_by_id = await self._load_index_lifecycle_evidence_by_instrument(
            exchanges,
            states,
        )

        eligible: List[Dict[str, Any]] = []
        stale_checked = 0
        for instrument in instruments or []:
            latest_quote_date = None
            instrument_type = str(instrument.get('type') or '').lower()
            exchange = str(instrument.get('exchange') or '').upper()
            if (
                instrument_type == 'index'
                and exchange in {'SSE', 'SZSE'}
                and config.get('enable_local_stale_no_quote', True)
            ):
                try:
                    latest_quote_date = self._date_from_any(
                        await self.db_ops.get_latest_quote_date(instrument.get('instrument_id'))
                    )
                    stale_checked += 1
                except Exception as exc:
                    diagnostics['warnings'].append(
                        f"latest quote lookup failed for {instrument.get('instrument_id')}: {exc}"
                    )

            window = self._resolve_repair_window_for_instrument(
                instrument,
                start_date=start_date,
                end_date=end_date,
                mode=normalized_mode,
                config=config,
                lifecycle_evidence=evidence_by_id.get(instrument.get('instrument_id')),
                latest_quote_date=latest_quote_date,
                override_lifecycle_filter=override_lifecycle_filter,
            )
            if not window.get('eligible'):
                diagnostics['skipped_instrument_count'] += 1
                self._record_repair_universe_skip(
                    diagnostics,
                    reason=window.get('reason', 'outside_lifecycle_window'),
                    instrument=instrument,
                    start_date=window.get('start_date'),
                    end_date=window.get('end_date'),
                )
                continue

            item = dict(instrument)
            item['_repair_start_date'] = window.get('start_date')
            item['_repair_end_date'] = window.get('end_date')
            item['_repair_universe_reason'] = window.get('reason')
            item['_repair_universe_clipped'] = bool(window.get('clipped'))
            if window.get('clipped'):
                diagnostics['clipped_instrument_count'] += 1
            eligible.append(item)

        if (
            stale_checked == 0
            and any(str(item.get('type') or '').lower() == 'index' for item in instruments or [])
        ):
            diagnostics['warnings'].append(
                'index lifecycle stale-no-quote diagnostics were not evaluated for this universe'
            )

        diagnostics['eligible_instrument_count'] = len(eligible)
        return eligible, diagnostics

    async def _is_gap_lifecycle_eligible(
        self,
        gap: DataGapInfo,
        instrument: Dict[str, Any],
        *,
        mode: str = 'historical_backfill',
        override_lifecycle_filter: bool = False,
    ) -> Tuple[bool, Optional[date], Optional[date], str]:
        """Re-check one persisted gap before source routing."""
        config = self._get_repair_universe_governance_config()
        evidence_by_id = await self._load_index_lifecycle_evidence_by_instrument(
            [gap.exchange],
            list(config.get('skip_index_lifecycle_states') or []),
        )
        latest_quote_date = None
        if (
            str(instrument.get('type') or '').lower() == 'index'
            and str(instrument.get('exchange') or gap.exchange).upper() in {'SSE', 'SZSE'}
            and config.get('enable_local_stale_no_quote', True)
        ):
            try:
                latest_quote_date = self._date_from_any(
                    await self.db_ops.get_latest_quote_date(instrument.get('instrument_id'))
                )
            except Exception:
                latest_quote_date = None

        window = self._resolve_repair_window_for_instrument(
            instrument,
            start_date=gap.gap_start,
            end_date=gap.gap_end,
            mode=self._normalize_repair_universe_mode(mode),
            config=config,
            lifecycle_evidence=evidence_by_id.get(instrument.get('instrument_id')),
            latest_quote_date=latest_quote_date,
            override_lifecycle_filter=override_lifecycle_filter,
        )
        return (
            bool(window.get('eligible')),
            window.get('start_date'),
            window.get('end_date'),
            str(window.get('reason') or 'outside_lifecycle_window'),
        )

    async def detect_data_gaps(
        self,
        exchanges: List[str],
        start_date: date,
        end_date: date,
        *,
        instrument_types: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        repair_universe_mode: Optional[str] = None,
        override_lifecycle_filter: bool = False,
        repair_universe_limit: Optional[int] = None,
        dry_run: bool = False,
        force_current_master_refresh: bool = False,
        current_master_refresh_scopes: Optional[List[str]] = None,
        include_diagnostics: bool = False,
    ) -> Union[List[DataGapInfo], Dict[str, Any]]:
        """检测数据缺口"""
        gaps = []
        mode = self._normalize_repair_universe_mode(repair_universe_mode, dry_run=dry_run)
        merged_diagnostics = self._build_repair_universe_diagnostics(
            mode=mode,
            start_date=start_date,
            end_date=end_date,
            override_lifecycle_filter=override_lifecycle_filter or mode == 'override',
        )
        governance_result: Optional[Dict[str, Any]] = None

        if force_current_master_refresh:
            refresh_types = self._instrument_types_for_governance_scopes(
                current_master_refresh_scopes,
                instrument_types or self.data_config.get('instrument_types', ['stock', 'index']),
            )
            governance_result = await self._run_repair_current_master_refresh(
                job_name='historical_repair_universe',
                exchanges=exchanges,
                instrument_types=refresh_types,
                target_date=end_date,
                scopes=current_master_refresh_scopes,
            )
            scopes = current_master_refresh_scopes or [
                child.get('scope')
                for child in (governance_result.get('children') or [])
                if isinstance(child, dict) and child.get('scope')
            ]
            merged_diagnostics['current_master_refresh'] = {
                'requested': True,
                'status': governance_result.get('status'),
                'scopes': scopes,
                'operator_requested': True,
            }

        for exchange in exchanges:
            try:
                # Repair/backfill needs local inactive rows too, so lifecycle
                # filtering can decide and report rather than silently dropping.
                if hasattr(self.db_ops, 'get_repair_universe_instruments'):
                    instruments = await self.db_ops.get_repair_universe_instruments(
                        exchange,
                        instrument_types=instrument_types,
                    )
                else:
                    instruments = await self.db_ops.get_active_instruments(
                        exchange,
                        instrument_types=instrument_types,
                    )
                filtered_instruments, diagnostics = await self.filter_repair_universe(
                    instruments,
                    start_date=start_date,
                    end_date=end_date,
                    mode=mode,
                    instrument_ids=instrument_ids,
                    override_lifecycle_filter=override_lifecycle_filter,
                    limit=repair_universe_limit,
                    dry_run=dry_run,
                )
                for key in ('input_instrument_count', 'eligible_instrument_count',
                            'clipped_instrument_count', 'skipped_instrument_count',
                            'skipped_gap_segment_count', 'skipped_missing_days'):
                    merged_diagnostics[key] += int(diagnostics.get(key, 0) or 0)
                merged_counter = Counter(merged_diagnostics.get('reason_distribution') or {})
                merged_counter.update(diagnostics.get('reason_distribution') or {})
                merged_diagnostics['reason_distribution'] = dict(merged_counter)
                sample_limit = int(
                    self._get_repair_universe_governance_config().get('sample_limit', 10) or 0
                )
                if sample_limit > 0:
                    merged_diagnostics['samples'].extend(
                        diagnostics.get('samples', [])[
                            : max(0, sample_limit - len(merged_diagnostics.get('samples') or []))
                        ]
                    )
                merged_diagnostics['warnings'].extend(diagnostics.get('warnings') or [])
                merged_diagnostics['errors'].extend(diagnostics.get('errors') or [])

                for instrument in filtered_instruments:
                    instrument_gaps = await self._detect_instrument_gaps(
                        instrument,
                        instrument.get('_repair_start_date') or start_date,
                        instrument.get('_repair_end_date') or end_date,
                    )
                    gaps.extend(instrument_gaps)

            except Exception as e:
                dm_logger.error(f"Failed to detect gaps for {exchange}: {e}")
                merged_diagnostics['errors'].append(f"{exchange}: {e}")

        if include_diagnostics:
            return {
                'gaps': gaps,
                'repair_universe': merged_diagnostics,
                'instrument_master_governance': governance_result,
            }
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

            # 获取股票的退市日期并限制 end_date
            delisted_date = instrument.get('delisted_date')
            if delisted_date:
                delisted_date_val = delisted_date.date() if isinstance(delisted_date, datetime) else delisted_date
                if isinstance(delisted_date_val, str):
                    try:
                        delisted_date_val = datetime.strptime(delisted_date_val[:10], "%Y-%m-%d").date()
                    except ValueError:
                        pass
                
                if isinstance(delisted_date_val, date) and end_date > delisted_date_val:
                    dm_logger.debug(f"End date {end_date} is after delisted date {delisted_date_val}, capping end date.")
                    end_date = delisted_date_val

            if start_date > end_date:
                # 如果上市日期比检测截止日还晚（或退市日比开始日还早），说明这段时间内该品种没有交易
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
                        missing_dates=gap_missing_dates,
                        instrument_type=instrument.get('type'),
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

    def build_gap_skip_key(self, instrument_id: str, gap_start: date, gap_end: date) -> str:
        """构造缺口跳表 key。"""
        return f"{instrument_id}|{gap_start.isoformat()}|{gap_end.isoformat()}"

    def is_gap_skipped(self, skip_set: Set[str], instrument_id: str,
                       gap_start: date, gap_end: date) -> bool:
        """检查指定缺口段是否已在跳表中。"""
        return self.build_gap_skip_key(instrument_id, gap_start, gap_end) in skip_set

    async def load_gap_skip_set(self, min_fail_count: int = 1,
                                ttl_days: int = 30) -> Set[str]:
        """加载近期失败的缺口段，用于避免重复无效修复。"""
        cutoff = (get_shanghai_time() - timedelta(days=ttl_days)).strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        SELECT instrument_id, gap_start, gap_end
        FROM gap_skip_list
        WHERE fail_count >= :min_fail_count
          AND last_attempted > :cutoff
        """
        rows = await self.db_ops.execute_read_query(
            sql,
            {'min_fail_count': min_fail_count, 'cutoff': cutoff}
        )
        return {
            self.build_gap_skip_key(
                row['instrument_id'],
                datetime.strptime(row['gap_start'], '%Y-%m-%d').date(),
                datetime.strptime(row['gap_end'], '%Y-%m-%d').date()
            )
            for row in rows
        }

    async def record_gap_skip(self, instrument_id: str, gap_start: date,
                              gap_end: date, reason: str = 'no_data') -> None:
        """记录失败缺口段，供后续任务短期跳过。"""
        from sqlalchemy.future import select

        start_str = gap_start.isoformat()
        end_str = gap_end.isoformat()
        now = get_shanghai_time()

        try:
            async with self.db_ops.get_async_session() as session:
                stmt = select(GapSkipDB).filter(
                    GapSkipDB.instrument_id == instrument_id,
                    GapSkipDB.gap_start == start_str,
                    GapSkipDB.gap_end == end_str
                )
                result = await session.execute(stmt)
                existing = result.scalar_one_or_none()

                if existing:
                    existing.fail_count += 1
                    existing.reason = reason
                    existing.last_attempted = now
                else:
                    session.add(GapSkipDB(
                        instrument_id=instrument_id,
                        gap_start=start_str,
                        gap_end=end_str,
                        fail_count=1,
                        reason=reason,
                        last_attempted=now,
                        created_at=now
                    ))

                await session.commit()
        except Exception as e:
            dm_logger.warning(
                "[DataManager] Failed to record gap skip %s %s~%s: %s",
                instrument_id,
                start_str,
                end_str,
                e
            )

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
        dry_run: bool = False,
        repair_universe_mode: Optional[str] = None,
        override_lifecycle_filter: bool = False,
        repair_universe_limit: Optional[int] = None,
        force_current_master_refresh: bool = False,
    ):
        """填补数据缺口"""
        try:
            dm_logger.info("Starting data gap filling process...")

            if end_date is None:
                end_date = date.today()

            exchanges = [exchange] if exchange else ['SSE', 'SZSE', 'BSE']

            # 获取需要填补的缺口
            gap_result = await self.detect_data_gaps(
                exchanges,
                start_date,
                end_date,
                instrument_ids=instrument_ids,
                repair_universe_mode=repair_universe_mode,
                override_lifecycle_filter=override_lifecycle_filter,
                repair_universe_limit=repair_universe_limit,
                dry_run=dry_run,
                force_current_master_refresh=force_current_master_refresh,
                include_diagnostics=True,
            )
            gaps = gap_result['gaps']

            # 过滤严重程度
            if severity_filter:
                gaps = [g for g in gaps if g.severity in severity_filter]
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

            return {
                'status': 'success',
                'dry_run': dry_run,
                'gaps_detected': len(gap_result['gaps']),
                'gaps_selected': len(gaps),
                'filled_count': filled_count,
                'repair_universe': gap_result.get('repair_universe', {}),
                'instrument_master_governance': gap_result.get('instrument_master_governance'),
            }

        except Exception as e:
            dm_logger.error(f"Data gap filling failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'dry_run': dry_run,
                'gaps_detected': 0,
                'gaps_selected': 0,
                'filled_count': 0,
            }

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

            eligible, clipped_start, clipped_end, lifecycle_reason = await self._is_gap_lifecycle_eligible(
                gap,
                instrument,
                mode='historical_backfill',
            )
            if not eligible:
                dm_logger.info(
                    "[DataManager] Gap fill lifecycle-skipped: %s %s~%s reason=%s",
                    gap.instrument_id,
                    gap.gap_start,
                    gap.gap_end,
                    lifecycle_reason,
                )
                return False

            effective_gap_start = clipped_start or gap.gap_start
            effective_gap_end = clipped_end or gap.gap_end
            start_date = datetime.combine(effective_gap_start, datetime.min.time())
            end_date = datetime.combine(effective_gap_end, datetime.max.time())

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
                end_date,
                instrument_type=instrument.get('type', 'stock')
            )

            if data:
                dm_logger.info(
                    f"[DataManager] Gap fill data fetched: {gap.instrument_id} "
                    f"{effective_gap_start} to {effective_gap_end}, rows={len(data)}"
                )
                # 保存数据
                for quote in data:
                    quote['instrument_id'] = instrument.get('instrument_id')
                success = await self.db_ops.save_daily_quotes(data)
                if success:
                    dm_logger.info(f"Filled gap for {gap.symbol}: {effective_gap_start} to {effective_gap_end}")

                    # 同步复权因子（仅限股票, 与 update_daily_data 逻辑保持一致）
                    if instrument.get('type', 'stock') == 'stock':
                        try:
                            factors = await self.source_factory.get_adjustment_factors(
                                gap.exchange,
                                instrument.get('instrument_id'),
                                source_symbol,
                                start_date,
                                end_date,
                            )
                            if factors:
                                await self.db_ops.save_adjustment_factors(factors)
                        except Exception as factor_e:
                            dm_logger.debug(
                                "[DataManager] Factor sync skipped during gap fill for %s: %s",
                                gap.instrument_id, factor_e
                            )

                    return True
                dm_logger.warning(
                    f"[DataManager] Gap fill save failed: {gap.instrument_id} "
                    f"{effective_gap_start} to {effective_gap_end}"
                )
            else:
                dm_logger.warning(
                    f"[DataManager] Gap fill returned no data: {gap.instrument_id} "
                    f"{effective_gap_start} to {effective_gap_end}"
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
            db_stats = await self.db_ops.get_database_statistics(fast_mode=True)
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
                    'duration': get_shanghai_time() - self.progress.start_time
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
                    WHERE i.exchange = :exchange AND i.is_active = 1
                    """

                    result = await self.db_ops.execute_read_query(exchange_query, {"exchange": exchange})
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

            exchange_stats = update_results.get('exchange_stats', {})
            if not isinstance(exchange_stats, dict):
                exchange_stats = {}
            instrument_master_sync = update_results.get('instrument_master_sync')
            instrument_master_governance = update_results.get('instrument_master_governance') or instrument_master_sync
            index_master_governance = update_results.get('index_master_governance')
            if not index_master_governance and isinstance(instrument_master_governance, dict):
                index_master_governance = instrument_master_governance.get('index_master_governance')
            catchup_stats = update_results.get('catchup_stats')
            if not isinstance(catchup_stats, dict):
                catchup_stats = {}

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
                'update_results': exchange_stats,
                'instrument_master_sync': instrument_master_sync,
                'instrument_master_governance': instrument_master_governance,
                'index_master_governance': index_master_governance,
                'catchup_stats': catchup_stats,
                'errors': []
            }

            # 统计更新结果
            for exchange in exchanges:
                try:
                    # 当前 daily_data_update 将各市场结果放在 update_results['exchange_stats']；
                    # 同时兼容旧调用直接将市场结果平铺在顶层的结构。
                    stats = exchange_stats.get(exchange, update_results.get(exchange, {}))
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
                            total_count = stats.get('total_instruments', stats.get('total_count', 0))
                            quotes_count = stats.get('quotes_added', stats.get('quotes_count', 0))
                            report['exchange_stats'][exchange] = {
                                'success_count': success_count,
                                'failure_count': stats.get('failure_count', 0),
                                'total_count': total_count,
                                'quotes_count': quotes_count,
                                'catchup_stats': stats.get('catchup_stats', {}),
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
            failure_count = sum(
                int(stats.get('failure_count', 0) or 0)
                for stats in report['exchange_stats'].values()
                if isinstance(stats, dict) and 'error' not in stats
            )
            no_op = (
                total_checked > 0
                and total_updated == 0
                and report['summary']['new_quotes_added'] == 0
                and failure_count == 0
                and not report['errors']
            )
            if no_op:
                report['summary']['success_rate'] = 100.0
                report['summary']['no_op'] = True
                report['summary']['no_op_reason'] = 'target_date_already_covered'
                report['summary']['summary_note'] = '目标日期已覆盖，无新增行情'
            else:
                report['summary']['success_rate'] = (total_updated / total_checked * 100) if total_checked > 0 else 100.0
                report['summary']['no_op'] = False

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
                'instrument_master_sync': update_results.get('instrument_master_sync'),
                'instrument_master_governance': update_results.get('instrument_master_governance'),
                'index_master_governance': update_results.get('index_master_governance'),
                'errors': [str(e)]
            }

    def _get_instrument_master_sync_config(self) -> Dict[str, Any]:
        """Return current A-share instrument master sync config with conservative defaults."""
        defaults = {
            'enabled': True,
            'run_before_daily_update': True,
            'skip_for_backfill': True,
            'continue_on_failure': True,
            'timeout_sec': 180,
            'freshness_threshold_hours': 48,
            'pytdx_validation_enabled': False,
            'exchanges': ['SSE', 'SZSE', 'BSE'],
            'bse_delisting_check_enabled': True,
            'bse_delisting_scan_days': 730,
            'bse_delisting_scan_max_pages': 30,
        }

        raw_config = self.data_config.get('instrument_master_sync')
        if not isinstance(raw_config, dict):
            raw_config = self.config.get_nested('data_config.instrument_master_sync', {})
        if isinstance(raw_config, dict):
            defaults.update(raw_config)
        return defaults

    def _get_instrument_master_governance_config(self) -> Dict[str, Any]:
        """Return shared master-governance config while preserving sync defaults."""
        sync_config = self._get_instrument_master_sync_config()
        defaults: Dict[str, Any] = {
            'enabled': sync_config.get('enabled', True),
            'reuse_fresh_master': True,
            'skip_for_backfill': sync_config.get('skip_for_backfill', True),
            'continue_on_failure': sync_config.get('continue_on_failure', True),
            'timeout_sec': sync_config.get('timeout_sec', 180),
            'freshness_threshold_hours': sync_config.get('freshness_threshold_hours', 48),
            'pytdx_validation_enabled': sync_config.get('pytdx_validation_enabled', False),
            'supported_exchanges': sync_config.get('exchanges', ['SSE', 'SZSE', 'BSE']),
            'force_refresh_job_names': ['daily_data_update', 'industry_standard_sync'],
            'current_job_names': [
                'daily_data_update',
                'hk_daily_data_update',
                'company_profile_shadow_sync',
                'industry_shadow_sync',
                'industry_standard_sync',
                'financial_summary_shadow_sync',
                'financial_statements_shadow_sync',
                'shareholder_shadow_sync',
                'shareholder_incremental_sync',
                'financial_disclosure_incremental_sync',
                'financial_disclosure_reconciliation_sync',
                'analyst_forecast_shadow_sync',
                'research_report_shadow_sync',
                'sentiment_event_shadow_sync',
                'technical_snapshot_refresh',
                'risk_snapshot_rebuild',
                'valuation_history_rebuild',
                'valuation_input_sync',
            ],
        }

        raw_config = self.data_config.get('instrument_master_governance')
        if not isinstance(raw_config, dict):
            raw_config = self.config.get_nested('data_config.instrument_master_governance', {})
        if isinstance(raw_config, dict):
            defaults.update(raw_config)
        return defaults

    def _get_master_governance_config(self) -> Dict[str, Any]:
        """Return modular master-governance config with legacy-compatible defaults."""
        legacy = self._get_instrument_master_governance_config()
        index_config = self._get_index_master_governance_config()
        hkex_config = self._get_hkex_instrument_master_sync_config()
        defaults: Dict[str, Any] = {
            'enabled': legacy.get('enabled', True),
            'policies': {
                'a_share_stock': {'enabled': legacy.get('enabled', True)},
                'a_share_index': {'enabled': index_config.get('enabled', False)},
                'hkex_instrument': {'enabled': hkex_config.get('enabled', False)},
            },
            'job_requirements': {},
        }
        raw_config = self.data_config.get('master_governance')
        if not isinstance(raw_config, dict):
            raw_config = self.config.get_nested('data_config.master_governance', {})
        if isinstance(raw_config, dict):
            merged = dict(defaults)
            merged.update({k: v for k, v in raw_config.items() if k not in {'policies', 'job_requirements'}})
            policies = dict(defaults['policies'])
            for scope, scope_config in (raw_config.get('policies') or {}).items():
                if isinstance(scope_config, dict):
                    policies[str(scope)] = {**policies.get(str(scope), {}), **scope_config}
            merged['policies'] = policies
            merged['job_requirements'] = raw_config.get('job_requirements') or {}
            return merged
        return defaults

    def _build_master_governance_orchestrator(self) -> MasterGovernanceOrchestrator:
        """Create a modular governance orchestrator bound to this DataManager."""
        legacy_config = self._get_instrument_master_governance_config()
        index_config = self._get_index_master_governance_config()
        hkex_config = self._get_hkex_instrument_master_sync_config()
        registry = PolicyRegistry([
            AShareStockPolicy(self, legacy_config),
            AShareIndexPolicy(self, index_config),
            HKEXInstrumentPolicy(self, hkex_config),
        ])
        modular_config = self._get_master_governance_config()
        return MasterGovernanceOrchestrator(
            registry=registry,
            policy_config=modular_config.get('policies') or {},
        )

    def _build_master_governance_requirements(
        self,
        *,
        job_name: str,
        exchanges: Optional[List[str]],
        instrument_types: Optional[List[str]] = None,
        job_type: str = 'current',
        target_date: Optional[date] = None,
        force_refresh: bool = False,
        include_pytdx_validation: Optional[bool] = None,
        timeout_sec: Optional[int] = None,
        freshness_threshold_hours: Optional[float] = None,
        continue_on_failure: Optional[bool] = None,
    ) -> List[MasterGovernanceRequirement]:
        """Resolve explicit modular requirements, falling back to legacy policy."""
        modular_config = self._get_master_governance_config()
        legacy_config = self._get_instrument_master_governance_config()
        if not modular_config.get('enabled', True):
            return []

        default_continue = (
            continue_on_failure
            if continue_on_failure is not None
            else legacy_config.get('continue_on_failure', True)
        )
        configured_requirements = (
            (modular_config.get('job_requirements') or {}).get(job_name)
        )
        resolved_runtime_exchanges = self._resolve_master_governance_exchanges(exchanges)
        requested_runtime_types = {
            str(item).lower()
            for item in (instrument_types or [])
            if str(item).strip()
        }
        local_today = get_shanghai_time().date()
        historical_job = job_type in {'historical', 'backfill', 'point_in_time'}
        if target_date is not None and target_date < local_today:
            historical_job = True
        if configured_requirements:
            requirements: List[MasterGovernanceRequirement] = []
            for raw in configured_requirements:
                requirement = MasterGovernanceRequirement.from_config(
                    raw,
                    job_name=job_name,
                    job_type=job_type,
                    target_date=target_date,
                    default_continue_on_error=default_continue,
                )
                if requested_runtime_types and not (
                    set(requirement.instrument_types) & requested_runtime_types
                ):
                    continue
                if resolved_runtime_exchanges:
                    scoped = [
                        exchange for exchange in requirement.exchanges
                        if exchange in set(resolved_runtime_exchanges)
                    ]
                    if not scoped:
                        continue
                    requirement = replace(requirement, exchanges=scoped)
                if historical_job and legacy_config.get('skip_for_backfill', True) and not force_refresh:
                    requirement = replace(requirement, mode='skip_for_backfill')
                elif force_refresh and 'force_refresh' in SUPPORTED_MODES_BY_SCOPE.get(requirement.scope, set()):
                    requirement = replace(requirement, mode='force_refresh')
                if timeout_sec is not None:
                    requirement = replace(requirement, timeout_sec=timeout_sec)
                requirements.append(requirement)
            return requirements

        resolved_exchanges = resolved_runtime_exchanges
        requested_types = {
            str(item).lower()
            for item in (instrument_types or ['stock'])
            if str(item).strip()
        }

        def _mode(default_force: bool = False) -> str:
            if historical_job and legacy_config.get('skip_for_backfill', True) and not force_refresh:
                return 'skip_for_backfill'
            return 'force_refresh' if (force_refresh or default_force) else 'freshness_gated'

        force_refresh_job_names = self._get_instrument_master_force_refresh_job_names()
        default_force = job_name in force_refresh_job_names and not historical_job
        requirements = []
        a_share_exchanges = [ex for ex in resolved_exchanges if ex in ('SSE', 'SZSE', 'BSE')]
        if a_share_exchanges and ('stock' in requested_types or not requested_types):
            requirements.append(MasterGovernanceRequirement(
                scope='a_share_stock',
                exchanges=a_share_exchanges,
                instrument_types=['stock'],
                mode=_mode(default_force),
                target_date=target_date,
                job_name=job_name,
                job_type=job_type,
                continue_on_error=bool(default_continue),
                timeout_sec=timeout_sec if timeout_sec is not None else legacy_config.get('timeout_sec'),
                freshness_threshold_hours=(
                    freshness_threshold_hours
                    if freshness_threshold_hours is not None
                    else legacy_config.get('freshness_threshold_hours')
                ),
                include_pytdx_validation=(
                    include_pytdx_validation
                    if include_pytdx_validation is not None
                    else legacy_config.get('pytdx_validation_enabled', False)
                ),
                legacy_fallback=True,
            ))

        index_config = self._get_index_master_governance_config()
        index_exchanges = [ex for ex in resolved_exchanges if ex in ('SSE', 'SZSE')]
        if (
            index_exchanges
            and 'index' in requested_types
            and index_config.get('enabled', False)
            and index_config.get('run_before_daily_update', True)
        ):
            requirements.append(MasterGovernanceRequirement(
                scope='a_share_index',
                exchanges=index_exchanges,
                instrument_types=['index'],
                mode=_mode(False),
                target_date=target_date,
                job_name=job_name,
                job_type=job_type,
                continue_on_error=bool(index_config.get('continue_on_failure', default_continue)),
                timeout_sec=index_config.get('timeout_sec'),
                freshness_threshold_hours=index_config.get('freshness_threshold_hours'),
                legacy_fallback=True,
            ))

        hkex_config = self._get_hkex_instrument_master_sync_config()
        if 'HKEX' in resolved_exchanges and hkex_config.get('enabled', False):
            hkex_mode = 'skip_for_backfill' if _mode(False) == 'skip_for_backfill' else hkex_config.get('mode', 'audit_only')
            requirements.append(MasterGovernanceRequirement(
                scope='hkex_instrument',
                exchanges=['HKEX'],
                instrument_types=['stock'],
                mode=hkex_mode,
                target_date=target_date,
                job_name=job_name,
                job_type=job_type,
                continue_on_error=bool(hkex_config.get('continue_on_failure', default_continue)),
                timeout_sec=timeout_sec if timeout_sec is not None else hkex_config.get('timeout_sec'),
                legacy_fallback=True,
            ))
        return requirements

    async def run_master_governance(
        self,
        requirements: List[MasterGovernanceRequirement],
    ) -> Dict[str, Any]:
        """Run modular master governance for explicit requirements."""
        orchestrator = self._build_master_governance_orchestrator()
        return await orchestrator.run(requirements)

    async def run_master_governance_for_job(
        self,
        *,
        job_name: str,
        exchanges: Optional[List[str]] = None,
        instrument_types: Optional[List[str]] = None,
        job_type: str = 'current',
        target_date: Optional[date] = None,
        force_refresh: bool = False,
        include_pytdx_validation: Optional[bool] = None,
        timeout_sec: Optional[int] = None,
        freshness_threshold_hours: Optional[float] = None,
        continue_on_failure: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Resolve and run master governance requirements for a business job."""
        requirements = self._build_master_governance_requirements(
            job_name=job_name,
            exchanges=exchanges,
            instrument_types=instrument_types,
            job_type=job_type,
            target_date=target_date,
            force_refresh=force_refresh,
            include_pytdx_validation=include_pytdx_validation,
            timeout_sec=timeout_sec,
            freshness_threshold_hours=freshness_threshold_hours,
            continue_on_failure=continue_on_failure,
        )
        if not requirements:
            started_at = get_shanghai_time()
            return {
                'status': 'skipped',
                'action': 'skipped',
                'reason': 'no_master_governance_requirements',
                'job_name': job_name,
                'job_type': job_type,
                'started_at': started_at.isoformat(),
                'finished_at': get_shanghai_time().isoformat(),
                'elapsed_sec': 0.0,
                'summary': {
                    'exchanges': [],
                    'added_instruments': 0,
                    'deactivated_instruments': 0,
                    'active_count': 0,
                },
                'exchanges': {},
                'children': [],
                'warnings': [],
                'errors': [],
            }
        result = await self.run_master_governance(requirements)
        result['job_name'] = job_name
        result['job_type'] = job_type
        return result

    def _get_index_master_governance_config(self) -> Dict[str, Any]:
        """Return A-share index master-governance config with conservative defaults."""
        default_master_admission = {
            'canonical_key': 'instrument_id',
            'duplicate_key_policy': 'skip_ambiguous',
            'ambiguous_duplicate_action': 'skip',
            'collapse_identical_duplicates': True,
            'conflict_signature_fields': [
                'name',
                'market',
                'industry',
                'sector',
                'metadata.cni_code',
                'metadata.full_name',
            ],
        }
        defaults: Dict[str, Any] = {
            'enabled': False,
            'run_before_daily_update': True,
            'exchanges': ['SSE', 'SZSE'],
            'official_sources': ['cnindex', 'csindex'],
            'freshness_threshold_hours': 48,
            'stale_no_quote_trading_days': 10,
            'skip_stale_no_quote': True,
            'write_stale_no_quote': False,
            'allow_series_inference': True,
            'sample_limit': 10,
            'continue_on_failure': True,
            'timeout_sec': 120,
            'master_admission': default_master_admission,
        }
        raw_config = self.data_config.get('index_master_governance')
        if not isinstance(raw_config, dict):
            raw_config = self.config.get_nested('data_config.index_master_governance', {})
        if isinstance(raw_config, dict):
            raw_admission = raw_config.get('master_admission')
            defaults.update(raw_config)
            if isinstance(raw_admission, dict):
                defaults['master_admission'] = {
                    **default_master_admission,
                    **raw_admission,
                }
        return defaults

    @staticmethod
    def _get_nested_mapping_value(row: Dict[str, Any], path: str) -> Any:
        value: Any = row
        for part in str(path).split('.'):
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _apply_index_master_admission_rules(
        self,
        rows: List[Dict[str, Any]],
        *,
        config: Dict[str, Any],
        sample_limit: int,
    ) -> Dict[str, Any]:
        """Apply configurable master-data admission rules before writing instruments."""
        admission = config.get('master_admission') or {}
        canonical_key = admission.get('canonical_key') or 'instrument_id'
        duplicate_policy = str(admission.get('duplicate_key_policy') or 'skip_ambiguous').lower()
        ambiguous_action = str(admission.get('ambiguous_duplicate_action') or 'skip').lower()
        collapse_identical = bool(admission.get('collapse_identical_duplicates', True))
        signature_fields = admission.get('conflict_signature_fields') or [
            'name',
            'market',
            'industry',
            'sector',
            'metadata.cni_code',
            'metadata.full_name',
        ]

        rows_by_key: Dict[str, List[Dict[str, Any]]] = {}
        missing_key_count = 0
        for row in rows or []:
            key = self._get_nested_mapping_value(row, canonical_key)
            if not key:
                missing_key_count += 1
                continue
            rows_by_key.setdefault(str(key), []).append(row)

        admitted_rows: List[Dict[str, Any]] = []
        ambiguous_groups: List[Dict[str, Any]] = []
        handled_ambiguous_groups: List[Dict[str, Any]] = []
        collapsed_duplicate_rows = 0
        forced_kept_groups = 0

        for key, rows_for_key in rows_by_key.items():
            if len(rows_for_key) == 1:
                admitted_rows.append(rows_for_key[0])
                continue

            signatures = {
                tuple(
                    str(self._get_nested_mapping_value(row, field) or '')
                    for field in signature_fields
                )
                for row in rows_for_key
            }
            if collapse_identical and len(signatures) == 1:
                admitted_rows.append(rows_for_key[-1])
                collapsed_duplicate_rows += len(rows_for_key) - 1
                continue

            if duplicate_policy in {'keep_first', 'first'}:
                admitted_rows.append(rows_for_key[0])
                forced_kept_groups += 1
                continue
            if duplicate_policy in {'keep_last', 'last'}:
                admitted_rows.append(rows_for_key[-1])
                forced_kept_groups += 1
                continue

            handled_group = self._classify_index_duplicate_group(
                key=key,
                rows_for_key=rows_for_key,
                sample_limit=sample_limit,
            )
            if handled_group.get('handled'):
                admitted_rows.extend(handled_group.get('rows') or [])
                handled_ambiguous_groups.append(handled_group)
                continue

            ambiguous_groups.append({
                'key': key,
                'row_count': len(rows_for_key),
                'samples': [
                    {
                        'instrument_id': row.get('instrument_id'),
                        'symbol': row.get('symbol'),
                        'name': row.get('name'),
                        'market': row.get('market'),
                        'industry': row.get('industry'),
                        'cni_code': (row.get('metadata') or {}).get('cni_code'),
                    }
                    for row in rows_for_key[:max(1, min(sample_limit or 5, 5))]
                ],
            })
            if ambiguous_action != 'skip':
                # Conservative fallback: unsupported ambiguous actions do not write unclear master rows.
                continue

        return {
            'rows': admitted_rows,
            'missing_key_count': missing_key_count,
            'ambiguous_groups': ambiguous_groups,
            'handled_ambiguous_groups': handled_ambiguous_groups,
            'collapsed_duplicate_rows': collapsed_duplicate_rows,
            'forced_kept_groups': forced_kept_groups,
        }

    def _classify_index_duplicate_group(
        self,
        *,
        key: str,
        rows_for_key: List[Dict[str, Any]],
        sample_limit: int,
    ) -> Dict[str, Any]:
        """Classify official index duplicate keys into deterministic outcomes.

        CNIndex publishes multiple identity namespaces. If a duplicate group can be
        reduced to one quote-capable row or one metadata-only representative, it is
        handled and should not become an operator warning.
        """
        quote_rows = [
            row for row in rows_for_key
            if self._is_quote_capable_index_master_row(row)
        ]
        metadata_rows = [
            row for row in rows_for_key
            if self._is_metadata_only_index_master_row(row)
        ]
        sample_rows = [
            {
                'instrument_id': row.get('instrument_id'),
                'symbol': row.get('symbol'),
                'name': row.get('name'),
                'source': row.get('source'),
                'status': row.get('status'),
                'szse_quote_code': (row.get('metadata') or {}).get('szse_quote_code'),
                'cni_code': (row.get('metadata') or {}).get('cni_code'),
            }
            for row in rows_for_key[:max(1, min(sample_limit or 5, 5))]
        ]
        if len(quote_rows) == 1:
            return {
                'handled': True,
                'key': key,
                'classification': 'single_quote_capable_identity',
                'row_count': len(rows_for_key),
                'rows': [quote_rows[0]],
                'samples': sample_rows,
            }
        if not quote_rows and len(metadata_rows) == len(rows_for_key):
            # Keep one representative metadata row for auditability. The metadata-only
            # status keeps it out of tradable daily quote universes.
            return {
                'handled': True,
                'key': key,
                'classification': 'metadata_only_duplicate_identity',
                'row_count': len(rows_for_key),
                'rows': [rows_for_key[-1]],
                'samples': sample_rows,
            }
        return {
            'handled': False,
            'key': key,
            'classification': 'unclassified_duplicate_identity',
            'row_count': len(rows_for_key),
            'rows': [],
            'samples': sample_rows,
        }

    @staticmethod
    def _is_metadata_only_index_master_row(row: Dict[str, Any]) -> bool:
        if str(row.get('type') or '').lower() != 'index':
            return False
        status = str(row.get('status') or '').lower()
        if status == 'metadata_only':
            return True
        metadata = row.get('metadata') or {}
        source = str(row.get('source') or '').lower()
        if source == 'cnindex' and not metadata.get('szse_quote_code'):
            return True
        return row.get('is_active') in (False, 0, '0', 'false', 'False') and row.get('trading_status') in (0, '0', False)

    @staticmethod
    def _is_quote_capable_index_master_row(row: Dict[str, Any]) -> bool:
        if str(row.get('type') or '').lower() != 'index':
            return False
        if str(row.get('status') or '').lower() in {'metadata_only', 'inactive', 'calculation_terminated'}:
            return False
        metadata = row.get('metadata') or {}
        source = str(row.get('source') or '').lower()
        instrument_id = str(row.get('instrument_id') or '')
        if source == 'cnindex':
            quote_code = str(metadata.get('szse_quote_code') or '')
            return bool(
                len(quote_code) == 6
                and quote_code.isdigit()
                and instrument_id == f'{quote_code}.SZ'
            )
        if source == 'csindex':
            return instrument_id.upper().endswith(('.SH', '.SZ')) and bool(row.get('admission_evidence'))
        return row.get('is_active') in (True, 1, '1', 'true', 'True')

    def _csindex_has_active_admission(
        self,
        row: Dict[str, Any],
        *,
        snapshots_before: Dict[str, Dict[str, Any]],
        config: Dict[str, Any],
        local_quote_ids: Optional[Set[str]] = None,
    ) -> tuple[bool, str]:
        """Return whether a CSIndex full-list row may enter active quote universe."""
        instrument_id = str(row.get('instrument_id') or '')
        symbol = str(row.get('symbol') or row.get('source_symbol') or '')
        exchange = str(row.get('exchange') or 'SSE').upper()
        snapshot = snapshots_before.get(exchange) or {}
        if instrument_id in (snapshot.get('all_ids') or set()):
            return True, 'local_master_identity'
        if instrument_id in (local_quote_ids or set()):
            return True, 'local_quote_history'

        admission = config.get('master_admission') or {}
        whitelist = set(str(item) for item in (
            config.get('csindex_active_admission_whitelist')
            or admission.get('csindex_active_admission_whitelist')
            or []
        ))
        if instrument_id in whitelist or symbol in whitelist:
            return True, 'configured_whitelist'

        metadata = row.get('metadata') or {}
        evidence = metadata.get('quote_admission_evidence') or row.get('admission_evidence')
        if evidence in {'official_recent_quote', 'fallback_recent_quote', 'local_quote_history'}:
            return True, str(evidence)
        return False, 'reference_only_no_quote_evidence'

    async def _get_existing_quote_instrument_ids(self, instrument_ids: List[str]) -> Set[str]:
        """Return instrument IDs with any local quote rows, batched for admission checks."""
        candidate_ids = sorted({str(item) for item in instrument_ids or [] if item})
        if not candidate_ids:
            return set()
        found_ids: Set[str] = set()
        chunk_size = 500
        for offset in range(0, len(candidate_ids), chunk_size):
            chunk = candidate_ids[offset:offset + chunk_size]
            params = {f'id_{idx}': instrument_id for idx, instrument_id in enumerate(chunk)}
            placeholders = ','.join(f':id_{idx}' for idx in range(len(chunk)))
            rows = await self.db_ops.execute_read_query(
                f"""
                SELECT DISTINCT instrument_id
                FROM daily_quotes
                WHERE instrument_id IN ({placeholders})
                """,
                params,
            )
            for row in rows or []:
                if row.get('instrument_id'):
                    found_ids.add(str(row.get('instrument_id')))
        return found_ids

    async def _filter_index_rows_colliding_with_stock_ids(
        self,
        rows: List[Dict[str, Any]],
        *,
        result: Dict[str, Any],
        sample_limit: int,
    ) -> List[Dict[str, Any]]:
        """Drop index upserts that would overwrite existing stock instrument IDs."""
        candidate_ids = sorted({
            str(row.get('instrument_id'))
            for row in rows or []
            if str(row.get('type') or '').lower() == 'index' and row.get('instrument_id')
        })
        if not candidate_ids:
            return rows

        stock_ids: Set[str] = set()
        chunk_size = 200
        for offset in range(0, len(candidate_ids), chunk_size):
            chunk = candidate_ids[offset:offset + chunk_size]
            params = {f'id_{idx}': instrument_id for idx, instrument_id in enumerate(chunk)}
            placeholders = ','.join(f':id_{idx}' for idx in range(len(chunk)))
            found = await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id
                FROM instruments
                WHERE type = 'stock'
                  AND instrument_id IN ({placeholders})
                """,
                params,
            )
            for row in found or []:
                if row.get('instrument_id'):
                    stock_ids.add(str(row.get('instrument_id')))

        if not stock_ids:
            return rows

        kept: List[Dict[str, Any]] = []
        summary = result.setdefault('summary', {})
        summary['stock_collision_index_rows_skipped'] = (
            int(summary.get('stock_collision_index_rows_skipped', 0) or 0)
            + len(stock_ids)
        )
        for row in rows:
            instrument_id = str(row.get('instrument_id') or '')
            if instrument_id in stock_ids and str(row.get('type') or '').lower() == 'index':
                if len(summary.setdefault('handled_samples', [])) < sample_limit:
                    summary['handled_samples'].append({
                        'instrument_id': instrument_id,
                        'state': 'stock_collision_skipped',
                        'source': row.get('source'),
                        'name': row.get('name'),
                    })
                continue
            kept.append(row)
        return kept

    def _get_instrument_master_force_refresh_job_names(self) -> set[str]:
        """Return job names that must bypass local master freshness reuse."""
        config = self._get_instrument_master_governance_config()
        return {
            str(item).strip()
            for item in (config.get('force_refresh_job_names') or [])
            if str(item).strip()
        }

    def _get_hkex_instrument_master_sync_config(self) -> Dict[str, Any]:
        """Return HKEX-specific master sync config with audit-first defaults."""
        defaults: Dict[str, Any] = {
            'enabled': False,
            'mode': 'audit_only',
            'timeout_sec': 60,
            'official_securities_list_url': 'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx',
            'official_securities_list_file': '',
            'hkexnews_active_list_url': 'https://www.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json',
            'hkexnews_active_list_file': '',
            'hkexnews_delisted_list_url': 'https://www.hkexnews.hk/ncms/script/eds/inactivestock_sehk_e.json',
            'hkexnews_delisted_list_file': '',
            'hkexnews_suspension_main_board_url': 'https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/Exchange-Reports/Prolonged-Suspension-Status-Report/psuspenrep_mb.pdf',
            'hkexnews_suspension_gem_url': 'https://www2.hkexnews.hk/-/media/HKEXnews/Homepage/Exchange-Reports/Prolonged-Suspension-Status-Report/psuspenrep_gem.pdf',
            'hkexnews_suspension_main_board_file': '',
            'hkexnews_suspension_gem_file': '',
            'manual_review_file': 'data/hkex_manual_review.json',
            'akshare_spot_file': '',
            'eastmoney_profile_file': '',
            'fetch_supplemental_live': False,
            'write_review_discrepancies': True,
            'allowed_product_types': ['ordinary_equity', 'reit', 'etf'],
        }
        raw_config = self.data_config.get('hkex_instrument_master_sync')
        if not isinstance(raw_config, dict):
            raw_config = self.config.get_nested('data_config.hkex_instrument_master_sync', {})
        if isinstance(raw_config, dict):
            defaults.update(raw_config)
        return defaults

    def _resolve_master_governance_exchanges(
        self,
        exchanges: Optional[List[str]],
    ) -> List[str]:
        """Resolve a job exchange list for master governance."""
        if exchanges:
            return list(exchanges)

        research_markets = getattr(self.research_config, 'markets', None)
        if research_markets:
            return list(research_markets)

        config = self._get_instrument_master_governance_config()
        return list(config.get('supported_exchanges') or ['SSE', 'SZSE', 'BSE'])

    def _parse_master_updated_at(self, value: Optional[Any]) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        except Exception:
            return None

    async def _build_fresh_master_governance_result(
        self,
        *,
        exchanges: List[str],
        freshness_threshold_hours: Optional[float],
        job_name: str,
        job_type: str,
        started_at: datetime,
        unsupported_exchanges: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return a fresh-governance payload when all supported exchanges are fresh."""
        if not exchanges or freshness_threshold_hours is None:
            return None

        now = datetime.now()
        exchange_results: Dict[str, Dict[str, Any]] = {}
        active_count = 0
        warnings: List[str] = []

        for exchange in exchanges:
            snapshot = await self._get_instrument_master_snapshot(exchange)
            latest_raw = snapshot.get('freshness', {}).get('latest_updated_at')
            latest = self._parse_master_updated_at(latest_raw)
            freshness = dict(snapshot.get('freshness') or {})
            if latest is None:
                return None
            age_hours = (now - latest).total_seconds() / 3600
            freshness['age_hours'] = round(age_hours, 2)
            if age_hours > float(freshness_threshold_hours):
                return None

            active_count += int(snapshot.get('active_count', 0) or 0)
            exchange_results[exchange] = {
                'status': 'fresh',
                'reason': 'master_data_within_freshness_window',
                'before': {},
                'after': {
                    'total_count': snapshot.get('total_count', 0),
                    'active_count': snapshot.get('active_count', 0),
                    'inactive_count': snapshot.get('inactive_count', 0),
                    'status_counts': snapshot.get('status_counts', {}),
                    'source_counts': snapshot.get('source_counts', {}),
                },
                'fetched_count': 0,
                'source_usage': {},
                'added_count': 0,
                'deactivated_count': 0,
                'added_samples': [],
                'deactivated_samples': [],
                'freshness': freshness,
                'pytdx_validation': None,
                'warnings': [],
                'errors': [],
            }

        unsupported = list(unsupported_exchanges or [])
        for exchange in unsupported:
            warnings.append(f"{exchange}: unsupported market for instrument master governance")

        finished_at = get_shanghai_time()
        status = 'warning' if warnings else 'fresh'
        return {
            'status': status,
            'action': 'reused_fresh_master',
            'reason': 'master_data_within_freshness_window',
            'job_name': job_name,
            'job_type': job_type,
            'started_at': started_at.isoformat(),
            'finished_at': finished_at.isoformat(),
            'elapsed_sec': round((finished_at - started_at).total_seconds(), 3),
            'source_priority': ['local_freshness', 'baostock', 'akshare', 'pytdx_validation_only'],
            'exchanges': exchange_results,
            'unsupported_exchanges': unsupported,
            'summary': {
                'exchanges': exchanges,
                'added_instruments': 0,
                'deactivated_instruments': 0,
                'active_count': active_count,
            },
            'warnings': warnings,
            'errors': [],
        }

    async def ensure_instrument_master_fresh(
        self,
        exchanges: Optional[List[str]] = None,
        *,
        job_name: str = 'unknown',
        job_type: str = 'current',
        target_date: Optional[date] = None,
        force_refresh: bool = False,
        include_pytdx_validation: Optional[bool] = None,
        timeout_sec: Optional[int] = None,
        freshness_threshold_hours: Optional[float] = None,
        continue_on_failure: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Shared pre-run governance for current instrument master freshness.

        This is the only production pre-run entry point for master freshness.
        It delegates actual A-share source collection and merge semantics to
        sync_instrument_master() so quote, research, and financial jobs share
        one master-data implementation.
        """
        started_at = get_shanghai_time()
        config = self._get_instrument_master_governance_config()

        if not config.get('enabled', True):
            return {
                'status': 'skipped',
                'action': 'skipped',
                'reason': 'disabled_by_config',
                'job_name': job_name,
                'job_type': job_type,
                'started_at': started_at.isoformat(),
                'finished_at': get_shanghai_time().isoformat(),
                'elapsed_sec': 0.0,
                'exchanges': {},
                'unsupported_exchanges': [],
                'summary': {'exchanges': [], 'added_instruments': 0, 'deactivated_instruments': 0, 'active_count': 0},
                'warnings': [],
                'errors': [],
            }

        resolved_exchanges = self._resolve_master_governance_exchanges(exchanges)
        result = await self.run_master_governance_for_job(
            job_name=job_name,
            exchanges=resolved_exchanges,
            instrument_types=['stock'],
            job_type=job_type,
            target_date=target_date,
            force_refresh=force_refresh,
            include_pytdx_validation=include_pytdx_validation,
            timeout_sec=timeout_sec,
            freshness_threshold_hours=freshness_threshold_hours,
            continue_on_failure=continue_on_failure,
        )
        if (
            result.get('reason') == 'no_master_governance_requirements'
            and resolved_exchanges
        ):
            unsupported_exchanges = list(resolved_exchanges)
            return {
                'status': 'skipped',
                'action': 'skipped',
                'reason': 'no_supported_exchange_in_update_scope',
                'job_name': job_name,
                'job_type': job_type,
                'started_at': started_at.isoformat(),
                'finished_at': get_shanghai_time().isoformat(),
                'elapsed_sec': 0.0,
                'exchanges': {},
                'unsupported_exchanges': unsupported_exchanges,
                'summary': {'exchanges': [], 'added_instruments': 0, 'deactivated_instruments': 0, 'active_count': 0},
                'warnings': [f"{ex}: unsupported market for instrument master governance" for ex in unsupported_exchanges],
                'errors': [],
            }
        return result

    async def _get_instrument_master_snapshot(self, exchange: str) -> Dict[str, Any]:
        """Read a compact stock master snapshot for one exchange."""
        rows = await self.db_ops.execute_read_query(
            """
            SELECT instrument_id, symbol, name, exchange, type, status, is_active,
                   listed_date, delisted_date, source, updated_at
            FROM instruments
            WHERE exchange = :exchange AND type = 'stock'
            """,
            {'exchange': exchange},
        )

        all_ids: Set[str] = set()
        active_ids: Set[str] = set()
        updated_values: List[str] = []
        status_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}

        for row in rows:
            instrument_id = row.get('instrument_id')
            if not instrument_id:
                continue
            all_ids.add(instrument_id)

            is_active = row.get('is_active')
            if is_active in (True, 1, '1', 'true', 'True'):
                active_ids.add(instrument_id)

            status = str(row.get('status') or 'unknown')
            source = str(row.get('source') or 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            source_counts[source] = source_counts.get(source, 0) + 1

            updated_at = row.get('updated_at')
            if updated_at:
                updated_values.append(str(updated_at))

        updated_values.sort()
        return {
            'exchange': exchange,
            'total_count': len(all_ids),
            'active_count': len(active_ids),
            'inactive_count': max(0, len(all_ids) - len(active_ids)),
            'all_ids': all_ids,
            'active_ids': active_ids,
            'status_counts': status_counts,
            'source_counts': source_counts,
            'freshness': {
                'oldest_updated_at': updated_values[0] if updated_values else None,
                'latest_updated_at': updated_values[-1] if updated_values else None,
            },
        }

    async def _get_index_master_snapshot(self, exchange: str) -> Dict[str, Any]:
        """Read a compact index master snapshot for one exchange."""
        rows = await self.db_ops.execute_read_query(
            """
            SELECT instrument_id, symbol, name, exchange, type, status, is_active,
                   trading_status, listed_date, delisted_date, source, updated_at
            FROM instruments
            WHERE exchange = :exchange AND type = 'index'
            """,
            {'exchange': exchange},
        )

        all_ids: Set[str] = set()
        active_ids: Set[str] = set()
        rows_by_id: Dict[str, Dict[str, Any]] = {}
        updated_values: List[str] = []
        status_counts: Dict[str, int] = {}
        source_counts: Dict[str, int] = {}

        for row in rows:
            instrument_id = row.get('instrument_id')
            if not instrument_id:
                continue
            all_ids.add(instrument_id)
            rows_by_id[instrument_id] = row

            is_active = row.get('is_active')
            if is_active in (True, 1, '1', 'true', 'True'):
                active_ids.add(instrument_id)

            status = str(row.get('status') or 'unknown')
            source = str(row.get('source') or 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
            source_counts[source] = source_counts.get(source, 0) + 1

            updated_at = row.get('updated_at')
            if updated_at:
                updated_values.append(str(updated_at))

        updated_values.sort()
        return {
            'exchange': exchange,
            'total_count': len(all_ids),
            'active_count': len(active_ids),
            'inactive_count': max(0, len(all_ids) - len(active_ids)),
            'all_ids': all_ids,
            'active_ids': active_ids,
            'rows_by_id': rows_by_id,
            'status_counts': status_counts,
            'source_counts': source_counts,
            'freshness': {
                'oldest_updated_at': updated_values[0] if updated_values else None,
                'latest_updated_at': updated_values[-1] if updated_values else None,
            },
        }

    def _summarize_instrument_source_usage(self, instruments: List[Dict[str, Any]]) -> Dict[str, int]:
        usage: Dict[str, int] = {}
        for instrument in instruments or []:
            source = str(instrument.get('source') or 'unknown')
            usage[source] = usage.get(source, 0) + 1
        return usage

    def _infer_stock_source_authority(self, instruments: List[Dict[str, Any]]) -> str:
        authorities = Counter(
            str(item.get('source_authority') or '')
            for item in instruments or []
            if item.get('source_authority')
        )
        if authorities:
            return authorities.most_common(1)[0][0]
        source_usage = self._summarize_instrument_source_usage(instruments)
        if any(str(source).endswith('_official') or source == 'exchange_official' for source in source_usage):
            return 'official'
        if source_usage.get('baostock'):
            return 'baostock_fallback'
        if source_usage.get('akshare'):
            return 'akshare_fallback'
        return 'degraded' if instruments else 'degraded'

    def _build_a_share_stock_metadata_rows(
        self,
        instruments: List[Dict[str, Any]],
        *,
        source_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        diagnostics = source_diagnostics or {}
        rows: List[Dict[str, Any]] = []
        for instrument in instruments or []:
            instrument_id = instrument.get('instrument_id')
            if not instrument_id:
                continue
            metadata = instrument.get('metadata') if isinstance(instrument.get('metadata'), dict) else {}
            rows.append({
                'instrument_id': instrument_id,
                'exchange': instrument.get('exchange'),
                'product_type': 'stock',
                'research_scope': 'include',
                'canonical_instrument_id': instrument_id,
                'is_canonical': True,
                'counter_currency': instrument.get('currency') or 'CNY',
                'official_lifecycle_source': instrument.get('official_lifecycle_source'),
                'source_url': instrument.get('source_url'),
                'raw_snapshot_hash': instrument.get('raw_snapshot_hash'),
                'parser_version': instrument.get('parser_version'),
                'metadata': {
                    **metadata,
                    'source_authority': instrument.get('source_authority'),
                    'selected_source': diagnostics.get('selected_source'),
                    'selected_source_authority': diagnostics.get('selected_source_authority'),
                    'fallback_sources': diagnostics.get('fallback_sources') or [],
                    'fallback_reason': diagnostics.get('fallback_reason'),
                },
            })
        return rows

    def _build_a_share_stock_discrepancy_rows(
        self,
        *,
        exchange: str,
        before: Dict[str, Any],
        instruments: List[Dict[str, Any]],
        source_diagnostics: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        diagnostics = source_diagnostics or {}
        authority = diagnostics.get('selected_source_authority') or self._infer_stock_source_authority(instruments)
        if authority not in {'official', 'official_with_fallback_fields'}:
            return []

        official_ids = {
            item.get('instrument_id')
            for item in instruments or []
            if item.get('instrument_id')
        }
        rows: List[Dict[str, Any]] = []
        for instrument_id in sorted((before.get('active_ids') or set()) - official_ids):
            rows.append({
                'instrument_id': instrument_id,
                'reason': 'official_current_list_missing_no_terminal_evidence',
                'source_authority': authority,
                'exchange': exchange,
            })
            if len(rows) >= 500:
                break
        for instrument_id in diagnostics.get('fallback_only_ids') or []:
            rows.append({
                'instrument_id': instrument_id,
                'reason': 'fallback_current_list_only_not_admitted_without_official_evidence',
                'source_authority': authority,
                'exchange': exchange,
            })
            if len(rows) >= 1000:
                break
        return rows

    async def _validate_instrument_master_with_pytdx(
        self,
        exchange: str,
        active_ids: Set[str],
    ) -> Dict[str, Any]:
        """Compare pytdx current-list coverage without mutating authoritative fields."""
        result = {
            'status': 'skipped',
            'source': 'pytdx',
            'count': 0,
            'missing_in_db_count': 0,
            'missing_in_pytdx_count': 0,
            'missing_in_db_samples': [],
            'missing_in_pytdx_samples': [],
            'warnings': [],
            'errors': [],
        }

        if self.source_factory is None:
            from data_sources.source_factory import get_data_source_factory
            self.source_factory = await get_data_source_factory(self.db_ops)

        pytdx_source = None
        if hasattr(self.source_factory, '_get_source_instance'):
            pytdx_source = self.source_factory._get_source_instance('pytdx', region='a_stock')
        if pytdx_source is None:
            result['warnings'].append('pytdx source unavailable')
            return result

        try:
            instruments = await pytdx_source.get_instrument_list(exchange)
            pytdx_ids = {
                inst.get('instrument_id')
                for inst in (instruments or [])
                if inst.get('instrument_id')
            }
            missing_in_db = sorted(pytdx_ids - active_ids)
            missing_in_pytdx = sorted(active_ids - pytdx_ids)
            result.update({
                'status': 'success',
                'count': len(pytdx_ids),
                'missing_in_db_count': len(missing_in_db),
                'missing_in_pytdx_count': len(missing_in_pytdx),
                'missing_in_db_samples': missing_in_db[:20],
                'missing_in_pytdx_samples': missing_in_pytdx[:20],
            })
            if missing_in_db or missing_in_pytdx:
                result['status'] = 'warning'
                result['warnings'].append('pytdx current-list differs from authoritative master data')
        except Exception as exc:
            result['status'] = 'error'
            result['errors'].append(str(exc))

        return result

    @staticmethod
    def _normalize_cninfo_title(title: Optional[str]) -> str:
        """Normalize CNInfo highlighted titles before keyword classification."""
        text = str(title or "")
        return (
            text.replace("<em>", "")
            .replace("</em>", "")
            .replace("&nbsp;", " ")
            .strip()
        )

    @classmethod
    def _classify_bse_delisting_title(cls, title: Optional[str]) -> str:
        """Classify BSE delisting announcement titles for master-data mutation."""
        text = cls._normalize_cninfo_title(title)
        if not text:
            return "irrelevant"
        if any(keyword in text for keyword in ("收购", "交易进展", "公开摘牌方式")):
            return "irrelevant"
        if "摘牌" in text and ("股票" in text or "终止上市" in text):
            return "confirmed_delisted"
        if "终止上市暨摘牌" in text or "退市整理期届满" in text:
            return "confirmed_delisted"
        if any(
            keyword in text
            for keyword in (
                "拟终止",
                "可能被终止上市",
                "退市风险",
                "风险提示",
                "事先告知书",
            )
        ):
            return "risk_only"
        if "终止上市决定" in text or "将被终止上市" in text:
            return "decision_pending_effective_date"
        return "irrelevant"

    @staticmethod
    def _cninfo_announcement_local_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
            return parsed.date()
        except Exception:
            return None

    async def _scan_bse_delisting_announcements(
        self,
        *,
        start_date: str,
        end_date: str,
        max_pages: int,
    ) -> List[Any]:
        """Scan CNInfo BSE delisting-related metadata without per-stock requests."""
        from research.providers.cninfo_announcements import (
            CninfoAnnouncementScanConfig,
            CninfoAnnouncementScanner,
        )

        scanner = CninfoAnnouncementScanner(
            request_timeout_seconds=20.0,
            request_interval_seconds=0.2,
            retry_attempts=2,
        )
        configs = [
            CninfoAnnouncementScanConfig(
                purpose_key="instrument_master_bse_delisting",
                market="BSE",
                column="neeq",
                plate="bj",
                category="category_tbclts_szsh",
                start_date=start_date,
                end_date=end_date,
                page_size=30,
                max_pages=max_pages,
            ),
            CninfoAnnouncementScanConfig(
                purpose_key="instrument_master_bse_delisting",
                market="BSE",
                column="neeq",
                plate="bj",
                category="category_tszlq_szsh",
                start_date=start_date,
                end_date=end_date,
                page_size=30,
                max_pages=max_pages,
            ),
            CninfoAnnouncementScanConfig(
                purpose_key="instrument_master_bse_delisting",
                market="BSE",
                column="neeq",
                plate="bj",
                search_key="终止上市",
                start_date=start_date,
                end_date=end_date,
                page_size=30,
                max_pages=max_pages,
            ),
            CninfoAnnouncementScanConfig(
                purpose_key="instrument_master_bse_delisting",
                market="BSE",
                column="neeq",
                plate="bj",
                search_key="摘牌",
                start_date=start_date,
                end_date=end_date,
                page_size=30,
                max_pages=max_pages,
            ),
        ]

        records_by_id: Dict[str, Any] = {}
        for config in configs:
            result = scanner.scan(config)
            for record in result.records:
                if record.announcement_id:
                    records_by_id[record.announcement_id] = record
        return list(records_by_id.values())

    async def _sync_bse_delisting_status(
        self,
        *,
        before_snapshot: Dict[str, Any],
        fetched_instruments: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Confirm BSE current-list disappearances through CNInfo terminal announcements."""
        config = self._get_instrument_master_sync_config()
        result: Dict[str, Any] = {
            "status": "skipped",
            "enabled": bool(config.get("bse_delisting_check_enabled", True)),
            "candidate_count": 0,
            "confirmed_count": 0,
            "risk_only_count": 0,
            "unconfirmed_count": 0,
            "updated_samples": [],
            "unconfirmed_samples": [],
            "warnings": [],
            "errors": [],
        }
        if not result["enabled"]:
            result["reason"] = "disabled_by_config"
            return result

        current_ids = {
            str(item.get("instrument_id") or "")
            for item in (fetched_instruments or [])
            if str(item.get("exchange") or "").upper() == "BSE"
            and str(item.get("type") or "stock").lower() == "stock"
            and item.get("instrument_id")
        }
        if not current_ids:
            result["status"] = "warning"
            result["warnings"].append("BSE current list empty; delisting check skipped")
            return result

        candidates = sorted(before_snapshot.get("active_ids", set()) - current_ids)
        result["candidate_count"] = len(candidates)
        if not candidates:
            result["status"] = "success"
            return result

        symbol_to_instrument = {
            instrument_id.split(".")[0]: instrument_id
            for instrument_id in candidates
            if instrument_id.endswith(".BJ")
        }
        end_dt = get_shanghai_time().date()
        start_dt = end_dt - timedelta(days=int(config.get("bse_delisting_scan_days", 730)))
        records = await self._scan_bse_delisting_announcements(
            start_date=start_dt.isoformat(),
            end_date=end_dt.isoformat(),
            max_pages=int(config.get("bse_delisting_scan_max_pages", 30)),
        )

        confirmed_ids: Set[str] = set()
        for record in records:
            classification = self._classify_bse_delisting_title(record.title)
            if classification == "irrelevant":
                continue
            for raw_symbol in record.symbols or []:
                symbol = str(raw_symbol).strip().zfill(6)
                instrument_id = symbol_to_instrument.get(symbol)
                if not instrument_id:
                    continue
                if classification != "confirmed_delisted":
                    result["risk_only_count"] += 1
                    continue

                delisted_on = self._cninfo_announcement_local_date(record.announcement_time)
                if delisted_on is None:
                    result["errors"].append(f"{instrument_id}: confirmed announcement has no parseable date")
                    continue
                updated = await self.db_ops.mark_instrument_delisted(
                    instrument_id,
                    delisted_date=delisted_on,
                    source="cninfo_bse_delisting",
                )
                if updated:
                    confirmed_ids.add(instrument_id)
                    result["confirmed_count"] += 1
                    if len(result["updated_samples"]) < 20:
                        result["updated_samples"].append(
                            {
                                "instrument_id": instrument_id,
                                "delisted_date": delisted_on.isoformat(),
                                "announcement_id": record.announcement_id,
                                "title": self._normalize_cninfo_title(record.title),
                            }
                        )

        unconfirmed = sorted(set(candidates) - confirmed_ids)
        result["unconfirmed_count"] = len(unconfirmed)
        result["unconfirmed_samples"] = unconfirmed[:20]
        if result["errors"]:
            result["status"] = "error"
        elif result["unconfirmed_count"]:
            result["status"] = "warning"
            result["warnings"].append("BSE current-list disappearance has no confirmed delisting announcement")
        else:
            result["status"] = "success"
        return result

    def _read_hkex_master_file(self, file_path: Optional[str]) -> Optional[str]:
        if not file_path:
            return None
        path = Path(str(file_path))
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8-sig")

    def _read_hkex_master_binary_file(self, file_path: Optional[str]) -> Optional[bytes]:
        if not file_path:
            return None
        path = Path(str(file_path))
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            return None
        return path.read_bytes()

    def _get_hkex_manual_review_path(self) -> Path:
        config = self._get_hkex_instrument_master_sync_config()
        file_path = config.get('manual_review_file') or 'data/hkex_manual_review.json'
        path = Path(str(file_path))
        if not path.is_absolute():
            path = Path.cwd() / path
        return path

    def _load_hkex_manual_review_entries(self) -> Tuple[Path, List[Dict[str, Any]]]:
        path = self._get_hkex_manual_review_path()
        entries: List[Dict[str, Any]] = []
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding='utf-8-sig') or '[]')
                if isinstance(payload, dict):
                    payload = payload.get('reviews') or payload.get('rows') or payload.get('data') or []
                if isinstance(payload, list):
                    entries = [item for item in payload if isinstance(item, dict)]
            except Exception as exc:
                raise ValueError(f"invalid HKEX manual review file {path}: {exc}") from exc
        return path, entries

    async def get_hkex_manual_review_evidence(self, *, limit: int = 100) -> Dict[str, Any]:
        """Return recently stored HKEX manual-review lifecycle evidence."""
        path, entries = self._load_hkex_manual_review_entries()
        bounded = entries[-max(1, min(int(limit), 1000)) :]
        return {
            'status': 'success',
            'path': str(path),
            'total': len(entries),
            'entries': bounded,
        }

    async def append_hkex_manual_review_evidence(
        self,
        *,
        instrument_id: str,
        action: str,
        effective_date: Optional[Any] = None,
        reason: str = '',
        evidence_url: str = '',
        reviewed_by: str = '',
    ) -> Dict[str, Any]:
        """Append one operator-reviewed HKEX lifecycle evidence row."""
        from data_sources.hkex_instrument_master import hkex_instrument_id, normalize_hkex_code

        normalized_code = normalize_hkex_code(instrument_id)
        normalized_id = hkex_instrument_id(normalized_code)
        if not normalized_id:
            raise ValueError("instrument_id or stock code is required")

        normalized_action = str(action or '').strip().lower()
        aliases = {
            'delist': 'delisted',
            'deactivate': 'delisted',
            'inactive': 'delisted',
            'suspend': 'suspended',
            'reactivate': 'active',
            'activate': 'active',
        }
        normalized_action = aliases.get(normalized_action, normalized_action)
        if normalized_action not in {'active', 'suspended', 'delisted'}:
            raise ValueError("action must be one of active, suspended, delisted")

        effective_date_text = ''
        if effective_date:
            if isinstance(effective_date, (datetime, date)):
                effective_date_text = effective_date.date().isoformat() if isinstance(effective_date, datetime) else effective_date.isoformat()
            else:
                effective_date_text = str(effective_date).strip()[:10]
                try:
                    datetime.fromisoformat(effective_date_text)
                except ValueError as exc:
                    raise ValueError("effective_date must use YYYY-MM-DD format") from exc

        path = self._get_hkex_manual_review_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        _, entries = self._load_hkex_manual_review_entries()
        entry = {
            'instrument_id': normalized_id,
            'code': normalized_code,
            'action': normalized_action,
            'effective_date': effective_date_text,
            'reason': str(reason or '').strip(),
            'evidence_url': str(evidence_url or '').strip(),
            'reviewed_by': str(reviewed_by or '').strip(),
            'reviewed_at': get_shanghai_time().isoformat(),
        }
        entries.append(entry)
        tmp_path = path.with_suffix(path.suffix + '.tmp')
        tmp_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        tmp_path.replace(path)
        return {
            'status': 'success',
            'path': str(path),
            'entry': entry,
            'total': len(entries),
        }

    async def _get_hkex_local_master_rows(self) -> List[Dict[str, Any]]:
        return await self.db_ops.execute_read_query(
            """
            SELECT instrument_id, symbol, name, exchange, type, status, is_active,
                   listed_date, delisted_date, source, source_symbol, currency,
                   market, industry, sector, trading_status, updated_at
            FROM instruments
            WHERE exchange = 'HKEX' AND type = 'stock'
            """
        )

    async def _get_hkex_quote_availability_rows(self, *, stale_days: int = 14) -> List[Dict[str, Any]]:
        rows = await self.db_ops.execute_read_query(
            """
            SELECT i.instrument_id, MAX(q.time) AS last_quote
            FROM instruments i
            LEFT JOIN daily_quotes q ON q.instrument_id = i.instrument_id
            WHERE i.exchange = 'HKEX' AND i.type = 'stock'
            GROUP BY i.instrument_id
            """
        )
        cutoff = get_shanghai_time().date() - timedelta(days=stale_days)
        for row in rows:
            last_quote = row.get('last_quote')
            parsed_date = self._parse_master_updated_at(last_quote)
            row['quote_stale'] = parsed_date is None or parsed_date.date() < cutoff
        return rows

    async def _fetch_hkex_instrument_master_sources(
        self,
        config: Dict[str, Any],
        *,
        timeout_sec: Optional[float],
    ) -> Dict[str, Any]:
        from data_sources.hkex_instrument_master import (
            HKEXManualReviewProvider,
            HKEXNewsStockListProvider,
            HKEXSecuritiesListProvider,
            HKEXSuspensionReportProvider,
            HKEXSupplementalAdapter,
        )

        result: Dict[str, Any] = {
            'snapshots': [],
            'official_active_rows': [],
            'official_delisted_rows': [],
            'supplemental_rows': [],
            'suspension_rows': [],
            'warnings': [],
            'errors': [],
        }
        effective_timeout = float(timeout_sec or config.get('timeout_sec') or 60)

        try:
            raw = self._read_hkex_master_file(config.get('official_securities_list_file'))
            provider = HKEXSecuritiesListProvider(
                source_url=config.get('official_securities_list_url') or ''
            )
            if raw is not None:
                snapshot = provider.parse_csv(raw)
            elif config.get('official_securities_list_url'):
                snapshot = await asyncio.to_thread(provider.fetch_csv, timeout_sec=effective_timeout)
            else:
                snapshot = None
                result['warnings'].append('HKEX official securities-list source not configured')
            if snapshot is not None:
                result['snapshots'].append(snapshot)
                result['official_active_rows'].extend(snapshot.rows)
        except Exception as exc:
            result['errors'].append(f"HKEX official securities-list fetch/parse failed: {exc}")

        try:
            raw = self._read_hkex_master_file(config.get('hkexnews_active_list_file'))
            provider = HKEXNewsStockListProvider(
                source_url=config.get('hkexnews_active_list_url') or ''
            )
            if raw is not None:
                snapshot = provider.parse_html(raw, lifecycle_status='active')
            elif config.get('hkexnews_active_list_url'):
                snapshot = await asyncio.to_thread(
                    provider.fetch_html,
                    lifecycle_status='active',
                    timeout_sec=effective_timeout,
                )
            else:
                snapshot = None
            if snapshot is not None:
                result['snapshots'].append(snapshot)
                result['official_active_rows'].extend(snapshot.rows)
        except Exception as exc:
            result['errors'].append(f"HKEXnews active-list fetch/parse failed: {exc}")

        try:
            raw = self._read_hkex_master_file(config.get('hkexnews_delisted_list_file'))
            provider = HKEXNewsStockListProvider(
                source_url=config.get('hkexnews_delisted_list_url') or ''
            )
            if raw is not None:
                snapshot = provider.parse_html(raw, lifecycle_status='delisted')
            elif config.get('hkexnews_delisted_list_url'):
                snapshot = await asyncio.to_thread(
                    provider.fetch_html,
                    lifecycle_status='delisted',
                    timeout_sec=effective_timeout,
                )
            else:
                snapshot = None
            if snapshot is not None:
                result['snapshots'].append(snapshot)
                result['official_delisted_rows'].extend(snapshot.rows)
        except Exception as exc:
            result['errors'].append(f"HKEXnews delisted-list fetch/parse failed: {exc}")

        try:
            raw = self._read_hkex_master_file(config.get('manual_review_file'))
            if raw is not None:
                snapshot = HKEXManualReviewProvider(
                    source_url=config.get('manual_review_file') or 'manual_review_file'
                ).parse(raw)
                result['snapshots'].append(snapshot)
                for row in snapshot.rows:
                    status = str(row.get('status') or '').lower()
                    if status == 'delisted':
                        result['official_delisted_rows'].append(row)
                    elif status == 'suspended':
                        result['suspension_rows'].append(row)
                        result['official_active_rows'].append(row)
                    elif status == 'active':
                        result['official_active_rows'].append(row)
        except Exception as exc:
            result['errors'].append(f"HKEX manual review evidence parse failed: {exc}")

        for config_file_key, config_url_key, market in (
            (
                'hkexnews_suspension_main_board_file',
                'hkexnews_suspension_main_board_url',
                'Main Board',
            ),
            ('hkexnews_suspension_gem_file', 'hkexnews_suspension_gem_url', 'GEM'),
        ):
            try:
                provider = HKEXSuspensionReportProvider(
                    source_url=config.get(config_url_key) or config.get(config_file_key) or '',
                    market=market,
                )
                raw_pdf = self._read_hkex_master_binary_file(config.get(config_file_key))
                if raw_pdf is not None:
                    snapshot = provider.parse_pdf(raw_pdf)
                elif config.get(config_url_key):
                    snapshot = await asyncio.to_thread(
                        provider.fetch_pdf,
                        timeout_sec=effective_timeout,
                    )
                else:
                    snapshot = None
                if snapshot is not None:
                    result['snapshots'].append(snapshot)
                    result['suspension_rows'].extend(snapshot.rows)
                    result['official_active_rows'].extend(snapshot.rows)
            except Exception as exc:
                result['warnings'].append(f"HKEX suspension report fetch/parse failed ({market}): {exc}")

        try:
            raw = self._read_hkex_master_file(config.get('akshare_spot_file'))
            if raw is not None:
                snapshot = HKEXSupplementalAdapter.parse_akshare_spot_csv(raw)
                result['snapshots'].append(snapshot)
                result['supplemental_rows'].extend(snapshot.rows)
        except Exception as exc:
            result['warnings'].append(f"AkShare HKEX supplemental fixture parse failed: {exc}")

        try:
            raw = self._read_hkex_master_file(config.get('eastmoney_profile_file'))
            if raw is not None:
                snapshot = HKEXSupplementalAdapter.parse_eastmoney_profile_csv(raw)
                result['snapshots'].append(snapshot)
                result['supplemental_rows'].extend(snapshot.rows)
        except Exception as exc:
            result['warnings'].append(f"Eastmoney HKEX supplemental fixture parse failed: {exc}")

        if config.get('fetch_supplemental_live'):
            try:
                if self.source_factory is None:
                    from data_sources.source_factory import get_data_source_factory
                    self.source_factory = await get_data_source_factory(self.db_ops)
                live_rows = await self.source_factory.get_instrument_list(
                    'HKEX',
                    force_refresh=True,
                    instrument_types=['stock'],
                )
                for row in live_rows or []:
                    item = dict(row)
                    item['lifecycle_authoritative'] = False
                    item['source'] = item.get('source') or 'hkex_supplemental_live'
                    result['supplemental_rows'].append(item)
            except Exception as exc:
                result['warnings'].append(f"HKEX supplemental live fetch failed: {exc}")

        return result

    def _merge_hkex_official_active_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        preserve_from_primary = {
            'product_type',
            'research_scope',
            'is_research_equity',
            'currency',
            'category',
            'sub_category',
            'isin',
            'canonical_instrument_id',
            'is_canonical',
            'counter_currency',
        }
        merged: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            instrument_id = row.get('instrument_id')
            if not instrument_id:
                continue
            existing = merged.get(instrument_id, {})
            combined = dict(existing)
            existing_source = existing.get('source')
            incoming_source = row.get('source')
            for key, value in row.items():
                if value in (None, ''):
                    continue
                if (
                    key in preserve_from_primary
                    and existing_source == 'hkex_securities_list'
                    and incoming_source == 'hkexnews_active_list'
                    and existing.get(key) not in (None, '')
                ):
                    continue
                combined[key] = value
            merged[instrument_id] = combined
        return list(merged.values())

    def _build_hkex_metadata_rows(
        self,
        *,
        rows: List[Dict[str, Any]],
        snapshots: List[Any],
    ) -> List[Dict[str, Any]]:
        from data_sources.hkex_instrument_master import build_dual_counter_map

        snapshot_by_source = {
            snapshot.source: snapshot
            for snapshot in snapshots or []
            if getattr(snapshot, 'source', None)
        }
        dual_map = build_dual_counter_map(rows)
        metadata_rows: List[Dict[str, Any]] = []
        for row in rows or []:
            instrument_id = row.get('instrument_id')
            if not instrument_id:
                continue
            item = dict(row)
            item.update(dual_map.get(instrument_id) or {
                'canonical_instrument_id': instrument_id,
                'is_canonical': True,
                'counter_currency': row.get('currency'),
            })
            snapshot = snapshot_by_source.get(row.get('source'))
            if snapshot is not None:
                item['raw_snapshot_hash'] = snapshot.raw_snapshot_hash
                item['parser_version'] = snapshot.parser_version
                item['source_url'] = snapshot.source_url
            metadata_rows.append(item)
        return metadata_rows

    def _filter_hkex_safe_write_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        config: Dict[str, Any],
        metadata_rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        metadata_by_id = {row.get('instrument_id'): row for row in metadata_rows if row.get('instrument_id')}
        allowed_product_types = set(config.get('allowed_product_types') or ['ordinary_equity', 'reit', 'etf'])
        valid_fields = {
            'instrument_id', 'symbol', 'name', 'exchange', 'type', 'currency',
            'listed_date', 'delisted_date', 'issue_date', 'industry', 'sector',
            'market', 'status', 'is_active', 'is_st', 'trading_status', 'source',
            'source_symbol',
        }
        safe_rows: List[Dict[str, Any]] = []
        for row in rows or []:
            instrument_id = row.get('instrument_id')
            meta = metadata_by_id.get(instrument_id, {})
            product_type = meta.get('product_type') or row.get('product_type')
            if product_type not in allowed_product_types:
                continue
            if meta.get('is_canonical') is False:
                continue
            item = {
                'instrument_id': instrument_id,
                'symbol': row.get('symbol'),
                'name': row.get('name') or instrument_id,
                'exchange': 'HKEX',
                'type': 'stock',
                'currency': row.get('currency') or meta.get('counter_currency') or 'HKD',
                'status': 'active',
                'is_active': True,
                'trading_status': 1,
                'source': row.get('source') or 'hkex_securities_list',
                'source_symbol': row.get('source_symbol') or row.get('symbol'),
            }
            if row.get('listed_date'):
                item['listed_date'] = row.get('listed_date')
            safe_rows.append({key: value for key, value in item.items() if key in valid_fields})
        return safe_rows

    async def sync_hkex_instrument_master(
        self,
        *,
        mode: Optional[str] = None,
        timeout_sec: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Refresh/audit HKEX stock master data using HKEX-specific source authority."""
        from data_sources.hkex_instrument_master import (
            HKEXLifecyclePolicy,
            HKEXSourceEvidencePolicy,
            build_quote_availability_diagnostics,
        )

        config = self._get_hkex_instrument_master_sync_config()
        started_at = get_shanghai_time()
        selected_mode = (mode or config.get('mode') or 'audit_only').strip().lower()
        if selected_mode not in {'audit_only', 'safe_write', 'lifecycle_write'}:
            selected_mode = 'audit_only'

        result: Dict[str, Any] = {
            'status': 'success',
            'started_at': started_at.isoformat(),
            'finished_at': None,
            'elapsed_sec': 0.0,
            'mode': selected_mode,
            'source_priority': [
                'hkex_securities_list',
                'hkexnews_active_list',
                'hkexnews_delisted_list',
                'akshare_eastmoney_diagnostics_only',
                'yfinance_local_quote_diagnostics_only',
            ],
            'exchanges': {},
            'summary': {
                'exchanges': ['HKEX'],
                'added_instruments': 0,
                'deactivated_instruments': 0,
                'suspended_instruments': 0,
                'reactivated_instruments': 0,
                'active_count': 0,
                'review_required': 0,
            },
            'warnings': [],
            'errors': [],
        }

        if not config.get('enabled', False):
            result.update({
                'status': 'skipped',
                'reason': 'hkex_instrument_master_sync_disabled',
                'finished_at': get_shanghai_time().isoformat(),
            })
            result['elapsed_sec'] = round((get_shanghai_time() - started_at).total_seconds(), 3)
            return result

        before = await self._get_instrument_master_snapshot('HKEX')
        local_rows = await self._get_hkex_local_master_rows()
        quote_rows = await self._get_hkex_quote_availability_rows(
            stale_days=int(config.get('quote_stale_days', 14))
        )
        source_bundle = await self._fetch_hkex_instrument_master_sources(
            config,
            timeout_sec=timeout_sec if timeout_sec is not None else config.get('timeout_sec'),
        )
        official_active_rows = self._merge_hkex_official_active_rows(
            source_bundle.get('official_active_rows') or []
        )
        official_delisted_rows = source_bundle.get('official_delisted_rows') or []
        metadata_rows = self._build_hkex_metadata_rows(
            rows=official_active_rows,
            snapshots=source_bundle.get('snapshots') or [],
        )
        decisions = HKEXLifecyclePolicy.build_decisions(
            local_rows=local_rows,
            official_active_rows=official_active_rows,
            official_delisted_rows=official_delisted_rows,
            supplemental_rows=source_bundle.get('supplemental_rows') or [],
        )
        source_evidence_policy = HKEXSourceEvidencePolicy.assess(
            snapshots=source_bundle.get('snapshots') or [],
            errors=source_bundle.get('errors') or [],
            official_active_rows=official_active_rows,
            official_delisted_rows=official_delisted_rows,
        )
        quote_diagnostics = build_quote_availability_diagnostics(
            local_rows=quote_rows,
            yfinance_rows=[],
        )
        safe_write_preview_rows = self._filter_hkex_safe_write_rows(
            decisions.get('insert_candidates', []) + decisions.get('metadata_update_candidates', []),
            config=config,
            metadata_rows=metadata_rows,
        )
        allowed_lifecycle_ids = {
            row.get('instrument_id')
            for row in self._filter_hkex_safe_write_rows(
                official_active_rows,
                config=config,
                metadata_rows=metadata_rows,
            )
            if row.get('instrument_id')
        }
        allowed_reactivation_count = sum(
            1
            for item in decisions.get('reactivation_candidates', [])
            if item.get('instrument_id') in allowed_lifecycle_ids
        )
        allowed_suspension_count = sum(
            1
            for item in decisions.get('suspension_candidates', [])
            if item.get('instrument_id') in allowed_lifecycle_ids
        )

        result['warnings'].extend(source_bundle.get('warnings') or [])
        result['errors'].extend(source_bundle.get('errors') or [])
        if not official_active_rows:
            result['warnings'].append('HKEX official active source returned no rows; lifecycle writes disabled')
        if source_evidence_policy.get('active_fallback_used'):
            result['warnings'].append(
                'HKEX primary securities-list source unavailable; using HKEXnews active fallback for audit only'
            )
        if result['errors']:
            result['status'] = 'error'
        elif result['warnings']:
            result['status'] = 'warning'

        metadata_saved = 0
        review_saved = 0
        written_rows = 0
        excluded_count = 0
        delisted_count = 0
        suspended_count = 0
        reactivated_count = 0
        if (
            selected_mode in {'safe_write', 'lifecycle_write'}
            and source_evidence_policy.get('safe_write_allowed')
            and official_active_rows
        ):
            safe_rows = safe_write_preview_rows
            if safe_rows:
                saved = await self.db_ops.save_instruments_batch(safe_rows)
                written_rows = len(safe_rows) if saved else 0
            if hasattr(self.db_ops, 'save_instrument_master_metadata_batch'):
                metadata_saved = await self.db_ops.save_instrument_master_metadata_batch(metadata_rows)
            if hasattr(self.db_ops, 'mark_instruments_excluded'):
                excluded_ids = [
                    row.get('instrument_id')
                    for row in metadata_rows
                    if row.get('instrument_id')
                    and row.get('research_scope') == 'exclude'
                ]
                excluded_count = await self.db_ops.mark_instruments_excluded(
                    excluded_ids,
                    source='hkex_product_scope_exclusion',
                )

        if (
            config.get('write_review_discrepancies', True)
            and selected_mode in {'safe_write', 'lifecycle_write'}
            and hasattr(self.db_ops, 'save_instrument_master_discrepancies')
        ):
            review_saved = await self.db_ops.save_instrument_master_discrepancies(
                decisions.get('review_required', []),
                exchange='HKEX',
                run_id=started_at.strftime('hkex_master_%Y%m%d_%H%M%S'),
            )

        if selected_mode == 'lifecycle_write' and official_active_rows:
            for item in decisions.get('delisting_candidates', []):
                if not source_evidence_policy.get('delisting_write_allowed'):
                    continue
                official = item.get('official') or {}
                updated = await self.db_ops.mark_instrument_delisted(
                    item.get('instrument_id'),
                    delisted_date=official.get('delisted_date'),
                    source=official.get('source') or 'hkexnews_delisted_list',
                )
                if updated:
                    delisted_count += 1
            for item in decisions.get('reactivation_candidates', []):
                if not source_evidence_policy.get('reactivation_write_allowed'):
                    continue
                if item.get('instrument_id') not in allowed_lifecycle_ids:
                    continue
                official = item.get('official') or {}
                if not hasattr(self.db_ops, 'mark_instrument_active'):
                    continue
                updated = await self.db_ops.mark_instrument_active(
                    item.get('instrument_id'),
                    source=official.get('source') or 'hkex_securities_list',
                    listed_date=official.get('listed_date'),
                )
                if updated:
                    reactivated_count += 1
            for item in decisions.get('suspension_candidates', []):
                if not source_evidence_policy.get('suspension_write_allowed'):
                    continue
                if item.get('instrument_id') not in allowed_lifecycle_ids:
                    continue
                official = item.get('official') or {}
                if not hasattr(self.db_ops, 'mark_instrument_suspended'):
                    continue
                updated = await self.db_ops.mark_instrument_suspended(
                    item.get('instrument_id'),
                    source=official.get('source') or 'hkex_official_suspension',
                )
                if updated:
                    suspended_count += 1

        after = await self._get_instrument_master_snapshot('HKEX')
        exchange_result = {
            'status': result['status'],
            'mode': selected_mode,
            'before': {
                'total_count': before['total_count'],
                'active_count': before['active_count'],
                'inactive_count': before['inactive_count'],
                'status_counts': before['status_counts'],
                'source_counts': before['source_counts'],
            },
            'after': {
                'total_count': after['total_count'],
                'active_count': after['active_count'],
                'inactive_count': after['inactive_count'],
                'status_counts': after['status_counts'],
                'source_counts': after['source_counts'],
            },
            'source_usage': {
                snapshot.source: snapshot.diagnostics.get('row_count', 0)
                for snapshot in source_bundle.get('snapshots') or []
            },
            'source_evidence_policy': source_evidence_policy,
            'official_active_count': len(official_active_rows),
            'official_delisted_count': len(official_delisted_rows),
            'official_suspension_count': len(source_bundle.get('suspension_rows') or []),
            'supplemental_count': len(source_bundle.get('supplemental_rows') or []),
            'decision_counts': decisions.get('counts', {}),
            'safe_write_preview_count': len(safe_write_preview_rows),
            'allowed_reactivation_count': allowed_reactivation_count,
            'allowed_suspension_count': allowed_suspension_count,
            'quote_availability': quote_diagnostics,
            'written_rows': written_rows,
            'metadata_saved': metadata_saved,
            'excluded_count': excluded_count,
            'review_discrepancies_saved': review_saved,
            'delisted_count': delisted_count,
            'suspended_count': suspended_count,
            'reactivated_count': reactivated_count,
            'review_required_samples': decisions.get('review_required', [])[:20],
            'warnings': result['warnings'],
            'errors': result['errors'],
        }
        result['exchanges']['HKEX'] = exchange_result
        result['summary'].update({
            'added_instruments': max(0, after['total_count'] - before['total_count']),
            'deactivated_instruments': delisted_count,
            'suspended_instruments': suspended_count,
            'reactivated_instruments': reactivated_count,
            'active_count': after['active_count'],
            'review_required': decisions.get('counts', {}).get('review_required', 0),
        })
        finished_at = get_shanghai_time()
        result['finished_at'] = finished_at.isoformat()
        result['elapsed_sec'] = round((finished_at - started_at).total_seconds(), 3)
        return result

    async def sync_instrument_master(
        self,
        exchanges: Optional[List[str]] = None,
        *,
        include_pytdx_validation: Optional[bool] = None,
        timeout_sec: Optional[int] = None,
        freshness_threshold_hours: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Refresh A-share stock master data and return structured diagnostics."""
        config = self._get_instrument_master_sync_config()
        sync_exchanges = exchanges or config.get('exchanges') or ['SSE', 'SZSE', 'BSE']
        sync_exchanges = [ex for ex in sync_exchanges if ex in ('SSE', 'SZSE', 'BSE')]
        include_pytdx = (
            config.get('pytdx_validation_enabled', True)
            if include_pytdx_validation is None
            else include_pytdx_validation
        )
        effective_timeout = timeout_sec if timeout_sec is not None else config.get('timeout_sec')
        freshness_hours = (
            freshness_threshold_hours
            if freshness_threshold_hours is not None
            else config.get('freshness_threshold_hours')
        )

        started_at = get_shanghai_time()
        result: Dict[str, Any] = {
            'status': 'success',
            'started_at': started_at.isoformat(),
            'finished_at': None,
            'elapsed_sec': 0.0,
            'source_priority': ['exchange_official', 'baostock', 'akshare', 'pytdx_validation_only'],
            'exchanges': {},
            'summary': {
                'exchanges': sync_exchanges,
                'added_instruments': 0,
                'deactivated_instruments': 0,
                'bse_delisting_confirmed': 0,
                'metadata_rows_saved': 0,
                'discrepancy_rows_saved': 0,
                'active_count': 0,
                'source_authority': {},
            },
            'warnings': [],
            'errors': [],
        }

        if self.source_factory is None:
            from data_sources.source_factory import get_data_source_factory
            self.source_factory = await get_data_source_factory(self.db_ops)

        for exchange in sync_exchanges:
            exchange_result: Dict[str, Any] = {
                'status': 'success',
                'before': {},
                'after': {},
                'fetched_count': 0,
                'source_usage': {},
                'source_authority': None,
                'source_diagnostics': {},
                'metadata_rows_saved': 0,
                'discrepancy_rows_saved': 0,
                'added_count': 0,
                'deactivated_count': 0,
                'added_samples': [],
                'deactivated_samples': [],
                'freshness': {},
                'pytdx_validation': None,
                'warnings': [],
                'errors': [],
            }
            try:
                before = await self._get_instrument_master_snapshot(exchange)
                fetch_coro = self.source_factory.get_instrument_list(
                    exchange,
                    force_refresh=True,
                    instrument_types=['stock'],
                )
                if effective_timeout:
                    instruments = await asyncio.wait_for(fetch_coro, timeout=effective_timeout)
                else:
                    instruments = await fetch_coro

                source_diagnostics = {}
                if hasattr(self.source_factory, 'get_last_instrument_list_diagnostics'):
                    source_diagnostics = self.source_factory.get_last_instrument_list_diagnostics(
                        exchange,
                        ['stock'],
                    )
                source_authority = (
                    source_diagnostics.get('selected_source_authority')
                    if isinstance(source_diagnostics, dict) else None
                ) or self._infer_stock_source_authority(instruments)

                bse_delisting = None
                if exchange == 'BSE':
                    bse_delisting = await self._sync_bse_delisting_status(
                        before_snapshot=before,
                        fetched_instruments=instruments or [],
                    )
                after = await self._get_instrument_master_snapshot(exchange)

                added_ids = sorted(after['all_ids'] - before['all_ids'])
                deactivated_ids = sorted(before['active_ids'] - after['active_ids'])
                source_usage = self._summarize_instrument_source_usage(instruments)
                metadata_rows_saved = 0
                discrepancy_rows_saved = 0
                metadata_saver = getattr(self.db_ops, 'save_instrument_master_metadata_batch', None)
                if instruments and callable(metadata_saver):
                    maybe_saved = metadata_saver(
                        self._build_a_share_stock_metadata_rows(
                            instruments,
                            source_diagnostics=source_diagnostics,
                        )
                    )
                    if inspect.isawaitable(maybe_saved):
                        metadata_rows_saved = await maybe_saved
                    else:
                        try:
                            metadata_rows_saved = int(maybe_saved or 0)
                        except (TypeError, ValueError):
                            metadata_rows_saved = 0

                discrepancy_rows = self._build_a_share_stock_discrepancy_rows(
                    exchange=exchange,
                    before=before,
                    instruments=instruments or [],
                    source_diagnostics=source_diagnostics,
                )
                discrepancy_saver = getattr(self.db_ops, 'save_instrument_master_discrepancies', None)
                if discrepancy_rows and callable(discrepancy_saver):
                    maybe_saved = discrepancy_saver(
                        discrepancy_rows,
                        exchange=exchange,
                        run_id=f"a_share_stock_master:{started_at.isoformat()}",
                    )
                    if inspect.isawaitable(maybe_saved):
                        discrepancy_rows_saved = await maybe_saved
                    else:
                        try:
                            discrepancy_rows_saved = int(maybe_saved or 0)
                        except (TypeError, ValueError):
                            discrepancy_rows_saved = 0

                exchange_result.update({
                    'before': {
                        'total_count': before['total_count'],
                        'active_count': before['active_count'],
                        'inactive_count': before['inactive_count'],
                        'status_counts': before['status_counts'],
                    },
                    'after': {
                        'total_count': after['total_count'],
                        'active_count': after['active_count'],
                        'inactive_count': after['inactive_count'],
                        'status_counts': after['status_counts'],
                        'source_counts': after['source_counts'],
                    },
                    'fetched_count': len(instruments or []),
                    'source_usage': source_usage,
                    'source_authority': source_authority,
                    'source_diagnostics': source_diagnostics,
                    'metadata_rows_saved': metadata_rows_saved,
                    'discrepancy_rows_saved': discrepancy_rows_saved,
                    'added_count': len(added_ids),
                    'deactivated_count': len(deactivated_ids),
                    'added_samples': added_ids[:20],
                    'deactivated_samples': deactivated_ids[:20],
                    'freshness': after['freshness'],
                    'bse_delisting': bse_delisting,
                })

                if not instruments:
                    exchange_result['status'] = 'warning'
                    exchange_result['warnings'].append('empty instrument list from primary route')
                if source_authority in {'baostock_fallback', 'akshare_fallback', 'degraded'}:
                    exchange_result['warnings'].append(
                        f"official exchange source did not provide authoritative rows; source_authority={source_authority}"
                    )
                    exchange_result['status'] = 'warning'

                if include_pytdx:
                    pytdx_result = await self._validate_instrument_master_with_pytdx(
                        exchange,
                        after['active_ids'],
                    )
                    exchange_result['pytdx_validation'] = pytdx_result
                    if pytdx_result.get('status') in ('warning', 'error'):
                        exchange_result['warnings'].extend(pytdx_result.get('warnings', []))
                        exchange_result['errors'].extend(pytdx_result.get('errors', []))
                        if exchange_result['status'] == 'success':
                            exchange_result['status'] = pytdx_result['status']

                if freshness_hours and after['freshness'].get('latest_updated_at'):
                    try:
                        latest = datetime.fromisoformat(str(after['freshness']['latest_updated_at']).replace('Z', '+00:00'))
                        if latest.tzinfo is not None:
                            latest = latest.replace(tzinfo=None)
                        age_hours = (datetime.now() - latest).total_seconds() / 3600
                        exchange_result['freshness']['age_hours'] = round(age_hours, 2)
                        if age_hours > float(freshness_hours):
                            exchange_result['warnings'].append(
                                f"master data freshness exceeds {freshness_hours}h"
                            )
                            if exchange_result['status'] == 'success':
                                exchange_result['status'] = 'warning'
                    except Exception:
                        exchange_result['warnings'].append('unable to parse master data freshness timestamp')
                        if exchange_result['status'] == 'success':
                            exchange_result['status'] = 'warning'

                result['summary']['added_instruments'] += exchange_result['added_count']
                result['summary']['deactivated_instruments'] += exchange_result['deactivated_count']
                if bse_delisting:
                    result['summary']['bse_delisting_confirmed'] += int(
                        bse_delisting.get('confirmed_count', 0) or 0
                    )
                    if bse_delisting.get('warnings'):
                        exchange_result['warnings'].extend(bse_delisting['warnings'])
                        if exchange_result['status'] == 'success':
                            exchange_result['status'] = 'warning'
                    if bse_delisting.get('errors'):
                        exchange_result['errors'].extend(bse_delisting['errors'])
                        exchange_result['status'] = 'error'
                result['summary']['active_count'] += exchange_result['after'].get('active_count', 0)
                result['summary']['metadata_rows_saved'] += metadata_rows_saved
                result['summary']['discrepancy_rows_saved'] += discrepancy_rows_saved
                if source_authority:
                    authority_counts = result['summary'].setdefault('source_authority', {})
                    authority_counts[source_authority] = authority_counts.get(source_authority, 0) + 1

            except asyncio.TimeoutError:
                exchange_result['status'] = 'error'
                exchange_result['errors'].append(f'instrument master sync timed out after {effective_timeout}s')
            except Exception as exc:
                exchange_result['status'] = 'error'
                exchange_result['errors'].append(str(exc))

            if exchange_result['warnings']:
                result['warnings'].extend([f"{exchange}: {w}" for w in exchange_result['warnings']])
            if exchange_result['errors']:
                result['errors'].extend([f"{exchange}: {e}" for e in exchange_result['errors']])
            result['exchanges'][exchange] = exchange_result

        if result['errors']:
            result['status'] = 'error'
        elif result['warnings']:
            result['status'] = 'warning'

        finished_at = get_shanghai_time()
        result['finished_at'] = finished_at.isoformat()
        result['elapsed_sec'] = round((finished_at - started_at).total_seconds(), 3)
        return result

    @staticmethod
    def _date_text(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        text = str(value).strip()
        return text[:10] if text else None

    @staticmethod
    def _date_from_any(value: Any) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            return datetime.fromisoformat(str(value)[:10]).date()
        except Exception:
            return None

    def _build_index_metadata_rows(
        self,
        rows: List[Dict[str, Any]],
        *,
        raw_snapshot_hash: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        metadata_rows: List[Dict[str, Any]] = []
        for row in rows or []:
            instrument_id = row.get('instrument_id')
            if not instrument_id:
                continue
            metadata_rows.append({
                'instrument_id': instrument_id,
                'exchange': row.get('exchange') or 'SZSE',
                'product_type': 'index',
                'research_scope': 'include',
                'canonical_instrument_id': instrument_id,
                'is_canonical': True,
                'counter_currency': row.get('currency') or 'CNY',
                'official_lifecycle_source': row.get('official_lifecycle_source'),
                'source_url': row.get('source_url'),
                'raw_snapshot_hash': raw_snapshot_hash or row.get('raw_snapshot_hash'),
                'parser_version': row.get('parser_version'),
                'metadata': row.get('metadata') or {},
            })
        return metadata_rows

    async def sync_index_master(
        self,
        exchanges: Optional[List[str]] = None,
        *,
        target_date: Optional[date] = None,
        timeout_sec: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Govern A-share index master data and lifecycle states before quote updates."""
        config = self._get_index_master_governance_config()
        started_at = get_shanghai_time()
        configured_exchanges = [str(ex).upper() for ex in (config.get('exchanges') or ['SSE', 'SZSE'])]
        sync_exchanges = [
            str(ex).upper()
            for ex in (exchanges or configured_exchanges)
            if str(ex).upper() in set(configured_exchanges)
        ]
        sample_limit = max(0, int(config.get('sample_limit', 10) or 0))
        effective_timeout = timeout_sec if timeout_sec is not None else config.get('timeout_sec')

        result: Dict[str, Any] = {
            'status': 'success',
            'action': 'index_master_governance',
            'started_at': started_at.isoformat(),
            'finished_at': None,
            'elapsed_sec': 0.0,
            'source_priority': list(config.get('official_sources') or ['cnindex', 'csindex']),
            'exchanges': {},
            'summary': {
                'exchanges': sync_exchanges,
                'master_rows_saved': 0,
                'evidence_rows_saved': 0,
                'direct_terminated_count': 0,
                'inferred_terminated_count': 0,
                'stale_no_quote_count': 0,
                'stale_no_quote_written_count': 0,
                'metadata_only_legacy_deactivated_count': 0,
                'invalid_quote_code_deactivated_count': 0,
                'reactivated_count': 0,
                'lifecycle_skip_count': 0,
                'active_count': 0,
                'ambiguous_master_duplicate_groups_skipped': 0,
                'handled_ambiguous_master_duplicate_groups': 0,
                'collapsed_duplicate_master_rows': 0,
                'csindex_active_admitted_count': 0,
                'csindex_reference_only_count': 0,
                'stock_collision_index_rows_skipped': 0,
                'source_usage': {},
                'samples': [],
                'handled_samples': [],
            },
            'warnings': [],
            'errors': [],
        }

        if not config.get('enabled', False):
            result.update({
                'status': 'skipped',
                'reason': 'disabled_by_config',
                'finished_at': get_shanghai_time().isoformat(),
            })
            return result
        if not sync_exchanges:
            result.update({
                'status': 'skipped',
                'reason': 'no_supported_exchange_in_update_scope',
                'finished_at': get_shanghai_time().isoformat(),
            })
            return result

        if self.source_factory is None:
            from data_sources.source_factory import get_data_source_factory
            self.source_factory = await get_data_source_factory(self.db_ops)

        def _source(base_name: str):
            if hasattr(self.source_factory, 'get_source_instance'):
                return self.source_factory.get_source_instance(base_name, region='a_stock')
            return self.source_factory._get_source_instance(base_name, region='a_stock')

        cnindex_source = _source('cnindex')
        csindex_source = _source('csindex')
        if cnindex_source is None:
            result['warnings'].append('cnindex source unavailable for index governance')
        if 'SSE' in sync_exchanges and csindex_source is None:
            result['warnings'].append('csindex source unavailable for SSE/CSI index governance')

        snapshots_before: Dict[str, Dict[str, Any]] = {
            exchange: await self._get_index_master_snapshot(exchange)
            for exchange in sync_exchanges
        }

        official_rows: List[Dict[str, Any]] = []
        cnindex_snapshot = None
        if cnindex_source is not None and any(ex in sync_exchanges for ex in ('SZSE', 'SSE')):
            try:
                fetch_coro = cnindex_source.get_index_master_snapshot()
                cnindex_snapshot = (
                    await asyncio.wait_for(fetch_coro, timeout=effective_timeout)
                    if effective_timeout else await fetch_coro
                )
                official_rows.extend([
                    row for row in (cnindex_snapshot.rows or [])
                    if row.get('exchange') in sync_exchanges
                ])
                result['summary']['source_usage']['cnindex'] = len(official_rows)
            except asyncio.TimeoutError:
                result['warnings'].append(f'cnindex index master fetch timed out after {effective_timeout}s')
            except Exception as exc:
                result['warnings'].append(f'cnindex index master fetch failed: {exc}')

        csindex_snapshot = None
        if csindex_source is not None and 'SSE' in sync_exchanges:
            try:
                fetch_coro = csindex_source.get_index_master_snapshot()
                csindex_snapshot = (
                    await asyncio.wait_for(fetch_coro, timeout=effective_timeout)
                    if effective_timeout else await fetch_coro
                )
                csindex_rows = [
                    row for row in (csindex_snapshot.rows or [])
                    if row.get('exchange') in sync_exchanges
                ]
                result['summary']['source_usage']['csindex'] = len(csindex_rows)
                csindex_local_quote_ids = await self._get_existing_quote_instrument_ids([
                    str(row.get('instrument_id') or '')
                    for row in csindex_rows
                ])
                for row in csindex_rows:
                    admitted, evidence = self._csindex_has_active_admission(
                        row,
                        snapshots_before=snapshots_before,
                        config=config,
                        local_quote_ids=csindex_local_quote_ids,
                    )
                    if not admitted:
                        result['summary']['csindex_reference_only_count'] += 1
                        if len(result['summary']['handled_samples']) < sample_limit:
                            result['summary']['handled_samples'].append({
                                'instrument_id': row.get('instrument_id'),
                                'state': 'csindex_reference_only',
                                'source': 'csindex',
                                'reason': evidence,
                            })
                        continue
                    admitted_row = dict(row)
                    admitted_row['admission_evidence'] = evidence
                    metadata = dict(admitted_row.get('metadata') or {})
                    metadata['admission_evidence'] = evidence
                    admitted_row['metadata'] = metadata
                    official_rows.append(admitted_row)
                    result['summary']['csindex_active_admitted_count'] += 1
            except asyncio.TimeoutError:
                result['warnings'].append(f'csindex index master fetch timed out after {effective_timeout}s')
            except Exception as exc:
                result['warnings'].append(f'csindex index master fetch failed: {exc}')

        if official_rows:
            admission_result = self._apply_index_master_admission_rules(
                official_rows,
                config=config,
                sample_limit=sample_limit,
            )
            ambiguous_groups = admission_result['ambiguous_groups']
            if admission_result['missing_key_count']:
                result['warnings'].append(
                    f"cnindex index master snapshot has {admission_result['missing_key_count']} rows missing admission key"
                )
            if admission_result['collapsed_duplicate_rows']:
                result['summary']['collapsed_duplicate_master_rows'] += admission_result['collapsed_duplicate_rows']
            handled_groups = admission_result.get('handled_ambiguous_groups') or []
            if handled_groups:
                result['summary']['handled_ambiguous_master_duplicate_groups'] += len(handled_groups)
                for group in handled_groups[:sample_limit]:
                    if len(result['summary']['handled_samples']) >= sample_limit:
                        break
                    result['summary']['handled_samples'].append({
                        'instrument_id': group.get('key'),
                        'state': 'handled_ambiguous_master_duplicate',
                        'classification': group.get('classification'),
                        'row_count': group.get('row_count'),
                    })
            if ambiguous_groups:
                result['summary']['ambiguous_master_duplicate_groups_skipped'] += len(ambiguous_groups)
                sample_ids = ','.join(
                    sorted(str(group['key']) for group in ambiguous_groups)[:sample_limit or 5]
                )
                result['warnings'].append(
                    f'index master admission has {len(ambiguous_groups)} unhandled ambiguous duplicate key groups: {sample_ids}'
                )
            official_rows = admission_result['rows']
            official_rows = await self._filter_index_rows_colliding_with_stock_ids(
                official_rows,
                result=result,
                sample_limit=sample_limit,
            )
            if official_rows:
                saved = await self.db_ops.save_instruments_batch(official_rows)
                if saved:
                    result['summary']['master_rows_saved'] += len(official_rows)
                else:
                    result['warnings'].append('index master rows were not saved')
                metadata_rows = self._build_index_metadata_rows(
                    official_rows,
                    raw_snapshot_hash=(
                        getattr(cnindex_snapshot, 'raw_snapshot_hash', None)
                        or getattr(csindex_snapshot, 'raw_snapshot_hash', None)
                    ),
                )
                result['summary']['metadata_rows_saved'] = await self.db_ops.save_instrument_master_metadata_batch(
                    metadata_rows
                )
                legacy_metadata_only_rows: List[Dict[str, Any]] = []
                seen_legacy_metadata_ids: Set[str] = set()
                for row in official_rows:
                    if str(row.get('exchange') or '').upper() != 'SZSE':
                        continue
                    if str(row.get('source') or '').lower() != 'cnindex':
                        continue
                    metadata = row.get('metadata') or {}
                    if metadata.get('szse_quote_code'):
                        continue
                    code = str(row.get('source_symbol') or row.get('symbol') or '').strip()
                    if len(code) != 6 or not code.isdigit():
                        continue
                    legacy_id = f'{code}.SZ'
                    if legacy_id == row.get('instrument_id'):
                        continue
                    before_row = (
                        snapshots_before.get('SZSE', {})
                        .get('rows_by_id', {})
                        .get(legacy_id)
                    )
                    if not before_row:
                        continue
                    if str(before_row.get('source') or '').lower() != 'cnindex':
                        continue
                    seen_legacy_metadata_ids.add(legacy_id)
                    legacy_metadata_only_rows.append({
                        'instrument_id': legacy_id,
                        'symbol': code,
                        'exchange': 'SZSE',
                        'lifecycle_state': 'metadata_only',
                        'event_type': 'cnindex_metadata_only_identity',
                        'confidence': 'official_master_metadata_only',
                        'source': 'cnindex_index_list',
                        'parser_version': row.get('parser_version'),
                        'raw_snapshot_hash': getattr(cnindex_snapshot, 'raw_snapshot_hash', None),
                        'diagnostics': {
                            'canonical_metadata_instrument_id': row.get('instrument_id'),
                            'name': row.get('name'),
                            'market': row.get('market'),
                            'industry': row.get('industry'),
                            'sector': row.get('sector'),
                            'cni_code': metadata.get('cni_code'),
                        },
                    })
                persisted_metadata_only_rows = await self.db_ops.execute_read_query(
                    """
                    SELECT i.instrument_id, i.symbol, i.name, i.exchange, i.market,
                           i.industry, i.sector, i.source, m.metadata_json
                    FROM instruments i
                    JOIN instrument_master_metadata m
                      ON m.instrument_id = i.instrument_id
                    WHERE i.exchange = 'SZSE'
                      AND i.type = 'index'
                      AND i.is_active = 1
                      AND COALESCE(i.trading_status, 1) = 1
                      AND lower(COALESCE(i.source, '')) IN ('cnindex', 'cnindex_index_list')
                      AND i.instrument_id = i.symbol || '.SZ'
                      AND COALESCE(json_extract(m.metadata_json, '$.metadata.szse_quote_code'), '') = ''
                      AND COALESCE(json_extract(m.metadata_json, '$.metadata.cni_code'), '') != ''
                    """
                )
                for persisted in persisted_metadata_only_rows or []:
                    legacy_id = persisted.get('instrument_id')
                    if not legacy_id or legacy_id in seen_legacy_metadata_ids:
                        continue
                    try:
                        metadata_payload = json.loads(persisted.get('metadata_json') or '{}')
                    except Exception:
                        metadata_payload = {}
                    metadata = metadata_payload.get('metadata') if isinstance(metadata_payload, dict) else {}
                    if not isinstance(metadata, dict):
                        metadata = {}
                    cni_code = metadata.get('cni_code')
                    seen_legacy_metadata_ids.add(legacy_id)
                    legacy_metadata_only_rows.append({
                        'instrument_id': legacy_id,
                        'symbol': persisted.get('symbol'),
                        'exchange': persisted.get('exchange') or 'SZSE',
                        'lifecycle_state': 'metadata_only',
                        'event_type': 'cnindex_metadata_only_identity',
                        'confidence': 'official_master_metadata_only',
                        'source': 'cnindex_index_list',
                        'parser_version': metadata_payload.get('parser_version') if isinstance(metadata_payload, dict) else None,
                        'raw_snapshot_hash': metadata_payload.get('raw_snapshot_hash') if isinstance(metadata_payload, dict) else None,
                        'diagnostics': {
                            'canonical_metadata_instrument_id': cni_code,
                            'name': persisted.get('name'),
                            'market': persisted.get('market') or metadata.get('index_family'),
                            'industry': persisted.get('industry') or metadata.get('index_category'),
                            'sector': persisted.get('sector') or metadata.get('coverage_scope'),
                            'cni_code': cni_code,
                        },
                    })
                if legacy_metadata_only_rows:
                    result['summary']['evidence_rows_saved'] += await self.db_ops.save_index_lifecycle_evidence(
                        legacy_metadata_only_rows
                    )
                    for row in legacy_metadata_only_rows:
                        ok = await self.db_ops.mark_index_lifecycle_state(
                            row.get('instrument_id'),
                            lifecycle_state='metadata_only',
                            source='cnindex_index_list',
                        )
                        if ok:
                            result['summary']['metadata_only_legacy_deactivated_count'] += 1
                            if len(result['summary']['samples']) < sample_limit:
                                result['summary']['samples'].append({
                                    'instrument_id': row.get('instrument_id'),
                                    'state': 'metadata_only',
                                    'confidence': row.get('confidence'),
                                    'canonical_metadata_instrument_id': row.get('diagnostics', {}).get('canonical_metadata_instrument_id'),
                                })
                invalid_quote_code_rows: List[Dict[str, Any]] = []
                for instrument_id, before_row in (
                    snapshots_before.get('SZSE', {}).get('rows_by_id', {}) or {}
                ).items():
                    if str(before_row.get('source') or '').lower() not in {'cnindex', 'cnindex_index_list'}:
                        continue
                    if before_row.get('is_active') not in (True, 1, '1', 'true', 'True'):
                        continue
                    if before_row.get('trading_status') in (0, '0', False):
                        continue
                    if not str(instrument_id).upper().endswith('.SZ'):
                        continue
                    quote_symbol = str(instrument_id).rsplit('.', 1)[0]
                    if len(quote_symbol) == 6 and quote_symbol.isdigit():
                        continue
                    invalid_quote_code_rows.append({
                        'instrument_id': instrument_id,
                        'symbol': before_row.get('symbol') or quote_symbol,
                        'exchange': 'SZSE',
                        'lifecycle_state': 'metadata_only',
                        'event_type': 'cnindex_invalid_quote_code_identity',
                        'confidence': 'invalid_exchange_quote_code',
                        'source': 'cnindex_index_list',
                        'parser_version': 'official-index-source-v1',
                        'diagnostics': {
                            'name': before_row.get('name'),
                            'source': before_row.get('source'),
                            'quote_symbol': quote_symbol,
                            'reason': 'szse_quote_code_must_be_six_digits',
                        },
                    })
                if invalid_quote_code_rows:
                    result['summary']['evidence_rows_saved'] += await self.db_ops.save_index_lifecycle_evidence(
                        invalid_quote_code_rows
                    )
                    for row in invalid_quote_code_rows:
                        ok = await self.db_ops.mark_index_lifecycle_state(
                            row.get('instrument_id'),
                            lifecycle_state='metadata_only',
                            source='cnindex_index_list',
                        )
                        if ok:
                            result['summary']['invalid_quote_code_deactivated_count'] += 1
                            if len(result['summary']['samples']) < sample_limit:
                                result['summary']['samples'].append({
                                    'instrument_id': row.get('instrument_id'),
                                    'state': 'metadata_only',
                                    'confidence': row.get('confidence'),
                                    'reason': row.get('diagnostics', {}).get('reason'),
                                })

        evidence_rows: List[Dict[str, Any]] = []
        if cnindex_source is not None:
            try:
                fetch_coro = cnindex_source.get_lifecycle_evidence()
                evidence_rows = (
                    await asyncio.wait_for(fetch_coro, timeout=effective_timeout)
                    if effective_timeout else await fetch_coro
                )
            except asyncio.TimeoutError:
                result['warnings'].append(f'cnindex lifecycle evidence fetch timed out after {effective_timeout}s')
            except Exception as exc:
                result['warnings'].append(f'cnindex lifecycle evidence fetch failed: {exc}')

        direct_evidence = [
            row for row in evidence_rows
            if row.get('exchange') in sync_exchanges
            and row.get('lifecycle_state') == 'calculation_terminated'
        ]
        if direct_evidence:
            result['summary']['evidence_rows_saved'] += await self.db_ops.save_index_lifecycle_evidence(
                direct_evidence
            )

        direct_applied = 0
        for row in direct_evidence:
            ok = await self.db_ops.mark_index_lifecycle_state(
                row.get('instrument_id'),
                lifecycle_state='calculation_terminated',
                source=row.get('source') or 'cnindex_announcement',
                effective_date=row.get('effective_date'),
                last_quote_date=row.get('last_quote_date'),
            )
            if ok:
                direct_applied += 1
                if len(result['summary']['samples']) < sample_limit:
                    result['summary']['samples'].append({
                        'instrument_id': row.get('instrument_id'),
                        'state': 'calculation_terminated',
                        'confidence': row.get('confidence') or 'direct',
                        'effective_date': self._date_text(row.get('effective_date')),
                        'evidence_url': row.get('evidence_url'),
                    })
        result['summary']['direct_terminated_count'] = direct_applied

        inferred_rows: List[Dict[str, Any]] = []
        if config.get('allow_series_inference', True):
            current_ids: Set[str] = set()
            for exchange in sync_exchanges:
                current_ids.update((await self._get_index_master_snapshot(exchange))['all_ids'])
            for row in direct_evidence:
                code = str(row.get('matched_code') or row.get('symbol') or '').strip()
                if len(code) != 6 or not code.startswith('9'):
                    continue
                paired_code = '4' + code[1:]
                paired_id = f'{paired_code}.SZ'
                if paired_id not in current_ids and not any(item.get('instrument_id') == paired_id for item in official_rows):
                    continue
                latest_quote = await self.db_ops.get_latest_quote_date(paired_id)
                effective_date = self._date_from_any(row.get('effective_date'))
                latest_date = self._date_from_any(latest_quote)
                if effective_date is None or latest_date is None:
                    continue
                if latest_date > effective_date:
                    continue
                inferred_rows.append({
                    'instrument_id': paired_id,
                    'symbol': paired_code,
                    'exchange': 'SZSE',
                    'lifecycle_state': 'calculation_terminated',
                    'event_type': 'series_inferred_calculation_terminated',
                    'effective_date': effective_date,
                    'last_quote_date': latest_date,
                    'announcement_date': row.get('announcement_date'),
                    'announcement_title': row.get('announcement_title'),
                    'evidence_url': row.get('evidence_url'),
                    'matched_code': code,
                    'confidence': 'series_inferred',
                    'source': 'cnindex_announcement_series_inference',
                    'parser_version': row.get('parser_version'),
                    'raw_snapshot_hash': row.get('raw_snapshot_hash'),
                    'diagnostics': {
                        'direct_code': code,
                        'paired_code': paired_code,
                        'latest_quote_date': latest_date.isoformat(),
                    },
                })

        if inferred_rows:
            result['summary']['evidence_rows_saved'] += await self.db_ops.save_index_lifecycle_evidence(
                inferred_rows
            )
        inferred_applied = 0
        for row in inferred_rows:
            ok = await self.db_ops.mark_index_lifecycle_state(
                row.get('instrument_id'),
                lifecycle_state='calculation_terminated',
                source=row.get('source') or 'cnindex_announcement_series_inference',
                effective_date=row.get('effective_date'),
                last_quote_date=row.get('last_quote_date'),
            )
            if ok:
                inferred_applied += 1
                if len(result['summary']['samples']) < sample_limit:
                    result['summary']['samples'].append({
                        'instrument_id': row.get('instrument_id'),
                        'state': 'calculation_terminated',
                        'confidence': 'series_inferred',
                        'effective_date': self._date_text(row.get('effective_date')),
                        'last_quote_date': self._date_text(row.get('last_quote_date')),
                    })
        result['summary']['inferred_terminated_count'] = inferred_applied

        stale_rows: List[Dict[str, Any]] = []
        stale_days = int(config.get('stale_no_quote_trading_days', 10) or 10)
        reference_date = target_date or get_shanghai_time().date()
        cutoff_date = reference_date - timedelta(days=stale_days)
        for exchange in sync_exchanges:
            snapshot = await self._get_index_master_snapshot(exchange)
            for instrument_id in sorted(snapshot['active_ids']):
                latest_quote = await self.db_ops.get_latest_quote_date(instrument_id)
                latest_date = self._date_from_any(latest_quote)
                if latest_date is None or latest_date >= cutoff_date:
                    continue
                stale_rows.append({
                    'instrument_id': instrument_id,
                    'symbol': (snapshot['rows_by_id'].get(instrument_id) or {}).get('symbol'),
                    'exchange': exchange,
                    'lifecycle_state': 'stale_no_quote',
                    'event_type': 'stale_no_quote',
                    'last_quote_date': latest_date,
                    'confidence': 'quote_gap',
                    'source': 'local_quote_freshness',
                    'diagnostics': {
                        'cutoff_date': cutoff_date.isoformat(),
                        'reference_date': reference_date.isoformat(),
                    },
                })

        result['summary']['stale_no_quote_count'] = len(stale_rows)
        if stale_rows and config.get('write_stale_no_quote', False):
            result['summary']['evidence_rows_saved'] += await self.db_ops.save_index_lifecycle_evidence(
                stale_rows
            )
            for row in stale_rows:
                ok = await self.db_ops.mark_index_lifecycle_state(
                    row.get('instrument_id'),
                    lifecycle_state='stale_no_quote',
                    source='local_quote_freshness',
                    last_quote_date=row.get('last_quote_date'),
                )
                if ok:
                    result['summary']['stale_no_quote_written_count'] += 1
        for row in stale_rows[:sample_limit]:
            if len(result['summary']['samples']) >= sample_limit:
                break
            result['summary']['samples'].append({
                'instrument_id': row.get('instrument_id'),
                'state': 'stale_no_quote',
                'confidence': row.get('confidence'),
                'last_quote_date': self._date_text(row.get('last_quote_date')),
            })

        reactivated = 0
        if cnindex_source is not None:
            stale_marked = await self.db_ops.execute_read_query(
                """
                SELECT instrument_id, symbol
                FROM instruments
                WHERE type = 'index' AND status = 'stale_no_quote'
                ORDER BY updated_at ASC
                LIMIT :limit
                """,
                {'limit': sample_limit},
            )
            for row in stale_marked:
                instrument_id = row.get('instrument_id')
                symbol = row.get('symbol') or instrument_id
                try:
                    rows = await cnindex_source.get_daily_data(
                        instrument_id,
                        symbol,
                        datetime.combine(reference_date, datetime.min.time()),
                        datetime.combine(reference_date, datetime.max.time()),
                        instrument_type='index',
                    )
                except Exception:
                    rows = []
                if rows:
                    ok = await self.db_ops.mark_index_lifecycle_state(
                        instrument_id,
                        lifecycle_state='active_quote',
                        source='cnindex_quote_recheck',
                        last_quote_date=reference_date,
                    )
                    if ok:
                        reactivated += 1
        result['summary']['reactivated_count'] = reactivated

        for exchange in sync_exchanges:
            after = await self._get_index_master_snapshot(exchange)
            before = snapshots_before.get(exchange, {})
            result['summary']['active_count'] += int(after.get('active_count', 0) or 0)
            result['exchanges'][exchange] = {
                'status': 'success',
                'before': {
                    'total_count': before.get('total_count', 0),
                    'active_count': before.get('active_count', 0),
                    'status_counts': before.get('status_counts', {}),
                },
                'after': {
                    'total_count': after.get('total_count', 0),
                    'active_count': after.get('active_count', 0),
                    'inactive_count': after.get('inactive_count', 0),
                    'status_counts': after.get('status_counts', {}),
                    'source_counts': after.get('source_counts', {}),
                },
                'warnings': [],
                'errors': [],
            }

        result['summary']['lifecycle_skip_count'] = (
            result['summary']['direct_terminated_count']
            + result['summary']['inferred_terminated_count']
            + result['summary']['stale_no_quote_written_count']
            + result['summary']['metadata_only_legacy_deactivated_count']
            + result['summary']['invalid_quote_code_deactivated_count']
        )
        if result['warnings']:
            result['status'] = 'warning'
        if result['errors']:
            result['status'] = 'error'
        finished_at = get_shanghai_time()
        result['finished_at'] = finished_at.isoformat()
        result['elapsed_sec'] = round((finished_at - started_at).total_seconds(), 3)
        return result

    async def _maybe_sync_instrument_master_before_daily_update(
        self,
        exchanges: List[str],
        target_date: date,
        instrument_types: Optional[List[str]] = None,
        job_name: str = 'daily_data_update',
    ) -> Dict[str, Any]:
        """Compatibility wrapper for daily update master governance."""
        config = self._get_instrument_master_sync_config()
        force_refresh_job_names = self._get_instrument_master_force_refresh_job_names()

        if not config.get('enabled', True) or not config.get('run_before_daily_update', True):
            return {
                'status': 'skipped',
                'reason': 'disabled_by_config',
                'exchanges': {},
                'warnings': [],
                'errors': [],
            }

        local_today = get_shanghai_time().date()
        current_retry_start = local_today - timedelta(days=1)
        force_refresh = (
            'daily_data_update' in force_refresh_job_names
            and target_date >= current_retry_start
        )

        result = await self.run_master_governance_for_job(
            job_name=job_name,
            exchanges=exchanges,
            instrument_types=instrument_types or ['stock', 'index'],
            job_type='current',
            target_date=target_date,
            force_refresh=force_refresh,
            include_pytdx_validation=config.get('pytdx_validation_enabled', True),
            continue_on_failure=config.get('continue_on_failure', True),
        )
        if result.get('reason') == 'historical_current_master_governance_skipped':
            result['reason'] = 'historical_backfill_current_master_sync_skipped'
        return result

    def _get_daily_update_catchup_config(self) -> Dict[str, Any]:
        """Return bounded catch-up settings for normal daily quote updates."""
        defaults = {
            'enabled': True,
            'exchanges': ['SSE', 'SZSE', 'BSE'],
            'new_instrument_catchup_days': 10,
            'short_gap_catchup_days': 5,
            'sample_limit': 10,
        }
        raw = self.data_config.get('daily_update_catchup', {})
        if not isinstance(raw, dict):
            raw = {}
        cfg = {**defaults, **raw}
        try:
            cfg['new_instrument_catchup_days'] = max(
                0,
                int(cfg.get('new_instrument_catchup_days', defaults['new_instrument_catchup_days'])),
            )
        except (TypeError, ValueError):
            cfg['new_instrument_catchup_days'] = defaults['new_instrument_catchup_days']
        try:
            cfg['short_gap_catchup_days'] = max(
                0,
                int(cfg.get('short_gap_catchup_days', defaults['short_gap_catchup_days'])),
            )
        except (TypeError, ValueError):
            cfg['short_gap_catchup_days'] = defaults['short_gap_catchup_days']
        try:
            cfg['sample_limit'] = max(0, int(cfg.get('sample_limit', defaults['sample_limit'])))
        except (TypeError, ValueError):
            cfg['sample_limit'] = defaults['sample_limit']
        cfg['exchanges'] = {
            str(exchange).strip().upper()
            for exchange in cfg.get('exchanges', defaults['exchanges'])
            if str(exchange).strip()
        }
        cfg['enabled'] = bool(cfg.get('enabled', True))
        return cfg

    @staticmethod
    def _coerce_date(value: Any) -> Optional[date]:
        """Normalize common DB/config date values to ``date``."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            return pd.to_datetime(value).date()
        except Exception:
            return None

    def _resolve_daily_update_fetch_window(
        self,
        *,
        exchange: str,
        target_date: date,
        latest_quote_date: Optional[datetime],
        listed_date: Optional[Any],
        instrument_type: str = 'stock',
        catchup_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Resolve the per-instrument daily quote fetch window and catch-up reason."""
        exchange_code = str(exchange or '').upper()
        is_a_stock = exchange_code in ('SSE', 'SZSE', 'BSE')
        is_stock = str(instrument_type or '').lower() == 'stock'
        normal_start = target_date - timedelta(days=1)
        if not is_a_stock:
            normal_start = DateUtils.get_previous_trading_day(exchange_code, target_date)

        fetch_start = normal_start
        reason = 'normal_daily_window'
        capped = False
        skipped_reason = None
        listed = self._coerce_date(listed_date)
        latest = self._coerce_date(latest_quote_date)

        catchup_enabled = (
            catchup_config.get('enabled', True)
            and exchange_code in catchup_config.get('exchanges', set())
            and is_stock
        )
        if catchup_enabled and latest is None:
            if listed and listed <= target_date:
                lower_bound = target_date - timedelta(
                    days=int(catchup_config.get('new_instrument_catchup_days', 10))
                )
                fetch_start = max(listed, lower_bound)
                reason = 'new_instrument_catchup'
                capped = listed < lower_bound
            elif listed and listed > target_date:
                skipped_reason = 'listed_after_target_date'
            else:
                skipped_reason = 'missing_listed_date'
        elif catchup_enabled and latest is not None and latest < normal_start:
            lower_bound = target_date - timedelta(
                days=int(catchup_config.get('short_gap_catchup_days', 5))
            )
            fetch_start = max(latest, lower_bound)
            reason = 'short_gap_catchup'
            capped = latest < lower_bound

        if listed and fetch_start < listed:
            fetch_start = listed

        return {
            'fetch_start_date': fetch_start,
            'end_date': target_date,
            'reason': reason,
            'capped': capped,
            'listed_date': listed,
            'latest_quote_date': latest,
            'skipped_reason': skipped_reason,
        }

    async def _ensure_research_job_instrument_master_governance(
        self,
        *,
        exchanges: Optional[List[str]],
        job_name: str,
        job_type: str = 'current',
    ) -> Dict[str, Any]:
        """Run shared master governance before research jobs resolve universes."""
        force_refresh_job_names = self._get_instrument_master_force_refresh_job_names()
        return await self.run_master_governance_for_job(
            job_name=job_name,
            exchanges=exchanges,
            instrument_types=['stock'],
            job_type=job_type,
            force_refresh=job_name in force_refresh_job_names,
        )

    def _attach_instrument_master_governance(
        self,
        result: Dict[str, Any],
        governance: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Attach governance diagnostics without changing domain-specific fields."""
        if isinstance(result, dict) and governance is not None:
            result['instrument_master_governance'] = governance
        return result

    async def resolve_hkex_current_universe(
        self,
        *,
        governance: Optional[Dict[str, Any]] = None,
        ensure_governance: bool = True,
        allowed_product_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Resolve the shared HKEX current research universe after master governance."""
        if governance is None and ensure_governance:
            governance = await self.run_master_governance_for_job(
                job_name='hkex_current_universe_resolver',
                exchanges=['HKEX'],
                instrument_types=['stock'],
                job_type='current',
            )

        instruments = await self.db_ops.get_active_instruments(
            'HKEX',
            instrument_types=['stock'],
        )
        allowed_types = set(
            allowed_product_types
            or self._get_hkex_instrument_master_sync_config().get(
                'allowed_product_types',
                ['ordinary_equity', 'reit', 'etf'],
            )
        )

        metadata_rows = await self.db_ops.execute_read_query(
            """
            SELECT instrument_id, product_type, research_scope,
                   canonical_instrument_id, is_canonical, counter_currency
            FROM instrument_master_metadata
            WHERE exchange = 'HKEX'
            """
        )
        metadata_by_id = {
            row.get('instrument_id'): row
            for row in metadata_rows or []
            if row.get('instrument_id')
        }

        resolved: List[Dict[str, Any]] = []
        excluded_count = 0
        for instrument in instruments or []:
            item = dict(instrument)
            metadata = metadata_by_id.get(item.get('instrument_id'))
            if metadata:
                product_type = metadata.get('product_type')
                is_canonical = metadata.get('is_canonical') in (True, 1, '1')
                if product_type not in allowed_types or not is_canonical:
                    excluded_count += 1
                    continue
                item['hkex_master_metadata'] = metadata
            resolved.append(item)

        warnings: List[str] = []
        readiness = 'ready'
        governance_status = governance.get('status') if isinstance(governance, dict) else None
        if governance_status in {'warning', 'error', 'skipped'}:
            readiness = 'degraded'
            warnings.append(f"HKEX master governance status is {governance_status}")
        if not metadata_rows:
            readiness = 'degraded'
            warnings.append('HKEX product metadata unavailable; falling back to active instruments')

        return {
            'status': 'success',
            'exchange': 'HKEX',
            'readiness': readiness,
            'instruments': resolved,
            'instrument_count': len(resolved),
            'excluded_count': excluded_count,
            'governance': governance,
            'warnings': warnings,
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

    async def _batch_sync_adjustment_factors(
        self,
        exchange: str,
        stocks: List[Dict[str, Any]],
        progress_log_every: int = 500,
        skip_filter: bool = False,
        sync_reason: str = 'daily',
    ) -> Dict[str, int]:
        """批量同步复权因子（Phase 2）

        在日线更新全部完成后统一调用，避免与日线获取竞争限流窗口。
        每只股票独立获取，单品种失败不影响其他品种。

        Args:
            exchange: 交易所代码
            stocks: 需要同步的品种列表，每项包含 instrument_id, symbol, start_date, end_date
            progress_log_every: 每处理多少只品种输出一次进度日志

        Returns:
            {'synced': int, 'skipped': int, 'failed': int, 'filtered_total': int}
        """
        total = len(stocks)
        result = {'synced': 0, 'skipped': 0, 'failed': 0, 'filtered_total': total}

        daily_sync_enabled = self.config.get_nested(
            f'routing.factor.{exchange}.daily_sync_enabled',
            True,
        )
        if sync_reason == 'daily' and not daily_sync_enabled:
            dm_logger.info(
                "[DataManager] Phase 2: factor sync disabled for %s in daily mode; "
                "skipping %d stocks",
                exchange, total
            )
            result['skipped'] = total
            return result

        if not skip_filter:
            # 港股/美股暂不支持精准除权除息查询 (stock_fhps_em 仅限 A 股)
            # 自动切换为全量同步模式
            if exchange not in ('SSE', 'SZSE', 'BSE'):
                dm_logger.info(
                    "[DataManager] Phase 2: exchange=%s not A-stock, "
                    "skipping ex-dividend filter, using full sync",
                    exchange
                )
                skip_filter = True

        if not skip_filter:
            # ★ 精准筛选：查询当天有除权除息的股票代码，仅对这些股票同步因子
            target_dates = self._build_factor_target_dates(stocks)

            ex_div_symbols = await self._query_ex_dividend_symbols(target_dates)

            if ex_div_symbols is not None:
                # 精准模式：仅同步有除权除息事件的品种
                filtered_stocks = [
                    s for s in stocks if s['symbol'] in ex_div_symbols
                ]
                dm_logger.info(
                    "[DataManager] Phase 2: %s ex-dividend filter: %d/%d stocks have events on %s",
                    exchange, len(filtered_stocks), total,
                    ','.join(str(d) for d in sorted(target_dates))
                )
                if not filtered_stocks:
                    dm_logger.info(
                        "[DataManager] %s factor sync skipped: no ex-dividend events today",
                        exchange
                    )
                    result['skipped'] = total
                    return result
                stocks = filtered_stocks
                result['filtered_total'] = len(stocks)
            else:
                # AkShare 查询失败，跳过本次因子同步，由周维护补充
                dm_logger.warning(
                    "[DataManager] Phase 2: ex-dividend query failed, skipping factor sync for %s "
                    "(will be handled by weekly maintenance)", exchange
                )
                result['skipped'] = total
                return result

        dm_logger.info(
            "[DataManager] Phase 2: Syncing adjustment factors for %s (%d stocks)",
            exchange, len(stocks)
        )

        for idx, stock in enumerate(stocks, start=1):
            try:
                factors = await self.source_factory.get_adjustment_factors(
                    exchange,
                    stock['instrument_id'],
                    stock['symbol'],
                    datetime.combine(stock['start_date'], datetime.min.time()),
                    datetime.combine(stock['end_date'], datetime.max.time()),
                )
                if factors:
                    await self.db_ops.save_adjustment_factors(factors)
                    result['synced'] += 1
                else:
                    result['skipped'] += 1
            except Exception as e:
                result['failed'] += 1
                dm_logger.debug(
                    "[DataManager] Factor sync failed for %s: %s",
                    stock['symbol'], e
                )

            if progress_log_every and idx % progress_log_every == 0:
                dm_logger.info(
                    "[DataManager] Factor sync progress %s: %d/%d (synced=%d, failed=%d)",
                    exchange, idx, len(stocks), result['synced'], result['failed']
                )

        dm_logger.info(
            "[DataManager] %s factor sync completed: synced=%d, skipped=%d, failed=%d",
            exchange, result['synced'], result['skipped'], result['failed']
        )
        return result

    @staticmethod
    def _build_factor_target_dates(stocks: List[Dict[str, Any]]) -> set:
        """Build the ex-dividend dates covered by a factor sync request."""
        target_dates = set()
        min_start: Optional[date] = None
        max_end: Optional[date] = None

        for stock in stocks:
            start = stock.get('start_date')
            end = stock.get('end_date')
            if isinstance(start, datetime):
                start = start.date()
            if isinstance(end, datetime):
                end = end.date()
            if start is not None:
                min_start = start if min_start is None else min(min_start, start)
            if end is not None:
                max_end = end if max_end is None else max(max_end, end)

        if min_start is None or max_end is None or max_end < min_start:
            return target_dates

        span_days = (max_end - min_start).days
        if span_days <= 366:
            return {min_start + timedelta(days=offset) for offset in range(span_days + 1)}

        dm_logger.warning(
            "[DataManager] Phase 2: factor date range %s~%s is too wide for "
            "precise ex-dividend filtering; using boundary dates only",
            min_start, max_end
        )
        return {min_start, max_end}

    async def _query_ex_dividend_symbols(
        self, target_dates: set
    ) -> Optional[set]:
        """通过 AkShare 批量查询指定日期有除权除息的股票代码

        使用 stock_fhps_em 接口获取全市场分红方案，筛选除权除息日
        匹配 target_dates 的股票。仅需 2~3 次 API 调用即可覆盖全年。

        Args:
            target_dates: 需要检查的日期集合

        Returns:
            匹配的股票代码集合(纯数字), 或 None 表示查询失败
        """
        try:
            import akshare as ak
            import pandas as pd

            # 查询最近两个报告期的分红方案（覆盖年报和中报）
            target_date_strs = {
                d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
                for d in target_dates if d is not None
            }
            if not target_date_strs:
                return set()

            # 根据目标日期推算需查询的报告期。区间补数可能跨年，不能只取
            # 一个 sample date，否则会漏掉另一年的分红方案。
            years = sorted({
                d.year if hasattr(d, 'year') else int(str(d)[:4])
                for d in target_dates if d is not None
            })
            report_periods = sorted({
                period
                for year in years
                for period in (f"{year - 1}1231", f"{year}0630")
            })

            all_records = []
            for period in report_periods:
                try:
                    df = await asyncio.to_thread(ak.stock_fhps_em, date=period)
                    if df is not None and '除权除息日' in df.columns:
                        valid = df[df['除权除息日'].notna()][['代码', '除权除息日']]
                        all_records.append(valid)
                        dm_logger.debug(
                            "[DataManager] stock_fhps_em(%s): %d records with ex-dividend dates",
                            period, len(valid)
                        )
                except Exception as e:
                    dm_logger.debug(
                        "[DataManager] stock_fhps_em(%s) failed: %s", period, e
                    )

            if not all_records:
                dm_logger.warning("[DataManager] No ex-dividend data available from AkShare")
                return None

            merged = pd.concat(all_records, ignore_index=True)
            merged['除权除息日'] = pd.to_datetime(merged['除权除息日']).dt.strftime('%Y-%m-%d')

            # 筛选匹配目标日期的股票
            matched = merged[merged['除权除息日'].isin(target_date_strs)]
            symbols = set(matched['代码'].tolist())

            dm_logger.info(
                "[DataManager] Ex-dividend query: %d stocks matched for dates %s (from %d total records)",
                len(symbols), target_date_strs, len(merged)
            )
            return symbols

        except ImportError:
            dm_logger.warning("[DataManager] AkShare not available for ex-dividend query")
            return None
        except Exception as e:
            dm_logger.warning("[DataManager] Ex-dividend query failed: %s", e)
            return None

    async def _tdx_factor_audit(
        self,
        exchange: str,
        stocks: list[dict],
        phase2_result: dict,
    ) -> None:
        """Phase 2.5: tdx 自研因子旁路审计

        计算 XDXR 自研因子, 与 Phase 2 获取的权威因子交叉验证, 结果写入审计表.
        ★ 仅在因子路由配置了 tdx_xdxr validator 时执行.
        ★ 失败不影响主流程.

        Args:
            exchange: 交易所代码
            stocks: 品种列表 (含 instrument_id, symbol, start_date, end_date)
            phase2_result: Phase 2 同步结果 (synced/skipped/failed)
        """
        # 检查因子路由中是否配置了 tdx_xdxr validator
        # ★ 必须严格检查 validator 类型：HKEX 等非 A 股交易所配置的是
        #   yfinance validator（非 None），若仅检查 is None 会导致误触发
        factor_cfg = self.source_factory.factor_routes.get(exchange, {})
        validator_engine = factor_cfg.get('validator_instance')
        if validator_engine is None:
            return

        # 仅当 validator 是 TdxFactorEngine 时才执行 TDX 审计
        from data_sources.tdx_factor_engine import TdxFactorEngine
        if not isinstance(validator_engine, TdxFactorEngine):
            return

        # 仅对 Phase 2 成功同步的品种进行审计 (有除权事件的)
        if phase2_result.get('synced', 0) == 0:
            return

        dm_logger.info(
            "[DataManager] Phase 2.5: Starting tdx audit for %s (%d synced stocks)",
            exchange, phase2_result.get('synced', 0)
        )

        # 获取 pytdx 实例
        pytdx_source = self.source_factory._find_source_by_base_name('pytdx')
        if not pytdx_source or not pytdx_source.factor_engine:
            dm_logger.warning(
                "[DataManager] Phase 2.5: pytdx source not available, skipping audit"
            )
            return

        from data_sources.tdx_factor_validator import TdxFactorValidator
        validator = TdxFactorValidator(tolerance=0.001)

        audit_factors: list[dict] = []
        audit_count = 0
        conflict_count = 0

        for stock in stocks:
            instrument_id = stock['instrument_id']
            symbol = stock['symbol']
            start_dt = datetime.combine(stock['start_date'], datetime.min.time())
            end_dt = datetime.combine(stock['end_date'], datetime.max.time())

            try:
                # 1. 计算 tdx 自研因子
                tdx_factors = await pytdx_source.get_adjustment_factors(
                    instrument_id, symbol, start_dt, end_dt
                )
                if not tdx_factors:
                    continue

                # 2. 获取权威源因子 (从 DB 中读取 Phase 2 已写入的数据)
                ref_factors_raw = await self.db_ops.get_adjustment_factors(
                    instrument_id, None, end_dt
                )
                # 转换为 validator 需要的格式
                ref_factors = [
                    {
                        'instrument_id': r.get('instrument_id'),
                        'ex_date': r.get('ex_date'),
                        'factor': r.get('factor', 1.0),
                        'cumulative_factor': r.get('cumulative_factor', 1.0),
                    }
                    for r in ref_factors_raw
                ]

                # 3. 交叉验证
                report = validator.validate(instrument_id, tdx_factors, ref_factors)

                # 4. 将验证结果附加到 tdx 因子中, 准备写入审计表
                for f in tdx_factors:
                    ex_date_key = f['ex_date'].strftime('%Y-%m-%d') if isinstance(f['ex_date'], datetime) else str(f['ex_date'])[:10]
                    matching_detail = next(
                        (d for d in report.details if d.ex_date.strftime('%Y-%m-%d') == ex_date_key),
                        None
                    )
                    f['validation_result'] = report.result.value
                    if matching_detail:
                        f['ref_factor'] = matching_detail.ref_factor
                        f['ref_source'] = factor_cfg.get('primary_instance', {})
                        if hasattr(f['ref_source'], 'name'):
                            f['ref_source'] = f['ref_source'].name
                        else:
                            f['ref_source'] = str(f['ref_source'])
                        f['ratio_diff_pct'] = matching_detail.ratio_diff_pct
                        f['conflict_reason'] = matching_detail.conflict_reason

                    audit_factors.append(f)

                if report.conflict_count > 0:
                    conflict_count += 1
                    dm_logger.warning(
                        "[DataManager] Phase 2.5: %s has %d factor conflicts",
                        instrument_id, report.conflict_count
                    )

                audit_count += 1

            except Exception as e:
                dm_logger.debug(
                    "[DataManager] Phase 2.5: audit failed for %s: %s",
                    instrument_id, e
                )

        # 5. 批量写入审计表
        if audit_factors:
            saved = await self.db_ops.save_tdx_audit_factors(audit_factors)
            dm_logger.info(
                "[DataManager] Phase 2.5: tdx audit for %s: %d stocks audited, "
                "%d records saved, %d conflicts",
                exchange, audit_count, saved, conflict_count
            )

    async def sync_all_adjustment_factors(
        self,
        exchanges: Optional[List[str]] = None,
        days_back: int = 7,
    ) -> Dict[str, Any]:
        """全量同步复权因子（周维护用）

        遍历所有活跃股票品种，通过各市场配置的复权因子路由获取近 N 天的复权因子。
        作为每日精准筛选的兜底验证，确保不会因日更筛选遗漏导致复权错误。

        Args:
            exchanges: 交易所列表，默认 ['SSE', 'SZSE', 'BSE']，港股数据源启用时追加 HKEX
            days_back: 回查天数（默认 7 天，覆盖一周）

        Returns:
            各交易所的同步统计
        """
        if exchanges is None:
            exchanges = ['SSE', 'SZSE', 'BSE']
            # 港股已启用时纳入周度因子全量同步
            if self.config.get_nested('data_sources.hk_stock.enabled', False):
                exchanges.append('HKEX')

        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        overall_result: Dict[str, Any] = {}

        dm_logger.info(
            "[DataManager] Weekly full adjustment factor sync: %s, range=%s~%s",
            exchanges, start_date, end_date
        )

        for exchange in exchanges:
            try:
                maintenance_sync_enabled = self.config.get_nested(
                    f'routing.factor.{exchange}.maintenance_sync_enabled',
                    True,
                )
                if not maintenance_sync_enabled:
                    dm_logger.info(
                        "[DataManager] Weekly factor sync disabled for %s by config",
                        exchange
                    )
                    overall_result[exchange] = {
                        'skipped': True,
                        'reason': 'maintenance_sync_disabled',
                    }
                    continue

                instruments = await self.db_ops.get_instruments_list(
                    exchange=exchange,
                    type='stock',
                    is_active=True,
                )
                stocks = [
                    {
                        'instrument_id': inst['instrument_id'],
                        'symbol': inst['symbol'],
                        'start_date': start_date,
                        'end_date': end_date,
                    }
                    for inst in instruments
                ]

                if not stocks:
                    dm_logger.info("[DataManager] No stocks found for %s, skipping", exchange)
                    continue

                result = await self._batch_sync_adjustment_factors(
                    exchange, stocks, skip_filter=True, sync_reason='maintenance'
                )
                overall_result[exchange] = result

            except Exception as e:
                dm_logger.error(
                    "[DataManager] Weekly factor sync failed for %s: %s", exchange, e
                )
                overall_result[exchange] = {'error': str(e)}

        dm_logger.info("[DataManager] Weekly full factor sync completed: %s", overall_result)
        return overall_result

    async def backfill_adjustment_factors(
        self,
        exchanges: Optional[List[str]] = None,
        mode: str = 'missing',
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        progress_log_every: int = 200,
    ) -> Dict[str, Any]:
        """正式复权因子回补入口，供 TG 命令和运维任务调用。

        Args:
            exchanges: 交易所列表。为 None 时使用当前启用市场。
            mode:
                - missing: 仅处理当前完全没有因子记录的股票
                - full: 对目标交易所全部股票执行全量重抓并 upsert
            start_date: 回补开始日期，默认 1990-01-01
            end_date: 回补结束日期，默认今天
            progress_log_every: 每处理多少只股票打印一次进度

        Returns:
            结构化统计结果
        """
        mode = (mode or 'missing').lower()
        if mode == 'resume':
            mode = 'missing'
        if mode not in ('missing', 'full'):
            raise ValueError(f"Unsupported factor backfill mode: {mode}")

        if exchanges is None:
            exchanges = ['SSE', 'SZSE', 'BSE']
            if self.config.get_nested('data_sources.hk_stock.enabled', False):
                exchanges.append('HKEX')
            if self.config.get_nested('data_sources.us_stock.enabled', False):
                exchanges.extend(['NASDAQ', 'NYSE'])

        exchanges = [exchange.upper() for exchange in exchanges]
        exchanges = list(dict.fromkeys(exchanges))

        start_date = start_date or date(1990, 1, 1)
        end_date = end_date or date.today()
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end_date, datetime.max.time())

        if not hasattr(self, 'source_factory') or not self.source_factory:
            from data_sources.source_factory import get_data_source_factory
            self.source_factory = await get_data_source_factory(self.db_ops)

        overall_result: Dict[str, Any] = {
            'mode': mode,
            'exchanges': exchanges,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'by_exchange': {},
            'totals': {
                'stocks_total': 0,
                'skipped_existing': 0,
                'synced_instruments': 0,
                'saved_records': 0,
                'no_factors': 0,
                'errors': 0,
            }
        }

        dm_logger.info(
            "[DataManager] Starting factor backfill: exchanges=%s, mode=%s, range=%s~%s",
            exchanges, mode, start_date, end_date
        )

        for exchange in exchanges:
            instruments = await self.db_ops.get_instruments_list(
                exchange=exchange, type='stock', is_active=True
            )
            exchange_stats: Dict[str, Any] = {
                'stocks_total': len(instruments),
                'skipped_existing': 0,
                'synced_instruments': 0,
                'saved_records': 0,
                'no_factors': 0,
                'errors': 0,
                'sources': {},
            }

            existing_factor_ids = set()
            if mode == 'missing':
                existing_rows = await self.db_ops.execute_read_query(
                    """
                    SELECT DISTINCT af.instrument_id
                    FROM adjustment_factors af
                    JOIN instruments i ON i.instrument_id = af.instrument_id
                    WHERE i.exchange = :exchange AND i.type = 'stock' AND i.is_active = 1
                    """,
                    {'exchange': exchange}
                )
                existing_factor_ids = {
                    row['instrument_id']
                    for row in existing_rows
                    if row.get('instrument_id')
                }

            for idx, inst in enumerate(instruments, start=1):
                instrument_id = inst['instrument_id']
                symbol = inst['symbol']

                if mode == 'missing' and instrument_id in existing_factor_ids:
                    exchange_stats['skipped_existing'] += 1
                    continue

                try:
                    factors = await self.source_factory.get_adjustment_factors(
                        exchange, instrument_id, symbol, start_dt, end_dt
                    )

                    if factors:
                        saved_count = await self.db_ops.save_adjustment_factors(factors)
                        source_name = str(factors[0].get('source', 'unknown')).lower()
                        exchange_stats['synced_instruments'] += 1
                        exchange_stats['saved_records'] += saved_count
                        exchange_stats['sources'][source_name] = (
                            exchange_stats['sources'].get(source_name, 0) + 1
                        )
                    else:
                        exchange_stats['no_factors'] += 1

                except Exception as e:
                    exchange_stats['errors'] += 1
                    dm_logger.warning(
                        "[DataManager] Factor backfill failed for %s (%s): %s",
                        instrument_id, exchange, e
                    )

                if progress_log_every and idx % progress_log_every == 0:
                    dm_logger.info(
                        "[DataManager] Factor backfill progress %s: %d/%d "
                        "(synced=%d, skipped_existing=%d, no_factors=%d, errors=%d)",
                        exchange,
                        idx,
                        len(instruments),
                        exchange_stats['synced_instruments'],
                        exchange_stats['skipped_existing'],
                        exchange_stats['no_factors'],
                        exchange_stats['errors'],
                    )

            overall_result['by_exchange'][exchange] = exchange_stats
            for key in ('stocks_total', 'skipped_existing', 'synced_instruments', 'saved_records', 'no_factors', 'errors'):
                overall_result['totals'][key] += exchange_stats[key]

            dm_logger.info(
                "[DataManager] Factor backfill %s completed: %s",
                exchange, exchange_stats
            )

        self.invalidate_factor_cache()
        dm_logger.info("[DataManager] Factor backfill completed: %s", overall_result)
        return overall_result

    async def update_daily_data(self, exchanges: Optional[List[str]] = None,
                               target_date: Optional[date] = None,
                               per_instrument_timeout_sec: Optional[int] = None,
                               progress_log_every: int = 200,
                               progress_log_interval_sec: int = 300,
                               instrument_types: Optional[List[str]] = None,
                               run_factor_audit: bool = True,
                               master_governance_job_name: str = 'daily_data_update') -> Optional[dict]:
        """每日数据更新"""
        try:
            dm_logger.info(f"[DataManager] Starting daily data update for exchanges: {exchanges}")

            if exchanges is None:
                exchanges = ['SSE', 'SZSE', 'BSE']

            if target_date is None:
                target_date = date.today()
                
            if instrument_types is None:
                instrument_types = self.data_config.get('instrument_types', ['stock', 'index'])

            # 统计更新结果
            update_results = {
                'success_count': 0,
                'failure_count': 0,
                'total_quotes_added': 0,
                'exchange_stats': {},
                'catchup_stats': {
                    'new_instrument_count': 0,
                    'short_gap_count': 0,
                    'capped_count': 0,
                    'skipped_missing_listed_date': 0,
                    'catchup_quotes_added': 0,
                    'samples': [],
                },
            }
            instrument_master_sync = await self._maybe_sync_instrument_master_before_daily_update(
                exchanges,
                target_date,
                instrument_types=instrument_types,
                job_name=master_governance_job_name,
            )
            update_results['instrument_master_sync'] = instrument_master_sync
            update_results['instrument_master_governance'] = instrument_master_sync
            update_results['index_master_governance'] = instrument_master_sync.get('index_master_governance')
            catchup_config = self._get_daily_update_catchup_config()
            catchup_sample_limit = int(catchup_config.get('sample_limit', 10))

            def _record_catchup_sample(exchange_result: Dict[str, Any], sample: Dict[str, Any]) -> None:
                if catchup_sample_limit <= 0:
                    return
                global_samples = update_results['catchup_stats']['samples']
                if len(global_samples) < catchup_sample_limit:
                    global_samples.append(sample)
                exchange_samples = exchange_result['catchup_stats']['samples']
                if len(exchange_samples) < catchup_sample_limit:
                    exchange_samples.append(sample)

            for exchange in exchanges:
                try:
                    dm_logger.info(f"[DataManager] Updating data for {exchange}, types: {instrument_types}")

                    # 获取该交易所的活跃股票
                    instruments = await self.db_ops.get_active_instruments(
                        exchange,
                        instrument_types=instrument_types,
                        tradable_only=True,
                    )
                    total_instruments = len(instruments)
                    exchange_result = {
                        'success_count': 0,
                        'failure_count': 0,
                        'quotes_added': 0,
                        'total_instruments': total_instruments,
                        'catchup_stats': {
                            'new_instrument_count': 0,
                            'short_gap_count': 0,
                            'capped_count': 0,
                            'skipped_missing_listed_date': 0,
                            'catchup_quotes_added': 0,
                            'samples': [],
                        },
                    }

                    last_progress_log = datetime.now()
                    stocks_needing_factors: list = []  # 收集需要同步复权因子的股票品种

                    for idx, instrument in enumerate(instruments, start=1):
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

                            # 从数据源获取数据
                            # A 股沿用 calendar-day 锚点；
                            # 港股/美股改为前一交易日锚点，仅将 target_date 作为因子业务窗口。
                            is_a_stock = exchange in ('SSE', 'SZSE', 'BSE')
                            window = self._resolve_daily_update_fetch_window(
                                exchange=exchange,
                                target_date=target_date,
                                latest_quote_date=latest_date,
                                listed_date=instrument.get('listed_date'),
                                instrument_type=instrument.get('type', 'stock'),
                                catchup_config=catchup_config,
                            )
                            fetch_start_date = window['fetch_start_date']
                            factor_start_date = fetch_start_date
                            if not is_a_stock:
                                factor_start_date = target_date

                            end_date = target_date
                            data = None  # 初始化，供后续复权因子收集判断
                            is_catchup = window['reason'] in (
                                'new_instrument_catchup',
                                'short_gap_catchup',
                            )
                            if window.get('skipped_reason') == 'missing_listed_date':
                                exchange_result['catchup_stats']['skipped_missing_listed_date'] += 1
                                update_results['catchup_stats']['skipped_missing_listed_date'] += 1
                            
                            if should_update:
                                if per_instrument_timeout_sec:
                                    data = await asyncio.wait_for(
                                        self.source_factory.get_daily_data(
                                            exchange,
                                            instrument['instrument_id'],
                                            instrument['symbol'],
                                            datetime.combine(fetch_start_date, datetime.min.time()),
                                            datetime.combine(end_date, datetime.max.time()),
                                            instrument_type=instrument.get('type', 'stock'),
                                            source_symbol=instrument.get('source_symbol', '')
                                        ),
                                        timeout=per_instrument_timeout_sec
                                    )
                                else:
                                    data = await self.source_factory.get_daily_data(
                                        exchange,
                                        instrument['instrument_id'],
                                        instrument['symbol'],
                                        datetime.combine(fetch_start_date, datetime.min.time()),
                                        datetime.combine(end_date, datetime.max.time()),
                                        instrument_type=instrument.get('type', 'stock'),
                                        source_symbol=instrument.get('source_symbol', '')
                                    )

                                if data:
                                    await self.db_ops.save_daily_quotes(data)
                                    exchange_result['quotes_added'] += len(data)
                                    update_results['total_quotes_added'] += len(data)
                                    if is_catchup:
                                        exchange_result['catchup_stats']['catchup_quotes_added'] += len(data)
                                        update_results['catchup_stats']['catchup_quotes_added'] += len(data)
                                    dm_logger.debug(f"[DataManager] Updated {len(data)} records for {instrument['symbol']}")

                                if is_catchup:
                                    counter_name = (
                                        'new_instrument_count'
                                        if window['reason'] == 'new_instrument_catchup'
                                        else 'short_gap_count'
                                    )
                                    exchange_result['catchup_stats'][counter_name] += 1
                                    update_results['catchup_stats'][counter_name] += 1
                                    if window.get('capped'):
                                        exchange_result['catchup_stats']['capped_count'] += 1
                                        update_results['catchup_stats']['capped_count'] += 1
                                    _record_catchup_sample(
                                        exchange_result,
                                        {
                                            'instrument_id': instrument.get('instrument_id'),
                                            'symbol': instrument.get('symbol'),
                                            'exchange': exchange,
                                            'reason': window['reason'],
                                            'listed_date': window['listed_date'].isoformat()
                                            if window.get('listed_date') else None,
                                            'latest_quote_date': window['latest_quote_date'].isoformat()
                                            if window.get('latest_quote_date') else None,
                                            'fetch_start_date': fetch_start_date.isoformat(),
                                            'end_date': end_date.isoformat(),
                                            'capped': bool(window.get('capped')),
                                            'quotes_added': len(data or []),
                                        },
                                    )

                                exchange_result['success_count'] += 1
                                update_results['success_count'] += 1

                            # 收集需要同步复权因子的股票
                            # A 股（有 ex-dividend 精准筛选）: 无条件收集，Phase 2 会再精准过滤
                            # 港股/美股（无精准筛选）: 仅对今天有新数据写入的品种收集，避免全量空跑
                            if instrument.get('type', 'stock') == 'stock':
                                if is_a_stock or (data and len(data) > 0):
                                    stocks_needing_factors.append({
                                        'instrument_id': instrument['instrument_id'],
                                        'symbol': instrument['symbol'],
                                        'start_date': factor_start_date,
                                        'end_date': end_date,
                                    })

                            # 进度日志：按数量或时间间隔输出
                            now = datetime.now()
                            if (
                                (progress_log_every and idx % progress_log_every == 0)
                                or (progress_log_interval_sec and (now - last_progress_log).total_seconds() >= progress_log_interval_sec)
                            ):
                                dm_logger.info(
                                    "[DataManager] Daily update progress %s: %s/%s (last=%s)",
                                    exchange,
                                    idx,
                                    total_instruments,
                                    instrument.get('symbol', instrument.get('instrument_id'))
                                )
                                last_progress_log = now

                        except asyncio.TimeoutError:
                            dm_logger.warning(
                                "[DataManager] Daily update timed out for %s (%s)",
                                instrument.get('symbol'),
                                instrument.get('instrument_id')
                            )
                            exchange_result['failure_count'] += 1
                            update_results['failure_count'] += 1
                        except Exception as e:
                            dm_logger.error(f"[DataManager] Failed to update {instrument['symbol']}: {e}")
                            exchange_result['failure_count'] += 1
                            update_results['failure_count'] += 1
                            continue

                    update_results['exchange_stats'][exchange] = exchange_result
                    dm_logger.info(f"[DataManager] {exchange} update completed: {exchange_result['success_count']} success, {exchange_result['failure_count']} failed, {exchange_result['quotes_added']} quotes added")

                    # Phase 2: 批量同步复权因子（日线全部完成后）
                    if stocks_needing_factors:
                        factor_result = await self._batch_sync_adjustment_factors(
                            exchange, stocks_needing_factors, sync_reason='daily'
                        )
                        update_results.setdefault('factor_stats', {})[exchange] = factor_result

                        # Phase 2.5: tdx 自研因子旁路审计（不阻塞主流程）
                        if run_factor_audit:
                            try:
                                await self._tdx_factor_audit(
                                    exchange, stocks_needing_factors, factor_result
                                )
                            except Exception as audit_e:
                                dm_logger.warning(
                                    "[DataManager] Phase 2.5 tdx audit failed for %s (non-critical): %s",
                                    exchange, audit_e
                                )

                except Exception as e:
                    dm_logger.error(f"[DataManager] Failed to update {exchange}: {e}")
                    update_results['failure_count'] += 1
                    update_results['exchange_stats'][exchange] = {'error': str(e)}
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

    async def update_daily_data_range(
        self,
        exchanges: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        per_instrument_timeout_sec: Optional[int] = None,
        progress_log_every: int = 200,
        progress_log_interval_sec: int = 300,
        instrument_types: Optional[List[str]] = None,
        run_factor_audit: bool = False,
        repair_universe_mode: str = 'historical_backfill',
        override_lifecycle_filter: bool = False,
        repair_universe_limit: Optional[int] = None,
        force_current_master_refresh: bool = False,
        current_master_refresh_scopes: Optional[List[str]] = None,
    ) -> dict:
        """补充一个日期区间的日线数据。

        每个交易所只获取一次活跃品种列表，每个品种只请求一次连续日期
        范围，并在区间结束后集中同步复权因子。历史补数默认不执行
        Phase 2.5 TDX 审计，避免重复审计拖慢任务。
        """
        if start_date is None or end_date is None:
            raise ValueError("start_date and end_date are required")
        if end_date < start_date:
            raise ValueError(f"end_date {end_date} is earlier than start_date {start_date}")

        if exchanges is None:
            exchanges = ['SSE', 'SZSE', 'BSE']
        if instrument_types is None:
            instrument_types = self.data_config.get('instrument_types', ['stock', 'index'])

        def _to_date(value: Any) -> Optional[date]:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            try:
                return pd.to_datetime(value).date()
            except Exception:
                return None

        dm_logger.info(
            "[DataManager] Starting range daily data backfill for exchanges=%s range=%s~%s",
            exchanges, start_date, end_date
        )

        update_results: dict = {
            'operation': 'range_backfill',
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'success_count': 0,
            'failure_count': 0,
            'total_quotes_added': 0,
            'exchange_stats': {},
            'instrument_master_sync': {
                'status': 'skipped',
                'reason': 'range_backfill_uses_existing_master',
                'target_date': end_date.isoformat(),
                'exchanges': {},
                'warnings': [],
                'errors': [],
            },
            'repair_universe': self._build_repair_universe_diagnostics(
                mode=self._normalize_repair_universe_mode(repair_universe_mode),
                start_date=start_date,
                end_date=end_date,
                override_lifecycle_filter=override_lifecycle_filter,
            ),
        }

        if force_current_master_refresh:
            refresh_types = self._instrument_types_for_governance_scopes(
                current_master_refresh_scopes,
                instrument_types or self.data_config.get('instrument_types', ['stock', 'index']),
            )
            governance = await self._run_repair_current_master_refresh(
                job_name='daily_data_backfill_range',
                exchanges=exchanges,
                instrument_types=refresh_types,
                target_date=end_date,
                scopes=current_master_refresh_scopes,
            )
            update_results['instrument_master_sync'] = governance
            update_results['instrument_master_governance'] = governance
            update_results['repair_universe']['current_master_refresh'] = {
                'requested': True,
                'status': governance.get('status'),
                'scopes': current_master_refresh_scopes or [
                    child.get('scope')
                    for child in (governance.get('children') or [])
                    if isinstance(child, dict) and child.get('scope')
                ],
                'operator_requested': True,
            }

        for exchange in exchanges:
            try:
                dm_logger.info(
                    "[DataManager] Range backfill updating %s, types=%s, range=%s~%s",
                    exchange, instrument_types, start_date, end_date
                )
                if hasattr(self.db_ops, 'get_repair_universe_instruments'):
                    raw_instruments = await self.db_ops.get_repair_universe_instruments(
                        exchange,
                        instrument_types=instrument_types,
                    )
                else:
                    raw_instruments = await self.db_ops.get_active_instruments(
                        exchange,
                        instrument_types=instrument_types,
                        tradable_only=True,
                    )
                instruments, repair_diag = await self.filter_repair_universe(
                    raw_instruments,
                    start_date=start_date,
                    end_date=end_date,
                    mode=repair_universe_mode,
                    override_lifecycle_filter=override_lifecycle_filter,
                    limit=repair_universe_limit,
                )
                for key in ('input_instrument_count', 'eligible_instrument_count',
                            'clipped_instrument_count', 'skipped_instrument_count',
                            'skipped_gap_segment_count', 'skipped_missing_days'):
                    update_results['repair_universe'][key] += int(repair_diag.get(key, 0) or 0)
                merged_counter = Counter(update_results['repair_universe'].get('reason_distribution') or {})
                merged_counter.update(repair_diag.get('reason_distribution') or {})
                update_results['repair_universe']['reason_distribution'] = dict(merged_counter)
                sample_limit = int(
                    self._get_repair_universe_governance_config().get('sample_limit', 10) or 0
                )
                if sample_limit > 0:
                    update_results['repair_universe']['samples'].extend(
                        repair_diag.get('samples', [])[
                            : max(0, sample_limit - len(update_results['repair_universe'].get('samples') or []))
                        ]
                    )
                update_results['repair_universe']['warnings'].extend(repair_diag.get('warnings') or [])
                update_results['repair_universe']['errors'].extend(repair_diag.get('errors') or [])

                total_instruments = len(instruments)
                exchange_result = {
                    'success_count': 0,
                    'failure_count': 0,
                    'quotes_added': 0,
                    'raw_total_instruments': len(raw_instruments),
                    'total_instruments': total_instruments,
                    'skipped_not_listed': 0,
                    'repair_universe': repair_diag,
                }

                is_a_stock = exchange in ('SSE', 'SZSE', 'BSE')
                range_fetch_start = start_date - timedelta(days=1)
                if not is_a_stock:
                    range_fetch_start = DateUtils.get_previous_trading_day(
                        exchange, start_date
                    )

                last_progress_log = datetime.now()
                stocks_needing_factors: list[dict] = []

                for idx, instrument in enumerate(instruments, start=1):
                    try:
                        listed_date = _to_date(instrument.get('listed_date'))
                        if listed_date and listed_date > end_date:
                            exchange_result['skipped_not_listed'] += 1
                            continue

                        query_start = range_fetch_start
                        repair_start = _to_date(instrument.get('_repair_start_date'))
                        repair_end = _to_date(instrument.get('_repair_end_date')) or end_date
                        if listed_date and listed_date > query_start:
                            query_start = listed_date
                        if repair_start and repair_start > query_start:
                            query_start = repair_start
                        query_end = min(end_date, repair_end)
                        if query_start > query_end:
                            exchange_result['skipped_not_listed'] += 1
                            continue

                        async def _fetch_range() -> list:
                            return await self.source_factory.get_daily_data(
                                exchange,
                                instrument['instrument_id'],
                                instrument['symbol'],
                                datetime.combine(query_start, datetime.min.time()),
                                datetime.combine(query_end, datetime.max.time()),
                                instrument_type=instrument.get('type', 'stock'),
                                source_symbol=instrument.get('source_symbol', ''),
                            )

                        if per_instrument_timeout_sec:
                            data = await asyncio.wait_for(
                                _fetch_range(),
                                timeout=per_instrument_timeout_sec,
                            )
                        else:
                            data = await _fetch_range()

                        if data:
                            await self.db_ops.save_daily_quotes(data)
                            exchange_result['quotes_added'] += len(data)
                            update_results['total_quotes_added'] += len(data)

                        exchange_result['success_count'] += 1
                        update_results['success_count'] += 1

                        if instrument.get('type', 'stock') == 'stock':
                            if is_a_stock or (data and len(data) > 0):
                                stocks_needing_factors.append({
                                    'instrument_id': instrument['instrument_id'],
                                    'symbol': instrument['symbol'],
                                    'start_date': query_start,
                                    'end_date': query_end,
                                })

                        now = datetime.now()
                        if (
                            (progress_log_every and idx % progress_log_every == 0)
                            or (
                                progress_log_interval_sec
                                and (now - last_progress_log).total_seconds() >= progress_log_interval_sec
                            )
                        ):
                            dm_logger.info(
                                "[DataManager] Range backfill progress %s: %s/%s (last=%s)",
                                exchange,
                                idx,
                                total_instruments,
                                instrument.get('symbol', instrument.get('instrument_id')),
                            )
                            last_progress_log = now

                    except asyncio.TimeoutError:
                        dm_logger.warning(
                            "[DataManager] Range backfill timed out for %s (%s)",
                            instrument.get('symbol'),
                            instrument.get('instrument_id'),
                        )
                        exchange_result['failure_count'] += 1
                        update_results['failure_count'] += 1
                    except Exception as e:
                        dm_logger.error(
                            "[DataManager] Range backfill failed for %s: %s",
                            instrument.get('symbol'),
                            e,
                        )
                        exchange_result['failure_count'] += 1
                        update_results['failure_count'] += 1

                update_results['exchange_stats'][exchange] = exchange_result
                dm_logger.info(
                    "[DataManager] Range backfill %s completed: %d success, %d failed, %d quotes added",
                    exchange,
                    exchange_result['success_count'],
                    exchange_result['failure_count'],
                    exchange_result['quotes_added'],
                )

                if stocks_needing_factors:
                    factor_result = await self._batch_sync_adjustment_factors(
                        exchange,
                        stocks_needing_factors,
                        sync_reason='daily',
                    )
                    update_results.setdefault('factor_stats', {})[exchange] = factor_result

                    if run_factor_audit:
                        try:
                            await self._tdx_factor_audit(
                                exchange, stocks_needing_factors, factor_result
                            )
                        except Exception as audit_e:
                            dm_logger.warning(
                                "[DataManager] Phase 2.5 tdx audit failed for %s (non-critical): %s",
                                exchange, audit_e
                            )

            except Exception as e:
                dm_logger.error("[DataManager] Range backfill failed for %s: %s", exchange, e)
                update_results['failure_count'] += 1
                update_results['exchange_stats'][exchange] = {'error': str(e)}

        dm_logger.info("[DataManager] Range daily data backfill completed: %s", update_results)
        return update_results

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
