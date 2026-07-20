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
from bisect import bisect_right
from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import inspect
import json
import os
import re
import sqlite3
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
from data_sources.corporate_action_validation import (
    compare_cumulative_factor_paths,
    match_official_announcement_evidence,
    normalize_cninfo_implementation_announcements,
    normalize_eastmoney_events,
    normalize_tdx_events,
    reconcile_event_fields,
)
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
        self.fx_storage = None
        self.special_commodity_storage = None

        # 复权因子内存缓存: {dataset/version/instrument: (timestamp, bundle)}
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
        bundle = await self.get_cached_adjustment_factor_bundle(instrument_id)
        return bundle["factors"]

    async def get_cached_adjustment_factor_bundle(
        self, instrument_id: str
    ) -> Dict[str, Any]:
        """Return factors plus the configured dataset/version provenance."""
        import time

        governance = self.data_config.get("adjustment_factor_governance", {})
        requested_dataset = str(governance.get("read_dataset", "legacy")).lower()
        series_version = str(
            governance.get("canonical_series_version", "a_share_event_product_v1")
        ).strip()
        if not series_version or len(series_version) > 64:
            raise ValueError("canonical_series_version must contain 1 to 64 characters")
        allow_legacy_fallback = bool(governance.get("allow_legacy_fallback", True))
        cache_key = (
            f"{requested_dataset}:{series_version}:{int(allow_legacy_fallback)}:"
            f"{instrument_id}"
        )
        now = time.monotonic()
        cached = self._factor_cache.get(cache_key)
        if cached is not None:
            ts, bundle = cached
            if (now - ts) < self._FACTOR_CACHE_TTL:
                return bundle

        actual_dataset = requested_dataset
        fallback_used = False
        availability_error = None
        factors: List[Dict[str, Any]] = []
        series_status = None
        instrument_status = None
        if requested_dataset == "canonical":
            series_status = await self.db_ops.get_adjustment_factor_series_status(
                series_version
            )
            if series_status and series_status.get("promotion_eligible"):
                instrument_status = (
                    await self.db_ops.get_adjustment_factor_instrument_status(
                        instrument_id, series_version
                    )
                )
                coverage_status = (
                    instrument_status.get("coverage_status")
                    if instrument_status else None
                )
                if coverage_status == "complete_with_events":
                    factors = await self.db_ops.get_canonical_adjustment_factors(
                        instrument_id, series_version
                    )
                    if not factors:
                        availability_error = (
                            "canonical coverage says events exist but no factor rows were found"
                        )
                elif coverage_status == "complete_no_events":
                    factors = []
                else:
                    availability_error = (
                        f"canonical factor coverage is unavailable for {instrument_id}"
                    )
            else:
                availability_error = (
                    f"canonical factor series {series_version} is not promotion eligible"
                )
            if availability_error and allow_legacy_fallback:
                factors = await self.db_ops.get_adjustment_factors(instrument_id)
                actual_dataset = "legacy"
                fallback_used = True
                availability_error = None
        else:
            factors = await self.db_ops.get_adjustment_factors(instrument_id)
            actual_dataset = "legacy"

        bundle = {
            "factors": factors,
            "requested_dataset": requested_dataset,
            "actual_dataset": actual_dataset,
            "series_version": series_version if actual_dataset == "canonical" else None,
            "fallback_used": fallback_used,
            "availability_error": availability_error,
            "series_status": series_status,
            "instrument_status": instrument_status,
        }
        self._factor_cache[cache_key] = (now, bundle)
        return bundle

    def invalidate_factor_cache(self, instrument_id: Optional[str] = None) -> None:
        """清除复权因子缓存

        Args:
            instrument_id: 若指定则清除单个品种缓存, 否则清除全部缓存
        """
        if instrument_id:
            suffix = f":{instrument_id}"
            for key in [key for key in self._factor_cache if key.endswith(suffix)]:
                self._factor_cache.pop(key, None)
        else:
            self._factor_cache.clear()
        dm_logger.debug(
            "[DataManager] Factor cache cleared for %s",
            instrument_id or "ALL"
        )

    async def _persist_adjustment_factor_batch(
        self,
        exchange: str,
        factors: List[Dict[str, Any]],
        *,
        ingestion_run_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist factor evidence and contain cumulative source-basis changes."""
        if not factors:
            return {"saved": 0, "observation_stats": {}, "rebase_stats": {}}

        is_a_share = str(exchange).upper() in {"SSE", "SZSE", "BSE"}
        governance = self.data_config.get("adjustment_factor_governance", {})
        if not is_a_share:
            saved = await self.db_ops.save_adjustment_factors(factors)
            return {"saved": saved, "observation_stats": {}, "rebase_stats": {}}

        from data_sources.adjustment_factor_governance import normalize_source_path

        observations = normalize_source_path(
            factors,
            normalization_version=str(
                governance.get("normalization_version", "event_ratio_v1")
            ),
        )
        if not observations:
            raise RuntimeError("A-share factor rows produced no valid source observations")
        observation_stats: Dict[str, int] = {}
        if governance.get("write_source_observations", True):
            observation_stats = await self.db_ops.save_adjustment_factor_observations(
                observations,
                ingestion_run_id=ingestion_run_id,
            )
            processed_observations = sum(
                int(observation_stats.get(key, 0))
                for key in ("inserted", "changed", "unchanged")
            )
            if (
                int(observation_stats.get("failed", 0)) > 0
                or processed_observations != len(observations)
            ):
                raise RuntimeError(
                    "source observation persistence incomplete: "
                    f"expected={len(observations)} stats={observation_stats}"
                )

        legacy_rows = factors
        rebase_stats: Dict[str, int] = {}
        if governance.get("rebase_legacy_appends", True):
            legacy_rows, rebase_stats = await self.db_ops.prepare_legacy_factor_appends(
                observations
            )
        saved = await self.db_ops.save_adjustment_factors(legacy_rows) if legacy_rows else 0
        for instrument_id in {str(item.get("instrument_id")) for item in factors}:
            self.invalidate_factor_cache(instrument_id)
        return {
            "saved": saved,
            "observation_stats": observation_stats,
            "rebase_stats": rebase_stats,
        }



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
            self._initialize_fx_storage()
            self._initialize_special_commodity_storage()

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

    def _initialize_fx_storage(self) -> None:
        """Initialize dedicated FX-domain storage when configured."""
        if not self.research_config.enabled:
            return
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        if not module_cfg.get("enabled", False):
            return
        try:
            from research.fx_market_data import FxMasterDataService, FxStorageManager

            self.fx_storage = FxStorageManager(self.research_config)
            self.fx_storage.initialize()
            FxMasterDataService(self.fx_storage, module_cfg).sync()
            dm_logger.info(
                "[DataManager] FX storage initialized: %s",
                self.fx_storage.db_path,
            )
        except Exception as e:
            dm_logger.warning(
                "[DataManager] FX storage initialization failed, continuing "
                "without FX storage: %s",
                e,
            )
            self.fx_storage = None

    def _require_fx_storage(self):
        """Return FX storage or raise a structured runtime error."""
        if not self.research_config.enabled:
            raise RuntimeError("research_config.enabled is false")
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        if not module_cfg.get("enabled", False):
            raise RuntimeError("research fx_market_data module is disabled")
        if self.fx_storage is None:
            self._initialize_fx_storage()
        if self.fx_storage is None:
            raise RuntimeError("FX storage is not initialized")
        return self.fx_storage

    def _initialize_special_commodity_storage(self) -> None:
        """Initialize isolated special-commodity tables in the futures-domain DB."""
        if not self.research_config.enabled:
            return
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        special_cfg = (module_cfg or {}).get("special_commodity_market_data", {})
        if not special_cfg:
            return
        try:
            from research.special_commodity_market_data import (
                SpecialCommodityMasterDataService,
                SpecialCommodityStorageManager,
            )

            self.special_commodity_storage = SpecialCommodityStorageManager(self.research_config)
            self.special_commodity_storage.initialize()
            SpecialCommodityMasterDataService(
                self.special_commodity_storage,
                special_cfg,
            ).sync()
            dm_logger.info(
                "[DataManager] Special commodity storage initialized: %s",
                self.special_commodity_storage.db_path,
            )
        except Exception as e:
            dm_logger.warning(
                "[DataManager] Special commodity storage initialization failed, continuing "
                "without special commodity storage: %s",
                e,
            )
            self.special_commodity_storage = None

    def _require_special_commodity_storage(self):
        """Return special commodity storage or raise a structured runtime error."""
        if not self.research_config.enabled:
            raise RuntimeError("research_config.enabled is false")
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        special_cfg = (module_cfg or {}).get("special_commodity_market_data", {})
        if not special_cfg:
            raise RuntimeError("special_commodity_market_data config is missing")
        if self.special_commodity_storage is None:
            self._initialize_special_commodity_storage()
        if self.special_commodity_storage is None:
            raise RuntimeError("special commodity storage is not initialized")
        return self.special_commodity_storage

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

    async def run_business_profile_structured_sync(
        self,
        *,
        as_of_date: Optional[str] = None,
        sources: Optional[List[str]] = None,
        industry_groups: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        max_instruments: Optional[int] = None,
        max_elapsed_seconds: Optional[float] = None,
        candidate_write: bool = True,
        operator_switch: str = "",
        checkpoint_path: Optional[str] = None,
        resume: bool = True,
    ) -> Dict[str, Any]:
        """Run the bounded candidate-only structured business-profile sync."""
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

        module = self.research_config.modules.get("business_profile_evidence", {})
        if module.get("enabled") is not True:
            return {
                "status": "disabled",
                "reason": "research business_profile_evidence module is disabled",
            }
        source_config = module.get("free_structured_sources", {})
        if source_config.get("enabled") is not True:
            return {
                "status": "disabled",
                "reason": "free structured business-profile sync is disabled",
            }

        from research.business_profile_structured_sync import (
            StructuredBusinessProfileSyncService,
        )

        service = StructuredBusinessProfileSyncService(
            storage=self.research_storage,
            research_config=self.research_config,
        )
        return await service.sync(
            as_of_date=as_of_date,
            sources=sources,
            industry_groups=industry_groups,
            instrument_ids=instrument_ids,
            max_instruments=max_instruments,
            max_elapsed_seconds=max_elapsed_seconds,
            dry_run=not candidate_write,
            candidate_write=candidate_write,
            operator_switch=operator_switch,
            cache_raw_snapshots=candidate_write,
            checkpoint_path=(
                Path(checkpoint_path) if checkpoint_path else None
            ),
            resume=resume,
        )

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

    async def get_research_company_business_profile(
        self,
        instrument_id: str,
        *,
        as_of_date: Optional[str] = None,
        include_candidates: bool = True,
    ) -> Dict[str, Any]:
        """Read the governed local business profile for one company."""
        storage = self._require_research_storage()
        normalized_id = convert_to_database_format(instrument_id)
        cutoff = str(as_of_date or get_shanghai_time().date().isoformat())[:10]
        membership = await self._get_dcf_industry_membership(
            storage,
            normalized_id,
            valuation_date=cutoff,
            historical_request=as_of_date is not None,
        )
        return await self._resolve_business_profile_context(
            storage,
            normalized_id,
            valuation_date=cutoff,
            industry_membership=membership,
            include_candidates=include_candidates,
        )

    async def get_research_company_business_profile_history(
        self,
        instrument_id: str,
        *,
        limit: int = 5000,
    ) -> Dict[str, Any]:
        """Read normalized profile history without applying valuation gates."""
        from research.business_profile_governance import BusinessProfileRepository

        storage = self._require_research_storage()
        normalized_id = convert_to_database_format(instrument_id)
        repository = BusinessProfileRepository(storage)
        history = await asyncio.to_thread(
            repository.get_profile_history,
            normalized_id,
            limit=limit,
        )
        return {
            "status": "success" if any(history.values()) else "empty",
            "instrument_id": normalized_id,
            "history": history,
        }

    async def get_research_company_commodity_exposures(
        self,
        instrument_id: str,
        *,
        as_of_date: Optional[str] = None,
        include_candidates: bool = True,
    ) -> Dict[str, Any]:
        """Read governed and executable commodity exposure for one company."""
        context = await self.get_research_company_business_profile(
            instrument_id,
            as_of_date=as_of_date,
            include_candidates=include_candidates,
        )
        return {
            "status": context.get("status"),
            "instrument_id": context.get("instrument_id"),
            "data_available_cutoff": context.get("data_available_cutoff"),
            "approved_exposures": context.get("approved_exposures") or [],
            "candidate_exposures": context.get("candidate_exposures") or [],
            "executable_exposure_mappings": context.get("executable_exposure_mappings") or [],
            "industry_default_profile": context.get("industry_default_profile") or {},
            "conflicts": context.get("conflicts") or [],
            "readiness": context.get("readiness") or {},
            "warnings": context.get("warnings") or [],
            "profile_version": context.get("profile_version"),
            "lineage_hash": context.get("lineage_hash"),
        }

    async def get_research_business_profile_review_queue(
        self,
        *,
        instrument_id: Optional[str] = None,
        record_type: Optional[str] = None,
        limit: int = 200,
    ) -> Dict[str, Any]:
        """Read local candidate facts awaiting review."""
        from research.business_profile_governance import BusinessProfileRepository

        storage = self._require_research_storage()
        normalized_id = (
            convert_to_database_format(instrument_id) if instrument_id else None
        )
        repository = BusinessProfileRepository(storage)
        rows = await asyncio.to_thread(
            repository.get_review_queue,
            instrument_id=normalized_id,
            record_type=record_type,
            limit=limit,
        )
        return {
            "status": "success" if rows else "empty",
            "row_count": len(rows),
            "rows": rows,
        }

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

    async def get_research_industry_as_of(
        self,
        instrument_id: str,
        as_of_date: str,
        *,
        taxonomy_system: Optional[str] = None,
        include_snapshot: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """读取研究域行业归属的时点化视图 (REQ-07.2)。"""
        storage = self._require_research_storage()
        industry_config = self.research_config.modules.get("industry", {})
        if not industry_config.get("enabled", False):
            raise RuntimeError("research industry module is disabled")

        normalized_id = convert_to_database_format(instrument_id)
        return await asyncio.to_thread(
            storage.get_industry_membership_as_of,
            normalized_id,
            as_of_date,
            taxonomy_system=taxonomy_system,
            include_snapshot=include_snapshot,
        )

    async def get_research_risk_free_rate(
        self,
        series_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """读取无风险利率序列观测值 (REQ-13)。无数据返回空序列而非报错。"""
        storage = self._require_research_storage()
        series_list = await asyncio.to_thread(storage.list_risk_free_rate_series)
        series_meta = next(
            (s for s in series_list if s.get("series_id") == series_id), None
        )
        observations = await asyncio.to_thread(
            storage.get_risk_free_rate_observations,
            series_id,
            start_date=start_date,
            end_date=end_date,
        )
        return {
            "series_id": series_id,
            "series": series_meta,
            "observations": observations,
            "total": len(observations),
        }

    async def list_research_risk_free_rate_series(self) -> List[Dict[str, Any]]:
        """列出所有无风险利率序列定义 (REQ-13)。"""
        storage = self._require_research_storage()
        return await asyncio.to_thread(storage.list_risk_free_rate_series)

    async def sync_risk_free_rate(
        self, *, data_as_of: Optional[str] = None
    ) -> Dict[str, Any]:
        """采集无风险利率序列写入研究存储 (REQ-13, 写入侧)。"""
        from research.risk_free_rate_sync import RiskFreeRateSyncService

        storage = self._require_research_storage()
        service = RiskFreeRateSyncService(storage)
        return await asyncio.to_thread(service.sync, data_as_of=data_as_of)

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

    async def run_fx_master_sync(self) -> Dict[str, Any]:
        """Seed and refresh FX currencies, instruments, series, manifests, and derivations."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxMasterDataService

        return await asyncio.to_thread(FxMasterDataService(storage, module_cfg).sync)

    async def run_fx_calendar_governance(
        self,
        *,
        source_profiles: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Maintain local FX publication/observation calendar governance."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxCalendarGovernanceService

        return await asyncio.to_thread(
            FxCalendarGovernanceService(storage, module_cfg).run,
            source_profiles=source_profiles,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def run_fx_rate_sync(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Run FX rate sync into the dedicated FX database."""
        storage = self._require_fx_storage()
        from research.fx_market_data import FxRateSyncService

        return await asyncio.to_thread(
            FxRateSyncService(storage, self.research_config).sync,
            scope_id=scope_id,
            scope_ids=scope_ids,
            series_ids=series_ids,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def run_fx_rate_backfill(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Run FX historical backfill; requires explicit start/end dates."""
        if not start_date or not end_date:
            raise ValueError("fx_rate_backfill requires start_date and end_date")
        return await self.run_fx_rate_sync(
            scope_id=scope_id,
            scope_ids=scope_ids,
            series_ids=series_ids,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def run_special_commodity_master_sync(self) -> Dict[str, Any]:
        """Seed and refresh special commodity instruments, series, and source manifests."""
        storage = self._require_special_commodity_storage()
        module_cfg = (
            self.research_config.modules.get("commodity_market_data", {})
            .get("special_commodity_market_data", {})
        )
        from research.special_commodity_market_data import SpecialCommodityMasterDataService

        return await asyncio.to_thread(
            SpecialCommodityMasterDataService(storage, module_cfg).sync
        )

    async def run_special_commodity_price_sync(
        self,
        *,
        scope_id: Optional[str] = None,
        scope_ids: Optional[List[str]] = None,
        venues: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        commodity_ids: Optional[List[str]] = None,
        series_ids: Optional[List[str]] = None,
        frequencies: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Run special commodity price sync into isolated commodity tables."""
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityPriceSyncService

        return await asyncio.to_thread(
            SpecialCommodityPriceSyncService(storage, self.research_config).sync,
            scope_id=scope_id,
            scope_ids=scope_ids,
            venues=venues,
            categories=categories,
            commodity_ids=commodity_ids,
            series_ids=series_ids,
            frequencies=frequencies,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def run_special_commodity_calendar_governance(
        self,
        *,
        scope_id: Optional[str] = None,
        series_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Govern special commodity observation/publication dates."""
        storage = self._require_special_commodity_storage()
        module_cfg = (
            self.research_config.modules.get("commodity_market_data", {})
            .get("special_commodity_market_data", {})
        )
        from research.special_commodity_market_data import SpecialCommodityCalendarGovernanceService

        return await asyncio.to_thread(
            SpecialCommodityCalendarGovernanceService(storage, module_cfg).run,
            scope_id=scope_id,
            series_ids=series_ids,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def get_special_commodity_dictionary(self) -> Dict[str, Any]:
        """Read special commodity instruments and series dictionaries."""
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityReadService

        return await asyncio.to_thread(SpecialCommodityReadService(storage).dictionary)

    async def get_special_commodity_series(
        self,
        *,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """Read special commodity series metadata."""
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityReadService

        return await asyncio.to_thread(
            SpecialCommodityReadService(storage).series,
            active_only=active_only,
        )

    async def get_special_commodity_observations(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Read special commodity observations by series id."""
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityReadService

        return await asyncio.to_thread(
            SpecialCommodityReadService(storage).observations,
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
        )

    async def get_special_commodity_diagnostics(
        self,
        *,
        target_currency: Optional[str] = None,
        max_fx_lag_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Read local special commodity diagnostics and optional local FX readiness."""
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityReadService

        diagnostics = await asyncio.to_thread(SpecialCommodityReadService(storage).diagnostics)
        if not target_currency:
            return diagnostics

        target = target_currency.upper()
        fx_checks: List[Dict[str, Any]] = []
        for row in diagnostics.get("latest_observations", []):
            currency = str(row.get("currency") or "").upper()
            if not currency or currency == target:
                continue
            try:
                fx_storage = self._require_fx_storage()
                module_cfg = self.research_config.modules.get("fx_market_data", {})
                from research.fx_market_data import FxReadService

                converted = await asyncio.to_thread(
                    FxReadService(fx_storage, module_cfg).convert,
                    from_currency=currency,
                    to_currency=target,
                    amount=float(row.get("value") or 1.0),
                    observation_date=str(row.get("observation_date") or ""),
                    max_lag_days=max_fx_lag_days,
                )
            except Exception as exc:
                converted = {
                    "success": False,
                    "status": "blocked",
                    "reason": "fx_conversion_check_failed",
                    "blockers": [str(exc)],
                }
            fx_checks.append(
                {
                    "series_id": row.get("series_id"),
                    "observation_date": row.get("observation_date"),
                    "from_currency": currency,
                    "to_currency": target,
                    "fx_conversion": converted,
                }
            )
        diagnostics["target_currency"] = target
        diagnostics["fx_checks"] = fx_checks
        diagnostics["fx_dependency_gaps"] = [
            item
            for item in fx_checks
            if not (item.get("fx_conversion") or {}).get("success")
        ]
        return diagnostics

    async def get_special_commodity_policy_events(
        self,
        *,
        commodity_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Read reviewed commodity policy/long-term-contract events."""
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityReadService

        return await asyncio.to_thread(
            SpecialCommodityReadService(storage).policy_events,
            commodity_id=commodity_id,
        )

    async def run_special_commodity_policy_discovery(
        self,
        *,
        adapter_id: str = "ndrc",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Discover official policy documents and persist governed candidates."""
        storage = self._require_special_commodity_storage()
        module_cfg = (
            self.research_config.modules.get("commodity_market_data", {})
            .get("special_commodity_market_data", {})
        )
        from research.special_commodity_market_data import SpecialCommodityPolicyDiscoveryService

        return await asyncio.to_thread(
            SpecialCommodityPolicyDiscoveryService(storage, module_cfg).run,
            adapter_id=adapter_id,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def get_special_commodity_policy_candidates(
        self,
        *,
        review_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        storage = self._require_special_commodity_storage()
        rows = await asyncio.to_thread(
            storage.read_policy_candidates,
            review_status=review_status,
        )
        documents = await asyncio.to_thread(storage.read_source_documents)
        document_by_id = {str(item["document_id"]): item for item in documents}
        for row in rows:
            row["review_code"] = str(row["candidate_id"]).rsplit(".", 1)[-1][:8]
            document = document_by_id.get(str(row.get("document_id") or ""), {})
            row["document_number"] = document.get("document_number")
            row["document_title"] = document.get("title")
        return {"status": "success", "review_status": review_status, "candidates": rows, "count": len(rows)}

    async def review_special_commodity_policy_candidate(
        self,
        *,
        candidate_ref: str,
        decision: str,
        reviewer: str = "operator",
        notes: str = "",
        promote: bool = True,
    ) -> Dict[str, Any]:
        """Record a decision and optionally promote through the formal validator."""
        normalized = str(decision or "").strip().lower()
        if normalized not in {"approved", "rejected"}:
            raise ValueError("decision must be approved or rejected")
        storage = self._require_special_commodity_storage()
        candidate = await asyncio.to_thread(storage.resolve_policy_candidate, candidate_ref)
        if candidate is None:
            return {
                "status": "blocked",
                "candidate_ref": candidate_ref,
                "decision": normalized,
                "reason": "policy_candidate_not_found",
            }
        candidate_id = str(candidate["candidate_id"])
        updated = await asyncio.to_thread(
            storage.set_policy_candidate_review_status,
            candidate_id=candidate_id,
            review_status=normalized,
            reviewer=str(reviewer or "operator").strip(),
            notes=str(notes or "").strip(),
        )
        if not updated:
            return {
                "status": "blocked",
                "candidate_id": candidate_id,
                "decision": normalized,
                "reason": "policy_candidate_not_found",
            }
        promotion: Dict[str, Any] = {}
        if normalized == "approved" and promote:
            module_cfg = (
                self.research_config.modules.get("commodity_market_data", {})
                .get("special_commodity_market_data", {})
            )
            from research.special_commodity_market_data import SpecialCommodityPolicyEventService

            promotion = await asyncio.to_thread(
                SpecialCommodityPolicyEventService(storage, module_cfg).promote_approved_candidates,
                dry_run=False,
            )
        status = "blocked" if promotion.get("status") == "blocked" else "success"
        return {
            "status": status,
            "candidate_id": candidate_id,
            "candidate_ref": candidate_ref,
            "decision": normalized,
            "reviewer": reviewer,
            "promoted": bool(normalized == "approved" and promote),
            "promotion": promotion,
            "next_task": None,
        }

    async def get_special_commodity_source_documents(
        self,
        *,
        source_profile: Optional[str] = None,
    ) -> Dict[str, Any]:
        storage = self._require_special_commodity_storage()
        rows = await asyncio.to_thread(
            storage.read_source_documents,
            source_profile=source_profile,
        )
        return {"status": "success", "source_profile": source_profile, "documents": rows, "count": len(rows)}

    async def get_special_commodity_indicators(
        self,
        *,
        category: Optional[str] = None,
        series_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        storage = self._require_special_commodity_storage()
        from research.special_commodity_market_data import SpecialCommodityReadService

        return await asyncio.to_thread(
            SpecialCommodityReadService(storage).indicators,
            category=category,
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
        )

    async def run_special_commodity_series_catalog_sync(
        self,
        *,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        storage = self._require_special_commodity_storage()
        module_cfg = (
            self.research_config.modules.get("commodity_market_data", {})
            .get("special_commodity_market_data", {})
        )
        from research.special_commodity_market_data import SpecialCommoditySeriesCatalogService

        return await asyncio.to_thread(
            SpecialCommoditySeriesCatalogService(storage, module_cfg).sync,
            dry_run=dry_run,
        )

    async def get_special_commodity_series_candidates(
        self,
        *,
        rollout_state: Optional[str] = None,
        category: Optional[str] = None,
    ) -> Dict[str, Any]:
        storage = self._require_special_commodity_storage()
        rows = await asyncio.to_thread(
            storage.read_series_candidates,
            rollout_state=rollout_state,
            category=category,
        )
        return {"status": "success", "rollout_state": rollout_state, "category": category, "candidates": rows, "count": len(rows)}

    async def run_fx_derivation_sync(
        self,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Generate configured inverse/cross FX derivations from local observations."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxDerivationService

        return await asyncio.to_thread(
            FxDerivationService(storage, module_cfg).run,
            start_date=start_date,
            end_date=end_date,
            dry_run=dry_run,
        )

    async def run_fx_quality_check(
        self,
        *,
        as_of_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run FX quality checks and persist local issue records."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxQualityService

        return await asyncio.to_thread(
            FxQualityService(storage, module_cfg).run,
            as_of_date=as_of_date,
        )

    async def get_research_fx_readiness(
        self,
        *,
        as_of_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return FX market-data readiness."""
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        if not self.research_config.enabled:
            return {
                "enabled": False,
                "domain": "fx_market_data",
                "status": "disabled",
                "as_of_date": as_of_date or get_shanghai_time().date().isoformat(),
                "reason": "research_config.enabled is false",
                "blockers": ["research_config_disabled"],
                "warnings": [],
                "coverage": {},
                "source_profiles": {},
                "quality_issues": [],
            }
        if not module_cfg.get("enabled", False):
            return {
                "enabled": False,
                "domain": "fx_market_data",
                "status": "disabled",
                "as_of_date": as_of_date or get_shanghai_time().date().isoformat(),
                "reason": "research fx_market_data module is disabled",
                "blockers": ["fx_market_data_disabled"],
                "warnings": [],
                "coverage": {},
                "source_profiles": {},
                "quality_issues": [],
            }
        storage = self._require_fx_storage()
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(
            FxReadService(storage, module_cfg).readiness,
            as_of_date=as_of_date,
        )

    async def get_research_fx_dictionary(self) -> Dict[str, Any]:
        """Return local FX data dictionary and source metadata."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(FxReadService(storage, module_cfg).dictionary)

    async def get_research_fx_series(self, *, active_only: bool = True) -> Dict[str, Any]:
        """Return local FX series metadata."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(
            FxReadService(storage, module_cfg).series,
            active_only=active_only,
        )

    async def get_research_fx_rates(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return local FX observations for one series."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(
            FxReadService(storage, module_cfg).rates,
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    async def convert_research_fx_rate(
        self,
        *,
        from_currency: str,
        to_currency: str,
        amount: float = 1.0,
        observation_date: Optional[str] = None,
        max_lag_days: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Convert an amount through local FX observations only."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(
            FxReadService(storage, module_cfg).convert,
            from_currency=from_currency,
            to_currency=to_currency,
            amount=amount,
            observation_date=observation_date,
            max_lag_days=max_lag_days,
        )

    async def get_research_fx_indices(
        self,
        *,
        index_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return local FX currency-index metadata."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(
            FxReadService(storage, module_cfg).indices,
            index_id=index_id,
        )

    async def get_research_fx_index_observations(
        self,
        *,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return local FX currency-index observations for one series."""
        storage = self._require_fx_storage()
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        from research.fx_market_data import FxReadService

        return await asyncio.to_thread(
            FxReadService(storage, module_cfg).index_observations,
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
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
            return self._load_futures_exposure_mappings_for_dcf(
                storage,
                normalized_id,
                industry_membership=None,
            )

        return await asyncio.to_thread(_load)

    async def get_local_futures_cycle_inputs_for_dcf(
        self,
        instrument_id: str,
        *,
        industry_membership: Optional[Dict[str, Any]] = None,
        as_of_date: Optional[str] = None,
        governed_exposure_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Return local futures exposure and diagnostics for cyclical DCF."""
        storage = self._require_futures_storage()
        normalized_id = convert_to_database_format(instrument_id)

        def _load() -> Dict[str, Any]:
            exposure = self._load_futures_exposure_mappings_for_dcf(
                storage,
                normalized_id,
                industry_membership=industry_membership,
                governed_exposure_mappings=governed_exposure_mappings,
            )
            if exposure.get("status") != "success":
                return exposure
            diagnostics_by_series: Dict[str, Any] = {}
            for mapping in exposure.get("mappings", []):
                series_ids = []
                if mapping.get("revenue_series_id"):
                    series_ids.append(mapping["revenue_series_id"])
                series_ids.extend(mapping.get("cost_series_ids") or [])
                for series_id in sorted(set(series_ids)):
                    diagnostics_by_series[series_id] = storage.get_cycle_diagnostics(
                        series_id,
                        as_of_date=as_of_date,
                    )
            return {
                **exposure,
                "diagnostics_by_series": diagnostics_by_series,
            }

        return await asyncio.to_thread(_load)

    def _load_futures_exposure_mappings_for_dcf(
        self,
        storage: Any,
        instrument_id: str,
        *,
        industry_membership: Optional[Dict[str, Any]],
        governed_exposure_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if governed_exposure_mappings:
            return {
                "status": "success",
                "instrument_id": instrument_id,
                "mapping_scope": "governed_business_profile",
                "mapping_scope_id": instrument_id,
                "mappings": governed_exposure_mappings,
                "input_gaps": [],
            }
        mappings = storage.get_exposure_mappings(
            scope_type="instrument",
            scope_id=instrument_id,
        )
        if mappings:
            return {
                "status": "success",
                "instrument_id": instrument_id,
                "mapping_scope": "instrument",
                "mapping_scope_id": instrument_id,
                "mappings": mappings,
                "input_gaps": [],
            }

        tried_scope_ids: List[str] = []
        for scope_id in self._dcf_futures_industry_mapping_candidates(industry_membership):
            tried_scope_ids.append(scope_id)
            mappings = storage.get_exposure_mappings(
                scope_type="industry",
                scope_id=scope_id,
            )
            if mappings:
                return {
                    "status": "success",
                    "instrument_id": instrument_id,
                    "mapping_scope": "industry",
                    "mapping_scope_id": scope_id,
                    "mappings": mappings,
                    "input_gaps": [],
                }

        return {
            "status": "missing",
            "instrument_id": instrument_id,
            "mapping_scope": None,
            "mapping_scope_id": None,
            "mappings": [],
            "industry_mapping_candidates": tried_scope_ids,
            "input_gaps": [
                {
                    "field": "futures_exposure_mapping",
                    "requiredness": "cyclical_dcf_optional_until_mapping_rollout",
                    "reason": "no_local_futures_exposure_mapping",
                }
            ],
        }

    @staticmethod
    def _dcf_futures_industry_mapping_candidates(
        industry_membership: Optional[Dict[str, Any]],
    ) -> List[str]:
        if not isinstance(industry_membership, dict):
            return []
        candidate_fields = (
            "industry_code",
            "sw_l3_code",
            "sw_l2_code",
            "sw_l1_code",
            "best_taxonomy_industry_code",
            "mapped_industry_code",
            "industry_name",
            "sw_l3_name",
            "sw_l2_name",
            "sw_l1_name",
            "source_industry_name",
            "mapped_industry_name",
        )
        seen: Set[str] = set()
        candidates: List[str] = []
        for field_name in candidate_fields:
            value = str(industry_membership.get(field_name) or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            candidates.append(value)
        return candidates

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
        target_valuation_date = str(
            valuation_date or get_shanghai_time().date().isoformat()
        )[:10]

        financial_history: List[Dict[str, Any]] = []
        if hasattr(storage, "get_financial_statement_bundles"):
            financial_history = await asyncio.to_thread(
                self._run_financial_storage_call,
                storage,
                "get_financial_statement_bundles",
                normalized_id,
                include_statements=False,
                limit=80 if valuation_date is not None else 16,
            )
            if not isinstance(financial_history, list):
                financial_history = []
        if not financial_history:
            latest_bundle = await asyncio.to_thread(
                self._run_financial_storage_call,
                storage,
                "get_financial_statement_bundle",
                normalized_id,
                include_statements=False,
            )
            if isinstance(latest_bundle, dict):
                financial_history = [latest_bundle]
        if not financial_history:
            return None

        from research.dcf_input_governance import (
            derive_cash_and_debt,
            derive_capital_expenditure,
            enrich_instrument_with_industry,
            select_financial_bundle_as_of,
        )

        financial_bundle = select_financial_bundle_as_of(
            financial_history,
            valuation_date=target_valuation_date,
            exchange=instrument.get("exchange"),
        )
        if financial_bundle is None:
            # Compatibility for synthetic/test bundles without top-level period metadata.
            financial_bundle = deepcopy(financial_history[0])

        industry_membership = await self._get_dcf_industry_membership(
            storage,
            normalized_id,
            valuation_date=target_valuation_date,
            historical_request=valuation_date is not None,
        )
        instrument = enrich_instrument_with_industry(instrument, industry_membership)
        if industry_membership:
            financial_bundle["dcf_industry_membership"] = deepcopy(industry_membership)
            lineage = financial_bundle.get("lineage")
            if not isinstance(lineage, dict):
                lineage = {}
                financial_bundle["lineage"] = lineage
            lineage["industry_membership"] = {
                "taxonomy_system": industry_membership.get("taxonomy_system"),
                "taxonomy_version": industry_membership.get("taxonomy_version"),
                "industry_code": industry_membership.get("industry_code"),
                "sw_l1_code": industry_membership.get("sw_l1_code"),
                "sw_l2_code": industry_membership.get("sw_l2_code"),
                "sw_l3_code": industry_membership.get("sw_l3_code"),
                "mapping_status": industry_membership.get("mapping_status"),
                "effective_date": industry_membership.get("effective_date"),
                "source": industry_membership.get("source"),
                "source_mode": industry_membership.get("source_mode"),
                "data_as_of": industry_membership.get("data_as_of"),
            }

        business_profile_context = await self._resolve_business_profile_context(
            storage,
            normalized_id,
            valuation_date=target_valuation_date,
            industry_membership=industry_membership,
            include_candidates=True,
        )
        financial_bundle["business_profile_context"] = business_profile_context
        profile_lineage = financial_bundle.get("lineage")
        if not isinstance(profile_lineage, dict):
            profile_lineage = {}
            financial_bundle["lineage"] = profile_lineage
        profile_lineage["business_profile_context"] = {
            "profile_version": business_profile_context.get("profile_version"),
            "lineage_hash": business_profile_context.get("lineage_hash"),
            "data_available_cutoff": business_profile_context.get("data_available_cutoff"),
            "status": business_profile_context.get("status"),
        }

        report_period = str(
            financial_bundle.get("report_period")
            or (financial_bundle.get("latest_facts") or {}).get("report_period")
            or ""
        )[:10]
        if report_period:
            capex_context = derive_capital_expenditure(
                financial_history,
                selected_report_period=report_period,
            )
            financial_bundle["capital_expenditure_context"] = capex_context
            if capex_context.get("value") is not None:
                financial_bundle["capital_expenditure"] = capex_context["value"]
                latest_facts = financial_bundle.get("latest_facts")
                if not isinstance(latest_facts, dict):
                    latest_facts = {}
                    financial_bundle["latest_facts"] = latest_facts
                latest_facts.setdefault(
                    "capital_expenditure",
                    capex_context["value"],
                )
                capex_lineage = financial_bundle.get("lineage")
                if not isinstance(capex_lineage, dict):
                    capex_lineage = {}
                    financial_bundle["lineage"] = capex_lineage
                capex_lineage["capital_expenditure"] = capex_context

        balance_sheet_context = derive_cash_and_debt(financial_bundle)
        financial_bundle["cash_and_debt_context"] = balance_sheet_context
        for field_name in (
            "cash_and_equivalents",
            "total_debt",
            "lease_liabilities",
        ):
            if balance_sheet_context.get(field_name) is not None:
                financial_bundle[field_name] = balance_sheet_context[field_name]
                latest_facts = financial_bundle.get("latest_facts")
                if not isinstance(latest_facts, dict):
                    latest_facts = {}
                    financial_bundle["latest_facts"] = latest_facts
                latest_facts[field_name] = balance_sheet_context[field_name]
        balance_lineage = financial_bundle.get("lineage")
        if not isinstance(balance_lineage, dict):
            balance_lineage = {}
            financial_bundle["lineage"] = balance_lineage
        balance_lineage["cash_and_debt"] = balance_sheet_context

        financial_bundle = await self._enrich_dcf_bundle_with_local_shares(
            storage,
            normalized_id,
            financial_bundle,
            valuation_date=target_valuation_date,
        )
        financial_bundle = await asyncio.to_thread(
            self._enrich_dcf_bundle_with_broker_risk_control_facts,
            storage,
            normalized_id,
            financial_bundle,
        )

        latest_quotes = await self.db_ops.get_daily_data(
            instrument_id=normalized_id,
            end_date=(
                datetime.fromisoformat(target_valuation_date)
                + timedelta(days=1)
                - timedelta(microseconds=1)
            ),
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
                "valuation_date": target_valuation_date,
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
        overrides["business_profile_context"] = business_profile_context
        risk_free_rate_context = await self._get_dcf_risk_free_rate_context(
            storage,
            valuation_date=target_valuation_date,
            exchange=instrument.get("exchange"),
            currency=str(financial_bundle.get("currency") or "CNY"),
        )
        if risk_free_rate_context and risk_free_rate_context.get("value") is not None:
            overrides["risk_free_rate"] = risk_free_rate_context["value"]
            overrides["risk_free_rate_source"] = risk_free_rate_context.get("source")
            overrides["risk_free_rate_source_profile"] = risk_free_rate_context.get(
                "source_profile"
            )
            overrides["risk_free_rate_quality_flag"] = risk_free_rate_context.get(
                "quality_flag"
            )
            overrides["risk_free_rate_as_of_date"] = risk_free_rate_context.get(
                "as_of_date"
            )
            overrides["risk_free_rate_last_updated_at"] = risk_free_rate_context.get(
                "last_updated_at"
            )
            overrides["risk_free_rate_lineage_hash"] = risk_free_rate_context.get(
                "lineage_hash"
            )
            overrides["risk_free_rate_metadata"] = risk_free_rate_context
        dcf_beta_cfg = module_cfg.get("dcf", {}).get("beta", {})
        if "beta" not in overrides and dcf_beta_cfg.get("enabled", True):
            try:
                beta_payload = await self.get_research_beta(
                    normalized_id,
                    benchmark_family=dcf_beta_cfg.get("benchmark_family", "market_default"),
                    benchmark_instrument_id=dcf_beta_cfg.get("benchmark_instrument_id"),
                    window_days=int(dcf_beta_cfg.get("window_days", 252)),
                    as_of_date=date.fromisoformat(target_valuation_date),
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
                    overrides["beta_quality_flag"] = beta_result.get("quality_flag")
                    overrides["beta_interpretation_flags"] = beta_result.get(
                        "interpretation_flags"
                    ) or []
                    overrides["beta_r_squared"] = beta_result.get("r_squared")
                    overrides["beta_p_value"] = beta_result.get("p_value_beta")
                    overrides["beta_benchmark"] = {
                        "benchmark_family": beta_result.get("benchmark_family"),
                        "benchmark_instrument_id": beta_result.get("benchmark_instrument_id"),
                        "benchmark_name": beta_result.get("benchmark_name"),
                        "window_days": beta_result.get("window_days"),
                        "as_of_date": beta_result.get("as_of_date"),
                    }

        futures_cycle_context = await self._get_dcf_futures_cycle_context(
            normalized_id,
            valuation_date=target_valuation_date,
            industry_membership=industry_membership,
            business_profile_context=business_profile_context,
        )
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

        special_commodity_context = await self._get_dcf_special_commodity_context(
            valuation_date=overrides.get("valuation_date"),
            target_currency="CNY",
        )
        if special_commodity_context:
            overrides["special_commodity_market_data_context"] = special_commodity_context

        fx_context = await self._get_dcf_fx_context(
            valuation_date=overrides.get("valuation_date"),
            research_mode=research_mode,
        )
        if fx_context:
            for assumption_key, assumption in (fx_context.get("assumptions") or {}).items():
                if assumption.get("value") is None:
                    continue
                overrides[assumption_key] = assumption["value"]
                overrides[f"{assumption_key}_source"] = assumption.get("source")
                overrides[f"{assumption_key}_quality_flag"] = assumption.get("quality_flag")
                overrides[f"{assumption_key}_fallback_used"] = assumption.get("fallback_used", False)
                overrides[f"{assumption_key}_as_of_date"] = assumption.get("as_of_date")
                overrides[f"{assumption_key}_last_updated_at"] = assumption.get("last_updated_at")
                overrides[f"{assumption_key}_lineage_hash"] = assumption.get("lineage_hash")
                overrides[f"{assumption_key}_metadata"] = assumption.get("metadata") or {}
            overrides["fx_market_data_context"] = fx_context

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
        result = deepcopy(result)
        result["business_profile_context"] = business_profile_context
        if futures_cycle_context:
            cyclical_diagnostics = result.setdefault("cyclical_model_diagnostics", {})
            if isinstance(cyclical_diagnostics, dict):
                cyclical_diagnostics["futures_market_data"] = futures_cycle_context
        if special_commodity_context:
            result = deepcopy(result)
            cyclical_diagnostics = result.setdefault("cyclical_model_diagnostics", {})
            if isinstance(cyclical_diagnostics, dict):
                cyclical_diagnostics["special_commodity_market_data"] = special_commodity_context
            if special_commodity_context.get("blockers"):
                result.setdefault("warnings", [])
                for blocker in special_commodity_context.get("blockers") or []:
                    warning = f"special_commodity_market_data_{blocker}"
                    if warning not in result["warnings"]:
                        result["warnings"].append(warning)
        if fx_context:
            result = deepcopy(result)
            result["fx_market_data"] = fx_context
            if fx_context.get("blockers"):
                result.setdefault("warnings", [])
                for blocker in fx_context.get("blockers") or []:
                    warning = f"fx_market_data_{blocker}"
                    if warning not in result["warnings"]:
                        result["warnings"].append(warning)
        if isinstance(result.get("warnings"), list):
            result["warnings"] = list(dict.fromkeys(result["warnings"]))
        self._store_dcf_run_cache(cache_key, result, module_cfg)
        return result

    async def _get_dcf_industry_membership(
        self,
        storage: Any,
        instrument_id: str,
        *,
        valuation_date: str,
        historical_request: bool,
    ) -> Optional[Dict[str, Any]]:
        """Resolve authoritative Shenwan membership for the DCF valuation date."""
        if historical_request and hasattr(storage, "get_industry_membership_as_of"):
            historical = await asyncio.to_thread(
                storage.get_industry_membership_as_of,
                instrument_id,
                valuation_date,
                taxonomy_system="sw",
                include_snapshot=True,
            )
            normalized = await self._normalize_dcf_historical_industry_membership(
                storage,
                historical,
            )
            if normalized:
                return normalized
            return None
        if not hasattr(storage, "get_industry_membership"):
            return None
        return await asyncio.to_thread(
            storage.get_industry_membership,
            instrument_id,
            include_snapshot=False,
        )

    async def _resolve_business_profile_context(
        self,
        storage: Any,
        instrument_id: str,
        *,
        valuation_date: str,
        industry_membership: Optional[Dict[str, Any]],
        include_candidates: bool,
    ) -> Dict[str, Any]:
        """Resolve local business facts and executable mappings as of valuation date."""
        from research.business_profile_governance import (
            BusinessProfileRepository,
            BusinessProfileResolver,
            build_empty_business_profile_context,
        )

        futures_storage = None
        try:
            futures_storage = self._require_futures_storage()
        except RuntimeError:
            pass
        resolver = BusinessProfileResolver(
            BusinessProfileRepository(storage),
            futures_storage=futures_storage,
        )
        try:
            return await asyncio.to_thread(
                resolver.resolve,
                instrument_id,
                as_of_date=valuation_date,
                industry_membership=industry_membership,
                include_candidates=include_candidates,
            )
        except (OSError, sqlite3.Error, AttributeError, TypeError) as exc:
            dm_logger.warning(
                "[DataManager] business profile unavailable for %s: %s",
                instrument_id,
                exc,
            )
            return build_empty_business_profile_context(
                instrument_id,
                as_of_date=valuation_date,
                warning="business_profile_storage_unavailable",
            )

    async def _normalize_dcf_historical_industry_membership(
        self,
        storage: Any,
        historical: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(historical, dict):
            return None
        if historical.get("sw_l1_name"):
            return historical
        if not hasattr(storage, "list_industry_taxonomy_records"):
            return None

        taxonomy_system = str(historical.get("taxonomy_system") or "sw")
        taxonomy_version = str(historical.get("taxonomy_version") or "")
        industry_code = str(
            historical.get("mapped_industry_code")
            or historical.get("official_industry_code")
            or ""
        )
        if not industry_code:
            return None
        nodes: Dict[int, Dict[str, Any]] = {}
        current_code: Optional[str] = industry_code
        while current_code and len(nodes) < 3:
            rows = await asyncio.to_thread(
                storage.list_industry_taxonomy_records,
                taxonomy_system=taxonomy_system,
                taxonomy_version=taxonomy_version,
                industry_code=current_code,
                active_only=False,
                limit=1,
            )
            if not rows:
                return None
            node = rows[0]
            nodes[int(node.get("industry_level") or 0)] = node
            current_code = node.get("parent_code")
        if not nodes:
            return None

        leaf = nodes[max(nodes)]
        normalized = deepcopy(historical)
        normalized.update(
            {
                "industry_code": leaf.get("industry_code"),
                "industry_name": leaf.get("industry_name"),
                "industry_level": leaf.get("industry_level"),
                "parent_code": leaf.get("parent_code"),
                "mapping_status": "authoritative",
                "sw_l1_code": (nodes.get(1) or {}).get("industry_code"),
                "sw_l1_name": (nodes.get(1) or {}).get("industry_name"),
                "sw_l2_code": (nodes.get(2) or {}).get("industry_code"),
                "sw_l2_name": (nodes.get(2) or {}).get("industry_name"),
                "sw_l3_code": (nodes.get(3) or {}).get("industry_code"),
                "sw_l3_name": (nodes.get(3) or {}).get("industry_name"),
            }
        )
        return normalized

    async def _enrich_dcf_bundle_with_local_shares(
        self,
        storage: Any,
        instrument_id: str,
        financial_bundle: Dict[str, Any],
        *,
        valuation_date: str,
    ) -> Dict[str, Any]:
        """Attach the latest locally available share count to the DCF bundle."""
        if not hasattr(storage, "get_latest_valuation_input"):
            return financial_bundle
        try:
            valuation_input = await asyncio.to_thread(
                storage.get_latest_valuation_input,
                instrument_id,
                as_of_date=valuation_date,
                include_diagnostics=True,
            )
        except Exception:
            return financial_bundle
        if not isinstance(valuation_input, dict):
            return financial_bundle
        try:
            shares = float(valuation_input.get("shares_outstanding"))
        except (TypeError, ValueError):
            return financial_bundle
        if shares <= 0:
            return financial_bundle

        enriched = deepcopy(financial_bundle)
        enriched["shares_outstanding"] = shares
        latest_facts = enriched.setdefault("latest_facts", {})
        if isinstance(latest_facts, dict):
            latest_facts["shares_outstanding"] = shares
        lineage = enriched.get("lineage")
        if not isinstance(lineage, dict):
            lineage = {}
            enriched["lineage"] = lineage
        share_lineage = {
            "value": shares,
            "as_of_date": valuation_input.get("as_of_date"),
            "data_as_of": valuation_input.get("data_as_of"),
            "source": valuation_input.get("source"),
            "source_mode": valuation_input.get("source_mode"),
            "input_kind": valuation_input.get("input_kind"),
            "unit": valuation_input.get("unit"),
            "quality_flag": "local_point_in_time_valuation_input",
        }
        share_lineage["lineage_hash"] = self._stable_hash(share_lineage)
        lineage["shares_outstanding"] = share_lineage
        return enriched

    async def _get_dcf_risk_free_rate_context(
        self,
        storage: Any,
        *,
        valuation_date: str,
        exchange: Optional[str],
        currency: str,
    ) -> Optional[Dict[str, Any]]:
        """Resolve the local China 10Y yield for RMB A-share DCF."""
        if str(exchange or "").upper() not in {"SSE", "SZSE", "BSE"}:
            return None
        if str(currency or "CNY").upper() != "CNY":
            return None
        if not (
            hasattr(storage, "list_risk_free_rate_series")
            and hasattr(storage, "get_risk_free_rate_observations")
        ):
            return None
        series_rows = await asyncio.to_thread(storage.list_risk_free_rate_series)
        if not isinstance(series_rows, list):
            return None
        series = next(
            (
                item
                for item in series_rows
                if item.get("series_id") == "china_treasury_10y"
            ),
            None,
        )
        if not series:
            return None
        observations = await asyncio.to_thread(
            storage.get_risk_free_rate_observations,
            "china_treasury_10y",
            start_date=(
                date.fromisoformat(valuation_date) - timedelta(days=31)
            ).isoformat(),
            end_date=valuation_date,
        )
        if not isinstance(observations, list):
            return None
        if not observations:
            observations = await asyncio.to_thread(
                storage.get_risk_free_rate_observations,
                "china_treasury_10y",
                end_date=valuation_date,
            )
            if not isinstance(observations, list):
                return None
        eligible = [
            item
            for item in observations
            if item.get("value") is not None
            and str(item.get("observation_date") or "")[:10] <= valuation_date
        ]
        if not eligible:
            return None
        observation = max(
            eligible,
            key=lambda item: str(item.get("observation_date") or ""),
        )
        raw_value = float(observation["value"])
        unit = str(series.get("unit") or "")
        decimal_value = raw_value / 100.0 if unit == "percent_annual" else raw_value
        context = {
            "assumption_key": "risk_free_rate_rmb_10y",
            "series_id": "china_treasury_10y",
            "value": decimal_value,
            "raw_value": raw_value,
            "unit": "rate_decimal_annual",
            "raw_unit": unit,
            "currency": series.get("currency") or "CNY",
            "tenor": series.get("tenor") or "10Y",
            "as_of_date": observation.get("observation_date"),
            "last_updated_at": series.get("updated_at"),
            "source": observation.get("source") or series.get("source"),
            "source_mode": observation.get("source_mode") or series.get("source_mode"),
            "source_profile": series.get("source_profile"),
            "quality_flag": "local_interests_db_observation",
            "fallback_used": False,
            "database": "interests.db",
        }
        context["lineage_hash"] = self._stable_hash(context)
        return context

    async def _get_dcf_special_commodity_context(
        self,
        *,
        valuation_date: Optional[str],
        target_currency: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return local special-commodity diagnostics for DCF without remote fetches."""
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        special_cfg = (module_cfg or {}).get("special_commodity_market_data", {})
        if not special_cfg or not special_cfg.get("enabled", False):
            return None
        try:
            diagnostics = await self.get_special_commodity_diagnostics(
                target_currency=target_currency,
                max_fx_lag_days=None,
            )
        except Exception as exc:
            return {
                "status": "blocked",
                "source_policy": "local_commodity_db_only",
                "blockers": ["special_commodity_market_data_unavailable"],
                "warnings": [str(exc)],
                "valuation_date": str(valuation_date or get_shanghai_time().date().isoformat())[:10],
            }
        blockers: List[str] = []
        warnings: List[str] = []
        if diagnostics.get("missing_observation_series"):
            warnings.append("special_commodity_series_missing_local_observation")
        if diagnostics.get("fx_dependency_gaps"):
            blockers.append("requires_fx_conversion")
        return {
            "status": "ready" if not blockers else "blocked",
            "valuation_date": str(valuation_date or get_shanghai_time().date().isoformat())[:10],
            "source_policy": "local_commodity_db_only",
            "target_currency": target_currency,
            "series_count": diagnostics.get("series_count", 0),
            "latest_observations": diagnostics.get("latest_observations", []),
            "missing_observation_series": diagnostics.get(
                "missing_observation_series", []
            ),
            "currencies": diagnostics.get("currencies", []),
            "units": diagnostics.get("units", []),
            "fx_checks": diagnostics.get("fx_checks", []),
            "fx_dependency_gaps": diagnostics.get("fx_dependency_gaps", []),
            "blockers": blockers,
            "warnings": warnings,
        }

    async def _get_dcf_fx_context(
        self,
        *,
        valuation_date: Optional[str],
        research_mode: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Return local FX assumptions for DCF without remote fetches."""
        module_cfg = self.research_config.modules.get("fx_market_data", {})
        if not module_cfg.get("enabled", False):
            return {
                "status": "disabled",
                "source_policy": "local_fx_db_only",
                "blockers": ["fx_market_data_disabled"],
                "warnings": [],
                "assumptions": {},
                "research_mode": research_mode,
            }
        target_date = str(valuation_date or get_shanghai_time().date().isoformat())[:10]
        quality_cfg = module_cfg.get("quality") or {}
        max_lag_days = int(quality_cfg.get("max_stale_observation_days") or 5)
        assumptions: Dict[str, Dict[str, Any]] = {}
        blockers: List[str] = []
        warnings: List[str] = []
        try:
            storage = self._require_fx_storage()
            from research.fx_market_data import FxReadService
            from research.fx_market_data import build_dcf_fx_context_from_local_service

            service = FxReadService(storage, module_cfg)
            return await asyncio.to_thread(
                build_dcf_fx_context_from_local_service,
                service,
                module_cfg,
                valuation_date=target_date,
                research_mode=research_mode,
            )
        except Exception as e:
            blockers.append("fx_market_data_unavailable")
            warnings.append(str(e))
        status = "ready" if not blockers else ("research_fallback_required" if research_mode else "blocked")
        return {
            "status": status,
            "valuation_date": target_date,
            "source_policy": "local_fx_db_only",
            "max_lag_days": max_lag_days,
            "assumptions": assumptions,
            "blockers": blockers,
            "warnings": warnings,
            "research_mode": research_mode,
        }

    async def _get_dcf_futures_cycle_context(
        self,
        instrument_id: str,
        *,
        valuation_date: Optional[str] = None,
        industry_membership: Optional[Dict[str, Any]] = None,
        business_profile_context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return compact local futures diagnostics for DCF, if configured."""
        module_cfg = self.research_config.modules.get("commodity_market_data", {})
        if not module_cfg.get("enabled", False):
            return None
        try:
            research_storage = self._require_research_storage()
            if industry_membership is None:
                industry_membership = await asyncio.to_thread(
                    research_storage.get_industry_membership,
                    instrument_id,
                    include_snapshot=False,
                )
        except Exception:
            industry_membership = None
        try:
            payload = await self.get_local_futures_cycle_inputs_for_dcf(
                instrument_id,
                industry_membership=industry_membership,
                as_of_date=valuation_date,
                governed_exposure_mappings=(
                    business_profile_context.get("executable_exposure_mappings")
                    if isinstance(business_profile_context, dict)
                    else None
                ),
            )
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
        selected_mapping = None
        for mapping in payload.get("mappings") or []:
            series_ids = [mapping.get("revenue_series_id")]
            series_ids.extend(mapping.get("cost_series_ids") or [])
            if selected_series_id in {item for item in series_ids if item}:
                selected_mapping = mapping
                break
        selected_series_diagnostics = diagnostics_by_series.get(selected_series_id) or []
        diagnostics_summary = {
            str(item.get("lookback_years")): {
                "latest_price": item.get("latest_price"),
                "mean_price": item.get("mean_price"),
                "median_price": item.get("median_price"),
                "percentile": item.get("percentile"),
                "mean_deviation_pct": item.get("mean_deviation_pct"),
                "cycle_state": item.get("cycle_state"),
                "as_of_date": item.get("as_of_date"),
                "history_coverage_ratio": item.get("history_coverage_ratio"),
                "observation_count": item.get("observation_count"),
            }
            for item in selected_series_diagnostics
            if item.get("lookback_years") is not None
        }
        return {
            "status": "success",
            "instrument_id": instrument_id,
            "mapping_scope": payload.get("mapping_scope"),
            "mapping_scope_id": payload.get("mapping_scope_id"),
            "selected_series_id": selected_series_id,
            "commodity_price_assumption": selected_diagnostic.get("latest_price"),
            "cycle_index_level": selected_diagnostic.get("percentile"),
            "midcycle_price_candidate": selected_diagnostic.get("mean_price")
            or selected_diagnostic.get("median_price"),
            "price_percentile": selected_diagnostic.get("percentile"),
            "cycle_state": selected_diagnostic.get("cycle_state"),
            "diagnostic_as_of_date": selected_diagnostic.get("as_of_date"),
            "diagnostic_lookback_years": selected_diagnostic.get("lookback_years"),
            "diagnostics_summary": diagnostics_summary,
            "diagnostic": selected_diagnostic,
            "selected_mapping": selected_mapping,
            "exposure_mappings": payload.get("mappings") or [],
            "diagnostics_by_series": diagnostics_by_series,
            "input_gaps": payload.get("input_gaps") or [],
            "source_policy": "local_futures_db_only",
            "business_profile_version": (
                business_profile_context.get("profile_version")
                if isinstance(business_profile_context, dict)
                else None
            ),
            "business_profile_lineage_hash": (
                business_profile_context.get("lineage_hash")
                if isinstance(business_profile_context, dict)
                else None
            ),
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
            'max_degraded_lifecycle_fallbacks_before_warning': 0,
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
            'clip_reason_distribution': {},
            'degraded_fallback_count': 0,
            'degraded_fallback_samples': [],
            'samples': [],
            'clip_samples': [],
            'warnings': [],
            'errors': [],
            'current_master_refresh': {
                'requested': False,
                'status': 'not_requested',
                'scopes': [],
                'operator_requested': False,
            },
        }

    def _record_repair_degraded_fallback(
        self,
        diagnostics: Dict[str, Any],
        instrument: Dict[str, Any],
        window: Dict[str, Any],
    ) -> None:
        """Record one bounded degraded lifecycle fallback sample."""
        sample_limit = int(
            self._get_repair_universe_governance_config().get('sample_limit', 10)
            or 0
        )
        samples = diagnostics.setdefault('degraded_fallback_samples', [])
        if sample_limit <= 0 or len(samples) >= sample_limit:
            return
        samples.append({
            'instrument_id': instrument.get('instrument_id'),
            'symbol': instrument.get('symbol'),
            'exchange': instrument.get('exchange'),
            'type': instrument.get('type'),
            'status': instrument.get('status'),
            'reason': window.get('reason'),
            'start_date': self._date_text(window.get('start_date')),
            'end_date': self._date_text(window.get('end_date')),
            'degraded_fallback': True,
        })

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
        evidence: Optional[Dict[str, Any]] = None,
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
        if evidence:
            sample.update({
                'evidence_source': evidence.get('source'),
                'evidence_confidence': evidence.get('confidence'),
                'boundary_date': evidence.get('boundary_date'),
                'boundary_field': evidence.get('boundary_field'),
            })
        diagnostics.setdefault('samples', []).append(sample)

    def _record_repair_universe_clip(
        self,
        diagnostics: Dict[str, Any],
        *,
        reason: str,
        instrument: Dict[str, Any],
        start_date: Optional[date],
        end_date: Optional[date],
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record lifecycle clipping separately from lifecycle skips."""
        reason = str(reason or 'unknown')
        distribution = Counter(diagnostics.get('clip_reason_distribution') or {})
        distribution[reason] += 1
        diagnostics['clip_reason_distribution'] = dict(distribution)

        sample_limit = int(self._get_repair_universe_governance_config().get('sample_limit', 10) or 0)
        if sample_limit <= 0 or len(diagnostics.get('clip_samples') or []) >= sample_limit:
            return

        sample = {
            'instrument_id': instrument.get('instrument_id'),
            'symbol': instrument.get('symbol'),
            'exchange': instrument.get('exchange'),
            'type': instrument.get('type'),
            'status': instrument.get('status'),
            'reason': reason,
            'start_date': start_date.isoformat() if start_date else None,
            'end_date': end_date.isoformat() if end_date else None,
        }
        if evidence:
            sample.update({
                'evidence_source': evidence.get('source'),
                'evidence_confidence': evidence.get('confidence'),
                'boundary_date': evidence.get('boundary_date'),
                'boundary_field': evidence.get('boundary_field'),
            })
        diagnostics.setdefault('clip_samples', []).append(sample)

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
        """Read local index lifecycle evidence and choose terminal-boundary rows by authority."""
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
            if not instrument_id:
                continue
            normalized = self._normalize_index_lifecycle_evidence(row)
            existing = evidence_by_id.get(instrument_id)
            if (
                existing is None
                or int(normalized.get('_evidence_precedence', 0) or 0)
                > int(existing.get('_evidence_precedence', 0) or 0)
            ):
                evidence_by_id[instrument_id] = normalized
        return evidence_by_id

    def _normalize_index_lifecycle_evidence(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Attach repair-specific authority metadata to one index lifecycle evidence row."""
        evidence = dict(row or {})
        state = str(evidence.get('lifecycle_state') or '').strip().lower()
        source = str(evidence.get('source') or '').strip().lower()
        confidence = str(evidence.get('confidence') or '').strip().lower()
        event_type = str(evidence.get('event_type') or '').strip().lower()
        last_quote_date = self._date_from_any(evidence.get('last_quote_date'))
        effective_date = self._date_from_any(evidence.get('effective_date'))

        degraded = (
            state == 'stale_no_quote'
            or source == 'local_quote_freshness'
            or confidence in {'quote_gap', 'local_quote_gap', 'local_stale_no_quote'}
        )
        official = (
            source.startswith('cnindex')
            or source.startswith('csindex')
            or confidence in {'direct', 'official', 'series_inferred', 'official_master_metadata_only'}
        )
        inferred = (
            'inferred' in confidence
            or 'local_quote_boundary' in confidence
            or 'inferred' in event_type
            or 'inference' in source
        )

        if last_quote_date:
            boundary_date = last_quote_date
            boundary_field = 'last_quote_date'
        elif effective_date:
            boundary_date = effective_date
            boundary_field = 'effective_date'
        else:
            boundary_date = None
            boundary_field = None

        precedence = 0
        if boundary_field == 'last_quote_date' and official and not degraded and not inferred:
            precedence = 500
        elif boundary_field == 'last_quote_date' and official and inferred:
            precedence = 450
        elif boundary_field == 'last_quote_date' and not degraded:
            precedence = 350
        elif boundary_field == 'effective_date' and official and not degraded:
            precedence = 300
        elif state in {'metadata_only', 'inactive', 'calculation_terminated'}:
            precedence = 250
        elif boundary_field == 'last_quote_date' and degraded:
            precedence = 100
        elif degraded:
            precedence = 50

        if degraded:
            boundary_reason = 'index_lifecycle_stale_no_quote_fallback'
        elif boundary_field == 'last_quote_date':
            boundary_reason = 'index_lifecycle_last_quote_date'
        elif boundary_field == 'effective_date':
            boundary_reason = 'index_lifecycle_effective_date'
        else:
            boundary_reason = f'index_lifecycle_{state}' if state else 'index_lifecycle_unknown'

        evidence['_terminal_boundary'] = boundary_date
        evidence['_terminal_boundary_field'] = boundary_field
        evidence['_terminal_boundary_reason'] = boundary_reason
        evidence['_terminal_boundary_degraded'] = degraded
        evidence['_evidence_precedence'] = precedence
        return evidence

    async def _load_first_quote_dates_by_instrument(
        self,
        instrument_ids: List[str],
    ) -> Dict[str, date]:
        """Return first local quote dates for a bounded instrument set."""
        unique_ids = [item for item in dict.fromkeys(instrument_ids or []) if item]
        if not unique_ids or not hasattr(self.db_ops, 'execute_read_query'):
            return {}

        first_dates: Dict[str, date] = {}
        chunk_size = 500
        for offset in range(0, len(unique_ids), chunk_size):
            chunk = unique_ids[offset: offset + chunk_size]
            placeholders = ", ".join(f":id_{idx}" for idx in range(len(chunk)))
            params = {f"id_{idx}": instrument_id for idx, instrument_id in enumerate(chunk)}
            try:
                rows = await self.db_ops.execute_read_query(
                    f"""
                    SELECT instrument_id, MIN(date(time)) AS first_quote_date
                    FROM daily_quotes
                    WHERE instrument_id IN ({placeholders})
                    GROUP BY instrument_id
                    """,
                    params,
                )
            except Exception as exc:
                dm_logger.warning("[DataManager] Failed to load first quote dates: %s", exc)
                continue
            for row in rows or []:
                parsed = self._date_from_any(row.get('first_quote_date'))
                instrument_id = row.get('instrument_id')
                if instrument_id and parsed:
                    first_dates[instrument_id] = parsed
        return first_dates

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
        first_quote_date: Optional[date] = None,
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
            self._date_from_any(evidence.get('_terminal_boundary'))
            or self._date_from_any(evidence.get('last_quote_date'))
            or self._date_from_any(evidence.get('effective_date'))
            or delisted
        )
        index_lifecycle_boundary_reason = (
            str(evidence.get('_terminal_boundary_reason') or '').strip()
            if evidence else ''
        )
        index_lifecycle_evidence_detail = {
            'source': evidence.get('source'),
            'confidence': evidence.get('confidence'),
            'boundary_date': (
                index_lifecycle_boundary.isoformat()
                if index_lifecycle_boundary else None
            ),
            'boundary_field': evidence.get('_terminal_boundary_field'),
        } if evidence else {}
        index_lifecycle_boundary_degraded = bool(evidence.get('_terminal_boundary_degraded'))
        has_governed_index_boundary = bool(
            instrument_type == 'index'
            and exchange in {'SSE', 'SZSE'}
            and evidence
            and index_lifecycle_boundary
            and not index_lifecycle_boundary_degraded
        )

        if (
            instrument_type == 'index'
            and exchange in {'SSE', 'SZSE'}
            and status in {'delisted', 'inactive', 'calculation_terminated'}
            and delisted is not None
            and latest_quote_date is not None
            and latest_quote_date < delisted
            and delisted - latest_quote_date <= timedelta(days=7)
            and not has_governed_index_boundary
        ):
            if requested_start and requested_start > latest_quote_date:
                return {
                    'eligible': False,
                    'reason': 'index_delisted_after_last_quote_fallback',
                    'start_date': start,
                    'end_date': latest_quote_date,
                    'clipped': True,
                    'degraded_fallback': True,
                }
            if end > latest_quote_date:
                end = latest_quote_date
                lifecycle_state = 'index_delisted_last_quote_fallback'

        if (
            exchange == 'HKEX'
            and instrument_type == 'stock'
            and status == 'active'
            and trading_status in (None, 1, True, '1')
            and listed is None
        ):
            if first_quote_date is None:
                return {
                    'eligible': False,
                    'reason': 'hkex_missing_listed_date_no_local_quote',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                }
            if first_quote_date > requested_end:
                return {
                    'eligible': False,
                    'reason': 'hkex_before_local_first_quote',
                    'start_date': start,
                    'end_date': end,
                    'clipped': True,
                }
            if start < first_quote_date:
                start = first_quote_date
                lifecycle_state = 'hkex_missing_listed_date_local_first_quote'

        if instrument_type == 'index' and lifecycle_state in index_states:
            if index_lifecycle_boundary:
                if end > index_lifecycle_boundary:
                    end = index_lifecycle_boundary
                if index_lifecycle_boundary_reason:
                    lifecycle_state = index_lifecycle_boundary_reason
            else:
                return {
                    'eligible': False,
                    'reason': f'index_lifecycle_{lifecycle_state}',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                    'evidence': index_lifecycle_evidence_detail,
                }

        if (
            instrument_type == 'index'
            and exchange in {'SSE', 'SZSE'}
            and lifecycle_state not in index_states
            and lifecycle_state != 'index_delisted_last_quote_fallback'
            and not has_governed_index_boundary
            and config.get('enable_local_stale_no_quote', True)
        ):
            stale_days = int(config.get('stale_no_quote_trading_days', 10) or 10)
            if latest_quote_date is not None and requested_end > latest_quote_date + timedelta(days=stale_days):
                if end > latest_quote_date:
                    end = latest_quote_date
                lifecycle_state = 'index_lifecycle_stale_no_quote_fallback'

        if (
            exchange == 'HKEX'
            and instrument_type == 'stock'
            and trading_status in (0, '0', False)
            and status == 'suspended'
        ):
            if latest_quote_date is not None:
                if requested_start and requested_start > latest_quote_date:
                    return {
                        'eligible': False,
                        'reason': 'hkex_suspended_after_last_quote',
                        'start_date': start,
                        'end_date': end,
                        'clipped': True,
                    }
                if end > latest_quote_date:
                    end = latest_quote_date
                    lifecycle_state = 'hkex_suspended'
            else:
                return {
                    'eligible': False,
                    'reason': 'hkex_suspended_no_local_quote',
                    'start_date': start,
                    'end_date': end,
                    'clipped': False,
                }

        if (
            mode in {'historical_backfill', 'dry_run'}
            and instrument_type == 'stock'
            and exchange in {'SSE', 'SZSE', 'BSE'}
            and is_active in (False, 0, '0')
            and not delisted
            and status not in {'active', 'active_quote'}
            and config.get('enable_a_share_stock_last_quote_fallback', True)
            and latest_quote_date is not None
        ):
            if requested_start and requested_start > latest_quote_date:
                return {
                    'eligible': False,
                    'reason': 'a_share_stock_after_last_quote_fallback',
                    'start_date': start,
                    'end_date': latest_quote_date,
                    'clipped': True,
                    'degraded_fallback': True,
                }
            if end > latest_quote_date:
                end = latest_quote_date
            lifecycle_state = 'a_share_stock_delisted_last_quote_fallback'

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
                and lifecycle_state != 'a_share_stock_delisted_last_quote_fallback'
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
            elif instrument_type == 'index' and str(lifecycle_state).startswith('index_lifecycle_'):
                reason = lifecycle_state
            elif instrument_type == 'index' and lifecycle_state in index_states | {'stale_no_quote'}:
                reason = index_lifecycle_boundary_reason or f'index_lifecycle_{lifecycle_state}'
            elif lifecycle_state == 'index_delisted_last_quote_fallback':
                reason = 'index_delisted_after_last_quote_fallback'
            elif delisted and requested_start and requested_start > delisted:
                reason = 'after_delisted_date'
            else:
                reason = 'outside_lifecycle_window'
            return {
                'eligible': False,
                'reason': reason,
                'start_date': start,
                'end_date': end,
                'clipped': True,
                'evidence': index_lifecycle_evidence_detail,
                'degraded_fallback': (
                    bool(index_lifecycle_boundary_degraded)
                    or reason.endswith('_fallback')
                ),
            }

        return {
            'eligible': True,
            'start_date': start,
            'end_date': end,
            'reason': lifecycle_state if lifecycle_state else 'eligible',
            'clipped': start != requested_start or end != requested_end,
            'evidence': index_lifecycle_evidence_detail,
            'degraded_fallback': (
                bool(index_lifecycle_boundary_degraded)
                or str(lifecycle_state).endswith('_fallback')
            ),
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
        first_quote_dates_by_id = await self._load_first_quote_dates_by_instrument([
            item.get('instrument_id')
            for item in instruments or []
            if str(item.get('exchange') or '').upper() == 'HKEX'
            and str(item.get('type') or '').lower() == 'stock'
            and str(item.get('status') or '').lower() == 'active'
            and item.get('trading_status') in (None, 1, True, '1')
            and not self._date_from_any(item.get('listed_date'))
            and item.get('instrument_id')
        ])

        eligible: List[Dict[str, Any]] = []
        index_stale_checked = 0
        for instrument in instruments or []:
            latest_quote_date = None
            instrument_type = str(instrument.get('type') or '').lower()
            exchange = str(instrument.get('exchange') or '').upper()
            if (
                (
                    instrument_type == 'index'
                    and exchange in {'SSE', 'SZSE'}
                    and config.get('enable_local_stale_no_quote', True)
                )
                or (
                    instrument_type == 'stock'
                    and exchange in {'SSE', 'SZSE', 'BSE'}
                    and instrument.get('is_active') in (False, 0, '0')
                    and not self._date_from_any(instrument.get('delisted_date'))
                    and str(instrument.get('status') or '').lower()
                    not in {'active', 'active_quote'}
                    and config.get(
                        'enable_a_share_stock_last_quote_fallback', True
                    )
                )
                or (
                    instrument_type == 'stock'
                    and exchange == 'HKEX'
                    and str(instrument.get('status') or '').lower() == 'suspended'
                    and instrument.get('trading_status') in (0, '0', False)
                )
            ):
                try:
                    latest_quote_date = self._date_from_any(
                        await self.db_ops.get_latest_quote_date(instrument.get('instrument_id'))
                    )
                    if instrument_type == 'index':
                        index_stale_checked += 1
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
                first_quote_date=first_quote_dates_by_id.get(instrument.get('instrument_id')),
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
                    evidence=window.get('evidence'),
                )
                if window.get('degraded_fallback'):
                    diagnostics['degraded_fallback_count'] += 1
                    self._record_repair_degraded_fallback(
                        diagnostics, instrument, window
                    )
                continue

            item = dict(instrument)
            item['_repair_start_date'] = window.get('start_date')
            item['_repair_end_date'] = window.get('end_date')
            item['_repair_universe_reason'] = window.get('reason')
            item['_repair_universe_clipped'] = bool(window.get('clipped'))
            if window.get('clipped'):
                diagnostics['clipped_instrument_count'] += 1
                self._record_repair_universe_clip(
                    diagnostics,
                    reason=window.get('reason', 'outside_lifecycle_window'),
                    instrument=instrument,
                    start_date=window.get('start_date'),
                    end_date=window.get('end_date'),
                    evidence=window.get('evidence'),
                )
            if window.get('degraded_fallback'):
                diagnostics['degraded_fallback_count'] += 1
                self._record_repair_degraded_fallback(
                    diagnostics, instrument, window
                )
            eligible.append(item)

        if (
            index_stale_checked == 0
            and any(str(item.get('type') or '').lower() == 'index' for item in instruments or [])
        ):
            diagnostics['warnings'].append(
                'index lifecycle stale-no-quote diagnostics were not evaluated for this universe'
            )

        fallback_warning_threshold = int(
            config.get('max_degraded_lifecycle_fallbacks_before_warning', 0) or 0
        )
        if int(diagnostics.get('degraded_fallback_count', 0) or 0) > fallback_warning_threshold:
            diagnostics['warnings'].append(
                "degraded index lifecycle fallback used "
                f"{diagnostics.get('degraded_fallback_count', 0)} times"
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
        first_quote_date = None
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
        if (
            str(instrument.get('type') or '').lower() == 'stock'
            and str(instrument.get('exchange') or gap.exchange).upper() == 'HKEX'
            and str(instrument.get('status') or '').lower() == 'active'
            and instrument.get('trading_status') in (None, 1, True, '1')
            and not self._date_from_any(instrument.get('listed_date'))
        ):
            first_quote_date = (
                await self._load_first_quote_dates_by_instrument([instrument.get('instrument_id')])
            ).get(instrument.get('instrument_id'))

        window = self._resolve_repair_window_for_instrument(
            instrument,
            start_date=gap.gap_start,
            end_date=gap.gap_end,
            mode=self._normalize_repair_universe_mode(mode),
            config=config,
            lifecycle_evidence=evidence_by_id.get(instrument.get('instrument_id')),
            latest_quote_date=latest_quote_date,
            first_quote_date=first_quote_date,
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
                            'skipped_gap_segment_count', 'skipped_missing_days',
                            'degraded_fallback_count'):
                    merged_diagnostics[key] += int(diagnostics.get(key, 0) or 0)
                merged_counter = Counter(merged_diagnostics.get('reason_distribution') or {})
                merged_counter.update(diagnostics.get('reason_distribution') or {})
                merged_diagnostics['reason_distribution'] = dict(merged_counter)
                merged_clip_counter = Counter(
                    merged_diagnostics.get('clip_reason_distribution') or {}
                )
                merged_clip_counter.update(diagnostics.get('clip_reason_distribution') or {})
                merged_diagnostics['clip_reason_distribution'] = dict(merged_clip_counter)
                sample_limit = int(
                    self._get_repair_universe_governance_config().get('sample_limit', 10) or 0
                )
                if sample_limit > 0:
                    merged_diagnostics['samples'].extend(
                        diagnostics.get('samples', [])[
                            : max(0, sample_limit - len(merged_diagnostics.get('samples') or []))
                        ]
                    )
                    merged_diagnostics['clip_samples'].extend(
                        diagnostics.get('clip_samples', [])[
                            : max(0, sample_limit - len(merged_diagnostics.get('clip_samples') or []))
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
                target_dates = {
                    missing_date
                    for missing_date in (gap.missing_dates or [])
                    if effective_gap_start <= missing_date <= effective_gap_end
                }
                if not target_dates:
                    current_date = effective_gap_start
                    while current_date <= effective_gap_end:
                        target_dates.add(current_date)
                        current_date += timedelta(days=1)
                fetched_dates = {
                    parsed_date
                    for quote in data
                    for parsed_date in [
                        self._date_from_any(
                            quote.get('time')
                            or quote.get('date')
                            or quote.get('trade_date')
                        )
                    ]
                    if parsed_date is not None
                }
                if target_dates and not target_dates.issubset(fetched_dates):
                    missing_after_fetch = sorted(target_dates - fetched_dates)
                    dm_logger.warning(
                        "[DataManager] Gap fill data did not cover requested dates: "
                        "%s requested=%s fetched=%s still_missing=%s",
                        gap.instrument_id,
                        sorted(target_dates),
                        sorted(fetched_dates),
                        missing_after_fetch,
                    )
                    return False
                dm_logger.info(
                    f"[DataManager] Gap fill data fetched: {gap.instrument_id} "
                    f"{effective_gap_start} to {effective_gap_end}, rows={len(data)}"
                )
                # 保存数据
                for quote in data:
                    quote['instrument_id'] = instrument.get('instrument_id')
                success = await self.db_ops.save_daily_quotes(data)
                if success:
                    saved_dates = {
                        self._date_from_any(existing_date)
                        for existing_date in await self.db_ops.get_existing_data_dates(
                            instrument.get('instrument_id'),
                            effective_gap_start,
                            effective_gap_end,
                        )
                    }
                    saved_dates.discard(None)
                    if target_dates and not target_dates.issubset(saved_dates):
                        still_missing = sorted(target_dates - saved_dates)
                        dm_logger.warning(
                            "[DataManager] Gap fill save did not persist requested dates: "
                            "%s requested=%s saved=%s still_missing=%s",
                            gap.instrument_id,
                            sorted(target_dates),
                            sorted(saved_dates),
                            still_missing,
                        )
                        return False
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
                                await self._persist_adjustment_factor_batch(
                                    gap.exchange, factors
                                )
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
            try:
                change_watermarks = await self.db_ops.get_change_watermark_health()
            except Exception as watermark_e:
                dm_logger.warning(
                    "[DataManager] Failed to collect change watermark health: %s",
                    watermark_e,
                )
                change_watermarks = {"status": "error", "error": str(watermark_e)}

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
                'change_watermarks': change_watermarks,
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
            changelog_stats = update_results.get('changelog_stats')
            if not isinstance(changelog_stats, dict):
                changelog_stats = {}

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
                'changelog_stats': changelog_stats,
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
                                'changelog_stats': stats.get('changelog_stats', {}),
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
                'changelog_stats': update_results.get('changelog_stats', {}),
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
        if len(quote_rows) > 1:
            preferred_row, classification = self._select_preferred_quote_variant(
                quote_rows,
                key=key,
            )
            if preferred_row is not None:
                return {
                    'handled': True,
                    'key': key,
                    'classification': classification,
                    'row_count': len(rows_for_key),
                    'rows': [preferred_row],
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
    def _select_preferred_quote_variant(
        rows: List[Dict[str, Any]],
        *,
        key: str,
    ) -> tuple[Optional[Dict[str, Any]], str]:
        """Pick a deterministic quote identity when official variants share one quote code.

        CNIndex can publish price-return, total-return, and HKD variants under the
        same exchange quote code. The daily quote universe needs one canonical row;
        source-specific identities remain auditable through metadata/evidence.
        """
        if not rows:
            return None, ''

        def _metadata(row: Dict[str, Any]) -> Dict[str, Any]:
            return row.get('metadata') or {}

        price_rows = [
            row for row in rows
            if str(_metadata(row).get('price_return_type') or '').strip() == '价格指数'
        ]
        if len(price_rows) == 1:
            return price_rows[0], 'preferred_price_return_quote_identity'

        cny_rows = [
            row for row in rows
            if '港币' not in str(row.get('name') or '')
            and '港币' not in str(_metadata(row).get('full_name') or '')
        ]
        if len(cny_rows) == 1:
            return cny_rows[0], 'preferred_non_hkd_quote_identity'

        exact_symbol_rows = [
            row for row in rows
            if str(row.get('source_symbol') or row.get('symbol') or '') == str(row.get('symbol') or '')
        ]
        if len(exact_symbol_rows) == 1:
            return exact_symbol_rows[0], 'preferred_exchange_code_quote_identity'

        source_symbol_rows = [
            row for row in rows
            if str(row.get('source_symbol') or '') == str(key).split('.')[0]
        ]
        if len(source_symbol_rows) == 1:
            return source_symbol_rows[0], 'preferred_canonical_source_symbol_identity'

        return None, ''

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

    async def _deactivate_official_absent_delisting_prefixed_stocks(
        self,
        *,
        exchange: str,
        before: Dict[str, Any],
        instruments: List[Dict[str, Any]],
        source_authority: Optional[str],
    ) -> Dict[str, Any]:
        """Deactivate delisting-period A-share stocks after official current-list disappearance."""
        result: Dict[str, Any] = {
            'checked_count': 0,
            'deactivated_count': 0,
            'samples': [],
        }
        if source_authority not in {'official', 'official_with_fallback_fields'}:
            return result
        official_ids = {
            item.get('instrument_id')
            for item in instruments or []
            if item.get('instrument_id')
        }
        before_rows = before.get('rows_by_id') or {}
        for instrument_id in sorted((before.get('active_ids') or set()) - official_ids):
            row = before_rows.get(instrument_id) or {}
            name = str(row.get('name') or '').strip()
            if not name.startswith('退市'):
                continue
            result['checked_count'] += 1
            latest_quote_date = None
            try:
                latest_quote = await self.db_ops.get_latest_quote_date(instrument_id)
                latest_quote_date = self._date_from_any(latest_quote)
            except Exception:
                latest_quote_date = None
            marker = getattr(self.db_ops, 'mark_instrument_delisted', None)
            if not callable(marker):
                continue
            updated = marker(
                instrument_id,
                delisted_date=None,
                source=f'{exchange.lower()}_official_current_list_absence',
            )
            if inspect.isawaitable(updated):
                updated = await updated
            if updated:
                result['deactivated_count'] += 1
                if len(result['samples']) < 20:
                    result['samples'].append({
                        'instrument_id': instrument_id,
                        'name': name,
                        'last_quote_date': self._date_text(latest_quote_date),
                    })
        return result

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
            'source_symbol', 'lot_size', 'tick_size',
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
            if row.get('lot_size') is not None:
                item['lot_size'] = row.get('lot_size')
            if row.get('tick_size') is not None:
                item['tick_size'] = row.get('tick_size')
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
                official_absent_delisting = await self._deactivate_official_absent_delisting_prefixed_stocks(
                    exchange=exchange,
                    before=before,
                    instruments=instruments or [],
                    source_authority=source_authority,
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
                    'official_absent_delisting': official_absent_delisting,
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

    @staticmethod
    def _bounded_samples(items: List[Dict[str, Any]], limit: int = 10) -> List[Dict[str, Any]]:
        samples: List[Dict[str, Any]] = []
        for item in items[: max(0, int(limit or 0))]:
            samples.append({
                'instrument_id': item.get('instrument_id'),
                'symbol': item.get('symbol'),
                'name': item.get('name'),
                'exchange': item.get('exchange'),
                'listed_date': DataManager._date_text(item.get('listed_date')),
                'delisted_date': DataManager._date_text(item.get('delisted_date')),
                'quote_rows': int(item.get('quote_rows') or 0),
                'first_quote_date': DataManager._date_text(item.get('first_quote_date')),
                'last_quote_date': DataManager._date_text(item.get('last_quote_date')),
                'coverage_status': item.get('coverage_status'),
            })
        return samples

    async def get_delisted_a_share_quote_backfill_coverage(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        delisted_year_start: Optional[int] = None,
        delisted_year_end: Optional[int] = None,
        instrument_ids: Optional[List[str]] = None,
        include_already_covered: bool = False,
        limit: Optional[int] = None,
        sample_limit: int = 10,
    ) -> Dict[str, Any]:
        """Return local coverage evidence for delisted A-share quote backfill."""
        candidates = await self.db_ops.get_delisted_a_share_quote_backfill_candidates(
            exchanges=exchanges,
            delisted_year_start=delisted_year_start,
            delisted_year_end=delisted_year_end,
            instrument_ids=instrument_ids,
            include_already_covered=include_already_covered,
            limit=limit,
        )
        by_year = await self.db_ops.get_delisted_a_share_quote_coverage_by_year(
            exchanges=exchanges,
            delisted_year_start=delisted_year_start,
            delisted_year_end=delisted_year_end,
        )
        status_counts = Counter(str(item.get('coverage_status') or 'unknown') for item in candidates)
        return {
            'status': 'success',
            'target_count': len(candidates),
            'coverage_status_counts': dict(status_counts),
            'coverage_by_year': by_year,
            'samples': self._bounded_samples(candidates, sample_limit),
        }

    async def run_delisted_a_share_quote_backfill(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        delisted_year_start: Optional[int] = None,
        delisted_year_end: Optional[int] = None,
        delisted_start_date: Optional[Union[str, date, datetime]] = None,
        delisted_end_date: Optional[Union[str, date, datetime]] = None,
        instrument_ids: Optional[List[str]] = None,
        include_already_covered: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = True,
        per_instrument_timeout_sec: Optional[int] = None,
        fail_fast: bool = False,
        sample_limit: int = 10,
        progress_log_every: int = 25,
    ) -> Dict[str, Any]:
        """Backfill historical quotes for locally known delisted A-share stocks."""
        started_at = get_shanghai_time()
        candidates = await self.db_ops.get_delisted_a_share_quote_backfill_candidates(
            exchanges=exchanges,
            delisted_year_start=delisted_year_start,
            delisted_year_end=delisted_year_end,
            delisted_start_date=delisted_start_date,
            delisted_end_date=delisted_end_date,
            instrument_ids=instrument_ids,
            include_already_covered=include_already_covered,
            limit=limit,
        )
        coverage_before = await self.db_ops.get_delisted_a_share_quote_coverage_by_year(
            exchanges=exchanges,
            delisted_year_start=delisted_year_start,
            delisted_year_end=delisted_year_end,
        )
        result: Dict[str, Any] = {
            'operation': 'delisted_a_share_quote_backfill',
            'dry_run': bool(dry_run),
            'started_at': started_at.isoformat(),
            'filters': {
                'exchanges': exchanges or ['SSE', 'SZSE', 'BSE'],
                'delisted_year_start': delisted_year_start,
                'delisted_year_end': delisted_year_end,
                'delisted_start_date': self._date_text(delisted_start_date),
                'delisted_end_date': self._date_text(delisted_end_date),
                'instrument_ids': instrument_ids or [],
                'include_already_covered': include_already_covered,
                'limit': limit,
            },
            'target_count': len(candidates),
            'processed_count': 0,
            'already_covered_count': 0,
            'saved_rows': 0,
            'source_empty_count': 0,
            'failure_count': 0,
            'timeout_count': 0,
            'skipped_lifecycle_count': 0,
            'coverage_before': coverage_before,
            'samples': {
                'targets': self._bounded_samples(candidates, sample_limit),
                'source_empty': [],
                'failures': [],
                'skipped_lifecycle': [],
                'saved': [],
            },
            'errors': [],
        }

        if dry_run:
            result['status'] = 'dry_run'
            result['finished_at'] = get_shanghai_time().isoformat()
            return result

        if self.source_factory is None:
            from data_sources.source_factory import get_data_source_factory

            self.source_factory = await get_data_source_factory(self.db_ops)

        sample_limit = max(0, int(sample_limit or 0))
        for idx, instrument in enumerate(candidates, start=1):
            instrument_id = instrument.get('instrument_id')
            listed = self._date_from_any(instrument.get('listed_date'))
            delisted = self._date_from_any(instrument.get('delisted_date'))
            if not listed or not delisted or listed > delisted:
                result['skipped_lifecycle_count'] += 1
                if len(result['samples']['skipped_lifecycle']) < sample_limit:
                    result['samples']['skipped_lifecycle'].append(self._bounded_samples([instrument], 1)[0])
                continue

            if instrument.get('coverage_status') == 'covered' and not include_already_covered:
                result['already_covered_count'] += 1
                continue

            async def _fetch() -> list:
                return await self.source_factory.get_daily_data(
                    instrument.get('exchange'),
                    instrument_id,
                    instrument.get('symbol'),
                    datetime.combine(listed, datetime.min.time()),
                    datetime.combine(delisted, datetime.max.time()),
                    instrument_type='stock',
                    source_symbol=instrument.get('source_symbol') or '',
                )

            try:
                if per_instrument_timeout_sec:
                    rows = await asyncio.wait_for(_fetch(), timeout=per_instrument_timeout_sec)
                else:
                    rows = await _fetch()
                result['processed_count'] += 1
                if rows:
                    await self.db_ops.save_daily_quotes(rows)
                    result['saved_rows'] += len(rows)
                    if len(result['samples']['saved']) < sample_limit:
                        result['samples']['saved'].append({
                            'instrument_id': instrument_id,
                            'symbol': instrument.get('symbol'),
                            'name': instrument.get('name'),
                            'exchange': instrument.get('exchange'),
                            'rows': len(rows),
                            'start_date': listed.isoformat(),
                            'end_date': delisted.isoformat(),
                        })
                else:
                    result['source_empty_count'] += 1
                    if len(result['samples']['source_empty']) < sample_limit:
                        result['samples']['source_empty'].append(self._bounded_samples([instrument], 1)[0])
            except asyncio.TimeoutError:
                result['processed_count'] += 1
                result['timeout_count'] += 1
                result['failure_count'] += 1
                message = f"{instrument_id}: timeout after {per_instrument_timeout_sec}s"
                result['errors'].append(message)
                if len(result['samples']['failures']) < sample_limit:
                    failure = self._bounded_samples([instrument], 1)[0]
                    failure['error'] = message
                    result['samples']['failures'].append(failure)
                if fail_fast:
                    break
            except Exception as exc:
                result['processed_count'] += 1
                result['failure_count'] += 1
                message = f"{instrument_id}: {exc}"
                result['errors'].append(message)
                if len(result['samples']['failures']) < sample_limit:
                    failure = self._bounded_samples([instrument], 1)[0]
                    failure['error'] = str(exc)
                    result['samples']['failures'].append(failure)
                if fail_fast:
                    break

            if progress_log_every and idx % int(progress_log_every) == 0:
                dm_logger.info(
                    "[DataManager] Delisted A-share quote backfill progress: %s/%s saved_rows=%s empty=%s failures=%s",
                    idx,
                    len(candidates),
                    result['saved_rows'],
                    result['source_empty_count'],
                    result['failure_count'],
                )

        result['coverage_after'] = await self.db_ops.get_delisted_a_share_quote_coverage_by_year(
            exchanges=exchanges,
            delisted_year_start=delisted_year_start,
            delisted_year_end=delisted_year_end,
        )
        result['status'] = 'success' if result['failure_count'] == 0 else 'warning'
        finished_at = get_shanghai_time()
        result['finished_at'] = finished_at.isoformat()
        result['elapsed_sec'] = round((finished_at - started_at).total_seconds(), 3)
        return result

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
                'terminal_boundary_inferred_count': 0,
                'terminal_boundary_missing_count': 0,
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

        def _quote_candidate_ids(row: Dict[str, Any]) -> List[str]:
            exchange_suffix = {
                'SSE': 'SH',
                'SZSE': 'SZ',
                'BSE': 'BJ',
            }.get(str(row.get('exchange') or '').upper())
            candidates: List[str] = []

            def _add(candidate: Any) -> None:
                text = str(candidate or '').strip().upper()
                if text and text not in candidates:
                    candidates.append(text)

            _add(row.get('instrument_id'))
            if exchange_suffix:
                for key in ('matched_code', 'symbol'):
                    code = str(row.get(key) or '').strip()
                    if len(code) == 6 and code.isdigit():
                        _add(f'{code}.{exchange_suffix}')
            return candidates

        direct_evidence_missing_boundary: List[Dict[str, Any]] = []
        for row in direct_evidence:
            if self._date_from_any(row.get('last_quote_date')):
                continue
            effective_date = self._date_from_any(row.get('effective_date'))
            latest_quote = None
            latest_quote_instrument_id = None
            for candidate_id in _quote_candidate_ids(row):
                try:
                    candidate_latest = await self.db_ops.get_latest_quote_date(candidate_id)
                except Exception:
                    candidate_latest = None
                candidate_date = self._date_from_any(candidate_latest)
                if not candidate_date:
                    continue
                latest_quote = candidate_latest
                latest_quote_instrument_id = candidate_id
                break
            latest_date = self._date_from_any(latest_quote)
            if latest_date and (effective_date is None or latest_date <= effective_date):
                row['last_quote_date'] = latest_date
                row['confidence'] = 'direct_lifecycle_local_quote_boundary'
                diagnostics = row.get('diagnostics') if isinstance(row.get('diagnostics'), dict) else {}
                row['diagnostics'] = {
                    **diagnostics,
                    'terminal_boundary_inference': 'local_latest_quote_on_or_before_effective_date',
                    'latest_quote_date': latest_date.isoformat(),
                    'quote_boundary_instrument_id': latest_quote_instrument_id,
                }
                result['summary']['terminal_boundary_inferred_count'] += 1
            else:
                direct_evidence_missing_boundary.append(row)
        if direct_evidence_missing_boundary:
            result['summary']['terminal_boundary_missing_count'] += len(
                direct_evidence_missing_boundary
            )
            result['warnings'].append(
                "index terminal quote boundary missing for "
                f"{len(direct_evidence_missing_boundary)} direct lifecycle evidence rows"
            )
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
                    await self._persist_adjustment_factor_batch(exchange, factors)
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
        return {min_start + timedelta(days=offset) for offset in range(span_days + 1)}

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

    async def rebuild_a_share_adjustment_factor_governance(
        self,
        *,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        source: str = "akshare",
        dry_run: bool = True,
        resume: bool = True,
        chunk_size: int = 100,
        request_interval_seconds: float = 1.0,
        checkpoint_id: Optional[str] = None,
        build_canonical: bool = True,
    ) -> Dict[str, Any]:
        """Backfill isolated observations and build a gated canonical series."""
        from utils.a_share_historical_backfill import (
            AShareBackfillCheckpointStore,
            coerce_date,
            normalize_string_list,
        )
        from data_sources.adjustment_factor_governance import (
            build_canonical_series,
            build_event_product_path,
            compare_normalized_cumulative_paths,
            normalize_source_path,
            reconcile_factor_events,
            source_transition_metrics,
        )

        normalized_start = coerce_date(start_date, field_name="start_date")
        normalized_end = coerce_date(end_date, field_name="end_date")
        if normalized_end < normalized_start:
            raise ValueError("end_date must not be earlier than start_date")
        normalized_exchanges = [
            item.upper() for item in normalize_string_list(exchanges)
        ] or ["SSE", "SZSE", "BSE"]
        unsupported = sorted(set(normalized_exchanges) - {"SSE", "SZSE", "BSE"})
        if unsupported:
            raise ValueError(f"unsupported A-share exchanges: {unsupported}")
        normalized_ids = set(normalize_string_list(instrument_ids))
        normalized_source = str(source or "akshare").strip().lower()
        if normalized_source not in {"akshare", "baostock"}:
            raise ValueError("source must be akshare or baostock")
        chunk_size = max(1, min(int(chunk_size), 1000))
        request_interval_seconds = max(0.0, float(request_interval_seconds))

        universe: List[Dict[str, Any]] = []
        for exchange in normalized_exchanges:
            instruments = await self.db_ops.get_instruments_list(
                exchange=exchange, type="stock", is_active=None
            )
            for instrument in instruments:
                instrument_id = str(instrument.get("instrument_id") or "")
                if normalized_ids and instrument_id not in normalized_ids:
                    continue
                universe.append({
                    "instrument_id": instrument_id,
                    "symbol": str(instrument.get("symbol") or ""),
                    "exchange": exchange,
                })
        universe.sort(key=lambda item: (item["exchange"], item["instrument_id"]))
        universe_ids = {item["instrument_id"] for item in universe}
        missing_requested_ids = sorted(normalized_ids - universe_ids)
        if missing_requested_ids:
            raise ValueError(
                "requested instruments are absent from the selected historical universe: "
                f"{missing_requested_ids[:20]}"
            )
        if not universe:
            raise ValueError("A-share historical factor universe is empty")

        governance = self.data_config.get("adjustment_factor_governance", {})
        target_series_version = str(
            governance.get("canonical_series_version", "a_share_event_product_v1")
        ).strip()
        if not target_series_version or len(target_series_version) > 64:
            raise ValueError("canonical_series_version must contain 1 to 64 characters")
        source_priority = list(
            governance.get("source_priority") or ["akshare", "tdx_xdxr", "baostock"]
        )
        parameters = {
            "start_date": normalized_start,
            "end_date": normalized_end,
            "exchanges": normalized_exchanges,
            "instrument_ids": sorted(normalized_ids),
            "source": normalized_source,
            "dry_run": bool(dry_run),
            "resume": bool(resume),
            "chunk_size": chunk_size,
            "request_interval_seconds": request_interval_seconds,
            "build_canonical": bool(build_canonical),
            "series_version": target_series_version,
        }
        identity = hashlib.sha256(json.dumps(
            {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in parameters.items() if key not in {"resume", "dry_run"}
            },
            sort_keys=True,
        ).encode("utf-8")).hexdigest()[:16]
        checkpoint_store = AShareBackfillCheckpointStore(
            self.data_config.get("data_dir", "data")
        )
        resolved_checkpoint_id = checkpoint_id or f"a_share_factor_{identity}"
        staging_suffix = f"__staging__{identity}"
        staging_series_version = (
            f"{target_series_version[:64 - len(staging_suffix)]}{staging_suffix}"
        )
        target_ids = [item["instrument_id"] for item in universe]
        existing_observations = await self.db_ops.list_adjustment_factor_observations(
            instrument_ids=target_ids,
            sources=[normalized_source],
            start_date=normalized_start,
            end_date=normalized_end,
        )
        existing_observation_instruments = {
            row["instrument_id"] for row in existing_observations
        }
        checkpoint_parameters = {
            key: value for key, value in parameters.items() if key != "dry_run"
        }
        checkpoint = None
        stage: Dict[str, Any] = {
            "completed_instruments": [], "empty_instruments": [], "errors": []
        }
        completed: set[str] = set()
        if not dry_run:
            checkpoint = (
                checkpoint_store.load(resolved_checkpoint_id, checkpoint_parameters)
                if resume else None
            )
            if checkpoint is None:
                checkpoint = checkpoint_store.initialize(
                    resolved_checkpoint_id, checkpoint_parameters, universe
                )
            stage = checkpoint.setdefault("stages", {}).setdefault(
                "factor_governance",
                {"completed_instruments": [], "empty_instruments": [], "errors": []},
            )
            completed = set(stage.get("completed_instruments") or []) if resume else set()
            stage["errors"] = [
                error for error in (stage.get("errors") or [])
                if error.get("instrument_id") not in completed
            ]
        pending = [item for item in universe if item["instrument_id"] not in completed]

        result: Dict[str, Any] = {
            "status": "dry_run" if dry_run else "running",
            "dry_run": bool(dry_run),
            "checkpoint_id": resolved_checkpoint_id,
            "parameters": {
                **parameters,
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
            },
            "universe": {
                "instrument_count": len(universe),
                "completed_count": len(completed),
                "pending_count": len(pending),
            },
            "observations": {
                "existing_rows": len(existing_observations),
                "existing_instruments": len({
                    row["instrument_id"] for row in existing_observations
                }),
            },
            "canonical": {},
            "target_series_version": target_series_version,
            "staging_series_version": staging_series_version,
        }
        if dry_run:
            return result

        source_instance = self.source_factory._find_source_by_base_name(normalized_source)
        if source_instance is None:
            result.update({
                "status": "failed",
                "errors": [f"source unavailable: {normalized_source}"],
            })
            return result

        counters = {
            "requested_instruments": 0,
            "completed_instruments": len(completed),
            "empty_instruments": len(set(stage.get("empty_instruments") or [])),
            "observation_inserted": 0,
            "observation_changed": 0,
            "observation_unchanged": 0,
            "errors": 0,
        }
        for index, item in enumerate(pending, start=1):
            instrument_id = item["instrument_id"]
            stage["errors"] = [
                error for error in (stage.get("errors") or [])
                if error.get("instrument_id") != instrument_id
            ]
            try:
                counters["requested_instruments"] += 1
                factors = await source_instance.get_adjustment_factors(
                    instrument_id,
                    item["symbol"],
                    datetime.combine(normalized_start, datetime.min.time()),
                    datetime.combine(normalized_end, datetime.max.time()),
                )
                if factors is None:
                    raise RuntimeError("source returned indeterminate factor response")
                observations = normalize_source_path(
                    factors,
                    normalization_version=str(
                        governance.get("normalization_version", "event_ratio_v1")
                    ),
                )
                observed_sources = {
                    str(row.get("source") or "").lower() for row in observations
                }
                if observed_sources - {normalized_source}:
                    raise RuntimeError(
                        "source identity mismatch: "
                        f"expected {normalized_source}, got {sorted(observed_sources)}"
                    )
                if (
                    not observations
                    and instrument_id in existing_observation_instruments
                ):
                    raise RuntimeError(
                        "source returned empty history despite existing observations"
                    )
                write_stats = await self.db_ops.save_adjustment_factor_observations(
                    observations,
                    ingestion_run_id=resolved_checkpoint_id,
                )
                processed_observations = sum(
                    int(write_stats.get(key, 0))
                    for key in ("inserted", "changed", "unchanged")
                )
                if (
                    int(write_stats.get("failed", 0)) > 0
                    or processed_observations != len(observations)
                ):
                    raise RuntimeError(
                        "source observation persistence incomplete: "
                        f"expected={len(observations)} stats={write_stats}"
                    )
                for key in ("inserted", "changed", "unchanged"):
                    counters[f"observation_{key}"] += int(write_stats.get(key, 0))
                if not observations:
                    empty_instruments = set(stage.get("empty_instruments") or [])
                    empty_instruments.add(instrument_id)
                    stage["empty_instruments"] = sorted(empty_instruments)
                else:
                    stage["empty_instruments"] = sorted(
                        set(stage.get("empty_instruments") or []) - {instrument_id}
                    )
                stage["completed_instruments"] = sorted(
                    set(stage.get("completed_instruments") or []) | {instrument_id}
                )
                completed.add(instrument_id)
                counters["completed_instruments"] += 1
            except Exception as exc:
                stage.setdefault("errors", []).append({
                    "instrument_id": instrument_id,
                    "reason": str(exc),
                })
            counters["empty_instruments"] = len(stage.get("empty_instruments") or [])
            counters["errors"] = len(stage.get("errors") or [])
            assert checkpoint is not None
            checkpoint_store.save(checkpoint)
            if index % chunk_size == 0:
                dm_logger.info(
                    "[DataManager] A-share factor rebuild progress: %d/%d completed=%d errors=%d",
                    index, len(pending), counters["completed_instruments"], counters["errors"],
                )
            if request_interval_seconds:
                await asyncio.sleep(request_interval_seconds)

        observation_rows = await self.db_ops.list_adjustment_factor_observations(
            instrument_ids=target_ids,
            sources=[normalized_source],
            start_date=normalized_start,
            end_date=normalized_end,
        )
        canonical_rows, canonical_summary = build_canonical_series(
            observation_rows,
            series_version=staging_series_version,
            source_priority=source_priority,
            target_instruments=target_ids,
            completed_sources={normalized_source: sorted(completed)},
        )

        legacy_rows: List[Dict[str, Any]] = []
        tdx_rows: List[Dict[str, Any]] = []
        for offset in range(0, len(target_ids), 500):
            chunk = target_ids[offset: offset + 500]
            placeholders = ", ".join(f":id_{index}" for index in range(len(chunk)))
            query_parameters: Dict[str, Any] = {
                f"id_{index}": instrument_id
                for index, instrument_id in enumerate(chunk)
            }
            query_parameters.update({
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
            })
            legacy_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date,
                       factor, cumulative_factor, source
                FROM adjustment_factors
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                ORDER BY instrument_id, ex_date
                """,
                query_parameters,
            ))
            tdx_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date,
                       factor, cumulative_factor, validation_result
                FROM adjustment_factors_tdx
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                  AND factor IS NOT NULL
                  AND factor > 0
                  AND COALESCE(validation_result, '') NOT LIKE 'pending_%'
                ORDER BY instrument_id, ex_date
                """,
                query_parameters,
            ))

        sessions_by_exchange: Dict[str, List[date]] = {}
        for exchange in normalized_exchanges:
            calendar_rows = await self.db_ops.get_trading_calendar_records(
                exchange,
                normalized_start - timedelta(days=14),
                normalized_end + timedelta(days=14),
            )
            sessions_by_exchange[exchange] = [
                parsed_date
                for row in calendar_rows
                if row.get("is_trading_day")
                if (parsed_date := self._date_from_any(row.get("date"))) is not None
            ]

        event_reconciliation = reconcile_factor_events(
            canonical_rows,
            tdx_rows,
            sessions_by_exchange=sessions_by_exchange,
            factor_tolerance_pct=float(
                governance.get("quality_thresholds", {}).get(
                    "max_tdx_factor_error_pct", 0.5
                )
            ),
        )
        tdx_event_product_rows = build_event_product_path(tdx_rows)
        tdx_path_comparison = compare_normalized_cumulative_paths(
            canonical_rows, tdx_event_product_rows
        )
        legacy_path_comparison = compare_normalized_cumulative_paths(
            canonical_rows, legacy_rows
        )
        legacy_transition_metrics = source_transition_metrics(legacy_rows)

        coverage_ratio = len(completed) / len(universe) if universe else 1.0
        thresholds = governance.get("quality_thresholds", {})
        conflict_ratio = (
            canonical_summary["conflict_count"] / canonical_summary["row_count"]
            if canonical_summary["row_count"] else 0.0
        )
        full_market_parameters = (
            not normalized_ids
            and set(normalized_exchanges) == {"SSE", "SZSE", "BSE"}
            and normalized_start <= date(1990, 12, 19)
        )
        latest_trading_dates: Dict[str, Optional[date]] = {}
        if full_market_parameters:
            for exchange in normalized_exchanges:
                latest_trading_dates[exchange] = self._date_from_any(
                    await self.db_ops.get_previous_trading_day(
                        exchange, date.today() + timedelta(days=1)
                    )
                )
        full_market_scope = (
            full_market_parameters
            and all(latest_trading_dates.values())
            and normalized_end >= max(
                trading_date
                for trading_date in latest_trading_dates.values()
                if trading_date is not None
            )
        )
        tdx_discrepancy_ratio = event_reconciliation.get("discrepancy_ratio")
        tdx_adjusted_error = tdx_path_comparison.get(
            "max_adjusted_price_error_pct"
        )
        quality_gates = {
            "full_market_scope": full_market_scope,
            "coverage": coverage_ratio >= float(
                thresholds.get("min_instrument_coverage_ratio", 0.99)
            ),
            "source_internal_consistency": conflict_ratio <= float(
                thresholds.get("max_conflict_ratio", 0.001)
            ),
            "tdx_event_reconciliation": (
                tdx_discrepancy_ratio is not None
                and tdx_discrepancy_ratio <= float(
                    thresholds.get("max_tdx_discrepancy_ratio", 0.02)
                )
            ),
            "tdx_adjusted_price_equivalence": (
                tdx_adjusted_error is not None
                and tdx_adjusted_error <= float(
                    thresholds.get(
                        "max_tdx_adjusted_price_error_pct",
                        thresholds.get("warning_cumulative_error_pct", 0.5),
                    )
                )
            ),
            "no_download_errors": counters["errors"] == 0,
        }
        promotion_eligible = all(quality_gates.values())
        combined_samples = [
            *(canonical_summary.get("samples") or []),
            *(event_reconciliation.get("samples") or []),
            *(
                tdx_path_comparison.get("samples") or []
            ),
            *(
                legacy_path_comparison.get("samples") or []
            ),
        ][:20]
        canonical_summary.update({
            "status": "success" if promotion_eligible else "partial",
            "source_priority": source_priority,
            "start_date": normalized_start.isoformat(),
            "end_date": normalized_end.isoformat(),
            "coverage_ratio": coverage_ratio,
            "conflict_ratio": conflict_ratio,
            "event_reconciliation": event_reconciliation,
            "tdx_adjusted_price_comparison": tdx_path_comparison,
            "legacy_adjusted_price_comparison": legacy_path_comparison,
            "legacy_source_transition_metrics": legacy_transition_metrics,
            "max_cumulative_error_pct": tdx_adjusted_error,
            "quality_gates": quality_gates,
            "latest_trading_dates": {
                exchange: trading_date.isoformat() if trading_date else None
                for exchange, trading_date in latest_trading_dates.items()
            },
            "samples": combined_samples,
            "promotion_eligible": promotion_eligible,
            "target_series_version": target_series_version,
            "staging_series_version": staging_series_version,
        })
        if build_canonical:
            event_counts: Dict[str, int] = {}
            for row in canonical_rows:
                instrument_id = str(row.get("instrument_id") or "")
                event_counts[instrument_id] = event_counts.get(instrument_id, 0) + 1
            instrument_status_rows = [{
                "instrument_id": item["instrument_id"],
                "source": normalized_source,
                "coverage_status": (
                    "complete_with_events"
                    if item["instrument_id"] in completed
                    and event_counts.get(item["instrument_id"], 0) > 0
                    else "complete_no_events"
                    if item["instrument_id"] in completed
                    else "incomplete"
                ),
                "event_count": event_counts.get(item["instrument_id"], 0),
                "start_date": normalized_start,
                "end_date": normalized_end,
                "ingestion_run_id": resolved_checkpoint_id,
            } for item in universe]
            canonical_summary["saved_rows"] = await self.db_ops.replace_canonical_adjustment_factors(
                canonical_rows,
                series_version=staging_series_version,
                instrument_ids=target_ids,
            )
            canonical_summary["saved_instrument_statuses"] = (
                await self.db_ops.replace_adjustment_factor_instrument_statuses(
                    instrument_status_rows,
                    series_version=staging_series_version,
                    instrument_ids=target_ids,
                )
            )
            staging_report = {
                **canonical_summary,
                "status": "validated_staging" if promotion_eligible else "partial",
                "candidate_promotion_eligible": promotion_eligible,
                "promotion_eligible": False,
            }
            await self.db_ops.upsert_adjustment_factor_series_status(
                staging_series_version, staging_report
            )
            canonical_summary["status_persisted"] = True
            canonical_summary["promoted"] = False
            if promotion_eligible:
                canonical_summary["promotion_result"] = (
                    await self.db_ops.promote_canonical_adjustment_factor_series(
                        staging_series_version=staging_series_version,
                        target_series_version=target_series_version,
                        report=canonical_summary,
                    )
                )
                canonical_summary["promoted"] = True
        result["observations"] = counters
        result["canonical"] = canonical_summary
        result["status"] = "success" if promotion_eligible else "partial"
        result["universe"]["completed_count"] = len(completed)
        result["universe"]["pending_count"] = len(universe) - len(completed)
        assert checkpoint is not None
        checkpoint_store.save(checkpoint)
        self.invalidate_factor_cache()
        return result

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
                        persist_result = await self._persist_adjustment_factor_batch(
                            exchange, factors
                        )
                        saved_count = int(persist_result.get('saved', 0))
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

    @staticmethod
    def _build_tdx_raw_event_row(
        instrument_id: str,
        event: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Convert one raw TDX XDXR event into the audit-table contract."""
        ex_date = DataManager._date_from_any(event.get('date') or event.get('ex_date'))
        if ex_date is None:
            return None
        return {
            'instrument_id': instrument_id,
            'ex_date': datetime.combine(ex_date, datetime.min.time()),
            'factor': 1.0,
            'cumulative_factor': 1.0,
            'pre_close': 0.0,
            'fenhong': float(event.get('fenhong', 0.0) or 0.0),
            'songzhuangu': float(event.get('songzhuangu', 0.0) or 0.0),
            'peigu': float(event.get('peigu', 0.0) or 0.0),
            'peigujia': float(event.get('peigujia', 0.0) or 0.0),
            'validation_result': 'pending_factor_missing_pre_close',
            'conflict_reason': 'missing_pre_close',
            'source': 'tdx_xdxr',
        }

    async def get_tdx_xdxr_pending_factor_summary(
        self,
        *,
        start_date: date,
        end_date: date,
        instrument_ids: List[str],
        sample_limit: int = 20,
    ) -> Dict[str, Any]:
        """Summarize persisted XDXR events that still lack prior-close evidence."""
        normalized_ids = list(dict.fromkeys(
            str(item).strip() for item in (instrument_ids or []) if str(item).strip()
        ))
        rows: List[Dict[str, Any]] = []
        for offset in range(0, len(normalized_ids), 500):
            chunk = normalized_ids[offset: offset + 500]
            placeholders = ", ".join(f":id_{idx}" for idx in range(len(chunk)))
            params: Dict[str, Any] = {
                f"id_{idx}": instrument_id
                for idx, instrument_id in enumerate(chunk)
            }
            params.update({
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
            })
            rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date,
                       fenhong, songzhuangu, peigu, peigujia
                FROM adjustment_factors_tdx
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                  AND validation_result = 'pending_factor_missing_pre_close'
                ORDER BY instrument_id, ex_date
                """,
                params,
            ))

        pending_ids = sorted({row.get('instrument_id') for row in rows if row.get('instrument_id')})
        cash_events = sum(float(row.get('fenhong') or 0.0) > 0 for row in rows)
        samples = [
            {
                'instrument_id': row.get('instrument_id'),
                'ex_date': self._date_text(row.get('ex_date')),
                'fenhong': float(row.get('fenhong') or 0.0),
                'songzhuangu': float(row.get('songzhuangu') or 0.0),
                'peigu': float(row.get('peigu') or 0.0),
                'reason': 'pending_factor_missing_pre_close',
            }
            for row in rows[:max(0, int(sample_limit or 0))]
        ]
        return {
            'status': 'partial' if rows else 'success',
            'totals': {
                'pending_factors': len(rows),
                'pending_instruments': len(pending_ids),
                'pending_cash_events': int(cash_events),
            },
            'instrument_ids': pending_ids,
            'samples': samples,
        }

    @staticmethod
    def _corporate_action_report_periods(
        start_date: date,
        end_date: date,
    ) -> List[str]:
        """Return annual and interim report periods that can produce ex-dates."""
        return [
            f"{year}{suffix}"
            for year in range(max(1990, start_date.year - 1), end_date.year + 1)
            for suffix in ("0630", "1231")
            if date(year, int(suffix[:2]), int(suffix[2:])) <= end_date
        ]

    async def backfill_a_share_cninfo_corporate_actions(
        self,
        *,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        scopes: Optional[List[str]] = None,
        dry_run: bool = True,
        resume: bool = True,
        chunk_size: int = 50,
        request_interval_seconds: float = 1.0,
        per_instrument_timeout_sec: int = 60,
        checkpoint_id: Optional[str] = None,
        active_only: bool = False,
    ) -> Dict[str, Any]:
        """Backfill isolated official CNInfo dividend and allotment evidence."""
        from data_sources.cninfo_corporate_actions import (
            ALLOTMENT_PROFILE,
            CNINFO_SOURCE,
            DIVIDEND_PROFILE,
            CninfoCorporateActionProvider,
            CninfoEndpointResult,
        )
        from utils.a_share_historical_backfill import (
            AShareBackfillCheckpointStore,
            coerce_date,
            normalize_string_list,
        )

        normalized_start = coerce_date(start_date, field_name="start_date")
        normalized_end = coerce_date(end_date, field_name="end_date")
        if normalized_end < normalized_start:
            raise ValueError("end_date must not be earlier than start_date")
        normalized_exchanges = [
            item.upper() for item in normalize_string_list(exchanges)
        ] or ["SSE", "SZSE", "BSE"]
        unsupported = sorted(set(normalized_exchanges) - {"SSE", "SZSE", "BSE"})
        if unsupported:
            raise ValueError(f"unsupported A-share exchanges: {unsupported}")
        normalized_ids = set(normalize_string_list(instrument_ids))
        normalized_scopes = [
            item.lower() for item in normalize_string_list(scopes)
        ] or ["dividends", "allotments"]
        unsupported_scopes = sorted(
            set(normalized_scopes) - {"dividends", "allotments"}
        )
        if unsupported_scopes:
            raise ValueError(
                f"unsupported CNInfo corporate-action scopes: {unsupported_scopes}"
            )
        chunk_size = max(1, min(int(chunk_size), 1000))
        request_interval_seconds = max(0.0, float(request_interval_seconds))
        per_instrument_timeout_sec = max(1, int(per_instrument_timeout_sec))

        universe: List[Dict[str, Any]] = []
        for exchange in normalized_exchanges:
            instruments = await self.db_ops.get_instruments_list(
                exchange=exchange,
                type="stock",
                is_active=True if active_only else None,
            )
            for instrument in instruments:
                instrument_id = str(instrument.get("instrument_id") or "").strip()
                if normalized_ids and instrument_id not in normalized_ids:
                    continue
                universe.append({
                    "instrument_id": instrument_id,
                    "symbol": str(instrument.get("symbol") or "").strip().zfill(6),
                    "exchange": exchange,
                })
        universe.sort(key=lambda item: (item["exchange"], item["instrument_id"]))
        universe_ids = {item["instrument_id"] for item in universe}
        missing_requested = sorted(normalized_ids - universe_ids)
        if missing_requested:
            raise ValueError(
                "requested instruments are absent from the selected historical universe: "
                f"{missing_requested[:20]}"
            )
        if not universe:
            raise ValueError("A-share CNInfo corporate-action universe is empty")

        parameters = {
            "start_date": normalized_start,
            "end_date": normalized_end,
            "exchanges": normalized_exchanges,
            "instrument_ids": sorted(normalized_ids),
            "scopes": normalized_scopes,
            "resume": bool(resume),
            "chunk_size": chunk_size,
            "request_interval_seconds": request_interval_seconds,
            "per_instrument_timeout_sec": per_instrument_timeout_sec,
            "active_only": bool(active_only),
        }
        identity = hashlib.sha256(json.dumps(
            {
                key: value.isoformat() if isinstance(value, date) else value
                for key, value in parameters.items() if key != "resume"
            },
            sort_keys=True,
        ).encode("utf-8")).hexdigest()[:16]
        resolved_checkpoint_id = checkpoint_id or f"a_share_cninfo_actions_{identity}"
        checkpoint_store = AShareBackfillCheckpointStore(
            self.data_config.get("data_dir", "data")
        )
        result: Dict[str, Any] = {
            "operation": "a_share_cninfo_corporate_action_backfill",
            "status": "dry_run" if dry_run else "running",
            "dry_run": bool(dry_run),
            "checkpoint_id": resolved_checkpoint_id,
            "parameters": {
                **parameters,
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
            },
            "universe": {
                "instrument_count": len(universe),
                "completed_count": 0,
                "pending_count": len(universe),
            },
            "production_isolation": True,
        }
        if dry_run:
            return result

        checkpoint_parameters = dict(parameters)
        checkpoint = (
            checkpoint_store.load(resolved_checkpoint_id, checkpoint_parameters)
            if resume else None
        )
        if checkpoint is None:
            checkpoint = checkpoint_store.initialize(
                resolved_checkpoint_id, checkpoint_parameters, universe
            )
        stage = checkpoint.setdefault("stages", {}).setdefault(
            "cninfo_corporate_actions",
            {
                "completed_instruments": [],
                "partial_instruments": [],
                "errors": [],
            },
        )
        completed = set(stage.get("completed_instruments") or []) if resume else set()
        stage["errors"] = [
            error for error in (stage.get("errors") or [])
            if error.get("instrument_id") not in completed
        ]
        pending = [
            item for item in universe if item["instrument_id"] not in completed
        ]
        result["universe"] = {
            "instrument_count": len(universe),
            "completed_count": len(completed),
            "pending_count": len(pending),
        }

        provider = CninfoCorporateActionProvider(
            request_timeout_seconds=per_instrument_timeout_sec
        )
        counters = Counter({
            "requested_instruments": 0,
            "requested_endpoints": 0,
            "completed_instruments": len(completed),
            "observations_inserted": 0,
            "observations_changed": 0,
            "observations_unchanged": 0,
            "observations_reactivated": 0,
            "observations_retired": 0,
            "complete_with_events": 0,
            "complete_no_events": 0,
            "partial_missing_fields": 0,
            "indeterminate": 0,
            "missing_ex_date_events": 0,
            "ignored_placeholders": 0,
        })
        endpoint_specs = []
        if "dividends" in normalized_scopes:
            endpoint_specs.append((DIVIDEND_PROFILE, provider.fetch_dividends))
        if "allotments" in normalized_scopes:
            endpoint_specs.append((ALLOTMENT_PROFILE, provider.fetch_allotments))

        for index, item in enumerate(pending, start=1):
            instrument_id = item["instrument_id"]
            counters["requested_instruments"] += 1
            stage["errors"] = [
                error for error in (stage.get("errors") or [])
                if error.get("instrument_id") != instrument_id
            ]
            instrument_indeterminate = False
            instrument_partial = False
            for source_profile, fetcher in endpoint_specs:
                counters["requested_endpoints"] += 1
                try:
                    endpoint_result = await asyncio.to_thread(
                        fetcher,
                        instrument_id,
                        item["symbol"],
                        start_date=normalized_start,
                        end_date=normalized_end,
                    )
                except Exception as exc:
                    endpoint_result = CninfoEndpointResult(
                        source_profile=source_profile,
                        coverage_status="indeterminate",
                        observations=[],
                        error=str(exc),
                    )

                observations = endpoint_result.observations
                write_stats = await self.db_ops.save_corporate_action_observations(
                    observations,
                    ingestion_run_id=resolved_checkpoint_id,
                )
                if int(write_stats.get("failed", 0)) > 0:
                    endpoint_result = CninfoEndpointResult(
                        source_profile=source_profile,
                        coverage_status="indeterminate",
                        observations=observations,
                        rows_received=endpoint_result.rows_received,
                        ignored_placeholders=endpoint_result.ignored_placeholders,
                        error=f"observation persistence incomplete: {write_stats}",
                    )
                elif endpoint_result.coverage_status != "indeterminate":
                    try:
                        retired = await self.db_ops.reconcile_corporate_action_observation_snapshot(
                            instrument_id=instrument_id,
                            source=CNINFO_SOURCE,
                            source_profile=source_profile,
                            requested_start_date=normalized_start,
                            requested_end_date=normalized_end,
                            seen_event_keys=[
                                str(observation.get("source_event_key") or "")
                                for observation in observations
                            ],
                            ingestion_run_id=resolved_checkpoint_id,
                        )
                        counters["observations_retired"] += int(retired)
                    except Exception as exc:
                        endpoint_result = CninfoEndpointResult(
                            source_profile=source_profile,
                            coverage_status="indeterminate",
                            observations=observations,
                            rows_received=endpoint_result.rows_received,
                            ignored_placeholders=endpoint_result.ignored_placeholders,
                            error=f"observation snapshot reconciliation failed: {exc}",
                        )
                for key in ("inserted", "changed", "unchanged", "reactivated"):
                    counters[f"observations_{key}"] += int(write_stats.get(key, 0))

                event_dates = [
                    self._date_from_any(observation.get("ex_date"))
                    for observation in observations
                    if observation.get("ex_date") is not None
                ]
                event_dates = [value for value in event_dates if value is not None]
                missing_ex_dates = sum(
                    observation.get("ex_date") is None
                    for observation in observations
                )
                counters["missing_ex_date_events"] += int(missing_ex_dates)
                counters["ignored_placeholders"] += int(
                    endpoint_result.ignored_placeholders
                )
                counters[endpoint_result.coverage_status] += 1
                try:
                    await self.db_ops.upsert_corporate_action_instrument_status({
                        "instrument_id": instrument_id,
                        "source": CNINFO_SOURCE,
                        "source_profile": source_profile,
                        "coverage_status": endpoint_result.coverage_status,
                        "event_count": len(observations),
                        "missing_ex_date_count": missing_ex_dates,
                        "requested_start_date": normalized_start,
                        "requested_end_date": normalized_end,
                        "earliest_event_date": (
                            min(event_dates) if event_dates else None
                        ),
                        "latest_event_date": max(event_dates) if event_dates else None,
                        "error_message": endpoint_result.error,
                        "ingestion_run_id": resolved_checkpoint_id,
                    })
                except Exception as exc:
                    counters[endpoint_result.coverage_status] -= 1
                    counters["indeterminate"] += 1
                    endpoint_result = CninfoEndpointResult(
                        source_profile=source_profile,
                        coverage_status="indeterminate",
                        observations=observations,
                        rows_received=endpoint_result.rows_received,
                        ignored_placeholders=endpoint_result.ignored_placeholders,
                        error=f"coverage persistence failed: {exc}",
                    )
                if endpoint_result.coverage_status == "indeterminate":
                    instrument_indeterminate = True
                    stage.setdefault("errors", []).append({
                        "instrument_id": instrument_id,
                        "source_profile": source_profile,
                        "reason": endpoint_result.error or "indeterminate response",
                    })
                elif endpoint_result.coverage_status == "partial_missing_fields":
                    instrument_partial = True
                if request_interval_seconds:
                    await asyncio.sleep(request_interval_seconds)

            if instrument_partial:
                stage["partial_instruments"] = sorted(
                    set(stage.get("partial_instruments") or []) | {instrument_id}
                )
            else:
                stage["partial_instruments"] = sorted(
                    set(stage.get("partial_instruments") or []) - {instrument_id}
                )
            if not instrument_indeterminate:
                completed.add(instrument_id)
                stage["completed_instruments"] = sorted(completed)
                counters["completed_instruments"] += 1
            checkpoint_store.save(checkpoint)
            if index == 1 or index % chunk_size == 0 or index == len(pending):
                dm_logger.info(
                    "[DataManager] CNInfo corporate-action progress: %d/%d "
                    "completed=%d partial=%d errors=%d observations=%d",
                    index,
                    len(pending),
                    counters["completed_instruments"],
                    len(stage.get("partial_instruments") or []),
                    len(stage.get("errors") or []),
                    counters["observations_inserted"]
                    + counters["observations_changed"]
                    + counters["observations_unchanged"],
                )

        has_partial = bool(stage.get("partial_instruments"))
        has_errors = bool(stage.get("errors"))
        result.update({
            "status": "partial" if has_partial or has_errors else "success",
            "universe": {
                "instrument_count": len(universe),
                "completed_count": len(completed),
                "pending_count": len(universe) - len(completed),
            },
            "counters": dict(counters),
            "partial_instruments": list(stage.get("partial_instruments") or []),
            "errors": list(stage.get("errors") or [])[:50],
            "announcement_recovery_required": (
                len(stage.get("partial_instruments") or [])
                + int(counters["indeterminate"])
            ),
        })
        checkpoint_store.save(checkpoint)
        return result

    async def rebuild_cninfo_primary_adjustment_factors(
        self,
        *,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        build_canonical: bool = False,
        series_version: str = "a_share_cninfo_primary_v1",
        field_tolerance: float = 0.0001,
        max_session_shift: int = 3,
        sample_limit: int = 20,
    ) -> Dict[str, Any]:
        """Persist independent CNInfo/TDX paths and benchmark all references."""
        from data_sources.adjustment_factor_governance import (
            build_factor_source_benchmark,
            compare_normalized_cumulative_paths,
        )
        from data_sources.cninfo_factor_governance import (
            build_cninfo_primary_candidate,
            build_quote_evidence_keys,
            derive_cninfo_factor_path,
            derive_tdx_factor_path,
            reconcile_cninfo_tdx_events,
        )
        from utils.a_share_historical_backfill import (
            coerce_date,
            normalize_string_list,
        )

        normalized_start = coerce_date(start_date, field_name="start_date")
        normalized_end = coerce_date(end_date, field_name="end_date")
        if normalized_end < normalized_start:
            raise ValueError("end_date must not be earlier than start_date")
        normalized_exchanges = [
            value.upper() for value in normalize_string_list(exchanges)
        ] or ["SSE", "SZSE", "BSE"]
        unsupported = sorted(set(normalized_exchanges) - {"SSE", "SZSE", "BSE"})
        if unsupported:
            raise ValueError(f"unsupported A-share exchanges: {unsupported}")
        requested_ids = set(normalize_string_list(instrument_ids))
        normalized_series_version = str(series_version or "").strip()
        if not normalized_series_version or len(normalized_series_version) > 64:
            raise ValueError("series_version must contain 1 to 64 characters")

        universe: List[Dict[str, Any]] = []
        for exchange in normalized_exchanges:
            instruments = await self.db_ops.get_instruments_list(
                exchange=exchange,
                type="stock",
                is_active=None,
            )
            for instrument in instruments:
                instrument_id = str(instrument.get("instrument_id") or "").strip()
                if requested_ids and instrument_id not in requested_ids:
                    continue
                universe.append({
                    "instrument_id": instrument_id,
                    "exchange": exchange,
                })
        universe.sort(key=lambda item: (item["exchange"], item["instrument_id"]))
        target_ids = [item["instrument_id"] for item in universe]
        missing_ids = sorted(requested_ids - set(target_ids))
        if missing_ids:
            raise ValueError(
                "requested instruments are absent from the selected universe: "
                f"{missing_ids[:20]}"
            )
        if not target_ids:
            raise ValueError("CNInfo factor rebuild universe is empty")

        cninfo_rows: List[Dict[str, Any]] = []
        tdx_rows: List[Dict[str, Any]] = []
        legacy_rows: List[Dict[str, Any]] = []
        endpoint_status_rows: List[Dict[str, Any]] = []
        tdx_endpoint_status_rows: List[Dict[str, Any]] = []
        for offset in range(0, len(target_ids), 400):
            chunk = target_ids[offset: offset + 400]
            placeholders = ", ".join(
                f":instrument_{index}" for index in range(len(chunk))
            )
            params: Dict[str, Any] = {
                f"instrument_{index}": instrument_id
                for index, instrument_id in enumerate(chunk)
            }
            params.update({
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
            })
            cninfo_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, source_profile, source_event_key,
                       action_type, fiscal_period, announcement_date,
                       record_date, ex_date, pay_date, share_arrival_date,
                       cash_dividend_per_share, bonus_shares_per_share,
                       capitalization_shares_per_share,
                       rights_shares_per_share, rights_price,
                       event_status, quality_status, is_current
                FROM corporate_action_observations
                WHERE instrument_id IN ({placeholders})
                  AND source = 'cninfo'
                  AND is_current = 1
                  AND (
                        ex_date IS NULL
                        OR date(ex_date) BETWEEN :start_date AND :end_date
                  )
                ORDER BY instrument_id, ex_date, source_profile
                """,
                params,
            ))
            tdx_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, ex_date, factor, cumulative_factor,
                       validation_result, pre_close,
                       fenhong, songzhuangu, peigu, peigujia
                FROM adjustment_factors_tdx
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                ORDER BY instrument_id, ex_date
                """,
                params,
            ))
            legacy_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, ex_date, factor, cumulative_factor, source
                FROM adjustment_factors
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                ORDER BY instrument_id, source, ex_date
                """,
                params,
            ))
            endpoint_status_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, source_profile, coverage_status,
                       event_count, missing_ex_date_count,
                       requested_start_date, requested_end_date, error_message
                FROM corporate_action_instrument_status
                WHERE instrument_id IN ({placeholders})
                  AND source = 'cninfo'
                  AND date(requested_start_date) <= :start_date
                  AND date(requested_end_date) >= :end_date
                ORDER BY instrument_id, source_profile, last_attempt_at DESC
                """,
                params,
            ))
            tdx_endpoint_status_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, source_profile, coverage_status,
                       event_count, missing_ex_date_count,
                       requested_start_date, requested_end_date, error_message
                FROM corporate_action_instrument_status
                WHERE instrument_id IN ({placeholders})
                  AND source = 'tdx'
                  AND source_profile = 'tdx_xdxr'
                  AND date(requested_start_date) <= :start_date
                  AND date(requested_end_date) >= :end_date
                ORDER BY instrument_id, last_attempt_at DESC
                """,
                params,
            ))

        source_event_keys = [
            str(row.get("source_event_key") or "")
            for row in cninfo_rows
            if row.get("source_event_key")
        ]
        resolved_date_evidence = (
            await self.db_ops.get_resolved_corporate_action_effective_dates(
                source_event_keys
            )
        )
        resolved_term_overlays: Dict[str, Dict[str, Any]] = {}
        terms_loader = getattr(
            self.db_ops, "get_corporate_action_resolved_terms", None
        )
        if callable(terms_loader):
            terms_result = terms_loader(source_event_keys)
            if inspect.isawaitable(terms_result):
                resolved_term_overlays = await terms_result
        factor_cninfo_rows: List[Dict[str, Any]] = []
        resolved_outside_range = 0
        for row in cninfo_rows:
            event_key = str(row.get("source_event_key") or "")
            resolved = resolved_date_evidence.get(event_key)
            resolved_terms = resolved_term_overlays.get(event_key) or {}
            resolved_date = (
                self._date_from_any(resolved.get("effective_date"))
                if resolved else None
            )
            if (
                row.get("ex_date") is None
                and resolved_date is not None
                and not normalized_start <= resolved_date <= normalized_end
            ):
                resolved_outside_range += 1
                continue
            merged_row = dict(row)
            economic_field_names = (
                "cash_dividend_per_share",
                "bonus_shares_per_share",
                "capitalization_shares_per_share",
                "rights_shares_per_share",
                "rights_price",
            )
            reviewed_fields = {
                str(item)
                for item in (resolved_terms.get("resolved_fields") or [])
                if str(item) in economic_field_names
            }
            quality_status = str(row.get("quality_status") or "")
            zero_placeholder_statuses = {
                "partial_missing_fields",
                "partial_missing_economic_fields",
                "partial_zero_effect",
            }
            applied_economic_fields = []
            for field_name in economic_field_names:
                reviewed_value = resolved_terms.get(field_name)
                if field_name not in reviewed_fields or reviewed_value is None:
                    continue
                current_value = merged_row.get(field_name)
                current_is_placeholder = False
                if quality_status in zero_placeholder_statuses:
                    try:
                        current_is_placeholder = (
                            current_value is not None and float(current_value) <= 0
                        )
                    except (TypeError, ValueError):
                        current_is_placeholder = True
                if current_value is None or current_is_placeholder:
                    merged_row[field_name] = reviewed_value
                    applied_economic_fields.append(field_name)
            factor_cninfo_rows.append({
                **merged_row,
                "resolved_effective_date": resolved_date,
                "resolved_date_basis": (
                    resolved.get("date_basis") if resolved else None
                ),
                "resolved_evidence_source": (
                    resolved.get("evidence_source") if resolved else None
                ),
                "resolved_evidence_key": (
                    resolved.get("evidence_key") if resolved else None
                ),
                "resolved_economic_terms": bool(applied_economic_fields),
                "resolved_economic_fields": applied_economic_fields,
            })

        quote_keys = build_quote_evidence_keys(factor_cninfo_rows, tdx_rows)
        quote_evidence = await self.db_ops.get_quote_evidence_for_event_dates(
            quote_keys
        )
        cninfo_path = derive_cninfo_factor_path(factor_cninfo_rows, quote_evidence)
        tdx_path = derive_tdx_factor_path(tdx_rows, quote_evidence)

        sessions_by_exchange: Dict[str, List[date]] = {}
        for exchange in normalized_exchanges:
            calendar_rows = await self.db_ops.get_trading_calendar_records(
                exchange,
                normalized_start - timedelta(days=14),
                normalized_end + timedelta(days=14),
            )
            sessions_by_exchange[exchange] = sorted({
                parsed
                for row in calendar_rows
                if row.get("is_trading_day")
                if (parsed := self._date_from_any(row.get("date"))) is not None
            })
        reconciliation = reconcile_cninfo_tdx_events(
            cninfo_path["events"],
            tdx_path["events"],
            sessions_by_exchange=sessions_by_exchange,
            field_tolerance=max(0.0, float(field_tolerance)),
            max_session_shift=max(0, int(max_session_shift)),
            sample_limit=max(0, int(sample_limit)),
        )

        identity = hashlib.sha256(json.dumps({
            "start_date": normalized_start.isoformat(),
            "end_date": normalized_end.isoformat(),
            "exchanges": normalized_exchanges,
            "instrument_ids": sorted(requested_ids),
            "series_version": normalized_series_version,
            "write_run": (
                get_shanghai_time().strftime("%Y%m%d%H%M%S%f")
                if not dry_run else "preview"
            ),
        }, sort_keys=True).encode("utf-8")).hexdigest()[:16]
        staging_suffix = f"__staging__{identity}"
        staging_version = (
            f"{normalized_series_version[:64 - len(staging_suffix)]}{staging_suffix}"
        )
        benchmark_suffix = f"__benchmark__{identity}"
        benchmark_version = (
            f"{normalized_series_version[:64 - len(benchmark_suffix)]}"
            f"{benchmark_suffix}"
        )
        candidate_rows: List[Dict[str, Any]] = []
        candidate_summary: Dict[str, Any] = {
            "series_version": None,
            "row_count": 0,
            "instrument_count": 0,
            "conflict_count": 0,
            "tdx_fallback_count": 0,
            "candidate_built": False,
            "source_selection_status": "deferred",
            "promotion_eligible": False,
        }
        if build_canonical:
            candidate_rows, candidate_summary = build_cninfo_primary_candidate(
                cninfo_path["events"],
                tdx_path["events"],
                reconciliation,
                series_version=staging_version,
            )
            candidate_summary["candidate_built"] = True
            candidate_summary["source_selection_status"] = "deferred"

        cninfo_comparison_rows = [
            {
                "instrument_id": row["instrument_id"],
                "ex_date": row["effective_date"],
                "cumulative_factor": row["cumulative_factor"],
            }
            for row in cninfo_path["events"]
        ]
        tdx_comparison_rows = [
            {
                "instrument_id": row["instrument_id"],
                "ex_date": row["effective_date"],
                "cumulative_factor": row["cumulative_factor"],
            }
            for row in tdx_path["events"]
        ]
        source_observations = await self.db_ops.list_adjustment_factor_observations(
            instrument_ids=target_ids,
            sources=["akshare"],
            start_date=normalized_start,
            end_date=normalized_end,
        )
        sina_rows = [
            {
                "instrument_id": row["instrument_id"],
                "ex_date": row["ex_date"],
                "cumulative_factor": row.get("provider_cumulative_factor"),
            }
            for row in source_observations
            if row.get("source_profile") == "sina_hfq_factor"
        ]
        baostock_rows = [
            row for row in legacy_rows
            if str(row.get("source") or "").lower() == "baostock"
        ]
        comparisons = {
            "tdx": compare_normalized_cumulative_paths(
                cninfo_comparison_rows, tdx_comparison_rows, sample_limit=sample_limit
            ),
            "sina": compare_normalized_cumulative_paths(
                cninfo_comparison_rows, sina_rows, sample_limit=sample_limit
            ),
            "baostock": compare_normalized_cumulative_paths(
                cninfo_comparison_rows, baostock_rows, sample_limit=sample_limit
            ),
        }

        endpoint_by_instrument: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        seen_endpoint_keys = set()
        for row in endpoint_status_rows:
            key = (row.get("instrument_id"), row.get("source_profile"))
            if key in seen_endpoint_keys:
                continue
            seen_endpoint_keys.add(key)
            endpoint_by_instrument[str(row.get("instrument_id"))].append(row)
        unresolved_ids = {
            str(item.get("instrument_id"))
            for item in [
                *cninfo_path["pending"],
                *tdx_path["pending"],
                *reconciliation.get("conflicts", []),
                *reconciliation.get("cninfo_only", []),
                *reconciliation.get("tdx_only", []),
            ]
            if item.get("instrument_id")
        }
        required_profiles = {"cninfo_dividend", "cninfo_allotment"}
        endpoint_incomplete_ids = set()
        missing_profile_samples: List[Dict[str, Any]] = []
        cninfo_pending_ids = {
            str(item.get("instrument_id"))
            for item in cninfo_path["pending"]
            if item.get("instrument_id")
        }
        for instrument_id in target_ids:
            rows = endpoint_by_instrument.get(instrument_id, [])
            profiles = {str(row.get("source_profile") or "") for row in rows}
            missing_profiles = sorted(required_profiles - profiles)
            has_indeterminate = any(
                str(row.get("coverage_status")) == "indeterminate"
                for row in rows
            )
            has_partial = any(
                str(row.get("coverage_status")) == "partial_missing_fields"
                for row in rows
            )
            if (
                missing_profiles
                or has_indeterminate
                or (has_partial and instrument_id in cninfo_pending_ids)
            ):
                endpoint_incomplete_ids.add(instrument_id)
            if missing_profiles and len(missing_profile_samples) < sample_limit:
                missing_profile_samples.append({
                    "instrument_id": instrument_id,
                    "reason": "missing_cninfo_endpoint_status",
                    "missing_profiles": missing_profiles,
                })
        overall_incomplete_ids = sorted(unresolved_ids | endpoint_incomplete_ids)
        overall_completeness = {
            "status": "partial" if overall_incomplete_ids else "success",
            "endpoint_status_rows": len(endpoint_status_rows),
            "endpoint_incomplete_instruments": len(endpoint_incomplete_ids),
            "reconciliation_incomplete_instruments": len(unresolved_ids),
            "overall_incomplete_instruments": len(overall_incomplete_ids),
            "instrument_ids": overall_incomplete_ids[:100],
            "missing_endpoint_profile_samples": missing_profile_samples,
        }
        full_market_scope = (
            not requested_ids
            and set(normalized_exchanges) == {"SSE", "SZSE", "BSE"}
            and normalized_start <= date(1990, 12, 19)
        )
        pending_factor_ids = {
            str(item.get("instrument_id"))
            for item in [*cninfo_path["pending"], *tdx_path["pending"]]
            if item.get("instrument_id")
        }
        baseline_covered_ids = sorted(
            set(target_ids) - endpoint_incomplete_ids - pending_factor_ids
        )
        tdx_covered_ids = set()
        seen_tdx_status_ids = set()
        for row in tdx_endpoint_status_rows:
            instrument_id = str(row.get("instrument_id") or "").strip()
            if not instrument_id or instrument_id in seen_tdx_status_ids:
                continue
            seen_tdx_status_ids.add(instrument_id)
            if str(row.get("coverage_status") or "") in {
                "complete_with_events",
                "complete_no_events",
            }:
                tdx_covered_ids.add(instrument_id)
        benchmark = build_factor_source_benchmark(
            cninfo_comparison_rows,
            {
                "tdx_event_derived_v1": tdx_comparison_rows,
                "sina_hfq_factor": sina_rows,
                "baostock_legacy": baostock_rows,
            },
            target_instruments=target_ids,
            baseline_covered_instruments=baseline_covered_ids,
            reference_covered_instruments={
                "tdx_event_derived_v1": sorted(tdx_covered_ids),
            },
            full_market_scope=full_market_scope,
            sample_limit=sample_limit,
        )
        benchmark.update({
            "benchmark_series_version": benchmark_version,
            "event_reconciliation": reconciliation.get("totals") or {},
            "pending_factor_events": (
                len(cninfo_path["pending"]) + len(tdx_path["pending"])
            ),
            "endpoint_incomplete_instruments": len(endpoint_incomplete_ids),
            "tdx_coverage_status_rows": len(tdx_endpoint_status_rows),
        })
        quality_gates = {
            "full_market_scope": full_market_scope,
            "endpoint_completeness": overall_completeness["status"] == "success",
            "no_pending_factor_events": not (
                cninfo_path["pending"] or tdx_path["pending"]
            ),
            "event_reconciliation": reconciliation.get("status") == "success",
            "no_unverified_tdx_fallback": (
                int(candidate_summary.get("tdx_fallback_count", 0) or 0) == 0
            ),
        }
        candidate_summary.update({
            "quality_gates": quality_gates,
            "candidate_promotion_eligible": (
                bool(build_canonical) and all(quality_gates.values())
            ),
            "promotion_eligible": (
                bool(build_canonical) and all(quality_gates.values())
            ),
        })

        run_id = f"a_share_cninfo_factor_{identity}"
        write_result: Dict[str, Any] = {
            "cninfo_observations": {},
            "tdx_observations": {},
            "canonical_saved_rows": 0,
            "benchmark_status_saved": False,
        }
        if not dry_run:
            write_result["cninfo_observations"] = (
                await self.db_ops.save_adjustment_factor_observations(
                    cninfo_path["observations"],
                    ingestion_run_id=run_id,
                )
            )
            write_result["tdx_observations"] = (
                await self.db_ops.save_adjustment_factor_observations(
                    tdx_path["observations"],
                    ingestion_run_id=run_id,
                )
            )
            benchmark_errors = [
                report.get("max_adjusted_price_error_pct")
                for report in benchmark.get("reference_sources", {}).values()
                if report.get("max_adjusted_price_error_pct") is not None
            ]
            await self.db_ops.upsert_adjustment_factor_series_status(
                benchmark_version,
                {
                    "status": (
                        "benchmarked"
                        if not cninfo_path["pending"]
                        and not tdx_path["pending"]
                        and not endpoint_incomplete_ids
                        else "partial"
                    ),
                    "source_priority": [],
                    "source_selection_status": "deferred",
                    "selected_primary_source": None,
                    "start_date": normalized_start.isoformat(),
                    "end_date": normalized_end.isoformat(),
                    "instrument_count": len(target_ids),
                    "row_count": len(cninfo_path["events"]),
                    "coverage_ratio": benchmark.get("baseline_coverage_ratio", 0.0),
                    "conflict_count": (
                        int(reconciliation.get("totals", {}).get("conflicts", 0))
                        + int(reconciliation.get("totals", {}).get("cninfo_only", 0))
                        + int(reconciliation.get("totals", {}).get("tdx_only", 0))
                    ),
                    "max_cumulative_error_pct": max(benchmark_errors, default=None),
                    "promotion_eligible": False,
                    "benchmark": benchmark,
                    "reconciliation": reconciliation,
                    "comparisons": comparisons,
                    "overall_completeness": overall_completeness,
                },
            )
            write_result["benchmark_status_saved"] = True
            if build_canonical:
                write_result["canonical_saved_rows"] = (
                    await self.db_ops.replace_canonical_adjustment_factors(
                        candidate_rows,
                        series_version=staging_version,
                        instrument_ids=target_ids,
                    )
                )
                event_counts = Counter(
                    row["instrument_id"] for row in candidate_rows
                )
                status_rows = [{
                    "instrument_id": instrument_id,
                    "source": "cninfo_primary",
                    "coverage_status": (
                        "incomplete"
                        if instrument_id in overall_incomplete_ids
                        else "complete_with_events"
                        if event_counts.get(instrument_id, 0) > 0
                        else "complete_no_events"
                    ),
                    "event_count": event_counts.get(instrument_id, 0),
                    "start_date": normalized_start,
                    "end_date": normalized_end,
                    "ingestion_run_id": run_id,
                } for instrument_id in target_ids]
                write_result["instrument_statuses"] = (
                    await self.db_ops.replace_adjustment_factor_instrument_statuses(
                        status_rows,
                        series_version=staging_version,
                        instrument_ids=target_ids,
                    )
                )
                await self.db_ops.upsert_adjustment_factor_series_status(
                    staging_version,
                    {
                        **candidate_summary,
                        "status": (
                            "validated_staging"
                            if candidate_summary["candidate_promotion_eligible"]
                            else "partial"
                        ),
                        "promotion_eligible": False,
                        "start_date": normalized_start.isoformat(),
                        "end_date": normalized_end.isoformat(),
                        "reconciliation": reconciliation,
                        "comparisons": comparisons,
                        "overall_completeness": overall_completeness,
                    },
                )
            self.invalidate_factor_cache()

        has_operational_issues = bool(
            cninfo_path["pending"]
            or tdx_path["pending"]
            or endpoint_incomplete_ids
        )
        result_status = (
            "dry_run" if dry_run
            else "partial"
            if has_operational_issues
            else "success"
        )
        return {
            "status": result_status,
            "operation": "a_share_cninfo_adjustment_factor_rebuild",
            "dry_run": bool(dry_run),
            "production_isolation": True,
            "parameters": {
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
                "exchanges": normalized_exchanges,
                "instrument_ids": sorted(requested_ids),
                "build_canonical": bool(build_canonical),
                "series_version": normalized_series_version,
            },
            "universe": {"instrument_count": len(target_ids)},
            "source_events": {
                "cninfo_rows": len(cninfo_rows),
                "tdx_rows": len(tdx_rows),
                "resolved_effective_date_events": len(resolved_date_evidence),
                "resolved_effective_dates_outside_range": resolved_outside_range,
            },
            "cninfo_path": {
                "derived_events": len(cninfo_path["events"]),
                "pending_count": len(cninfo_path["pending"]),
                "pending": cninfo_path["pending"][:sample_limit],
            },
            "tdx_path": {
                "derived_events": len(tdx_path["events"]),
                "pending_count": len(tdx_path["pending"]),
                "pending": tdx_path["pending"][:sample_limit],
            },
            "reconciliation": reconciliation,
            "comparisons": comparisons,
            "benchmark": benchmark,
            "source_selection": {
                "status": "deferred",
                "selected_primary_source": None,
            },
            "overall_completeness": overall_completeness,
            "candidate": {
                **candidate_summary,
                "staging_series_version": (
                    staging_version if build_canonical else None
                ),
                "promotion_eligible": bool(
                    candidate_summary.get("candidate_promotion_eligible")
                ),
                "promoted": False,
            },
            "write_result": write_result,
        }

    async def maintain_a_share_cninfo_primary_factors(
        self,
        *,
        start_date: Optional[Union[str, date, datetime]] = None,
        end_date: Optional[Union[str, date, datetime]] = None,
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        rolling_days: int = 7,
        request_interval_seconds: float = 0.5,
        per_instrument_timeout_sec: int = 60,
        build_canonical: bool = False,
        series_version: str = "a_share_cninfo_primary_v1",
    ) -> Dict[str, Any]:
        """Refresh recent CNInfo/TDX events and rebuild isolated factor paths.

        Source refresh is bounded to a rolling window. Factor derivation reads
        stored full history so cumulative paths do not reset at that boundary.
        The operation never modifies or promotes the production factor table.
        Canonical staging is opt-in; the default persists independent paths and
        source-neutral benchmark evidence only.
        """
        from data_sources.cninfo_corporate_actions import CNINFO_SUPPORTED_EXCHANGES
        from utils.a_share_historical_backfill import coerce_date, normalize_string_list

        normalized_end = coerce_date(end_date or date.today(), field_name="end_date")
        normalized_start = coerce_date(
            start_date or normalized_end - timedelta(days=max(1, int(rolling_days))),
            field_name="start_date",
        )
        normalized_exchanges = [
            item.upper() for item in normalize_string_list(exchanges)
        ] or ["SSE", "SZSE", "BSE"]
        requested_ids = set(normalize_string_list(instrument_ids))
        active_ids_by_exchange: Dict[str, List[str]] = {}
        for exchange in normalized_exchanges:
            rows = await self.db_ops.get_active_instruments(
                exchange, instrument_types=["stock"], tradable_only=False
            )
            active_ids_by_exchange[exchange] = sorted({
                str(row.get("instrument_id") or "").strip()
                for row in rows
                if row.get("instrument_id")
                and (not requested_ids or row.get("instrument_id") in requested_ids)
            })
        active_ids = sorted({
            instrument_id
            for values in active_ids_by_exchange.values()
            for instrument_id in values
        })
        if requested_ids:
            missing_ids = sorted(requested_ids - set(active_ids))
            if missing_ids:
                return {
                    "status": "partial",
                    "operation": "a_share_cninfo_primary_daily_maintenance",
                    "production_isolation": True,
                    "error": "requested instruments are not active",
                    "missing_instruments": missing_ids[:50],
                }
        if not active_ids:
            return {
                "status": "partial",
                "operation": "a_share_cninfo_primary_daily_maintenance",
                "production_isolation": True,
                "error": "active A-share stock universe is empty",
            }

        cninfo_exchanges = [
            exchange
            for exchange in normalized_exchanges
            if exchange in CNINFO_SUPPORTED_EXCHANGES
        ]
        cninfo_excluded_exchanges = [
            exchange
            for exchange in normalized_exchanges
            if exchange not in CNINFO_SUPPORTED_EXCHANGES
        ]
        cninfo_active_ids = sorted({
            instrument_id
            for exchange in cninfo_exchanges
            for instrument_id in active_ids_by_exchange.get(exchange, [])
        })
        if cninfo_active_ids:
            cninfo_result = await self.backfill_a_share_cninfo_corporate_actions(
                start_date=normalized_start,
                end_date=normalized_end,
                exchanges=cninfo_exchanges,
                instrument_ids=cninfo_active_ids,
                dry_run=False,
                resume=False,
                chunk_size=50,
                request_interval_seconds=request_interval_seconds,
                per_instrument_timeout_sec=per_instrument_timeout_sec,
                active_only=True,
            )
        else:
            cninfo_result = {
                "status": "skipped",
                "operation": "a_share_cninfo_corporate_action_backfill",
                "production_isolation": True,
                "reason": "no_instruments_in_supported_exchanges",
            }
        cninfo_result["source_coverage"] = {
            "supported_exchanges": sorted(CNINFO_SUPPORTED_EXCHANGES),
            "requested_exchanges": normalized_exchanges,
            "refreshed_exchanges": cninfo_exchanges,
            "excluded_exchanges": cninfo_excluded_exchanges,
            "excluded_reason": (
                "source_not_supported" if cninfo_excluded_exchanges else None
            ),
        }
        tdx_result = await self.backfill_tdx_xdxr_history(
            exchanges=normalized_exchanges,
            start_date=normalized_start,
            end_date=normalized_end,
            instrument_ids=active_ids,
            derive_factors=True,
            repair_universe_mode="current_repair",
            per_instrument_timeout_sec=per_instrument_timeout_sec,
            dry_run=False,
        )
        rebuild_result = await self.rebuild_cninfo_primary_adjustment_factors(
            start_date=date(1990, 12, 19),
            end_date=normalized_end,
            exchanges=normalized_exchanges,
            instrument_ids=active_ids,
            dry_run=False,
            build_canonical=build_canonical,
            series_version=series_version,
        )
        statuses = {
            str(cninfo_result.get("status")),
            str(tdx_result.get("status")),
            str(rebuild_result.get("status")),
        }
        return {
            "status": "partial" if statuses & {"partial", "failed"} else "success",
            "operation": "a_share_cninfo_primary_daily_maintenance",
            "production_isolation": True,
            "parameters": {
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
                "exchanges": normalized_exchanges,
                "cninfo_exchanges": cninfo_exchanges,
                "cninfo_excluded_exchanges": cninfo_excluded_exchanges,
                "tdx_exchanges": normalized_exchanges,
                "instrument_ids": active_ids,
                "rolling_days": int(rolling_days),
            },
            "cninfo_refresh": cninfo_result,
            "tdx_refresh": tdx_result,
            "factor_rebuild": rebuild_result,
        }

    async def _fetch_eastmoney_corporate_action_rows(
        self,
        *,
        start_date: date,
        end_date: date,
        target_symbols: Set[str],
        per_period_timeout_sec: int,
    ) -> Dict[str, Any]:
        """Fetch implemented-distribution candidates through AkShare/Eastmoney."""
        periods = self._corporate_action_report_periods(start_date, end_date)
        rows: List[Dict[str, Any]] = []
        failed_periods: List[Dict[str, str]] = []
        empty_periods: List[str] = []
        try:
            import akshare as ak
        except ImportError as exc:
            return {
                'status': 'unavailable',
                'source': 'eastmoney_stock_fhps',
                'adapter': 'akshare.stock_fhps_em',
                'rows': [],
                'periods_requested': periods,
                'periods_succeeded': 0,
                'empty_periods': [],
                'failed_periods': [{'period': '*', 'error': str(exc)}],
            }

        for index, period in enumerate(periods, start=1):
            try:
                frame = await asyncio.wait_for(
                    asyncio.to_thread(ak.stock_fhps_em, date=period),
                    timeout=max(1, int(per_period_timeout_sec)),
                )
                if frame is None or frame.empty:
                    empty_periods.append(period)
                    continue
                period_rows = frame.to_dict(orient='records')
                for row in period_rows:
                    symbol = str(row.get('代码') or '').strip().zfill(6)
                    if target_symbols and symbol not in target_symbols:
                        continue
                    rows.append({**row, '_report_period': period})
            except Exception as exc:
                failed_periods.append({'period': period, 'error': str(exc)})
            if index == 1 or index % 4 == 0 or index == len(periods):
                dm_logger.info(
                    "[DataManager] corporate-action Eastmoney progress: %d/%d "
                    "rows=%d failures=%d",
                    index,
                    len(periods),
                    len(rows),
                    len(failed_periods),
                )
        return {
            'status': 'partial' if failed_periods else 'success',
            'source': 'eastmoney_stock_fhps',
            'adapter': 'akshare.stock_fhps_em',
            'rows': rows,
            'periods_requested': periods,
            'periods_succeeded': len(periods) - len(failed_periods),
            'empty_periods': empty_periods,
            'failed_periods': failed_periods,
        }

    async def _scan_cninfo_corporate_action_announcements(
        self,
        *,
        instrument_ids: List[str],
        start_date: date,
        end_date: date,
        per_instrument_timeout_sec: int,
    ) -> Dict[str, Any]:
        """Scan bounded official implementation-announcement metadata."""
        from research.providers.cninfo_announcements import (
            CninfoAnnouncementScanConfig,
            CninfoAnnouncementScanner,
        )

        scanner = CninfoAnnouncementScanner()
        records: List[Any] = []
        errors: List[Dict[str, str]] = []
        scanned = 0
        exchange_config = {
            'SSE': {'column': 'sse', 'plate': 'sh'},
            'SZSE': {'column': 'szse', 'plate': 'sz'},
            'BSE': {'column': 'neeq', 'plate': 'bj'},
        }
        for index, instrument_id in enumerate(instrument_ids, start=1):
            symbol = str(instrument_id).split('.')[0].zfill(6)
            exchange = (
                'SSE'
                if str(instrument_id).endswith('.SH')
                else ('SZSE' if str(instrument_id).endswith('.SZ') else 'BSE')
            )
            config = exchange_config[exchange]
            try:
                identity = await asyncio.wait_for(
                    asyncio.to_thread(scanner.resolve_stock_identity, symbol),
                    timeout=max(1, int(per_instrument_timeout_sec)),
                )
                if not identity:
                    errors.append({
                        'instrument_id': instrument_id,
                        'error': 'cninfo_stock_identity_unavailable',
                    })
                    continue
                scan_result = await asyncio.wait_for(
                    asyncio.to_thread(
                        scanner.scan,
                        CninfoAnnouncementScanConfig(
                            purpose_key='a_share_corporate_action_validation',
                            market=exchange,
                            column=config['column'],
                            plate=config['plate'],
                            search_key='权益分派实施公告',
                            stock=identity['stock'],
                            org_id=identity['org_id'],
                            start_date=start_date.isoformat(),
                            end_date=end_date.isoformat(),
                            page_size=30,
                            max_pages=20,
                        ),
                    ),
                    timeout=max(1, int(per_instrument_timeout_sec)),
                )
                scanned += 1
                records.extend(scan_result.records)
                errors.extend({
                    'instrument_id': instrument_id,
                    'error': error,
                } for error in scan_result.errors)
            except Exception as exc:
                errors.append({'instrument_id': instrument_id, 'error': str(exc)})
            if index == 1 or index % 10 == 0 or index == len(instrument_ids):
                dm_logger.info(
                    "[DataManager] corporate-action CNInfo progress: %d/%d "
                    "records=%d errors=%d",
                    index,
                    len(instrument_ids),
                    len(records),
                    len(errors),
                )
        return {
            'status': 'partial' if errors else 'success',
            'source': 'cninfo_announcement_metadata',
            'records': records,
            'instruments_requested': len(instrument_ids),
            'instruments_scanned': scanned,
            'errors': errors,
        }

    async def discover_cninfo_special_action_effective_dates(
        self,
        *,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        max_events: int = 500,
        target_offset: int = 0,
        window_before_days: int = 10,
        window_after_days: int = 30,
        max_window_days: int = 180,
        page_size: int = 30,
        max_pages: int = 5,
        request_interval_seconds: float = 0.5,
        per_event_timeout_sec: int = 60,
        sample_limit: int = 20,
    ) -> Dict[str, Any]:
        """Discover candidate official documents for unresolved special actions."""
        from data_sources.cninfo_corporate_actions import CNINFO_SUPPORTED_EXCHANGES
        from data_sources.cninfo_special_action_resolution import (
            announcement_match_reasons,
            build_candidate_evidence,
            build_search_target,
            parse_date,
        )
        from research.providers.cninfo_announcements import (
            CninfoAnnouncementScanConfig,
            CninfoAnnouncementScanner,
        )
        from utils.a_share_historical_backfill import (
            coerce_date,
            normalize_string_list,
        )

        normalized_start = coerce_date(start_date, field_name="start_date")
        normalized_end = coerce_date(end_date, field_name="end_date")
        if normalized_end < normalized_start:
            raise ValueError("end_date must not be earlier than start_date")
        normalized_exchanges = [
            value.upper() for value in normalize_string_list(exchanges)
        ] or ["SSE", "SZSE"]
        unsupported = sorted(
            set(normalized_exchanges) - {"SSE", "SZSE", "BSE"}
        )
        if unsupported:
            raise ValueError(f"unsupported A-share exchanges: {unsupported}")
        scan_exchanges = [
            value
            for value in normalized_exchanges
            if value in CNINFO_SUPPORTED_EXCHANGES
        ]
        excluded_exchanges = [
            value
            for value in normalized_exchanges
            if value not in CNINFO_SUPPORTED_EXCHANGES
        ]
        requested_ids = sorted(set(normalize_string_list(instrument_ids)))
        suffixes = {
            "SSE": ".SH",
            "SZSE": ".SZ",
        }
        params: Dict[str, Any] = {}
        exchange_filters = []
        for index, exchange in enumerate(scan_exchanges):
            key = f"exchange_suffix_{index}"
            params[key] = f"%{suffixes[exchange]}"
            exchange_filters.append(f"instrument_id LIKE :{key}")
        if not exchange_filters:
            rows: List[Dict[str, Any]] = []
        else:
            instrument_filter = ""
            if requested_ids:
                placeholders = []
                for index, instrument_id in enumerate(requested_ids):
                    key = f"instrument_{index}"
                    params[key] = instrument_id
                    placeholders.append(f":{key}")
                instrument_filter = (
                    f" AND instrument_id IN ({', '.join(placeholders)})"
                )
            rows = await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, source_profile, source_event_key,
                       action_type, fiscal_period, announcement_date,
                       record_date, ex_date, pay_date, share_arrival_date,
                       cash_dividend_per_share, bonus_shares_per_share,
                       capitalization_shares_per_share,
                       rights_shares_per_share, rights_price, description,
                       event_status, quality_status, raw_payload_json
                FROM corporate_action_observations
                WHERE source = 'cninfo'
                  AND source_profile IN ('cninfo_dividend', 'cninfo_allotment')
                  AND is_current = 1
                  AND (
                    quality_status = 'partial_missing_ex_date'
                    OR quality_status = 'partial_missing_fields'
                    OR quality_status = 'partial_missing_economic_fields'
                    OR quality_status = 'partial_zero_effect'
                  )
                  AND ({' OR '.join(exchange_filters)})
                  {instrument_filter}
                ORDER BY instrument_id, announcement_date, record_date,
                         source_event_key
                """,
                params,
            )

        event_dates_by_instrument: Dict[str, List[date]] = defaultdict(list)
        row_instrument_ids = sorted({
            str(row.get("instrument_id") or "").strip()
            for row in rows
            if row.get("instrument_id")
        })
        for offset in range(0, len(row_instrument_ids), 400):
            chunk = row_instrument_ids[offset: offset + 400]
            adjacent_params = {
                f"adjacent_{index}": instrument_id
                for index, instrument_id in enumerate(chunk)
            }
            placeholders = ", ".join(
                f":adjacent_{index}" for index in range(len(chunk))
            )
            adjacent_rows = await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, ex_date, announcement_date, record_date,
                       share_arrival_date
                FROM corporate_action_observations
                WHERE source = 'cninfo'
                  AND is_current = 1
                  AND instrument_id IN ({placeholders})
                """,
                adjacent_params,
            )
            for item in adjacent_rows:
                instrument_id = str(item.get("instrument_id") or "").strip()
                for field_name in (
                    "ex_date",
                    "announcement_date",
                    "record_date",
                    "share_arrival_date",
                ):
                    parsed = parse_date(item.get(field_name))
                    if instrument_id and parsed is not None:
                        event_dates_by_instrument[instrument_id].append(parsed)

        targets = []
        skipped_outside_range = 0
        skipped_without_bounded_anchor = 0
        for row in rows:
            instrument_id = str(row.get("instrument_id") or "").strip()
            structured_anchors = [
                parsed
                for field_name in (
                    "announcement_date",
                    "record_date",
                    "share_arrival_date",
                )
                if (parsed := parse_date(row.get(field_name))) is not None
            ]
            if structured_anchors and (
                max(structured_anchors) < normalized_start
                or min(structured_anchors) > normalized_end
            ):
                skipped_outside_range += 1
                continue
            adjacent_dates: List[date] = []
            if not structured_anchors:
                fiscal_match = re.search(
                    r"(19|20)\d{2}", str(row.get("fiscal_period") or "")
                )
                if fiscal_match:
                    pivot = date(int(fiscal_match.group(0)), 12, 31)
                    dates = sorted(set(event_dates_by_instrument[instrument_id]))
                    previous = [value for value in dates if value < pivot]
                    following = [value for value in dates if value > pivot]
                    if previous and following:
                        adjacent_dates = [previous[-1], following[0]]
            target = build_search_target(
                row,
                adjacent_dates=adjacent_dates,
                window_before_days=window_before_days,
                window_after_days=window_after_days,
                max_window_days=max_window_days,
            )
            if target is None:
                skipped_without_bounded_anchor += 1
                continue
            if (
                target.end_date < normalized_start
                or target.start_date > normalized_end
            ):
                skipped_outside_range += 1
                continue
            targets.append(target)

        total_searchable_events = len(targets)
        target_limit = min(5000, max(1, int(max_events)))
        normalized_target_offset = max(0, int(target_offset))
        batch_end_offset = min(
            total_searchable_events,
            normalized_target_offset + target_limit,
        )
        targets = targets[normalized_target_offset:batch_end_offset]
        has_more_targets = batch_end_offset < total_searchable_events
        bounded_request_timeout = max(
            1.0,
            min(20.0, float(per_event_timeout_sec) / 3.0),
        )
        effective_request_interval = max(0.2, float(request_interval_seconds))
        scanner = CninfoAnnouncementScanner(
            request_timeout_seconds=bounded_request_timeout,
            request_interval_seconds=effective_request_interval,
        )
        exchange_config = {
            "SSE": {"column": "sse", "plate": "sh"},
            "SZSE": {"column": "szse", "plate": "sz"},
        }
        identity_cache: Dict[str, Optional[Dict[str, str]]] = {}
        evidence_rows: List[Dict[str, Any]] = []
        target_results: List[Dict[str, Any]] = []
        errors: List[Dict[str, str]] = []
        for index, target in enumerate(targets, start=1):
            symbol = target.instrument_id.split(".")[0].zfill(6)
            exchange = "SSE" if target.instrument_id.endswith(".SH") else "SZSE"
            config = exchange_config[exchange]
            try:
                if symbol not in identity_cache:
                    identity_cache[symbol] = await asyncio.to_thread(
                        scanner.resolve_stock_identity,
                        symbol,
                    )
                identity = identity_cache[symbol]
                if not identity:
                    raise RuntimeError("cninfo_stock_identity_unavailable")
                scan_result = await asyncio.to_thread(
                    scanner.scan,
                    CninfoAnnouncementScanConfig(
                        purpose_key="a_share_cninfo_special_action_discovery",
                        market=exchange,
                        column=config["column"],
                        plate=config["plate"],
                        stock=identity["stock"],
                        org_id=identity["org_id"],
                        start_date=target.start_date.isoformat(),
                        end_date=target.end_date.isoformat(),
                        page_size=min(50, max(1, int(page_size))),
                        max_pages=min(20, max(1, int(max_pages))),
                    ),
                    filters=[
                        lambda record, current=target: announcement_match_reasons(
                            current, record.title
                        )
                    ],
                )
                candidates = build_candidate_evidence(
                    target, scan_result.selected_records
                )
                evidence_rows.extend(candidates)
                target_results.append({
                    "instrument_id": target.instrument_id,
                    "source_event_key": target.source_event_key,
                    "event_class": target.event_class,
                    "search_start_date": target.start_date.isoformat(),
                    "search_end_date": target.end_date.isoformat(),
                    "search_basis": target.search_basis,
                    "announcements_seen": scan_result.announcements_seen,
                    "candidate_count": len(candidates),
                    "errors": list(scan_result.errors),
                })
                for error in scan_result.errors:
                    errors.append({
                        "instrument_id": target.instrument_id,
                        "source_event_key": target.source_event_key,
                        "error": error,
                    })
            except Exception as exc:
                errors.append({
                    "instrument_id": target.instrument_id,
                    "source_event_key": target.source_event_key,
                    "error": str(exc),
                })
            if index == 1 or index % 10 == 0 or index == len(targets):
                dm_logger.info(
                    "[DataManager] CNInfo special-action discovery: %d/%d "
                    "candidates=%d errors=%d",
                    index,
                    len(targets),
                    len(evidence_rows),
                    len(errors),
                )
            if index < len(targets):
                await asyncio.sleep(effective_request_interval)

        identity = hashlib.sha256(json.dumps({
            "start_date": normalized_start.isoformat(),
            "end_date": normalized_end.isoformat(),
            "exchanges": scan_exchanges,
            "instrument_ids": requested_ids,
            "target_offset": normalized_target_offset,
            "target_keys": [target.source_event_key for target in targets],
        }, sort_keys=True).encode("utf-8")).hexdigest()[:16]
        run_id = f"a_share_cninfo_special_action_{identity}"
        write_result = {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "failed": 0,
        }
        if not dry_run:
            write_result = (
                await self.db_ops.save_corporate_action_effective_date_evidence(
                    evidence_rows,
                    ingestion_run_id=run_id,
                )
            )
        status = (
            "partial"
            if (
                errors
                or int(write_result.get("failed", 0) or 0) > 0
                or (has_more_targets and not dry_run)
            )
            else "dry_run" if dry_run else "success"
        )
        batch_failed = bool(
            errors or int(write_result.get("failed", 0) or 0) > 0
        )
        return {
            "status": status,
            "operation": "a_share_cninfo_special_action_discovery",
            "dry_run": bool(dry_run),
            "production_isolation": True,
            "parameters": {
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
                "requested_exchanges": normalized_exchanges,
                "scanned_exchanges": scan_exchanges,
                "excluded_exchanges": excluded_exchanges,
                "excluded_reason": (
                    "source_not_supported" if excluded_exchanges else None
                ),
                "instrument_ids": requested_ids,
                "max_events": target_limit,
                "target_offset": normalized_target_offset,
                "window_before_days": int(window_before_days),
                "window_after_days": int(window_after_days),
                "max_window_days": int(max_window_days),
                "request_interval_seconds": effective_request_interval,
                "request_timeout_seconds": bounded_request_timeout,
            },
            "targets": {
                "candidate_rows_loaded": len(rows),
                "searchable_events": total_searchable_events,
                "batch_events": len(targets),
                "target_offset": normalized_target_offset,
                "batch_end_offset": batch_end_offset,
                "has_more": has_more_targets,
                "next_target_offset": (
                    batch_end_offset
                    if has_more_targets and not batch_failed
                    else None
                ),
                "retry_target_offset": (
                    normalized_target_offset if batch_failed else None
                ),
                "skipped_outside_range": skipped_outside_range,
                "skipped_without_bounded_anchor": skipped_without_bounded_anchor,
                "events_with_candidates": sum(
                    1 for item in target_results if item["candidate_count"] > 0
                ),
                "events_without_candidates": sum(
                    1 for item in target_results if item["candidate_count"] == 0
                ),
            },
            "evidence": {
                "candidate_count": len(evidence_rows),
                "resolved_count": 0,
                "write_result": write_result,
            },
            "target_samples": target_results[:max(0, int(sample_limit))],
            "errors": errors[:max(0, int(sample_limit))],
            "ingestion_run_id": run_id,
        }

    async def analyze_cninfo_corporate_action_candidates(
        self,
        *,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        max_events: int = 100,
        target_offset: int = 0,
        profile: str = "semantic_extraction",
        resume: bool = True,
        dry_run: bool = True,
        download_documents: bool = True,
        run_ocr: bool = False,
        refresh_documents: bool = False,
        discover_candidates: bool = False,
        sample_limit: int = 20,
        llm_client: Any = None,
        ocr_adapter: Any = None,
    ) -> Dict[str, Any]:
        """Analyze CNInfo announcement candidates without promoting resolutions.

        Raw observations and effective-date evidence are deliberately read-only here.
        Write mode persists only document/page and LLM lineage; a human review is
        required before an effective date can be written as ``resolved``.
        """
        from tempfile import TemporaryDirectory
        from data_sources.cninfo_corporate_action_documents import (
            CninfoCorporateActionDocumentService,
            CorporateActionPageText,
            select_relevant_pages,
        )
        from data_sources.cninfo_corporate_action_llm import (
            CninfoCorporateActionLlmResolver,
            PARSER_VERSION,
            PROMPT_VERSION,
            SCHEMA_VERSION,
        )
        from utils.llm import LlmClient

        task_started = asyncio.get_running_loop().time()
        normalized_start = date.fromisoformat(str(start_date or "1990-12-19")[:10])
        normalized_end = date.fromisoformat(
            str(end_date or get_shanghai_time().date())[:10]
        )
        if normalized_end < normalized_start:
            raise ValueError("end_date must not be earlier than start_date")
        if refresh_documents and not download_documents:
            raise ValueError(
                "refresh_documents requires download_documents=true"
            )
        exchange_suffixes = {
            "SSE": ".SH", "SZSE": ".SZ", "BSE": ".BJ",
        }
        selected_exchanges = [str(item).upper() for item in (exchanges or ["SSE", "SZSE"])]
        unsupported = sorted(set(selected_exchanges) - set(exchange_suffixes))
        if unsupported:
            raise ValueError(f"unsupported A-share exchanges: {unsupported}")
        selected_ids = sorted({str(item).strip() for item in (instrument_ids or []) if str(item).strip()})
        dm_logger.info(
            "[DataManager] CNInfo LLM resolution preparing: range=%s..%s "
            "exchanges=%s instruments=%s max_events=%s offset=%s profile=%s "
            "resume=%s dry_run=%s download_documents=%s run_ocr=%s "
            "refresh_documents=%s discover_candidates=%s",
            normalized_start,
            normalized_end,
            selected_exchanges,
            selected_ids or "all",
            max_events,
            target_offset,
            profile,
            resume,
            dry_run,
            download_documents,
            run_ocr,
            refresh_documents,
            discover_candidates,
        )
        discovery_result = None
        if discover_candidates:
            dm_logger.info(
                "[DataManager] CNInfo LLM resolution candidate discovery started"
            )
            discovery_result = await self.discover_cninfo_special_action_effective_dates(
                start_date=normalized_start,
                end_date=normalized_end,
                exchanges=selected_exchanges,
                instrument_ids=selected_ids or None,
                dry_run=bool(dry_run),
                max_events=max_events,
                target_offset=target_offset,
            )
            dm_logger.info(
                "[DataManager] CNInfo LLM resolution candidate discovery completed: status=%s",
                (discovery_result or {}).get("status"),
            )
        params: Dict[str, Any] = {"start_date": normalized_start.isoformat(), "end_date": normalized_end.isoformat()}
        suffix_clauses = []
        for index, exchange in enumerate(selected_exchanges):
            key = f"suffix_{index}"
            params[key] = f"%{exchange_suffixes[exchange]}"
            suffix_clauses.append(f"o.instrument_id LIKE :{key}")
        id_clause = ""
        if selected_ids:
            keys = []
            for index, item in enumerate(selected_ids):
                key = f"instrument_{index}"
                params[key] = item
                keys.append(f":{key}")
            id_clause = f" AND o.instrument_id IN ({', '.join(keys)})"
        dm_logger.info(
            "[DataManager] CNInfo LLM resolution loading candidate evidence"
        )
        rows = await self.db_ops.execute_read_query(
            f"""
            SELECT o.instrument_id, o.source_event_key, o.source_profile,
                   o.action_type, o.fiscal_period, o.announcement_date,
                   o.record_date, o.ex_date, o.pay_date, o.share_arrival_date,
                   o.cash_dividend_per_share, o.bonus_shares_per_share,
                   o.capitalization_shares_per_share, o.rights_shares_per_share,
                   o.rights_price, o.description, o.raw_payload_json,
                   e.announcement_id, e.announcement_title, e.announcement_time,
                   e.evidence_url, e.raw_payload_json AS evidence_payload_json
            FROM corporate_action_observations o
            JOIN corporate_action_effective_date_evidence e
              ON e.instrument_id = o.instrument_id
             AND e.source_event_key = o.source_event_key
             AND e.resolution_status = 'candidate'
             AND e.evidence_source = 'cninfo_announcement_metadata'
            WHERE o.source = 'cninfo' AND o.is_current = 1
              AND o.source_profile IN ('cninfo_dividend', 'cninfo_allotment')
              AND (o.announcement_date >= :start_date AND o.announcement_date < :end_date_exclusive
                   OR o.record_date >= :start_date AND o.record_date < :end_date_exclusive
                   OR e.announcement_time >= :start_date AND e.announcement_time < :end_date_exclusive
                   OR o.fiscal_period LIKE :fiscal_year)
              AND ({' OR '.join(suffix_clauses)}) {id_clause}
            ORDER BY o.instrument_id, o.source_event_key, e.announcement_id
            """,
            {
                **params,
                "end_date_exclusive": (normalized_end + timedelta(days=1)).isoformat(),
                "fiscal_year": f"%{normalized_start.year}%",
            },
        )
        grouped: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            key = str(row.get("source_event_key") or "").strip()
            if not key:
                continue
            event = grouped.setdefault(key, {
                "instrument_id": row.get("instrument_id"),
                "source_event_key": key,
                "source_profile": row.get("source_profile"),
                "action_type": row.get("action_type"),
                "fiscal_period": row.get("fiscal_period"),
                "announcement_date": row.get("announcement_date"),
                "record_date": row.get("record_date"),
                "ex_date": row.get("ex_date"),
                "pay_date": row.get("pay_date"),
                "share_arrival_date": row.get("share_arrival_date"),
                "cash_dividend_per_share": row.get("cash_dividend_per_share"),
                "bonus_shares_per_share": row.get("bonus_shares_per_share"),
                "capitalization_shares_per_share": row.get("capitalization_shares_per_share"),
                "rights_shares_per_share": row.get("rights_shares_per_share"),
                "rights_price": row.get("rights_price"),
                "description": row.get("description"),
                "raw_payload_json": row.get("raw_payload_json"),
                "candidates": [],
            })
            event["candidates"].append({
                "announcement_id": row.get("announcement_id"),
                "announcement_title": row.get("announcement_title"),
                "announcement_time": row.get("announcement_time"),
                "evidence_url": row.get("evidence_url"),
                "raw_payload_json": row.get("evidence_payload_json"),
            })
        all_events = sorted(grouped.values(), key=lambda item: (str(item.get("instrument_id")), str(item.get("source_event_key"))))
        limit = max(1, min(int(max_events), 5000))
        offset = max(0, int(target_offset))
        batch = all_events[offset: offset + limit]
        has_more = offset + len(batch) < len(all_events)
        run_id = "a_share_cninfo_corporate_action_llm_" + hashlib.sha256(
            json.dumps({"start": normalized_start.isoformat(), "end": normalized_end.isoformat(), "ids": selected_ids, "offset": offset}, sort_keys=True).encode()
        ).hexdigest()[:16]
        dm_logger.info(
            "[DataManager] CNInfo LLM resolution candidates loaded: run_id=%s "
            "evidence_rows=%s candidate_events=%s batch_events=%s has_more=%s",
            run_id,
            len(rows),
            len(all_events),
            len(batch),
            has_more,
        )
        result: Dict[str, Any] = {
            "status": "dry_run" if dry_run else "success",
            "operation": "a_share_cninfo_corporate_action_llm_resolution",
            "dry_run": bool(dry_run),
            "production_isolation": True,
            "parameters": {
                "start_date": normalized_start.isoformat(),
                "end_date": normalized_end.isoformat(),
                "exchanges": selected_exchanges,
                "instrument_ids": selected_ids,
                "max_events": limit,
                "target_offset": offset,
                "profile": profile,
                "resume": bool(resume),
                "download_documents": bool(download_documents),
                "run_ocr": bool(run_ocr),
                "ocr_adapter_configured": ocr_adapter is not None,
                "refresh_documents": bool(refresh_documents),
                "discover_candidates": bool(discover_candidates),
            },
            "targets": {"candidate_events": len(all_events), "batch_events": len(batch), "has_more": has_more, "next_target_offset": offset + len(batch) if has_more else None},
            "counts": {"processed": 0, "analyzed": 0, "resumed": 0, "validated_candidates": 0, "manual_required": 0, "llm_disabled": 0, "document_failures": 0, "errors": 0, "persisted_analyses": 0},
            "review_workload": {
                "tiers": {
                    "machine_rework": 0,
                    "quick_review": 0,
                    "deep_review": 0,
                },
                "gate_signatures": {},
            },
            "llm_metrics": {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "provider_output_budget_overruns": 0,
                "latency_ms": {
                    "count": 0, "min": None, "p50": None, "p95": None, "max": None,
                },
            },
            "errors": [], "samples": [], "discovery": discovery_result,
            "ingestion_run_id": run_id,
        }
        latency_samples: List[int] = []
        llm_config = self.config.get_llm_config()
        if llm_client is None:
            if not llm_config.enabled or not (llm_config.profiles.get(profile) and llm_config.profiles[profile].enabled):
                dm_logger.warning(
                    "[DataManager] CNInfo LLM resolution stopped before analysis: "
                    "profile=%s gateway_enabled=%s profile_enabled=%s",
                    profile,
                    llm_config.enabled,
                    bool(llm_config.profiles.get(profile) and llm_config.profiles[profile].enabled),
                )
                result["counts"]["llm_disabled"] = len(batch)
                result["status"] = "partial" if batch else result["status"]
                return result
            llm_client = LlmClient(llm_config)
        configured_profile = llm_config.profiles.get(profile)
        dm_logger.info(
            "[DataManager] CNInfo LLM profile ready: run_id=%s profile=%s model=%s "
            "deadline_seconds=%s attempt_timeout_seconds=%s max_attempts=%s",
            run_id,
            profile,
            configured_profile.model if configured_profile else "injected_client",
            configured_profile.timeout_seconds if configured_profile else None,
            configured_profile.attempt_timeout_seconds if configured_profile else None,
            configured_profile.max_retries + 1 if configured_profile else None,
        )
        resolver = CninfoCorporateActionLlmResolver(
            llm_client,
            profile=profile,
            model_identity=configured_profile.model if configured_profile else None,
        )

        with TemporaryDirectory(prefix="cninfo_ca_llm_") as temporary_root:
            archive_root = temporary_root if dry_run else self.data_config.get("data_dir", "data")
            service = CninfoCorporateActionDocumentService(
                archive_root=Path(archive_root) / "filings" / "corporate_actions",
                ocr_adapter=ocr_adapter if run_ocr else None,
            )
            for event_index, event in enumerate(batch, start=1):
                event_started = asyncio.get_running_loop().time()
                result["counts"]["processed"] += 1
                dm_logger.info(
                    "[DataManager] CNInfo LLM event started: run_id=%s progress=%s/%s "
                    "instrument=%s event_key=%s profile=%s action_type=%s candidates=%s",
                    run_id,
                    event_index,
                    len(batch),
                    event.get("instrument_id"),
                    event.get("source_event_key"),
                    event.get("source_profile"),
                    event.get("action_type"),
                    len(event.get("candidates") or []),
                )
                pages: list[CorporateActionPageText] = []
                artifact_ids: list[int] = []
                for candidate_index, candidate in enumerate(event["candidates"], start=1):
                    announcement_id = str(candidate.get("announcement_id") or "").strip()
                    source_url = str(candidate.get("evidence_url") or "").strip()
                    if not announcement_id or not source_url:
                        result["counts"]["document_failures"] += 1
                        result["errors"].append({"source_event_key": event["source_event_key"], "code": "document_url_missing", "announcement_id": announcement_id})
                        continue
                    try:
                        existing = await self.db_ops.get_corporate_action_document_bundle(
                            announcement_id=announcement_id,
                            limit=1000,
                            offset=0,
                        )
                        should_download = bool(download_documents) and (
                            refresh_documents or not existing.get("items")
                        )
                        if should_download:
                            dm_logger.info(
                                "[DataManager] CNInfo document download started: run_id=%s "
                                "event=%s candidate=%s/%s announcement_id=%s refresh=%s",
                                run_id,
                                event.get("source_event_key"),
                                candidate_index,
                                len(event["candidates"]),
                                announcement_id,
                                refresh_documents,
                            )
                            bundle = await asyncio.to_thread(
                                service.ingest,
                                announcement_id=announcement_id,
                                source_url=source_url,
                                title=candidate.get("announcement_title"),
                                announcement_time=candidate.get("announcement_time"),
                            )
                            selected_bundle_pages = select_relevant_pages(bundle.pages)
                            pages.extend(selected_bundle_pages)
                            dm_logger.info(
                                "[DataManager] CNInfo document extracted: run_id=%s "
                                "event=%s announcement_id=%s bytes=%s pages_total=%s "
                                "pages_selected=%s extraction_status=%s",
                                run_id,
                                event.get("source_event_key"),
                                announcement_id,
                                bundle.content_length,
                                len(bundle.pages),
                                len(selected_bundle_pages),
                                bundle.extraction_status,
                            )
                            if not dry_run:
                                saved = await self.db_ops.save_corporate_action_document_bundle(
                                    bundle.artifact_row(
                                        title=candidate.get("announcement_title"),
                                        announcement_time=candidate.get("announcement_time"),
                                    ),
                                    [page.to_row() for page in bundle.pages],
                                )
                                artifact_ids.append(int(saved["artifact_id"]))
                                dm_logger.info(
                                    "[DataManager] CNInfo document lineage persisted: "
                                    "run_id=%s event=%s announcement_id=%s artifact_id=%s",
                                    run_id,
                                    event.get("source_event_key"),
                                    announcement_id,
                                    saved.get("artifact_id"),
                                )
                        elif existing.get("items"):
                            item = existing["items"][-1]
                            stored_pages: list[CorporateActionPageText] = []
                            for page in item.get("pages", []):
                                stored_pages.append(CorporateActionPageText(page_number=int(page["page_number"]), text=str(page["text"]), text_hash=str(page["text_hash"]), announcement_id=announcement_id, extraction_method=str(page.get("extraction_method") or "native_text"), quality_status=str(page.get("quality_status") or "usable")))
                            selected_stored_pages = select_relevant_pages(stored_pages)
                            pages.extend(selected_stored_pages)
                            artifact_ids.append(int(item["artifact_id"]))
                            dm_logger.info(
                                "[DataManager] CNInfo document reused: run_id=%s event=%s "
                                "announcement_id=%s artifact_id=%s stored_pages=%s "
                                "pages_selected=%s",
                                run_id,
                                event.get("source_event_key"),
                                announcement_id,
                                item.get("artifact_id"),
                                len(stored_pages),
                                len(selected_stored_pages),
                            )
                        else:
                            result["counts"]["document_failures"] += 1
                            result["errors"].append({"source_event_key": event["source_event_key"], "announcement_id": announcement_id, "code": "document_download_disabled"})
                    except Exception as exc:
                        result["counts"]["document_failures"] += 1
                        error_code = str(exc)
                        if (
                            run_ocr
                            and ocr_adapter is None
                            and error_code == "ocr_unavailable"
                        ):
                            error_code = "ocr_adapter_unconfigured"
                        result["errors"].append({
                            "source_event_key": event["source_event_key"],
                            "announcement_id": announcement_id,
                            "code": error_code,
                        })
                        dm_logger.warning(
                            "[DataManager] CNInfo document processing failed: run_id=%s "
                            "event=%s announcement_id=%s code=%s",
                            run_id,
                            event.get("source_event_key"),
                            announcement_id,
                            error_code,
                        )
                if not pages:
                    dm_logger.warning(
                        "[DataManager] CNInfo LLM event skipped without usable pages: "
                        "run_id=%s event=%s document_failures=%s",
                        run_id,
                        event.get("source_event_key"),
                        result["counts"]["document_failures"],
                    )
                    continue
                current_input_hash = resolver.input_hash(event, pages)
                prompt_payload = resolver.build_payload(event, pages)
                context_window = prompt_payload.get("context_window") or {}
                dm_logger.info(
                    "[DataManager] CNInfo LLM analysis started: run_id=%s event=%s "
                    "selected_pages=%s prompt_characters=%s context_complete=%s "
                    "input_hash=%s",
                    run_id,
                    event.get("source_event_key"),
                    len(pages),
                    context_window.get("prompt_characters"),
                    context_window.get("context_complete"),
                    current_input_hash[:16],
                )
                try:
                    if resume and not dry_run:
                        prior = await self.db_ops.get_corporate_action_llm_analyses(
                            instrument_id=event["instrument_id"],
                            source_event_key=event["source_event_key"],
                            limit=1000,
                            offset=0,
                        )
                        if any(
                            item.get("input_hash") == current_input_hash
                            and item.get("validation_status")
                            in {"validated_candidate", "manual_required", "no_matching_evidence"}
                            for item in prior.get("items", [])
                        ):
                            result["counts"]["resumed"] += 1
                            dm_logger.info(
                                "[DataManager] CNInfo LLM event resumed from prior analysis: "
                                "run_id=%s event=%s input_hash=%s",
                                run_id,
                                event.get("source_event_key"),
                                current_input_hash[:16],
                            )
                            continue
                    analysis = await resolver.analyze(event=event, pages=pages, allowed_start=normalized_start, allowed_end=normalized_end)
                    result["counts"]["analyzed"] += 1
                    if analysis.validation_status == "validated_candidate":
                        result["counts"]["validated_candidates"] += 1
                    else:
                        result["counts"]["manual_required"] += 1
                    classification = (
                        analysis.result.get("_review_classification") or {}
                    )
                    review_tier = str(
                        classification.get("review_tier") or "deep_review"
                    )
                    tier_counts = result["review_workload"]["tiers"]
                    tier_counts[review_tier] = int(tier_counts.get(review_tier, 0)) + 1
                    gate_signature = str(
                        classification.get("gate_signature") or "unclassified"
                    )
                    signature_counts = result["review_workload"]["gate_signatures"]
                    signature_counts[gate_signature] = int(
                        signature_counts.get(gate_signature, 0)
                    ) + 1
                    if analysis.usage:
                        for token_name in (
                            "input_tokens", "output_tokens", "total_tokens"
                        ):
                            result["llm_metrics"][token_name] += int(
                                analysis.usage.get(token_name) or 0
                            )
                    if "provider_output_budget_exceeded" in analysis.warnings:
                        result["llm_metrics"]["provider_output_budget_overruns"] += 1
                    if analysis.latency_ms is not None:
                        latency_samples.append(int(analysis.latency_ms))
                    dm_logger.info(
                        "[DataManager] CNInfo LLM analysis completed: run_id=%s event=%s "
                        "validation_status=%s review_tier=%s gate_signature=%s "
                        "attempts=%s latency_ms=%s passed_gates=%s/%s "
                        "failed_gates=%s date_facts=%s economic_primitives=%s "
                        "economic_derivations=%s derivation_conflicts=%s usage=%s warnings=%s",
                        run_id,
                        event.get("source_event_key"),
                        analysis.validation_status,
                        review_tier,
                        gate_signature,
                        analysis.attempt_count,
                        analysis.latency_ms,
                        sum(1 for value in analysis.gate_results.values() if value is True),
                        len(analysis.gate_results),
                        sorted(
                            key for key, value in analysis.gate_results.items()
                            if value is not True
                        ),
                        len(analysis.result.get("date_facts") or []),
                        len(analysis.result.get("economic_primitives") or []),
                        len(analysis.result.get("economic_derivations") or []),
                        len(analysis.result.get("economic_derivation_conflicts") or []),
                        analysis.usage,
                        list(analysis.warnings),
                    )
                    if not dry_run:
                        saved = await self.db_ops.save_corporate_action_llm_analysis({
                            "analysis_key": hashlib.sha256(f"{event['source_event_key']}:{analysis.input_hash}:{SCHEMA_VERSION}".encode()).hexdigest(),
                            "instrument_id": event["instrument_id"], "source_event_key": event["source_event_key"],
                            "analysis_status": analysis.result.get("analysis_status"), "validation_status": analysis.validation_status,
                            "profile": profile, "model": analysis.model, "schema_version": SCHEMA_VERSION,
                            "prompt_version": PROMPT_VERSION, "parser_version": PARSER_VERSION, "input_hash": analysis.input_hash,
                            "response_hash": analysis.response_hash, "request_id": analysis.request_id, "artifact_ids": artifact_ids,
                            "result": analysis.result, "gate_results": analysis.gate_results,
                            "usage": {
                                **(analysis.usage or {}),
                                "warnings": list(analysis.warnings),
                            },
                            "latency_ms": analysis.latency_ms, "attempt_count": analysis.attempt_count, "ingestion_run_id": run_id,
                        })
                        result["counts"]["persisted_analyses"] += 1 if saved else 0
                        dm_logger.info(
                            "[DataManager] CNInfo LLM analysis lineage persisted: "
                            "run_id=%s event=%s analysis_id=%s",
                            run_id,
                            event.get("source_event_key"),
                            saved.get("analysis_id") if isinstance(saved, dict) else None,
                        )
                    if len(result["samples"]) < max(0, int(sample_limit)):
                        result["samples"].append({"source_event_key": event["source_event_key"], "validation_status": analysis.validation_status, "gate_results": analysis.gate_results, "result": analysis.result})
                except Exception as exc:
                    result["counts"]["errors"] += 1
                    llm_error_code = str(
                        getattr(exc, "code", None) or "llm_failed"
                    )
                    result["review_workload"]["tiers"]["machine_rework"] += 1
                    error_signature = f"analysis_error:{llm_error_code}"
                    signature_counts = result["review_workload"]["gate_signatures"]
                    signature_counts[error_signature] = int(
                        signature_counts.get(error_signature, 0)
                    ) + 1
                    result["errors"].append({
                        "source_event_key": event["source_event_key"],
                        "code": llm_error_code,
                        "error": str(exc),
                        "attempt_count": getattr(exc, "attempt_count", None),
                    })
                    dm_logger.warning(
                        "[DataManager] CNInfo LLM analysis failed: run_id=%s event=%s "
                        "code=%s attempts=%s elapsed_seconds=%.1f detail=%s",
                        run_id,
                        event.get("source_event_key"),
                        llm_error_code,
                        getattr(exc, "attempt_count", None),
                        asyncio.get_running_loop().time() - event_started,
                        str(exc),
                    )
                    if not dry_run:
                        try:
                            await self.db_ops.save_corporate_action_llm_analysis({
                                "analysis_key": hashlib.sha256(f"{event['source_event_key']}:{current_input_hash}:{SCHEMA_VERSION}".encode()).hexdigest(),
                                "instrument_id": event["instrument_id"],
                                "source_event_key": event["source_event_key"],
                                "analysis_status": "manual_required",
                                "validation_status": "failed",
                                "profile": profile,
                                "model": configured_profile.model if configured_profile else None,
                                "schema_version": SCHEMA_VERSION,
                                "prompt_version": PROMPT_VERSION,
                                "parser_version": PARSER_VERSION,
                                "input_hash": current_input_hash,
                                "artifact_ids": artifact_ids,
                                "result": {},
                                "gate_results": {},
                                "attempt_count": int(
                                    getattr(exc, "attempt_count", 0) or 0
                                ),
                                "error_code": llm_error_code,
                                "error_message": str(exc),
                                "ingestion_run_id": run_id,
                            })
                        except Exception as persistence_exc:
                            result["errors"].append({
                                "source_event_key": event["source_event_key"],
                                "code": "persistence_failed",
                                "error": str(persistence_exc),
                            })
                            dm_logger.exception(
                                "[DataManager] CNInfo LLM failure lineage persistence failed: "
                                "run_id=%s event=%s",
                                run_id,
                                event.get("source_event_key"),
                            )
                dm_logger.info(
                    "[DataManager] CNInfo LLM event finished: run_id=%s progress=%s/%s "
                    "elapsed_seconds=%.1f analyzed=%s errors=%s document_failures=%s",
                    run_id,
                    event_index,
                    len(batch),
                    asyncio.get_running_loop().time() - event_started,
                    result["counts"]["analyzed"],
                    result["counts"]["errors"],
                    result["counts"]["document_failures"],
                )
        if latency_samples:
            ordered_latencies = sorted(latency_samples)
            def _latency_percentile(percentile: float) -> int:
                index = round((len(ordered_latencies) - 1) * percentile)
                return ordered_latencies[max(0, min(index, len(ordered_latencies) - 1))]

            result["llm_metrics"]["latency_ms"] = {
                "count": len(ordered_latencies),
                "min": ordered_latencies[0],
                "p50": _latency_percentile(0.50),
                "p95": _latency_percentile(0.95),
                "max": ordered_latencies[-1],
            }
        result["status"] = "partial" if result["errors"] or result["counts"]["document_failures"] or has_more else result["status"]
        result["checkpoint"] = {"run_id": run_id, "next_target_offset": result["targets"]["next_target_offset"], "input_event_keys": [item["source_event_key"] for item in batch]}
        dm_logger.info(
            "[DataManager] CNInfo LLM resolution finished: run_id=%s status=%s "
            "elapsed_seconds=%.1f processed=%s analyzed=%s validated=%s manual=%s "
            "machine_rework=%s quick_review=%s deep_review=%s input_tokens=%s "
            "output_tokens=%s budget_overruns=%s resumed=%s document_failures=%s "
            "errors=%s persisted=%s next_offset=%s",
            run_id,
            result["status"],
            asyncio.get_running_loop().time() - task_started,
            result["counts"]["processed"],
            result["counts"]["analyzed"],
            result["counts"]["validated_candidates"],
            result["counts"]["manual_required"],
            result["review_workload"]["tiers"].get("machine_rework", 0),
            result["review_workload"]["tiers"].get("quick_review", 0),
            result["review_workload"]["tiers"].get("deep_review", 0),
            result["llm_metrics"]["input_tokens"],
            result["llm_metrics"]["output_tokens"],
            result["llm_metrics"]["provider_output_budget_overruns"],
            result["counts"]["resumed"],
            result["counts"]["document_failures"],
            result["counts"]["errors"],
            result["counts"]["persisted_analyses"],
            result["targets"]["next_target_offset"],
        )
        return result

    async def review_cninfo_corporate_action_resolution(
        self,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Persist an evidence-bound review without changing raw CNInfo facts."""
        from data_sources.cninfo_corporate_action_documents import (
            CorporateActionPageText,
        )
        from data_sources.cninfo_corporate_action_llm import (
            analysis_schema_for_version,
            canonical_supported_economic_fields,
            normalize_analysis_result,
            official_quote_supports_date,
            validate_analysis,
        )
        from utils.llm import LlmSchemaValidationError
        from utils.llm.schema import validate_data

        instrument_id = convert_to_database_format(str(payload.get("instrument_id") or "").strip())
        source_event_key = str(payload.get("source_event_key") or "").strip()
        reviewer = str(payload.get("reviewer") or "").strip()
        decision = str(payload.get("decision") or "").strip().lower()
        analysis_id = int(payload.get("analysis_id") or 0)
        evidence_key = str(payload.get("evidence_key") or "").strip()
        if not all((instrument_id, source_event_key, reviewer, decision, analysis_id)):
            raise ValueError("instrument_id, source_event_key, analysis_id, decision, and reviewer are required")
        if len(reviewer) > 128:
            raise ValueError("reviewer exceeds 128 characters")
        if len(str(payload.get("notes") or "")) > 4000:
            raise ValueError("notes exceeds 4000 characters")
        analyses = await self.db_ops.get_corporate_action_llm_analyses(
            instrument_id=instrument_id,
            source_event_key=source_event_key,
            limit=1000,
            offset=0,
        )
        analysis = next(
            (item for item in analyses.get("items", []) if int(item.get("analysis_id") or 0) == analysis_id),
            None,
        )
        if analysis is None:
            raise ValueError("analysis_id does not belong to the requested event")
        original_result = deepcopy(analysis.get("result") or {})
        original_classification = original_result.get("_review_classification") or {}
        if (
            payload.get("_batch_review")
            and decision == "resolved"
            and original_classification.get("review_tier") != "quick_review"
        ):
            raise ValueError("batch resolved review requires quick_review tier")
        corrected_result = payload.get("corrected_result")
        if corrected_result is not None and decision != "resolved":
            raise ValueError("corrected_result is supported only for resolved reviews")
        if corrected_result is not None and not isinstance(corrected_result, dict):
            raise ValueError("corrected_result must be an object")
        allowed_corrections = {
            "event_type",
            "effective_date",
            "effective_date_type",
            "date_basis",
            "economic_terms",
            "evidence",
            "alternative_dates",
            "date_facts",
            "economic_primitives",
            "conflicts",
        }
        unsupported_corrections = sorted(
            set((corrected_result or {}).keys()) - allowed_corrections
        )
        if unsupported_corrections:
            raise ValueError(
                "unsupported corrected_result fields: "
                + ", ".join(unsupported_corrections)
            )
        proposed_result = {
            key: value for key, value in original_result.items()
            if not str(key).startswith("_")
        }
        if corrected_result:
            proposed_result.update(deepcopy(corrected_result))
        proposed_result.update({
            "schema_version": original_result.get("schema_version"),
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "event_match": True,
        })
        if decision == "resolved":
            proposed_result["analysis_status"] = "resolved_candidate"

        effective_date = payload.get("effective_date")
        date_basis = str(payload.get("date_basis") or "").strip() or None
        selected_evidence = None
        resolved_source_profile = None
        post_gate_results = None
        post_validation_status = None
        validated_result = original_result
        if decision == "resolved":
            if analysis.get("validation_status") not in {
                "validated_candidate", "manual_required",
            }:
                raise ValueError(
                    "resolved review requires a candidate analysis"
                )
            normalized_proposed = normalize_analysis_result(proposed_result)
            try:
                validate_data(
                    normalized_proposed,
                    analysis_schema_for_version(
                        normalized_proposed.get("schema_version")
                    ),
                )
            except (LlmSchemaValidationError, ValueError) as exc:
                raise ValueError("corrected_result does not match the analysis schema") from exc
            if not evidence_key:
                raise ValueError(
                    "resolved review requires evidence_key"
                )

            observation_page = await self.db_ops.get_corporate_action_observations(
                instrument_id=instrument_id,
                source_event_key=source_event_key,
                source="cninfo",
                include_inactive=True,
                limit=10,
                offset=0,
            )
            observation = next(iter(observation_page.get("items", [])), None)
            if observation is None:
                raise ValueError("stored CNInfo observation is missing")
            candidate_page = await self.db_ops.get_corporate_action_effective_date_evidence(
                instrument_id=instrument_id,
                source_event_key=source_event_key,
                evidence_source="cninfo_announcement_metadata",
                limit=1000,
                offset=0,
            )
            candidates = candidate_page.get("items", [])
            candidate_by_announcement = {
                str(item.get("announcement_id") or ""): item
                for item in candidates
                if str(item.get("announcement_id") or "")
            }
            pages: list[CorporateActionPageText] = []
            announcement_ids = sorted({
                str(item.get("announcement_id") or "").strip()
                for item in normalized_proposed.get("evidence", [])
                if isinstance(item, dict)
                and str(item.get("announcement_id") or "").strip()
            })
            unsupported_announcements = sorted(
                set(announcement_ids) - set(candidate_by_announcement)
            )
            if unsupported_announcements:
                raise ValueError(
                    "corrected evidence is not linked to the requested event: "
                    + ", ".join(unsupported_announcements)
                )
            for announcement_id in announcement_ids:
                documents = await self.db_ops.get_corporate_action_document_bundle(
                    announcement_id=announcement_id,
                    limit=1000,
                    offset=0,
                )
                for artifact in documents.get("items", []):
                    for page in artifact.get("pages", []):
                        pages.append(CorporateActionPageText(
                            page_number=int(page.get("page_number") or 0),
                            text=str(page.get("text") or ""),
                            text_hash=str(page.get("text_hash") or ""),
                            announcement_id=announcement_id,
                            extraction_method=str(
                                page.get("extraction_method") or "native_text"
                            ),
                            quality_status=str(
                                page.get("quality_status") or "usable"
                            ),
                        ))
            if not pages:
                raise ValueError("archived official pages are missing")
            input_context = original_result.get("_input_context") or {}
            allowed_start_value = input_context.get("allowed_start")
            allowed_end_value = input_context.get("allowed_end")
            post_validation_status, post_gate_results, validated_result = validate_analysis(
                normalized_proposed,
                instrument_id=instrument_id,
                source_event_key=source_event_key,
                pages=pages,
                allowed_start=(
                    date.fromisoformat(str(allowed_start_value)[:10])
                    if allowed_start_value else None
                ),
                allowed_end=(
                    date.fromisoformat(str(allowed_end_value)[:10])
                    if allowed_end_value else None
                ),
                source_profile=observation.get("source_profile"),
                action_type=observation.get("action_type"),
                candidate_titles=tuple(
                    str(item.get("announcement_title") or "")
                    for item in candidates
                ),
                context_complete=bool(input_context.get("context_complete", True)),
            )
            if post_validation_status != "validated_candidate" or not all(
                bool(value) for value in post_gate_results.values()
            ):
                failed = sorted(
                    name for name, passed in post_gate_results.items()
                    if not bool(passed)
                )
                raise ValueError(
                    "corrected result failed evidence gates: " + ", ".join(failed)
                )
            validated_effective_date = validated_result.get("effective_date")
            validated_date_basis = str(
                validated_result.get("date_basis") or ""
            ).strip() or None
            if payload.get("effective_date") and str(payload["effective_date"])[:10] != str(
                validated_effective_date or ""
            )[:10]:
                raise ValueError(
                    "review effective_date conflicts with validated result"
                )
            if payload.get("date_basis") and str(payload["date_basis"]).strip() != str(
                validated_date_basis or ""
            ).strip():
                raise ValueError("review date_basis conflicts with validated result")
            effective_date = payload.get("effective_date") or validated_effective_date
            date_basis = (
                str(payload.get("date_basis") or "").strip()
                or validated_date_basis
            )
            if not effective_date or not date_basis:
                raise ValueError(
                    "resolved review requires effective_date and date_basis"
                )
            result_evidence = [
                item for item in validated_result.get("evidence", [])
                if isinstance(item, dict)
            ]
            selected_date_fact = next((
                item for item in validated_result.get("date_facts", [])
                if isinstance(item, dict)
                and str(item.get("date") or "")[:10] == str(effective_date)[:10]
                and str(item.get("date_type") or "") == str(
                    validated_result.get("effective_date_type") or ""
                )
            ), None)
            preferred_evidence_ids = set(
                (selected_date_fact or {}).get("evidence_ids") or []
            )
            selected_evidence = next((
                item for item in result_evidence
                if str(item.get("announcement_id") or "") == evidence_key
                and (
                    not preferred_evidence_ids
                    or str(item.get("evidence_id") or "") in preferred_evidence_ids
                )
                and str(item.get("exact_quote") or "").strip()
                and int(item.get("page_number") or 0) > 0
                and str(item.get("text_hash") or "").strip()
            ), None)
            if selected_evidence is None:
                raise ValueError("selected official page evidence is missing or incomplete")
            try:
                date.fromisoformat(str(effective_date)[:10])
            except ValueError as exc:
                raise ValueError("resolved effective_date must be ISO formatted") from exc
            if not official_quote_supports_date(
                str(effective_date)[:10],
                str(selected_evidence.get("exact_quote") or ""),
            ):
                raise ValueError("review effective_date must appear in the selected official quote")
            resolved_source_profile = (
                candidate_by_announcement.get(evidence_key) or {}
            ).get("source_profile")
            if not resolved_source_profile:
                raise ValueError("selected evidence is not a stored CNInfo announcement candidate")
        elif decision not in {"rejected", "conflict", "manual_required"}:
            raise ValueError("unsupported corporate-action review decision")
        review_key = hashlib.sha256(json.dumps({
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "analysis_id": analysis_id,
            "decision": decision,
            "effective_date": str(effective_date or ""),
            "date_basis": date_basis,
            "evidence_key": evidence_key,
            "reviewer": reviewer,
            "notes": str(payload.get("notes") or ""),
            "corrected_result": corrected_result or {},
        }, sort_keys=True).encode()).hexdigest()
        review_row = {
            "review_key": review_key,
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "analysis_id": analysis_id,
            "evidence_key": evidence_key or None,
            "decision": decision,
            "effective_date": effective_date,
            "date_basis": date_basis,
            "reviewer": reviewer,
            "notes": payload.get("notes"),
            "review_payload": {
                "original_result": original_result,
                "corrected_result": corrected_result,
                "validated_result": validated_result if decision == "resolved" else None,
                "pre_validation_status": analysis.get("validation_status"),
                "post_validation_status": post_validation_status,
                "pre_gate_results": analysis.get("gate_results") or {},
                "post_gate_results": post_gate_results,
                "selected_evidence": selected_evidence,
                "analysis_versions": {
                    "schema_version": analysis.get("schema_version"),
                    "prompt_version": analysis.get("prompt_version"),
                    "parser_version": analysis.get("parser_version"),
                    "input_hash": analysis.get("input_hash"),
                    "response_hash": analysis.get("response_hash"),
                },
            },
            "supersedes_review_id": payload.get("supersedes_review_id"),
        }
        analysis_result = validated_result if decision == "resolved" else original_result
        economic_terms = analysis_result.get("economic_terms") or {}
        term_field_map = {
            "cash_dividend": "cash_dividend_per_share",
            "bonus_shares": "bonus_shares_per_share",
            "capitalization_shares": "capitalization_shares_per_share",
            "rights_shares": "rights_shares_per_share",
            "rights_price": "rights_price",
        }
        normalized_terms = {field_name: None for field_name in term_field_map.values()}
        resolved_fields = set()
        currency = None
        economic_field_evidence: Dict[str, List[Dict[str, Any]]] = {}
        if decision == "resolved":
            evidence_by_id = {
                str(item.get("evidence_id") or ""): item
                for item in analysis_result.get("evidence", [])
                if isinstance(item, dict)
                and str(item.get("evidence_id") or "")
            }
            for evidence in analysis_result.get("evidence", []):
                if not isinstance(evidence, dict):
                    continue
                for source_name in canonical_supported_economic_fields(
                    evidence.get("supports_fields")
                ):
                    economic_field_evidence.setdefault(source_name, []).append(evidence)
            for derivation in analysis_result.get("economic_derivations", []):
                if not isinstance(derivation, dict):
                    continue
                source_name = str(derivation.get("output_field") or "")
                if source_name not in term_field_map:
                    continue
                for evidence_id in derivation.get("evidence_ids") or []:
                    evidence = evidence_by_id.get(str(evidence_id))
                    if (
                        evidence is not None
                        and evidence not in economic_field_evidence.setdefault(
                            source_name, []
                        )
                    ):
                        economic_field_evidence[source_name].append(evidence)
            for source_name, field_name in term_field_map.items():
                term = economic_terms.get(source_name)
                if (
                    not isinstance(term, dict)
                    or term.get("value") is None
                    or not economic_field_evidence.get(source_name)
                ):
                    continue
                value = float(term["value"])
                unit = str(term.get("unit") or "")
                if unit == "per_10_shares":
                    value /= 10.0
                normalized_terms[field_name] = value
                resolved_fields.add(field_name)
                currency = currency or term.get("currency")
        terms_row = {
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "analysis_id": analysis_id,
            **normalized_terms,
            "currency": currency,
            "resolved_fields": sorted(resolved_fields),
            "evidence": {
                "selected_date_evidence": selected_evidence,
                "economic_field_evidence": economic_field_evidence,
            } if decision == "resolved" else {},
            "is_active": decision == "resolved",
        }
        evidence_row = None
        if decision == "resolved":
            evidence_row = {
                "instrument_id": instrument_id,
                "source_event_key": source_event_key,
                "observation_source": "cninfo",
                "source_profile": resolved_source_profile,
                "evidence_source": "cninfo_reviewed_official_document",
                "evidence_key": source_event_key,
                "resolution_status": "resolved",
                "effective_date": effective_date,
                "date_basis": date_basis,
                "announcement_id": evidence_key,
                "announcement_title": None,
                "evidence_url": None,
                "confidence": analysis_result.get("confidence"),
                "raw_payload": {
                    "review_key": review_key,
                    "analysis_id": analysis_id,
                    "selected_evidence": selected_evidence,
                    "corrected_result": corrected_result,
                },
            }
        else:
            prior = await self.db_ops.get_corporate_action_effective_date_evidence(
                instrument_id=instrument_id,
                source_event_key=source_event_key,
                evidence_source="cninfo_reviewed_official_document",
                limit=10,
                offset=0,
            )
            if prior.get("items"):
                prior_row = prior["items"][0]
                evidence_row = {
                    "instrument_id": instrument_id,
                    "source_event_key": source_event_key,
                    "observation_source": "cninfo",
                    "source_profile": prior_row.get("source_profile"),
                    "evidence_source": "cninfo_reviewed_official_document",
                    "evidence_key": source_event_key,
                    "resolution_status": "rejected",
                    "raw_payload": {"review_key": review_key, "decision": decision},
                }
        bundle_write = await self.db_ops.save_corporate_action_review_bundle(
            review_row=review_row,
            terms_row=terms_row,
            evidence_row=evidence_row,
            ingestion_run_id=f"corporate_action_review_{review_key[:16]}",
        )
        return {
            "status": "success",
            "review": bundle_write["review"],
            "terms_write": bundle_write["terms_write"],
            "evidence_write": bundle_write["evidence_write"],
            "raw_observation_modified": False,
            "production_factor_modified": False,
        }

    async def review_cninfo_corporate_action_resolutions_batch(
        self,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Review at most 100 quick-review items with per-item isolation."""
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("batch review requires a non-empty items array")
        if len(items) > 100:
            raise ValueError("batch review accepts at most 100 items")
        default_reviewer = str(payload.get("reviewer") or "").strip()
        results = []
        succeeded = 0
        failed = 0
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                failed += 1
                results.append({
                    "index": index,
                    "status": "failed",
                    "error": "batch item must be an object",
                })
                continue
            request = dict(item)
            request.setdefault("reviewer", default_reviewer)
            request["_batch_review"] = True
            try:
                reviewed = await self.review_cninfo_corporate_action_resolution(
                    request
                )
                succeeded += 1
                results.append({
                    "index": index,
                    "status": "success",
                    "review": reviewed.get("review"),
                    "evidence_write": reviewed.get("evidence_write"),
                })
            except Exception as exc:
                failed += 1
                results.append({
                    "index": index,
                    "status": "failed",
                    "error": str(exc),
                })
        return {
            "status": "success" if failed == 0 else "partial",
            "total": len(items),
            "succeeded": succeeded,
            "failed": failed,
            "items": results,
            "raw_observation_modified": False,
            "production_factor_modified": False,
        }

    async def validate_a_share_corporate_actions(
        self,
        *,
        start_date: date,
        end_date: date,
        exchanges: Optional[List[str]] = None,
        instrument_ids: Optional[List[str]] = None,
        reference_sources: Optional[List[str]] = None,
        scan_official_announcements: bool = True,
        official_sample_limit: int = 50,
        official_lookback_years: int = 3,
        field_tolerance: float = 0.0001,
        acceptable_cumulative_error_pct: float = 0.1,
        warning_cumulative_error_pct: float = 0.5,
        per_source_timeout_sec: int = 60,
        sample_limit: int = 20,
    ) -> Dict[str, Any]:
        """Run read-only event, official-evidence, and cumulative validation."""
        if end_date < start_date:
            raise ValueError('end_date must not be earlier than start_date')
        normalized_exchanges = list(dict.fromkeys(
            str(item).strip().upper()
            for item in (exchanges or ['SSE', 'SZSE', 'BSE'])
            if str(item).strip()
        ))
        unsupported = sorted(set(normalized_exchanges) - {'SSE', 'SZSE', 'BSE'})
        if unsupported:
            raise ValueError(f'unsupported A-share exchanges: {unsupported}')
        sources = list(dict.fromkeys(
            str(item).strip().lower()
            for item in (reference_sources or ['baostock', 'akshare'])
            if str(item).strip()
        ))

        requested_ids = list(dict.fromkeys(
            str(item).strip().upper()
            for item in (instrument_ids or [])
            if str(item).strip()
        ))
        instruments: List[Dict[str, Any]] = []
        if requested_ids:
            requested_set = set(requested_ids)
            for exchange in normalized_exchanges:
                rows = await self.db_ops.get_research_target_instruments_by_exchange(
                    exchange,
                    is_active=None,
                )
                instruments.extend(
                    item for item in rows
                    if str(item.get('instrument_id')) in requested_set
                )
        else:
            for exchange in normalized_exchanges:
                instruments.extend(
                    await self.db_ops.get_research_target_instruments_by_exchange(
                        exchange,
                        is_active=None,
                    )
                )
        instrument_ids_final = sorted({
            str(item.get('instrument_id') or '').strip()
            for item in instruments
            if str(item.get('instrument_id') or '').strip()
        })
        symbol_to_instrument = {
            str(item.get('symbol') or '').strip().zfill(6): str(item['instrument_id'])
            for item in instruments
            if item.get('symbol') and item.get('instrument_id')
        }
        if not instrument_ids_final:
            return {
                'status': 'blocked',
                'operation': 'a_share_corporate_action_validation',
                'blockers': ['validation_universe_empty'],
                'warnings': [],
                'errors': [],
            }

        tdx_rows: List[Dict[str, Any]] = []
        factor_rows: List[Dict[str, Any]] = []
        for offset in range(0, len(instrument_ids_final), 500):
            chunk = instrument_ids_final[offset: offset + 500]
            placeholders = ', '.join(f':id_{idx}' for idx in range(len(chunk)))
            source_placeholders = ', '.join(
                f':source_{idx}' for idx in range(len(sources))
            )
            params: Dict[str, Any] = {
                f'id_{idx}': instrument_id
                for idx, instrument_id in enumerate(chunk)
            }
            params.update({
                f'source_{idx}': source for idx, source in enumerate(sources)
            })
            params.update({
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
            })
            tdx_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date,
                       factor, cumulative_factor, validation_result,
                       fenhong, songzhuangu, peigu, peigujia
                FROM adjustment_factors_tdx
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                ORDER BY instrument_id, ex_date
                """,
                params,
            ))
            factor_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date, source,
                       factor, cumulative_factor
                FROM adjustment_factors
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) <= :end_date
                  AND lower(source) IN ({source_placeholders})
                ORDER BY instrument_id, source, ex_date
                """,
                params,
            ))

        tdx_events = normalize_tdx_events(
            tdx_rows,
            start_date=start_date,
            end_date=end_date,
        )
        eastmoney_fetch = await self._fetch_eastmoney_corporate_action_rows(
            start_date=start_date,
            end_date=end_date,
            target_symbols=set(symbol_to_instrument),
            per_period_timeout_sec=per_source_timeout_sec,
        )
        eastmoney_events = normalize_eastmoney_events(
            eastmoney_fetch.get('rows') or [],
            symbol_to_instrument=symbol_to_instrument,
            start_date=start_date,
            end_date=end_date,
        )

        sessions_by_exchange: Dict[str, List[date]] = {}
        for exchange in normalized_exchanges:
            records = await self.db_ops.get_trading_calendar_records(
                exchange,
                start_date - timedelta(days=14),
                end_date + timedelta(days=14),
            )
            sessions_by_exchange[exchange] = sorted({
                parsed
                for record in records
                if record.get('is_trading_day')
                if (parsed := self._date_from_any(record.get('date'))) is not None
            })

        event_validation = reconcile_event_fields(
            tdx_events,
            eastmoney_events,
            trading_sessions_by_exchange=sessions_by_exchange,
            field_tolerance=field_tolerance,
            sample_limit=sample_limit,
        )
        cumulative_validation = compare_cumulative_factor_paths(
            tdx_events,
            factor_rows,
            start_date=start_date,
            end_date=end_date,
            reference_sources=sources,
            instrument_ids=instrument_ids_final,
            acceptable_error_pct=acceptable_cumulative_error_pct,
            warning_error_pct=warning_cumulative_error_pct,
            sample_limit=sample_limit,
        )

        official_validation: Dict[str, Any] = {
            'status': 'skipped',
            'reason': 'official_scan_disabled',
            'evidence_scope': 'announcement_existence_only',
            'totals': {},
        }
        official_scan: Dict[str, Any] = {
            'status': 'skipped',
            'records': [],
            'errors': [],
        }
        if scan_official_announcements:
            follow_up_ids = event_validation.get('follow_up_instrument_ids') or []
            if requested_ids:
                available_ids = set(instrument_ids_final)
                follow_up_ids = [
                    item for item in requested_ids if item in available_ids
                ]
            official_targets = follow_up_ids[:max(0, int(official_sample_limit))]
            official_start = max(
                start_date,
                end_date - timedelta(days=max(1, int(official_lookback_years)) * 366),
            )
            official_target_set = set(official_targets)
            official_event_map: Dict[tuple[str, date], Dict[str, Any]] = {}
            for item in [*eastmoney_events, *tdx_events]:
                if (
                    item['instrument_id'] in official_target_set
                    and item['ex_date'] >= official_start
                    and (
                        float(item.get('cash_per_10') or 0.0) > 0
                        or float(item.get('bonus_per_10') or 0.0) > 0
                    )
                ):
                    official_event_map[(
                        item['instrument_id'], item['ex_date']
                    )] = dict(item)
            official_events = sorted(
                official_event_map.values(),
                key=lambda item: (item['instrument_id'], item['ex_date']),
            )
            if official_targets:
                official_scan = await self._scan_cninfo_corporate_action_announcements(
                    instrument_ids=official_targets,
                    start_date=official_start - timedelta(days=180),
                    end_date=end_date,
                    per_instrument_timeout_sec=per_source_timeout_sec,
                )
                announcements = normalize_cninfo_implementation_announcements(
                    official_scan.get('records') or [],
                    symbol_to_instrument=symbol_to_instrument,
                )
                official_validation = match_official_announcement_evidence(
                    official_events,
                    announcements,
                    sample_limit=sample_limit,
                )
                official_validation['target_instrument_ids'] = official_targets
                official_validation['scan_start_date'] = official_start.isoformat()
                official_validation['scan_errors'] = official_scan.get('errors') or []
                if official_scan.get('errors'):
                    official_validation['status'] = 'partial'
            else:
                official_validation = {
                    'status': 'success',
                    'reason': 'no_event_follow_up_targets',
                    'evidence_scope': 'announcement_existence_only',
                    'totals': {
                        'events_checked': 0,
                        'official_announcement_evidence_found': 0,
                        'official_announcement_evidence_not_found': 0,
                    },
                }

        reasons: List[str] = []
        if eastmoney_fetch.get('status') != 'success':
            reasons.append('eastmoney_source_coverage_partial')
        if event_validation.get('status') != 'success':
            reasons.append('event_field_evidence_unresolved')
        if cumulative_validation.get('status') != 'success':
            reasons.append('cumulative_factor_evidence_unresolved')
        if official_validation.get('status') not in {'success', 'skipped'}:
            reasons.append('official_announcement_evidence_unresolved')
        return {
            'status': 'partial' if reasons else 'success',
            'operation': 'a_share_corporate_action_validation',
            'read_only': True,
            'parameters': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'exchanges': normalized_exchanges,
                'instrument_ids': requested_ids,
                'reference_sources': sources,
                'scan_official_announcements': scan_official_announcements,
                'official_sample_limit': official_sample_limit,
                'official_lookback_years': official_lookback_years,
                'field_tolerance': field_tolerance,
                'acceptable_cumulative_error_pct': acceptable_cumulative_error_pct,
                'warning_cumulative_error_pct': warning_cumulative_error_pct,
            },
            'universe': {
                'instrument_count': len(instrument_ids_final),
                'tdx_events': len(tdx_events),
                'eastmoney_events': len(eastmoney_events),
            },
            'source_coverage': {
                key: value
                for key, value in eastmoney_fetch.items()
                if key != 'rows'
            },
            'event_validation': event_validation,
            'official_validation': official_validation,
            'cumulative_validation': cumulative_validation,
            'reasons': reasons,
            'warnings': [],
            'errors': [],
        }

    async def reconcile_tdx_xdxr_history(
        self,
        *,
        start_date: date,
        end_date: date,
        instrument_ids: List[str],
        reference_sources: Optional[List[str]] = None,
        sample_limit: int = 20,
    ) -> Dict[str, Any]:
        """Reconcile TDX actions with independent cumulative-factor evidence."""
        normalized_ids = list(dict.fromkeys(
            str(item).strip() for item in (instrument_ids or []) if str(item).strip()
        ))
        sources = list(dict.fromkeys(
            str(item).strip().lower()
            for item in (reference_sources or ['baostock', 'akshare'])
            if str(item).strip()
        ))
        tdx_rows: List[Dict[str, Any]] = []
        reference_rows: List[Dict[str, Any]] = []
        for offset in range(0, len(normalized_ids), 500):
            chunk = normalized_ids[offset: offset + 500]
            placeholders = ", ".join(f":id_{idx}" for idx in range(len(chunk)))
            source_placeholders = ", ".join(
                f":source_{idx}" for idx in range(len(sources))
            )
            params: Dict[str, Any] = {
                f"id_{idx}": instrument_id
                for idx, instrument_id in enumerate(chunk)
            }
            params.update({
                f"source_{idx}": source
                for idx, source in enumerate(sources)
            })
            params.update({
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
            })
            tdx_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date,
                       factor, cumulative_factor, validation_result,
                       pre_close, fenhong, songzhuangu, peigu, peigujia
                FROM adjustment_factors_tdx
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) BETWEEN :start_date AND :end_date
                ORDER BY instrument_id, ex_date
                """,
                params,
            ))
            reference_rows.extend(await self.db_ops.execute_read_query(
                f"""
                SELECT instrument_id, date(ex_date) AS ex_date, source,
                       factor, cumulative_factor
                FROM adjustment_factors
                WHERE instrument_id IN ({placeholders})
                  AND date(ex_date) <= :end_date
                  AND lower(source) IN ({source_placeholders})
                ORDER BY instrument_id, ex_date
                """,
                params,
            ))

        def _positive_float(value: Any) -> Optional[float]:
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            if pd.isna(number) or number <= 0:
                return None
            return number

        def _event_date(value: Any) -> Optional[date]:
            return self._date_from_any(value)

        def _exchange_for(instrument_id: str) -> Optional[str]:
            normalized = str(instrument_id or '').upper()
            if normalized.endswith('.SH'):
                return 'SSE'
            if normalized.endswith('.SZ'):
                return 'SZSE'
            if normalized.endswith(('.BJ', '.BSE')):
                return 'BSE'
            return None

        def _factor_diff(left: Optional[float], right: Optional[float]) -> Optional[float]:
            if left is None or right is None or right <= 0:
                return None
            return abs((left / right) - 1.0)

        def _action_bucket(item: Dict[str, Any]) -> str:
            cash = float(item.get('fenhong') or 0.0) > 0
            bonus = float(item.get('songzhuangu') or 0.0) > 0
            rights = float(item.get('peigu') or 0.0) > 0
            active = sum((cash, bonus, rights))
            if active > 1:
                return 'mixed'
            if cash:
                return 'cash_only'
            if bonus:
                return 'bonus_only'
            if rights:
                return 'rights_only'
            return 'no_standard_action'

        def _decade_bucket(item: Dict[str, Any], key: str) -> str:
            value = item.get(key)
            parsed = value if isinstance(value, date) else _event_date(value)
            return f'{parsed.year // 10 * 10}s' if parsed else 'unknown'

        def _session_distance(left: date, right: date, sessions: List[date]) -> int:
            if left == right:
                return 0
            if right > left:
                return bisect_right(sessions, right) - bisect_right(sessions, left)
            return -(bisect_right(sessions, left) - bisect_right(sessions, right))

        tdx_events: List[Dict[str, Any]] = []
        seen_tdx: Set[tuple[str, date]] = set()
        for row in tdx_rows:
            instrument_id = str(row.get('instrument_id') or '').strip()
            ex_date = _event_date(row.get('ex_date'))
            if not instrument_id or ex_date is None:
                continue
            key = (instrument_id, ex_date)
            if key in seen_tdx:
                continue
            seen_tdx.add(key)
            validation_result = str(row.get('validation_result') or '')
            comparable_factor = (
                None
                if validation_result.startswith('pending_')
                else _positive_float(row.get('factor'))
            )
            tdx_events.append({
                'instrument_id': instrument_id,
                'ex_date': ex_date,
                'factor': comparable_factor,
                'source': 'tdx_xdxr',
                'validation_result': validation_result,
                'pre_close': float(row.get('pre_close') or 0.0),
                'fenhong': float(row.get('fenhong') or 0.0),
                'songzhuangu': float(row.get('songzhuangu') or 0.0),
                'peigu': float(row.get('peigu') or 0.0),
                'peigujia': float(row.get('peigujia') or 0.0),
            })

        reference_groups: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for row in reference_rows:
            instrument_id = str(row.get('instrument_id') or '').strip()
            source = str(row.get('source') or 'unknown').lower()
            ex_date = _event_date(row.get('ex_date'))
            if not instrument_id or ex_date is None:
                continue
            reference_groups.setdefault((instrument_id, source), []).append({
                **row,
                'instrument_id': instrument_id,
                'source': source,
                'ex_date': ex_date,
            })

        reference_events: List[Dict[str, Any]] = []
        seen_reference: Set[tuple[str, str, date]] = set()
        for (instrument_id, source), rows in sorted(reference_groups.items()):
            previous_cumulative: Optional[float] = None
            for row in sorted(rows, key=lambda item: item['ex_date']):
                cumulative = _positive_float(row.get('cumulative_factor'))
                stored_factor = _positive_float(row.get('factor'))
                if cumulative is not None and previous_cumulative is not None:
                    comparable_factor = cumulative / previous_cumulative
                    if abs(comparable_factor - 1.0) <= 1e-12:
                        previous_cumulative = cumulative
                        continue
                elif stored_factor is not None and abs(stored_factor - 1.0) > 1e-12:
                    comparable_factor = stored_factor
                elif cumulative is not None and abs(cumulative - 1.0) > 1e-12:
                    # A complete history starts from a unit baseline, but old
                    # persisted BaoStock series can begin at their first event.
                    comparable_factor = cumulative
                else:
                    if cumulative is not None:
                        previous_cumulative = cumulative
                    continue
                if cumulative is not None:
                    previous_cumulative = cumulative
                ex_date = row['ex_date']
                if ex_date < start_date or ex_date > end_date:
                    continue
                key = (instrument_id, source, ex_date)
                if key in seen_reference:
                    continue
                seen_reference.add(key)
                reference_events.append({
                    'instrument_id': instrument_id,
                    'ex_date': ex_date,
                    'factor': comparable_factor,
                    'cumulative_factor': cumulative,
                    'source': source,
                })

        calendar_start = start_date - timedelta(days=14)
        calendar_end = end_date + timedelta(days=14)
        sessions_by_exchange: Dict[str, List[date]] = {}
        warnings: List[str] = []
        requested_exchanges = sorted({
            exchange
            for instrument_id in normalized_ids
            if (exchange := _exchange_for(instrument_id)) is not None
        })
        for exchange in requested_exchanges:
            records = await self.db_ops.get_trading_calendar_records(
                exchange,
                calendar_start,
                calendar_end,
            )
            sessions = sorted({
                parsed
                for record in records
                if record.get('is_trading_day')
                if (parsed := _event_date(record.get('date'))) is not None
            })
            sessions_by_exchange[exchange] = sessions
            if not sessions:
                warnings.append(
                    f'trading calendar evidence unavailable for {exchange}'
                )

        tdx_by_instrument: Dict[str, List[Dict[str, Any]]] = {}
        reference_by_instrument: Dict[str, List[Dict[str, Any]]] = {}
        for event in tdx_events:
            tdx_by_instrument.setdefault(event['instrument_id'], []).append(event)
        for event in reference_events:
            reference_by_instrument.setdefault(event['instrument_id'], []).append(event)

        exact_matches: List[Dict[str, Any]] = []
        shifted_matches: List[Dict[str, Any]] = []
        factor_conflicts: List[Dict[str, Any]] = []
        reference_only: List[Dict[str, Any]] = []
        tdx_only: List[Dict[str, Any]] = []
        calendar_unavailable_instruments: Set[str] = set()
        max_session_distance = 3
        factor_tolerance = 0.05

        for instrument_id in sorted(set(tdx_by_instrument) | set(reference_by_instrument)):
            tdx_items = sorted(
                tdx_by_instrument.get(instrument_id, []),
                key=lambda item: item['ex_date'],
            )
            reference_items = sorted(
                reference_by_instrument.get(instrument_id, []),
                key=lambda item: (item['ex_date'], item['source']),
            )
            used_tdx: Set[int] = set()
            used_reference: Set[int] = set()

            exact_candidates = []
            for tdx_idx, tdx_event in enumerate(tdx_items):
                for ref_idx, reference_event in enumerate(reference_items):
                    if tdx_event['ex_date'] != reference_event['ex_date']:
                        continue
                    difference = _factor_diff(
                        tdx_event.get('factor'), reference_event.get('factor')
                    )
                    if difference is not None and difference <= factor_tolerance:
                        exact_candidates.append((
                            difference,
                            tdx_event['ex_date'],
                            reference_event['source'],
                            tdx_idx,
                            ref_idx,
                        ))
            for difference, _, _, tdx_idx, ref_idx in sorted(exact_candidates):
                if tdx_idx in used_tdx or ref_idx in used_reference:
                    continue
                used_tdx.add(tdx_idx)
                used_reference.add(ref_idx)
                exact_matches.append({
                    'instrument_id': instrument_id,
                    'tdx_ex_date': tdx_items[tdx_idx]['ex_date'],
                    'reference_ex_date': reference_items[ref_idx]['ex_date'],
                    'tdx_factor': tdx_items[tdx_idx].get('factor'),
                    'reference_factor': reference_items[ref_idx].get('factor'),
                    'factor_diff_pct': difference * 100.0,
                    'trading_session_distance': 0,
                    'source': reference_items[ref_idx]['source'],
                    'reason': 'exact_factor_match',
                })

            exchange = _exchange_for(instrument_id)
            sessions = sessions_by_exchange.get(exchange or '', [])
            remaining_tdx = [idx for idx in range(len(tdx_items)) if idx not in used_tdx]
            remaining_reference = [
                idx for idx in range(len(reference_items))
                if idx not in used_reference
            ]
            if remaining_tdx and remaining_reference and not sessions:
                if any(
                    tdx_items[tdx_idx]['ex_date'] != reference_items[ref_idx]['ex_date']
                    for tdx_idx in remaining_tdx
                    for ref_idx in remaining_reference
                ):
                    calendar_unavailable_instruments.add(instrument_id)

            shifted_candidates = []
            if sessions:
                for tdx_idx in remaining_tdx:
                    for ref_idx in remaining_reference:
                        tdx_event = tdx_items[tdx_idx]
                        reference_event = reference_items[ref_idx]
                        if tdx_event['ex_date'] == reference_event['ex_date']:
                            continue
                        session_distance = _session_distance(
                            tdx_event['ex_date'], reference_event['ex_date'], sessions
                        )
                        if abs(session_distance) > max_session_distance:
                            continue
                        difference = _factor_diff(
                            tdx_event.get('factor'), reference_event.get('factor')
                        )
                        if difference is None or difference > factor_tolerance:
                            continue
                        shifted_candidates.append((
                            abs(session_distance),
                            difference,
                            abs((reference_event['ex_date'] - tdx_event['ex_date']).days),
                            tdx_event['ex_date'],
                            reference_event['ex_date'],
                            reference_event['source'],
                            session_distance,
                            tdx_idx,
                            ref_idx,
                        ))
            for candidate in sorted(shifted_candidates):
                *_, session_distance, tdx_idx, ref_idx = candidate
                if tdx_idx in used_tdx or ref_idx in used_reference:
                    continue
                used_tdx.add(tdx_idx)
                used_reference.add(ref_idx)
                tdx_event = tdx_items[tdx_idx]
                reference_event = reference_items[ref_idx]
                shifted_matches.append({
                    'instrument_id': instrument_id,
                    'tdx_ex_date': tdx_event['ex_date'],
                    'reference_ex_date': reference_event['ex_date'],
                    'tdx_factor': tdx_event.get('factor'),
                    'reference_factor': reference_event.get('factor'),
                    'factor_diff_pct': (
                        _factor_diff(
                            tdx_event.get('factor'), reference_event.get('factor')
                        ) or 0.0
                    ) * 100.0,
                    'trading_session_distance': session_distance,
                    'calendar_day_distance': (
                        reference_event['ex_date'] - tdx_event['ex_date']
                    ).days,
                    'source': reference_event['source'],
                    'reason': 'provider_date_shift_factor_match',
                })

            conflict_candidates = []
            for tdx_idx, tdx_event in enumerate(tdx_items):
                if tdx_idx in used_tdx:
                    continue
                for ref_idx, reference_event in enumerate(reference_items):
                    if ref_idx in used_reference:
                        continue
                    if tdx_event['ex_date'] == reference_event['ex_date']:
                        session_distance = 0
                    elif sessions:
                        session_distance = _session_distance(
                            tdx_event['ex_date'], reference_event['ex_date'], sessions
                        )
                        if abs(session_distance) > max_session_distance:
                            continue
                    else:
                        continue
                    difference = _factor_diff(
                        tdx_event.get('factor'), reference_event.get('factor')
                    )
                    if difference is None or difference <= factor_tolerance:
                        continue
                    conflict_candidates.append((
                        abs(session_distance),
                        abs((reference_event['ex_date'] - tdx_event['ex_date']).days),
                        difference,
                        tdx_event['ex_date'],
                        reference_event['ex_date'],
                        reference_event['source'],
                        session_distance,
                        tdx_idx,
                        ref_idx,
                    ))
            for candidate in sorted(conflict_candidates):
                *_, session_distance, tdx_idx, ref_idx = candidate
                if tdx_idx in used_tdx or ref_idx in used_reference:
                    continue
                used_tdx.add(tdx_idx)
                used_reference.add(ref_idx)
                tdx_event = tdx_items[tdx_idx]
                reference_event = reference_items[ref_idx]
                factor_conflicts.append({
                    'instrument_id': instrument_id,
                    'tdx_ex_date': tdx_event['ex_date'],
                    'reference_ex_date': reference_event['ex_date'],
                    'tdx_factor': tdx_event.get('factor'),
                    'reference_factor': reference_event.get('factor'),
                    'factor_diff_pct': (
                        _factor_diff(
                            tdx_event.get('factor'), reference_event.get('factor')
                        ) or 0.0
                    ) * 100.0,
                    'trading_session_distance': session_distance,
                    'source': reference_event['source'],
                    'pre_close': tdx_event.get('pre_close', 0.0),
                    'fenhong': tdx_event.get('fenhong', 0.0),
                    'songzhuangu': tdx_event.get('songzhuangu', 0.0),
                    'peigu': tdx_event.get('peigu', 0.0),
                    'peigujia': tdx_event.get('peigujia', 0.0),
                    'validation_result': tdx_event.get('validation_result'),
                    'reason': 'nearby_factor_conflict',
                })

            tdx_only.extend(
                event for idx, event in enumerate(tdx_items) if idx not in used_tdx
            )
            reference_only.extend(
                event
                for idx, event in enumerate(reference_items)
                if idx not in used_reference
            )

        bounded_limit = max(0, int(sample_limit or 0))
        source_distribution = dict(Counter(
            event['source'] for event in reference_events
        ))
        if not reference_events:
            status = 'unavailable'
            warnings.append('independent production factor evidence unavailable')
        elif calendar_unavailable_instruments:
            status = 'unavailable'
            warnings.append(
                'trading calendar evidence unavailable for shifted-date reconciliation'
            )
        elif reference_only or factor_conflicts:
            status = 'partial'
        else:
            status = 'success'

        def _serialize_match(item: Dict[str, Any]) -> Dict[str, Any]:
            return {
                **item,
                'tdx_ex_date': self._date_text(item.get('tdx_ex_date')),
                'reference_ex_date': self._date_text(item.get('reference_ex_date')),
            }

        reference_only_samples = [
            {
                'instrument_id': item['instrument_id'],
                'ex_date': self._date_text(item['ex_date']),
                'factor': item.get('factor'),
                'source': item['source'],
                'reason': 'reference_factor_change_unmatched',
            }
            for item in reference_only[:bounded_limit]
        ]
        tdx_only_samples = [
            {
                'instrument_id': item['instrument_id'],
                'ex_date': self._date_text(item['ex_date']),
                'factor': item.get('factor'),
                'pre_close': item.get('pre_close', 0.0),
                'fenhong': item.get('fenhong', 0.0),
                'songzhuangu': item.get('songzhuangu', 0.0),
                'peigu': item.get('peigu', 0.0),
                'peigujia': item.get('peigujia', 0.0),
                'validation_result': item.get('validation_result'),
                'source': 'tdx_xdxr',
                'reason': 'tdx_event_unmatched_by_reference_factor',
            }
            for item in tdx_only[:bounded_limit]
        ]

        totals = {
            'tdx_events': len(tdx_events),
            'reference_factor_changes': len(reference_events),
            'reference_events': len(reference_events),
            'exact_factor_matches': len(exact_matches),
            'shifted_factor_matches': len(shifted_matches),
            'factor_conflicts': len(factor_conflicts),
            'reference_factor_change_only': len(reference_only),
            'reference_factor_change_only_instruments': len({
                item['instrument_id'] for item in reference_only
            }),
            'tdx_event_only': len(tdx_only),
            'tdx_event_only_instruments': len({
                item['instrument_id'] for item in tdx_only
            }),
            'calendar_unavailable_instruments': len(calendar_unavailable_instruments),
            # Compatibility aliases for existing result consumers.
            'overlap_events': len(exact_matches),
            'reference_only_events': len(reference_only),
            'reference_only_instruments': len({
                item['instrument_id'] for item in reference_only
            }),
            'tdx_only_events': len(tdx_only),
            'tdx_only_instruments': len({
                item['instrument_id'] for item in tdx_only
            }),
        }
        return {
            'status': status,
            'reference_sources': sources,
            'reference_source_distribution': source_distribution,
            'matching_policy': {
                'max_trading_session_distance': max_session_distance,
                'factor_tolerance_pct': factor_tolerance * 100.0,
            },
            'totals': totals,
            'exact_match_samples': [
                _serialize_match(item) for item in exact_matches[:bounded_limit]
            ],
            'shifted_match_samples': [
                _serialize_match(item) for item in shifted_matches[:bounded_limit]
            ],
            'factor_conflict_samples': [
                _serialize_match(item) for item in factor_conflicts[:bounded_limit]
            ],
            'reference_factor_change_only_samples': reference_only_samples,
            'tdx_event_only_samples': tdx_only_samples,
            'reference_only_samples': reference_only_samples,
            'tdx_only_samples': tdx_only_samples,
            'calendar_unavailable_instrument_ids': sorted(
                calendar_unavailable_instruments
            )[:bounded_limit],
            'distributions': {
                'factor_conflicts_by_action': dict(Counter(
                    _action_bucket(item) for item in factor_conflicts
                )),
                'factor_conflicts_by_decade': dict(Counter(
                    _decade_bucket(item, 'tdx_ex_date')
                    for item in factor_conflicts
                )),
                'reference_factor_change_only_by_source': dict(Counter(
                    item['source'] for item in reference_only
                )),
                'reference_factor_change_only_by_decade': dict(Counter(
                    _decade_bucket(item, 'ex_date') for item in reference_only
                )),
                'tdx_event_only_by_action': dict(Counter(
                    _action_bucket(item) for item in tdx_only
                )),
                'tdx_event_only_by_decade': dict(Counter(
                    _decade_bucket(item, 'ex_date') for item in tdx_only
                )),
            },
            'warnings': list(dict.fromkeys(warnings)),
        }

    async def backfill_tdx_xdxr_history(
        self,
        exchanges: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        instrument_ids: Optional[List[str]] = None,
        instrument_date_ranges: Optional[Dict[str, Dict[str, Any]]] = None,
        limit: Optional[int] = None,
        derive_factors: bool = True,
        repair_universe_mode: str = 'historical_backfill',
        override_lifecycle_filter: bool = False,
        per_instrument_timeout_sec: Optional[int] = 30,
        progress_log_every: int = 200,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Backfill raw TDX XDXR history without dropping uncomputable events.

        Raw events are always isolated in ``adjustment_factors_tdx``. When a
        prior close is available, the optional derivation pass updates the same
        row with computed audit factors. Existing computed fields are preserved
        by the raw-event upsert pass.
        """
        if start_date is None:
            start_date = date(1990, 1, 1)
        if end_date is None:
            end_date = date.today()
        if end_date < start_date:
            raise ValueError(f"end_date {end_date} is earlier than start_date {start_date}")

        exchanges = [str(item).upper() for item in (exchanges or ['SSE', 'SZSE', 'BSE'])]
        exchanges = list(dict.fromkeys(exchanges))
        unsupported = sorted(set(exchanges) - {'SSE', 'SZSE', 'BSE'})
        if unsupported:
            raise ValueError(f"Unsupported A-share exchanges: {unsupported}")

        if not hasattr(self, 'source_factory') or not self.source_factory:
            from data_sources.source_factory import get_data_source_factory
            self.source_factory = await get_data_source_factory(self.db_ops)
        tdx_source = self.source_factory._find_source_by_base_name('pytdx')
        if not tdx_source or not hasattr(tdx_source, 'get_xdxr_events'):
            return {
                'status': 'failed',
                'operation': 'tdx_xdxr_history_backfill',
                'dry_run': bool(dry_run),
                'error': 'pytdx XDXR source unavailable',
                'totals': {'errors': 1},
                'by_exchange': {},
            }

        result: Dict[str, Any] = {
            'status': 'dry_run' if dry_run else 'success',
            'operation': 'tdx_xdxr_history_backfill',
            'dry_run': bool(dry_run),
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'exchanges': exchanges,
            'derive_factors': bool(derive_factors),
            'by_exchange': {},
            'totals': {
                'input_instruments': 0,
                'eligible_instruments': 0,
                'processed_instruments': 0,
                'empty_instruments': 0,
                'raw_events': 0,
                'saved_events': 0,
                'existing_events_refreshed': 0,
                'derived_factors': 0,
                'pending_factors': 0,
                'lifecycle_filtered_events': 0,
                'timeouts': 0,
                'errors': 0,
            },
            'warnings': [],
            'errors': [],
            'samples': [],
        }

        for exchange in exchanges:
            raw_instruments = await self.db_ops.get_repair_universe_instruments(
                exchange,
                instrument_types=['stock'],
            )
            instruments, universe_diag = await self.filter_repair_universe(
                raw_instruments,
                start_date=start_date,
                end_date=end_date,
                mode=repair_universe_mode,
                instrument_ids=instrument_ids,
                override_lifecycle_filter=override_lifecycle_filter,
                limit=limit,
                dry_run=dry_run,
            )
            stats = {
                'input_instruments': len(raw_instruments),
                'eligible_instruments': len(instruments),
                'processed_instruments': 0,
                'empty_instruments': 0,
                'raw_events': 0,
                'saved_events': 0,
                'existing_events_refreshed': 0,
                'derived_factors': 0,
                'pending_factors': 0,
                'lifecycle_filtered_events': 0,
                'timeouts': 0,
                'errors': 0,
                'repair_universe': universe_diag,
            }

            for idx, instrument in enumerate(instruments, start=1):
                instrument_id = instrument['instrument_id']
                symbol = instrument['symbol']
                effective_start = self._date_from_any(
                    instrument.get('_repair_start_date')
                ) or start_date
                effective_end = self._date_from_any(
                    instrument.get('_repair_end_date')
                ) or end_date
                frozen_range = (instrument_date_ranges or {}).get(instrument_id) or {}
                effective_start = self._date_from_any(
                    frozen_range.get('start_date')
                ) or effective_start
                effective_end = self._date_from_any(
                    frozen_range.get('end_date')
                ) or effective_end
                coverage_start = effective_start if frozen_range else start_date
                coverage_end = effective_end if frozen_range else end_date
                try:
                    event_coro = tdx_source.get_xdxr_events(instrument_id)
                    events = (
                        await asyncio.wait_for(event_coro, timeout=per_instrument_timeout_sec)
                        if per_instrument_timeout_sec
                        else await event_coro
                    )
                    events = list(events or [])
                    filtered_events = []
                    missing_event_dates = 0
                    for event in events:
                        event_date = self._date_from_any(event.get('date') or event.get('ex_date'))
                        if event_date is None:
                            missing_event_dates += 1
                            continue
                        if effective_start <= event_date <= effective_end:
                            filtered_events.append(event)
                    stats['lifecycle_filtered_events'] += max(
                        0, len(events) - len(filtered_events)
                    )

                    if not filtered_events:
                        if not dry_run:
                            await self.db_ops.upsert_corporate_action_instrument_status({
                                'instrument_id': instrument_id,
                                'source': 'tdx',
                                'source_profile': 'tdx_xdxr',
                                'coverage_status': (
                                    'partial_missing_fields'
                                    if missing_event_dates else 'complete_no_events'
                                ),
                                'event_count': 0,
                                'missing_ex_date_count': missing_event_dates,
                                'requested_start_date': coverage_start,
                                'requested_end_date': coverage_end,
                                'earliest_event_date': None,
                                'latest_event_date': None,
                                'ingestion_run_id': (
                                    f"tdx_xdxr_{start_date:%Y%m%d}_{end_date:%Y%m%d}"
                                ),
                            })
                        stats['empty_instruments'] += 1
                        stats['processed_instruments'] += 1
                        continue

                    raw_rows = [
                        row
                        for row in (
                            self._build_tdx_raw_event_row(instrument_id, event)
                            for event in filtered_events
                        )
                        if row is not None
                    ]
                    event_dates = {
                        self._date_from_any(row.get('ex_date'))
                        for row in raw_rows
                    }
                    existing_rows = await self.db_ops.execute_read_query(
                        """
                        SELECT ex_date, pre_close, validation_result
                        FROM adjustment_factors_tdx
                        WHERE instrument_id = :instrument_id
                        """,
                        {'instrument_id': instrument_id},
                    )
                    existing_dates = {
                        self._date_from_any(row.get('ex_date'))
                        for row in existing_rows
                    }
                    computed_dates = {
                        self._date_from_any(row.get('ex_date'))
                        for row in existing_rows
                        if float(row.get('pre_close') or 0.0) > 0
                    }

                    stats['raw_events'] += len(raw_rows)
                    stats['existing_events_refreshed'] += len(event_dates & existing_dates)
                    derived_dates = set()
                    if not dry_run:
                        stats['saved_events'] += await self.db_ops.save_tdx_audit_factors(
                            raw_rows,
                            preserve_computed_fields=True,
                        )

                    if derive_factors:
                        pre_close_overrides = await self.db_ops.get_xdxr_pre_close_overrides(
                            instrument_id,
                            [event_date for event_date in event_dates if event_date is not None],
                        )
                        dm_logger.debug(
                            "[DataManager] XDXR prior-close evidence %s: local=%d events=%d",
                            instrument_id,
                            len(pre_close_overrides),
                            len(event_dates),
                        )
                        factor_coro = tdx_source.get_adjustment_factors(
                            instrument_id,
                            symbol,
                            datetime.combine(effective_start, datetime.min.time()),
                            datetime.combine(effective_end, datetime.max.time()),
                            pre_close_overrides=pre_close_overrides,
                        )
                        factors = (
                            await asyncio.wait_for(factor_coro, timeout=per_instrument_timeout_sec)
                            if per_instrument_timeout_sec
                            else await factor_coro
                        )
                        for factor in factors or []:
                            factor.setdefault('validation_result', 'computed_unvalidated')
                            factor.setdefault('source', 'tdx_xdxr')
                            derived_date = self._date_from_any(factor.get('ex_date'))
                            if derived_date is not None:
                                derived_dates.add(derived_date)
                        if factors:
                            if dry_run:
                                stats['derived_factors'] += len(factors)
                            else:
                                stats['derived_factors'] += await self.db_ops.save_tdx_audit_factors(
                                    factors
                                )

                    stats['pending_factors'] += len(
                        event_dates - computed_dates - derived_dates
                    )
                    if not dry_run:
                        valid_event_dates = sorted(
                            event_date
                            for event_date in event_dates
                            if event_date is not None
                        )
                        await self.db_ops.upsert_corporate_action_instrument_status({
                            'instrument_id': instrument_id,
                            'source': 'tdx',
                            'source_profile': 'tdx_xdxr',
                            'coverage_status': (
                                'partial_missing_fields'
                                if missing_event_dates else 'complete_with_events'
                            ),
                            'event_count': len(raw_rows),
                            'missing_ex_date_count': missing_event_dates,
                            'requested_start_date': coverage_start,
                            'requested_end_date': coverage_end,
                            'earliest_event_date': (
                                valid_event_dates[0] if valid_event_dates else None
                            ),
                            'latest_event_date': (
                                valid_event_dates[-1] if valid_event_dates else None
                            ),
                            'ingestion_run_id': (
                                f"tdx_xdxr_{start_date:%Y%m%d}_{end_date:%Y%m%d}"
                            ),
                        })
                    stats['processed_instruments'] += 1
                except asyncio.TimeoutError:
                    stats['timeouts'] += 1
                    result['samples'].append({
                        'instrument_id': instrument_id,
                        'exchange': exchange,
                        'reason': 'timeout',
                    })
                except Exception as exc:
                    stats['errors'] += 1
                    result['errors'].append(f"{instrument_id}: {exc}")
                    if len(result['samples']) < 20:
                        result['samples'].append({
                            'instrument_id': instrument_id,
                            'exchange': exchange,
                            'reason': str(exc),
                        })

                if progress_log_every and idx % progress_log_every == 0:
                    dm_logger.info(
                        "[DataManager] XDXR history progress %s: %d/%d events=%d errors=%d timeouts=%d",
                        exchange,
                        idx,
                        len(instruments),
                        stats['raw_events'],
                        stats['errors'],
                        stats['timeouts'],
                    )

            result['by_exchange'][exchange] = stats
            for key in result['totals']:
                result['totals'][key] += int(stats.get(key, 0) or 0)

        result['samples'] = result['samples'][:20]
        result['errors'] = result['errors'][:50]
        if result['totals']['errors'] or result['totals']['timeouts']:
            result['status'] = 'partial'
        return result

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
                'changelog_stats': {
                    'inserted': 0,
                    'changed': 0,
                    'unchanged': 0,
                    'skipped': 0,
                    'failed': 0,
                    'changelog_written': 0,
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
                        'changelog_stats': {
                            'inserted': 0,
                            'changed': 0,
                            'unchanged': 0,
                            'skipped': 0,
                            'failed': 0,
                            'changelog_written': 0,
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
                                    write_stats = await self.db_ops.save_daily_quotes(
                                        data,
                                        return_stats=True,
                                    )
                                    if not isinstance(write_stats, dict):
                                        write_stats = {}
                                    for stat_key in (
                                        'inserted', 'changed', 'unchanged',
                                        'skipped', 'failed', 'changelog_written',
                                    ):
                                        stat_value = int(write_stats.get(stat_key, 0) or 0)
                                        exchange_result['changelog_stats'][stat_key] += stat_value
                                        update_results['changelog_stats'][stat_key] += stat_value
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
        instrument_ids: Optional[List[str]] = None,
        instrument_date_ranges: Optional[Dict[str, Dict[str, Any]]] = None,
        sync_adjustment_factors: bool = True,
        factor_sync_reason: str = 'daily',
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
                    instrument_ids=instrument_ids,
                    override_lifecycle_filter=override_lifecycle_filter,
                    limit=repair_universe_limit,
                )
                for key in ('input_instrument_count', 'eligible_instrument_count',
                            'clipped_instrument_count', 'skipped_instrument_count',
                            'skipped_gap_segment_count', 'skipped_missing_days',
                            'degraded_fallback_count'):
                    update_results['repair_universe'][key] += int(repair_diag.get(key, 0) or 0)
                merged_counter = Counter(update_results['repair_universe'].get('reason_distribution') or {})
                merged_counter.update(repair_diag.get('reason_distribution') or {})
                update_results['repair_universe']['reason_distribution'] = dict(merged_counter)
                merged_clip_counter = Counter(
                    update_results['repair_universe'].get('clip_reason_distribution') or {}
                )
                merged_clip_counter.update(repair_diag.get('clip_reason_distribution') or {})
                update_results['repair_universe']['clip_reason_distribution'] = dict(merged_clip_counter)
                sample_limit = int(
                    self._get_repair_universe_governance_config().get('sample_limit', 10) or 0
                )
                if sample_limit > 0:
                    update_results['repair_universe']['samples'].extend(
                        repair_diag.get('samples', [])[
                            : max(0, sample_limit - len(update_results['repair_universe'].get('samples') or []))
                        ]
                    )
                    update_results['repair_universe']['clip_samples'].extend(
                        repair_diag.get('clip_samples', [])[
                            : max(0, sample_limit - len(update_results['repair_universe'].get('clip_samples') or []))
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
                        frozen_range = (instrument_date_ranges or {}).get(
                            instrument.get('instrument_id')
                        ) or {}
                        repair_start = _to_date(
                            frozen_range.get('start_date')
                        ) or repair_start
                        repair_end = _to_date(
                            frozen_range.get('end_date')
                        ) or repair_end
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

                if sync_adjustment_factors and stocks_needing_factors:
                    factor_result = await self._batch_sync_adjustment_factors(
                        exchange,
                        stocks_needing_factors,
                        skip_filter=factor_sync_reason != 'daily',
                        sync_reason=factor_sync_reason,
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
