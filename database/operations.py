"""
database operations for the quote system.
Supports comprehensive data management with new schema.
"""

import asyncio
import hashlib
import json
import math
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from sqlalchemy import (
    text, func, desc, asc, tuple_, literal, union_all, delete, case, and_, or_,
)
from sqlalchemy.orm import sessionmaker
from utils.date_utils import get_shanghai_time
from utils import db_logger, config_manager


# 异步查询需要 select
from sqlalchemy.future import select
from .connection import db_manager
from .models import (
    InstrumentDB, DailyQuoteDB, TradingCalendarDB, TradingSessionDB,
    DataUpdateDB, DataSourceStatusDB, AdjustmentFactorDB, AdjustmentFactorTdxDB,
    AdjustmentFactorObservationDB, AdjustmentFactorCanonicalDB,
    AdjustmentFactorSeriesStatusDB, AdjustmentFactorInstrumentStatusDB,
    CorporateActionObservationDB, CorporateActionInstrumentStatusDB,
    CorporateActionEffectiveDateEvidenceDB,
    CorporateActionDocumentArtifactDB, CorporateActionDocumentPageDB,
    CorporateActionLlmAnalysisDB, CorporateActionResolutionReviewDB,
    CorporateActionResolvedTermsDB, CorporateActionResolutionStateDB,
    DataChangeLogDB,
)


GOVERNED_CORPORATE_ACTION_EFFECTIVE_DATE_EVIDENCE_SOURCES = (
    "cninfo_reviewed_official_document",
    "cninfo_announcement_review",
    "cninfo_announcement",
    "cninfo_tdx_xdxr_review",
    "cninfo_tdx_xdxr_operator_review",
    "cninfo_operator_attestation",
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
                self.AsyncSessionLocal = self.db.TaskAsyncSessionLocal
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
            self.AsyncSessionLocal = self.db.TaskAsyncSessionLocal

            self.db_logger.info("DatabaseOperations initialized successfully")
        except Exception as e:
            self.db_logger.error(f"Failed to initialize DatabaseOperations: {e}")
            raise

    def get_async_session(self):
        """Get async database session"""
        return self.db.get_async_session()

    def get_session(self):
        """Get synchronous database session"""
        return self.SessionLocal()

    @staticmethod
    def _coerce_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if isinstance(value, date):
            return datetime(value.year, value.month, value.day)
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        if hasattr(parsed, "to_pydatetime"):
            parsed = parsed.to_pydatetime()
        return parsed.replace(tzinfo=None) if isinstance(parsed, datetime) else None

    @staticmethod
    def _canonical_hash_value(value: Any) -> Any:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                return None
            try:
                return format(Decimal(str(value)).normalize(), "f")
            except InvalidOperation:
                return str(value)
        if isinstance(value, Decimal):
            return format(value.normalize(), "f")
        if isinstance(value, dict):
            return {
                str(k): DatabaseOperations._canonical_hash_value(v)
                for k, v in sorted(value.items())
            }
        if isinstance(value, (list, tuple)):
            return [DatabaseOperations._canonical_hash_value(v) for v in value]
        return str(value)

    @classmethod
    def _semantic_hash(cls, values: Dict[str, Any]) -> str:
        payload = {
            key: cls._canonical_hash_value(values.get(key))
            for key in sorted(values.keys())
        }
        encoded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @classmethod
    def _daily_quote_payload(cls, quote: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "time": cls._coerce_datetime(quote.get("time") or quote.get("date")),
            "instrument_id": quote.get("instrument_id"),
            "open": float(quote.get("open", 0.0)),
            "high": float(quote.get("high", 0.0)),
            "low": float(quote.get("low", 0.0)),
            "close": float(quote.get("close", 0.0)),
            "volume": int(quote.get("volume", 0) or 0),
            "amount": float(quote.get("amount", 0.0) or 0.0),
            "turnover": cls._optional_float(quote.get("turnover")),
            "pre_close": cls._optional_float(quote.get("pre_close")),
            "change": cls._optional_float(quote.get("change")),
            "pct_change": cls._optional_float(quote.get("pct_change")),
            "tradestatus": int(quote.get("tradestatus") if quote.get("tradestatus") is not None else 1),
            "factor": float(quote.get("factor", 1.0) or 1.0),
            "adjustment_type": quote.get("adjustment_type", "none"),
            "is_complete": bool(quote.get("is_complete", True)),
            "quality_score": cls._optional_float(quote.get("quality_score", 1.0)),
            "source": quote.get("source"),
            "batch_id": quote.get("batch_id"),
        }

    @classmethod
    def _daily_quote_hash(cls, payload: Dict[str, Any]) -> str:
        hash_fields = {
            key: payload.get(key)
            for key in (
                "instrument_id", "time", "open", "high", "low", "close",
                "volume", "amount", "turnover", "pre_close", "change",
                "pct_change", "tradestatus", "factor", "adjustment_type",
                "is_complete", "quality_score", "source",
            )
        }
        return cls._semantic_hash(hash_fields)

    @classmethod
    def _adjustment_factor_payload(cls, factor: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "instrument_id": factor.get("instrument_id"),
            "ex_date": cls._coerce_datetime(factor.get("ex_date")),
            "factor": float(factor.get("factor", 1.0) or 1.0),
            "cumulative_factor": float(factor.get("cumulative_factor", 1.0) or 1.0),
            "dividend": float(factor.get("dividend", 0.0) or 0.0),
            "bonus_shares": float(factor.get("bonus_shares", 0.0) or 0.0),
            "rights_shares": float(factor.get("rights_shares", 0.0) or 0.0),
            "rights_price": float(factor.get("rights_price", 0.0) or 0.0),
            "event_type": factor.get("event_type"),
            "source": factor.get("source"),
        }

    @classmethod
    def _adjustment_factor_hash(cls, payload: Dict[str, Any]) -> str:
        return cls._semantic_hash(payload)

    @staticmethod
    def _optional_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        return float(value)

    @staticmethod
    def _empty_write_stats() -> Dict[str, int]:
        return {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "skipped": 0,
            "failed": 0,
            "changelog_written": 0,
        }

    def _change_watermark_config(self) -> Dict[str, Any]:
        """Return sanitized change watermark config with conservative defaults."""
        cfg = self.config_manager.get_nested("database_config.change_watermark", {}) or {}
        return cfg if isinstance(cfg, dict) else {}

    def _change_watermark_int_config(self, key: str, default: int) -> int:
        cfg = self._change_watermark_config()
        try:
            value = int(cfg.get(key, default) or default)
        except (TypeError, ValueError):
            self.db_logger.warning(
                "Invalid change watermark config %s=%r, using default %s",
                key,
                cfg.get(key),
                default,
            )
            value = default
        return max(1, value)

    def _change_watermark_enabled_map(
        self,
        key: str,
        defaults: Dict[str, bool],
    ) -> Dict[str, bool]:
        cfg = self._change_watermark_config()
        configured = cfg.get(key) or {}
        if not isinstance(configured, dict):
            configured = {}
        return {
            name: bool(configured.get(name, enabled))
            for name, enabled in defaults.items()
        }

    def _is_changelog_enabled(self, domain: str, dataset: str) -> bool:
        """Return whether one domain/dataset should emit persistent changelog rows."""
        cfg = self._change_watermark_config()
        if cfg.get("enabled", True) is False:
            return False
        domains = cfg.get("domains") or {}
        datasets = cfg.get("datasets") or {}
        domains = domains if isinstance(domains, dict) else {}
        datasets = datasets if isinstance(datasets, dict) else {}
        if domain in domains and not bool(domains[domain]):
            return False
        if dataset in datasets and not bool(datasets[dataset]):
            return False
        return True

    @staticmethod
    def _change_log_record(
        *,
        domain: str,
        dataset: str,
        change_type: str,
        business_key: Dict[str, Any],
        instrument_id: Optional[str] = None,
        series_id: Optional[str] = None,
        observation_date: Optional[datetime] = None,
        period: Optional[str] = None,
        old_hash: Optional[str] = None,
        new_hash: Optional[str] = None,
        row_version: Optional[int] = None,
        source: Optional[str] = None,
        source_mode: Optional[str] = None,
        source_profile: Optional[str] = None,
        ingestion_run_id: Optional[str] = None,
        batch_id: Optional[str] = None,
    ) -> DataChangeLogDB:
        return DataChangeLogDB(
            domain=domain,
            dataset=dataset,
            change_type=change_type,
            business_key_json=json.dumps(
                business_key, ensure_ascii=False, sort_keys=True, default=str
            ),
            instrument_id=instrument_id,
            series_id=series_id,
            observation_date=observation_date,
            period=period,
            old_hash=old_hash,
            new_hash=new_hash,
            row_version=row_version,
            source=source,
            source_mode=source_mode,
            source_profile=source_profile,
            ingestion_run_id=ingestion_run_id,
            batch_id=batch_id,
            changed_at=get_shanghai_time(),
        )

    @staticmethod
    def _serialize_instrument_row(instrument: InstrumentDB) -> Dict[str, Any]:
        """Convert one instrument ORM row into the shared dict payload."""
        return {
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
            'data_version': instrument.data_version,
        }

    # === Instrument Operations ===

    async def get_instruments_by_exchange(self, exchange: str, is_active: bool = True) -> List[Dict[str, Any]]:
        """根据交易所获取交易品种列表"""
        try:
            async with self.get_async_session() as session:
                stmt = select(InstrumentDB).filter(InstrumentDB.exchange == exchange)

                if is_active is not None:
                    stmt = stmt.filter(InstrumentDB.is_active == is_active)

                stmt = stmt.order_by(InstrumentDB.symbol)
                result = await session.execute(stmt)
                instruments_db = result.scalars().all()

                return [
                    self._serialize_instrument_row(instrument)
                    for instrument in instruments_db
                ]

        except Exception as e:
            self.db_logger.error(f"Failed to get instruments by exchange {exchange}: {e}")
            return []

    @staticmethod
    def _is_research_target_instrument_type(instrument_type: Optional[str]) -> bool:
        """Return whether one instrument type belongs to the research stock universe."""
        if instrument_type is None:
            return True
        return str(instrument_type).upper() == "STOCK"

    def get_research_target_instrument_ids_by_exchange_sync(
        self,
        exchange: str,
        *,
        is_active: bool = True,
    ) -> List[str]:
        """Return research target instrument ids for one exchange via a lightweight sync read."""
        try:
            with self.get_session() as session:
                stmt = select(
                    InstrumentDB.instrument_id,
                    InstrumentDB.type,
                ).filter(InstrumentDB.exchange == exchange)

                if is_active is not None:
                    stmt = stmt.filter(InstrumentDB.is_active == is_active)

                rows = session.execute(stmt).all()

            return sorted(
                {
                    str(instrument_id).strip()
                    for instrument_id, instrument_type in rows
                    if instrument_id
                    and self._is_research_target_instrument_type(instrument_type)
                }
            )
        except Exception as e:
            self.db_logger.error(
                f"Failed to get research target instrument ids for {exchange}: {e}"
            )
            return []

    async def get_research_target_instrument_ids_by_exchange(
        self,
        exchange: str,
        *,
        is_active: bool = True,
    ) -> List[str]:
        """Return research target instrument ids for one exchange."""
        return self.get_research_target_instrument_ids_by_exchange_sync(
            exchange,
            is_active=is_active,
        )

    def get_research_target_instruments_by_exchange_sync(
        self,
        exchange: str,
        *,
        is_active: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return research target stock instruments via a lightweight sync read."""
        try:
            with self.get_session() as session:
                stmt = select(InstrumentDB).filter(InstrumentDB.exchange == exchange)

                if is_active is not None:
                    stmt = stmt.filter(InstrumentDB.is_active == is_active)

                stmt = stmt.order_by(InstrumentDB.symbol)
                rows = session.execute(stmt).scalars().all()

            return [
                self._serialize_instrument_row(instrument)
                for instrument in rows
                if self._is_research_target_instrument_type(instrument.type)
            ]
        except Exception as e:
            self.db_logger.error(
                f"Failed to get research target instruments for {exchange}: {e}"
            )
            return []

    async def get_research_target_instruments_by_exchange(
        self,
        exchange: str,
        *,
        is_active: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return research target stock instruments for one exchange."""
        return self.get_research_target_instruments_by_exchange_sync(
            exchange,
            is_active=is_active,
        )

    async def get_active_instruments(
        self,
        exchange: str = None,
        instrument_types: Optional[List[str]] = None,
        tradable_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """获取活跃交易品种列表"""
        try:
            async with self.get_async_session() as session:
                stmt = select(InstrumentDB).filter(InstrumentDB.is_active == True)

                if exchange:
                    stmt = stmt.filter(InstrumentDB.exchange == exchange)
                    
                if instrument_types:
                    stmt = stmt.filter(InstrumentDB.type.in_(instrument_types))

                if tradable_only:
                    stmt = stmt.filter(InstrumentDB.trading_status == 1)

                stmt = stmt.order_by(InstrumentDB.exchange, InstrumentDB.symbol)
                result = await session.execute(stmt)
                instruments_db = result.scalars().all()

                instruments = []
                for instrument in instruments_db:
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
                        'lot_size': instrument.lot_size,
                        'tick_size': instrument.tick_size,
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

    async def get_repair_universe_instruments(
        self,
        exchange: str = None,
        instrument_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Return local instruments for historical repair/backfill governance.

        Unlike ``get_active_instruments()``, this intentionally does not filter
        on ``is_active``. The repair-universe layer needs inactive rows so it
        can produce lifecycle skip diagnostics and still allow bounded
        pre-delisting historical repair where policy permits it.
        """
        try:
            async with self.get_async_session() as session:
                stmt = select(InstrumentDB)

                if exchange:
                    stmt = stmt.filter(InstrumentDB.exchange == exchange)

                if instrument_types:
                    stmt = stmt.filter(InstrumentDB.type.in_(instrument_types))

                stmt = stmt.order_by(InstrumentDB.exchange, InstrumentDB.symbol)
                result = await session.execute(stmt)
                return [
                    self._serialize_instrument_row(instrument)
                    for instrument in result.scalars().all()
                ]

        except Exception as e:
            self.db_logger.error(f"Failed to get repair universe instruments: {e}")
            return []

    @staticmethod
    def _normalize_sequence_filter(values: Optional[List[Any]]) -> List[str]:
        return [str(item).strip() for item in (values or []) if str(item).strip()]

    @staticmethod
    def _coerce_date_value(value: Any) -> Optional[date]:
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
    def _chunked_values(values: List[str], size: int = 800) -> List[List[str]]:
        return [values[idx: idx + size] for idx in range(0, len(values), size)]

    async def get_delisted_a_share_quote_backfill_candidates(
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
    ) -> List[Dict[str, Any]]:
        """List delisted A-share stocks that need historical quote backfill."""
        def _to_datetime(value: Optional[Union[str, date, datetime]], end: bool = False) -> Optional[datetime]:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime.combine(value, datetime.max.time() if end else datetime.min.time())
            parsed = datetime.fromisoformat(str(value)[:10])
            return datetime.combine(parsed.date(), datetime.max.time() if end else datetime.min.time())

        try:
            normalized_exchanges = self._normalize_sequence_filter(exchanges) or ['SSE', 'SZSE', 'BSE']
            normalized_ids = self._normalize_sequence_filter(instrument_ids)
            start_dt = _to_datetime(delisted_start_date)
            end_dt = _to_datetime(delisted_end_date, end=True)

            async with self.get_async_session() as session:
                stmt = (
                    select(
                        InstrumentDB.instrument_id,
                        InstrumentDB.symbol,
                        InstrumentDB.name,
                        InstrumentDB.exchange,
                        InstrumentDB.listed_date,
                        InstrumentDB.delisted_date,
                        InstrumentDB.status,
                        InstrumentDB.is_active,
                        InstrumentDB.trading_status,
                        InstrumentDB.source_symbol,
                    )
                    .filter(
                        InstrumentDB.type == 'stock',
                        InstrumentDB.exchange.in_(normalized_exchanges),
                        InstrumentDB.listed_date.isnot(None),
                        InstrumentDB.delisted_date.isnot(None),
                    )
                )

                if delisted_year_start is not None:
                    stmt = stmt.filter(func.strftime('%Y', InstrumentDB.delisted_date) >= str(int(delisted_year_start)))
                if delisted_year_end is not None:
                    stmt = stmt.filter(func.strftime('%Y', InstrumentDB.delisted_date) <= str(int(delisted_year_end)))
                if start_dt is not None:
                    stmt = stmt.filter(InstrumentDB.delisted_date >= start_dt)
                if end_dt is not None:
                    stmt = stmt.filter(InstrumentDB.delisted_date <= end_dt)
                if normalized_ids:
                    stmt = stmt.filter(InstrumentDB.instrument_id.in_(normalized_ids))

                stmt = stmt.order_by(InstrumentDB.delisted_date, InstrumentDB.exchange, InstrumentDB.symbol)
                result = await session.execute(stmt)
                instrument_rows = [dict(row) for row in result.mappings().all()]
                instrument_ids_to_check = [row['instrument_id'] for row in instrument_rows if row.get('instrument_id')]
                coverage_by_id: Dict[str, Dict[str, Any]] = {}
                for chunk in self._chunked_values(instrument_ids_to_check):
                    coverage_stmt = (
                        select(
                            DailyQuoteDB.instrument_id.label('instrument_id'),
                            func.count(DailyQuoteDB.instrument_id).label('quote_rows'),
                            func.min(DailyQuoteDB.time).label('first_quote_date'),
                            func.max(DailyQuoteDB.time).label('last_quote_date'),
                        )
                        .filter(DailyQuoteDB.instrument_id.in_(chunk))
                        .group_by(DailyQuoteDB.instrument_id)
                    )
                    coverage_result = await session.execute(coverage_stmt)
                    for row in coverage_result.mappings().all():
                        coverage_by_id[row['instrument_id']] = dict(row)

                rows: List[Dict[str, Any]] = []
                for item in instrument_rows:
                    coverage = coverage_by_id.get(item.get('instrument_id'), {})
                    quote_rows = int(coverage.get('quote_rows') or 0)
                    first_quote_date = coverage.get('first_quote_date')
                    last_quote_date = coverage.get('last_quote_date')
                    listed = self._coerce_date_value(item.get('listed_date'))
                    delisted = self._coerce_date_value(item.get('delisted_date'))
                    first_quote = self._coerce_date_value(first_quote_date)
                    last_quote = self._coerce_date_value(last_quote_date)
                    if quote_rows <= 0:
                        coverage_status = 'missing'
                    elif listed and delisted and first_quote and last_quote and first_quote <= listed and last_quote >= delisted:
                        coverage_status = 'covered'
                    else:
                        coverage_status = 'partial'

                    if not include_already_covered and coverage_status == 'covered':
                        continue

                    item.update({
                        'quote_rows': quote_rows,
                        'first_quote_date': first_quote_date,
                        'last_quote_date': last_quote_date,
                        'coverage_status': coverage_status,
                    })
                    rows.append(item)
                    if limit is not None and int(limit) > 0 and len(rows) >= int(limit):
                        break
                return rows
        except Exception as e:
            self.db_logger.error("Failed to list delisted A-share quote backfill candidates: %s", e)
            return []

    async def get_delisted_a_share_quote_coverage_by_year(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        delisted_year_start: Optional[int] = None,
        delisted_year_end: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Summarize local quote coverage for delisted A-share stocks by delisted year."""
        try:
            normalized_exchanges = self._normalize_sequence_filter(exchanges) or ['SSE', 'SZSE', 'BSE']
            async with self.get_async_session() as session:
                year = func.strftime('%Y', InstrumentDB.delisted_date)
                stmt = (
                    select(
                        InstrumentDB.instrument_id,
                        InstrumentDB.listed_date,
                        InstrumentDB.delisted_date,
                        year.label('delisted_year'),
                    )
                    .filter(
                        InstrumentDB.type == 'stock',
                        InstrumentDB.exchange.in_(normalized_exchanges),
                        InstrumentDB.listed_date.isnot(None),
                        InstrumentDB.delisted_date.isnot(None),
                    )
                    .order_by(year)
                )
                if delisted_year_start is not None:
                    stmt = stmt.filter(year >= str(int(delisted_year_start)))
                if delisted_year_end is not None:
                    stmt = stmt.filter(year <= str(int(delisted_year_end)))
                stmt = stmt.order_by(year, InstrumentDB.instrument_id)
                result = await session.execute(stmt)
                instrument_rows = [dict(row) for row in result.mappings().all()]
                instrument_ids_to_check = [row['instrument_id'] for row in instrument_rows if row.get('instrument_id')]
                coverage_by_id: Dict[str, Dict[str, Any]] = {}
                for chunk in self._chunked_values(instrument_ids_to_check):
                    coverage_stmt = (
                        select(
                            DailyQuoteDB.instrument_id.label('instrument_id'),
                            func.count(DailyQuoteDB.instrument_id).label('quote_rows'),
                            func.min(DailyQuoteDB.time).label('first_quote_date'),
                            func.max(DailyQuoteDB.time).label('last_quote_date'),
                        )
                        .filter(DailyQuoteDB.instrument_id.in_(chunk))
                        .group_by(DailyQuoteDB.instrument_id)
                    )
                    coverage_result = await session.execute(coverage_stmt)
                    for row in coverage_result.mappings().all():
                        coverage_by_id[row['instrument_id']] = dict(row)

                grouped: Dict[str, Dict[str, Any]] = {}
                for item in instrument_rows:
                    delisted_year = str(item.get('delisted_year') or '')
                    if not delisted_year:
                        continue
                    bucket = grouped.setdefault(delisted_year, {
                        'delisted_year': delisted_year,
                        'instrument_count': 0,
                        'with_quotes_count': 0,
                        'no_quotes_count': 0,
                        'covered_count': 0,
                        'first_quote_date': None,
                        'last_quote_date': None,
                    })
                    bucket['instrument_count'] += 1
                    coverage = coverage_by_id.get(item.get('instrument_id'), {})
                    quote_rows = int(coverage.get('quote_rows') or 0)
                    first_quote = self._coerce_date_value(coverage.get('first_quote_date'))
                    last_quote = self._coerce_date_value(coverage.get('last_quote_date'))
                    listed = self._coerce_date_value(item.get('listed_date'))
                    delisted = self._coerce_date_value(item.get('delisted_date'))

                    if quote_rows > 0:
                        bucket['with_quotes_count'] += 1
                        raw_first = coverage.get('first_quote_date')
                        raw_last = coverage.get('last_quote_date')
                        if raw_first is not None and (
                            bucket['first_quote_date'] is None or raw_first < bucket['first_quote_date']
                        ):
                            bucket['first_quote_date'] = raw_first
                        if raw_last is not None and (
                            bucket['last_quote_date'] is None or raw_last > bucket['last_quote_date']
                        ):
                            bucket['last_quote_date'] = raw_last
                    else:
                        bucket['no_quotes_count'] += 1

                    if quote_rows > 0 and listed and delisted and first_quote and last_quote and first_quote <= listed and last_quote >= delisted:
                        bucket['covered_count'] += 1

                rows = []
                for delisted_year in sorted(grouped):
                    item = grouped[delisted_year]
                    item['uncovered_count'] = item['instrument_count'] - item['covered_count']
                    rows.append(item)
                return rows
        except Exception as e:
            self.db_logger.error("Failed to summarize delisted A-share quote coverage: %s", e)
            return []

    async def get_existing_data_dates(self, instrument_id: str, start_date: date, end_date: date) -> List[date]:
        """获取指定品种的已有数据日期"""
        try:
            query_start = (
                datetime.combine(start_date, datetime.min.time())
                if isinstance(start_date, date) and not isinstance(start_date, datetime)
                else start_date
            )
            query_end = (
                datetime.combine(end_date, datetime.max.time())
                if isinstance(end_date, date) and not isinstance(end_date, datetime)
                else end_date
            )
            async with self.get_async_session() as session:
                stmt = select(DailyQuoteDB.time).filter(
                    DailyQuoteDB.instrument_id == instrument_id,
                    DailyQuoteDB.time >= query_start,
                    DailyQuoteDB.time <= query_end
                ).order_by(DailyQuoteDB.time)

                result = await session.execute(stmt)
                dates = []
                for row in result.scalars().all():
                    if isinstance(row, datetime):
                        dates.append(row.date())
                    else:
                        dates.append(row)

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
        delisted_after: date = None,
        delisted_before: date = None,
        limit: int = 100,
        offset: int = 0,
        sort_by: str = "symbol",
        sort_order: str = "asc"
    ) -> List[Dict[str, Any]]:
        """根据过滤条件获取交易品种列表"""
        try:
            async with self.get_async_session() as session:
                stmt = select(InstrumentDB)

                # 应用过滤器
                if exchange:
                    stmt = stmt.filter(InstrumentDB.exchange == exchange)
                if instrument_type:
                    # type 存储为小写 (stock/index/...); 归一化大小写避免静默返回 0 条 (A2)
                    stmt = stmt.filter(InstrumentDB.type == instrument_type.lower())
                if industry:
                    stmt = stmt.filter(InstrumentDB.industry == industry)
                if sector:
                    stmt = stmt.filter(InstrumentDB.sector == sector)
                if market:
                    stmt = stmt.filter(InstrumentDB.market == market)
                if status:
                    stmt = stmt.filter(InstrumentDB.status == status)
                if is_active is not None:
                    stmt = stmt.filter(InstrumentDB.is_active == is_active)
                if is_st is not None:
                    stmt = stmt.filter(InstrumentDB.is_st == is_st)
                if trading_status is not None:
                    stmt = stmt.filter(InstrumentDB.trading_status == trading_status)
                if listed_after:
                    stmt = stmt.filter(InstrumentDB.listed_date >= listed_after)
                if listed_before:
                    stmt = stmt.filter(InstrumentDB.listed_date <= listed_before)
                if delisted_after:
                    stmt = stmt.filter(InstrumentDB.delisted_date >= delisted_after)
                if delisted_before:
                    stmt = stmt.filter(InstrumentDB.delisted_date <= delisted_before)

                # 排序
                if hasattr(InstrumentDB, sort_by):
                    sort_column = getattr(InstrumentDB, sort_by)
                    if sort_order.lower() == 'desc':
                        stmt = stmt.order_by(desc(sort_column))
                    else:
                        stmt = stmt.order_by(asc(sort_column))

                # 分页
                stmt = stmt.limit(limit).offset(offset)
                result = await session.execute(stmt)
                instruments_db = result.scalars().all()

                instruments = []
                for instrument in instruments_db:
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
                        'lot_size': instrument.lot_size,
                        'tick_size': instrument.tick_size,
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

    async def count_quotes_by_instrument(self, instrument_id: str, start_date: date = None,
                                         end_date: date = None) -> int:
        """统计指定股票的数据记录数"""
        try:
            async with self.get_async_session() as session:
                stmt = select(func.count()).select_from(DailyQuoteDB).filter(
                    DailyQuoteDB.instrument_id == instrument_id
                )

                if start_date:
                    stmt = stmt.filter(DailyQuoteDB.time >= start_date)
                if end_date:
                    stmt = stmt.filter(DailyQuoteDB.time <= end_date)

                return await session.scalar(stmt)

        except Exception as e:
            self.db_logger.error(f"Failed to count quotes for {instrument_id}: {e}")
            return 0

    async def get_daily_coverage(
        self,
        as_of: date,
        exchange: Optional[str] = None,
        instrument_type: str = "stock",
    ) -> Dict[str, Any]:
        """给定日期, 返回当日上市证券数与库内有行情证券数 (REQ-01.3)

        listed_count: 当日已上市且未退市的证券数 (listed_date<=d 且 delisted_date 为空或>=d)。
        quoted_count: 当日在 daily_quotes 有行情行、且 listed_date 已知的不同证券数 —— 与
            listed_count 统一口径 (均要求 listed_date 非空), 避免 listed_date 大面积缺失的市场
            (如港股) 把 coverage_ratio 拉到 >1 (A6)。
        unknown_listed_date_quoted_count: 当日有行情但 listed_date 未知的证券数, 不计入主比值,
            但透明暴露, 不隐藏信息。
        """
        try:
            from sqlalchemy import or_, and_
            day_str = as_of.isoformat()
            async with self.get_async_session() as session:
                listed_stmt = select(func.count()).select_from(InstrumentDB).filter(
                    InstrumentDB.listed_date <= as_of,
                    or_(
                        InstrumentDB.delisted_date.is_(None),
                        InstrumentDB.delisted_date >= as_of,
                    ),
                )
                if instrument_type:
                    listed_stmt = listed_stmt.filter(InstrumentDB.type == instrument_type)
                if exchange:
                    listed_stmt = listed_stmt.filter(InstrumentDB.exchange == exchange)
                listed_count = await session.scalar(listed_stmt) or 0

                def _quoted_count_stmt(*, require_known_listed_date: bool):
                    stmt = select(
                        func.count(func.distinct(DailyQuoteDB.instrument_id))
                    ).select_from(DailyQuoteDB).filter(
                        func.date(DailyQuoteDB.time) == day_str
                    )
                    stmt = stmt.join(
                        InstrumentDB,
                        InstrumentDB.instrument_id == DailyQuoteDB.instrument_id,
                    )
                    if instrument_type:
                        stmt = stmt.filter(InstrumentDB.type == instrument_type)
                    if exchange:
                        stmt = stmt.filter(InstrumentDB.exchange == exchange)
                    if require_known_listed_date:
                        stmt = stmt.filter(InstrumentDB.listed_date.isnot(None))
                    else:
                        stmt = stmt.filter(InstrumentDB.listed_date.is_(None))
                    return stmt

                quoted_count = await session.scalar(
                    _quoted_count_stmt(require_known_listed_date=True)
                ) or 0
                unknown_listed_date_quoted_count = await session.scalar(
                    _quoted_count_stmt(require_known_listed_date=False)
                ) or 0

                ratio = (quoted_count / listed_count) if listed_count else None
                return {
                    "date": day_str,
                    "exchange": exchange,
                    "instrument_type": instrument_type,
                    "listed_count": int(listed_count),
                    "quoted_count": int(quoted_count),
                    "unknown_listed_date_quoted_count": int(unknown_listed_date_quoted_count),
                    "coverage_ratio": ratio,
                }
        except Exception as e:
            self.db_logger.error(f"Failed to compute daily coverage for {as_of}: {e}")
            return {
                "date": as_of.isoformat(),
                "exchange": exchange,
                "instrument_type": instrument_type,
                "listed_count": 0,
                "quoted_count": 0,
                "unknown_listed_date_quoted_count": 0,
                "coverage_ratio": None,
            }

    async def get_instrument_date_range(self, instrument_id: str, start_date: date = None,
                                        end_date: date = None) -> Dict[str, Any]:
        """获取指定股票的数据日期范围"""
        try:
            async with self.get_async_session() as session:
                stmt = select(
                    func.min(DailyQuoteDB.time).label('min_date'),
                    func.max(DailyQuoteDB.time).label('max_date')
                ).filter(
                    DailyQuoteDB.instrument_id == instrument_id
                )

                if start_date:
                    stmt = stmt.filter(DailyQuoteDB.time >= start_date)
                if end_date:
                    stmt = stmt.filter(DailyQuoteDB.time <= end_date)

                result = (await session.execute(stmt)).first()

                if result and result.min_date:
                    return {
                        'start_date': result.min_date.date() if isinstance(result.min_date, datetime) else result.min_date,
                        'end_date': result.max_date.date() if isinstance(result.max_date, datetime) else result.max_date
                    }
                else:
                    return {}

        except Exception as e:
            self.db_logger.error(f"Failed to get date range for {instrument_id}: {e}")
            return {}


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
            limit=limit,
            return_format=return_format
        )

    async def save_instrument_list(self, instruments: List[Dict[str, Any]]) -> bool:
        """保存交易品种列表（别名方法）"""
        return await self.save_instruments_batch(instruments)

    @staticmethod
    def _apply_delisted_status(record) -> None:
        """基于 delisted_date 自动校正生命周期字段（防线 B）

        若 delisted_date 已过期，强制设置 is_active=False、
        status='delisted'、trading_status=0。确保无论数据源传入什么值，
        退市标的都不会残留为可交易状态。
        """
        final_delisted = getattr(record, 'delisted_date', None)
        if final_delisted is None:
            return
        from datetime import date as date_type
        if isinstance(final_delisted, datetime):
            delisted_val = final_delisted.date()
        elif isinstance(final_delisted, date_type):
            delisted_val = final_delisted
        else:
            return
        if delisted_val <= date_type.today():
            record.is_active = False
            record.status = 'delisted'
            record.trading_status = 0

    @staticmethod
    def _should_preserve_protected_inactive_status(
        existing_status: str,
        incoming_source: str,
        incoming_delisted_date,
    ) -> bool:
        """Return True when a non-authoritative current-list source must not reactivate a row."""
        status = existing_status or ''
        protected_status = (
            status == 'delisted'
            or status.startswith('auto_deactivated')
            or status in {'calculation_terminated', 'inactive', 'stale_no_quote'}
        )
        source = (incoming_source or '').lower()
        official_source = (
            source == 'exchange_official'
            or source.endswith('_official')
            or source in {'sse_official', 'szse_official', 'bse_official'}
        )
        return protected_status and incoming_delisted_date is None and not official_source

    async def save_instruments_batch(self, instruments: List[Dict[str, Any]]) -> bool:
        """批量保存交易品种信息"""
        try:
            unique_instruments: Dict[str, Dict[str, Any]] = {}
            duplicate_count = 0
            for instrument in instruments or []:
                instrument_id = instrument.get('instrument_id')
                if not instrument_id:
                    duplicate_count += 1
                    continue
                if instrument_id in unique_instruments:
                    duplicate_count += 1
                unique_instruments[instrument_id] = instrument

            if duplicate_count:
                self.db_logger.warning(
                    "Deduplicated %s duplicate/invalid instrument rows before batch save",
                    duplicate_count,
                )
            instruments = list(unique_instruments.values())
            if not instruments:
                return False

            async with self.get_async_session() as session:
                upserted_count = 0
                for instrument_data in instruments:
                    try:
                        # 预处理日期字段，将字符串转换为datetime对象
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
                        stmt = select(InstrumentDB).filter(
                            InstrumentDB.instrument_id == processed_data['instrument_id']
                        )
                        result = await session.execute(stmt)
                        existing = result.scalar_one_or_none()

                        if existing:
                            existing_type = getattr(existing, 'type', None)
                            incoming_type = processed_data.get('type')
                            if existing_type and incoming_type and existing_type != incoming_type:
                                incoming_source = processed_data.get('source')
                                # A-share stocks and CNIndex metadata can share six-digit codes.
                                # Never let an index row overwrite an existing stock row; allow the
                                # stock master refresh to repair older polluted rows in the reverse case.
                                if existing_type == 'stock' and incoming_type == 'index':
                                    self.db_logger.warning(
                                        "Skipped instrument type collision for %s: existing=%s incoming=%s source=%s",
                                        processed_data.get('instrument_id'),
                                        existing_type,
                                        incoming_type,
                                        incoming_source,
                                    )
                                    continue
                            # 更新现有记录
                            for key, value in processed_data.items():
                                # 防线 A: 保护已有的退市日期不被空值覆盖
                                if key == 'delisted_date' and value is None and getattr(existing, key, None) is not None:
                                    continue
                                # 防线 C: 拦截缺少退市字段的数据源对已封禁/退市品种的强行唤醒。
                                # BaoStock 的 outDate/status 才能作为 A 股退市状态的主判据；AkShare/pytdx
                                # 等当前名单源不得用空 delisted_date 覆盖既有退市或自动封禁状态。
                                existing_status = getattr(existing, 'status', '') or ''
                                incoming_source = processed_data.get('source')
                                incoming_delisted_date = processed_data.get('delisted_date')
                                if (
                                    key in ('is_active', 'status')
                                    and self._should_preserve_protected_inactive_status(
                                        existing_status,
                                        incoming_source,
                                        incoming_delisted_date,
                                    )
                                ):
                                    continue

                                # 确保只更新模型中存在的字段，防止动态添加属性
                                if hasattr(existing, key) and getattr(existing, key) != value:
                                    setattr(existing, key, value)
                            existing.updated_at = get_shanghai_time()
                            # 防线 B: 基于最终 delisted_date 强制校正 is_active/status
                            self._apply_delisted_status(existing)
                        else:
                            # 创建新记录
                            try:
                                # 确保必需字段存在
                                required_fields = ['instrument_id', 'symbol', 'name', 'exchange', 'type', 'currency']
                                for field in required_fields:
                                    if field not in processed_data:
                                        self.db_logger.warning(f"Missing required field '{field}' for instrument {instrument_data.get('instrument_id', 'unknown')}")

                                # 过滤掉模型中不存在的字段，避免创建错误
                                valid_data = {k: v for k, v in processed_data.items() if hasattr(InstrumentDB, k)}

                                # 调试：打印数据信息
                                self.db_logger.debug(f"Creating instrument {processed_data.get('instrument_id', 'unknown')} with {len(valid_data)} fields")

                                db_instrument = InstrumentDB(**valid_data)
                                session.add(db_instrument)
                                # 防线 B: 新记录同样校正
                                self._apply_delisted_status(db_instrument)
                            except Exception as create_error:
                                self.db_logger.error(f"Error creating instrument: {create_error}")
                                self.db_logger.error(f"Original data keys: {list(processed_data.keys())}")
                                raise
                        upserted_count += 1
                    except Exception as e:
                        self.db_logger.error(f"Error saving instrument {instrument_data.get('instrument_id', 'unknown')}: {e}")

                await session.commit()
                self.db_logger.info(f"Successfully upserted {upserted_count}/{len(instruments)} instruments")
                return upserted_count > 0

        except Exception as e:
            import traceback
            self.db_logger.error(f"Failed to save instruments batch: {e}")
            self.db_logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    async def mark_instrument_delisted(
        self,
        instrument_id: str,
        *,
        delisted_date: Optional[Union[str, date, datetime]] = None,
        source: str = "cninfo_bse_delisting",
    ) -> bool:
        """Mark one instrument as formally delisted using confirmed evidence.

        This mutates only lifecycle status fields; historical quotes remain
        intact and available for research.
        """
        if not instrument_id:
            return False

        parsed_date = None
        if isinstance(delisted_date, datetime):
            parsed_date = delisted_date
        elif isinstance(delisted_date, date):
            parsed_date = datetime.combine(delisted_date, datetime.min.time())
        elif isinstance(delisted_date, str):
            text_value = delisted_date.strip()
            if text_value:
                try:
                    parsed_date = datetime.fromisoformat(text_value[:10])
                except ValueError:
                    self.db_logger.warning(
                        "Invalid delisted_date for %s: %s",
                        instrument_id,
                        delisted_date,
                    )
                    return False
        elif delisted_date is not None:
            return False

        try:
            async with self.get_async_session() as session:
                result = await session.execute(
                    select(InstrumentDB).filter(InstrumentDB.instrument_id == instrument_id)
                )
                record = result.scalar_one_or_none()
                if record is None:
                    return False

                record.delisted_date = parsed_date
                record.status = "delisted"
                record.is_active = False
                record.trading_status = 0
                record.source = source
                record.updated_at = get_shanghai_time()
                await session.commit()
                return True
        except Exception as exc:
            self.db_logger.error("Failed to mark %s delisted: %s", instrument_id, exc)
            return False

    async def mark_instrument_active(
        self,
        instrument_id: str,
        *,
        source: str,
        listed_date: Optional[Union[str, date, datetime]] = None,
    ) -> bool:
        """Reactivate one instrument using confirmed official lifecycle evidence."""
        if not instrument_id or not source:
            return False

        parsed_listed_date = None
        if listed_date:
            if isinstance(listed_date, datetime):
                parsed_listed_date = listed_date
            elif isinstance(listed_date, date):
                parsed_listed_date = datetime.combine(listed_date, datetime.min.time())
            elif isinstance(listed_date, str):
                try:
                    parsed_listed_date = datetime.fromisoformat(listed_date[:10])
                except ValueError:
                    parsed_listed_date = None

        try:
            async with self.get_async_session() as session:
                result = await session.execute(
                    select(InstrumentDB).filter(InstrumentDB.instrument_id == instrument_id)
                )
                record = result.scalar_one_or_none()
                if record is None:
                    return False

                record.status = "active"
                record.is_active = True
                record.trading_status = 1
                record.delisted_date = None
                record.source = source
                if parsed_listed_date is not None and record.listed_date is None:
                    record.listed_date = parsed_listed_date
                record.updated_at = get_shanghai_time()
                await session.commit()
                return True
        except Exception as exc:
            self.db_logger.error("Failed to mark %s active: %s", instrument_id, exc)
            return False

    async def mark_instrument_suspended(
        self,
        instrument_id: str,
        *,
        source: str,
    ) -> bool:
        """Mark one instrument suspended using confirmed official lifecycle evidence."""
        if not instrument_id or not source:
            return False
        try:
            async with self.get_async_session() as session:
                result = await session.execute(
                    select(InstrumentDB).filter(InstrumentDB.instrument_id == instrument_id)
                )
                record = result.scalar_one_or_none()
                if record is None:
                    return False

                record.status = "suspended"
                record.is_active = True
                record.trading_status = 0
                record.source = source
                record.updated_at = get_shanghai_time()
                await session.commit()
                return True
        except Exception as exc:
            self.db_logger.error("Failed to mark %s suspended: %s", instrument_id, exc)
            return False

    async def mark_index_lifecycle_state(
        self,
        instrument_id: str,
        *,
        lifecycle_state: str,
        source: str,
        effective_date: Optional[Union[str, date, datetime]] = None,
        last_quote_date: Optional[Union[str, date, datetime]] = None,
    ) -> bool:
        """Apply an official index lifecycle state without deleting history."""
        if not instrument_id or not lifecycle_state:
            return False

        excluded_states = {"calculation_terminated", "inactive", "metadata_only", "stale_no_quote"}

        def _parse(value: Optional[Union[str, date, datetime]]) -> Optional[datetime]:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime.combine(value, datetime.min.time())
            if isinstance(value, str) and value.strip():
                try:
                    return datetime.fromisoformat(value[:10])
                except ValueError:
                    return None
            return None

        parsed_effective = _parse(effective_date)
        try:
            async with self.get_async_session() as session:
                result = await session.execute(
                    select(InstrumentDB).filter(InstrumentDB.instrument_id == instrument_id)
                )
                record = result.scalar_one_or_none()
                if record is None:
                    return False
                if getattr(record, 'type', None) != 'index':
                    self.db_logger.warning(
                        "Skipped non-index lifecycle update for %s: type=%s state=%s",
                        instrument_id,
                        getattr(record, 'type', None),
                        lifecycle_state,
                    )
                    return False

                record.status = lifecycle_state
                record.is_active = lifecycle_state not in excluded_states
                record.trading_status = 0 if lifecycle_state in excluded_states else 1
                record.source = source
                # Index publication stops are not stock delistings. Keep the
                # effective date in index_lifecycle_evidence so the generic
                # stock delisting guard does not rewrite the semantic status.
                if lifecycle_state == "active_quote":
                    record.delisted_date = None
                record.updated_at = get_shanghai_time()
                await session.commit()
                return True
        except Exception as exc:
            self.db_logger.error(
                "Failed to mark index %s lifecycle_state=%s: %s",
                instrument_id,
                lifecycle_state,
                exc,
            )
            return False

    async def mark_instruments_excluded(
        self,
        instrument_ids: List[str],
        *,
        source: str = "hkex_product_scope_exclusion",
    ) -> int:
        """Mark non-research HKEX products unavailable without deleting history."""
        ids = sorted({str(item).strip() for item in instrument_ids or [] if item})
        if not ids:
            return 0

        updated_count = 0
        try:
            async with self.get_async_session() as session:
                for start in range(0, len(ids), 500):
                    chunk = ids[start:start + 500]
                    result = await session.execute(
                        select(InstrumentDB).filter(InstrumentDB.instrument_id.in_(chunk))
                    )
                    for record in result.scalars().all():
                        changed = (
                            record.status != "excluded"
                            or record.is_active is not False
                            or record.trading_status != 0
                            or record.source != source
                        )
                        record.status = "excluded"
                        record.is_active = False
                        record.trading_status = 0
                        record.source = source
                        record.updated_at = get_shanghai_time()
                        if changed:
                            updated_count += 1
                await session.commit()
                return updated_count
        except Exception as exc:
            self.db_logger.error("Failed to mark excluded instruments: %s", exc)
            return 0

    async def save_instrument_master_metadata_batch(
        self,
        rows: List[Dict[str, Any]],
    ) -> int:
        """Save auxiliary instrument-master metadata outside the core instruments table."""
        if not rows:
            return 0

        create_sql = text(
            """
            CREATE TABLE IF NOT EXISTS instrument_master_metadata (
                instrument_id TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                product_type TEXT,
                research_scope TEXT,
                canonical_instrument_id TEXT,
                is_canonical INTEGER,
                counter_currency TEXT,
                official_lifecycle_source TEXT,
                source_url TEXT,
                raw_snapshot_hash TEXT,
                parser_version TEXT,
                metadata_json TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        upsert_sql = text(
            """
            INSERT INTO instrument_master_metadata (
                instrument_id, exchange, product_type, research_scope,
                canonical_instrument_id, is_canonical, counter_currency,
                official_lifecycle_source, source_url, raw_snapshot_hash,
                parser_version, metadata_json, updated_at
            ) VALUES (
                :instrument_id, :exchange, :product_type, :research_scope,
                :canonical_instrument_id, :is_canonical, :counter_currency,
                :official_lifecycle_source, :source_url, :raw_snapshot_hash,
                :parser_version, :metadata_json, :updated_at
            )
            ON CONFLICT(instrument_id) DO UPDATE SET
                exchange=excluded.exchange,
                product_type=excluded.product_type,
                research_scope=excluded.research_scope,
                canonical_instrument_id=excluded.canonical_instrument_id,
                is_canonical=excluded.is_canonical,
                counter_currency=excluded.counter_currency,
                official_lifecycle_source=excluded.official_lifecycle_source,
                source_url=excluded.source_url,
                raw_snapshot_hash=excluded.raw_snapshot_hash,
                parser_version=excluded.parser_version,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """
        )

        now_text = get_shanghai_time().isoformat()
        saved = 0
        try:
            async with self.get_async_session() as session:
                await session.execute(create_sql)
                for row in rows:
                    instrument_id = row.get("instrument_id")
                    if not instrument_id:
                        continue
                    await session.execute(
                        upsert_sql,
                        {
                            "instrument_id": instrument_id,
                            "exchange": row.get("exchange") or "HKEX",
                            "product_type": row.get("product_type"),
                            "research_scope": row.get("research_scope"),
                            "canonical_instrument_id": row.get("canonical_instrument_id"),
                            "is_canonical": 1 if row.get("is_canonical") else 0,
                            "counter_currency": row.get("counter_currency") or row.get("currency"),
                            "official_lifecycle_source": row.get("official_lifecycle_source"),
                            "source_url": row.get("source_url"),
                            "raw_snapshot_hash": row.get("raw_snapshot_hash"),
                            "parser_version": row.get("parser_version"),
                            "metadata_json": json.dumps(row, ensure_ascii=False, default=str),
                            "updated_at": now_text,
                        },
                    )
                    saved += 1
                await session.commit()
            return saved
        except Exception as exc:
            self.db_logger.error("Failed to save instrument master metadata batch: %s", exc)
            return 0

    async def save_instrument_master_discrepancies(
        self,
        rows: List[Dict[str, Any]],
        *,
        exchange: str,
        run_id: str,
    ) -> int:
        """Persist review-required instrument-master discrepancies."""
        if not rows:
            return 0

        create_sql = text(
            """
            CREATE TABLE IF NOT EXISTS instrument_master_discrepancies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                exchange TEXT NOT NULL,
                instrument_id TEXT,
                reason TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        insert_sql = text(
            """
            INSERT INTO instrument_master_discrepancies (
                run_id, exchange, instrument_id, reason, payload_json, created_at
            ) VALUES (
                :run_id, :exchange, :instrument_id, :reason, :payload_json, :created_at
            )
            """
        )
        now_text = get_shanghai_time().isoformat()
        saved = 0
        try:
            async with self.get_async_session() as session:
                await session.execute(create_sql)
                for row in rows:
                    await session.execute(
                        insert_sql,
                        {
                            "run_id": run_id,
                            "exchange": exchange,
                            "instrument_id": row.get("instrument_id"),
                            "reason": row.get("reason"),
                            "payload_json": json.dumps(row, ensure_ascii=False, default=str),
                            "created_at": now_text,
                        },
                    )
                    saved += 1
                await session.commit()
            return saved
        except Exception as exc:
            self.db_logger.error("Failed to save instrument master discrepancies: %s", exc)
            return 0

    async def save_index_lifecycle_evidence(
        self,
        rows: List[Dict[str, Any]],
    ) -> int:
        """Persist official index lifecycle evidence with audit lineage."""
        if not rows:
            return 0

        create_sql = text(
            """
            CREATE TABLE IF NOT EXISTS index_lifecycle_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_id TEXT NOT NULL,
                symbol TEXT,
                exchange TEXT,
                lifecycle_state TEXT NOT NULL,
                event_type TEXT,
                effective_date TEXT,
                last_quote_date TEXT,
                announcement_date TEXT,
                announcement_title TEXT,
                evidence_url TEXT,
                matched_code TEXT,
                confidence TEXT,
                source TEXT,
                parser_version TEXT,
                raw_snapshot_hash TEXT,
                diagnostics_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(instrument_id, lifecycle_state, event_type, evidence_url, confidence)
            )
            """
        )
        upsert_sql = text(
            """
            INSERT INTO index_lifecycle_evidence (
                instrument_id, symbol, exchange, lifecycle_state, event_type,
                effective_date, last_quote_date, announcement_date,
                announcement_title, evidence_url, matched_code, confidence,
                source, parser_version, raw_snapshot_hash, diagnostics_json,
                created_at, updated_at
            ) VALUES (
                :instrument_id, :symbol, :exchange, :lifecycle_state, :event_type,
                :effective_date, :last_quote_date, :announcement_date,
                :announcement_title, :evidence_url, :matched_code, :confidence,
                :source, :parser_version, :raw_snapshot_hash, :diagnostics_json,
                :created_at, :updated_at
            )
            ON CONFLICT(instrument_id, lifecycle_state, event_type, evidence_url, confidence)
            DO UPDATE SET
                symbol=excluded.symbol,
                exchange=excluded.exchange,
                effective_date=excluded.effective_date,
                last_quote_date=excluded.last_quote_date,
                announcement_date=excluded.announcement_date,
                announcement_title=excluded.announcement_title,
                matched_code=excluded.matched_code,
                source=excluded.source,
                parser_version=excluded.parser_version,
                raw_snapshot_hash=excluded.raw_snapshot_hash,
                diagnostics_json=excluded.diagnostics_json,
                updated_at=excluded.updated_at
            """
        )

        def _date_text(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, datetime):
                return value.date().isoformat()
            if isinstance(value, date):
                return value.isoformat()
            text_value = str(value).strip()
            return text_value[:10] if text_value else None

        now_text = get_shanghai_time().isoformat()
        saved = 0
        try:
            async with self.get_async_session() as session:
                await session.execute(create_sql)
                for row in rows:
                    instrument_id = row.get("instrument_id")
                    lifecycle_state = row.get("lifecycle_state")
                    if not instrument_id or not lifecycle_state:
                        continue
                    await session.execute(
                        upsert_sql,
                        {
                            "instrument_id": instrument_id,
                            "symbol": row.get("symbol"),
                            "exchange": row.get("exchange"),
                            "lifecycle_state": lifecycle_state,
                            "event_type": row.get("event_type") or lifecycle_state,
                            "effective_date": _date_text(row.get("effective_date")),
                            "last_quote_date": _date_text(row.get("last_quote_date")),
                            "announcement_date": _date_text(row.get("announcement_date")),
                            "announcement_title": row.get("announcement_title"),
                            "evidence_url": row.get("evidence_url"),
                            "matched_code": row.get("matched_code"),
                            "confidence": row.get("confidence") or "unknown",
                            "source": row.get("source"),
                            "parser_version": row.get("parser_version"),
                            "raw_snapshot_hash": row.get("raw_snapshot_hash"),
                            "diagnostics_json": json.dumps(
                                row.get("diagnostics") or row,
                                ensure_ascii=False,
                                default=str,
                            ),
                            "created_at": now_text,
                            "updated_at": now_text,
                        },
                    )
                    saved += 1
                await session.commit()
            return saved
        except Exception as exc:
            self.db_logger.error("Failed to save index lifecycle evidence: %s", exc)
            return 0

    async def get_index_lifecycle_evidence(
        self,
        *,
        exchanges: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Read persisted official index lifecycle evidence."""
        clauses = ["1=1"]
        params: Dict[str, Any] = {}
        if exchanges:
            placeholders = []
            for idx, exchange in enumerate(exchanges):
                key = f"exchange_{idx}"
                placeholders.append(f":{key}")
                params[key] = exchange
            clauses.append(f"exchange IN ({','.join(placeholders)})")
        if states:
            placeholders = []
            for idx, state in enumerate(states):
                key = f"state_{idx}"
                placeholders.append(f":{key}")
                params[key] = state
            clauses.append(f"lifecycle_state IN ({','.join(placeholders)})")

        table_exists = await self.execute_read_query(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'index_lifecycle_evidence'
            """
        )
        if not table_exists:
            return []

        return await self.execute_read_query(
            f"""
            SELECT *
            FROM index_lifecycle_evidence
            WHERE {' AND '.join(clauses)}
            ORDER BY updated_at DESC, id DESC
            """,
            params,
        )

    async def cleanup_ghost_instruments(self, grace_days: int, zombie_grace_days: int = 180) -> int:
        """
        清理由于数据源脏数据引入的长期无交易记录的“幽灵股”。
        
        将 `is_active=1`、建库时间超过 `grace_days`、且在 `daily_quotes` 中没有任何一条历史数据的品种，
        批量标记为 `is_active=0` 和 `status='auto_deactivated_no_data'`。
        
        Args:
            grace_days: 宽限期天数（在这几天内刚被加入的不受影响，防止误杀即将上市的新股）。
            
        Returns:
            int: 成功封禁的幽灵股数量。
        """
        try:
            from utils.date_utils import get_shanghai_time
            from datetime import timedelta
            from sqlalchemy.future import select
            from sqlalchemy import func
            cutoff_date = get_shanghai_time() - timedelta(days=grace_days)
            zombie_cutoff = get_shanghai_time() - timedelta(days=zombie_grace_days)
            
            async with self.get_async_session() as session:
                # 使用子查询：查找存在于 daily_quotes 中的所有独特股票ID
                stmt_active_with_no_data = select(InstrumentDB).filter(
                    InstrumentDB.is_active == True,
                    InstrumentDB.created_at < cutoff_date,
                    ~InstrumentDB.instrument_id.in_(
                        select(DailyQuoteDB.instrument_id).distinct()
                    )
                )
                result = await session.execute(stmt_active_with_no_data)
                ghosts = result.scalars().all()
                
                ghost_count = len(ghosts)
                if ghost_count > 0:
                    for ghost in ghosts:
                        ghost.is_active = False
                        ghost.status = 'auto_deactivated_no_data'
                        ghost.updated_at = get_shanghai_time()
                    
                    await session.commit()
                    self.db_logger.info(f"Successfully auto-deactivated {ghost_count} ghost instruments (grace_days={grace_days}).")
                else:
                    self.db_logger.debug("No ghost instruments found to cleanup.")
                
                # 步骤二：清理僵尸股 (有历史数据，但最后交易日期超期)
                stmt_zombies = select(InstrumentDB).filter(
                    InstrumentDB.is_active == True,
                    InstrumentDB.instrument_id.in_(
                        select(DailyQuoteDB.instrument_id)
                        .group_by(DailyQuoteDB.instrument_id)
                        .having(func.max(DailyQuoteDB.time) < zombie_cutoff)
                    )
                )
                zombie_result = await session.execute(stmt_zombies)
                zombies = zombie_result.scalars().all()
                
                zombie_count = len(zombies)
                if zombie_count > 0:
                    for zombie in zombies:
                        zombie.is_active = False
                        zombie.status = 'auto_deactivated_zombie'
                        zombie.updated_at = get_shanghai_time()
                    
                    await session.commit()
                    self.db_logger.info(f"Successfully auto-deactivated {zombie_count} zombie instruments (zombie_grace_days={zombie_grace_days}).")
                    
                return ghost_count + zombie_count
        except Exception as e:
            self.db_logger.error(f"Failed to run cleanup_ghost_instruments: {e}")
            return 0

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
            async with self.get_async_session() as session:
                stmt = select(InstrumentDB)

                # Apply filters
                if exchange:
                    stmt = stmt.filter(InstrumentDB.exchange == exchange)
                if type:
                    stmt = stmt.filter(InstrumentDB.type == type)
                if is_active is not None:
                    stmt = stmt.filter(InstrumentDB.is_active == is_active)
                if status:
                    stmt = stmt.filter(InstrumentDB.status == status)
                if industry:
                    stmt = stmt.filter(InstrumentDB.industry == industry)

                # Apply ordering and pagination
                stmt = stmt.order_by(InstrumentDB.exchange, InstrumentDB.symbol)
                if limit:
                    stmt = stmt.limit(limit).offset(offset)
                result = await session.execute(stmt)
                query = result.scalars().all()

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
                        'lot_size': instrument.lot_size,
                        'tick_size': instrument.tick_size,
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
        if not symbol and not instrument_id:
            self.db_logger.warning("get_instrument_info called without symbol or instrument_id")
            return None

        try:
            async with self.get_async_session() as session:
                stmt = select(InstrumentDB)

                if instrument_id:
                    stmt = stmt.filter(InstrumentDB.instrument_id == instrument_id)
                elif symbol:
                    stmt = stmt.filter(InstrumentDB.symbol == symbol)
                    # 优先返回股票，避免和同代码的指数冲突
                    stmt = stmt.order_by(InstrumentDB.type.desc()) 
                else:
                    return None

                result = await session.execute(stmt)
                instrument = result.scalars().first()
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
                    'lot_size': instrument.lot_size,
                    'tick_size': instrument.tick_size,
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

    async def get_instrument_by_id(self, instrument_id: str) -> Optional[Dict[str, Any]]:
        """根据ID获取交易品种信息 (get_instrument_info的别名)"""
        return await self.get_instrument_info(instrument_id=instrument_id)

    async def get_instrument_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """根据交易代码获取交易品种信息 (get_instrument_info的别名)"""
        return await self.get_instrument_info(symbol=symbol)

    # === Daily Quote Operations ===

    async def save_daily_data(
        self,
        quotes: List[Dict[str, Any]],
        *,
        return_stats: bool = False,
    ) -> Union[bool, Dict[str, int]]:
        """批量保存日线数据.

        默认返回 bool 以兼容既有调用方; return_stats=True 时返回增量写入计数。
        """
        if not quotes:
            return self._empty_write_stats() if return_stats else True
        stats = self._empty_write_stats()
        try:
            emit_changelog = self._is_changelog_enabled("quotes", "daily_quotes")
            payloads_by_key: Dict[tuple, Dict[str, Any]] = {}
            for quote in quotes:
                try:
                    payload = self._daily_quote_payload(quote)
                    if not payload.get("instrument_id") or payload.get("time") is None:
                        stats["skipped"] += 1
                        continue
                    payload["row_hash"] = self._daily_quote_hash(payload)
                except (TypeError, ValueError, OverflowError) as row_error:
                    stats["failed"] += 1
                    self.db_logger.warning(
                        "Failed to normalize daily quote for %s: %s",
                        quote.get("instrument_id") if isinstance(quote, dict) else None,
                        row_error,
                    )
                    continue
                key = (payload["time"], payload["instrument_id"])
                if key in payloads_by_key:
                    stats["skipped"] += 1
                payloads_by_key[key] = payload

            normalized_payloads = list(payloads_by_key.values())
            if not normalized_payloads:
                return stats if return_stats else stats["failed"] == 0

            async with self.get_async_session() as session:
                chunk_size = 1000
                for i in range(0, len(normalized_payloads), chunk_size):
                    payloads = normalized_payloads[i:i + chunk_size]

                    keys = [
                        (payload["time"], payload["instrument_id"])
                        for payload in payloads
                    ]
                    result = await session.execute(
                        select(DailyQuoteDB).where(
                            tuple_(DailyQuoteDB.time, DailyQuoteDB.instrument_id).in_(keys)
                        )
                    )
                    existing_by_key = {
                        (row.time, row.instrument_id): row
                        for row in result.scalars().all()
                    }

                    for payload in payloads:
                        key = (payload["time"], payload["instrument_id"])
                        existing = existing_by_key.get(key)
                        if existing is None:
                            row = DailyQuoteDB(**payload, row_version=1)
                            session.add(row)
                            if emit_changelog:
                                session.add(self._change_log_record(
                                    domain="quotes",
                                    dataset="daily_quotes",
                                    change_type="insert",
                                    business_key={
                                        "instrument_id": payload["instrument_id"],
                                        "trade_date": payload["time"].date().isoformat(),
                                    },
                                    instrument_id=payload["instrument_id"],
                                    observation_date=payload["time"],
                                    old_hash=None,
                                    new_hash=payload["row_hash"],
                                    row_version=1,
                                    source=payload.get("source"),
                                    batch_id=payload.get("batch_id"),
                                ))
                                stats["changelog_written"] += 1
                            stats["inserted"] += 1
                            continue

                        old_hash = existing.row_hash or self._daily_quote_hash({
                            "time": existing.time,
                            "instrument_id": existing.instrument_id,
                            "open": existing.open,
                            "high": existing.high,
                            "low": existing.low,
                            "close": existing.close,
                            "volume": existing.volume,
                            "amount": existing.amount,
                            "turnover": existing.turnover,
                            "pre_close": existing.pre_close,
                            "change": existing.change,
                            "pct_change": existing.pct_change,
                            "tradestatus": existing.tradestatus,
                            "factor": existing.factor,
                            "adjustment_type": existing.adjustment_type,
                            "is_complete": existing.is_complete,
                            "quality_score": existing.quality_score,
                            "source": existing.source,
                        })

                        if old_hash == payload["row_hash"]:
                            if existing.row_hash is None:
                                existing.row_hash = old_hash
                            if not existing.row_version:
                                existing.row_version = 1
                            stats["unchanged"] += 1
                            continue

                        previous_version = existing.row_version or 1
                        next_version = previous_version + 1
                        for field in (
                            "open", "high", "low", "close", "volume", "amount",
                            "turnover", "pre_close", "change", "pct_change",
                            "tradestatus", "factor", "adjustment_type",
                            "is_complete", "quality_score", "source", "batch_id",
                        ):
                            setattr(existing, field, payload.get(field))
                        existing.row_hash = payload["row_hash"]
                        existing.row_version = next_version
                        existing.updated_at = get_shanghai_time()
                        if emit_changelog:
                            session.add(self._change_log_record(
                                domain="quotes",
                                dataset="daily_quotes",
                                change_type="update",
                                business_key={
                                    "instrument_id": payload["instrument_id"],
                                    "trade_date": payload["time"].date().isoformat(),
                                },
                                instrument_id=payload["instrument_id"],
                                observation_date=payload["time"],
                                old_hash=old_hash,
                                new_hash=payload["row_hash"],
                                row_version=next_version,
                                source=payload.get("source"),
                                batch_id=payload.get("batch_id"),
                            ))
                            stats["changelog_written"] += 1
                        stats["changed"] += 1

                await session.commit()

                self.db_logger.info(
                    "Saved daily quotes with CDC counters: %s", stats
                )
                return stats if return_stats else True
                
        except Exception as e:
            self.db_logger.error(f"Failed to save daily data: {e}")
            if return_stats:
                skipped = int(stats.get("skipped", 0) or 0)
                stats = self._empty_write_stats()
                stats["skipped"] = skipped
                stats["failed"] = max(0, len(quotes) - skipped)
                return stats
            return False

    async def save_daily_quotes(
        self,
        quotes: List[Dict[str, Any]],
        *,
        return_stats: bool = False,
    ) -> Union[bool, Dict[str, int]]:
        """批量保存日线数据 (save_daily_data的别名)"""
        return await self.save_daily_data(quotes, return_stats=return_stats)

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
            async with self.get_async_session() as session:
                stmt = select(DailyQuoteDB)

                # Apply instrument filter
                if instrument_id:
                    stmt = stmt.filter(DailyQuoteDB.instrument_id == instrument_id)
                elif symbol:
                    # Join with instruments table to get instrument_id from symbol
                    stmt = stmt.join(InstrumentDB).filter(InstrumentDB.symbol == symbol)

                # Apply date range filter
                if start_date:
                    stmt = stmt.filter(DailyQuoteDB.time >= start_date)
                if end_date:
                    stmt = stmt.filter(DailyQuoteDB.time <= end_date)

                # Apply other filters
                if tradestatus is not None:
                    stmt = stmt.filter(DailyQuoteDB.tradestatus == tradestatus)
                if is_complete is not None:
                    stmt = stmt.filter(DailyQuoteDB.is_complete == is_complete)
                if min_volume:
                    stmt = stmt.filter(DailyQuoteDB.volume >= min_volume)

                # Order and limit
                stmt = stmt.order_by(DailyQuoteDB.time.desc())
                if limit:
                    stmt = stmt.limit(limit)
                result = await session.execute(stmt)
                query = result.scalars().all()

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

    @staticmethod
    def _serialize_change_log_row(row: DataChangeLogDB) -> Dict[str, Any]:
        try:
            business_key = json.loads(row.business_key_json)
        except (TypeError, json.JSONDecodeError):
            business_key = {}
        return {
            "sequence_id": row.sequence_id,
            "domain": row.domain,
            "dataset": row.dataset,
            "change_type": row.change_type,
            "business_key": business_key,
            "instrument_id": row.instrument_id,
            "series_id": row.series_id,
            "observation_date": row.observation_date,
            "period": row.period,
            "old_hash": row.old_hash,
            "new_hash": row.new_hash,
            "row_version": row.row_version,
            "source": row.source,
            "source_mode": row.source_mode,
            "source_profile": row.source_profile,
            "ingestion_run_id": row.ingestion_run_id,
            "batch_id": row.batch_id,
            "changed_at": row.changed_at,
        }

    async def get_change_watermark(
        self,
        *,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return the latest local-observed changelog sequence."""
        try:
            async with self.get_async_session() as session:
                stmt = select(func.max(DataChangeLogDB.sequence_id))
                if domain:
                    stmt = stmt.where(DataChangeLogDB.domain == domain)
                if dataset:
                    stmt = stmt.where(DataChangeLogDB.dataset == dataset)
                latest = await session.scalar(stmt)
                return {
                    "domain": domain,
                    "dataset": dataset,
                    "latest_sequence": int(latest or 0),
                    "is_empty": latest is None,
                }
        except Exception as e:
            self.db_logger.error("Failed to get change watermark: %s", e)
            raise

    async def get_data_changes(
        self,
        *,
        since_sequence: int = 0,
        domain: Optional[str] = None,
        dataset: Optional[str] = None,
        instrument_id: Optional[str] = None,
        series_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """List changelog records after a sequence in stable ascending order."""
        if since_sequence < 0:
            raise ValueError("since_sequence must be >= 0")
        if limit is None:
            limit = self._change_watermark_int_config("default_limit", 1000)
        if limit <= 0:
            raise ValueError("limit must be > 0")
        limit = min(int(limit), self._change_watermark_int_config("max_limit", 5000))

        start_dt = self._coerce_datetime(start_date)
        end_dt = self._coerce_datetime(end_date)
        async with self.get_async_session() as session:
            stmt = select(DataChangeLogDB).where(
                DataChangeLogDB.sequence_id > since_sequence
            )
            latest_stmt = select(func.max(DataChangeLogDB.sequence_id))
            if domain:
                stmt = stmt.where(DataChangeLogDB.domain == domain)
                latest_stmt = latest_stmt.where(DataChangeLogDB.domain == domain)
            if dataset:
                stmt = stmt.where(DataChangeLogDB.dataset == dataset)
                latest_stmt = latest_stmt.where(DataChangeLogDB.dataset == dataset)
            if instrument_id:
                stmt = stmt.where(DataChangeLogDB.instrument_id == instrument_id)
                latest_stmt = latest_stmt.where(DataChangeLogDB.instrument_id == instrument_id)
            if series_id:
                stmt = stmt.where(DataChangeLogDB.series_id == series_id)
                latest_stmt = latest_stmt.where(DataChangeLogDB.series_id == series_id)
            if start_dt:
                stmt = stmt.where(DataChangeLogDB.observation_date >= start_dt)
                latest_stmt = latest_stmt.where(DataChangeLogDB.observation_date >= start_dt)
            if end_dt:
                stmt = stmt.where(DataChangeLogDB.observation_date <= end_dt)
                latest_stmt = latest_stmt.where(DataChangeLogDB.observation_date <= end_dt)

            stmt = stmt.order_by(asc(DataChangeLogDB.sequence_id)).limit(limit + 1)
            result = await session.execute(stmt)
            rows = result.scalars().all()
            has_more = len(rows) > limit
            page_rows = rows[:limit]
            changes = [self._serialize_change_log_row(row) for row in page_rows]
            latest_available = await session.scalar(latest_stmt)
            latest_returned = changes[-1]["sequence_id"] if changes else since_sequence
            return {
                "since_sequence": since_sequence,
                "latest_sequence": int(latest_available or 0),
                "latest_returned_sequence": int(latest_returned or 0),
                "next_sequence": int(latest_returned or since_sequence),
                "has_more": has_more,
                "limit": limit,
                "count": len(changes),
                "changes": changes,
            }

    async def get_change_watermark_health(self) -> Dict[str, Any]:
        """Return a compact operational health snapshot for P0 change watermarks."""
        cfg = self._change_watermark_config()
        domains = self._change_watermark_enabled_map(
            "domains",
            {"quotes": True, "adjustment_factor": True},
        )
        datasets = self._change_watermark_enabled_map(
            "datasets",
            {"daily_quotes": True, "adjustment_factors": True},
        )
        try:
            async with self.get_async_session() as session:
                total_rows = await session.scalar(select(func.count(DataChangeLogDB.sequence_id)))
                latest_rows = await session.execute(
                    select(DataChangeLogDB.domain, func.max(DataChangeLogDB.sequence_id))
                    .group_by(DataChangeLogDB.domain)
                    .order_by(DataChangeLogDB.domain)
                )
                latest_by_domain = {
                    domain: int(sequence or 0)
                    for domain, sequence in latest_rows.all()
                }
            return {
                "enabled": bool(cfg.get("enabled", True)),
                "domains": domains,
                "datasets": datasets,
                "latest_by_domain": latest_by_domain,
                "total_change_rows": int(total_rows or 0),
                "default_limit": self._change_watermark_int_config("default_limit", 1000),
                "max_limit": self._change_watermark_int_config("max_limit", 5000),
            }
        except Exception as e:
            self.db_logger.error("Failed to get change watermark health: %s", e)
            return {
                "enabled": bool(cfg.get("enabled", True)),
                "domains": domains,
                "datasets": datasets,
                "latest_by_domain": {},
                "total_change_rows": 0,
                "status": "error",
                "error": str(e),
            }

    async def get_latest_quote_date(self, instrument_id: str) -> Optional[datetime]:
        """获取最新日期"""
        try:
            async with self.get_async_session() as session:
                stmt = select(DailyQuoteDB).filter(
                    DailyQuoteDB.instrument_id == instrument_id
                ).order_by(DailyQuoteDB.time.desc()).limit(1)
                result = await session.execute(stmt)
                latest = result.scalar_one_or_none()
                if latest:
                    return latest.time
                return None

        except Exception as e:
            self.db_logger.error(f"Failed to get latest quote date for {instrument_id}: {e}")
            return None

    # === Trading Calendar Operations ===

    async def save_trading_calendar(self, calendar_data: List[Dict[str, Any]]) -> bool:
        """保存交易日历数据（批量 upsert）"""
        try:
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert

            async with self.get_async_session() as session:
                chunk_size = 500
                total_saved = 0

                for i in range(0, len(calendar_data), chunk_size):
                    chunk = calendar_data[i:i + chunk_size]

                    stmt = sqlite_insert(TradingCalendarDB).values(chunk)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['exchange', 'date'],
                        set_={
                            'is_trading_day': stmt.excluded.is_trading_day,
                            'reason': stmt.excluded.reason,
                            'session_type': stmt.excluded.session_type,
                            'source': stmt.excluded.source,
                            'updated_at': get_shanghai_time(),
                        }
                    )
                    await session.execute(stmt)
                    total_saved += len(chunk)

                await session.commit()
                self.db_logger.info(f"Successfully upserted {total_saved} calendar records")
                return total_saved

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
            async with self.get_async_session() as session:
                stmt = select(TradingCalendarDB.date).distinct()

                # Apply filters
                if exchange:
                    stmt = stmt.filter(TradingCalendarDB.exchange == exchange)
                if start_date:
                    # Convert string to date if needed
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                    # Convert date to datetime to match database storage format
                    start_datetime = datetime.combine(start_date, datetime.min.time())
                    stmt = stmt.filter(TradingCalendarDB.date >= start_datetime)
                if end_date:
                    # Convert string to date if needed
                    if isinstance(end_date, str):
                        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                    # Convert date to datetime to match database storage format
                    # Use end of day to ensure the end date is included
                    end_datetime = datetime.combine(end_date, datetime.max.time())
                    stmt = stmt.filter(TradingCalendarDB.date <= end_datetime)
                if is_trading_day is not None:
                    stmt = stmt.filter(TradingCalendarDB.is_trading_day == is_trading_day)

                stmt = stmt.order_by(TradingCalendarDB.date)
                result = await session.execute(stmt)

                trading_days = []
                for row in result.scalars().all():
                    if isinstance(row, datetime):
                        trading_days.append(row.date())
                    else:
                        trading_days.append(row)

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

            async with self.get_async_session() as session:
                stmt = select(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date > check_datetime,
                    TradingCalendarDB.is_trading_day == True
                ).order_by(TradingCalendarDB.date).limit(1)
                result_proxy = await session.execute(stmt)
                result = result_proxy.scalar_one_or_none()
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

            async with self.get_async_session() as session:
                stmt = select(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date < check_datetime,
                    TradingCalendarDB.is_trading_day == True
                ).order_by(TradingCalendarDB.date.desc()).limit(1)
                result_proxy = await session.execute(stmt)
                result = result_proxy.scalar_one_or_none()
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

            async with self.get_async_session() as session:
                stmt = select(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date >= start_datetime,
                    TradingCalendarDB.date <= end_datetime
                ).limit(1)
                result = (await session.execute(stmt)).scalar_one_or_none()

                return result.is_trading_day if result else False

        except Exception as e:
            self.db_logger.error(f"Failed to check trading day for {exchange} {check_date}: {e}")
            return False

    # === Data Update Operations ===

    async def create_data_update(self, update_info: Dict[str, Any]) -> str:
        """创建数据更新记录"""
        try:
            async with self.get_async_session() as session:
                db_update = DataUpdateDB(**update_info)
                session.add(db_update)
                await session.commit()
                await session.refresh(db_update)
                return db_update.batch_id

        except Exception as e:
            self.db_logger.error(f"Failed to create data update record: {e}")
            return None

    async def update_data_update_progress(self, batch_id: str, progress: float, status: str = None) -> bool:
        """更新数据进度"""
        try:
            async with self.get_async_session() as session:
                stmt = select(DataUpdateDB).filter(
                    DataUpdateDB.batch_id == batch_id
                )
                result = await session.execute(stmt)
                update = result.scalar_one_or_none()
                if update:
                    update.progress = progress
                    if status:
                        update.status = status
                    if status == 'completed':
                        update.completed_at = get_shanghai_time()
                        update.duration_seconds = int((get_shanghai_time() - update.started_at).total_seconds())

                    await session.commit()
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
            async with self.get_async_session() as session:
                stmt = select(DataUpdateDB).order_by(DataUpdateDB.created_at.desc())

                if batch_id:
                    stmt = stmt.filter(DataUpdateDB.batch_id == batch_id)
                if status:
                    stmt = stmt.filter(DataUpdateDB.status == status)
                if limit:
                    stmt = stmt.limit(limit)

                result = await session.execute(stmt)
                updates = []
                for update in result.scalars().all():
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

    async def get_database_statistics(self, fast_mode: bool = False) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            stats = {}

            async with self.get_async_session() as session:
                # Instruments statistics
                stats['instruments'] = {
                    'total': await session.scalar(select(func.count()).select_from(InstrumentDB)),
                    'active': await session.scalar(select(func.count()).select_from(InstrumentDB).filter(InstrumentDB.is_active == True)),
                    'by_exchange': {},
                    'by_type': {},
                    'by_status': {}
                }

                # Exchange distribution
                exchange_counts_res = await session.execute(select(InstrumentDB.exchange, func.count(InstrumentDB.exchange)).group_by(InstrumentDB.exchange))
                for exchange, count in exchange_counts_res.all():
                    stats['instruments']['by_exchange'][exchange] = count

                # Type distribution
                type_counts_res = await session.execute(select(InstrumentDB.type, func.count(InstrumentDB.type)).group_by(InstrumentDB.type))
                for type_name, count in type_counts_res.all():
                    stats['instruments']['by_type'][type_name] = count

                # Status distribution
                status_counts_res = await session.execute(select(
                    InstrumentDB.status, func.count(InstrumentDB.status)
                ).group_by(InstrumentDB.status))
                for status, count in status_counts_res.all():
                    stats['instruments']['by_status'][status] = count

                # Daily quotes statistics
                stats['daily_quotes'] = {
                    'total': await session.scalar(select(func.count()).select_from(DailyQuoteDB)),
                    'by_trading_status': {},
                    'by_source': {},
                    'latest_date': None,
                    'earliest_date': None
                }

                # Trading status distribution
                if not fast_mode:
                    trade_status_res = await session.execute(select(
                        DailyQuoteDB.tradestatus, func.count(DailyQuoteDB.tradestatus)
                    ).group_by(DailyQuoteDB.tradestatus))
                    for status, count in trade_status_res.all():
                        stats['daily_quotes']['by_trading_status'][status] = count

                    # Source distribution
                    source_counts_res = await session.execute(select(
                        DailyQuoteDB.source, func.count(DailyQuoteDB.source)
                    ).group_by(DailyQuoteDB.source))
                    for source, count in source_counts_res.all():
                        stats['daily_quotes']['by_source'][source or 'unknown'] = count

                    # Date range
                    latest = await session.scalar(select(func.max(DailyQuoteDB.time)))
                    earliest = await session.scalar(select(func.min(DailyQuoteDB.time)))
                    stats['daily_quotes']['latest_date'] = latest
                    stats['daily_quotes']['earliest_date'] = earliest

                # Trading calendar statistics
                stats['trading_calendar'] = {
                    'total_records': await session.scalar(select(func.count()).select_from(TradingCalendarDB)),
                    'trading_days': await session.scalar(select(func.count()).select_from(TradingCalendarDB).filter(TradingCalendarDB.is_trading_day == True)),
                    'by_exchange': {}
                }
 
                for exchange in ['SSE', 'SZSE', 'BSE', 'HKEX']:
                    trading_days = await session.scalar(select(func.count()).select_from(TradingCalendarDB).filter(
                        TradingCalendarDB.exchange == exchange,
                        TradingCalendarDB.is_trading_day == True
                    ))
                    if trading_days > 0:
                        stats['trading_calendar']['by_exchange'][exchange] = trading_days

                # Data updates statistics
                stats['data_updates'] = {
                    'total': await session.scalar(select(func.count()).select_from(DataUpdateDB)),
                    'by_status': {},
                    'latest': None
                }

                # Status distribution
                update_status_res = await session.execute(select(
                    DataUpdateDB.status, func.count(DataUpdateDB.status)
                ).group_by(DataUpdateDB.status))
                for status, count in update_status_res.all():
                    stats['data_updates']['by_status'][status] = count

                # Latest update
                latest_res = await session.execute(select(DataUpdateDB).order_by(DataUpdateDB.created_at.desc()).limit(1))
                latest = latest_res.scalar_one_or_none()
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

    async def get_stats_supplement(self) -> Dict[str, Any]:
        """/stats 专用补充统计 (行业分布 + 交易日历/行情日期范围), 不动 get_database_statistics() (A3)。

        /stats 对 get_database_statistics() 使用 fast_mode=True 以跳过 daily_quotes 的
        by_trading_status/by_source group-by (在 2900万+ 行规模下合计耗时 ~35s, 且 /stats
        从不消费这两个字段, 纯浪费); 但 fast_mode 同时会跳过 daily_quotes 的 min/max(time),
        而这正是 quotes_date_range 需要的, 所以在此单独用一次轻量 min/max 查询补回。
        """
        try:
            async with self.get_async_session() as session:
                by_industry: Dict[str, int] = {}
                industry_res = await session.execute(
                    select(InstrumentDB.industry, func.count(InstrumentDB.industry))
                    .filter(InstrumentDB.industry.isnot(None))
                    .group_by(InstrumentDB.industry)
                )
                for industry, count in industry_res.all():
                    by_industry[industry] = count

                earliest = await session.scalar(select(func.min(TradingCalendarDB.date)))
                latest = await session.scalar(select(func.max(TradingCalendarDB.date)))

                quotes_earliest = await session.scalar(select(func.min(DailyQuoteDB.time)))
                quotes_latest = await session.scalar(select(func.max(DailyQuoteDB.time)))

                return {
                    'by_industry': by_industry,
                    'trading_calendar_earliest': earliest,
                    'trading_calendar_latest': latest,
                    'quotes_earliest': quotes_earliest,
                    'quotes_latest': quotes_latest,
                }
        except Exception as e:
            self.db_logger.error(f"Failed to get stats supplement: {e}")
            return {'by_industry': {}, 'trading_calendar_earliest': None, 'trading_calendar_latest': None}

    # === Database Maintenance ===

    async def optimize_database(self) -> bool:
        """优化数据库"""
        try:
            async with self.get_async_session() as session:
                await session.execute(text("VACUUM"))
                await session.execute(text("ANALYZE"))
                await session.commit()
                self.db_logger.info("Database optimization completed")
                return True

        except Exception as e:
            self.db_logger.error(f"Failed to optimize database: {e}")
            return False

    async def assess_data_quality(self, instrument_id: str, start_date: date, end_date: date) -> float:
        """评估指定品种在指定日期范围内的数据质量"""
        try:
            async with self.get_async_session() as session:
                stmt = select(DailyQuoteDB).filter(
                    DailyQuoteDB.instrument_id == instrument_id,
                    DailyQuoteDB.time >= start_date,
                    DailyQuoteDB.time <= end_date
                )
                count_stmt = select(func.count()).select_from(DailyQuoteDB).where(stmt.whereclause)
                total_records = await session.scalar(count_stmt)
                if total_records == 0:
                    return 0.0
                query = (await session.execute(stmt)).scalars().all()

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

    async def execute_read_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """执行只读SQL查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            List[Dict[str, Any]]: 查询结果列表
        """
        try:
            query_upper = query.strip().upper()
            if not query_upper.startswith('SELECT'):
                self.db_logger.warning(f"Blocked non-SELECT query in read operation: {query[:100]}...")
                return []
                
            async with self.get_async_session() as session:
                if params:
                    result = await session.execute(text(query), params)
                else:
                    result = await session.execute(text(query))
                
                rows = result.mappings().all()
                self.db_logger.debug(f"Successfully executed read query: {query[:100]}...")
                return [dict(row) for row in rows]
                
        except Exception as e:
            self.db_logger.error(f"Failed to execute read query '{query[:100]}...': {e}")
            return []

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
            
            async with self.get_async_session() as session:
                if params:
                    await session.execute(text(query), params)
                else:
                    await session.execute(text(query))
                await session.commit()

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

            async with self.get_async_session() as session:
                # 1. 检查重复的行情记录
                duplicate_stmt = select(
                    DailyQuoteDB.instrument_id,
                    DailyQuoteDB.time,
                    func.count(DailyQuoteDB.time).label('count')
                ).group_by(
                    DailyQuoteDB.instrument_id,
                    DailyQuoteDB.time
                ).having(
                    func.count(DailyQuoteDB.time) > 1)
                duplicate_quotes = len((await session.execute(duplicate_stmt)).all())

                if duplicate_quotes > 0:
                    validation_results['issues'].append({
                        'type': 'duplicate_quotes',
                        'count': duplicate_quotes,
                        'severity': 'high',
                        'description': f'Found {duplicate_quotes} duplicate quote records'
                    })
                    validation_results['total_issues'] += duplicate_quotes

                # 2. 检查无效的价格数据
                invalid_prices_stmt = select(func.count()).select_from(DailyQuoteDB).filter(
                    (DailyQuoteDB.high < DailyQuoteDB.low) |
                    (DailyQuoteDB.high < 0) |
                    (DailyQuoteDB.low < 0) |
                    (DailyQuoteDB.open < 0) |
                    (DailyQuoteDB.close < 0) |
                    (DailyQuoteDB.volume < 0) |
                    (DailyQuoteDB.amount < 0)
                )
                invalid_prices = await session.scalar(invalid_prices_stmt)

                if invalid_prices > 0:
                    validation_results['issues'].append({
                        'type': 'invalid_prices',
                        'count': invalid_prices,
                        'severity': 'high',
                        'description': f'Found {invalid_prices} records with invalid price/volume data'
                    })
                    validation_results['total_issues'] += invalid_prices

                # 3. 检查缺失的交易品种信息
                orphaned_stmt = select(func.count()).select_from(DailyQuoteDB).outerjoin(
                    InstrumentDB, DailyQuoteDB.instrument_id == InstrumentDB.instrument_id
                ).filter(InstrumentDB.instrument_id.is_(None))
                orphaned_quotes = await session.scalar(orphaned_stmt)

                if orphaned_quotes > 0:
                    validation_results['issues'].append({
                        'type': 'orphaned_quotes',
                        'count': orphaned_quotes,
                        'severity': 'medium',
                        'description': f'Found {orphaned_quotes} quote records without corresponding instrument'
                    })
                    validation_results['total_issues'] += orphaned_quotes

                # 4. 检查数据质量评分低于阈值的记录
                low_quality_stmt = select(func.count()).select_from(DailyQuoteDB).filter(
                    (DailyQuoteDB.quality_score < 0.5) |
                    (DailyQuoteDB.quality_score.is_(None))
                )
                low_quality_quotes = await session.scalar(low_quality_stmt)

                if low_quality_quotes > 0:
                    validation_results['warnings'].append({
                        'type': 'low_quality',
                        'count': low_quality_quotes,
                        'severity': 'low',
                        'description': f'Found {low_quality_quotes} records with low quality scores'
                    })

                # 5. 统计信息
                validation_results['statistics'] = {
                    'total_quotes': await session.scalar(select(func.count()).select_from(DailyQuoteDB)),
                    'total_instruments': await session.scalar(select(func.count()).select_from(InstrumentDB)),
                    'instruments_without_quotes': await session.scalar(select(func.count()).select_from(InstrumentDB).outerjoin(
                        DailyQuoteDB, InstrumentDB.instrument_id == DailyQuoteDB.instrument_id
                    ).filter(DailyQuoteDB.instrument_id.is_(None))),
                    'trading_calendar_records': await session.scalar(select(func.count()).select_from(TradingCalendarDB)),
                    'data_update_records': await session.scalar(select(func.count()).select_from(DataUpdateDB))
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
            async with self.get_async_session() as session:
                # 获取交易所的所有活跃交易品种
                stmt = select(InstrumentDB.instrument_id).filter(
                    InstrumentDB.exchange == exchange,
                    InstrumentDB.is_active == True
                )
                instrument_ids_res = await session.execute(stmt)
                instrument_ids = instrument_ids_res.scalars().all()

                if not instrument_ids:
                    return set()

                # 查询这些品种在指定日期范围内的数据
                stmt = select(DailyQuoteDB.time).filter(
                    DailyQuoteDB.instrument_id.in_(instrument_ids),
                    DailyQuoteDB.time >= datetime.combine(start_date, datetime.min.time()),
                    DailyQuoteDB.time <= datetime.combine(end_date, datetime.max.time())
                ).distinct()

                result = await session.execute(stmt)
                data_dates = set()
                for row in result.scalars().all():
                    if isinstance(row, datetime):
                        data_dates.add(row.date())
                    else:
                        data_dates.add(row)

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
            async with self.get_async_session() as session:
                stmt = select(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.date >= datetime.combine(start_date, datetime.min.time()),
                    TradingCalendarDB.date <= datetime.combine(end_date, datetime.max.time())
                ).order_by(TradingCalendarDB.date)

                records = []
                query = (await session.execute(stmt)).scalars().all()
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
            async with self.get_async_session() as session:
                stmt = select(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange
                ).order_by(TradingCalendarDB.date.desc()).limit(1)
                result = await session.execute(stmt)
                record = result.scalar_one_or_none()
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
            async with self.get_async_session() as session:
                # 总记录数
                total_days = await session.scalar(select(func.count()).select_from(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange
                ))

                # 交易日数
                trading_days = await session.scalar(select(func.count()).select_from(TradingCalendarDB).filter(
                    TradingCalendarDB.exchange == exchange,
                    TradingCalendarDB.is_trading_day == True
                ))

                # 非交易日数
                non_trading_days = total_days - trading_days

                # 日期范围
                earliest = await session.scalar(select(func.min(TradingCalendarDB.date)).filter(
                    TradingCalendarDB.exchange == exchange
                ))

                latest = await session.scalar(select(func.max(TradingCalendarDB.date)).filter(
                    TradingCalendarDB.exchange == exchange
                ))

                # 最后更新时间
                last_updated = await session.scalar(select(func.max(TradingCalendarDB.updated_at)).filter(
                    TradingCalendarDB.exchange == exchange
                ))

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


    # ------------------------------------------------------------------
    # 复权因子操作
    # ------------------------------------------------------------------

    async def save_adjustment_factors(
        self,
        factors: List[Dict[str, Any]],
        *,
        return_stats: bool = False,
    ) -> Union[int, Dict[str, int]]:
        """批量保存复权因子（upsert 语义）

        Args:
            factors: 复权因子列表, 每项含:
                instrument_id, ex_date, factor, cumulative_factor,
                dividend, bonus_shares, rights_shares, rights_price,
                event_type, source

        Returns:
            默认返回成功插入或语义更新的记录数; return_stats=True 时返回增量写入计数。
        """
        if not factors:
            return self._empty_write_stats() if return_stats else 0

        saved_count = 0
        stats = self._empty_write_stats()
        try:
            emit_changelog = self._is_changelog_enabled(
                "adjustment_factor", "adjustment_factors"
            )
            async with self.get_async_session() as session:
                for f in factors:
                    try:
                        payload = self._adjustment_factor_payload(f)
                        instrument_id = payload.get('instrument_id')
                        ex_date = payload.get('ex_date')
                        if not instrument_id or not ex_date:
                            stats["skipped"] += 1
                            continue
                        payload["row_hash"] = self._adjustment_factor_hash(payload)

                        # 检查是否已存在
                        stmt = select(AdjustmentFactorDB).where(
                            AdjustmentFactorDB.instrument_id == instrument_id,
                            AdjustmentFactorDB.ex_date == ex_date
                        )
                        result = await session.execute(stmt)
                        existing = result.scalar_one_or_none()

                        if existing:
                            old_hash = existing.row_hash or self._adjustment_factor_hash({
                                "instrument_id": existing.instrument_id,
                                "ex_date": existing.ex_date,
                                "factor": existing.factor,
                                "cumulative_factor": existing.cumulative_factor,
                                "dividend": existing.dividend,
                                "bonus_shares": existing.bonus_shares,
                                "rights_shares": existing.rights_shares,
                                "rights_price": existing.rights_price,
                                "event_type": existing.event_type,
                                "source": existing.source,
                            })
                            if old_hash == payload["row_hash"]:
                                if existing.row_hash is None:
                                    existing.row_hash = old_hash
                                if not existing.row_version:
                                    existing.row_version = 1
                                stats["unchanged"] += 1
                                saved_count += 1
                                continue

                            next_version = (existing.row_version or 1) + 1
                            for field in (
                                "factor", "cumulative_factor", "dividend",
                                "bonus_shares", "rights_shares", "rights_price",
                                "event_type", "source",
                            ):
                                setattr(existing, field, payload.get(field))
                            existing.row_hash = payload["row_hash"]
                            existing.row_version = next_version
                            existing.updated_at = get_shanghai_time()
                            if emit_changelog:
                                session.add(self._change_log_record(
                                    domain="adjustment_factor",
                                    dataset="adjustment_factors",
                                    change_type="update",
                                    business_key={
                                        "instrument_id": instrument_id,
                                        "ex_date": ex_date.date().isoformat(),
                                    },
                                    instrument_id=instrument_id,
                                    observation_date=ex_date,
                                    old_hash=old_hash,
                                    new_hash=payload["row_hash"],
                                    row_version=next_version,
                                    source=payload.get("source"),
                                ))
                                stats["changelog_written"] += 1
                            stats["changed"] += 1
                        else:
                            # 新增
                            new_record = AdjustmentFactorDB(
                                instrument_id=instrument_id,
                                ex_date=ex_date,
                                factor=payload["factor"],
                                cumulative_factor=payload["cumulative_factor"],
                                dividend=payload["dividend"],
                                bonus_shares=payload["bonus_shares"],
                                rights_shares=payload["rights_shares"],
                                rights_price=payload["rights_price"],
                                event_type=payload["event_type"],
                                source=payload["source"],
                                row_hash=payload["row_hash"],
                                row_version=1,
                            )
                            session.add(new_record)
                            if emit_changelog:
                                session.add(self._change_log_record(
                                    domain="adjustment_factor",
                                    dataset="adjustment_factors",
                                    change_type="insert",
                                    business_key={
                                        "instrument_id": instrument_id,
                                        "ex_date": ex_date.date().isoformat(),
                                    },
                                    instrument_id=instrument_id,
                                    observation_date=ex_date,
                                    old_hash=None,
                                    new_hash=payload["row_hash"],
                                    row_version=1,
                                    source=payload.get("source"),
                                ))
                                stats["changelog_written"] += 1
                            stats["inserted"] += 1

                        saved_count += 1
                    except Exception as row_e:
                        stats["failed"] += 1
                        self.db_logger.warning(
                            "Failed to save adjustment factor for %s: %s",
                            f.get('instrument_id'), row_e
                        )
                        continue

                await session.commit()

            self.db_logger.info(
                "Saved %d adjustment factors with CDC counters: %s",
                saved_count, stats
            )
            return stats if return_stats else saved_count

        except Exception as e:
            self.db_logger.error("Failed to save adjustment factors: %s", e)
            if return_stats:
                skipped = int(stats.get("skipped", 0) or 0)
                failed = int(stats.get("failed", 0) or 0)
                stats = self._empty_write_stats()
                stats["skipped"] = skipped
                stats["failed"] = max(failed, len(factors) - skipped)
                return stats
            return 0

    async def get_adjustment_factors(
        self,
        instrument_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """查询指定品种的复权因子

        Args:
            instrument_id: 品种ID
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）

        Returns:
            复权因子列表, 按 ex_date 升序
        """
        try:
            async with self.get_async_session() as session:
                from sqlalchemy import select
                stmt = select(AdjustmentFactorDB).where(
                    AdjustmentFactorDB.instrument_id == instrument_id
                )

                if start_date:
                    stmt = stmt.where(AdjustmentFactorDB.ex_date >= start_date)
                if end_date:
                    stmt = stmt.where(AdjustmentFactorDB.ex_date <= end_date)

                stmt = stmt.order_by(AdjustmentFactorDB.ex_date.asc())
                result = await session.execute(stmt)
                rows = result.scalars().all()

                return [
                    {
                        'instrument_id': r.instrument_id,
                        'ex_date': r.ex_date,
                        'factor': r.factor,
                        'cumulative_factor': r.cumulative_factor,
                        'dividend': r.dividend,
                        'bonus_shares': r.bonus_shares,
                        'rights_shares': r.rights_shares,
                        'rights_price': r.rights_price,
                        'event_type': r.event_type,
                        'source': r.source,
                    }
                    for r in rows
                ]

        except Exception as e:
            self.db_logger.error(
                "Failed to get adjustment factors for %s: %s",
                instrument_id, e
            )
            return []

    async def get_latest_cumulative_factor(
        self, instrument_id: str
    ) -> float:
        """获取指定品种最新的累积后复权因子

        Returns:
            最新的累积因子, 无记录时返回 1.0
        """
        try:
            async with self.get_async_session() as session:
                from sqlalchemy import select
                stmt = (
                    select(AdjustmentFactorDB.cumulative_factor)
                    .where(AdjustmentFactorDB.instrument_id == instrument_id)
                    .order_by(AdjustmentFactorDB.ex_date.desc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                row = result.scalar_one_or_none()
                return float(row) if row is not None else 1.0

        except Exception as e:
            self.db_logger.error(
                "Failed to get latest cumulative factor for %s: %s",
                instrument_id, e
            )
            return 1.0

    async def save_adjustment_factor_observations(
        self,
        observations: List[Dict[str, Any]],
        *,
        ingestion_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Upsert source-isolated factor observations."""
        stats = {"inserted": 0, "changed": 0, "unchanged": 0, "failed": 0}
        if not observations:
            return stats
        try:
            async with self.get_async_session() as session:
                for item in observations:
                    try:
                        instrument_id = str(item.get("instrument_id") or "").strip()
                        ex_date = self._coerce_datetime(item.get("ex_date"))
                        source = str(item.get("source") or "unknown").strip().lower()
                        source_profile = str(item.get("source_profile") or "default")
                        if not instrument_id or ex_date is None:
                            stats["failed"] += 1
                            continue
                        result = await session.execute(select(AdjustmentFactorObservationDB).where(
                            AdjustmentFactorObservationDB.instrument_id == instrument_id,
                            AdjustmentFactorObservationDB.ex_date == ex_date,
                            AdjustmentFactorObservationDB.source == source,
                            AdjustmentFactorObservationDB.source_profile == source_profile,
                        ))
                        existing = result.scalar_one_or_none()
                        values = {
                            "provider_factor": item.get("provider_factor"),
                            "provider_cumulative_factor": item.get("provider_cumulative_factor"),
                            "normalized_factor": item.get("normalized_factor"),
                            "normalization_version": str(item.get("normalization_version") or "event_ratio_v1"),
                            "quality_status": str(item.get("quality_status") or "unvalidated"),
                            "ingestion_run_id": ingestion_run_id or item.get("ingestion_run_id"),
                            "raw_payload_json": json.dumps(
                                item.get("raw_payload") or {}, ensure_ascii=True, default=str, sort_keys=True
                            ),
                        }
                        if existing is None:
                            session.add(AdjustmentFactorObservationDB(
                                instrument_id=instrument_id,
                                ex_date=ex_date,
                                source=source,
                                source_profile=source_profile,
                                **values,
                            ))
                            stats["inserted"] += 1
                            continue
                        changed = any(getattr(existing, key) != value for key, value in values.items())
                        if not changed:
                            stats["unchanged"] += 1
                            continue
                        for key, value in values.items():
                            setattr(existing, key, value)
                        existing.updated_at = get_shanghai_time()
                        stats["changed"] += 1
                    except Exception as row_error:
                        stats["failed"] += 1
                        self.db_logger.warning("Failed to save factor observation: %s", row_error)
                await session.commit()
            return stats
        except Exception as exc:
            self.db_logger.error("Failed to save factor observations: %s", exc)
            stats["failed"] = max(stats["failed"], len(observations))
            return stats

    async def replace_adjustment_factor_observations(
        self,
        observations: List[Dict[str, Any]],
        *,
        instrument_ids: List[str],
        source: str,
        source_profile: str,
        cleanup_source_event_keys: Optional[List[str]] = None,
        additional_keys: Optional[List[tuple[str, date]]] = None,
        ingestion_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Atomically replace emitted identities and explicitly superseded events."""
        affected = sorted({
            str(instrument_id).strip()
            for instrument_id in instrument_ids
            if str(instrument_id).strip()
        })
        normalized_source = str(source or "").strip().lower()
        normalized_profile = str(source_profile or "").strip()
        stats = {"deleted": 0, "inserted": 0, "failed": 0}
        if not affected or not normalized_source or not normalized_profile:
            return stats
        replacement_identity = {
            (str(instrument_id).strip(), self._coerce_datetime(ex_date))
            for instrument_id, ex_date in (additional_keys or [])
            if str(instrument_id).strip() in affected
            and self._coerce_datetime(ex_date) is not None
        }
        for item in observations:
            instrument_id = str(item.get("instrument_id") or "").strip()
            ex_date = self._coerce_datetime(item.get("ex_date"))
            if instrument_id in affected and ex_date is not None:
                replacement_identity.add((instrument_id, ex_date))
        cleanup_keys = sorted({
            str(value).strip()
            for value in (cleanup_source_event_keys or [])
            if str(value).strip()
        })
        scope = and_(
            AdjustmentFactorObservationDB.instrument_id.in_(affected),
            AdjustmentFactorObservationDB.source == normalized_source,
            AdjustmentFactorObservationDB.source_profile == normalized_profile,
        )
        cleanup_conditions = []
        if replacement_identity:
            cleanup_conditions.append(
                tuple_(
                    AdjustmentFactorObservationDB.instrument_id,
                    AdjustmentFactorObservationDB.ex_date,
                ).in_(sorted(replacement_identity))
            )
        cleanup_conditions.extend(
            AdjustmentFactorObservationDB.raw_payload_json.like(
                f'%"{event_key}"%'
            )
            for event_key in cleanup_keys
        )
        async with self.get_async_session() as session:
            if cleanup_conditions:
                result = await session.execute(
                    delete(AdjustmentFactorObservationDB).where(
                        scope,
                        or_(*cleanup_conditions),
                    )
                )
                stats["deleted"] = max(0, int(result.rowcount or 0))
            for item in observations:
                instrument_id = str(item.get("instrument_id") or "").strip()
                ex_date = self._coerce_datetime(item.get("ex_date"))
                item_source = str(
                    item.get("source") or normalized_source
                ).strip().lower()
                item_profile = str(
                    item.get("source_profile") or normalized_profile
                ).strip()
                if (
                    instrument_id not in affected
                    or ex_date is None
                    or item_source != normalized_source
                    or item_profile != normalized_profile
                ):
                    stats["failed"] += 1
                    continue
                session.add(AdjustmentFactorObservationDB(
                    instrument_id=instrument_id,
                    ex_date=ex_date,
                    source=normalized_source,
                    source_profile=normalized_profile,
                    provider_factor=item.get("provider_factor"),
                    provider_cumulative_factor=item.get(
                        "provider_cumulative_factor"
                    ),
                    normalized_factor=item.get("normalized_factor"),
                    normalization_version=str(
                        item.get("normalization_version") or "event_ratio_v1"
                    ),
                    quality_status=str(
                        item.get("quality_status") or "unvalidated"
                    ),
                    ingestion_run_id=ingestion_run_id
                    or item.get("ingestion_run_id"),
                    raw_payload_json=json.dumps(
                        item.get("raw_payload") or {},
                        ensure_ascii=True,
                        default=str,
                        sort_keys=True,
                    ),
                ))
                stats["inserted"] += 1
            await session.commit()
        return stats

    @staticmethod
    def _corporate_action_observation_values(
        row: Dict[str, Any],
        *,
        ingestion_run_id: Optional[str],
    ) -> Dict[str, Any]:
        """Build normalized values for one official corporate-action observation."""
        raw_payload = row.get("raw_payload") or {}
        values = {
            "action_type": str(row.get("action_type") or "unknown"),
            "fiscal_period": row.get("fiscal_period"),
            "announcement_date": DatabaseOperations._coerce_datetime(
                row.get("announcement_date")
            ),
            "record_date": DatabaseOperations._coerce_datetime(row.get("record_date")),
            "ex_date": DatabaseOperations._coerce_datetime(row.get("ex_date")),
            "pay_date": DatabaseOperations._coerce_datetime(row.get("pay_date")),
            "share_arrival_date": DatabaseOperations._coerce_datetime(
                row.get("share_arrival_date")
            ),
            "cash_dividend_per_share": row.get("cash_dividend_per_share"),
            "bonus_shares_per_share": row.get("bonus_shares_per_share"),
            "capitalization_shares_per_share": row.get(
                "capitalization_shares_per_share"
            ),
            "rights_shares_per_share": row.get("rights_shares_per_share"),
            "rights_price": row.get("rights_price"),
            "currency": str(row.get("currency") or "CNY"),
            "description": row.get("description"),
            "event_status": str(row.get("event_status") or "unvalidated"),
            "quality_status": str(row.get("quality_status") or "unvalidated"),
            "ingestion_run_id": ingestion_run_id,
            "raw_payload_json": json.dumps(
                raw_payload, ensure_ascii=True, default=str, sort_keys=True
            ),
        }
        hash_values = {
            key: value for key, value in values.items() if key != "ingestion_run_id"
        }
        values["row_hash"] = hashlib.sha256(
            json.dumps(
                hash_values, ensure_ascii=True, default=str, sort_keys=True
            ).encode("utf-8")
        ).hexdigest()
        return values

    async def save_corporate_action_observations(
        self,
        observations: List[Dict[str, Any]],
        *,
        ingestion_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Idempotently persist source-neutral corporate-action evidence."""
        stats = {
            "inserted": 0,
            "changed": 0,
            "unchanged": 0,
            "reactivated": 0,
            "failed": 0,
        }
        if not observations:
            return stats
        try:
            async with self.get_async_session() as session:
                for row in observations:
                    instrument_id = str(row.get("instrument_id") or "").strip()
                    source = str(row.get("source") or "").strip().lower()
                    source_profile = str(row.get("source_profile") or "").strip()
                    source_event_key = str(row.get("source_event_key") or "").strip()
                    if not all((instrument_id, source, source_profile, source_event_key)):
                        stats["failed"] += 1
                        continue
                    existing = (await session.execute(
                        select(CorporateActionObservationDB).where(
                            CorporateActionObservationDB.instrument_id == instrument_id,
                            CorporateActionObservationDB.source == source,
                            CorporateActionObservationDB.source_profile == source_profile,
                            CorporateActionObservationDB.source_event_key == source_event_key,
                        )
                    )).scalar_one_or_none()
                    values = self._corporate_action_observation_values(
                        row, ingestion_run_id=ingestion_run_id
                    )
                    if existing is None:
                        session.add(CorporateActionObservationDB(
                            instrument_id=instrument_id,
                            source=source,
                            source_profile=source_profile,
                            source_event_key=source_event_key,
                            is_current=True,
                            last_seen_run_id=ingestion_run_id,
                            **values,
                        ))
                        stats["inserted"] += 1
                    else:
                        was_current = bool(existing.is_current)
                        if existing.row_hash == values["row_hash"]:
                            existing.ingestion_run_id = ingestion_run_id
                            stats["unchanged"] += 1
                        else:
                            for key, value in values.items():
                                setattr(existing, key, value)
                            existing.row_version = int(existing.row_version or 1) + 1
                            stats["changed"] += 1
                        existing.is_current = True
                        existing.last_seen_run_id = ingestion_run_id
                        existing.retired_at = None
                        existing.retired_run_id = None
                        existing.retirement_reason = None
                        existing.updated_at = get_shanghai_time()
                        if not was_current:
                            stats["reactivated"] += 1
                await session.commit()
            return stats
        except Exception as exc:
            self.db_logger.error("Failed to save corporate-action observations: %s", exc)
            return {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "reactivated": 0,
                "failed": len(observations),
            }

    @staticmethod
    def _corporate_action_row_in_requested_range(
        row: CorporateActionObservationDB,
        start_date: date,
        end_date: date,
    ) -> bool:
        """Match persisted rows using the same temporal fallback as normalization."""
        candidate = row.ex_date or row.announcement_date
        if candidate is not None:
            candidate_date = candidate.date() if isinstance(candidate, datetime) else candidate
            return start_date <= candidate_date <= end_date
        fiscal_period = str(row.fiscal_period or "")
        year_match = re.search(r"(19|20)\d{2}", fiscal_period)
        return bool(
            year_match
            and start_date.year - 1 <= int(year_match.group(0)) <= end_date.year
        )

    async def reconcile_corporate_action_observation_snapshot(
        self,
        *,
        instrument_id: str,
        source: str,
        source_profile: str,
        requested_start_date: Union[str, date, datetime],
        requested_end_date: Union[str, date, datetime],
        seen_event_keys: List[str],
        ingestion_run_id: Optional[str],
    ) -> int:
        """Retire current rows absent from one complete endpoint snapshot."""
        normalized_source = str(source or "").strip().lower()
        normalized_profile = str(source_profile or "").strip()
        start_dt = self._coerce_datetime(requested_start_date)
        end_dt = self._coerce_datetime(requested_end_date)
        if not all((instrument_id, normalized_source, normalized_profile, start_dt, end_dt)):
            raise ValueError("snapshot identity and requested date range are required")
        start_value = start_dt.date()
        end_value = end_dt.date()
        if end_value < start_value:
            raise ValueError("requested_end_date must not precede requested_start_date")
        seen = {str(value).strip() for value in seen_event_keys if str(value).strip()}
        retired = 0
        async with self.get_async_session() as session:
            rows = (await session.execute(
                select(CorporateActionObservationDB).where(
                    CorporateActionObservationDB.instrument_id == instrument_id,
                    CorporateActionObservationDB.source == normalized_source,
                    CorporateActionObservationDB.source_profile == normalized_profile,
                    CorporateActionObservationDB.is_current.is_(True),
                )
            )).scalars().all()
            now = get_shanghai_time()
            for row in rows:
                if row.source_event_key in seen:
                    continue
                if not self._corporate_action_row_in_requested_range(
                    row, start_value, end_value
                ):
                    continue
                row.is_current = False
                row.retired_at = now
                row.retired_run_id = ingestion_run_id
                row.retirement_reason = "missing_from_complete_source_snapshot"
                row.updated_at = now
                retired += 1
            await session.commit()
        return retired

    async def upsert_corporate_action_instrument_status(
        self,
        row: Dict[str, Any],
    ) -> None:
        """Persist one endpoint-level official source coverage state."""
        instrument_id = str(row.get("instrument_id") or "").strip()
        source = str(row.get("source") or "").strip().lower()
        source_profile = str(row.get("source_profile") or "").strip()
        if not all((instrument_id, source, source_profile)):
            raise ValueError("instrument_id, source, and source_profile are required")
        requested_start_date = self._coerce_datetime(row.get("requested_start_date"))
        requested_end_date = self._coerce_datetime(row.get("requested_end_date"))
        if requested_start_date is None or requested_end_date is None:
            raise ValueError("requested_start_date and requested_end_date are required")
        if requested_end_date < requested_start_date:
            raise ValueError("requested_end_date must not precede requested_start_date")
        async with self.get_async_session() as session:
            existing = (await session.execute(
                select(CorporateActionInstrumentStatusDB).where(
                    CorporateActionInstrumentStatusDB.instrument_id == instrument_id,
                    CorporateActionInstrumentStatusDB.source == source,
                    CorporateActionInstrumentStatusDB.source_profile == source_profile,
                    CorporateActionInstrumentStatusDB.requested_start_date
                    == requested_start_date,
                    CorporateActionInstrumentStatusDB.requested_end_date
                    == requested_end_date,
                )
            )).scalar_one_or_none()
            values = {
                "coverage_status": str(row.get("coverage_status") or "indeterminate"),
                "event_count": int(row.get("event_count") or 0),
                "missing_ex_date_count": int(row.get("missing_ex_date_count") or 0),
                "requested_start_date": requested_start_date,
                "requested_end_date": requested_end_date,
                "earliest_event_date": self._coerce_datetime(
                    row.get("earliest_event_date")
                ),
                "latest_event_date": self._coerce_datetime(
                    row.get("latest_event_date")
                ),
                "error_message": row.get("error_message"),
                "ingestion_run_id": row.get("ingestion_run_id"),
                "last_attempt_at": get_shanghai_time(),
            }
            if existing is None:
                session.add(CorporateActionInstrumentStatusDB(
                    instrument_id=instrument_id,
                    source=source,
                    source_profile=source_profile,
                    **values,
                ))
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
            await session.commit()

    async def get_corporate_action_observations(
        self,
        *,
        instrument_id: Optional[str] = None,
        source_event_key: Optional[str] = None,
        source: Optional[str] = None,
        source_profile: Optional[str] = None,
        action_type: Optional[str] = None,
        quality_status: Optional[str] = None,
        include_inactive: bool = False,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return a paginated official corporate-action observation page."""
        async with self.get_async_session() as session:
            filters = []
            if instrument_id:
                filters.append(CorporateActionObservationDB.instrument_id == instrument_id)
            if source_event_key:
                filters.append(
                    CorporateActionObservationDB.source_event_key == source_event_key
                )
            if source:
                filters.append(CorporateActionObservationDB.source == source.lower())
            if source_profile:
                filters.append(
                    CorporateActionObservationDB.source_profile == source_profile
                )
            if action_type:
                filters.append(CorporateActionObservationDB.action_type == action_type)
            if quality_status:
                filters.append(
                    CorporateActionObservationDB.quality_status == quality_status
                )
            if not include_inactive:
                filters.append(CorporateActionObservationDB.is_current.is_(True))
            if start_date:
                filters.append(
                    CorporateActionObservationDB.ex_date
                    >= self._coerce_datetime(start_date)
                )
            if end_date:
                filters.append(
                    CorporateActionObservationDB.ex_date
                    < self._coerce_datetime(end_date + timedelta(days=1))
                )
            total = await session.scalar(
                select(func.count()).select_from(CorporateActionObservationDB).where(
                    *filters
                )
            )
            rows = (await session.execute(
                select(CorporateActionObservationDB)
                .where(*filters)
                .order_by(
                    CorporateActionObservationDB.instrument_id,
                    CorporateActionObservationDB.ex_date,
                    CorporateActionObservationDB.announcement_date,
                )
                .offset(offset)
                .limit(limit)
            )).scalars().all()
            items = [{
                "instrument_id": row.instrument_id,
                "source": row.source,
                "source_profile": row.source_profile,
                "source_event_key": row.source_event_key,
                "action_type": row.action_type,
                "fiscal_period": row.fiscal_period,
                "announcement_date": row.announcement_date,
                "record_date": row.record_date,
                "ex_date": row.ex_date,
                "pay_date": row.pay_date,
                "share_arrival_date": row.share_arrival_date,
                "cash_dividend_per_share": row.cash_dividend_per_share,
                "bonus_shares_per_share": row.bonus_shares_per_share,
                "capitalization_shares_per_share": (
                    row.capitalization_shares_per_share
                ),
                "rights_shares_per_share": row.rights_shares_per_share,
                "rights_price": row.rights_price,
                "currency": row.currency,
                "description": row.description,
                "event_status": row.event_status,
                "quality_status": row.quality_status,
                "ingestion_run_id": row.ingestion_run_id,
                "row_hash": row.row_hash,
                "row_version": row.row_version,
                "is_current": bool(row.is_current),
                "last_seen_run_id": row.last_seen_run_id,
                "retired_at": row.retired_at,
                "retired_run_id": row.retired_run_id,
                "retirement_reason": row.retirement_reason,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": limit,
                "offset": offset,
                "returned": len(items),
                "has_more": offset + len(items) < total_value,
                "items": items,
            }

    async def get_corporate_action_instrument_status_page(
        self,
        *,
        instrument_id: Optional[str] = None,
        source_profile: Optional[str] = None,
        coverage_status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return endpoint-level official source coverage states."""
        async with self.get_async_session() as session:
            filters = []
            if instrument_id:
                filters.append(
                    CorporateActionInstrumentStatusDB.instrument_id == instrument_id
                )
            if source_profile:
                filters.append(
                    CorporateActionInstrumentStatusDB.source_profile == source_profile
                )
            if coverage_status:
                filters.append(
                    CorporateActionInstrumentStatusDB.coverage_status == coverage_status
                )
            total = await session.scalar(
                select(func.count())
                .select_from(CorporateActionInstrumentStatusDB)
                .where(*filters)
            )
            rows = (await session.execute(
                select(CorporateActionInstrumentStatusDB)
                .where(*filters)
                .order_by(
                    CorporateActionInstrumentStatusDB.instrument_id,
                    CorporateActionInstrumentStatusDB.source_profile,
                    CorporateActionInstrumentStatusDB.requested_start_date,
                    CorporateActionInstrumentStatusDB.requested_end_date,
                )
                .offset(offset)
                .limit(limit)
            )).scalars().all()
            items = [{
                "instrument_id": row.instrument_id,
                "source": row.source,
                "source_profile": row.source_profile,
                "coverage_status": row.coverage_status,
                "event_count": row.event_count,
                "missing_ex_date_count": row.missing_ex_date_count,
                "requested_start_date": row.requested_start_date,
                "requested_end_date": row.requested_end_date,
                "earliest_event_date": row.earliest_event_date,
                "latest_event_date": row.latest_event_date,
                "error_message": row.error_message,
                "ingestion_run_id": row.ingestion_run_id,
                "last_attempt_at": row.last_attempt_at,
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": limit,
                "offset": offset,
                "returned": len(items),
                "has_more": offset + len(items) < total_value,
                "items": items,
            }

    async def save_corporate_action_effective_date_evidence(
        self,
        evidence_rows: List[Dict[str, Any]],
        *,
        ingestion_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Idempotently persist derived effective-date evidence."""
        stats = {"inserted": 0, "changed": 0, "unchanged": 0, "failed": 0}
        if not evidence_rows:
            return stats
        try:
            async with self.get_async_session() as session:
                for row in evidence_rows:
                    instrument_id = str(row.get("instrument_id") or "").strip()
                    source_event_key = str(row.get("source_event_key") or "").strip()
                    evidence_source = str(row.get("evidence_source") or "").strip()
                    evidence_key = str(row.get("evidence_key") or "").strip()
                    source_profile = str(row.get("source_profile") or "").strip()
                    if not all((
                        instrument_id,
                        source_event_key,
                        evidence_source,
                        evidence_key,
                        source_profile,
                    )):
                        stats["failed"] += 1
                        continue
                    raw_payload = row.get("raw_payload") or {}
                    resolution_status = str(
                        row.get("resolution_status") or "candidate"
                    ).strip().lower()
                    effective_date = self._coerce_datetime(row.get("effective_date"))
                    if resolution_status not in {"candidate", "resolved", "rejected"}:
                        stats["failed"] += 1
                        continue
                    if resolution_status == "resolved" and effective_date is None:
                        stats["failed"] += 1
                        continue
                    if resolution_status == "resolved" and not str(
                        row.get("date_basis") or ""
                    ).strip():
                        stats["failed"] += 1
                        continue
                    if resolution_status != "resolved":
                        effective_date = None
                    values = {
                        "observation_source": str(
                            row.get("observation_source") or "cninfo"
                        ).lower(),
                        "source_profile": source_profile,
                        "resolution_status": resolution_status,
                        "effective_date": effective_date,
                        "date_basis": row.get("date_basis"),
                        "announcement_id": row.get("announcement_id"),
                        "announcement_title": row.get("announcement_title"),
                        "announcement_time": self._coerce_datetime(
                            row.get("announcement_time")
                        ),
                        "evidence_url": row.get("evidence_url"),
                        "confidence": row.get("confidence"),
                        "ingestion_run_id": ingestion_run_id,
                        "raw_payload_json": json.dumps(
                            raw_payload,
                            ensure_ascii=True,
                            default=str,
                            sort_keys=True,
                        ),
                    }
                    hash_values = {
                        key: value
                        for key, value in values.items()
                        if key != "ingestion_run_id"
                    }
                    values["row_hash"] = hashlib.sha256(
                        json.dumps(
                            hash_values,
                            ensure_ascii=True,
                            default=str,
                            sort_keys=True,
                        ).encode("utf-8")
                    ).hexdigest()
                    existing = (await session.execute(
                        select(CorporateActionEffectiveDateEvidenceDB).where(
                            CorporateActionEffectiveDateEvidenceDB.instrument_id
                            == instrument_id,
                            CorporateActionEffectiveDateEvidenceDB.source_event_key
                            == source_event_key,
                            CorporateActionEffectiveDateEvidenceDB.evidence_source
                            == evidence_source,
                            CorporateActionEffectiveDateEvidenceDB.evidence_key
                            == evidence_key,
                        )
                    )).scalar_one_or_none()
                    if existing is None:
                        session.add(CorporateActionEffectiveDateEvidenceDB(
                            instrument_id=instrument_id,
                            source_event_key=source_event_key,
                            evidence_source=evidence_source,
                            evidence_key=evidence_key,
                            **values,
                        ))
                        stats["inserted"] += 1
                    elif (
                        resolution_status == "candidate"
                        and existing.resolution_status in {"resolved", "rejected"}
                    ):
                        existing.ingestion_run_id = ingestion_run_id
                        existing.updated_at = get_shanghai_time()
                        stats["unchanged"] += 1
                    elif existing.row_hash == values["row_hash"]:
                        existing.ingestion_run_id = ingestion_run_id
                        existing.updated_at = get_shanghai_time()
                        stats["unchanged"] += 1
                    else:
                        for key, value in values.items():
                            setattr(existing, key, value)
                        existing.updated_at = get_shanghai_time()
                        stats["changed"] += 1
                await session.commit()
            return stats
        except Exception as exc:
            self.db_logger.error(
                "Failed to save corporate-action effective-date evidence: %s", exc
            )
            return {
                "inserted": 0,
                "changed": 0,
                "unchanged": 0,
                "failed": len(evidence_rows),
            }

    async def save_corporate_action_document_bundle(
        self,
        artifact: Dict[str, Any],
        pages: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Idempotently persist one official document and its page text."""
        announcement_id = str(artifact.get("announcement_id") or "").strip()
        content_hash = str(artifact.get("content_hash") or "").strip().lower()
        source_url = str(artifact.get("source_url") or "").strip()
        archive_path = str(artifact.get("archive_path") or "").strip()
        if not all((announcement_id, content_hash, source_url, archive_path)):
            raise ValueError("document artifact identity and paths are required")
        parser_version = str(artifact.get("parser_version") or "").strip()
        if not parser_version:
            raise ValueError("document artifact parser_version is required")
        async with self.get_async_session() as session:
            existing = (await session.execute(
                select(CorporateActionDocumentArtifactDB).where(
                    CorporateActionDocumentArtifactDB.announcement_id == announcement_id,
                    CorporateActionDocumentArtifactDB.content_hash == content_hash,
                )
            )).scalar_one_or_none()
            values = {
                "announcement_id": announcement_id,
                "source": str(artifact.get("source") or "cninfo").strip().lower(),
                "source_url": source_url,
                "announcement_title": artifact.get("announcement_title"),
                "announcement_time": self._coerce_datetime(
                    artifact.get("announcement_time")
                ),
                "content_hash": content_hash,
                "content_type": artifact.get("content_type"),
                "content_length": int(artifact.get("content_length") or 0),
                "archive_path": archive_path,
                "download_status": str(
                    artifact.get("download_status") or "downloaded"
                ).strip(),
                "extraction_status": str(
                    artifact.get("extraction_status") or "pending"
                ).strip(),
                "parser_version": parser_version,
                "error_message": artifact.get("error_message"),
                "metadata_json": json.dumps(
                    artifact.get("metadata") or {},
                    ensure_ascii=True,
                    sort_keys=True,
                    default=str,
                ),
            }
            if existing is None:
                existing = CorporateActionDocumentArtifactDB(**values)
                session.add(existing)
                await session.flush()
                artifact_status = "inserted"
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
                artifact_status = "unchanged"
            artifact_id = int(existing.id)
            page_count = 0
            for page in pages:
                page_number = int(page.get("page_number") or 0)
                text_value = str(page.get("text") or "").strip()
                page_parser_version = str(
                    page.get("parser_version") or parser_version
                ).strip()
                text_hash = str(page.get("text_hash") or "").strip().lower()
                if page_number < 1 or not text_value or not text_hash:
                    continue
                page_row = (await session.execute(
                    select(CorporateActionDocumentPageDB).where(
                        CorporateActionDocumentPageDB.artifact_id == artifact_id,
                        CorporateActionDocumentPageDB.page_number == page_number,
                        CorporateActionDocumentPageDB.parser_version
                        == page_parser_version,
                    )
                )).scalar_one_or_none()
                page_values = {
                    "artifact_id": artifact_id,
                    "page_number": page_number,
                    "extraction_method": str(
                        page.get("extraction_method") or "native_text"
                    ).strip(),
                    "quality_status": str(
                        page.get("quality_status") or "usable"
                    ).strip(),
                    "text": text_value,
                    "text_hash": text_hash,
                    "character_count": len(text_value),
                    "parser_version": page_parser_version,
                }
                if page_row is None:
                    session.add(CorporateActionDocumentPageDB(**page_values))
                else:
                    for key, value in page_values.items():
                        setattr(page_row, key, value)
                    page_row.updated_at = get_shanghai_time()
                page_count += 1
            await session.commit()
            return {
                "artifact_id": artifact_id,
                "artifact_status": artifact_status,
                "page_count": page_count,
            }

    async def get_corporate_action_document_bundle(
        self,
        *,
        announcement_id: Optional[str] = None,
        content_hash: Optional[str] = None,
        source_event_key: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return document/page metadata, optionally linked to an event candidate."""
        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        async with self.get_async_session() as session:
            filters = []
            if announcement_id:
                filters.append(
                    CorporateActionDocumentArtifactDB.announcement_id
                    == announcement_id
                )
            if content_hash:
                filters.append(
                    CorporateActionDocumentArtifactDB.content_hash == content_hash
                )
            if source_event_key:
                filters.append(
                    select(CorporateActionEffectiveDateEvidenceDB.id).where(
                        CorporateActionEffectiveDateEvidenceDB.source_event_key
                        == source_event_key,
                        CorporateActionEffectiveDateEvidenceDB.announcement_id
                        == CorporateActionDocumentArtifactDB.announcement_id,
                    ).exists()
                )
            artifact_query = select(CorporateActionDocumentArtifactDB).where(*filters)
            total = await session.scalar(
                select(func.count()).select_from(
                    CorporateActionDocumentArtifactDB
                ).where(*filters)
            )
            artifacts = (await session.execute(
                artifact_query.order_by(
                    CorporateActionDocumentArtifactDB.announcement_time,
                    CorporateActionDocumentArtifactDB.id,
                ).offset(normalized_offset).limit(normalized_limit)
            )).scalars().all()
            items = []
            for artifact in artifacts:
                page_rows = (await session.execute(
                    select(CorporateActionDocumentPageDB).where(
                        CorporateActionDocumentPageDB.artifact_id == artifact.id
                    ).order_by(CorporateActionDocumentPageDB.page_number)
                )).scalars().all()
                item = {
                    "artifact_id": artifact.id,
                    "announcement_id": artifact.announcement_id,
                    "source": artifact.source,
                    "source_url": artifact.source_url,
                    "announcement_title": artifact.announcement_title,
                    "announcement_time": artifact.announcement_time,
                    "content_hash": artifact.content_hash,
                    "content_type": artifact.content_type,
                    "content_length": artifact.content_length,
                    "archive_path": artifact.archive_path,
                    "download_status": artifact.download_status,
                    "extraction_status": artifact.extraction_status,
                    "parser_version": artifact.parser_version,
                    "error_message": artifact.error_message,
                    "metadata": json.loads(artifact.metadata_json or "{}"),
                    "pages": [{
                        "page_id": page.id,
                        "page_number": page.page_number,
                        "extraction_method": page.extraction_method,
                        "quality_status": page.quality_status,
                        "text": page.text,
                        "text_hash": page.text_hash,
                        "character_count": page.character_count,
                        "parser_version": page.parser_version,
                    } for page in page_rows],
                }
                items.append(item)
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(artifacts) < total_value,
                "items": items,
            }

    async def save_corporate_action_llm_analysis(
        self,
        row: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Persist one versioned candidate analysis idempotently."""
        analysis_key = str(row.get("analysis_key") or "").strip()
        instrument_id = str(row.get("instrument_id") or "").strip()
        source_event_key = str(row.get("source_event_key") or "").strip()
        input_hash = str(row.get("input_hash") or "").strip().lower()
        if not all((analysis_key, instrument_id, source_event_key, input_hash)):
            raise ValueError("LLM analysis identity is required")
        values = {
            "analysis_key": analysis_key,
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "analysis_status": str(row.get("analysis_status") or "manual_required"),
            "validation_status": str(row.get("validation_status") or "failed"),
            "profile": str(row.get("profile") or "semantic_extraction"),
            "model": row.get("model"),
            "schema_version": str(row.get("schema_version") or ""),
            "prompt_version": str(row.get("prompt_version") or ""),
            "parser_version": str(row.get("parser_version") or ""),
            "input_hash": input_hash,
            "response_hash": row.get("response_hash"),
            "request_id": row.get("request_id"),
            "artifact_ids_json": json.dumps(
                row.get("artifact_ids") or [], ensure_ascii=True, sort_keys=True
            ),
            "result_json": json.dumps(
                row.get("result") or {}, ensure_ascii=True, sort_keys=True, default=str
            ),
            "gate_results_json": json.dumps(
                row.get("gate_results") or {},
                ensure_ascii=True,
                sort_keys=True,
                default=str,
            ),
            "usage_json": json.dumps(
                row.get("usage") or {}, ensure_ascii=True, sort_keys=True, default=str
            ),
            "latency_ms": row.get("latency_ms"),
            "attempt_count": int(row.get("attempt_count") or 0),
            "error_code": row.get("error_code"),
            "error_message": row.get("error_message"),
            "ingestion_run_id": row.get("ingestion_run_id"),
        }
        async with self.get_async_session() as session:
            existing = await session.scalar(
                select(CorporateActionLlmAnalysisDB).where(
                    CorporateActionLlmAnalysisDB.analysis_key == analysis_key
                )
            )
            if existing is None:
                existing = CorporateActionLlmAnalysisDB(**values)
                session.add(existing)
                status = "inserted"
            elif (
                values["validation_status"] == "failed"
                and existing.validation_status != "failed"
            ):
                status = "unchanged"
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
                status = "updated"
            await session.flush()
            analysis_id = int(existing.id)
            await session.commit()
            return {"analysis_id": analysis_id, "status": status}

    async def get_corporate_action_llm_analyses(
        self,
        *,
        instrument_id: Optional[str] = None,
        source_event_key: Optional[str] = None,
        validation_status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return paginated versioned LLM analysis results."""
        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        async with self.get_async_session() as session:
            filters = []
            if instrument_id:
                filters.append(CorporateActionLlmAnalysisDB.instrument_id == instrument_id)
            if source_event_key:
                filters.append(
                    CorporateActionLlmAnalysisDB.source_event_key == source_event_key
                )
            if validation_status:
                filters.append(
                    CorporateActionLlmAnalysisDB.validation_status == validation_status
                )
            total = await session.scalar(
                select(func.count()).select_from(CorporateActionLlmAnalysisDB).where(*filters)
            )
            rows = (await session.execute(
                select(CorporateActionLlmAnalysisDB).where(*filters).order_by(
                    CorporateActionLlmAnalysisDB.created_at.desc(),
                    CorporateActionLlmAnalysisDB.id.desc(),
                ).offset(normalized_offset).limit(normalized_limit)
            )).scalars().all()
            items = [{
                "analysis_id": row.id,
                "analysis_key": row.analysis_key,
                "instrument_id": row.instrument_id,
                "source_event_key": row.source_event_key,
                "analysis_status": row.analysis_status,
                "validation_status": row.validation_status,
                "profile": row.profile,
                "model": row.model,
                "schema_version": row.schema_version,
                "prompt_version": row.prompt_version,
                "parser_version": row.parser_version,
                "input_hash": row.input_hash,
                "response_hash": row.response_hash,
                "request_id": row.request_id,
                "artifact_ids": json.loads(row.artifact_ids_json or "[]"),
                "result": json.loads(row.result_json or "{}"),
                "gate_results": json.loads(row.gate_results_json or "{}"),
                "usage": json.loads(row.usage_json or "{}"),
                "latency_ms": row.latency_ms,
                "attempt_count": row.attempt_count,
                "error_code": row.error_code,
                "error_message": row.error_message,
                "ingestion_run_id": row.ingestion_run_id,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(items) < total_value,
                "items": items,
            }

    async def get_corporate_action_review_queue(
        self,
        *,
        instrument_id: Optional[str] = None,
        validation_status: Optional[str] = None,
        review_tier: Optional[str] = None,
        failed_gate: Optional[str] = None,
        gate_signature: Optional[str] = None,
        source_profile: Optional[str] = None,
        action_type: Optional[str] = None,
        event_type: Optional[str] = None,
        reviewed_state: Optional[str] = None,
        include_machine_rework: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return latest-analysis review cards with compact official lineage."""
        from data_sources.cninfo_special_action_resolution import (
            deterministic_title_match,
        )

        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        normalized_reviewed_state = str(reviewed_state or "").strip().lower()
        if normalized_reviewed_state not in {"", "reviewed", "unreviewed"}:
            raise ValueError("reviewed_state must be reviewed or unreviewed")
        normalized_failed_gate = str(failed_gate or "").strip()
        if normalized_failed_gate and not re.fullmatch(
            r"[A-Za-z][A-Za-z0-9_]*", normalized_failed_gate
        ):
            raise ValueError("failed_gate contains unsupported characters")

        latest_analysis = select(
            func.max(CorporateActionLlmAnalysisDB.id).label("analysis_id")
        ).group_by(
            CorporateActionLlmAnalysisDB.instrument_id,
            CorporateActionLlmAnalysisDB.source_event_key,
        ).subquery()
        reviewed_exists = select(CorporateActionResolutionReviewDB.id).where(
            CorporateActionResolutionReviewDB.analysis_id
            == CorporateActionLlmAnalysisDB.id
        ).exists()
        active_candidate_exists = select(
            CorporateActionEffectiveDateEvidenceDB.id
        ).where(
            CorporateActionEffectiveDateEvidenceDB.instrument_id
            == CorporateActionLlmAnalysisDB.instrument_id,
            CorporateActionEffectiveDateEvidenceDB.source_event_key
            == CorporateActionLlmAnalysisDB.source_event_key,
            CorporateActionEffectiveDateEvidenceDB.evidence_source
            == "cninfo_announcement_metadata",
            CorporateActionEffectiveDateEvidenceDB.resolution_status == "candidate",
        ).exists()
        no_current_candidate_state_exists = select(
            CorporateActionResolutionStateDB.id
        ).where(
            CorporateActionResolutionStateDB.instrument_id
            == CorporateActionLlmAnalysisDB.instrument_id,
            CorporateActionResolutionStateDB.source_event_key
            == CorporateActionLlmAnalysisDB.source_event_key,
            CorporateActionResolutionStateDB.state_reason
            == "no_current_implementation_candidate",
        ).exists()
        review_tier_expression = case(
            (
                CorporateActionLlmAnalysisDB.validation_status == "failed",
                literal("machine_rework"),
            ),
            else_=func.coalesce(
                func.json_extract(
                    CorporateActionLlmAnalysisDB.result_json,
                    "$._review_classification.review_tier",
                ),
                literal("deep_review"),
            ),
        )
        gate_signature_expression = case(
            (
                CorporateActionLlmAnalysisDB.validation_status == "failed",
                func.coalesce(
                    CorporateActionLlmAnalysisDB.error_code,
                    literal("analysis_failed"),
                ),
            ),
            else_=func.coalesce(
                func.json_extract(
                    CorporateActionLlmAnalysisDB.result_json,
                    "$._review_classification.gate_signature",
                ),
                literal(""),
            ),
        )
        filters = [
            CorporateActionLlmAnalysisDB.id == latest_analysis.c.analysis_id,
            CorporateActionObservationDB.instrument_id
            == CorporateActionLlmAnalysisDB.instrument_id,
            CorporateActionObservationDB.source_event_key
            == CorporateActionLlmAnalysisDB.source_event_key,
            CorporateActionObservationDB.source == "cninfo",
            CorporateActionObservationDB.is_current.is_(True),
            or_(
                CorporateActionLlmAnalysisDB.validation_status == "failed",
                and_(
                    active_candidate_exists,
                    ~no_current_candidate_state_exists,
                ),
            ),
        ]
        if instrument_id:
            filters.append(
                CorporateActionLlmAnalysisDB.instrument_id == instrument_id
            )
        if validation_status:
            filters.append(
                CorporateActionLlmAnalysisDB.validation_status == validation_status
            )
        if source_profile:
            filters.append(
                CorporateActionObservationDB.source_profile == source_profile
            )
        if action_type:
            filters.append(CorporateActionObservationDB.action_type == action_type)
        if not include_machine_rework:
            filters.append(review_tier_expression != "machine_rework")
        if review_tier:
            filters.append(review_tier_expression == review_tier)
        if normalized_failed_gate:
            filters.append(
                func.json_extract(
                    CorporateActionLlmAnalysisDB.gate_results_json,
                    f'$."{normalized_failed_gate}"',
                ) == 0
            )
        if gate_signature:
            filters.append(gate_signature_expression == gate_signature)
        if event_type:
            filters.append(
                func.json_extract(
                    CorporateActionLlmAnalysisDB.result_json,
                    "$.event_type",
                ) == event_type
            )
        if normalized_reviewed_state == "reviewed":
            filters.append(reviewed_exists)
        elif normalized_reviewed_state == "unreviewed":
            filters.append(~reviewed_exists)

        async with self.get_async_session() as session:
            base_query = select(CorporateActionLlmAnalysisDB.id).select_from(
                CorporateActionLlmAnalysisDB
            ).join(
                latest_analysis,
                CorporateActionLlmAnalysisDB.id == latest_analysis.c.analysis_id,
            ).join(
                CorporateActionObservationDB,
                CorporateActionObservationDB.source_event_key
                == CorporateActionLlmAnalysisDB.source_event_key,
            ).where(*filters)
            total_value = int(await session.scalar(
                select(func.count()).select_from(base_query.subquery())
            ) or 0)
            tier_order_expression = case(
                (review_tier_expression == "quick_review", 0),
                (review_tier_expression == "deep_review", 1),
                (review_tier_expression == "machine_rework", 2),
                else_=3,
            )
            rows = (await session.execute(
                select(
                    CorporateActionLlmAnalysisDB,
                    CorporateActionObservationDB,
                    reviewed_exists.label("is_reviewed"),
                    review_tier_expression.label("review_tier"),
                    gate_signature_expression.label("gate_signature"),
                ).select_from(CorporateActionLlmAnalysisDB).join(
                    latest_analysis,
                    CorporateActionLlmAnalysisDB.id
                    == latest_analysis.c.analysis_id,
                ).join(
                    CorporateActionObservationDB,
                    CorporateActionObservationDB.source_event_key
                    == CorporateActionLlmAnalysisDB.source_event_key,
                ).where(*filters).order_by(
                    tier_order_expression,
                    CorporateActionLlmAnalysisDB.created_at,
                    CorporateActionLlmAnalysisDB.id,
                ).offset(normalized_offset).limit(normalized_limit)
            )).all()

            candidates: list[dict[str, Any]] = []
            for analysis, observation, is_reviewed, item_tier, item_signature in rows:
                result = json.loads(analysis.result_json or "{}")
                gates = json.loads(analysis.gate_results_json or "{}")
                classification = result.get("_review_classification") or {}
                if analysis.validation_status == "failed":
                    item_tier = str(item_tier or "machine_rework")
                    item_signature = str(item_signature or "analysis_failed")
                    review_reasons = [f"analysis_error:{item_signature}"]
                    reason_codes = [
                        "provider_retryable"
                        if any(
                            marker in item_signature
                            for marker in ("retry", "transport", "timeout")
                        )
                        else "provider_or_pipeline_error"
                    ]
                    operator_summary = [
                        "LLM/传输阶段失败，可重试，不应进入人工事实判断。"
                        if reason_codes[0] == "provider_retryable"
                        else "流水线失败，需先修复机器阶段。"
                    ]
                else:
                    item_tier = str(item_tier or "deep_review")
                    item_signature = str(item_signature or "")
                    review_reasons = list(
                        classification.get("review_reasons") or []
                    )
                    reason_codes = list(classification.get("reason_codes") or [])
                    operator_summary = list(
                        classification.get("operator_summary") or []
                    )
                failed_gates = sorted(
                    name for name, passed in gates.items() if not bool(passed)
                )
                candidates.append({
                    "analysis": analysis,
                    "observation": observation,
                    "is_reviewed": bool(is_reviewed),
                    "result": result,
                    "gates": gates,
                    "review_tier": item_tier,
                    "gate_signature": item_signature,
                    "review_reasons": review_reasons,
                    "reason_codes": reason_codes,
                    "operator_summary": operator_summary,
                    "failed_gates": failed_gates,
                })
            selected = candidates
            analysis_ids = [item["analysis"].id for item in selected]
            event_keys = [item["analysis"].source_event_key for item in selected]
            artifact_ids = sorted({
                int(artifact_id)
                for item in selected
                for artifact_id in json.loads(
                    item["analysis"].artifact_ids_json or "[]"
                )
                if str(artifact_id).isdigit()
            })

            reviews_by_analysis: dict[int, list[dict[str, Any]]] = defaultdict(list)
            if analysis_ids:
                review_rows = (await session.execute(
                    select(CorporateActionResolutionReviewDB).where(
                        CorporateActionResolutionReviewDB.analysis_id.in_(analysis_ids)
                    ).order_by(
                        CorporateActionResolutionReviewDB.created_at,
                        CorporateActionResolutionReviewDB.id,
                    )
                )).scalars().all()
                for review in review_rows:
                    reviews_by_analysis[int(review.analysis_id)].append({
                        "review_id": review.id,
                        "decision": review.decision,
                        "reviewer": review.reviewer,
                        "effective_date": review.effective_date,
                        "date_basis": review.date_basis,
                        "notes": review.notes,
                        "created_at": review.created_at,
                    })

            announcements_by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
            observations_by_event = {
                item["analysis"].source_event_key: item["observation"]
                for item in selected
            }
            if event_keys:
                announcement_rows = (await session.execute(
                    select(CorporateActionEffectiveDateEvidenceDB).where(
                        CorporateActionEffectiveDateEvidenceDB.source_event_key.in_(
                            event_keys
                        ),
                        CorporateActionEffectiveDateEvidenceDB.evidence_source
                        == "cninfo_announcement_metadata",
                    ).order_by(
                        CorporateActionEffectiveDateEvidenceDB.announcement_time,
                        CorporateActionEffectiveDateEvidenceDB.id,
                    )
                )).scalars().all()
                for evidence in announcement_rows:
                    raw_payload = {}
                    try:
                        parsed_payload = json.loads(
                            evidence.raw_payload_json or "{}"
                        )
                        if isinstance(parsed_payload, dict):
                            raw_payload = parsed_payload
                    except (TypeError, ValueError, json.JSONDecodeError):
                        raw_payload = {}
                    persisted_deterministic = raw_payload.get(
                        "deterministic_match"
                    )
                    observation = observations_by_event.get(
                        evidence.source_event_key
                    )
                    current_deterministic = deterministic_title_match(
                        "",
                        getattr(observation, "fiscal_period", None),
                        evidence.announcement_title,
                        getattr(observation, "action_type", None),
                    )
                    deterministic = (
                        current_deterministic
                        if current_deterministic.get("status") != "accepted"
                        or not isinstance(persisted_deterministic, dict)
                        else persisted_deterministic
                    )
                    effective_status = evidence.resolution_status
                    if (
                        effective_status == "candidate"
                        and deterministic.get("status") != "accepted"
                    ):
                        effective_status = "rejected_by_current_policy"
                    announcements_by_event[evidence.source_event_key].append({
                        "evidence_key": evidence.evidence_key,
                        "announcement_id": evidence.announcement_id,
                        "announcement_title": evidence.announcement_title,
                        "announcement_time": evidence.announcement_time,
                        "evidence_url": evidence.evidence_url,
                        "resolution_status": evidence.resolution_status,
                        "effective_status": effective_status,
                        "selection_reasons": raw_payload.get(
                            "selection_reasons"
                        ) or [],
                        "deterministic_match": deterministic,
                        "title_classification": raw_payload.get(
                            "title_classification"
                        ),
                        "search_windows": raw_payload.get(
                            "search_windows"
                        ) or [],
                    })

            artifacts_by_id: dict[int, dict[str, Any]] = {}
            if artifact_ids:
                artifact_rows = (await session.execute(
                    select(CorporateActionDocumentArtifactDB).where(
                        CorporateActionDocumentArtifactDB.id.in_(artifact_ids)
                    )
                )).scalars().all()
                page_rows = (await session.execute(
                    select(CorporateActionDocumentPageDB).where(
                        CorporateActionDocumentPageDB.artifact_id.in_(artifact_ids)
                    )
                )).scalars().all()
                page_metadata: dict[int, list[dict[str, Any]]] = defaultdict(list)
                for page in page_rows:
                    page_metadata[page.artifact_id].append({
                        "page_number": page.page_number,
                        "quality_status": page.quality_status,
                        "text_hash": page.text_hash,
                        "extraction_method": page.extraction_method,
                    })
                for artifact in artifact_rows:
                    artifacts_by_id[artifact.id] = {
                        "artifact_id": artifact.id,
                        "announcement_id": artifact.announcement_id,
                        "announcement_title": artifact.announcement_title,
                        "announcement_time": artifact.announcement_time,
                        "source_url": artifact.source_url,
                        "content_hash": artifact.content_hash,
                        "pages": sorted(
                            page_metadata.get(artifact.id, []),
                            key=lambda page: page["page_number"],
                        ),
                    }

            items = []
            for candidate in selected:
                analysis = candidate["analysis"]
                observation = candidate["observation"]
                result = candidate["result"]
                ids = json.loads(analysis.artifact_ids_json or "[]")
                items.append({
                    "analysis_id": analysis.id,
                    "instrument_id": analysis.instrument_id,
                    "source_event_key": analysis.source_event_key,
                    "validation_status": analysis.validation_status,
                    "review_tier": candidate["review_tier"],
                    "gate_signature": candidate["gate_signature"],
                    "review_reasons": candidate["review_reasons"],
                    "reason_codes": candidate["reason_codes"],
                    "operator_summary": candidate["operator_summary"],
                    "failed_gates": candidate["failed_gates"],
                    "reviewed": candidate["is_reviewed"],
                    "source": {
                        "source_profile": observation.source_profile,
                        "action_type": observation.action_type,
                        "fiscal_period": observation.fiscal_period,
                        "announcement_date": observation.announcement_date,
                        "record_date": observation.record_date,
                        "ex_date": observation.ex_date,
                        "pay_date": observation.pay_date,
                        "share_arrival_date": observation.share_arrival_date,
                        "cash_dividend_per_share": observation.cash_dividend_per_share,
                        "bonus_shares_per_share": observation.bonus_shares_per_share,
                        "capitalization_shares_per_share": (
                            observation.capitalization_shares_per_share
                        ),
                        "rights_shares_per_share": observation.rights_shares_per_share,
                        "rights_price": observation.rights_price,
                        "currency": observation.currency,
                        "description": str(observation.description or "")[:1000] or None,
                    },
                    "proposed": {
                        key: result.get(key)
                        for key in (
                            "event_type", "event_stage", "effective_date",
                            "effective_date_type", "date_basis",
                            "economic_terms", "alternative_dates", "conflicts",
                            "confidence", "reason",
                        )
                    },
                    "evidence": list(result.get("evidence") or []),
                    "announcements": announcements_by_event.get(
                        analysis.source_event_key, []
                    ),
                    "artifacts": [
                        artifacts_by_id[int(artifact_id)]
                        for artifact_id in ids
                        if str(artifact_id).isdigit()
                        and int(artifact_id) in artifacts_by_id
                    ],
                    "usage": json.loads(analysis.usage_json or "{}"),
                    "latency_ms": analysis.latency_ms,
                    "attempt_count": analysis.attempt_count,
                    "lineage": {
                        "profile": analysis.profile,
                        "model": analysis.model,
                        "schema_version": analysis.schema_version,
                        "prompt_version": analysis.prompt_version,
                        "parser_version": analysis.parser_version,
                        "input_hash": analysis.input_hash,
                        "response_hash": analysis.response_hash,
                        "request_id": analysis.request_id,
                        "created_at": analysis.created_at,
                    },
                    "prior_reviews": reviews_by_analysis.get(analysis.id, []),
                })
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(items) < total_value,
                "items": items,
            }

    async def save_corporate_action_resolution_review(
        self,
        row: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Persist an explicit resolution review with an idempotent review key."""
        review_key = str(row.get("review_key") or "").strip()
        instrument_id = str(row.get("instrument_id") or "").strip()
        source_event_key = str(row.get("source_event_key") or "").strip()
        reviewer = str(row.get("reviewer") or "").strip()
        decision = str(row.get("decision") or "").strip().lower()
        if not all((review_key, instrument_id, source_event_key, reviewer)):
            raise ValueError("review identity is required")
        if decision not in {"resolved", "rejected", "conflict", "manual_required"}:
            raise ValueError("unsupported corporate-action review decision")
        effective_date = self._coerce_datetime(row.get("effective_date"))
        if decision == "resolved" and not effective_date:
            raise ValueError("resolved review requires effective_date")
        if decision == "resolved" and not str(row.get("date_basis") or "").strip():
            raise ValueError("resolved review requires date_basis")
        values = {
            "review_key": review_key,
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "analysis_id": row.get("analysis_id"),
            "evidence_key": row.get("evidence_key"),
            "decision": decision,
            "effective_date": effective_date,
            "date_basis": row.get("date_basis"),
            "reviewer": reviewer,
            "notes": row.get("notes"),
            "review_payload_json": json.dumps(
                row.get("review_payload") or {},
                ensure_ascii=True,
                sort_keys=True,
                default=str,
            ),
            "supersedes_review_id": row.get("supersedes_review_id"),
        }
        async with self.get_async_session() as session:
            existing = await session.scalar(
                select(CorporateActionResolutionReviewDB).where(
                    CorporateActionResolutionReviewDB.review_key == review_key
                )
            )
            if existing is None:
                existing = CorporateActionResolutionReviewDB(**values)
                session.add(existing)
                status = "inserted"
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
                status = "updated"
            await session.flush()
            review_id = int(existing.id)
            await session.commit()
            return {"review_id": review_id, "status": status}

    async def get_corporate_action_resolution_reviews(
        self,
        *,
        source_event_key: Optional[str] = None,
        decision: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return explicit company-action review decisions."""
        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        async with self.get_async_session() as session:
            filters = []
            if source_event_key:
                filters.append(
                    CorporateActionResolutionReviewDB.source_event_key == source_event_key
                )
            if decision:
                filters.append(CorporateActionResolutionReviewDB.decision == decision)
            total = await session.scalar(
                select(func.count()).select_from(CorporateActionResolutionReviewDB).where(*filters)
            )
            rows = (await session.execute(
                select(CorporateActionResolutionReviewDB).where(*filters).order_by(
                    CorporateActionResolutionReviewDB.created_at.desc(),
                    CorporateActionResolutionReviewDB.id.desc(),
                ).offset(normalized_offset).limit(normalized_limit)
            )).scalars().all()
            items = [{
                "review_id": row.id,
                "review_key": row.review_key,
                "instrument_id": row.instrument_id,
                "source_event_key": row.source_event_key,
                "analysis_id": row.analysis_id,
                "evidence_key": row.evidence_key,
                "decision": row.decision,
                "effective_date": row.effective_date,
                "date_basis": row.date_basis,
                "reviewer": row.reviewer,
                "notes": row.notes,
                "review_payload": json.loads(row.review_payload_json or "{}"),
                "supersedes_review_id": row.supersedes_review_id,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(items) < total_value,
                "items": items,
            }

    async def save_corporate_action_resolved_terms(
        self,
        row: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Upsert the latest explicitly reviewed economic-term overlay."""
        instrument_id = str(row.get("instrument_id") or "").strip()
        source_event_key = str(row.get("source_event_key") or "").strip()
        analysis_id = int(row.get("analysis_id") or 0)
        review_id = int(row.get("review_id") or 0)
        if not all((instrument_id, source_event_key, analysis_id, review_id)):
            raise ValueError("resolved-term review identity is required")
        values = {
            "analysis_id": analysis_id,
            "review_id": review_id,
            "cash_dividend_per_share": row.get("cash_dividend_per_share"),
            "bonus_shares_per_share": row.get("bonus_shares_per_share"),
            "capitalization_shares_per_share": row.get("capitalization_shares_per_share"),
            "rights_shares_per_share": row.get("rights_shares_per_share"),
            "rights_price": row.get("rights_price"),
            "currency": row.get("currency"),
            "is_active": bool(row.get("is_active", True)),
            "resolved_fields_json": json.dumps(
                sorted({str(item) for item in row.get("resolved_fields", []) if str(item)}),
                ensure_ascii=True,
            ),
            "evidence_json": json.dumps(
                row.get("evidence") or {}, ensure_ascii=True, sort_keys=True, default=str
            ),
        }
        async with self.get_async_session() as session:
            existing = await session.scalar(
                select(CorporateActionResolvedTermsDB).where(
                    CorporateActionResolvedTermsDB.instrument_id == instrument_id,
                    CorporateActionResolvedTermsDB.source_event_key == source_event_key,
                )
            )
            if existing is None:
                existing = CorporateActionResolvedTermsDB(
                    instrument_id=instrument_id,
                    source_event_key=source_event_key,
                    **values,
                )
                session.add(existing)
                status = "inserted"
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
                status = "updated"
            await session.commit()
            return {"resolved_terms_id": existing.id, "status": status}

    async def save_corporate_action_review_bundle(
        self,
        *,
        review_row: Dict[str, Any],
        terms_row: Optional[Dict[str, Any]] = None,
        evidence_row: Optional[Dict[str, Any]] = None,
        ingestion_run_id: Optional[str] = None,
        reject_if_prior_event_review: bool = False,
    ) -> Dict[str, Any]:
        """Atomically persist a review, optional terms overlay, and evidence."""
        review_key = str(review_row.get("review_key") or "").strip()
        instrument_id = str(review_row.get("instrument_id") or "").strip()
        source_event_key = str(review_row.get("source_event_key") or "").strip()
        reviewer = str(review_row.get("reviewer") or "").strip()
        decision = str(review_row.get("decision") or "").strip().lower()
        raw_analysis_id = review_row.get("analysis_id")
        analysis_id = (
            int(raw_analysis_id)
            if raw_analysis_id not in (None, "", 0, "0")
            else None
        )
        if not all((review_key, instrument_id, source_event_key, reviewer)):
            raise ValueError("review identity is required")
        if terms_row is not None and analysis_id is None:
            raise ValueError("resolved-term overlay requires analysis_id")
        if decision not in {"resolved", "rejected", "conflict", "manual_required"}:
            raise ValueError("unsupported corporate-action review decision")
        effective_date = self._coerce_datetime(review_row.get("effective_date"))
        if decision == "resolved" and effective_date is None:
            raise ValueError("resolved review requires effective_date")
        if decision == "resolved" and not str(
            review_row.get("date_basis") or ""
        ).strip():
            raise ValueError("resolved review requires date_basis")
        review_values = {
            "review_key": review_key,
            "instrument_id": instrument_id,
            "source_event_key": source_event_key,
            "analysis_id": analysis_id,
            "evidence_key": review_row.get("evidence_key"),
            "decision": decision,
            "effective_date": effective_date,
            "date_basis": review_row.get("date_basis"),
            "reviewer": reviewer,
            "notes": review_row.get("notes"),
            "review_payload_json": json.dumps(
                review_row.get("review_payload") or {},
                ensure_ascii=True,
                sort_keys=True,
                default=str,
            ),
            "supersedes_review_id": review_row.get("supersedes_review_id"),
        }
        terms_values = None
        if terms_row is not None:
            terms_values = {
                "analysis_id": analysis_id,
                "cash_dividend_per_share": terms_row.get(
                    "cash_dividend_per_share"
                ),
                "bonus_shares_per_share": terms_row.get(
                    "bonus_shares_per_share"
                ),
                "capitalization_shares_per_share": terms_row.get(
                    "capitalization_shares_per_share"
                ),
                "rights_shares_per_share": terms_row.get(
                    "rights_shares_per_share"
                ),
                "rights_price": terms_row.get("rights_price"),
                "currency": terms_row.get("currency"),
                "is_active": bool(terms_row.get("is_active", True)),
                "resolved_fields_json": json.dumps(
                    sorted({
                        str(item)
                        for item in terms_row.get("resolved_fields", [])
                        if str(item)
                    }),
                    ensure_ascii=True,
                ),
                "evidence_json": json.dumps(
                    terms_row.get("evidence") or {},
                    ensure_ascii=True,
                    sort_keys=True,
                    default=str,
                ),
            }
        if (
            terms_values is not None
            and bool(terms_values["is_active"]) != (decision == "resolved")
        ):
            raise ValueError("resolved-term activation must match review decision")
        if decision == "resolved" and evidence_row is None:
            raise ValueError("resolved review requires effective-date evidence")

        prepared_evidence: Optional[Dict[str, Any]] = None
        if evidence_row is not None:
            evidence_instrument = str(
                evidence_row.get("instrument_id") or ""
            ).strip()
            evidence_event = str(
                evidence_row.get("source_event_key") or ""
            ).strip()
            evidence_source = str(
                evidence_row.get("evidence_source") or ""
            ).strip()
            evidence_key = str(evidence_row.get("evidence_key") or "").strip()
            source_profile = str(
                evidence_row.get("source_profile") or ""
            ).strip()
            if (
                evidence_instrument != instrument_id
                or evidence_event != source_event_key
                or not all((evidence_source, evidence_key, source_profile))
            ):
                raise ValueError("review evidence identity is invalid")
            resolution_status = str(
                evidence_row.get("resolution_status") or ""
            ).strip().lower()
            if resolution_status not in {"resolved", "rejected"}:
                raise ValueError("review evidence must be resolved or rejected")
            expected_resolution = (
                "resolved" if decision == "resolved" else "rejected"
            )
            if resolution_status != expected_resolution:
                raise ValueError("review evidence status must match review decision")
            evidence_effective_date = self._coerce_datetime(
                evidence_row.get("effective_date")
            )
            if resolution_status == "resolved" and evidence_effective_date is None:
                raise ValueError("resolved evidence requires effective_date")
            if resolution_status == "resolved" and not str(
                evidence_row.get("date_basis") or ""
            ).strip():
                raise ValueError("resolved evidence requires date_basis")
            if resolution_status != "resolved":
                evidence_effective_date = None
            evidence_values = {
                "observation_source": str(
                    evidence_row.get("observation_source") or "cninfo"
                ).lower(),
                "source_profile": source_profile,
                "resolution_status": resolution_status,
                "effective_date": evidence_effective_date,
                "date_basis": evidence_row.get("date_basis"),
                "announcement_id": evidence_row.get("announcement_id"),
                "announcement_title": evidence_row.get("announcement_title"),
                "announcement_time": self._coerce_datetime(
                    evidence_row.get("announcement_time")
                ),
                "evidence_url": evidence_row.get("evidence_url"),
                "confidence": evidence_row.get("confidence"),
                "ingestion_run_id": ingestion_run_id,
                "raw_payload_json": json.dumps(
                    evidence_row.get("raw_payload") or {},
                    ensure_ascii=True,
                    default=str,
                    sort_keys=True,
                ),
            }
            evidence_values["row_hash"] = hashlib.sha256(
                json.dumps(
                    {
                        key: value
                        for key, value in evidence_values.items()
                        if key != "ingestion_run_id"
                    },
                    ensure_ascii=True,
                    default=str,
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            prepared_evidence = {
                "instrument_id": evidence_instrument,
                "source_event_key": evidence_event,
                "evidence_source": evidence_source,
                "evidence_key": evidence_key,
                "values": evidence_values,
            }

        async with self.get_async_session() as session:
            async with session.begin():
                observation_id = await session.scalar(
                    select(CorporateActionObservationDB.id)
                    .where(
                        CorporateActionObservationDB.instrument_id == instrument_id,
                        CorporateActionObservationDB.source_event_key
                        == source_event_key,
                    )
                    .order_by(
                        CorporateActionObservationDB.is_current.desc(),
                        CorporateActionObservationDB.id.desc(),
                    )
                    .limit(1)
                    .with_for_update()
                )
                if observation_id is None:
                    raise ValueError("reviewed corporate-action observation is missing")
                if reject_if_prior_event_review:
                    prior_review_id = await session.scalar(
                        select(CorporateActionResolutionReviewDB.id)
                        .where(
                            CorporateActionResolutionReviewDB.instrument_id
                            == instrument_id,
                            CorporateActionResolutionReviewDB.source_event_key
                            == source_event_key,
                        )
                        .limit(1)
                    )
                    if prior_review_id is not None:
                        raise ValueError(
                            "corporate-action event already has a review decision"
                        )
                review = await session.scalar(
                    select(CorporateActionResolutionReviewDB).where(
                        CorporateActionResolutionReviewDB.review_key == review_key
                    )
                )
                if review is None:
                    review = CorporateActionResolutionReviewDB(**review_values)
                    session.add(review)
                    review_status = "inserted"
                else:
                    for key, value in review_values.items():
                        setattr(review, key, value)
                    review.updated_at = get_shanghai_time()
                    review_status = "updated"
                await session.flush()
                review_id = int(review.id)

                terms = await session.scalar(
                    select(CorporateActionResolvedTermsDB).where(
                        CorporateActionResolvedTermsDB.instrument_id == instrument_id,
                        CorporateActionResolvedTermsDB.source_event_key
                        == source_event_key,
                    )
                )
                if terms_values is None:
                    if terms is None:
                        terms_write = {
                            "resolved_terms_id": None,
                            "status": "absent",
                        }
                    else:
                        terms_status = (
                            "deactivated" if terms.is_active else "unchanged"
                        )
                        terms.is_active = False
                        terms.updated_at = get_shanghai_time()
                        terms_write = {
                            "resolved_terms_id": int(terms.id),
                            "status": terms_status,
                        }
                else:
                    terms_values["review_id"] = review_id
                    if terms is None:
                        terms = CorporateActionResolvedTermsDB(
                            instrument_id=instrument_id,
                            source_event_key=source_event_key,
                            **terms_values,
                        )
                        session.add(terms)
                        terms_status = "inserted"
                    else:
                        for key, value in terms_values.items():
                            setattr(terms, key, value)
                        terms.updated_at = get_shanghai_time()
                        terms_status = "updated"
                    await session.flush()
                    terms_write = {
                        "resolved_terms_id": int(terms.id),
                        "status": terms_status,
                    }

                evidence_write = None
                if prepared_evidence is not None:
                    evidence = await session.scalar(
                        select(CorporateActionEffectiveDateEvidenceDB).where(
                            CorporateActionEffectiveDateEvidenceDB.instrument_id
                            == prepared_evidence["instrument_id"],
                            CorporateActionEffectiveDateEvidenceDB.source_event_key
                            == prepared_evidence["source_event_key"],
                            CorporateActionEffectiveDateEvidenceDB.evidence_source
                            == prepared_evidence["evidence_source"],
                            CorporateActionEffectiveDateEvidenceDB.evidence_key
                            == prepared_evidence["evidence_key"],
                        )
                    )
                    evidence_values = prepared_evidence["values"]
                    evidence_write = {
                        "inserted": 0,
                        "changed": 0,
                        "unchanged": 0,
                        "failed": 0,
                    }
                    if evidence is None:
                        evidence = CorporateActionEffectiveDateEvidenceDB(
                            instrument_id=prepared_evidence["instrument_id"],
                            source_event_key=prepared_evidence["source_event_key"],
                            evidence_source=prepared_evidence["evidence_source"],
                            evidence_key=prepared_evidence["evidence_key"],
                            **evidence_values,
                        )
                        session.add(evidence)
                        evidence_write["inserted"] = 1
                    elif evidence.row_hash == evidence_values["row_hash"]:
                        evidence.ingestion_run_id = ingestion_run_id
                        evidence.updated_at = get_shanghai_time()
                        evidence_write["unchanged"] = 1
                    else:
                        for key, value in evidence_values.items():
                            setattr(evidence, key, value)
                        evidence.updated_at = get_shanghai_time()
                        evidence_write["changed"] = 1

            return {
                "review": {"review_id": review_id, "status": review_status},
                "terms_write": terms_write,
                "evidence_write": evidence_write,
            }

    async def get_corporate_action_resolved_terms(
        self,
        source_event_keys: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Return economic overlays plus explicit review-only no-effect policies."""
        keys = sorted({str(item).strip() for item in source_event_keys if str(item).strip()})
        if not keys:
            return {}
        rows = []
        review_rows = []
        async with self.get_async_session() as session:
            for offset in range(0, len(keys), 400):
                chunk = keys[offset: offset + 400]
                rows.extend((await session.execute(
                    select(CorporateActionResolvedTermsDB).where(
                        CorporateActionResolvedTermsDB.source_event_key.in_(
                            chunk
                        ),
                        CorporateActionResolvedTermsDB.is_active.is_(True),
                    )
                )).scalars().all())
                review_rows.extend((await session.execute(
                    select(CorporateActionResolutionReviewDB).where(
                        CorporateActionResolutionReviewDB.source_event_key.in_(
                            chunk
                        ),
                    ).order_by(
                        CorporateActionResolutionReviewDB.id.desc()
                    )
                )).scalars().all())
        result = {
            row.source_event_key: self._resolved_terms_payload(row)
            for row in rows
        }
        seen_review_events = set()
        for review in review_rows:
            if (
                review.source_event_key in result
                or review.source_event_key in seen_review_events
            ):
                continue
            seen_review_events.add(review.source_event_key)
            if str(review.decision or "").strip() != "resolved":
                continue
            payload = json.loads(review.review_payload_json or "{}")
            if (
                payload.get("resolution_policy")
                != "cninfo_operator_attested_passthrough_v1"
                or str(payload.get("factor_effect") or "").strip().lower()
                != "none"
            ):
                continue
            result[review.source_event_key] = {
                "cash_dividend_per_share": None,
                "bonus_shares_per_share": None,
                "capitalization_shares_per_share": None,
                "rights_shares_per_share": None,
                "rights_price": None,
                "currency": None,
                "resolved_fields": [],
                "evidence": payload,
                "factor_effect": "none",
                "factor_override": None,
                "factor_reference": {},
                "authoritative_override": False,
                "analysis_id": None,
                "review_id": review.id,
            }
        return result

    @staticmethod
    def _resolved_terms_payload(
        row: CorporateActionResolvedTermsDB,
    ) -> Dict[str, Any]:
        evidence = json.loads(row.evidence_json or "{}")
        return {
            "cash_dividend_per_share": row.cash_dividend_per_share,
            "bonus_shares_per_share": row.bonus_shares_per_share,
            "capitalization_shares_per_share": row.capitalization_shares_per_share,
            "rights_shares_per_share": row.rights_shares_per_share,
            "rights_price": row.rights_price,
            "currency": row.currency,
            "resolved_fields": json.loads(row.resolved_fields_json or "[]"),
            "evidence": evidence,
            "factor_effect": str(
                evidence.get("factor_effect") or "normal"
            ).strip().lower(),
            "factor_override": evidence.get("factor_override"),
            "factor_reference": evidence.get("factor_reference") or {},
            "authoritative_override": bool(
                evidence.get("authoritative_override")
            ),
            "analysis_id": row.analysis_id,
            "review_id": row.review_id,
        }

    async def get_corporate_action_resolved_terms_page(
        self,
        *,
        instrument_id: Optional[str] = None,
        source_event_key: Optional[str] = None,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return paginated reviewed economic-term overlays."""
        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        filters = []
        if instrument_id:
            filters.append(CorporateActionResolvedTermsDB.instrument_id == instrument_id)
        if source_event_key:
            filters.append(
                CorporateActionResolvedTermsDB.source_event_key == source_event_key
            )
        if active_only:
            filters.append(CorporateActionResolvedTermsDB.is_active.is_(True))
        async with self.get_async_session() as session:
            total = await session.scalar(
                select(func.count()).select_from(CorporateActionResolvedTermsDB).where(
                    *filters
                )
            )
            rows = (await session.execute(
                select(CorporateActionResolvedTermsDB).where(*filters).order_by(
                    CorporateActionResolvedTermsDB.instrument_id,
                    CorporateActionResolvedTermsDB.source_event_key,
                ).offset(normalized_offset).limit(normalized_limit)
            )).scalars().all()
            items = [{
                "resolved_terms_id": row.id,
                "instrument_id": row.instrument_id,
                "source_event_key": row.source_event_key,
                "is_active": bool(row.is_active),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                **self._resolved_terms_payload(row),
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(items) < total_value,
                "items": items,
            }

    async def upsert_corporate_action_resolution_states(
        self,
        rows: List[Dict[str, Any]],
        *,
        ingestion_run_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """Persist current event-level governance projections idempotently."""
        allowed_states = {
            "resolved_source", "resolved_evidence", "not_applicable",
            "source_not_supported", "candidate_pending_analysis",
            "validated_candidate", "machine_rework", "document_rework",
            "manual_required",
            "conflict", "non_effective", "superseded",
            "scope_mismatch", "pre_listing",
            "official_archive_unavailable",
            "evidence_unavailable", "discovery_pending", "retryable_error",
        }
        counters = {"inserted": 0, "changed": 0, "unchanged": 0, "failed": 0}
        if not rows:
            return counters
        async with self.get_async_session() as session:
            async with session.begin():
                for row in rows:
                    instrument_id = str(row.get("instrument_id") or "").strip()
                    source_event_key = str(row.get("source_event_key") or "").strip()
                    source_profile = str(row.get("source_profile") or "").strip()
                    exchange = str(row.get("exchange") or "").strip().upper()
                    state = str(row.get("resolution_state") or "").strip().lower()
                    if not all((instrument_id, source_event_key, source_profile, exchange)):
                        counters["failed"] += 1
                        continue
                    if state not in allowed_states:
                        raise ValueError(f"unsupported corporate-action resolution state: {state}")
                    values = {
                        "source_profile": source_profile,
                        "action_type": row.get("action_type"),
                        "exchange": exchange,
                        "policy_version": str(row.get("policy_version") or ""),
                        "state_version": str(row.get("state_version") or ""),
                        "resolution_state": state,
                        "is_terminal": bool(row.get("is_terminal")),
                        "factor_blocking": bool(row.get("factor_blocking")),
                        "state_reason": str(row.get("state_reason") or "unknown")[:128],
                        "next_action": str(row.get("next_action") or "unknown")[:64],
                        "candidate_count": max(0, int(row.get("candidate_count") or 0)),
                        "latest_analysis_id": row.get("latest_analysis_id"),
                        "latest_review_id": row.get("latest_review_id"),
                        "resolved_effective_date": self._coerce_datetime(
                            row.get("resolved_effective_date")
                        ),
                        "last_attempt_at": self._coerce_datetime(
                            row.get("last_attempt_at")
                        ),
                        "ingestion_run_id": ingestion_run_id,
                        "diagnostics_json": json.dumps(
                            row.get("diagnostics") or {},
                            ensure_ascii=True,
                            sort_keys=True,
                            default=str,
                        ),
                    }
                    if not values["policy_version"] or not values["state_version"]:
                        raise ValueError("resolution state versions are required")
                    existing = await session.scalar(
                        select(CorporateActionResolutionStateDB).where(
                            CorporateActionResolutionStateDB.instrument_id
                            == instrument_id,
                            CorporateActionResolutionStateDB.source_event_key
                            == source_event_key,
                        )
                    )
                    if existing is None:
                        session.add(CorporateActionResolutionStateDB(
                            instrument_id=instrument_id,
                            source_event_key=source_event_key,
                            **values,
                        ))
                        counters["inserted"] += 1
                        continue
                    if values["last_attempt_at"] is None:
                        values["last_attempt_at"] = existing.last_attempt_at
                    changed = any(getattr(existing, key) != value for key, value in values.items())
                    if changed:
                        for key, value in values.items():
                            setattr(existing, key, value)
                        existing.updated_at = get_shanghai_time()
                        counters["changed"] += 1
                    else:
                        counters["unchanged"] += 1
        return counters

    async def get_corporate_action_resolution_states(
        self,
        *,
        instrument_id: Optional[str] = None,
        source_event_key: Optional[str] = None,
        resolution_state: Optional[str] = None,
        is_terminal: Optional[bool] = None,
        factor_blocking: Optional[bool] = None,
        next_action: Optional[str] = None,
        current_only: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return paginated current corporate-action governance states."""
        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        filters = []
        if instrument_id:
            filters.append(CorporateActionResolutionStateDB.instrument_id == instrument_id)
        if source_event_key:
            filters.append(
                CorporateActionResolutionStateDB.source_event_key == source_event_key
            )
        if resolution_state:
            filters.append(
                CorporateActionResolutionStateDB.resolution_state == resolution_state
            )
        if is_terminal is not None:
            filters.append(
                CorporateActionResolutionStateDB.is_terminal.is_(bool(is_terminal))
            )
        if factor_blocking is not None:
            filters.append(
                CorporateActionResolutionStateDB.factor_blocking.is_(
                    bool(factor_blocking)
                )
            )
        if next_action:
            filters.append(CorporateActionResolutionStateDB.next_action == next_action)
        if current_only:
            filters.append(
                CorporateActionResolutionStateDB.source_event_key.in_(
                    select(CorporateActionObservationDB.source_event_key).where(
                        CorporateActionObservationDB.source == "cninfo",
                        CorporateActionObservationDB.is_current.is_(True),
                    )
                )
            )
        async with self.get_async_session() as session:
            total = await session.scalar(
                select(func.count())
                .select_from(CorporateActionResolutionStateDB)
                .where(*filters)
            )
            rows = (await session.execute(
                select(CorporateActionResolutionStateDB)
                .where(*filters)
                .order_by(
                    CorporateActionResolutionStateDB.is_terminal,
                    CorporateActionResolutionStateDB.instrument_id,
                    CorporateActionResolutionStateDB.source_event_key,
                )
                .offset(normalized_offset)
                .limit(normalized_limit)
            )).scalars().all()
            items = [{
                "state_id": row.id,
                "instrument_id": row.instrument_id,
                "source_event_key": row.source_event_key,
                "source_profile": row.source_profile,
                "action_type": row.action_type,
                "exchange": row.exchange,
                "policy_version": row.policy_version,
                "state_version": row.state_version,
                "resolution_state": row.resolution_state,
                "is_terminal": bool(row.is_terminal),
                "factor_blocking": bool(row.factor_blocking),
                "state_reason": row.state_reason,
                "next_action": row.next_action,
                "candidate_count": row.candidate_count,
                "latest_analysis_id": row.latest_analysis_id,
                "latest_review_id": row.latest_review_id,
                "resolved_effective_date": row.resolved_effective_date,
                "last_attempt_at": row.last_attempt_at,
                "ingestion_run_id": row.ingestion_run_id,
                "diagnostics": json.loads(row.diagnostics_json or "{}"),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(items) < total_value,
                "items": items,
            }

    async def get_corporate_action_effective_date_evidence(
        self,
        *,
        instrument_id: Optional[str] = None,
        source_event_key: Optional[str] = None,
        source_profile: Optional[str] = None,
        evidence_source: Optional[str] = None,
        resolution_status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return paginated special-action effective-date evidence."""
        normalized_limit = max(1, min(int(limit), 1000))
        normalized_offset = max(0, int(offset))
        async with self.get_async_session() as session:
            filters = []
            if instrument_id:
                filters.append(
                    CorporateActionEffectiveDateEvidenceDB.instrument_id
                    == instrument_id
                )
            if source_event_key:
                filters.append(
                    CorporateActionEffectiveDateEvidenceDB.source_event_key
                    == source_event_key
                )
            if source_profile:
                filters.append(
                    CorporateActionEffectiveDateEvidenceDB.source_profile
                    == source_profile
                )
            if evidence_source:
                filters.append(
                    CorporateActionEffectiveDateEvidenceDB.evidence_source
                    == evidence_source
                )
            if resolution_status:
                filters.append(
                    CorporateActionEffectiveDateEvidenceDB.resolution_status
                    == resolution_status
                )
            total = await session.scalar(
                select(func.count())
                .select_from(CorporateActionEffectiveDateEvidenceDB)
                .where(*filters)
            )
            rows = (await session.execute(
                select(CorporateActionEffectiveDateEvidenceDB)
                .where(*filters)
                .order_by(
                    CorporateActionEffectiveDateEvidenceDB.instrument_id,
                    CorporateActionEffectiveDateEvidenceDB.source_event_key,
                    CorporateActionEffectiveDateEvidenceDB.announcement_time,
                )
                .offset(normalized_offset)
                .limit(normalized_limit)
            )).scalars().all()
            items = [{
                "instrument_id": row.instrument_id,
                "source_event_key": row.source_event_key,
                "observation_source": row.observation_source,
                "source_profile": row.source_profile,
                "evidence_source": row.evidence_source,
                "evidence_key": row.evidence_key,
                "resolution_status": row.resolution_status,
                "effective_date": row.effective_date,
                "date_basis": row.date_basis,
                "announcement_id": row.announcement_id,
                "announcement_title": row.announcement_title,
                "announcement_time": row.announcement_time,
                "evidence_url": row.evidence_url,
                "confidence": row.confidence,
                "ingestion_run_id": row.ingestion_run_id,
                "raw_payload": json.loads(row.raw_payload_json or "{}"),
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            } for row in rows]
            total_value = int(total or 0)
            return {
                "total": total_value,
                "limit": normalized_limit,
                "offset": normalized_offset,
                "returned": len(items),
                "has_more": normalized_offset + len(items) < total_value,
                "items": items,
            }

    async def get_resolved_corporate_action_effective_dates(
        self,
        source_event_keys: List[str],
        *,
        _session: Any = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Return one resolved date per source event for factor derivation."""
        normalized_keys = sorted({
            str(value).strip() for value in source_event_keys if str(value).strip()
        })
        if not normalized_keys:
            return {}
        if _session is None:
            async with self.get_async_session() as session:
                return await self.get_resolved_corporate_action_effective_dates(
                    normalized_keys,
                    _session=session,
                )
        rows = []
        current_reviews_by_event: Dict[
            str, CorporateActionResolutionReviewDB
        ] = {}
        latest_reviews_by_event: Dict[
            str, CorporateActionResolutionReviewDB
        ] = {}
        operator_attestation_events = set()
        session = _session
        for offset in range(0, len(normalized_keys), 400):
            chunk = normalized_keys[offset: offset + 400]
            rows.extend((await session.execute(
                select(CorporateActionEffectiveDateEvidenceDB).where(
                    CorporateActionEffectiveDateEvidenceDB.source_event_key.in_(
                        chunk
                    ),
                    CorporateActionEffectiveDateEvidenceDB.resolution_status
                    == "resolved",
                    CorporateActionEffectiveDateEvidenceDB.observation_source
                    == "cninfo",
                    CorporateActionEffectiveDateEvidenceDB.evidence_source.in_(
                        GOVERNED_CORPORATE_ACTION_EFFECTIVE_DATE_EVIDENCE_SOURCES
                    ),
                    CorporateActionEffectiveDateEvidenceDB.effective_date.is_not(
                        None
                    ),
                ).order_by(
                    CorporateActionEffectiveDateEvidenceDB.updated_at.desc(),
                    CorporateActionEffectiveDateEvidenceDB.id.desc(),
                )
            )).scalars().all())
            current_review_rows = (await session.execute(
                select(
                    CorporateActionResolvedTermsDB.source_event_key,
                    CorporateActionResolutionReviewDB,
                )
                .join(
                    CorporateActionResolutionReviewDB,
                    CorporateActionResolutionReviewDB.id
                    == CorporateActionResolvedTermsDB.review_id,
                )
                .where(
                    CorporateActionResolvedTermsDB.source_event_key.in_(
                        chunk
                    ),
                    CorporateActionResolvedTermsDB.is_active.is_(True),
                )
            )).all()
            current_reviews_by_event.update({
                source_event_key: review
                for source_event_key, review in current_review_rows
            })
            review_rows = (await session.execute(
                select(CorporateActionResolutionReviewDB).where(
                    CorporateActionResolutionReviewDB.source_event_key.in_(
                        chunk
                    ),
                ).order_by(
                    CorporateActionResolutionReviewDB.id.desc()
                )
            )).scalars().all()
            for review in review_rows:
                latest_reviews_by_event.setdefault(
                    review.source_event_key,
                    review,
                )
                try:
                    review_payload = json.loads(
                        review.review_payload_json or "{}"
                    )
                except (TypeError, ValueError):
                    review_payload = {}
                if (
                    review_payload.get("resolution_policy")
                    == "cninfo_operator_attested_passthrough_v1"
                ):
                    operator_attestation_events.add(
                        review.source_event_key
                    )
        for source_event_key in operator_attestation_events:
            current_review = latest_reviews_by_event.get(source_event_key)
            if current_review is not None:
                current_reviews_by_event.setdefault(
                    source_event_key,
                    current_review,
                )
        rows_by_event: Dict[str, List[Any]] = {}
        for row in rows:
            rows_by_event.setdefault(row.source_event_key, []).append(row)
        resolved = {}
        for source_event_key, event_rows in rows_by_event.items():
            current_review = current_reviews_by_event.get(source_event_key)
            if current_review is not None:
                if str(current_review.decision or "").strip() != "resolved":
                    continue
                current_review_key = str(
                    current_review.review_key or ""
                ).strip()
                current_evidence_key = str(
                    current_review.evidence_key or ""
                ).strip()
                review_key_rows = []
                for row in event_rows:
                    try:
                        raw_payload = json.loads(row.raw_payload_json or "{}")
                    except (TypeError, ValueError):
                        raw_payload = {}
                    if (
                        current_review_key
                        and str(raw_payload.get("review_key") or "").strip()
                        == current_review_key
                    ):
                        review_key_rows.append(row)
                matching_rows = review_key_rows
                if not matching_rows and current_evidence_key:
                    matching_rows = [
                        row for row in event_rows
                        if str(row.evidence_key or "").strip()
                        == current_evidence_key
                    ]
                if not matching_rows:
                    self.db_logger.warning(
                        "Current corporate-action review has no matching "
                        "resolved evidence: source_event_key=%s review_id=%s",
                        source_event_key,
                        current_review.id,
                    )
                    continue
                event_rows = matching_rows
            dates = {
                row.effective_date.date()
                if isinstance(row.effective_date, datetime)
                else row.effective_date
                for row in event_rows
            }
            if len(dates) != 1:
                self.db_logger.warning(
                    "Conflicting resolved corporate-action dates ignored: "
                    "source_event_key=%s dates=%s",
                    source_event_key,
                    sorted(str(value) for value in dates),
                )
                continue
            row = event_rows[0]
            resolved[source_event_key] = {
                "effective_date": row.effective_date,
                "date_basis": row.date_basis,
                "evidence_source": row.evidence_source,
                "evidence_key": row.evidence_key,
            }
        return resolved

    async def reset_cninfo_corporate_action_resolution_data(
        self,
        *,
        start_date: date,
        end_date: date,
        exchanges: List[str],
        instrument_ids: Optional[List[str]] = None,
        source_event_keys: Optional[List[str]] = None,
        include_unanchored: bool = False,
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Preview or remove non-resolved CNInfo resolution development data."""
        normalized_start = self._coerce_datetime(start_date).date()
        normalized_end = self._coerce_datetime(end_date).date()
        if normalized_end < normalized_start:
            raise ValueError("end_date must not be earlier than start_date")
        suffixes = {"SSE": ".SH", "SZSE": ".SZ"}
        normalized_exchanges = sorted({
            str(item).strip().upper() for item in exchanges
            if str(item).strip().upper() in suffixes
        })
        if not normalized_exchanges:
            raise ValueError("reset requires SSE and/or SZSE")
        selected_ids = {
            str(item).strip() for item in (instrument_ids or []) if str(item).strip()
        }
        selected_event_keys = {
            str(item).strip() for item in (source_event_keys or []) if str(item).strip()
        }

        def observation_anchors(row: CorporateActionObservationDB) -> List[date]:
            anchors = [
                value.date() if isinstance(value, datetime) else value
                for value in (
                    row.announcement_date, row.record_date, row.share_arrival_date
                )
                if value is not None
            ]
            match = re.search(r"(19|20)\d{2}", str(row.fiscal_period or ""))
            if not anchors and match:
                anchors.append(date(int(match.group(0)), 12, 31))
            return anchors

        async with self.get_async_session() as session:
            filters = [
                CorporateActionObservationDB.source == "cninfo",
                CorporateActionObservationDB.source_profile.in_((
                    "cninfo_dividend", "cninfo_allotment"
                )),
            ]
            if selected_ids:
                filters.append(CorporateActionObservationDB.instrument_id.in_(selected_ids))
            if selected_event_keys:
                filters.append(
                    CorporateActionObservationDB.source_event_key.in_(selected_event_keys)
                )
            observations = (await session.execute(
                select(CorporateActionObservationDB).where(*filters)
            )).scalars().all()
            allowed_suffixes = tuple(suffixes[item] for item in normalized_exchanges)
            selected_identities = set()
            for row in observations:
                if not row.instrument_id.endswith(allowed_suffixes):
                    continue
                anchors = observation_anchors(row)
                if (
                    bool(selected_event_keys)
                    or any(
                        normalized_start <= anchor <= normalized_end
                        for anchor in anchors
                    )
                    or (
                        not anchors
                        and include_unanchored
                    )
                ):
                    selected_identities.add(
                        (row.instrument_id, row.source_event_key)
                    )
            empty_result = {
                "dry_run": bool(dry_run),
                "selected_events": len(selected_identities),
                "protected_resolved_events": 0,
                "reset_events": 0,
                "deleted": {},
                "selected_instrument_ids": sorted({item[0] for item in selected_identities}),
                "selected_source_event_keys": sorted({item[1] for item in selected_identities}),
                "reset_instrument_ids": [],
                "reset_source_event_keys": [],
                "protected_instrument_ids": [],
                "protected_announcement_ids": [],
            }
            if not selected_identities:
                return empty_result

            evidence_identity = tuple_(
                CorporateActionEffectiveDateEvidenceDB.instrument_id,
                CorporateActionEffectiveDateEvidenceDB.source_event_key,
            )
            protected_rows = (await session.execute(
                select(
                    CorporateActionEffectiveDateEvidenceDB.instrument_id,
                    CorporateActionEffectiveDateEvidenceDB.source_event_key,
                    CorporateActionEffectiveDateEvidenceDB.announcement_id,
                    CorporateActionEffectiveDateEvidenceDB.evidence_source,
                    CorporateActionEffectiveDateEvidenceDB.evidence_key,
                ).where(
                    evidence_identity.in_(selected_identities),
                    CorporateActionEffectiveDateEvidenceDB.observation_source == "cninfo",
                    CorporateActionEffectiveDateEvidenceDB.resolution_status == "resolved",
                    CorporateActionEffectiveDateEvidenceDB.evidence_source.in_(
                        GOVERNED_CORPORATE_ACTION_EFFECTIVE_DATE_EVIDENCE_SOURCES
                    ),
                    CorporateActionEffectiveDateEvidenceDB.effective_date.is_not(None),
                )
            )).all()
            attested_event_keys = {
                row.source_event_key for row in protected_rows
                if row.evidence_source == "cninfo_operator_attestation"
            }
            if attested_event_keys:
                current_dates = (
                    await self.get_resolved_corporate_action_effective_dates(
                        sorted(attested_event_keys),
                        _session=session,
                    )
                )
                protected_rows = [
                    row for row in protected_rows
                    if (
                        row.source_event_key not in attested_event_keys
                        or (
                            current_dates.get(row.source_event_key, {}).get(
                                "evidence_source"
                            ) == row.evidence_source
                            and current_dates.get(
                                row.source_event_key, {}
                            ).get("evidence_key") == row.evidence_key
                        )
                    )
                ]
            protected_identities = {
                (row.instrument_id, row.source_event_key) for row in protected_rows
            }
            reset_identities = selected_identities - protected_identities
            empty_result.update({
                "protected_resolved_events": len(protected_identities),
                "reset_events": len(reset_identities),
                "reset_instrument_ids": sorted({item[0] for item in reset_identities}),
                "reset_source_event_keys": sorted({item[1] for item in reset_identities}),
                "protected_instrument_ids": sorted({
                    item[0] for item in protected_identities
                }),
                "protected_announcement_ids": sorted({
                    str(row.announcement_id) for row in protected_rows if row.announcement_id
                }),
            })
            if not reset_identities:
                return empty_result

            analysis_identity = tuple_(
                CorporateActionLlmAnalysisDB.instrument_id,
                CorporateActionLlmAnalysisDB.source_event_key,
            )
            review_identity = tuple_(
                CorporateActionResolutionReviewDB.instrument_id,
                CorporateActionResolutionReviewDB.source_event_key,
            )
            terms_identity = tuple_(
                CorporateActionResolvedTermsDB.instrument_id,
                CorporateActionResolvedTermsDB.source_event_key,
            )
            state_identity = tuple_(
                CorporateActionResolutionStateDB.instrument_id,
                CorporateActionResolutionStateDB.source_event_key,
            )
            evidence_filter = evidence_identity.in_(reset_identities)
            analysis_filter = analysis_identity.in_(reset_identities)
            review_filter = review_identity.in_(reset_identities)
            terms_filter = terms_identity.in_(reset_identities)
            state_filter = state_identity.in_(reset_identities)

            announcement_ids = set((await session.execute(
                select(CorporateActionEffectiveDateEvidenceDB.announcement_id).where(
                    evidence_filter,
                    CorporateActionEffectiveDateEvidenceDB.announcement_id.is_not(None),
                )
            )).scalars().all())
            artifact_rows = []
            if announcement_ids:
                artifact_rows = (await session.execute(
                    select(CorporateActionDocumentArtifactDB).where(
                        CorporateActionDocumentArtifactDB.announcement_id.in_(announcement_ids)
                    )
                )).scalars().all()
            remaining_evidence_announcements = set()
            if announcement_ids:
                remaining_evidence_announcements = set((await session.execute(
                    select(CorporateActionEffectiveDateEvidenceDB.announcement_id).where(
                        CorporateActionEffectiveDateEvidenceDB.announcement_id.in_(
                            announcement_ids
                        ),
                        ~evidence_filter,
                    )
                )).scalars().all())
            candidate_artifact_ids = {int(row.id) for row in artifact_rows}
            remaining_analysis_artifact_ids = set()
            if candidate_artifact_ids:
                remaining_analysis_rows = (await session.execute(
                    select(CorporateActionLlmAnalysisDB.artifact_ids_json).where(
                        ~analysis_filter
                    )
                )).scalars().all()
                for raw_ids in remaining_analysis_rows:
                    try:
                        parsed_ids = json.loads(raw_ids or "[]")
                    except (TypeError, json.JSONDecodeError):
                        continue
                    remaining_analysis_artifact_ids.update(
                        int(item) for item in parsed_ids if str(item).isdigit()
                    )
            unreferenced_artifact_ids = {
                int(row.id) for row in artifact_rows
                if row.announcement_id not in remaining_evidence_announcements
                and int(row.id) not in remaining_analysis_artifact_ids
            }

            async def row_count(model: Any, clause: Any) -> int:
                return int(await session.scalar(
                    select(func.count()).select_from(model).where(clause)
                ) or 0)

            counts = {
                "resolution_states": await row_count(
                    CorporateActionResolutionStateDB, state_filter
                ),
                "resolved_terms": await row_count(
                    CorporateActionResolvedTermsDB, terms_filter
                ),
                "reviews": await row_count(
                    CorporateActionResolutionReviewDB, review_filter
                ),
                "llm_analyses": await row_count(
                    CorporateActionLlmAnalysisDB, analysis_filter
                ),
                "effective_date_evidence": await row_count(
                    CorporateActionEffectiveDateEvidenceDB, evidence_filter
                ),
                "document_pages": 0,
                "document_artifacts": len(unreferenced_artifact_ids),
            }
            if unreferenced_artifact_ids:
                counts["document_pages"] = await row_count(
                    CorporateActionDocumentPageDB,
                    CorporateActionDocumentPageDB.artifact_id.in_(
                        unreferenced_artifact_ids
                    ),
                )

            if not dry_run:
                await session.execute(delete(CorporateActionResolutionStateDB).where(
                    state_filter
                ))
                await session.execute(delete(CorporateActionResolvedTermsDB).where(
                    terms_filter
                ))
                await session.execute(delete(CorporateActionResolutionReviewDB).where(
                    review_filter
                ))
                await session.execute(delete(CorporateActionLlmAnalysisDB).where(
                    analysis_filter
                ))
                await session.execute(delete(
                    CorporateActionEffectiveDateEvidenceDB
                ).where(evidence_filter))
                if unreferenced_artifact_ids:
                    await session.execute(delete(CorporateActionDocumentPageDB).where(
                        CorporateActionDocumentPageDB.artifact_id.in_(
                            unreferenced_artifact_ids
                        )
                    ))
                    await session.execute(delete(CorporateActionDocumentArtifactDB).where(
                        CorporateActionDocumentArtifactDB.id.in_(
                            unreferenced_artifact_ids
                        )
                    ))
                await session.commit()

            return {**empty_result, "deleted": counts}

    async def get_adjustment_factor_observations(
        self,
        *,
        instrument_id: Optional[str] = None,
        source: Optional[str] = None,
        source_profile: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        async with self.get_async_session() as session:
            filters = []
            if instrument_id:
                filters.append(AdjustmentFactorObservationDB.instrument_id == instrument_id)
            if source:
                filters.append(AdjustmentFactorObservationDB.source == source.lower())
            if source_profile:
                filters.append(
                    AdjustmentFactorObservationDB.source_profile == source_profile
                )
            if start_date:
                filters.append(AdjustmentFactorObservationDB.ex_date >= self._coerce_datetime(start_date))
            if end_date:
                filters.append(
                    AdjustmentFactorObservationDB.ex_date
                    < self._coerce_datetime(end_date + timedelta(days=1))
                )
            total = await session.scalar(
                select(func.count()).select_from(AdjustmentFactorObservationDB).where(*filters)
            )
            rows = (await session.execute(
                select(AdjustmentFactorObservationDB)
                .where(*filters)
                .order_by(
                    AdjustmentFactorObservationDB.instrument_id,
                    AdjustmentFactorObservationDB.ex_date,
                    AdjustmentFactorObservationDB.source,
                    AdjustmentFactorObservationDB.source_profile,
                )
                .offset(offset).limit(limit)
            )).scalars().all()
            items = [{
                "instrument_id": row.instrument_id,
                "ex_date": row.ex_date,
                "source": row.source,
                "source_profile": row.source_profile,
                "provider_factor": row.provider_factor,
                "provider_cumulative_factor": row.provider_cumulative_factor,
                "normalized_factor": row.normalized_factor,
                "normalization_version": row.normalization_version,
                "quality_status": row.quality_status,
                "ingestion_run_id": row.ingestion_run_id,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            } for row in rows]
            return {
                "total": int(total or 0), "limit": limit, "offset": offset,
                "returned": len(items), "has_more": offset + len(items) < int(total or 0),
                "items": items,
            }

    async def list_adjustment_factor_observations(
        self,
        *,
        instrument_ids: Optional[List[str]] = None,
        sources: Optional[List[str]] = None,
        source_profiles: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Return complete bounded observation sets for rebuild services."""
        async with self.get_async_session() as session:
            stmt = select(AdjustmentFactorObservationDB)
            if instrument_ids:
                stmt = stmt.where(AdjustmentFactorObservationDB.instrument_id.in_(instrument_ids))
            if sources:
                stmt = stmt.where(
                    AdjustmentFactorObservationDB.source.in_([item.lower() for item in sources])
                )
            if source_profiles:
                stmt = stmt.where(
                    AdjustmentFactorObservationDB.source_profile.in_(source_profiles)
                )
            if start_date:
                stmt = stmt.where(
                    AdjustmentFactorObservationDB.ex_date >= self._coerce_datetime(start_date)
                )
            if end_date:
                stmt = stmt.where(
                    AdjustmentFactorObservationDB.ex_date
                    < self._coerce_datetime(end_date + timedelta(days=1))
                )
            rows = (await session.execute(stmt.order_by(
                AdjustmentFactorObservationDB.instrument_id,
                AdjustmentFactorObservationDB.source,
                AdjustmentFactorObservationDB.source_profile,
                AdjustmentFactorObservationDB.ex_date,
            ))).scalars().all()
            return [{
                "instrument_id": row.instrument_id,
                "ex_date": row.ex_date,
                "source": row.source,
                "source_profile": row.source_profile,
                "provider_factor": row.provider_factor,
                "provider_cumulative_factor": row.provider_cumulative_factor,
                "normalized_factor": row.normalized_factor,
                "normalization_version": row.normalization_version,
                "quality_status": row.quality_status,
                "ingestion_run_id": row.ingestion_run_id,
            } for row in rows]

    async def get_quote_evidence_for_event_dates(
        self,
        event_dates: List[tuple[str, date]],
        *,
        effective_end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Return bounded unadjusted quote evidence for each source event date.

        A source date may fall on a weekend, holiday, or suspended session.
        When ``effective_end_date`` is supplied, the first valid traded quote
        through that date is accepted so a long suspension becomes effective
        on its first resumed session. Callers that omit the bound retain the
        legacy fourteen-day lookup. The factor formula must use the prior
        trading session's raw close. An ex-date row's ``pre_close`` may already
        be an exchange or provider adjusted reference price, and suspended
        placeholder rows are not valid effective sessions.
        """
        if effective_end_date is not None and not isinstance(
            effective_end_date, date
        ):
            raise ValueError("effective_end_date must be a date")
        normalized = sorted({
            (str(instrument_id).strip(), parsed_date)
            for instrument_id, parsed_date in event_dates
            if str(instrument_id).strip() and isinstance(parsed_date, date)
        })
        rows: List[Dict[str, Any]] = []
        async with self.get_async_session() as session:
            for offset in range(0, len(normalized), 200):
                chunk = normalized[offset: offset + 200]
                values_sql = ", ".join(
                    f"(:instrument_{index}, :date_{index})"
                    for index in range(len(chunk))
                )
                parameters: Dict[str, Any] = {}
                for index, (instrument_id, source_date) in enumerate(chunk):
                    parameters[f"instrument_{index}"] = instrument_id
                    parameters[f"date_{index}"] = source_date.isoformat()
                parameters["effective_end_date"] = (
                    effective_end_date.isoformat()
                    if effective_end_date is not None else None
                )
                result = await session.execute(text(f"""
                    WITH requested(instrument_id, source_date) AS (
                        VALUES {values_sql}
                    ), requested_state AS MATERIALIZED (
                        SELECT requested.instrument_id,
                               requested.source_date,
                               CASE
                                   WHEN :effective_end_date IS NULL THEN NULL
                                   ELSE (
                                       SELECT q.tradestatus
                                       FROM daily_quotes q
                                       WHERE q.instrument_id
                                             = requested.instrument_id
                                         AND q.time >= datetime(
                                             requested.source_date
                                         )
                                         AND q.time < datetime(
                                             :effective_end_date, '+1 day'
                                         )
                                         AND q.close > 0
                                       ORDER BY q.time
                                       LIMIT 1
                                   )
                               END AS first_quote_tradestatus
                        FROM requested
                    ), evidence AS MATERIALIZED (
                        SELECT requested_state.instrument_id,
                               requested_state.source_date,
                               (
                                   SELECT q.time
                                   FROM daily_quotes q
                                   WHERE q.instrument_id
                                         = requested_state.instrument_id
                                     AND q.time >= datetime(
                                         requested_state.source_date
                                     )
                                     AND q.time < CASE
                                         WHEN :effective_end_date IS NULL
                                         THEN datetime(
                                             requested_state.source_date,
                                             '+15 day'
                                         )
                                         WHEN requested_state
                                              .first_quote_tradestatus = 0
                                         THEN datetime(
                                             :effective_end_date, '+1 day'
                                         )
                                         ELSE min(
                                             datetime(
                                                 requested_state.source_date,
                                                 '+15 day'
                                             ),
                                             datetime(
                                                 :effective_end_date, '+1 day'
                                             )
                                         )
                                     END
                                     AND q.tradestatus = 1
                                     AND q.close > 0
                                   ORDER BY q.time
                                   LIMIT 1
                               ) AS effective_time
                        FROM requested_state
                    )
                    SELECT evidence.instrument_id,
                           evidence.source_date,
                           date(evidence.effective_time) AS effective_date,
                           (
                               SELECT q.close
                               FROM daily_quotes q
                               WHERE q.instrument_id = evidence.instrument_id
                                 AND q.time < evidence.effective_time
                                 AND q.tradestatus = 1
                                 AND q.close > 0
                               ORDER BY q.time DESC
                               LIMIT 1
                           ) AS pre_close,
                           (
                               SELECT q.close
                               FROM daily_quotes q
                               WHERE q.instrument_id = evidence.instrument_id
                                 AND q.time = evidence.effective_time
                                 AND q.tradestatus = 1
                                 AND q.close > 0
                               ORDER BY q.time
                               LIMIT 1
                           ) AS close
                    FROM evidence
                    ORDER BY evidence.instrument_id, evidence.source_date
                """), parameters)
                rows.extend(dict(row) for row in result.mappings().all())
        return rows

    async def replace_canonical_adjustment_factors(
        self,
        rows: List[Dict[str, Any]],
        *,
        series_version: str,
        instrument_ids: Optional[List[str]] = None,
    ) -> int:
        """Replace canonical rows for affected instruments and one version."""
        affected = sorted(set(instrument_ids or [str(row.get("instrument_id")) for row in rows]))
        if not affected:
            return 0
        async with self.get_async_session() as session:
            await session.execute(delete(AdjustmentFactorCanonicalDB).where(
                AdjustmentFactorCanonicalDB.series_version == series_version,
                AdjustmentFactorCanonicalDB.instrument_id.in_(affected),
            ))
            saved = 0
            for row in rows:
                ex_date = self._coerce_datetime(row.get("ex_date"))
                factor = row.get("factor")
                cumulative = row.get("cumulative_factor")
                if ex_date is None or factor is None or cumulative is None:
                    continue
                session.add(AdjustmentFactorCanonicalDB(
                    instrument_id=str(row.get("instrument_id")),
                    ex_date=ex_date,
                    series_version=series_version,
                    factor=float(factor),
                    cumulative_factor=float(cumulative),
                    selected_source=str(row.get("selected_source") or "unknown"),
                    source_profile=str(row.get("source_profile") or "default"),
                    quality_status=str(row.get("quality_status") or "unvalidated"),
                    evidence_count=int(row.get("evidence_count") or 1),
                ))
                saved += 1
            await session.commit()
            return saved

    async def get_canonical_adjustment_factors(
        self,
        instrument_id: str,
        series_version: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        async with self.get_async_session() as session:
            stmt = select(AdjustmentFactorCanonicalDB).where(
                AdjustmentFactorCanonicalDB.instrument_id == instrument_id,
                AdjustmentFactorCanonicalDB.series_version == series_version,
            )
            if start_date:
                stmt = stmt.where(AdjustmentFactorCanonicalDB.ex_date >= start_date)
            if end_date:
                stmt = stmt.where(AdjustmentFactorCanonicalDB.ex_date <= end_date)
            rows = (await session.execute(
                stmt.order_by(AdjustmentFactorCanonicalDB.ex_date)
            )).scalars().all()
            return [{
                "instrument_id": row.instrument_id,
                "ex_date": row.ex_date,
                "factor": row.factor,
                "cumulative_factor": row.cumulative_factor,
                "source": row.selected_source,
                "source_profile": row.source_profile,
                "quality_status": row.quality_status,
                "series_version": row.series_version,
            } for row in rows]

    async def get_canonical_adjustment_factor_page(
        self,
        *,
        series_version: str,
        instrument_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        async with self.get_async_session() as session:
            filters = [AdjustmentFactorCanonicalDB.series_version == series_version]
            if instrument_id:
                filters.append(AdjustmentFactorCanonicalDB.instrument_id == instrument_id)
            if start_date:
                filters.append(
                    AdjustmentFactorCanonicalDB.ex_date >= self._coerce_datetime(start_date)
                )
            if end_date:
                filters.append(
                    AdjustmentFactorCanonicalDB.ex_date
                    < self._coerce_datetime(end_date + timedelta(days=1))
                )
            total = await session.scalar(
                select(func.count()).select_from(AdjustmentFactorCanonicalDB).where(*filters)
            )
            rows = (await session.execute(
                select(AdjustmentFactorCanonicalDB).where(*filters)
                .order_by(AdjustmentFactorCanonicalDB.instrument_id, AdjustmentFactorCanonicalDB.ex_date)
                .offset(offset).limit(limit)
            )).scalars().all()
            items = [{
                "instrument_id": row.instrument_id, "ex_date": row.ex_date,
                "factor": row.factor, "cumulative_factor": row.cumulative_factor,
                "series_version": row.series_version, "selected_source": row.selected_source,
                "source_profile": row.source_profile, "quality_status": row.quality_status,
                "evidence_count": row.evidence_count,
            } for row in rows]
            return {
                "total": int(total or 0), "limit": limit, "offset": offset,
                "returned": len(items), "has_more": offset + len(items) < int(total or 0),
                "items": items,
            }

    async def upsert_adjustment_factor_series_status(
        self,
        series_version: str,
        report: Dict[str, Any],
    ) -> None:
        async with self.get_async_session() as session:
            existing = await session.get(AdjustmentFactorSeriesStatusDB, series_version)
            values = self._adjustment_factor_series_status_values(report)
            if existing is None:
                session.add(AdjustmentFactorSeriesStatusDB(series_version=series_version, **values))
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
            await session.commit()

    def _adjustment_factor_series_status_values(
        self,
        report: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build one normalized persistence payload for series quality status."""
        return {
            "status": str(report.get("status") or "built"),
            "source_priority_json": json.dumps(
                report.get("source_priority") or [], ensure_ascii=True
            ),
            "start_date": self._coerce_datetime(report.get("start_date")),
            "end_date": self._coerce_datetime(report.get("end_date")),
            "instrument_count": int(report.get("instrument_count") or 0),
            "row_count": int(report.get("row_count") or 0),
            "coverage_ratio": float(report.get("coverage_ratio") or 0.0),
            "conflict_count": int(report.get("conflict_count") or 0),
            "max_cumulative_error_pct": report.get("max_cumulative_error_pct"),
            "promotion_eligible": bool(report.get("promotion_eligible")),
            "report_json": json.dumps(
                report, ensure_ascii=True, default=str, sort_keys=True
            ),
        }

    async def get_adjustment_factor_series_status(self, series_version: str) -> Optional[Dict[str, Any]]:
        async with self.get_async_session() as session:
            row = await session.get(AdjustmentFactorSeriesStatusDB, series_version)
            if row is None:
                return None
            return json.loads(row.report_json or "{}") | {
                "series_version": row.series_version,
                "status": row.status,
                "promotion_eligible": bool(row.promotion_eligible),
            }

    async def replace_adjustment_factor_instrument_statuses(
        self,
        rows: List[Dict[str, Any]],
        *,
        series_version: str,
        instrument_ids: Optional[List[str]] = None,
    ) -> int:
        """Replace per-instrument completeness states for one series version."""
        affected = sorted(set(
            instrument_ids or [str(row.get("instrument_id") or "") for row in rows]
        ) - {""})
        if not affected:
            return 0
        async with self.get_async_session() as session:
            await session.execute(delete(AdjustmentFactorInstrumentStatusDB).where(
                AdjustmentFactorInstrumentStatusDB.series_version == series_version,
                AdjustmentFactorInstrumentStatusDB.instrument_id.in_(affected),
            ))
            saved = 0
            for row in rows:
                instrument_id = str(row.get("instrument_id") or "").strip()
                coverage_status = str(row.get("coverage_status") or "").strip()
                if not instrument_id or not coverage_status:
                    continue
                session.add(AdjustmentFactorInstrumentStatusDB(
                    instrument_id=instrument_id,
                    series_version=series_version,
                    source=str(row.get("source") or "unknown"),
                    coverage_status=coverage_status,
                    event_count=int(row.get("event_count") or 0),
                    start_date=self._coerce_datetime(row.get("start_date")),
                    end_date=self._coerce_datetime(row.get("end_date")),
                    ingestion_run_id=row.get("ingestion_run_id"),
                ))
                saved += 1
            await session.commit()
            return saved

    async def get_adjustment_factor_instrument_status(
        self,
        instrument_id: str,
        series_version: str,
    ) -> Optional[Dict[str, Any]]:
        """Return canonical completeness for one instrument and version."""
        async with self.get_async_session() as session:
            row = (await session.execute(
                select(AdjustmentFactorInstrumentStatusDB).where(
                    AdjustmentFactorInstrumentStatusDB.instrument_id == instrument_id,
                    AdjustmentFactorInstrumentStatusDB.series_version == series_version,
                )
            )).scalar_one_or_none()
            if row is None:
                return None
            return {
                "instrument_id": row.instrument_id,
                "series_version": row.series_version,
                "source": row.source,
                "coverage_status": row.coverage_status,
                "event_count": row.event_count,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "ingestion_run_id": row.ingestion_run_id,
            }

    async def promote_canonical_adjustment_factor_series(
        self,
        *,
        staging_series_version: str,
        target_series_version: str,
        report: Dict[str, Any],
    ) -> Dict[str, int]:
        """Atomically replace one production series from a validated staging version."""
        if staging_series_version == target_series_version:
            raise ValueError("staging and target series versions must differ")
        async with self.get_async_session() as session:
            staging_rows = (await session.execute(
                select(AdjustmentFactorCanonicalDB).where(
                    AdjustmentFactorCanonicalDB.series_version == staging_series_version
                )
            )).scalars().all()
            staging_statuses = (await session.execute(
                select(AdjustmentFactorInstrumentStatusDB).where(
                    AdjustmentFactorInstrumentStatusDB.series_version == staging_series_version
                )
            )).scalars().all()
            if not staging_statuses:
                raise RuntimeError("staging series has no instrument coverage states")

            await session.execute(delete(AdjustmentFactorCanonicalDB).where(
                AdjustmentFactorCanonicalDB.series_version == target_series_version
            ))
            await session.execute(delete(AdjustmentFactorInstrumentStatusDB).where(
                AdjustmentFactorInstrumentStatusDB.series_version == target_series_version
            ))
            for row in staging_rows:
                session.add(AdjustmentFactorCanonicalDB(
                    instrument_id=row.instrument_id,
                    ex_date=row.ex_date,
                    series_version=target_series_version,
                    factor=row.factor,
                    cumulative_factor=row.cumulative_factor,
                    selected_source=row.selected_source,
                    source_profile=row.source_profile,
                    quality_status=row.quality_status,
                    evidence_count=row.evidence_count,
                ))
            for row in staging_statuses:
                session.add(AdjustmentFactorInstrumentStatusDB(
                    instrument_id=row.instrument_id,
                    series_version=target_series_version,
                    source=row.source,
                    coverage_status=row.coverage_status,
                    event_count=row.event_count,
                    start_date=row.start_date,
                    end_date=row.end_date,
                    ingestion_run_id=row.ingestion_run_id,
                ))

            target_report = {
                **report,
                "series_version": target_series_version,
                "staging_series_version": staging_series_version,
                "status": "promoted",
                "promotion_eligible": True,
            }
            existing = await session.get(
                AdjustmentFactorSeriesStatusDB, target_series_version
            )
            values = self._adjustment_factor_series_status_values(target_report)
            values["row_count"] = len(staging_rows)
            if existing is None:
                session.add(AdjustmentFactorSeriesStatusDB(
                    series_version=target_series_version, **values
                ))
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                existing.updated_at = get_shanghai_time()
            await session.commit()
            return {
                "canonical_rows": len(staging_rows),
                "instrument_statuses": len(staging_statuses),
            }

    async def prepare_legacy_factor_appends(
        self,
        factors: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
        """Rebase only new tail events; historical rows remain observation-only."""
        from data_sources.adjustment_factor_governance import rebase_legacy_tail

        stats = {"rebased": 0, "historical_skipped": 0, "invalid": 0}
        prepared: List[Dict[str, Any]] = []
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for factor in factors:
            grouped.setdefault(str(factor.get("instrument_id") or ""), []).append(factor)
        async with self.get_async_session() as session:
            for instrument_id, items in grouped.items():
                if not instrument_id:
                    stats["invalid"] += len(items)
                    continue
                latest = (await session.execute(
                    select(AdjustmentFactorDB).where(
                        AdjustmentFactorDB.instrument_id == instrument_id
                    ).order_by(AdjustmentFactorDB.ex_date.desc()).limit(1)
                )).scalar_one_or_none()
                rows, row_stats = rebase_legacy_tail(
                    items,
                    latest_date=latest.ex_date if latest else None,
                    latest_cumulative_factor=(
                        float(latest.cumulative_factor) if latest else 1.0
                    ),
                )
                prepared.extend(rows)
                for key in stats:
                    stats[key] += int(row_stats.get(key, 0))
        return prepared, stats

    async def get_xdxr_pre_close_overrides(
        self,
        instrument_id: str,
        event_dates: List[Union[datetime, date, str]],
    ) -> Dict[date, float]:
        """Resolve local raw prior closes for XDXR factor derivation.

        A suspended same-day placeholder keeps the preceding raw close in
        ``pre_close``. On a normally traded ex-date, ``pre_close`` may instead
        be the ex-right reference price, so the latest earlier trading close
        is used.
        """
        normalized_dates = sorted({
            parsed.date()
            for value in event_dates
            if (parsed := self._coerce_datetime(value)) is not None
        })
        if not instrument_id or not normalized_dates:
            return {}

        try:
            statements = []
            for event_date in normalized_dates:
                event_start = datetime.combine(event_date, datetime.min.time())
                event_end = event_start + timedelta(days=1)
                suspended_pre_close = (
                    select(DailyQuoteDB.pre_close)
                    .where(
                        DailyQuoteDB.instrument_id == instrument_id,
                        DailyQuoteDB.time >= event_start,
                        DailyQuoteDB.time < event_end,
                        DailyQuoteDB.tradestatus != 1,
                        DailyQuoteDB.pre_close > 0,
                    )
                    .order_by(DailyQuoteDB.time.desc())
                    .limit(1)
                    .scalar_subquery()
                )
                prior_trading_close = (
                    select(DailyQuoteDB.close)
                    .where(
                        DailyQuoteDB.instrument_id == instrument_id,
                        DailyQuoteDB.time < event_start,
                        DailyQuoteDB.tradestatus == 1,
                        DailyQuoteDB.close > 0,
                    )
                    .order_by(DailyQuoteDB.time.desc())
                    .limit(1)
                    .scalar_subquery()
                )
                statements.append(select(
                    literal(event_start).label('ex_date'),
                    suspended_pre_close.label('suspended_pre_close'),
                    prior_trading_close.label('prior_trading_close'),
                ))

            stmt = statements[0] if len(statements) == 1 else union_all(*statements)
            async with self.get_async_session() as session:
                rows = (await session.execute(stmt)).mappings().all()

            overrides: Dict[date, float] = {}
            for row in rows:
                event_dt = self._coerce_datetime(row.get('ex_date'))
                pre_close = self._resolve_xdxr_pre_close_candidate(
                    row.get('suspended_pre_close'),
                    row.get('prior_trading_close'),
                )
                if event_dt is not None and pre_close > 0:
                    overrides[event_dt.date()] = pre_close
            return overrides
        except Exception as exc:
            self.db_logger.error(
                "Failed to resolve XDXR pre-close overrides for %s: %s",
                instrument_id,
                exc,
            )
            return {}

    @staticmethod
    def _resolve_xdxr_pre_close_candidate(
        suspended_pre_close: Any,
        prior_trading_close: Any,
    ) -> float:
        """Choose safe local XDXR evidence in financial-semantic order."""
        for candidate in (suspended_pre_close, prior_trading_close):
            try:
                value = float(candidate or 0.0)
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
        return 0.0

    async def get_tdx_audit_factors(
        self,
        instrument_id: Optional[str] = None,
        start_date: Optional[Union[datetime, date]] = None,
        end_date: Optional[Union[datetime, date]] = None,
        validation_result: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return a filtered, stable page from the TDX-only audit table."""
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))
        conditions = []
        if instrument_id:
            conditions.append(AdjustmentFactorTdxDB.instrument_id == instrument_id)
        if start_date:
            start_dt = self._coerce_datetime(start_date)
            if start_dt is not None:
                conditions.append(
                    AdjustmentFactorTdxDB.ex_date >= datetime.combine(
                        start_dt.date(), datetime.min.time()
                    )
                )
        if end_date:
            end_dt = self._coerce_datetime(end_date)
            if end_dt is not None:
                conditions.append(
                    AdjustmentFactorTdxDB.ex_date < datetime.combine(
                        end_dt.date() + timedelta(days=1), datetime.min.time()
                    )
                )
        if validation_result:
            conditions.append(
                AdjustmentFactorTdxDB.validation_result == validation_result
            )

        try:
            async with self.get_async_session() as session:
                count_stmt = select(func.count()).select_from(AdjustmentFactorTdxDB)
                if conditions:
                    count_stmt = count_stmt.where(*conditions)
                total = int((await session.execute(count_stmt)).scalar_one() or 0)

                stmt = select(AdjustmentFactorTdxDB)
                if conditions:
                    stmt = stmt.where(*conditions)
                stmt = (
                    stmt.order_by(
                        AdjustmentFactorTdxDB.instrument_id.asc(),
                        AdjustmentFactorTdxDB.ex_date.asc(),
                        AdjustmentFactorTdxDB.id.asc(),
                    )
                    .offset(offset)
                    .limit(limit)
                )
                rows = (await session.execute(stmt)).scalars().all()

            items = [{
                'instrument_id': row.instrument_id,
                'ex_date': row.ex_date,
                'factor': row.factor,
                'cumulative_factor': row.cumulative_factor,
                'pre_close': row.pre_close,
                'fenhong': row.fenhong,
                'songzhuangu': row.songzhuangu,
                'peigu': row.peigu,
                'peigujia': row.peigujia,
                'validation_result': row.validation_result,
                'ref_factor': row.ref_factor,
                'ref_source': row.ref_source,
                'ratio_diff_pct': row.ratio_diff_pct,
                'conflict_reason': row.conflict_reason,
                'source': row.source,
                'created_at': row.created_at,
                'updated_at': row.updated_at,
            } for row in rows]
            return {
                'items': items,
                'total': total,
                'limit': limit,
                'offset': offset,
                'returned': len(items),
                'has_more': offset + len(items) < total,
            }
        except Exception as exc:
            self.db_logger.error("Failed to query TDX audit factors: %s", exc)
            return {
                'items': [],
                'total': 0,
                'limit': limit,
                'offset': offset,
                'returned': 0,
                'has_more': False,
            }

    async def save_tdx_audit_factors(
        self,
        factors: list[dict[str, Any]],
        *,
        preserve_computed_fields: bool = False,
    ) -> int:
        """保存通达信自研复权因子到审计表 (upsert 语义)

        ★ 写入 adjustment_factors_tdx, 不碰生产表 adjustment_factors!

        Args:
            factors: 因子列表, 每项含:
                instrument_id, ex_date, factor, cumulative_factor,
                pre_close, fenhong, songzhuangu, peigu, peigujia,
                validation_result, ref_factor, ref_source, ratio_diff_pct, source
            preserve_computed_fields: 更新已存在行时只刷新原始 XDXR 字段，
                保留已计算的因子、前收盘和验证结果。用于原始事件历史回补。

        Returns:
            成功保存/更新的记录数
        """
        if not factors:
            return 0

        saved_count = 0
        try:
            async with self.get_async_session() as session:
                for f in factors:
                    try:
                        instrument_id = f.get('instrument_id')
                        ex_date = f.get('ex_date')
                        if not instrument_id or not ex_date:
                            continue

                        # 检查是否已存在
                        from sqlalchemy import select
                        stmt = select(AdjustmentFactorTdxDB).where(
                            AdjustmentFactorTdxDB.instrument_id == instrument_id,
                            AdjustmentFactorTdxDB.ex_date == ex_date
                        )
                        result = await session.execute(stmt)
                        existing = result.scalar_one_or_none()

                        if existing:
                            existing.fenhong = float(f.get('fenhong', 0.0))
                            existing.songzhuangu = float(f.get('songzhuangu', 0.0))
                            existing.peigu = float(f.get('peigu', 0.0))
                            existing.peigujia = float(f.get('peigujia', 0.0))
                            existing.source = f.get('source', 'tdx_xdxr')
                            if not preserve_computed_fields:
                                existing.factor = float(f.get('factor', 1.0))
                                existing.cumulative_factor = float(f.get('cumulative_factor', 1.0))
                                existing.pre_close = float(f.get('pre_close', 0.0))
                                existing.validation_result = f.get('validation_result')
                                existing.ref_factor = (
                                    float(f['ref_factor']) if f.get('ref_factor') is not None else None
                                )
                                existing.ref_source = f.get('ref_source')
                                existing.ratio_diff_pct = (
                                    float(f['ratio_diff_pct']) if f.get('ratio_diff_pct') is not None else None
                                )
                                existing.conflict_reason = f.get('conflict_reason')
                        else:
                            new_record = AdjustmentFactorTdxDB(
                                instrument_id=instrument_id,
                                ex_date=ex_date,
                                factor=float(f.get('factor', 1.0)),
                                cumulative_factor=float(f.get('cumulative_factor', 1.0)),
                                pre_close=float(f.get('pre_close', 0.0)),
                                fenhong=float(f.get('fenhong', 0.0)),
                                songzhuangu=float(f.get('songzhuangu', 0.0)),
                                peigu=float(f.get('peigu', 0.0)),
                                peigujia=float(f.get('peigujia', 0.0)),
                                validation_result=f.get('validation_result'),
                                ref_factor=(
                                    float(f['ref_factor'])
                                    if f.get('ref_factor') is not None else None
                                ),
                                ref_source=f.get('ref_source'),
                                ratio_diff_pct=(
                                    float(f['ratio_diff_pct'])
                                    if f.get('ratio_diff_pct') is not None else None
                                ),
                                conflict_reason=f.get('conflict_reason'),
                                source=f.get('source', 'tdx_xdxr'),
                            )
                            session.add(new_record)

                        saved_count += 1
                    except Exception as row_e:
                        self.db_logger.warning(
                            "Failed to save tdx audit factor for %s: %s",
                            f.get('instrument_id'), row_e
                        )
                        continue

                await session.commit()

            self.db_logger.info(
                "Saved %d tdx audit factors to adjustment_factors_tdx", saved_count
            )
            return saved_count

        except Exception as e:
            self.db_logger.error("Failed to save tdx audit factors: %s", e)
            return 0


# Global instance
database_operations = DatabaseOperations()
