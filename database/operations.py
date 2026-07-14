"""
database operations for the quote system.
Supports comprehensive data management with new schema.
"""

import asyncio
import hashlib
import json
import math
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from sqlalchemy import text, func, desc, asc, tuple_
from sqlalchemy.orm import sessionmaker
from utils.date_utils import get_shanghai_time
from utils import db_logger, config_manager


# 异步查询需要 select
from sqlalchemy.future import select
from .connection import db_manager
from .models import (
    InstrumentDB, DailyQuoteDB, TradingCalendarDB, TradingSessionDB,
    DataUpdateDB, DataSourceStatusDB, AdjustmentFactorDB, AdjustmentFactorTdxDB,
    DataChangeLogDB,
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

    async def save_tdx_audit_factors(
        self, factors: list[dict[str, Any]]
    ) -> int:
        """保存通达信自研复权因子到审计表 (upsert 语义)

        ★ 写入 adjustment_factors_tdx, 不碰生产表 adjustment_factors!

        Args:
            factors: 因子列表, 每项含:
                instrument_id, ex_date, factor, cumulative_factor,
                pre_close, fenhong, songzhuangu, peigu, peigujia,
                validation_result, ref_factor, ref_source, ratio_diff_pct, source

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
                            existing.factor = float(f.get('factor', 1.0))
                            existing.cumulative_factor = float(f.get('cumulative_factor', 1.0))
                            existing.pre_close = float(f.get('pre_close', 0.0))
                            existing.fenhong = float(f.get('fenhong', 0.0))
                            existing.songzhuangu = float(f.get('songzhuangu', 0.0))
                            existing.peigu = float(f.get('peigu', 0.0))
                            existing.peigujia = float(f.get('peigujia', 0.0))
                            existing.validation_result = f.get('validation_result')
                            existing.ref_factor = (
                                float(f['ref_factor']) if f.get('ref_factor') is not None else None
                            )
                            existing.ref_source = f.get('ref_source')
                            existing.ratio_diff_pct = (
                                float(f['ratio_diff_pct']) if f.get('ratio_diff_pct') is not None else None
                            )
                            existing.conflict_reason = f.get('conflict_reason')
                            existing.source = f.get('source', 'tdx_xdxr')
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
