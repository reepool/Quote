"""
Database connection management.
Provides SQLite database connection with async support and connection pooling.
"""

import os
from datetime import date, datetime
from utils import db_logger, config_manager

import aiosqlite
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, StaticPool


_db_workload: ContextVar[str] = ContextVar("quote_db_workload", default="task")


def get_current_db_workload() -> str:
    """Return the current DB workload class for async session routing."""
    return _db_workload.get()


@asynccontextmanager
async def db_workload_context(workload: str):
    """Temporarily route async DB sessions for the current async context."""
    normalized = "api" if str(workload).lower() == "api" else "task"
    token = _db_workload.set(normalized)
    try:
        yield
    finally:
        _db_workload.reset(token)


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: str = config_manager.get_nested('database_config.db_path')):
        self.db_path = db_path 
        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        db_logger.info(f"[Database] Using database path: {db_path}")
        self.sync_engine = None
        self.async_engine = None
        self.api_async_engine = None
        self.task_async_engine = None
        self.SessionLocal = None
        self.AsyncSessionLocal = None
        self.ApiAsyncSessionLocal = None
        self.TaskAsyncSessionLocal = None

    def initialize(self):
        """初始化数据库连接"""
        try:
            legacy_async_pool_config = (
                config_manager.get_nested('database_config.async_pool', {}) or {}
            )
            task_async_pool_config = (
                config_manager.get_nested('database_config.task_async_pool', {})
                or legacy_async_pool_config
                or {}
            )
            api_async_pool_config = (
                config_manager.get_nested('database_config.api_async_pool', {})
                or legacy_async_pool_config
                or {}
            )

            # 同步连接引擎
            self.sync_engine = create_engine(
                f"sqlite:///{self.db_path}",
                poolclass=StaticPool,
                connect_args={"check_same_thread": False}
            )

            # 注册连接层面 PRAGMA 保证每个协程获取到的连接都开启最优特性
            def set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA cache_size=-64000")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

            event.listen(self.sync_engine, 'connect', set_sqlite_pragma)

            self.task_async_engine = self._create_async_engine(
                "task",
                task_async_pool_config,
                default_pool_size=8,
                default_max_overflow=0,
                default_pool_timeout=30,
                pragma_listener=set_sqlite_pragma,
            )
            self.api_async_engine = self._create_async_engine(
                "api",
                api_async_pool_config,
                default_pool_size=2,
                default_max_overflow=6,
                default_pool_timeout=30,
                pragma_listener=set_sqlite_pragma,
            )

            # Backward-compatible aliases: unclassified internal work uses task pool.
            self.async_engine = self.task_async_engine

            # 创建会话工厂
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.sync_engine
            )

            self.TaskAsyncSessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.task_async_engine,
                class_=AsyncSession
            )
            self.ApiAsyncSessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.api_async_engine,
                class_=AsyncSession
            )
            self.AsyncSessionLocal = self.TaskAsyncSessionLocal

            self._ensure_change_watermark_schema()
            self._ensure_adjustment_factor_governance_schema()
            self._ensure_corporate_action_governance_schema()
            self._ensure_backtest_data_schema()

            db_logger.info("[Database] Database connection initialized successfully")

        except Exception as e:
            db_logger.error(f"[Database] Failed to initialize database: {e}")
            raise

    def _ensure_backtest_data_schema(self) -> None:
        """Create additive PIT backtest tables without acquiring data."""
        from research.backtest_data.quote_store import BacktestQuoteStore

        BacktestQuoteStore(self.db_path).initialize()

    def _ensure_adjustment_factor_governance_schema(self) -> None:
        """Create additive adjustment-factor governance tables on existing DBs."""
        from .models import (
            AdjustmentFactorCanonicalDB,
            AdjustmentFactorDecisionDB,
            AdjustmentFactorInstrumentStatusDB,
            AdjustmentFactorObservationDB,
            AdjustmentFactorSeriesStatusDB,
            OperationalWatermarkDB,
        )

        tables = (
            AdjustmentFactorObservationDB.__table__,
            AdjustmentFactorCanonicalDB.__table__,
            AdjustmentFactorSeriesStatusDB.__table__,
            AdjustmentFactorDecisionDB.__table__,
            AdjustmentFactorInstrumentStatusDB.__table__,
            OperationalWatermarkDB.__table__,
        )
        with self.sync_engine.begin() as connection:
            for table in tables:
                table.create(bind=connection, checkfirst=True)

            instruments_table_exists = connection.execute(text(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='instruments'"
            )).scalar_one_or_none()
            if instruments_table_exists is None:
                return

            obsolete_profiles = (
                "akshare_tencent_price_ratio_v1",
                "akshare_eastmoney_price_ratio_v1",
            )
            obsolete_series = ("akshare_market_price_ratio_snapshot_v1",)
            deleted_observations = connection.execute(
                AdjustmentFactorObservationDB.__table__.delete().where(
                    AdjustmentFactorObservationDB.source_profile.in_(
                        obsolete_profiles
                    )
                )
            ).rowcount
            deleted_statuses = connection.execute(
                AdjustmentFactorInstrumentStatusDB.__table__.delete().where(
                    AdjustmentFactorInstrumentStatusDB.series_version.in_(
                        obsolete_series
                    )
                )
            ).rowcount
            deleted_total = sum(
                int(value or 0)
                for value in (
                    deleted_observations,
                    deleted_statuses,
                )
            )
            if deleted_total:
                db_logger.info(
                    "[Database] Removed %d obsolete A-share price-ratio "
                    "factor governance rows",
                    deleted_total,
                )

    def _ensure_corporate_action_governance_schema(self) -> None:
        """Create additive official corporate-action tables on existing DBs."""
        from .models import (
            CorporateActionEffectiveDateEvidenceDB,
            CorporateActionInstrumentStatusDB,
            CorporateActionObservationDB,
            CorporateActionDocumentArtifactDB,
            CorporateActionDocumentPageDB,
            CorporateActionLlmAnalysisDB,
            CorporateActionResolutionReviewDB,
            CorporateActionResolvedTermsDB,
            CorporateActionResolutionStateDB,
        )

        tables = (
            CorporateActionObservationDB.__table__,
            CorporateActionInstrumentStatusDB.__table__,
            CorporateActionEffectiveDateEvidenceDB.__table__,
            CorporateActionDocumentArtifactDB.__table__,
            CorporateActionDocumentPageDB.__table__,
            CorporateActionLlmAnalysisDB.__table__,
            CorporateActionResolutionReviewDB.__table__,
            CorporateActionResolvedTermsDB.__table__,
            CorporateActionResolutionStateDB.__table__,
        )
        with self.sync_engine.begin() as connection:
            for table in tables:
                table.create(bind=connection, checkfirst=True)

            observation_columns = {
                row[1]
                for row in connection.execute(
                    text("PRAGMA table_info(corporate_action_observations)")
                ).fetchall()
            }
            additive_columns = (
                ("is_current", "BOOLEAN NOT NULL DEFAULT 1"),
                ("last_seen_run_id", "VARCHAR(64)"),
                ("retired_at", "DATETIME"),
                ("retired_run_id", "VARCHAR(64)"),
                ("retirement_reason", "VARCHAR(64)"),
            )
            for column_name, column_type in additive_columns:
                if column_name not in observation_columns:
                    connection.execute(text(
                        "ALTER TABLE corporate_action_observations "
                        f"ADD COLUMN {column_name} {column_type}"
                    ))
            connection.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_corporate_action_observation_current "
                "ON corporate_action_observations(is_current)"
            ))
            desired_unique_columns = (
                "instrument_id",
                "source",
                "source_profile",
                "requested_start_date",
                "requested_end_date",
            )
            unique_indexes = []
            for index_row in connection.execute(text(
                "PRAGMA index_list(corporate_action_instrument_status)"
            )).fetchall():
                if not bool(index_row[2]):
                    continue
                index_name = str(index_row[1]).replace("'", "''")
                unique_indexes.append(tuple(
                    row[2]
                    for row in connection.execute(text(
                        f"PRAGMA index_info('{index_name}')"
                    )).fetchall()
                ))
            if desired_unique_columns not in unique_indexes:
                legacy_rows = [
                    dict(row)
                    for row in connection.execute(text(
                        "SELECT * FROM corporate_action_instrument_status"
                    )).mappings().all()
                ]
                connection.execute(text(
                    "DROP TABLE corporate_action_instrument_status"
                ))
                CorporateActionInstrumentStatusDB.__table__.create(
                    bind=connection,
                    checkfirst=False,
                )

                def coerce_legacy_datetime(value):
                    if isinstance(value, datetime):
                        return value.replace(tzinfo=None)
                    if isinstance(value, date):
                        return datetime(value.year, value.month, value.day)
                    if value is None:
                        return None
                    try:
                        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(
                            tzinfo=None
                        )
                    except ValueError:
                        return None

                datetime_columns = (
                    "requested_start_date",
                    "requested_end_date",
                    "earliest_event_date",
                    "latest_event_date",
                    "last_attempt_at",
                    "created_at",
                    "updated_at",
                )
                for legacy_row in legacy_rows:
                    for column_name in datetime_columns:
                        legacy_row[column_name] = coerce_legacy_datetime(
                            legacy_row.get(column_name)
                        )
                    if (
                        legacy_row.get("requested_start_date") is None
                        or legacy_row.get("requested_end_date") is None
                    ):
                        legacy_row["requested_start_date"] = datetime(1900, 1, 1)
                        legacy_row["requested_end_date"] = datetime(1900, 1, 1)
                        legacy_row["coverage_status"] = "indeterminate"
                        previous_error = str(legacy_row.get("error_message") or "").strip()
                        migration_error = "legacy coverage interval was unavailable"
                        legacy_row["error_message"] = (
                            f"{previous_error}; {migration_error}"
                            if previous_error else migration_error
                        )
                    connection.execute(
                        CorporateActionInstrumentStatusDB.__table__.insert(),
                        legacy_row,
                    )

    def _create_async_engine(
        self,
        role: str,
        pool_config: dict,
        *,
        default_pool_size: int,
        default_max_overflow: int,
        default_pool_timeout: float,
        pragma_listener,
    ):
        pool_size = int(pool_config.get('pool_size', default_pool_size) or default_pool_size)
        max_overflow = int(
            pool_config.get('max_overflow', default_max_overflow) or default_max_overflow
        )
        pool_timeout = float(
            pool_config.get('pool_timeout_seconds', default_pool_timeout)
            or default_pool_timeout
        )

        engine = create_async_engine(
            f"sqlite+aiosqlite:///{self.db_path}",
            # QueuePool waits synchronously for a connection.  Under an
            # asyncio workload that can block the event loop before the
            # coroutine holding a connection gets a chance to release it.
            poolclass=AsyncAdaptedQueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            connect_args={"check_same_thread": False}
        )
        event.listen(engine.sync_engine, 'connect', pragma_listener)
        db_logger.info(
            "[Database] %s async pool configured: pool_size=%s max_overflow=%s "
            "pool_timeout=%ss capacity=%s",
            role,
            pool_size,
            max_overflow,
            pool_timeout,
            pool_size + max_overflow,
        )
        return engine

    def create_tables(self):
        """创建数据库表"""
        try:
            from .models import Base

            # 创建表
            Base.metadata.create_all(bind=self.sync_engine)
            db_logger.info("[Database] Database tables created successfully")

            # 创建索引
            self._create_indexes()

        except Exception as e:
            db_logger.error(f"[Database] Failed to create tables: {e}")
            raise

    def _create_indexes(self):
        """创建数据库索引"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_instruments_exchange_type ON instruments(exchange, type)",
            "CREATE INDEX IF NOT EXISTS idx_instruments_symbol ON instruments(symbol)",
        ]

        with self.sync_engine.connect() as conn:
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                    db_logger.debug(f"[Database] Created index: {index_sql}")
                except Exception as e:
                    # 区分不同类型的错误
                    if "already exists" in str(e).lower():
                        db_logger.debug(f"[Database] Index already exists: {index_sql}")
                    elif "permission" in str(e).lower():
                        db_logger.error(f"[Database] Permission denied creating index {index_sql}: {e}")
                        raise  # 权限错误需要抛出
                    else:
                        db_logger.warning(f"[Database] Failed to create index {index_sql}: {e}")
                    # 继续执行其他索引创建

            conn.commit()

    def _ensure_change_watermark_schema(self):
        """Apply additive schema pieces needed by local change watermarks."""
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS data_change_log (
                sequence_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                domain VARCHAR(32) NOT NULL,
                dataset VARCHAR(64) NOT NULL,
                change_type VARCHAR(32) NOT NULL,
                business_key_json TEXT NOT NULL,
                instrument_id VARCHAR(32),
                series_id VARCHAR(64),
                observation_date DATETIME,
                period VARCHAR(32),
                old_hash VARCHAR(64),
                new_hash VARCHAR(64),
                row_version INTEGER,
                source VARCHAR(32),
                source_mode VARCHAR(32),
                source_profile VARCHAR(64),
                ingestion_run_id VARCHAR(64),
                batch_id VARCHAR(64),
                changed_at DATETIME NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_daily_quotes_row_hash ON daily_quotes(row_hash)",
            "CREATE INDEX IF NOT EXISTS idx_adj_factor_row_hash ON adjustment_factors(row_hash)",
            "CREATE INDEX IF NOT EXISTS idx_change_log_domain_sequence ON data_change_log(domain, sequence_id)",
            "CREATE INDEX IF NOT EXISTS idx_change_log_dataset_sequence ON data_change_log(dataset, sequence_id)",
            "CREATE INDEX IF NOT EXISTS idx_change_log_domain_dataset_sequence ON data_change_log(domain, dataset, sequence_id)",
            "CREATE INDEX IF NOT EXISTS idx_change_log_instrument_date ON data_change_log(instrument_id, observation_date)",
            "CREATE INDEX IF NOT EXISTS idx_change_log_series_date ON data_change_log(series_id, observation_date)",
        ]
        additive_columns = {
            "daily_quotes": [
                ("row_hash", "VARCHAR(64)"),
                ("row_version", "INTEGER NOT NULL DEFAULT 1"),
            ],
            "adjustment_factors": [
                ("row_hash", "VARCHAR(64)"),
                ("row_version", "INTEGER NOT NULL DEFAULT 1"),
            ],
            "corporate_action_llm_analyses": [
                ("source_label", "VARCHAR(128)"),
                ("selected_profile", "VARCHAR(128)"),
                ("route_fingerprint", "VARCHAR(64)"),
                ("lineage_json", "TEXT"),
                ("failover_count", "INTEGER NOT NULL DEFAULT 0"),
            ],
        }

        with self.sync_engine.connect() as conn:
            existing_tables = {
                row[0]
                for row in conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                ).fetchall()
            }

            for table_name, columns in additive_columns.items():
                if table_name not in existing_tables:
                    continue
                existing_columns = {
                    row[1]
                    for row in conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
                }
                for column_name, column_type in columns:
                    if column_name not in existing_columns:
                        conn.execute(
                            text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                        )

            for stmt in ddl:
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    message = str(e).lower()
                    if "no such table" in message and (
                        "daily_quotes" in message or "adjustment_factors" in message
                    ):
                        continue
                    raise
            conn.commit()

    def get_session(self) -> Session:
        """获取同步数据库会话"""
        if not self.SessionLocal:
            raise RuntimeError("Database not initialized")
        return self.SessionLocal()

    def get_async_session(self, workload: str = None) -> AsyncSession:
        """获取异步数据库会话"""
        if not self.TaskAsyncSessionLocal or not self.ApiAsyncSessionLocal:
            raise RuntimeError("Database not initialized")
        normalized = workload or get_current_db_workload()
        if str(normalized).lower() == "api":
            return self.ApiAsyncSessionLocal()
        return self.TaskAsyncSessionLocal()

    @asynccontextmanager
    async def get_async_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """获取异步SQLite连接"""
        conn = None
        try:
            conn = await aiosqlite.connect(self.db_path)
            await conn.execute("PRAGMA journal_mode = WAL")  # 开启WAL模式以提高并发写入性能
            await conn.execute("PRAGMA synchronous = NORMAL") # 在WAL模式下是安全的折衷方案
            await conn.execute("PRAGMA foreign_keys = ON")   # 确保外键约束生效
            yield conn
        finally:
            if conn:
                await conn.close()

    def backup_database(self, backup_path: str = None) -> bool:
        """备份数据库"""
        try:
            import shutil
            from datetime import datetime

            if not backup_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_dir = os.path.join(os.path.dirname(self.db_path), "backups")
                os.makedirs(backup_dir, exist_ok=True)
                backup_path = os.path.join(backup_dir, f"quotes_backup_{timestamp}.db")

            shutil.copy2(self.db_path, backup_path)
            db_logger.info(f"[Database] Database backed up to: {backup_path}")
            return True

        except Exception as e:
            db_logger.error(f"[Database] Failed to backup database: {e}")
            return False

    def close(self):
        """关闭数据库连接"""
        try:
            if self.sync_engine:
                self.sync_engine.dispose()
            if self.task_async_engine:
                self.task_async_engine.sync_engine.dispose()
            if self.api_async_engine:
                self.api_async_engine.sync_engine.dispose()
            db_logger.info("[Database] Database connections closed")
        except Exception as e:
            db_logger.error(f"[Database] Error closing database connections: {e}")

    async def close_async(self):
        """关闭数据库连接（异步入口，优先用于应用退出）。"""
        try:
            if self.sync_engine:
                self.sync_engine.dispose()
            if self.task_async_engine:
                await self.task_async_engine.dispose()
            if self.api_async_engine:
                await self.api_async_engine.dispose()
            db_logger.info("[Database] Database connections closed")
        except Exception as e:
            db_logger.error(f"[Database] Error closing database connections: {e}")


# 全局数据库管理器实例
db_manager = DatabaseManager()


@asynccontextmanager
async def get_async_db():
    """依赖注入：获取异步数据库会话（上下文管理器）"""
    async with db_manager.get_async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_db_async():
    """同步数据库会话的异步上下文管理器"""
    session = db_manager.get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Session:
    """依赖注入：获取数据库会话（非上下文管理器）"""
    return db_manager.get_session()
