"""
Database connection management.
Provides SQLite database connection with async support and connection pooling.
"""

import os
from utils import db_logger, config_manager

import aiosqlite
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import StaticPool, QueuePool


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
                default_pool_size=2,
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

            db_logger.info("[Database] Database connection initialized successfully")

        except Exception as e:
            db_logger.error(f"[Database] Failed to initialize database: {e}")
            raise

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
            poolclass=QueuePool,
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
