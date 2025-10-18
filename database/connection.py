"""
Database connection management.
Provides SQLite database connection with async support and connection pooling.
"""

import os
from utils import db_logger, config_manager

import aiosqlite
from typing import AsyncGenerator
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import StaticPool


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path: str = config_manager.get_nested('database_config.db_path')):
        self.db_path = db_path 
        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        db_logger.info(f"[Database] Using database path: {db_path}")
        self.sync_engine = None
        self.async_engine = None
        self.SessionLocal = None
        self.AsyncSessionLocal = None

    def initialize(self):
        """初始化数据库连接"""
        try:
            # 同步连接引擎
            self.sync_engine = create_engine(
                f"sqlite:///{self.db_path}",
                poolclass=StaticPool,
                connect_args={"check_same_thread": False}
            )

            # 异步连接引擎
            self.async_engine = create_async_engine(
                f"sqlite+aiosqlite:///{self.db_path}",
                poolclass=StaticPool,
                connect_args={"check_same_thread": False}
            )

            # 创建会话工厂
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.sync_engine
            )

            self.AsyncSessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.async_engine,
                class_=AsyncSession
            )

            db_logger.info("[Database] Database connection initialized successfully")

        except Exception as e:
            db_logger.error(f"[Database] Failed to initialize database: {e}")
            raise

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
            "CREATE INDEX IF NOT EXISTS idx_daily_quotes_instrument_id ON daily_quotes(instrument_id)",
            "CREATE INDEX IF NOT EXISTS idx_daily_quotes_time ON daily_quotes(time)",
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

    def get_async_session(self) -> AsyncSession:
        """获取异步数据库会话"""
        if not self.AsyncSessionLocal:
            raise RuntimeError("Database not initialized")
        return self.AsyncSessionLocal()

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
            if self.async_engine:
                self.async_engine.dispose()
            db_logger.info("[Database] Database connections closed")
        except Exception as e:
            db_logger.error(f"[Database] Error closing database connections: {e}")


# 全局数据库管理器实例
db_manager = DatabaseManager()


@asynccontextmanager
async def get_async_db():
    """依赖注入：获取异步数据库会话（上下文管理器）"""
    if not db_manager.AsyncSessionLocal:
        raise RuntimeError("Database not initialized")

    async with db_manager.AsyncSessionLocal() as session:
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