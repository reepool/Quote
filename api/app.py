"""
FastAPI application for the quote system.
Main application entry point for the API server.
"""

import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware

from utils import api_logger, config_manager

from .routes import router
from .middleware import setup_middleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    api_logger.info("[API] Starting Quote System API...")

    # 启动时初始化
    try:
        from data_manager import data_manager
        await data_manager.initialize()
        api_logger.info("[API] DataManager initialized successfully")
    except Exception as e:
        api_logger.error(f"[API] Failed to initialize DataManager: {e}")
        # 不阻止应用启动，但记录错误

    yield

    # 关闭时清理
    api_logger.info("[API] Shutting down Quote System API...")
    try:
        from data_manager import data_manager
        if hasattr(data_manager, 'close'):
            await data_manager.close()
    except Exception as e:
        api_logger.error(f"[API] Error during shutdown: {e}")


# 创建FastAPI应用
app = FastAPI(
    title="Quote System API",
    description="A comprehensive stock quote data management system API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# 设置中间件
setup_middleware(app)

# 添加路由
app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "Quote System API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """健康检查端点"""
    from datetime import datetime
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }


if __name__ == "__main__":
    # 获取API配置
    api_config = config_manager.get_nested('api_config', {})
    host = api_config.get('host', '0.0.0.0')
    port = api_config.get('port', 8000)
    workers = api_config.get('workers', 1)
    reload = api_config.get('reload', False)

    api_logger.info(f"[API] Starting server on {host}:{port}")

    # 开发模式
    if reload:
        uvicorn.run(
            "api.app:app",
            host=host,
            port=port,
            reload=True,
            log_level="info"
        )
    # 生产模式
    else:
        uvicorn.run(
            app,
            host=host,
            port=port,
            workers=workers,
            log_level="info"
        )