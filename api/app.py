"""
FastAPI application for the quote system.
Main application entry point for the API server.
"""

import os
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware

from utils import api_logger, config_manager
from utils.llm import load_project_environment

from .routes import router
from .announcement_asset_routes import router as announcement_asset_router
from .middleware import setup_middleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    api_logger.info("[API] Starting Quote System API...")
    load_project_environment()

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
    version="1.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


def _is_annual_report_asset_request(request: Request) -> bool:
    path = str(request.url.path)
    return "annual-report" in path or path.endswith("/business-profile")


@app.exception_handler(HTTPException)
async def annual_report_http_exception_handler(
    request: Request,
    exc: HTTPException,
):
    if not _is_annual_report_asset_request(request):
        return await http_exception_handler(request, exc)
    detail = exc.detail
    if isinstance(detail, dict) and detail.get("schema_version") == (
        "annual_report_error.v1"
    ):
        payload = {
            "schema_version": "annual_report_error.v1",
            "error_code": str(detail.get("error_code") or "request_failed"),
            "message": str(detail.get("message") or "request failed"),
            "retryable": bool(detail.get("retryable", False)),
            "details": dict(detail.get("details") or {}),
        }
    else:
        payload = {
            "schema_version": "annual_report_error.v1",
            "error_code": "request_failed",
            "message": str(detail),
            "retryable": False,
            "details": {},
        }
    return JSONResponse(
        status_code=exc.status_code,
        content=payload,
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def annual_report_validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
):
    if not _is_annual_report_asset_request(request):
        return await request_validation_exception_handler(request, exc)
    errors = [
        {
            "location": [str(item) for item in error.get("loc", ())],
            "message": str(error.get("msg") or "invalid value"),
            "type": str(error.get("type") or "validation_error"),
        }
        for error in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={
            "schema_version": "annual_report_error.v1",
            "error_code": "invalid_request",
            "message": "request validation failed",
            "retryable": False,
            "details": {"errors": errors},
        },
    )

# 设置中间件
setup_middleware(app)

# 添加路由
app.include_router(router, prefix="/api/v1")
app.include_router(announcement_asset_router, prefix="/api/v1")


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
            log_level="info",
            access_log=False
        )
    # 生产模式
    else:
        uvicorn.run(
            app,
            host=host,
            port=port,
            workers=workers,
            log_level="info",
            access_log=False
        )
