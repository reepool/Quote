"""
Middleware for the quote system API.
Provides CORS, logging, authentication, and other middleware components.
"""

import time
import json
from typing import Callable
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware

from utils import api_logger, config_manager


class LoggingMiddleware(BaseHTTPMiddleware):
    """日志中间件"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        # 记录请求信息
        api_logger.info(f"[API] {request.method} {request.url}")

        # 获取客户端IP
        client_ip = request.client.host if request.client else "unknown"

        # 获取用户代理
        user_agent = request.headers.get("user-agent", "unknown")

        try:
            response = await call_next(request)

            # 记录响应信息
            process_time = time.time() - start_time
            api_logger.info(f"[API] {request.method} {request.url} - {response.status_code} - {process_time:.3f}s")

            # 添加处理时间到响应头
            response.headers["X-Process-Time"] = str(process_time)
            response.headers["X-Request-ID"] = str(id(request))

            return response

        except Exception as e:
            process_time = time.time() - start_time
            api_logger.error(f"[API] {request.method} {request.url} - ERROR - {process_time:.3f}s - {str(e)}")
            raise


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """错误处理中间件"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            response = await call_next(request)
            return response

        except ValueError as e:
            api_logger.warning(f"[API] Validation error: {str(e)}")
            return JSONResponse(
                status_code=422,
                content={
                    "error": "Validation Error",
                    "error_code": "VALIDATION_ERROR",
                    "timestamp": time.time(),
                    "details": {"message": str(e)}
                }
            )

        except PermissionError as e:
            api_logger.warning(f"[API] Permission error: {str(e)}")
            return JSONResponse(
                status_code=403,
                content={
                    "error": "Permission Denied",
                    "error_code": "PERMISSION_ERROR",
                    "timestamp": time.time(),
                    "details": {"message": str(e)}
                }
            )

        except FileNotFoundError as e:
            api_logger.warning(f"[API] Not found error: {str(e)}")
            return JSONResponse(
                status_code=404,
                content={
                    "error": "Not Found",
                    "error_code": "NOT_FOUND",
                    "timestamp": time.time(),
                    "details": {"message": str(e)}
                }
            )

        except Exception as e:
            api_logger.error(f"[API] Unexpected error: {str(e)}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal Server Error",
                    "error_code": "INTERNAL_ERROR",
                    "timestamp": time.time(),
                    "details": {"message": "An unexpected error occurred"}
                }
            )


class RateLimitMiddleware(BaseHTTPMiddleware):
    """简单的限流中间件"""

    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.request_counts = {}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        client_ip = request.client.host if request.client else "unknown"
        current_time = time.time()

        # 清理过期的记录
        self.request_counts = {
            ip: timestamps
            for ip, timestamps in self.request_counts.items()
            if timestamps and current_time - timestamps[-1] < 60
        }

        # 检查限流
        if client_ip not in self.request_counts:
            self.request_counts[client_ip] = []

        # 移除1分钟前的请求记录
        minute_ago = current_time - 60
        self.request_counts[client_ip] = [
            timestamp for timestamp in self.request_counts[client_ip]
            if timestamp > minute_ago
        ]

        # 检查是否超过限制
        if len(self.request_counts[client_ip]) >= self.requests_per_minute:
            api_logger.warning(f"[API] Rate limit exceeded for IP: {client_ip}")
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate Limit Exceeded",
                    "error_code": "RATE_LIMIT_EXCEEDED",
                    "timestamp": current_time,
                    "details": {
                        "message": f"Too many requests. Limit is {self.requests_per_minute} requests per minute.",
                        "limit": self.requests_per_minute,
                        "window": "60 seconds"
                    }
                }
            )

        # 记录当前请求
        self.request_counts[client_ip].append(current_time)

        response = await call_next(request)
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """安全头中间件"""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        response = await call_next(request)

        # 添加安全相关的响应头
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self'"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        return response


def setup_cors(app):
    """设置CORS"""
    api_config = config_manager.get_nested('api_config', {})
    cors_origins = api_config.get('cors_origins', ["http://localhost:3000", "http://localhost:8080"])

    # 安全检查：确保不是通配符
    if "*" in cors_origins and len(cors_origins) == 1:
        api_logger.warning("[CORS] Using wildcard origin is not recommended for production")
        # 在生产环境中应该使用具体的域名
        if config_manager.get_nested('data_config.env', 'development') == 'production':
            cors_origins = []  # 生产环境禁用通配符

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def setup_middleware(app):
    """设置所有中间件"""
    # 设置CORS
    setup_cors(app)

    # 添加中间件（顺序很重要）
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(RateLimitMiddleware, requests_per_minute=100)
    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(LoggingMiddleware)

    api_logger.info("[API] Middleware setup completed")