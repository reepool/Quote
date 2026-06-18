"""
Middleware for the quote system API.
Provides CORS, logging, authentication, and other middleware components.
"""

import time
import json
import re
import asyncio
from collections import defaultdict
from typing import Callable
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware

from database.connection import db_workload_context
from utils import api_logger, config_manager


def normalize_repeated_slashes(path: str) -> str:
    """Collapse repeated slashes while preserving a single leading slash."""
    if "//" not in path:
        return path
    return re.sub(r"/{2,}", "/", path)


class PathNormalizationMiddleware(BaseHTTPMiddleware):
    """Normalize repeated slashes in request paths before routing."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.scope.get("path", "")
        if "//" in path:
            normalized_path = normalize_repeated_slashes(path)
            request.scope["path"] = normalized_path
            request.scope["raw_path"] = normalized_path.encode("ascii")
        return await call_next(request)


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
    """Rate-limit and shed expensive API requests before they occupy DB workers."""

    def __init__(
        self,
        app,
        requests_per_minute: int = 60,
        path_limits: dict | None = None,
        concurrency_limits: dict | None = None,
        protected_paths: dict | None = None,
    ):
        super().__init__(app)
        self.requests_per_minute = requests_per_minute
        self.path_limits = {
            str(path): int(limit)
            for path, limit in (path_limits or {}).items()
            if path and int(limit) > 0
        }
        self.concurrency_limits = {
            str(path): int(limit)
            for path, limit in (concurrency_limits or {}).items()
            if path and int(limit) > 0
        }
        self.protected_paths = self._normalize_protected_paths(
            protected_paths,
            self.concurrency_limits,
        )
        self.request_counts = {}
        self.active_counts = defaultdict(int)
        self.queue_counts = defaultdict(int)
        self.semaphores = {
            path: asyncio.Semaphore(config["active_limit"])
            for path, config in self.protected_paths.items()
        }

    @staticmethod
    def _normalize_protected_paths(
        protected_paths: dict | None,
        legacy_concurrency_limits: dict,
    ) -> dict:
        normalized = {}
        for path, config in (protected_paths or {}).items():
            if not path or not isinstance(config, dict):
                continue
            active_limit = int(config.get("active_limit", 0) or 0)
            if active_limit <= 0:
                continue
            normalized[str(path)] = {
                "active_limit": active_limit,
                "queue_limit": max(int(config.get("queue_limit", 0) or 0), 0),
                "queue_timeout_seconds": max(
                    float(config.get("queue_timeout_seconds", 0) or 0),
                    0.0,
                ),
                "busy_status_code": int(config.get("busy_status_code", 503) or 503),
                "retry_after_seconds": int(config.get("retry_after_seconds", 5) or 5),
            }

        # Backward-compatible path for older concurrency-only configuration.
        for path, active_limit in legacy_concurrency_limits.items():
            normalized.setdefault(
                str(path),
                {
                    "active_limit": int(active_limit),
                    "queue_limit": 0,
                    "queue_timeout_seconds": 0.0,
                    "busy_status_code": 503,
                    "retry_after_seconds": 5,
                },
            )
        return normalized

    def _matched_path_key(self, path: str, limits: dict) -> str | None:
        """Return the most specific configured prefix matching the request path."""
        matches = [prefix for prefix in limits if path.startswith(prefix)]
        if not matches:
            return None
        return max(matches, key=len)

    def _rate_limit_for_path(self, path: str) -> tuple[str, int]:
        path_key = self._matched_path_key(path, self.path_limits)
        if path_key is None:
            return "*", self.requests_per_minute
        return path_key, self.path_limits[path_key]

    def _busy_response(
        self,
        *,
        request_path: str,
        reason: str,
        config: dict,
        current_time: float,
    ) -> JSONResponse:
        retry_after = str(config.get("retry_after_seconds", 5))
        return JSONResponse(
            status_code=int(config.get("busy_status_code", 503) or 503),
            content={
                "error": "Service Busy",
                "error_code": reason,
                "timestamp": current_time,
                "details": {
                    "message": "This endpoint is temporarily busy. Please wait or retry later.",
                    "path": request_path,
                    "active_limit": config.get("active_limit"),
                    "queue_limit": config.get("queue_limit"),
                    "queue_timeout_seconds": config.get("queue_timeout_seconds"),
                },
            },
            headers={"Retry-After": retry_after},
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        client_ip = request.client.host if request.client else "unknown"
        request_path = normalize_repeated_slashes(request.url.path)
        current_time = time.time()
        rate_key, requests_per_minute = self._rate_limit_for_path(request_path)
        request_count_key = (client_ip, rate_key)

        # 清理过期的记录
        self.request_counts = {
            key: timestamps
            for key, timestamps in self.request_counts.items()
            if timestamps and current_time - timestamps[-1] < 60
        }

        # 检查限流
        if request_count_key not in self.request_counts:
            self.request_counts[request_count_key] = []

        # 移除1分钟前的请求记录
        minute_ago = current_time - 60
        self.request_counts[request_count_key] = [
            timestamp for timestamp in self.request_counts[request_count_key]
            if timestamp > minute_ago
        ]

        # 检查是否超过限制
        if len(self.request_counts[request_count_key]) >= requests_per_minute:
            api_logger.warning(
                "[API] Rate limit exceeded for IP: %s path=%s limit=%s",
                client_ip,
                request_path,
                requests_per_minute,
            )
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate Limit Exceeded",
                    "error_code": "RATE_LIMIT_EXCEEDED",
                    "timestamp": current_time,
                    "details": {
                        "message": f"Too many requests. Limit is {requests_per_minute} requests per minute.",
                        "limit": requests_per_minute,
                        "window": "60 seconds"
                    }
                }
            )

        # 记录当前请求
        self.request_counts[request_count_key].append(current_time)

        concurrency_key = self._matched_path_key(request_path, self.protected_paths)
        protected_config = (
            self.protected_paths[concurrency_key]
            if concurrency_key is not None
            else None
        )
        acquired_slot = False
        if concurrency_key is not None and protected_config is not None:
            semaphore = self.semaphores[concurrency_key]
            wait_started_at = None
            if semaphore.locked():
                queue_limit = protected_config["queue_limit"]
                if queue_limit <= 0 or self.queue_counts[concurrency_key] >= queue_limit:
                    api_logger.warning(
                        "[API] Admission queue full path=%s active=%s active_limit=%s "
                        "queue=%s queue_limit=%s client_ip=%s",
                        request_path,
                        self.active_counts[concurrency_key],
                        protected_config["active_limit"],
                        self.queue_counts[concurrency_key],
                        queue_limit,
                        client_ip,
                    )
                    return self._busy_response(
                        request_path=request_path,
                        reason="ADMISSION_QUEUE_FULL",
                        config=protected_config,
                        current_time=current_time,
                    )

                self.queue_counts[concurrency_key] += 1
                wait_started_at = time.monotonic()
                try:
                    await asyncio.wait_for(
                        semaphore.acquire(),
                        timeout=protected_config["queue_timeout_seconds"],
                    )
                    acquired_slot = True
                    waited_seconds = time.monotonic() - wait_started_at
                    api_logger.info(
                        "[API] Admission queue released path=%s wait_seconds=%.3f "
                        "active_limit=%s queue_limit=%s client_ip=%s",
                        request_path,
                        waited_seconds,
                        protected_config["active_limit"],
                        queue_limit,
                        client_ip,
                    )
                except asyncio.TimeoutError:
                    waited_seconds = time.monotonic() - wait_started_at
                    api_logger.warning(
                        "[API] Admission queue timeout path=%s wait_seconds=%.3f "
                        "active=%s active_limit=%s queue=%s queue_limit=%s client_ip=%s",
                        request_path,
                        waited_seconds,
                        self.active_counts[concurrency_key],
                        protected_config["active_limit"],
                        self.queue_counts[concurrency_key],
                        queue_limit,
                        client_ip,
                    )
                    return self._busy_response(
                        request_path=request_path,
                        reason="ADMISSION_QUEUE_TIMEOUT",
                        config=protected_config,
                        current_time=current_time,
                    )
                finally:
                    self.queue_counts[concurrency_key] = max(
                        self.queue_counts[concurrency_key] - 1,
                        0,
                    )
            else:
                await semaphore.acquire()
                acquired_slot = True

            if acquired_slot:
                self.active_counts[concurrency_key] += 1

            if not acquired_slot:
                api_logger.warning(
                    "[API] Admission failed path=%s active=%s active_limit=%s client_ip=%s",
                    request_path,
                    self.active_counts[concurrency_key],
                    protected_config["active_limit"],
                    client_ip,
                )
                return self._busy_response(
                    request_path=request_path,
                    reason="ADMISSION_UNAVAILABLE",
                    config=protected_config,
                    current_time=current_time,
                )

        try:
            async with db_workload_context("api"):
                response = await call_next(request)
            return response
        finally:
            if (
                concurrency_key is not None
                and protected_config is not None
                and acquired_slot
            ):
                self.active_counts[concurrency_key] -= 1
                self.semaphores[concurrency_key].release()


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
    api_config = config_manager.get_nested('api_config', {})
    rate_limit_config = api_config.get('rate_limit', {}) if isinstance(api_config, dict) else {}
    resource_config = (
        api_config.get('resource_protection', {}) if isinstance(api_config, dict) else {}
    )
    requests_per_minute = int(rate_limit_config.get('requests_per_minute', 100))
    path_limits = rate_limit_config.get('path_limits', {}) or {}
    concurrency_limits = rate_limit_config.get('concurrency_limits', {}) or {}
    protected_paths = (
        resource_config.get('protected_paths', {}) or {}
        if resource_config.get('enabled', True)
        else {}
    )

    # 设置CORS
    setup_cors(app)

    # 添加中间件（顺序很重要）
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(
        RateLimitMiddleware,
        requests_per_minute=requests_per_minute,
        path_limits=path_limits,
        concurrency_limits=concurrency_limits,
        protected_paths=protected_paths,
    )
    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(PathNormalizationMiddleware)

    api_logger.info("[API] Middleware setup completed")
