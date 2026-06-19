"""
Unit tests for API middleware helpers.
"""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.responses import JSONResponse

from api.middleware import LoggingMiddleware, RateLimitMiddleware, normalize_repeated_slashes
from database.connection import get_current_db_workload


@pytest.mark.unit
def test_normalize_repeated_slashes_collapses_path_segments():
    assert normalize_repeated_slashes("//api/v1/research/industry/component-sets") == (
        "/api/v1/research/industry/component-sets"
    )
    assert normalize_repeated_slashes("/api//v1//health") == "/api/v1/health"
    assert normalize_repeated_slashes("/api/v1/health") == "/api/v1/health"


@pytest.mark.unit
def test_rate_limit_middleware_prefers_specific_path_prefix():
    middleware = RateLimitMiddleware(
        app=lambda scope, receive, send: None,
        requests_per_minute=100,
        path_limits={
            "/api/v1/quotes": 60,
            "/api/v1/quotes/daily": 30,
        },
        concurrency_limits={
            "/api/v1/quotes/daily": 2,
        },
    )

    assert middleware._matched_path_key(
        "/api/v1/quotes/daily", middleware.path_limits
    ) == "/api/v1/quotes/daily"
    assert middleware._rate_limit_for_path("/api/v1/quotes/daily")[1] == 30
    assert middleware._rate_limit_for_path("/api/v1/quotes/monthly")[1] == 60
    assert middleware._rate_limit_for_path("/api/v1/health")[1] == 100


@pytest.mark.unit
def test_rate_limit_middleware_root_protected_path_matches_all_api_paths():
    middleware = RateLimitMiddleware(
        app=lambda scope, receive, send: None,
        requests_per_minute=100,
        protected_paths={
            "/": {
                "active_limit": 4,
                "queue_limit": 80,
                "queue_timeout_seconds": 120,
                "busy_status_code": 503,
                "retry_after_seconds": 30,
            }
        },
    )

    assert middleware._matched_path_key(
        "/api/v1/health", middleware.protected_paths
    ) == "/"
    assert middleware._matched_path_key(
        "/api/v1/quotes/daily", middleware.protected_paths
    ) == "/"


def _request(path: str = "/api/v1/quotes/daily"):
    return SimpleNamespace(
        client=SimpleNamespace(host="127.0.0.1"),
        url=SimpleNamespace(path=path),
    )


def _logging_request(path: str = "/api/v1/quotes/daily"):
    return SimpleNamespace(
        method="GET",
        client=SimpleNamespace(host="127.0.0.1"),
        headers={"user-agent": "pytest-client"},
        url=f"http://testserver{path}?symbol=000001",
    )


def _protected_middleware(queue_timeout_seconds: float = 0.2) -> RateLimitMiddleware:
    return RateLimitMiddleware(
        app=lambda scope, receive, send: None,
        requests_per_minute=100,
        protected_paths={
            "/api/v1/quotes/daily": {
                "active_limit": 1,
                "queue_limit": 1,
                "queue_timeout_seconds": queue_timeout_seconds,
                "busy_status_code": 503,
                "retry_after_seconds": 5,
            }
        },
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_protected_path_waits_for_slot_before_execution():
    middleware = _protected_middleware(queue_timeout_seconds=0.5)
    path_key = "/api/v1/quotes/daily"
    await middleware.semaphores[path_key].acquire()
    middleware.active_counts[path_key] = 1
    call_count = 0

    async def call_next(request):
        nonlocal call_count
        call_count += 1
        return JSONResponse({"ok": True})

    dispatch_task = asyncio.create_task(middleware.dispatch(_request(), call_next))
    await asyncio.sleep(0.05)
    middleware.active_counts[path_key] -= 1
    middleware.semaphores[path_key].release()

    response = await dispatch_task

    assert response.status_code == 200
    assert call_count == 1
    assert middleware.active_counts[path_key] == 0
    assert middleware.queue_counts[path_key] == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_protected_path_marks_backend_execution_as_api_workload():
    middleware = _protected_middleware(queue_timeout_seconds=0.5)

    async def call_next(request):
        assert get_current_db_workload() == "api"
        return JSONResponse({"ok": True})

    assert get_current_db_workload() == "task"
    response = await middleware.dispatch(_request(), call_next)

    assert response.status_code == 200
    assert get_current_db_workload() == "task"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_protected_path_times_out_when_queue_wait_expires():
    middleware = _protected_middleware(queue_timeout_seconds=0.01)
    path_key = "/api/v1/quotes/daily"
    await middleware.semaphores[path_key].acquire()
    middleware.active_counts[path_key] = 1

    async def call_next(request):
        raise AssertionError("request should not enter backend execution")

    try:
        response = await middleware.dispatch(_request(), call_next)
    finally:
        middleware.active_counts[path_key] -= 1
        middleware.semaphores[path_key].release()

    payload = json.loads(response.body)
    assert response.status_code == 503
    assert payload["error_code"] == "ADMISSION_QUEUE_TIMEOUT"
    assert middleware.queue_counts[path_key] == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_protected_path_rejects_when_queue_is_full():
    middleware = _protected_middleware(queue_timeout_seconds=0.5)
    path_key = "/api/v1/quotes/daily"
    await middleware.semaphores[path_key].acquire()
    middleware.active_counts[path_key] = 1
    middleware.queue_counts[path_key] = 1

    async def call_next(request):
        raise AssertionError("request should not enter backend execution")

    try:
        response = await middleware.dispatch(_request(), call_next)
    finally:
        middleware.active_counts[path_key] -= 1
        middleware.queue_counts[path_key] = 0
        middleware.semaphores[path_key].release()

    payload = json.loads(response.body)
    assert response.status_code == 503
    assert payload["error_code"] == "ADMISSION_QUEUE_FULL"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_logging_middleware_uses_access_logger(monkeypatch):
    access_logger = Mock()
    monkeypatch.setattr("api.middleware.access_logger", access_logger)
    middleware = LoggingMiddleware(app=lambda scope, receive, send: None)

    async def call_next(request):
        return JSONResponse({"ok": True}, status_code=200)

    response = await middleware.dispatch(_logging_request(), call_next)

    assert response.status_code == 200
    assert "X-Request-ID" in response.headers
    assert access_logger.info.call_count == 2
    assert access_logger.error.call_count == 0
    complete_args = access_logger.info.call_args_list[-1].args
    assert "request_complete" in complete_args[0]
    assert "GET" in complete_args
    assert 200 in complete_args


@pytest.mark.unit
@pytest.mark.asyncio
async def test_logging_middleware_logs_access_errors(monkeypatch):
    access_logger = Mock()
    monkeypatch.setattr("api.middleware.access_logger", access_logger)
    middleware = LoggingMiddleware(app=lambda scope, receive, send: None)

    async def call_next(request):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await middleware.dispatch(_logging_request(), call_next)

    assert access_logger.info.call_count == 1
    assert access_logger.error.call_count == 1
    error_args = access_logger.error.call_args.args
    assert "request_error" in error_args[0]
    assert "GET" in error_args
