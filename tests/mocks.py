"""
Mock objects and utilities for Quote System tests
Provides comprehensive mocking capabilities for external dependencies
"""

import asyncio
from datetime import datetime, date, timedelta
from unittest.mock import Mock, AsyncMock, MagicMock
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import aioresponses
import pytest
from aioresponses import aioresponses as aioresponses_decorator

from tests.factories import (
    InstrumentFactory, QuoteFactory, DataSourceFactory,
    TradingCalendarFactory, ConfigFactory, APIDataFactory
)


class MockDataSource:
    """Comprehensive mock data source for testing"""

    def __init__(self, name: str = "mock_source"):
        self.name = name
        self.enabled = True
        self.rate_limiter = Mock()
        self.rate_limiter.acquire = AsyncMock(return_value=True)

        # Configure default mock responses
        self._setup_default_responses()

    def _setup_default_responses(self):
        """Setup default mock responses"""
        self.get_stock_list = Mock(return_value=DataSourceFactory.create_stock_list_response())
        self.get_daily_data = Mock(return_value=DataSourceFactory.create_daily_data_response())
        self.is_trading_day = Mock(return_value=True)
        self.get_latest_trading_date = Mock(return_value=date.today())
        self.get_previous_trading_date = Mock(return_value=date.today() - timedelta(days=1))
        self.get_next_trading_date = Mock(return_value=date.today() + timedelta(days=1))
        self.get_trading_days = Mock(return_value=TradingCalendarFactory.create_trading_days())
        self.get_instrument_info = Mock(return_value=InstrumentFactory.create_instrument())
        self.validate_data = Mock(return_value=True)
        self.connect = AsyncMock(return_value=True)
        self.disconnect = AsyncMock(return_value=True)
        self.health_check = AsyncMock(return_value={"status": "healthy", "source": self.name})

    def configure_failure(self, method_name: str, exception: Exception):
        """Configure a method to raise an exception"""
        if hasattr(self, method_name):
            getattr(self, method_name).side_effect = exception

    def configure_empty_response(self, method_name: str):
        """Configure a method to return empty response"""
        if hasattr(self, method_name):
            if method_name in ['get_stock_list', 'get_daily_data']:
                getattr(self, method_name).return_value = pd.DataFrame()
            elif method_name == 'get_trading_days':
                getattr(self, method_name).return_value = []
            else:
                getattr(self, method_name).return_value = None

    def configure_rate_limit(self, delay: float = 1.0):
        """Configure rate limiting behavior"""
        self.rate_limiter.acquire = AsyncMock(side_effect=asyncio.sleep(delay))


class MockDatabaseManager:
    """Mock database manager for testing"""

    def __init__(self):
        self.async_engine = Mock()
        self.async_session = Mock()
        self.initialize = AsyncMock(return_value=True)
        self.close = AsyncMock(return_value=True)
        self.execute = AsyncMock()
        self.fetch_one = AsyncMock()
        self.fetch_all = AsyncMock()
        self.execute_many = AsyncMock()

        # Mock transaction context
        self.transaction = Mock()
        self.transaction.begin = AsyncMock()
        self.transaction.commit = AsyncMock()
        self.transaction.rollback = AsyncMock()

    def configure_query_result(self, result_data: List[Dict], query_type: str = "all"):
        """Configure query to return specific data"""
        if query_type == "one":
            self.fetch_one.return_value = result_data[0] if result_data else None
        elif query_type == "all":
            self.fetch_all.return_value = result_data
        elif query_type == "execute":
            self.execute.return_value = Mock(rowcount=len(result_data))

    def configure_error(self, operation: str, exception: Exception):
        """Configure database operation to raise an exception"""
        if hasattr(self, operation):
            getattr(self, operation).side_effect = exception


class MockAPIClient:
    """Mock API client for external API testing"""

    def __init__(self, base_url: str = "http://test-api.com"):
        self.base_url = base_url
        self.session = Mock()
        self.session.get = AsyncMock()
        self.session.post = AsyncMock()
        self.session.put = AsyncMock()
        self.session.delete = AsyncMock()

        # Default successful response
        self._setup_default_responses()

    def _setup_default_responses(self):
        """Setup default successful responses"""
        success_response = APIDataFactory.create_api_response()

        self.session.get.return_value = Mock(
            status_code=200,
            json=AsyncMock(return_value=success_response),
            text=AsyncMock(return_value=str(success_response))
        )

        self.session.post.return_value = Mock(
            status_code=201,
            json=AsyncMock(return_value=success_response),
            text=AsyncMock(return_value=str(success_response))
        )

    def configure_response(self, method: str, url: str, response: Dict[str, Any], status_code: int = 200):
        """Configure specific endpoint response"""
        mock_response = Mock(
            status_code=status_code,
            json=AsyncMock(return_value=response),
            text=AsyncMock(return_value=str(response))
        )

        if hasattr(self.session, method):
            getattr(self.session, method).return_value = mock_response

    def configure_error(self, method: str, url: str, status_code: int = 400, error_message: str = "Error"):
        """Configure endpoint to return error"""
        error_response = APIDataFactory.create_error_response(
            status_code=status_code,
            message=error_message
        )
        self.configure_response(method, url, error_response, status_code)


class MockTelegramBot:
    """Mock Telegram bot for testing task management"""

    def __init__(self):
        self.is_connected = Mock(return_value=True)
        self.send_message = AsyncMock(return_value={"message_id": 123})
        self.send_alert = AsyncMock(return_value={"message_id": 124})
        self.edit_message = AsyncMock(return_value={"message_id": 125})
        self.delete_message = AsyncMock(return_value=True)
        self.get_updates = AsyncMock(return_value=[])

        # Mock bot info
        self.bot_info = {
            "id": 123456789,
            "first_name": "Test Bot",
            "username": "test_quote_bot"
        }

    def configure_connected(self, connected: bool):
        """Configure connection status"""
        self.is_connected.return_value = connected

    def configure_failure(self, method: str, exception: Exception):
        """Configure method to raise exception"""
        if hasattr(self, method):
            getattr(self, method).side_effect = exception

    def get_message_history(self):
        """Get history of sent messages"""
        return {
            "messages_sent": self.send_message.call_args_list,
            "alerts_sent": self.send_alert.call_args_list,
            "messages_edited": self.edit_message.call_args_list
        }


class MockCacheManager:
    """Mock cache manager for testing"""

    def __init__(self):
        self.cache = {}
        self.initialize = AsyncMock(return_value=True)
        self.clear = AsyncMock(return_value=True)
        self.get = AsyncMock(side_effect=self._get)
        self.set = AsyncMock(side_effect=self._set)
        self.delete = AsyncMock(side_effect=self._delete)
        self.exists = AsyncMock(side_effect=self._exists)
        self.get_stats = Mock(return_value=self._get_stats())

    def _get(self, key: str, default=None):
        """Internal get method"""
        return self.cache.get(key, default)

    def _set(self, key: str, value: Any, ttl: int = None):
        """Internal set method"""
        self.cache[key] = value
        return True

    def _delete(self, key: str):
        """Internal delete method"""
        return self.cache.pop(key, None) is not None

    def _exists(self, key: str):
        """Internal exists method"""
        return key in self.cache

    def _get_stats(self):
        """Internal stats method"""
        return {
            "keys_count": len(self.cache),
            "memory_usage": len(str(self.cache)),
            "hits": 0,
            "misses": 0
        }

    def configure_ttl(self, key: str, ttl_seconds: int):
        """Configure TTL for specific key"""
        # In real implementation, this would set expiration
        pass

    def configure_get_delay(self, delay_seconds: float):
        """Configure delay for get operations"""
        original_get = self._get
        async def delayed_get(key, default=None):
            await asyncio.sleep(delay_seconds)
            return original_get(key, default)
        self.get = AsyncMock(side_effect=delayed_get)


class MockScheduler:
    """Mock scheduler for testing task management"""

    def __init__(self):
        self.jobs = {}
        self.running = False
        self.initialize = AsyncMock(return_value=True)
        self.start = AsyncMock(return_value=True)
        self.stop = AsyncMock(return_value=True)
        self.add_job = Mock(side_effect=self._add_job)
        self.remove_job = Mock(side_effect=self._remove_job)
        self.get_job = Mock(side_effect=self._get_job)
        self.get_jobs = Mock(return_value=list(self.jobs.values()))
        self.pause_job = Mock(side_effect=self._pause_job)
        self.resume_job = Mock(side_effect=self._resume_job)

    def _add_job(self, job_id: str, func, **kwargs):
        """Internal add job method"""
        job = Mock(
            id=job_id,
            func=func,
            kwargs=kwargs,
            next_run_time=datetime.now() + timedelta(minutes=1),
            paused=False
        )
        self.jobs[job_id] = job
        return job

    def _remove_job(self, job_id: str):
        """Internal remove job method"""
        return self.jobs.pop(job_id, None) is not None

    def _get_job(self, job_id: str):
        """Internal get job method"""
        return self.jobs.get(job_id)

    def _pause_job(self, job_id: str):
        """Internal pause job method"""
        if job_id in self.jobs:
            self.jobs[job_id].paused = True
            return True
        return False

    def _resume_job(self, job_id: str):
        """Internal resume job method"""
        if job_id in self.jobs:
            self.jobs[job_id].paused = False
            return True
        return False

    def configure_job_failure(self, job_id: str, exception: Exception):
        """Configure job to fail when executed"""
        if job_id in self.jobs:
            self.jobs[job_id].func.side_effect = exception

    def get_job_status(self, job_id: str):
        """Get job status"""
        job = self.jobs.get(job_id)
        if job:
            return {
                "id": job.id,
                "paused": job.paused,
                "next_run_time": job.next_run_time,
                "running": self.running
            }
        return None


class MockRateLimiter:
    """Mock rate limiter for testing API rate limiting"""

    def __init__(self, max_requests: int = 60, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
        self.acquire = AsyncMock(return_value=True)

    def configure_limit_reached(self, limit_reached: bool):
        """Configure rate limiter to simulate limit reached"""
        if limit_reached:
            self.acquire.return_value = False
            # Simulate delay when limit is reached
            self.acquire.side_effect = asyncio.sleep(1.0)
        else:
            self.acquire.return_value = True
            self.acquire.side_effect = None

    def configure_delay(self, delay_seconds: float):
        """Configure delay for acquire method"""
        self.acquire.side_effect = asyncio.sleep(delay_seconds)

    def get_request_count(self):
        """Get current request count"""
        return len(self.requests)


# Utility functions for mock creation
def create_mock_data_source(name: str = "test_source", **kwargs) -> MockDataSource:
    """Create a configured mock data source"""
    mock = MockDataSource(name)
    for key, value in kwargs.items():
        if hasattr(mock, key):
            setattr(mock, key, value)
    return mock


def create_mock_database(**kwargs) -> MockDatabaseManager:
    """Create a configured mock database manager"""
    mock = MockDatabaseManager()
    for key, value in kwargs.items():
        if hasattr(mock, key):
            setattr(mock, key, value)
    return mock


def create_mock_api_client(base_url: str = "http://test-api.com", **kwargs) -> MockAPIClient:
    """Create a configured mock API client"""
    mock = MockAPIClient(base_url)
    for key, value in kwargs.items():
        if hasattr(mock, key):
            setattr(mock, key, value)
    return mock


def create_mock_telegram_bot(**kwargs) -> MockTelegramBot:
    """Create a configured mock Telegram bot"""
    mock = MockTelegramBot()
    for key, value in kwargs.items():
        if hasattr(mock, key):
            setattr(mock, key, value)
    return mock


# Context manager for aioresponses
class MockHTTPContext:
    """Context manager for mocking HTTP requests"""

    def __init__(self):
        self.responses = aioresponses_decorator()

    def __enter__(self):
        self.responses.start()
        return self.responses

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.responses.stop()

    def configure_get(self, url: str, response: Dict[str, Any], status: int = 200):
        """Configure GET response"""
        self.responses.get(url, payload=response, status=status)

    def configure_post(self, url: str, response: Dict[str, Any], status: int = 201):
        """Configure POST response"""
        self.responses.post(url, payload=response, status=status)

    def configure_error(self, url: str, status: int = 400, response: Dict[str, Any] = None):
        """Configure error response"""
        if response is None:
            response = {"error": "Bad Request"}
        self.responses.get(url, payload=response, status=status)