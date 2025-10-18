"""
pytest configuration and fixtures for Quote System tests
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date
from unittest.mock import Mock, AsyncMock
import pandas as pd
import sys
import os

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from database.connection import DatabaseManager
from database.models import Base
from utils.config_manager import UnifiedConfigManager
from utils.logging_manager import LoggingManager
from utils.cache import CacheManager
from utils.validation import DataValidator


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_config():
    """Test configuration fixture"""
    return {
        "database": {
            "url": "sqlite:///:memory:",
            "echo": False
        },
        "logging": {
            "level": "WARNING",  # Reduce noise in tests
            "console_enabled": False
        },
        "data_sources": {
            "baostock": {
                "enabled": False,  # Disable external sources in tests
                "rate_limit": {"requests_per_second": 1}
            },
            "yfinance": {
                "enabled": False,
                "rate_limit": {"requests_per_second": 1}
            },
            "akshare": {
                "enabled": False,
                "rate_limit": {"requests_per_second": 1}
            }
        },
        "cache": {
            "enabled": True,
            "ttl": 300
        },
        "api": {
            "host": "127.0.0.1",
            "port": 8001,  # Different port for tests
            "cors_origins": ["http://localhost:3000"]
        }
    }


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
async def test_database(test_config):
    """Create a test database session"""
    connection = DatabaseManager(test_config["database"]["url"])
    await connection.initialize()

    # Create all tables
    async with connection.async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield connection

    # Cleanup
    await connection.close()


@pytest.fixture
def sample_stock_data():
    """Sample stock data for testing"""
    return pd.DataFrame({
        'code': ['000001.SZ', '000002.SZ', '600000.SH'],
        'name': ['平安银行', '万科A', '浦发银行'],
        'industry': ['银行', '房地产', '银行'],
        'market': ['SZSE', 'SZSE', 'SSE'],
        'list_date': ['1991-04-03', '1991-01-29', '1999-11-10']
    })


@pytest.fixture
def sample_quote_data():
    """Sample quote data for testing"""
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    trading_dates = [d for d in dates if d.weekday() < 5]  # Weekdays only

    data = []
    for code in ['000001.SZ', '000002.SZ']:
        for trade_date in trading_dates:
            data.append({
                'code': code,
                'date': trade_date.strftime('%Y-%m-%d'),
                'open': 10.0 + hash(code) % 5,
                'high': 12.0 + hash(code) % 5,
                'low': 9.0 + hash(code) % 5,
                'close': 11.0 + hash(code) % 5,
                'volume': 1000000 + hash(code) % 500000,
                'amount': 10000000 + hash(code) % 5000000
            })

    return pd.DataFrame(data)


@pytest.fixture
def mock_data_source():
    """Mock data source for testing"""
    mock = AsyncMock()

    # Configure mock methods
    mock.get_stock_list.return_value = pd.DataFrame({
        'code': ['000001.SZ', '000002.SZ'],
        'name': ['平安银行', '万科A']
    })

    mock.get_daily_data.return_value = pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02'],
        'open': [10.0, 10.5],
        'high': [11.0, 11.5],
        'low': [9.5, 10.0],
        'close': [10.8, 11.2],
        'volume': [1000000, 1200000]
    })

    mock.is_trading_day.return_value = True
    mock.get_latest_trading_date.return_value = date(2024, 1, 2)

    return mock


@pytest.fixture
def mock_config_manager(test_config):
    """Mock configuration manager"""
    mock = Mock(spec=UnifiedConfigManager)

    def get_side_effect(key, default=None):
        keys = key.split('.')
        value = test_config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    mock.get.side_effect = get_side_effect
    mock.get_all.return_value = test_config

    return mock


@pytest.fixture
def mock_logger():
    """Mock logger for testing"""
    mock = Mock()
    mock.debug = Mock()
    mock.info = Mock()
    mock.warning = Mock()
    mock.error = Mock()
    mock.critical = Mock()
    mock.exception = Mock()
    return mock


@pytest.fixture
def sample_instrument_data():
    """Sample instrument data for testing"""
    return [
        {
            'code': '000001.SZ',
            'name': '平安银行',
            'market': 'SZSE',
            'industry': '银行',
            'list_date': '1991-04-03',
            'status': 'active'
        },
        {
            'code': '000002.SZ',
            'name': '万科A',
            'market': 'SZSE',
            'industry': '房地产',
            'list_date': '1991-01-29',
            'status': 'active'
        }
    ]


@pytest.fixture
def mock_telegram_bot():
    """Mock Telegram bot for testing"""
    mock = Mock()
    mock.send_message = AsyncMock()
    mock.send_alert = AsyncMock()
    mock.is_connected.return_value = True
    return mock


@pytest.fixture
async def cache_manager(test_config):
    """Cache manager fixture"""
    cache = CacheManager(test_config["cache"])
    await cache.initialize()
    yield cache
    await cache.clear()


@pytest.fixture
def data_validator():
    """Data validator fixture"""
    return DataValidator()


@pytest.fixture
def mock_trading_calendar():
    """Mock trading calendar"""
    mock = Mock()
    mock.is_trading_day.return_value = True
    mock.get_previous_trading_day.return_value = date(2024, 1, 1)
    mock.get_next_trading_day.return_value = date(2024, 1, 3)
    mock.get_trading_days.return_value = [
        date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    ]
    return mock


# Pytest configuration
def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
    config.addinivalue_line(
        "markers", "e2e: mark test as an end-to-end test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


@pytest.fixture(autouse=True)
async def cleanup_cache():
    """Cleanup cache after each test"""
    yield
    # Add any cleanup logic here if needed


# Test utilities
@pytest.fixture
def async_test():
    """Decorator to mark async tests"""
    def decorator(func):
        func.__pytest_asyncio__ = True
        return func
    return decorator


# Mock data factories
@pytest.fixture
def create_test_instrument():
    """Factory to create test instruments"""
    def _create_instrument(code="000001.SZ", name="测试股票", market="SZSE"):
        return {
            'code': code,
            'name': name,
            'market': market,
            'industry': '测试行业',
            'list_date': '2020-01-01',
            'status': 'active'
        }
    return _create_instrument


@pytest.fixture
def create_test_quote():
    """Factory to create test quotes"""
    def _create_quote(code="000001.SZ", trade_date="2024-01-01"):
        return {
            'code': code,
            'date': trade_date,
            'open': 10.0,
            'high': 11.0,
            'low': 9.5,
            'close': 10.8,
            'volume': 1000000,
            'amount': 10800000
        }
    return _create_quote