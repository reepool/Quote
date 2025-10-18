"""
Basic import tests to verify module structure
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def test_basic_imports():
    """Test basic module imports"""
    # Test utils modules
    from utils.config_manager import UnifiedConfigManager
    from utils.logging_manager import LoggingManager
    from utils.validation import DataValidator

    # Test database modules
    from database.connection import DatabaseManager
    from database.models import Base, Instrument, DailyQuote
    from database.operations import DatabaseOperations

    # Test data source modules
    from data_sources.baostock_source import BaostockSource
    from data_sources.yfinance_source import YFinanceSource
    from data_sources.source_factory import data_source_factory

    # Test API modules
    from api.app import app

    # Test scheduler modules
    from scheduler.scheduler import TaskScheduler

    # Test main module
    from main import QuoteSystem

    assert True  # All imports succeeded


def test_class_instantiation():
    """Test basic class instantiation"""
    from utils.logging_manager import LoggingManager
    from utils.validation import DataValidator

    # Test validator instantiation
    validator = DataValidator()
    assert validator is not None


def test_database_models():
    """Test database model definitions"""
    from database.models import Base, InstrumentDB, DailyQuoteDB

    # Check model attributes
    assert hasattr(InstrumentDB, '__tablename__')
    assert hasattr(DailyQuoteDB, '__tablename__')

    # Check table names
    assert InstrumentDB.__tablename__ == 'instruments'
    assert DailyQuoteDB.__tablename__ == 'daily_quotes'


def test_api_app():
    """Test API app creation"""
    from api.app import app

    assert app is not None
    assert hasattr(app, 'title')
    assert app.title == "Quote System API"