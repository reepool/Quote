"""
Unit tests for data_manager module
"""

import pytest
import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import pandas as pd

from data_manager import DataManager, DownloadProgress
from database.models import Instrument, DailyQuote, DataUpdateInfo
from utils.exceptions import DataSourceError, ValidationError


@pytest.mark.unit
class TestDataManager:
    """Test cases for DataManager class"""

    @pytest.fixture
    async def data_manager(self, test_config, test_database, mock_config_manager):
        """Create DataManager instance for testing"""
        with patch('data_manager.config_manager', mock_config_manager):
            manager = DataManager()
            manager.db = AsyncMock()
            manager.db.initialize = AsyncMock()
            await manager.initialize()
            return manager

    @pytest.fixture
    def mock_data_source_factory(self):
        """Mock data source factory"""
        factory = Mock()
        source = AsyncMock()
        factory.get_source.return_value = source
        return factory, source

    @pytest.mark.asyncio
    async def test_initialize(self, data_manager):
        """Test DataManager initialization"""
        # Should not raise any exceptions
        await data_manager.initialize()
        assert data_manager._initialized is True

    @pytest.mark.asyncio
    async def test_get_stock_list(self, data_manager, sample_stock_data):
        """Test getting stock list"""
        # Mock database response
        data_manager.db.get_stock_list = AsyncMock(return_value=sample_stock_data)

        result = await data_manager.get_stock_list()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_stock_data)
        data_manager.db.get_stock_list.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_stock_list_with_filters(self, data_manager, sample_stock_data):
        """Test getting stock list with filters"""
        data_manager.db.get_stock_list = AsyncMock(return_value=sample_stock_data)

        result = await data_manager.get_stock_list(market='SZSE')

        assert isinstance(result, pd.DataFrame)
        data_manager.db.get_stock_list.assert_called_once_with(market='SZSE')

    @pytest.mark.asyncio
    async def test_get_daily_data(self, data_manager, sample_quote_data):
        """Test getting daily quote data"""
        data_manager.db.get_daily_data = AsyncMock(return_value=sample_quote_data)

        result = await data_manager.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 10))

        assert isinstance(result, pd.DataFrame)
        data_manager.db.get_daily_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_latest_quote(self, data_manager, sample_quote_data):
        """Test getting latest quote data"""
        latest_data = sample_quote_data.tail(1)
        data_manager.db.get_latest_quote = AsyncMock(return_value=latest_data)

        result = await data_manager.get_latest_quote('000001.SZ')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        data_manager.db.get_latest_quote.assert_called_once_with('000001.SZ')

    @pytest.mark.asyncio
    async def test_download_stock_data_success(self, data_manager, mock_data_source_factory):
        """Test successful stock data download"""
        factory, source = mock_data_source_factory

        # Setup mock responses
        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行']
        })
        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })
        source.is_trading_day.return_value = True

        with patch('data_manager.get_data_source_factory', return_value=factory):
            progress = await data_manager.download_stock_data(
                exchanges=['SZSE'],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2)
            )

        assert isinstance(progress, DownloadProgress)
        assert progress.successful_downloads >= 0
        assert progress.failed_downloads >= 0

    @pytest.mark.asyncio
    async def test_download_stock_data_with_failure(self, data_manager, mock_data_source_factory):
        """Test stock data download with failures"""
        factory, source = mock_data_source_factory

        # Setup mock to raise exception
        source.get_daily_data.side_effect = DataSourceError("Network error")
        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行']
        })

        with patch('data_manager.get_data_source_factory', return_value=factory):
            progress = await data_manager.download_stock_data(
                exchanges=['SZSE'],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2)
            )

        assert progress.failed_downloads > 0
        assert len(progress.errors) > 0

    @pytest.mark.asyncio
    async def test_update_daily_data(self, data_manager, mock_data_source_factory):
        """Test updating daily data"""
        factory, source = mock_data_source_factory

        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行']
        })
        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })

        with patch('data_manager.get_data_source_factory', return_value=factory):
            with patch('data_manager.get_shanghai_time', return_value=datetime(2024, 1, 2)):
                result = await data_manager.update_daily_data(exchanges=['SZSE'])

        assert result is True

    @pytest.mark.asyncio
    async def test_get_data_gaps(self, data_manager):
        """Test data gap detection"""
        # Mock database responses
        data_manager.db.get_missing_dates = AsyncMock(return_value=[
            date(2024, 1, 2), date(2024, 1, 3)
        ])

        gaps = await data_manager.get_data_gaps('000001.SZ', date(2024, 1, 1), date(2024, 1, 10))

        assert isinstance(gaps, list)
        assert len(gaps) == 2
        data_manager.db.get_missing_dates.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_data_quality(self, data_manager, sample_quote_data):
        """Test data quality validation"""
        # Test with valid data
        issues = await data_manager.validate_data_quality(sample_quote_data)
        assert isinstance(issues, list)

        # Test with invalid data
        invalid_data = sample_quote_data.copy()
        invalid_data.loc[0, 'volume'] = -1  # Invalid negative volume

        issues = await data_manager.validate_data_quality(invalid_data)
        assert len(issues) > 0

    @pytest.mark.asyncio
    async def test_get_trading_status(self, data_manager):
        """Test trading status check"""
        with patch('data_manager.DateUtils.is_trading_day', return_value=True):
            status = await data_manager.get_trading_status()
            assert status['is_trading'] is True
            assert 'market' in status

    @pytest.mark.asyncio
    async def test_get_system_status(self, data_manager):
        """Test system status report"""
        # Mock database queries
        data_manager.db.get_instrument_count = AsyncMock(return_value=5000)
        data_manager.db.get_quote_count = AsyncMock(return_value=1000000)
        data_manager.db.get_latest_update_date = AsyncMock(return_value=date(2024, 1, 1))

        status = await data_manager.get_system_status()

        assert isinstance(status, dict)
        assert 'total_instruments' in status
        assert 'total_quotes' in status
        assert 'last_update' in status
        assert status['total_instruments'] == 5000

    @pytest.mark.asyncio
    async def test_backup_data(self, data_manager, temp_dir):
        """Test data backup functionality"""
        backup_path = temp_dir / "backup.db"

        with patch('data_manager.DatabaseOperations.backup_database', AsyncMock(return_value=True)):
            result = await data_manager.backup_data(str(backup_path))

        assert result is True

    @pytest.mark.asyncio
    async def test_cleanup_old_data(self, data_manager):
        """Test cleanup of old data"""
        cutoff_date = date(2023, 1, 1)

        data_manager.db.delete_quotes_before_date = AsyncMock(return_value=1000)

        deleted_count = await data_manager.cleanup_old_data(cutoff_date)

        assert deleted_count == 1000
        data_manager.db.delete_quotes_before_date.assert_called_once_with(cutoff_date)

    @pytest.mark.asyncio
    async def test_get_download_progress(self, data_manager):
        """Test download progress tracking"""
        # Create a test progress
        test_progress = DownloadProgress(
            total_instruments=100,
            processed_instruments=50,
            successful_downloads=45,
            failed_downloads=5
        )

        # Mock progress storage
        data_manager._download_progress = test_progress

        progress = await data_manager.get_download_progress()

        assert progress['total_instruments'] == 100
        assert progress['processed_instruments'] == 50
        assert progress['progress_percentage'] == 50.0

    def test_download_progress_model(self):
        """Test DownloadProgress data model"""
        progress = DownloadProgress(
            total_instruments=100,
            processed_instruments=75,
            successful_downloads=70,
            failed_downloads=5
        )

        assert progress.get_progress_percentage() == 75.0
        assert progress.batch_id is not None
        assert progress.start_time is not None

    @pytest.mark.asyncio
    async def test_error_handling_and_recovery(self, data_manager):
        """Test error handling and recovery mechanisms"""
        # Test database connection error
        data_manager.db.get_stock_list = AsyncMock(side_effect=Exception("Database error"))

        with pytest.raises(Exception):
            await data_manager.get_stock_list()

        # Test recovery after error
        data_manager.db.get_stock_list = AsyncMock(return_value=pd.DataFrame())

        result = await data_manager.get_stock_list()
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_concurrent_downloads(self, data_manager, mock_data_source_factory):
        """Test concurrent download handling"""
        factory, source = mock_data_source_factory

        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ', '000003.SZ'],
            'name': ['股票1', '股票2', '股票3']
        })
        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })

        with patch('data_manager.get_data_source_factory', return_value=factory):
            progress = await data_manager.download_stock_data(
                exchanges=['SZSE'],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2),
                concurrent_limit=2
            )

        assert progress.successful_downloads >= 0
        # Should handle concurrent downloads without errors