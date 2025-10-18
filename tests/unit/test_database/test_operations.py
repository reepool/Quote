"""
Unit tests for database operations
"""

import pytest
import asyncio
from datetime import date, datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import pandas as pd

from database.operations import DatabaseOperations
from database.models import Instrument, DailyQuote, TradingCalendar, DataUpdateInfo
from utils.exceptions import DatabaseError, ValidationError


@pytest.mark.unit
class TestDatabaseOperations:
    """Test cases for DatabaseOperations class"""

    @pytest.fixture
    async def db_operations(self, test_database):
        """Create DatabaseOperations instance for testing"""
        ops = DatabaseOperations(test_database)
        await ops.initialize()
        return ops

    @pytest.mark.asyncio
    async def test_initialize(self, db_operations):
        """Test database operations initialization"""
        assert db_operations is not None
        assert db_operations.db is not None

    @pytest.mark.asyncio
    async def test_insert_instruments(self, db_operations, sample_instrument_data):
        """Test inserting instrument data"""
        result = await db_operations.insert_instruments(sample_instrument_data)
        assert result is True

        # Verify data was inserted
        instruments = await db_operations.get_stock_list()
        assert len(instruments) == len(sample_instrument_data)

    @pytest.mark.asyncio
    async def test_insert_duplicate_instruments(self, db_operations, sample_instrument_data):
        """Test handling duplicate instrument inserts"""
        # Insert first time
        await db_operations.insert_instruments(sample_instrument_data)

        # Try to insert same data again
        result = await db_operations.insert_instruments(sample_instrument_data)
        assert result is True  # Should handle duplicates gracefully

    @pytest.mark.asyncio
    async def test_get_stock_list(self, db_operations, sample_instrument_data):
        """Test getting stock list"""
        # Insert test data
        await db_operations.insert_instruments(sample_instrument_data)

        # Get all stocks
        stocks = await db_operations.get_stock_list()
        assert isinstance(stocks, pd.DataFrame)
        assert len(stocks) == len(sample_instrument_data)

        # Get stocks with filter
        szse_stocks = await db_operations.get_stock_list(market='SZSE')
        assert len(szse_stocks) == 2  # Two SZSE stocks in sample data

    @pytest.mark.asyncio
    async def test_get_instrument_by_code(self, db_operations, sample_instrument_data):
        """Test getting instrument by code"""
        await db_operations.insert_instruments(sample_instrument_data)

        instrument = await db_operations.get_instrument_by_code('000001.SZ')
        assert instrument is not None
        assert instrument['code'] == '000001.SZ'
        assert instrument['name'] == '平安银行'

    @pytest.mark.asyncio
    async def test_get_nonexistent_instrument(self, db_operations):
        """Test getting non-existent instrument"""
        instrument = await db_operations.get_instrument_by_code('NONEXISTENT')
        assert instrument is None

    @pytest.mark.asyncio
    async def test_insert_daily_quotes(self, db_operations, sample_quote_data):
        """Test inserting daily quote data"""
        result = await db_operations.insert_daily_quotes(sample_quote_data)
        assert result is True

        # Verify data was inserted
        quotes = await db_operations.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 10))
        assert isinstance(quotes, pd.DataFrame)
        assert len(quotes) > 0

    @pytest.mark.asyncio
    async def test_get_daily_data(self, db_operations, sample_quote_data):
        """Test getting daily quote data"""
        # Insert test data
        await db_operations.insert_daily_quotes(sample_quote_data)

        # Get data for specific stock
        quotes = await db_operations.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 10))
        assert isinstance(quotes, pd.DataFrame)
        assert len(quotes) > 0
        assert 'open' in quotes.columns
        assert 'high' in quotes.columns
        assert 'low' in quotes.columns
        assert 'close' in quotes.columns

    @pytest.mark.asyncio
    async def test_get_daily_data_no_results(self, db_operations):
        """Test getting daily data with no results"""
        quotes = await db_operations.get_daily_data('NONEXISTENT', date(2024, 1, 1), date(2024, 1, 10))
        assert isinstance(quotes, pd.DataFrame)
        assert len(quotes) == 0

    @pytest.mark.asyncio
    async def test_get_latest_quote(self, db_operations, sample_quote_data):
        """Test getting latest quote"""
        await db_operations.insert_daily_quotes(sample_quote_data)

        latest_quote = await db_operations.get_latest_quote('000001.SZ')
        assert isinstance(latest_quote, pd.DataFrame)
        assert len(latest_quote) == 1

    @pytest.mark.asyncio
    async def test_update_instrument_status(self, db_operations, sample_instrument_data):
        """Test updating instrument status"""
        await db_operations.insert_instruments(sample_instrument_data)

        # Update status
        result = await db_operations.update_instrument_status('000001.SZ', 'inactive')
        assert result is True

        # Verify update
        instrument = await db_operations.get_instrument_by_code('000001.SZ')
        assert instrument['status'] == 'inactive'

    @pytest.mark.asyncio
    async def test_get_missing_dates(self, db_operations, sample_quote_data):
        """Test getting missing trading dates"""
        # Insert data for some dates only
        partial_data = sample_quote_data[sample_quote_data['date'] <= '2024-01-05']
        await db_operations.insert_daily_quotes(partial_data)

        # Get missing dates
        missing_dates = await db_operations.get_missing_dates('000001.SZ', date(2024, 1, 1), date(2024, 1, 10))
        assert isinstance(missing_dates, list)
        assert len(missing_dates) > 0

    @pytest.mark.asyncio
    async def test_get_instrument_count(self, db_operations, sample_instrument_data):
        """Test getting instrument count"""
        await db_operations.insert_instruments(sample_instrument_data)

        count = await db_operations.get_instrument_count()
        assert isinstance(count, int)
        assert count == len(sample_instrument_data)

    @pytest.mark.asyncio
    async def test_get_quote_count(self, db_operations, sample_quote_data):
        """Test getting quote count"""
        await db_operations.insert_daily_quotes(sample_quote_data)

        count = await db_operations.get_quote_count()
        assert isinstance(count, int)
        assert count == len(sample_quote_data)

    @pytest.mark.asyncio
    async def test_get_latest_update_date(self, db_operations, sample_quote_data):
        """Test getting latest update date"""
        await db_operations.insert_daily_quotes(sample_quote_data)

        latest_date = await db_operations.get_latest_update_date()
        assert isinstance(latest_date, date)

    @pytest.mark.asyncio
    async def test_delete_quotes_before_date(self, db_operations, sample_quote_data):
        """Test deleting quotes before a specific date"""
        await db_operations.insert_daily_quotes(sample_quote_data)

        # Delete data before 2024-01-05
        deleted_count = await db_operations.delete_quotes_before_date(date(2024, 1, 5))
        assert isinstance(deleted_count, int)
        assert deleted_count > 0

        # Verify deletion
        remaining_quotes = await db_operations.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 10))
        assert all(pd.to_datetime(quotes['date']) >= date(2024, 1, 5) for quotes in [remaining_quotes])

    @pytest.mark.asyncio
    async def test_backup_database(self, db_operations, temp_dir):
        """Test database backup"""
        backup_path = temp_dir / "backup.db"

        result = await db_operations.backup_database(str(backup_path))
        assert result is True
        assert backup_path.exists()

    @pytest.mark.asyncio
    async def test_restore_database(self, db_operations, temp_dir):
        """Test database restore"""
        # First create backup
        backup_path = temp_dir / "backup.db"
        await db_operations.backup_database(str(backup_path))

        # Then restore from backup
        result = await db_operations.restore_database(str(backup_path))
        assert result is True

    @pytest.mark.asyncio
    async def test_get_data_quality_report(self, db_operations, sample_quote_data):
        """Test getting data quality report"""
        await db_operations.insert_daily_quotes(sample_quote_data)

        report = await db_operations.get_data_quality_report()
        assert isinstance(report, dict)
        assert 'total_quotes' in report
        assert 'missing_dates' in report
        assert 'duplicate_records' in report

    @pytest.mark.asyncio
    async def test_vacuum_database(self, db_operations):
        """Test database vacuum operation"""
        result = await db_operations.vacuum_database()
        assert result is True

    @pytest.mark.asyncio
    async def test_get_database_stats(self, db_operations, sample_instrument_data, sample_quote_data):
        """Test getting database statistics"""
        await db_operations.insert_instruments(sample_instrument_data)
        await db_operations.insert_daily_quotes(sample_quote_data)

        stats = await db_operations.get_database_stats()
        assert isinstance(stats, dict)
        assert 'table_sizes' in stats
        assert 'total_records' in stats
        assert 'database_size' in stats

    @pytest.mark.asyncio
    async def test_transaction_handling(self, db_operations):
        """Test transaction handling"""
        # Test successful transaction
        async with db_operations.transaction():
            await db_operations.insert_instruments([{
                'code': 'TEST.TRANSACTION',
                'name': 'Test Transaction',
                'market': 'TEST',
                'industry': 'Test',
                'list_date': '2024-01-01',
                'status': 'active'
            }])

        # Verify data was committed
        instrument = await db_operations.get_instrument_by_code('TEST.TRANSACTION')
        assert instrument is not None

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, db_operations):
        """Test transaction rollback on error"""
        # Insert initial data
        await db_operations.insert_instruments([{
            'code': 'TEST.ROLLBACK',
            'name': 'Test Rollback',
            'market': 'TEST',
            'industry': 'Test',
            'list_date': '2024-01-01',
            'status': 'active'
        }])

        # Test transaction rollback
        try:
            async with db_operations.transaction():
                await db_operations.update_instrument_status('TEST.ROLLBACK', 'modified')
                raise Exception("Simulated error")
        except Exception:
            pass

        # Verify data was rolled back
        instrument = await db_operations.get_instrument_by_code('TEST.ROLLBACK')
        assert instrument['status'] == 'active'  # Should not be 'modified'

    @pytest.mark.asyncio
    async def test_bulk_insert_performance(self, db_operations):
        """Test bulk insert performance"""
        # Create large dataset
        large_dataset = []
        for i in range(1000):
            large_dataset.append({
                'code': f'TEST{i:04d}.BULK',
                'name': f'Test Stock {i}',
                'market': 'BULK',
                'industry': 'Test',
                'list_date': '2024-01-01',
                'status': 'active'
            })

        # Test bulk insert
        start_time = datetime.now()
        result = await db_operations.insert_instruments(large_dataset)
        end_time = datetime.now()

        assert result is True
        assert (end_time - start_time).total_seconds() < 5.0  # Should complete quickly

    @pytest.mark.asyncio
    async def test_connection_error_handling(self, db_operations):
        """Test database connection error handling"""
        # Mock database connection error
        db_operations.db.execute = AsyncMock(side_effect=Exception("Connection lost"))

        with pytest.raises(DatabaseError):
            await db_operations.get_stock_list()

    @pytest.mark.asyncio
    async def test_data_validation(self, db_operations):
        """Test data validation"""
        # Test with invalid instrument data
        invalid_data = [{
            'code': '',  # Invalid empty code
            'name': 'Test',
            'market': 'TEST',
            'industry': 'Test',
            'list_date': '2024-01-01',
            'status': 'active'
        }]

        with pytest.raises(ValidationError):
            await db_operations.insert_instruments(invalid_data)