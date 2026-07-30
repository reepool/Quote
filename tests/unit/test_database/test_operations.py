"""
Unit tests for database operations
"""

import pytest
import asyncio
from datetime import date, datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
    async def test_factor_quote_evidence_uses_prior_trading_close(
        self,
        tmp_path,
    ):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'factor_evidence.db'}"
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE daily_quotes (
                        time DATETIME NOT NULL,
                        instrument_id VARCHAR(32) NOT NULL,
                        close FLOAT NOT NULL,
                        pre_close FLOAT,
                        tradestatus INTEGER NOT NULL
                    )
                """))
                await connection.execute(text("""
                    INSERT INTO daily_quotes
                        (time, instrument_id, close, pre_close, tradestatus)
                    VALUES
                        ('2024-06-13', '000001.SZ', 10.80, 10.88, 1),
                        ('2024-06-14', '000001.SZ', 10.80, 10.80, 0),
                        ('2024-06-17', '000001.SZ', 10.18, 10.08, 1)
                """))

            sessions = async_sessionmaker(engine, expire_on_commit=False)
            operations = DatabaseOperations(auto_initialize=False)
            operations.get_async_session = sessions
            evidence = await operations.get_quote_evidence_for_event_dates([
                ("000001.SZ", date(2024, 6, 14)),
            ])
        finally:
            await engine.dispose()

        assert evidence == [{
            "instrument_id": "000001.SZ",
            "source_date": "2024-06-14",
            "effective_date": "2024-06-17",
            "pre_close": 10.80,
            "close": 10.18,
        }]

    @pytest.mark.asyncio
    async def test_factor_quote_evidence_uses_first_resumed_session(
        self,
        tmp_path,
    ):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'suspended_factor_evidence.db'}"
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE daily_quotes (
                        time DATETIME NOT NULL,
                        instrument_id VARCHAR(32) NOT NULL,
                        close FLOAT NOT NULL,
                        pre_close FLOAT,
                        tradestatus INTEGER NOT NULL
                    )
                """))
                await connection.execute(text("""
                    INSERT INTO daily_quotes
                        (time, instrument_id, close, pre_close, tradestatus)
                    VALUES
                        ('2014-06-10', '002076.SZ', 10.07, 10.07, 1),
                        ('2014-06-13', '002076.SZ', 10.02, 10.02, 0),
                        ('2014-09-10', '002076.SZ', 10.02, 10.02, 0),
                        ('2014-09-11', '002076.SZ', 11.02, 10.02, 1)
                """))

            sessions = async_sessionmaker(engine, expire_on_commit=False)
            operations = DatabaseOperations(auto_initialize=False)
            operations.get_async_session = sessions
            evidence = await operations.get_quote_evidence_for_event_dates(
                [("002076.SZ", date(2014, 6, 13))],
                effective_end_date=date(2014, 12, 31),
            )
        finally:
            await engine.dispose()

        assert evidence == [{
            "instrument_id": "002076.SZ",
            "source_date": "2014-06-13",
            "effective_date": "2014-09-11",
            "pre_close": 10.07,
            "close": 11.02,
        }]

    @pytest.mark.asyncio
    async def test_factor_quote_evidence_does_not_cross_rebuild_end_date(
        self,
        tmp_path,
    ):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'bounded_factor_evidence.db'}"
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE daily_quotes (
                        time DATETIME NOT NULL,
                        instrument_id VARCHAR(32) NOT NULL,
                        close FLOAT NOT NULL,
                        pre_close FLOAT,
                        tradestatus INTEGER NOT NULL
                    )
                """))
                await connection.execute(text("""
                    INSERT INTO daily_quotes
                        (time, instrument_id, close, pre_close, tradestatus)
                    VALUES
                        ('2014-06-10', '002076.SZ', 10.07, 10.07, 1),
                        ('2014-06-13', '002076.SZ', 10.02, 10.02, 0),
                        ('2014-09-11', '002076.SZ', 11.02, 10.02, 1)
                """))

            sessions = async_sessionmaker(engine, expire_on_commit=False)
            operations = DatabaseOperations(auto_initialize=False)
            operations.get_async_session = sessions
            evidence = await operations.get_quote_evidence_for_event_dates(
                [("002076.SZ", date(2014, 6, 13))],
                effective_end_date=date(2014, 8, 31),
            )
        finally:
            await engine.dispose()

        assert evidence == [{
            "instrument_id": "002076.SZ",
            "source_date": "2014-06-13",
            "effective_date": None,
            "pre_close": None,
            "close": None,
        }]

    @pytest.mark.asyncio
    async def test_factor_quote_evidence_does_not_treat_quote_gap_as_suspension(
        self,
        tmp_path,
    ):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'quote_gap_factor_evidence.db'}"
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE daily_quotes (
                        time DATETIME NOT NULL,
                        instrument_id VARCHAR(32) NOT NULL,
                        close FLOAT NOT NULL,
                        pre_close FLOAT,
                        tradestatus INTEGER NOT NULL
                    )
                """))
                await connection.execute(text("""
                    INSERT INTO daily_quotes
                        (time, instrument_id, close, pre_close, tradestatus)
                    VALUES
                        ('2010-01-04', '000001.SZ', 10.00, 9.90, 1),
                        ('2020-01-02', '000001.SZ', 12.00, 11.90, 1)
                """))

            sessions = async_sessionmaker(engine, expire_on_commit=False)
            operations = DatabaseOperations(auto_initialize=False)
            operations.get_async_session = sessions
            evidence = await operations.get_quote_evidence_for_event_dates(
                [("000001.SZ", date(2010, 2, 1))],
                effective_end_date=date(2020, 12, 31),
            )
        finally:
            await engine.dispose()

        assert evidence[0]["effective_date"] is None

    @pytest.mark.asyncio
    async def test_factor_quote_evidence_opt_in_aligns_long_gap_to_next_trade(
        self,
        tmp_path,
    ):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'long_gap_alignment.db'}"
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE daily_quotes (
                        time DATETIME NOT NULL,
                        instrument_id VARCHAR(32) NOT NULL,
                        close FLOAT NOT NULL,
                        pre_close FLOAT,
                        tradestatus INTEGER NOT NULL
                    )
                """))
                await connection.execute(text("""
                    INSERT INTO daily_quotes
                        (time, instrument_id, close, pre_close, tradestatus)
                    VALUES
                        ('2016-03-17', '600145.SH', 6.40, 6.30, 1),
                        ('2016-03-18', '600145.SH', 0.00, 0.00, 0),
                        ('2020-06-30', '600145.SH', 5.10, 5.00, 1)
                """))

            sessions = async_sessionmaker(engine, expire_on_commit=False)
            operations = DatabaseOperations(auto_initialize=False)
            operations.get_async_session = sessions
            evidence = await operations.get_quote_evidence_for_event_dates(
                [("600145.SH", date(2016, 3, 18))],
                effective_end_date=date(2020, 12, 31),
                effective_end_dates_by_instrument={
                    "600145.SH": date(2020, 12, 31),
                },
                align_to_next_observed_trade=True,
            )
        finally:
            await engine.dispose()

        assert evidence == [{
            "instrument_id": "600145.SH",
            "source_date": "2016-03-18",
            "effective_date": "2020-06-30",
            "pre_close": 6.40,
            "close": 5.10,
        }]

    @pytest.mark.asyncio
    async def test_factor_quote_evidence_next_trade_respects_lifecycle_bound(
        self,
        tmp_path,
    ):
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{tmp_path / 'lifecycle_bound.db'}"
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE daily_quotes (
                        time DATETIME NOT NULL,
                        instrument_id VARCHAR(32) NOT NULL,
                        close FLOAT NOT NULL,
                        pre_close FLOAT,
                        tradestatus INTEGER NOT NULL
                    )
                """))
                await connection.execute(text("""
                    INSERT INTO daily_quotes
                        (time, instrument_id, close, pre_close, tradestatus)
                    VALUES
                        ('2006-12-18', '000549.SZ', 5.00, 4.90, 1),
                        ('2010-01-04', '000549.SZ', 8.00, 7.90, 1)
                """))

            sessions = async_sessionmaker(engine, expire_on_commit=False)
            operations = DatabaseOperations(auto_initialize=False)
            operations.get_async_session = sessions
            evidence = await operations.get_quote_evidence_for_event_dates(
                [("000549.SZ", date(2007, 4, 23))],
                effective_end_date=date(2010, 12, 31),
                effective_end_dates_by_instrument={
                    "000549.SZ": date(2007, 4, 27),
                },
                align_to_next_observed_trade=True,
            )
        finally:
            await engine.dispose()

        assert evidence == [{
            "instrument_id": "000549.SZ",
            "source_date": "2007-04-23",
            "effective_date": None,
            "pre_close": None,
            "close": None,
        }]

    @pytest.mark.asyncio
    async def test_factor_quote_evidence_next_trade_requires_finite_bound(
        self,
    ):
        operations = DatabaseOperations(auto_initialize=False)

        with pytest.raises(
            ValueError,
            match="effective_end_date is required",
        ):
            await operations.get_quote_evidence_for_event_dates(
                [("600145.SH", date(2016, 3, 18))],
                align_to_next_observed_trade=True,
            )

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
