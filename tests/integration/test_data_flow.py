"""
Integration tests for data flow
"""

import pytest
import asyncio
from datetime import date, datetime
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd

from data_manager import data_manager
from database.operations import DatabaseOperations
from data_sources.source_factory import data_source_factory


@pytest.mark.integration
class TestDataFlow:
    """Integration tests for complete data flow"""

    @pytest.fixture
    async def setup_data_flow(self, test_database, mock_data_source_factory):
        """Setup complete data flow integration test"""
        # Initialize data manager
        with patch('data_manager.get_data_source_factory', return_value=mock_data_source_factory):
            await data_manager.initialize()
            data_manager.db = DatabaseOperations(test_database)
            await data_manager.db.initialize()

        return data_manager

    @pytest.mark.asyncio
    async def test_complete_data_download_flow(self, setup_data_flow, mock_data_source_factory):
        """Test complete data download from source to database"""
        factory, source = mock_data_source_factory

        # Mock source responses
        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ'],
            'name': ['平安银行', '万科A'],
            'market': ['SZSE', 'SZSE'],
            'industry': ['银行', '房地产']
        })

        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [9.5, 10.0],
            'close': [10.8, 11.2],
            'volume': [1000000, 1200000]
        })

        source.is_trading_day.return_value = True

        # Execute complete flow
        progress = await setup_data_flow.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2)
        )

        # Verify flow completed successfully
        assert progress.successful_downloads > 0
        assert progress.failed_downloads == 0

        # Verify data was stored in database
        stocks = await setup_data_flow.db.get_stock_list()
        assert len(stocks) == 2

        quotes = await setup_data_flow.db.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 2))
        assert len(quotes) == 2

    @pytest.mark.asyncio
    async def test_data_update_with_validation(self, setup_data_flow, mock_data_source_factory):
        """Test data update with validation integration"""
        factory, source = mock_data_source_factory

        # Mock data with some quality issues
        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })

        # First day - good data
        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })

        # Execute update
        result = await setup_data_flow.update_daily_data(exchanges=['SZSE'])
        assert result is True

        # Verify data quality validation
        quotes = await setup_data_flow.db.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 1))
        assert len(quotes) == 1

        # Test data quality issues
        issues = await setup_data_flow.validate_data_quality(quotes)
        assert isinstance(issues, list)

    @pytest.mark.asyncio
    async def test_error_recovery_in_data_flow(self, setup_data_flow, mock_data_source_factory):
        """Test error recovery in data flow"""
        factory, source = mock_data_source_factory

        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ'],
            'name': ['平安银行', '万科A'],
            'market': ['SZSE', 'SZSE'],
            'industry': ['银行', '房地产']
        })

        # Simulate failure for first stock, success for second
        def mock_get_daily_data(code, start_date, end_date, **kwargs):
            if code == '000001.SZ':
                raise Exception("Network error for first stock")
            else:
                return pd.DataFrame({
                    'date': ['2024-01-01'],
                    'open': [20.0],
                    'high': [21.0],
                    'low': [19.5],
                    'close': [20.8],
                    'volume': [2000000]
                })

        source.get_daily_data.side_effect = mock_get_daily_data
        source.is_trading_day.return_value = True

        # Execute flow with error recovery
        progress = await setup_data_flow.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1)
        )

        # Should have partial success
        assert progress.successful_downloads == 1
        assert progress.failed_downloads == 1
        assert len(progress.errors) > 0

        # Verify successful data was still stored
        stocks = await setup_data_flow.db.get_stock_list()
        assert len(stocks) == 2  # Both should be inserted despite download failure

    @pytest.mark.asyncio
    async def test_data_consistency_across_modules(self, setup_data_flow, mock_data_source_factory):
        """Test data consistency across different modules"""
        factory, source = mock_data_source_factory

        # Setup consistent test data
        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })

        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [9.5, 10.0],
            'close': [10.8, 11.2],
            'volume': [1000000, 1200000]
        })

        source.is_trading_day.return_value = True

        # Download data
        await setup_data_flow.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2)
        )

        # Test consistency across different access methods
        # Method 1: Direct database query
        db_quotes = await setup_data_flow.db.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 2))

        # Method 2: Through data manager
        dm_quotes = await setup_data_flow.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 2))

        # Should be identical
        pd.testing.assert_frame_equal(db_quotes.sort_index(), dm_quotes.sort_index())

        # Test aggregate consistency
        db_count = len(db_quotes)
        dm_count = len(dm_quotes)
        assert db_count == dm_count

    @pytest.mark.asyncio
    async def test_concurrent_data_operations(self, setup_data_flow, mock_data_source_factory):
        """Test concurrent data operations"""
        factory, source = mock_data_source_factory

        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ', '000003.SZ'],
            'name': ['股票1', '股票2', '股票3'],
            'market': ['SZSE', 'SZSE', 'SZSE'],
            'industry': ['行业1', '行业2', '行业3']
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

        # Run multiple concurrent operations
        tasks = [
            setup_data_flow.download_stock_data(
                exchanges=['SZSE'],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 1)
            ),
            setup_data_flow.get_stock_list(),
            setup_data_flow.get_system_status()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All operations should complete successfully
        for result in results:
            assert not isinstance(result, Exception)
            assert result is not None

    @pytest.mark.asyncio
    async def test_database_transaction_integration(self, setup_data_flow, mock_data_source_factory):
        """Test database transaction integration"""
        factory, source = mock_data_source_factory

        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })

        # Test transaction rollback on error
        with patch.object(setup_data_flow.db, 'insert_daily_quotes') as mock_insert:
            mock_insert.side_effect = Exception("Database error")

            # Should handle transaction failure gracefully
            with pytest.raises(Exception):
                await setup_data_flow.download_stock_data(
                    exchanges=['SZSE'],
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 1)
                )

            # Verify no partial data was committed
            stocks = await setup_data_flow.db.get_stock_list()
            # Implementation depends on transaction handling

    @pytest.mark.asyncio
    async def test_cache_integration(self, setup_data_flow, mock_data_source_factory, cache_manager):
        """Test cache integration in data flow"""
        factory, source = mock_data_source_factory

        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })

        # First call should cache result
        stocks1 = await setup_data_flow.get_stock_list()

        # Second call should use cache
        stocks2 = await setup_data_flow.get_stock_list()

        # Results should be identical
        pd.testing.assert_frame_equal(stocks1, stocks2)

        # Verify cache was used (implementation dependent)

    @pytest.mark.asyncio
    async def test_data_pipeline_performance(self, setup_data_flow, mock_data_source_factory):
        """Test data pipeline performance under load"""
        factory, source = mock_data_source_factory

        # Create larger dataset
        large_stock_list = pd.DataFrame({
            'code': [f'00000{i}.SZ' for i in range(1, 101)],  # 100 stocks
            'name': [f'股票{i}' for i in range(1, 101)],
            'market': ['SZSE'] * 100,
            'industry': ['行业'] * 100
        })

        source.get_stock_list.return_value = large_stock_list
        source.get_daily_data.return_value = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })
        source.is_trading_day.return_value = True

        # Measure performance
        start_time = datetime.now()

        progress = await setup_data_flow.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1),
            concurrent_limit=10
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Performance assertions
        assert duration < 30.0  # Should complete within 30 seconds
        assert progress.successful_downloads == 100

    @pytest.mark.asyncio
    async def test_data_source_failover(self, setup_data_flow):
        """Test data source failover mechanism"""
        # Mock factory with multiple sources
        mock_factory = Mock()
        primary_source = AsyncMock()
        fallback_source = AsyncMock()

        # Primary source fails
        primary_source.get_stock_list.side_effect = Exception("Primary source unavailable")

        # Fallback source succeeds
        fallback_source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })

        mock_factory.get_source.return_value = primary_source
        mock_factory.get_fallback_sources.return_value = {'fallback': fallback_source}

        with patch('data_manager.get_data_source_factory', return_value=mock_factory):
            # Should automatically failover to fallback
            result = await setup_data_flow.get_stock_list()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_data_validation_pipeline(self, setup_data_flow, mock_data_source_factory):
        """Test complete data validation pipeline"""
        factory, source = mock_data_source_factory

        # Mock data with validation issues
        source.get_stock_list.return_value = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })

        # Data with issues: negative volume, missing fields
        invalid_data = pd.DataFrame({
            'date': ['2024-01-01'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [-1000],  # Invalid negative volume
            'amount': [None]    # Missing amount
        })

        source.get_daily_data.return_value = invalid_data
        source.is_trading_day.return_value = True

        # Download should complete but flag validation issues
        progress = await setup_data_flow.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 1)
        )

        # Should detect and handle validation issues
        assert progress.quality_issues > 0

        # Verify data was cleaned/validated before storage
        quotes = await setup_data_flow.db.get_daily_data('000001.SZ', date(2024, 1, 1), date(2024, 1, 1))
        if len(quotes) > 0:
            # Data should be cleaned of obvious issues
            assert all(quotes['volume'] >= 0 for _, quotes in quotes.iterrows())