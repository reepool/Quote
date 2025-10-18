"""
Unit tests for BaoStock data source
"""

import pytest
import asyncio
from datetime import date, datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import pandas as pd

from data_sources.baostock_source import BaostockSource
from data_sources.base_source import BaseDataSource
from utils.exceptions import DataSourceError, ValidationError


@pytest.mark.unit
class TestBaoStockSource:
    """Test cases for BaoStockSource class"""

    @pytest.fixture
    def baostock_config(self):
        """BaoStock configuration for testing"""
        return {
            "enabled": True,
            "rate_limit": {
                "requests_per_second": 1,
                "burst_size": 5
            },
            "timeout": 30,
            "retry_attempts": 3,
            "retry_delay": 1
        }

    @pytest.fixture
    async def baostock_source(self, baostock_config):
        """Create BaoStockSource instance for testing"""
        with patch('data_sources.baostock_source.baostock'):
            source = BaoStockSource(baostock_config)
            await source.initialize()
            return source

    @pytest.mark.asyncio
    async def test_initialize(self, baostock_source):
        """Test BaoStock source initialization"""
        assert baostock_source.is_initialized() is True
        assert baostock_source.name == "BaoStock"

    @pytest.mark.asyncio
    async def test_get_stock_list(self, baostock_source):
        """Test getting stock list from BaoStock"""
        # Mock baostock response
        mock_stock_basic = pd.DataFrame({
            'code': ['sh.000001', 'sz.000001'],
            'code_name': ['平安银行', '平安银行'],
            'industry': ['银行', '银行'],
            'type': ['1', '1'],
            'status': ['1', '1']
        })

        with patch('data_sources.baostock_source.baostock.query_stock_basic') as mock_query:
            mock_query.return_value = (mock_stock_basic, "mock_error")

            result = await baostock_source.get_stock_list()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert '000001.SH' in result['code'].values
        assert '000001.SZ' in result['code'].values

    @pytest.mark.asyncio
    async def test_get_stock_list_with_exchange_filter(self, baostock_source):
        """Test getting stock list with exchange filter"""
        mock_stock_basic = pd.DataFrame({
            'code': ['sh.000001', 'sz.000001'],
            'code_name': ['平安银行', '平安银行'],
            'industry': ['银行', '银行'],
            'type': ['1', '1'],
            'status': ['1', '1']
        })

        with patch('data_sources.baostock_source.baostock.query_stock_basic') as mock_query:
            mock_query.return_value = (mock_stock_basic, "mock_error")

            result = await baostock_source.get_stock_list(exchange='SSE')

        assert isinstance(result, pd.DataFrame)
        assert all(code.endswith('.SH') for code in result['code'])

    @pytest.mark.asyncio
    async def test_get_daily_data(self, baostock_source):
        """Test getting daily quote data"""
        mock_data = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'code': ['sh.000001', 'sh.000001'],
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [9.5, 10.0],
            'close': [10.8, 11.2],
            'preclose': [10.0, 10.8],
            'volume': [1000000, 1200000],
            'amount': [10800000, 13440000]
        })

        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.return_value = (mock_data, "mock_error")

            result = await baostock_source.get_daily_data(
                '000001.SH',
                date(2024, 1, 1),
                date(2024, 1, 2)
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'open' in result.columns
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'close' in result.columns

    @pytest.mark.asyncio
    async def test_get_daily_data_with_invalid_symbol(self, baostock_source):
        """Test getting daily data with invalid symbol"""
        with pytest.raises(ValidationError):
            await baostock_source.get_daily_data(
                'INVALID',
                date(2024, 1, 1),
                date(2024, 1, 2)
            )

    @pytest.mark.asyncio
    async def test_rate_limiting(self, baostock_source):
        """Test rate limiting functionality"""
        mock_data = pd.DataFrame({
            'date': ['2024-01-01'],
            'code': ['sh.000001'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })

        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.return_value = (mock_data, "mock_error")

            # Make multiple requests quickly
            start_time = datetime.now()
            for i in range(3):
                await baostock_source.get_daily_data(
                    '000001.SH',
                    date(2024, 1, 1),
                    date(2024, 1, 2)
                )
            end_time = datetime.now()

            # Should respect rate limiting (1 request per second)
            assert (end_time - start_time).total_seconds() >= 2.0

    @pytest.mark.asyncio
    async def test_retry_mechanism(self, baostock_source):
        """Test retry mechanism on failures"""
        # First two calls fail, third succeeds
        mock_data = pd.DataFrame({
            'date': ['2024-01-01'],
            'code': ['sh.000001'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'volume': [1000000]
        })

        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.side_effect = [
                Exception("Network error"),
                Exception("Network error"),
                (mock_data, "mock_error")
            ]

            result = await baostock_source.get_daily_data(
                '000001.SH',
                date(2024, 1, 1),
                date(2024, 1, 2)
            )

        assert isinstance(result, pd.DataFrame)
        assert mock_query.call_count == 3

    @pytest.mark.asyncio
    async def test_max_retry_exceeded(self, baostock_source):
        """Test behavior when max retries are exceeded"""
        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.side_effect = Exception("Persistent network error")

            with pytest.raises(DataSourceError):
                await baostock_source.get_daily_data(
                    '000001.SH',
                    date(2024, 1, 1),
                    date(2024, 1, 2)
                )

        assert mock_query.call_count == 3  # Configured retry attempts

    @pytest.mark.asyncio
    async def test_connection_error_handling(self, baostock_source):
        """Test handling of connection errors"""
        with patch('data_sources.baostock_source.baostock.login', side_effect=Exception("Connection failed")):
            with pytest.raises(DataSourceError):
                await baostock_source.get_stock_list()

    @pytest.mark.asyncio
    async def test_data_validation(self, baostock_source):
        """Test data validation for returned data"""
        # Test with invalid data (missing required columns)
        invalid_data = pd.DataFrame({
            'date': ['2024-01-01'],
            'code': ['sh.000001'],
            'open': [10.0]
            # Missing other required columns
        })

        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.return_value = (invalid_data, "mock_error")

            result = await baostock_source.get_daily_data(
                '000001.SH',
                date(2024, 1, 1),
                date(2024, 1, 2)
            )

        # Should add missing columns with default values
        assert isinstance(result, pd.DataFrame)
        assert 'high' in result.columns
        assert 'low' in result.columns
        assert 'close' in result.columns

    @pytest.mark.asyncio
    async def test_empty_data_handling(self, baostock_source):
        """Test handling of empty data responses"""
        empty_data = pd.DataFrame()

        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.return_value = (empty_data, "mock_error")

            result = await baostock_source.get_daily_data(
                '000001.SH',
                date(2024, 1, 1),
                date(2024, 1, 2)
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_symbol_format_conversion(self, baostock_source):
        """Test symbol format conversion"""
        # Test conversion from standard format to baostock format
        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_data = pd.DataFrame({
                'date': ['2024-01-01'],
                'code': ['sh.000001'],
                'open': [10.0],
                'high': [11.0],
                'low': [9.5],
                'close': [10.8],
                'volume': [1000000]
            })
            mock_query.return_value = (mock_data, "mock_error")

            await baostock_source.get_daily_data(
                '000001.SH',  # Standard format
                date(2024, 1, 1),
                date(2024, 1, 2)
            )

        # Should convert to baostock format (sh.000001)
        called_symbol = mock_query.call_args[0][0]
        assert called_symbol == 'sh.000001'

    @pytest.mark.asyncio
    async def test_trading_day_check(self, baostock_source):
        """Test trading day functionality"""
        # Mock baostock query_trade_date response
        mock_data = pd.DataFrame({
            'cal_date': ['2024-01-01', '2024-01-02'],
            'is_trading_day': ['0', '1']
        })

        with patch('data_sources.baostock_source.baostock.query_trade_date') as mock_query:
            mock_query.return_value = (mock_data, "mock_error")

            # Test non-trading day
            is_trading = await baostock_source.is_trading_day(date(2024, 1, 1))
            assert is_trading is False

            # Test trading day
            is_trading = await baostock_source.is_trading_day(date(2024, 1, 2))
            assert is_trading is True

    @pytest.mark.asyncio
    async def test_get_adjusted_data(self, baostock_source):
        """Test getting adjusted price data"""
        mock_data = pd.DataFrame({
            'date': ['2024-01-01'],
            'code': ['sh.000001'],
            'open': [10.0],
            'high': [11.0],
            'low': [9.5],
            'close': [10.8],
            'preclose': [10.0],
            'adjustflag': ['3'],  # Post-adjusted
            'volume': [1000000]
        })

        with patch('data_sources.baostock_source.baostock.query_history_k_data_plus') as mock_query:
            mock_query.return_value = (mock_data, "mock_error")

            result = await baostock_source.get_daily_data(
                '000001.SH',
                date(2024, 1, 1),
                date(2024, 1, 2),
                adjust='post'
            )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_cleanup(self, baostock_source):
        """Test cleanup and resource release"""
        # Should not raise any exceptions
        await baostock_source.cleanup()
        assert baostock_source.is_initialized() is False