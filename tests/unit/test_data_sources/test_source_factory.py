"""
Unit tests for data source factory
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from data_sources.source_factory import DataSourceFactory
from data_sources.baostock_source import BaoStockSource
from data_sources.yfinance_source import YFinanceSource
from data_sources.akshare_source import AkShareSource
from utils.exceptions import ConfigurationError


@pytest.mark.unit
class TestDataSourceFactory:
    """Test cases for DataSourceFactory class"""

    @pytest.fixture
    def factory_config(self):
        """Configuration for data source factory"""
        return {
            "data_sources": {
                "baostock": {
                    "enabled": True,
                    "rate_limit": {"requests_per_second": 1}
                },
                "yfinance": {
                    "enabled": True,
                    "rate_limit": {"requests_per_second": 2}
                },
                "akshare": {
                    "enabled": False,
                    "rate_limit": {"requests_per_second": 3}
                }
            }
        }

    @pytest.fixture
    def source_factory(self, factory_config):
        """Create DataSourceFactory instance for testing"""
        with patch('data_sources.source_factory.config_manager.get', return_value=factory_config):
            return DataSourceFactory()

    def test_factory_initialization(self, source_factory):
        """Test factory initialization"""
        assert source_factory is not None
        assert hasattr(source_factory, 'get_source')
        assert hasattr(source_factory, 'get_all_sources')

    def test_get_baostock_source(self, source_factory):
        """Test getting BaoStock source"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_class:
            mock_instance = AsyncMock()
            mock_class.return_value = mock_instance

            source = source_factory.get_source('baostock')

            assert source == mock_instance
            mock_class.assert_called_once()

    def test_get_yfinance_source(self, source_factory):
        """Test getting YFinance source"""
        with patch('data_sources.source_factory.YFinanceSource') as mock_class:
            mock_instance = AsyncMock()
            mock_class.return_value = mock_instance

            source = source_factory.get_source('yfinance')

            assert source == mock_instance
            mock_class.assert_called_once()

    def test_get_akshare_source(self, source_factory):
        """Test getting AKShare source"""
        with patch('data_sources.source_factory.AkShareSource') as mock_class:
            mock_instance = AsyncMock()
            mock_class.return_value = mock_instance

            source = source_factory.get_source('akshare')

            assert source == mock_instance
            mock_class.assert_called_once()

    def test_get_nonexistent_source(self, source_factory):
        """Test getting non-existent source"""
        with pytest.raises(ConfigurationError):
            source_factory.get_source('nonexistent')

    def test_get_disabled_source(self, source_factory):
        """Test getting disabled source"""
        with pytest.raises(ConfigurationError):
            source_factory.get_source('akshare')  # Disabled in config

    def test_get_all_enabled_sources(self, source_factory):
        """Test getting all enabled sources"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_baostock, \
             patch('data_sources.source_factory.YFinanceSource') as mock_yfinance:

            mock_baostock_instance = AsyncMock()
            mock_yfinance_instance = AsyncMock()
            mock_baostock.return_value = mock_baostock_instance
            mock_yfinance.return_value = mock_yfinance_instance

            sources = source_factory.get_all_sources()

        assert len(sources) == 2
        assert 'baostock' in sources
        assert 'yfinance' in sources
        assert sources['baostock'] == mock_baostock_instance
        assert sources['yfinance'] == mock_yfinance_instance

    def test_get_source_by_priority(self, source_factory):
        """Test getting source by priority"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_baostock, \
             patch('data_sources.source_factory.YFinanceSource') as mock_yfinance:

            mock_baostock_instance = AsyncMock()
            mock_yfinance_instance = AsyncMock()
            mock_baostock.return_value = mock_baostock_instance
            mock_yfinance.return_value = mock_yfinance_instance

            # Test getting primary source (BaoStock)
            source = source_factory.get_primary_source()
            assert source == mock_baostock_instance

    def test_get_fallback_sources(self, source_factory):
        """Test getting fallback sources"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_baostock, \
             patch('data_sources.source_factory.YFinanceSource') as mock_yfinance:

            mock_baostock_instance = AsyncMock()
            mock_yfinance_instance = AsyncMock()
            mock_baostock.return_value = mock_baostock_instance
            mock_yfinance.return_value = mock_yfinance_instance

            fallback_sources = source_factory.get_fallback_sources()

        assert len(fallback_sources) == 1  # Only YFinance as fallback
        assert 'yfinance' in fallback_sources

    def test_source_priority_ordering(self, source_factory):
        """Test source priority ordering"""
        priorities = source_factory.get_source_priorities()

        assert 'baostock' in priorities
        assert 'yfinance' in priorities
        assert priorities.index('baostock') < priorities.index('yfinance')

    def test_is_source_enabled(self, source_factory):
        """Test checking if source is enabled"""
        assert source_factory.is_source_enabled('baostock') is True
        assert source_factory.is_source_enabled('yfinance') is True
        assert source_factory.is_source_enabled('akshare') is False
        assert source_factory.is_source_enabled('nonexistent') is False

    def test_get_source_config(self, source_factory):
        """Test getting source configuration"""
        config = source_factory.get_source_config('baostock')

        assert 'enabled' in config
        assert 'rate_limit' in config
        assert config['enabled'] is True

    def test_source_caching(self, source_factory):
        """Test source instance caching"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_class:
            mock_instance = AsyncMock()
            mock_class.return_value = mock_instance

            # Get source twice
            source1 = source_factory.get_source('baostock')
            source2 = source_factory.get_source('baostock')

            # Should return same instance (cached)
            assert source1 == source2
            # Should only create instance once
            mock_class.assert_called_once()

    def test_validate_source_config(self, source_factory):
        """Test source configuration validation"""
        # Test valid config
        valid_config = {
            "enabled": True,
            "rate_limit": {"requests_per_second": 1}
        }
        assert source_factory._validate_source_config('baostock', valid_config) is True

        # Test invalid config (missing required fields)
        invalid_config = {"enabled": True}
        assert source_factory._validate_source_config('baostock', invalid_config) is False

    def test_register_custom_source(self, source_factory):
        """Test registering custom data source"""
        # Create a mock custom source class
        custom_source_class = Mock()
        custom_source_instance = AsyncMock()
        custom_source_class.return_value = custom_source_instance

        # Register custom source
        source_factory.register_source('custom', custom_source_class, {
            "enabled": True,
            "rate_limit": {"requests_per_second": 5}
        })

        # Test getting custom source
        source = source_factory.get_source('custom')
        assert source == custom_source_instance

    def test_unregister_source(self, source_factory):
        """Test unregistering data source"""
        # First verify source exists
        assert source_factory.is_source_enabled('baostock') is True

        # Unregister source
        source_factory.unregister_source('baostock')

        # Source should no longer be available
        assert source_factory.is_source_enabled('baostock') is False

    def test_get_source_health_status(self, source_factory):
        """Test getting source health status"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_class:
            mock_instance = AsyncMock()
            mock_instance.health_check.return_value = True
            mock_class.return_value = mock_instance

            health_status = source_factory.get_source_health_status('baostock')

        assert health_status['source'] == 'baostock'
        assert health_status['healthy'] is True

    def test_get_all_sources_health_status(self, source_factory):
        """Test getting health status for all sources"""
        with patch('data_sources.source_factory.BaoStockSource') as mock_baostock, \
             patch('data_sources.source_factory.YFinanceSource') as mock_yfinance:

            mock_baostock_instance = AsyncMock()
            mock_yfinance_instance = AsyncMock()
            mock_baostock_instance.health_check.return_value = True
            mock_yfinance_instance.health_check.return_value = False
            mock_baostock.return_value = mock_baostock_instance
            mock_yfinance.return_value = mock_yfinance_instance

            health_status = source_factory.get_all_sources_health_status()

        assert len(health_status) == 2
        assert status['source'] == 'baostock' for status in health_status if 'baostock' in status.values()
        assert status['healthy'] is True for status in health_status if 'baostock' in status.values()