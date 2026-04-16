from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from data_sources.source_factory import DataSourceFactory
from utils.exceptions import ConfigurationError, ErrorCodes


def _build_source(name: str, supported_exchanges=None):
    source = Mock()
    source.name = name
    source.supported_exchanges = supported_exchanges
    source.instrument_types_supported = []
    source.get_daily_data = AsyncMock(return_value=[])
    return source


@pytest.mark.unit
class TestSourceFactoryRouting:
    def setup_method(self):
        self.factory = DataSourceFactory(Mock())
        self.factory.config = Mock()
        self.factory.config.get = Mock(return_value={})

        self.pytdx = _build_source('pytdx_a_stock', ['SSE', 'SZSE', 'BSE'])
        self.baostock = _build_source('baostock_a_stock', ['SSE', 'SZSE'])
        self.akshare = _build_source('akshare_a_stock', ['SSE', 'SZSE', 'BSE'])
        self.yfinance = _build_source('yfinance_hk_stock', ['HKEX', 'NASDAQ', 'NYSE'])
        self.yfinance.get_adjustment_factors = AsyncMock(return_value=[
            {
                'instrument_id': '00001.HK',
                'ex_date': datetime(2026, 4, 13),
                'factor': 1.02,
                'cumulative_factor': 1.15,
                'source': 'yfinance',
            }
        ])

        self.factory.sources = {
            'pytdx_a_stock': self.pytdx,
            'baostock_a_stock': self.baostock,
            'akshare_a_stock': self.akshare,
            'yfinance_hk_stock': self.yfinance,
        }
        self.factory.region_sources = {
            'a_stock': [self.pytdx, self.baostock, self.akshare],
            'hk_stock': [self.yfinance],
        }
        self.factory.source_instances_by_region = {
            'a_stock': {
                'pytdx': self.pytdx,
                'baostock': self.baostock,
                'akshare': self.akshare,
            },
            'hk_stock': {
                'yfinance': self.yfinance,
            },
        }
        self.factory.routing = {
            'daily': {
                'SSE': {
                    'stock': ['pytdx', 'baostock', 'akshare'],
                    'index': ['baostock', 'akshare'],
                },
            },
            'daily_behavior': {
                'default': {
                    'stock': {'skip_backup_on_empty_short_range': True},
                    'index': {'skip_backup_on_empty_short_range': False},
                },
            },
            'instrument_list': {'a_stock': ['baostock']},
            'calendar': {'a_stock': ['baostock']},
            'factor': {
                'SSE': {
                    'primary': 'baostock',
                    'validator': 'tdx_xdxr',
                    'fallback': 'akshare',
                },
                'HKEX': {
                    'primary': 'yfinance',
                    'validator': None,
                    'fallback': None,
                },
            },
        }
        self.factory._init_factor_routes()

    def test_get_daily_source_chain_uses_configured_index_route(self):
        chain = self.factory._get_daily_source_chain('SSE', 'index')

        assert [source.name for source in chain] == [
            'baostock_a_stock',
            'akshare_a_stock',
        ]

    def test_missing_daily_route_raises_configuration_error(self):
        with pytest.raises(ConfigurationError) as exc_info:
            self.factory._get_daily_source_chain('HKEX', 'stock')

        assert exc_info.value.error_code == ErrorCodes.CONFIG_MISSING_KEY

    def test_validate_routing_ignores_disabled_regions(self):
        self.factory.config.get = Mock(return_value={
            'a_stock': {'enabled': True},
            'us_stock': {'enabled': False},
        })
        self.factory.routing['instrument_list']['us_stock'] = ['akshare']
        self.factory.routing['calendar']['us_stock'] = ['akshare']
        self.factory.routing['factor']['NASDAQ'] = {
            'primary': 'yfinance',
            'validator': None,
            'fallback': None,
        }

        self.factory._validate_routing_config()

    @pytest.mark.asyncio
    async def test_get_adjustment_factors_uses_hkex_primary_route(self):
        factors = await self.factory.get_adjustment_factors(
            'HKEX',
            '00001.HK',
            '00001',
            datetime(2026, 4, 1),
            datetime(2026, 4, 13),
        )

        assert factors
        assert factors[0]['source'] == 'yfinance'
        self.yfinance.get_adjustment_factors.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_adjustment_factors_falls_back_when_primary_returns_none(self):
        self.factory.routing['factor']['HKEX'] = {
            'primary': 'akshare',
            'validator': None,
            'fallback': 'yfinance',
        }
        self.factory.source_instances_by_region['hk_stock']['akshare'] = self.akshare
        self.akshare.supported_exchanges = ['HKEX']
        self.akshare.get_adjustment_factors = AsyncMock(return_value=None)
        self.factory._init_factor_routes()

        factors = await self.factory.get_adjustment_factors(
            'HKEX',
            '00001.HK',
            '00001',
            datetime(2026, 4, 1),
            datetime(2026, 4, 13),
        )

        assert factors
        assert factors[0]['source'] == 'yfinance'
        self.akshare.get_adjustment_factors.assert_awaited_once()
        self.yfinance.get_adjustment_factors.assert_awaited()

    @pytest.mark.asyncio
    async def test_get_adjustment_factors_does_not_fall_back_on_empty_list(self):
        self.factory.routing['factor']['HKEX'] = {
            'primary': 'akshare',
            'validator': None,
            'fallback': 'yfinance',
        }
        self.factory.source_instances_by_region['hk_stock']['akshare'] = self.akshare
        self.akshare.supported_exchanges = ['HKEX']
        self.akshare.get_adjustment_factors = AsyncMock(return_value=[])
        self.yfinance.get_adjustment_factors = AsyncMock(return_value=[{
            'instrument_id': '00001.HK',
            'ex_date': datetime(2026, 4, 13),
            'factor': 1.02,
            'cumulative_factor': 1.15,
            'source': 'yfinance',
        }])
        self.factory._init_factor_routes()

        factors = await self.factory.get_adjustment_factors(
            'HKEX',
            '00001.HK',
            '00001',
            datetime(2026, 4, 1),
            datetime(2026, 4, 13),
        )

        assert factors == []
        self.akshare.get_adjustment_factors.assert_awaited_once()
        self.yfinance.get_adjustment_factors.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_daily_data_allows_backup_for_index_short_range(self):
        start_date = datetime(2026, 4, 12)
        end_date = datetime(2026, 4, 13)
        expected = [{'instrument_id': '000300.SH', 'time': end_date}]

        self.baostock.get_daily_data.return_value = []
        self.pytdx.get_daily_data.return_value = []
        self.akshare.get_daily_data.return_value = expected
        self.factory._validate_daily_data = Mock(return_value=True)

        result = await self.factory.get_daily_data(
            'SSE',
            '000300.SH',
            '000300',
            start_date,
            end_date,
            instrument_type='index',
        )

        assert result == expected
        self.akshare.get_daily_data.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_daily_data_stock_short_range_still_skips_backup(self):
        start_date = datetime(2026, 4, 12)
        end_date = datetime(2026, 4, 13)

        self.pytdx.get_daily_data.return_value = []
        self.factory._validate_daily_data = Mock(return_value=True)

        result = await self.factory.get_daily_data(
            'SSE',
            '600000.SH',
            '600000',
            start_date,
            end_date,
            instrument_type='stock',
        )

        assert result == []
        self.baostock.get_daily_data.assert_not_awaited()
        self.akshare.get_daily_data.assert_not_awaited()

    def test_get_daily_route_config_merges_default_and_exchange_override(self):
        self.factory.routing['daily_behavior']['SSE'] = {
            'stock': {'skip_backup_on_empty_short_range': False},
        }
        cfg = self.factory._get_daily_route_config('SSE', 'stock')

        assert cfg['skip_backup_on_empty_short_range'] is False
