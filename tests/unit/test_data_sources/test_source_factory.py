from unittest.mock import AsyncMock, Mock

import pytest

from data_sources.source_factory import DataSourceFactory


def _build_source(name: str, supported_exchanges=None, instrument_types=None):
    source = Mock()
    source.name = name
    source.supported_exchanges = supported_exchanges or []
    source.instrument_types_supported = instrument_types or []
    source.close = AsyncMock()
    return source


@pytest.mark.unit
class TestDataSourceFactory:
    def setup_method(self):
        self.factory = DataSourceFactory(Mock())

        self.pytdx = _build_source('pytdx_a_stock', ['SSE', 'SZSE', 'BSE'], ['stock'])
        self.baostock = _build_source('baostock_a_stock', ['SSE', 'SZSE'], ['stock', 'index'])
        self.akshare = _build_source('akshare_a_stock', ['SSE', 'SZSE', 'BSE'], ['stock', 'index'])

        self.factory.sources = {
            'pytdx_a_stock': self.pytdx,
            'baostock_a_stock': self.baostock,
            'akshare_a_stock': self.akshare,
        }
        self.factory.region_sources = {
            'a_stock': [self.pytdx, self.baostock, self.akshare],
        }
        self.factory.source_instances_by_region = {
            'a_stock': {
                'pytdx': self.pytdx,
                'baostock': self.baostock,
                'akshare': self.akshare,
            }
        }
        self.factory.routing = {
            'daily': {
                'SSE': {
                    'stock': ['pytdx', 'baostock', 'akshare'],
                    'index': ['baostock', 'akshare'],
                }
            },
            'daily_behavior': {
                'default': {
                    'stock': {'skip_backup_on_empty_short_range': True},
                    'index': {'skip_backup_on_empty_short_range': False},
                }
            },
            'instrument_list': {'a_stock': ['baostock']},
            'calendar': {'a_stock': ['baostock']},
            'factor': {
                'SSE': {
                    'primary': 'baostock',
                    'validator': 'tdx_xdxr',
                    'fallback': 'akshare',
                }
            },
        }

    def test_get_source_returns_primary_daily_source(self):
        source = self.factory.get_source('SSE')

        assert source is self.pytdx

    def test_get_primary_and_backup_sources_follow_routing(self):
        primary = self.factory.get_primary_source('SSE', instrument_type='index')
        backups = self.factory.get_backup_sources('SSE', instrument_type='index')

        assert primary is self.baostock
        assert backups == [self.akshare]
        assert self.factory.get_backup_source('SSE', instrument_type='index') is self.akshare

    def test_get_source_instance_resolves_by_exchange_and_region(self):
        assert self.factory._get_source_instance('baostock', exchange='SSE') is self.baostock
        assert self.factory._get_source_instance('baostock', region='a_stock') is self.baostock
        assert self.factory._get_source_instance('nonexistent', exchange='SSE') is None

    @pytest.mark.asyncio
    async def test_close_all_clears_cached_state(self):
        await self.factory.close_all()

        self.pytdx.close.assert_awaited_once()
        self.baostock.close.assert_awaited_once()
        self.akshare.close.assert_awaited_once()
        assert self.factory.sources == {}
        assert self.factory.region_sources == {}
        assert self.factory.source_instances_by_region == {}
        assert self.factory.factor_routes == {}
        assert self.factory.routing == {}
