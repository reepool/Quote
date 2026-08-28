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
        db_ops = Mock()
        db_ops.get_trading_days = AsyncMock(return_value=[datetime(2026, 6, 16).date()])
        self.factory = DataSourceFactory(db_ops)
        self.factory.config = Mock()
        self.factory.config.get = Mock(return_value={})

        self.pytdx = _build_source('pytdx_a_stock', ['SSE', 'SZSE', 'BSE'])
        self.baostock = _build_source('baostock_a_stock', ['SSE', 'SZSE'])
        self.akshare = _build_source('akshare_a_stock', ['SSE', 'SZSE', 'BSE'])
        self.cnindex = _build_source('cnindex_a_stock', ['SSE', 'SZSE'])
        self.cnindex.instrument_types_supported = ['index']
        self.csindex = _build_source('csindex_a_stock', ['SSE', 'SZSE'])
        self.csindex.instrument_types_supported = ['index']
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
            'cnindex_a_stock': self.cnindex,
            'csindex_a_stock': self.csindex,
            'yfinance_hk_stock': self.yfinance,
        }
        self.factory.region_sources = {
            'a_stock': [self.pytdx, self.baostock, self.akshare, self.cnindex, self.csindex],
            'hk_stock': [self.yfinance],
        }
        self.factory.source_instances_by_region = {
            'a_stock': {
                'pytdx': self.pytdx,
                'baostock': self.baostock,
                'akshare': self.akshare,
                'cnindex': self.cnindex,
                'csindex': self.csindex,
            },
            'hk_stock': {
                'yfinance': self.yfinance,
            },
        }
        self.factory.routing = {
            'daily': {
                'SSE': {
                    'stock': ['pytdx', 'baostock', 'akshare'],
                    'index': ['csindex', 'baostock', 'akshare'],
                },
                'SZSE': {
                    'stock': ['pytdx', 'baostock', 'akshare'],
                    'index': ['cnindex', 'baostock', 'akshare'],
                },
            },
            'daily_behavior': {
                'default': {
                    'stock': {'skip_backup_on_empty_short_range': True},
                    'index': {
                        'skip_backup_on_empty_short_range': False,
                        'require_end_date_coverage': True,
                    },
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
            'csindex_a_stock',
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

    def test_validate_routing_allows_unavailable_source_when_backup_exists(self):
        self.factory.source_instances_by_region['a_stock'].pop('baostock')
        self.factory.routing['instrument_list']['a_stock'] = ['baostock', 'akshare']
        self.factory.routing['calendar']['a_stock'] = ['baostock', 'akshare']

        self.factory._validate_routing_config()

        stock_chain = self.factory._get_daily_source_chain('SSE', 'stock')
        index_chain = self.factory._get_daily_source_chain('SSE', 'index')

        assert [source.name for source in stock_chain] == [
            'pytdx_a_stock',
            'akshare_a_stock',
        ]
        assert [source.name for source in index_chain] == [
            'csindex_a_stock',
            'akshare_a_stock',
        ]

    def test_index_daily_route_preserves_official_first_and_fallback_sources(self):
        chain = self.factory._get_daily_source_chain('SSE', 'index')

        assert [source.name for source in chain] == [
            'csindex_a_stock',
            'baostock_a_stock',
            'akshare_a_stock',
        ]

    def test_szse_index_route_uses_cnindex_without_csindex_empty_probe(self):
        chain = self.factory._get_daily_source_chain('SZSE', 'index')

        assert [source.name for source in chain] == [
            'cnindex_a_stock',
            'baostock_a_stock',
            'akshare_a_stock',
        ]

    def test_factor_route_promotes_fallback_when_primary_is_unavailable(self):
        self.factory.source_instances_by_region['a_stock'].pop('baostock')

        self.factory._init_factor_routes()

        route = self.factory.factor_routes['SSE']
        assert route['primary_instance'] is self.akshare
        assert route['fallback_instance'] is None

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
    async def test_index_daily_data_falls_back_when_primary_is_stale(self):
        self.csindex.get_daily_data = AsyncMock(return_value=[
            {
                'instrument_id': '000300.SH',
                'time': datetime(2026, 6, 15),
                'open': 1.0,
                'high': 1.1,
                'low': 0.9,
                'close': 1.0,
            }
        ])
        self.baostock.get_daily_data = AsyncMock(return_value=[
            {
                'instrument_id': '000300.SH',
                'time': datetime(2026, 6, 16),
                'open': 2.0,
                'high': 2.1,
                'low': 1.9,
                'close': 2.0,
            }
        ])

        rows = await self.factory.get_daily_data(
            'SSE',
            '000300.SH',
            '000300',
            datetime(2026, 6, 15),
            datetime(2026, 6, 16),
            instrument_type='index',
        )

        assert rows[0]['close'] == 2.0
        self.csindex.get_daily_data.assert_awaited_once()
        self.baostock.get_daily_data.assert_awaited_once()
        assert self.factory.db_ops.get_trading_days.await_count == 1

    @pytest.mark.asyncio
    async def test_index_daily_source_stale_breaker_skips_source_after_threshold(self):
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 6, 17).date(), datetime(2026, 6, 18).date()]
        )

        async def stale_cnindex(instrument_id, *_args, **_kwargs):
            return [
                {
                    'instrument_id': instrument_id,
                    'time': datetime(2026, 6, 17),
                    'open': 1.0,
                    'high': 1.0,
                    'low': 1.0,
                    'close': 1.0,
                }
            ]

        async def fresh_akshare(instrument_id, *_args, **_kwargs):
            return [
                {
                    'instrument_id': instrument_id,
                    'time': datetime(2026, 6, 18),
                    'open': 2.0,
                    'high': 2.0,
                    'low': 2.0,
                    'close': 2.0,
                }
            ]

        self.cnindex.get_daily_data = AsyncMock(side_effect=stale_cnindex)
        self.baostock.get_daily_data = AsyncMock(return_value=[])
        self.akshare.get_daily_data = AsyncMock(side_effect=fresh_akshare)

        for symbol in ['399282', '399283', '399284', '399285']:
            rows = await self.factory.get_daily_data(
                'SZSE',
                f'{symbol}.SZ',
                symbol,
                datetime(2026, 6, 17),
                datetime(2026, 6, 18),
                instrument_type='index',
            )
            assert rows[0]['close'] == 2.0

        assert self.cnindex.get_daily_data.await_count == 3
        assert self.akshare.get_daily_data.await_count == 4
        breaker_key = ('SZSE', 'index', 'cnindex_a_stock', datetime(2026, 6, 18).date())
        assert breaker_key in self.factory.daily_stale_source_breakers

    @pytest.mark.asyncio
    async def test_index_daily_data_uses_last_trading_day_for_coverage(self):
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 6, 15).date(), datetime(2026, 6, 16).date()]
        )
        self.csindex.get_daily_data = AsyncMock(return_value=[
            {
                'instrument_id': '000300.SH',
                'time': datetime(2026, 6, 16),
                'open': 1.0,
                'high': 1.1,
                'low': 0.9,
                'close': 1.0,
            }
        ])

        rows = await self.factory.get_daily_data(
            'SSE',
            '000300.SH',
            '000300',
            datetime(2026, 6, 15),
            datetime(2026, 6, 17),
            instrument_type='index',
        )

        assert rows[0]['close'] == 1.0
        self.csindex.get_daily_data.assert_awaited_once()
        self.baostock.get_daily_data.assert_not_awaited()

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

        self.factory.db_ops.get_trading_days = AsyncMock(return_value=[end_date.date()])
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

    @pytest.mark.asyncio
    async def test_hkex_yfinance_zero_volume_is_not_coverage(self):
        expected_date = datetime(2026, 8, 26).date()
        self.factory.db_ops.get_trading_days = AsyncMock(return_value=[expected_date])
        self.factory.routing['daily']['HKEX'] = {
            'stock': ['akshare', 'yfinance'],
        }
        self.factory.routing['daily_behavior']['default']['stock'] = {
            'skip_backup_on_empty_short_range': False,
            'require_end_date_coverage': True,
        }
        akshare_hk = _build_source('akshare_hk_stock', ['HKEX'])
        self.factory.sources['akshare_hk_stock'] = akshare_hk
        self.factory.source_instances_by_region['hk_stock']['akshare'] = akshare_hk
        self.factory._validate_daily_data = Mock(return_value=True)
        akshare_hk.get_daily_data.return_value = []
        self.yfinance.get_daily_data.return_value = [
            {
                'instrument_id': '01712.HK',
                'time': datetime(2026, 8, 26),
                'open': 1.0,
                'high': 1.0,
                'low': 1.0,
                'close': 1.0,
                'volume': None,
            }
        ]

        result = await self.factory.get_daily_data(
            'HKEX',
            '01712.HK',
            '01712',
            datetime(2026, 8, 25),
            datetime(2026, 8, 26),
            instrument_type='stock',
        )

        assert result == []
        self.yfinance.get_daily_data.assert_awaited_once()

    def _set_index_breaker_behavior(
        self,
        *,
        stale_threshold: int = 3,
        stale_probe_every: int = 0,
        transport_threshold: int = 3,
        transport_probe_every: int = 0,
    ) -> None:
        self.factory.routing['daily_behavior']['default']['index'] = {
            'skip_backup_on_empty_short_range': False,
            'require_end_date_coverage': True,
            'stale_source_circuit_breaker_threshold': stale_threshold,
            'stale_source_circuit_breaker_probe_every': stale_probe_every,
            'transport_error_circuit_breaker_threshold': transport_threshold,
            'transport_error_circuit_breaker_probe_every': transport_probe_every,
        }

    @staticmethod
    def _index_quote(instrument_id: str, quote_date: datetime, close: float) -> dict:
        return {
            'instrument_id': instrument_id,
            'time': quote_date,
            'open': close,
            'high': close,
            'low': close,
            'close': close,
        }

    def test_http_throttle_error_classifier(self):
        assert self.factory._is_daily_http_throttle_error(
            Exception('HTTP Error 403: Forbidden')
        )
        err = Exception('boom')
        err.code = 429
        assert self.factory._is_daily_http_throttle_error(err)
        assert not self.factory._is_daily_http_throttle_error(Exception('timeout'))

    @pytest.mark.asyncio
    async def test_index_daily_source_skips_after_http_403_threshold(self):
        self._set_index_breaker_behavior(transport_threshold=3, transport_probe_every=0)
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 8, 27).date()]
        )
        self.csindex.get_daily_data = AsyncMock(
            side_effect=Exception('HTTP Error 403: Forbidden')
        )
        self.baostock.get_daily_data = AsyncMock(
            side_effect=lambda instrument_id, *_args, **_kwargs: [
                self._index_quote(instrument_id, datetime(2026, 8, 27), 2.0)
            ]
        )

        for symbol in ['000842', '000939', '000940', '000941']:
            rows = await self.factory.get_daily_data(
                'SSE',
                f'{symbol}.SH',
                symbol,
                datetime(2026, 8, 27),
                datetime(2026, 8, 27),
                instrument_type='index',
            )
            assert rows[0]['close'] == 2.0

        assert self.csindex.get_daily_data.await_count == 3
        assert self.baostock.get_daily_data.await_count == 4
        breaker_key = ('SSE', 'index', 'csindex_a_stock', datetime(2026, 8, 27).date())
        assert breaker_key in self.factory.daily_transport_error_breakers
        assert self.factory.last_daily_data_diagnostic['skipped_sources'] == [
            'csindex_a_stock'
        ]

    @pytest.mark.asyncio
    async def test_official_only_retry_ignores_open_http_403_breaker(self):
        self._set_index_breaker_behavior(transport_threshold=3, transport_probe_every=0)
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 8, 28).date()]
        )
        self.csindex.get_daily_data = AsyncMock(
            side_effect=[
                Exception('HTTP Error 403: Forbidden'),
                Exception('HTTP Error 403: Forbidden'),
                Exception('HTTP Error 403: Forbidden'),
                [self._index_quote('000842.SH', datetime(2026, 8, 28), 3.0)],
            ]
        )
        self.baostock.get_daily_data = AsyncMock(
            side_effect=lambda instrument_id, *_args, **_kwargs: [
                self._index_quote(instrument_id, datetime(2026, 8, 28), 2.0)
            ]
        )

        for symbol in ['000841', '000843', '000844']:
            rows = await self.factory.get_daily_data(
                'SSE',
                f'{symbol}.SH',
                symbol,
                datetime(2026, 8, 28),
                datetime(2026, 8, 28),
                instrument_type='index',
            )
            assert rows[0]['close'] == 2.0

        baostock_calls = self.baostock.get_daily_data.await_count
        assert self.csindex.get_daily_data.await_count == 3

        rows = await self.factory.get_daily_data(
            'SSE',
            '000842.SH',
            '000842',
            datetime(2026, 8, 28),
            datetime(2026, 8, 28),
            instrument_type='index',
            official_source_only=True,
            ignore_coverage_breaker=True,
        )

        assert rows[0]['close'] == 3.0
        assert self.csindex.get_daily_data.await_count == 4
        assert self.baostock.get_daily_data.await_count == baostock_calls
        assert self.factory.last_daily_data_diagnostic['official_source_only'] is True
        assert self.factory.last_daily_data_diagnostic['ignore_coverage_breaker'] is True
        assert self.factory.last_daily_data_diagnostic['skipped_sources'] == []

    @pytest.mark.asyncio
    async def test_empty_index_result_does_not_open_http_403_breaker(self):
        self._set_index_breaker_behavior(transport_threshold=3, transport_probe_every=0)
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 8, 27).date()]
        )
        self.csindex.get_daily_data = AsyncMock(return_value=[])
        self.baostock.get_daily_data = AsyncMock(
            side_effect=lambda instrument_id, *_args, **_kwargs: [
                self._index_quote(instrument_id, datetime(2026, 8, 27), 2.0)
            ]
        )

        for symbol in ['000842', '000939', '000940']:
            await self.factory.get_daily_data(
                'SSE',
                f'{symbol}.SH',
                symbol,
                datetime(2026, 8, 27),
                datetime(2026, 8, 27),
                instrument_type='index',
            )

        assert self.csindex.get_daily_data.await_count == 3
        assert not self.factory.daily_transport_error_breakers

    @pytest.mark.asyncio
    async def test_stale_index_breaker_recovers_on_half_open_probe(self):
        self._set_index_breaker_behavior(stale_threshold=2, stale_probe_every=2)
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 8, 26).date(), datetime(2026, 8, 27).date()]
        )
        stale_calls = {'n': 0}

        async def cnindex_impl(instrument_id, *_args, **_kwargs):
            stale_calls['n'] += 1
            quote_date = (
                datetime(2026, 8, 26)
                if stale_calls['n'] <= 2
                else datetime(2026, 8, 27)
            )
            return [self._index_quote(instrument_id, quote_date, 3.0 if stale_calls['n'] > 2 else 1.0)]

        self.cnindex.get_daily_data = AsyncMock(side_effect=cnindex_impl)
        self.baostock.get_daily_data = AsyncMock(return_value=[])
        self.akshare.get_daily_data = AsyncMock(
            side_effect=lambda instrument_id, *_args, **_kwargs: [
                self._index_quote(instrument_id, datetime(2026, 8, 27), 2.0)
            ]
        )

        closes = []
        for symbol in ['399001', '399002', '399003', '399004', '399005']:
            rows = await self.factory.get_daily_data(
                'SZSE',
                f'{symbol}.SZ',
                symbol,
                datetime(2026, 8, 26),
                datetime(2026, 8, 27),
                instrument_type='index',
            )
            closes.append(rows[0]['close'])

        assert closes == [2.0, 2.0, 2.0, 3.0, 3.0]
        assert self.cnindex.get_daily_data.await_count == 4
        breaker_key = ('SZSE', 'index', 'cnindex_a_stock', datetime(2026, 8, 27).date())
        assert breaker_key not in self.factory.daily_stale_source_breakers

    @pytest.mark.asyncio
    async def test_http_403_half_open_probe_keeps_breaker_when_still_forbidden(self):
        self._set_index_breaker_behavior(
            transport_threshold=2,
            transport_probe_every=2,
        )
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 8, 27).date()]
        )
        self.csindex.get_daily_data = AsyncMock(
            side_effect=Exception('HTTP Error 403: Forbidden')
        )
        self.baostock.get_daily_data = AsyncMock(
            side_effect=lambda instrument_id, *_args, **_kwargs: [
                self._index_quote(instrument_id, datetime(2026, 8, 27), 2.0)
            ]
        )

        for symbol in ['000842', '000939', '000940', '000941']:
            await self.factory.get_daily_data(
                'SSE',
                f'{symbol}.SH',
                symbol,
                datetime(2026, 8, 27),
                datetime(2026, 8, 27),
                instrument_type='index',
            )

        assert self.csindex.get_daily_data.await_count == 3
        breaker_key = ('SSE', 'index', 'csindex_a_stock', datetime(2026, 8, 27).date())
        assert breaker_key in self.factory.daily_transport_error_breakers
        assert self.factory.last_daily_data_diagnostic.get('probed_sources') == [
            'csindex_a_stock'
        ]

    @pytest.mark.asyncio
    async def test_http_403_half_open_probe_recovers_when_source_covers_t_day(self):
        self._set_index_breaker_behavior(
            transport_threshold=2,
            transport_probe_every=2,
        )
        self.factory.db_ops.get_trading_days = AsyncMock(
            return_value=[datetime(2026, 8, 27).date()]
        )
        calls = {'n': 0}

        async def csindex_impl(instrument_id, *_args, **_kwargs):
            calls['n'] += 1
            if calls['n'] <= 2:
                raise Exception('HTTP Error 403: Forbidden')
            return [self._index_quote(instrument_id, datetime(2026, 8, 27), 3.0)]

        self.csindex.get_daily_data = AsyncMock(side_effect=csindex_impl)
        self.baostock.get_daily_data = AsyncMock(
            side_effect=lambda instrument_id, *_args, **_kwargs: [
                self._index_quote(instrument_id, datetime(2026, 8, 27), 2.0)
            ]
        )

        closes = []
        for symbol in ['000842', '000939', '000940', '000941', '000942']:
            rows = await self.factory.get_daily_data(
                'SSE',
                f'{symbol}.SH',
                symbol,
                datetime(2026, 8, 27),
                datetime(2026, 8, 27),
                instrument_type='index',
            )
            closes.append(rows[0]['close'])

        assert closes == [2.0, 2.0, 2.0, 3.0, 3.0]
        assert self.csindex.get_daily_data.await_count == 4
        breaker_key = ('SSE', 'index', 'csindex_a_stock', datetime(2026, 8, 27).date())
        assert breaker_key not in self.factory.daily_transport_error_breakers
