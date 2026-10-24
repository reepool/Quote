"""Suspension-exemption tests for A-share daily update classification.

Covers the `classify-suspended-stock-empty-quotes-legitimate` change:
- `DataManager._is_latest_quote_suspended` evidence helper (fail-closed),
- empty-result classification through `update_daily_data`,
- the stock watermark block predicate,
- the operator-report integrity line.
"""

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from utils.report.engine import ReportEngine

TARGET_DATE = date(2026, 10, 23)
LATEST_QUOTE = datetime(2026, 10, 22)


def _stub_data_manager(db_ops, data_config=None):
    """Build a DataManager with only the attributes update_daily_data needs."""

    dm = DataManager.__new__(DataManager)
    dm.db_ops = db_ops
    dm.data_config = data_config or {
        'instrument_types': ['stock', 'index'],
        'daily_update_catchup': {},
        'daily_update_unresolved_retry': {
            'enabled': True,
            'exchanges': ['SZSE'],
            'max_instruments': 10,
            'sources': ['primary'],
        },
    }
    dm.progress = SimpleNamespace(batch_id='test-batch')
    dm.source_factory = Mock()
    dm.source_factory.get_daily_data = AsyncMock(return_value=[])
    dm.source_factory.last_daily_data_diagnostic = {}
    return dm


def _instrument(instrument_id='000016.SZ', trading_status=1):
    return {
        'instrument_id': instrument_id,
        'symbol': instrument_id.split('.')[0],
        'type': 'stock',
        'exchange': 'SZSE',
        'trading_status': trading_status,
        'listed_date': date(1992, 3, 27),
        'source_symbol': instrument_id.split('.')[0],
    }


def _quote_row(tradestatus):
    return {
        'time': LATEST_QUOTE,
        'instrument_id': '000016.SZ',
        'tradestatus': tradestatus,
        'volume': 0,
    }


def _db_ops(get_daily_quote_return=None, instruments=None):
    db_ops = AsyncMock()
    db_ops.get_active_instruments = AsyncMock(
        return_value=instruments if instruments is not None else [_instrument()]
    )
    db_ops.get_latest_quote_date = AsyncMock(return_value=LATEST_QUOTE)
    db_ops.get_daily_quote = AsyncMock(return_value=get_daily_quote_return)
    db_ops.get_previous_trading_day = AsyncMock(return_value=date(2026, 10, 22))
    return db_ops


def _patch_dm_collaborators(monkeypatch, dm):
    """Patch everything around the classification branch."""

    monkeypatch.setattr(
        dm,
        '_maybe_sync_instrument_master_before_daily_update',
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        dm,
        '_batch_sync_adjustment_factors',
        AsyncMock(
            return_value={'status': 'success', 'synced': 0, 'failed': 0}
        ),
    )
    monkeypatch.setattr(
        dm,
        '_record_a_share_quote_composite_watermark',
        AsyncMock(return_value={'status': 'partial'}),
    )
    monkeypatch.setattr(
        dm,
        '_generate_daily_update_report',
        AsyncMock(return_value={'name': 'test'}),
    )
    retry_mock = AsyncMock(return_value={})
    monkeypatch.setattr(
        dm, '_retry_empty_unresolved_official_daily', retry_mock
    )
    return retry_mock


@pytest.mark.unit
class TestIsLatestQuoteSuspended:
    """3.1 Evidence helper contract (fail-closed)."""

    def _call(self, db_ops, latest_quote_date):
        stub = SimpleNamespace(db_ops=db_ops)
        return DataManager._is_latest_quote_suspended(
            stub, '000016.SZ', latest_quote_date
        )

    @pytest.mark.asyncio
    async def test_suspended_row_returns_true(self):
        db_ops = AsyncMock()
        db_ops.get_daily_quote = AsyncMock(return_value=_quote_row(0))
        assert await self._call(db_ops, LATEST_QUOTE) is True

    @pytest.mark.asyncio
    async def test_trading_row_returns_false(self):
        db_ops = AsyncMock()
        db_ops.get_daily_quote = AsyncMock(return_value=_quote_row(1))
        assert await self._call(db_ops, LATEST_QUOTE) is False

    @pytest.mark.asyncio
    async def test_missing_date_returns_false_without_lookup(self):
        db_ops = AsyncMock()
        db_ops.get_daily_quote = AsyncMock(return_value=_quote_row(0))
        assert await self._call(db_ops, None) is False
        db_ops.get_daily_quote.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_row_returns_false(self):
        db_ops = AsyncMock()
        db_ops.get_daily_quote = AsyncMock(return_value=None)
        assert await self._call(db_ops, LATEST_QUOTE) is False

    @pytest.mark.asyncio
    async def test_lookup_error_returns_false(self):
        db_ops = AsyncMock()
        db_ops.get_daily_quote = AsyncMock(side_effect=RuntimeError('db down'))
        assert await self._call(db_ops, LATEST_QUOTE) is False

    @pytest.mark.asyncio
    async def test_non_integer_tradestatus_returns_false(self):
        db_ops = AsyncMock()
        db_ops.get_daily_quote = AsyncMock(
            return_value={'tradestatus': '0'}
        )
        assert await self._call(db_ops, LATEST_QUOTE) is False

    @pytest.mark.asyncio
    async def test_getter_unavailable_returns_false(self):
        db_ops = SimpleNamespace()  # no get_daily_quote attribute
        assert await self._call(db_ops, LATEST_QUOTE) is False

    @pytest.mark.asyncio
    async def test_non_async_getter_returns_false(self):
        db_ops = SimpleNamespace(
            get_daily_quote=lambda instrument_id, quote_date: _quote_row(0)
        )
        assert await self._call(db_ops, LATEST_QUOTE) is False


@pytest.mark.unit
class TestEmptyResultClassification:
    """3.2 Classification branch through update_daily_data."""

    def _instruments(self):
        return [_instrument()]

    @pytest.mark.asyncio
    async def test_suspended_empty_result_is_legitimate(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        db_ops = _db_ops(get_daily_quote_return=_quote_row(0))
        dm = _stub_data_manager(db_ops)
        retry_mock = _patch_dm_collaborators(monkeypatch, dm)

        result = await dm.update_daily_data(
            exchanges=['SZSE'],
            target_date=TARGET_DATE,
            run_factor_audit=False,
        )

        assert result['integrity_stats']['legitimate_no_quote'] == 1
        assert result['integrity_stats']['empty_unresolved'] == 0
        szse = result['exchange_stats']['SZSE']
        assert szse['stock_failure_count'] == 0
        assert szse['failure_count'] == 0
        retry_mock.assert_not_called()
        # Exempted instruments stay out of the shared anomaly samples.
        assert result['integrity_stats']['samples'] == []

    @pytest.mark.asyncio
    async def test_trading_empty_result_still_fails(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        db_ops = _db_ops(get_daily_quote_return=_quote_row(1))
        dm = _stub_data_manager(db_ops)
        retry_mock = _patch_dm_collaborators(monkeypatch, dm)

        result = await dm.update_daily_data(
            exchanges=['SZSE'],
            target_date=TARGET_DATE,
            run_factor_audit=False,
        )

        assert result['integrity_stats']['empty_unresolved'] == 1
        assert result['integrity_stats']['legitimate_no_quote'] == 0
        szse = result['exchange_stats']['SZSE']
        assert szse['stock_failure_count'] == 1
        retry_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_quality_rejected_rows_prevent_exemption(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        db_ops = _db_ops(get_daily_quote_return=_quote_row(0))
        dm = _stub_data_manager(db_ops)
        _patch_dm_collaborators(monkeypatch, dm)
        # One invalid row (high < low): empty after validation, but rejected.
        dm.source_factory.get_daily_data = AsyncMock(
            return_value=[
                {
                    'time': TARGET_DATE,
                    'open': 10.0,
                    'high': 9.0,
                    'low': 8.0,
                    'close': 8.5,
                    'volume': 100,
                }
            ]
        )

        result = await dm.update_daily_data(
            exchanges=['SZSE'],
            target_date=TARGET_DATE,
            run_factor_audit=False,
        )

        assert result['integrity_stats']['empty_unresolved'] == 1
        assert result['integrity_stats']['legitimate_no_quote'] == 0
        szse = result['exchange_stats']['SZSE']
        assert szse['stock_failure_count'] == 1

    @pytest.mark.asyncio
    async def test_non_a_share_exchange_is_unchanged(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        instrument = _instrument()
        instrument['exchange'] = 'HKEX'
        db_ops = _db_ops(get_daily_quote_return=_quote_row(0), instruments=[instrument])
        dm = _stub_data_manager(db_ops)
        retry_mock = _patch_dm_collaborators(monkeypatch, dm)

        result = await dm.update_daily_data(
            exchanges=['HKEX'],
            target_date=TARGET_DATE,
            run_factor_audit=False,
        )

        assert result['integrity_stats']['empty_unresolved'] == 1
        assert result['integrity_stats']['legitimate_no_quote'] == 0
        retry_mock.assert_not_called()  # retry config only lists SZSE


@pytest.mark.unit
class TestStockWatermarkPredicate:
    """3.3 Only stock failures block the exchange readiness watermark."""

    def test_clean_state_does_not_block(self):
        assert (
            DataManager._quote_exchange_blocks_stock_watermark(
                {
                    'stock_failure_count': 0,
                    'failure_count': 0,
                    'changelog_stats': {'failed': 0},
                }
            )
            is False
        )

    def test_index_only_failures_do_not_block(self):
        assert (
            DataManager._quote_exchange_blocks_stock_watermark(
                {
                    'stock_failure_count': 0,
                    'failure_count': 5,
                    'changelog_stats': {'failed': 0},
                }
            )
            is False
        )

    def test_stock_failure_blocks(self):
        assert (
            DataManager._quote_exchange_blocks_stock_watermark(
                {
                    'stock_failure_count': 1,
                    'failure_count': 1,
                    'changelog_stats': {'failed': 0},
                }
            )
            is True
        )

    def test_write_failure_blocks(self):
        assert (
            DataManager._quote_exchange_blocks_stock_watermark(
                {
                    'stock_failure_count': 0,
                    'failure_count': 0,
                    'changelog_stats': {'failed': 2},
                }
            )
            is True
        )

    def test_run_error_blocks(self):
        assert (
            DataManager._quote_exchange_blocks_stock_watermark(
                {'error': 'exchange update failed'}
            )
            is True
        )


@pytest.mark.unit
class TestIntegrityReportLine:
    """3.4 Operator report shows the suspension exemption count."""

    def test_exempt_only_run_renders_section(self):
        text = ReportEngine()._format_daily_integrity_summary(
            {'legitimate_no_quote': 1}
        )
        assert text is not None
        assert '停牌无行情: 1' in text

    def test_zero_exempt_omits_segment(self):
        text = ReportEngine()._format_daily_integrity_summary(
            {'empty_unresolved': 2}
        )
        assert '停牌无行情' not in text

    def test_mixed_run_renders_both(self):
        text = ReportEngine()._format_daily_integrity_summary(
            {'empty_unresolved': 1, 'legitimate_no_quote': 3}
        )
        assert '未覆盖: 1' in text
        assert '停牌无行情: 3' in text

    def test_all_zero_returns_empty(self):
        assert (
            ReportEngine()._format_daily_integrity_summary(
                {
                    'empty_unresolved': 0,
                    'legitimate_no_quote': 0,
                    'stale_source': 0,
                    'quality_rejected': 0,
                }
            )
            == ''
        )
