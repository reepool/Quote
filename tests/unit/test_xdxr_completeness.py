from datetime import date, datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from data_manager import DataManager


def _manager_with_rows(*, pending=None, tdx=None, reference=None):
    config = Mock()
    config.get_nested.side_effect = lambda key, default=None: {
        'telegram_config.enabled': False,
        'data_config': {'data_dir': 'data'},
    }.get(key, default)
    with patch('data_manager.config_manager', config):
        manager = DataManager()

    class _DbOps:
        async def execute_read_query(self, query, params=None):
            if "validation_result = 'pending_factor_missing_pre_close'" in query:
                return list(pending or [])
            if 'FROM adjustment_factors_tdx' in query:
                return list(tdx or [])
            if 'FROM adjustment_factors' in query:
                return list(reference or [])
            return []

        async def get_trading_calendar_records(self, exchange, start_date, end_date):
            records = []
            current = start_date
            while current <= end_date:
                records.append({
                    'exchange': exchange,
                    'date': datetime.combine(current, datetime.min.time()),
                    'is_trading_day': current.weekday() < 5,
                })
                current += timedelta(days=1)
            return records

    manager.db_ops = _DbOps()
    return manager


@pytest.mark.asyncio
async def test_pending_factor_summary_reports_cash_events_and_instruments():
    manager = _manager_with_rows(pending=[
        {
            'instrument_id': '600000.SH',
            'ex_date': '2020-06-01',
            'fenhong': 1.0,
            'songzhuangu': 0.0,
            'peigu': 0.0,
            'peigujia': 0.0,
        },
        {
            'instrument_id': '600000.SH',
            'ex_date': '2021-06-01',
            'fenhong': 0.0,
            'songzhuangu': 2.0,
            'peigu': 0.0,
            'peigujia': 0.0,
        },
    ])

    result = await manager.get_tdx_xdxr_pending_factor_summary(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['600000.SH'],
    )

    assert result['status'] == 'partial'
    assert result['totals'] == {
        'pending_factors': 2,
        'pending_instruments': 1,
        'pending_cash_events': 1,
    }
    assert result['instrument_ids'] == ['600000.SH']
    assert result['samples'][0]['reason'] == 'pending_factor_missing_pre_close'


@pytest.mark.asyncio
async def test_xdxr_reconciliation_reports_overlap_and_both_single_sided_sets():
    manager = _manager_with_rows(
        tdx=[
            {
                'instrument_id': '600000.SH',
                'ex_date': '2020-06-01',
                'factor': 1.1,
                'validation_result': 'computed_unvalidated',
            },
            {
                'instrument_id': '600000.SH',
                'ex_date': '2021-06-01',
                'factor': 1.3,
                'validation_result': 'computed_unvalidated',
            },
        ],
        reference=[
            {
                'instrument_id': '600000.SH',
                'ex_date': '2019-01-01',
                'source': 'baostock',
                'factor': 1.0,
                'cumulative_factor': 1.0,
            },
            {
                'instrument_id': '600000.SH',
                'ex_date': '2020-06-01',
                'source': 'baostock',
                'factor': 1.1,
                'cumulative_factor': 1.1,
            },
            {
                'instrument_id': '600000.SH',
                'ex_date': '2022-06-01',
                'source': 'akshare',
                'factor': 1.2,
                'cumulative_factor': 1.2,
            },
        ],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['600000.SH'],
    )

    assert result['status'] == 'partial'
    assert result['totals']['overlap_events'] == 1
    assert result['totals']['exact_factor_matches'] == 1
    assert result['totals']['shifted_factor_matches'] == 0
    assert result['totals']['reference_only_events'] == 1
    assert result['totals']['tdx_only_events'] == 1
    assert result['reference_source_distribution'] == {
        'baostock': 1,
        'akshare': 1,
    }
    assert result['reference_only_samples'][0]['ex_date'] == '2022-06-01'
    assert result['tdx_only_samples'][0]['ex_date'] == '2021-06-01'


@pytest.mark.asyncio
async def test_xdxr_reconciliation_is_unavailable_without_reference_rows():
    manager = _manager_with_rows(
        tdx=[{
            'instrument_id': '600000.SH',
            'ex_date': '2020-06-01',
            'factor': 1.1,
            'validation_result': 'computed_unvalidated',
        }],
        reference=[],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['600000.SH'],
    )

    assert result['status'] == 'unavailable'
    assert result['totals']['reference_events'] == 0
    assert result['warnings']


@pytest.mark.asyncio
async def test_xdxr_reconciliation_succeeds_when_reference_dates_are_covered():
    rows = [{
        'instrument_id': '600000.SH',
        'ex_date': '2020-06-01',
        'factor': 1.1,
        'validation_result': 'computed_unvalidated',
    }]
    manager = _manager_with_rows(
        tdx=rows,
        reference=[
            {
                'instrument_id': '600000.SH',
                'ex_date': '2019-01-01',
                'source': 'baostock',
                'factor': 1.0,
                'cumulative_factor': 1.0,
            },
            {
                **rows[0],
                'source': 'baostock',
                'factor': 1.1,
                'cumulative_factor': 1.1,
            },
        ],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['600000.SH'],
    )

    assert result['status'] == 'success'
    assert result['totals']['exact_factor_matches'] == 1
    assert result['totals']['reference_only_events'] == 0
    assert result['totals']['tdx_only_events'] == 0


@pytest.mark.asyncio
async def test_xdxr_reconciliation_absorbs_matching_provider_date_shift():
    manager = _manager_with_rows(
        tdx=[{
            'instrument_id': '600000.SH',
            'ex_date': '2020-06-01',
            'factor': 1.1,
            'validation_result': 'computed_unvalidated',
        }],
        reference=[
            {
                'instrument_id': '600000.SH',
                'ex_date': '2019-01-01',
                'source': 'baostock',
                'factor': 1.0,
                'cumulative_factor': 1.0,
            },
            {
                'instrument_id': '600000.SH',
                'ex_date': '2020-06-02',
                'source': 'baostock',
                'factor': 1.1,
                'cumulative_factor': 1.1,
            },
        ],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['600000.SH'],
    )

    assert result['status'] == 'success'
    assert result['totals']['exact_factor_matches'] == 0
    assert result['totals']['shifted_factor_matches'] == 1
    assert result['totals']['reference_factor_change_only'] == 0
    assert result['totals']['tdx_event_only'] == 0
    assert result['shifted_match_samples'][0]['trading_session_distance'] == 1


@pytest.mark.asyncio
async def test_xdxr_reconciliation_classifies_nearby_factor_conflict_once():
    manager = _manager_with_rows(
        tdx=[{
            'instrument_id': '000001.SZ',
            'ex_date': '2020-06-01',
            'factor': 1.1,
            'validation_result': 'computed_unvalidated',
        }],
        reference=[
            {
                'instrument_id': '000001.SZ',
                'ex_date': '2019-01-01',
                'source': 'baostock',
                'factor': 1.0,
                'cumulative_factor': 1.0,
            },
            {
                'instrument_id': '000001.SZ',
                'ex_date': '2020-06-01',
                'source': 'baostock',
                'factor': 1.3,
                'cumulative_factor': 1.3,
            },
        ],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['000001.SZ'],
    )

    assert result['status'] == 'partial'
    assert result['totals']['factor_conflicts'] == 1
    assert result['totals']['reference_factor_change_only'] == 0
    assert result['totals']['tdx_event_only'] == 0
    assert result['factor_conflict_samples'][0]['reason'] == 'nearby_factor_conflict'


@pytest.mark.asyncio
async def test_xdxr_reconciliation_uses_cumulative_ratio_over_legacy_factor_value():
    manager = _manager_with_rows(
        tdx=[{
            'instrument_id': '600000.SH',
            'ex_date': '2020-06-01',
            'factor': 1.2,
            'validation_result': 'computed_unvalidated',
        }],
        reference=[
            {
                'instrument_id': '600000.SH',
                'ex_date': '2019-01-01',
                'source': 'baostock',
                'factor': 1.1,
                'cumulative_factor': 1.1,
            },
            {
                'instrument_id': '600000.SH',
                'ex_date': '2020-06-01',
                'source': 'baostock',
                'factor': 1.32,
                'cumulative_factor': 1.32,
            },
        ],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(2020, 1, 1),
        end_date=date(2020, 12, 31),
        instrument_ids=['600000.SH'],
    )

    assert result['status'] == 'success'
    assert result['totals']['exact_factor_matches'] == 1
    assert result['exact_match_samples'][0]['reference_factor'] == pytest.approx(1.2)


@pytest.mark.asyncio
async def test_xdxr_reconciliation_uses_stored_factor_without_source_predecessor():
    manager = _manager_with_rows(
        tdx=[{
            'instrument_id': '000060.SZ',
            'ex_date': '2026-07-09',
            'factor': 1.008416,
            'validation_result': 'computed_unvalidated',
        }],
        reference=[{
            'instrument_id': '000060.SZ',
            'ex_date': '2026-07-09',
            'source': 'akshare',
            'factor': 1.008416,
            'cumulative_factor': 41.977894,
        }],
    )

    result = await manager.reconcile_tdx_xdxr_history(
        start_date=date(1990, 12, 19),
        end_date=date(2026, 7, 15),
        instrument_ids=['000060.SZ'],
    )

    assert result['status'] == 'success'
    assert result['totals']['exact_factor_matches'] == 1
    assert result['totals']['factor_conflicts'] == 0
    assert result['exact_match_samples'][0]['reference_factor'] == pytest.approx(1.008416)
