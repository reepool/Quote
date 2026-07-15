from datetime import date
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
            {'instrument_id': '600000.SH', 'ex_date': '2020-06-01'},
            {'instrument_id': '600000.SH', 'ex_date': '2021-06-01'},
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
        tdx=[{'instrument_id': '600000.SH', 'ex_date': '2020-06-01'}],
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
    rows = [{'instrument_id': '600000.SH', 'ex_date': '2020-06-01'}]
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
    assert result['totals']['reference_only_events'] == 0
    assert result['totals']['tdx_only_events'] == 0
