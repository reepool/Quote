from datetime import date, datetime
from unittest.mock import AsyncMock, Mock, mock_open, patch

import pytest

from data_manager import DataManager
from utils.report.engine import ReportEngine


def _build_config_manager() -> Mock:
    config = Mock()
    config.get_nested.side_effect = lambda key, default=None: {
        'telegram_config.enabled': False,
        'data_config': {'data_dir': 'data', 'download_chunk_days': 7},
    }.get(key, default)
    return config


@pytest.mark.asyncio
async def test_generate_daily_update_report_uses_exchange_stats_payload():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    update_results = {
        'exchange_stats': {
            'SSE': {
                'success_count': 2587,
                'failure_count': 0,
                'quotes_added': 2307,
                'total_instruments': 2587,
            },
            'SZSE': {
                'success_count': 3770,
                'failure_count': 0,
                'quotes_added': 2886,
                'total_instruments': 3770,
            },
        }
    }

    report = await manager._generate_daily_update_report(
        ['SSE', 'SZSE'],
        date(2026, 4, 13),
        update_results,
    )

    assert report['summary']['total_instruments_checked'] == 6357
    assert report['summary']['updated_instruments'] == 6357
    assert report['summary']['new_quotes_added'] == 5193
    assert report['summary']['success_rate'] == 100.0
    assert report['exchange_stats']['SSE']['success_count'] == 2587
    assert report['exchange_stats']['SSE']['quotes_count'] == 2307
    assert report['errors'] == []


@pytest.mark.asyncio
async def test_generate_daily_update_report_supports_legacy_flat_payload():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    update_results = {
        'SSE': {
            'success_count': 10,
            'failure_count': 1,
            'quotes_added': 8,
            'total_instruments': 12,
        }
    }

    report = await manager._generate_daily_update_report(
        ['SSE'],
        date(2026, 4, 13),
        update_results,
    )

    assert report['summary']['total_instruments_checked'] == 12
    assert report['summary']['updated_instruments'] == 10
    assert report['summary']['new_quotes_added'] == 8
    assert report['exchange_stats']['SSE']['failure_count'] == 1


@pytest.mark.asyncio
async def test_generate_daily_update_report_includes_instrument_master_sync_payload():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    master_sync = {
        'status': 'warning',
        'summary': {'added_instruments': 14, 'deactivated_instruments': 1, 'active_count': 6690},
        'warnings': ['BSE: pytdx current-list differs from authoritative master data'],
        'errors': [],
    }
    update_results = {
        'success_count': 1,
        'failure_count': 0,
        'total_quotes_added': 1,
        'exchange_stats': {
            'BSE': {
                'success_count': 1,
                'failure_count': 0,
                'quotes_added': 1,
                'total_instruments': 1,
            }
        },
        'instrument_master_sync': master_sync,
    }

    report = await manager._generate_daily_update_report(
        ['BSE'],
        date(2026, 5, 21),
        update_results,
    )

    assert report['instrument_master_sync'] == master_sync


def test_report_engine_formats_instrument_master_sync_warnings():
    engine = ReportEngine()
    summary = engine._format_instrument_master_sync_summary({
        'status': 'warning',
        'summary': {'added_instruments': 14, 'deactivated_instruments': 1, 'active_count': 6690},
        'exchanges': {
            'BSE': {
                'status': 'warning',
                'after': {'active_count': 314},
                'added_count': 14,
                'deactivated_count': 0,
            }
        },
        'warnings': ['BSE: master data freshness exceeds 48h'],
        'errors': [],
    })

    assert '状态: warning' in summary
    assert '新增: 14，停用: 1' in summary
    assert 'BSE 状态=warning 活跃=314 +14/-0' in summary
    assert 'master data freshness exceeds 48h' in summary


def test_daily_update_new_instrument_catchup_window_uses_listed_date():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    window = manager._resolve_daily_update_fetch_window(
        exchange='BSE',
        target_date=date(2026, 6, 15),
        latest_quote_date=None,
        listed_date=datetime(2026, 6, 12),
        catchup_config={
            'enabled': True,
            'exchanges': {'SSE', 'SZSE', 'BSE'},
            'new_instrument_catchup_days': 10,
            'short_gap_catchup_days': 5,
        },
    )

    assert window['reason'] == 'new_instrument_catchup'
    assert window['fetch_start_date'] == date(2026, 6, 12)
    assert not window['capped']


def test_daily_update_short_gap_catchup_window_is_capped():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    window = manager._resolve_daily_update_fetch_window(
        exchange='BSE',
        target_date=date(2026, 6, 15),
        latest_quote_date=datetime(2026, 6, 1),
        listed_date=datetime(2026, 1, 1),
        catchup_config={
            'enabled': True,
            'exchanges': {'SSE', 'SZSE', 'BSE'},
            'new_instrument_catchup_days': 10,
            'short_gap_catchup_days': 5,
        },
    )

    assert window['reason'] == 'short_gap_catchup'
    assert window['fetch_start_date'] == date(2026, 6, 10)
    assert window['capped']


def test_daily_update_catchup_skips_non_stock_instruments():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    window = manager._resolve_daily_update_fetch_window(
        exchange='SSE',
        target_date=date(2026, 6, 15),
        latest_quote_date=datetime(2026, 6, 1),
        listed_date=datetime(2026, 1, 1),
        instrument_type='index',
        catchup_config={
            'enabled': True,
            'exchanges': {'SSE', 'SZSE', 'BSE'},
            'new_instrument_catchup_days': 10,
            'short_gap_catchup_days': 5,
        },
    )

    assert window['reason'] == 'normal_daily_window'
    assert window['fetch_start_date'] == date(2026, 6, 14)
    assert not window['capped']


@pytest.mark.asyncio
async def test_generate_daily_update_report_includes_catchup_stats():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    catchup_stats = {
        'new_instrument_count': 1,
        'short_gap_count': 1,
        'capped_count': 1,
        'skipped_missing_listed_date': 0,
        'catchup_quotes_added': 3,
        'samples': [
            {
                'instrument_id': '920083.BJ',
                'reason': 'new_instrument_catchup',
                'fetch_start_date': '2026-06-11',
                'end_date': '2026-06-15',
                'quotes_added': 2,
            }
        ],
    }
    update_results = {
        'success_count': 1,
        'failure_count': 0,
        'total_quotes_added': 3,
        'catchup_stats': catchup_stats,
        'exchange_stats': {
            'BSE': {
                'success_count': 1,
                'failure_count': 0,
                'quotes_added': 3,
                'total_instruments': 1,
                'catchup_stats': catchup_stats,
            }
        },
    }

    report = await manager._generate_daily_update_report(
        ['BSE'],
        date(2026, 6, 15),
        update_results,
    )

    assert report['catchup_stats'] == catchup_stats
    assert report['exchange_stats']['BSE']['catchup_stats'] == catchup_stats


def test_report_engine_formats_daily_catchup_summary():
    engine = ReportEngine()
    summary = engine._format_daily_catchup_summary({
        'new_instrument_count': 1,
        'short_gap_count': 1,
        'capped_count': 1,
        'skipped_missing_listed_date': 0,
        'catchup_quotes_added': 3,
        'samples': [
            {
                'instrument_id': '920083.BJ',
                'reason': 'new_instrument_catchup',
                'fetch_start_date': '2026-06-11',
                'end_date': '2026-06-15',
                'quotes_added': 2,
            }
        ],
    })

    assert '新股追补: 1' in summary
    assert '短缺口追补: 1' in summary
    assert '920083.BJ' in summary


def test_report_engine_formats_index_master_governance_summary_concisely():
    engine = ReportEngine()
    summary = engine._format_index_master_governance_summary({
        'status': 'warning',
        'summary': {
            'master_rows_saved': 2,
            'evidence_rows_saved': 2,
            'active_count': 128,
            'lifecycle_skip_count': 2,
            'direct_terminated_count': 1,
            'inferred_terminated_count': 1,
            'stale_no_quote_count': 6,
            'source_usage': {'cnindex': 2},
            'samples': [
                {
                    'instrument_id': f'48005{i}.SZ',
                    'state': 'calculation_terminated',
                    'confidence': 'series_inferred',
                }
                for i in range(10)
            ],
        },
        'warnings': ['CSIndex full-list endpoint is not enabled'],
        'errors': [],
    })

    assert '状态: warning' in summary
    assert '停编跳过: 2' in summary
    assert '直接: 1' in summary
    assert '推断: 1' in summary
    assert summary.count('calculation_terminated') == 5
    assert len(summary) < 900


def test_daily_update_report_does_not_render_nested_catchup_stats_in_exchange_table():
    engine = ReportEngine()
    sample = {
        'instrument_id': '920083.BJ',
        'symbol': '920083',
        'exchange': 'BSE',
        'reason': 'new_instrument_catchup',
        'listed_date': '2026-06-11',
        'latest_quote_date': None,
        'fetch_start_date': '2026-06-11',
        'end_date': '2026-06-12',
        'capped': False,
        'quotes_added': 2,
    }
    catchup_stats = {
        'new_instrument_count': 1,
        'short_gap_count': 17,
        'capped_count': 6,
        'skipped_missing_listed_date': 0,
        'catchup_quotes_added': 24,
        'samples': [sample] * 10,
    }
    message = engine.generate(
        'daily_update_report',
        {
            'date': '2026-06-12',
            'status': 'success',
            'update_results': {
                'success_count': 6688,
                'failure_count': 0,
                'total_quotes_added': 13348,
                'catchup_stats': catchup_stats,
                'exchange_stats': {
                    'SSE': {
                        'success_count': 2594,
                        'failure_count': 0,
                        'quotes_added': 5186,
                        'total_instruments': 2594,
                        'catchup_stats': catchup_stats,
                    },
                    'SZSE': {
                        'success_count': 3774,
                        'failure_count': 0,
                        'quotes_added': 7524,
                        'total_instruments': 3774,
                        'catchup_stats': catchup_stats,
                    },
                    'BSE': {
                        'success_count': 320,
                        'failure_count': 0,
                        'quotes_added': 638,
                        'total_instruments': 320,
                        'catchup_stats': catchup_stats,
                    },
                },
            },
        },
        'telegram',
    )

    assert len(message) < 4000
    assert 'Catchup Stats' not in message
    assert "'samples':" not in message
    assert '*行情追补*' in message


def test_daily_update_report_renders_index_governance_without_exceeding_message_limit():
    engine = ReportEngine()
    message = engine.generate(
        'daily_update_report',
        {
            'date': '2026-06-12',
            'status': 'success',
            'update_results': {
                'success_count': 20,
                'failure_count': 0,
                'total_quotes_added': 18,
                'exchange_stats': {
                    'SZSE': {
                        'success_count': 20,
                        'failure_count': 0,
                        'quotes_added': 18,
                        'total_instruments': 20,
                    }
                },
                'index_master_governance': {
                    'status': 'warning',
                    'summary': {
                        'master_rows_saved': 120,
                        'evidence_rows_saved': 6,
                        'active_count': 118,
                        'lifecycle_skip_count': 6,
                        'direct_terminated_count': 3,
                        'inferred_terminated_count': 3,
                        'stale_no_quote_count': 12,
                        'source_usage': {'cnindex': 120},
                        'samples': [
                            {
                                'instrument_id': f'48005{i}.SZ',
                                'state': 'calculation_terminated',
                                'confidence': 'series_inferred',
                                'evidence_url': 'https://example.test/very/long/evidence/url',
                            }
                            for i in range(20)
                        ],
                    },
                    'warnings': ['CSIndex full-list endpoint is not enabled'],
                    'errors': [],
                },
            },
            'source': 'Quote System',
            'generated_at': '2026-06-12 21:30:00',
        },
        'telegram',
    )

    assert '*指数主数据治理*' in message
    assert message.count('calculation_terminated') == 5
    assert 'very/long/evidence/url' not in message
    assert len(message) < 4096


@pytest.mark.asyncio
async def test_update_daily_data_fetches_new_instrument_from_listed_date():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()
    manager.data_config['daily_update_catchup'] = {
        'enabled': True,
        'exchanges': ['SSE', 'SZSE', 'BSE'],
        'new_instrument_catchup_days': 10,
        'short_gap_catchup_days': 5,
        'sample_limit': 10,
    }
    manager._maybe_sync_instrument_master_before_daily_update = AsyncMock(
        return_value={'status': 'success', 'action': 'synced'}
    )
    manager._batch_sync_adjustment_factors = AsyncMock(return_value={'synced': 0})
    manager._generate_daily_update_report = AsyncMock(return_value={})
    manager.db_ops = Mock()
    manager.db_ops.get_active_instruments = AsyncMock(return_value=[
        {
            'instrument_id': '920083.BJ',
            'symbol': '920083',
            'exchange': 'BSE',
            'type': 'stock',
            'listed_date': datetime(2026, 6, 12),
            'source_symbol': '920083',
        }
    ])
    manager.db_ops.get_latest_quote_date = AsyncMock(return_value=None)
    manager.db_ops.save_daily_quotes = AsyncMock(return_value=True)
    manager.source_factory = Mock()
    manager.source_factory.get_daily_data = AsyncMock(return_value=[
        {
            'instrument_id': '920083.BJ',
            'time': datetime(2026, 6, 12),
            'open': 1,
            'high': 1,
            'low': 1,
            'close': 1,
            'volume': 1,
            'amount': 1,
            'tradestatus': 1,
            'factor': 1,
        }
    ])

    with patch('builtins.open', mock_open()):
        result = await manager.update_daily_data(
            exchanges=['BSE'],
            target_date=date(2026, 6, 15),
            instrument_types=['stock'],
            run_factor_audit=False,
        )

    args = manager.source_factory.get_daily_data.await_args.args
    assert args[3].date() == date(2026, 6, 12)
    manager._maybe_sync_instrument_master_before_daily_update.assert_awaited_once_with(
        ['BSE'],
        date(2026, 6, 15),
        instrument_types=['stock'],
    )
    assert result['catchup_stats']['new_instrument_count'] == 1
    assert result['catchup_stats']['catchup_quotes_added'] == 1
