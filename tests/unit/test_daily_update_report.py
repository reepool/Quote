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
async def test_generate_daily_update_report_marks_idempotent_noop_as_successful():
    with patch('data_manager.config_manager', _build_config_manager()):
        manager = DataManager()

    update_results = {
        'exchange_stats': {
            'SSE': {
                'success_count': 0,
                'failure_count': 0,
                'quotes_added': 0,
                'total_instruments': 2594,
            },
            'SZSE': {
                'success_count': 0,
                'failure_count': 0,
                'quotes_added': 0,
                'total_instruments': 3281,
            },
        }
    }

    report = await manager._generate_daily_update_report(
        ['SSE', 'SZSE'],
        date(2026, 6, 15),
        update_results,
    )

    assert report['summary']['total_instruments_checked'] == 5875
    assert report['summary']['updated_instruments'] == 0
    assert report['summary']['new_quotes_added'] == 0
    assert report['summary']['success_rate'] == 100.0
    assert report['summary']['no_op'] is True
    assert report['summary']['no_op_reason'] == 'target_date_already_covered'
    assert report['summary']['summary_note'] == '目标日期已覆盖，无新增行情'


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


def test_report_engine_formats_hkex_instrument_master_source_usage():
    engine = ReportEngine()
    summary = engine._format_instrument_master_sync_summary({
        'status': 'success',
        'summary': {
            'added_instruments': 1,
            'deactivated_instruments': 113,
            'active_count': 3040,
        },
        'exchanges': {
            'HKEX': {
                'status': 'success',
                'after': {'active_count': 3040},
                'added_count': 0,
                'deactivated_count': 0,
                'source_usage': {
                    'hkex_securities_list': 17885,
                    'hkexnews_active_list': 18035,
                    'hkexnews_delisted_list': 864,
                    'akshare_eastmoney_diagnostics_only': 0,
                },
            }
        },
        'warnings': [],
        'errors': [],
    })

    assert 'HKEX 状态=success 活跃=3040 +0/-0' in summary
    assert 'source=unknown' not in summary
    assert 'source=hkex_securities_list+hkexnews_active_list+hkexnews_delisted_list' in summary


def test_daily_update_report_uses_stock_child_for_instrument_master_summary():
    engine = ReportEngine()
    merged_governance = {
        'status': 'warning',
        'summary': {'active_count': 6196, 'master_rows_saved': 1300},
        'exchanges': {
            'SSE': {'status': 'success', 'after': {'active_count': 280}},
            'SZSE': {'status': 'success', 'after': {'active_count': 388}},
            'BSE': {'status': 'warning', 'after': {'active_count': 321}},
        },
        'index_master_governance': {
            'status': 'warning',
            'scope': 'a_share_index',
            'summary': {'master_rows_saved': 1300, 'active_count': 668},
            'warnings': ['CSIndex full-list endpoint is not enabled'],
            'errors': [],
        },
    }
    stock_child = {
        'status': 'warning',
        'scope': 'a_share_stock',
        'summary': {
            'added_instruments': 0,
            'deactivated_instruments': 0,
            'active_count': 5528,
        },
        'exchanges': {
            'SSE': {
                'status': 'success',
                'after': {'active_count': 2314},
                'added_count': 0,
                'deactivated_count': 0,
            },
            'SZSE': {
                'status': 'success',
                'after': {'active_count': 2893},
                'added_count': 0,
                'deactivated_count': 0,
            },
            'BSE': {
                'status': 'warning',
                'after': {'active_count': 321},
                'added_count': 0,
                'deactivated_count': 0,
            },
        },
        'warnings': ['BSE: BaoStock primary did not contribute rows'],
        'errors': [],
    }
    merged_governance['children'] = [stock_child, merged_governance['index_master_governance']]

    message = engine.generate(
        'daily_update_report',
        {
            'date': '2026-06-17',
            'status': 'success',
            'update_results': {
                'success_count': 6196,
                'failure_count': 0,
                'total_quotes_added': 12371,
                'exchange_stats': {},
                'instrument_master_sync': merged_governance,
                'index_master_governance': merged_governance['index_master_governance'],
            },
            'source': 'Quote System',
            'generated_at': '2026-06-17 20:26:03',
        },
        'telegram',
    )

    assert '活跃合计: 5528' in message
    assert 'SSE 状态=success 活跃=2314 +0/-0' in message
    assert 'SZSE 状态=success 活跃=2893 +0/-0' in message
    assert 'SSE 状态=success 活跃=280' not in message
    assert 'SZSE 状态=success 活跃=388' not in message


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
            'metadata_only_legacy_deactivated_count': 1,
            'invalid_quote_code_deactivated_count': 1,
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
    assert 'metadata-only: 1' in summary
    assert 'invalid-quote: 1' in summary
    assert summary.count('calculation_terminated') == 5
    assert len(summary) < 900


def test_gap_report_renders_repair_universe_lifecycle_summary():
    engine = ReportEngine()
    message = engine.generate(
        'gap_report',
        {
            'status': 'success',
            'summary': {
                'total_gaps': 2,
                'affected_stocks': 1,
                'severity_distribution': {'low': 2},
                'lifecycle_skipped_instruments': 3,
                'lifecycle_skipped_gap_segments': 4,
            },
            'repair_universe': {
                'skipped_instrument_count': 3,
                'skipped_gap_segment_count': 4,
                'reason_distribution': {'index_lifecycle_stale_no_quote': 3},
            },
            'repair_universe_summary': (
                "模式: historical_backfill\n"
                "标的: 输入=10，可修复=7，裁剪=1，生命周期跳过=3\n"
                "原因: index_lifecycle_stale_no_quote=3"
            ),
            'top_affected_stocks': [],
        },
        'telegram',
    )

    assert '*生命周期过滤*' in message
    assert '生命周期跳过=3' in message
    assert 'Lifecycle Skipped Instruments' in message


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


def test_daily_update_report_renders_idempotent_noop_note_as_success():
    engine = ReportEngine()
    message = engine.generate(
        'daily_update_report',
        {
            'date': '2026-06-15',
            'status': 'success',
            'update_results': {
                'success_count': 0,
                'failure_count': 0,
                'total_quotes_added': 0,
                'success_rate': 100.0,
                'summary_note': '目标日期已覆盖，无新增行情',
                'exchange_stats': {
                    'SSE': {
                        'success_count': 0,
                        'failure_count': 0,
                        'quotes_added': 0,
                        'total_instruments': 2594,
                    },
                },
            },
        },
        'telegram',
    )

    assert '成功率: 100.0%' in message
    assert '备注: 目标日期已覆盖，无新增行情' in message
    assert '任务执行状态: ✅ success' in message


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
        job_name='daily_data_update',
    )
    assert result['catchup_stats']['new_instrument_count'] == 1
    assert result['catchup_stats']['catchup_quotes_added'] == 1
