from datetime import date
from unittest.mock import Mock, patch

import pytest

from data_manager import DataManager


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
