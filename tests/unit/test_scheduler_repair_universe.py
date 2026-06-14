from datetime import date
from unittest.mock import AsyncMock, Mock

import pytest

import scheduler.tasks as scheduler_tasks
from data_manager import DataGapInfo
from scheduler.tasks import ScheduledTasks, _format_repair_universe_summary


def test_scheduler_formats_repair_universe_summary_concisely():
    summary = _format_repair_universe_summary({
        'mode': 'historical_backfill',
        'input_instrument_count': 100,
        'eligible_instrument_count': 80,
        'clipped_instrument_count': 5,
        'skipped_instrument_count': 20,
        'skipped_gap_segment_count': 30,
        'skipped_missing_days': 90,
        'reason_distribution': {
            'index_lifecycle_stale_no_quote': 18,
            'after_delisted_date': 2,
        },
        'current_master_refresh': {
            'requested': True,
            'status': 'success',
            'scopes': ['a_share_index'],
        },
        'samples': [
            {'instrument_id': f'00506{i}.SZ', 'reason': 'index_lifecycle_stale_no_quote'}
            for i in range(10)
        ],
    })

    assert '模式: historical_backfill' in summary
    assert '生命周期跳过=20' in summary
    assert 'segments=30' in summary
    assert '显式主数据刷新: success' in summary
    assert summary.count('index_lifecycle_stale_no_quote') <= 6


@pytest.mark.asyncio
async def test_find_gap_and_repair_skips_remaining_segments_after_no_data_limit(monkeypatch):
    task = ScheduledTasks()
    task.config = Mock()
    task.config.get_nested.side_effect = lambda *args, default=None: (
        2 if args == ('data_config.repair_universe_governance.max_no_data_failures_per_instrument',)
        else 1 if args == ('data_config.repair_universe_governance.max_index_no_data_failures_per_instrument',)
        else default
    )
    task._send_task_report = AsyncMock()

    gaps = [
        DataGapInfo(
            instrument_id='005125.SZ',
            symbol='005125',
            exchange='SZSE',
            gap_start=date(2026, 1, idx + 1),
            gap_end=date(2026, 1, idx + 1),
            gap_days=1,
            gap_type='missing_data',
            severity='low',
            recommendation='test',
            missing_dates=[date(2026, 1, idx + 1)],
            instrument_type='index',
        )
        for idx in range(5)
    ]

    fake_manager = Mock()
    fake_manager.detect_data_gaps = AsyncMock(return_value={
        'gaps': gaps,
        'repair_universe': {
            'skipped_instrument_count': 0,
            'skipped_gap_segment_count': 0,
            'skipped_missing_days': 0,
            'reason_distribution': {},
        },
        'instrument_master_governance': None,
    })
    fake_manager.load_gap_skip_set = AsyncMock(return_value=set())
    fake_manager.is_gap_skipped.side_effect = lambda *args: False
    fake_manager._fill_single_gap = AsyncMock(return_value=False)
    fake_manager.record_gap_skip = AsyncMock()
    fake_manager.build_gap_skip_key.side_effect = (
        lambda instrument_id, gap_start, gap_end: f"{instrument_id}|{gap_start}|{gap_end}"
    )
    fake_manager.get_top_affected_stocks.return_value = []

    monkeypatch.setattr(scheduler_tasks, 'data_manager', fake_manager)
    monkeypatch.setattr(scheduler_tasks.asyncio, 'sleep', AsyncMock())

    assert await task.find_gap_and_repair(
        exchanges=['SZSE'],
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 5),
        skip_failed_segments=True,
    ) is False

    assert fake_manager._fill_single_gap.await_count == 1
    assert fake_manager.record_gap_skip.await_count == 5
    report_data = task._send_task_report.await_args.kwargs['report_data']
    assert report_data['summary']['failed_repairs'] == 1
    assert report_data['summary']['skipped_after_no_data_failures'] == 4
