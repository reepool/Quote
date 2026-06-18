import asyncio
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from utils.task_manager.handlers import TaskManagerHandlers


def _build_handler() -> tuple[TaskManagerHandlers, Mock]:
    task_manager = Mock()
    task_manager.logger = Mock()
    task_manager.send_message = AsyncMock()
    task_manager.job_config_manager = Mock()
    return TaskManagerHandlers(task_manager), task_manager


@pytest.mark.asyncio
async def test_run_daily_update_with_date_passes_job_config_for_report():
    handler, task_manager = _build_handler()
    target_date = date(2026, 6, 15)
    job_config = SimpleNamespace(report=True)
    task_manager.job_config_manager.get_job_config.return_value = job_config

    with patch(
        'utils.config_manager.config_manager.get_nested',
        return_value={
            'parameters': {
                'exchanges': ['SSE', 'SZSE', 'BSE'],
                'instrument_types': ['stock', 'index'],
            },
        },
    ), patch(
        'scheduler.tasks.scheduled_tasks.daily_data_update',
        new=AsyncMock(return_value=True),
    ) as run_update:
        success = await handler._execute_task_direct(
            chat_id=1,
            job_id='daily_data_update',
            target_date=target_date,
        )

    assert success is True
    run_update.assert_awaited_once_with(
        exchanges=['SSE', 'SZSE', 'BSE'],
        target_date=target_date,
        wait_for_market_close=False,
        enable_trading_day_check=False,
        instrument_types=['stock', 'index'],
        job_config=job_config,
    )


@pytest.mark.asyncio
async def test_backfill_single_day_keeps_legacy_syntax():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(chat_id=1, sender_id=2, text='/backfill 2026-04-09 BSE')

    with patch('scheduler.tasks.scheduled_tasks.daily_data_update', new=AsyncMock(return_value=True)) as run_update:
        await handler.handle_backfill_command(event)

    run_update.assert_awaited_once_with(
        exchanges=['BSE'],
        target_date=date(2026, 4, 9),
        wait_for_market_close=False,
        enable_trading_day_check=False,
        run_factor_audit=False,
    )
    assert task_manager.send_message.await_count == 2


@pytest.mark.asyncio
async def test_backfill_range_runs_single_range_backfill():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(
        chat_id=1,
        sender_id=2,
        text='/backfill 2026-04-09 2026-04-10 SSE SZSE BSE',
    )

    with patch(
        'scheduler.tasks.scheduled_tasks.daily_data_backfill_range',
        new=AsyncMock(return_value={'failure_count': 0, 'total_quotes_added': 10}),
    ) as run_update:
        await handler.handle_backfill_command(event)

    run_update.assert_awaited_once_with(
        start_date=date(2026, 4, 9),
        end_date=date(2026, 4, 10),
        exchanges=['SSE', 'SZSE', 'BSE'],
        run_factor_audit=False,
    )
    assert task_manager.send_message.await_count == 2


@pytest.mark.asyncio
async def test_backfill_range_rejects_invalid_exchange():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(chat_id=1, sender_id=2, text='/backfill 2026-04-09 2026-04-10 BAD')

    await handler.handle_backfill_command(event)

    sent_message = task_manager.send_message.await_args.args[1]
    assert '无效的交易所代码' in sent_message


@pytest.mark.asyncio
async def test_futures_calendar_backfill_manual_task_passes_direct_parameters():
    handler, task_manager = _build_handler()
    task_manager.task_scheduler = Mock()
    task_manager.task_scheduler.execute_job_direct = AsyncMock(return_value=True)

    await handler._run_futures_calendar_backfill_task(
        chat_id=1,
        scope_id='gfex_all',
        exchanges=['GFEX'],
        categories=['all'],
        instrument_ids=None,
        series_ids=None,
        series_types=['main_continuous'],
        start_date='2022-12-22',
        end_date='2022-12-31',
        dry_run=False,
        max_days=10,
    )

    task_manager.task_scheduler.execute_job_direct.assert_awaited_once_with(
        'futures_official_calendar_backfill',
        parameters={
            'scope_id': 'gfex_all',
            'scope_ids': None,
            'exchanges': ['GFEX'],
            'categories': ['all'],
            'instrument_ids': None,
            'series_ids': None,
            'series_types': ['main_continuous'],
            'start_date': '2022-12-22',
            'end_date': '2022-12-31',
            'dry_run': False,
            'max_days': 10,
        },
        include_dependencies=True,
    )
    sent_message = task_manager.send_message.await_args.args[1]
    assert '期货交易日历手工回填成功' in sent_message


@pytest.mark.asyncio
async def test_run_futures_calendar_backfill_with_args_delegates_to_manual_command():
    handler, task_manager = _build_handler()
    task_manager.task_scheduler = Mock()
    task_manager.task_scheduler.execute_job_direct = AsyncMock(return_value=True)
    event = SimpleNamespace(
        chat_id=1,
        sender_id=2,
        text='/run futures_official_calendar_backfill exchange=GFEX start=2022-12-22 end=2022-12-31 dry_run max_days=10',
    )

    await handler.handle_run_command(event)
    await asyncio.sleep(0)

    task_manager.task_scheduler.execute_job_direct.assert_awaited_once_with(
        'futures_official_calendar_backfill',
        parameters={
            'scope_id': None,
            'scope_ids': None,
            'exchanges': ['GFEX'],
            'categories': None,
            'instrument_ids': None,
            'series_ids': None,
            'series_types': None,
            'start_date': '2022-12-22',
            'end_date': '2022-12-31',
            'dry_run': True,
            'max_days': 10,
        },
        include_dependencies=True,
    )
    assert any(
        '期货交易日历手工回填成功' in call.args[1]
        for call in task_manager.send_message.await_args_list
    )


@pytest.mark.asyncio
async def test_hkex_review_command_appends_manual_evidence():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(
        chat_id=1,
        sender_id=2,
        text='/hkex_review 08888.HK delisted 2026-05-30 已确认退市 evidence=https://www.hkexnews.hk/',
    )

    with patch('data_manager.data_manager') as dm:
        dm.append_hkex_manual_review_evidence = AsyncMock(return_value={
            'status': 'success',
            'path': 'data/hkex_manual_review.json',
            'entry': {
                'instrument_id': '08888.HK',
                'action': 'delisted',
                'effective_date': '2026-05-30',
            },
            'total': 1,
        })
        await handler.handle_hkex_review_command(event)

    dm.append_hkex_manual_review_evidence.assert_awaited_once_with(
        instrument_id='08888.HK',
        action='delisted',
        effective_date='2026-05-30',
        reason='已确认退市',
        evidence_url='https://www.hkexnews.hk/',
        reviewed_by='2',
    )
    sent_message = task_manager.send_message.await_args.args[1]
    assert '已追加港股主数据人工复核' in sent_message


@pytest.mark.asyncio
async def test_hkex_review_pending_command_runs_audit_only():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(chat_id=1, sender_id=2, text='/hkex_review pending 3')

    with patch('data_manager.data_manager') as dm:
        dm.sync_hkex_instrument_master = AsyncMock(return_value={
            'status': 'success',
            'summary': {'review_required': 1},
            'exchanges': {
                'HKEX': {
                    'review_required_samples': [
                        {'instrument_id': '08888.HK', 'reason': 'missing', 'local': {'name': 'REVIEW SAMPLE'}}
                    ]
                }
            },
        })
        await handler.handle_hkex_review_command(event)

    dm.sync_hkex_instrument_master.assert_awaited_once_with(mode='audit_only')
    sent_message = task_manager.send_message.await_args.args[1]
    assert '08888.HK' in sent_message
