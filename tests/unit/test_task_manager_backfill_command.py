from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from utils.task_manager.handlers import TaskManagerHandlers


def _build_handler() -> tuple[TaskManagerHandlers, Mock]:
    task_manager = Mock()
    task_manager.logger = Mock()
    task_manager.send_message = AsyncMock()
    return TaskManagerHandlers(task_manager), task_manager


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
    )
    assert task_manager.send_message.await_count == 2


@pytest.mark.asyncio
async def test_backfill_range_runs_each_trading_day_in_range():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(
        chat_id=1,
        sender_id=2,
        text='/backfill 2026-04-09 2026-04-10 SSE SZSE BSE',
    )

    with patch('scheduler.tasks.scheduled_tasks.daily_data_update', new=AsyncMock(return_value=True)) as run_update:
        await handler.handle_backfill_command(event)

    assert run_update.await_count == 2
    target_dates = [call.kwargs['target_date'] for call in run_update.await_args_list]
    assert target_dates == [date(2026, 4, 9), date(2026, 4, 10)]
    for call in run_update.await_args_list:
        assert call.kwargs['exchanges'] == ['SSE', 'SZSE', 'BSE']
        assert call.kwargs['wait_for_market_close'] is False
        assert call.kwargs['enable_trading_day_check'] is False
    assert task_manager.send_message.await_count == 2


@pytest.mark.asyncio
async def test_backfill_range_rejects_invalid_exchange():
    handler, task_manager = _build_handler()
    event = SimpleNamespace(chat_id=1, sender_id=2, text='/backfill 2026-04-09 2026-04-10 BAD')

    await handler.handle_backfill_command(event)

    sent_message = task_manager.send_message.await_args.args[1]
    assert '无效的交易所代码' in sent_message
