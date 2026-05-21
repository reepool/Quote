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
