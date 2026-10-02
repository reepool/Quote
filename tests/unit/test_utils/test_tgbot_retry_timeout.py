"""_retry_on_failure 装饰器的单次尝试超时行为验证。

背景：2026-10-02 港股日更被开场通知阻塞 2 小时以上，根因之一是
网络黑洞时单次 Telethon 发送无限挂起，重试逻辑形同虚设。
"""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import utils.tgbot as tgbot_module
from utils.tgbot import _retry_on_failure


class FakeBot:
    """仅提供装饰器读取的重试配置属性。"""

    def __init__(self, attempt_timeout=0, retry_interval=0.01, retry_times=3):
        self.tg_msg_attempt_timeout = attempt_timeout
        self.tg_msg_retry_interval = retry_interval
        self.tg_msg_retry_times = retry_times
        self.attempts = 0


@_retry_on_failure
async def hanging_send(bot, chat_id, message):
    bot.attempts += 1
    await asyncio.sleep(30)
    return "never"


@_retry_on_failure
async def flaky_send(bot, chat_id, message):
    bot.attempts += 1
    if bot.attempts < 3:
        raise RuntimeError("transient")
    return f"sent:{chat_id}"


def test_attempt_timeout_gives_up_without_resubmitting():
    """超时后不得盲目重试：投递状态未知，重发可能在网络恢复后重复投递。"""
    bot = FakeBot(attempt_timeout=0.05)

    started = time.monotonic()
    result = asyncio.run(hanging_send(bot, 1, "m"))
    elapsed = time.monotonic() - started

    assert result is None
    assert bot.attempts == 1
    assert elapsed < 5


def test_retry_returns_result_when_attempt_succeeds():
    bot = FakeBot(attempt_timeout=5)

    result = asyncio.run(flaky_send(bot, 42, "m"))

    assert result == "sent:42"
    assert bot.attempts == 3


def test_flood_wait_still_retries_with_server_mandated_wait():
    """显式抛出的限流（FloodError）仍按服务器要求等待并重试，不受单次超时影响。"""
    from telethon.errors import FloodWaitError

    bot = FakeBot(attempt_timeout=0.05, retry_interval=0.01, retry_times=3)
    # seconds = int(capture)，取 1 秒以真实验证服务器要求的等待
    flood = FloodWaitError(request=None, capture=1)

    @_retry_on_failure
    async def flood_then_send(bot_self, chat_id, message):
        bot_self.attempts += 1
        if bot_self.attempts == 1:
            raise flood
        return "sent"

    started = time.monotonic()
    result = asyncio.run(flood_then_send(bot, 1, "m"))
    elapsed = time.monotonic() - started

    assert result == "sent"
    assert bot.attempts == 2
    # FloodError 分支等待了服务器要求的时长
    assert elapsed >= 0.95


def test_outer_cancellation_propagates_through_retry_wrapper():
    bot = FakeBot(attempt_timeout=30, retry_times=5)

    async def outer():
        await asyncio.wait_for(hanging_send(bot, 1, "m"), timeout=0.05)

    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(outer())

    assert bot.attempts == 1


def test_send_message_async_returns_client_result(monkeypatch):
    """成功路径必须把 Telethon 的发送结果返回给上层，否则成功会被误计为失败。"""
    bot = tgbot_module.TelegramBot()
    fake_client = SimpleNamespace(send_message=AsyncMock(return_value="MSG_HANDLE"))
    monkeypatch.setattr(bot, "bot_thon", fake_client)

    result = asyncio.run(bot.send_message_async(471105519, "hello"))

    assert result == "MSG_HANDLE"
    fake_client.send_message.assert_awaited_once_with(471105519, "hello")


def test_send_notification_counts_timeout_giveup_as_failure(monkeypatch):
    """全链路验证：单次尝试超时放弃后 send_notification 必须返回失败。"""
    monkeypatch.setattr(
        tgbot_module,
        "telegram_config",
        {"enabled": True, "chat_id": ["471105519"]},
    )
    bot = tgbot_module.TelegramBot()
    monkeypatch.setattr(bot, "tg_msg_attempt_timeout", 0.01)

    async def slow_send(chat_id, message):
        await asyncio.sleep(30)

    fake_client = SimpleNamespace(send_message=slow_send)
    monkeypatch.setattr(bot, "bot_thon", fake_client)

    result = asyncio.run(bot.send_notification("hello", prefix="[t]", level="info"))

    assert result is False


def test_send_notification_returns_true_when_client_delivers(monkeypatch):
    monkeypatch.setattr(
        tgbot_module,
        "telegram_config",
        {"enabled": True, "chat_id": ["471105519"]},
    )
    bot = tgbot_module.TelegramBot()
    fake_client = SimpleNamespace(send_message=AsyncMock(return_value="MSG_HANDLE"))
    monkeypatch.setattr(bot, "bot_thon", fake_client)

    result = asyncio.run(bot.send_notification("hello", prefix="[t]", level="info"))

    assert result is True
