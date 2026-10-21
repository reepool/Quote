"""Tests for the shared nodriver start-with-reclaim helper."""

import sys
import types

import pytest

from research.providers.nodriver_session import (
    reclaim_nodriver_user_data_dir,
    start_nodriver_with_reclaim,
)


def _patch_kill(monkeypatch, sink):
    def fake_kill(user_data_dir):
        sink.append(user_data_dir)
        return 3

    monkeypatch.setattr(
        "research.providers.nodriver_session.kill_browser_processes_for_user_data_dir",
        fake_kill,
    )


async def test_start_reclaims_and_retries_until_success(monkeypatch):
    state = {"start_calls": 0, "start_dirs": [], "kill_calls": []}

    class FakeBrowser:
        pass

    async def fake_start(**kwargs):
        state["start_calls"] += 1
        state["start_dirs"].append(kwargs.get("user_data_dir"))
        if state["start_calls"] <= 2:
            raise Exception("Failed to connect to browser")
        return FakeBrowser()

    monkeypatch.setitem(sys.modules, "nodriver", types.SimpleNamespace(start=fake_start))
    _patch_kill(monkeypatch, state["kill_calls"])

    browser, user_data_dir = await start_nodriver_with_reclaim(
        start_timeout_seconds=5,
        retry_attempts=2,
        retry_backoff_seconds=0,
    )

    assert isinstance(browser, FakeBrowser)
    assert state["start_calls"] == 3
    assert user_data_dir == state["start_dirs"][-1]
    assert len(set(state["start_dirs"])) == 3
    assert state["kill_calls"] == state["start_dirs"][:2]


async def test_start_raises_last_error_after_retries_exhausted(monkeypatch):
    state = {"start_calls": 0, "kill_calls": []}

    async def fake_start(**kwargs):
        state["start_calls"] += 1
        raise RuntimeError("boom")

    monkeypatch.setitem(sys.modules, "nodriver", types.SimpleNamespace(start=fake_start))
    _patch_kill(monkeypatch, state["kill_calls"])

    with pytest.raises(RuntimeError, match="boom"):
        await start_nodriver_with_reclaim(
            start_timeout_seconds=5,
            retry_attempts=1,
            retry_backoff_seconds=0,
        )

    assert state["start_calls"] == 2
    assert len(state["kill_calls"]) == 2


async def test_zero_retry_attempts_runs_single_attempt(monkeypatch):
    state = {"start_calls": 0, "kill_calls": []}

    async def fake_start(**kwargs):
        state["start_calls"] += 1
        raise RuntimeError("no chrome")

    monkeypatch.setitem(sys.modules, "nodriver", types.SimpleNamespace(start=fake_start))
    _patch_kill(monkeypatch, state["kill_calls"])

    with pytest.raises(RuntimeError, match="no chrome"):
        await start_nodriver_with_reclaim(
            start_timeout_seconds=5,
            retry_attempts=0,
        )

    assert state["start_calls"] == 1
    assert len(state["kill_calls"]) == 1


def test_reclaim_kills_processes_and_removes_dir(monkeypatch, tmp_path):
    kill_calls = []
    profile_dir = tmp_path / "uc_nodriver_probe"
    profile_dir.mkdir()

    def fake_kill(user_data_dir):
        kill_calls.append(user_data_dir)
        return 2

    monkeypatch.setattr(
        "research.providers.nodriver_session.kill_browser_processes_for_user_data_dir",
        fake_kill,
    )
    signalled = reclaim_nodriver_user_data_dir(str(profile_dir))

    assert signalled == 2
    assert kill_calls == [str(profile_dir)]
    assert not profile_dir.exists()
