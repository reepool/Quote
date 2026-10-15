"""The registry keeps its source semantics while its entry time is charged."""

import asyncio
import sqlite3
import threading
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from research.announcement_assets.repository import AnnouncementAssetRepository
from research.company_profile import operations
from research.company_profile.candidate_registry import (
    load_official_task_candidate_registry,
)


def test_asset_read_scope_reuses_connection_and_restores_after_exception(tmp_path):
    repo = AnnouncementAssetRepository(tmp_path / "assets.db")
    with repo.connection() as conn:
        conn.execute("CREATE TABLE sample (value INTEGER)")
        conn.commit()
    with pytest.raises(ValueError, match="probe"), repo.read_connection_scope():
        with (
            repo.connection() as first,
            repo.read_connection_scope(),
            repo.connection() as second,
        ):
            assert first is second
            with pytest.raises(sqlite3.OperationalError, match="readonly"):
                first.execute("INSERT INTO sample VALUES (1)")
            other = []

            def another_thread():
                with repo.connection() as conn:
                    other.append(conn is first)

            worker = threading.Thread(target=another_thread)
            worker.start()
            worker.join(timeout=2)
            assert not worker.is_alive() and other == [False]
        raise ValueError("probe")
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        first.execute("SELECT 1")
    with repo.connection() as conn:
        conn.execute("INSERT INTO sample VALUES (2)")
        conn.commit()
        assert conn.execute("SELECT value FROM sample").fetchall()[0][0] == 2


def test_registry_read_scope_preserves_full_denominator_and_every_lookup(tmp_path):
    repo = AnnouncementAssetRepository(tmp_path / "assets.db")
    with repo.connection() as conn:
        conn.execute("CREATE TABLE probe (value INTEGER)")
    repo.get_latest_full_market_universe_snapshot = lambda: {
        "snapshot_id": "full",
        "policy_version": "a_share_active.v1",
        "paired_census_snapshot_id": "census",
        "snapshot_at": "2026-09-17T00:00:00+00:00",
        "instrument_rows": {
            "items": [
                {"instrument_id": ins, "exchange": "SSE", "name": ins}
                for ins in ["600094.SH", "600103.SH", "600000.SH"]
            ]
        },
    }
    repo.get_full_market_universe_snapshot_as_of = lambda _: (
        repo.get_latest_full_market_universe_snapshot()
    )
    repo.list_asset_coverage = lambda _: []
    calls = []

    def report(ins, **kwargs):
        with repo.connection() as conn:
            assert conn.execute("SELECT 1").fetchone()[0] == 1
        calls.append((ins, kwargs))

    access = SimpleNamespace(repository=repo, get_effective_asset=report)
    scoped = load_official_task_candidate_registry(
        as_of="2026-09-17", storage=SimpleNamespace(), shared_asset_access=access
    )
    expected_calls = list(calls)
    repo.read_connection_scope = nullcontext
    calls.clear()
    unscoped = load_official_task_candidate_registry(
        as_of="2026-09-17", storage=SimpleNamespace(), shared_asset_access=access
    )
    assert scoped == unscoped
    assert scoped.counts["total"] == 3
    assert calls == expected_calls and len(calls) == 3


@pytest.mark.parametrize("elapsed", [2.0, 12.0])
def test_entry_charges_registry_and_construction_before_enqueue(
    monkeypatch, tmp_path, elapsed
):
    ticks = iter([100.0, 100.0 + elapsed])
    monkeypatch.setattr(
        operations, "time", SimpleNamespace(monotonic=lambda: next(ticks))
    )
    monkeypatch.setattr(operations, "load_publication_control", lambda _: None)
    monkeypatch.setattr(
        operations, "load_official_task_candidate_registry", lambda **_: "full_registry"
    )
    calls = []

    class Service:
        def __init__(self, **kwargs):
            assert kwargs["candidate_registry"] == "full_registry"

        async def execute(self, action, **kwargs):
            calls.append(kwargs)
            return {"action": action}

    monkeypatch.setattr(operations, "CompanyProfileTaskService", Service)
    kwargs = {
        "action": "run",
        "storage": object(),
        "shared_asset_access": object(),
        "checkpoint_root": tmp_path,
        "max_elapsed_seconds": 10,
    }
    if elapsed >= 10:
        with pytest.raises(TimeoutError, match="before enqueue"):
            asyncio.run(operations.execute_published_task(**kwargs))
        assert not calls
    else:
        assert asyncio.run(operations.execute_published_task(**kwargs)) == {
            "action": "run"
        }
        assert calls[0]["max_elapsed_seconds"] == 8
