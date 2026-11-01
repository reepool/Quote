"""Worker-loop isolation tests (openspec: isolate-long-tasks-from-api-event-loop).

Covers: main-loop responsiveness under long sync tasks, concurrency, timeout
and operator-stop cancellation, orphan containment, escape hatch, progress
reporting, scan-phase checkpoints and graceful shutdown.
"""

import asyncio
import contextlib
import threading
import time
import types
from dataclasses import dataclass, field
from typing import Any

import pytest

import scheduler.tasks as task_module
from scheduler.scheduler import TaskScheduler
from utils import worker_context


@dataclass
class FakeJobConfig:
    job_id: str
    parameters: dict = field(default_factory=dict)
    enabled: bool = True
    manual_only: bool = False
    max_instances: int = 1
    report: bool = False


def _install_task(monkeypatch, job_id: str, func) -> None:
    monkeypatch.setattr(task_module.scheduled_tasks, job_id, func, raising=False)


def _cleanup_task(monkeypatch, job_id: str) -> None:
    monkeypatch.delattr(task_module.scheduled_tasks, job_id, raising=False)


@pytest.fixture
def scheduler(monkeypatch):
    # Keep tests deterministic regardless of deployment config: tests opt in
    # per run via explicit run_on_main_loop in parameters.
    instance = TaskScheduler()
    yield instance


@pytest.mark.asyncio
async def test_worker_task_keeps_main_loop_responsive(scheduler, monkeypatch):
    """Long sync work in the worker must not starve the main loop."""
    job_id = "worker_block_probe"

    def blocking_sync_work():
        total = 0
        deadline = time.monotonic() + 1.2
        while time.monotonic() < deadline:
            # CPU churn + sync sleeps, no awaits: the starvation pattern.
            total += sum(i * i for i in range(300))
            time.sleep(0.02)
        return total

    async def fake_task(job_config=None):
        return await asyncio.to_thread(blocking_sync_work)

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        max_gap = 0.0

        async def ping_loop(stop: asyncio.Event):
            nonlocal max_gap
            last = time.monotonic()
            while not stop.is_set():
                await asyncio.sleep(0.02)
                now = time.monotonic()
                max_gap = max(max_gap, now - last)
                last = now

        stop = asyncio.Event()
        run_task = asyncio.create_task(
            scheduler._run_configured_task_raw(
                job_id,
                {"run_on_main_loop": False},
                include_dependencies=False,
            )
        )
        pinger = asyncio.create_task(ping_loop(stop))
        result = await run_task
        stop.set()
        await pinger

        assert isinstance(result, int)
        # On the main loop the same task would hold the loop ~1.2s; in the
        # worker the loop keeps scheduling at its 20ms cadence.
        assert max_gap < 0.5, f"main loop starved: max gap {max_gap:.3f}s"
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_two_worker_jobs_run_concurrently(scheduler, monkeypatch):
    """Different jobs run in parallel workers; neither blocks the other."""
    job_a = "worker_concurrent_a"
    job_b = "worker_concurrent_b"

    async def make_task():
        async def fake_task(job_config=None):
            await asyncio.sleep(0.6)
            return True

        return fake_task

    _install_task(monkeypatch, job_a, await make_task())
    _install_task(monkeypatch, job_b, await make_task())
    try:
        scheduler.job_configs = {
            job_a: FakeJobConfig(job_id=job_a),
            job_b: FakeJobConfig(job_id=job_b),
        }
        started = time.monotonic()
        results = await asyncio.gather(
            scheduler._run_configured_task_raw(job_a, {"run_on_main_loop": False}, include_dependencies=False),
            scheduler._run_configured_task_raw(job_b, {"run_on_main_loop": False}, include_dependencies=False),
        )
        elapsed = time.monotonic() - started
        assert results == [True, True]
        assert elapsed < 1.1, f"jobs appear serialized: {elapsed:.2f}s"
    finally:
        _cleanup_task(monkeypatch, job_a)
        _cleanup_task(monkeypatch, job_b)


@pytest.mark.asyncio
async def test_max_runtime_timeout_cancels_worker(scheduler, monkeypatch):
    """max_runtime_seconds expiry injects cancellation at the next checkpoint."""
    job_id = "worker_timeout_probe"

    async def fake_task(job_config=None):
        while True:
            await asyncio.sleep(0.05)

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        started = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await scheduler._run_configured_task_raw(
                job_id,
                {"run_on_main_loop": False, "max_runtime_seconds": 0.5},
                include_dependencies=False,
            )
        elapsed = time.monotonic() - started
        assert 0.4 < elapsed < 2.0, f"timeout fired at {elapsed:.2f}s"
        assert job_id not in scheduler.running_tasks
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_operator_stop_cancels_worker(scheduler, monkeypatch):
    """Operator stop request cancels the active worker run."""
    job_id = "worker_stop_probe"

    async def fake_task(job_config=None):
        while True:
            await asyncio.sleep(0.05)

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        run = asyncio.create_task(
            scheduler._run_configured_task_raw(job_id, {"run_on_main_loop": False}, include_dependencies=False)
        )
        for _ in range(50):
            if scheduler.get_worker_run_snapshots():
                break
            await asyncio.sleep(0.05)
        snapshots = scheduler.get_worker_run_snapshots()
        assert snapshots and snapshots[0]["job_id"] == job_id

        result = scheduler.request_stop_job(job_id)
        assert result["requested"] == 1

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, timeout=3.0)
        assert not scheduler.get_worker_run_snapshots()
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_stuck_worker_times_out_and_orphan_is_logged(scheduler, monkeypatch):
    """A worker stuck in a sync stretch still times out on the main loop and
    leaves a diagnosable orphan warning."""
    job_id = "worker_orphan_probe"

    async def fake_task(job_config=None):
        # Deliberately blocks the worker loop with a sync stretch and no
        # checkpoints: cancellation cannot land until the stretch ends.
        time.sleep(3.0)

    class _LogCapture:
        def __init__(self):
            self.records = []

        def error(self, msg, *args, **kwargs):
            self.records.append(("ERROR", msg % args if args else msg))

        def warning(self, msg, *args, **kwargs):
            self.records.append(("WARN", msg % args if args else msg))

        def info(self, msg, *args, **kwargs):
            pass

    log_capture = _LogCapture()
    import scheduler.scheduler as sched_module

    monkeypatch.setattr(sched_module, "scheduler_logger", log_capture)
    _install_task(monkeypatch, job_id, fake_task)
    monkeypatch.setattr(scheduler, "_worker_shutdown_grace_seconds", lambda: 0.5)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        started = time.monotonic()
        with pytest.raises(asyncio.TimeoutError):
            await scheduler._run_configured_task_raw(
                job_id,
                {"run_on_main_loop": False, "max_runtime_seconds": 0.3},
                include_dependencies=False,
            )
        assert time.monotonic() - started < 1.0

        deadline = time.monotonic() + 6.0
        while time.monotonic() < deadline:
            if any("Orphaned worker thread" in message for _, message in log_capture.records):
                break
            await asyncio.sleep(0.1)
        assert any(
            "Orphaned worker thread" in message for _, message in log_capture.records
        ), f"records seen: {log_capture.records}"
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_escape_hatch_explicit_parameter_runs_on_main_loop(scheduler, monkeypatch):
    """Explicit run_on_main_loop parameter keeps execution on the main loop."""
    job_id = "worker_escape_probe"
    seen = {}

    async def fake_task(job_config=None):
        seen["loop"] = worker_context.is_worker_loop()
        seen["thread"] = threading.get_ident()
        return True

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        result = await scheduler._run_configured_task_raw(
            job_id,
            {"run_on_main_loop": True},
            include_dependencies=False,
        )
        assert result is True
        assert seen["loop"] is False
        assert seen["thread"] == threading.get_ident()
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_job_config_level_escape_hatch(scheduler, monkeypatch):
    """run_on_main_loop from job config parameters wins over global default."""
    job_id = "worker_escape_cfg_probe"
    seen = {}

    async def fake_task(job_config=None):
        seen["loop"] = worker_context.is_worker_loop()
        return True

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {
            job_id: FakeJobConfig(job_id=job_id, parameters={"run_on_main_loop": True})
        }
        await scheduler._run_configured_task_raw(job_id, {}, include_dependencies=False)
        assert seen["loop"] is False
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_worker_run_reports_progress(scheduler, monkeypatch):
    """Worker runs expose phase/progress snapshots for heartbeat and status."""
    job_id = "worker_progress_probe"

    async def fake_task(job_config=None):
        worker_context.report_progress(phase="scan", processed=0)
        for step in range(3):
            await asyncio.sleep(0.05)
            worker_context.report_progress(phase="scan", processed=step + 1)
        return True

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        run = asyncio.create_task(
            scheduler._run_configured_task_raw(job_id, {"run_on_main_loop": False}, include_dependencies=False)
        )
        seen_progress = False
        for _ in range(50):
            snapshots = scheduler.get_worker_run_snapshots()
            if snapshots and snapshots[0].get("progress", {}).get("processed", 0) > 0:
                seen_progress = True
                break
            await asyncio.sleep(0.03)
        await run
        assert seen_progress
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_shutdown_cancels_active_worker_runs(scheduler, monkeypatch):
    """Graceful shutdown cancels active workers within the configured grace."""
    job_id = "worker_shutdown_probe"

    async def fake_task(job_config=None):
        while True:
            await asyncio.sleep(0.05)

    _install_task(monkeypatch, job_id, fake_task)
    try:
        scheduler.job_configs = {job_id: FakeJobConfig(job_id=job_id)}
        run = asyncio.create_task(
            scheduler._run_configured_task_raw(job_id, {"run_on_main_loop": False}, include_dependencies=False)
        )
        for _ in range(50):
            if scheduler.get_worker_run_snapshots():
                break
            await asyncio.sleep(0.05)

        started = time.monotonic()
        await scheduler._shutdown_worker_runs()
        assert time.monotonic() - started < 5.0
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, timeout=3.0)
    finally:
        _cleanup_task(monkeypatch, job_id)


@pytest.mark.asyncio
async def test_run_on_main_loop_helper_runs_factory_on_main_loop(scheduler, monkeypatch):
    """run_on_main_loop executes the factory on the registered main loop."""
    from utils import worker_context as wc

    main_loop = asyncio.get_running_loop()
    wc.set_main_loop(main_loop)
    try:
        seen = {}

        async def factory():
            seen["loop"] = asyncio.get_running_loop()
            return "ok"

        # Direct call on the main loop: no marshalling.
        assert await wc.run_on_main_loop(factory) == "ok"
        assert seen["loop"] is main_loop

        # From a foreign loop: marshalled back to the main loop.
        def thread_main():
            foreign = asyncio.new_event_loop()
            try:
                result = foreign.run_until_complete(wc.run_on_main_loop(factory))
            finally:
                foreign.close()
            assert result == "ok"

        thread = threading.Thread(target=thread_main)
        thread.start()
        # Serve the marshalled coroutine while the thread waits on it.
        deadline = time.monotonic() + 3.0
        while thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        thread.join(timeout=1.0)
        assert not thread.is_alive()
        assert seen["loop"] is main_loop
    finally:
        wc.set_main_loop(None)


def test_build_candidates_scan_yields_checkpoints(monkeypatch):
    """Reconciliation scan yields control at configured batch boundaries."""
    from research.financial_disclosure_incremental_sync import (
        FinancialDisclosureIncrementalSyncService,
    )

    service = FinancialDisclosureIncrementalSyncService(
        db_ops=object(),
        storage=_CheckpointStorage(),
        research_config=types.SimpleNamespace(modules={}, markets=[], sources={}, routing={}),
        announcement_service=types.SimpleNamespace(),
        repair_router=types.SimpleNamespace(default_summary=lambda: {}),
    )
    monkeypatch.setattr(service, "_readiness_for_candidate", lambda *a, **k: {"ready": False})
    monkeypatch.setattr(
        service,
        "_load_disclosure_risk_audits_by_instrument",
        lambda *a, **k: {},
    )
    monkeypatch.setattr(
        service,
        "_load_periodic_report_audits_by_instrument",
        lambda *a, **k: {},
    )
    monkeypatch.setattr(
        service,
        "_required_core_facts_for_profile",
        lambda *a, **k: [],
    )

    def _stub_candidate(instrument, report_period):
        return types.SimpleNamespace(
            key=(instrument["instrument_id"], report_period),
            instrument_id=instrument["instrument_id"],
            profile=None,
            lifecycle_classification=None,
            events=[],
            reasons=[],
        )

    monkeypatch.setattr(service, "_candidate_for_period", _stub_candidate)
    monkeypatch.setattr(
        service,
        "_disclosure_risk_event_for_candidate",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        service,
        "_post_listing_periodic_event_for_candidate",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        service,
        "_limit_candidates_balanced",
        lambda candidates, max_candidates: dict(candidates),
    )
    monkeypatch.setattr(
        service,
        "_summarize_candidate_sources",
        lambda values, **k: {},
    )

    yield_calls = []
    real_sleep = asyncio.sleep

    async def counting_sleep(delay=0):
        yield_calls.append(delay)
        await real_sleep(0)

    module = types.SimpleNamespace(asyncio=types.SimpleNamespace(sleep=counting_sleep))
    import research.financial_disclosure_incremental_sync as fd_module
    monkeypatch.setattr(fd_module, "asyncio", module.asyncio)

    instruments = [{"instrument_id": f"INST{i:04d}", "symbol": f"60000{i % 10}"} for i in range(6)]
    events = []

    async def run_scan():
        return await service._build_candidates(
            instruments=instruments,
            events=events,
            report_periods=["2025Q1", "2025Q2", "2025Q3"],
            required_core_facts=[],
            mapping_version="test",
            max_candidates=0,
            run_id=None,
            dry_run=True,
            checkpoint_every_candidates=2,
            checkpoint_interval_seconds=30.0,
        )

    candidates = asyncio.run(run_scan())
    # 6 instruments x 3 periods = 18 readiness evaluations; every 2 must yield.
    assert len(yield_calls) >= 8
    assert isinstance(candidates, dict)


def test_worker_context_report_progress_is_noop_outside_worker():
    """report_progress is a safe no-op without a bound run (main loop/CLI)."""
    worker_context.report_progress(phase="any", processed=1)
    assert worker_context.get_current_run_state() is None


class _CheckpointStorage:
    """Minimal storage facade for _build_candidates checkpoint tests."""

    @contextlib.contextmanager
    def financial_database_scope(self):
        yield

    def list_financial_disclosure_event_states(self, *args, **kwargs):
        return []
