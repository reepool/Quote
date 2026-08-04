import asyncio

from research.business_profile_backfill_control import (
    BusinessProfileBackfillControlStore,
    ContinuousBackfillOptions,
    ContinuousBusinessProfileBackfillRunner,
)


def _cycle_result(
    *,
    phase_ready=False,
    claimable=0,
    running=0,
    terminal=0,
    completed=0,
):
    return {
        "status": "success",
        "discovery": {
            "status": "success",
            "discovery_window_backlog": 0,
            "frontier_inserted": 0,
            "frontier_changed": 0,
        },
        "enqueue": {"inserted": 0, "reused": 1},
        "workers": {
            "acquire": {
                "status": "success",
                "claimed": completed,
                "completed": completed,
                "retried": 0,
                "terminal_failures": 0,
                "lease_conflicts": 0,
            }
        },
        "queue_health": {
            "claimable": claimable,
            "running": running,
            "terminal": terminal,
        },
        "rollout_readiness": {
            "phase_ready": phase_ready,
            "phase_reason_codes": [] if phase_ready else ["claimable_work_remaining"],
            "current_annual_coverage_ratio": 1.0,
        },
    }


def test_control_store_is_atomic_and_stale_stop_does_not_target_restart(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    control = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    assert control._lock is store._lock
    first = store.begin(
        run_id="run-1",
        mode="continuous",
        phase="structured_shadow",
        parameters={"continuous": True},
    )
    assert first["state"] == "running"
    stopped = control.request_stop(reason="test")
    assert stopped["status"] == "stop_requested"
    assert store.should_stop("run-1")["reason"] == "test"

    second = store.begin(
        run_id="run-2",
        mode="continuous",
        phase="structured_shadow",
        parameters={"continuous": True},
    )
    assert second["superseded_run_id"] == "run-1"
    assert store.should_stop("run-2") is None
    assert not list(store.control_root.glob("*.tmp"))


def test_control_store_reports_not_started_and_does_not_create_wildcard_stop(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")

    assert store.status()["state"] == "not_started"
    response = store.request_stop(reason="test")

    assert response["status"] == "not_running"
    assert response["target_run_id"] is None
    assert not store.stop_path.exists()


def test_continuous_runner_stops_when_active_phase_is_ready(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    runner = ContinuousBusinessProfileBackfillRunner(
        store,
        options=ContinuousBackfillOptions(
            poll_interval_seconds=0,
            max_idle_cycles=3,
            heartbeat_interval_seconds=1,
        ),
    )
    calls = 0

    async def run_cycle(_should_stop):
        nonlocal calls
        calls += 1
        return _cycle_result(phase_ready=True, completed=1)

    progress = asyncio.run(
        runner.run(
            run_id="ready-run",
            phase="structured_shadow",
            parameters={"continuous": True},
            run_cycle=run_cycle,
        )
    )
    assert calls == 1
    assert progress["state"] == "completed"
    assert progress["reason_codes"] == ["active_phase_ready"]
    assert progress["cumulative_workers"]["acquire"]["completed"] == 1


def test_continuous_runner_observes_targeted_stop_inside_cycle(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    runner = ContinuousBusinessProfileBackfillRunner(
        store,
        options=ContinuousBackfillOptions(
            poll_interval_seconds=0,
            max_idle_cycles=3,
            heartbeat_interval_seconds=1,
        ),
    )

    async def run_cycle(should_stop):
        response = store.request_stop(reason="test_stop")
        assert response["target_run_id"] == "stop-run"
        assert should_stop() is True
        return {**_cycle_result(), "status": "stopped"}

    progress = asyncio.run(
        runner.run(
            run_id="stop-run",
            phase="structured_shadow",
            parameters={"continuous": True},
            run_cycle=run_cycle,
        )
    )
    assert progress["state"] == "stopped"
    assert progress["reason_codes"] == ["operator_stop_requested"]


def test_continuous_runner_blocks_after_no_progress_limit(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    runner = ContinuousBusinessProfileBackfillRunner(
        store,
        options=ContinuousBackfillOptions(
            poll_interval_seconds=0,
            max_idle_cycles=2,
            heartbeat_interval_seconds=1,
        ),
    )

    async def run_cycle(_should_stop):
        return _cycle_result(phase_ready=False, claimable=0)

    progress = asyncio.run(
        runner.run(
            run_id="idle-run",
            phase="structured_shadow",
            parameters={"continuous": True},
            run_cycle=run_cycle,
        )
    )
    assert progress["state"] == "blocked"
    assert progress["cycle"] == 2
    assert "no_progress_limit_reached" in progress["reason_codes"]


def test_continuous_runner_waits_for_running_lease_before_counting_idle(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    runner = ContinuousBusinessProfileBackfillRunner(
        store,
        options=ContinuousBackfillOptions(
            poll_interval_seconds=0,
            max_idle_cycles=2,
            max_cycles=2,
            heartbeat_interval_seconds=1,
        ),
    )

    async def run_cycle(_should_stop):
        return _cycle_result(phase_ready=False, running=1)

    progress = asyncio.run(
        runner.run(
            run_id="leased-run",
            phase="structured_shadow",
            parameters={"continuous": True},
            run_cycle=run_cycle,
        )
    )

    assert progress["state"] == "stopped"
    assert progress["cycle"] == 2
    assert progress["idle_cycles"] == 0
    assert progress["reason_codes"] == ["cycle_limit_reached"]


def test_continuous_runner_stops_on_not_ready_cycle(tmp_path):
    store = BusinessProfileBackfillControlStore(tmp_path / "checkpoints")
    runner = ContinuousBusinessProfileBackfillRunner(
        store,
        options=ContinuousBackfillOptions(
            poll_interval_seconds=0,
            max_idle_cycles=3,
            heartbeat_interval_seconds=1,
        ),
    )
    calls = 0

    async def run_cycle(_should_stop):
        nonlocal calls
        calls += 1
        return {
            **_cycle_result(),
            "status": "not_ready",
            "reason": "rollout_not_ready",
        }

    progress = asyncio.run(
        runner.run(
            run_id="not-ready-run",
            phase="structured_shadow",
            parameters={"continuous": True},
            run_cycle=run_cycle,
        )
    )

    assert calls == 1
    assert progress["state"] == "blocked"
    assert progress["reason_codes"] == ["rollout_not_ready"]
