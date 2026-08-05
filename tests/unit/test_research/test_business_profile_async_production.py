import asyncio
import threading
import time
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager, _derive_business_profile_bootstrap_start
from research.business_profile_async_production import (
    BusinessProfileAsyncProductionService,
    BusinessProfileWorkRepository,
    BusinessProfileWriteCoordinator,
    StageBudget,
    get_business_profile_write_coordinator,
)
from research.business_profile_production_operations import (
    BusinessProfileAnnouncementFrontierRepository,
)
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
    _quotes,
)


def _frontier(storage):
    repository = BusinessProfileAnnouncementFrontierRepository(storage)
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }
    repository.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-2024",
            "某公司2024年年度报告",
            published_at="2025-03-20T08:00:00+08:00",
        ),
    )
    repository.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-2025",
            "某公司2025年年度报告",
            published_at="2026-03-20T08:00:00+08:00",
        ),
    )
    repository.upsert_record(
        instrument=instrument,
        record=_announcement(
            "semi-2026",
            "某公司2026年半年度报告",
            published_at="2026-08-20T08:00:00+08:00",
        ),
    )
    return repository, instrument


def test_latest_annual_enqueue_is_idempotent_and_excludes_semiannual(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )

    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    second = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )

    assert first["inserted"] == 1
    assert second["reused"] == 1
    with storage.get_connection() as conn:
        rows = conn.execute(
            "SELECT announcement_id, policy, stage, status "
            "FROM business_profile_work_items"
        ).fetchall()
    assert [dict(row) for row in rows] == [
        {
            "announcement_id": "annual-2025",
            "policy": "latest_annual_only",
            "stage": "acquire",
            "status": "pending",
        }
    ]


def test_latest_annual_enqueue_applies_company_and_date_scope(tmp_path):
    storage = _storage(tmp_path)
    frontier, _instrument = _frontier(storage)
    second = {
        "instrument_id": "000001.SZ",
        "symbol": "000001",
        "exchange": "SZSE",
    }
    frontier.upsert_record(
        instrument=second,
        record=_announcement(
            "second-2024",
            "第二公司2024年年度报告",
            published_at="2025-04-10T08:00:00+08:00",
        ),
    )
    frontier.upsert_record(
        instrument=second,
        record=_announcement(
            "second-2025",
            "第二公司2025年年度报告",
            published_at="2026-04-10T08:00:00+08:00",
        ),
    )
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        instrument_ids=("600000.SH",),
        start_date="2025-01-01",
        end_date="2025-12-31",
    )

    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT instrument_id, announcement_id FROM business_profile_work_items"
        ).fetchone()
    assert dict(row) == {
        "instrument_id": "600000.SH",
        "announcement_id": "annual-2024",
    }


def test_latest_annual_excludes_records_without_a_known_availability_date(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_announcement_frontier SET published_at = NULL "
            "WHERE announcement_id = 'annual-2025'"
        )
        conn.commit()
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )

    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        announcement_id = conn.execute(
            "SELECT announcement_id FROM business_profile_work_items"
        ).fetchone()[0]
    assert announcement_id == "annual-2024"


def test_force_requeues_terminal_item_without_changing_work_identity(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        max_attempts=1,
    )
    claimed = queue.claim(
        "acquire",
        limit=1,
        lease_owner="worker",
        lease_seconds=30,
    )[0]
    queue.fail(
        claimed["work_id"],
        lease_owner="worker",
        error="permanent",
        retryable=False,
    )

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        max_attempts=1,
        force=True,
    )

    assert result["reset"] == 1
    assert result["inserted"] == 0
    with storage.get_connection() as conn:
        rows = conn.execute(
            "SELECT work_id, stage, status FROM business_profile_work_items"
        ).fetchall()
    assert [dict(row) for row in rows] == [
        {"work_id": claimed["work_id"], "stage": "acquire", "status": "pending"}
    ]


def test_correction_supersedes_unstarted_original_work(tmp_path):
    storage = _storage(tmp_path)
    frontier, instrument = _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-03-30",
        processing_identity={"rules": "v1"},
    )

    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-2025-corrected",
            "某公司2025年年度报告（更正后）",
            published_at="2026-04-02T08:00:00+08:00",
        ),
    )
    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-04-03",
        processing_identity={"rules": "v1"},
    )

    assert result["inserted"] == 1
    assert result["superseded"] == 1
    with storage.get_connection() as conn:
        statuses = {
            row["announcement_id"]: row["status"]
            for row in conn.execute(
                "SELECT announcement_id, status FROM business_profile_work_items"
            ).fetchall()
        }
    assert statuses == {
        "annual-2025": "superseded",
        "annual-2025-corrected": "pending",
    }

    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET status = 'retry_due' "
            "WHERE announcement_id = 'annual-2025'"
        )
        conn.commit()
    repeated = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-04-03",
        processing_identity={"rules": "v1"},
    )
    assert repeated["reused"] == 1
    assert repeated["superseded"] == 1


def test_known_correction_prevents_later_original_from_being_enqueued(tmp_path):
    storage = _storage(tmp_path)
    frontier = BusinessProfileAnnouncementFrontierRepository(storage)
    instrument = {
        "instrument_id": "600000.SH",
        "symbol": "600000",
        "exchange": "SSE",
    }
    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-2025-corrected",
            "某公司2025年年度报告（修订版）",
            published_at="2026-04-02T08:00:00+08:00",
        ),
    )
    frontier.upsert_record(
        instrument=instrument,
        record=_announcement(
            "annual-2025",
            "某公司2025年年度报告",
            published_at="2026-04-03T08:00:00+08:00",
        ),
    )
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-04-04",
        processing_identity={"rules": "v1"},
    )

    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        rows = conn.execute(
            "SELECT announcement_id, status FROM business_profile_work_items"
        ).fetchall()
    assert [dict(row) for row in rows] == [
        {"announcement_id": "annual-2025-corrected", "status": "pending"}
    ]


def test_claim_acknowledge_and_retry_are_durable(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        max_attempts=2,
    )

    claimed = queue.claim(
        "acquire",
        limit=1,
        lease_owner="worker-1",
        lease_seconds=30,
    )
    assert claimed[0]["status"] == "running"
    queue.acknowledge(
        claimed[0]["work_id"],
        lease_owner="worker-1",
        result={"status": "success"},
    )
    parsed = queue.claim(
        "parse",
        limit=1,
        lease_owner="worker-2",
        lease_seconds=30,
    )
    assert parsed[0]["attempt_count"] == 1
    assert (
        queue.fail(
            parsed[0]["work_id"],
            lease_owner="worker-2",
            error="timeout",
            retryable=True,
        )
        == "retry_due"
    )
    with storage.get_connection() as conn:
        conn.execute("UPDATE business_profile_work_items SET next_attempt_at = NULL")
        conn.commit()
    retried = queue.claim(
        "parse",
        limit=1,
        lease_owner="worker-3",
        lease_seconds=30,
    )
    assert retried[0]["attempt_count"] == 2
    assert (
        queue.fail(
            retried[0]["work_id"],
            lease_owner="worker-3",
            error="timeout",
            retryable=True,
        )
        == "terminal_failure"
    )
    assert queue.health()["terminal"] == 1


def test_expired_worker_cannot_acknowledge_reclaimed_lease(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    claimed = queue.claim(
        "acquire",
        limit=1,
        lease_owner="worker-old",
        lease_seconds=30,
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET lease_owner = 'worker-new' "
            "WHERE work_id = ?",
            (claimed[0]["work_id"],),
        )
        conn.commit()

    with pytest.raises(RuntimeError, match="acknowledgement conflict"):
        queue.acknowledge(
            claimed[0]["work_id"],
            lease_owner="worker-old",
            result={"status": "success"},
        )


def test_scoped_backfill_honors_end_date_against_existing_frontier(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )

    result = queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        instrument_ids=("600000.SH",),
        end_date="2025-12-31",
    )

    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        announcement_ids = [
            row["announcement_id"]
            for row in conn.execute(
                "SELECT announcement_id FROM business_profile_work_items"
            ).fetchall()
        ]
    assert announcement_ids == ["annual-2024"]


def test_daily_discovery_runs_before_semantic_backpressure(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', status = 'pending'"
        )
        conn.commit()
    calls = []

    async def discover(**_kwargs):
        calls.append("discover")
        return {"status": "success", "selected_announcements": 0}

    async def stage_runner(stage, _item):
        calls.append(stage)
        return {"status": "success"}

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=discover,
        stage_runner=stage_runner,
    )

    report = asyncio.run(
        service.run_daily(
            knowledge_cutoff="2026-08-30",
            processing_identity={"rules": "v1"},
            discovery_kwargs={},
            stage_budgets={
                "acquire": StageBudget(max_items=1, high_water_mark=1),
                "semantic": StageBudget(max_items=1, high_water_mark=1),
            },
        )
    )

    assert calls[0] == "discover"
    assert report["workers"]["acquire"]["status"] == "backpressured"
    assert report["workers"]["semantic"]["completed"] == 1


def test_backfill_discovery_failure_does_not_block_existing_queue(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "automatic"},
    )

    async def discover(**_kwargs):
        raise TimeoutError("provider timeout")

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=discover,
        stage_runner=AsyncMock(return_value={"status": "success"}),
    )
    report = asyncio.run(
        service.run_backfill(
            knowledge_cutoff="2026-08-30",
            processing_identity={"rules": "backfill"},
            instrument_ids=("600000.SH",),
            discovery_kwargs={"start_date": "2026-08-01"},
            stage_budgets={
                "acquire": StageBudget(max_items=1, max_concurrency=1),
            },
        )
    )

    assert report["status"] == "degraded"
    assert report["discovery"]["status"] == "failed"
    assert report["workers"]["acquire"]["completed"] == 1


def test_stage_stop_request_finishes_inflight_batch_before_next_claim():
    repository = Mock()
    first = {"work_id": "work-1"}
    second = {"work_id": "work-2"}
    repository.claim.side_effect = [[first], [second]]
    repository.acknowledge.return_value = "completed"
    stop = False

    async def stage_runner(_stage, _item):
        nonlocal stop
        stop = True
        return {"status": "success"}

    service = BusinessProfileAsyncProductionService(
        repository=repository,
        discovery_runner=AsyncMock(),
        stage_runner=stage_runner,
        write_coordinator=BusinessProfileWriteCoordinator(inter_write_seconds=0),
    )
    result = asyncio.run(
        service._drain_stage(
            "acquire",
            StageBudget(max_items=2, max_concurrency=1),
            should_stop=lambda: stop,
        )
    )

    assert result["status"] == "stopped"
    assert result["claimed"] == 1
    assert result["completed"] == 1
    repository.claim.assert_called_once()
    repository.acknowledge.assert_called_once()


def test_invalid_backfill_scope_fails_before_discovery(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    discover = AsyncMock(return_value={"status": "success"})
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=discover,
        stage_runner=AsyncMock(return_value={"status": "success"}),
    )

    with pytest.raises(ValueError, match="specialist document types"):
        asyncio.run(
            service.run_backfill(
                knowledge_cutoff="2026-08-30",
                processing_identity={"rules": "v1"},
                start_date="2026-01-01",
                document_types=("resource_report",),
                discovery_kwargs={"start_date": "2026-01-01"},
                selection_policy="latest_annual_only",
            )
        )
    discover.assert_not_awaited()


def test_stage_consumers_run_independently_without_download_blocking_parse(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "download"},
    )
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "parse"},
        instrument_ids=("600000.SH",),
        document_types=("annual_report",),
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'parse' WHERE work_id = ("
            "SELECT work_id FROM business_profile_work_items "
            "WHERE policy = 'expanded' LIMIT 1)"
        )
        conn.commit()

    parse_started = asyncio.Event()

    async def stage_runner(stage, _item):
        if stage == "acquire":
            await asyncio.wait_for(parse_started.wait(), timeout=1)
        elif stage == "parse":
            parse_started.set()
        return {"status": "success"}

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
    )
    result = asyncio.run(
        asyncio.wait_for(
            service._run_workers(
                {
                    "acquire": StageBudget(max_items=1, max_concurrency=1),
                    "parse": StageBudget(max_items=1, max_concurrency=1),
                }
            ),
            timeout=2,
        )
    )

    assert result["acquire"]["completed"] == 1
    assert result["parse"]["completed"] == 1


def test_parse_compute_overlaps_while_sqlite_writes_remain_serial(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    for identity in ("first", "second"):
        queue.enqueue_latest_annual(
            knowledge_cutoff="2026-08-30",
            processing_identity={"rules": identity},
        )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'parse', status = 'pending'"
        )
        conn.commit()

    coordinator = BusinessProfileWriteCoordinator(inter_write_seconds=0)
    compute_lock = threading.Lock()
    active_compute = 0
    max_active_compute = 0

    async def stage_runner(_stage, item):
        nonlocal active_compute, max_active_compute
        with compute_lock:
            active_compute += 1
            max_active_compute = max(max_active_compute, active_compute)
        await asyncio.sleep(0.03)
        with compute_lock:
            active_compute -= 1

        def persist_result():
            with storage.coordinated_writes(coordinator):
                with storage.get_connection() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "UPDATE business_profile_work_items SET last_error = ? "
                        "WHERE work_id = ?",
                        ("parsed", item["work_id"]),
                    )
                    time.sleep(0.02)
                    conn.commit()

        await asyncio.to_thread(persist_result)
        return {"status": "success"}

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
        write_coordinator=coordinator,
    )
    report = asyncio.run(
        service._run_workers({"parse": StageBudget(max_items=2, max_concurrency=2)})
    )

    assert report["parse"]["completed"] == 2
    assert max_active_compute == 2
    assert coordinator.snapshot()["max_active_writers"] == 1
    assert coordinator.snapshot()["write_transactions"] >= 5


def test_write_coordinator_is_shared_per_storage_manager(tmp_path):
    storage = _storage(tmp_path)

    first = get_business_profile_write_coordinator(
        storage,
        inter_write_seconds=0.01,
    )
    second = get_business_profile_write_coordinator(
        storage,
        inter_write_seconds=0.5,
    )

    assert first is second
    assert second.inter_write_seconds == 0.01


def test_data_manager_daily_advances_each_stage_without_draining_globally(tmp_path):
    storage = _storage(tmp_path)
    _quotes(
        storage,
        [
            (
                "600000.SH",
                "600000",
                "某公司",
                "SSE",
                "stock",
                "2000-01-01",
                None,
                "active",
                1,
            )
        ],
    )
    _frontier(storage)
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = Mock(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"promotion_enabled": False},
                "production_operations": {
                    "async_production_enabled": True,
                    "discovery_enabled": True,
                    "checkpoint_root": str(tmp_path / "checkpoints"),
                },
            }
        },
    )
    manager.run_business_profile_index_discovery = AsyncMock(
        return_value={"status": "success", "selected_announcements": 0}
    )
    manager.run_business_profile_semantic_production = AsyncMock(
        return_value={"status": "success"}
    )

    result = asyncio.run(
        manager.run_business_profile_daily_incremental(
            knowledge_cutoff="2026-08-30",
            field_families=["atomic_activities"],
            runtime_identities={"rules": "v1"},
            stage_budgets={
                stage: {
                    "max_items": 1,
                    "max_concurrency": 1,
                    "max_elapsed_seconds": 30,
                    "high_water_mark": 100,
                }
                for stage in ("acquire", "parse", "semantic", "publish")
            },
        )
    )

    assert result["status"] == "success"
    queue_group = result["queue_health"]["groups"][0]
    assert queue_group["stage"] == "publish"
    assert queue_group["status"] == "completed"
    assert queue_group["row_count"] == 1
    assert result["queue_health"]["terminal"] == 0
    assert result["queue_health"]["completed"] == 1
    assert result["queue_health"]["finalized"] == 1
    assert [
        call.kwargs["mode"]
        for call in manager.run_business_profile_semantic_production.await_args_list
    ] == ["plan", "select", "extract", "verify"]
    manager.run_business_profile_index_discovery.assert_awaited_once()
    assert manager.run_business_profile_index_discovery.await_args.kwargs[
        "write_coordinator"
    ] is get_business_profile_write_coordinator(storage)


def test_latest_annual_bootstrap_derives_current_filing_year_only_when_unscoped():
    assert _derive_business_profile_bootstrap_start(
        knowledge_cutoff="2026-08-05",
        selection_policy="latest_annual_only",
        instrument_ids=(),
        start_date=None,
    ) == ("2026-01-01", True)
    assert _derive_business_profile_bootstrap_start(
        knowledge_cutoff="2026-08-05",
        selection_policy="latest_annual_only",
        instrument_ids=(),
        start_date="2025-01-01",
    ) == ("2025-01-01", False)


def test_data_manager_publish_does_not_complete_when_promotion_fails(tmp_path):
    storage = _storage(tmp_path)
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.run_business_profile_semantic_production = AsyncMock(
        side_effect=[
            {"status": "success", "pipeline_status": "completed"},
            {"status": "failed", "reason": "manifest_mismatch"},
        ]
    )

    service, _identity = manager._build_business_profile_async_service(
        cutoff="2026-08-30",
        configured_families=("atomic_activities",),
        identities={"rules": "v1"},
        operations={"checkpoint_root": str(tmp_path / "checkpoints")},
        semantic={"promotion_enabled": True},
        default_exchanges=("SSE",),
    )
    result = asyncio.run(
        service.stage_runner(
            "publish",
            {
                "instrument_id": "600000.SH",
                "checkpoint_path": str(tmp_path / "checkpoint.json"),
                "policy": "latest_annual_only",
            },
        )
    )

    assert result["status"] == "failed"
    assert result["reason"] == "business_profile_promotion_failed"
    assert result["promotion"]["reason"] == "manifest_mismatch"
