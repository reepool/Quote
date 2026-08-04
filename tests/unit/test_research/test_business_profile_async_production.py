import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager
from research.business_profile_async_production import (
    BusinessProfileAsyncProductionService,
    BusinessProfileWorkRepository,
    StageBudget,
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
        conn.execute(
            "UPDATE business_profile_work_items SET next_attempt_at = NULL"
        )
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
    assert [
        call.kwargs["mode"]
        for call in manager.run_business_profile_semantic_production.await_args_list
    ] == ["plan", "select", "extract", "verify"]
    manager.run_business_profile_index_discovery.assert_awaited_once()


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
