import asyncio
import io
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from data_manager import DataManager, _derive_business_profile_bootstrap_start
from research.business_profile_async_production import (
    BusinessProfileAsyncProductionService,
    BusinessProfileFrontierBoundAcquirer,
    BusinessProfileWorkRepository,
    BusinessProfileWriteCoordinator,
    StageBudget,
    _business_profile_operation_status,
    _migrate_unfinished_legacy_publish_items,
    get_business_profile_write_coordinator,
)
from research.business_profile_production_operations import (
    BusinessProfileAnnouncementFrontierRepository,
    register_business_profile_shared_annual_report_asset,
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


def _processing_identity(model: str = "model-v1"):
    return {
        "field_families": ("structured_segments", "tabular_operating_facts"),
        "runtime_identities": {
            "model": model,
            "parser": "parser-v1",
            "rules": "rules-v1",
        },
        "promotion_manifest_hashes": {},
    }


def _configure_empty_shared_assets(manager, tmp_path):
    shared = SimpleNamespace(
        repository=SimpleNamespace(),
        list_effective_assets=Mock(return_value={"items": [], "returned": 0}),
    )
    manager.research_config.storage = SimpleNamespace(
        db_path=str(tmp_path / "research.db")
    )
    manager._get_announcement_asset_access = Mock(return_value=shared)
    return shared


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
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    item = queue.get(work_id)
    assert item["metadata"]["knowledge_cutoff"] == "2026-08-30"


def test_enqueue_current_identity_supersedes_obsolete_failed_work(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    obsolete = _processing_identity("model-obsolete")
    current = _processing_identity("model-current")
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=obsolete,
    )
    with storage.get_connection() as conn:
        obsolete_work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', "
            "status = 'terminal_failure', last_error = ? WHERE work_id = ?",
            (
                "ValueError: stale semantic production checkpoint scope",
                obsolete_work_id,
            ),
        )
        conn.commit()

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=current,
    )

    assert result["inserted"] == 1
    assert result["identity_superseded"] == 1
    assert queue.get(obsolete_work_id)["status"] == "superseded"
    with storage.get_connection() as conn:
        groups = conn.execute(
            "SELECT status, COUNT(*) FROM business_profile_work_items "
            "GROUP BY status ORDER BY status"
        ).fetchall()
    assert [tuple(row) for row in groups] == [("pending", 1), ("superseded", 1)]


def test_new_runtime_identity_reprocesses_completed_coverage(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-obsolete"),
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'publish', "
            "status = 'completed'"
        )
        conn.commit()

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-current"),
    )

    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT metadata_json FROM business_profile_work_items "
            "WHERE status = 'pending'"
        ).fetchone()
    assert json.loads(row["metadata_json"])["reprocess_complete_coverage"] is True


def test_newer_latest_annual_supersedes_older_terminal_work(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    obsolete = _processing_identity("model-obsolete")
    queue.enqueue_latest_annual(
        knowledge_cutoff="2025-12-31",
        processing_identity=obsolete,
    )
    with storage.get_connection() as conn:
        old_work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
        conn.execute(
            "UPDATE business_profile_work_items SET status = 'terminal_failure', "
            "last_error = 'obsolete annual report failed' WHERE work_id = ?",
            (old_work_id,),
        )
        conn.commit()

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-current"),
    )

    assert result["inserted"] == 1
    assert result["superseded"] == 1
    assert queue.get(old_work_id)["status"] == "superseded"


def test_enqueue_current_identity_does_not_supersede_running_work(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-obsolete"),
    )
    running = queue.claim(
        "acquire", limit=1, lease_owner="worker-old", lease_seconds=300
    )[0]

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-current"),
    )

    assert result["identity_superseded"] == 0
    assert queue.get(running["work_id"])["status"] == "running"


def test_enqueue_current_identity_supersedes_expired_obsolete_lease(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-obsolete"),
    )
    running = queue.claim(
        "acquire", limit=1, lease_owner="worker-old", lease_seconds=300
    )[0]
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items "
            "SET lease_expires_at = '2020-01-01T00:00:00+08:00' WHERE work_id = ?",
            (running["work_id"],),
        )
        conn.commit()

    result = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-current"),
    )

    assert result["identity_superseded"] == 1
    assert queue.get(running["work_id"])["status"] == "superseded"


def test_claim_filters_work_by_processing_identity(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    current = _processing_identity("model-current")
    obsolete = _processing_identity("model-obsolete")
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=obsolete,
    )
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity=current,
        instrument_ids=("600000.SH",),
        document_types=("annual_report",),
    )
    with storage.get_connection() as conn:
        current_hash = conn.execute(
            "SELECT processing_identity_hash FROM business_profile_work_items "
            "WHERE policy = 'expanded'"
        ).fetchone()[0]

    claimed = queue.claim(
        "acquire",
        limit=3,
        lease_owner="worker",
        lease_seconds=30,
        processing_identity_hash=current_hash,
    )

    assert len(claimed) == 2
    assert {item["policy"] for item in claimed} == {"expanded"}


def test_claimable_depth_filters_obsolete_processing_identity(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-obsolete"),
    )
    current = _processing_identity("model-current")
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity=current,
        instrument_ids=("600000.SH",),
        document_types=("annual_report",),
    )
    with storage.get_connection() as conn:
        conn.execute("UPDATE business_profile_work_items SET stage = 'semantic'")
        current_hash = conn.execute(
            "SELECT processing_identity_hash FROM business_profile_work_items "
            "WHERE policy = 'expanded'"
        ).fetchone()[0]
        conn.commit()

    assert queue.claimable_count("semantic") == 3
    assert queue.claimable_count("semantic", processing_identity_hash=current_hash) == 2


def test_queue_health_filters_obsolete_processing_identity(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-obsolete"),
    )
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("model-current"),
        instrument_ids=("600000.SH",),
        document_types=("annual_report",),
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET status = 'terminal_failure' "
            "WHERE policy = 'latest_annual_only'"
        )
        current_hash = conn.execute(
            "SELECT processing_identity_hash FROM business_profile_work_items "
            "WHERE policy = 'expanded'"
        ).fetchone()[0]
        conn.commit()

    assert queue.health()["terminal"] == 1
    current_health = queue.health(processing_identity_hash=current_hash)
    assert current_health["terminal"] == 0
    assert current_health["claimable"] == 2


def test_queue_health_filters_other_instruments_with_same_identity(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    identity = _processing_identity("model-current")
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
        instrument_ids=("600000.SH",),
        document_types=("annual_report",),
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items "
            "SET status = 'terminal_failure', instrument_id = '000001.SZ' "
            "WHERE policy = 'latest_annual_only'"
        )
        identity_hash = conn.execute(
            "SELECT processing_identity_hash FROM business_profile_work_items "
            "WHERE policy = 'expanded'"
        ).fetchone()[0]
        conn.commit()

    scoped = queue.health(
        processing_identity_hash=identity_hash,
        instrument_ids=("600000.SH",),
    )

    assert scoped["terminal"] == 0
    assert scoped["claimable"] == 2


def test_claimable_probe_obeys_due_time_expired_lease_and_exclusions(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "probe"},
    )
    claimed = queue.claim(
        "acquire",
        limit=1,
        lease_owner="probe-worker",
        lease_seconds=300,
    )[0]

    assert (
        queue.has_claimable(
            "acquire",
            exclude_work_ids=(claimed["work_id"],),
        )
        is False
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET "
            "status = 'retry_due', next_attempt_at = '2999-01-01T00:00:00+08:00'"
        )
        conn.commit()
    assert queue.has_claimable("acquire") is False
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET status = 'running', "
            "lease_expires_at = '2020-01-01T00:00:00+08:00' WHERE work_id = ?",
            (claimed["work_id"],),
        )
        conn.commit()
    assert queue.has_claimable("acquire") is True
    assert (
        queue.has_claimable(
            "acquire",
            exclude_work_ids=(claimed["work_id"],),
        )
        is False
    )


def test_claim_prioritizes_work_waiting_in_stage_before_fresh_recovery(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("ordinary"),
    )
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity=_processing_identity("recovered"),
        instrument_ids=("600000.SH",),
        document_types=("annual_report",),
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', "
            "status = 'pending', updated_at = CASE policy "
            "WHEN 'latest_annual_only' THEN '2026-08-01T00:00:00+08:00' "
            "ELSE '2026-08-02T00:00:00+08:00' END"
        )
        conn.commit()

    claimed = queue.claim("semantic", limit=1, lease_owner="worker", lease_seconds=30)

    assert claimed[0]["policy"] == "latest_annual_only"


def test_shared_non_effective_business_profile_filing_returns_metadata_only(
    tmp_path,
):
    storage = _storage(tmp_path)
    _frontier(storage)

    class _SharedAccess:
        repository = SimpleNamespace(get_effective_report=lambda *args, **kwargs: None)

        def get_effective_asset(self, *_args, **_kwargs):
            return None

        def ensure(self, request):
            assert request.allow_network is False
            return {
                "availability": "superseded",
                "asset": {
                    "source": request.source,
                    "source_announcement_id": request.source_announcement_id,
                },
                "reason_code": "non_effective_exact_filing_content_unavailable",
            }

    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
        shared_asset_access=_SharedAccess(),
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    item = queue.get(work_id)
    acquirer = BusinessProfileFrontierBoundAcquirer(
        repository=queue,
    )

    result = acquirer.acquire(item)

    assert result["status"] == "metadata_only"
    assert result["asset_availability"] == "superseded"
    assert result["local_content_unavailable"] is True
    assert result["reason_code"] == ("non_effective_exact_filing_content_unavailable")


def test_bound_shared_asset_does_not_drift_to_later_effective_correction(tmp_path):
    storage = _storage(tmp_path)
    original = {
        "asset_id": "asset-original-2025",
        "instrument_id": "600000.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "annual-original-2025",
        "attachment_id": "attachment-original-2025",
        "observation_version": "observation-original-2025",
        "content_hash": "a" * 64,
        "published_at": "2026-03-20T08:00:00+08:00",
        "is_correction": False,
    }
    correction = {
        **original,
        "asset_id": "asset-correction-2025",
        "source_announcement_id": "annual-correction-2025",
        "attachment_id": "attachment-correction-2025",
        "observation_version": "observation-correction-2025",
        "content_hash": "b" * 64,
        "published_at": "2026-04-20T08:00:00+08:00",
        "is_correction": True,
    }
    register_business_profile_shared_annual_report_asset(
        storage=storage,
        asset=original,
    )
    register_business_profile_shared_annual_report_asset(
        storage=storage,
        asset=correction,
    )
    exact_requests = []

    class _SharedAccess:
        class _Repository:
            @staticmethod
            def get_effective_report(*_args, **_kwargs):
                raise AssertionError(
                    "bound processing must not reselect effective asset"
                )

        repository = _Repository()

        def exact_observation_handle(self, request, *, authorized):
            assert authorized is True
            exact_requests.append(request)
            return {
                "source": request.source,
                "source_announcement_id": request.source_announcement_id,
                "attachment_id": request.attachment_id,
                "observation_version": request.observation_version,
                "content_hash": request.expected_content_hash,
                "content_length": 123,
                "path": tmp_path / "original.pdf",
                "file_handle": io.BytesIO(b"%PDF-original"),
            }

    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
        shared_asset_access=_SharedAccess(),
    )
    result = queue.enqueue_bound_annual_report_asset(
        knowledge_cutoff="2026-05-01",
        processing_identity={"rules": "v1"},
        asset=original,
    )
    assert result["inserted"] == 1
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    work = queue.get(work_id)
    assert work["announcement_id"] == "annual-original-2025"
    assert work["metadata"]["bound_shared_asset"]["asset_id"] == ("asset-original-2025")
    assert queue.get_bound_frontier(work)["status"] == "superseded"

    manifest = queue.get_bound_source_asset(work)

    assert manifest is not None
    assert manifest["source_file_id"] == "shared-asset:asset-original-2025"
    assert manifest["content_hash"] == "a" * 64
    assert len(exact_requests) == 1
    assert exact_requests[0].observation_version == "observation-original-2025"
    assert exact_requests[0].expected_content_hash == "a" * 64


def test_historical_effective_projection_opens_its_exact_observation(tmp_path):
    storage = _storage(tmp_path)
    historical = {
        "asset_id": "asset-historical-projection",
        "instrument_id": "601088.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "annual-601088-2025",
        "attachment_id": "attachment-601088-2025",
        "observation_version": "observation-601088-2025",
        "content_hash": "c" * 64,
        "published_at": "2026-03-22T08:00:00+08:00",
        "is_correction": False,
    }
    exact_requests = []

    class _SharedAccess:
        def get_effective_asset(self, instrument_id, **kwargs):
            assert instrument_id == "601088.SH"
            assert kwargs == {
                "fiscal_year": 2025,
                "knowledge_cutoff": "2026-08-20",
            }
            return historical

        def content_handle(self, _asset_id):
            raise AssertionError(
                "historical projection ids are not persisted current asset ids"
            )

        def exact_observation_handle(self, request, *, authorized):
            assert authorized is True
            exact_requests.append(request)
            return {
                "path": tmp_path / "601088-2025.pdf",
                "content_length": 456,
                "file_handle": io.BytesIO(b"%PDF-historical"),
            }

    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
        shared_asset_access=_SharedAccess(),
    )
    manifest = queue.get_bound_source_asset(
        {
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "document_type": "annual_report",
            "source": "cninfo",
            "announcement_id": "annual-601088-2025",
            "metadata": {"knowledge_cutoff": "2026-08-20"},
        }
    )

    assert manifest is not None
    assert manifest["source_asset_id"] == "asset-historical-projection"
    assert manifest["metadata"]["selector_kind"] == "bound_exact_observation"
    assert len(exact_requests) == 1
    assert exact_requests[0].observation_version == "observation-601088-2025"


def test_stale_scope_recovery_requeues_terminal_items_without_content_attempts(
    tmp_path,
):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'publish', "
            "status = 'terminal_failure', attempt_count = 3, last_error = ? "
            "WHERE work_id = ?",
            ("ValueError: stale semantic production checkpoint scope", work_id),
        )
        conn.commit()

    first = queue.recover_stale_scope_items()
    second = queue.recover_stale_scope_items()
    recovered = queue.get(work_id)

    assert first["requeued"] == 1
    assert second["requeued"] == 0
    assert recovered["status"] == "pending"
    assert recovered["stage"] == "acquire"
    assert recovered["attempt_count"] == 0
    assert ".scope-recovery-" in recovered["checkpoint_path"]
    assert "stage_results" not in recovered["metadata"]
    assert recovered["metadata"]["recovery_history"][-1]["reason"] == (
        "stale_scope_checkpoint_rotated"
    )


def test_stale_scope_recovery_preserves_checkpoint_cutoff_and_stage(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    identity = _processing_identity()
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    item = queue.claim("acquire", limit=1, lease_owner="worker", lease_seconds=30)[0]
    checkpoint = Path(item["checkpoint_path"])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps(
            {
                "scope": {
                    "instruments": ["600000.SH"],
                    "field_families": list(identity["field_families"]),
                    "knowledge_cutoff": "2026-08-01",
                    "identities": identity["runtime_identities"],
                    "promotion_manifest_hashes": {},
                },
                "completed_stages": ["plan", "select", "extract", "verify"],
                "artifacts": {"plan": "plan.json"},
            }
        ),
        encoding="utf-8",
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'publish', "
            "status = 'terminal_failure', attempt_count = 3, "
            "metadata_json = ?, last_error = ? WHERE work_id = ?",
            (
                json.dumps({"stage_results": {"parse": {"status": "success"}}}),
                "ValueError: stale semantic production checkpoint scope",
                item["work_id"],
            ),
        )
        conn.commit()

    result = queue.recover_stale_scope_items(processing_identity=identity)
    recovered = queue.get(item["work_id"])

    assert result["checkpoint_preserved"] == 1
    assert result["checkpoint_rotated"] == 0
    assert recovered["stage"] == "publish"
    assert recovered["checkpoint_path"] == str(checkpoint)
    assert recovered["metadata"]["knowledge_cutoff"] == "2026-08-01"
    assert recovered["metadata"]["stage_results"]["parse"]["status"] == "success"


def test_stale_scope_recovery_rotates_complete_but_incompatible_scope(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    identity = _processing_identity("model-current")
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    item = queue.claim("acquire", limit=1, lease_owner="worker", lease_seconds=30)[0]
    checkpoint = Path(item["checkpoint_path"])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps(
            {
                "scope": {
                    "instruments": ["600000.SH"],
                    "field_families": list(identity["field_families"]),
                    "knowledge_cutoff": "2026-08-01",
                    "identities": {
                        **identity["runtime_identities"],
                        "model": "model-obsolete",
                    },
                    "promotion_manifest_hashes": {},
                }
            }
        ),
        encoding="utf-8",
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', "
            "status = 'terminal_failure', attempt_count = 3, last_error = ? "
            "WHERE work_id = ?",
            ("ValueError: stale semantic production checkpoint scope", item["work_id"]),
        )
        conn.commit()

    result = queue.recover_stale_scope_items(processing_identity=identity)
    recovered = queue.get(item["work_id"])

    assert result["checkpoint_preserved"] == 0
    assert result["checkpoint_rotated"] == 1
    assert recovered["stage"] == "acquire"
    assert recovered["checkpoint_path"] != str(checkpoint)
    assert recovered["metadata"]["recovery_history"][-1]["reason"] == (
        "stale_scope_checkpoint_rotated"
    )


def test_stale_scope_recovery_does_not_requeue_obsolete_processing_identity(
    tmp_path,
):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    obsolete = _processing_identity("model-obsolete")
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=obsolete,
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', "
            "status = 'terminal_failure', last_error = ? WHERE work_id = ?",
            ("ValueError: stale semantic production checkpoint scope", work_id),
        )
        conn.commit()

    result = queue.recover_stale_scope_items(
        processing_identity=_processing_identity("model-current")
    )

    assert result["eligible_stale_scope_items"] == 0
    assert result["requeued"] == 0
    assert queue.get(work_id)["status"] == "terminal_failure"


def test_repeated_preserved_scope_failure_rotates_checkpoint(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    identity = _processing_identity()
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    item = queue.claim("acquire", limit=1, lease_owner="worker", lease_seconds=30)[0]
    checkpoint = Path(item["checkpoint_path"])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps(
            {
                "scope": {
                    "instruments": ["600000.SH"],
                    "field_families": list(identity["field_families"]),
                    "knowledge_cutoff": "2026-08-01",
                    "identities": identity["runtime_identities"],
                    "promotion_manifest_hashes": {},
                }
            }
        ),
        encoding="utf-8",
    )
    metadata = {
        **item["metadata"],
        "recovery_history": [
            {
                "reason": "stale_scope_cutoff_restored",
                "from_checkpoint_path": str(checkpoint),
            }
        ],
    }
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', "
            "status = 'terminal_failure', metadata_json = ?, last_error = ? "
            "WHERE work_id = ?",
            (
                json.dumps(metadata),
                "ValueError: stale semantic production checkpoint scope",
                item["work_id"],
            ),
        )
        conn.commit()

    result = queue.recover_stale_scope_items(processing_identity=identity)
    recovered = queue.get(item["work_id"])

    assert result["checkpoint_rotated"] == 1
    assert recovered["stage"] == "acquire"
    assert recovered["checkpoint_path"] != str(checkpoint)


def test_reused_work_backfills_cutoff_from_checkpoint(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    item = queue.claim("acquire", limit=1, lease_owner="worker", lease_seconds=30)[0]
    checkpoint = Path(item["checkpoint_path"])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps({"scope": {"knowledge_cutoff": "2026-08-01"}}),
        encoding="utf-8",
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET metadata_json = ? WHERE work_id = ?",
            (json.dumps({}), item["work_id"]),
        )
        conn.commit()

    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )

    assert queue.get(item["work_id"])["metadata"]["knowledge_cutoff"] == "2026-08-01"


def test_configuration_blocked_stage_does_not_consume_attempt(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
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

    async def stage_runner(_stage, _item):
        return {
            "status": "stopped",
            "reason": "blocked_configuration:extract:semantic_network_disabled",
            "quality": {
                "stage_ready": False,
                "blocked_configuration": True,
                "blocked_configuration_reasons": {"semantic_network_disabled": 1},
            },
        }

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
        retry_backoff_seconds=1,
    )
    semantic = asyncio.run(
        service._drain_stage(
            "semantic",
            StageBudget(max_items=1, max_concurrency=1),
        )
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    item = queue.get(work_id)

    assert semantic["configuration_blocked"] == 1
    assert semantic["retried"] == 0
    assert semantic["terminal_failures"] == 0
    assert item["status"] == "retry_due"
    assert item["attempt_count"] == 0
    assert item["last_error"].startswith("blocked_configuration:")


@pytest.mark.parametrize(
    ("stage", "reason", "pipeline_status"),
    [
        ("parse", "selector_gap", "success"),
        ("semantic", "numeric_reconciliation_failed", "stopped"),
    ],
)
def test_machine_rework_is_finalized_without_content_retry(
    tmp_path, stage, reason, pipeline_status
):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = ?, status = 'pending'",
            (stage,),
        )
        conn.commit()

    async def stage_runner(_stage, _item):
        return {
            "status": pipeline_status,
            "quality": {
                "stage_ready": False,
                "blocking_machine_rework": 1,
                "selected_documents": 1,
                "machine_rework_reasons": {reason: 1},
            },
        }

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
    )
    stage_result = asyncio.run(
        service._drain_stage(
            stage,
            StageBudget(max_items=1, max_concurrency=1),
        )
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    item = queue.get(work_id)

    assert stage_result["machine_rework_deferred"] == 1
    assert stage_result["retried"] == 0
    assert stage_result["terminal_failures"] == 0
    assert item["status"] == "machine_rework"
    assert item["attempt_count"] == 1
    assert item["last_error"] == f"machine_rework:{reason}"
    assert item["metadata"]["stage_results"][stage]["status"] == pipeline_status
    assert queue.health()["machine_rework"] == 1
    assert queue.health()["terminal"] == 0

    reset = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        force=True,
    )
    assert reset["reset"] == 1
    assert queue.get(work_id)["status"] == "pending"
    assert queue.get(work_id)["stage"] == "acquire"


def test_auto_approved_unit_rule_defers_artifact_replay_without_attempt_cost(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
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

    async def stage_runner(_stage, _item):
        return {
            "status": "success",
            "quality": {
                "stage_ready": False,
                "blocking_machine_rework": 1,
                "machine_rework_reasons": {"unit_normalization_failed": 1},
            },
            "metrics": {"unit_rule_auto_approved": 1},
        }

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
    )
    result = asyncio.run(
        service._drain_stage("semantic", StageBudget(max_items=1, max_concurrency=1))
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    item = queue.get(work_id)

    assert result["artifact_replay_deferred"] == 1
    assert result["machine_rework_deferred"] == 0
    assert result["retried"] == 0
    assert item["status"] == "retry_due"
    assert item["attempt_count"] == 0
    assert item["completed_at"] is None


def test_context_incomplete_reselects_once_before_machine_rework(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items "
            "SET stage = 'semantic', status = 'pending'"
        )
        conn.commit()

    async def stage_runner(_stage, _item):
        return {
            "status": "stopped",
            "quality": {
                "stage_ready": False,
                "blocking_machine_rework": 1,
                "machine_rework_reasons": {"context_incomplete": 1},
            },
        }

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
    )
    first = asyncio.run(
        service._drain_stage("semantic", StageBudget(max_items=1, max_concurrency=1))
    )
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    item = queue.get(work_id)

    assert first["context_reselect_deferred"] == 1
    assert first["machine_rework_deferred"] == 0
    assert item["stage"] == "parse"
    assert item["status"] == "retry_due"
    assert item["attempt_count"] == 0
    assert item["metadata"]["automated_rework_counts"] == {"context_incomplete": 1}

    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items "
            "SET stage = 'semantic', status = 'pending'"
        )
        conn.commit()
    second = asyncio.run(
        service._drain_stage("semantic", StageBudget(max_items=1, max_concurrency=1))
    )
    item = queue.get(work_id)

    assert second["context_reselect_deferred"] == 0
    assert second["machine_rework_deferred"] == 1
    assert item["stage"] == "semantic"
    assert item["status"] == "machine_rework"


def test_stage_quality_gate_finalizes_machine_rework_without_acknowledging(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
    )
    with storage.get_connection() as conn:
        conn.execute("UPDATE business_profile_work_items SET stage = 'parse'")
        conn.commit()
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(),
        stage_runner=AsyncMock(
            return_value={
                "status": "success",
                "quality": {
                    "stage_ready": False,
                    "blocking_machine_rework": 1,
                    "selected_documents": 0,
                    "outline_pages_scoped": 18,
                    "outline_sources": {"table_of_contents": 1},
                    "outline_confidences": {"high": 1},
                    "empty_output_documents": 1,
                    "empty_output_reasons": {"ambiguous_table_layout": 1},
                },
            }
        ),
        retry_backoff_seconds=1,
    )

    report = asyncio.run(
        service._run_workers({"parse": StageBudget(max_items=1, max_concurrency=1)})
    )

    assert report["parse"]["completed"] == 0
    assert report["parse"]["retried"] == 0
    assert report["parse"]["terminal_failures"] == 0
    assert report["parse"]["machine_rework_deferred"] == 1
    assert report["parse"]["quality"] == {
        "blocking_machine_rework": 1,
        "selected_documents": 0,
        "selected_pages": 0,
        "outline_pages_scoped": 18,
        "evidence_records": 0,
        "record_count": 0,
        "verified_records": 0,
        "empty_output_documents": 1,
        "outline_sources": {"table_of_contents": 1},
        "outline_confidences": {"high": 1},
        "empty_output_reasons": {"ambiguous_table_layout": 1},
        "expected_non_disclosure_documents": 0,
        "structured_fallback_required": 0,
        "structured_fallback_calls": 0,
        "structured_fallback_accepted_records": 0,
        "structured_fallback_rejected": 0,
        "page_artifact_cache_hits": 0,
        "page_artifact_cache_misses": 0,
        "pdf_parser_warning_count": 0,
        "blocked_configuration_reasons": {},
        "machine_rework_reasons": {},
    }
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT stage, status FROM business_profile_work_items"
        ).fetchone()
    assert dict(row) == {"stage": "parse", "status": "machine_rework"}


def test_stage_worker_claims_retryable_work_once_per_invocation(tmp_path):
    class RetryRepository:
        def __init__(self):
            self.status = "pending"
            self.attempt_count = 0
            self.claim_exclusions = []

        def claim(self, _stage, *, exclude_work_ids=(), lease_owner, **_kwargs):
            excluded = tuple(exclude_work_ids)
            self.claim_exclusions.append(excluded)
            if "work-1" in excluded or self.status not in {"pending", "retry_due"}:
                return ()
            self.status = "running"
            self.lease_owner = lease_owner
            self.attempt_count += 1
            return ({"work_id": "work-1", "instrument_id": "601088.SH"},)

        def fail(self, work_id, *, lease_owner, **_kwargs):
            assert work_id == "work-1"
            assert lease_owner == self.lease_owner
            self.status = "retry_due"
            return "retry_due"

    queue = RetryRepository()
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(),
        stage_runner=AsyncMock(side_effect=TimeoutError("retryable")),
        retry_backoff_seconds=1,
        write_coordinator=BusinessProfileWriteCoordinator(inter_write_seconds=0),
    )

    first = asyncio.run(
        service._drain_stage(
            "parse",
            StageBudget(max_items=2, max_concurrency=1),
        )
    )
    second = asyncio.run(
        service._drain_stage(
            "parse",
            StageBudget(max_items=1, max_concurrency=1),
        )
    )

    assert first["claimed"] == 1
    assert first["retried"] == 1
    assert first["claim_exclusions"] == 1
    assert second["claimed"] == 1
    assert queue.attempt_count == 2
    assert queue.claim_exclusions == [(), ("work-1",), ()]


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
    with storage.get_connection() as conn:
        checkpoint_path = Path(
            conn.execute(
                "SELECT checkpoint_path FROM business_profile_work_items"
            ).fetchone()[0]
        )
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps(
            {
                "completed_stages": [
                    "plan",
                    "select",
                    "extract",
                    "verify",
                    "promote",
                ]
            }
        ),
        encoding="utf-8",
    )
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET metadata_json = ?",
            (json.dumps({"stage_results": {"publish": {"status": "success"}}}),),
        )
        conn.commit()
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
        max_attempts=5,
        force=True,
    )

    assert result["reset"] == 1
    assert result["checkpoint_rotated"] == 1
    assert result["inserted"] == 0
    with storage.get_connection() as conn:
        rows = conn.execute(
            "SELECT work_id, stage, status, max_attempts, checkpoint_path, metadata_json "
            "FROM business_profile_work_items"
        ).fetchall()
    assert len(rows) == 1
    row = dict(rows[0])
    assert row["work_id"] == claimed["work_id"]
    assert row["stage"] == "acquire"
    assert row["status"] == "pending"
    assert row["max_attempts"] == 5
    assert row["checkpoint_path"] != str(checkpoint_path)
    assert not Path(row["checkpoint_path"]).exists()
    metadata = json.loads(row["metadata_json"])
    assert metadata["recovery_history"][-1]["reason"] == (
        "force_replay_checkpoint_rotated"
    )
    assert metadata["recovery_history"][-1]["from_stage"] == "acquire"
    assert metadata["recovery_history"][-1]["invalidated_stage_result_names"] == [
        "publish"
    ]
    assert metadata["reprocess_complete_coverage"] is True
    assert "stage_results" not in metadata
    assert checkpoint_path.exists()


def test_force_rotates_retry_due_checkpoint_requeued_by_contract_recovery(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        instrument_ids=("600000.SH",),
        start_date="2026-01-01",
        document_types=("annual_report", "annual_report_correction"),
    )
    with storage.get_connection() as conn:
        row = conn.execute(
            "SELECT work_id, checkpoint_path FROM business_profile_work_items"
        ).fetchone()
        checkpoint_path = Path(row["checkpoint_path"])
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'semantic', "
            "status = 'retry_due', lease_owner = NULL, lease_expires_at = NULL, "
            "metadata_json = ? WHERE work_id = ?",
            (
                json.dumps(
                    {
                        "stage_results": {
                            "semantic": {"status": "success", "candidate_records": 0},
                            "verify": {"status": "success", "verified_records": 0},
                            "publish": {"status": "success", "promoted_records": 0},
                        }
                    }
                ),
                row["work_id"],
            ),
        )
        conn.commit()
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.write_text(
        json.dumps(
            {
                "completed_stages": [
                    "plan",
                    "select",
                    "extract",
                    "verify",
                    "promote",
                ]
            }
        ),
        encoding="utf-8",
    )

    result = queue.enqueue_scoped(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        instrument_ids=("600000.SH",),
        start_date="2026-01-01",
        document_types=("annual_report", "annual_report_correction"),
        force=True,
    )

    assert result["reset"] == 1
    assert result["checkpoint_rotated"] == 1
    assert result["reused"] == 0
    recovered = queue.get(row["work_id"])
    assert recovered["stage"] == "acquire"
    assert recovered["status"] == "pending"
    assert recovered["checkpoint_path"] != str(checkpoint_path)
    assert "stage_results" not in recovered["metadata"]
    history = recovered["metadata"]["recovery_history"][-1]
    assert history["from_stage"] == "semantic"
    assert history["from_status"] == "retry_due"
    assert history["invalidated_stage_result_names"] == [
        "publish",
        "semantic",
        "verify",
    ]
    assert checkpoint_path.exists()


def test_replace_policy_rotates_completed_work_without_force(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    identity = {"rules": "v1", "result_policy": "replace"}
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    assert first["inserted"] == 1
    with storage.get_connection() as conn:
        work_id = conn.execute(
            "SELECT work_id FROM business_profile_work_items"
        ).fetchone()[0]
    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET status = 'completed', "
            "stage = 'publish', completed_at = updated_at WHERE work_id = ?",
            (work_id,),
        )
        conn.commit()

    second = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )

    assert second["reset"] == 1
    assert second["checkpoint_rotated"] == 1
    recovered = queue.get(work_id)
    assert recovered["status"] == "pending"
    assert recovered["stage"] == "acquire"
    assert recovered["metadata"]["replacement_generation"] == 2
    assert recovered["metadata"]["recovery_history"][-1]["reason"] == (
        "replace_replay_checkpoint_rotated"
    )


def test_force_replay_refreshes_work_cutoff(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-20",
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

    queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity={"rules": "v1"},
        max_attempts=1,
        force=True,
    )

    item = queue.get(claimed["work_id"])
    assert item["metadata"]["knowledge_cutoff"] == "2026-08-30"
    assert item["metadata"]["processing_identity"] == {"rules": "v1"}


def test_legacy_unfinished_publish_migrates_by_verify_checkpoint_state(tmp_path):
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
        row = conn.execute(
            "SELECT work_id, checkpoint_path FROM business_profile_work_items"
        ).fetchone()
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'publish' "
            "WHERE work_id = ?",
            (row["work_id"],),
        )
        conn.commit()
    checkpoint = Path(row["checkpoint_path"])
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text(
        json.dumps({"completed_stages": ["plan", "select", "extract"]}),
        encoding="utf-8",
    )

    migrated = _migrate_unfinished_legacy_publish_items(storage)

    assert migrated == {"moved_to_verify": 1, "retained_publish": 0}
    assert queue.get(row["work_id"])["stage"] == "verify"

    with storage.get_connection() as conn:
        conn.execute(
            "UPDATE business_profile_work_items SET stage = 'publish' "
            "WHERE work_id = ?",
            (row["work_id"],),
        )
        conn.commit()
    checkpoint.write_text(
        json.dumps(
            {"completed_stages": ["plan", "select", "extract", "verify"]}
        ),
        encoding="utf-8",
    )

    retained = _migrate_unfinished_legacy_publish_items(storage)

    assert retained == {"moved_to_verify": 0, "retained_publish": 1}
    assert queue.get(row["work_id"])["stage"] == "publish"


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
    assert report["throughput"]["enqueued"] == report["enqueue"]["inserted"] == 3
    assert report["throughput"]["worker_completed"] == 0
    assert report["throughput"]["stage_completed"] == {
        "acquire": 1,
        "parse": 0,
        "semantic": 0,
        "verify": 0,
        "publish": 0,
    }


def test_backfill_worker_terminal_failure_degrades_top_level_status(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )

    async def stage_runner(_stage, _item):
        raise ValueError("invalid checkpoint scope")

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=stage_runner,
    )
    report = asyncio.run(
        asyncio.wait_for(
            service.run_backfill(
                knowledge_cutoff="2026-08-30",
                processing_identity=_processing_identity(),
                instrument_ids=("600000.SH",),
                stage_budgets={
                    "acquire": StageBudget(max_items=1, max_concurrency=1),
                },
            ),
            timeout=5,
        )
    )

    assert report["workers"]["acquire"]["terminal_failures"] == 1
    assert report["status"] == "degraded"
    assert report["reason_codes"] == ["worker_terminal_failures"]


@pytest.mark.parametrize(
    ("counter", "reason"),
    [
        ("retried", "worker_retries"),
        ("configuration_blocked", "worker_configuration_blocked"),
        ("lease_conflicts", "worker_lease_conflicts"),
    ],
)
def test_operation_status_reports_nonterminal_worker_health(counter, reason):
    status, reason_codes = _business_profile_operation_status(
        discovery={"status": "success"},
        workers={"semantic": {"status": "success", counter: 1}},
    )

    assert status == "degraded"
    assert reason_codes == [reason]


def test_operation_status_allows_healthy_bounded_backlog():
    status, reason_codes = _business_profile_operation_status(
        discovery={
            "status": "degraded",
            "discovery_window_backlog": 3,
            "errors": [],
        },
        workers={"acquire": {"status": "success", "completed": 20}},
    )

    assert status == "success"
    assert reason_codes == []


def test_operation_status_does_not_hide_deferred_or_backpressured_workers():
    status, reason_codes = _business_profile_operation_status(
        discovery={"status": "success"},
        workers={
            "parse": {"status": "deferred", "reason": "no_stage_budget"},
            "acquire": {"status": "backpressured"},
        },
    )

    assert status == "degraded"
    assert reason_codes == ["worker_backpressured"]


def test_operation_status_degrades_for_actionable_publication_gaps():
    status, reason_codes = _business_profile_operation_status(
        discovery={"status": "success"},
        workers={
            "publish": {
                "status": "success",
                "quality": {"publication_gaps": 1},
            }
        },
    )

    assert status == "degraded"
    assert reason_codes == ["publication_gaps"]


def test_backfill_emits_lifecycle_and_inflight_progress_logs(
    tmp_path, caplog, monkeypatch
):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )

    async def discover(**_kwargs):
        await asyncio.sleep(0.15)
        return {
            "status": "degraded",
            "pages_scanned": 2,
            "discovery_window_backlog": 1,
            "incomplete_windows": [{"exchange": "SZSE"}],
        }

    async def stage_runner(_stage, _item):
        await asyncio.sleep(0.15)
        return {"status": "success"}

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=discover,
        stage_runner=stage_runner,
        progress_log_interval_seconds=0.1,
    )
    logger = logging.getLogger("research.business_profile_async_production")
    monkeypatch.setattr(logger, "propagate", True)
    caplog.set_level("INFO", logger=logger.name)

    report = asyncio.run(
        service.run_backfill(
            knowledge_cutoff="2026-08-30",
            processing_identity={"rules": "logs"},
            instrument_ids=("600000.SH",),
            discovery_kwargs={"start_date": "2026-01-01"},
            stage_budgets={
                "acquire": StageBudget(max_items=1, max_concurrency=1),
            },
        )
    )

    messages = [record.getMessage() for record in caplog.records]
    assert report["status"] == "success"
    assert any("business-profile backfill start" in message for message in messages)
    assert any(
        "business-profile discovery heartbeat" in message for message in messages
    )
    assert any("business-profile discovery end" in message for message in messages)
    assert any("business-profile stage heartbeat" in message for message in messages)
    assert any("business-profile stage end" in message for message in messages)
    assert any("business-profile run end" in message for message in messages)


def test_discovery_heartbeat_does_not_leave_task_running_after_cancel(tmp_path):
    storage = _storage(tmp_path)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    cancelled = asyncio.Event()

    async def discover(**_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=discover,
        stage_runner=AsyncMock(),
        progress_log_interval_seconds=0.1,
    )

    async def run_and_cancel():
        task = asyncio.create_task(
            service._run_discovery_with_heartbeat(
                operation="backfill",
                discovery_kwargs={},
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()

    asyncio.run(run_and_cancel())


def test_stage_heartbeat_does_not_leave_batch_running_after_cancel():
    repository = Mock()
    repository.claim.return_value = [{"work_id": "work-1"}]
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def stage_runner(_stage, _item):
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    service = BusinessProfileAsyncProductionService(
        repository=repository,
        discovery_runner=AsyncMock(),
        stage_runner=stage_runner,
        progress_log_interval_seconds=0.1,
        write_coordinator=BusinessProfileWriteCoordinator(inter_write_seconds=0),
    )

    async def run_and_cancel():
        task = asyncio.create_task(
            service._drain_stage(
                "semantic",
                StageBudget(max_items=1, max_concurrency=1),
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set()

    asyncio.run(run_and_cancel())


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


def test_stage_continuously_refills_available_concurrency_slots():
    class StreamingRepository:
        def __init__(self):
            self.available = [{"work_id": "work-1", "instrument_id": "600001.SH"}]
            self.completed = []

        def has_claimable(self, _stage, *, exclude_work_ids=(), **_kwargs):
            excluded = set(exclude_work_ids)
            return any(item["work_id"] not in excluded for item in self.available)

        def claim(self, _stage, *, limit, exclude_work_ids=(), **_kwargs):
            excluded = set(exclude_work_ids)
            claimed = []
            remaining = []
            for item in self.available:
                if len(claimed) < limit and item["work_id"] not in excluded:
                    claimed.append(item)
                else:
                    remaining.append(item)
            self.available = remaining
            return tuple(claimed)

        def acknowledge(self, work_id, **_kwargs):
            self.completed.append(work_id)
            return "completed"

    repository = StreamingRepository()
    first_started = asyncio.Event()
    second_started = asyncio.Event()

    async def stage_runner(_stage, item):
        if item["work_id"] == "work-1":
            first_started.set()
            await second_started.wait()
        else:
            second_started.set()
        return {
            "status": "success",
            "quality": {"selection_seconds": 0.01},
        }

    service = BusinessProfileAsyncProductionService(
        repository=repository,
        discovery_runner=AsyncMock(),
        stage_runner=stage_runner,
        progress_log_interval_seconds=0.05,
        write_coordinator=BusinessProfileWriteCoordinator(inter_write_seconds=0),
    )

    async def run_streaming_stage():
        task = asyncio.create_task(
            service._drain_stage(
                "semantic",
                StageBudget(max_items=2, max_concurrency=2, max_elapsed_seconds=1),
            )
        )
        await asyncio.wait_for(first_started.wait(), timeout=0.5)
        repository.available.append({"work_id": "work-2", "instrument_id": "600002.SH"})
        return await asyncio.wait_for(task, timeout=1)

    result = asyncio.run(run_streaming_stage())

    assert result["completed"] == 2
    assert result["peak_in_flight"] == 2
    assert result["queue_underfilled_slots"] >= 1
    assert result["gateway_admission"]["throttled_requests"] == 0
    assert result["quality"]["selection_seconds"] == pytest.approx(0.02)
    assert set(repository.completed) == {"work-1", "work-2"}


@pytest.mark.parametrize(
    (
        "reason_code",
        "expected_events",
        "expected_effective_concurrency",
        "expected_deferred",
        "expected_retried",
    ),
    [
        ("numeric_reconciliation_failed", 0, 2, 1, 0),
        ("gateway_failure", 1, 1, 0, 1),
    ],
)
def test_semantic_adaptive_concurrency_only_reacts_to_provider_pressure(
    reason_code,
    expected_events,
    expected_effective_concurrency,
    expected_deferred,
    expected_retried,
):
    repository = Mock()
    repository.claim.return_value = [
        {"work_id": "work-1", "instrument_id": "600000.SH"}
    ]
    repository.finalize_machine_rework.return_value = "machine_rework"
    repository.fail.return_value = "retry_due"

    async def stage_runner(_stage, _item):
        return {
            "status": "success",
            "quality": {
                "stage_ready": False,
                "blocking_machine_rework": 1,
                "selected_documents": 1,
                "machine_rework_reasons": {reason_code: 1},
            },
        }

    service = BusinessProfileAsyncProductionService(
        repository=repository,
        discovery_runner=AsyncMock(),
        stage_runner=stage_runner,
        write_coordinator=BusinessProfileWriteCoordinator(inter_write_seconds=0),
    )

    result = asyncio.run(
        service._drain_stage(
            "semantic",
            StageBudget(max_items=1, max_concurrency=2),
        )
    )

    gateway = result["gateway_admission"]
    assert result["machine_rework_deferred"] == expected_deferred
    assert result["retried"] == expected_retried
    assert gateway["provider_congestion_events"] == expected_events
    assert gateway["effective_concurrency"] == expected_effective_concurrency


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


def test_broad_backfill_pre_batch_gate_blocks_known_lifecycle_findings(
    tmp_path, monkeypatch
):
    storage = _storage(tmp_path)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    discover = AsyncMock(return_value={"status": "success"})
    stage_runner = AsyncMock(return_value={"status": "success"})

    def audit(_service, *, instrument_ids, apply=False, **_kwargs):
        assert tuple(instrument_ids) == ("600000.SH", "000001.SZ")
        assert apply is False
        return {
            "issue_counts": {"failed_work_item": 1},
            "instruments": [
                {
                    "instrument_id": "600000.SH",
                    "issues": [
                        {
                            "code": "failed_work_item",
                            "stable_id": "failed_work_item:600000.SH:1",
                            "details": {"work_id": "work-1"},
                        }
                    ],
                },
                {"instrument_id": "000001.SZ", "issues": []},
            ],
        }

    monkeypatch.setattr(
        "research.business_profile_semantic_repair.BusinessProfileSemanticRepairService.run",
        audit,
    )
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=discover,
        stage_runner=stage_runner,
    )

    report = asyncio.run(
        service.run_backfill(
            knowledge_cutoff="2026-08-30",
            processing_identity={"rules": "gate"},
            instrument_ids=("600000.SH", "000001.SZ"),
            discovery_kwargs={"start_date": "2026-01-01"},
        )
    )

    assert report["status"] == "not_ready"
    assert report["reason_codes"] == ["pre_batch_gate_blocked"]
    assert report["pre_batch_gate"]["llm_calls"] == 0
    assert report["pre_batch_gate"]["blocking_instruments"] == [
        {
            "instrument_id": "600000.SH",
            "issues": [
                {
                    "code": "failed_work_item",
                    "stable_id": "failed_work_item:600000.SH:1",
                    "details": {"work_id": "work-1"},
                }
            ],
        }
    ]
    discover.assert_not_awaited()
    stage_runner.assert_not_awaited()


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


def test_empty_downstream_poll_does_not_take_writer_and_claims_new_work(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    coordinator = BusinessProfileWriteCoordinator(inter_write_seconds=0)
    upstream_done = asyncio.Event()
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(return_value={"status": "success"}),
        stage_runner=AsyncMock(return_value={"status": "success"}),
        write_coordinator=coordinator,
    )

    async def run_poll_then_enqueue():
        task = asyncio.create_task(
            service._drain_stage(
                "parse",
                StageBudget(
                    max_items=1,
                    max_concurrency=1,
                    max_elapsed_seconds=0.05,
                ),
                upstream_done=upstream_done,
            )
        )
        await asyncio.sleep(0.12)
        assert coordinator.snapshot()["write_transactions"] == 0
        queue.enqueue_latest_annual(
            knowledge_cutoff="2026-08-30",
            processing_identity={"rules": "late-parse"},
        )
        with storage.get_connection() as conn:
            conn.execute("UPDATE business_profile_work_items SET stage = 'parse'")
            conn.commit()
        return await asyncio.wait_for(task, timeout=2)

    result = asyncio.run(run_poll_then_enqueue())

    assert result["completed"] == 1
    assert result["empty_polls"] >= 1
    assert result["active_work_seconds"] < 0.05
    assert coordinator.snapshot()["write_transactions"] == 2


def test_storage_operation_coordinates_each_transaction_not_whole_function(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage,
        checkpoint_root=tmp_path / "checkpoints",
    )
    coordinator = BusinessProfileWriteCoordinator(inter_write_seconds=0)
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=AsyncMock(),
        stage_runner=AsyncMock(),
        write_coordinator=coordinator,
    )

    def mixed_operation():
        with storage.get_connection() as conn:
            conn.execute("SELECT COUNT(*) FROM business_profile_work_items").fetchone()
        with storage.get_connection() as conn:
            conn.execute(
                "INSERT INTO business_profile_operation_state "
                "(state_key, state_value_json, updated_at) VALUES "
                "('first', '{}', '2026-08-09T12:00:00+08:00')"
            )
            conn.commit()
        with storage.get_connection() as conn:
            conn.execute("SELECT COUNT(*) FROM business_profile_work_items").fetchone()
        with storage.get_connection() as conn:
            conn.execute(
                "INSERT INTO business_profile_operation_state "
                "(state_key, state_value_json, updated_at) VALUES "
                "('second', '{}', '2026-08-09T12:00:01+08:00')"
            )
            conn.commit()

    asyncio.run(service._run_storage_operation(mixed_operation))

    writer = coordinator.snapshot()
    assert writer["write_transactions"] == 2
    assert writer["max_active_writers"] == 1


def test_parse_and_semantic_compute_parallel_without_blocking_sqlite(tmp_path):
    storage = _storage(tmp_path)
    coordinator = BusinessProfileWriteCoordinator(inter_write_seconds=0)
    compute_lock = threading.Lock()
    active_compute = 0
    max_active_compute = 0
    compute_barrier = threading.Barrier(5)
    release_compute = threading.Event()
    worker_errors = []

    def worker(stage, index):
        nonlocal active_compute, max_active_compute
        try:
            with compute_lock:
                active_compute += 1
                max_active_compute = max(max_active_compute, active_compute)
            compute_barrier.wait(timeout=1)
            assert release_compute.wait(timeout=1)
            with compute_lock:
                active_compute -= 1
            with storage.coordinated_writes(coordinator):
                with storage.get_connection() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        "INSERT INTO business_profile_operation_state "
                        "(state_key, state_value_json, updated_at) VALUES (?, '{}', ?)",
                        (
                            f"{stage}-{index}",
                            "2026-08-09T12:00:00+08:00",
                        ),
                    )
                    time.sleep(0.02)
                    conn.commit()
        except Exception as exc:
            worker_errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(stage, index))
        for stage in ("parse", "semantic")
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    compute_barrier.wait(timeout=1)

    with sqlite3.connect(storage.db_path, timeout=0.25) as conn:
        conn.execute("PRAGMA busy_timeout = 250")
        conn.execute(
            "INSERT INTO business_profile_operation_state "
            "(state_key, state_value_json, updated_at) VALUES "
            "('external-client', '{}', '2026-08-09T12:00:00+08:00')"
        )
        conn.commit()

    release_compute.set()
    for thread in threads:
        thread.join(timeout=2)

    assert all(not thread.is_alive() for thread in threads)
    assert worker_errors == []
    assert max_active_compute == 4
    assert coordinator.snapshot()["max_active_writers"] == 1
    assert coordinator.snapshot()["write_transactions"] == 4
    with storage.get_connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_operation_state WHERE "
                "state_key = 'external-client' OR state_key LIKE 'parse-%' "
                "OR state_key LIKE 'semantic-%'"
            ).fetchone()[0]
            == 5
        )


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


def test_data_manager_daily_advances_each_stage_without_draining_globally(
    tmp_path, monkeypatch
):
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
    shared = _configure_empty_shared_assets(manager, tmp_path)
    bound_acquire = Mock(
        return_value={"status": "success", "source_file_id": "source-1"}
    )
    monkeypatch.setattr(
        BusinessProfileFrontierBoundAcquirer,
        "acquire",
        bound_acquire,
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
                for stage in ("acquire", "parse", "semantic", "verify", "publish")
            },
        )
    )

    assert result["status"] == "success"
    queue_group = result["queue_health"]["groups"][0]
    assert queue_group["stage"] == "verify"
    assert queue_group["status"] == "completed"
    assert queue_group["row_count"] == 1
    assert result["queue_health"]["terminal"] == 0
    assert result["queue_health"]["completed"] == 1
    assert result["queue_health"]["finalized"] == 1
    bound_acquire.assert_called_once()
    assert [
        call.kwargs["mode"]
        for call in manager.run_business_profile_semantic_production.await_args_list
    ] == ["plan", "select", "extract", "verify"]
    manager.run_business_profile_index_discovery.assert_not_awaited()
    shared.list_effective_assets.assert_any_call(
        document_family="annual_report",
        knowledge_cutoff="2026-08-30",
        availability="local_valid",
        limit=30,
        offset=0,
    )


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
    manager.research_config = Mock(modules={"business_profile_evidence": {}})
    manager.run_business_profile_semantic_production = AsyncMock(
        return_value={"status": "failed", "reason": "manifest_mismatch"}
    )
    _configure_empty_shared_assets(manager, tmp_path)

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
    assert result["reason"] == "manifest_mismatch"
    assert (
        manager.run_business_profile_semantic_production.await_args.kwargs["mode"]
        == "promote"
    )


def test_data_manager_shared_only_discovery_never_calls_business_profile_provider(
    tmp_path,
):
    storage = _storage(tmp_path)
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = SimpleNamespace(
        modules={"business_profile_evidence": {}},
        storage=SimpleNamespace(db_path=str(tmp_path / "research.db")),
    )
    manager.run_business_profile_index_discovery = AsyncMock(
        side_effect=AssertionError("business-profile provider discovery must not run")
    )

    class _SharedAccess:
        repository = SimpleNamespace()

        def list_effective_assets(self, **kwargs):
            assert kwargs["document_family"] == "annual_report"
            assert kwargs["availability"] == "local_valid"
            return {"items": [], "returned": 0}

    shared = _SharedAccess()
    manager._get_announcement_asset_access = Mock(return_value=shared)
    service, _identity = manager._build_business_profile_async_service(
        cutoff="2026-08-30",
        configured_families=("atomic_activities",),
        identities={"rules": "v1"},
        operations={"checkpoint_root": str(tmp_path / "checkpoints")},
        semantic={"promotion_enabled": False},
        default_exchanges=("SSE",),
    )

    result = asyncio.run(
        service.discovery_runner(
            category="annual_report",
            end_date="2026-08-30",
            page_size=100,
            max_pages_per_market=2,
        )
    )

    assert result["operation"] == "shared_annual_report_discovery"
    assert result["provider_requests"] == 0
    manager.run_business_profile_index_discovery.assert_not_awaited()


def test_data_manager_targeted_backfill_scopes_discovery_and_reconciliation_access(
    tmp_path,
    monkeypatch,
):
    from contextlib import nullcontext

    import research.business_profile_async_production as async_module
    import research.business_profile_production_operations as operations_module

    storage = Mock()
    storage.coordinated_writes.return_value = nullcontext()

    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", run_inline)
    monkeypatch.setattr(
        async_module,
        "ensure_business_profile_storage_ready",
        lambda _storage: None,
    )
    monkeypatch.setattr(
        async_module,
        "get_business_profile_write_coordinator",
        lambda *_args, **_kwargs: object(),
    )
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = SimpleNamespace(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"enabled": True},
                "production_operations": {
                    "async_production_enabled": True,
                    "use_rollout_config": False,
                },
            }
        },
    )
    service = SimpleNamespace(
        run_backfill=AsyncMock(
            return_value={
                "status": "success",
                "queue_health": {"claimable": 0, "running": 0, "terminal": 0},
            }
        )
    )
    manager._build_business_profile_async_service = Mock(
        return_value=(service, {"rules": "v1"})
    )
    shared_access = SimpleNamespace()
    manager._get_announcement_asset_access = Mock(return_value=shared_access)
    reconciliation = Mock(return_value={"status": "success"})
    monkeypatch.setattr(
        operations_module,
        "build_business_profile_reconciliation_report",
        reconciliation,
    )

    result = asyncio.run(
        manager.run_business_profile_backfill(
            knowledge_cutoff="2026-08-20",
            selection_policy="expanded",
            instrument_ids=["601088.SH"],
            start_date="2026-01-01",
            document_types=["annual_report", "annual_report_correction"],
            field_families=["named_relationships"],
            runtime_identities={"rules": "v1"},
        )
    )

    assert result["reconciliation"]["status"] == "success"
    discovery_kwargs = service.run_backfill.await_args.kwargs["discovery_kwargs"]
    assert discovery_kwargs["instrument_ids"] == ["601088.SH"]
    assert reconciliation.call_args.kwargs["shared_asset_access"] is shared_access


def test_targeted_expanded_atomic_backfill_selects_complete_publication_phase(
    tmp_path, monkeypatch
):
    import research.business_profile_async_production as async_module
    import research.business_profile_production_operations as operations_module

    storage = _storage(tmp_path)
    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", run_inline)
    monkeypatch.setattr(
        async_module,
        "ensure_business_profile_storage_ready",
        lambda _storage: None,
    )
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = SimpleNamespace(
        enabled=True,
        modules={
            "business_profile_evidence": {
                "enabled": True,
                "semantic_production": {"enabled": True},
                "production_operations": {
                    "async_production_enabled": True,
                    "use_rollout_config": True,
                    "runtime_identity_mode": "derived",
                    "checkpoint_root": str(tmp_path / "checkpoints"),
                },
            }
        },
        storage=SimpleNamespace(db_path=str(tmp_path / "research.db")),
    )
    service = SimpleNamespace(
        run_backfill=AsyncMock(
            return_value={
                "status": "success",
                "workers": {},
                "queue_health": {"claimable": 0, "running": 0, "terminal": 0},
                "publication_summary": {},
                "execution_mode": "complete_publication",
            }
        )
    )
    captured = {}

    def build_service(**kwargs):
        captured.update(kwargs)
        return service, {"processing": "identity"}

    manager._build_business_profile_async_service = Mock(side_effect=build_service)
    shared_access = SimpleNamespace()
    manager._get_announcement_asset_access = Mock(return_value=shared_access)
    manager._dispatch_business_profile_unit_rule_notifications = AsyncMock(
        return_value={"status": "disabled", "delivered": 0}
    )
    monkeypatch.setattr(
        operations_module,
        "build_business_profile_reconciliation_report",
        lambda *_args, **_kwargs: {
            "status": "success",
            "active_universe_count": 1,
            "current_annual_instrument_count": 1,
        },
    )

    result = asyncio.run(
        manager.run_business_profile_backfill(
            knowledge_cutoff="2026-08-20",
            selection_policy="expanded",
            instrument_ids=["601088.SH"],
            start_date="2026-01-01",
            document_types=["annual_report", "annual_report_correction"],
            field_families=["atomic_activities", "named_relationships"],
        )
    )

    assert result["effective_rollout_phase"] == "semantic_complete_targeted"
    assert captured["configured_families"] == (
        "atomic_activities",
        "named_relationships",
        "derived_value_chain_roles",
        "commodity_exposure_facts",
        "commodity_exposure_publication",
    )
    assert captured["semantic"]["promotion_enabled"] is True
    budgets = service.run_backfill.await_args.kwargs["stage_budgets"]
    assert budgets["verify"].max_concurrency == 20
    assert budgets["publish"].max_concurrency == 1


def test_data_manager_stage_runner_uses_work_bound_cutoff(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    manager = DataManager.__new__(DataManager)
    manager.research_storage = storage
    manager.research_config = Mock(modules={"business_profile_evidence": {}})
    manager.run_business_profile_semantic_production = AsyncMock(
        return_value={"status": "success"}
    )
    _configure_empty_shared_assets(manager, tmp_path)
    service, identity = manager._build_business_profile_async_service(
        cutoff="2026-08-30",
        configured_families=("atomic_activities",),
        identities={"rules": "v1"},
        operations={"checkpoint_root": str(tmp_path / "checkpoints")},
        semantic={"promotion_enabled": False},
        default_exchanges=("SSE",),
    )
    service.repository.enqueue_latest_annual(
        knowledge_cutoff="2026-08-01",
        processing_identity=identity,
    )
    item = service.repository.claim(
        "acquire", limit=1, lease_owner="worker", lease_seconds=30
    )[0]

    asyncio.run(service.stage_runner("parse", item))

    assert (
        manager.run_business_profile_semantic_production.await_args.kwargs[
            "knowledge_cutoff"
        ]
        == "2026-08-01"
    )
