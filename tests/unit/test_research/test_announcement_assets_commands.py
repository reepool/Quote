from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path
from threading import Event
from time import sleep

import pytest

from research.announcement_assets.commands import (
    DAILY_UPDATE_JOB,
    LATEST_BACKFILL_JOB,
    AnnualReportSchedulerCommandService,
    AuthorizationBoundaryUnavailable,
    CommandPrincipal,
)
from research.announcement_assets.config import AnnouncementAssetConfig
from research.announcement_assets.models import BatchOutcome, OperationStatus
from research.announcement_assets.repository import AnnouncementAssetRepository
from research.announcement_assets.service import acquisition_work_fingerprint
from research.announcements import (
    AnnouncementAcquisitionConfig,
    AnnouncementAcquisitionService,
    AnnouncementProviderCapabilities,
    AnnouncementProviderRegistry,
    AnnouncementRouteConfig,
)


def _config(tmp_path: Path, *, trusted: bool = True) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "scheduled_enabled": True,
            "dry_run": False,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "permissions": {
                "trusted_identity_enabled": trusted,
                "operator": "annual_report_assets:operator",
                "principals": (
                    [
                        {
                            "principal": "operator-1",
                            "token_env": "ANNOUNCEMENT_ASSET_TEST_TOKEN",
                            "scopes": ["annual_report_assets:operator"],
                        },
                        {
                            "principal": "service:annual-report-asset-scheduler",
                            "token_env": "ANNOUNCEMENT_ASSET_SCHEDULER_TEST_TOKEN",
                            "scopes": ["annual_report_assets:operator"],
                        },
                    ]
                    if trusted
                    else []
                ),
            },
            "jobs": {
                "latest_backfill_manual_only": True,
                "daily_enabled": True,
            },
        },
        project_root=tmp_path,
    )


def _principal() -> CommandPrincipal:
    return CommandPrincipal(
        principal_id="operator-1",
        permissions=frozenset({"annual_report_assets:operator"}),
    )


def _service_principal() -> CommandPrincipal:
    return CommandPrincipal(
        principal_id="service:annual-report-asset-scheduler",
        permissions=frozenset({"annual_report_assets:operator"}),
        service_identity=True,
    )


class _Provider:
    def __init__(self, source: str, *, max_page_size: int = 30):
        self.source_name = source
        self.capabilities = AnnouncementProviderCapabilities(
            exchanges=frozenset({"SSE"}),
            supports_date_filter=True,
            supports_category_filter=True,
            max_page_size=max_page_size,
        )

    def discover(self, query):  # pragma: no cover - fingerprints never call providers
        raise AssertionError("fingerprint construction must be zero-network")


def _acquisition_service(*, route: tuple[str, ...], max_page_size: int = 30):
    providers = [_Provider(source, max_page_size=max_page_size) for source in route]
    return AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry(providers),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=route)
        ),
    )


def test_command_service_single_flights_and_audits_manual_runs(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path)
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-v1",
        runners={
            LATEST_BACKFILL_JOB: lambda operation: {
                "status": "success",
                "run_cutoff": "2026-08-10T03:00:00+00:00",
                "records_seen": 1,
            }
        },
    )
    principal = _principal()

    first = command.start(
        LATEST_BACKFILL_JOB,
        principal=principal,
        trigger_kind="manual",
        scope={"exchanges": ["SSE", "SZSE", "BSE"]},
        bounds={"max_instruments": 10},
    )
    second = command.start(
        LATEST_BACKFILL_JOB,
        principal=principal,
        trigger_kind="manual",
        scope={"exchanges": ["BSE", "SSE", "SZSE", "SSE"]},
        bounds={"max_instruments": 10},
    )
    assert first.reused is False
    assert second.reused is True
    assert first.run_id == second.run_id
    incompatible_bound = command.start(
        LATEST_BACKFILL_JOB,
        principal=principal,
        trigger_kind="manual",
        scope={"exchanges": ["SSE", "SZSE", "BSE"]},
        bounds={"max_instruments": 9},
    )
    assert incompatible_bound.reused is False
    assert incompatible_bound.run_id != first.run_id
    version_two = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-v2",
    ).start(
        LATEST_BACKFILL_JOB,
        principal=principal,
        trigger_kind="manual",
        scope={"exchanges": ["SSE", "SZSE", "BSE"]},
        bounds={"max_instruments": 10},
    )
    assert version_two.reused is False
    assert version_two.run_id != first.run_id
    completed = command.execute(first.run_id, principal=principal)
    assert completed.status is OperationStatus.COMPLETED
    assert completed.progress["records_seen"] == 1
    assert repository.list_job_command_audit(operation_id=first.run_id)


def test_command_authorization_requires_registered_principal_and_configured_scope(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path)
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-v1",
    )

    with pytest.raises(PermissionError, match="principal_not_registered"):
        command.start(
            LATEST_BACKFILL_JOB,
            principal=CommandPrincipal(
                principal_id="operator:forged",
                permissions=frozenset({"annual_report_assets:operator"}),
            ),
            trigger_kind="manual",
        )
    assert repository.list_operations(limit=10) == []

    restricted = AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": False,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "permissions": {
                "trusted_identity_enabled": True,
                "operator": "annual_report_assets:operator",
                "principals": [
                    {
                        "principal": "operator-1",
                        "token_env": "ANNOUNCEMENT_ASSET_TEST_TOKEN",
                        "scopes": ["annual_report_assets:read_content"],
                    }
                ],
            },
        },
        project_root=tmp_path,
    )
    restricted_command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=restricted,
        config_version="assets-config-restricted-v1",
    )
    with pytest.raises(PermissionError, match="operator_scope_not_configured"):
        restricted_command.start(
            LATEST_BACKFILL_JOB,
            principal=CommandPrincipal(
                principal_id="operator-1",
                permissions=frozenset({"annual_report_assets:operator"}),
            ),
            trigger_kind="manual",
        )
    assert repository.list_operations(limit=10) == []


def test_dry_run_blocks_job_before_repository_access(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    config = AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "dry_run": True,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "permissions": {
                "trusted_identity_enabled": True,
                "operator": "annual_report_assets:operator",
                "principals": [
                    {
                        "principal": "operator-1",
                        "token_env": "ANNOUNCEMENT_ASSET_TEST_TOKEN",
                        "scopes": ["annual_report_assets:operator"],
                    }
                ],
            },
        },
        project_root=tmp_path,
    )
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-dry-run-v1",
    )

    with pytest.raises(
        RuntimeError,
        match="annual_report_asset_dry_run_blocks_job_execution",
    ):
        command.preflight_start(
            LATEST_BACKFILL_JOB,
            principal=_principal(),
            trigger_kind="manual",
        )

    assert not repository.db_path.exists()


def test_execute_rechecks_dry_run_after_job_was_queued(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    writable_config = _config(tmp_path)
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=writable_config,
        config_version="assets-config-v1",
    )
    started = command.start(
        LATEST_BACKFILL_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )
    dry_run_config = replace(writable_config, dry_run=True)
    called = False

    def runner(_operation):
        nonlocal called
        called = True

    guarded = AnnualReportSchedulerCommandService(
        repository=repository,
        config=dry_run_config,
        config_version="assets-config-v1",
        runners={LATEST_BACKFILL_JOB: runner},
    )

    with pytest.raises(
        RuntimeError,
        match="annual_report_asset_dry_run_blocks_job_execution",
    ):
        guarded.execute(started.run_id, principal=_principal())

    assert called is False
    operation = repository.get_operation(started.run_id)
    assert operation is not None
    assert operation.status is OperationStatus.QUEUED


def test_backup_command_rejects_unsupported_bounds_and_records_empty_bounds(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
    )

    with pytest.raises(
        ValueError,
        match="backup job does not support caller bounds",
    ):
        command.start(
            ARCHIVE_BACKUP_JOB,
            principal=_principal(),
            trigger_kind="manual",
            bounds={"max_elapsed_seconds": 30},
        )

    assert repository.list_operations(limit=10) == []
    started = command.start(
        ARCHIVE_BACKUP_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )
    assert started.accepted_bounds == {}
    operation = repository.get_operation(started.run_id)
    assert operation is not None
    assert operation.scope["bounds"] == {}


def test_acquisition_work_fingerprint_covers_route_capability_integrity_and_bounds(
    tmp_path,
):
    config = _config(tmp_path)
    scope = {"instrument_id": "600000.SH", "fiscal_year": 2025}
    baseline = acquisition_work_fingerprint(
        operation_type="ensure_annual_report",
        scope=scope,
        config=config,
        accepted_bounds={"max_pages": 2},
        integrity_policy="hash",
        acquisition_service=_acquisition_service(route=("cninfo",)),
    )
    equivalent = acquisition_work_fingerprint(
        operation_type="ensure_annual_report",
        scope={"fiscal_year": 2025, "instrument_id": "600000.SH"},
        config=config,
        accepted_bounds={"max_pages": 2},
        integrity_policy="HASH",
        acquisition_service=_acquisition_service(route=("cninfo",)),
    )
    assert equivalent == baseline
    assert (
        acquisition_work_fingerprint(
            operation_type="ensure_annual_report",
            scope=scope,
            config=config,
            accepted_bounds={"max_pages": 1},
            integrity_policy="hash",
            acquisition_service=_acquisition_service(route=("cninfo",)),
        )
        != baseline
    )
    assert (
        acquisition_work_fingerprint(
            operation_type="ensure_annual_report",
            scope=scope,
            config=config,
            accepted_bounds={"max_pages": 2},
            integrity_policy="signature",
            acquisition_service=_acquisition_service(route=("cninfo",)),
        )
        != baseline
    )
    assert (
        acquisition_work_fingerprint(
            operation_type="ensure_annual_report",
            scope=scope,
            config=config,
            accepted_bounds={"max_pages": 2},
            integrity_policy="hash",
            acquisition_service=_acquisition_service(
                route=("cninfo",), max_page_size=20
            ),
        )
        != baseline
    )
    assert (
        acquisition_work_fingerprint(
            operation_type="ensure_annual_report",
            scope=scope,
            config=config,
            accepted_bounds={"max_pages": 2},
            integrity_policy="hash",
            acquisition_service=_acquisition_service(route=("sse",)),
        )
        != baseline
    )


@pytest.mark.parametrize(
    "blocker",
    (
        "bootstrap_incomplete",
        "discovery_gaps_present",
        "effective_blob_integrity_failure",
        "storage_unavailable",
        "backup_configuration_disabled",
        "effective_blobs_unprotected",
        "asset_adoption_promotion_incomplete",
    ),
)
def test_command_service_blocks_latest_backfill_from_cron_and_daily_before_readiness(
    tmp_path,
    blocker,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path)
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-v1",
        readiness_gate=lambda job: (False, (blocker,)),
    )
    principal = _service_principal()
    with pytest.raises(RuntimeError, match="manual_only"):
        command.start(
            LATEST_BACKFILL_JOB,
            principal=principal,
            trigger_kind="cron",
        )
    with pytest.raises(RuntimeError, match="readiness_blocked"):
        command.start(
            DAILY_UPDATE_JOB,
            principal=principal,
            trigger_kind="cron",
        )
    assert repository.list_operations(limit=10) == []


def test_command_service_requires_trigger_identity_consistency_before_operation(
    tmp_path,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
    )

    with pytest.raises(PermissionError, match="cron_trigger_requires_service_identity"):
        command.start(
            DAILY_UPDATE_JOB,
            principal=_principal(),
            trigger_kind="cron",
        )
    with pytest.raises(PermissionError, match="service_identity_requires_cron_trigger"):
        command.start(
            LATEST_BACKFILL_JOB,
            principal=_service_principal(),
            trigger_kind="manual",
        )

    assert repository.list_operations(limit=10) == []


def test_command_service_fails_closed_without_trusted_identity(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path, trusted=False),
        config_version="assets-config-v1",
    )
    with pytest.raises(AuthorizationBoundaryUnavailable):
        command.start(
            DAILY_UPDATE_JOB,
            principal=_principal(),
            trigger_kind="manual",
        )


def test_integrity_audit_defaults_to_read_only_and_persists_no_action_flags(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
    )

    started = command.start(
        INTEGRITY_AUDIT_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )

    assert started.normalized_scope["read_only"] is True
    assert not any(started.normalized_scope["action_flags"].values())
    assert repository.get_operation(started.run_id) is not None


@pytest.mark.parametrize(
    ("scope", "action_flags", "error"),
    [
        ({}, {"delete": True}, "explicit target scope"),
        (
            {"content_hashes": ["a" * 64]},
            {"delete": True},
            "requires explicit deletion_ids",
        ),
        (
            {"deletion_ids": ["deletion-1"]},
            {"network_repair": True},
            "requires explicit content_hashes",
        ),
        (
            {
                "content_hashes": ["a" * 64],
                "deletion_ids": ["deletion-1"],
            },
            {"network_repair": True},
            "targets not used",
        ),
    ],
)
def test_integrity_destructive_actions_require_exact_target_type_before_operation(
    tmp_path,
    scope,
    action_flags,
    error,
):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
    )

    with pytest.raises(ValueError, match=error):
        command.start(
            INTEGRITY_AUDIT_JOB,
            principal=_principal(),
            trigger_kind="manual",
            scope=scope,
            action_flags=action_flags,
        )

    assert repository.list_operations(limit=10) == []
    assert repository.list_job_command_audit() == []


def test_integrity_overbroad_target_scope_creates_no_operation(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path)
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-v1",
    )
    hashes = [
        f"{index:064x}"
        for index in range(config.discovery.max_instruments + 1)
    ]

    with pytest.raises(ValueError, match="exceeds configured bound"):
        command.start(
            INTEGRITY_AUDIT_JOB,
            principal=_principal(),
            trigger_kind="manual",
            scope={"content_hashes": hashes},
            action_flags={"network_repair": True},
        )

    assert repository.list_operations(limit=10) == []


def test_integrity_cron_rejects_destructive_flags_before_operation(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
    )

    with pytest.raises(ValueError, match="must remain read-only"):
        command.start(
            INTEGRITY_AUDIT_JOB,
            principal=_principal(),
            trigger_kind="cron",
            scope={"deletion_ids": ["deletion-1"]},
            action_flags={"delete": True},
        )

    assert repository.list_operations(limit=10) == []


def test_non_integrity_job_rejects_action_or_target_scope_before_operation(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
    )

    with pytest.raises(ValueError, match="only valid for integrity audit"):
        command.start(
            DAILY_UPDATE_JOB,
            principal=_principal(),
            trigger_kind="manual",
            action_flags={"delete": True},
        )
    with pytest.raises(ValueError, match="target scope is only valid"):
        command.start(
            DAILY_UPDATE_JOB,
            principal=_principal(),
            trigger_kind="manual",
            scope={"content_hashes": ["a" * 64]},
        )

    assert repository.list_operations(limit=10) == []


def test_degraded_integrity_result_is_not_reported_as_full_success(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
        runners={INTEGRITY_AUDIT_JOB: lambda operation: {"status": "degraded"}},
    )
    started = command.start(
        INTEGRITY_AUDIT_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )

    completed = command.execute(started.run_id, principal=_principal())

    assert completed.status is OperationStatus.COMPLETED
    assert completed.outcome is BatchOutcome.PARTIAL


def test_command_runner_heartbeats_running_operation(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    config = _config(tmp_path)
    config = AnnouncementAssetConfig.from_mapping(
        {
            **config.normalized_mapping(),
                "retry": {
                    **config.normalized_mapping()["retry"],
                    "lease_seconds": 30,
                    "heartbeat_seconds": 1,
                    "lease_safety_grace_seconds": 1,
                },
        },
        project_root=tmp_path,
    )
    started = Event()
    release = Event()

    def runner(operation):
        started.set()
        assert release.wait(timeout=5)
        return {"status": "success"}

    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="assets-config-v1",
        runners={LATEST_BACKFILL_JOB: runner},
    )
    run = command.start(
        LATEST_BACKFILL_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )
    from threading import Thread

    result: list = []
    thread = Thread(
        target=lambda: result.append(command.execute(run.run_id, principal=_principal()))
    )
    thread.start()
    assert started.wait(timeout=2)
    original = repository.get_operation(run.run_id)
    assert original is not None
    sleep(1.2)
    refreshed = repository.get_operation(run.run_id)
    assert refreshed is not None
    assert refreshed.heartbeat_at != original.heartbeat_at
    release.set()
    thread.join(timeout=5)
    assert result[0].status is OperationStatus.COMPLETED


def test_unexpected_runner_exception_reaches_failed_terminal_state(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
        runners={LATEST_BACKFILL_JOB: lambda operation: (_ for _ in ()).throw(sqlite3.Error("boom"))},
    )
    run = command.start(
        LATEST_BACKFILL_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )

    completed = command.execute(run.run_id, principal=_principal())

    assert completed.status is OperationStatus.FAILED
    assert completed.reason_code == "job_runner_exception"
    assert completed.diagnostics["error_type"] == "Error"


def test_expired_same_principal_worker_cannot_terminalize_reclaimed_run(tmp_path):
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    reclaimed = []

    def runner(operation):
        with repository.transaction() as conn:
            conn.execute(
                "UPDATE official_asset_operations SET lease_expires_at=? "
                "WHERE operation_id=?",
                ("2000-01-01T00:00:00+00:00", operation.operation_id),
            )
        reclaimed.append(
            repository.claim_operation(
                operation.operation_id,
                lease_owner=_principal().principal_id,
                lease_expires_at="2099-01-01T00:00:00+00:00",
            )
        )
        return {"status": "success"}

    command = AnnualReportSchedulerCommandService(
        repository=repository,
        config=_config(tmp_path),
        config_version="assets-config-v1",
        runners={LATEST_BACKFILL_JOB: runner},
    )
    run = command.start(
        LATEST_BACKFILL_JOB,
        principal=_principal(),
        trigger_kind="manual",
    )

    result = command.execute(run.run_id, principal=_principal())

    assert result.status is OperationStatus.RUNNING
    assert result.lease_generation == reclaimed[0].lease_generation
    assert result.outcome is None
