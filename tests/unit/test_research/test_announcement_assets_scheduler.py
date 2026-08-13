from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from research.announcement_assets import (
    DAILY_UPDATE_JOB,
    LATEST_BACKFILL_JOB,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AnnualReportCronAdapter,
    AnnualReportOperatorAdapter,
    AnnualReportSchedulerCommandService,
    AuthorizationBoundaryUnavailable,
    CommandPrincipal,
    ContentAddressedBlobStore,
    EnsureRequest,
    OperationStage,
    OperationStatus,
    annual_report_scheduler_job_definitions,
    daily_schedule_fingerprint,
    latest_backfill_schedule_fingerprint,
)
from research.announcements import (
    AnnouncementAttachment,
    AnnouncementRecord,
    AnnouncementRetrievalResult,
    build_announcement_key,
)


def _config(
    tmp_path: Path,
    *,
    scheduled: bool = True,
    trusted: bool = True,
) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "scheduled_enabled": scheduled,
            "dry_run": False,
            "active_exchanges": ["SSE"],
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
            "storage": {
                "warning_utilization": 0.98,
                "hard_stop_utilization": 0.999,
                "free_space_reserve_bytes": 1,
                "max_attachment_bytes": 1024 * 1024,
                "unknown_length_reservation_bytes": 4096,
            },
            "discovery": {
                "max_pages": 2,
                "page_size": 10,
                "max_requests": 20,
                "max_windows": 4,
                "max_instruments": 10,
                "max_elapsed_seconds": 60,
            },
            "acquisition": {
                "source_routes": ["cninfo"],
                "normalized_categories": ["annual_report"],
                "download_concurrency": 1,
                "per_source_concurrency": 1,
            },
            "jobs": {
                "latest_backfill_manual_only": True,
                "daily_enabled": scheduled,
                "daily_cron": "15 3 * * *",
                "integrity_enabled": False,
                "backup_enabled": False,
            },
            "permissions": {
                "trusted_identity_enabled": trusted,
                "operator": "annual_report_assets:operator",
                "principals": (
                    [
                        {
                            "principal": principal,
                            "token_env": token_env,
                            "scopes": ["annual_report_assets:operator"],
                        }
                        for principal, token_env in (
                            ("operator:alice", "ANNOUNCEMENT_ASSET_OPERATOR_TOKEN"),
                            ("service:annual-report-cron", "ANNOUNCEMENT_ASSET_CRON_TOKEN"),
                            ("service:cron", "ANNOUNCEMENT_ASSET_GENERIC_CRON_TOKEN"),
                        )
                    ]
                    if trusted
                    else []
                ),
            },
        },
        project_root=tmp_path,
    )


def _principal(name: str, *, permitted: bool = True, service: bool = False):
    return CommandPrincipal(
        principal_id=name,
        permissions=(
            frozenset({"annual_report_assets:operator"})
            if permitted
            else frozenset()
        ),
        service_identity=service,
    )


def _commands(tmp_path: Path, *, scheduled: bool = True, trusted: bool = True):
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = _config(tmp_path, scheduled=scheduled, trusted=trusted)
    repository = AnnouncementAssetRepository(tmp_path / "research.db")
    repository.initialize_schema()
    runners = {
        DAILY_UPDATE_JOB: lambda operation: {
            "status": "success",
            "run_cutoff": "2026-08-10T03:00:00+00:00",
            "errors": (),
            "operation_id": operation.operation_id,
        },
        LATEST_BACKFILL_JOB: lambda operation: {
            "status": "partial",
            "run_cutoff": "2026-08-10T03:00:00+00:00",
            "errors": ("bounded_target_pending",),
        },
    }
    commands = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="announcement-assets-config.v7",
        runners=runners,
        readiness_gate=lambda job: (True, ()),
    )
    return config, repository, commands


def test_scheduler_definitions_are_independent_and_side_effect_free(tmp_path):
    config, repository, _ = _commands(tmp_path)
    definitions = annual_report_scheduler_job_definitions(config)
    assert [item.name for item in definitions] == [
        LATEST_BACKFILL_JOB,
        DAILY_UPDATE_JOB,
        "annual_report_asset_integrity_audit",
        "annual_report_asset_backup",
    ]
    assert definitions[0].manual_only is True
    assert definitions[0].timezone == config.timezone
    assert definitions[0].cadence_fingerprint == latest_backfill_schedule_fingerprint(
        config
    )
    assert definitions[1].enabled is True
    assert definitions[1].timezone == config.timezone
    assert definitions[1].overlap_days == config.discovery.overlap_days
    assert definitions[1].catch_up_max_days == config.daily_catch_up_max_days
    assert definitions[1].minimum_runs_per_calendar_day == (
        config.daily_min_runs_per_calendar_day
    )
    assert definitions[1].cadence_fingerprint == daily_schedule_fingerprint(config)
    assert definitions[2].manual_only is True
    assert definitions[2].enabled is False
    assert definitions[3].enabled is False
    assert repository.list_operations(limit=10) == []
    assert "business_profile" not in repr(definitions)
    assert "broker" not in repr(definitions)


def test_scheduler_definitions_fail_closed_without_required_capacity_artifact(
    tmp_path,
):
    config = replace(_config(tmp_path), capacity_artifact_required=True)

    definitions = annual_report_scheduler_job_definitions(config)

    assert all(item.enabled is False for item in definitions)


def test_cron_cli_and_api_share_one_durable_run_and_audit_principals(tmp_path):
    config, repository, commands = _commands(tmp_path)
    operator = _principal("operator:alice")
    service = _principal("service:annual-report-cron", service=True)
    adapter = AnnualReportOperatorAdapter(commands)
    scope = {"run_cutoff": "2026-08-10T03:00:00+00:00"}

    cli = adapter.start(
        DAILY_UPDATE_JOB,
        principal=operator,
        adapter_kind="cli",
        scope=scope,
    )
    api = adapter.start(
        DAILY_UPDATE_JOB,
        principal=operator,
        adapter_kind="api",
        scope=scope,
    )
    cron = AnnualReportCronAdapter(commands, service).start_daily(scope=scope)
    assert cli.run_id == api.run_id == cron.run_id
    assert cli.reused is False
    assert api.reused is True and cron.reused is True
    assert cli.run_id == repository.get_operation(cli.run_id).operation_id
    persisted_scope = repository.get_operation(cli.run_id).scope
    assert persisted_scope["schedule_timezone"] == "Asia/Shanghai"
    assert persisted_scope["schedule_cron"] == config.jobs.daily_cron
    assert persisted_scope["overlap_days"] == config.discovery.overlap_days
    assert persisted_scope["catch_up_max_days"] == config.daily_catch_up_max_days
    assert persisted_scope["cadence_fingerprint"] == daily_schedule_fingerprint(config)

    audit = repository.list_job_command_audit(operation_id=cli.run_id)
    assert {row["trigger_kind"] for row in audit} == {"cli", "api", "cron"}
    assert {row["principal"] for row in audit} == {
        "operator:alice",
        "service:annual-report-cron",
    }
    assert {row["config_version"] for row in audit} == {
        "announcement-assets-config.v7"
    }

    completed = commands.execute(cli.run_id, principal=service)
    assert completed.status is OperationStatus.COMPLETED
    assert completed.outcome.value == "success"
    assert completed.progress["run_cutoff"] == "2026-08-10T03:00:00+00:00"
    history = commands.history(
        principal=operator,
        job_name=DAILY_UPDATE_JOB,
        now="2026-08-10T04:00:00+00:00",
    )
    assert history.runs[0].operation_id == cli.run_id
    assert list(history.last_successful_cutoff.values()) == [
        "2026-08-10T03:00:00+00:00"
    ]


def test_latest_backfill_operator_scope_is_manual_and_has_no_cron_contract(tmp_path):
    config, repository, commands = _commands(tmp_path)
    operator = _principal("operator:alice")
    started = AnnualReportOperatorAdapter(commands).start(
        LATEST_BACKFILL_JOB,
        principal=operator,
        adapter_kind="cli",
        scope={"as_of": "2026-08-10"},
    )
    operation = repository.get_operation(started.run_id)
    assert operation is not None
    assert operation.scope["manual_only"] is True
    assert operation.scope.get("cron") is None
    assert operation.scope["schedule_timezone"] == config.timezone
    assert operation.scope["cadence_fingerprint"] == (
        latest_backfill_schedule_fingerprint(config)
    )


def test_stop_and_resume_keep_run_id_increment_generation_and_attempt(tmp_path):
    _, repository, commands = _commands(tmp_path)
    operator = _principal("operator:alice")
    started = commands.start(
        LATEST_BACKFILL_JOB,
        principal=operator,
        trigger_kind="cli",
        scope={"as_of": "2026-08-10"},
    )
    stopped = commands.stop(started.run_id, principal=operator)
    assert stopped.status is OperationStatus.CANCELLED
    assert stopped.progress["stop_requested"] is True

    resumed = commands.resume(started.run_id, principal=operator)
    assert resumed.operation_id == started.run_id
    assert resumed.status is OperationStatus.QUEUED
    assert resumed.progress["resume_generation"] == 1
    completed = commands.execute(started.run_id, principal=operator)
    assert completed.operation_id == started.run_id
    assert completed.status is OperationStatus.COMPLETED
    assert completed.outcome.value == "partial"
    assert completed.attempt == 1
    assert [
        row["command"]
        for row in reversed(
            repository.list_job_command_audit(operation_id=started.run_id)
        )
    ] == ["start", "stop", "resume", "execute"]


def test_running_stop_is_cooperative_and_non_cancellable_stage_rejects(tmp_path):
    config, repository, commands = _commands(tmp_path)
    operator = _principal("operator:alice")
    started = commands.start(
        DAILY_UPDATE_JOB,
        principal=operator,
        trigger_kind="cli",
        scope={"run_cutoff": "2026-08-11T03:00:00+00:00"},
    )
    repository.claim_operation(
        started.run_id,
        lease_owner=operator.principal_id,
        lease_expires_at="2026-08-11T04:00:00+00:00",
        stage=OperationStage.DISCOVERING,
    )
    requested = commands.stop(started.run_id, principal=operator)
    assert requested.status is OperationStatus.RUNNING
    assert requested.progress["stop_requested"] is True
    cancelled = commands.execute(started.run_id, principal=operator)
    assert cancelled.status is OperationStatus.CANCELLED
    assert cancelled.reason_code == "operator_stop"

    backup_like, _ = repository.create_or_reuse_operation(
        operation_type=DAILY_UPDATE_JOB,
        idempotency_key="non-cancellable-stage",
        scope={"run_cutoff": "2026-08-12T03:00:00+00:00"},
        policy_version=config.policy_version,
    )
    repository.claim_operation(
        backup_like.operation_id,
        lease_owner=operator.principal_id,
        lease_expires_at="2026-08-12T04:00:00+00:00",
        stage=OperationStage.BACKING_UP,
    )
    with pytest.raises(RuntimeError, match="not cooperatively cancellable"):
        commands.stop(backup_like.operation_id, principal=operator)


def test_stale_running_job_resumes_same_id_but_live_lease_is_rejected(tmp_path):
    _, repository, commands = _commands(tmp_path)
    operator = _principal("operator:alice")
    started = commands.start(
        DAILY_UPDATE_JOB,
        principal=operator,
        trigger_kind="cli",
        scope={"run_cutoff": "2026-08-13T03:00:00+00:00"},
    )
    repository.claim_operation(
        started.run_id,
        lease_owner="dead-worker",
        lease_expires_at="2000-01-01T00:00:00+00:00",
    )
    resumed = commands.resume(started.run_id, principal=operator)
    assert resumed.operation_id == started.run_id
    assert resumed.progress["resume_generation"] == 1
    repository.claim_operation(
        started.run_id,
        lease_owner="live-worker",
        lease_expires_at="2200-01-01T00:00:00+00:00",
    )
    with pytest.raises(ValueError, match="not resumable"):
        commands.resume(started.run_id, principal=operator)


def test_control_plane_auth_fails_before_mutation(tmp_path):
    _, repository, commands = _commands(tmp_path)
    with pytest.raises(PermissionError, match="operator_permission_required"):
        commands.start(
            DAILY_UPDATE_JOB,
            principal=_principal("user:bob", permitted=False),
            trigger_kind="api",
        )
    assert repository.list_operations(limit=10) == []

    _, unavailable_repository, unavailable = _commands(
        tmp_path / "untrusted", trusted=False
    )
    with pytest.raises(
        AuthorizationBoundaryUnavailable, match="authorization_boundary_unavailable"
    ):
        unavailable.start(
            DAILY_UPDATE_JOB,
            principal=_principal("operator:alice"),
            trigger_kind="api",
        )
    assert unavailable_repository.list_operations(limit=10) == []


class _Retriever:
    def __init__(self):
        self.calls: list[str] = []

    def retrieve(self, source, attachment, *, require_pdf=False):
        self.calls.append(str(attachment.attachment_id))
        content = b"%PDF-1.4\nlocal read\n%%EOF\n"
        return AnnouncementRetrievalResult(
            source=source,
            attachment=attachment,
            status="success",
            content=content,
            content_hash=hashlib.sha256(content).hexdigest(),
            content_length=len(content),
            final_url=attachment.source_url,
            response_media_type="application/pdf",
            retrieved_at="2026-08-10T02:00:00+00:00",
            signature_status="valid_pdf",
        )


def test_disabled_cron_does_not_disable_local_reads_or_on_demand_ensure(tmp_path):
    config, repository, commands = _commands(tmp_path, scheduled=False)
    retriever = _Retriever()
    store = ContentAddressedBlobStore(config)
    store.prepare()
    service = AnnouncementAssetService(
        repository=repository,
        config=config,
        blob_store=store,
        attachment_retriever=retriever,
    )
    record = AnnouncementRecord(
        source="cninfo",
        source_announcement_id="local-original",
        announcement_key=build_announcement_key("cninfo", "local-original"),
        title="测试公司2025年年度报告",
        published_at="2026-03-20T01:00:00+00:00",
        exchange="SSE",
        symbols=("600000",),
        attachments=(
            AnnouncementAttachment(
                source_url="https://static.example/local-original.pdf",
                attachment_id="local-original",
                name="local-original.pdf",
                media_type="application/pdf",
            ),
        ),
        raw_payload={"announcementId": "local-original"},
    )
    registered = service.register_discovered_record(
        record, instrument_id="600000.SH"
    )[0]
    assert service.acquire_attachment(registered.attachment_id) is not None
    before = tuple(retriever.calls)

    definitions = annual_report_scheduler_job_definitions(config)
    assert definitions[1].enabled is False
    with pytest.raises(RuntimeError, match="daily_cron_disabled"):
        AnnualReportCronAdapter(
            commands, _principal("service:cron", service=True)
        ).start_daily()
    local = service.ensure_annual_report(
        EnsureRequest(
            instrument_id="600000.SH",
            fiscal_year=2025,
            allow_network=False,
        )
    )
    assert local.disposition.value == "local_hit"
    assert tuple(retriever.calls) == before
    assert repository.list_operations(limit=10) == []


@pytest.mark.asyncio
async def test_scheduled_tasks_expose_and_delegate_all_four_asset_jobs(monkeypatch):
    import scheduler.tasks as scheduler_tasks_module

    manager = SimpleNamespace(
        run_annual_report_asset_latest_backfill=AsyncMock(
            return_value={"status": "completed", "outcome": "success"}
        ),
        run_annual_report_asset_daily_update=AsyncMock(
            return_value={"status": "completed", "outcome": "partial"}
        ),
        run_annual_report_asset_integrity_audit=AsyncMock(
            return_value={"status": "completed", "outcome": "success"}
        ),
        run_annual_report_asset_backup=AsyncMock(
            return_value={"status": "completed", "outcome": "success"}
        ),
    )
    monkeypatch.setattr(scheduler_tasks_module, "data_manager", manager)
    tasks = object.__new__(scheduler_tasks_module.ScheduledTasks)
    tasks._send_task_report = AsyncMock(return_value=False)

    assert await tasks.annual_report_asset_latest_backfill(
        as_of="2026-08-12"
    )
    assert await tasks.annual_report_asset_daily_update(
        timezone="Asia/Shanghai",
        overlap_days=3,
        catch_up_max_days=14,
        minimum_runs_per_calendar_day=1,
        universe_refresh_cadence="before_each_daily_run.v1",
    )
    assert await tasks.annual_report_asset_integrity_audit(read_only=True)
    assert await tasks.annual_report_asset_backup(
        recovery_journal_retention_policy=(
            "append_only_no_automatic_gc.v1"
        ),
        recovery_journal_integrity_policy=(
            "sha256_chain_with_watermarks.v1"
        ),
    )

    manager.run_annual_report_asset_latest_backfill.assert_awaited_once_with(
        as_of="2026-08-12",
        bounds=None,
        trigger_kind="manual",
    )
    daily = manager.run_annual_report_asset_daily_update.await_args.kwargs
    assert daily["trigger_kind"] == "cron"
    assert daily["principal_id"] == "service:annual-report-asset-scheduler"
    assert daily["expected_schedule"]["overlap_days"] == 3
    manager.run_annual_report_asset_integrity_audit.assert_awaited_once()
    backup = manager.run_annual_report_asset_backup.await_args.kwargs
    assert backup["trigger_kind"] == "cron"
    assert backup["principal_id"] == "service:annual-report-asset-scheduler"
    assert tasks._send_task_report.await_count == 4


@pytest.mark.asyncio
async def test_scheduled_integrity_repair_flags_fail_before_dispatch(monkeypatch):
    import scheduler.tasks as scheduler_tasks_module

    dispatch = AsyncMock()
    monkeypatch.setattr(
        scheduler_tasks_module,
        "data_manager",
        SimpleNamespace(run_annual_report_asset_integrity_audit=dispatch),
    )
    tasks = object.__new__(scheduler_tasks_module.ScheduledTasks)

    with pytest.raises(ValueError, match="read-only.*repair actions"):
        await tasks.annual_report_asset_integrity_audit(
            read_only=True,
            content_hashes=["a" * 64],
            action_flags={"quarantine": True},
        )
    with pytest.raises(ValueError, match="requires an action flag"):
        await tasks.annual_report_asset_integrity_audit(read_only=False)
    dispatch.assert_not_awaited()


@pytest.mark.asyncio
async def test_low_frequency_integrity_registration_uses_cron_trigger(monkeypatch):
    import scheduler.tasks as scheduler_tasks_module

    dispatch = AsyncMock(
        return_value={"status": "completed", "outcome": "success"}
    )
    monkeypatch.setattr(
        scheduler_tasks_module,
        "data_manager",
        SimpleNamespace(run_annual_report_asset_integrity_audit=dispatch),
    )
    tasks = object.__new__(scheduler_tasks_module.ScheduledTasks)
    tasks._send_task_report = AsyncMock(return_value=False)

    assert await tasks.annual_report_asset_integrity_audit(
        read_only=True,
        job_config=SimpleNamespace(manual_only=False, report=False),
    )

    assert dispatch.await_args.kwargs["trigger_kind"] == "cron"
