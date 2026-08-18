from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock

import pytest

from data_manager import DataManager
from research.announcement_assets import (
    DAILY_UPDATE_JOB,
    LATEST_BACKFILL_JOB,
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnualReportSchedulerCommandService,
    BatchOutcome,
    OperationStage,
    OperationStatus,
)


def _profile_payload() -> dict:
    return {
        "schema_version": "company_business_profile.v1",
        "status": "ready",
        "instrument_id": "600000.SH",
        "data_available_cutoff": "2026-04-30",
        "industry_default_profile": {},
        "company_specific_profile": {},
        "segment_profiles": [],
        "approved_exposures": [],
        "candidate_exposures": [],
        "candidate_facts": {},
        "exceptions": [],
        "profile_lifecycle": {},
        "executable_exposure_mappings": [],
        "model_scores": {},
        "model_recommendation": "company_specific",
        "conflicts": [],
        "warnings": [],
        "profile_version": "profile-v1",
        "lineage_hash": "lineage-v1",
        "readiness": {"status": "ready"},
    }


@pytest.mark.asyncio
async def test_business_profile_get_adds_zero_network_shared_asset_lineage():
    manager = object.__new__(DataManager)
    manager._require_research_storage = lambda: object()
    manager._get_dcf_industry_membership = AsyncMock(return_value={})
    manager._resolve_business_profile_context = AsyncMock(
        return_value=_profile_payload()
    )
    provider_trap = AsyncMock(side_effect=AssertionError("provider must not run"))
    repository = SimpleNamespace(schema_initialized=lambda: True)
    asset = {
        "asset_id": "asset-current",
        "asset_availability": "local_valid",
        "availability": "local_valid",
        "instrument_id": "600000.SH",
        "fiscal_year": 2025,
        "report_period": "2025-12-31",
        "source": "cninfo",
        "source_announcement_id": "filing-current",
        "filing_id": "filing-current",
        "attachment_id": "attachment-current",
        "observation_version": "version-current",
        "version_available_at": "2026-04-30T10:00:00+00:00",
        "variant": "correction",
        "content_hash": "a" * 64,
        "content_length": 1024,
        "integrity": "valid",
        "effective_decision_state": "current",
        "canonical_source_filing": {
            "source": "cninfo",
            "source_announcement_id": "filing-current",
        },
        "equivalent_source_filings": [],
        "canonical_projection_policy_version": "canonical_source_filing.v1",
        "evidence_set_hash": "b" * 64,
    }
    access = SimpleNamespace(
        repository=repository,
        get_effective_asset=lambda *args, **kwargs: asset,
        provider_trap=provider_trap,
    )
    manager._get_announcement_asset_access = lambda **kwargs: access

    payload = await manager.get_research_company_business_profile(
        "600000.SH",
        as_of_date="2026-04-30",
    )

    assert payload["source_assets"]["annual_report_asset"] == asset
    assert (
        payload["source_assets"]["annual_report_asset"]["version_available_at"]
        == "2026-04-30T10:00:00+00:00"
    )
    assert payload["consumer_processing_status"] is None
    provider_trap.assert_not_awaited()


@pytest.mark.asyncio
async def test_business_profile_get_keeps_nullable_lineage_before_schema_exists():
    manager = object.__new__(DataManager)
    manager._require_research_storage = lambda: object()
    manager._get_dcf_industry_membership = AsyncMock(return_value={})
    manager._resolve_business_profile_context = AsyncMock(
        return_value=_profile_payload()
    )
    manager._get_announcement_asset_access = lambda **kwargs: SimpleNamespace(
        repository=SimpleNamespace(schema_initialized=lambda: False)
    )

    payload = await manager.get_research_company_business_profile("600000.SH")

    assert payload["source_assets"] == {"annual_report_asset": None}
    assert payload["consumer_processing_status"] is None


@pytest.mark.asyncio
async def test_explicit_restart_recovery_resumes_asset_work():
    manager = object.__new__(DataManager)
    asset_resume = lambda **kwargs: ("operation-1", "operation-2")
    access = SimpleNamespace(
        repository=SimpleNamespace(schema_initialized=lambda: True),
        service=SimpleNamespace(resume_pending_ensure_operations=asset_resume),
    )
    manager._get_announcement_asset_access = lambda **kwargs: access

    result = await manager.resume_shared_annual_report_pending_work(limit=25)

    assert result == {
        "asset_operation_ids": ["operation-1", "operation-2"],
        "resumed": 2,
    }


@pytest.mark.asyncio
async def test_explicit_restart_recovery_does_not_initialize_or_dispatch_empty_catalog():
    manager = object.__new__(DataManager)
    access = SimpleNamespace(
        repository=SimpleNamespace(schema_initialized=lambda: False),
        service=SimpleNamespace(
            resume_pending_ensure_operations=lambda **kwargs: (_ for _ in ()).throw(
                AssertionError("empty catalog must not dispatch")
            )
        ),
    )
    manager._get_announcement_asset_access = lambda **kwargs: access

    result = await manager.resume_shared_annual_report_pending_work()

    assert result == {
        "asset_operation_ids": [],
        "resumed": 0,
    }


@pytest.mark.asyncio
def _runtime_config(tmp_path: Path) -> AnnouncementAssetConfig:
    return AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "scheduled_enabled": True,
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
                "max_pages": 20,
                "page_size": 10,
                "max_requests": 30,
                "max_windows": 8,
                "max_instruments": 10,
                "max_elapsed_seconds": 60,
            },
            "acquisition": {
                "source_routes": ["cninfo"],
                "normalized_categories": ["annual_report"],
                "download_concurrency": 1,
                "per_source_concurrency": 1,
                "max_task_download_bytes": 1024 * 1024,
            },
            "jobs": {
                "daily_enabled": True,
                "daily_cron": "15 3 * * *",
            },
            "permissions": {
                "trusted_identity_enabled": True,
                "principals": [
                    {
                        "principal": "service:annual-report-asset-scheduler",
                        "token_env": "ANNOUNCEMENT_ASSET_SERVICE_TOKEN",
                        "scopes": ["annual_report_assets:operator"],
                    },
                    {
                        "principal": "operator:test",
                        "token_env": "ANNOUNCEMENT_ASSET_OPERATOR_TEST_TOKEN",
                        "scopes": ["annual_report_assets:operator"],
                    },
                ],
            },
        },
        project_root=tmp_path,
    )


def test_enabled_access_lazily_binds_real_discovery_and_retrieval(
    tmp_path,
    monkeypatch,
):
    from research.announcements import AnnouncementAttachmentRetriever

    manager = object.__new__(DataManager)
    manager.research_config = SimpleNamespace(
        modules={
            "official_announcement_assets": {
                **_runtime_config(Path.cwd()).normalized_mapping(),
                "paths": {
                    "filings_root": "data/filings",
                    "archive_root": "data/filings/announcements",
                    "temp_root": "data/filings/announcements/tmp",
                    "quarantine_root": "data/filings/announcements/quarantine",
                    "require_mount": False,
                },
            }
        },
        storage=SimpleNamespace(db_path=str(tmp_path / "research.db")),
    )
    manager._announcement_asset_access = None
    manager._announcement_asset_access_signature = None
    acquisition = SimpleNamespace(
        config=SimpleNamespace(provider_configs={"cninfo": {"enabled": True}})
    )
    manager._build_official_announcement_acquisition_service = MagicMock(
        return_value=acquisition
    )
    retriever = object()
    retrieval_factory = MagicMock(return_value=retriever)
    monkeypatch.setattr(
        AnnouncementAttachmentRetriever,
        "from_provider_configs",
        retrieval_factory,
    )

    access = manager._get_announcement_asset_access(initialize_schema=False)

    assert access.service.acquisition_service is acquisition
    assert access.service.attachment_retriever is retriever
    manager._build_official_announcement_acquisition_service.assert_called_once_with()
    retrieval_factory.assert_called_once_with(
        {
            "cninfo": {
                "enabled": True,
                "max_attachment_bytes": 1024 * 1024,
            }
        }
    )
    assert acquisition.config.provider_configs == {"cninfo": {"enabled": True}}
    assert not (tmp_path / "research.db").exists()


def test_asset_runtime_binds_exchange_routes_without_changing_other_purposes(
    tmp_path,
):
    from research.announcements import (
        AnnouncementAcquisitionConfig,
        AnnouncementAcquisitionService,
        AnnouncementProviderCapabilities,
        AnnouncementProviderRegistry,
        AnnouncementRouteConfig,
    )

    providers = []
    for source, exchanges in (
        ("cninfo", {"SSE", "SZSE", "BSE"}),
        ("sse", {"SSE"}),
        ("szse", {"SZSE"}),
        ("bse", {"BSE"}),
    ):
        providers.append(
            SimpleNamespace(
                source_name=source,
                capabilities=AnnouncementProviderCapabilities(
                    exchanges=frozenset(exchanges),
                    supports_market_scope=True,
                    supports_instrument_scope=source != "bse",
                    supports_date_filter=True,
                    supports_category_filter=source != "bse",
                ),
            )
        )
    service = AnnouncementAcquisitionService(
        registry=AnnouncementProviderRegistry(providers),
        config=AnnouncementAcquisitionConfig(
            default_route=AnnouncementRouteConfig(sources=("cninfo",)),
            purpose_routes={
                "business_profile_evidence": {
                    "SSE": AnnouncementRouteConfig(sources=("cninfo",))
                }
            },
        ),
    )

    asset_config = _runtime_config(tmp_path)
    asset_config = replace(
        asset_config,
        exchanges=("SSE", "SZSE", "BSE"),
        acquisition=replace(
            asset_config.acquisition,
            source_routes=("cninfo", "sse", "szse", "bse"),
        ),
    )
    bound = DataManager._bind_announcement_asset_routes(
        service,
        asset_config,
    )

    assert bound.config.route_for("official_announcement_assets", "SSE").sources == (
        "sse",
        "cninfo",
    )
    assert bound.config.route_for("official_announcement_assets", "SZSE").sources == (
        "szse",
        "cninfo",
    )
    assert bound.config.route_for("official_announcement_assets", "BSE").sources == (
        "cninfo",
    )
    assert bound.config.route_for("business_profile_evidence", "SSE").sources == (
        "cninfo",
    )


def test_scheduler_command_plane_registers_shared_asset_runners(tmp_path):
    manager = object.__new__(DataManager)
    manager._announcement_asset_access_signature = ("research.db", "config")
    manager._announcement_asset_scheduler_commands = None
    manager._announcement_asset_scheduler_commands_signature = None
    config = _runtime_config(tmp_path)
    access = SimpleNamespace(
        config=config,
        repository=object(),
        service=SimpleNamespace(acquisition_service=object()),
    )
    manager._get_announcement_asset_access = lambda **kwargs: access

    commands = manager._get_announcement_asset_scheduler_commands()

    assert set(commands.runners) == {
        LATEST_BACKFILL_JOB,
        DAILY_UPDATE_JOB,
    }
    assert all(
        runner == manager._run_announcement_asset_operation
        for runner in commands.runners.values()
    )
    assert commands.acquisition_service is access.service.acquisition_service
    assert commands.readiness_gate is None


def test_accepted_command_bounds_are_applied_to_worker_config(tmp_path):
    config = _runtime_config(tmp_path)
    operation = SimpleNamespace(
        scope={
            "bounds": {
                "max_pages": 3,
                "max_requests": 4,
                "max_windows": 2,
                "max_instruments": 5,
                "max_elapsed_seconds": 6,
                "max_download_bytes": 7000,
            }
        },
        progress={},
    )

    bounded = DataManager._announcement_asset_bounded_config(config, operation)

    assert bounded.discovery.max_pages == 3
    assert bounded.discovery.max_requests == 4
    assert bounded.discovery.max_windows == 2
    assert bounded.discovery.max_instruments == 5
    assert bounded.discovery.max_elapsed_seconds == 6
    assert bounded.acquisition.max_task_download_bytes == 7000
    assert config.discovery.max_pages == 20
    assert config.acquisition.max_task_download_bytes == 1024 * 1024


def test_universe_freshness_fails_closed_without_authoritative_refresh_watermark(
    tmp_path,
):
    config = _runtime_config(tmp_path)
    repository = SimpleNamespace(
        get_latest_complete_listed_security_census_snapshot=lambda: None
    )
    manager = object.__new__(DataManager)
    manager.db_ops = SimpleNamespace(
        get_research_target_instruments_by_exchange_sync=lambda *args, **kwargs: [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "status": "listed",
                "is_active": True,
                "updated_at": "2026-08-10T00:00:00+00:00",
            },
            {
                "instrument_id": "600001.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "status": "listed",
                "is_active": True,
                "updated_at": "2026-08-12T00:00:00+00:00",
            },
        ]
    )

    snapshot = manager._materialize_announcement_asset_universe(
        repository=repository,
        config=config,
        snapshot_at="2026-08-12T01:00:00+00:00",
    )

    assert snapshot.master_data_last_success_at is None
    assert snapshot.status == "eligibility_indeterminate"
    assert snapshot.indeterminate[-1]["reason"] == (
        "missing_authoritative_master_refresh_watermark"
    )


def test_universe_uses_persisted_authoritative_full_refresh_watermark(tmp_path):
    config = _runtime_config(tmp_path)
    completed_at = "2026-08-12T00:00:00+00:00"
    repository = SimpleNamespace(
        list_operational_reports=lambda **kwargs: [
            {
                "config_fingerprint": config.evidence_fingerprint,
                "payload": {
                    "status": "complete",
                    "scope": "full_refresh",
                    "source": "instrument_master_governance:a_share_stock",
                    "watermark": "master-refresh-1",
                    "exchanges": ["SSE"],
                    "completed_at": completed_at,
                },
            }
        ],
        get_latest_complete_listed_security_census_snapshot=lambda: None,
    )
    manager = object.__new__(DataManager)
    manager.db_ops = SimpleNamespace(
        get_research_target_instruments_by_exchange_sync=lambda *args, **kwargs: [
            {
                "instrument_id": "600000.SH",
                "exchange": "SSE",
                "type": "stock",
                "currency": "CNY",
                "status": "listed",
                "is_active": True,
                "updated_at": "2026-07-01T00:00:00+00:00",
            }
        ]
    )

    snapshot = manager._materialize_announcement_asset_universe(
        repository=repository,
        config=config,
        snapshot_at="2026-08-12T01:00:00+00:00",
    )

    assert snapshot.master_data_last_success_at == completed_at
    assert snapshot.status == "complete"
    assert snapshot.indeterminate == ()


@pytest.mark.asyncio
async def test_runtime_census_producer_persists_only_official_snapshot(tmp_path):
    config = replace(
        _runtime_config(tmp_path),
        exchanges=("SSE", "SZSE", "BSE"),
    )
    persisted = []
    repository = SimpleNamespace(
        upsert_listed_security_census_snapshot=lambda value: persisted.append(value)
    )

    class OfficialSource:
        parser_version = "official-parser.v1"

        async def get_instrument_list(self, exchange, instrument_types=None):
            instrument_id = {
                "SSE": "600000.SH",
                "SZSE": "000001.SZ",
                "BSE": "920001.BJ",
            }[exchange]
            return [
                {
                    "instrument_id": instrument_id,
                    "exchange": exchange,
                    "type": "stock",
                    "currency": "CNY",
                    "status": "active",
                    "is_active": True,
                    "source_authority": "official",
                    "source_url": f"https://official.example/{exchange}",
                    "raw_snapshot_hash": {
                        "SSE": "a" * 64,
                        "SZSE": "b" * 64,
                        "BSE": "c" * 64,
                    }[exchange],
                    "parser_version": self.parser_version,
                }
            ]

    manager = object.__new__(DataManager)
    manager.config = SimpleNamespace(
        get_nested=lambda path, default=None: default,
    )
    census = await manager._produce_announcement_asset_listed_security_census(
        repository=repository,
        config=config,
        snapshot_at="2026-08-12T01:00:00+00:00",
        source=OfficialSource(),
    )

    assert census.is_complete
    assert len(persisted) == 1
    assert persisted[0]["source"] == "official_exchange_current_lists"
    assert persisted[0]["metadata"]["counts_by_exchange"] == {
        "SSE": 1,
        "SZSE": 1,
        "BSE": 1,
    }


def test_runtime_census_refresh_failure_does_not_raise_or_mutate_repository():
    manager = object.__new__(DataManager)
    manager._produce_announcement_asset_listed_security_census = AsyncMock(
        side_effect=TimeoutError("official source unavailable")
    )
    repository = MagicMock()

    manager._refresh_announcement_asset_listed_security_census(
        repository=repository,
        config=SimpleNamespace(),
        snapshot_at="2026-08-12T01:00:00+00:00",
    )

    repository.upsert_listed_security_census_snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_partial_census_refresh_does_not_replace_complete_snapshot(tmp_path):
    config = replace(_runtime_config(tmp_path), exchanges=("SSE", "SZSE", "BSE"))
    manager = object.__new__(DataManager)
    manager.config = SimpleNamespace(
        get_nested=lambda path, default=None: default,
    )

    class PartialSource:
        parser_version = "official-test.v1"

        async def get_instrument_list(self, exchange, instrument_types=None):
            if exchange == "BSE":
                raise TimeoutError("bse unavailable")
            return [
                {
                    "instrument_id": "600000.SH" if exchange == "SSE" else "000001.SZ",
                    "exchange": exchange,
                    "type": "stock",
                    "currency": "CNY",
                    "is_active": True,
                    "name": "test",
                    "raw_snapshot_hash": "a" * 64,
                    "source_authority": "official",
                }
            ]

    repository = MagicMock()
    census = await manager._produce_announcement_asset_listed_security_census(
        repository=repository,
        config=config,
        snapshot_at="2026-08-12T01:00:00+00:00",
        source=PartialSource(),
    )

    assert census.status == "partial"
    repository.upsert_listed_security_census_snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_daily_runtime_entry_persists_schedule_and_executes_with_service_identity(
    tmp_path,
    monkeypatch,
):
    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", run_inline)
    manager = object.__new__(DataManager)
    config = _runtime_config(tmp_path)
    call_order = []
    started = SimpleNamespace(
        run_id="operation-1",
        reused=False,
        config_version="config-v1",
    )
    completed = SimpleNamespace(
        operation_id="operation-1",
        operation_type=DAILY_UPDATE_JOB,
        status=OperationStatus.COMPLETED,
        stage=OperationStage.DOWNLOADING,
        outcome=BatchOutcome.SUCCESS,
        attempt=0,
        progress={"run_cutoff": "2026-08-12T03:15:00+08:00"},
        diagnostics={},
    )
    commands = SimpleNamespace(
        config=config,
        repository=SimpleNamespace(
            initialize_schema=MagicMock(
                side_effect=lambda: call_order.append("initialize_schema")
            )
        ),
        preflight_start=MagicMock(
            side_effect=lambda *args, **kwargs: call_order.append("preflight")
        ),
        start=MagicMock(
            side_effect=lambda *args, **kwargs: (
                call_order.append("start") or started
            )
        ),
        execute=MagicMock(
            side_effect=lambda *args, **kwargs: (
                call_order.append("execute") or completed
            )
        ),
    )
    manager._get_announcement_asset_scheduler_commands = lambda **kwargs: commands

    result = await manager.run_annual_report_asset_daily_update(
        run_cutoff="2026-08-12T03:15:00+08:00",
        bounds={"max_pages": 2},
        trigger_kind="cron",
        principal_id="service:annual-report-asset-scheduler",
        expected_schedule={
            "timezone": "Asia/Shanghai",
            "overlap_days": config.discovery.overlap_days,
            "catch_up_max_days": config.daily_catch_up_max_days,
            "minimum_runs_per_calendar_day": (
                config.daily_min_runs_per_calendar_day
            ),
            "universe_refresh_cadence": config.universe_refresh_cadence,
        },
    )

    assert result["status"] == "completed"
    assert result["outcome"] == "success"
    start_kwargs = commands.start.call_args.kwargs
    assert start_kwargs["trigger_kind"] == "cron"
    assert start_kwargs["scope"]["schedule_timezone"] == "Asia/Shanghai"
    assert start_kwargs["scope"]["schedule_cron"] == config.jobs.daily_cron
    assert start_kwargs["scope"]["overlap_days"] == (
        config.discovery.overlap_days
    )
    assert start_kwargs["bounds"] == {"max_pages": 2}
    assert start_kwargs["principal"].service_identity is True
    assert start_kwargs["principal"].principal_id == (
        "service:annual-report-asset-scheduler"
    )
    commands.execute.assert_called_once_with(
        "operation-1",
        principal=start_kwargs["principal"],
    )
    commands.repository.initialize_schema.assert_called_once_with()
    commands.preflight_start.assert_called_once()
    assert call_order == ["preflight", "initialize_schema", "start", "execute"]






@pytest.mark.asyncio
async def test_daily_runtime_rejects_divergent_scheduler_registration(tmp_path):
    manager = object.__new__(DataManager)
    manager._get_announcement_asset_scheduler_commands = lambda **kwargs: (
        SimpleNamespace(config=_runtime_config(tmp_path))
    )
    manager._start_and_execute_announcement_asset_job = AsyncMock()

    with pytest.raises(ValueError, match="daily schedule mismatch"):
        await manager.run_annual_report_asset_daily_update(
            expected_schedule={"overlap_days": 999}
        )
    manager._start_and_execute_announcement_asset_job.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_identity_boundary_fails_before_schema_or_operation(tmp_path):
    manager = object.__new__(DataManager)
    config = _runtime_config(tmp_path)
    config = SimpleNamespace(
        **{
            **config.__dict__,
            "trusted_identity_enabled": False,
        }
    )
    repository = SimpleNamespace(initialize_schema=MagicMock())
    commands = SimpleNamespace(
        config=config,
        repository=repository,
        preflight_start=MagicMock(),
        start=MagicMock(),
        execute=MagicMock(),
    )
    manager._get_announcement_asset_scheduler_commands = lambda **kwargs: commands

    with pytest.raises(
        RuntimeError,
        match="authorization_boundary_unavailable",
    ):
        await manager._start_and_execute_announcement_asset_job(
            DAILY_UPDATE_JOB,
            trigger_kind="cron",
            principal_id="service:annual-report-asset-scheduler",
        )

    repository.initialize_schema.assert_not_called()
    commands.start.assert_not_called()
    commands.execute.assert_not_called()


@pytest.mark.asyncio
async def test_cron_runtime_requires_explicit_scheduler_service_identity_before_schema(
    tmp_path,
    monkeypatch,
):
    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", run_inline)
    manager = object.__new__(DataManager)
    config = _runtime_config(tmp_path)
    repository = SimpleNamespace(initialize_schema=MagicMock())
    commands = SimpleNamespace(
        config=config,
        repository=repository,
        preflight_start=MagicMock(),
        start=MagicMock(),
        execute=MagicMock(),
    )
    manager._get_announcement_asset_scheduler_commands = lambda **kwargs: commands

    for principal_id in (None, "operator:forged"):
        with pytest.raises(RuntimeError, match="scheduler_service_identity_unavailable"):
            await manager._start_and_execute_announcement_asset_job(
                DAILY_UPDATE_JOB,
                trigger_kind="cron",
                principal_id=principal_id,
            )

    repository.initialize_schema.assert_not_called()
    commands.preflight_start.assert_not_called()
    commands.start.assert_not_called()
    commands.execute.assert_not_called()


@pytest.mark.asyncio
async def test_manual_runtime_rejects_scheduler_service_identity_before_schema(
    tmp_path,
    monkeypatch,
):
    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", run_inline)
    manager = object.__new__(DataManager)
    config = _runtime_config(tmp_path)
    repository = SimpleNamespace(initialize_schema=MagicMock())
    commands = SimpleNamespace(
        config=config,
        repository=repository,
        preflight_start=MagicMock(),
        start=MagicMock(),
        execute=MagicMock(),
    )
    manager._get_announcement_asset_scheduler_commands = lambda **kwargs: commands

    with pytest.raises(RuntimeError, match="scheduler_service_identity_requires_cron"):
        await manager._start_and_execute_announcement_asset_job(
            LATEST_BACKFILL_JOB,
            trigger_kind="manual",
            principal_id="service:annual-report-asset-scheduler",
        )

    repository.initialize_schema.assert_not_called()
    commands.preflight_start.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_job_requests_fail_preflight_before_schema_initialization(
    tmp_path,
    monkeypatch,
):
    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    monkeypatch.setattr("data_manager.asyncio.to_thread", run_inline)
    manager = object.__new__(DataManager)
    config = _runtime_config(tmp_path)
    repository = SimpleNamespace(initialize_schema=MagicMock())
    commands = AnnualReportSchedulerCommandService(
        repository=repository,
        config=config,
        config_version="config-v1",
    )
    manager._get_announcement_asset_scheduler_commands = lambda **kwargs: commands

    requests = (
        (
            PermissionError,
            "principal_not_registered",
            {
                "job_name": LATEST_BACKFILL_JOB,
                "trigger_kind": "manual",
                "principal_id": "operator:unknown",
            },
        ),
        (
            ValueError,
            "unsupported job trigger",
            {
                "job_name": DAILY_UPDATE_JOB,
                "trigger_kind": "timer",
                "principal_id": "operator:test",
            },
        ),
        (
            ValueError,
            "unconfigured source",
            {
                "job_name": LATEST_BACKFILL_JOB,
                "trigger_kind": "manual",
                "principal_id": "operator:test",
                "scope": {"sources": ["sse"]},
            },
        ),
        (
            ValueError,
            "max_pages exceeds configured bound",
            {
                "job_name": LATEST_BACKFILL_JOB,
                "trigger_kind": "manual",
                "principal_id": "operator:test",
                "bounds": {"max_pages": config.discovery.max_pages + 1},
            },
        ),
    )

    for error_type, message, kwargs in requests:
        with pytest.raises(error_type, match=message):
            await manager._start_and_execute_announcement_asset_job(**kwargs)

    repository.initialize_schema.assert_not_called()
