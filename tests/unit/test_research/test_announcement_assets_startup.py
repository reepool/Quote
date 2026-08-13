from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
    AnnouncementAssetService,
    AnnualReportDailyUpdater,
    annual_report_scheduler_job_definitions,
)


def test_import_and_service_scheduler_construction_have_zero_asset_side_effects(
    tmp_path: Path,
):
    config = AnnouncementAssetConfig.from_mapping(
        {
            "enabled": True,
            "paths": {
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "require_mount": False,
            },
        },
        project_root=tmp_path,
    )
    db_path = tmp_path / "research.db"
    repository = AnnouncementAssetRepository(db_path)
    service = AnnouncementAssetService(repository=repository, config=config)
    updater = AnnualReportDailyUpdater(
        service=service,
        repository=repository,
        config=config,
    )

    definitions = annual_report_scheduler_job_definitions(config)
    imported = importlib.import_module("research.announcement_assets")
    api_module = importlib.import_module("api.app")

    assert updater.service is service
    assert {item.name for item in definitions} == {
        "annual_report_asset_latest_backfill",
        "annual_report_asset_daily_update",
        "annual_report_asset_integrity_audit",
        "annual_report_asset_backup",
    }
    by_name = {item.name: item for item in definitions}
    assert by_name["annual_report_asset_latest_backfill"].manual_only is True
    assert by_name["annual_report_asset_latest_backfill"].cron is None
    assert by_name["annual_report_asset_daily_update"].enabled is False
    assert by_name["annual_report_asset_backup"].enabled is False
    assert by_name["annual_report_asset_integrity_audit"].manual_only is True
    assert imported.AnnouncementAssetService is AnnouncementAssetService
    assert api_module.app.openapi_url == "/openapi.json"
    assert not db_path.exists()
    assert not config.archive_root.exists()
    assert not config.temp_root.exists()
    assert not config.quarantine_root.exists()


def test_application_and_datamanager_registration_validate_locally_without_work(
    tmp_path: Path,
):
    """Import/registration must not turn a disabled module into a worker."""

    from data_manager import DataManager
    from research.announcement_assets import AnnouncementArchiveInventory
    from research.announcement_assets.lifecycle import AnnouncementAssetLifecycleManager

    manager = DataManager()
    manager.research_config = SimpleNamespace(
        modules={"official_announcement_assets": {"enabled": False}},
        storage=SimpleNamespace(db_path=str(tmp_path / "research.db")),
    )
    access = manager._get_announcement_asset_access(initialize_schema=False)
    assert access.config.enabled is False

    # These collaborators are deliberately tripwires.  Construction and
    # scheduler registration should not call any provider or destructive API.
    class Tripwire:
        def __getattr__(self, name):
            raise AssertionError(f"startup invoked work: {name}")

    repository = AnnouncementAssetRepository(tmp_path / "standalone.db")
    service = AnnouncementAssetService(
        repository=repository,
        config=access.config,
        acquisition_service=Tripwire(),
        attachment_retriever=Tripwire(),
    )
    lifecycle = AnnouncementAssetLifecycleManager(
        repository=repository,
        blob_store=service.blob_store,
        primary_failure_domain=None,
    )
    inventory = AnnouncementArchiveInventory().inventory(
        business_profile_root=tmp_path / "missing-business",
        broker_root=tmp_path / "missing-broker",
    )
    definitions = annual_report_scheduler_job_definitions(access.config)

    assert service.repository is repository
    assert lifecycle.repository is repository
    assert inventory.network_requests == 0
    assert inventory.files_moved == inventory.files_linked == 0
    assert inventory.files_quarantined == inventory.files_deleted == 0
    assert all(item.cron is None or not item.enabled for item in definitions)
    assert not (tmp_path / "research.db").exists()
    assert not (tmp_path / "standalone.db").exists()

    # Local schema/config validation remains an explicit, side-effectful action
    # and is separate from application registration.
    repository.initialize_schema()
    assert (tmp_path / "standalone.db").exists()
    with repository.connection() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_operations"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM official_asset_deletion_intents"
        ).fetchone()[0] == 0
    assert not access.config.archive_root.exists()
    assert not access.config.temp_root.exists()
    assert not access.config.quarantine_root.exists()
