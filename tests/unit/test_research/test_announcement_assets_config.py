from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from research.announcement_assets.config import (
    AnnouncementAssetConfig,
    BackupProtectionRuntimeState,
)


def _production_mapping() -> dict:
    return json.loads(Path("config/10_research.json").read_text(encoding="utf-8"))[
        "research_config"
    ]["modules"]["official_announcement_assets"]


def _config(tmp_path: Path, **overrides) -> AnnouncementAssetConfig:
    value = {
        "paths": {
            "filings_root": "data/filings",
            "archive_root": "data/filings/announcements",
            "temp_root": "data/filings/announcements/tmp",
            "quarantine_root": "data/filings/announcements/quarantine",
            "require_mount": False,
        }
    }
    value.update(overrides)
    return AnnouncementAssetConfig.from_mapping(value, project_root=tmp_path)


def test_required_config_fields_round_trip_and_enter_fingerprint():
    production = _production_mapping()
    template = json.loads(
        Path("config/config-template.json.example").read_text(encoding="utf-8")
    )["research_config"]["modules"]["official_announcement_assets"]
    assert production == template

    required_discovery_fields = {
        "reconciliation_lookback_days",
        "reconciliation_cohort_size",
        "reconciliation_max_cycle_days",
        "missing_repair_cohort_size",
        "targeted_repair_max_requests",
        "targeted_repair_max_instruments",
        "targeted_repair_max_elapsed_seconds",
    }
    required_top_level_fields = {
        "universe_refresh_cadence",
        "wait_seconds_default",
        "wait_seconds_maximum",
    }
    required_backup_fields = {
        "recovery_journal_retention_policy",
        "recovery_journal_integrity_policy",
    }
    assert required_discovery_fields <= production["discovery"].keys()
    assert required_top_level_fields <= production.keys()
    assert required_backup_fields <= production["backup"].keys()
    assert production["schema_version"] == "official_announcement_assets.config.v1"
    assert production["universe_policy_version"] == "a_share_active.v1"
    assert production["overdue_missing_readiness_policy"] == "degraded"

    config = AnnouncementAssetConfig.from_mapping(production)
    assert config.normalized_mapping() == production
    fingerprint_payload = json.dumps(
        config.normalized_mapping(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    assert config.config_fingerprint == hashlib.sha256(fingerprint_payload).hexdigest()
    round_tripped = AnnouncementAssetConfig.from_mapping(config.normalized_mapping())
    assert round_tripped.normalized_mapping() == config.normalized_mapping()
    assert round_tripped.config_fingerprint == config.config_fingerprint

    for field in sorted(required_discovery_fields):
        changed = copy.deepcopy(production)
        changed["discovery"][field] -= 1
        assert (
            AnnouncementAssetConfig.from_mapping(changed).config_fingerprint
            != config.config_fingerprint
        )

    for field, value in (
        ("wait_seconds_default", 1.0),
        ("wait_seconds_maximum", 15.0),
    ):
        changed = copy.deepcopy(production)
        changed[field] = value
        changed_config = AnnouncementAssetConfig.from_mapping(changed)
        assert changed_config.config_fingerprint != config.config_fingerprint

    for field, value in (
        ("recovery_journal_retention_policy", "append_only_no_automatic_gc.v2"),
        ("recovery_journal_integrity_policy", "sha512_chain.v1"),
    ):
        changed = copy.deepcopy(production)
        changed["backup"][field] = value
        with pytest.raises(ValueError):
            AnnouncementAssetConfig.from_mapping(changed)


def test_scheduler_parameters_match_versioned_research_config_policies():
    production = _production_mapping()
    scheduler = json.loads(
        Path("config/05_scheduler.json").read_text(encoding="utf-8")
    )["scheduler_config"]["jobs"]

    daily_parameters = scheduler["annual_report_asset_daily_update"]["parameters"]
    assert (
        daily_parameters["universe_refresh_cadence"]
        == production["universe_refresh_cadence"]
    )

    backup_parameters = scheduler["annual_report_asset_backup"]["parameters"]
    assert backup_parameters == {
        "recovery_journal_retention_policy": production["backup"][
            "recovery_journal_retention_policy"
        ],
        "recovery_journal_integrity_policy": production["backup"][
            "recovery_journal_integrity_policy"
        ],
    }


def test_versioned_universe_cadence_and_wait_bounds_fail_closed(tmp_path: Path):
    with pytest.raises(ValueError, match="universe_refresh_cadence"):
        _config(tmp_path, universe_refresh_cadence="daily")

    for field, invalid in (
        ("wait_seconds_default", -1),
        ("wait_seconds_default", True),
        ("wait_seconds_default", float("nan")),
        ("wait_seconds_maximum", 0),
        ("wait_seconds_maximum", float("inf")),
    ):
        with pytest.raises((TypeError, ValueError)):
            _config(tmp_path, **{field: invalid})

    with pytest.raises(ValueError, match="cannot exceed"):
        _config(tmp_path, wait_seconds_default=31, wait_seconds_maximum=30)

    with pytest.raises(ValueError, match="lease_safety_grace_seconds"):
        _config(
            tmp_path,
            retry={
                "lease_seconds": 30,
                "heartbeat_seconds": 5,
                "lease_safety_grace_seconds": 30,
            },
        )


@pytest.mark.parametrize(
    "field",
    [
        "reconciliation_lookback_days",
        "reconciliation_cohort_size",
        "reconciliation_max_cycle_days",
        "missing_repair_cohort_size",
        "targeted_repair_max_requests",
        "targeted_repair_max_instruments",
        "targeted_repair_max_elapsed_seconds",
    ],
)
@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, "1"])
def test_reconciliation_and_repair_bounds_reject_non_positive_or_non_integer(
    tmp_path: Path,
    field: str,
    invalid: object,
):
    with pytest.raises((TypeError, ValueError)):
        _config(tmp_path, discovery={field: invalid})


def test_latest_backfill_configuration_can_never_register_a_cron(tmp_path: Path):
    with pytest.raises(ValueError, match="manual-only"):
        _config(tmp_path, jobs={"latest_backfill_manual_only": False})
    with pytest.raises(ValueError, match="cannot have a cron"):
        _config(tmp_path, jobs={"latest_backfill_cron": "0 1 * * *"})


@pytest.mark.parametrize("field", ["max_unprotected_bytes", "max_unprotected_age_seconds"])
@pytest.mark.parametrize("invalid", [0, -1, True, 1.5, "1"])
def test_unprotected_thresholds_reject_zero_and_unsafe_boundaries(
    tmp_path: Path,
    field: str,
    invalid: object,
):
    with pytest.raises((TypeError, ValueError)):
        _config(tmp_path, backup={field: invalid})


def test_unprotected_accumulation_survives_restart_and_only_verified_closure_unblocks(
    tmp_path: Path,
):
    config = _config(
        tmp_path,
        backup={
            "max_unprotected_bytes": 10,
            "max_unprotected_age_seconds": 60,
            "unprotected_accumulation_origin": "first_unprotected_at",
        },
    )
    state = BackupProtectionRuntimeState.fresh(config).record_unprotected_bytes(
        5,
        observed_at="2026-08-10T00:00:00+00:00",
        config=config,
    )
    persisted = json.loads(json.dumps(state.normalized_mapping()))

    restarted = BackupProtectionRuntimeState.from_mapping(
        persisted,
        config=config,
        now="2026-08-10T00:01:00+00:00",
    )
    assert restarted.unprotected_bytes == 5
    assert restarted.blocked is True
    assert restarted.blocker_reasons == ("max_unprotected_age_reached",)

    failed = restarted.record_backup_attempt(
        attempted_at="2026-08-10T00:02:00+00:00",
        verified_closure=False,
        config=config,
    )
    assert failed.blocked is True
    assert failed.unprotected_bytes == 5

    verified = failed.record_backup_attempt(
        attempted_at="2026-08-10T00:03:00+00:00",
        verified_closure=True,
        config=config,
    )
    assert verified.blocked is False
    assert verified.blocker_reasons == ()
    assert verified.unprotected_bytes == 0
    assert verified.accumulation_started_at is None
    assert verified.last_verified_backup_at == "2026-08-10T00:03:00+00:00"

    byte_boundary = BackupProtectionRuntimeState.fresh(
        config
    ).record_unprotected_bytes(
        10,
        observed_at="2026-08-10T01:00:00+00:00",
        config=config,
    )
    assert byte_boundary.blocked is True
    assert "max_unprotected_bytes_reached" in byte_boundary.blocker_reasons


def test_trusted_principal_configuration_is_fixed_and_fail_closed(tmp_path: Path):
    permissions = {
        "trusted_identity_enabled": True,
        "principals": [
            {
                "principal": "annual-report-ui",
                "token_env": "ANNUAL_REPORT_UI_TOKEN",
                "scopes": ["annual_report_assets:acquire"],
            }
        ],
    }
    config = _config(tmp_path, permissions=permissions)
    assert config.trusted_principals[0].principal == "annual-report-ui"
    assert config.trusted_principals[0].token_env == "ANNUAL_REPORT_UI_TOKEN"
    assert config.trusted_principals[0].scopes == (
        "annual_report_assets:acquire",
    )
    assert config.normalized_mapping()["permissions"]["principals"] == permissions[
        "principals"
    ]

    for invalid_principals in (
        [],
        ["not-a-mapping"],
        [{"principal": "ui", "token_env": "9INVALID", "scopes": ["scope"]}],
        [{"principal": "ui", "token_env": "TOKEN", "scopes": "scope"}],
    ):
        with pytest.raises((TypeError, ValueError)):
            _config(
                tmp_path,
                permissions={
                    "trusted_identity_enabled": True,
                    "principals": invalid_principals,
                },
            )

    with pytest.raises(ValueError, match="principal values must be unique"):
        _config(
            tmp_path,
            permissions={
                "trusted_identity_enabled": True,
                "principals": [permissions["principals"][0], {
                    "principal": "annual-report-ui",
                    "token_env": "ANNUAL_REPORT_UI_TOKEN_2",
                    "scopes": ["annual_report_assets:read_content"],
                }],
            },
        )
