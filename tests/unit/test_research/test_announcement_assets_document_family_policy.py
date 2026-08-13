from __future__ import annotations

import copy

import pytest

from research.announcement_assets import (
    ACQUISITION_POLICY_SCHEMA_VERSION,
    AnnouncementAssetConfig,
    DocumentFamilyAcquisitionPolicy,
    DocumentFamilyAcquisitionScope,
)


def _policy_mapping(
    scope: DocumentFamilyAcquisitionScope,
    *,
    enabled: bool,
    environment: str = "test",
) -> dict:
    bounded = scope is DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE
    full_market = scope is DocumentFamilyAcquisitionScope.FULL_MARKET
    return {
        "schema_version": ACQUISITION_POLICY_SCHEMA_VERSION,
        "document_family": "test_document_family",
        "policy_version": "test-document-family-acquisition.v1",
        "scope": scope.value,
        "proactive_enabled": enabled,
        "explicit_universe": ["600000.SH", "000001.SZ"] if bounded else [],
        "universe_policy_version": (
            "test-universe.v1" if bounded or full_market else None
        ),
        "governance_policy_version": "test-governance.v1" if full_market else None,
        "environment": environment,
        "effective_version_policy": {
            "policy_version": "test-effective.v1",
            "precedence_rules": ["correction_over_original", "latest_published"],
            "tie_break": "stable_legal_identity",
            "conflict_policy": "fail_closed",
            "parameters": {"period_key": "instrument+report_period"},
        },
        "retention_policy": {
            "policy_version": "test-retention.v1",
            "mode": "one_effective_per_period",
            "retain_metadata": True,
            "retain_superseded_bytes": "governed_recovery_only",
            "max_effective_per_instrument_period": 1,
            "parameters": {"superseded_metadata": "append_only"},
        },
    }


@pytest.mark.parametrize(
    ("scope", "enabled"),
    [
        (DocumentFamilyAcquisitionScope.METADATA_ONLY, False),
        (DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE, True),
        (DocumentFamilyAcquisitionScope.FULL_MARKET, True),
    ],
)
def test_future_family_policy_round_trips_and_fingerprints_each_scope(scope, enabled):
    original = DocumentFamilyAcquisitionPolicy.from_mapping(
        _policy_mapping(scope, enabled=enabled)
    )
    restored = DocumentFamilyAcquisitionPolicy.from_mapping(original.normalized_mapping())

    assert restored.normalized_mapping() == original.normalized_mapping()
    assert restored.fingerprint == original.fingerprint
    assert original.effective_version_policy.policy_version == "test-effective.v1"
    assert original.retention_policy.policy_version == "test-retention.v1"

    changed = copy.deepcopy(original.normalized_mapping())
    changed["retention_policy"]["policy_version"] = "test-retention.v2"
    assert (
        DocumentFamilyAcquisitionPolicy.from_mapping(changed).fingerprint
        != original.fingerprint
    )

    if scope is DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE:
        assert original.attachment_acquisition_allowed("600000.SH") is True
        assert original.attachment_acquisition_allowed("300001.SZ") is False
    elif scope is DocumentFamilyAcquisitionScope.FULL_MARKET:
        assert original.attachment_acquisition_allowed("300001.SZ") is True
        assert original.attachment_acquisition_allowed(None) is False
    else:
        assert original.attachment_acquisition_allowed("600000.SH") is False


def test_production_non_annual_proactive_acquisition_is_fail_closed():
    value = _policy_mapping(
        DocumentFamilyAcquisitionScope.FULL_MARKET,
        enabled=True,
        environment="production",
    )
    with pytest.raises(ValueError, match="production proactive acquisition"):
        DocumentFamilyAcquisitionPolicy.from_mapping(value)


def test_policy_scope_validation_prevents_implicit_unbounded_or_metadata_download():
    metadata = _policy_mapping(
        DocumentFamilyAcquisitionScope.METADATA_ONLY,
        enabled=False,
    )
    metadata["explicit_universe"] = ["600000.SH"]
    with pytest.raises(ValueError, match="metadata_only"):
        DocumentFamilyAcquisitionPolicy.from_mapping(metadata)

    bounded = _policy_mapping(
        DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE,
        enabled=True,
    )
    bounded["explicit_universe"] = []
    with pytest.raises(ValueError, match="non-empty universe"):
        DocumentFamilyAcquisitionPolicy.from_mapping(bounded)

    full_market = _policy_mapping(
        DocumentFamilyAcquisitionScope.FULL_MARKET,
        enabled=True,
    )
    full_market["governance_policy_version"] = None
    with pytest.raises(ValueError, match="governance_policy_version"):
        DocumentFamilyAcquisitionPolicy.from_mapping(full_market)


def test_policy_fingerprint_is_detached_from_mutable_input():
    value = _policy_mapping(
        DocumentFamilyAcquisitionScope.BOUNDED_EXPLICIT_UNIVERSE,
        enabled=True,
    )
    policy = DocumentFamilyAcquisitionPolicy.from_mapping(value)
    fingerprint = policy.fingerprint

    value["effective_version_policy"]["parameters"]["period_key"] = "changed"
    value["retention_policy"]["parameters"]["superseded_metadata"] = "changed"

    assert policy.fingerprint == fingerprint
    assert policy.policy_fingerprint == fingerprint


def test_v1_production_config_keeps_acquisition_category_annual_only(tmp_path):
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
            "acquisition": {"normalized_categories": ["annual_report"]},
        },
        project_root=tmp_path,
    )
    assert config.acquisition.normalized_categories == ("annual_report",)
    assert not DocumentFamilyAcquisitionPolicy.from_mapping(
        _policy_mapping(
            DocumentFamilyAcquisitionScope.METADATA_ONLY,
            enabled=False,
            environment="production",
        )
    ).attachment_acquisition_allowed("600000.SH")
