from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from research.announcement_assets.config import (
    LEGACY_ARCHIVE_EXCLUSION_POLICY_VERSION,
    LEGACY_ARCHIVE_REGISTRY_VERSION,
    LEGACY_ARCHIVE_TEMPLATE_VERSION,
    AnnouncementAssetConfig,
)
from research.announcement_assets.migration import AnnouncementArchiveInventory

PDF_A = b"%PDF-1.4\nlegacy registry annual A\n%%EOF\n"
PDF_B = b"%PDF-1.4\nlegacy registry annual B\n%%EOF\n"


def _digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _config(tmp_path: Path, **overrides) -> AnnouncementAssetConfig:
    value = {
        "paths": {
            "filings_root": "data/filings",
            "archive_root": "data/filings/announcements",
            "temp_root": "data/filings/announcements/tmp",
            "quarantine_root": "data/filings/announcements/quarantine",
            "adoption_roots": [
                "data/filings/business_profile",
                "data/filings/financial_statements/broker_risk_control",
            ],
            "require_mount": False,
        }
    }
    value.update(overrides)
    return AnnouncementAssetConfig.from_mapping(value, project_root=tmp_path)


def _manifest(
    path: Path,
    *,
    instrument_id: str,
    exchange: str,
    report_period: str,
    filing_id: str,
    content: bytes,
    report_type: str = "annual_report",
) -> dict[str, object]:
    return {
        "archive_path": str(path),
        "source_file_id": f"source-{filing_id}",
        "instrument_id": instrument_id,
        "exchange": exchange,
        "report_period": report_period,
        "filing_id": filing_id,
        "source": "cninfo",
        "report_type": report_type,
        "content_hash": _digest(content),
        "content_length": len(content),
    }


def test_default_legacy_registry_is_versioned_and_part_of_config_fingerprint(tmp_path):
    config = _config(tmp_path)
    normalized = config.normalized_mapping()["legacy_inventory"]

    assert normalized == {
        "registry_version": LEGACY_ARCHIVE_REGISTRY_VERSION,
        "path_template_version": LEGACY_ARCHIVE_TEMPLATE_VERSION,
        "exclusion_policy_version": LEGACY_ARCHIVE_EXCLUSION_POLICY_VERSION,
        "roots": {
            "business_profile": {
                "base_root": "data/filings/business_profile",
                "path_template": "business_profile/{fiscal_year}/{exchange}/",
            },
            "broker_risk_control": {
                "base_root": (
                    "data/filings/financial_statements/broker_risk_control"
                ),
                "path_template": "broker_risk_control/{exchange}/{symbol}/",
            },
        },
        "exclusions": {
            "allowed_document_families": ["annual_report"],
            "business_profile_subtrees": ["derived"],
            "broker_document_families": ["semiannual_report"],
        },
    }
    changed = _config(
        tmp_path,
        legacy_inventory={
            **normalized,
            "registry_version": "legacy_annual_report_roots.v2-test",
        },
    )
    assert changed.config_fingerprint != config.config_fingerprint


def test_legacy_registry_override_requires_both_named_roots_and_path_parity(tmp_path):
    with pytest.raises(ValueError, match="must define business_profile"):
        _config(tmp_path, legacy_inventory={"roots": {}})

    with pytest.raises(ValueError, match="must define business_profile"):
        _config(
            tmp_path,
            legacy_inventory={
                "roots": {
                    "business_profile": {
                        "base_root": "data/filings/business_profile",
                        "path_template": (
                            "business_profile/{fiscal_year}/{exchange}/"
                        ),
                    }
                }
            },
        )

    with pytest.raises(ValueError, match="conflicts with legacy_inventory"):
        _config(
            tmp_path,
            paths={
                "filings_root": "data/filings",
                "archive_root": "data/filings/announcements",
                "temp_root": "data/filings/announcements/tmp",
                "quarantine_root": "data/filings/announcements/quarantine",
                "adoption_roots": [
                    "data/filings/other_business",
                    "data/filings/financial_statements/broker_risk_control",
                ],
                "require_mount": False,
            },
        )


def test_registered_inventory_is_bounded_to_templates_allowlist_and_known_roots(
    tmp_path,
):
    config = _config(tmp_path)
    filings = tmp_path / "data/filings"
    business = filings / "business_profile"
    broker = filings / "financial_statements/broker_risk_control"
    business_2025 = business / "2025/SSE"
    business_2024 = business / "2024/SSE"
    broker_2025 = broker / "SZSE/000001"
    misplaced_dir = business / "misc/SSE"
    unknown_dir = filings / "unregistered_consumer/deep"
    for directory in (
        business_2025,
        business_2024,
        broker_2025,
        misplaced_dir,
        unknown_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    annual_business = business_2025 / (
        f"600000_SH_2025Q4_bp-2025_{_digest(PDF_A)}.pdf"
    )
    old_business = business_2024 / (
        f"600000_SH_2024Q4_bp-2024_{_digest(PDF_B)}.pdf"
    )
    annual_broker = broker_2025 / "000001_2025-12-31_broker-2025.pdf"
    semiannual_broker = broker_2025 / "000001_2025-06-30_broker-half.pdf"
    misplaced = misplaced_dir / (
        f"600000_SH_2025Q4_misplaced_{_digest(PDF_A)}.pdf"
    )
    other_family = business_2025 / (
        f"600001_SH_2025Q4_quarterly_{_digest(PDF_A)}.pdf"
    )
    unknown_file = unknown_dir / "600000_SH_2025Q4_unknown.pdf"
    derived = business_2025 / "derived/pages.json"
    derived.parent.mkdir()
    annual_business.write_bytes(PDF_A)
    old_business.write_bytes(PDF_B)
    annual_broker.write_bytes(PDF_B)
    semiannual_broker.write_bytes(PDF_A)
    misplaced.write_bytes(PDF_A)
    other_family.write_bytes(PDF_A)
    unknown_file.write_bytes(PDF_A)
    derived.write_text("{}", encoding="utf-8")
    unknown_before = (
        unknown_file.stat().st_mtime_ns,
        _digest(unknown_file.read_bytes()),
    )

    manifests = [
        _manifest(
            annual_business,
            instrument_id="600000.SH",
            exchange="SSE",
            report_period="2025-12-31",
            filing_id="bp-2025",
            content=PDF_A,
        ),
        _manifest(
            old_business,
            instrument_id="600000.SH",
            exchange="SSE",
            report_period="2024-12-31",
            filing_id="bp-2024",
            content=PDF_B,
        ),
        _manifest(
            annual_broker,
            instrument_id="000001.SZ",
            exchange="SZSE",
            report_period="2025-12-31",
            filing_id="broker-2025",
            content=PDF_B,
        ),
        _manifest(
            semiannual_broker,
            instrument_id="000001.SZ",
            exchange="SZSE",
            report_period="2025-06-30",
            filing_id="broker-half",
            content=PDF_A,
            report_type="semiannual",
        ),
        _manifest(
            misplaced,
            instrument_id="600000.SH",
            exchange="SSE",
            report_period="2025-12-31",
            filing_id="misplaced",
            content=PDF_A,
        ),
        _manifest(
            other_family,
            instrument_id="600001.SH",
            exchange="SSE",
            report_period="2025-12-31",
            filing_id="quarterly",
            content=PDF_A,
            report_type="quarterly_report",
        ),
    ]
    inventory = AnnouncementArchiveInventory()
    report = inventory.inventory_registered(
        config=config,
        manifest_rows=manifests,
        fiscal_year_allowlist=(2025,),
    )

    by_name = {Path(item.path).name: item for item in report.items}
    assert by_name[annual_business.name].status == "adoptable"
    assert by_name[annual_broker.name].status == "adoptable"
    assert by_name[old_business.name].reason == "fiscal_year_not_allowlisted"
    assert by_name[semiannual_broker.name].status == "out_of_scope"
    assert by_name[misplaced.name].reason == "path_outside_registered_template"
    assert by_name[other_family.name].reason == "non_annual_document_family"
    assert by_name[derived.name].status == "derived"
    assert str(unknown_file) not in {item.path for item in report.items}
    assert report.out_of_scope_directories == (
        str((filings / "unregistered_consumer").resolve()),
    )
    assert report.root_registry_version == LEGACY_ARCHIVE_REGISTRY_VERSION
    assert report.path_template_version == LEGACY_ARCHIVE_TEMPLATE_VERSION
    assert report.exclusion_policy_version == (
        LEGACY_ARCHIVE_EXCLUSION_POLICY_VERSION
    )
    assert report.inventory_fingerprint
    assert report.network_requests == 0
    assert report.files_moved == report.files_linked == report.files_deleted == 0
    assert (
        unknown_file.stat().st_mtime_ns,
        _digest(unknown_file.read_bytes()),
    ) == unknown_before

    repeated = inventory.inventory_registered(
        config=config,
        manifest_rows=manifests,
        fiscal_year_allowlist=(2025,),
    )
    broader = inventory.inventory_registered(
        config=config,
        manifest_rows=manifests,
        fiscal_year_allowlist=(2024, 2025),
    )
    assert repeated.inventory_fingerprint == report.inventory_fingerprint
    assert broader.inventory_fingerprint != report.inventory_fingerprint


def test_registered_inventory_rejects_directory_identity_mismatches_before_read(
    tmp_path,
):
    config = _config(tmp_path)
    filings = tmp_path / "data/filings"
    business = filings / "business_profile"
    broker = filings / "financial_statements/broker_risk_control"
    invalid_bytes = b"directory identity mismatch must fail before PDF inspection"

    year_mismatch = business / "2024/SSE" / (
        f"600000_SH_2025Q4_bp-year_{_digest(invalid_bytes)}.pdf"
    )
    business_exchange_mismatch = business / "2025/SZSE" / (
        f"600001_SH_2025Q4_bp-exchange_{_digest(invalid_bytes)}.pdf"
    )
    broker_exchange_mismatch = (
        broker / "SSE/000001/000001_2025-12-31_broker-exchange.pdf"
    )
    broker_symbol_mismatch = (
        broker / "SZSE/999999/000002_2025-12-31_broker-symbol.pdf"
    )
    files = (
        year_mismatch,
        business_exchange_mismatch,
        broker_exchange_mismatch,
        broker_symbol_mismatch,
    )
    for path in files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(invalid_bytes)
    before = {
        str(path): (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in files
    }
    manifests = (
        _manifest(
            year_mismatch,
            instrument_id="600000.SH",
            exchange="SSE",
            report_period="2025-12-31",
            filing_id="bp-year",
            content=invalid_bytes,
        ),
        _manifest(
            business_exchange_mismatch,
            instrument_id="600001.SH",
            exchange="SSE",
            report_period="2025-12-31",
            filing_id="bp-exchange",
            content=invalid_bytes,
        ),
        _manifest(
            broker_exchange_mismatch,
            instrument_id="000001.SZ",
            exchange="SZSE",
            report_period="2025-12-31",
            filing_id="broker-exchange",
            content=invalid_bytes,
        ),
        _manifest(
            broker_symbol_mismatch,
            instrument_id="000002.SZ",
            exchange="SZSE",
            report_period="2025-12-31",
            filing_id="broker-symbol",
            content=invalid_bytes,
        ),
    )

    report = AnnouncementArchiveInventory().inventory_registered(
        config=config,
        manifest_rows=manifests,
        fiscal_year_allowlist=(2025,),
    )

    reasons = {Path(item.path).name: item.reason for item in report.items}
    assert reasons[year_mismatch.name] == "template_fiscal_year_mismatch"
    assert (
        reasons[business_exchange_mismatch.name]
        == "template_exchange_mismatch"
    )
    assert reasons[broker_exchange_mismatch.name] == "template_exchange_mismatch"
    assert reasons[broker_symbol_mismatch.name] == "template_symbol_mismatch"
    assert all(item.status == "out_of_scope" for item in report.items)
    assert {
        str(path): (path.stat().st_mtime_ns, _digest(path.read_bytes()))
        for path in files
    } == before
