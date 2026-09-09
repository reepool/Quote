from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from research.company_profile.shadow_batch import (
    SHADOW_EXCHANGE_TARGETS,
    SHADOW_REPORT_COUNT,
    ShadowSampleManifest,
    collect_shadow_exclusions,
    load_shadow_sample_manifest,
    load_shadow_selection_receipt,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CHANGE_ROOT = (
    REPOSITORY_ROOT
    / "openspec/changes/validate-manufacturing-materials-company-profile-shadow-batch"
)
SELECTION_RECEIPT = CHANGE_ROOT / "selection-receipt.v1.json"
SHADOW_MANIFEST = CHANGE_ROOT / "shadow-manifest.v1.json"
HISTORICAL_INSTRUMENTS = {
    "300750.SZ",
    "603659.SH",
    "920015.BJ",
    "302132.SZ",
    "600019.SH",
    "000717.SZ",
}


def test_frozen_shadow_cohort_is_hash_bound_diverse_and_unseen() -> None:
    receipt = load_shadow_selection_receipt(
        SELECTION_RECEIPT,
        repository_root=REPOSITORY_ROOT,
    )
    manifest = load_shadow_sample_manifest(
        SHADOW_MANIFEST,
        repository_root=REPOSITORY_ROOT,
        selection_receipt=receipt,
    )

    assert len(manifest.reports) == SHADOW_REPORT_COUNT
    assert receipt.exchange_counts == SHADOW_EXCHANGE_TARGETS
    assert {item.industry_group for item in manifest.reports} == {
        "coal",
        "nonferrous_and_solid_mineral",
        "steel",
        "petrochemical",
        "basic_chemical",
        "building_material",
    }
    assert not (
        {item.report.instrument_id for item in manifest.reports}
        & HISTORICAL_INSTRUMENTS
    )
    assert all(item.local_path.is_file() for item in manifest.reports)
    assert all(item.production_authorization == "not_authorized" for item in manifest.reports)


def test_shadow_manifest_rejects_content_tampering() -> None:
    payload = json.loads(SHADOW_MANIFEST.read_text(encoding="utf-8"))
    payload["reports"][0]["company_name"] = "tampered"

    with pytest.raises(ValidationError, match="manifest hash mismatch"):
        ShadowSampleManifest.model_validate_json(json.dumps(payload))


def test_shadow_exclusion_scan_contains_all_historical_authorities() -> None:
    exclusions = collect_shadow_exclusions(REPOSITORY_ROOT)

    assert HISTORICAL_INSTRUMENTS <= set(exclusions.instrument_ids)
    assert exclusions.content_hashes
    assert any(path.startswith("openspec/changes/archive/") for path in exclusions.source_paths)
