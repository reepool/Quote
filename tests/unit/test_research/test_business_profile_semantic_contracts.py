import pytest

from research.business_profile_semantic_contracts import (
    BUSINESS_PROFILE_FIELD_FAMILY_SCHEMA_VERSION,
    FIELD_FAMILY_DEFINITIONS,
    BusinessProfileFieldFamily,
    business_profile_field_family_manifest,
    get_business_profile_field_family,
)


def test_field_family_manifest_is_versioned_complete_and_unique():
    manifest = business_profile_field_family_manifest()

    assert manifest["schema_version"] == BUSINESS_PROFILE_FIELD_FAMILY_SCHEMA_VERSION
    assert len(manifest["field_families"]) == 7
    assert {item["field_family"] for item in manifest["field_families"]} == {
        item.value for item in BusinessProfileFieldFamily
    }
    assert len({item.field_family for item in FIELD_FAMILY_DEFINITIONS}) == 7


def test_semantic_families_require_verification_and_derived_families_forbid_llm():
    activities = get_business_profile_field_family("atomic_activities")
    publication = get_business_profile_field_family(
        BusinessProfileFieldFamily.COMMODITY_EXPOSURE_PUBLICATION
    )

    assert activities.llm_allowed is True
    assert activities.verification_policy == "independent_semantic_verification"
    assert publication.llm_allowed is False
    assert publication.verification_policy == "approved_component_assembly"
    assert all(item.requires_official_evidence for item in FIELD_FAMILY_DEFINITIONS)


def test_unknown_field_family_fails_closed():
    with pytest.raises(ValueError, match="unsupported business-profile field family"):
        get_business_profile_field_family("whole_report_guess")
