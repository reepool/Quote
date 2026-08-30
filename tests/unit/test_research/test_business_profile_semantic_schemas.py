import hashlib

import pytest

from research.business_profile_semantic_schemas import (
    BUSINESS_PROFILE_SEMANTIC_SCHEMAS,
    business_profile_semantic_schema_manifest,
    get_business_profile_semantic_schema,
    validate_business_profile_artifact,
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def test_schema_set_is_versioned_closed_and_complete():
    manifest = business_profile_semantic_schema_manifest()

    assert set(BUSINESS_PROFILE_SEMANTIC_SCHEMAS) == {
        "selected_section_bundle",
        "atomic_activity",
        "semantic_verification",
        "promotion_decision",
        "exception_record",
        "exposure_publication",
    }
    assert set(manifest["artifact_schema_versions"]) == set(
        BUSINESS_PROFILE_SEMANTIC_SCHEMAS
    )
    assert all(
        schema["additionalProperties"] is False
        for schema in BUSINESS_PROFILE_SEMANTIC_SCHEMAS.values()
    )


def test_atomic_activity_rejects_unknown_fields_and_broad_roles():
    payload = {
        "schema_version": "business_profile_atomic_activity.v1",
        "activity_id": "activity-1",
        "instrument_id": "601088.SH",
        "subject_scope": "issuer",
        "action": "produces",
        "object_raw": "thermal coal",
        "object_id": None,
        "segment_id": None,
        "source_row_key": "report-1:31:section-1:row-1",
        "contract_reference_raw": None,
        "report_period": "2025-12-31",
        "value": 100.0,
        "unit": "tonne",
        "evidence": {
            "source_document_id": "report-1",
            "page_number": 31,
            "section_id": "section-1",
            "quote": "The company produced thermal coal.",
            "normalized_start": 0,
            "normalized_end": 34,
            "quote_hash": _hash("quote"),
            "section_hash": _hash("section"),
        },
        "review_status": "candidate",
    }

    validate_business_profile_artifact("atomic_activity", payload)
    with pytest.raises(ValueError, match="Additional properties"):
        validate_business_profile_artifact(
            "atomic_activity", {**payload, "value_chain_role": "upstream"}
        )
    with pytest.raises(ValueError, match="not one of"):
        validate_business_profile_artifact(
            "atomic_activity", {**payload, "action": "upstream"}
        )


def test_exposure_publication_requires_component_and_policy_lineage():
    payload = {
        "schema_version": "business_profile_exposure_publication.v1",
        "exposure_id": "exposure-1",
        "instrument_id": "601088.SH",
        "fact_ids": ["fact-1"],
        "mapping_ids": ["mapping-1"],
        "assumption_ids": [],
        "direction_rule_id": "direction.revenue.v1",
        "build_policy_version": "publication.v1",
        "build_policy_hash": _hash("policy"),
        "component_lineage_hash": _hash("components"),
        "effective_from": "2025-01-01",
        "effective_to": None,
        "knowledge_from": "2026-03-28",
        "knowledge_to": None,
    }
    validate_business_profile_artifact("exposure_publication", payload)

    invalid = dict(payload)
    invalid["fact_ids"] = []
    with pytest.raises(ValueError, match="non-empty"):
        validate_business_profile_artifact("exposure_publication", invalid)


def test_unknown_artifact_type_fails_closed_and_schema_copy_is_isolated():
    schema = get_business_profile_semantic_schema("promotion_decision")
    schema["properties"].clear()
    assert get_business_profile_semantic_schema("promotion_decision")["properties"]
    with pytest.raises(ValueError, match="unsupported business-profile artifact type"):
        get_business_profile_semantic_schema("whole_report")
