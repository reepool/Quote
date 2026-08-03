import copy
import json

import pytest

from research.business_profile_fact_catalog import (
    DEFAULT_BUSINESS_FACT_CATALOG_PATH,
    load_business_fact_catalog,
    parse_business_fact_catalog,
)


def _default_payload():
    return json.loads(DEFAULT_BUSINESS_FACT_CATALOG_PATH.read_text(encoding="utf-8"))


def test_default_catalog_covers_governed_record_types_and_dcf_fields():
    catalog = load_business_fact_catalog()

    assert catalog.schema_version == "business_profile_fact_catalog.v1"
    assert catalog.catalog_version == "business_profile_facts.2026.2"
    assert {definition.record_type for definition in catalog.definitions} == {
        "segments",
        "operating_facts",
        "value_chain_roles",
        "exposures",
    }
    assert {
        "segment.revenue",
        "operating.production_volume",
        "operating.unit_cost",
        "operating.reserve_or_resource",
        "role.value_chain",
        "exposure.commodity",
        "exposure.direction",
    }.issubset({definition.field_id for definition in catalog.definitions})

    dcf_fields = catalog.list_definitions(dcf_eligibility="approved_only")
    assert dcf_fields
    assert all(definition.requires_human_review for definition in dcf_fields)


def test_sensitive_fields_are_manual_and_keep_prohibited_inferences():
    catalog = load_business_fact_catalog()

    unit_cost = catalog.require("operating.unit_cost")
    reserve = catalog.require("operating.reserve_or_resource")
    exposure = catalog.require("exposure.pass_through_score")

    assert unit_cost.review_policy == "human_required_sensitive"
    assert reserve.review_policy == "human_required_sensitive"
    assert exposure.candidate_policy == "review_only"
    assert "do_not_treat_revenue_share_as_pass_through" in (
        exposure.prohibited_inferences
    )
    assert exposure.machine_candidate_enabled is False


def test_catalog_filters_by_record_type_and_returns_copy_payload():
    catalog = load_business_fact_catalog()

    exposure_fields = catalog.list_definitions(record_type="exposures")
    payload = catalog.to_dict()
    payload["fields"][0]["canonical_units"].append("mutated")

    assert exposure_fields
    assert all(item.record_type == "exposures" for item in exposure_fields)
    assert "mutated" not in catalog.definitions[0].canonical_units


def test_enum_values_are_separate_from_canonical_units():
    catalog = load_business_fact_catalog()

    segment_type = catalog.require("segment.type")
    direction = catalog.require("exposure.direction")

    assert segment_type.canonical_units == ()
    assert segment_type.allowed_values == (
        "product",
        "service",
        "industry",
        "geography",
        "customer",
        "other",
    )
    assert direction.canonical_units == ()
    assert direction.allowed_values == ("positive", "negative", "mixed")


def test_catalog_rejects_duplicate_field_ids():
    payload = _default_payload()
    payload["fields"].append(copy.deepcopy(payload["fields"][0]))

    with pytest.raises(ValueError, match="duplicate business fact field_id"):
        parse_business_fact_catalog(payload)


def test_catalog_rejects_approved_only_without_human_review():
    payload = _default_payload()
    field = next(
        item for item in payload["fields"] if item["dcf_eligibility"] == "approved_only"
    )
    field["review_policy"] = "rule_assisted_after_promotion"

    with pytest.raises(ValueError, match="approved_only field requires human review"):
        parse_business_fact_catalog(payload)


def test_catalog_rejects_sensitive_low_materiality_field():
    payload = _default_payload()
    field = next(
        item
        for item in payload["fields"]
        if item["review_policy"] == "human_required_sensitive"
    )
    field["materiality"] = "low"

    with pytest.raises(ValueError, match="must have critical or high materiality"):
        parse_business_fact_catalog(payload)


def test_catalog_rejects_numeric_field_without_units():
    payload = _default_payload()
    field = next(item for item in payload["fields"] if item["value_type"] == "decimal")
    field["canonical_units"] = []

    with pytest.raises(ValueError, match="numeric field requires canonical_units"):
        parse_business_fact_catalog(payload)


def test_catalog_rejects_enum_without_allowed_values():
    payload = _default_payload()
    field = next(item for item in payload["fields"] if item["value_type"] == "enum")
    field["allowed_values"] = []

    with pytest.raises(ValueError, match="enum field requires allowed_values"):
        parse_business_fact_catalog(payload)


def test_catalog_version_and_document_applicability_fail_closed():
    load_business_fact_catalog.cache_clear()

    with pytest.raises(ValueError, match="unsupported business fact catalog version"):
        load_business_fact_catalog(version="business_profile_facts.2099.1")
    with pytest.raises(ValueError, match="not applicable to document_date"):
        load_business_fact_catalog(document_date="2020-12-31")

    catalog = load_business_fact_catalog(document_date="2025-12-31")
    assert catalog.catalog_version == "business_profile_facts.2026.2"
    assert catalog.released_on == "2026-07-17"
    assert catalog.document_applicable_from == "2021-01-01"
