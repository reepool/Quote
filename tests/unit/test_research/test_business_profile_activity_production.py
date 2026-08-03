import pytest

from research.business_profile_activity_production import (
    BusinessProfileActivityProducer,
    GovernedCounterpartyResolver,
    classify_entity_resolution_exception,
)


class _Repository:
    pass


def test_exact_entity_resolution_and_ambiguity_fail_closed():
    resolver = GovernedCounterpartyResolver(
        entities=[
            {
                "entity_id": "entity-a",
                "official_identifier": "91310000A",
                "legal_name": "上海甲公司",
            },
            {
                "entity_id": "entity-b",
                "official_identifier": "91310000B",
                "legal_name": "上海乙公司",
            },
        ],
        aliases=[
            {
                "alias": "甲公司",
                "entity_id": "entity-a",
                "review_status": "approved",
            }
        ],
    )

    official = resolver.resolve("ignored", official_identifier="91310000A")
    alias = resolver.resolve("甲公司")

    assert official.entity_id == "entity-a"
    assert official.basis == "official_identifier"
    assert alias.entity_id == "entity-a"
    assert alias.basis == "approved_exact_alias"

    ambiguous = GovernedCounterpartyResolver(
        entities=[
            {"entity_id": "entity-a", "legal_name": "同名公司"},
            {"entity_id": "entity-b", "legal_name": "同名公司"},
        ]
    ).resolve("同名公司")
    exception = classify_entity_resolution_exception(ambiguous)
    assert ambiguous.status == "ambiguous"
    assert exception["tier"] == "quick_review"
    assert exception["ranked_local_choices"] == ["entity-a", "entity-b"]


def test_activity_candidate_is_atomic_and_role_is_derived_locally():
    producer = BusinessProfileActivityProducer(_Repository())
    activity = producer.build_activity_candidate(
        {
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "subject_scope": "issuer",
            "action": "produces",
            "object_type": "product",
            "object_raw": "动力煤",
            "object_id": "coal.thermal_coal",
            "segment_id": "coal",
            "confidence": 0.98,
        },
        evidence_id="evidence-2025-ar",
        run_id="run-1",
        data_available_date="2026-03-28",
        extraction_method="semantic_verified",
    )
    approved = {**activity, "review_status": "approved"}
    roles = producer.derive_role_candidates([approved])

    assert activity["action"] == "produces"
    assert "role" not in activity
    assert roles[0]["role"] == "producer"
    assert roles[0]["metadata"]["supporting_activity_ids"] == [
        activity["activity_id"]
    ]
    assert roles[0]["metadata"]["valuation_effects"] == {}


def test_multiple_segment_roles_are_preserved_and_scope_ambiguity_fails_closed():
    producer = BusinessProfileActivityProducer(_Repository())
    activities = []
    for action, segment_id, object_raw in (
        ("extracts", "mine", "原煤"),
        ("processes", "washing", "洗选煤"),
        ("trades", "trading", "煤炭贸易"),
    ):
        candidate = producer.build_activity_candidate(
            {
                "instrument_id": "601088.SH",
                "report_period": "2025-12-31",
                "subject_scope": "issuer",
                "action": action,
                "object_type": "product",
                "object_raw": object_raw,
                "segment_id": segment_id,
                "confidence": 1.0,
            },
            evidence_id="evidence-2025-ar",
            run_id="run-1",
            data_available_date="2026-03-28",
            extraction_method="native_text",
        )
        activities.append({**candidate, "review_status": "approved"})
    roles = producer.derive_role_candidates(activities)
    assert {(item["segment_id"], item["role"]) for item in roles} == {
        ("mine", "producer"),
        ("washing", "processor"),
        ("trading", "trader"),
    }
    assert all(item["metadata"]["valuation_effects"] == {} for item in roles)

    with pytest.raises(ValueError, match="issuer scope is unresolved"):
        producer.build_activity_candidate(
            {
                "instrument_id": "601088.SH",
                "report_period": "2025-12-31",
                "subject_scope": "named_subsidiary",
                "action": "produces",
                "object_type": "product",
                "object_raw": "动力煤",
            },
            evidence_id="evidence-2025-ar",
            run_id="run-1",
            data_available_date="2026-03-28",
            extraction_method="semantic",
        )


def test_named_relationship_requires_resolved_entity_and_preserves_direction():
    producer = BusinessProfileActivityProducer(_Repository())
    resolution = GovernedCounterpartyResolver(
        entities=[{"entity_id": "entity-customer", "legal_name": "客户股份有限公司"}]
    ).resolve("客户股份有限公司")
    record_type, relationship = producer.build_relationship_or_concentration_candidate(
        {
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "relationship_type": "sells_to",
            "counterparty_name_raw": "客户股份有限公司",
            "scope_type": "segment",
            "scope_id": "coal",
            "object_raw": "动力煤",
            "object_id": "coal.thermal_coal",
            "confidence": 0.95,
        },
        resolution=resolution,
        evidence_id="evidence-2025-ar",
        run_id="run-1",
        data_available_date="2026-03-28",
    )

    assert record_type == "relationships"
    assert relationship["direction"] == "outbound"
    assert relationship["counterparty_entity_id"] == "entity-customer"
    assert relationship["resolution_basis"] == "exact_legal_name"


def test_anonymous_disclosure_creates_concentration_fact_without_edge():
    producer = BusinessProfileActivityProducer(_Repository())
    record_type, fact = producer.build_relationship_or_concentration_candidate(
        {
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "relationship_type": "sells_to",
            "counterparty_name_raw": "客户A",
            "anonymous": True,
            "disclosed_share": 0.25,
            "scope_id": "company",
            "confidence": 1.0,
        },
        resolution=GovernedCounterpartyResolver(entities=[]).resolve("客户A"),
        evidence_id="evidence-2025-ar",
        run_id="run-1",
        data_available_date="2026-03-28",
    )

    assert record_type == "operating_facts"
    assert fact["fact_type"] == "customer_concentration_share"
    assert fact["metadata"]["no_relationship_edge_created"] is True
