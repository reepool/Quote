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
    assert roles[0]["metadata"]["supporting_activity_ids"] == [activity["activity_id"]]
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
    linked_activities = []
    for action, object_raw in (("purchases", "原煤"), ("sells", "洗选煤")):
        linked = producer.build_activity_candidate(
            {
                "instrument_id": "601088.SH",
                "report_period": "2025-12-31",
                "subject_scope": "issuer",
                "action": action,
                "object_type": "product",
                "object_raw": object_raw,
                "segment_id": "washing",
                "confidence": 1.0,
            },
            evidence_id="evidence-2025-ar",
            run_id="run-1",
            data_available_date="2026-03-28",
            extraction_method="native_text",
        )
        linked_activities.append({**linked, "review_status": "approved"})
    processor_activity = next(
        item for item in activities if item["action"] == "processes"
    )
    processor_activity["metadata"].update(
        {
            "transformation_input_activity_ids": [linked_activities[0]["activity_id"]],
            "transformation_input_fact_ids": ["fact-raw-coal-volume"],
            "transformation_output_activity_ids": [linked_activities[1]["activity_id"]],
        }
    )
    supporting_facts = [
        {
            "record_id": "fact-raw-coal-volume",
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "segment_id": "washing",
            "review_status": "approved",
        }
    ]
    activities.extend(linked_activities)
    roles = producer.derive_role_candidates(
        activities,
        supporting_facts=supporting_facts,
    )
    assert {(item["segment_id"], item["role"]) for item in roles} == {
        ("mine", "producer"),
        ("washing", "processor"),
        ("trading", "trader"),
    }
    assert all(item["metadata"]["valuation_effects"] == {} for item in roles)
    processor = next(item for item in roles if item["role"] == "processor")
    assert set(processor["metadata"]["supporting_activity_ids"]) >= {
        linked_activities[0]["activity_id"],
        linked_activities[1]["activity_id"],
    }
    assert processor["metadata"]["supporting_fact_ids"] == ["fact-raw-coal-volume"]

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


def test_standalone_processes_activity_does_not_create_processor_role():
    producer = BusinessProfileActivityProducer(_Repository())
    candidate = producer.build_activity_candidate(
        {
            "instrument_id": "601088.SH",
            "report_period": "2025-12-31",
            "subject_scope": "issuer",
            "action": "processes",
            "object_type": "product",
            "object_raw": "洗选煤",
            "segment_id": "washing",
            "confidence": 1.0,
        },
        evidence_id="evidence-2025-ar",
        run_id="run-1",
        data_available_date="2026-03-28",
        extraction_method="native_text",
    )
    approved = {**candidate, "review_status": "approved"}

    assert producer.derive_role_candidates([approved]) == []
    assert producer.role_derivation_gap(approved) == "transformation_lineage_missing"


def test_processor_links_must_resolve_to_approved_same_scope_inputs_and_outputs():
    producer = BusinessProfileActivityProducer(_Repository())

    def activity(action, object_raw, segment_id):
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
        return {**candidate, "review_status": "approved"}

    input_activity = activity("purchases", "原煤", "mine")
    output_activity = activity("sells", "洗选煤", "washing")
    processor = activity("processes", "洗选煤", "washing")
    processor["metadata"].update(
        {
            "transformation_input_activity_ids": [input_activity["activity_id"]],
            "transformation_output_activity_ids": [output_activity["activity_id"]],
        }
    )
    rows = [input_activity, output_activity, processor]

    assert producer.derive_role_candidates(rows) == []
    assert (
        producer.role_derivation_gap(processor, activities=rows)
        == "transformation_lineage_missing"
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


def test_distinct_anonymous_concentrations_in_same_evidence_have_distinct_ids():
    producer = BusinessProfileActivityProducer(_Repository())
    common = {
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "relationship_type": "sells_to",
        "anonymous": True,
        "object_raw": "收入",
        "scope_id": "601088.SH",
        "confidence": 1.0,
    }

    _, top_five = producer.build_relationship_or_concentration_candidate(
        {
            **common,
            "counterparty_name_raw": "前五大客户",
            "disclosed_share": 0.595,
        },
        resolution=GovernedCounterpartyResolver(entities=[]).resolve("前五大客户"),
        evidence_id="evidence-major-customers",
        run_id="run-1",
        data_available_date="2026-03-28",
    )
    _, related_parties = producer.build_relationship_or_concentration_candidate(
        {
            **common,
            "counterparty_name_raw": "关联方",
            "disclosed_share": 0.323,
        },
        resolution=GovernedCounterpartyResolver(entities=[]).resolve("关联方"),
        evidence_id="evidence-major-customers",
        run_id="run-1",
        data_available_date="2026-03-28",
    )

    assert top_five["record_id"] != related_parties["record_id"]
    assert top_five["fact_scope"] != related_parties["fact_scope"]
    assert top_five["metadata"]["anonymous_label"] == "前五大客户"
    assert related_parties["metadata"]["anonymous_label"] == "关联方"

    _, top_five_alias = producer.build_relationship_or_concentration_candidate(
        {
            **common,
            "counterparty_name_raw": "前五名客户",
            "object_raw": "营业收入",
            "disclosed_share": 0.61,
        },
        resolution=GovernedCounterpartyResolver(entities=[]).resolve("前五名客户"),
        evidence_id="evidence-major-customers-next-year",
        run_id="run-2",
        data_available_date="2027-03-28",
    )

    assert top_five_alias["fact_scope"] == top_five["fact_scope"]
    assert top_five_alias["metadata"]["anonymous_label_key"] == "top_five_customers"
    assert top_five_alias["metadata"]["object_key"] == "revenue"
