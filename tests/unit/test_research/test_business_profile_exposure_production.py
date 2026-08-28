import pytest

from research.business_profile_exposure_production import (
    BusinessProfileExposureAssumptionWriter,
    BusinessProfileExposureFactProducer,
    BusinessProfileExposurePublisher,
    GovernedCommodityMappingResolver,
)
from research.business_profile_product_catalog import (
    BusinessProductCatalog,
    CommodityReference,
    ProductCommodityMapping,
    ProductEntity,
)
from research.business_profile_governance import BusinessProfileResolver
from tests.unit.test_research.test_business_profile_exposure_components import (
    _approved_evidence,
    _promote,
    _storage,
)
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_promotion import FieldFamilyPromotionManifest


def _publication_context():
    manifest = FieldFamilyPromotionManifest(
        field_family="commodity_exposure_publication",
        enabled=True,
        benchmark_passed=True,
        identities={"test": "publication"},
    )
    return {
        "promotion_manifest": manifest,
        "promotion_gates": {name: True for name in manifest.required_gates},
    }


def _catalog(
    *,
    candidate_only=False,
    targets=1,
    applicable_to=None,
    exposure_role="revenue",
    evidence_requirement="explicit_product",
):
    product = ProductEntity(
        product_id="coal.coking_coal",
        label_zh="coking coal",
        label_en="Coking coal",
        product_kind="finished_product",
        industry_groups=("coal",),
    )
    mapping = ProductCommodityMapping(
        mapping_id=f"coal.coking.{exposure_role}.promoted.v1",
        product_id=product.product_id,
        commodity_id="COMMODITY.coal.coking_coal",
        exposure_role=exposure_role,
        targets=tuple(
            CommodityReference(
                "futures_instrument",
                f"CNF.JM.DCE.{index}",
                index,
                f"CNF.JM.DCE.{index}.main",
            )
            for index in range(1, targets + 1)
        ),
        ambiguity_policy="single_target" if targets == 1 else "one_to_many_review",
        evidence_requirement=evidence_requirement,
        candidate_only=candidate_only,
        promotion_evidence=("test:governed_registry",),
    )
    return BusinessProductCatalog(
        schema_version="business_profile_product_catalog.v3",
        catalog_version="test.products.v1",
        released_on="2026-01-01",
        document_applicable_from="2020-01-01",
        document_applicable_to=applicable_to,
        products=(product,),
        aliases=(),
        commodity_mappings=(mapping,),
    )


def _approved_sales_activity(
    repository,
    *,
    activity_id="activity-coking-coal-sales",
    object_raw="coking coal",
    object_id="coal.coking_coal",
    action="sells",
):
    payload = {
        "activity_id": activity_id,
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "subject_scope": "consolidated_group",
        "action": action,
        "object_type": "product",
        "object_raw": object_raw,
        "object_id": object_id,
        "segment_id": "coal",
        "value": 100.0,
        "unit": "tonne",
        "share": None,
        "evidence_id": "evidence-2025-ar",
        "run_id": "run-1",
        "data_available_date": "2026-03-28",
        "extraction_method": "native_table",
        "confidence": 1.0,
        "review_status": "candidate",
        "valid_from": "2025-01-01",
        "valid_to": "2026-12-31",
        "knowledge_from": "2026-03-28",
        "version": 1,
        "metadata": {"period_basis": "period_total"},
    }
    repository.upsert("activities", payload)
    _promote(
        repository,
        "activities",
        payload["activity_id"],
        references=["evidence-2025-ar"],
    )
    return next(
        item
        for item in repository.list_records("activities")
        if item["activity_id"] == payload["activity_id"]
    )


def test_approved_activity_maps_to_fact_and_preserves_unknown_materiality(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)

    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )

    assert fact["review_status"] == "candidate"
    assert fact["exposure_fact_type"] == "sales_volume"
    assert fact["share"] is None
    assert fact["metadata"]["unknown_value_preserved"] is False
    assert fact["metadata"]["source_activity_action"] == "sells"


def test_period_basis_is_preserved_and_unknown_does_not_assume_full_year(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    activity = {**activity, "metadata": {"period_basis": "instant"}}
    fact = BusinessProfileExposureFactProducer(repository).build_from_activity(activity)

    assert fact["metadata"]["period_basis"] == "instant"
    assert fact["metadata"]["period_basis_source"] == "activity"
    assert fact["metadata"]["numeric_reconciliation_valid"] is True


def test_publication_requires_external_manifest_and_gate_context(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(repository, "exposure_facts", fact["fact_id"], references=["evidence-2025-ar"])

    with pytest.raises(ValueError, match="promotion manifest is required"):
        BusinessProfileExposurePublisher(repository).publish_basic(
            fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30"
        )


def test_failed_gate_does_not_reuse_approved_publication_as_success(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(repository, "exposure_facts", fact["fact_id"], references=["evidence-2025-ar"])
    publisher = BusinessProfileExposurePublisher(
        repository, mapping_resolver=GovernedCommodityMappingResolver(_catalog())
    )
    context = _publication_context()
    assert publisher.publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **context
    )["status"] == "published"
    failed = dict(context["promotion_gates"])
    failed["no_conflicts"] = False
    result = publisher.publish_basic(
        fact_id=fact["fact_id"],
        knowledge_cutoff="2026-04-30",
        promotion_manifest=context["promotion_manifest"],
        promotion_gates=failed,
    )
    assert result["status"] == "held"
    assert result["reason"] == "publication_gates_failed"


def test_fact_production_rejects_candidate_activity(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    with pytest.raises(ValueError, match="approved activity"):
        BusinessProfileExposureFactProducer(repository).build_from_activity(
            {
                "activity_id": "candidate",
                "review_status": "candidate",
            }
        )


def test_unknown_unit_is_not_silently_classified_as_volume(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    activity = {
        "activity_id": "activity-unknown-unit",
        "instrument_id": "601088.SH",
        "report_period": "2025-12-31",
        "action": "purchases",
        "object_raw": "特殊材料",
        "object_id": "material.special",
        "value": 12,
        "unit": "未知计量单位",
        "evidence_id": "evidence-1",
        "review_status": "approved",
    }
    fact = BusinessProfileExposureFactProducer(repository).build_from_activity(activity)
    assert fact["exposure_fact_type"] == "purchase_activity"
    assert fact["value_normalized"] is None
    assert fact["metadata"]["publication_blocker"] == "unit_normalization_failed"


def test_mapping_resolution_preserves_identity_without_unique_market_series():
    candidate = GovernedCommodityMappingResolver(_catalog(candidate_only=True)).resolve(
        product_id="coal.coking_coal",
        exposure_role="revenue",
        evidence_requirement="explicit_product",
        knowledge_cutoff="2026-04-01",
    )
    multi_target = GovernedCommodityMappingResolver(_catalog(targets=2)).resolve(
        product_id="coal.coking_coal",
        exposure_role="revenue",
        evidence_requirement="explicit_product",
        knowledge_cutoff="2026-04-01",
    )

    assert candidate.commodity_id == "COMMODITY.coal.coking_coal"
    assert candidate.price_series_id is None
    assert multi_target.commodity_id == "COMMODITY.coal.coking_coal"
    assert multi_target.price_series_id is None


def test_mapping_resolution_still_fails_closed_for_stale_catalog():
    with pytest.raises(ValueError, match="stale"):
        GovernedCommodityMappingResolver(_catalog(applicable_to="2025-12-31")).resolve(
            product_id="coal.coking_coal",
            exposure_role="revenue",
            evidence_requirement="explicit_product",
            knowledge_cutoff="2026-04-01",
        )


@pytest.mark.parametrize(
    ("object_raw", "product_id", "commodity_id"),
    [
        ("聚乙烯", "polymer.polyethylene", "COMMODITY.polymer.polyethylene"),
        ("聚丙烯", "polymer.polypropylene", "COMMODITY.polymer.polypropylene"),
    ],
)
def test_known_commodity_identity_publishes_without_forcing_market_series(
    tmp_path, object_raw, product_id, commodity_id
):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(
        repository,
        activity_id=f"activity-{product_id}",
        object_raw=object_raw,
        object_id=product_id,
    )
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository,
        "exposure_facts",
        fact["fact_id"],
        references=["evidence-2025-ar"],
    )

    published = BusinessProfileExposurePublisher(repository).publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )

    assert published["status"] == "fact_only"
    assert published["reason"] == "market_series_unresolved"
    assert published["mapping"]["commodity_id"] == commodity_id


def test_composite_process_fact_is_retained_without_false_publication_gap(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(
        repository,
        activity_id="activity-coal-to-olefins",
        object_raw="煤制烯烃",
        object_id=None,
    )
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository,
        "exposure_facts",
        fact["fact_id"],
        references=["evidence-2025-ar"],
    )

    result = BusinessProfileExposurePublisher(repository).publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )

    persisted = repository.get_record("exposure_facts", fact["fact_id"])
    assert result["status"] == "fact_only"
    assert result["reason"] == "commodity_identity_unresolved"
    assert persisted["review_status"] == "approved"
    assert persisted["object_raw"] == "煤制烯烃"
    assert persisted["product_id"] is None


def test_mapped_production_activity_publishes_positive_output_exposure(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(
        repository,
        activity_id="activity-coking-coal-production",
        action="produces",
    )
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository,
        "exposure_facts",
        fact["fact_id"],
        references=["evidence-2025-ar"],
    )

    exposure = BusinessProfileExposurePublisher(
        repository,
        mapping_resolver=GovernedCommodityMappingResolver(_catalog()),
    ).publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )["exposure"]

    assert exposure["exposure_role"] == "revenue"
    assert exposure["direction"] == "positive"


def test_assumption_writer_rejects_llm_and_accepts_calibration(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    writer = BusinessProfileExposureAssumptionWriter(repository)
    with pytest.raises(ValueError, match="LLM"):
        writer.write(
            instrument_id="601088.SH",
            scope_type="product",
            scope_id="coal.coking_coal",
            assumption_type="pass_through_score",
            assumption_value=0.5,
            unit="fraction",
            method="llm_semantic_estimate",
            source_kind="calibrated",
            data_available_date="2026-04-01",
            effective_from="2026-01-01",
        )

    row = writer.write(
        instrument_id="601088.SH",
        scope_type="product",
        scope_id="coal.coking_coal",
        assumption_type="lag_days",
        assumption_value=20,
        unit="day",
        method="cross_correlation_v1",
        source_kind="calibrated",
        data_available_date="2026-04-01",
        effective_from="2026-01-01",
        sample_start="2021-01-01",
        sample_end="2025-12-31",
    )
    assert row["review_status"] == "candidate"
    assert row["metadata"]["source_kind"] == "calibrated"


def test_basic_publication_is_audited_idempotent_and_excludes_optional_assumptions(
    tmp_path,
):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository, "exposure_facts", fact["fact_id"], references=["evidence-2025-ar"]
    )
    publisher = BusinessProfileExposurePublisher(
        repository,
        mapping_resolver=GovernedCommodityMappingResolver(_catalog()),
    )

    first = publisher.publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )
    second = publisher.publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )

    exposure = first["exposure"]
    assert first["status"] == "published"
    assert second["status"] == "unchanged"
    assert exposure["review_status"] == "approved"
    assert exposure["direction"] == "positive"
    assert exposure["commodity_id"] == "COMMODITY.coal.coking_coal"
    assert exposure["commodity_id"] != fact["product_id"]
    assert exposure["price_series_id"] == "CNF.JM.DCE.1.main"
    assert exposure["materiality"] is None
    assert exposure["lag_days"] is None
    assert exposure["pass_through_score"] is None
    assert exposure["hedge_adjustment"] is None
    assert exposure["assumption_ids"] == []
    assert first["audit"]["reviewer"].startswith("system:")


def test_purchase_activity_uses_explicit_negative_input_cost_rule(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = dict(_approved_sales_activity(repository))
    activity = {
        key: value
        for key, value in activity.items()
        if key not in {"created_at", "updated_at", "lineage_hash", "review_status"}
    }
    activity.update(
        {
            "activity_id": "activity-coking-coal-purchase",
            "action": "purchases",
            "review_status": "candidate",
        }
    )
    repository.upsert("activities", activity)
    _promote(
        repository,
        "activities",
        activity["activity_id"],
        references=["evidence-2025-ar"],
    )
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository, "exposure_facts", fact["fact_id"], references=["evidence-2025-ar"]
    )
    publisher = BusinessProfileExposurePublisher(
        repository,
        mapping_resolver=GovernedCommodityMappingResolver(
            _catalog(
                exposure_role="feedstock_cost",
                evidence_requirement="explicit_raw_material",
            )
        ),
    )

    exposure = publisher.publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )["exposure"]
    assert exposure["exposure_role"] == "feedstock_cost"
    assert exposure["direction"] == "negative"
    assert ":purchases:feedstock_cost:negative" in exposure["direction_rule_id"]


def test_consumer_specific_publication_requires_approved_current_assumption(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository, "exposure_facts", fact["fact_id"], references=["evidence-2025-ar"]
    )
    publisher = BusinessProfileExposurePublisher(
        repository, mapping_resolver=GovernedCommodityMappingResolver(_catalog())
    )

    with pytest.raises(ValueError, match="missing or ambiguous"):
        publisher.publish_basic(
            fact_id=fact["fact_id"],
            knowledge_cutoff="2026-04-30",
            **_publication_context(),
            required_assumption_types=("lag_days",),
            consumer_id="dcf.v1",
        )

    assumption = BusinessProfileExposureAssumptionWriter(repository).write(
        instrument_id="601088.SH",
        scope_type="product",
        scope_id="coal.coking_coal",
        assumption_type="lag_days",
        assumption_value=20,
        unit="day",
        method="cross_correlation_v1",
        source_kind="calibrated",
        data_available_date="2026-04-01",
        effective_from="2026-01-01",
    )
    _promote(
        repository,
        "exposure_assumptions",
        assumption["assumption_id"],
        references=["calibration:v1"],
    )
    result = publisher.publish_basic(
        fact_id=fact["fact_id"],
        knowledge_cutoff="2026-04-30",
        **_publication_context(),
        required_assumption_types=("lag_days",),
        consumer_id="dcf.v1",
    )
    assert result["exposure"]["lag_days"] == 20
    assert result["exposure"]["assumption_ids"] == [assumption["assumption_id"]]
    assert result["exposure"]["scope_type"] == "model_consumer"


def test_future_fact_and_candidate_publication_do_not_leak_to_resolver(tmp_path):
    class _SeriesStorage:
        @staticmethod
        def get_series(series_id):
            return {"series_id": series_id, "active": True}

    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    fact = BusinessProfileExposureFactProducer(repository).persist_from_activity_id(
        activity["activity_id"]
    )
    _promote(
        repository, "exposure_facts", fact["fact_id"], references=["evidence-2025-ar"]
    )
    publisher = BusinessProfileExposurePublisher(
        repository, mapping_resolver=GovernedCommodityMappingResolver(_catalog())
    )

    with pytest.raises(ValueError, match="unavailable at cutoff"):
        publisher.publish_basic(
            fact_id=fact["fact_id"],
            knowledge_cutoff="2026-03-01",
            **_publication_context(),
        )

    published = publisher.publish_basic(
        fact_id=fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )["exposure"]
    candidate = dict(published)
    candidate.pop("created_at", None)
    candidate.pop("updated_at", None)
    candidate.pop("lineage_hash", None)
    candidate["exposure_id"] = "candidate-leak-check"
    candidate["commodity_id"] = "candidate-only"
    candidate["review_status"] = "candidate"
    repository.upsert("exposures", candidate)

    context = BusinessProfileResolver(
        repository, futures_storage=_SeriesStorage()
    ).resolve("601088.SH", as_of_date="2026-04-30", include_candidates=True)
    assert {item["exposure_id"] for item in context["approved_exposures"]} == {
        published["exposure_id"]
    }
    assert "candidate-leak-check" not in {
        item.get("exposure_id") for item in context["executable_exposure_mappings"]
    }


def test_new_fact_publication_supersedes_prior_exposure_at_knowledge_cutoff(tmp_path):
    repository = BusinessProfileRepository(_storage(tmp_path))
    _approved_evidence(repository)
    activity = _approved_sales_activity(repository)
    producer = BusinessProfileExposureFactProducer(repository)
    first_fact = producer.persist_from_activity_id(activity["activity_id"])
    _promote(
        repository,
        "exposure_facts",
        first_fact["fact_id"],
        references=["evidence-2025-ar"],
    )
    publisher = BusinessProfileExposurePublisher(
        repository, mapping_resolver=GovernedCommodityMappingResolver(_catalog())
    )
    first_exposure = publisher.publish_basic(
        fact_id=first_fact["fact_id"], knowledge_cutoff="2026-04-30", **_publication_context()
    )["exposure"]

    successor = {
        key: value
        for key, value in first_fact.items()
        if key
        not in {"fact_id", "created_at", "updated_at", "lineage_hash", "review_status"}
    }
    successor.update(
        {
            "fact_id": "exposure-fact-successor",
            "report_period": "2026-06-30",
            "value_raw": 120.0,
            "value_normalized": 120.0,
            "data_available_date": "2026-08-01",
            "knowledge_from": "2026-08-01",
            "supersedes_fact_id": first_fact["fact_id"],
            "review_status": "candidate",
            "version": 2,
        }
    )
    repository.upsert("exposure_facts", successor)
    _promote(
        repository,
        "exposure_facts",
        successor["fact_id"],
        references=["evidence-2025-ar"],
    )
    second_exposure = publisher.publish_basic(
        fact_id=successor["fact_id"], knowledge_cutoff="2026-08-15", **_publication_context()
    )["exposure"]

    assert second_exposure["supersedes_exposure_id"] == first_exposure["exposure_id"]
    assert second_exposure["version"] == 2
    eligible = repository.get_approved_as_of(
        "exposures", instrument_id="601088.SH", cutoff="2026-08-15"
    )
    assert [item["exposure_id"] for item in eligible] == [
        second_exposure["exposure_id"]
    ]
