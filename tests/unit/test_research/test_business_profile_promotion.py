import pytest

from research.business_profile_promotion import (
    BusinessProfilePromotionClassifier,
    BusinessProfilePromotionService,
    FieldFamilyPromotionManifest,
    PromotionContext,
)
from research.business_profile_review import BusinessProfileReviewService
from tests.unit.test_research.test_business_profile_approval_integrity import (
    _candidate_evidence,
    _repository,
)


def _manifest(**overrides):
    values = {
        "field_family": "structured_segments",
        "enabled": True,
        "benchmark_passed": True,
        "identities": {
            "schema": "segments.v1",
            "parser": "table.v1",
            "selector": "selector.v1",
            "catalog": "facts.2026.2",
            "policy": "promotion.2026.1",
        },
    }
    values.update(overrides)
    return FieldFamilyPromotionManifest(**values)


def _gates(**overrides):
    gates = {
        "official_identity": True,
        "artifact_quality": True,
        "exact_evidence": True,
        "catalogs_current": True,
        "temporal_scope": True,
        "numeric_reconciliation": True,
        "no_conflicts": True,
        "field_family_manifest": True,
        "runtime_identity_match": True,
        "candidate_current": True,
        "semantic_proof": True,
    }
    gates.update(overrides)
    return gates


def _context(candidate, manifest, **overrides):
    values = {
        "target_type": "evidence",
        "target_id": candidate["evidence_id"],
        "instrument_id": candidate["instrument_id"],
        "field_family": manifest.field_family,
        "expected_updated_at": candidate["updated_at"],
        "gates": _gates(),
        "runtime_identities": manifest.identities,
        "evidence_references": ("document:hash:page:1",),
    }
    values.update(overrides)
    return PromotionContext(**values)


def test_classifier_auto_promotes_only_complete_current_boolean_gates():
    manifest = _manifest()
    candidate = {
        "evidence_id": "evidence-1",
        "instrument_id": "601088.SH",
        "updated_at": "2026-03-28T00:00:00+08:00",
    }
    classifier = BusinessProfilePromotionClassifier()

    passed = classifier.classify(_context(candidate, manifest), manifest)
    missing = classifier.classify(
        _context(
            candidate,
            manifest,
            gates={
                key: value for key, value in _gates().items() if key != "exact_evidence"
            },
        ),
        manifest,
    )
    stale = classifier.classify(
        _context(
            candidate,
            manifest,
            runtime_identities={**manifest.identities, "parser": "table.v2"},
        ),
        manifest,
    )

    assert passed["classification"] == "auto_promoted"
    assert passed["reason_codes"] == []
    assert missing["classification"] == "machine_rework"
    assert "missing_gate:exact_evidence" in missing["reason_codes"]
    assert stale["classification"] == "machine_rework"
    assert "runtime_identity_mismatch" in stale["reason_codes"]


@pytest.mark.parametrize(
    "reason",
    [
        "pass_through_judgment",
        "hedge_effectiveness_judgment",
        "unsupported_materiality",
        "ambiguous_direction",
        "conflicting_disclosures",
        "complex_scope_change",
    ],
)
def test_high_risk_economic_and_scope_reasons_always_route_deep_review(reason):
    manifest = _manifest()
    candidate = {
        "evidence_id": "evidence-1",
        "instrument_id": "601088.SH",
        "updated_at": "2026-03-28T00:00:00+08:00",
    }
    decision = BusinessProfilePromotionClassifier().classify(
        _context(candidate, manifest, high_risk_flags=(reason,)),
        manifest,
    )

    assert decision["classification"] == "deep_review"
    assert reason in decision["reason_codes"]


def test_service_promotes_through_system_review_and_rejects_concurrent_replay(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    candidate = repository.list_records("evidence")[0]
    manifest = _manifest()
    service = BusinessProfilePromotionService(BusinessProfileReviewService(repository))
    context = _context(candidate, manifest)

    result = service.process(context, manifest)

    approved = repository.list_records("evidence")[0]
    assert result["promoted"] is True
    assert approved["review_status"] == "approved"
    assert approved["reviewed_by"] == "system:business_profile_auto_promotion.v1"
    assert result["audit"]["metadata"]["gate_signature"]

    with pytest.raises(ValueError, match="stale business profile review state"):
        service.process(context, manifest)


def test_machine_rework_retries_are_bounded_and_clean_recovery_resolves_queue(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    candidate = repository.list_records("evidence")[0]
    manifest = _manifest()
    service = BusinessProfilePromotionService(
        BusinessProfileReviewService(repository),
        max_machine_retries=2,
    )
    rework = _context(
        candidate,
        manifest,
        gates=_gates(artifact_quality=False),
        exception_reasons=("ocr_required",),
    )

    first = service.process(rework, manifest)
    second = service.process(rework, manifest)
    exhausted = service.process(rework, manifest)

    assert first["exception"]["tier"] == "machine_rework"
    assert first["exception"]["retry_count"] == 1
    assert second["exception"]["retry_count"] == 2
    assert second["exception"]["next_retry_at"] is not None
    assert exhausted["exception"]["tier"] == "machine_rework"
    assert exhausted["exception"]["retry_count"] == 2
    assert exhausted["exception"]["next_retry_at"] is None
    assert "machine_rework_exhausted" in exhausted["exception"]["reason_codes"]

    recovered = service.process(_context(candidate, manifest), manifest)

    assert recovered["promoted"] is True
    assert service.list_exceptions(status="open") == []
    resolved = service.list_exceptions(status="resolved")
    assert len(resolved) == 1
    assert resolved[0]["resolved_at"] is not None


def test_quick_and_deep_review_queues_keep_evidence_and_ranked_choices(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    candidate = repository.list_records("evidence")[0]
    manifest = _manifest()
    service = BusinessProfilePromotionService(BusinessProfileReviewService(repository))

    quick = service.process(
        _context(
            candidate,
            manifest,
            exception_reasons=("entity_ambiguity",),
            ranked_choices=({"entity_id": "entity-1", "basis": "exact_name"},),
        ),
        manifest,
    )
    deep = service.process(
        _context(
            candidate,
            manifest,
            high_risk_flags=("ambiguous_issuer_scope",),
        ),
        manifest,
    )

    assert quick["decision"]["classification"] == "quick_review"
    assert quick["exception"]["ranked_choices"][0]["entity_id"] == "entity-1"
    assert quick["exception"]["evidence_references"] == ["document:hash:page:1"]
    assert deep["decision"]["classification"] == "deep_review"
    assert deep["exception"]["tier"] == "deep_review"
    assert deep["exception"]["gate_signature"] == deep["decision"]["gate_signature"]
    assert deep["exception"]["exception_id"] != quick["exception"]["exception_id"]
    assert len(service.list_exceptions(status="open")) == 2
    repository_exceptions = repository.list_exceptions(
        instrument_id=candidate["instrument_id"],
        target_type="evidence",
    )
    assert {item["tier"] for item in repository_exceptions} == {
        "quick_review",
        "deep_review",
    }
    assert repository_exceptions[0]["reason_codes"]


def test_prior_human_hold_is_not_overwritten_by_automatic_processing(tmp_path):
    repository, _ = _repository(tmp_path)
    repository.upsert("evidence", _candidate_evidence())
    candidate = repository.list_records("evidence")[0]
    review = BusinessProfileReviewService(repository)
    review.review_record(
        "evidence",
        candidate["evidence_id"],
        decision="held",
        reviewer="analyst@example",
        reason="scope ambiguity",
        expected_review_status="candidate",
        expected_updated_at=candidate["updated_at"],
    )
    held = repository.list_records("evidence")[0]
    manifest = _manifest()
    service = BusinessProfilePromotionService(review)

    result = service.process(
        _context(
            held,
            manifest,
            high_risk_flags=("prior_human_decision",),
        ),
        manifest,
    )

    assert result["promoted"] is False
    assert result["decision"]["classification"] == "deep_review"
    assert repository.list_records("evidence")[0]["review_status"] == "held"
