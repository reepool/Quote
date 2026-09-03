import json
from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from research import company_profile
from research.company_profile import (
    PRODUCTION_AUTHORIZATION,
    Activity,
    CompanyProfileTaskResult,
    CoverageResult,
    Disposition,
    DispositionStatus,
    Measurement,
    ReportIdentity,
    contract_example_manifest,
    contract_schema_manifest,
    project_research_view,
    semantic_record_json_schema,
)
from research.company_profile.contracts import ContractErrorCode
from research.company_profile.models import SemanticRecord

ROOT = Path(__file__).resolve().parents[3]
REFERENCE_INPUT = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_input.json"
)
REFERENCE_EXPECTED = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_expected.json"
)
GOLD_PATH = (
    ROOT
    / "docs/development/company_profile_manufacturing_materials_gold_annotations.v1.json"
)
RECORD_ADAPTER = TypeAdapter(SemanticRecord)


def _validate_json(model, payload):
    return model.model_validate_json(json.dumps(payload, ensure_ascii=False))


def _record(payload):
    return RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))


def _reference_bundle():
    payload = json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))
    report = _validate_json(ReportIdentity, payload["report"])
    result = CompanyProfileTaskResult(
        request_id="reference-profile",
        records=tuple(_record(item) for item in payload["records"]),
        dispositions=tuple(
            _validate_json(Disposition, item) for item in payload["dispositions"]
        ),
        coverage=tuple(
            _validate_json(CoverageResult, item) for item in payload["coverage"]
        ),
        human_review_items=(),
        task_complete=True,
    )
    return payload, report, result


def _gold():
    return json.loads(GOLD_PATH.read_text(encoding="utf-8"))


def _gold_report(sample_id):
    instrument = sample_id.split("-")[2]
    suffix = (
        ".BJ"
        if instrument.startswith("92")
        else ".SH" if instrument.startswith("6") else ".SZ"
    )
    return {
        "instrument_id": f"{instrument}{suffix}",
        "report_id": f"gold:{sample_id}",
        "document_version": "gold.v1",
        "report_period": "2025-12-31",
        "published_at": "2026-09-03T00:00:00+08:00",
        "document_type": "annual_report",
    }


def _gold_evidence(annotation):
    evidence = annotation["evidence"]
    anchor = evidence["physical_anchor"]
    if {
        "row_label",
        "column_header",
        "cell_locator",
        "value_cell_locator",
    }.intersection(anchor):
        physical_anchor = {
            "anchor_type": "table",
            "table_label": evidence["section_title"],
            "row_label": anchor.get("row_label"),
            "column_header": anchor.get("column_header"),
            "cell_locator": anchor.get("cell_locator")
            or anchor.get("value_cell_locator"),
        }
    else:
        physical_anchor = {
            "anchor_type": "text",
            "bounded_quote": anchor.get("bounded_quote") or annotation["annotation_id"],
            "match_index": 0,
        }
    return {
        "evidence_id": f"gold-evidence:{annotation['annotation_id']}",
        "report": _gold_report(annotation["sample_id"]),
        "page": evidence["page"],
        "printed_page_label": evidence.get("printed_page_label"),
        "section_title": evidence["section_title"],
        "continuation_pages": evidence.get("continuation_pages", []),
        "subject_evidence_pages": evidence.get("subject_evidence_pages", []),
        "anchor": physical_anchor,
    }


def _adapt_observed_gold(annotation):
    semantic = annotation["semantic"]
    source = deepcopy(annotation["source_native"])
    source["footnote_refs"] = annotation["evidence"]["physical_anchor"].get(
        "footnote_refs", []
    )
    source["source_aliases"] = semantic.get("source_aliases", [])
    base = {
        "record_id": f"gold-record:{annotation['annotation_id']}",
        "field_id": annotation["field_id"],
        "chapter_task": annotation["chapter_task"],
        "report": _gold_report(annotation["sample_id"]),
        "subject_scope": semantic["subject_scope"],
        "subject_name": semantic.get("subject_name"),
        "subject_basis": semantic.get("subject_basis")
        or (
            "direct_source_wording"
            if semantic["subject_scope"] == "consolidated_group"
            else None
        ),
        "reported_period": semantic.get(
            "reported_period", semantic.get("period", "2025")
        ),
        "period_type": (
            "instant"
            if annotation["field_id"] == "inventory_volume"
            else "event" if semantic["object_type"] == "BusinessEvent" else "duration"
        ),
        "knowledge_time": semantic.get("knowledge_time"),
        "assertion_class": semantic["assertion_class"],
        "evidence": [_gold_evidence(annotation)],
        "source_native": source,
        "uncertainty": (
            [semantic["subject_uncertainty"]]
            if semantic.get("subject_uncertainty")
            else []
        ),
        "object_type": semantic["object_type"],
    }
    object_type = semantic["object_type"]
    if object_type == "BusinessOverview":
        base["source_text"] = annotation["evidence"]["physical_anchor"]["bounded_quote"]
    elif object_type == "Activity":
        base.update(
            action=semantic["action"],
            activity_actor="issuer",
            source_actor="issuer",
            actor_basis="direct_grammatical_actor",
            object_name=semantic["object_name"],
            source_verb="生产",
        )
    elif object_type == "Segment":
        base.update(
            dimension=semantic["segment_dimension"],
            label=semantic["segment_label"],
            row_class=semantic.get("row_class"),
        )
    elif object_type == "Measurement":
        base.update(
            metric_type=semantic["metric_type"],
            logical_slot=semantic["logical_slot"],
            measured_object=semantic.get("segment_label")
            or source.get("name")
            or annotation["field_id"],
            segment_dimension=semantic.get("segment_dimension"),
            segment_label=semantic.get("segment_label")
            or (source.get("name") if semantic.get("row_class") else None),
            processing_direction=(
                "external_service_provided"
                if semantic["metric_type"] == "processing_volume"
                else None
            ),
            row_class=semantic.get("row_class"),
            is_restated_comparative=semantic.get("comparison_basis")
            == "same_control_restated",
            comparison_basis=semantic.get("comparison_basis"),
            relationship_context=semantic.get("relationship_context"),
        )
    elif object_type == "Relationship":
        base.update(
            relation_type=semantic["relation_type"],
            object_name=semantic.get("object_name") or source["name"],
            identity_class=semantic.get("identity_class"),
        )
    elif object_type == "BusinessEvent":
        base.update(
            event_type=semantic["event_type"],
            description=source["name"],
            event_date=semantic.get("regime_effective_at"),
            regime_effective_at=semantic.get("regime_effective_at"),
            comparison_basis=semantic.get("comparison_basis"),
        )
    return _record(base)


def test_reference_profile_matches_stable_research_projection():
    payload, report, result = _reference_bundle()
    view = project_research_view(
        company_name=payload["company_name"], report=report, task_results=(result,)
    )

    assert view.model_dump(mode="json") == json.loads(
        REFERENCE_EXPECTED.read_text(encoding="utf-8")
    )
    assert view.production_authorization == PRODUCTION_AUTHORIZATION
    assert len(view.operating_measurements) == 7
    assert view.commodity_exposure.status == "not_assessed"
    assert view.value_chain_position.status == "insufficient_evidence"


def test_reference_profile_preserves_source_values_capacity_kind_and_empty_footnotes():
    _, _, result = _reference_bundle()
    measurements = {
        item.field_id: item for item in result.records if isinstance(item, Measurement)
    }

    assert measurements["operating_revenue"].source_native.value == "316506369"
    assert measurements["operating_revenue"].source_native.unit == "千元"
    assert measurements["gross_margin_reported"].source_native.value == "23.84"
    assert measurements["gross_margin_reported"].source_native.unit == "%"
    assert (
        measurements["production_capacity"].capacity_kind.value
        == "report_period_capacity"
    )
    assert measurements["inventory_volume"].source_native.footnote_refs == ()


def test_occurrence_identity_excludes_evidence_id_and_semantic_interpretation():
    _, _, result = _reference_bundle()
    revenue = next(
        item for item in result.records if item.field_id == "operating_revenue"
    )
    changed = revenue.model_dump(mode="json")
    changed["record_id"] = "different-record"
    changed["report"]["report_id"] = "regenerated-report-id"
    changed["evidence"][0]["evidence_id"] = "different-evidence"
    changed["evidence"][0]["report"] = changed["report"]
    changed["uncertainty"] = ["a new semantic interpretation"]
    changed_record = _record(changed)

    assert changed_record.occurrence_id() == revenue.occurrence_id()
    assert (
        changed_record.semantic_content_fingerprint()
        != revenue.semantic_content_fingerprint()
    )

    identity_only = revenue.model_dump(mode="json")
    identity_only["record_id"] = "identity-only-change"
    identity_only["report"]["report_id"] = "regenerated-report-id"
    identity_only["evidence"][0]["evidence_id"] = "regenerated-evidence-id"
    identity_only["evidence"][0]["report"] = identity_only["report"]
    identity_record = _record(identity_only)
    assert (
        identity_record.semantic_content_fingerprint()
        == revenue.semantic_content_fingerprint()
    )


def test_same_row_different_logical_slots_are_distinct_occurrences():
    _, _, result = _reference_bundle()
    revenue = next(
        item for item in result.records if item.field_id == "operating_revenue"
    )
    cost = next(item for item in result.records if item.field_id == "operating_cost")

    assert revenue.evidence[0].anchor.row_label == cost.evidence[0].anchor.row_label
    assert revenue.logical_slot != cost.logical_slot
    assert revenue.occurrence_id() != cost.occurrence_id()


def test_text_occurrence_uses_normalized_quote_not_evidence_identifier():
    _, _, result = _reference_bundle()
    activity = next(item for item in result.records if isinstance(item, Activity))
    payload = activity.model_dump(mode="json")
    payload["evidence"][0]["evidence_id"] = "regenerated"
    payload["evidence"][0]["anchor"][
        "bounded_quote"
    ] = "主要从事动力电池、  储能电池的研发、生产、销售"

    assert _record(payload).occurrence_id() == activity.occurrence_id()


def test_occurrence_identity_excludes_semantic_object_type():
    _, _, result = _reference_bundle()
    activity = next(item for item in result.records if isinstance(item, Activity))
    payload = activity.model_dump(mode="json")
    for field in (
        "action",
        "activity_actor",
        "source_actor",
        "actor_basis",
        "object_name",
        "source_verb",
    ):
        payload.pop(field)
    payload.update(
        record_id="same-source-overview",
        field_id="business_overview_source",
        object_type="BusinessOverview",
        source_text=activity.evidence[0].anchor.bounded_quote,
    )
    overview = _record(payload)

    assert overview.occurrence_id() == activity.occurrence_id()
    assert (
        overview.semantic_content_fingerprint()
        != activity.semantic_content_fingerprint()
    )


def test_occurrence_identity_is_stable_when_evidence_order_changes():
    _, _, result = _reference_bundle()
    activity = next(item for item in result.records if isinstance(item, Activity))
    other_evidence = next(
        item.evidence[0]
        for item in result.records
        if item.field_id == "material_input"
    )
    first = activity.model_copy(
        update={"evidence": (activity.evidence[0], other_evidence)}
    )
    reversed_order = activity.model_copy(
        update={"evidence": (other_evidence, activity.evidence[0])}
    )

    assert first.occurrence_id() == reversed_order.occurrence_id()


def test_business_overview_cannot_paraphrase_its_source_evidence():
    _, _, result = _reference_bundle()
    overview = next(
        item for item in result.records if item.object_type == "BusinessOverview"
    )
    payload = overview.model_dump(mode="json")
    payload["source_text"] = "公司具有显著技术领先优势"

    with pytest.raises(ValidationError, match="source_text must match text evidence"):
        _record(payload)


def test_gold_adapter_loads_all_approved_annotations_and_legal_empty_results():
    gold = _gold()
    observed = [
        item for item in gold["annotations"] if item["coverage_status"] == "observed"
    ]
    legal_empty = [
        item for item in gold["annotations"] if item["coverage_status"] != "observed"
    ]
    records = [_adapt_observed_gold(item) for item in observed]

    assert len(gold["annotations"]) == 24
    assert len(records) == 21
    assert len(legal_empty) == 3
    assert {item["coverage_status"] for item in legal_empty} == {
        "not_disclosed",
        "not_applicable",
    }
    assert len(gold["contract_negative_cases"]) == 19


def test_gold_processing_and_sales_measurements_use_separate_physical_anchors():
    records = {
        item["annotation_id"]: _adapt_observed_gold(item)
        for item in _gold()["annotations"]
        if item["coverage_status"] == "observed"
    }
    processing = records["mm-603659-processing-volume"]
    sales = records["mm-603659-coating-sales-volume"]

    assert processing.metric_type.value == "processing_volume"
    assert processing.processing_direction.value == "external_service_provided"
    assert processing.source_native.name == "涂覆加工量（销量）"
    assert processing.source_native.source_aliases == ("销量",)
    assert processing.occurrence_id() != sales.occurrence_id()


def test_gold_adjustment_is_one_marked_row_and_three_independent_measurements():
    records = [
        _adapt_observed_gold(item)
        for item in _gold()["annotations"]
        if item["annotation_id"].startswith("mm-603659-adjustment")
    ]

    assert len(records) == 4
    segment = next(item for item in records if item.object_type == "Segment")
    measurements = [item for item in records if item.object_type == "Measurement"]
    assert segment.dimension == "adjustment"
    assert segment.row_class.value == "consolidation_adjustment"
    assert {item.logical_slot.value for item in measurements} == {
        "revenue",
        "cost",
        "gross_margin",
    }
    assert all(
        item.row_class.value == "consolidation_adjustment" for item in measurements
    )


def test_gold_preserves_physical_pdf_page_and_printed_label():
    annotation = next(
        item
        for item in _gold()["annotations"]
        if item["annotation_id"] == "mm-302132-regime-effective"
    )
    event = _adapt_observed_gold(annotation)

    assert event.evidence[0].page == 59
    assert event.evidence[0].printed_page_label == "58"


def test_schema_is_generated_from_models_and_versioned():
    semantic_schema = semantic_record_json_schema()
    contracts = contract_schema_manifest()
    examples = contract_example_manifest()

    assert semantic_schema["$defs"]["Measurement"]["additionalProperties"] is False
    assert contracts["schema_version"] == "company_profile_semantic_contract.v1"
    assert contracts["extract_request"]["additionalProperties"] is False
    assert contracts["verify_response"]["additionalProperties"] is False
    assert examples["positive"]["repair_scope"]["maximum_attempts"] == 1
    assert (
        examples["negative"]["source_value_rewrite"]["error_code"]
        == "source_value_mutation"
    )


def test_chapter_task_and_action_sets_are_closed_to_the_approved_contract():
    assert {item.value for item in company_profile.ChapterTask} == {
        "extract_business_overview",
        "extract_segment_financials",
        "extract_operating_quantities",
        "extract_material_inputs",
        "extract_counterparties_and_concentration",
        "extract_business_regime",
    }
    assert {item.value for item in company_profile.ActivityAction} == {
        "develops",
        "produces",
        "processes",
        "sells",
        "purchases",
        "provides_service",
        "operates",
    }


@pytest.mark.parametrize(
    ("field_id", "metric_type", "logical_slot", "unit"),
    [
        ("sales_volume", "sales_volume", "sales_volume", "千元"),
        ("inventory_volume", "inventory_volume", "inventory_volume", "万元"),
    ],
)
def test_operating_volume_rejects_currency_values(
    field_id, metric_type, logical_slot, unit
):
    _, _, result = _reference_bundle()
    template = next(item for item in result.records if isinstance(item, Measurement))
    payload = template.model_dump(mode="json")
    payload.update(
        field_id=field_id,
        metric_type=metric_type,
        logical_slot=logical_slot,
        measured_object="电池系统",
        capacity_kind=None,
        processing_direction=None,
    )
    payload["source_native"].update(value="94526239", unit=unit)

    with pytest.raises(
        ValidationError, match="physical volume cannot use a currency unit"
    ):
        _record(payload)


def test_capacity_requires_kind_and_restated_comparative_requires_basis():
    _, _, result = _reference_bundle()
    capacity = next(
        item for item in result.records if item.field_id == "production_capacity"
    )
    missing_kind = capacity.model_dump(mode="json")
    missing_kind["capacity_kind"] = None
    with pytest.raises(
        ValidationError, match="production_capacity requires capacity_kind"
    ):
        _record(missing_kind)

    revenue = next(
        item for item in result.records if item.field_id == "operating_revenue"
    )
    missing_basis = revenue.model_dump(mode="json")
    missing_basis["is_restated_comparative"] = True
    missing_basis["comparison_basis"] = None
    with pytest.raises(
        ValidationError, match="restated comparative requires comparison_basis"
    ):
        _record(missing_basis)


def test_consolidated_subject_requires_affirmative_basis():
    _, _, result = _reference_bundle()
    revenue = next(
        item for item in result.records if item.field_id == "operating_revenue"
    )
    payload = revenue.model_dump(mode="json")
    payload["subject_basis"] = None

    with pytest.raises(
        ValidationError, match="consolidated_group requires affirmative"
    ):
        _record(payload)


def test_anonymous_identity_is_report_local_and_does_not_require_catalog_resolution():
    _, _, result = _reference_bundle()
    relationship = next(
        item
        for item in result.records
        if item.object_type == "Relationship"
        and item.field_id == "counterparty_relationship"
    )

    assert relationship.identity_class.value == "report_local_anonymous"
    assert relationship.external_entity_id is None
    invalid = relationship.model_dump(mode="json")
    invalid["external_entity_id"] = "global-catalog-id"
    with pytest.raises(ValidationError, match="report-local identity"):
        _record(invalid)


def test_explicit_confidentiality_reason_requires_source_wording():
    payload = {
        "field_id": "sales_volume",
        "chapter_task": "extract_operating_quantities",
        "requirement_level": "conditional",
        "status": "not_disclosed",
        "reason_code": "explicit_confidentiality",
    }

    with pytest.raises(ValidationError, match="requires source wording"):
        _validate_json(CoverageResult, payload)


def test_activity_rejects_numeric_fields_and_unknown_action():
    _, _, result = _reference_bundle()
    activity = next(item for item in result.records if isinstance(item, Activity))
    numeric = activity.model_dump(mode="json")
    numeric["value"] = "748"
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        _record(numeric)

    unknown = activity.model_dump(mode="json")
    unknown["action"] = "recycles"
    with pytest.raises(ValidationError):
        _record(unknown)


def test_processing_volume_rejects_missing_direction_and_metric_slot_mismatch():
    _, _, result = _reference_bundle()
    sales = next(item for item in result.records if item.field_id == "sales_volume")
    payload = sales.model_dump(mode="json")
    payload.update(
        field_id="processing_volume",
        metric_type="processing_volume",
        logical_slot="processing_volume",
    )
    with pytest.raises(ValidationError, match="requires external_service_provided"):
        _record(payload)

    payload.update(
        processing_direction="external_service_provided", logical_slot="sales_volume"
    )
    with pytest.raises(ValidationError, match="frozen mapping"):
        _record(payload)


def test_projection_contains_no_generated_summary_or_direction_claim():
    payload, report, result = _reference_bundle()
    projected = project_research_view(
        company_name=payload["company_name"], report=report, task_results=(result,)
    ).model_dump(mode="json")
    serialized = json.dumps(projected, ensure_ascii=False)

    assert "summary" not in projected
    assert "technology moat" not in serialized
    assert "profit_positive" not in serialized
    assert "profit_negative" not in serialized


def test_projection_excludes_blocked_records():
    payload, report, result = _reference_bundle()
    revenue = next(
        item for item in result.records if item.field_id == "operating_revenue"
    )
    blocked = CompanyProfileTaskResult(
        request_id="blocked",
        records=(revenue,),
        dispositions=(
            Disposition(
                target_id=revenue.record_id,
                field_id=revenue.field_id,
                status=DispositionStatus.BLOCKED,
                reason_codes=(ContractErrorCode.SUBJECT_UNSUPPORTED,),
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=False,
    )

    view = project_research_view(
        company_name=payload["company_name"], report=report, task_results=(blocked,)
    )

    assert view.operating_measurements == ()
    assert view.evidence_index == ()


def test_projection_rejects_records_from_another_report():
    payload, report, result = _reference_bundle()
    revenue = next(
        item for item in result.records if item.field_id == "operating_revenue"
    )
    other_report = revenue.report.model_copy(update={"report_id": "other-report"})
    other_evidence = tuple(
        item.model_copy(update={"report": other_report}) for item in revenue.evidence
    )
    other_record = revenue.model_copy(
        update={"report": other_report, "evidence": other_evidence}
    )
    mixed = CompanyProfileTaskResult(
        request_id="mixed-report",
        records=(other_record,),
        dispositions=(
            Disposition(
                target_id=other_record.record_id,
                field_id=other_record.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            ),
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )

    with pytest.raises(ValueError, match="cannot mix records"):
        project_research_view(
            company_name=payload["company_name"],
            report=report,
            task_results=(mixed,),
        )
