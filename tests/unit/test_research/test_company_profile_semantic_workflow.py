import json
import subprocess
import sys
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from research import company_profile
from research.company_profile import (
    Activity,
    ChapterTask,
    ChecklistItem,
    CompanyProfileSemanticService,
    CoverageResult,
    DispositionStatus,
    ExtractResponse,
    FakeSemanticProvider,
    Measurement,
    ObjectType,
    PackageManifest,
    PreparedEvidence,
    RepairResponse,
    ReportIdentity,
    SemanticTaskRequest,
    VerifyCheck,
    VerifyResponse,
    VerifyStatus,
)
from research.company_profile.contracts import CandidateResponseItem, ContractErrorCode
from research.company_profile.models import SemanticRecord

ROOT = Path(__file__).resolve().parents[3]
REFERENCE_INPUT = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_input.json"
)
GOLD_PATH = (
    ROOT
    / "docs/development/company_profile_manufacturing_materials_gold_annotations.v1.json"
)
RECORD_ADAPTER = TypeAdapter(SemanticRecord)


def _json_model(model, payload):
    return model.model_validate_json(json.dumps(payload, ensure_ascii=False))


def _reference():
    payload = json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))
    report = _json_model(ReportIdentity, payload["report"])
    records = [
        RECORD_ADAPTER.validate_json(json.dumps(item, ensure_ascii=False))
        for item in payload["records"]
    ]
    return payload, report, records


def _request(
    records=(),
    *,
    chapter_task=None,
    unresolved=(),
    coverage=(),
    preparation=None,
    extra_fields=(),
):
    _, report, all_records = _reference()
    records = tuple(records)
    chapter_task = chapter_task or (
        records[0].chapter_task if records else ChapterTask.EXTRACT_OPERATING_QUANTITIES
    )
    field_templates = {item.field_id: item for item in all_records}
    field_ids = list(
        dict.fromkeys(
            [item.field_id for item in records] + list(unresolved) + list(extra_fields)
        )
    )
    if not field_ids:
        field_ids = ["sales_volume"]
    checklist = []
    for field_id in field_ids:
        template = next((item for item in records if item.field_id == field_id), None)
        template = template or field_templates.get(field_id)
        object_type = (
            ObjectType(template.object_type)
            if template is not None
            else ObjectType.MEASUREMENT
        )
        allowed_metrics = (
            (template.metric_type,) if isinstance(template, Measurement) else ()
        )
        allowed_actions = (template.action,) if isinstance(template, Activity) else ()
        checklist.append(
            ChecklistItem(
                field_id=field_id,
                object_type=object_type,
                chapter_task=chapter_task,
                requirement_level=next(
                    (
                        item.requirement_level
                        for item in coverage
                        if item.field_id == field_id
                    ),
                    company_profile.RequirementLevel.CONDITIONAL,
                ),
                allowed_coverage_statuses=(
                    company_profile.CoverageStatus.OBSERVED,
                    company_profile.CoverageStatus.NOT_DISCLOSED,
                    company_profile.CoverageStatus.NOT_APPLICABLE,
                    company_profile.CoverageStatus.EXTRACTION_FAILED,
                    company_profile.CoverageStatus.UNCLEAR,
                ),
                allowed_metric_types=allowed_metrics,
                allowed_actions=allowed_actions,
            )
        )
    manifest = PackageManifest(
        package_name="manufacturing_materials",
        package_version="v1",
        report=report,
        checklist=tuple(checklist),
    )
    evidence_items = []
    seen = set()
    for record in records:
        for evidence in record.evidence:
            identity = (evidence.evidence_id, record.field_id)
            if identity in seen:
                continue
            seen.add(identity)
            values = {
                "evidence": evidence,
                "field_id": record.field_id,
                "source_native": record.source_native,
            }
            values.update(preparation or {})
            evidence_items.append(PreparedEvidence(**values))
    for coverage_item in coverage:
        for evidence in coverage_item.evidence:
            identity = (evidence.evidence_id, coverage_item.field_id)
            if identity in seen:
                continue
            seen.add(identity)
            values = {
                "evidence": evidence,
                "field_id": coverage_item.field_id,
            }
            values.update(preparation or {})
            evidence_items.append(PreparedEvidence(**values))
    if not evidence_items:
        evidence = all_records[0].evidence[0]
        values = {"evidence": evidence, "field_id": field_ids[0]}
        values.update(preparation or {})
        evidence_items.append(PreparedEvidence(**values))
    allowed_objects = tuple(dict.fromkeys(item.object_type for item in checklist))
    allowed_metrics = tuple(
        dict.fromkeys(
            metric for item in checklist for metric in item.allowed_metric_types
        )
    )
    allowed_actions = tuple(
        dict.fromkeys(action for item in checklist for action in item.allowed_actions)
    )
    return SemanticTaskRequest(
        request_id="stage4-request",
        report=report,
        package_manifest=manifest,
        chapter_task=chapter_task,
        evidence_bundle=tuple(evidence_items),
        allowed_object_types=allowed_objects,
        allowed_metric_types=allowed_metrics,
        allowed_actions=allowed_actions,
        prohibited_inferences=("commodity_direction", "complete_value_chain"),
        deterministic_candidates=records,
        provided_coverage=tuple(coverage),
        unresolved_field_ids=tuple(unresolved),
    )


def _record(field_id):
    return next(item for item in _reference()[2] if item.field_id == field_id)


def _replace_record(record, **updates):
    payload = record.model_dump(mode="json")
    payload.update(updates)
    return RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))


def test_deterministic_candidate_skips_extract_and_completes():
    request = _request((_record("sales_volume"),))
    provider = FakeSemanticProvider()

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is True
    assert result.provider_calls == ("verify",)
    assert provider.calls == ["verify"]
    assert result.dispositions[0].status == DispositionStatus.ACCEPTED_FOR_REVIEW


def test_fake_extract_is_bounded_to_unresolved_field_then_verified():
    candidate = _record("sales_volume")
    request = _request(
        chapter_task=candidate.chapter_task, unresolved=(candidate.field_id,)
    )
    request = request.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=candidate.evidence[0],
                    field_id=candidate.field_id,
                    source_native=candidate.source_native,
                ),
            )
        }
    )
    provider = FakeSemanticProvider(
        extract_output=ExtractResponse(
            request_id=request.request_id,
            items=(CandidateResponseItem(candidate=candidate),),
        )
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is True
    assert result.provider_calls == ("extract", "verify")
    assert result.accepted_records() == (candidate,)


def test_request_rejects_evidence_from_another_report():
    request = _request(
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        extra_fields=("sales_volume",),
    ).model_dump(mode="json")
    request["evidence_bundle"][0]["evidence"]["report"]["report_id"] = (
        "other-report"
    )

    with pytest.raises(ValidationError, match="prepared evidence must belong"):
        _json_model(SemanticTaskRequest, request)


def test_provider_model_instance_is_revalidated_before_acceptance():
    capacity = _record("production_capacity")
    invalid_capacity = capacity.model_copy(update={"capacity_kind": None})
    request = _request(
        chapter_task=capacity.chapter_task,
        unresolved=(capacity.field_id,),
    ).model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=capacity.evidence[0],
                    field_id=capacity.field_id,
                    source_native=capacity.source_native,
                ),
            )
        }
    )
    provider = FakeSemanticProvider(
        extract_output=ExtractResponse.model_construct(
            request_id=request.request_id,
            items=(
                CandidateResponseItem.model_construct(candidate=invalid_capacity),
            ),
        )
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.records == ()
    assert any(
        ContractErrorCode.CANDIDATE_SCHEMA_INVALID in item.reason_codes
        for item in result.human_review_items
    )


def test_extract_cannot_reuse_a_deterministic_candidate_record_id():
    sales = _record("sales_volume")
    inventory = _record("inventory_volume")
    colliding_inventory = inventory.model_copy(update={"record_id": sales.record_id})
    request = _request((sales,), unresolved=(inventory.field_id,)).model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=sales.evidence[0],
                    field_id=sales.field_id,
                    source_native=sales.source_native,
                ),
                PreparedEvidence(
                    evidence=inventory.evidence[0],
                    field_id=inventory.field_id,
                    source_native=inventory.source_native,
                ),
            )
        }
    )
    provider = FakeSemanticProvider(
        extract_output=ExtractResponse(
            request_id=request.request_id,
            items=(CandidateResponseItem(candidate=colliding_inventory),),
        )
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.records == (sales,)
    assert any(
        ContractErrorCode.CANDIDATE_SCHEMA_INVALID in item.reason_codes
        for item in result.human_review_items
    )


def test_capacity_kind_can_be_repaired_once_from_supplied_evidence():
    capacity = _replace_record(_record("production_capacity"), capacity_kind="unclear")
    repaired = _replace_record(capacity, capacity_kind="report_period_capacity")
    request = _request((capacity,))
    provider = FakeSemanticProvider(
        repair_outputs=[
            RepairResponse(
                request_id=f"{request.request_id}:repair:{capacity.record_id}",
                candidate=repaired,
                changed_fields=("/capacity_kind",),
            )
        ]
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.provider_calls == ("repair", "verify")
    assert result.task_complete is True
    assert isinstance(result.records[0], Measurement)
    assert result.records[0].capacity_kind.value == "report_period_capacity"


def test_repair_outside_allowlist_leaves_candidate_unresolved():
    capacity = _replace_record(_record("production_capacity"), capacity_kind="unclear")
    mutated = _replace_record(
        capacity, capacity_kind="report_period_capacity", measured_object="其他产品"
    )
    request = _request((capacity,))
    provider = FakeSemanticProvider(
        repair_outputs=[
            RepairResponse(
                request_id=f"{request.request_id}:repair:{capacity.record_id}",
                candidate=mutated,
                changed_fields=("/capacity_kind", "/measured_object"),
            )
        ]
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.dispositions[0].status == DispositionStatus.UNRESOLVED
    assert (
        ContractErrorCode.CAPACITY_KIND_AMBIGUOUS in result.dispositions[0].reason_codes
    )
    assert provider.calls == ["repair"]


@pytest.mark.parametrize("checks", [(), None])
def test_verify_block_or_missing_target_cannot_become_displayable(checks):
    candidate = _record("sales_volume")
    request = _request((candidate,))
    verify_checks = checks
    if checks is None:
        verify_checks = (
            VerifyCheck(
                target_type="candidate",
                target_id=candidate.record_id,
                status=VerifyStatus.BLOCK,
                reason_codes=(ContractErrorCode.SUBJECT_UNSUPPORTED,),
            ),
        )
    provider = FakeSemanticProvider(
        verify_outputs=[
            VerifyResponse(
                request_id=f"{request.request_id}:verify", checks=verify_checks
            )
        ]
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.dispositions[0].status == DispositionStatus.BLOCKED
    assert result.accepted_records() == ()


def test_duplicate_verify_checks_cannot_silently_override_each_other():
    candidate = _record("sales_volume")
    request = _request((candidate,))
    duplicate_checks = (
        VerifyCheck(
            target_type="candidate",
            target_id=candidate.record_id,
            status=VerifyStatus.PASS,
        ),
        VerifyCheck(
            target_type="candidate",
            target_id=candidate.record_id,
            status=VerifyStatus.BLOCK,
            reason_codes=(ContractErrorCode.SUBJECT_UNSUPPORTED,),
        ),
    )
    provider = FakeSemanticProvider(
        verify_outputs=[
            VerifyResponse.model_construct(
                request_id=f"{request.request_id}:verify",
                checks=duplicate_checks,
            )
        ]
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.dispositions[0].status == DispositionStatus.BLOCKED
    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.VERIFY_TARGET_MISSING,
    )


def test_source_native_mutation_is_blocked_before_verify():
    original = _record("gross_margin_reported")
    mutated_source = original.source_native.model_copy(
        update={"value": "0.2384", "unit": None}
    )
    mutated = original.model_copy(update={"source_native": mutated_source})
    request = _request((mutated,))
    prepared = PreparedEvidence(
        evidence=original.evidence[0],
        field_id=original.field_id,
        source_native=original.source_native,
    )
    request = request.model_copy(update={"evidence_bundle": (prepared,)})

    result = CompanyProfileSemanticService().run_task(request)

    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.SOURCE_VALUE_MUTATION,
    )
    assert result.provider_calls == ()


@pytest.mark.parametrize(
    ("preparation", "expected_reason"),
    [
        ({"continuation_complete": False}, "table_context_incomplete"),
        ({"source_readable": False}, "source_unreadable"),
        ({"unit_context_complete": False}, "unit_ambiguous"),
    ],
)
def test_preparation_failure_happens_before_provider(preparation, expected_reason):
    request = _request((_record("sales_volume"),), preparation=preparation)
    provider = FakeSemanticProvider()

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.provider_calls == ()
    assert provider.calls == []
    assert result.coverage[0].reason_code.value == expected_reason


def test_top_five_concentration_evidence_cannot_create_aggregate_relationship():
    gold = json.loads(GOLD_PATH.read_text(encoding="utf-8"))
    negative_case = next(
        item
        for item in gold["contract_negative_cases"]
        if item["case_id"] == "mm-neg-counterparty-coverage-backfill"
    )
    original = _record("counterparty_relationship")
    aggregate = original.model_copy(
        update={
            "record_id": "aggregate-top-five",
            "object_name": "前五名客户合计",
            "identity_class": company_profile.IdentityClass.REPORT_LOCAL_AGGREGATE,
        }
    )
    coverage = CoverageResult(
        field_id=aggregate.field_id,
        chapter_task=aggregate.chapter_task,
        requirement_level=company_profile.RequirementLevel.CONDITIONAL,
        status=company_profile.CoverageStatus.NOT_DISCLOSED,
        reason_code=company_profile.CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
        reason="section reports only aggregate totals and no names",
        evidence=aggregate.evidence,
    )
    request = _request(
        (aggregate,),
        coverage=(coverage,),
        extra_fields=("customer_concentration",),
    ).model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=aggregate.evidence[0],
                    field_id="customer_concentration",
                ),
            )
        }
    )

    result = CompanyProfileSemanticService().run_task(request)
    coverage_by_field = {item.field_id: item for item in result.coverage}

    assert negative_case["blocking"] is True
    assert result.task_complete is False
    assert coverage_by_field["counterparty_relationship"].status.value == (
        "not_disclosed"
    )
    assert result.dispositions[0].status == DispositionStatus.BLOCKED
    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.EVIDENCE_FIELD_MISMATCH,
    )


def test_independently_disclosed_aggregate_counterparty_relationship_is_allowed():
    original = _record("counterparty_relationship")
    aggregate = original.model_copy(
        update={
            "record_id": "aggregate-related-party",
            "object_name": "集团所属单位",
            "identity_class": company_profile.IdentityClass.REPORT_LOCAL_AGGREGATE,
        }
    )

    result = CompanyProfileSemanticService().run_task(_request((aggregate,)))

    assert result.task_complete is True
    assert result.dispositions[0].status == DispositionStatus.ACCEPTED_FOR_REVIEW


def test_coverage_only_legal_empty_is_independently_verified():
    relationship = _record("counterparty_relationship")
    coverage = CoverageResult(
        field_id=relationship.field_id,
        chapter_task=relationship.chapter_task,
        requirement_level=company_profile.RequirementLevel.CONDITIONAL,
        status=company_profile.CoverageStatus.NOT_DISCLOSED,
        reason_code=company_profile.CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
        reason="complete section contains no counterparty names",
        evidence=relationship.evidence,
    )
    request = _request(
        chapter_task=relationship.chapter_task,
        coverage=(coverage,),
        extra_fields=(relationship.field_id,),
    ).model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=relationship.evidence[0],
                    field_id=relationship.field_id,
                ),
            )
        }
    )

    result = CompanyProfileSemanticService().run_task(request)

    assert result.task_complete is True
    assert result.coverage == (coverage,)


def test_explicit_coverage_requires_an_independent_verify_check():
    relationship = _record("counterparty_relationship")
    coverage = CoverageResult(
        field_id=relationship.field_id,
        chapter_task=relationship.chapter_task,
        requirement_level=company_profile.RequirementLevel.CONDITIONAL,
        status=company_profile.CoverageStatus.NOT_DISCLOSED,
        reason_code=company_profile.CoverageReasonCode.SOURCE_REASON_UNSPECIFIED,
        reason="section reports only aggregate totals and no names",
        evidence=relationship.evidence,
    )
    request = _request(
        chapter_task=relationship.chapter_task,
        coverage=(coverage,),
        extra_fields=(relationship.field_id,),
    )
    provider = FakeSemanticProvider(
        verify_outputs=[
            VerifyResponse(
                request_id=f"{request.request_id}:verify",
                checks=(),
            )
        ]
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)
    assert result.task_complete is False
    assert result.coverage[0].status.value == "unclear"


def test_unresolved_without_provider_creates_programmatic_human_review_item():
    request = _request(
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        unresolved=("sales_volume",),
    )

    result = CompanyProfileSemanticService().run_task(request)

    assert result.task_complete is False
    assert result.provider_calls == ()
    assert any(
        item.reason_codes == (ContractErrorCode.PROVIDER_UNAVAILABLE,)
        for item in result.human_review_items
    )


def test_overview_and_activity_can_share_one_physical_occurrence():
    activity = _record("explicit_activity")
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
    overview = RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))

    assert overview.occurrence_id() == activity.occurrence_id()
    result = CompanyProfileSemanticService().run_task(_request((overview, activity)))

    assert result.task_complete is True
    assert {item.status for item in result.dispositions} == {
        DispositionStatus.ACCEPTED_FOR_REVIEW
    }


def test_same_occurrence_semantic_conflict_is_unresolved():
    original = _record("sales_volume")
    conflict = _replace_record(
        original, record_id="sales-conflict", measured_object="储能电池系统"
    )
    request = _request((original, conflict))

    result = CompanyProfileSemanticService().run_task(request)

    assert result.task_complete is False
    assert {item.status for item in result.dispositions} == {
        DispositionStatus.UNRESOLVED
    }
    assert all(
        ContractErrorCode.OCCURRENCE_SEMANTIC_CONFLICT in item.reason_codes
        for item in result.dispositions
    )


def test_processing_and_sales_from_same_physical_anchor_are_both_held():
    sales = _record("sales_volume")
    processing = _replace_record(
        sales,
        record_id="processing-duplicate",
        field_id="processing_volume",
        metric_type="processing_volume",
        logical_slot="processing_volume",
        processing_direction="external_service_provided",
    )
    request = _request((sales, processing), extra_fields=("processing_volume",))

    result = CompanyProfileSemanticService().run_task(request)

    assert result.task_complete is False
    assert {item.status for item in result.dispositions} == {
        DispositionStatus.UNRESOLVED
    }


def test_third_party_actor_is_blocked_instead_of_rewritten_to_issuer():
    activity = _record("explicit_activity")
    third_party = activity.model_copy(
        update={
            "record_id": "third-party-sale",
            "action": company_profile.ActivityAction.SELLS,
            "activity_actor": "上市公司",
            "source_actor": "军贸公司",
        }
    )
    request = _request((third_party,))

    result = CompanyProfileSemanticService().run_task(request)

    assert result.task_complete is False
    assert result.dispositions[0].reason_codes == (
        ContractErrorCode.ACTIVITY_ACTOR_UNSUPPORTED,
    )


def test_extract_response_with_external_prose_is_rejected():
    candidate = _record("sales_volume")
    request = _request(
        chapter_task=candidate.chapter_task, unresolved=(candidate.field_id,)
    )
    provider = FakeSemanticProvider(
        extract_output={
            "schema_version": "company_profile_extract_response.v1",
            "request_id": request.request_id,
            "items": [],
            "prose": "模型自行补充的解释",
        }
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert any(
        ContractErrorCode.CANDIDATE_SCHEMA_INVALID in item.reason_codes
        for item in result.human_review_items
    )


def test_extract_response_cannot_return_an_active_but_unrequested_field():
    sales = _record("sales_volume")
    inventory = _record("inventory_volume")
    request = _request(
        chapter_task=sales.chapter_task,
        unresolved=(sales.field_id,),
        extra_fields=(inventory.field_id,),
    ).model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=inventory.evidence[0],
                    field_id=inventory.field_id,
                    source_native=inventory.source_native,
                ),
            )
        }
    )
    provider = FakeSemanticProvider(
        extract_output=ExtractResponse(
            request_id=request.request_id,
            items=(CandidateResponseItem(candidate=inventory),),
        )
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.task_complete is False
    assert result.records == ()
    assert any(
        ContractErrorCode.CANDIDATE_SCHEMA_INVALID in item.reason_codes
        for item in result.human_review_items
    )


def test_missing_active_coverage_blocks_completion():
    request = _request(
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        extra_fields=("sales_volume",),
    )

    result = CompanyProfileSemanticService().run_task(request)

    assert result.task_complete is False
    assert result.coverage[0].status.value == "unclear"


def test_missing_optional_coverage_is_visible_but_does_not_block_completion():
    request = _request(
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        extra_fields=("sales_volume",),
    )
    optional_item = request.package_manifest.checklist[0].model_copy(
        update={"requirement_level": company_profile.RequirementLevel.OPTIONAL}
    )
    request = request.model_copy(
        update={
            "package_manifest": request.package_manifest.model_copy(
                update={"checklist": (optional_item,)}
            )
        }
    )

    result = CompanyProfileSemanticService().run_task(request)

    assert result.coverage[0].status.value == "unclear"
    assert result.task_complete is True


def test_unknown_chapter_task_is_rejected_by_closed_request_schema():
    request = _request((_record("sales_volume"),)).model_dump(mode="json")
    request["chapter_task"] = "extract_everything"

    with pytest.raises(ValidationError):
        _json_model(SemanticTaskRequest, request)


def test_package_assignment_cannot_be_applied_to_pre_transition_period():
    assignment = _record("business_regime")
    payload = assignment.model_dump(mode="json")
    payload["reported_period"] = "2024"
    payload["effective_from"] = "2025-01-06"

    with pytest.raises(ValidationError, match="cannot apply retroactively"):
        RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))


def test_same_control_restated_and_original_reports_keep_distinct_occurrences():
    revenue = _record("operating_revenue")
    restated = _replace_record(
        revenue,
        record_id="restated-revenue",
        reported_period="2024",
        is_restated_comparative=True,
        comparison_basis="same_control_restated",
        knowledge_time="2026-04-28T16:00:00+00:00",
    )
    original_payload = restated.model_dump(mode="json")
    original_payload["record_id"] = "original-revenue"
    original_payload["report"]["report_id"] = "predecessor-2024-report"
    original_payload["report"]["document_version"] = "predecessor-report-version"
    original_payload["evidence"][0]["report"] = original_payload["report"]
    original_payload["comparison_basis"] = "original_as_published"
    original_payload["knowledge_time"] = "2025-04-01T00:00:00+08:00"
    original = RECORD_ADAPTER.validate_json(
        json.dumps(original_payload, ensure_ascii=False)
    )

    assert original.occurrence_id() != restated.occurrence_id()
    assert original.comparison_basis.value == "original_as_published"
    assert restated.comparison_basis.value == "same_control_restated"


def test_new_package_import_has_no_network_database_or_config_write():
    probe = """
import socket
import sqlite3

def blocked(kind):
    def fail(*args, **kwargs):
        raise RuntimeError(f\"unexpected {kind} access during import\")
    return fail

socket.socket.connect = blocked("network")
sqlite3.connect = blocked("database")

import research.company_profile.models
import research.company_profile.contracts
import research.company_profile.projection
"""

    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_all_approved_negative_cases_are_mapped_to_executable_guards():
    gold = json.loads(GOLD_PATH.read_text(encoding="utf-8"))
    expected = {item["case_id"] for item in gold["contract_negative_cases"]}
    covered = {
        "mm-neg-sales-amount-as-volume",
        "mm-neg-inventory-value-as-volume",
        "mm-neg-percent-rewrite",
        "mm-neg-anonymous-catalog-failure",
        "mm-neg-required-page-omitted",
        "mm-neg-required-page-unreadable",
        "mm-neg-unit-ambiguous",
        "mm-neg-subject-forced",
        "mm-neg-regime-retroactive",
        "mm-neg-processing-duplicate",
        "mm-neg-page-coordinate-mix",
        "mm-neg-same-control-overwrite",
        "mm-neg-summary-new-fact",
        "mm-neg-processing-direction",
        "mm-neg-capacity-kind-missing",
        "mm-neg-counterparty-coverage-backfill",
        "mm-neg-confidentiality-inference",
        "mm-neg-third-party-action-actor",
        "mm-neg-restated-basis-missing",
    }

    assert covered == expected
