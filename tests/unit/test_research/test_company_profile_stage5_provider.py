from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from research.company_profile.contracts import (
    ChecklistItem,
    ContractErrorCode,
    ExtractResponse,
    PackageManifest,
    PreparedEvidence,
    RepairRequest,
    SemanticProviderError,
    SemanticTaskRequest,
    VerifyCheck,
    VerifyRequest,
    VerifyResponse,
    VerifyStatus,
)
from research.company_profile.models import (
    ActivityAction,
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    ComparisonBasis,
    CoverageReasonCode,
    CoverageResult,
    CoverageStatus,
    Evidence,
    MetricType,
    ObjectType,
    PeriodType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TableAnchor,
    TextAnchor,
)
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope
from research.company_profile.stage5_provider import (
    _SCOPE_INSTRUCTIONS,
    _TASK_INSTRUCTIONS,
    CommonGatewaySemanticProvider,
    _coverage_draft_schema,
    _expand_compact_measurements,
    _expand_extract_response,
    _merge_segment_partition_responses,
    _minimal_extract_schema,
    _minimal_verify_schema,
    _normalize_adapter_reported_period,
    _normalize_extract_response,
    _normalize_segment_partition_reported_period,
    _segment_dimension_options,
    _segment_partition_request,
    _unique_segment_scope_heading,
)
from research.company_profile.workflow import (
    CompanyProfileSemanticService,
    FakeSemanticProvider,
)
from utils.llm import (
    LlmDeadlineExceededError,
    LlmMessage,
    LlmRateLimitError,
    LlmRequest,
    LlmResponse,
)


@dataclass
class _FakeGatewayClient:
    outputs: list[Any]
    requests: list[LlmRequest] = field(default_factory=list)

    async def complete(self, request: LlmRequest) -> LlmResponse:
        self.requests.append(request)
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return LlmResponse(
            status="success",
            data=output,
            raw_content=json.dumps(output, ensure_ascii=False),
            provider="fake-gateway",
            model="fake-model",
            finish_reason="stop",
            usage=None,
            request_id=f"gateway-{len(self.requests)}",
            provider_request_id=None,
            request_hash="a" * 64,
            response_hash="b" * 64,
            schema_name=request.schema_name,
            schema_version=request.schema_version,
            structured_output_mode="json_schema",
            latency_ms=1,
            attempt_count=1,
        )


def test_common_gateway_provider_sends_one_bounded_scope_and_stage4_schema() -> None:
    prepared = _prepared_scope().model_copy(update={"scope_id": "generic_overview"})
    request = _extract_request(prepared)
    client = _FakeGatewayClient(
        outputs=[
            {
                "schema_version": "company_profile_extract_response.v1",
                "request_id": request.request_id,
                "items": [],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    assert response["request_id"] == request.request_id
    assert len(client.requests) == 1
    gateway_request = client.requests[0]
    safety_message = LlmMessage.from_value(gateway_request.messages[0])
    assert safety_message.is_safety_instruction is True
    assert "公司 alone does not prove consolidated_group" in safety_message.content
    assert "Every consolidated_group candidate must include" in safety_message.content
    assert gateway_request.schema_name == "company_profile_extract_response"
    assert isinstance(gateway_request.response_schema, dict)
    assert len(json.dumps(gateway_request.response_schema)) < 8_000
    item_schemas = gateway_request.response_schema["properties"]["items"]["items"][
        "oneOf"
    ]
    candidate_schema = next(
        item["properties"]["candidate"]
        for item in item_schemas
        if item["properties"]["item_type"]["const"] == "candidate"
    )
    assert [
        item["properties"]["object_type"]["const"] for item in candidate_schema["oneOf"]
    ] == ["BusinessOverview"]
    overview_schema = candidate_schema["oneOf"][0]
    assert "record_id" not in overview_schema["properties"]
    assert "report" not in overview_schema["properties"]
    assert "evidence" not in overview_schema["properties"]
    assert "evidence_ids" in overview_schema["properties"]
    envelope = json.loads(LlmMessage.from_value(gateway_request.messages[1]).content)
    assert envelope["request_scope"]["scope_id"] == "generic_overview"
    assert envelope["request_scope"]["field_ids"] == ["business_overview_source"]
    assert envelope["request_scope"]["scope_instructions"] == ""
    assert "page_contexts" in envelope["request_scope"]
    assert "evidence_bundle" not in envelope["runtime_request"]
    assert len(envelope["runtime_request"]["evidence_catalog"]) == 1
    assert "package_manifest" not in envelope["runtime_request"]
    assert (
        "activity_actor and source_actor must be the same"
        in envelope["request_scope"]["task_instructions"]
    )
    assert (
        "coordinated verbs in the same sentence"
        in envelope["request_scope"]["task_instructions"]
    )
    assert (
        "omit the Activity when neither basis is explicit"
        in envelope["request_scope"]["task_instructions"]
    )
    assert (
        "row printed on the continuation page"
        in _TASK_INSTRUCTIONS["extract_segment_financials"]
    )
    assert (
        "never invent residual, other, subtotal, or total rows"
        in _TASK_INSTRUCTIONS["extract_segment_financials"]
    )
    assert (
        "Never add or rewrite a dimension field"
        in _TASK_INSTRUCTIONS["extract_segment_financials"]
    )
    assert (
        "exact heading 报告分部的财务信息"
        in _TASK_INSTRUCTIONS["extract_segment_financials"]
    )
    assert (
        "Return compact JSON only"
        in _TASK_INSTRUCTIONS["extract_segment_financials"]
    )
    assert (
        "listing all three source values"
        in _TASK_INSTRUCTIONS["extract_segment_financials"]
    )
    assert (
        "capacity_under_construction must not carry capacity_kind"
        in _TASK_INSTRUCTIONS["extract_operating_quantities"]
    )
    assert (
        "第一名, 第二名, 客户A, or 供应商A"
        in _TASK_INSTRUCTIONS["extract_counterparties_and_concentration"]
    )
    assert (
        "gold" not in LlmMessage.from_value(gateway_request.messages[1]).content.lower()
    )
    assert provider.traces[0].call_type == "extract"
    assert provider.traces[0].status == "success"


@pytest.mark.parametrize(
    ("scope_id", "required_text"),
    [
        ("business_overview", "product uses, customer industries"),
        ("capacity_and_processing_narrative", "do not emit a second sales_volume"),
        ("capacity_project_narrative", "do not convert its value or unit"),
        ("procurement_mode", "to invent named material inputs"),
        ("customer_ranking_rows", "must not produce supplier Relationships"),
        ("supplier_ranking_rows", "must not produce customer Relationships"),
        ("reported_business_change", "consolidation-change scope"),
        ("top_five_customer_totals_only", "This request scope is totals-only"),
        ("top_five_supplier_totals_only", "This request scope is totals-only"),
        ("quantity_disclosure_check", "do not infer quantities from"),
        ("classified_volume_not_available", "not a parser or table-context failure"),
        ("reported_business_change", "Do not merge a separate"),
        ("same_control_comparison_basis", "not as Segment rows"),
        ("business_mode_and_extension", "not a BusinessRegime"),
        ("restructuring_commitment", "source-native Chinese"),
    ],
)
def test_common_gateway_provider_adds_frozen_scope_instructions(
    scope_id: str,
    required_text: str,
) -> None:
    prepared = _prepared_scope().model_copy(update={"scope_id": scope_id})
    request = _extract_request(prepared)
    client = _FakeGatewayClient(
        outputs=[
            {
                "schema_version": "company_profile_extract_response.v1",
                "request_id": request.request_id,
                "items": [],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    provider.extract(request)

    user_content = LlmMessage.from_value(client.requests[0].messages[1]).content
    if user_content.startswith("{"):
        envelope = json.loads(user_content)
        instructions = envelope["request_scope"]["scope_instructions"]
    else:
        instructions = user_content
    assert required_text in instructions
    assert required_text in _SCOPE_INSTRUCTIONS[scope_id]


def test_business_overview_uses_flat_source_and_activity_drafts() -> None:
    prepared = (
        _prepared_scope()
        .model_copy(update={"scope_id": "business_overview"})
        .model_copy(
            update={
                "field_ids": ("business_overview_source", "explicit_activity"),
            }
        )
    )
    checklist = (
        ChecklistItem(
            field_id="business_overview_source",
            object_type=ObjectType.BUSINESS_OVERVIEW,
            chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            requirement_level=RequirementLevel.REQUIRED,
            allowed_coverage_statuses=(
                CoverageStatus.OBSERVED,
                CoverageStatus.EXTRACTION_FAILED,
                CoverageStatus.UNCLEAR,
            ),
        ),
        ChecklistItem(
            field_id="explicit_activity",
            object_type=ObjectType.ACTIVITY,
            chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=(
                CoverageStatus.OBSERVED,
                CoverageStatus.EXTRACTION_FAILED,
                CoverageStatus.UNCLEAR,
            ),
            allowed_actions=tuple(ActivityAction),
        ),
    )
    request = SemanticTaskRequest(
        request_id="business-overview-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=checklist,
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.BUSINESS_OVERVIEW, ObjectType.ACTIVITY),
        allowed_actions=tuple(ActivityAction),
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    source_text = "公司主要从事动力电池研发、生产和销售。"
    client = _FakeGatewayClient(
        outputs=[
            {
                "overview": {
                    "source_name": "主要业务",
                    "source_text": source_text,
                    "evidence_id": evidence_id,
                },
                "activities": [
                    {
                        "action": "produces",
                        "actor": "公司",
                        "actor_basis": "direct_grammatical_actor",
                        "object_name": "动力电池",
                        "source_verb": "生产",
                        "evidence_id": evidence_id,
                    }
                ],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    schema = client.requests[0].response_schema
    assert set(schema["properties"]) == {"overview", "activities"}
    assert "subject_scope" not in json.dumps(schema)
    assert not LlmMessage.from_value(client.requests[0].messages[1]).content.startswith(
        "{"
    )
    overview = response["items"][0]["candidate"]
    activity = response["items"][1]["candidate"]
    assert overview["source_text"] == source_text
    assert overview["subject_scope"] == "unclear"
    assert overview["reported_period"] == "2025"
    assert activity["activity_actor"] == "公司"
    assert activity["source_actor"] == "公司"
    assert activity["source_verb"] == "生产"
    assert activity["source_native"]["header"] == "主要业务"


def test_procurement_mode_uses_flat_draft_and_reconstructs_mechanical_fields() -> None:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        scope_id="procurement_mode",
        field_id="material_input",
        source_text="公司生产所需主要原材料为铁矿石，采用集中采购模式。",
    ).model_copy(
        update={
            "candidate_pages": (14,),
        }
    )
    request = _material_input_extract_request(prepared)
    client = _FakeGatewayClient(
        outputs=[
            {
                "material_inputs": [],
                "coverage": {
                    "status": "not_disclosed",
                    "reason_code": "source_reason_unspecified",
                },
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)
    assert not LlmMessage.from_value(client.requests[0].messages[1]).content.startswith(
        "{"
    )

    schema = client.requests[0].response_schema
    assert set(schema["properties"]) == {"material_inputs", "coverage"}
    assert "subject_scope" not in json.dumps(schema)
    assert response["items"][0]["coverage"]["field_id"] == "material_input"
    assert response["items"][0]["coverage"]["status"] == "not_disclosed"


def test_totals_only_schema_cannot_emit_relationship_and_expands_measurement() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "top_five_supplier_totals_only",
            "chapter_task": ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            "field_ids": ("counterparty_relationship", "supplier_concentration"),
        }
    )
    request = _supplier_totals_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "measurements": [
                    {
                        "metric_type": "disclosed_share",
                        "name": "前五名供应商采购额占年度采购总额",
                        "value": "13.98",
                        "unit": "%",
                        "evidence_id": evidence_id,
                    }
                ],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)
    assert not LlmMessage.from_value(client.requests[0].messages[1]).content.startswith(
        "{"
    )

    schema_text = json.dumps(client.requests[0].response_schema)
    assert "Relationship" not in schema_text
    candidate = response["items"][0]["candidate"]
    assert candidate["field_id"] == "supplier_concentration"
    assert candidate["subject_scope"] == "unclear"
    assert candidate["reported_period"] == "2025"
    assert response["items"][1]["coverage"]["status"] == "not_disclosed"


def test_stage5_capacity_processing_scope_uses_flat_measurements() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "capacity_and_processing_narrative",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("production_capacity", "processing_volume"),
        }
    )
    statuses = (
        CoverageStatus.OBSERVED,
        CoverageStatus.EXTRACTION_FAILED,
        CoverageStatus.UNCLEAR,
    )
    metrics = (MetricType.PRODUCTION_CAPACITY, MetricType.PROCESSING_VOLUME)
    checklist = tuple(
        ChecklistItem(
            field_id=metric.value,
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=statuses,
            allowed_metric_types=(metric,),
        )
        for metric in metrics
    )
    request = SemanticTaskRequest(
        request_id="slice-1:capacity-processing",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=checklist,
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=metrics,
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "production_capacity": [
                    {
                        "name": "负极材料有效产能",
                        "value": "21.0",
                        "unit": "万吨",
                        "capacity_kind": "effective_capacity",
                        "evidence_id": evidence_id,
                    }
                ],
                "processing_volume": [
                    {
                        "name": "涂覆加工量（销量）",
                        "value": "109.42",
                        "unit": "亿㎡",
                        "evidence_id": evidence_id,
                    }
                ],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    schema = client.requests[0].response_schema
    assert set(schema["properties"]) == {"production_capacity", "processing_volume"}
    assert not LlmMessage.from_value(client.requests[0].messages[1]).content.startswith(
        "{"
    )
    candidates = [item["candidate"] for item in response["items"]]
    assert [item["field_id"] for item in candidates] == [
        "production_capacity",
        "processing_volume",
    ]
    assert candidates[0]["capacity_kind"] == "effective_capacity"
    assert candidates[1]["processing_direction"] == "external_service_provided"
    assert candidates[1]["logical_slot"] == "processing_volume"


def test_equal_utilization_rows_receive_distinct_table_occurrences() -> None:
    prepared = _prepared_scope()
    source_text = "产能及产能利用率\n类别 产能利用率\n铁 96%\n坯材 96%"
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "page": 22,
            "section_title": "产能及产能利用率",
            "anchor": TextAnchor(bounded_quote=source_text),
        }
    )
    prepared = prepared.model_copy(
        update={
            "scope_id": "capacity_and_steel_process_tables",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("capacity_utilization",),
            "evidence_bundle": (
                PreparedEvidence(evidence=evidence, field_id="capacity_utilization"),
            ),
            "page_contexts": (
                PreparedPageContext(
                    page=22,
                    text=source_text,
                    text_hash="d" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )
    checklist = ChecklistItem(
        field_id="capacity_utilization",
        object_type=ObjectType.MEASUREMENT,
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(MetricType.CAPACITY_UTILIZATION,),
    )
    request = SemanticTaskRequest(
        request_id="slice-1:equal-utilization",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(MetricType.CAPACITY_UTILIZATION,),
        unresolved_field_ids=("capacity_utilization",),
    )
    expanded = _expand_extract_response(
        {
            "measurements": [
                {
                    "metric_type": "capacity_utilization",
                    "name": "铁",
                    "value": "96",
                    "unit": "%",
                    "header": "产能利用率",
                    "evidence_id": evidence.evidence_id,
                },
                {
                    "metric_type": "capacity_utilization",
                    "name": "坯材",
                    "value": "96",
                    "unit": "%",
                    "header": "产能利用率",
                    "evidence_id": evidence.evidence_id,
                },
            ],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )
    candidates = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()

    assert [candidate.measured_object for candidate in candidates] == ["铁", "坯材"]
    assert all(
        isinstance(candidate.evidence[0].anchor, TableAnchor)
        for candidate in candidates
    )
    assert [candidate.evidence[0].anchor.row_label for candidate in candidates] == [
        "铁",
        "坯材",
    ]
    assert len({candidate.occurrence_id() for candidate in candidates}) == 2

    result = CompanyProfileSemanticService().run_task(
        request.model_copy(
            update={
                "deterministic_candidates": candidates,
                "unresolved_field_ids": (),
            }
        )
    )
    assert result.task_complete is True
    assert {item.status.value for item in result.dispositions} == {
        "accepted_for_review"
    }


def test_composite_processing_label_can_pass_one_primary_metric_verification() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "capacity_and_processing_narrative",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("processing_volume",),
        }
    )
    checklist = ChecklistItem(
        field_id="processing_volume",
        object_type=ObjectType.MEASUREMENT,
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(MetricType.PROCESSING_VOLUME,),
    )
    request = SemanticTaskRequest(
        request_id="slice-1:composite-processing",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(MetricType.PROCESSING_VOLUME,),
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "production_capacity": [],
                "processing_volume": [
                    {
                        "name": "涂覆加工量（销量）",
                        "value": "109.42",
                        "unit": "亿㎡",
                        "evidence_id": evidence_id,
                    }
                ],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    extracted = ExtractResponse.model_validate_json(
        json.dumps(provider.extract(request), ensure_ascii=False)
    )
    candidate = extracted.candidates()[0]
    assert candidate.source_native.name == "涂覆加工量（销量）"
    assert candidate.metric_type == MetricType.PROCESSING_VOLUME

    verify_request = VerifyRequest(
        request_id=f"{request.request_id}:verify",
        original_request_id=request.request_id,
        report=request.report,
        evidence_bundle=request.evidence_bundle,
        candidates=(candidate,),
        coverage=(),
    )
    client.outputs.append(
        {
            "schema_version": "company_profile_verify_response.v1",
            "request_id": verify_request.request_id,
            "checks": [
                {
                    "target_type": "candidate",
                    "target_id": candidate.record_id,
                    "status": "pass",
                    "reason_codes": [],
                }
            ],
        }
    )
    verified = provider.verify(verify_request)
    verify_instruction = LlmMessage.from_value(client.requests[1].messages[0]).content

    assert verified["checks"][0]["status"] == "pass"
    assert "加工量（销量） does not require a second metric" in verify_instruction


def test_general_operating_scope_uses_compact_measurements_and_full_local_validation() -> (
    None
):
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "capacity_narrative",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("production_capacity", "capacity_under_construction"),
        }
    )
    request = _capacity_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "measurements": [
                    {
                        "metric_type": "production_capacity",
                        "name": "报告期内产能",
                        "value": "772",
                        "unit": "GWh",
                        "capacity_kind": "report_period_capacity",
                        "evidence_id": evidence_id,
                    },
                    {
                        "metric_type": "capacity_under_construction",
                        "name": "在建产能",
                        "value": "321",
                        "unit": "GWh",
                        "evidence_id": evidence_id,
                    },
                ],
                "coverage": [],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    assert set(client.requests[0].response_schema["properties"]) == {
        "measurements",
        "coverage",
    }
    candidates = [item["candidate"] for item in response["items"]]
    assert [item["field_id"] for item in candidates] == [
        "production_capacity",
        "capacity_under_construction",
    ]
    assert candidates[0]["capacity_kind"] == "report_period_capacity"
    assert candidates[1]["capacity_kind"] is None


def test_non_totals_counterparty_scope_uses_compact_relationships_and_measurements() -> (
    None
):
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "supplier_ranking_rows",
            "chapter_task": ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            "field_ids": ("counterparty_relationship", "supplier_concentration"),
        }
    )
    request = _supplier_totals_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "relationships": [
                    {
                        "relation_type": "supplier",
                        "name": "供应商A",
                        "identity_class": "report_local_anonymous",
                        "evidence_id": evidence_id,
                    }
                ],
                "measurements": [
                    {
                        "metric_type": "supplier_purchase_amount",
                        "name": "供应商A采购额",
                        "value": "100",
                        "unit": "万元",
                        "evidence_id": evidence_id,
                    }
                ],
                "coverage": [],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    assert set(client.requests[0].response_schema["properties"]) == {
        "relationships",
        "measurements",
        "coverage",
    }
    relationship, measurement = [item["candidate"] for item in response["items"]]
    assert relationship["identity_class"] == "report_local_anonymous"
    assert relationship["field_id"] == "counterparty_relationship"
    assert measurement["field_id"] == "supplier_concentration"
    assert measurement["logical_slot"] == "supplier_purchase_amount"


def test_counterparty_measurements_bind_customer_and_supplier_directions() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "top_five_totals_and_legal_empty_names",
            "chapter_task": ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            "field_ids": (
                "counterparty_relationship",
                "customer_concentration",
                "supplier_concentration",
            ),
        }
    )
    checklist = (
        ChecklistItem(
            field_id="counterparty_relationship",
            object_type=ObjectType.RELATIONSHIP,
            chapter_task=prepared.chapter_task,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
        ),
        ChecklistItem(
            field_id="customer_concentration",
            object_type=ObjectType.MEASUREMENT,
            chapter_task=prepared.chapter_task,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
            allowed_metric_types=(
                MetricType.CUSTOMER_SALES_AMOUNT,
                MetricType.DISCLOSED_SHARE,
            ),
        ),
        ChecklistItem(
            field_id="supplier_concentration",
            object_type=ObjectType.MEASUREMENT,
            chapter_task=prepared.chapter_task,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
            allowed_metric_types=(
                MetricType.SUPPLIER_PURCHASE_AMOUNT,
                MetricType.DISCLOSED_SHARE,
            ),
        ),
    )
    request = SemanticTaskRequest(
        request_id="counterparty-both-directions",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=checklist,
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.RELATIONSHIP, ObjectType.MEASUREMENT),
        allowed_metric_types=(
            MetricType.CUSTOMER_SALES_AMOUNT,
            MetricType.SUPPLIER_PURCHASE_AMOUNT,
            MetricType.DISCLOSED_SHARE,
        ),
        unresolved_field_ids=(
            "counterparty_relationship",
            "customer_concentration",
            "supplier_concentration",
        ),
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "relationships": [],
                "measurements": [
                    {
                        "metric_type": "customer_sales_amount",
                        "name": "前五名客户销售额",
                        "value": "100",
                        "unit": "亿元",
                        "evidence_id": evidence_id,
                    },
                    {
                        "metric_type": "supplier_purchase_amount",
                        "name": "前五名供应商采购额",
                        "value": "95",
                        "unit": "亿元",
                        "evidence_id": evidence_id,
                    },
                    {
                        "metric_type": "disclosed_share",
                        "name": "前五名客户销售额占比",
                        "value": "20",
                        "unit": "%",
                        "evidence_id": evidence_id,
                    },
                    {
                        "metric_type": "disclosed_share",
                        "name": "前五名供应商采购额占比",
                        "value": "30",
                        "unit": "%",
                        "evidence_id": evidence_id,
                    },
                ],
                "coverage": [],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.extract(request)
    candidates = [item["candidate"] for item in response["items"]]

    assert [item["field_id"] for item in candidates] == [
        "customer_concentration",
        "supplier_concentration",
        "customer_concentration",
        "supplier_concentration",
    ]
    assert [item["logical_slot"] for item in candidates] == [
        "customer_sales_amount",
        "supplier_purchase_amount",
        "disclosed_share",
        "disclosed_share",
    ]


def test_related_party_scope_schema_rejects_transaction_measurements() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "related_party_sales_purchases_and_services",
            "chapter_task": ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            "field_ids": ("counterparty_relationship",),
        }
    )
    checklist = ChecklistItem(
        field_id="counterparty_relationship",
        object_type=ObjectType.RELATIONSHIP,
        chapter_task=prepared.chapter_task,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
    )
    request = SemanticTaskRequest(
        request_id="related-party-only",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.RELATIONSHIP,),
        unresolved_field_ids=("counterparty_relationship",),
    )
    client = _FakeGatewayClient(
        outputs=[{"relationships": [], "measurements": [], "coverage": []}]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    provider.extract(request)

    schema = client.requests[0].response_schema
    assert schema["properties"]["measurements"] == {
        "type": "array",
        "maxItems": 0,
    }
    user_message = LlmMessage.from_value(client.requests[0].messages[1]).content
    assert "not top-five customer or supplier concentration" in user_message


def test_material_input_scope_can_return_observed_items_without_false_coverage() -> (
    None
):
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "material_risk_disclosure",
            "chapter_task": ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "field_ids": ("material_input",),
        }
    )
    request = _material_input_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "material_inputs": [{"name": "正极材料", "evidence_id": evidence_id}],
                "coverage": None,
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    assert response["items"][0]["candidate"]["object_name"] == "正极材料"
    assert all("coverage" not in item for item in response["items"])


def test_business_regime_scope_uses_compact_events_and_restores_full_record() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "equity_transfer_effective",
            "chapter_task": ChapterTask.EXTRACT_BUSINESS_REGIME,
            "field_ids": ("business_regime",),
        }
    )
    checklist = ChecklistItem(
        field_id="business_regime",
        object_type=ObjectType.BUSINESS_EVENT,
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        requirement_level=RequirementLevel.REQUIRED,
        allowed_coverage_statuses=tuple(CoverageStatus),
    )
    request = SemanticTaskRequest(
        request_id="business-regime-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(
            ObjectType.BUSINESS_EVENT,
            ObjectType.BUSINESS_REGIME,
            ObjectType.INDUSTRY_PACKAGE_ASSIGNMENT,
        ),
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            {
                "events": [
                    {
                        "event_type": "equity_transfer_effective",
                        "description": "成飞100.00%的股权完成过户",
                        "event_date": "2025-01-06",
                        "regime_effective_at": "2025-01-06",
                        "evidence_id": evidence_id,
                    }
                ],
                "regimes": [],
                "package_assignments": [],
                "coverage": [],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    candidate = response["items"][0]["candidate"]
    assert candidate["object_type"] == "BusinessEvent"
    assert candidate["event_date"] == "2025-01-06"
    assert candidate["regime_effective_at"] == "2025-01-06"
    assert candidate["knowledge_time"] == prepared.report.published_at
    assert candidate["evidence"][0]["evidence_id"] == evidence_id


def test_restructuring_commitment_uses_occurrence_period_not_report_year() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "restructuring_commitment",
            "chapter_task": ChapterTask.EXTRACT_BUSINESS_REGIME,
            "field_ids": ("business_regime",),
        }
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    expanded = _expand_extract_response(
        {
            "events": [
                {
                    "event_type": "major_asset_restructuring_project_launched",
                    "description": "2023年，公司启动收购成飞100%股权重大资产重组项目。",
                    "event_date": "2023",
                    "evidence_id": evidence_id,
                }
            ],
            "regimes": [],
            "package_assignments": [],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )

    candidate = expanded["items"][0]["candidate"]
    assert candidate["reported_period"] == "2023"
    assert candidate["knowledge_time"] == prepared.report.published_at
    assert candidate["source_native"]["value"].startswith("2023年，公司启动")


def test_reported_business_change_keeps_event_and_not_applicable_coverage_separate() -> (
    None
):
    prepared = _reported_business_change_scope()
    prepared = prepared.model_copy(
        update={
            "scope_id": "reported_business_change",
            "chapter_task": ChapterTask.EXTRACT_BUSINESS_REGIME,
            "field_ids": ("business_regime",),
        }
    )
    checklist = ChecklistItem(
        field_id="business_regime",
        object_type=ObjectType.BUSINESS_EVENT,
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        requirement_level=RequirementLevel.REQUIRED,
        allowed_coverage_statuses=tuple(CoverageStatus),
    )
    request = SemanticTaskRequest(
        request_id="business-change-separation",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(
            ObjectType.BUSINESS_EVENT,
            ObjectType.BUSINESS_REGIME,
            ObjectType.INDUSTRY_PACKAGE_ASSIGNMENT,
        ),
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(
            outputs=[
                {
                    "events": [
                        {
                            "event_type": "consolidation_scope_change",
                            "description": "报告期内合并范围发生变动",
                            "evidence_id": evidence_id,
                        }
                    ],
                    "regimes": [],
                    "package_assignments": [],
                    "coverage": [
                        {
                            "field_id": "business_regime",
                            "status": "not_applicable",
                            "reason_code": "source_explicitly_not_applicable",
                            "evidence_ids": [evidence_id],
                        }
                    ],
                }
            ]
        ),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = ExtractResponse.model_validate_json(
        json.dumps(provider.extract(request), ensure_ascii=False)
    )

    assert len(response.candidates()) == 1
    assert response.candidates()[0].event_type == "consolidation_scope_change"
    assert response.coverage_results()[0].status == CoverageStatus.NOT_APPLICABLE
    assert response.coverage_results()[0].reason_code == (
        CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE
    )

    result = CompanyProfileSemanticService().run_task(
        request,
        provider=FakeSemanticProvider(extract_output=response),
    )

    assert len(result.accepted_records()) == 1
    assert result.coverage[0].status == CoverageStatus.NOT_APPLICABLE
    assert result.coverage[0].reason_code == (
        CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE
    )
    assert result.task_complete is True


def test_product_extension_drops_operating_mode_no_change_coverage() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "business_mode_and_extension",
            "chapter_task": ChapterTask.EXTRACT_BUSINESS_REGIME,
            "field_ids": ("business_regime",),
        }
    )
    source_text = (
        "报告期内，新增电子级羟胺水溶液供应。公司在报告期内，经营模式未发生重大变化。"
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "section_title": "商业模式 / 产品扩展",
            "anchor": TextAnchor(bounded_quote=source_text),
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=evidence, field_id="business_regime"),
            ),
            "page_contexts": (
                PreparedPageContext(
                    page=evidence.page,
                    text=source_text,
                    text_hash="e" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )
    request = _business_regime_request(prepared)
    expanded = _expand_extract_response(
        {
            "events": [
                {
                    "event_type": "product_extension",
                    "description": "报告期内，新增电子级羟胺水溶液供应。",
                    "evidence_id": evidence.evidence_id,
                }
            ],
            "regimes": [],
            "package_assignments": [],
            "coverage": [
                {
                    "field_id": "business_regime",
                    "status": "not_applicable",
                    "reason_code": "source_explicitly_not_applicable",
                    "evidence_ids": [evidence.evidence_id],
                }
            ],
        },
        request=request,
        prepared_scope=prepared,
    )
    response = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    )

    assert len(response.candidates()) == 1
    assert response.candidates()[0].event_type == "product_extension"
    assert response.coverage_results() == ()

    result = CompanyProfileSemanticService().run_task(
        request,
        provider=FakeSemanticProvider(extract_output=response),
    )

    assert len(result.accepted_records()) == 1
    assert result.coverage[0].status == CoverageStatus.OBSERVED
    assert result.task_complete is True


def test_consolidation_change_not_applicable_is_explicitly_supported_for_verify() -> (
    None
):
    prepared = _reported_business_change_scope()
    source_text = "（八）合并报表范围的变化情况 □适用 √不适用"
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "section_title": "合并报表范围的变化情况",
            "anchor": TextAnchor(bounded_quote=source_text),
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=evidence, field_id="business_regime"),
            ),
            "page_contexts": (
                PreparedPageContext(
                    page=evidence.page,
                    text=source_text,
                    text_hash="d" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )
    coverage = CoverageResult(
        field_id="business_regime",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        requirement_level=RequirementLevel.REQUIRED,
        status=CoverageStatus.NOT_APPLICABLE,
        reason_code=CoverageReasonCode.SOURCE_EXPLICITLY_NOT_APPLICABLE,
        evidence=(evidence,),
    )
    verify_request = VerifyRequest(
        request_id="consolidation-change-not-applicable:verify",
        original_request_id="consolidation-change-not-applicable",
        report=prepared.report,
        evidence_bundle=prepared.evidence_bundle,
        candidates=(),
        coverage=(coverage,),
    )
    client = _FakeGatewayClient(
        outputs=[
            {
                "schema_version": "company_profile_verify_response.v1",
                "request_id": verify_request.request_id,
                "checks": [
                    {
                        "target_type": "coverage",
                        "target_id": "extract_business_regime:business_regime",
                        "status": "pass",
                        "reason_codes": [],
                    }
                ],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.verify(verify_request)
    instruction = LlmMessage.from_value(client.requests[0].messages[0]).content

    assert response["checks"][0]["status"] == "pass"
    assert "合并报表范围的变化情况" in instruction
    assert "answers only whether consolidation scope changed" in instruction
    assert "must not create a BusinessEvent" in instruction


def test_material_input_verifier_accepts_explicit_energy_inputs() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "material_and_energy_table",
            "chapter_task": ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "field_ids": ("material_input",),
        }
    )
    source_text = "原材料及能源名称 蒸汽 合理范围 定向采购 电 合理范围 定向采购"
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "section_title": "主要原材料及能源采购",
            "anchor": TextAnchor(bounded_quote=source_text),
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=evidence, field_id="material_input"),
            ),
            "page_contexts": (
                PreparedPageContext(
                    page=evidence.page,
                    text=source_text,
                    text_hash="f" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )
    request = _material_input_extract_request(prepared)
    extract_response = ExtractResponse.model_validate_json(
        json.dumps(
            _expand_extract_response(
                {
                    "material_inputs": [
                        {"name": "蒸汽", "evidence_id": evidence.evidence_id},
                        {"name": "电", "evidence_id": evidence.evidence_id},
                    ],
                    "coverage": None,
                },
                request=request,
                prepared_scope=prepared,
            ),
            ensure_ascii=False,
        )
    )
    verify_request = VerifyRequest(
        request_id=f"{request.request_id}:verify",
        original_request_id=request.request_id,
        report=prepared.report,
        evidence_bundle=prepared.evidence_bundle,
        candidates=extract_response.candidates(),
        coverage=(),
    )
    client = _FakeGatewayClient(
        outputs=[
            {
                "schema_version": "company_profile_verify_response.v1",
                "request_id": verify_request.request_id,
                "checks": [
                    {
                        "target_type": "candidate",
                        "target_id": candidate.record_id,
                        "status": "pass",
                        "reason_codes": [],
                    }
                    for candidate in extract_response.candidates()
                ],
            }
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    response = provider.verify(verify_request)
    instruction = LlmMessage.from_value(client.requests[0].messages[0]).content

    assert len(response["checks"]) == 2
    assert "covers both explicitly named raw-material inputs" in instruction
    assert "do not return object_not_allowed" in instruction


def test_reported_business_change_rejects_event_only_when_source_says_not_applicable() -> (
    None
):
    prepared = _reported_business_change_scope()
    checklist = ChecklistItem(
        field_id="business_regime",
        object_type=ObjectType.BUSINESS_EVENT,
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        requirement_level=RequirementLevel.REQUIRED,
        allowed_coverage_statuses=tuple(CoverageStatus),
    )
    request = SemanticTaskRequest(
        request_id="business-change-missing-coverage",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.BUSINESS_EVENT,),
        unresolved_field_ids=prepared.field_ids,
    )
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(
            outputs=[
                {
                    "events": [
                        {
                            "event_type": "consolidation_scope_change",
                            "description": "报告期内合并范围发生变动",
                            "evidence_id": prepared.evidence_bundle[
                                0
                            ].evidence.evidence_id,
                        }
                    ],
                    "regimes": [],
                    "package_assignments": [],
                    "coverage": [],
                }
            ]
        ),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=1000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    assert provider.traces[0].error_code == "candidate_schema_invalid"


def test_segment_financials_use_compact_rows_and_expand_locally() -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    compact = {
        "schema_version": "company_profile_extract_response.v1",
        "request_id": request.request_id,
        "items": [
            {
                "item_type": "segment_row",
                "row": {
                    "label": "动力电池系统",
                    "subject_scope": "unclear",
                    "reported_period": "2025",
                    "period_type": "duration",
                    "evidence_ids": [evidence_id],
                    "cells": {
                        "operating_revenue": {
                            "value": "316,506,369",
                            "unit": "千元",
                            "header": "营业收入",
                        },
                        "operating_cost": {
                            "value": "241,064,397",
                            "unit": "千元",
                            "header": "营业成本",
                        },
                        "gross_margin_reported": {
                            "value": "23.84%",
                            "unit": "%",
                            "header": "毛利率",
                        },
                    },
                },
            }
        ],
    }
    client = _FakeGatewayClient(outputs=[compact])
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    schema = _minimal_extract_schema(request, prepared_scope=prepared)
    assert len(json.dumps(schema)) < 5_000
    row_schema = schema["properties"]["items"]["items"]["oneOf"][0]["properties"]["row"]
    assert "dimension" not in row_schema["properties"]
    assert "dimension" not in row_schema["required"]
    assert row_schema["properties"]["label"] == {"enum": ["动力电池系统"]}
    assert row_schema["properties"]["evidence_ids"]["items"] == {"enum": [evidence_id]}
    assert (
        schema["properties"]["items"]["items"]["oneOf"][0]["properties"]["item_type"][
            "const"
        ]
        == "segment_row"
    )
    candidates = [item["candidate"] for item in response["items"]]
    assert [item["object_type"] for item in candidates] == [
        "Segment",
        "Measurement",
        "Measurement",
        "Measurement",
    ]
    assert [item["field_id"] for item in candidates] == [
        "segment_dimension",
        "operating_revenue",
        "operating_cost",
        "gross_margin_reported",
    ]
    assert len({item["record_id"] for item in candidates}) == 4
    assert candidates[1]["logical_slot"] == "revenue"
    assert candidates[1]["segment_label"] == "动力电池系统"
    assert candidates[1]["evidence"][0]["evidence_id"] == evidence_id
    assert candidates[1]["evidence"][0]["anchor"] == {
        "anchor_type": "table",
        "table_label": "分产品",
        "row_label": "动力电池系统",
        "column_header": "营业收入",
        "cell_locator": None,
    }
    assert candidates[0]["dimension"] == "分产品"
    assert candidates[0]["source_native"]["header"] == "分产品"
    assert all(
        candidate["subject_scope"] == "business_segment" for candidate in candidates
    )


def test_high_cardinality_segment_financials_partition_and_merge_once() -> None:
    prepared = _high_cardinality_segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    request_ids = [
        f"{request.request_id}:partition-{index:02d}-of-03" for index in range(1, 4)
    ]
    outputs = [
        _segment_partition_response(
            request_id=request_ids[0],
            evidence_id=evidence_id,
            metric_field="operating_revenue",
        ),
        _segment_partition_response(
            request_id=request_ids[1],
            evidence_id=evidence_id,
            metric_field="operating_cost",
        ),
        _segment_partition_response(
            request_id=request_ids[2],
            evidence_id=evidence_id,
            metric_field="gross_margin_reported",
        ),
    ]
    admissions: list[int] = []
    client = _FakeGatewayClient(outputs=outputs)
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
        physical_call_admission=lambda: admissions.append(len(admissions) + 1),
    )

    response = provider.extract(request)

    assert len(client.requests) == 3
    assert admissions == [1, 2, 3]
    runtime_fields = []
    for gateway_request in client.requests:
        user_message = LlmMessage.from_value(gateway_request.messages[1])
        envelope = json.loads(user_message.content)
        runtime_fields.append(envelope["runtime_request"]["unresolved_field_ids"])
    assert runtime_fields == [
        ["segment_dimension", "operating_revenue"],
        ["segment_dimension", "operating_cost"],
        ["segment_dimension", "gross_margin_reported"],
    ]
    candidates = [item["candidate"] for item in response["items"]]
    assert [candidate["field_id"] for candidate in candidates] == [
        "segment_dimension",
        "operating_revenue",
        "operating_cost",
        "gross_margin_reported",
    ]
    assert len([item for item in candidates if item["object_type"] == "Segment"]) == 1
    assert [trace.semantic_request_id for trace in provider.traces] == request_ids
    assert all(
        trace.parent_semantic_request_id == request.request_id
        for trace in provider.traces
    )
    assert [trace.partition_index for trace in provider.traces] == [1, 2, 3]
    assert all(trace.partition_count == 3 for trace in provider.traces)


def test_segment_partition_merge_reconciles_owned_metadata_and_evidence() -> None:
    prepared = _multi_evidence_high_cardinality_segment_prepared_scope()
    request = _segment_extract_request(prepared)
    metrics = (
        "operating_revenue",
        "operating_cost",
        "gross_margin_reported",
    )
    periods = ("2025-12-31", "2025年", "2025年度")
    scopes = ("unclear", "business_segment", "unclear")
    partitions = []
    for index, (metric, period, subject_scope, prepared_evidence) in enumerate(
        zip(metrics, periods, scopes, prepared.evidence_bundle, strict=True),
        start=1,
    ):
        partition_request = _segment_partition_request(
            request,
            fields=("segment_dimension", metric),
            partition_index=index,
            partition_count=3,
        )
        partitions.append(
            (
                partition_request,
                _segment_partition_response(
                    request_id=partition_request.request_id,
                    evidence_id=prepared_evidence.evidence.evidence_id,
                    metric_field=metric,
                    subject_scope=subject_scope,
                    reported_period=period,
                    uncertainty=("source-native row",) if index == 2 else (),
                ),
            )
        )

    merged = _merge_segment_partition_responses(
        request,
        tuple(partitions),
        prepared_scope=prepared,
    )

    row = merged["items"][0]["row"]
    assert set(row["evidence_ids"]) == {
        item.evidence.evidence_id for item in prepared.evidence_bundle
    }
    assert row["subject_scope"] == "unclear"
    assert row["reported_period"] == "2025"
    assert row["knowledge_time"] == prepared.report.published_at
    assert row["uncertainty"] == ["source-native row"]
    assert set(row["cells"]) == set(metrics)


def test_segment_partition_multi_evidence_expands_through_existing_provider_path() -> None:
    prepared = _multi_evidence_high_cardinality_segment_prepared_scope()
    request = _segment_extract_request(prepared)
    metrics = (
        "operating_revenue",
        "operating_cost",
        "gross_margin_reported",
    )
    outputs = []
    for index, (metric, prepared_evidence) in enumerate(
        zip(metrics, prepared.evidence_bundle, strict=True),
        start=1,
    ):
        outputs.append(
            _segment_partition_response(
                request_id=f"{request.request_id}:partition-{index:02d}-of-03",
                evidence_id=prepared_evidence.evidence.evidence_id,
                metric_field=metric,
                subject_scope="business_segment" if index == 2 else "unclear",
                reported_period=("2025-12-31", "2025", "2025")[index - 1],
            )
        )
    client = _FakeGatewayClient(outputs=outputs)
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.extract(request)

    assert len(client.requests) == 3
    candidates = [item["candidate"] for item in response["items"]]
    assert [item["field_id"] for item in candidates] == [
        "segment_dimension",
        *metrics,
    ]
    expected_evidence_ids = {
        item.evidence.evidence_id for item in prepared.evidence_bundle
    }
    assert all(
        {item["evidence_id"] for item in candidate["evidence"]}
        == expected_evidence_ids
        for candidate in candidates
    )
    measurement_by_field = {
        item["field_id"]: item for item in candidates if item["object_type"] == "Measurement"
    }
    for field_id, evidence_id in zip(
        metrics,
        ("segment-revenue-evidence", "segment-cost-evidence", "segment-margin-evidence"),
        strict=True,
    ):
        anchors = {
            item["evidence_id"]: item["anchor"]
            for item in measurement_by_field[field_id]["evidence"]
        }
        assert anchors[evidence_id]["anchor_type"] == "table"
        assert all(
            anchor["anchor_type"] == ("table" if item_id == evidence_id else "text")
            for item_id, anchor in anchors.items()
        )


def test_segment_partition_merge_rejects_period_and_duplicate_cell_conflicts() -> None:
    prepared = _high_cardinality_segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    revenue_request = _segment_partition_request(
        request,
        fields=("segment_dimension", "operating_revenue"),
        partition_index=1,
        partition_count=2,
    )
    cost_request = _segment_partition_request(
        request,
        fields=("segment_dimension", "operating_cost"),
        partition_index=2,
        partition_count=2,
    )
    revenue = _segment_partition_response(
        request_id=revenue_request.request_id,
        evidence_id=evidence_id,
        metric_field="operating_revenue",
    )
    cost = _segment_partition_response(
        request_id=cost_request.request_id,
        evidence_id=evidence_id,
        metric_field="operating_cost",
        reported_period="2024",
    )

    with pytest.raises(
        ValueError,
        match="identity conflict: reported_period: '2025' != '2024'",
    ):
        _merge_segment_partition_responses(
            request,
            ((revenue_request, revenue), (cost_request, cost)),
            prepared_scope=prepared,
        )

    duplicate_request = _segment_partition_request(
        request,
        fields=("segment_dimension", "operating_revenue"),
        partition_index=2,
        partition_count=2,
    )
    duplicate = _segment_partition_response(
        request_id=duplicate_request.request_id,
        evidence_id=evidence_id,
        metric_field="operating_revenue",
    )
    with pytest.raises(ValueError, match="duplicate metric cell"):
        _merge_segment_partition_responses(
            request,
            ((revenue_request, revenue), (duplicate_request, duplicate)),
            prepared_scope=prepared,
        )


def test_segment_partition_provider_failure_returns_no_partial_extract() -> None:
    prepared = _high_cardinality_segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    client = _FakeGatewayClient(
        outputs=[
            _segment_partition_response(
                request_id=f"{request.request_id}:partition-01-of-03",
                evidence_id=evidence_id,
                metric_field="operating_revenue",
            ),
            LlmRateLimitError("rate limited"),
            _segment_partition_response(
                request_id=f"{request.request_id}:partition-03-of-03",
                evidence_id=evidence_id,
                metric_field="gross_margin_reported",
            ),
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.PROVIDER_UNAVAILABLE
    assert len(client.requests) == 2
    assert [trace.status for trace in provider.traces] == ["success", "failed"]
    assert all(
        trace.parent_semantic_request_id == request.request_id
        for trace in provider.traces
    )


def test_segment_partition_merge_conflict_fails_closed() -> None:
    prepared = _high_cardinality_segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    outputs = [
        _segment_partition_response(
            request_id=f"{request.request_id}:partition-01-of-03",
            evidence_id=evidence_id,
            metric_field="operating_revenue",
        ),
        _segment_partition_response(
            request_id=f"{request.request_id}:partition-02-of-03",
            evidence_id=evidence_id,
            metric_field="operating_cost",
            subject_scope="issuer",
        ),
        _segment_partition_response(
            request_id=f"{request.request_id}:partition-03-of-03",
            evidence_id=evidence_id,
            metric_field="gross_margin_reported",
        ),
    ]
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=outputs),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    assert "identity conflict: subject_scope" in str(exc_info.value)
    assert "merged segment partitions violate" not in str(exc_info.value)


def test_repeated_segment_rows_receive_unique_local_record_ids() -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    repeated_row = {
        "item_type": "segment_row",
        "row": {
            "label": "动力电池系统",
            "subject_scope": "unclear",
            "reported_period": "2025",
            "period_type": "duration",
            "evidence_ids": [evidence_id],
            "cells": {
                "operating_revenue": {
                    "value": "316,506,369",
                    "unit": "千元",
                    "header": "营业收入",
                },
                "operating_cost": {
                    "value": "241,064,397",
                    "unit": "千元",
                    "header": "营业成本",
                },
                "gross_margin_reported": {
                    "value": "23.84%",
                    "unit": "%",
                    "header": "毛利率",
                },
            },
        },
    }

    expanded = _expand_extract_response(
        {
            "schema_version": "company_profile_extract_response.v1",
            "request_id": request.request_id,
            "items": [repeated_row, repeated_row],
        },
        request=request,
        prepared_scope=prepared,
    )
    response = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    )

    candidates = response.candidates()
    assert len(candidates) == 8
    assert len({candidate.record_id for candidate in candidates}) == 8
    assert (
        candidates[0].semantic_content_fingerprint()
        == candidates[4].semantic_content_fingerprint()
    )


def test_segment_adjustment_row_does_not_inherit_business_segment_scope() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote="分产品 营业收入 营业成本 毛利率 合并抵消项 -1 -2 118.30%"
            )
        }
    )
    prepared = prepared.model_copy(
        update={
            "source_row_dimensions": {"合并抵消项": "分产品"},
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
        }
    )
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    expanded = _expand_extract_response(
        _segment_row_response(
            request_id=request.request_id,
            evidence_id=evidence_id,
            label="合并抵消项",
            row_class="consolidation_adjustment",
        ),
        request=request,
        prepared_scope=prepared,
    )
    candidates = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()
    assert candidates
    assert all(
        candidate.subject_scope == SubjectScope.CONSOLIDATED_GROUP
        for candidate in candidates
    )
    assert all(
        candidate.subject_basis == SubjectBasis.DIRECT_SOURCE_WORDING
        for candidate in candidates
    )


def test_inter_segment_elimination_keeps_unclear_subject_scope() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote="分行业 营业收入 营业成本 分部间抵消 -1 -2"
            )
        }
    )
    prepared = prepared.model_copy(
        update={
            "source_row_dimensions": {"分部间抵消": "分行业"},
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
        }
    )
    request = _segment_extract_request(prepared)
    expanded = _expand_extract_response(
        _segment_row_response(
            request_id=request.request_id,
            evidence_id=evidence.evidence_id,
            label="分部间抵消",
            row_class="consolidation_adjustment",
        ),
        request=request,
        prepared_scope=prepared,
    )
    candidates = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()

    assert candidates
    assert all(
        candidate.row_class.value == "consolidation_adjustment"
        for candidate in candidates
    )
    assert all(
        candidate.subject_scope == SubjectScope.UNCLEAR for candidate in candidates
    )
    assert all(candidate.subject_basis is None for candidate in candidates)


def test_plan_bound_sales_mode_dimension_uses_source_native_table_header() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote=(
                    "主营业务分销售模式情况\n"
                    "销售模式 营业收入 营业成本 毛利率\n"
                    "集中销售 100 80 20%"
                )
            )
        }
    )
    prepared = prepared.model_copy(
        update={
            "source_row_dimensions": {"集中销售": "销售模式"},
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
        }
    )
    request = _segment_extract_request(prepared)
    evidence_id = evidence.evidence_id

    expanded = _expand_extract_response(
        _segment_row_response(
            request_id=request.request_id,
            evidence_id=evidence_id,
            label="集中销售",
        ),
        request=request,
        prepared_scope=prepared,
    )
    candidates = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()

    assert candidates[0].dimension == "销售模式"
    assert candidates[0].source_native.header == "销售模式"
    assert candidates[1].segment_dimension == "销售模式"


def test_same_page_totals_in_different_dimensions_are_distinct_occurrences() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote=(
                    "分产品 营业收入 营业成本 毛利率 合计 100 80 20% "
                    "分地区 营业收入 营业成本 毛利率 合计 100 80 20%"
                )
            )
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
        }
    )
    request = _segment_extract_request(prepared)

    records = []
    for dimension in ("分产品", "分地区"):
        response = _segment_row_response(
            request_id=request.request_id,
            evidence_id=evidence.evidence_id,
            dimension=dimension,
            label="合计",
        )
        expanded = ExtractResponse.model_validate_json(
            json.dumps(
                _expand_extract_response(
                    response,
                    request=request,
                    prepared_scope=prepared,
                ),
                ensure_ascii=False,
            )
        )
        records.append(
            next(
                item
                for item in expanded.candidates()
                if item.field_id == "operating_revenue"
            )
        )

    assert records[0].evidence[0].anchor.table_label == "分产品"
    assert records[1].evidence[0].anchor.table_label == "分地区"
    assert records[0].occurrence_id() != records[1].occurrence_id()


def test_same_control_comparison_uses_measurement_contract_not_segment_rows() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "same_control_comparison_basis",
            "chapter_task": ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "field_ids": ("operating_revenue",),
        }
    )
    checklist = ChecklistItem(
        field_id="operating_revenue",
        object_type=ObjectType.MEASUREMENT,
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(MetricType.OPERATING_REVENUE,),
    )
    request = SemanticTaskRequest(
        request_id="same-control-comparison-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(MetricType.OPERATING_REVENUE,),
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    expanded = _expand_extract_response(
        {
            "measurements": [
                {
                    "metric_type": "operating_revenue",
                    "name": "营业收入",
                    "value": "65047476349.46",
                    "unit": "元",
                    "header": "调整后",
                    "reported_period": "2024年度",
                    "is_restated_comparative": True,
                    "comparison_basis": "same_control_restated",
                    "evidence_id": evidence_id,
                }
            ],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )

    schema = _minimal_extract_schema(request, prepared_scope=prepared)
    assert set(schema["properties"]) == {"measurements", "coverage"}
    candidate = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()[0]
    assert candidate.object_type == "Measurement"
    assert candidate.is_restated_comparative is True
    assert candidate.comparison_basis.value == "same_control_restated"
    assert candidate.reported_period == "2024年度"
    assert candidate.knowledge_time == prepared.report.published_at


def test_same_control_adjusted_and_pre_adjustment_columns_keep_distinct_period_basis() -> (
    None
):
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "same_control_comparison_basis",
            "chapter_task": ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "field_ids": ("operating_revenue",),
        }
    )
    checklist = ChecklistItem(
        field_id="operating_revenue",
        object_type=ObjectType.MEASUREMENT,
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(MetricType.OPERATING_REVENUE,),
    )
    request = SemanticTaskRequest(
        request_id="same-control-distinct-columns-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(MetricType.OPERATING_REVENUE,),
        unresolved_field_ids=prepared.field_ids,
    )
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    expanded = _expand_extract_response(
        {
            "measurements": [
                {
                    "metric_type": "operating_revenue",
                    "name": "营业收入",
                    "value": "65054925106.17",
                    "unit": "元",
                    "header": "2024年 调整后",
                    "reported_period": "2024年度",
                    "is_restated_comparative": True,
                    "comparison_basis": "same_control_restated",
                    "evidence_id": evidence_id,
                },
                {
                    "metric_type": "operating_revenue",
                    "name": "营业收入",
                    "value": "1779761710.30",
                    "unit": "元",
                    "header": "2024年 调整前",
                    "reported_period": "2024年度",
                    "is_restated_comparative": False,
                    "comparison_basis": "original_as_published",
                    "evidence_id": evidence_id,
                },
            ],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )
    records = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()

    assert len(records) == 2
    assert {item.reported_period for item in records} == {"2024年度"}
    assert {item.comparison_basis for item in records} == {
        ComparisonBasis.SAME_CONTROL_RESTATED,
        ComparisonBasis.ORIGINAL_AS_PUBLISHED,
    }
    assert {item.knowledge_time for item in records} == {prepared.report.published_at}
    assert records[0].occurrence_id() != records[1].occurrence_id()


def test_adapter_period_semantics_distinguish_duration_instant_and_narrower_periods() -> (
    None
):
    prepared = _prepared_scope()

    assert (
        _normalize_adapter_reported_period(
            "2025-12-31",
            period_type="duration",
            prepared_scope=prepared,
        )
        == "2025"
    )
    assert (
        _normalize_adapter_reported_period(
            "2025-12-31",
            period_type="instant",
            prepared_scope=prepared,
        )
        == "2025-12-31"
    )
    assert (
        _normalize_adapter_reported_period(
            "2025年1-6月",
            period_type="duration",
            prepared_scope=prepared,
        )
        == "2025年1-6月"
    )


def test_segment_partition_period_semantics_accepts_only_closed_annual_aliases() -> (
    None
):
    prepared = _prepared_scope()

    assert _normalize_adapter_reported_period(
        "2025年", period_type="duration", prepared_scope=prepared
    ) == "2025年"
    assert _normalize_segment_partition_reported_period(
        "2025年", period_type="duration", prepared_scope=prepared
    ) == "2025"
    assert _normalize_segment_partition_reported_period(
        "2025年度", period_type="duration", prepared_scope=prepared
    ) == "2025"
    assert _normalize_segment_partition_reported_period(
        "2025年度", period_type="instant", prepared_scope=prepared
    ) == "2025年度"
    assert _normalize_segment_partition_reported_period(
        "2024年度", period_type="duration", prepared_scope=prepared
    ) == "2024年度"


def test_segment_dimension_options_preserve_complete_report_segment_headings() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "segment_financials-01",
            "chapter_task": ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "field_ids": (
                "segment_dimension",
                "operating_revenue",
                "operating_cost",
                "gross_margin_reported",
            ),
            "evidence_bundle": (
                _prepared_scope().evidence_bundle[0].model_copy(
                    update={
                        "evidence": _prepared_scope()
                        .evidence_bundle[0]
                        .evidence.model_copy(
                            update={
                                "anchor": TextAnchor(
                                    bounded_quote=(
                                        "6、分部信息\n"
                                        "(1).报告分部的确定依据与会计政策\n"
                                        "(2).报告分部的财务信息"
                                    )
                                )
                            }
                        )
                    }
                ),
            )
        }
    )

    options = _segment_dimension_options(prepared)
    schema = _minimal_extract_schema(
        _segment_extract_request(prepared), prepared_scope=prepared
    )
    row_schema = schema["properties"]["items"]["items"]["oneOf"][0][
        "properties"
    ]["row"]

    assert "报告分部的财务信息" in options
    assert "报告分部的确定依据与会计政策" not in options
    assert "报告分部" not in options
    assert _unique_segment_scope_heading(prepared) == "报告分部的财务信息"
    assert "dimension" not in row_schema["properties"]
    assert "dimension" not in row_schema["required"]


@pytest.mark.parametrize(
    ("source_text", "expected_options"),
    [
        (
            "分产品 动力电池系统 100 80 分地区 境内 90 70",
            {"分产品", "分地区"},
        ),
        ("业务板块 动力电池系统 100 80", set()),
    ],
)
def test_ambiguous_or_missing_segment_heading_remains_provider_owned(
    source_text: str,
    expected_options: set[str],
) -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"anchor": TextAnchor(bounded_quote=source_text)}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
            "page_contexts": (
                prepared.page_contexts[0].model_copy(update={"text": source_text}),
            ),
        }
    )

    schema = _minimal_extract_schema(
        _segment_extract_request(prepared), prepared_scope=prepared
    )
    row_schema = schema["properties"]["items"]["items"]["oneOf"][0][
        "properties"
    ]["row"]

    assert _unique_segment_scope_heading(prepared) is None
    assert set(_segment_dimension_options(prepared)) == expected_options
    assert "dimension" in row_schema["properties"]
    assert "dimension" in row_schema["required"]


def test_unique_segment_heading_expands_dimension_free_failure_shape() -> None:
    prepared = _segment_prepared_scope()
    source_text = (
        "6、分部信息\n"
        "（1）报告分部的确定依据与会计政策\n"
        "（2）报告分部的财务信息\n"
        "项目 动力电池系统\n"
        "营业收入 316,506,369\n"
        "营业成本 241,064,397\n"
        "毛利率 23.84%"
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"anchor": TextAnchor(bounded_quote=source_text)}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
            "page_contexts": (
                prepared.page_contexts[0].model_copy(update={"text": source_text}),
            ),
        }
    )
    request = _segment_extract_request(prepared)

    expanded = _normalize_extract_response(
        _segment_row_response(
            request_id=request.request_id,
            evidence_id=evidence.evidence_id,
        ),
        request=request,
        prepared_scope=prepared,
    )

    candidates = [item["candidate"] for item in expanded["items"]]
    assert [item["field_id"] for item in candidates] == [
        "segment_dimension",
        "operating_revenue",
    ]
    assert all(
        item.get("dimension", item.get("segment_dimension"))
        == "报告分部的财务信息"
        for item in candidates
    )
    assert candidates[0]["source_native"]["header"] == "报告分部的财务信息"
    assert candidates[1]["evidence"][0]["anchor"]["table_label"] == (
        "报告分部的财务信息"
    )


def test_unique_segment_heading_rejects_provider_dimension_prefix() -> None:
    prepared = _segment_prepared_scope()
    source_text = "报告分部的财务信息 动力电池系统 316,506,369"
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"anchor": TextAnchor(bounded_quote=source_text)}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
        }
    )
    request = _segment_extract_request(prepared)

    with pytest.raises(ValueError, match="controlled Evidence binds it locally"):
        _normalize_extract_response(
            _segment_row_response(
                request_id=request.request_id,
                evidence_id=evidence.evidence_id,
                dimension="报告分部",
            ),
            request=request,
            prepared_scope=prepared,
        )


def test_business_event_keeps_occurrence_effective_and_knowledge_time_separate() -> (
    None
):
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "restructuring_commitment",
            "chapter_task": ChapterTask.EXTRACT_BUSINESS_REGIME,
            "field_ids": ("business_regime",),
        }
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    expanded = _expand_extract_response(
        {
            "events": [
                {
                    "event_type": "equity_transfer",
                    "description": "公司已完成股权过户并纳入合并报表范围",
                    "reported_period": "2025",
                    "event_date": "2025-01-06",
                    "regime_effective_at": "2025-01-06",
                    "comparison_basis": "current_period_after_restructuring",
                    "evidence_id": evidence_id,
                }
            ],
            "regimes": [],
            "package_assignments": [],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )
    event = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()[0]

    assert event.reported_period == "2025"
    assert event.event_date == "2025-01-06"
    assert event.regime_effective_at == "2025-01-06"
    assert event.knowledge_time == prepared.report.published_at


def test_inventory_measurement_uses_report_date_as_instant() -> None:
    prepared = _prepared_scope()
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    expanded = _expand_compact_measurements(
        [
            {
                "metric_type": "inventory_volume",
                "name": "负极材料库存量",
                "value": "39299.86",
                "unit": "吨",
                "header": "库存量",
                "evidence_id": evidence_id,
            }
        ],
        field_id_for_metric=lambda metric_type: metric_type,
        prepared_scope=prepared,
    )
    candidate = expanded[0]["candidate"]

    assert candidate["period_type"] == "instant"
    assert candidate["reported_period"] == "2025-12-31"


def test_capacity_completion_qualifier_is_source_native_and_does_not_replace_period() -> (
    None
):
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "capacity_project_narrative",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("production_capacity", "capacity_under_construction"),
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=_prepared_scope()
                    .evidence_bundle[0]
                    .evidence.model_copy(update={"continuation_pages": (50,)})
                ),
            ),
        }
    )
    request = _capacity_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    expanded = _expand_extract_response(
        {
            "measurements": [
                {
                    "metric_type": "capacity_under_construction",
                    "name": "羟胺盐在建产能",
                    "value": "40000",
                    "unit": "吨/年",
                    "qualifier": "预计2026年完工",
                    "reported_period": "2025-12-31",
                    "evidence_id": evidence_id,
                }
            ],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )
    measurement = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()[0]

    assert measurement.reported_period == "2025"
    assert measurement.period_type == PeriodType.DURATION
    assert measurement.source_native.qualifier == "预计2026年完工"
    assert measurement.evidence[0].page == 14
    assert measurement.evidence[0].continuation_pages == (50,)


def test_capacity_completion_qualifier_remains_absent_when_source_omits_it() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "capacity_project_narrative",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("production_capacity", "capacity_under_construction"),
        }
    )
    request = _capacity_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    expanded = _expand_extract_response(
        {
            "measurements": [
                {
                    "metric_type": "capacity_under_construction",
                    "name": "羟胺盐在建产能",
                    "value": "40000",
                    "unit": "吨/年",
                    "evidence_id": evidence_id,
                }
            ],
            "coverage": [],
        },
        request=request,
        prepared_scope=prepared,
    )
    measurement = ExtractResponse.model_validate_json(
        json.dumps(expanded, ensure_ascii=False)
    ).candidates()[0]

    assert measurement.reported_period == "2025"
    assert measurement.source_native.qualifier is None


def test_same_control_compact_schema_requires_basis_for_every_column() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "same_control_comparison_basis",
            "chapter_task": ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "field_ids": ("operating_revenue",),
        }
    )
    request = _same_control_request(prepared)
    schema = _minimal_extract_schema(request, prepared_scope=prepared)
    item_schema = schema["properties"]["measurements"]["items"]
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    base = {
        "metric_type": "operating_revenue",
        "name": "营业收入",
        "value": "100",
        "unit": "元",
        "reported_period": "2025年",
        "evidence_id": evidence_id,
    }
    validator = Draft202012Validator(item_schema)

    assert not validator.is_valid(base)
    assert validator.is_valid(
        {
            **base,
            "comparison_basis": "current_period_after_restructuring",
            "is_restated_comparative": False,
        }
    )


def test_measurement_schema_scopes_capacity_kind_to_metric_type() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "capacity_narrative",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("production_capacity", "capacity_under_construction"),
        }
    )
    request = _capacity_extract_request(prepared)

    schema = _minimal_extract_schema(request, prepared_scope=prepared)
    item_schema = schema["properties"]["measurements"]["items"]
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    base = {
        "name": "产能",
        "value": "10",
        "unit": "GWh",
        "evidence_id": evidence_id,
    }
    validator = Draft202012Validator(item_schema)

    assert validator.is_valid(
        {
            **base,
            "metric_type": "production_capacity",
            "capacity_kind": "report_period_capacity",
        }
    )
    assert not validator.is_valid({**base, "metric_type": "production_capacity"})
    assert validator.is_valid({**base, "metric_type": "capacity_under_construction"})
    assert not validator.is_valid(
        {
            **base,
            "metric_type": "capacity_under_construction",
            "capacity_kind": "design_capacity",
        }
    )


def test_coverage_schema_requires_typed_reason_for_not_disclosed() -> None:
    schema = _coverage_draft_schema(
        field_ids=["counterparty_relationship"],
        statuses=["not_disclosed", "not_applicable"],
    )
    branches = schema["oneOf"]
    not_disclosed = next(
        item
        for item in branches
        if item["properties"]["status"] == {"const": "not_disclosed"}
    )
    not_applicable = next(
        item
        for item in branches
        if item["properties"]["status"] == {"const": "not_applicable"}
    )

    assert "reason_code" in not_disclosed["required"]
    assert not_disclosed["properties"]["reason_code"]["enum"] == [
        "explicit_confidentiality",
        "explicit_disclosure_exemption",
        "source_reason_unspecified",
    ]
    assert "reason_code" in not_applicable["required"]


def test_quantity_disclosure_check_uses_coverage_only_schema() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "quantity_disclosure_check",
            "chapter_task": ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "field_ids": ("production_volume", "sales_volume", "inventory_volume"),
        }
    )
    statuses = tuple(CoverageStatus)
    checklist = tuple(
        ChecklistItem(
            field_id=field_id,
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=statuses,
            allowed_metric_types=(metric_type,),
        )
        for field_id, metric_type in (
            ("production_volume", MetricType.PRODUCTION_VOLUME),
            ("sales_volume", MetricType.SALES_VOLUME),
            ("inventory_volume", MetricType.INVENTORY_VOLUME),
        )
    )
    request = SemanticTaskRequest(
        request_id="slice-1:quantity-disclosure",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=checklist,
        ),
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(
            MetricType.PRODUCTION_VOLUME,
            MetricType.SALES_VOLUME,
            MetricType.INVENTORY_VOLUME,
        ),
        unresolved_field_ids=prepared.field_ids,
    )
    schema = _minimal_extract_schema(request, prepared_scope=prepared)
    item = schema["properties"]["items"]["items"]
    assert item["properties"]["item_type"] == {"const": "coverage"}
    coverage = item["properties"]["coverage"]
    assert coverage["oneOf"]
    assert all(
        "candidate" not in branch.get("properties", {}) for branch in coverage["oneOf"]
    )


def test_numeric_reconciliation_requires_non_empty_uncertainty() -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=prepared.evidence_bundle[0].evidence.evidence_id,
    )
    row = compact["items"][0]["row"]
    row["subject_scope"] = "consolidated_group"
    row["subject_basis"] = "numeric_reconciliation_to_consolidated_statement"
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[compact]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    assert "non-empty uncertainty" in (provider.traces[0].error_detail or "")


def test_consolidated_segment_subject_requires_affirmative_basis() -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=prepared.evidence_bundle[0].evidence.evidence_id,
    )
    compact["items"][0]["row"]["subject_scope"] = "consolidated_group"
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[compact]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    assert "subject_basis" in (provider.traces[0].error_detail or "")


@pytest.mark.parametrize("dimension", ["分业务", "产品"])
def test_segment_financials_reject_semantic_dimension_rewrite(
    dimension: str,
) -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=evidence_id,
        dimension=dimension,
    )
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[compact]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.CANDIDATE_SCHEMA_INVALID
    assert provider.traces[0].error_code == "candidate_schema_invalid"
    assert provider.traces[0].gateway_request_id == "gateway-1"
    assert "dimension" in (provider.traces[0].error_detail or "")


def test_segment_financials_report_the_rejected_source_label() -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=prepared.evidence_bundle[0].evidence.evidence_id,
        label="其他业务",
    )
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[compact]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError):
        provider.extract(request)

    assert "其他业务" in (provider.traces[0].error_detail or "")


def test_segment_dimension_heading_can_precede_cited_continuation_row() -> None:
    prepared = _segment_prepared_scope()
    base = prepared.evidence_bundle[0].evidence
    heading = base.model_copy(
        update={
            "evidence_id": "segment-heading-evidence",
            "page": 25,
            "anchor": TextAnchor(
                bounded_quote="分产品 营业收入 营业成本 毛利率 单位：千元"
            ),
        }
    )
    row_evidence = base.model_copy(
        update={
            "evidence_id": "segment-continuation-row-evidence",
            "page": 26,
            "anchor": TextAnchor(
                bounded_quote="续表 动力电池系统 316,506,369 241,064,397 23.84%"
            ),
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=heading),
                PreparedEvidence(evidence=row_evidence),
            ),
            "source_row_dimensions": {},
            "candidate_pages": (25, 26),
            "page_contexts": (
                PreparedPageContext(
                    page=25,
                    text=heading.anchor.bounded_quote,
                    text_hash="1" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
                PreparedPageContext(
                    page=26,
                    text=row_evidence.anchor.bounded_quote,
                    text_hash="2" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )
    request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=row_evidence.evidence_id,
    )

    expanded = _normalize_extract_response(
        compact,
        request=request,
        prepared_scope=prepared,
    )

    candidates = [item["candidate"] for item in expanded["items"]]
    assert {item["evidence_id"] for item in candidates[0]["evidence"]} == {
        heading.evidence_id,
        row_evidence.evidence_id,
    }
    evidence_by_id = {
        item["evidence_id"]: item["anchor"] for item in candidates[1]["evidence"]
    }
    assert evidence_by_id[heading.evidence_id]["anchor_type"] == "text"
    assert evidence_by_id[row_evidence.evidence_id] == {
        "anchor_type": "table",
        "table_label": "分产品",
        "row_label": "动力电池系统",
        "column_header": "营业收入",
        "cell_locator": None,
    }


def test_consolidation_adjustment_normalizes_internal_dimension() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote=(
                    "报告分部的财务信息 "
                    "分部间抵销 -61,069,781 -60,974,432 0.16%"
                )
            )
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
        }
    )
    request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=evidence.evidence_id,
        label="分部间抵销",
        row_class="consolidation_adjustment",
    )

    expanded = _expand_extract_response(
        compact,
        request=request,
        prepared_scope=prepared,
    )

    assert expanded["items"][0]["candidate"]["dimension"] == "adjustment"
    assert expanded["items"][0]["candidate"]["row_class"] == "consolidation_adjustment"


def test_unique_segment_heading_merges_dimension_free_partitions() -> None:
    prepared = _high_cardinality_segment_prepared_scope()
    source_text = prepared.page_contexts[0].text.replace(
        "分产品", "报告分部的财务信息"
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"anchor": TextAnchor(bounded_quote=source_text)}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
            "page_contexts": (
                prepared.page_contexts[0].model_copy(update={"text": source_text}),
            ),
        }
    )
    request = _segment_extract_request(prepared)
    partitions = tuple(
        (
            partition_request,
            _segment_partition_response(
                request_id=partition_request.request_id,
                evidence_id=evidence.evidence_id,
                metric_field=metric,
            ),
        )
        for index, metric in enumerate(
            ("operating_revenue", "operating_cost"),
            start=1,
        )
        for partition_request in (
            _segment_partition_request(
                request,
                fields=("segment_dimension", metric),
                partition_index=index,
                partition_count=2,
            ),
        )
    )

    merged = _merge_segment_partition_responses(
        request,
        partitions,
        prepared_scope=prepared,
    )
    expanded = _normalize_extract_response(
        merged,
        request=request,
        prepared_scope=prepared,
    )

    assert "dimension" not in merged["items"][0]["row"]
    candidates = [item["candidate"] for item in expanded["items"]]
    assert [item["field_id"] for item in candidates] == [
        "segment_dimension",
        "operating_revenue",
        "operating_cost",
    ]
    assert candidates[0]["dimension"] == "报告分部的财务信息"


def test_explicit_source_row_dimensions_remain_authoritative() -> None:
    prepared = _segment_prepared_scope()
    request = _segment_extract_request(prepared)
    schema = _minimal_extract_schema(request, prepared_scope=prepared)
    row_schema = schema["properties"]["items"]["items"]["oneOf"][0][
        "properties"
    ]["row"]

    assert _unique_segment_scope_heading(prepared) is None
    assert "dimension" not in row_schema["properties"]
    assert row_schema["properties"]["label"] == {"enum": ["动力电池系统"]}

    expanded = _normalize_extract_response(
        _segment_row_response(
            request_id=request.request_id,
            evidence_id=prepared.evidence_bundle[0].evidence.evidence_id,
        ),
        request=request,
        prepared_scope=prepared,
    )
    assert expanded["items"][0]["candidate"]["dimension"] == "分产品"


def test_consolidation_adjustment_requires_explicit_adjustment_label() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(bounded_quote="分产品 新能源电池材料与服务 100 80 20%")
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
        }
    )
    request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=request.request_id,
        evidence_id=evidence.evidence_id,
        label="新能源电池材料与服务",
        row_class="consolidation_adjustment",
    )

    with pytest.raises(ValueError, match="explicitly identify an adjustment"):
        _expand_extract_response(
            compact,
            request=request,
            prepared_scope=prepared,
        )


def test_verify_accepts_explicit_consolidation_adjustment_subject() -> None:
    prepared, request = _consolidation_adjustment_verify_request()
    client = _FakeGatewayClient(
        outputs=[
            VerifyResponse(
                request_id=request.request_id,
                checks=tuple(
                    VerifyCheck(
                        target_type="candidate",
                        target_id=candidate.record_id,
                        status=VerifyStatus.BLOCK,
                        reason_codes=(ContractErrorCode.SUBJECT_UNSUPPORTED,),
                    )
                    for candidate in request.candidates
                ),
            ).model_dump(mode="json")
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.verify(request)

    assert {item["status"] for item in response["checks"]} == {"pass"}
    assert {tuple(item["reason_codes"]) for item in response["checks"]} == {()}
    assert {candidate.object_type for candidate in request.candidates} == {
        "Segment",
        "Measurement",
    }
    system_message = LlmMessage.from_value(client.requests[0].messages[0]).content
    assert "affirmative group wording such as 合并抵消项" in system_message
    assert "do not return subject_unsupported solely" in system_message


def test_verify_does_not_normalize_ordinary_segment_subject_block() -> None:
    prepared = _segment_prepared_scope()
    extract_request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=extract_request.request_id,
        evidence_id=prepared.evidence_bundle[0].evidence.evidence_id,
    )
    candidates = ExtractResponse.model_validate_json(
        json.dumps(
            _expand_extract_response(
                compact,
                request=extract_request,
                prepared_scope=prepared,
            ),
            ensure_ascii=False,
        )
    ).candidates()
    request = VerifyRequest(
        request_id=f"{extract_request.request_id}:verify",
        original_request_id=extract_request.request_id,
        report=prepared.report,
        evidence_bundle=prepared.evidence_bundle,
        candidates=candidates,
        coverage=(),
    )
    blocked = VerifyResponse(
        request_id=request.request_id,
        checks=tuple(
            VerifyCheck(
                target_type="candidate",
                target_id=candidate.record_id,
                status=VerifyStatus.BLOCK,
                reason_codes=(ContractErrorCode.SUBJECT_UNSUPPORTED,),
            )
            for candidate in request.candidates
        ),
    ).model_dump(mode="json")
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[blocked]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.verify(request)

    assert {item["status"] for item in response["checks"]} == {"block"}
    assert {tuple(item["reason_codes"]) for item in response["checks"]} == {
        (ContractErrorCode.SUBJECT_UNSUPPORTED.value,)
    }


def test_verify_keeps_additional_adjustment_failure_blocking() -> None:
    prepared, request = _consolidation_adjustment_verify_request()
    blocked = VerifyResponse(
        request_id=request.request_id,
        checks=tuple(
            VerifyCheck(
                target_type="candidate",
                target_id=candidate.record_id,
                status=VerifyStatus.BLOCK,
                reason_codes=(
                    ContractErrorCode.SUBJECT_UNSUPPORTED,
                    ContractErrorCode.EVIDENCE_FIELD_MISMATCH,
                ),
            )
            for candidate in request.candidates
        ),
    ).model_dump(mode="json")
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[blocked]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    response = provider.verify(request)

    assert {item["status"] for item in response["checks"]} == {"block"}
    assert all(
        ContractErrorCode.EVIDENCE_FIELD_MISMATCH.value in item["reason_codes"]
        for item in response["checks"]
    )


def test_provider_expands_compact_report_and_evidence_references() -> None:
    prepared = _prepared_scope()
    request = _extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
    compact = {
        "schema_version": "company_profile_extract_response.v1",
        "request_id": "x",
        "items": [
            {
                "item_type": "candidate",
                "candidate": {
                    "object_type": "BusinessOverview",
                    "field_id": "business_overview_source",
                    "subject_scope": "unclear",
                    "reported_period": "2025",
                    "period_type": "duration",
                    "source_native": {"name": "主要业务"},
                    "evidence_ids": [evidence_id],
                    "source_text": "公司主要从事动力电池研发、生产和销售。",
                },
            }
        ],
    }
    expanded = _expand_extract_response(
        compact,
        request=request,
        prepared_scope=prepared,
    )
    candidate = expanded["items"][0]["candidate"]
    assert candidate["report"] == prepared.report.model_dump(mode="json")
    assert candidate["evidence"][0] == prepared.evidence_bundle[0].evidence.model_dump(
        mode="json"
    )
    assert candidate["chapter_task"] == "extract_business_overview"
    assert candidate["assertion_class"] == "reported_fact"
    assert candidate["data_status"] == "research_fixture"
    assert candidate["record_id"].startswith("stage5-")


def test_common_gateway_provider_uses_separate_repair_and_verify_requests() -> None:
    prepared = _prepared_scope()
    candidate = _overview_candidate(prepared)
    repair_request = RepairRequest(
        request_id="slice-1:repair:overview-1",
        original_request_id="slice-1:business_overview",
        original_candidate=candidate,
        error_code=ContractErrorCode.SUBJECT_UNSUPPORTED,
        writable_fields=("/subject_scope",),
        evidence_bundle=prepared.evidence_bundle,
    )
    verify_request = VerifyRequest(
        request_id="slice-1:business_overview:verify",
        original_request_id="slice-1:business_overview",
        report=prepared.report,
        evidence_bundle=prepared.evidence_bundle,
        candidates=(candidate,),
        coverage=(),
    )
    client = _FakeGatewayClient(
        outputs=[
            {
                "schema_version": "company_profile_repair_response.v1",
                "request_id": repair_request.request_id,
                "updates": {"subject_scope": "issuer"},
                "changed_fields": ["/subject_scope"],
            },
            {
                "schema_version": "company_profile_verify_response.v1",
                "request_id": verify_request.request_id,
                "checks": [
                    {
                        "target_type": "candidate",
                        "target_id": candidate.record_id,
                        "status": "pass",
                        "reason_codes": [],
                    }
                ],
            },
        ]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        verify_max_output_tokens=1200,
        timeout_seconds=30,
    )

    repaired = provider.repair(repair_request)
    provider.verify(verify_request)
    assert repaired["candidate"]["subject_scope"] == "issuer"

    assert [request.schema_name for request in client.requests] == [
        "company_profile_repair_response",
        "company_profile_verify_response",
    ]
    assert [request.max_output_tokens for request in client.requests] == [2000, 1200]
    assert [trace.call_type for trace in provider.traces] == ["repair", "verify"]
    assert client.requests[0].idempotency_key != client.requests[1].idempotency_key
    repair_schema = client.requests[0].response_schema
    assert "updates" in repair_schema["properties"]
    assert "candidate" not in repair_schema["properties"]
    repair_envelope = json.loads(
        LlmMessage.from_value(client.requests[0].messages[1]).content
    )
    assert "report" not in repair_envelope["runtime_request"]["original_candidate"]
    assert "evidence" not in repair_envelope["runtime_request"]["original_candidate"]
    verify_envelope = json.loads(
        LlmMessage.from_value(client.requests[1].messages[1]).content
    )
    assert len(verify_envelope["runtime_request"]["evidence_catalog"]) == 1
    verify_candidate = verify_envelope["runtime_request"]["candidates"][0]
    verify_schema = _minimal_verify_schema(verify_request)
    checks_schema = verify_schema["properties"]["checks"]
    assert verify_schema["properties"]["request_id"] == {
        "const": verify_request.request_id
    }
    assert checks_schema["minItems"] == checks_schema["maxItems"] == 1
    assert checks_schema["items"] is False
    assert checks_schema["prefixItems"][0]["properties"]["target_id"] == {
        "const": candidate.record_id
    }
    verify_instruction = LlmMessage.from_value(client.requests[1].messages[0]).content
    assert "exactly one check for each target" in verify_instruction
    assert (
        "legal-empty coverage target must not be marked unclear" in verify_instruction
    )
    assert "evidence_catalog field_ids list is the authoritative field binding" in (
        verify_instruction
    )
    assert "one Evidence item may be bound to several field_ids" in verify_instruction
    assert "source_value_mutation as applicable only when" in verify_instruction
    assert (
        "does not normally need comparison_basis merely because" in verify_instruction
    )
    assert "same_control_comparison_basis scope" in verify_instruction
    assert "activity_actor_unsupported reason applies only to Activity" in (
        verify_instruction
    )
    assert "candidate with subject_scope=unclear may pass" in verify_instruction
    assert "must not be translated" in verify_instruction
    assert "subject_scope is business_segment is supported" in verify_instruction
    assert "report" not in verify_candidate and "evidence" not in verify_candidate


def test_common_gateway_provider_maps_gateway_failures_to_typed_workflow_outcome() -> (
    None
):
    prepared = _prepared_scope()
    request = _extract_request(prepared)
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(outputs=[LlmRateLimitError("gateway is congested")]),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.provider_calls == ("extract",)
    assert result.task_complete is False
    assert result.human_review_items[0].reason_codes == (
        ContractErrorCode.PROVIDER_UNAVAILABLE,
    )
    assert provider.traces[0].error_code == "provider_unavailable"


def test_common_gateway_provider_preserves_deadline_failure_identity() -> None:
    prepared = _prepared_scope()
    request = _extract_request(prepared)
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(
            outputs=[
                LlmDeadlineExceededError().with_context(
                    request_id="gateway-deadline-1",
                    attempt_count=1,
                )
            ]
        ),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    result = CompanyProfileSemanticService().run_task(request, provider=provider)

    assert result.human_review_items[0].reason_codes == (
        ContractErrorCode.DEADLINE_EXCEEDED,
    )
    assert provider.traces[0].error_code == "deadline_exceeded"
    assert provider.traces[0].gateway_request_id == "gateway-deadline-1"


def test_common_gateway_provider_rejects_response_identity_mismatch() -> None:
    prepared = _prepared_scope()
    request = _extract_request(prepared)
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(
            outputs=[
                {
                    "schema_version": "company_profile_extract_response.v1",
                    "request_id": "wrong-request",
                    "items": [],
                }
            ]
        ),
        profile="semantic_extraction",
        prepared_scope=prepared,
        max_output_tokens=2000,
        timeout_seconds=30,
    )

    with pytest.raises(SemanticProviderError) as exc_info:
        provider.extract(request)

    assert exc_info.value.code == ContractErrorCode.REQUEST_IDENTITY_MISMATCH
    assert provider.traces[0].error_code == "request_identity_mismatch"


def test_business_overview_accepts_source_native_contiguous_excerpt() -> None:
    prepared = _prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote="公司是全球领先的零碳新能源科技公司，主要从事动力电池、储能电池的研发、生产、销售。"
            )
        }
    )
    candidate = BusinessOverview(
        record_id="overview-substring",
        field_id="business_overview_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=prepared.report,
        subject_scope=SubjectScope.UNCLEAR,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="主要业务"),
        source_text="主要从事动力电池、储能电池的研发、生产、销售。",
    )
    assert candidate.source_text in evidence.anchor.bounded_quote


def test_business_overview_rejects_text_not_present_in_evidence() -> None:
    prepared = _prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(bounded_quote="公司主要从事动力电池研发、生产和销售。")
        }
    )
    with pytest.raises(ValueError, match="must match text evidence"):
        BusinessOverview(
            record_id="overview-fabricated",
            field_id="business_overview_source",
            chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            report=prepared.report,
            subject_scope=SubjectScope.UNCLEAR,
            reported_period="2025",
            period_type=PeriodType.DURATION,
            assertion_class=AssertionClass.REPORTED_FACT,
            evidence=(evidence,),
            source_native=SourceNativeValue(name="主要业务"),
            source_text="公司还经营芯片设计业务。",
        )


def test_business_overview_rejects_cross_reference_only_target() -> None:
    prepared = _prepared_scope()
    pointer = (
        "参见第三节“管理层讨论与分析”中“一、报告期内公司从事的主要业务”"
        "的相关内容。"
    )
    substantive = "公司的商业开发项目主要采用自行开发，部分销售、部分自持的经营模式。"
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"anchor": TextAnchor(bounded_quote=f"{substantive}\n{pointer}")}
    )

    with pytest.raises(ValueError, match="substantive business text"):
        BusinessOverview(
            record_id="overview-pointer-only",
            field_id="business_overview_source",
            chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
            report=prepared.report,
            subject_scope=SubjectScope.UNCLEAR,
            reported_period="2025",
            period_type=PeriodType.DURATION,
            assertion_class=AssertionClass.REPORTED_FACT,
            evidence=(evidence,),
            source_native=SourceNativeValue(name="主营业务概述"),
            source_text=pointer,
        )

    accepted = BusinessOverview(
        record_id="overview-substantive-same-evidence",
        field_id="business_overview_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=prepared.report,
        subject_scope=SubjectScope.UNCLEAR,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="主要经营模式"),
        source_text=substantive,
    )
    assert accepted.source_text == substantive


def _prepared_scope() -> PreparedRequestScope:
    report = ReportIdentity(
        instrument_id="300750.SZ",
        report_id="asset_3b09f6c831975c7177b6bb3287cab781",
        document_version="ver_09c0e677ec8192dc4fc12cb620069f29",
        report_period="2025-12-31",
        published_at="2026-03-09T16:00:00+00:00",
    )
    evidence = Evidence(
        evidence_id="stage5-provider-evidence",
        report=report,
        page=14,
        section_title="主要业务",
        anchor=TextAnchor(bounded_quote="公司主要从事动力电池研发、生产和销售。"),
    )
    return PreparedRequestScope(
        sample_id="manufacturing-materials-300750-2025",
        scope_id="business_overview",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        field_ids=("business_overview_source",),
        report=report,
        evidence_bundle=(PreparedEvidence(evidence=evidence),),
        page_contexts=(
            PreparedPageContext(
                page=14,
                text="公司主要从事动力电池研发、生产和销售。",
                text_hash="a" * 64,
                extraction_method="pypdf",
                quality_status="usable",
            ),
        ),
        plan_version="manufacturing_materials.2026-09-04.1",
    )


def _reported_business_change_scope() -> PreparedRequestScope:
    prepared = _prepared_scope()
    text = (
        "（6）报告期内合并范围是否发生变动：是。"
        "（7）公司报告期内业务、产品或服务发生重大变化或调整有关情况："
        "□适用  ☑不适用。"
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "page": 27,
            "section_title": "业务、产品或服务重大变化 / 合并范围变化",
            "anchor": TextAnchor(bounded_quote=text),
        }
    )
    return prepared.model_copy(
        update={
            "scope_id": "reported_business_change",
            "chapter_task": ChapterTask.EXTRACT_BUSINESS_REGIME,
            "field_ids": ("business_regime",),
            "evidence_bundle": (
                PreparedEvidence(
                    evidence=evidence,
                    field_id="business_regime",
                ),
            ),
            "page_contexts": (
                PreparedPageContext(
                    page=27,
                    text=text,
                    text_hash="c" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )


def _segment_prepared_scope() -> PreparedRequestScope:
    prepared = _prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "page": 25,
            "section_title": "收入与成本",
            "anchor": TextAnchor(
                bounded_quote=("分产品 动力电池系统 316,506,369 241,064,397 23.84%")
            ),
        }
    )
    return prepared.model_copy(
        update={
            "scope_id": "segment_product_industry_region",
            "chapter_task": ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "field_ids": (
                "segment_dimension",
                "operating_revenue",
                "operating_cost",
                "gross_margin_reported",
            ),
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {"动力电池系统": "分产品"},
            "candidate_pages": (25,),
            "page_contexts": (
                PreparedPageContext(
                    page=25,
                    text=evidence.anchor.bounded_quote,
                    text_hash="b" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )


def _high_cardinality_segment_prepared_scope() -> PreparedRequestScope:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence
    source = f"{evidence.anchor.bounded_quote} " + " ".join(
        str(index) for index in range(1, 42)
    )
    evidence = evidence.model_copy(update={"anchor": TextAnchor(bounded_quote=source)})
    return prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "page_contexts": (
                prepared.page_contexts[0].model_copy(
                    update={"text": source, "text_hash": "d" * 64}
                ),
            ),
        }
    )


def _multi_evidence_high_cardinality_segment_prepared_scope() -> PreparedRequestScope:
    prepared = _high_cardinality_segment_prepared_scope()
    base = prepared.evidence_bundle[0].evidence
    evidence = (
        base.model_copy(
            update={
                "evidence_id": "segment-revenue-evidence",
                "page": 25,
                "anchor": TextAnchor(
                    bounded_quote="分产品 动力电池系统 营业收入 316,506,369 千元"
                ),
            }
        ),
        base.model_copy(
            update={
                "evidence_id": "segment-cost-evidence",
                "page": 26,
                "anchor": TextAnchor(
                    bounded_quote="分产品 动力电池系统 营业成本 241,064,397 千元"
                ),
            }
        ),
        base.model_copy(
            update={
                "evidence_id": "segment-margin-evidence",
                "page": 27,
                "anchor": TextAnchor(
                    bounded_quote="分产品 动力电池系统 毛利率 23.84%"
                ),
            }
        ),
    )
    source = " ".join(item.anchor.bounded_quote for item in evidence)
    source = f"{source} " + " ".join(str(index) for index in range(1, 42))
    return prepared.model_copy(
        update={
            "evidence_bundle": tuple(PreparedEvidence(evidence=item) for item in evidence),
            "candidate_pages": (25, 26, 27),
            "page_contexts": tuple(
                PreparedPageContext(
                    page=item.page,
                    text=(source if item.page == 25 else item.anchor.bounded_quote),
                    text_hash={25: "e", 26: "f", 27: "0"}[item.page] * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                )
                for item in evidence
            ),
        }
    )


def _segment_extract_request(
    prepared: PreparedRequestScope,
) -> SemanticTaskRequest:
    coverage_statuses = (
        CoverageStatus.OBSERVED,
        CoverageStatus.EXTRACTION_FAILED,
        CoverageStatus.UNCLEAR,
    )
    checklist = (
        ChecklistItem(
            field_id="segment_dimension",
            object_type=ObjectType.SEGMENT,
            chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            requirement_level=RequirementLevel.REQUIRED,
            allowed_coverage_statuses=coverage_statuses,
        ),
        ChecklistItem(
            field_id="operating_revenue",
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=coverage_statuses,
            allowed_metric_types=(MetricType.OPERATING_REVENUE,),
        ),
        ChecklistItem(
            field_id="operating_cost",
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=coverage_statuses,
            allowed_metric_types=(MetricType.OPERATING_COST,),
        ),
        ChecklistItem(
            field_id="gross_margin_reported",
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=coverage_statuses,
            allowed_metric_types=(MetricType.GROSS_MARGIN_REPORTED,),
        ),
    )
    package = PackageManifest(
        package_name="manufacturing_materials",
        package_version="v1",
        report=prepared.report,
        checklist=checklist,
    )
    return SemanticTaskRequest(
        request_id="slice-1:segment",
        report=prepared.report,
        package_manifest=package,
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.SEGMENT, ObjectType.MEASUREMENT),
        allowed_metric_types=(
            MetricType.OPERATING_REVENUE,
            MetricType.OPERATING_COST,
            MetricType.GROSS_MARGIN_REPORTED,
        ),
        unresolved_field_ids=prepared.field_ids,
    )


def _same_control_request(prepared: PreparedRequestScope) -> SemanticTaskRequest:
    checklist = ChecklistItem(
        field_id="operating_revenue",
        object_type=ObjectType.MEASUREMENT,
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=(MetricType.OPERATING_REVENUE,),
    )
    return SemanticTaskRequest(
        request_id="same-control-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(MetricType.OPERATING_REVENUE,),
        unresolved_field_ids=prepared.field_ids,
    )


def _business_regime_request(prepared: PreparedRequestScope) -> SemanticTaskRequest:
    checklist = ChecklistItem(
        field_id="business_regime",
        object_type=ObjectType.BUSINESS_EVENT,
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        requirement_level=RequirementLevel.REQUIRED,
        allowed_coverage_statuses=tuple(CoverageStatus),
    )
    return SemanticTaskRequest(
        request_id="business-regime-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(
            ObjectType.BUSINESS_EVENT,
            ObjectType.BUSINESS_REGIME,
            ObjectType.INDUSTRY_PACKAGE_ASSIGNMENT,
        ),
        unresolved_field_ids=prepared.field_ids,
    )


def _capacity_extract_request(
    prepared: PreparedRequestScope,
) -> SemanticTaskRequest:
    statuses = (
        CoverageStatus.OBSERVED,
        CoverageStatus.EXTRACTION_FAILED,
        CoverageStatus.UNCLEAR,
    )
    checklist = tuple(
        ChecklistItem(
            field_id=metric_type.value,
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=statuses,
            allowed_metric_types=(metric_type,),
        )
        for metric_type in (
            MetricType.PRODUCTION_CAPACITY,
            MetricType.CAPACITY_UNDER_CONSTRUCTION,
        )
    )
    package = PackageManifest(
        package_name="manufacturing_materials",
        package_version="v1",
        report=prepared.report,
        checklist=checklist,
    )
    return SemanticTaskRequest(
        request_id="slice-1:capacity",
        report=prepared.report,
        package_manifest=package,
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.MEASUREMENT,),
        allowed_metric_types=(
            MetricType.PRODUCTION_CAPACITY,
            MetricType.CAPACITY_UNDER_CONSTRUCTION,
        ),
        unresolved_field_ids=prepared.field_ids,
    )


def _material_input_extract_request(
    prepared: PreparedRequestScope,
) -> SemanticTaskRequest:
    checklist = ChecklistItem(
        field_id="material_input",
        object_type=ObjectType.RELATIONSHIP,
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
    )
    return SemanticTaskRequest(
        request_id="material-input-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.RELATIONSHIP,),
        unresolved_field_ids=("material_input",),
    )


def _supplier_totals_extract_request(
    prepared: PreparedRequestScope,
) -> SemanticTaskRequest:
    checklist = (
        ChecklistItem(
            field_id="counterparty_relationship",
            object_type=ObjectType.RELATIONSHIP,
            chapter_task=ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
        ),
        ChecklistItem(
            field_id="supplier_concentration",
            object_type=ObjectType.MEASUREMENT,
            chapter_task=ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
            requirement_level=RequirementLevel.CONDITIONAL,
            allowed_coverage_statuses=tuple(CoverageStatus),
            allowed_metric_types=(
                MetricType.SUPPLIER_PURCHASE_AMOUNT,
                MetricType.DISCLOSED_SHARE,
            ),
        ),
    )
    return SemanticTaskRequest(
        request_id="supplier-totals-request",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=checklist,
        ),
        chapter_task=ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.RELATIONSHIP, ObjectType.MEASUREMENT),
        allowed_metric_types=(
            MetricType.SUPPLIER_PURCHASE_AMOUNT,
            MetricType.DISCLOSED_SHARE,
        ),
        unresolved_field_ids=("counterparty_relationship", "supplier_concentration"),
    )


def _segment_row_response(
    *,
    request_id: str,
    evidence_id: str,
    dimension: str | None = None,
    label: str = "动力电池系统",
    row_class: str | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "label": label,
        "subject_scope": "unclear",
        "reported_period": "2025",
        "period_type": "duration",
        "evidence_ids": [evidence_id],
        "cells": {
            "operating_revenue": {
                "value": "316,506,369",
                "unit": "千元",
                "header": "营业收入",
            }
        },
    }
    if dimension is not None:
        row["dimension"] = dimension
    if row_class is not None:
        row["row_class"] = row_class
    return {
        "schema_version": "company_profile_extract_response.v1",
        "request_id": request_id,
        "items": [{"item_type": "segment_row", "row": row}],
    }


def _segment_partition_response(
    *,
    request_id: str,
    evidence_id: str,
    metric_field: str,
    subject_scope: str = "unclear",
    reported_period: str = "2025",
    uncertainty: tuple[str, ...] = (),
) -> dict[str, Any]:
    cells = {
        "operating_revenue": {
            "value": "316,506,369",
            "unit": "千元",
            "header": "营业收入",
        },
        "operating_cost": {
            "value": "241,064,397",
            "unit": "千元",
            "header": "营业成本",
        },
        "gross_margin_reported": {
            "value": "23.84%",
            "unit": "%",
            "header": "毛利率",
        },
    }
    return {
        "schema_version": "company_profile_extract_response.v1",
        "request_id": request_id,
        "items": [
            {
                "item_type": "segment_row",
                "row": {
                    "label": "动力电池系统",
                    "subject_scope": subject_scope,
                    "reported_period": reported_period,
                    "period_type": "duration",
                    "evidence_ids": [evidence_id],
                    "cells": {metric_field: cells[metric_field]},
                    **(
                        {"uncertainty": list(uncertainty)} if uncertainty else {}
                    ),
                },
            }
        ],
    }


def _consolidation_adjustment_verify_request() -> tuple[
    PreparedRequestScope, VerifyRequest
]:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(bounded_quote="分产品 合并抵消项 -2,098,859,323.96 元")
        }
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {},
        }
    )
    extract_request = _segment_extract_request(prepared)
    compact = _segment_row_response(
        request_id=extract_request.request_id,
        evidence_id=evidence.evidence_id,
        label="合并抵消项",
        row_class="consolidation_adjustment",
    )
    candidates = ExtractResponse.model_validate_json(
        json.dumps(
            _expand_extract_response(
                compact,
                request=extract_request,
                prepared_scope=prepared,
            ),
            ensure_ascii=False,
        )
    ).candidates()
    return prepared, VerifyRequest(
        request_id=f"{extract_request.request_id}:verify",
        original_request_id=extract_request.request_id,
        report=prepared.report,
        evidence_bundle=prepared.evidence_bundle,
        candidates=candidates,
        coverage=(),
    )


def _extract_request(prepared: PreparedRequestScope) -> SemanticTaskRequest:
    checklist = ChecklistItem(
        field_id="business_overview_source",
        object_type=ObjectType.BUSINESS_OVERVIEW,
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        requirement_level=RequirementLevel.REQUIRED,
        allowed_coverage_statuses=(
            CoverageStatus.OBSERVED,
            CoverageStatus.EXTRACTION_FAILED,
            CoverageStatus.UNCLEAR,
        ),
    )
    package = PackageManifest(
        package_name="manufacturing_materials",
        package_version="v1",
        report=prepared.report,
        checklist=(checklist,),
    )
    return SemanticTaskRequest(
        request_id="slice-1:business_overview",
        report=prepared.report,
        package_manifest=package,
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(ObjectType.BUSINESS_OVERVIEW,),
        unresolved_field_ids=("business_overview_source",),
    )


def _overview_candidate(prepared: PreparedRequestScope) -> BusinessOverview:
    quote = prepared.evidence_bundle[0].evidence.anchor.bounded_quote
    return BusinessOverview(
        record_id="overview-1",
        field_id="business_overview_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=prepared.report,
        subject_scope=SubjectScope.UNCLEAR,
        reported_period="2025",
        period_type=PeriodType.DURATION,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(prepared.evidence_bundle[0].evidence,),
        source_native=SourceNativeValue(name="主要业务"),
        source_text=quote,
    )


def _scope_with_source_text(
    *,
    chapter_task: ChapterTask,
    scope_id: str,
    field_id: str,
    source_text: str,
) -> PreparedRequestScope:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": scope_id,
            "chapter_task": chapter_task,
            "field_ids": (field_id,),
        }
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "section_title": scope_id,
            "anchor": TextAnchor(bounded_quote=source_text),
        }
    )
    return prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=evidence, field_id=field_id),
            ),
            "page_contexts": (
                PreparedPageContext(
                    page=evidence.page,
                    text=source_text,
                    text_hash="f" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )


@pytest.mark.parametrize(
    "source_text",
    [
        "报告期主要子公司股权变动导致合并范围变化 √适用 □不适用。",
        "合并报表范围发生变化，2025年1月27日将仙人掌科技纳入合并报表范围，并新设全资子公司。",
    ],
)
def test_business_regime_rejects_not_applicable_when_control_change_is_evidenced(
    source_text: str,
) -> None:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        scope_id="business_regime",
        field_id="business_regime",
        source_text=source_text,
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    with pytest.raises(
        ValueError, match="contradicts an evidenced control-scope change"
    ):
        _normalize_extract_response(
            {
                "events": [],
                "regimes": [],
                "package_assignments": [],
                "coverage": [
                    {
                        "field_id": "business_regime",
                        "status": "not_applicable",
                        "reason_code": "source_explicitly_not_applicable",
                        "evidence_ids": [evidence_id],
                    }
                ],
            },
            request=request,
            prepared_scope=prepared,
        )


@pytest.mark.parametrize(
    "source_text",
    [
        "公司主营业务数据统计口径在报告期发生调整的情况下，公司最近1年按报告期末口径调整后的主营业务数据 □适用 ☑不适用。",
        "公司主营业务数据统计口径在报告期发生调整的情况下，公司最近1年按报告期末口径调整后的主营业务数据 □适用 不适用。",
        "公司主营业务数据统计口径在报告期发生调整的情况下，公司最近1年按报告期末口径调整后的主营业务数据 □适用 √不适用。",
    ],
)
def test_business_regime_rejects_statistical_calibre_only_not_applicable(
    source_text: str,
) -> None:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        scope_id="business_regime",
        field_id="business_regime",
        source_text=source_text,
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    with pytest.raises(ValueError, match="statistical-calibre coverage"):
        _normalize_extract_response(
            {
                "events": [],
                "regimes": [],
                "package_assignments": [],
                "coverage": [
                    {
                        "field_id": "business_regime",
                        "status": "not_applicable",
                        "reason_code": "source_explicitly_not_applicable",
                        "evidence_ids": [evidence_id],
                    }
                ],
            },
            request=request,
            prepared_scope=prepared,
        )


def test_business_regime_accepts_complete_explicit_no_change_coverage() -> None:
    source_text = (
        "公司报告期内业务、产品或服务发生重大变化或调整有关情况 □适用 √不适用。"
    )
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        scope_id="business_regime",
        field_id="business_regime",
        source_text=source_text,
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    result = _normalize_extract_response(
        {
            "events": [],
            "regimes": [],
            "package_assignments": [],
            "coverage": [
                {
                    "field_id": "business_regime",
                    "status": "not_applicable",
                    "reason_code": "source_explicitly_not_applicable",
                    "evidence_ids": [evidence_id],
                }
            ],
        },
        request=request,
        prepared_scope=prepared,
    )

    assert result["items"][0]["coverage"]["status"] == "not_applicable"


@pytest.mark.parametrize("name", ["材料", "原材料", "原料", "原燃料", "燃料", "能源"])
def test_material_input_rejects_generic_cost_categories(name: str) -> None:
    source_text = f"分产品 成本构成项目 本期金额 煤炭 {name} 21,283.40"
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        scope_id="material_inputs",
        field_id="material_input",
        source_text=source_text,
    )
    request = _material_input_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    with pytest.raises(ValueError, match="specifically named"):
        _normalize_extract_response(
            {
                "material_inputs": [{"name": name, "evidence_id": evidence_id}],
                "coverage": None,
            },
            request=request,
            prepared_scope=prepared,
        )


@pytest.mark.parametrize("name", ["材料", "原材料", "原料", "原燃料", "燃料", "能源"])
def test_material_input_accepts_generic_word_when_source_explicitly_procures_it(
    name: str,
) -> None:
    source_text = f"公司直接采购{name}用于生产。"
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        scope_id="material_inputs",
        field_id="material_input",
        source_text=source_text,
    )
    request = _material_input_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    result = _normalize_extract_response(
        {
            "material_inputs": [{"name": name, "evidence_id": evidence_id}],
            "coverage": None,
        },
        request=request,
        prepared_scope=prepared,
    )

    assert result["items"][0]["candidate"]["object_name"] == name


@pytest.mark.parametrize("name", ["电", "蒸汽", "铁矿石", "天然原材料", "合成原材料"])
def test_material_input_accepts_explicit_named_inputs(name: str) -> None:
    source_text = f"公司生产所需主要原材料及能源包括{name}，采用集中采购模式。"
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        scope_id="material_inputs",
        field_id="material_input",
        source_text=source_text,
    )
    request = _material_input_extract_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    result = _normalize_extract_response(
        {
            "material_inputs": [{"name": name, "evidence_id": evidence_id}],
            "coverage": None,
        },
        request=request,
        prepared_scope=prepared,
    )

    assert result["items"][0]["candidate"]["object_name"] == name


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CORRECTION_CHANGE_ROOT = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-correct-company-profile-shadow-evidence-routing-and-regime-coverage"
)
SOURCE_REVIEW_PACKAGE = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-validate-refined-company-profile-shadow-batch/source-text-review-package.v2.json"
)


def test_reviewed_semantic_corrections_hit_the_typed_guards() -> None:
    cases = json.loads(
        (CORRECTION_CHANGE_ROOT / "reviewed-correction-cases.v1.json").read_text(
            encoding="utf-8"
        )
    )
    review_rows = {
        row["review_row_id"]: row
        for row in json.loads(SOURCE_REVIEW_PACKAGE.read_text(encoding="utf-8"))["rows"]
    }
    semantic_cases = [
        item
        for item in cases["cases"]
        if item["correction_family"] != "evidence_routing"
    ]

    assert len(semantic_cases) == 6
    for case in semantic_cases:
        row = review_rows[case["review_row_id"]]
        source_text = row["source_quote"]
        if case["correction_family"] == "generic_material_input":
            prepared = _scope_with_source_text(
                chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
                scope_id=row["scope_id"],
                field_id="material_input",
                source_text=source_text,
            )
            request = _material_input_extract_request(prepared)
            evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
            payload = {
                "material_inputs": [{"name": "材料", "evidence_id": evidence_id}],
                "coverage": None,
            }
            expected_error = "specifically named"
        else:
            prepared = _scope_with_source_text(
                chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
                scope_id=row["scope_id"],
                field_id="business_regime",
                source_text=source_text,
            )
            request = _business_regime_request(prepared)
            evidence_id = prepared.evidence_bundle[0].evidence.evidence_id
            payload = {
                "events": [],
                "regimes": [],
                "package_assignments": [],
                "coverage": [
                    {
                        "field_id": "business_regime",
                        "status": "not_applicable",
                        "reason_code": "source_explicitly_not_applicable",
                        "evidence_ids": [evidence_id],
                    }
                ],
            }
            expected_error = (
                "statistical-calibre coverage"
                if case["correction_family"] == "statistical_calibre_scope"
                else "contradicts an evidenced control-scope change"
            )
        with pytest.raises(ValueError, match=expected_error):
            _normalize_extract_response(
                payload,
                request=request,
                prepared_scope=prepared,
            )


_EXACT_CONTROL_CHANGE_AND_BUSINESS_NO_CHANGE = (
    "（6）报告期内合并范围是否发生变动 ☑是 □否 "
    "截至2025年12月31日，本集团纳入合并范围的子公司共43户。"
    "本集团本期合并范围比上年增加1户，减少4户，净减少3户。"
    "（7）公司报告期内业务、产品或服务发生重大变化或调整有关情况 "
    "□适用 ☑不适用"
)


def test_business_regime_exact_control_change_requires_event() -> None:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        scope_id="business_regime",
        field_id="business_regime",
        source_text=_EXACT_CONTROL_CHANGE_AND_BUSINESS_NO_CHANGE,
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    with pytest.raises(
        ValueError, match="contradicts an evidenced control-scope change"
    ):
        _normalize_extract_response(
            {
                "events": [],
                "regimes": [],
                "package_assignments": [],
                "coverage": [
                    {
                        "field_id": "business_regime",
                        "status": "not_applicable",
                        "reason_code": "source_explicitly_not_applicable",
                        "evidence_ids": [evidence_id],
                    }
                ],
            },
            request=request,
            prepared_scope=prepared,
        )


def test_business_regime_preserves_event_and_separate_business_no_change() -> None:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        scope_id="business_regime",
        field_id="business_regime",
        source_text=_EXACT_CONTROL_CHANGE_AND_BUSINESS_NO_CHANGE,
    )
    request = _business_regime_request(prepared)
    evidence_id = prepared.evidence_bundle[0].evidence.evidence_id

    result = _normalize_extract_response(
        {
            "events": [
                {
                    "event_type": "consolidation_scope_change",
                    "description": "本集团本期合并范围比上年增加1户，减少4户，净减少3户。",
                    "evidence_id": evidence_id,
                }
            ],
            "regimes": [],
            "package_assignments": [],
            "coverage": [
                {
                    "field_id": "business_regime",
                    "status": "not_applicable",
                    "reason_code": "source_explicitly_not_applicable",
                    "evidence_ids": [evidence_id],
                }
            ],
        },
        request=request,
        prepared_scope=prepared,
    )

    assert [item["item_type"] for item in result["items"]] == [
        "candidate",
        "coverage",
    ]


def test_business_regime_control_no_change_alone_cannot_close_broad_field() -> None:
    source_text = "（八）合并报表范围的变化情况 □适用 √不适用"
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_BUSINESS_REGIME,
        scope_id="business_regime",
        field_id="business_regime",
        source_text=source_text,
    )
    request = _business_regime_request(prepared)

    with pytest.raises(ValueError, match="cannot close broader business_regime"):
        _normalize_extract_response(
            {
                "events": [],
                "regimes": [],
                "package_assignments": [],
                "coverage": [
                    {
                        "field_id": "business_regime",
                        "status": "not_disclosed",
                        "reason_code": "source_explicitly_not_disclosed",
                        "evidence_ids": [
                            prepared.evidence_bundle[0].evidence.evidence_id
                        ],
                    }
                ],
            },
            request=request,
            prepared_scope=prepared,
        )


def _owner_and_context_material_scope() -> PreparedRequestScope:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        scope_id="material_inputs",
        field_id="material_input",
        source_text="公司生产所需主要原材料为铁矿石，采用集中采购模式。",
    )
    owner = prepared.evidence_bundle[0].evidence
    context = owner.model_copy(
        update={
            "evidence_id": "stage5-provider-context-evidence",
            "page": owner.page + 1,
            "section_title": "相邻上下文",
            "anchor": TextAnchor(bounded_quote="相邻页仅讨论销售模式。"),
        }
    )
    return prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=owner, field_id="material_input"),
                PreparedEvidence(evidence=context, field_id="material_input"),
            ),
            "candidate_pages": (owner.page,),
            "page_contexts": prepared.page_contexts
            + (
                PreparedPageContext(
                    page=context.page,
                    text=context.anchor.bounded_quote,
                    text_hash="9" * 64,
                    extraction_method="pypdf",
                    quality_status="usable",
                ),
            ),
        }
    )


def test_coverage_without_ids_binds_only_candidate_page_evidence() -> None:
    prepared = _owner_and_context_material_scope()
    request = _material_input_extract_request(prepared)

    result = _normalize_extract_response(
        {
            "schema_version": "company_profile_extract_response.v1",
            "request_id": request.request_id,
            "items": [
                {
                    "item_type": "coverage",
                    "coverage": {
                        "field_id": "material_input",
                        "status": "not_disclosed",
                        "reason_code": "source_explicitly_not_disclosed",
                    },
                }
            ],
        },
        request=request,
        prepared_scope=prepared,
    )

    assert result["items"][0]["coverage"]["evidence"][0]["page"] == 14
    assert len(result["items"][0]["coverage"]["evidence"]) == 1


def test_coverage_rejects_explicit_context_only_evidence() -> None:
    prepared = _owner_and_context_material_scope()
    request = _material_input_extract_request(prepared)
    context_id = prepared.evidence_bundle[1].evidence.evidence_id

    with pytest.raises(ValueError, match="context-only ids"):
        _normalize_extract_response(
            {
                "schema_version": "company_profile_extract_response.v1",
                "request_id": request.request_id,
                "items": [
                    {
                        "item_type": "coverage",
                        "coverage": {
                            "field_id": "material_input",
                            "status": "not_disclosed",
                            "reason_code": "source_explicitly_not_disclosed",
                            "evidence_ids": [context_id],
                        },
                    }
                ],
            },
            request=request,
            prepared_scope=prepared,
        )


def _owner_coverage_request(prepared: PreparedRequestScope) -> SemanticTaskRequest:
    field_id = prepared.field_ids[0]
    object_types = {
        "production_capacity": ObjectType.MEASUREMENT,
        "production_volume": ObjectType.MEASUREMENT,
        "material_input": ObjectType.RELATIONSHIP,
        "segment_dimension": ObjectType.SEGMENT,
    }
    metric_types = {
        "production_capacity": (MetricType.PRODUCTION_CAPACITY,),
        "production_volume": (MetricType.PRODUCTION_VOLUME,),
    }.get(field_id, ())
    checklist = ChecklistItem(
        field_id=field_id,
        object_type=object_types[field_id],
        chapter_task=prepared.chapter_task,
        requirement_level=RequirementLevel.CONDITIONAL,
        allowed_coverage_statuses=tuple(CoverageStatus),
        allowed_metric_types=metric_types,
    )
    return SemanticTaskRequest(
        request_id=f"owner-coverage:{field_id}",
        report=prepared.report,
        package_manifest=PackageManifest(
            package_name="manufacturing_materials",
            package_version="v1",
            report=prepared.report,
            checklist=(checklist,),
        ),
        chapter_task=prepared.chapter_task,
        evidence_bundle=prepared.evidence_bundle,
        allowed_object_types=(object_types[field_id],),
        allowed_metric_types=metric_types,
        unresolved_field_ids=(field_id,),
    )


@pytest.mark.parametrize(
    ("chapter_task", "field_id", "section_title", "source_text"),
    [
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "production_capacity",
            "business_overview",
            "营业收入下降主要是受钢材销售量及销售价格降低影响。",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "production_capacity",
            "industry_context",
            "国内PVC库存量仍处于较高水平，沿海地区新建产能集中投产。",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "production_capacity",
            "industry_context",
            "前期在建产能有序释放，国内煤炭产能保障根基持续夯实。",
        ),
        (
            ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "material_input",
            "controlling_shareholder",
            "四、控股股东及实际控制人情况：控股股东主要经营业务包括稀土原料生产与供应。",
        ),
        (
            ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
            "segment_dimension",
            "industry_context",
            "报告期内公司所处行业情况：炼化一体化产业链布局持续完善。",
        ),
        (
            ChapterTask.EXTRACT_OPERATING_QUANTITIES,
            "production_volume",
            "business_model",
            "公司根据销售预测量、往年同期产量、销量和目前库存量制定生产计划。",
        ),
    ],
)
def test_legal_empty_coverage_rejects_frozen_non_owner_shapes(
    chapter_task: ChapterTask,
    field_id: str,
    section_title: str,
    source_text: str,
) -> None:
    prepared = _scope_with_source_text(
        chapter_task=chapter_task,
        scope_id=f"{field_id}-owner-regression",
        field_id=field_id,
        source_text=source_text,
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"section_title": section_title}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence, field_id=field_id),),
            "candidate_pages": (evidence.page,),
        }
    )
    request = _owner_coverage_request(prepared)

    with pytest.raises(ValueError, match="chapter-owning Evidence"):
        _normalize_extract_response(
            {
                "schema_version": "company_profile_extract_response.v1",
                "request_id": request.request_id,
                "items": [
                    {
                        "item_type": "coverage",
                        "coverage": {
                            "field_id": field_id,
                            "status": "not_disclosed",
                            "reason_code": "source_explicitly_not_disclosed",
                            "evidence_ids": [evidence.evidence_id],
                        },
                    }
                ],
            },
            request=request,
            prepared_scope=prepared,
        )


def test_material_legal_empty_preserves_explicit_owner_applicability() -> None:
    source_text = (
        "三、主要原材料及能源采购 （一）主要原材料及能源情况 "
        "□适用 √不适用 四、安全生产与环保"
    )
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_MATERIAL_INPUTS,
        scope_id="material_inputs-01",
        field_id="material_input",
        source_text=source_text,
    )
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"section_title": "procurement_and_costs"}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (
                PreparedEvidence(evidence=evidence, field_id="material_input"),
            ),
            "candidate_pages": (evidence.page,),
        }
    )
    request = _owner_coverage_request(prepared)

    result = _normalize_extract_response(
        {
            "schema_version": "company_profile_extract_response.v1",
            "request_id": request.request_id,
            "items": [
                {
                    "item_type": "coverage",
                    "coverage": {
                        "field_id": "material_input",
                        "status": "not_applicable",
                        "reason_code": "source_explicitly_not_applicable",
                        "evidence_ids": [evidence.evidence_id],
                    },
                }
            ],
        },
        request=request,
        prepared_scope=prepared,
    )

    assert result["items"][0]["coverage"]["status"] == "not_applicable"
    assert result["items"][0]["coverage"]["evidence"][0]["evidence_id"] == (
        evidence.evidence_id
    )


def test_unclear_coverage_does_not_require_legal_empty_field_owner() -> None:
    prepared = _scope_with_source_text(
        chapter_task=ChapterTask.EXTRACT_OPERATING_QUANTITIES,
        scope_id="production_volume-owner-regression",
        field_id="production_volume",
        source_text="公司根据销售预测量和目前库存量制定生产计划。",
    )
    evidence = prepared.evidence_bundle[0].evidence
    prepared = prepared.model_copy(
        update={"candidate_pages": (evidence.page,)}
    )
    request = _owner_coverage_request(prepared)

    result = _normalize_extract_response(
        {
            "schema_version": "company_profile_extract_response.v1",
            "request_id": request.request_id,
            "items": [
                {
                    "item_type": "coverage",
                    "coverage": {
                        "field_id": "production_volume",
                        "status": "unclear",
                        "reason_code": "candidate_unresolved",
                        "evidence_ids": [evidence.evidence_id],
                    },
                }
            ],
        },
        request=request,
        prepared_scope=prepared,
    )

    assert result["items"][0]["coverage"]["status"] == "unclear"


def test_cost_component_cannot_become_segment_identity() -> None:
    prepared = _segment_prepared_scope()
    source_text = "分行业 | 钢铁业 | 原材料及燃动费 | 分行业 营业收入 316,506,369"
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={"anchor": TextAnchor(bounded_quote=source_text)}
    )
    prepared = prepared.model_copy(
        update={
            "evidence_bundle": (PreparedEvidence(evidence=evidence),),
            "source_row_dimensions": {
                "钢铁业": "分行业",
                "原材料及燃动费": "分行业",
            },
            "page_contexts": (
                prepared.page_contexts[0].model_copy(update={"text": source_text}),
            ),
        }
    )
    request = _segment_extract_request(prepared)

    with pytest.raises(ValueError, match="cost-component row"):
        _normalize_extract_response(
            _segment_row_response(
                request_id=request.request_id,
                evidence_id=evidence.evidence_id,
                dimension="分行业",
                label="原材料及燃动费",
            ),
            request=request,
            prepared_scope=prepared,
        )

    valid = _normalize_extract_response(
        _segment_row_response(
            request_id=request.request_id,
            evidence_id=evidence.evidence_id,
            dimension=None,
            label="钢铁业",
        ),
        request=request,
        prepared_scope=prepared,
    )
    assert valid["items"][0]["candidate"]["label"] == "钢铁业"
