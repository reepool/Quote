from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from research.company_profile.contracts import (
    ChecklistItem,
    ContractErrorCode,
    PackageManifest,
    PreparedEvidence,
    RepairRequest,
    SemanticProviderError,
    SemanticTaskRequest,
    VerifyRequest,
)
from research.company_profile.models import (
    ActivityAction,
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    CoverageStatus,
    Evidence,
    MetricType,
    ObjectType,
    PeriodType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope
from research.company_profile.stage5_provider import (
    _SCOPE_INSTRUCTIONS,
    _TASK_INSTRUCTIONS,
    CommonGatewaySemanticProvider,
    _coverage_draft_schema,
    _expand_extract_response,
    _minimal_extract_schema,
)
from research.company_profile.workflow import CompanyProfileSemanticService
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
        ("procurement_mode", "to invent named material inputs"),
        ("top_five_customer_totals_only", "This request scope is totals-only"),
        ("top_five_supplier_totals_only", "This request scope is totals-only"),
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
    assert overview["reported_period"] == "2025年度"
    assert activity["activity_actor"] == "公司"
    assert activity["source_actor"] == "公司"
    assert activity["source_verb"] == "生产"
    assert activity["source_native"]["header"] == "主要业务"


def test_procurement_mode_uses_flat_draft_and_reconstructs_mechanical_fields() -> None:
    prepared = _prepared_scope().model_copy(
        update={
            "scope_id": "procurement_mode",
            "chapter_task": ChapterTask.EXTRACT_MATERIAL_INPUTS,
            "field_ids": ("material_input",),
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
    assert candidate["reported_period"] == "2025年度"
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
    assert candidates[0]["dimension"] == "分产品"
    assert candidates[0]["source_native"]["header"] == "分产品"


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
    candidate_schemas = schema["properties"]["items"]["items"]["oneOf"][0][
        "properties"
    ]["candidate"]["oneOf"]
    by_metric = {
        item["properties"]["metric_type"]["enum"][0]: item for item in candidate_schemas
    }

    production = by_metric["production_capacity"]
    under_construction = by_metric["capacity_under_construction"]
    assert production["properties"]["field_id"] == {"const": "production_capacity"}
    assert "capacity_kind" in production["properties"]
    assert "capacity_kind" in production["required"]
    assert under_construction["properties"]["field_id"] == {
        "const": "capacity_under_construction"
    }
    assert "capacity_kind" not in under_construction["properties"]
    assert "capacity_kind" not in under_construction["required"]


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
    assert "reason_code" not in not_applicable["required"]


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


def test_consolidation_adjustment_normalizes_internal_dimension() -> None:
    prepared = _segment_prepared_scope()
    evidence = prepared.evidence_bundle[0].evidence.model_copy(
        update={
            "anchor": TextAnchor(
                bounded_quote="分行业 分部间抵销 -61,069,781 -60,974,432 0.16%"
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
        dimension="分产品",
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
        dimension="分产品",
        label="新能源电池材料与服务",
        row_class="consolidation_adjustment",
    )

    with pytest.raises(ValueError, match="explicitly identify an adjustment"):
        _expand_extract_response(
            compact,
            request=request,
            prepared_scope=prepared,
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
        timeout_seconds=30,
    )

    repaired = provider.repair(repair_request)
    provider.verify(verify_request)
    assert repaired["candidate"]["subject_scope"] == "issuer"

    assert [request.schema_name for request in client.requests] == [
        "company_profile_repair_response",
        "company_profile_verify_response",
    ]
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
