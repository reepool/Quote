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
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    CoverageStatus,
    Evidence,
    ObjectType,
    PeriodType,
    ReportIdentity,
    RequirementLevel,
    SourceNativeValue,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.stage5 import PreparedPageContext, PreparedRequestScope
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.workflow import CompanyProfileSemanticService
from utils.llm import LlmMessage, LlmRateLimitError, LlmRequest, LlmResponse


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
    prepared = _prepared_scope()
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
    assert gateway_request.schema_name == "company_profile_extract_response"
    assert gateway_request.response_schema.__name__ == "ExtractResponse"
    envelope = json.loads(LlmMessage.from_value(gateway_request.messages[1]).content)
    assert envelope["request_scope"]["scope_id"] == prepared.scope_id
    assert envelope["request_scope"]["field_ids"] == ["business_overview_source"]
    assert "gold" not in LlmMessage.from_value(gateway_request.messages[1]).content.lower()
    assert provider.traces[0].call_type == "extract"
    assert provider.traces[0].status == "success"


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
                "candidate": candidate.model_dump(mode="json"),
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

    provider.repair(repair_request)
    provider.verify(verify_request)

    assert [request.schema_name for request in client.requests] == [
        "company_profile_repair_response",
        "company_profile_verify_response",
    ]
    assert [trace.call_type for trace in provider.traces] == ["repair", "verify"]
    assert client.requests[0].idempotency_key != client.requests[1].idempotency_key


def test_common_gateway_provider_maps_gateway_failures_to_typed_workflow_outcome() -> None:
    prepared = _prepared_scope()
    request = _extract_request(prepared)
    provider = CommonGatewaySemanticProvider(
        client=_FakeGatewayClient(
            outputs=[LlmRateLimitError("gateway is congested")]
        ),
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
            "anchor": TextAnchor(
                bounded_quote="公司主要从事动力电池研发、生产和销售。"
            )
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
