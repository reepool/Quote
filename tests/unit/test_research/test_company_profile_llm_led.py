"""Offline acceptance of the new report policy; no real model or blind samples."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field, replace

import pytest
from pydantic import ValidationError

from research.company_profile.contracts import (
    ReportSemanticRequest,
    ReportSemanticResponse,
)
from research.company_profile.models import (
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    Evidence,
    PeriodType,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.report_interpretation import (
    DIMENSIONS,
    core_assessment,
    interpret_report,
    offline_processing_identity,
)
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from tests.unit.test_research.test_company_profile_runtime import _report
from tests.unit.test_research.test_company_profile_stage5_provider import (
    _FakeGatewayClient,
)

TEXT = (
    "云桥公司经营云运维与工业设计两条业务线，向企业收取订阅费和设计服务费。"
    "公司采购服务器、电力和外部设计服务，向制造企业直接提供服务。"
    "第三方经销商销售设备；公司拟于下一年推出设备销售，但本期尚未开展。"
)
ANSWERS = {
    "principal_business": "经营云运维与工业设计两条业务线。",
    "products_services": "向企业提供云运维和工业设计服务。",
    "revenue_model": "向企业收取订阅费和设计服务费。",
    "output_exposure": "收入端是云运维订阅服务和工业设计服务。",
    "input_exposure": "明示投入包括服务器、电力和外部设计服务。",
    "supply_chain_role": "承担服务提供者角色；设备销售是下一年计划，本期未开展。",
    "upstream_downstream": "明示下游是制造企业，未披露具名交易对手。",
}


def _evidence(report, text=TEXT, page=1):
    return Evidence(
        evidence_id=f"source-{page}",
        report=report,
        page=page,
        section_title="新形态业务叙述",
        anchor=TextAnchor(bounded_quote=text),
    )


def _overview(report, evidence):
    return BusinessOverview(
        record_id="overview",
        field_id="business_overview_source",
        chapter_task=ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        report=report,
        subject_scope=SubjectScope.CONSOLIDATED_GROUP,
        subject_basis=SubjectBasis.REPORT_DEFAULT_GROUP_SCOPE,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        knowledge_time=report.published_at,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(evidence,),
        source_native=SourceNativeValue(name="业务说明"),
        source_text=TEXT,
    )


@dataclass
class FakeReportProvider:
    offline_only = True
    requests: list = field(default_factory=list)
    omit_major: bool = False
    blocked_dimension: str | None = None
    bad_quote: bool = False
    wrong_period: bool = False
    omit_verify: bool = False

    def interpret(self, request):
        self.requests.append(request)
        evidence = _evidence(request.report)
        if self.bad_quote:
            evidence = evidence.model_copy(
                update={"anchor": TextAnchor(bounded_quote="不存在的原文")}
            )
        payload = {"request_id": request.request_id, "purpose": request.purpose}
        if request.purpose == "source_inventory":
            payload["source_conditions"] = tuple(
                {
                    "condition_id": dimension,
                    "dimension": dimension,
                    "description": f"源条件：{dimension}",
                    "evidence": (evidence,),
                    "major": True,
                    "statement_status": "current",
                }
                for dimension in DIMENSIONS
            )
            if self.omit_major:
                payload["source_conditions"] += (
                    {
                        "condition_id": "industrial_design",
                        "dimension": "products_services",
                        "description": "工业设计是另一主要板块",
                        "evidence": (evidence,),
                        "major": True,
                        "statement_status": "current",
                    },
                )
        elif request.purpose == "synthesize_views":
            record = _overview(request.report, evidence)
            if self.wrong_period:
                record = record.model_copy(update={"reported_period": "1900"})
            payload["records"] = (record,)
            payload["claims"] = tuple(
                {
                    "claim_id": dimension,
                    "dimension": dimension,
                    "status": "observed",
                    "answer": ANSWERS[dimension],
                    "condition_ids": tuple(
                        c.condition_id
                        for c in request.source_conditions
                        if c.dimension == dimension
                        and "industrial_design" not in c.condition_id
                    ),
                    "supporting_record_ids": ("overview",),
                    "evidence": (evidence,),
                }
                for dimension in DIMENSIONS
            )
        else:
            targets = [("condition", c.condition_id) for c in request.source_conditions]
            targets += [("claim", c.claim_id) for c in request.claims]
            targets += [("record", c.record_id) for c in request.records]
            payload["checks"] = tuple(
                {
                    "target_type": kind,
                    "target_id": target,
                    "status": (
                        "block"
                        if (target == self.blocked_dimension and kind == "claim")
                        or "industrial_design" in target
                        else "pass"
                    ),
                    "explanation": "按完整来源核验；计划、否定和第三方不能当成本期公司经营。",
                }
                for kind, target in targets
            )
            if self.omit_verify:
                payload["checks"] = payload["checks"][:-1]
        return ReportSemanticResponse.model_validate(payload)


class FakeReportGatewayClient(_FakeGatewayClient):
    """Exercise the actual gateway adapter with in-memory, schema-checked replies."""

    def __init__(self):
        super().__init__(outputs=[])
        self.semantic = FakeReportProvider()

    async def complete(self, request):
        semantic_request = ReportSemanticRequest.model_validate_json(
            request.messages[1].content
        )
        output = self.semantic.interpret(semantic_request)
        self.outputs.append(output.model_dump(mode="json"))
        response = await super().complete(request)
        # Verify that the receipts preserve known usage and model/request lineage.
        from utils.llm.models import LlmUsage

        return replace(
            response,
            usage=LlmUsage(input_tokens=100, output_tokens=50, total_tokens=150),
        )


class OfflineGatewayProvider(CommonGatewaySemanticProvider):
    offline_only = True


def _run(provider, **overrides):
    from research.company_profile.runtime import _CORE_FIELDS, _checklist_item

    args = {
        "report": _report(),
        "pages": ({"page": 1, "text": TEXT},),
        "processing_identity": offline_processing_identity(),
        "provider": provider,
        "work_id": "offline-b1",
        "token_budget": 120_000,
        "active_checklist": tuple(
            _checklist_item(field_id, chapter)
            for chapter, fields in _CORE_FIELDS.items()
            for field_id in fields
        ),
    }
    args.update(overrides)
    return interpret_report(**args)


def test_source_first_reading_three_views_without_heading_regex():
    provider = FakeReportProvider()
    result = _run(provider)
    assert [r.purpose for r in provider.requests] == [
        "source_inventory",
        "synthesize_views",
        "verify_views",
    ]
    assert not provider.requests[0].records and not provider.requests[0].claims
    assert set(result.verified_dimensions) == set(DIMENSIONS)
    assert result.draft_complete
    assert not result.semantic_complete
    assert not result.real_semantic_complete
    assert result.execution_mode == "offline_fake"
    assert result.call_receipts[0]["usage"] is None
    assert result.tokens_charged > 0
    assert core_assessment(result).mapping_version.endswith("v2")
    assert result.page_coverage[0]["status"] == "read"


def test_source_first_inventory_finds_deliberately_omitted_major_segment():
    result = _run(FakeReportProvider(omit_major=True))
    assert not result.draft_complete
    assert not core_assessment(result).products_services.answered
    assert any("major_condition_not_delivered" in gap for gap in result.gaps)


@pytest.mark.parametrize("provider", [None, object()])
def test_missing_provider_never_completes_despite_source_overview(provider):
    result = _run(provider)
    assert not result.draft_complete
    assert not core_assessment(result).core_complete
    assert any("semantic_provider_unavailable" in gap for gap in result.gaps)


@pytest.mark.parametrize(
    "dimension", ["revenue_model", "supply_chain_role", "output_exposure"]
)
def test_semantic_plan_negation_third_party_or_conflict_blocks_dependent_answer(
    dimension,
):
    result = _run(FakeReportProvider(blocked_dimension=dimension))
    claim = next(item for item in result.claims if item.dimension == dimension)
    assert claim.status == "unclear"
    assert claim.limitation
    assert not result.draft_complete
    assert dimension not in result.verified_dimensions
    assert result.records  # unrelated source facts remain independently available


def test_unreadable_page_cannot_become_not_disclosed():
    result = _run(
        FakeReportProvider(),
        pages=({"page": 1, "text": TEXT}, {"page": 2, "text": "", "readable": False}),
    )
    assert not result.draft_complete
    assert result.page_coverage[1]["status"] == "extraction_failed"
    assert not result.verified_dimensions


@pytest.mark.parametrize(
    "kwargs", [{"bad_quote": True}, {"wrong_period": True}, {"omit_verify": True}]
)
def test_bad_evidence_period_or_incomplete_verify_cannot_pass(kwargs):
    result = _run(FakeReportProvider(**kwargs))
    assert not result.draft_complete
    assert result.gaps


def test_zero_budget_stops_before_fake_provider():
    provider = FakeReportProvider()
    result = _run(provider, token_budget=0)
    assert not provider.requests
    assert not result.draft_complete
    assert any("token_budget" in gap for gap in result.gaps)


def test_unknown_fact_field_is_isolated_even_if_semantic_verify_passes():
    class UnknownFieldProvider(FakeReportProvider):
        def interpret(self, request):
            response = super().interpret(request)
            if request.purpose == "synthesize_views":
                return response.model_copy(
                    update={
                        "records": tuple(
                            record.model_copy(update={"field_id": "unregistered_field"})
                            for record in response.records
                        )
                    }
                )
            return response

    result = _run(UnknownFieldProvider())
    assert not result.records
    assert not result.draft_complete
    assert any("invalid_record" in gap for gap in result.gaps)


def test_all_pages_are_accounted_for_independently_of_headings():
    pages = tuple({"page": n, "text": TEXT} for n in range(1, 5))
    result = _run(FakeReportProvider(), pages=pages)
    assert [item["page"] for item in result.page_coverage] == [1, 2, 3, 4]
    assert all(item["status"] == "read" for item in result.page_coverage)
    assert (
        len(result.call_receipts) == 3
    )  # small pages share a bounded discovery request
    missing = _run(FakeReportProvider(), pages=({"page": 2, "text": TEXT},))
    assert "physical_pages_missing" in missing.gaps
    assert not missing.draft_complete


@pytest.mark.parametrize("mutated_field", [None, "value", "unit"])
def test_reliable_numeric_records_reused_and_model_value_or_unit_mutation_isolated(
    mutated_field,
):
    from pydantic import TypeAdapter

    from research.company_profile.models import Measurement, SemanticRecord
    from tests.unit.test_research.test_company_profile_runtime import _reference_payload

    adapter = TypeAdapter(SemanticRecord)
    numeric = next(
        adapter.validate_json(json.dumps(raw))
        for raw in _reference_payload()["records"]
        if raw["object_type"] == "Measurement"
    )
    assert isinstance(numeric, Measurement)

    class StructuredProvider(FakeReportProvider):
        def interpret(self, request):
            response = super().interpret(request)
            if request.purpose == "synthesize_views":
                measure = request.records[0]
                if mutated_field:
                    measure = measure.model_copy(
                        update={
                            "source_native": measure.source_native.model_copy(
                                update={mutated_field: "wrong"}
                            )
                        }
                    )
                claims = tuple(
                    (
                        c.model_copy(
                            update={
                                "supporting_record_ids": ("overview", measure.record_id)
                            }
                        )
                        if c.dimension == "revenue_model"
                        else c
                    )
                    for c in response.claims
                )
                return response.model_copy(
                    update={"records": (*response.records, measure), "claims": claims}
                )
            return response

    result = _run(
        StructuredProvider(), report=numeric.report, structured_records=(numeric,)
    )
    if mutated_field:
        assert numeric.record_id not in {record.record_id for record in result.records}
        assert not core_assessment(result).revenue_model.answered
    else:
        assert numeric in result.records
        assert core_assessment(result).revenue_model.answered
        unavailable = _run(None, report=numeric.report, structured_records=(numeric,))
        assert numeric in unavailable.records
        assert not unavailable.semantic_complete


def test_real_execution_is_not_authorized_by_offline_development():
    provider = FakeReportProvider()
    result = _run(
        provider,
        processing_identity={
            **offline_processing_identity(),
            "execution_mode": "provider",
        },
    )
    assert not provider.requests
    assert not result.real_semantic_complete
    assert any("a0_real_execution_binding_required" in gap for gap in result.gaps)


def test_unknown_purpose_and_source_first_answer_leak_rejected_before_provider():
    kwargs = {
        "request_id": "x",
        "report": _report(),
        "processing_identity": offline_processing_identity(),
        "source_pages": ({"page": 1, "text": TEXT},),
        "max_output_tokens": 100,
        "timeout_seconds": 1.0,
    }
    with pytest.raises(ValidationError):
        ReportSemanticRequest(purpose="new_agent", **kwargs)
    record = _overview(_report(), _evidence(_report()))
    with pytest.raises(ValidationError):
        ReportSemanticRequest(purpose="source_inventory", records=(record,), **kwargs)


def test_gateway_adapter_uses_versioned_report_contract_and_preserves_usage_unknown():
    report = _report()
    request = ReportSemanticRequest(
        request_id="offline-b1:source",
        purpose="source_inventory",
        report=report,
        processing_identity=offline_processing_identity(),
        source_pages=({"page": 1, "text": TEXT},),
        max_output_tokens=2000,
        timeout_seconds=20.0,
    )
    client = _FakeGatewayClient(
        outputs=[{"request_id": request.request_id, "purpose": request.purpose}]
    )
    provider = CommonGatewaySemanticProvider(
        client=client,
        profile="semantic_extraction",
        max_output_tokens=2000,
        timeout_seconds=30,
    )
    provider.interpret(request)
    sent = client.requests[0]
    assert sent.schema_version == "company_profile_report_response.v2"
    assert sent.content_is_untrusted
    assert "source_inventory" in sent.messages[0].content
    assert json.loads(sent.messages[1].content)["claims"] == []
    assert provider.traces[0].total_tokens is None
    assert provider.traces[0].gateway_request_id


def test_offline_gateway_runs_through_runtime_and_records_three_call_receipts(tmp_path):
    from research.company_profile.runtime import (
        CompanyProfileResearchWriter,
        CompanyProfileStageRuntime,
    )

    client = FakeReportGatewayClient()
    provider = OfflineGatewayProvider(
        client=client,
        profile="semantic_extraction",
        max_output_tokens=4000,
        timeout_seconds=30,
    )
    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        provider=provider,
        token_budget=120_000,
    )
    item = {
        "work_id": "gateway-offline",
        "report": _report(),
        "pages": ({"page": 1, "text": TEXT},),
        "processing_identity": offline_processing_identity(),
    }
    for stage in ("acquire", "parse", "semantic", "verify", "publish"):
        result = runtime.run_stage(stage, item)
    assert len(client.requests) == 3, result


def test_existing_owner_offline_query_export_and_reload_are_consistent(tmp_path):
    from research.company_profile.operations import CompanyProfileTaskService
    from research.company_profile.reads import CompanyProfileReadService
    from tests.unit.test_research.test_business_profile_async_production import (
        _frontier,
    )
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )

    storage = _storage(tmp_path)
    _frontier(storage)
    report = _report(instrument_id="600000.SH")
    client = FakeReportGatewayClient()
    provider = OfflineGatewayProvider(
        client=client,
        profile="semantic_extraction",
        max_output_tokens=4000,
        timeout_seconds=30,
    )
    existing = _overview(report, _evidence(report))
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        page_source=lambda item: {
            "report": report,
            "pages": ({"page": 1, "text": TEXT},),
            "accepted_records": (existing,),
        },
        provider=provider,
        processing_identity=offline_processing_identity(),
        token_budget=120_000,
    )
    run_result = asyncio.run(
        service.execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )
    assert [r.purpose for r in client.semantic.requests] == [
        "source_inventory",
        "synthesize_views",
        "verify_views",
    ], run_result
    queried = asyncio.run(service.execute("query", instrument_ids=("600000.SH",)))
    assert queried["delivered"] == 1
    profile = queried["profiles"][0]
    assert profile["schema_version"].endswith("v2")
    assert len(profile["three_views"]["views"]) == 3
    assert profile["three_views"]["classification"] == "offline_fixture"
    assert not profile["three_views"]["real_semantic_complete"]
    assert not profile["core_complete"]
    assert not profile["three_views"]["semantic_complete"]
    assert profile["three_views"]["draft_complete"]
    receipts = profile["three_views"]["call_receipts"]
    assert len(receipts) == 3
    assert all(item["usage"]["total_tokens"] == 150 for item in receipts)
    assert all(item["gateway_traces"][0]["model"] == "fake-model" for item in receipts)
    exported = asyncio.run(
        service.execute(
            "export",
            instrument_ids=("600000.SH",),
            output_directory=str(tmp_path / "export"),
        )
    )
    assert exported["profiles"] == queried["profiles"]
    reloaded = CompanyProfileReadService(tmp_path / "output").query(("600000.SH",))
    assert reloaded["profiles"] == queried["profiles"]
    assert len(profile["accepted_facts"]) == 1
