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
    ReportSemanticResponseV3,
)
from research.company_profile.models import (
    AssertionClass,
    BusinessOverview,
    ChapterTask,
    Evidence,
    LogicalSlot,
    Measurement,
    MetricType,
    PeriodType,
    SourceNativeValue,
    SubjectBasis,
    SubjectScope,
    TextAnchor,
)
from research.company_profile.report_interpretation import (
    DIMENSIONS,
    compact_processing_identity,
    core_assessment,
    interpret_report,
    layered_processing_identity,
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
        assert numeric in result.records  # reject the variant, preserve the native fact
        assert all(
            r == numeric for r in result.records if r.record_id == numeric.record_id
        )
        assert result.record_dispositions[0]["status"] == "candidate_rejected"
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


def test_native_numeric_fact_survives_model_omission_and_is_verified():
    from pydantic import TypeAdapter

    from research.company_profile.models import SemanticRecord
    from tests.unit.test_research.test_company_profile_runtime import _reference_payload

    numeric = next(
        TypeAdapter(SemanticRecord).validate_json(json.dumps(raw))
        for raw in _reference_payload()["records"]
        if raw["object_type"] == "Measurement"
    )
    provider = FakeReportProvider()
    result = _run(provider, report=numeric.report, structured_records=(numeric,))
    assert numeric in result.records
    assert numeric in provider.requests[-1].records
    assert result.draft_complete


def test_preserved_native_fact_keeps_its_source_page_in_verify_context():
    from pydantic import TypeAdapter

    from research.company_profile.models import SemanticRecord
    from tests.unit.test_research.test_company_profile_runtime import _reference_payload

    numeric = next(
        TypeAdapter(SemanticRecord).validate_json(json.dumps(raw))
        for raw in _reference_payload()["records"]
        if raw["object_type"] == "Measurement"
    )
    native_text = f"{numeric.source_native.name} {numeric.source_native.value} {numeric.source_native.unit}"
    numeric = numeric.model_copy(
        update={
            "evidence": tuple(
                e.model_copy(
                    update={"page": 3, "anchor": TextAnchor(bounded_quote=native_text)}
                )
                for e in numeric.evidence
            )
        }
    )
    provider = FakeReportProvider()
    result = _run(
        provider,
        report=numeric.report,
        structured_records=(numeric,),
        pages=(
            {"page": 1, "text": TEXT},
            {"page": 2, "text": "中间页"},
            {"page": 3, "text": native_text},
        ),
    )
    assert numeric in result.records
    assert 3 in {p["page"] for p in provider.requests[-1].source_pages}


@pytest.mark.parametrize("new_id", [False, True])
def test_segment_cannot_add_amount_even_with_a_new_candidate_id(new_id):
    from pydantic import TypeAdapter

    from research.company_profile.models import SemanticRecord
    from tests.unit.test_research.test_company_profile_runtime import _reference_payload

    original = next(
        TypeAdapter(SemanticRecord).validate_json(json.dumps(raw))
        for raw in _reference_payload()["records"]
        if raw["object_type"] == "Segment"
    )

    class AddedAmount(FakeReportProvider):
        def interpret(self, request):
            response = super().interpret(request)
            if request.purpose == "synthesize_views":
                changed = original.model_copy(
                    update={
                        "record_id": "new-segment" if new_id else original.record_id,
                        "source_native": original.source_native.model_copy(
                            update={"value": "999999999", "unit": "元"}
                        ),
                    }
                )
                return response.model_copy(
                    update={"records": (*response.records, changed)}
                )
            return response

    result = _run(AddedAmount(), report=original.report, structured_records=(original,))
    assert original in result.records
    assert all(r.source_native.value != "999999999" for r in result.records)
    assert result.record_dispositions[0]["status"] == "candidate_rejected"
    assert any("invalid_record" in g for g in result.gaps)


@pytest.mark.parametrize("incomplete_verify", [False, True])
def test_native_fact_semantic_conflict_quarantines_fact_and_dependent_answer(
    incomplete_verify,
):
    from pydantic import TypeAdapter

    from research.company_profile.models import SemanticRecord
    from tests.unit.test_research.test_company_profile_runtime import _reference_payload

    numeric = next(
        TypeAdapter(SemanticRecord).validate_json(json.dumps(raw))
        for raw in _reference_payload()["records"]
        if raw["object_type"] == "Measurement"
    )

    class ConflictingFact(FakeReportProvider):
        def interpret(self, request):
            response = super().interpret(request)
            if request.purpose == "synthesize_views":
                return response.model_copy(
                    update={
                        "claims": tuple(
                            c.model_copy(
                                update={"supporting_record_ids": (numeric.record_id,)}
                            )
                            if c.dimension == "revenue_model"
                            else c
                            for c in response.claims
                        )
                    }
                )
            if request.purpose == "verify_views":
                return response.model_copy(
                    update={
                        "checks": tuple(
                            c.model_copy(
                                update={
                                    "status": "unclear",
                                    "explanation": "同口径来源冲突待核",
                                }
                            )
                            if c.target_type == "record"
                            and c.target_id == numeric.record_id
                            else c
                            for c in response.checks
                            if not incomplete_verify
                            or c.target_id != "upstream_downstream"
                        )
                    }
                )
            return response

    result = _run(
        ConflictingFact(), report=numeric.report, structured_records=(numeric,)
    )
    assert numeric not in result.records
    if not incomplete_verify:
        assert result.records  # unrelated, verified overview survives
    assert not result.draft_complete
    assert (
        next(c for c in result.claims if c.dimension == "revenue_model").status
        == "unclear"
    )
    assert result.record_dispositions[0]["reason"] == "同口径来源冲突待核"


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


def test_nonempty_chain_survives_failover_publish_and_a_later_failure(tmp_path):
    """Offline owner path: real client, simulated transport, original writer.

    This is not a real-model score. It checks that nonempty conditions, claims,
    evidence and receipts stay readable after failover, output overshoot,
    verify/publish, and a later discovery failure.
    """
    import time

    from research.company_profile.operations import CompanyProfileTaskService
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.report_interpretation import (
        compact_processing_identity,
    )
    from tests.unit.test_research.test_business_profile_async_production import (
        _frontier,
    )
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )
    from tests.unit.test_utils.test_llm_physical_budget import _client
    from tests.unit.test_utils.test_llm_routed_client import _config as routed_config
    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget
    from utils.llm.errors import LlmTransientTransportError
    from utils.llm.testing import ScriptedTransport

    report = _report(instrument_id="600000.SH")
    page_text = f"{TEXT}云运维业务营业收入10万元。"
    pages = (
        {"page": 1, "text": page_text, "readable": True},
        {"page": 2, "text": "中间页不在本次切片。", "readable": True},
        {"page": 3, "text": "后续发现页，不应抹掉已核主张。", "readable": True},
    )
    identity = compact_processing_identity(discovery_scope="page_map")
    identity.update(
        partial_slice=True,
        ledger_tokens=120_000,
        max_physical_requests=8,
        continue_after_verify=True,
        parent_attempt="offline-fixture",
        navigation_receipts=(
            {
                "status": "success",
                "source_scope": "page_map",
                "purpose": "source_inventory",
                "prompt_version": identity["prompt_version"],
                "source_pages": [1, 2, 3],
                "response_payload": {
                    "purpose": "source_inventory",
                    "requested_pages": [1, 3],
                    "source_conditions": [],
                },
            },
        ),
    )
    quote = TEXT[:24]
    native_fact = Measurement(
        record_id="rev-cloud",
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        report=report,
        subject_scope=SubjectScope.ISSUER,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        knowledge_time=report.published_at,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(report, text="云运维业务营业收入10万元。", page=1),),
        source_native=SourceNativeValue(
            name="云运维业务营业收入", value="10", unit="万元"
        ),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object="云运维业务",
    )

    def reply(raw):
        purpose = raw["purpose"]
        base = {
            "schema_version": "company_profile_report_response.v3",
            "request_id": raw["request_id"],
            "purpose": purpose,
        }
        if purpose == "source_inventory":
            page = next(item for item in raw["source_pages"] if item["text"].strip())
            base.update(
                requested_pages=[],
                source_conditions=[
                    {
                        "condition_id": "biz",
                        "dimension": "principal_business",
                        "description": "来源写明的经营事项",
                        "evidence": [{"page": page["page"], "quote": quote}],
                        "major": True,
                        "statement_status": "current",
                    }
                ],
            )
            return base
        if purpose == "synthesize_views":
            condition = raw["source_conditions"][0]
            base["claims"] = [
                {
                    "claim_id": "claim-biz",
                    "dimension": "principal_business",
                    "status": "observed",
                    "answer": "来源写明公司经营云运维与工业设计。",
                    "condition_ids": [condition["condition_id"]],
                    "supporting_record_ids": [
                        item["record_id"] for item in raw.get("records", [])
                    ],
                    "evidence": condition.get("evidence_refs") or condition["evidence"],
                }
            ]
            return base
        base["checks"] = [
            {
                "target_type": kind,
                "target_id": item[key],
                "status": "pass",
                "explanation": "离线链核验，不是真实年报结论。",
            }
            for kind, values, key in (
                ("condition", raw.get("source_conditions", []), "condition_id"),
                ("claim", raw.get("claims", []), "claim_id"),
                ("record", raw.get("records", []), "record_id"),
            )
            for item in values
        ]
        return base

    from utils.llm.transport import TransportResponse

    class ReplyTransport(ScriptedTransport):
        def __init__(self):
            super().__init__([])
            self.inventory = 0

        async def send(self, url, headers, payload, timeout_seconds):
            self.calls.append({"payload": dict(payload)})
            raw = json.loads(
                next(m["content"] for m in payload["messages"] if m["role"] == "user")
            )
            if len(self.calls) == 1:
                raise LlmTransientTransportError("offline")
            if raw["purpose"] == "source_inventory":
                self.inventory += 1
                if self.inventory > 1:
                    raise LlmTransientTransportError("later-discovery")
            output = 6000 if raw["purpose"] == "source_inventory" else 40
            return TransportResponse(
                status_code=200,
                data={
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(reply(raw), ensure_ascii=False)
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 100,
                        "completion_tokens": output,
                        "total_tokens": 100 + output,
                    },
                },
            )

    transport = ReplyTransport()
    client = _client(
        transport,
        None,
        routed_config(timeout_seconds=10, attempt_timeout_seconds=5),
    )
    budget = LlmRunBudget(
        deadline=time.monotonic() + 60,
        prices={},
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        max_tokens=120_000,
        max_requests=8,
    )
    client.physical_budget = budget
    provider = OfflineGatewayProvider(
        client=client,
        profile="semantic",
        max_output_tokens=4000,
        timeout_seconds=30,
    )
    storage = _storage(tmp_path)
    _frontier(storage)
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        page_source=lambda item: {
            "report": report,
            "pages": pages,
            "accepted_records": (native_fact,),
        },
        provider=provider,
        processing_identity=identity,
        execution_budget=budget,
        token_budget=120_000,
    )
    budget.deadline = time.monotonic() + 60
    run_result = asyncio.run(
        service.execute("run", knowledge_cutoff="2026-08-30", max_items=1)
    )
    queried = asyncio.run(service.execute("query", instrument_ids=("600000.SH",)))
    assert queried["delivered"] == 1, json.dumps(
        {
            "state": run_result.get("state") if isinstance(run_result, dict) else None,
            "drain": run_result.get("drain") if isinstance(run_result, dict) else None,
            "queue": run_result.get("queue") if isinstance(run_result, dict) else None,
        },
        ensure_ascii=False,
        default=str,
    )[:2500]
    exported = asyncio.run(
        service.execute(
            "export",
            instrument_ids=("600000.SH",),
            output_directory=str(tmp_path / "export"),
        )
    )
    assert exported["profiles"] == queried["profiles"]
    assert queried["delivered"] == 1
    profile = queried["profiles"][0]
    views = profile["three_views"]
    assert views["draft_complete"] is False
    assert views["semantic_complete"] is False
    assert views["real_semantic_complete"] is False
    assert views["classification"] == "offline_fixture"
    observed = [
        claim
        for view in views["views"].values()
        for claim in view["claims"]
        if claim["status"] == "observed"
    ]
    assert observed, (
        [(c.get("status"), c.get("claim_id")) for v in views["views"].values() for c in v["claims"]],
        [g for g in views["gaps"] if not str(g).startswith("unsampled")],
    )
    assert quote in json.dumps(observed, ensure_ascii=False)
    body = [
        item
        for item in views["call_receipts"]
        if item.get("source_scope") == "full_context"
        and item.get("purpose") == "source_inventory"
        and item.get("status") == "success"
    ]
    assert body and body[0]["response_payload"]["source_conditions"]
    assert any("later-discovery" in gap or "semantic_incomplete" in gap for gap in views["gaps"])
    saved = json.loads(
        next((tmp_path / "output").rglob("checkpoints/*.json")).read_text()
    )
    saved_identity = saved["report_interpretation"]["processing_identity"]
    assert "partial_plan" not in saved_identity
    from research.company_profile.runtime import _reading_policy

    assert _reading_policy(saved_identity) == _reading_policy(identity)
    reloaded = CompanyProfileReadService(tmp_path / "output").query(("600000.SH",))
    assert reloaded["profiles"] == queried["profiles"]
    inventory_usage = [
        item
        for item in budget.snapshot()["attempts"]
        if item.get("usage") and item["usage"]["output_tokens"] == 6000
    ]
    assert inventory_usage
    assert inventory_usage[0]["charged_tokens"] == 6100
    assert len({item["provider_resource"] for item in budget.snapshot()["attempts"][:2]}) == 2
    assert any(item["record_id"] == "rev-cloud" for item in profile["accepted_facts"])
    followups = []
    for call in transport.calls:
        raw = json.loads(
            next(
                message["content"]
                for message in call["payload"]["messages"]
                if message["role"] == "user"
            )
        )
        if raw["purpose"] in {"synthesize_views", "verify_views"}:
            followups.append(raw)
    assert followups
    assert all(
        "rev-cloud" in json.dumps(item, ensure_ascii=False) for item in followups
    )


def _span_measurement(report, record_id, quote, page):
    return Measurement(
        record_id=record_id,
        field_id="operating_revenue",
        chapter_task=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        report=report,
        subject_scope=SubjectScope.ISSUER,
        reported_period=report.report_period,
        period_type=PeriodType.DURATION,
        knowledge_time=report.published_at,
        assertion_class=AssertionClass.REPORTED_FACT,
        evidence=(_evidence(report, text=quote, page=page),),
        source_native=SourceNativeValue(name=quote, value="10", unit="万元"),
        metric_type=MetricType.OPERATING_REVENUE,
        logical_slot=LogicalSlot.REVENUE,
        measured_object="有线电视网络",
    )


def _partial_identity(*, ledger_tokens=120_000):
    identity = compact_processing_identity(discovery_scope="page_map")
    identity.update(
        partial_slice=True,
        ledger_tokens=ledger_tokens,
        max_physical_requests=8,
        navigation_receipts=(
            {
                "status": "success",
                "source_scope": "page_map",
                "purpose": "source_inventory",
                "prompt_version": identity["prompt_version"],
                "source_pages": [1, 2],
                "response_payload": {
                    "purpose": "source_inventory",
                    "requested_pages": [1],
                    "source_conditions": [],
                },
            },
        ),
    )
    return identity


class _SliceProvider(FakeReportProvider):
    def __init__(self, *, conflict=False, huge=False):
        super().__init__()
        self.conflict = conflict
        self.huge = huge

    def interpret(self, request):
        self.requests.append(request)
        evidence = _evidence(request.report, text="公司经营有线电视网络传输", page=1)
        if request.purpose == "source_inventory":
            return ReportSemanticResponseV3.model_validate(
                {
                    "request_id": request.request_id,
                    "purpose": request.purpose,
                    "requested_pages": (),
                    "source_conditions": (
                        {
                            "condition_id": "biz",
                            "dimension": "principal_business",
                            "description": (
                                "超界" * 5000 if self.huge else "来源写明网络传输"
                            ),
                            "evidence": (evidence,),
                            "major": True,
                            "statement_status": "current",
                        },
                    ),
                }
            )
        if request.purpose == "synthesize_views":
            condition_id = request.source_conditions[0].condition_id
            return ReportSemanticResponseV3.model_validate(
                {
                    "request_id": request.request_id,
                    "purpose": request.purpose,
                    "claims": (
                        {
                            "claim_id": "claim-biz",
                            "dimension": "principal_business",
                            "status": "observed",
                            "answer": "公司经营有线电视网络传输并提供信息服务。",
                            "condition_ids": (condition_id,),
                            "supporting_record_ids": ("rev-in",),
                            "evidence": (evidence,),
                        },
                    ),
                }
            )
        return ReportSemanticResponseV3.model_validate(
            {
                "request_id": request.request_id,
                "purpose": request.purpose,
                "checks": tuple(
                    {
                        "target_type": kind,
                        "target_id": item,
                        "status": (
                            "unclear"
                            if self.conflict and kind == "record" and item == "rev-in"
                            else "pass"
                        ),
                        "explanation": "离线核验",
                    }
                    for kind, values in (
                        (
                            "condition",
                            [c.condition_id for c in request.source_conditions],
                        ),
                        ("claim", [c.claim_id for c in request.claims]),
                        ("record", [c.record_id for c in request.records]),
                    )
                    for item in values
                ),
            }
        )


def _slice_pages():
    return (
        {"page": 1, "text": "公司经营有线电视网络传输，并向用户提供信息服务。", "readable": True},
        {"page": 2, "text": "第18页收入表不在本次阅读范围。", "readable": True},
    )


def test_partial_slice_verifies_only_records_inside_the_read_span():
    report = _report()
    inside = _span_measurement(report, "rev-in", "公司经营有线电视网络传输", 1)
    outside = _span_measurement(report, "rev-out", "第18页收入表不在本次阅读范围。", 2)
    provider = _SliceProvider()
    result = _run(
        provider,
        report=report,
        pages=_slice_pages(),
        processing_identity=_partial_identity(),
        structured_records=(inside, outside),
    )
    verify = next(item for item in provider.requests if item.purpose == "verify_views")
    assert [item.record_id for item in verify.records] == ["rev-in"]
    assert any(item.record_id == "rev-in" for item in result.records)
    assert all(item.record_id != "rev-out" for item in result.records)
    assert "native_fact_outside_read_span:rev-out" in result.gaps
    assert result.real_semantic_complete is False


def test_in_span_record_conflict_isolates_the_dependent_claim():
    report = _report()
    inside = _span_measurement(report, "rev-in", "公司经营有线电视网络传输", 1)
    provider = _SliceProvider(conflict=True)
    result = _run(
        provider,
        report=report,
        pages=_slice_pages(),
        processing_identity=_partial_identity(),
        structured_records=(inside,),
    )
    assert all(item.record_id != "rev-in" for item in result.records)
    claim = next(item for item in result.claims if item.claim_id == "claim-biz")
    assert claim.status == "unclear"
    assert any("record_semantic_conflict:rev-in" in gap for gap in result.gaps)


def test_followup_over_citation_ceiling_keeps_the_body_response():
    import time

    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget

    report = _report()
    inside = _span_measurement(report, "rev-in", "公司经营有线电视网络传输", 1)
    provider = _SliceProvider(huge=True)
    budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        max_tokens=40_000,
        max_requests=8,
    )
    result = _run(
        provider,
        report=report,
        pages=_slice_pages(),
        processing_identity=_partial_identity(ledger_tokens=40_000),
        structured_records=(inside,),
        execution_budget=budget,
    )
    assert [item.purpose for item in provider.requests] == ["source_inventory"]
    body = next(
        item
        for item in result.call_receipts
        if item.get("purpose") == "source_inventory"
        and item.get("source_scope") != "page_map"
    )
    assert body["status"] == "success"
    assert body["response_payload"]["source_conditions"]
    assert all(item.status != "observed" for item in result.claims)
    assert any(
        "partial_tail_hold_would_be_exhausted" in gap for gap in result.gaps
    )


def test_text_claim_with_empty_record_ids_verifies_and_asset_id_stays_unclear():
    report = _report()
    pages = _slice_pages()
    identity = _partial_identity()

    class _Claims(FakeReportProvider):
        def __init__(self, record_ids):
            super().__init__()
            self.record_ids = record_ids

        def interpret(self, request):
            self.requests.append(request)
            evidence = _evidence(request.report, text="公司经营有线电视网络传输", page=1)
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3.model_validate(
                    {
                        "request_id": request.request_id,
                        "purpose": request.purpose,
                        "requested_pages": (),
                        "source_conditions": (
                            {
                                "condition_id": "biz",
                                "dimension": "principal_business",
                                "description": "来源写明网络传输",
                                "evidence": (evidence,),
                                "major": True,
                                "statement_status": "current",
                            },
                        ),
                    }
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3.model_validate(
                    {
                        "request_id": request.request_id,
                        "purpose": request.purpose,
                        "claims": (
                            {
                                "claim_id": "claim-biz",
                                "dimension": "principal_business",
                                "status": "observed",
                                "answer": "公司经营有线电视网络传输。",
                                "condition_ids": (
                                    request.source_conditions[0].condition_id,
                                ),
                                "supporting_record_ids": self.record_ids,
                                "evidence": (evidence,),
                            },
                        ),
                    }
                )
            return ReportSemanticResponseV3.model_validate(
                {
                    "request_id": request.request_id,
                    "purpose": request.purpose,
                    "checks": tuple(
                        {
                            "target_type": kind,
                            "target_id": item,
                            "status": "pass",
                            "explanation": "离线核验",
                        }
                        for kind, values in (
                            (
                                "condition",
                                [c.condition_id for c in request.source_conditions],
                            ),
                            ("claim", [c.claim_id for c in request.claims]),
                            ("record", [c.record_id for c in request.records]),
                        )
                        for item in values
                    ),
                }
            )

    legal = _run(
        _Claims(()),
        report=report,
        pages=pages,
        processing_identity=identity,
    )
    assert any(claim.status == "observed" for claim in legal.claims)
    blocked = _run(
        _Claims((report.report_id,)),
        report=report,
        pages=pages,
        processing_identity=identity,
    )
    assert all(claim.status == "unclear" for claim in blocked.claims)
    assert any("invalid_claim_reference" in gap for gap in blocked.gaps)


def test_saved_segment_responses_reach_verification_and_keep_bad_record_ids_unclear(
    tmp_path,
):
    """Offline replay of the saved segment responses. Not a real verification score."""
    import time
    from pathlib import Path

    from research.company_profile.models import ReportIdentity
    from research.company_profile.operations import CompanyProfileTaskService
    from research.company_profile.reads import CompanyProfileReadService
    from tests.unit.test_research.test_business_profile_async_production import (
        _announcement,
        _frontier,
    )
    from tests.unit.test_research.test_business_profile_exposure_components import (
        _storage,
    )
    from tests.unit.test_utils.test_llm_physical_budget import _client
    from tests.unit.test_utils.test_llm_routed_client import _config as routed_config
    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget
    from utils.llm.testing import ScriptedTransport
    from utils.llm.transport import TransportResponse

    change = (
        Path(__file__).resolve().parents[3]
        / "openspec/changes/restore-company-profile-llm-led-generalization"
    )
    saved = json.loads(
        (change / "b1-pool-observation-evidence/segment/work-checkpoint.json").read_text()
    )
    payloads = {
        item["purpose"]: item["response_payload"]
        for item in saved["report_interpretation"]["call_receipts"]
        if item.get("source_scope") == "full_context"
    }
    report = ReportIdentity.model_validate(saved["report"])
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(str(page.get("text") or "").strip()),
        }
        for page in saved["pages"]
    )
    identity = dict(saved["report_interpretation"]["processing_identity"])
    identity.update(execution_mode="offline_fake", model="offline_replay")

    def compact_evidence(item):
        anchor = item.get("anchor") or {}
        return {"page": item["page"], "quote": anchor["bounded_quote"]}

    def compact_response(raw, request_id):
        purpose = raw["purpose"]
        body = {
            "schema_version": "company_profile_report_response.v3",
            "request_id": request_id,
            "purpose": purpose,
        }
        if purpose == "source_inventory":
            body["requested_pages"] = []
            body["source_conditions"] = [
                {
                    "condition_id": item["condition_id"],
                    "dimension": item["dimension"],
                    "description": item["description"],
                    "major": item["major"],
                    "statement_status": item["statement_status"],
                    "evidence": [compact_evidence(evidence) for evidence in item["evidence"]],
                }
                for item in raw["source_conditions"]
            ]
        elif purpose == "synthesize_views":
            body["claims"] = []
            for item in raw["claims"]:
                evidence = [
                    compact_evidence(evidence)
                    for evidence in item.get("evidence") or []
                    if isinstance(evidence, dict) and evidence.get("anchor")
                ]
                body["claims"].append(
                    {
                        "claim_id": item["claim_id"],
                        "dimension": item["dimension"],
                        "status": item["status"],
                        "answer": item["answer"],
                        "condition_ids": list(item.get("condition_ids") or []),
                        "supporting_record_ids": list(item.get("supporting_record_ids") or []),
                        "evidence": evidence,
                    }
                )
        return body

    class ReplayTransport(ScriptedTransport):
        def __init__(self):
            super().__init__([])

        async def send(self, url, headers, payload, timeout_seconds):
            self.calls.append({"payload": dict(payload)})
            raw = json.loads(
                next(message["content"] for message in payload["messages"] if message["role"] == "user")
            )
            saved_payload = payloads.get(raw["purpose"])
            if saved_payload is None:
                checks = [
                    {
                        "target_type": kind,
                        "target_id": item["condition_id" if kind == "condition" else "claim_id" if kind == "claim" else "record_id"],
                        "status": "pass",
                        "explanation": "离线回放核验，不是真实年报结论。",
                    }
                    for kind, key in (
                        ("condition", "source_conditions"),
                        ("claim", "claims"),
                        ("record", "records"),
                    )
                    for item in raw.get(key) or []
                ]
                content = {
                    "schema_version": "company_profile_report_response.v3",
                    "request_id": raw["request_id"],
                    "purpose": raw["purpose"],
                    "checks": checks,
                }
                usage = {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
            else:
                content = compact_response(saved_payload, raw["request_id"])
                if raw["purpose"] == "synthesize_views":
                    by_suffix = {
                        item["condition_id"].split(":")[-1]: item["condition_id"]
                        for item in raw.get("source_conditions") or []
                    }
                    for claim in content["claims"]:
                        claim["condition_ids"] = [
                            by_suffix.get(item.split(":")[-1], item)
                            for item in claim["condition_ids"]
                        ]
                    usage = {
                        "prompt_tokens": 1936,
                        "completion_tokens": 4781,
                        "total_tokens": 6717,
                    }
                else:
                    usage = {
                        "prompt_tokens": 1239,
                        "completion_tokens": 1073,
                        "total_tokens": 2312,
                    }
            return TransportResponse(
                status_code=200,
                data={
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(content, ensure_ascii=False)
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": usage,
                },
            )

    transport = ReplayTransport()
    client = _client(
        transport,
        None,
        routed_config(timeout_seconds=10, attempt_timeout_seconds=5),
    )
    budget = LlmRunBudget(
        deadline=time.monotonic() + 180,
        prices={},
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        max_tokens=40_000,
        max_requests=6,
    )
    client.physical_budget = budget
    provider = OfflineGatewayProvider(
        client=client,
        profile="semantic",
        max_output_tokens=4000,
        timeout_seconds=30,
    )
    storage = _storage(tmp_path)
    repository, _ignored = _frontier(storage)
    repository.upsert_record(
        instrument={
            "instrument_id": report.instrument_id,
            "symbol": report.instrument_id.split(".")[0],
            "exchange": "SSE",
        },
        record=_announcement(
            "annual-2025-gehua",
            "歌华有线2025年年度报告",
            published_at="2026-04-28T16:00:00+00:00",
        ),
    )
    service = CompanyProfileTaskService(
        storage=storage,
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        page_source=lambda item: {"report": report, "pages": pages},
        provider=provider,
        processing_identity=identity,
        execution_budget=budget,
        token_budget=40_000,
    )
    budget.deadline = time.monotonic() + 180
    run_result = asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-09-17",
            instrument_ids=(report.instrument_id,),
            max_items=1,
        )
    )
    queried = asyncio.run(service.execute("query", instrument_ids=(report.instrument_id,)))
    drain = run_result.get("drain") if isinstance(run_result, dict) else {}
    stage_errors = {
        name: {
            "status": stage.get("status"),
            "errors": stage.get("errors"),
            "completed": stage.get("completed"),
            "terminal_failures": stage.get("terminal_failures"),
        }
        for name, stage in (drain or {}).items()
        if isinstance(stage, dict)
    }
    assert queried["delivered"] == 1, json.dumps(
        {"state": run_result.get("state"), "stages": stage_errors},
        ensure_ascii=False,
        default=str,
    )[:4000]
    exported = asyncio.run(
        service.execute(
            "export",
            instrument_ids=(report.instrument_id,),
            output_directory=str(tmp_path / "export"),
        )
    )
    assert exported["profiles"] == queried["profiles"]
    assert queried["delivered"] == 1
    profile = queried["profiles"][0]
    views = profile["three_views"]
    assert views["draft_complete"] is False
    assert views["semantic_complete"] is False
    assert views["real_semantic_complete"] is False
    claims = [claim for view in views["views"].values() for claim in view["claims"]]
    assert len(claims) == 7
    assert all(claim["status"] == "unclear" for claim in claims)
    assert any("invalid_claim_reference" in gap for gap in views["gaps"])
    purposes = [item.get("purpose") for item in views["call_receipts"] if item.get("source_scope") == "full_context"]
    assert purposes == ["source_inventory", "synthesize_views", "verify_views"]
    verify = next(item for item in transport.calls if json.loads(next(m["content"] for m in item["payload"]["messages"] if m["role"] == "user"))["purpose"] == "verify_views")
    verify_raw = json.loads(next(m["content"] for m in verify["payload"]["messages"] if m["role"] == "user"))
    assert {page["page"] for page in verify_raw["source_pages"]} == {12}
    assert any(
        report.report_id in claim.get("supporting_record_ids", [])
        for claim in verify_raw["claims"]
    )
    reloaded = CompanyProfileReadService(tmp_path / "output").query((report.instrument_id,))
    assert reloaded["profiles"] == queried["profiles"]


def test_layered_reading_supplements_a_hidden_business_and_labels_unread_notes():
    pages = (
        {"page": 1, "text": "公司经营有线电视网络传输，并向用户提供信息服务。", "readable": True},
        {"page": 2, "text": "附注：会计政策说明，不是本期主要经营业务。", "readable": True},
        {
            "page": 3,
            "text": "本公司本期主要经营电池回收服务，占营业收入百分之六十。",
            "readable": True,
        },
    )
    identity = layered_processing_identity()
    identity["max_physical_requests"] = 16

    class _Layered(FakeReportProvider):
        def __init__(self, *, find_hidden: bool):
            super().__init__()
            self.find_hidden = find_hidden
            self.maps = 0

        def interpret(self, request):
            self.requests.append(request)
            if request.source_scope == "page_map":
                self.maps += 1
                chosen = [3] if self.find_hidden and self.maps > 1 else [1]
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=tuple(chosen),
                )
            evidence_text = request.source_pages[0]["text"]
            hidden = "电池回收" in evidence_text
            evidence = _evidence(
                request.report,
                text=(
                    "本公司本期主要经营电池回收服务"
                    if hidden
                    else "公司经营有线电视网络传输"
                ),
                page=int(request.source_pages[0]["page"]),
            )
            if request.purpose == "source_inventory":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(),
                    source_conditions=(
                        {
                            "condition_id": "hidden" if hidden else "network",
                            "dimension": "principal_business",
                            "description": (
                                "来源写明电池回收是本期主要经营"
                                if hidden
                                else "来源写明网络传输"
                            ),
                            "evidence": (evidence,),
                            "major": True,
                            "statement_status": "current",
                        },
                    ),
                )
            if request.purpose == "synthesize_views":
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    claims=tuple(
                        {
                            "claim_id": f"claim-{condition.condition_id}",
                            "dimension": "principal_business",
                            "status": "observed",
                            "answer": condition.description,
                            "condition_ids": (condition.condition_id,),
                            "supporting_record_ids": (),
                            "evidence": condition.evidence,
                        }
                        for condition in request.source_conditions
                    ),
                )
            return ReportSemanticResponseV3(
                request_id=request.request_id,
                purpose=request.purpose,
                checks=tuple(
                    {
                        "target_type": kind,
                        "target_id": item,
                        "status": "pass",
                        "explanation": "离线核验",
                    }
                    for kind, values in (
                        (
                            "condition",
                            [item.condition_id for item in request.source_conditions],
                        ),
                        ("claim", [item.claim_id for item in request.claims]),
                        ("record", [item.record_id for item in request.records]),
                    )
                    for item in values
                ),
            )

    found = _run(
        _Layered(find_hidden=True),
        pages=pages,
        processing_identity=identity,
    )
    assert any("电池回收" in condition.description for condition in found.source_conditions)
    assert any("supplement_unread:2" == gap for gap in found.gaps)
    assert not any(gap.startswith("unsampled_body_not_discovered:") for gap in found.gaps)
    missed = _run(
        _Layered(find_hidden=False),
        pages=pages,
        processing_identity=identity,
    )
    assert not any("电池回收" in condition.description for condition in missed.source_conditions)
    assert any(gap == "supplement_unread:3" for gap in missed.gaps)
    assert missed.draft_complete is False
