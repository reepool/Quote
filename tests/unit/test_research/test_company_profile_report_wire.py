"""Compact protocol keeps original guards; previews never count as full reading."""

import json

import jsonschema
import pytest

from research.company_profile.contracts import (
    ReportSemanticRequestV3,
    ReportSemanticResponseV3,
)
from research.company_profile.report_interpretation import (
    compact_processing_identity,
    page_map,
)
from research.company_profile.report_wire import (
    compact_request,
    expand_response,
    response_schema,
)
from research.company_profile.resource_probe import protocol_reply
from tests.unit.test_research.test_company_profile_llm_led import (
    TEXT,
    FakeReportProvider,
    _run,
)
from tests.unit.test_research.test_company_profile_runtime import _report


def request(purpose="source_inventory", **kwargs):
    return ReportSemanticRequestV3(
        request_id="test:v3",
        purpose=purpose,
        report=_report(),
        processing_identity=compact_processing_identity(discovery_scope="page_map"),
        source_pages=({"page": 1, "text": TEXT, "readable": True},),
        max_output_tokens=4000,
        timeout_seconds=10.0,
        **kwargs,
    )


def test_purpose_schema_excludes_unreachable_facts_and_validates_nonempty_replies():
    inventory = request()
    reply = protocol_reply(compact_request(inventory))
    jsonschema.validate(reply, response_schema(inventory))
    expanded = ReportSemanticResponseV3.model_validate_json(
        json.dumps(expand_response(reply, inventory))
    )
    assert len(expanded.source_conditions) == 14
    synthesis = request(
        "synthesize_views", source_conditions=expanded.source_conditions
    )
    reply = protocol_reply(compact_request(synthesis))
    jsonschema.validate(reply, response_schema(synthesis))
    expanded_claims = ReportSemanticResponseV3.model_validate_json(
        json.dumps(expand_response(reply, synthesis))
    )
    assert len(expanded_claims.claims) == 7
    verify = request(
        "verify_views",
        source_conditions=expanded.source_conditions,
        claims=expanded_claims.claims,
    )
    reply = protocol_reply(compact_request(verify))
    jsonschema.validate(reply, response_schema(verify))
    assert len(expand_response(reply, verify)["checks"]) == 21
    for r, forbidden in (
        (inventory, "claims"),
        (synthesis, "records"),
        (verify, "source_conditions"),
    ):
        schema = response_schema(r)
        assert forbidden not in schema["properties"]
        assert "ReportIdentity" not in schema.get("$defs", {})
        assert "Measurement" not in schema.get("$defs", {})
        assert len(json.dumps(schema)) < 6500


def test_shared_evidence_is_sent_once_and_unknown_reference_rejected():
    r = request()
    first = expand_response(protocol_reply(compact_request(r)), r)
    conditions = ReportSemanticResponseV3.model_validate_json(
        json.dumps(first)
    ).source_conditions
    synthesis = request("synthesize_views", source_conditions=conditions)
    packed = compact_request(synthesis)
    assert len(packed["evidence_catalog"]) == 1
    assert all(
        "evidence_refs" in c and "evidence" not in c
        for c in packed["source_conditions"]
    )
    reply = protocol_reply(packed)
    reply["claims"][0]["evidence"] = ["not-in-catalog"]
    with pytest.raises(ValueError, match="unknown compact Evidence"):
        expand_response(reply, synthesis)


def test_wire_cannot_insert_measurements_or_treat_preview_as_source_condition():
    r = request(source_scope="page_map")
    reply = protocol_reply(compact_request(r))
    reply["records"] = [{"record_id": "invented-number", "value": "999999999"}]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(reply, response_schema(r))
    reply = protocol_reply(compact_request(request()))
    with pytest.raises(ValueError, match="page map"):
        expand_response(reply, r)


class CompactSemanticFixture(FakeReportProvider):
    def interpret(self, r):
        if r.source_scope == "page_map":
            self.requests.append(r)
            return ReportSemanticResponseV3(
                request_id=r.request_id, purpose=r.purpose, requested_pages=(1,)
            )
        result = super().interpret(r).model_dump(mode="json")
        result["schema_version"] = "company_profile_report_response.v3"
        return ReportSemanticResponseV3.model_validate_json(json.dumps(result))


@pytest.mark.parametrize("scope", ["full_text", "page_map"])
def test_compact_plan_preserves_source_first_omission_checks_and_partial_coverage(
    scope,
):
    p = CompactSemanticFixture(omit_major=True)
    result = _run(
        p,
        processing_identity=compact_processing_identity(discovery_scope=scope),
        pages=(
            {"page": 1, "text": TEXT},
            {"page": 2, "text": TEXT},
            {"page": 3, "text": "陌生章节及无匹配标题内容，仍应对模型可见。"},
        ),
    )
    assert not result.draft_complete
    assert any("major_condition_not_delivered" in gap for gap in result.gaps)
    if scope == "page_map":
        assert [x["page"] for x in p.requests[0].source_pages] == [1, 2, 3]
        assert not p.requests[0].records and not p.requests[0].source_conditions
        assert result.page_coverage[2]["status"] == "partially_checked"
        assert result.page_coverage[2]["map_checked"]


def test_map_indexes_arbitrary_pages_without_claiming_unsampled_body_read():
    pages = (
        {"page": i, "text": (f"陌生内容{i}" * 200), "readable": True}
        for i in range(1, 9)
    )
    mapped = page_map(tuple(pages))
    assert len(mapped) == 8
    assert all(p["scope"] == "structure_preview_only" for p in mapped)
    assert all(p["source_characters"] > len(p["text"]) for p in mapped)
    assert all(len(p["sample_offsets"]) == 3 for p in mapped)


def test_actual_v3_gateway_nonempty_all_purposes_share_unknown_usage_budget():
    import asyncio
    import time
    from decimal import Decimal

    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_utils.test_llm_gateway import _config
    from utils.llm import CallableTransport, LlmClient
    from utils.llm.budget import LlmPrice, LlmRunBudget

    requests = []

    async def send(_url, _headers, payload, _timeout):
        raw = json.loads(
            next(m["content"] for m in payload["messages"] if m["role"] == "user")
        )
        requests.append(raw)
        return {
            "data": {
                "choices": [{"message": {"content": json.dumps(protocol_reply(raw))}}]
            }
        }

    budget = LlmRunBudget(
        deadline=time.monotonic() + 15,
        prices={
            ("openai_compatible:TEST_LLM_KEY", "test-model"): LlmPrice(
                Decimal(1), Decimal(2), "synthetic", 120000
            )
        },
    )
    client = LlmClient(
        _config(max_retries=0, max_schema_repair_attempts=0, timeout_seconds=10),
        transport=CallableTransport(send),
        environment={"TEST_LLM_KEY": "fake"},
        physical_budget=budget,
    )

    class Probe(CommonGatewaySemanticProvider):
        offline_only = True

    provider = Probe(
        client=client,
        profile="test",
        max_output_tokens=4000,
        timeout_seconds=10,
    )
    try:
        result = _run(
            provider,
            processing_identity=compact_processing_identity(discovery_scope="page_map"),
            execution_budget=budget,
        )
    finally:
        asyncio.run(client.close())
    assert [r["purpose"] for r in requests] == [
        "source_inventory",
        "source_inventory",
        "synthesize_views",
        "verify_views",
    ]
    assert len(result.source_conditions) == 14 and len(result.claims) == 7
    assert len(result.checks) == 21
    assert all(
        a["usage_status"] == "unknown_reserved" for a in budget.snapshot()["attempts"]
    )
    assert not result.real_semantic_complete and not result.draft_complete


@pytest.mark.parametrize("conflict", [False, True])
def test_compact_reading_keeps_native_fact_or_quarantines_conflict_and_dependencies(
    conflict,
):
    from pydantic import TypeAdapter

    from research.company_profile.models import SemanticRecord
    from tests.unit.test_research.test_company_profile_runtime import _reference_payload

    numeric = next(
        TypeAdapter(SemanticRecord).validate_json(json.dumps(r))
        for r in _reference_payload()["records"]
        if r["object_type"] == "Measurement"
    )

    class Native(CompactSemanticFixture):
        def interpret(self, r):
            result = super().interpret(r)
            if r.purpose == "synthesize_views":
                return result.model_copy(
                    update={
                        "records": (),
                        "claims": tuple(
                            c.model_copy(
                                update={"supporting_record_ids": (numeric.record_id,)}
                            )
                            for c in result.claims
                        ),
                    }
                )
            if conflict and r.purpose == "verify_views":
                return result.model_copy(
                    update={
                        "checks": tuple(
                            c.model_copy(
                                update={
                                    "status": "block",
                                    "explanation": "source conflict",
                                }
                            )
                            if c.target_type == "record"
                            else c
                            for c in result.checks
                        )
                    }
                )
            return result

    result = _run(
        Native(),
        report=numeric.report,
        structured_records=(numeric,),
        processing_identity=compact_processing_identity(discovery_scope="full_text"),
    )
    assert (numeric in result.records) is not conflict
    if conflict:
        assert all(c.status == "unclear" for c in result.claims)
        assert result.record_dispositions[0]["status"] == "quarantined"
