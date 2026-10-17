"""Compact protocol keeps original guards; previews never count as full reading."""

import json

import jsonschema
import pytest

from research.company_profile.contracts import (
    ReportSemanticRequestV3,
    ReportSemanticResponseV3,
)
from research.company_profile.report_interpretation import (
    DEVELOPMENT_WIRE_MARGIN,
    DIMENSIONS,
    compact_processing_identity,
    compact_wire_bytes,
    pack_fitting_batches,
    page_map,
    unsampled_ranges,
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
    _evidence,
    _run,
)
from tests.unit.test_research.test_company_profile_runtime import _report
from utils.llm.budget import DEVELOPMENT_PAYLOAD_BYTE_LIMIT


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


def test_payload_batches_keep_every_page_inside_the_development_byte_cap():
    pages = tuple(
        {
            "page": page,
            "text": "年报正文" * 4000,
            "readable": True,
            "offset": 0,
            "body_scope": "full_page",
        }
        for page in range(1, 5)
    )
    identity = compact_processing_identity(discovery_scope="page_map")
    report = _report()

    def fits(batch):
        return (
            compact_wire_bytes(
                batch,
                purpose="source_inventory",
                source_scope="full_context",
                report=report,
                identity=identity,
                max_output_tokens=2500,
            )
            + DEVELOPMENT_WIRE_MARGIN
            <= DEVELOPMENT_PAYLOAD_BYTE_LIMIT
        )

    batches = pack_fitting_batches(pages, fits)
    assert len(batches) > 1
    assert "".join(page["text"] for batch in batches for page in batch) == "".join(
        page["text"] for page in pages
    )
    assert [page["page"] for batch in batches for page in batch] == [
        page["page"] for page in pages
    ]
    assert all(fits(batch) for batch in batches)


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
                            (
                                c.model_copy(
                                    update={
                                        "status": "block",
                                        "explanation": "source conflict",
                                    }
                                )
                                if c.target_type == "record"
                                else c
                            )
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


def _hidden_page(hidden: str) -> str:
    return "附加说明。" * 50 + hidden + "附加说明。" * 400


def test_preview_outside_body_is_sent_to_condition_capable_discovery():
    """A's three-page counterexample: unseen body must be offered and cannot complete.

    The blind fixture quotes the original page-1 text. On a residue-only request
    that quote is not in source, so the run stops incomplete instead of treating
    the map as coverage. The sibling test is the case where the hidden business
    is actually established and then not delivered.
    """
    hidden = "另外，本公司本期主要经营电池回收服务，占营业收入百分之六十。"
    page3 = _hidden_page(hidden)
    pages = (
        {"page": 1, "text": TEXT, "readable": True},
        {"page": 2, "text": "与第一页配套的普通说明。", "readable": True},
        {"page": 3, "text": page3, "readable": True},
    )
    mapped = page_map(pages)
    assert hidden not in mapped[2]["text"]
    assert any(
        start <= page3.index(hidden) < end for start, end in unsampled_ranges(page3)
    )
    provider = CompactSemanticFixture()
    result = _run(
        provider,
        processing_identity=compact_processing_identity(discovery_scope="page_map"),
        pages=pages,
    )
    residue_requests = [
        request
        for request in provider.requests
        if request.source_scope == "full_context"
        and any(hidden in page["text"] for page in request.source_pages)
    ]
    assert residue_requests
    assert all(
        TEXT not in page["text"]
        for request in residue_requests
        for page in request.source_pages
    )
    assert result.page_coverage[2]["status"] == "residue_discovered"
    assert any(
        chunk["scope"] == "unsampled_span"
        for chunk in result.page_coverage[2]["chunks"]
    )
    assert not result.draft_complete
    assert any(gap.startswith("semantic_incomplete:") for gap in result.gaps)


def test_hidden_major_business_blocks_completion_when_seen_but_not_delivered():
    hidden = "另外，本公司本期主要经营电池回收服务，占营业收入百分之六十。"
    pages = (
        {"page": 1, "text": TEXT, "readable": True},
        {"page": 2, "text": "与第一页配套的普通说明。", "readable": True},
        {"page": 3, "text": _hidden_page(hidden), "readable": True},
    )

    class HiddenMajor(CompactSemanticFixture):
        def interpret(self, r):
            if r.source_scope == "page_map":
                return super().interpret(r)
            if r.purpose == "source_inventory" and any(
                hidden in page["text"] for page in r.source_pages
            ):
                self.requests.append(r)
                return ReportSemanticResponseV3.model_validate(
                    {
                        "request_id": r.request_id,
                        "purpose": r.purpose,
                        "source_conditions": (
                            {
                                "condition_id": "hidden_major",
                                "dimension": "principal_business",
                                "description": "预览采样点之外的另一主要经营",
                                "evidence": (_evidence(r.report, text=hidden, page=3),),
                                "major": True,
                                "statement_status": "current",
                            },
                        ),
                    }
                )
            result = super().interpret(r)
            if r.purpose == "synthesize_views":
                return result.model_copy(
                    update={
                        "claims": tuple(
                            claim.model_copy(
                                update={
                                    "condition_ids": tuple(
                                        item
                                        for item in claim.condition_ids
                                        if "hidden_major" not in item
                                    )
                                }
                            )
                            for claim in result.claims
                        )
                    }
                )
            if r.purpose == "verify_views":
                return result.model_copy(
                    update={
                        "checks": tuple(
                            (
                                check.model_copy(
                                    update={
                                        "status": "block",
                                        "explanation": "预览外主要业务未进入交付。",
                                    }
                                )
                                if "hidden_major" in check.target_id
                                else check
                            )
                            for check in result.checks
                        )
                    }
                )
            return result

    provider = HiddenMajor()
    result = _run(
        provider,
        processing_identity=compact_processing_identity(discovery_scope="page_map"),
        pages=pages,
    )
    assert any(
        request.source_scope == "full_context"
        and any(hidden in page["text"] for page in request.source_pages)
        for request in provider.requests
    )
    assert result.page_coverage[2]["status"] == "residue_discovered"
    assert result.page_coverage[2]["status"] != "read"
    assert not result.draft_complete
    assert any("hidden_major" in gap for gap in result.gaps)
    assert not any(
        "hidden_major" in item
        for claim in result.claims
        if claim.status == "observed"
        for item in claim.condition_ids
    )


def test_long_unrelated_note_need_not_be_extracted_line_by_line():
    line = "附注明细行，与主营无关。"
    footnote = line * 80
    pages = (
        {"page": 1, "text": TEXT, "readable": True},
        {"page": 2, "text": "承接说明。", "readable": True},
        *({"page": page, "text": "短附注。", "readable": True} for page in range(3, 8)),
        {"page": 8, "text": footnote, "readable": True},
    )

    class IgnoresFootnoteLines(CompactSemanticFixture):
        def interpret(self, r):
            if r.source_scope == "page_map":
                return super().interpret(r)
            if r.purpose == "source_inventory" and all(
                TEXT not in page["text"] for page in r.source_pages
            ):
                self.requests.append(r)
                return ReportSemanticResponseV3(
                    request_id=r.request_id, purpose=r.purpose
                )
            return super().interpret(r)

    provider = IgnoresFootnoteLines()
    result = _run(
        provider,
        processing_identity=compact_processing_identity(discovery_scope="page_map"),
        pages=pages,
    )
    assert any(
        request.source_scope == "full_context"
        and any(line in page["text"] for page in request.source_pages)
        for request in provider.requests
    )
    assert result.page_coverage[7]["status"] == "residue_discovered"
    assert all(
        item["status"] == "partially_checked" for item in result.page_coverage[2:7]
    )
    assert result.draft_complete
    assert len(result.records) == 1
    assert all(
        line not in (getattr(record, "source_text", "") or "")
        for record in result.records
    )
    assert len(result.source_conditions) == len(DIMENSIONS)
