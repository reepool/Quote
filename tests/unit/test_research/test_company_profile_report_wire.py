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
    fits_development_payload,
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


def test_gehu_batches_keep_failover_synthesis_and_verify_after_one_unknown_failure():
    import gzip
    import time
    from pathlib import Path

    from research.company_profile.models import ReportIdentity
    from research.company_profile.report_interpretation import (
        cited_quote_pages,
        structural_discovery_chunks,
    )
    from tests.unit.test_utils.test_llm_routed_client import _config as routed_config
    from utils.llm.budget import (
        DEVELOPMENT_RECOVERY_RESERVATION,
        DEVELOPMENT_UNPRICED_BILLING,
        LlmRunBudget,
    )
    from utils.llm.client import LlmClient
    from utils.llm.errors import LlmTransientTransportError
    from utils.llm.models import LlmMessage, LlmRequest
    from utils.llm.testing import ScriptedTransport

    change = (
        Path(__file__).resolve().parents[3]
        / "openspec/changes/restore-company-profile-llm-led-generalization"
    )
    checkpoint = json.loads(
        gzip.decompress(
            (change / "b1-resource-evidence/iteration2/checkpoint.json.gz").read_bytes()
        )
    )
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(page.get("readable", True))
            and bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    )
    report = ReportIdentity.model_validate(
        checkpoint["report_interpretation"]["report"]
    )
    identity = compact_processing_identity(discovery_scope="page_map")
    mapped = page_map(pages)

    def fit_map(batch):
        return fits_development_payload(
            batch,
            purpose="source_inventory",
            source_scope="page_map",
            report=report,
            identity=identity,
            max_output_tokens=1000,
        )

    def fit_body(batch):
        return fits_development_payload(
            batch,
            purpose="source_inventory",
            source_scope="full_context",
            report=report,
            identity=identity,
            max_output_tokens=2500,
        )

    map_batches = pack_fitting_batches(mapped, fit_map)
    assert [page["page"] for batch in map_batches for page in batch] == [
        page["page"] for page in mapped
    ]
    successor = json.loads(
        (change / "b1-pool-observation-evidence/successor/query.json").read_text()
    )
    requested = set()
    for receipt in successor["profiles"][0]["three_views"]["call_receipts"][:2]:
        requested.update(
            (receipt.get("response_payload") or {}).get("requested_pages") or []
        )
    neighbors = {
        page["page"]
        for page in pages
        if any(abs(page["page"] - number) <= 1 for number in requested)
    }
    plans = {
        "unsampled_only": structural_discovery_chunks(
            pages, set(), payload_fits=fit_body
        ),
        "requested_neighbors": structural_discovery_chunks(
            pages, neighbors, payload_fits=fit_body
        ),
        "all_pages_selected": structural_discovery_chunks(
            pages,
            {page["page"] for page in pages if page["readable"]},
            payload_fits=fit_body,
        ),
    }
    for name, body in plans.items():
        assert body, name
        assert len(map_batches) + len(body) + 2 <= 15, name
        covered: dict[int, set[int]] = {}
        for batch in body:
            for fragment in batch:
                start = int(fragment["offset"])
                indexes = covered.setdefault(fragment["page"], set())
                indexes.update(range(start, start + len(fragment["text"])))
        for page in pages:
            if not page["readable"]:
                continue
            if name == "all_pages_selected" or (
                name == "requested_neighbors" and page["page"] in neighbors
            ):
                expected = range(len(page["text"]))
            else:
                expected = (
                    index
                    for start, end in unsampled_ranges(page["text"])
                    for index in range(start, end)
                )
            missing = [
                index
                for index in expected
                if index not in covered.get(page["page"], set())
            ]
            assert not missing, (name, page["page"], missing[:5])

    def gateway_payload(batch, purpose, scope, max_output, **extra):
        request = ReportSemanticRequestV3(
            request_id="size",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(batch),
            source_scope=scope,
            max_output_tokens=max_output,
            timeout_seconds=30,
            **extra,
        )
        user = json.dumps(
            compact_request(request), ensure_ascii=False, separators=(",", ":")
        )
        return LlmClient._build_payload(
            model="gemini-3.8-flash-high",
            messages=[
                {"role": "system", "content": "size"},
                {"role": "user", "content": user},
            ],
            schema=response_schema(request),
            schema_name="company_profile_report_response",
            mode="json_object",
            temperature=0,
            max_output_tokens=max_output,
            max_output_tokens_field="max_completion_tokens",
            stream=False,
            stream_include_usage=False,
        )

    discovery = [
        gateway_payload(batch, "source_inventory", "page_map", 1000)
        for batch in map_batches
    ]
    discovery += [
        gateway_payload(batch, "source_inventory", "full_context", 2500)
        for batch in plans["requested_neighbors"]
    ]
    from research.company_profile.contracts import ReportSourceCondition
    from research.company_profile.models import SEMANTIC_RECORD_ADAPTER

    def as_tuples(value):
        if isinstance(value, list):
            return tuple(as_tuples(item) for item in value)
        if isinstance(value, dict):
            return {key: as_tuples(item) for key, item in value.items()}
        return value

    conditions = tuple(
        ReportSourceCondition.model_validate(as_tuples(item))
        for item in checkpoint["report_interpretation"]["source_conditions"]
    )
    records = tuple(
        SEMANTIC_RECORD_ADAPTER.validate_json(json.dumps(item, ensure_ascii=False))
        for item in checkpoint["report_interpretation"]["records"]
    )
    quotes = cited_quote_pages(
        tuple(evidence for condition in conditions for evidence in condition.evidence)
        + tuple(evidence for record in records for evidence in record.evidence)
    )
    assert quotes
    assert sum(len(page["text"]) for page in quotes) < sum(
        len(page["text"]) for page in pages
    )
    followups = [
        gateway_payload(
            quotes,
            "synthesize_views",
            "full_context",
            2500,
            source_conditions=conditions,
            records=records,
        ),
        gateway_payload(
            quotes,
            "verify_views",
            "full_context",
            2000,
            source_conditions=conditions,
            claims=(),
            records=records,
        ),
    ]
    payloads = discovery + followups
    encoded = [
        len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
        for item in payloads
    ]
    assert max(encoded) <= DEVELOPMENT_PAYLOAD_BYTE_LIMIT
    assert all(
        size + 256 + item.get("max_completion_tokens", item.get("max_tokens"))
        <= DEVELOPMENT_RECOVERY_RESERVATION
        for size, item in zip(encoded, payloads)
    )
    largest_body = max(
        plans["requested_neighbors"],
        key=lambda batch: sum(len(page["text"]) for page in batch),
    )
    semantic = ReportSemanticRequestV3(
        request_id="size",
        purpose="source_inventory",
        report=report,
        processing_identity=identity,
        source_pages=tuple(largest_body),
        source_scope="full_context",
        max_output_tokens=2500,
        timeout_seconds=30,
    )
    user = json.dumps(
        compact_request(semantic), ensure_ascii=False, separators=(",", ":")
    )
    config = routed_config(timeout_seconds=10, attempt_timeout_seconds=5)
    budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        max_tokens=120_000,
        max_requests=16,
    )
    transport = ScriptedTransport(
        [
            LlmTransientTransportError("offline"),
            {
                "data": {
                    "choices": [
                        {
                            "message": {"content": '{"label":"ok"}'},
                            "finish_reason": "stop",
                        }
                    ]
                }
            },
        ]
    )
    from tests.unit.test_utils.test_llm_physical_budget import _client

    asyncio_request = LlmRequest(
        profile="semantic",
        messages=[LlmMessage(role="user", content=user)],
        response_schema={
            "type": "object",
            "properties": {"label": {"type": "string"}},
            "required": ["label"],
        },
        schema_name="semantic",
        schema_version="v1",
        idempotency_key="b1-large-payload",
        max_output_tokens=2500,
        metadata={"workload": "semantic", "business_item_key": "600037.SH"},
    )
    import asyncio

    asyncio.run(_client(transport, budget, config).complete(asyncio_request))
    ledger = budget.snapshot()
    assert len(transport.calls) == ledger["physical_requests"] == 2
    assert len({item["provider_resource"] for item in ledger["attempts"]}) == 2
    assert [call["payload"]["messages"][-1]["content"] for call in transport.calls] == [
        user,
        user,
    ]
    sent = [
        len(
            json.dumps(
                call["payload"], ensure_ascii=False, separators=(",", ":")
            ).encode()
        )
        for call in transport.calls
    ]
    assert min(sent) > 30_000
    assert ledger["charged_cost_cny"] == "unknown"
    assert all(
        item["usage_status"] == "unknown_reserved" for item in ledger["attempts"]
    )
    assert ledger["charged_tokens"] <= 120_000


def test_successful_map_batches_remain_when_a_later_batch_fails(monkeypatch):
    import research.company_profile.report_interpretation as ri
    from tests.unit.test_research.test_company_profile_llm_led import (
        _evidence,
        _overview,
        _report,
    )

    pages = tuple(
        {
            "page": number,
            "text": TEXT if number == 1 else f"第{number}页说明。" * 30,
            "readable": True,
        }
        for number in range(1, 4)
    )
    original = ri.pack_fitting_batches

    def one_preview(items, fits, **kwargs):
        batches = original(items, fits, **kwargs)
        if items and items[0].get("scope") == "structure_preview_only":
            return [[page] for batch in batches for page in batch]
        return batches

    monkeypatch.setattr(ri, "pack_fitting_batches", one_preview)
    report = _report()
    native = _overview(report, _evidence(report))

    class FailThird(CompactSemanticFixture):
        def interpret(self, request):
            if request.source_scope == "page_map":
                self.requests.append(request)
                if len(self.requests) == 3:
                    raise ValueError("map_batch_failed")
                return ReportSemanticResponseV3(
                    request_id=request.request_id,
                    purpose=request.purpose,
                    requested_pages=(request.source_pages[0]["page"],),
                )
            return super().interpret(request)

    result = _run(
        FailThird(),
        report=report,
        pages=pages,
        structured_records=(native,),
        processing_identity=compact_processing_identity(discovery_scope="page_map"),
    )
    by_page = {item["page"]: item for item in result.page_coverage}
    assert by_page[1]["map_checked"] and by_page[1]["map_selected"]
    assert by_page[2]["map_checked"] and by_page[2]["map_selected"]
    assert by_page[1]["status"] == by_page[2]["status"] == "partially_checked"
    assert not by_page[3].get("map_checked")
    assert all(item["status"] != "read" for item in result.page_coverage)
    assert result.call_receipts[-1]["status"] == "failed"
    assert result.call_receipts[-1]["source_pages"] == [3]
    assert native in result.records
    assert not result.draft_complete
    assert any("map_batch_failed" in gap for gap in result.gaps)


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
    """A's three-page counterexample: the unsampled sentence must be sent.

    Scopes may share a request so the full plan stays inside 15 sends. The
    hidden page is still an unsampled span, not a preview and not a full-text
    read. The sibling test blocks completion when that business is established
    and then omitted.
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
    hidden_pages = [
        page
        for request in residue_requests
        for page in request.source_pages
        if hidden in page["text"]
    ]
    assert hidden_pages
    assert all(page["text"] != page3 for page in hidden_pages)
    assert result.page_coverage[2]["status"] == "residue_discovered"
    assert result.page_coverage[2]["status"] != "read"
    assert any(
        chunk["scope"] == "unsampled_span"
        for chunk in result.page_coverage[2]["chunks"]
    )


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


def test_used_recovery_is_not_reserved_again_before_synthesis():
    from research.company_profile.reading_budget import remaining_tail_units

    plan = {
        "synthesis_units": 13_563,
        "verification_units": 11_774,
        "recovery_units": 13_614,
    }
    attempts = (
        {"status": "failed_or_cancelled", "usage": None},
        {"status": "success", "usage": {"total_tokens": 14_589}},
    )
    assert remaining_tail_units(plan, (), "synthesize_views") == 11_774 + 13_614
    assert remaining_tail_units(plan, attempts, "synthesize_views") == 11_774
    assert (
        remaining_tail_units(
            plan, ({"status": "http_failure", "usage": None},), "synthesize_views"
        )
        == 11_774
    )
    assert (
        remaining_tail_units(
            plan,
            ({"status": "failed_or_cancelled", "usage": {"total_tokens": 10}},),
            "synthesize_views",
        )
        == 11_774
    )
    charged = 13_633 + 14_589
    assert charged + plan["synthesis_units"] + remaining_tail_units(
        plan, attempts, "synthesize_views"
    ) <= 62_220


def test_verified_slice_survives_a_later_discovery_failure_and_stays_incomplete():
    hidden = "另外，本公司本期主要经营电池回收服务，占营业收入百分之六十。"
    pages = (
        {"page": 1, "text": TEXT, "readable": True},
        {"page": 2, "text": "承接上页的普通说明。", "readable": True},
        {"page": 3, "text": hidden, "readable": True},
    )
    identity = compact_processing_identity(discovery_scope="page_map")
    identity.update(
        partial_slice=True,
        ledger_tokens=120_000,
        max_physical_requests=8,
        continue_after_verify=True,
        parent_attempt="special",
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
                "usage": {"total_tokens": 10},
                "model": "parent-model",
            },
        ),
    )

    class LaterFailure(CompactSemanticFixture):
        def interpret(self, request):
            if request.source_scope == "full_context" and request.purpose == "source_inventory" and any(
                hidden in page["text"] for page in request.source_pages
            ):
                raise ValueError("later_discovery_failed")
            return super().interpret(request)

    result = _run(LaterFailure(), processing_identity=identity, pages=pages)
    assert any(claim.status == "observed" for claim in result.claims)
    assert result.draft_complete is False
    assert result.semantic_complete is False
    assert result.real_semantic_complete is False
    assert result.page_coverage[2]["status"] != "read"
    assert any("later_discovery_failed" in gap for gap in result.gaps)
    views = __import__(
        "research.company_profile.report_interpretation", fromlist=["research_views"]
    ).research_views(result)
    assert views["views"]["company_profile"]["claims"]
    assert views["draft_complete"] is False
    assert all(
        item.get("physical_request") is False
        for item in result.call_receipts
        if item.get("reused_navigation")
    )


def test_shrunk_slice_body_tail_and_recovery_share_one_ledger():
    import asyncio
    import gzip
    import time
    from pathlib import Path

    from research.company_profile.models import ReportIdentity
    from research.company_profile.reading_budget import (
        partial_slice_plan,
        reused_map_receipts,
    )
    from research.company_profile.report_interpretation import COMPACT_REPORT_PROMPT
    from tests.unit.test_utils.test_llm_physical_budget import _client
    from tests.unit.test_utils.test_llm_routed_client import _config as routed_config
    from utils.llm.budget import DEVELOPMENT_UNPRICED_BILLING, LlmRunBudget
    from utils.llm.errors import LlmTransientTransportError
    from utils.llm.models import LlmMessage, LlmRequest
    from utils.llm.testing import ScriptedTransport

    change = (
        Path(__file__).resolve().parents[3]
        / "openspec/changes/restore-company-profile-llm-led-generalization"
    )
    checkpoint = json.loads(
        gzip.decompress(
            (change / "b1-resource-evidence/iteration2/checkpoint.json.gz").read_bytes()
        )
    )
    query = json.loads(
        (change / "b1-pool-observation-evidence/special/query.json").read_text()
    )
    pages = tuple(
        {
            "page": int(page["page"]),
            "text": str(page.get("text") or ""),
            "readable": bool(str(page.get("text") or "").strip()),
        }
        for page in checkpoint["pages"]
    )
    _, selected = reused_map_receipts(
        query["profiles"][0]["three_views"]["call_receipts"],
        prompt_version=COMPACT_REPORT_PROMPT,
        page_count=len(pages),
    )
    report = ReportIdentity.model_validate(
        checkpoint["report_interpretation"]["report"]
    )
    identity = compact_processing_identity(discovery_scope="page_map")
    plan = partial_slice_plan(
        pages,
        sorted(selected),
        report=report,
        identity=identity,
        ledger_units=62_220,
    )
    assert plan["total_units"] <= 62_220
    assert plan["pages"]
    assert plan["fragments"][-1]["text"].rstrip()[-1] in "。！？"

    def request_for(purpose, max_output):
        semantic = ReportSemanticRequestV3(
            request_id="slice",
            purpose=purpose,
            report=report,
            processing_identity=identity,
            source_pages=tuple(plan["fragments"]),
            source_scope="full_context",
            max_output_tokens=max_output,
            timeout_seconds=30,
        )
        user = json.dumps(
            compact_request(semantic), ensure_ascii=False, separators=(",", ":")
        )
        return LlmRequest(
            profile="semantic",
            messages=[LlmMessage(role="user", content=user)],
            response_schema={
                "type": "object",
                "properties": {"label": {"type": "string"}},
                "required": ["label"],
            },
            schema_name="semantic",
            schema_version="v1",
            idempotency_key=purpose,
            max_output_tokens=max_output,
            metadata={"workload": "semantic", "business_item_key": "600037.SH"},
        )

    budget = LlmRunBudget(
        deadline=time.monotonic() + 30,
        prices={},
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,
        max_tokens=62_220,
        max_requests=8,
    )
    transport = ScriptedTransport(
        [
            LlmTransientTransportError("offline"),
            {"data": {"choices": [{"message": {"content": '{"label":"body"}'}}]}},
            {"data": {"choices": [{"message": {"content": '{"label":"synth"}'}}]}},
            {"data": {"choices": [{"message": {"content": '{"label":"verify"}'}}]}},
        ]
    )
    client = _client(
        transport, budget, routed_config(timeout_seconds=10, attempt_timeout_seconds=5)
    )
    asyncio.run(client.complete(request_for("source_inventory", 2500)))
    asyncio.run(client.complete(request_for("synthesize_views", 2500)))
    asyncio.run(client.complete(request_for("verify_views", 2000)))
    ledger = budget.snapshot()
    assert ledger["physical_requests"] == 4
    assert len({item["provider_resource"] for item in ledger["attempts"][:2]}) == 2
    assert ledger["charged_tokens"] <= 62_220
    assert ledger["charged_cost_cny"] == "unknown"
    assert (
        min(
            len(call["payload"]["messages"][-1]["content"].encode())
            for call in transport.calls
        )
        > 1000
    )
