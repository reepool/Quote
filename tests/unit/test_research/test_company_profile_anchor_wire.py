"""The anchor request sent by the public gateway is the request the parser accepts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import jsonschema
import pytest

from research.company_profile.contracts import (
    Evidence,
    ReportSemanticRequestV3,
    ReportSourceCondition,
    ReportViewClaim,
    SemanticProviderError,
)
from research.company_profile.models import TextAnchor
from research.company_profile.report_interpretation import (
    restore_source_anchor,
    source_anchor_catalog,
)
from research.company_profile.report_wire import (
    expand_response,
    instructions,
    response_schema,
)
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from tests.unit.test_research.test_company_profile_runtime import _report
from utils.llm import LlmMessage, LlmRequest, LlmResponse
from utils.llm.schema import validate_data

PAGE = "公司主要从事纸业生产。主要产品为纸袋纸。"


@dataclass
class _Gateway:
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


def _identity(**extra):
    return {
        "reading_strategy": "direct_core_v1",
        "source_reference": "anchor_v1",
        "chapter_select": True,
        "execution_mode": "offline_fake",
        **extra,
    }


def _request(purpose="source_inventory", **kwargs):
    report = _report()
    return ReportSemanticRequestV3(
        request_id=f"anchor:{purpose}",
        purpose=purpose,
        report=report,
        processing_identity=_identity(),
        source_pages=({"page": 9, "text": PAGE, "readable": True, "offset": 0},),
        max_output_tokens=4000,
        timeout_seconds=10.0,
        **kwargs,
    )


def _send(request, payload):
    gateway = _Gateway(outputs=[payload])
    provider = CommonGatewaySemanticProvider(
        client=gateway,
        profile="semantic_extraction",
        max_output_tokens=4000,
        timeout_seconds=10,
    )
    try:
        result = provider.interpret(request)
    except SemanticProviderError as exc:
        result = exc
    sent = gateway.requests[0]
    text = "\n".join(
        LlmMessage.from_value(item).content for item in sent.messages
    )
    return result, sent.response_schema, text


def _anchor(request):
    catalog = source_anchor_catalog(
        request.source_pages, request.report.document_version
    )
    chosen = next(item for item in catalog if "主要从事" in PAGE[
        int(item["start"]): int(item["end"])
    ])
    return chosen["id"]


def test_gateway_anchor_contract_accepts_only_ids_it_sent():
    inventory = _request()
    anchor_id = _anchor(inventory)
    restored = restore_source_anchor(
        anchor_id, inventory.report, inventory.source_pages
    )
    result, schema, text = _send(
        inventory,
        {
            "source_conditions": [
                {
                    "condition_id": "principal",
                    "dimension": "principal_business",
                    "description": "公司从事纸业生产",
                    "major": True,
                    "statement_status": "current",
                    "evidence": [anchor_id],
                }
            ]
        },
    )
    assert "{page,quote}" not in text
    assert "source_anchors" in text
    assert schema["$defs"]["Evidence"]["enum"]
    assert anchor_id in schema["$defs"]["Evidence"]["enum"]
    assert "anyOf" not in schema["$defs"]["Evidence"]
    assert result["source_conditions"][0]["evidence"][0]["anchor"]["bounded_quote"] == (
        restored["anchor"]["bounded_quote"]
    )

    quoted = {
        "source_conditions": [
            {
                "condition_id": "principal",
                "dimension": "principal_business",
                "description": "公司从事纸业生产",
                "major": True,
                "statement_status": "current",
                "evidence": [{"page": 9, "quote": "公司主要从事纸业生产"}],
            }
        ]
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(quoted, schema)
    rejected, _, _ = _send(inventory, quoted)
    assert isinstance(rejected, SemanticProviderError)
    assert "source anchor id required; evidence=object:page,quote" in str(rejected)

    unknown = {
        "source_conditions": [
            {
                "condition_id": "principal",
                "dimension": "principal_business",
                "description": "公司从事纸业生产",
                "major": True,
                "statement_status": "current",
                "evidence": ["other-version:9:0:4"],
            }
        ]
    }
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(unknown, schema)
    rejected, _, _ = _send(inventory, unknown)
    assert "unknown source anchor" in str(rejected)


def test_gateway_reuses_only_evidence_ids_on_this_request():
    report = _report()
    kept = Evidence(
        evidence_id="kept-input",
        report=report,
        page=9,
        section_title="采购",
        anchor=TextAnchor(bounded_quote="国内木竹片采购定价主要采取挂牌采购"),
    )
    repair = _request(
        source_scope="evidence_repair",
        source_conditions=(
            ReportSourceCondition(
                condition_id="input",
                dimension="input_exposure",
                description="采购木竹片",
                major=True,
                statement_status="current",
                evidence=(kept,),
            ),
        ),
    )
    result, schema, text = _send(
        repair,
        {
            "source_conditions": [
                {
                    "condition_id": "input",
                    "dimension": "input_exposure",
                    "description": "采购木竹片",
                    "major": True,
                    "statement_status": "current",
                    "evidence": ["kept-input"],
                }
            ]
        },
    )
    assert "kept-input" in schema["$defs"]["Evidence"]["enum"]
    assert "{page,quote}" not in text
    assert result["source_conditions"][0]["evidence"][0]["evidence_id"] == "kept-input"
    assert (
        result["source_conditions"][0]["evidence"][0]["anchor"]["bounded_quote"]
        == "国内木竹片采购定价主要采取挂牌采购"
    )


def test_gateway_draft_and_verify_use_the_same_id_contract():
    report = _report()
    evidence = Evidence(
        evidence_id="kept-role",
        report=report,
        page=9,
        section_title="经营",
        anchor=TextAnchor(bounded_quote="公司主要从事纸业生产"),
    )
    condition = ReportSourceCondition(
        condition_id="role",
        dimension="supply_chain_role",
        description="公司从事纸业生产",
        major=True,
        statement_status="current",
        evidence=(evidence,),
    )
    draft = _request("synthesize_views", source_conditions=(condition,))
    drafted, schema, text = _send(
        draft,
        {
            "claims": [
                {
                    "claim_id": "role-claim",
                    "dimension": "supply_chain_role",
                    "status": "observed",
                    "answer": "公司从事纸业生产",
                    "condition_ids": ["role"],
                    "evidence": ["kept-role"],
                }
            ]
        },
    )
    assert "kept-role" in schema["$defs"]["Evidence"]["enum"]
    assert "{page,quote}" not in text
    assert drafted["claims"][0]["evidence"][0]["evidence_id"] == "kept-role"
    claim = ReportViewClaim(
        claim_id="role-claim",
        dimension="supply_chain_role",
        status="observed",
        answer="公司从事纸业生产",
        condition_ids=("role",),
        evidence=(evidence,),
    )
    verify = _request(
        "verify_views", source_conditions=(condition,), claims=(claim,)
    )
    checked, verify_schema, verify_text = _send(
        verify,
        {
            "checks": [
                {
                    "target_type": "claim",
                    "target_id": "role-claim",
                    "status": "pass",
                    "explanation": "原文支持该经营表述",
                },
                {
                    "target_type": "condition",
                    "target_id": "role",
                    "status": "pass",
                    "explanation": "该条件由同一主张覆盖",
                    "covers_target_type": "claim",
                    "covers_target_id": "role-claim",
                },
            ]
        },
    )
    assert "{page,quote}" not in verify_text
    assert "may be omitted" in verify_text
    assert checked["checks"][1]["covers_target_id"] == "role-claim"
    assert "Evidence" not in verify_schema.get("$defs", {})


def test_old_identity_still_accepts_a_page_quote_object():
    report = _report()
    old = ReportSemanticRequestV3(
        request_id="old:v3",
        purpose="source_inventory",
        report=report,
        processing_identity={"discovery_scope": "full_text"},
        source_pages=({"page": 9, "text": PAGE, "readable": True},),
        max_output_tokens=4000,
        timeout_seconds=10.0,
    )
    assert "{page,quote}" in instructions(old)
    schema = response_schema(old)
    assert "anyOf" in schema["$defs"]["Evidence"]
    payload = {
        "source_conditions": [
            {
                "condition_id": "principal",
                "dimension": "principal_business",
                "description": "公司从事纸业生产",
                "major": True,
                "statement_status": "current",
                "evidence": [{"page": 9, "quote": "公司主要从事纸业生产"}],
            }
        ],
        "requested_pages": [],
    }
    jsonschema.validate(payload, schema)
    expanded = expand_response(payload, old)
    assert expanded["source_conditions"][0]["evidence"][0]["anchor"]["bounded_quote"] == (
        "公司主要从事纸业生产"
    )


def test_required_field_diagnostic_names_the_field_without_the_body():
    with pytest.raises(Exception) as caught:
        validate_data(
            {"source_conditions": [{"evidence": [{"page": 9, "quote": "原文"}]}]},
            {"type": "object", "required": ["claims"], "properties": {"claims": {}}},
        )
    message = str(caught.value)
    assert "missing=claims" in message
    assert "keys=source_conditions" in message
    assert "source_conditions.evidence=object" in message
    assert "sha256=" in message
    assert "原文" not in message
