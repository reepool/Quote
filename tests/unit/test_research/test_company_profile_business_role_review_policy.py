"""Opted-in source alignment reaches all four actual wires; no semantic-success claim."""
import asyncio
from copy import deepcopy
import hashlib
import json

import pytest

from research.company_profile.runtime import _reading_policy
from research.company_profile.workpaper import (
    BUSINESS_ROLE_REVIEW_FLAG, BUSINESS_ROLE_REVIEW_POLICY, WORKPAPER_BUSINESS_ROLE_INSTRUCTIONS,
    WorkpaperDraftResponse, bind_workpaper_draft, workpaper_draft_wire_schema,
)
from research.company_profile.workpaper_llm import run_workpaper, workpaper_processing_identity
from research.company_profile.workpaper_review import review_workpaper
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review
from tests.unit.test_utils.test_llm_gateway import _response


# Captured before this role-policy edit, with the original v22/v16 no-flag identity.
OLD_SYSTEM_HASHES = (
    "2683dca2af755f6ea2324ae5e619a744dfa9597c1ab87e11485df10fda59efff",
    "6db270382eba676ac925d1a2b79759193888b21a1f5240d79019504e83f3155f",
    "313d977adc7f758d8f70da4b473daf8de6c14904b69891156a93047d16fb67c9",
    "16d19666d1d01f66e52bc428c7e5e13c5f5d736387ee073b9564e42e879e522b",
)
OLD_SCHEMA_HASHES = (
    "199e826933e4cfa6bd7c07745a2cb7f18d148510649cf183f9daef4bf3b7b876",
    "e233b2cfdac6c81016f717af60acd768d4d0a53b93e875a94fdf39beb4bdb25f",
    "53c182ff7bba714b27d74b4f98ace2f52b77096b2fffc88692970ea73434bfa4",
    "10b47ba18e02e4f236d84a1c1eaa4a0956f318901088d24718e929e4549054e5",
)


def sha(value):
    encoded = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("role-policy engineering tests must not send real requests")
    monkeypatch.setattr("socket.getaddrinfo", denied)
    monkeypatch.setattr("socket.create_connection", denied)


def material():
    text = ("公司也购销工业材料，采购后销售并取得商品买卖价差。\n"
            "公司按合同收取仓储服务费；已付运费或采购成本本身不说明产权。\n"
            "公司计划2026年扩建仓库，尚未完成。\n")
    pages = tuple({**p, "text": p["text"].replace("公司提供存贷款和结算服务。", "公司提供仓储服务。")
        + (text if p["page"] == 2 else "报告指标同比下降0.04个百分点。\n" if p["page"] == 3 else "")}
        for p in deepcopy(PAGES))
    packet = build_section_packet(_report(), pages, index_sections(pages), _selection(index_sections(pages)))
    draft = _draft(packet).model_dump(mode="json")
    draft["elements"]["business_structure"]["facts"][0]["text"] = "公司提供仓储服务。"
    evidence = [packet.evidence[0].evidence_id]
    issue = dict(finding_id="missing-role", element="revenue_model", severity="material",
        issue_type="material_omission", action="possible_omission", affected_fact_id=None,
        evidence_ids=evidence, explanation="所选原文还有与服务收费不同的商品购销角色。")
    candidate = dict(fact_id="review-add:missing-role", text="公司也购销工业材料，采购后销售并取得商品买卖价差。",
        subject_scope="consolidated_group", period="2025", evidence_ids=evidence, metrics=[])
    replies = [
        _selection(index_sections(pages)).model_dump(mode="json"), draft,
        {"reviewed_fact_ids": ["main", "revenue"], "findings": [issue]},
        {"repaired_facts": [candidate]},
        {"checked_fact_ids": [candidate["fact_id"]], "checks": [dict(fact_id=candidate["fact_id"],
            accepted=True, evidence_ids=evidence, explanation="受控回应，仅验证工程接线。")]}]
    return pages, packet, replies


def exercise(enabled):
    pages, packet, replies = material()
    provider, _, transport, client, _ = gateway([_response(r) for r in replies])
    identity = workpaper_processing_identity(real=False, review=True, subject_scope_limits=True,
        business_role_review_policy=BUSINESS_ROLE_REVIEW_POLICY if enabled else None)
    identity.update(quality_review_profile="review", repair_profile="test",
        previous_profile={"marker": "OLD_PROFILE_NOT_MODEL_INPUT"})
    try:
        extracted = run_workpaper(report=_report(), pages=pages, processing_identity=identity,
            provider=provider, work_id="role-wire")
        assert extracted.packet == packet
        independent = use_review(extracted.workpaper, {"main": "source_checked"})
        independent["decisions"][0]["reason"] = "A_REFERENCE_NOT_MODEL_INPUT"
        guarded = bind_workpaper_use_review(extracted.workpaper, independent)
        reviewed, _ = review_workpaper(guarded, packet=packet, provider=provider,
            work_id="role-wire", identity=identity)
        assert len(transport.calls) == 5  # selection + the original four semantic stages only
        assert reviewed.origin == "controlled_fixture"
        assert reviewed.quality_review.status == "completed"
        assert reviewed.quality_review.findings[0].resolution == "corrected_pending_trust"
        assert reviewed.elements.revenue_model.facts[0].text == replies[3]["repaired_facts"][0]["text"]
        reviewed.validate_pages(pages)
        return transport.calls, identity, extracted.workpaper.model_dump_json()
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize("enabled", [False, True])
def test_actual_four_stage_messages_and_schemas_use_only_explicit_role_policy(enabled):
    calls, identity, serialized = exercise(enabled)
    shared = WORKPAPER_BUSINESS_ROLE_INSTRUCTIONS
    assert BUSINESS_ROLE_REVIEW_FLAG not in serialized  # not a model-authored metadata field
    if enabled:
        assert identity[BUSINESS_ROLE_REVIEW_FLAG] == BUSINESS_ROLE_REVIEW_POLICY
    else:
        assert BUSINESS_ROLE_REVIEW_FLAG not in identity
    for index, call in enumerate(calls[1:]):
        from utils.llm.schema import compact_schema_instruction

        systems = [m["content"] for m in call["payload"]["messages"] if m["role"] == "system"]
        assert len(systems) == 1
        if index == 0:
            # The actual shape-recovery extraction wire embeds its schema in
            # the system message; the review/repair/final wires use response_format.
            assert "response_format" not in call["payload"]
            schema = workpaper_draft_wire_schema()
            prefix = compact_schema_instruction(schema) + "\n"
            assert systems[0].startswith(prefix)
            message = systems[0][len(prefix):]
        else:
            schema = call["payload"]["response_format"]["json_schema"]["schema"]
            message = systems[0]
        if enabled:
            assert message.endswith("\n" + shared) and message.count(shared) == 1
            original = message[:-len("\n" + shared)]
            assert sha(original) == OLD_SYSTEM_HASHES[index]
            for clause in (
                "buying/reselling goods", "charging service fees", "operating for others", "investment",
                "Freight or procurement cost alone proves neither", "Check fact.period independently of the report year",
                "actual versus plan/outlook", "event time and conditions", "Unselected or unfound information is unknown",
                "not 'the report did not disclose'", "support for EVERY substantive clause",
                "governing headers and continuation pages", "direction/sign, unit and comparison basis",
                "positive decrease magnitude must explicitly retain decrease", "value=null and a specific",
                "do not guess a sign", "additional source,\nextra call or supply-chain detail",
            ):
                assert clause in shared and clause in message
        else:
            assert shared not in message and sha(message) == OLD_SYSTEM_HASHES[index]
        assert sha(schema) == OLD_SCHEMA_HASHES[index]
        payload = json.loads(call["payload"]["messages"][-1]["content"])
        assert "商品买卖价差" in payload["source_text"] and payload["source_text"]
        assert "2026年" in payload["source_text"] and "下降0.04个百分点" in payload["source_text"]
        assert "source-0001" in payload["source_text"] and "source-0002" in payload["source_text"]
        assert "A_REFERENCE_NOT_MODEL_INPUT" not in json.dumps(payload)
        assert "OLD_PROFILE_NOT_MODEL_INPUT" not in json.dumps(payload)
        assert "use_review" not in payload and "quality_review" not in payload and "generation" not in payload
    # Selection is unaffected; this policy does not expand the source or request count.
    assert shared not in calls[0]["payload"]["messages"][0]["content"]


def test_default_identity_hash_serialization_and_runtime_policy_do_not_mix():
    old = workpaper_processing_identity(real=False, review=True, subject_scope_limits=True)
    explicit_none = workpaper_processing_identity(real=False, review=True, subject_scope_limits=True,
        business_role_review_policy=None)
    new = workpaper_processing_identity(real=False, review=True, subject_scope_limits=True,
        business_role_review_policy=BUSINESS_ROLE_REVIEW_POLICY)
    assert old == explicit_none and BUSINESS_ROLE_REVIEW_FLAG not in old
    assert sha(old) == "e78699adad4b1661010c2ade7985190eaf35be5c320d6cb417739bc2bc94e9a4"
    assert new == {**old, BUSINESS_ROLE_REVIEW_FLAG: BUSINESS_ROLE_REVIEW_POLICY}
    assert sha(new) != sha(old) and _reading_policy(new) != _reading_policy(old)
    assert _reading_policy(old) == _reading_policy(explicit_none)
    assert old["prompt_version"].endswith("v22") and old["workpaper_review_policy"].endswith("v16")


def test_new_policy_cannot_change_unflagged_outputs_or_payloads():
    old_calls, _, old_paper = exercise(False)
    new_calls, _, new_paper = exercise(True)
    # Scripted answers are intentionally identical: this checks transport, not understanding.
    before, after = json.loads(old_paper), json.loads(new_paper)
    before_hash = before["generation"]["extraction"].pop("request_hash")
    after_hash = after["generation"]["extraction"].pop("request_hash")
    assert before_hash != after_hash  # New actual instructions have a distinct request identity.
    assert before == after  # Content, response attribution and serialized field set stay the same.
    assert old_calls[0]["payload"] == new_calls[0]["payload"]
    for old, new in zip(old_calls[1:], new_calls[1:]):
        assert old["payload"]["messages"][-1] == new["payload"]["messages"][-1]
        assert old["payload"].get("response_format") == new["payload"].get("response_format")


@pytest.mark.parametrize("policy", ["source_business_roles.v2", "", False])
def test_unknown_role_policy_rejects_before_request_at_factory_and_both_callers(policy):
    pages, packet, replies = material()
    with pytest.raises(ValueError, match="business role review policy"):
        workpaper_processing_identity(real=False, business_role_review_policy=policy)
    provider, identity, transport, client, _ = gateway([])
    identity[BUSINESS_ROLE_REVIEW_FLAG] = policy
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(replies[1]),
        report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence, origin="controlled_fixture")
    try:
        with pytest.raises(ValueError, match="business role review policy"):
            run_workpaper(report=_report(), pages=pages, processing_identity=identity, provider=provider, work_id="bad-role")
        with pytest.raises(ValueError, match="business role review policy"):
            review_workpaper(paper, packet=packet, provider=provider, work_id="bad-role", identity=identity)
        assert not transport.calls
    finally:
        asyncio.run(client.close())
