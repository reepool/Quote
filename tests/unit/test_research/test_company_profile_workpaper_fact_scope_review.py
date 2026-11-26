"""Fact scope/period and metric scope are separate source-review obligations.

Saved responses reproduce known misses. Scripted new verdicts test the ordinary
gateway and consumer, never a real model's ability to detect those misses.
"""
import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperDraftResponse, bind_workpaper_draft,
    workpaper_elements,
)
from research.company_profile.workpaper_delivery import project_workpaper_delivery
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import (
    FACT_SCOPE_INSTRUCTIONS, REVIEW_POLICY, review_workpaper, workpaper_hash,
)
from research.company_profile.workpaper_sections import SectionPacket, build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_utils.test_llm_gateway import _response


ROOT = Path(__file__).resolve().parents[3]
CASES = (
    ("920003.BJ", "bp-work-0788cd766ed03e2101c554ec", "ccm-2"),
    ("600704.SH", "bp-work-e53470a4bfe65c83d9a82b89", "F-CC-01"),
    ("920006.BJ", "bp-work-9d9c2637afa2f4ffc35c6fcc", "F021"),
)


def _run(paper, packet, replies):
    provider, identity, transport, client, budget = gateway([_response(reply) for reply in replies])
    # The saved complete source packet exceeds the helper's small synthetic
    # context. This changes only this ScriptedTransport client's test price map.
    budget.prices = {key: replace(price, context_tokens=1_000_000) for key, price in budget.prices.items()}
    try:
        result, receipts = review_workpaper(paper, packet=packet, provider=provider,
            work_id="fact-scope-controlled", identity=identity)
        assert len(receipts) == len(replies) == len(transport.calls), result.quality_review.error
        for call in transport.calls:
            messages = call["payload"]["messages"]
            assert FACT_SCOPE_INSTRUCTIONS in messages[0]["content"]
            assert json.loads(messages[-1]["content"])["source_text"] == packet.model_text()
        assert result.quality_review.policy_version == REVIEW_POLICY
        return result, transport.calls
    finally:
        asyncio.run(client.close())


def _saved_case(stock, work_id, fact_id):
    directory = ROOT / "data/checkpoints/company_profile_v1_acceptance_20261125" / stock / "execution/wires"
    record_path = ROOT / "data/research/company_profile_v1_acceptance_20261125/company_profile_common_core.v1" / f"{work_id}.json"
    paths = [record_path, *(directory / f"{i:03}.json" for i in (3, 4, 5))]
    if not all(path.is_file() for path in paths):
        pytest.skip("saved 6.FN source/response corpus is not installed")
    original_bytes = {path: path.read_bytes() for path in paths}
    record, *wires = [json.loads(original_bytes[path]) for path in paths]
    request = json.loads(wires[0]["request"]["messages"][-1]["content"])
    paper = CompanyProfileWorkpaper.model_validate({**record["workpaper"], **request["draft"],
        "quality_review": None, "use_review": None})
    packet = SectionPacket(report=paper.report, reading_scope=paper.reading_scope, evidence=paper.evidence)
    assert packet.model_text() == request["source_text"]
    replies = [json.loads(wire["response"]["choices"][0]["message"]["content"]) for wire in wires]
    finding = next(item for item in replies[0]["findings"] if item["affected_fact_id"] == fact_id)
    fact = next(f for group in workpaper_elements(paper).values() for f in group.facts if f.fact_id == fact_id)
    assert finding["action"] == "limit_metrics"
    return paper, packet, replies, finding, fact, original_bytes


@pytest.mark.parametrize("stock,work_id,fact_id", CASES)
@pytest.mark.parametrize("resolved_scope", [False, True])
def test_saved_metric_only_miss_needs_independent_fact_scope_resolution(stock, work_id, fact_id, resolved_scope):
    paper, packet, replies, metric_finding, original, original_bytes = _saved_case(stock, work_id, fact_id)
    old_hash = workpaper_hash(paper)

    # Exact saved model responses retain the old missed scope; v14 is a prompt
    # change, not a program pretending to infer meaning from an explanation.
    replay, _ = _run(paper, packet, replies)
    old_delivery = project_workpaper_delivery(replay)
    missed = next(item for item in old_delivery["facts"] if item["fact_id"] == fact_id)
    assert missed["state"] == "limited"
    assert missed["fact"]["subject_scope"] == original.subject_scope
    assert not missed["fact"]["metrics"]

    # Controlled verdict adds a separate whole-fact issue to the actual saved
    # replies. The old metric limit must not override it or bypass final review.
    scripted = deepcopy(replies)
    scope_issue = {key: value for key, value in metric_finding.items() if key != "metric_limit"}
    scope_issue.update(finding_id="controlled-fact-scope", action="correct",
        explanation="事实自身范围也未得到原文支持，隔离指标不能修正该确定标签。")
    scripted[0]["findings"].append(scope_issue)
    candidate = original.model_dump(mode="json")
    candidate["metrics"] = []
    if resolved_scope:
        candidate.update(subject_scope="unclear",
            uncertainty_reason="保留原文所述局部对象；该范围不能安全统一为集团整体或业务分部。")
    scripted[1]["repaired_facts"].append(candidate)
    scripted[2]["checked_fact_ids"].append(fact_id)
    scripted[2]["checks"].append(dict(fact_id=fact_id, accepted=resolved_scope,
        evidence_ids=list(original.evidence_ids), explanation="保留有源原句及范围不确定原因。" if resolved_scope
        else "仅移除指标，事实自身确定的范围仍无据。"))
    result, calls = _run(paper, packet, scripted)
    assert len(calls) == 3  # one review, one combined repair, one final
    targets = json.loads(calls[1]["payload"]["messages"][-1]["content"])["repair_targets"]
    assert sum(target["target_fact_id"] == fact_id for target in targets) == 1
    delivery = project_workpaper_delivery(result)
    delivered = next((item for item in delivery["facts"] if item["fact_id"] == fact_id), None)
    if resolved_scope:
        assert delivered["state"] == "limited"
        assert delivered["fact"]["text"] == original.text
        assert delivered["fact"]["subject_scope"] == "unclear"
        assert delivered["fact"]["uncertainty_reason"] == candidate["uncertainty_reason"]
        assert delivered["evidence_ids"] == list(original.evidence_ids)
        assert delivered["model_review_stage"] == "final"
    else:
        assert delivered is None
        assert all(f.fact_id != fact_id for group in workpaper_elements(result).values() for f in group.facts)
        rejected = next(f for f in result.quality_review.findings if f.finding_id == "controlled-fact-scope")
        assert rejected.resolution == "repair_rejected" and rejected.original_fact == original
    assert [item for item in delivery["facts"] if item["fact_id"] != fact_id] == [
        item for item in old_delivery["facts"] if item["fact_id"] != fact_id]
    assert workpaper_hash(paper) == old_hash
    assert all(path.read_bytes() == content for path, content in original_bytes.items())


@pytest.mark.parametrize("fixed_period", [False, True])
def test_conditional_plan_fact_period_is_checked_even_with_no_metrics(fixed_period):
    pages = tuple({**p, "text": p["text"] + ("\n公司计划2026年交易完成后开展服务，交易尚未完成。\n"
        if p["page"] == 3 else "")} for p in PAGES)
    packet = build_section_packet(_report(), pages, index_sections(pages), _selection(index_sections(pages)))
    raw = _draft(packet).model_dump(mode="json")
    fact = dict(fact_id="conditional-plan", text="公司计划2026年交易完成后开展服务，交易尚未完成。",
        subject_scope="consolidated_group", period="2025已实现", metrics=[],
        evidence_ids=[packet.evidence[-1].evidence_id])
    raw["elements"]["material_changes"]["facts"] = [fact]
    raw["elements"]["material_changes"]["status"] = "observed"
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence, origin="llm_generated")
    candidate = {**fact, "period": "2026年计划，交易完成后；尚未实现" if fixed_period else fact["period"]}
    replies = [dict(reviewed_fact_ids=["main", "revenue", fact["fact_id"]], findings=[dict(
        finding_id="plan-period", element="material_changes", severity="material", issue_type="wrong_period",
        action="correct", affected_fact_id=fact["fact_id"], evidence_ids=fact["evidence_ids"],
        explanation="正文是交易后计划，事实期间却标2025已实现。")]),
        dict(repaired_facts=[candidate]), dict(checked_fact_ids=[fact["fact_id"]], checks=[dict(
            fact_id=fact["fact_id"], accepted=fixed_period, evidence_ids=fact["evidence_ids"],
            explanation="计划和未完成条件须与事实期间一致。")])]
    result, _ = _run(paper, packet, replies)
    delivered = next((item for item in project_workpaper_delivery(result)["facts"] if item["fact_id"] == fact["fact_id"]), None)
    if fixed_period:
        assert delivered["state"] == "reviewed"
        assert delivered["fact"]["period"] == candidate["period"]
        assert delivered["fact"]["text"] == fact["text"]
    else:
        assert delivered is None and not result.elements.material_changes.facts
        assert result.quality_review.findings[0].resolution == "repair_rejected"
    assert result.elements.scale_and_mix == paper.elements.scale_and_mix


def test_known_wrong_narrative_is_not_rescued_by_unclear_scope_and_empty_metrics():
    from tests.unit.test_research.test_company_profile_workpaper_review import material

    paper, packet, _, review, verification = material()
    paper = paper.model_copy(update={"origin": "llm_generated"})
    candidate = paper.elements.scale_and_mix.facts[0].model_dump(mode="json")
    candidate.update(subject_scope="unclear", metrics=[], uncertainty_reason="保留原文，主体待确定。")
    verification["checks"][0].update(accepted=False, explanation="原文是营业收入，正文仍误写为净利润。")
    result, _ = _run(paper, packet, [review, dict(repaired_facts=[candidate]), verification])
    assert all(item["fact_id"] != "revenue" for item in project_workpaper_delivery(result)["facts"])
    assert not result.elements.scale_and_mix.facts
    assert result.quality_review.findings[0].resolution == "repair_rejected"
    assert result.elements.business_structure == paper.elements.business_structure


def test_default_review_identity_changes_without_rewriting_saved_v13():
    _, work_id, _ = CASES[0]
    path = ROOT / "data/research/company_profile_v1_acceptance_20261125/company_profile_common_core.v1" / f"{work_id}.json"
    if not path.is_file():
        pytest.skip("saved v13 corpus is not installed")
    before = path.read_bytes()
    paper = CompanyProfileWorkpaper.model_validate(json.loads(before)["workpaper"])
    old_hash = workpaper_hash(paper)
    assert paper.quality_review.policy_version == "company_profile_workpaper_review.v13"
    identity = workpaper_processing_identity(real=False, review=True)
    assert identity["workpaper_review_policy"] == "company_profile_workpaper_review.v14"
    assert workpaper_hash(paper) == old_hash
    assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(before).digest()
