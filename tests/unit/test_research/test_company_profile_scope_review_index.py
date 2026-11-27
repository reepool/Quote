"""The v15 queue changes request presentation, not source judgments or acceptance."""
import asyncio
from copy import deepcopy
import json
from pathlib import Path

import pytest

from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, SUPPLY_CHAIN_LABELS, WorkpaperDraftResponse,
    WorkpaperReviewResponse, bind_workpaper_draft, workpaper_elements,
)
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import (
    REVIEW_POLICY, SCOPE_REVIEW_INDEX_INSTRUCTIONS, review_workpaper, workpaper_hash,
)
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_review import material, _retained_review
from tests.unit.test_utils.test_llm_gateway import _response


def _source_and_draft():
    pages = tuple({**p, "text": p["text"] + (
        "\n租赁业务收入60.00元。公司计划2026年开展服务。\n" if p["page"] == 3 else "")}
        for p in PAGES)
    packet = build_section_packet(_report(), pages, index_sections(pages), _selection(index_sections(pages)))
    raw = _draft(packet).model_dump(mode="json")
    revenue = raw["elements"]["scale_and_mix"]["facts"][0]
    revenue["text"] += "租赁业务收入60.00元。"
    revenue["metrics"].append({**revenue["metrics"][0], "name": "租赁业务收入",
        "raw_value": "60.00", "value": "60.00", "subject_scope": "consolidated_group"})
    main = raw["elements"]["business_structure"]["facts"][0]
    raw["supply_chain"] = {
        key: {"status": "observed" if key == "position" else "unclear",
              "facts": [{**main, "fact_id": "service-role"}] if key == "position" else [],
              "limitations": [] if key == "position" else ["所选章节未获取该项。"]}
        for key in SUPPLY_CHAIN_LABELS
    }
    raw["company_features"] = {"status": "observed", "limitations": [], "facts": [{
        "fact_id": "future-service", "text": "公司计划2026年开展服务。",
        "subject_scope": "unclear", "period": "2026年计划", "metrics": [],
        "uncertainty_reason": "保留原计划，不当作本年已实现业务。",
        "evidence_ids": [packet.evidence[-1].evidence_id],
    }]}
    paper = bind_workpaper_draft(WorkpaperDraftResponse.model_validate(raw), report=_report(),
        reading_scope=packet.reading_scope, evidence=packet.evidence, origin="llm_generated")
    return paper, packet


def _payload(call):
    return json.loads(call["payload"]["messages"][-1]["content"])


@pytest.mark.parametrize("version", ["v13", "v14", "v15"])
def test_actual_initial_gateway_wire_queues_every_original_scope_without_values_or_new_gate(version):
    paper, packet = _source_and_draft()
    original_hash = workpaper_hash(paper)
    original = [f for group in workpaper_elements(paper).values() for f in group.facts]
    reply = dict(reviewed_fact_ids=[f.fact_id for f in original], findings=[])
    provider, identity, transport, client, _ = gateway([_response(reply)])
    identity["workpaper_review_policy"] = "company_profile_workpaper_review." + version
    try:
        result, _ = review_workpaper(paper, packet=packet, provider=provider,
            work_id="scope-index-" + version, identity=identity)
        assert len(transport.calls) == 1
        call = transport.calls[0]
        payload = _payload(call)
        assert payload["source_text"] == packet.model_text()
        instructions = "\n".join(m["content"] for m in call["payload"]["messages"] if m["role"] == "system")
        schema = call["payload"]["response_format"]["json_schema"]["schema"]
        assert schema == WorkpaperReviewResponse.model_json_schema()
        if version == "v15":
            rows = payload["scope_review_index"]
            assert [r["fact_id"] for r in rows] == [f.fact_id for f in original]
            assert {r["fact_id"] for r in rows} == {"main", "revenue", "service-role", "future-service"}
            for row, fact in zip(rows, original):
                assert set(row) == {"fact_id", "subject_scope", "period", "evidence_ids", "metrics"}
                assert row["subject_scope"] == fact.subject_scope
                assert row["period"] == fact.period
                assert row["evidence_ids"] == list(fact.evidence_ids)
                assert row["metrics"] == [{"metric_index": i, "name": m.name,
                    "subject_scope": m.subject_scope, "period": m.period} for i, m in enumerate(fact.metrics)]
            encoded = json.dumps(rows, ensure_ascii=False)
            assert "100.00" not in encoded and "60.00" not in encoded
            assert all(f.text not in encoded for f in original)
            assert SCOPE_REVIEW_INDEX_INSTRUCTIONS in instructions
            assert instructions.index("complete\nthe scope/period check") < instructions.index("Seek material missing MAIN")
        else:
            assert "scope_review_index" not in payload
            assert SCOPE_REVIEW_INDEX_INSTRUCTIONS not in instructions
        assert result.quality_review.policy_version == identity["workpaper_review_policy"]
        assert result.quality_review.status == "completed"
        assert result.elements == paper.elements and result.supply_chain == paper.supply_chain
        assert result.company_features == paper.company_features
        assert workpaper_hash(paper) == original_hash
    finally:
        asyncio.run(client.close())


def test_scope_queue_only_enters_initial_wire_and_leaves_repair_final_contracts_unchanged():
    paper, packet, fixed, review, verify = material()
    provider, identity, transport, client, _ = gateway([
        _response(review), _response({"repaired_facts": [fixed]}), _response(verify)])
    try:
        result, _ = review_workpaper(paper, packet=packet, provider=provider,
            work_id="scope-index-three-stages", identity=identity)
        assert len(transport.calls) == 3
        assert "scope_review_index" in _payload(transport.calls[0])
        for call in transport.calls[1:]:
            assert "scope_review_index" not in _payload(call)
            assert all(SCOPE_REVIEW_INDEX_INSTRUCTIONS not in m["content"]
                       for m in call["payload"]["messages"] if m["role"] == "system")
        assert _payload(transport.calls[1])["repair_targets"][0]["target_fact_id"] == "revenue"
        assert _payload(transport.calls[2])["candidates"] == [fixed]
        assert result.quality_review.policy_version == REVIEW_POLICY
        assert result.quality_review.findings[0].resolution == "corrected_pending_trust"
        assert result.elements.business_structure == paper.elements.business_structure
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize("mode", ["verify_only", "repair_and_verify"])
def test_retained_v14_initial_remains_old_review_without_a_new_scope_queue_or_checked_hashes(mode):
    paper, packet, fixed, review, verify = material()
    retained = _retained_review(paper, review, fixed)
    retained.update(mode=mode, review_policy_version="company_profile_workpaper_review.v14")
    if mode == "repair_and_verify":
        retained["receipts"] = retained["receipts"][:1]
        retained["receipts"][0]["finish_reason"] = "stop"
        retained.pop("repair_response")
    before = deepcopy(retained)
    replies = ([_response({"repaired_facts": [fixed]})] if mode == "repair_and_verify" else []) + [_response(verify)]
    provider, identity, transport, client, _ = gateway(replies)
    try:
        result, receipts = review_workpaper(paper, packet=packet, provider=provider,
            work_id="scope-index-retained", identity=identity, resume=retained)
        assert len(transport.calls) == len(replies)
        assert result.quality_review.policy_version == "company_profile_workpaper_review.v14"
        assert result.quality_review.checked_fact_hashes is None
        assert result.quality_review.retained_request_ids == tuple(r["request_id"] for r in retained["receipts"])
        assert all("scope_review_index" not in _payload(c) for c in transport.calls)
        assert all(c.purpose != "review_profile_workpaper" or c.request_id in result.quality_review.retained_request_ids
                   for c in result.quality_review.calls)
        assert retained == before
        assert receipts[0]["raw_content"] == retained["receipts"][0]["raw_content"]
        assert result.quality_review.status == "completed"
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize("filename,version", [
    ("bp-work-0788cd766ed03e2101c554ec.json", "v13"),
    ("bp-work-69d683022dd1747f88f9c631.json", "v14"),
])
def test_new_default_identity_preserves_saved_v13_v14_bytes_and_decoding(filename, version):
    root = Path(__file__).resolve().parents[3]
    path = root / "data/research/company_profile_classification_scope_20261127/company_profile_common_core.v1" / filename
    if not path.is_file():
        pytest.skip("saved classification/scope corpus is not installed")
    before = path.read_bytes()
    paper = CompanyProfileWorkpaper.model_validate(json.loads(before)["workpaper"])
    serialized = paper.model_dump_json()
    assert paper.quality_review.policy_version == "company_profile_workpaper_review." + version
    assert workpaper_processing_identity(real=False, review=True)["workpaper_review_policy"] == REVIEW_POLICY
    assert REVIEW_POLICY == "company_profile_workpaper_review.v15"
    assert paper.model_dump_json() == serialized and path.read_bytes() == before


def test_prepared_continuation_carries_original_initial_review_policy():
    from research.company_profile.runtime import CompanyProfileRuntimeRecordV3
    from research.company_profile.workpaper_review import prepare_review_recovery

    root = Path(__file__).resolve().parents[3]
    directory = root / "data/research/company_profile_classification_scope_20261127/company_profile_common_core.v1"
    paths = [directory / (work_id + ".json") for work_id in (
        "bp-work-69d683022dd1747f88f9c631", "bp-work-0788cd766ed03e2101c554ec")]
    if not all(p.is_file() for p in paths):
        pytest.skip("saved v14 continuation corpus is not installed")
    originals = [p.read_bytes() for p in paths]
    data = json.loads(originals[0])
    quality = data["workpaper"]["quality_review"]
    quality.update(status="incomplete", error="LLM response message content is empty")
    quality["calls"] = [c for c in quality["calls"] if c["purpose"] != "verify_profile_workpaper_repair"]
    for finding in quality["findings"]:
        if finding.get("revised_fact") is not None:
            finding["resolution"] = "verification_failed"
    record = CompanyProfileRuntimeRecordV3.model_validate_json(json.dumps(data))
    parent = CompanyProfileRuntimeRecordV3.model_validate_json(originals[1])
    _, resume, _ = prepare_review_recovery(record, current_draft_base=parent)
    assert resume["review_policy_version"] == "company_profile_workpaper_review.v14"
    assert resume["mode"] == "verify_only"
    assert {r["purpose"] for r in resume["receipts"]} == {
        "review_profile_workpaper", "repair_profile_workpaper"}
    assert [p.read_bytes() for p in paths] == originals
