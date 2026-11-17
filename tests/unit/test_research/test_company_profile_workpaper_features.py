"""Optional autonomous discoveries share the existing source/review/write path."""
import asyncio
import copy
import csv
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperDraftResponse, bind_workpaper_draft,
    normalize_workpaper_draft, render_workpaper, workpaper_draft_wire_schema,
)
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import review_workpaper, workpaper_hash
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_research.test_company_profile_workpaper_review import Provider


def material(*, wrong_column=False):
    text = "\n公司将结算系统以开放接口嵌入企业经营流程。该开放接口结算服务收入20.00元。\n"
    pages = tuple({**p, "text": p["text"] + text} if p["page"] == 2 else p for p in PAGES)
    index = index_sections(pages)
    packet = build_section_packet(_report(), pages, index, _selection(index))
    raw = _draft(packet).model_dump(mode="json")
    feature = {"fact_id": "feature-mechanism", "text": "公司将结算系统以开放接口嵌入企业经营流程。",
               "subject_scope": "consolidated_group", "period": "2025",
               "evidence_ids": [packet.evidence[0].evidence_id], "metrics": []}
    income = {**feature, "fact_id": "feature-income", "text": "该开放接口结算服务收入20.00元。",
              "metrics": [{"name": "开放接口结算服务收入", "raw_value": "20.00", "value": "20.00",
                           "unit": "元", "currency": "CNY", "subject_scope": "consolidated_group", "period": "2025"}]}
    fixed = copy.deepcopy(income)
    if wrong_column:
        income["text"] = "该开放接口结算服务利润20.00元。"
        income["metrics"][0]["name"] = "开放接口结算服务利润"
    raw["company_features"] = {"status": "observed", "facts": [feature, income], "limitations": []}
    return pages, packet, raw, fixed


def bind(raw, packet):
    return bind_workpaper_draft(WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(raw)),
        report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence, origin="controlled_fixture")


def review_messages(packet, fixed):
    ids = list(fixed["evidence_ids"])
    review = {"reviewed_fact_ids": ["main", "revenue", "feature-mechanism", "feature-income"], "findings": [{
        "finding_id": "feature-column", "element": "company_features", "severity": "minor",
        "issue_type": "wrong_column", "action": "correct", "affected_fact_id": "feature-income",
        "evidence_ids": ids, "explanation": "特色有依据，但原文20.00是收入，不是利润；不影响基础画像。"}]}
    verify = {"checked_fact_ids": [fixed["fact_id"]], "checks": [{"fact_id": fixed["fact_id"],
        "accepted": True, "evidence_ids": ids, "explanation": "原文明确为接口结算服务收入20.00元。"}]}
    return review, {"repaired_facts": [fixed]}, verify


@pytest.mark.parametrize("bad", ["number", "source", "duplicate", "typed"])
def test_bad_feature_is_local_and_valid_feature_and_core_survive(bad):
    _, packet, raw, _ = material()
    baseline = bind(copy.deepcopy(raw), packet)
    fact = raw["company_features"]["facts"][1]
    if bad == "number":
        fact["text"] = "接口结算服务收入999.00元。"
        fact["metrics"][0].update(raw_value="999.00", value="999.00")
    elif bad == "source":
        fact["evidence_ids"] = ["not-in-packet"]
    elif bad == "duplicate":
        fact["fact_id"] = "main"
    else:
        fact["metrics"][0]["value"] = "not-a-number"
    before = copy.deepcopy(raw)
    result = bind(raw, packet)
    assert result.elements == baseline.elements and raw == before
    assert [f.fact_id for f in result.company_features.facts] == ["feature-mechanism"]
    assert result.company_features.limitations


@pytest.mark.parametrize("value", ["wrong container", ["wrong"], {"facts": []}])
def test_malformed_optional_container_does_not_erase_core(value):
    from utils.llm.schema import validate_data
    _, packet, raw, _ = material()
    raw["company_features"] = value
    validate_data(raw, workpaper_draft_wire_schema())
    result = bind(raw, packet)
    assert result.company_features is None
    assert result.elements.business_structure.facts and result.elements.scale_and_mix.facts
    assert any("公司特色" in note for note in result.limitations)


def test_legacy_shape_and_hash_do_not_gain_an_empty_extension():
    _, packet, _, _ = material()
    paper = bind_workpaper_draft(_draft(packet), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
    raw = paper.model_dump(mode="json")
    assert "company_features" not in raw and "company_features" not in json.loads(paper.model_dump_json())
    expected = hashlib.sha256(json.dumps(raw, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    assert workpaper_hash(paper) == expected
    assert CompanyProfileWorkpaper.model_validate(raw) == paper


def test_features_cannot_replace_a_missing_baseline_element():
    _, packet, raw, _ = material()
    raw['elements'].pop('business_structure')
    with pytest.raises(ValueError):
        bind(raw, packet)


@pytest.mark.parametrize("bad_core", ["source", "typed"])
def test_feature_cannot_reuse_a_withheld_core_identity(bad_core):
    _, packet, raw, _ = material()
    raw['elements']['scale_and_mix']['facts'][0]['metrics'][0].update(raw_value='999', value='999')
    if bad_core == 'typed':
        raw['elements']['scale_and_mix']['facts'][0]['metrics'][0]['value'] = 'not-a-number'
    raw['company_features']['facts'][1]['fact_id'] = 'revenue'
    result = bind(raw, packet)
    assert not result.elements.scale_and_mix.facts
    assert [f.fact_id for f in result.company_features.facts] == ['feature-mechanism']
    assert '公司特色与自主发现' in render_workpaper(result)


def test_scope_review_cannot_erase_optional_format_isolation_reason():
    _, packet, raw, _ = material()
    raw['company_features']['facts'][1]['metrics'][0]['value'] = 'not-a-number'
    paper = bind(raw, packet)
    provider = Provider([{'reviewed_fact_ids': ['main', 'revenue', 'feature-mechanism'], 'findings': [{
        'finding_id': 'scope', 'element': 'limitations', 'severity': 'minor', 'issue_type': 'other',
        'action': 'human_review', 'affected_fact_id': None, 'evidence_ids': [packet.evidence[0].evidence_id],
        'explanation': '所选材料之外未知。'}]}])
    result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id='feature-scope',
        identity=workpaper_processing_identity(real=False, review=True))
    # Scope review legitimately updates limitations, never the retained facts.
    for name in type(paper.elements).model_fields:
        assert getattr(result.elements, name).facts == getattr(paper.elements, name).facts
    assert result.company_features.facts == paper.company_features.facts
    assert any('特色事实feature-income格式错误' in n for n in result.standardization_notes)


@pytest.mark.parametrize("final_ok", [True, False])
def test_feature_secondary_review_repairs_locally_and_requires_final(final_ok):
    _, packet, raw, fixed = material(wrong_column=True)
    paper = bind(raw, packet)
    review, repair, verify = review_messages(packet, fixed)
    provider = Provider([review, repair, verify if final_ok else ValueError("final unavailable")])
    result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id="features",
        identity=workpaper_processing_identity(real=False, review=True))
    assert result.elements == paper.elements
    assert [f.fact_id for f in result.company_features.facts] == ["feature-mechanism"] + (["feature-income"] if final_ok else [])
    assert result.quality_review.findings[0].resolution == ("corrected_pending_trust" if final_ok else "verification_failed")
    if final_ok:
        assert result.company_features.facts[1].metrics[0].name == "开放接口结算服务收入"
    assert "fixed feature list" in provider.calls[0]["instructions"]


@pytest.mark.parametrize("final_ok", [True, False])
def test_reviewer_can_add_a_source_backed_feature_without_making_it_mandatory(final_ok):
    _, packet, _, fixed = material()
    paper = bind_workpaper_draft(_draft(packet), report=_report(), reading_scope=packet.reading_scope, evidence=packet.evidence)
    candidate = {**fixed, "fact_id": "review-add:distinctive-mechanism"}
    review = {"reviewed_fact_ids": ["main", "revenue"], "findings": [{
        "finding_id": "distinctive-mechanism", "element": "company_features", "severity": "material",
        "issue_type": "material_omission", "action": "possible_omission", "affected_fact_id": None,
        "evidence_ids": fixed["evidence_ids"], "explanation": "所读原文中的接口结算机制值得展示。"}]}
    verify = {"checked_fact_ids": [candidate["fact_id"]], "checks": [{"fact_id": candidate["fact_id"],
        "accepted": final_ok, "evidence_ids": fixed["evidence_ids"], "explanation": "原文核对结果。"}]}
    provider = Provider([review, {"repaired_facts": [candidate]}, verify])
    result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id="feature-add",
        identity=workpaper_processing_identity(real=False, review=True))
    assert result.elements == paper.elements
    assert bool(result.company_features) is final_ok
    assert paper.company_features is None


def test_gateway_fresh_features_review_repair_writer_and_three_reads():
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
    from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
    from tests.unit.test_utils.test_llm_gateway import _config, _response
    from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
    from utils.llm.testing import ScriptedTransport

    pages, packet, raw, fixed = material(wrong_column=True)
    selection = _selection(index_sections(pages))
    review, repair, verify = review_messages(packet, fixed)
    transport = ScriptedTransport([_response(v) for v in (selection.model_dump(mode="json"), raw, review, repair, verify)])
    client = _client(transport, _budget(), _config(max_schema_repair_attempts=0))
    provider = CommonGatewaySemanticProvider(client=client, profile="test", max_output_tokens=2500, timeout_seconds=10,
        execution_authorization={"status": "authorized", "document_version": _report().document_version,
                                 "approval_reference": "controlled-feature-gateway-test"})
    provider.offline_only = True
    identity = workpaper_processing_identity(real=False, review=True)
    identity.update(quality_review_profile="test", repair_profile="test")
    try:
        with TemporaryDirectory(prefix="workpaper-cq-test-") as directory:
            root = Path(directory)
            runtime = CompanyProfileStageRuntime(writer=CompanyProfileResearchWriter(root/'output'), provider=provider)
            item = {"work_id": "features-fresh", "report": _report(), "pages": pages,
                    "processing_identity": identity}
            for stage in ("acquire", "parse", "semantic", "verify", "publish"):
                assert runtime.run_stage(stage, item)["status"] == "success"
            query = CompanyProfileReadService(root/'output').query(("600000.SH",))
            export = CompanyProfileReadService(root/'output').export(("600000.SH",), root/'export')
            loaded = CompanyProfileReadService(root/'output').query(("600000.SH",))
            assert query['profiles'] == export['profiles'] == loaded['profiles']
            profile = query['profiles'][0]
            assert profile['workpaper']['origin'] == 'controlled_fixture'
            assert profile['workpaper']['quality_review']['status'] == 'completed', profile['workpaper']['quality_review']['error']
            assert profile['workpaper']['company_features']['facts'][1]['metrics'][0]['name'] == '开放接口结算服务收入'
            assert profile['workpaper_summary']['element_fact_counts']['company_features'] == 2
            assert profile['workpaper_summary']['fact_count'] == 4
            assert '公司特色与自主发现' in profile['workpaper_markdown']
            assert len(transport.calls) == 5
            assert 'optional ROOT company_features' in transport.calls[1]['payload']['messages'][0]['content']
            assert 'company_features' in transport.calls[2]['payload']['messages'][1]['content']
            extraction = transport.calls[1]['payload']['messages'][0]['content']
            initial_review = transport.calls[2]['payload']['messages'][0]['content']
            repair_instructions = transport.calls[3]['payload']['messages'][0]['content']
            final_instructions = transport.calls[4]['payload']['messages'][0]['content']
            assert 'retain ALL material categories' in extraction
            assert 'not an industry account checklist' in extraction
            assert 'what this detail concretely helps the reader understand' in extraction
            assert 'not an allowed-feature list' in extraction
            assert 'next-year plan is not a current result' in extraction
            assert 'compact main income composition' in initial_review
            assert 'not grounds for another repair or' in initial_review
            for instructions in (extraction, initial_review, repair_instructions, final_instructions):
                assert 'Applicability checkboxes' in instructions or 'applicability checkbox' in instructions
                assert 'disclosure' in instructions
                assert 'issuer means an explicitly parent-only company' in instructions
                assert 'business_segment means the specified business, product or industry row' in instructions
                assert "Judge each metric's own source object independently" in instructions
                assert 'does not inherit consolidated_group' in instructions
                assert 'arithmetic sum alone cannot establish' in instructions
            # Only extraction and repair RETURN facts; review/final return findings/checks.
            for call in (transport.calls[1], transport.calls[3]):
                schema = call['payload']['response_format']['json_schema']['schema']
                for name in ('WorkpaperFact', 'WorkpaperMetric'):
                    description = schema['$defs'][name]['properties']['subject_scope']['description']
                    assert 'explicitly parent-only' in description
                    assert 'specified business, product or industry row' in description
            rows = list(csv.DictReader((root/'export/profiles.csv').read_text().splitlines()))
            assert rows[0]['workpaper_fact_count'] == '4'
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('final_ok', [True, False])
def test_source_native_main_mix_supplement_preserves_total_and_autonomous_feature(final_ok):
    """The provider owns semantic coverage; no account whitelist or automatic fill-in."""
    pages, _, _, _ = material()
    pages = tuple({**p, 'text': p['text'] + '\n主要收入构成：接口授权费70.00元；流程协同服务费20.00元；历史数据检索费10.00元。\n'}
                  if p['page'] == 3 else p for p in pages)
    packet = build_section_packet(_report(), pages, index_sections(pages), _selection(index_sections(pages)))
    raw = _draft(packet).model_dump(mode='json')
    base = raw['elements']['business_structure']['facts'][0]
    source_ids = [e.evidence_id for e in packet.evidence]
    raw['elements']['revenue_model'] = {'status': 'observed', 'limitations': [], 'facts': [{
        **base, 'fact_id': 'largest-income', 'text': '接口授权费70.00元。',
        'evidence_ids': source_ids, 'metrics': [{'name': '接口授权费', 'raw_value': '70.00',
            'value': '70.00', 'unit': '元', 'currency': 'CNY',
            'subject_scope': base['subject_scope'], 'period': base['period']}]}]}
    feature = {**base, 'fact_id': 'autonomous-mechanism',
               'text': '公司将结算系统以开放接口嵌入企业经营流程。'}
    raw['company_features'] = {'status': 'observed', 'facts': [feature], 'limitations': []}
    paper = bind(raw, packet)
    candidate = {**base, 'fact_id': 'review-add:main-mix',
                 'text': '流程协同服务费20.00元；历史数据检索费10.00元。', 'evidence_ids': source_ids,
                 'metrics': [{'name': name, 'raw_value': number, 'value': number, 'unit': '元', 'currency': 'CNY',
                              'subject_scope': base['subject_scope'], 'period': base['period']}
                             for name,number in [('流程协同服务费','20.00'), ('历史数据检索费','10.00')]]}
    review = {'reviewed_fact_ids': ['main','revenue','largest-income','autonomous-mechanism'],
              'findings': [{'finding_id': 'main-mix', 'element': 'revenue_model', 'severity': 'material',
                  'issue_type': 'material_omission', 'action': 'possible_omission',
                  'affected_fact_id': 'largest-income', 'evidence_ids': source_ids,
                  'explanation': '原最大项正确，所读主要收入构成还有两项，需保留它们解释主干。'}]}
    final = {'checked_fact_ids': [candidate['fact_id']], 'checks': [{
        'fact_id': candidate['fact_id'], 'accepted': final_ok,
        'evidence_ids': source_ids, 'explanation': '受控原文核验结果。'}]}
    provider = Provider([review, {'repaired_facts': [candidate]}, final])
    result, _ = review_workpaper(paper, packet=packet, provider=provider, work_id='main-mix',
        identity=workpaper_processing_identity(real=False, review=True))
    assert result.elements.scale_and_mix == paper.elements.scale_and_mix
    assert result.company_features == paper.company_features
    assert result.elements.revenue_model.facts[0] == paper.elements.revenue_model.facts[0]
    assert len(result.elements.revenue_model.facts) == (2 if final_ok else 1)
    assert len(provider.calls) == 3
    if final_ok:
        assert [m.name for m in result.elements.revenue_model.facts[1].metrics] == ['流程协同服务费','历史数据检索费']
    else:
        # An explicit negative final is a rejection, unlike an unavailable final.
        assert result.quality_review.findings[0].resolution == 'repair_rejected'
    result.validate_pages(pages)
