"""The existing repair owner consumes opt-in format fallback before its own final."""
import asyncio
from copy import deepcopy
import hashlib
import json

import pytest

from research.company_profile.workpaper_delivery import project_workpaper_delivery
from research.company_profile.workpaper import WorkpaperMetric, WorkpaperDraftResponse
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime, CompanyProfileRuntimeRecordV3
from research.company_profile.reads import CompanyProfileReadService, _record_processing_identity
from research.company_profile.workpaper_metric_format import METRIC_FORMAT_FLAG, METRIC_FORMAT_POLICY
from research.company_profile.workpaper_llm import METRIC_FORMAT_INSTRUCTIONS
from research.company_profile.workpaper_review import review_workpaper, prepare_review_recovery, workpaper_hash
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _selection
from research.company_profile.workpaper_sections import index_sections
from tests.unit.test_research.test_company_profile_workpaper_review import material
from tests.unit.test_utils.test_llm_gateway import _response


def directional_repair():
    pages = deepcopy(PAGES)
    pages[2]['text'] += '\n营业收入同比下降0.71%。\n'
    paper, packet, fixed, initial, verify = material(pages)
    fixed['text'] += '同比下降0.71%。'
    fixed['metrics'].append({**fixed['metrics'][0], 'name': '营业收入同比变动',
        'raw_value': '同比下降0.71%', 'value': '-0.71', 'unit': '%', 'source_unit': '%', 'currency': None})
    return paper, packet, fixed, initial, verify


@pytest.mark.parametrize('final', ['accepted', 'rejected', 'unavailable', 'legacy'])
def test_repair_needs_own_final_and_preserves_original_receipt(final):
    paper, packet, fixed, initial, verify = directional_repair()
    if final == 'rejected':
        verify['checks'][0]['accepted'] = False
    replies = [_response(initial), _response({'repaired_facts': [fixed]})]
    if final == 'unavailable':
        empty = _response(None, choices=[{'message': {'content': ''}, 'finish_reason': None}])
        replies += [empty, empty]
    elif final != 'legacy':
        replies += [_response(verify)]
    provider, identity, transport, client, _ = gateway(replies)
    if final != 'legacy':
        identity[METRIC_FORMAT_FLAG] = METRIC_FORMAT_POLICY
    else:
        identity.pop(METRIC_FORMAT_FLAG, None)
    try:
        result, receipts = review_workpaper(paper, packet=packet, provider=provider,
            work_id='format-repair', identity=identity)
        assert len(transport.calls) == {'accepted': 3, 'rejected': 3, 'unavailable': 4, 'legacy': 2}[final]
        repaired = next(r for r in receipts if r['purpose']=='repair_profile_workpaper')
        repair_wire=transport.calls[1]['payload']['messages']
        assert any(METRIC_FORMAT_INSTRUCTIONS in message['content'] for message in repair_wire) is (final!='legacy')
        assert repaired['response_payload']['repaired_facts']==[fixed]
        assert json.loads(repaired['raw_content'])==repaired['response_payload']
        assert repaired['contract_status']=='accepted' and fixed['metrics'][1]['value']=='-0.71'
        finding = result.quality_review.findings[0]
        assert finding.original_fact==paper.elements.scale_and_mix.facts[0]
        if final == 'legacy':
            assert finding.resolution=='repair_rejected'
        else:
            candidate = finding.revised_fact
            assert candidate.text==fixed['text'] and candidate.evidence_ids==tuple(fixed['evidence_ids'])
            assert candidate.metrics[0]==WorkpaperMetric.model_validate(fixed['metrics'][0])
            assert candidate.metrics[1].value is None and candidate.metrics[1].raw_value=='同比下降0.71%'
            assert '本地格式' in candidate.metrics[1].uncertainty_reason
            assert any('本地格式' in n for n in result.standardization_notes)
        if final == 'accepted':
            assert result.elements.scale_and_mix.facts[0]==finding.revised_fact
            row=next(f for f in project_workpaper_delivery(result)['facts'] if f['fact_id']=='revenue')
            assert row['state']=='limited' and row['fact']['metrics'][1]['value'] is None
        else:
            assert not result.elements.scale_and_mix.facts
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('bad', ['narrative', 'evidence', 'unit', 'arbitrary_field'])
def test_format_fallback_does_not_wash_other_defects_or_send_final(bad):
    paper, packet, fixed, initial, _ = directional_repair()
    if bad=='narrative':
        fixed['text'] += '另有营业收入999.00元。'
    elif bad=='evidence':
        fixed['evidence_ids']=['source-unknown']
    elif bad=='unit':
        fixed['metrics'][1]['source_unit']='万元'
    else:
        fixed['metrics'][1]['unrecognized_field']='not silently ignored'
    repair = _response({'repaired_facts':[fixed]})
    provider, identity, transport, client, _=gateway([_response(initial), repair, *([repair] if bad=='arbitrary_field' else [])])
    identity[METRIC_FORMAT_FLAG]=METRIC_FORMAT_POLICY
    try:
        result, _=review_workpaper(paper,packet=packet,provider=provider,work_id='bad-format',identity=identity)
        assert len(transport.calls)==(3 if bad=='arbitrary_field' else 2)
        assert result.quality_review.findings[0].resolution in {'repair_rejected', 'quarantined'}
        assert all(json.loads(c['payload']['messages'][-1]['content']).get('candidates') is None for c in transport.calls)
        assert not result.elements.scale_and_mix.facts
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize('failed_stage', ['initial', 'final'])
def test_new_format_failed_review_saved_plan_reconstructs_exact_seed_and_own_candidate(tmp_path, failed_stage):
    if failed_stage=='initial':
        from tests.unit.test_research.test_company_profile_metric_format import material as extracted_material

        pages, packet, raw = extracted_material()
        original_paper = None
    else:
        original_paper, packet, fixed, initial, verify = directional_repair()
        pages=deepcopy(PAGES)
        pages[2]['text'] += '\n营业收入同比下降0.71%。\n'
        raw=WorkpaperDraftResponse(elements=original_paper.elements).model_dump(mode='json')
    empty=_response(None,choices=[{'message':{'content':''},'finish_reason':None}])
    replies=[_response(_selection(index_sections(pages)).model_dump(mode='json')), _response(raw)]
    if failed_stage=='final':
        replies += [_response(initial), _response({'repaired_facts':[fixed]})]
    replies += [empty, empty]
    provider, identity, transport, client, _=gateway(replies)
    writer=CompanyProfileResearchWriter(tmp_path/'output')
    runtime=CompanyProfileStageRuntime(writer=writer,provider=provider)
    item={'work_id':'format-failed','report':packet.report,'pages':pages,'processing_identity':identity}
    try:
        for stage in ('acquire','parse','semantic','verify','publish'):
            assert runtime.run_stage(stage,item)['status']=='success'
        record=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
        assert _record_processing_identity(record)[METRIC_FORMAT_FLAG]==METRIC_FORMAT_POLICY
        before=writer.latest_path().read_bytes()
        plan=CompanyProfileReadService(tmp_path/'output').plan_review_recovery('600000.SH',request_id='planned-resume')
        assert plan['state']=='planned'
        seed,resume,receipts=prepare_review_recovery(record)
        assert workpaper_hash(seed)==record.workpaper.quality_review.original_draft_hash
        assert seed.generation==record.workpaper.generation and len(receipts)==2
        assert writer.latest_path().read_bytes()==before
        if failed_stage=='initial':
            assert seed.elements.scale_and_mix.facts[0].metrics[0].value is None
            assert resume is None
        else:
            assert resume['mode']=='verify_only'
            assert resume['repair_response']['repaired_facts']==[fixed]
            assert not record.workpaper.elements.scale_and_mix.facts  # No own final, no adoption.
            follow, follow_identity, follow_transport, follow_client, _=gateway([_response(verify)])
            try:
                safe, final_receipts=review_workpaper(seed,packet=packet,provider=follow,
                    work_id='own-final',identity=follow_identity,resume=resume)
                assert len(follow_transport.calls)==1
                sent=json.loads(follow_transport.calls[0]['payload']['messages'][-1]['content'])
                assert sent['candidates'][0]['metrics'][1]['value'] is None
                assert safe.elements.scale_and_mix.facts[0].metrics[1].value is None
                assert next(r for r in final_receipts if r['purpose']=='repair_profile_workpaper')['response_payload']['repaired_facts']==[fixed]
            finally:
                asyncio.run(follow_client.close())
    finally:
        asyncio.run(client.close())
