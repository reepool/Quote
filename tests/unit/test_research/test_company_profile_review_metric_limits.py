"""Normal source review can limit exact metric positions without authoring UseReview."""
import asyncio
from copy import deepcopy
import json

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime, CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import CompanyProfileWorkpaper, workpaper_fact_hash
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_review import review_workpaper, workpaper_hash
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _report
from tests.unit.test_research.test_company_profile_workpaper_delivery import material, use_review
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import _selection
from tests.unit.test_utils.test_llm_gateway import _response


def content():
    paper=material().model_copy(update={'origin':'llm_generated'})
    data=paper.model_dump(mode='json')
    fact=data['elements']['scale_and_mix']['facts'][0]
    fact['metrics'].append({**fact['metrics'][0], 'name':'另一个有源指标'})
    paper=CompanyProfileWorkpaper.model_validate(data)
    packet=build_section_packet(_report(),PAGES,index_sections(PAGES),_selection(index_sections(PAGES)))
    fact=paper.elements.scale_and_mix.facts[0]
    issue=dict(finding_id='metric-scope',element='scale_and_mix',severity='material',issue_type='wrong_subject',
        action='limit_metrics',affected_fact_id=fact.fact_id,evidence_ids=list(fact.evidence_ids),
        explanation='正文受原文支持，但原位置0的标准化指标口径不可靠；保留其他指标。',
        metric_limit=dict(fact_sha256=workpaper_fact_hash(fact),narrative_supported=True,metric_indices=[0]))
    return paper,packet,dict(reviewed_fact_ids=['main','revenue'],findings=[issue])


def review(paper,packet,reply,*,legacy=False,expected_calls=1):
    provider,identity,transport,client,_=gateway([_response(reply)] * expected_calls)
    if legacy:identity.pop('workpaper_metric_limit_policy')
    try:
        result,receipts=review_workpaper(paper,packet=packet,provider=provider,work_id='metric-review',identity=identity)
        assert len(transport.calls)==expected_calls
        return result,receipts,transport.calls
    finally:asyncio.run(client.close())


def test_normal_review_runtime_writer_API_and_export_keep_narrative_and_safe_metric(tmp_path):
    paper,packet,reply=content()
    provider,identity,transport,client,_=gateway([_response(reply)])
    writer=CompanyProfileResearchWriter(tmp_path/'output')
    runtime=CompanyProfileStageRuntime(writer=writer,provider=provider)
    item={'work_id':'normal-review','report':_report(),'pages':PAGES,'review_seed':paper.model_dump(mode='json'),
        'processing_identity':{**identity,'review_existing_draft_hash':workpaper_hash(paper)}}
    try:
        for stage in ('acquire','parse','semantic','verify','publish'):
            assert runtime.run_stage(stage,item)['status']=='success'
    finally:asyncio.run(client.close())
    assert len(transport.calls)==1
    record=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    assert record.workpaper.elements==paper.elements and record.workpaper.evidence==paper.evidence
    assert record.workpaper.use_review is None
    finding=record.workpaper.quality_review.findings[0]
    assert finding.resolution=='metrics_limited' and finding.metric_limit.metric_indices==(0,)
    profile=CompanyProfileReadService(tmp_path/'output').research_profile('600000.SH')
    fact=next(f for f in profile['facts'] if f['fact_id']=='revenue')
    assert fact['state']=='limited' and fact['review_basis']=='model_source_check'
    assert fact['fact']['text']==paper.elements.scale_and_mix.facts[0].text
    assert fact['fact']['metrics']==[paper.elements.scale_and_mix.facts[0].metrics[1].model_dump(mode='json')]
    assert fact['metric_gaps'][0]['name']=='营业收入' and 'value' not in fact['metric_gaps'][0]
    assert any(g.get('fact_id')=='revenue' and g.get('active',True) for g in profile['information_gaps'])
    reader=CompanyProfileReadService(tmp_path/'output');reader.export(('600000.SH',),export_directory=tmp_path/'export')
    exported=json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles'][0]
    assert exported==profile
    assert '另一个有源指标' in (tmp_path/'export/annual_profiles.md').read_text()
    payload=json.loads(transport.calls[0]['payload']['messages'][-1]['content'])
    assert payload['fact_hashes']['revenue']==workpaper_fact_hash(paper.elements.scale_and_mix.facts[0])


@pytest.mark.parametrize('failure',['hash','unknown_index','duplicate_index','wrong_element','unknown_fact','legacy',
    'bool_index','negative_index','narrative_false'])
def test_invalid_model_limit_never_becomes_fully_reviewed(failure):
    paper,packet,reply=content();issue=reply['findings'][0]
    if failure=='hash':issue['metric_limit']['fact_sha256']='0'*64
    elif failure=='unknown_index':issue['metric_limit']['metric_indices']=[9]
    elif failure=='duplicate_index':issue['metric_limit']['metric_indices']=[0,0]
    elif failure=='wrong_element':issue['element']='products_services'
    elif failure=='unknown_fact':issue['affected_fact_id']='unknown'
    elif failure=='bool_index':issue['metric_limit']['metric_indices']=[True]
    elif failure=='negative_index':issue['metric_limit']['metric_indices']=[-1]
    elif failure=='narrative_false':issue['metric_limit']['narrative_supported']=False
    # Wire-schema bool/literal violations use the existing one bounded retry;
    # they must remain rejected when both physical responses repeat the defect.
    result,_,_=review(paper,packet,reply,legacy=failure=='legacy',
        expected_calls=2 if failure in {'bool_index','narrative_false'} else 1)
    assert result.quality_review.status!='completed'
    delivered=project_workpaper_delivery(result)
    assert not any(f['fact_id']=='revenue' and f['state']=='reviewed' for f in delivered['facts'])


@pytest.mark.parametrize('independent',['source_checked','partial','withheld','source_only','duplicate'])
def test_model_and_independent_limits_take_conservative_union(independent):
    paper,packet,reply=content();result,_,_=review(paper,packet,reply)
    choice='source_checked' if independent=='partial' else independent
    decision=use_review(result,{'revenue':choice})
    if independent=='partial':decision['decisions'][0]['metric_restrictions']=[dict(metric_index=1,reason='独立审原位置1不可靠。')]
    bound=bind_workpaper_use_review(result,decision)
    f=next(f for f in project_workpaper_delivery(bound)['facts'] if f['fact_id']=='revenue')
    if independent in {'withheld','source_only','duplicate'}:
        assert f['fact'] is None and f['state']==independent
    else:
        assert f['state']=='limited'
        assert {g['metric_index'] for g in f['metric_gaps']}==({0,1} if independent=='partial' else {0})
        model_gap=next(g for g in f['metric_gaps'] if g['metric_index']==0)
        assert model_gap['review_basis']=='model_source_check' and model_gap['model_finding_ids']==['metric-scope']
        if independent=='partial':
            assert 'review_basis' not in next(g for g in f['metric_gaps'] if g['metric_index']==1)


@pytest.mark.parametrize('partial',[False,True])
@pytest.mark.parametrize('defect',['bad_limit','whole'])
def test_new_whole_defect_not_overridden_by_old_independent_checked(partial,defect):
    paper,packet,reply=content()
    if defect=='bad_limit':reply['findings'][0]['metric_limit']['metric_indices']=[99]
    else:
        reply['findings'][0].update(action='quarantine');reply['findings'][0].pop('metric_limit')
    result,_,_=review(paper,packet,reply)
    # Real handler removes a whole-invalid object. Challenge delivery too with
    # the old content retained in a compatibility/audit record.
    result=result.model_copy(update={'elements':paper.elements})
    decision=use_review(result,{'revenue':'source_checked'})
    if partial:decision['decisions'][0]['metric_restrictions']=[dict(metric_index=1,reason='旧限制。')]
    f=next(f for f in project_workpaper_delivery(bind_workpaper_use_review(result,decision))['facts'] if f['fact_id']=='revenue')
    assert f['fact'] is None and f['state']=='withheld'


def test_new_fields_absent_legacy_serialization_and_stale_content_fails_closed():
    paper,packet,reply=content();result,_,_=review(paper,packet,reply)
    payload=result.model_dump(mode='json');payload['elements']['scale_and_mix']['facts'][0]['metrics'].reverse()
    with pytest.raises(ValueError,match='stale'):CompanyProfileWorkpaper.model_validate(payload)
    legacy=deepcopy(reply);legacy['findings'][0].update(action='quarantine');legacy['findings'][0].pop('metric_limit')
    old,_,_=review(paper,packet,legacy,legacy=True)
    assert 'metric_limit_policy' not in old.quality_review.model_dump(mode='json')
    assert 'metric_limit' not in old.quality_review.findings[0].model_dump(mode='json')


def test_metric_only_limit_survives_other_fact_repair_failure_and_retained_review_resume():
    from research.company_profile.workpaper_llm import run_workpaper
    from tests.unit.test_research.test_company_profile_workpaper import _draft

    paper,packet,reply=content()
    reply['findings'].append(dict(finding_id='other-correction',element='business_structure',severity='material',
        issue_type='other',action='correct',affected_fact_id='main',evidence_ids=['source-0001'],explanation='核对另一条。'))
    # A valid initial metric-only decision must survive an unrelated later failure.
    provider,identity,transport,client,_=gateway([_response(reply)])
    original_request=provider.workpaper_request
    def fail_repair(**kwargs):
        if kwargs['purpose']=='repair_profile_workpaper':raise ValueError('controlled repair unavailable')
        return original_request(**kwargs)
    provider.workpaper_request=fail_repair
    try:
        result,receipts=review_workpaper(paper,packet=packet,provider=provider,work_id='interrupted',identity=identity)
    finally:asyncio.run(client.close())
    assert result.quality_review.status=='incomplete'
    entry=next(f for f in project_workpaper_delivery(result)['facts'] if f['fact_id']=='revenue')
    assert entry['state']=='limited' and len(entry['fact']['metrics'])==1
    raw_initial=next(r for r in receipts if r['purpose']=='review_profile_workpaper')
    resume=dict(mode='repair_and_verify',original_draft_hash=workpaper_hash(paper),document_version=paper.report.document_version,
        receipts=[raw_initial],review_response=raw_initial['response_payload'])
    provider,identity,transport,client,_=gateway([_response({'repaired_facts':[]})])
    try:
        after,_=review_workpaper(paper,packet=packet,provider=provider,work_id='resumed',identity=identity,resume=resume)
    finally:asyncio.run(client.close())
    entry=next(f for f in project_workpaper_delivery(after)['facts'] if f['fact_id']=='revenue')
    assert entry['state']=='limited' and entry['metric_gaps'][0]['metric_index']==0
    assert len(transport.calls)==1
