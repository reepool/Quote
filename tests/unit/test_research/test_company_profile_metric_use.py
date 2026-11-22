"""Component use decisions preserve original source facts and legacy shapes."""
import copy
import csv
import json

import pytest

from research.company_profile.workpaper import CompanyProfileWorkpaper, WorkpaperDraftResponse, render_workpaper, workpaper_fact_hash
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_review import ReviewRecoveryLink, inherit_recovery_use_review, workpaper_hash
from tests.unit.test_research.test_company_profile_workpaper_delivery import material, use_review


def restricted(*, disposition='source_checked', indices=(0,)):
    paper=material()
    review=use_review(paper,{'revenue':disposition})
    review['decisions'][0]['metric_restrictions']=[{'metric_index':i,'reason':'独立原文审核：指标主体标签不可靠，保留有源正文。'}for i in indices]
    return paper,review


def test_component_restriction_keeps_text_but_no_unsafe_numeric_metric():
    raw,decision=restricted()
    paper=bind_workpaper_use_review(raw,decision)
    current=project_workpaper_delivery(paper)
    entry=next(f for f in current['facts']if f['fact_id']=='revenue')
    assert entry['state']=='limited'and entry['fact']['text']==raw.elements.scale_and_mix.facts[0].text
    assert entry['fact']['metrics']==[]and entry['metric_gaps'][0]['metric_index']==0
    assert entry['metric_gaps'][0]['name']=='营业收入'and 'value'not in entry['metric_gaps'][0]
    assert current['information_gaps'][-1]['fact_id']=='revenue'
    assert '精读'in current['suggested_acquisitions'][-1]['reason']
    assert paper.elements==raw.elements and paper.evidence==raw.evidence
    assert '营业收入：'not in render_workpaper(paper,delivery=current)
    assert raw.elements.scale_and_mix.facts[0].text in render_workpaper(paper,delivery=current)


@pytest.mark.parametrize('indices',[(1,),(-1,),(0,0),(True,)])
def test_invalid_metric_positions_rejected(indices):
    paper,review=restricted(indices=indices)
    with pytest.raises(ValueError):bind_workpaper_use_review(paper,review)


def test_stale_metric_content_and_reordered_array_rejected():
    paper,review=restricted()
    payload=paper.model_dump(mode='json')
    fact=payload['elements']['scale_and_mix']['facts'][0]
    fact['metrics'].append({**fact['metrics'][0],'name':'原有不同第二指标'})
    paper=CompanyProfileWorkpaper.model_validate(payload)
    review['decisions'][0]['fact_sha256']=workpaper_fact_hash(paper.elements.scale_and_mix.facts[0])
    bind_workpaper_use_review(paper,review)
    payload['elements']['scale_and_mix']['facts'][0]['metrics'].reverse()
    with pytest.raises(ValueError,match='stale'):
        bind_workpaper_use_review(CompanyProfileWorkpaper.model_validate(payload),review)


def test_safe_metrics_remain_identical_with_original_index_mapping():
    paper,review=restricted()
    payload=paper.model_dump(mode='json');fact=payload['elements']['scale_and_mix']['facts'][0]
    fact['metrics'].append({**fact['metrics'][0],'name':'另一个有源指标'})
    paper=CompanyProfileWorkpaper.model_validate(payload)
    review['decisions'][0]['fact_sha256']=workpaper_fact_hash(paper.elements.scale_and_mix.facts[0])
    delivery=project_workpaper_delivery(bind_workpaper_use_review(paper,review))
    item=next(x for x in delivery['facts']if x['fact_id']=='revenue')
    assert item['metric_source_indices']==[1]
    assert item['fact']['metrics']==[fact['metrics'][1]]


@pytest.mark.parametrize('state',['withheld','source_only','duplicate'])
def test_whole_fact_restriction_takes_priority(state):
    paper,review=restricted(disposition=state)
    item=next(x for x in project_workpaper_delivery(bind_workpaper_use_review(paper,review))['facts']if x['fact_id']=='revenue')
    assert item['state']==state and item['fact']is None and 'metric_gaps'not in item


def test_legacy_serialization_and_projection_do_not_gain_empty_fields():
    paper=material();review=use_review(paper,{'revenue':'source_checked'})
    original=bind_workpaper_use_review(paper,review)
    payload=original.model_dump(mode='json')
    assert 'metric_restrictions'not in payload['use_review']['decisions'][0]
    with_empty=copy.deepcopy(review);with_empty['decisions'][0]['metric_restrictions']=[]
    assert bind_workpaper_use_review(paper,with_empty).model_dump_json()==original.model_dump_json()
    assert all('metric_gaps'not in f for f in project_workpaper_delivery(original)['facts'])


def test_model_cannot_inject_metric_use_decisions():
    paper,review=restricted()
    with pytest.raises(ValueError):
        WorkpaperDraftResponse.model_validate({'elements':paper.elements.model_dump(mode='json'),'use_review':review})


def test_recovery_preserves_unchanged_components_and_blocks_reordered_content():
    raw,review=restricted();bound=bind_workpaper_use_review(raw,review)
    link=ReviewRecoveryLink(request_id='component-recovery',base_work_id='original-work',
        base_workpaper_sha256=workpaper_hash(bound),report=raw.report,plan_sha256='a'*64,prior_use_review=bound.use_review)
    unchanged=inherit_recovery_use_review(raw,link)
    assert unchanged.use_review.decisions[0].metric_restrictions==bound.use_review.decisions[0].metric_restrictions
    payload=raw.model_dump(mode='json');payload['elements']['scale_and_mix']['facts'][0]['metrics'][0]['name']='模型改了标签'
    changed=inherit_recovery_use_review(CompanyProfileWorkpaper.model_validate(payload),link)
    decision=changed.use_review.decisions[0]
    assert decision.disposition=='withheld'and not decision.metric_restrictions


def test_invalid_report_and_unknown_fact_remain_rejected():
    paper,review=restricted();review['report']['document_version']='wrong-report'
    with pytest.raises(ValueError):bind_workpaper_use_review(paper,review)


def test_original_writer_reader_exports_and_saved_plan_preserve_metric_limits(tmp_path):
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime, CompanyProfileRuntimeRecordV3
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    from tests.unit.test_research.test_company_profile_workpaper import _Provider, _report, PAGES
    writer=CompanyProfileResearchWriter(tmp_path/'output');provider=_Provider()
    runtime=CompanyProfileStageRuntime(writer=writer,provider=provider)
    item={'work_id':'component-use','report':_report(),'pages':PAGES,'processing_identity':workpaper_processing_identity(real=False)}
    for stage in ('acquire','parse','semantic','verify','publish'):
        assert runtime.run_stage(stage,item)['status']=='success'
    assert provider.calls
    record=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    decision=use_review(record.workpaper,{'revenue':'source_checked'})
    decision['decisions'][0]['metric_restrictions']=[{'metric_index':0,'reason':'该原标准化指标不可计算。'}]
    paper=bind_workpaper_use_review(record.workpaper,decision)
    writer.persist(record.model_copy(update={'workpaper':paper}))
    reader=CompanyProfileReadService(tmp_path/'output')
    query=reader.query(('600000.SH',));profile=query['profiles'][0]
    entry=next(x for x in profile['workpaper_delivery']['facts']if x['fact_id']=='revenue')
    assert entry['state']=='limited'and entry['fact']['metrics']==[]
    assert profile['workpaper_summary']['metric_count']==0
    reader.export(('600000.SH',),tmp_path/'export')
    row=next(csv.DictReader((tmp_path/'export/profiles.csv').open()))
    assert row['workpaper_metric_count']=='0'and json.loads(row['workpaper_metric_gaps'])[0]['metric_index']==0
    assert '营业收入：'not in (tmp_path/'export/600000.SH_2025-12-31.md').read_text()
    annual=json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles'][0]
    assert any(f.get('metric_gaps')and not f['fact']['metrics']for f in annual['facts'])
    fresh=CompanyProfileReadService(tmp_path/'output')
    assert fresh.query(('600000.SH',))==query
    review_plan=fresh.plan_review_recovery('600000.SH',request_id='component-plan')
    assert review_plan['workpaper_sha256']==workpaper_hash(paper)
    enrich=fresh.plan_enrichment('600000.SH',request_id='component-read',targets=[{'element':'scale_and_mix','question':'核定该指标主体。'}])
    assert enrich['enrichment_plan']['base_workpaper_sha256']==workpaper_hash(paper)


def test_query_export_follow_exact_component_limited_successor_not_unrestricted_parent(tmp_path):
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime, CompanyProfileRuntimeRecordV3
    from research.company_profile.reads import CompanyProfileReadService
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    from tests.unit.test_research.test_company_profile_workpaper import _Provider, _report, PAGES
    from tests.unit.test_research.test_company_profile_workpaper_delivery import model_review
    writer=CompanyProfileResearchWriter(tmp_path/'output')
    runtime=CompanyProfileStageRuntime(writer=writer,provider=_Provider())
    item={'work_id':'base-unrestricted','report':_report(),'pages':PAGES,'processing_identity':workpaper_processing_identity(real=False)}
    for stage in ('acquire','parse','semantic','verify','publish'):assert runtime.run_stage(stage,item)['status']=='success'
    parent=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
    choice=use_review(parent.workpaper,{'revenue':'source_checked'})
    choice['decisions'][0]['metric_restrictions']=[{'metric_index':0,'reason':'指标元数据不可靠。'}]
    paper=bind_workpaper_use_review(model_review(parent.workpaper),choice)
    link=ReviewRecoveryLink(request_id='linked-component',base_work_id=parent.work_id,
        base_workpaper_sha256=workpaper_hash(parent.workpaper),report=parent.report,plan_sha256='a'*64)
    identity=parent.execution.input_identity.model_copy(update={'processing_identity':{'workpaper_review_recovery':link.model_dump(mode='json')}})
    child=parent.model_copy(update={'work_id':'component-successor','workpaper':paper,
        'execution':parent.execution.model_copy(update={'input_identity':identity})})
    writer.persist(child)
    reader=CompanyProfileReadService(tmp_path/'output')
    profile=reader.query(('600000.SH',))['profiles'][0]
    assert profile['work_id']==child.work_id
    assert next(f for f in profile['workpaper_delivery']['facts']if f['fact_id']=='revenue')['fact']['metrics']==[]
    assert reader.query(('600000.SH',),work_id=parent.work_id)['profiles'][0]['work_id']==parent.work_id
    reader.export(('600000.SH',),tmp_path/'export')
    assert '营业收入：'not in (tmp_path/'export/600000.SH_2025-12-31.md').read_text()
    annual=json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles'][0]
    assert annual['base_work_id']==parent.work_id and annual['latest_reading_work_id']==child.work_id
    changed_payload=paper.model_dump(mode='json')
    changed_payload.pop('use_review')
    changed_payload['elements']['scale_and_mix']['facts'][0]['metrics'][0]['name']='模型改动后的指标标签'
    next_link=ReviewRecoveryLink(request_id='coarser-successor',base_work_id=child.work_id,
        base_workpaper_sha256=workpaper_hash(child.workpaper),report=child.report,plan_sha256='b'*64,
        prior_use_review=child.workpaper.use_review)
    coarser=inherit_recovery_use_review(CompanyProfileWorkpaper.model_validate(changed_payload),next_link)
    assert coarser.use_review.decisions[0].disposition=='withheld'
    assert not coarser.use_review.decisions[0].metric_restrictions
    grand_identity=parent.execution.input_identity.model_copy(update={'processing_identity':{'workpaper_review_recovery':next_link.model_dump(mode='json')}})
    grandchild=parent.model_copy(update={'work_id':'coarse-grandchild','workpaper':coarser,
        'execution':parent.execution.model_copy(update={'input_identity':grand_identity})})
    writer.persist(grandchild)
    fresh=CompanyProfileReadService(tmp_path/'output')
    latest=fresh.query(('600000.SH',))['profiles'][0]
    assert latest['work_id']==grandchild.work_id
    row=next(f for f in latest['workpaper_delivery']['facts']if f['fact_id']=='revenue')
    assert row['state']=='withheld'and row['fact']is None
    assert fresh.query(('600000.SH',),work_id=parent.work_id)['profiles'][0]['workpaper_delivery']['facts'][1]['fact']['metrics']
    assert fresh.query(('600000.SH',),work_id=child.work_id)['profiles'][0]['work_id']==child.work_id
    fresh.export(('600000.SH',),tmp_path/'export-grandchild')
    current=json.loads((tmp_path/'export-grandchild/annual_profiles.json').read_text())['profiles'][0]
    assert current['latest_reading_work_id']==grandchild.work_id
    assert all(f['fact']is None for f in current['facts']if f['fact_id']=='revenue')
    assert '营业收入：'not in (tmp_path/'export-grandchild/600000.SH_2025-12-31.md').read_text()
