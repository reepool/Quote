"""Research delivery grades real reviewed scope, never formal core acceptance."""
import copy
import csv
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime, CompanyProfileRuntimeRecordV3
from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperDraftResponse, WorkpaperModelCall, WorkpaperQualityReview,
    WorkpaperReviewFinding, bind_workpaper_draft, render_workpaper, workpaper_fact_hash,
)
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, bind_saved_model_review_scope, project_workpaper_delivery
from research.company_profile.workpaper_review import workpaper_hash
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _draft, _report, _selection


def material():
    packet=build_section_packet(_report(),PAGES,index_sections(PAGES),_selection(index_sections(PAGES)))
    return bind_workpaper_draft(_draft(packet),report=_report(),reading_scope=packet.reading_scope,evidence=packet.evidence,
        origin='controlled_fixture')


def use_review(paper, choices):
    facts={f.fact_id:f for e in paper.elements.model_dump().keys() for f in getattr(paper.elements,e).facts}
    return {'report':paper.report.model_dump(mode='json'),'reviewer':'independent unit source review',
        'reference':'unit/source-check','reference_sha256':'a'*64,
        'decisions':[{'fact_id':fid,'fact_sha256':workpaper_fact_hash(facts[fid]),'disposition':state,
            'evidence_ids':list(facts[fid].evidence_ids),'reason':'本对象按引用核对后的使用范围。'} for fid,state in choices.items()]}


def model_review(paper, *, status='completed', hashes=True):
    # Synthetic metadata exercises the branch, not a real model-quality result.
    paper=paper.model_copy(update={'origin':'llm_generated'})
    call=WorkpaperModelCall(purpose='review_profile_workpaper',request_id='synthetic-review',model='synthetic-model',
        source_label='synthetic:source',contract_status='accepted',response_hash='a'*64,finish_reason='stop',
        scope_kind='selected_source_sections',section_ids=tuple(s.section_id for s in paper.reading_scope.sections),
        physical_pages=tuple(sorted({s.page for e in paper.evidence for s in e.source_spans})))
    bound={f.fact_id:workpaper_fact_hash(f) for e in paper.elements.model_dump().keys() for f in getattr(paper.elements,e).facts}
    review=WorkpaperQualityReview(status=status,original_draft_hash=workpaper_hash(paper),checked_fact_ids=tuple(bound),
        checked_fact_hashes=bound if hashes else None,calls=(call,))
    return paper.model_copy(update={'quality_review':review})


def test_independent_checked_pending_and_raw_are_distinct():
    paper=material();before=paper.model_dump(mode='json')
    bound=bind_workpaper_use_review(paper,use_review(paper,{'main':'source_checked'}))
    delivery=project_workpaper_delivery(bound)
    assert delivery['counts']['reviewed']==1 and delivery['counts']['pending']==1
    assert delivery['facts'][0]['review_basis']=='independent_source_check'
    assert delivery['formal_acceptance']=='not_assessed' and delivery['production_authorization']=='not_authorized'
    after=bound.model_dump(mode='json');after.pop('use_review')
    assert after==before and 'use_review' not in before


@pytest.mark.parametrize('state',['source_only','withheld','duplicate'])
def test_restriction_overrides_model_completed_without_erasing_raw(state):
    paper=model_review(material())
    bound=bind_workpaper_use_review(paper,use_review(paper,{'main':state}))
    delivery=project_workpaper_delivery(bound)
    entry=next(e for e in delivery['facts'] if e['fact_id']=='main')
    assert entry['state']==state and entry['fact'] is None
    assert bound.elements.business_structure.facts==paper.elements.business_structure.facts
    body=render_workpaper(bound,delivery=delivery).split('## 阅读范围与取舍')[0]
    assert '公司提供存贷款和结算服务。' not in body
    assert '报告表列本期营业收入100.00元。' in body


@pytest.mark.parametrize('failure',['version','fact','hash','evidence','duplicate'])
def test_invalid_or_stale_independent_use_review_rejected(failure):
    paper=material();review=use_review(paper,{'main':'source_checked'})
    if failure=='version':review['report']['document_version']='another-report'
    elif failure=='fact':review['decisions'][0]['fact_id']='unknown'
    elif failure=='hash':review['decisions'][0]['fact_sha256']='0'*64
    elif failure=='evidence':review['decisions'][0]['evidence_ids']=['unknown']
    else:review['decisions'].append(copy.deepcopy(review['decisions'][0]))
    with pytest.raises(ValueError):bind_workpaper_use_review(paper,review)


def test_model_cannot_author_independent_review_metadata():
    paper=material()
    payload={'elements':paper.elements.model_dump(mode='json'),'use_review':use_review(paper,{'main':'source_checked'})}
    with pytest.raises(ValueError):WorkpaperDraftResponse.model_validate(payload)


@pytest.mark.parametrize('status',['completed','incomplete','completed_with_open_findings'])
def test_actual_model_scope_requires_current_content_binding_not_company_state(status):
    paper=model_review(material(),status=status)
    assert project_workpaper_delivery(paper)['counts']['reviewed']==2
    payload=paper.model_dump(mode='json')
    payload['elements']['business_structure']['facts'][0]['text']='公司提供另一种服务。'
    changed=CompanyProfileWorkpaper.model_validate(payload)
    states={e['fact_id']:e['state'] for e in project_workpaper_delivery(changed)['facts']}
    assert states=={'main':'pending','revenue':'reviewed'}


def test_legacy_exact_draft_hash_can_bind_unchanged_scope_but_not_changed_text():
    paper=model_review(material(),hashes=False)
    assert 'checked_fact_hashes' not in paper.quality_review.model_dump(mode='json')
    assert project_workpaper_delivery(paper)['counts']['reviewed']==2
    payload=paper.model_dump(mode='json');payload['elements']['business_structure']['facts'][0]['text']='改变原句。'
    assert project_workpaper_delivery(CompanyProfileWorkpaper.model_validate(payload))['counts']['reviewed']==0


def test_saved_initial_input_can_bind_only_unchanged_facts_after_repair():
    paper=model_review(material(),hashes=False)
    original=paper.model_copy(update={'quality_review':None})
    payload=paper.model_dump(mode='json')
    payload['elements']['business_structure']['facts'][0]['text']='改变原句。'
    changed=CompanyProfileWorkpaper.model_validate(payload)
    bound=bind_saved_model_review_scope(changed,original)
    states={e['fact_id']:e['state'] for e in project_workpaper_delivery(bound)['facts']}
    assert states=={'main':'pending','revenue':'reviewed'}
    assert bound.quality_review.calls==changed.quality_review.calls
    assert bound.quality_review.original_draft_hash==changed.quality_review.original_draft_hash
    wrong=original.model_copy(update={'limitations':('不是原输入。',)})
    with pytest.raises(ValueError):bind_saved_model_review_scope(changed,wrong)


def test_retained_initial_reply_cannot_be_relabelled_as_checking_new_seed():
    paper=model_review(material(),hashes=False)
    original=paper.model_copy(update={'quality_review':None})
    review=paper.quality_review.model_copy(update={'retained_request_ids':('synthetic-review',)})
    paper=paper.model_copy(update={'quality_review':review})
    assert project_workpaper_delivery(paper)['counts']['reviewed']==0
    with pytest.raises(ValueError,match='retained'):
        bind_saved_model_review_scope(paper,original)


def test_review_status_without_actual_accepted_call_never_means_reviewed():
    paper=model_review(material())
    review=paper.quality_review.model_copy(update={'calls':()})
    assert project_workpaper_delivery(paper.model_copy(update={'quality_review':review}))['counts']['reviewed']==0


def test_repaired_content_needs_matching_final_not_old_initial_id():
    paper=model_review(material())
    fact=paper.elements.business_structure.facts[0]
    finding=WorkpaperReviewFinding(finding_id='repair',element='business_structure',severity='material',issue_type='other',
        affected_fact_id=fact.fact_id,evidence_ids=fact.evidence_ids,explanation='当前修订待最后源核。',
        resolution='corrected_pending_trust',original_fact=fact,revised_fact=fact)
    review=paper.quality_review.model_copy(update={'findings':(finding,)})
    assert project_workpaper_delivery(paper.model_copy(update={'quality_review':review}))['counts']['pending']==1
    final=review.calls[0].model_copy(update={'purpose':'verify_profile_workpaper_repair','request_id':'synthetic-final'})
    review=review.model_copy(update={'calls':(*review.calls,final)})
    assert project_workpaper_delivery(paper.model_copy(update={'quality_review':review}))['counts']['reviewed']==2


def test_uncertainty_remains_readable_and_limited_not_forced_exact():
    paper=material();payload=paper.model_dump(mode='json')
    payload['elements']['scale_and_mix']['facts'][0]['uncertainty_reason']='原文保留范围不确定，不作精确比较。'
    limited=CompanyProfileWorkpaper.model_validate(payload)
    delivery=project_workpaper_delivery(limited)
    assert delivery['counts']['limited']==1 and delivery['facts'][1]['fact']['metrics'][0]['raw_value']=='100.00'
    assert delivery['pending_review_count']==2 and '待核' in delivery['facts'][1]['label']
    assert '报告表列本期营业收入100.00元。' in render_workpaper(limited,delivery=delivery)


def test_source_checked_fact_with_explicit_use_limit_explains_it_in_markdown():
    paper=material();review=use_review(paper,{'revenue':'source_checked'})
    review['decisions'][0]['limitation']='只按报告期表列收入使用，不推作收现。'
    paper=bind_workpaper_use_review(paper,review)
    delivery=project_workpaper_delivery(paper)
    assert delivery['counts']['limited']==1 and delivery['pending_review_count']==1
    assert '只按报告期表列收入使用，不推作收现。' in render_workpaper(paper,delivery=delivery)


@pytest.mark.parametrize('excluded', ['withheld', 'source_only', 'duplicate'])
@pytest.mark.parametrize('uncertain', [False, True])
def test_original_writer_query_export_csv_and_fresh_read_agree_with_restrictions(excluded, uncertain):
    with TemporaryDirectory(prefix='workpaper-cz-delivery-') as directory:
        root=Path(directory);provider=_Provider();writer=CompanyProfileResearchWriter(root/'output')
        runtime=CompanyProfileStageRuntime(writer=writer,provider=provider)
        from research.company_profile.workpaper_llm import workpaper_processing_identity
        item={'work_id':'cz-controlled','report':_report(),'pages':PAGES,
            'processing_identity':workpaper_processing_identity(real=False)}
        for stage in ('acquire','parse','semantic','verify','publish'):assert runtime.run_stage(stage,item)['status']=='success'
        record=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
        payload=record.workpaper.model_dump(mode='json')
        gap_reason='未获取成功：具体产品子类；建议有需要时从业务介绍补充。'
        payload['elements']['products_services'].update(status='extraction_failed',facts=[],limitations=[gap_reason])
        payload['limitations']=[gap_reason]
        if uncertain:
            payload['elements']['scale_and_mix']['facts'][0]['metrics'][0].update(
                value=None,uncertainty_reason='只保留原值，不参与精确比较。')
        paper=CompanyProfileWorkpaper.model_validate(payload)
        paper=bind_workpaper_use_review(paper,use_review(paper,{'main':excluded,'revenue':'source_checked'}))
        writer.persist(record.model_copy(update={'workpaper':paper}))
        reader=CompanyProfileReadService(root/'output');query=reader.query(('600000.SH',))
        export=reader.export(('600000.SH',),root/'export')
        assert query['profiles']==export['profiles']==CompanyProfileReadService(root/'output').query(('600000.SH',))['profiles']
        profile=query['profiles'][0]
        assert profile['workpaper_summary']['business_description']==''
        assert profile['workpaper_delivery']['counts'][excluded]==1
        assert profile['workpaper']['elements']['business_structure']['facts'][0]['text']=='公司提供存贷款和结算服务。'
        assert profile['accepted_facts']==[] and not profile['core_complete']
        row=next(csv.DictReader((root/'export/profiles.csv').open()))
        visible_state='limited' if uncertain else 'reviewed'
        assert row['workpaper_delivery_'+excluded+'_count']=='1'
        assert row['workpaper_delivery_'+visible_state+'_count']=='1'
        assert row['accepted_fact_count']=='0' and row['workpaper_business_description']==''
        research=json.loads((root/'export/research_profiles.json').read_text())
        assert research['research_only'] and research['formal_acceptance']=='not_assessed'
        assert research['production_authorization']=='not_authorized'
        assert len(research['profiles'])==1
        exported=research['profiles'][0]
        assert 'workpaper' not in exported and 'findings' not in exported['review']
        assert exported['delivery']==profile['workpaper_delivery']
        assert exported['generation']==profile['workpaper']['generation']
        assert exported['reading_scope']==profile['workpaper']['reading_scope']
        assert exported['limitations']==[gap_reason]
        assert exported['source_element_statuses']['products_services']=={
            'status':'extraction_failed','limitations':[gap_reason]}
        assert {'element':'products_services','status':'extraction_failed','reasons':[gap_reason]} in exported['delivery']['information_gaps']
        assert gap_reason in profile['workpaper_markdown']
        assert exported['report']==profile['report']
        entries={f['fact_id']:f for f in exported['delivery']['facts']}
        assert entries['main']['state']==excluded and entries['main']['fact'] is None
        assert entries['revenue']['state']==visible_state
        assert entries['revenue']['fact']['metrics'][0]['raw_value']=='100.00'
        assert (entries['revenue']['fact']['metrics'][0]['value'] is None)==uncertain
        assert {e['evidence_id'] for e in exported['evidence']}=={
            eid for entry in exported['delivery']['facts'] for eid in entry['evidence_ids']}
        manifest=json.loads((root/'export/export_manifest.json').read_text())
        assert str(root/'export/research_profiles.json') in export['files']
        assert str(root/'export/research_profiles.json') in manifest['files']
        assert len(provider.calls)==2
    assert not root.exists()
