"""Observed local-consumer defects; these tests never establish model success."""
import copy

import pytest

from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperMetric, WorkpaperSpan, WorkpaperEvidence,
    bind_workpaper_draft, render_workpaper,
)
from tests.unit.test_research.test_company_profile_workpaper import _report, _draft, _selection, PAGES
from tests.unit.test_research.test_company_profile_workpaper_review import material, invoke
from research.company_profile.workpaper_sections import build_section_packet, index_sections


@pytest.mark.parametrize('source_unit', ['百分点', '%', None])
def test_equivalent_literal_point_suffix_does_not_leave_a_measure_word(source_unit):
    data=dict(name='毛利率同比增加',raw_value='2.19个百分点',value='2.19',unit='百分点',
              source_unit=source_unit,currency=None,subject_scope='consolidated_group',period='2025')
    metric=WorkpaperMetric.model_validate(data)
    assert metric.raw_value=='2.19个百分点' and metric.value=='2.19'
    with pytest.raises(ValueError):WorkpaperMetric.model_validate({**data,'value':'-2.19'})
    with pytest.raises(ValueError):WorkpaperMetric.model_validate({**data,'value':'2.35'})
    with pytest.raises(ValueError):WorkpaperMetric.model_validate({**data,'unit':'%'})


def monetary_material(*, uncertain=False, matching=False):
    packet=build_section_packet(_report(),PAGES,index_sections(PAGES),_selection(index_sections(PAGES)))
    quote='租金收入情况\n单位：万元 币种：人民币\n商业综合体 58,322,708.81\n'
    source=WorkpaperEvidence(evidence_id='unit-test-table',document_version=_report().document_version,
        section_path=('管理层讨论与分析','租金收入情况'),physical_pages=(99,),
        source_spans=(WorkpaperSpan(page=99,start=0,end=len(quote),quote=quote),))
    response=_draft(packet)
    fact=response.elements.scale_and_mix.facts[0].model_dump(mode='json')
    fact.update(text='租金原表数字58,322,708.81，单位栏万元，尚未统一。' if uncertain else '租金收入58,322,708.81'+('万元。' if matching else '元。'),evidence_ids=['unit-test-table'])
    metric=fact['metrics'][0]
    metric.update(name='租金收入',raw_value='58,322,708.81',value=None if uncertain else '58322708.81',
        unit=None if uncertain else '万元' if matching else '元',source_unit='万元' if uncertain or matching else '元',
        uncertainty_reason='仅保原文，单位尚未统一。' if uncertain else None)
    payload=response.model_dump(mode='json'); payload['elements']['scale_and_mix']['facts']=[fact]
    from research.company_profile.workpaper import WorkpaperDraftResponse
    return WorkpaperDraftResponse.model_validate(payload),packet,(*packet.evidence,source)


def test_new_binding_withholds_unit_conflict_but_legacy_read_does_not_mutate():
    response,packet,evidence=monetary_material()
    paper=bind_workpaper_draft(response,report=_report(),reading_scope=packet.reading_scope,evidence=evidence)
    assert not paper.elements.scale_and_mix.facts
    assert paper.elements.business_structure.facts==response.elements.business_structure.facts
    assert any('原表万元' in note and '返回元' in note for note in paper.standardization_notes)
    assert '原表万元' in render_workpaper(paper)
    legacy=paper.model_dump(mode='json'); legacy['elements']=response.elements.model_dump(mode='json')
    original=copy.deepcopy(legacy)
    assert CompanyProfileWorkpaper.model_validate(legacy).elements.scale_and_mix.facts
    assert legacy==original


@pytest.mark.parametrize('uncertain,matching', [(True,False),(False,True)])
def test_sourced_uncertain_or_matching_monetary_units_remain_readable(uncertain,matching):
    response,packet,evidence=monetary_material(uncertain=uncertain,matching=matching)
    paper=bind_workpaper_draft(response,report=_report(),reading_scope=packet.reading_scope,evidence=evidence)
    fact=paper.elements.scale_and_mix.facts[0]
    assert fact==response.elements.scale_and_mix.facts[0]
    assert fact.metrics[0].value== (None if uncertain else '58322708.81')


def test_explicit_paragraph_unit_is_not_overridden_by_preceding_table_header():
    from research.company_profile.workpaper import WorkpaperDraftResponse
    response,packet,evidence=monetary_material(matching=True)
    payload=response.model_dump(mode='json'); fact=payload['elements']['scale_and_mix']['facts'][0]
    fact['text']='公司营业收入22.47亿元。'
    fact['metrics'][0].update(raw_value='22.47',value='22.47',unit='亿元',source_unit='亿元')
    quote='财务表\n单位：元\n营业成本100.00\n收入和成本分析\n公司营业收入22.47 亿元。'
    span=WorkpaperSpan(page=99,start=0,end=len(quote),quote=quote)
    source=evidence[-1].model_copy(update={'source_spans':(span,)})
    paper=bind_workpaper_draft(WorkpaperDraftResponse.model_validate(payload),report=_report(),
        reading_scope=packet.reading_scope,evidence=(*evidence[:-1],source))
    assert paper.elements.scale_and_mix.facts[0].metrics[0].unit=='亿元'


def test_known_fact_id_binds_actual_element_without_approving_it():
    original,_,_,review,_=material()
    review['findings'][0].update(element='business_structure',action='human_review')
    before=copy.deepcopy(review)
    _,result,provider,_=invoke([review])
    assert review==before
    assert result.elements.business_structure==original.elements.business_structure
    assert not result.elements.scale_and_mix.facts
    assert result.quality_review.findings[0].element=='scale_and_mix'
    assert result.quality_review.findings[0].resolution=='quarantined'
    assert any('business_structure' in n and 'scale_and_mix' in n for n in result.standardization_notes)
    assert len(provider.calls)==1


@pytest.mark.parametrize('verified', [False,True])
def test_existing_fact_omission_corrects_same_id_and_still_requires_final(verified):
    _,_,fixed,review,verification=material()
    review['findings'][0].update(element='business_structure',action='possible_omission',issue_type='material_omission')
    final=verification if verified else ValueError('offline: no real final response')
    _,result,provider,_=invoke([review,{'repaired_facts':[fixed]},final])
    repair=provider.calls[1]['payload']['repair_targets'][0]
    assert repair['target_fact_id']=='revenue' and repair['finding']['element']=='scale_and_mix'
    assert repair['finding']['action']=='correct'
    assert any('按原编号修订' in note for note in result.standardization_notes)
    assert bool(result.elements.scale_and_mix.facts)==verified
    assert result.quality_review.findings[0].resolution==('corrected_pending_trust' if verified else 'verification_failed')


def test_minor_omission_of_existing_fact_does_not_withdraw_or_request_repair():
    from tests.unit.test_research.test_company_profile_workpaper_review import Provider
    from research.company_profile.workpaper_review import review_workpaper
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    _,packet,_,review,_=material()
    paper=bind_workpaper_draft(_draft(packet),report=_report(),reading_scope=packet.reading_scope,evidence=packet.evidence)
    review['findings'][0].update(action='possible_omission',issue_type='material_omission',severity='minor')
    provider=Provider([review])
    result,_=review_workpaper(paper,packet=packet,provider=provider,work_id='minor-omission',
        identity=workpaper_processing_identity(real=False,review=True))
    assert result.elements==paper.elements and len(provider.calls)==1
    assert result.quality_review.findings[0].resolution=='omission_candidate'
    assert result.quality_review.findings[0].affected_fact_id is None
    assert any('不撤回原事实或触发修订' in note for note in result.standardization_notes)
