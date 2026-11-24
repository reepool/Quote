"""One strict record snapshot per read call, with fresh reads and export drift checks."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile import reads
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileRuntimeRecordV3, CompanyProfileStageRuntime
from research.company_profile.workpaper import CompanyProfileWorkpaper
from research.company_profile.workpaper_delivery import bind_workpaper_use_review
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_review import workpaper_hash
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _Provider, _report
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review


def two_lineages(tmp_path):
    writer=CompanyProfileResearchWriter(tmp_path/'output')
    bases=[]
    for number,instrument in enumerate(('600000.SH','600001.SH')):
        runtime=CompanyProfileStageRuntime(writer=writer,provider=_Provider())
        report=_report().model_copy(update={'instrument_id':instrument})
        item={'work_id':f'base-{number}','report':report,'pages':PAGES,
            'processing_identity':workpaper_processing_identity(real=False)}
        for stage in ('acquire','parse','semantic','verify','publish'):
            assert runtime.run_stage(stage,item)['status']=='success'
        base=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_bytes())
        decision=use_review(base.workpaper,{'revenue':'source_checked'})
        decision['decisions'][0]['metric_restrictions']=[{'metric_index':0,'reason':'独立限制保留原正文。'}]
        base=base.model_copy(update={'workpaper':bind_workpaper_use_review(base.workpaper,decision)})
        writer.persist(base);bases.append(base)
        raw=base.workpaper.model_dump(mode='json')
        fact=deepcopy(raw['elements']['business_structure']['facts'][0]);fact['fact_id']='supplement'
        for element in raw['elements'].values():
            element.update(status='unclear',facts=[],limitations=['本次非目标未读。'])
        raw['elements']['products_services']={'status':'observed','facts':[fact],'limitations':[]}
        raw.pop('use_review',None)
        paper=CompanyProfileWorkpaper.model_validate(raw)
        if number==1:
            paper=bind_workpaper_use_review(paper,use_review(paper,{'supplement':'source_only'}))
        identity=base.execution.input_identity
        plan={'schema_version':'company_profile_enrichment.v1','request_id':f'supplement-{number}',
            'base_work_id':base.work_id,'base_workpaper_sha256':workpaper_hash(base.workpaper),
            'report':report.model_dump(mode='json'),
            'targets':[{'element':'products_services','question':'有哪些已披露服务？'}]}
        writer.persist(base.model_copy(update={'work_id':f'supplement-{number}','workpaper':paper,
            'execution':base.execution.model_copy(update={'input_identity':identity.model_copy(update={
                'processing_identity':{**identity.processing_identity,'workpaper_enrichment':plan}})})}))
    return writer,bases,CompanyProfileReadService(tmp_path/'output')


class CountingAdapter:
    def __init__(self,delegate):self.delegate,self.calls=delegate,[]
    def validate_json(self,raw):
        self.calls.append(hashlib.sha256(raw if isinstance(raw,bytes) else raw.encode()).hexdigest())
        return self.delegate.validate_json(raw)


def test_batch_and_export_reuse_all_strict_records_once_without_losing_lineage_or_limits(tmp_path,monkeypatch):
    writer,bases,reader=two_lineages(tmp_path)
    ids=[b.report.instrument_id for b in bases]
    expected=[reader.research_profile(code) for code in ids]
    counter=CountingAdapter(reads._RUNTIME_ADAPTER);monkeypatch.setattr(reads,'_RUNTIME_ADAPTER',counter)
    result=reader.research_profiles(ids)
    assert result['profiles']==expected and len(counter.calls)==4 and len(set(counter.calls))==4
    assert all(len(p['readings'])==2 for p in result['profiles'])
    revenue=next(r for r in expected[0]['facts'] if r['fact_id']=='revenue')
    assert revenue['state']=='limited' and revenue['fact']['metrics']==[] and revenue['metric_gaps']
    withheld=next(r for r in expected[1]['facts'] if r['fact_id']=='supplement')
    assert withheld['state']=='source_only' and withheld['fact'] is None
    counter.calls.clear()
    exported=reader.export(ids,tmp_path/'export')
    assert len(counter.calls)==4
    assert json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles']==expected
    assert exported['delivered']==2


@pytest.mark.parametrize('damage',['source','use_hash','lineage_hash'])
def test_invalid_source_use_review_and_ancestry_are_still_rejected(tmp_path,damage):
    writer,bases,reader=two_lineages(tmp_path)
    target=writer.output_root/('supplement-0.json' if damage=='lineage_hash' else 'base-0.json')
    raw=json.loads(target.read_text())
    if damage=='source':
        for evidence in raw['workpaper']['evidence']:evidence['quote']='不含所引数值的错误源'
    elif damage=='use_hash':
        raw['workpaper']['use_review']['decisions'][0]['fact_sha256']='0'*64
    else:
        raw['execution']['input_identity']['processing_identity']['workpaper_enrichment']['base_workpaper_sha256']='0'*64
    target.write_text(json.dumps(raw,ensure_ascii=False))
    result=reader.research_profiles(['600000.SH','600001.SH'])
    if damage=='lineage_hash':
        profile=next(p for p in result['profiles'] if p['instrument_id']=='600000.SH')
        assert [r['work_id'] for r in profile['readings']]==['base-0']
    else:
        assert result['missing_instrument_ids']==['600000.SH']
    assert any(p['instrument_id']=='600001.SH' for p in result['profiles'])


def test_same_service_observes_changed_and_new_records_next_call(tmp_path):
    writer,bases,reader=two_lineages(tmp_path)
    first=reader.research_profile('600000.SH')
    changed=bases[0].model_copy(update={'workpaper':bases[0].workpaper.model_copy(update={'limitations':('更新的本地范围。',)})})
    writer.persist(changed)
    second=reader.research_profile('600000.SH')
    assert len(first['readings'])==2 and len(second['readings'])==1
    assert second['readings'][0]['limitations']==['更新的本地范围。']
    writer.persist(changed.model_copy(update={'work_id':'zz-new-base'}))
    assert reader.research_profile('600000.SH')['base_work_id']=='zz-new-base'


@pytest.mark.parametrize('drift',['content','add','remove'])
def test_export_detects_disk_changes_before_writing(tmp_path,monkeypatch,drift):
    writer,bases,reader=two_lineages(tmp_path)
    original=reader.render_profile
    changed=False
    def mutate(record):
        nonlocal changed
        result=original(record)
        if not changed:
            changed=True
            if drift=='content':
                writer.persist(bases[0].model_copy(update={'workpaper':bases[0].workpaper.model_copy(update={'limitations':('并发改稿。',)})}))
            elif drift=='add':writer.persist(bases[0].model_copy(update={'work_id':'new-record'}))
            else:(writer.output_root/'supplement-0.json').unlink()
        return result
    monkeypatch.setattr(reader,'render_profile',mutate)
    with pytest.raises(ValueError,match='changed during export'):
        reader.export(['600000.SH','600001.SH'],tmp_path/'export')
    assert not (tmp_path/'export').exists()


def test_catalog_and_enrichment_plan_each_validate_once(tmp_path,monkeypatch):
    _,_,reader=two_lineages(tmp_path)
    counter=CountingAdapter(reads._RUNTIME_ADAPTER);monkeypatch.setattr(reads,'_RUNTIME_ADAPTER',counter)
    assert reader.research_profile_catalog()['total']==2 and len(counter.calls)==4
    counter.calls.clear()
    plan=reader.plan_enrichment('600000.SH',request_id='planned-only',targets=[{'element':'products_services','question':'具体服务是什么？'}])
    assert plan['state']=='planned' and plan['execution_started'] is False
    assert plan['enrichment_plan']['base_work_id']=='supplement-0' and len(counter.calls)==4


@pytest.mark.parametrize('mode',['stable_unreadable','becomes_unreadable','becomes_readable'])
def test_unrelated_unreadable_file_is_local_but_readability_drift_rejects_export(tmp_path,monkeypatch,mode):
    _,_,reader=two_lineages(tmp_path)
    unrelated=reader.namespace_root/'unrelated.json'
    unrelated.write_text('invalid unrelated JSON')
    unreadable=mode in {'stable_unreadable','becomes_readable'}
    original_read=Path.read_bytes
    original_render=reader.render_profile
    def read(path):
        if path==unrelated and unreadable:
            raise PermissionError('controlled unrelated unreadable file')
        return original_read(path)
    def render(record):
        nonlocal unreadable
        value=original_render(record)
        if mode=='becomes_unreadable':unreadable=True
        elif mode=='becomes_readable':unreadable=False
        return value
    monkeypatch.setattr(Path,'read_bytes',read)
    monkeypatch.setattr(reader,'render_profile',render)
    if mode=='stable_unreadable':
        assert reader.export(['600000.SH','600001.SH'],tmp_path/'export')['delivered']==2
    else:
        with pytest.raises(ValueError,match='changed during export'):
            reader.export(['600000.SH','600001.SH'],tmp_path/'export')
        assert not (tmp_path/'export').exists()
