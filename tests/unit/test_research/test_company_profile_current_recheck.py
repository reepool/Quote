"""Current-draft source recheck: no rewind, no historical answers, no unlock."""
import asyncio
from copy import deepcopy
import json

import pytest

from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.runtime import CompanyProfileRuntimeRecordV3, CompanyProfileStageRuntime
from research.company_profile.workpaper import workpaper_elements, workpaper_fact_hash
from research.company_profile.workpaper_delivery import bind_workpaper_use_review, project_workpaper_delivery
from research.company_profile.workpaper_review import (
    CURRENT_DRAFT_REVIEW_POLICY, METRIC_LIMIT_POLICY, ReviewRecoveryLink,
    current_review_binding, current_review_draft, inherit_recovery_use_review,
    prepare_review_recovery, review_workpaper, workpaper_hash,
)
from research.company_profile.workpaper_sections import SectionPacket
from tests.unit.test_research.test_company_profile_review_execution import failed_record, storage_for
from tests.unit.test_research.test_company_profile_review_failures import gateway
from tests.unit.test_research.test_company_profile_review_metric_limits import content, review
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report
from tests.unit.test_research.test_company_profile_workpaper_delivery import use_review
from tests.unit.test_utils.test_llm_gateway import _response


def completed_record(root, *, quarantine=False):
    writer, base, _, _, verify = failed_record(root)
    seed, resume, retained = prepare_review_recovery(base)
    provider, identity, _, client, _ = gateway([_response(verify)])
    try:
        current, receipts = review_workpaper(seed,
            packet=SectionPacket(report=seed.report, reading_scope=seed.reading_scope, evidence=seed.evidence),
            provider=provider, work_id='completed', identity=identity, resume=resume)
    finally:
        asyncio.run(client.close())
    if quarantine:
        # A prior ordinary source review removed the defective fact entirely.
        issue = current.quality_review.findings[0].model_copy(update={
            'resolution': 'quarantined', 'revised_fact': None})
        current = current.model_copy(update={'elements': current.elements.model_copy(update={
            'scale_and_mix': current.elements.scale_and_mix.model_copy(update={'facts': (), 'status': 'unclear',
                'limitations': ('旧问题仍未取得可靠收入。',)})}),
            'quality_review': current.quality_review.model_copy(update={'findings': (issue,)})})
    data = current.model_dump(mode='json')
    data['elements']['business_structure']['limitations'].append('A-only secret annotation, not a new model answer.')
    current = current.__class__.model_validate(data)
    current = bind_workpaper_use_review(current, use_review(current, {'main': 'source_only'}))
    record = base.model_copy(update={'workpaper': current, 'workpaper_receipts': (*retained, *receipts)})
    writer.persist(record)
    return writer, record


@pytest.mark.parametrize('quarantine', [False, True])
def test_current_plan_batch_writer_fresh_read_and_exports_do_not_rewind(tmp_path, quarantine):
    writer, base = completed_record(tmp_path, quarantine=quarantine)
    old = writer.latest_path().read_bytes()
    reader = CompanyProfileReadService(tmp_path/'output')
    assert reader.plan_review_recovery('600000.SH', request_id='default')['state'] == 'not_needed'
    plan = reader.plan_review_recovery('600000.SH', request_id='current',
        source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    assert plan['state'] == 'planned'
    seed, resume, receipts = prepare_review_recovery(base, source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    assert resume is None and len(receipts) == 2
    assert seed.elements == base.workpaper.elements
    assert plan['current_review_binding'] == current_review_binding(seed)
    ids = [f.fact_id for e in workpaper_elements(seed).values() for f in e.facts]
    provider, identity, transport, client, budget = gateway([_response(dict(reviewed_fact_ids=ids, findings=[]))])
    service = CompanyProfileTaskService(storage=storage_for(tmp_path), output_root=tmp_path/'output',
        checkpoint_root=tmp_path/'checkpoints', page_source=lambda _: {'report': _report(), 'pages': PAGES},
        provider=provider, processing_identity=identity, execution_budget=budget)
    try:
        result = asyncio.run(service.execute_review_batch([plan], knowledge_cutoff='2026-11-24', max_elapsed_seconds=25))
        assert result['state'] == 'completed' and len(transport.calls) == 1
        assert result['items'][0]['review_status'] == ('completed_with_open_findings' if quarantine else 'completed')
        sent = json.loads(transport.calls[0]['payload']['messages'][-1]['content'])
        assert sent['draft'] == current_review_draft(seed)
        assert 'A-only' not in json.dumps(sent) and 'quality_review' not in sent['draft'] and 'use_review' not in sent['draft']
        assert sent['source_text'] == SectionPacket(report=seed.report,
            reading_scope=seed.reading_scope, evidence=seed.evidence).model_text()
        new_reader = CompanyProfileReadService(tmp_path/'output')
        profile = new_reader.research_profile('600000.SH')
        visible = [f['fact'] for f in profile['facts'] if f['fact']]
        assert not any('净利润' in f['text'] for f in visible)
        assert any('营业收入' in f['text'] for f in visible) is (not quarantine)
        assert all(f['fact_id'] != 'main' for f in visible)  # Independent whole guard survives.
        assert any('旧问题' in ' '.join(g.get('reasons', [])) for g in profile['information_gaps']) is quarantine
        new_reader.export(('600000.SH',), export_directory=tmp_path/'export')
        assert json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles'][0] == profile
        assert (tmp_path/'export/annual_profiles.md').is_file() and (tmp_path/'export/profiles.csv').is_file()
        assert writer.output_root.joinpath(base.work_id+'.json').read_bytes() == old
        assert asyncio.run(service.execute_review_recovery(plan))['state'] == 'already_processed'
        assert len(transport.calls) == 1
    finally:
        asyncio.run(client.close())


def test_current_wrong_plan_binding_rejects_before_send(tmp_path):
    _, base = completed_record(tmp_path)
    reader = CompanyProfileReadService(tmp_path/'output')
    plan = reader.plan_review_recovery('600000.SH', request_id='no-send', source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    provider, identity, transport, client, budget = gateway([])
    service = CompanyProfileTaskService(storage=storage_for(tmp_path), output_root=tmp_path/'output',
        checkpoint_root=tmp_path/'checkpoints', page_source=lambda _: {'report': _report(), 'pages': PAGES},
        provider=provider, processing_identity=identity, execution_budget=budget)
    try:
        for damage in ['base', 'report', 'source_packet_sha256', 'current_draft_sha256', 'request_draft_sha256']:
            bad = deepcopy(plan)
            if damage == 'base':
                bad['workpaper_sha256'] = '0'*64
            elif damage == 'report':
                bad['report']['document_version'] = '0'*64
            else:
                bad['current_review_binding'][damage] = '0'*64
            with pytest.raises(ValueError):
                asyncio.run(service.execute_review_recovery(bad))
        assert transport.calls == []
    finally:
        asyncio.run(client.close())


def test_existing_model_metric_guards_survive_new_no_findings_and_changed_fact_fails_closed():
    paper, packet, reply = content()
    prior, _, _ = review(paper, packet, reply)
    seed = prior.model_copy(update={'quality_review': None})
    fresh, _, _ = review(seed, packet, dict(reviewed_fact_ids=['main', 'revenue'], findings=[]))
    link = ReviewRecoveryLink(request_id='guards', base_work_id='old', base_workpaper_sha256=workpaper_hash(prior),
        report=paper.report, plan_sha256='0'*64, current_draft_policy=CURRENT_DRAFT_REVIEW_POLICY,
        prior_quality_review=prior.quality_review)
    result = inherit_recovery_use_review(fresh, link)
    assert inherit_recovery_use_review(result, link)==result
    row = next(f for f in project_workpaper_delivery(result)['facts'] if f['fact_id']=='revenue')
    assert row['state']=='limited' and len(row['fact']['metrics'])==1
    assert row['metric_gaps'][0]['review_basis']=='model_source_check'
    assert row['metric_gaps'][0]['model_finding_ids']==['inherited:metric-scope']
    raw = fresh.model_dump(mode='json')
    raw['elements']['scale_and_mix']['facts'][0]['metrics'].reverse()
    changed = inherit_recovery_use_review(fresh.__class__.model_validate(raw), link)
    assert next(f for f in project_workpaper_delivery(changed)['facts'] if f['fact_id']=='revenue')['fact'] is None


def test_legacy_recovery_link_serialization_has_no_new_keys():
    paper, *_ = content()
    link=ReviewRecoveryLink(request_id='legacy',base_work_id='old',base_workpaper_sha256=workpaper_hash(paper),
        report=paper.report,plan_sha256='0'*64)
    assert 'current_draft_policy' not in link.model_dump(mode='json')
    assert 'prior_quality_review' not in link.model_dump(mode='json')


def test_failed_current_recheck_resumes_exact_parent_not_earliest_extract(tmp_path):
    writer, base=completed_record(tmp_path)
    reader=CompanyProfileReadService(tmp_path/'output')
    seed, _, receipts=prepare_review_recovery(base,source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY)
    link=ReviewRecoveryLink(request_id='current-fails',base_work_id=base.work_id,
        base_workpaper_sha256=workpaper_hash(base.workpaper),report=base.report,plan_sha256='0'*64,
        current_draft_policy=CURRENT_DRAFT_REVIEW_POLICY,prior_use_review=base.workpaper.use_review,
        prior_quality_review=base.workpaper.quality_review)
    empty=_response(None,choices=[{'message':{'content':''},'finish_reason':None}])
    provider,identity,transport,client,_=gateway([empty,empty])
    from hashlib import sha256
    identity.update(source_recheck_policy=CURRENT_DRAFT_REVIEW_POLICY,current_review_binding=current_review_binding(seed),
        workpaper_review_recovery=link.model_dump(mode='json'),review_existing_draft_hash=workpaper_hash(seed),
        review_seed_receipts_hash=sha256(json.dumps(receipts,ensure_ascii=False,sort_keys=True).encode()).hexdigest())
    runtime=CompanyProfileStageRuntime(writer=writer,provider=provider)
    item={'work_id':'failed-current','report':base.report,'pages':PAGES,'processing_identity':identity,
        'review_seed':seed.model_dump(mode='json'),'review_seed_receipts':receipts}
    try:
        for stage in ('acquire','parse','semantic','verify','publish'):
            assert runtime.run_stage(stage,item)['status']=='success'
        failed=CompanyProfileRuntimeRecordV3.model_validate_json(writer.latest_path().read_text())
        assert failed.workpaper.quality_review.status=='failed' and len(transport.calls)==2
        assert reader.plan_review_recovery('600000.SH',request_id='resume-current')['state']=='planned'
        restored,resume,_=reader._prepare_review_recovery(failed)
        assert resume is None and restored==seed
        assert restored.elements.scale_and_mix.facts[0].metrics[0].name=='营业收入'
        # The old policy still enforces its original-draft identity. A review
        # of the corrected current seed cannot be relabelled as that old input.
        with pytest.raises(ValueError,match='saved original draft cannot be reconstructed exactly'):
            prepare_review_recovery(failed,source_recheck_policy=METRIC_LIMIT_POLICY)
        with pytest.raises(ValueError,match='exact saved parent'):
            prepare_review_recovery(failed)
    finally:
        asyncio.run(client.close())
