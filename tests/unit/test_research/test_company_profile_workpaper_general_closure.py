"""General business slice: fresh entry, independent omissions and local rejection.

Only the external transport is controlled. This is not a real-model quality score.
"""
import asyncio
from copy import deepcopy
from dataclasses import replace
import time

import pytest

from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.workpaper import WorkpaperReviewResponse, WorkpaperRepairVerificationResponse, WorkpaperFact
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from research.storage import ResearchStorageManager
from tests.unit.test_research.test_business_profile_production_operations import _announcement
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_utils.test_llm_gateway import _config, _response
from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
from utils.config_manager import ResearchConfig, ResearchStorageConfig
from utils.llm.testing import ScriptedTransport


@pytest.mark.parametrize('adjustment_accepted', [True, False])
def test_fresh_task_keeps_total_and_segment_when_independent_adjustment_is_rejected(tmp_path, adjustment_accepted):
    pages = deepcopy(list(PAGES))
    pages[1]['text'] = pages[1]['text'].replace('公司提供存贷款和结算服务。', '公司经营制造和服务业务，提供设备与运维服务。')
    pages[2]['text'] = pages[2]['text'].replace('三、风险因素',
        '单位：元\n制造业务营业收入    60.00    50.00\n服务业务营业收入    50.00    45.00\n合并抵销    (10.00)    (5.00)\n三、风险因素')
    index = index_sections(pages)
    selected = _selection(index)
    packet = build_section_packet(_report(), pages, index, selected)
    payload = _draft(packet).model_dump(mode='json')
    payload['elements']['business_structure']['facts'][0]['text'] = '公司经营制造和服务业务，提供设备与运维服务。'
    payload['elements']['products_services'] = dict(status='observed', facts=[{
        **payload['elements']['business_structure']['facts'][0], 'fact_id':'products',
        'text':'公司提供设备与运维服务。'}], limitations=[])
    evidence_ids = [e.evidence_id for e in packet.evidence]
    findings = [dict(finding_id=fid, element='revenue_model', severity='material', issue_type='material_omission',
        affected_fact_id=None, evidence_ids=evidence_ids, action='possible_omission', explanation=reason)
        for fid,reason in [('segment','未列出主要制造业务收入。'),('adjustment','合并抵销应与集团总额分别呈现。')]]
    review = dict(reviewed_fact_ids=['main','products','revenue'], findings=findings)
    template = payload['elements']['scale_and_mix']['facts'][0]
    segment = {**deepcopy(template), 'fact_id':'review-add:segment', 'text':'制造业务营业收入60.00元。',
        'subject_scope':'business_segment', 'metrics':[{
            **template['metrics'][0], 'name':'制造业务营业收入', 'raw_value':'60.00', 'value':'60.00',
            'subject_scope':'business_segment'}]}
    adjustment = {**deepcopy(template), 'fact_id':'review-add:adjustment', 'text':'合并抵销金额-10.00元。',
        'subject_scope':'unclear' if adjustment_accepted else 'consolidated_group',
        'uncertainty_reason':'原文为合并抵销调整列，并非独立经营主体。' if adjustment_accepted else None,
        'metrics':[{
            **template['metrics'][0], 'name':'合并抵销金额', 'raw_value':'-10.00', 'value':'-10.00',
            'subject_scope':'unclear' if adjustment_accepted else 'consolidated_group',
            'uncertainty_reason':'合并调整列，保原文而非集团收入。' if adjustment_accepted else None}]}
    verify = dict(checked_fact_ids=[segment['fact_id'],adjustment['fact_id']], checks=[
        dict(fact_id=segment['fact_id'],accepted=True,evidence_ids=evidence_ids,explanation='制造业务本期列60.00元。'),
        dict(fact_id=adjustment['fact_id'],accepted=adjustment_accepted,evidence_ids=evidence_ids,
            explanation='有源调整列保不确定限定。' if adjustment_accepted else '抵销列不是集团整体收入，主体错误。')])
    WorkpaperReviewResponse.model_validate(review)
    WorkpaperRepairVerificationResponse.model_validate(verify)
    WorkpaperFact.model_validate(segment)
    WorkpaperFact.model_validate(adjustment)
    transport = ScriptedTransport([_response(selected.model_dump(mode='json')), _response(payload),
        _response(review), _response(dict(repaired_facts=[segment,adjustment])), _response(verify)])
    cfg = _config(max_schema_repair_attempts=0)
    cfg = replace(cfg,profiles={**cfg.profiles,'review':replace(cfg.profiles['test'],name='review',reasoning_effort='high')})
    budget = _budget(deadline=time.monotonic()+180)
    client = _client(transport,budget,cfg)
    db=str(tmp_path/'research.db')
    storage=ResearchStorageManager(ResearchConfig(storage=ResearchStorageConfig(db_path=db,financials_db_path=db,
        valuation_db_path=db,interests_db_path=db,quotes_db_path=str(tmp_path/'quotes.db'),attach_quotes_db=False)))
    try:
        storage.initialize()
        BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
            instrument=dict(instrument_id='600000.SH',symbol='600000',exchange='SSE'),
            record=_announcement('closure-fixture','公司2025年年度报告',published_at='2026-04-01T16:00:00+00:00'))
        provider=CommonGatewaySemanticProvider(client=client,profile='test',max_output_tokens=4000,timeout_seconds=20)
        provider.offline_only=True
        identity=workpaper_processing_identity(real=False,review=True)
        identity.update(quality_review_profile='review',repair_profile='test')
        service=CompanyProfileTaskService(storage=storage,output_root=tmp_path/'output',checkpoint_root=tmp_path/'checkpoints',
            page_source=lambda _:dict(report=_report(),pages=pages),provider=provider,
            processing_identity=identity,execution_budget=budget)
        result=asyncio.run(service.execute('run',knowledge_cutoff='2026-11-19',instrument_ids=('600000.SH',),max_items=1,max_elapsed_seconds=180))
        assert result['state']=='completed'
        queried=asyncio.run(service.execute('query',instrument_ids=('600000.SH',)))
        exported=asyncio.run(service.execute('export',instrument_ids=('600000.SH',),output_directory=tmp_path/'export'))
        reader=CompanyProfileReadService(tmp_path/'output')
        assert queried['profiles']==exported['profiles']==reader.query(('600000.SH',))['profiles']
        profile=queried['profiles'][0]
        facts={f['fact_id']:f for element in profile['workpaper']['elements'].values() for f in element['facts']}
        assert facts['revenue']['metrics'][0]['value']=='100.00'
        assert facts[segment['fact_id']]['metrics'][0]['value']=='60.00'
        assert (adjustment['fact_id'] in facts) is adjustment_accepted
        assert profile['core_complete'] is False  # controlled output never becomes real acceptance
        assert len(transport.calls)==5
        assert [r['purpose'] for r in provider.workpaper_receipts]==[
            'select_profile_sections','extract_profile_workpaper','review_profile_workpaper',
            'repair_profile_workpaper','verify_profile_workpaper_repair']
        # Persist events intentionally repeat the same path. Archive unique files,
        # not event count, without changing published_this_round semantics.
        writer=service.runtime.writer
        assert len(writer.paths)>1 and len(set(writer.paths))==1
        assert writer.latest_path()==next(iter(set(writer.paths)))
        api=reader.research_profile('600000.SH')
        visible={f['fact_id'] for f in api['facts'] if f['fact'] is not None}
        assert {'main','products','revenue',segment['fact_id']}<=visible
        assert (adjustment['fact_id'] in visible) is adjustment_accepted
        assert api==CompanyProfileReadService(tmp_path/'output').research_profile('600000.SH')
        assert len(profile['workpaper']['quality_review']['findings'])==2
    finally:
        asyncio.run(client.close())
