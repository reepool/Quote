"""Versioned business-local recovery; all model transports here are controlled."""
import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.company_profile import task_entry
from research.company_profile.contracts import SemanticProviderError
from research.company_profile.models import ReportIdentity
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.stage5_provider import CommonGatewaySemanticProvider
from research.company_profile.workpaper import ELEMENT_LABELS, WorkpaperDraftResponse, workpaper_elements
from research.company_profile.workpaper_llm import (
    AUTO_FORMAT_RECOVERY, EXTRACTION_INSTRUCTIONS, normalize_retained_workpaper_format,
    run_workpaper, workpaper_processing_identity,
)
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from research.business_profile_production_operations import BusinessProfileAnnouncementFrontierRepository
from tests.unit.test_research.test_business_profile_production_operations import _announcement
from tests.unit.test_research.test_business_profile_pdf_artifacts import _pdf_bytes
from tests.unit.test_research.test_company_profile_review_execution import storage_for
from tests.unit.test_research.test_company_profile_configured_task import plan_for, save, config_for_test
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _report, _selection, _draft
from tests.unit.test_research.test_company_profile_workpaper_format_consumption import NoModelCalls
from tests.unit.test_utils.test_llm_gateway import _config, _response
from tests.unit.test_utils.test_llm_physical_budget import _budget, _client
from utils.llm.testing import ScriptedTransport

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def _deny_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('automatic recovery tests must not access the network')
    monkeypatch.setattr('socket.getaddrinfo', denied)
    monkeypatch.setattr('socket.create_connection', denied)


def _malformed(payload):
    nested = deepcopy(payload)
    for name in ('limitations', 'company_features', 'supply_chain'):
        nested['elements'][name] = nested.pop(name, None if name != 'limitations' else [])
    return json.dumps(nested, ensure_ascii=False)[:-1]


def _provider(responses):
    transport = ScriptedTransport(responses)
    budget = _budget()
    client = _client(transport, budget, _config(max_retries=1, max_schema_repair_attempts=1))
    provider = CommonGatewaySemanticProvider(client=client, profile='test', max_output_tokens=12000, timeout_seconds=120)
    provider.offline_only = True
    return provider, transport, client


@pytest.mark.parametrize('damage', [None, 'length', 'array', 'string', 'missing_element', 'root_conflict',
    'duplicate', 'nan', 'overflow', 'number_source', 'legacy'])
def test_live_raw_recovery_strictness_and_no_paid_format_retry(damage):
    index = index_sections(PAGES)
    selected = _selection(index)
    draft = _draft(build_section_packet(_report(), PAGES, index, selected)).model_dump(mode='json')
    if damage == 'number_source':
        draft['elements']['scale_and_mix']['facts'][0]['metrics'][0].update(raw_value='999.00', value='999.00')
    if damage == 'missing_element': draft['elements'].pop('business_structure')
    raw = _malformed(draft)
    if damage == 'array': raw = raw[:-2]
    elif damage == 'string': raw = raw[:raw.index('公司提供') + 2]
    elif damage == 'root_conflict': raw = raw.replace('"elements":', '"limitations":[],"elements":', 1)
    elif damage == 'duplicate': raw = raw.replace('"elements":', '"schema_version":"duplicate","elements":', 1)
    elif damage in ('nan', 'overflow'):
        raw = raw.replace('"100.00"', 'NaN' if damage == 'nan' else '1e999', 1)
    response = _response(None, choices=[{'message': {'content': raw}, 'finish_reason': 'length' if damage == 'length' else 'stop'}])
    provider, transport, client = _provider([_response(selected.model_dump(mode='json')), response, response])
    identity = workpaper_processing_identity(real=False)
    if damage == 'legacy': identity['workpaper_format_recovery'] = 'validation_feedback.v2.syntax-diagnosis'
    try:
        if damage not in (None, 'number_source'):
            with pytest.raises((ValueError, SemanticProviderError)):
                run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
                    provider=provider, work_id='automatic-strict')
        else:
            result = run_workpaper(report=_report(), pages=PAGES, processing_identity=identity,
                provider=provider, work_id='automatic-strict')
            assert result.workpaper.generation.local_format_normalization
            assert result.workpaper.generation.extraction.contract_status == 'rejected'
            assert result.workpaper.generation.extraction.model
            assert result.receipts[-1]['raw_content'] == raw
            assert result.workpaper.generation.extraction.response_hash == hashlib.sha256(raw.encode()).hexdigest()
            assert result.workpaper.elements.business_structure.facts
            assert bool(result.workpaper.elements.scale_and_mix.facts) is (damage is None)
            assert result.workpaper.quality_review is None
        assert len(transport.calls) == (3 if damage == 'legacy' else 2)
        if damage == 'legacy':
            feedback=json.loads(transport.calls[-1]['payload']['messages'][-1]['content'])
            assert feedback['prior_response']==raw and feedback['validation_error']
        else:
            assert 'response_format' not in transport.calls[1]['payload']
            assert any('schema' in m['content'].lower() for m in transport.calls[1]['payload']['messages'] if m['role']=='system')
    finally:
        asyncio.run(client.close())


def _core_draft():
    elements = {key: dict(status='unclear', facts=[], limitations=['Not obtained from selected source.']) for key in ELEMENT_LABELS}
    for key, fid, text in [('business_structure','main','Company provides loans and settlement services.'),
        ('products_services','products','Loans and settlement services.'),
        ('revenue_model','revenue','Company earns service fees.')]:
        elements[key] = dict(status='observed', facts=[dict(fact_id=fid, text=text, subject_scope='consolidated_group',
            period='2025', evidence_ids=['source-0001'], metrics=[])], limitations=[])
    return dict(schema_version='company_profile_workpaper_draft.v1', elements=elements, limitations=[],
        company_features=None, supply_chain=None)


def test_disk_job_mixed_batch_review_failure_and_pause_resume_without_reextract(tmp_path, monkeypatch):
    storage = storage_for(tmp_path)
    for code in ('600001', '600002'):
        BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
            instrument={'instrument_id':code+'.SH','symbol':code,'exchange':'SSE'},
            record=_announcement(code+'-report','公司2025年年度报告',published_at='2026-04-01T16:00:00+00:00'))
    text = '1. Business\nCompany provides loans and settlement services. Company earns service fees.'
    pdf = tmp_path/'source.pdf'; pdf.write_bytes(_pdf_bytes([text]))
    digest = hashlib.sha256(pdf.read_bytes()).hexdigest()
    def source(item):
        return dict(report=ReportIdentity(instrument_id=item['instrument_id'],report_id=item['announcement_id'],
            document_version=digest,report_period=item['report_period'],published_at=item['metadata']['published_at']),
            pages=({'page':1,'text':text,'readable':True},),source_pdf_path=str(pdf))
    config = config_for_test(monkeypatch)
    sequence=[]
    class Transport(ScriptedTransport):
        async def send(self,url,headers,payload,timeout_seconds):
            # A genuine gateway schema retry appends feedback after the source.
            data=next(value for message in payload['messages'] if message['role']=='user'
                and isinstance(value:=json.loads(message['content']),dict) and 'report' in value)
            code=data['report']['instrument_id']
            if 'sections' in data:
                purpose='select_profile_sections'; selected=max(data['sections'],key=lambda row:row[3])[0]
                reply=_response(dict(selected_section_ids=[selected],rationale='Read the compact business section.',deferred=[],stop_reason='Bounded reading.'))
            elif 'draft' in data:
                purpose='review_profile_workpaper'
                reply=_response(dict(reviewed_fact_ids=['main','products','revenue'],findings=[])) if code=='600000.SH' else _response(None,choices=[{'message':{'content':''},'finish_reason':'stop'}])
            else:
                purpose='extract_profile_workpaper'; draft=_core_draft()
                if code=='600000.SH': reply=_response(draft)
                else:
                    raw=_malformed(draft)
                    if code=='600002.SH': raw=raw[:raw.index('Company provides')+5]
                    reply=_response(None,choices=[{'message':{'content':raw},'finish_reason':'stop'}])
                assert 'response_format' not in payload
            sequence.append(dict(instrument_id=code,purpose=purpose,physical_timeout_seconds=timeout_seconds))
            self.responses.append(reply)
            return await super().send(url,headers,payload,timeout_seconds)
    transport=Transport([])
    monkeypatch.setattr(task_entry,'LlmClient',lambda config,physical_budget:_client(transport,physical_budget,config))
    plan=plan_for(tmp_path); plan['instrument_ids']=['600000.SH','600001.SH','600002.SH']
    plan['document_versions']={code:digest for code in plan['instrument_ids']}
    path=save(tmp_path,plan)
    def invoke(action,file):
        return asyncio.run(task_entry.execute_configured_task(action=action,storage=storage,execution_plan_file=file,
            page_source=source,llm_config=config))
    first=invoke('first_read',path)
    assert len(sequence)==6
    reader=CompanyProfileReadService(tmp_path/'output')
    batch=reader.research_profiles(plan['instrument_ids'])
    assert batch['delivered']==2 and batch['missing_instrument_ids']==['600002.SH']
    for profile in batch['profiles']:
        assert len([f for f in profile['facts'] if f['fact'] is not None])==3
        assert profile['readings'][0]['review_status']=='not_reviewed'
        assert profile['readings'][0]['generation']['extraction']['contract_status']==('accepted' if profile['instrument_id']=='600000.SH' else 'rejected')
    run_root=tmp_path/'checkpoints/executions/first'
    completed_snapshot={p:(p.read_bytes(),p.stat().st_mtime_ns) for p in (
        run_root/'result.json',run_root/'budget.json',*reader.namespace_root.glob('*.json'))}
    assert invoke('first_read',path)==first and len(sequence)==6
    assert all(p.read_bytes()==content and p.stat().st_mtime_ns==mtime for p,(content,mtime) in completed_snapshot.items())
    originals={p:p.read_bytes() for p in reader.namespace_root.glob('*.json')}
    identity=workpaper_processing_identity(real=True,review=True,review_profile='review',defer_review=True)
    service=CompanyProfileTaskService(storage=storage,output_root=tmp_path/'output',checkpoint_root=tmp_path/'checkpoints',
        provider=NoModelCalls(),processing_identity=identity,page_source=source)
    assert asyncio.run(service.execute('pause'))['state']=='paused'
    asyncio.run(service.execute('resume',max_elapsed_seconds=30))
    assert len(sequence)==6  # Finished/rejected work is not re-extracted by resume.
    plans=[reader.plan_review_recovery(code,request_id='review-'+code[:6]) for code in ('600000.SH','600001.SH')]
    plan.update(run_id='review',action='review_batch',instrument_ids=['600000.SH','600001.SH'],
        document_versions={code:digest for code in ('600000.SH','600001.SH')},review_plans=plans)
    review_path=save(tmp_path,plan); reviewed=invoke('review_batch',review_path)
    assert len(sequence)==9
    assert invoke('review_batch',review_path)==reviewed and len(sequence)==9
    current=reader.research_profiles(['600000.SH','600001.SH'])
    by_code={p['instrument_id']:p for p in current['profiles']}
    assert by_code['600000.SH']['readings'][-1]['review_status']=='completed'
    assert by_code['600001.SH']['readings'][-1]['review_status']=='failed'
    assert any(f['fact'] is not None for f in by_code['600001.SH']['facts'])
    assert all(p.read_bytes()==content for p,content in originals.items())
    exported=reader.export(['600000.SH','600001.SH'],export_directory=tmp_path/'export')
    assert {p['instrument_id']:p for p in json.loads((tmp_path/'export/annual_profiles.json').read_text())['profiles']}==by_code
    assert CompanyProfileReadService(tmp_path/'output').research_profiles(['600000.SH','600001.SH'])==current
    if target:=os.environ.get('QUOTE_AUTOMATIC_RECOVERY_EVIDENCE_DIR'):
        evidence=dict(kind='offline controlled transport, not new real model success',first_delivered=2,
            first_missing=['600002.SH'],all_three_core_answers_nonempty=True,physical_sequence=sequence,
            repeated_ids_no_resend=True,pause_resume_no_resend=True,review_failure_keeps_pending=True,
            original_first_writers_unchanged=True,api_export_fresh_equal=True,new_real_model_requests=0)
        (Path(target)/'mixed-batch.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2)+'\n')


def test_automatic_recovery_inline_run_keeps_rejected_extraction_and_reviews(tmp_path):
    from research.company_profile.runtime import CompanyProfileResearchWriter, CompanyProfileStageRuntime
    from tests.unit.test_research.test_company_profile_review_failures import gateway

    selected=_selection(index_sections(PAGES))
    draft=_draft(build_section_packet(_report(),PAGES,index_sections(PAGES),selected)).model_dump(mode='json')
    raw=_malformed(draft)
    provider,identity,transport,client,budget=gateway([_response(selected.model_dump(mode='json')),
        _response(None,choices=[{'message':{'content':raw},'finish_reason':'stop'}]),
        _response(dict(reviewed_fact_ids=['main','revenue'],findings=[]))])
    writer=CompanyProfileResearchWriter(tmp_path/'output')
    runtime=CompanyProfileStageRuntime(writer=writer,provider=provider,execution_budget=budget)
    item=dict(work_id='inline-auto',report=_report(),pages=PAGES,processing_identity=identity)
    try:
        for stage in ('acquire','parse','semantic','verify','publish'):
            assert runtime.run_stage(stage,item)['status']=='success'
        paper=CompanyProfileReadService(tmp_path/'output').query(['600000.SH'])['profiles'][0]['workpaper']
        assert paper['quality_review']['status']=='completed'
        assert paper['generation']['extraction']['contract_status']=='rejected'
        assert [r['purpose'] for r in provider.workpaper_receipts]==[
            'select_profile_sections','extract_profile_workpaper','review_profile_workpaper']
        assert len(transport.calls)==3
    finally:
        asyncio.run(client.close())


def test_new_instance_resume_after_pausing_first_recovered_company(tmp_path):
    from tests.unit.test_research.test_company_profile_review_failures import gateway

    storage=storage_for(tmp_path)
    BusinessProfileAnnouncementFrontierRepository(storage).upsert_record(
        instrument={'instrument_id':'600001.SH','symbol':'600001','exchange':'SSE'},
        record=_announcement('second-report','公司2025年年度报告',published_at='2026-04-01T16:00:00+00:00'))
    selected=_selection(index_sections(PAGES))
    draft=_draft(build_section_packet(_report(),PAGES,index_sections(PAGES),selected)).model_dump(mode='json')
    raw=_malformed(draft)
    provider,identity,transport,client,budget=gateway([
        _response(selected.model_dump(mode='json')),_response(None,choices=[{'message':{'content':raw},'finish_reason':'stop'}]),
        _response(selected.model_dump(mode='json')),_response(draft)])
    identity['workpaper_review_execution']='deferred'
    def source(item):
        return dict(report=_report().model_copy(update={'instrument_id':item['instrument_id']}),pages=PAGES)
    arguments=dict(storage=storage,output_root=tmp_path/'output',checkpoint_root=tmp_path/'checkpoints',
        provider=provider,processing_identity=identity,execution_budget=budget,page_source=source)
    service=CompanyProfileTaskService(**arguments)
    # Local recovery throws before returning the malformed extraction. Pause at
    # the physical scripted boundary instead so the first company can finish.
    send=transport.send
    async def send_and_pause(*args,**kwargs):
        response=await send(*args,**kwargs)
        if len(transport.calls)==2:service.pause(reason='controlled_pause_after_first_extraction')
        return response
    transport.send=send_and_pause
    try:
        first=asyncio.run(service.execute('run',instrument_ids=['600000.SH','600001.SH'],
            knowledge_cutoff='2026-11-21',max_items=2,max_elapsed_seconds=90))
        assert first['state']=='paused' and len(transport.calls)==2
        reader=CompanyProfileReadService(tmp_path/'output')
        partial=reader.research_profiles(['600000.SH','600001.SH'])
        assert partial['delivered']==1 and partial['profiles'][0]['facts']
        first_path=next(reader.namespace_root.glob('*.json'));before=first_path.read_bytes()
        fresh=CompanyProfileTaskService(**arguments)
        resumed=asyncio.run(fresh.execute('resume',max_items=2,max_elapsed_seconds=90))
        assert resumed['state']=='completed' and len(transport.calls)==4
        assert reader.research_profiles(['600000.SH','600001.SH'])['delivered']==2
        original_record=json.loads(before)
        resumed_record=json.loads(first_path.read_bytes())
        changed_fields={key for key in original_record if original_record[key]!=resumed_record[key]}
        assert changed_fields <= {'assessment','execution','provider_calls'}
        assert original_record['workpaper']==resumed_record['workpaper']
        assert original_record['workpaper_receipts']==resumed_record['workpaper_receipts']
        completed_files={p:p.read_bytes() for p in reader.namespace_root.glob('*.json')}
        asyncio.run(fresh.execute('resume',max_items=2,max_elapsed_seconds=30))
        assert len(transport.calls)==4
        assert all(p.read_bytes()==content for p,content in completed_files.items())
        if target:=os.environ.get('QUOTE_AUTOMATIC_RECOVERY_EVIDENCE_DIR'):
            proof=dict(kind='offline controlled real pause after first extraction, new service resumes remaining work',
                first_result=first['state'],first_delivered=1,resumed_state=resumed['state'],final_delivered=2,
                physical_calls_before_resume=2,physical_calls_after_resume=4,after_completed_repeat=4,
                first_record_changed_fields=sorted(changed_fields),workpaper_and_original_receipts_unchanged=True,
                stage_summary_provider_calls_before=original_record['provider_calls'],stage_summary_provider_calls_after=resumed_record['provider_calls'],
                completed_records_byte_identical_on_repeat=True,new_real_model_requests=0)
            (Path(target)/'pause-resume.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2)+'\n')
    finally:
        asyncio.run(client.close())


def test_actual_saved_yuantong_stop_response_automatically_recovers_without_resume(tmp_path):
    from research.company_profile.saved_draft_input import load_saved_draft_input

    old=ROOT/'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-saved-draft-job-20261122/b-development/local-plan.json'
    if not old.is_file():pytest.skip('saved real source binding unavailable')
    parsed=task_entry.CompanyProfileLocalTaskPlan.model_validate_json(old.read_text())
    bound=load_saved_draft_input(parsed,ROOT)
    references=[parsed.saved_draft.selection_receipt,parsed.saved_draft.failed_receipt]
    wires=[json.loads(reference.path.read_text()) for reference in references]
    before={str(reference.path):hashlib.sha256(reference.path.read_bytes()).hexdigest() for reference in references}
    responses=[dict(status_code=200,data=wire['response']) for wire in wires]
    transport=ScriptedTransport(responses)
    budget=_budget(deadline=time.monotonic()+600,max_tokens=1000000,
        billing_mode='development_unpriced',run_authorization='company_profile_workpaper_development.2026-11-11')
    client=_client(transport,budget,_config(model='deepseek-flash',max_retries=1,max_schema_repair_attempts=1))
    provider=CommonGatewaySemanticProvider(client=client,profile='test',max_output_tokens=12000,timeout_seconds=120,
        execution_authorization=dict(status='authorized',document_version=bound.report.document_version,approval_reference='offline saved content replay only'))
    identity=workpaper_processing_identity(real=True)
    try:
        result=run_workpaper(report=bound.report,pages=bound.pages,processing_identity=identity,provider=provider,
            work_id='offline-yuantong-auto',source_pdf_path=bound.source_pdf_path)
        assert len(transport.calls)==2 and identity['workpaper_format_recovery']==AUTO_FORMAT_RECOVERY
        assert 'workpaper_extraction_resume_hash' not in identity
        original_sent=next(json.loads(message['content']) for message in wires[-1]['request']['messages']
            if message['role']=='user' and message['content'].startswith('{'))
        replay_sent=json.loads(transport.calls[1]['payload']['messages'][-1]['content'])
        original_packet={key:original_sent[key] for key in ('reading_scope','source_text')}
        replay_packet={key:replay_sent[key] for key in ('reading_scope','source_text')}
        assert replay_packet==original_packet
        packet_hash=lambda value:hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
        assert packet_hash(replay_packet)==packet_hash(original_packet)
        expected=normalize_retained_workpaper_format(bound.resume['failed_response']['content'])[0]
        originals={f['fact_id']:f for e in [*expected['elements'].values(),expected['company_features']] for f in e['facts']}
        originals.update({f['fact_id']:f for group in (expected.get('supply_chain') or {}).values()
                          if isinstance(group,dict) for f in group.get('facts',[])})
        facts={f.fact_id:f for e in workpaper_elements(result.workpaper).values() for f in e.facts}
        assert len(originals)==30 and len(facts)==28 and set(originals)-set(facts)=={'F15','F19'}
        for fid,fact in facts.items():
            assert fact.text==originals[fid]['text'] and list(fact.evidence_ids)==originals[fid]['evidence_ids']
            assert fact.subject_scope==originals[fid]['subject_scope'] and fact.period==originals[fid]['period']
            assert [m.raw_value for m in fact.metrics]==[m['raw_value'] for m in originals[fid].get('metrics',[])]
        generation=result.workpaper.generation
        assert generation.extraction.contract_status=='rejected'
        assert generation.extraction.model=='deepseek-flash'
        assert generation.extraction.usage.total_tokens==wires[-1]['response']['usage']['total_tokens']
        assert generation.local_format_normalization.normalized_object_sha256=='74107510f2f77f179080b6e6255913092ec075b8faa8db18e8eb4236e451bcf7'
        assert result.receipts[-1]['raw_content']==bound.resume['failed_response']['content']
        assert all(hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest for path,digest in before.items())
        if target:=os.environ.get('QUOTE_AUTOMATIC_RECOVERY_EVIDENCE_DIR'):
            proof=dict(kind='offline saved real response transport replay; no new real model success',
                source_refs_sha256=before,source_pdf_sha256=bound.report.document_version,
                actual_replay_and_old002_source_text_and_reading_scope_equal=True,
                original_sent_source_packet_sha256=packet_hash(original_packet),replay_sent_source_packet_sha256=packet_hash(replay_packet),
                physical_sequence=[r['purpose'] for r in result.receipts],normalization=generation.local_format_normalization.model_dump(mode='json'),
                raw_author_facts=30,retained_typed_source_facts=28,isolated=['F15','F19'],all_retained_content_preserved=True,
                controlled_replay_generation=generation.model_dump(mode='json'),source_artifacts_unchanged=True,new_real_model_requests=0)
            (Path(target)/'saved-real-response-replay.json').write_text(json.dumps(proof,ensure_ascii=False,indent=2)+'\n')
    finally:
        asyncio.run(client.close())
