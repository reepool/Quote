"""Job-plan wiring with controlled transport and the original application owners."""
import asyncio
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from research.company_profile import task_entry
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper import ELEMENT_LABELS
from tests.unit.test_research.test_business_profile_pdf_artifacts import _pdf_bytes
from tests.unit.test_research.test_company_profile_operations import _OfficialAssetAccess
from tests.unit.test_research.test_company_profile_review_execution import storage_for
from tests.unit.test_utils.test_llm_gateway import _config, _response
from tests.unit.test_utils.test_llm_physical_budget import _client
from utils.llm.testing import ScriptedTransport


def plan_for(root):
    return dict(schema_version='company_profile_task_plan.v1', run_id='first', authorization_status='authorized',
        approval_reference='offline-controlled-only', action='first_read', instrument_ids=['600000.SH'],
        document_versions={'600000.SH': 'fixture-document'}, knowledge_cutoff='2026-11-21',
        output_root=str(root/'output'), checkpoint_root=str(root/'checkpoints'),
        extract=dict(name='test', model='test-model', host='provider.example', attempt_seconds=20),
        review=dict(name='review', model='test-model', host='provider.example', attempt_seconds=20, reasoning_effort='high'),
        budget=dict(seconds=180, tokens=500000, physical_requests=10, recoveries=3,
            logical_seconds=30, priced_cost_cny='10', billing_mode='development_unpriced'))


def save(root, plan):
    path=root/(plan['run_id']+'.json')
    path.write_text(json.dumps(plan))
    return path


def config_for_test(monkeypatch):
    config=_config()
    config=replace(config,profiles={**config.profiles,'review':replace(config.profiles['test'],name='review',reasoning_effort='high')})
    monkeypatch.setenv('TEST_LLM_KEY','offline-unit-key')
    monkeypatch.setattr(task_entry,'load_project_environment',lambda:False)
    return config


def test_explicit_job_uses_its_own_runtime_without_scheduler_notifications(monkeypatch):
    import main
    import scheduler.tasks as jobs

    system=main.QuoteSystem.__new__(main.QuoteSystem)
    system.config=Mock(side_effect=AssertionError('explicit plan must not load scheduler defaults'))
    direct=AsyncMock(return_value=True)
    monkeypatch.setattr(jobs.scheduled_tasks,'company_profile_common_core',direct)
    scheduler=AsyncMock(side_effect=AssertionError('explicit plan must not inherit scheduler timeout/reports'))
    monkeypatch.setattr(main.task_scheduler,'execute_job_direct',scheduler)
    monkeypatch.setattr(main.data_manager,'close',AsyncMock())
    parameters=dict(action='first_read',execution_plan_file='planned.json')
    assert asyncio.run(system.run_job('company_profile_common_core',parameters=parameters))
    direct.assert_awaited_once_with(**parameters)
    scheduler.assert_not_awaited()


def test_company_profile_initialization_only_uses_existing_research_owner(monkeypatch):
    import main

    manager=SimpleNamespace(research_storage=object(),_initialize_research_storage=Mock(),
        initialize=AsyncMock(side_effect=AssertionError('must not start unrelated data sources')))
    monkeypatch.setattr(main,'data_manager',manager)
    system=main.QuoteSystem.__new__(main.QuoteSystem)
    asyncio.run(system.initialize_lightweight(company_profile_only=True))
    manager._initialize_research_storage.assert_called_once_with()
    manager.initialize.assert_not_awaited()


def test_explicit_run_receipts_preserve_failed_schema_body_without_auth_headers(tmp_path):
    raw='{"source_conditions": [{"quote": "公开原文"}]}'
    delegate=SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(status_code=200,
        data={'choices':[{'message':{'content':raw}}],'usage':{'total_tokens':12}},
        provider_request_id='request-1')),close=AsyncMock())
    transport=task_entry._TaskReceiptTransport(delegate,tmp_path/'receipts')
    asyncio.run(transport.send('https://provider.example/v1/chat/completions?private=omitted',
        {'Authorization':'Bearer secret-not-recorded'},{'messages':[{'role':'user','content':'公开材料'}]},1200))
    saved=(tmp_path/'receipts/001.json').read_text()
    receipt=json.loads(saved)
    assert receipt['response']['choices'][0]['message']['content']==raw
    assert receipt['physical_timeout_seconds']==1200 and receipt['response']['usage']['total_tokens']==12
    assert 'secret-not-recorded' not in saved and 'private=omitted' not in saved
    delegate.send.side_effect=TimeoutError('sensitive error text must not be recorded')
    with pytest.raises(TimeoutError):
        asyncio.run(transport.send('https://provider.example',{}, {'messages':[]},600))
    failed=json.loads((tmp_path/'receipts/002.json').read_text())
    assert failed['error_type']=='TimeoutError' and 'response' not in failed
    assert 'sensitive error text' not in json.dumps(failed)
    asyncio.run(transport.close())
    delegate.close.assert_awaited_once()


@pytest.mark.parametrize('case',['missing','two_stocks','other_stock','other_version','empty_targets','first_read','review_batch'])
def test_enrichment_plan_scope_is_checked_before_model_construction(tmp_path,monkeypatch,case):
    from tests.unit.test_research.test_company_profile_workpaper import _report

    plan=plan_for(tmp_path)
    report=_report().model_dump(mode='json');report['document_version']='fixture-document'
    plan.update(action='enrich',enrichment_plan=dict(schema_version='company_profile_enrichment.v1',
        request_id='specific-question',base_work_id='original-reading',base_workpaper_sha256='a'*64,
        report=report,targets=[dict(element='products_services',question='What services are provided?')]))
    if case=='missing':plan.pop('enrichment_plan')
    elif case=='two_stocks':
        plan['instrument_ids'].append('600001.SH');plan['document_versions']['600001.SH']='fixture-document'
    elif case=='other_stock':report['instrument_id']='600001.SH'
    elif case=='other_version':report['document_version']='other-document'
    elif case=='empty_targets':plan['enrichment_plan']['targets']=[]
    elif case=='first_read':plan['action']='first_read'
    elif case=='review_batch':plan.update(action='review_batch',review_plans=[dict(instrument_id='600000.SH',report=report)])
    client=Mock(side_effect=AssertionError('invalid scope must not construct a client'))
    monkeypatch.setattr(task_entry,'LlmClient',client)
    with pytest.raises(ValueError):
        asyncio.run(task_entry.execute_configured_task(action=plan['action'],storage=None,
            execution_plan_file=save(tmp_path,plan)))
    client.assert_not_called()
    assert not (tmp_path/'checkpoints').exists()


@pytest.mark.parametrize('continuation',['review_batch','enrich'])
def test_explicit_plan_uses_official_source_gateway_first_read_and_saved_review_on_same_owner(tmp_path,monkeypatch,continuation):
    content=_pdf_bytes(['1. Business', 'Company provides loans and settlement services with source-backed business information.'])
    pdf=tmp_path/'annual.pdf';pdf.write_bytes(content)
    plan=plan_for(tmp_path);plan['document_versions']['600000.SH']=hashlib.sha256(content).hexdigest()
    file=save(tmp_path,plan)
    config=config_for_test(monkeypatch)
    clients=[]

    class Transport(ScriptedTransport):
        async def send(self,url,headers,payload,timeout_seconds):
            source=json.loads(payload['messages'][-1]['content'])
            if len(self.calls)==0 or (continuation=='enrich' and len(self.calls)==2):
                assert source['sections']
                if len(self.calls)==2:
                    assert source['targeted_reading']['targets'][0]['element']=='products_services'
                selected=max(source['sections'],key=lambda row:row[3])[0]
                reply=dict(selected_section_ids=[selected],rationale='Read the source business section.',deferred=[],stop_reason='Bounded selection.')
            elif len(self.calls)==1 or (continuation=='enrich' and len(self.calls)==3):
                supplemental=len(self.calls)==3
                elements={name:dict(status='unclear',facts=[],limitations=['Not obtained from this selected source.']) for name in ELEMENT_LABELS}
                elements['products_services' if supplemental else 'business_structure']=dict(status='observed',facts=[dict(
                    fact_id='supplement' if supplemental else 'main',
                    text='Loans and settlement services.' if supplemental else 'Company provides loans and settlement services.',subject_scope='consolidated_group',
                    period='2025',evidence_ids=['source-0001'],metrics=[])],limitations=[])
                if supplemental:
                    assert source['targeted_reading']['targets'][0]['element']=='products_services'
                reply=dict(schema_version='company_profile_workpaper_draft.v1',elements=elements)
            else:
                assert len(self.calls)==(4 if continuation=='enrich' else 2)
                reply=dict(reviewed_fact_ids=['supplement' if continuation=='enrich' else 'main'],findings=[])
            self.responses.append(_response(reply))
            return await super().send(url,headers,payload,timeout_seconds)

    transport=Transport([])

    def client_factory(config,*,physical_budget):
        client=_client(transport,physical_budget,config)
        clients.append(client)
        return client

    monkeypatch.setattr(task_entry,'LlmClient',client_factory)
    storage=storage_for(tmp_path)
    access=_OfficialAssetAccess(pdf_path=pdf,digest=plan['document_versions']['600000.SH'],announcement_id='recovery-report')
    import main
    import scheduler.tasks as jobs
    task=jobs.ScheduledTasks.__new__(jobs.ScheduledTasks)
    task._active_tasks=set();task._send_task_report=AsyncMock()
    monkeypatch.setattr(jobs,'scheduled_tasks',task)
    manager=SimpleNamespace(research_storage=storage,_get_announcement_asset_access=lambda **_:access)
    monkeypatch.setattr(jobs,'data_manager',manager)
    monkeypatch.setattr(task_entry,'UnifiedConfigManager',lambda _:SimpleNamespace(get_llm_config=lambda:config))
    system=main.QuoteSystem.__new__(main.QuoteSystem)
    system.initialize_lightweight=AsyncMock()
    monkeypatch.setattr(main,'QuoteSystem',lambda:system)

    async def dispatch(job_id,parameters):
        assert job_id=='company_profile_common_core'
        return await task.company_profile_common_core(**parameters)

    monkeypatch.setattr(main.task_scheduler,'execute_job_direct',dispatch)

    def invoke(action,path):
        monkeypatch.setattr('sys.argv',['quote','job','--job-id','company_profile_common_core',
            '--action',action,'--execution-plan-file',str(path)])
        asyncio.run(main.main())
        return task._send_task_report.await_args.kwargs['report_data']['company_profile_common_core']

    first=invoke('first_read',file)
    assert first['state']=='completed' and len(transport.calls)==2 and access.exact_calls
    reader=CompanyProfileReadService(tmp_path/'output')
    before=reader.research_profile('600000.SH')
    base_path=tmp_path/'output/company_profile_common_core.v1'/f"{before['readings'][0]['work_id']}.json"
    base_bytes=base_path.read_bytes()
    assert before['facts'][0]['fact']['text']=='Company provides loans and settlement services.'
    assert before['readings'][0]['review_execution']=='deferred' and before['readings'][0]['review_status']=='not_reviewed'
    assert asyncio.run(task_entry.execute_configured_task(action='first_read',storage=storage,
        execution_plan_file=file,llm_config=None))==first  # No configuration/client reloaded on replay.
    assert len(clients)==1 and len(transport.calls)==2
    review=deepcopy(plan);review.update(run_id='review',action=continuation)
    if continuation=='enrich':
        from api.company_profile_routes import EnrichmentPlanRequest, plan_annual_company_profile_enrichment

        requested=EnrichmentPlanRequest(request_id='job-enrich',targets=[dict(
            element='products_services',question='What products and services does the company provide?')])
        response=asyncio.run(plan_annual_company_profile_enrichment('600000.SH',requested,reader))
        review['enrichment_plan']=response['enrichment_plan']
    else:
        review['review_plans']=[reader.plan_review_recovery('600000.SH',request_id='job-review')]
    review_file=save(tmp_path,review)
    final=invoke(continuation,review_file)
    assert final['state']=='completed' and len(transport.calls)==(5 if continuation=='enrich' else 3) and len(clients)==2
    current=CompanyProfileReadService(tmp_path/'output').research_profile('600000.SH')
    assert current['readings'][-1]['review_status']=='completed'
    assert len([f for f in current['facts'] if f['fact']])==(2 if continuation=='enrich' else 1)
    assert base_path.read_bytes()==base_bytes
    if continuation=='enrich':
        assert current['facts'][0]['fact']['text']==before['facts'][0]['fact']['text']
        assert any(f['element']=='products_services' and f['fact']['text']=='Loans and settlement services.'
            for f in current['facts'] if f['fact'])
        assert current['information_gaps']  # Acquiring one element does not claim all gaps closed.
        exported=reader.export(('600000.SH',),export_directory=tmp_path/'export')
        annual=next(Path(p) for p in exported['files'] if Path(p).name=='annual_profiles.json')
        assert json.loads(annual.read_text())['profiles'][0]==current
        stale=deepcopy(review);stale['run_id']='stale';stale['enrichment_plan']['base_workpaper_sha256']='0'*64
        stale['enrichment_plan']['request_id']='stale-base'
        with pytest.raises(ValueError,match='hash changed'):
            asyncio.run(task_entry.execute_configured_task(action='enrich',storage=storage,
                execution_plan_file=save(tmp_path,stale),llm_config=config,shared_asset_access=access))
        assert len(transport.calls)==5 and base_path.read_bytes()==base_bytes
        assert CompanyProfileReadService(tmp_path/'output').research_profile('600000.SH')==current
    else:
        assert current['facts'][0]['state']=='superseded'
    count=len(clients)
    assert asyncio.run(task_entry.execute_configured_task(action=continuation,storage=storage,
        execution_plan_file=review_file))==final
    assert len(clients)==count
    assert (tmp_path/'checkpoints/executions/first/budget.json').is_file()
    assert (tmp_path/'checkpoints/executions/review/budget.json').is_file()


@pytest.mark.parametrize('case',['unknown_field','infinite','zero_tokens','no_stock','version_scope','bad_review','missing_approval','profile_drift','credential','action'])
def test_invalid_plan_stops_before_client_or_execution_record(tmp_path,monkeypatch,case):
    plan=plan_for(tmp_path);config=config_for_test(monkeypatch)
    if case=='unknown_field':plan['api_key']='not-allowed-in-plan'
    elif case=='infinite':plan['budget']['seconds']=float('inf')
    elif case=='zero_tokens':plan['budget']['tokens']=0
    elif case=='no_stock':plan['instrument_ids']=[]
    elif case=='version_scope':plan['document_versions']={}
    elif case=='bad_review':plan.update(action='review_batch',review_plans=[{'instrument_id':'600000.SH','report':None}])
    elif case=='missing_approval':plan['approval_reference']=''
    elif case=='profile_drift':plan['extract']['model']='unapproved-model'
    elif case=='credential':monkeypatch.delenv('TEST_LLM_KEY')
    action='review_batch' if case=='action' else plan['action']
    client=Mock(side_effect=AssertionError('client must not be constructed'))
    monkeypatch.setattr(task_entry,'LlmClient',client)
    with pytest.raises(ValueError) as error:
        asyncio.run(task_entry.execute_configured_task(action=action,storage=None,
            execution_plan_file=save(tmp_path,plan),llm_config=config))
    assert 'not-allowed-in-plan' not in str(error.value)
    client.assert_not_called()
    assert not (tmp_path/'checkpoints/executions').exists()


@pytest.mark.parametrize('action',['status','query','pause','export'])
def test_control_actions_use_plan_roots_without_model_or_authorization(tmp_path,monkeypatch,action):
    plan=plan_for(tmp_path);plan.update(authorization_status='planned',approval_reference='')
    execute=AsyncMock(return_value={'action':action,'state':'idle'})
    monkeypatch.setattr(task_entry,'execute_published_task',execute)
    monkeypatch.setattr(task_entry,'LlmClient',Mock(side_effect=AssertionError('no model')))
    result=asyncio.run(task_entry.execute_configured_task(action=action,storage='storage',execution_plan_file=save(tmp_path,plan)))
    assert result['action']==action
    assert execute.await_args.kwargs['output_root']==tmp_path/'output'
    assert 'provider' not in execute.await_args.kwargs and not (tmp_path/'checkpoints').exists()


def test_started_plan_cannot_get_new_budget_or_change_parameters(tmp_path,monkeypatch):
    plan=plan_for(tmp_path);file=save(tmp_path,plan)
    parsed=task_entry.CompanyProfileTaskPlan.model_validate_json(file.read_text())
    # A marker produced before enrich was added must still match an unchanged v1 file.
    digest=hashlib.sha256(json.dumps(parsed.model_dump(mode='json',exclude={'enrichment_plan'}),sort_keys=True).encode()).hexdigest()
    root=tmp_path/'checkpoints/executions/first';root.mkdir(parents=True)
    (root/'started.json').write_text(json.dumps({'plan_sha256':digest}))
    monkeypatch.setattr(task_entry,'LlmClient',Mock(side_effect=AssertionError('no fresh client')))
    with pytest.raises(ValueError,match='already started'):
        asyncio.run(task_entry.execute_configured_task(action='first_read',storage=None,execution_plan_file=file))
    plan['budget']['tokens']+=1
    with pytest.raises(ValueError,match='different task parameters'):
        asyncio.run(task_entry.execute_configured_task(action='first_read',storage=None,execution_plan_file=save(tmp_path,plan)))


def test_publication_stop_and_report_scope_are_not_bypassed(tmp_path,monkeypatch):
    plan=plan_for(tmp_path)
    monkeypatch.setattr(task_entry,'publication_allows_new_writes',lambda _:False)
    with pytest.raises(ValueError,match='publication is stopped'):
        asyncio.run(task_entry.execute_configured_task(action='first_read',storage=None,execution_plan_file=save(tmp_path,plan)))
    assert not (tmp_path/'checkpoints').exists()
    provider=object.__new__(task_entry._PlanProvider)
    provider.document_versions={'600000.SH':'allowed'}
    with pytest.raises(ValueError,match='does not authorize'):
        provider.workpaper_request(report=SimpleNamespace(instrument_id='600001.SH',document_version='allowed'))
    with pytest.raises(ValueError,match='does not authorize'):
        provider.workpaper_request(report=SimpleNamespace(instrument_id='600000.SH',document_version='other'))


def test_job_plan_is_thin_forwarding_and_new_actions_require_a_file(monkeypatch):
    import scheduler.tasks as module
    task=module.ScheduledTasks.__new__(module.ScheduledTasks)
    task._active_tasks=set();task._send_task_report=AsyncMock()
    manager=Mock();manager.research_storage=object();manager._get_announcement_asset_access.return_value='official'
    monkeypatch.setattr(module,'data_manager',manager)
    execute=AsyncMock(return_value={'action':'first_read','state':'completed'})
    monkeypatch.setattr(task_entry,'execute_configured_task',execute)
    assert asyncio.run(task.company_profile_common_core(action='first_read',execution_plan_file='explicit.json'))
    assert execute.await_args.kwargs['execution_plan_file']=='explicit.json'
    assert execute.await_args.kwargs['shared_asset_access']=='official'
    execute.reset_mock()
    assert not asyncio.run(task.company_profile_common_core(action='first_read'))
    execute.assert_not_awaited()
    assert not task._active_tasks
    legacy=AsyncMock(return_value={'action':'preview','state':'idle'})
    monkeypatch.setattr('research.company_profile.operations.execute_published_task',legacy)
    config=object()
    assert asyncio.run(task.company_profile_common_core('preview',None,None,2,50000,300.0,'operator_request',None,config))
    assert task._send_task_report.await_args.kwargs['job_config'] is config


@pytest.mark.parametrize('action',['first_read','enrich'])
@pytest.mark.parametrize('extra',[['--token-budget','1'],['--job-id','another-job'],['missing-file']])
def test_cli_rejects_ambiguous_scope_or_other_job_before_initialization(monkeypatch,extra,action):
    import main
    args=['quote','job','--job-id','company_profile_common_core','--action',action,'--execution-plan-file','plan.json']
    if extra[0]=='--job-id':args[3]=extra[1]
    elif extra[0]=='missing-file':args=args[:-2]
    else:args+=extra
    monkeypatch.setattr('sys.argv',args)
    monkeypatch.setattr(main,'QuoteSystem',Mock(side_effect=AssertionError('must reject before initialization')))
    with pytest.raises(SystemExit):asyncio.run(main.main())


def test_cli_forwards_plan_file_to_existing_job(monkeypatch):
    import main
    system=SimpleNamespace(initialize_lightweight=AsyncMock(),run_job=AsyncMock(),cleanup=AsyncMock())
    monkeypatch.setattr(main,'QuoteSystem',lambda:system)
    monkeypatch.setattr('sys.argv',['quote','job','--job-id','company_profile_common_core','--action','review_batch','--execution-plan-file','saved.json'])
    asyncio.run(main.main())
    assert system.run_job.await_args.kwargs['parameters']=={'action':'review_batch','execution_plan_file':'saved.json'}
