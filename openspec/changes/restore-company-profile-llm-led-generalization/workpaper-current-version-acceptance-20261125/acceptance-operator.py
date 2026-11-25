"""6.FN thin original-owner batch adapter. No send without exact A freeze."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from copy import deepcopy
from dataclasses import asdict, replace
from decimal import Decimal
import hashlib
import inspect
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import threading
import time
import traceback
from types import SimpleNamespace
from urllib.parse import urlsplit

ROOT = Path('/home/python/Quote')
sys.path.insert(0, str(ROOT))
HERE = Path(__file__).resolve().parent
EVIDENCE = ROOT / 'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-current-version-acceptance-20261125'
CARD = EVIDENCE.parent / 'company-profile-current-version-acceptance-card.md'
METHOD = EVIDENCE / 'reference-and-audit-method.md'
RAW = ROOT / 'data/research/company_profile_v1_acceptance_20261125'
CHECKPOINT = ROOT / 'data/checkpoints/company_profile_v1_acceptance_20261125'
GROUPS = ('manufacturing', 'services', 'financial', 'multiple_businesses')
EXTRACT = 'semantic_extraction__deepseek'
REVIEW = 'semantic_extraction__scorpio_grok'


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def json_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def save(path, value):
    from research.company_profile.task_entry import _save_result
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    _save_result(path, value)


def ordered_samples():
    values = read_json(EVIDENCE / 'b-sample-manifest.json')['recommended']
    assert len(values) == len({row['instrument_id'] for row in values}) == 20
    assert all(sum(row['group'] == group for row in values) == 5 for group in GROUPS)
    return sorted(values, key=lambda row: (row['group_position'], GROUPS.index(row['group'])))


def bound_config():
    from utils.config_manager import UnifiedConfigManager
    config = UnifiedConfigManager('config').get_llm_config()
    assert config.enabled
    profiles = {}
    for name, model, host, attempt in ((EXTRACT, 'deepseek-flash', 'api.deepseek.com', 600),
                                       (REVIEW, 'grok-4.7', 'scorpio.reepool.com', 1200)):
        profile = config.profiles[name]
        assert profile.enabled and profile.model == model and urlsplit(profile.base_url).hostname == host
        if name == REVIEW:
            assert profile.reasoning_effort == 'high'
        profiles[name] = replace(profile, timeout_seconds=2450, attempt_timeout_seconds=attempt,
                                 max_retries=1, max_schema_repair_attempts=1)
    return replace(config, profiles=profiles, routes={}, pools={})


def identities(config):
    from research.company_profile.workpaper_llm import workpaper_processing_identity
    paths = subprocess.check_output(['git', 'ls-files', 'research/company_profile', 'utils/llm',
        'research/announcement_assets', 'research/business_profile*', 'research/storage.py',
        'research/pdf*'], cwd=ROOT, text=True).splitlines()
    code = {path: digest(ROOT / path) for path in sorted(paths) if path.endswith('.py') and (ROOT / path).is_file()}
    identity = workpaper_processing_identity(real=True, review=True, review_profile=REVIEW)
    identity.update(logical_request_timeout_seconds=2450, review_max_output_tokens=16384,
                    repair_profile=EXTRACT)
    public_config = {'profiles': {name: asdict(value) for name, value in config.profiles.items()},
                     'provider_resources': {name: asdict(value) for name, value in config.provider_resources.items()}}
    return {'driver_path': str(Path(__file__).resolve()), 'driver_sha256': digest(__file__),
        'original_phases_sha256': hashlib.sha256(inspect.getsource(original_phases).encode()).hexdigest(),
        'manifest_sha256': digest(EVIDENCE / 'b-sample-manifest.json'),
        'card_sha256': digest(CARD), 'method_sha256': digest(METHOD),
        'source_sha256': code, 'source_bundle_sha256': json_digest(code),
        'profiles': public_config, 'profiles_sha256': json_digest(public_config),
        'processing_identity': identity, 'processing_identity_sha256': json_digest(identity),
        'ordered_stocks': [row['instrument_id'] for row in ordered_samples()],
        'document_versions': {row['instrument_id']: row['document_version'] for row in ordered_samples()}}


def freeze_preparation():
    plan = read_json(EVIDENCE / 'b-plan.json')
    plan['prepared_driver'] = identities(bound_config())
    plan['state'] = 'driver_frozen_waiting_A_send_freeze'
    plan['metadata_verification']['operator_status'] = 'original owner wired; exact shared-budget success/failure checks passed; explicit A send freeze still required'
    plan['new_model_requests'] = 0
    plan['roots']['per_company_layout'] = 'One flat raw20 writer root; per-stock checkpoint subdirectories, own per-stock isolated DBs in B QuoteBak.'
    plan['execution']['dispatch_order'] = 'group_position1..5, within each manufacturing/services/financial/multiple_businesses; a stock needs its own pre-send A reference hash, not other stock outputs.'
    plan['A_freeze_format'] = {
        'schema_version': 'company_profile_6fn_A_freeze.v1', 'authorized': True,
        'approval_reference': 'A explicit release under user-approved 6.FN',
        **{key: plan['prepared_driver'][key] for key in ('driver_sha256','manifest_sha256','card_sha256',
             'method_sha256','source_bundle_sha256','profiles_sha256','processing_identity_sha256',
             'ordered_stocks','document_versions')},
        'output_root': str(RAW), 'checkpoint_root': str(CHECKPOINT),
        'ready_references_file': str(EVIDENCE / 'a-ready-references.json'),
        'reference_item_format': {'600000.SH': {'path': 'absolute A-owned source-reference file', 'sha256': 'exact file hash'}},
        'runtime_stop_file': str(EVIDENCE / 'a-stop.json'),
        'limits': {'company_seconds':6000,'company_tokens':1000000,'company_physical':10,'company_recoveries':3,
                   'per_logical_recovery':1,'company_priced_cny':'10','group_seconds':30600,'group_tokens':20000000,
                   'group_physical':200,'group_priced_cny':'200','concurrency':4}}
    plan['A_freeze_format'].pop('reference_item_format')
    plan['ready_reference_sidecar_format'] = {'schema_version':'company_profile_6fn_ready_references.v1',
        'references': {'300124.SZ': {'path':'A actual source-reference path', 'sha256':'exact hash'}}}
    plan['runtime_stop_sidecar_format'] = {'reason':'A concrete stop reason','stop_sends':False,
        'meaning':'Presence stops new dispatch; true stop_sends additionally blocks later physical sends; normal in-flight work otherwise keeps its original limit.'}
    plan['run_command'] = f'source scripts/quote_dev_env.sh; /home/python/miniconda3/envs/Quote/bin/python {__file__} run --freeze A_FREEZE_FILE'
    save(EVIDENCE / 'b-plan.json', plan)
    print(json.dumps({key: plan['prepared_driver'][key] for key in ('driver_sha256','source_bundle_sha256','profiles_sha256','manifest_sha256')}, indent=2))


def network_preflight():
    from utils.llm import load_project_environment
    load_project_environment()
    config = bound_config()
    observations = []
    for name, profile in config.profiles.items():
        if not os.environ.get(profile.api_key_env):
            raise ValueError('configured profile credential not present: ' + name)
        host = urlsplit(profile.base_url).hostname
        answers = socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)
        with socket.create_connection((host,443),timeout=30) as raw:
            with ssl.create_default_context().wrap_socket(raw,server_hostname=host) as tls:
                observations.append({'profile':name,'host':host,'model':profile.model,
                    'credential_present':True,'dns_answer_count':len(answers), 'tls':tls.version(),
                    'verified_certificate':bool(tls.getpeercert()),'model_requests':0})
    plan = read_json(EVIDENCE / 'b-plan.json')
    plan['network_preflight'] = {'execution_environment':'require_escalated','new_model_requests':0,
        'observations':observations,'wall_time_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())}
    save(EVIDENCE / 'b-plan.json',plan)
    print(json.dumps(plan['network_preflight'],indent=2))


async def original_phases(*, row, storage, provider, client, budget, identity, output_root,
                          checkpoint_root, evidence_root, shared_asset_access=None, page_source=None):
    """Parameter-only phase dispatch; semantic loops remain original TaskService."""
    from research.company_profile.operations import execute_published_task
    from research.company_profile.reads import CompanyProfileReadService
    stock = row['instrument_id']
    base = dict(storage=storage,output_root=output_root,checkpoint_root=checkpoint_root,
        provider=provider,shared_asset_access=shared_asset_access,page_source=page_source,
        processing_identity=identity,execution_budget=budget,token_budget=budget.max_tokens,
        instrument_ids=(stock,),knowledge_cutoff='2026-11-25',max_items=1,reason='6.FN fixed current-version acceptance')
    assert client.physical_budget is budget and provider._client is client
    deadline = budget.deadline
    result = {'instrument_id':stock,'action_states':{},'budget_shared_exact_instance':True}
    first = await execute_published_task(action='first_read',max_elapsed_seconds=budget.remaining_time(),**base)
    result['action_states']['first_read'] = first
    save(evidence_root/'first-result.json',first)
    save(evidence_root/'budget-after-first.json',budget.snapshot())
    reader = CompanyProfileReadService(output_root)
    profile = reader.research_profile(stock)
    save(evidence_root/'first-api.json',profile)
    if not any(item.get('fact') for item in profile.get('facts',[])):
        result.update(state='failed',failure_code='first_read_no_readable_workpaper')
        return result
    plan = reader.plan_review_recovery(stock,request_id='6fn-review-'+stock.replace('.','-'))
    save(evidence_root/'review-plan.json',plan)
    if plan.get('state') != 'planned':
        result.update(state='failed',failure_code='review_plan_'+str(plan.get('state')))
        return result
    assert budget.deadline == deadline and client.physical_budget is budget
    reviewed = await execute_published_task(action='review_batch',review_plans=(plan,),
        max_elapsed_seconds=budget.remaining_time(),**base)
    result['action_states']['review_batch'] = reviewed
    final = CompanyProfileReadService(output_root).research_profile(stock)
    save(evidence_root/'automatic-final-api.json',final)
    result.update(state='completed',review_status=(reviewed.get('items') or [{}])[0].get('review_status'),
        unchanged_budget_deadline=budget.deadline==deadline)
    if result['review_status'] not in {'completed', 'completed_with_open_findings'}:
        result.update(state='failed',failure_code='source_review_'+str(result['review_status']))
    return result


def seed_official_frontier(storage, row, access):
    """Reuse the existing exact official-metadata owner, never previous semantics."""
    from research.business_profile_production_operations import register_business_profile_shared_annual_report_asset
    asset=access.get_effective_asset(row['instrument_id'],fiscal_year=2025,knowledge_cutoff='2026-11-25')
    if (not asset or asset.get('asset_id')!=row['asset_id']
        or asset.get('content_hash')!=row['document_version']
        or asset.get('instrument_id')!=row['instrument_id']):
        raise ValueError('official metadata differs from frozen stock/report')
    status=register_business_profile_shared_annual_report_asset(storage=storage,asset=asset)
    return {'instrument_id':row['instrument_id'],'asset_id':asset['asset_id'],
        'document_version':asset['content_hash'],'frontier_status':status,'metadata_only':True,
        'owner':'register_business_profile_shared_annual_report_asset'}


def structured_execution_root(provider, output_root, stock):
    """Use existing typed review/trace codes; stock/request IDs are not causes."""
    from research.company_profile.reads import CompanyProfileReadService
    try:
        record=CompanyProfileReadService(output_root)._latest_by_instrument(include_enrichments=True).get(stock)
        review=getattr(getattr(record,'workpaper',None),'quality_review',None)
    except (ValueError,FileNotFoundError):
        review=None
    if review is not None and review.status in {'completed','completed_with_open_findings'}:
        return None
    failure=getattr(review,'execution_failure',None)
    if failure is not None:
        # Typed diagnostic already defines kind/code/channel. Keep the purpose
        # for contract failures; network failures can span initial/final calls.
        cause=failure.model_dump(exclude_none=True,exclude={'purpose'})
        if failure.kind=='contract_error':
            cause['purpose']=failure.purpose
        return 'review:'+json.dumps(cause,sort_keys=True,ensure_ascii=False)
    traces=list(getattr(provider,'traces',()))
    if traces and traces[-1].status=='failed':
        trace=traces[-1]
        cause={key:getattr(trace,key,None) for key in ('call_type','error_code','selected_profile','source_label','model')}
        return 'gateway:'+json.dumps(cause,sort_keys=True,ensure_ascii=False)
    return None


class BatchState:
    def __init__(self, freeze, deadline):
        self.freeze,self.deadline=freeze,deadline
        self.lock=threading.RLock()
        self.active={}
        self.stop_reason=None
        self.stop_sends=False
        self.last_failure=None

    def check(self):
        stop_file=Path(self.freeze['runtime_stop_file'])
        if time.monotonic()>=self.deadline:
            self.stop_reason='group_absolute_deadline'
            self.stop_sends=True
        if stop_file.exists():
            instructions=read_json(stop_file)
            self.stop_reason='A_explicit_stop:'+str(instructions.get('reason',''))
            self.stop_sends=self.stop_sends or instructions.get('stop_sends') is True
        return self.stop_reason

    def admit_send(self):
        self.check()
        if self.stop_sends:
            raise RuntimeError('group physical sending stopped')

    def complete(self,result):
        with self.lock:
            # Only concrete gateway/queue errors establish a repeated root.
            # Missing content/review completion alone is not a shared cause.
            cause=result.get('execution_root_key')
            if cause and self.last_failure==cause:
                self.stop_reason='two_consecutive_same_core_execution_failure:'+cause
            self.last_failure=cause

    def cancel_all(self,reason):
        with self.lock:
            self.stop_reason=reason
            self.stop_sends=True
            for budget in self.active.values():
                budget.cancel(reason)


def reference_ready(freeze,row):
    path=Path(freeze['ready_references_file'])
    if not path.is_file():
        return None
    bindings=read_json(path).get('references',{})
    binding=bindings.get(row['instrument_id'])
    if not binding:
        return None
    reference=Path(binding['path']).resolve()
    if not reference.is_relative_to(EVIDENCE.resolve()) or digest(reference)!=binding['sha256']:
        raise ValueError('A source-reference binding invalid: '+row['instrument_id'])
    return {'path':str(reference),'sha256':binding['sha256']}


def run_company(row,reference,state,config,identity):
    from research.company_profile.operations import _resolve_shared_asset_access
    from research.company_profile.task_entry import _PlanProvider,_TaskReceiptTransport
    from research.storage import ResearchStorageManager
    from utils.config_manager import UnifiedConfigManager
    from utils.llm import LlmClient
    from utils.llm.budget import LlmRunBudget,LogicalRecoveryPolicy,DEVELOPMENT_UNPRICED_BILLING
    stock=row['instrument_id']
    checkpoint=CHECKPOINT/stock
    receipts=checkpoint/'execution'
    receipts.mkdir(parents=True,exist_ok=True)
    started=time.monotonic()
    marker=receipts/'started.json'
    with marker.open('x',encoding='utf-8') as stream:
        json.dump({'stock':stock,'document_version':row['document_version'],'reference':reference,
            'A_freeze_sha256':state.freeze['actual_freeze_sha256'],'original_time_budget_seconds':6000},stream)
    budget=LlmRunBudget(deadline=min(started+6000,state.deadline),prices={},max_tokens=1000000,
        max_requests=10,max_cost_cny=Decimal('10'),ledger_path=receipts/'budget.json',
        billing_mode=DEVELOPMENT_UNPRICED_BILLING,recovery_policy=LogicalRecoveryPolicy(3,1),
        run_authorization='company_profile_workpaper_development.2026-11-11')
    with state.lock:
        state.active[stock]=budget
    async def invoke():
        config_manager=UnifiedConfigManager('config')
        main_config=config_manager.get_research_config()
        access=_resolve_shared_asset_access(SimpleNamespace(research_config=main_config,db_path=main_config.storage.db_path))
        if access is None:
            raise ValueError('official shared asset access unavailable')
        cfg=deepcopy(main_config)
        directory=HERE/'run-databases'/stock
        directory.mkdir(parents=True,exist_ok=True)
        for key in ('db_path','financials_db_path','valuation_db_path','interests_db_path'):
            setattr(cfg.storage,key,str(directory/'research.db'))
        cfg.storage.attach_quotes_db=False
        storage=ResearchStorageManager(cfg)
        storage.initialize()
        save(receipts/'official-metadata-seed.json',seed_official_frontier(storage,row,access))
        client=LlmClient(config,physical_budget=budget)
        class GuardedReceipts(_TaskReceiptTransport):
            async def send(self,url,headers,payload,timeout_seconds):
                state.admit_send()
                if digest(reference['path'])!=reference['sha256']:
                    state.cancel_all('A_frozen_reference_changed')
                    raise ValueError('A frozen source reference changed before model send')
                response=await super().send(url,headers,payload,timeout_seconds)
                if response.status_code in (401,403):
                    state.cancel_all('authentication_or_permission_failure')
                return response
        client.transport=GuardedReceipts(client.transport,receipts/'wires')
        provider=_PlanProvider(client=client,profile=EXTRACT,max_output_tokens=12000,timeout_seconds=2450,
            physical_call_admission=state.admit_send,
            execution_authorization={'status':'authorized','approval_reference':state.freeze['approval_reference']})
        provider.document_versions={stock:row['document_version']}
        outcome=None
        try:
            outcome=await original_phases(row=row,storage=storage,provider=provider,client=client,budget=budget,
                identity=identity,output_root=RAW,checkpoint_root=checkpoint,evidence_root=receipts,
                shared_asset_access=access)
            return outcome
        finally:
            root=structured_execution_root(provider,RAW,stock)
            if root:
                (outcome if outcome is not None else result)['execution_root_key']=root
            await client.close()
    result={'instrument_id':stock,'state':'failed','failure_code':'operator_setup_failure'}
    try:
        result=asyncio.run(invoke())
    except Exception as exc:
        frames=traceback.extract_tb(exc.__traceback__)
        location=frames[-1] if frames else None
        result.update(failure_code=type(exc).__name__,error_type=type(exc).__name__,
            failure_location={'file':location.filename,'line':location.lineno,'function':location.name} if location else None)
    finally:
        snapshot=budget.snapshot()
        save(receipts/'budget.json',snapshot)
        result.update(budget=snapshot,elapsed_seconds=time.monotonic()-started)
        save(receipts/'result.json',result)
        with state.lock:
            state.active.pop(stock,None)
        # Actual public response envelopes determine authentication stops.
        for wire in (receipts/'wires').glob('*.json'):
            envelope=read_json(wire)
            if envelope.get('status_code') in (401,403):
                state.cancel_all('authentication_or_permission_failure')
            if result['state']=='failed' and not result.get('execution_root_key') and envelope.get('error_type'):
                result['execution_root_key']='transport:'+envelope['error_type']
        db=HERE/'run-databases'/stock/'research.db'
        if result['state']=='failed' and db.exists():
            import sqlite3
            with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as conn:
                errors=[{'stage':r[0],'last_error':r[1]} for r in conn.execute(
                    'SELECT stage,last_error FROM business_profile_work_items WHERE instrument_id=? AND last_error IS NOT NULL ORDER BY updated_at', (stock,))]
            if errors:
                save(receipts/'execution-errors.json',{'items':errors})
        save(receipts/'result.json',result)
        state.complete(result)
    print(json.dumps({'completed':stock,'state':result['state'],'review_status':result.get('review_status'),
        'requests':snapshot['physical_requests'],'tokens':snapshot['charged_tokens']},ensure_ascii=False),flush=True)
    return result


def validate_A_freeze(path,config):
    freeze=read_json(path)
    expected=read_json(EVIDENCE/'b-plan.json')['A_freeze_format']
    if freeze.get('authorized') is not True or not freeze.get('approval_reference','').strip():
        raise ValueError('explicit A send freeze required')
    for key,value in expected.items():
        if key in ('approval_reference',):
            continue
        if freeze.get(key)!=value:
            raise ValueError('A freeze field differs: '+key)
    current=identities(config)
    for key in ('driver_sha256','manifest_sha256','card_sha256','method_sha256','source_bundle_sha256',
                'profiles_sha256','processing_identity_sha256','ordered_stocks','document_versions'):
        if freeze[key]!=current[key]:
            raise ValueError('actual frozen runtime drift: '+key)
    for row in ordered_samples():
        if digest(row['pdf_path'])!=row['document_version']:
            raise ValueError('frozen source PDF changed: '+row['instrument_id'])
    freeze['actual_freeze_sha256']=digest(path)
    return freeze,current


def run_group(freeze_path):
    from utils.llm import load_project_environment
    load_project_environment()
    config=bound_config()
    freeze,current=validate_A_freeze(freeze_path,config)
    prepared=read_json(EVIDENCE/'b-plan.json')
    if not prepared.get('network_preflight',{}).get('observations'):
        raise ValueError('authorized-network DNS/TLS preflight missing')
    CHECKPOINT.mkdir(parents=True,exist_ok=True)
    with (CHECKPOINT/'group-started.json').open('x',encoding='utf-8') as stream:
        json.dump({'freeze_sha256':freeze['actual_freeze_sha256'],'ordered_stocks':current['ordered_stocks']},stream)
    RAW.mkdir(parents=True,exist_ok=True)
    started=time.monotonic()
    state=BatchState(freeze,started+30600)
    todo=list(ordered_samples())
    pending={}
    results=[]
    with ThreadPoolExecutor(max_workers=4,thread_name_prefix='6fn-owner') as pool:
        while todo or pending:
            while todo and len(pending)<4 and not state.check():
                row=todo[0]
                reference=reference_ready(freeze,row)
                if reference is None:
                    break
                todo.pop(0)
                future=pool.submit(run_company,row,reference,state,config,current['processing_identity'])
                pending[future]=row['instrument_id']
            if pending:
                done,_=wait(pending,timeout=1,return_when=FIRST_COMPLETED)
                for future in done:
                    pending.pop(future)
                    completed=future.result()
                    ledger=completed['budget']
                    results.append({key:completed.get(key) for key in ('instrument_id','state','review_status',
                        'failure_code','execution_root_key','elapsed_seconds')} | {
                        'physical_requests':ledger['physical_requests'],'charged_tokens':ledger['charged_tokens'],
                        'charged_cost_cny':ledger['charged_cost_cny'],
                        'result_path':str(CHECKPOINT/completed['instrument_id']/'execution/result.json')})
            elif todo and not state.check():
                time.sleep(1)
            if state.check() and not pending:
                break
            save(CHECKPOINT/'group-progress.json',{'completed':results,'unstarted':[r['instrument_id'] for r in todo],
                'in_flight':list(pending.values()),'stop_reason':state.stop_reason,'elapsed_seconds':time.monotonic()-started})
    summary={'schema_version':'company_profile_6fn_group_result.v1','results':results,
        'unstarted':[r['instrument_id'] for r in todo],'stop_reason':state.stop_reason,
        'elapsed_seconds':time.monotonic()-started,'quality_status':'A_independent_assessment_required'}
    save(CHECKPOINT/'group-result.json',summary)
    print(json.dumps({'group_state':'stopped' if state.stop_reason else 'completed','started_companies':len(results),
        'unstarted':summary['unstarted'],'stop_reason':state.stop_reason},ensure_ascii=False),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=('prepare','network-preflight','run'))
    parser.add_argument('--freeze',type=Path)
    args=parser.parse_args()
    os.chdir(ROOT)
    if args.command=='prepare':
        freeze_preparation()
    elif args.command=='network-preflight':
        network_preflight()
    elif args.freeze is None:
        parser.error('run requires --freeze')
    else:
        run_group(args.freeze)
