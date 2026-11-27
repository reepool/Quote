"""A's bounded dispatch wrapper; all semantic work stays in the original task owner.

`authorize` requires an actual separately recorded human approval, and creates
distinct authorized plans without replacing the planned originals. `run` checks
the same frozen bytes before every send. It cannot resume an ended group.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import socket
import ssl
import sys
import threading
import time
from urllib.parse import urlsplit

E=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('operator_6fu',E/'review_operator.py')
op=importlib.util.module_from_spec(spec)
spec.loader.exec_module(op)
ROOT=op.ROOT
APPROVAL=E/'a-approval.json'
FREEZE=E/'a-send-freeze.json'


def authorize():
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from utils.config_manager import UnifiedConfigManager
    from utils.llm import load_project_environment
    approval=op.read(APPROVAL)
    assert approval['authorized'] is True and approval['approval_reference']
    assert set(approval['stocks'])==set(op.STOCKS)
    assert not FREEZE.exists() and not (E/'a-group-started.json').exists()
    baseline=op.read(op.BASELINE)
    for name,h in baseline['protected'].items():
        assert op.digest(ROOT/name)==h, 'protected source/configuration drift: '+name
    load_project_environment()
    config=UnifiedConfigManager('config').get_llm_config()
    prepared=op.read(E/'b-ready.json')
    assert prepared['state']=='engineering_ready_pending_A_send_freeze_and_specific_human_approval'
    frozen=dict(prepared['frozen_files'])
    for name,h in frozen.items():
        assert op.digest(name)==h, 'B ready input drift: '+name
    cases=[]
    for row in prepared['cases']:
        old=CompanyProfileTaskPlan.model_validate_json(Path(row['plan']).read_text())
        assert old.authorization_status=='planned' and old.instrument_ids==[row['instrument_id']]
        assert old.budget.seconds==6000 and old.budget.tokens==1000000 and old.budget.physical_requests==6
        assert old.budget.recoveries==3 and old.budget.logical_seconds==2450
        assert old.review.attempt_seconds==1200 and old.extract.attempt_seconds==600
        for wanted in (old.extract,old.review):
            actual=config.profiles[wanted.name]
            assert actual.enabled and actual.model==wanted.model and urlsplit(actual.base_url).hostname==wanted.host
            assert actual.reasoning_effort==wanted.reasoning_effort
            assert __import__('os').environ.get(actual.api_key_env), 'configured credential absent'
        body=old.model_dump(mode='json')
        body.update(authorization_status='authorized',approval_reference=approval['approval_reference'])
        path=Path(row['plan']).with_name('execution-plan-authorized.json')
        assert not path.exists()
        new=CompanyProfileTaskPlan.model_validate(body)
        op.save(path,new.model_dump(mode='json'))
        frozen[str(path)]=op.digest(path)
        cases.append({'instrument_id':row['instrument_id'],'plan':str(path),'plan_sha256':op.digest(path)})
    for path in (APPROVAL,E/'a-source-reference.json',E/'b-plan.json',E/'b-ready.json',Path(__file__).resolve()):
        frozen[str(path)]=op.digest(path)
    op.save(FREEZE,{'task':'6.FU','authorized':True,'cases':cases,'frozen_files':frozen,
                    'approval_reference':approval['approval_reference'],'group':prepared['group'],
                    'protected':baseline['protected'],'A_reference_is_not_model_input':True})
    print('Exact authorized plans frozen; no client constructed and no model sent.')


def run(scratch_root):
    from research.company_profile import task_entry
    freeze=op.read(FREEZE)
    assert freeze['authorized'] is True
    assert not (E/'a-group-started.json').exists(), 'one authorized group already used'
    scratch_root=Path(scratch_root).resolve()
    assert scratch_root.parent==ROOT/'data/QuoteBak' and scratch_root.name.startswith('company-profile-6fu-run-')
    assert scratch_root.is_dir() and not any(scratch_root.iterdir())
    def guard():
        for name,h in {**freeze['frozen_files'],**{str(ROOT/p):v for p,v in freeze['protected'].items()}}.items():
            assert op.digest(name)==h, 'frozen input drift: '+name
    guard()
    preflight={}
    for host in ('api.deepseek.com','scorpio.reepool.com'):
        socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)
        with socket.create_connection((host,443),timeout=30) as raw:
            with ssl.create_default_context().wrap_socket(raw,server_hostname=host):
                pass
        preflight[host]='DNS_TLS_OK_same_authorized_network_environment'
    op.save(E/'a-network-preflight.json',{'task':'6.FU','hosts':preflight,'supplier_requests':0})
    deadline=time.monotonic()+12600
    stopped=threading.Event()
    lock=threading.Lock()
    sends=0
    original=task_entry.LlmClient
    class GuardedTransport:
        def __init__(self,delegate):
            self.delegate=delegate
        async def send(self,url,headers,payload,timeout_seconds):
            nonlocal sends
            with lock:
                if stopped.is_set() or time.monotonic()>=deadline or sends>=18:
                    raise RuntimeError('6FU_group_stopped_before_new_send')
                try:
                    guard()
                except (AssertionError,ValueError):
                    stopped.set()
                    raise
                sends+=1
            response=await self.delegate.send(url,headers,payload,min(timeout_seconds,max(0.01,deadline-time.monotonic())))
            if response.status_code in (401,403):
                stopped.set()
            return response
        async def close(self):
            await self.delegate.close()
    def factory(*args,**kwargs):
        client=original(*args,**kwargs)
        client.transport=GuardedTransport(client.transport)
        return client
    started=time.monotonic()
    with (E/'a-group-started.json').open('x') as f:
        json.dump({'task':'6.FU','freeze_sha256':op.digest(FREEZE),'started_epoch':time.time(),
                   'group_deadline_epoch':time.time()+12600,'scratch_root':str(scratch_root)},f)
    task_entry.LlmClient=factory
    def company(row):
        stock=row['instrument_id']
        if stopped.is_set():
            return {'instrument_id':stock,'state':'not_started_group_stopped'}
        try:
            result=asyncio.run(op.run_one(row['plan'],scratch_root=scratch_root))
            return {'instrument_id':stock,'result':result}
        except Exception as exc:
            return {'instrument_id':stock,'state':'failed','error_type':type(exc).__name__}
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results=list(pool.map(company,freeze['cases']))
        guard()
        op.save(E/'a-run-result.json',{'task':'6.FU','cases':results,'elapsed_seconds':time.monotonic()-started,
            'transport_send_attempts':sends,'group_stop_set':stopped.is_set(),'cost':'unknown',
            'publication':'isolated_pending_A_source_review','frozen_inputs_unchanged':True,
            'new_source_selection_or_extraction':False})
        print('Authorized group finished; isolated results await independent A source review.')
    finally:
        task_entry.LlmClient=original


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['authorize','run'])
    parser.add_argument('--scratch-root',type=Path)
    args=parser.parse_args()
    if args.action=='authorize':
        authorize()
    else:
        assert args.scratch_root is not None
        run(args.scratch_root)
