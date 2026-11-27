"""One-shot authorized parameter dispatch, through the existing task owner only."""
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
from pathlib import Path
import socket
import ssl
import time

E=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('operator_6fv',E/'review_operator.py')
op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)
ROOT=op.ROOT
read=op.helpers.read
save=op.helpers.save
sha=op.digest


def authorize():
    from research.company_profile.task_entry import CompanyProfileTaskPlan
    from utils.config_manager import UnifiedConfigManager
    from utils.llm import load_project_environment
    from urllib.parse import urlsplit
    approval=read(E/'a-approval.json')
    assert approval['authorized'] is True and approval['stocks']==[op.STOCK]
    assert approval['approval_reference'] and not (E/'a-send-freeze.json').exists()
    ready=read(E/'b-ready.json');baseline=read(E/'a-baseline.json')
    assert ready['state']=='engineering_ready_pending_A_send_freeze_and_specific_human_approval'
    frozen=dict(ready['frozen_files'])
    frozen.update({str(ROOT/p):h for p,h in baseline['protected'].items()})
    for name,h in frozen.items():
        assert sha(name)==h, 'input drift: '+name
    plan=CompanyProfileTaskPlan.model_validate_json(Path(ready['plan']).read_text())
    assert plan.authorization_status=='planned' and plan.instrument_ids==(op.STOCK,)
    assert plan.budget.seconds==6000 and plan.budget.tokens==1000000 and plan.budget.physical_requests==6
    assert plan.budget.recoveries==3 and plan.budget.logical_seconds==2450
    assert plan.review.attempt_seconds==1200 and plan.extract.attempt_seconds==600
    load_project_environment();config=UnifiedConfigManager('config').get_llm_config()
    for wanted in (plan.extract,plan.review):
        actual=config.profiles[wanted.name]
        assert actual.enabled and actual.model==wanted.model and urlsplit(actual.base_url).hostname==wanted.host
        assert actual.reasoning_effort==wanted.reasoning_effort
        assert __import__('os').environ.get(actual.api_key_env),'configured credential absent'
    body=plan.model_dump(mode='json')
    body.update(authorization_status='authorized',approval_reference=approval['approval_reference'])
    authorized=CompanyProfileTaskPlan.model_validate_json(json.dumps(body))
    path=Path(ready['plan']).with_name('execution-plan-authorized.json');assert not path.exists()
    save(path,authorized.model_dump(mode='json'))
    for pattern in ('research/company_profile/*.py','utils/llm/*.py'):
        frozen.update({str(p):sha(p) for p in ROOT.glob(pattern)})
    for p in (path,E/'a-approval.json',E/'a-source-reference.json',E/'b-ready.json',Path(__file__).resolve()):
        frozen[str(p)]=sha(p)
    save(E/'a-send-freeze.json',{'task':'6.FV','authorized':True,'plan':str(path),
        'frozen_files':frozen,'approval_reference':approval['approval_reference'],
        'readiness_sha256':sha(E/'b-ready.json'),'one_stock':op.STOCK})
    print('One authorized plan frozen; zero clients or sends.')


def run(scratch_root):
    from research.company_profile import task_entry
    freeze=read(E/'a-send-freeze.json');assert freeze['authorized'] is True
    assert not (E/'a-started.json').exists(),'one permitted attempt already started'
    scratch_root=Path(scratch_root).resolve()
    assert scratch_root.parent==ROOT/'data/QuoteBak' and scratch_root.name.startswith('company-profile-6fv-run-')
    assert scratch_root.is_dir() and not any(scratch_root.iterdir())
    def guard():
        for name,h in freeze['frozen_files'].items():
            assert sha(name)==h,'frozen source/code/configuration changed: '+name
    guard()
    for host in ('api.deepseek.com','scorpio.reepool.com'):
        socket.getaddrinfo(host,443,type=socket.SOCK_STREAM)
        with socket.create_connection((host,443),timeout=20) as raw:
            with ssl.create_default_context().wrap_socket(raw,server_hostname=host):
                pass
    save(E/'a-network-preflight.json',{'task':'6.FV','DNS_TLS':'both configured hosts passed in actual authorized network environment','supplier_requests':0})
    started=time.monotonic();deadline=started+6000;sends=0
    original=task_entry.LlmClient
    class GuardedTransport:
        def __init__(self,delegate):
            self.delegate=delegate
        async def send(self,url,headers,payload,timeout_seconds):
            nonlocal sends
            if time.monotonic()>=deadline or sends>=6:
                raise RuntimeError('6FV_before_send_limit')
            guard();sends+=1
            return await self.delegate.send(url,headers,payload,min(timeout_seconds,max(.01,deadline-time.monotonic())))
        async def close(self):
            await self.delegate.close()
    def factory(*args,**kwargs):
        client=original(*args,**kwargs);client.transport=GuardedTransport(client.transport);return client
    with (E/'a-started.json').open('x') as stream:
        json.dump({'task':'6.FV','freeze_sha256':sha(E/'a-send-freeze.json'),
                   'started_epoch':time.time(),'deadline_epoch':time.time()+6000,'scratch_root':str(scratch_root)},stream)
    task_entry.LlmClient=factory
    try:
        result=asyncio.run(op.run_one(freeze['plan'],scratch_root=scratch_root))
        guard()
        save(E/'a-run-result.json',{'task':'6.FV','result':result,'elapsed_seconds':time.monotonic()-started,
             'physical_send_attempts':sends,'frozen_inputs_unchanged':True,
             'publication':'isolated_pending_A_source_review','cost':'unknown','automatic_new_window':False})
    finally:
        task_entry.LlmClient=original


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['authorize','run']);parser.add_argument('--scratch-root',type=Path)
    args=parser.parse_args()
    if args.action=='authorize':authorize()
    else:
        assert args.scratch_root is not None
        run(args.scratch_root)
