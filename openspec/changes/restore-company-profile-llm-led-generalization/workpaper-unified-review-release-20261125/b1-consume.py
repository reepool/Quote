"""Bounded 6.FK consumption verification of the original read service/router."""
from __future__ import annotations
import asyncio
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time

REPO = Path('/home/python/Quote')
TEMP = Path(__file__).resolve().parent
ROOT = REPO / 'data/research/company_profile_research_v1_20261123'
sys.path.insert(0, str(REPO))
os.environ['QUOTE_DEV_LOG_DIR'] = str(TEMP / 'logs')
os.environ['QUOTE_COMPANY_PROFILE_RESEARCH_ROOT'] = str(ROOT)
DENIED = []
OWNERS = []

def guard(event, args):
    if event in {'socket.connect', 'socket.connect_ex', 'socket.getaddrinfo', 'socket.gethostbyname',
                 'socket.bind', 'socket.sendto', 'subprocess.Popen', 'os.system'}:
        DENIED.append(event)
        raise PermissionError('6.FK consumption forbids external networking and processes')
    if event == 'socket.__new__' and args[1] in {socket.AF_INET, socket.AF_INET6}:
        DENIED.append(event)
        raise PermissionError('6.FK consumption forbids IP sockets')
    if event == 'open' and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
        if not isinstance(args[0], (str, bytes, os.PathLike)) or not Path(args[0]).resolve().is_relative_to(TEMP):
            DENIED.append('file_write_outside_own_temp')
            raise PermissionError('6.FK consumption writes only its own QuoteBak root')

def owner_guard(frame, event, arg):
    if event != 'call':
        return
    module = frame.f_globals.get('__name__', '')
    name = frame.f_code.co_name
    instance = frame.f_locals.get('self')
    cls = type(instance).__name__ if instance is not None else ''
    forbidden = (name == '__init__' and (cls.endswith('Provider') or cls in {'LlmClient', 'CompanyProfileTaskService'}))
    forbidden |= ('enqueue' in name and (module.startswith('research.company_profile') or module.startswith('utils.llm')))
    if forbidden and (module.startswith('research.company_profile') or module.startswith('utils.llm')):
        OWNERS.append(module + '.' + cls + '.' + name)
        raise AssertionError('read/plan must not construct provider/client or enqueue')

sys.addaudithook(guard)
sys.setprofile(owner_guard)
threading.setprofile(owner_guard)

def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()

def check_profile(p):
    assert p['coverage_assessment'] == 'selected_scope_only'
    assert p['full_report_completeness'] == 'not_assessed'
    assert p['production_authorization'] == 'not_authorized'
    assert p['information_gaps'] and any(g.get('active', True) for g in p['information_gaps'])
    return dict(profile_sha256=digest(p), latest_work_id=p['latest_reading_work_id'],
                facts=sum(f.get('fact') is not None for f in p['facts']),
                metrics=sum(len(f['fact'].get('metrics', [])) for f in p['facts'] if f.get('fact')),
                active_gaps=sum(g.get('active', True) for g in p['information_gaps']),
                information_gaps_sha256=digest(p['information_gaps']),
                review_status=p['readings'][-1]['review_status'])

snapshot = json.loads((TEMP / 'first-integrated-api.json').read_text())
expected = {p['instrument_id']: p for p in snapshot['profiles']}
targets = {
    '600348.SH': [dict(element='scale_and_mix', question='请区分所选年报中煤炭业务分品种表的产销量合计与公司原煤产量，保留各自主体、期间和表头口径；未读或无法核实的部分明确说明。')],
    '600901.SH': [dict(element='material_changes', question='请按年报原表核实不良融资租赁资产率相较上年末的变化方向与单位，区分比率本期值、上期值和变动；无法安全标准化则保原文和缺项。')],
    '000526.SZ': [dict(element='scale_and_mix', question='请从已披露经营章节核对教育培训服务费收入、成本和业务权重的金额、期间及主体，保留跨行金额的原始依据和仍未获取的细项，不用其他业务替代。')],
}

def request_body(instrument):
    return dict(request_id='6fk-b1-consumption-' + instrument[:6],
                base_work_id=expected[instrument]['latest_reading_work_id'], targets=targets[instrument])

def check_plan(plan, instrument):
    assert plan['state'] == 'planned' and plan['execution_started'] is False
    binding = plan['enrichment_plan']
    p = expected[instrument]
    assert binding['base_work_id'] == p['latest_reading_work_id']
    assert binding['base_workpaper_sha256'] == p['readings'][-1]['workpaper_sha256']
    assert binding['report'] == p['report'] and binding['targets'] == targets[instrument]
    return plan

async def http_check():
    import httpx
    from fastapi import FastAPI
    from api.company_profile_routes import annual_profile_reader, router
    app = FastAPI()
    app.include_router(router, prefix='/api/v1')
    async def dependency():
        reader = annual_profile_reader()
        assert reader.output_root == ROOT
        return reader
    app.dependency_overrides[annual_profile_reader] = dependency
    events = []
    direct = json.loads((TEMP / 'consume-direct.json').read_text())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://in-process.invalid', trust_env=False) as client:
        start = time.monotonic()
        response = await client.post('/api/v1/research/company/annual-profiles/query', json={'instrument_ids':list(expected)})
        assert response.status_code == 200
        assert response.json() == snapshot
        events.append(dict(request='batch20', status=200, seconds=round(time.monotonic()-start, 3), payload_sha256=digest(response.json())))
        for instrument in targets:
            start = time.monotonic()
            response = await client.get(f'/api/v1/research/company/{instrument}/annual-profile')
            assert response.status_code == 200 and response.json() == expected[instrument]
            events.append(dict(request='detail_and_gaps', instrument_id=instrument, status=200,
                               seconds=round(time.monotonic()-start, 3), **check_profile(response.json())))
            start = time.monotonic()
            response = await client.post(f'/api/v1/research/company/{instrument}/annual-profile/enrichment-plan', json=request_body(instrument))
            assert response.status_code == 200
            plan = check_plan(response.json(), instrument)
            assert plan == direct['plans'][instrument]
            events.append(dict(request='enrichment_plan_only', instrument_id=instrument, status=200,
                               seconds=round(time.monotonic()-start, 3), payload_sha256=digest(plan)))
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    await asyncio.get_running_loop().shutdown_default_executor()
    return events

started = time.monotonic()
if '--http' in sys.argv:
    events = asyncio.run(http_check())
    assert len(threading.enumerate()) == 1
    result = dict(mode='original_router_in_process_ASGI', requests=events, client_closed=True,
                  executor_closed=True, event_loop_closed=True, remaining_threads=[t.name for t in threading.enumerate()])
    filename = 'consume-http.json'
else:
    from research.company_profile.reads import CompanyProfileReadService
    reader = CompanyProfileReadService(ROOT)
    batch = reader.research_profiles(list(expected))
    assert batch == snapshot
    details = {}
    plans = {}
    for instrument in targets:
        p = reader.research_profile(instrument)
        assert p == expected[instrument]
        details[instrument] = check_profile(p)
        plans[instrument] = check_plan(reader.plan_enrichment(instrument, **request_body(instrument)), instrument)
    assert CompanyProfileReadService(ROOT).research_profiles(list(expected)) == snapshot
    result = dict(mode='ReadService', batch20_sha256=digest(batch), all_profiles_equal=True,
                  details=details, plans=plans, new_reader_instance_equal=True)
    filename = 'consume-direct.json'
assert not OWNERS
assert not any(event not in {'socket.__new__', 'file_write_outside_own_temp'} for event in DENIED), DENIED
result.update(elapsed_seconds=round(time.monotonic()-started, 3),
    denied_initialization_attempts=DENIED, forbidden_provider_or_enqueue_calls=OWNERS,
    new_model_requests=0, external_network_sends=0, record_writes=0,
    source_script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
(TEMP / filename).write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
print(json.dumps(result, ensure_ascii=False, indent=2))
