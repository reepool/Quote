"""Only the new batch20 and one new bound enrichment plan; original router."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time

REPO=Path('/home/python/Quote')
TEMP=Path(os.environ.get('B1_LATE_VERIFY_ROOT',Path(__file__).parent)).resolve()
assert TEMP.is_relative_to(REPO/'data/QuoteBak')
ROOT=REPO/'data/research/company_profile_research_v1_20261123'
E=REPO/'openspec/changes/restore-company-profile-llm-led-generalization/workpaper-unified-review-release-20261125'
sys.path.insert(0,str(REPO))
os.environ['QUOTE_DEV_LOG_DIR']=str(TEMP/'logs')
os.environ['QUOTE_COMPANY_PROFILE_RESEARCH_ROOT']=str(ROOT)
DENIED=[]
def guard(event,args):
    if event in {'socket.connect','socket.connect_ex','socket.getaddrinfo','socket.gethostbyname','socket.bind','socket.sendto','subprocess.Popen','os.system'}:
        DENIED.append(event);raise PermissionError('B1 offline ASGI only')
    if event=='socket.__new__' and args[1] in {socket.AF_INET,socket.AF_INET6}:
        DENIED.append(event);raise PermissionError('B1 forbids IP sockets')
    if event=='open' and args[2] & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC|os.O_APPEND):
        if not isinstance(args[0],(str,bytes,os.PathLike)) or not Path(args[0]).resolve().is_relative_to(TEMP):
            DENIED.append('file_write_outside_own_temp');raise PermissionError('B1 writes own temp only')
sys.addaudithook(guard)
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
async def check():
    import httpx
    from fastapi import FastAPI
    from api.company_profile_routes import annual_profile_reader,router
    batch=json.loads((ROOT/'current-api-20261125.json').read_text())
    release=json.loads((E/'b1-late-release.json').read_text())
    assert digest(batch)==release['current_api_batch_sha256']
    app=FastAPI();app.include_router(router,prefix='/api/v1')
    async def dependency():
        reader=annual_profile_reader();assert reader.output_root==ROOT;return reader
    app.dependency_overrides[annual_profile_reader]=dependency
    events=[]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://in-process.invalid',trust_env=False) as client:
        start=time.monotonic()
        response=await client.post('/api/v1/research/company/annual-profiles/query',json={'instrument_ids':batch['requested_instrument_ids']})
        assert response.status_code==200 and response.json()==batch
        events.append(dict(request='new_batch20',status=200,payload_sha256=digest(response.json()),seconds=round(time.monotonic()-start,3)))
        instrument='000526.SZ'
        expected=release['new_base_hash_plans'][instrument]
        plan=expected['enrichment_plan']
        start=time.monotonic()
        response=await client.post(f'/api/v1/research/company/{instrument}/annual-profile/enrichment-plan',json={k:plan[k] for k in ('request_id','base_work_id','targets')})
        assert response.status_code==200 and response.json()==expected
        assert response.json()['execution_started'] is False
        events.append(dict(request='new_representative_plan',instrument_id=instrument,status=200,payload_sha256=digest(response.json()),base_work_id=plan['base_work_id'],base_workpaper_sha256=plan['base_workpaper_sha256'],seconds=round(time.monotonic()-start,3)))
    assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    await asyncio.get_running_loop().shutdown_default_executor()
    return events
started=time.monotonic()
events=asyncio.run(check())
assert [t.name for t in threading.enumerate()]==['MainThread']
assert not any(e not in {'socket.__new__','file_write_outside_own_temp'} for e in DENIED),DENIED
result=dict(task='6.FK',phase='approved_late_successor_consumption',requests=events,client_closed=True,executor_closed=True,event_loop_closed=True,remaining_threads=['MainThread'],
    denied_initialization_attempts=DENIED,model_requests=0,record_writes=0,external_sends=0,
    reused_initial_HTTP='b1-consumption.json: previous 7 HTTP200/clean exit remains valid for unchanged route/ReadService paths',
    elapsed_seconds=round(time.monotonic()-started,3),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
(TEMP/'late-http.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(result,ensure_ascii=False,indent=2))
