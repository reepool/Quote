"""Bounded, offline B2 asyncio shutdown diagnosis; emits JSON lines only."""
import asyncio
import faulthandler
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time

START = time.monotonic()
DENIED = []
WRITE_ROOT = Path(os.environ.get("B2_DIAGNOSTIC_ROOT", Path(__file__).parent)).resolve()
assert WRITE_ROOT.is_relative_to(Path("/home/python/Quote/data/QuoteBak"))
os.environ["QUOTE_DEV_LOG_DIR"] = str(WRITE_ROOT / "logs")


def emit(event, **details):
    print(json.dumps({"event": event, "seconds": round(time.monotonic() - START, 6), **details}), flush=True)


def offline_guard(event, args):
    if event in {"socket.connect", "socket.connect_ex", "socket.getaddrinfo", "socket.gethostbyname", "socket.bind", "socket.sendto", "subprocess.Popen", "os.system"}:
        DENIED.append(event)
        raise PermissionError("B2 offline check forbids network operations")
    if event == "socket.__new__" and args[1] in {socket.AF_INET, socket.AF_INET6}:
        DENIED.append(event)
        raise PermissionError("B2 offline check forbids IP sockets")
    if event == "open" and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND) and not Path(args[0]).resolve().is_relative_to(WRITE_ROOT):
        DENIED.append("file_write")
        raise PermissionError("B2 allows diagnostic writes only inside its own QuoteBak root")


async def router_check():
    import httpx
    from fastapi import FastAPI
    from api.company_profile_routes import annual_profile_reader, router
    from research.company_profile.reads import CompanyProfileReadService

    root = Path("/home/python/Quote/data/research/company_profile_research_v1_20261123")
    instrument = "000099.SZ"
    snapshot = json.loads((root / "current-api-20261124.json").read_text())
    expected = next(p for p in snapshot["profiles"] if p["instrument_id"] == instrument)
    reader = CompanyProfileReadService(root)
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")

    async def dependency():
        return reader

    app.dependency_overrides[annual_profile_reader] = dependency
    emit("original_router_ready", instrument=instrument)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://in-process.invalid", trust_env=False) as client:
        response = await client.get(f"/api/v1/research/company/{instrument}/annual-profile")
        assert response.status_code == 200, response.status_code
        body = response.json()
        assert body == expected
        digest = hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        emit("single_profile_equal", status=response.status_code, canonical_sha256=digest)
    emit("asgi_client_closed", pending_other_tasks=[task.get_name() for task in asyncio.all_tasks() if task is not asyncio.current_task() and not task.done()])


async def main():
    emit("main_enter", loop=type(asyncio.get_running_loop()).__name__)

    def work():
        emit("worker_return", thread=threading.current_thread().name)
        return 42

    if "--router" in sys.argv:
        await router_check()
    else:
        result = await asyncio.to_thread(work)
        emit("to_thread_awaited", result=result)
    await asyncio.get_running_loop().shutdown_default_executor()
    emit("executor_shutdown_complete", threads=[t.name for t in threading.enumerate()])


if __name__ == "__main__":
    if "--guard" in sys.argv:
        sys.addaudithook(offline_guard)
    emit("start", python=sys.version.split()[0], guard="--guard" in sys.argv)
    faulthandler.dump_traceback_later(10 if "--router" in sys.argv else 3)
    asyncio.run(main())
    faulthandler.cancel_dump_traceback_later()
    emit("asyncio_run_returned", threads=[t.name for t in threading.enumerate()], forbidden_attempts=DENIED)
