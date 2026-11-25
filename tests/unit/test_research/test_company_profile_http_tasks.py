"""HTTP research commands use the real entry/owner with an offline wire only."""
import asyncio
import importlib
import json
import sys
import threading
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from api.company_profile_routes import annual_profile_reader, router
from research.company_profile import http_tasks, task_entry
from research.company_profile.http_tasks import (
    CompanyProfileHttpTaskPolicy,
    CompanyProfileHttpTaskRequest,
    CompanyProfileHttpTasks,
    HttpTaskError,
    load_http_task_policy,
)
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper_llm import workpaper_processing_identity
from research.company_profile.workpaper_sections import (
    build_section_packet,
    index_sections,
)
from tests.unit.test_research.test_company_profile_review_execution import storage_for
from tests.unit.test_research.test_company_profile_workpaper import (
    PAGES,
    _draft,
    _report,
    _selection,
)
from tests.unit.test_utils.test_llm_gateway import _config, _response
from utils.llm.client import LlmClient
from utils.llm.testing import ScriptedTransport

AUTH = {"Authorization": "Bearer isolated-research-token"}
URL = "/api/v1/research/company/annual-profile-tasks"


def policy_for(root, *, limit=8):
    profile = {"name": "test", "model": "test-model", "host": "provider.example", "attempt_seconds": 10}
    return CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps({
        "schema_version": "company_profile_http_task_policy.v1", "policy_id": "offline-http-test",
        "allowed_actions": ["first_read", "review_batch", "enrich"], "max_submissions": limit,
        "template": {"schema_version": "company_profile_task_plan.v1", "run_id": "template",
            "authorization_status": "authorized", "approval_reference": "6.FL controlled offline wire only",
            "action": "first_read", "instrument_ids": ["600000.SH"],
            "document_versions": {"600000.SH": _report().document_version}, "knowledge_cutoff": "2026-11-25",
            "output_root": str(root / "output"), "checkpoint_root": str(root / "checkpoints"),
            "extract": profile, "review": {**profile, "name": "review", "reasoning_effort": "high"},
            "budget": {"seconds": 180, "tokens": 1000000, "physical_requests": 10,
                "recoveries": 3, "logical_seconds": 30, "priced_cost_cny": "10",
                "billing_mode": "development_unpriced"}}}))


def request_for(name, action="first_read", **kwargs):
    return {"request_id": name, "action": action, "instrument_ids": ["600000.SH"], **kwargs}


def test_app(service, monkeypatch):
    monkeypatch.setenv("QUOTE_COMPANY_PROFILE_TASK_TOKEN", "isolated-research-token")
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.state.company_profile_http_tasks = service
    app.state.company_profile_http_task_error = "unavailable"
    app.dependency_overrides[annual_profile_reader] = lambda: service.reader
    return app


test_app.__test__ = False


async def terminal(client, task):
    for _ in range(1500):
        response = await client.get(task["status_url"], headers=AUTH)
        assert response.status_code == 200, response.text
        status = response.json()
        if status["state"] not in {"queued", "running"}:
            return status
        await asyncio.sleep(0.01)
    pytest.fail("bounded offline task failed to finish")


def install_offline_client(monkeypatch, scripts):
    config = _config(max_retries=0, max_schema_repair_attempts=0, requests_per_minute=0)
    config = replace(config, profiles={**config.profiles,
        "review": replace(config.profiles["test"], name="review", reasoning_effort="high")})
    transports = []

    def client_factory(configuration, **kwargs):
        transport = ScriptedTransport(scripts.pop(0))
        transports.append(transport)
        return LlmClient(configuration, transport=transport, environment={"TEST_LLM_KEY": "offline-fake"}, **kwargs)

    monkeypatch.setattr(task_entry, "LlmClient", client_factory)
    monkeypatch.setattr(task_entry, "load_project_environment", lambda: None)
    monkeypatch.setattr(task_entry, "workpaper_processing_identity", lambda **kwargs:
        workpaper_processing_identity(**{**kwargs, "real": False}))
    monkeypatch.setattr(task_entry._PlanProvider, "offline_only", True, raising=False)
    monkeypatch.setenv("TEST_LLM_KEY", "offline-fake")
    return config, transports


@pytest.mark.asyncio
async def test_http_first_read_explicit_review_and_enrich_use_original_owner(tmp_path, monkeypatch):
    """Only transport is scripted: task_entry, queue, writer and reads are real."""
    selection = _selection(index_sections(PAGES))
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), selection)
    draft = _draft(packet).model_dump(mode="json")
    enriched = _draft(packet).model_dump(mode="json")
    enriched["elements"]["operating_drivers"] = {"status": "observed", "limitations": [], "facts": [{
        "fact_id": "interest-risk", "text": "经营受利率环境影响。", "subject_scope": "consolidated_group",
        "period": "2025", "evidence_ids": [packet.evidence[-1].evidence_id], "metrics": []}]}
    scripts = [[_response(selection.model_dump(mode="json")), _response(draft)],
        [_response({"reviewed_fact_ids": ["main", "revenue"], "findings": []})],
        [_response(selection.model_dump(mode="json")), _response(enriched),
         _response({"reviewed_fact_ids": ["main", "revenue", "interest-risk"], "findings": []})]]
    config, transports = install_offline_client(monkeypatch, scripts)
    storage = storage_for(tmp_path)
    service = CompanyProfileHttpTasks(policy_for(tmp_path), dependencies=lambda: {
        "storage": storage, "page_source": lambda _: {"report": _report(), "pages": PAGES}, "llm_config": config})
    app = test_app(service, monkeypatch)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            submitted = await client.post(URL, json=request_for("first"), headers=AUTH)
            assert submitted.status_code == 202, submitted.text
            task = submitted.json()
            first = await terminal(client, task)
            assert first["state"] == "completed", first
            assert first["completed_items"] == first["available_profiles"] == 1
            assert first["allowance"]["accepted_submissions"] == 1
            assert first["allowance"]["aggregate_ceiling"]["tokens"] == 8000000
            assert len(first["produced_readings"]) == 1
            assert first["produced_readings"][0]["review_status"] == "not_reviewed"
            pending = (await client.get(task["result_url"], headers=AUTH)).json()["result"]["profiles"][0]
            assert any(f["fact"] for f in pending["facts"])
            assert pending["information_gaps"]
            assert pending["readings"][0]["review_status"] == "not_reviewed"
            read = (await client.get("/api/v1/research/company/600000.SH/annual-profile")).json()
            assert read == pending
            repeats = await client.post(URL, json=request_for("first"), headers=AUTH)
            assert repeats.json()["task_id"] == task["task_id"]
            conflict = await client.post(URL, json=request_for("first", "review_batch"), headers=AUTH)
            assert conflict.status_code == 409
            assert len(transports) == 1 and len(transports[0].calls) == 2

            review = await client.post(URL, json=request_for("review", "review_batch"), headers=AUTH)
            assert review.status_code == 202, review.text
            assert (await terminal(client, review.json()))["state"] == "completed"
            reviewed = (await client.get(review.json()["result_url"], headers=AUTH)).json()
            profile = reviewed["result"]["profiles"][0]
            assert len(profile["readings"]) == 2
            assert profile["readings"][-1]["review_status"] == "completed"
            assert profile["readings"][0]["superseded_by_review_work_id"]

            supplement = await client.post(URL, json=request_for("question", "enrich", targets=[{
                "element": "operating_drivers", "question": "经营主要受什么因素影响？"}]), headers=AUTH)
            assert supplement.status_code == 202, supplement.text
            assert (await terminal(client, supplement.json()))["state"] == "completed"
            result = (await client.get(supplement.json()["result_url"], headers=AUTH)).json()
            current = result["result"]["profiles"][0]
            assert any(f["fact"] and "利率" in f["fact"]["text"] for f in current["facts"])
            assert len(current["readings"]) == 3
            assert current == CompanyProfileReadService(tmp_path / "output").research_profile("600000.SH")
            assert len(transports) == 3 and [len(t.calls) for t in transports] == [2, 1, 3]
            before = sum(len(t.calls) for t in transports)
            for url in [task["status_url"], task["result_url"], "/api/v1/research/company/annual-profiles"]:
                assert (await client.get(url, headers=AUTH)).status_code == 200
            assert (await client.post("/api/v1/research/company/600000.SH/annual-profile/enrichment-plan",
                json={"request_id": "plan-only", "targets": [{"element": "operating_drivers", "question": "风险？"}]})).status_code == 200
            assert before == sum(len(t.calls) for t in transports)
            assert len((await client.get(task["status_url"], headers=AUTH)).json()["produced_readings"]) == 1
            assert "offline-fake" not in json.dumps(result)
            assert str(tmp_path) not in json.dumps(result)
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_disabled_credentials_scope_and_finite_policy(tmp_path, monkeypatch):
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    monkeypatch.delenv("QUOTE_COMPANY_PROFILE_TASK_TOKEN", raising=False)
    monkeypatch.delenv("QUOTE_COMPANY_PROFILE_TASK_POLICY", raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.post(URL, json=request_for("off"))).status_code == 503
        monkeypatch.setenv("QUOTE_COMPANY_PROFILE_TASK_TOKEN", "isolated-research-token")
        assert (await client.post(URL, json=request_for("off"))).status_code == 401
        assert (await client.post(URL, json=request_for("off"), headers=AUTH)).status_code == 503
    policy = policy_for(tmp_path, limit=1)
    service = CompanyProfileHttpTasks(policy, dependencies=lambda: (_ for _ in ()).throw(RuntimeError("SECRET /private")))
    app = test_app(service, monkeypatch)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.post(URL, json=request_for("scope", instrument_ids=["600001.SH"]), headers=AUTH)).status_code == 403
            assert (await client.post(URL, json={**request_for("bad"), "output_root": "/untrusted"}, headers=AUTH)).status_code == 422
            submitted = (await client.post(URL, json=request_for("one"), headers=AUTH)).json()
            failed = await terminal(client, submitted)
            assert failed["state"] == "failed" and failed["completed_items"] == 0
            assert failed["failure_code"] == "task_execution_failed" and "SECRET" not in json.dumps(failed)
            assert (await client.post(URL, json=request_for("two"), headers=AUTH)).status_code == 429
            assert (await client.get(submitted["status_url"], headers={"Authorization": "Bearer bad"})).status_code == 401
            changed = json.loads(policy.model_dump_json())
            changed["max_submissions"] = 8
            with pytest.raises(HttpTaskError, match="drift"):
                CompanyProfileHttpTasks(CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(changed)))
    finally:
        await service.close()


def test_read_root_mismatch_and_ordinary_reader_stays_independent(tmp_path, monkeypatch):
    policy = policy_for(tmp_path)
    path = tmp_path / "policy.json"
    path.write_text(policy.model_dump_json())
    monkeypatch.setenv("QUOTE_COMPANY_PROFILE_TASK_POLICY", str(path))
    monkeypatch.delenv("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT", raising=False)
    with pytest.raises(HttpTaskError, match="policy_invalid"):
        load_http_task_policy()
    policy.output_root.mkdir()
    monkeypatch.setenv("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT", str(policy.output_root))
    assert load_http_task_policy().output_root == policy.output_root
    path.write_text("invalid policy")
    assert annual_profile_reader().output_root == policy.output_root


@pytest.mark.asyncio
async def test_scope_serialization_shutdown_join_and_restart_never_resends(tmp_path, monkeypatch):
    """Boundary-only fake invocation; the successful vertical test uses the owner."""
    entered = [threading.Event(), threading.Event()]
    release = threading.Event()
    finished = threading.Event()
    calls = []

    async def controlled_execution(**kwargs):
        index = len(calls)
        calls.append(kwargs["execution_plan_file"])
        entered[index].set()
        if index == 0:
            while not release.is_set():
                await asyncio.sleep(0.01)
            finished.set()
        else:
            assert finished.is_set()  # Never overlap the two writer scopes.
        return {"state": "completed"}

    monkeypatch.setattr(http_tasks, "execute_configured_task", controlled_execution)
    policy = policy_for(tmp_path)
    first = CompanyProfileHttpTasks(policy, dependencies=dict)
    second = CompanyProfileHttpTasks(policy, dependencies=dict)
    one = await first.submit(CompanyProfileHttpTaskRequest.model_validate(request_for("one")))
    for _ in range(200):
        if entered[0].is_set():
            break
        await asyncio.sleep(0.01)
    assert entered[0].is_set()
    two = await second.submit(CompanyProfileHttpTaskRequest.model_validate(request_for("two")))
    await asyncio.sleep(0.1)
    assert not entered[1].is_set()
    assert second.status(two["task_id"])["state"] == "queued"
    closing = asyncio.create_task(first.close())
    try:
        await asyncio.sleep(0.1)
        assert not closing.done() and not finished.is_set()
        closing.cancel()
        await asyncio.sleep(0.1)
        assert not closing.done() and not entered[1].is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await closing
        for _ in range(200):
            if entered[1].is_set():
                break
            await asyncio.sleep(0.01)
        await second.close()
    assert entered[1].is_set() and len(calls) == 2
    assert first.status(one["task_id"])["state"] == "interrupted"
    # Persisted work whose owner was lost is inspectable, never re-enqueued.
    second._update(two["task_id"], state="running")
    restored = CompanyProfileHttpTasks(policy, dependencies=lambda: pytest.fail("restart must never send"))
    try:
        assert restored.status(two["task_id"])["state"] == "interrupted"
        repeated = await restored.submit(CompanyProfileHttpTaskRequest.model_validate(request_for("two")))
        assert repeated["task_id"] == two["task_id"] and repeated["state"] == "interrupted"
        assert len(calls) <= 2
        changed = json.loads(policy.model_dump_json())
        changed["policy_id"] = "renamed-policy"
        changed["template"]["checkpoint_root"] = str(tmp_path / "different-checkpoint")
        with pytest.raises(HttpTaskError, match="drift"):
            CompanyProfileHttpTasks(CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(changed)))
    finally:
        await restored.close()


def test_terminal_progress_does_not_claim_preexisting_or_later_readings(tmp_path, monkeypatch):
    service = CompanyProfileHttpTasks(policy_for(tmp_path), dependencies=dict)
    task_id = "profile-http-" + "a" * 32
    request = request_for("attribution")
    with service._db() as db:
        db.execute("INSERT INTO tasks(task_id,request_id,fingerprint,request_json,plan_json,state,created_at,updated_at,"
                   "before_json,summary_json) VALUES(?,?,?,?,?,'completed',?,?,?,?)",
                   (task_id, "attribution", "irrelevant", json.dumps(request), "{}", "created", "updated",
                    json.dumps({"600000.SH": ["old"]}),
                    json.dumps({"execution_state": "completed", "work_ids": ["old", "this-task"]})))
    monkeypatch.setattr(service.reader, "research_profiles", lambda _: {"profiles": [{
        "instrument_id": "600000.SH", "readings": [{"work_id": work, "review_status": "not_reviewed"}
        for work in ("old", "this-task", "later-task")]}]})
    status = service.status(task_id)
    assert status["completed_items"] == 1
    assert [r["work_id"] for r in status["produced_readings"]] == ["this-task"]
    assert len(status["saved_readings"]) == 3


@pytest.mark.asyncio
async def test_api_lifespan_joins_research_task_before_storage_close(monkeypatch):
    module = importlib.import_module("api.app")
    calls = []

    async def initialize():
        calls.append("storage_initialized")

    async def close_storage():
        calls.append("storage_closed")

    async def close_tasks(_):
        await asyncio.sleep(0)
        calls.append("tasks_joined")

    monkeypatch.setitem(sys.modules, "data_manager", SimpleNamespace(data_manager=SimpleNamespace(
        initialize=initialize, close=close_storage)))
    monkeypatch.setattr(module, "load_project_environment", lambda: None)
    monkeypatch.setattr(module, "initialize_annual_profile_tasks", lambda _: calls.append("tasks_installed"))
    monkeypatch.setattr(module, "close_annual_profile_tasks", close_tasks)
    async with module.lifespan(FastAPI()):
        calls.append("serving")
    assert calls == ["storage_initialized", "tasks_installed", "serving", "tasks_joined", "storage_closed"]
