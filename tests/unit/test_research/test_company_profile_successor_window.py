"""6.FQ: controlled_fixture only; no supplier, socket server or live roots."""
import asyncio
import hashlib
import json
from contextlib import ExitStack

import httpx
import pytest

from research.company_profile import http_tasks
from research.company_profile.http_tasks import (
    CompanyProfileHttpTaskPolicy,
    CompanyProfileHttpTaskRequest,
    CompanyProfileHttpTasks,
    HttpTaskError,
    activate_successor_window,
)
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.workpaper_sections import build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_http_tasks import (
    AUTH, URL, install_offline_client, policy_for, request_for, terminal, test_app,
)
from tests.unit.test_research.test_company_profile_review_execution import storage_for
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection
from tests.unit.test_utils.test_llm_gateway import _response


def successor_for(policy, *, name="second", limit=1, **updates):
    data = policy.model_dump(mode="json")
    data.update(policy_id=name, max_submissions=limit)
    data["template"]["checkpoint_root"] = str(policy.checkpoint_root.parent / (name + "-checkpoint"))
    data["template"]["approval_reference"] = "6.FQ controlled_fixture, zero supplier authorization"
    data["template"].update(updates)
    return CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(data))


def sources_for(root, old, new):
    paths = root / "previous-policy.json", root / "successor-policy.json"
    for path, policy in zip(paths, (old, new)):
        path.write_text(policy.model_dump_json(indent=2))
    return dict(previous_policy_file=paths[0], successor_policy_file=paths[1],
                expected_previous_sha256=old.sha256)


def seed(service, *, state="failed", name="closed"):
    """Boundary fixture only; successful tests below use the real TaskService."""
    request = CompanyProfileHttpTaskRequest.model_validate(request_for(name))
    task_id = "profile-http-" + hashlib.sha256(name.encode()).hexdigest()[:32]
    plan = service._plan(request, task_id)
    root = service._task_root(task_id)
    root.mkdir()
    (root / "plan.json").write_text(plan.model_dump_json())
    with service._db() as db:
        db.execute("INSERT INTO tasks(task_id,request_id,fingerprint,request_json,plan_json,state,created_at,updated_at) "
                   "VALUES(?,?,?,?,?,?,?,?)", (task_id, name, http_tasks._hash(request.model_dump(mode="json")),
                   request.model_dump_json(), plan.model_dump_json(), state, "created", "updated"))
    return task_id


def frozen_bytes(service, sources):
    paths = [p for p in service.policy.checkpoint_root.rglob("*") if p.is_file()]
    paths += [sources["previous_policy_file"]]
    paths += [p for p in service.policy.output_root.rglob("*") if p.is_file() and not p.name.startswith(".http-")]
    return {p: p.read_bytes() for p in paths}


@pytest.mark.asyncio
async def test_two_windows_original_http_owner_nonempty_history_and_allowances(tmp_path, monkeypatch):
    selection = _selection(index_sections(PAGES))
    packet = build_section_packet(_report(), PAGES, index_sections(PAGES), selection)
    first_draft = _draft(packet).model_dump(mode="json")
    supplement = _draft(packet).model_dump(mode="json")
    supplement["elements"]["operating_drivers"] = {"status": "observed", "limitations": [], "facts": [{
        "fact_id": "interest-risk", "text": "经营受利率环境影响。", "subject_scope": "consolidated_group",
        "period": "2025", "evidence_ids": [packet.evidence[-1].evidence_id], "metrics": []}]}
    scripts = [[_response(selection.model_dump(mode="json")), _response(first_draft)],
               [_response(selection.model_dump(mode="json")), _response(supplement),
                _response({"reviewed_fact_ids": ["main", "revenue", "interest-risk"], "findings": []})]]
    config, transports = install_offline_client(monkeypatch, scripts)
    storage = storage_for(tmp_path)
    dependencies = lambda: {"storage": storage, "page_source": lambda _: {"report": _report(), "pages": PAGES},
                            "llm_config": config}
    previous = policy_for(tmp_path, limit=1)
    successor = successor_for(previous, limit=2, budget={**previous.template.budget.model_dump(mode="json"),
                                                       "tokens": 750000, "physical_requests": 8})
    sources = sources_for(tmp_path, previous, successor)
    old = CompanyProfileHttpTasks(previous, dependencies=dependencies)
    new = None
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=test_app(old, monkeypatch)),
                                   base_url="http://controlled-fixture") as client:
            response = await client.post(URL, json=request_for("first-window"), headers=AUTH)
            assert response.status_code == 202, response.text
            first = response.json()
            status = await terminal(client, first)
            assert status["state"] == "completed" and status["produced_readings"]
            assert status["allowance"]["remaining_submissions"] == 0
            assert (await client.post(URL, json=request_for("exhausted"), headers=AUTH)).status_code == 429
        # Terminal state can precede the final owner release by a few instructions.
        for _ in range(300):
            with old._mutex:
                if not old._workers:
                    break
            await asyncio.sleep(0.01)
        preserved = frozen_bytes(old, sources)
        with pytest.raises(HttpTaskError, match="drift"):
            CompanyProfileHttpTasks(successor, dependencies=dependencies)
        receipt = activate_successor_window(**sources)
        assert len(transports) == 1 and len(transports[0].calls) == 2
        assert receipt == activate_successor_window(**sources)
        with http_tasks._file_lock(successor.checkpoint_root / ".http-profile-execution.lock"):
            assert receipt == activate_successor_window(**sources)  # A committed retry never reactivates.
        new = CompanyProfileHttpTasks(successor, dependencies=dependencies)
        with pytest.raises(HttpTaskError, match="window_inactive"):
            await old.submit(CompanyProfileHttpTaskRequest.model_validate(request_for("stale-worker")))
        lease = ExitStack()
        lease.enter_context(http_tasks._file_lock(old._task_root(first["task_id"]) / "owner.lock"))
        old._execute(first["task_id"], lease)  # Stale dispatch cannot send or rewrite old ledger.
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=test_app(new, monkeypatch)),
                                   base_url="http://controlled-fixture") as client:
            reused = await client.post(URL, json=request_for("first-window"), headers=AUTH)
            assert reused.status_code == 202 and reused.json()["task_id"] == first["task_id"]
            assert (await client.post(URL, json=request_for("first-window", "review_batch"), headers=AUTH)).status_code == 409
            with new._db() as db:
                assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
            before = (await client.get(first["result_url"], headers=AUTH)).json()
            assert before["policy_sha256"] == previous.sha256
            assert before["allowance"]["accepted_submissions"] == 1
            assert before["allowance"]["remaining_submissions"] == 0
            assert before["allowance"]["max_submissions"] == 1
            assert before["allowance"]["per_task"]["tokens"] == 1000000
            assert any(f["fact"] for f in before["result"]["profiles"][0]["facts"])
            response = await client.post(URL, json=request_for("second-window", "enrich", targets=[{
                "element": "operating_drivers", "question": "经营主要受什么因素影响？"}]), headers=AUTH)
            assert response.status_code == 202, response.text
            second = response.json()
            status = await terminal(client, second)
            assert status["state"] == "completed" and status["produced_readings"]
            assert status["policy_id"] == successor.policy_id and status["policy_sha256"] == successor.sha256
            assert status["allowance"]["accepted_submissions"] == 1
            assert status["allowance"]["remaining_submissions"] == 1
            assert status["allowance"]["max_submissions"] == 2
            assert status["allowance"]["per_task"]["tokens"] == 750000
            assert status["allowance"]["aggregate_ceiling"]["tokens"] == 1500000
            current = (await client.get(second["result_url"], headers=AUTH)).json()["result"]["profiles"][0]
            assert any(f["fact"] and "利率" in f["fact"]["text"] for f in current["facts"])
            assert all(r["origin"] == "controlled_fixture" for r in current["readings"])
            assert current == CompanyProfileReadService(previous.output_root).research_profile("600000.SH")
            old_result = (await client.get(first["result_url"], headers=AUTH)).json()
            assert old_result["result"]["profiles"][0] == current
            assert old_result["policy_sha256"] == previous.sha256
            assert old_result["allowance"] == before["allowance"]
        assert [len(t.calls) for t in transports] == [2, 3]
        assert all(p.read_bytes() == data for p, data in preserved.items())
        await new.close()
        new = CompanyProfileHttpTasks(successor, dependencies=lambda: pytest.fail("restart must not send"))
        assert new.status(first["task_id"])["policy_sha256"] == previous.sha256
        assert new.result(second["task_id"])["result"]["profiles"][0] == current
        assert [len(t.calls) for t in transports] == [2, 3]
    finally:
        await old.close()
        if new is not None:
            await new.close()


@pytest.mark.parametrize("case", ["unspent", "queued", "running", "owner", "execution", "wrong_hash",
                                  "source_drift", "pin_drift", "used_checkpoint", "cross_root", "unauthorized"])
def test_activation_rejects_unsafe_predecessor_or_successor(tmp_path, case):
    previous = policy_for(tmp_path, limit=1)
    service = CompanyProfileHttpTasks(previous, dependencies=dict)
    task_id = None if case == "unspent" else seed(service, state=case if case in {"queued", "running"} else "failed")
    successor = successor_for(previous)
    if case == "cross_root":
        successor = successor_for(previous, output_root=str(tmp_path / "other-output"))
    sources = sources_for(tmp_path, previous, successor)
    if case == "wrong_hash":
        sources["expected_previous_sha256"] = "0" * 64
    if case == "source_drift":
        sources["previous_policy_file"].write_text(successor.model_dump_json())
    if case == "unauthorized":
        data = successor.model_dump(mode="json")
        data["template"]["authorization_status"] = "planned"
        sources["successor_policy_file"].write_text(json.dumps(data))
    if case == "pin_drift":
        (previous.output_root / ".http-profile-policy.json").write_text("{}")
    if case == "used_checkpoint":
        successor.checkpoint_root.mkdir()
        (successor.checkpoint_root / "started.json").write_text("old budget must not be reset")
    preserved = frozen_bytes(service, sources)
    active = (previous.output_root / ".http-profile-policy.json").read_bytes()
    with ExitStack() as locks:
        if case in {"owner", "execution"}:
            path = (service._task_root(task_id) / "owner.lock" if case == "owner" else
                    previous.output_root / ".http-profile-execution.lock")
            locks.enter_context(http_tasks._file_lock(path))
        with pytest.raises(HttpTaskError):
            activate_successor_window(**sources)
    assert (previous.output_root / ".http-profile-policy.json").read_bytes() == active
    assert all(p.read_bytes() == data for p, data in preserved.items())


@pytest.mark.parametrize("interrupted_at", [1, 2, 3, 4])
@pytest.mark.asyncio
async def test_partial_activation_requires_explicit_identical_resume(tmp_path, monkeypatch, interrupted_at):
    previous = policy_for(tmp_path, limit=1)
    old = CompanyProfileHttpTasks(previous, dependencies=lambda: pytest.fail("operator must not construct model"))
    task_id = seed(old)
    successor = successor_for(previous)
    sources = sources_for(tmp_path, previous, successor)
    preserved = frozen_bytes(old, sources)
    write = http_tasks._atomic_json
    calls = []

    def interrupt(path, payload):
        calls.append(path)
        if interrupted_at == 4 and path == previous.output_root / ".http-profile-policy.json":
            write(path, payload)
            raise OSError("controlled lost commit acknowledgement")
        if len(calls) == interrupted_at:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.with_name(path.name + ".pending").write_text('{"controlled_partial":')
            raise OSError("controlled activation interruption")
        write(path, payload)

    monkeypatch.setattr(http_tasks, "_atomic_json", interrupt)
    with pytest.raises(HttpTaskError, match="activation_invalid"):
        activate_successor_window(**sources)
    if interrupted_at <= 3:
        with pytest.raises(HttpTaskError, match="drift"):
            CompanyProfileHttpTasks(successor, dependencies=dict)
    if interrupted_at in {2, 3}:
        alternate = tmp_path / "alternate-policy.json"
        alternate.write_text(successor_for(previous, name="other").model_dump_json())
        with pytest.raises(HttpTaskError, match="successor_conflict"):
            activate_successor_window(**{**sources, "successor_policy_file": alternate})
    monkeypatch.setattr(http_tasks, "_atomic_json", write)
    receipt = activate_successor_window(**sources)
    assert receipt == activate_successor_window(**sources)
    new = CompanyProfileHttpTasks(successor, dependencies=lambda: pytest.fail("restart must not send"))
    assert new.status(task_id)["state"] == "failed"
    assert new.status(task_id)["allowance"]["remaining_submissions"] == 0
    assert all(p.read_bytes() == data for p, data in preserved.items())
    assert new.database != old.database
    await new.close()
    await old.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["ledger", "missing_transition", "invalid_transition"])
async def test_chain_failure_never_resubmits_old_request_and_conflicting_successor_rejected(tmp_path, damage):
    previous = policy_for(tmp_path, limit=1)
    old = CompanyProfileHttpTasks(previous, dependencies=dict)
    task_id = seed(old)
    successor = successor_for(previous)
    sources = sources_for(tmp_path, previous, successor)
    activate_successor_window(**sources)
    new = CompanyProfileHttpTasks(successor, dependencies=lambda: pytest.fail("no model"))
    other = successor_for(previous, name="conflicting")
    sources["successor_policy_file"].write_text(other.model_dump_json())
    with pytest.raises(HttpTaskError, match="successor_conflict"):
        activate_successor_window(**sources)
    if damage == "ledger":
        old._update(task_id, failure_code="tampered-original-failure")
    elif damage == "missing_transition":
        http_tasks._transition_path(previous).unlink()
    else:
        http_tasks._transition_path(previous).write_text("{}")
    with pytest.raises(HttpTaskError, match="history_invalid"):
        await new.submit(CompanyProfileHttpTaskRequest.model_validate(request_for("closed")))
    with pytest.raises(HttpTaskError, match="history_invalid"):
        new.status(task_id)
    with new._db() as db:
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
    await new.close()
    await old.close()


@pytest.mark.asyncio
async def test_multiple_successors_keep_original_policy_and_request_ownership(tmp_path):
    previous = policy_for(tmp_path, limit=1)
    first = CompanyProfileHttpTasks(previous, dependencies=dict)
    first_id = seed(first)
    second_policy = successor_for(previous)
    sources = sources_for(tmp_path, previous, second_policy)
    activate_successor_window(**sources)
    second = CompanyProfileHttpTasks(second_policy, dependencies=dict)
    second_id = seed(second, name="next-closed")
    third_policy = successor_for(second_policy, name="third", limit=2)
    third_source = tmp_path / "third-policy.json"
    third_source.write_text(third_policy.model_dump_json())
    activate_successor_window(previous_policy_file=sources["successor_policy_file"],
        successor_policy_file=third_source, expected_previous_sha256=second_policy.sha256)
    third = CompanyProfileHttpTasks(third_policy, dependencies=lambda: pytest.fail("history never sends"))
    for task_id, policy, name in [(first_id, previous, "closed"), (second_id, second_policy, "next-closed")]:
        assert third.status(task_id)["policy_sha256"] == policy.sha256
        assert third.status(task_id)["allowance"]["remaining_submissions"] == 0
        assert (await third.submit(CompanyProfileHttpTaskRequest.model_validate(request_for(name))))["task_id"] == task_id
    with third._db() as db:
        assert db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
    await third.close()
    await second.close()
    await first.close()


def test_operator_cli_reuses_owner_and_authorized_policy(tmp_path, monkeypatch, capsys):
    previous = policy_for(tmp_path, limit=1)
    old = CompanyProfileHttpTasks(previous, dependencies=dict)
    seed(old)
    sources = sources_for(tmp_path, previous, successor_for(previous))
    monkeypatch.setattr(http_tasks, "_default_dependencies", lambda: pytest.fail("activation constructs no model"))
    assert http_tasks.main(["activate", "--previous-policy", str(sources["previous_policy_file"]),
        "--policy", str(sources["successor_policy_file"]), "--expected-previous-sha256", previous.sha256]) == 0
    assert json.loads(capsys.readouterr().out)["previous_policy"]["policy_id"] == previous.policy_id
