"""Finite HTTP commands for the existing annual-profile task entry.

This is a research-only adapter: it stores commands and references, never facts.
Only an explicit submit schedules work; construction, reads and restart do not.
"""
from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import threading
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from utils.llm.models import project_root

from .models import PRODUCTION_AUTHORIZATION
from .reads import CompanyProfileReadService
from .task_entry import CompanyProfileTaskPlan, execute_configured_task
from .workpaper_enrichment import EnrichmentTarget

logger = logging.getLogger(__name__)


class HttpTaskError(Exception):
    def __init__(self, status: int, code: str):
        self.status, self.code = status, code
        super().__init__(code)


class CompanyProfileHttpTaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)
    schema_version: Literal["company_profile_http_task_request.v1"] = "company_profile_http_task_request.v1"
    request_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    action: Literal["first_read", "review_batch", "enrich"]
    instrument_ids: list[str] = Field(min_length=1, max_length=100)
    targets: list[EnrichmentTarget] = Field(default_factory=list, max_length=12)

    @model_validator(mode="after")
    def scope(self):
        import re

        if len(set(self.instrument_ids)) != len(self.instrument_ids) or any(
            not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", code) for code in self.instrument_ids
        ):
            raise ValueError("distinct canonical stock IDs are required")
        if self.action == "enrich":
            if len(self.instrument_ids) != 1 or not self.targets:
                raise ValueError("enrich requires one stock and concrete targets")
        elif self.targets:
            raise ValueError("targets are only valid for enrich")
        return self


class CompanyProfileHttpTaskPolicy(BaseModel):
    """One immutable, finite server installation, not a new execution grant."""
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)
    schema_version: Literal["company_profile_http_task_policy.v1"]
    policy_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
    allowed_actions: tuple[Literal["first_read", "review_batch", "enrich"], ...] = Field(min_length=1)
    max_submissions: int = Field(ge=1, le=100)
    template: CompanyProfileTaskPlan

    @model_validator(mode="after")
    def authorized_template(self):
        if (self.template.authorization_status != "authorized" or self.template.action != "first_read"
            or len(set(self.allowed_actions)) != len(self.allowed_actions)):
            raise ValueError("policy requires an authorized first_read template and distinct actions")
        return self

    @property
    def sha256(self) -> str:
        return _hash(self.model_dump(mode="json"))

    @property
    def output_root(self) -> Path:
        return (project_root() / self.template.output_root).resolve()

    @property
    def checkpoint_root(self) -> Path:
        return (project_root() / self.template.checkpoint_root).resolve()


def load_http_task_policy() -> CompanyProfileHttpTaskPolicy:
    name = os.environ.get("QUOTE_COMPANY_PROFILE_TASK_POLICY", "").strip()
    if not name:
        raise HttpTaskError(503, "company_profile_tasks_not_configured")
    try:
        policy = CompanyProfileHttpTaskPolicy.model_validate_json(Path(name).expanduser().read_text())
        if policy.output_root == policy.checkpoint_root:
            raise ValueError("output and checkpoint roots must be distinct")
        from .operations import DEFAULT_OUTPUT_ROOT

        configured = os.environ.get("QUOTE_COMPANY_PROFILE_RESEARCH_ROOT")
        read_root = Path(configured).expanduser() if configured is not None else Path(DEFAULT_OUTPUT_ROOT)
        if (configured is not None and not configured.strip()) or read_root.resolve() != policy.output_root:
            raise ValueError("HTTP reads and execution must share the output root")
        return policy
    except (OSError, ValueError) as exc:
        raise HttpTaskError(503, "company_profile_task_policy_invalid") from exc


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pin(policy: CompanyProfileHttpTaskPolicy) -> dict:
    return {"policy_sha256": policy.sha256, "output_root": str(policy.output_root),
            "checkpoint_root": str(policy.checkpoint_root)}


def _transition_path(policy: CompanyProfileHttpTaskPolicy) -> Path:
    return policy.output_root / ".http-profile-transitions" / (policy.sha256 + ".json")


def _atomic_json(path: Path, value: dict) -> None:
    """Publish a durable complete file; the output pin is the activation commit."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_name(path.name + ".pending")
    try:
        with pending.open("w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        pending.unlink(missing_ok=True)


@contextmanager
def _read_db(path: Path):
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def _windows(policy: CompanyProfileHttpTaskPolicy, pin: dict):
    """Resolve only explicit, hash-bound predecessors; never install old owners."""
    windows, seen = [], set()
    try:
        while True:
            base = _pin(policy)
            if (policy.sha256 in seen or any(pin.get(k) != v for k, v in base.items()) or
                json.loads((policy.checkpoint_root / ".http-profile-policy.json").read_text()) != pin):
                raise ValueError("invalid window pin")
            seen.add(policy.sha256)
            database = policy.checkpoint_root / "http_tasks" / "tasks.sqlite3"
            windows.append((policy, pin, database))
            if pin == base:
                return windows
            previous_hash = pin["previous_policy_sha256"]
            receipt = json.loads((policy.output_root / ".http-profile-transitions" /
                                  (previous_hash + ".json")).read_text())
            if (_hash(receipt) != pin["transition_sha256"] or
                receipt["successor_policy"] != policy.model_dump(mode="json")):
                raise ValueError("invalid transition")
            previous = CompanyProfileHttpTaskPolicy.model_validate_json(json.dumps(receipt["previous_policy"]))
            if (previous.sha256 != previous_hash or previous.output_root != policy.output_root or
                previous.checkpoint_root == policy.checkpoint_root):
                raise ValueError("invalid predecessor")
            database = previous.checkpoint_root / "http_tasks" / "tasks.sqlite3"
            if hashlib.sha256(database.read_bytes()).hexdigest() != receipt["previous_database_sha256"]:
                raise ValueError("historical ledger changed")
            policy, pin = previous, receipt["previous_pin"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise HttpTaskError(503, "company_profile_task_history_invalid") from exc


def activate_successor_window(*, previous_policy_file: str | Path,
                              successor_policy_file: str | Path,
                              expected_previous_sha256: str) -> dict:
    """Activate a separately authorized finite window without constructing a model.

    expected_previous_sha256 is the canonical policy hash exposed by task status
    and the installation pin. Receipts also preserve both source-file byte hashes.
    A durable intent reserves one successor; only re-invoking this operator action
    can finish an interrupted activation. Constructors never complete it.
    """
    try:
        sources = [Path(p).expanduser().resolve() for p in (previous_policy_file, successor_policy_file)]
        raw = [p.read_bytes() for p in sources]
        previous, successor = [CompanyProfileHttpTaskPolicy.model_validate_json(p) for p in raw]
        if previous.sha256 != expected_previous_sha256:
            raise HttpTaskError(409, "company_profile_task_predecessor_mismatch")
        roots = [previous.output_root, previous.checkpoint_root, successor.checkpoint_root]
        if (successor.output_root != previous.output_root or
            any(a == b or a in b.parents or b in a.parents for i, a in enumerate(roots) for b in roots[i + 1:])):
            raise HttpTaskError(409, "company_profile_task_successor_scope_conflict")
        with ExitStack() as locks:
            for root in sorted(roots):
                locks.enter_context(_file_lock(root / ".http-profile-install.lock", blocking=True))
            old_pin = json.loads((previous.checkpoint_root / ".http-profile-policy.json").read_text())
            history = _windows(previous, old_pin)
            if any(successor.checkpoint_root == p.checkpoint_root or
                   successor.checkpoint_root in p.checkpoint_root.parents or
                   p.checkpoint_root in successor.checkpoint_root.parents for p, _, _ in history):
                raise HttpTaskError(409, "company_profile_task_checkpoint_used")
            database = history[0][2]
            with _read_db(database) as db:
                rows = db.execute("SELECT task_id,state FROM tasks").fetchall()
            if len(rows) != previous.max_submissions:
                raise HttpTaskError(409, "company_profile_task_predecessor_not_exhausted")
            if any(row["state"] not in {"completed", "failed", "interrupted"} for row in rows):
                raise HttpTaskError(409, "company_profile_task_predecessor_in_flight")
            receipt = {"schema_version": "company_profile_http_window_transition.v1",
                "previous_policy": previous.model_dump(mode="json"), "previous_pin": old_pin,
                "successor_policy": successor.model_dump(mode="json"),
                "previous_database_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
                "policy_sources": [{"path": str(path), "file_sha256": hashlib.sha256(data).hexdigest()}
                                   for path, data in zip(sources, raw)]}
            next_pin = {**_pin(successor), "previous_policy_sha256": previous.sha256,
                        "transition_sha256": _hash(receipt)}
            intent = _transition_path(previous)
            active_path = previous.output_root / ".http-profile-policy.json"
            active = json.loads(active_path.read_text())
            if intent.exists() and json.loads(intent.read_text()) != receipt:
                raise HttpTaskError(409, "company_profile_task_successor_conflict")
            if active == next_pin:
                _windows(successor, next_pin)
                return receipt
            if active != old_pin:
                raise HttpTaskError(409, "company_profile_task_policy_drift")
            for root in sorted(roots):
                locks.enter_context(_file_lock(root / ".http-profile-execution.lock"))
            for path in sorted((previous.checkpoint_root / "http_tasks").glob("*/owner.lock")):
                locks.enter_context(_file_lock(path))
            if hashlib.sha256(database.read_bytes()).hexdigest() != receipt["previous_database_sha256"]:
                raise HttpTaskError(409, "company_profile_task_policy_drift")
            allowed = {".http-profile-install.lock", ".http-profile-execution.lock"}
            if intent.exists():
                allowed.update({".http-profile-policy.json", ".http-profile-policy.json.pending"})
            if any(p.name not in allowed for p in successor.checkpoint_root.iterdir()):
                raise HttpTaskError(409, "company_profile_task_checkpoint_used")
            checkpoint_pin = successor.checkpoint_root / ".http-profile-policy.json"
            if checkpoint_pin.exists() and json.loads(checkpoint_pin.read_text()) != next_pin:
                raise HttpTaskError(409, "company_profile_task_policy_drift")
            if not intent.exists():
                _atomic_json(intent, receipt)
            if not checkpoint_pin.exists():
                _atomic_json(checkpoint_pin, next_pin)
            _atomic_json(active_path, next_pin)
            return receipt
    except HttpTaskError:
        raise
    except BlockingIOError as exc:
        raise HttpTaskError(409, "company_profile_task_window_busy") from exc
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        raise HttpTaskError(409, "company_profile_task_activation_invalid") from exc


@contextmanager
def _file_lock(path: Path, *, blocking: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        try:
            yield stream
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _default_dependencies() -> dict[str, Any]:
    from data_manager import data_manager

    storage = getattr(data_manager, "research_storage", None)
    if storage is None:
        raise HttpTaskError(503, "research_storage_unavailable")
    getter = getattr(data_manager, "_get_announcement_asset_access", None)
    access = getter(initialize_schema=False) if callable(getter) else None
    return {"storage": storage, "shared_asset_access": access}


class CompanyProfileHttpTasks:
    """Persist explicit commands, serialize the original writer scope, project reads.

    Workers use their own event loop so synchronous source processing cannot
    block HTTP reads. Shutdown is cooperative and joins the actual execution;
    cancelling an asyncio wrapper around a still-running source thread is unsafe.
    """

    def __init__(self, policy: CompanyProfileHttpTaskPolicy, *, dependencies=None):
        self.policy = CompanyProfileHttpTaskPolicy.model_validate_json(policy.model_dump_json())
        self.reader = CompanyProfileReadService(self.policy.output_root)
        self.root = self.policy.checkpoint_root / "http_tasks"
        self.database = self.root / "tasks.sqlite3"
        self._dependencies = dependencies or _default_dependencies
        self._closing = threading.Event()
        self._workers: dict[str, threading.Thread] = {}
        self._running: set[str] = set()
        self._mutex = threading.Lock()
        # Pin both roots. A renamed policy or a different checkpoint may not
        # bypass the same output writer, nor vice versa.
        with ExitStack() as stack:
            for root in sorted({self.policy.output_root, self.policy.checkpoint_root}):
                stack.enter_context(_file_lock(root / ".http-profile-install.lock", blocking=True))
            for root in (self.policy.output_root, self.policy.checkpoint_root):
                pin = root / ".http-profile-policy.json"
                expected = _pin(self.policy)
                if pin.exists():
                    saved = json.loads(pin.read_text())
                    if any(saved.get(k) != v for k, v in expected.items()):
                        raise HttpTaskError(503, "company_profile_task_policy_drift")
                    if saved != expected:
                        _windows(self.policy, saved)
                    expected = saved
                if root == self.policy.output_root:
                    self._pin = expected
                elif expected != self._pin:
                    raise HttpTaskError(503, "company_profile_task_policy_drift")
            for root in (self.policy.output_root, self.policy.checkpoint_root):
                pin = root / ".http-profile-policy.json"
                if not pin.exists():
                    _atomic_json(pin, self._pin)
            self.root.mkdir(parents=True, exist_ok=True)
            with self._db() as db:
                db.execute("""CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL,
                    fingerprint TEXT NOT NULL, request_json TEXT NOT NULL, plan_json TEXT NOT NULL,
                    state TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    before_json TEXT NOT NULL DEFAULT '{}', summary_json TEXT NOT NULL DEFAULT '{}',
                    failure_code TEXT)""")
        self._recover_interrupted()

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def _task_root(self, task_id: str) -> Path:
        import re

        if not re.fullmatch(r"profile-http-[0-9a-f]{32}", task_id):
            raise HttpTaskError(404, "company_profile_task_not_found")
        return self.root / task_id

    def _assert_active(self) -> None:
        try:
            if (json.loads((self.policy.output_root / ".http-profile-policy.json").read_text()) != self._pin or
                _transition_path(self.policy).exists()):
                raise ValueError("inactive window")
        except (OSError, ValueError) as exc:
            raise HttpTaskError(409, "company_profile_task_window_inactive") from exc

    def _recover_interrupted(self, task_id: str | None = None) -> None:
        with _file_lock(self.policy.output_root / ".http-profile-install.lock", blocking=True):
            try:
                self._assert_active()
            except HttpTaskError:
                return  # Closed/pending predecessor ledgers are immutable.
            self._recover_active(task_id)

    def _recover_active(self, task_id: str | None) -> None:
        with self._db() as db:
            rows = db.execute("SELECT task_id FROM tasks WHERE state IN ('queued','running')" +
                              (" AND task_id=?" if task_id else ""), (task_id,) if task_id else ()).fetchall()
            for row in rows:
                try:
                    with _file_lock(self._task_root(row["task_id"]) / "owner.lock"):
                        db.execute("UPDATE tasks SET state='interrupted', failure_code='execution_interrupted', "
                                   "updated_at=? WHERE task_id=? AND state IN ('queued','running')",
                                   (_now(), row["task_id"]))
                except BlockingIOError:
                    pass  # A different live API worker still owns this task.

    def _plan(self, request: CompanyProfileHttpTaskRequest, task_id: str) -> CompanyProfileTaskPlan:
        policy = self.policy
        if request.action not in policy.allowed_actions or not set(request.instrument_ids) <= set(
            policy.template.instrument_ids
        ):
            raise HttpTaskError(403, "company_profile_task_scope_not_authorized")
        data = policy.template.model_dump(mode="json")
        data.update(run_id=task_id, action=request.action, instrument_ids=request.instrument_ids,
                    document_versions={code: data["document_versions"][code] for code in request.instrument_ids},
                    output_root=str(policy.output_root), checkpoint_root=str(policy.checkpoint_root))
        try:
            if request.action == "review_batch":
                data["review_plans"] = [self.reader.plan_review_recovery(code,
                    request_id=f"{task_id}-{index}") for index, code in enumerate(request.instrument_ids)]
                if any(p["state"] != "planned" for p in data["review_plans"]):
                    raise ValueError("review is not needed or cannot be planned")
            elif request.action == "enrich":
                data["enrichment_plan"] = self.reader.plan_enrichment(request.instrument_ids[0],
                    request_id=task_id, targets=request.targets)["enrichment_plan"]
            return CompanyProfileTaskPlan.model_validate_json(json.dumps(data))
        except (ValueError, FileNotFoundError) as exc:
            raise HttpTaskError(409, "company_profile_task_plan_unavailable_or_stale") from exc

    async def submit(self, request: CompanyProfileHttpTaskRequest) -> dict[str, Any]:
        if self._closing.is_set():
            raise HttpTaskError(503, "company_profile_tasks_shutting_down")
        request_json = request.model_dump(mode="json")
        fingerprint = _hash(request_json)
        task_id = "profile-http-" + hashlib.sha256(request.request_id.encode()).hexdigest()[:32]
        owner = None
        with _file_lock(self.policy.output_root / ".http-profile-install.lock", blocking=True), self._db() as db:
            self._assert_active()
            for policy, _, database in _windows(self.policy, self._pin)[1:]:
                try:
                    with _read_db(database) as old:
                        row = old.execute("SELECT * FROM tasks WHERE request_id=?", (request.request_id,)).fetchone()
                except sqlite3.Error as exc:
                    raise HttpTaskError(503, "company_profile_task_history_invalid") from exc
                if row is not None:
                    if row["fingerprint"] != fingerprint:
                        raise HttpTaskError(409, "company_profile_task_request_conflict")
                    return self._links(dict(row))
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM tasks WHERE request_id=?", (request.request_id,)).fetchone()
            if row is not None:
                if row["fingerprint"] != fingerprint:
                    raise HttpTaskError(409, "company_profile_task_request_conflict")
                return self._links(dict(row))
            if db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] >= self.policy.max_submissions:
                raise HttpTaskError(429, "company_profile_task_submission_limit")
            plan = self._plan(request, task_id)
            # This lease is held before the queued row is visible. Other API
            # workers can distinguish a live queued task from a crashed owner.
            owner = ExitStack()
            try:
                owner.enter_context(_file_lock(self._task_root(task_id) / "owner.lock"))
                path = self._task_root(task_id) / "plan.json"
                path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")
                now = _now()
                db.execute("INSERT INTO tasks(task_id,request_id,fingerprint,request_json,plan_json,state,created_at,updated_at) "
                           "VALUES(?,?,?,?,?,'queued',?,?)", (task_id, request.request_id, fingerprint,
                           json.dumps(request_json), plan.model_dump_json(), now, now))
            except BaseException:
                owner.close()
                raise
        worker = threading.Thread(target=self._execute, args=(task_id, owner),
                                  name="profile-http-" + task_id[-8:], daemon=False)
        with self._mutex:
            self._workers[task_id] = worker
        try:
            worker.start()
        except RuntimeError as exc:
            owner.close()
            with self._mutex:
                self._workers.pop(task_id, None)
            self._update(task_id, state="failed", failure_code="task_worker_start_failed")
            raise HttpTaskError(503, "task_worker_start_failed") from exc
        return self._links({"task_id": task_id, "state": "queued", "action": request.action,
                            "request_id": request.request_id})

    @staticmethod
    def _links(row: dict) -> dict:
        task_id = row["task_id"]
        request = json.loads(row["request_json"]) if "request_json" in row else row
        url = "/api/v1/research/company/annual-profile-tasks/" + task_id
        return {"schema_version": "company_profile_http_task.v1", "task_id": task_id,
                "request_id": request["request_id"], "action": request["action"], "state": row["state"],
                "status_url": url, "result_url": url + "/result", "research_only": True,
                "production_authorization": PRODUCTION_AUTHORIZATION}

    def _update(self, task_id: str, **values) -> None:
        values["updated_at"] = _now()
        with self._db() as db:
            db.execute("UPDATE tasks SET " + ",".join(k + "=?" for k in values) + " WHERE task_id=?",
                       (*values.values(), task_id))

    def _row(self, task_id: str) -> dict:
        self._task_root(task_id)
        for _, _, database in _windows(self.policy, self._pin):
            try:
                with _read_db(database) as db:
                    row = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            except sqlite3.Error as exc:
                raise HttpTaskError(503, "company_profile_task_history_invalid") from exc
            if row is not None:
                return dict(row)
        raise HttpTaskError(404, "company_profile_task_not_found")

    def _validate_saved_binding(self, plan: CompanyProfileTaskPlan) -> None:
        # Review/enrichment must bind the same current reading selected at
        # acceptance; a queued predecessor may have changed it in the meantime.
        for code in plan.instrument_ids:
            if plan.action == "first_read":
                continue
            profile = self.reader.research_profile(code)
            if plan.action == "enrich":
                expected = plan.enrichment_plan
                regenerated = self.reader.plan_enrichment(code, request_id=expected.request_id,
                                                          targets=expected.targets)["enrichment_plan"]
                if regenerated != expected.model_dump(mode="json"):
                    raise HttpTaskError(409, "company_profile_task_plan_stale")
            else:
                expected = next(p for p in plan.review_plans if p["instrument_id"] == code)
                if (profile["latest_reading_work_id"] != expected["reading_work_id"] or
                    self.reader.plan_review_recovery(code, request_id=expected["request_id"]) != expected):
                    raise HttpTaskError(409, "company_profile_task_plan_stale")

    def _execute(self, task_id: str, owner: ExitStack) -> None:
        with owner, ExitStack() as scope:
            try:
                # Same output OR checkpoint implies serialization, independent
                # of process, policy ID or request ID. Never block the API loop.
                paths = sorted({self.policy.output_root / ".http-profile-execution.lock",
                                self.policy.checkpoint_root / ".http-profile-execution.lock"})
                for path in paths:
                    while True:
                        if self._closing.wait(0.02):
                            self._update(task_id, state="interrupted", failure_code="execution_interrupted")
                            return
                        try:
                            scope.enter_context(_file_lock(path))
                            break
                        except BlockingIOError:
                            continue
                self._assert_active()
                plan = CompanyProfileTaskPlan.model_validate_json(self._row(task_id)["plan_json"])
                self._validate_saved_binding(plan)
                profiles = self.reader.research_profiles(plan.instrument_ids)["profiles"]
                before = {code: [] for code in plan.instrument_ids}
                before.update({p["instrument_id"]: [r["work_id"] for r in p["readings"]] for p in profiles})
                self._update(task_id, state="running", before_json=json.dumps(before))
                with self._mutex:
                    self._running.add(task_id)
                result = asyncio.run(execute_configured_task(action=plan.action,
                    execution_plan_file=self._task_root(task_id) / "plan.json", **self._dependencies()))
                summary = self._summary(result)
                interrupted = self._closing.is_set()
                self._update(task_id, state="interrupted" if interrupted else (
                    "completed" if summary["execution_state"] == "completed" else "failed"),
                    summary_json=json.dumps(summary), failure_code="execution_interrupted" if interrupted else (
                        None if summary["execution_state"] == "completed" else "task_execution_incomplete"))
            except BaseException as exc:
                # Public state never contains raw provider errors, URLs, keys,
                # filesystem paths or tracebacks. Original receipts stay local.
                logger.warning("Company-profile HTTP task failed task_id=%s kind=%s", task_id, type(exc).__name__)
                if isinstance(exc, HttpTaskError) and exc.code == "company_profile_task_window_inactive":
                    return  # Never rewrite the frozen predecessor ledger.
                self._update(task_id, state="interrupted" if self._closing.is_set() else "failed",
                    failure_code=("execution_interrupted" if self._closing.is_set() else
                                  exc.code if isinstance(exc, HttpTaskError) else "task_execution_failed"))
            finally:
                with self._mutex:
                    self._running.discard(task_id)
                    self._workers.pop(task_id, None)

    @staticmethod
    def _summary(result: dict) -> dict:
        items = result.get("items") or []
        work_ids = list((result.get("enqueue") or {}).get("work_ids") or [])
        work_ids += [i["work_id"] for i in items if i.get("work_id")]
        for item in items:
            work_ids += list((item.get("enqueue") or {}).get("work_ids") or [])
        return {"execution_state": str(result.get("state") or "unknown"),
                "work_ids": list(dict.fromkeys(work_ids))}

    def status(self, task_id: str) -> dict:
        self._recover_interrupted(task_id)
        row = self._row(task_id)
        request = json.loads(row["request_json"])
        available = self.reader.research_profiles(request["instrument_ids"])["profiles"]
        before, summary = json.loads(row["before_json"]), json.loads(row["summary_json"])
        for policy, _, database in _windows(self.policy, self._pin):
            with _read_db(database) as db:
                if db.execute("SELECT 1 FROM tasks WHERE task_id=?", (task_id,)).fetchone():
                    accepted = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
                    break
        budget = policy.template.budget
        per_task = {"seconds": budget.seconds, "tokens": budget.tokens,
                    "physical_requests": budget.physical_requests}
        allowance = {"max_submissions": policy.max_submissions, "accepted_submissions": accepted,
                     "remaining_submissions": policy.max_submissions - accepted,
                     "per_task": per_task,
                     "aggregate_ceiling": {key: value * policy.max_submissions for key, value in per_task.items()}}
        produced, saved = [], []
        completed = 0
        for profile in available:
            for reading in profile["readings"]:
                current = {"instrument_id": profile["instrument_id"], "work_id": reading["work_id"],
                           "review_status": reading["review_status"]}
                saved.append(current)
                new_reading = (profile["instrument_id"] in before and
                               reading["work_id"] not in before[profile["instrument_id"]])
                attributable = new_reading and (row["state"] in {"queued", "running"} or
                                                reading["work_id"] in summary.get("work_ids", ()))
                if attributable:
                    produced.append(current)
            if any(r["work_id"] in summary.get("work_ids", ()) for r in profile["readings"]):
                completed += 1
        return {**self._links(row), "instrument_ids": request["instrument_ids"],
                "created_at": row["created_at"], "updated_at": row["updated_at"],
                "policy_id": policy.policy_id, "policy_sha256": policy.sha256,
                "requested_items": len(request["instrument_ids"]),
                "allowance": allowance,
                "completed_items": completed, "available_profiles": len(available),
                "saved_readings": saved, "produced_readings": produced,
                "execution_state": summary.get("execution_state"), "failure_code": row["failure_code"]}

    def result(self, task_id: str) -> dict:
        status = self.status(task_id)
        return {**status, "result_semantics": "current_saved_research_profiles_not_a_quality_certificate",
                "result": self.reader.research_profiles(status["instrument_ids"])}

    async def close(self) -> None:
        """Stop cooperatively and join workers before releasing writer locks.

        The original production owner has synchronous stage threads. Cancelling
        its asyncio wrapper would not join them, so do not detach them on exit.
        An in-flight send finishes under its existing finite budget; subsequent
        stage dispatch observes the original stop control. Restart never sends.
        """
        from .operations import CompanyProfileTaskControl

        self._closing.set()
        async def join_workers():
            with self._mutex:
                pending = list(self._workers.values())
            while any(worker.is_alive() for worker in pending):
                with self._mutex:
                    running = bool(self._running)
                if running:
                    control = CompanyProfileTaskControl(self.policy.checkpoint_root)
                    if control.read().get("state") in {"running", "stop_requested"}:
                        control.request_stop(reason="http_research_shutdown")
                await asyncio.sleep(0.05)
            for worker in pending:
                worker.join()

        joined = asyncio.create_task(join_workers())
        try:
            await asyncio.shield(joined)
        except asyncio.CancelledError:
            # Lifespan cancellation must not close shared storage underneath a
            # still-running stage thread or release the writer lock early.
            await joined
            raise


def main(argv: list[str] | None = None) -> int:
    """Operator-only activation; authorization belongs to the supplied policy."""
    import argparse

    parser = argparse.ArgumentParser(description="Activate an authorized finite company-profile successor")
    commands = parser.add_subparsers(dest="command", required=True)
    activate = commands.add_parser("activate")
    activate.add_argument("--previous-policy", required=True, type=Path)
    activate.add_argument("--policy", required=True, type=Path)
    activate.add_argument("--expected-previous-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        receipt = activate_successor_window(previous_policy_file=args.previous_policy,
            successor_policy_file=args.policy, expected_previous_sha256=args.expected_previous_sha256)
    except HttpTaskError as exc:
        print(json.dumps({"error": exc.code, "status": exc.status}))
        return 1
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
