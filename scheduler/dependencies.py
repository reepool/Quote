"""Configuration-driven scheduler task dependency execution."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence


DEPENDENCY_PHASES = ("pre_success", "post_success", "post_always")
DEPENDENCY_MODES = {"parallel", "serial"}
FAILURE_POLICIES = {"fail_parent", "degrade_parent", "ignore", "stop_chain"}


@dataclass(frozen=True)
class DependencyNodeConfig:
    job_id: str
    inherit: tuple[str, ...] = ()
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: Optional[float] = None
    failure_policy: str = ""
    enabled: bool = True


@dataclass(frozen=True)
class DependencyGroupConfig:
    group_id: str
    mode: str
    jobs: tuple[DependencyNodeConfig, ...] = ()


@dataclass(frozen=True)
class JobDependencyConfig:
    pre_success: tuple[DependencyGroupConfig, ...] = ()
    post_success: tuple[DependencyGroupConfig, ...] = ()
    post_always: tuple[DependencyGroupConfig, ...] = ()

    def groups_for_phase(self, phase: str) -> tuple[DependencyGroupConfig, ...]:
        return getattr(self, phase, ())

    def has_dependencies(self) -> bool:
        return any(self.groups_for_phase(phase) for phase in DEPENDENCY_PHASES)


RawJobRunner = Callable[[str, Dict[str, Any], bool], Awaitable[Any]]


def parse_job_dependency_config(raw: Optional[Dict[str, Any]]) -> JobDependencyConfig:
    """Parse a raw job-level dependencies block into immutable config objects."""
    if not isinstance(raw, dict) or not raw:
        return JobDependencyConfig()
    parsed: Dict[str, tuple[DependencyGroupConfig, ...]] = {}
    for phase in DEPENDENCY_PHASES:
        groups = []
        for group_index, group_raw in enumerate(raw.get(phase) or []):
            if not isinstance(group_raw, dict):
                group_raw = {}
            jobs = []
            for node_raw in group_raw.get("jobs") or []:
                if not isinstance(node_raw, dict):
                    node_raw = {}
                timeout = node_raw.get("timeout_seconds")
                inherit_raw = node_raw.get("inherit") or []
                inherit = (
                    tuple(str(item) for item in inherit_raw)
                    if isinstance(inherit_raw, (list, tuple))
                    else ("",)
                )
                parameters_raw = node_raw.get("parameters") or {}
                jobs.append(
                    DependencyNodeConfig(
                        job_id=str(node_raw.get("job_id") or ""),
                        inherit=inherit,
                        parameters=dict(parameters_raw) if isinstance(parameters_raw, dict) else {},
                        timeout_seconds=float(timeout) if timeout is not None else None,
                        failure_policy=str(node_raw.get("failure_policy") or ""),
                        enabled=bool(node_raw.get("enabled", True)),
                    )
                )
            groups.append(
                DependencyGroupConfig(
                    group_id=str(group_raw.get("group_id") or f"{phase}_{group_index + 1}"),
                    mode=str(group_raw.get("mode") or "serial"),
                    jobs=tuple(jobs),
                )
            )
        parsed[phase] = tuple(groups)
    return JobDependencyConfig(**parsed)


def validate_job_dependencies(job_configs: Dict[str, Any]) -> List[str]:
    """Return dependency configuration errors for all loaded jobs."""
    errors: List[str] = []
    job_ids = set(job_configs)
    graph: Dict[str, List[str]] = {job_id: [] for job_id in job_ids}
    for job_id, job_config in job_configs.items():
        dependencies = getattr(job_config, "dependencies", JobDependencyConfig())
        for phase in DEPENDENCY_PHASES:
            seen_group_ids: set[str] = set()
            for group in dependencies.groups_for_phase(phase):
                if group.group_id in seen_group_ids:
                    errors.append(f"{job_id}.{phase}: duplicate group_id {group.group_id!r}")
                seen_group_ids.add(group.group_id)
                if group.mode not in DEPENDENCY_MODES:
                    errors.append(f"{job_id}.{phase}.{group.group_id}: invalid mode {group.mode!r}")
                if not group.jobs:
                    errors.append(f"{job_id}.{phase}.{group.group_id}: jobs must not be empty")
                for node in group.jobs:
                    if not node.job_id:
                        errors.append(f"{job_id}.{phase}.{group.group_id}: dependency job_id is required")
                        continue
                    if node.job_id not in job_ids:
                        errors.append(f"{job_id}.{phase}.{group.group_id}: unknown dependency job_id {node.job_id!r}")
                    else:
                        graph[job_id].append(node.job_id)
                    if not isinstance(node.inherit, tuple) or any(not item for item in node.inherit):
                        errors.append(f"{job_id}.{phase}.{group.group_id}.{node.job_id}: invalid inherit list")
                    if node.timeout_seconds is not None and node.timeout_seconds <= 0:
                        errors.append(f"{job_id}.{phase}.{group.group_id}.{node.job_id}: timeout_seconds must be positive")
                    if node.failure_policy and node.failure_policy not in FAILURE_POLICIES:
                        errors.append(
                            f"{job_id}.{phase}.{group.group_id}.{node.job_id}: invalid failure_policy {node.failure_policy!r}"
                        )
    errors.extend(_detect_dependency_cycles(graph))
    return errors


def _detect_dependency_cycles(graph: Dict[str, List[str]]) -> List[str]:
    errors: List[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()
    path: List[str] = []

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            try:
                start = path.index(node)
                cycle = path[start:] + [node]
            except ValueError:
                cycle = [node, node]
            errors.append("dependency cycle detected: " + " -> ".join(cycle))
            return
        visiting.add(node)
        path.append(node)
        for child in graph.get(node, []):
            visit(child)
        path.pop()
        visiting.remove(node)
        visited.add(node)

    for job_id in graph:
        visit(job_id)
    return errors


def is_successful_task_result(result: Any) -> bool:
    """Normalize task return values into scheduler success semantics."""
    if isinstance(result, bool):
        return result
    if isinstance(result, dict):
        status = str(result.get("status") or "").lower()
        return status in {"success", "degraded", "partial", "scan_only", "disabled", "unavailable"}
    return bool(result)


def dependency_results_empty() -> Dict[str, List[Dict[str, Any]]]:
    return {phase: [] for phase in DEPENDENCY_PHASES}


class SchedulerDependencyExecutor:
    """Execute configured scheduler dependencies around a raw parent job."""

    def __init__(
        self,
        *,
        job_configs: Dict[str, Any],
        raw_job_runner: RawJobRunner,
        logger: Any,
    ) -> None:
        self.job_configs = job_configs
        self.raw_job_runner = raw_job_runner
        self.logger = logger

    async def run_job(
        self,
        job_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        *,
        include_dependencies: bool = True,
        chain: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        """Run a job and configured dependency groups, returning aggregate state."""
        runtime_parameters = dict(parameters or {})
        dependency_results = dependency_results_empty()
        chain_tuple = tuple(chain or ())
        if job_id in chain_tuple:
            return {
                "job_id": job_id,
                "status": "failed",
                "success": False,
                "error": "dependency cycle at runtime: " + " -> ".join([*chain_tuple, job_id]),
                "result": False,
                "dependency_results": dependency_results,
            }
        job_config = self.job_configs.get(job_id)
        dependencies = getattr(job_config, "dependencies", JobDependencyConfig()) if job_config else JobDependencyConfig()
        if not include_dependencies or not dependencies.has_dependencies():
            return await self._run_raw_parent(job_id, runtime_parameters)

        parent_started = False
        parent_result: Any = False
        parent_success = False
        final_status = "success"
        pre_ok = await self._run_phase(
            job_id,
            "pre_success",
            dependencies.groups_for_phase("pre_success"),
            runtime_parameters,
            dependency_results,
            chain_tuple,
        )
        if not pre_ok:
            final_status = "failed"
            return {
                "job_id": job_id,
                "status": final_status,
                "success": False,
                "parent_started": False,
                "result": False,
                "dependency_results": dependency_results,
            }

        parent_started = True
        parent = await self._run_raw_parent(job_id, runtime_parameters)
        parent_result = parent.get("result")
        parent_success = bool(parent.get("success"))
        final_status = "success" if parent_success else "failed"

        if parent_success:
            post_ok = await self._run_phase(
                job_id,
                "post_success",
                dependencies.groups_for_phase("post_success"),
                runtime_parameters,
                dependency_results,
                chain_tuple,
            )
            if not post_ok:
                final_status = "degraded"

        always_ok = await self._run_phase(
            job_id,
            "post_always",
            dependencies.groups_for_phase("post_always"),
            runtime_parameters,
            dependency_results,
            chain_tuple,
        )
        if not always_ok and final_status == "success":
            final_status = "degraded"

        success = parent_success and final_status == "success"
        if parent_success and final_status == "degraded":
            success = False
        return {
            "job_id": job_id,
            "status": final_status,
            "success": success,
            "parent_started": parent_started,
            "result": parent_result,
            "dependency_results": dependency_results,
        }

    async def _run_raw_parent(self, job_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        started = time.monotonic()
        try:
            result = await self.raw_job_runner(job_id, parameters, False)
            return {
                "job_id": job_id,
                "status": "success" if is_successful_task_result(result) else "failed",
                "success": is_successful_task_result(result),
                "result": result,
                "elapsed_seconds": time.monotonic() - started,
            }
        except Exception as exc:
            return {
                "job_id": job_id,
                "status": "failed",
                "success": False,
                "result": False,
                "error": str(exc),
                "elapsed_seconds": time.monotonic() - started,
            }

    async def _run_phase(
        self,
        parent_job_id: str,
        phase: str,
        groups: Sequence[DependencyGroupConfig],
        parent_parameters: Dict[str, Any],
        dependency_results: Dict[str, List[Dict[str, Any]]],
        chain: tuple[str, ...],
    ) -> bool:
        phase_ok = True
        for group in groups:
            if group.mode == "parallel":
                group_result = await self._run_parallel_group(
                    parent_job_id, phase, group, parent_parameters, chain
                )
            else:
                group_result = await self._run_serial_group(
                    parent_job_id, phase, group, parent_parameters, chain
                )
            dependency_results[phase].append(group_result)
            if not group_result.get("success", False):
                phase_ok = False
                if phase == "pre_success":
                    break
        return phase_ok

    async def _run_parallel_group(
        self,
        parent_job_id: str,
        phase: str,
        group: DependencyGroupConfig,
        parent_parameters: Dict[str, Any],
        chain: tuple[str, ...],
    ) -> Dict[str, Any]:
        nodes = await asyncio.gather(
            *[
                self._run_node(parent_job_id, phase, group, node, parent_parameters, chain)
                for node in group.jobs
            ]
        )
        success = all(_node_allows_group_success(node, phase) for node in nodes)
        return {
            "phase": phase,
            "group_id": group.group_id,
            "mode": group.mode,
            "status": "success" if success else "failed",
            "success": success,
            "nodes": nodes,
        }

    async def _run_serial_group(
        self,
        parent_job_id: str,
        phase: str,
        group: DependencyGroupConfig,
        parent_parameters: Dict[str, Any],
        chain: tuple[str, ...],
    ) -> Dict[str, Any]:
        nodes: List[Dict[str, Any]] = []
        success = True
        stopped = False
        for node in group.jobs:
            if stopped:
                nodes.append(
                    {
                        "job_id": node.job_id,
                        "phase": phase,
                        "group_id": group.group_id,
                        "status": "skipped",
                        "success": True,
                        "skipped": True,
                        "reason": "previous_serial_dependency_failed",
                    }
                )
                continue
            node_result = await self._run_node(parent_job_id, phase, group, node, parent_parameters, chain)
            nodes.append(node_result)
            if not _node_allows_group_success(node_result, phase):
                success = False
                if node_result.get("failure_policy") in {"stop_chain", "fail_parent", "degrade_parent"}:
                    stopped = True
        return {
            "phase": phase,
            "group_id": group.group_id,
            "mode": group.mode,
            "status": "success" if success else "failed",
            "success": success,
            "nodes": nodes,
        }

    async def _run_node(
        self,
        parent_job_id: str,
        phase: str,
        group: DependencyGroupConfig,
        node: DependencyNodeConfig,
        parent_parameters: Dict[str, Any],
        chain: tuple[str, ...],
    ) -> Dict[str, Any]:
        started = time.monotonic()
        policy = _default_failure_policy(phase, node.failure_policy)
        inherited = {
            name: parent_parameters[name]
            for name in node.inherit
            if name in parent_parameters
        }
        if not node.enabled:
            return {
                "job_id": node.job_id,
                "phase": phase,
                "group_id": group.group_id,
                "status": "disabled",
                "success": True,
                "failure_policy": policy,
                "elapsed_seconds": 0.0,
                "inherited_parameters": inherited,
                "summary": {},
            }
        job_config = self.job_configs.get(node.job_id)
        if job_config is not None and not getattr(job_config, "enabled", True):
            return {
                "job_id": node.job_id,
                "phase": phase,
                "group_id": group.group_id,
                "status": "disabled",
                "success": policy == "ignore",
                "failure_policy": policy,
                "elapsed_seconds": 0.0,
                "inherited_parameters": inherited,
                "summary": {},
            }
        parameters = self._resolve_node_parameters(node, parent_parameters)
        self.logger.info(
            "[Scheduler] Dependency node start: parent=%s phase=%s group=%s job=%s policy=%s",
            parent_job_id,
            phase,
            group.group_id,
            node.job_id,
            policy,
        )
        try:
            coroutine = self.raw_job_runner(node.job_id, parameters, False)
            result = (
                await asyncio.wait_for(coroutine, timeout=node.timeout_seconds)
                if node.timeout_seconds
                else await coroutine
            )
            node_success = is_successful_task_result(result)
            status = "success" if node_success else "failed"
            error = None
        except Exception as exc:
            result = False
            node_success = False
            status = "failed"
            error = str(exc)
        elapsed = time.monotonic() - started
        self.logger.info(
            "[Scheduler] Dependency node done: parent=%s phase=%s group=%s job=%s status=%s elapsed=%.1fs",
            parent_job_id,
            phase,
            group.group_id,
            node.job_id,
            status,
            elapsed,
        )
        return {
            "job_id": node.job_id,
            "phase": phase,
            "group_id": group.group_id,
            "status": status,
            "success": node_success,
            "failure_policy": policy,
            "elapsed_seconds": elapsed,
            "inherited_parameters": {
                name: parameters.get(name)
                for name in node.inherit
                if name in parameters
            },
            "summary": _summarize_task_result(result),
            "error": error,
        }

    def _resolve_node_parameters(
        self,
        node: DependencyNodeConfig,
        parent_parameters: Dict[str, Any],
    ) -> Dict[str, Any]:
        job_config = self.job_configs.get(node.job_id)
        parameters = dict(getattr(job_config, "parameters", {}) or {})
        for name in node.inherit:
            if name in parent_parameters:
                parameters[name] = parent_parameters[name]
        parameters.update(dict(node.parameters or {}))
        return parameters


def _default_failure_policy(phase: str, policy: str) -> str:
    if policy:
        return policy
    if phase == "pre_success":
        return "fail_parent"
    if phase == "post_always":
        return "ignore"
    return "degrade_parent"


def _node_allows_group_success(node_result: Dict[str, Any], phase: str) -> bool:
    if node_result.get("success"):
        return True
    policy = node_result.get("failure_policy") or _default_failure_policy(phase, "")
    return policy == "ignore"


def _summarize_task_result(result: Any) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {"result": bool(result)}
    summary: Dict[str, Any] = {}
    for key in (
        "status",
        "changed_count",
        "facts_written",
        "facts_parsed",
        "reports_parsed",
        "reports_discovered",
        "elapsed_seconds",
    ):
        if key in result:
            summary[key] = result[key]
    backfill = result.get("backfill")
    if isinstance(backfill, dict):
        for key in ("facts_written", "facts_parsed", "reports_parsed", "reports_discovered"):
            if key in backfill:
                summary[key] = backfill[key]
    return summary
