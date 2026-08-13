"""Build and validate the fail-closed announcement-asset release trace report."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.dev_validation.migrate_announcement_asset_traceability_v2 import (
    MigrationError,
    parse_tasks,
    validate_v2_registry_data,
)

CHANGE_DIR = (
    PROJECT_ROOT
    / "openspec/changes/establish-shared-announcement-asset-management"
)
EVIDENCE_DIR = CHANGE_DIR / "evidence"
REGISTRY_PATH = EVIDENCE_DIR / "traceability_registry.json"
REPORT_PATH = EVIDENCE_DIR / "release_trace_report.json"
REPORT_SCHEMA_VERSION = "annual_report_asset_release_trace_report.v1"
OPEN_SPEC_SCHEMA_VERSION = "spec-driven"
API_CLIENT_REGISTRY_NAME = "api_client_registry.json"
RELEASE_APPROVAL_REGISTRY_NAME = "release_approval_registry.json"
PRODUCTION_ROLLOUT_REGISTRY_NAME = "production_rollout_registry.json"

LOCAL_TEST_COMMAND = (
    "/home/python/miniconda3/envs/Quote/bin/python -m pytest -q "
    "--basetemp=/dev/shm/aam-release tests/unit -k announcement_asset"
)
PENDING_TASK_EVIDENCE: Mapping[str, tuple[str, ...]] = {
    "9.5": ("evidence/api_client_registry.json",),
    "11.2": (
        "evidence/live_provider_correction_probe_20260812.json",
        "evidence/live_provider_route_gap_20260812.json",
    ),
    "11.3": (
        "evidence/production_capacity_inventory_20260813_v1.json",
        "evidence/production_shadow_adoption_drill_20260812_v4.json",
        "evidence/production_rollout_registry.json",
        "scripts/dev_validation/prepare_announcement_asset_production_shadow.py",
        "scripts/dev_validation/reconcile_announcement_asset_production_shadow.py",
        "scripts/dev_validation/promote_announcement_asset_production_shadow.py",
    ),
    "11.4": ("evidence/production_rollout_registry.json",),
    "11.5": ("evidence/production_rollout_registry.json",),
    "11.6": ("evidence/production_rollout_registry.json",),
    "11.7": (
        "evidence/traceability_registry.json",
        "evidence/api_client_registry.json",
        "evidence/release_approval_registry.json",
    ),
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"JSON object required: {path}")
    return value


def _resolve_previous_v2(registry: Mapping[str, Any], evidence_dir: Path) -> Path:
    baseline = registry.get("previous_requirement_baseline")
    if not isinstance(baseline, Mapping) or not baseline.get("registry_sha256"):
        raise MigrationError("promoted registry lacks a pinned previous v2 baseline")
    expected = str(baseline["registry_sha256"])
    matches = [
        path
        for path in evidence_dir.glob("*.json")
        if path.name != REGISTRY_PATH.name and _sha256(path) == expected
    ]
    if len(matches) != 1:
        raise MigrationError(
            f"pinned previous v2 baseline resolution is ambiguous: {len(matches)}"
        )
    return matches[0]


def _validate_api_client_registry(evidence_dir: Path) -> dict[str, Any]:
    """Validate the API-only client acceptance gate."""

    path = evidence_dir / API_CLIENT_REGISTRY_NAME
    payload = _load(path)
    if payload.get("schema_version") != "annual_report_asset_api_client_registry.v1":
        raise MigrationError("API client registry schema mismatch")
    if payload.get("change_name") != "establish-shared-announcement-asset-management":
        raise MigrationError("API client registry change mismatch")
    api_client = payload.get("api_client")
    required = payload.get("required_acceptance_evidence")
    rollout = payload.get("rollout_effect")
    if not isinstance(api_client, Mapping) or not isinstance(required, Mapping):
        raise MigrationError("API client registry sections missing")
    if not isinstance(rollout, Mapping):
        raise MigrationError("API client rollout effect missing")
    if api_client.get("client_mode") != "ai_api_only":
        raise MigrationError("API client mode must be ai_api_only")
    client_fields = (
        "client_mode",
        "repository",
        "accountable_owner",
        "bound_openapi_contract_version",
        "state_mapping_evidence_path",
        "polling_evidence_path",
        "content_contract_evidence_path",
    )
    for field in client_fields:
        if not isinstance(api_client.get(field), str) or not str(
            api_client[field]
        ).strip():
            raise MigrationError(f"API client registry field missing: {field}")
    freeze_path = api_client.get("backend_candidate_freeze_path")
    freeze_id = api_client.get("backend_candidate_id")
    if freeze_path != "evidence/backend_candidate_freeze.json" or not freeze_id:
        raise MigrationError("API client backend candidate binding is incomplete")
    freeze = _load(evidence_dir / "backend_candidate_freeze.json")
    if freeze.get("schema_version") != "annual_report_asset_backend_candidate_freeze.v1":
        raise MigrationError("backend candidate freeze schema mismatch")
    if freeze.get("status") != "frozen_candidate":
        raise MigrationError("backend candidate is not frozen")
    if freeze.get("candidate_id") != freeze_id:
        raise MigrationError("API client backend candidate id mismatch")
    contract = freeze.get("contract")
    if not isinstance(contract, Mapping) or contract.get(
        "openapi_contract_version"
    ) != api_client.get("bound_openapi_contract_version"):
        raise MigrationError("API client backend OpenAPI binding mismatch")
    required_keys = {
        "authorized_explicit_acquire",
        "unauthorized_denial",
        "duplicate_submit_suppression",
        "retry_after_aligned_caller_owned_polling",
        "content_url_only_when_local_valid",
        "safe_content_streaming",
    }
    if set(required) != required_keys:
        raise MigrationError("API client acceptance evidence keys mismatch")
    gate_status = payload.get("gate_status")
    if gate_status not in {"pending_evidence", "passed"}:
        raise MigrationError("API client registry gate status invalid")
    if gate_status == "passed":
        missing_evidence = [
            key
            for key, value in required.items()
            if not isinstance(value, str)
            or not value.strip()
            or value.strip() == "not_provided"
        ]
        if missing_evidence:
            raise MigrationError(
                "API client passed gate lacks acceptance evidence: "
                + ",".join(sorted(missing_evidence))
            )
        for key, node_id in required.items():
            _validate_pytest_node(str(node_id), label=key)
        for field in (
            "state_mapping_evidence_path",
            "polling_evidence_path",
            "content_contract_evidence_path",
        ):
            _validate_pytest_node(str(api_client[field]), label=field)
        if rollout.get("api_client_integration_claim_allowed") is not True:
            raise MigrationError("API client passed gate does not allow integration claim")
        if rollout.get("production_rollout_allowed") is not False:
            raise MigrationError("API client gate must not bypass production rollout")
        if rollout.get("consumer_cutover_allowed") is not False:
            raise MigrationError("API client gate must not bypass consumer cutover")
    else:
        if rollout.get("api_client_integration_claim_allowed") is True:
            raise MigrationError("API client registry claims readiness without passed gate")
    return payload


def _validate_pytest_node(node_id: str, *, label: str) -> None:
    """Reject acceptance evidence that does not name a real top-level pytest test."""

    parts = node_id.split("::")
    if len(parts) != 2 or not parts[0].startswith("tests/"):
        raise MigrationError(f"API client evidence is not a pytest node: {label}")
    path = PROJECT_ROOT / parts[0]
    if not path.is_file():
        raise MigrationError(f"API client evidence file is missing: {label}")
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError) as exc:
        raise MigrationError(f"API client evidence file is invalid: {label}") from exc
    functions = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    if parts[1] not in functions or not parts[1].startswith("test_"):
        raise MigrationError(f"API client pytest node is missing: {label}")


def _validate_release_approval_registry(evidence_dir: Path) -> dict[str, Any]:
    """Validate the explicit product/operations sign-offs required by task 11.7."""

    path = evidence_dir / RELEASE_APPROVAL_REGISTRY_NAME
    payload = _load(path)
    if payload.get("schema_version") != "annual_report_asset_release_approval_registry.v1":
        raise MigrationError("release approval registry schema mismatch")
    if payload.get("change_name") != "establish-shared-announcement-asset-management":
        raise MigrationError("release approval registry change mismatch")
    signoffs = payload.get("required_signoffs")
    rollout = payload.get("rollout_effect")
    if not isinstance(signoffs, Mapping) or not isinstance(rollout, Mapping):
        raise MigrationError("release approval registry sections missing")
    expected = {
        "deletion_retention_product",
        "deletion_retention_operations",
        "full_market_scope_product",
    }
    if set(signoffs) != expected:
        raise MigrationError("release approval sign-off keys mismatch")
    gate_status = payload.get("gate_status")
    if gate_status not in {"pending_signoff", "passed"}:
        raise MigrationError("release approval gate status invalid")
    if gate_status == "passed":
        incomplete = []
        for key, raw in signoffs.items():
            if not isinstance(raw, Mapping):
                incomplete.append(key)
                continue
            if raw.get("status") != "approved" or any(
                not isinstance(raw.get(field), str) or not str(raw[field]).strip()
                for field in ("accountable_owner", "approved_at", "evidence_path")
            ):
                incomplete.append(key)
        if incomplete:
            raise MigrationError(
                "release approval passed gate lacks sign-off evidence: "
                + ",".join(sorted(incomplete))
            )
        if rollout.get("release_claim_allowed") is not True:
            raise MigrationError("release approval passed gate does not allow release claim")
    elif rollout.get("release_claim_allowed") is True:
        raise MigrationError("release approval registry allows release without passed gate")
    return payload


def _validate_production_rollout_registry(evidence_dir: Path) -> dict[str, Any]:
    """Validate fixed evidence slots for the production-only rollout stages."""

    payload = _load(evidence_dir / PRODUCTION_ROLLOUT_REGISTRY_NAME)
    if payload.get("schema_version") != "annual_report_asset_production_rollout_registry.v1":
        raise MigrationError("production rollout registry schema mismatch")
    if payload.get("change_name") != "establish-shared-announcement-asset-management":
        raise MigrationError("production rollout registry change mismatch")
    stages = payload.get("stages")
    if not isinstance(stages, Mapping) or set(stages) != {
        "11.3",
        "11.4",
        "11.5",
        "11.6",
    }:
        raise MigrationError("production rollout registry stage keys mismatch")
    for task_id, raw in stages.items():
        if not isinstance(raw, Mapping):
            raise MigrationError(f"production rollout stage invalid: {task_id}")
        status = raw.get("status")
        if status not in {"pending", "passed"}:
            raise MigrationError(f"production rollout stage status invalid: {task_id}")
        if status == "passed" and any(
            not isinstance(raw.get(field), str) or not str(raw[field]).strip()
            for field in ("accountable_owner", "completed_at", "evidence_path")
        ):
            raise MigrationError(
                f"production rollout passed stage lacks evidence: {task_id}"
            )
    return payload


def _coverage_by_node(
    registry: Mapping[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    requirements: dict[str, list[dict[str, Any]]] = defaultdict(list)
    specs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in registry.get("coverage_links", []):
        if not isinstance(raw, Mapping) or raw.get("status") != "active":
            continue
        link = dict(raw)
        requirements[str(link["requirement_leaf_id"])].append(link)
        specs[str(link["spec_clause_id"])].append(link)
    return requirements, specs


def _trace_row(
    *,
    kind: str,
    node: Mapping[str, Any],
    links: Sequence[Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    node_id_key = "requirement_leaf_id" if kind == "requirement_leaf" else "spec_clause_id"
    node_id = str(node[node_id_key])
    if node.get("status") != "active":
        return {
            "id": node_id,
            "kind": kind,
            "source_locator": node.get("source_locator"),
            "text_sha256": node.get("text_sha256"),
            "task_ids": [],
            "accountable_owners": [],
            "evidence": [],
            "final_status": "retired",
            "blocking_tasks": [],
        }
    if not links:
        raise MigrationError(f"active trace node lacks exact coverage links: {node_id}")
    task_ids = sorted(
        {str(task_id) for link in links for task_id in link.get("task_ids", [])},
        key=lambda value: tuple(int(part) for part in value.split(".")),
    )
    unknown = set(task_ids) - set(tasks)
    if unknown:
        raise MigrationError(f"trace node references unknown tasks: {node_id}")
    blocking = [task_id for task_id in task_ids if not tasks[task_id]["checked"]]
    evidence_paths = sorted(
        {
            path
            for task_id in blocking
            for path in PENDING_TASK_EVIDENCE.get(task_id, ())
        }
    )
    evidence = (
        [
            {
                "kind": "test_command",
                "command": LOCAL_TEST_COMMAND,
                "status": "passed_local",
            }
        ]
        if not blocking
        else [
            {
                "kind": "gate_evidence",
                "path": path,
                "status": "partial_or_pending",
            }
            for path in evidence_paths
        ]
    )
    return {
        "id": node_id,
        "kind": kind,
        "source_locator": node.get("source_locator"),
        "text_sha256": node.get("text_sha256"),
        "task_ids": task_ids,
        "accountable_owners": sorted(
            {str(link["owner"]) for link in links if str(link.get("owner") or "").strip()}
        ),
        "evidence": evidence,
        "final_status": "passed_local" if not blocking else "pending_release_gate",
        "blocking_tasks": blocking,
    }


def build_report(
    *,
    registry_path: Path = REGISTRY_PATH,
    evidence_dir: Path = EVIDENCE_DIR,
) -> dict[str, Any]:
    registry = _load(registry_path)
    tasks = parse_tasks()
    api_client = _validate_api_client_registry(evidence_dir)
    release_approval = _validate_release_approval_registry(evidence_dir)
    production_rollout = _validate_production_rollout_registry(evidence_dir)
    previous_v2 = _resolve_previous_v2(registry, evidence_dir)
    baseline_path = PROJECT_ROOT / str(registry["previous_baseline"]["registry_path"])
    strict = validate_v2_registry_data(
        registry,
        baseline_path=baseline_path,
        previous_v2_path=previous_v2,
        require_complete=True,
    )
    requirement_links, spec_links = _coverage_by_node(registry)
    rows = [
        *[
            _trace_row(
                kind="requirement_leaf",
                node=node,
                links=requirement_links.get(str(node["requirement_leaf_id"]), ()),
                tasks=tasks,
            )
            for node in registry["requirement_leaves"]
        ],
        *[
            _trace_row(
                kind="spec_clause",
                node=node,
                links=spec_links.get(str(node["spec_clause_id"]), ()),
                tasks=tasks,
            )
            for node in registry["spec_clauses"]
        ],
    ]
    pending_tasks = sorted(
        (task_id for task_id, task in tasks.items() if not task["checked"]),
        key=lambda value: tuple(int(part) for part in value.split(".")),
    )
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "change_name": "establish-shared-announcement-asset-management",
        "openspec_schema_version": OPEN_SPEC_SCHEMA_VERSION,
        "registry_path": str(registry_path.relative_to(CHANGE_DIR)),
        "registry_sha256": _sha256(registry_path),
        "strict_validation": strict,
        "api_client_gate": {
            "status": api_client.get("gate_status"),
            "api_client_integration_claim_allowed": bool(
                api_client.get("rollout_effect", {}).get(
                    "api_client_integration_claim_allowed", False
                )
            ),
            "production_rollout_allowed": bool(
                api_client.get("rollout_effect", {}).get(
                    "production_rollout_allowed", False
                )
            ),
            "consumer_cutover_allowed": bool(
                api_client.get("rollout_effect", {}).get(
                    "consumer_cutover_allowed", False
                )
            ),
        },
        "release_approval_gate": {
            "status": release_approval.get("gate_status"),
            "release_claim_allowed": bool(
                release_approval.get("rollout_effect", {}).get(
                    "release_claim_allowed", False
                )
            ),
        },
        "production_rollout_gate": {
            "status": (
                "passed"
                if all(
                    stage.get("status") == "passed"
                    for stage in production_rollout["stages"].values()
                )
                else "pending"
            ),
            "stages": {
                task_id: stage.get("status")
                for task_id, stage in production_rollout["stages"].items()
            },
        },
        "summary": {
            "requirement_leaf_rows": len(registry["requirement_leaves"]),
            "spec_clause_rows": len(registry["spec_clauses"]),
            "total_rows": len(rows),
            "passed_local_rows": sum(
                row["final_status"] == "passed_local" for row in rows
            ),
            "pending_release_gate_rows": sum(
                row["final_status"] == "pending_release_gate" for row in rows
            ),
            "retired_rows": sum(row["final_status"] == "retired" for row in rows),
            "pending_tasks": pending_tasks,
        },
        "release_status": (
            "ready"
            if not pending_tasks
            and api_client.get("gate_status") == "passed"
            and release_approval.get("gate_status") == "passed"
            and all(
                stage.get("status") == "passed"
                for stage in production_rollout["stages"].values()
            )
            else "pending_release_gates"
        ),
        "rows": rows,
    }
    validate_report(report, registry=registry, tasks=tasks)
    return report


def validate_report(
    report: Mapping[str, Any],
    *,
    registry: Mapping[str, Any],
    tasks: Mapping[str, Mapping[str, Any]],
) -> None:
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        raise ValueError("release trace report schema version mismatch")
    rows = report.get("rows")
    if not isinstance(rows, list):
        raise TypeError("release trace report rows must be a list")
    expected = {
        *(str(node["requirement_leaf_id"]) for node in registry["requirement_leaves"]),
        *(str(node["spec_clause_id"]) for node in registry["spec_clauses"]),
    }
    ids = [str(row.get("id") or "") for row in rows if isinstance(row, Mapping)]
    if len(ids) != len(set(ids)):
        raise ValueError("release trace report contains duplicate ids")
    if set(ids) != expected:
        raise ValueError("release trace report does not exactly cover registry ids")
    pending_tasks = {
        task_id for task_id, task in tasks.items() if not task["checked"]
    }
    if pending_tasks and report.get("release_status") == "ready":
        raise ValueError("release trace report cannot be ready with pending tasks")
    if report.get("release_status") == "ready":
        if report.get("api_client_gate", {}).get("status") != "passed":
            raise ValueError("release trace report cannot be ready without API client acceptance")
        if report.get("release_approval_gate", {}).get("status") != "passed":
            raise ValueError("release trace report cannot be ready without sign-offs")
        if report.get("production_rollout_gate", {}).get("status") != "passed":
            raise ValueError("release trace report cannot be ready without production rollout")
    for row in rows:
        if row["final_status"] == "passed_local" and row["blocking_tasks"]:
            raise ValueError(f"passed trace row has blocking tasks: {row['id']}")
        if row["final_status"] == "pending_release_gate" and not row["blocking_tasks"]:
            raise ValueError(f"pending trace row has no blocking tasks: {row['id']}")
        for evidence in row.get("evidence", []):
            command = evidence.get("command") if isinstance(evidence, Mapping) else None
            if isinstance(command, str) and ("<" in command or ">" in command):
                raise ValueError(
                    f"trace row contains non-reproducible placeholder command: {row['id']}"
                )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--output", type=Path, default=REPORT_PATH)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)
    if args.validate_only:
        actual = _load(args.output)
        expected = build_report(
            registry_path=args.registry,
            evidence_dir=args.registry.parent,
        )
        if actual != expected:
            raise ValueError(
                "release trace report is stale or does not match current release inputs"
            )
        print(json.dumps({"status": "valid", "path": str(args.output)}, sort_keys=True))
        return 0
    report = build_report(
        registry_path=args.registry,
        evidence_dir=args.registry.parent,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
