from __future__ import annotations

import json

import pytest

from scripts.dev_validation import (
    build_announcement_asset_release_trace_report as release,
)


def _registry() -> dict:
    return {
        "requirement_leaves": [
            {
                "requirement_leaf_id": "AAM-V1-REQ-0001",
                "status": "active",
                "source_locator": {"line": 1},
                "text_sha256": "a" * 64,
            }
        ],
        "spec_clauses": [
            {
                "spec_clause_id": "AAM-V1-0001",
                "status": "active",
                "source_locator": {"line": 2},
                "text_sha256": "b" * 64,
            }
        ],
    }


def test_trace_row_fails_closed_for_pending_release_task() -> None:
    row = release._trace_row(
        kind="spec_clause",
        node=_registry()["spec_clauses"][0],
        links=(
            {
                "task_ids": ["11.2"],
                "owner": "provider-validation",
            },
        ),
        tasks={"11.2": {"checked": False}},
    )

    assert row["final_status"] == "pending_release_gate"
    assert row["blocking_tasks"] == ["11.2"]
    assert {item["path"] for item in row["evidence"]} == {
        "evidence/live_provider_correction_probe_20260812.json",
        "evidence/live_provider_route_gap_20260812.json",
    }


def test_trace_row_uses_corrected_capacity_and_shadow_evidence() -> None:
    row = release._trace_row(
        kind="spec_clause",
        node=_registry()["spec_clauses"][0],
        links=({"task_ids": ["11.3"], "owner": "operations"},),
        tasks={"11.3": {"checked": False}},
    )

    assert {item["path"] for item in row["evidence"]} == {
        "evidence/production_capacity_inventory_20260813_v1.json",
        "evidence/production_shadow_adoption_drill_20260812_v4.json",
        "scripts/dev_validation/prepare_announcement_asset_production_shadow.py",
        "scripts/dev_validation/reconcile_announcement_asset_production_shadow.py",
        "scripts/dev_validation/promote_announcement_asset_production_shadow.py",
        "evidence/production_rollout_registry.json",
    }


def test_validate_report_requires_exact_unique_registry_rows() -> None:
    registry = _registry()
    report = {
        "schema_version": release.REPORT_SCHEMA_VERSION,
        "release_status": "pending_release_gates",
        "rows": [
            {
                "id": "AAM-V1-REQ-0001",
                "final_status": "passed_local",
                "blocking_tasks": [],
            },
            {
                "id": "AAM-V1-0001",
                "final_status": "pending_release_gate",
                "blocking_tasks": ["11.2"],
            },
        ],
    }
    release.validate_report(
        report,
        registry=registry,
        tasks={"11.2": {"checked": False}},
    )

    report["rows"][1]["id"] = "AAM-V1-REQ-0001"
    with pytest.raises(ValueError, match="duplicate ids"):
        release.validate_report(
            report,
            registry=registry,
            tasks={"11.2": {"checked": False}},
        )


def test_validate_report_rejects_ready_with_pending_tasks() -> None:
    registry = _registry()
    rows = [
        {
            "id": "AAM-V1-REQ-0001",
            "final_status": "passed_local",
            "blocking_tasks": [],
        },
        {
            "id": "AAM-V1-0001",
            "final_status": "passed_local",
            "blocking_tasks": [],
        },
    ]
    report = {
        "schema_version": release.REPORT_SCHEMA_VERSION,
        "release_status": "ready",
        "rows": json.loads(json.dumps(rows)),
    }

    with pytest.raises(ValueError, match="cannot be ready"):
        release.validate_report(
            report,
            registry=registry,
            tasks={"11.7": {"checked": False}},
        )


def test_validate_report_rejects_placeholder_test_command() -> None:
    registry = _registry()
    report = {
        "schema_version": release.REPORT_SCHEMA_VERSION,
        "release_status": "pending_release_gates",
        "rows": [
            {
                "id": "AAM-V1-REQ-0001",
                "final_status": "passed_local",
                "blocking_tasks": [],
                "evidence": [{"command": "pytest <registered-test-nodes>"}],
            },
            {
                "id": "AAM-V1-0001",
                "final_status": "pending_release_gate",
                "blocking_tasks": ["11.7"],
                "evidence": [],
            },
        ],
    }

    with pytest.raises(ValueError, match="placeholder command"):
        release.validate_report(
            report,
            registry=registry,
            tasks={"11.7": {"checked": False}},
        )


def test_api_client_registry_fails_closed_when_it_claims_readiness(tmp_path):
    path = tmp_path / release.API_CLIENT_REGISTRY_NAME
    payload = {
        "schema_version": "annual_report_asset_api_client_registry.v1",
        "change_name": "establish-shared-announcement-asset-management",
        "api_client": {
            "client_mode": "ai_api_only",
            "repository": "git@github.com:reepool/Quote.git",
            "accountable_owner": "backend_api",
            "backend_candidate_freeze_path": "evidence/backend_candidate_freeze.json",
            "backend_candidate_id": "candidate",
            "bound_openapi_contract_version": "annual_report_asset_openapi_v1",
            "state_mapping_evidence_path": "test-state",
            "polling_evidence_path": "test-polling",
            "content_contract_evidence_path": "test-content",
        },
        "required_acceptance_evidence": {
            "authorized_explicit_acquire": "not_provided",
            "unauthorized_denial": "not_provided",
            "duplicate_submit_suppression": "not_provided",
            "retry_after_aligned_caller_owned_polling": "not_provided",
            "content_url_only_when_local_valid": "not_provided",
            "safe_content_streaming": "not_provided",
        },
        "gate_status": "pending_evidence",
        "rollout_effect": {
            "api_client_integration_claim_allowed": True,
            "production_rollout_allowed": False,
            "consumer_cutover_allowed": False,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "backend_candidate_freeze.json").write_text(
        json.dumps(
            {
                "schema_version": "annual_report_asset_backend_candidate_freeze.v1",
                "status": "frozen_candidate",
                "candidate_id": "candidate",
                "contract": {
                    "openapi_contract_version": "annual_report_asset_openapi_v1"
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="claims readiness"):
        release._validate_api_client_registry(tmp_path)


def test_api_client_passed_gate_requires_complete_fields_and_evidence(tmp_path):
    path = tmp_path / release.API_CLIENT_REGISTRY_NAME
    payload = {
        "schema_version": "annual_report_asset_api_client_registry.v1",
        "change_name": "establish-shared-announcement-asset-management",
        "api_client": {
            "client_mode": "ai_api_only",
            "repository": "",
            "accountable_owner": "backend_api",
            "backend_candidate_freeze_path": "evidence/backend_candidate_freeze.json",
            "backend_candidate_id": "candidate",
            "bound_openapi_contract_version": "annual_report_asset_openapi_v1",
            "state_mapping_evidence_path": "tests/unit/test_api/test_announcement_asset_routes.py",
            "polling_evidence_path": "tests/unit/test_api/test_announcement_asset_routes.py",
            "content_contract_evidence_path": "tests/unit/test_api/test_announcement_asset_routes.py",
        },
        "required_acceptance_evidence": {
            "authorized_explicit_acquire": "test-acquire",
            "unauthorized_denial": "test-denial",
            "duplicate_submit_suppression": "test-dedup",
            "retry_after_aligned_caller_owned_polling": "test-polling",
            "content_url_only_when_local_valid": "test-content-url",
            "safe_content_streaming": "test-content-stream",
        },
        "gate_status": "passed",
        "rollout_effect": {
            "api_client_integration_claim_allowed": True,
            "production_rollout_allowed": False,
            "consumer_cutover_allowed": False,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(Exception, match="registry field missing"):
        release._validate_api_client_registry(tmp_path)

    payload["api_client"]["repository"] = "git@github.com:reepool/Quote.git"
    (tmp_path / "backend_candidate_freeze.json").write_text(
        json.dumps(
            {
                "schema_version": "annual_report_asset_backend_candidate_freeze.v1",
                "status": "frozen_candidate",
                "candidate_id": "candidate",
                "contract": {
                    "openapi_contract_version": "annual_report_asset_openapi_v1"
                },
            }
        ),
        encoding="utf-8",
    )
    payload["required_acceptance_evidence"]["unauthorized_denial"] = "not_provided"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(Exception, match="lacks acceptance evidence"):
        release._validate_api_client_registry(tmp_path)


def test_api_client_passed_gate_rejects_missing_pytest_node(tmp_path):
    path = tmp_path / release.API_CLIENT_REGISTRY_NAME
    evidence = (
        "tests/unit/test_api/test_announcement_asset_routes.py::"
        "test_authenticated_ensure_returns_caller_handle_without_internal_operation_id"
    )
    payload = {
        "schema_version": "annual_report_asset_api_client_registry.v1",
        "change_name": "establish-shared-announcement-asset-management",
        "api_client": {
            "client_mode": "ai_api_only",
            "repository": "git@github.com:reepool/Quote.git",
            "accountable_owner": "backend_api",
            "backend_candidate_freeze_path": "evidence/backend_candidate_freeze.json",
            "backend_candidate_id": "candidate",
            "bound_openapi_contract_version": "annual_report_asset_openapi_v1",
            "state_mapping_evidence_path": evidence,
            "polling_evidence_path": evidence,
            "content_contract_evidence_path": evidence,
        },
        "required_acceptance_evidence": {
            "authorized_explicit_acquire": evidence,
            "unauthorized_denial": evidence,
            "duplicate_submit_suppression": evidence,
            "retry_after_aligned_caller_owned_polling": evidence,
            "content_url_only_when_local_valid": (
                "tests/unit/test_api/test_announcement_asset_routes.py::"
                "test_does_not_exist"
            ),
            "safe_content_streaming": evidence,
        },
        "gate_status": "passed",
        "rollout_effect": {
            "api_client_integration_claim_allowed": True,
            "production_rollout_allowed": False,
            "consumer_cutover_allowed": False,
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "backend_candidate_freeze.json").write_text(
        json.dumps(
            {
                "schema_version": "annual_report_asset_backend_candidate_freeze.v1",
                "status": "frozen_candidate",
                "candidate_id": "candidate",
                "contract": {
                    "openapi_contract_version": "annual_report_asset_openapi_v1"
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(Exception, match="pytest node is missing"):
        release._validate_api_client_registry(tmp_path)


def test_release_approval_gate_requires_all_three_explicit_signoffs(tmp_path):
    path = tmp_path / release.RELEASE_APPROVAL_REGISTRY_NAME
    payload = {
        "schema_version": "annual_report_asset_release_approval_registry.v1",
        "change_name": "establish-shared-announcement-asset-management",
        "gate_status": "passed",
        "required_signoffs": {
            key: {
                "status": "approved",
                "accountable_owner": "owner",
                "approved_at": "2026-08-13T00:00:00+00:00",
                "evidence_path": "evidence/signoff.json",
            }
            for key in (
                "deletion_retention_product",
                "deletion_retention_operations",
                "full_market_scope_product",
            )
        },
        "rollout_effect": {"release_claim_allowed": True},
    }
    payload["required_signoffs"]["deletion_retention_operations"][
        "accountable_owner"
    ] = None
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(Exception, match="lacks sign-off evidence"):
        release._validate_release_approval_registry(tmp_path)


def test_production_rollout_passed_stage_requires_owner_time_and_evidence(tmp_path):
    path = tmp_path / release.PRODUCTION_ROLLOUT_REGISTRY_NAME
    payload = {
        "schema_version": "annual_report_asset_production_rollout_registry.v1",
        "change_name": "establish-shared-announcement-asset-management",
        "stages": {
            task_id: {
                "status": "pending",
                "accountable_owner": None,
                "completed_at": None,
                "evidence_path": None,
            }
            for task_id in ("11.3", "11.4", "11.5", "11.6")
        },
    }
    payload["stages"]["11.4"]["status"] = "passed"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(Exception, match="lacks evidence"):
        release._validate_production_rollout_registry(tmp_path)


def test_validate_only_rebuilds_report_and_rejects_stale_output(
    tmp_path, monkeypatch
):
    registry = tmp_path / "registry.json"
    output = tmp_path / "report.json"
    registry.write_text("{}", encoding="utf-8")
    output.write_text(json.dumps({"schema_version": release.REPORT_SCHEMA_VERSION}), encoding="utf-8")
    expected = {"schema_version": release.REPORT_SCHEMA_VERSION, "rows": []}
    monkeypatch.setattr(release, "build_report", lambda **_kwargs: expected)

    with pytest.raises(ValueError, match="stale"):
        release.main(
            ["--registry", str(registry), "--output", str(output), "--validate-only"]
        )
