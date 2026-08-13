from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

SCRIPT = Path("scripts/dev_validation/freeze_announcement_asset_backend_candidate.py")
MANIFEST = Path(
    "openspec/changes/establish-shared-announcement-asset-management/evidence/backend_candidate_freeze.json"
)
API_CLIENT_REGISTRY = Path(
    "openspec/changes/establish-shared-announcement-asset-management/evidence/api_client_registry.json"
)


def test_backend_candidate_freeze_manifest_is_current():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--validate-only", "--output", str(MANIFEST)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    assert payload["status"] == "valid"
    assert payload["candidate_id"].startswith("aam-backend-")


def test_backend_candidate_freeze_manifest_records_api_only_client_acceptance():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert manifest["status"] == "frozen_candidate"
    assert manifest["api_client"]["client_mode"] == "ai_api_only"
    assert manifest["api_client"]["integration_status"] == "passed"
    assert manifest["contract"]["openapi_contract_version"] == "annual_report_asset_openapi_v1"
    assert len(manifest["contract"]["openapi_fixture_sha256"]) == 64
    assert len(manifest["contract"]["runtime_openapi_sha256"]) == 64
    assert len(manifest["contract"]["config_fingerprint"]) == 64

    registry = json.loads(API_CLIENT_REGISTRY.read_text(encoding="utf-8"))
    assert registry["gate_status"] == "passed"
    assert registry["api_client"]["backend_candidate_id"] == manifest["candidate_id"]
    assert registry["api_client"]["backend_candidate_freeze_path"] == (
        "evidence/backend_candidate_freeze.json"
    )
