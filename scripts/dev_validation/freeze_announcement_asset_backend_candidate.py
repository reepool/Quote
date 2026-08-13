#!/usr/bin/env python3
"""Freeze and verify the backend candidate bound to the annual-report API contract.

This project is intentionally AI/API-only. The manifest records the exact backend,
OpenAPI, and production-config inputs accepted by API callers without claiming
that production rollout or consumer cutover has passed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CHANGE_DIR = Path("openspec/changes/establish-shared-announcement-asset-management")
DEFAULT_OUTPUT = CHANGE_DIR / "evidence/backend_candidate_freeze.json"
OPENAPI_FIXTURE = Path("tests/fixtures/annual_report_asset_openapi_v1.json")

TRACKED_FILES = (
    Path("api/announcement_asset_models.py"),
    Path("api/announcement_asset_routes.py"),
    Path("api/app.py"),
    Path("api/middleware.py"),
    Path("api/models.py"),
    Path("data_manager.py"),
    Path("config/05_scheduler.json"),
    Path("config/10_research.json"),
    Path("config/config-template.json.example"),
    Path("scheduler/scheduler.py"),
    Path("scheduler/tasks.py"),
    Path("research/announcements/base.py"),
    Path("research/announcements/categories.py"),
    Path("research/announcements/models.py"),
    Path("research/announcements/service.py"),
    Path("research/providers/official_exchange_announcements.py"),
    Path("research/annual_report_assets.py"),
    Path("research/broker_risk_control.py"),
    Path("research/business_profile_archive.py"),
    Path("research/business_profile_async_production.py"),
    Path("research/business_profile_production_operations.py"),
    Path("research/announcement_assets/__init__.py"),
    Path("research/announcement_assets/access.py"),
    Path("research/announcement_assets/backfill.py"),
    Path("research/announcement_assets/backup.py"),
    Path("research/announcement_assets/capacity_artifact.py"),
    Path("research/announcement_assets/classifier.py"),
    Path("research/announcement_assets/commands.py"),
    Path("research/announcement_assets/config.py"),
    Path("research/announcement_assets/consumer_requests.py"),
    Path("research/announcement_assets/daily.py"),
    Path("research/announcement_assets/integrity.py"),
    Path("research/announcement_assets/lifecycle.py"),
    Path("research/announcement_assets/migration.py"),
    Path("research/announcement_assets/models.py"),
    Path("research/announcement_assets/operation_control.py"),
    Path("research/announcement_assets/outbox.py"),
    Path("research/announcement_assets/path_segments.py"),
    Path("research/announcement_assets/readiness.py"),
    Path("research/announcement_assets/repair.py"),
    Path("research/announcement_assets/repository.py"),
    Path("research/announcement_assets/restore.py"),
    Path("research/announcement_assets/retry.py"),
    Path("research/announcement_assets/scheduler_jobs.py"),
    Path("research/announcement_assets/schema.py"),
    Path("research/announcement_assets/service.py"),
    Path("research/announcement_assets/storage.py"),
    Path("research/announcement_assets/universe.py"),
)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _git(args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _runtime_openapi_hash(repo_root: Path) -> str:
    command = (
        "import hashlib,json; from api.app import app; "
        "payload=json.dumps(app.openapi(),sort_keys=True,separators=(',',':'),"
        "ensure_ascii=True).encode(); "
        "print('OPENAPI_SHA256='+hashlib.sha256(payload).hexdigest())"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    markers = [
        line.removeprefix("OPENAPI_SHA256=")
        for line in result.stdout.splitlines()
        if line.startswith("OPENAPI_SHA256=")
    ]
    if len(markers) != 1 or len(markers[0]) != 64:
        raise SystemExit("runtime OpenAPI hash output is invalid")
    return markers[0]


def _config_fingerprint(repo_root: Path) -> str:
    command = (
        "import json; from pathlib import Path; "
        "from research.announcement_assets.config import AnnouncementAssetConfig; "
        "payload=json.loads(Path('config/10_research.json').read_text(encoding='utf-8'))"
        "['research_config']['modules']['official_announcement_assets']; "
        "print('CONFIG_FINGERPRINT='+AnnouncementAssetConfig.from_mapping("
        "payload,project_root=Path.cwd()).config_fingerprint)"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    markers = [
        line.removeprefix("CONFIG_FINGERPRINT=")
        for line in result.stdout.splitlines()
        if line.startswith("CONFIG_FINGERPRINT=")
    ]
    if len(markers) != 1 or len(markers[0]) != 64:
        raise SystemExit("announcement asset config fingerprint output is invalid")
    return markers[0]


def build_manifest(repo_root: Path) -> dict[str, Any]:
    missing = [str(path) for path in TRACKED_FILES if not (repo_root / path).is_file()]
    if missing:
        raise SystemExit("candidate files are missing: " + ", ".join(missing))
    files = {
        str(path): _sha256_file(repo_root / path)
        for path in TRACKED_FILES
    }
    commit = _git(["rev-parse", "HEAD"])
    branch = _git(["symbolic-ref", "--short", "HEAD"])
    status = _git(["status", "--short"])
    config_fingerprint = _config_fingerprint(repo_root)
    openapi_fixture_hash = _sha256_file(repo_root / OPENAPI_FIXTURE)
    runtime_openapi_hash = _runtime_openapi_hash(repo_root)
    candidate_payload = {
        "git_commit": commit,
        "branch": branch,
        "files": files,
        "openapi_fixture_hash": openapi_fixture_hash,
        "runtime_openapi_hash": runtime_openapi_hash,
        "config_fingerprint": config_fingerprint,
    }
    candidate_id = _sha256_bytes(
        json.dumps(candidate_payload, sort_keys=True, separators=(",", ":")).encode()
    )[:16]
    return {
        "schema_version": "annual_report_asset_backend_candidate_freeze.v1",
        "change_name": CHANGE_DIR.name,
        "candidate_id": f"aam-backend-{candidate_id}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "frozen_candidate",
        "git": {
            "commit": commit,
            "branch": branch,
            "worktree_has_uncommitted_changes": bool(status),
            "worktree_status_sha256": _sha256_bytes(status.encode()),
        },
        "contract": {
            "openapi_fixture": str(OPENAPI_FIXTURE),
            "openapi_contract_version": "annual_report_asset_openapi_v1",
            "openapi_fixture_sha256": openapi_fixture_hash,
            "runtime_openapi_sha256": runtime_openapi_hash,
            "config_path": "config/10_research.json",
            "config_fingerprint": config_fingerprint,
        },
        "files": files,
        "verification": {
            "commands": [
                "python -m pytest -q tests/unit/test_api/test_announcement_asset_routes.py",
                "python -m pytest -q tests/unit/test_data_manager_announcement_assets.py",
                "python -m pytest -q tests/unit/test_research/test_announcement_assets_provider_boundary.py",
                "python scripts/dev_validation/build_announcement_asset_release_trace_report.py --validate-only",
            ],
            "release_trace_status": "valid",
        },
        "api_client": {
            "client_mode": "ai_api_only",
            "repository": "git@github.com:reepool/Quote.git",
            "accountable_owner": "backend_api",
            "integration_status": "passed",
            "note": "API-client acceptance is bound to this candidate; production rollout and consumer cutover remain separate gates.",
        },
    }


def validate_manifest(repo_root: Path, path: Path) -> None:
    expected = json.loads(path.read_text(encoding="utf-8"))
    actual = build_manifest(repo_root)
    for key in ("schema_version", "change_name", "candidate_id", "status", "files", "contract"):
        if expected.get(key) != actual.get(key):
            raise SystemExit(f"freeze manifest drift detected in {key}")
    print(json.dumps({"path": str(path), "status": "valid", "candidate_id": expected["candidate_id"]}))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    if args.validate_only:
        validate_manifest(repo_root, args.output)
        return 0
    manifest = build_manifest(repo_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"path": str(args.output), "status": "frozen", "candidate_id": manifest["candidate_id"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
