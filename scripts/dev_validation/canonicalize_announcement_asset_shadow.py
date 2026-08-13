#!/usr/bin/env python3
"""Copy production shadow blobs into the shared canonical archive.

Planning is read-only. Apply requires the exact immutable plan, explicit
confirmation, and a disabled/dry-run module. Legacy files are never modified or
deleted; each unique blob is copy-verified and atomically published first.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.announcement_assets import (
    AnnouncementAssetConfig,
    AnnouncementAssetRepository,
)
from research.announcement_assets.storage import (
    ContentAddressedBlobStore,
    probe_mount_identity,
)
from scripts.dev_validation.inventory_announcement_asset_capacity import (
    _validate_new_output_path,
    _write_new_json,
)
from scripts.dev_validation.prepare_announcement_asset_production_shadow import (
    _require_production_catalog,
    _require_shadow_safe_config,
)
from utils.config_manager import config_manager

SCHEMA_VERSION = "annual_report_asset_shadow_canonicalization.v1"
CONFIRMATION_TOKEN = "CANONICALIZE_PRODUCTION_SHADOW"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_pdf(path: Path, content_hash: str, content_length: int) -> None:
    state = path.stat()
    if path.is_symlink() or not path.is_file() or int(state.st_size) != content_length:
        raise RuntimeError(f"canonicalization file identity mismatch: {path}")
    with path.open("rb") as handle:
        if handle.read(5) != b"%PDF-":
            raise RuntimeError(f"canonicalization source is not PDF: {path}")
    if _sha256(path) != content_hash:
        raise RuntimeError(f"canonicalization hash mismatch: {path}")


def _canonical_target(root: Path, content_hash: str) -> Path:
    if len(content_hash) != 64 or any(ch not in "0123456789abcdef" for ch in content_hash):
        raise ValueError("canonicalization content hash is not lowercase SHA-256")
    return root / content_hash[:2] / f"{content_hash}.pdf"


def _catalog_identity(path: Path) -> Mapping[str, Any]:
    state = path.stat()
    return {
        "path": str(path.resolve(strict=True)),
        "device": int(state.st_dev),
        "inode": int(state.st_ino),
    }


def build_plan(
    *,
    production_db: Path,
    config: AnnouncementAssetConfig,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, Any]:
    _require_shadow_safe_config(config)
    production = _require_production_catalog(production_db, project_root=project_root)
    ContentAddressedBlobStore(config).validate_mount()
    repository = AnnouncementAssetRepository(production)
    reports = repository.list_effective_reports(include_shadow=True, limit=100000)
    if not reports or any(report.visibility_state != "shadow" for report in reports):
        raise RuntimeError("canonicalization requires an all-shadow production catalog")
    entries: dict[str, Mapping[str, Any]] = {}
    for report in reports:
        if not report.content_hash:
            raise RuntimeError("shadow report has no content hash")
        blob = repository.get_blob(report.content_hash)
        if blob is None:
            raise RuntimeError("shadow report blob is missing")
        source = Path(blob.canonical_path).resolve(strict=True)
        target = _canonical_target(config.blob_root, blob.content_hash)
        _verify_pdf(source, blob.content_hash, int(blob.content_length))
        row = {
            "content_hash": blob.content_hash,
            "content_length": int(blob.content_length),
            "source_path": str(source),
            "target_path": str(target),
            "source_mount_filesystem_key": probe_mount_identity(source).filesystem_key,
            "target_mount_filesystem_key": probe_mount_identity(config.blob_root).filesystem_key,
        }
        previous = entries.setdefault(blob.content_hash, row)
        if previous != row:
            raise RuntimeError("one content hash has inconsistent canonicalization evidence")
    ordered = [entries[key] for key in sorted(entries)]
    plan_basis = {
        "schema_version": SCHEMA_VERSION,
        "configuration_fingerprint": config.config_fingerprint,
        "production_catalog": _catalog_identity(production),
        "canonical_root": str(config.blob_root.resolve(strict=False)),
        "entries": ordered,
    }
    fingerprint = hashlib.sha256(
        json.dumps(plan_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        **plan_basis,
        "mode": "plan",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "plan_fingerprint": fingerprint,
        "summary": {
            "report_count": len(reports),
            "unique_blob_count": len(ordered),
            "total_bytes": sum(int(item["content_length"]) for item in ordered),
        },
        "network_requests": 0,
        "legacy_archive_mutations": 0,
        "production_visible_rows_added": 0,
    }


def _publish_copy(
    source: Path, target: Path, content_hash: str, content_length: int
) -> bool:
    root = target.parents[1]
    shard = target.parent.name
    temporary_name = f".{target.name}.{os.getpid()}.part"
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise RuntimeError("canonicalization root is a symlink")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root_fd = os.open(root, directory_flags)
    shard_fd: int | None = None
    try:
        try:
            os.mkdir(shard, mode=0o755, dir_fd=root_fd)
        except FileExistsError:
            pass
        shard_fd = os.open(shard, directory_flags, dir_fd=root_fd)
        if target.exists():
            _verify_published_fd(
                shard_fd, target.name, content_hash, content_length, target
            )
            return False
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=shard_fd,
        )
        with source.open("rb") as source_handle, os.fdopen(
            temporary_fd, "wb"
        ) as target_handle:
            shutil.copyfileobj(source_handle, target_handle, length=1024 * 1024)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        _verify_fd(shard_fd, temporary_name, content_hash, content_length)
        _verify_pdf(source, content_hash, content_length)
        try:
            os.link(
                temporary_name,
                target.name,
                src_dir_fd=shard_fd,
                dst_dir_fd=shard_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            _verify_published_fd(
                shard_fd, target.name, content_hash, content_length, target
            )
            return False
        _verify_published_fd(shard_fd, target.name, content_hash, content_length, target)
        os.fsync(shard_fd)
        return True
    finally:
        if shard_fd is not None:
            try:
                os.unlink(temporary_name, dir_fd=shard_fd)
            except FileNotFoundError:
                pass
            os.close(shard_fd)
        os.close(root_fd)


def _verify_fd(
    directory_fd: int,
    name: str,
    content_hash: str,
    content_length: int,
) -> os.stat_result:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    digest = hashlib.sha256()
    try:
        state = os.fstat(fd)
        if int(state.st_size) != content_length:
            raise RuntimeError("canonicalization published length mismatch")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            if handle.read(5) != b"%PDF-":
                raise RuntimeError("canonicalization published file is not PDF")
            handle.seek(0)
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != content_hash:
            raise RuntimeError("canonicalization published hash mismatch")
        return state
    finally:
        os.close(fd)


def _verify_published_fd(
    directory_fd: int,
    name: str,
    content_hash: str,
    content_length: int,
    target: Path,
) -> None:
    descriptor_state = _verify_fd(
        directory_fd, name, content_hash, content_length
    )
    if target.is_symlink():
        raise RuntimeError("canonicalization target is a symlink")
    path_state = target.stat()
    if (path_state.st_dev, path_state.st_ino) != (
        descriptor_state.st_dev,
        descriptor_state.st_ino,
    ):
        raise RuntimeError("canonicalization target directory changed during publish")
    _verify_pdf(target, content_hash, content_length)


def _validated_plan(
    plan: Mapping[str, Any],
    *,
    production: Path,
    repository: AnnouncementAssetRepository,
    config: AnnouncementAssetConfig,
) -> list[Mapping[str, Any]]:
    entries = plan.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("canonicalization plan entries are missing")
    plan_basis = {
        "schema_version": plan.get("schema_version"),
        "configuration_fingerprint": plan.get("configuration_fingerprint"),
        "production_catalog": plan.get("production_catalog"),
        "canonical_root": plan.get("canonical_root"),
        "entries": entries,
    }
    fingerprint = hashlib.sha256(
        json.dumps(plan_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if plan.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("canonicalization plan schema mismatch")
    if plan.get("plan_fingerprint") != fingerprint:
        raise ValueError("canonicalization plan fingerprint mismatch")
    if plan.get("configuration_fingerprint") != config.config_fingerprint:
        raise ValueError("canonicalization plan config mismatch")
    if plan.get("production_catalog") != _catalog_identity(production):
        raise ValueError("canonicalization production catalog identity changed")
    if plan.get("canonical_root") != str(config.blob_root.resolve(strict=False)):
        raise ValueError("canonicalization root changed")

    reports = repository.list_effective_reports(include_shadow=True, limit=100000)
    if not reports or any(report.visibility_state != "shadow" for report in reports):
        raise RuntimeError("canonicalization requires an all-shadow production catalog")
    expected_hashes = {str(report.content_hash) for report in reports if report.content_hash}
    planned_hashes = {str(entry.get("content_hash") or "") for entry in entries}
    if len(planned_hashes) != len(entries) or planned_hashes != expected_hashes:
        raise RuntimeError("canonicalization plan does not cover the exact shadow set")
    for entry in entries:
        content_hash = str(entry.get("content_hash") or "")
        content_length = int(entry.get("content_length") or 0)
        source = Path(str(entry.get("source_path") or "")).resolve(strict=True)
        target = Path(str(entry.get("target_path") or "")).resolve(strict=False)
        if target != _canonical_target(config.blob_root, content_hash):
            raise ValueError("canonicalization target does not match content hash")
        _verify_pdf(source, content_hash, content_length)
        if (
            probe_mount_identity(source).filesystem_key
            != entry.get("source_mount_filesystem_key")
        ):
            raise RuntimeError("canonicalization source mount changed")
        if (
            probe_mount_identity(config.blob_root).filesystem_key
            != entry.get("target_mount_filesystem_key")
        ):
            raise RuntimeError("canonicalization target mount changed")
        blob = repository.get_blob(content_hash)
        if blob is None or int(blob.content_length) != content_length:
            raise RuntimeError("canonicalization blob metadata changed")
        current = Path(blob.canonical_path).resolve(strict=False)
        if current not in {source, target}:
            raise RuntimeError("blob canonical path changed after planning")
    return [dict(entry) for entry in entries]


def apply_plan(
    *,
    production_db: Path,
    plan: Mapping[str, Any],
    config: AnnouncementAssetConfig,
    operator: str,
    confirmation: str,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, Any]:
    _require_shadow_safe_config(config)
    if confirmation != CONFIRMATION_TOKEN or not str(operator).strip():
        raise PermissionError("canonicalization requires operator and confirmation")
    production = _require_production_catalog(production_db, project_root=project_root)
    ContentAddressedBlobStore(config).validate_mount()
    repository = AnnouncementAssetRepository(production)
    entries = _validated_plan(
        plan,
        production=production,
        repository=repository,
        config=config,
    )
    before_visible = len(repository.list_effective_reports(limit=100000))
    required_copy_bytes = sum(
        int(entry["content_length"])
        for entry in entries
        if not Path(str(entry["target_path"])).exists()
    )
    usage = shutil.disk_usage(config.filings_root)
    if usage.free - required_copy_bytes < config.storage.free_space_reserve_bytes:
        raise RuntimeError("canonicalization storage reserve would be breached")
    copied = reused = 0
    for entry in entries:
        source = Path(str(entry["source_path"]))
        target = Path(str(entry["target_path"]))
        content_hash = str(entry["content_hash"])
        content_length = int(entry["content_length"])
        if _publish_copy(source, target, content_hash, content_length):
            copied += 1
        else:
            reused += 1
        repository.compare_and_set_blob_path(
            content_hash,
            expected_path=source,
            canonical_path=target,
        )
        _verify_pdf(source, content_hash, content_length)
        _verify_pdf(target, content_hash, content_length)
    after_visible = len(repository.list_effective_reports(limit=100000))
    if before_visible != 0 or after_visible != 0:
        raise RuntimeError("canonicalization changed production visibility")
    return {
        **dict(plan),
        "mode": "apply",
        "operator": str(operator).strip(),
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "canonical_copies_created": copied,
        "canonical_copies_reused": reused,
        "catalog_blob_paths_updated": len(entries),
        "legacy_archive_mutations": 0,
        "production_visible_rows_added": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--production-db", type=Path, default=PROJECT_ROOT / "data/research.db")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator", default="")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    output = _validate_new_output_path(args.output, project_root=PROJECT_ROOT)
    config = AnnouncementAssetConfig.from_research_config(
        config_manager.get_research_config(), project_root=PROJECT_ROOT
    )
    if args.apply:
        if args.plan is None:
            parser.error("--apply requires --plan")
        plan = json.loads(args.plan.read_text(encoding="utf-8"))
        payload = apply_plan(
            production_db=args.production_db,
            plan=plan,
            config=config,
            operator=args.operator,
            confirmation=args.confirm,
        )
    else:
        payload = build_plan(production_db=args.production_db, config=config)
    _write_new_json(output, payload)
    print(json.dumps(payload, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
