"""Read-only integrity audit and explicitly authorized bounded repair dispatch."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .commands import INTEGRITY_REPAIR_ACTIONS
from .config import AnnouncementAssetConfig
from .operation_control import operation_stop_reason
from .repository import AnnouncementAssetRepository


@dataclass(frozen=True)
class IntegrityFinding:
    content_hash: str
    status: str
    canonical_path: str | None
    expected_length: int | None
    actual_length: int | None = None
    actual_hash: str | None = None


@dataclass(frozen=True)
class IntegrityAuditResult:
    schema_version: str
    status: str
    generated_at: str
    config_fingerprint: str
    read_only: bool
    inspected_count: int
    valid_count: int
    findings: tuple[IntegrityFinding, ...]
    requested_actions: tuple[str, ...]
    completed_actions: int
    report_id: str | None = None


RepairHandler = Callable[[str, str, IntegrityFinding | None], None]


class AnnouncementAssetIntegrityAuditService:
    """Verify catalog bytes without mutation unless every repair gate passes."""

    def __init__(
        self,
        *,
        repository: AnnouncementAssetRepository,
        config: AnnouncementAssetConfig,
        repair_handlers: Mapping[str, RepairHandler] | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.repair_handlers = dict(repair_handlers or {})

    def run(
        self,
        *,
        content_hashes: Sequence[str] | None = None,
        deletion_ids: Sequence[str] | None = None,
        action_flags: Mapping[str, bool] | None = None,
        operator_authorized: bool = False,
        max_targets: int | None = None,
        persist: bool = False,
        operation_id: str | None = None,
        now: str | None = None,
    ) -> IntegrityAuditResult:
        targets = tuple(sorted({str(item).strip() for item in content_hashes or ()}))
        if any(not re.fullmatch(r"[0-9a-f]{64}", item) for item in targets):
            raise ValueError("content_hashes must contain canonical SHA-256 values")
        deletion_targets = tuple(
            sorted({str(item).strip() for item in deletion_ids or ()})
        )
        if any(
            not item
            or item in {".", ".."}
            or "/" in item
            or "\\" in item
            or any(ord(character) < 32 for character in item)
            for item in deletion_targets
        ):
            raise ValueError("deletion_ids contain an invalid identity")
        actions = self._validate_repair_request(
            targets=targets,
            deletion_targets=deletion_targets,
            action_flags=action_flags or {},
            operator_authorized=operator_authorized,
            max_targets=max_targets,
        )
        generated_at = now or datetime.now(timezone.utc).isoformat()
        rows = self._catalog_rows(targets)
        findings: list[IntegrityFinding] = []
        valid = 0
        stopped_reason: str | None = None
        for row in rows:
            if stopped_reason := operation_stop_reason(operation_id):
                break
            finding = self._inspect(row)
            if finding.status == "valid":
                valid += 1
            else:
                findings.append(finding)
        known = {str(row["content_hash"]) for row in rows}
        for missing_hash in sorted(set(targets) - known):
            findings.append(
                IntegrityFinding(
                    content_hash=missing_hash,
                    status="missing_catalog_record",
                    canonical_path=None,
                    expected_length=None,
                )
            )
        findings.extend(self._catalog_invariant_findings())

        completed_actions = 0
        if actions:
            by_hash = {item.content_hash: item for item in findings}
            rows_by_hash = {str(row["content_hash"]): row for row in rows}
            for action in actions:
                handler = self.repair_handlers[action]
                action_targets = (
                    deletion_targets if action == "delete" else targets
                )
                for content_hash in action_targets:
                    if stopped_reason := operation_stop_reason(operation_id):
                        break
                    row = rows_by_hash.get(content_hash)
                    handler(
                        action,
                        content_hash,
                        by_hash.get(
                            content_hash,
                            IntegrityFinding(
                                content_hash=content_hash,
                                status=(
                                    "deletion_intent"
                                    if action == "delete"
                                    else "valid"
                                ),
                                canonical_path=(
                                    None if row is None else str(row["canonical_path"])
                                ),
                                expected_length=(
                                    None if row is None else int(row["content_length"])
                                ),
                            ),
                        ),
                    )
                    completed_actions += 1
                if stopped_reason:
                    break

        result = IntegrityAuditResult(
            schema_version="official_asset_integrity_audit_result.v1",
            status=(
                "partial"
                if stopped_reason
                else "success"
                if not findings
                else "degraded"
            ),
            generated_at=generated_at,
            config_fingerprint=self.config.config_fingerprint,
            read_only=not bool(actions),
            inspected_count=len(rows) + len(set(targets) - known),
            valid_count=valid,
            findings=tuple(findings),
            requested_actions=actions,
            completed_actions=completed_actions,
        )
        if not persist:
            return result
        if result.read_only:
            raise ValueError(
                "read-only integrity audit cannot persist catalog state"
            )
        stored = self.repository.persist_operational_report(
            report_kind="integrity_audit",
            schema_version=result.schema_version,
            config_fingerprint=result.config_fingerprint,
            status=result.status,
            generated_at=result.generated_at,
            payload=asdict(result),
            operation_id=operation_id,
            scope_key="global" if not targets else "bounded_hashes",
        )
        return IntegrityAuditResult(
            **{**result.__dict__, "report_id": str(stored["report_id"])}
        )

    def _validate_repair_request(
        self,
        *,
        targets: tuple[str, ...],
        deletion_targets: tuple[str, ...],
        action_flags: Mapping[str, bool],
        operator_authorized: bool,
        max_targets: int | None,
    ) -> tuple[str, ...]:
        unknown = set(action_flags) - INTEGRITY_REPAIR_ACTIONS
        if unknown:
            raise ValueError(f"unsupported integrity repair actions: {sorted(unknown)}")
        if any(not isinstance(value, bool) for value in action_flags.values()):
            raise ValueError("integrity repair action flags must be boolean")
        actions = tuple(sorted(name for name, enabled in action_flags.items() if enabled))
        if not actions:
            return ()
        if not operator_authorized:
            raise PermissionError("operator authorization is required for repair actions")
        if any(action == "delete" for action in actions) and not deletion_targets:
            raise ValueError("delete requires explicit deletion_id targets")
        if any(action != "delete" for action in actions) and not targets:
            raise ValueError("repair actions require explicit content_hash targets")
        ceiling = self.config.discovery.max_instruments
        if max_targets is not None:
            if int(max_targets) <= 0 or int(max_targets) > ceiling:
                raise ValueError("repair max_targets exceeds configured bound")
            ceiling = int(max_targets)
        if len(targets) + len(deletion_targets) > ceiling:
            raise ValueError("repair target scope exceeds configured bound")
        missing_handlers = set(actions) - set(self.repair_handlers)
        if missing_handlers:
            raise ValueError(
                f"repair handlers are unavailable: {sorted(missing_handlers)}"
            )
        return actions

    def _catalog_rows(self, targets: tuple[str, ...]) -> list[dict[str, Any]]:
        where = ""
        params: tuple[Any, ...] = ()
        if targets:
            placeholders = ",".join("?" for _ in targets)
            where = f" WHERE content_hash IN ({placeholders})"
            params = targets
        with self.repository.connection() as conn:
            rows = conn.execute(
                "SELECT content_hash, content_length, canonical_path "
                "FROM official_document_blobs" + where + " ORDER BY content_hash",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def _inspect(self, row: Mapping[str, Any]) -> IntegrityFinding:
        content_hash = str(row["content_hash"])
        expected_length = int(row["content_length"])
        path = Path(str(row["canonical_path"]))
        if not self._path_is_controlled(path):
            return IntegrityFinding(
                content_hash, "path_outside_controlled_roots", str(path), expected_length
            )
        try:
            stat = path.stat()
        except FileNotFoundError:
            return IntegrityFinding(
                content_hash, "missing_file", str(path), expected_length
            )
        actual_hash = _sha256(path)
        if stat.st_size != expected_length:
            status = "length_mismatch"
        elif actual_hash != content_hash:
            status = "hash_mismatch"
        elif not _is_pdf(path):
            status = "invalid_pdf_signature"
        else:
            status = "valid"
        return IntegrityFinding(
            content_hash=content_hash,
            status=status,
            canonical_path=str(path),
            expected_length=expected_length,
            actual_length=int(stat.st_size),
            actual_hash=actual_hash,
        )

    def _catalog_invariant_findings(self) -> list[IntegrityFinding]:
        with self.repository.connection() as conn:
            duplicates = conn.execute(
                """SELECT instrument_id, fiscal_year, COUNT(*) AS row_count
                   FROM effective_annual_reports
                   WHERE availability='local_valid'
                   GROUP BY instrument_id, fiscal_year HAVING COUNT(*)<>1"""
            ).fetchall()
            orphans = conn.execute(
                """SELECT e.asset_id, e.content_hash
                   FROM effective_annual_reports e
                   LEFT JOIN official_document_blobs b
                     ON b.content_hash=e.content_hash
                   WHERE e.availability='local_valid'
                     AND (e.content_hash IS NULL OR b.content_hash IS NULL)"""
            ).fetchall()
            effective_rows = conn.execute(
                """SELECT e.asset_id, e.content_hash, e.availability,
                          e.decision_state, e.visibility_state,
                          b.canonical_path, b.integrity_status,
                          v.visibility_state AS version_visibility,
                          v.integrity_status AS version_integrity
                   FROM effective_annual_reports e
                   LEFT JOIN official_document_blobs b
                     ON b.content_hash=e.content_hash
                   LEFT JOIN official_attachment_versions v
                     ON v.version_id=e.version_id"""
            ).fetchall()
            same_hash_deletions = conn.execute(
                """SELECT deletion_id, blob_hash, status
                   FROM official_asset_deletion_intents
                   WHERE replacement_blob_hash=blob_hash
                     AND status IN ('planned', 'deleting', 'failed', 'deleted')"""
            ).fetchall()
            false_unlink_audits = conn.execute(
                """SELECT audit_id, blob_hash
                   FROM official_asset_deletion_audit
                   WHERE replacement_blob_hash=blob_hash
                     AND status='deleted'"""
            ).fetchall()
        findings = [
            IntegrityFinding(
                content_hash=f"scope:{row['instrument_id']}:{row['fiscal_year']}",
                status="contradictory_effective_rows",
                canonical_path=None,
                expected_length=None,
            )
            for row in duplicates
        ]
        findings.extend(
            IntegrityFinding(
                content_hash=str(row["content_hash"] or f"asset:{row['asset_id']}"),
                status="orphan_effective_blob",
                canonical_path=None,
                expected_length=None,
            )
            for row in orphans
        )
        temp_root = self.config.temp_root.resolve(strict=False)
        quarantine_root = self.config.quarantine_root.resolve(strict=False)
        backup_root = (
            self.config.backup.destination_root.resolve(strict=False)
            if self.config.backup.enabled
            else None
        )
        for row in effective_rows:
            asset_id = str(row["asset_id"])
            content_hash = str(row["content_hash"] or f"asset:{asset_id}")
            availability = str(row["availability"])
            decision_state = str(row["decision_state"])
            if availability != "local_valid" and decision_state == "current":
                findings.append(
                    IntegrityFinding(
                        content_hash,
                        "unavailable_effective_marked_current",
                        row["canonical_path"],
                        None,
                    )
                )
            if decision_state == "withdrawn" and availability == "local_valid":
                findings.append(
                    IntegrityFinding(
                        content_hash,
                        "withdrawn_effective_is_consumer_available",
                        row["canonical_path"],
                        None,
                    )
                )
            if availability != "local_valid":
                continue
            if str(row["visibility_state"]) != "production" or str(
                row["version_visibility"] or ""
            ) != "production":
                findings.append(
                    IntegrityFinding(
                        content_hash,
                        "nonproduction_object_is_effective",
                        row["canonical_path"],
                        None,
                    )
                )
            if str(row["integrity_status"] or "") != "valid" or str(
                row["version_integrity"] or ""
            ) != "valid":
                findings.append(
                    IntegrityFinding(
                        content_hash,
                        "invalid_object_is_effective",
                        row["canonical_path"],
                        None,
                    )
                )
            if row["canonical_path"]:
                path = Path(str(row["canonical_path"])).resolve(strict=False)
                forbidden_roots = tuple(
                    root
                    for root in (temp_root, quarantine_root, backup_root)
                    if root is not None
                )
                if any(path == root or root in path.parents for root in forbidden_roots):
                    findings.append(
                        IntegrityFinding(
                            content_hash,
                            "nonconsumer_storage_object_is_effective",
                            str(path),
                            None,
                        )
                    )
        findings.extend(
            IntegrityFinding(
                content_hash=str(row["blob_hash"]),
                status="same_hash_physical_deletion_intent",
                canonical_path=f"deletion:{row['deletion_id']}",
                expected_length=None,
            )
            for row in same_hash_deletions
        )
        findings.extend(
            IntegrityFinding(
                content_hash=str(row["blob_hash"]),
                status="same_hash_false_deletion_audit",
                canonical_path=f"deletion-audit:{row['audit_id']}",
                expected_length=None,
            )
            for row in false_unlink_audits
        )
        return findings

    def _path_is_controlled(self, path: Path) -> bool:
        resolved = path.resolve(strict=False)
        roots = (self.config.archive_root, *self.config.adoption_roots)
        return any(
            resolved == root.resolve(strict=False)
            or root.resolve(strict=False) in resolved.parents
            for root in roots
        )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_pdf(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(5) == b"%PDF-"
